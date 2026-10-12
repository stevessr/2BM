//! Crash-resilient and encrypted mailbox for own-device QQ webhooks.
//! NapCat may synchronously wait for HTTP webhook completion while processing
//! our outgoing OneBot send. ACK the authenticated webhook after a durable
//! database commit, then process asynchronously with source-ID idempotency.
use crate::{crypto, relay, AppState};
use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::sync::Arc;

pub async fn enqueue_qq_own(
    state:&Arc<AppState>,account:&str,chat:&str,message_id:&str,text:&str
)->Result<()>{
    let key=format!("{account}:{chat}:{message_id}");
    let sealed=crypto::encrypt(&state.config.credential_key,
        &json!({"chat":chat,"message_id":message_id,"text":text}))?;
    sqlx::query("INSERT OR IGNORE INTO qq_own_inbox
        (source_key,account_id,sealed) VALUES (?,?,?)")
        .bind(key).bind(account).bind(sealed).execute(&state.pool).await?;
    Ok(())
}

pub async fn drain_qq_own(state:&Arc<AppState>,limit:i64)->Result<usize>{
    let items:Vec<(String,String,String)>=sqlx::query_as(
        "SELECT source_key,account_id,sealed FROM qq_own_inbox
         WHERE next_attempt_at <= CURRENT_TIMESTAMP
         ORDER BY next_attempt_at,received_at,source_key LIMIT ?")
        .bind(limit.clamp(1,100)).fetch_all(&state.pool).await?;
    let mut success=0;
    for (key,account,sealed) in items {
        // Do not lose a queued message if the decryption key is misconfigured;
        // the record stays in SQLite for operator-controlled recovery.
        let result:Result<()> = async {
            // Accounts can be disabled between durable HTTP ACK and worker
            // delivery. Retain the envelope instead of silently discarding
            // it through relay::incoming's disabled-account no-op.
            let active=crate::db::account(&state.pool,&account).await?
                .is_some_and(|a|a.enabled && a.platform=="qq");
            if !active {
                anyhow::bail!("QQ account is disabled or missing; queued message retained");
            }
            let msg:Value=crypto::decrypt(&state.config.credential_key,&sealed)
                .context("cannot decrypt queued QQ webhook")?;
            let chat=msg["chat"].as_str().context("queued QQ chat missing")?;
            let msg_id=msg["message_id"].as_str()
                .context("queued QQ message id missing")?;
            let text=msg["text"].as_str().context("queued QQ text missing")?;
            relay::incoming_own(
                state,&account,chat,msg_id,&format!("qq-self:{account}"),
                "Me (QQ)",text
            ).await
        }.await;
        match result {
            Ok(())=>{
                sqlx::query("DELETE FROM qq_own_inbox WHERE source_key=?")
                    .bind(&key).execute(&state.pool).await?;
                success+=1;
            },
            Err(err)=>{
                sqlx::query("UPDATE qq_own_inbox SET attempts=attempts+1,
                    next_attempt_at=datetime('now',
                        '+' || MIN(3600,(attempts+1)*(attempts+1)*5) || ' seconds'),
                    last_error='delivery temporarily unavailable' WHERE source_key=?")
                    .bind(&key).execute(&state.pool).await?;
                tracing::warn!(account=%account,%err,
                    "queued QQ webhook could not be delivered; retry scheduled");
                // A failed account must not starve other accounts in the inbox.
            }
        }
    }
    Ok(success)
}


/// Store incoming messages, recalls and group membership changes before
/// acknowledging NapCat. The payload stays XChaCha-encrypted at rest.
pub async fn enqueue_qq_event(
    state:&Arc<AppState>,account:&str,source_key:&str,body:&Value
)->Result<()>{
    let sealed=crypto::encrypt(&state.config.credential_key,body)?;
    sqlx::query("INSERT OR IGNORE INTO qq_events_inbox
        (source_key,account_id,sealed) VALUES (?,?,?)")
        .bind(source_key).bind(account).bind(sealed)
        .execute(&state.pool).await?;
    Ok(())
}

pub async fn drain_qq_events(state:&Arc<AppState>,limit:i64)->Result<usize>{
    let items:Vec<(String,String,String)>=sqlx::query_as(
        "SELECT source_key,account_id,sealed FROM qq_events_inbox
         WHERE next_attempt_at <= CURRENT_TIMESTAMP
         ORDER BY next_attempt_at,received_at,source_key LIMIT ?")
        .bind(limit.clamp(1,100)).fetch_all(&state.pool).await?;
    let mut processed=0;
    for (key,account_id,sealed) in items {
        let outcome:Result<()> = async {
            let account=crate::db::account(&state.pool,&account_id).await?
                .filter(|a|a.enabled && a.platform=="qq")
                .context("QQ account disabled; event must remain queued")?;
            let body:Value=crypto::decrypt(&state.config.credential_key,&sealed)
                .context("cannot decrypt queued QQ callback")?;
            if body["post_type"]=="notice" {
                if let Some((chat,id))=crate::adapters::qq::recall_target(&body) {
                    relay::recall(state,&account_id,&chat,&id).await?;
                } else if let Some((chat,contact,joined))=
                    crate::adapters::qq::membership_target(&body) {
                    crate::adapters::qq::membership_notice(
                        state,&account,&chat,&contact,joined).await?;
                }
            } else if body["post_type"]=="message" {
                let chat=crate::adapters::qq::chat_id(&body)
                    .context("queued QQ message has no chat")?;
                let id=body["message_id"].as_str().map(str::to_owned)
                    .or_else(||body["message_id"].as_i64().map(|n|n.to_string()))
                    .context("queued QQ message has no ID")?;
                let sender_id=body["user_id"].as_i64()
                    .map(|n|n.to_string())
                    .context("queued QQ message has no sender")?;
                let sender=body["sender"]["card"].as_str().filter(|s|!s.is_empty())
                    .or_else(||body["sender"]["nickname"].as_str())
                    .unwrap_or("QQ user");
                let text=crate::adapters::qq::message_text(&body);
                let reply=crate::adapters::qq::reply_target(&body);
                relay::incoming_with_reply(
                    state,&account_id,&chat,&id,&sender_id,sender,
                    &text,reply.as_deref()).await?;
            }else{
                anyhow::bail!("unsupported QQ callback queued");
            }
            Ok(())
        }.await;
        match outcome {
            Ok(())=>{
                sqlx::query("DELETE FROM qq_events_inbox WHERE source_key=?")
                    .bind(&key).execute(&state.pool).await?;
                processed+=1;
            },
            Err(error)=>{
                sqlx::query("UPDATE qq_events_inbox SET attempts=attempts+1,
                    next_attempt_at=datetime('now',
                        '+' || MIN(3600,(attempts+1)*(attempts+1)*5) || ' seconds'),
                    last_error='callback delivery temporarily unavailable'
                    WHERE source_key=?")
                    .bind(&key).execute(&state.pool).await?;
                tracing::warn!(account=%account_id,%error,
                    "queued QQ callback was retained for later delivery");
            }
        }
    }
    Ok(processed)
}

/// Process encrypted QQ callbacks independently from account discovery.
pub async fn run_worker(state:Arc<AppState>){
    loop {
        if let Err(error)=drain_qq_own(&state,100).await {
            tracing::warn!(%error,"QQ self-message inbox drain failed");
        }
        if let Err(error)=drain_qq_events(&state,100).await {
            tracing::warn!(%error,"QQ callback inbox drain failed");
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

pub async fn queued_count(state:&Arc<AppState>)->Result<i64>{
    let total:i64=sqlx::query_scalar(
        "SELECT (SELECT COUNT(*) FROM qq_own_inbox) +
                (SELECT COUNT(*) FROM qq_events_inbox)")
        .fetch_one(&state.pool).await?;
    Ok(total)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn persist_duplicate_qq_webhooks_once_with_encrypted_payload(){
        let path=std::env::temp_dir().join(format!(
            "2bm-qq-inbox-{}.sqlite",uuid::Uuid::new_v4()));
        let pool=crate::db::connect(&format!("sqlite://{}",path.display()))
            .await.unwrap();
        sqlx::query("INSERT INTO accounts(id,platform,label,owner_mxid,credentials,enabled)
            VALUES ('qq','qq','QA','@owner:example.org','encrypted',1)")
            .execute(&pool).await.unwrap();
        let state=Arc::new(AppState{
            config:crate::config::Config{
                admin_listen:"127.0.0.1:9000".parse().unwrap(),
                matrix_listen:"127.0.0.1:9001".parse().unwrap(),
                combined_listen:None,
                database_url:String::new(),homeserver:String::new(),
                server_name:"example.org".to_owned(),
                as_token:String::new(),hs_token:String::new(),
                admin_token:String::new(),credential_key:[7;32],
            },
            pool,http:reqwest::Client::new(),
            telegram:crate::telegram_user::Gateway::default(),
            room_creation:tokio::sync::Mutex::new(()),
            delivery_lock:tokio::sync::Mutex::new(()),
            transaction_lock:tokio::sync::Mutex::new(()),
        });
        enqueue_qq_own(&state,"qq","private:123","15","top secret").await.unwrap();
        enqueue_qq_own(&state,"qq","private:123","15","top secret").await.unwrap();
        assert_eq!(queued_count(&state).await.unwrap(),1);
        let payload:String=sqlx::query_scalar(
            "SELECT sealed FROM qq_own_inbox WHERE account_id='qq'")
            .fetch_one(&state.pool).await.unwrap();
        assert!(!payload.contains("top secret"));
        let decoded=crypto::decrypt(&state.config.credential_key,&payload).unwrap();
        assert_eq!(decoded["text"],"top secret");
        // A corrupt record must remain for recovery, with a future retry
        // deadline so it doesn't create an uncontrolled hot loop.
        sqlx::query("UPDATE qq_own_inbox SET sealed='corrupted' WHERE account_id='qq'")
            .execute(&state.pool).await.unwrap();
        assert_eq!(drain_qq_own(&state,10).await.unwrap(),0);
        let delayed:i64=sqlx::query_scalar(
            "SELECT COUNT(*) FROM qq_own_inbox
             WHERE next_attempt_at > CURRENT_TIMESTAMP AND attempts=1")
            .fetch_one(&state.pool).await.unwrap();
        assert_eq!(delayed,1);
        assert_eq!(drain_qq_own(&state,10).await.unwrap(),0);

        // A disabled account must never cause its durable callback to be
        // ACKed and removed by the background worker.
        sqlx::query("UPDATE qq_own_inbox SET sealed=?,next_attempt_at='1970-01-01'
            WHERE account_id='qq'")
            .bind(&payload).execute(&state.pool).await.unwrap();
        sqlx::query("UPDATE accounts SET enabled=0 WHERE id='qq'")
            .execute(&state.pool).await.unwrap();
        assert_eq!(drain_qq_own(&state,10).await.unwrap(),0);
        assert_eq!(queued_count(&state).await.unwrap(),1);

        state.pool.close().await;
        drop(state);
        let _=std::fs::remove_file(path);
    }
}
