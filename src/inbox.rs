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
         ORDER BY received_at,source_key LIMIT ?")
        .bind(limit.clamp(1,100)).fetch_all(&state.pool).await?;
    let mut success=0;
    for (key,account,sealed) in items {
        // Do not lose a queued message if the decryption key is misconfigured;
        // the record stays in SQLite for operator-controlled recovery.
        let result:Result<()> = async {
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
                    last_error='delivery temporarily unavailable' WHERE source_key=?")
                    .bind(&key).execute(&state.pool).await?;
                tracing::warn!(account=%account,%err,
                    "queued QQ webhook could not be delivered; retained for retry");
                // Backoff: stop at the first failure to avoid costly repeated
                // Matrix calls on every event in a disconnected account.
                break;
            }
        }
    }
    Ok(success)
}

pub async fn queued_count(state:&Arc<AppState>)->Result<i64>{
    Ok(sqlx::query_scalar("SELECT COUNT(*) FROM qq_own_inbox")
        .fetch_one(&state.pool).await?)
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
        state.pool.close().await;
        drop(state);
        let _=std::fs::remove_file(path);
    }
}
