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
        let result:Result<()> = tokio::time::timeout(
            std::time::Duration::from_secs(20), async {
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
        }).await.unwrap_or_else(|_|Err(anyhow::anyhow!(
            "QQ self-message bridge attempt timed out")));
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
        let outcome:Result<()> = tokio::time::timeout(
            std::time::Duration::from_secs(20), async {
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
        }).await.unwrap_or_else(|_|Err(anyhow::anyhow!(
            "QQ callback bridge attempt timed out")));
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


/// Write MTProto updates before confirming/persisting update-stream state.
/// Keys are stable per Telegram dialog/message and survive process restarts.
pub async fn enqueue_telegram(
    state:&Arc<AppState>,account:&str,chat:&str,message_id:&str,
    sender_id:&str,sender:&str,text:&str,outgoing:bool,reply_to:Option<&str>
)->Result<()>{
    let key=format!("telegram:{account}:{chat}:{message_id}");
    let payload=json!({
        "chat":chat,"message_id":message_id,"sender_id":sender_id,
        "sender":sender,"text":text,"outgoing":outgoing,"reply_to":reply_to
    });
    let sealed=crypto::encrypt(&state.config.credential_key,&payload)?;
    sqlx::query("INSERT OR IGNORE INTO telegram_events_inbox
        (source_key,account_id,sealed) VALUES (?,?,?)")
        .bind(&key).bind(account).bind(sealed).execute(&state.pool).await?;
    Ok(())
}

pub async fn drain_telegram(state:&Arc<AppState>,limit:i64)->Result<usize>{
    let items:Vec<(String,String,String)>=sqlx::query_as(
        "SELECT source_key,account_id,sealed FROM telegram_events_inbox
         WHERE next_attempt_at <= CURRENT_TIMESTAMP
         ORDER BY next_attempt_at,received_at,source_key LIMIT ?")
        .bind(limit.clamp(1,100)).fetch_all(&state.pool).await?;
    let mut delivered=0;
    for (key,account_id,sealed) in items {
        let outcome:Result<()> = tokio::time::timeout(
            std::time::Duration::from_secs(20), async {
            let active=crate::db::account(&state.pool,&account_id).await?
                .is_some_and(|a|a.enabled && a.platform=="telegram_user");
            if !active {
                anyhow::bail!("Telegram account not active; queue retained");
            }
            let payload:Value=crypto::decrypt(&state.config.credential_key,&sealed)
                .context("Telegram queued message decryption failed")?;
            let chat=payload["chat"].as_str().context("queued chat missing")?;
            let id=payload["message_id"].as_str().context("queued ID missing")?;
            let sender_id=payload["sender_id"].as_str()
                .context("queued sender ID missing")?;
            let sender=payload["sender"].as_str().unwrap_or("Telegram user");
            let text=payload["text"].as_str().context("queued text missing")?;
            if payload["outgoing"]==true {
                relay::incoming_own(state,&account_id,chat,id,sender_id,sender,text)
                    .await?;
            }else{
                relay::incoming_with_reply(
                    state,&account_id,chat,id,sender_id,sender,text,
                    payload["reply_to"].as_str()).await?;
            }
            Ok(())
        }).await.unwrap_or_else(|_|Err(anyhow::anyhow!(
            "Telegram inbound bridge attempt timed out")));
        match outcome {
            Ok(())=>{
                sqlx::query("DELETE FROM telegram_events_inbox WHERE source_key=?")
                    .bind(&key).execute(&state.pool).await?;
                delivered+=1;
            }
            Err(error)=>{
                sqlx::query("UPDATE telegram_events_inbox SET attempts=attempts+1,
                    next_attempt_at=datetime('now',
                        '+' || MIN(3600,(attempts+1)*(attempts+1)*5) || ' seconds'),
                    last_error='MTProto bridge delivery temporarily unavailable'
                    WHERE source_key=?")
                    .bind(&key).execute(&state.pool).await?;
                tracing::warn!(account=%account_id,%error,
                    "MTProto update retained in durable encrypted queue");
            }
        }
    }
    Ok(delivered)
}

pub async fn telegram_pending_count(state:&Arc<AppState>)->Result<i64>{
    Ok(sqlx::query_scalar("SELECT COUNT(*) FROM telegram_events_inbox")
        .fetch_one(&state.pool).await?)
}

/// Process encrypted QQ callbacks independently from account discovery.
pub async fn run_worker(state:Arc<AppState>){
    // Keep independent consumer futures. A slow or disconnected upstream
    // must not prevent another platform's encrypted inbox from draining.
    let own_state=state.clone();
    let qq_state=state.clone();
    let own_loop=async move {
        loop {
            if let Err(error)=drain_qq_own(&own_state,20).await {
                tracing::warn!(%error,"QQ self-message inbox drain failed");
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    };
    let qq_loop=async move {
        loop {
            if let Err(error)=drain_qq_events(&qq_state,20).await {
                tracing::warn!(%error,"QQ callback inbox drain failed");
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    };
    let tg_loop=async move {
        loop {
            if let Err(error)=drain_telegram(&state,20).await {
                tracing::warn!(%error,"Telegram update inbox drain failed");
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    };
    let _=tokio::join!(own_loop,qq_loop,tg_loop);
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
    async fn telegram_durable_inbox_delivers_once_to_matrix(){
        use axum::{extract::State,http::StatusCode,routing::{get,post,put},
            Json,Router};
        use std::sync::atomic::{AtomicUsize,Ordering};
        use serde_json::json;
        async fn register()->Json<Value> {
            Json(json!({"user_id":"@_2bm_p_test:example.org"}))
        }
        async fn ok()->Json<Value> {Json(json!({}))}
        async fn no_encryption()->StatusCode {StatusCode::NOT_FOUND}
        async fn send_message(State(sent):State<Arc<AtomicUsize>>)->Json<Value>{
            sent.fetch_add(1,Ordering::SeqCst);
            Json(json!({"event_id":"$durable:example.org"}))
        }
        let sent=Arc::new(AtomicUsize::new(0));
        let router=Router::new()
            .route("/_matrix/client/v3/register",post(register))
            .route("/_matrix/client/v3/profile/{user}/displayname",put(ok))
            .route("/_matrix/client/v3/join/{room}",post(ok))
            .route("/_matrix/client/v3/rooms/{room}/state/m.room.encryption",
                get(no_encryption))
            .route("/_matrix/client/v3/rooms/{room}/send/m.room.message/{txn}",
                put(send_message))
            .with_state(sent.clone());
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url=format!("http://{}",listener.local_addr().unwrap());
        let server=tokio::spawn(async move{
            axum::serve(listener,router).await.unwrap();
        });
        let path=std::env::temp_dir().join(format!(
            "2bm-telegram-inbox-integration-{}.sqlite",uuid::Uuid::new_v4()));
        let pool=crate::db::connect(&format!("sqlite://{}",path.display()))
            .await.unwrap();
        sqlx::query("INSERT INTO accounts
            (id,platform,label,owner_mxid,credentials,enabled)
            VALUES ('t','telegram_user','TG','@owner:example.org','sealed',1)")
            .execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO mappings(account_id,external_chat,matrix_room)
            VALUES ('t','123','!room:example.org')")
            .execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO virtual_chats(account_id,external_chat,title,direct)
            VALUES ('t','123','Contact',1)")
            .execute(&pool).await.unwrap();
        let state=Arc::new(AppState{
            config:crate::config::Config{
                admin_listen:"127.0.0.1:9000".parse().unwrap(),
                matrix_listen:"127.0.0.1:9001".parse().unwrap(),
                combined_listen:None,database_url:String::new(),
                homeserver:url,server_name:"example.org".into(),
                as_token:"appservice".into(),hs_token:"homeserver".into(),
                admin_token:"administrator".into(),credential_key:[9;32],
            },
            pool,http:reqwest::Client::new(),
            telegram:crate::telegram_user::Gateway::default(),
            room_creation:tokio::sync::Mutex::new(()),
            delivery_lock:tokio::sync::Mutex::new(()),
            transaction_lock:tokio::sync::Mutex::new(()),
        });
        enqueue_telegram(&state,"t","123","15","123","Contact",
            "hello from MTProto",false,None).await.unwrap();
        assert_eq!(drain_telegram(&state,10).await.unwrap(),1);
        assert_eq!(sent.load(Ordering::SeqCst),1);
        assert_eq!(telegram_pending_count(&state).await.unwrap(),0);
        // A replayed MTProto update must dequeue cleanly without a second
        // Matrix send (persistent source-key dedup survives restart).
        enqueue_telegram(&state,"t","123","15","123","Contact",
            "hello from MTProto",false,None).await.unwrap();
        assert_eq!(drain_telegram(&state,10).await.unwrap(),1);
        assert_eq!(sent.load(Ordering::SeqCst),1);
        server.abort();
        state.pool.close().await;
        drop(state);
        let _=std::fs::remove_file(path);
    }

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

        // Incoming (non-self) messages are also protected by an encrypted,
        // crash-resilient queue and duplicate OneBot callbacks are harmless.
        let normal=json!({"post_type":"message","message_type":"private",
            "user_id":123,"message_id":77,
            "raw_message":"private inbound event",
            "sender":{"nickname":"Contact"}});
        enqueue_qq_event(&state,"qq","qq:message:private:123:77",&normal)
            .await.unwrap();
        enqueue_qq_event(&state,"qq","qq:message:private:123:77",&normal)
            .await.unwrap();
        let (sealed,count):(String,i64)=sqlx::query_as(
            "SELECT sealed,(SELECT COUNT(*) FROM qq_events_inbox)
             FROM qq_events_inbox LIMIT 1")
            .fetch_one(&state.pool).await.unwrap();
        assert_eq!(count,1);
        assert!(!sealed.contains("private inbound event"));
        assert_eq!(crypto::decrypt(&state.config.credential_key,&sealed)
            .unwrap()["raw_message"],"private inbound event");
        // A disabled account is deliberately not processed as success.
        assert_eq!(drain_qq_events(&state,20).await.unwrap(),0);
        assert_eq!(queued_count(&state).await.unwrap(),2);
        let retries:i64=sqlx::query_scalar(
            "SELECT attempts FROM qq_events_inbox LIMIT 1")
            .fetch_one(&state.pool).await.unwrap();
        assert_eq!(retries,1);

        // MTProto updates are independently deduplicated and encrypted even
        // when the account is temporarily disabled.
        sqlx::query("INSERT INTO accounts(id,platform,label,owner_mxid,credentials,enabled)
            VALUES ('tg','telegram_user','TG','@owner:example.org','encrypted',0)")
            .execute(&state.pool).await.unwrap();
        enqueue_telegram(&state,"tg","123","99","123","Contact",
            "private Telegram message",false,Some("98")).await.unwrap();
        enqueue_telegram(&state,"tg","123","99","123","Contact",
            "private Telegram message",false,Some("98")).await.unwrap();
        assert_eq!(telegram_pending_count(&state).await.unwrap(),1);
        let telegram_payload:String=sqlx::query_scalar(
            "SELECT sealed FROM telegram_events_inbox WHERE account_id='tg'")
            .fetch_one(&state.pool).await.unwrap();
        assert!(!telegram_payload.contains("private Telegram message"));
        let decrypted=crypto::decrypt(&state.config.credential_key,
            &telegram_payload).unwrap();
        assert_eq!(decrypted["reply_to"],"98");
        assert_eq!(decrypted["text"],"private Telegram message");
        assert_eq!(drain_telegram(&state,10).await.unwrap(),0);
        assert_eq!(telegram_pending_count(&state).await.unwrap(),1);

        state.pool.close().await;
        drop(state);
        let _=std::fs::remove_file(path);
    }
}
