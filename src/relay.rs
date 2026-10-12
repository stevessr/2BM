use crate::{adapters, db, matrix, AppState};
use anyhow::Result;
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

/// Incoming messages appear under Matrix ghost identities, not as bot-prefixed text.
pub async fn incoming(
    state: &Arc<AppState>, account_id: &str, chat: &str, message_id: &str,
    sender_id: &str, sender: &str, text: &str
) -> Result<()> {
    if text.trim().is_empty() || sender_id.is_empty() { return Ok(()); }
    let Some(account) = db::account(&state.pool, account_id).await?
        .filter(|a| a.enabled) else { return Ok(()); };
    let source_key = format!("{account_id}:{chat}:{message_id}");
    if db::has_key(&state.pool,"incoming","source_key",&source_key).await? { return Ok(()); }
    // OneBot may deliver a recall before the corresponding message. Never
    // resurrect recalled content on a delayed retry.
    let recalled:Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM recall_tombstones WHERE account_id=? AND external_chat=? AND external_message=?"
    ).bind(account_id).bind(chat).bind(message_id)
        .fetch_optional(&state.pool).await?;
    if recalled.is_some() { return Ok(()); }
    let is_direct = chat.starts_with("private:") ||
        (account.platform.starts_with("telegram") && !chat.starts_with("-"));
    let descriptor = crate::virtual_rooms::RemoteChat::new(
        chat, if is_direct { sender } else { chat },
        is_direct, if is_direct { Some(sender_id.to_owned()) } else { None }
    );
    let mapping = crate::virtual_rooms::ensure(
        state, &account, &descriptor, Some((sender_id,sender))
    ).await?;
    let ghost = matrix::ghost_user_id(state,account_id,sender_id);
    let txn = Uuid::new_v5(&Uuid::NAMESPACE_URL,source_key.as_bytes()).to_string();
    let event_id=matrix::send_as(state,&mapping.matrix_room,text,&txn,&ghost).await?;
    sqlx::query("INSERT INTO event_links(
            account_id,external_chat,external_message,matrix_room,matrix_event,sender_mxid)
        VALUES (?,?,?,?,?,?)
        ON CONFLICT(account_id,external_chat,external_message) DO NOTHING")
        .bind(account_id).bind(chat).bind(message_id).bind(&mapping.matrix_room)
        .bind(&event_id).bind(&ghost).execute(&state.pool).await?;
    sqlx::query("INSERT OR IGNORE INTO incoming(source_key) VALUES (?)")
        .bind(source_key).execute(&state.pool).await?;
    // A recall can arrive while the Matrix send was in flight.
    let recalled:Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM recall_tombstones WHERE account_id=? AND external_chat=? AND external_message=?"
    ).bind(account_id).bind(chat).bind(message_id).fetch_optional(&state.pool).await?;
    if recalled.is_some() { recall(state,account_id,chat,message_id).await?; }
    Ok(())
}

/// OneBot recall events are idempotent and may arrive before their messages.
/// Unknown message IDs become tombstones, not errors or newly-created rooms.
pub async fn recall(
    state:&Arc<AppState>,account_id:&str,chat:&str,message_id:&str
)->Result<()>{
    sqlx::query("INSERT OR IGNORE INTO recall_tombstones(
        account_id,external_chat,external_message) VALUES (?,?,?)")
        .bind(account_id).bind(chat).bind(message_id).execute(&state.pool).await?;
    let entry:Option<(String,String,String,i64)>=sqlx::query_as(
        "SELECT matrix_room,matrix_event,sender_mxid,redacted
         FROM event_links WHERE account_id=? AND external_chat=? AND external_message=?")
        .bind(account_id).bind(chat).bind(message_id)
        .fetch_optional(&state.pool).await?;
    let Some((room,event,sender,redacted))=entry else { return Ok(()); };
    if redacted!=0 { return Ok(()); }
    let txn=Uuid::new_v5(&Uuid::NAMESPACE_URL,
        format!("recall:{account_id}:{chat}:{message_id}").as_bytes()).to_string();
    matrix::redact_as(state,&room,&event,&txn,&sender).await?;
    sqlx::query("UPDATE event_links SET redacted=1
        WHERE account_id=? AND external_chat=? AND external_message=?")
        .bind(account_id).bind(chat).bind(message_id)
        .execute(&state.pool).await?;
    Ok(())
}

pub async fn matrix_event(state: &Arc<AppState>, event: &Value) -> Result<()> {
    let kind = event["type"].as_str().unwrap_or_default();
    if kind == "m.room.member" {
        if event["state_key"] == state.config.bot_user() && event["content"]["membership"] == "invite" {
            if let Some(room) = event["room_id"].as_str() { matrix::join_bot(state,room).await?; }
        }
        return Ok(());
    }
    if kind != "m.room.message" { return Ok(()); }
    let Some(sender) = event["sender"].as_str() else { return Ok(()); };
    if sender == state.config.bot_user() || sender.starts_with("@_2bm_p_") { return Ok(()); }
    if !matches!(event["content"]["msgtype"].as_str(),Some("m.text"|"m.emote")) { return Ok(()); }
    let Some(room) = event["room_id"].as_str() else { return Ok(()); };
    let Some(event_id) = event["event_id"].as_str() else { return Ok(()); };
    let Some(body) = event["content"]["body"].as_str().filter(|s| !s.trim().is_empty()) else { return Ok(()); };
    for mapping in db::mappings_for_room(&state.pool,room).await? {
        let key = format!("{event_id}:{}",mapping.id);
        if db::has_key(&state.pool,"outgoing","event_key",&key).await? { continue; }
        let Some(account) = db::account(&state.pool,&mapping.account_id).await? else { continue; };
        // A joined stranger cannot operate a foreign user's account.
        if !account.enabled || sender != account.owner_mxid { continue; }
        adapters::send(state,&account,&mapping.external_chat,body).await?;
        sqlx::query("INSERT OR IGNORE INTO outgoing(event_key) VALUES (?)")
            .bind(key).execute(&state.pool).await?;
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::{Query,State},routing::put,Json,Router};
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize,Ordering};

    async fn harness() -> (Arc<AppState>,Arc<AtomicUsize>,tokio::task::JoinHandle<()>,std::path::PathBuf) {
        async fn redact(
            State(calls):State<Arc<AtomicUsize>>,
            Query(params):Query<std::collections::HashMap<String,String>>,
        )->Json<Value> {
            assert_eq!(params.get("user_id").map(String::as_str),Some("@_2bm_p_sender:example.org"));
            calls.fetch_add(1,Ordering::SeqCst);
            Json(json!({"event_id":"$redacted:example.org"}))
        }
        let counter=Arc::new(AtomicUsize::new(0));
        let router=Router::new().route(
            "/_matrix/client/v3/rooms/{room}/redact/{event}/{txn}",put(redact)
        ).with_state(counter.clone());
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url=format!("http://{}",listener.local_addr().unwrap());
        let task=tokio::spawn(async move {axum::serve(listener,router).await.unwrap();});
        let path=std::env::temp_dir().join(format!("2bm-recall-{}.sqlite",Uuid::new_v4()));
        let pool=crate::db::connect(&format!("sqlite://{}",path.display())).await.unwrap();
        sqlx::query("INSERT INTO accounts(id,platform,label,owner_mxid,credentials,enabled)
            VALUES ('acct','qq','qq','@owner:example.org','encrypted',1)")
            .execute(&pool).await.unwrap();
        let state=Arc::new(AppState {
            config:crate::config::Config {
                admin_listen:"127.0.0.1:9000".parse().unwrap(),
                matrix_listen:"127.0.0.1:9001".parse().unwrap(),
                combined_listen:None,
                database_url:format!("sqlite://{}",path.display()),
                homeserver:url,server_name:"example.org".into(),
                as_token:"test-as".into(),hs_token:"test-hs".into(),
                admin_token:"test-admin".into(),credential_key:[0u8;32],
            },
            pool,
            http:reqwest::Client::new(),
            telegram:crate::telegram_user::Gateway::default(),
            room_creation:tokio::sync::Mutex::new(()),
        });
        (state,counter,task,path)
    }

    #[tokio::test]
    async fn recall_is_idempotent_and_stops_late_message_replay() {
        let (state,calls,server,path)=harness().await;
        sqlx::query("INSERT INTO event_links
            (account_id,external_chat,external_message,matrix_room,matrix_event,sender_mxid)
            VALUES ('acct','group:42','123','!room:example.org','$event:example.org','@_2bm_p_sender:example.org')")
            .execute(&state.pool).await.unwrap();

        recall(&state,"acct","group:42","123").await.unwrap();
        recall(&state,"acct","group:42","123").await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst),1);
        let redacted:i64=sqlx::query_scalar(
            "SELECT redacted FROM event_links WHERE account_id='acct' AND external_message='123'"
        ).fetch_one(&state.pool).await.unwrap();
        assert_eq!(redacted,1);

        // OneBot delivered the recall before the corresponding message.
        recall(&state,"acct","private:77","200").await.unwrap();
        incoming(&state,"acct","private:77","200","77","Friend","content that must stay removed")
            .await.unwrap();
        let rooms:i64=sqlx::query_scalar("SELECT COUNT(*) FROM virtual_chats")
            .fetch_one(&state.pool).await.unwrap();
        assert_eq!(rooms,0);
        assert_eq!(calls.load(Ordering::SeqCst),1);

        state.pool.close().await;
        server.abort();
        drop(state);
        let _=std::fs::remove_file(path);
    }
}
