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
    incoming_with_reply(state,account_id,chat,message_id,
        sender_id,sender,text,None).await
}
pub async fn incoming_with_reply(
    state: &Arc<AppState>, account_id: &str, chat: &str, message_id: &str,
    sender_id: &str, sender: &str, text: &str, reply_to:Option<&str>
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
    // Matrix-originated messages are already in the room as their owner.
    // Do not emit a second ghost event when NapCat/MTProto echoes them.
    let already_linked:Option<i64>=sqlx::query_scalar(
        "SELECT 1 FROM event_links WHERE account_id=? AND external_chat=? AND external_message=?")
        .bind(account_id).bind(chat).bind(message_id).fetch_optional(&state.pool).await?;
    if already_linked.is_some() { return Ok(()); }
    let is_direct = chat.starts_with("private:") ||
        (account.platform.starts_with("telegram") && !chat.starts_with("-"));
    let descriptor = crate::virtual_rooms::RemoteChat::new(
        chat, if is_direct { sender } else { chat },
        is_direct, if is_direct {
            Some(chat.strip_prefix("private:")
                .unwrap_or_else(||if account.platform.starts_with("telegram") {chat}else{sender_id})
                .to_owned())
        } else { None }
    );
    let mapping = crate::virtual_rooms::ensure(
        state, &account, &descriptor, Some((sender_id,sender))
    ).await?;
    let ghost = matrix::ghost_user_id(state,account_id,sender_id);
    let txn = Uuid::new_v5(&Uuid::NAMESPACE_URL,source_key.as_bytes()).to_string();
    let matrix_reply=if let Some(remote_reply)=reply_to {
        sqlx::query_scalar::<_,String>(
            "SELECT matrix_event FROM event_links
             WHERE account_id=? AND external_chat=? AND external_message=?
               AND redacted=0 LIMIT 1")
            .bind(account_id).bind(chat).bind(remote_reply)
            .fetch_optional(&state.pool).await?
    } else {None};
    let event_id=matrix::send_as_reply(state,&mapping.matrix_room,text,
        &txn,&ghost,matrix_reply.as_deref()).await?;
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

/// For outgoing messages observed on another device, serialize against
/// Matrix -> platform sends so the message-ID mapping is committed first.
pub async fn incoming_own(
    state:&Arc<AppState>,account_id:&str,chat:&str,message_id:&str,
    sender_id:&str,sender:&str,text:&str
)->Result<()>{
    let _delivery=state.delivery_lock.lock().await;
    incoming(state,account_id,chat,message_id,sender_id,sender,text).await
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

/// Matrix clients prepend quoted text as a plaintext fallback for replies.
/// This quote is not part of the new user's actual message.
fn strip_reply_fallback<'a>(text:&'a str,is_reply:bool)->&'a str {
    if !is_reply || !text.starts_with("> ") {return text;}
    match text.split_once("\n\n") {
        Some((quote,body)) if quote.lines().all(|line|line.starts_with("> ")) && !body.is_empty()
            => body,
        _ => text,
    }
}
pub async fn matrix_event(state: &Arc<AppState>, event: &Value) -> Result<()> {
    let kind = event["type"].as_str().unwrap_or_default();
    if matches!(kind,"m.room.encrypted"|"m.room.encryption") {
        if let Some(room)=event["room_id"].as_str() {
            if !db::mappings_for_room(&state.pool,room).await?.is_empty() {
                sqlx::query("INSERT INTO room_warnings(matrix_room,reason)
                    VALUES (?,'e2ee_unsupported')
                    ON CONFLICT(matrix_room) DO UPDATE SET
                        event_count=room_warnings.event_count+1,
                        last_seen=CURRENT_TIMESTAMP")
                    .bind(room).execute(&state.pool).await?;
                tracing::error!(room,"encrypted Matrix bridge room is unsupported; message not relayed");
            }
        }
        return Ok(());
    }
    if kind == "m.room.member" {
        if let (Some(room),Some(member))=(
            event["room_id"].as_str(),event["state_key"].as_str()
        ) {
            if member.starts_with("@_2bm_p_") {
                if matches!(event["content"]["membership"].as_str(),Some("leave"|"ban")) {
                    sqlx::query("DELETE FROM ghost_room_memberships
                        WHERE matrix_room=? AND ghost_mxid=?")
                        .bind(room).bind(member).execute(&state.pool).await?;
                } else if event["content"]["membership"]=="join" {
                    sqlx::query("INSERT OR IGNORE INTO ghost_room_memberships(matrix_room,ghost_mxid)
                        VALUES (?,?)").bind(room).bind(member).execute(&state.pool).await?;
                }
            }
        }
        if event["state_key"] == state.config.bot_user() && event["content"]["membership"] == "invite" {
            if let Some(room) = event["room_id"].as_str() { matrix::join_bot(state,room).await?; }
        }
        return Ok(());
    }
    if kind != "m.room.message" { return Ok(()); }
    if let Some(room)=event["room_id"].as_str() {
        let blocked:Option<i64>=sqlx::query_scalar(
            "SELECT 1 FROM room_warnings WHERE matrix_room=? AND reason='e2ee_unsupported'"
        ).bind(room).fetch_optional(&state.pool).await?;
        if blocked.is_some() {
            // Never attempt to forward plaintext from a room flagged encrypted.
            return Ok(());
        }
    }
    let Some(sender) = event["sender"].as_str() else { return Ok(()); };
    if sender == state.config.bot_user() || sender.starts_with("@_2bm_p_") { return Ok(()); }
    if !matches!(event["content"]["msgtype"].as_str(),Some("m.text"|"m.emote")) { return Ok(()); }
    let Some(room) = event["room_id"].as_str() else { return Ok(()); };
    let Some(event_id) = event["event_id"].as_str() else { return Ok(()); };
    // Matrix edits use m.replace + m.new_content; forwarding them as a new
    // message would silently duplicate the old message on the source platform.
    if event["content"]["m.relates_to"]["rel_type"] == "m.replace" {
        return Ok(());
    }
    let Some(raw_body)=event["content"]["body"].as_str().filter(|s|!s.trim().is_empty()) else {
        return Ok(());
    };
    let reply_event=event["content"]["m.relates_to"]["m.in_reply_to"]["event_id"].as_str();
    let body=strip_reply_fallback(raw_body,reply_event.is_some());
    for mapping in db::mappings_for_room(&state.pool,room).await? {
        let _delivery=state.delivery_lock.lock().await;
        let key = format!("{event_id}:{}",mapping.id);
        if db::has_key(&state.pool,"outgoing","event_key",&key).await? { continue; }
        let Some(account) = db::account(&state.pool,&mapping.account_id).await? else { continue; };
        // A joined stranger cannot operate a foreign user's account.
        if !account.enabled || sender != account.owner_mxid { continue; }
        crate::delivery::begin(state,&key,&account.id,event_id,&mapping.external_chat).await?;
        // Use a native QQ reply segment only when the quoted Matrix event
        // was actually bridged from THIS account and THIS external chat.
        let remote_reply=if let Some(reply_event)=reply_event {
            sqlx::query_scalar::<_,String>(
                "SELECT external_message FROM event_links
                 WHERE account_id=? AND external_chat=? AND matrix_room=?
                   AND matrix_event=? LIMIT 1")
                .bind(&account.id).bind(&mapping.external_chat)
                .bind(room).bind(reply_event)
                .fetch_optional(&state.pool).await?
        } else {None};
        let outcome:Result<Option<String>>=async {
            Ok(match account.platform.as_str() {
                "qq" => {
                    let secrets=adapters::credentials(state,&account)?;
                    crate::adapters::qq::send_reply_id(
                        state,&secrets,&mapping.external_chat,body,
                        remote_reply.as_deref()).await?
                }
                "telegram_user" => Some(crate::telegram_user::send_with_id(
                    state,&account.id,&mapping.external_chat,body).await?.to_string()),
                _ => {
                    adapters::send(state,&account,&mapping.external_chat,body).await?;
                    None
                }
            })
        }.await;
        match outcome {
            Ok(remote_sent)=>{
                crate::delivery::complete(state,&key,&account.id,
                    &mapping.external_chat,remote_sent.as_deref(),
                    room,event_id,sender).await?;
            }
            Err(error)=>{
                crate::delivery::uncertain(state,&key,
                    "External send failed or timed out; check remote delivery before retry").await?;
                return Err(error);
            }
        }

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
            delivery_lock:tokio::sync::Mutex::new(()),
        });
        (state,counter,task,path)
    }

    #[tokio::test]
    async fn matrix_qq_reply_uses_original_remote_id_and_is_idempotent() {
        use axum::{extract::State,routing::post};
        use std::sync::Mutex;

        async fn send(
            State(messages):State<Arc<Mutex<Vec<Value>>>>,Json(payload):Json<Value>
        )->Json<Value> {
            messages.lock().unwrap().push(payload);
            Json(json!({"status":"ok","retcode":0,"data":{"message_id":200}}))
        }
        let messages=Arc::new(Mutex::new(Vec::<Value>::new()));
        let router=Router::new()
            .route("/send_group_msg",post(send))
            .with_state(messages.clone());
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url=format!("http://{}",listener.local_addr().unwrap());
        let server=tokio::spawn(async move {axum::serve(listener,router).await.unwrap();});

        let (state,_,redaction_server,path)=harness().await;
        let credentials=crate::crypto::encrypt(&state.config.credential_key,
            &json!({"api_base":url,"access_token":"local-test"})).unwrap();
        sqlx::query("UPDATE accounts SET credentials=? WHERE id='acct'")
            .bind(credentials).execute(&state.pool).await.unwrap();
        sqlx::query("INSERT INTO mappings(account_id,external_chat,matrix_room)
            VALUES ('acct','group:42','!room:example.org')")
            .execute(&state.pool).await.unwrap();
        sqlx::query("INSERT INTO event_links
            (account_id,external_chat,external_message,matrix_room,matrix_event,sender_mxid)
            VALUES ('acct','group:42','123','!room:example.org','$quoted:example.org','@_2bm_p_sender:example.org')")
            .execute(&state.pool).await.unwrap();
        let event=json!({
            "type":"m.room.message","event_id":"$new:example.org",
            "sender":"@owner:example.org","room_id":"!room:example.org",
            "content":{
                "msgtype":"m.text",
                "body":"> <ghost> quoted text\n\nresponse",
                "m.relates_to":{"m.in_reply_to":{"event_id":"$quoted:example.org"}}
            }
        });
        matrix_event(&state,&event).await.unwrap();
        matrix_event(&state,&event).await.unwrap();
        let received=messages.lock().unwrap();
        assert_eq!(received.len(),1,"Matrix transaction replay must not re-send");
        assert_eq!(received[0]["message"][0]["type"],"reply");
        assert_eq!(received[0]["message"][0]["data"]["id"],"123");
        assert_eq!(received[0]["message"][1]["data"]["text"],"response");
        drop(received);

        let edited=json!({
            "type":"m.room.message","event_id":"$edit:example.org",
            "sender":"@owner:example.org","room_id":"!room:example.org",
            "content":{"msgtype":"m.text","body":"new content",
                "m.relates_to":{"rel_type":"m.replace","event_id":"$new:example.org"}}
        });
        matrix_event(&state,&edited).await.unwrap();
        assert_eq!(messages.lock().unwrap().len(),1);
        state.pool.close().await;
        redaction_server.abort();
        server.abort();
        drop(state);
        let _=std::fs::remove_file(path);
    }

    #[test]
    fn reply_fallback_is_removed_without_harming_regular_quotes() {
        assert_eq!(strip_reply_fallback("> <alice> first\n> quoted line\n\nsecond",true),"second");
        assert_eq!(strip_reply_fallback("> normal markdown\n\nparagraph",false),
            "> normal markdown\n\nparagraph");
        assert_eq!(strip_reply_fallback("plain",true),"plain");
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
