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
