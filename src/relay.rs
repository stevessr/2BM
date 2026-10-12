use crate::{adapters, db, matrix, AppState};
use anyhow::Result;
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

pub async fn incoming(
    state: &Arc<AppState>,
    account_id: &str,
    chat: &str,
    message_id: &str,
    sender: &str,
    text: &str,
) -> Result<()> {
    if text.trim().is_empty() { return Ok(()); }
    let Some(mapping) = db::mapping_for_chat(&state.pool, account_id, chat).await? else {
        return Ok(());
    };
    let source_key = format!("{account_id}:{chat}:{message_id}");
    if db::has_key(&state.pool, "incoming", "source_key", &source_key).await? {
        return Ok(());
    }
    let account = db::account(&state.pool, account_id).await?;
    let Some(account) = account.filter(|a| a.enabled) else { return Ok(()); };
    let body = format!("[{}] {}: {}", account.platform, sender, text);
    // Deterministic Matrix transaction ids protect against provider retries.
    let txn = Uuid::new_v5(&Uuid::NAMESPACE_URL, source_key.as_bytes()).to_string();
    matrix::send_text(state, &mapping.matrix_room, &body, &txn).await?;
    sqlx::query("INSERT OR IGNORE INTO incoming(source_key) VALUES (?)")
        .bind(source_key).execute(&state.pool).await?;
    Ok(())
}

pub async fn matrix_event(state: &Arc<AppState>, event: &Value) -> Result<()> {
    let kind = event["type"].as_str().unwrap_or_default();
    if kind == "m.room.member" {
        if event["state_key"] == state.config.bot_user() && event["content"]["membership"] == "invite" {
            if let Some(room) = event["room_id"].as_str() {
                matrix::join_bot(state, room).await?;
            }
        }
        return Ok(());
    }
    if kind != "m.room.message" { return Ok(()); }
    let sender = event["sender"].as_str().unwrap_or_default();
    if sender == state.config.bot_user() || sender.starts_with("@_2bm_") {
        return Ok(());
    }
    let msgtype = event["content"]["msgtype"].as_str().unwrap_or_default();
    if !matches!(msgtype, "m.text" | "m.notice" | "m.emote") { return Ok(()); }
    let Some(room) = event["room_id"].as_str() else { return Ok(()); };
    let Some(event_id) = event["event_id"].as_str() else { return Ok(()); };
    let Some(body) = event["content"]["body"].as_str() else { return Ok(()); };
    if body.trim().is_empty() { return Ok(()); }
    let mappings = db::mappings_for_room(&state.pool, room).await?;
    for mapping in mappings {
        let key = format!("{event_id}:{}", mapping.id);
        if db::has_key(&state.pool, "outgoing", "event_key", &key).await? { continue; }
        let Some(account) = db::account(&state.pool, &mapping.account_id).await? else { continue; };
        if !account.enabled { continue; }
        let text = format!("[Matrix] {sender}: {body}");
        adapters::send(state, &account, &mapping.external_chat, &text).await?;
        sqlx::query("INSERT OR IGNORE INTO outgoing(event_key) VALUES (?)")
            .bind(key).execute(&state.pool).await?;
    }
    Ok(())
}
