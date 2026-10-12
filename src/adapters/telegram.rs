use crate::{adapters::token, relay, AppState};
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use sqlx::Row;
use std::sync::Arc;

pub async fn send(state: &Arc<AppState>, credentials: &Value, chat: &str, text: &str) -> Result<()> {
    let bot_token = token(credentials, "token")?;
    let chat_id: i64 = chat.parse().context("Telegram chat id must be an integer")?;
    let response = state.http
        .post(format!("https://api.telegram.org/bot{bot_token}/sendMessage"))
        .json(&json!({"chat_id": chat_id, "text": text}))
        .send().await?
        .error_for_status()?;
    let result: Value = response.json().await?;
    if result["ok"] != true { bail!("Telegram rejected sendMessage"); }
    Ok(())
}

fn remote_message_id(message:&Value,update_id:i64)->String {
    message["message_id"].as_i64().unwrap_or(update_id).to_string()
}
pub async fn poll(state: &Arc<AppState>, account_id: &str, credentials: &Value) -> Result<()> {
    let bot_token = token(credentials, "token")?;
    let offset = sqlx::query("SELECT next_offset FROM telegram_offsets WHERE account_id = ?")
        .bind(account_id)
        .fetch_optional(&state.pool).await?
        .map(|row| row.get::<i64, _>(0))
        .unwrap_or(0);
    let response = state.http
        .get(format!("https://api.telegram.org/bot{bot_token}/getUpdates"))
        .query(&[("offset", offset.to_string()), ("timeout", "25".into()),
                  ("allowed_updates", r#"["message"]"#.into())])
        .send().await?
        .error_for_status()?;
    let body: Value = response.json().await?;
    if body["ok"] != true { bail!("Telegram rejected getUpdates"); }
    if let Some(updates) = body["result"].as_array() {
        for update in updates {
            let Some(update_id) = update["update_id"].as_i64() else { continue; };
            let message = &update["message"];
            if message.is_object() && message["from"]["is_bot"] != true {
                if let Some(chat) = message["chat"]["id"].as_i64() {
                    let text = message["text"].as_str()
                        .or_else(|| message["caption"].as_str())
                        .unwrap_or_default();
                    let sender = message["from"]["username"].as_str()
                        .or_else(|| message["from"]["first_name"].as_str())
                        .unwrap_or("Telegram user");
                    let sender_id = message["from"]["id"].as_i64()
                        .map(|id| id.to_string()).unwrap_or_default();
                    relay::incoming(state, account_id, &chat.to_string(),
                        &remote_message_id(message,update_id), &sender_id, sender, text).await?;
                }
            }
            sqlx::query(
                "INSERT INTO telegram_offsets(account_id,next_offset) VALUES (?,?)
                 ON CONFLICT(account_id) DO UPDATE SET next_offset = excluded.next_offset"
            )
            .bind(account_id).bind(update_id + 1).execute(&state.pool).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn remote_message_id_tracks_original_message_not_update_sequence(){
        assert_eq!(remote_message_id(&json!({"message_id":23}),1000),"23");
        assert_eq!(remote_message_id(&json!({}),1000),"1000");
    }
}
