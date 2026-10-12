use crate::{adapters::token, AppState};
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::sync::Arc;

pub async fn send(state: &Arc<AppState>, credentials: &Value, chat: &str, text: &str) -> Result<()> {
    let api_base = token(credentials, "api_base")?.trim_end_matches('/');
    let (kind, id) = chat.split_once(':')
        .context("QQ chat must use group:<id> or private:<id>")?;
    let numeric_id: i64 = id.parse().context("QQ target id must be an integer")?;
    let (action, request) = match kind {
        "group" => ("send_group_msg", json!({
            "group_id": numeric_id,
            "message": [{"type": "text", "data": {"text": text}}]
        })),
        "private" => ("send_private_msg", json!({
            "user_id": numeric_id,
            "message": [{"type": "text", "data": {"text": text}}]
        })),
        _ => bail!("QQ chat must be group:<id> or private:<id>"),
    };
    let response = state.http
        .post(format!("{api_base}/{action}"))
        .bearer_auth(token(credentials, "access_token")?)
        .json(&request)
        .send().await?
        .error_for_status()?;
    let result: Value = response.json().await?;
    if result["status"] != "ok" && result["retcode"] != 0 {
        bail!("OneBot API rejected message: {}", result["retcode"]);
    }
    Ok(())
}

pub fn chat_id(event: &Value) -> Option<String> {
    match event["message_type"].as_str()? {
        "group" => event["group_id"].as_i64().map(|id| format!("group:{id}")),
        "private" => event["user_id"].as_i64().map(|id| format!("private:{id}")),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn classify_onebot_chats() {
        assert_eq!(chat_id(&json!({"message_type":"group","group_id":456})), Some("group:456".into()));
        assert_eq!(chat_id(&json!({"message_type":"private","user_id":123})), Some("private:123".into()));
    }
}
