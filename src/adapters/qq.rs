use crate::{adapters::token, db::Account, virtual_rooms::RemoteChat, AppState};
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


async fn onebot_list(state: &Arc<AppState>, credentials: &Value, action: &str) -> Result<Vec<Value>> {
    let base = token(credentials, "api_base")?.trim_end_matches('/');
    let response = state.http.get(format!("{base}/{action}"))
        .bearer_auth(token(credentials, "access_token")?)
        .send().await?.error_for_status()?;
    let payload: Value = response.json().await?;
    if payload["status"] != "ok" && payload["retcode"] != 0 {
        bail!("OneBot {action} failed: {}", payload["retcode"]);
    }
    let items = payload["data"].as_array()
        .with_context(|| format!("OneBot {action} did not return a list"))?;
    Ok(items.clone())
}

/// Discover existing groups and private contacts through an authenticated,
/// logged-in QQ OneBot connector. Do not create rooms for arbitrary public users.
pub async fn list_chats(state: &Arc<AppState>, account: &Account) -> Result<Vec<RemoteChat>> {
    let creds = crate::adapters::credentials(state, account)?;
    let friends = onebot_list(state, &creds, "get_friend_list").await?;
    let groups = onebot_list(state, &creds, "get_group_list").await?;
    let mut chats = Vec::with_capacity(friends.len() + groups.len());
    for friend in friends {
        let Some(id) = friend["user_id"].as_i64().filter(|id| *id > 0) else { continue; };
        let title = friend["remark"].as_str().filter(|s| !s.is_empty())
            .or_else(|| friend["nickname"].as_str()).unwrap_or("QQ contact");
        chats.push(RemoteChat::new(format!("private:{id}"),
            title, true, Some(id.to_string())));
    }
    for group in groups {
        let Some(id) = group["group_id"].as_i64().filter(|id| *id > 0) else { continue; };
        let title = group["group_name"].as_str().filter(|s| !s.is_empty())
            .unwrap_or("QQ group");
        chats.push(RemoteChat::new(format!("group:{id}"),title,false,None));
    }
    Ok(chats)
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
