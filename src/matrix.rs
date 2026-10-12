use crate::AppState;
use anyhow::{bail, Result};
use serde_json::{json, Value};
use std::sync::Arc;

pub async fn register_bot(state: &Arc<AppState>) -> Result<()> {
    let url = format!("{}/_matrix/client/v3/register?kind=user", state.config.homeserver);
    let response = state.http.post(url)
        .bearer_auth(&state.config.as_token)
        .json(&json!({
            "type": "m.login.application_service",
            "username": "_2bm",
            "inhibit_login": true
        }))
        .send().await?;
    let status = response.status();
    if status.is_success() { return Ok(()); }
    let body: Value = response.json().await.unwrap_or(Value::Null);
    if body["errcode"] == "M_USER_IN_USE" { return Ok(()); }
    bail!("Matrix user registration returned {status}: {body}");
}

pub async fn join_bot(state: &Arc<AppState>, room: &str) -> Result<()> {
    let url = format!(
        "{}/_matrix/client/v3/join/{}",
        state.config.homeserver, urlencoding::encode(room)
    );
    state.http.post(url)
        .bearer_auth(&state.config.as_token)
        .json(&json!({}))
        .send().await?
        .error_for_status()?;
    Ok(())
}

pub async fn send_text(state: &Arc<AppState>, room: &str, text: &str, txn: &str) -> Result<()> {
    let url = format!(
        "{}/_matrix/client/v3/rooms/{}/send/m.room.message/{}",
        state.config.homeserver,
        urlencoding::encode(room),
        urlencoding::encode(txn)
    );
    state.http.put(url)
        .bearer_auth(&state.config.as_token)
        .json(&json!({"msgtype": "m.text", "body": text}))
        .send().await?
        .error_for_status()?;
    Ok(())
}
