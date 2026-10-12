use crate::AppState;
use anyhow::{bail, Result};
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

pub async fn register_bot(state: &Arc<AppState>) -> Result<()> {
    register(state, "_2bm").await
}
async fn register(state: &Arc<AppState>, localpart: &str) -> Result<()> {
    let response = state.http.post(format!("{}/_matrix/client/v3/register?kind=user", state.config.homeserver))
        .bearer_auth(&state.config.as_token)
        .json(&json!({"type":"m.login.application_service","username":localpart,"inhibit_login":true}))
        .send().await?;
    let status = response.status();
    if status.is_success() { return Ok(()); }
    let body: Value = response.json().await.unwrap_or(Value::Null);
    if body["errcode"] == "M_USER_IN_USE" { return Ok(()); }
    bail!("Matrix registration {status}: {body}")
}
pub async fn ghost(state: &Arc<AppState>, account: &str, sender: &str, display: &str) -> Result<String> {
    let key = format!("{account}:{sender}");
    let name = format!("_2bm_p_{}", Uuid::new_v5(&Uuid::NAMESPACE_URL, key.as_bytes()).simple());
    register(state, &name).await?;
    let mxid = format!("@{}:{}", name, state.config.server_name);
    let response = state.http.put(format!("{}/_matrix/client/v3/profile/{}/displayname", state.config.homeserver, urlencoding::encode(&mxid)))
        .bearer_auth(&state.config.as_token)
        .query(&[("user_id",&mxid)])
        .json(&json!({"displayname":display.chars().take(100).collect::<String>()}))
        .send().await;
    if let Err(e) = response { tracing::debug!(%e,"unable to set ghost display name"); }
    Ok(mxid)
}
pub async fn join_bot(state: &Arc<AppState>, room: &str) -> Result<()> {
    join(state, room, None).await
}
pub async fn join_ghost(state: &Arc<AppState>, room: &str, mxid: &str) -> Result<()> {
    join(state, room, Some(mxid)).await
}
async fn join(state: &Arc<AppState>, room: &str, as_user: Option<&str>) -> Result<()> {
    let req = state.http.post(format!("{}/_matrix/client/v3/join/{}", state.config.homeserver, urlencoding::encode(room)))
        .bearer_auth(&state.config.as_token);
    let req = if let Some(user) = as_user { req.query(&[("user_id",user)]) } else { req };
    req.json(&json!({})).send().await?.error_for_status()?;
    Ok(())
}
pub async fn create_room(state: &Arc<AppState>, platform: &str, chat: &str, owner: &str, ghost: &str) -> Result<String> {
    let response = state.http.post(format!("{}/_matrix/client/v3/createRoom", state.config.homeserver))
        .bearer_auth(&state.config.as_token)
        .json(&json!({"preset":"private_chat","visibility":"private",
            "name":format!("{} · {}",platform,chat),"invite":[owner,ghost]}))
        .send().await?.error_for_status()?;
    let body: Value = response.json().await?;
    let room = body["room_id"].as_str().ok_or_else(|| anyhow::anyhow!("room ID missing"))?;
    join_ghost(state, room, ghost).await?;
    Ok(room.to_string())
}
pub async fn ensure_ghost_in_room(state: &Arc<AppState>, room: &str, ghost: &str) -> Result<()> {
    // The Matrix homeserver may return M_FORBIDDEN when the user was already invited/joined.
    let r = state.http.post(format!("{}/_matrix/client/v3/rooms/{}/invite",
            state.config.homeserver,urlencoding::encode(room)))
        .bearer_auth(&state.config.as_token)
        .json(&json!({"user_id":ghost}))
        .send().await?;
    if !r.status().is_success() {
        tracing::debug!(status=%r.status(),"invite may be redundant");
    }
    join_ghost(state, room, ghost).await
}
pub async fn send_as(state: &Arc<AppState>, room: &str, body: &str, txn: &str, mxid: &str) -> Result<()> {
    state.http.put(format!("{}/_matrix/client/v3/rooms/{}/send/m.room.message/{}",
            state.config.homeserver,urlencoding::encode(room),urlencoding::encode(txn)))
        .bearer_auth(&state.config.as_token)
        .query(&[("user_id",mxid)])
        .json(&json!({"msgtype":"m.text","body":body}))
        .send().await?.error_for_status()?;
    Ok(())
}
