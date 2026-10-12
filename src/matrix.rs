use crate::{virtual_rooms, AppState};
use anyhow::{bail, Result};
use reqwest::StatusCode;
use serde_json::{json, Value};
use std::sync::Arc;
use uuid::Uuid;

pub async fn register_bot(state: &Arc<AppState>) -> Result<()> { register(state,"_2bm").await }
async fn register(state: &Arc<AppState>, localpart: &str) -> Result<()> {
    let response = state.http.post(format!("{}/_matrix/client/v3/register?kind=user",state.config.homeserver))
        .bearer_auth(&state.config.as_token)
        .json(&json!({"type":"m.login.application_service","username":localpart,"inhibit_login":true}))
        .send().await?;
    let status = response.status();
    if status.is_success() {return Ok(());}
    let body: Value = response.json().await.unwrap_or(Value::Null);
    if body["errcode"]=="M_USER_IN_USE" {return Ok(());}
    bail!("Matrix registration {status}: {body}")
}
pub fn ghost_user_id(state:&Arc<AppState>,account:&str,sender:&str)->String{
    let name = format!("_2bm_p_{}",Uuid::new_v5(&Uuid::NAMESPACE_URL,format!("{account}:{sender}").as_bytes()).simple());
    format!("@{}:{}",name,state.config.server_name)
}
pub async fn ghost(state: &Arc<AppState>,account:&str,sender:&str,display:&str)->Result<String>{
    let mxid=ghost_user_id(state,account,sender);
    let previous: Option<String> = sqlx::query_scalar(
        "SELECT display_name FROM ghost_users WHERE mxid = ?")
        .bind(&mxid).fetch_optional(&state.pool).await?;
    if previous.as_deref() == Some(display) { return Ok(mxid); }
    if previous.is_none() {
        let localpart = mxid.trim_start_matches('@')
            .split(':').next().ok_or_else(||anyhow::anyhow!("invalid ghost MXID"))?;
        register(state,localpart).await?;
    }
    state.http.put(format!("{}/_matrix/client/v3/profile/{}/displayname",
        state.config.homeserver,urlencoding::encode(&mxid)))
        .bearer_auth(&state.config.as_token)
        .query(&[("user_id",&mxid)])
        .json(&json!({"displayname":display.chars().take(100).collect::<String>()}))
        .send().await?.error_for_status()?;
    sqlx::query("INSERT INTO ghost_users(mxid,account_id,contact_id,display_name)
        VALUES (?,?,?,?) ON CONFLICT(mxid) DO UPDATE SET display_name=excluded.display_name")
        .bind(&mxid).bind(account).bind(sender).bind(display)
        .execute(&state.pool).await?;
    Ok(mxid)
}
pub async fn join_bot(state:&Arc<AppState>,room:&str)->Result<()> {join(state,room,None).await}
pub async fn join_ghost(state:&Arc<AppState>,room:&str,user:&str)->Result<()> {join(state,room,Some(user)).await}
async fn join(state:&Arc<AppState>,room:&str,as_user:Option<&str>)->Result<()>{
    let request=state.http.post(format!("{}/_matrix/client/v3/join/{}",
        state.config.homeserver,urlencoding::encode(room))).bearer_auth(&state.config.as_token);
    let request=if let Some(u)=as_user {request.query(&[("user_id",u)])}else{request};
    request.json(&json!({})).send().await?.error_for_status()?;
    Ok(())
}
/// Matrix alias makes provisioning crash-idempotent. 409 is only treated as a
/// recovery if the exact alias resolves to a room; other failures propagate.
pub async fn create_virtual_room(
    state:&Arc<AppState>,account:&str,chat:&str,title:&str,
    owner:&str,initial_ghost:Option<&str>,direct:bool
)->Result<String>{
    let localpart=virtual_rooms::alias(account,chat);
    let alias=format!("#{}:{}",localpart,state.config.server_name);
    let creator=if direct {initial_ghost} else {None};
    let mut request=state.http.post(format!("{}/_matrix/client/v3/createRoom",state.config.homeserver))
        .bearer_auth(&state.config.as_token);
    if let Some(user)=creator {request=request.query(&[("user_id",user)]);}
    let body=json!({
        "preset":"private_chat",
        "visibility":"private",
        "room_alias_name":localpart,
        "name":title,
        "topic":"Managed by 2BM Matrix bridge",
        "is_direct":direct,
        "invite":[owner],
        "initial_state":[{
            "type":"com.2bm.bridge","state_key":"",
            "content":{"account_id":account,"external_chat":chat,"network":"2bm"}
        }]
    });
    let response=request.json(&body).send().await?;
    if response.status()==StatusCode::CONFLICT {
        let lookup=state.http.get(format!("{}/_matrix/client/v3/directory/room/{}",
            state.config.homeserver,urlencoding::encode(&alias)))
            .bearer_auth(&state.config.as_token).send().await?.error_for_status()?;
        let body:Value=lookup.json().await?;
        return body["room_id"].as_str().map(str::to_owned)
            .ok_or_else(||anyhow::anyhow!("room alias conflict but resolution gave no room ID"));
    }
    let response=response.error_for_status()?;
    let result:Value=response.json().await?;
    let room=result["room_id"].as_str()
        .ok_or_else(||anyhow::anyhow!("Matrix createRoom returned no room ID"))?;
    if let Some(ghost)=creator {
        sqlx::query("INSERT OR IGNORE INTO ghost_room_memberships(matrix_room,ghost_mxid)
            VALUES (?,?)").bind(room).bind(ghost).execute(&state.pool).await?;
    }
    Ok(room.to_owned())
}
pub async fn leave_ghost(state:&Arc<AppState>,room:&str,ghost:&str)->Result<()>{
    state.http.post(format!("{}/_matrix/client/v3/rooms/{}/leave",
        state.config.homeserver,urlencoding::encode(room)))
        .bearer_auth(&state.config.as_token)
        .query(&[("user_id",ghost)])
        .json(&json!({}))
        .send().await?.error_for_status()?;
    sqlx::query("DELETE FROM ghost_room_memberships WHERE matrix_room=? AND ghost_mxid=?")
        .bind(room).bind(ghost).execute(&state.pool).await?;
    Ok(())
}
pub async fn ensure_ghost_in_room(state:&Arc<AppState>,room:&str,ghost:&str)->Result<()>{
    let known:Option<i64>=sqlx::query_scalar(
        "SELECT 1 FROM ghost_room_memberships WHERE matrix_room=? AND ghost_mxid=?"
    ).bind(room).bind(ghost).fetch_optional(&state.pool).await?;
    if known.is_some() {return Ok(());}
    // Joining as a Matrix appservice masqueraded user may work immediately
    // when it created the room; otherwise the bot must invite the ghost.
    if join_ghost(state,room,ghost).await.is_err() {
        let response=state.http.post(format!("{}/_matrix/client/v3/rooms/{}/invite",
            state.config.homeserver,urlencoding::encode(room)))
            .bearer_auth(&state.config.as_token)
            .json(&json!({"user_id":ghost})).send().await?;
        if !response.status().is_success() {
            tracing::debug!(status=%response.status(),"Matrix ghost invite not accepted");
        }
        join_ghost(state,room,ghost).await?;
    }
    sqlx::query("INSERT OR IGNORE INTO ghost_room_memberships(matrix_room,ghost_mxid)
        VALUES (?,?)").bind(room).bind(ghost).execute(&state.pool).await?;
    Ok(())
}

pub async fn set_virtual_room_name(
    state:&Arc<AppState>,room:&str,title:&str,actor:Option<&str>
)->Result<()>{
    let request=state.http.put(format!("{}/_matrix/client/v3/rooms/{}/state/m.room.name",
        state.config.homeserver,urlencoding::encode(room)))
        .bearer_auth(&state.config.as_token);
    let request=if let Some(actor)=actor {request.query(&[("user_id",actor)])}
        else {request};
    request.json(&json!({"name":title})).send().await?.error_for_status()?;
    Ok(())
}
pub async fn send_as(state:&Arc<AppState>,room:&str,body:&str,txn:&str,mxid:&str)->Result<String>{
    send_as_reply(state,room,body,txn,mxid,None).await
}
pub async fn send_as_reply(
    state:&Arc<AppState>,room:&str,body:&str,txn:&str,mxid:&str,
    reply_to:Option<&str>
)->Result<String>{
    let mut content=json!({"msgtype":"m.text","body":body});
    if let Some(reply)=reply_to {
        content["m.relates_to"]=json!({"m.in_reply_to":{"event_id":reply}});
    }
    let response = state.http.put(format!("{}/_matrix/client/v3/rooms/{}/send/m.room.message/{}",
        state.config.homeserver,urlencoding::encode(room),urlencoding::encode(txn)))
        .bearer_auth(&state.config.as_token)
        .query(&[("user_id",mxid)])
        .json(&content)
        .send().await?.error_for_status()?;
    let data:Value=response.json().await?;
    data["event_id"].as_str().map(str::to_owned)
        .ok_or_else(||anyhow::anyhow!("Matrix send message has no event_id"))
}

/// A ghost can redact messages it originally sent, including when it has no
/// power to redact other group members' messages.
pub async fn redact_as(
    state:&Arc<AppState>,room:&str,event:&str,txn:&str,sender:&str
)->Result<()>{
    state.http.put(format!("{}/_matrix/client/v3/rooms/{}/redact/{}/{}",
        state.config.homeserver,urlencoding::encode(room),
        urlencoding::encode(event),urlencoding::encode(txn)))
        .bearer_auth(&state.config.as_token)
        .query(&[("user_id",sender)])
        .json(&json!({"reason":"Removed on the source network"}))
        .send().await?.error_for_status()?;
    Ok(())
}
