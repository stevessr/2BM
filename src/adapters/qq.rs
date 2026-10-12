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

#[derive(serde::Serialize)]
pub struct MemberSyncReport {
    pub discovered: usize,
    pub joined: usize,
    pub failed: usize,
}
pub async fn sync_group_members(
    state: &Arc<AppState>, account: &Account, chat: &str, limit: usize
) -> Result<MemberSyncReport> {
    if account.platform != "qq" { bail!("QQ account required"); }
    let group: i64 = chat.strip_prefix("group:")
        .context("expected group:<id>")?.parse()?;
    if group <= 0 { bail!("invalid QQ group ID"); }
    let mapping = crate::db::mapping_for_chat(&state.pool,&account.id,chat).await?
        .context("virtual room must exist before membership sync")?;
    let creds = crate::adapters::credentials(state,account)?;
    let base=token(&creds,"api_base")?.trim_end_matches('/');
    let response=state.http.get(format!("{base}/get_group_member_list"))
        .bearer_auth(token(&creds,"access_token")?)
        .query(&[("group_id",group)])
        .send().await?.error_for_status()?;
    let payload: Value=response.json().await?;
    if payload["status"]!="ok" && payload["retcode"]!=0 {
        bail!("OneBot get_group_member_list failed: {}",payload["retcode"]);
    }
    let members=payload["data"].as_array()
        .context("OneBot get_group_member_list returned no array")?;
    let mut report=MemberSyncReport {discovered:members.len(),joined:0,failed:0};
    // Large QQ groups may contain thousands of members. A batch only provisions
    // the first limited roster slice; live notices cover later arrivals.
    for member in members.iter().take(limit.clamp(1,100)) {
        let Some(id)=member["user_id"].as_i64().filter(|x|*x>0) else {continue};
        let name=member["card"].as_str().filter(|n|!n.is_empty())
            .or_else(||member["nickname"].as_str()).unwrap_or("QQ user");
        let outcome=async {
            let ghost=crate::matrix::ghost(state,&account.id,&id.to_string(),name).await?;
            crate::matrix::ensure_ghost_in_room(state,&mapping.matrix_room,&ghost).await
        }.await;
        match outcome {
            Ok(())=>report.joined+=1,
            Err(error)=>{
                report.failed+=1;
                tracing::warn!(account=%account.id,group,member=id,%error,"QQ group member sync failed");
            }
        }
    }
    Ok(report)
}

pub fn membership_target(event:&Value)->Option<(String,String,bool)>{
    let group=event["group_id"].as_i64().filter(|n|*n>0)?;
    let user=event["user_id"].as_i64().filter(|n|*n>0)?;
    let joined=match event["notice_type"].as_str()? {
        "group_increase"=>true,
        "group_decrease"=>false,
        _=>return None,
    };
    Some((format!("group:{group}"),user.to_string(),joined))
}

pub async fn membership_notice(
    state:&Arc<AppState>,account:&Account,chat:&str,contact:&str,joined:bool
)->Result<()>{
    // Never create a room purely from an untrusted notification.
    let Some(mapping)=crate::db::mapping_for_chat(&state.pool,&account.id,chat).await? else {
        return Ok(());
    };
    let mxid=crate::matrix::ghost_user_id(state,&account.id,contact);
    if joined {
        let ghost=crate::matrix::ghost(state,&account.id,contact,
            &format!("QQ {contact}")).await?;
        crate::matrix::ensure_ghost_in_room(state,&mapping.matrix_room,&ghost).await?;
    } else {
        // A departure can be delivered repeatedly or before a join.
        if let Err(error)=crate::matrix::leave_ghost(state,&mapping.matrix_room,&mxid).await {
            tracing::debug!(%error,room=%mapping.matrix_room,"QQ group leave may already be applied");
        }
    }
    Ok(())
}

pub fn recall_target(event:&Value)->Option<(String,String)>{
    let msg=event["message_id"].as_i64().map(|id|id.to_string())
        .or_else(||event["message_id"].as_str().map(str::to_owned))?;
    let chat=match event["notice_type"].as_str()? {
        "group_recall" => format!("group:{}",event["group_id"].as_i64()?),
        "friend_recall" => format!("private:{}",event["user_id"].as_i64()?),
        _ => return None,
    };
    Some((chat,msg))
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
    fn classify_membership_notices() {
        assert_eq!(membership_target(&json!({"notice_type":"group_increase","group_id":12,"user_id":34})),
            Some(("group:12".into(),"34".into(),true)));
        assert_eq!(membership_target(&json!({"notice_type":"group_decrease","group_id":12,"user_id":34})),
            Some(("group:12".into(),"34".into(),false)));
        assert_eq!(membership_target(&json!({"notice_type":"group_recall","group_id":12,"user_id":34})),None);
    }
    #[test]
    fn classify_onebot_recall_events() {
        assert_eq!(recall_target(&json!({"notice_type":"group_recall","group_id":25,"message_id":14})),
            Some(("group:25".into(),"14".into())));
        assert_eq!(recall_target(&json!({"notice_type":"friend_recall","user_id":99,"message_id":12})),
            Some(("private:99".into(),"12".into())));
        assert_eq!(recall_target(&json!({"notice_type":"poke","user_id":99,"message_id":12})),None);
    }
    #[test]
    fn classify_onebot_chats() {
        assert_eq!(chat_id(&json!({"message_type":"group","group_id":456})), Some("group:456".into()));
        assert_eq!(chat_id(&json!({"message_type":"private","user_id":123})), Some("private:123".into()));
    }
}
