use crate::{adapters::token, db::Account, virtual_rooms::RemoteChat, AppState};
use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use std::sync::Arc;

pub async fn send(state: &Arc<AppState>, credentials: &Value, chat: &str, text: &str) -> Result<()> {
    send_reply(state,credentials,chat,text,None).await
}
pub fn outbound_segments(text:&str,reply_to:Option<&str>)->Vec<Value>{
    let mut segments=Vec::new();
    if let Some(reply_to)=reply_to.filter(|s| !s.is_empty()) {
        segments.push(json!({"type":"reply","data":{"id":reply_to}}));
    }
    segments.push(json!({"type":"text","data":{"text":text}}));
    segments
}
pub async fn send_reply(
    state: &Arc<AppState>, credentials: &Value, chat: &str,
    text: &str, reply_to: Option<&str>
) -> Result<()> {
    send_reply_id(state,credentials,chat,text,reply_to).await.map(|_|())
}
pub async fn send_reply_id(
    state: &Arc<AppState>, credentials: &Value, chat: &str,
    text: &str, reply_to: Option<&str>
) -> Result<Option<String>> {
    let api_base = token(credentials, "api_base")?.trim_end_matches('/');
    let (kind, id) = chat.split_once(':')
        .context("QQ chat must use group:<id> or private:<id>")?;
    let numeric_id: i64 = id.parse().context("QQ target id must be an integer")?;
    let segments=outbound_segments(text,reply_to);
    let (action, request) = match kind {
        "group" => ("send_group_msg", json!({
            "group_id": numeric_id,
            "message": segments
        })),
        "private" => ("send_private_msg", json!({
            "user_id": numeric_id,
            "message": segments
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
    Ok(result["data"]["message_id"].as_i64().map(|id|id.to_string())
        .or_else(||result["data"]["message_id"].as_str().map(str::to_owned)))
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
    pub already_present: usize,
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
    let mut report=MemberSyncReport {
        discovered:members.len(),joined:0,already_present:0,failed:0
    };
    // Skip already provisioned members on subsequent batches, allowing the next
    // invocation to advance through a large group instead of repeating page one.
    let mut attempted=0;
    for member in members {
        let Some(id)=member["user_id"].as_i64().filter(|x|*x>0) else {continue};
        let contact=id.to_string();
        let exists:Option<i64>=sqlx::query_scalar(
            "SELECT 1 FROM qq_synced_members WHERE account_id=? AND external_chat=? AND contact_id=?"
        ).bind(&account.id).bind(chat).bind(&contact).fetch_optional(&state.pool).await?;
        if exists.is_some() {report.already_present+=1;continue;}
        if attempted>=limit.clamp(1,100) {break;}
        attempted+=1;
        let name=member["card"].as_str().filter(|n|!n.is_empty())
            .or_else(||member["nickname"].as_str()).unwrap_or("QQ user");
        let outcome=async {
            let ghost=crate::matrix::ghost(state,&account.id,&contact,name).await?;
            crate::matrix::ensure_ghost_in_room(state,&mapping.matrix_room,&ghost).await?;
            sqlx::query("INSERT OR IGNORE INTO qq_synced_members(account_id,external_chat,contact_id)
                VALUES (?,?,?)").bind(&account.id).bind(chat).bind(&contact)
                .execute(&state.pool).await?;
            Ok::<(),anyhow::Error>(())
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
        sqlx::query("INSERT OR IGNORE INTO qq_synced_members(account_id,external_chat,contact_id)
            VALUES (?,?,?)").bind(&account.id).bind(chat).bind(contact)
            .execute(&state.pool).await?;
    } else {
        // A departure can be delivered repeatedly or before a join.
        if let Err(error)=crate::matrix::leave_ghost(state,&mapping.matrix_room,&mxid).await {
            tracing::debug!(%error,room=%mapping.matrix_room,"QQ group leave may already be applied");
        }
        sqlx::query("DELETE FROM qq_synced_members
            WHERE account_id=? AND external_chat=? AND contact_id=?")
            .bind(&account.id).bind(chat).bind(contact)
            .execute(&state.pool).await?;
    }
    Ok(())
}


pub fn reply_target(event:&Value)->Option<String>{
    let segments=event["message"].as_array()?;
    let reply=segments.iter().find(|segment|segment["type"]=="reply")?;
    reply["data"]["id"].as_str().map(str::to_owned)
        .or_else(||reply["data"]["id"].as_i64().map(|id|id.to_string()))
}

pub fn message_text(event:&Value)->String{
    if let Some(segments)=event["message"].as_array() {
        let mut text=String::new();
        for segment in segments {
            match segment["type"].as_str() {
                Some("text")=>text.push_str(segment["data"]["text"].as_str().unwrap_or("")),
                Some("at")=>{
                    text.push('@');
                    text.push_str(segment["data"]["qq"].as_str().unwrap_or("user"));
                }
                Some("image")=>text.push_str("[image]"),
                Some("file")=>text.push_str("[file]"),
                Some("record")=>text.push_str("[voice]"),
                Some("video")=>text.push_str("[video]"),
                _=>{},
            }
        }
        if !text.trim().is_empty() {return text;}
    }
    event["raw_message"].as_str().or_else(||event["message"].as_str())
        .unwrap_or_default().to_owned()
}

pub fn recall_target(event:&Value)->Option<(String,String)>{
    let msg=event["message_id"].as_i64().map(|id|id.to_string())
        .or_else(||event["message_id"].as_str().map(str::to_owned))?;
    let chat=match event["notice_type"].as_str()? {
        "group_recall" => format!("group:{}",event["group_id"].as_i64()?),
        "friend_recall" => {
            let actor=event["user_id"].as_i64()?;
            let peer=if Some(actor)==event["self_id"].as_i64() {
                event["target_id"].as_i64().unwrap_or(actor)
            } else {actor};
            format!("private:{peer}")
        },
        _ => return None,
    };
    Some((chat,msg))
}

/// NapCat includes message_sent for messages sent from the logged-in QQ
/// account, including another device. A private sent chat uses target_id.
pub fn own_chat_id(event:&Value)->Option<String>{
    let self_id=event["self_id"].as_i64()?;
    let sender=event["user_id"].as_i64()?;
    if self_id!=sender {return None;}
    match event["message_type"].as_str()? {
        "group" => event["group_id"].as_i64()
            .or_else(||event["target_id"].as_i64())
            .map(|id|format!("group:{id}")),
        "private" => event["target_id"].as_i64()
            .map(|id|format!("private:{id}")),
        _=>None,
    }
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
    fn build_native_onebot_reply_segments() {
        let result=outbound_segments("second",Some("123"));
        assert_eq!(result[0],json!({"type":"reply","data":{"id":"123"}}));
        assert_eq!(result[1],json!({"type":"text","data":{"text":"second"}}));
        assert_eq!(outbound_segments("plain",None).len(),1);
    }
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
        assert_eq!(recall_target(&json!({"notice_type":"friend_recall",
            "self_id":999,"user_id":999,"target_id":123,"message_id":13})),
            Some(("private:123".into(),"13".into())));
        assert_eq!(recall_target(&json!({"notice_type":"poke","user_id":99,"message_id":12})),None);
    }
    #[test]
    fn classify_sent_messages_without_self_to_self_dm() {
        let group=json!({"post_type":"message_sent","message_type":"group",
            "self_id":12,"user_id":12,"group_id":34,"message_id":54});
        let dm=json!({"post_type":"message_sent","message_type":"private",
            "self_id":12,"user_id":12,"target_id":87,"message_id":55});
        assert_eq!(own_chat_id(&group),Some("group:34".into()));
        assert_eq!(own_chat_id(&dm),Some("private:87".into()));
        assert_eq!(own_chat_id(&json!({"self_id":12,"user_id":88,
            "message_type":"private","target_id":87})),None);
    }
    #[test]
    fn parse_remote_onebot_reply_and_text_segments(){
        let event=json!({"message":[
            {"type":"reply","data":{"id":"123"}},
            {"type":"text","data":{"text":"response"}},
            {"type":"image","data":{"file":"test"}}
        ]});
        assert_eq!(reply_target(&event),Some("123".to_owned()));
        assert_eq!(message_text(&event),"response[image]");
    }
    #[test]
    fn classify_onebot_chats() {
        assert_eq!(chat_id(&json!({"message_type":"group","group_id":456})), Some("group:456".into()));
        assert_eq!(chat_id(&json!({"message_type":"private","user_id":123})), Some("private:123".into()));
    }
}
