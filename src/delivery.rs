//! Durable, conservative outbound delivery journal.
//!
//! Remote send APIs do not all support caller-supplied idempotency keys.
//! Retrying after a timeout could silently duplicate a message. Persist a
//! "sending" intent before the request and require operator reconciliation
//! if success is unknown. Only explicitly resolved messages are retried.
use crate::AppState;
use anyhow::{bail, Result};
use serde::Serialize;
use sqlx::FromRow;
use std::sync::Arc;

#[derive(Debug, Serialize, FromRow)]
pub struct Attempt {
    pub event_key: String,
    pub account_id: String,
    pub matrix_event: String,
    pub external_chat: String,
    pub status: String,
    pub last_error: Option<String>,
    pub updated_at: String,
}

/// Before accepting application-service traffic after a restart, every
/// interrupted "sending" attempt becomes an explicit operator incident.
/// The remote API may have accepted the message immediately before the crash.
pub async fn recover_interrupted(state:&Arc<AppState>)->Result<u64>{
    let result=sqlx::query(
        "UPDATE delivery_attempts SET status='uncertain',
         last_error='Bridge restarted during remote send; verify source before retry',
         updated_at=CURRENT_TIMESTAMP WHERE status='sending'")
         .execute(&state.pool).await?;
    Ok(result.rows_affected())
}

pub async fn begin(
    state:&Arc<AppState>,event_key:&str,account_id:&str,event_id:&str,chat:&str
)->Result<()> {
    let result=sqlx::query(
        "INSERT OR IGNORE INTO delivery_attempts
        (event_key,account_id,matrix_event,external_chat,status)
        VALUES (?,?,?,?,'sending')"
    ).bind(event_key).bind(account_id).bind(event_id).bind(chat)
        .execute(&state.pool).await?;
    if result.rows_affected()==0 {
        bail!("delivery {event_key} requires reconciliation before automatic retry")
    }
    Ok(())
}

pub async fn uncertain(state:&Arc<AppState>,event_key:&str,reason:&str)->Result<()>{
    // Never expose external service credentials in the admin incident table.
    let summary:String=reason.chars().take(240).collect();
    sqlx::query("UPDATE delivery_attempts SET status='uncertain',
        last_error=?,updated_at=CURRENT_TIMESTAMP WHERE event_key=?")
        .bind(summary).bind(event_key).execute(&state.pool).await?;
    Ok(())
}

pub async fn complete(
    state:&Arc<AppState>,event_key:&str,account_id:&str,chat:&str,
    remote_id:Option<&str>,room:&str,matrix_event:&str,sender:&str
)->Result<()>{
    let mut tx=state.pool.begin().await?;
    if let Some(remote_id)=remote_id {
        sqlx::query("INSERT OR IGNORE INTO event_links
            (account_id,external_chat,external_message,matrix_room,matrix_event,sender_mxid)
            VALUES (?,?,?,?,?,?)")
            .bind(account_id).bind(chat).bind(remote_id)
            .bind(room).bind(matrix_event).bind(sender).execute(&mut *tx).await?;
    }
    sqlx::query("INSERT OR IGNORE INTO outgoing(event_key) VALUES (?)")
        .bind(event_key).execute(&mut *tx).await?;
    sqlx::query("UPDATE delivery_attempts SET status='delivered',
        last_error=NULL,updated_at=CURRENT_TIMESTAMP WHERE event_key=?")
        .bind(event_key).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(())
}

pub async fn list_unresolved(state:&Arc<AppState>)->Result<Vec<Attempt>> {
    Ok(sqlx::query_as::<_,Attempt>(
        "SELECT event_key,account_id,matrix_event,external_chat,status,
            last_error,updated_at
         FROM delivery_attempts WHERE status!='delivered'
         ORDER BY updated_at ASC LIMIT 500")
        .fetch_all(&state.pool).await?)
}

/// Explicit operator decision: "retry" risks duplicate remote delivery;
/// "acknowledge" can lose a message if the remote API never accepted it.
pub async fn resolve(state:&Arc<AppState>,key:&str,action:&str)->Result<bool>{
    let mut tx=state.pool.begin().await?;
    let row:Option<String>=sqlx::query_scalar(
        "SELECT status FROM delivery_attempts WHERE event_key=?")
        .bind(key).fetch_optional(&mut *tx).await?;
    let Some(status)=row else {return Ok(false)};
    if status=="delivered" {return Ok(false)}
    match action {
        "retry"=>{
            sqlx::query("DELETE FROM delivery_attempts WHERE event_key=?")
                .bind(key).execute(&mut *tx).await?;
        }
        "acknowledge"=>{
            sqlx::query("INSERT OR IGNORE INTO outgoing(event_key) VALUES (?)")
                .bind(key).execute(&mut *tx).await?;
            sqlx::query("UPDATE delivery_attempts SET status='delivered',
                last_error='operator acknowledged',updated_at=CURRENT_TIMESTAMP
                WHERE event_key=?").bind(key).execute(&mut *tx).await?;
        }
        _=>bail!("invalid delivery resolution action")
    }
    tx.commit().await?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sqlite_journal_forbids_implicit_retry_and_requires_operator_resolution() {
        let dbfile=std::env::temp_dir().join(format!("2bm-attempts-{}.db",uuid::Uuid::new_v4()));
        let pool=crate::db::connect(&format!("sqlite://{}",dbfile.display())).await.unwrap();
        sqlx::query("INSERT INTO accounts
            (id,platform,label,owner_mxid,credentials,enabled)
            VALUES ('a','qq','a','@a:host','no-secret',1)")
            .execute(&pool).await.unwrap();
        let state=Arc::new(AppState{
            config:crate::config::Config{
                admin_listen:"127.0.0.1:9000".parse().unwrap(),
                matrix_listen:"127.0.0.1:9001".parse().unwrap(),
                combined_listen:None,database_url:"sqlite://unused".into(),
                homeserver:"http://127.0.0.1:1".into(),
                server_name:"host".into(),as_token:"as".into(),
                hs_token:"hs".into(),admin_token:"admin".into(),
                credential_key:[0;32]},
            pool,http:reqwest::Client::new(),
            telegram:crate::telegram_user::Gateway::default(),
            room_creation:tokio::sync::Mutex::new(()),
            delivery_lock:tokio::sync::Mutex::new(()),
            transaction_lock:tokio::sync::Mutex::new(()),
        });
        begin(&state,"event:a","a","$evt","private:2").await.unwrap();
        assert!(begin(&state,"event:a","a","$evt","private:2").await.is_err());
        assert_eq!(recover_interrupted(&state).await.unwrap(),1);
        let interrupted=list_unresolved(&state).await.unwrap();
        assert_eq!(interrupted[0].status,"uncertain");
        assert!(begin(&state,"event:a","a","$evt","private:2").await.is_err());
        uncertain(&state,"event:a","remote request timed out").await.unwrap();
        let pending=list_unresolved(&state).await.unwrap();
        assert_eq!(pending.len(),1);
        assert_eq!(pending[0].status,"uncertain");
        assert!(resolve(&state,"event:a","retry").await.unwrap());
        begin(&state,"event:a","a","$evt","private:2").await.unwrap();
        complete(&state,"event:a","a","private:2",Some("42"),
            "!r:host","$evt","@a:host").await.unwrap();
        assert!(list_unresolved(&state).await.unwrap().is_empty());
        let count:i64=sqlx::query_scalar("SELECT COUNT(*) FROM outgoing WHERE event_key='event:a'")
            .fetch_one(&state.pool).await.unwrap();
        assert_eq!(count,1);
        assert!(!resolve(&state,"event:a","retry").await.unwrap());
        state.pool.close().await;
        drop(state);
        let _=std::fs::remove_file(dbfile);
    }
}
