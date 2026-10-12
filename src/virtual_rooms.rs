use crate::{db, matrix, AppState};
use anyhow::{Context, Result};
use serde::Serialize;
use std::sync::Arc;
use uuid::Uuid;

/// A virtual conversation is a normal private Matrix room, provisioned and
/// owned by 2BM. The Matrix client needs no nonstandard protocol extensions.
#[derive(Clone, Debug)]
pub struct RemoteChat {
    pub external_chat: String,
    pub title: String,
    pub direct: bool,
    /// Human contact for private chats. Group members are added on demand.
    pub contact_id: Option<String>,
}
impl RemoteChat {
    pub fn new(chat: impl Into<String>, title: impl Into<String>, direct: bool, contact: Option<String>) -> Self {
        Self { external_chat: chat.into(), title: title.into(), direct, contact_id: contact }
    }
}
#[derive(Serialize)]
pub struct SyncReport {
    pub discovered: usize,
    pub provisioned: usize,
    pub failed: usize,
    pub already_present: usize,
}

/// A serialized critical section prevents two concurrent messages from making
/// two rooms in one process. A stable Matrix room alias also permits recovery
/// after a crash between createRoom and committing the SQLite mapping.
pub async fn ensure(
    state: &Arc<AppState>,
    account: &db::Account,
    chat: &RemoteChat,
    sender: Option<(&str, &str)>,
) -> Result<db::Mapping> {
    let _guard = state.room_creation.lock().await;
    let title = chat.title.trim();
    let title = if title.is_empty() { chat.external_chat.as_str() } else { title };
    let title: String = title.chars().take(160).collect();
    let old_title:Option<String>=sqlx::query_scalar(
        "SELECT title FROM virtual_chats WHERE account_id=? AND external_chat=?")
        .bind(&account.id).bind(&chat.external_chat)
        .fetch_optional(&state.pool).await?;
    sqlx::query("INSERT INTO virtual_chats(account_id,external_chat,title,direct)
        VALUES (?,?,?,?)
        ON CONFLICT(account_id,external_chat) DO UPDATE SET
          title = CASE WHEN excluded.title = virtual_chats.external_chat
               THEN virtual_chats.title ELSE excluded.title END,
          direct = excluded.direct, updated_at = CURRENT_TIMESTAMP")
        .bind(&account.id).bind(&chat.external_chat).bind(&title).bind(chat.direct)
        .execute(&state.pool).await?;

    let sender_ghost = if let Some((sender_id, name)) = sender {
        Some(matrix::ghost(state, &account.id, sender_id,
            &format!("{} ({})",name,account.platform)).await?)
    } else { None };
    let mapping = if let Some(existing) =
        db::mapping_for_chat(&state.pool, &account.id, &chat.external_chat).await? {
        existing
    } else {
        let initial_ghost = if chat.direct {
            if let Some(ghost) = sender_ghost.as_deref() {
                Some(ghost.to_owned())
            } else if let Some(contact) = chat.contact_id.as_deref() {
                Some(matrix::ghost(state, &account.id, contact, &title).await?)
            } else { None }
        } else { None };
        let room = matrix::create_virtual_room(
            state, &account.id, &chat.external_chat, &title,
            &account.owner_mxid, initial_ghost.as_deref(), chat.direct
        ).await?;
        sqlx::query("INSERT OR IGNORE INTO mappings(account_id,external_chat,matrix_room)
                     VALUES (?,?,?)")
            .bind(&account.id).bind(&chat.external_chat).bind(&room)
            .execute(&state.pool).await?;
        db::mapping_for_chat(&state.pool, &account.id, &chat.external_chat)
            .await?.context("unable to persist provisioned room")?
    };
    if let Some(ghost) = sender_ghost.as_deref() {
        matrix::ensure_ghost_in_room(state, &mapping.matrix_room, ghost).await?;
    }
    // Only propagate meaningful source title changes, not fallback IDs.
    // A non-bridge/manual room might reject renames; this must never block
    // an otherwise successful message delivery.
    if old_title.as_deref().is_some_and(|old|old!=title)
        && title != chat.external_chat {
        let actor=if chat.direct {
            chat.contact_id.as_deref()
                .or_else(||sender.map(|(id,_)|id))
                .map(|id|matrix::ghost_user_id(state,&account.id,id))
        } else {None};
        if let Err(error)=matrix::set_virtual_room_name(
            state,&mapping.matrix_room,&title,actor.as_deref()).await {
            tracing::warn!(room=%mapping.matrix_room,%error,
                "unable to synchronize virtual room display name");
            // Keep the previous title in SQLite so the next sync will retry.
            if let Some(previous)=old_title.as_deref() {
                sqlx::query("UPDATE virtual_chats SET title=? WHERE account_id=? AND external_chat=?")
                    .bind(previous).bind(&account.id).bind(&chat.external_chat)
                    .execute(&state.pool).await?;
            }
        }
    }
    Ok(mapping)
}

pub async fn sync(
    state: &Arc<AppState>, account: &db::Account, limit: usize
) -> Result<SyncReport> {
    let conversations = match account.platform.as_str() {
        "qq" => crate::adapters::qq::list_chats(state, account).await?,
        "telegram_user" => crate::telegram_user::list_chats(state, &account.id).await?,
        _ => anyhow::bail!("only QQ and Telegram human-account modes support chat discovery"),
    };
    let discovered = conversations.len();
    let mut report = SyncReport {
        discovered, provisioned: 0, failed: 0, already_present: 0
    };
    let mut attempted = 0;
    for chat in conversations {
        if db::mapping_for_chat(&state.pool,&account.id,&chat.external_chat)
            .await?.is_some() {
            report.already_present += 1;
            let desired: String=chat.title.trim().chars().take(160).collect();
            let current:Option<String>=sqlx::query_scalar(
                "SELECT title FROM virtual_chats WHERE account_id=? AND external_chat=?"
            ).bind(&account.id).bind(&chat.external_chat).fetch_optional(&state.pool).await?;
            if desired != chat.external_chat && current.as_deref() != Some(&desired) {
                if let Err(error)=ensure(state,account,&chat,None).await {
                    report.failed+=1;
                    tracing::warn!(account=%account.id,chat=%chat.external_chat,%error,
                        "virtual room title refresh failed");
                }
            }
            continue;
        }
        if attempted >= limit { break; }
        attempted += 1;
        match ensure(state, account, &chat, None).await {
            Ok(_) => report.provisioned += 1,
            Err(error) => {
                report.failed += 1;
                tracing::warn!(account=%account.id, chat=%chat.external_chat,
                    %error, "virtual room provision failed");
            }
        }
    }
    Ok(report)
}

pub fn alias(account: &str, chat: &str) -> String {
    let key = format!("{account}:{chat}");
    format!("_2bm_r_{}", Uuid::new_v5(&Uuid::NAMESPACE_URL,key.as_bytes()).simple())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn room_provisioning_is_idempotent_and_recovers_from_sqlite_loss() {
        use axum::{extract::State, http::StatusCode, routing::{get,post}, Json, Router};
        use serde_json::{json,Value};
        use std::sync::atomic::{AtomicUsize,Ordering};

        struct Mock {
            creates: AtomicUsize,
            expected_alias: String,
        }
        async fn create(
            State(mock):State<Arc<Mock>>, Json(body):Json<Value>
        )->(StatusCode,Json<Value>){
            assert_eq!(body["room_alias_name"],mock.expected_alias);
            let attempt=mock.creates.fetch_add(1,Ordering::SeqCst);
            if attempt==0 {(StatusCode::OK,Json(json!({"room_id":"!stable:example.org"})))}
            else {(StatusCode::CONFLICT,Json(json!({"errcode":"M_ROOM_IN_USE"})))}
        }
        async fn resolve()->Json<Value> {Json(json!({"room_id":"!stable:example.org"}))}

        let mock=Arc::new(Mock {
            creates:AtomicUsize::new(0),
            expected_alias:alias("test-account","group:987"),
        });
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hs=format!("http://{}",listener.local_addr().unwrap());
        let router=Router::new()
            .route("/_matrix/client/v3/createRoom",post(create))
            .route("/_matrix/client/v3/directory/room/{alias}",get(resolve))
            .with_state(mock.clone());
        let server=tokio::spawn(async move {
            axum::serve(listener,router).await.unwrap();
        });

        let path=std::env::temp_dir().join(format!("2bm-vrooms-{}.sqlite",Uuid::new_v4()));
        let pool=crate::db::connect(&format!("sqlite://{}",path.display())).await.unwrap();
        sqlx::query("INSERT INTO accounts(id,platform,label,owner_mxid,credentials,enabled)
            VALUES ('test-account','qq','test','@owner:example.org','sealed',1)")
            .execute(&pool).await.unwrap();
        let account=crate::db::account(&pool,"test-account").await.unwrap().unwrap();
        let config=crate::config::Config {
            admin_listen:"127.0.0.1:9000".parse().unwrap(),
            matrix_listen:"127.0.0.1:9001".parse().unwrap(),
            combined_listen:None,
            database_url:format!("sqlite://{}",path.display()),
            homeserver:hs,server_name:"example.org".into(),
            as_token:"as-token".into(),hs_token:"hs-token".into(),
            admin_token:"admin-token".into(),credential_key:[0u8;32],
        };
        let state=Arc::new(crate::AppState {
            config,pool:pool.clone(),http:reqwest::Client::new(),
            telegram:crate::telegram_user::Gateway::default(),
            room_creation:tokio::sync::Mutex::new(()),
        });
        let chat=RemoteChat::new("group:987","Test Group",false,None);
        let (first,second)=tokio::join!(
            ensure(&state,&account,&chat,None),
            ensure(&state,&account,&chat,None)
        );
        assert_eq!(first.unwrap().matrix_room,"!stable:example.org");
        assert_eq!(second.unwrap().matrix_room,"!stable:example.org");
        assert_eq!(mock.creates.load(Ordering::SeqCst),1);

        // Crash after homeserver accepts createRoom but before SQLite persists:
        sqlx::query("DELETE FROM mappings WHERE account_id='test-account'")
            .execute(&state.pool).await.unwrap();
        let recovered=ensure(&state,&account,&chat,None).await.unwrap();
        assert_eq!(recovered.matrix_room,"!stable:example.org");
        assert_eq!(mock.creates.load(Ordering::SeqCst),2);
        let count:i64=sqlx::query_scalar(
            "SELECT COUNT(*) FROM mappings WHERE account_id='test-account'")
            .fetch_one(&state.pool).await.unwrap();
        assert_eq!(count,1);

        server.abort();
        drop(state);
        pool.close().await;
        let _=std::fs::remove_file(path);
    }

    #[test]
    fn alias_is_stable_scoped_and_opaque() {
        let a = alias("acct-1","group:1234");
        assert_eq!(a, alias("acct-1","group:1234"));
        assert_ne!(a, alias("acct-2","group:1234"));
        assert_ne!(a, alias("acct-1","private:1234"));
        assert!(a.starts_with("_2bm_r_"));
        assert_eq!(a.len(), 39);
        assert!(!a.contains("1234"));
    }
}
