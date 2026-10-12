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
    sqlx::query("INSERT INTO virtual_chats(account_id,external_chat,title,direct)
        VALUES (?,?,?,?)
        ON CONFLICT(account_id,external_chat) DO UPDATE SET
          title = CASE WHEN excluded.title = virtual_chats.external_chat
               THEN virtual_chats.title ELSE excluded.title END,
          direct = excluded.direct, updated_at = CURRENT_TIMESTAMP")
        .bind(&account.id).bind(&chat.external_chat).bind(&title).bind(chat.direct)
        .execute(&state.pool).await?;

    let mapping = if let Some(existing) =
        db::mapping_for_chat(&state.pool, &account.id, &chat.external_chat).await? {
        existing
    } else {
        let initial_ghost = if chat.direct {
            if let Some(contact) = chat.contact_id.as_deref() {
                Some(matrix::ghost(state, &account.id, contact, &title).await?)
            } else if let Some((sender_id, name)) = sender {
                Some(matrix::ghost(state, &account.id, sender_id, name).await?)
            } else {
                None
            }
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
    if let Some((sender_id, name)) = sender {
        let ghost = matrix::ghost(state, &account.id, sender_id, name).await?;
        matrix::ensure_ghost_in_room(state, &mapping.matrix_room, &ghost).await?;
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
    let mut report = SyncReport { discovered, provisioned: 0, failed: 0 };
    for chat in conversations.into_iter().take(limit) {
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
