use anyhow::Result;
use serde::Serialize;
use sqlx::{sqlite::{SqliteConnectOptions, SqlitePoolOptions,
    SqliteJournalMode, SqliteSynchronous}, FromRow, SqlitePool};
use std::{str::FromStr, time::Duration};

#[derive(Debug, Clone, FromRow)]
pub struct Account {
    pub id: String,
    pub platform: String,
    pub label: String,
    pub owner_mxid: String,
    pub credentials: String,
    pub enabled: bool,
}

#[derive(Debug, Serialize, FromRow)]
pub struct AccountPublic {
    pub id: String,
    pub platform: String,
    pub label: String,
    pub owner_mxid: String,
    pub enabled: bool,
}

#[derive(Debug, Serialize, FromRow)]
pub struct Mapping {
    pub id: i64,
    pub account_id: String,
    pub external_chat: String,
    pub matrix_room: String,
}

pub async fn connect(database_url: &str) -> Result<SqlitePool> {
    // Per-connection WAL and FULL synchronous are required for our
    // acknowledged QQ webhook inbox and outbound delivery journal.
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true)
        .foreign_keys(true)
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Full)
        .busy_timeout(Duration::from_secs(10));
    let db = SqlitePoolOptions::new().max_connections(5).connect_with(options).await?;
    for statement in [
        "PRAGMA wal_autocheckpoint=1000",
        "PRAGMA journal_size_limit=67108864",
        "CREATE TABLE IF NOT EXISTS accounts (
            id TEXT PRIMARY KEY, platform TEXT NOT NULL, label TEXT NOT NULL,
            owner_mxid TEXT NOT NULL DEFAULT '', credentials TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1
        )",
        "CREATE TABLE IF NOT EXISTS adapter_status (
            account_id TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
            state TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE IF NOT EXISTS mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT NOT NULL
            REFERENCES accounts(id) ON DELETE CASCADE,
            external_chat TEXT NOT NULL, matrix_room TEXT NOT NULL,
            UNIQUE(account_id, external_chat)
        )",
        "CREATE INDEX IF NOT EXISTS idx_mappings_room ON mappings(matrix_room)",
        "CREATE TABLE IF NOT EXISTS virtual_chats (
            account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            external_chat TEXT NOT NULL, title TEXT NOT NULL,
            direct INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY(account_id,external_chat)
        )",
        "CREATE TABLE IF NOT EXISTS ghost_users (
            mxid TEXT PRIMARY KEY,
            account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            contact_id TEXT NOT NULL,
            display_name TEXT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS qq_synced_members (
            account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            external_chat TEXT NOT NULL,
            contact_id TEXT NOT NULL,
            PRIMARY KEY(account_id,external_chat,contact_id)
        )",
        "CREATE TABLE IF NOT EXISTS incoming (
            source_key TEXT PRIMARY KEY, delivered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE IF NOT EXISTS room_warnings (
            matrix_room TEXT PRIMARY KEY,
            reason TEXT NOT NULL,
            event_count INTEGER NOT NULL DEFAULT 1,
            first_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE IF NOT EXISTS ghost_room_memberships (
            matrix_room TEXT NOT NULL,
            ghost_mxid TEXT NOT NULL,
            PRIMARY KEY(matrix_room,ghost_mxid)
        )",
        "CREATE TABLE IF NOT EXISTS event_links (
            account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            external_chat TEXT NOT NULL,
            external_message TEXT NOT NULL,
            matrix_room TEXT NOT NULL,
            matrix_event TEXT NOT NULL,
            sender_mxid TEXT NOT NULL,
            redacted INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY(account_id,external_chat,external_message)
        )",
        "CREATE INDEX IF NOT EXISTS idx_event_links_matrix
            ON event_links(matrix_room,matrix_event)",
        "CREATE TABLE IF NOT EXISTS recall_tombstones (
            account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            external_chat TEXT NOT NULL,
            external_message TEXT NOT NULL,
            PRIMARY KEY(account_id,external_chat,external_message)
        )",
        "CREATE TABLE IF NOT EXISTS delivery_attempts (
            event_key TEXT PRIMARY KEY,
            account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            matrix_event TEXT NOT NULL,
            external_chat TEXT NOT NULL,
            status TEXT NOT NULL CHECK(status IN ('sending','uncertain','delivered')),
            last_error TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE INDEX IF NOT EXISTS idx_delivery_unresolved
            ON delivery_attempts(status,updated_at)",
        "CREATE TABLE IF NOT EXISTS outgoing (
            event_key TEXT PRIMARY KEY, delivered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE IF NOT EXISTS telegram_offsets (
            account_id TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
            next_offset INTEGER NOT NULL DEFAULT 0
        )",
        "CREATE TABLE IF NOT EXISTS qq_own_inbox (
            source_key TEXT PRIMARY KEY,
            account_id TEXT NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
            sealed TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0,
            received_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            next_attempt_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            last_error TEXT
        )",
        "CREATE INDEX IF NOT EXISTS idx_qq_own_inbox_received
            ON qq_own_inbox(received_at)",
        "CREATE TABLE IF NOT EXISTS appservice_events (
            event_id TEXT PRIMARY KEY,
            handled_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE IF NOT EXISTS transactions (
            txn_id TEXT PRIMARY KEY, handled_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
    ] {
        sqlx::query(statement).execute(&db).await?;
    }
    // Allow upgrading a beta database that created the encrypted inbox
    // before per-message backoff was introduced.
    use sqlx::Row;
    let cols=sqlx::query("PRAGMA table_info('qq_own_inbox')")
        .fetch_all(&db).await?;
    if !cols.iter().any(|row|row.get::<String,_>("name")=="next_attempt_at") {
        // SQLite cannot ADD COLUMN with a nonconstant default. Existing queued
        // entries become immediately eligible for retry on migration.
        sqlx::query("ALTER TABLE qq_own_inbox ADD COLUMN next_attempt_at
            TEXT NOT NULL DEFAULT '1970-01-01 00:00:00'").execute(&db).await?;
    }
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_qq_own_inbox_next
        ON qq_own_inbox(next_attempt_at,received_at)").execute(&db).await?;
    Ok(db)
}

pub async fn accounts(db: &SqlitePool) -> Result<Vec<Account>> {
    Ok(sqlx::query_as::<_, Account>(
        "SELECT id,platform,label,owner_mxid,credentials,enabled FROM accounts ORDER BY label"
    ).fetch_all(db).await?)
}

pub async fn account(db: &SqlitePool, id: &str) -> Result<Option<Account>> {
    Ok(sqlx::query_as::<_, Account>(
        "SELECT id,platform,label,owner_mxid,credentials,enabled FROM accounts WHERE id = ?"
    ).bind(id).fetch_optional(db).await?)
}

pub async fn mappings_for_room(db: &SqlitePool, room: &str) -> Result<Vec<Mapping>> {
    Ok(sqlx::query_as::<_, Mapping>(
        "SELECT id,account_id,external_chat,matrix_room FROM mappings WHERE matrix_room = ?"
    ).bind(room).fetch_all(db).await?)
}

pub async fn mapping_for_chat(db: &SqlitePool, account: &str, chat: &str) -> Result<Option<Mapping>> {
    Ok(sqlx::query_as::<_, Mapping>(
        "SELECT id,account_id,external_chat,matrix_room FROM mappings
         WHERE account_id = ? AND external_chat = ?"
    ).bind(account).bind(chat).fetch_optional(db).await?)
}

pub async fn has_key(db: &SqlitePool, table: &str, field: &str, key: &str) -> Result<bool> {
    let sql = match (table, field) {
        ("incoming", "source_key") => "SELECT 1 FROM incoming WHERE source_key = ?",
        ("outgoing", "event_key") => "SELECT 1 FROM outgoing WHERE event_key = ?",
        ("transactions", "txn_id") => "SELECT 1 FROM transactions WHERE txn_id = ?",
        _ => anyhow::bail!("invalid deduplication table"),
    };
    Ok(sqlx::query_scalar::<_, i64>(sql).bind(key).fetch_optional(db).await?.is_some())
}


#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn every_sqlite_connection_uses_durable_full_sync(){
        let path=std::env::temp_dir().join(format!(
            "2bm-db-durable-{}.sqlite",uuid::Uuid::new_v4()));
        let pool=connect(&format!("sqlite://{}",path.display())).await.unwrap();
        let mut connections=Vec::new();
        for _ in 0..5{
            let conn=pool.acquire().await.unwrap();
            connections.push(conn);
        }
        for mut conn in connections {
            let level:i64=sqlx::query_scalar("PRAGMA synchronous")
                .fetch_one(&mut *conn).await.unwrap();
            assert_eq!(level,2,"FULL synchronous must apply per connection");
            let fks:i64=sqlx::query_scalar("PRAGMA foreign_keys")
                .fetch_one(&mut *conn).await.unwrap();
            assert_eq!(fks,1);
        }
        pool.close().await;
        let _=std::fs::remove_file(path);
    }
}
