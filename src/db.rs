use anyhow::Result;
use serde::Serialize;
use sqlx::{sqlite::{SqliteConnectOptions, SqlitePoolOptions}, FromRow, SqlitePool};
use std::str::FromStr;

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
    let options = SqliteConnectOptions::from_str(database_url)?
        .create_if_missing(true)
        .foreign_keys(true);
    let db = SqlitePoolOptions::new().max_connections(5).connect_with(options).await?;
    for statement in [
        "PRAGMA journal_mode=WAL",
        "CREATE TABLE IF NOT EXISTS accounts (
            id TEXT PRIMARY KEY, platform TEXT NOT NULL, label TEXT NOT NULL,
            owner_mxid TEXT NOT NULL DEFAULT '', credentials TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1
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
        "CREATE TABLE IF NOT EXISTS outgoing (
            event_key TEXT PRIMARY KEY, delivered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
        "CREATE TABLE IF NOT EXISTS telegram_offsets (
            account_id TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
            next_offset INTEGER NOT NULL DEFAULT 0
        )",
        "CREATE TABLE IF NOT EXISTS transactions (
            txn_id TEXT PRIMARY KEY, handled_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )",
    ] {
        sqlx::query(statement).execute(&db).await?;
    }
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
