use anyhow::Result;
use serde::Serialize;
use sqlx::{sqlite::{SqliteConnectOptions, SqlitePoolOptions}, FromRow, SqlitePool};
use std::str::FromStr;

#[derive(Debug, Clone, FromRow)]
pub struct Account {
    pub id: String,
    pub platform: String,
    pub label: String,
    pub credentials: String,
    pub enabled: bool,
}

#[derive(Debug, Serialize, FromRow)]
pub struct AccountPublic {
    pub id: String,
    pub platform: String,
    pub label: String,
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
            credentials TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1
        )",
        "CREATE TABLE IF NOT EXISTS mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT, account_id TEXT NOT NULL
            REFERENCES accounts(id) ON DELETE CASCADE,
            external_chat TEXT NOT NULL, matrix_room TEXT NOT NULL,
            UNIQUE(account_id, external_chat)
        )",
        "CREATE INDEX IF NOT EXISTS idx_mappings_room ON mappings(matrix_room)",
        "CREATE TABLE IF NOT EXISTS incoming (
            source_key TEXT PRIMARY KEY, delivered_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
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
        "SELECT id,platform,label,credentials,enabled FROM accounts ORDER BY label"
    ).fetch_all(db).await?)
}

pub async fn account(db: &SqlitePool, id: &str) -> Result<Option<Account>> {
    Ok(sqlx::query_as::<_, Account>(
        "SELECT id,platform,label,credentials,enabled FROM accounts WHERE id = ?"
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
