pub mod discord;
pub mod qq;
pub mod telegram;

use crate::{crypto, db::Account, AppState};
use anyhow::{bail, Result};
use serde_json::Value;
use std::sync::Arc;

pub fn credentials(state: &Arc<AppState>, account: &Account) -> Result<Value> {
    crypto::decrypt(&state.config.credential_key, &account.credentials)
}

pub async fn send(state: &Arc<AppState>, account: &Account, chat: &str, text: &str) -> Result<()> {
    let secrets = credentials(state, account)?;
    match account.platform.as_str() {
        "telegram" => telegram::send(state, &secrets, chat, text).await,
        "telegram_user" => crate::telegram_user::send(state, &account.id, chat, text).await,
        "discord" => discord::send(state, &secrets, chat, text).await,
        "qq" => qq::send(state, &secrets, chat, text).await,
        other => bail!("unsupported adapter {other}"),
    }
}

pub fn token<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value[field].as_str()
        .filter(|t| !t.is_empty())
        .ok_or_else(|| anyhow::anyhow!("missing account credential {field}"))
}
