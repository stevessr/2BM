use crate::{relay, AppState};
use anyhow::{bail, Context, Result};
use grammers_client::{
    client::{LoginToken, PasswordToken, UpdatesConfiguration},
    update::Update,
    Client, SignInError,
};
use grammers_mtsender::SenderPool;
use grammers_session::{storages::SqliteSession, types::PeerId};
use serde_json::Value;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tokio::{sync::{Mutex, RwLock}, task::JoinHandle};

#[derive(Default)]
pub struct Gateway {
    pub live: RwLock<HashMap<String, Client>>,
    pending: Mutex<HashMap<String, Pending>>,
}
struct Pending {
    client: Client,
    runner: JoinHandle<()>,
    phase: LoginPhase,
}
enum LoginPhase {
    Code(LoginToken),
    Password(PasswordToken),
}
pub fn api_id(credentials: &Value) -> Result<i32> {
    let id = credentials["api_id"].as_i64().context("Telegram API ID missing")?;
    Ok(i32::try_from(id).context("Telegram API ID is out of range")?)
}
fn session_path(account_id: &str) -> Result<PathBuf> {
    let uuid = uuid::Uuid::parse_str(account_id).context("invalid account ID")?;
    let base = std::env::var("BRIDGE_TG_SESSION_DIR")
        .unwrap_or_else(|_| "data/telegram".into());
    Ok(PathBuf::from(base).join(format!("{uuid}.sqlite")))
}
pub async fn begin(state: &Arc<AppState>, account_id: &str, phone: &str) -> Result<&'static str> {
    let account = crate::db::account(&state.pool, account_id).await?
        .filter(|a| a.platform == "telegram_user")
        .context("Telegram personal account not found")?;
    if state.telegram.pending.lock().await.contains_key(account_id) {
        bail!("Telegram authentication already started");
    }
    let creds = crate::adapters::credentials(state, &account)?;
    let hash = crate::adapters::token(&creds, "api_hash")?;
    let path = session_path(account_id)?;
    if let Some(parent) = path.parent() { tokio::fs::create_dir_all(parent).await?; }
    let session = Arc::new(SqliteSession::open(path).await?);
    let SenderPool { runner, handle, .. } = SenderPool::new(session, api_id(&creds)?);
    let client = Client::new(handle);
    let runner = tokio::spawn(async move { let _ = runner.run().await; });
    if client.is_authorized().await? {
        sqlx::query("UPDATE accounts SET enabled = 1 WHERE id = ?")
            .bind(account_id).execute(&state.pool).await?;
        runner.abort();
        return Ok("ready");
    }
    let token = client.request_login_code(phone, hash).await?;
    state.telegram.pending.lock().await.insert(account_id.to_string(),
        Pending { client, runner, phase: LoginPhase::Code(token) });
    Ok("code_sent")
}
pub async fn verify(
    state: &Arc<AppState>, account_id: &str, value: &str
) -> Result<&'static str> {
    let mut pending = state.telegram.pending.lock().await.remove(account_id)
        .context("No pending Telegram authentication; request a code first")?;
    let result = match pending.phase {
        LoginPhase::Code(token) => {
            match pending.client.sign_in(&token, value).await {
                Ok(_) => Ok("ready"),
                Err(SignInError::PasswordRequired(password)) => {
                    pending.phase = LoginPhase::Password(password);
                    state.telegram.pending.lock().await.insert(account_id.to_string(), pending);
                    return Ok("password_required");
                }
                Err(error) => {
                    pending.phase = LoginPhase::Code(token);
                    state.telegram.pending.lock().await.insert(account_id.to_string(), pending);
                    return Err(anyhow::anyhow!("Telegram code verification failed: {error}"));
                }
            }
        }
        LoginPhase::Password(password) => {
            pending.client.check_password(password, value).await
                .map(|_| "ready")
                .map_err(|e| anyhow::anyhow!("Telegram 2FA verification failed: {e}"))
        }
    };
    result?;
    sqlx::query("UPDATE accounts SET enabled = 1 WHERE id = ?")
        .bind(account_id).execute(&state.pool).await?;
    pending.runner.abort();
    Ok("ready")
}
pub async fn send(state: &Arc<AppState>, account_id: &str, chat: &str, text: &str) -> Result<()> {
    let chat_id: i64 = chat.parse().context("invalid Telegram chat ID")?;
    let peer = PeerId::from_bot_api_dialog_id(chat_id)
        .context("invalid Telegram peer ID")?;
    let client = state.telegram.live.read().await.get(account_id)
        .cloned().context("Telegram user session is not online")?;
    let resolved = client.resolve_peer(peer.to_ambient_ref()).await?;
    let reference = resolved.to_ref().await?
        .context("Telegram peer is not accessible with this account")?;
    client.send_message(reference, text).await?;
    Ok(())
}
pub async fn run(state: &Arc<AppState>, account_id: &str, credentials: &Value) -> Result<()> {
    let path = session_path(account_id)?;
    let session = Arc::new(SqliteSession::open(path).await?);
    let SenderPool { runner, updates, handle } =
        SenderPool::new(session, api_id(credentials)?);
    let client = Client::new(handle.clone());
    let pool_task = tokio::spawn(async move { let _ = runner.run().await; });
    if !client.is_authorized().await? { bail!("Telegram session is not authorized"); }
    // Populate peer cache for chats so sending back to contacts has the access hash.
    let mut dialogs = client.iter_dialogs();
    while let Some(_) = dialogs.next().await? {}
    state.telegram.live.write().await.insert(account_id.into(), client.clone());
    let mut updates = client.stream_updates(
        updates, UpdatesConfiguration { catch_up: true, ..Default::default() }
    ).await?;
    let result: Result<()> = loop {
        match updates.next().await {
            Ok(Update::NewMessage(message)) if !message.outgoing() => {
                if let Some(chat) = message.peer_id().bot_api_dialog_id() {
                    let sender = message.sender()
                        .and_then(|p| p.name())
                        .unwrap_or("Telegram contact");
                    let sender_id = message.sender_id()
                        .and_then(|p| p.bot_api_dialog_id())
                        .unwrap_or(chat);
                    if let Err(e) = relay::incoming(
                        state, account_id, &chat.to_string(), &message.id().to_string(),
                        &sender_id.to_string(), sender, message.text()
                    ).await {
                        tracing::warn!(account=%account_id,%e,"MTProto event relaying failed");
                    }
                }
            }
            Ok(_) => {}
            Err(error) => break Err(error.into()),
        }
    };
    state.telegram.live.write().await.remove(account_id);
    if let Err(error) = updates.sync_update_state().await {
        tracing::warn!(%error, "unable to persist Telegram update state");
    }
    handle.quit();
    let _ = pool_task.await;
    result
}
