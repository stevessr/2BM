use crate::{relay, virtual_rooms::RemoteChat, AppState};
use anyhow::{bail, Context, Result};
use grammers_client::{
    client::{LoginToken, PasswordToken, UpdatesConfiguration},
    update::Update,
    Client, SignInError,
};
use grammers_mtsender::SenderPool;
use grammers_session::{storages::SqliteSession, types::{PeerId, PeerKind}};
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

/// Restrict Telegram MTProto authorization keys to the bridge's Unix account.
/// These files remain unencrypted: access to this directory grants account
/// access. Deploy on a private filesystem and encrypt storage/backups.
async fn prepare_session_path(path:&std::path::Path)->Result<()>{
    if let Some(parent)=path.parent(){
        tokio::fs::create_dir_all(parent).await?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            tokio::fs::set_permissions(parent,std::fs::Permissions::from_mode(0o700)).await?;
        }
    }
    Ok(())
}
async fn protect_session_file(path:&std::path::Path)->Result<()>{
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(path,std::fs::Permissions::from_mode(0o600)).await?;
    }
    #[cfg(not(unix))]
    { let _=path; }
    Ok(())
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
    prepare_session_path(&path).await?;
    let session = Arc::new(SqliteSession::open(&path).await?);
    protect_session_file(&path).await?;
    let SenderPool { runner, handle, .. } = SenderPool::new(session, api_id(&creds)?);
    let client = Client::new(handle);
    let runner = tokio::spawn(async move { let _ = runner.run().await; });
    let authorized=match client.is_authorized().await {
        Ok(auth)=>auth,
        Err(error)=>{runner.abort();return Err(error.into());}
    };
    if authorized {
        sqlx::query("UPDATE accounts SET enabled = 1 WHERE id = ?")
            .bind(account_id).execute(&state.pool).await?;
        runner.abort();
        return Ok("ready");
    }
    let token = match client.request_login_code(phone, hash).await {
        Ok(token)=>token,
        Err(error)=>{runner.abort();return Err(error.into());}
    };
    state.telegram.pending.lock().await.insert(account_id.to_string(),
        Pending { client, runner, phase: LoginPhase::Code(token) });
    Ok("code_sent")
}
pub async fn verify(
    state: &Arc<AppState>, account_id: &str, value: &str
) -> Result<&'static str> {
    let mut pending = state.telegram.pending.lock().await.remove(account_id)
        .context("No pending Telegram authentication; request a code first")?;
    let result: Result<&'static str> = match pending.phase {
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
            match pending.client.check_password(password, value).await {
                Ok(_) => Ok("ready"),
                Err(SignInError::InvalidPassword(next_token)) => {
                    pending.phase=LoginPhase::Password(next_token);
                    state.telegram.pending.lock().await
                        .insert(account_id.to_owned(),pending);
                    return Err(anyhow::anyhow!("incorrect Telegram 2FA password; try again"));
                }
                Err(error) => {
                    pending.runner.abort();
                    return Err(anyhow::anyhow!("Telegram 2FA verification failed: {error}"));
                }
            }
        }
    };
    result?;
    sqlx::query("UPDATE accounts SET enabled = 1 WHERE id = ?")
        .bind(account_id).execute(&state.pool).await?;
    pending.runner.abort();
    Ok("ready")
}

pub async fn logout(state:&Arc<AppState>,account_id:&str)->Result<()> {
    let account=crate::db::account(&state.pool,account_id).await?
        .filter(|a|a.platform=="telegram_user")
        .context("Telegram personal account not found")?;
    if !account.enabled { return Ok(()); }
    let client=state.telegram.live.read().await.get(account_id).cloned()
        .context("Telegram account must be online to revoke the remote session")?;
    tokio::time::timeout(std::time::Duration::from_secs(20),client.sign_out())
        .await.context("Telegram logout timed out; remote outcome is unknown")??;
    sqlx::query("UPDATE accounts SET enabled=0 WHERE id=?")
        .bind(account_id).execute(&state.pool).await?;
    client.disconnect();
    state.telegram.live.write().await.remove(account_id);
    Ok(())
}

pub async fn purge_local_session(state:&Arc<AppState>,account_id:&str)->Result<()> {
    if let Some(pending)=state.telegram.pending.lock().await.remove(account_id) {
        pending.runner.abort();
    }
    let path=session_path(account_id)?;
    let files=[
        path.clone(),
        std::path::PathBuf::from(format!("{}-wal",path.display())),
        std::path::PathBuf::from(format!("{}-shm",path.display())),
    ];
    for file in files {
        match tokio::fs::remove_file(&file).await {
            Ok(())=>{},
            Err(err) if err.kind()==std::io::ErrorKind::NotFound=>{},
            Err(err)=>return Err(err.into()),
        }
    }
    Ok(())
}

pub async fn send(state: &Arc<AppState>, account_id: &str, chat: &str, text: &str) -> Result<()> {
    send_with_id(state,account_id,chat,text).await.map(|_|())
}
pub async fn send_with_id(state: &Arc<AppState>, account_id: &str, chat: &str, text: &str) -> Result<i32> {
    let chat_id: i64 = chat.parse().context("invalid Telegram chat ID")?;
    let peer = PeerId::from_bot_api_dialog_id(chat_id)
        .context("invalid Telegram peer ID")?;
    let client = state.telegram.live.read().await.get(account_id)
        .cloned().context("Telegram user session is not online")?;
    let resolved = client.resolve_peer(peer.to_ambient_ref()).await?;
    let reference = resolved.to_ref().await
        .map_err(|e| anyhow::anyhow!("unable to resolve peer reference: {e}"))?
        .context("Telegram peer is not accessible with this account")?;
    let message=client.send_message(reference, text).await?;
    Ok(message.id())
}

pub async fn list_chats(state: &Arc<AppState>, account_id: &str) -> Result<Vec<RemoteChat>> {
    let client = state.telegram.live.read().await.get(account_id)
        .cloned().context("Telegram user session is not online")?;
    let mut iter = client.iter_dialogs();
    let mut chats = Vec::new();
    // Bound expensive Matrix provisioning and MTProto iteration for each sync.
    while chats.len() < 200 {
        let Some(dialog) = iter.next().await? else { break; };
        let peer = dialog.peer();
        let id = peer.id();
        let Some(chat_id) = id.bot_api_dialog_id() else { continue; };
        let direct = id.kind() == PeerKind::User;
        let title = peer.name().unwrap_or("Telegram chat").to_owned();
        chats.push(RemoteChat::new(
            chat_id.to_string(), title, direct,
            if direct { Some(chat_id.to_string()) } else { None }
        ));
    }
    Ok(chats)
}


#[derive(serde::Serialize)]
pub struct HistoryImportReport {
    pub fetched: usize,
    pub imported: usize,
    pub already_present: usize,
    pub skipped_empty: usize,
    pub failed: usize,
}
/// Opt-in recent-message import. These are ordinary Matrix messages sent
/// now, NOT Matrix historical events retaining original Telegram timestamps.
pub async fn import_recent(
    state:&Arc<AppState>, account:&crate::db::Account, chat:&str,limit:usize
)->Result<HistoryImportReport>{
    if account.platform!="telegram_user" { bail!("Telegram personal account required"); }
    if crate::db::mapping_for_chat(&state.pool,&account.id,chat).await?.is_none() {
        bail!("the Matrix virtual room does not exist; synchronize chats first");
    }
    let id:i64=chat.parse().context("Telegram chat must be a numeric dialog ID")?;
    let peer=PeerId::from_bot_api_dialog_id(id).context("invalid Telegram peer ID")?;
    let client=state.telegram.live.read().await.get(&account.id).cloned()
        .context("Telegram personal session is not online")?;
    let resolved=client.resolve_peer(peer.to_ambient_ref()).await?;
    let reference=resolved.to_ref().await
        .map_err(|e|anyhow::anyhow!("Telegram peer reference: {e}"))?
        .context("Telegram dialog reference is unavailable")?;
    let mut iterator=client.iter_messages(reference).limit(limit.clamp(1,50));
    let mut history=Vec::new();
    while let Some(message)=iterator.next().await? {
        history.push(message);
    }
    let mut report=HistoryImportReport {
        fetched:history.len(),imported:0,already_present:0,skipped_empty:0,failed:0
    };
    // Telegram API lists newest-first; deliver oldest-first to the Matrix timeline.
    for message in history.into_iter().rev() {
        let text=message.text();
        if text.trim().is_empty() { report.skipped_empty+=1;continue; }
        let key=format!("{}:{}:{}",account.id,chat,message.id());
        if crate::db::has_key(&state.pool,"incoming","source_key",&key).await? {
            report.already_present+=1;continue;
        }
        let sender_id=if message.outgoing() {
            format!("telegram-self:{}",account.id)
        } else {
            message.sender_id().and_then(|p|p.bot_api_dialog_id())
                .unwrap_or(id).to_string()
        };
        let name=if message.outgoing() {"Me (Telegram)"}
            else {message.sender().and_then(|p|p.name()).unwrap_or("Telegram contact")};
        match relay::incoming(state,&account.id,chat,&message.id().to_string(),
            &sender_id,name,text).await {
            Ok(())=>report.imported+=1,
            Err(error)=>{
                report.failed+=1;
                tracing::warn!(account=%account.id,chat,message=message.id(),%error,"Telegram recent import failed");
            }
        }
    }
    Ok(report)
}

pub async fn run(state: &Arc<AppState>, account_id: &str, credentials: &Value) -> Result<()> {
    let path = session_path(account_id)?;
    prepare_session_path(&path).await?;
    let session = Arc::new(SqliteSession::open(&path).await?);
    protect_session_file(&path).await?;
    let SenderPool { runner, updates, handle } =
        SenderPool::new(session, api_id(credentials)?);
    let client = Client::new(handle.clone());
    let pool_task = tokio::spawn(async move { let _ = runner.run().await; });
    if !client.is_authorized().await? {
        handle.quit();
        let _=pool_task.await;
        bail!("Telegram session is not authorized");
    }
    // Populate peer cache for chats so sending back to contacts has the access hash.
    let mut dialogs = client.iter_dialogs();
    while let Some(_) = dialogs.next().await? {}
    state.telegram.live.write().await.insert(account_id.into(), client.clone());
    let _=crate::workers::set_status(state,account_id,"online").await;
    // Existing chats become Matrix rooms even before the next external message.
    // Run asynchronously so update processing is not blocked by Matrix requests.
    let service = state.clone();
    let sync_id = account_id.to_string();
    tokio::spawn(async move {
        if let Ok(Some(account)) = crate::db::account(&service.pool,&sync_id).await {
            if let Err(error) = crate::virtual_rooms::sync(&service,&account,30).await {
                tracing::warn!(account=%sync_id,%error,"Telegram virtual room discovery failed");
            }
        }
    });
    let mut updates = client.stream_updates(
        updates, UpdatesConfiguration { catch_up: true, ..Default::default() }
    ).await.map_err(|e| anyhow::anyhow!("unable to initialize MTProto update stream: {e}"))?;
    let result: Result<()> = loop {
        match updates.next().await {
            Ok(Update::NewMessage(message)) => {
                if let Some(chat) = message.peer_id().bot_api_dialog_id() {
                    let chat_str=chat.to_string();
                    let msg=message.id().to_string();
                    let result=if message.outgoing() {
                        relay::incoming_own(state,account_id,&chat_str,&msg,
                            &format!("telegram-self:{account_id}"),
                            "Me (Telegram)",message.text()).await
                    } else {
                        let sender=message.sender().and_then(|p|p.name())
                            .unwrap_or("Telegram contact");
                        let sender_id=message.sender_id()
                            .and_then(|p|p.bot_api_dialog_id()).unwrap_or(chat);
                        let reply_id=message.reply_to_message_id()
                            .map(|id|id.to_string());
                        relay::incoming_with_reply(state,account_id,&chat_str,&msg,
                            &sender_id.to_string(),sender,message.text(),
                            reply_id.as_deref()).await
                    };
                    if let Err(e)=result {
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
