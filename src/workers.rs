use crate::{adapters, db, AppState};
use std::{collections::{HashMap, HashSet}, sync::Arc, time::Duration};
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub async fn set_status(state:&Arc<AppState>, account_id:&str, status:&str)
    ->anyhow::Result<()> {
    sqlx::query("INSERT INTO adapter_status(account_id,state) VALUES (?,?)
        ON CONFLICT(account_id) DO UPDATE SET state=excluded.state,
        updated_at=CURRENT_TIMESTAMP")
        .bind(account_id).bind(status).execute(&state.pool).await?;
    Ok(())
}
pub async fn run(state: Arc<AppState>) {
    let mut jobs: HashMap<String, (String, JoinHandle<()>)> = HashMap::new();
    loop {
        match db::accounts(&state.pool).await {
            Ok(accounts) => {
                let active: HashMap<String, String> = accounts.iter()
                    .filter(|a| a.enabled && matches!(a.platform.as_str(), "telegram" | "telegram_user" | "discord" | "qq"))
                    .map(|a| (a.id.clone(), format!("{}:{}", a.platform, a.credentials)))
                    .collect();
                let stale: HashSet<_> = jobs.iter()
                    .filter(|(id, (signature, task))|
                        active.get(*id) != Some(signature) || task.is_finished())
                    .map(|(id, _)| id.clone())
                    .collect();
                for id in stale {
                    if let Some((_, task)) = jobs.remove(&id) { task.abort(); }
                    state.telegram.live.write().await.remove(&id);
                    if let Err(error)=set_status(&state,&id,"stopped").await {
                        warn!(account=%id,%error,"unable to update adapter state");
                    }
                }
                for account in accounts {
                    if !active.contains_key(&account.id) || jobs.contains_key(&account.id) { continue; }
                    let secrets = match adapters::credentials(&state, &account) {
                        Ok(secrets) => secrets,
                        Err(error) => { warn!(account = %account.id, %error, "invalid credentials"); continue; }
                    };
                    info!(platform = %account.platform, account = %account.id, "starting adapter");
                    let _=set_status(&state,&account.id,"connecting").await;
                    let signature = active[&account.id].clone();
                    let id = account.id.clone();
                    let service = state.clone();
                    let task = tokio::spawn(async move {
                        loop {
                            let result = match account.platform.as_str() {
                                "telegram" => adapters::telegram::poll(&service, &id, &secrets).await,
                                "telegram_user" => crate::telegram_user::run(&service, &id, &secrets).await,
                                "discord" => adapters::discord::gateway(&service, &id, &secrets).await,
                                "qq" => {
                                    let result = match db::account(&service.pool, &id).await {
                                        Ok(Some(account)) => crate::virtual_rooms::sync(
                                            &service, &account, 30).await.map(|_| ()),
                                        Ok(None) => Ok(()),
                                        Err(error) => Err(error),
                                    };
                                    // A successful discovery means the QQ API is
                                    // reachable; don't show "connecting" for ten minutes.
                                    let _=set_status(&service,&id,
                                        if result.is_ok(){"online"}else{"error"}).await;
                                    tokio::time::sleep(Duration::from_secs(600)).await;
                                    result
                                },
                                _ => break,
                            };
                            match result {
                                Ok(())=>{
                                    let _=set_status(&service,&id,"online").await;
                                }
                                Err(_error)=>{
                                    // Some APIs embed their tokens in URL path components.
                                    // Do not leak reqwest errors (including Telegram Bot
                                    // endpoint credentials) to untrusted action logs.
                                    let _=set_status(&service,&id,"error").await;
                                    warn!(platform = %account.platform, account = %id,
                                        "adapter disconnected; check credentials and network");
                                }
                            }
                            tokio::time::sleep(Duration::from_secs(8)).await;
                        }
                    });
                    jobs.insert(account.id, (signature, task));
                }
            }
            Err(error) => warn!(%error, "adapter reconciliation failed"),
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}
