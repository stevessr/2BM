mod adapters;
mod config;
mod crypto;
mod db;
mod matrix;
mod telegram_user;
mod relay;
mod web;
mod virtual_rooms;
mod workers;

use anyhow::Result;
use axum::Router;
use config::Config;
use sqlx::SqlitePool;
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tracing::info;

pub struct AppState {
    pub config: Config,
    pub pool: SqlitePool,
    pub http: reqwest::Client,
    pub telegram: telegram_user::Gateway,
    pub room_creation: tokio::sync::Mutex<()>,
    /// Serializes outbound remote sends against inbound self-message echoes.
    pub delivery_lock: tokio::sync::Mutex<()>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "two_bm=info,tower_http=info".into()))
        .init();

    let config = Config::from_env()?;
    if let Some(path) = config.database_url.strip_prefix("sqlite://") {
        if let Some(dir) = std::path::Path::new(path).parent() {
            if !dir.as_os_str().is_empty() { std::fs::create_dir_all(dir)?; }
        }
    }
    let pool = db::connect(&config.database_url).await?;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(35))
        .user_agent("2BM/0.1")
        .build()?;
    let state = Arc::new(AppState { config, pool, http, telegram: telegram_user::Gateway::default(), room_creation: tokio::sync::Mutex::new(()), delivery_lock: tokio::sync::Mutex::new(()) });

    // A service that serves 200 /healthz without a functional appservice
    // silently loses messages. Fail startup until the homeserver registration
    // is correct (container restart policy retries after transient outages).
    matrix::register_bot(&state).await?;
    tokio::spawn(workers::run(state.clone()));

    let config = &state.config;
    if let Some(listen) = config.combined_listen.or_else(|| {
        (config.admin_listen == config.matrix_listen).then_some(config.admin_listen)
    }) {
        info!(%listen, "combined listener");
        serve(listen, web::admin_router().merge(web::matrix_router()).with_state(state)).await?;
    } else {
        let admin_addr = config.admin_listen;
        let matrix_addr = config.matrix_listen;
        info!(%admin_addr, %matrix_addr, "separate listeners");
        let admin_state = state.clone();
        let admin = tokio::spawn(async move {
            serve(admin_addr, web::admin_router().with_state(admin_state)).await
        });
        serve(matrix_addr, web::matrix_router().with_state(state)).await?;
        admin.await??;
    }
    Ok(())
}

async fn serve(addr: SocketAddr, router: Router) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;
    Ok(())
}
