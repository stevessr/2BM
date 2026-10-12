use anyhow::{bail, Context, Result};
use std::{env, net::SocketAddr};

#[derive(Clone)]
pub struct Config {
    pub admin_listen: SocketAddr,
    pub matrix_listen: SocketAddr,
    pub combined_listen: Option<SocketAddr>,
    pub database_url: String,
    pub homeserver: String,
    pub server_name: String,
    pub as_token: String,
    pub hs_token: String,
    pub admin_token: String,
    pub credential_key: [u8; 32],
}

impl Config {
    pub fn from_env() -> Result<Self> {
        let admin_listen = listen("BRIDGE_ADMIN_LISTEN", "127.0.0.1:9000")?;
        let matrix_listen = listen("BRIDGE_MATRIX_LISTEN", "127.0.0.1:9001")?;
        let combined_listen = env::var("BRIDGE_COMBINED_LISTEN")
            .ok()
            .filter(|s| !s.is_empty())
            .map(|s| s.parse().context("invalid BRIDGE_COMBINED_LISTEN"))
            .transpose()?;
        let homeserver = required("BRIDGE_MATRIX_HOMESERVER")?;
        if !(homeserver.starts_with("http://") || homeserver.starts_with("https://")) {
            bail!("BRIDGE_MATRIX_HOMESERVER must be an HTTP(S) URL");
        }
        let server_name = required("BRIDGE_MATRIX_SERVER_NAME")?;
        let as_token = required("BRIDGE_MATRIX_AS_TOKEN")?;
        let hs_token = required("BRIDGE_MATRIX_HS_TOKEN")?;
        let admin_token = required("BRIDGE_ADMIN_TOKEN")?;
        if admin_token.len() < 24 || as_token.len() < 24 || hs_token.len() < 24 {
            bail!("admin/as/hs tokens must each contain at least 24 characters");
        }
        let key = hex::decode(required("BRIDGE_CREDENTIAL_KEY")?)
            .context("BRIDGE_CREDENTIAL_KEY must be 64 hex characters")?;
        let credential_key: [u8; 32] = key.try_into()
            .map_err(|_| anyhow::anyhow!("BRIDGE_CREDENTIAL_KEY must be 32 bytes"))?;
        Ok(Self {
            admin_listen, matrix_listen, combined_listen,
            database_url: env::var("BRIDGE_DATABASE_URL")
                .unwrap_or_else(|_| "sqlite://data/2bm.sqlite".into()),
            homeserver: homeserver.trim_end_matches('/').into(),
            server_name, as_token, hs_token, admin_token, credential_key,
        })
    }

    pub fn bot_user(&self) -> String {
        format!("@_2bm:{}", self.server_name)
    }
}

fn required(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("missing {name}"))?;
    if value.trim().is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(value)
}

fn listen(name: &str, default: &str) -> Result<SocketAddr> {
    env::var(name)
        .unwrap_or_else(|_| default.into())
        .parse()
        .with_context(|| format!("invalid {name}"))
}
