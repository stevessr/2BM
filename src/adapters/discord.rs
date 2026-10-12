use crate::{adapters::token, relay, AppState};
use anyhow::{bail, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::time::Instant;
use tokio_tungstenite::{connect_async, tungstenite::Message};

pub async fn send(state: &Arc<AppState>, credentials: &Value, chat: &str, text: &str) -> Result<()> {
    let bot_token = token(credentials, "token")?;
    // Discord limits message content to 2000 characters.
    let content: String = text.chars().take(1900).collect();
    state.http.post(format!("https://discord.com/api/v10/channels/{chat}/messages"))
        .header("Authorization", format!("Bot {bot_token}"))
        .json(&json!({
            "content": content,
            "allowed_mentions": {"parse": []}
        }))
        .send().await?
        .error_for_status()?;
    Ok(())
}

pub async fn gateway(state: &Arc<AppState>, account_id: &str, credentials: &Value) -> Result<()> {
    let bot_token = token(credentials, "token")?;
    let gateway_response = state.http.get("https://discord.com/api/v10/gateway/bot")
        .header("Authorization", format!("Bot {bot_token}"))
        .send().await?.error_for_status()?;
    let info: Value = gateway_response.json().await?;
    let gateway_url = token(&info, "url")?;
    let url = format!("{}/?v=10&encoding=json", gateway_url.trim_end_matches('/'));
    let (mut stream, _) = connect_async(url).await?;
    let mut sequence = Value::Null;
    let mut heartbeat_due: Option<Instant> = None;
    let mut interval = Duration::from_secs(45);
    let mut acknowledged = true;

    loop {
        tokio::select! {
            next = stream.next() => {
                let Some(frame) = next else { bail!("Discord gateway closed"); };
                match frame? {
                    Message::Text(payload) => {
                        let event: Value = serde_json::from_str(&payload)?;
                        if let Some(s) = event.get("s").filter(|v| !v.is_null()) {
                            sequence = s.clone();
                        }
                        match event["op"].as_i64() {
                            Some(10) => {
                                let ms = event["d"]["heartbeat_interval"].as_u64()
                                    .unwrap_or(45000);
                                interval = Duration::from_millis(ms);
                                heartbeat_due = Some(Instant::now() + interval);
                                // GUILDS | GUILD_MESSAGES | DIRECT_MESSAGES | MESSAGE_CONTENT.
                                stream.send(Message::Text(json!({
                                    "op": 2,
                                    "d": {"token": bot_token, "intents": 37377,
                                          "properties": {"os": "linux", "browser": "2BM", "device": "2BM"}}
                                }).to_string().into())).await?;
                            }
                            Some(1) => {
                                stream.send(Message::Text(json!({"op":1,"d":sequence})
                                    .to_string().into())).await?;
                            }
                            Some(11) => { acknowledged = true; }
                            Some(7) | Some(9) => bail!("Discord requested reconnect"),
                            Some(0) if event["t"] == "MESSAGE_CREATE" => {
                                let data = &event["d"];
                                if data["author"]["bot"] == true { continue; }
                                let Some(channel) = data["channel_id"].as_str() else { continue; };
                                let Some(id) = data["id"].as_str() else { continue; };
                                let Some(body) = data["content"].as_str() else { continue; };
                                let sender = data["author"]["global_name"].as_str()
                                    .or_else(|| data["author"]["username"].as_str())
                                    .unwrap_or("Discord user");
                                relay::incoming(state, account_id, channel, id, sender, body).await?;
                            }
                            _ => {}
                        }
                    }
                    Message::Close(_) => bail!("Discord gateway requested close"),
                    Message::Ping(bytes) => stream.send(Message::Pong(bytes)).await?,
                    _ => {}
                }
            }
            _ = async {
                if let Some(deadline) = heartbeat_due {
                    tokio::time::sleep_until(deadline).await;
                }
            }, if heartbeat_due.is_some() => {
                if !acknowledged { bail!("Discord heartbeat was not acknowledged"); }
                acknowledged = false;
                stream.send(Message::Text(json!({"op":1,"d":sequence})
                    .to_string().into())).await?;
                heartbeat_due = Some(Instant::now() + interval);
            }
        }
    }
}
