use crate::{adapters, crypto, db, matrix, relay, AppState};
use axum::{
    extract::{Path, Query, State},
    http::{header, HeaderMap, StatusCode},
    response::Html,
    routing::{delete, get, post, put},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::Arc};
use subtle::ConstantTimeEq;
use tracing::warn;
use uuid::Uuid;

type ApiError = (StatusCode, Json<Value>);
type ApiResult<T> = Result<T, ApiError>;

pub fn admin_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(admin_page))
        .route("/admin", get(admin_page))
        .route("/api/admin/accounts", get(list_accounts).post(create_account))
        .route("/api/admin/accounts/{id}", delete(delete_account))
        .route("/api/admin/mappings", get(list_mappings).post(create_mapping))
        .route("/api/admin/mappings/{id}", delete(delete_mapping))
}

pub fn matrix_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/healthz", get(|| async { Json(json!({"status":"ok"})) }))
        .route("/_matrix/app/v1/transactions/{txn}", put(transaction))
        .route("/_matrix/app/v1/ping", post(ping))
        .route("/_matrix/app/v1/users/{user}", get(query_user))
        .route("/_matrix/app/v1/rooms/{alias}", get(not_found))
        .route("/_matrix/app/v1/thirdparty/location", get(empty_list))
        .route("/_matrix/app/v1/thirdparty/user", get(empty_list))
        .route("/_matrix/app/v1/thirdparty/location/{protocol}", get(empty_list))
        .route("/_matrix/app/v1/thirdparty/user/{protocol}", get(empty_list))
        .route("/_matrix/app/v1/thirdparty/protocol/{protocol}", get(not_found))
        .route("/hooks/qq/{account_id}", post(qq_hook))
}

async fn admin_page() -> ([(header::HeaderName, &'static str); 2], Html<&'static str>) {
    ([
        (header::CACHE_CONTROL, "no-store"),
        (header::CONTENT_SECURITY_POLICY, "default-src 'none'; style-src 'unsafe-inline'; script-src 'unsafe-inline'; connect-src 'self'; form-action 'self'; base-uri 'none'; frame-ancestors 'none'"),
    ], Html(include_str!("../static/admin.html")))
}

fn authorized(headers: &HeaderMap, query: &HashMap<String, String>, expected: &str) -> bool {
    let bearer = headers.get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "));
    let legacy = query.get("access_token").map(String::as_str);
    if bearer.is_none() && legacy.is_none() { return false; }
    bearer.is_none_or(|s| constant_eq(s, expected))
        && legacy.is_none_or(|s| constant_eq(s, expected))
}

fn constant_eq(value: &str, expected: &str) -> bool {
    bool::from(value.as_bytes().ct_eq(expected.as_bytes()))
}

fn admin_check(headers: &HeaderMap, state: &AppState) -> ApiResult<()> {
    if authorized(headers, &HashMap::new(), &state.config.admin_token) { Ok(()) }
    else { Err(error(StatusCode::UNAUTHORIZED, "invalid admin token")) }
}

fn hs_check(headers: &HeaderMap, query: &HashMap<String, String>, state: &AppState) -> ApiResult<()> {
    if authorized(headers, query, &state.config.hs_token) { Ok(()) }
    else { Err((StatusCode::FORBIDDEN, Json(json!({
        "errcode": "M_FORBIDDEN", "error": "invalid application service token"
    })))) }
}

fn error(status: StatusCode, message: &str) -> ApiError {
    (status, Json(json!({"error":message})))
}

fn internal(err: impl std::fmt::Display) -> ApiError {
    tracing::error!(error = %err, "request failed");
    error(StatusCode::INTERNAL_SERVER_ERROR, "internal service error")
}

async fn list_accounts(
    State(state): State<Arc<AppState>>, headers: HeaderMap
) -> ApiResult<Json<Value>> {
    admin_check(&headers, &state)?;
    let accounts: Vec<db::AccountPublic> = sqlx::query_as(
        "SELECT id,platform,label,enabled FROM accounts ORDER BY label"
    ).fetch_all(&state.pool).await.map_err(internal)?;
    Ok(Json(json!(accounts)))
}

#[derive(Deserialize)]
struct CreateAccount {
    platform: String,
    label: String,
    credentials: Value,
}

async fn create_account(
    State(state): State<Arc<AppState>>, headers: HeaderMap,
    Json(request): Json<CreateAccount>
) -> ApiResult<(StatusCode, Json<Value>)> {
    admin_check(&headers, &state)?;
    if request.label.trim().is_empty() || request.label.len() > 100 {
        return Err(error(StatusCode::BAD_REQUEST, "label must contain 1-100 characters"));
    }
    match request.platform.as_str() {
        "telegram" | "discord" => {
            if request.credentials["token"].as_str().is_none_or(|s| s.len() < 10) {
                return Err(error(StatusCode::BAD_REQUEST, "bot token is required"));
            }
        }
        "qq" => {
            for key in ["api_base", "access_token", "hook_secret"] {
                if request.credentials[key].as_str().is_none_or(str::is_empty) {
                    return Err(error(StatusCode::BAD_REQUEST, "QQ requires api_base, access_token and hook_secret"));
                }
            }
            let base = request.credentials["api_base"].as_str().unwrap_or_default();
            if !(base.starts_with("http://") || base.starts_with("https://")) {
                return Err(error(StatusCode::BAD_REQUEST, "QQ api_base must be an HTTP(S) URL"));
            }
        }
        _ => return Err(error(StatusCode::BAD_REQUEST, "platform must be telegram, qq or discord")),
    }
    let id = Uuid::new_v4().to_string();
    let sealed = crypto::encrypt(&state.config.credential_key, &request.credentials)
        .map_err(internal)?;
    sqlx::query(
        "INSERT INTO accounts(id,platform,label,credentials,enabled) VALUES (?,?,?,?,1)"
    ).bind(&id).bind(&request.platform).bind(&request.label).bind(sealed)
        .execute(&state.pool).await.map_err(internal)?;
    Ok((StatusCode::CREATED, Json(json!({
        "id": id, "platform": request.platform, "label": request.label, "enabled": true
    }))))
}

async fn delete_account(
    State(state): State<Arc<AppState>>, headers: HeaderMap, Path(id): Path<String>
) -> ApiResult<StatusCode> {
    admin_check(&headers, &state)?;
    let outcome = sqlx::query("DELETE FROM accounts WHERE id = ?")
        .bind(id).execute(&state.pool).await.map_err(internal)?;
    if outcome.rows_affected() == 0 { return Err(error(StatusCode::NOT_FOUND, "account not found")); }
    Ok(StatusCode::NO_CONTENT)
}

async fn list_mappings(
    State(state): State<Arc<AppState>>, headers: HeaderMap
) -> ApiResult<Json<Value>> {
    admin_check(&headers, &state)?;
    let rows: Vec<db::Mapping> = sqlx::query_as(
        "SELECT id,account_id,external_chat,matrix_room FROM mappings ORDER BY id"
    ).fetch_all(&state.pool).await.map_err(internal)?;
    Ok(Json(json!(rows)))
}

#[derive(Deserialize)]
struct CreateMapping {
    account_id: String,
    external_chat: String,
    matrix_room: String,
}

async fn create_mapping(
    State(state): State<Arc<AppState>>, headers: HeaderMap,
    Json(request): Json<CreateMapping>
) -> ApiResult<(StatusCode, Json<Value>)> {
    admin_check(&headers, &state)?;
    if !request.matrix_room.starts_with('!') || !request.matrix_room.contains(':') {
        return Err(error(StatusCode::BAD_REQUEST, "matrix_room must be a Matrix room ID (!room:server)"));
    }
    if request.external_chat.is_empty() || request.external_chat.len() > 200 {
        return Err(error(StatusCode::BAD_REQUEST, "external_chat is required (max 200 characters)"));
    }
    let account = db::account(&state.pool, &request.account_id)
        .await.map_err(internal)?
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "account not found"))?;
    let valid_chat = match account.platform.as_str() {
        "telegram" => request.external_chat.parse::<i64>().is_ok(),
        "discord" => request.external_chat.parse::<u64>().is_ok(),
        "qq" => adapters::qq::chat_id(&json!({
            "message_type": request.external_chat.split(':').next().unwrap_or(""),
            "group_id": request.external_chat.strip_prefix("group:").and_then(|x| x.parse::<i64>().ok()),
            "user_id": request.external_chat.strip_prefix("private:").and_then(|x| x.parse::<i64>().ok())
        })).as_deref() == Some(&request.external_chat),
        _ => false,
    };
    if !valid_chat {
        return Err(error(StatusCode::BAD_REQUEST, "invalid external chat ID for this platform"));
    }
    let mapping: db::Mapping = sqlx::query_as(
        "INSERT INTO mappings(account_id,external_chat,matrix_room) VALUES (?,?,?)
         RETURNING id,account_id,external_chat,matrix_room"
    ).bind(&request.account_id).bind(&request.external_chat).bind(&request.matrix_room)
        .fetch_one(&state.pool).await.map_err(|err| {
            if err.as_database_error().is_some_and(|e| e.is_unique_violation()) {
                error(StatusCode::CONFLICT, "this account and chat already have a mapping")
            } else { internal(err) }
        })?;
    // An invite from a Matrix user may be required first.
    if let Err(err) = matrix::join_bot(&state, &request.matrix_room).await {
        warn!(room = %request.matrix_room, %err, "bot could not join; invite it into the room");
    }
    Ok((StatusCode::CREATED, Json(json!(mapping))))
}

async fn delete_mapping(
    State(state): State<Arc<AppState>>, headers: HeaderMap, Path(id): Path<i64>
) -> ApiResult<StatusCode> {
    admin_check(&headers, &state)?;
    let outcome = sqlx::query("DELETE FROM mappings WHERE id = ?")
        .bind(id).execute(&state.pool).await.map_err(internal)?;
    if outcome.rows_affected() == 0 { return Err(error(StatusCode::NOT_FOUND, "mapping not found")); }
    Ok(StatusCode::NO_CONTENT)
}

async fn transaction(
    State(state): State<Arc<AppState>>, Path(txn): Path<String>,
    headers: HeaderMap, Query(query): Query<HashMap<String, String>>,
    Json(body): Json<Value>
) -> ApiResult<Json<Value>> {
    hs_check(&headers, &query, &state)?;
    if db::has_key(&state.pool, "transactions", "txn_id", &txn).await.map_err(internal)? {
        return Ok(Json(json!({})));
    }
    let events = body["events"].as_array()
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "missing events array"))?;
    for event in events {
        relay::matrix_event(&state, event).await.map_err(internal)?;
    }
    sqlx::query("INSERT OR IGNORE INTO transactions(txn_id) VALUES (?)")
        .bind(txn).execute(&state.pool).await.map_err(internal)?;
    Ok(Json(json!({})))
}

async fn ping(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap, Query(query): Query<HashMap<String, String>>
) -> ApiResult<Json<Value>> {
    hs_check(&headers, &query, &state)?;
    Ok(Json(json!({})))
}

async fn query_user(
    State(state): State<Arc<AppState>>, Path(user): Path<String>,
    headers: HeaderMap, Query(query): Query<HashMap<String, String>>
) -> ApiResult<Json<Value>> {
    hs_check(&headers, &query, &state)?;
    if user == state.config.bot_user() { Ok(Json(json!({}))) }
    else { Err(error(StatusCode::NOT_FOUND, "unknown Matrix user")) }
}

async fn not_found() -> ApiError {
    error(StatusCode::NOT_FOUND, "not supported")
}

async fn empty_list() -> Json<Value> { Json(json!([])) }

async fn qq_hook(
    State(state): State<Arc<AppState>>, Path(account_id): Path<String>,
    headers: HeaderMap, Json(body): Json<Value>
) -> ApiResult<Json<Value>> {
    let account = db::account(&state.pool, &account_id)
        .await.map_err(internal)?
        .filter(|a| a.enabled && a.platform == "qq")
        .ok_or_else(|| error(StatusCode::NOT_FOUND, "QQ account not found"))?;
    let secrets = adapters::credentials(&state, &account).map_err(internal)?;
    let expected = adapters::token(&secrets, "hook_secret").map_err(internal)?;
    if !authorized(&headers, &HashMap::new(), expected) {
        return Err(error(StatusCode::UNAUTHORIZED, "invalid OneBot webhook token"));
    }
    if body["post_type"] != "message" { return Ok(Json(json!({}))); }
    let Some(chat) = adapters::qq::chat_id(&body) else { return Ok(Json(json!({}))); };
    let message_id = body["message_id"].as_str()
        .map(str::to_owned)
        .or_else(|| body["message_id"].as_i64().map(|n| n.to_string()))
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "missing message_id"))?;
    let sender = body["sender"]["card"].as_str()
        .filter(|s| !s.is_empty())
        .or_else(|| body["sender"]["nickname"].as_str())
        .unwrap_or("QQ user");
    let text = body["raw_message"].as_str()
        .or_else(|| body["message"].as_str())
        .unwrap_or_default();
    relay::incoming(&state, &account_id, &chat, &message_id, sender, text)
        .await.map_err(internal)?;
    Ok(Json(json!({})))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bearer_and_legacy_must_match() {
        let mut headers = HeaderMap::new();
        headers.insert(header::AUTHORIZATION, "Bearer right".parse().unwrap());
        assert!(authorized(&headers, &HashMap::new(), "right"));
        let mut q = HashMap::new();
        q.insert("access_token".into(), "wrong".into());
        assert!(!authorized(&headers, &q, "right"));
        headers.clear();
        q.insert("access_token".into(), "right".into());
        assert!(authorized(&headers, &q, "right"));
    }
}
