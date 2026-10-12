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
        .route("/api/admin/adapter-status",get(list_adapter_status))
        .route("/api/admin/accounts/{id}", delete(delete_account))
        .route("/api/admin/accounts/{id}/telegram/start", post(telegram_start))
        .route("/api/admin/accounts/{id}/telegram/verify", post(telegram_verify))
        .route("/api/admin/accounts/{id}/telegram/logout", post(telegram_logout))
        .route("/api/admin/accounts/{id}/telegram/history", post(telegram_history))
        .route("/api/admin/accounts/{id}/sync", post(sync_virtual_rooms))
        .route("/api/admin/virtual-rooms", get(list_virtual_rooms))
        .route("/api/admin/delivery-issues",get(list_delivery_issues))
        .route("/api/admin/room-warnings",get(list_room_warnings))
        .route("/api/admin/qq-inbox",get(list_qq_inbox))
        .route("/api/admin/telegram-inbox",get(list_telegram_inbox))
        .route("/api/admin/queues/retry",post(retry_queued_message))
        .route("/api/admin/queues/capacity",get(queue_capacity))
        .route("/api/admin/delivery-issues/resolve",post(resolve_delivery_issue))
        .route("/api/admin/accounts/{id}/qq/members",post(sync_qq_members))
        .route("/api/admin/mappings", get(list_mappings).post(create_mapping))
        .route("/api/admin/mappings/{id}", delete(delete_mapping))
}

pub fn matrix_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/healthz", get(|| async { Json(json!({"status":"ok"})) }))
        .route("/readyz", get(readiness))
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

fn inbound_error(err:anyhow::Error)->ApiError {
    let description=err.to_string();
    if description.contains("inbound queue full for this account") {
        tracing::warn!("inbound per-account queue capacity exceeded");
        return error(StatusCode::SERVICE_UNAVAILABLE,
            "inbound queue full; please retry webhook later");
    }
    if description.contains("encrypted message exceeds 256 KiB inbox limit") {
        return error(StatusCode::PAYLOAD_TOO_LARGE,
            "inbound message exceeds configured encrypted envelope limit");
    }
    internal(err)
}

fn internal(err: impl std::fmt::Display) -> ApiError {
    tracing::error!(error = %err, "request failed");
    error(StatusCode::INTERNAL_SERVER_ERROR, "internal service error")
}



#[derive(serde::Serialize,sqlx::FromRow)]
struct RoomWarning {
    matrix_room:String,
    reason:String,
    event_count:i64,
    first_seen:String,
    last_seen:String,
}
#[derive(serde::Serialize,sqlx::FromRow)]
struct QqInboxInfo {
    account_id:String,
    source_key:String,
    attempts:i64,
    received_at:String,
    next_attempt_at:String,
    last_error:Option<String>,
    queue_kind:String,
}
fn queued_table(kind:&str)->Option<&'static str>{
    match kind {
        "qq_own"=>Some("qq_own_inbox"),
        "qq_events"=>Some("qq_events_inbox"),
        "telegram_events"=>Some("telegram_events_inbox"),
        _=>None,
    }
}
#[derive(Deserialize)]
struct QueueRetryRequest {
    account_id:String,
    source_key:String,
    kind:String,
}
async fn queue_capacity(
    State(state):State<Arc<AppState>>,headers:HeaderMap
)->ApiResult<Json<Value>>{
    admin_check(&headers,&state)?;
    let pending=crate::inbox::queue_load(&state).await.map_err(internal)?;
    let rows:Vec<Value>=pending.into_iter().map(|(account_id,count)|
        json!({"account_id":account_id,"pending":count,
            "limit":crate::inbox::MAX_PENDING_PER_ACCOUNT})
    ).collect();
    Ok(Json(json!({"max_per_account":crate::inbox::MAX_PENDING_PER_ACCOUNT,
        "accounts":rows})))
}

async fn retry_queued_message(
    State(state):State<Arc<AppState>>,headers:HeaderMap,
    Json(request):Json<QueueRetryRequest>
)->ApiResult<Json<Value>> {
    admin_check(&headers,&state)?;
    let table=queued_table(&request.kind).ok_or_else(||error(
        StatusCode::BAD_REQUEST,"unknown queue kind"))?;
    if request.source_key.len()>1024 || request.account_id.len()>128 {
        return Err(error(StatusCode::BAD_REQUEST,"invalid queue identifier"));
    }
    // The table name comes only from a fixed allowlist. Keys are bound.
    let statement=format!(
        "UPDATE {table} SET next_attempt_at=CURRENT_TIMESTAMP
         WHERE account_id=? AND source_key=?");
    let updated=sqlx::query(&statement)
        .bind(&request.account_id).bind(&request.source_key)
        .execute(&state.pool).await.map_err(internal)?;
    if updated.rows_affected()==0 {
        return Err(error(StatusCode::NOT_FOUND,"queued record not found"));
    }
    tracing::info!(account=%request.account_id,kind=%request.kind,
        "administrator scheduled an immediate encrypted inbox retry");
    Ok(Json(json!({"scheduled":true})))
}

async fn list_telegram_inbox(
    State(state):State<Arc<AppState>>,headers:HeaderMap
)->ApiResult<Json<Value>>{
    admin_check(&headers,&state)?;
    let entries:Vec<QqInboxInfo>=sqlx::query_as(
        "SELECT account_id,source_key,attempts,received_at,next_attempt_at,last_error,
             'telegram_events' AS queue_kind
         FROM telegram_events_inbox ORDER BY received_at ASC LIMIT 200")
        .fetch_all(&state.pool).await.map_err(internal)?;
    let pending=crate::inbox::telegram_pending_count(&state)
        .await.map_err(internal)?;
    Ok(Json(json!({"pending":pending,"entries":entries})))
}

async fn list_qq_inbox(
    State(state):State<Arc<AppState>>,headers:HeaderMap
)->ApiResult<Json<Value>>{
    admin_check(&headers,&state)?;
    // Never expose encrypted body or decrypted private QQ message content.
    let entries:Vec<QqInboxInfo>=sqlx::query_as(
        "SELECT account_id,source_key,attempts,received_at,next_attempt_at,last_error,
             'qq_own' AS queue_kind FROM qq_own_inbox
         UNION ALL
         SELECT account_id,source_key,attempts,received_at,next_attempt_at,last_error,
             'qq_events' AS queue_kind FROM qq_events_inbox
         ORDER BY received_at ASC LIMIT 200")
        .fetch_all(&state.pool).await.map_err(internal)?;
    let total=crate::inbox::queued_count(&state).await.map_err(internal)?;
    Ok(Json(json!({"pending":total,"entries":entries})))
}

async fn list_room_warnings(
    State(state):State<Arc<AppState>>,headers:HeaderMap
)->ApiResult<Json<Value>> {
    admin_check(&headers,&state)?;
    let warnings:Vec<RoomWarning>=sqlx::query_as(
        "SELECT matrix_room,reason,event_count,first_seen,last_seen
         FROM room_warnings ORDER BY last_seen DESC LIMIT 200")
        .fetch_all(&state.pool).await.map_err(internal)?;
    Ok(Json(json!(warnings)))
}

async fn list_delivery_issues(
    State(state):State<Arc<AppState>>,headers:HeaderMap
)->ApiResult<Json<Value>> {
    admin_check(&headers,&state)?;
    let issues=crate::delivery::list_unresolved(&state).await.map_err(internal)?;
    Ok(Json(json!(issues)))
}
#[derive(Deserialize)]
struct ResolveDeliveryIssue {
    event_key:String,
    action:String,
}
async fn resolve_delivery_issue(
    State(state):State<Arc<AppState>>,headers:HeaderMap,
    Json(request):Json<ResolveDeliveryIssue>
)->ApiResult<Json<Value>> {
    admin_check(&headers,&state)?;
    if !matches!(request.action.as_str(),"retry"|"acknowledge") {
        return Err(error(StatusCode::BAD_REQUEST,"action must be retry or acknowledge"));
    }
    let found=crate::delivery::resolve(&state,&request.event_key,&request.action)
        .await.map_err(internal)?;
    if !found {return Err(error(StatusCode::NOT_FOUND,"unresolved delivery not found"));}
    Ok(Json(json!({"status":"resolved","action":request.action})))
}


#[derive(serde::Serialize,sqlx::FromRow)]
struct AdapterStatus {
    account_id:String,
    state:String,
    updated_at:String,
}
async fn list_adapter_status(
    State(state):State<Arc<AppState>>,headers:HeaderMap
)->ApiResult<Json<Value>> {
    admin_check(&headers,&state)?;
    let statuses:Vec<AdapterStatus>=sqlx::query_as(
        "SELECT account_id,state,updated_at FROM adapter_status
         ORDER BY account_id"
    ).fetch_all(&state.pool).await.map_err(internal)?;
    Ok(Json(json!(statuses)))
}

async fn list_accounts(
    State(state): State<Arc<AppState>>, headers: HeaderMap
) -> ApiResult<Json<Value>> {
    admin_check(&headers, &state)?;
    let accounts: Vec<db::AccountPublic> = sqlx::query_as(
        "SELECT id,platform,label,owner_mxid,enabled FROM accounts ORDER BY label"
    ).fetch_all(&state.pool).await.map_err(internal)?;
    Ok(Json(json!(accounts)))
}

#[derive(Deserialize)]
struct CreateAccount {
    platform: String,
    label: String,
    owner_mxid: String,
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
    if !request.owner_mxid.starts_with('@') || !request.owner_mxid.contains(':')
        || request.owner_mxid.len() > 255 {
        return Err(error(StatusCode::BAD_REQUEST, "owner_mxid must be @user:server"));
    }
    match request.platform.as_str() {
        "telegram_user" => {
            if request.credentials["api_id"].as_i64().filter(|id| *id > 0).is_none()
                || request.credentials["api_hash"].as_str().is_none_or(|s| s.len() < 20) {
                return Err(error(StatusCode::BAD_REQUEST,
                    "Telegram user requires api_id and api_hash from my.telegram.org"));
            }
        }
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
    let enabled = request.platform != "telegram_user";
    sqlx::query(
        "INSERT INTO accounts(id,platform,label,owner_mxid,credentials,enabled) VALUES (?,?,?,?,?,?)"
    ).bind(&id).bind(&request.platform).bind(&request.label).bind(&request.owner_mxid).bind(sealed).bind(enabled)
        .execute(&state.pool).await.map_err(internal)?;
    Ok((StatusCode::CREATED, Json(json!({
        "id": id, "platform": request.platform, "label": request.label, "owner_mxid": request.owner_mxid, "enabled": enabled
    }))))
}

#[derive(Deserialize)]
struct TelegramPhone {
    phone: String,
}
async fn telegram_start(
    State(state): State<Arc<AppState>>, headers: HeaderMap, Path(id): Path<String>,
    Json(input): Json<TelegramPhone>,
) -> ApiResult<Json<Value>> {
    admin_check(&headers, &state)?;
    if !input.phone.starts_with('+') || input.phone.len() > 25 {
        return Err(error(StatusCode::BAD_REQUEST,"phone must include +country-code"));
    }
    let status = crate::telegram_user::begin(&state,&id,&input.phone).await
        .map_err(|e| error(StatusCode::UNPROCESSABLE_ENTITY,&e.to_string()))?;
    Ok(Json(json!({"status":status})))
}
#[derive(Deserialize)]
struct TelegramVerification {
    value: String,
}
async fn telegram_verify(
    State(state): State<Arc<AppState>>, headers: HeaderMap, Path(id): Path<String>,
    Json(input): Json<TelegramVerification>,
) -> ApiResult<Json<Value>> {
    admin_check(&headers, &state)?;
    if input.value.trim().is_empty() || input.value.len() > 256 {
        return Err(error(StatusCode::BAD_REQUEST,"verification is required"));
    }
    let status = crate::telegram_user::verify(&state,&id,&input.value).await
        .map_err(|e| error(StatusCode::UNPROCESSABLE_ENTITY,&e.to_string()))?;
    Ok(Json(json!({"status":status})))
}


#[derive(Deserialize)]
struct TelegramHistoryRequest {
    chat: String,
    limit: Option<usize>,
}
async fn telegram_history(
    State(state):State<Arc<AppState>>,headers:HeaderMap,Path(id):Path<String>,
    Json(request):Json<TelegramHistoryRequest>
)->ApiResult<Json<Value>>{
    admin_check(&headers,&state)?;
    let account=db::account(&state.pool,&id).await.map_err(internal)?
        .filter(|a|a.enabled && a.platform=="telegram_user")
        .ok_or_else(||error(StatusCode::NOT_FOUND,"enabled Telegram personal account not found"))?;
    let report=crate::telegram_user::import_recent(
        &state,&account,&request.chat,request.limit.unwrap_or(15).clamp(1,50)
    ).await.map_err(|e|error(StatusCode::UNPROCESSABLE_ENTITY,&e.to_string()))?;
    Ok(Json(json!(report)))
}

async fn telegram_logout(
    State(state):State<Arc<AppState>>,headers:HeaderMap,Path(id):Path<String>
)->ApiResult<Json<Value>> {
    admin_check(&headers,&state)?;
    crate::telegram_user::logout(&state,&id).await
        .map_err(|err|error(StatusCode::CONFLICT,&err.to_string()))?;
    Ok(Json(json!({"status":"logged_out","remote_authorization":"revoked"})))
}

async fn delete_account(
    State(state): State<Arc<AppState>>, headers: HeaderMap, Path(id): Path<String>
) -> ApiResult<StatusCode> {
    admin_check(&headers, &state)?;
    let account=db::account(&state.pool,&id).await.map_err(internal)?
        .ok_or_else(||error(StatusCode::NOT_FOUND,"account not found"))?;
    let pending=db::pending_account_records(&state.pool,&id)
        .await.map_err(internal)?;
    if pending>0 {
        return Err(error(StatusCode::CONFLICT,
            "account has undelivered callbacks or uncertain outbound sends; reconcile queued messages before deleting"));
    }
    if account.platform=="telegram_user" {
        if account.enabled {
            return Err(error(StatusCode::CONFLICT,
                "revoke Telegram authorization in WebUI before deleting this account"));
        }
        crate::telegram_user::purge_local_session(&state,&id).await.map_err(internal)?;
    }
    let outcome = sqlx::query("DELETE FROM accounts WHERE id = ?")
        .bind(id).execute(&state.pool).await.map_err(internal)?;
    if outcome.rows_affected() == 0 { return Err(error(StatusCode::NOT_FOUND, "account not found")); }
    Ok(StatusCode::NO_CONTENT)
}


#[derive(Deserialize)]
struct SyncRequest {
    limit: Option<usize>,
}
async fn sync_virtual_rooms(
    State(state): State<Arc<AppState>>, headers: HeaderMap, Path(id): Path<String>,
    Json(input): Json<SyncRequest>,
) -> ApiResult<Json<Value>> {
    admin_check(&headers,&state)?;
    let account = db::account(&state.pool,&id).await.map_err(internal)?
        .ok_or_else(||error(StatusCode::NOT_FOUND,"account not found"))?;
    if !account.enabled {
        return Err(error(StatusCode::CONFLICT,"account must be logged in/enabled"));
    }
    let limit = input.limit.unwrap_or(30).clamp(1,100);
    let report = crate::virtual_rooms::sync(&state,&account,limit).await
        .map_err(|e|error(StatusCode::UNPROCESSABLE_ENTITY,&e.to_string()))?;
    Ok(Json(json!(report)))
}
#[derive(serde::Serialize, sqlx::FromRow)]
struct VirtualRoomInfo {
    account_id: String,
    external_chat: String,
    title: String,
    direct: bool,
    matrix_room: Option<String>,
    updated_at: String,
}
#[derive(Deserialize)]
struct MemberSyncRequest {
    chat: String,
    limit: Option<usize>,
}
async fn sync_qq_members(
    State(state):State<Arc<AppState>>,headers:HeaderMap,Path(id):Path<String>,
    Json(request):Json<MemberSyncRequest>
)->ApiResult<Json<Value>>{
    admin_check(&headers,&state)?;
    let account=db::account(&state.pool,&id).await.map_err(internal)?
        .filter(|a|a.enabled && a.platform=="qq")
        .ok_or_else(||error(StatusCode::NOT_FOUND,"enabled QQ account not found"))?;
    let report=adapters::qq::sync_group_members(
        &state,&account,&request.chat,request.limit.unwrap_or(30).clamp(1,100)
    ).await.map_err(|e|error(StatusCode::UNPROCESSABLE_ENTITY,&e.to_string()))?;
    Ok(Json(json!(report)))
}

async fn list_virtual_rooms(
    State(state): State<Arc<AppState>>,headers:HeaderMap
)->ApiResult<Json<Value>>{
    admin_check(&headers,&state)?;
    let rooms:Vec<VirtualRoomInfo>=sqlx::query_as(
        "SELECT v.account_id,v.external_chat,v.title,v.direct,v.updated_at,m.matrix_room
        FROM virtual_chats v LEFT JOIN mappings m ON v.account_id=m.account_id
            AND v.external_chat=m.external_chat
        ORDER BY v.updated_at DESC, v.title ASC LIMIT 1000"
    ).fetch_all(&state.pool).await.map_err(internal)?;
    Ok(Json(json!(rooms)))
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
        "telegram" | "telegram_user" => request.external_chat.parse::<i64>().is_ok(),
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
    matrix::require_unencrypted(&state,&request.matrix_room,None).await
        .map_err(|err|error(StatusCode::UNPROCESSABLE_ENTITY,&err.to_string()))?;
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
    // Synapse may retry or send concurrent transactions. Serialize the entire
    // application-service handler so check -> delivery -> checkpoint is atomic
    // with respect to other handlers in this process.
    let _guard=state.transaction_lock.lock().await;
    if db::has_key(&state.pool, "transactions", "txn_id", &txn).await.map_err(internal)? {
        return Ok(Json(json!({})));
    }
    let events = body["events"].as_array()
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "missing events array"))?;
    for event in events {
        let event_id=event["event_id"].as_str();
        if let Some(id)=event_id {
            let seen:Option<i64>=sqlx::query_scalar(
                "SELECT 1 FROM appservice_events WHERE event_id=? LIMIT 1"
            ).bind(id).fetch_optional(&state.pool).await.map_err(internal)?;
            if seen.is_some(){continue;}
        }
        relay::matrix_event(&state, event).await.map_err(internal)?;
        // Checkpoint each completed event. If a later event fails, Synapse
        // retries the transaction but earlier events will not run again.
        if let Some(id)=event_id {
            sqlx::query("INSERT OR IGNORE INTO appservice_events(event_id) VALUES (?)")
                .bind(id).execute(&state.pool).await.map_err(internal)?;
        }
    }
    sqlx::query("INSERT OR IGNORE INTO transactions(txn_id) VALUES (?)")
        .bind(txn).execute(&state.pool).await.map_err(internal)?;
    Ok(Json(json!({})))
}

async fn readiness(
    State(state):State<Arc<AppState>>
)->ApiResult<Json<Value>> {
    let db: i64 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&state.pool).await.map_err(|_|error(
            StatusCode::SERVICE_UNAVAILABLE,"database unavailable"))?;
    if db != 1 {
        return Err(error(StatusCode::SERVICE_UNAVAILABLE,"database not ready"));
    }
    let response=state.http.get(format!("{}/_matrix/client/versions",
        state.config.homeserver))
        .timeout(std::time::Duration::from_secs(5))
        .send().await.map_err(|_|error(
            StatusCode::SERVICE_UNAVAILABLE,"Matrix homeserver unavailable"))?;
    if !response.status().is_success() {
        return Err(error(StatusCode::SERVICE_UNAVAILABLE,"Matrix homeserver not ready"));
    }
    Ok(Json(json!({"status":"ready"})))
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
    if user == state.config.bot_user() { return Ok(Json(json!({}))); }
    let owned: Option<i64> = sqlx::query_scalar(
        "SELECT 1 FROM ghost_users WHERE mxid = ? LIMIT 1"
    ).bind(&user).fetch_optional(&state.pool).await.map_err(internal)?;
    if owned.is_some() { Ok(Json(json!({}))) }
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
    if body["post_type"] == "notice" {
        let key=if let Some((chat,id))=adapters::qq::recall_target(&body) {
            Some(format!("{account_id}:recall:{chat}:{id}"))
        }else if adapters::qq::membership_target(&body).is_some() {
            // OneBot has no stable unique membership-notice ID. Retaining
            // each notice preserves rapid leave+join sequences.
            Some(format!("{account_id}:membership:{}",Uuid::new_v4()))
        }else{None};
        if let Some(key)=key {
            crate::inbox::enqueue_qq_event(&state,&account_id,&key,&body)
                .await.map_err(inbound_error)?;
        }
        return Ok(Json(json!({})));
    }
    if !matches!(body["post_type"].as_str(),Some("message"|"message_sent")) {
        return Ok(Json(json!({})));
    }
    let own = body["post_type"]=="message_sent"
        || body["user_id"].as_i64().zip(body["self_id"].as_i64())
            .is_some_and(|(user,self_id)|user==self_id);
    let chat=if own {adapters::qq::own_chat_id(&body)}
        else {adapters::qq::chat_id(&body)};
    let Some(chat)=chat else {return Ok(Json(json!({})));};
    let message_id = body["message_id"].as_str()
        .map(str::to_owned)
        .or_else(|| body["message_id"].as_i64().map(|n| n.to_string()))
        .ok_or_else(|| error(StatusCode::BAD_REQUEST, "missing message_id"))?;
    let sender_id = body["user_id"].as_i64()
        .map(|n| n.to_string()).unwrap_or_default();
    if sender_id.is_empty() { return Err(error(StatusCode::BAD_REQUEST,"missing QQ sender")); }
    if own {
        // Commit the authenticated webhook to encrypted SQLite before ACK.
        // A separate worker reconciles it against Matrix after in-flight
        // outbound requests finish, even when the process restarts.
        let text=adapters::qq::message_text(&body);
        crate::inbox::enqueue_qq_own(&state,&account_id,&chat,&message_id,&text)
            .await.map_err(inbound_error)?;
    } else {
        let key=format!("{account_id}:message:{chat}:{message_id}");
        crate::inbox::enqueue_qq_event(&state,&account_id,&key,&body)
            .await.map_err(inbound_error)?;
    }
    Ok(Json(json!({})))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn queued_retry_only_accepts_static_tables(){
        assert_eq!(queued_table("telegram_events"),Some("telegram_events_inbox"));
        assert_eq!(queued_table("qq_events"),Some("qq_events_inbox"));
        assert_eq!(queued_table("qq_own"),Some("qq_own_inbox"));
        assert!(queued_table("mappings;DROP TABLE accounts").is_none());
    }
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
