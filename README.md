# 2BM — Telegram, QQ & Discord ↔ Matrix

A modular **Rust Matrix Application Service bridge** with a lightweight, independently served account-management WebUI.

**Status: experimental MVP.** Plain-text messages can travel in both directions after configuration. 2BM is **not a Matrix homeserver** and requires Synapse or another homeserver supporting Application Services.

## Components and ports

- **Matrix AS listener** (default `127.0.0.1:9001`): receives `/_matrix/app/v1/transactions/{txn}` and QQ OneBot webhook events at `/hooks/qq/{account-id}`.
- **Admin listener** (default `127.0.0.1:9000`): serves `/admin` and protected `/api/admin/*` API.
- **Combined listener**: set `BRIDGE_COMBINED_LISTEN=0.0.0.0:9000` (or set both listener addresses equal) to serve both APIs on one port.
- **Persistence**: SQLite with encrypted account credentials, room mappings, message deduplication and Telegram offsets.
- **Background adapters**: Telegram Bot API long polling, Discord bot Gateway; QQ OneBot 11 HTTP event callback.

## Quick start

1. Copy `.env.example` to `.env`. Generate independent random tokens for the three TOKEN values using `openssl rand -hex 32`, and a credential encryption key using `openssl rand -hex 32`. Do not commit the secrets.
2. Set `BRIDGE_MATRIX_HOMESERVER` to the Client API URL that the bridge can reach and `BRIDGE_MATRIX_SERVER_NAME` to the Matrix **server name**.
3. Generate your registration: `python3 scripts/generate_registration.py --url http://127.0.0.1:9001`. The URL must be reachable **from the homeserver**, so use `http://2bm:9001` on a shared Docker network, for example. Register the generated `registration.yaml` in Synapse and restart Synapse.
4. Run `cargo run --release`, or `docker compose up --build -d`, then open <http://127.0.0.1:9000/admin>. Log in with `BRIDGE_ADMIN_TOKEN`.
5. Add a platform account. Invite `@_2bm:YOUR_MATRIX_SERVER_NAME` to the Matrix room, then create a mapping using the actual `!room:server` ID.

Accounts and external chats are mapped to Matrix rooms. One Matrix room can receive messages from multiple external chats.

## Connecting external platforms

| Network | Receive | Send | External chat ID |
| --- | --- | --- | --- |
| Telegram | Bot API `getUpdates` polling | Bot API `sendMessage` | integer chat ID, e.g. `-1001234567890` |
| Discord | Gateway `MESSAGE_CREATE` | REST channel message | Discord channel snowflake |
| QQ | OneBot 11 HTTP webhook | OneBot 11 HTTP API | `group:123456` or `private:123456` |

**Telegram**: Create a bot in BotFather, add it to your chats, and configure its privacy settings and message access as needed. Do not poll the same bot token elsewhere.

**Discord**: Create an application/bot and grant it read/send access. Enable the privileged **Message Content Intent** to receive message content. Use a bot token, not a user token.

**QQ**: Perform QQ login in **NapCat or another OneBot 11-compatible connector**. In WebUI, supply `api_base` (e.g. `http://napcat:3000`), `access_token`, and `hook_secret`. Configure OneBot HTTP event delivery to:

```http
POST http://<bridge-host>:9001/hooks/qq/<account-id>
Authorization: Bearer <hook_secret>
Content-Type: application/json
```

The account ID is visible in WebUI. Some OneBot connectors use their API token as the event callback Bearer token; in that case set `hook_secret` to the same value. Keep the webhook reachable only from trusted networks.

## Environment variables

| Variable | Description |
| --- | --- |
| `BRIDGE_ADMIN_LISTEN` | WebUI bind address, default `127.0.0.1:9000` |
| `BRIDGE_MATRIX_LISTEN` | Matrix and QQ callback bind, default `127.0.0.1:9001` |
| `BRIDGE_COMBINED_LISTEN` | Optional single-port bind override |
| `BRIDGE_MATRIX_HOMESERVER` | Matrix Client API base URL |
| `BRIDGE_MATRIX_SERVER_NAME` | Matrix user ID domain |
| `BRIDGE_MATRIX_AS_TOKEN` | Appservice token matching registration |
| `BRIDGE_MATRIX_HS_TOKEN` | Homeserver token matching registration |
| `BRIDGE_ADMIN_TOKEN` | Admin REST API Bearer token (min 24 chars) |
| `BRIDGE_CREDENTIAL_KEY` | XChaCha20-Poly1305 encryption key (64 hex characters) |
| `BRIDGE_DATABASE_URL` | SQLite URL, default `sqlite://data/2bm.sqlite` |
| `RUST_LOG` | Runtime log filter |

Back up **both** SQLite and the key: the encrypted account credentials cannot be recovered without the key. Admin credentials must be protected by HTTPS when accessed remotely. Docker Compose exposes both ports on localhost only by default; adjust networking for your homeserver/OneBot connector.

## Available endpoints

The admin REST API requires `Authorization: Bearer $BRIDGE_ADMIN_TOKEN`.

- `GET/POST /api/admin/accounts` and `DELETE /api/admin/accounts/{id}`
- `GET/POST /api/admin/mappings` and `DELETE /api/admin/mappings/{id}`
- On Matrix listener: `GET /healthz`, `PUT /_matrix/app/v1/transactions/{txn}`, `POST /_matrix/app/v1/ping`, user lookup, and `POST /hooks/qq/{account-id}`.

## MVP limitations

**Implemented:** plaintext message bridging; account management; split/combined HTTP listeners; encrypted credentials at rest; SQLite room mappings, incoming deduplication and Matrix transaction deduplication; Telegram poll offsets; Discord Gateway reconnect; simple OneBot HTTP integration.

**Not yet implemented:** Matrix E2EE, full ghost/puppeting identities, QQ QR login inside 2BM, file/media transfer, historical sync, message edits/deletes/reactions/replies, presence/typing/read receipts, Discord Gateway resume, HA multi-replica mode or full Matrix third-party lookups. Only standard plaintext messages and captions are handled.

Relaying uses one Matrix bot identity `@_2bm:server` with a source prefix. External outbound delivery is at-least-once across crashes between network delivery and local acknowledgement. Avoid running multiple replicas using the same account and database.

## Development

```bash
cargo check --all-targets
cargo test
cargo fmt --all
```

Main modules: `src/adapters` for platforms, `src/matrix.rs` for Matrix Client API calls, `src/relay.rs` for routing, `src/web.rs` for WebUI/API, and `src/workers.rs` for background adapters.

MIT licensed. See `LICENSE`.
