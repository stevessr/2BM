# 2BM — Telegram, QQ & Discord ↔ Matrix

A modular **Rust Matrix Application Service bridge** with a lightweight, independently served account-management WebUI.

**Status: experimental MVP; QQ and Telegram have human-account modes; Discord only supports the official bot mode.** Plain-text messages can travel in both directions after configuration. 2BM is **not a Matrix homeserver** and requires Synapse or another homeserver supporting Application Services.

## Production deployment and release criteria

**2BM is not yet approved for production use.** See [the production runbook](docs/production.md) for deployment, TLS, session protection, backups, manual delivery reconciliation and a required real-account release checklist.

CI now includes a [real Synapse integration workflow](.github/workflows/synapse-integration.yml) with a local OneBot stub. This exercises the actual Matrix appservice protocol but does not replace testing with genuine NapCat/QQ and Telegram accounts.

Admin APIs also expose per-account adapter state, pending/uncertain outbound deliveries and encrypted-room warnings. Matrix E2EE is **not** supported: enabling encryption on a bridged room blocks plaintext forwarding and records an operator warning.

## Components and ports

- **Matrix AS listener** (default `127.0.0.1:9001`): receives `/_matrix/app/v1/transactions/{txn}` and QQ OneBot webhook events at `/hooks/qq/{account-id}`.
- **Admin listener** (default `127.0.0.1:9000`): serves `/admin` and protected `/api/admin/*` API.
- **Combined listener**: set `BRIDGE_COMBINED_LISTEN=0.0.0.0:9000` (or set both listener addresses equal) to serve both APIs on one port.
- **Persistence**: SQLite with encrypted account credentials, room mappings, message deduplication and Telegram offsets.
- **Background adapters**: Telegram personal MTProto via Rust grammers (with WebUI phone-code/2FA authentication), legacy Telegram Bot API polling, Discord Bot Gateway; QQ OneBot 11 HTTP event callback.

## Quick start

1. Copy `.env.example` to `.env`. Generate independent random tokens for the three TOKEN values using `openssl rand -hex 32`, and a credential encryption key using `openssl rand -hex 32`. Do not commit the secrets.
2. Set `BRIDGE_MATRIX_HOMESERVER` to the Client API URL that the bridge can reach and `BRIDGE_MATRIX_SERVER_NAME` to the Matrix **server name**.
3. Generate your registration: `python3 scripts/generate_registration.py --url http://127.0.0.1:9001`. The URL must be reachable **from the homeserver**, so use `http://2bm:9001` on a shared Docker network, for example. Register the generated `registration.yaml` in Synapse and restart Synapse.
4. Run `cargo run --release`, or `docker compose up --build -d`, then open <http://127.0.0.1:9000/admin>. Log in with `BRIDGE_ADMIN_TOKEN`.
5. Add a platform account **with a Matrix owner ID** such as `@me:example.org`. On incoming messages rooms are created and the owner is invited automatically. Alternatively invite `@_2bm:YOUR_MATRIX_SERVER_NAME` to the Matrix room, then create a mapping using the actual `!room:server` ID.

Accounts and external chats are mapped to Matrix rooms. Incoming messages appear under stable Matrix ghost identities. Unmapped conversations automatically create Matrix rooms and invite the Matrix owner. Only the owner can control the external account through the room.

## Connecting external platforms

| Network | Receive | Send | External chat ID |
| --- | --- | --- | --- |
| Telegram | Bot API `getUpdates` polling | Bot API `sendMessage` | integer chat ID, e.g. `-1001234567890` |
| Discord | Gateway `MESSAGE_CREATE` | REST channel message | Discord channel snowflake |
| QQ | OneBot 11 HTTP webhook | OneBot 11 HTTP API | `group:123456` or `private:123456` |

**Telegram Bot compatibility mode:** Create a bot in BotFather, add it to your chats, and configure its privacy settings and message access as needed. Do not poll the same bot token elsewhere.

**Telegram personal account (Rust MTProto):** Create an API ID and API hash at my.telegram.org. In WebUI choose **Telegram 真人账号**, set the owner Matrix ID and the API credentials, then enter your phone verification code and optional 2FA password. Authorized accounts reconnect using session SQLite files. Telegram sessions contain authorization keys and must be kept private.

**Discord (currently BOT mode; ordinary user automation violates Discord policy)**: Create an application/bot and grant it read/send access. Enable the privileged **Message Content Intent** to receive message content. Use a bot token, not a user token.

**QQ (HUMAN account)**: Perform QQ login in **NapCat or another OneBot 11-compatible connector**. In WebUI, supply `api_base` (e.g. `http://napcat:3000`), `access_token`, and `hook_secret`. Configure OneBot HTTP event delivery to:

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
| `BRIDGE_TG_SESSION_DIR` | Telegram personal MTProto session directory (protect as highly sensitive) |
| `RUST_LOG` | Runtime log filter |

**Telegram MTProto session files are currently unencrypted SQLite storage, separate from encrypted account settings**. Restrict filesystem permissions, keep backups private, and do not expose the session directory through the WebUI. Back up **both** SQLite and the key: the encrypted account credentials cannot be recovered without the key. Admin credentials must be protected by HTTPS when accessed remotely. Docker Compose exposes both ports on localhost only by default; adjust networking for your homeserver/OneBot connector.

## Available endpoints

The admin REST API requires `Authorization: Bearer $BRIDGE_ADMIN_TOKEN`.

- `GET/POST /api/admin/accounts` and `DELETE /api/admin/accounts/{id}`
- `GET /api/admin/virtual-rooms` and `POST /api/admin/accounts/{id}/sync` (JSON `{"limit":50}`)
- `GET/POST /api/admin/mappings` and `DELETE /api/admin/mappings/{id}` (manual advanced mapping)
- On Matrix listener: `GET /healthz`, `PUT /_matrix/app/v1/transactions/{txn}`, `POST /_matrix/app/v1/ping`, user lookup, and `POST /hooks/qq/{account-id}`.

## MVP limitations

**Implemented:** plaintext message bridging with virtual Matrix rooms and individual ghost senders; QQ friend/group and Telegram dialog discovery; admin-triggered and limited automatic room provisioning; account management; split/combined HTTP listeners; encrypted account credentials; SQLite room mappings, sender registry, incoming deduplication and Matrix transaction deduplication; Telegram poll offsets; Discord Gateway reconnect; OneBot HTTP integration.

**Not yet implemented:** complete Telegram dialogs pagination beyond the current discovery cap, historic message backfill, full remote membership snapshot, Discord human login (official API does not support self-bots), Matrix E2EE, QQ QR login inside 2BM, file/media transfer, edits/deletes/reactions/replies, presence/typing/read receipts, Discord Gateway resume, HA multi-replica mode or full Matrix third-party lookups. Only standard plaintext messages and captions are handled.

Inbound messages are sent as per-contact Matrix ghost identities rather than under a single source-prefixed bot. Each real external chat maps to a persistent private Matrix room. Outbound Matrix messages are accepted only from the account's configured Matrix owner. External outbound delivery is at-least-once across crashes between network delivery and local acknowledgement; avoid active replicas using the same account and database.

## Development

```bash
cargo check --all-targets
cargo test
cargo fmt --all
```

Main modules: `src/adapters` for platforms, `src/matrix.rs` for Matrix Client API calls, `src/relay.rs` for routing, `src/web.rs` for WebUI/API, and `src/workers.rs` for background adapters.

MIT licensed. See `LICENSE`.


### Build prerequisites
Install `pkg-config` and `libsqlite3-dev` on Debian/Ubuntu. SQLx dynamically links to the system SQLite shared object via `.cargo/config.toml`, while Telegram Grammers uses libsql's bundled SQLite; this avoids duplicate `sqlite3_*` symbols at link time.

## Virtual rooms (default)

2BM creates **real, private Matrix rooms managed as virtual external conversations**, not synthetic ephemeral rooms. This works in standard Element/FluffyChat clients. On startup QQ contacts/groups are discovered through OneBot, while Telegram contacts/chats are discovered from MTProto user dialogs; Matrix owners are automatically invited. New messages also lazily create rooms. There is no history backfill yet.

- DM: the external contact's Matrix ghost user creates the room and invites the configured Matrix owner. Group: the 2BM service user creates the room, while group participants join as individual ghost users when they send messages.
- Every external chat has an opaque deterministic alias `#_2bm_r_<uuid>:server`. If room creation succeeds but SQLite persistence fails, a retry resolves the existing alias rather than creating a second room.
- `GET /api/admin/virtual-rooms` lists discovered rooms. `POST /api/admin/accounts/{id}/sync` with JSON `{"limit":50}` synchronizes the next batch of unmapped conversations (1–100 per request); repeat to progressively provision the remaining chats.
- Background provisioning starts with up to 30 existing conversations per QQ account (every ten minutes) and Telegram user account (on session reconnect). Telegram dialog discovery currently caps each scan at 200 conversations; larger accounts will require pagination support.
- SQLite persists the virtual-chat catalogue and the ghost MXID registry. Manual mappings remain available under the advanced section of WebUI.
- Private rooms still reside on the Matrix homeserver and count against its storage. Matrix E2EE and full remote membership/history mirroring remain unsupported.

**Operational note:** the first sync can issue many Matrix createRoom, register, profile, and invite requests. Keep the batch size moderate to avoid Matrix homeserver rate limits and do not run several 2BM processes against the same accounts.

## QQ group membership sync and Telegram recent-message import

- **QQ roster:** authenticated OneBot `get_group_member_list` can populate Ghost users in an existing Matrix virtual group room. Management endpoint `POST /api/admin/accounts/{id}/qq/members` accepts `{"chat":"group:12345","limit":20}` (1–100). `group_increase` and `group_decrease` notifications update the Matrix ghost membership for already-mapped rooms. This is deliberately bounded to avoid mass registration for large groups; it does not yet paginate through every member of huge groups.
- **Telegram recent import (opt in only):** `POST /api/admin/accounts/{id}/telegram/history` accepts `{"chat":"-10012345","limit":20}` (1–50), requires an existing Matrix virtual room and an online Telegram user session. The latest messages are imported oldest-first, with stable source IDs to avoid duplication. Text messages sent by other Telegram devices are shown via an account-scoped "Me (Telegram)" ghost identity.
- This is **not Matrix-native historical backfill**. The imported messages receive **new Matrix timestamps**, not the original Telegram sent timestamp. Import is disabled by default, explicitly initiated by an administrator, and only supports text content. It does not restore edits, reactions, attachments, or historical membership.
- **QQ recall:** OneBot `group_recall` and `friend_recall` notices redact linked incoming Matrix events using the original ghost identity. A recall arriving before its message is persisted as a tombstone, preventing the removed text from being replayed later. There is not yet a general bidirectional edit/delete/recall feature.

## Linked replies, recalls, and room title refresh

- A mapping table tracks `(account_id, external_chat, external_message)` to the Matrix event ID and the ghost identity that sent it.
- OneBot `friend_recall` and `group_recall` events now redact matching Matrix events. Out-of-order recalls are persisted as tombstones, preventing later replay. Unknown/unbridged messages are ignored safely.
- Matrix replies to already-bridged QQ messages use OneBot 11's native `reply` segment, provided that the quoted Matrix event belongs to the **same** QQ account and chat. Other replies fall back to plaintext without Matrix quote boilerplate. `m.replace` Matrix edit events are intentionally ignored until actual bidirectional edits are implemented.
- QQ group member sync advances past members already recorded in `qq_synced_members` on repeated requests; QQ join/leave notifications update that registry. Membership is best-effort and requires real-home-server testing.
- Existing Matrix room names follow changed QQ/TG conversation titles during sync, using the service/DM ghost identity as the Matrix state-event author. If the homeserver rejects a rename, SQLite retains the old synced name so subsequent sync attempts retry.
- **Not yet supported:** Matrix-to-QQ recall, Telegram native reply/edit/delete mapping, attachments/media transfer, full native timestamp-preserving history backfill, group-member pagination across all QQ/TG groups, and historical member join timestamps.

### Verification

Rust CI runs `cargo check --all-targets`, `cargo test --all-targets`, and `cargo clippy --all-targets`. Mock-server tests include Matrix room alias crash recovery, QQ source recall idempotency/tombstones, and Matrix-to-QQ native replies with repeated AS transaction suppression. These tests do **not** replace a real Synapse/QQ/Telegram end-to-end exercise.

## Crash-safe delivery and encryption policy

- **Application Service replay:** Matrix transactions run serially within one 2BM process, with per-event SQLite checkpoints. Repeated events from different transaction IDs are skipped, including after a partial batch failure. This does **not** support multi-instance HA, which requires distributed coordination.
- **QQ self-device webhooks:** authenticated `message_sent` payloads are XChaCha-encrypted and committed to `qq_own_inbox` before returning HTTP 200. A separate worker drains up to 100 queued items every two seconds. Failed items receive exponential backoff (capped at one hour), without starving other queued accounts. The admin panel lists queue counts and retry metadata, never decrypted chat bodies.
- **Database durability:** every SQLite connection uses WAL, `synchronous=FULL`, foreign key enforcement and a 10-second busy timeout. The extra filesystem synchronization favors safe webhook acknowledgments over maximum throughput.
- **Encrypted Matrix rooms:** unsupported E2EE rooms are quarantined. 2BM refuses to deliver QQ/Telegram plaintext to them and refuses new manual mappings if the Matrix room is encrypted or its encryption state cannot be verified. Once enabled, Matrix room encryption cannot simply be turned off; create a new unencrypted virtual room.
- **Privileged operations:** DM Ghost creators send member invitations. QQ recalls targeting messages initially sent from Matrix use a sufficiently privileged bridge-owned room creator identity rather than attempting to impersonate the real Matrix user.
- **Tests:** Rust CI checks schema/inbox/actor properties. A real Synapse integration job validates virtual-room identities, native replies, callback retransmission, source recalls, appservice event replay, own-message echoes and encrypted-room plaintext quarantine.

**Remaining production blockers:** real NapCat and Telegram human-account interoperability, 24h soak, fault injection around remote delivery ambiguity, secure backup/restore acceptance, attachment/E2EE support if required.

## Durable QQ/Telegram ingestion

Recent hardening expands the crash-resilient SQLite inbox to cover **all inbound OneBot events**, not only messages sent from another QQ device. Authenticated QQ message, recall and group-membership webhooks are XChaCha20-Poly1305 encrypted and saved to `qq_events_inbox` before returning HTTP 200. The independent worker retries failures with capped backoff. A successful webhook HTTP response therefore means **durably accepted**, not that Matrix delivery has already completed. Administrators can inspect pending counts/retries via `GET /api/admin/qq-inbox`.

Telegram **human-account MTProto** updates are also encrypted into `telegram_events_inbox` before progressing the update loop. A separate delivery path replays failures after restart and deduplicates per `(account, dialog, message_id)`. `GET /api/admin/telegram-inbox` and the WebUI expose queue metadata without text. Errors while persisting Telegram updates stop stream consumption; updates not durably accepted must not be intentionally marked processed. Bot-mode Telegram polling continues to use its existing persisted offset.

The outgoing Matrix→remote delivery journal remains separate, because QQ/Telegram remote sends have no universal caller-controlled idempotency token. Ambiguous remote sending requires operator reconciliation, not blind retry.
