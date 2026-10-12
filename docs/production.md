# 2BM Production Deployment / Release Gate

**Status: Experimental. Not approved for production without live acceptance tests.**

2BM is a Matrix Application Service requiring an existing Synapse-compatible
homeserver. "Virtual rooms" are ordinary private Matrix rooms. The bridge
does not support Matrix E2EE, full media, complete edit/delete parity, or
timestamp-preserving backfill. It supports real QQ through NapCat/OneBot,
Telegram personal login through MTProto, and official bot compatibility modes.
Discord human-user selfbot automation is not supported.

## Deploy

1. Use one 2BM instance per database and external account. HA replicas are
   unsupported. Place NapCat and both 2BM listeners on trusted networks.
2. Copy .env.example to .env. Use independent high-entropy admin, AS and HS
   tokens, and an independent 32-byte encryption key. Store .env at chmod 0600.
3. Set BRIDGE_MATRIX_HOMESERVER to the Client API URL reachable from the
   2BM container; 127.0.0.1 in a container does not mean another container.
4. Generate Matrix registration from the same .env:
       python3 scripts/generate_registration.py --url http://2bm:9001
   Adjust URL to what Synapse can actually reach. Set chmod 0600.
5. Add the registration to Synapse homeserver.yaml under
   app_service_config_files, then restart Synapse. The registration reserves
   ghost user IDs and virtual-room aliases. Older registrations without the
   alias namespace must be regenerated.
6. Run:
       docker compose config
       docker compose up --build -d
       docker compose logs -f 2bm
       curl -fsS http://127.0.0.1:9001/readyz
7. Open the admin UI only behind an authenticated, TLS-protected proxy or an
   SSH tunnel; create external accounts with a specific Matrix owner MXID.
   Accept generated room invitations in the Matrix client.

## Secrets, privacy and reliability

- The Matrix AS token grants impersonation over the registered namespace.
  Limit access to the registration and do not log secrets.
- The QQ webhook requires its own shared secret; never expose NapCat's action
  API publicly. Restrict egress and protect remote endpoints with TLS.
- Telegram session SQLite files contain MTProto authorization keys and are
  NOT encrypted by the 2BM account-credential key. Encrypt their storage and
  backups. The bridge enforces mode 0700/0600 on Unix files/directories.
- The Docker sample restricts Linux capabilities, uses a read-only rootfs
  and local-only published ports, and provides a readiness healthcheck.
- A successful /healthz means only that the process responds. /readyz also
  checks DB and homeserver reachability. Neither guarantees QQ/TG health.
- Outbound deliveries are journaled before calling external APIs. An
  ambiguous timeout stays pending/uncertain: blindly replaying could create
  duplicate remote messages. Review the source chat and choose retry only if
  the external message was not sent, or acknowledge if it did arrive.
- A partial Matrix Application Service transaction may be re-sent. Remote
  platforms without caller-controlled idempotency are not exactly-once.
- Only a successful Telegram remote logout should allow removal of a live
  Telegram personal account. Local session file deletion by itself DOES NOT
  revoke remote authorization.

## Backup and restore

Stop the bridge before backing up or restoring its persistent volume. Preserve
SQLite, its WAL state, MTProto session directory, .env encryption key and
the matching Matrix AS registration as an encrypted, consistent unit. Test
restore on an isolated homeserver. Restoring just Matrix room aliases does not
restore old message IDs, deduplication or authorization sessions. Virtual
rooms also survive account deletion; manage their server-side retention
separately. Do not run two writers against the same DB and external accounts.

## Release acceptance checklist

- [ ] Rust CI (check/test/clippy) and Matrix registration generator tests pass
      on the exact release commit.
- [ ] Disposable real Synapse integration CI passes. It starts a real Matrix
      homeserver but stubs QQ via a local OneBot HTTP server.
- [ ] Test with a real isolated Synapse and Matrix client: ghost users, room
      aliases, owner invites, power levels, sync, edits/recalls fail safely.
- [ ] Test with real NapCat/QQ test account: DM, groups, group member churn,
      repeated webhooks, own-message echoes, replies and recalls.
- [ ] Test with real Telegram test account: code + 2FA, reconnect, dialog
      discovery, sends from another device, group messages, remote logout.
- [ ] Inject network timeouts, dropped callbacks, crashes and restarts;
      reconcile uncertain sends without unreviewed duplicates.
- [ ] 24-hour soak: no silent losses, acceptable p95 latency, CPU/RSS,
      storage/WAL growth and rate limit behavior.
- [ ] TLS/network controls, secret rotation, session revocation and backup
      restoration verified; operational owner trained in incident review.
- [ ] The operator explicitly accepts the unencrypted-text-only bridge scope.
      If encrypted conversations or file transfer are required, do not
      approve production until those features are built and verified.

## Known blockers and omissions

Matrix E2EE, attachment/media transfer, full cross-platform edits, native
historical timestamp insertion, unlimited/paginated Telegram dialog discovery,
scalable QQ group roster reconciliation, multi-replica support, metrics/alerts,
automatic retention pruning and complete secure Matrix room teardown have not
been validated or are not implemented. Real-account acceptance tests cannot be
substituted with mocks or a green compilation check.

## Additional production hardening (2026-10)

The Matrix Application Service keeps a durable per-event completion table in
addition to per-transaction records, and serializes inbound transactions within
each process. Restart/retry of partially completed transaction batches will not
reprocess completed Matrix events. This is not distributed HA.

The QQ self-device callback path now commits an encrypted SQLite queue record
before responding with success. A dedicated worker drains the queue and backs
off individually failing records up to one hour, avoiding starvation of healthy
accounts. The admin dashboard shows only IDs and delivery metadata, not text.
Monitor this queue and the outbound uncertain-send journal; investigate
persistent backlogs before they grow without bounds.

All SQLite pool connections explicitly set WAL, synchronous FULL, a busy
timeout and foreign key enforcement. Full sync reduces throughput and can
still be defeated by defective hardware, broken volume durability semantics
or inconsistent backup methods. Validate on the actual VPS filesystem.

Matrix E2EE rooms must NOT be mapped to this plaintext bridge. The API
rejects encrypted/unknown room state on manual mapping, and dynamically
quarantines rooms when encryption state is observed; source messages then
fail rather than entering Matrix as unencrypted data. Create a new
unencrypted room to recover, and inspect source messages that were not
successfully bridged while quarantined.

The Synapse integration suite now includes AS event replay under distinct
transaction IDs, source recall of Matrix-originated messages, self-first QQ
DM identity, and encryption quarantine. It remains a OneBot stub, not real QQ.

## Durable callback semantics and queue alarms

The bridge now **ACKs QQ message/notice webhooks only after inserting an encrypted
SQLite record**. The webhook return code no longer tells NapCat that Matrix
delivery succeeded: the message may remain queued due to rate limits, Matrix
outage or unsupported encryption. Monitor `/api/admin/qq-inbox` and
`/api/admin/telegram-inbox`, including `pending`, `attempts` and
`next_attempt_at`. Do not expose these APIs without admin authentication.

QQ `message`, `message_sent`, `group_recall`, `friend_recall`,
`group_increase`, and `group_decrease` notifications are retained before
ACK; other OneBot notice types are currently ignored by design. Repeated
message/recall callbacks use stable identifiers; membership notices lack a
reliable unique upstream ID and may be applied repeatedly, with idempotent
join/leave semantics.

The Telegram human-account update loop persists each supported `NewMessage`
into an encrypted SQLite inbox, independent from the actual Matrix send.
Disk errors abort update processing rather than silently advancing past an
unaccepted callback. A disabled account must not cause queued messages to be
mistaken for delivered ones. Check both queues and the separate outbound
uncertain-delivery journal before removing an external account.

Backoff grows as 5, 20, 45, ... seconds and is capped at 3600 seconds per
message. A recovered external connection or Matrix homeserver does **not**
guarantee immediate delivery of an older backlogged event. Provision disk
space for the worst planned outage; queued content is encrypted in SQLite
but metadata, logs, memory and filesystem backups still require protection.
`synchronous=FULL` assumes real filesystem fsync durability. Do not use
unsafe network or ephemeral filesystems for authoritative DB storage.

## Alias ownership, live encryption and independent inbox consumers

During SQLite mapping recovery, an existing Matrix alias is never sufficient to
prove that the room belongs to the account. The appservice verifies the
`com.2bm.bridge` state marker using the bridge-owned room creator identity.
Do not manually delete or edit bridge marker state without expecting recovery
to fail closed. A foreign or inaccessible room alias is a security incident.

Every inbound plaintext Matrix ghost message additionally checks the live
room `m.room.encryption` state, using the Ghost sender. This adds one read
request per delivered source message but closes the gap after missed state
events, restores and stale caches. When homeserver access fails, delivery
remains queued for operator review/retry; never turn off the check to force
recovery into an encrypted room.

QQ self events, other QQ callbacks and Telegram personal-account updates have
independent queue-consumer loops and 20-second per-attempt timeouts. Source
webhooks are acknowledged **only after durable insertion**; metrics in WebUI
show pending and retry state, not chat bodies. An account deactivated while a
callback is queued does not mark that message delivered. Operators must
resolve lingering encrypted inbox entries before removing the account.

Telegram dialog discovery scans beyond the original fixed 200-dialog cap and
finds successive batches of new conversations. A defensive maximum of 10,000
dialog entries per scan still applies to pathological accounts, and a very
large contact graph may need separate import/runbook planning.

The Rust integration suite now mocks a Homeserver to exercise real encrypted
Telegram queue delivery and repeat-update suppression. It still does not
connect to Telegram MTProto in live mode, and the QQ integration uses a local
OneBot stub rather than a real NapCat installation.

## Administrator queue reconciliation

Authenticated `POST /api/admin/queues/retry` accepts:
`{"account_id":"<account UUID>","source_key":"<exact inbox key>",
"kind":"qq_events"}`. The queue kind must be `qq_own`, `qq_events`,
or `telegram_events`. The WebUI exposes this as **立即重试** next to
each queued message. It does not directly submit a message to the remote
source and does not override E2EE or account-disable safeguards.

Account deletion will be **refused (409)** until the account's QQ/Telegram
inbox queues and unresolved Matrix-to-external sends are reconciled. This
avoids SQLite foreign-key cascades losing undelivered data. Log out and
revoke Telegram remote sessions separately; deleting a local database row
alone cannot revoke upstream authorization. If pending records are stuck
because the account is no longer active, back them up and establish an
explicit data-retention/discard policy before destructive action.

Each QQ self-message, ordinary QQ callback, and Telegram inbound consumer
runs independently, with up to 20 selected events per iteration and a
20-second timeout per attempt. Rate limits and backlog handling are
deliberately conservative; realistic production traffic and long outages
still require load testing against the actual VPS.

## Storage exhaustion and crash-state drills

New inbound callback records are limited to 20,000 per account across all
three encrypted inboxes, with a maximum 256 KiB encrypted payload each.
These are protective maxima, NOT a disk quota. Combined ciphertext, SQLite
WAL, Telegram session files, logs, OS metadata and backups may require
substantially more space. Monitor free bytes/inodes and set an external alert
well before filesystem exhaustion. `GET /api/admin/queues/capacity` returns
per-account pressure; the WebUI shows it alongside the retry controls.

HTTP webhook ACK means the event was durably inserted or already present.
When storage exceeds per-account pressure, new QQ webhook messages respond
503; an oversized encrypted event responds 413. These are *not* accepted:
not every NapCat installation retries HTTP failures. Verify source retry
behavior on the exact installed version and provide historical reconciliation
for discarded source events. Existing duplicate callback IDs are accepted
at capacity to avoid damaging upstream replay.

Test a full queue, a corrupt encrypted row, a disabled account, Matrix
outage, disk-full behavior, and a crash after a remote send before its receipt
is committed. The last failure leaves a `delivery_attempts.status=sending`
row: 2BM now rewrites these to `uncertain` during startup. Check the source
conversation and resolve using the separate outbound-delivery incident UI.
Do not simply delete SQLite journal entries to silence alarms.

For Telegram personal accounts, encrypted inbox insertion precedes update
state checkpoints, with a checkpoint every 64 accepted new messages and at
graceful stream interruption when safe. A failed durable insertion prevents
advancing the source cursor. This improves but does not establish exactly
once delivery in the presence of upstream session changes or external
Telegram authorization loss.

Matrix alias recovery recognizes Synapse's 400/M_ROOM_IN_USE as well as
equivalent 409/M_ROOM_IN_USE responses, then verifies the original signed
m.room.create event sender and com.2bm.bridge event sender/content. A mapping
will not recover if a different party controls that alias, which is preferable
to cross-account chat disclosure.
