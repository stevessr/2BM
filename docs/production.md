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
