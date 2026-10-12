#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
WORK="$RUNNER_TEMP/2bm-smoke-synapse"
BRIDGE_WORK="$RUNNER_TEMP/2bm-smoke-bridge"
mkdir -p "$WORK" "$BRIDGE_WORK"
BRIDGE_PID=""
SYNAPSE_IMAGE="matrixdotorg/synapse:latest"
export BRIDGE_ADMIN_TOKEN="$(printf 'a%.0s' {1..64})"
export BRIDGE_MATRIX_AS_TOKEN="$(printf 'b%.0s' {1..64})"
export BRIDGE_MATRIX_HS_TOKEN="$(printf 'c%.0s' {1..64})"
export BRIDGE_CREDENTIAL_KEY="$(printf 'd%.0s' {1..64})"
export BRIDGE_MATRIX_HOMESERVER="http://127.0.0.1:8008"
export BRIDGE_MATRIX_SERVER_NAME="smoke.test"
export BRIDGE_ADMIN_LISTEN="127.0.0.1:9000"
export BRIDGE_MATRIX_LISTEN="0.0.0.0:9001"
export BRIDGE_DATABASE_URL="sqlite://$BRIDGE_WORK/bridge.sqlite"
export BRIDGE_TG_SESSION_DIR="$BRIDGE_WORK/telegram"
docker run --rm -v "$WORK:/data" -e SYNAPSE_SERVER_NAME=smoke.test \
    -e SYNAPSE_REPORT_STATS=no "$SYNAPSE_IMAGE" generate
sudo chmod -R a+rwX "$WORK"
python3 scripts/generate_registration.py --output "$WORK/registration.yaml" \
    --url "http://host.docker.internal:9001"
python3 - "$WORK/homeserver.yaml" <<'PY'
from pathlib import Path
import re
import sys
path=Path(sys.argv[1])
config=path.read_text()
config=re.sub(r"(?m)^app_service_config_files:\s*.*\n","",config)
config+=("\napp_service_config_files:\n"
         "  - /data/registration.yaml\n"
         "registration_shared_secret: ci-registration-secret\n")
path.write_text(config)
PY
sudo chmod -R a+rwX "$WORK"
docker run -d --name 2bm-ci-synapse \
    --add-host=host.docker.internal:host-gateway \
    -p 127.0.0.1:8008:8008 \
    -v "$WORK:/data" "$SYNAPSE_IMAGE"
cleanup() {
    echo "=== 2BM logs ==="
    cat "$BRIDGE_WORK/bridge.log" 2>/dev/null || true
    echo "=== Synapse logs ==="
    docker logs --tail 150 2bm-ci-synapse || true
    docker rm -f 2bm-ci-synapse || true
    if [ -n "$BRIDGE_PID" ]; then kill "$BRIDGE_PID" || true; fi
}
trap cleanup EXIT
for i in $(seq 1 120); do
    if curl --silent --fail http://127.0.0.1:8008/_matrix/client/versions >/dev/null; then
        break
    fi
    sleep 2
done
curl --fail --silent http://127.0.0.1:8008/_matrix/client/versions >/dev/null
docker exec 2bm-ci-synapse register_new_matrix_user \
    -c /data/homeserver.yaml --no-admin -u owner \
    -p ci-owner-password http://127.0.0.1:8008
./target/debug/two-bm > "$BRIDGE_WORK/bridge.log" 2>&1 &
BRIDGE_PID=$!
for i in $(seq 1 60); do
    if curl --silent --fail http://127.0.0.1:9001/readyz >/dev/null; then
        break
    fi
    if ! kill -0 "$BRIDGE_PID" 2>/dev/null; then
        echo "Bridge exited before ready"
        exit 1
    fi
    sleep 1
done
python3 scripts/smoke_synapse.py
