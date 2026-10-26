#!/usr/bin/env bash
# Run python integration tests against a throwaway duckport-server on a fresh database.
#
#   tests/python/run_with_server.sh test_doctor.py test_upgrade.py ...
#
# Env: DUCKPORT_BIN (default target/release/duckport-server), PORT (50071), PYTHON (python3),
# TAIL (log lines shown per test, 4). Each test gets the server at DUCKPORT_SERVER / DUCKPORT_ADDR.
set -u
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BIN="${DUCKPORT_BIN:-$ROOT/target/release/duckport-server}"
PORT="${PORT:-50071}"
PYTHON="${PYTHON:-python3}"
WORK="$(mktemp -d)"

DUCKPORT_DB_PATH="$WORK/it.db" DUCKPORT_LISTEN_ADDR="127.0.0.1:$PORT" DUCKPORT_SEED_DEMO=1 RUST_LOG=info \
  "$BIN" > "$WORK/server.log" 2>&1 &
PID=$!
for _ in $(seq 1 50); do (echo > "/dev/tcp/127.0.0.1/$PORT") 2>/dev/null && break; sleep 0.2; done

rc=0
for t in "$@"; do
  echo "################ $t"
  (cd "$HERE" && DUCKPORT_SERVER="grpc://127.0.0.1:$PORT" DUCKPORT_ADDR="127.0.0.1:$PORT" \
     DUCKPORT_TEST_ARCHIVE_DIR="$WORK/archive" timeout 900 "$PYTHON" -u "$t") > "$WORK/$t.log" 2>&1
  r=$?
  [[ $r -ne 0 ]] && rc=1
  echo "exit=$r"
  tail -"${TAIL:-4}" "$WORK/$t.log"
done
kill "$PID"; wait "$PID" 2>/dev/null
if grep -q "panicked" "$WORK/server.log"; then
  echo "---- server panicked"; grep -A3 "panicked" "$WORK/server.log" | head -20; rc=1
fi
rm -rf "$WORK"
exit $rc
