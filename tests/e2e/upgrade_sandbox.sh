#!/usr/bin/env bash
# `duckport upgrade` end to end, in a sandbox installation next to the real one.
#
# Builds a second deployment under $SBX with its own systemd units ($PREFIX-server,
# $PREFIX-ingestor@), port, database and CLI: duckport-server $OLD_SERVER_TAG from the GitHub
# release and both plugins at their 0.1.0 commits, writing real Binance data into keyed
# tables. Then runs `duckport upgrade` against "origin" repositories holding the commits
# under test, and checks: server restarted at the new version, plugins reinstalled and
# running, data aligned (doctor clean), ingestors still writing, a second run does nothing,
# `upgrade --compact` refuses a half-done swap and otherwise rewrites the database again,
# and the release binary is only downloaded when its sha256 differs, and then verified.
#
# With LEGACY_CLI=<tag> the upgrade is started by that release's CLI instead. Its `upgrade`
# only pulls and runs deploy.sh, so deploy.sh has to hand over to the new CLI.
#
# Needs root, systemd, uv, network (via $PROXY) and the three repositories checked out
# side by side with the commits to test (uncommitted changes are not used).
#
#   sudo tests/e2e/upgrade_sandbox.sh            # run, then remove the sandbox
#   sudo KEEP=1 tests/e2e/upgrade_sandbox.sh     # keep it for inspection
#   sudo tests/e2e/upgrade_sandbox.sh --cleanup  # remove a kept sandbox
#   sudo LEGACY_CLI=v1.0.4 tests/e2e/upgrade_sandbox.sh  # start the upgrade with the v1.0.4 CLI
#   sudo MEMORY_LIMIT=500GB EXPECT_MEMORY_CAP=1 tests/e2e/upgrade_sandbox.sh  # server.env limit above the host's RAM
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PLUGINS_DIR="${PLUGINS_DIR:-$(dirname "$REPO")}"
SBX="${SBX:-/root/work/sbx}"
PREFIX="${PREFIX:-dptest}"
PORT="${PORT:-50081}"
PROXY="${PROXY:-http://localhost:1082}"
OLD_SERVER_TAG="${OLD_SERVER_TAG:-v1.0.4}"
OLD_SERVER_BIN="${OLD_SERVER_BIN:-}"      # a local copy of that release binary skips the download
LEGACY_CLI="${LEGACY_CLI:-}"              # start the upgrade with this release's CLI (e.g. v1.0.4)
OLD_INGESTOR="${OLD_INGESTOR:-4c8f295}"   # binance-ingestor 0.1.0
OLD_FUNDING="${OLD_FUNDING:-a049f7f}"     # binance-funding 0.1.0
PROD_PORT="${PROD_PORT:-8815}"            # the real deployment, which must stay untouched
export DUCKPORT_HOME="$SBX/opt"
CLI="$SBX/bin/duckport"
PY="${PY:-$(ls -d /opt/duckport/ingestors/*/.venv/bin/python3 | head -1)}"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO/target}"   # reuse the build cache
[[ "$PREFIX" != "duckport" ]] || { echo "PREFIX must not be duckport"; exit 1; }

cleanup() {
  systemctl stop "$PREFIX-ingestor@*" "$PREFIX-server" 2>/dev/null || true
  systemctl disable "$PREFIX-server" 2>/dev/null || true
  rm -f "/etc/systemd/system/$PREFIX-server.service" "/etc/systemd/system/$PREFIX-ingestor@.service"
  systemctl daemon-reload
  rm -rf "$SBX"
}
if [[ "${1:-}" == "--cleanup" ]]; then cleanup; exit 0; fi

step() { echo -e "\n\033[1m######## $*\033[0m"; }
fail() { echo -e "\033[0;31mFAIL: $*\033[0m"; exit 1; }
q() {  # q PORT SQL -> rows as JSON lines
  "$PY" -c "
import json, sys
sys.path.insert(0, '$REPO/scripts')
from duckport_flight import Duckport
for r in Duckport('127.0.0.1:' + sys.argv[1]).query(sys.argv[2]):
    print(json.dumps(r, default=str))" "$1" "$2"
}
scalar() { q "$1" "$2" | head -1 | "$PY" -c "import json,sys; d=json.loads(sys.stdin.read() or 'null'); print('' if d is None else list(d.values())[0])"; }

step "fresh sandbox in $SBX (units $PREFIX-*, port $PORT)"
cleanup
mkdir -p "$SBX"/{origin,code,bin,data}
export https_proxy="$PROXY" http_proxy="$PROXY" no_proxy="localhost,127.0.0.1"

# "origin" repositories: main = the commits under test; duckport-rs gets one more commit so
# that the upgrade really pulls and continues with the pulled CLI.
for r in duckport-rs duckport-binance-ingestor duckport-binance-funding; do
  src="$PLUGINS_DIR/$r"; [[ "$r" == duckport-rs ]] && src="$REPO"
  git clone -q --bare "$src" "$SBX/origin/$r.git"
  git -C "$SBX/origin/$r.git" branch -f main "$(git -C "$src" rev-parse HEAD)"
  git -C "$SBX/origin/$r.git" symbolic-ref HEAD refs/heads/main
done
git clone -q "$SBX/origin/duckport-rs.git" "$SBX/tmp-rs"
git -C "$SBX/tmp-rs" -c user.name=sbx -c user.email=sbx@localhost commit -q --allow-empty -m "sbx: next commit"
git -C "$SBX/tmp-rs" push -q origin main
rm -rf "$SBX/tmp-rs"

# The sandbox's checkouts start one commit behind (duckport-rs) and at the 0.1.0 plugins.
git clone -q "$SBX/origin/duckport-rs.git" "$SBX/code/duckport-rs"
git -C "$SBX/code/duckport-rs" reset -q --hard HEAD~1
git clone -q "$SBX/origin/duckport-binance-ingestor.git" "$SBX/code/duckport-binance-ingestor"
git -C "$SBX/code/duckport-binance-ingestor" reset -q --hard "$OLD_INGESTOR"
git clone -q "$SBX/origin/duckport-binance-funding.git" "$SBX/code/duckport-binance-funding"
git -C "$SBX/code/duckport-binance-funding" reset -q --hard "$OLD_FUNDING"

step "install duckport-server $OLD_SERVER_TAG"
(cd "$SBX/code/duckport-rs" && DEPLOY_OPT="$SBX/opt" DEPLOY_DATA="$SBX/data" DUCKPORT_UNIT_PREFIX="$PREFIX" \
  DUCKPORT_CLI_DIR="$SBX/bin" LISTEN_ADDR="127.0.0.1:$PORT" MEMORY_LIMIT="${MEMORY_LIMIT:-4GB}" RELEASE_TAG="$OLD_SERVER_TAG" \
  DUCKPORT_SERVER_BIN="$OLD_SERVER_BIN" \
  bash deploy.sh > "$SBX/deploy-initial.log" 2>&1) || { tail -20 "$SBX/deploy-initial.log"; fail "initial deploy"; }
grep -q "^EnvironmentFile=$SBX/opt/server.env$" "/etc/systemd/system/$PREFIX-server.service" \
  || fail "$PREFIX-server.service does not use $SBX/opt"
systemctl start "$PREFIX-server"
for i in $(seq 1 60); do nc -z localhost "$PORT" && break; sleep 1; done
[[ "$(scalar "$PORT" "SELECT library_version FROM pragma_version()")" != "" ]] || fail "sandbox server not up"

step "plugin instances at 0.1.0"
start_date=$(date -u -d '-2 day' +%F)
make_instance() {  # name exec source
  local dir="$SBX/opt/ingestors/$1"
  mkdir -p "$dir"
  uv venv -q --python 3.12 "$dir/.venv"
  uv pip install -q -e "$3" --python "$dir/.venv/bin/python3"
  cat > "$dir/config.env" <<EOF
INGESTOR_EXEC=$2
KLINE_INTERVAL=5m
DATA_SOURCES=usdt_perp
CONCURRENCY=2
START_DATE=$start_date
PROXY_URL=$PROXY
INGESTOR_NAME=$1
DUCKPORT_LISTEN_ADDR=127.0.0.1:$PORT
EOF
  systemctl start "$PREFIX-ingestor@$1"
}
make_instance sbx5m binance-ingestor "$SBX/code/duckport-binance-ingestor"
make_instance sbxfund binance-funding "$SBX/code/duckport-binance-funding"

step "wait for the old plugins to fill keyed tables"
for i in $(seq 1 120); do
  k=$(scalar "$PORT" "SELECT count(*) FROM duckdb_tables() WHERE schema_name = 'data' AND table_name IN ('usdt_perp_5m', 'usdt_perp_funding', 'exginfo') AND estimated_size > 0")
  [[ "$k" == "3" ]] && break
  sleep 5
done
[[ "$k" == "3" ]] || { journalctl -u "$PREFIX-ingestor@sbx5m" -n 30 --no-pager; fail "old plugins wrote no data"; }
q "$PORT" "SELECT table_name, estimated_size FROM duckdb_tables() WHERE schema_name = 'data' ORDER BY 1"
keyed=$(scalar "$PORT" "SELECT count(*) FROM duckdb_constraints() WHERE schema_name = 'data' AND constraint_type = 'PRIMARY KEY' AND table_name IN ('usdt_perp_5m', 'usdt_perp_funding', 'exginfo')")
[[ "$keyed" == "3" ]] || fail "expected 3 keyed tables from the 0.1.0 plugins, got $keyed"
[[ "$(scalar "$PROD_PORT" "SELECT count(*) FROM data.watermark WHERE ingestor LIKE 'sbx%'")" == "0" ]] \
  || fail "a sandbox instance wrote to the real deployment"
rows_before=$(scalar "$PORT" "SELECT count(*) FROM data.usdt_perp_5m")

if [[ -n "$LEGACY_CLI" ]]; then
  # No --plan here: a legacy CLI ignores its arguments and would run the upgrade.
  step "$LEGACY_CLI CLI: its upgrade only pulls and runs deploy.sh"
  # That CLI has /opt/duckport built in; point it at the sandbox.
  git -C "$REPO" show "$LEGACY_CLI:duckport" | sed "s|DEPLOY_OPT=\"/opt/duckport\"|DEPLOY_OPT=\"$SBX/opt\"|" > "$CLI"
  grep -q "DEPLOY_OPT=\"$SBX/opt\"" "$CLI" || fail "could not point the $LEGACY_CLI CLI at the sandbox"
  grep -q "^cmd_upgrade()" "$CLI" && ! grep -q -- "--after-pull" "$CLI" || fail "$LEGACY_CLI is not a legacy CLI"
else
  step "duckport upgrade --plan"
  rc=0; "$CLI" upgrade --plan --proxy "$PROXY" | tee "$SBX/plan.log" || rc=$?
  [[ $rc -eq 1 ]] || fail "plan exit $rc (expected 1: work to do, nothing blocked once plugins are upgraded)"
  grep -q "v1.0.4 → v" "$SBX/plan.log" || fail "plan does not show the server upgrade"
fi

step "duckport upgrade --yes"
t_upgrade=$(date -u '+%F %T')
# A legacy CLI runs deploy.sh without DEPLOY_OPT, which would mean /opt/duckport.
DEPLOY_OPT="$SBX/opt" DEPLOY_DATA="$SBX/data" \
  "$CLI" upgrade --yes --proxy "$PROXY" --delete-bad-rows 2>&1 | tee "$SBX/upgrade-1.log"
[[ ${PIPESTATUS[0]} -eq 0 ]] || fail "upgrade failed"
grep -q "sha256 校验通过" "$SBX/upgrade-1.log" || fail "the downloaded binary was not checked against the release digest"
if [[ -n "${EXPECT_MEMORY_CAP:-}" ]]; then   # MEMORY_LIMIT above the host's RAM: stage 1 fixes server.env and restarts
  grep -q "超过本机内存" "$SBX/plan.log" || fail "plan did not warn about memory_limit above the host's memory"
  grep -q "server.env：DUCKPORT_DUCKDB_MEMORY_LIMIT=" "$SBX/upgrade-1.log" || fail "stage 1 did not fix server.env"
  grep -q "应用新的 memory_limit\|→ v" "$SBX/upgrade-1.log" || fail "no server restart after fixing server.env"
  want_mem="$(( $(awk '/^MemTotal:/{print $2}' /proc/meminfo) * 1024 / 2 / 1073741824 ))GB"
  grep -q "^DUCKPORT_DUCKDB_MEMORY_LIMIT=$want_mem\$" "$SBX/opt/server.env" || fail "server.env is not half the host's RAM ($want_mem)"
  if grep -q "，降为 " "$SBX/upgrade-1.log"; then fail "the window still had to lower memory_limit after the fix"; fi
  if "$CLI" doctor 2>&1 | grep -q "\[M1\]"; then fail "M1 still reported after the fix"; fi
fi
if [[ -n "$LEGACY_CLI" ]]; then
  grep -q "交给新版 CLI 继续升级" "$SBX/upgrade-1.log" || fail "deploy.sh did not hand over to the new CLI"
  grep -q "升级已经在上面完成" "$SBX/upgrade-1.log" || fail "no note about the legacy CLI's restart hint"
  cmp -s "$CLI" "$SBX/code/duckport-rs/duckport" || fail "the new CLI was not installed"
fi

step "checks"
want=$(grep -m1 '^version = ' "$SBX/code/duckport-rs/Cargo.toml" | cut -d'"' -f2)
[[ "$(git -C "$SBX/code/duckport-rs" log -1 --format=%s)" == "sbx: next commit" ]] || fail "repo was not pulled"
[[ "$(cat "$SBX/opt/cli_dir")" == "$SBX/bin" ]] || fail "cli_dir"
got=$("$PY" -c "
import json, sys
import pyarrow.flight as f
r = list(f.FlightClient('grpc://127.0.0.1:$PORT').do_action(f.Action('duckport.ping', b'')))
print(json.loads(r[0].body.to_pybytes())['server_version'])")
[[ "$got" == "$want" ]] || fail "server is v$got, expected v$want"
for inst in sbx5m sbxfund; do
  [[ "$(systemctl is-active "$PREFIX-ingestor@$inst")" == "active" ]] || fail "$inst not running"
done
for exe in binance-ingestor:sbx5m binance-funding:sbxfund; do
  v=$("$SBX/opt/ingestors/${exe#*:}/.venv/bin/python3" -c "from importlib import metadata; print(metadata.version('${exe%%:*}'))")
  [[ "$v" == "0.3.0" ]] || fail "${exe#*:} has ${exe%%:*} $v"
done
[[ "$(scalar "$PORT" "SELECT count(*) FROM duckdb_constraints() WHERE schema_name = 'data' AND constraint_type = 'PRIMARY KEY' AND table_name IN ('usdt_perp_5m', 'usdt_perp_funding', 'exginfo')")" == "0" ]] \
  || fail "business tables still keyed"
[[ "$(scalar "$PORT" "SELECT count(*) FROM data.usdt_perp_5m")" -ge "$rows_before" ]] || fail "rows lost"
ls "$SBX/data/backup/"*.db >/dev/null || fail "no backup taken"
[[ "$(scalar "$PORT" "SELECT count(*) FILTER (status = 'error') FROM data.upgrade_log")" == "0" ]] || fail "upgrade_log has errors"

step "ingestors keep writing after the upgrade (up to 8 minutes)"
for i in $(seq 1 96); do
  [[ "$(scalar "$PORT" "SELECT count(*) FROM data.watermark WHERE table_name = 'usdt_perp_5m' AND updated_at > TIMESTAMP '$t_upgrade'")" == "1" ]] && break
  sleep 5
done
[[ "$i" -lt 96 ]] || fail "usdt_perp_5m watermark did not move after the upgrade"
[[ "$(scalar "$PORT" "SELECT count(*) FROM data.table_specs")" -ge "3" ]] || fail "plugins did not register their specs"
"$CLI" doctor || fail "doctor after the upgrade"

step "second run: nothing to do"
"$CLI" upgrade --yes --proxy "$PROXY" 2>&1 | tee "$SBX/upgrade-2.log"
[[ ${PIPESTATUS[0]} -eq 0 ]] || fail "second upgrade failed"
grep -q "不需要重启" "$SBX/upgrade-2.log" || fail "second run restarted the server"
grep -q "sha256 与 release 一致），跳过下载" "$SBX/upgrade-2.log" || fail "second run downloaded the binary again"
grep -q "已与期望状态一致" "$SBX/upgrade-2.log" || fail "second run changed data"
"$CLI" upgrade --finalize

step "upgrade --compact: refuses a half-done swap, removes a stale .compact, rewrites the catalog"
db="$SBX/data/duckport.db"
touch "$db.preswap"
if "$CLI" upgrade --compact --yes > "$SBX/compact-refused.log" 2>&1; then fail "--compact ran with a .preswap present"; fi
grep -q "没有完成回退" "$SBX/compact-refused.log" || fail "no explanation for refusing with a .preswap"
rm -f "$db.preswap"
echo stale > "$db.compact"
"$CLI" upgrade --compact --yes 2>&1 | tee "$SBX/compact.log"
[[ ${PIPESTATUS[0]} -eq 0 ]] || fail "upgrade --compact failed"
grep -q "已删除 $db.compact" "$SBX/compact.log" || fail "stale .compact was not removed"
grep -q "没有待清理的旧表" "$SBX/compact.log" || fail "nothing was left to drop, compact-verify should say so"
grep -q "已删除换下的原文件" "$SBX/compact.log" || fail "--compact did not finish"
[[ ! -e "$db.compact" && ! -e "$db.preswap" ]] || fail "files left behind by --compact"
for inst in sbx5m sbxfund; do
  [[ "$(systemctl is-active "$PREFIX-ingestor@$inst")" == "active" ]] || fail "$inst not running after --compact"
done
[[ "$(scalar "$PORT" "SELECT count(*) FROM data.usdt_perp_5m")" -ge "$rows_before" ]] || fail "rows lost by --compact"

[[ "$(scalar "$PROD_PORT" "SELECT count(*) FROM data.watermark WHERE ingestor LIKE 'sbx%'")" == "0" ]] \
  || fail "a sandbox instance wrote to the real deployment"
echo -e "\n\033[0;32mUPGRADE SANDBOX TEST PASSED\033[0m"
if [[ "${KEEP:-0}" != "1" ]]; then cleanup; fi
