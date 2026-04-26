#!/usr/bin/env bash
# duckport-rs 一键部署脚本（在目标服务器上直接运行）
#
# 用法：
#   ./deploy.sh                   # 首次完整部署（含数据迁移）
#   ./deploy.sh --upgrade         # 仅升级二进制 + 重启服务
#   ./deploy.sh --no-migrate      # 首次部署，跳过旧数据迁移
#
# 前置条件：
#   - 已通过 git clone 获取本仓库
#   - 私有仓库需先执行 gh auth login 或设置 GITHUB_TOKEN
#
# 示例（私有仓库）：
#   export GITHUB_TOKEN=ghp_xxxx
#   git clone https://x-token:${GITHUB_TOKEN}@github.com/OWNER/duckport-rs /w/code/duckport-rs
#   cd /w/code/duckport-rs && ./deploy.sh

set -euo pipefail

# ─── 可配置项 ─────────────────────────────────────────────────────────────────

# GitHub 仓库（自动从 git remote 读取，也可手动指定）
GITHUB_REPO="${GITHUB_REPO:-}"

# release tag（默认下载最新 release）
RELEASE_TAG="${RELEASE_TAG:-latest}"

# 部署路径
DEPLOY_BIN="/opt/duckport/bin"
DEPLOY_OPT="/opt/duckport"
DEPLOY_DATA="/data/duckport"
DEPLOY_INGESTOR="/opt/duckport/ingestor"

# duckport-server 运行配置
KLINE_INTERVAL="${KLINE_INTERVAL:-5m}"
DATA_SOURCES="${DATA_SOURCES:-usdt_perp,usdt_spot}"
DB_PATH="${DB_PATH:-${DEPLOY_DATA}/duckport.db}"
LISTEN_ADDR="${LISTEN_ADDR:-0.0.0.0:50051}"
READ_POOL_SIZE="${READ_POOL_SIZE:-4}"
MEMORY_LIMIT="${MEMORY_LIMIT:-8GB}"
START_DATE="${START_DATE:-2021-01-01}"
PROXY_URL="${PROXY_URL:-}"
ENABLE_WS="${ENABLE_WS:-false}"

# 旧版数据路径（策略 B 迁移用；不存在时自动跳过）
OLD_DB_PATH="${OLD_DB_PATH:-/w/data/5m/duckdb.db}"
OLD_PQT_DIR="${OLD_PQT_DIR:-/w/data/5m/pqt}"

# ─── 参数解析 ─────────────────────────────────────────────────────────────────

MODE="full"
RUN_MIGRATE=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --upgrade)    MODE="upgrade";  RUN_MIGRATE=false ;;
    --no-migrate) MODE="full";     RUN_MIGRATE=false ;;
    --tag)        RELEASE_TAG="$2"; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
  shift
done

# ─── 工具函数 ─────────────────────────────────────────────────────────────────

BOLD='\033[1m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
step() { echo -e "\n${BOLD}${GREEN}▶ $*${NC}"; }
info() { echo -e "  ${YELLOW}$*${NC}"; }
ok()   { echo -e "  ${GREEN}✓ $*${NC}"; }
fail() { echo -e "  ${RED}✗ $*${NC}"; exit 1; }

# ─── 确定仓库根目录 ───────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ ! -f "$SCRIPT_DIR/Cargo.toml" ]]; then
  fail "请在 duckport-rs 仓库根目录下运行此脚本"
fi
REPO_DIR="$SCRIPT_DIR"

# ─── 获取 GitHub 仓库名 ───────────────────────────────────────────────────────

if [[ -z "$GITHUB_REPO" ]]; then
  REMOTE_URL=$(git -C "$REPO_DIR" remote get-url origin 2>/dev/null || echo "")
  if [[ "$REMOTE_URL" =~ github\.com[:/](.+/.+?)(\.git)?$ ]]; then
    GITHUB_REPO="${BASH_REMATCH[1]}"
  else
    fail "无法从 git remote 推断 GitHub 仓库，请设置 GITHUB_REPO=owner/repo"
  fi
fi
info "GitHub 仓库：$GITHUB_REPO"

# ─── 构造 binary 下载 URL ─────────────────────────────────────────────────────

GH_API="https://api.github.com/repos/${GITHUB_REPO}"
AUTH_HEADER=""
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  AUTH_HEADER="-H \"Authorization: Bearer ${GITHUB_TOKEN}\""
fi

get_binary_url() {
  local api_url
  if [[ "$RELEASE_TAG" == "latest" ]]; then
    api_url="${GH_API}/releases/latest"
  else
    api_url="${GH_API}/releases/tags/${RELEASE_TAG}"
  fi
  curl -fsSL ${GITHUB_TOKEN:+-H "Authorization: Bearer ${GITHUB_TOKEN}"} "$api_url" \
    | grep '"browser_download_url"' \
    | grep 'duckport-server-linux-x86_64' \
    | cut -d'"' -f4
}

# ─── Step 0：git pull（保持代码最新）─────────────────────────────────────────

step "更新代码 (git pull)"
git -C "$REPO_DIR" pull --ff-only 2>/dev/null || info "git pull 失败（可能是 detached HEAD），继续..."
ok "代码已是最新"

# ─── Step 1：下载预构建二进制 ─────────────────────────────────────────────────

step "从 GitHub Releases 下载 duckport-server 二进制"
info "仓库：$GITHUB_REPO，tag：$RELEASE_TAG"

mkdir -p "$DEPLOY_BIN"

BINARY_URL=$(get_binary_url)
[[ -z "$BINARY_URL" ]] && fail "未找到 duckport-server-linux-x86_64 资产，请确认 release 已上传"

info "下载：$BINARY_URL"
curl -fL ${GITHUB_TOKEN:+-H "Authorization: Bearer ${GITHUB_TOKEN}"} \
  "$BINARY_URL" -o "$DEPLOY_BIN/duckport-server"
chmod +x "$DEPLOY_BIN/duckport-server"

BIN_SIZE=$(ls -lh "$DEPLOY_BIN/duckport-server" | awk '{print $5}')
ok "已下载：$DEPLOY_BIN/duckport-server ($BIN_SIZE)"

# ─── 升级模式：仅替换二进制并重启 ────────────────────────────────────────────

if [[ "$MODE" == "upgrade" ]]; then
  step "升级模式：停止服务 → 替换二进制 → 重启"
  systemctl stop binance-ingestor 2>/dev/null || true
  systemctl stop duckport-server  2>/dev/null || true
  sleep 2
  systemctl start duckport-server
  sleep 4
  systemctl start binance-ingestor
  sleep 2
  systemctl is-active duckport-server  && ok "duckport-server: active"  || fail "duckport-server 启动失败"
  systemctl is-active binance-ingestor && ok "binance-ingestor: active" || fail "binance-ingestor 启动失败"
  echo -e "\n${BOLD}${GREEN}升级完成！${NC}"
  exit 0
fi

# ─── Step 2：创建目录结构 ─────────────────────────────────────────────────────

step "创建目录结构"
mkdir -p "$DEPLOY_BIN" "$DEPLOY_DATA/pqt" "$DEPLOY_DATA/hist" "$DEPLOY_INGESTOR"
ok "目录就绪"

# ─── Step 3：写配置文件 ───────────────────────────────────────────────────────

step "写入配置文件"

cat > "$DEPLOY_OPT/server.env" << EOF
DUCKPORT_DB_PATH=${DB_PATH}
DUCKPORT_LISTEN_ADDR=${LISTEN_ADDR}
DUCKPORT_CATALOG_NAME=duckport
DUCKPORT_READ_POOL_SIZE=${READ_POOL_SIZE}
DUCKPORT_DUCKDB_THREADS=0
DUCKPORT_DUCKDB_MEMORY_LIMIT=${MEMORY_LIMIT}
DUCKPORT_RETENTION_ENABLED=false
RUST_LOG=info
EOF

cat > "$DEPLOY_INGESTOR/config.env" << EOF
DUCKPORT_ADDR=localhost:50051
DUCKPORT_SCHEMA=data
KLINE_INTERVAL=${KLINE_INTERVAL}
PARQUET_DIR=${OLD_PQT_DIR}
DATA_SOURCES=${DATA_SOURCES}
CONCURRENCY=2
RETENTION_ENABLED=false
RETENTION_DAYS=7
START_DATE=${START_DATE}
PROXY_URL=${PROXY_URL}
ENABLE_WS=${ENABLE_WS}
RESOURCE_PATH=${DEPLOY_DATA}/hist
EOF

ok "server.env 和 ingestor/config.env 已写入"

# ─── Step 4：安装 binance-ingestor ───────────────────────────────────────────

step "安装 binance-ingestor"

# 安装 uv（如未安装）
if ! command -v uv &>/dev/null; then
  info "安装 uv..."
  pip3 install uv --break-system-packages -q || pip3 install uv -q
fi

PYTHON_BIN=$(command -v python3.11 || command -v python3.12 || command -v python3)
PYTHON_VER=$("$PYTHON_BIN" --version 2>&1 | grep -oP '\d+\.\d+')
info "使用 Python $PYTHON_VER ($PYTHON_BIN)"

cd "$DEPLOY_INGESTOR"
uv venv --python "$PYTHON_VER" .venv -q 2>/dev/null || true
uv pip install -e "$REPO_DIR/ingestor/" -q 2>&1 | grep -v "^$" || true
INSTALLED=$(ls .venv/bin/ | grep -E 'binance-ingestor|loadhist' | tr '\n' ' ')
ok "已安装：$INSTALLED"
cd "$REPO_DIR"

# ─── Step 5：停止旧服务 ───────────────────────────────────────────────────────

step "停止现有服务（如有）"
systemctl stop binance-ingestor 2>/dev/null || true
systemctl stop duckport-server  2>/dev/null || true
# 确保没有残留进程占用 DB 文件
pkill -f duckport-server 2>/dev/null || true
sleep 1
ok "已停止"

# ─── Step 6：初始化 schema ────────────────────────────────────────────────────

step "启动 duckport-server 初始化 schema"

DUCKPORT_DB_PATH="$DB_PATH" \
DUCKPORT_LISTEN_ADDR=0.0.0.0:50051 \
DUCKPORT_CATALOG_NAME=duckport \
DUCKPORT_READ_POOL_SIZE=4 \
RUST_LOG=warn \
  nohup "$DEPLOY_BIN/duckport-server" > /tmp/duckport-init.log 2>&1 &
SERVER_PID=$!

# 等待端口就绪
RETRY=0
until nc -z localhost 50051 2>/dev/null; do
  sleep 1; RETRY=$((RETRY+1))
  [[ $RETRY -ge 15 ]] && { cat /tmp/duckport-init.log; fail "duckport-server 启动超时"; }
done

"$DEPLOY_INGESTOR/.venv/bin/python3" - << PYEOF
import sys
sys.path.insert(0, "$REPO_DIR/ingestor")
from binance_ingestor.duckport_client import DuckportClient
markets = [m.strip() for m in "$DATA_SOURCES".split(",")]
dc = DuckportClient(addr="127.0.0.1:50051", schema="data")
dc.init_schema(markets=markets, interval="$KLINE_INTERVAL", data_sources=set(markets))
dc.verify_kline_interval("$KLINE_INTERVAL")
dc.close()
print("schema initialized")
PYEOF

ok "Schema 初始化完成"
kill $SERVER_PID 2>/dev/null || true
sleep 2

# ─── Step 7：数据迁移 ─────────────────────────────────────────────────────────

if [[ "$RUN_MIGRATE" == "true" ]]; then
  if [[ -f "$OLD_DB_PATH" ]]; then
    step "数据迁移：$OLD_DB_PATH → $DB_PATH"
    info "迁移热库 + Parquet，数据量较大，请耐心等待..."

    "$DEPLOY_INGESTOR/.venv/bin/python3" - << PYEOF
import glob, os, duckdb

old_db = "$OLD_DB_PATH"
new_db = "$DB_PATH"
iv     = "$KLINE_INTERVAL"
mkts   = [m.strip() for m in "$DATA_SOURCES".split(",")]

con = duckdb.connect(new_db)
con.execute(f"ATTACH '{old_db}' AS old_db (READ_ONLY)")
print(f"Attached old DB: {old_db}")

for mkt in mkts:
    tbl = f"{mkt}_{iv}"
    try:
        print(f"  Migrating {tbl} (hot) ...")
        con.execute(f"""
            INSERT INTO data.{tbl}
            SELECT open_time, symbol, open, high, low, close,
                   volume, quote_volume, trade_num,
                   taker_buy_base_asset_volume, taker_buy_quote_asset_volume,
                   avg_price
            FROM old_db.{tbl} ON CONFLICT DO NOTHING
        """)
        n = con.execute(f"SELECT count(*) FROM data.{tbl}").fetchone()[0]
        print(f"  {tbl}: {n:,} rows")
    except Exception as e:
        print(f"  {tbl}: skip ({e})")

try:
    con.execute("""
        INSERT INTO data.config_dict (key, value)
        SELECT key, value FROM old_db.config_dict
        WHERE NOT ends_with(key, '_pqt_time')
        ON CONFLICT(key) DO UPDATE SET value = EXCLUDED.value
    """)
    rows = con.execute("SELECT key, value FROM data.config_dict").fetchall()
    for k, v in rows:
        print(f"  config_dict: {k} = {v}")
except Exception as e:
    print(f"  config_dict: {e}")

try:
    con.execute("""
        INSERT INTO data.exginfo
        SELECT market, symbol, status, base_asset, quote_asset,
               price_tick, lot_size, min_notional_value,
               contract_type, margin_asset, pre_market, created_time
        FROM old_db.exginfo ON CONFLICT DO NOTHING
    """)
    n = con.execute("SELECT count(*) FROM data.exginfo").fetchone()[0]
    print(f"  exginfo: {n:,} rows")
except Exception as e:
    print(f"  exginfo: {e}")

con.execute("DETACH old_db")

old_pqt = "$OLD_PQT_DIR"
for mkt in mkts:
    tbl     = f"{mkt}_{iv}"
    pattern = os.path.join(old_pqt, f"{mkt}_{iv}", f"{mkt}_*.parquet")
    files   = glob.glob(pattern)
    if not files:
        print(f"  parquet: no files for {pattern}, skip")
        continue
    print(f"  Importing {len(files)} parquet files for {tbl} ...")
    try:
        con.execute(f"""
            INSERT INTO data.{tbl}
            SELECT open_time, symbol, open, high, low, close,
                   volume, quote_volume, trade_num,
                   taker_buy_base_asset_volume, taker_buy_quote_asset_volume,
                   avg_price
            FROM read_parquet('{pattern}') ON CONFLICT DO NOTHING
        """)
        n = con.execute(f"SELECT count(*) FROM data.{tbl}").fetchone()[0]
        print(f"  merged {tbl}: {n:,} rows total")
    except Exception as e:
        print(f"  parquet {tbl}: {e}")

con.execute("CHECKPOINT")
con.close()
print("Migration done!")
PYEOF
    ok "数据迁移完成"
  else
    info "未找到旧库 $OLD_DB_PATH，跳过数据迁移"
  fi
fi

# ─── Step 8：注册 systemd 服务 ────────────────────────────────────────────────

step "配置 systemd 服务"

cat > /etc/systemd/system/duckport-server.service << 'SVCEOF'
[Unit]
Description=duckport-rs gRPC Database Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=/opt/duckport
EnvironmentFile=/opt/duckport/server.env
ExecStart=/opt/duckport/bin/duckport-server
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
SVCEOF

cat > /etc/systemd/system/binance-ingestor.service << SVCEOF
[Unit]
Description=Binance Data Ingestor for duckport-rs
After=duckport-server.service
Requires=duckport-server.service

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=${DEPLOY_INGESTOR}
Environment=INGESTOR_ENV_FILE=${DEPLOY_INGESTOR}/config.env
ExecStart=${DEPLOY_INGESTOR}/.venv/bin/binance-ingestor
Restart=on-failure
RestartSec=10
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
SVCEOF

systemctl daemon-reload
systemctl enable duckport-server binance-ingestor
ok "systemd 服务已注册"

# ─── Step 9：启动并验证 ───────────────────────────────────────────────────────

step "启动服务"
systemctl start duckport-server
sleep 4
systemctl start binance-ingestor
sleep 3

systemctl is-active duckport-server  && ok "duckport-server: active"  || fail "duckport-server 启动失败"
systemctl is-active binance-ingestor && ok "binance-ingestor: active" || fail "binance-ingestor 启动失败"

step "健康验证"
"$DEPLOY_INGESTOR/.venv/bin/python3" - << PYEOF
import sys, json
sys.path.insert(0, "$REPO_DIR/ingestor")
import pyarrow.flight as flight
from binance_ingestor.duckport_client import DuckportClient

c = flight.connect("grpc://localhost:50051")
res = list(c.do_action(flight.Action("duckport.ping", b"{}")))
info = json.loads(res[0].body.to_pybytes())
print(f"  ping: server={info['server']}, duckdb={info['duckdb_version']}")

dc = DuckportClient(addr="localhost:50051", schema="data")
for mkt in [m.strip() for m in "$DATA_SOURCES".split(",")]:
    tbl = f"{mkt}_$KLINE_INTERVAL"
    try:
        t  = dc.read_table("data", tbl)
        dt = dc.read_duck_time(mkt)
        print(f"  {tbl}: {t.num_rows:,} rows, duck_time={dt}")
    except Exception as e:
        print(f"  {tbl}: {e}")
dc.close()
PYEOF

# ─── 完成 ─────────────────────────────────────────────────────────────────────

echo ""
echo -e "${BOLD}${GREEN}========================================${NC}"
echo -e "${BOLD}${GREEN}  部署完成！${NC}"
echo -e "${BOLD}${GREEN}========================================${NC}"
printf "  %-16s %s\n" "监听地址："   "$LISTEN_ADDR"
printf "  %-16s %s\n" "数据库："     "$DB_PATH"
printf "  %-16s %s\n" "K 线周期："   "$KLINE_INTERVAL"
printf "  %-16s %s\n" "数据源："     "$DATA_SOURCES"
echo ""
echo "  查看日志："
echo "    journalctl -u duckport-server  -f"
echo "    journalctl -u binance-ingestor -f"
