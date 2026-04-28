#!/usr/bin/env bash
# duckport-rs 环境配置脚本
#
# 职责：配置环境（目录、binary、Python 包、systemd unit）
#       不启动、不停止任何服务
#
# 用法：
#   ./deploy.sh                        # 安装 / 升级（自动 MD5 检测）
#   ./deploy.sh --instance <名称>      # 指定默认实例名（默认 binance-5m）
#   ./deploy.sh --loadhist [<实例>]    # 补历史数据
#
# 环境变量（可覆盖默认值）：
#   GITHUB_REPO, RELEASE_TAG
#   DB_PATH, LISTEN_ADDR, READ_POOL_SIZE, MEMORY_LIMIT

set -euo pipefail

# ─── 默认配置 ─────────────────────────────────────────────────────────────────

GITHUB_REPO="${GITHUB_REPO:-leomajesty/duckport-rs}"
RELEASE_TAG="${RELEASE_TAG:-latest}"

DEPLOY_BIN="/opt/duckport/bin"
DEPLOY_OPT="/opt/duckport"
DEPLOY_DATA="/data/duckport"
INGESTORS_DIR="/opt/duckport/ingestors"

DB_PATH="${DB_PATH:-${DEPLOY_DATA}/duckport.db}"
LISTEN_ADDR="${LISTEN_ADDR:-0.0.0.0:50051}"
READ_POOL_SIZE="${READ_POOL_SIZE:-4}"
MEMORY_LIMIT="${MEMORY_LIMIT:-8GB}"

# ─── 参数解析 ─────────────────────────────────────────────────────────────────

MODE="install"
LOADHIST_INSTANCE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --loadhist)  MODE="loadhist"; LOADHIST_INSTANCE="${2:-}"; [[ -n "${2:-}" ]] && shift ;;
    --tag)       RELEASE_TAG="$2"; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
  shift
done

# ─── 工具函数 ─────────────────────────────────────────────────────────────────

BOLD='\033[1m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; RED='\033[0;31m'; NC='\033[0m'
step()  { echo -e "\n${BOLD}${GREEN}▶ $*${NC}"; }
info()  { echo -e "  ${YELLOW}$*${NC}"; }
ok()    { echo -e "  ${GREEN}✓ $*${NC}"; }
skip()  { echo -e "  ${CYAN}– $*${NC}"; }
fail()  { echo -e "  ${RED}✗ $*${NC}"; exit 1; }

md5_of() { md5sum "$1" 2>/dev/null | cut -d' ' -f1; }

# ─── 确定仓库根目录 ───────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -f "$SCRIPT_DIR/Cargo.toml" ]] || fail "请在 duckport-rs 仓库根目录下运行此脚本"
REPO_DIR="$SCRIPT_DIR"

# ─── loadhist 模式 ────────────────────────────────────────────────────────────

if [[ "$MODE" == "loadhist" ]]; then
  step "loadhist${LOADHIST_INSTANCE:+ — $LOADHIST_INSTANCE}"

  _run_loadhist() {
    local inst="$1"
    local inst_dir="$INGESTORS_DIR/$inst"
    local cfg="$inst_dir/config.env"
    local py="$inst_dir/.venv/bin/python3"
    [[ -x "$py" && -f "$cfg" ]] || { info "实例 $inst 环境不完整，跳过"; return; }

    info "⚠ 停止 duckport-ingestor@$inst 以独占 staging 表"
    systemctl stop "duckport-ingestor@$inst" 2>/dev/null || true

    INGESTOR_ENV_FILE="$cfg" "$inst_dir/.venv/bin/loadhist" && \
      ok "loadhist 完成" || { info "loadhist 失败"; }

    info "提示：请手动执行 duckport start $inst 以恢复采集"
  }

  if [[ -n "$LOADHIST_INSTANCE" ]]; then
    _run_loadhist "$LOADHIST_INSTANCE"
  else
    # 对所有实例逐一执行
    find "$INGESTORS_DIR" -maxdepth 2 -name "config.env" 2>/dev/null \
      | sed "s|$INGESTORS_DIR/||;s|/config.env||" | sort \
      | while IFS= read -r inst; do _run_loadhist "$inst"; done
  fi
  exit 0
fi

# ═══════════════════════════════════════════════════════════════════════════════
# install 模式
# ═══════════════════════════════════════════════════════════════════════════════

info "GitHub 仓库：$GITHUB_REPO  tag：$RELEASE_TAG"

# ─── 辅助：获取 Release binary URL ───────────────────────────────────────────

_get_binary_url() {
  local api_url
  if [[ "$RELEASE_TAG" == "latest" ]]; then
    api_url="https://api.github.com/repos/${GITHUB_REPO}/releases/latest"
  else
    api_url="https://api.github.com/repos/${GITHUB_REPO}/releases/tags/${RELEASE_TAG}"
  fi
  curl -fsSL \
    ${GITHUB_TOKEN:+-H "Authorization: Bearer ${GITHUB_TOKEN}"} \
    "$api_url" 2>/dev/null \
    | grep '"browser_download_url"' \
    | grep 'duckport-server-linux-x86_64' \
    | cut -d'"' -f4
}

# ─── 辅助：本机编译回落 ───────────────────────────────────────────────────────

_build_from_source() {
  info "回落：在本机编译（首次含 DuckDB bundled，约需 2-5 分钟）..."
  export PATH="$HOME/.cargo/bin:$PATH"
  if ! command -v cargo &>/dev/null; then
    info "安装 Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
    export PATH="$HOME/.cargo/bin:$PATH"
  fi
  ok "Rust: $(rustc --version)"
  cargo build --release --bin duckport-server --manifest-path "$REPO_DIR/Cargo.toml" 2>&1 | tail -3
  echo "$REPO_DIR/target/release/duckport-server"
}

# ─── Step 1：目录结构 ─────────────────────────────────────────────────────────

step "创建目录结构"
mkdir -p "$DEPLOY_BIN" "$DEPLOY_DATA/pqt" "$DEPLOY_DATA/hist" "$INGESTORS_DIR"
ok "目录就绪"

# ─── Step 2：获取 binary（MD5 比对决定是否替换）──────────────────────────────

step "获取 duckport-server binary"

INSTALLED_BIN="$DEPLOY_BIN/duckport-server"
TMP_BIN="$(mktemp)"
trap 'rm -f "$TMP_BIN"' EXIT

BINARY_URL=$(_get_binary_url || true)

if [[ -n "$BINARY_URL" ]]; then
  info "下载：$BINARY_URL"
  curl -fsSL \
    ${GITHUB_TOKEN:+-H "Authorization: Bearer ${GITHUB_TOKEN}"} \
    "$BINARY_URL" -o "$TMP_BIN"
else
  info "GitHub Release 暂无预构建包，尝试本机编译..."
  SRC_BIN=$(_build_from_source)
  cp "$SRC_BIN" "$TMP_BIN"
fi

chmod +x "$TMP_BIN"

if [[ -f "$INSTALLED_BIN" ]]; then
  OLD_MD5=$(md5_of "$INSTALLED_BIN")
  NEW_MD5=$(md5_of "$TMP_BIN")
  if [[ "$OLD_MD5" == "$NEW_MD5" ]]; then
    skip "binary 未变化（MD5 相同），跳过替换"
    rm -f "$TMP_BIN"; trap - EXIT
  else
    mv "$TMP_BIN" "$INSTALLED_BIN"; trap - EXIT
    ok "binary 已升级（${OLD_MD5:0:8}… → ${NEW_MD5:0:8}…）"
  fi
else
  mv "$TMP_BIN" "$INSTALLED_BIN"; trap - EXIT
  ok "binary 已安装：$INSTALLED_BIN ($(ls -lh "$INSTALLED_BIN" | awk '{print $5}'))"
fi

# ─── Step 3：写配置文件（已存在则跳过）──────────────────────────────────────

step "配置文件"

_write_if_missing() {
  local path="$1"; shift
  if [[ -f "$path" ]]; then
    skip "$(basename "$path") 已存在，跳过"
  else
    cat > "$path"; ok "已写入 $path"
  fi
}

_write_if_missing "$DEPLOY_OPT/server.env" << EOF
DUCKPORT_DB_PATH=${DB_PATH}
DUCKPORT_LISTEN_ADDR=${LISTEN_ADDR}
DUCKPORT_CATALOG_NAME=duckport
DUCKPORT_READ_POOL_SIZE=${READ_POOL_SIZE}
DUCKPORT_DUCKDB_THREADS=0
DUCKPORT_DUCKDB_MEMORY_LIMIT=${MEMORY_LIMIT}
DUCKPORT_RETENTION_ENABLED=false
RUST_LOG=info
EOF

# ─── Step 4：确保 uv 可用（duckport install 依赖它）─────────────────────────

step "检查 uv（Python 环境管理器）"
export PATH="$HOME/.local/bin:$PATH"
if command -v uv &>/dev/null; then
  skip "uv 已安装：$(uv --version)"
else
  info "安装 uv..."
  curl -fsSL https://astral.sh/uv/install.sh | sh
  export PATH="$HOME/.local/bin:$PATH"
  ok "uv 已安装：$(uv --version)"
fi

# ─── Step 5：注册 systemd unit（不启动）──────────────────────────────────────

step "注册 systemd unit 文件"

# duckport-server.service（固定）
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

# duckport-ingestor@.service（template，%i = 实例名）
cat > /etc/systemd/system/duckport-ingestor@.service << 'SVCEOF'
[Unit]
Description=duckport Ingestor (%i)
After=duckport-server.service
Requires=duckport-server.service

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=/opt/duckport/ingestors/%i
EnvironmentFile=/opt/duckport/ingestors/%i/config.env
Environment=INGESTOR_ENV_FILE=/opt/duckport/ingestors/%i/config.env
ExecStart=/opt/duckport/bin/ingestor-run %i
Restart=on-failure
RestartSec=10
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
SVCEOF

# 通用启动器：读取实例 config.env 中的 INGESTOR_EXEC
cat > "$DEPLOY_BIN/ingestor-run" << 'RUNEOF'
#!/usr/bin/env bash
# 通用 ingestor 启动器（由 duckport-ingestor@.service 调用）
INSTANCE="${1:?实例名不能为空}"
INST_DIR="/opt/duckport/ingestors/$INSTANCE"
CFG="$INST_DIR/config.env"

[[ -f "$CFG" ]] || { echo "找不到配置：$CFG"; exit 1; }

EXEC_CMD=$(grep '^INGESTOR_EXEC=' "$CFG" | cut -d= -f2 | tr -d '"' || echo "binance-ingestor")
EXEC_BIN="$INST_DIR/.venv/bin/$EXEC_CMD"

[[ -x "$EXEC_BIN" ]] || { echo "找不到可执行文件：$EXEC_BIN"; exit 1; }

export INGESTOR_ENV_FILE="$CFG"
exec "$EXEC_BIN"
RUNEOF
chmod +x "$DEPLOY_BIN/ingestor-run"

systemctl daemon-reload
systemctl enable duckport-server 2>/dev/null || true
ok "duckport-server.service 已注册并 enable"
ok "duckport-ingestor@.service (template) 已注册"

# ─── Step 6：注册 duckport CLI ────────────────────────────────────────────────

step "注册 duckport 命令行工具"
# 将仓库路径持久化，供 duckport CLI 读取（upgrade / ingestor add 需要）
echo "$REPO_DIR" > "$DEPLOY_OPT/repo_dir"
install -m 755 "$REPO_DIR/duckport" /usr/local/bin/duckport
ok "已安装：/usr/local/bin/duckport"
ok "仓库路径已写入：$DEPLOY_OPT/repo_dir → $REPO_DIR"

# ─── 完成摘要 ─────────────────────────────────────────────────────────────────

SV_ACTIVE=$(systemctl is-active duckport-server 2>/dev/null || echo "inactive")

echo ""
echo -e "${BOLD}${GREEN}══════════════════════════════════════════${NC}"
echo -e "${BOLD}${GREEN}  环境配置完成${NC}"
echo -e "${BOLD}${GREEN}══════════════════════════════════════════${NC}"
printf "  %-20s %s\n" "binary："      "$INSTALLED_BIN"
printf "  %-20s %s\n" "server.env：" "$DEPLOY_OPT/server.env"
printf "  %-20s %s\n" "数据库："      "$DB_PATH"
echo ""
echo -e "${BOLD}后续步骤：${NC}"
echo "  duckport list                    # 查看可用插件"
echo "  duckport install binance-ingestor # 安装 Binance 数据插件"
echo ""
if [[ "$SV_ACTIVE" == "inactive" ]]; then
  echo "  duckport start server            # 启动 duckport-server"
else
  echo "  duckport status                  # 查看服务状态和数据水位"
fi
echo "  duckport logs server             # 查看服务日志"
