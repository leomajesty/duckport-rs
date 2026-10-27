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
#   DEPLOY_OPT, DEPLOY_DATA, DUCKPORT_UNIT_PREFIX, DUCKPORT_CLI_DIR（同机第二套部署）
#   DUCKPORT_SERVER_BIN（离线安装：用这个 binary，不从 GitHub 下载）

set -euo pipefail

# ─── 平台检测 ─────────────────────────────────────────────────────────────────

OS=$(uname -s)    # Linux | Darwin
ARCH=$(uname -m)  # x86_64 | arm64

# ─── 默认配置 ─────────────────────────────────────────────────────────────────

GITHUB_REPO="${GITHUB_REPO:-sjmc-quant/duckport-rs}"
TAG_GIVEN="${RELEASE_TAG:+1}"   # 调用方指定了 tag（新版 CLI 总会指定），见文末「交给新版 CLI」
RELEASE_TAG="${RELEASE_TAG:-latest}"

# macOS 用用户路径（无需 sudo），Linux 用系统路径
if [[ "$OS" == "Darwin" ]]; then
  DEPLOY_OPT="${DEPLOY_OPT:-$HOME/.duckport}"
  DEPLOY_DATA="${DEPLOY_DATA:-$HOME/data/duckport}"
  PLIST_DIR="$HOME/Library/LaunchAgents"
else
  DEPLOY_OPT="${DEPLOY_OPT:-/opt/duckport}"
  DEPLOY_DATA="${DEPLOY_DATA:-/data/duckport}"
  PLIST_DIR=""  # Linux 用 systemd，无需此变量
fi

DEPLOY_BIN="$DEPLOY_OPT/bin"
# systemd unit 名前缀和 CLI 安装目录：同机部署第二套（演练、测试）时改掉，默认不变
# 首次部署时的选择记在 $DEPLOY_OPT 下，之后的升级沿用
UNIT_PREFIX="${DUCKPORT_UNIT_PREFIX:-$(cat "$DEPLOY_OPT/unit_prefix" 2>/dev/null || echo duckport)}"
CLI_DIR="${DUCKPORT_CLI_DIR:-$(cat "$DEPLOY_OPT/cli_dir" 2>/dev/null || true)}"
INGESTORS_DIR="$DEPLOY_OPT/ingestors"
LOG_DIR="$DEPLOY_OPT/logs"

DB_PATH="${DB_PATH:-${DEPLOY_DATA}/duckport.db}"
LISTEN_ADDR="${LISTEN_ADDR:-0.0.0.0:50051}"
READ_POOL_SIZE="${READ_POOL_SIZE:-4}"
# DuckDB 的内存上限：没指定就取本机现在能给出的内存的一半（不超过总内存的一半，按 256 MiB 取整，
# 最少 512 MiB）。以前固定 8GB，在小机器上 DuckDB 以为内存够用而不落盘，重建时被内核 OOM 杀掉；
# 总内存的一半也不可靠——机器上往往还有别的进程。上限高于本机内存的 server.env，doctor 会提示（M1），
# duckport upgrade 会询问并代改。
_default_memory_limit() {
  local total=0 avail=0 b
  if [[ -r /proc/meminfo ]]; then
    total=$(( $(awk '/^MemTotal:/{print $2}' /proc/meminfo) * 1024 ))
    avail=$(( $(awk '/^MemAvailable:/{print $2}' /proc/meminfo) * 1024 ))
  else
    total=$(sysctl -n hw.memsize 2>/dev/null || echo 0)
    avail=$(vm_stat 2>/dev/null | awk '/page size of/ { page = $8 } /^Pages (free|inactive|speculative):/ { gsub(/\./, "", $NF); pages += $NF } END { if (page) printf "%d\n", pages * page }')
    avail=${avail:-0}
  fi
  b=$(( avail / 2 ))
  [[ $total -gt 0 && $b -gt $(( total / 2 )) ]] && b=$(( total / 2 ))
  b=$(( b / 268435456 * 268435456 ))
  [[ $b -ge 536870912 ]] || b=536870912
  echo "$(( b / 1048576 ))MiB"
}
MEMORY_LIMIT_GIVEN="${MEMORY_LIMIT:+1}"
MEMORY_LIMIT="${MEMORY_LIMIT:-$(_default_memory_limit)}"

# ─── 参数解析 ─────────────────────────────────────────────────────────────────

MODE="install"
LOADHIST_INSTANCE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --loadhist)  MODE="loadhist"; LOADHIST_INSTANCE="${2:-}"; [[ -n "${2:-}" ]] && shift ;;
    --tag)       RELEASE_TAG="$2"; TAG_GIVEN=1; shift ;;
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

md5_of() {
  if [[ "$OS" == "Darwin" ]]; then
    md5 -q "$1" 2>/dev/null
  else
    md5sum "$1" 2>/dev/null | cut -d' ' -f1
  fi
}

sha256_of() {
  if command -v sha256sum &>/dev/null; then
    sha256sum "$1" | cut -d' ' -f1
  else
    shasum -a 256 "$1" | cut -d' ' -f1
  fi
}

# ─── 确定仓库根目录 ───────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -f "$SCRIPT_DIR/Cargo.toml" ]] || fail "请在 duckport-rs 仓库根目录下运行此脚本"
REPO_DIR="$SCRIPT_DIR"

# 同 duckport CLI 的 _retarget_origin：origin 还指着停更的 leomajesty/duckport-rs 就改到 sjmc-quant。
# 旧版 CLI 的 upgrade 在 pull 之后才执行本脚本，所以这里改了要到下一次 pull 才生效。
_retarget_origin() {
  local dir="$1" url
  url=$(git -C "$dir" remote get-url origin 2>/dev/null) || return 0
  case "$url" in
    *github.com[:/]leomajesty/duckport-rs|*github.com[:/]leomajesty/duckport-rs.git)
      git -C "$dir" remote set-url origin "${url/leomajesty/sjmc-quant}"
      info "origin 已从 leomajesty 改为 sjmc-quant：${url/leomajesty/sjmc-quant}" ;;
  esac
}
_retarget_origin "$REPO_DIR"

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
    if [[ "$OS" == "Linux" ]]; then
      systemctl stop "duckport-ingestor@$inst" 2>/dev/null || true
    else
      launchctl unload "$PLIST_DIR/com.duckport.ingestor.${inst}.plist" 2>/dev/null || true
    fi

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

# ─── 辅助：获取 Release binary 的 URL 和 sha256 ─────────────────────────────

# 输出 "<url> <sha256>"；sha256 是 GitHub 为每个 release 文件算好的 digest，拿不到时为空。
_release_asset() {
  local api_url asset_name
  if [[ "$OS" == "Darwin" ]]; then
    # uname -m 在 Apple Silicon 返回 arm64，release 产物命名用 aarch64
    local norm_arch
    norm_arch="$([[ "$ARCH" == "arm64" ]] && echo "aarch64" || echo "$ARCH")"
    asset_name="duckport-server-macos-${norm_arch}"
  else
    asset_name="duckport-server-linux-${ARCH}"
  fi
  if [[ "$RELEASE_TAG" == "latest" ]]; then
    api_url="https://api.github.com/repos/${GITHUB_REPO}/releases/latest"
  else
    api_url="https://api.github.com/repos/${GITHUB_REPO}/releases/tags/${RELEASE_TAG}"
  fi
  # 响应是逐行排版的 JSON；每个 asset 里 name 在前，digest 其次，browser_download_url 最后
  curl -fsSL \
    ${GITHUB_TOKEN:+-H "Authorization: Bearer ${GITHUB_TOKEN}"} \
    "$api_url" 2>/dev/null \
    | awk -v want="\"name\": \"${asset_name}\"" '
        index($0, want) { inside = 1; digest = "" }
        inside && /"digest": *"sha256:/ { match($0, /sha256:[0-9a-f]+/); digest = substr($0, RSTART + 7, RLENGTH - 7) }
        inside && /"browser_download_url"/ { split($0, f, "\""); print f[4], digest; exit }
      '
}

# ─── 辅助：本机编译回落 ───────────────────────────────────────────────────────

_build_from_source() {
  # 所有诊断输出走 stderr，只有最终路径走 stdout（供调用方 $(...) 捕获）
  info "回落：在本机编译（首次含 DuckDB bundled，约需 2-5 分钟）..." >&2
  export PATH="$HOME/.cargo/bin:$PATH"
  if ! command -v cargo &>/dev/null; then
    info "安装 Rust..." >&2
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable >&2
    export PATH="$HOME/.cargo/bin:$PATH"
  fi
  ok "Rust: $(rustc --version)" >&2
  cargo build --release --bin duckport-server --manifest-path "$REPO_DIR/Cargo.toml" >&2
  local bin="${CARGO_TARGET_DIR:-$REPO_DIR/target}/release/duckport-server"
  [[ -f "$bin" ]] || { fail "编译完成但找不到产物：$bin" >&2; exit 1; }
  echo "$bin"
}

# ─── Step 1：目录结构 ─────────────────────────────────────────────────────────

step "创建目录结构"
mkdir -p "$DEPLOY_BIN" "$DEPLOY_DATA/pqt" "$DEPLOY_DATA/hist" "$INGESTORS_DIR" "$DEPLOY_OPT/logs"
ok "目录就绪"

# ─── Step 2：获取 binary ─────────────────────────────────────────────────────
# 先用 release 的 sha256 和已安装的比：一样就不下载；下载的文件也要对得上这个 sha256。
# 拿不到 sha256（离线安装、API 不可用）时照旧：下载后 MD5 比对决定是否替换。

step "获取 duckport-server binary"

INSTALLED_BIN="$DEPLOY_BIN/duckport-server"
TMP_BIN="$(mktemp)"
trap 'rm -f "$TMP_BIN"' EXIT

BINARY_URL=""; BINARY_SHA256=""
if [[ -z "${DUCKPORT_SERVER_BIN:-}" ]]; then
  read -r BINARY_URL BINARY_SHA256 <<< "$(_release_asset || true)" || true
fi

if [[ -n "${DUCKPORT_SERVER_BIN:-}" ]]; then
  # 离线安装：用指定的 binary，不访问 GitHub
  info "使用本地 binary：$DUCKPORT_SERVER_BIN"
  cp "$DUCKPORT_SERVER_BIN" "$TMP_BIN"
elif [[ -n "$BINARY_SHA256" && -f "$INSTALLED_BIN" && "$(sha256_of "$INSTALLED_BIN")" == "$BINARY_SHA256" ]]; then
  skip "binary 未变化（sha256 与 release 一致），跳过下载"
  rm -f "$TMP_BIN"; trap - EXIT
  TMP_BIN=""
elif [[ -n "$BINARY_URL" ]]; then
  info "下载：$BINARY_URL"
  # --progress-bar：TTY 下显示进度条；-f 保留 HTTP 错误失败；不加 -s 以免静默掉进度
  curl -fL --progress-bar \
    ${GITHUB_TOKEN:+-H "Authorization: Bearer ${GITHUB_TOKEN}"} \
    "$BINARY_URL" -o "$TMP_BIN"
  echo
  if [[ -n "$BINARY_SHA256" ]]; then
    GOT_SHA256=$(sha256_of "$TMP_BIN")
    [[ "$GOT_SHA256" == "$BINARY_SHA256" ]] \
      || fail "下载的 binary 与 release 记录的 sha256 不符（${GOT_SHA256:0:12}… ≠ ${BINARY_SHA256:0:12}…），未替换"
    ok "sha256 校验通过"
  fi
else
  info "GitHub Release 暂无预构建包，尝试本机编译..."
  SRC_BIN=$(_build_from_source)
  cp "$SRC_BIN" "$TMP_BIN"
fi

if [[ -n "$TMP_BIN" ]]; then
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

if [[ ! -f "$DEPLOY_OPT/server.env" && -z "$MEMORY_LIMIT_GIVEN" ]]; then
  info "DUCKPORT_DUCKDB_MEMORY_LIMIT=${MEMORY_LIMIT}（本机现在能给出的内存的一半；MEMORY_LIMIT=… 可覆盖）"
fi
_write_if_missing "$DEPLOY_OPT/server.env" << EOF
DUCKPORT_DB_PATH=${DB_PATH}
DUCKPORT_LISTEN_ADDR=${LISTEN_ADDR}
DUCKPORT_CATALOG_NAME=duckport
DUCKPORT_READ_POOL_SIZE=${READ_POOL_SIZE}
DUCKPORT_DUCKDB_THREADS=0
DUCKPORT_DUCKDB_MEMORY_LIMIT=${MEMORY_LIMIT}
DUCKPORT_RETENTION_ENABLED=false
DUCKPORT_RETENTION_DRY_RUN=true
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

# ─── Step 5：注册服务（systemd on Linux，launchd on macOS）────────────────────

step "注册服务"

# 通用 ingestor 启动器（两个平台共用）
# 使用非引号 heredoc：$INGESTORS_DIR 在写入时展开，运行时变量用 \$ 转义
cat > "$DEPLOY_BIN/ingestor-run" << RUNEOF
#!/usr/bin/env bash
# 通用 ingestor 启动器（由 systemd/launchd 调用）
INSTANCE="\${1:?实例名不能为空}"
INST_DIR="${INGESTORS_DIR}/\$INSTANCE"
CFG="\$INST_DIR/config.env"

[[ -f "\$CFG" ]] || { echo "找不到配置：\$CFG"; exit 1; }

EXEC_CMD=\$(grep '^INGESTOR_EXEC=' "\$CFG" | cut -d= -f2 | tr -d '"' || echo "binance-ingestor")
EXEC_BIN="\$INST_DIR/.venv/bin/\$EXEC_CMD"

[[ -x "\$EXEC_BIN" ]] || { echo "找不到可执行文件：\$EXEC_BIN"; exit 1; }

export INGESTOR_ENV_FILE="\$CFG"
# 插件从这里读服务端地址（DUCKPORT_LISTEN_ADDR）；同机多套部署时各读各的
export DUCKPORT_SERVER_ENV="${DEPLOY_OPT}/server.env"
export INGESTOR_NAME="\$INSTANCE"
export INGESTOR_INSTANCE="\$INSTANCE"
exec "\$EXEC_BIN"
RUNEOF
chmod +x "$DEPLOY_BIN/ingestor-run"

if [[ "$OS" == "Linux" ]]; then
  # ── Linux：systemd unit ──────────────────────────────────────────────────────

  # ${UNIT_PREFIX}-server.service；非引号 heredoc：部署路径在写入时展开
  cat > "/etc/systemd/system/${UNIT_PREFIX}-server.service" << SVCEOF
[Unit]
Description=duckport-rs gRPC Database Service
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=${DEPLOY_OPT}
EnvironmentFile=${DEPLOY_OPT}/server.env
ExecStart=${DEPLOY_BIN}/duckport-server
# SIGTERM → drain inflight RPCs → CHECKPOINT → exit 0
KillSignal=SIGTERM
# 停服要把 WAL checkpoint 进库文件再关库，几百 GB 的库要几分钟；60 秒会在半路被 SIGKILL
# （pro.studio 273 GB 时发生过，下次启动只能回放 WAL）。systemd 到点就杀，CLI 等多久都没用。
TimeoutStopSec=900
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
SVCEOF

  # ${UNIT_PREFIX}-ingestor@.service（template，%i = 实例名）
  cat > "/etc/systemd/system/${UNIT_PREFIX}-ingestor@.service" << SVCEOF
[Unit]
Description=duckport Ingestor (%i)
After=${UNIT_PREFIX}-server.service
Requires=${UNIT_PREFIX}-server.service

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=${INGESTORS_DIR}/%i
EnvironmentFile=${INGESTORS_DIR}/%i/config.env
Environment=INGESTOR_ENV_FILE=${INGESTORS_DIR}/%i/config.env
ExecStart=${DEPLOY_BIN}/ingestor-run %i
# Ingestor handles SIGTERM (client.close) before exit
KillSignal=SIGTERM
TimeoutStopSec=30
Restart=on-failure
RestartSec=10
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
SVCEOF

  systemctl daemon-reload
  systemctl enable "${UNIT_PREFIX}-server" 2>/dev/null || true
  ok "${UNIT_PREFIX}-server.service 已注册并 enable"
  ok "${UNIT_PREFIX}-ingestor@.service (template) 已注册"

else
  # ── macOS：launchd plist（PLIST_DIR 已在默认配置块中按平台设置）────────────

  mkdir -p "$PLIST_DIR"

  # server-run wrapper：launchd 不支持 EnvironmentFile，由此脚本 source 后 exec
  # 非引号 heredoc：$DEPLOY_OPT/$DEPLOY_BIN 在写入时展开
  cat > "$DEPLOY_BIN/server-run" << RUNEOF
#!/usr/bin/env bash
set -a
source ${DEPLOY_OPT}/server.env
set +a
exec ${DEPLOY_BIN}/duckport-server
RUNEOF
  chmod +x "$DEPLOY_BIN/server-run"
  ok "server-run wrapper 已写入 $DEPLOY_BIN/server-run"

  # com.duckport.server.plist（RunAtLoad=false，由 duckport start 触发）
  cat > "$PLIST_DIR/com.duckport.server.plist" << PLISTEOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.duckport.server</string>
  <key>ProgramArguments</key>
  <array>
    <string>${DEPLOY_BIN}/server-run</string>
  </array>
  <key>RunAtLoad</key>
  <false/>
  <key>KeepAlive</key>
  <dict>
    <key>Crashed</key>
    <true/>
    <key>ThrottleInterval</key>
    <integer>5</integer>
  </dict>
  <key>StandardOutPath</key>
  <string>${LOG_DIR}/server.log</string>
  <key>StandardErrorPath</key>
  <string>${LOG_DIR}/server.err</string>
  <key>WorkingDirectory</key>
  <string>${DEPLOY_OPT}</string>
</dict>
</plist>
PLISTEOF
  ok "com.duckport.server.plist 已写入 $PLIST_DIR"
  ok "（ingestor plist 将在 duckport install 时按实例生成）"
fi

# ─── Step 6：注册 duckport CLI ────────────────────────────────────────────────

step "注册 duckport 命令行工具"
# 将仓库路径持久化，供 duckport CLI 读取（upgrade / ingestor add 需要）
echo "$REPO_DIR" > "$DEPLOY_OPT/repo_dir"
echo "$UNIT_PREFIX" > "$DEPLOY_OPT/unit_prefix"
if [[ "$OS" == "Darwin" ]]; then
  CLI_DIR="${CLI_DIR:-$HOME/.local/bin}"
  mkdir -p "$CLI_DIR"
  install -m 755 "$REPO_DIR/duckport" "$CLI_DIR/duckport"
  ok "已安装：$CLI_DIR/duckport"
  if [[ ":$PATH:" != *":$CLI_DIR:"* ]]; then
    info "提示：请确保 $CLI_DIR 在 PATH 中（可加入 ~/.zshrc）："
    info "  export PATH=\"\$HOME/.local/bin:\$PATH\""
  fi
else
  CLI_DIR="${CLI_DIR:-/usr/local/bin}"
  mkdir -p "$CLI_DIR"
  install -m 755 "$REPO_DIR/duckport" "$CLI_DIR/duckport"
  ok "已安装：$CLI_DIR/duckport"
fi
echo "$CLI_DIR" > "$DEPLOY_OPT/cli_dir"
ok "仓库路径已写入：$DEPLOY_OPT/repo_dir → $REPO_DIR"

# Linux 上非 root 用户也要能读到仓库里的脚本：duckport status / doctor / retention 都靠它们。仓库在
# root 的家目录下时做不到（以 root 运行旧版 install.sh 的默认位置就是 /root/code）。macOS 的部署
# 本来就是每个用户自己的（~/.duckport，不用 sudo），不检查。
_world_readable() {  # 文件对 other 可读，且路径上每级目录对 other 可进入
  local p="$1" d m
  m=$(stat -c %a "$p" 2>/dev/null || stat -f %Lp "$p" 2>/dev/null) || return 1
  [[ $(( 0$m & 4 )) -ne 0 ]] || return 1
  d=$(dirname "$p")
  while [[ "$d" != "/" && "$d" != "." ]]; do
    m=$(stat -c %a "$d" 2>/dev/null || stat -f %Lp "$d" 2>/dev/null) || return 1
    [[ $(( 0$m & 1 )) -ne 0 ]] || return 1
    d=$(dirname "$d")
  done
}
if [[ "$OS" == "Linux" ]] && ! _world_readable "$REPO_DIR/scripts/doctor.py"; then
  info "⚠ 仓库 ${REPO_DIR} 对非 root 用户不可读：duckport status / doctor / retention 只能用 sudo 执行"
  info "  建议放到所有用户可读的位置，例如 ${DEPLOY_OPT}/src/duckport-rs，再把新路径写入 ${DEPLOY_OPT}/repo_dir"
fi

# ─── 旧版 CLI 的 upgrade：交给新版 CLI 接着做 ─────────────────────────────────
# v1.0.x / v1.1.x 的 `duckport upgrade` 只做 git pull 和本脚本，参数也不传，然后提示
# `duckport restart` 就结束了：插件和数据都没升级。它在 pull 之后执行的正是这份新脚本，
# 所以由这里接手：调用方是 `duckport upgrade`、不是新版 CLI 的 `upgrade --after-pull`，
# 也没有指定 tag（新版 CLI 总会指定）时，exec 新版 CLI 的 `upgrade --after-pull`，参数从
# 父进程的命令行取回。DUCKPORT_HANDOFF 保证不会再接手一次。

# 父进程是旧版 `duckport upgrade` 时输出它的参数（每行一个），否则返回 1
_legacy_upgrade_args() {
  local args=() arg i j n
  if [[ -r "/proc/$PPID/cmdline" ]]; then
    while IFS= read -r -d '' arg; do args+=("$arg"); done < "/proc/$PPID/cmdline"
  else
    read -ra args <<< "$(ps -o args= -p "$PPID" 2>/dev/null)"
  fi
  n=${#args[@]}
  for ((i = 0; i + 1 < n; i++)); do
    [[ "${args[i]##*/}" == "duckport" && "${args[i+1]}" == "upgrade" ]] || continue
    for ((j = i + 2; j < n; j++)); do
      [[ "${args[j]}" == "--after-pull" ]] && return 1
      printf '%s\n' "${args[j]}"
    done
    return 0
  done
  return 1
}

if [[ "$MODE" == "install" && -z "$TAG_GIVEN" && -z "${DUCKPORT_HANDOFF:-}" ]] \
   && LEGACY_ARGS=$(_legacy_upgrade_args); then
  step "交给新版 CLI 继续升级"
  info "旧版 duckport upgrade 只会 git pull 和执行本脚本，接下来由新版 CLI 升级服务端、插件和数据"
  FWD=()
  while IFS= read -r arg; do [[ -n "$arg" ]] && FWD+=("$arg"); done <<< "$LEGACY_ARGS"
  exec env DUCKPORT_HOME="$DEPLOY_OPT" DUCKPORT_HANDOFF=1 \
    bash "$REPO_DIR/duckport" upgrade --after-pull ${FWD[@]+"${FWD[@]}"}
fi

# ─── 完成摘要 ─────────────────────────────────────────────────────────────────

if [[ "$OS" == "Linux" ]]; then
  SV_ACTIVE=$(systemctl is-active "${UNIT_PREFIX}-server" 2>/dev/null || echo "inactive")
else
  SV_ACTIVE=$(launchctl list 2>/dev/null | awk '$3=="com.duckport.server" {print ($1=="-")?"inactive":"active"}' || echo "inactive")
  SV_ACTIVE="${SV_ACTIVE:-inactive}"
fi

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
