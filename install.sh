#!/usr/bin/env bash
# duckport-rs 一键安装脚本
#
# 公开仓库：
#   curl -fsSL https://raw.githubusercontent.com/sjmc-quant/duckport-rs/main/install.sh | bash
#
# 私有仓库（需先设置 GITHUB_TOKEN）：
#   export GITHUB_TOKEN=ghp_xxxx
#   curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" \
#     https://raw.githubusercontent.com/sjmc-quant/duckport-rs/main/install.sh | bash
#
# 传参示例（升级模式）：
#   curl -fsSL ... | bash -s -- --upgrade
#   curl -fsSL ... | bash -s -- --no-migrate

set -euo pipefail

REPO="sjmc-quant/duckport-rs"

# 安装目录。已有部署就沿用它记录的仓库位置；否则以 root 安装时放到 /opt/duckport/src，而不是
# /root 下：/root 对其他用户不可进入，而 duckport 命令要读仓库里的脚本，非 root 用户就用不了
# doctor / retention / status 的 retention 部分（grid102 就是这样装出来的）。
_recorded_repo() {
  local f
  for f in /opt/duckport/repo_dir "$HOME/.duckport/repo_dir"; do
    [[ -r "$f" && -d "$(cat "$f" 2>/dev/null)/.git" ]] && { cat "$f"; return 0; }
  done
  return 1
}
if [[ -n "${DUCKPORT_INSTALL_DIR:-}" ]]; then
  INSTALL_DIR="$DUCKPORT_INSTALL_DIR"
elif INSTALL_DIR=$(_recorded_repo); then
  :
elif [[ "$(id -u)" -eq 0 && "$(uname -s)" == "Linux" ]]; then
  INSTALL_DIR="/opt/duckport/src/duckport-rs"
else
  INSTALL_DIR="$HOME/code/duckport-rs"
fi

# 构造带认证的 clone URL（私有仓库需要 GITHUB_TOKEN）
if [[ -n "${GITHUB_TOKEN:-}" ]]; then
  CLONE_URL="https://x-token:${GITHUB_TOKEN}@github.com/${REPO}.git"
else
  CLONE_URL="https://github.com/${REPO}.git"
fi

echo "▶ duckport-rs installer"
echo "  仓库：https://github.com/${REPO}"
echo "  安装目录：${INSTALL_DIR}"

if [[ -d "${INSTALL_DIR}/.git" ]]; then
  echo "▶ 已有仓库，执行 git pull..."
  git -C "${INSTALL_DIR}" pull --ff-only
else
  echo "▶ 克隆仓库到 ${INSTALL_DIR}..."
  mkdir -p "$(dirname "${INSTALL_DIR}")"
  git clone "${CLONE_URL}" "${INSTALL_DIR}"
fi

echo "▶ 执行 deploy.sh $*"
exec bash "${INSTALL_DIR}/deploy.sh" "$@"
