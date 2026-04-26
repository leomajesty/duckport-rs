#!/usr/bin/env bash
# duckport-rs 一键安装脚本
#
# 公开仓库：
#   curl -fsSL https://raw.githubusercontent.com/leomajesty/duckport-rs/main/install.sh | bash
#
# 私有仓库（需先设置 GITHUB_TOKEN）：
#   export GITHUB_TOKEN=ghp_xxxx
#   curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" \
#     https://raw.githubusercontent.com/leomajesty/duckport-rs/main/install.sh | bash
#
# 传参示例（升级模式）：
#   curl -fsSL ... | bash -s -- --upgrade
#   curl -fsSL ... | bash -s -- --no-migrate

set -euo pipefail

REPO="leomajesty/duckport-rs"
INSTALL_DIR="${DUCKPORT_INSTALL_DIR:-/w/code/duckport-rs}"

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
  git clone "${CLONE_URL}" "${INSTALL_DIR}"
fi

echo "▶ 执行 deploy.sh $*"
exec bash "${INSTALL_DIR}/deploy.sh" "$@"
