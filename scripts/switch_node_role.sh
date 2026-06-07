#!/bin/bash
# ============================================================
# 切换 WORKER_NODE_ROLE 并重新应用 systemd target
#
# 用法:
#   sudo bash scripts/switch_node_role.sh caption
#   sudo bash scripts/switch_node_role.sh asr
#   sudo bash scripts/switch_node_role.sh all
# ============================================================

set -euo pipefail

ROLE="${1:-}"
if [[ -z "$ROLE" ]]; then
    echo "用法: $0 <caption|asr|all>" >&2
    exit 1
fi

case "$ROLE" in
    caption|asr|all) ;;
    *)
        echo "❌ 无效角色: $ROLE（可选: caption / asr / all）" >&2
        exit 1
        ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(dirname "$SCRIPT_DIR")"
ENV_FILE="$BASE_DIR/.env"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "❌ 找不到 $ENV_FILE" >&2
    exit 1
fi

if grep -q '^WORKER_NODE_ROLE=' "$ENV_FILE"; then
    sed -i "s/^WORKER_NODE_ROLE=.*/WORKER_NODE_ROLE=${ROLE}/" "$ENV_FILE"
else
    echo "WORKER_NODE_ROLE=${ROLE}" >> "$ENV_FILE"
fi

echo "已设置 WORKER_NODE_ROLE=${ROLE}，重新运行 setup.sh 应用 systemd..."
exec bash "$BASE_DIR/setup.sh"
