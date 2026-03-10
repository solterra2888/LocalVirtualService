#!/bin/bash
# ===========================================
# 本地开发：根据 deployment 配置生成项目根目录 .env（无需 root）
# 与 deploy.sh 使用同一套 deploy.conf / .env.secrets / config.defaults.env
# ===========================================
# 使用步骤：
#   1. cp deployment/deploy.conf.example deployment/deploy.conf
#   2. cp deployment/.env.secrets.example deployment/.env.secrets
#   3. 按本地环境修改 deploy.conf（如 DB_HOST=127.0.0.1, SERVER_HOST=127.0.0.1）
#   4. 按需修改 .env.secrets 中的密钥
#   5. 运行本脚本: bash development/generate_env_from_deploy.sh
#   6. 运行: ./development/start_local_dev.sh
# ===========================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
DEPLOY_DIR="$PROJECT_DIR/deployment"

DEPLOY_CONF="$DEPLOY_DIR/deploy.conf"
SECRETS_FILE="$DEPLOY_DIR/.env.secrets"
DEFAULTS_FILE="$DEPLOY_DIR/config.defaults.env"
ENV_FILE="$PROJECT_DIR/.env"

for f in "$DEPLOY_CONF" "$SECRETS_FILE" "$DEFAULTS_FILE"; do
    if [ ! -f "$f" ]; then
        echo "❌ 缺少配置文件: $f"
        echo "   请从 deployment 下的 .example 复制并填写，例如："
        echo "   cp deployment/deploy.conf.example deployment/deploy.conf"
        echo "   cp deployment/.env.secrets.example deployment/.env.secrets"
        exit 1
    fi
done

echo "📂 加载 deployment 配置（仅生成 .env，无需 root）..."

load_conf() {
    local file="$1"
    while IFS= read -r line || [[ -n "$line" ]]; do
        line="$(echo "$line" | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')"
        [[ -z "$line" || "$line" == \#* ]] && continue
        local key="${line%%=*}"
        local value="${line#*=}"
        export "$key=$value"
    done < "$file"
}

load_conf "$DEPLOY_CONF"

# 本地开发：占位符或未配置时使用 127.0.0.1，不写回 deploy.conf
SERVER_HOST_PLACEHOLDER="你的服务器公网IP"
if [ -z "${SERVER_HOST:-}" ] || [ "$SERVER_HOST" = "$SERVER_HOST_PLACEHOLDER" ]; then
    export SERVER_HOST="127.0.0.1"
    echo "   ℹ️  SERVER_HOST 使用 127.0.0.1（本地开发）"
fi

echo "   ✅ APP_NAME=$APP_NAME"
echo "   ✅ DB_HOST=$DB_HOST / DB_NAME=$DB_NAME"

# 读取 Redis 密码并构造 REDIS_URL
REDIS_PASSWORD=""
while IFS= read -r line || [[ -n "$line" ]]; do
    line="$(echo "$line" | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')"
    [[ -z "$line" || "$line" == \#* ]] && continue
    key="${line%%=*}"
    value="${line#*=}"
    if [ "$key" = "REDIS_PASSWORD" ]; then
        REDIS_PASSWORD="$value"
    fi
done < "$SECRETS_FILE"

if [ -n "$REDIS_PASSWORD" ]; then
    ENCODED_REDIS_PW=$(REDIS_PASSWORD="$REDIS_PASSWORD" python3 -c "import urllib.parse, os; print(urllib.parse.quote(os.environ['REDIS_PASSWORD'], safe=''))")
    REDIS_URL="redis://:${ENCODED_REDIS_PW}@${REDIS_HOST}:${REDIS_PORT}/0"
else
    REDIS_URL="redis://${REDIS_HOST}:${REDIS_PORT}/0"
fi

SCHEDULER_SERVICE_URL="http://localhost:${SCHEDULER_PORT}"
WEBAPP_URL="http://localhost:${WEBAPP_PORT}"
FRONTEND_URL="http://localhost:${FRONTEND_PORT}"
LOG_DIR="${LOG_DIR:-$PROJECT_DIR/logs}"
API_KEY="$( (grep '^EODHD_API_TOKEN=' "$SECRETS_FILE" 2>/dev/null || echo '=') | cut -d= -f2)"

echo "📝 生成 .env -> $ENV_FILE"

cat > "$ENV_FILE" << ENVEOF
# ============================================================
# 此文件由 development/generate_env_from_deploy.sh 生成（本地开发）
# 修改请编辑: deployment/deploy.conf
#              deployment/.env.secrets
#              deployment/config.defaults.env
# 然后重新运行: bash development/generate_env_from_deploy.sh
# ============================================================

# ===== 项目标识 =====
APP_NAME=$APP_NAME
APP_VERSION=$APP_VERSION

# ===== 数据库 =====
DB_HOST=$DB_HOST
DB_PORT=$DB_PORT
DB_NAME=$DB_NAME
DB_USER=$DB_USER
ENVEOF

while IFS= read -r line || [[ -n "$line" ]]; do
    line="$(echo "$line" | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')"
    [[ -z "$line" || "$line" == \#* ]] && continue
    echo "$line" >> "$ENV_FILE"
done < "$SECRETS_FILE"

echo "" >> "$ENV_FILE"
echo "# EODHD 别名（向后兼容）" >> "$ENV_FILE"
echo "API_KEY=$API_KEY" >> "$ENV_FILE"

cat >> "$ENV_FILE" << ENVEOF

# ===== 派生变量（自动生成）=====
REDIS_URL=$REDIS_URL
SCHEDULER_SERVICE_URL=$SCHEDULER_SERVICE_URL
WEBAPP_URL=$WEBAPP_URL
FRONTEND_URL=$FRONTEND_URL
SERVER_HOST=$SERVER_HOST
LOG_DIR=$LOG_DIR
RC_AGENT_BASE_URL=$RC_AGENT_BASE_URL
RC_AGENT_TOKEN_default=$(grep '^RC_AGENT_TOKEN=' "$SECRETS_FILE" | cut -d= -f2)
ENVEOF

{
    echo ""
    echo "# ===== 代理 ====="
    grep -E '^(HTTP_PROXY|HTTPS_PROXY|YOUTUBE_PROXY)=' "$DEPLOY_CONF" 2>/dev/null || true
} >> "$ENV_FILE"

{
    echo ""
    echo "# ===== YouTube 认证 ====="
    grep -E '^(YOUTUBE_COOKIES_FILE|YOUTUBE_PO_TOKEN|YOUTUBE_VISITOR_DATA)=' "$DEPLOY_CONF" 2>/dev/null || true
} >> "$ENV_FILE"

cat >> "$ENV_FILE" << ENVEOF

# ===== 前端（本地开发使用绝对地址，无 Nginx 代理）=====
REACT_APP_API_BASE_URL=/api
REACT_APP_SCHEDULER_BASE_URL=http://localhost:${SCHEDULER_PORT}/api
REACT_APP_APP_NAME=$APP_NAME
REACT_APP_APP_VERSION=$APP_VERSION
REACT_APP_RC_AGENT_BASE_URL=$RC_AGENT_BASE_URL
ENVEOF

echo "" >> "$ENV_FILE"
echo "# ===== 应用默认参数 =====" >> "$ENV_FILE"
while IFS= read -r line || [[ -n "$line" ]]; do
    line="$(echo "$line" | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')"
    [[ -z "$line" || "$line" == \#* ]] && continue
    key="${line%%=*}"
    grep -q "^${key}=" "$ENV_FILE" 2>/dev/null && continue
    echo "$line" >> "$ENV_FILE"
done < "$DEFAULTS_FILE"

echo "✅ .env 已生成，可运行: ./development/start_local_dev.sh"
echo "   请确保 development/dev.conf 已配置（conda 路径等）： cp development/dev.conf.example development/dev.conf"
