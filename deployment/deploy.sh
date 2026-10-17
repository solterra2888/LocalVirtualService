#!/bin/bash
# ===========================================
# GuanHe Project 一键部署脚本
# 读取 deploy.conf + .env.secrets + config.defaults.env
# 生成 .env / systemd services / nginx.conf
# ===========================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

APP_NAME="$(grep -s '^APP_NAME=' "$PROJECT_DIR/.env" 2>/dev/null | cut -d= -f2 || true)"
APP_NAME="${APP_NAME:-GuanHe Project}"

echo "🚀 $APP_NAME 部署脚本"
echo "================================"

# ── 检查 root 权限 ──────────────────────────────────────────
if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以 root 权限运行: sudo $0"
    exit 1
fi

# ── 检查必需文件 ─────────────────────────────────────────────
DEPLOY_CONF="$SCRIPT_DIR/deploy.conf"
SECRETS_FILE="$SCRIPT_DIR/.env.secrets"
DEFAULTS_FILE="$SCRIPT_DIR/config.defaults.env"
TEMPLATES_DIR="$SCRIPT_DIR/templates"
GENERATED_DIR="$SCRIPT_DIR/generated"

for f in "$DEPLOY_CONF" "$SECRETS_FILE" "$DEFAULTS_FILE"; do
    if [ ! -f "$f" ]; then
        echo "❌ 缺少配置文件: $f"
        echo "   请从 .example 文件复制并填写"
        exit 1
    fi
done

# ── 加载 deploy.conf 变量 ────────────────────────────────────
echo "📂 加载部署配置..."

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

# ── 处理服务器地址（占位或为空时尝试自动检测公网 IP）────────────────
SERVER_HOST_PLACEHOLDER="你的服务器公网IP"
if [ -z "${SERVER_HOST:-}" ] || [ "$SERVER_HOST" = "$SERVER_HOST_PLACEHOLDER" ]; then
    DETECTED_IP=""
    for url in "https://ifconfig.me/ip" "https://icanhazip.com" "http://ifconfig.me/ip"; do
        DETECTED_IP=$(curl -s --connect-timeout 3 --max-time 5 "$url" 2>/dev/null | tr -d '\r\n' | grep -E '^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$') && break
    done
    if [ -n "$DETECTED_IP" ]; then
        SERVER_HOST="$DETECTED_IP"
        export SERVER_HOST
        echo "   🌐 SERVER_HOST 未配置或为占位符，已自动检测公网 IP: $SERVER_HOST"
        # 写回 deploy.conf，下次无需再检测
        if [ -f "$DEPLOY_CONF" ] && grep -q "^SERVER_HOST=$SERVER_HOST_PLACEHOLDER" "$DEPLOY_CONF" 2>/dev/null; then
            sed -i "s|^SERVER_HOST=.*|SERVER_HOST=$SERVER_HOST|" "$DEPLOY_CONF"
            echo "   ✅ 已更新 deploy.conf 中的 SERVER_HOST"
        fi
    else
        echo "   ⚠️  SERVER_HOST 未配置且无法自动检测公网 IP（请检查网络或代理）"
        echo "      请在 deployment/deploy.conf 中设置 SERVER_HOST=你的服务器公网IP或域名"
        SERVER_HOST="127.0.0.1"
        export SERVER_HOST
        echo "      当前临时使用 SERVER_HOST=127.0.0.1，部署后请尽快修改 deploy.conf 并重新运行 deploy.sh"
    fi
fi

echo "   ✅ APP_NAME=$APP_NAME"
echo "   ✅ PROJECT_DIR=$PROJECT_DIR"
echo "   ✅ SERVER_HOST=$SERVER_HOST"
echo "   ✅ DB_HOST=$DB_HOST / DB_NAME=$DB_NAME"

# ── 生成派生变量 ─────────────────────────────────────────────
echo ""
echo "🔧 生成派生变量..."

# 读取 Redis 密码
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

# 构造 REDIS_URL
if [ -n "$REDIS_PASSWORD" ]; then
    ENCODED_REDIS_PW=$(printf '%s' "$REDIS_PASSWORD" | python3 -c "import sys, urllib.parse; print(urllib.parse.quote(sys.stdin.read(), safe=''))")
    REDIS_URL="redis://:${ENCODED_REDIS_PW}@${REDIS_HOST}:${REDIS_PORT}/0"
else
    REDIS_URL="redis://${REDIS_HOST}:${REDIS_PORT}/0"
fi

SCHEDULER_SERVICE_URL="http://localhost:${SCHEDULER_PORT}"
WEBAPP_URL="http://localhost:${WEBAPP_PORT}"
FRONTEND_URL="http://localhost:${FRONTEND_PORT}"
PUBLIC_WEB_URL="${PUBLIC_WEB_URL:-http://${SERVER_HOST}}"
LOG_DIR="${LOG_DIR:-$PROJECT_DIR/logs}"
LOG_FILE_DAYS="$(grep '^LOG_FILE_DAYS=' "$DEFAULTS_FILE" 2>/dev/null | cut -d= -f2)"
LOG_FILE_DAYS="${LOG_FILE_DAYS:-30}"
export LOG_FILE_DAYS
API_KEY="$( (grep '^EODHD_API_TOKEN=' "$SECRETS_FILE" 2>/dev/null || echo '=') | cut -d= -f2)"

echo "   ✅ REDIS_URL=redis://...@${REDIS_HOST}:${REDIS_PORT}/0"
echo "   ✅ SCHEDULER_SERVICE_URL=$SCHEDULER_SERVICE_URL"

# ── 1. 生成 .env ─────────────────────────────────────────────
echo ""
echo "📝 生成 .env 文件..."

ENV_FILE="$PROJECT_DIR/.env"

cat > "$ENV_FILE" << ENVEOF
# ============================================================
# 此文件由 deploy.sh 自动生成，请勿手动编辑
# 修改请编辑: deployment/deploy.conf
#              deployment/.env.secrets
#              deployment/config.defaults.env
# 然后重新运行: sudo bash deployment/deploy.sh
# ============================================================

# ===== 项目标识（含空格的值加引号，避免 source 时被 shell 拆成命令）=====
APP_NAME="$APP_NAME"
APP_VERSION="$APP_VERSION"

# ===== 数据库 =====
DB_HOST=$DB_HOST
DB_PORT=$DB_PORT
DB_NAME=$DB_NAME
DB_USER=$DB_USER
ENVEOF

# 追加 secrets
while IFS= read -r line || [[ -n "$line" ]]; do
    line="$(echo "$line" | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')"
    [[ -z "$line" || "$line" == \#* ]] && continue
    echo "$line" >> "$ENV_FILE"
done < "$SECRETS_FILE"

# 追加 EODHD_API_TOKEN 的别名
echo "" >> "$ENV_FILE"
echo "# EODHD 别名（向后兼容）" >> "$ENV_FILE"
echo "API_KEY=$API_KEY" >> "$ENV_FILE"

# 追加派生变量
cat >> "$ENV_FILE" << ENVEOF

# ===== 派生变量（自动生成）=====
REDIS_URL=$REDIS_URL
SCHEDULER_SERVICE_URL=$SCHEDULER_SERVICE_URL
WEBAPP_URL=$WEBAPP_URL
FRONTEND_URL=$FRONTEND_URL
SERVER_HOST=$SERVER_HOST
PUBLIC_WEB_URL=$PUBLIC_WEB_URL
LOG_DIR=$LOG_DIR
RC_AGENT_BASE_URL=$RC_AGENT_BASE_URL
RC_AGENT_TOKEN_default=$(grep '^RC_AGENT_TOKEN=' "$SECRETS_FILE" | sed 's/^RC_AGENT_TOKEN=//')
ENVEOF

# 追加代理配置（如果有）
{
    echo ""
    echo "# ===== 代理 ====="
    grep -E '^(HTTP_PROXY|HTTPS_PROXY|YOUTUBE_PROXY)=' "$DEPLOY_CONF" 2>/dev/null || true
} >> "$ENV_FILE"

# 追加 YouTube 认证配置（如果有）
{
    echo ""
    echo "# ===== YouTube 认证 ====="
    grep -E '^(YOUTUBE_COOKIES_FILE|YOUTUBE_PO_TOKEN|YOUTUBE_VISITOR_DATA)=' "$DEPLOY_CONF" 2>/dev/null || true
} >> "$ENV_FILE"

# 追加前端环境变量
cat >> "$ENV_FILE" << ENVEOF

# ===== 前端 =====
REACT_APP_API_BASE_URL=/api
REACT_APP_SCHEDULER_BASE_URL=/scheduler
REACT_APP_APP_NAME="$APP_NAME"
REACT_APP_APP_VERSION="$APP_VERSION"
REACT_APP_RC_AGENT_BASE_URL=$RC_AGENT_BASE_URL
ENVEOF

# 追加默认配置
echo "" >> "$ENV_FILE"
echo "# ===== 应用默认参数 =====" >> "$ENV_FILE"
while IFS= read -r line || [[ -n "$line" ]]; do
    line="$(echo "$line" | sed 's/^[[:space:]]*//' | sed 's/[[:space:]]*$//')"
    [[ -z "$line" || "$line" == \#* ]] && continue
    key="${line%%=*}"
    # 跳过已经在上面写过的变量
    grep -q "^${key}=" "$ENV_FILE" 2>/dev/null && continue
    echo "$line" >> "$ENV_FILE"
done < "$DEFAULTS_FILE"

echo "   ✅ 已生成 $ENV_FILE"

# ── 2. 渲染模板生成 service 文件和 nginx.conf ────────────────
echo ""
echo "📝 渲染服务模板..."

mkdir -p "$GENERATED_DIR"

render_template() {
    local input="$1"
    local output="$2"
    local content
    content="$(cat "$input")"

    content="${content//\{\{APP_NAME\}\}/$APP_NAME}"
    content="${content//\{\{APP_VERSION\}\}/$APP_VERSION}"
    content="${content//\{\{PROJECT_DIR\}\}/$PROJECT_DIR}"
    content="${content//\{\{CONDA_ENV\}\}/$CONDA_ENV}"
    content="${content//\{\{VOLTA_DIR\}\}/$VOLTA_DIR}"
    content="${content//\{\{LOG_DIR\}\}/$LOG_DIR}"
    content="${content//\{\{SERVER_HOST\}\}/$SERVER_HOST}"
    content="${content//\{\{WEBAPP_PORT\}\}/$WEBAPP_PORT}"
    content="${content//\{\{SCHEDULER_PORT\}\}/$SCHEDULER_PORT}"
    content="${content//\{\{FRONTEND_PORT\}\}/$FRONTEND_PORT}"
    content="${content//\{\{CELERY_AI_CONCURRENCY\}\}/$CELERY_AI_CONCURRENCY}"
    content="${content//\{\{CELERY_DATA_CONCURRENCY\}\}/$CELERY_DATA_CONCURRENCY}"
    content="${content//\{\{UVICORN_WORKERS\}\}/$UVICORN_WORKERS}"
    content="${content//\{\{RC_AGENT_BASE_URL\}\}/$RC_AGENT_BASE_URL}"
    content="${content//\{\{LOG_FILE_DAYS\}\}/$LOG_FILE_DAYS}"

    printf '%s\n' "$content" > "$output"
}

for tpl in "$TEMPLATES_DIR"/*.tpl; do
    [ -f "$tpl" ] || continue
    basename="$(basename "$tpl" .tpl)"
    render_template "$tpl" "$GENERATED_DIR/$basename"
    echo "   ✅ $basename"
done

# ── 3. 安装到系统 ────────────────────────────────────────────
echo ""
echo "📦 安装到系统..."

# systemd services (rc-frontend 已由 Docker Nginx 静态文件替代，不再安装)
for svc in rc-webapp rc-celery-ai rc-celery-data rc-celery-zsxq-files rc-scheduler; do
    src="$GENERATED_DIR/${svc}.service"
    if [ -f "$src" ]; then
        cp "$src" "/etc/systemd/system/${svc}.service"
        echo "   ✅ ${svc}.service → /etc/systemd/system/"
    fi
done

# logrotate（多进程安全轮转 app.log / celery_*.log）
LOGROTATE_SRC="$GENERATED_DIR/guanhe-logrotate"
if [ -f "$LOGROTATE_SRC" ]; then
    cp "$LOGROTATE_SRC" "/etc/logrotate.d/guanhe"
    echo "   ✅ guanhe-logrotate → /etc/logrotate.d/guanhe"
fi

# Docker Nginx: 同步 web.conf 到 Docker 卷
WEBCONF_SRC="$PROJECT_DIR/web.conf"
WEBCONF_DST="/opt/nginx/conf/vhosts/web.conf"
if [ -f "$WEBCONF_SRC" ]; then
    mkdir -p "$(dirname "$WEBCONF_DST")"
    cp "$WEBCONF_SRC" "$WEBCONF_DST"
    echo "   ✅ web.conf → $WEBCONF_DST (Docker Nginx 配置)"
else
    echo "   ⚠️  未找到 $WEBCONF_SRC，跳过 Nginx 配置同步"
fi

# 前端构建产物通过 Docker volume mount 直接挂载到 Nginx 容器
# （-v /opt/guanhetech/web-frontend/build:/usr/share/nginx/html/guanhe:ro）
# npm run deploy 后自动生效，无需复制或符号链接
FRONTEND_BUILD="$PROJECT_DIR/web-frontend/build"
if [ -d "$FRONTEND_BUILD" ]; then
    echo "   ✅ 前端构建目录存在: $FRONTEND_BUILD (通过 Docker mount 提供给 Nginx)"
else
    echo "   ⚠️  前端构建目录不存在: $FRONTEND_BUILD，请先执行 cd web-frontend && npm run deploy"
fi

# manifest.json（前端 PWA 配置）
MANIFEST_GEN="$GENERATED_DIR/manifest.json"
if [ -f "$MANIFEST_GEN" ]; then
    cp "$MANIFEST_GEN" "$PROJECT_DIR/web-frontend/public/manifest.json"
    echo "   ✅ manifest.json → web-frontend/public/"
fi

# daemon reload
systemctl daemon-reload
echo "   ✅ systemd daemon-reload 完成"

# ── 4. 清理残留 PID 文件 ─────────────────────────────────────
echo ""
echo "🧹 清理残留 PID 文件..."
for pidfile in "$LOG_DIR"/celery_*.pid; do
    if [ -f "$pidfile" ]; then
        pid=$(cat "$pidfile" 2>/dev/null)
        if [ -n "$pid" ] && ! kill -0 "$pid" 2>/dev/null; then
            rm -f "$pidfile"
            echo "   ✅ 已清理过期 PID 文件: $(basename "$pidfile")"
        fi
    fi
done

# ── 5. 创建日志目录 ──────────────────────────────────────────
mkdir -p "$LOG_DIR"

# ── 完成 ─────────────────────────────────────────────────────
echo ""
echo "🎉 部署配置生成完成!"
echo "================================"
echo ""
echo "📂 生成的文件:"
echo "   - $ENV_FILE"
for f in "$GENERATED_DIR"/*; do
    echo "   - $f"
done
echo ""
echo "🔧 下一步:"
echo "   sudo ./start_services.sh      # 启动所有服务"
echo "   sudo ./restart_services.sh    # 重启所有服务"
echo ""
echo "💡 以后修改配置只需编辑 deployment/ 下的文件，然后重新运行:"
echo "   sudo bash deployment/deploy.sh && sudo ./restart_services.sh"
