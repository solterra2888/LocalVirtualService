#!/bin/bash
# ============================================================
# YouTube Remote Worker 一键部署脚本（家用 Ubuntu 虚拟机）
# 默认：通过公网直连服务器 Redis + PostgreSQL（与服务器 .env 中账号一致）
# ============================================================
set -e

SERVER_HOST="${SERVER_HOST:-43.99.37.76}"
PROJECT_DIR="$HOME/guanhetech"
VENV_DIR="$PROJECT_DIR/venv"

echo "=========================================="
echo "  YouTube Remote Worker 部署"
echo "  服务器: $SERVER_HOST（公网直连 Redis/PG）"
echo "=========================================="

# 1. 基础依赖（不再需要 autossh）
if ! command -v git &> /dev/null; then
    echo "[1/5] 安装 git、python3-venv..."
    sudo apt update && sudo apt install -y git python3 python3-venv python3-pip curl
else
    echo "[1/5] 基础工具已就绪 ✓"
fi

# 2. 克隆/更新项目
if [ -d "$PROJECT_DIR/.git" ]; then
    echo "[2/5] 更新项目代码..."
    cd "$PROJECT_DIR" && git pull || true
else
    echo "[2/5] 克隆项目..."
    if ssh -o BatchMode=yes -o ConnectTimeout=5 "root@$SERVER_HOST" "echo ok" &>/dev/null; then
        REPO_URL=$(ssh "root@$SERVER_HOST" "cd /opt/guanhetech && git remote get-url origin 2>/dev/null || echo ''")
        if [ -n "$REPO_URL" ]; then
            git clone "$REPO_URL" "$PROJECT_DIR"
        else
            echo "  无法从服务器读取 git remote，请手动: git clone <仓库> $PROJECT_DIR"
            exit 1
        fi
    else
        echo "  无法免密 SSH 到服务器，请手动克隆项目到 $PROJECT_DIR 后重新运行本脚本"
        exit 1
    fi
fi
cd "$PROJECT_DIR"

# 3. Python 环境
echo "[3/5] 配置 Python 环境..."
if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
fi
# shellcheck source=/dev/null
source "$VENV_DIR/bin/activate"
pip install -q --upgrade pip
pip install -q -r requirements.txt 2>/dev/null || pip install -q celery redis psycopg2-binary feedparser requests youtube-transcript-api

mkdir -p logs

# 4. 生成 .env（直连公网：DB_HOST / REDIS_URL 中的主机换为 SERVER_HOST）
if [ ! -f "$PROJECT_DIR/.env" ]; then
    echo "[4/5] 从服务器拉取密码并生成 .env（直连 $SERVER_HOST）..."

    if ! ssh -o BatchMode=yes -o ConnectTimeout=10 "root@$SERVER_HOST" "test -f /opt/guanhetech/.env"; then
        echo "  无法读取服务器 /opt/guanhetech/.env，请手动复制为 $PROJECT_DIR/.env（参考 deployment/remote-youtube-worker/.env.template）"
        exit 1
    fi

    DB_PASSWORD=$(ssh "root@$SERVER_HOST" "grep '^DB_PASSWORD=' /opt/guanhetech/.env | cut -d= -f2-")
    REDIS_URL_RAW=$(ssh "root@$SERVER_HOST" "grep '^REDIS_URL=' /opt/guanhetech/.env | cut -d= -f2-")
    # 127.0.0.1 -> 公网 IP（端口保持 6379）
    REDIS_URL_PUBLIC=$(echo "$REDIS_URL_RAW" | sed "s/127\\.0\\.0\\.1/${SERVER_HOST}/g")

    cat > "$PROJECT_DIR/.env" <<EOF
# YouTube Remote Worker（公网直连，由 setup.sh 生成）
DB_HOST=${SERVER_HOST}
DB_PORT=5432
DB_NAME=guanhe_database
DB_USER=postgres
DB_PASSWORD=${DB_PASSWORD}
REDIS_URL=${REDIS_URL_PUBLIC}
LOG_DIR=./logs
LOG_LEVEL=INFO
LOG_TO_FILE=true
LOG_TO_CONSOLE=true
EOF
    chmod 600 "$PROJECT_DIR/.env"
    echo "  .env 已生成 ✓"
else
    echo "[4/5] .env 已存在，跳过（若改 IP 请手动编辑）✓"
fi

# 5. 仅安装 youtube-worker systemd（无 SSH 隧道）
echo "[5/5] 安装 systemd 用户服务..."
SYSTEMD_DIR="$HOME/.config/systemd/user"
mkdir -p "$SYSTEMD_DIR"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/youtube-worker.service" ]; then
    cp "$SCRIPT_DIR/youtube-worker.service" "$SYSTEMD_DIR/"
elif [ -f "$PROJECT_DIR/deployment/remote-youtube-worker/youtube-worker.service" ]; then
    cp "$PROJECT_DIR/deployment/remote-youtube-worker/youtube-worker.service" "$SYSTEMD_DIR/"
fi

if [ -n "$CONDA_PREFIX" ] && [ "$CONDA_PREFIX/bin/celery" != "$VENV_DIR/bin/celery" ]; then
    sed -i "s|%h/guanhetech/venv/bin/celery|$CONDA_PREFIX/bin/celery|g" "$SYSTEMD_DIR/youtube-worker.service" 2>/dev/null || true
fi

systemctl --user daemon-reload
systemctl --user enable youtube-worker.service
loginctl enable-linger "$USER" 2>/dev/null || true

echo ""
echo "=========================================="
echo "  部署完成"
echo "=========================================="
echo ""
echo "启动 Worker:"
echo "  systemctl --user start youtube-worker.service"
echo ""
echo "或前台调试:"
echo "  cd $PROJECT_DIR && source venv/bin/activate && export PYTHONPATH=$PROJECT_DIR"
echo "  celery -A backend.celery_app worker --queues=youtube_fetching --concurrency=1 --loglevel=info"
echo ""
echo "查看日志:"
echo "  tail -f $PROJECT_DIR/logs/celery_youtube_worker.log"
echo ""
