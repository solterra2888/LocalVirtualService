#!/bin/bash
# ============================================================
# YouTube Remote Worker 一键部署脚本
# 在家用 Linux 电脑上运行
# ============================================================
set -e

SERVER_HOST="43.99.37.76"
PROJECT_DIR="$HOME/guanhetech"
VENV_DIR="$PROJECT_DIR/venv"

echo "=========================================="
echo "  YouTube Remote Worker 部署"
echo "=========================================="

# 1. 检查 autossh
if ! command -v autossh &> /dev/null; then
    echo "[1/6] 安装 autossh..."
    sudo apt update && sudo apt install -y autossh
else
    echo "[1/6] autossh 已安装 ✓"
fi

# 2. 检查 SSH 免密登录
echo "[2/6] 检查 SSH 连接..."
if ! ssh -o BatchMode=yes -o ConnectTimeout=5 root@$SERVER_HOST "echo ok" &> /dev/null; then
    echo ""
    echo "  ⚠️  SSH 免密登录未配置，正在设置..."
    echo "  请按提示输入服务器 root 密码："
    ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N "" -q 2>/dev/null || true
    ssh-copy-id root@$SERVER_HOST
    echo "  SSH 免密登录配置完成 ✓"
else
    echo "  SSH 免密连接正常 ✓"
fi

# 3. 克隆/更新项目
if [ -d "$PROJECT_DIR/.git" ]; then
    echo "[3/6] 更新项目代码..."
    cd "$PROJECT_DIR" && git pull
else
    echo "[3/6] 克隆项目代码..."
    REPO_URL=$(ssh root@$SERVER_HOST "cd /opt/guanhetech && git remote get-url origin 2>/dev/null || echo ''")
    if [ -z "$REPO_URL" ]; then
        echo "  ⚠️  无法获取仓库地址，请手动克隆项目到 $PROJECT_DIR"
        exit 1
    fi
    git clone "$REPO_URL" "$PROJECT_DIR"
fi
cd "$PROJECT_DIR"

# 4. Python 环境
echo "[4/6] 配置 Python 环境..."
if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"
pip install -q -r requirements.txt 2>/dev/null || pip install -q celery redis psycopg2-binary feedparser requests youtube-transcript-api

mkdir -p logs

# 5. 生成 .env
if [ ! -f "$PROJECT_DIR/.env" ]; then
    echo "[5/6] 生成 .env 配置..."

    # 从服务器读取密码
    DB_PASSWORD=$(ssh root@$SERVER_HOST "grep '^DB_PASSWORD=' /opt/guanhetech/.env | cut -d= -f2-")
    REDIS_URL_RAW=$(ssh root@$SERVER_HOST "grep '^REDIS_URL=' /opt/guanhetech/.env | cut -d= -f2-")
    # 替换 Redis URL 中的 127.0.0.1:6379 为本地隧道端口
    REDIS_URL_LOCAL=$(echo "$REDIS_URL_RAW" | sed 's/127\.0\.0\.1:6379/127.0.0.1:16379/')

    cat > "$PROJECT_DIR/.env" <<EOF
# YouTube Remote Worker 环境变量（自动生成）
DB_HOST=127.0.0.1
DB_PORT=15432
DB_NAME=guanhe_database
DB_USER=postgres
DB_PASSWORD=$DB_PASSWORD
REDIS_URL=$REDIS_URL_LOCAL
LOG_DIR=./logs
LOG_LEVEL=INFO
LOG_TO_FILE=true
LOG_TO_CONSOLE=true
EOF
    echo "  .env 已生成 ✓"
else
    echo "[5/6] .env 已存在，跳过 ✓"
fi

# 6. 安装 systemd 用户服务
echo "[6/6] 安装 systemd 服务..."
SYSTEMD_DIR="$HOME/.config/systemd/user"
mkdir -p "$SYSTEMD_DIR"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 如果脚本在 deployment/remote-youtube-worker/ 下
if [ -f "$SCRIPT_DIR/ssh-tunnel.service" ]; then
    cp "$SCRIPT_DIR/ssh-tunnel.service" "$SYSTEMD_DIR/"
    cp "$SCRIPT_DIR/youtube-worker.service" "$SYSTEMD_DIR/"
# 否则从项目目录拷贝
elif [ -f "$PROJECT_DIR/deployment/remote-youtube-worker/ssh-tunnel.service" ]; then
    cp "$PROJECT_DIR/deployment/remote-youtube-worker/ssh-tunnel.service" "$SYSTEMD_DIR/"
    cp "$PROJECT_DIR/deployment/remote-youtube-worker/youtube-worker.service" "$SYSTEMD_DIR/"
fi

# 如果使用 conda 而非 venv，替换 ExecStart 路径
if [ -n "$CONDA_PREFIX" ] && [ "$CONDA_PREFIX" != "$VENV_DIR" ]; then
    sed -i "s|%h/guanhetech/venv/bin/celery|$CONDA_PREFIX/bin/celery|g" "$SYSTEMD_DIR/youtube-worker.service"
fi

systemctl --user daemon-reload
systemctl --user enable ssh-tunnel.service
systemctl --user enable youtube-worker.service

# 允许用户服务在用户未登录时保持运行
loginctl enable-linger "$USER" 2>/dev/null || true

echo ""
echo "=========================================="
echo "  部署完成！"
echo "=========================================="
echo ""
echo "启动服务:"
echo "  systemctl --user start ssh-tunnel.service"
echo "  systemctl --user start youtube-worker.service"
echo ""
echo "查看状态:"
echo "  systemctl --user status ssh-tunnel.service"
echo "  systemctl --user status youtube-worker.service"
echo ""
echo "查看日志:"
echo "  tail -f $PROJECT_DIR/logs/celery_youtube_worker.log"
echo ""
