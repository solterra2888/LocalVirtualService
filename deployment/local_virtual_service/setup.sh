#!/bin/bash
# ============================================================
# Local Virtual Service — 一键部署脚本
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="${DEPLOY_DIR:-$HOME/local_virtual_service}"
VENV_DIR="$DEPLOY_DIR/venv"

echo "=========================================="
echo "  YouTube Transcription Worker 部署"
echo "  部署目录: $DEPLOY_DIR"
echo "=========================================="

# 1. 基础依赖
echo "[1/5] 检查系统依赖..."
for cmd in python3 pip3 ffmpeg node; do
    if ! command -v $cmd &>/dev/null; then
        echo "  安装缺失的依赖..."
        sudo apt update && sudo apt install -y python3 python3-venv python3-pip ffmpeg nodejs
        break
    fi
done
echo "  系统依赖就绪 ✓"

# 2. 复制服务文件
echo "[2/5] 部署服务文件..."
mkdir -p "$DEPLOY_DIR/worker" "$DEPLOY_DIR/logs"
cp "$SCRIPT_DIR/requirements.txt" "$DEPLOY_DIR/"
cp "$SCRIPT_DIR/worker/"*.py "$DEPLOY_DIR/worker/"

# 3. Python 环境
echo "[3/5] 配置 Python 环境..."
if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"
pip install -q --upgrade pip
pip install -q -r "$DEPLOY_DIR/requirements.txt"

# 4. 环境配置
if [ ! -f "$DEPLOY_DIR/.env" ]; then
    echo "[4/5] 生成 .env 模板..."
    cp "$SCRIPT_DIR/.env.template" "$DEPLOY_DIR/.env"
    chmod 600 "$DEPLOY_DIR/.env"
    echo "  ⚠️  请编辑 $DEPLOY_DIR/.env 填入实际密码和配置"
else
    echo "[4/5] .env 已存在 ✓"
fi

# 5. systemd 服务
echo "[5/5] 安装 systemd 服务..."
SYSTEMD_DIR="$HOME/.config/systemd/user"
mkdir -p "$SYSTEMD_DIR"
cp "$SCRIPT_DIR/youtube-worker.service" "$SYSTEMD_DIR/"
systemctl --user daemon-reload
systemctl --user enable youtube-worker.service
loginctl enable-linger "$USER" 2>/dev/null || true

echo ""
echo "=========================================="
echo "  部署完成！"
echo "=========================================="
echo ""
echo "1. 编辑配置:  nano $DEPLOY_DIR/.env"
echo ""
echo "2. 启动服务:  systemctl --user start youtube-worker.service"
echo ""
echo "3. 前台调试:"
echo "   cd $DEPLOY_DIR && source venv/bin/activate"
echo "   celery -A worker.celery_app worker \\"
echo "     --queues=youtube_fetching,youtube_transcription \\"
echo "     --concurrency=1 --loglevel=info"
echo ""
echo "4. 查看日志:  tail -f $DEPLOY_DIR/logs/worker.log"
echo ""
