#!/bin/bash
# ============================================================
# Local Virtual Service — 一键部署脚本（conda 版）
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="${DEPLOY_DIR:-$HOME/local_virtual_service}"
CONDA_ENV_NAME="yt_service"
CONDA_BASE=$(conda info --base 2>/dev/null || echo "$HOME/miniconda3")
CONDA_ENV_BIN="$CONDA_BASE/envs/$CONDA_ENV_NAME/bin"

echo "=========================================="
echo "  YouTube Transcription Worker 部署"
echo "  部署目录: $DEPLOY_DIR"
echo "  Conda 环境: $CONDA_ENV_NAME"
echo "=========================================="

# 1. 基础依赖
echo "[1/4] 检查系统依赖..."
if ! command -v conda &>/dev/null; then
    echo "  ❌ 未找到 conda，请先安装 Miniconda: https://docs.conda.io/en/latest/miniconda.html"
    exit 1
fi
if ! command -v ffmpeg &>/dev/null; then
    echo "  安装 ffmpeg..."
    sudo apt update && sudo apt install -y ffmpeg
fi
echo "  系统依赖就绪 ✓"

# 2. 复制服务文件
echo "[2/4] 部署服务文件..."
mkdir -p "$DEPLOY_DIR/worker" "$DEPLOY_DIR/logs"
cp "$SCRIPT_DIR/requirements.txt" "$DEPLOY_DIR/"
cp "$SCRIPT_DIR/worker/"*.py "$DEPLOY_DIR/worker/"
cp "$SCRIPT_DIR/start.sh" "$DEPLOY_DIR/"
chmod +x "$DEPLOY_DIR/start.sh"
echo "  服务文件就绪 ✓"

# 3. Conda 环境
echo "[3/4] 配置 Conda 环境 ($CONDA_ENV_NAME)..."
if conda env list | grep -q "^$CONDA_ENV_NAME "; then
    echo "  环境已存在，跳过创建 ✓"
else
    echo "  创建新环境..."
    conda create -n "$CONDA_ENV_NAME" python=3.11 -y
fi
echo "  安装 Python 依赖..."
"$CONDA_ENV_BIN/pip" install -q --upgrade pip
"$CONDA_ENV_BIN/pip" install -q -r "$DEPLOY_DIR/requirements.txt"
echo "  Python 依赖就绪 ✓"

# 4. 环境配置
if [ ! -f "$DEPLOY_DIR/.env" ]; then
    echo "[4/4] 生成 .env 模板..."
    cp "$SCRIPT_DIR/.env.template" "$DEPLOY_DIR/.env"
    chmod 600 "$DEPLOY_DIR/.env"
    echo "  ⚠️  请编辑 $DEPLOY_DIR/.env 填入实际密码和配置"
else
    echo "[4/4] .env 已存在 ✓"
fi

echo ""
echo "=========================================="
echo "  部署完成！"
echo "=========================================="
echo ""
echo "1. 编辑配置:  nano $DEPLOY_DIR/.env"
echo ""
echo "2. 前台启动（直接查看日志）:"
echo "   bash $DEPLOY_DIR/start.sh"
echo ""
echo "3. 后台启动（日志写入文件）:"
echo "   nohup bash $DEPLOY_DIR/start.sh > $DEPLOY_DIR/logs/worker.log 2>&1 &"
echo ""
echo "4. 查看后台日志:  tail -f $DEPLOY_DIR/logs/worker.log"
echo ""
