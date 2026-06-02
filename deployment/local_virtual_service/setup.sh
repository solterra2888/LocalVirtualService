#!/bin/bash
# ============================================================
# Local Virtual Service — 一键部署脚本（conda 版）
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="${DEPLOY_DIR:-/opt/local_virtual_service}"
CONDA_ENV_NAME="yt_service"
CONDA_BASE=$(conda info --base 2>/dev/null || echo "$HOME/miniconda3")
CONDA_ENV_BIN="$CONDA_BASE/envs/$CONDA_ENV_NAME/bin"
SYSTEMD_DEST="/etc/systemd/system"

echo "=========================================="
echo "  YouTube Transcription Worker 部署"
echo "  部署目录: $DEPLOY_DIR"
echo "  Conda 环境: $CONDA_ENV_NAME"
echo "=========================================="

# 1. 基础依赖
echo "[1/5] 检查系统依赖..."
if ! command -v conda &>/dev/null; then
    echo "  ❌ 未找到 conda，请先安装 Miniconda: https://docs.conda.io/en/latest/miniconda.html"
    exit 1
fi
if ! command -v nc &>/dev/null; then
    echo "  安装 netcat-openbsd（health check 用）..."
    sudo apt update && sudo apt install -y netcat-openbsd
fi
if ! command -v ffmpeg &>/dev/null; then
    echo "  安装 ffmpeg..."
    sudo apt update && sudo apt install -y ffmpeg
fi
echo "  系统依赖就绪 ✓"

# 2. 复制服务文件（从其他目录部署到 DEPLOY_DIR 时才复制；已在部署目录则跳过）
echo "[2/5] 部署服务文件..."
mkdir -p "$DEPLOY_DIR/worker" "$DEPLOY_DIR/logs" "$DEPLOY_DIR/scripts"
if [ "$(cd "$SCRIPT_DIR" && pwd -P)" = "$(cd "$DEPLOY_DIR" && pwd -P)" ]; then
    echo "  已在部署目录 ($DEPLOY_DIR)，跳过文件复制 ✓"
else
    cp "$SCRIPT_DIR/requirements.txt" "$DEPLOY_DIR/"
    cp "$SCRIPT_DIR/worker/"*.py "$DEPLOY_DIR/worker/"
    cp "$SCRIPT_DIR/start.sh" "$DEPLOY_DIR/"
    cp "$SCRIPT_DIR/run_worker.sh" "$DEPLOY_DIR/"
    cp "$SCRIPT_DIR/scripts/health_check.sh" "$DEPLOY_DIR/scripts/"
fi
chmod +x "$DEPLOY_DIR/start.sh" "$DEPLOY_DIR/run_worker.sh" "$DEPLOY_DIR/scripts/health_check.sh"
echo "  服务文件就绪 ✓"

# 3. Conda 环境
echo "[3/5] 配置 Conda 环境 ($CONDA_ENV_NAME)..."
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
    echo "[4/5] 生成 .env 模板..."
    cp "$SCRIPT_DIR/.env.template" "$DEPLOY_DIR/.env"
    chmod 600 "$DEPLOY_DIR/.env"
    echo "  ⚠️  请编辑 $DEPLOY_DIR/.env 填入实际密码和配置"
else
    echo "[4/5] .env 已存在 ✓"
fi

# 5. systemd unit 安装与迁移
echo "[5/5] 安装 systemd unit..."

# 5a. 迁移旧的 yt-worker.service（单进程模式）
if systemctl is-active --quiet yt-worker 2>/dev/null; then
    echo "  检测到旧 yt-worker.service 正在运行，停止并禁用..."
    systemctl disable --now yt-worker || true
fi
if [ -f "$SYSTEMD_DEST/yt-worker.service" ]; then
    echo "  删除旧 $SYSTEMD_DEST/yt-worker.service"
    rm -f "$SYSTEMD_DEST/yt-worker.service"
fi

# 5b. 安装新的 unit 文件
NEW_UNITS=(
    "yt-worker.target"
    "yt-worker-main.service"
    "yt-worker-long.service"
    "yt-worker-priority-transcript.service"
    "yt-worker-priority-asr.service"
    "yt-worker-health.service"
    "yt-worker-health.timer"
)
for unit in "${NEW_UNITS[@]}"; do
    src="$SCRIPT_DIR/systemd/$unit"
    if [ ! -f "$src" ]; then
        echo "  ❌ 找不到 $src" >&2
        exit 1
    fi
    # 替换 unit 中的部署路径占位符（如果 DEPLOY_DIR 非默认值）
    if [ "$DEPLOY_DIR" != "/opt/local_virtual_service" ]; then
        sed "s|/opt/local_virtual_service|$DEPLOY_DIR|g" "$src" > "$SYSTEMD_DEST/$unit"
    else
        cp "$src" "$SYSTEMD_DEST/$unit"
    fi
    echo "  已安装 $SYSTEMD_DEST/$unit"
done

systemctl daemon-reload
echo "  daemon-reload 完成 ✓"

# 5c. enable & start
systemctl enable yt-worker.target
systemctl enable yt-worker-main.service
systemctl enable yt-worker-long.service
systemctl enable yt-worker-priority-transcript.service
systemctl enable yt-worker-priority-asr.service
systemctl enable yt-worker-health.timer
echo "  systemd units enabled ✓"

systemctl start yt-worker.target
systemctl start yt-worker-health.timer
echo "  yt-worker.target 已启动 ✓"
echo "  yt-worker-health.timer 已启动 ✓"

echo ""
echo "=========================================="
echo "  部署完成！"
echo "=========================================="
echo ""
echo "常用命令:"
echo "  重启所有 worker:   systemctl restart yt-worker.target"
echo "  查看 worker 状态:  systemctl status 'yt-worker-*.service'"
echo "  查看 timer:        systemctl list-timers yt-worker-health"
echo "  实时日志:          tail -f $DEPLOY_DIR/logs/worker.log"
echo "  手动健康检查:      bash $DEPLOY_DIR/scripts/health_check.sh"
echo ""
echo "如需 Lark 告警，在 $DEPLOY_DIR/.env 填入:"
echo "  LARK_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/..."
echo ""