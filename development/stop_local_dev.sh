#!/bin/bash
# 本地开发环境停止脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

echo -e "${RED}🛑 本地开发环境停止脚本${NC}"
echo "=============================================="
echo ""

# ── 从 PID 文件停止 ──────────────────────────────────────────
PID_FILE="$SCRIPT_DIR/.dev_pids"
if [ -f "$PID_FILE" ]; then
    echo -e "${BLUE}📖 从 PID 文件读取进程信息...${NC}"
    source "$PID_FILE"
    dev_stop_process "${FRONTEND_PID:-}" "前端应用"
    dev_stop_process "${WEBAPP_PID:-}" "Web 应用"
    dev_stop_process "${CELERY_PID:-}" "Celery file/ai Worker"
    dev_stop_process "${DATA_CELERY_PID:-}" "Celery data Worker"
    rm -f "$PID_FILE"
    echo -e "${GREEN}✅ PID 文件已清理${NC}"
else
    echo -e "${YELLOW}⚠️  PID 文件不存在，将通过端口/进程模式查找${NC}"
fi

# ── 停止由 start_local_dev 启动的 Redis（若有）─────────────────
if [ -f "$DEV_REDIS_PID_FILE" ]; then
    REDIS_PID=$(cat "$DEV_REDIS_PID_FILE")
    dev_stop_process "$REDIS_PID" "Redis (本地)"
    rm -f "$DEV_REDIS_PID_FILE" "$DEV_DIR/.env.redis.local"
fi

# ── 按端口停止 ────────────────────────────────────────────────
echo ""
echo -e "${BLUE}🔍 检查并停止端口上的进程...${NC}"
dev_stop_by_port 3000 "前端应用"
dev_stop_by_port 8000 "Web 应用"
dev_stop_by_port 5555 "Flower 监控"

# ── 按进程模式清理 ────────────────────────────────────────────
echo ""
echo -e "${BLUE}🧹 清理相关进程...${NC}"
dev_stop_by_pattern "npm start"          "Node.js (npm start)"
dev_stop_by_pattern "uvicorn.*main:app"  "Python (uvicorn)"
dev_stop_by_pattern "celery.*worker"     "Celery Worker"
dev_stop_by_pattern "celery.*flower"     "Flower 监控"

# ── 清理日志（可选）──────────────────────────────────────────
DEV_LOGS_DIR="$DEV_DIR/logs"
if [ -d "$DEV_LOGS_DIR" ]; then
    echo ""
    read -p "是否清理开发日志文件？(y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -f "$DEV_LOGS_DIR"/*.log
        echo -e "${GREEN}✅ 日志文件已清理${NC}"
    fi
fi

# ── 最终检查 ─────────────────────────────────────────────────
echo ""
echo -e "${BLUE}🔍 最终端口检查...${NC}"
for entry in "3000:前端应用" "8000:Web应用" "5555:Flower监控"; do
    port="${entry%%:*}"
    name="${entry##*:}"
    if lsof -Pi :"$port" -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo -e "${RED}❌ 端口 $port ($name) 仍有进程${NC}"
    else
        echo -e "${GREEN}✅ 端口 $port ($name) 已释放${NC}"
    fi
done

echo ""
echo -e "${GREEN}🎉 开发服务已停止!${NC}"
echo "=============================================="
echo -e "${CYAN}💡 重新启动: ./development/start_local_dev.sh${NC}"
echo "=============================================="
