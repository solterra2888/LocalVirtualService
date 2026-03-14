#!/bin/bash
# 本地开发环境统一启动脚本
# 按顺序：环境检查 → Web应用 → 前端应用 → Celery file/ai Worker → Celery data Worker

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

echo -e "${BLUE}🚀 本地开发环境启动脚本${NC}"
echo "=============================================="
echo -e "${CYAN}📂 项目根目录: $PROJECT_ROOT${NC}"
echo ""

dev_load_conf
dev_load_env
dev_detect_python

# ── 检查端口占用 ─────────────────────────────────────────────
echo ""
echo -e "${BLUE}🔍 检查端口占用...${NC}"
PORT_CONFLICT=0
dev_check_port 6379 "Redis"      || PORT_CONFLICT=1
dev_check_port 8000 "Web应用"    || PORT_CONFLICT=1
dev_check_port 3000 "前端应用"   || PORT_CONFLICT=1
dev_check_port 5555 "Flower监控" || true

if [ $PORT_CONFLICT -eq 1 ]; then
    echo ""
    echo "   如需停止现有服务: ./development/stop_local_dev.sh"
    read -p "是否继续启动？(y/N): " -n 1 -r
    echo ""
    [[ ! $REPLY =~ ^[Yy]$ ]] && { echo -e "${YELLOW}启动已取消${NC}"; exit 0; }
fi

echo ""
echo -e "${GREEN}📋 启动顺序:${NC}"
echo "   1. 环境与依赖检查（含 Redis）"
echo "   2. Web应用 (localhost:8000)"
echo "   3. 前端应用 (localhost:3000)"
echo "   4. Celery Worker - file_processing + ai_processing（上传文档/音频/AI分析）"
echo "   5. Celery Worker - data_fetching（订阅、数据爬取等后台任务）"
echo ""

dev_ensure_logs

# ── 步骤 1/5: 环境检查 ──────────────────────────────────────
echo -e "${BLUE}========================================${NC}"
echo -e "${PURPLE}🔍 步骤 1/5: 环境与依赖检查${NC}"
echo -e "${BLUE}========================================${NC}"

dev_check_db
dev_check_redis || { dev_start_redis_if_needed || {
    read -p "Redis 不可用，是否继续？(y/N): " -n 1 -r
    echo ""
    [[ ! $REPLY =~ ^[Yy]$ ]] && exit 1
} }
dev_check_celery || true
dev_check_remote_scheduler

echo -e "${GREEN}🎉 环境检查完成${NC}"
echo ""

# ── 步骤 2/5: 启动 Web 应用 ─────────────────────────────────
echo -e "${BLUE}========================================${NC}"
echo -e "${PURPLE}🌐 步骤 2/5: 启动 Web 应用${NC}"
echo -e "${BLUE}========================================${NC}"

DEV_LOGS_DIR="$DEV_DIR/logs"
mkdir -p "$DEV_LOGS_DIR"

nohup bash "$SCRIPT_DIR/start_webapp_dev.sh" > "$DEV_LOGS_DIR/webapp.log" 2>&1 &
WEBAPP_PID=$!
echo "Web 应用进程 PID: $WEBAPP_PID"

echo "等待 Web 应用启动..."
for i in {1..30}; do
    if curl -s http://localhost:8000/health >/dev/null 2>&1; then
        echo -e "${GREEN}✅ Web 应用已启动 (http://localhost:8000)${NC}"
        break
    fi
    sleep 2
    echo -n "."
done

if ! curl -s http://localhost:8000/health >/dev/null 2>&1; then
    echo -e "${RED}❌ Web 应用启动失败${NC}"
    echo "查看日志: tail -f $DEV_LOGS_DIR/webapp.log"
    exit 1
fi

sleep 3

# ── 步骤 3/5: 启动前端应用 ──────────────────────────────────
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${PURPLE}🎨 步骤 3/5: 启动前端应用${NC}"
echo -e "${BLUE}========================================${NC}"

nohup bash "$SCRIPT_DIR/start_frontend_dev.sh" > "$DEV_LOGS_DIR/frontend.log" 2>&1 &
FRONTEND_PID=$!
echo "前端应用进程 PID: $FRONTEND_PID"

echo "等待前端应用启动..."
for i in {1..60}; do
    if curl -s http://localhost:3000 >/dev/null 2>&1; then
        echo -e "${GREEN}✅ 前端应用已启动 (http://localhost:3000)${NC}"
        break
    fi
    sleep 2
    echo -n "."
done

if ! curl -s http://localhost:3000 >/dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  前端应用可能还未就绪，请查看日志: tail -f $DEV_LOGS_DIR/frontend.log${NC}"
fi

# ── 步骤 4/5: 启动 Celery Worker（file_processing + ai_processing）──
# 处理上传文档/音频（file_processing）及AI分析（ai_processing）队列的任务
# 没有这个 Worker，上传任务进度会一直为 0
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${PURPLE}📤 步骤 4/5: 启动 Celery Worker (file_processing + ai_processing)${NC}"
echo -e "${BLUE}========================================${NC}"

nohup bash "$SCRIPT_DIR/start_celery_worker_dev.sh" > /dev/null 2>&1 &
CELERY_PID=$!
echo "Celery file/ai Worker 进程 PID: $CELERY_PID"
sleep 2
if kill -0 "$CELERY_PID" 2>/dev/null; then
    echo -e "${GREEN}✅ Celery file/ai Worker 已启动${NC}"
    echo -e "   日志: tail -f $LOGS_DIR/celery_worker.log"
else
    echo -e "${YELLOW}⚠️  Celery file/ai Worker 可能启动失败，请查看: tail -f $LOGS_DIR/celery_worker.log${NC}"
fi

# ── 步骤 5/5: 启动 Celery Worker（data_fetching）──
# 处理订阅内容拉取（Twitter、YouTube Feed）、EODHD 数据等后台数据任务
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${PURPLE}📊 步骤 5/5: 启动 Celery Worker (data_fetching)${NC}"
echo -e "${BLUE}========================================${NC}"

nohup bash "$SCRIPT_DIR/start_celery_worker_data_fetching.sh" > /dev/null 2>&1 &
DATA_CELERY_PID=$!
echo "Celery data Worker 进程 PID: $DATA_CELERY_PID"
sleep 2
if kill -0 "$DATA_CELERY_PID" 2>/dev/null; then
    echo -e "${GREEN}✅ Celery data Worker 已启动${NC}"
    echo -e "   日志: tail -f $LOGS_DIR/celery_worker_data_fetching.log"
else
    echo -e "${YELLOW}⚠️  Celery data Worker 可能启动失败，请查看: tail -f $LOGS_DIR/celery_worker_data_fetching.log${NC}"
fi

# 保存 PID
PID_FILE="$SCRIPT_DIR/.dev_pids"
cat > "$PID_FILE" <<EOF
WEBAPP_PID=$WEBAPP_PID
FRONTEND_PID=$FRONTEND_PID
CELERY_PID=$CELERY_PID
DATA_CELERY_PID=$DATA_CELERY_PID
EOF

echo ""
echo -e "${GREEN}🎉 所有服务启动完成!${NC}"
echo "=============================================="
echo -e "${CYAN}🌐 访问地址:${NC}"
echo "   - 前端应用: http://localhost:3000"
echo "   - Web 应用: http://localhost:8000"
echo "   - API 文档: http://localhost:8000/docs"
echo ""
echo -e "${CYAN}📝 日志文件:${NC}"
echo "   - Web 应用:       tail -f $DEV_LOGS_DIR/webapp.log"
echo "   - 前端应用:       tail -f $DEV_LOGS_DIR/frontend.log"
echo "   - Celery file/ai: tail -f $LOGS_DIR/celery_worker.log"
echo "   - Celery data:    tail -f $LOGS_DIR/celery_worker_data_fetching.log"
echo ""
echo -e "${YELLOW}⚠️  停止服务: ./development/stop_local_dev.sh${NC}"
echo "=============================================="
