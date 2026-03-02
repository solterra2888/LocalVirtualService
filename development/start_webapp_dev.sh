#!/bin/bash
# Web 应用本地开发启动脚本（uvicorn + 热重载）

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

echo -e "${BLUE}🌐 Web 应用开发模式启动${NC}"
echo "======================================"

dev_load_conf
dev_load_env
dev_detect_python

cd "$PROJECT_ROOT"

if [ ! -f "backend/main.py" ]; then
    echo -e "${RED}❌ backend/main.py 不存在${NC}"
    exit 1
fi

echo ""
echo -e "${CYAN}🚀 启动配置:${NC}"
echo "   - 服务地址: http://localhost:8000"
echo "   - API 文档: http://localhost:8000/docs"
echo "   - 数据库: ${DB_HOST:-localhost}:${DB_PORT:-5432}/${DB_NAME:-rc_database}"
echo "   - 调度器: ${SCHEDULER_SERVICE_URL:-http://localhost:8001}"
echo "   - 部署模式: ${DEPLOYMENT_MODE:-development}"
echo ""
echo "   按 Ctrl+C 停止服务"
echo ""

exec "$PYTHON_CMD" -m uvicorn backend.main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --reload \
    --reload-dir backend \
    --reload-dir config \
    --log-level info \
    --access-log
