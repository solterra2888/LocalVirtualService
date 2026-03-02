#!/bin/bash
# 调度器本地开发启动脚本（uvicorn，不启用热重载避免任务重复执行）

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

echo -e "${PURPLE}🕒 调度器本地开发模式启动${NC}"
echo "=============================================="

dev_load_conf
dev_load_env
dev_detect_python

cd "$PROJECT_ROOT"

if [ ! -f "scheduler_main.py" ]; then
    echo -e "${RED}❌ scheduler_main.py 不存在${NC}"
    exit 1
fi

# 检查端口占用
echo ""
if ! dev_check_port 8001 "调度器"; then
    read -p "是否强制停止现有服务并继续？(y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        dev_stop_by_port 8001 "调度器"
        sleep 2
    else
        echo -e "${YELLOW}启动已取消${NC}"
        exit 0
    fi
fi

dev_check_db || exit 1

echo ""
echo -e "${CYAN}🚀 启动配置:${NC}"
echo "   - 服务地址: http://localhost:8001"
echo "   - API 文档: http://localhost:8001/scheduler-docs"
echo "   - 健康检查: http://localhost:8001/api/health"
echo "   - 数据库: ${DB_HOST:-localhost}:${DB_PORT:-5432}/${DB_NAME:-rc_database}"
echo "   - 部署模式: ${DEPLOYMENT_MODE:-development}"
echo ""
echo "   按 Ctrl+C 停止服务"
echo ""

exec "$PYTHON_CMD" -m uvicorn scheduler_main:scheduler_app \
    --host 0.0.0.0 \
    --port 8001 \
    --log-level info \
    --access-log
