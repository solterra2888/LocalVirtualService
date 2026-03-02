#!/bin/bash
# Flower 监控开发启动脚本

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

echo -e "${BLUE}🌸 Flower 监控开发模式启动${NC}"
echo "=============================================="

dev_load_conf
dev_load_env
dev_detect_python

cd "$PROJECT_ROOT"

dev_check_redis || exit 1

# 检查 Flower 依赖
echo -e "${BLUE}🔍 检查 Flower 依赖...${NC}"
if ! $PYTHON_CMD -c "import flower" 2>/dev/null; then
    echo -e "${RED}❌ Flower 未安装，请执行: pip install flower${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Flower 已安装${NC}"

REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"

echo ""
echo -e "${CYAN}🚀 启动配置:${NC}"
echo "   - 监控地址: http://localhost:${FLOWER_PORT:-5555}"
echo "   - Redis: $REDIS_URL"
if [ -n "${FLOWER_AUTH:-}" ]; then
    echo "   - 认证: 已启用"
fi
echo ""
echo "   按 Ctrl+C 停止服务"
echo ""

FLOWER_ARGS=(
    -A backend.celery_app
    --broker="$REDIS_URL"
    flower
    --port="${FLOWER_PORT:-5555}"
)
[ -n "${FLOWER_AUTH:-}" ] && FLOWER_ARGS+=(--basic_auth="$FLOWER_AUTH")

exec "$PYTHON_CMD" -m celery "${FLOWER_ARGS[@]}"
