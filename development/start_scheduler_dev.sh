#!/bin/bash
# RC Project 调度器本地开发启动脚本
# 用于本地开发环境独立测试调度器功能

set -e

# 脚本目录和项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${PURPLE}🕒 RC Project - 调度器本地开发模式启动${NC}"
echo "=============================================="
echo -e "${CYAN}📂 项目根目录: $PROJECT_ROOT${NC}"
echo -e "${CYAN}📂 开发脚本目录: $SCRIPT_DIR${NC}"
echo ""

# 加载环境变量（从项目根目录的 .env 文件）
ENV_FILE="$PROJECT_ROOT/.env"
if [ -f "$ENV_FILE" ]; then
    echo -e "${BLUE}📝 加载环境配置: $ENV_FILE${NC}"
    set -a  # 自动导出变量
    source "$ENV_FILE"
    set +a  # 关闭自动导出
    echo -e "${GREEN}✅ 环境变量加载完成${NC}"
else
    echo -e "${RED}⚠️  环境配置文件不存在: $ENV_FILE${NC}"
    echo -e "${YELLOW}   请确保项目根目录有 .env 文件${NC}"
    echo -e "${YELLOW}   可以从 example.env 复制: cp example.env .env${NC}"
    exit 1
fi

# 检查Python环境
echo ""
echo -e "${BLUE}🔍 检查Python环境...${NC}"

# 检查conda环境
CONDA_ENV_PATH="/opt/anaconda3/envs/rcTool/bin/python"
if [ -f "$CONDA_ENV_PATH" ]; then
    PYTHON_CMD="$CONDA_ENV_PATH"
    echo -e "${GREEN}✅ 使用conda环境rcTool: $PYTHON_CMD${NC}"
else
    # 回退到系统Python
    if command -v python3 &> /dev/null; then
        PYTHON_CMD="python3"
        echo -e "${YELLOW}⚠️  conda环境rcTool不存在，使用系统Python: $PYTHON_CMD${NC}"
    else
        echo -e "${RED}❌ Python未安装${NC}"
        exit 1
    fi
fi

echo -e "${GREEN}✅ Python版本: $($PYTHON_CMD --version)${NC}"

# 检查项目根目录
cd "$PROJECT_ROOT"

# 检查scheduler_main.py是否存在
if [ ! -f "scheduler_main.py" ]; then
    echo -e "${RED}❌ scheduler_main.py 不存在${NC}"
    exit 1
fi

# 设置Python路径
export PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}"

# 检查端口占用
echo ""
echo -e "${BLUE}🔍 检查端口占用...${NC}"
if lsof -Pi :8001 -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo -e "${YELLOW}⚠️  端口 8001 已被占用${NC}"
    echo "   如需停止现有调度器服务，请运行:"
    echo "   pkill -f scheduler_main.py"
    echo ""
    read -p "是否强制停止现有服务并继续？(y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}🔄 停止现有调度器服务...${NC}"
        pkill -f scheduler_main.py 2>/dev/null || true
        sleep 2
    else
        echo -e "${YELLOW}启动已取消${NC}"
        exit 0
    fi
fi

# 测试数据库连接
echo ""
echo -e "${BLUE}🔍 测试数据库连接...${NC}"
echo "   数据库地址: ${DB_HOST:-localhost}:${DB_PORT:-5432}"
echo "   数据库名称: ${DB_NAME:-rc_database}"

if $PYTHON_CMD -c "
import sys
import asyncio
sys.path.insert(0, '$PROJECT_ROOT')

async def test_db():
    try:
        from backend.database.db_manager import DatabaseManager
        from config.settings import config
        
        db_config = {
            'db_host': config.get('DB_HOST', 'localhost'),
            'db_port': config.get('DB_PORT', '5432'),
            'db_name': config.get('DB_NAME', 'rc_database'),
            'db_user': config.get('DB_USER', 'postgres'),
            'db_password': config.get('DB_PASSWORD', '')
        }
        
        db_manager = DatabaseManager(db_config)
        if db_manager.ensure_schema():
            print('✅ 数据库连接和架构检查成功')
        else:
            print('❌ 数据库连接失败')
            exit(1)
    except Exception as e:
        print(f'❌ 数据库连接失败: {str(e)}')
        exit(1)

asyncio.run(test_db())
" 2>/dev/null; then
    echo -e "${GREEN}✅ 数据库连接正常${NC}"
else
    echo -e "${RED}❌ 数据库连接失败${NC}"
    echo "请检查数据库配置和网络连接"
    exit 1
fi

# 测试调度器模块导入
echo ""
echo -e "${BLUE}🔍 测试调度器模块导入...${NC}"
if $PYTHON_CMD -c "
import sys
sys.path.insert(0, '$PROJECT_ROOT')
try:
    from backend.services.scheduler_service import get_scheduler_service
    from backend.api.scheduler import router
    import scheduler_main
    print('✅ 所有调度器模块导入成功')
except Exception as e:
    print(f'❌ 模块导入失败: {str(e)}')
    exit(1)
" 2>/dev/null; then
    echo -e "${GREEN}✅ 调度器模块导入正常${NC}"
else
    echo -e "${RED}❌ 调度器模块导入失败${NC}"
    echo "请检查Python环境和依赖包"
    exit 1
fi

# 显示启动配置
echo ""
echo -e "${BLUE}🚀 调度器启动配置:${NC}"
echo "   - 服务地址: http://localhost:8001"
echo "   - API文档: http://localhost:8001/scheduler-docs"
echo "   - 健康检查: http://localhost:8001/api/health"
echo "   - 详细状态: http://localhost:8001/api/status"
echo "   - 数据库: ${DB_HOST:-localhost}:${DB_PORT:-5432}/${DB_NAME:-rc_database}"
echo "   - 部署模式: ${DEPLOYMENT_MODE:-development}"
echo "   - 日志级别: ${LOG_LEVEL:-INFO}"
echo ""

# 创建日志目录
LOGS_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOGS_DIR"

# 启动调度器服务
echo -e "${PURPLE}🕒 启动调度器开发服务器...${NC}"
echo "   按 Ctrl+C 停止服务"
echo ""

# 使用uvicorn启动调度器服务，开启热重载
exec "$PYTHON_CMD" -m uvicorn scheduler_main:scheduler_app \
    --host 0.0.0.0 \
    --port 8001 \
    --reload \
    --reload-dir backend \
    --reload-dir config \
    --log-level info \
    --access-log
