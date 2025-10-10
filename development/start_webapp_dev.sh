#!/bin/bash
# RC Project Web应用本地开发启动脚本
# 用于本地开发环境

set -e

# 脚本目录和项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "🌐 RC Project - Web应用开发模式启动"
echo "======================================"
echo "📂 项目根目录: $PROJECT_ROOT"
echo ""

# 加载开发环境变量
ENV_FILE="$SCRIPT_DIR/env.development"
if [ -f "$ENV_FILE" ]; then
    echo "📝 加载开发环境配置: $ENV_FILE"
    set -a  # 自动导出变量
    source "$ENV_FILE"
    set +a  # 关闭自动导出
    echo "✅ 环境变量加载完成"
else
    echo "⚠️  环境配置文件不存在: $ENV_FILE"
    echo "   使用默认配置"
fi

# 检查Python环境
echo ""
echo "🔍 检查Python环境..."

# 检查conda环境
CONDA_ENV_PATH="/opt/anaconda3/envs/rcTool/bin/python"
if [ -f "$CONDA_ENV_PATH" ]; then
    PYTHON_CMD="$CONDA_ENV_PATH"
    echo "✅ 使用conda环境rcTool: $PYTHON_CMD"
else
    # 回退到系统Python
    if command -v python3 &> /dev/null; then
        PYTHON_CMD="python3"
        echo "⚠️  conda环境rcTool不存在，使用系统Python: $PYTHON_CMD"
    else
        echo "❌ Python未安装"
        exit 1
    fi
fi

echo "✅ Python版本: $($PYTHON_CMD --version)"

# 检查项目根目录
cd "$PROJECT_ROOT"

# 检查backend/main.py是否存在
if [ ! -f "backend/main.py" ]; then
    echo "❌ backend/main.py 不存在"
    exit 1
fi

# 设置Python路径
export PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}"

# 测试数据库连接
echo ""
echo "🔍 测试数据库连接..."
echo "   数据库地址: ${DB_HOST:-localhost}:${DB_PORT:-5432}"
echo "   数据库名称: ${DB_NAME:-rc_database}"

# 显示启动配置
echo ""
echo "🚀 Web应用启动配置:"
echo "   - 服务地址: http://localhost:8000"
echo "   - API文档: http://localhost:8000/docs"
echo "   - 数据库: ${DB_HOST:-localhost}:${DB_PORT:-5432}/${DB_NAME:-rc_database}"
echo "   - 调度器地址: ${SCHEDULER_SERVICE_URL:-http://localhost:8001}"
echo "   - 部署模式: ${DEPLOYMENT_MODE:-development}"
echo "   - 日志级别: ${LOG_LEVEL:-INFO}"
echo ""

# 启动Web应用
echo "🌐 启动Web应用开发服务器..."
echo "   按 Ctrl+C 停止服务"
echo ""

# 使用uvicorn启动FastAPI应用，开启热重载
exec "$PYTHON_CMD" -m uvicorn backend.main:app \
    --host 0.0.0.0 \
    --port 8000 \
    --reload \
    --reload-dir backend \
    --reload-dir config \
    --log-level info \
    --access-log
