#!/bin/bash
# RC Project Celery Worker - Data Fetching Queue
# 专门处理数据爬取任务的Worker（Futu主题等）

set -e

# 脚本目录和项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "🔄 RC Project - Celery Worker (Data Fetching Queue)"
echo "=============================================="
echo "📂 项目根目录: $PROJECT_ROOT"
echo ""

# 加载环境变量（从项目根目录的 .env 文件）
ENV_FILE="$PROJECT_ROOT/.env"
if [ -f "$ENV_FILE" ]; then
    echo "📝 加载环境配置: $ENV_FILE"
    set -a  # 自动导出变量
    source "$ENV_FILE"
    set +a  # 关闭自动导出
    echo "✅ 环境变量加载完成"
else
    echo "⚠️  环境配置文件不存在: $ENV_FILE"
    echo "   请确保项目根目录有 .env 文件"
    echo "   可以从 example.env 复制: cp example.env .env"
    exit 1
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
    # 回退到当前激活的Python
    if command -v python &> /dev/null; then
        PYTHON_CMD="python"
        echo "✅ 使用当前激活的Python: $PYTHON_CMD"
    elif command -v python3 &> /dev/null; then
        PYTHON_CMD="python3"
        echo "⚠️  使用系统Python: $PYTHON_CMD"
    else
        echo "❌ Python未安装"
        exit 1
    fi
fi

echo "✅ Python版本: $($PYTHON_CMD --version)"

# 检查项目根目录
cd "$PROJECT_ROOT"

# 设置Python路径
export PYTHONPATH="$PROJECT_ROOT:${PYTHONPATH:-}"

# 检查Redis服务
echo ""
echo "🔍 检查Redis服务..."
REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"
echo "   Redis地址: $REDIS_URL"

# 测试Redis连接
if $PYTHON_CMD -c "
import redis
try:
    r = redis.from_url('$REDIS_URL')
    result = r.ping()
    if result:
        print('✅ Redis连接成功')
    else:
        print('❌ Redis ping失败')
        exit(1)
except Exception as e:
    print(f'❌ Redis连接失败: {e}')
    exit(1)
" 2>/dev/null; then
    echo "✅ Redis服务正常"
else
    echo "❌ Redis服务不可用"
    echo "请先启动Redis服务:"
    echo "   brew services start redis"
    echo "   或: redis-server"
    exit 1
fi

# 检查Celery配置
echo ""
echo "🔍 检查Celery配置..."
if $PYTHON_CMD -c "
import sys
sys.path.insert(0, '$PROJECT_ROOT')
try:
    from backend.celery_app import app as celery_app
    from backend.celery_config import broker_url, result_backend
    print(f'✅ Celery配置正常 - Broker: {broker_url}')
except Exception as e:
    print(f'❌ Celery配置错误: {e}')
    exit(1)
" 2>/dev/null; then
    echo "✅ Celery配置正常"
else
    echo "❌ Celery配置错误"
    exit 1
fi

# 创建日志目录
echo ""
echo "📁 创建日志目录..."
mkdir -p "$PROJECT_ROOT/logs"
echo "✅ 日志目录已创建: $PROJECT_ROOT/logs"

# 显示启动配置
echo ""
echo "🚀 Celery Worker 启动配置 (Data Fetching):"
echo "   - 队列: data_fetching"
echo "   - 并发数: 1 (避免并发爬取导致IP被封)"
echo "   - 最大任务数: 10 (每个子进程最多处理10个任务后重启)"
echo "   - 日志级别: ${CELERY_LOG_LEVEL:-info}"
echo "   - Redis地址: $REDIS_URL"
echo "   - Worker日志: logs/celery_worker_data_fetching.log"
echo "   - PID文件: logs/celery_worker_data_fetching.pid"
echo ""
echo "📌 注意: 此Worker专门处理长时间运行的数据爬取任务（如Futu主题爬取，约1小时）"
echo ""

# 启动Celery Worker
echo "🔄 启动Celery Worker..."
echo "   按 Ctrl+C 停止服务"
echo ""

# 使用celery命令启动Worker，专门用于data_fetching队列
exec $PYTHON_CMD -m celery -A backend.celery_app worker \
    -Q data_fetching \
    --concurrency=1 \
    --max-tasks-per-child=10 \
    --loglevel=${CELERY_LOG_LEVEL:-info} \
    --logfile="$PROJECT_ROOT/logs/celery_worker_data_fetching.log" \
    --pidfile="$PROJECT_ROOT/logs/celery_worker_data_fetching.pid" \
    -n data_fetching_worker@%h
