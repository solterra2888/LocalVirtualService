#!/bin/bash
# RC Project Celery Worker 开发启动脚本
# 用于本地开发环境启动Celery Worker

set -e

# 脚本目录和项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "🔄 RC Project - Celery Worker 开发模式启动"
echo "=============================================="
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
CONDA_ENV_PATH="/Users/kevinliu/anaconda3/envs/rcTool/bin/python"
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

# 检查任务注册
echo ""
echo "🔍 检查任务注册..."
if $PYTHON_CMD -c "
import sys
sys.path.insert(0, '$PROJECT_ROOT')
try:
    from backend.celery_app import app as celery_app
    registered_tasks = celery_app.tasks.keys()
    custom_tasks = [t for t in registered_tasks if not t.startswith('celery.')]
    if 'process_files_async' in registered_tasks:
        print(f'✅ process_files_async 任务已注册')
        print(f'✅ 总共注册了 {len(custom_tasks)} 个自定义任务')
    else:
        print('❌ process_files_async 任务未注册')
        print(f'已注册的任务: {custom_tasks}')
        exit(1)
except Exception as e:
    print(f'❌ 任务注册检查失败: {e}')
    exit(1)
" 2>/dev/null; then
    echo "✅ 任务注册正常"
else
    echo "❌ 任务注册异常"
    exit 1
fi

# 显示启动配置
echo ""
echo "🚀 Celery Worker 启动配置:"
echo "   - Worker数量: ${CELERY_CONCURRENCY:-2}"
echo "   - 日志级别: ${CELERY_LOG_LEVEL:-info}"
echo "   - Redis地址: $REDIS_URL"
echo "   - 队列: file_processing, ai_processing"
echo ""

# 启动Celery Worker
echo "🔄 启动Celery Worker..."
echo "   按 Ctrl+C 停止服务"
echo ""

# 使用celery命令启动Worker
exec $PYTHON_CMD -m celery -A backend.celery_app worker \
    --loglevel=${CELERY_LOG_LEVEL:-info} \
    --concurrency=${CELERY_CONCURRENCY:-2} \
    --queues=file_processing,ai_processing \
    --hostname=dev-worker@%h
