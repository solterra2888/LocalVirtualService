#!/bin/bash
# 重启 Celery Data Fetching Worker

echo "🔄 重启 Celery Data Fetching Worker..."

# 项目根目录
PROJECT_ROOT="/Users/kevinliu/rcproject"
cd "$PROJECT_ROOT"

# 日志文件
LOG_FILE="logs/celery_worker_data_fetching.log"
PID_FILE="logs/celery_worker_data_fetching.pid"

# 1. 停止现有 worker
echo "⏹️  停止现有 worker..."
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p $PID > /dev/null 2>&1; then
        kill $PID
        echo "   已发送停止信号到进程 $PID"
        
        # 等待进程停止
        for i in {1..10}; do
            if ! ps -p $PID > /dev/null 2>&1; then
                echo "   ✅ 进程已停止"
                break
            fi
            echo "   等待进程停止... ($i/10)"
            sleep 1
        done
        
        # 如果还没停止，强制杀死
        if ps -p $PID > /dev/null 2>&1; then
            echo "   ⚠️  强制停止进程"
            kill -9 $PID
        fi
    fi
    rm -f "$PID_FILE"
else
    # 尝试通过进程名杀死
    pkill -f "celery.*data_fetching"
    echo "   已尝试停止所有 data_fetching worker"
fi

# 等待一下确保端口释放
sleep 2

# 2. 清理旧日志（可选）
if [ -f "$LOG_FILE" ]; then
    # 备份旧日志
    BACKUP_LOG="${LOG_FILE}.$(date +%Y%m%d_%H%M%S)"
    mv "$LOG_FILE" "$BACKUP_LOG"
    echo "   旧日志已备份到: $BACKUP_LOG"
fi

# 3. 启动新 worker
echo "🚀 启动新 worker..."
celery -A backend.celery_app worker \
    --loglevel=INFO \
    --queues=data_fetching \
    --concurrency=2 \
    --max-tasks-per-child=50 \
    --logfile="$LOG_FILE" \
    --pidfile="$PID_FILE" \
    --detach

# 4. 验证启动
sleep 3

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p $PID > /dev/null 2>&1; then
        echo "✅ Worker 启动成功!"
        echo "   PID: $PID"
        echo "   日志: tail -f $LOG_FILE"
        
        # 检查任务注册
        echo ""
        echo "🔍 检查任务注册..."
        sleep 2
        celery -A backend.celery_app inspect registered 2>/dev/null | grep -q "fetch_eodhd_fundamentals"
        if [ $? -eq 0 ]; then
            echo "✅ 任务 'fetch_eodhd_fundamentals' 已注册"
        else
            echo "⚠️  任务 'fetch_eodhd_fundamentals' 未找到"
            echo "   请检查日志: tail -f $LOG_FILE"
        fi
    else
        echo "❌ Worker 启动失败"
        echo "   请检查日志: tail -f $LOG_FILE"
        exit 1
    fi
else
    echo "❌ Worker 启动失败（未找到 PID 文件）"
    echo "   请检查日志: tail -f $LOG_FILE"
    exit 1
fi

echo ""
echo "📊 Worker 信息:"
echo "   队列: data_fetching"
echo "   并发: 2"
echo "   日志: $LOG_FILE"
echo "   PID: $PID_FILE"
echo ""
echo "💡 提示:"
echo "   - 查看日志: tail -f $LOG_FILE"
echo "   - 查看状态: celery -A backend.celery_app inspect active"
echo "   - 测试任务: python3 test_fundamentals_celery.py"

