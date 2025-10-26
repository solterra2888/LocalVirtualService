#!/bin/bash
# 安全重启调度器脚本

echo "============================================================"
echo "调度器重启脚本"
echo "============================================================"

# 查找调度器进程
SCHEDULER_PID=$(ps aux | grep -E "scheduler_main|uvicorn.*8001" | grep -v grep | awk '{print $2}')

if [ -z "$SCHEDULER_PID" ]; then
    echo "❌ 未发现运行中的调度器进程"
else
    echo "🔍 发现调度器进程: PID=$SCHEDULER_PID"
    echo "⏳ 正在停止调度器..."
    kill $SCHEDULER_PID
    
    # 等待进程结束
    sleep 2
    
    # 确认已停止
    if ps -p $SCHEDULER_PID > /dev/null 2>&1; then
        echo "⚠️  进程仍在运行，强制终止..."
        kill -9 $SCHEDULER_PID
        sleep 1
    fi
    
    echo "✅ 调度器已停止"
fi

echo ""
echo "📋 请手动启动调度器："
echo "   cd /Users/kevinliu/rcproject"
echo "   /opt/anaconda3/envs/rcTool/bin/python -m uvicorn scheduler_main:scheduler_app --host 0.0.0.0 --port 8001 --reload --reload-dir backend --reload-dir config --log-level info --access-log"
echo ""
echo "💡 提示："
echo "   - 启动后，调度器将使用新的配置（coalesce=True + Redis锁）"
echo "   - 任务重叠问题将得到解决"
echo "   - 可以通过 tail -f logs/app.log 查看日志"
echo ""
echo "============================================================"

