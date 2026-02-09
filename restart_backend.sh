#!/bin/bash
# 重启后端服务脚本

echo "======================================"
echo "🔄 重启后端服务"
echo "======================================"
echo ""

cd /Users/kevinliu/Documents/rcproject

# 检查是否有正在运行的 uvicorn 进程
echo "1️⃣ 检查现有进程..."
PIDS=$(ps aux | grep "uvicorn backend.main:app" | grep -v grep | awk '{print $2}')

if [ -n "$PIDS" ]; then
    echo "找到运行中的后端进程: $PIDS"
    echo "正在停止..."
    echo "$PIDS" | xargs kill
    sleep 2
    echo "✅ 已停止旧进程"
else
    echo "未找到运行中的后端进程"
fi

echo ""
echo "2️⃣ 启动后端服务..."
echo "======================================"
echo ""

# 启动新的后端服务
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
