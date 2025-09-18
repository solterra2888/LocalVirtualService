#!/bin/bash
# RC Project 调度器停止脚本
# 独立停止调度器服务

echo "🛑 RC Project 调度器停止脚本"
echo "================================"

# 检查是否以root权限运行
if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

echo "📋 停止调度器服务..."
echo "   - 停止调度器服务"
echo "   - 清理残留进程"
echo "   - 验证服务已停止"
echo ""

# 停止调度器服务
echo "🕒 停止调度器服务..."
systemctl stop rc-scheduler
echo "✅ 调度器服务已停止"

# 强制清理残留进程
echo ""
echo "🧹 清理残留进程..."

# 清理8001端口进程
PORT_8001_PIDS=$(lsof -t -i:8001 2>/dev/null)
if [ ! -z "$PORT_8001_PIDS" ]; then
    echo "🔄 发现8001端口残留进程，正在清理..."
    kill -15 $PORT_8001_PIDS 2>/dev/null
    sleep 2
    # 如果还存在，强制终止
    PORT_8001_PIDS=$(lsof -t -i:8001 2>/dev/null)
    if [ ! -z "$PORT_8001_PIDS" ]; then
        kill -9 $PORT_8001_PIDS 2>/dev/null
    fi
    echo "✅ 8001端口进程已清理"
else
    echo "✅ 8001端口无残留进程"
fi

# 验证服务已停止
echo ""
echo "🔍 验证服务状态..."
if systemctl is-active --quiet rc-scheduler; then
    echo "⚠️ 调度器服务仍在运行，尝试强制停止..."
    systemctl kill rc-scheduler
    sleep 2
    if systemctl is-active --quiet rc-scheduler; then
        echo "❌ 调度器服务无法停止"
        exit 1
    else
        echo "✅ 调度器服务已强制停止"
    fi
else
    echo "✅ 调度器服务已完全停止"
fi

echo ""
echo "🎉 调度器服务停止完成!"
echo "================================"
echo "💡 要重新启动调度器，请运行:"
echo "   sudo ./scheduler/start_scheduler.sh"
