#!/bin/bash
# GuanHe Project 调度器重启脚本
# 重启调度器服务

APP_NAME="$(grep -s '^APP_NAME=' "$(cd "$(dirname "$0")/.." && pwd)/.env" 2>/dev/null | cut -d= -f2)"
APP_NAME="${APP_NAME:-GuanHe Project}"

echo "🔄 $APP_NAME 调度器重启脚本"
echo "================================"

# 检查是否以root权限运行
if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

echo "📋 重启调度器服务..."
echo "   - 停止当前调度器服务"
echo "   - 清理残留进程"
echo "   - 重新启动调度器服务"
echo ""

# 停止调度器服务
echo "🛑 停止当前调度器服务..."
systemctl stop rc-scheduler
echo "✅ 调度器服务已停止"

# 清理残留进程
echo ""
echo "🧹 清理残留进程..."
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
fi

# 等待一下确保完全停止
sleep 3

# 重新启动调度器服务
echo ""
echo "🕒 重新启动调度器服务..."
systemctl start rc-scheduler

# 等待调度器启动
sleep 5

# 检查调度器状态
if systemctl is-active --quiet rc-scheduler; then
    echo "✅ 调度器服务重启成功"
    
    # 获取调度器状态信息
    echo ""
    echo "📊 调度器服务状态:"
    systemctl status rc-scheduler --no-pager -l
    
    SERVER_IP=$(grep -E '^SERVER_HOST=' "$(cd "$(dirname "$(dirname "$0")")" && pwd)/.env" 2>/dev/null | cut -d'=' -f2 | tr -d '"' | head -1)
    SCHED_PORT=$(grep -E '^SCHEDULER_PORT=' "$(cd "$(dirname "$(dirname "$0")")" && pwd)/deployment/deploy.conf" 2>/dev/null | cut -d'=' -f2 | head -1)
    SERVER_IP="${SERVER_IP:-localhost}"
    SCHED_PORT="${SCHED_PORT:-8001}"

    echo ""
    echo "🌐 调度器访问信息:"
    echo "   - 服务地址: http://$SERVER_IP:$SCHED_PORT"
    echo "   - API文档: http://$SERVER_IP:$SCHED_PORT/scheduler-docs"
    echo "   - 健康检查: http://$SERVER_IP:$SCHED_PORT/api/health"
    echo "   - 详细状态: http://$SERVER_IP:$SCHED_PORT/api/status"
    
else
    echo "❌ 调度器服务重启失败"
    echo ""
    echo "🔍 查看错误日志:"
    journalctl -u rc-scheduler --no-pager -l -n 20
    exit 1
fi

echo ""
echo "🎉 调度器服务重启完成!"
echo "================================"
