#!/bin/bash
# RC Project 服务停止脚本
# 停止所有服务

echo "🛑 RC Project 服务停止脚本"
echo "================================"

# 检查是否以root权限运行
if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

echo "📋 停止服务列表:"
echo "   - Nginx代理"
echo "   - 前端应用"
echo "   - Web应用"
echo "   - PostgreSQL数据库"
echo ""
echo "ℹ️  注意: 调度器服务需要单独停止"
echo "   停止调度器: sudo ./scheduler/stop_scheduler.sh"
echo ""

# 停止Nginx
echo "🌐 停止Nginx代理..."
systemctl stop nginx
echo "✅ Nginx已停止"

# 停止前端应用
echo "🎨 停止前端应用..."
systemctl stop rc-frontend
echo "✅ 前端应用已停止"

# 停止Web应用
echo "🌐 停止Web应用..."
systemctl stop rc-webapp
echo "✅ Web应用已停止"

# 停止PostgreSQL
echo "🗄️ 停止PostgreSQL数据库..."
systemctl stop postgresql
echo "✅ PostgreSQL已停止"

# 强制清理残留进程
echo ""
echo "🧹 清理残留进程..."

# 清理8000端口进程
PORT_8000_PIDS=$(lsof -t -i:8000 2>/dev/null)
if [ ! -z "$PORT_8000_PIDS" ]; then
    echo "🔄 发现8000端口残留进程，正在清理..."
    kill -15 $PORT_8000_PIDS 2>/dev/null
    sleep 2
    # 如果还存在，强制终止
    PORT_8000_PIDS=$(lsof -t -i:8000 2>/dev/null)
    if [ ! -z "$PORT_8000_PIDS" ]; then
        kill -9 $PORT_8000_PIDS 2>/dev/null
    fi
    echo "✅ 8000端口进程已清理"
fi

# 注意：8001端口由调度器管理脚本处理

# 清理3000端口进程
PORT_3000_PIDS=$(lsof -t -i:3000 2>/dev/null)
if [ ! -z "$PORT_3000_PIDS" ]; then
    echo "🔄 发现3000端口残留进程，正在清理..."
    kill -15 $PORT_3000_PIDS 2>/dev/null
    sleep 2
    # 如果还存在，强制终止
    PORT_3000_PIDS=$(lsof -t -i:3000 2>/dev/null)
    if [ ! -z "$PORT_3000_PIDS" ]; then
        kill -9 $PORT_3000_PIDS 2>/dev/null
    fi
    echo "✅ 3000端口进程已清理"
fi

echo ""
echo "🎉 所有服务已停止!"
echo "================================"
echo "💡 要重新启动服务，请运行:"
echo "   sudo ./start_services.sh"
echo ""
echo "🕒 调度器管理:"
echo "   - 启动调度器: sudo ./scheduler/start_scheduler.sh"
echo "   - 停止调度器: sudo ./scheduler/stop_scheduler.sh"
echo "   - 查看状态: sudo ./scheduler/status_scheduler.sh"
