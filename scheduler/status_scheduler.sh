#!/bin/bash
# GuanHe Project 调度器状态检查脚本
# 检查调度器服务状态

APP_NAME="$(grep -s '^APP_NAME=' "$(cd "$(dirname "$0")/.." && pwd)/.env" 2>/dev/null | cut -d= -f2)"
APP_NAME="${APP_NAME:-GuanHe Project}"

echo "📊 $APP_NAME 调度器状态检查"
echo "================================"

# 检查是否以root权限运行
if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

echo "🔍 检查调度器服务状态..."
echo ""

# 检查systemd服务状态
echo "📋 Systemd服务状态:"
systemctl status rc-scheduler --no-pager -l

echo ""
echo "🌐 端口占用情况:"
echo "   8001端口 (调度器服务):"
PORT_8001_PIDS=$(lsof -t -i:8001 2>/dev/null)
if [ ! -z "$PORT_8001_PIDS" ]; then
    echo "   ✅ 8001端口被占用 (PID: $PORT_8001_PIDS)"
    lsof -i:8001
else
    echo "   ❌ 8001端口未被占用"
fi

echo ""
echo "🔗 网络连接测试:"
echo "   测试调度器API连接..."

# 测试健康检查接口
if command -v curl >/dev/null 2>&1; then
    echo "   - 健康检查接口:"
    curl -s http://localhost:8001/api/health 2>/dev/null | head -5
    if [ $? -eq 0 ]; then
        echo "   ✅ 健康检查接口正常"
    else
        echo "   ❌ 健康检查接口无响应"
    fi
    
    echo "   - 服务状态接口:"
    curl -s http://localhost:8001/api/status 2>/dev/null | head -5
    if [ $? -eq 0 ]; then
        echo "   ✅ 状态接口正常"
    else
        echo "   ❌ 状态接口无响应"
    fi
else
    echo "   ⚠️ curl命令不可用，无法测试API连接"
fi

echo ""
echo "📝 最近日志 (最后20行):"
journalctl -u rc-scheduler --no-pager -l -n 20

echo ""
echo "💾 资源使用情况:"
if command -v ps >/dev/null 2>&1; then
    ps aux | grep scheduler_main.py | grep -v grep
fi

echo ""
echo "🔧 管理命令:"
echo "   - 启动服务: sudo ./scheduler/start_scheduler.sh"
echo "   - 停止服务: sudo ./scheduler/stop_scheduler.sh"
echo "   - 重启服务: sudo ./scheduler/restart_scheduler.sh"
echo "   - 查看日志: journalctl -u rc-scheduler -f"
echo "   - 实时状态: watch -n 2 'systemctl status rc-scheduler'"
