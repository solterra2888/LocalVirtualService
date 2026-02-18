#!/bin/bash
# RC Project 服务停止脚本

echo "🛑 RC Project 服务停止脚本"
echo "================================"

# 检查是否以root权限运行
if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

echo "📋 停止服务列表:"
echo "   - Nginx代理"
echo "   - 调度器服务"
echo "   - Celery Worker (AI/文件处理)"
echo "   - Celery Worker (数据获取)"
echo "   - 前端应用"
echo "   - Web应用"
echo "   - Redis缓存"
echo "   - PostgreSQL数据库"
echo ""

# 停止服务并验证状态的辅助函数
stop_service() {
    local service=$1
    systemctl stop "$service" 2>/dev/null || true
    if systemctl is-active --quiet "$service"; then
        echo "   ⚠️  $service 可能未完全停止"
    else
        echo "   ✅ $service 已停止"
    fi
}

# 按依赖顺序停止（先停外层，再停内层）
stop_service nginx
stop_service rc-scheduler
stop_service rc-celery-ai
stop_service rc-celery-data
stop_service rc-frontend
stop_service rc-webapp
stop_service redis
stop_service postgresql

# ── 清理残留进程 ──────────────────────────────────────────────
echo ""
echo "🧹 清理残留进程..."

cleanup_port() {
    local port=$1
    local pids
    pids=$(lsof -t -i:"$port" 2>/dev/null)
    if [ -n "$pids" ]; then
        echo "   🔄 发现 $port 端口残留进程 (PID: $pids)，正在清理..."
        kill -15 $pids 2>/dev/null || true
        sleep 2
        pids=$(lsof -t -i:"$port" 2>/dev/null)
        if [ -n "$pids" ]; then
            kill -9 $pids 2>/dev/null || true
        fi
        echo "   ✅ $port 端口已清理"
    fi
}

cleanup_port 8000   # Web应用
cleanup_port 8001   # 调度器
cleanup_port 3000   # 前端应用

echo ""
echo "🎉 所有服务已停止!"
echo "================================"
echo "💡 要重新启动服务，请运行: sudo ./start_services.sh"
