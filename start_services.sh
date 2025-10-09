#!/bin/bash
# RC Project 服务启动脚本
# 快速启动所有服务

echo "🚀 RC Project 服务启动脚本"
echo "================================"

# 检查是否以root权限运行
if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

echo "📋 启动服务列表:"
echo "   - PostgreSQL数据库"
echo "   - Web应用"
echo "   - 前端应用"
echo "   - Nginx代理"
echo ""
echo "ℹ️  注意: 调度器服务需要单独启动"
echo "   启动调度器: sudo ./scheduler/start_scheduler.sh"
echo ""

# 启动PostgreSQL
echo "🗄️ 启动PostgreSQL数据库..."
systemctl start postgresql
if systemctl is-active --quiet postgresql; then
    echo "✅ PostgreSQL已启动"
else
    echo "❌ PostgreSQL启动失败"
    exit 1
fi

# 等待数据库启动
sleep 3

# 启动Web应用
echo "🌐 启动Web应用..."
systemctl start rc-webapp
if systemctl is-active --quiet rc-webapp; then
    echo "✅ Web应用已启动"
else
    echo "❌ Web应用启动失败"
    exit 1
fi

# 等待Web应用启动
sleep 3

# 启动前端应用
echo "🎨 启动前端应用..."
systemctl start rc-frontend
if systemctl is-active --quiet rc-frontend; then
    echo "✅ 前端应用已启动"
else
    echo "❌ 前端应用启动失败"
    exit 1
fi

# 启动Nginx
echo "🌐 启动Nginx代理..."
systemctl start nginx
if systemctl is-active --quiet nginx; then
    echo "✅ Nginx已启动"
else
    echo "❌ Nginx启动失败"
    exit 1
fi

echo ""
echo "🎉 所有服务启动完成!"
echo "================================"
echo "🌐 访问地址:"
echo "   - 前端应用: http://111.230.48.119:3000"
echo "   - Web应用: http://111.230.48.119:8000"
echo "   - API文档: http://111.230.48.119:8000/api/docs"
echo ""
echo "🔧 管理命令:"
echo "   - 查看状态: systemctl status rc-*"
echo "   - 停止服务: sudo ./stop_services.sh"
echo "   - 重启服务: sudo ./restart_services.sh"
echo ""
echo "🕒 调度器管理:"
echo "   - 启动调度器: sudo ./scheduler/start_scheduler.sh"
echo "   - 停止调度器: sudo ./scheduler/stop_scheduler.sh"
echo "   - 重启调度器: sudo ./scheduler/restart_scheduler.sh"
echo "   - 查看状态: sudo ./scheduler/status_scheduler.sh"
echo "   - 查看日志: sudo ./scheduler/logs_scheduler.sh"
