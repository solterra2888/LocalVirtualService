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
echo "   - Redis缓存"
echo "   - Web应用"
echo "   - 前端应用"
echo "   - Celery Worker (AI处理)"
echo "   - Celery Worker (数据获取)"
echo "   - 调度器服务"
echo "   - Nginx代理"
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

# 启动Redis
echo "🔴 启动Redis缓存..."
systemctl start redis
if systemctl is-active --quiet redis; then
    echo "✅ Redis已启动"
else
    echo "⚠️ Redis启动失败，Celery任务将无法工作"
fi

# 等待数据库和缓存启动
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

# 启动Celery AI Worker
echo "🤖 启动Celery AI Worker..."
systemctl start rc-celery-ai
if systemctl is-active --quiet rc-celery-ai; then
    echo "✅ Celery AI Worker已启动"
else
    echo "⚠️ Celery AI Worker启动失败，新闻分析任务将无法执行"
fi

# 启动Celery Data Worker
echo "📊 启动Celery Data Worker..."
systemctl start rc-celery-data
if systemctl is-active --quiet rc-celery-data; then
    echo "✅ Celery Data Worker已启动"
else
    echo "⚠️ Celery Data Worker启动失败，数据获取任务将无法执行"
fi

# 启动调度器
echo "🕒 启动调度器服务..."
systemctl start rc-scheduler
if systemctl is-active --quiet rc-scheduler; then
    echo "✅ 调度器服务已启动"
else
    echo "⚠️ 调度器服务启动失败，定时任务将无法执行"
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
echo "   - 调度器API: http://111.230.48.119:8001"
echo ""
echo "🔧 服务管理命令:"
echo "   - 查看所有服务: systemctl status 'rc-*'"
echo "   - 停止所有服务: sudo ./stop_services.sh"
echo "   - 重启所有服务: sudo ./restart_services.sh"
echo ""
echo "📊 查看服务日志:"
echo "   - Web应用: journalctl -u rc-webapp -f"
echo "   - 调度器: journalctl -u rc-scheduler -f"
echo "   - Celery AI: journalctl -u rc-celery-ai -f"
echo "   - Celery Data: journalctl -u rc-celery-data -f"
echo ""
echo "🤖 Celery任务监控:"
echo "   - AI Worker日志: tail -f logs/celery_ai_worker.log"
echo "   - Data Worker日志: tail -f logs/celery_data_worker.log"
