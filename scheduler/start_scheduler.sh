#!/bin/bash
# RC Project 调度器启动脚本
# 独立启动调度器服务

echo "🕒 RC Project 调度器启动脚本"
echo "================================"

# 检查是否以root权限运行
if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

echo "📋 启动调度器服务..."
echo "   - 检查PostgreSQL数据库状态"
echo "   - 启动调度器服务"
echo "   - 验证服务运行状态"
echo ""

# 检查PostgreSQL是否运行
echo "🗄️ 检查PostgreSQL数据库状态..."
if systemctl is-active --quiet postgresql; then
    echo "✅ PostgreSQL正在运行"
else
    echo "⚠️ PostgreSQL未运行，正在启动..."
    systemctl start postgresql
    if systemctl is-active --quiet postgresql; then
        echo "✅ PostgreSQL已启动"
    else
        echo "❌ PostgreSQL启动失败，调度器无法启动"
        exit 1
    fi
fi

# 等待数据库完全启动
sleep 3

# 启动调度器服务
echo "🕒 启动调度器服务..."
systemctl start rc-scheduler

# 等待调度器启动
sleep 5

# 检查调度器状态
if systemctl is-active --quiet rc-scheduler; then
    echo "✅ 调度器服务已启动"
    
    # 获取调度器状态信息
    echo ""
    echo "📊 调度器服务状态:"
    systemctl status rc-scheduler --no-pager -l
    
    echo ""
    echo "🌐 调度器访问信息:"
    echo "   - 服务地址: http://111.230.48.119:8001"
    echo "   - API文档: http://111.230.48.119:8001/scheduler-docs"
    echo "   - 健康检查: http://111.230.48.119:8001/api/health"
    echo "   - 详细状态: http://111.230.48.119:8001/api/status"
    
    echo ""
    echo "🔧 管理命令:"
    echo "   - 查看状态: systemctl status rc-scheduler"
    echo "   - 查看日志: journalctl -u rc-scheduler -f"
    echo "   - 停止服务: sudo ./scheduler/stop_scheduler.sh"
    echo "   - 重启服务: sudo ./scheduler/restart_scheduler.sh"
    
else
    echo "❌ 调度器服务启动失败"
    echo ""
    echo "🔍 查看错误日志:"
    journalctl -u rc-scheduler --no-pager -l -n 20
    exit 1
fi

echo ""
echo "🎉 调度器服务启动完成!"
echo "================================"
