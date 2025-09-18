#!/bin/bash
# RC Project 服务重启脚本
# 重启所有服务

echo "🔄 RC Project 服务重启脚本"
echo "================================"

# 检查是否以root权限运行
if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

echo "📋 重启服务列表:"
echo "   - 所有RC Project服务"
echo ""

# 重启所有RC Project服务
echo "🔄 重启所有服务..."
systemctl restart rc-scheduler rc-webapp rc-frontend nginx

# 等待服务启动
sleep 5

# 检查服务状态
echo "🔍 检查服务状态..."
services=("rc-scheduler" "rc-webapp" "rc-frontend" "nginx")
all_running=true

for service in "${services[@]}"; do
    if systemctl is-active --quiet $service; then
        echo "   ✅ $service: 运行中"
    else
        echo "   ❌ $service: 未运行"
        all_running=false
    fi
done

echo ""
if [ "$all_running" = true ]; then
    echo "🎉 所有服务重启成功!"
    echo "================================"
    echo "🌐 访问地址:"
    echo "   - 前端应用: http://111.230.48.119:3000"
    echo "   - Web应用: http://111.230.48.119:8000"
    echo "   - API文档: http://111.230.48.119:8000/api/docs"
    echo "   - 调度器: http://111.230.48.119:8001"
    echo "   - 调度器文档: http://111.230.48.119:8001/scheduler-docs"
else
    echo "⚠️ 部分服务重启失败，请检查日志:"
    echo "   journalctl -u rc-scheduler -f"
    echo "   journalctl -u rc-webapp -f"
    echo "   journalctl -u rc-frontend -f"
fi
