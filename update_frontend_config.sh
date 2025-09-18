#!/bin/bash
# 更新前端服务配置并重启的脚本
# 需要root权限运行

echo "🔧 更新前端服务配置..."
echo "================================"

# 检查是否以root权限运行
if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

echo "1. 复制更新的配置文件到系统目录..."
cp /home/kevin/rcproject/deployment/rc-frontend.service /etc/systemd/system/rc-frontend.service
if [ $? -eq 0 ]; then
    echo "✅ 配置文件已更新"
else
    echo "❌ 配置文件更新失败"
    exit 1
fi

echo "2. 重新加载systemd配置..."
systemctl daemon-reload
if [ $? -eq 0 ]; then
    echo "✅ systemd配置已重新加载"
else
    echo "❌ systemd配置重新加载失败"
    exit 1
fi

echo "3. 重启前端服务..."
systemctl restart rc-frontend
if [ $? -eq 0 ]; then
    echo "✅ 前端服务已重启"
else
    echo "❌ 前端服务重启失败"
    exit 1
fi

echo "4. 等待服务完全启动..."
sleep 15

echo "5. 检查服务状态..."
systemctl status rc-frontend --no-pager -l

echo ""
echo "6. 验证环境变量..."
PID=$(pgrep -f "react-scripts start" | head -1)
if [ ! -z "$PID" ]; then
    echo "当前环境变量:"
    cat /proc/$PID/environ | tr '\0' '\n' | grep REACT_APP
else
    echo "⚠️ 未找到React进程，服务可能还在启动中"
fi

echo ""
echo "🎉 前端服务配置更新完成!"
echo "================================"
echo "现在前端应该使用正确的API地址了："
echo "   API: http://111.230.48.119/api"
echo "   Scheduler: http://111.230.48.119/scheduler"
echo ""
echo "🌐 请在其他电脑上重新尝试登录："
echo "   访问: http://111.230.48.119"
