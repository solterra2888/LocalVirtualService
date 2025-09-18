#!/bin/bash
# RC Project 统一服务管理脚本
# 提供所有服务的统一管理接口

echo "🎛️ RC Project 统一服务管理"
echo "================================"

# 检查是否以root权限运行
if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

# 显示使用说明
show_usage() {
    echo "用法: $0 [命令] [选项]"
    echo ""
    echo "命令:"
    echo "  start          启动所有服务（不包括调度器）"
    echo "  stop           停止所有服务（不包括调度器）"
    echo "  restart        重启所有服务（不包括调度器）"
    echo "  status         查看所有服务状态"
    echo "  logs           查看服务日志"
    echo ""
    echo "调度器管理:"
    echo "  scheduler-start    启动调度器服务"
    echo "  scheduler-stop     停止调度器服务"
    echo "  scheduler-restart  重启调度器服务"
    echo "  scheduler-status   查看调度器状态"
    echo "  scheduler-logs     查看调度器日志"
    echo ""
    echo "选项:"
    echo "  -h, --help     显示此帮助信息"
    echo ""
    echo "示例:"
    echo "  $0 start                    # 启动所有服务"
    echo "  $0 scheduler-start          # 启动调度器"
    echo "  $0 status                   # 查看所有服务状态"
    echo "  $0 scheduler-logs -f        # 实时查看调度器日志"
}

# 检查服务状态
check_services() {
    echo "📊 服务状态概览:"
    echo "================================"
    
    # 主要服务
    echo "🌐 主要服务:"
    services=("postgresql" "rc-webapp" "rc-frontend" "nginx")
    for service in "${services[@]}"; do
        if systemctl is-active --quiet $service; then
            echo "   ✅ $service: 运行中"
        else
            echo "   ❌ $service: 未运行"
        fi
    done
    
    echo ""
    echo "🕒 调度器服务:"
    if systemctl is-active --quiet rc-scheduler; then
        echo "   ✅ rc-scheduler: 运行中"
    else
        echo "   ❌ rc-scheduler: 未运行"
    fi
    
    echo ""
    echo "🔗 端口占用:"
    ports=("3000:前端" "8000:Web应用" "8001:调度器" "5432:PostgreSQL")
    for port_info in "${ports[@]}"; do
        port=${port_info%:*}
        name=${port_info#*:}
        if lsof -t -i:$port >/dev/null 2>&1; then
            echo "   ✅ 端口 $port ($name): 已占用"
        else
            echo "   ❌ 端口 $port ($name): 未占用"
        fi
    done
}

# 查看服务日志
view_logs() {
    local service="$1"
    shift
    
    case $service in
        "webapp")
            echo "📝 Web应用日志:"
            journalctl -u rc-webapp -f "$@"
            ;;
        "frontend")
            echo "📝 前端应用日志:"
            journalctl -u rc-frontend -f "$@"
            ;;
        "scheduler")
            echo "📝 调度器日志:"
            journalctl -u rc-scheduler -f "$@"
            ;;
        "nginx")
            echo "📝 Nginx日志:"
            journalctl -u nginx -f "$@"
            ;;
        "all")
            echo "📝 所有服务日志:"
            journalctl -u rc-webapp -u rc-frontend -u rc-scheduler -u nginx -f "$@"
            ;;
        *)
            echo "❌ 未知的服务: $service"
            echo "可用服务: webapp, frontend, scheduler, nginx, all"
            exit 1
            ;;
    esac
}

# 主逻辑
case "$1" in
    "start")
        echo "🚀 启动所有服务..."
        ./start_services.sh
        ;;
    "stop")
        echo "🛑 停止所有服务..."
        ./stop_services.sh
        ;;
    "restart")
        echo "🔄 重启所有服务..."
        ./restart_services.sh
        ;;
    "status")
        check_services
        ;;
    "logs")
        if [ -z "$2" ]; then
            echo "❌ 请指定要查看日志的服务"
            echo "用法: $0 logs [webapp|frontend|scheduler|nginx|all] [选项]"
            exit 1
        fi
        shift
        view_logs "$@"
        ;;
    "scheduler-start")
        echo "🕒 启动调度器服务..."
        ./scheduler/start_scheduler.sh
        ;;
    "scheduler-stop")
        echo "🛑 停止调度器服务..."
        ./scheduler/stop_scheduler.sh
        ;;
    "scheduler-restart")
        echo "🔄 重启调度器服务..."
        ./scheduler/restart_scheduler.sh
        ;;
    "scheduler-status")
        echo "📊 查看调度器状态..."
        ./scheduler/status_scheduler.sh
        ;;
    "scheduler-logs")
        shift
        echo "📝 查看调度器日志..."
        ./scheduler/logs_scheduler.sh "$@"
        ;;
    "-h"|"--help"|"help")
        show_usage
        ;;
    "")
        echo "❌ 请指定命令"
        echo ""
        show_usage
        exit 1
        ;;
    *)
        echo "❌ 未知命令: $1"
        echo ""
        show_usage
        exit 1
        ;;
esac
