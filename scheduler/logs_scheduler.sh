#!/bin/bash
# GuanHe Project 调度器日志查看脚本
# 查看调度器服务日志

APP_NAME="$(grep -s '^APP_NAME=' "$(cd "$(dirname "$0")/.." && pwd)/.env" 2>/dev/null | cut -d= -f2)"
APP_NAME="${APP_NAME:-GuanHe Project}"

echo "📝 $APP_NAME 调度器日志查看"
echo "================================"

# 检查是否以root权限运行
if [ "$EUID" -ne 0 ]; then
    echo "❌ 请以root权限运行此脚本: sudo $0"
    exit 1
fi

# 解析命令行参数
FOLLOW=false
LINES=50
SINCE=""

while [[ $# -gt 0 ]]; do
    case $1 in
        -f|--follow)
            FOLLOW=true
            shift
            ;;
        -n|--lines)
            LINES="$2"
            shift 2
            ;;
        -s|--since)
            SINCE="$2"
            shift 2
            ;;
        -h|--help)
            echo "用法: $0 [选项]"
            echo ""
            echo "选项:"
            echo "  -f, --follow     实时跟踪日志"
            echo "  -n, --lines N    显示最后N行日志 (默认: 50)"
            echo "  -s, --since TIME 显示指定时间之后的日志"
            echo "  -h, --help       显示此帮助信息"
            echo ""
            echo "示例:"
            echo "  $0                    # 显示最后50行日志"
            echo "  $0 -f                 # 实时跟踪日志"
            echo "  $0 -n 100             # 显示最后100行日志"
            echo "  $0 -s '1 hour ago'    # 显示1小时前的日志"
            echo "  $0 -f -n 20           # 实时跟踪，显示最后20行"
            exit 0
            ;;
        *)
            echo "未知选项: $1"
            echo "使用 -h 或 --help 查看帮助信息"
            exit 1
            ;;
    esac
done

echo "📋 调度器日志查看配置:"
echo "   - 跟踪模式: $([ "$FOLLOW" = true ] && echo "是" || echo "否")"
echo "   - 显示行数: $LINES"
echo "   - 时间范围: ${SINCE:-"全部"}"
echo ""

# 构建journalctl命令
JOURNALCTL_CMD="journalctl -u rc-scheduler --no-pager -l"

if [ "$FOLLOW" = true ]; then
    JOURNALCTL_CMD="$JOURNALCTL_CMD -f"
else
    JOURNALCTL_CMD="$JOURNALCTL_CMD -n $LINES"
fi

if [ ! -z "$SINCE" ]; then
    JOURNALCTL_CMD="$JOURNALCTL_CMD --since '$SINCE'"
fi

echo "🔍 执行命令: $JOURNALCTL_CMD"
echo ""

# 执行日志查看命令
if [ "$FOLLOW" = true ]; then
    echo "📺 实时跟踪调度器日志 (按 Ctrl+C 退出):"
    echo "================================"
    eval $JOURNALCTL_CMD
else
    echo "📋 调度器服务日志:"
    echo "================================"
    eval $JOURNALCTL_CMD
fi
