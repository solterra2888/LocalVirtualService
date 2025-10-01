#!/bin/bash
# RC Project 本地开发环境停止脚本
# 停止所有本地开发服务

set -e

# 脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${RED}🛑 RC Project - 本地开发环境停止脚本${NC}"
echo "=============================================="
echo ""

# PID文件路径
PID_FILE="$SCRIPT_DIR/.dev_pids"

# 停止指定PID的进程
stop_process() {
    local pid=$1
    local service_name=$2
    
    if [ -n "$pid" ]; then
        if ps -p $pid > /dev/null 2>&1; then
            echo -e "${YELLOW}🔄 停止 $service_name (PID: $pid)...${NC}"
            kill $pid 2>/dev/null || true
            
            # 等待进程结束
            for i in {1..10}; do
                if ! ps -p $pid > /dev/null 2>&1; then
                    echo -e "${GREEN}✅ $service_name 已停止${NC}"
                    return 0
                fi
                sleep 1
            done
            
            # 如果进程仍然存在，强制杀死
            if ps -p $pid > /dev/null 2>&1; then
                echo -e "${YELLOW}⚠️  强制停止 $service_name...${NC}"
                kill -9 $pid 2>/dev/null || true
                echo -e "${GREEN}✅ $service_name 已强制停止${NC}"
            fi
        else
            echo -e "${CYAN}ℹ️  $service_name (PID: $pid) 已经停止${NC}"
        fi
    fi
}

# 从PID文件读取进程ID并停止
if [ -f "$PID_FILE" ]; then
    echo -e "${BLUE}📖 从PID文件读取进程信息...${NC}"
    source "$PID_FILE"
    
    # 按逆序停止服务（前端 → Web应用）
    stop_process "$FRONTEND_PID" "前端应用"
    stop_process "$WEBAPP_PID" "Web应用"
    
    # 删除PID文件
    rm -f "$PID_FILE"
    echo -e "${GREEN}✅ PID文件已清理${NC}"
else
    echo -e "${YELLOW}⚠️  PID文件不存在，尝试通过端口查找进程...${NC}"
fi

# 通过端口查找并停止进程
stop_process_by_port() {
    local port=$1
    local service_name=$2
    
    local pid=$(lsof -ti:$port 2>/dev/null)
    if [ -n "$pid" ]; then
        echo -e "${YELLOW}🔍 发现端口 $port 上的进程 ($service_name): $pid${NC}"
        stop_process "$pid" "$service_name"
    else
        echo -e "${CYAN}ℹ️  端口 $port ($service_name) 没有活动进程${NC}"
    fi
}

echo ""
echo -e "${BLUE}🔍 检查并停止端口上的进程...${NC}"

# 停止各个端口上的进程
stop_process_by_port 3000 "前端应用"
stop_process_by_port 8000 "Web应用"
stop_process_by_port 5555 "Flower监控"

# 清理可能的Node.js进程
echo ""
echo -e "${BLUE}🧹 清理相关进程...${NC}"

# 查找并停止相关的Node.js进程
NODE_PIDS=$(pgrep -f "npm start" 2>/dev/null || true)
if [ -n "$NODE_PIDS" ]; then
    echo -e "${YELLOW}🔄 停止相关Node.js进程...${NC}"
    echo "$NODE_PIDS" | xargs kill 2>/dev/null || true
    sleep 2
    # 强制停止仍在运行的进程
    NODE_PIDS=$(pgrep -f "npm start" 2>/dev/null || true)
    if [ -n "$NODE_PIDS" ]; then
        echo "$NODE_PIDS" | xargs kill -9 2>/dev/null || true
    fi
    echo -e "${GREEN}✅ Node.js进程已清理${NC}"
fi

# 查找并停止相关的Python进程（仅限Web应用，不包含调度器）
PYTHON_PIDS=$(pgrep -f "uvicorn.*main:app" 2>/dev/null || true)
if [ -n "$PYTHON_PIDS" ]; then
    echo -e "${YELLOW}🔄 停止相关Python进程...${NC}"
    echo "$PYTHON_PIDS" | xargs kill 2>/dev/null || true
    sleep 2
    # 强制停止仍在运行的进程
    PYTHON_PIDS=$(pgrep -f "uvicorn.*main:app" 2>/dev/null || true)
    if [ -n "$PYTHON_PIDS" ]; then
        echo "$PYTHON_PIDS" | xargs kill -9 2>/dev/null || true
    fi
    echo -e "${GREEN}✅ Python进程已清理${NC}"
fi

# 查找并停止Celery Worker进程
CELERY_PIDS=$(pgrep -f "celery.*worker" 2>/dev/null || true)
if [ -n "$CELERY_PIDS" ]; then
    echo -e "${YELLOW}🔄 停止Celery Worker进程...${NC}"
    echo "$CELERY_PIDS" | xargs kill 2>/dev/null || true
    sleep 2
    # 强制停止仍在运行的进程
    CELERY_PIDS=$(pgrep -f "celery.*worker" 2>/dev/null || true)
    if [ -n "$CELERY_PIDS" ]; then
        echo "$CELERY_PIDS" | xargs kill -9 2>/dev/null || true
    fi
    echo -e "${GREEN}✅ Celery Worker进程已清理${NC}"
fi

# 查找并停止Flower监控进程
FLOWER_PIDS=$(pgrep -f "celery.*flower" 2>/dev/null || true)
if [ -n "$FLOWER_PIDS" ]; then
    echo -e "${YELLOW}🔄 停止Flower监控进程...${NC}"
    echo "$FLOWER_PIDS" | xargs kill 2>/dev/null || true
    sleep 2
    # 强制停止仍在运行的进程
    FLOWER_PIDS=$(pgrep -f "celery.*flower" 2>/dev/null || true)
    if [ -n "$FLOWER_PIDS" ]; then
        echo "$FLOWER_PIDS" | xargs kill -9 2>/dev/null || true
    fi
    echo -e "${GREEN}✅ Flower监控进程已清理${NC}"
fi

# 清理日志文件（可选）
LOGS_DIR="$SCRIPT_DIR/logs"
if [ -d "$LOGS_DIR" ]; then
    echo ""
    read -p "是否清理日志文件？(y/N): " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        rm -f "$LOGS_DIR"/*.log
        echo -e "${GREEN}✅ 日志文件已清理${NC}"
    fi
fi

# 最终检查
echo ""
echo -e "${BLUE}🔍 最终检查端口状态...${NC}"

check_port_status() {
    local port=$1
    local service_name=$2
    
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo -e "${RED}❌ 端口 $port ($service_name) 仍有进程在运行${NC}"
        return 1
    else
        echo -e "${GREEN}✅ 端口 $port ($service_name) 已释放${NC}"
        return 0
    fi
}

check_port_status 3000 "前端应用"
check_port_status 8000 "Web应用"
check_port_status 5555 "Flower监控"

echo ""
echo -e "${GREEN}🎉 开发服务已停止!${NC}"
echo "=============================================="
echo -e "${CYAN}💡 提示:${NC}"
echo "   - 重新启动: ./development/start_local_dev.sh"
echo "   - 查看状态: ps aux | grep -E 'uvicorn.*main:app|npm'"
echo "   - 调度器独立运行，不受此脚本管理"
echo "=============================================="
