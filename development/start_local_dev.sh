#!/bin/bash
# RC Project 本地开发环境统一启动脚本
# 按正确顺序启动所有服务：调度器 → Web应用 → 前端应用

set -e

# 脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 RC Project - 本地开发环境启动脚本${NC}"
echo "=============================================="
echo -e "${CYAN}📂 项目根目录: $PROJECT_ROOT${NC}"
echo -e "${CYAN}📂 开发脚本目录: $SCRIPT_DIR${NC}"
echo ""

# 检查是否有其他实例在运行
echo -e "${YELLOW}🔍 检查现有服务进程...${NC}"

# 检查端口占用
check_port() {
    local port=$1
    local service_name=$2
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo -e "${YELLOW}⚠️  端口 $port 已被占用 ($service_name)${NC}"
        echo "   如需停止现有服务，请运行: ./development/stop_local_dev.sh"
        return 1
    fi
    return 0
}

# 检查关键端口
if ! check_port 8001 "调度器"; then
    echo -e "${RED}❌ 调度器端口已占用${NC}"
fi

if ! check_port 8000 "Web应用"; then
    echo -e "${RED}❌ Web应用端口已占用${NC}"
fi

if ! check_port 3000 "前端应用"; then
    echo -e "${RED}❌ 前端应用端口已占用${NC}"
fi

# 询问是否继续
echo ""
read -p "是否继续启动开发环境？(y/N): " -n 1 -r
echo ""
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}启动已取消${NC}"
    exit 0
fi

echo ""
echo -e "${GREEN}📋 启动顺序:${NC}"
echo "   1. 🕒 调度器服务 (localhost:8001)"
echo "   2. 🌐 Web应用 (localhost:8000)" 
echo "   3. 🎨 前端应用 (localhost:3000)"
echo ""

# 创建日志目录
LOGS_DIR="$PROJECT_ROOT/development/logs"
mkdir -p "$LOGS_DIR"

# 启动调度器服务
echo -e "${BLUE}========================================${NC}"
echo -e "${PURPLE}🕒 步骤 1/3: 启动调度器服务${NC}"
echo -e "${BLUE}========================================${NC}"

# 在后台启动调度器
nohup bash "$SCRIPT_DIR/start_scheduler_dev.sh" > "$LOGS_DIR/scheduler.log" 2>&1 &
SCHEDULER_PID=$!
echo "调度器进程ID: $SCHEDULER_PID"

# 等待调度器启动
echo "等待调度器服务启动..."
for i in {1..30}; do
    if curl -s http://localhost:8001/api/health >/dev/null 2>&1; then
        echo -e "${GREEN}✅ 调度器服务已启动 (http://localhost:8001)${NC}"
        break
    fi
    sleep 2
    echo -n "."
done

if ! curl -s http://localhost:8001/api/health >/dev/null 2>&1; then
    echo -e "${RED}❌ 调度器服务启动失败${NC}"
    echo "查看日志: tail -f $LOGS_DIR/scheduler.log"
    exit 1
fi

sleep 3

# 启动Web应用
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${PURPLE}🌐 步骤 2/3: 启动Web应用${NC}"
echo -e "${BLUE}========================================${NC}"

# 在后台启动Web应用
nohup bash "$SCRIPT_DIR/start_webapp_dev.sh" > "$LOGS_DIR/webapp.log" 2>&1 &
WEBAPP_PID=$!
echo "Web应用进程ID: $WEBAPP_PID"

# 等待Web应用启动
echo "等待Web应用启动..."
for i in {1..30}; do
    if curl -s http://localhost:8000/health >/dev/null 2>&1; then
        echo -e "${GREEN}✅ Web应用已启动 (http://localhost:8000)${NC}"
        break
    fi
    sleep 2
    echo -n "."
done

if ! curl -s http://localhost:8000/health >/dev/null 2>&1; then
    echo -e "${RED}❌ Web应用启动失败${NC}"
    echo "查看日志: tail -f $LOGS_DIR/webapp.log"
    exit 1
fi

sleep 3

# 启动前端应用
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${PURPLE}🎨 步骤 3/3: 启动前端应用${NC}"
echo -e "${BLUE}========================================${NC}"

# 在后台启动前端应用
nohup bash "$SCRIPT_DIR/start_frontend_dev.sh" > "$LOGS_DIR/frontend.log" 2>&1 &
FRONTEND_PID=$!
echo "前端应用进程ID: $FRONTEND_PID"

# 等待前端应用启动
echo "等待前端应用启动..."
for i in {1..60}; do
    if curl -s http://localhost:3000 >/dev/null 2>&1; then
        echo -e "${GREEN}✅ 前端应用已启动 (http://localhost:3000)${NC}"
        break
    fi
    sleep 2
    echo -n "."
done

# 保存进程ID到文件
PID_FILE="$SCRIPT_DIR/.dev_pids"
echo "SCHEDULER_PID=$SCHEDULER_PID" > "$PID_FILE"
echo "WEBAPP_PID=$WEBAPP_PID" >> "$PID_FILE"
echo "FRONTEND_PID=$FRONTEND_PID" >> "$PID_FILE"

# 启动完成总结
echo ""
echo -e "${GREEN}🎉 所有服务启动完成!${NC}"
echo "=============================================="
echo -e "${CYAN}🌐 访问地址:${NC}"
echo "   - 前端应用: http://localhost:3000"
echo "   - Web应用: http://localhost:8000"
echo "   - API文档: http://localhost:8000/docs"
echo "   - 调度器: http://localhost:8001"
echo "   - 调度器文档: http://localhost:8001/scheduler-docs"
echo ""
echo -e "${CYAN}📊 服务状态:${NC}"
echo "   - 调度器: 进程 $SCHEDULER_PID"
echo "   - Web应用: 进程 $WEBAPP_PID"
echo "   - 前端应用: 进程 $FRONTEND_PID"
echo ""
echo -e "${CYAN}📝 日志文件:${NC}"
echo "   - 调度器日志: $LOGS_DIR/scheduler.log"
echo "   - Web应用日志: $LOGS_DIR/webapp.log"
echo "   - 前端应用日志: $LOGS_DIR/frontend.log"
echo ""
echo -e "${YELLOW}⚠️  停止服务: ./development/stop_local_dev.sh${NC}"
echo "=============================================="
