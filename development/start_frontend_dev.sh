#!/bin/bash
# 前端应用本地开发启动脚本（React dev server）

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/common.sh"

dev_load_env

FRONTEND_DIR="$PROJECT_ROOT/web-frontend"

echo -e "${BLUE}🎨 前端应用开发模式启动${NC}"
echo "=========================================="
echo -e "${CYAN}📂 前端目录: $FRONTEND_DIR${NC}"
echo ""

if [ ! -d "$FRONTEND_DIR" ]; then
    echo -e "${RED}❌ 前端目录不存在: $FRONTEND_DIR${NC}"
    exit 1
fi

# 检查 Node.js
echo -e "${BLUE}🔍 检查前端环境...${NC}"
if ! command -v node &>/dev/null; then
    echo -e "${RED}❌ Node.js 未安装${NC}"
    exit 1
fi
if ! command -v npm &>/dev/null; then
    echo -e "${RED}❌ npm 未安装${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Node.js: $(node --version), npm: $(npm --version)${NC}"

cd "$FRONTEND_DIR"

if [ ! -f "package.json" ]; then
    echo -e "${RED}❌ package.json 不存在${NC}"
    exit 1
fi

# 自动安装依赖
if [ ! -d "node_modules" ]; then
    echo ""
    echo -e "${BLUE}📦 安装前端依赖...${NC}"
    npm install
    echo -e "${GREEN}✅ 依赖安装完成${NC}"
fi

export PORT=3000
export BROWSER=${BROWSER:-none}
export REACT_APP_API_BASE_URL=${REACT_APP_API_BASE_URL:-http://localhost:8000/api}
export REACT_APP_SCHEDULER_BASE_URL=${REACT_APP_SCHEDULER_BASE_URL:-http://localhost:8001/api}

echo ""
echo -e "${CYAN}🚀 启动配置:${NC}"
echo "   - 开发服务器: http://localhost:3000"
echo "   - API 地址: $REACT_APP_API_BASE_URL"
echo "   - 调度器: $REACT_APP_SCHEDULER_BASE_URL"
echo ""
echo "   按 Ctrl+C 停止服务"
echo ""

npm start
