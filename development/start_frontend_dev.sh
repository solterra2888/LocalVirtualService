#!/bin/bash
# RC Project 前端应用本地开发启动脚本
# 用于本地开发环境

set -e

# 脚本目录和项目根目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
FRONTEND_DIR="$PROJECT_ROOT/web-frontend"

echo "🎨 RC Project - 前端应用开发模式启动"
echo "=========================================="
echo "📂 项目根目录: $PROJECT_ROOT"
echo "📂 前端目录: $FRONTEND_DIR"
echo ""

# 检查前端目录是否存在
if [ ! -d "$FRONTEND_DIR" ]; then
    echo "❌ 前端目录不存在: $FRONTEND_DIR"
    exit 1
fi

# 加载开发环境变量
ENV_FILE="$SCRIPT_DIR/env.development"
if [ -f "$ENV_FILE" ]; then
    echo "📝 加载开发环境配置: $ENV_FILE"
    set -a  # 自动导出变量
    source "$ENV_FILE"
    set +a  # 关闭自动导出
    echo "✅ 环境变量加载完成"
else
    echo "⚠️  环境配置文件不存在: $ENV_FILE"
    echo "   使用默认配置"
fi

# 检查Node.js和npm
echo ""
echo "🔍 检查前端环境..."
if ! command -v node &> /dev/null; then
    echo "❌ Node.js 未安装"
    exit 1
fi

if ! command -v npm &> /dev/null; then
    echo "❌ npm 未安装"
    exit 1
fi

echo "✅ Node.js: $(node --version)"
echo "✅ npm: $(npm --version)"

# 进入前端目录
cd "$FRONTEND_DIR"

# 检查package.json
if [ ! -f "package.json" ]; then
    echo "❌ package.json 不存在"
    exit 1
fi

# 安装依赖 (如果node_modules不存在)
if [ ! -d "node_modules" ]; then
    echo ""
    echo "📦 安装前端依赖..."
    npm install
    echo "✅ 依赖安装完成"
fi

# 显示启动配置
echo ""
echo "🚀 前端启动配置:"
echo "   - API地址: ${REACT_APP_API_BASE_URL:-http://localhost:8000/api}"
echo "   - 调度器地址: ${REACT_APP_SCHEDULER_BASE_URL:-http://localhost:8001/api}"
echo "   - 开发服务器: http://localhost:3000"
echo "   - 自动打开浏览器: ${BROWSER:-default}"
echo ""

# 启动前端开发服务器
echo "🎨 启动前端开发服务器..."
echo "   按 Ctrl+C 停止服务"
echo ""

# 设置前端特定的环境变量
export PORT=3000
export BROWSER=${BROWSER:-none}
export REACT_APP_API_BASE_URL=${REACT_APP_API_BASE_URL:-http://localhost:8000/api}
export REACT_APP_SCHEDULER_BASE_URL=${REACT_APP_SCHEDULER_BASE_URL:-http://localhost:8001/api}

# 启动React开发服务器
npm start
