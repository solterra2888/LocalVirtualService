#!/bin/bash
# RC Project 本地开发环境统一启动脚本
# 按正确顺序启动所有服务：检查调度器 → Web应用 → 前端应用

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
if ! check_port 8000 "Web应用"; then
    echo -e "${RED}❌ Web应用端口已占用${NC}"
fi

if ! check_port 3000 "前端应用"; then
    echo -e "${RED}❌ 前端应用端口已占用${NC}"
fi

if ! check_port 6379 "Redis服务"; then
    echo -e "${RED}❌ Redis端口已占用${NC}"
fi

if ! check_port 5555 "Flower监控"; then
    echo -e "${RED}❌ Flower监控端口已占用${NC}"
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
echo "   1. 🔍 检查调度器状态"
echo "   2. 🌐 Web应用 (localhost:8000)" 
echo "   3. 🎨 前端应用 (localhost:3000)"
echo ""
echo -e "${CYAN}💡 提示: 如需独立测试调度器，请运行:${NC}"
echo "   ./development/start_scheduler_dev.sh"
echo ""

# 创建日志目录
LOGS_DIR="$PROJECT_ROOT/development/logs"
mkdir -p "$LOGS_DIR"

# 检查调度器状态
echo -e "${BLUE}========================================${NC}"
echo -e "${PURPLE}🔍 步骤 1/3: 检查调度器状态${NC}"
echo -e "${BLUE}========================================${NC}"

echo "正在检查调度器组件状态..."

# 检查调度器模块导入
echo "🔍 测试调度器模块导入..."
if /Users/kevinliu/anaconda3/envs/rcTool/bin/python -c "
import sys
sys.path.insert(0, '$PROJECT_ROOT')
try:
    from config.settings import config
    from backend.database.db_manager import DatabaseManager
    from backend.services.scheduler_service import SchedulerService
    from backend.api.scheduler import router
    import scheduler_main
    print('✅ 所有核心模块导入成功')
except Exception as e:
    print(f'❌ 模块导入失败: {str(e)}')
    exit(1)
" 2>/dev/null; then
    echo -e "${GREEN}✅ 调度器模块导入正常${NC}"
else
    echo -e "${RED}❌ 调度器模块导入失败${NC}"
    echo "请检查Python环境和依赖包"
    exit 1
fi

# 检查数据库连接
echo "🔍 测试数据库连接..."
if /Users/kevinliu/anaconda3/envs/rcTool/bin/python -c "
import sys
import asyncio
sys.path.insert(0, '$PROJECT_ROOT')

async def test_db():
    try:
        from backend.database.db_manager import DatabaseManager
        from config.settings import config
        
        db_config = {
            'db_host': config.get('DB_HOST', 'localhost'),
            'db_port': config.get('DB_PORT', '5432'),
            'db_name': config.get('DB_NAME', 'rc_database'),
            'db_user': config.get('DB_USER', 'postgres'),
            'db_password': config.get('DB_PASSWORD', '')
        }
        
        db_manager = DatabaseManager(db_config)
        if db_manager.ensure_schema():
            print('✅ 数据库连接和架构检查成功')
        else:
            print('❌ 数据库连接失败')
            exit(1)
    except Exception as e:
        print(f'❌ 数据库连接失败: {str(e)}')
        exit(1)

asyncio.run(test_db())
" 2>/dev/null; then
    echo -e "${GREEN}✅ 数据库连接正常${NC}"
else
    echo -e "${RED}❌ 数据库连接失败${NC}"
    echo "请检查数据库配置和网络连接"
    exit 1
fi

# 检查调度器服务功能
echo "🔍 测试调度器服务功能..."
if /Users/kevinliu/anaconda3/envs/rcTool/bin/python -c "
import sys
import asyncio
sys.path.insert(0, '$PROJECT_ROOT')

async def test_scheduler():
    try:
        from backend.services.scheduler_service import get_scheduler_service
        
        scheduler_service = await get_scheduler_service()
        tasks = await scheduler_service.get_tasks()
        enabled_tasks = len([t for t in tasks if t.get('enabled', False)])
        
        print(f'✅ 调度器服务正常 - 总任务: {len(tasks)}, 启用: {enabled_tasks}')
        
        await scheduler_service.stop()
    except Exception as e:
        print(f'❌ 调度器服务测试失败: {str(e)}')
        exit(1)

asyncio.run(test_scheduler())
" 2>/dev/null; then
    echo -e "${GREEN}✅ 调度器服务功能正常${NC}"
else
    echo -e "${RED}❌ 调度器服务功能异常${NC}"
    echo "请检查调度器配置"
    exit 1
fi

# 检查远程调度器服务状态
echo "🔍 检查远程调度器服务状态..."
SCHEDULER_URL="http://111.230.48.119:8001"
echo "   调度器服务地址: $SCHEDULER_URL"

# 检查调度器健康状态
if curl -s --connect-timeout 10 --max-time 30 "$SCHEDULER_URL/api/health" >/dev/null 2>&1; then
    echo -e "${GREEN}✅ 远程调度器服务正在运行${NC}"
    
    # 获取详细状态信息
    HEALTH_RESPONSE=$(curl -s --connect-timeout 10 --max-time 30 "$SCHEDULER_URL/api/health" 2>/dev/null)
    if [ $? -eq 0 ] && [ -n "$HEALTH_RESPONSE" ]; then
        echo "   调度器状态详情:"
        echo "$HEALTH_RESPONSE" | /Users/kevinliu/anaconda3/envs/rcTool/bin/python -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(f'   - 运行状态: {\"运行中\" if data.get(\"scheduler_running\") else \"已停止\"}')
    print(f'   - 总任务数: {data.get(\"total_tasks\", 0)}')
    print(f'   - 启用任务数: {data.get(\"enabled_tasks\", 0)}')
except:
    print('   - 无法解析状态信息')
" 2>/dev/null || echo "   - 无法获取详细状态"
    fi
else
    echo -e "${YELLOW}⚠️  远程调度器服务未运行或无法访问${NC}"
    echo "   请检查远程服务器上的调度器服务状态"
    echo "   启动命令: systemctl start rc-scheduler"
    echo "   检查状态: systemctl status rc-scheduler"
    echo "   注意: 调度器服务不可用不会影响Web应用和前端应用的启动"
fi

echo -e "${GREEN}🎉 调度器状态检查完成，所有组件正常工作！${NC}"
echo "   调度器不需要启动，系统将使用独立的调度器服务"
echo ""

# 检查Redis服务
echo ""
echo -e "${BLUE}========================================${NC}"
echo -e "${PURPLE}🔍 步骤 1.5/4: 检查Redis服务${NC}"
echo -e "${BLUE}========================================${NC}"

echo "正在检查Redis服务状态..."

# 检查Redis连接
echo "🔍 测试Redis连接..."
if /Users/kevinliu/anaconda3/envs/rcTool/bin/python -c "
import redis
try:
    r = redis.Redis(host='localhost', port=6379, db=0)
    result = r.ping()
    if result:
        print('✅ Redis连接成功')
    else:
        print('❌ Redis ping失败')
        exit(1)
except Exception as e:
    print(f'❌ Redis连接失败: {e}')
    exit(1)
" 2>/dev/null; then
    echo -e "${GREEN}✅ Redis服务正常${NC}"
else
    echo -e "${RED}❌ Redis服务不可用${NC}"
    echo "请先启动Redis服务:"
    echo "   brew services start redis"
    echo "   或: redis-server"
    echo ""
    read -p "是否继续启动其他服务？(y/N): " -n 1 -r
    echo ""
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}启动已取消${NC}"
        exit 0
    fi
fi

# 检查Celery配置
echo "🔍 测试Celery配置..."
if /Users/kevinliu/anaconda3/envs/rcTool/bin/python -c "
import sys
sys.path.insert(0, '$PROJECT_ROOT')
try:
    from backend.celery_app import app as celery_app
    from backend.celery_config import broker_url, result_backend
    print(f'✅ Celery配置正常 - Broker: {broker_url}')
except Exception as e:
    print(f'❌ Celery配置错误: {e}')
    exit(1)
" 2>/dev/null; then
    echo -e "${GREEN}✅ Celery配置正常${NC}"
else
    echo -e "${RED}❌ Celery配置错误${NC}"
    echo "请检查Celery相关依赖是否已安装"
fi

echo -e "${GREEN}🎉 Redis和Celery检查完成！${NC}"
echo "   如需启动Celery Worker，请运行: ./development/start_celery_worker_dev.sh"
echo "   如需启动Flower监控，请运行: ./development/start_flower_dev.sh"
echo ""

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
echo "WEBAPP_PID=$WEBAPP_PID" > "$PID_FILE"
echo "FRONTEND_PID=$FRONTEND_PID" >> "$PID_FILE"

# 启动完成总结
echo ""
echo -e "${GREEN}🎉 所有服务启动完成!${NC}"
echo "=============================================="
echo -e "${CYAN}🌐 访问地址:${NC}"
echo "   - 前端应用: http://localhost:3000"
echo "   - Web应用: http://localhost:8000"
echo "   - API文档: http://localhost:8000/docs"
echo ""
echo -e "${CYAN}📊 服务状态:${NC}"
echo "   - 调度器: ✅ 已检查，组件正常"
echo "   - Web应用: 进程 $WEBAPP_PID"
echo "   - 前端应用: 进程 $FRONTEND_PID"
echo ""
echo -e "${CYAN}📝 日志文件:${NC}"
echo "   - Web应用日志: $LOGS_DIR/webapp.log"
echo "   - 前端应用日志: $LOGS_DIR/frontend.log"
echo ""
echo -e "${YELLOW}⚠️  停止服务: ./development/stop_local_dev.sh${NC}"
echo -e "${YELLOW}💡 调度器独立运行，无需通过此脚本管理${NC}"
echo "=============================================="
