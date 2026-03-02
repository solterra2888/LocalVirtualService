#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
独立调度器服务主程序
专门负责定时任务调度和执行，与主Web应用分离
"""

import os
import sys
import asyncio
import uvicorn
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from backend.services.scheduler_service import get_scheduler_service, shutdown_scheduler_service
from backend.utils.logger import init_logging, get_logger, info, error, warning
from backend.database.db_manager import DatabaseManager
from config.settings import config

logger = get_logger("scheduler_main")

@asynccontextmanager
async def scheduler_lifespan(app: FastAPI):
    """调度器服务生命周期管理"""
    # 启动时初始化
    try:
        # 初始化日志系统
        logging_config = config.get_logging_config()
        init_logging(**logging_config)
        info("🕒 独立调度器服务启动中...", source="scheduler_startup")
    except Exception as e:
        print(f"⚠️ 调度器日志系统初始化失败: {str(e)}")
    
    # 启动时初始化数据库连接
    try:
        db_conn_params = {
            'db_host': config.get('DB_HOST', 'localhost'),
            'db_port': config.get('DB_PORT', '5432'),
            'db_name': config.get('DB_NAME', ''),
            'db_user': config.get('DB_USER', ''),
            'db_password': config.get('DB_PASSWORD', '')
        }
        
        db_manager = DatabaseManager(db_conn_params)
        if db_manager.ensure_schema():
            info("✅ 调度器数据库连接初始化完成", source="scheduler_startup")
        else:
            warning("⚠️ 调度器数据库初始化失败，但服务仍可继续运行", source="scheduler_startup")
    except Exception as e:
        error(f"⚠️ 调度器数据库初始化出错: {str(e)}", source="scheduler_startup")
    
    # 启动调度器服务
    try:
        scheduler_service = await get_scheduler_service()
        info("✅ 独立调度器服务启动完成", source="scheduler_startup")
        info("🔄 开始执行定时任务调度...", source="scheduler_startup")
    except Exception as e:
        error(f"⚠️ 调度器服务启动失败: {str(e)}", source="scheduler_startup")
    
    yield
    
    # 关闭时清理资源
    info("🔄 调度器服务关闭，清理资源...", source="scheduler_shutdown")
    
    try:
        await shutdown_scheduler_service()
        info("✅ 调度器服务已关闭", source="scheduler_shutdown")
    except Exception as e:
        error(f"⚠️ 调度器服务关闭失败: {str(e)}", source="scheduler_shutdown")

# 创建独立的调度器FastAPI应用
scheduler_app = FastAPI(
    title=f"{config.APP_NAME} - Scheduler Service",
    description="独立的定时任务调度器服务",
    version=config.APP_VERSION,
    lifespan=scheduler_lifespan,
    docs_url="/scheduler-docs",
    redoc_url="/scheduler-redoc"
)

# 配置CORS - 允许主Web应用访问
scheduler_app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        f"http://localhost:{config.FRONTEND_PORT}",
        f"http://localhost:{config.WEBAPP_PORT}",
        f"http://{config.SERVER_HOST}:{config.FRONTEND_PORT}",
        f"http://{config.SERVER_HOST}:{config.WEBAPP_PORT}",
        f"http://{config.SERVER_HOST}",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 导入调度器相关的API路由
from backend.api.scheduler import router as scheduler_router

# 注册调度器API路由
scheduler_app.include_router(scheduler_router, prefix="/api")

@scheduler_app.get("/", summary="调度器服务根路径")
async def scheduler_root():
    """调度器服务根路径"""
    return {
        "service": f"{config.APP_NAME} Scheduler Service",
        "version": config.APP_VERSION,
        "status": "running",
        "description": "独立的定时任务调度器服务",
        "docs": "/scheduler-docs"
    }

@scheduler_app.get("/api/health", summary="调度器服务健康检查")
async def scheduler_health():
    """调度器服务健康检查"""
    try:
        # 检查调度器服务状态
        scheduler_service = await get_scheduler_service()
        is_running = scheduler_service.scheduler.running
        
        # 获取任务统计
        tasks = await scheduler_service.get_tasks()
        enabled_tasks = len([t for t in tasks if t.get('enabled', False)])
        
        return {
            "status": "healthy",
            "service": "scheduler",
            "scheduler_running": is_running,
            "total_tasks": len(tasks),
            "enabled_tasks": enabled_tasks,
            "timestamp": asyncio.get_event_loop().time()
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"调度器服务不健康: {str(e)}")

@scheduler_app.get("/api/status", summary="调度器详细状态")
async def scheduler_detailed_status():
    """获取调度器详细状态信息"""
    try:
        scheduler_service = await get_scheduler_service()
        
        # 获取调度器基本信息
        scheduler_info = {
            "running": scheduler_service.scheduler.running,
            "timezone": str(scheduler_service.scheduler.timezone),
            "job_defaults": getattr(scheduler_service.scheduler, 'job_defaults', {})
        }
        
        # 获取所有任务
        tasks = await scheduler_service.get_tasks()
        
        # 统计信息
        stats = {
            "total_tasks": len(tasks),
            "enabled_tasks": len([t for t in tasks if t.get('enabled', False)]),
            "disabled_tasks": len([t for t in tasks if not t.get('enabled', False)]),
            "task_types": {}
        }
        
        # 按类型统计任务
        for task in tasks:
            task_type = task.get('type', 'unknown')
            if task_type not in stats["task_types"]:
                stats["task_types"][task_type] = 0
            stats["task_types"][task_type] += 1
        
        return {
            "service": "scheduler",
            "scheduler_info": scheduler_info,
            "statistics": stats,
            "tasks": tasks
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取调度器状态失败: {str(e)}")

def main():
    """调度器服务主函数"""
    print("=" * 70)
    print(f"🕒 {config.APP_NAME} - 独立调度器服务")
    print("=" * 70)
    print("📌 这是专门的定时任务调度器服务:")
    print("   - 负责所有定时任务的调度和执行")
    print("   - 独立运行，不影响主Web应用性能")
    print("   - 通过API与主应用通信")
    print()
    
    # 获取配置
    host = "0.0.0.0"
    port = config.SCHEDULER_PORT
    
    print(f"🚀 调度器服务启动配置:")
    print(f"   - 服务地址: http://{host}:{port}")
    print(f"   - 数据库地址: {config.get('DB_HOST')}:{config.get('DB_PORT')}")
    print(f"   - 数据库名称: {config.get('DB_NAME')}")
    print(f"   - API文档: http://{host}:{port}/scheduler-docs")
    print()
    
    try:
        # 启动调度器服务
        uvicorn.run(
            "scheduler_main:scheduler_app",
            host=host,
            port=port,
            reload=False,  # 生产环境建议关闭热重载
            log_level="info",
            access_log=True
        )
    except KeyboardInterrupt:
        print("\n🔄 调度器服务正在停止...")
        print("👋 调度器服务已停止")
    except Exception as e:
        print(f"❌ 调度器服务启动失败: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

