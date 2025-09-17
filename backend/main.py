#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastAPI 主应用入口
"""

import os
import sys
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.api import auth, files, tasks, users, news, markets, dashboard, screener, scheduler_proxy
from backend.database.db_manager import DatabaseManager
from backend.processors.task_manager import TaskManager
from backend.auth.jwt_utils import get_current_user
from backend.utils.logger import init_logging, info, error, warning
from config.settings import config

# 安全认证
security = HTTPBearer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 初始化日志系统
    try:
        logging_config = config.get_logging_config()
        init_logging(**logging_config)
        info("🚀 Universal Glance API 启动中...", source="startup")
    except Exception as e:
        print(f"⚠️ 日志系统初始化失败: {str(e)}")
    
    # 启动时初始化数据库
    try:
        # 获取数据库连接参数
        db_conn_params = {
            'db_host': config.get('DB_HOST', 'localhost'),
            'db_port': config.get('DB_PORT', '5432'),
            'db_name': config.get('DB_NAME', 'targettracking'),
            'db_user': config.get('DB_USER', 'postgres'),
            'db_password': config.get('DB_PASSWORD', '')
        }
        
        db_manager = DatabaseManager(db_conn_params)
        if db_manager.ensure_schema():
            info("✅ 数据库初始化完成", source="startup")
        else:
            warning("⚠️ 数据库初始化失败，但服务仍可继续运行", source="startup")
    except Exception as e:
        error(f"⚠️ 数据库初始化出错: {str(e)}", source="startup")
    
    # 注意：调度器服务已分离，现在由独立的调度器服务运行
    info("ℹ️ 调度器服务已分离到独立进程运行", source="startup")
    
    yield
    
    # 关闭时清理资源
    info("🔄 应用关闭，清理资源...", source="shutdown")
    
    # 注意：调度器服务已分离，由独立进程管理生命周期
    info("ℹ️ 主应用关闭完成，调度器服务独立运行", source="shutdown")

# 创建FastAPI应用
app = FastAPI(
    title="Universal Glance API",
    description="文件处理和任务管理API",
    version="2.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    lifespan=lifespan
)

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境应该限制具体域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(auth.router, prefix="/api/auth", tags=["认证"])
app.include_router(files.router, prefix="/api/files", tags=["文件处理"])
app.include_router(tasks.router, prefix="/api/tasks", tags=["任务管理"])
app.include_router(users.router, prefix="/api/users", tags=["用户管理"])
app.include_router(news.router, prefix="/api/news", tags=["新闻管理"])
app.include_router(markets.router, prefix="/api/markets", tags=["市场数据"])
# 使用调度器代理API，与独立调度器服务通信
app.include_router(scheduler_proxy.router, prefix="/api", tags=["定时任务调度代理"])
app.include_router(dashboard.router, prefix="/api/dashboard", tags=["仪表板"])
app.include_router(screener.router, prefix="/api", tags=["股票筛选器"])

# 静态文件服务
if os.path.exists("web-frontend/dist"):
    app.mount("/", StaticFiles(directory="web-frontend/dist", html=True), name="static")

@app.get("/")
async def root():
    """根路径"""
    return {"message": "Universal Glance API", "version": "2.0.0"}

@app.get("/api/health")
async def health_check():
    """健康检查"""
    try:
        # 基础健康状态
        health_status = {
            "status": "healthy",
            "message": "API运行正常",
            "timestamp": str(datetime.now()),
            "version": "2.0.0"
        }
        
        # 数据库连接检查
        try:
            # 获取数据库连接参数
            db_conn_params = {
                'db_host': config.get('DB_HOST', 'localhost'),
                'db_port': config.get('DB_PORT', '5432'),
                'db_name': config.get('DB_NAME', 'targettracking'),
                'db_user': config.get('DB_USER', 'postgres'),
                'db_password': config.get('DB_PASSWORD', '')
            }
            
            db_manager = DatabaseManager(db_conn_params)
            conn = db_manager.get_connection()
            if conn:
                conn.close()
                health_status["database"] = "connected"
            else:
                health_status["database"] = "disconnected"
                health_status["status"] = "warning"
        except Exception as e:
            health_status["database"] = f"error: {str(e)}"
            health_status["status"] = "warning"
        
        # 配置检查
        try:
            config_status = {
                "has_jwt_secret": bool(config.get("JWT_SECRET_KEY")),
                "has_db_config": bool(config.get("DB_HOST")),
            }
            health_status["config"] = config_status
        except Exception as e:
            health_status["config"] = f"error: {str(e)}"
        
        return health_status
    except Exception as e:
        return {
            "status": "error", 
            "message": f"健康检查失败: {str(e)}",
            "timestamp": str(datetime.now())
        }

@app.get("/api/debug/config")
async def debug_config(current_user=Depends(get_current_user)):
    """调试配置信息（需要认证）"""
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="需要管理员权限")
    
    return {
        "database": {
            "host": config.get("DB_HOST", "未配置"),
            "port": config.get("DB_PORT", "未配置"),
            "name": config.get("DB_NAME", "未配置"),
            "user": config.get("DB_USER", "未配置"),
            "password_set": bool(config.get("DB_PASSWORD"))
        },
        "jwt": {
            "secret_key_set": bool(config.get("JWT_SECRET_KEY"))
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    ) 