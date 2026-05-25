#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FastAPI 主应用入口
"""

import os
import sys
import time
from datetime import datetime
from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.staticfiles import StaticFiles
from contextlib import asynccontextmanager

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from backend.api import auth, files, tasks, users, news, markets, dashboard, screener, scheduler_proxy, llm_config, llm_prompt_templates, themes_unified, market_weekly, file_tags, file_folders, logs, monitoring, subscriptions, subscription_content_tags, twitter_daily_reports, youtube_daily_reports, daily, feed_summary_settings, personal_tokens, agent_v1, agent_v2_search
from backend.database.db_manager import DatabaseManager
from backend.auth.jwt_utils import get_current_user
from backend.utils.logger import init_logging, info, error, warning
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import install_psycopg2_utc_adapter
from config.settings import config

# ─── 全系统时区契约: 进程启动时立即注册 psycopg2 UTC 适配器 ───
# 任何 naive datetime (含历史代码 datetime.now()) 写入 PG 时自动转 UTC.
# 必须在任何 DB 连接之前 (这里 import 阶段就执行).
# 详见 backend/utils/datetime_utils.py 与 docs/instructions/DAILY_MODULE.md §13.
install_psycopg2_utc_adapter()

# API 进程启动时间（Unix 秒），供前端判断「整站重启」后清理 localStorage 中的 Celery 任务条
APP_BOOT_UNIX = time.time()

# 安全认证
security = HTTPBearer()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # 初始化日志系统
    try:
        logging_config = config.get_logging_config()
        init_logging(**logging_config)
        info(f"🚀 {config.APP_NAME} API 启动中...", source="startup")
    except Exception as e:
        print(f"⚠️ 日志系统初始化失败: {str(e)}")
    
    # 启动时初始化数据库
    try:
        # 获取数据库连接参数
        db_conn_params = get_database_config()
        
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

# 创建FastAPI应用（生产 ENV=production 时默认不暴露文档，见 config.settings.ENABLE_API_DOCS）
_api_docs = config.ENABLE_API_DOCS
app = FastAPI(
    title=f"{config.APP_NAME} API",
    description="文件处理和任务管理API",
    version=config.APP_VERSION,
    docs_url="/api/docs" if _api_docs else None,
    redoc_url="/api/redoc" if _api_docs else None,
    # 与 docs_url 同前缀：Nginx 仅代理 /api/ 时，默认根路径 /openapi.json 会落到前端 SPA。
    openapi_url="/api/openapi.json" if _api_docs else None,
    lifespan=lifespan,
)

# 配置CORS
# allow_credentials=True 与 allow_origins=["*"] 不能同时使用（CORS规范限制）
# 生产环境中 nginx 统一代理（同源），CORS 头主要服务于本地开发跨端口场景
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


# PR-F: LLM per-user × per-provider 限速命中时, 转 HTTP 429 + Retry-After 头.
# 让前端可以根据 retry_after_seconds 友好提示用户 "请 N 秒后重试".
from fastapi.responses import JSONResponse  # noqa: E402
from fastapi import Request  # noqa: E402
from backend.core.llm_rate_limiter import LLMRateLimitExceeded  # noqa: E402
from backend.core.pat_rate_limiter import PATRateLimitExceeded  # noqa: E402


@app.exception_handler(PATRateLimitExceeded)
async def pat_rate_limit_handler(request: Request, exc: PATRateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={
            "detail": (
                f"API token rate limit exceeded. Retry in {exc.retry_after_seconds}s "
                f"({exc.current}/{exc.limit} per minute)."
            ),
            "retry_after_seconds": exc.retry_after_seconds,
            "current": exc.current,
            "limit": exc.limit,
        },
        headers={"Retry-After": str(exc.retry_after_seconds)},
    )


@app.exception_handler(LLMRateLimitExceeded)
async def llm_rate_limit_handler(request: Request, exc: LLMRateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={
            "detail": (
                f"You're triggering AI too quickly. Please wait {exc.retry_after_seconds}s "
                f"before retrying ({exc.current}/{exc.limit} per minute on {exc.provider})."
            ),
            "retry_after_seconds": exc.retry_after_seconds,
            "current": exc.current,
            "limit": exc.limit,
            "provider": exc.provider,
        },
        headers={"Retry-After": str(exc.retry_after_seconds)},
    )

# 注册路由
app.include_router(auth.router, prefix="/api/auth", tags=["认证"])
app.include_router(files.router, prefix="/api/files", tags=["文件处理"])
app.include_router(file_tags.router, tags=["文件标签"])
app.include_router(file_folders.router, prefix="/api/files", tags=["文件夹"])
app.include_router(tasks.router, prefix="/api/tasks", tags=["任务管理"])
app.include_router(users.router, prefix="/api/users", tags=["用户管理"])
app.include_router(personal_tokens.router, prefix="/api/users", tags=["Personal API Tokens"])
app.include_router(news.router, prefix="/api/news", tags=["新闻管理"])
app.include_router(markets.router, prefix="/api/markets", tags=["市场数据"])
# Market Weekly Report API
app.include_router(market_weekly.router, prefix="/api", tags=["Market Weekly"])
# Twitter Daily Report API
app.include_router(twitter_daily_reports.router, prefix="/api", tags=["Twitter Daily Report"])
# YouTube Daily Report API
app.include_router(youtube_daily_reports.router, prefix="/api", tags=["YouTube Daily Report"])
# 使用调度器代理API，与独立调度器服务通信
app.include_router(scheduler_proxy.router, prefix="/api", tags=["定时任务调度代理"])
app.include_router(subscriptions.router, prefix="/api/subscriptions", tags=["订阅管理"])
# Feeds 单条内容自动打标 + 摘要 (Phase 3, 与 subscriptions 路由共享 /api/subscriptions 前缀,
# 但通过独立 APIRouter 与 schema 解耦; 设计文档 docs/instructions/FEEDS_AUTO_TAGGING_PLAN.md)
app.include_router(subscription_content_tags.router, prefix="/api/subscriptions", tags=["订阅内容标签"])
# PR-N1: Feed 个性化摘要 用户设置 (右上角齿轮配置)
app.include_router(feed_summary_settings.router, prefix="/api/feeds", tags=["Feed 摘要设置"])
app.include_router(dashboard.router, prefix="/api/dashboard", tags=["仪表板"])
# Daily 聚合视图: 把 Upload Files + Feeds 订阅合并成首页时间轴信息流 (/daily 默认页)
# 详见 backend/api/daily.py 与 docs/instructions/UPLOAD_FILES_AND_LLM_ANALYSIS_FLOW.md
app.include_router(daily.router, prefix="/api/daily", tags=["Daily 聚合视图"])
# Agent / MCP readonly API (Bearer JWT or PAT ght_...)
app.include_router(agent_v1.router)
app.include_router(agent_v2_search.router)
app.include_router(screener.router, prefix="/api", tags=["股票筛选器"])

# 图表事件API
from backend.api import chart_events
app.include_router(chart_events.router, prefix="/api/chart-events", tags=["图表事件"])

# 统一主题管理API（支持 Primary Theme）
app.include_router(themes_unified.router, prefix="/api", tags=["统一主题管理"])

# EOD Fetch API
from backend.api import eod_fetch
app.include_router(eod_fetch.router, prefix="/api", tags=["EOD数据获取"])

# LLM配置API
app.include_router(llm_config.router, prefix="/api", tags=["LLM配置"])

# LLM Prompt模板API
app.include_router(llm_prompt_templates.router, prefix="/api", tags=["Prompt模板"])

# 新闻LLM分析API
from backend.api import news_analysis
app.include_router(news_analysis.router, prefix="/api", tags=["新闻分析"])

# 投资主题向量化API
from backend.api import investment_themes
app.include_router(investment_themes.router, prefix="/api", tags=["投资主题"])

# 用户新闻筛选预设API
from backend.api import user_news_presets
app.include_router(user_news_presets.router, prefix="/api", tags=["用户新闻筛选预设"])

# 投资项目管理API
from backend.api import projects
app.include_router(projects.router, prefix="/api", tags=["投资项目管理"])

# 系统日志API
app.include_router(logs.router, tags=["系统日志"])

# Redis / Celery 运行时监控 API（admin-only）
app.include_router(monitoring.router, tags=["系统监控"])

# 静态文件服务
# Agent Skill 文档 (SKILL.md)
_agent_skill_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "docs", "agent"
)
if os.path.isdir(_agent_skill_dir):
    app.mount(
        "/agent/skill",
        StaticFiles(directory=_agent_skill_dir, html=False),
        name="agent_skill_docs",
    )

# MCP 包下载（wheel 文件，供用户 pip install 使用）
_dist_dir = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dist"
)
if os.path.isdir(_dist_dir):
    app.mount(
        "/downloads",
        StaticFiles(directory=_dist_dir, html=False),
        name="downloads",
    )

# 为上传的文件提供静态文件访问
if os.path.exists("uploads"):
    app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

if os.path.exists("web-frontend/dist"):
    app.mount("/", StaticFiles(directory="web-frontend/dist", html=True), name="static")

@app.get("/")
async def root():
    """根路径"""
    return {"message": f"{config.APP_NAME} API", "version": config.APP_VERSION}

@app.get("/api/health")
async def health_check():
    """健康检查"""
    try:
        # 基础健康状态
        health_status = {
            "status": "healthy",
            "message": "API运行正常",
            "timestamp": str(datetime.now()),
            "version": config.APP_VERSION,
            "app_boot_unix": APP_BOOT_UNIX,
        }
        
        # 数据库连接检查
        try:
            # 获取数据库连接参数
            db_conn_params = get_database_config()
            
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
                "has_jwt_secret": bool(os.getenv("JWT_SECRET_KEY")),
                "has_db_config": bool(os.getenv("DB_HOST")),
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
            "host": os.getenv("DB_HOST", "未配置"),
            "port": os.getenv("DB_PORT", "未配置"),
            "name": os.getenv("DB_NAME", "未配置"),
            "user": os.getenv("DB_USER", "未配置"),
            "password_set": bool(os.getenv("DB_PASSWORD"))
        },
        "jwt": {
            "secret_key_set": bool(os.getenv("JWT_SECRET_KEY"))
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