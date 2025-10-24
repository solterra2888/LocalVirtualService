# -*- coding: utf-8 -*-
"""
投资主题向量化管理API
提供主题向量初始化、匹配、统计等功能
"""

import asyncio
from fastapi import APIRouter, HTTPException, BackgroundTasks
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from backend.services.investment_theme_embedding_service import InvestmentThemeEmbeddingService
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

router = APIRouter(prefix="/investment-themes", tags=["Investment Themes"])
logger = get_logger("investment_themes_api")


# ==================== Request/Response Models ====================

class ThemeMatchRequest(BaseModel):
    """主题匹配请求"""
    news_text: str = Field(..., description="新闻文本（标题+内容）")
    news_id: Optional[int] = Field(None, description="新闻ID（用于保存匹配结果）")
    market: Optional[str] = Field(None, description="市场过滤（HK/US/CN）")
    top_k: int = Field(5, ge=1, le=20, description="返回Top-K个主题")
    min_similarity: float = Field(0.6, ge=0.0, le=1.0, description="最小相似度阈值")
    save_to_db: bool = Field(False, description="是否保存匹配结果到数据库")


class ThemeMatchResponse(BaseModel):
    """主题匹配响应"""
    theme_id: str
    theme_name: str
    market: str
    similarity: float
    confidence: float
    rank: int


class InitializeThemesRequest(BaseModel):
    """初始化主题向量请求"""
    force_update: bool = Field(False, description="是否强制更新已存在的向量")


class UpdateThemeRequest(BaseModel):
    """更新单个主题向量请求"""
    theme_id: str = Field(..., description="主题ID")
    theme_name: str = Field(..., description="主题名称")
    market: str = Field(..., description="市场代码（HK/US/CN）")


class FillDescriptionsRequest(BaseModel):
    """批量填充主题描述请求"""
    force_regenerate: bool = Field(False, description="whether to force regenerate existing descriptions")
    market: Optional[str] = Field(None, description="market filter (HK/US/CN), if empty, process all markets")


# ==================== API Endpoints ====================

@router.post("/match", response_model=List[ThemeMatchResponse])
async def match_news_to_themes(request: ThemeMatchRequest):
    """
    将新闻匹配到投资主题（基于向量相似度）
    
    使用场景：
    - 新闻分析后自动匹配相关投资主题
    - 前端实时预览新闻可能匹配的主题
    
    示例：
    ```
    POST /api/investment-themes/match
    {
        "news_text": "特斯拉发布新款电动车，配备先进自动驾驶功能",
        "market": "US",
        "top_k": 3,
        "min_similarity": 0.6
    }
    ```
    """
    try:
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        logger.info(
            f"Received theme matching request",
            news_id=request.news_id,
            market=request.market,
            top_k=request.top_k
        )
        
        matches = await service.match_news_to_themes(
            news_text=request.news_text,
            news_id=request.news_id,
            market=request.market,
            top_k=request.top_k,
            min_similarity=request.min_similarity,
            save_to_db=request.save_to_db
        )
        
        logger.info(f"Theme matching completed, found {len(matches)} matches")
        
        return matches
        
    except Exception as e:
        logger.error(f"Theme matching failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"主题匹配失败: {str(e)}")


@router.post("/initialize")
async def initialize_theme_embeddings(request: InitializeThemesRequest):
    """
    Initialize all investment theme embeddings (using Celery)
    
    说明：
    - 从 futu_investment_themes 表读取主题
    - 调用Qwen Embedding API生成向量
    - 存储到 investment_theme_embeddings 表
    - 使用 Celery 后台任务执行（异步处理）
    
    注意：
    - 首次运行需要几分钟时间
    - force_update=True 将重新生成所有向量
    - 任务在后台异步执行，使用返回的 task_id 查询进度
    
    示例：
    ```
    POST /api/investment-themes/initialize
    {
        "force_update": false
    }
    ```
    
    返回：
    ```
    {
        "success": true,
        "message": "Task started",
        "task_id": "abc-123-def-456",
        "status_url": "/api/investment-themes/task-status/abc-123-def-456"
    }
    ```
    """
    try:
        from backend.tasks.theme_tasks import initialize_theme_embeddings_task
        
        logger.info("Starting theme embeddings initialization task (Celery)", force_update=request.force_update)
        
        # Submit Celery task
        task = initialize_theme_embeddings_task.apply_async(
            kwargs={'force_update': request.force_update}
        )
        
        logger.info(f"Theme embeddings initialization task submitted: {task.id}")
        
        return {
            "success": True,
            "message": "Theme embeddings initialization task started",
            "task_id": task.id,
            "status_url": f"/api/investment-themes/task-status/{task.id}"
        }
            
    except Exception as e:
        logger.error(f"Failed to start theme embeddings initialization task: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to start task: {str(e)}")




@router.put("/update-theme")
async def update_theme_embedding(request: UpdateThemeRequest):
    """
    更新单个主题的向量
    
    使用场景：
    - 新增主题后生成向量
    - 主题名称修改后更新向量
    
    示例：
    ```
    PUT /api/investment-themes/update-theme
    {
        "theme_id": "EV_US_001",
        "theme_name": "Electric Vehicles",
        "market": "US"
    }
    ```
    """
    try:
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        logger.info(f"Updating theme vector: {request.theme_id} - {request.theme_name}")
        
        success = await service.update_single_theme_embedding(
            theme_id=request.theme_id,
            theme_name=request.theme_name,
            market=request.market
        )
        
        if success:
            return {
                "success": True,
                "message": f"Theme vector updated successfully: {request.theme_name}"
            }
        else:
            raise HTTPException(status_code=500, detail="Update failed")
            
    except Exception as e:
        logger.error(f"Updating theme vector failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Update failed: {str(e)}")


@router.get("/news/{news_id}/themes")
async def get_news_themes(news_id: int):
    """
    获取某条新闻匹配的投资主题
    
    从 news_theme_matches 表查询已保存的匹配结果
    
    示例：
    ```
    GET /api/investment-themes/news/123/themes
    ```
    """
    try:
        from backend.database.db_manager import DatabaseManager
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                theme_id,
                theme_name,
                market,
                similarity_score,
                match_rank,
                match_method,
                matched_at
            FROM news_theme_matches
            WHERE news_id = %s
            ORDER BY match_rank ASC
        """, (news_id,))
        
        results = []
        for row in cursor.fetchall():
            results.append({
                'theme_id': row[0],
                'theme_name': row[1],
                'market': row[2],
                'similarity': float(row[3]),
                'rank': row[4],
                'match_method': row[5],
                'matched_at': row[6].isoformat() if row[6] else None
            })
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "news_id": news_id,
            "themes": results,
            "count": len(results)
        }
        
    except Exception as e:
        logger.error(f"Getting news themes failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Getting failed: {str(e)}")


@router.get("/themes")
async def list_all_themes(
    market: Optional[str] = None,
    limit: int = 100,
    offset: int = 0
):
    """
    列出所有投资主题
    
    参数：
    - market: 市场过滤（HK/US/CN）
    - limit: 每页数量
    - offset: 偏移量
    
    示例：
    ```
    GET /api/investment-themes/themes?market=US&limit=20
    ```
    """
    try:
        from backend.database.db_manager import DatabaseManager
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor()
        
        # 构建查询
        where_clause = ""
        where_clause_with_alias = ""
        params = []
        
        if market:
            where_clause = "WHERE market = %s"
            where_clause_with_alias = "WHERE ite.market = %s"
            params.append(market)
        
        # 获取总数
        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM investment_theme_embeddings 
            {where_clause}
        """, params)
        total_count = cursor.fetchone()[0]
        
        # 获取列表（联合查询获取 description 相关字段）
        params.extend([limit, offset])
        cursor.execute(f"""
            SELECT 
                ite.theme_id,
                ite.theme_name,
                ite.market,
                ite.embedding_model,
                ite.created_at,
                ite.updated_at,
                fit.description,
                fit.description_source,
                fit.description_updated_at
            FROM investment_theme_embeddings ite
            LEFT JOIN futu_investment_themes fit 
                ON ite.theme_id = fit.theme_id AND ite.market = fit.market
            {where_clause_with_alias}
            ORDER BY ite.market, ite.theme_name
            LIMIT %s OFFSET %s
        """, params)
        
        themes = []
        for row in cursor.fetchall():
            themes.append({
                'theme_id': row[0],
                'theme_name': row[1],
                'market': row[2],
                'embedding_model': row[3],
                'created_at': row[4].isoformat() if row[4] else None,
                'updated_at': row[5].isoformat() if row[5] else None,
                'description': row[6],  # 主题描述
                'description_source': row[7],  # 描述来源
                'description_updated_at': row[8].isoformat() if row[8] else None  # 描述更新时间
            })
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "themes": themes,
            "total": total_count,
            "limit": limit,
            "offset": offset
        }
        
    except Exception as e:
        logger.error(f"Getting theme list failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Getting failed: {str(e)}")


@router.get("/task-status/{task_id}")
async def get_task_status(task_id: str):
    """
    Get Celery task status and progress
    
    说明：
    - 查询 Celery 任务的当前状态和进度
    - 适用于所有投资主题相关的异步任务
    
    示例：
    ```
    GET /api/investment-themes/task-status/abc-123-def-456
    ```
    
    返回：
    ```
    {
        "task_id": "abc-123-def-456",
        "status": "PROGRESS",
        "progress": 45,
        "message": "Processing batch 5/10",
        "result": null
    }
    ```
    
    状态说明：
    - PENDING: 任务等待中
    - PROGRESS: 任务执行中
    - SUCCESS: 任务成功完成
    - FAILURE: 任务失败
    """
    try:
        from celery.result import AsyncResult
        from backend.celery_app import app
        
        # Get task result
        task_result = AsyncResult(task_id, app=app)
        
        response = {
            "task_id": task_id,
            "status": task_result.state,
            "progress": 0,
            "message": "",
            "result": None
        }
        
        if task_result.state == 'PENDING':
            response["message"] = "Task is waiting to be executed"
        elif task_result.state == 'PROGRESS':
            # Get progress information from task meta
            if task_result.info:
                response["progress"] = task_result.info.get('progress', 0)
                response["message"] = task_result.info.get('message', 'Processing...')
                # Include additional metadata
                for key in ['total', 'processed', 'failed', 'current_batch', 'total_batches']:
                    if key in task_result.info:
                        response[key] = task_result.info[key]
        elif task_result.state == 'SUCCESS':
            response["progress"] = 100
            response["message"] = "Task completed successfully"
            response["result"] = task_result.result
        elif task_result.state == 'FAILURE':
            response["progress"] = 0
            response["message"] = f"Task failed: {str(task_result.info)}"
            response["error"] = str(task_result.info)
        
        return response
        
    except Exception as e:
        logger.error(f"Failed to get task status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to get task status: {str(e)}")


@router.post("/fill-descriptions")
async def fill_theme_descriptions(request: FillDescriptionsRequest):
    """
    Batch fill theme descriptions using LLM (using Celery)
    
    参数：
    - force_regenerate: 是否强制重新生成已有描述
    - market: 市场过滤（可选）
    
    说明：
    - 默认只填充缺失的描述（增量更新）
    - force_regenerate=True 时会重新生成所有描述
    - 使用 Celery 后台任务异步处理，避免超时
    - 任务在后台异步执行，使用返回的 task_id 查询进度
    
    示例：
    ```
    POST /api/investment-themes/fill-descriptions
    {
        "force_regenerate": false,
        "market": "US"
    }
    ```
    
    返回：
    ```
    {
        "success": true,
        "message": "Task started",
        "task_id": "abc-123-def-456",
        "status_url": "/api/investment-themes/task-status/abc-123-def-456"
    }
    ```
    """
    try:
        from backend.tasks.theme_tasks import fill_theme_descriptions_task
        
        logger.info(
            "Starting theme descriptions filling task (Celery)", 
            force_regenerate=request.force_regenerate,
            market=request.market
        )
        
        # Submit Celery task
        task = fill_theme_descriptions_task.apply_async(
            kwargs={
                'force_regenerate': request.force_regenerate,
                'market': request.market
            }
        )
        
        logger.info(f"Theme descriptions filling task submitted: {task.id}")
        
        return {
            "success": True,
            "message": "Theme descriptions filling task started",
            "task_id": task.id,
            "status_url": f"/api/investment-themes/task-status/{task.id}"
        }
        
    except Exception as e:
        logger.error(f"Failed to start theme descriptions filling task: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to start task: {str(e)}")


@router.post("/regenerate-all")
async def regenerate_all_theme_embeddings(market: Optional[str] = None):
    """
    Regenerate all theme embeddings (descriptions + vectors) using Celery
    
    说明：
    - 完全重新生成所有主题的描述和向量
    - 包含两个步骤：1) 生成描述 2) 生成向量
    - 适用于模型更新或大规模数据更新后
    - 使用 Celery 后台任务异步处理
    
    参数：
    - market: 市场过滤（HK/US/CN），为空则处理所有市场
    
    示例：
    ```
    POST /api/investment-themes/regenerate-all?market=US
    ```
    
    返回：
    ```
    {
        "success": true,
        "message": "Task started",
        "task_id": "abc-123-def-456",
        "status_url": "/api/investment-themes/task-status/abc-123-def-456"
    }
    ```
    """
    try:
        from backend.tasks.theme_tasks import regenerate_all_theme_embeddings_task
        
        logger.info("Starting regenerate all theme embeddings task (Celery)", market=market)
        
        # Submit Celery task
        task = regenerate_all_theme_embeddings_task.apply_async(
            kwargs={'market': market}
        )
        
        logger.info(f"Regenerate all theme embeddings task submitted: {task.id}")
        
        return {
            "success": True,
            "message": "Regenerate all theme embeddings task started",
            "task_id": task.id,
            "status_url": f"/api/investment-themes/task-status/{task.id}"
        }
        
    except Exception as e:
        logger.error(f"Failed to start regenerate all task: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to start task: {str(e)}")


@router.post("/fetch-futu-themes")
async def fetch_futu_themes(
    markets: Optional[List[str]] = None,
    max_pages: Optional[int] = None,
    scrape_stocks: bool = True
):
    """
    Fetch Futu investment themes data using Celery (手动触发)
    
    说明：
    - 手动触发富途投资主题数据爬取
    - 任务通常需要约1小时运行时间
    - 使用 Celery 后台任务异步处理
    - 爬取港股、美股等市场的投资主题及相关股票
    
    参数：
    - markets: 市场列表，如 ['HK', 'US']，为空则爬取所有市场
    - max_pages: 每个市场最大爬取页数，为空则爬取所有页面
    - scrape_stocks: 是否爬取每个主题的股票列表（默认：True）
    
    示例：
    ```
    POST /api/investment-themes/fetch-futu-themes
    {
        "markets": ["HK", "US"],
        "max_pages": null,
        "scrape_stocks": true
    }
    ```
    
    返回：
    ```
    {
        "success": true,
        "message": "Futu themes fetching task started",
        "task_id": "abc-123-def-456",
        "status_url": "/api/investment-themes/task-status/abc-123-def-456"
    }
    ```
    """
    try:
        from backend.tasks.theme_tasks import fetch_futu_themes_task
        
        logger.info(
            "手动触发Futu投资主题爬取任务 (Celery)", 
            markets=markets or "ALL",
            max_pages=max_pages or "ALL",
            scrape_stocks=scrape_stocks
        )
        
        # 提交Celery任务
        task = fetch_futu_themes_task.apply_async(
            kwargs={
                'markets': markets,
                'max_pages': max_pages,
                'scrape_stocks': scrape_stocks
            }
        )
        
        logger.info(f"Futu投资主题爬取任务已提交: {task.id}")
        
        return {
            "success": True,
            "message": "Futu themes fetching task started (expected to run for ~1 hour)",
            "task_id": task.id,
            "status_url": f"/api/investment-themes/task-status/{task.id}",
            "estimated_duration": "约1小时"
        }
        
    except Exception as e:
        logger.error(f"Failed to start Futu themes fetching task: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to start task: {str(e)}")

