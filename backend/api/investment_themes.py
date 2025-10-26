# -*- coding: utf-8 -*-
"""
投资主题向量化管理API
提供主题向量初始化、匹配、统计等功能
"""

import asyncio
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from backend.services.investment_theme_embedding_service import InvestmentThemeEmbeddingService
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger
from backend.auth.jwt_utils import get_current_admin_user, TokenData

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


class UpdateEnabledStatusRequest(BaseModel):
    """更新启用状态请求"""
    market: str = Field(..., description="市场代码（HK/US/CN）")
    is_enabled: bool = Field(..., description="是否启用")


class BatchUpdateEnabledRequest(BaseModel):
    """批量更新启用状态请求"""
    theme_ids: List[str] = Field(..., description="主题ID列表")
    markets: List[str] = Field(..., description="市场代码列表（与theme_ids对应）")
    is_enabled: bool = Field(..., description="是否启用")


class UpdateDescriptionRequest(BaseModel):
    """更新主题描述请求"""
    market: str = Field(..., description="市场代码（HK/US/CN）")
    description: str = Field(..., min_length=1, max_length=2000, description="新的描述内容")
    version: Optional[int] = Field(None, description="版本号（用于乐观锁）")


class DeleteThemeRequest(BaseModel):
    """删除主题请求"""
    market: str = Field(..., description="市场代码（HK/US/CN）")


class GenerateDescriptionRequest(BaseModel):
    """生成主题描述请求"""
    theme_name: str = Field(..., min_length=1, max_length=200, description="主题名称")
    market: str = Field(..., description="市场代码（HK/US/CN）")


class CreateThemeRequest(BaseModel):
    """创建主题请求"""
    theme_name: str = Field(..., min_length=1, max_length=200, description="主题名称")
    market: str = Field(..., description="市场代码（HK/US/CN）")
    description: str = Field(..., min_length=10, max_length=2000, description="主题描述")
    theme_id: Optional[str] = Field(None, max_length=50, description="主题ID（可选，不提供则自动生成）")


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
        
        # 获取列表（联合查询获取 description 和管理相关字段）
        params.extend([limit, offset])
        cursor.execute(f"""
            SELECT 
                ite.theme_id,
                ite.theme_name,
                ite.market,
                ite.embedding_model,
                ite.created_at,
                ite.updated_at,
                ite.version,
                ite.last_manual_edit_at,
                ite.last_manual_edit_by,
                fit.description,
                fit.description_source,
                fit.is_enabled_for_matching,
                fit.is_deleted,
                fit.stock_count,
                fit.change_percent
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
                'version': row[6],  # 版本号
                'last_manual_edit_at': row[7].isoformat() if row[7] else None,  # 最后编辑时间
                'last_manual_edit_by': row[8],  # 最后编辑者
                'description_preview': row[9],  # 主题描述预览（从futu_investment_themes获取）
                'description_source': row[10],  # 描述来源
                'is_enabled_for_matching': row[11] if row[11] is not None else True,  # 是否启用
                'is_deleted': row[12] if row[12] is not None else False,  # 是否已删除
                'stock_count': row[13] if row[13] else 0,  # 股票数量
                'change_percent': row[14]  # 涨跌幅
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


# ==================== Theme Management Endpoints (Admin Only) ====================

@router.patch("/{theme_id}/enabled")
async def update_theme_enabled_status(
    theme_id: str,
    request: UpdateEnabledStatusRequest,
    current_user: TokenData = Depends(get_current_admin_user)
):
    """
    更新主题的启用/禁用状态（仅管理员）
    
    权限：仅管理员
    
    说明：
    - 禁用的主题不会参与新闻匹配
    - 启用主题前会检查是否已向量化
    
    示例：
    ```
    PATCH /api/investment-themes/BK1001/enabled
    {
        "market": "HK",
        "is_enabled": false
    }
    ```
    """
    try:
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        logger.info(
            f"Admin {current_user.username} updating theme status",
            theme_id=theme_id,
            market=request.market,
            is_enabled=request.is_enabled
        )
        
        result = service.update_theme_enabled_status(
            theme_id=theme_id,
            market=request.market,
            is_enabled=request.is_enabled,
            operator_username=current_user.username
        )
        
        if result["success"]:
            return result
        else:
            raise HTTPException(status_code=400, detail=result["message"])
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update theme enabled status failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"更新失败: {str(e)}")


@router.post("/batch-enabled")
async def batch_update_theme_enabled_status(
    request: BatchUpdateEnabledRequest,
    current_user: TokenData = Depends(get_current_admin_user)
):
    """
    批量更新主题的启用/禁用状态（仅管理员）
    
    权限：仅管理员
    
    示例：
    ```
    POST /api/investment-themes/batch-enabled
    {
        "theme_ids": ["BK1001", "BK1002"],
        "markets": ["HK", "HK"],
        "is_enabled": false
    }
    ```
    """
    try:
        if len(request.theme_ids) != len(request.markets):
            raise HTTPException(
                status_code=400,
                detail="theme_ids和markets数组长度必须一致"
            )
        
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        logger.info(
            f"Admin {current_user.username} batch updating theme status",
            count=len(request.theme_ids),
            is_enabled=request.is_enabled
        )
        
        results = []
        success_count = 0
        failed_count = 0
        
        for theme_id, market in zip(request.theme_ids, request.markets):
            result = service.update_theme_enabled_status(
                theme_id=theme_id,
                market=market,
                is_enabled=request.is_enabled,
                operator_username=current_user.username
            )
            
            results.append({
                "theme_id": theme_id,
                "market": market,
                "success": result["success"],
                "message": result["message"]
            })
            
            if result["success"]:
                success_count += 1
            else:
                failed_count += 1
        
        return {
            "success": True,
            "total": len(request.theme_ids),
            "success_count": success_count,
            "failed_count": failed_count,
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Batch update theme enabled status failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"批量更新失败: {str(e)}")


@router.patch("/{theme_id}/description")
async def update_theme_description(
    theme_id: str,
    request: UpdateDescriptionRequest,
    current_user: TokenData = Depends(get_current_admin_user)
):
    """
    更新主题描述并重新向量化（仅管理员）
    
    权限：仅管理员
    
    说明：
    - 更新主题的描述内容
    - 自动重新生成向量
    - 如果向量化失败，描述仍会保存，但会返回错误提示
    - 支持乐观锁（可选版本号参数）
    
    示例：
    ```
    PATCH /api/investment-themes/BK1001/description
    {
        "market": "HK",
        "description": "这是一个关于人工智能和机器学习的主题，包含相关的科技公司...",
        "version": 5
    }
    ```
    
    返回：
    ```
    {
        "success": true,
        "message": "描述和向量更新成功",
        "description_saved": true,
        "embedding_updated": true
    }
    ```
    
    或（向量化失败时）：
    ```
    {
        "success": false,
        "message": "描述已保存，但向量更新失败: API限流",
        "description_saved": true,
        "embedding_updated": false,
        "error": "API限流"
    }
    ```
    """
    try:
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        logger.info(
            f"Admin {current_user.username} updating theme description",
            theme_id=theme_id,
            market=request.market,
            description_length=len(request.description)
        )
        
        result = await service.update_theme_description(
            theme_id=theme_id,
            market=request.market,
            new_description=request.description,
            editor_username=current_user.username,
            version=request.version
        )
        
        # 根据结果返回不同的状态码
        if result["success"]:
            return result
        elif result.get("description_saved"):
            # 描述保存成功但向量化失败，返回206 Partial Content
            return result
        else:
            # 完全失败
            raise HTTPException(status_code=400, detail=result["message"])
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update theme description failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"更新失败: {str(e)}")


@router.delete("/{theme_id}")
async def delete_theme(
    theme_id: str,
    market: str,
    current_user: TokenData = Depends(get_current_admin_user)
):
    """
    删除投资主题（软删除，仅管理员）
    
    权限：仅管理员
    
    说明：
    - 软删除主题（标记为已删除，不会从数据库物理删除）
    - 级联删除相关数据：
      * 向量数据（investment_theme_embeddings）
      * 新闻匹配记录（news_theme_matches）
    - 爬虫不会重新创建已删除的主题
    
    示例：
    ```
    DELETE /api/investment-themes/BK1001?market=HK
    ```
    
    返回：
    ```
    {
        "success": true,
        "message": "主题 'AI芯片' 已删除",
        "deleted_matches_count": 156,
        "deleted_embeddings": true
    }
    ```
    """
    try:
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        logger.info(
            f"Admin {current_user.username} deleting theme",
            theme_id=theme_id,
            market=market
        )
        
        result = service.delete_theme(
            theme_id=theme_id,
            market=market,
            operator_username=current_user.username
        )
        
        if result["success"]:
            return result
        else:
            raise HTTPException(status_code=400, detail=result["message"])
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Delete theme failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"删除失败: {str(e)}")


@router.post("/generate-description")
async def generate_theme_description(
    request: GenerateDescriptionRequest,
    current_user: TokenData = Depends(get_current_admin_user)
):
    """
    为新主题生成LLM描述（预览，不保存，仅管理员）
    
    权限：仅管理员
    
    说明：
    - 使用LLM生成主题描述
    - 仅返回生成的描述，不保存到数据库
    - 用户可以在创建主题前预览和编辑描述
    
    示例：
    ```
    POST /api/investment-themes/generate-description
    {
        "theme_name": "AI芯片",
        "market": "US"
    }
    ```
    
    返回：
    ```
    {
        "success": true,
        "description": "AI芯片是指专门为人工智能计算设计的...",
        "message": "描述生成成功"
    }
    ```
    """
    try:
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        logger.info(
            f"Admin {current_user.username} generating description for new theme",
            theme_name=request.theme_name,
            market=request.market
        )
        
        result = await service.generate_theme_description(
            theme_name=request.theme_name,
            market=request.market
        )
        
        if result["success"]:
            return result
        else:
            raise HTTPException(status_code=500, detail=result["message"])
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Generate description failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"生成描述失败: {str(e)}")


@router.post("/create")
async def create_theme(
    request: CreateThemeRequest,
    current_user: TokenData = Depends(get_current_admin_user)
):
    """
    创建新的投资主题（仅管理员）
    
    权限：仅管理员
    
    说明：
    - 创建新主题并保存到数据库
    - 自动生成向量
    - theme_id可选，不提供则自动生成（格式：CUSTOM_<MARKET>_<TIMESTAMP>）
    - 如果向量化失败，主题仍会保存，但会返回错误提示
    
    流程：
    1. 保存主题基本信息到 futu_investment_themes
    2. 生成向量并保存到 investment_theme_embeddings
    3. 返回创建结果
    
    示例：
    ```
    POST /api/investment-themes/create
    {
        "theme_name": "AI芯片",
        "market": "US",
        "description": "AI芯片是指专门为人工智能计算设计的半导体芯片...",
        "theme_id": "AI_CHIP_US"  // 可选
    }
    ```
    
    返回（成功）：
    ```
    {
        "success": true,
        "message": "主题 'AI芯片' 创建成功",
        "theme_id": "AI_CHIP_US",
        "theme_saved": true,
        "embedding_created": true
    }
    ```
    
    返回（主题保存成功但向量化失败）：
    ```
    {
        "success": false,
        "message": "主题已保存，但向量生成失败: API限流",
        "theme_id": "AI_CHIP_US",
        "theme_saved": true,
        "embedding_created": false,
        "error": "API限流"
    }
    ```
    """
    try:
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        logger.info(
            f"Admin {current_user.username} creating new theme",
            theme_name=request.theme_name,
            market=request.market,
            theme_id=request.theme_id
        )
        
        result = await service.create_theme(
            theme_name=request.theme_name,
            market=request.market,
            description=request.description,
            creator_username=current_user.username,
            theme_id=request.theme_id
        )
        
        # 根据结果返回不同的状态码
        if result["success"]:
            return result
        elif result.get("theme_saved"):
            # 主题保存成功但向量化失败，返回206 Partial Content
            return result
        else:
            # 完全失败
            raise HTTPException(status_code=400, detail=result["message"])
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create theme failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"创建主题失败: {str(e)}")

