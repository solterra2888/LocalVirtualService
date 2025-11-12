# -*- coding: utf-8 -*-
"""
投资主题向量化管理API
提供主题向量初始化、匹配、统计等功能
"""

import asyncio
from fastapi import APIRouter, HTTPException, BackgroundTasks, Depends, Query, UploadFile, File, Form
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import os
import tempfile
import shutil
from backend.services.investment_theme_embedding_service import InvestmentThemeEmbeddingService
from backend.services.theme_company_association_service import ThemeCompanyAssociationService
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
    top_k: int = Field(5, ge=1, le=20, description="返回Top-K个主题")
    min_similarity: float = Field(0.6, ge=0.0, le=1.0, description="最小相似度阈值")
    save_to_db: bool = Field(False, description="是否保存匹配结果到数据库")


class ThemeMatchResponse(BaseModel):
    """主题匹配响应"""
    theme_id: str
    theme_name: str
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


class FillDescriptionsRequest(BaseModel):
    """批量填充主题描述请求"""
    force_regenerate: bool = Field(False, description="whether to force regenerate existing descriptions")


class UpdateEnabledStatusRequest(BaseModel):
    """更新启用状态请求"""
    is_enabled: bool = Field(..., description="是否启用")


class BatchUpdateEnabledRequest(BaseModel):
    """批量更新启用状态请求"""
    theme_ids: List[str] = Field(..., description="主题ID列表")
    is_enabled: bool = Field(..., description="是否启用")


class UpdateDescriptionRequest(BaseModel):
    """更新主题描述请求"""
    description: str = Field(..., min_length=1, max_length=2000, description="新的描述内容")
    version: Optional[int] = Field(None, description="版本号（用于乐观锁）")


class DeleteThemeRequest(BaseModel):
    """删除主题请求"""
    pass  # theme_id将从路径参数获取


class BatchDeleteThemeRequest(BaseModel):
    """批量删除主题请求"""
    theme_ids: List[str] = Field(..., description="主题ID列表")


class GenerateDescriptionRequest(BaseModel):
    """生成主题描述请求"""
    theme_name: str = Field(..., min_length=1, max_length=200, description="主题名称")
    category: str = Field(..., min_length=1, max_length=100, description="主题分类/类别")
    market: Optional[str] = Field(None, description="市场代码（US/HK/CN）")
    tickers: Optional[List[str]] = Field(None, description="关联的股票代码列表（简写，如 ['NVDA', 'AMD']）")


class CreateThemeRequest(BaseModel):
    """创建主题请求"""
    theme_name: str = Field(..., min_length=1, max_length=200, description="主题名称")
    category: str = Field(..., min_length=1, max_length=100, description="主题分类/类别")
    description: str = Field(..., min_length=10, max_length=2000, description="主题描述")
    theme_id: Optional[str] = Field(None, max_length=50, description="主题ID（可选，不提供则自动生成）")
    market: Optional[str] = Field(None, description="市场代码（US/HK/CN）")
    tickers: Optional[List[str]] = Field(None, description="关联的股票代码列表（简写，如 ['NVDA', 'AMD']）")


class VerifyTickerRequest(BaseModel):
    """验证ticker请求"""
    theme_id: str = Field(..., description="主题ID")
    ticker: str = Field(..., description="要验证的ticker（完整格式，如 TLN.US）")


class VerifyTickerResponse(BaseModel):
    """验证ticker响应"""
    success: bool
    message: str
    ticker: str
    is_verified: bool
    company_info: Optional[Dict[str, Any]] = None
    data_updated: Optional[Dict[str, bool]] = None
    execution_time: Optional[float] = None


class ThemeAssociationResponseItem(BaseModel):
    """主题关联股票信息"""
    ticker: str
    market: Optional[str] = None
    is_verified: Optional[bool] = None
    association_weight: Optional[float] = None


class ThemeAssociationsResponse(BaseModel):
    """主题关联股票响应"""
    success: bool
    theme_id: str
    associations: List[ThemeAssociationResponseItem]
    count: int


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
            top_k=request.top_k
        )
        
        matches = await service.match_news_to_themes(
            news_text=request.news_text,
            news_id=request.news_id,
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
    - 从 investment_theme_embeddings 表读取主题
    - 调用Qwen Embedding API生成向量
    - 更新到 investment_theme_embeddings 表
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
                'similarity': float(row[2]),
                'rank': row[3],
                'match_method': row[4],
                'matched_at': row[5].isoformat() if row[5] else None
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
    search: Optional[str] = Query(None, description="Search keyword for theme name or category"),
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
        
        # 获取总数
        filters = []
        filter_params: List[Any] = []

        search_value = search.strip() if search else None
        if search_value:
            filters.append("(ite.theme_name ILIKE %s OR ite.category ILIKE %s)")
            pattern = f"%{search_value}%"
            filter_params.extend([pattern, pattern])

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

        cursor.execute(f"""
            SELECT COUNT(*) 
            FROM investment_theme_embeddings ite
            {where_clause}
        """, filter_params)
        total_count = cursor.fetchone()[0]
        
        # 获取列表（直接从 investment_theme_embeddings 查询所有字段）
        list_params = list(filter_params)
        list_params.extend([limit, offset])
        cursor.execute(f"""
            SELECT 
                ite.theme_id,
                ite.theme_name,
                ite.category,
                ite.embedding_model,
                ite.created_at,
                ite.updated_at,
                ite.version,
                ite.last_manual_edit_at,
                ite.last_manual_edit_by,
                ite.created_by,
                ite.description,
                ite.description_source,
                ite.is_enabled_for_matching
            FROM investment_theme_embeddings ite
            {where_clause}
            ORDER BY ite.theme_name
            LIMIT %s OFFSET %s
        """, list_params)
        
        themes = []
        rows = cursor.fetchall()
        for row in rows:
            themes.append({
                'theme_id': row[0],
                'theme_name': row[1],
                'category': row[2],
                'embedding_model': row[3],
                'created_at': row[4].isoformat() if row[4] else None,
                'updated_at': row[5].isoformat() if row[5] else None,
                'version': row[6],
                'last_manual_edit_at': row[7].isoformat() if row[7] else None,
                'last_manual_edit_by': row[8],
                'created_by': row[9],
                'description_preview': row[10],
                'description_source': row[11],
                'is_enabled_for_matching': row[12] if row[12] is not None else True,
                'has_embedding': True,
                'tickers': []
            })
        
        if themes:
            theme_ids = [theme['theme_id'] for theme in themes]
            cursor.execute("""
                SELECT 
                    theme_id,
                    ARRAY_AGG(ticker ORDER BY created_at DESC) AS tickers,
                    COUNT(*) FILTER (WHERE is_verified = TRUE) AS verified_count,
                    COUNT(*) AS total_count
                FROM theme_company_associations
                WHERE theme_id = ANY(%s)
                GROUP BY theme_id
            """, (theme_ids,))
            
            ticker_rows = cursor.fetchall()
            ticker_map = {
                row[0]: {
                    "tickers": [ticker for ticker in row[1] if ticker],
                    "verified_count": row[2] or 0,
                    "total_count": row[3] or 0
                }
                for row in ticker_rows
            }
            
            for theme in themes:
                counts = ticker_map.get(theme['theme_id'])
                if counts:
                    theme['tickers'] = counts["tickers"]
                    theme['verified_tickers'] = counts["verified_count"]
                    theme['total_tickers'] = counts["total_count"]
                else:
                    theme['tickers'] = []
                    theme['verified_tickers'] = 0
                    theme['total_tickers'] = 0
        
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


@router.get("/{theme_id}/associations", response_model=ThemeAssociationsResponse)
async def get_theme_associations(theme_id: str) -> ThemeAssociationsResponse:
    """
    获取主题关联的股票列表
    """
    try:
        db_config = get_database_config()
        association_service = ThemeCompanyAssociationService(db_config)
        association_records = association_service.get_associations_by_theme(theme_id)

        associations = [
            ThemeAssociationResponseItem(
                ticker=item.get("ticker"),
                market=item.get("market"),
                is_verified=item.get("is_verified"),
                association_weight=item.get("association_weight")
            )
            for item in association_records
            if item.get("ticker")
        ]

        return ThemeAssociationsResponse(
            success=True,
            theme_id=theme_id,
            associations=associations,
            count=len(associations)
        )
    except Exception as e:
        logger.error(f"Getting theme associations failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取主题关联失败: {str(e)}")


@router.post("/{theme_id}/verify-ticker", response_model=VerifyTickerResponse)
async def verify_ticker(
    theme_id: str,
    request: VerifyTickerRequest,
    current_user=Depends(get_current_admin_user)
) -> VerifyTickerResponse:
    """
    验证主题关联的ticker，如果数据库中不存在则从EODHD获取数据
    
    验证逻辑：
    1. 检查ticker是否在companies表中（通过primaryticker字段）
    2. 如果不存在，尝试从EODHD获取fundamentals数据
    3. 如果EODHD有数据，保存到companies表，并更新theme_company_associations的is_verified为true
    4. 如果EODHD没有数据，is_verified保持false，提示用户ticker可能不正确
    """
    import time
    start_time = time.time()
    
    try:
        from backend.database.db_manager import DatabaseManager
        from backend.services.eodhd_fundamentals_service import EODHDFundamentalsService
        from backend.config.exchange_config import format_ticker_for_eodhd
        
        ticker = request.ticker.strip()
        
        logger.info(f"开始验证ticker: {ticker} (theme_id: {theme_id})",
                   user=current_user.username if hasattr(current_user, 'username') else None)
        
        # 获取数据库连接
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        # 1. 首先检查ticker是否在companies表中
        check_sql = """
        SELECT id, code, name, exchange, primaryticker
        FROM companies
        WHERE UPPER(primaryticker) = UPPER(%s)
        LIMIT 1
        """
        
        with db_manager.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(check_sql, (ticker,))
            company_result = cursor.fetchone()
        
        # 如果在数据库中找到了
        if company_result:
            logger.info(f"Ticker {ticker} 在数据库中找到")
            
            # 更新theme_company_associations的is_verified状态为true
            update_sql = """
            UPDATE theme_company_associations
            SET is_verified = TRUE,
                company_name = %s,
                updated_at = NOW()
            WHERE theme_id = %s AND ticker = %s
            """
            
            with db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(update_sql, (company_result[2], theme_id, ticker))
                conn.commit()
            
            execution_time = time.time() - start_time
            
            return VerifyTickerResponse(
                success=True,
                message=f"Ticker {ticker} 验证成功（数据库中已存在）",
                ticker=ticker,
                is_verified=True,
                company_info={
                    "id": company_result[0],
                    "code": company_result[1],
                    "name": company_result[2],
                    "exchange": company_result[3],
                    "primaryticker": company_result[4]
                },
                execution_time=execution_time
            )
        
        # 2. 数据库中未找到，尝试从EODHD获取
        logger.info(f"数据库中未找到ticker {ticker}，尝试从EODHD获取")
        
        # 解析ticker格式
        formatted_ticker = format_ticker_for_eodhd(ticker)
        
        if '.' in formatted_ticker:
            symbol, exchange = formatted_ticker.rsplit('.', 1)
        else:
            symbol = formatted_ticker
            exchange = 'US'
        
        logger.info(f"尝试从EODHD获取: {symbol}.{exchange}")
        
        # 初始化EODHD服务
        fundamentals_service = EODHDFundamentalsService()
        
        # 从EODHD获取数据
        full_data = fundamentals_service.fetch_company_full_data(symbol, exchange)
        
        if not full_data:
            # EODHD也没有数据
            execution_time = time.time() - start_time
            
            logger.warning(f"EODHD也没有ticker {ticker} 的数据")
            
            return VerifyTickerResponse(
                success=False,
                message=f"未找到ticker {ticker} 的数据。数据库和EODHD均无此ticker，请确认ticker是否正确。",
                ticker=ticker,
                is_verified=False,
                execution_time=execution_time
            )
        
        # 3. EODHD有数据，保存到数据库
        logger.info(f"成功从EODHD获取 {symbol}.{exchange} 的数据，开始保存")
        
        # 构建保存数据的格式
        company_data = {
            'symbol': symbol,
            'exchange': exchange,
            'general_data': full_data.get('General', full_data),
            'address_data': full_data.get('General', full_data).get('AddressData') if full_data.get('General') else None,
            'officers_data': fundamentals_service._extract_officers_list(full_data.get('General', full_data).get('Officers', {})) if full_data.get('General') else [],
            'listings_data': full_data.get('General', full_data).get('Listings') if full_data.get('General') else None,
            'full_data': full_data
        }
        
        # 保存到数据库
        save_result = fundamentals_service.batch_save_companies_data([company_data])
        
        # 4. 更新theme_company_associations的is_verified状态
        general_data = full_data.get('General', full_data)
        company_name = general_data.get('Name', symbol)
        
        update_sql = """
        UPDATE theme_company_associations
        SET is_verified = TRUE,
            company_name = %s,
            updated_at = NOW()
        WHERE theme_id = %s AND ticker = %s
        """
        
        with db_manager.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(update_sql, (company_name, theme_id, ticker))
            conn.commit()
        
        # 构建响应数据
        data_updated = {
            'company_info': save_result.get('companies_saved', 0) > 0,
            'address': save_result.get('addresses_saved', 0) > 0,
            'officers': save_result.get('officers_saved', 0) > 0,
            'financials': save_result.get('financials_saved', 0) > 0,
            'highlights': save_result.get('highlights_saved', 0) > 0,
            'valuations': save_result.get('valuations_saved', 0) > 0,
            'technicals': save_result.get('technicals_saved', 0) > 0,
            'earnings': save_result.get('earnings_saved', 0) > 0,
            'outstanding_shares': save_result.get('outstanding_shares_saved', 0) > 0,
            'analyst_ratings': save_result.get('analyst_ratings_saved', 0) > 0,
            'splits_dividends': save_result.get('splits_dividends_saved', 0) > 0,
            'insider_transactions': save_result.get('insider_transactions_saved', 0) > 0
        }
        
        execution_time = time.time() - start_time
        
        updated_components = [k for k, v in data_updated.items() if v]
        success_message = f"成功从EODHD获取并保存 {company_name} ({ticker}) 的数据"
        if updated_components:
            success_message += f"，保存了 {len(updated_components)} 个数据组件"
        
        logger.info(f"完成ticker验证和数据保存: {ticker}",
                   execution_time=execution_time,
                   user=current_user.username if hasattr(current_user, 'username') else None)
        
        return VerifyTickerResponse(
            success=True,
            message=success_message,
            ticker=ticker,
            is_verified=True,
            company_info={
                'code': symbol,
                'exchange': exchange,
                'name': company_name,
                'primaryticker': ticker
            },
            data_updated=data_updated,
            execution_time=execution_time
        )
        
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"验证ticker失败: {str(e)}",
                    ticker=ticker,
                    theme_id=theme_id,
                    execution_time=execution_time)
        
        return VerifyTickerResponse(
            success=False,
            message=f"验证ticker失败: {str(e)}",
            ticker=ticker,
            is_verified=False,
            execution_time=execution_time
        )


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


# /fetch-futu-themes endpoint 已删除
# 原因：移除 futu 爬虫相关功能，改为纯手动主题管理
# 用户应通过 /create endpoint 手动创建主题


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
            is_enabled=request.is_enabled
        )
        
        result = service.update_theme_enabled_status(
            theme_id=theme_id,
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
        "is_enabled": false
    }
    ```
    """
    try:
        if not request.theme_ids:
            raise HTTPException(
                status_code=400,
                detail="主题ID列表不能为空"
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
        
        for theme_id in request.theme_ids:
            result = service.update_theme_enabled_status(
                theme_id=theme_id,
                is_enabled=request.is_enabled,
                operator_username=current_user.username
            )
            
            results.append({
                "theme_id": theme_id,
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
            description_length=len(request.description)
        )
        
        result = await service.update_theme_description(
            theme_id=theme_id,
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
    current_user: TokenData = Depends(get_current_admin_user)
):
    """
    删除投资主题（仅管理员，物理删除）
    
    权限：仅管理员
    
    说明：
    - 硬删除主题（物理删除，不可恢复）
    - 级联删除相关数据：
      * 向量数据（investment_theme_embeddings）
      * 新闻匹配记录（news_theme_matches）
    - ⚠️ 警告：此操作不可逆！
    
    示例：
    ```
    DELETE /api/investment-themes/BK1001
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
            theme_id=theme_id
        )
        
        result = service.delete_theme(
            theme_id=theme_id,
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


@router.post("/batch-delete")
async def batch_delete_themes(
    request: BatchDeleteThemeRequest,
    current_user: TokenData = Depends(get_current_admin_user)
):
    """
    批量删除投资主题（仅管理员，物理删除）
    
    权限：仅管理员
    
    说明：
    - 硬删除多个主题（物理删除，不可恢复）
    - 批量删除相关数据：
      * 向量数据（investment_theme_embeddings）
      * 新闻匹配记录（news_theme_matches）
    - ⚠️ 警告：此操作不可逆！
    
    示例：
    ```
    POST /api/investment-themes/batch-delete
    {
        "theme_ids": ["BK1001", "US_TECH"]
    }
    ```
    
    返回：
    ```
    {
        "success": true,
        "message": "成功批量删除 2 个主题，删除 156 条匹配记录",
        "deleted_count": 2,
        "failed_count": 0,
        "total_matches_deleted": 156,
        "details": [...]
    }
    ```
    """
    try:
        db_config = get_database_config()
        service = InvestmentThemeEmbeddingService(db_config)
        
        logger.info(
            f"Admin {current_user.username} batch deleting {len(request.theme_ids)} themes",
            theme_ids=request.theme_ids
        )
        
        result = service.batch_delete_themes(
            theme_keys=request.theme_ids,
            operator_username=current_user.username
        )
        
        if result["success"] or result["deleted_count"] > 0:
            return result
        else:
            raise HTTPException(status_code=400, detail=result["message"])
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Batch delete themes failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"批量删除失败: {str(e)}")


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
        "theme_name": "AI芯片"
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
            category=request.category,
            market=request.market,
            tickers=request.tickers
        )
        
        # 如果提供了 tickers 和 market，验证并获取公司信息
        stock_symbols = None
        company_names = None
        
        if request.tickers and request.market:
            try:
                from backend.services.theme_company_association_service import ThemeCompanyAssociationService
                
                association_service = ThemeCompanyAssociationService(db_config)
                validation_result = association_service.validate_and_complete_tickers(
                    request.tickers,
                    request.market
                )
                
                if validation_result["valid"]:
                    stock_symbols = ", ".join([v["full_ticker"] for v in validation_result["valid"]])
                    company_names = ", ".join([v["company_name"] for v in validation_result["valid"]])
                    logger.info(f"验证通过的股票: {stock_symbols}")
                    
            except Exception as e:
                logger.warning(f"验证股票代码失败: {str(e)}")
                # 继续执行，不影响描述生成
        
        result = await service.generate_theme_description(
            theme_name=request.theme_name,
            category=request.category,
            stock_symbols=stock_symbols,
            company_names=company_names
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
    - theme_id可选，不提供则自动生成（格式：CUSTOM_<TIMESTAMP>）
    - 如果向量化失败，主题仍会保存，但会返回错误提示
    
    流程：
    1. 生成向量
    2. 保存主题信息和向量到 investment_theme_embeddings
    3. 返回创建结果
    
    示例：
    ```
    POST /api/investment-themes/create
    {
        "theme_name": "AI芯片",
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
            category=request.category,
            theme_id=request.theme_id,
            market=request.market,
            tickers=request.tickers
        )
        
        result = await service.create_theme(
            theme_name=request.theme_name,
            category=request.category,
            description=request.description,
            creator_username=current_user.username,
            theme_id=request.theme_id,
            market=request.market,
            tickers=request.tickers
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


@router.post("/batch-upload")
async def batch_upload_themes(
    file: UploadFile = File(...),
    market: str = Form("US"),
    current_user: TokenData = Depends(get_current_admin_user)
):
    """
    批量上传主题文件（仅管理员）
    
    权限：仅管理员
    
    核心逻辑（重要）：
    - theme 列必须只有一个唯一值（整个文件）
    - 按 (theme, category) 组合分组，每个组合创建一个主题
    - 例如：crypto.csv 中 theme='crypto'，8 个不同 category → 创建 8 个主题
    
    Args:
        file: CSV或Excel文件
        market: 市场选择（US/HK/CN），用户在前端选择
    
    文件格式要求：
    - 必需列：theme, category, ticker
    - 可选列：company
    - theme 列必须只有一个唯一值
    - category 可以有多个不同值
    
    文件格式示例（crypto.csv）:
    ```csv
    theme,category,ticker,company
    crypto,ETF,ARKB,ARK 21Shares Bitcoin ETF
    crypto,ETF,GBTC,Grayscale Bitcoin Trust (BTC)
    crypto,Treasury,MSTR,MicroStrategy Inc
    crypto,Treasury,MARA,Marathon Holdings Inc
    ```
    
    说明：上述文件会创建 2 个主题：
    1. Theme: crypto, Category: ETF, Tickers: ARKB,GBTC
    2. Theme: crypto, Category: Treasury, Tickers: MSTR,MARA
    
    示例：
    ```bash
    curl -X POST "http://localhost:8000/api/investment-themes/batch-upload" \\
      -H "Authorization: Bearer YOUR_TOKEN" \\
      -F "file=@crypto.csv" \\
      -F "market=US"
    ```
    
    返回：
    ```json
    {
        "success": true,
        "message": "批量上传任务已提交，共 8 个主题（8 个 category）",
        "task_id": "abc-123-def-456",
        "theme_name": "crypto",
        "preview": {
            "theme_name": "crypto",
            "total_themes": 8,
            "total_tickers": 30,
            "categories": {"ETF": 4, "Treasury": 9, ...},
            "categories_sample": ["ETF", "Treasury", "Exchange_Custody", ...]
        },
        "status_url": "/api/investment-themes/task-status/abc-123-def-456"
    }
    ```
    """
    try:
        logger.info(
            f"Admin {current_user.username} uploading batch theme file: {file.filename}, market: {market}"
        )
        
        # 1. 验证 market 参数
        if market not in ['US', 'HK', 'CN']:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid market: {market}. Must be US, HK, or CN."
            )
        
        # 2. 验证文件格式
        if not file.filename:
            raise HTTPException(status_code=400, detail="File name is empty")
        
        file_ext = file.filename.lower().split('.')[-1]
        if file_ext not in ['csv', 'xlsx', 'xls']:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported file format: .{file_ext}. Only .csv, .xlsx, .xls are supported."
            )
        
        # 3. 解析文件
        from backend.utils.theme_file_parser import ThemeFileParser
        
        parser = ThemeFileParser()
        
        # 重置文件指针
        await file.seek(0)
        
        parse_result = parser.parse_file(file, market=market)
        
        # 4. 如果解析失败，返回错误
        if not parse_result.get('success'):
            error_msg = parse_result.get('error', 'Unknown error')
            logger.error(f"File parsing failed: {error_msg}")
            raise HTTPException(
                status_code=400,
                detail=f"File parsing failed: {error_msg}"
            )
        
        theme_name = parse_result['theme_name']
        themes_data = parse_result['data']
        preview = parse_result['preview']
        
        logger.info(
            f"File parsed successfully: theme='{theme_name}', "
            f"{preview['total_themes']} categories (themes), "
            f"{preview['total_tickers']} tickers"
        )
        
        # 5. 提交 Celery 任务
        from backend.tasks.theme_tasks import batch_create_themes_task
        
        task = batch_create_themes_task.apply_async(
            kwargs={
                'themes_data': themes_data,
                'creator_username': current_user.username
            }
        )
        
        logger.info(f"Batch create themes task submitted: {task.id}")
        
        # 6. 返回结果
        return {
            "success": True,
            "message": f"Batch upload started: {preview['total_themes']} themes ({preview['total_themes']} categories)",
            "task_id": task.id,
            "theme_name": theme_name,
            "preview": preview,
            "status_url": f"/api/investment-themes/task-status/{task.id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Batch upload failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Batch upload failed: {str(e)}")

