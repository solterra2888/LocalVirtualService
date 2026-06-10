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
from backend.auth.jwt_utils import get_current_admin_user, get_current_user, TokenData

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
    category: Optional[str] = None
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
    group: Optional[str] = Field(None, max_length=100, description="主题分组（可选）")
    version: Optional[int] = Field(None, description="版本号（用于乐观锁）")


class DeleteThemeRequest(BaseModel):
    """删除主题请求"""
    pass  # theme_id将从路径参数获取


class BatchDeleteThemeRequest(BaseModel):
    """批量删除主题请求"""
    theme_ids: List[str] = Field(..., description="主题ID列表")


class BatchShareThemeRequest(BaseModel):
    """批量更新主题分享状态请求"""
    theme_ids: List[str] = Field(..., description="主题ID列表")
    is_shared: bool = Field(..., description="是否公开分享")


class GenerateDescriptionRequest(BaseModel):
    """生成主题描述请求"""
    group: Optional[str] = Field(None, max_length=100, description="主题分组（如 AI、crypto 等，可选）")
    theme_name: str = Field(..., min_length=1, max_length=200, description="主题名称")
    category: str = Field(..., min_length=1, max_length=100, description="主题分类/类别")
    market: Optional[str] = Field(None, description="默认市场代码（US/HK/CN），仅用于没有后缀的ticker")
    tickers: Optional[List[str]] = Field(None, description="关联的股票代码列表，支持完整格式（如 'NVDA.US', '688256.SHG'）或简写（如 'NVDA'）")


class CreateThemeRequest(BaseModel):
    """创建主题请求"""
    group: Optional[str] = Field(None, max_length=100, description="主题分组（如 AI、crypto 等，可选）")
    theme_name: str = Field(..., min_length=1, max_length=200, description="主题名称")
    category: str = Field(..., min_length=1, max_length=100, description="主题分类/类别")
    description: str = Field(..., min_length=10, max_length=2000, description="主题描述")
    theme_id: Optional[str] = Field(None, max_length=50, description="主题ID（可选，不提供则自动生成）")
    market: Optional[str] = Field(None, description="默认市场代码（US/HK/CN），仅用于没有后缀的ticker")
    tickers: Optional[List[str]] = Field(None, description="关联的股票代码列表，支持完整格式（如 'NVDA.US', '688256.SHG'）或简写（如 'NVDA'），可混合不同交易所")


class UpdateThemeTickersRequest(BaseModel):
    """更新主题tickers并重新生成描述和向量的请求"""
    theme_id: str = Field(..., description="主题ID")
    tickers: List[str] = Field(..., description="最终选择的股票代码列表")
    theme_name: str = Field(..., description="主题名称")
    category: str = Field(..., description="主题分类")
    group: Optional[str] = Field(None, description="主题分组")
    market: Optional[str] = Field(None, description="默认市场代码")


class UpdateThemeWithConflictResolutionRequest(BaseModel):
    """冲突解决后更新主题的请求"""
    theme_name: str = Field(..., description="主题名称")
    category: str = Field(..., description="主题分类")
    description: str = Field(..., description="当前主题描述")
    selected_tickers: List[str] = Field(..., description="用户选择的最终 ticker 列表（已标准化，带市场后缀）")
    group: Optional[str] = Field(None, description="主题分组")


class BatchResolveConflictsRequest(BaseModel):
    """批量解决冲突的请求"""
    conflict_resolutions: Dict[str, UpdateThemeWithConflictResolutionRequest] = Field(
        ..., 
        description="冲突解决方案字典，key 为 theme_id，value 为解决方案"
    )


class VerifyTickerRequest(BaseModel):
    """验证ticker请求"""
    theme_id: str = Field(..., description="主题ID")
    ticker: str = Field(..., description="要验证的ticker（完整格式，如 TLN.US）")


class UpdateShareStatusRequest(BaseModel):
    """更新分享状态请求"""
    is_shared: bool = Field(..., description="是否公开分享")


class BatchTagRequest(BaseModel):
    """批量打标签请求"""
    theme_ids: List[str] = Field(..., description="主题ID列表")
    tags: List[str] = Field(..., description="标签名称列表")
    action: str = Field(..., description="操作类型：add 或 remove")


class AddTagToThemeRequest(BaseModel):
    """为主题添加标签请求"""
    tag_name: str = Field(..., min_length=1, max_length=100, description="标签名称")


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




@router.get("/themes")
async def list_all_themes(
    market: Optional[str] = None,
    search: Optional[str] = Query(None, description="Search keyword for theme name or category"),
    group: Optional[List[str]] = Query(None, description="Filter by group name(s) - multiple selection supported"),
    created_by: Optional[List[str]] = Query(None, description="Filter by creator username(s) - multiple selection supported (admin only)"),
    show_shared: bool = Query(True, description="是否显示已分享的themes（非admin用户）"),
    limit: int = 100,
    offset: int = 0,
    current_user: TokenData = Depends(get_current_user)
):
    """
    列出所有投资主题
    
    权限：
    - admin用户：可以查看所有主题
    - 非admin用户：可以查看自己创建的主题 + 已分享的主题(is_shared=true)
    
    参数：
    - market: 市场过滤（HK/US/CN）
    - search: 搜索关键词（主题名称或类别）
    - group: 按组名筛选（支持多个，逗号分隔或多次传递）
    - created_by: 按创建者用户名筛选（支持多个，仅admin用户可用）
    - show_shared: 是否显示已分享的themes（非admin用户，默认true）
    - limit: 每页数量
    - offset: 偏移量
    
    示例：
    ```
    GET /api/investment-themes/themes?market=US&limit=20&group=AI&group=crypto&created_by=user1&created_by=user2&show_shared=true
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

        # 权限和筛选逻辑
        if not current_user.is_admin:
            # 非admin用户：自己创建的 OR 已分享的
            if show_shared:
                filters.append("(ite.created_by = %s OR ite.is_shared = TRUE)")
                filter_params.append(current_user.username)
            else:
                # 只看自己创建的
                filters.append("ite.created_by = %s")
                filter_params.append(current_user.username)
        else:
            # admin用户：如果选择"仅我创建的"，则只显示自己创建的
            if not show_shared:
                filters.append("ite.created_by = %s")
                filter_params.append(current_user.username)

        search_value = search.strip() if search else None
        if search_value:
            filters.append("(ite.theme_name ILIKE %s OR ite.category ILIKE %s)")
            pattern = f"%{search_value}%"
            filter_params.extend([pattern, pattern])
        
        # 🔥 添加 group (tags) 筛选（支持多个 tag）
        # 现在基于用户个性化标签筛选
        if group and len(group) > 0:
            # 过滤掉空字符串
            valid_tags = [g.strip() for g in group if g and g.strip()]
            if valid_tags:
                # 筛选拥有这些标签的主题（当前用户的标签）
                placeholders = ','.join(['%s'] * len(valid_tags))
                filters.append(f"""
                    EXISTS (
                        SELECT 1 FROM theme_tags tt
                        WHERE tt.theme_id = ite.theme_id
                          AND tt.user_id = %s
                          AND tt.tag_name IN ({placeholders})
                    )
                """)
                filter_params.append(current_user.username)
                filter_params.extend(valid_tags)
        
        # 添加 created_by 筛选（支持多个用户，仅admin用户可用）
        if created_by and len(created_by) > 0 and current_user.is_admin:
            # 过滤掉空字符串
            valid_users = [u.strip() for u in created_by if u and u.strip()]
            if valid_users:
                placeholders = ','.join(['%s'] * len(valid_users))
                filters.append(f"ite.created_by IN ({placeholders})")
                filter_params.extend(valid_users)

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
                ite.is_enabled_for_matching,
                ite."group",
                ite.is_shared,
                ite.shared_at,
                ite.shared_by
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
                'group': row[13],
                'is_shared': row[14] if row[14] is not None else False,
                'shared_at': row[15].isoformat() if row[15] else None,
                'shared_by': row[16],
                'has_embedding': True,
                'tickers': [],
                # 权限标记
                'can_edit': current_user.is_admin or row[9] == current_user.username,
                'can_delete': current_user.is_admin or row[9] == current_user.username,
                'can_share': current_user.is_admin or row[9] == current_user.username,
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
            
            # 🔥 查询主题的标签（只返回当前用户的标签）
            cursor.execute("""
                SELECT theme_id, ARRAY_AGG(tag_name ORDER BY created_at) AS tags
                FROM theme_tags
                WHERE theme_id = ANY(%s)
                  AND user_id = %s
                GROUP BY theme_id
            """, (theme_ids, current_user.username))
            
            tags_rows = cursor.fetchall()
            tags_map = {row[0]: [tag for tag in row[1] if tag] for row in tags_rows}
            
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
                
                # 🔥 添加标签列表
                theme['tags'] = tags_map.get(theme['theme_id'], [])
        
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


@router.get("/groups")
async def get_all_groups():
    """
    获取所有唯一的 group 值（用于筛选器）
    
    返回所有 investment_theme_embeddings 表中唯一的 group 值列表
    
    示例：
    ```
    GET /api/investment-themes/groups
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
        
        # 查询所有唯一的非空 group 值
        cursor.execute("""
            SELECT DISTINCT "group"
            FROM investment_theme_embeddings
            WHERE "group" IS NOT NULL AND "group" != ''
            ORDER BY "group"
        """)
        
        groups = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "groups": groups,
            "count": len(groups)
        }
        
    except Exception as e:
        logger.error(f"Getting groups list failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Getting groups failed: {str(e)}")


@router.get("/users")
async def get_all_users(
    current_user: TokenData = Depends(get_current_admin_user)
):
    """
    获取所有唯一的 created_by 用户值（用于筛选器，仅admin用户可用）
    
    返回所有 investment_theme_embeddings 表中唯一的 created_by 值列表
    
    示例：
    ```
    GET /api/investment-themes/users
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
        
        # 查询所有唯一的非空 created_by 值
        cursor.execute("""
            SELECT DISTINCT created_by
            FROM investment_theme_embeddings
            WHERE created_by IS NOT NULL AND created_by != ''
            ORDER BY created_by
        """)
        
        users = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "users": users,
            "count": len(users)
        }
        
    except Exception as e:
        logger.error(f"Getting users failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Getting users failed: {str(e)}")


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
                # 🔥 统一格式：将所有 meta 信息放在 meta 字段中
                response["meta"] = task_result.info
        elif task_result.state == 'SUCCESS':
            response["progress"] = 100
            response["message"] = "Task completed successfully"
            response["result"] = task_result.result
            # 🔥 统一格式：在 SUCCESS 状态也包含 meta 字段（从 result 中提取统计信息）
            if isinstance(task_result.result, dict):
                response["meta"] = {
                    "progress": 100,
                    "status": "completed",
                    **task_result.result.get('statistics', {})
                }
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
    current_user: TokenData = Depends(get_current_user)
):
    """
    更新主题描述并重新向量化
    
    权限：
    - admin用户：可以编辑所有主题
    - 非admin用户：只能编辑自己创建的主题
    
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
        from backend.database.db_manager import DatabaseManager
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        # 非admin用户需要检查是否是主题的创建者
        if not current_user.is_admin:
            conn = db_manager.get_connection()
            if not conn:
                raise HTTPException(status_code=500, detail="Database connection failed")
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT created_by FROM investment_theme_embeddings 
                WHERE theme_id = %s
            """, (theme_id,))
            result_row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not result_row:
                raise HTTPException(status_code=404, detail="主题不存在")
            
            if result_row[0] != current_user.username:
                raise HTTPException(
                    status_code=403, 
                    detail="权限不足：只能编辑自己创建的主题"
                )
        
        service = InvestmentThemeEmbeddingService(db_config)
        
        logger.info(
            f"User {current_user.username} updating theme description",
            theme_id=theme_id,
            description_length=len(request.description),
            is_admin=current_user.is_admin
        )
        
        result = await service.update_theme_description(
            theme_id=theme_id,
            new_description=request.description,
            editor_username=current_user.username,
            new_group=request.group,
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
    current_user: TokenData = Depends(get_current_user)
):
    """
    删除投资主题（物理删除）
    
    权限：
    - admin用户：可以删除所有主题
    - 非admin用户：只能删除自己创建的主题
    
    说明：
    - 硬删除主题（物理删除，不可恢复）
    - 级联删除相关数据：
      * 向量数据（investment_theme_embeddings）
      * 主题-公司关联（theme_company_associations）
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
        from backend.database.db_manager import DatabaseManager
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        # 非admin用户需要检查是否是主题的创建者
        if not current_user.is_admin:
            conn = db_manager.get_connection()
            if not conn:
                raise HTTPException(status_code=500, detail="Database connection failed")
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT created_by FROM investment_theme_embeddings 
                WHERE theme_id = %s
            """, (theme_id,))
            result_row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not result_row:
                raise HTTPException(status_code=404, detail="主题不存在")
            
            if result_row[0] != current_user.username:
                raise HTTPException(
                    status_code=403, 
                    detail="权限不足：只能删除自己创建的主题"
                )
        
        service = InvestmentThemeEmbeddingService(db_config)
        
        logger.info(
            f"User {current_user.username} deleting theme",
            theme_id=theme_id,
            is_admin=current_user.is_admin
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


@router.patch("/{theme_id}/share")
async def update_theme_share_status(
    theme_id: str,
    request: UpdateShareStatusRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    更新主题的分享状态
    
    权限：
    - Admin：可以分享/取消分享所有主题
    - 普通用户：只能分享/取消分享自己创建的主题
    
    说明：
    - is_shared=true: 主题对所有用户可见和可用（但只有创建者和admin可编辑）
    - is_shared=false: 主题仅对创建者和admin可见
    
    示例：
    ```
    PATCH /api/investment-themes/BK1001/share
    {
        "is_shared": true
    }
    ```
    """
    try:
        from backend.database.db_manager import DatabaseManager
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        # 非admin用户需要检查是否是主题的创建者
        if not current_user.is_admin:
            conn = db_manager.get_connection()
            if not conn:
                raise HTTPException(status_code=500, detail="Database connection failed")
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT created_by FROM investment_theme_embeddings 
                WHERE theme_id = %s
            """, (theme_id,))
            result_row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not result_row:
                raise HTTPException(status_code=404, detail="主题不存在")
            
            if result_row[0] != current_user.username:
                raise HTTPException(
                    status_code=403, 
                    detail="权限不足：只能分享自己创建的主题"
                )
        
        # 更新分享状态
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor()
        
        if request.is_shared:
            # 设置为分享
            cursor.execute("""
                UPDATE investment_theme_embeddings
                SET is_shared = TRUE,
                    shared_at = NOW(),
                    shared_by = %s,
                    updated_at = NOW()
                WHERE theme_id = %s
            """, (current_user.username, theme_id))
        else:
            # 取消分享
            cursor.execute("""
                UPDATE investment_theme_embeddings
                SET is_shared = FALSE,
                    shared_at = NULL,
                    shared_by = NULL,
                    updated_at = NOW()
                WHERE theme_id = %s
            """, (theme_id,))
        
        if cursor.rowcount == 0:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="主题不存在")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(
            f"User {current_user.username} updated share status for theme {theme_id}",
            is_shared=request.is_shared,
            is_admin=current_user.is_admin
        )
        
        return {
            "success": True,
            "message": f"主题{'已分享' if request.is_shared else '已取消分享'}",
            "theme_id": theme_id,
            "is_shared": request.is_shared
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update theme share status failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"更新失败: {str(e)}")


@router.post("/batch-share")
async def batch_update_theme_share_status(
    request: BatchShareThemeRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    批量更新主题的分享状态

    权限：
    - Admin：可以分享/取消分享所有主题
    - 普通用户：只能分享/取消分享自己创建的主题

    示例：
    ```
    POST /api/investment-themes/batch-share
    {
        "theme_ids": ["BK1001", "US_TECH"],
        "is_shared": true
    }
    ```
    """
    try:
        from backend.database.db_manager import DatabaseManager

        if not request.theme_ids:
            raise HTTPException(status_code=400, detail="主题ID列表不能为空")

        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)

        if not current_user.is_admin:
            conn = db_manager.get_connection()
            if not conn:
                raise HTTPException(status_code=500, detail="Database connection failed")

            cursor = conn.cursor()
            placeholders = ','.join(['%s'] * len(request.theme_ids))
            cursor.execute(f"""
                SELECT theme_id, created_by
                FROM investment_theme_embeddings
                WHERE theme_id IN ({placeholders})
            """, request.theme_ids)

            theme_creators = {row[0]: row[1] for row in cursor.fetchall()}
            cursor.close()
            conn.close()

            unauthorized_themes = [
                tid for tid in request.theme_ids
                if theme_creators.get(tid) != current_user.username
            ]

            if unauthorized_themes:
                raise HTTPException(
                    status_code=403,
                    detail=f"权限不足：只能分享自己创建的主题: {', '.join(unauthorized_themes)}"
                )

        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")

        cursor = conn.cursor()
        placeholders = ','.join(['%s'] * len(request.theme_ids))

        if request.is_shared:
            cursor.execute(f"""
                UPDATE investment_theme_embeddings
                SET is_shared = TRUE,
                    shared_at = NOW(),
                    shared_by = %s,
                    updated_at = NOW()
                WHERE theme_id IN ({placeholders})
            """, (current_user.username, *request.theme_ids))
        else:
            cursor.execute(f"""
                UPDATE investment_theme_embeddings
                SET is_shared = FALSE,
                    shared_at = NULL,
                    shared_by = NULL,
                    updated_at = NOW()
                WHERE theme_id IN ({placeholders})
            """, (*request.theme_ids,))

        updated_count = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()

        action_text = "分享" if request.is_shared else "取消分享"
        logger.info(
            f"User {current_user.username} batch {action_text} {updated_count} themes",
            theme_ids=request.theme_ids,
            is_shared=request.is_shared,
            is_admin=current_user.is_admin
        )

        return {
            "success": True,
            "message": f"成功{action_text} {updated_count} 个主题",
            "updated_count": updated_count,
            "is_shared": request.is_shared
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Batch update theme share status failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"批量更新失败: {str(e)}")


@router.post("/batch-delete")
async def batch_delete_themes(
    request: BatchDeleteThemeRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    批量删除投资主题（物理删除）
    
    权限：
    - admin用户：可以删除所有主题
    - 非admin用户：只能删除自己创建的主题
    
    说明：
    - 硬删除多个主题（物理删除，不可恢复）
    - 批量删除相关数据：
      * 向量数据（investment_theme_embeddings）
      * 主题-公司关联（theme_company_associations）
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
        from backend.database.db_manager import DatabaseManager
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        # 非admin用户需要检查每个主题是否是自己创建的
        if not current_user.is_admin:
            conn = db_manager.get_connection()
            if not conn:
                raise HTTPException(status_code=500, detail="Database connection failed")
            
            cursor = conn.cursor()
            placeholders = ','.join(['%s'] * len(request.theme_ids))
            cursor.execute(f"""
                SELECT theme_id, created_by 
                FROM investment_theme_embeddings 
                WHERE theme_id IN ({placeholders})
            """, request.theme_ids)
            
            theme_creators = {row[0]: row[1] for row in cursor.fetchall()}
            cursor.close()
            conn.close()
            
            # 检查是否有不属于当前用户的主题
            unauthorized_themes = [
                tid for tid in request.theme_ids 
                if theme_creators.get(tid) != current_user.username
            ]
            
            if unauthorized_themes:
                raise HTTPException(
                    status_code=403, 
                    detail=f"权限不足：无法删除以下主题（不是您创建的）: {', '.join(unauthorized_themes)}"
                )
        
        service = InvestmentThemeEmbeddingService(db_config)
        
        logger.info(
            f"User {current_user.username} batch deleting {len(request.theme_ids)} themes",
            theme_ids=request.theme_ids,
            is_admin=current_user.is_admin
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
    current_user: TokenData = Depends(get_current_user)
):
    """
    为新主题生成LLM描述（预览，不保存）
    
    权限：所有登录用户
    
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
            f"User {current_user.username} generating description for new theme",
            theme_name=request.theme_name,
            category=request.category,
            market=request.market,
            tickers=request.tickers,
            is_admin=current_user.is_admin
        )
        
        # 如果提供了 tickers，验证并获取公司信息
        stock_symbols = None
        company_names = None
        
        if request.tickers:
            try:
                from backend.services.theme_company_association_service import ThemeCompanyAssociationService
                
                association_service = ThemeCompanyAssociationService(db_config)
                
                # 使用默认市场或US（仅用于没有后缀的ticker）
                default_market = request.market if request.market else 'US'
                
                validation_result = association_service.validate_and_complete_tickers(
                    request.tickers,
                    default_market
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
    current_user: TokenData = Depends(get_current_user)
):
    """
    创建新的投资主题
    
    权限：所有登录用户
    
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
            f"User {current_user.username} creating new theme",
            group=request.group,
            theme_name=request.theme_name,
            category=request.category,
            theme_id=request.theme_id,
            market=request.market,
            tickers=request.tickers,
            is_admin=current_user.is_admin
        )
        
        result = await service.create_theme(
            theme_name=request.theme_name,
            category=request.category,
            description=request.description,
            creator_username=current_user.username,
            group=request.group,
            theme_id=request.theme_id,
            market=request.market,
            tickers=request.tickers
        )
        
        # 🔥 自动验证新创建的 tickers（如果有）
        verified_count = 0
        if result.get("theme_saved") and result.get("associations_created", 0) > 0 and request.tickers:
            theme_id = result.get("theme_id")
            logger.info(f"🔍 Auto-verifying {len(request.tickers)} tickers for new theme {theme_id}...")
            
            try:
                from backend.database.db_manager import DatabaseManager
                from backend.services.eodhd_fundamentals_service import EODHDFundamentalsService
                from backend.config.exchange_config import format_ticker_for_eodhd
                
                db_config = get_database_config()
                db_manager = DatabaseManager(db_config)
                
                for ticker in request.tickers:
                    try:
                        # 检查ticker是否在companies表中
                        verify_conn = db_manager.get_connection()
                        verify_cursor = verify_conn.cursor()
                        
                        verify_cursor.execute("""
                            SELECT id, code, name, exchange, primaryticker
                            FROM companies
                            WHERE UPPER(primaryticker) = UPPER(%s)
                            LIMIT 1
                        """, (ticker,))
                        
                        company_result = verify_cursor.fetchone()
                        
                        if company_result:
                            # 在数据库中找到，更新is_verified
                            verify_cursor.execute("""
                                UPDATE theme_company_associations
                                SET is_verified = TRUE,
                                    company_name = %s,
                                    updated_at = NOW()
                                WHERE theme_id = %s AND ticker = %s
                            """, (company_result[2], theme_id, ticker))
                            verify_conn.commit()
                            verified_count += 1
                            logger.debug(f"✅ Ticker {ticker} verified (found in DB)")
                        else:
                            # 不在数据库中，尝试从EODHD获取
                            logger.debug(f"🔍 Ticker {ticker} not in DB, fetching from EODHD...")
                            
                            formatted_ticker = format_ticker_for_eodhd(ticker)
                            if '.' in formatted_ticker:
                                symbol, exchange = formatted_ticker.rsplit('.', 1)
                            else:
                                symbol = formatted_ticker
                                exchange = 'US'
                            
                            fundamentals_service = EODHDFundamentalsService()
                            full_data = fundamentals_service.fetch_company_full_data(symbol, exchange)
                            
                            if full_data:
                                # 保存到数据库
                                company_data = {
                                    'symbol': symbol,
                                    'exchange': exchange,
                                    'general_data': full_data.get('General', full_data),
                                    'address_data': full_data.get('General', full_data).get('AddressData') if full_data.get('General') else None,
                                    'officers_data': fundamentals_service._extract_officers_list(full_data.get('General', full_data).get('Officers', {})) if full_data.get('General') else [],
                                    'listings_data': full_data.get('General', full_data).get('Listings') if full_data.get('General') else None,
                                    'full_data': full_data
                                }
                                
                                fundamentals_service.batch_save_companies_data([company_data])
                                
                                # 更新is_verified
                                general_data = full_data.get('General', full_data)
                                company_name = general_data.get('Name', symbol)
                                
                                verify_cursor.execute("""
                                    UPDATE theme_company_associations
                                    SET is_verified = TRUE,
                                        company_name = %s,
                                        updated_at = NOW()
                                    WHERE theme_id = %s AND ticker = %s
                                """, (company_name, theme_id, ticker))
                                verify_conn.commit()
                                verified_count += 1
                                logger.info(f"✅ Ticker {ticker} verified and fetched from EODHD")
                            else:
                                logger.warning(f"⚠️ Ticker {ticker} not found in EODHD")
                        
                        verify_cursor.close()
                        verify_conn.close()
                        
                    except Exception as verify_error:
                        logger.warning(f"Failed to verify ticker {ticker}: {str(verify_error)}")
                        continue
                
                logger.info(f"🔍 Auto-verification completed: {verified_count}/{len(request.tickers)} tickers verified")
                
                # 添加验证结果到返回值
                result["verified_count"] = verified_count
                
            except Exception as e:
                logger.warning(f"Auto-verification failed: {str(e)}")
        
        # 根据结果返回不同的状态码
        if result["success"]:
            return result
        elif result.get("conflict"):
            # 检测到冲突，返回冲突信息让前端处理（返回200状态码）
            logger.info(f"Theme conflict detected: {result.get('theme_id')}")
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


@router.post("/update-theme-tickers")
async def update_theme_tickers(
    request: UpdateThemeTickersRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    更新主题的 tickers 并重新生成描述和向量
    
    使用场景：
    - 当创建主题时发现 theme_id 冲突
    - 用户选择要保留的 tickers 后调用此接口
    - 会删除旧的 tickers 关联，创建新的关联
    - 重新生成 LLM 描述和向量
    
    权限：所有登录用户（但非 admin 只能更新自己创建的主题）
    """
    try:
        from backend.database.db_manager import DatabaseManager
        from backend.services.theme_company_association_service import ThemeCompanyAssociationService
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        # 检查权限：非 admin 只能更新自己创建的主题
        if not current_user.is_admin:
            conn = db_manager.get_connection()
            if not conn:
                raise HTTPException(status_code=500, detail="Database connection failed")
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT created_by FROM investment_theme_embeddings 
                WHERE theme_id = %s
            """, (request.theme_id,))
            result_row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not result_row:
                raise HTTPException(status_code=404, detail="主题不存在")
            
            if result_row[0] != current_user.username:
                raise HTTPException(
                    status_code=403, 
                    detail="权限不足：只能更新自己创建的主题"
                )
        
        service = InvestmentThemeEmbeddingService(db_config)
        association_service = ThemeCompanyAssociationService(db_config)
        
        logger.info(
            f"User {current_user.username} updating theme tickers",
            theme_id=request.theme_id,
            tickers_count=len(request.tickers),
            is_admin=current_user.is_admin
        )
        
        # 1. 验证 tickers 并获取公司信息
        stock_symbols = None
        company_names = None
        valid_tickers = []
        
        if request.tickers and request.market:
            try:
                validation_result = association_service.validate_and_complete_tickers(
                    request.tickers,
                    request.market
                )
                
                if validation_result.get("valid"):
                    valid_tickers = [v["full_ticker"] for v in validation_result["valid"]]
                    stock_symbols = ", ".join(valid_tickers)
                    company_names = ", ".join([v["company_name"] for v in validation_result["valid"]])
                    logger.info(f"Validated {len(valid_tickers)} tickers for theme {request.theme_id}")
            except Exception as e:
                logger.warning(f"Failed to validate tickers: {str(e)}")
        
        # 2. 使用 LLM 重新生成描述
        desc_result = await service.generate_theme_description(
            theme_name=request.theme_name,
            category=request.category,
            stock_symbols=stock_symbols,
            company_names=company_names
        )
        
        if not desc_result.get('success'):
            raise HTTPException(
                status_code=500,
                detail=f"Failed to generate description: {desc_result.get('message')}"
            )
        
        new_description = desc_result.get('description', '')
        
        # 3. 更新主题描述和向量
        update_result = await service.update_theme_description(
            theme_id=request.theme_id,
            new_description=new_description,
            editor_username=current_user.username,
            new_group=request.group,
            version=None  # 不使用乐观锁
        )
        
        if not update_result.get('description_saved'):
            raise HTTPException(
                status_code=500,
                detail=f"Failed to update theme: {update_result.get('message')}"
            )
        
        # 4. 删除旧的 ticker 关联
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM theme_company_associations
            WHERE theme_id = %s
        """, (request.theme_id,))
        
        old_count = cursor.rowcount
        logger.info(f"Deleted {old_count} old ticker associations for theme {request.theme_id}")
        
        # 5. 创建新的 ticker 关联
        new_count = 0
        if valid_tickers:
            for ticker in valid_tickers:
                # 解析 ticker 获取 market
                if '.' in ticker:
                    symbol, exchange_suffix = ticker.rsplit('.', 1)
                    # 简单映射
                    market_map = {'US': 'US', 'HK': 'HK', 'SHG': 'CN', 'SHE': 'CN'}
                    ticker_market = market_map.get(exchange_suffix, request.market or 'US')
                else:
                    ticker_market = request.market or 'US'
                
                cursor.execute("""
                    INSERT INTO theme_company_associations 
                    (theme_id, ticker, market, association_weight, created_at)
                    VALUES (%s, %s, %s, 1.0, NOW())
                    ON CONFLICT (theme_id, ticker) DO NOTHING
                """, (request.theme_id, ticker, ticker_market))
                
                new_count += cursor.rowcount
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Created {new_count} new ticker associations for theme {request.theme_id}")
        
        # 🔍 自动验证新添加的 tickers
        verified_count = 0
        if valid_tickers:
            logger.info(f"🔍 Auto-verifying {len(valid_tickers)} tickers for theme {request.theme_id}...")
            
            try:
                from backend.services.eodhd_fundamentals_service import EODHDFundamentalsService
                from backend.config.exchange_config import format_ticker_for_eodhd
                
                for ticker in valid_tickers:
                    try:
                        # 检查ticker是否在companies表中
                        verify_conn = db_manager.get_connection()
                        verify_cursor = verify_conn.cursor()
                        
                        verify_cursor.execute("""
                            SELECT id, code, name, exchange, primaryticker
                            FROM companies
                            WHERE UPPER(primaryticker) = UPPER(%s)
                            LIMIT 1
                        """, (ticker,))
                        
                        company_result = verify_cursor.fetchone()
                        
                        if company_result:
                            # 在数据库中找到，更新is_verified
                            verify_cursor.execute("""
                                UPDATE theme_company_associations
                                SET is_verified = TRUE,
                                    company_name = %s,
                                    updated_at = NOW()
                                WHERE theme_id = %s AND ticker = %s
                            """, (company_result[2], request.theme_id, ticker))
                            verify_conn.commit()
                            verified_count += 1
                            logger.debug(f"  ✅ {ticker} verified (found in DB)")
                        else:
                            # 不在数据库中，尝试从EODHD获取
                            logger.debug(f"  🔍 {ticker} not in DB, fetching from EODHD...")
                            
                            formatted_ticker = format_ticker_for_eodhd(ticker)
                            if '.' in formatted_ticker:
                                symbol, exchange = formatted_ticker.rsplit('.', 1)
                            else:
                                symbol = formatted_ticker
                                exchange = 'US'
                            
                            fundamentals_service = EODHDFundamentalsService()
                            full_data = fundamentals_service.fetch_company_full_data(symbol, exchange)
                            
                            if full_data:
                                # 保存到数据库
                                company_data = {
                                    'symbol': symbol,
                                    'exchange': exchange,
                                    'general_data': full_data.get('General', full_data),
                                    'address_data': full_data.get('General', full_data).get('AddressData') if full_data.get('General') else None,
                                    'officers_data': fundamentals_service._extract_officers_list(full_data.get('General', full_data).get('Officers', {})) if full_data.get('General') else [],
                                    'listings_data': full_data.get('General', full_data).get('Listings') if full_data.get('General') else None,
                                    'full_data': full_data
                                }
                                
                                fundamentals_service.batch_save_companies_data([company_data])
                                
                                # 更新is_verified
                                general_data = full_data.get('General', full_data)
                                company_name = general_data.get('Name', symbol)
                                
                                verify_cursor.execute("""
                                    UPDATE theme_company_associations
                                    SET is_verified = TRUE,
                                        company_name = %s,
                                        updated_at = NOW()
                                    WHERE theme_id = %s AND ticker = %s
                                """, (company_name, request.theme_id, ticker))
                                verify_conn.commit()
                                verified_count += 1
                                logger.info(f"  ✅ {ticker} verified (fetched from EODHD)")
                            else:
                                logger.warning(f"  ⚠️ {ticker} not found in EODHD")
                        
                        verify_cursor.close()
                        verify_conn.close()
                        
                    except Exception as verify_error:
                        logger.warning(f"  ❌ Failed to verify {ticker}: {str(verify_error)}")
                        continue
                
                logger.info(f"🔍 Auto-verification completed: {verified_count}/{len(valid_tickers)} tickers verified")
                
            except Exception as e:
                logger.warning(f"Auto-verification failed: {str(e)}")
        
        return {
            "success": True,
            "message": f"主题更新成功：删除 {old_count} 个旧关联，创建 {new_count} 个新关联",
            "theme_id": request.theme_id,
            "description_updated": update_result.get('description_saved'),
            "embedding_updated": update_result.get('embedding_updated'),
            "old_associations_deleted": old_count,
            "new_associations_created": new_count,
            "verified_count": verified_count
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update theme tickers failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"更新失败: {str(e)}")


@router.post("/{theme_id}/update-with-conflict-resolution")
async def update_theme_with_conflict_resolution(
    theme_id: str,
    request: UpdateThemeWithConflictResolutionRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    冲突解决后更新主题
    
    使用场景：
    - 批量上传或单个创建时发现主题冲突
    - 用户在冲突解决界面选择要保留的 tickers
    - 更新主题的 tickers 关联并重新生成向量
    
    权限：所有登录用户（但非 admin 只能更新自己创建的主题）
    """
    try:
        from backend.database.db_manager import DatabaseManager
        from backend.services.theme_company_association_service import ThemeCompanyAssociationService
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        logger.info(
            f"User {current_user.username} updating theme with conflict resolution",
            theme_id=theme_id,
            theme_name=request.theme_name,
            category=request.category,
            selected_tickers_count=len(request.selected_tickers),
            is_admin=current_user.is_admin
        )
        
        # 检查权限：非 admin 只能更新自己创建的主题
        if not current_user.is_admin:
            conn = db_manager.get_connection()
            if not conn:
                raise HTTPException(status_code=500, detail="Database connection failed")
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT created_by FROM investment_theme_embeddings 
                WHERE theme_id = %s
            """, (theme_id,))
            result_row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not result_row:
                raise HTTPException(status_code=404, detail="主题不存在")
            
            if result_row[0] != current_user.username:
                raise HTTPException(
                    status_code=403, 
                    detail="权限不足：只能更新自己创建的主题"
                )
        
        service = InvestmentThemeEmbeddingService(db_config)
        association_service = ThemeCompanyAssociationService(db_config)
        
        # selected_tickers 已经是标准化的（带市场后缀），直接使用
        valid_tickers = request.selected_tickers
        
        # 获取公司名称用于生成描述
        stock_symbols = ", ".join(valid_tickers)
        company_names = None
        
        try:
            # 尝试获取公司名称
            conn = db_manager.get_connection()
            if conn:
                cursor = conn.cursor()
                placeholders = ','.join(['%s'] * len(valid_tickers))
                cursor.execute(f"""
                    SELECT ticker, company_name 
                    FROM company_profiles 
                    WHERE ticker IN ({placeholders})
                """, valid_tickers)
                
                companies = {row[0]: row[1] for row in cursor.fetchall()}
                company_names = ", ".join([companies.get(t, t) for t in valid_tickers])
                
                cursor.close()
                conn.close()
        except Exception as e:
            logger.warning(f"Failed to fetch company names: {str(e)}")
        
        # 使用 LLM 重新生成描述
        desc_result = await service.generate_theme_description(
            theme_name=request.theme_name,
            category=request.category,
            stock_symbols=stock_symbols,
            company_names=company_names
        )
        
        if not desc_result.get('success'):
            logger.warning(f"Failed to generate description, using existing: {desc_result.get('message')}")
            new_description = request.description  # 使用现有描述
        else:
            new_description = desc_result.get('description', request.description)
        
        # 更新主题描述和向量
        update_result = await service.update_theme_description(
            theme_id=theme_id,
            new_description=new_description,
            editor_username=current_user.username,
            new_group=request.group,
            version=None  # 不使用乐观锁
        )
        
        if not update_result.get('description_saved'):
            raise HTTPException(
                status_code=500,
                detail=f"Failed to update theme: {update_result.get('message')}"
            )
        
        # 删除旧的 ticker 关联（先获取旧的用于对比）
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor()
        
        # 获取旧的tickers用于对比（找出新增的）
        cursor.execute("""
            SELECT ticker FROM theme_company_associations
            WHERE theme_id = %s
        """, (theme_id,))
        old_tickers = set([row[0] for row in cursor.fetchall()])
        
        # 删除旧关联
        cursor.execute("""
            DELETE FROM theme_company_associations
            WHERE theme_id = %s
        """, (theme_id,))
        
        old_count = cursor.rowcount
        logger.info(f"Deleted {old_count} old ticker associations for theme {theme_id}")
        
        # 创建新的 ticker 关联
        new_count = 0
        newly_added_tickers = []  # 记录新增的ticker
        if valid_tickers:
            for ticker in valid_tickers:
                # 解析 ticker 获取 market
                if '.' in ticker:
                    symbol, exchange_suffix = ticker.rsplit('.', 1)
                    # 简单映射
                    market_map = {'US': 'US', 'HK': 'HK', 'SHG': 'CN', 'SHE': 'CN'}
                    ticker_market = market_map.get(exchange_suffix, 'US')
                else:
                    ticker_market = 'US'
                
                cursor.execute("""
                    INSERT INTO theme_company_associations 
                    (theme_id, ticker, market, created_at)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (theme_id, ticker) DO NOTHING
                """, (theme_id, ticker, ticker_market))
                new_count += cursor.rowcount
                
                # 记录新增的ticker
                if ticker not in old_tickers:
                    newly_added_tickers.append(ticker)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Created {new_count} new ticker associations for theme {theme_id}")
        
        # 🔥 自动验证新增的 tickers（只验证真正新增的，不验证已存在的）
        verified_count = 0
        if len(newly_added_tickers) > 0:
            logger.info(f"🔍 Auto-verifying {len(newly_added_tickers)} newly added tickers for {theme_id}...")
            try:
                from backend.services.eodhd_fundamentals_service import EODHDFundamentalsService
                from backend.config.exchange_config import format_ticker_for_eodhd
                
                for ticker in newly_added_tickers:
                    try:
                        # 检查ticker是否在companies表中
                        verify_conn = db_manager.get_connection()
                        verify_cursor = verify_conn.cursor()
                        
                        verify_cursor.execute("""
                            SELECT id, code, name, exchange, primaryticker
                            FROM companies
                            WHERE UPPER(primaryticker) = UPPER(%s)
                            LIMIT 1
                        """, (ticker,))
                        
                        company_result = verify_cursor.fetchone()
                        
                        if company_result:
                            # 在数据库中找到，更新is_verified
                            verify_cursor.execute("""
                                UPDATE theme_company_associations
                                SET is_verified = TRUE,
                                    company_name = %s,
                                    updated_at = NOW()
                                WHERE theme_id = %s AND ticker = %s
                            """, (company_result[2], theme_id, ticker))
                            verify_conn.commit()
                            verified_count += 1
                            logger.debug(f"✅ Ticker {ticker} verified (found in DB)")
                        else:
                            # 不在数据库中，尝试从EODHD获取
                            logger.debug(f"🔍 Ticker {ticker} not in DB, fetching from EODHD...")
                            
                            formatted_ticker = format_ticker_for_eodhd(ticker)
                            if '.' in formatted_ticker:
                                symbol, exchange = formatted_ticker.rsplit('.', 1)
                            else:
                                symbol = formatted_ticker
                                exchange = 'US'
                            
                            fundamentals_service = EODHDFundamentalsService()
                            full_data = fundamentals_service.fetch_company_full_data(symbol, exchange)
                            
                            if full_data:
                                # 保存到数据库
                                company_data = {
                                    'symbol': symbol,
                                    'exchange': exchange,
                                    'general_data': full_data.get('General', full_data),
                                    'address_data': full_data.get('General', full_data).get('AddressData') if full_data.get('General') else None,
                                    'officers_data': fundamentals_service._extract_officers_list(full_data.get('General', full_data).get('Officers', {})) if full_data.get('General') else [],
                                    'listings_data': full_data.get('General', full_data).get('Listings') if full_data.get('General') else None,
                                    'full_data': full_data
                                }
                                
                                fundamentals_service.batch_save_companies_data([company_data])
                                
                                # 更新is_verified
                                general_data = full_data.get('General', full_data)
                                company_name = general_data.get('Name', symbol)
                                
                                verify_cursor.execute("""
                                    UPDATE theme_company_associations
                                    SET is_verified = TRUE,
                                        company_name = %s,
                                        updated_at = NOW()
                                    WHERE theme_id = %s AND ticker = %s
                                """, (company_name, theme_id, ticker))
                                verify_conn.commit()
                                verified_count += 1
                                logger.info(f"✅ Ticker {ticker} verified and fetched from EODHD")
                            else:
                                logger.warning(f"⚠️ Ticker {ticker} not found in EODHD")
                        
                        verify_cursor.close()
                        verify_conn.close()
                        
                    except Exception as verify_error:
                        logger.warning(f"Failed to verify ticker {ticker}: {str(verify_error)}")
                        continue
                
                logger.info(f"🔍 Auto-verification completed: {verified_count}/{len(newly_added_tickers)} newly added tickers verified")
                
            except Exception as e:
                logger.warning(f"Auto-verification failed: {str(e)}")
        
        return {
            "success": True,
            "message": f"主题 '{request.theme_name}' 已更新",
            "theme_id": theme_id,
            "theme_saved": True,
            "embedding_created": update_result.get('vector_saved', False),
            "associations_updated": new_count,
            "verified_count": verified_count  # 返回验证的数量
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update theme with conflict resolution failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"更新主题失败: {str(e)}")


@router.post("/batch-resolve-conflicts")
async def batch_resolve_conflicts(
    request: BatchResolveConflictsRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    批量解决冲突
    
    使用场景：
    - 批量上传时检测到多个主题冲突
    - 用户在统一界面中选择所有冲突的解决方案
    - 提交到 Celery 进行批量异步处理
    
    权限：所有登录用户（但非 admin 只能更新自己创建的主题）
    """
    try:
        logger.info(
            f"User {current_user.username} batch resolving conflicts",
            conflicts_count=len(request.conflict_resolutions),
            is_admin=current_user.is_admin
        )
        
        # 转换为普通字典用于 Celery 任务
        conflict_resolutions_dict = {}
        for theme_id, resolution in request.conflict_resolutions.items():
            conflict_resolutions_dict[theme_id] = {
                "theme_name": resolution.theme_name,
                "category": resolution.category,
                "description": resolution.description,
                "selected_tickers": resolution.selected_tickers,
                "group": resolution.group
            }
        
        # 提交 Celery 任务
        from backend.tasks.theme_tasks import batch_resolve_conflicts_task
        
        task = batch_resolve_conflicts_task.apply_async(
            args=[conflict_resolutions_dict, current_user.username],
            queue='ai_processing'
        )
        
        logger.info(f"Batch resolve conflicts task submitted: {task.id}")
        
        return {
            "success": True,
            "message": "批量冲突解决任务已提交",
            "task_id": task.id
        }
        
    except Exception as e:
        logger.error(f"Batch resolve conflicts failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"批量冲突解决失败: {str(e)}")


@router.post("/batch-upload")
async def batch_upload_themes(
    file: UploadFile = File(...),
    group: Optional[str] = Form(None),
    market: str = Form("US"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    批量上传主题文件
    
    权限：所有登录用户
    
    核心逻辑（重要）：
    - 支持文件中包含多个 theme 值
    - 按 (theme, category) 组合分组，每个组合创建一个主题
    - 例如：gs_themes.csv 含 theme='GS Data Driven' 与 theme='Quantum Computing'，
      各自按 category 分组后分别创建多条 investment theme 记录
    
    Args:
        file: CSV或Excel文件
        market: 市场选择（US/HK/CN），用户在前端选择
    
    文件格式要求：
    - 必需列：theme, category, ticker
    - 可选列：company
    - theme 列可有多个不同值
    - category 可有多个不同值
    
    文件格式示例（multi_theme.csv）:
    ```csv
    theme,category,ticker,company
    crypto,ETF,ARKB,ARK 21Shares Bitcoin ETF
    crypto,Treasury,MSTR,MicroStrategy Inc
    AI,Infrastructure,NVDA,NVIDIA Corp
    AI,Application,MSFT,Microsoft Corp
    ```
    
    说明：上述文件会创建 4 个主题（2 个 theme × 各 2 个 category）
    
    示例：
    ```bash
    curl -X POST "http://localhost:8000/api/investment-themes/batch-upload" \\
      -H "Authorization: Bearer YOUR_TOKEN" \\
      -F "file=@multi_theme.csv" \\
      -F "market=US"
    ```
    
    返回：
    ```json
    {
        "success": true,
        "message": "Batch upload started: 2 themes, 4 (theme, category) groups",
        "task_id": "abc-123-def-456",
        "theme_names": ["ai", "crypto"],
        "preview": {
            "theme_names": ["ai", "crypto"],
            "total_themes": 4,
            "total_tickers": 4,
            "themes": {"crypto": {"categories": {"etf": 1, "treasury": 1}, "total_tickers": 2}, ...},
            "categories": {"etf": 1, "treasury": 1, ...},
            "categories_sample": ["etf", "treasury", ...]
        },
        "status_url": "/api/investment-themes/task-status/abc-123-def-456"
    }
    ```
    """
    try:
        logger.info(
            f"User {current_user.username} uploading batch theme file: {file.filename}, group: {group}, market: {market}, is_admin: {current_user.is_admin}"
        )
        
        # 1. 验证 market 参数
        if market not in ['US', 'HK', 'CN']:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid market: {market}. Must be US, HK, or CN."
            )
        
        # 3. 验证文件格式
        if not file.filename:
            raise HTTPException(status_code=400, detail="File name is empty")
        
        file_ext = file.filename.lower().split('.')[-1]
        if file_ext not in ['csv', 'xlsx', 'xls']:
            raise HTTPException(
                status_code=400, 
                detail=f"Unsupported file format: .{file_ext}. Only .csv, .xlsx, .xls are supported."
            )
        
        # 4. 解析文件
        from backend.utils.theme_file_parser import ThemeFileParser
        
        parser = ThemeFileParser()
        
        # 重置文件指针
        await file.seek(0)
        
        parse_result = parser.parse_file(file, market=market, group=group, creator_username=current_user.username)
        
        # 5. 如果解析失败，返回错误
        if not parse_result.get('success'):
            error_msg = parse_result.get('error', 'Unknown error')
            logger.error(f"File parsing failed: {error_msg}")
            raise HTTPException(
                status_code=400,
                detail=f"File parsing failed: {error_msg}"
            )
        
        theme_names = parse_result['theme_names']
        themes_data = parse_result['data']
        preview = parse_result['preview']
        
        logger.info(
            f"File parsed successfully: group='{group}', themes={len(theme_names)}, "
            f"{preview['total_themes']} (theme, category) groups, "
            f"{preview['total_tickers']} tickers"
        )
        
        # 6. 提交 Celery 任务
        from backend.tasks.theme_tasks import batch_create_themes_task
        
        task = batch_create_themes_task.apply_async(
            kwargs={
                'themes_data': themes_data,
                'creator_username': current_user.username
            }
        )
        
        logger.info(f"Batch create themes task submitted: {task.id}")
        
        # 7. 返回结果
        return {
            "success": True,
            "message": (
                f"Batch upload started: {len(theme_names)} theme(s), "
                f"{preview['total_themes']} (theme, category) groups"
            ),
            "task_id": task.id,
            "theme_names": theme_names,
            "preview": preview,
            "status_url": f"/api/investment-themes/task-status/{task.id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Batch upload failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Batch upload failed: {str(e)}")


# ==================== Theme Tags Management Endpoints ====================

@router.get("/{theme_id}/tags")
async def get_theme_tags(
    theme_id: str,
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取主题的所有标签
    
    Returns:
        {
            "success": true,
            "theme_id": "theme_001",
            "tags": ["Market Weekly", "Broker Report"]
        }
    """
    try:
        from backend.database.db_manager import DatabaseManager
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor()
        
        # 查询主题的所有标签
        cursor.execute("""
            SELECT tag_name, created_at, created_by
            FROM theme_tags
            WHERE theme_id = %s
            ORDER BY created_at DESC
        """, (theme_id,))
        
        rows = cursor.fetchall()
        tags = [{"tag_name": row[0], "created_at": row[1].isoformat() if row[1] else None, "created_by": row[2]} for row in rows]
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "theme_id": theme_id,
            "tags": tags
        }
        
    except Exception as e:
        logger.error(f"Get theme tags failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取标签失败: {str(e)}")


@router.get("/available-tags")
async def get_available_tags(
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取当前用户的所有可用标签及其使用次数（用户个性化）
    
    🔥 只返回当前用户自己创建的标签
    
    Returns:
        {
            "success": true,
            "tags": [
                {"tag_name": "Market Weekly", "count": 15},
                {"tag_name": "Broker Report", "count": 3}
            ]
        }
    """
    try:
        from backend.database.db_manager import DatabaseManager
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor()
        
        # 🔥 查询当前用户的标签及其使用次数
        cursor.execute("""
            SELECT tag_name, COUNT(DISTINCT theme_id) as count
            FROM theme_tags
            WHERE user_id = %s
            GROUP BY tag_name
            ORDER BY count DESC, tag_name
        """, (current_user.username,))
        
        rows = cursor.fetchall()
        tags = [{"tag_name": row[0], "count": row[1]} for row in rows]
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "tags": tags
        }
        
    except Exception as e:
        logger.error(f"Get available tags failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取可用标签失败: {str(e)}")


@router.post("/batch-tag")
async def batch_tag_themes(
    request: BatchTagRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    批量为主题添加或移除标签（用户个性化标签）
    
    🔥 重要变更：标签现在是用户个性化的
    - 每个用户可以独立为任何主题（包括别人分享的）打标签
    - 用户只能添加/删除自己的标签，不影响其他用户的标签
    - 同一个主题可以被不同用户独立打上相同的标签
    
    权限：
    - 所有用户都可以为可见的主题打标签（自己创建的 + 已分享的）
    - 用户只能删除自己添加的标签
    
    Args:
        request: {
            "theme_ids": ["theme_001", "theme_002"],
            "tags": ["Market Weekly", "Broker Report"],
            "action": "add" | "remove"
        }
    
    Returns:
        {
            "success": true,
            "message": "成功为 2 个主题添加标签",
            "affected_count": 2
        }
    """
    try:
        from backend.database.db_manager import DatabaseManager
        
        if not request.theme_ids:
            raise HTTPException(status_code=400, detail="主题ID列表不能为空")
        
        if not request.tags:
            raise HTTPException(status_code=400, detail="标签列表不能为空")
        
        if request.action not in ["add", "remove"]:
            raise HTTPException(status_code=400, detail="操作类型必须是 'add' 或 'remove'")
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor()
        
        # 🔥 验证用户是否有权限查看这些主题
        # 用户可以为自己创建的主题 OR 已分享的主题打标签
        placeholders = ','.join(['%s'] * len(request.theme_ids))
        if not current_user.is_admin:
            cursor.execute(f"""
                SELECT theme_id FROM investment_theme_embeddings
                WHERE theme_id IN ({placeholders})
                  AND (created_by = %s OR is_shared = TRUE)
            """, (*request.theme_ids, current_user.username))
        else:
            # Admin can tag all themes
            cursor.execute(f"""
                SELECT theme_id FROM investment_theme_embeddings
                WHERE theme_id IN ({placeholders})
            """, (*request.theme_ids,))
        
        accessible_themes = {row[0] for row in cursor.fetchall()}
        inaccessible_themes = set(request.theme_ids) - accessible_themes
        
        if inaccessible_themes:
            cursor.close()
            conn.close()
            raise HTTPException(
                status_code=403,
                detail=f"权限不足：无法访问以下主题: {', '.join(inaccessible_themes)}"
            )
        
        affected_count = 0
        
        if request.action == "add":
            # 🔥 批量添加标签（记录 user_id）
            for theme_id in request.theme_ids:
                for tag_name in request.tags:
                    cursor.execute("""
                        INSERT INTO theme_tags (theme_id, tag_name, user_id, created_by, created_at)
                        VALUES (%s, %s, %s, %s, NOW())
                        ON CONFLICT (theme_id, tag_name, user_id) DO NOTHING
                    """, (theme_id, tag_name, current_user.username, current_user.username))
                    affected_count += cursor.rowcount
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(
                f"User {current_user.username} added personal tags to themes",
                theme_ids=request.theme_ids,
                tags=request.tags,
                affected_count=affected_count
            )
            
            return {
                "success": True,
                "message": f"成功为 {len(request.theme_ids)} 个主题添加标签",
                "affected_count": affected_count
            }
        
        else:  # action == "remove"
            # 🔥 批量移除标签（只删除当前用户的标签）
            placeholders_themes = ','.join(['%s'] * len(request.theme_ids))
            placeholders_tags = ','.join(['%s'] * len(request.tags))
            
            cursor.execute(f"""
                DELETE FROM theme_tags
                WHERE theme_id IN ({placeholders_themes})
                  AND tag_name IN ({placeholders_tags})
                  AND user_id = %s
            """, (*request.theme_ids, *request.tags, current_user.username))
            
            affected_count = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(
                f"User {current_user.username} removed personal tags from themes",
                theme_ids=request.theme_ids,
                tags=request.tags,
                affected_count=affected_count
            )
            
            return {
                "success": True,
                "message": f"成功从 {len(request.theme_ids)} 个主题移除标签",
                "affected_count": affected_count
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Batch tag operation failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"批量标签操作失败: {str(e)}")


@router.post("/{theme_id}/tags")
async def add_tag_to_theme(
    theme_id: str,
    request: AddTagToThemeRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    为单个主题添加标签
    
    权限：
    - Admin：可以为所有主题添加标签
    - 普通用户：只能为自己创建的主题添加标签
    """
    try:
        from backend.database.db_manager import DatabaseManager
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        # 非admin用户需要检查权限
        if not current_user.is_admin:
            conn = db_manager.get_connection()
            if not conn:
                raise HTTPException(status_code=500, detail="Database connection failed")
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT created_by FROM investment_theme_embeddings
                WHERE theme_id = %s
            """, (theme_id,))
            result_row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not result_row:
                raise HTTPException(status_code=404, detail="主题不存在")
            
            if result_row[0] != current_user.username:
                raise HTTPException(
                    status_code=403,
                    detail="权限不足：只能为自己创建的主题添加标签"
                )
        
        # 添加标签
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO theme_tags (theme_id, tag_name, created_by, created_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (theme_id, tag_name) DO NOTHING
        """, (theme_id, request.tag_name, current_user.username))
        
        affected = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        if affected == 0:
            return {
                "success": True,
                "message": "标签已存在",
                "theme_id": theme_id,
                "tag_name": request.tag_name
            }
        
        return {
            "success": True,
            "message": "标签添加成功",
            "theme_id": theme_id,
            "tag_name": request.tag_name
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Add tag to theme failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"添加标签失败: {str(e)}")


@router.delete("/{theme_id}/tags/{tag_name}")
async def remove_tag_from_theme(
    theme_id: str,
    tag_name: str,
    current_user: TokenData = Depends(get_current_user)
):
    """
    从单个主题移除标签
    
    权限：
    - Admin：可以移除所有主题的标签
    - 普通用户：只能移除自己创建的主题的标签
    """
    try:
        from backend.database.db_manager import DatabaseManager
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        # 非admin用户需要检查权限
        if not current_user.is_admin:
            conn = db_manager.get_connection()
            if not conn:
                raise HTTPException(status_code=500, detail="Database connection failed")
            
            cursor = conn.cursor()
            cursor.execute("""
                SELECT created_by FROM investment_theme_embeddings
                WHERE theme_id = %s
            """, (theme_id,))
            result_row = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not result_row:
                raise HTTPException(status_code=404, detail="主题不存在")
            
            if result_row[0] != current_user.username:
                raise HTTPException(
                    status_code=403,
                    detail="权限不足：只能移除自己创建的主题的标签"
                )
        
        # 移除标签
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM theme_tags
            WHERE theme_id = %s AND tag_name = %s
        """, (theme_id, tag_name))
        
        affected = cursor.rowcount
        conn.commit()
        cursor.close()
        conn.close()
        
        if affected == 0:
            raise HTTPException(status_code=404, detail="标签不存在")
        
        return {
            "success": True,
            "message": "标签移除成功",
            "theme_id": theme_id,
            "tag_name": tag_name
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Remove tag from theme failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"移除标签失败: {str(e)}")


# ==================== Content Theme Matches API ====================

class ContentMatchTriggerRequest(BaseModel):
    """手动触发单条内容匹配"""
    content_type: str = Field(..., pattern=r'^(file|feed)$', description="内容类型: file | feed")
    content_id: int = Field(..., description="内容 ID")
    match_mode: str = Field('both', pattern=r'^(ticker|vector|both)$', description="匹配模式: ticker | vector | both")


class ContentMatchBackfillRequest(BaseModel):
    """批量回填请求"""
    content_type: Optional[str] = Field(None, pattern=r'^(file|feed)$', description="内容类型（空=两者都回填）")
    force: bool = Field(False, description="是否强制重新匹配已有记录")
    hours: int = Field(48, ge=1, le=720, description="回填时间窗口（小时），默认 48")


@router.get("/content-matches")
async def get_content_matches(
    content_type: Optional[str] = Query(None, pattern=r'^(file|feed)$', description="内容类型"),
    content_id: Optional[int] = Query(None, description="内容 ID"),
    theme_id: Optional[str] = Query(None, description="主题 ID"),
    match_mode: Optional[str] = Query(None, pattern=r'^(ticker|vector)$', description="匹配模式"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    current_user: TokenData = Depends(get_current_user),
):
    """查询内容与投资主题的匹配结果"""
    from backend.services.content_theme_matching_service import ContentThemeMatchingService

    service = ContentThemeMatchingService()

    if content_type and content_id:
        # 查询指定内容的匹配
        matches = service.get_matches_for_content(content_type, content_id, match_mode=match_mode)
        return {"items": matches, "total": len(matches)}
    elif theme_id:
        # 查询指定主题的所有关联内容
        result = service.get_matches_for_theme(
            theme_id,
            content_type=content_type,
            match_mode=match_mode,
            limit=limit,
            offset=offset,
        )
        return result
    else:
        raise HTTPException(
            status_code=400,
            detail="必须提供 content_type + content_id 或 theme_id 参数",
        )


@router.post("/content-matches/trigger")
async def trigger_content_match(
    request: ContentMatchTriggerRequest,
    current_user: TokenData = Depends(get_current_admin_user),
):
    """手动触发单条内容的主题匹配（Admin only）"""
    from backend.tasks.theme_matching_tasks import (
        match_content_themes_ticker_task,
        match_content_themes_vector_task,
    )

    task_ids = []

    if request.match_mode in ('ticker', 'both'):
        task = match_content_themes_ticker_task.apply_async(
            args=(request.content_type, request.content_id),
            countdown=1,
        )
        task_ids.append({"mode": "ticker", "task_id": task.id})

    if request.match_mode in ('vector', 'both'):
        # 如果是 both，vector 会由 ticker 任务自动链式派发，这里只单独触发 vector 时手动派发
        if request.match_mode == 'vector':
            task = match_content_themes_vector_task.apply_async(
                args=(request.content_type, request.content_id),
                countdown=2,
            )
            task_ids.append({"mode": "vector", "task_id": task.id})

    return {
        "success": True,
        "message": f"已派发 {len(task_ids)} 个匹配任务",
        "tasks": task_ids,
    }


@router.post("/content-matches/backfill")
async def backfill_content_matches(
    request: ContentMatchBackfillRequest,
    current_user: TokenData = Depends(get_current_admin_user),
):
    """批量回填历史内容的主题匹配（Admin only）"""
    from backend.tasks.theme_matching_tasks import backfill_content_theme_matches_task

    task = backfill_content_theme_matches_task.apply_async(
        kwargs={
            "content_type": request.content_type,
            "force": request.force,
            "hours": request.hours,
        },
    )

    return {
        "success": True,
        "message": "回填任务已派发",
        "task_id": task.id,
    }

