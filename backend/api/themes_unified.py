# -*- coding: utf-8 -*-
"""
统一主题管理 API - 支持 Primary Theme 查询和管理
"""

from fastapi import APIRouter, HTTPException, Depends, Query
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

from backend.services.theme_mapping_service import ThemeMappingService
from backend.database.db_manager import DatabaseManager
from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.utils.logger import get_logger
from backend.utils.database_config import get_database_config

logger = get_logger("themes_unified_api")

router = APIRouter(
    prefix="/themes",
    tags=["统一主题管理"]
)


# ==================== Pydantic 模型 ====================

class PrimaryThemeResponse(BaseModel):
    """Primary Theme 响应模型"""
    theme_master_id: int
    primary_theme: str
    display_name: str
    description: Optional[str]
    confidence_score: Optional[float]
    metadata: Optional[Dict[str, Any]]
    created_at: Optional[str]


class StandardCategoryResponse(BaseModel):
    """标准分类响应模型"""
    id: int
    category: str
    display_name: str
    description: Optional[str]
    priority: int


class SetPrimaryThemeRequest(BaseModel):
    """设置 Primary Theme 请求模型"""
    news_id: int = Field(..., description="新闻ID")
    primary_theme: str = Field(..., description="主题分类")
    confidence_score: float = Field(0.8, ge=0.0, le=1.0, description="置信度")
    metadata: Optional[Dict[str, Any]] = Field(None, description="元数据")


class NewsThemeListResponse(BaseModel):
    """新闻主题列表响应"""
    news_id: int
    primary_theme: Optional[PrimaryThemeResponse]
    all_themes: List[Dict[str, Any]]


class ThemeFilterRequest(BaseModel):
    """主题筛选请求模型"""
    # 标准分类（theme_master_id）
    categories: Optional[List[str]] = Field(None, description="标准主题分类列表")
    
    # 分类维度（llm_classification_title）
    event_types: Optional[List[str]] = Field(None, description="事件类型列表")
    impact_scopes: Optional[List[str]] = Field(None, description="影响范围列表")
    urgency_levels: Optional[List[str]] = Field(None, description="时效性列表")
    related_sectors: Optional[List[str]] = Field(None, description="相关行业列表")
    related_countries: Optional[List[str]] = Field(None, description="相关国家/地区列表")
    
    # 来源类型
    source_types: Optional[List[str]] = Field(None, description="来源类型列表")
    
    # 筛选条件
    min_confidence: float = Field(0.5, ge=0.0, le=1.0, description="最低置信度")
    only_primary: bool = Field(False, description="只显示主要主题")
    market: Optional[str] = Field(None, description="市场筛选")
    
    # 日期范围
    start_date: Optional[str] = Field(None, description="开始日期 (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="结束日期 (YYYY-MM-DD)")
    
    # 分页
    page: int = Field(1, ge=1, description="页码")
    page_size: int = Field(20, ge=1, le=100, description="每页数量")


class NewsFilterResponse(BaseModel):
    """新闻筛选响应"""
    success: bool
    total: int
    page: int
    page_size: int
    total_pages: int
    news: List[Dict[str, Any]]
    filter_summary: Dict[str, Any]


class TrendingThemeItem(BaseModel):
    """热门主题项"""
    theme_name: str
    display_name: Optional[str]
    news_count: int
    avg_confidence: float
    trend: str  # '↑' 上升, '↓' 下降, '→' 持平
    daily_distribution: List[Dict[str, Any]]


class TrendingThemesResponse(BaseModel):
    """热门主题响应"""
    success: bool
    dimension: str
    period_days: int
    themes: List[TrendingThemeItem]
    total_themes: int


class DimensionValuesResponse(BaseModel):
    """维度值响应"""
    success: bool
    dimension: str
    values: List[Dict[str, Any]]
    total: int


# ==================== API 端点 ====================

@router.get("/categories", response_model=List[StandardCategoryResponse], summary="获取所有标准主题分类")
async def get_standard_categories(
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取所有标准主题分类
    
    返回系统预定义的12个主题分类，与 LLM news_classification 模板对应
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        theme_service = ThemeMappingService(db_manager)
        
        categories = theme_service.get_all_standard_categories()
        
        return categories
        
    except Exception as e:
        logger.error(f"Failed to get standard categories: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取标准分类失败: {str(e)}")


@router.get("/news/{news_id}/primary", response_model=PrimaryThemeResponse, summary="获取新闻的 Primary Theme")
async def get_news_primary_theme(
    news_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取指定新闻的 Primary Theme
    
    Args:
        news_id: 新闻ID
        
    Returns:
        Primary Theme 信息
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        theme_service = ThemeMappingService(db_manager)
        
        primary_theme = theme_service.get_primary_theme(news_id)
        
        if not primary_theme:
            raise HTTPException(status_code=404, detail=f"新闻 {news_id} 未设置 Primary Theme")
        
        return primary_theme
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get primary theme for news {news_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取 Primary Theme 失败: {str(e)}")


@router.post("/news/primary", summary="设置新闻的 Primary Theme")
async def set_news_primary_theme(
    request: SetPrimaryThemeRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    手动设置新闻的 Primary Theme
    
    Args:
        request: 包含新闻ID、主题分类、置信度等信息
        
    Returns:
        设置结果
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        theme_service = ThemeMappingService(db_manager)
        
        result_id = theme_service.upsert_primary_theme(
            news_id=request.news_id,
            primary_theme=request.primary_theme,
            confidence_score=request.confidence_score,
            metadata=request.metadata
        )
        
        if result_id:
            logger.info(f"User {current_user.username} set primary theme for news {request.news_id}: {request.primary_theme}")
            return {
                "success": True,
                "message": f"Primary Theme 设置成功",
                "record_id": result_id,
                "primary_theme": request.primary_theme
            }
        else:
            raise HTTPException(status_code=500, detail="设置 Primary Theme 失败")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to set primary theme: {str(e)}")
        raise HTTPException(status_code=500, detail=f"设置 Primary Theme 失败: {str(e)}")


@router.get("/news/{news_id}/all", response_model=NewsThemeListResponse, summary="获取新闻的所有主题")
async def get_news_all_themes(
    news_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取新闻的所有主题（包括 Primary Theme 和其他来源的主题）
    
    Args:
        news_id: 新闻ID
        
    Returns:
        主题列表，包括 Primary Theme 和所有其他主题
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        theme_service = ThemeMappingService(db_manager)
        
        # 获取 Primary Theme
        primary_theme = theme_service.get_primary_theme(news_id)
        
        # 获取所有主题
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                ntu.id,
                ntu.theme_name,
                ntu.source_type,
                ntu.confidence_score,
                ntu.is_primary,
                ntu.market,
                ntu.metadata,
                tmd.display_name,
                tmd.category
            FROM news_themes_unified ntu
            LEFT JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
            WHERE ntu.news_id = %s
              AND ntu.is_active = true
            ORDER BY ntu.is_primary DESC, ntu.confidence_score DESC
        """, (news_id,))
        
        all_themes = []
        for row in cursor.fetchall():
            all_themes.append({
                'id': row[0],
                'theme_name': row[1],
                'source_type': row[2],
                'confidence_score': float(row[3]) if row[3] else None,
                'is_primary': row[4],
                'market': row[5],
                'metadata': row[6],
                'display_name': row[7],
                'category': row[8]
            })
        
        cursor.close()
        conn.close()
        
        return {
            'news_id': news_id,
            'primary_theme': primary_theme,
            'all_themes': all_themes
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get all themes for news {news_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取主题列表失败: {str(e)}")


@router.post("/news/fused-themes/batch", summary="批量获取多条新闻的主题")
async def get_batch_news_themes(
    request: Dict[str, Any],
    current_user: TokenData = Depends(get_current_user)
):
    """
    批量获取多条新闻的主题信息
    
    Args:
        request: 包含news_ids的请求体
        
    Returns:
        字典，key为news_id，value为该新闻的所有主题
    """
    try:
        news_ids = request.get('news_ids', [])
        if not news_ids:
            return {}
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        cursor = conn.cursor()
        
        # 批量查询所有主题
        cursor.execute("""
            SELECT 
                ntu.news_id,
                ntu.theme_name,
                ntu.source_type,
                ntu.confidence_score,
                ntu.is_primary,
                ntu.market,
                ntu.metadata,
                tmd.display_name,
                tmd.category
            FROM news_themes_unified ntu
            LEFT JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
            WHERE ntu.news_id = ANY(%s)
              AND ntu.is_active = true
            ORDER BY ntu.news_id, ntu.is_primary DESC, ntu.confidence_score DESC
        """, (news_ids,))
        
        # 组织结果
        result = {}
        for news_id in news_ids:
            result[news_id] = {
                'news_id': news_id,
                'themes': [],
                'total_sources': 0,
                'generated_at': None
            }
        
        for row in cursor.fetchall():
            news_id = row[0]
            theme = {
                'theme_name': row[1],
                'source_type': row[2],
                'confidence_score': float(row[3]) if row[3] else None,
                'is_primary': row[4],
                'market': row[5],
                'metadata': row[6],
                'display_name': row[7],
                'category': row[8]
            }
            if news_id in result:
                result[news_id]['themes'].append(theme)
        
        # 计算每个新闻的source数量
        for news_id in result:
            sources = set(t['source_type'] for t in result[news_id]['themes'])
            result[news_id]['total_sources'] = len(sources)
        
        cursor.close()
        conn.close()
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get batch news themes: {str(e)}")
        raise HTTPException(status_code=500, detail=f"批量获取主题失败: {str(e)}")


@router.get("/primary/stats", summary="Primary Theme 统计")
async def get_primary_theme_stats(
    days: int = Query(30, ge=1, le=365, description="统计天数"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取 Primary Theme 的统计信息
    
    Args:
        days: 统计的天数范围
        
    Returns:
        各个主题的新闻数量统计
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                tmd.category,
                tmd.display_name,
                tmd.description,
                COUNT(DISTINCT ntu.news_id) as news_count,
                AVG(ntu.confidence_score) as avg_confidence,
                COUNT(DISTINCT DATE(n.date)) as active_days,
                MAX(n.date) as latest_news_date
            FROM news_themes_unified ntu
            INNER JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
            INNER JOIN news n ON ntu.news_id = n.id
            WHERE ntu.is_primary = true
              AND ntu.is_active = true
              AND n.date >= CURRENT_DATE - INTERVAL '%s days'
            GROUP BY tmd.category, tmd.display_name, tmd.description
            ORDER BY news_count DESC
        """, (days,))
        
        stats = []
        for row in cursor.fetchall():
            stats.append({
                'category': row[0],
                'display_name': row[1],
                'description': row[2],
                'news_count': row[3],
                'avg_confidence': float(row[4]) if row[4] else None,
                'active_days': row[5],
                'latest_news_date': row[6].isoformat() if row[6] else None
            })
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "period_days": days,
            "stats": stats,
            "total_themes": len(stats)
        }
        
    except Exception as e:
        logger.error(f"Failed to get primary theme stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取统计数据失败: {str(e)}")


@router.get("/news/{news_id}/classification", summary="获取新闻的所有分类维度")
async def get_news_classification_dimensions(
    news_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取新闻的所有分类维度（primary_theme、event_type、impact_scope、urgency、related_sectors）
    
    Args:
        news_id: 新闻ID
        
    Returns:
        分类维度信息
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        theme_service = ThemeMappingService(db_manager)
        
        dimensions = theme_service.get_classification_dimensions(news_id)
        
        if not dimensions:
            raise HTTPException(status_code=404, detail=f"新闻 {news_id} 未找到分类维度")
        
        return {
            "success": True,
            "news_id": news_id,
            "dimensions": dimensions
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get classification dimensions for news {news_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取分类维度失败: {str(e)}")


@router.get("/primary/search", summary="按 Primary Theme 搜索新闻")
async def search_news_by_primary_theme(
    category: str = Query(..., description="主题分类"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    start_date: Optional[str] = Query(None, description="开始日期 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="结束日期 (YYYY-MM-DD)"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    按 Primary Theme 搜索新闻
    
    Args:
        category: 主题分类（如：technology, finance）
        page: 页码
        page_size: 每页数量
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        符合条件的新闻列表
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        # 构建查询条件
        conditions = ["ntu.is_primary = true", "ntu.is_active = true", "tmd.category = %s"]
        params = [category]
        
        if start_date:
            conditions.append("n.date >= %s")
            params.append(start_date)
        
        if end_date:
            conditions.append("n.date <= %s")
            params.append(end_date)
        
        where_clause = " AND ".join(conditions)
        
        cursor = conn.cursor()
        
        # 统计总数
        count_query = f"""
            SELECT COUNT(DISTINCT n.id)
            FROM news n
            INNER JOIN news_themes_unified ntu ON n.id = ntu.news_id
            INNER JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
            WHERE {where_clause}
        """
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        # 分页查询
        offset = (page - 1) * page_size
        params.extend([page_size, offset])
        
        query = f"""
            SELECT 
                n.id,
                n.title,
                n.content,
                n.date,
                n.source,
                n.link,
                tmd.category as primary_theme,
                tmd.display_name as theme_display_name,
                ntu.confidence_score
            FROM news n
            INNER JOIN news_themes_unified ntu ON n.id = ntu.news_id
            INNER JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
            WHERE {where_clause}
            ORDER BY n.date DESC
            LIMIT %s OFFSET %s
        """
        
        cursor.execute(query, params)
        
        news_list = []
        for row in cursor.fetchall():
            news_list.append({
                'id': row[0],
                'title': row[1],
                'content': row[2][:200] if row[2] else None,  # 摘要
                'date': row[3].isoformat() if row[3] else None,
                'source': row[4],
                'link': row[5],
                'primary_theme': row[6],
                'theme_display_name': row[7],
                'confidence_score': float(row[8]) if row[8] else None
            })
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "category": category,
            "total": total_count,
            "page": page,
            "page_size": page_size,
            "total_pages": (total_count + page_size - 1) // page_size,
            "news": news_list
        }
        
    except Exception as e:
        logger.error(f"Failed to search news by primary theme: {str(e)}")
        raise HTTPException(status_code=500, detail=f"搜索新闻失败: {str(e)}")


@router.post("/filter/news", response_model=NewsFilterResponse, summary="多维度主题筛选新闻")
async def filter_news_by_themes(
    request: ThemeFilterRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """
    多维度主题筛选新闻
    
    筛选逻辑：
    - categories之间是OR关系（满足任一即可）
    - 不同维度之间是AND关系（必须同时满足）
    - 同一维度内是OR关系
    
    示例：
    categories=['finance'] AND event_types=['earnings_report'] 
    → 返回所有"金融类的财报新闻"
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        cursor = conn.cursor()
        
        # 构建筛选条件
        conditions = ["ntu.is_active = true"]
        main_params = []  # 主查询的参数
        subquery_params = []  # 子查询的参数
        join_clauses = ["FROM news n"]
        
        # 基础条件（用于主查询）
        if request.min_confidence:
            conditions.append("ntu.confidence_score >= %s")
            main_params.append(request.min_confidence)
        
        if request.only_primary:
            conditions.append("ntu.is_primary = true")
        
        if request.market:
            conditions.append("ntu.market = %s")
            main_params.append(request.market)
        
        if request.start_date:
            conditions.append("n.date >= %s")
            main_params.append(request.start_date)
        
        if request.end_date:
            conditions.append("n.date <= %s")
            main_params.append(request.end_date)
        
        # 构建主题筛选子查询
        theme_subqueries = []
        
        # 标准分类筛选
        if request.categories:
            placeholders = ', '.join(['%s'] * len(request.categories))
            theme_subqueries.append(f"""
                SELECT DISTINCT news_id FROM news_themes_unified ntu
                INNER JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
                WHERE ntu.is_active = true
                  AND ntu.confidence_score >= %s
                  AND tmd.category IN ({placeholders})
            """)
            subquery_params.extend([request.min_confidence] + request.categories)
        
        # 事件类型筛选
        if request.event_types:
            placeholders = ', '.join(['%s'] * len(request.event_types))
            theme_subqueries.append(f"""
                SELECT DISTINCT news_id FROM news_themes_unified
                WHERE is_active = true
                  AND confidence_score >= %s
                  AND llm_classification_title = 'event_type'
                  AND theme_name IN ({placeholders})
            """)
            subquery_params.extend([request.min_confidence] + request.event_types)
        
        # 影响范围筛选
        if request.impact_scopes:
            placeholders = ', '.join(['%s'] * len(request.impact_scopes))
            theme_subqueries.append(f"""
                SELECT DISTINCT news_id FROM news_themes_unified
                WHERE is_active = true
                  AND confidence_score >= %s
                  AND llm_classification_title = 'impact_scope'
                  AND theme_name IN ({placeholders})
            """)
            subquery_params.extend([request.min_confidence] + request.impact_scopes)
        
        # 时效性筛选
        if request.urgency_levels:
            placeholders = ', '.join(['%s'] * len(request.urgency_levels))
            theme_subqueries.append(f"""
                SELECT DISTINCT news_id FROM news_themes_unified
                WHERE is_active = true
                  AND confidence_score >= %s
                  AND llm_classification_title = 'urgency'
                  AND theme_name IN ({placeholders})
            """)
            subquery_params.extend([request.min_confidence] + request.urgency_levels)
        
        # 相关行业筛选
        if request.related_sectors:
            placeholders = ', '.join(['%s'] * len(request.related_sectors))
            theme_subqueries.append(f"""
                SELECT DISTINCT news_id FROM news_themes_unified
                WHERE is_active = true
                  AND confidence_score >= %s
                  AND llm_classification_title = 'related_sectors'
                  AND theme_name IN ({placeholders})
            """)
            subquery_params.extend([request.min_confidence] + request.related_sectors)
        
        # 相关国家/地区筛选
        if request.related_countries:
            placeholders = ', '.join(['%s'] * len(request.related_countries))
            theme_subqueries.append(f"""
                SELECT DISTINCT news_id FROM news_themes_unified
                WHERE is_active = true
                  AND confidence_score >= %s
                  AND llm_classification_title = 'related_countries'
                  AND theme_name IN ({placeholders})
            """)
            subquery_params.extend([request.min_confidence] + request.related_countries)
        
        # 来源类型筛选
        if request.source_types:
            placeholders = ', '.join(['%s'] * len(request.source_types))
            conditions.append(f"ntu.source_type IN ({placeholders})")
            main_params.extend(request.source_types)
        
        # 组合所有子查询（AND关系）
        if theme_subqueries:
            # 使用INTERSECT实现AND逻辑
            combined_subquery = " INTERSECT ".join(theme_subqueries)
            join_clauses.append(f"INNER JOIN ({combined_subquery}) AS filtered_news ON n.id = filtered_news.news_id")
        
        # 总是需要JOIN news_themes_unified，因为WHERE条件中总是会引用ntu
        join_clauses.append("INNER JOIN news_themes_unified ntu ON n.id = ntu.news_id")
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # 合并参数列表：子查询参数在前，主查询参数在后
        params = subquery_params + main_params
        
        # 统计总数
        count_query = f"""
            SELECT COUNT(DISTINCT n.id)
            {' '.join(join_clauses)}
            WHERE {where_clause}
        """
        
        cursor.execute(count_query, params)
        total_count = cursor.fetchone()[0]
        
        # 分页参数
        offset = (request.page - 1) * request.page_size
        page_params = params + [request.page_size, offset]
        
        # 查询新闻列表
        query = f"""
            SELECT DISTINCT
                n.id,
                n.title,
                n.content,
                n.date,
                n.source,
                n.link,
                n.analysis_result,
                n.analysis_status
            {' '.join(join_clauses)}
            WHERE {where_clause}
            ORDER BY n.date DESC
            LIMIT %s OFFSET %s
        """
        
        cursor.execute(query, page_params)
        
        news_list = []
        for row in cursor.fetchall():
            news_id = row[0]
            
            # 获取该新闻的所有主题
            cursor.execute("""
                SELECT 
                    ntu.theme_name,
                    ntu.source_type,
                    ntu.llm_classification_title,
                    ntu.confidence_score,
                    ntu.is_primary,
                    tmd.display_name,
                    tmd.category
                FROM news_themes_unified ntu
                LEFT JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
                WHERE ntu.news_id = %s
                  AND ntu.is_active = true
                ORDER BY ntu.is_primary DESC, ntu.confidence_score DESC
            """, (news_id,))
            
            themes = []
            for theme_row in cursor.fetchall():
                themes.append({
                    'theme_name': theme_row[0],
                    'source_type': theme_row[1],
                    'dimension': theme_row[2],
                    'confidence': float(theme_row[3]) if theme_row[3] else None,
                    'is_primary': theme_row[4],
                    'display_name': theme_row[5],
                    'category': theme_row[6]
                })
            
            news_list.append({
                'id': news_id,
                'title': row[1],
                'content': row[2][:300] if row[2] else None,  # 摘要
                'date': row[3].isoformat() if row[3] else None,
                'source': row[4],
                'link': row[5],
                'analysis_result': row[6],
                'analysis_status': row[7],
                'themes': themes
            })
        
        cursor.close()
        conn.close()
        
        # 构建筛选摘要
        filter_summary = {
            'categories': request.categories or [],
            'event_types': request.event_types or [],
            'impact_scopes': request.impact_scopes or [],
            'urgency_levels': request.urgency_levels or [],
            'related_sectors': request.related_sectors or [],
            'related_countries': request.related_countries or [],
            'source_types': request.source_types or [],
            'min_confidence': request.min_confidence,
            'only_primary': request.only_primary,
            'date_range': {
                'start': request.start_date,
                'end': request.end_date
            }
        }
        
        return {
            "success": True,
            "total": total_count,
            "page": request.page,
            "page_size": request.page_size,
            "total_pages": (total_count + request.page_size - 1) // request.page_size,
            "news": news_list,
            "filter_summary": filter_summary
        }
        
    except Exception as e:
        logger.error(f"Failed to filter news by themes: {str(e)}")
        raise HTTPException(status_code=500, detail=f"筛选新闻失败: {str(e)}")


@router.get("/trending", response_model=TrendingThemesResponse, summary="获取热门主题")
async def get_trending_themes(
    dimension: str = Query('primary_theme', description="维度：primary_theme, event_type, impact_scope, urgency, related_sectors, related_countries"),
    days: int = Query(7, ge=1, le=90, description="统计天数"),
    top_k: int = Query(10, ge=1, le=50, description="返回Top K"),
    source_type: Optional[str] = Query(None, description="来源类型筛选"),
    min_confidence: float = Query(0.7, ge=0.0, le=1.0, description="最低置信度"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取热门主题排行
    
    返回：
    - theme_name: 主题名称
    - news_count: 新闻数量
    - avg_confidence: 平均置信度
    - trend: 趋势 (↑上升/↓下降/→持平)
    - daily_distribution: 每日新闻数量分布
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        cursor = conn.cursor()
        
        # 构建查询条件
        conditions = ["ntu.is_active = true", "ntu.confidence_score >= %s"]
        params = [min_confidence]
        
        # 添加日期范围
        conditions.append("n.date >= CURRENT_DATE - INTERVAL '%s days'")
        params.append(days)
        
        # 添加来源类型筛选
        if source_type:
            conditions.append("ntu.source_type = %s")
            params.append(source_type)
        
        # 根据维度构建查询
        if dimension == 'primary_theme':
            # 主要主题：使用theme_master_data
            conditions.append("ntu.is_primary = true")
            group_by = "tmd.category, tmd.display_name"
            select_fields = "tmd.category as theme_name, tmd.display_name"
            join_clause = "INNER JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id"
        else:
            # 其他维度：使用llm_classification_title
            conditions.append("ntu.llm_classification_title = %s")
            params.append(dimension)
            group_by = "ntu.theme_name"
            select_fields = "ntu.theme_name, ntu.theme_name as display_name"
            join_clause = ""
        
        where_clause = " AND ".join(conditions)
        
        # 查询主题统计
        query = f"""
            SELECT 
                {select_fields},
                COUNT(DISTINCT ntu.news_id) as news_count,
                AVG(ntu.confidence_score) as avg_confidence
            FROM news_themes_unified ntu
            INNER JOIN news n ON ntu.news_id = n.id
            {join_clause}
            WHERE {where_clause}
            GROUP BY {group_by}
            ORDER BY news_count DESC
            LIMIT %s
        """
        
        params.append(top_k)
        cursor.execute(query, params)
        
        themes = []
        for row in cursor.fetchall():
            theme_name = row[0]
            display_name = row[1] if row[1] else theme_name
            news_count = row[2]
            avg_conf = float(row[3]) if row[3] else 0.0
            
            # 计算趋势（对比前一周期）
            trend_params = [min_confidence, days * 2, days, theme_name]
            if source_type:
                trend_conditions = "ntu.is_active = true AND ntu.confidence_score >= %s AND ntu.source_type = %s AND n.date >= CURRENT_DATE - INTERVAL '%s days' AND n.date < CURRENT_DATE - INTERVAL '%s days'"
                trend_params = [min_confidence, source_type, days * 2, days, theme_name]
            else:
                trend_conditions = "ntu.is_active = true AND ntu.confidence_score >= %s AND n.date >= CURRENT_DATE - INTERVAL '%s days' AND n.date < CURRENT_DATE - INTERVAL '%s days'"
            
            if dimension == 'primary_theme':
                trend_query = f"""
                    SELECT COUNT(DISTINCT ntu.news_id)
                    FROM news_themes_unified ntu
                    INNER JOIN news n ON ntu.news_id = n.id
                    INNER JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
                    WHERE {trend_conditions} AND ntu.is_primary = true AND tmd.category = %s
                """
            else:
                trend_conditions += " AND ntu.llm_classification_title = %s"
                trend_params.insert(-1, dimension)
                trend_query = f"""
                    SELECT COUNT(DISTINCT ntu.news_id)
                    FROM news_themes_unified ntu
                    INNER JOIN news n ON ntu.news_id = n.id
                    WHERE {trend_conditions} AND ntu.theme_name = %s
                """
            
            cursor.execute(trend_query, trend_params)
            prev_count = cursor.fetchone()[0]
            
            # 计算趋势
            if prev_count == 0:
                trend = '→'
            elif news_count > prev_count * 1.2:
                trend = '↑'
            elif news_count < prev_count * 0.8:
                trend = '↓'
            else:
                trend = '→'
            
            # 获取每日分布
            daily_params = [min_confidence, days, theme_name]
            if source_type:
                daily_conditions = "ntu.is_active = true AND ntu.confidence_score >= %s AND ntu.source_type = %s AND n.date >= CURRENT_DATE - INTERVAL '%s days'"
                daily_params = [min_confidence, source_type, days, theme_name]
            else:
                daily_conditions = "ntu.is_active = true AND ntu.confidence_score >= %s AND n.date >= CURRENT_DATE - INTERVAL '%s days'"
            
            if dimension == 'primary_theme':
                daily_query = f"""
                    SELECT 
                        DATE(n.date) as day,
                        COUNT(DISTINCT ntu.news_id) as count
                    FROM news_themes_unified ntu
                    INNER JOIN news n ON ntu.news_id = n.id
                    INNER JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
                    WHERE {daily_conditions} AND ntu.is_primary = true AND tmd.category = %s
                    GROUP BY DATE(n.date)
                    ORDER BY day DESC
                """
            else:
                daily_conditions += " AND ntu.llm_classification_title = %s"
                daily_params.insert(-1, dimension)
                daily_query = f"""
                    SELECT 
                        DATE(n.date) as day,
                        COUNT(DISTINCT ntu.news_id) as count
                    FROM news_themes_unified ntu
                    INNER JOIN news n ON ntu.news_id = n.id
                    WHERE {daily_conditions} AND ntu.theme_name = %s
                    GROUP BY DATE(n.date)
                    ORDER BY day DESC
                """
            
            cursor.execute(daily_query, daily_params)
            daily_distribution = [
                {'date': row[0].isoformat(), 'count': row[1]}
                for row in cursor.fetchall()
            ]
            
            themes.append({
                'theme_name': theme_name,
                'display_name': display_name,
                'news_count': news_count,
                'avg_confidence': avg_conf,
                'trend': trend,
                'daily_distribution': daily_distribution
            })
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "dimension": dimension,
            "period_days": days,
            "themes": themes,
            "total_themes": len(themes)
        }
        
    except Exception as e:
        logger.error(f"Failed to get trending themes: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取热门主题失败: {str(e)}")


@router.get("/dimensions", response_model=DimensionValuesResponse, summary="获取指定维度的所有可能值")
async def get_dimension_values(
    dimension: str = Query(..., description="维度：primary_theme, event_type, impact_scope, urgency, related_sectors, related_countries, source_type"),
    days: Optional[int] = Query(None, ge=1, le=365, description="统计最近N天的数据"),
    min_count: int = Query(1, ge=1, description="最小新闻数量"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取指定维度的所有可能值及其统计
    
    用于填充筛选器的下拉选项
    
    Args:
        dimension: 维度名称
        days: 只统计最近N天的数据
        min_count: 只返回至少有N条新闻的值
        
    Returns:
        该维度的所有值及其新闻数量
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        cursor = conn.cursor()
        
        # 构建查询
        if dimension == 'primary_theme':
            # 标准分类
            query = """
                SELECT 
                    tmd.category as value,
                    tmd.display_name,
                    tmd.description,
                    COUNT(DISTINCT ntu.news_id) as news_count,
                    AVG(ntu.confidence_score) as avg_confidence
                FROM news_themes_unified ntu
                INNER JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
                INNER JOIN news n ON ntu.news_id = n.id
                WHERE ntu.is_primary = true
                  AND ntu.is_active = true
            """
            
            if days:
                query += " AND n.date >= CURRENT_DATE - INTERVAL '%s days'"
                params = [days, min_count]
            else:
                params = [min_count]
            
            query += """
                GROUP BY tmd.category, tmd.display_name, tmd.description
                HAVING COUNT(DISTINCT ntu.news_id) >= %s
                ORDER BY news_count DESC
            """
            
        elif dimension == 'source_type':
            # 来源类型
            query = """
                SELECT 
                    ntu.source_type as value,
                    ntu.source_type as display_name,
                    '' as description,
                    COUNT(DISTINCT ntu.news_id) as news_count,
                    AVG(ntu.confidence_score) as avg_confidence
                FROM news_themes_unified ntu
                INNER JOIN news n ON ntu.news_id = n.id
                WHERE ntu.is_active = true
            """
            
            if days:
                query += " AND n.date >= CURRENT_DATE - INTERVAL '%s days'"
                params = [days, min_count]
            else:
                params = [min_count]
            
            query += """
                GROUP BY ntu.source_type
                HAVING COUNT(DISTINCT ntu.news_id) >= %s
                ORDER BY news_count DESC
            """
            
        else:
            # 其他维度（event_type, impact_scope, urgency, related_sectors, related_countries）
            query = """
                SELECT 
                    ntu.theme_name as value,
                    ntu.theme_name as display_name,
                    '' as description,
                    COUNT(DISTINCT ntu.news_id) as news_count,
                    AVG(ntu.confidence_score) as avg_confidence
                FROM news_themes_unified ntu
                INNER JOIN news n ON ntu.news_id = n.id
                WHERE ntu.is_active = true
                  AND ntu.llm_classification_title = %s
            """
            
            if days:
                query += " AND n.date >= CURRENT_DATE - INTERVAL '%s days'"
                params = [dimension, days, min_count]
            else:
                params = [dimension, min_count]
            
            query += """
                GROUP BY ntu.theme_name
                HAVING COUNT(DISTINCT ntu.news_id) >= %s
                ORDER BY news_count DESC
            """
        
        cursor.execute(query, params)
        
        values = []
        for row in cursor.fetchall():
            values.append({
                'value': row[0],
                'display_name': row[1] if row[1] else row[0],
                'description': row[2],
                'news_count': row[3],
                'avg_confidence': float(row[4]) if row[4] else None
            })
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "dimension": dimension,
            "values": values,
            "total": len(values)
        }
        
    except Exception as e:
        logger.error(f"Failed to get dimension values: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取维度值失败: {str(e)}")
