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
