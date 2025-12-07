# -*- coding: utf-8 -*-
"""
新闻API路由 - 基于数据库中的新闻数据
"""

from fastapi import APIRouter, HTTPException, status, Depends, Query, Body
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta

from backend.schemas.news import (
    NewsResponse,
    NewsListResponse,
    NewsTopicResponse,
    NewsSymbolResponse,
    NewsTagResponse,
    DeleteNewsByDateRangeRequest,
    DeleteNewsByDateRangeStats,
    DeleteNewsByDateRangeResponse
)
from backend.auth.jwt_utils import get_current_user, get_current_admin_user
from backend.database.db_manager import DatabaseManager
from backend.services.eodhd_news_service import EODHDNewsService, NewsThemeService
from backend.utils.logger import get_logger
from backend.utils.database_config import get_database_config

# 获取日志实例
logger = get_logger("news_api")

router = APIRouter()




class NewsService:
    """新闻数据库服务类"""
    
    def __init__(self, db_conn_params: Dict[str, Any]):
        self.db_manager = DatabaseManager(db_conn_params)
    
    def get_news_list(
        self, 
        page: int = 1, 
        page_size: int = 20,
        topic_id: Optional[int] = None,
        symbol_code: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        search_text: Optional[str] = None
    ) -> Dict[str, Any]:
        """获取新闻列表"""
        conn = self.db_manager.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        try:
            cursor = conn.cursor()
            
            # 构建基础查询 - 简化版本，先不包含复杂的JOIN
            base_query = """
                SELECT n.id, n.date, n.title, n.content, n.link, n.source,
                       n.sentiment_polarity, n.sentiment_neg, n.sentiment_neu, n.sentiment_pos,
                       n.created_at, n.updated_at, n.scheduler_source, n.analysis_result
                FROM news n
            """
            
            conditions = []
            params = []
            joins_added = []
            
            # 添加股票代码过滤
            if symbol_code:
                base_query += """
                    INNER JOIN news_symbols ns ON n.id = ns.news_id
                    INNER JOIN news_symbols_dict nsd ON ns.symbol_id = nsd.id
                """
                conditions.append("nsd.symbol_code = %s")
                params.append(symbol_code)
                joins_added.append('symbol')
            
            # 添加主题过滤 - 简化逻辑
            if topic_id:
                # 暂时忽略复杂的主题关键词匹配，直接通过主题ID匹配（如果有直接关联的话）
                # 这里我们先不实现主题过滤，等基本功能正常后再添加
                pass
            
            # 添加日期过滤
            if start_date:
                conditions.append("n.date >= %s")
                params.append(start_date)
            
            if end_date:
                conditions.append("n.date <= %s")
                params.append(end_date)
            
            # 添加文本搜索
            if search_text:
                conditions.append("(n.title ILIKE %s OR n.content ILIKE %s)")
                search_pattern = f"%{search_text}%"
                params.extend([search_pattern, search_pattern])
            
            # 组装WHERE子句
            if conditions:
                base_query += " WHERE " + " AND ".join(conditions)
            
            # 计算总数
            count_query = base_query.replace(
                "SELECT n.id, n.date, n.title, n.content, n.link, n.source,\n                       n.sentiment_polarity, n.sentiment_neg, n.sentiment_neu, n.sentiment_pos,\n                       n.created_at, n.updated_at, n.scheduler_source, n.analysis_result",
                "SELECT COUNT(n.id)"
            )
            cursor.execute(count_query, params)
            total_count = cursor.fetchone()[0]
            
            # 添加排序和分页
            base_query += " ORDER BY n.date DESC LIMIT %s OFFSET %s"
            params.extend([page_size, (page - 1) * page_size])
            
            cursor.execute(base_query, params)
            rows = cursor.fetchall()
            
            news_list = []
            for row in rows:
                news_item = {
                    'id': row[0],
                    'date': row[1].isoformat() if row[1] else None,
                    'title': row[2],
                    'content': row[3],
                    'link': row[4],
                    'source': row[5],
                    'sentiment_polarity': row[6],
                    'sentiment_neg': row[7],
                    'sentiment_neu': row[8],
                    'sentiment_pos': row[9],
                    'created_at': row[10].isoformat() if row[10] else None,
                    'updated_at': row[11].isoformat() if row[11] else None,
                    'scheduler_source': row[12],
                    'analysis_result': row[13],  # 添加 analysis_result 字段
                    'symbols': [],
                    'tags': [],
                    'auto_tags': []
                }
                
                # 获取相关股票代码
                symbol_query = """
                    SELECT nsd.symbol_code
                    FROM news_symbols ns
                    JOIN news_symbols_dict nsd ON ns.symbol_id = nsd.id
                    WHERE ns.news_id = %s
                """
                cursor.execute(symbol_query, (row[0],))
                symbols = [s[0] for s in cursor.fetchall()]
                news_item['symbols'] = symbols
                
                # 获取相关标签
                tag_query = """
                    SELECT ntd.tag_name
                    FROM news_tags nt
                    JOIN news_tags_dict ntd ON nt.tag_id = ntd.id
                    WHERE nt.news_id = %s
                """
                cursor.execute(tag_query, (row[0],))
                tags = [t[0] for t in cursor.fetchall()]
                news_item['tags'] = tags
                
                # 获取相关auto tags
                auto_tag_query = """
                    SELECT nat.tag_name
                    FROM news_auto_tags_relations natr
                    JOIN news_auto_tags nat ON natr.auto_tag_id = nat.id
                    WHERE natr.news_id = %s
                """
                cursor.execute(auto_tag_query, (row[0],))
                auto_tags = [t[0] for t in cursor.fetchall()]
                news_item['auto_tags'] = auto_tags
                
                # 获取投资主题匹配结果
                theme_query = """
                    SELECT 
                        theme_id, theme_name, category, similarity_score, match_rank
                    FROM news_theme_matches
                    WHERE news_id = %s
                    ORDER BY match_rank ASC
                    LIMIT 5
                """
                cursor.execute(theme_query, (row[0],))
                theme_matches = []
                for theme_row in cursor.fetchall():
                    theme_matches.append({
                        'theme_id': theme_row[0],
                        'theme_name': theme_row[1],
                        'category': theme_row[2],
                        'similarity': float(theme_row[3]) if theme_row[3] is not None else 0.0,
                        'confidence': float(theme_row[3]) if theme_row[3] is not None else 0.0,  # 使用相似度作为置信度
                        'rank': theme_row[4]
                    })
                news_item['theme_matches'] = theme_matches
                
                # 获取统一主题数据 (news_themes_unified)
                unified_themes_query = """
                    SELECT 
                        ntu.id,
                        ntu.theme_name,
                        ntu.canonical_theme_name,
                        ntu.source_type,
                        ntu.confidence_score,
                        ntu.quality_score,
                        ntu.rank_in_news,
                        ntu.market,
                        ntu.is_verified,
                        ntu.is_primary,
                        ntu.metadata,
                        tmd.display_name,
                        tmd.category,
                        tmd.sub_category,
                        tmd.description
                    FROM news_themes_unified ntu
                    LEFT JOIN theme_master_data tmd ON ntu.theme_master_id = tmd.id
                    WHERE ntu.news_id = %s 
                      AND ntu.is_active = true
                    ORDER BY ntu.quality_score DESC NULLS LAST, ntu.confidence_score DESC
                    LIMIT 10
                """
                cursor.execute(unified_themes_query, (row[0],))
                unified_themes = []
                for ut_row in cursor.fetchall():
                    unified_themes.append({
                        'id': ut_row[0],
                        'theme_name': ut_row[1],
                        'canonical_theme_name': ut_row[2],
                        'source_type': ut_row[3],
                        'confidence_score': float(ut_row[4]) if ut_row[4] is not None else 0.0,
                        'quality_score': float(ut_row[5]) if ut_row[5] is not None else 0.0,
                        'rank_in_news': ut_row[6],
                        'market': ut_row[7],
                        'is_verified': ut_row[8],
                        'is_primary': ut_row[9],
                        'metadata': ut_row[10],
                        'display_name': ut_row[11],
                        'category': ut_row[12],
                        'sub_category': ut_row[13],
                        'description': ut_row[14]
                    })
                news_item['unified_themes'] = unified_themes
                
                news_list.append(news_item)
            
            return {
                'news': news_list,
                'total': total_count,
                'page': page,
                'page_size': page_size,
                'total_pages': (total_count + page_size - 1) // page_size
            }
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"查询新闻失败: {str(e)}")
        finally:
            cursor.close()
            conn.close()
    
    def get_news_topics(self) -> List[Dict[str, Any]]:
        """获取新闻主题列表"""
        conn = self.db_manager.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        try:
            cursor = conn.cursor()
            query = """
                SELECT id, topic_name, description, is_active, color, created_at, updated_at
                FROM news_topics
                WHERE is_active = true
                ORDER BY topic_name
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            
            topics = []
            for row in rows:
                topic = {
                    'id': row[0],
                    'topic_name': row[1],
                    'description': row[2],
                    'is_active': row[3],
                    'color': row[4],
                    'created_at': row[5].isoformat() if row[5] else None,
                    'updated_at': row[6].isoformat() if row[6] else None
                }
                topics.append(topic)
            
            return topics
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"获取主题失败: {str(e)}")
        finally:
            cursor.close()
            conn.close()
    
    def get_news_symbols(self) -> List[Dict[str, Any]]:
        """获取新闻相关的股票代码列表"""
        conn = self.db_manager.get_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        try:
            cursor = conn.cursor()
            query = """
                SELECT DISTINCT nsd.symbol_code, COUNT(ns.news_id) as news_count
                FROM news_symbols_dict nsd
                LEFT JOIN news_symbols ns ON nsd.id = ns.symbol_id
                GROUP BY nsd.symbol_code
                ORDER BY news_count DESC, nsd.symbol_code
                LIMIT 100
            """
            cursor.execute(query)
            rows = cursor.fetchall()
            
            symbols = []
            for row in rows:
                symbol = {
                    'symbol_code': row[0],
                    'news_count': row[1]
                }
                symbols.append(symbol)
            
            return symbols
            
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"获取股票代码失败: {str(e)}")
        finally:
            cursor.close()
            conn.close()


@router.get("/", response_model=NewsListResponse, summary="获取新闻列表")
async def get_news_list(
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页数量"),
    topic_id: Optional[int] = Query(None, description="主题ID过滤"),
    symbol_code: Optional[str] = Query(None, description="股票代码过滤"),
    start_date: Optional[str] = Query(None, description="开始日期 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="结束日期 (YYYY-MM-DD)"),
    search_text: Optional[str] = Query(None, description="搜索文本"),
    current_user=Depends(get_current_user)
):
    """
    获取新闻列表，支持分页和多种过滤条件
    """
    try:
        # 解析日期参数
        start_datetime = None
        end_datetime = None
        
        if start_date:
            try:
                start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="开始日期格式错误，请使用 YYYY-MM-DD")
        
        if end_date:
            try:
                end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="结束日期格式错误，请使用 YYYY-MM-DD")
        
        db_conn_params = get_database_config()
        news_service = NewsService(db_conn_params)
        
        result = news_service.get_news_list(
            page=page,
            page_size=page_size,
            topic_id=topic_id,
            symbol_code=symbol_code,
            start_date=start_datetime,
            end_date=end_datetime,
            search_text=search_text
        )
        
        return result
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取新闻列表失败: {str(e)}")


@router.get("/topics", response_model=List[NewsTopicResponse], summary="获取新闻主题列表")
async def get_news_topics(current_user=Depends(get_current_user)):
    """
    获取所有激活的新闻主题
    """
    try:
        db_conn_params = get_database_config()
        news_service = NewsService(db_conn_params)
        topics = news_service.get_news_topics()
        return topics
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取新闻主题失败: {str(e)}")


@router.get("/symbols", response_model=List[NewsSymbolResponse], summary="获取新闻相关股票代码")
async def get_news_symbols(current_user=Depends(get_current_user)):
    """
    获取新闻中涉及的股票代码列表，按新闻数量排序
    """
    try:
        db_conn_params = get_database_config()
        news_service = NewsService(db_conn_params)
        symbols = news_service.get_news_symbols()
        return symbols
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取股票代码失败: {str(e)}")




# ============ EODHD新闻获取API ============

@router.post("/fetch", summary="Get News from EODHD")
async def fetch_news_from_eodhd(
    symbols: Optional[str] = Query(None, description="股票代码，逗号分隔"),
    topic: Optional[str] = Query(None, description="题材标签"),
    start_date: Optional[str] = Query(None, description="开始日期 (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="结束日期 (YYYY-MM-DD)"),
    max_results: int = Query(100, ge=1, le=1000, description="最大结果数"),
    topic_filter: Optional[str] = Query(None, description="主题过滤器"),
    keywords: Optional[str] = Query(None, description="关键词，逗号分隔"),
    current_user=Depends(get_current_user)
):
    """
    从EODHD API获取新闻并存储到数据库
    """
    
    # 记录请求开始
    logger.info("开始新闻获取请求", 
                user=current_user.username if current_user else "unknown",
                symbols=symbols,
                topic=topic,
                start_date=start_date,
                end_date=end_date,
                max_results=max_results,
                topic_filter=topic_filter,
                keywords=keywords)
    
    try:
        # 解析关键词
        keywords_list = None
        if keywords:
            keywords_list = [kw.strip() for kw in keywords.split(',') if kw.strip()]
            logger.debug("解析关键词完成", keywords_count=len(keywords_list), keywords=keywords_list)
        
        # 创建EODHD新闻服务
        logger.info("初始化EODHD新闻服务")
        eodhd_service = EODHDNewsService()
        
        # 获取新闻
        logger.info("开始调用EODHD新闻服务获取新闻")
        result = eodhd_service.fetch_and_store_news(
            symbols=symbols,
            topic=topic,
            start_date=start_date,
            end_date=end_date,
            max_results=max_results,
            topic_filter=topic_filter,
            keywords=keywords_list
        )
        
        # 记录成功结果
        logger.info("新闻获取请求完成", 
                   fetched_count=result['fetched_count'],
                   success_count=result['success_count'],
                   existing_count=result['existing_count'],
                   failed_count=result['failed_count'],
                   total_count=result['total_count'])
        
        # 获取实际新增的新闻数量（从result中直接获取）
        new_count = result.get('new_count', 0)
        
        if result['existing_count'] > 0:
            message = f"成功获取 {result['fetched_count']} 条新闻，其中新增 {new_count} 条，{result['existing_count']} 条已存在"
        else:
            message = f"成功获取并存储 {result['success_count']} 条新闻"
        
        return {
            "success": True,
            "message": message,
            "data": result
        }
        
    except ValueError as e:
        logger.error("新闻获取请求参数错误", exception=str(e), 
                    user=current_user.username if current_user else "unknown")
        raise HTTPException(status_code=400, detail=f"参数错误: {str(e)}")
    except Exception as e:
        logger.error("新闻获取请求失败", exception=str(e), 
                    user=current_user.username if current_user else "unknown")
        raise HTTPException(status_code=500, detail=f"获取新闻失败: {str(e)}")


@router.get("/test-eodhd", summary="测试EODHD API连接")
async def test_eodhd_connection(current_user=Depends(get_current_user)):
    """
    测试EODHD API连接是否正常
    """
    try:
        eodhd_service = EODHDNewsService()
        
        # 尝试获取1条新闻进行测试
        test_result = eodhd_service.get_news(limit=1)
        
        if test_result:
            return {
                "success": True,
                "message": "EODHD API连接正常",
                "data": {"api_available": True}
            }
        else:
            return {
                "success": False,
                "message": "EODHD API连接失败，请检查API密钥",
                "data": {"api_available": False}
            }
            
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"配置错误: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"测试连接失败: {str(e)}")


# ============ 主题管理API ============

@router.get("/themes", summary="获取所有新闻主题")
async def get_news_themes(current_user=Depends(get_current_user)):
    """
    获取所有新闻主题及其关键词
    """
    try:
        theme_service = NewsThemeService()
        themes = theme_service.get_all_themes()
        
        return {
            "success": True,
            "data": themes,
            "total": len(themes)
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取主题失败: {str(e)}")


@router.post("/themes", summary="创建新主题")
async def create_news_theme(
    topic_name: str = Query(..., description="主题名称"),
    description: Optional[str] = Query(None, description="主题描述"),
    keywords: Optional[str] = Query(None, description="关键词，逗号分隔"),
    color: Optional[str] = Query(None, description="主题颜色 (hex)"),
    current_user=Depends(get_current_user)
):
    """
    创建新的新闻主题
    """
    try:
        # 解析关键词
        keywords_list = []
        if keywords:
            keywords_list = [kw.strip() for kw in keywords.split(',') if kw.strip()]
        
        theme_service = NewsThemeService()
        theme_id = theme_service.create_theme(
            topic_name=topic_name,
            description=description,
            keywords=keywords_list,
            color=color
        )
        
        if theme_id:
            return {
                "success": True,
                "message": f"主题 '{topic_name}' 创建成功",
                "data": {"theme_id": theme_id}
            }
        else:
            raise HTTPException(status_code=400, detail="创建主题失败，可能名称已存在")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"创建主题失败: {str(e)}")


@router.put("/themes/{theme_id}", summary="更新主题")
async def update_news_theme(
    theme_id: int,
    topic_name: Optional[str] = Query(None, description="主题名称"),
    description: Optional[str] = Query(None, description="主题描述"),
    is_active: Optional[bool] = Query(None, description="是否激活"),
    color: Optional[str] = Query(None, description="主题颜色 (hex)"),
    current_user=Depends(get_current_user)
):
    """
    更新主题信息
    """
    try:
        theme_service = NewsThemeService()
        success = theme_service.update_theme(
            theme_id=theme_id,
            topic_name=topic_name,
            description=description,
            is_active=is_active,
            color=color
        )
        
        if success:
            return {
                "success": True,
                "message": "主题更新成功"
            }
        else:
            raise HTTPException(status_code=404, detail="主题不存在")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"更新主题失败: {str(e)}")


@router.delete("/themes/{theme_id}", summary="删除主题")
async def delete_news_theme(
    theme_id: int,
    current_user=Depends(get_current_user)
):
    """
    删除主题（会同时删除相关关键词）
    """
    try:
        theme_service = NewsThemeService()
        success = theme_service.delete_theme(theme_id)
        
        if success:
            return {
                "success": True,
                "message": "主题删除成功"
            }
        else:
            raise HTTPException(status_code=404, detail="主题不存在")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除主题失败: {str(e)}")


@router.post("/themes/{theme_id}/keywords", summary="为主题添加关键词")
async def add_theme_keywords(
    theme_id: int,
    keywords: str = Query(..., description="关键词，逗号分隔"),
    weights: Optional[str] = Query(None, description="权重，逗号分隔，默认为1.0"),
    current_user=Depends(get_current_user)
):
    """
    为主题添加关键词
    """
    try:
        # 解析关键词
        keywords_list = [kw.strip() for kw in keywords.split(',') if kw.strip()]
        
        # 解析权重
        weights_list = None
        if weights:
            try:
                weights_list = [float(w.strip()) for w in weights.split(',') if w.strip()]
                if len(weights_list) != len(keywords_list):
                    weights_list = None  # 如果权重数量不匹配，使用默认权重
            except ValueError:
                weights_list = None
        
        theme_service = NewsThemeService()
        success = theme_service.add_theme_keywords(
            theme_id=theme_id,
            keywords=keywords_list,
            weights=weights_list
        )
        
        if success:
            return {
                "success": True,
                "message": f"成功为主题添加 {len(keywords_list)} 个关键词"
            }
        else:
            raise HTTPException(status_code=400, detail="添加关键词失败")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"添加关键词失败: {str(e)}")


@router.delete("/themes/{theme_id}/keywords", summary="删除主题关键词")
async def remove_theme_keywords(
    theme_id: int,
    keywords: str = Query(..., description="要删除的关键词，逗号分隔"),
    current_user=Depends(get_current_user)
):
    """
    从主题中删除关键词
    """
    try:
        # 解析关键词
        keywords_list = [kw.strip() for kw in keywords.split(',') if kw.strip()]
        
        theme_service = NewsThemeService()
        success = theme_service.remove_theme_keywords(
            theme_id=theme_id,
            keywords=keywords_list
        )
        
        if success:
            return {
                "success": True,
                "message": f"成功删除 {len(keywords_list)} 个关键词"
            }
        else:
            raise HTTPException(status_code=400, detail="删除关键词失败")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除关键词失败: {str(e)}")


# ============ 主题初始化API ============

@router.post("/themes/initialize", summary="初始化默认主题")
async def initialize_default_themes(current_user=Depends(get_current_user)):
    """
    初始化系统默认主题（如果尚未存在）
    """
    try:
        from backend.services.news_theme_init import initialize_default_themes
        created_count = initialize_default_themes()
        
        return {
            "success": True,
            "message": f"初始化完成，创建了 {created_count} 个新主题",
            "data": {"created_count": created_count}
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"初始化主题失败: {str(e)}")


@router.post("/themes/{theme_id}/reset", summary="重置主题到默认状态")
async def reset_theme_to_default(
    theme_id: int,
    current_user=Depends(get_current_user)
):
    """
    重置主题到系统默认状态（仅适用于系统预定义主题）
    """
    try:
        from backend.services.news_theme_init import get_default_themes_config
        
        theme_service = NewsThemeService()
        
        # 获取当前主题信息
        themes = theme_service.get_all_themes()
        current_theme = None
        for theme in themes:
            if theme['id'] == theme_id:
                current_theme = theme
                break
        
        if not current_theme:
            raise HTTPException(status_code=404, detail="主题不存在")
        
        # 获取默认配置
        default_config = get_default_themes_config()
        theme_name = current_theme['topic_name'].lower()
        
        if theme_name not in default_config:
            raise HTTPException(
                status_code=400, 
                detail=f"主题 '{current_theme['topic_name']}' 不是系统预定义主题，无法重置"
            )
        
        config = default_config[theme_name]
        
        # 删除现有关键词
        if current_theme['keywords']:
            theme_service.remove_theme_keywords(theme_id, current_theme['keywords'])
        
        # 添加默认关键词
        theme_service.add_theme_keywords(theme_id, config['keywords'])
        
        # 更新主题信息
        theme_service.update_theme(
            theme_id=theme_id,
            description=config['description'],
            color=config['color']
        )
        
        return {
            "success": True,
            "message": f"主题 '{current_theme['topic_name']}' 已重置到默认状态",
            "data": {
                "keywords_count": len(config['keywords']),
                "description": config['description'],
                "color": config['color']
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"重置主题失败: {str(e)}")

# ============ 新闻详情API (参数化路由，必须在最后) ============

@router.get("/{news_id}", response_model=NewsResponse, summary="获取新闻详情")
async def get_news_detail(
    news_id: int,
    current_user=Depends(get_current_user)
):
    """
    获取指定ID的新闻详情
    """
    db_conn_params = get_database_config()
    news_service = NewsService(db_conn_params)
    
    conn = news_service.db_manager.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="数据库连接失败")
    
    try:
        cursor = conn.cursor()
        query = """
            SELECT id, date, title, content, link, source, 
                   sentiment_polarity, sentiment_neg, sentiment_neu, sentiment_pos,
                   created_at, updated_at, analysis_result
            FROM news
            WHERE id = %s
        """
        cursor.execute(query, (news_id,))
        row = cursor.fetchone()
        
        if not row:
            raise HTTPException(status_code=404, detail="新闻未找到")
        
        news_item = {
            'id': row[0],
            'date': row[1].isoformat() if row[1] else None,
            'title': row[2],
            'content': row[3],
            'link': row[4],
            'source': row[5],
            'sentiment_polarity': row[6],
            'sentiment_neg': row[7],
            'sentiment_neu': row[8],
            'sentiment_pos': row[9],
            'created_at': row[10].isoformat() if row[10] else None,
            'updated_at': row[11].isoformat() if row[11] else None,
            'analysis_result': row[12],  # 添加 analysis_result 字段
            'symbols': [],
            'tags': [],
            'auto_tags': []
        }
        
        # 获取相关股票代码
        symbol_query = """
            SELECT nsd.symbol_code
            FROM news_symbols ns
            JOIN news_symbols_dict nsd ON ns.symbol_id = nsd.id
            WHERE ns.news_id = %s
        """
        cursor.execute(symbol_query, (news_id,))
        symbols = [s[0] for s in cursor.fetchall()]
        news_item['symbols'] = symbols
        
        # 获取相关标签
        tag_query = """
            SELECT ntd.tag_name
            FROM news_tags nt
            JOIN news_tags_dict ntd ON nt.tag_id = ntd.id
            WHERE nt.news_id = %s
        """
        cursor.execute(tag_query, (news_id,))
        tags = [t[0] for t in cursor.fetchall()]
        news_item['tags'] = tags
        
        # 获取相关auto tags
        auto_tag_query = """
            SELECT nat.tag_name
            FROM news_auto_tags_relations natr
            JOIN news_auto_tags nat ON natr.auto_tag_id = nat.id
            WHERE natr.news_id = %s
        """
        cursor.execute(auto_tag_query, (news_id,))
        auto_tags = [t[0] for t in cursor.fetchall()]
        news_item['auto_tags'] = auto_tags
        
        # 获取投资主题匹配结果
        theme_query = """
            SELECT 
                theme_id, theme_name, category, similarity_score, match_rank
            FROM news_theme_matches
            WHERE news_id = %s
            ORDER BY match_rank ASC
            LIMIT 5
        """
        cursor.execute(theme_query, (news_id,))
        theme_matches = []
        for theme_row in cursor.fetchall():
            theme_matches.append({
                'theme_id': theme_row[0],
                'theme_name': theme_row[1],
                'category': theme_row[2],
                'similarity': float(theme_row[3]) if theme_row[3] is not None else 0.0,
                'confidence': float(theme_row[3]) if theme_row[3] is not None else 0.0,  # 使用相似度作为置信度
                'rank': theme_row[4]
            })
        news_item['theme_matches'] = theme_matches
        
        return news_item
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取新闻详情失败: {str(e)}")
    finally:
        cursor.close()
        conn.close()


# ============ 按日期范围删除新闻API ============

@router.post("/delete-by-date-range/preview", response_model=DeleteNewsByDateRangeStats, summary="预览按日期范围删除新闻的统计")
async def preview_delete_news_by_date_range(
    request: DeleteNewsByDateRangeRequest,
    current_user=Depends(get_current_admin_user)
):
    """
    预览按日期范围删除新闻的统计信息
    
    **权限要求**: 管理员
    
    **注意**: 
    - 此接口仅预览，不会实际删除数据
    - 会统计所有将被删除的记录数
    - 日期范围为左闭右闭区间，包含开始和结束日期当天的所有新闻
    """
    try:
        # 解析日期（左闭右闭区间：包含开始和结束日期）
        try:
            start_datetime = datetime.strptime(request.start_date, "%Y-%m-%d")
            # 结束日期+1天，转换为左闭右开区间以包含结束日期当天的所有数据
            end_datetime = datetime.strptime(request.end_date, "%Y-%m-%d") + timedelta(days=1)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"日期格式错误: {str(e)}")
        
        # 验证日期范围
        if start_datetime >= end_datetime:
            raise HTTPException(status_code=400, detail="开始日期必须早于结束日期")
        
        # 获取数据库连接
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        try:
            cursor = conn.cursor()
            
            # 统计将要删除的记录数
            # 1. 统计news数量
            cursor.execute("""
                SELECT COUNT(*) FROM news
                WHERE date >= %s AND date < %s
            """, (start_datetime, end_datetime))
            news_count = cursor.fetchone()[0]
            
            # 2. 统计news_symbols数量
            cursor.execute("""
                SELECT COUNT(*) FROM news_symbols
                WHERE news_id IN (
                    SELECT id FROM news
                    WHERE date >= %s AND date < %s
                )
            """, (start_datetime, end_datetime))
            news_symbols_count = cursor.fetchone()[0]
            
            # 3. 统计news_tags数量
            cursor.execute("""
                SELECT COUNT(*) FROM news_tags
                WHERE news_id IN (
                    SELECT id FROM news
                    WHERE date >= %s AND date < %s
                )
            """, (start_datetime, end_datetime))
            news_tags_count = cursor.fetchone()[0]
            
            # 4. 统计news_theme_matches数量
            cursor.execute("""
                SELECT COUNT(*) FROM news_theme_matches
                WHERE news_id IN (
                    SELECT id FROM news
                    WHERE date >= %s AND date < %s
                )
            """, (start_datetime, end_datetime))
            news_theme_matches_count = cursor.fetchone()[0]
            
            # 5. 统计news_themes_unified数量
            cursor.execute("""
                SELECT COUNT(*) FROM news_themes_unified
                WHERE news_id IN (
                    SELECT id FROM news
                    WHERE date >= %s AND date < %s
                )
            """, (start_datetime, end_datetime))
            news_themes_unified_count = cursor.fetchone()[0]
            
            total_count = (
                news_count + 
                news_symbols_count + 
                news_tags_count + 
                news_theme_matches_count + 
                news_themes_unified_count
            )
            
            logger.info(f"预览删除统计 - 用户: {current_user.username}, 日期范围: {request.start_date} ~ {request.end_date}, "
                       f"news: {news_count}, 总计: {total_count}")
            
            return DeleteNewsByDateRangeStats(
                news_count=news_count,
                news_symbols_count=news_symbols_count,
                news_tags_count=news_tags_count,
                news_theme_matches_count=news_theme_matches_count,
                news_themes_unified_count=news_themes_unified_count,
                project_news_tracking_count=0,
                total_count=total_count
            )
            
        finally:
            cursor.close()
            conn.close()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"预览删除统计失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"预览删除统计失败: {str(e)}")


@router.delete("/delete-by-date-range", response_model=DeleteNewsByDateRangeResponse, summary="按日期范围删除新闻")
async def delete_news_by_date_range(
    request: DeleteNewsByDateRangeRequest = Body(...),
    current_user=Depends(get_current_admin_user)
):
    """
    按日期范围删除新闻及其所有关联数据
    
    **权限要求**: 管理员
    
    **注意事项**:
    - 此操作不可逆，请谨慎使用
    - 会级联删除以下关联表的数据：
      - news_symbols (新闻-股票关联)
      - news_tags (新闻-标签关联)
      - news_theme_matches (新闻-主题匹配)
      - news_themes_unified (新闻-统一主题)
      - project_news_tracking (项目新闻跟踪) - 已移除
    - 必须将 confirm 字段设置为 true 才能执行删除
    - 建议先调用预览接口查看统计信息
    
    **删除过程**:
    1. 验证权限和参数
    2. 统计将要删除的记录数
    3. 在事务中执行删除操作
    4. 记录操作日志
    
    **日期范围说明**:
    - 左闭右闭区间，包含开始和结束日期当天的所有新闻
    - 例如：start_date=2025-12-05, end_date=2025-12-06 将删除这两天的所有新闻
    """
    try:
        # 验证确认标志
        if not request.confirm:
            raise HTTPException(
                status_code=400, 
                detail="必须将 confirm 字段设置为 true 才能执行删除操作"
            )
        
        # 解析日期（左闭右闭区间：包含开始和结束日期）
        try:
            start_datetime = datetime.strptime(request.start_date, "%Y-%m-%d")
            # 结束日期+1天，转换为左闭右开区间以包含结束日期当天的所有数据
            end_datetime = datetime.strptime(request.end_date, "%Y-%m-%d") + timedelta(days=1)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=f"日期格式错误: {str(e)}")
        
        # 验证日期范围
        if start_datetime >= end_datetime:
            raise HTTPException(status_code=400, detail="开始日期必须早于结束日期")
        
        # 获取数据库连接
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        try:
            cursor = conn.cursor()
            
            # 开始事务
            conn.autocommit = False
            
            # 1. 先统计将要删除的记录数（用于返回）
            cursor.execute("""
                SELECT COUNT(*) FROM news
                WHERE date >= %s AND date < %s
            """, (start_datetime, end_datetime))
            news_count = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) FROM news_symbols
                WHERE news_id IN (
                    SELECT id FROM news WHERE date >= %s AND date < %s
                )
            """, (start_datetime, end_datetime))
            news_symbols_count = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) FROM news_tags
                WHERE news_id IN (
                    SELECT id FROM news WHERE date >= %s AND date < %s
                )
            """, (start_datetime, end_datetime))
            news_tags_count = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) FROM news_theme_matches
                WHERE news_id IN (
                    SELECT id FROM news WHERE date >= %s AND date < %s
                )
            """, (start_datetime, end_datetime))
            news_theme_matches_count = cursor.fetchone()[0]
            
            cursor.execute("""
                SELECT COUNT(*) FROM news_themes_unified
                WHERE news_id IN (
                    SELECT id FROM news WHERE date >= %s AND date < %s
                )
            """, (start_datetime, end_datetime))
            news_themes_unified_count = cursor.fetchone()[0]
            
            total_count = (
                news_count + 
                news_symbols_count + 
                news_tags_count + 
                news_theme_matches_count + 
                news_themes_unified_count
            )
            
            # 2. 执行删除操作（由于外键CASCADE，只需删除news表即可）
            logger.info(f"开始删除新闻 - 用户: {current_user.username}, 日期范围: {request.start_date} ~ {request.end_date}, "
                       f"将删除 {news_count} 条新闻，总计 {total_count} 条记录")
            
            cursor.execute("""
                DELETE FROM news
                WHERE date >= %s AND date < %s
            """, (start_datetime, end_datetime))
            
            deleted_news_count = cursor.rowcount
            
            # 提交事务
            conn.commit()
            
            logger.info(f"删除新闻完成 - 用户: {current_user.username}, "
                       f"实际删除 {deleted_news_count} 条新闻，预估级联删除 {total_count} 条记录")
            
            # 3. 返回删除统计
            executed_at = datetime.now().isoformat()
            
            return DeleteNewsByDateRangeResponse(
                success=True,
                message=f"成功删除 {deleted_news_count} 条新闻及其关联数据",
                deleted_stats=DeleteNewsByDateRangeStats(
                    news_count=news_count,
                    news_symbols_count=news_symbols_count,
                    news_tags_count=news_tags_count,
                    news_theme_matches_count=news_theme_matches_count,
                    news_themes_unified_count=news_themes_unified_count,
                    project_news_tracking_count=0,
                    total_count=total_count
                ),
                start_date=request.start_date,
                end_date=request.end_date,
                executed_at=executed_at,
                executed_by=current_user.username
            )
            
        except Exception as e:
            # 回滚事务
            conn.rollback()
            logger.error(f"删除新闻失败，已回滚 - 用户: {current_user.username}, 错误: {str(e)}")
            raise HTTPException(status_code=500, detail=f"删除新闻失败: {str(e)}")
        finally:
            cursor.close()
            conn.close()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除新闻失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"删除新闻失败: {str(e)}")
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取新闻详情失败: {str(e)}")
