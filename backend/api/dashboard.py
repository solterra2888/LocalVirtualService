# -*- coding: utf-8 -*-
"""
仪表板API路由
"""

from fastapi import APIRouter, HTTPException, status, Depends, Query
from typing import List, Optional, Dict, Any
from datetime import datetime

from backend.schemas.user import UserResponse
from backend.auth.jwt_utils import get_current_user
from backend.database.db_manager import DatabaseManager
from config.settings import config

router = APIRouter()

# 获取数据库连接参数
def get_db_conn_params() -> Dict[str, Any]:
    """获取数据库连接参数"""
    return {
        'db_host': config.get('DB_HOST', 'localhost'),
        'db_port': config.get('DB_PORT', '5432'),
        'db_name': config.get('DB_NAME', 'targettracking'),
        'db_user': config.get('DB_USER', 'postgres'),
        'db_password': config.get('DB_PASSWORD', '')
    }

@router.get("/sectors", summary="获取所有行业信息")
async def get_sectors(current_user: UserResponse = Depends(get_current_user)):
    """
    获取所有行业信息及其包含的公司数量
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        # 查询行业信息
        query = """
        SELECT 
            sector,
            COUNT(*) as company_count
        FROM companies 
        WHERE sector IS NOT NULL AND sector != ''
        GROUP BY sector 
        ORDER BY company_count DESC
        """
        
        results = db_manager.execute_query(query)
        
        sectors = []
        for row in results:
            sector_info = {
                "sector": row[0],
                "company_count": row[1],
                "companies": []  # 这里可以后续添加公司列表
            }
            sectors.append(sector_info)
        
        return sectors
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取行业信息失败: {str(e)}"
        )

@router.get("/companies", summary="获取公司信息")
async def get_companies(
    sector: Optional[str] = Query(None, description="行业筛选"),
    search: Optional[str] = Query(None, description="搜索关键词"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取公司信息列表，支持按行业筛选和关键词搜索
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        # 构建查询条件
        where_conditions = []
        params = []
        
        if sector:
            where_conditions.append("sector = %s")
            params.append(sector)
        
        if search:
            where_conditions.append("(companyname ILIKE %s OR primaryticker ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        query = f"""
        SELECT 
            id, companyname, primaryticker, sector, industry,
            gicsector, gicindustry, gicsubindustry, market_cap, country
        FROM companies 
        WHERE {where_clause}
        ORDER BY companyname
        LIMIT 100
        """
        
        results = db_manager.execute_query(query, params)
        
        companies = []
        for row in results:
            company = {
                "id": row[0],
                "companyname": row[1],
                "primaryticker": row[2],
                "sector": row[3],
                "industry": row[4],
                "gicsector": row[5],
                "gicindustry": row[6],
                "gicsubindustry": row[7],
                "market_cap": row[8],
                "country": row[9]
            }
            companies.append(company)
        
        return companies
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取公司信息失败: {str(e)}"
        )

@router.get("/companies/{ticker}", summary="获取特定公司信息")
async def get_company_info(
    ticker: str,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    根据股票代码获取特定公司的详细信息
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        query = """
        SELECT 
            id, companyname, primaryticker, sector, industry,
            gicsector, gicindustry, gicsubindustry, market_cap, country
        FROM companies 
        WHERE primaryticker = %s
        """
        
        results = db_manager.execute_query(query, [ticker])
        
        if not results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到股票代码为 {ticker} 的公司"
            )
        
        row = results[0]
        company = {
            "id": row[0],
            "companyname": row[1],
            "primaryticker": row[2],
            "sector": row[3],
            "industry": row[4],
            "gicsector": row[5],
            "gicindustry": row[6],
            "gicsubindustry": row[7],
            "market_cap": row[8],
            "country": row[9]
        }
        
        return company
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取公司信息失败: {str(e)}"
        )

@router.get("/companies/{ticker}/news", summary="获取公司相关新闻")
async def get_company_news(
    ticker: str,
    limit: Optional[int] = Query(10, description="返回新闻数量限制"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取与特定公司相关的新闻
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        query = """
        SELECT 
            n.id, n.title, n.content, n.date, n.source, n.link,
            n.sentiment_polarity, n.sentiment_pos, n.sentiment_neg, n.sentiment_neu
        FROM news n
        JOIN news_symbols ns ON n.id = ns.news_id
        WHERE ns.symbol = %s
        ORDER BY n.date DESC
        LIMIT %s
        """
        
        results = db_manager.execute_query(query, [ticker, limit])
        
        news_items = []
        for row in results:
            # 获取新闻标签
            tag_query = """
            SELECT tag FROM news_tags WHERE news_id = %s
            """
            tags = [tag_row[0] for tag_row in db_manager.execute_query(tag_query, [row[0]])]
            
            # 获取新闻符号
            symbol_query = """
            SELECT symbol FROM news_symbols WHERE news_id = %s
            """
            symbols = [symbol_row[0] for symbol_row in db_manager.execute_query(symbol_query, [row[0]])]
            
            news_item = {
                "id": row[0],
                "title": row[1],
                "content": row[2],
                "date": row[3].isoformat() if row[3] else None,
                "source": row[4],
                "link": row[5],
                "sentiment_polarity": row[6],
                "sentiment_pos": row[7],
                "sentiment_neg": row[8],
                "sentiment_neu": row[9],
                "symbols": symbols,
                "tags": tags
            }
            news_items.append(news_item)
        
        return news_items
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取公司新闻失败: {str(e)}"
        )

@router.get("/sectors/{sector}/news", summary="获取行业相关新闻")
async def get_sector_news(
    sector: str,
    limit: Optional[int] = Query(10, description="返回新闻数量限制"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取与特定行业相关的新闻
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        query = """
        SELECT DISTINCT
            n.id, n.title, n.content, n.date, n.source, n.link,
            n.sentiment_polarity, n.sentiment_pos, n.sentiment_neg, n.sentiment_neu
        FROM news n
        JOIN news_symbols ns ON n.id = ns.news_id
        JOIN companies c ON ns.symbol = c.primaryticker
        WHERE c.sector = %s
        ORDER BY n.date DESC
        LIMIT %s
        """
        
        results = db_manager.execute_query(query, [sector, limit])
        
        news_items = []
        for row in results:
            # 获取新闻标签
            tag_query = """
            SELECT tag FROM news_tags WHERE news_id = %s
            """
            tags = [tag_row[0] for tag_row in db_manager.execute_query(tag_query, [row[0]])]
            
            # 获取新闻符号
            symbol_query = """
            SELECT symbol FROM news_symbols WHERE news_id = %s
            """
            symbols = [symbol_row[0] for symbol_row in db_manager.execute_query(symbol_query, [row[0]])]
            
            news_item = {
                "id": row[0],
                "title": row[1],
                "content": row[2],
                "date": row[3].isoformat() if row[3] else None,
                "source": row[4],
                "link": row[5],
                "sentiment_polarity": row[6],
                "sentiment_pos": row[7],
                "sentiment_neg": row[8],
                "sentiment_neu": row[9],
                "symbols": symbols,
                "tags": tags
            }
            news_items.append(news_item)
        
        return news_items
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取行业新闻失败: {str(e)}"
        )

@router.get("/companies/{ticker}/analysis", summary="获取公司AI分析结果")
async def get_company_analysis(
    ticker: str,
    limit: Optional[int] = Query(10, description="返回分析结果数量限制"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取特定公司的AI分析结果
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        query = """
        SELECT 
            f.id, f.filename, f.content, f.created_at, f.file_type, f.status,
            f.total_tokens, f.cost, t.id as task_id
        FROM files f
        LEFT JOIN tasks t ON f.task_id = t.id
        WHERE f.symbols LIKE %s
        ORDER BY f.created_at DESC
        LIMIT %s
        """
        
        results = db_manager.execute_query(query, [f"%{ticker}%", limit])
        
        analysis_results = []
        for row in results:
            analysis_result = {
                "id": row[0],
                "filename": row[1],
                "content": row[2],
                "created_at": row[3].isoformat() if row[3] else None,
                "file_type": row[4],
                "status": row[5],
                "total_tokens": row[6] or 0,
                "cost": row[7] or 0,
                "task_id": str(row[8]) if row[8] else None
            }
            analysis_results.append(analysis_result)
        
        return analysis_results
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取公司AI分析失败: {str(e)}"
        )

@router.get("/sectors/{sector}/analysis", summary="获取行业AI分析结果")
async def get_sector_analysis(
    sector: str,
    limit: Optional[int] = Query(10, description="返回分析结果数量限制"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取特定行业的AI分析结果
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        query = """
        SELECT DISTINCT
            f.id, f.filename, f.content, f.created_at, f.file_type, f.status,
            f.total_tokens, f.cost, t.id as task_id
        FROM files f
        LEFT JOIN tasks t ON f.task_id = t.id
        JOIN companies c ON f.symbols LIKE '%' || c.primaryticker || '%'
        WHERE c.sector = %s
        ORDER BY f.created_at DESC
        LIMIT %s
        """
        
        results = db_manager.execute_query(query, [sector, limit])
        
        analysis_results = []
        for row in results:
            analysis_result = {
                "id": row[0],
                "filename": row[1],
                "content": row[2],
                "created_at": row[3].isoformat() if row[3] else None,
                "file_type": row[4],
                "status": row[5],
                "total_tokens": row[6] or 0,
                "cost": row[7] or 0,
                "task_id": str(row[8]) if row[8] else None
            }
            analysis_results.append(analysis_result)
        
        return analysis_results
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取行业AI分析失败: {str(e)}"
        )

@router.get("/search/companies", summary="搜索公司")
async def search_companies(
    q: str = Query(..., description="搜索关键词"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    根据关键词搜索公司
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        query = """
        SELECT 
            id, companyname, primaryticker, sector, industry,
            gicsector, gicindustry, gicsubindustry, market_cap, country
        FROM companies 
        WHERE companyname ILIKE %s OR primaryticker ILIKE %s
        ORDER BY companyname
        LIMIT 50
        """
        
        search_term = f"%{q}%"
        results = db_manager.execute_query(query, [search_term, search_term])
        
        companies = []
        for row in results:
            company = {
                "id": row[0],
                "companyname": row[1],
                "primaryticker": row[2],
                "sector": row[3],
                "industry": row[4],
                "gicsector": row[5],
                "gicindustry": row[6],
                "gicsubindustry": row[7],
                "market_cap": row[8],
                "country": row[9]
            }
            companies.append(company)
        
        return companies
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"搜索公司失败: {str(e)}"
        )

@router.get("/files/{file_id}/content", summary="获取文件内容")
async def get_file_content(
    file_id: int,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取指定文件的内容
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        query = """
        SELECT filename, content FROM files WHERE id = %s
        """
        
        results = db_manager.execute_query(query, [file_id])
        
        if not results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到ID为 {file_id} 的文件"
            )
        
        row = results[0]
        return {
            "filename": row[0],
            "content": row[1]
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取文件内容失败: {str(e)}"
        )

@router.get("/health", summary="健康检查")
async def health_check():
    """
    仪表板API健康检查端点
    """
    return {"status": "healthy", "service": "dashboard", "timestamp": datetime.now().isoformat()}



