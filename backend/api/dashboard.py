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
    获取所有行业信息及其包含的公司数量（基于finviz_screener表）
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        # 查询行业信息，包含每个行业的公司列表
        query = """
        SELECT 
            f.sector,
            f.ticker,
            f.company,
            f.industry,
            f.market_cap,
            f.price,
            c.id as company_id
        FROM finviz_screener f
        LEFT JOIN companies c ON f.ticker = c.code
        WHERE f.sector IS NOT NULL AND f.sector != ''
        AND f.snapshot_date = (SELECT MAX(snapshot_date) FROM finviz_screener)
        ORDER BY f.sector, f.market_cap DESC NULLS LAST
        """
        
        results = db_manager.fetch_all(query)
        
        # 按行业分组整理数据
        sectors_dict = {}
        for row in results:
            sector = row[0]
            if sector not in sectors_dict:
                sectors_dict[sector] = {
                    "sector": sector,
                    "company_count": 0,
                    "companies": []
                }
            
            company_info = {
                "id": row[6] if row[6] else 0,  # company_id from companies table
                "companyname": row[2],  # company name from finviz
                "primaryticker": row[1],  # ticker
                "sector": row[0],
                "industry": row[3],
                "market_cap": str(row[4]) if row[4] else None,
                "price": float(row[5]) if row[5] else None
            }
            
            sectors_dict[sector]["companies"].append(company_info)
            sectors_dict[sector]["company_count"] += 1
        
        # 转换为列表并按公司数量排序
        sectors = list(sectors_dict.values())
        sectors.sort(key=lambda x: x["company_count"], reverse=True)
        
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
        
        results = db_manager.fetch_all(query, params)
        
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
        
        results = db_manager.fetch_all(query, [ticker])
        
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
    limit: Optional[int] = Query(20, description="返回新闻数量限制"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取与特定公司相关的新闻（支持多种ticker格式匹配）
    优化版本：使用单次查询获取所有数据，避免N+1查询问题
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        # 优化后的单次查询，一次性获取新闻及其标签和符号
        query = """
        WITH news_data AS (
            SELECT DISTINCT
                n.id, n.title, n.content, n.date, n.source, n.link,
                n.sentiment_polarity, n.sentiment_pos, n.sentiment_neg, n.sentiment_neu
            FROM news n
            JOIN news_symbols ns ON n.id = ns.news_id
            JOIN news_symbols_dict nsd ON ns.symbol_id = nsd.id
            WHERE (
                nsd.symbol_code = %s OR 
                nsd.symbol_code = %s OR 
                nsd.symbol_code LIKE %s OR
                nsd.symbol_code LIKE %s
            )
            ORDER BY n.date DESC
            LIMIT %s
        )
        SELECT 
            nd.id, nd.title, nd.content, nd.date, nd.source, nd.link,
            nd.sentiment_polarity, nd.sentiment_pos, nd.sentiment_neg, nd.sentiment_neu,
            COALESCE(
                ARRAY_AGG(DISTINCT ntd.tag_name) FILTER (WHERE ntd.tag_name IS NOT NULL),
                ARRAY[]::text[]
            ) as tags,
            COALESCE(
                ARRAY_AGG(DISTINCT nsd2.symbol_code) FILTER (WHERE nsd2.symbol_code IS NOT NULL),
                ARRAY[]::text[]
            ) as symbols
        FROM news_data nd
        LEFT JOIN news_tags nt ON nd.id = nt.news_id
        LEFT JOIN news_tags_dict ntd ON nt.tag_id = ntd.id
        LEFT JOIN news_symbols ns2 ON nd.id = ns2.news_id
        LEFT JOIN news_symbols_dict nsd2 ON ns2.symbol_id = nsd2.id
        GROUP BY nd.id, nd.title, nd.content, nd.date, nd.source, nd.link,
                 nd.sentiment_polarity, nd.sentiment_pos, nd.sentiment_neg, nd.sentiment_neu
        ORDER BY nd.date DESC
        """
        
        # 尝试多种ticker格式匹配
        ticker_formats = [
            ticker,  # 原始ticker
            f"{ticker}.US",  # 添加.US后缀
            f"{ticker}%",  # 模糊匹配
            f"%.{ticker}%"  # 包含ticker的格式
        ]
        
        results = db_manager.fetch_all(query, ticker_formats + [limit])
        
        news_items = []
        for row in results:
            news_item = {
                "id": row[0],
                "title": row[1],
                "content": row[2],
                "date": row[3].isoformat() if row[3] else None,
                "source": row[4],
                "link": row[5],
                "sentiment_polarity": float(row[6]) if row[6] is not None else None,
                "sentiment_pos": float(row[7]) if row[7] is not None else None,
                "sentiment_neg": float(row[8]) if row[8] is not None else None,
                "sentiment_neu": float(row[9]) if row[9] is not None else None,
                "symbols": row[11] if row[11] else [],  # 直接从查询结果获取
                "tags": row[10] if row[10] else []  # 直接从查询结果获取
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
    limit: Optional[int] = Query(20, description="返回新闻数量限制"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取与特定行业相关的新闻（基于finviz_screener的sector分类）
    优化版本：使用单次查询获取所有数据，避免N+1查询问题
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        # 优化后的单次查询，一次性获取新闻及其标签和符号
        query = """
        WITH sector_news AS (
            SELECT DISTINCT
                n.id, n.title, n.content, n.date, n.source, n.link,
                n.sentiment_polarity, n.sentiment_pos, n.sentiment_neg, n.sentiment_neu
            FROM news n
            JOIN news_symbols ns ON n.id = ns.news_id
            JOIN news_symbols_dict nsd ON ns.symbol_id = nsd.id
            JOIN finviz_screener f ON (
                f.ticker = REPLACE(nsd.symbol_code, '.US', '') OR
                f.ticker = nsd.symbol_code OR
                nsd.symbol_code LIKE '%' || f.ticker || '%'
            )
            WHERE f.sector = %s
            AND f.snapshot_date = (SELECT MAX(snapshot_date) FROM finviz_screener)
            ORDER BY n.date DESC
            LIMIT %s
        )
        SELECT 
            sn.id, sn.title, sn.content, sn.date, sn.source, sn.link,
            sn.sentiment_polarity, sn.sentiment_pos, sn.sentiment_neg, sn.sentiment_neu,
            COALESCE(
                ARRAY_AGG(DISTINCT ntd.tag_name) FILTER (WHERE ntd.tag_name IS NOT NULL),
                ARRAY[]::text[]
            ) as tags,
            COALESCE(
                ARRAY_AGG(DISTINCT nsd2.symbol_code) FILTER (WHERE nsd2.symbol_code IS NOT NULL),
                ARRAY[]::text[]
            ) as symbols
        FROM sector_news sn
        LEFT JOIN news_tags nt ON sn.id = nt.news_id
        LEFT JOIN news_tags_dict ntd ON nt.tag_id = ntd.id
        LEFT JOIN news_symbols ns2 ON sn.id = ns2.news_id
        LEFT JOIN news_symbols_dict nsd2 ON ns2.symbol_id = nsd2.id
        GROUP BY sn.id, sn.title, sn.content, sn.date, sn.source, sn.link,
                 sn.sentiment_polarity, sn.sentiment_pos, sn.sentiment_neg, sn.sentiment_neu
        ORDER BY sn.date DESC
        """
        
        results = db_manager.fetch_all(query, [sector, limit])
        
        news_items = []
        for row in results:
            news_item = {
                "id": row[0],
                "title": row[1],
                "content": row[2],
                "date": row[3].isoformat() if row[3] else None,
                "source": row[4],
                "link": row[5],
                "sentiment_polarity": float(row[6]) if row[6] is not None else None,
                "sentiment_pos": float(row[7]) if row[7] is not None else None,
                "sentiment_neg": float(row[8]) if row[8] is not None else None,
                "sentiment_neu": float(row[9]) if row[9] is not None else None,
                "symbols": row[11] if row[11] else [],  # 直接从查询结果获取
                "tags": row[10] if row[10] else []  # 直接从查询结果获取
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
        
        results = db_manager.fetch_all(query, [f"%{ticker}%", limit])
        
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
        
        results = db_manager.fetch_all(query, [sector, limit])
        
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
        results = db_manager.fetch_all(query, [search_term, search_term])
        
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
        
        results = db_manager.fetch_all(query, [file_id])
        
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

@router.get("/watchlist", summary="获取用户的watchlist")
async def get_user_watchlist(current_user: UserResponse = Depends(get_current_user)):
    """
    获取当前用户的watchlist（关注的股票列表）
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        # 查询用户的watchlist，并关联finviz_screener获取最新数据
        query = """
        SELECT 
            ttc.id as watchlist_id,
            ttc.symbol,
            ttc.name as custom_name,
            ttc.group_name,
            ttc.priority,
            ttc.notes,
            ttc.added_at,
            f.company,
            f.sector,
            f.industry,
            f.market_cap,
            f.price,
            f.change_percent,
            c.id as company_id
        FROM target_tracking_config ttc
        LEFT JOIN finviz_screener f ON ttc.symbol = f.ticker 
            AND f.snapshot_date = (SELECT MAX(snapshot_date) FROM finviz_screener)
        LEFT JOIN companies c ON ttc.symbol = c.code
        WHERE ttc.user_id = %s AND ttc.enabled = true
        ORDER BY ttc.priority DESC, ttc.added_at DESC
        """
        
        results = db_manager.fetch_all(query, [current_user.user_id])
        
        # 按group_name分组整理数据
        watchlist_dict = {}
        for row in results:
            group_name = row[3] or 'default'
            if group_name not in watchlist_dict:
                watchlist_dict[group_name] = {
                    "group_name": group_name,
                    "items": []
                }
            
            item = {
                "watchlist_id": row[0],
                "symbol": row[1],
                "custom_name": row[2],
                "priority": row[4],
                "notes": row[5],
                "added_at": row[6].isoformat() if row[6] else None,
                "company": row[7],
                "sector": row[8],
                "industry": row[9],
                "market_cap": float(row[10]) if row[10] else None,
                "price": float(row[11]) if row[11] else None,
                "change_percent": row[12],
                "company_id": row[13]
            }
            
            watchlist_dict[group_name]["items"].append(item)
        
        # 转换为列表
        watchlist_groups = list(watchlist_dict.values())
        
        return watchlist_groups
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取用户watchlist失败: {str(e)}"
        )

@router.post("/watchlist", summary="添加股票到watchlist")
async def add_to_watchlist(
    request: dict,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    添加股票到用户的watchlist
    
    请求参数:
    - symbol: 股票代码 (必需)
    - name: 自定义名称 (可选)
    - group_name: 分组名称 (可选，默认为'default')
    - priority: 优先级 (可选，默认为1)
    - notes: 备注 (可选)
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        symbol = request.get('symbol')
        if not symbol:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="股票代码不能为空"
            )
        
        # 验证股票代码是否存在于finviz_screener表中
        check_query = """
        SELECT ticker, company, sector FROM finviz_screener 
        WHERE ticker = %s AND snapshot_date = (SELECT MAX(snapshot_date) FROM finviz_screener)
        LIMIT 1
        """
        stock_info = db_manager.fetch_one(check_query, [symbol])
        
        if not stock_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Stock {symbol} not found"
            )
        
        # 检查是否已经在watchlist中
        existing_query = """
        SELECT id FROM target_tracking_config 
        WHERE user_id = %s AND symbol = %s
        """
        existing = db_manager.fetch_one(existing_query, [current_user.user_id, symbol])
        
        if existing:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Stock {symbol} is already in your watchlist"
            )
        
        # 获取company_id
        company_query = "SELECT id FROM companies WHERE code = %s"
        company_result = db_manager.fetch_one(company_query, [symbol])
        company_id = company_result[0] if company_result else None
        
        # 插入到watchlist
        insert_query = """
        INSERT INTO target_tracking_config 
        (user_id, company_id, symbol, name, group_name, priority, notes, enabled)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """
        
        values = [
            current_user.user_id,
            company_id,
            symbol,
            request.get('name', stock_info[1]),  # 使用公司名称
            request.get('group_name', stock_info[2] or 'Other'),  # 使用sector作为默认分组
            request.get('priority', 1),
            request.get('notes', ''),
            True
        ]
        
        result = db_manager.fetch_one(insert_query, values)
        
        if result:
            return {
                "message": f"Successfully added {symbol} to watchlist",
                "watchlist_id": result[0],
                "symbol": symbol,
                "company_name": stock_info[1]
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to add to watchlist"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to add to watchlist: {str(e)}"
        )

@router.delete("/watchlist/{watchlist_id}", summary="Remove stock from watchlist")
async def remove_from_watchlist(
    watchlist_id: int,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    Remove stock from user's watchlist
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        # 检查该watchlist项是否属于当前用户
        check_query = """
        SELECT symbol FROM target_tracking_config 
        WHERE id = %s AND user_id = %s
        """
        result = db_manager.fetch_one(check_query, [watchlist_id, current_user.user_id])
        
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Watchlist item not found or permission denied"
            )
        
        symbol = result[0]
        
        # 删除watchlist项
        delete_query = "DELETE FROM target_tracking_config WHERE id = %s AND user_id = %s"
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database connection failed"
            )
        
        try:
            cursor = conn.cursor()
            cursor.execute(delete_query, [watchlist_id, current_user.user_id])
            affected_rows = cursor.rowcount
            conn.commit()
            conn.close()
            
            if affected_rows > 0:
                return {
                    "message": f"Successfully removed {symbol} from watchlist",
                    "symbol": symbol
                }
            else:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Watchlist item not found"
                )
        except Exception as e:
            if conn:
                conn.rollback()
                conn.close()
            raise e
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"移除watchlist项失败: {str(e)}"
        )

@router.get("/search/stocks", summary="搜索股票（用于添加到watchlist）")
async def search_stocks(
    q: str = Query(..., description="搜索关键词"),
    limit: int = Query(20, description="返回结果数量限制"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    搜索股票，用于添加到watchlist时的股票选择
    """
    try:
        db_conn_params = get_db_conn_params()
        db_manager = DatabaseManager(db_conn_params)
        
        query = """
        SELECT 
            f.ticker,
            f.company,
            f.sector,
            f.industry,
            f.price,
            f.market_cap,
            c.id as company_id
        FROM finviz_screener f
        LEFT JOIN companies c ON f.ticker = c.code
        WHERE f.snapshot_date = (SELECT MAX(snapshot_date) FROM finviz_screener)
        AND (
            f.ticker ILIKE %s OR 
            f.company ILIKE %s OR
            f.sector ILIKE %s OR
            f.industry ILIKE %s
        )
        ORDER BY f.market_cap DESC NULLS LAST
        LIMIT %s
        """
        
        search_term = f"%{q}%"
        results = db_manager.fetch_all(query, [search_term, search_term, search_term, search_term, limit])
        
        stocks = []
        for row in results:
            stock = {
                "ticker": row[0],
                "company": row[1],
                "sector": row[2],
                "industry": row[3],
                "price": float(row[4]) if row[4] else None,
                "market_cap": float(row[5]) if row[5] else None,
                "company_id": row[6]
            }
            stocks.append(stock)
        
        return stocks
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"搜索股票失败: {str(e)}"
        )

@router.get("/health", summary="健康检查")
async def health_check():
    """
    仪表板API健康检查端点
    """
    return {"status": "healthy", "service": "dashboard", "timestamp": datetime.now().isoformat()}



