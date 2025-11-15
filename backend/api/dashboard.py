# -*- coding: utf-8 -*-
"""
仪表板API路由
"""

from fastapi import APIRouter, HTTPException, status, Depends, Query
from typing import List, Optional, Dict, Any
from datetime import datetime
import os
import urllib.parse
import asyncio
import time

from backend.schemas.user import UserResponse
from backend.auth.jwt_utils import get_current_user
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.config.exchange_config import ExchangeConfig
from backend.utils.database_config import get_database_config

logger = get_logger("dashboard_api")

def get_smart_ticker_formats(ticker: str) -> List[str]:
    """
    基于exchange_config.py的智能ticker格式匹配
    
    这个函数解决了1810.HK和1810.US被错误关联的问题：
    - 优先精确匹配
    - 基于交易所配置进行智能扩展
    - 避免模糊匹配导致的误匹配
    
    Args:
        ticker: 输入的ticker，如 "1810.HK" 或 "1810"
        
    Returns:
        匹配格式列表，按优先级排序
    """
    if not ticker:
        return []
    
    ticker = ticker.strip().upper()
    formats = [ticker]  # 精确匹配优先
    
    # 如果ticker已经包含交易所后缀，只进行精确匹配
    if '.' in ticker:
        symbol, exchange = ticker.split('.', 1)
        
        # 验证交易所是否有效
        exchange_info = ExchangeConfig.get_exchange_info(exchange)
        if exchange_info:
            # 只返回精确匹配，避免模糊匹配
            return [ticker]
        else:
            # 如果交易所无效，尝试常见格式
            return [ticker, f"{symbol}.US", f"{symbol}.HK"]
    
    # 如果ticker没有交易所后缀，基于数字特征智能添加
    if ticker.isdigit():
        # 基于exchange_config.py中的TICKER_PATTERNS进行智能匹配
        detected_exchange = ExchangeConfig._detect_exchange_by_ticker(ticker)
        if detected_exchange:
            # 只添加检测到的交易所，避免添加所有可能的交易所
            formats.append(f"{ticker}.{detected_exchange}")
        else:
            # 如果无法检测，添加最常见的交易所
            formats.extend([f"{ticker}.US", f"{ticker}.HK"])
    else:
        # 非数字ticker，添加常见交易所后缀
        formats.extend([f"{ticker}.US", f"{ticker}.HK"])
    
    return formats

router = APIRouter()


@router.get("/sectors", summary="获取所有行业信息")
async def get_sectors(current_user: UserResponse = Depends(get_current_user)):
    """
    获取所有行业信息及其包含的公司数量（基于finviz_screener表）
    """
    try:
        db_conn_params = get_database_config()
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
            detail=f"Failed to get sectors: {str(e)}"
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
        db_conn_params = get_database_config()
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
            detail=f"Failed to get company info: {str(e)}"
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
        db_conn_params = get_database_config()
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
                detail=f"Company with ticker {ticker} not found"
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
            detail=f"Failed to get company info: {str(e)}"
        )

@router.get("/companies/{ticker}/news", summary="获取公司相关新闻")
async def get_company_news(
    ticker: str,
    limit: Optional[int] = Query(20, description="返回新闻数量限制"),
    offset: Optional[int] = Query(0, description="偏移量，用于分页"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取与特定公司相关的新闻（支持多种ticker格式匹配）
    优化版本：使用单次查询获取所有数据，避免N+1查询问题
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 优化后的单次查询，一次性获取新闻及其标签和符号
        query = """
        WITH news_data AS (
            SELECT DISTINCT
                n.id, n.title, n.content, n.date, n.source, n.link,
                n.sentiment_polarity, n.sentiment_pos, n.sentiment_neg, n.sentiment_neu,
                n.analysis_result, n.scheduler_source, n.updated_at
            FROM news n
            JOIN news_symbols ns ON n.id = ns.news_id
            JOIN news_symbols_dict nsd ON ns.symbol_id = nsd.id
            WHERE nsd.symbol_code = ANY(%s)
            ORDER BY n.date DESC
            LIMIT %s OFFSET %s
        )
        SELECT 
            nd.id, nd.title, nd.content, nd.date, nd.source, nd.link,
            nd.sentiment_polarity, nd.sentiment_pos, nd.sentiment_neg, nd.sentiment_neu,
            nd.analysis_result, nd.scheduler_source, nd.updated_at,
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
                 nd.sentiment_polarity, nd.sentiment_pos, nd.sentiment_neg, nd.sentiment_neu,
                 nd.analysis_result, nd.scheduler_source, nd.updated_at
        ORDER BY nd.date DESC
        """
        
        # 使用智能ticker格式匹配
        ticker_formats = get_smart_ticker_formats(ticker)
        
        results = db_manager.fetch_all(query, [ticker_formats, limit, offset])
        
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
                "analysis_result": row[10] if row[10] else None,  # 分析结果（JSON格式）
                "scheduler_source": row[11],  # 调度器来源
                "updated_at": row[12].isoformat() if row[12] else None,  # 更新时间
                "tags": row[13] if row[13] else [],  # 直接从查询结果获取
                "symbols": row[14] if row[14] else [],  # 直接从查询结果获取
                "auto_tags": []  # 添加 auto_tags 字段以保持一致性
            }
            news_items.append(news_item)
        
        return news_items
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get company news: {str(e)}"
        )

@router.get("/sectors/{sector}/news", summary="获取行业相关新闻")
async def get_sector_news(
    sector: str,
    limit: Optional[int] = Query(20, description="返回新闻数量限制"),
    offset: Optional[int] = Query(0, description="偏移量，用于分页"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取与特定行业相关的新闻（基于finviz_screener的sector分类）
    优化版本：使用单次查询获取所有数据，避免N+1查询问题
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 优化后的单次查询，一次性获取新闻及其标签和符号
        query = """
        WITH sector_news AS (
            SELECT DISTINCT
                n.id, n.title, n.content, n.date, n.source, n.link,
                n.sentiment_polarity, n.sentiment_pos, n.sentiment_neg, n.sentiment_neu,
                n.analysis_result, n.updated_at
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
            LIMIT %s OFFSET %s
        )
        SELECT 
            sn.id, sn.title, sn.content, sn.date, sn.source, sn.link,
            sn.sentiment_polarity, sn.sentiment_pos, sn.sentiment_neg, sn.sentiment_neu,
            sn.analysis_result, sn.updated_at,
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
                 sn.sentiment_polarity, sn.sentiment_pos, sn.sentiment_neg, sn.sentiment_neu,
                 sn.analysis_result, sn.updated_at
        ORDER BY sn.date DESC
        """
        
        results = db_manager.fetch_all(query, [sector, limit, offset])
        
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
                "analysis_result": row[10] if row[10] else None,  # 分析结果（JSON格式）
                "updated_at": row[11].isoformat() if row[11] else None,  # 更新时间
                "tags": row[12] if row[12] else [],  # 直接从查询结果获取
                "symbols": row[13] if row[13] else [],  # 直接从查询结果获取
                "auto_tags": []  # 添加 auto_tags 字段以保持一致性
            }
            news_items.append(news_item)
        
        return news_items
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get sector news: {str(e)}"
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
        db_conn_params = get_database_config()
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
            detail=f"Failed to get company analysis: {str(e)}"
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
        db_conn_params = get_database_config()
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
            detail=f"Failed to get sector analysis: {str(e)}"
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
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        query = """
        SELECT 
            id, companyname, primaryticker, sector, industry,
            gicsector, gicindustry, gicsubindustry, market_cap, country
        FROM companies 
        WHERE companyname ILIKE %s OR primaryticker ILIKE %s
        ORDER BY 
            CASE 
                WHEN UPPER(primaryticker) = UPPER(%s) THEN 1
                WHEN UPPER(companyname) = UPPER(%s) THEN 2
                WHEN UPPER(primaryticker) LIKE UPPER(%s) THEN 3
                WHEN UPPER(companyname) LIKE UPPER(%s) THEN 4
                ELSE 5
            END,
            companyname
        LIMIT 50
        """
        
        search_term = f"%{q}%"
        exact_match = q.upper()
        results = db_manager.fetch_all(query, [
            search_term, search_term,  # WHERE conditions
            exact_match, exact_match, search_term, search_term  # ORDER BY conditions
        ])
        
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
            detail=f"Failed to search companies: {str(e)}"
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
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        query = """
        SELECT filename, content FROM files WHERE id = %s
        """
        
        results = db_manager.fetch_all(query, [file_id])
        
        if not results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"File with ID {file_id} not found"
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
            detail=f"Failed to get file content: {str(e)}"
        )

@router.get("/files/{file_id}/download", summary="下载文件")
async def download_file(
    file_id: int,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    下载指定文件
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 查询文件信息，从 file_processing_details 表获取
        query = """
        SELECT filename, original_file_path, file_type 
        FROM file_processing_details 
        WHERE id = %s AND status = 'completed'
        """
        
        results = db_manager.fetch_all(query, [file_id])
        
        if not results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"File with ID {file_id} not found"
            )
        
        filename, file_path, file_type = results[0]
        
        # 检查文件是否存在
        if not file_path or not os.path.exists(file_path):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="File not found"
            )
        
        # 确定MIME类型
        mime_type = "application/octet-stream"
        if file_type:
            type_lower = file_type.lower()
            if type_lower == "pdf":
                mime_type = "application/pdf"
            elif type_lower in ["doc", "docx"]:
                mime_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            elif type_lower in ["xls", "xlsx"]:
                mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif type_lower in ["jpg", "jpeg"]:
                mime_type = "image/jpeg"
            elif type_lower == "png":
                mime_type = "image/png"
        
        # 返回文件响应
        from fastapi.responses import FileResponse
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type=mime_type,
            headers={
                "Content-Disposition": f"attachment; filename*=UTF-8''{urllib.parse.quote(filename)}"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to download file: {str(e)}"
        )

@router.get("/watchlist", summary="Get user's watchlist")
async def get_user_watchlist(current_user: UserResponse = Depends(get_current_user)):
    """
    获取当前用户的watchlist（关注的股票列表）
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 查询用户的watchlist，只关联companies表获取基础信息
        query = """
        SELECT 
            ttc.id as watchlist_id,
            ttc.symbol,
            ttc.name as custom_name,
            ttc.group_name,
            ttc.priority,
            ttc.notes,
            ttc.added_at,
            ttc.primaryticker,
            c.name as company,
            c.sector,
            c.industry,
            c.exchange,
            c.id as company_id
        FROM target_tracking_config ttc
        LEFT JOIN companies c ON ttc.symbol = c.code
        WHERE ttc.user_id = %s AND ttc.enabled = true
        ORDER BY COALESCE(ttc.sort_order, 999999), ttc.priority DESC, ttc.added_at DESC
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
                "primaryticker": row[7],
                "company": row[8],
                "sector": row[9],
                "industry": row[10],
                "exchange": row[11],
                "company_id": row[12]
            }
            
            watchlist_dict[group_name]["items"].append(item)
        
        # 转换为列表
        watchlist_groups = list(watchlist_dict.values())
        
        return watchlist_groups
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get user watchlist: {str(e)}"
        )

@router.post("/watchlist", summary="Add stock to watchlist")
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
    - priority: 优先级 (可选，默认为1，支持1=Low, 2=Medium, 3=High)
    - notes: 备注 (可选)
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        symbol = request.get('symbol')
        if not symbol:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Stock code cannot be empty"
            )
        
        # 验证股票代码是否存在于companies表中
        check_query = """
        SELECT code, name, sector, primaryticker FROM companies 
        WHERE code = %s AND ticker_active = true
        LIMIT 1
        """
        stock_info = db_manager.fetch_one(check_query, [symbol])
        
        if not stock_info:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Stock {symbol} not found in companies database"
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
        group_name = request.get('group_name', stock_info[2] or 'Other')  # 使用sector作为默认分组
        
        # 计算新的排序序号（同一用户及分组内递增）
        sort_order_query = """
        SELECT COALESCE(MAX(sort_order), 0) + 1
        FROM target_tracking_config
        WHERE user_id = %s AND COALESCE(group_name, 'default') = COALESCE(%s, 'default')
        """
        next_sort_order_result = db_manager.fetch_one(sort_order_query, [current_user.user_id, group_name])
        next_sort_order = next_sort_order_result[0] if next_sort_order_result and next_sort_order_result[0] else 1
        
        insert_query = """
        INSERT INTO target_tracking_config 
        (user_id, company_id, symbol, name, group_name, priority, notes, enabled, primaryticker, sort_order)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """
        
        values = [
            current_user.user_id,
            company_id,
            symbol,
            request.get('name', stock_info[1]),  # 使用公司名称
            group_name,
            request.get('priority', 1),
            request.get('notes', ''),
            True,
            stock_info[3],  # primaryticker
            next_sort_order
        ]
        
        result = db_manager.fetch_one(insert_query, values)
        
        if result:
            watchlist_id = result[0]
            
            # 🔥 异步更新EODHD fundamentals数据（通过 Celery 后台任务）
            fundamentals_task_result = None
            try:
                logger.info(f"准备提交 Celery 任务更新 {symbol} 的 fundamentals 数据", 
                           user=current_user.username, symbol=symbol)
                
                # 检查fundamentals数据是否在24小时内更新过
                freshness_check_query = """
                SELECT MAX(updated_at) as last_update 
                FROM companies 
                WHERE code = %s AND updated_at > NOW() - INTERVAL '1 day'
                """
                freshness_result = db_manager.fetch_one(freshness_check_query, [symbol])
                
                if freshness_result and freshness_result[0]:
                    # 数据在24小时内更新过，跳过更新
                    logger.info(f"Fundamentals data for {symbol} is fresh, skipping update", 
                               user=current_user.username, symbol=symbol,
                               last_update=freshness_result[0])
                    
                    fundamentals_task_result = {
                        "success": True,
                        "message": f"Data is fresh, skipping update. Last update: {freshness_result[0]}",
                        "skipped": True,
                        "last_update": freshness_result[0].isoformat() if freshness_result[0] else None
                    }
                else:
                    # 数据需要更新，提交 Celery 任务
                    logger.info(f"Fundamentals data for {symbol} needs update, submitting Celery task", 
                               user=current_user.username, symbol=symbol)
                    
                    # 获取公司的exchange信息
                    exchange_query = "SELECT exchange FROM companies WHERE code = %s"
                    exchange_result = db_manager.fetch_one(exchange_query, [symbol])
                    exchange = exchange_result[0] if exchange_result else 'US'
                    
                    # 导入并提交 Celery 任务（异步执行，不阻塞）
                    from backend.tasks.data_tasks import update_single_ticker_fundamentals_task
                    
                    # 提交任务到 Celery
                    task = update_single_ticker_fundamentals_task.apply_async(
                        args=[symbol, exchange]
                    )
                    
                    fundamentals_task_result = {
                        "success": True,
                        "task_id": task.id,
                        "message": f"Fundamentals update task submitted for {symbol}.{exchange}"
                    }
                    
                    logger.info(f"成功提交 fundamentals 更新任务", 
                               user=current_user.username,
                               symbol=symbol,
                               exchange=exchange,
                               task_id=task.id)
                    
            except Exception as e:
                fundamentals_task_result = {
                    "success": False,
                    "message": f"Failed to submit fundamentals update task: {str(e)}"
                }
                logger.error(f"提交 fundamentals 更新任务失败", 
                           user=current_user.username,
                           symbol=symbol,
                           error=str(e))
            
            # 🔥 异步获取该 ticker 的全部历史 EOD 数据（通过 Celery 后台任务）
            eod_task_result = None
            try:
                # 使用 primaryticker 作为 ticker（如果有的话）
                ticker_for_eod = stock_info[3] if stock_info[3] else symbol
                
                logger.info(f"提交 Celery 任务获取 {ticker_for_eod} 的历史数据", 
                           user=current_user.username, 
                           symbol=symbol,
                           ticker=ticker_for_eod)
                
                # 导入并提交 Celery 任务（异步执行，不阻塞）
                from backend.tasks.data_tasks import fetch_single_ticker_eod_history_task
                
                # 提交任务到 Celery，获取全部历史数据（10年）
                task = fetch_single_ticker_eod_history_task.apply_async(
                    args=[ticker_for_eod],
                    kwargs={'days_back': None}  # None 表示获取全部历史数据
                )
                
                eod_task_result = {
                    "success": True,
                    "task_id": task.id,
                    "message": f"Historical data fetch task submitted for {ticker_for_eod}"
                }
                
                logger.info(f"成功提交历史数据获取任务", 
                           user=current_user.username,
                           symbol=symbol,
                           ticker=ticker_for_eod,
                           task_id=task.id)
                
            except Exception as e:
                eod_task_result = {
                    "success": False,
                    "message": f"Failed to submit historical data task: {str(e)}"
                }
                logger.error(f"提交历史数据获取任务失败", 
                           user=current_user.username,
                           symbol=symbol,
                           error=str(e))
            
            # 构建响应消息
            response_message = f"Successfully added {symbol} to watchlist"
            
            # 添加 fundamentals 任务信息
            if fundamentals_task_result:
                if fundamentals_task_result.get("skipped"):
                    response_message += f". Fundamentals data is fresh (last update: {fundamentals_task_result.get('last_update', 'N/A')})"
                elif fundamentals_task_result["success"]:
                    task_id = fundamentals_task_result.get('task_id', '')
                    response_message += f". Fundamentals update is being processed in background (task: {task_id[:8]}...)"
                else:
                    response_message += f". Note: {fundamentals_task_result['message']}"
            
            # 添加 EOD 历史数据任务信息
            if eod_task_result and eod_task_result["success"]:
                response_message += f". Historical data is being fetched in background (task: {eod_task_result['task_id'][:8]}...)"
            
            return {
                "message": response_message,
                "watchlist_id": watchlist_id,
                "symbol": symbol,
                "company_name": stock_info[1],
                "fundamentals_task": fundamentals_task_result,
                "eod_task": eod_task_result
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

@router.get("/watchlist/{ticker}/research-reports", summary="获取ticker关联的研究报告")
async def get_ticker_research_reports(
    ticker: str,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取与指定ticker关联的上传文件（研究报告）
    
    Args:
        ticker: 股票代码
        
    Returns:
        包含文件信息的列表
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 查询与ticker关联的文件
        # 支持多种ticker格式匹配，类似新闻查询的逻辑
        query = """
        SELECT DISTINCT
            f.id,
            f.filename,
            f.original_file_path,
            f.file_size_mb,
            f.file_type,
            f.created_at,
            f.status,
            ft.sector,
            ft.industry,
            ft.ticker,
            ft.tagged_at
        FROM file_processing_details f
        INNER JOIN file_tags ft ON f.id = ft.file_id
        WHERE ft.ticker = ANY(%s)
        AND f.status = 'completed'
        ORDER BY ft.tagged_at DESC, f.created_at DESC
        """
        
        # 使用智能ticker格式匹配
        ticker_formats = get_smart_ticker_formats(ticker)
        
        results = db_manager.fetch_all(query, [ticker_formats])
        
        research_reports = []
        for row in results:
            report = {
                "id": row[0],
                "file_name": row[1],
                "file_path": row[2],
                "file_size": int(row[3] * 1024 * 1024) if row[3] else 0,  # 转换为字节
                "file_type": row[4],
                "uploaded_at": row[5].isoformat() if row[5] else None,
                "status": row[6],
                "sector": row[7],
                "industry": row[8],
                "ticker": row[9],
                "tagged_at": row[10].isoformat() if row[10] else None
            }
            research_reports.append(report)
        
        return {
            "ticker": ticker,
            "count": len(research_reports),
            "reports": research_reports
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get research reports: {str(e)}"
        )

@router.get("/test/research-reports/{ticker}", summary="测试获取ticker关联的研究报告（无需认证）")
async def test_get_ticker_research_reports(ticker: str):
    """
    测试端点：获取与指定ticker关联的上传文件（研究报告）
    无需认证，用于测试
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 查询与ticker关联的文件
        query = """
        SELECT DISTINCT
            f.id,
            f.filename,
            f.original_file_path,
            f.file_size_mb,
            f.file_type,
            f.created_at,
            f.status,
            ft.sector,
            ft.industry,
            ft.ticker,
            ft.tagged_at
        FROM file_processing_details f
        INNER JOIN file_tags ft ON f.id = ft.file_id
        WHERE ft.ticker = ANY(%s)
        AND f.status = 'completed'
        ORDER BY ft.tagged_at DESC, f.created_at DESC
        """
        
        # 使用智能ticker格式匹配
        ticker_formats = get_smart_ticker_formats(ticker)
        
        results = db_manager.fetch_all(query, [ticker_formats])
        
        research_reports = []
        for row in results:
            report = {
                "id": row[0],
                "file_name": row[1],
                "file_path": row[2],
                "file_size": int(row[3] * 1024 * 1024) if row[3] else 0,  # 转换为字节
                "file_type": row[4],
                "uploaded_at": row[5].isoformat() if row[5] else None,
                "status": row[6],
                "sector": row[7],
                "industry": row[8],
                "ticker": row[9],
                "tagged_at": row[10].isoformat() if row[10] else None
            }
            research_reports.append(report)
        
        return {
            "ticker": ticker,
            "count": len(research_reports),
            "reports": research_reports
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get research reports: {str(e)}"
        )

@router.get("/stock-prices", summary="Get real-time prices for stocks")
async def get_stock_prices(
    symbols: str = Query(..., description="Stock codes, separated by commas, e.g.: AAPL,MSFT,GOOGL"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    获取股票的实时价格数据
    
    从stock_prices_realtime表获取最新的价格信息
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 解析股票代码
        symbol_list = [s.strip().upper() for s in symbols.split(',') if s.strip()]
        if not symbol_list:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Please provide valid stock codes"
            )
        
        # 构建查询条件
        placeholders = ','.join(['%s'] * len(symbol_list))
        query = f"""
        SELECT 
            code,
            open,
            high,
            low,
            close,
            volume,
            previous_close,
            change_amount,
            change_percent,
            last_updated,
            created_at
        FROM stock_prices_realtime 
        WHERE code IN ({placeholders})
        ORDER BY last_updated DESC
        """
        
        results = db_manager.fetch_all(query, symbol_list)
        
        # 格式化结果
        price_data = []
        for row in results:
            price_data.append({
                "code": row[0],
                "open": row[1],
                "high": row[2], 
                "low": row[3],
                "close": row[4],
                "volume": row[5],
                "previous_close": row[6],
                "change_amount": row[7],
                "change_percent": row[8],
                "last_updated": row[9].isoformat() if row[9] else None,
                "created_at": row[10].isoformat() if row[10] else None
            })
        
        return {
            "success": True,
            "data": price_data,
            "count": len(price_data)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get stock prices: {str(e)}"
        )

@router.get("/watchlist/prices", summary="Get real-time prices for stocks in watchlist")
async def get_watchlist_stock_prices(current_user: UserResponse = Depends(get_current_user)):
    """
    获取用户watchlist中所有股票的实时价格数据
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 获取用户watchlist中的所有股票代码，优先使用primaryticker
        watchlist_query = """
        SELECT DISTINCT 
            CASE 
                WHEN primaryticker IS NOT NULL AND primaryticker != '' 
                THEN primaryticker 
                ELSE symbol 
            END as ticker
        FROM target_tracking_config 
        WHERE user_id = %s AND enabled = true
        AND (symbol IS NOT NULL AND symbol != '')
        """
        watchlist_results = db_manager.fetch_all(watchlist_query, [current_user.user_id])
        
        if not watchlist_results:
            return {
                "success": True,
                "data": [],
                "count": 0,
                "message": "No stocks in watchlist"
            }
        
        # 提取股票代码（已经包含正确的交易所后缀）
        symbols = []
        for row in watchlist_results:
            ticker = row[0]
            if ticker:
                symbols.append(ticker)
        
        # 获取价格数据
        if symbols:
            placeholders = ','.join(['%s'] * len(symbols))
            price_query = f"""
            SELECT 
                code,
                open,
                high,
                low,
                close,
                volume,
                previous_close,
                change_amount,
                change_percent,
                last_updated,
                updated_at
            FROM stock_prices_realtime 
            WHERE code IN ({placeholders})
            ORDER BY last_updated DESC
            """
            
            price_results = db_manager.fetch_all(price_query, symbols)
            
            # 格式化结果
            price_data = []
            for row in price_results:
                price_data.append({
                    "code": row[0],
                    "open": row[1],
                    "high": row[2], 
                    "low": row[3],
                    "close": row[4],
                    "volume": row[5],
                    "previous_close": row[6],
                    "change_amount": row[7],
                    "change_percent": row[8],
                    "last_updated": row[9].isoformat() if row[9] else None,
                    "updated_at": row[10].isoformat() if row[10] else None
                })
            
            return {
                "success": True,
                "data": price_data,
                "count": len(price_data)
            }
        else:
            return {
                "success": True,
                "data": [],
                "count": 0
            }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get watchlist stock prices: {str(e)}"
        )

@router.get("/test-stock-historical-data/{symbol}", summary="Test historical stock data (no auth)")
async def test_stock_historical_data(
    symbol: str,
    days: int = Query(365, ge=30, le=3650, description="Number of days of historical data (30-3650)")
):
    """Test endpoint without authentication"""
    try:
        print(f"Getting historical data for symbol: {symbol}, days: {days}")
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # Calculate date range
        from datetime import datetime, timedelta
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        print(f"Date range: {start_date} to {end_date}")
        
        # Query historical data from stock_prices_eod table
        query = """
        SELECT 
            date,
            open,
            high,
            low,
            close,
            volume
        FROM stock_prices_eod 
        WHERE code = %s 
        AND date >= %s 
        AND date <= %s
        AND period = 'd'
        ORDER BY date ASC
        """
        
        results = db_manager.fetch_all(query, [symbol, start_date, end_date])
        print(f"Found {len(results)} records for symbol {symbol}")
        
        # If no results and symbol doesn't have .US suffix, try adding it
        if len(results) == 0 and not symbol.endswith('.US'):
            print(f"No results for {symbol}, trying with .US suffix")
            symbol_with_us = f"{symbol}.US"
            results = db_manager.fetch_all(query, [symbol_with_us, start_date, end_date])
            print(f"Found {len(results)} records for symbol {symbol_with_us}")
        
        # Format results for chart
        historical_data = []
        for row in results:
            historical_data.append({
                "date": row[0].strftime('%Y-%m-%d'),
                "open": float(row[1]) if row[1] else 0,
                "high": float(row[2]) if row[2] else 0,
                "low": float(row[3]) if row[3] else 0,
                "close": float(row[4]) if row[4] else 0,
                "volume": int(row[5]) if row[5] else 0
            })
        
        return {
            "success": True,
            "data": historical_data,
            "count": len(historical_data),
            "symbol": symbol,
            "date_range": {
                "start": start_date.strftime('%Y-%m-%d'),
                "end": end_date.strftime('%Y-%m-%d')
            }
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return {
            "success": False,
            "error": str(e),
            "data": [],
            "count": 0
        }

@router.get("/stock-historical-data/{symbol}", summary="Get historical stock data for candlestick chart")
async def get_stock_historical_data(
    symbol: str,
    days: int = Query(365, ge=30, le=3650, description="Number of days of historical data (30-3650)"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    Get historical stock data for candlestick chart
    
    Returns OHLCV data for the specified symbol over the given time period
    """
    try:
        print(f"Getting historical data for symbol: {symbol}, days: {days}")
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # Calculate date range
        from datetime import datetime, timedelta
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        print(f"Date range: {start_date} to {end_date}")
        
        # Query historical data from stock_prices_eod table first
        # Try exact match first, then try with .US suffix
        query_stock = """
        SELECT 
            date,
            open,
            high,
            low,
            close,
            volume
        FROM stock_prices_eod 
        WHERE code = %s 
        AND date >= %s 
        AND date <= %s
        AND period = 'd'
        ORDER BY date ASC
        """
        
        results = db_manager.fetch_all(query_stock, [symbol, start_date, end_date])
        print(f"Found {len(results)} records in stock_prices_eod for symbol {symbol}")
        
        # If no results and symbol doesn't have .US suffix, try adding it
        if len(results) == 0 and not symbol.endswith('.US'):
            print(f"No results for {symbol}, trying with .US suffix")
            symbol_with_us = f"{symbol}.US"
            results = db_manager.fetch_all(query_stock, [symbol_with_us, start_date, end_date])
            print(f"Found {len(results)} records in stock_prices_eod for symbol {symbol_with_us}")
        
        # If still no results, try market_data_eod table (for indices, forex, commodities, etc.)
        if len(results) == 0:
            print(f"No results in stock_prices_eod, trying market_data_eod for symbol {symbol}")
            query_market = """
            SELECT 
                date,
                open,
                high,
                low,
                close,
                volume
            FROM market_data_eod 
            WHERE code = %s 
            AND date >= %s 
            AND date <= %s
            ORDER BY date ASC
            """
            
            results = db_manager.fetch_all(query_market, [symbol, start_date, end_date])
            print(f"Found {len(results)} records in market_data_eod for symbol {symbol}")
            
            # If still no results and symbol doesn't have .US suffix, try adding it
            if len(results) == 0 and not symbol.endswith('.US'):
                print(f"No results for {symbol}, trying with .US suffix in market_data_eod")
                symbol_with_us = f"{symbol}.US"
                results = db_manager.fetch_all(query_market, [symbol_with_us, start_date, end_date])
                print(f"Found {len(results)} records in market_data_eod for symbol {symbol_with_us}")
        
        # Format results for chart
        historical_data = []
        for row in results:
            historical_data.append({
                "date": row[0].strftime('%Y-%m-%d'),
                "open": float(row[1]) if row[1] else 0,
                "high": float(row[2]) if row[2] else 0,
                "low": float(row[3]) if row[3] else 0,
                "close": float(row[4]) if row[4] else 0,
                "volume": int(row[5]) if row[5] else 0
            })
        
        # 优化：如果最后一天是今天，尝试用实时数据替换/补充
        # 这样 candlestick 图表的最后一根 K 线会显示实时价格
        today_str = datetime.now().date().strftime('%Y-%m-%d')
        has_today_data = any(item['date'] == today_str for item in historical_data)
        
        # 查询实时价格数据
        realtime_query = """
        SELECT 
            code,
            open,
            high,
            low,
            close,
            volume,
            last_updated
        FROM stock_prices_realtime 
        WHERE code = %s
        LIMIT 1
        """
        
        realtime_result = db_manager.fetch_one(realtime_query, [symbol])
        
        # 如果没有找到，并且 symbol 不包含 .US 后缀，尝试添加后缀
        if not realtime_result and not symbol.endswith('.US'):
            symbol_with_us = f"{symbol}.US"
            realtime_result = db_manager.fetch_one(realtime_query, [symbol_with_us])
        
        if realtime_result:
            # 提取实时数据
            realtime_data = {
                "date": today_str,
                "open": float(realtime_result[1]) if realtime_result[1] else 0,
                "high": float(realtime_result[2]) if realtime_result[2] else 0,
                "low": float(realtime_result[3]) if realtime_result[3] else 0,
                "close": float(realtime_result[4]) if realtime_result[4] else 0,
                "volume": int(realtime_result[5]) if realtime_result[5] else 0
            }
            
            # 如果有实时数据且数据有效（收盘价 > 0）
            if realtime_data['close'] > 0:
                if has_today_data:
                    # 替换今天的 EOD 数据为实时数据
                    for i, item in enumerate(historical_data):
                        if item['date'] == today_str:
                            historical_data[i] = realtime_data
                            print(f"✅ 用实时数据替换了今天 {today_str} 的 K 线数据")
                            break
                else:
                    # 添加今天的实时数据作为新的 K 线
                    historical_data.append(realtime_data)
                    print(f"✅ 添加了今天 {today_str} 的实时 K 线数据")
        
        return {
            "success": True,
            "data": historical_data,
            "count": len(historical_data),
            "symbol": symbol,
            "date_range": {
                "start": start_date.strftime('%Y-%m-%d'),
                "end": end_date.strftime('%Y-%m-%d')
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch historical data: {str(e)}"
        )

@router.put("/watchlist/reorder", summary="Reorder watchlist items")
async def reorder_watchlist_items(
    request: dict,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    批量更新watchlist的自定义排序顺序
    
    请求参数:
    - items: [{ "watchlist_id": int, "sort_order": int }]
    """
    items = request.get('items')
    if not items or not isinstance(items, list):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Items must be a non-empty list"
        )
    
    # 构建更新映射并校验参数
    updates_map = {}
    for item in items:
        if not isinstance(item, dict):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Each item must be an object containing watchlist_id and sort_order"
            )
        
        watchlist_id = item.get('watchlist_id')
        sort_order = item.get('sort_order')
        group_name = item.get('group_name')
        
        if watchlist_id is None or sort_order is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="watchlist_id and sort_order are required for each item"
            )
        
        try:
            watchlist_id = int(watchlist_id)
            sort_order = int(sort_order)
        except (TypeError, ValueError):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="watchlist_id and sort_order must be integers"
            )
        
        if sort_order < 1:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="sort_order must be greater than or equal to 1"
            )
        
        if group_name is not None:
            if not isinstance(group_name, str):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="group_name must be a string when provided"
                )
            group_name = group_name.strip()
            if group_name == '':
                group_name = None
        
        updates_map[watchlist_id] = {
            "sort_order": sort_order,
            "group_name": group_name
        }
    
    if not updates_map:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid watchlist items to update"
        )
    
    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    conn = db_manager.get_connection()
    
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Database connection failed"
        )
    
    try:
        cursor = conn.cursor()
        watchlist_ids = list(updates_map.keys())
        
        cursor.execute(
            """
            SELECT id, group_name 
            FROM target_tracking_config 
            WHERE user_id = %s AND id = ANY(%s)
            """,
            (current_user.user_id, watchlist_ids)
        )
        rows = cursor.fetchall()
        
        found_ids = {row[0] for row in rows}
        missing_ids = sorted(set(watchlist_ids) - found_ids)
        if missing_ids:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Watchlist items not found or permission denied: {missing_ids}"
            )
        
        # 校验每个目标分组内的排序值不重复，并准备更新数据
        group_sort_orders = {}
        final_updates = {}
        for watchlist_id, existing_group_name in rows:
            update_info = updates_map[watchlist_id]
            sort_order = update_info["sort_order"]
            target_group_name = update_info.get("group_name")
            
            # 如果未提供新的分组，则保持原分组
            if target_group_name is None:
                target_group_name = existing_group_name
            
            normalized_group_key = target_group_name or 'default'
            group_sort_orders.setdefault(normalized_group_key, set())
            if sort_order in group_sort_orders[normalized_group_key]:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Duplicate sort_order {sort_order} detected in group {normalized_group_key}"
                )
            group_sort_orders[normalized_group_key].add(sort_order)
            
            # 存储最终需要更新的数据（group_name保留None表示默认分组）
            final_updates[watchlist_id] = {
                "sort_order": sort_order,
                "group_name": target_group_name
            }
        
        for watchlist_id, _ in rows:
            update_values = final_updates[watchlist_id]
            target_group_name = update_values["group_name"]
            sort_order = update_values["sort_order"]
            
            # 空字符串作为默认分组，需要存储为NULL
            db_group_name = target_group_name if target_group_name not in (None, '', 'default') else None
            
            cursor.execute(
                """
                UPDATE target_tracking_config
                SET sort_order = %s,
                    group_name = %s
                WHERE id = %s AND user_id = %s
                """,
                (sort_order, db_group_name, watchlist_id, current_user.user_id)
            )
        
        conn.commit()
        return {
            "message": "Watchlist sort order updated successfully",
            "updated": len(rows)
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update watchlist sort order: {str(e)}"
        )
    finally:
        conn.close()

@router.put("/watchlist/{watchlist_id}", summary="Update watchlist item")
async def update_watchlist_item(
    watchlist_id: int,
    request: dict,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    更新watchlist项的priority和notes
    
    请求参数:
    - priority: 优先级 (可选，支持1=Low, 2=Medium, 3=High)
    - notes: 备注 (可选)
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 检查该watchlist项是否属于当前用户
        check_query = """
        SELECT symbol, priority, notes FROM target_tracking_config 
        WHERE id = %s AND user_id = %s
        """
        result = db_manager.fetch_one(check_query, [watchlist_id, current_user.user_id])
        
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Watchlist item not found or permission denied"
            )
        
        symbol = result[0]
        current_priority = result[1]
        current_notes = result[2]
        
        # 获取更新的值，如果没有提供则使用当前值
        new_priority = request.get('priority', current_priority)
        new_notes = request.get('notes', current_notes)
        
        # 验证priority值
        if new_priority not in [1, 2, 3]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Priority must be 1 (Low), 2 (Medium), or 3 (High)"
            )
        
        # 更新watchlist项
        update_query = """
        UPDATE target_tracking_config 
        SET priority = %s, notes = %s
        WHERE id = %s AND user_id = %s
        RETURNING id
        """
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Database connection failed"
            )
        
        try:
            cursor = conn.cursor()
            cursor.execute(update_query, [new_priority, new_notes, watchlist_id, current_user.user_id])
            result = cursor.fetchone()
            conn.commit()
            conn.close()
            
            if result:
                return {
                    "message": f"Watchlist item {symbol} updated successfully",
                    "watchlist_id": watchlist_id,
                    "symbol": symbol,
                    "priority": new_priority,
                    "notes": new_notes
                }
            else:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to update watchlist item"
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
            detail=f"Failed to update watchlist item: {str(e)}"
        )

@router.delete("/watchlist/{watchlist_id}", summary="Remove stock from watchlist")
async def remove_from_watchlist(
    watchlist_id: int,
    current_user: UserResponse = Depends(get_current_user)
):
    """
    Remove stock from user's watchlist and cascade delete related chart events
    """
    try:
        db_conn_params = get_database_config()
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
                # 级联删除该股票的所有图表事件
                from backend.services.chart_events_service import get_chart_events_service
                chart_events_service = get_chart_events_service()
                deleted_events_count = chart_events_service.delete_events_by_symbol(
                    current_user.user_id, 
                    symbol
                )
                
                logger.info(
                    f"Removed {symbol} from watchlist and deleted {deleted_events_count} chart events",
                    user_id=current_user.user_id,
                    symbol=symbol,
                    deleted_events=deleted_events_count
                )
                
                return {
                    "message": f"Successfully removed {symbol} from watchlist",
                    "symbol": symbol,
                    "deleted_events": deleted_events_count
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
            detail=f"Failed to remove watchlist item: {str(e)}"
        )

@router.get("/search/stocks", summary="Search stocks (for adding to watchlist)")
async def search_stocks(
    q: str = Query(..., description="Search keywords"),
    limit: int = Query(20, description="Return result limit"),
    current_user: UserResponse = Depends(get_current_user)
):
    """
    搜索股票，用于添加到watchlist时的股票选择
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        query = """
        SELECT 
            c.code as ticker,
            c.name as company,
            c.sector,
            c.industry,
            c.exchange,
            c.id as company_id
        FROM companies c
        WHERE c.ticker_active = true
        AND (
            c.code ILIKE %s OR 
            c.name ILIKE %s OR
            c.sector ILIKE %s OR
            c.industry ILIKE %s
        )
        ORDER BY 
            CASE 
                WHEN UPPER(c.code) = UPPER(%s) THEN 1
                WHEN UPPER(c.code) LIKE UPPER(%s) THEN 2
                WHEN UPPER(c.name) LIKE UPPER(%s) THEN 3
                WHEN UPPER(c.sector) LIKE UPPER(%s) THEN 4
                WHEN UPPER(c.industry) LIKE UPPER(%s) THEN 5
                ELSE 6
            END,
            c.name
        LIMIT %s
        """
        
        search_term = f"%{q}%"
        exact_match = q.upper()
        results = db_manager.fetch_all(query, [
            search_term, search_term, search_term, search_term,  # WHERE conditions
            exact_match, search_term, search_term, search_term, search_term,  # ORDER BY conditions
            limit
        ])
        
        stocks = []
        for row in results:
            stock = {
                "ticker": row[0],
                "company": row[1],
                "sector": row[2],
                "industry": row[3],
                "exchange": row[4],
                "company_id": row[5]
            }
            stocks.append(stock)
        
        return stocks
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Search stocks failed: {str(e)}"
        )

@router.get("/health", summary="check dashboard API health")
async def health_check():
    """
    仪表板API健康检查端点
    """
    return {"status": "healthy", "service": "dashboard", "timestamp": datetime.now().isoformat()}



