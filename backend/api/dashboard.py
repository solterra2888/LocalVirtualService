#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dashboard API - 仪表板数据接口

提供公司、行业、新闻、AI分析等数据的综合查询接口
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, HTTPException, Depends, Query, status
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_user
from backend.utils.logger import info, error, warning
from backend.database.db_manager import DatabaseManager
from config.settings import get_db_conn_params

router = APIRouter()

# === 响应模型 ===

class CompanyInfoResponse(BaseModel):
    """公司信息响应模型"""
    id: int
    name: str
    primaryticker: str
    sector: Optional[str] = None
    industry: Optional[str] = None
    gicsector: Optional[str] = None
    gicindustry: Optional[str] = None
    gicsubindustry: Optional[str] = None
    countryname: Optional[str] = None

class SectorInfoResponse(BaseModel):
    """行业信息响应模型"""
    sector: str
    company_count: int
    companies: List[CompanyInfoResponse]

class DashboardNewsResponse(BaseModel):
    """Dashboard新闻响应模型"""
    id: int
    title: str
    content: Optional[str] = None
    date: Optional[str] = None
    source: Optional[str] = None
    link: Optional[str] = None
    sentiment_polarity: Optional[float] = None
    sentiment_pos: Optional[float] = None
    sentiment_neg: Optional[float] = None
    sentiment_neu: Optional[float] = None
    symbols: List[str] = Field(default_factory=list)
    tags: List[str] = Field(default_factory=list)

class DashboardAnalysisResponse(BaseModel):
    """Dashboard分析结果响应模型"""
    id: int
    filename: str
    content: str
    created_at: str
    file_type: str
    status: str
    total_tokens: int
    cost: float
    task_id: Optional[str] = None

# === 辅助函数 ===

def get_db_connection():
    """获取数据库连接"""
    db_conn_params = get_db_conn_params()
    db_manager = DatabaseManager(db_conn_params)
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="无法连接到数据库"
        )
    return conn

# === API端点 ===

@router.get("/sectors", response_model=List[SectorInfoResponse], summary="获取所有行业信息")
async def get_sectors(current_user=Depends(get_current_user)):
    """
    获取所有行业信息，包括每个行业的公司数量
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # 获取行业及其公司数量
            cursor.execute("""
                SELECT sector, COUNT(*) as company_count
                FROM companies 
                WHERE sector IS NOT NULL AND sector != '' AND ticker_active = true
                GROUP BY sector
                ORDER BY company_count DESC, sector
            """)
            
            sector_stats = cursor.fetchall()
            result = []
            
            for sector_name, company_count in sector_stats:
                # 获取该行业的前20家公司
                cursor.execute("""
                    SELECT id, name, primaryticker, sector, industry, 
                           gicsector, gicindustry, gicsubindustry, countryname
                    FROM companies 
                    WHERE sector = %s AND ticker_active = true
                    ORDER BY name
                    LIMIT 20
                """, (sector_name,))
                
                companies_data = cursor.fetchall()
                companies = [
                    CompanyInfoResponse(
                        id=row[0],
                        name=row[1],
                        primaryticker=row[2],
                        sector=row[3],
                        industry=row[4],
                        gicsector=row[5],
                        gicindustry=row[6],
                        gicsubindustry=row[7],
                        countryname=row[8]
                    )
                    for row in companies_data
                ]
                
                result.append(SectorInfoResponse(
                    sector=sector_name,
                    company_count=company_count,
                    companies=companies
                ))
            
            info(f"获取行业信息成功: {len(result)} 个行业", source="dashboard_api")
            return result
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        error(f"获取行业信息失败: {str(e)}", source="dashboard_api")
        raise HTTPException(status_code=500, detail=f"获取行业信息失败: {str(e)}")

@router.get("/companies", response_model=List[CompanyInfoResponse], summary="获取公司信息")
async def get_companies(
    sector: Optional[str] = Query(None, description="行业筛选"),
    search: Optional[str] = Query(None, description="搜索关键词"),
    limit: int = Query(100, ge=1, le=1000, description="返回数量限制"),
    current_user=Depends(get_current_user)
):
    """
    获取公司信息，支持按行业筛选和关键词搜索
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # 构建查询条件
            where_conditions = ["ticker_active = true"]
            params = []
            
            if sector:
                where_conditions.append("sector = %s")
                params.append(sector)
            
            if search:
                where_conditions.append("""
                    (LOWER(name) LIKE %s 
                     OR LOWER(primaryticker) LIKE %s 
                     OR LOWER(industry) LIKE %s)
                """)
                search_term = f"%{search.lower()}%"
                params.extend([search_term, search_term, search_term])
            
            where_clause = " AND ".join(where_conditions)
            
            cursor.execute(f"""
                SELECT id, name, primaryticker, sector, industry, 
                       gicsector, gicindustry, gicsubindustry, countryname
                FROM companies 
                WHERE {where_clause}
                ORDER BY name
                LIMIT %s
            """, params + [limit])
            
            companies_data = cursor.fetchall()
            result = [
                CompanyInfoResponse(
                    id=row[0],
                    name=row[1],
                    primaryticker=row[2],
                    sector=row[3],
                    industry=row[4],
                    gicsector=row[5],
                    gicindustry=row[6],
                    gicsubindustry=row[7],
                    countryname=row[8]
                )
                for row in companies_data
            ]
            
            info(f"获取公司信息成功: {len(result)} 家公司", source="dashboard_api")
            return result
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        error(f"获取公司信息失败: {str(e)}", source="dashboard_api")
        raise HTTPException(status_code=500, detail=f"获取公司信息失败: {str(e)}")

@router.get("/companies/{ticker}", response_model=CompanyInfoResponse, summary="获取特定公司信息")
async def get_company_info(
    ticker: str,
    current_user=Depends(get_current_user)
):
    """
    获取特定公司的详细信息
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT id, name, primaryticker, sector, industry, 
                       gicsector, gicindustry, gicsubindustry, countryname
                FROM companies 
                WHERE primaryticker = %s AND ticker_active = true
            """, (ticker.upper(),))
            
            company_data = cursor.fetchone()
            if not company_data:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"公司不存在: {ticker}"
                )
            
            result = CompanyInfoResponse(
                id=company_data[0],
                name=company_data[1],
                primaryticker=company_data[2],
                sector=company_data[3],
                industry=company_data[4],
                gicsector=company_data[5],
                gicindustry=company_data[6],
                gicsubindustry=company_data[7],
                countryname=company_data[8]
            )
            
            info(f"获取公司详情成功: {ticker}", source="dashboard_api")
            return result
            
        finally:
            cursor.close()
            conn.close()
            
    except HTTPException:
        raise
    except Exception as e:
        error(f"获取公司详情失败: {str(e)}", source="dashboard_api")
        raise HTTPException(status_code=500, detail=f"获取公司详情失败: {str(e)}")

@router.get("/companies/{ticker}/news", response_model=List[DashboardNewsResponse], summary="获取公司相关新闻")
async def get_company_news(
    ticker: str,
    limit: int = Query(20, ge=1, le=100, description="返回数量限制"),
    current_user=Depends(get_current_user)
):
    """
    获取特定公司相关的新闻
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT DISTINCT n.id, n.title, n.content, n.date, n.source, n.link,
                       n.sentiment_polarity, n.sentiment_pos, n.sentiment_neg, n.sentiment_neu
                FROM news n
                JOIN news_symbols ns ON n.id = ns.news_id
                JOIN news_symbols_dict nsd ON ns.symbol_id = nsd.id
                WHERE UPPER(nsd.symbol_code) = %s
                ORDER BY n.date DESC, n.id DESC
                LIMIT %s
            """, (ticker.upper(), limit))
            
            news_data = cursor.fetchall()
            result = []
            
            for row in news_data:
                news_id = row[0]
                
                # 获取相关股票代码
                cursor.execute("""
                    SELECT nsd.symbol_code
                    FROM news_symbols ns
                    JOIN news_symbols_dict nsd ON ns.symbol_id = nsd.id
                    WHERE ns.news_id = %s
                """, (news_id,))
                symbols = [s[0] for s in cursor.fetchall()]
                
                # 获取相关标签
                cursor.execute("""
                    SELECT nt.tag_name
                    FROM news_tags_mapping ntm
                    JOIN news_tags nt ON ntm.tag_id = nt.id
                    WHERE ntm.news_id = %s
                """, (news_id,))
                tags = [t[0] for t in cursor.fetchall()]
                
                result.append(DashboardNewsResponse(
                    id=news_id,
                    title=row[1],
                    content=row[2],
                    date=row[3].isoformat() if row[3] else None,
                    source=row[4],
                    link=row[5],
                    sentiment_polarity=row[6],
                    sentiment_pos=row[7],
                    sentiment_neg=row[8],
                    sentiment_neu=row[9],
                    symbols=symbols,
                    tags=tags
                ))
            
            info(f"获取公司新闻成功: {ticker}, {len(result)} 条", source="dashboard_api")
            return result
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        error(f"获取公司新闻失败: {str(e)}", source="dashboard_api")
        raise HTTPException(status_code=500, detail=f"获取公司新闻失败: {str(e)}")

@router.get("/companies/{ticker}/analysis", response_model=List[DashboardAnalysisResponse], summary="获取公司AI分析结果")
async def get_company_analysis(
    ticker: str,
    limit: int = Query(10, ge=1, le=50, description="返回数量限制"),
    current_user=Depends(get_current_user)
):
    """
    获取特定公司相关的AI分析结果
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # 搜索文件名包含股票代码的AI分析结果
            cursor.execute("""
                SELECT id, filename, content, created_at, file_type, status, 
                       total_tokens, cost, task_id
                FROM file_processing_details
                WHERE (UPPER(filename) LIKE %s OR UPPER(content) LIKE %s)
                  AND status = 'completed'
                  AND content IS NOT NULL 
                  AND content != ''
                ORDER BY created_at DESC
                LIMIT %s
            """, (f'%{ticker.upper()}%', f'%{ticker.upper()}%', limit))
            
            analysis_data = cursor.fetchall()
            result = [
                DashboardAnalysisResponse(
                    id=row[0],
                    filename=row[1],
                    content=row[2],
                    created_at=row[3].isoformat() if row[3] else '',
                    file_type=row[4] or '',
                    status=row[5],
                    total_tokens=row[6] or 0,
                    cost=float(row[7]) if row[7] else 0.0,
                    task_id=row[8]
                )
                for row in analysis_data
            ]
            
            info(f"获取公司AI分析成功: {ticker}, {len(result)} 条", source="dashboard_api")
            return result
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        error(f"获取公司AI分析失败: {str(e)}", source="dashboard_api")
        raise HTTPException(status_code=500, detail=f"获取公司AI分析失败: {str(e)}")

@router.get("/companies/{ticker}/reports", response_model=List[DashboardAnalysisResponse], summary="获取公司相关研报文件")
async def get_company_reports(
    ticker: str,
    limit: int = Query(10, ge=1, le=50, description="返回数量限制"),
    current_user=Depends(get_current_user)
):
    """
    获取特定公司相关的原始研报文件（不包含AI分析内容）
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # 搜索文件名包含股票代码的原始文件
            cursor.execute("""
                SELECT id, filename, '' as content, created_at, file_type, status, 
                       0 as total_tokens, 0 as cost, task_id
                FROM file_processing_details
                WHERE UPPER(filename) LIKE %s
                  AND file_type IN ('pdf', 'docx', 'doc', 'txt')
                ORDER BY created_at DESC
                LIMIT %s
            """, (f'%{ticker.upper()}%', limit))
            
            reports_data = cursor.fetchall()
            result = [
                DashboardAnalysisResponse(
                    id=row[0],
                    filename=row[1],
                    content=row[2],  # 空内容，因为这是原始文件
                    created_at=row[3].isoformat() if row[3] else '',
                    file_type=row[4] or '',
                    status=row[5],
                    total_tokens=row[6] or 0,
                    cost=float(row[7]) if row[7] else 0.0,
                    task_id=row[8]
                )
                for row in reports_data
            ]
            
            info(f"获取公司研报成功: {ticker}, {len(result)} 条", source="dashboard_api")
            return result
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        error(f"获取公司研报失败: {str(e)}", source="dashboard_api")
        raise HTTPException(status_code=500, detail=f"获取公司研报失败: {str(e)}")

# 行业相关的API端点（类似公司的实现）
@router.get("/sectors/{sector}/news", response_model=List[DashboardNewsResponse], summary="获取行业相关新闻")
async def get_sector_news(
    sector: str,
    limit: int = Query(20, ge=1, le=100, description="返回数量限制"),
    current_user=Depends(get_current_user)
):
    """
    获取特定行业相关的新闻
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            # 首先获取该行业的所有公司股票代码
            cursor.execute("""
                SELECT DISTINCT primaryticker 
                FROM companies 
                WHERE sector = %s AND ticker_active = true
            """, (sector,))
            
            tickers = [t[0] for t in cursor.fetchall()]
            
            if not tickers:
                return []
            
            # 获取这些公司相关的新闻
            ticker_placeholders = ','.join(['%s'] * len(tickers))
            cursor.execute(f"""
                SELECT DISTINCT n.id, n.title, n.content, n.date, n.source, n.link,
                       n.sentiment_polarity, n.sentiment_pos, n.sentiment_neg, n.sentiment_neu
                FROM news n
                JOIN news_symbols ns ON n.id = ns.news_id
                JOIN news_symbols_dict nsd ON ns.symbol_id = nsd.id
                WHERE UPPER(nsd.symbol_code) IN ({ticker_placeholders})
                ORDER BY n.date DESC, n.id DESC
                LIMIT %s
            """, [t.upper() for t in tickers] + [limit])
            
            news_data = cursor.fetchall()
            result = []
            
            for row in news_data:
                news_id = row[0]
                
                # 获取相关股票代码
                cursor.execute("""
                    SELECT nsd.symbol_code
                    FROM news_symbols ns
                    JOIN news_symbols_dict nsd ON ns.symbol_id = nsd.id
                    WHERE ns.news_id = %s
                """, (news_id,))
                symbols = [s[0] for s in cursor.fetchall()]
                
                # 获取相关标签
                cursor.execute("""
                    SELECT nt.tag_name
                    FROM news_tags_mapping ntm
                    JOIN news_tags nt ON ntm.tag_id = nt.id
                    WHERE ntm.news_id = %s
                """, (news_id,))
                tags = [t[0] for t in cursor.fetchall()]
                
                result.append(DashboardNewsResponse(
                    id=news_id,
                    title=row[1],
                    content=row[2],
                    date=row[3].isoformat() if row[3] else None,
                    source=row[4],
                    link=row[5],
                    sentiment_polarity=row[6],
                    sentiment_pos=row[7],
                    sentiment_neg=row[8],
                    sentiment_neu=row[9],
                    symbols=symbols,
                    tags=tags
                ))
            
            info(f"获取行业新闻成功: {sector}, {len(result)} 条", source="dashboard_api")
            return result
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        error(f"获取行业新闻失败: {str(e)}", source="dashboard_api")
        raise HTTPException(status_code=500, detail=f"获取行业新闻失败: {str(e)}")

@router.get("/files/{file_id}/content", summary="获取文件内容")
async def get_file_content(
    file_id: int,
    current_user=Depends(get_current_user)
):
    """
    获取文件的详细内容
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT filename, content, file_type
                FROM file_processing_details
                WHERE id = %s
            """, (file_id,))
            
            file_data = cursor.fetchone()
            if not file_data:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"文件不存在: {file_id}"
                )
            
            return {
                "filename": file_data[0],
                "content": file_data[1] or "内容为空",
                "file_type": file_data[2]
            }
            
        finally:
            cursor.close()
            conn.close()
            
    except HTTPException:
        raise
    except Exception as e:
        error(f"获取文件内容失败: {str(e)}", source="dashboard_api")
        raise HTTPException(status_code=500, detail=f"获取文件内容失败: {str(e)}")
