#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Markets API - 市场数据接口

提供经济事件和宏观指标数据的API端点
"""

import os
import sys
import io
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
from fastapi import APIRouter, HTTPException, Depends, Query, status, Response
from pydantic import BaseModel, Field

from backend.auth.jwt_utils import get_current_user
from backend.utils.logger import info, error, warning
from backend.utils.service_registry import get_service_registry
from backend.services.earnings_calendar_service import EarningsCalendarService
from backend.services.economic_events_service import (
    EconomicEventsService,
    calculate_event_impact,
)
from backend.services.macro_indicators_service import MacroIndicatorsService
from backend.services.market_data_service import MarketDataService
import psycopg2.extras

def process_logo_url(logo_url):
    """处理LogoURL，确保包含完整的EODHD域名"""
    if not logo_url or logo_url == "N/A" or logo_url == "" or logo_url == "None":
        return None
    
    logo_url = str(logo_url).strip()
    
    # 如果已经是完整URL，直接返回
    if logo_url.startswith(('http://', 'https://')):
        return logo_url
    
    # 如果是相对路径，添加EODHD域名前缀
    if logo_url.startswith('/'):
        return f"https://eodhd.com{logo_url}"
    
    # 如果路径不以/开头，添加/前缀
    return f"https://eodhd.com/{logo_url}"

router = APIRouter()

# === 服务获取辅助函数 ===

def get_market_data_service() -> MarketDataService:
    """获取市场数据服务实例（单例）"""
    registry = get_service_registry()
    return registry.get_or_create_service("market_data_service", MarketDataService)

def get_macro_indicators_service() -> MacroIndicatorsService:
    """获取宏观指标服务实例（单例）"""
    registry = get_service_registry()
    return registry.get_or_create_service("macro_indicators_service", MacroIndicatorsService)

def get_economic_events_service() -> EconomicEventsService:
    """获取经济事件服务实例（单例）"""
    registry = get_service_registry()
    return registry.get_or_create_service("economic_events_service", EconomicEventsService)

def get_earnings_calendar_service() -> EarningsCalendarService:
    """获取财报日历服务实例（单例）"""
    registry = get_service_registry()
    return registry.get_or_create_service("earnings_calendar_service", EarningsCalendarService)

# === Pydantic Models ===

class EconomicEventResponse(BaseModel):
    """经济事件响应模型"""
    date: Optional[str] = Field(None, description="事件日期")
    time: Optional[str] = Field(None, description="事件时间")
    country: Optional[str] = Field(None, description="国家代码")
    event: Optional[str] = Field(None, description="事件名称")
    period: Optional[str] = Field(None, description="周期")
    actual: Optional[Union[str, int, float]] = Field(None, description="实际值")
    previous: Optional[Union[str, int, float]] = Field(None, description="前值")
    forecast: Optional[Union[str, int, float]] = Field(None, description="预期值")
    comparison: Optional[str] = Field(None, description="对比口径: null=指数级 / yoy / mom")
    calculated_change: Optional[float] = Field(None, description="计算变化值")
    calculated_change_pct: Optional[float] = Field(None, description="计算变化率")
    impact: Optional[int] = Field(None, description="重要性等级 (1-3星)")

class MacroIndicatorResponse(BaseModel):
    """宏观指标响应模型"""
    country_code: str = Field(..., description="国家代码")
    country_name: str = Field(..., description="国家名称")
    indicator: str = Field(..., description="指标代码")
    indicator_name: str = Field(..., description="指标名称")
    year: int = Field(..., description="年份")
    value: Optional[float] = Field(None, description="指标值")
    date_str: Optional[str] = Field(None, description="原始日期字符串")
    fetch_timestamp: Optional[datetime] = Field(None, description="获取时间戳")

class MarketStatsResponse(BaseModel):
    """市场统计响应模型"""
    total_events: int = Field(..., description="总事件数")
    latest_date: Optional[str] = Field(None, description="最新数据日期")
    country_stats: Dict[str, int] = Field(..., description="按国家统计")
    date_stats: Dict[str, int] = Field(..., description="按日期统计")
    total_countries: int = Field(..., description="总国家数")

class CountryInfo(BaseModel):
    """国家信息模型"""
    code: str = Field(..., description="国家代码")
    name: str = Field(..., description="国家名称")

class IndicatorInfo(BaseModel):
    """指标信息模型"""
    indicator: str = Field(..., description="指标代码")
    indicator_name: str = Field(..., description="指标名称")
    data_points: int = Field(..., description="数据点数量")
    earliest_year: int = Field(..., description="最早年份")
    latest_year: int = Field(..., description="最新年份")

class EarningsCalendarItemResponse(BaseModel):
    """财报日历条目响应模型"""
    code: str = Field(..., description="股票代码")
    company_name: Optional[str] = Field(None, description="公司名称")
    logo_url: Optional[str] = Field(None, description="公司Logo URL")
    report_date: Optional[str] = Field(None, description="财报发布日期 (YYYY-MM-DD)")
    before_after_market: Optional[str] = Field(None, description="发布时间: BeforeMarket/AfterMarket")
    currency: Optional[str] = Field(None, description="货币")
    eps_actual: Optional[float] = Field(None, description="EPS 实际值（未公布时为空）")
    eps_estimate: Optional[float] = Field(None, description="EPS 预期值")
    eps_difference: Optional[float] = Field(None, description="EPS 差异（实际-预期）")
    eps_surprise_percent: Optional[float] = Field(None, description="EPS 惊喜率（%）")
    reported: bool = Field(False, description="是否已公布")
    fetch_date: Optional[str] = Field(None, description="数据快照日期")

class EarningsCalendarMetaResponse(BaseModel):
    """财报日历查询元信息"""
    total: int = Field(..., description="符合条件的总条数")
    limit: int = Field(..., description="本次返回上限")
    offset: int = Field(..., description="本次偏移量")
    scope: str = Field(..., description="查询范围: watchlist/theme/us")
    from_date: Optional[str] = Field(None, description="查询开始日期")
    to_date: Optional[str] = Field(None, description="查询结束日期")
    latest_fetch_date: Optional[str] = Field(None, description="数据最近一次抓取日期")
    latest_update_at: Optional[str] = Field(None, description="数据最近更新时间")

class EarningsCalendarResponse(BaseModel):
    """财报日历查询响应"""
    items: List[EarningsCalendarItemResponse] = Field(..., description="财报条目列表")
    meta: EarningsCalendarMetaResponse = Field(..., description="查询元信息")

class EarningsMonthDayTopItem(BaseModel):
    """月视图单日 Top 公司条目"""
    code: str = Field(..., description="股票代码")
    company_name: Optional[str] = Field(None, description="公司名称")
    logo_url: Optional[str] = Field(None, description="公司Logo URL")
    eps_estimate: Optional[float] = Field(None, description="EPS 预期值")
    eps_actual: Optional[float] = Field(None, description="EPS 实际值")
    eps_surprise_percent: Optional[float] = Field(None, description="EPS 惊喜率（%）")
    reported: bool = Field(False, description="是否已公布")
    market_cap: Optional[float] = Field(None, description="市值（美元）")

class EarningsMonthDaySummary(BaseModel):
    """月视图单日汇总"""
    date: Optional[str] = Field(None, description="日期 (YYYY-MM-DD)")
    count: int = Field(0, description="当日财报家数")
    before_count: int = Field(0, description="盘前家数")
    after_count: int = Field(0, description="盘后家数")
    top: List[EarningsMonthDayTopItem] = Field(default_factory=list, description="市值 Top N 公司")

class EarningsCalendarMonthSummaryResponse(BaseModel):
    """财报日历月度汇总响应"""
    month: str = Field(..., description="月份 (YYYY-MM)")
    scope: str = Field(..., description="查询范围: watchlist/theme/us/hk")
    total: int = Field(..., description="当月财报总家数")
    days: List[EarningsMonthDaySummary] = Field(default_factory=list, description="逐日汇总")

# === API Endpoints ===

@router.get("/economic-events", response_model=List[EconomicEventResponse], summary="获取经济事件数据")
async def get_economic_events(
    from_date: Optional[str] = Query(None, description="开始日期 (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="结束日期 (YYYY-MM-DD)"),
    country: Optional[str] = Query(None, description="国家代码"),
    comparison: Optional[str] = Query('index', description="对比口径: index=指数级(默认) / yoy / mom / all=全部"),
    limit: int = Query(100, ge=1, le=1000, description="返回数量限制"),
    offset: int = Query(0, ge=0, description="偏移量"),
    current_user=Depends(get_current_user)
):
    """
    获取经济事件数据（从数据库）

    - **from_date**: 开始日期，格式 YYYY-MM-DD
    - **to_date**: 结束日期，格式 YYYY-MM-DD
    - **country**: 国家代码 (ISO 3166)，支持多个国家用逗号分隔
    - **comparison**: 对比口径。index=指数级(comparison 为空, 默认) / yoy=同比 / mom=环比 / all=全部口径
    - **limit**: 返回数量限制 (1-1000)
    - **offset**: 偏移量
    """
    try:
        info(f"获取经济事件数据请求: user={current_user.username}, from_date={from_date}, to_date={to_date}, country={country}, comparison={comparison}", source="markets_api")

        # 从数据库获取数据
        events_service = get_economic_events_service()
        events = events_service.get_economic_events_from_db(
            from_date=from_date,
            to_date=to_date,
            country=country,
            comparison=None if comparison == 'all' else comparison,
            limit=limit,
            offset=offset
        )
        
        # 计算变化值
        for event in events:
            # 计算 calculated_change 和 calculated_change_pct
            if event.get('actual') and event.get('previous'):
                try:
                    actual_val = float(event['actual'])
                    previous_val = float(event['previous'])
                    change = actual_val - previous_val
                    change_pct = (change / previous_val) * 100 if previous_val != 0 else 0
                    event['calculated_change'] = round(change, 4)
                    event['calculated_change_pct'] = round(change_pct, 2)
                except (ValueError, TypeError):
                    event['calculated_change'] = None
                    event['calculated_change_pct'] = None
            else:
                event['calculated_change'] = None
                event['calculated_change_pct'] = None
        
        info(f"从数据库获取到 {len(events)} 条经济事件数据", source="markets_api")
        return events
        
    except Exception as e:
        error(f"获取经济事件数据失败: {str(e)}", source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取经济事件数据失败: {str(e)}"
        )



@router.get("/macro-indicators", response_model=List[MacroIndicatorResponse], summary="获取宏观指标数据")
async def get_macro_indicators(
    country: Optional[Union[str, List[str]]] = Query(None, description="国家代码，支持单个或多个国家"),
    indicator: Optional[Union[str, List[str]]] = Query(None, description="指标代码，支持单个或多个指标"),
    year_from: Optional[int] = Query(None, description="起始年份"),
    year_to: Optional[int] = Query(None, description="结束年份"),
    limit: int = Query(1000, ge=1, le=5000, description="返回数量限制"),
    current_user=Depends(get_current_user)
):
    """
    获取宏观指标数据
    
    - **country**: 国家代码 (ISO 3166 Alpha-3)，支持单个或多个国家，多个国家用逗号分隔
    - **indicator**: 指标代码，支持单个或多个指标，多个指标用逗号分隔
    - **year_from**: 起始年份
    - **year_to**: 结束年份
    - **limit**: 返回数量限制 (1-5000)
    """
    try:
        info(f"获取宏观指标数据请求: user={current_user.username}, country={country}, indicator={indicator}", source="markets_api")
        
        # 处理多选参数
        processed_country = None
        processed_indicator = None
        
        if country:
            if isinstance(country, str):
                # 如果是字符串，按逗号分割
                processed_country = [c.strip() for c in country.split(',') if c.strip()]
            elif isinstance(country, list):
                # 如果是列表，需要处理每个元素可能包含逗号的情况
                processed_country = []
                for item in country:
                    if isinstance(item, str):
                        processed_country.extend([c.strip() for c in item.split(',') if c.strip()])
                    else:
                        processed_country.append(str(item))
            else:
                processed_country = [str(country)]
        
        if indicator:
            if isinstance(indicator, str):
                # 如果是字符串，按逗号分割
                processed_indicator = [i.strip() for i in indicator.split(',') if i.strip()]
            elif isinstance(indicator, list):
                # 如果是列表，需要处理每个元素可能包含逗号的情况
                processed_indicator = []
                for item in indicator:
                    if isinstance(item, str):
                        processed_indicator.extend([i.strip() for i in item.split(',') if i.strip()])
                    else:
                        processed_indicator.append(str(item))
            else:
                processed_indicator = [str(indicator)]
        
        # 初始化API客户端
        macro_service = get_macro_indicators_service()
        
        # 从数据库获取数据
        data = macro_service.get_stored_data(
            country=processed_country,
            indicator=processed_indicator,
            year_from=year_from,
            year_to=year_to,
            limit=limit
        )
        
        info(f"获取到 {len(data)} 条宏观指标数据", source="markets_api")
        return data
        
    except Exception as e:
        error(f"获取宏观指标数据失败: {str(e)}", source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取宏观指标数据失败: {str(e)}"
        )



@router.get("/statistics", response_model=MarketStatsResponse, summary="获取市场数据统计")
async def get_market_statistics(
    current_user=Depends(get_current_user)
):
    """
    获取市场数据统计信息
    """
    try:
        info(f"获取市场数据统计: user={current_user.username}", source="markets_api")
        
        market_service = get_market_data_service()
        stats = market_service.get_events_statistics()
        
        return stats
        
    except Exception as e:
        error(f"获取市场数据统计失败: {str(e)}", source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取市场数据统计失败: {str(e)}"
        )

@router.get("/countries", response_model=List[CountryInfo], summary="获取可用国家列表")
async def get_available_countries(
    current_user=Depends(get_current_user)
):
    """
    获取数据库中可用的国家列表
    """
    try:
        # 初始化API客户端
        macro_service = get_macro_indicators_service()
        
        # 从数据库获取国家列表
        countries = macro_service.get_available_countries()
        
        # 如果数据库中没有数据，返回常用国家列表
        if not countries:
            common_countries = macro_service.list_common_countries()
            countries = [{"code": code, "name": name} for code, name in common_countries.items()]
        
        return countries
        
    except Exception as e:
        error(f"获取国家列表失败: {str(e)}", source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取国家列表失败: {str(e)}"
        )

@router.get("/economic-events/countries", response_model=List[CountryInfo], summary="获取经济事件可用国家列表")
async def get_economic_events_countries(
    current_user=Depends(get_current_user)
):
    """
    获取经济事件数据库中可用的国家列表
    """
    try:
        # 获取经济事件服务
        events_service = get_economic_events_service()
        
        # 从数据库获取可用的国家列表
        countries = events_service.get_available_countries_from_db()
        
        return countries
        
    except Exception as e:
        error(f"获取经济事件国家列表失败: {str(e)}", source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取经济事件国家列表失败: {str(e)}"
        )

@router.get("/earnings-calendar", response_model=EarningsCalendarResponse, summary="获取财报日历数据")
async def get_earnings_calendar(
    from_date: Optional[str] = Query(None, description="开始日期 (YYYY-MM-DD)，默认今天-7天"),
    to_date: Optional[str] = Query(None, description="结束日期 (YYYY-MM-DD)，默认今天+14天"),
    scope: str = Query('watchlist', pattern='^(watchlist|theme|us|hk)$', description="范围: watchlist=自选 / theme=主题关联公司 / us=全部美股 / hk=全部港股"),
    before_after: Optional[str] = Query(None, pattern='^(before|after)$', description="发布时间筛选: before=盘前 / after=盘后"),
    search: Optional[str] = Query(None, max_length=50, description="模糊搜索 ticker 或公司名"),
    reported: Optional[str] = Query(None, pattern='^(yes|no)$', description="是否已公布: yes=已公布 / no=未公布"),
    min_market_cap: Optional[float] = Query(None, description="最小市值筛选(USD 美元, 港股已按汇率换算), 如 1e10=$10B"),
    limit: int = Query(500, ge=1, le=2000, description="返回数量限制 (1-2000)"),
    offset: int = Query(0, ge=0, description="偏移量"),
    current_user=Depends(get_current_user)
):
    """
    获取财报日历展示数据（来自 earnings_calendar 表，每日 18:00 自动更新）

    - **scope**: watchlist=自选股(target_tracking_config) / theme=主题关联公司 / us=全部美股 / hk=全部港股
    - **reported**: 已公布的财报带 EPS 实际值与惊喜率，未公布只带预期值
    """
    try:
        info(f"获取财报日历数据请求: user={current_user.username}, scope={scope}, from={from_date}, to={to_date}", source="markets_api")

        # 默认窗口: 过去7天 ~ 未来14天
        if not from_date:
            from_date = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not to_date:
            to_date = (datetime.utcnow() + timedelta(days=14)).strftime('%Y-%m-%d')

        service = get_earnings_calendar_service()
        result = service.get_earnings_calendar_view(
            from_date=from_date,
            to_date=to_date,
            scope=scope,
            before_after=before_after,
            search=search,
            reported=reported,
            limit=limit,
            offset=offset,
            min_market_cap=min_market_cap
        )

        # Logo 相对路径补全为 EODHD 完整 URL
        for item in result.get('items', []):
            item['logo_url'] = process_logo_url(item.get('logo_url'))

        return result

    except Exception as e:
        error(f"获取财报日历数据失败: {str(e)}", source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取财报日历数据失败: {str(e)}"
        )

@router.get("/earnings-calendar/month-summary", response_model=EarningsCalendarMonthSummaryResponse, summary="获取财报日历月度汇总")
async def get_earnings_calendar_month_summary(
    month: Optional[str] = Query(None, description="月份 (YYYY-MM)，默认当月"),
    scope: str = Query('watchlist', pattern='^(watchlist|theme|us|hk)$', description="范围: watchlist=自选 / theme=主题关联公司 / us=全部美股 / hk=全部港股"),
    search: Optional[str] = Query(None, max_length=50, description="模糊搜索 ticker 或公司名"),
    top_n: int = Query(5, ge=1, le=50, description="每日返回的市值 Top N 公司数"),
    min_market_cap: Optional[float] = Query(None, description="最小市值筛选(USD 美元, 港股已按汇率换算)"),
    current_user=Depends(get_current_user)
):
    """
    获取财报日历月度汇总（Month View 用）：逐日财报家数 + 市值 Top N 公司。
    市值优先取 EODHD company_highlights，缺失时回退 finviz_screener。
    """
    try:
        info(f"获取财报月度汇总请求: user={current_user.username}, month={month}, scope={scope}", source="markets_api")

        if not month:
            month = datetime.utcnow().strftime('%Y-%m')

        service = get_earnings_calendar_service()
        result = service.get_earnings_month_summary(
            month=month,
            scope=scope,
            search=search,
            top_n=top_n,
            min_market_cap=min_market_cap
        )

        for day in result.get('days', []):
            for item in day.get('top', []):
                item['logo_url'] = process_logo_url(item.get('logo_url'))

        return result

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        error(f"获取财报月度汇总失败: {str(e)}", source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取财报月度汇总失败: {str(e)}"
        )

@router.get("/earnings-calendar/company-panel", summary="获取财报日历 Ticker 财务信息面板")
async def get_earnings_calendar_company_panel(
    code: str = Query(..., min_length=1, max_length=20, description="股票代码(美股裸代码/港股带.HK后缀)"),
    current_user=Depends(get_current_user)
):
    """
    获取 Ticker 财务信息面板（财报日历右侧详情用）:
    公司信息 / 关键指标 / 下次财报 / 最近 12 季 EPS 历史 / 一致预期 / 评级 / 技术指标 / 实时价格。
    """
    try:
        info(f"获取公司面板请求: user={current_user.username}, code={code}", source="markets_api")

        service = get_earnings_calendar_service()
        result = service.get_company_panel(code)

        if result.get('company'):
            result['company']['logo_url'] = process_logo_url(result['company'].get('logo_url'))

        return result

    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        error(f"获取公司面板失败: {str(e)}", source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取公司面板失败: {str(e)}"
        )

@router.get("/earnings-calendar/feed", summary="获取 Earnings Feed（右栏默认事件流）")
async def get_earnings_calendar_feed(
    from_date: Optional[str] = Query(None, description="开始日期 (YYYY-MM-DD)，默认今天-7天"),
    to_date: Optional[str] = Query(None, description="结束日期 (YYYY-MM-DD)，默认今天+14天"),
    scope: str = Query('watchlist', pattern='^(watchlist|theme|us|hk)$', description="范围: watchlist=自选 / theme=主题关联公司 / us=全部美股 / hk=全部港股"),
    search: Optional[str] = Query(None, max_length=50, description="模糊搜索 ticker 或公司名"),
    min_market_cap: Optional[float] = Query(None, description="最小市值筛选(USD 美元, 港股已按汇率换算)"),
    limit: int = Query(100, ge=1, le=300, description="返回条数上限 (1-300)"),
    current_user=Depends(get_current_user)
):
    """
    Earnings Feed: 当前窗口财报事件流（已公布在前、未公布在后），
    每条附实时价格(stock_prices_realtime)与最近季度营收(company_income_statement)。
    """
    try:
        info(f"获取 Earnings Feed 请求: user={current_user.username}, scope={scope}", source="markets_api")

        if not from_date:
            from_date = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not to_date:
            to_date = (datetime.utcnow() + timedelta(days=14)).strftime('%Y-%m-%d')

        service = get_earnings_calendar_service()
        result = service.get_earnings_feed(
            from_date=from_date,
            to_date=to_date,
            scope=scope,
            min_market_cap=min_market_cap,
            search=search,
            limit=limit
        )

        for item in result.get('items', []):
            item['logo_url'] = process_logo_url(item.get('logo_url'))

        return result

    except Exception as e:
        error(f"获取 Earnings Feed 失败: {str(e)}", source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取 Earnings Feed 失败: {str(e)}"
        )

@router.post("/earnings-calendar/company-recap", summary="生成/获取财报电话会 AI Recap")
async def generate_earnings_company_recap(
    code: str = Query(..., min_length=1, max_length=20, description="股票代码(美股裸代码)"),
    current_user=Depends(get_current_user)
):
    """
    按需生成某 ticker 最新一期财报电话会的 AI Recap:
    Motley Fool(免费网页) → AlphaVantage(兜底) → 中央 LLM(qwen 等) 生成 Markdown 回顾,
    结果永久缓存于 earnings_call_recaps, 同公司同季只抓取一次。
    全流程约 10-30 秒（主要为 LLM 生成耗时）。
    """
    try:
        info(f"生成公司 Recap 请求: user={current_user.username}, code={code}", source="markets_api")

        from backend.services.earnings_recap_service import EarningsRecapService
        registry = get_service_registry()
        service = registry.get_or_create_service("earnings_recap_service", EarningsRecapService)

        result = await service.get_or_generate(code)
        return result

    except Exception as e:
        error(f"生成公司 Recap 失败: {str(e)}", source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"生成公司 Recap 失败: {str(e)}"
        )

@router.get("/indicators", summary="获取可用指标列表")
async def get_available_indicators(
    country: Optional[str] = Query(None, description="国家代码，获取该国家的可用指标"),
    current_user=Depends(get_current_user)
):
    """
    获取可用的宏观指标列表

    - **country**: 可选，获取指定国家的可用指标
    """
    try:
        # 初始化API客户端
        macro_service = get_macro_indicators_service()

        if country:
            # 获取指定国家的可用指标
            indicators = macro_service.get_available_indicators_for_country(country)
            return {
                "country": country,
                "indicators": indicators
            }
        else:
            # 获取所有指标定义
            all_indicators = macro_service.list_all_indicators()
            indicators = [
                {
                    "indicator": code,
                    "indicator_name": name,
                    "data_points": 0,
                    "earliest_year": 0,
                    "latest_year": 0
                }
                for code, name in all_indicators.items()
            ]
            return {
                "country": None,
                "indicators": indicators
            }
        
    except Exception as e:
        error(f"获取指标列表失败: {str(e)}", source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取指标列表失败: {str(e)}"
        )

@router.get("/supported-countries", summary="获取支持的国家代码")
async def get_supported_countries(
    current_user=Depends(get_current_user)
):
    """
    获取API支持的国家代码列表
    """
    try:
        # 经济事件API支持的国家
        events_service = get_economic_events_service()
        event_countries = events_service.get_supported_countries()
        
        # 宏观指标API支持的国家
        macro_service = get_macro_indicators_service()
        macro_countries = macro_service.list_common_countries()
        
        return {
            "economic_events": event_countries,
            "macro_indicators": macro_countries
        }
        
    except Exception as e:
        error(f"获取支持的国家代码失败: {str(e)}", source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取支持的国家代码失败: {str(e)}"
        )

@router.post("/economic-events/fetch", summary="获取经济事件数据")
async def fetch_economic_events(
    from_date: Optional[str] = Query(None, description="开始日期 (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="结束日期 (YYYY-MM-DD)"),
    country: Optional[str] = Query(None, description="国家代码"),
    limit: int = Query(100, ge=1, le=1000, description="获取数量限制"),
    current_user=Depends(get_current_user)
):
    """
    从EODHD API获取经济事件数据并存储到数据库
    
    - **from_date**: 开始日期，格式 YYYY-MM-DD
    - **to_date**: 结束日期，格式 YYYY-MM-DD  
    - **country**: 国家代码 (ISO 3166)
    - **limit**: 获取数量限制 (1-1000)
    """
    try:
        if not current_user.is_admin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="需要管理员权限"
            )
        
        info(f"获取经济事件数据请求: user={current_user.username}, from_date={from_date}, to_date={to_date}, country={country}", source="markets_api")
        
        # 获取经济事件服务
        events_service = get_economic_events_service()
        market_service = get_market_data_service()
        
        # 从API获取数据
        events_data = events_service.get_economic_events(
            from_date=from_date,
            to_date=to_date,
            country=country,
            limit=limit
        )
        
        if not events_data:
            return {
                "message": "未获取到经济事件数据",
                "total_fetched": 0,
                "new_records": 0,
                "updated_records": 0
            }
        
        # 存储数据并计算impact
        total_fetched = len(events_data)
        new_records = 0
        updated_records = 0
        failed_records = 0
        
        for event_data in events_data:
            # 格式化事件数据
            formatted_event = events_service.format_event_data(event_data)
            
            # 计算impact值
            impact = calculate_event_impact(formatted_event['event'])
            formatted_event['impact'] = impact
            
            # 存储到数据库
            upsert_result = market_service.insert_economic_event(formatted_event)
            if upsert_result.get('inserted'):
                new_records += 1
            elif upsert_result.get('id'):
                updated_records += 1
            else:
                failed_records += 1
        
        info(f"经济事件数据获取完成: 获取{total_fetched}条，新增{new_records}条，更新{updated_records}条，失败{failed_records}条", source="markets_api")
        
        return {
            "message": f"经济事件数据获取完成",
            "total_fetched": total_fetched,
            "new_records": new_records,
            "updated_records": updated_records,
            "failed_records": failed_records
        }
        
    except Exception as e:
        error(f"获取经济事件数据失败: {str(e)}", source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取经济事件数据失败: {str(e)}"
        )

@router.delete("/economic-events/cleanup", summary="清理旧的经济事件数据")
async def cleanup_old_economic_events(
    days: int = Query(365, ge=30, le=3650, description="保留天数"),
    current_user=Depends(get_current_user)
):
    """
    清理指定天数之前的旧经济事件数据
    
    - **days**: 保留天数 (30-3650)
    """
    try:
        if not current_user.is_admin:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="需要管理员权限"
            )
        
        info(f"清理旧经济事件数据: user={current_user.username}, days={days}", source="markets_api")
        
        market_service = get_market_data_service()
        deleted_count = market_service.delete_old_events(days)
        
        info(f"清理完成，删除了 {deleted_count} 条旧数据", source="markets_api")
        
        return {
            "message": f"清理完成，删除了 {deleted_count} 条旧数据",
            "deleted_count": deleted_count,
            "retention_days": days
        }
        
    except HTTPException:
        raise
    except Exception as e:
        error(f"清理旧数据失败: {str(e)}", source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"清理旧数据失败: {str(e)}"
        )

@router.get("/macro-indicators/export", summary="导出宏观指标数据为CSV")
async def export_macro_indicators_csv(
    country: Optional[Union[str, List[str]]] = Query(None, description="国家代码，支持单个或多个国家"),
    indicator: Optional[Union[str, List[str]]] = Query(None, description="指标代码，支持单个或多个指标"),
    year_from: Optional[int] = Query(None, description="起始年份"),
    year_to: Optional[int] = Query(None, description="结束年份"),
    current_user=Depends(get_current_user)
):
    """
    导出宏观指标数据为CSV格式
    
    - **country**: 国家代码 (ISO 3166 Alpha-3)，支持单个或多个国家，多个国家用逗号分隔
    - **indicator**: 指标代码，支持单个或多个指标，多个指标用逗号分隔
    - **year_from**: 起始年份
    - **year_to**: 结束年份
    
    如果所有参数都为空，则导出整个宏观指标表的数据
    """
    try:
        info(f"导出宏观指标CSV请求: user={current_user.username}, country={country}, indicator={indicator}, year_from={year_from}, year_to={year_to}", source="markets_api")
        
        # 处理多选参数
        processed_country = None
        processed_indicator = None
        
        if country:
            if isinstance(country, str):
                # 如果是字符串，按逗号分割
                processed_country = [c.strip() for c in country.split(',') if c.strip()]
            elif isinstance(country, list):
                # 如果是列表，需要处理每个元素可能包含逗号的情况
                processed_country = []
                for item in country:
                    if isinstance(item, str):
                        processed_country.extend([c.strip() for c in item.split(',') if c.strip()])
                    else:
                        processed_country.append(str(item))
            else:
                processed_country = [str(country)]
        
        if indicator:
            if isinstance(indicator, str):
                # 如果是字符串，按逗号分割
                processed_indicator = [i.strip() for i in indicator.split(',') if i.strip()]
            elif isinstance(indicator, list):
                # 如果是列表，需要处理每个元素可能包含逗号的情况
                processed_indicator = []
                for item in indicator:
                    if isinstance(item, str):
                        processed_indicator.extend([i.strip() for i in item.split(',') if i.strip()])
                    else:
                        processed_indicator.append(str(item))
            else:
                processed_indicator = [str(indicator)]
        
        # 初始化API客户端
        macro_service = get_macro_indicators_service()
        
        # 从数据库获取数据
        data = macro_service.get_stored_data(
            country=processed_country,
            indicator=processed_indicator,
            year_from=year_from,
            year_to=year_to,
            limit=50000  # 导出时允许更大的数据量
        )
        
        info(f"获取到 {len(data)} 条宏观指标数据用于导出", source="markets_api")
        
        # 生成CSV内容
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # 写入CSV头部
        headers = [
            "Country Code", "Country Name", "Indicator", "Indicator Name", 
            "Year", "Value", "Date String", "Fetch Timestamp"
        ]
        writer.writerow(headers)
        
        # 写入数据行
        for item in data:
            row = [
                item.get("country_code", ""),
                item.get("country_name", ""),
                item.get("indicator", ""),
                item.get("indicator_name", ""),
                item.get("year", ""),
                item.get("value", ""),
                item.get("date_str", ""),
                item.get("fetch_timestamp", "")
            ]
            writer.writerow(row)
        
        csv_content = output.getvalue()
        output.close()
        
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"macro_indicators_{timestamp}.csv"
        
        # 如果有筛选条件，在文件名中体现
        if processed_country or processed_indicator or year_from or year_to:
            conditions = []
            if processed_country:
                conditions.append(f"countries_{len(processed_country)}")
            if processed_indicator:
                conditions.append(f"indicators_{len(processed_indicator)}")
            if year_from and year_to:
                conditions.append(f"years_{year_from}-{year_to}")
            elif year_from:
                conditions.append(f"from_{year_from}")
            elif year_to:
                conditions.append(f"to_{year_to}")
            
            if conditions:
                filename = f"macro_indicators_{'_'.join(conditions)}_{timestamp}.csv"
        
        info(f"成功生成CSV文件: {filename}", 
             record_count=len(data),
             user=current_user.username,
             source="markets_api")
        
        from fastapi.responses import StreamingResponse
        
        return StreamingResponse(
            io.StringIO(csv_content),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "text/csv; charset=utf-8"
            }
        )
        
    except Exception as e:
        error(f"导出宏观指标CSV失败: {str(e)}", source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"导出宏观指标CSV失败: {str(e)}"
        )


# 基本面信息查询相关的数据模型
class CompanySearchRequest(BaseModel):
    query: str = Field(..., description="股票代码或公司名称")

class CompanyFundamentalsResponse(BaseModel):
    company: Dict[str, Any]
    highlights: Optional[Dict[str, Any]] = None
    balance_sheet: List[Dict[str, Any]] = []
    income_statement: List[Dict[str, Any]] = []
    cash_flow: List[Dict[str, Any]] = []
    valuations: Optional[Dict[str, Any]] = None
    analyst_ratings: List[Dict[str, Any]] = []
    dividend_info: List[Dict[str, Any]] = []
    dividend_by_year: List[Dict[str, Any]] = []
    insider_transactions: List[Dict[str, Any]] = []
    technicals: Optional[Dict[str, Any]] = None
    earnings_annual: List[Dict[str, Any]] = []
    earnings_history: List[Dict[str, Any]] = []
    earnings_trend: List[Dict[str, Any]] = []
    institutional_holdings: List[Dict[str, Any]] = []
    officers: List[Dict[str, Any]] = []
    outstanding_shares: List[Dict[str, Any]] = []


@router.post("/company/search", response_model=CompanyFundamentalsResponse)
async def search_company_fundamentals(
    request: CompanySearchRequest,
    current_user=Depends(get_current_user)
):
    """
    查询公司基本面信息
    
    支持通过股票代码或公司名称搜索，返回完整的基本面数据
    """
    try:
        from backend.database.db_manager import DatabaseManager
        
        info(f"开始查询公司基本面信息", 
             query=request.query,
             user=current_user.username,
             source="markets_api")
        
        # 获取数据库连接参数
        from backend.utils.database_config import get_database_config
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 查询公司基本信息
        query_sql = """
        SELECT * FROM companies 
        WHERE UPPER(code) = UPPER(%s) 
           OR UPPER(primaryticker) = UPPER(%s) 
           OR UPPER(name) ILIKE %s
        ORDER BY 
            CASE 
                WHEN UPPER(code) = UPPER(%s) THEN 1
                WHEN UPPER(primaryticker) = UPPER(%s) THEN 2
                ELSE 3
            END
        LIMIT 1
        """
        
        search_pattern = f"%{request.query}%"
        # 获取数据库连接并使用DictCursor
        with db_manager.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(query_sql, (request.query, request.query, search_pattern, request.query, request.query))
            company_result = cursor.fetchall()
        
        if not company_result:
            # 提供更友好的错误信息，建议用户启用自动更新功能
            error_detail = f"数据库中未找到 {request.query} 的数据。建议启用自动更新功能以从EODHD获取最新数据。"
            
            info(f"搜索失败 - 数据库中未找到公司",
                 query=request.query,
                 user=current_user.username,
                 source="markets_api")
            
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=error_detail
            )
        
        company = dict(company_result[0])
        company_id = company['id']
        
        # 使用同一个连接执行所有查询
        with db_manager.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            # 查询highlights (最新数据)
            highlights_sql = """
            SELECT * FROM company_highlights 
            WHERE company_id = %s 
            ORDER BY data_date DESC 
            LIMIT 1
            """
            cursor.execute(highlights_sql, (company_id,))
            highlights_result = cursor.fetchall()
            highlights = dict(highlights_result[0]) if highlights_result else None
            
            # 查询资产负债表 (最近4期数据，包含季度和年度)
            balance_sheet_sql = """
            SELECT * FROM company_balance_sheet 
            WHERE company_id = %s 
            ORDER BY date DESC 
            LIMIT 20
            """
            cursor.execute(balance_sheet_sql, (company_id,))
            balance_sheet_raw = [dict(row) for row in cursor.fetchall()]
            
            # 查询利润表 (最近4期数据，包含季度和年度)
            income_statement_sql = """
            SELECT * FROM company_income_statement 
            WHERE company_id = %s 
            ORDER BY date DESC 
            LIMIT 20
            """
            cursor.execute(income_statement_sql, (company_id,))
            income_statement_raw = [dict(row) for row in cursor.fetchall()]
            
            # 查询现金流量表 (最近4期数据，包含季度和年度)
            cash_flow_sql = """
            SELECT * FROM company_cash_flow 
            WHERE company_id = %s 
            ORDER BY date DESC 
            LIMIT 20
            """
            cursor.execute(cash_flow_sql, (company_id,))
            cash_flow_raw = [dict(row) for row in cursor.fetchall()]
            
            # 按期间类型分组数据
            def group_by_period_type(data_list):
                quarterly = []
                annual = []
                for item in data_list:
                    period_type = item.get('period_type', '').lower()
                    if period_type in ['quarterly', 'q', 'quarter']:
                        quarterly.append(item)
                    elif period_type in ['annual', 'a', 'year', 'yearly']:
                        annual.append(item)
                return quarterly, annual
            
            # 分组财务数据
            balance_sheet_q, balance_sheet_a = group_by_period_type(balance_sheet_raw)
            income_statement_q, income_statement_a = group_by_period_type(income_statement_raw)
            cash_flow_q, cash_flow_a = group_by_period_type(cash_flow_raw)
            
            # 返回包含两种期间类型的数据，但保持Pydantic模型兼容性
            # 将季度和年度数据合并，并添加period_type标识
            balance_sheet = []
            income_statement = []
            cash_flow = []
            
            # 添加季度数据
            for item in balance_sheet_q[:4]:
                item['period_type'] = 'quarterly'
                balance_sheet.append(item)
            for item in income_statement_q[:4]:
                item['period_type'] = 'quarterly'
                income_statement.append(item)
            for item in cash_flow_q[:4]:
                item['period_type'] = 'quarterly'
                cash_flow.append(item)
            
            # 添加年度数据
            for item in balance_sheet_a[:4]:
                item['period_type'] = 'annual'
                balance_sheet.append(item)
            for item in income_statement_a[:4]:
                item['period_type'] = 'annual'
                income_statement.append(item)
            for item in cash_flow_a[:4]:
                item['period_type'] = 'annual'
                cash_flow.append(item)
            
            # 按日期排序（最新的在前）
            balance_sheet.sort(key=lambda x: x['date'], reverse=True)
            income_statement.sort(key=lambda x: x['date'], reverse=True)
            cash_flow.sort(key=lambda x: x['date'], reverse=True)
            
            # 查询估值信息 (最新数据)
            valuations_sql = """
            SELECT * FROM company_valuations 
            WHERE company_id = %s 
            ORDER BY data_date DESC 
            LIMIT 1
            """
            cursor.execute(valuations_sql, (company_id,))
            valuations_result = cursor.fetchall()
            valuations = dict(valuations_result[0]) if valuations_result else None
            
            # 查询分析师评级 (最近10条)
            analyst_ratings_sql = """
            SELECT 
                id,
                company_id,
                rating,
                targetprice as target_price,
                strongbuy,
                buy,
                hold,
                sell,
                strongsell,
                data_date as rating_date,
                created_at,
                updated_at
            FROM company_analyst_ratings 
            WHERE company_id = %s 
            ORDER BY created_at DESC 
            LIMIT 10
            """
            cursor.execute(analyst_ratings_sql, (company_id,))
            analyst_ratings_raw = cursor.fetchall()
            
            # 转换数据格式以匹配前端期望
            analyst_ratings = []
            for row in analyst_ratings_raw:
                rating_data = dict(row)
                
                # 获取所有评级分布数据
                strong_buy = rating_data.get('strongbuy', 0) or 0
                buy_count = rating_data.get('buy', 0) or 0
                hold_count = rating_data.get('hold', 0) or 0
                sell_count = rating_data.get('sell', 0) or 0
                strong_sell = rating_data.get('strongsell', 0) or 0
                total_analysts = strong_buy + buy_count + hold_count + sell_count + strong_sell
                
                # 根据数值评级生成文本评级
                rating_value = rating_data.get('rating')
                if rating_value is not None:
                    if rating_value >= 4.5:
                        rating_text = "Strong Buy"
                    elif rating_value >= 3.5:
                        rating_text = "Buy"
                    elif rating_value >= 2.5:
                        rating_text = "Hold"
                    elif rating_value >= 1.5:
                        rating_text = "Sell"
                    else:
                        rating_text = "Strong Sell"
                else:
                    rating_text = "N/A"
                
                # 计算评级分布百分比
                def calculate_percentage(count, total):
                    return round((count / total * 100), 1) if total > 0 else 0
                
                # 生成详细的分析师信息
                if total_analysts > 0:
                    analyst_name = f"Consensus ({total_analysts} analysts)"
                    rating_breakdown = {
                        'strong_buy': {
                            'count': strong_buy,
                            'percentage': calculate_percentage(strong_buy, total_analysts)
                        },
                        'buy': {
                            'count': buy_count,
                            'percentage': calculate_percentage(buy_count, total_analysts)
                        },
                        'hold': {
                            'count': hold_count,
                            'percentage': calculate_percentage(hold_count, total_analysts)
                        },
                        'sell': {
                            'count': sell_count,
                            'percentage': calculate_percentage(sell_count, total_analysts)
                        },
                        'strong_sell': {
                            'count': strong_sell,
                            'percentage': calculate_percentage(strong_sell, total_analysts)
                        }
                    }
                else:
                    analyst_name = "Consensus"
                    rating_breakdown = {
                        'strong_buy': {'count': 0, 'percentage': 0},
                        'buy': {'count': 0, 'percentage': 0},
                        'hold': {'count': 0, 'percentage': 0},
                        'sell': {'count': 0, 'percentage': 0},
                        'strong_sell': {'count': 0, 'percentage': 0}
                    }
                
                # 生成券商名称
                brokerage = "Multiple Brokers"
                
                analyst_ratings.append({
                    'id': rating_data['id'],
                    'company_id': rating_data['company_id'],
                    'rating': rating_text,
                    'rating_value': rating_value,
                    'target_price': rating_data.get('target_price'),
                    'analyst_name': analyst_name,
                    'brokerage': brokerage,
                    'rating_date': rating_data.get('rating_date'),
                    'created_at': rating_data['created_at'],
                    'updated_at': rating_data.get('updated_at'),
                    'total_analysts': total_analysts,
                    'rating_breakdown': rating_breakdown
                })
            
            # 查询股息信息 (最近5年)
            dividend_sql = """
            SELECT * FROM company_dividend_info 
            WHERE company_id = %s 
            ORDER BY exdividenddate DESC 
            LIMIT 10
            """
            cursor.execute(dividend_sql, (company_id,))
            dividend_info = [dict(row) for row in cursor.fetchall()]
            
            # 查询年度分红统计 (最近50年)
            dividend_by_year_sql = """
            SELECT * FROM company_dividend_by_year 
            WHERE company_id = %s 
            ORDER BY year DESC 
            LIMIT 50
            """
            cursor.execute(dividend_by_year_sql, (company_id,))
            dividend_by_year = [dict(row) for row in cursor.fetchall()]
            
            # 查询内部交易 (最近100条)
            insider_sql = """
            SELECT * FROM company_insider_transactions 
            WHERE company_id = %s 
            ORDER BY transactiondate DESC 
            LIMIT 100
            """
            cursor.execute(insider_sql, (company_id,))
            insider_transactions = [dict(row) for row in cursor.fetchall()]
            
            # 查询技术指标 (最新数据)
            technicals_sql = """
            SELECT * FROM company_technicals 
            WHERE company_id = %s 
            ORDER BY data_date DESC 
            LIMIT 1
            """
            cursor.execute(technicals_sql, (company_id,))
            technicals_result = cursor.fetchall()
            technicals = dict(technicals_result[0]) if technicals_result else None
            
            # 查询年度盈利数据 (最近100年)
            earnings_annual_sql = """
            SELECT * FROM company_earnings_annual 
            WHERE company_id = %s 
            ORDER BY date DESC 
            LIMIT 100
            """
            cursor.execute(earnings_annual_sql, (company_id,))
            earnings_annual = [dict(row) for row in cursor.fetchall()]
            
            # 查询盈利历史数据 (最近100条)
            earnings_history_sql = """
            SELECT * FROM company_earnings_history 
            WHERE company_id = %s 
            ORDER BY date DESC 
            LIMIT 100
            """
            cursor.execute(earnings_history_sql, (company_id,))
            earnings_history = [dict(row) for row in cursor.fetchall()]
            
            # 查询盈利趋势数据 (最近100条)
            earnings_trend_sql = """
            SELECT * FROM company_earnings_trend 
            WHERE company_id = %s 
            ORDER BY date DESC 
            LIMIT 100
            """
            cursor.execute(earnings_trend_sql, (company_id,))
            earnings_trend = [dict(row) for row in cursor.fetchall()]
            
            # 查询股东信息 (最近100条)
            institutional_holdings_sql = """
            SELECT 
                id,
                company_id,
                holder_type,
                name,
                date,
                totalshares,
                totalassets,
                currentshares,
                change,
                change_p,
                created_at,
                updated_at
            FROM company_institutional_holdings 
            WHERE company_id = %s 
            ORDER BY date DESC, currentshares DESC 
            LIMIT 100
            """
            cursor.execute(institutional_holdings_sql, (company_id,))
            institutional_holdings = [dict(row) for row in cursor.fetchall()]
            
            # 查询高管信息
            officers_sql = """
            SELECT 
                id,
                company_id,
                name,
                title,
                yearborn,
                sort_order,
                is_active,
                created_at,
                updated_at
            FROM company_officers 
            WHERE company_id = %s 
            ORDER BY sort_order ASC, name ASC
            """
            cursor.execute(officers_sql, (company_id,))
            officers = [dict(row) for row in cursor.fetchall()]
            
            # 查询流通股数据 (最近50条)
            outstanding_shares_sql = """
            SELECT 
                id,
                company_id,
                date,
                dateformatted,
                sharesmln,
                shares,
                type,
                created_at,
                updated_at
            FROM company_outstanding_shares 
            WHERE company_id = %s 
            ORDER BY dateformatted DESC 
            LIMIT 50
            """
            cursor.execute(outstanding_shares_sql, (company_id,))
            outstanding_shares = [dict(row) for row in cursor.fetchall()]
        
        # 处理company中的logo URL
        if 'logourl' in company:
            company['logourl'] = process_logo_url(company['logourl'])
        
        # 构建响应
        response = CompanyFundamentalsResponse(
            company=company,
            highlights=highlights,
            balance_sheet=balance_sheet or [],
            income_statement=income_statement or [],
            cash_flow=cash_flow or [],
            valuations=valuations,
            analyst_ratings=analyst_ratings or [],
            dividend_info=dividend_info or [],
            dividend_by_year=dividend_by_year or [],
            insider_transactions=insider_transactions or [],
            technicals=technicals,
            earnings_annual=earnings_annual or [],
            earnings_history=earnings_history or [],
            earnings_trend=earnings_trend or [],
            institutional_holdings=institutional_holdings or [],
            officers=officers or [],
            outstanding_shares=outstanding_shares or []
        )
        
        info(f"成功查询公司基本面信息", 
             company_name=company.get('name'),
             company_code=company.get('code'),
             data_points={
                 'highlights': 1 if highlights else 0,
                 'balance_sheet': len(balance_sheet or []),
                 'income_statement': len(income_statement or []),
                 'cash_flow': len(cash_flow or []),
                 'valuations': 1 if valuations else 0,
                 'analyst_ratings': len(analyst_ratings or []),
                 'dividend_info': len(dividend_info or []),
                 'dividend_by_year': len(dividend_by_year or []),
                 'insider_transactions': len(insider_transactions or []),
                 'earnings_annual': len(earnings_annual or []),
                 'earnings_history': len(earnings_history or []),
                 'earnings_trend': len(earnings_trend or []),
                 'institutional_holdings': len(institutional_holdings or []),
                 'outstanding_shares': len(outstanding_shares or [])
             },
             user=current_user.username,
             source="markets_api")
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        error(f"查询公司基本面信息失败: {str(e)}", 
              query=request.query,
              source="markets_api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"查询公司基本面信息失败: {str(e)}"
        )


class CompanyFundamentalsUpdateRequest(BaseModel):
    """公司基本面数据更新请求模型"""
    query: str = Field(..., description="股票代码或公司名称")
    force_update: bool = Field(default=False, description="是否强制更新，即使数据已是最新")


class CompanyFundamentalsUpdateResponse(BaseModel):
    """公司基本面数据更新响应模型"""
    success: bool
    message: str
    company_info: Optional[Dict[str, Any]] = None
    data_updated: Dict[str, bool] = {}
    execution_time: Optional[float] = None


@router.post("/company/update-fundamentals", response_model=CompanyFundamentalsUpdateResponse)
async def update_company_fundamentals(
    request: CompanyFundamentalsUpdateRequest,
    current_user=Depends(get_current_user)
):
    """
    获取或更新单个公司的fundamentals数据
    
    参考EODHD Fundamental调度器的实现，为指定公司获取和更新所有基本面数据，包括：
    - General: 公司基本信息
    - Financials: 财务报表数据 
    - Highlights: 关键财务指标
    - Valuation: 估值数据
    - Technicals: 技术指标
    - Holders: 股东数据
    - outstandingShares: 流通股数据
    - Earnings: 盈利数据
    - SharesStats: 股票统计
    - AnalystRatings: 分析师评级
    - SplitsDividends: 拆股分红
    - InsiderTransactions: 内部人交易
    """
    import time
    start_time = time.time()
    
    try:
        from backend.database.db_manager import DatabaseManager
        from backend.services.eodhd_fundamentals_service import EODHDFundamentalsService
        
        info(f"开始更新公司基本面数据", 
             query=request.query,
             force_update=request.force_update,
             user=current_user.username,
             source="markets_api")
        
        # 获取数据库连接参数
        from backend.utils.database_config import get_database_config
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 首先查找公司信息
        query_sql = """
        SELECT code, name, exchange, primaryticker, id 
        FROM companies 
        WHERE UPPER(code) = UPPER(%s) 
           OR UPPER(primaryticker) = UPPER(%s) 
           OR UPPER(name) ILIKE %s
        ORDER BY 
            CASE 
                WHEN UPPER(code) = UPPER(%s) THEN 1
                WHEN UPPER(primaryticker) = UPPER(%s) THEN 2
                ELSE 3
            END
        LIMIT 1
        """
        
        search_pattern = f"%{request.query}%"
        with db_manager.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(query_sql, (request.query, request.query, search_pattern, request.query, request.query))
            company_result = cursor.fetchall()
        
        # 如果在数据库中未找到公司，尝试从EODHD直接获取数据
        if not company_result:
            info(f"数据库中未找到公司: {request.query}，尝试从EODHD获取数据",
                 user=current_user.username)
            
            # 解析ticker格式，提取symbol和exchange
            from backend.config.exchange_config import format_ticker_for_eodhd
            
            # 格式化ticker为EODHD格式（如果用户输入TLN.US，会被格式化为TLN.US）
            formatted_ticker = format_ticker_for_eodhd(request.query.strip())
            
            # 分离symbol和exchange
            if '.' in formatted_ticker:
                symbol, exchange = formatted_ticker.rsplit('.', 1)
            else:
                # 如果没有交易所后缀，默认使用US
                symbol = formatted_ticker
                exchange = 'US'
            
            info(f"尝试从EODHD获取ticker: {symbol}.{exchange}",
                 user=current_user.username)
            
            # 初始化EODHD基本面数据服务
            fundamentals_service = EODHDFundamentalsService()
            
            # 尝试从EODHD获取基本面数据
            full_data = fundamentals_service.fetch_company_full_data(symbol, exchange)
            
            if not full_data:
                return CompanyFundamentalsUpdateResponse(
                    success=False,
                    message=f"未找到公司: {request.query}。数据库和EODHD均无此ticker的数据，请确认ticker是否正确。",
                    execution_time=time.time() - start_time
                )
            
            info(f"成功从EODHD获取 {symbol}.{exchange} 的数据，开始保存到数据库",
                 user=current_user.username)
            
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
            
            # 保存数据到数据库
            save_result = fundamentals_service.batch_save_companies_data([company_data])
            
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
            
            # 获取保存后的公司信息
            general_data = full_data.get('General', full_data)
            company_name = general_data.get('Name', symbol)
            
            # 构建成功消息
            updated_components = [k for k, v in data_updated.items() if v]
            success_message = f"成功从EODHD获取并保存新公司 {company_name} ({symbol}.{exchange}) 的基本面数据"
            if updated_components:
                success_message += f"，保存了 {len(updated_components)} 个数据组件: {', '.join(updated_components)}"
            
            info(f"完成新公司基本面数据获取和保存",
                 symbol=f"{symbol}.{exchange}",
                 company_name=company_name,
                 execution_time=execution_time,
                 data_updated=data_updated,
                 user=current_user.username)
            
            return CompanyFundamentalsUpdateResponse(
                success=True,
                message=success_message,
                company_info={
                    'code': symbol,
                    'exchange': exchange,
                    'name': company_name,
                    'primaryticker': f"{symbol}.{exchange}"
                },
                data_updated=data_updated,
                execution_time=execution_time
            )
        
        company = dict(company_result[0])
        symbol = company['code'] or company['primaryticker']
        exchange = company['exchange']
        company_name = company['name']
        
        info(f"找到公司", 
             symbol=symbol,
             exchange=exchange,
             company_name=company_name,
             user=current_user.username)
        
        # 检查是否需要更新（如果不是强制更新，检查数据新鲜度）
        if not request.force_update:
            # 检查最后更新时间
            with db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT MAX(updated_at) as last_update 
                    FROM companies 
                    WHERE id = %s AND updated_at > NOW() - INTERVAL '1 day'
                """, (company['id'],))
                recent_update = cursor.fetchone()
                
                if recent_update and recent_update[0]:
                    info(f"公司数据较新，跳过更新", 
                         symbol=symbol,
                         last_update=recent_update[0],
                         user=current_user.username)
                    
                    return CompanyFundamentalsUpdateResponse(
                        success=True,
                        message=f" {company_name} ({symbol})Lately updated: {recent_update[0]}",
                        company_info=company,
                        execution_time=time.time() - start_time
                    )
        
        # 初始化EODHD基本面数据服务
        fundamentals_service = EODHDFundamentalsService()
        
        # 获取完整的基本面数据
        info(f"开始从EODHD获取 {symbol} 的完整基本面数据", user=current_user.username)
        
        full_data = fundamentals_service.fetch_company_full_data(symbol, exchange)
        
        if not full_data:
            return CompanyFundamentalsUpdateResponse(
                success=False,
                message=f"无法从EODHD获取公司 {company_name} ({symbol}) 的基本面数据",
                company_info=company,
                execution_time=time.time() - start_time
            )
        
        # 保存数据到数据库
        info(f"开始保存 {symbol} 的基本面数据到数据库", user=current_user.username)
        
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
        
        # 使用批量保存方法
        save_result = fundamentals_service.batch_save_companies_data([company_data])
        
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
        
        # 构建成功消息
        updated_components = [k for k, v in data_updated.items() if v]
        success_message = f"成功更新公司 {company_name} ({symbol}) 的基本面数据"
        if updated_components:
            success_message += f"，更新了 {len(updated_components)} 个数据组件: {', '.join(updated_components)}"
        
        info(f"完成公司基本面数据更新", 
             symbol=symbol,
             company_name=company_name,
             execution_time=execution_time,
             data_updated=data_updated,
             user=current_user.username)
        
        return CompanyFundamentalsUpdateResponse(
            success=True,
            message=success_message,
            company_info=company,
            data_updated=data_updated,
            execution_time=execution_time
        )
        
    except Exception as e:
        execution_time = time.time() - start_time
        error(f"更新公司基本面数据失败: {str(e)}", 
              query=request.query,
              execution_time=execution_time,
              user=current_user.username,
              source="markets_api")
        
        return CompanyFundamentalsUpdateResponse(
            success=False,
            message=f"更新公司基本面数据失败: {str(e)}",
            execution_time=execution_time
        )


# 财务报表导出相关的数据模型
class FinancialStatementsExportRequest(BaseModel):
    """财务报表导出请求"""
    query: str = Field(..., description="股票代码或公司名称")
    year_range: Optional[List[int]] = Field(None, description="年份范围，如[2020, 2023]")
    period_type: str = Field(default="yearly", description="期间类型: yearly 或 quarterly")
    display_unit: str = Field(default="original", description="显示单位: original, thousands, millions, billions")


@router.post("/company/export-financial-statements", summary="导出财务报表为Excel")
async def export_financial_statements(
    request: FinancialStatementsExportRequest,
    current_user=Depends(get_current_user)
):
    """
    导出指定公司的财务报表为Excel文件
    
    功能：
    1. 根据查询条件查找公司
    2. 获取指定年份范围和期间类型的财务报表数据
    3. 生成包含三个工作表的Excel文件：
       - Sheet1: Balance Sheet (资产负债表)
       - Sheet2: Income Statement (利润表) 
       - Sheet3: Cash Flow Statement (现金流量表)
    """
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.utils.dataframe import dataframe_to_rows
    
    try:
        from backend.database.db_manager import DatabaseManager
        
        info(f"开始导出财务报表", 
             query=request.query,
             year_range=request.year_range,
             period_type=request.period_type,
             display_unit=request.display_unit,
             user=current_user.username,
             source="markets_api")
        
        # 获取数据库连接参数
        from backend.utils.database_config import get_database_config
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 查找公司信息
        query_sql = """
        SELECT id, code, name, exchange, primaryticker, currencycode
        FROM companies 
        WHERE UPPER(code) = UPPER(%s) 
           OR UPPER(primaryticker) = UPPER(%s) 
           OR UPPER(name) ILIKE %s
        ORDER BY 
            CASE 
                WHEN UPPER(code) = UPPER(%s) THEN 1
                WHEN UPPER(primaryticker) = UPPER(%s) THEN 2
                ELSE 3
            END
        LIMIT 1
        """
        
        search_pattern = f"%{request.query}%"
        with db_manager.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cursor.execute(query_sql, (request.query, request.query, search_pattern, request.query, request.query))
            company_result = cursor.fetchall()
        
        if not company_result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到公司: {request.query}"
            )
        
        company = dict(company_result[0])
        company_id = company['id']
        
        # 构建年份过滤条件
        year_filter = ""
        params = [company_id]
        if request.year_range and len(request.year_range) == 2:
            start_year, end_year = request.year_range
            year_filter = " AND EXTRACT(YEAR FROM date) BETWEEN %s AND %s"
            params.extend([start_year, end_year])
        
        # 构建期间类型过滤条件
        period_filter = ""
        if request.period_type == "quarterly":
            period_filter = " AND period_type = 'quarterly'"
        else:
            period_filter = " AND period_type = 'annual'"
        
        # 查询财务报表数据
        with db_manager.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            
            # 查询资产负债表
            balance_sheet_sql = f"""
            SELECT * FROM company_balance_sheet 
            WHERE company_id = %s{year_filter}{period_filter}
            ORDER BY date DESC
            """
            cursor.execute(balance_sheet_sql, params)
            balance_sheet_data = [dict(row) for row in cursor.fetchall()]
            
            # 查询利润表
            income_statement_sql = f"""
            SELECT * FROM company_income_statement 
            WHERE company_id = %s{year_filter}{period_filter}
            ORDER BY date DESC
            """
            cursor.execute(income_statement_sql, params)
            income_statement_data = [dict(row) for row in cursor.fetchall()]
            
            # 查询现金流量表
            cash_flow_sql = f"""
            SELECT * FROM company_cash_flow 
            WHERE company_id = %s{year_filter}{period_filter}
            ORDER BY date DESC
            """
            cursor.execute(cash_flow_sql, params)
            cash_flow_data = [dict(row) for row in cursor.fetchall()]
        
        if not balance_sheet_data and not income_statement_data and not cash_flow_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"未找到符合条件的财务报表数据: {request.query}"
            )
        
        # 创建Excel文件
        output = io.BytesIO()
        workbook = Workbook()
        
        # 删除默认工作表
        workbook.remove(workbook.active)
        
        # 定义字段映射 - 与前端FundamentalsPage完全一致
        balance_sheet_fields = {
            # Assets Section - Current Assets
            'cash': 'Cash',
            'cashandequivalents': 'Cash and Equivalents',
            'cashandshortterminvestments': 'Cash and Short-term Investments',
            'shortterminvestments': 'Short-term Investments',
            'netreceivables': 'Net Receivables',
            'inventory': 'Inventory',
            'othercurrentassets': 'Other Current Assets',
            'totalcurrentassets': 'Total Current Assets',
            
            # Non-Current Assets
            'longterminvestments': 'Long-term Investments',
            'propertyplantequipment': 'Property, Plant & Equipment',
            'propertyplantandequipmentgross': 'PP&E Gross',
            'propertyplantandequipmentnet': 'PP&E Net',
            'accumulateddepreciation': 'Accumulated Depreciation',
            'goodwill': 'Goodwill',
            'intangibleassets': 'Intangible Assets',
            'accumulatedamortization': 'Accumulated Amortization',
            'noncurrrentassetsother': 'Other Non-current Assets',
            'deferredlongtermassetcharges': 'Deferred Long-term Asset Charges',
            'otherassets': 'Other Assets',
            'earningassets': 'Earning Assets',
            'nettangibleassets': 'Net Tangible Assets',
            'noncurrentassetstotal': 'Total Non-current Assets',
            
            # Total Assets
            'totalassets': 'Total Assets',

            # Liabilities Section - Current Liabilities
            'accountspayable': 'Accounts Payable',
            'shorttermdebt': 'Short-term Debt',
            'shortlongtermdebt': 'Short/Long-term Debt',
            'shortlongtermdebttotal': 'Short/Long-term Debt Total',
            'othercurrentliab': 'Other Current Liabilities',
            'currentdeferredrevenue': 'Current Deferred Revenue',
            'totalcurrentliabilities': 'Total Current Liabilities',
            
            # Non-Current Liabilities
            'longtermdebt': 'Long-term Debt',
            'longtermdebttotal': 'Total Long-term Debt',
            'capitalleaseobligations': 'Capital Lease Obligations',
            'deferredlongtermliab': 'Deferred Long-term Liabilities',
            'noncurrentliabilitiesother': 'Other Non-current Liabilities',
            'otherliab': 'Other Liabilities',
            'netdebt': 'Net Debt',
            'noncurrentliabilitiestotal': 'Total Non-current Liabilities',
            
            # Total Liabilities
            'totalliab': 'Total Liabilities',

            # Equity Section
            'commonstock': 'Common Stock',
            'capitalstock': 'Capital Stock',
            'retainedearnings': 'Retained Earnings',
            'otherstockholderequity': 'Other Stockholder Equity',
            'accumulatedothercomprehensiveincome': 'Accumulated Other Comprehensive Income',
            'additionalpaidincapital': 'Additional Paid-in Capital',
            'commonstocktotalequity': 'Common Stock Total Equity',
            'preferredstocktotalequity': 'Preferred Stock Total Equity',
            'retainedearningstotalequity': 'Retained Earnings Total Equity',
            'treasurystock': 'Treasury Stock',
            'totalpermanentequity': 'Total Permanent Equity',
            'noncontrollinginterestinconsolidatedentity': 'Non-controlling Interest in Consolidated Entity',
            'temporaryequityredeemablenoncontrollinginterests': 'Temporary Equity Redeemable Non-controlling Interests',
            'negativegoodwill': 'Negative Goodwill',
            'warrants': 'Warrants',
            'preferredstockredeemable': 'Preferred Stock Redeemable',
            'capitalsurpluse': 'Capital Surplus',
            'liabilitiesandstockholdersequity': 'Liabilities and Stockholders Equity',
            'networkingcapital': 'Net Working Capital',
            'netinvestedcapital': 'Net Invested Capital',
            'commonstocksharesoutstanding': 'Common Stock Shares Outstanding',
            'totalstockholderequity': 'Total Stockholder Equity'
        }
        
        income_statement_fields = {
            # Revenue Section
            'totalrevenue': 'Total Revenue',
            'costofrevenue': 'Cost of Revenue',
            'grossprofit': 'Gross Profit',
            
            # Operating Expenses Section
            'totaloperatingexpenses': 'Total Operating Expenses',
            'researchdevelopment': 'Research & Development',
            'sellinggeneraladministrative': 'Selling, General & Administrative',
            'sellingandmarketingexpenses': 'Selling and Marketing Expenses',
            'otheroperatingexpenses': 'Other Operating Expenses',
            
            # Operating Income Section
            'operatingincome': 'Operating Income',
            'ebit': 'EBIT',
            'ebitda': 'EBITDA',
            
            # Interest Section
            'interestincome': 'Interest Income',
            'interestexpense': 'Interest Expense',
            'netinterestincome': 'Net Interest Income',
            
            # Other Income/Expense Section
            'totalotherincomeexpensenet': 'Total Other Income/Expense Net',
            'nonoperatingincomenetother': 'Non-Operating Income Net Other',
            
            # Pre-Tax Income Section
            'incomebeforetax': 'Income Before Tax',
            
            # Tax Section
            'taxprovision': 'Tax Provision',
            'incometaxexpense': 'Income Tax Expense',
            
            # Net Income Section
            'netincome': 'Net Income',
            'netincomefromcontinuingops': 'Net Income from Continuing Operations',
            'netincomeapplicabletocommonshares': 'Net Income Applicable to Common Shares',
            
            # Depreciation & Amortization Section
            'reconcileddepreciation': 'Reconciled Depreciation',
            'depreciationandamortization': 'Depreciation and Amortization',
            
            # Special Items Section
            'extraordinaryitems': 'Extraordinary Items',
            'nonrecurring': 'Non-Recurring',
            'otheritems': 'Other Items',
            'discontinuedoperations': 'Discontinued Operations',
            'effectofaccountingcharges': 'Effect of Accounting Charges',
            
            # Minority Interest & Adjustments Section
            'minorityinterest': 'Minority Interest',
            'preferredstockandotheradjustments': 'Preferred Stock and Other Adjustments'
        }
        
        cash_flow_fields = {
            # Operating Activities
            'netincome': 'Net Income',
            'depreciation': 'Depreciation',
            'stockbasedcompensation': 'Stock Based Compensation',
            'changeinworkingcapital': 'Change in Working Capital',
            'changetoaccountreceivables': 'Change to Account Receivables',
            'changetoinventory': 'Change to Inventory',
            'changetoliabilities': 'Change to Liabilities',
            'changetooperatingactivities': 'Change to Operating Activities',
            'changetonetincome': 'Change to Net Income',
            'changereceivables': 'Change Receivables',
            'othernoncashitems': 'Other Non-Cash Items',
            'cashflowsotheroperating': 'Cash Flows Other Operating',
            'totalcashfromoperatingactivities': 'Total Cash from Operating Activities',
            
            # Investing Activities
            'capitalexpenditures': 'Capital Expenditures',
            'investments': 'Investments',
            'othercashflowsfrominvestingactivities': 'Other Cash Flows from Investing',
            'salepurchaseofstock': 'Sale/Purchase of Stock',
            'totalcashflowsfrominvestingactivities': 'Total Cash from Investing Activities',
            
            # Financing Activities
            'dividendspaid': 'Dividends Paid',
            'netborrowings': 'Net Borrowings',
            'issuanceofcapitalstock': 'Issuance of Capital Stock',
            'othercashflowsfromfinancingactivities': 'Other Cash Flows from Financing',
            'totalcashfromfinancingactivities': 'Total Cash from Financing Activities',
            
            # Cash Flow Summary
            'changeincash': 'Change in Cash',
            'beginperiodcashflow': 'Beginning Period Cash Flow',
            'endperiodcashflow': 'End Period Cash Flow',
            'exchangeratechanges': 'Exchange Rate Changes',
            'cashandcashequivalentschanges': 'Cash and Cash Equivalents Changes',
            'freecashflow': 'Free Cash Flow'
        }
        
        # 处理显示单位
        def format_value(value, unit="original"):
            if value is None or value == '':
                return None
            if unit == "thousands":
                return value / 1000
            elif unit == "millions":
                return value / 1000000
            elif unit == "billions":
                return value / 1000000000
            return value
        
        # 创建资产负债表工作表
        if balance_sheet_data:
            ws_balance = workbook.create_sheet(title=f"balance_sheet_{request.period_type}")
            _create_financial_sheet(ws_balance, balance_sheet_data, balance_sheet_fields, 
                                 request.display_unit, company)
        
        # 创建利润表工作表
        if income_statement_data:
            ws_income = workbook.create_sheet(title=f"income_statement_{request.period_type}")
            _create_financial_sheet(ws_income, income_statement_data, income_statement_fields,
                                 request.display_unit, company)
        
        # 创建现金流量表工作表
        if cash_flow_data:
            ws_cashflow = workbook.create_sheet(title=f"cashflow_statement_{request.period_type}")
            _create_financial_sheet(ws_cashflow, cash_flow_data, cash_flow_fields,
                                 request.display_unit, company)
        
        # 保存到内存
        workbook.save(output)
        output.seek(0)
        
        # 生成文件名
        company_code = company.get('code', 'company')
        period_suffix = 'quarterly' if request.period_type == 'quarterly' else 'yearly'
        year_suffix = ""
        if request.year_range and len(request.year_range) == 2:
            year_suffix = f"_{request.year_range[0]}-{request.year_range[1]}"
        
        filename = f"{company_code}_financial_statements_{period_suffix}{year_suffix}.xlsx"
        
        info(f"财务报表导出完成", 
             company_code=company_code,
             filename=filename,
             balance_sheet_records=len(balance_sheet_data),
             income_statement_records=len(income_statement_data),
             cash_flow_records=len(cash_flow_data),
             user=current_user.username,
             source="markets_api")
        
        return Response(
            content=output.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        error(f"导出财务报表失败: {str(e)}", 
              query=request.query,
              user=current_user.username,
              source="markets_api")
        
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"导出财务报表失败: {str(e)}"
        )


def _create_financial_sheet(worksheet, data, field_mapping, display_unit, company):
    """创建财务报表工作表的辅助函数"""
    from openpyxl.styles import Font, PatternFill, Alignment
    
    # 设置标题样式
    title_font = Font(bold=True, size=14)
    header_font = Font(bold=True, size=11)
    header_fill = PatternFill(start_color="CCCCCC", end_color="CCCCCC", fill_type="solid")
    
    # 添加公司信息
    worksheet['A1'] = f"Company: {company.get('name', 'N/A')}"
    worksheet['A1'].font = title_font
    worksheet['A2'] = f"Code: {company.get('code', 'N/A')}"
    worksheet['A3'] = f"Exchange: {company.get('exchange', 'N/A')}"
    worksheet['A4'] = f"Currency: {company.get('currencycode', 'USD')}"
    worksheet['A5'] = f"Display Unit: {display_unit}"
    worksheet['A6'] = f"Export Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}"
    
    # 创建数据表格
    row_start = 8
    
    # 创建表头
    headers = ['Financial Item', 'Field Name']
    periods = sorted(set(item['date'] for item in data), reverse=True)
    headers.extend([f"{period}" for period in periods])
    
    for col, header in enumerate(headers, 1):
        cell = worksheet.cell(row=row_start, column=col, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal='center')
    
    # 填充数据
    row = row_start + 1
    for field_key, field_label in field_mapping.items():
        worksheet.cell(row=row, column=1, value=field_label)
        worksheet.cell(row=row, column=2, value=field_key)
        
        for col, period in enumerate(periods, 3):
            # 查找对应期间的数据
            period_data = next((item for item in data if item['date'] == period), None)
            if period_data and field_key in period_data:
                value = period_data[field_key]
                if value is not None:
                    # 应用显示单位转换
                    if display_unit == "thousands":
                        formatted_value = value / 1000
                    elif display_unit == "millions":
                        formatted_value = value / 1000000
                    elif display_unit == "billions":
                        formatted_value = value / 1000000000
                    else:
                        formatted_value = value
                    
                    worksheet.cell(row=row, column=col, value=formatted_value)
        
        row += 1
    
    # 调整列宽
    for column in worksheet.columns:
        max_length = 0
        column_letter = column[0].column_letter
        for cell in column:
            try:
                cell_length = len(str(cell.value or ''))
                if cell_length > max_length:
                    max_length = cell_length
            except Exception:
                pass
        adjusted_width = min(max_length + 2, 50)
        worksheet.column_dimensions[column_letter].width = adjusted_width
