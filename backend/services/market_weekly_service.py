# -*- coding: utf-8 -*-
"""
Market Weekly Report Service

生成Market Weekly报告的核心服务，包括：
- Week-over-Week计算
- 相对表现计算（indexed to 100）
- 执行摘要生成
- 日历数据聚合（经济事件、财报、IPO）
- 静态HTML报告生成与OSS上传（新增）
"""

import os
import re
import json
import time
import secrets
from datetime import datetime, timedelta, date, timezone
from typing import Dict, List, Any, Optional, Tuple
from zoneinfo import ZoneInfo
from uuid import uuid4

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.datetime_utils import utcnow, utc_today
from backend.utils.logger import get_logger
from backend.services.economic_events_service import EconomicEventsService
from backend.services.oss_service import OSSService
from config.settings import config

# Jinja2 模板环境
from jinja2 import Environment, FileSystemLoader, select_autoescape
TEMPLATE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'templates')
_jinja_env = Environment(
    loader=FileSystemLoader(TEMPLATE_DIR),
    autoescape=select_autoescape(['html', 'xml']),
)

logger = get_logger("market_weekly_service")

TAG_NAME = "Market Weekly"

# 内存缓存（简单实现，如果Redis不可用则使用此缓存）
_memory_cache = {}
_cache_lock = {}  # 用于防止并发请求


class MarketWeeklyService:
    """Market Weekly报告生成服务"""
    
    def __init__(self):
        """初始化服务"""
        db_config = get_database_config()
        self.db_manager = DatabaseManager(db_config)
        logger.info("MarketWeeklyService initialized")
    
    def generate_report(self, period: str = 'latest', use_cache: bool = True, cache_ttl: int = 3600, user_id: str = None) -> Dict[str, Any]:
        """
        生成Market Weekly报告
        
        Args:
            period: 报告周期，'latest' 表示最新一周
            use_cache: 是否使用缓存，默认True
            cache_ttl: 缓存过期时间（秒），默认3600秒（1小时）
            user_id: 用户ID，用于筛选用户个性化的主题标签
        
        Returns:
            完整的报告数据字典
        """
        # 🔥 缓存key包含用户ID，确保每个用户的报告独立缓存
        cache_key = f"market_weekly_report:{period}:{user_id}" if user_id else f"market_weekly_report:{period}"
        
        # 尝试从缓存获取
        if use_cache:
            cached_data = self._get_from_cache(cache_key)
            if cached_data:
                logger.info(f"Using cached report for period: {period}")
                return cached_data
        
        # 如果缓存不存在或已过期，生成新报告
        logger.info(f"Generating Market Weekly report for period: {period}")
        
        # 防止并发请求同时生成报告
        if cache_key in _cache_lock:
            logger.info(f"Report generation already in progress for {period}, waiting...")
            # 等待最多10秒
            for _ in range(20):
                time.sleep(0.5)
                cached_data = self._get_from_cache(cache_key)
                if cached_data:
                    return cached_data
            logger.warning(f"Timeout waiting for report generation, proceeding with new generation")
        
        _cache_lock[cache_key] = True
        try:
            # 1. 计算周度时间范围
            week_start, week_end = self._calculate_week_range(period)
            generated_at = utcnow().isoformat().replace('+00:00', 'Z')
            
            logger.info(f"Report period: {week_start} to {week_end}")
            
            # 2. 计算Week-over-Week变化
            indices_wow = self._calculate_wow('index', week_start, week_end)
            sectors_wow = self._calculate_wow('sector', week_start, week_end)
            forex_wow = self._calculate_wow('forex', week_start, week_end)
            commodities_wow = self._calculate_wow('commodity', week_start, week_end)
            yields_wow = self._calculate_wow('bond', week_start, week_end)
            
            # 2.5 计算AI Related数据（从数据库获取Market Weekly主题，分US和China市场）
            us_ai_wow, us_ai_relative, china_ai_wow, china_ai_relative = self._calculate_ai_related_performance(week_start, week_end, days=90, user_id=user_id)
            
            # 3. 计算相对表现（3个月，indexed to 100）
            indices_relative = self._calculate_relative_performance('index', days=90)
            sectors_relative = self._calculate_relative_performance('sector', days=90)
            forex_relative = self._calculate_relative_performance('forex', days=90)
            commodities_relative = self._calculate_relative_performance('commodity', days=90)
            yields_data = self._calculate_relative_performance('bond', days=90)
            
            # 4. 生成执行摘要
            executive_summary = self._generate_executive_summary(indices_wow, week_start, week_end)
            
            # 5. 获取技术视图数据（资产列表）
            technical_views = self._get_technical_views_data()
            
            # 5.5 获取My Themes股票数据（个股走势图）
            my_themes_stocks = self._get_my_themes_stocks(user_id=user_id)
            
            # 6. 获取下周展望数据
            next_week_outlook = self._get_next_week_outlook(week_end)
            
            # 整合报告数据
            report = {
                'meta': {
                    'week_start': week_start.strftime('%Y-%m-%d'),
                    'week_end': week_end.strftime('%Y-%m-%d'),
                    'generated_at': generated_at
                },
                'executive_summary': executive_summary,
                'weekly_performance': {
                    'indices_wow': indices_wow,
                    'indices_relative': indices_relative,
                    'sectors_wow': sectors_wow,
                    'sectors_relative': sectors_relative,
                    'us_ai_wow': us_ai_wow,
                    'us_ai_relative': us_ai_relative,
                    'china_ai_wow': china_ai_wow,
                    'china_ai_relative': china_ai_relative,
                    'forex_wow': forex_wow,
                    'forex_relative': forex_relative,
                    'commodities_wow': commodities_wow,
                    'commodities_relative': commodities_relative,
                    'yields_wow': yields_wow,
                    'yields_data': yields_data
                },
                'technical_views': technical_views,
                'my_themes_stocks': my_themes_stocks,
                'next_week_outlook': next_week_outlook
            }
            
            logger.info("Report generated successfully")
            
            # 保存到缓存
            if use_cache:
                self._save_to_cache(cache_key, report, cache_ttl)
            
            return report
        finally:
            # 释放锁
            if cache_key in _cache_lock:
                del _cache_lock[cache_key]
    
    def _calculate_week_range(self, period: str) -> tuple:
        """
        计算周度时间范围
        
        完全参考report_debug.py的compute_wow逻辑：
        1. resample("W-FRI") - 按周五重采样
        2. wk.index[-2], wk.index[-1] - 取最后两个周五
        3. w0 (week_start) = 倒数第二个周五
        4. w1 (week_end) = 最后一个周五
        
        这样可以确保：
        - 在周末时，用户看到的是上周完整的报告（周五到周五）
        - 避免在周末或周初显示"不完整"的周数据
        
        Args:
            period: 'latest' 或特定日期
        
        Returns:
            (week_start, week_end) 元组，都是datetime对象，都是周五
        """
        if period == 'latest':
            try:
                conn = self.db_manager.get_connection()
                cursor = conn.cursor()
                
                # 1. 获取过去30天的所有周五交易日（确保能找到至少2个）
                # 模拟 resample("W-FRI").last() 的逻辑
                cursor.execute("""
                    WITH all_fridays AS (
                        SELECT DISTINCT date::date as friday_date
                        FROM market_data_eod
                        WHERE EXTRACT(DOW FROM date) = 5  -- 周五
                          AND date >= CURRENT_DATE - INTERVAL '30 days'
                          AND date <= CURRENT_DATE
                        ORDER BY date DESC
                    )
                    SELECT friday_date FROM all_fridays
                    ORDER BY friday_date DESC
                    LIMIT 2
                """)
                
                fridays = cursor.fetchall()
                cursor.close()
                conn.close()
                
                if len(fridays) >= 2:
                    # 完全按照report_debug.py的逻辑：
                    # w0, w1 = wk.index[-2], wk.index[-1]
                    week_end_date = fridays[0][0]      # wk.index[-1] = 最后一个周五
                    week_start_date = fridays[1][0]    # wk.index[-2] = 倒数第二个周五
                    
                    logger.info(f"Found 2 Fridays from DB: week_start={week_start_date}, week_end={week_end_date}")
                elif len(fridays) == 1:
                    # 只找到一个周五，使用该周五和前一周
                    week_end_date = fridays[0][0]
                    week_start_date = week_end_date - timedelta(days=7)
                    logger.warning(f"Only found 1 Friday, using week_start={week_start_date}, week_end={week_end_date}")
                else:
                    # 没有找到周五，使用fallback
                    raise ValueError("No Fridays found in database, using fallback")
                
            except Exception as e:
                logger.error(f"Error querying week range from database: {str(e)}, using fallback")
                # 使用fallback逻辑（不依赖数据库）
                today = utc_today()
                
                # 找到最近的周五
                days_since_friday = (today.weekday() - 4) % 7
                if days_since_friday == 0:
                    # 今天是周五
                    last_friday = today
                else:
                    # 往前找最近的周五
                    if days_since_friday < 0:
                        days_since_friday += 7
                    last_friday = today - timedelta(days=days_since_friday)
                
                # 上一个周五
                previous_friday = last_friday - timedelta(days=7)
                
                week_end_date = last_friday
                week_start_date = previous_friday
                
                logger.info(f"Using fallback logic: week_start={week_start_date}, week_end={week_end_date}")
        else:
            # TODO: 支持特定日期范围
            today = utc_today()
            days_since_friday = (today.weekday() - 4) % 7
            if days_since_friday == 0:
                last_friday = today
            else:
                if days_since_friday < 0:
                    days_since_friday += 7
                last_friday = today - timedelta(days=days_since_friday)
            
            week_end_date = last_friday
            week_start_date = last_friday - timedelta(days=7)
        
        # 转换为datetime对象
        week_start = datetime.combine(week_start_date, datetime.min.time())
        week_end = datetime.combine(week_end_date, datetime.max.time())
        
        logger.info(f"Final result: week_start={week_start.date()} ({week_start.strftime('%A')}), "
                   f"week_end={week_end.date()} ({week_end.strftime('%A')})")
        
        return week_start, week_end
    
    def _calculate_wow(self, asset_type: str, week_start: datetime, week_end: datetime) -> List[Dict[str, Any]]:
        """
        计算Week-over-Week变化率
        
        对比week_start和week_end两个日期的收盘价
        如果指定日期没有数据，使用该日期之前最近可用的交易数据
        
        Args:
            asset_type: 资产类型 ('index', 'sector', 'forex', 'commodity', 'bond')
            week_start: 起始日期（通常是上周五）
            week_end: 结束日期（本周最新交易日）
        
        Returns:
            WoW变化数据列表，按变化率排序
        """
        logger.info(f"Calculating WoW for {asset_type} from {week_start.date()} to {week_end.date()}")
        
        # SQL查询：获取week_start和week_end两个日期的收盘价并计算变化率
        # 如果指定日期没有数据，使用该日期之前最近可用的数据（<=指定日期的最大日期）
        # 这样可以确保每个资产都能找到对应的数据，类似于 report_debug.py 中的 resample 逻辑
        query = """
        WITH week_start_available_date AS (
            SELECT 
                code,
                MAX(date)::date as latest_date
            FROM market_data_eod
            WHERE asset_type = %s
              AND date <= %s
            GROUP BY code
        ),
        week_end_available_date AS (
            SELECT 
                code,
                MAX(date)::date as latest_date
            FROM market_data_eod
            WHERE asset_type = %s
              AND date <= %s
            GROUP BY code
        ),
        week_start_prices AS (
            SELECT 
                eod.code,
                COALESCE(assets.name_en, rt.asset_name, eod.code) as name,
                eod.close as start_close
            FROM market_data_eod eod
            INNER JOIN week_start_available_date wad ON eod.code = wad.code 
                AND eod.date::date = wad.latest_date
            LEFT JOIN market_assets assets ON eod.code = assets.code
            LEFT JOIN market_data_realtime rt ON eod.code = rt.code
            WHERE eod.asset_type = %s
        ),
        week_end_prices AS (
            SELECT 
                eod.code,
                eod.close as end_close
            FROM market_data_eod eod
            INNER JOIN week_end_available_date wad ON eod.code = wad.code 
                AND eod.date::date = wad.latest_date
            WHERE eod.asset_type = %s
        )
        SELECT 
            ws.name,
            ROUND(((we.end_close - ws.start_close) / NULLIF(ws.start_close, 0) * 100)::numeric, 2) as change_pct
        FROM week_start_prices ws
        INNER JOIN week_end_prices we ON ws.code = we.code
        WHERE ws.start_close IS NOT NULL 
          AND ws.start_close != 0
          AND we.end_close IS NOT NULL
        ORDER BY change_pct DESC;
        """
        
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (
                asset_type, week_start.date(),  # for week_start_available_date
                asset_type, week_end.date(),    # for week_end_available_date
                asset_type,                     # for week_start_prices
                asset_type                      # for week_end_prices
            ))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'name': row[0],
                    'change_pct': float(row[1]) if row[1] is not None else 0.0
                })
            
            cursor.close()
            conn.close()
            
            logger.info(f"Calculated WoW for {len(results)} {asset_type} assets")
            return results
        
        except Exception as e:
            logger.error(f"Error calculating WoW for {asset_type}: {str(e)}")
            return []
    
    def _calculate_relative_performance(self, asset_type: str, days: int = 90) -> List[Dict[str, Any]]:
        """
        计算相对表现（indexed to 100）
        
        使用时间范围内每个资产的第一个交易日作为基准（100），计算后续日期的相对表现
        类似于 report_debug.py 中的 relative_panel 函数
        
        Args:
            asset_type: 资产类型
            days: 回溯天数
        
        Returns:
            相对表现数据列表，每个日期一条记录
        """
        logger.info(f"Calculating relative performance for {asset_type}, {days} days")
        
        # SQL查询：获取历史数据并计算相对表现
        # 优化：使用窗口函数来获取每个资产的第一个收盘价，性能更好且逻辑更清晰
        query = """
        WITH daily_data AS (
            SELECT 
                eod.code,
                COALESCE(assets.name_en, rt.asset_name, eod.code) as name,
                eod.date::date as trade_date,
                eod.close,
                FIRST_VALUE(eod.close) OVER (
                    PARTITION BY eod.code 
                    ORDER BY eod.date
                ) as first_close
            FROM market_data_eod eod
            LEFT JOIN market_assets assets ON eod.code = assets.code
            LEFT JOIN market_data_realtime rt ON eod.code = rt.code
            WHERE eod.asset_type = %s
              AND eod.date >= CURRENT_DATE - %s
        )
        SELECT 
            trade_date,
            name,
            ROUND((close / NULLIF(first_close, 0) * 100)::numeric, 2) as indexed_value
        FROM daily_data
        WHERE first_close IS NOT NULL 
          AND first_close != 0
          AND close IS NOT NULL
        ORDER BY trade_date, name;
        """
        
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (asset_type, days))
            
            # 组织数据：按日期分组
            date_data_map = {}
            for row in cursor.fetchall():
                trade_date = row[0].strftime('%Y-%m-%d')
                name = row[1]
                indexed_value = float(row[2]) if row[2] is not None else 100.0
                
                if trade_date not in date_data_map:
                    date_data_map[trade_date] = {'date': trade_date}
                
                date_data_map[trade_date][name] = indexed_value
            
            # 转换为列表
            results = list(date_data_map.values())
            results.sort(key=lambda x: x['date'])
            
            cursor.close()
            conn.close()
            
            logger.info(f"Calculated relative performance for {len(results)} days")
            return results
        
        except Exception as e:
            logger.error(f"Error calculating relative performance for {asset_type}: {str(e)}")
            return []
    
    def _generate_executive_summary(self, indices_wow: List[Dict], week_start: datetime, week_end: datetime) -> Dict[str, Any]:
        """
        生成执行摘要
        
        Args:
            indices_wow: 指数WoW数据
            week_start: 周开始日期
            week_end: 周结束日期
        
        Returns:
            执行摘要数据
        """
        logger.info("Generating executive summary")
        
        if not indices_wow:
            return {
                'top_performer': {'name': 'N/A', 'change': 0.0},
                'bottom_performer': {'name': 'N/A', 'change': 0.0},
                'regional_overview': {
                    'us_markets': 'insufficient data',
                    'eu_markets': 'insufficient data',
                    'asia_markets': 'insufficient data'
                }
            }
        
        # Top和Bottom performer
        top_performer = indices_wow[0] if indices_wow else {'name': 'N/A', 'change_pct': 0.0}
        bottom_performer = indices_wow[-1] if indices_wow else {'name': 'N/A', 'change_pct': 0.0}
        
        # 区域概览
        us_indices = ['S&P 500', 'Nasdaq 100', 'Dow Jones']
        eu_indices = ['STOXX 50', 'DAX (Germany)', 'CAC 40 (France)']
        asia_indices = ['Nikkei 225', 'Hang Seng']
        
        us_avg = self._calculate_regional_avg(indices_wow, us_indices)
        eu_avg = self._calculate_regional_avg(indices_wow, eu_indices)
        asia_avg = self._calculate_regional_avg(indices_wow, asia_indices)
        
        return {
            'top_performer': {
                'name': top_performer['name'],
                'change': top_performer['change_pct']
            },
            'bottom_performer': {
                'name': bottom_performer['name'],
                'change': bottom_performer['change_pct']
            },
            'regional_overview': {
                'us_markets': self._format_regional_summary(us_avg),
                'eu_markets': self._format_regional_summary(eu_avg),
                'asia_markets': self._format_regional_summary(asia_avg)
            }
        }
    
    def _calculate_regional_avg(self, wow_data: List[Dict], indices: List[str]) -> float:
        """计算区域平均变化率"""
        regional_data = [item['change_pct'] for item in wow_data if item['name'] in indices]
        if not regional_data:
            return 0.0
        return sum(regional_data) / len(regional_data)
    
    def _format_regional_summary(self, avg_change: float) -> str:
        """格式化区域摘要文字"""
        if avg_change > 1.0:
            return f"broad strength (+{avg_change:.2f}% avg)"
        elif avg_change < -1.0:
            return f"widespread weakness ({avg_change:.2f}% avg)"
        else:
            return f"mixed performance ({avg_change:+.2f}% avg)"
    
    def _get_technical_views_data(self) -> Dict[str, List[Dict]]:
        """
        获取技术视图数据（资产列表）
        
        Returns:
            各类资产的列表信息
        """
        logger.info("Fetching technical views data")
        
        query = """
        SELECT DISTINCT 
            rt.code as symbol,
            COALESCE(assets.name_en, rt.asset_name, rt.code) as name,
            rt.asset_type,
            CASE 
                WHEN COALESCE(assets.name_en, rt.asset_name, rt.code) LIKE '%(US)%' 
                     OR COALESCE(assets.name_en, rt.asset_name, rt.code) LIKE '%S&P%' 
                     OR COALESCE(assets.name_en, rt.asset_name, rt.code) LIKE '%Nasdaq%' 
                     OR COALESCE(assets.name_en, rt.asset_name, rt.code) LIKE '%Dow%' THEN 'US'
                WHEN COALESCE(assets.name_en, rt.asset_name, rt.code) LIKE '%(Europe)%' 
                     OR COALESCE(assets.name_en, rt.asset_name, rt.code) LIKE '%STOXX%' 
                     OR COALESCE(assets.name_en, rt.asset_name, rt.code) LIKE '%DAX%' 
                     OR COALESCE(assets.name_en, rt.asset_name, rt.code) LIKE '%CAC%' THEN 'Europe'
                WHEN COALESCE(assets.name_en, rt.asset_name, rt.code) LIKE '%(Asia)%' 
                     OR COALESCE(assets.name_en, rt.asset_name, rt.code) LIKE '%Nikkei%' 
                     OR COALESCE(assets.name_en, rt.asset_name, rt.code) LIKE '%Hang Seng%' THEN 'Asia'
                ELSE 'Other'
            END as region
        FROM market_data_realtime rt
        LEFT JOIN market_assets assets ON rt.code = assets.code
        WHERE rt.asset_type IN ('index', 'sector', 'forex', 'commodity', 'bond')
          AND rt.updated_at >= CURRENT_DATE - 7
        ORDER BY rt.asset_type, name;
        """
        
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            cursor.execute(query)
            
            indices = []
            sectors = []
            forex = []
            commodities = []
            yields = []
            
            for row in cursor.fetchall():
                symbol = row[0]
                name = row[1]
                asset_type = row[2]
                region = row[3]
                
                asset_info = {
                    'symbol': symbol,
                    'name': name,
                    'primary_ticker': symbol  # 可以后续优化
                }
                
                if asset_type == 'index':
                    asset_info['region'] = region
                    indices.append(asset_info)
                elif asset_type == 'sector':
                    sectors.append(asset_info)
                elif asset_type == 'forex':
                    forex.append(asset_info)
                elif asset_type == 'commodity':
                    commodities.append(asset_info)
                elif asset_type == 'bond':
                    yields.append(asset_info)
            
            cursor.close()
            conn.close()
            
            logger.info(f"Technical views data: {len(indices)} indices, {len(sectors)} sectors, "
                       f"{len(forex)} forex, {len(commodities)} commodities, {len(yields)} yields")
            
            return {
                'indices': indices,
                'sectors': sectors,
                'forex': forex,
                'commodities': commodities,
                'yields': yields
            }
        
        except Exception as e:
            logger.error(f"Error fetching technical views data: {str(e)}")
            return {
                'indices': [],
                'sectors': [],
                'forex': [],
                'commodities': [],
                'yields': []
            }
    
    def _get_next_week_outlook(self, week_end: datetime) -> Dict[str, List[Dict]]:
        """
        获取下周展望数据
        
        Args:
            week_end: 本周结束日期
        
        Returns:
            包含经济日历、财报日历、IPO日历的字典
        """
        logger.info("Fetching next week outlook data")
        
        # 计算下周时间范围
        next_week_start = week_end + timedelta(days=1)
        next_week_end = next_week_start + timedelta(days=7)
        
        # 获取经济日历
        economic_calendar = self._get_economic_calendar(next_week_start, next_week_end)
        
        # 获取财报日历
        earnings_calendar = self._get_earnings_calendar(next_week_start, next_week_end)
        
        # 获取IPO日历
        ipo_calendar = self._get_ipo_calendar(next_week_start, next_week_end)
        
        return {
            'economic_calendar': economic_calendar,
            'earnings_calendar': earnings_calendar,
            'ipo_calendar': ipo_calendar
        }
    
    def _get_economic_calendar(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """获取经济日历数据"""
        logger.info("Fetching economic calendar (next week)")
        
        target_countries = ['US', 'CN', 'HK']
        from_date = start_date.date().strftime('%Y-%m-%d')
        to_date = end_date.date().strftime('%Y-%m-%d')
        
        # 首选：通过EconomicEventsService读取已存储的数据
        try:
            events_service = EconomicEventsService()
            raw_events = events_service.get_economic_events_from_db(
                from_date=from_date,
                to_date=to_date,
                country=",".join(target_countries),
                limit=200
            )
            formatted = self._format_economic_events(raw_events, target_countries)
            if formatted:
                logger.info(f"Found {len(formatted)} economic events via service")
                return formatted
            logger.info("Economic events service returned no results after filtering, falling back to legacy query")
        except Exception as e:
            logger.error(f"Error fetching economic events via service: {str(e)}")
        
        # 兜底：直接查询数据库
        return self._get_economic_calendar_legacy(start_date, end_date, target_countries)
    
    def _get_economic_calendar_legacy(
        self,
        start_date: datetime,
        end_date: datetime,
        target_countries: List[str]
    ) -> List[Dict]:
        """Legacy fallback: directly query economic_events table"""
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            # 确认表是否存在
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'economic_events'
                )
            """)
            if not cursor.fetchone()[0]:
                logger.warning("economic_events table does not exist, returning empty list")
                cursor.close()
                conn.close()
                return []
            
            query = """
            SELECT 
                "date",
                "time",
                "event",
                COALESCE("country", 'US') AS country,
                "actual",
                "forecast",
                "previous",
                "impact"
            FROM economic_events
            WHERE "date" >= %s AND "date" <= %s
              AND COALESCE("country", 'US') = ANY(%s)
            ORDER BY "date" ASC, "time" NULLS LAST, country
            LIMIT 200;
            """
            
            from_date = start_date.date().strftime('%Y-%m-%d')
            to_date = end_date.date().strftime('%Y-%m-%d')
            cursor.execute(query, (from_date, to_date, target_countries))
            
            rows = cursor.fetchall()
            cursor.close()
            conn.close()
            
            raw_events = []
            for row in rows:
                raw_events.append({
                    'date': row[0] if row[0] else '',
                    'time': row[1] or '',
                    'event': row[2] or '',
                    'country': (row[3] or 'US').upper(),
                    'actual': row[4],
                    'forecast': row[5],
                    'previous': row[6],
                    'impact': row[7]
                })
            
            formatted = self._format_economic_events(raw_events, target_countries)
            if formatted:
                logger.info(f"Found {len(formatted)} economic events via legacy query")
            else:
                logger.info("Legacy economic events query returned no results after filtering")
            return formatted
        
        except Exception as e:
            logger.error(f"Error fetching economic calendar via legacy query: {str(e)}")
            return []
    
    def _format_economic_events(
        self,
        raw_events: List[Dict[str, Any]],
        target_countries: List[str]
    ) -> List[Dict]:
        """格式化并筛选经济事件数据"""
        if not raw_events:
            return []
        
        formatted_events: List[Dict[str, Any]] = []
        for event in raw_events:
            if not event:
                continue
            
            date_str = (event.get('date') or event.get('event_date') or '').strip()
            if not date_str:
                continue
            
            country = (event.get('country') or 'US').upper()
            if target_countries and country not in target_countries:
                continue
            
            time_str = (event.get('time') or '').strip()
            impact_value = self._normalize_event_impact(event.get('impact'))
            
            formatted_events.append({
                'date': date_str,
                'time': time_str,
                'event': (event.get('event') or event.get('title') or event.get('name') or '').strip(),
                'country': country,
                'actual': self._normalize_event_value(event.get('actual')),
                'forecast': self._normalize_event_value(event.get('forecast')),
                'previous': self._normalize_event_value(event.get('previous')),
                'impact': impact_value if impact_value > 0 else None,
                '_impact_value': impact_value,
                '_sort_key': (
                    self._safe_parse_date(date_str),
                    self._safe_parse_time(time_str),
                    country
                )
            })
        
        if not formatted_events:
            return []
        
        formatted_events.sort(key=lambda item: item['_sort_key'])
        
        prioritized = [item for item in formatted_events if item['_impact_value'] >= 2]
        if not prioritized:
            prioritized = formatted_events
        
        limited = prioritized[:40]
        
        normalized: List[Dict[str, Any]] = []
        for item in limited:
            normalized.append({
                'date': item['date'],
                'time': item['time'],
                'event': item['event'],
                'country': item['country'],
                'actual': item['actual'],
                'forecast': item['forecast'],
                'previous': item['previous'],
                'impact': item['impact']
            })
        
        return normalized
    
    def _normalize_event_impact(self, impact_value: Any) -> int:
        """将impact字段统一转换为整数等级"""
        if impact_value is None:
            return 0
        
        if isinstance(impact_value, (int, float)):
            return int(impact_value)
        
        impact_str = str(impact_value).strip()
        if not impact_str:
            return 0
        
        if set(impact_str) == {'*'}:
            return min(len(impact_str), 3)
        
        lower = impact_str.lower()
        mapping = {
            '3': 3,
            'high': 3,
            'very high': 3,
            '重要': 3,
            '2': 2,
            'medium': 2,
            'moderate': 2,
            'medium-high': 2,
            '1': 1,
            'low': 1
        }
        if lower in mapping:
            return mapping[lower]
        
        if 'high' in lower:
            return 3
        if 'medium' in lower or 'moderate' in lower:
            return 2
        if 'low' in lower:
            return 1
        
        return 0
    
    def _normalize_event_value(self, value: Any) -> str:
        """格式化实际值/预测值/前值"""
        if value is None:
            return '-'
        
        if isinstance(value, (int, float)):
            value_str = f"{value}"
        else:
            value_str = str(value).strip()
        
        if not value_str:
            return '-'
        
        if value_str.lower() in ('nan', 'none', 'null'):
            return '-'
        
        return value_str
    
    def _safe_parse_date(self, date_str: str) -> datetime:
        """安全地解析日期字符串以便排序"""
        if not date_str:
            return datetime.max
        
        for fmt in ('%Y-%m-%d', '%Y/%m/%d'):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        return datetime.max
    
    def _safe_parse_time(self, time_str: str):
        """安全地解析时间字符串以便排序"""
        if not time_str:
            return datetime.min.time()
        
        normalized = time_str.strip().lower()
        if normalized in ('all day', 'allday'):
            return datetime.min.time()
        if normalized in ('tentative', 'tbd', 'tba'):
            return datetime.max.time()
        
        for fmt in ('%H:%M', '%H:%M:%S'):
            try:
                return datetime.strptime(time_str, fmt).time()
            except ValueError:
                continue
        
        return datetime.min.time()
    
    def _get_earnings_calendar(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """获取财报日历数据"""
        # 使用company_earnings_history表，并且只展示：
        # - 用户watchlist中的股票（来自 target_tracking_config.primaryticker）
        # - 主题设置中的股票（来自 theme_company_associations.ticker，且已验证）
        query = """
        WITH watchlist_tickers AS (
            SELECT DISTINCT primaryticker AS ticker
            FROM target_tracking_config
            WHERE enabled = TRUE
              AND primaryticker IS NOT NULL
              AND primaryticker != ''
        ),
        theme_tickers AS (
            SELECT DISTINCT ticker
            FROM theme_company_associations
            WHERE is_verified = TRUE
              AND ticker IS NOT NULL
              AND ticker != ''
        ),
        combined_tickers AS (
            SELECT ticker FROM watchlist_tickers
            UNION
            SELECT ticker FROM theme_tickers
        )
        SELECT 
            ceh.reportdate AS date,
            c.code,
            c.name,
            ceh.beforeaftermarket,
            c.logourl
        FROM company_earnings_history ceh
        INNER JOIN companies c ON ceh.company_id = c.id
        INNER JOIN combined_tickers ct ON c.primaryticker = ct.ticker
        WHERE ceh.reportdate >= %s AND ceh.reportdate <= %s
        ORDER BY ceh.reportdate, c.code
        LIMIT 200;
        """
        
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (start_date, end_date))
            
            results = []
            for row in cursor.fetchall():
                # 格式化时间字段
                when = row[3] if row[3] else ''
                if 'Before' in when or 'bmo' in when.lower():
                    time_str = 'Before Open'
                elif 'After' in when or 'amc' in when.lower():
                    time_str = 'After Close'
                elif 'dmh' in when.lower():
                    time_str = 'During Hours'
                else:
                    time_str = when if when else '-'
                
                # 去掉ticker的.US后缀
                ticker = row[1].replace('.US', '') if row[1] else ''
                
                results.append({
                    'date': row[0].strftime('%Y-%m-%d') if row[0] else '',
                    'time': time_str,
                    'ticker': ticker,
                    'company': row[2] or '',
                    'logo_url': row[4] or ''
                })
            
            cursor.close()
            conn.close()
            
            logger.info(f"Found {len(results)} earnings events")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching earnings calendar: {str(e)}")
            return []
    
    def _get_ipo_calendar(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """获取IPO日历数据"""
        query = """
        SELECT 
            start_date,
            code,
            name,
            exchange,
            deal_type,
            price_from,
            price_to,
            shares
        FROM ipo_calendar
        WHERE start_date >= %s AND start_date <= %s
          AND (exchange LIKE '%%US%%' OR exchange LIKE '%%NYSE%%' OR exchange LIKE '%%NASDAQ%%' 
               OR exchange LIKE '%%HK%%' OR exchange LIKE '%%Hong Kong%%')
        ORDER BY start_date, code
        LIMIT 50;
        """
        
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            # 使用date()方法确保传递的是date对象而不是datetime对象
            cursor.execute(query, (start_date.date(), end_date.date()))
            
            results = []
            rows = cursor.fetchall()
            
            for row in rows:
                try:
                    # 确保row有足够的元素
                    if not row or len(row) < 8:
                        logger.warning(f"IPO row has insufficient columns: {len(row) if row else 0}, row data: {row}")
                        continue
                    
                    # 安全地访问每个索引
                    start_date_val = row[0]
                    code_val = row[1] if row[1] else ''
                    name_val = row[2] if row[2] else ''
                    exchange_val = row[3] if row[3] else ''
                    deal_type_val = row[4] if row[4] else '-'
                    price_from = row[5]
                    price_to = row[6]
                    shares_val = row[7]
                    
                    # 格式化价格范围
                    if price_from is not None and price_to is not None:
                        price_range = f"${float(price_from):.2f} - ${float(price_to):.2f}"
                    elif price_from is not None:
                        price_range = f"${float(price_from):.2f}"
                    else:
                        price_range = '-'
                    
                    # 格式化股数
                    if shares_val is not None:
                        # 格式化大数字，添加千位分隔符
                        shares = f"{int(shares_val):,}"
                    else:
                        shares = '-'
                    
                    # 格式化日期
                    if start_date_val:
                        if hasattr(start_date_val, 'strftime'):
                            date_str = start_date_val.strftime('%Y-%m-%d')
                        else:
                            date_str = str(start_date_val)
                    else:
                        date_str = ''
                    
                    results.append({
                        'date': date_str,
                        'ticker': code_val,
                        'company': name_val,
                        'exchange': exchange_val,
                        'deal_type': deal_type_val,
                        'price_range': price_range,
                        'shares': shares
                    })
                except (IndexError, AttributeError, TypeError, ValueError) as e:
                    logger.warning(f"Error processing IPO row: {str(e)}, row data: {row}")
                    continue
            
            cursor.close()
            conn.close()
            
            logger.info(f"Found {len(results)} IPO events")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching IPO calendar: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def get_watchlist_stocks(self, user_id: int) -> List[Dict]:
        """
        获取用户watchlist中的股票
        
        Args:
            user_id: 用户ID
        
        Returns:
            股票列表
        """
        logger.info(f"Fetching watchlist for user {user_id}")
        
        # 使用target_tracking_config表代替watchlist表
        query = """
        SELECT 
            ttc.symbol,
            c.name as company_name,
            COALESCE(ttc.primaryticker, ttc.symbol) as primary_ticker
        FROM target_tracking_config ttc
        LEFT JOIN companies c ON ttc.symbol = c.code
        WHERE ttc.user_id = %s AND ttc.enabled = true
        ORDER BY ttc.symbol;
        """
        
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (user_id,))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'symbol': row[0] or '',
                    'company_name': row[1] or row[0],  # 如果没有公司名，使用symbol
                    'primary_ticker': row[2] or row[0]
                })
            
            cursor.close()
            conn.close()
            
            logger.info(f"Found {len(results)} stocks in watchlist")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching watchlist stocks: {str(e)}")
            return []
    
    def refresh_report(self, period: str = 'latest'):
        """
        刷新报告（清除缓存，重新生成）
        
        Args:
            period: 报告周期，'latest' 表示最新一周
        """
        cache_key = f"market_weekly_report:{period}"
        
        # 清除缓存
        self._clear_cache(cache_key)
        
        logger.info(f"Cache cleared for period: {period}, next request will generate fresh report")
    
    def _get_from_cache(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """从缓存获取数据（优先使用Redis，否则使用内存缓存）"""
        try:
            # 尝试使用Redis
            redis_url = config.REDIS_URL
            if redis_url:
                import redis
                redis_client = redis.from_url(redis_url, decode_responses=True)
                cached = redis_client.get(cache_key)
                if cached:
                    data = json.loads(cached)
                    logger.info(f"Retrieved from Redis cache: {cache_key}")
                    return data
        except Exception as e:
            logger.debug(f"Redis cache not available: {str(e)}, using memory cache")
        
        # 使用内存缓存
        if cache_key in _memory_cache:
            cached_data = _memory_cache[cache_key]
            # 检查是否过期
            if time.time() < cached_data['expires_at']:
                logger.info(f"Retrieved from memory cache: {cache_key}")
                return cached_data['data']
            else:
                # 缓存已过期，删除
                del _memory_cache[cache_key]
        
        return None
    
    def _save_to_cache(self, cache_key: str, data: Dict[str, Any], ttl: int = 3600):
        """保存数据到缓存（优先使用Redis，否则使用内存缓存）"""
        try:
            # 尝试使用Redis
            redis_url = config.REDIS_URL
            if redis_url:
                import redis
                redis_client = redis.from_url(redis_url, decode_responses=True)
                redis_client.setex(cache_key, ttl, json.dumps(data))
                logger.info(f"Saved to Redis cache: {cache_key}, TTL: {ttl}s")
                return
        except Exception as e:
            logger.debug(f"Redis cache not available: {str(e)}, using memory cache")
        
        # 使用内存缓存
        _memory_cache[cache_key] = {
            'data': data,
            'expires_at': time.time() + ttl
        }
        logger.info(f"Saved to memory cache: {cache_key}, TTL: {ttl}s")
    
    def _clear_cache(self, cache_key: str):
        """清除缓存"""
        try:
            # 尝试清除Redis缓存
            redis_url = config.REDIS_URL
            if redis_url:
                import redis
                redis_client = redis.from_url(redis_url, decode_responses=True)
                redis_client.delete(cache_key)
                logger.info(f"Cleared Redis cache: {cache_key}")
        except Exception as e:
            logger.debug(f"Redis cache not available: {str(e)}")
        
        # 清除内存缓存
        if cache_key in _memory_cache:
            del _memory_cache[cache_key]
            logger.info(f"Cleared memory cache: {cache_key}")
    
    async def get_ohlc_data_batch(self, symbols: List[str], days: int = 90, request=None, data_source: str = 'auto') -> Dict[str, Any]:
        """
        批量获取OHLC原始数据（用于前端渲染图表，支持请求中断检测）
        
        Args:
            symbols: 股票代码列表，格式为 [{"symbol": "SPY.US", "label": "S&P 500"}, ...]
            days: 天数
            request: FastAPI Request对象，用于检测客户端断开
            data_source: 数据源选择
                - 'market': 优先使用 market_data_eod (用于 indices, sectors, forex, commodities, bonds)
                - 'stock': 优先使用 stock_prices_eod (用于个股数据)
                - 'auto': 自动判断（默认，向后兼容）
        
        Returns:
            {
                "status": "completed" | "cancelled",
                "processed": int,
                "total": int,
                "results": [
                    {
                        "symbol": "SPY.US",
                        "label": "S&P 500",
                        "data": {
                            "ohlc": [{time, open, high, low, close, volume}],
                            "ema20": [{time, value}],
                            "ema50": [{time, value}],
                            "ema200": [{time, value}]
                        },
                        "success": true,
                        "error": null
                    }
                ]
            }
        """
        import pandas as pd
        from datetime import datetime, timedelta
        
        results = []
        total = len(symbols)
        
        logger.info(f"[Batch OHLC] Starting batch data fetch for {total} symbols")
        
        for i, symbol_info in enumerate(symbols):
            # 🔥 检测客户端是否断开连接
            if request:
                is_disconnected = await request.is_disconnected()
                if is_disconnected:
                    logger.info(f"[Batch OHLC] Client disconnected. Processed {i}/{total}, stopping.")
                    return {
                        "status": "cancelled",
                        "processed": i,
                        "total": total,
                        "results": results
                    }
            
            symbol = symbol_info.get('symbol') if isinstance(symbol_info, dict) else symbol_info
            label = symbol_info.get('label', symbol) if isinstance(symbol_info, dict) else symbol
            
            try:
                logger.debug(f"[Batch OHLC] Fetching data {i+1}/{total}: {symbol}")
                
                # 获取历史数据
                end_date = utcnow()
                start_date = end_date - timedelta(days=days)
                
                conn = self.db_manager.get_connection()
                cursor = conn.cursor()
                
                rows = None
                
                # 根据 data_source 参数决定查询哪个表
                if data_source == 'market':
                    # 优先使用 market_data_eod (用于 indices, sectors, forex, commodities, bonds)
                    query_market = """
                    SELECT date, open, high, low, close, volume
                    FROM market_data_eod
                    WHERE code = %s 
                    AND date >= %s 
                    AND date <= %s
                    ORDER BY date ASC
                    """
                    cursor.execute(query_market, (symbol, start_date, end_date))
                    rows = cursor.fetchall()
                    
                elif data_source == 'stock':
                    # 优先使用 stock_prices_eod (用于个股数据)
                    query_stock = """
                    SELECT date, open, high, low, close, volume
                    FROM stock_prices_eod
                    WHERE code = %s 
                    AND date >= %s 
                    AND date <= %s
                    AND period = 'd'
                    ORDER BY date ASC
                    """
                    cursor.execute(query_stock, (symbol, start_date, end_date))
                    rows = cursor.fetchall()
                    
                else:  # data_source == 'auto' (默认，向后兼容)
                    # 首先尝试从 stock_prices_eod 表获取数据
                    query_stock = """
                    SELECT date, open, high, low, close, volume
                    FROM stock_prices_eod
                    WHERE code = %s 
                    AND date >= %s 
                    AND date <= %s
                    AND period = 'd'
                    ORDER BY date ASC
                    """
                    cursor.execute(query_stock, (symbol, start_date, end_date))
                    rows = cursor.fetchall()
                    
                    # 如果没有数据，尝试从 market_data_eod 表获取
                    if not rows:
                        query_market = """
                        SELECT date, open, high, low, close, volume
                        FROM market_data_eod
                        WHERE code = %s 
                        AND date >= %s 
                        AND date <= %s
                        ORDER BY date ASC
                        """
                        cursor.execute(query_market, (symbol, start_date, end_date))
                        rows = cursor.fetchall()
                
                cursor.close()
                conn.close()
                
                if not rows:
                    results.append({
                        "symbol": symbol,
                        "label": label,
                        "data": None,
                        "success": False,
                        "error": "No data available"
                    })
                    continue
                
                # 转换为DataFrame计算EMA
                df = pd.DataFrame(rows, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
                df['date'] = pd.to_datetime(df['date'])
                df = df.astype({'open': float, 'high': float, 'low': float, 'close': float, 'volume': float})
                
                # 计算EMA
                df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
                df['ema50'] = df['close'].ewm(span=50, adjust=False).mean()
                df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
                
                # 转换为前端需要的格式
                ohlc_data = []
                ema20_data = []
                ema50_data = []
                ema200_data = []
                
                for _, row in df.iterrows():
                    timestamp = int(row['date'].timestamp())
                    
                    ohlc_data.append({
                        "time": timestamp,
                        "open": round(float(row['open']), 4),
                        "high": round(float(row['high']), 4),
                        "low": round(float(row['low']), 4),
                        "close": round(float(row['close']), 4),
                        "volume": int(row['volume']) if pd.notna(row['volume']) else 0
                    })
                    
                    if pd.notna(row['ema20']):
                        ema20_data.append({"time": timestamp, "value": round(float(row['ema20']), 4)})
                    if pd.notna(row['ema50']):
                        ema50_data.append({"time": timestamp, "value": round(float(row['ema50']), 4)})
                    if pd.notna(row['ema200']):
                        ema200_data.append({"time": timestamp, "value": round(float(row['ema200']), 4)})
                
                results.append({
                    "symbol": symbol,
                    "label": label,
                    "data": {
                        "ohlc": ohlc_data,
                        "ema20": ema20_data,
                        "ema50": ema50_data,
                        "ema200": ema200_data
                    },
                    "success": True,
                    "error": None
                })
                
            except Exception as e:
                logger.error(f"[Batch OHLC] Error fetching data for {symbol}: {str(e)}")
                results.append({
                    "symbol": symbol,
                    "label": label,
                    "data": None,
                    "success": False,
                    "error": str(e)
                })
            
            # 每5个输出一次进度
            if (i + 1) % 5 == 0:
                logger.info(f"[Batch OHLC] Progress: {i+1}/{total} processed")
        
        logger.info(f"[Batch OHLC] Completed: {total}/{total} processed")
        
        return {
            "status": "completed",
            "processed": total,
            "total": total,
            "results": results
        }
    
    async def generate_charts_batch(self, symbols: List[str], days: int = 90, request=None) -> List[Dict[str, Any]]:
        """
        批量生成candlestick图表图片（支持请求中断检测）
        
        Args:
            symbols: 股票代码列表，格式为 [{"symbol": "SPY.US", "label": "S&P 500"}, ...]
            days: 天数
            request: FastAPI Request对象，用于检测客户端断开
        
        Returns:
            图表数据列表，每个元素包含 {symbol, label, data, success, error}
        """
        import asyncio
        
        results = []
        total = len(symbols)
        
        logger.info(f"[Batch Chart] Starting batch generation for {total} symbols")
        
        for i, symbol_info in enumerate(symbols):
            # 🔥 检测客户端是否断开连接
            if request:
                is_disconnected = await request.is_disconnected()
                if is_disconnected:
                    logger.info(f"[Batch Chart] Client disconnected. Processed {i}/{total}, stopping.")
                    return {
                        "status": "cancelled",
                        "processed": i,
                        "total": total,
                        "results": results
                    }
            
            symbol = symbol_info.get('symbol') if isinstance(symbol_info, dict) else symbol_info
            label = symbol_info.get('label', symbol) if isinstance(symbol_info, dict) else symbol
            
            try:
                logger.debug(f"[Batch Chart] Generating chart {i+1}/{total}: {symbol}")
                chart_data = self.generate_chart_image(symbol=symbol, label=label, days=days)
                
                results.append({
                    "symbol": symbol,
                    "label": label,
                    "data": chart_data,
                    "success": chart_data is not None,
                    "error": None if chart_data else "No data available"
                })
            except Exception as e:
                logger.error(f"[Batch Chart] Error generating chart for {symbol}: {str(e)}")
                results.append({
                    "symbol": symbol,
                    "label": label,
                    "data": None,
                    "success": False,
                    "error": str(e)
                })
            
            # 每5个chart输出一次进度
            if (i + 1) % 5 == 0:
                logger.info(f"[Batch Chart] Progress: {i+1}/{total} charts processed")
        
        logger.info(f"[Batch Chart] Completed: {total}/{total} charts processed")
        
        return {
            "status": "completed",
            "processed": total,
            "total": total,
            "results": results
        }
    
    def generate_chart_image(self, symbol: str, label: str, days: int = 90) -> Optional[str]:
        """
        生成candlestick图表图片，样式参考report_debug.py
        
        Args:
            symbol: 股票代码
            label: 图表标题
            days: 天数
        
        Returns:
            base64编码的PNG图片字符串，如果没有数据则返回None
        """
        import pandas as pd
        import matplotlib
        matplotlib.use('Agg')  # 使用非GUI后端
        import matplotlib.pyplot as plt
        import mplfinance as mpf
        from matplotlib.lines import Line2D
        import base64
        import io
        
        logger.info(f"Generating chart for {symbol}, days={days}")
        
        try:
            # 1. 获取历史数据
            end_date = utcnow()
            start_date = end_date - timedelta(days=days)
            
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            rows = []
            
            # 首先尝试从 stock_prices_eod 表获取数据（个股）
            query_stock = """
            SELECT date, open, high, low, close, volume
            FROM stock_prices_eod
            WHERE code = %s 
            AND date >= %s 
            AND date <= %s
            AND period = 'd'
            ORDER BY date ASC
            """
            
            cursor.execute(query_stock, (symbol, start_date, end_date))
            rows = cursor.fetchall()
            
            # 如果没有数据，尝试从 market_data_eod 表获取（指数、外汇、商品、债券）
            if not rows:
                logger.info(f"No data in stock_prices_eod for {symbol}, trying market_data_eod table")
                
                query_market = """
                SELECT date, open, high, low, close, volume
                FROM market_data_eod
                WHERE code = %s 
                AND date >= %s 
                AND date <= %s
                ORDER BY date ASC
                """
                
                cursor.execute(query_market, (symbol, start_date, end_date))
                rows = cursor.fetchall()
                
                if rows:
                    logger.info(f"Found {len(rows)} records in market_data_eod table for {symbol}")
            
            cursor.close()
            conn.close()
            
            if not rows:
                logger.warning(f"No data found for {symbol} in either stock_prices_eod or market_data_eod")
                return None
            
            # 2. 转换为DataFrame
            df = pd.DataFrame(rows, columns=['date', 'open', 'high', 'low', 'close', 'volume'])
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)
            df = df.astype(float)
            
            # 3. 检查是否有OHLC数据
            has_ohlc = all(col in df.columns for col in ['open', 'high', 'low', 'close'])
            
            if not has_ohlc:
                # 如果没有OHLC数据，只有close，使用简单折线图
                plt.rcParams.update({
                    "figure.facecolor": "white",
                    "axes.facecolor": "#FAFAFA",
                    "axes.edgecolor": "#CCCCCC",
                    "axes.linewidth": 1.2,
                    "axes.grid": True,
                    "axes.grid.axis": "y",
                    "grid.color": "#E5E5E5",
                    "grid.linestyle": "-",
                    "grid.linewidth": 0.8,
                    "grid.alpha": 0.6,
                    "font.size": 10,
                    "savefig.dpi": 300
                })
                
                fig, ax = plt.subplots(figsize=(12, 6))
                df['EMA_20'] = df['close'].ewm(span=20, adjust=False).mean()
                df['EMA_50'] = df['close'].ewm(span=50, adjust=False).mean()
                
                ax.plot(df.index, df['close'], color='#2C3E50', linewidth=2, label='Close')
                ax.plot(df.index, df['EMA_20'], color='#BBBBBB', linewidth=1.5, label='EMA 20')
                ax.plot(df.index, df['EMA_50'], color='#AAAAAA', linewidth=1.5, label='EMA 50')
                
                ax.set_ylabel("Price", fontweight="bold", color="#2C3E50")
                ax.set_title(f"{label} — Daily Chart", loc="left", fontsize=11, pad=8, color="#2C3E50", fontweight="bold")
                ax.legend(loc="upper left", fontsize=8, framealpha=0.9, edgecolor="#CCCCCC")
                ax.spines["top"].set_visible(False)
                ax.spines["right"].set_visible(False)
                fig.autofmt_xdate()
                plt.tight_layout()
                
                # 转换为base64
                buffer = io.BytesIO()
                plt.savefig(buffer, format='png', dpi=300, bbox_inches='tight', facecolor='white')
                plt.close(fig)
                buffer.seek(0)
                image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
                return image_base64
            
            # 4. 计算EMA指标
            df['EMA_20'] = df['close'].ewm(span=20, adjust=False).mean()
            df['EMA_50'] = df['close'].ewm(span=50, adjust=False).mean()
            df['EMA_200'] = df['close'].ewm(span=200, adjust=False).mean()
            
            # 5. 设置mplfinance样式（参考report_debug.py）
            ema_colors = ["#BBBBBB", "#AAAAAA", "#999999"]
            ema_plots = [
                mpf.make_addplot(df['EMA_20'], color=ema_colors[0], width=1.2),
                mpf.make_addplot(df['EMA_50'], color=ema_colors[1], width=1.2),
                mpf.make_addplot(df['EMA_200'], color=ema_colors[2], width=1.2)
            ]
            
            # Market colors配置（白色上涨蜡烛，红色下跌蜡烛）
            mc = mpf.make_marketcolors(
                up="#FFFFFF",
                down="#E74C3C",
                edge={"up": "#000000", "down": "#E74C3C"},
                wick={"up": "#000000", "down": "#E74C3C"},
                volume={"up": "#95A5A6", "down": "#F1948A"}
            )
            
            # Style配置
            s = mpf.make_mpf_style(
                marketcolors=mc,
                gridcolor="#E5E5E5",
                gridstyle="-",
                gridaxis="horizontal",
                facecolor="#F8F9FA",
                figcolor="#F8F9FA",
                edgecolor="#CCCCCC",
                y_on_right=True,
                rc={
                    "font.size": 8,
                    "axes.labelsize": 9,
                    "axes.titlesize": 11,
                    "xtick.labelsize": 8,
                    "ytick.labelsize": 8,
                    "legend.fontsize": 8
                }
            )
            
            # 6. 生成图表
            fig, axes = mpf.plot(
                df,
                type="candle",
                style=s,
                title="",
                ylabel="Price",
                figsize=(12, 6),
                returnfig=True,
                show_nontrading=False,
                datetime_format="%b %d",
                xrotation=0,
                scale_padding={"left": 0.05, "right": 0.35, "top": 0.3, "bottom": 0.3},
                update_width_config=dict(candle_linewidth=0.6, candle_width=0.6),
                addplot=ema_plots,
                volume=False
            )
            
            # 7. 设置标题和图例
            for ax in axes:
                ax.margins(x=0)
            
            axes[0].set_title(f"{label} — Daily Chart", loc="left", fontsize=11, pad=8, color="#2C3E50", fontweight="bold")
            
            # 添加图例
            legend_handles = [Line2D([0], [0], color=c, linewidth=2) for c in ema_colors]
            axes[0].legend(
                handles=legend_handles,
                labels=["EMA 20", "EMA 50", "EMA 200"],
                loc="upper left",
                fontsize=8,
                framealpha=0.9,
                edgecolor="#CCCCCC"
            )
            
            # 8. 转换为base64
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png', dpi=300, bbox_inches='tight', facecolor='white')
            plt.close(fig)
            buffer.seek(0)
            image_base64 = base64.b64encode(buffer.getvalue()).decode('utf-8')
            
            logger.info(f"Chart generated successfully for {symbol}")
            return image_base64
            
        except Exception as e:
            logger.error(f"Error generating chart for {symbol}: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def _calculate_ai_related_performance(self, week_start: datetime, week_end: datetime, days: int = 90, user_id: str = None) -> tuple:
        """
        计算AI Related主题的WoW和相对表现（分US和China市场）
        
        🔥 从数据库获取用户个性化的 'Market Weekly' 标签主题，通过theme_company_associations获取相关ticker，
        然后从stock_prices_eod获取价格数据进行计算
        
        US市场: market = 'US'
        China市场: market = 'HK' 或 'CN'
        
        Args:
            week_start: 起始日期
            week_end: 结束日期
            days: 相对表现回溯天数
            user_id: 用户ID，用于筛选用户个性化的标签
        
        Returns:
            (us_ai_wow, us_ai_relative, china_ai_wow, china_ai_relative) 四元组
        """
        logger.info(f"Calculating AI Related performance from database themes (US and China markets) for user: {user_id}")
        
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            # 🔥 1. 获取用户个性化的Market Weekly主题及其关联的ticker
            if user_id:
                theme_query = """
                    SELECT DISTINCT
                    ite.theme_id,
                    ite.theme_name,
                    ite.category,
                    tca.ticker,
                    tca.market,
                    tca.company_name
                FROM investment_theme_embeddings ite
                INNER JOIN theme_company_associations tca ON ite.theme_id = tca.theme_id
                    INNER JOIN theme_tags tt ON ite.theme_id = tt.theme_id
                    WHERE tt.tag_name = 'Market Weekly'
                      AND tt.user_id = %s
                  AND tca.is_verified = TRUE
                ORDER BY ite.theme_name, tca.market, tca.ticker;
                """
                cursor.execute(theme_query, (user_id,))
            else:
                # 如果没有user_id，返回空结果（个性化标签必须指定用户）
                logger.warning("No user_id provided for personalized Market Weekly themes")
                return [], [], [], []
            
            theme_tickers = cursor.fetchall()
            
            if not theme_tickers:
                logger.warning(f"No Market Weekly AI themes found for user: {user_id}")
                return [], [], [], []
            
            # 2. 按主题和市场分组ticker
            # US市场: market = 'US'
            # China市场: market = 'HK' 或 'CN'
            us_themes_map = {}
            china_themes_map = {}
            
            for row in theme_tickers:
                theme_id, theme_name, category, ticker, market, company_name = row
                
                # 判断市场归属
                if market == 'US':
                    target_map = us_themes_map
                elif market in ('HK', 'CN'):
                    target_map = china_themes_map
                else:
                    continue  # 跳过其他市场
                
                if theme_name not in target_map:
                    target_map[theme_name] = {
                        'theme_id': theme_id,
                        'category': category,
                        'tickers': []
                    }
                target_map[theme_name]['tickers'].append(ticker)
            
            logger.info(f"Found {len(us_themes_map)} US AI themes and {len(china_themes_map)} China AI themes")
            
            # 3. 计算US市场的WoW和相对表现
            us_ai_wow = self._calculate_market_wow(us_themes_map, week_start, week_end, cursor)
            us_ai_relative = self._calculate_market_relative(us_themes_map, days, cursor)
            
            # 4. 计算China市场的WoW和相对表现
            china_ai_wow = self._calculate_market_wow(china_themes_map, week_start, week_end, cursor)
            china_ai_relative = self._calculate_market_relative(china_themes_map, days, cursor)
            
            cursor.close()
            conn.close()
            
            logger.info(f"US AI: {len(us_ai_wow)} themes WoW, {len(us_ai_relative)} days relative")
            logger.info(f"China AI: {len(china_ai_wow)} themes WoW, {len(china_ai_relative)} days relative")
            
            return us_ai_wow, us_ai_relative, china_ai_wow, china_ai_relative
            
        except Exception as e:
            logger.error(f"Error calculating AI related performance: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return [], [], [], []
    
    def _calculate_market_wow(self, themes_map: dict, week_start: datetime, week_end: datetime, cursor) -> list:
        """
        计算某个市场所有主题的WoW
        
        优化逻辑：与report_debug.py逻辑一致
        1. 获取所有股票的原始价格数据
        2. 使用ffill/bfill填充缺失值（确保数据连续性）
        3. 计算每日平均价格（所有股票的横向平均）
        4. 对比week_start和week_end两个周五的平均价格，计算WoW
        """
        import pandas as pd
        
        market_wow = []
        
        for theme_name, theme_data in themes_map.items():
            tickers = theme_data['tickers']
            
            if not tickers:
                continue
            
            # 1. 获取所有股票的原始数据（包含week_start前后30天，确保有足够数据用于填充）
            query = """
            SELECT code, date::date as trade_date, close
            FROM stock_prices_eod
            WHERE code = ANY(%s)
              AND date BETWEEN %s - INTERVAL '30 days' AND %s
              AND close IS NOT NULL
            ORDER BY code, date
            """
            
            cursor.execute(query, (tickers, week_start.date(), week_end.date()))
            rows = cursor.fetchall()
            
            if not rows:
                logger.warning(f"No WoW data found for theme '{theme_name}' with {len(tickers)} tickers")
                continue
            
            # 2. 转换为DataFrame
            df = pd.DataFrame(rows, columns=['code', 'date', 'close'])
            
            # 2.5. 🔥 关键：去重处理（如果同一股票同一天有多条记录，取最新的）
            df = df.sort_values('date').drop_duplicates(subset=['code', 'date'], keep='last')
            
            # 3. Pivot成wide format (每只股票一列)
            pivot_df = df.pivot(index='date', columns='code', values='close')
            
            # 4. 使用ffill/bfill填充缺失值（模拟report_debug.py的逻辑）
            pivot_df = pivot_df.ffill().bfill()
            
            # 5. 计算每日平均价格（所有股票的横向平均）
            # 与report_debug.py中的 us_panel.mean(axis=1) 逻辑一致
            daily_avg = pivot_df.mean(axis=1)
            
            # 6. 获取week_start和week_end的平均价格
            try:
                # 找到最接近week_start的日期（<=week_start的最大日期）
                start_dates = daily_avg.index[daily_avg.index <= week_start.date()]
                if len(start_dates) == 0:
                    logger.warning(f"No data before week_start for theme '{theme_name}'")
                    continue
                start_price = daily_avg[start_dates[-1]]
                
                # 找到最接近week_end的日期（<=week_end的最大日期）
                end_dates = daily_avg.index[daily_avg.index <= week_end.date()]
                if len(end_dates) == 0:
                    logger.warning(f"No data before week_end for theme '{theme_name}'")
                    continue
                end_price = daily_avg[end_dates[-1]]
                
                # 7. 计算WoW变化率
                if start_price > 0:
                    change_pct = ((end_price - start_price) / start_price * 100)
                    market_wow.append({
                        'name': theme_name,
                        'change_pct': round(float(change_pct), 2)
                    })
                    
                    logger.debug(f"Theme '{theme_name}' WoW: {start_price:.2f} -> {end_price:.2f} = {change_pct:+.2f}%")
                else:
                    logger.warning(f"Theme '{theme_name}' has invalid start price: {start_price}")
            
            except Exception as e:
                logger.error(f"Error calculating WoW for theme '{theme_name}': {str(e)}")
                continue
        
        # 按涨跌幅排序
        market_wow.sort(key=lambda x: x['change_pct'], reverse=True)
        
        logger.info(f"Calculated WoW for {len(market_wow)} themes")
        return market_wow
    
    def _calculate_market_relative(self, themes_map: dict, days: int, cursor) -> list:
        """
        计算某个市场所有主题的相对表现
        
        优化逻辑：与report_debug.py保持一致
        1. 获取所有股票的原始价格数据
        2. 使用ffill/bfill填充缺失值（模拟停牌期间价格不变）
        3. 计算每日平均价格（确保每天都基于相同数量的股票）
        4. 以第一个交易日为基准（100），计算相对表现
        """
        import pandas as pd
        
        market_relative = []
        
        for theme_name, theme_data in themes_map.items():
            tickers = theme_data['tickers']
            
            if not tickers:
                continue
            
            # 1. 获取所有股票的原始数据（每只股票的每日收盘价）
            query = """
            SELECT code, date::date as trade_date, close
            FROM stock_prices_eod
            WHERE code = ANY(%s)
              AND date >= CURRENT_DATE - %s
              AND close IS NOT NULL
            ORDER BY code, date
            """
            
            cursor.execute(query, (tickers, days))
            rows = cursor.fetchall()
            
            if not rows:
                logger.warning(f"No data found for theme '{theme_name}' with {len(tickers)} tickers")
                continue
            
            # 2. 转换为DataFrame
            df = pd.DataFrame(rows, columns=['code', 'date', 'close'])
            
            # 2.5. 🔥 关键：去重处理（如果同一股票同一天有多条记录，取最新的）
            # 这可能发生在数据导入时有重复，或者有数据修正
            df = df.sort_values('date').drop_duplicates(subset=['code', 'date'], keep='last')
            
            # 3. Pivot成wide format (每只股票一列，每个日期一行)
            # 这样可以清晰地看到哪些日期哪些股票缺失数据
            pivot_df = df.pivot(index='date', columns='code', values='close')
            
            # 4. 使用ffill填充缺失值（模拟report_debug.py的逻辑）
            # ffill: 用前一个有效值填充（假设停牌期间价格不变）
            # bfill: 如果开头有缺失，用后一个有效值填充
            pivot_df = pivot_df.ffill().bfill()
            
            # 5. 计算每日平均价格（所有股票的横向平均）
            # 与report_debug.py中的 us_panel.mean(axis=1) 逻辑一致
            daily_avg = pivot_df.mean(axis=1)
            
            # 6. 计算相对表现（indexed to 100）
            if len(daily_avg) > 0:
                first_value = daily_avg.iloc[0]  # 第一个交易日的平均价格
                if first_value > 0:
                    # 以第一天为基准100，计算后续每天的相对表现
                    indexed_values = (daily_avg / first_value * 100).round(2)
                    
                    # 7. 添加到结果中
                    for date, value in indexed_values.items():
                        date_str = date.strftime('%Y-%m-%d')
                        
                        # 查找或创建该日期的记录
                        date_record = next((r for r in market_relative if r['date'] == date_str), None)
                        if not date_record:
                            date_record = {'date': date_str}
                            market_relative.append(date_record)
                        
                        date_record[theme_name] = float(value)
                    
                    logger.debug(f"Theme '{theme_name}': {len(indexed_values)} days, "
                               f"from {indexed_values.iloc[0]:.2f} to {indexed_values.iloc[-1]:.2f}")
                else:
                    logger.warning(f"Theme '{theme_name}' has invalid first value: {first_value}")
            else:
                logger.warning(f"Theme '{theme_name}' has no valid data after processing")
        
        # 按日期排序
        market_relative.sort(key=lambda x: x['date'])
        
        logger.info(f"Calculated relative performance for {len(market_relative)} days")
        return market_relative
    
    def _get_my_themes_stocks(self, user_id: str = None) -> Dict[str, Any]:
        """
        获取My Themes的个股数据（用于Technical Views章节的个股走势图）
        
        从investment_theme_embeddings表获取group='Market Weekly'的主题，
        通过theme_company_associations获取关联的ticker列表，
        按US Stock和China Stock分类
        
        Returns:
            {
                "us_stocks": [
                    {
                        "ticker": "NVDA.US",
                        "company_name": "NVIDIA Corporation",
                        "theme_name": "AI Chips",
                        "market": "US"
                    },
                    ...
                ],
                "china_stocks": [
                    {
                        "ticker": "0700.HK",
                        "company_name": "Tencent Holdings Ltd",
                        "theme_name": "Cloud & Platform",
                        "market": "HK"
                    },
                    ...
                ]
            }
        """
        logger.info(f"Fetching My Themes stocks for individual ticker charts for user: {user_id}")
        
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            # 🔥 获取用户个性化的Market Weekly主题及其关联的ticker
            if user_id:
                query = """
                    SELECT DISTINCT
                    ite.theme_id,
                    ite.theme_name,
                    ite.category,
                    tca.ticker,
                    tca.market,
                    tca.company_name
                FROM investment_theme_embeddings ite
                INNER JOIN theme_company_associations tca ON ite.theme_id = tca.theme_id
                    INNER JOIN theme_tags tt ON ite.theme_id = tt.theme_id
                    WHERE tt.tag_name = 'Market Weekly'
                      AND tt.user_id = %s
                  AND tca.is_verified = TRUE
                ORDER BY ite.theme_name, tca.market, tca.ticker;
                """
                cursor.execute(query, (user_id,))
            else:
                # 如果没有user_id，返回空结果（个性化标签必须指定用户）
                logger.warning("No user_id provided for personalized Market Weekly themes")
                cursor.close()
                conn.close()
                return {"us_stocks": [], "china_stocks": []}
            
            rows = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            if not rows:
                logger.warning(f"No Market Weekly themes found for user: {user_id}")
                return {"us_stocks": [], "china_stocks": []}
            
            # 按市场分类
            us_stocks = []
            china_stocks = []
            
            for row in rows:
                theme_id, theme_name, category, ticker, market, company_name = row
                
                stock_info = {
                    "ticker": ticker,
                    "company_name": company_name or ticker,
                    "theme_name": theme_name,
                    "theme_id": theme_id,
                    "category": category,
                    "market": market
                }

                # 判断市场归属
                if market == 'US':
                    us_stocks.append(stock_info)
                elif market in ('HK', 'CN'):
                    china_stocks.append(stock_info)

            logger.info(f"Found {len(us_stocks)} US stocks and {len(china_stocks)} China stocks in My Themes")

            return {
                "us_stocks": us_stocks,
                "china_stocks": china_stocks
            }

        except Exception as e:
            logger.error(f"Error fetching My Themes stocks: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return {"us_stocks": [], "china_stocks": []}

    # =====================================================
    # 静态 HTML 报告生成与 OSS 上传（新增）
    # =====================================================

    _TZ_DISPLAY_LABELS: Dict[str, str] = {
        "America/New_York": "US Eastern (New York)",
        "Asia/Shanghai": "China (Beijing)",
    }

    def _build_generation_time_display(
        self,
        generated_at_utc: datetime,
        scheduler_timezone: Optional[str] = None,
    ) -> Dict[str, Optional[str]]:
        """Format report generation instant in NY, Beijing, and optional scheduler TZ."""
        if generated_at_utc.tzinfo is None:
            generated_at_utc = generated_at_utc.replace(tzinfo=timezone.utc)

        def _fmt_in_zone(dt: datetime, tz_name: str, place_label: str) -> str:
            local = dt.astimezone(ZoneInfo(tz_name))
            return f"{local.strftime('%b %d, %Y %H:%M %Z')} ({place_label})"

        out: Dict[str, Optional[str]] = {
            "generated_at_utc": generated_at_utc.strftime("%Y-%m-%d %H:%M UTC"),
            "generated_at_new_york": _fmt_in_zone(
                generated_at_utc, "America/New_York", "New York"
            ),
            "generated_at_beijing": _fmt_in_zone(
                generated_at_utc, "Asia/Shanghai", "Beijing"
            ),
            "scheduler_timezone": scheduler_timezone,
            "scheduler_timezone_label": None,
            "generated_at_scheduler_tz": None,
        }

        if scheduler_timezone:
            try:
                local = generated_at_utc.astimezone(ZoneInfo(scheduler_timezone))
                out["scheduler_timezone_label"] = self._TZ_DISPLAY_LABELS.get(
                    scheduler_timezone,
                    scheduler_timezone.replace("_", " "),
                )
                out["generated_at_scheduler_tz"] = local.strftime("%b %d, %Y %H:%M %Z")
            except Exception:
                out["scheduler_timezone_label"] = scheduler_timezone

        return out

    def generate_and_upload(
        self,
        user_id: str,
        week_end: Optional[date] = None,
        scheduler_timezone: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        为指定用户生成 Market Weekly HTML 报告，上传 OSS，写入数据库。

        流程:
        1. 计算周度时间范围
        2. 创建 pending 记录（market_weekly_reports 表）
        3. 调用现有 generate_report() 获取数据
        4. 获取 OHLC 数据用于 K 线图
        5. 渲染 Jinja2 HTML 模板
        6. 上传至 OSS（reports/MarketWeekly/{user_id}/...）
        7. 更新数据库状态为 completed
        8. 返回结果（含 share_token）

        Args:
            user_id: 用户标识
            week_end: 周结束日期（周五），默认使用最新周五
            scheduler_timezone: 调度器时区（可选）

        Returns:
            dict with keys: success, oss_object_name, share_token, week_start, week_end, stats
        """
        start_time = time.time()

        # 1. 计算周度时间范围
        if week_end is None:
            # 使用默认逻辑获取最新周五
            week_start_dt, week_end_dt = self._calculate_week_range('latest')
            week_start = week_start_dt.date()
            week_end = week_end_dt.date()
        else:
            # 使用指定的 week_end
            week_start = week_end - timedelta(days=7)
            week_start_dt = datetime.combine(week_start, datetime.min.time())
            week_end_dt = datetime.combine(week_end, datetime.max.time())

        logger.info(f"[generate_and_upload] user={user_id}, week={week_start} to {week_end}")

        # 2. 创建 pending 记录
        share_token = secrets.token_urlsafe(16)[:32]
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()

            # 检查是否已存在该周的记录
            cursor.execute("""
                SELECT id, status, oss_object_name, share_token
                FROM market_weekly_reports
                WHERE user_id = %s AND week_end = %s
            """, (user_id, week_end))
            existing = cursor.fetchone()

            if existing:
                report_id = existing[0]
                existing_status = existing[1]
                # 如果已完成，直接返回
                if existing_status == 'completed' and existing[2]:
                    logger.info(f"[generate_and_upload] Found existing completed report id={report_id}")
                    cursor.close()
                    conn.close()
                    return {
                        "success": True,
                        "report_id": report_id,
                        "oss_object_name": existing[2],
                        "share_token": existing[3],
                        "week_start": str(week_start),
                        "week_end": str(week_end),
                        "stats": {"existing": True},
                    }
                # 否则更新状态为 generating
                cursor.execute("""
                    UPDATE market_weekly_reports
                    SET status = 'generating', updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (report_id,))
            else:
                # 创建新记录
                cursor.execute("""
                    INSERT INTO market_weekly_reports
                    (user_id, week_start, week_end, share_token, status)
                    VALUES (%s, %s, %s, %s, 'generating')
                    RETURNING id
                """, (user_id, week_start, week_end, share_token))
                report_id = cursor.fetchone()[0]

            conn.commit()
            cursor.close()
            conn.close()

        except Exception as e:
            logger.error(f"[generate_and_upload] Failed to create DB record: {e}")
            return {"success": False, "error": str(e)}

        # 3. 生成报告数据（不使用缓存）
        try:
            # 获取报告 JSON 数据
            report_data = self.generate_report(
                period='latest',
                use_cache=False,
                user_id=user_id
            )

            # 更新 week_start/week_end 使用计算出的值
            report_data['meta']['week_start'] = week_start.strftime('%Y-%m-%d')
            report_data['meta']['week_end'] = week_end.strftime('%Y-%m-%d')

            # 4. 获取 OHLC 数据用于 K 线图（使用 Theme Report 格式）
            # 技术视图中的资产
            technical_views = report_data.get('technical_views', {})
            chart_tickers = []
            for asset_type in ['indices', 'sectors', 'forex', 'commodities', 'yields']:
                for asset in technical_views.get(asset_type, []):
                    chart_tickers.append(asset.get('symbol'))

            # My Themes 中的股票
            my_themes = report_data.get('my_themes_stocks', {})
            for stock in my_themes.get('us_stocks', []):
                chart_tickers.append(stock.get('ticker'))
            for stock in my_themes.get('china_stocks', []):
                chart_tickers.append(stock.get('ticker'))

            # 使用 Theme Report 格式的数据获取方法
            stock_chart_data = {}
            if chart_tickers:
                try:
                    conn = self.db_manager.get_connection()
                    cursor = conn.cursor()
                    stock_chart_data = self._fetch_stock_chart_data(
                        tickers=chart_tickers,
                        days=380,
                        cursor=cursor
                    )
                    cursor.close()
                    conn.close()
                    logger.info(f"[generate_and_upload] Stock chart data prepared for {len(stock_chart_data)} tickers")
                except Exception as e:
                    logger.warning(f"[generate_and_upload] Failed to get stock chart data: {e}")

            # 5. 渲染 HTML
            generated_at = utcnow()
            generation_time_display = self._build_generation_time_display(
                generated_at, scheduler_timezone
            )

            html_content = self._render_html(
                report_data=report_data,
                stock_chart_data=stock_chart_data,
                generation_time_display=generation_time_display,
            )

            # 6. 上传 OSS
            oss_service = OSSService()
            timestamp = generated_at.strftime("%Y%m%dT%H%M%SZ")
            oss_object_name = f"reports/MarketWeekly/{user_id}/report_{week_end.strftime('%Y%m%d')}_{timestamp}.html"

            upload_result = oss_service.upload_bytes(
                object_name=oss_object_name,
                data=html_content.encode('utf-8'),
                content_type='text/html; charset=utf-8',
                content_disposition='inline'
            )

            if not upload_result.get('success'):
                raise Exception(f"OSS upload failed: {upload_result.get('error')}")

            logger.info(f"[generate_and_upload] Uploaded to OSS: {oss_object_name}")

            # 7. 更新数据库状态为 completed
            elapsed = time.time() - start_time
            try:
                conn = self.db_manager.get_connection()
                cursor = conn.cursor()

                metadata = {
                    "generation_time_seconds": round(elapsed, 2),
                    "week_start": str(week_start),
                    "week_end": str(week_end),
                    "generated_at_utc": generated_at.strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "assets_count": len(chart_tickers),
                    "stock_chart_tickers_count": len(stock_chart_data),
                }

                cursor.execute("""
                    UPDATE market_weekly_reports
                    SET status = 'completed',
                        oss_object_name = %s,
                        metadata = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (oss_object_name, json.dumps(metadata), report_id))

                conn.commit()
                cursor.close()
                conn.close()

            except Exception as e:
                logger.error(f"[generate_and_upload] Failed to update DB: {e}")

            # 8. 返回结果
            return {
                "success": True,
                "report_id": report_id,
                "oss_object_name": oss_object_name,
                "share_token": share_token,
                "week_start": str(week_start),
                "week_end": str(week_end),
                "stats": {
                    "generation_time_seconds": round(elapsed, 2),
                    "assets_count": len(chart_tickers),
                    "stock_chart_tickers_count": len(stock_chart_data),
                },
            }

        except Exception as e:
            logger.error(f"[generate_and_upload] Generation failed: {e}")
            import traceback
            logger.error(traceback.format_exc())

            # 更新数据库状态为 failed
            try:
                conn = self.db_manager.get_connection()
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE market_weekly_reports
                    SET status = 'failed', error_message = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                """, (str(e), report_id))
                conn.commit()
                cursor.close()
                conn.close()
            except Exception:
                pass

            return {"success": False, "error": str(e), "report_id": report_id}

    def _render_html(
        self,
        report_data: Dict[str, Any],
        stock_chart_data: Dict[str, List[List[Any]]],
        generation_time_display: Dict[str, Any],
    ) -> str:
        """
        使用 Jinja2 模板渲染 Market Weekly HTML 报告。

        Args:
            report_data: 报告 JSON 数据
            stock_chart_data: Theme Report 格式的股票图表数据 {ticker: [[date, o, h, l, c, vol], ...]}
            generation_time_display: 时间显示配置

        Returns:
            渲染后的 HTML 字符串
        """
        import json
        template = _jinja_env.get_template('market_weekly_report.html.j2')

        # 构建模板上下文
        context = {
            "meta": report_data.get('meta', {}),
            "executive_summary": report_data.get('executive_summary', {}),
            "weekly_performance": report_data.get('weekly_performance', {}),
            "technical_views": report_data.get('technical_views', {}),
            "my_themes_stocks": report_data.get('my_themes_stocks', {}),
            "next_week_outlook": report_data.get('next_week_outlook', {}),
            "stock_chart_data": stock_chart_data,
            "stock_chart_data_json": json.dumps(stock_chart_data),
            "generation_time": generation_time_display,
        }

        html = template.render(**context)
        return html

    def _fetch_stock_chart_data(
        self,
        tickers: List[str],
        days: int = 380,
        cursor=None,
    ) -> Dict[str, List[List[Any]]]:
        """
        获取股票图表数据（Theme Report 格式）。

        同时查询 stock_prices_eod 和 market_data_eod 表：
        - 个股数据从 stock_prices_eod 获取
        - 指数、板块、外汇、商品、债券从 market_data_eod 获取

        Args:
            tickers: 股票代码列表
            days: 回溯天数
            cursor: 数据库 cursor（可选，如不提供则新建连接）

        Returns:
            {ticker: [[date, open, high, low, close, volume], ...]}
        """
        if not tickers:
            return {}

        from datetime import timedelta
        as_of = utc_today()
        lookback_start = as_of - timedelta(days=days)

        need_close_conn = False
        if cursor is None:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            need_close_conn = True

        try:
            # 1. 从 stock_prices_eod 获取个股数据
            cursor.execute("""
                SELECT code, date::date AS trade_date,
                       open, high, low, close, volume
                FROM stock_prices_eod
                WHERE code = ANY(%s)
                  AND date BETWEEN %s AND %s
                  AND period = 'd'
                  AND close IS NOT NULL
                ORDER BY code, date
            """, (tickers, lookback_start, as_of))

            rows = cursor.fetchall()

            # 2. 如果个股数据不足，从 market_data_eod 获取（指数、板块等）
            found_codes = set(row[0] for row in rows)
            missing_codes = [t for t in tickers if t not in found_codes]

            if missing_codes:
                cursor.execute("""
                    SELECT code, date::date AS trade_date,
                           open, high, low, close, volume
                    FROM market_data_eod
                    WHERE code = ANY(%s)
                      AND date BETWEEN %s AND %s
                      AND close IS NOT NULL
                    ORDER BY code, date
                """, (missing_codes, lookback_start, as_of))

                rows.extend(cursor.fetchall())

            if not rows:
                logger.warning("[_fetch_stock_chart_data] No OHLCV data found")
                return {}

            by_code: Dict[str, List[List[Any]]] = {}
            for code, trade_date, o, h, l, c, vol in rows:
                close = float(c)
                open_p = float(o) if o is not None else close
                high_p = float(h) if h is not None else close
                low_p = float(l) if l is not None else close
                volume = int(vol) if vol is not None else 0
                bar = [
                    str(trade_date),
                    open_p,
                    high_p,
                    low_p,
                    close,
                    volume,
                ]
                # 使用简化的 ticker key（去掉 .US/.HK）
                key = code.replace('.US', '').replace('.HK', '')
                by_code.setdefault(key, []).append(bar)

            # 限制最多 260 条
            chart_data: Dict[str, List[List[Any]]] = {}
            for key, bars in by_code.items():
                if len(bars) < 2:
                    continue
                chart_data[key] = bars[-260:]

            logger.info(
                f"[_fetch_stock_chart_data] Prepared data for {len(chart_data)} tickers "
                f"({sum(len(v) for v in chart_data.values())} bars total)"
            )
            return chart_data

        finally:
            if need_close_conn:
                cursor.close()
                conn.close()

    def get_user_reports(
        self, user_id, limit: int = 30, offset: int = 0
    ) -> Tuple[List[Dict[str, Any]], int]:
        """分页获取用户 Market Weekly 报告列表（供 Agent API 使用）。

        每条记录不含正文 —— 正文存 OSS，通过 share_url 访问。
        user_id 内部归一化为 str（表列为 VARCHAR，实际存 username）。
        """
        user_id = str(user_id)
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM market_weekly_reports WHERE user_id = %s",
                (user_id,),
            )
            total = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT id, week_start, week_end, status, metadata, created_at, share_token
                FROM market_weekly_reports
                WHERE user_id = %s
                ORDER BY week_end DESC
                LIMIT %s OFFSET %s
                """,
                (user_id, limit, offset),
            )
            columns = [
                "id", "week_start", "week_end", "status",
                "metadata", "created_at", "share_token",
            ]
            reports = []
            for row in cursor.fetchall():
                d = dict(zip(columns, row))
                if d.get("week_start"):
                    d["week_start"] = str(d["week_start"])
                if d.get("week_end"):
                    d["week_end"] = str(d["week_end"])
                if d.get("created_at"):
                    d["created_at"] = d["created_at"].isoformat()
                reports.append(d)
            return reports, total
        finally:
            cursor.close()
            conn.close()

    def get_report_by_date(self, user_id, week_end) -> Optional[Dict[str, Any]]:
        """获取指定 week_end 的已完成 Market Weekly 报告（供 Agent API 使用）。"""
        user_id = str(user_id)
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                SELECT id, week_start, week_end, status, metadata, created_at, share_token
                FROM market_weekly_reports
                WHERE user_id = %s AND week_end = %s AND status = 'completed'
                """,
                (user_id, week_end),
            )
            row = cursor.fetchone()
            if not row:
                return None
            columns = [
                "id", "week_start", "week_end", "status",
                "metadata", "created_at", "share_token",
            ]
            d = dict(zip(columns, row))
            if d.get("week_start"):
                d["week_start"] = str(d["week_start"])
            if d.get("week_end"):
                d["week_end"] = str(d["week_end"])
            if d.get("created_at"):
                d["created_at"] = d["created_at"].isoformat()
            return d
        finally:
            cursor.close()
            conn.close()

    def get_latest_report(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        获取用户最新一份已完成的 Market Weekly 报告。

        Returns:
            报告元数据，含 share_token 和 OSS signed URL；若无则返回 None。
        """
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT
                    id, user_id, week_start, week_end, oss_object_name, share_token,
                    status, metadata, created_at, updated_at, error_message
                FROM market_weekly_reports
                WHERE user_id = %s AND status = 'completed'
                ORDER BY week_end DESC
                LIMIT 1
            """, (user_id,))

            row = cursor.fetchone()
            cursor.close()
            conn.close()

            if not row:
                return None

            report_id, uid, week_start, week_end, oss_name, token, status, meta, created, updated, error = row

            # 生成 OSS signed URL（24h 有效）
            oss_service = OSSService()
            signed_url = oss_service.get_signed_url(oss_name, expires=86400) if oss_name else None

            return {
                "id": report_id,
                "user_id": uid,
                "week_start": str(week_start),
                "week_end": str(week_end),
                "oss_object_name": oss_name,
                "share_token": token,
                "signed_url": signed_url,
                "status": status,
                "metadata": meta or {},
                "created_at": created.isoformat() if created else None,
                "updated_at": updated.isoformat() if updated else None,
                "error_message": error,
            }

        except Exception as e:
            logger.error(f"[get_latest_report] Error: {e}")
            return None

    def list_reports(self, user_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        获取用户 Market Weekly 报告历史列表。

        Returns:
            报告列表，最新在前，每条含 share_token 和 OSS signed URL。
        """
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT
                    id, user_id, week_start, week_end, oss_object_name, share_token,
                    status, metadata, created_at, updated_at, error_message
                FROM market_weekly_reports
                WHERE user_id = %s
                ORDER BY week_end DESC
                LIMIT %s
            """, (user_id, limit))

            rows = cursor.fetchall()
            cursor.close()
            conn.close()

            oss_service = OSSService()
            results = []

            for row in rows:
                report_id, uid, week_start, week_end, oss_name, token, status, meta, created, updated, error = row

                signed_url = oss_service.get_signed_url(oss_name, expire_seconds=86400) if oss_name and status == 'completed' else None

                results.append({
                    "id": report_id,
                    "user_id": uid,
                    "week_start": str(week_start),
                    "week_end": str(week_end),
                    "oss_object_name": oss_name,
                    "share_token": token,
                    "signed_url": signed_url,
                    "status": status,
                    "metadata": meta or {},
                    "created_at": created.isoformat() if created else None,
                    "updated_at": updated.isoformat() if updated else None,
                    "error_message": error,
                })

            return results

        except Exception as e:
            logger.error(f"[list_reports] Error: {e}")
            return []

    def get_report_html_by_token(self, token: str) -> Optional[bytes]:
        """
        通过 share_token 获取报告 HTML 内容（从 OSS）。

        Args:
            token: 分享 token

        Returns:
            HTML 内容 bytes；若找不到或未完成则返回 None。
        """
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()

            cursor.execute("""
                SELECT oss_object_name, status
                FROM market_weekly_reports
                WHERE share_token = %s
            """, (token,))

            row = cursor.fetchone()
            cursor.close()
            conn.close()

            if not row:
                logger.warning(f"[get_report_html_by_token] Token not found: {token[:8]}...")
                return None

            oss_name, status = row
            if status != 'completed' or not oss_name:
                logger.warning(f"[get_report_html_by_token] Report not completed: status={status}")
                return None

            # 从 OSS 获取 HTML
            oss_service = OSSService()
            html_bytes = oss_service.get_object_bytes(oss_name)

            return html_bytes

        except Exception as e:
            logger.error(f"[get_report_html_by_token] Error: {e}")
            return None

    def get_users_with_tag(self) -> List[str]:
        """Return distinct user_ids that have at least one theme tagged 'Market Weekly'."""
        conn = self.db_manager.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT DISTINCT user_id FROM theme_tags WHERE tag_name = %s",
                (TAG_NAME,),
            )
            return [row[0] for row in cursor.fetchall()]
        finally:
            cursor.close()
            conn.close()

