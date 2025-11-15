# -*- coding: utf-8 -*-
"""
Market Weekly Report Service

生成Market Weekly报告的核心服务，包括：
- Week-over-Week计算
- 相对表现计算（indexed to 100）
- 执行摘要生成
- 日历数据聚合（经济事件、财报、IPO）
"""

import os
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger
from backend.services.economic_events_service import EconomicEventsService

logger = get_logger("market_weekly_service")

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
    
    def generate_report(self, period: str = 'latest', use_cache: bool = True, cache_ttl: int = 3600) -> Dict[str, Any]:
        """
        生成Market Weekly报告
        
        Args:
            period: 报告周期，'latest' 表示最新一周
            use_cache: 是否使用缓存，默认True
            cache_ttl: 缓存过期时间（秒），默认3600秒（1小时）
        
        Returns:
            完整的报告数据字典
        """
        cache_key = f"market_weekly_report:{period}"
        
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
            generated_at = datetime.utcnow().isoformat() + 'Z'
            
            logger.info(f"Report period: {week_start} to {week_end}")
            
            # 2. 计算Week-over-Week变化
            indices_wow = self._calculate_wow('index', week_start, week_end)
            sectors_wow = self._calculate_wow('sector', week_start, week_end)
            forex_wow = self._calculate_wow('forex', week_start, week_end)
            commodities_wow = self._calculate_wow('commodity', week_start, week_end)
            yields_wow = self._calculate_wow('bond', week_start, week_end)
            
            # 2.5 计算AI Related数据（从数据库获取Market Weekly主题，分US和China市场）
            us_ai_wow, us_ai_relative, china_ai_wow, china_ai_relative = self._calculate_ai_related_performance(week_start, week_end, days=90)
            
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
        
        逻辑：
        - week_end: 数据库中最新的交易日（本周最新数据）
        - week_start: 上一个周五（作为上周的代表）
        
        这样可以确保：
        - 如果本周有数据（即使不是周五），也会使用本周数据
        - 对比的是"上周五"到"本周最新交易日"
        
        Args:
            period: 'latest' 或特定日期
        
        Returns:
            (week_start, week_end) 元组，都是datetime对象
        """
        if period == 'latest':
            try:
                conn = self.db_manager.get_connection()
                cursor = conn.cursor()
                
                # 1. 获取数据库中的最新交易日（作为week_end）
                cursor.execute("""
                    SELECT MAX(date)::date as latest_date
                    FROM market_data_eod
                    WHERE date >= CURRENT_DATE - INTERVAL '14 days'
                """)
                result = cursor.fetchone()
                week_end_date = result[0] if result and result[0] else None
                
                if not week_end_date:
                    raise ValueError("No recent data found in market_data_eod")
                
                # 2. 获取week_end之前最近的周五（作为week_start）
                # 如果week_end本身就是周五，则取上一个周五
                cursor.execute("""
                    SELECT MAX(date)::date as friday_date
                    FROM market_data_eod
                    WHERE EXTRACT(DOW FROM date) = 5  -- 周五
                      AND date < %s
                      AND date >= %s - INTERVAL '14 days'
                """, (week_end_date, week_end_date))
                
                result = cursor.fetchone()
                week_start_date = result[0] if result and result[0] else None
                
                cursor.close()
                conn.close()
                
                if not week_start_date:
                    # 如果找不到之前的周五，使用week_end - 7天
                    logger.warning("Could not find previous Friday, using week_end - 7 days")
                    week_start_date = week_end_date - timedelta(days=7)
                
                logger.info(f"Calculated week range from database: {week_start_date} to {week_end_date}")
                
            except Exception as e:
                logger.error(f"Error querying week range from database: {str(e)}")
                # 使用fallback逻辑
                today = datetime.utcnow().date()
                week_end_date = today
                # 找到上一个周五
                days_since_friday = (today.weekday() - 4) % 7
                if days_since_friday == 0:
                    days_since_friday = 7
                week_start_date = today - timedelta(days=days_since_friday)
        else:
            # TODO: 支持特定日期范围
            today = datetime.utcnow().date()
            week_end_date = today
            week_start_date = week_end_date - timedelta(days=7)
        
        # 转换为datetime对象
        week_start = datetime.combine(week_start_date, datetime.min.time())
        week_end = datetime.combine(week_end_date, datetime.max.time())
        
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
        # 使用company_earnings_history表代替earnings_calendar表
        query = """
        SELECT 
            ceh.reportdate as date,
            c.code,
            c.name,
            ceh.beforeaftermarket
        FROM company_earnings_history ceh
        INNER JOIN companies c ON ceh.company_id = c.id
        WHERE ceh.reportdate >= %s AND ceh.reportdate <= %s
        ORDER BY ceh.reportdate, c.code
        LIMIT 100;
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
                    'company': row[2] or ''
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
            redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
            if redis_url and redis_url != 'redis://localhost:6379/0':
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
            redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
            if redis_url and redis_url != 'redis://localhost:6379/0':
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
            redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
            if redis_url and redis_url != 'redis://localhost:6379/0':
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
    
    async def get_ohlc_data_batch(self, symbols: List[str], days: int = 90, request=None) -> Dict[str, Any]:
        """
        批量获取OHLC原始数据（用于前端渲染图表，支持请求中断检测）
        
        Args:
            symbols: 股票代码列表，格式为 [{"symbol": "SPY.US", "label": "S&P 500"}, ...]
            days: 天数
            request: FastAPI Request对象，用于检测客户端断开
        
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
                end_date = datetime.utcnow()
                start_date = end_date - timedelta(days=days)
                
                conn = self.db_manager.get_connection()
                cursor = conn.cursor()
                
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
            end_date = datetime.utcnow()
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
    
    def _calculate_ai_related_performance(self, week_start: datetime, week_end: datetime, days: int = 90) -> tuple:
        """
        计算AI Related主题的WoW和相对表现（分US和China市场）
        
        从数据库获取group='Market Weekly'的主题，通过theme_company_associations获取相关ticker，
        然后从stock_prices_eod获取价格数据进行计算
        
        US市场: market = 'US'
        China市场: market = 'HK' 或 'CN'
        
        Args:
            week_start: 起始日期
            week_end: 结束日期
            days: 相对表现回溯天数
        
        Returns:
            (us_ai_wow, us_ai_relative, china_ai_wow, china_ai_relative) 四元组
        """
        logger.info("Calculating AI Related performance from database themes (US and China markets)")
        
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            # 1. 获取Market Weekly AI主题及其关联的ticker
            theme_query = """
            SELECT 
                ite.theme_id,
                ite.theme_name,
                ite.category,
                tca.ticker,
                tca.market,
                tca.company_name
            FROM investment_theme_embeddings ite
            INNER JOIN theme_company_associations tca ON ite.theme_id = tca.theme_id
            WHERE ite."group" = 'Market Weekly'
              AND tca.is_verified = TRUE
            ORDER BY ite.theme_name, tca.market, tca.ticker;
            """
            
            cursor.execute(theme_query)
            theme_tickers = cursor.fetchall()
            
            if not theme_tickers:
                logger.warning("No Market Weekly AI themes found in database")
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
        
        关键：与report_debug.py逻辑一致 - 先计算每日平均价格，再计算WoW
        """
        market_wow = []
        
        for theme_name, theme_data in themes_map.items():
            tickers = theme_data['tickers']
            
            # 关键逻辑：先计算每日的平均价格（所有股票的平均），再计算WoW
            # 这与report_debug.py的compute_ai_sector_panels() + compute_wow()逻辑一致
            wow_query = """
            WITH daily_avg_prices AS (
                -- 步骤1: 计算每日所有股票的平均收盘价
                SELECT 
                    date::date as trade_date,
                    AVG(close) as avg_close
                FROM stock_prices_eod
                WHERE code = ANY(%s)
                  AND date BETWEEN %s - INTERVAL '30 days' AND %s
                  AND close IS NOT NULL
                GROUP BY date::date
                HAVING COUNT(*) > 0
            ),
            week_start_price AS (
                -- 步骤2: 获取week_start当天或之前最近的平均价格
                SELECT avg_close as start_price
                FROM daily_avg_prices
                WHERE trade_date <= %s
                ORDER BY trade_date DESC
                LIMIT 1
            ),
            week_end_price AS (
                -- 步骤3: 获取week_end当天或之前最近的平均价格
                SELECT avg_close as end_price
                FROM daily_avg_prices
                WHERE trade_date <= %s
                ORDER BY trade_date DESC
                LIMIT 1
            )
            -- 步骤4: 计算涨跌幅（基于平均价格）
            SELECT 
                ROUND(((wep.end_price - wsp.start_price) / NULLIF(wsp.start_price, 0) * 100)::numeric, 2) as change_pct
            FROM week_start_price wsp
            CROSS JOIN week_end_price wep
            WHERE wsp.start_price IS NOT NULL 
              AND wsp.start_price != 0
              AND wep.end_price IS NOT NULL;
            """
            
            cursor.execute(wow_query, (
                tickers, week_start.date(), week_end.date(),
                week_start.date(),
                week_end.date()
            ))
            
            result = cursor.fetchone()
            if result and result[0] is not None:
                market_wow.append({
                    'name': theme_name,
                    'change_pct': round(float(result[0]), 2)
                })
        
        # 按涨跌幅排序
        market_wow.sort(key=lambda x: x['change_pct'], reverse=True)
        return market_wow
    
    def _calculate_market_relative(self, themes_map: dict, days: int, cursor) -> list:
        """计算某个市场所有主题的相对表现"""
        market_relative = []
        
        for theme_name, theme_data in themes_map.items():
            tickers = theme_data['tickers']
            
            relative_query = """
            WITH daily_avg_prices AS (
                SELECT 
                    date::date as trade_date,
                    AVG(close) as avg_close
                FROM stock_prices_eod
                WHERE code = ANY(%s)
                  AND date >= CURRENT_DATE - %s
                  AND close IS NOT NULL
                GROUP BY date::date
                HAVING COUNT(*) > 0
            ),
            first_price AS (
                SELECT avg_close as first_close
                FROM daily_avg_prices
                ORDER BY trade_date
                LIMIT 1
            )
            SELECT 
                dap.trade_date,
                ROUND((dap.avg_close / NULLIF(fp.first_close, 0) * 100)::numeric, 2) as indexed_value
            FROM daily_avg_prices dap
            CROSS JOIN first_price fp
            WHERE fp.first_close IS NOT NULL AND fp.first_close != 0
            ORDER BY dap.trade_date;
            """
            
            cursor.execute(relative_query, (tickers, days))
            daily_data = cursor.fetchall()
            
            # 添加到结果中
            for row in daily_data:
                trade_date_str = row[0].strftime('%Y-%m-%d')
                indexed_value = float(row[1]) if row[1] is not None else 100.0
                
                # 查找或创建该日期的记录
                date_record = next((r for r in market_relative if r['date'] == trade_date_str), None)
                if not date_record:
                    date_record = {'date': trade_date_str}
                    market_relative.append(date_record)
                
                date_record[theme_name] = indexed_value
        
        # 按日期排序
        market_relative.sort(key=lambda x: x['date'])
        return market_relative

