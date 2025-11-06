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
        # 注意：economic_events表可能不存在或列名不同，这里返回空列表避免错误
        # TODO: 如果需要经济日历功能，需要先创建economic_events表
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            
            # 先检查表是否存在
            cursor.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'economic_events'
                )
            """)
            table_exists = cursor.fetchone()[0]
            
            if not table_exists:
                logger.warning("economic_events table does not exist, returning empty list")
                cursor.close()
                conn.close()
                return []
            
            # 表存在，尝试查询（不使用importance列）
            query = """
            SELECT 
                date,
                event,
                country,
                actual,
                forecast,
                previous
            FROM economic_events
            WHERE date >= %s AND date <= %s
              AND country = 'US'
            ORDER BY date DESC
            LIMIT 50;
            """
            
            cursor.execute(query, (start_date, end_date))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'date': row[0].strftime('%Y-%m-%d') if row[0] else '',
                    'time': row[0].strftime('%H:%M') if hasattr(row[0], 'hour') else '',
                    'event': row[1] or '',
                    'country': row[2] or 'US',
                    'actual': str(row[3]) if row[3] is not None else '-',
                    'forecast': str(row[4]) if row[4] is not None else '-',
                    'previous': str(row[5]) if row[5] is not None else '-'
                })
            
            cursor.close()
            conn.close()
            
            logger.info(f"Found {len(results)} economic events")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching economic calendar: {str(e)}")
            return []
    
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

