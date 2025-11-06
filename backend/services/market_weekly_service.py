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
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("market_weekly_service")


class MarketWeeklyService:
    """Market Weekly报告生成服务"""
    
    def __init__(self):
        """初始化服务"""
        db_config = get_database_config()
        self.db_manager = DatabaseManager(db_config)
        logger.info("MarketWeeklyService initialized")
    
    def generate_report(self, period: str = 'latest') -> Dict[str, Any]:
        """
        生成Market Weekly报告
        
        Args:
            period: 报告周期，'latest' 表示最新一周
        
        Returns:
            完整的报告数据字典
        """
        logger.info(f"Generating Market Weekly report for period: {period}")
        
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
        return report
    
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
          AND (exchange LIKE '%US%' OR exchange LIKE '%NYSE%' OR exchange LIKE '%NASDAQ%' 
               OR exchange LIKE '%HK%' OR exchange LIKE '%Hong Kong%')
        ORDER BY start_date, code
        LIMIT 50;
        """
        
        try:
            conn = self.db_manager.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, (start_date, end_date))
            
            results = []
            rows = cursor.fetchall()
            
            for row in rows:
                try:
                    # 安全地访问每个索引，避免元组索引错误
                    start_date_val = row[0] if len(row) > 0 else None
                    code_val = row[1] if len(row) > 1 else ''
                    name_val = row[2] if len(row) > 2 else ''
                    exchange_val = row[3] if len(row) > 3 else ''
                    deal_type_val = row[4] if len(row) > 4 else '-'
                    price_from = row[5] if len(row) > 5 else None
                    price_to = row[6] if len(row) > 6 else None
                    shares_val = row[7] if len(row) > 7 else None
                    
                    # 格式化价格范围
                    if price_from and price_to:
                        price_range = f"${price_from} - ${price_to}"
                    elif price_from:
                        price_range = f"${price_from}"
                    else:
                        price_range = '-'
                    
                    # 格式化股数
                    shares = str(shares_val) if shares_val else '-'
                    
                    results.append({
                        'date': start_date_val.strftime('%Y-%m-%d') if start_date_val else '',
                        'ticker': code_val or '',
                        'company': name_val or '',
                        'exchange': exchange_val or '',
                        'deal_type': deal_type_val or '-',
                        'price_range': price_range,
                        'shares': shares
                    })
                except (IndexError, AttributeError) as e:
                    logger.warning(f"Error processing IPO row: {str(e)}, row data: {row}")
                    continue
            
            cursor.close()
            conn.close()
            
            logger.info(f"Found {len(results)} IPO events")
            return results
        
        except Exception as e:
            logger.error(f"Error fetching IPO calendar: {str(e)}")
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
    
    def refresh_report(self):
        """
        刷新报告（清除缓存，重新生成）
        
        Note: 当前实现没有使用缓存，每次都是实时生成
        TODO: 如果启用缓存，在这里清除缓存
        """
        logger.info("Report refresh requested (currently generates fresh data on every request)")
        pass

