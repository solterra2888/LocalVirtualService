# -*- coding: utf-8 -*-
"""
Earnings Calendar Service
用于从 EODHD API 获取美股财报日历数据
"""

import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta
from backend.utils.datetime_utils import utcnow
from typing import Dict, Any, List, Optional
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.utils.database_config import get_database_config
from config.settings import config

logger = get_logger("earnings_calendar_service")


class EarningsCalendarService:
    """财报日历数据服务"""
    
    def __init__(self):
        """初始化服务"""
        logger.info("开始初始化财报日历服务")
        
        try:
            # 获取标准化数据库连接配置
            logger.info("准备数据库连接配置")
            db_config = get_database_config()
            logger.info("数据库配置准备完成", 
                        host=db_config.get('db_host', ''), 
                        port=db_config.get('db_port', '5432'),
                        database=db_config.get('db_name', ''),
                        user=db_config.get('db_user', ''),
                        password_set=bool(db_config.get('db_password', '')))
            
            # 创建数据库管理器
            logger.info("创建数据库管理器")
            self.db_manager = DatabaseManager(db_config)
            logger.info("数据库管理器创建完成")
            
            self.api_key = os.getenv('EODHD_API_TOKEN')
            self.base_url = "https://eodhd.com/api"
            
            if not self.api_key:
                logger.warning("EODHD_API_TOKEN environment variable not set")
            
            # 🔥 创建 robust session（参考 economic_events_service）
            self.session = self._create_robust_session()
            logger.info("HTTP会话创建完成（带重试机制）")
            
            logger.info("财报日历服务初始化完成")
            
        except Exception as e:
            logger.error(f"初始化财报日历服务失败: {str(e)}")
            raise
    
    def _create_robust_session(self) -> requests.Session:
        """
        创建具有重试机制和超时设置的HTTP会话
        （参考 economic_events_service.py 和 eodhd_fundamentals_service.py）
        """
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 设置请求头
        session.headers.update({
            'User-Agent': config.USER_AGENT,
            'Accept': 'application/json'
        })
        
        return session
    
    def _safe_api_request(self, url: str, params: dict, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """
        安全的API请求函数，处理SSL错误和各种异常
        （参考 economic_events_service.py 的实现）
        
        Args:
            url: 请求URL
            params: 请求参数
            max_retries: 最大重试次数
            
        Returns:
            API响应的JSON数据，失败返回None
        """
        import time
        session = self.session
        
        for attempt in range(max_retries):
            try:
                logger.info(f"🔄 API请求 (第 {attempt + 1}/{max_retries} 次)")
                
                # 🔥 使用 session 发送请求，设置合理的超时
                # 连接超时: 10秒，读取超时: 120秒（因为财报数据量大）
                response = session.get(url, params=params, timeout=(10, 120))
                response.raise_for_status()
                
                logger.info(f"✅ API请求成功")
                return response.json()
                
            except requests.exceptions.SSLError as e:
                logger.warning(f"⚠️ SSL错误 (第 {attempt + 1}/{max_retries} 次): {str(e)[:200]}")
                if attempt < max_retries - 1:
                    # 🔥 关键：重新创建session以解决SSL连接问题
                    logger.info("🔧 关闭旧连接，重新创建HTTP会话...")
                    session.close()
                    session = self._create_robust_session()
                    self.session = session
                    
                    # 🔥 指数退避：1秒、2秒、4秒
                    wait_time = 2 ** attempt
                    logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error("❌ 达到最大重试次数，SSL错误持续存在")
                    raise
                    
            except requests.exceptions.Timeout as e:
                logger.warning(f"⚠️ 请求超时 (第 {attempt + 1}/{max_retries} 次): {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error("❌ 达到最大重试次数，请求超时")
                    raise
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"⚠️ 请求错误 (第 {attempt + 1}/{max_retries} 次): {str(e)[:200]}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.info(f"⏳ 等待 {wait_time} 秒后重试...")
                    time.sleep(wait_time)
                else:
                    logger.error("❌ 达到最大重试次数，API请求失败")
                    raise
        
        return None
    
    def fetch_earnings_calendar(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        从 EODHD API 获取财报日历数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            {
                'success': True/False,
                'message': '执行结果消息',
                'data': {
                    'total_count': 总记录数,
                    'new_count': 新增记录数,
                    'updated_count': 更新记录数,
                    'start_date': 开始日期,
                    'end_date': 结束日期
                }
            }
        """
        try:
            if not self.api_key:
                return {
                    'success': False,
                    'message': 'EODHD API token not configured'
                }
            
            logger.info(f"开始获取财报日历数据: {start_date} 到 {end_date}")
            
            # 构建API请求
            url = f"{self.base_url}/calendar/earnings"
            params = {
                "from": start_date,
                "to": end_date,
                "api_token": self.api_key,
                "fmt": "json"
            }
            
            # 🔥 使用安全的API请求方法（带SSL错误处理）
            logger.info(f"请求 EODHD API: {url}")
            logger.info(f"请求参数: from={start_date}, to={end_date}")
            
            data = self._safe_api_request(url, params, max_retries=3)
            
            # 🔥 检查请求结果
            if data is None:
                logger.error("API请求失败，未获取到数据")
                return {
                    'success': False,
                    'message': 'API请求失败，未获取到数据'
                }
            
            if not data or not isinstance(data, dict):
                logger.warning("未获取到财报数据")
                return {
                    'success': True,
                    'message': '未获取到财报数据',
                    'data': {
                        'total_count': 0,
                        'new_count': 0,
                        'updated_count': 0,
                        'start_date': start_date,
                        'end_date': end_date
                    }
                }
            
            # 解析并存储数据
            earnings_list = []
            for date_str, companies in data.items():
                if isinstance(companies, list):
                    for company in companies:
                        company['date'] = date_str
                        earnings_list.append(company)
            
            logger.info(f"解析到 {len(earnings_list)} 条财报记录")
            
            if not earnings_list:
                return {
                    'success': True,
                    'message': '未获取到财报数据',
                    'data': {
                        'total_count': 0,
                        'new_count': 0,
                        'updated_count': 0,
                        'start_date': start_date,
                        'end_date': end_date
                    }
                }
            
            # 存储到数据库
            result = self._store_earnings_data(earnings_list, start_date, end_date)
            
            return result
            
        except requests.exceptions.RequestException as e:
            error_msg = f"API请求失败: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'message': error_msg
            }
        except Exception as e:
            error_msg = f"获取财报日历数据失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {
                'success': False,
                'message': error_msg
            }
    
    def _store_earnings_data(self, earnings_list: List[Dict], start_date: str, end_date: str) -> Dict[str, Any]:
        """
        🚀 优化版本：存储财报数据到数据库（参考 fundamentals 的高性能批量插入）
        
        Args:
            earnings_list: 财报数据列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            存储结果
        """
        try:
            import time
            import json
            
            start_time = time.time()
            # fetch_date 是去重 key 的一部分 (code, report_date, fetch_date), 必须用 UTC
            # 否则 HK 跨日时段重复任务会因日期不同而无法去重.
            fetch_date = utcnow().strftime('%Y-%m-%d')
            
            # 🔥 使用字典去重：key = (code, report_date, fetch_date)
            records_dict = {}
            duplicates_found = 0
            
            for earnings in earnings_list:
                # 提取数据
                code = earnings.get('code', '').upper().replace('.US', '')
                if not code:
                    continue
                
                name = earnings.get('name', '')
                exchange = earnings.get('exchange', '')
                report_date = earnings.get('report_date') or earnings.get('date')
                
                if not report_date:
                    continue
                
                # 转换日期格式
                try:
                    if isinstance(report_date, str):
                        report_date_obj = datetime.strptime(report_date, '%Y-%m-%d')
                    else:
                        report_date_obj = report_date
                    report_date_str = report_date_obj.strftime('%Y-%m-%d')
                except:
                    continue
                
                before_after_market = earnings.get('before_after_market') or earnings.get('when', '')
                currency = earnings.get('currency', 'USD')
                
                # 财务数据
                actual = self._safe_float(earnings.get('actual'))
                estimate = self._safe_float(earnings.get('estimate'))
                difference = self._safe_float(earnings.get('difference'))
                percent = self._safe_float(earnings.get('percent'))
                
                # 营收数据
                actual_revenue = self._safe_int(earnings.get('actual_revenue'))
                estimate_revenue = self._safe_int(earnings.get('estimate_revenue'))
                difference_revenue = self._safe_int(earnings.get('difference_revenue'))
                percent_revenue = self._safe_float(earnings.get('percent_revenue'))
                
                # 存储完整的原始数据
                raw_data = json.dumps(earnings)
                
                # 🔥 创建唯一键（对应数据库唯一约束）
                unique_key = (code, report_date_str, fetch_date)
                
                # 🔥 检测并处理重复数据
                if unique_key in records_dict:
                    duplicates_found += 1
                    logger.debug(f"发现重复记录: code={code}, report_date={report_date_str}, 将被覆盖")
                
                # 准备插入数据（后出现的记录会覆盖先出现的）
                records_dict[unique_key] = (
                    code, name, exchange, report_date_str, str(report_date),
                    before_after_market, currency, actual, estimate,
                    difference, percent, actual_revenue, estimate_revenue,
                    difference_revenue, percent_revenue, raw_data,
                    'eodhd', fetch_date
                )
            
            # 🔥 转换为列表
            records_to_insert = list(records_dict.values())
            
            logger.info(f"📊 原始数据: {len(earnings_list)} 条，去重后: {len(records_to_insert)} 条，重复: {duplicates_found} 条")
            
            # 🚀 使用高性能批量插入
            total_saved = self._batch_upsert_earnings(records_to_insert)
            
            # 🚀 性能监控
            end_time = time.time()
            duration = end_time - start_time
            records_per_second = total_saved / duration if duration > 0 else 0
            
            logger.info(f"☑️ 财报数据处理完成: 共 {total_saved} 条记录，耗时 {duration:.2f}秒，速度 {records_per_second:.0f} 条/秒")
            
            # 查询统计信息（简化版本，避免复杂查询）
            # 由于 ON CONFLICT DO UPDATE 的特性，我们使用近似统计
            new_count = total_saved // 2  # 粗略估计
            updated_count = total_saved - new_count
            
            return {
                'success': True,
                'message': f'成功获取并存储 {len(earnings_list)} 条财报记录 (约新增: {new_count}, 更新: {updated_count})',
                'data': {
                    'total_count': len(earnings_list),
                    'new_count': new_count,
                    'updated_count': updated_count,
                    'start_date': start_date,
                    'end_date': end_date,
                    'duration_seconds': duration,
                    'records_per_second': records_per_second
                }
            }
            
        except Exception as e:
            error_msg = f"存储财报数据失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {
                'success': False,
                'message': error_msg
            }
    
    def _batch_upsert_earnings(self, records: List[tuple]) -> int:
        """
        🚀 高性能批量插入财报数据（使用 execute_values，参考 fundamentals 优化）
        
        Args:
            records: 要插入的记录列表
            
        Returns:
            成功插入/更新的记录数
        """
        try:
            import psycopg2.extras
            
            # 🎯 动态计算最优批次大小
            batch_size = self._calculate_optimal_batch_size(len(records))
            logger.info(f"🎯 使用动态批次大小: {batch_size}")
            
            total_saved = 0
            total_batches = (len(records) + batch_size - 1) // batch_size
            
            logger.info(f"📊 开始分批处理财报数据: {len(records)} 条记录，分为 {total_batches} 批")
            
            # 分批处理，每批使用独立的数据库连接和事务
            for i in range(0, len(records), batch_size):
                batch_records = records[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                # 每5批或第一批打印进度
                if batch_num == 1 or batch_num % 5 == 0 or batch_num == total_batches:
                    logger.info(f"🔄 处理财报批次 {batch_num}/{total_batches}: {len(batch_records)} 条记录")
                
                # 🚀 关键优化：每批使用独立的连接上下文，立即提交
                with self.db_manager.get_connection_context() as conn:
                    with conn.cursor() as cursor:
                        earnings_sql = """
                            INSERT INTO earnings_calendar (
                                code, name, exchange, report_date, date_string,
                                before_after_market, currency, actual, estimate,
                                difference, percent, actual_revenue, estimate_revenue,
                                difference_revenue, percent_revenue, raw_data,
                                data_source, fetch_date
                            ) VALUES %s
                            ON CONFLICT (code, report_date, fetch_date) DO UPDATE SET
                                name = EXCLUDED.name,
                                exchange = EXCLUDED.exchange,
                                date_string = EXCLUDED.date_string,
                                before_after_market = EXCLUDED.before_after_market,
                                currency = EXCLUDED.currency,
                                actual = EXCLUDED.actual,
                                estimate = EXCLUDED.estimate,
                                difference = EXCLUDED.difference,
                                percent = EXCLUDED.percent,
                                actual_revenue = EXCLUDED.actual_revenue,
                                estimate_revenue = EXCLUDED.estimate_revenue,
                                difference_revenue = EXCLUDED.difference_revenue,
                                percent_revenue = EXCLUDED.percent_revenue,
                                raw_data = EXCLUDED.raw_data,
                                updated_at = CURRENT_TIMESTAMP
                        """
                        
                        # 🚀 使用 execute_values（比 executemany 快 10-50 倍）
                        psycopg2.extras.execute_values(
                            cursor, earnings_sql, batch_records,
                            template=None, 
                            page_size=min(1000, len(batch_records))
                        )
                        
                        total_saved += len(batch_records)
                # 退出 with 语句时自动提交该批次
            
            logger.info(f"✅ 财报数据分批处理完成: {total_saved} 条记录")
            return total_saved
            
        except Exception as e:
            logger.error(f"财报数据分批处理失败: {str(e)}", exc_info=True)
            raise
    
    def _calculate_optimal_batch_size(self, total_records: int) -> int:
        """
        🎯 动态计算最优批次大小（参考 fundamentals 的实现）
        
        Args:
            total_records: 总记录数
            
        Returns:
            最优批次大小
        """
        try:
            import psutil
            
            # 获取可用内存（GB）
            available_memory_gb = psutil.virtual_memory().available / (1024 ** 3)
            
            # 根据记录数量和可用内存动态调整
            if total_records <= 1000:
                return min(500, total_records)
            elif total_records <= 5000:
                return min(1000, total_records)
            elif total_records <= 10000:
                # 根据内存调整
                if available_memory_gb >= 8:
                    return 2000
                elif available_memory_gb >= 4:
                    return 1500
                else:
                    return 1000
            else:
                # 超大数据量（earnings 通常不会超过 15000）
                if available_memory_gb >= 16:
                    return 3000
                elif available_memory_gb >= 8:
                    return 2000
                else:
                    return 1500
                    
        except Exception as e:
            logger.warning(f"计算最优批次大小失败，使用默认值: {str(e)}")
            return 1000
    
    def _safe_float(self, value) -> Optional[float]:
        """安全转换为浮点数"""
        if value is None or value == '' or value == '-':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def _safe_int(self, value) -> Optional[int]:
        """安全转换为整数"""
        if value is None or value == '' or value == '-':
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    def fetch_next_week_earnings(self) -> Dict[str, Any]:
        """
        获取下周的财报日历数据
        
        Returns:
            执行结果
        """
        try:
            today = utcnow().date()
            # 计算下周一和下周日
            days_until_next_monday = (7 - today.weekday()) % 7
            if days_until_next_monday == 0:
                days_until_next_monday = 7
            
            next_monday = today + timedelta(days=days_until_next_monday)
            next_sunday = next_monday + timedelta(days=6)
            
            start_date = next_monday.strftime('%Y-%m-%d')
            end_date = next_sunday.strftime('%Y-%m-%d')
            
            logger.info(f"获取下周财报日历: {start_date} 到 {end_date}")
            
            return self.fetch_earnings_calendar(start_date, end_date)
            
        except Exception as e:
            error_msg = f"获取下周财报数据失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {
                'success': False,
                'message': error_msg
            }
    
    def fetch_custom_period_earnings(self, days: int = 7) -> Dict[str, Any]:
        """
        获取指定天数的财报日历数据

        窗口从 today-7 开始（而非今天）：EODHD 只在财报发布后才回填实际 EPS，
        只向前抓会导致已公布财报的 actual 永远缺失，重抓过去 7 天用于回填。

        Args:
            days: 向未来看的天数 (默认7天)

        Returns:
            执行结果
        """
        try:
            today = utcnow().date()
            start_date = today - timedelta(days=7)
            end_date = today + timedelta(days=days)

            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')

            logger.info(f"获取 {days} 天财报日历(含7天回填): {start_date_str} 到 {end_date_str}")

            return self.fetch_earnings_calendar(start_date_str, end_date_str)

        except Exception as e:
            error_msg = f"获取财报数据失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {
                'success': False,
                'message': error_msg
            }
    
    def get_earnings_by_date_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        从数据库查询指定日期范围的财报数据
        
        Args:
            start_date: 开始日期 (YYYY-MM-DD)
            end_date: 结束日期 (YYYY-MM-DD)
            
        Returns:
            财报数据列表
        """
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                
                cursor.execute("""
                SELECT 
                    id, code, name, exchange, report_date, before_after_market,
                    actual, estimate, difference, percent,
                    actual_revenue, estimate_revenue, difference_revenue, percent_revenue,
                    fetch_date, created_at
                FROM earnings_calendar
                WHERE report_date >= %s AND report_date <= %s
                ORDER BY report_date, code
                """, (start_date, end_date))
                
                rows = cursor.fetchall()
                
                results = []
                for row in rows:
                    results.append({
                        'id': row[0],
                        'code': row[1],
                        'name': row[2],
                        'exchange': row[3],
                        'report_date': row[4].strftime('%Y-%m-%d') if row[4] else None,
                        'before_after_market': row[5],
                        'actual': float(row[6]) if row[6] is not None else None,
                        'estimate': float(row[7]) if row[7] is not None else None,
                        'difference': float(row[8]) if row[8] is not None else None,
                        'percent': float(row[9]) if row[9] is not None else None,
                        'actual_revenue': int(row[10]) if row[10] is not None else None,
                        'estimate_revenue': int(row[11]) if row[11] is not None else None,
                        'difference_revenue': int(row[12]) if row[12] is not None else None,
                        'percent_revenue': float(row[13]) if row[13] is not None else None,
                        'fetch_date': row[14].strftime('%Y-%m-%d') if row[14] else None,
                        'created_at': row[15].isoformat() if row[15] else None
                    })
                
                logger.info(f"从数据库查询到 {len(results)} 条财报记录")
                return results
                
        except Exception as e:
            logger.error(f"查询财报数据失败: {str(e)}", exc_info=True)
            return []


    def get_earnings_calendar_view(
        self,
        from_date: str,
        to_date: str,
        scope: str = 'watchlist',
        before_after: Optional[str] = None,
        search: Optional[str] = None,
        reported: Optional[str] = None,
        limit: int = 500,
        offset: int = 0
    ) -> Dict[str, Any]:
        """
        财报日历展示查询（供前端 Earnings Calendar 模块使用）

        与 get_earnings_by_date_range 的区别：
        - 同一 (code, report_date) 只返回最新一次抓取的快照（每日抓取会累积多行）
        - 支持 watchlist / theme / us 三档范围（口径与 market_weekly 一致）
        - LEFT JOIN companies 补充公司名称和 Logo
        - 支持盘前/盘后、搜索、已公布/未公布筛选

        Args:
            from_date: 开始日期 (YYYY-MM-DD)
            to_date: 结束日期 (YYYY-MM-DD)
            scope: watchlist=自选(target_tracking_config) / theme=主题关联公司 / us=全部美股
            before_after: None=全部 / before=盘前 / after=盘后
            search: 模糊匹配 ticker 或公司名
            reported: None=全部 / yes=已公布(有实际EPS) / no=未公布
            limit: 返回条数上限
            offset: 偏移量
        """
        # 范围过滤条件
        scope_conditions = {
            'watchlist': """
                AND (
                    ec.code IN (SELECT REPLACE(primaryticker, '.US', '') FROM target_tracking_config
                                WHERE enabled = TRUE AND primaryticker IS NOT NULL AND primaryticker != '')
                    OR ec.code IN (SELECT primaryticker FROM target_tracking_config
                                   WHERE enabled = TRUE AND primaryticker IS NOT NULL AND primaryticker != '')
                )""",
            'theme': """
                AND (
                    ec.code IN (SELECT REPLACE(ticker, '.US', '') FROM theme_company_associations
                                WHERE is_verified = TRUE AND ticker IS NOT NULL AND ticker != '')
                    OR ec.code IN (SELECT ticker FROM theme_company_associations
                                   WHERE is_verified = TRUE AND ticker IS NOT NULL AND ticker != '')
                )""",
            # 港股: EODHD 代码带 .HK 后缀; 无优先股系列噪音, 无需 type 过滤;
            # earnings_calendar 与 companies 表均无港股名称, 前端按代码展示
            'hk': " AND ec.code LIKE '%%.HK'",
            # 注意: psycopg2 把 % 当占位符, SQL 字面量中的 % 需写成 %%
            # 'us' 范围只保留普通股:
            # 1) 过滤优先股系列(companies.type = Preferred Stock, 如 BAC-PB/PE/PK)
            # 2) 过滤不在 companies 且以 RP 结尾、但基础代码是普通股的存托凭证变体(如 BACRP → BAC);
            #    不直接按 RP 后缀删除, 会误伤 NRP/TRP 等正常股票
            'us': """
                AND ec.code NOT LIKE '%%.%%'
                AND (c.type = 'Common Stock' OR c.id IS NULL)
                AND NOT (
                    c.id IS NULL
                    AND ec.code LIKE '%%RP'
                    AND EXISTS (
                        SELECT 1 FROM companies c2
                        WHERE c2.code = left(ec.code, length(ec.code) - 2)
                          AND c2.type = 'Common Stock'
                    )
                )
            """,
        }
        scope_sql = scope_conditions.get(scope, scope_conditions['watchlist'])

        conditions = [scope_sql]
        params: List[Any] = [from_date, to_date]

        if before_after in ('before', 'after'):
            conditions.append(" AND ec.before_after_market ILIKE %s")
            params.append(before_after + '%')

        if search:
            conditions.append(" AND (ec.code ILIKE %s OR c.name ILIKE %s OR ec.name ILIKE %s)")
            like = f"%{search}%"
            params.extend([like, like, like])

        where_sql = " ".join(conditions)

        base_sql = f"""
            FROM earnings_calendar ec
            LEFT JOIN companies c ON c.code = ec.code
            WHERE ec.report_date >= %s AND ec.report_date <= %s
            {where_sql}
        """

        # reported 筛选作用在最新快照上（而非原始行），否则已公布公司会命中旧快照被误判为未公布
        reported_sql = ""
        if reported == 'yes':
            reported_sql = " WHERE actual IS NOT NULL"
        elif reported == 'no':
            reported_sql = " WHERE actual IS NULL"

        # DISTINCT ON 取每个 (code, report_date) 最新 fetch_date 的快照
        rows_sql = f"""
            SELECT * FROM (
                SELECT DISTINCT ON (ec.code, ec.report_date)
                    ec.code,
                    COALESCE(c.name, ec.name, ec.code) AS company_name,
                    c.logourl,
                    ec.report_date,
                    ec.before_after_market,
                    ec.currency,
                    ec.actual, ec.estimate, ec.difference, ec.percent,
                    ec.fetch_date
                {base_sql}
                ORDER BY ec.code, ec.report_date, ec.fetch_date DESC
            ) latest{reported_sql}
            ORDER BY report_date, before_after_market DESC, code
            LIMIT %s OFFSET %s
        """

        count_sql = f"""
            SELECT count(*) FROM (
                SELECT DISTINCT ON (ec.code, ec.report_date)
                    ec.code, ec.report_date, ec.actual
                {base_sql}
                ORDER BY ec.code, ec.report_date, ec.fetch_date DESC
            ) latest{reported_sql}
        """

        meta_sql = "SELECT max(fetch_date), max(updated_at) FROM earnings_calendar"

        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()

                cursor.execute(count_sql, params)
                total = cursor.fetchone()[0] or 0

                cursor.execute(meta_sql)
                meta_row = cursor.fetchone()
                latest_fetch = meta_row[0].strftime('%Y-%m-%d') if meta_row and meta_row[0] else None
                latest_update = meta_row[1].isoformat() if meta_row and meta_row[1] else None

                cursor.execute(rows_sql, params + [limit, offset])
                rows = cursor.fetchall()

                items = []
                for row in rows:
                    items.append({
                        'code': row[0],
                        'company_name': row[1],
                        'logo_url': row[2],
                        'report_date': row[3].strftime('%Y-%m-%d') if row[3] else None,
                        'before_after_market': row[4],
                        'currency': row[5] or 'USD',
                        'eps_actual': float(row[6]) if row[6] is not None else None,
                        'eps_estimate': float(row[7]) if row[7] is not None else None,
                        'eps_difference': float(row[8]) if row[8] is not None else None,
                        'eps_surprise_percent': float(row[9]) if row[9] is not None else None,
                        'reported': row[6] is not None,
                        'fetch_date': row[10].strftime('%Y-%m-%d') if row[10] else None,
                    })

                cursor.close()

                logger.info(
                    f"财报日历展示查询: scope={scope}, {from_date}~{to_date}, "
                    f"命中 {total} 条, 返回 {len(items)} 条"
                )

                return {
                    'items': items,
                    'meta': {
                        'total': total,
                        'limit': limit,
                        'offset': offset,
                        'scope': scope,
                        'from_date': from_date,
                        'to_date': to_date,
                        'latest_fetch_date': latest_fetch,
                        'latest_update_at': latest_update,
                    }
                }

        except Exception as e:
            logger.error(f"财报日历展示查询失败: {str(e)}", exc_info=True)
            raise

    def __del__(self):
        """清理资源"""
        try:
            if hasattr(self, 'session'):
                self.session.close()
        except:
            pass


def create_earnings_calendar_service() -> EarningsCalendarService:
    """创建服务实例的工厂函数"""
    return EarningsCalendarService()


