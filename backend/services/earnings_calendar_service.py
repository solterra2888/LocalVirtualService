# -*- coding: utf-8 -*-
"""
Earnings Calendar Service
用于从 EODHD API 获取美股财报日历数据
"""

import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import date, datetime, timedelta
from backend.utils.datetime_utils import utcnow
from typing import Dict, Any, List, Optional
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.utils.database_config import get_database_config
from config.settings import config

logger = get_logger("earnings_calendar_service")


class EarningsCalendarService:
    """财报日历数据服务"""

    # T3 点击静默刷新的进程内粗去重: {code: last_enqueue_ts}（10 分钟窗口, 多 worker 下允许偶发重复）
    _lazy_refresh_seen: Dict[str, float] = {}

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
            LEFT JOIN companies c ON c.code = ec.code OR c.primaryticker = ec.code
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

    def get_earnings_month_summary(
        self,
        month: str,
        scope: str = 'watchlist',
        search: Optional[str] = None,
        top_n: int = 5
    ) -> Dict[str, Any]:
        """
        月度汇总（Month View 用）：每个日期的财报家数 + 市值 Top N 公司

        只应用 scope 和 search 过滤（月视图是总览，盘前盘后/已公布等
        明细筛选对"每日多少家"的语义没有意义）。

        Args:
            month: 月份 (YYYY-MM)
            scope: watchlist / theme / us / hk（口径同 get_earnings_calendar_view）
            search: 模糊匹配 ticker 或公司名
            top_n: 每日返回的市值 Top N 公司数
        """
        try:
            year, mon = month.split('-')
            year, mon = int(year), int(mon)
            start = date(year, mon, 1)
            end = date(year + 1, 1, 1) - timedelta(days=1) if mon == 12 else date(year, mon + 1, 1) - timedelta(days=1)
        except (ValueError, IndexError):
            raise ValueError(f"无效月份格式: {month}, 应为 YYYY-MM")

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
            'us': " AND ec.code NOT LIKE '%%.%%' AND (c.type = 'Common Stock' OR c.id IS NULL)",
            'hk': " AND ec.code LIKE '%%.HK'",
        }
        scope_sql = scope_conditions.get(scope, scope_conditions['watchlist'])

        conditions = [scope_sql]
        params: List[Any] = [start.isoformat(), end.isoformat()]
        if search:
            conditions.append(" AND (ec.code ILIKE %s OR c.name ILIKE %s OR ec.name ILIKE %s)")
            like = f"%{search}%"
            params.extend([like, like, like])
        where_sql = " ".join(conditions)

        sql = f"""
            WITH latest AS (
                SELECT DISTINCT ON (ec.code, ec.report_date)
                    ec.code,
                    COALESCE(c.name, ec.name, ec.code) AS company_name,
                    c.logourl,
                    ec.report_date,
                    ec.before_after_market,
                    ec.actual, ec.estimate, ec.percent,
                    COALESCE(h.marketcapitalization, f.market_cap * 1000000.0) AS market_cap
                FROM earnings_calendar ec
                LEFT JOIN companies c ON c.code = ec.code OR c.primaryticker = ec.code
                LEFT JOIN company_highlights h ON h.company_id = c.id
                LEFT JOIN finviz_screener f ON f.ticker = ec.code
                WHERE ec.report_date >= %s AND ec.report_date <= %s
                {where_sql}
                ORDER BY ec.code, ec.report_date, ec.fetch_date DESC
            ),
            ranked AS (
                SELECT latest.*,
                       row_number() OVER (PARTITION BY report_date
                                          ORDER BY market_cap DESC NULLS LAST, code) AS rn,
                       count(*) OVER (PARTITION BY report_date) AS day_count
                FROM latest
            )
            SELECT
                report_date,
                max(day_count) AS day_count,
                sum(CASE WHEN before_after_market ILIKE 'before%%' THEN 1 ELSE 0 END) AS before_count,
                sum(CASE WHEN before_after_market ILIKE 'after%%' THEN 1 ELSE 0 END) AS after_count,
                json_agg(json_build_object(
                    'code', code,
                    'company_name', company_name,
                    'logo_url', logourl,
                    'eps_estimate', estimate,
                    'eps_actual', actual,
                    'eps_surprise_percent', percent,
                    'reported', actual IS NOT NULL,
                    'market_cap', market_cap
                ) ORDER BY market_cap DESC NULLS LAST) FILTER (WHERE rn <= %s) AS top
            FROM ranked
            GROUP BY report_date
            ORDER BY report_date
        """
        params.append(top_n)

        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                cursor.close()

            # logo 相对路径补全
            days = []
            for row in rows:
                days.append({
                    'date': row[0].strftime('%Y-%m-%d') if row[0] else None,
                    'count': int(row[1] or 0),
                    'before_count': int(row[2] or 0),
                    'after_count': int(row[3] or 0),
                    'top': row[4] or [],
                })

            total = sum(d['count'] for d in days)
            logger.info(f"月度汇总: month={month}, scope={scope}, {len(days)} 天, 共 {total} 家")

            return {
                'month': month,
                'scope': scope,
                'total': total,
                'days': days,
            }

        except Exception as e:
            logger.error(f"月度汇总查询失败: {str(e)}", exc_info=True)
            raise

    def _maybe_lazy_refresh(self, code: str, bare_code: str, exchange: Optional[str],
                            company_updated_at) -> None:
        """
        T3 点击静默刷新: 面板打开时若该公司 fundamentals 超 3 天未更新,
        fire-and-forget 提交单票 Celery 更新（不阻塞响应, 失败静默）。
        仅覆盖 EODHD fundamentals 可用市场（美/港）; A股与其它市场跳过。
        """
        try:
            import time as _time
            if (exchange or '') not in ('NYSE', 'NASDAQ', 'US', 'HK'):
                return
            if company_updated_at is not None:
                from datetime import datetime, timedelta, timezone as _tz
                updated = company_updated_at if company_updated_at.tzinfo else \
                    company_updated_at.replace(tzinfo=_tz.utc)
                if updated > datetime.now(_tz.utc) - timedelta(days=3):
                    return
            now_ts = _time.time()
            last = self._lazy_refresh_seen.get(code)
            if last and now_ts - last < 600:
                return
            self._lazy_refresh_seen[code] = now_ts
            if len(self._lazy_refresh_seen) > 5000:
                self._lazy_refresh_seen.clear()

            from backend.tasks.data_tasks import update_single_ticker_fundamentals_task
            update_single_ticker_fundamentals_task.apply_async(
                args=[bare_code, exchange or 'US'])
            logger.info(f"T3 静默刷新已入队: {bare_code}.{exchange}")
        except Exception as e:
            logger.debug(f"T3 静默刷新跳过: {code}: {e}")

    def get_company_panel(self, code: str) -> Dict[str, Any]:
        """
        Ticker 财务信息面板（财报日历右侧详情用）

        汇总: 公司基本信息 / highlights 关键指标(含 PEG·Fwd P/E·P/S·TTM 同比) / 下次财报(含当季
        营收预估) / 最近 12 季 EPS 历史 + beat 统计 / 分析师一致预期 / 评级 / 技术指标(含做空) /
        实时价格(含成交量·前收盘)。各数据块独立缺失互不影响。

        Args:
            code: 财报日历口径的代码（美股裸代码如 AAPL，港股带后缀如 0700.HK）
        """
        code = (code or '').strip()
        if not code:
            raise ValueError('code 不能为空')

        def _f(v):
            return float(v) if v is not None else None

        def _i(v):
            return int(v) if v is not None else None

        with self.db_manager.get_connection_context() as conn:
            cursor = conn.cursor()

            # 1) 公司基本信息（美股匹配 code, 港股/国际匹配 primaryticker）
            cursor.execute("""
                SELECT c.id, c.code, c.name, c.exchange, c.sector, c.industry,
                       c.countryname, c.description, c.weburl, c.logourl,
                       c.fulltimeemployees, c.currencycode, c.updated_at
                FROM companies c
                WHERE c.code = %s OR c.primaryticker = %s
                ORDER BY CASE WHEN c.code = %s THEN 1 ELSE 2 END
                LIMIT 1
            """, (code, code, code))
            row = cursor.fetchone()
            if not row:
                return {'found': False, 'code': code}
            (company_id, bare_code, name, exchange, sector, industry, country,
             description, weburl, logourl, employees, currency, company_updated_at) = row

            company = {
                'code': bare_code,
                'name': name,
                'exchange': exchange,
                'sector': sector,
                'industry': industry,
                'country': country,
                'description': description,
                'web_url': weburl,
                'logo_url': logourl,
                'employees': _i(employees),
                'currency': currency,
            }

            # 2) highlights 最新（含 Overview 指标: PEG / 营收与利润同比 / 每股股息）
            cursor.execute("""
                SELECT marketcapitalization, peratio, dividendyield, dilutedepsttm,
                       epsestimatecurrentquarter, epsestimatenextquarter,
                       epsestimatecurrentyear, epsestimatenextyear,
                       revenuettm, profitmargin, wallstreettargetprice, mostrecentquarter,
                       pegratio, dividendshare, quarterlyrevenuegrowthyoy, quarterlyearningsgrowthyoy
                FROM company_highlights WHERE company_id = %s
                ORDER BY data_date DESC LIMIT 1
            """, (company_id,))
            h = cursor.fetchone()
            highlights = None
            if h:
                highlights = {
                    'market_cap': _f(h[0]), 'pe_ratio': _f(h[1]), 'dividend_yield': _f(h[2]),
                    'eps_ttm': _f(h[3]),
                    'eps_est_current_quarter': _f(h[4]), 'eps_est_next_quarter': _f(h[5]),
                    'eps_est_current_year': _f(h[6]), 'eps_est_next_year': _f(h[7]),
                    'revenue_ttm': _f(h[8]), 'profit_margin': _f(h[9]),
                    'target_price': _f(h[10]), 'most_recent_quarter': h[11],
                    'peg_ratio': _f(h[12]), 'dividend_share': _f(h[13]),
                    'revenue_growth_yoy': _f(h[14]), 'earnings_growth_yoy': _f(h[15]),
                }

            # 2.5) finviz 快照（Forward P/E、P/S — highlights 没有的补充口径）
            cursor.execute("""
                SELECT forward_pe, ps_ratio
                FROM finviz_screener WHERE ticker = %s
                ORDER BY snapshot_date DESC LIMIT 1
            """, (bare_code,))
            fz = cursor.fetchone()
            if highlights is None:
                highlights = {}
            if fz:
                highlights['forward_pe'] = _f(fz[0])
                highlights['ps_ratio'] = _f(fz[1])

            # 3) 下次财报（earnings_calendar 最新快照中未来最早一场）
            cursor.execute("""
                SELECT DISTINCT ON (ec.code, ec.report_date)
                    ec.report_date, ec.before_after_market, ec.estimate
                FROM earnings_calendar ec
                WHERE ec.code = %s AND ec.report_date >= CURRENT_DATE
                ORDER BY ec.code, ec.report_date, ec.fetch_date DESC
                LIMIT 1
            """, (code,))
            n = cursor.fetchone()
            next_earnings = None
            if n:
                next_earnings = {
                    'report_date': n[0].strftime('%Y-%m-%d') if n[0] else None,
                    'before_after_market': n[1],
                    'eps_estimate': _f(n[2]),
                }

            # 4) 最近 12 季 EPS 历史（升序返回便于画图）
            cursor.execute("""
                SELECT reportdate, date, beforeaftermarket, epsactual, epsestimate, surprisepercent
                FROM company_earnings_history WHERE company_id = %s
                ORDER BY reportdate DESC LIMIT 12
            """, (company_id,))
            history_rows = cursor.fetchall()
            earnings_history = []
            for r in reversed(history_rows):
                earnings_history.append({
                    'report_date': r[0].strftime('%Y-%m-%d') if r[0] else None,
                    'fiscal_period': r[1].strftime('%Y-%m-%d') if r[1] else None,
                    'before_after_market': r[2],
                    'eps_actual': _f(r[3]),
                    'eps_estimate': _f(r[4]),
                    'surprise_percent': _f(r[5]),
                })

            # 5) 分析师一致预期（优先当季 '0q'，其次最新一期；'0q' 的营收预估同时用于下次财报卡片）
            cursor.execute("""
                SELECT date, period, growth, earningsestimateavg, earningsestimatehigh,
                       earningsestimatelow, earningsestimatenumberofanalysts,
                       revenueestimateavg, revenueestimatenumberofanalysts,
                       epsrevisionsuplast30days, epsrevisionsdownlast30days
                FROM company_earnings_trend WHERE company_id = %s
                ORDER BY (period = '0q') DESC, date DESC LIMIT 1
            """, (company_id,))
            t = cursor.fetchone()
            trend = None
            if t:
                trend = {
                    'period': t[1],
                    'period_date': t[0].strftime('%Y-%m-%d') if t[0] else None,
                    'growth': _f(t[2]),
                    'eps_avg': _f(t[3]), 'eps_high': _f(t[4]), 'eps_low': _f(t[5]),
                    'analysts': _i(t[6]),
                    'revenue_avg': _f(t[7]), 'revenue_analysts': _i(t[8]),
                    'revisions_up_30d': _i(t[9]), 'revisions_down_30d': _i(t[10]),
                }
                # 当季一致预期的营收预估挂到下次财报（EODHD 日历本身无营收预估）
                if next_earnings and trend['period'] == '0q' and trend['revenue_avg'] is not None:
                    next_earnings['revenue_estimate'] = trend['revenue_avg']

            # 5.5) 季度营收历史（最近 12 季, 升序返回便于画图; 币种随报表本币）
            cursor.execute("""
                SELECT date, totalrevenue, currency_symbol
                FROM company_income_statement
                WHERE company_id = %s AND period_type = 'quarterly'
                  AND totalrevenue IS NOT NULL
                ORDER BY date DESC LIMIT 12
            """, (company_id,))
            rev_rows = cursor.fetchall()
            revenue_history = []
            revenue_currency = None
            for r in reversed(rev_rows):
                revenue_currency = r[2] or revenue_currency
                revenue_history.append({
                    'period': r[0].strftime('%Y-%m-%d') if r[0] else None,
                    'revenue': _f(r[1]),
                })

            # 5.6) 季度股息支付历史（cash flow 口径, 取绝对值——EODHD 符号正负混杂, 升序画图）
            cursor.execute("""
                SELECT date, ABS(dividendspaid), currency_symbol
                FROM company_cash_flow
                WHERE company_id = %s AND period_type = 'quarterly'
                  AND dividendspaid IS NOT NULL AND dividendspaid != 0
                ORDER BY date DESC LIMIT 12
            """, (company_id,))
            div_rows = cursor.fetchall()
            dividend_history = []
            dividend_currency = None
            for r in reversed(div_rows):
                dividend_currency = r[2] or dividend_currency
                dividend_history.append({
                    'period': r[0].strftime('%Y-%m-%d') if r[0] else None,
                    'amount': _f(r[1]),
                })

            # 6) 分析师评级（最新）
            cursor.execute("""
                SELECT rating, targetprice, strongbuy, buy, hold, sell, strongsell
                FROM company_analyst_ratings WHERE company_id = %s
                ORDER BY data_date DESC LIMIT 1
            """, (company_id,))
            a = cursor.fetchone()
            analyst_ratings = None
            if a:
                analyst_ratings = {
                    'rating': a[0], 'target_price': _f(a[1]),
                    'strong_buy': _i(a[2]), 'buy': _i(a[3]), 'hold': _i(a[4]),
                    'sell': _i(a[5]), 'strong_sell': _i(a[6]),
                }

            # 7) 技术指标（最新, 含做空数据）
            cursor.execute("""
                SELECT beta, "52WeekHigh", "52WeekLow", "50DayMA", "200DayMA",
                       sharesshort, shortpercent, data_date
                FROM company_technicals WHERE company_id = %s
                ORDER BY data_date DESC LIMIT 1
            """, (company_id,))
            tc = cursor.fetchone()
            technicals = None
            if tc:
                technicals = {
                    'beta': _f(tc[0]), 'week52_high': _f(tc[1]), 'week52_low': _f(tc[2]),
                    'ma50': _f(tc[3]), 'ma200': _f(tc[4]),
                    'short_volume': _f(tc[5]), 'short_percent': _f(tc[6]),
                    'short_as_of': tc[7].strftime('%Y-%m-%d') if tc[7] else None,
                }

            # 7.5) EPS beat 统计（最近 17 个有预估的季度, 对齐 EarningsHub 口径）
            cursor.execute("""
                SELECT epsactual, epsestimate
                FROM company_earnings_history
                WHERE company_id = %s AND epsactual IS NOT NULL AND epsestimate IS NOT NULL
                ORDER BY reportdate DESC LIMIT 17
            """, (company_id,))
            beat_rows = cursor.fetchall()
            eps_est_quarters = len(beat_rows)
            eps_beat_count = sum(1 for br in beat_rows if br[0] > br[1])

            # 8) 实时价格（code 格式为 xxx.US）
            cursor.execute("""
                SELECT close, change_percent, last_updated, volume, previous_close
                FROM stock_prices_realtime
                WHERE code IN (%s, %s)
                ORDER BY last_updated DESC LIMIT 1
            """, (f"{bare_code}.US", code))
            p = cursor.fetchone()
            price = None
            if p:
                price = {
                    'last': _f(p[0]),
                    'change_percent': _f(p[1]),
                    'as_of': p[2].isoformat() if p[2] else None,
                    'volume': _f(p[3]),
                    'previous_close': _f(p[4]),
                }

            # 8.5) Forward P/E / P/S 兜底计算（finviz 缺失时用现价与年度预估推算）
            if highlights is not None and price and price.get('last'):
                if highlights.get('forward_pe') is None and highlights.get('eps_est_next_year'):
                    try:
                        highlights['forward_pe'] = round(price['last'] / highlights['eps_est_next_year'], 2)
                    except (TypeError, ZeroDivisionError):
                        pass
                if highlights.get('ps_ratio') is None and highlights.get('market_cap') and highlights.get('revenue_ttm'):
                    try:
                        highlights['ps_ratio'] = round(highlights['market_cap'] / highlights['revenue_ttm'], 2)
                    except (TypeError, ZeroDivisionError):
                        pass

            cursor.close()

        logger.info(f"公司面板查询: code={code}, company_id={company_id}, "
                    f"history={len(earnings_history)}季")

        # T3 点击静默刷新: fundamentals 超 3 天的欧美/港股公司, 后台异步刷新, 当次立即返回现有数据
        self._maybe_lazy_refresh(code, bare_code, exchange, company_updated_at)

        return {
            'found': True,
            'code': code,
            'company': company,
            'highlights': highlights,
            'next_earnings': next_earnings,
            'earnings_history': earnings_history,
            'revenue_history': revenue_history,
            'revenue_currency': revenue_currency,
            'dividend_history': dividend_history,
            'dividend_currency': dividend_currency,
            'earnings_trend': trend,
            'analyst_ratings': analyst_ratings,
            'technicals': technicals,
            'price': price,
            'eps_beat_count': eps_beat_count,
            'eps_est_quarters': eps_est_quarters,
        }

    def get_earnings_feed(
        self,
        from_date: str,
        to_date: str,
        scope: str = 'watchlist',
        search: Optional[str] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Earnings Feed（右栏默认态）: 当前窗口的财报事件流, 每条附实时价格与最近季度营收。

        排序: 已公布在前(report_date 倒序), 未公布在后(report_date 正序)。
        营收来自 company_income_statement(仅实际值, EODHD 无历史季度营收预估)。
        """
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
            'us': " AND ec.code NOT LIKE '%%.%%' AND (c.type = 'Common Stock' OR c.id IS NULL)",
            'hk': " AND ec.code LIKE '%%.HK'",
        }
        scope_sql = scope_conditions.get(scope, scope_conditions['watchlist'])

        conditions = [scope_sql]
        params: List[Any] = [from_date, to_date]
        if search:
            conditions.append(" AND (ec.code ILIKE %s OR c.name ILIKE %s OR ec.name ILIKE %s)")
            like = f"%{search}%"
            params.extend([like, like, like])
        where_sql = " ".join(conditions)

        sql = f"""
            WITH latest AS (
                SELECT DISTINCT ON (ec.code, ec.report_date)
                    ec.code,
                    COALESCE(c.name, ec.name, ec.code) AS company_name,
                    c.logourl,
                    ec.report_date,
                    ec.before_after_market,
                    ec.estimate,
                    ec.actual,
                    ec.percent
                FROM earnings_calendar ec
                LEFT JOIN companies c ON c.code = ec.code OR c.primaryticker = ec.code
                WHERE ec.report_date >= %s AND ec.report_date <= %s
                {where_sql}
                ORDER BY ec.code, ec.report_date, ec.fetch_date DESC
            )
            SELECT
                l.code, l.company_name, l.logourl,
                l.report_date, l.before_after_market,
                l.estimate, l.actual, l.percent,
                p.close, p.change_percent,
                r.totalrevenue, r.rev_date
            FROM latest l
            LEFT JOIN LATERAL (
                SELECT close, change_percent
                FROM stock_prices_realtime sp
                WHERE sp.code = l.code || '.US'
                ORDER BY sp.last_updated DESC LIMIT 1
            ) p ON TRUE
            LEFT JOIN LATERAL (
                SELECT totalrevenue, date AS rev_date
                FROM company_income_statement ist
                JOIN companies c2 ON ist.company_id = c2.id
                WHERE (c2.code = l.code OR c2.primaryticker = l.code)
                  AND ist.date <= l.report_date
                  -- EODHD 报表按本币, 非美元公司的营收直接展示会误导(TSM 为新台币)
                  AND c2.currencycode = 'USD'
                ORDER BY ist.date DESC LIMIT 1
            ) r ON TRUE
            ORDER BY (l.actual IS NULL),
                     CASE WHEN l.actual IS NULL
                          THEN l.report_date - DATE '2000-01-01'
                          ELSE DATE '2000-01-01' - l.report_date
                     END
            LIMIT %s
        """
        params.append(limit)

        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                cursor.close()

            items = []
            for row in rows:
                items.append({
                    'code': row[0],
                    'company_name': row[1],
                    'logo_url': row[2],
                    'report_date': row[3].strftime('%Y-%m-%d') if row[3] else None,
                    'before_after_market': row[4],
                    'eps_estimate': float(row[5]) if row[5] is not None else None,
                    'eps_actual': float(row[6]) if row[6] is not None else None,
                    'eps_surprise_percent': float(row[7]) if row[7] is not None else None,
                    'reported': row[6] is not None,
                    'price': float(row[8]) if row[8] is not None else None,
                    'change_percent': float(row[9]) if row[9] is not None else None,
                    'revenue': float(row[10]) if row[10] is not None else None,
                    'revenue_period': row[11].strftime('%Y-%m-%d') if row[11] else None,
                })

            reported_n = sum(1 for i in items if i['reported'])
            logger.info(f"Earnings Feed: {from_date}~{to_date}, scope={scope}, "
                        f"{len(items)} 条(已公布 {reported_n})")

            return {
                'items': items,
                'meta': {
                    'total': len(items),
                    'reported': reported_n,
                    'scope': scope,
                    'from_date': from_date,
                    'to_date': to_date,
                }
            }

        except Exception as e:
            logger.error(f"Earnings Feed 查询失败: {str(e)}", exc_info=True)
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


