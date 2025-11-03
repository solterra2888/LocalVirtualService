# -*- coding: utf-8 -*-
"""
Earnings Calendar Service
用于从 EODHD API 获取美股财报日历数据
"""

import os
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.utils.database_config import get_database_config

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
            
            logger.info("财报日历服务初始化完成")
            
        except Exception as e:
            logger.error(f"初始化财报日历服务失败: {str(e)}")
            raise
    
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
            
            # 发送请求
            logger.info(f"请求 EODHD API: {url}")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
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
        存储财报数据到数据库（使用批量插入优化性能）
        
        Args:
            earnings_list: 财报数据列表
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            存储结果
        """
        try:
            fetch_date = datetime.now().strftime('%Y-%m-%d')
            
            # 预处理数据，准备批量插入
            import json
            records_to_insert = []
            batch_size = 1000  # 批量插入大小
            
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
                
                # 准备插入数据
                records_to_insert.append((
                    code, name, exchange, report_date_str, str(report_date),
                    before_after_market, currency, actual, estimate,
                    difference, percent, actual_revenue, estimate_revenue,
                    difference_revenue, percent_revenue, raw_data,
                    'eodhd', fetch_date
                ))
            
            # 批量插入数据
            new_count = 0
            updated_count = 0
            
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                
                # 分批处理，避免一次性插入过多数据
                for i in range(0, len(records_to_insert), batch_size):
                    batch = records_to_insert[i:i + batch_size]
                    
                    try:
                        # 使用 executemany 批量插入
                        cursor.executemany("""
                        INSERT INTO earnings_calendar (
                            code, name, exchange, report_date, date_string,
                            before_after_market, currency, actual, estimate,
                            difference, percent, actual_revenue, estimate_revenue,
                            difference_revenue, percent_revenue, raw_data,
                            data_source, fetch_date
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
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
                        """, batch)
                        
                        # 统计新增和更新数量（ON CONFLICT UPDATE 会返回0或1，这里简化处理）
                        # 实际情况下，需要单独查询来区分新增和更新
                        processed_count = len(batch)
                        
                        if i == 0:
                            logger.info(f"批量插入进度: {min(i + batch_size, len(records_to_insert))}/{len(records_to_insert)}")
                        elif i % (batch_size * 5) == 0:  # 每5批打印一次进度
                            logger.info(f"批量插入进度: {min(i + batch_size, len(records_to_insert))}/{len(records_to_insert)}")
                    
                    except Exception as e:
                        logger.warning(f"批量插入失败（批次 {i//batch_size + 1}）: {str(e)}")
                        # 如果批量插入失败，尝试逐条插入该批次
                        for record in batch:
                            try:
                                cursor.execute("""
                                INSERT INTO earnings_calendar (
                                    code, name, exchange, report_date, date_string,
                                    before_after_market, currency, actual, estimate,
                                    difference, percent, actual_revenue, estimate_revenue,
                                    difference_revenue, percent_revenue, raw_data,
                                    data_source, fetch_date
                                ) VALUES (
                                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                )
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
                                """, record)
                                
                                if cursor.rowcount == 1:
                                    new_count += 1
                                else:
                                    updated_count += 1
                            except Exception as e2:
                                logger.warning(f"单条插入失败 {record[0]}: {str(e2)}")
                                continue
                
                # 查询统计信息（区分新增和更新）
                try:
                    cursor.execute("""
                    SELECT 
                        COUNT(*) FILTER (WHERE created_at::date = CURRENT_DATE) as new_count,
                        COUNT(*) FILTER (WHERE updated_at::date = CURRENT_DATE AND created_at::date != CURRENT_DATE) as updated_count
                    FROM earnings_calendar
                    WHERE fetch_date = %s
                    """, (fetch_date,))
                    
                    stats = cursor.fetchone()
                    if stats:
                        new_count = stats[0] or 0
                        updated_count = stats[1] or 0
                except Exception as e:
                    logger.warning(f"统计新增/更新数量失败: {str(e)}")
                    # 如果统计失败，使用总数作为估算
                    new_count = len(records_to_insert) // 2
                    updated_count = len(records_to_insert) - new_count
                
                logger.info(f"财报数据存储完成: 总数 {len(records_to_insert)}, 新增约 {new_count}, 更新约 {updated_count}")
            
            return {
                'success': True,
                'message': f'成功获取并存储 {len(earnings_list)} 条财报记录 (新增: {new_count}, 更新: {updated_count})',
                'data': {
                    'total_count': len(earnings_list),
                    'new_count': new_count,
                    'updated_count': updated_count,
                    'start_date': start_date,
                    'end_date': end_date
                }
            }
            
        except Exception as e:
            error_msg = f"存储财报数据失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {
                'success': False,
                'message': error_msg
            }
    
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
            today = datetime.now().date()
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
        
        Args:
            days: 从今天开始的天数 (默认7天)
            
        Returns:
            执行结果
        """
        try:
            today = datetime.now().date()
            end_date = today + timedelta(days=days)
            
            start_date_str = today.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            logger.info(f"获取 {days} 天财报日历: {start_date_str} 到 {end_date_str}")
            
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


def create_earnings_calendar_service() -> EarningsCalendarService:
    """创建服务实例的工厂函数"""
    return EarningsCalendarService()


