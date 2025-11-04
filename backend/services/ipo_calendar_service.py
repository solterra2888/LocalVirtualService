# -*- coding: utf-8 -*-
"""
IPO Calendar Service
用于从 EODHD API 获取 IPO 日历数据
"""

import os
import requests
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.utils.database_config import get_database_config

logger = get_logger("ipo_calendar_service")


class IPOCalendarService:
    """IPO 日历数据服务"""
    
    def __init__(self):
        """初始化服务"""
        logger.info("开始初始化 IPO 日历服务")
        
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
            
            logger.info("IPO 日历服务初始化完成")
            
        except Exception as e:
            logger.error(f"初始化 IPO 日历服务失败: {str(e)}")
            raise
    
    def fetch_ipo_calendar(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        从 EODHD API 获取 IPO 日历数据
        
        注意：EODHD API 不支持 exchanges 参数，所有交易所的数据都会返回。
        如果需要过滤特定交易所，可以在查询数据库时使用 WHERE 条件。
        
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
            
            logger.info(f"开始获取 IPO 日历数据: {start_date} 到 {end_date}")
            
            # 构建API请求
            # 注意：EODHD API 只支持 from 和 to 参数，不支持 exchanges 参数
            url = f"{self.base_url}/calendar/ipos"
            params = {
                "from": start_date,
                "to": end_date,
                "api_token": self.api_key,
                "fmt": "json"
            }
            
            # 发送请求（禁用代理以避免连接问题）
            proxies = {
                'http': None,
                'https': None
            }
            
            logger.info(f"请求 EODHD API: {url}")
            response = requests.get(url, params=params, timeout=30, proxies=proxies)
            response.raise_for_status()
            
            data = response.json()
            
            if not data or not isinstance(data, dict):
                logger.warning("未获取到 IPO 数据")
                return {
                    'success': True,
                    'message': '未获取到 IPO 数据',
                    'data': {
                        'total_count': 0,
                        'new_count': 0,
                        'updated_count': 0,
                        'start_date': start_date,
                        'end_date': end_date
                    }
                }
            
            # 解析数据
            ipos = data.get('ipos', [])
            
            if not ipos:
                logger.info("未找到 IPO 数据")
                return {
                    'success': True,
                    'message': '未找到 IPO 数据',
                    'data': {
                        'total_count': 0,
                        'new_count': 0,
                        'updated_count': 0,
                        'start_date': start_date,
                        'end_date': end_date
                    }
                }
            
            logger.info(f"解析到 {len(ipos)} 条 IPO 记录")
            
            # 存储到数据库（存储所有交易所的数据）
            result = self._store_ipo_data(ipos, start_date, end_date)
            
            return result
            
        except requests.exceptions.RequestException as e:
            error_msg = f"API请求失败: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'message': error_msg
            }
        except Exception as e:
            error_msg = f"获取 IPO 日历数据失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return {
                'success': False,
                'message': error_msg
            }
    
    def fetch_custom_period_ipo(self, days: int = 30) -> Dict[str, Any]:
        """
        获取未来指定天数的 IPO 数据
        
        Args:
            days: 从今天开始的天数（默认30天）
        
        Returns:
            同 fetch_ipo_calendar
        """
        today = datetime.now().date()
        start_date = today.strftime("%Y-%m-%d")
        end_date = (today + timedelta(days=days)).strftime("%Y-%m-%d")
        
        logger.info(f"获取未来 {days} 天的 IPO 数据: {start_date} 到 {end_date}")
        
        return self.fetch_ipo_calendar(start_date, end_date)
    
    def fetch_next_week_ipo(self) -> Dict[str, Any]:
        """
        获取下周的 IPO 数据
        
        Returns:
            同 fetch_ipo_calendar
        """
        today = datetime.now().date()
        # 计算下周的开始日期（下周一）
        days_until_next_monday = (7 - today.weekday()) % 7
        if days_until_next_monday == 0:
            days_until_next_monday = 7
        next_monday = today + timedelta(days=days_until_next_monday)
        next_sunday = next_monday + timedelta(days=6)
        
        start_date = next_monday.strftime("%Y-%m-%d")
        end_date = next_sunday.strftime("%Y-%m-%d")
        
        logger.info(f"获取下周 IPO 数据: {start_date} 到 {end_date}")
        
        return self.fetch_ipo_calendar(start_date, end_date)
    
    def _store_ipo_data(self, ipos: List[Dict[str, Any]], start_date: str, end_date: str) -> Dict[str, Any]:
        """
        存储 IPO 数据到数据库
        
        Args:
            ipos: IPO 数据列表
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            存储结果
        """
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return {
                    'success': False,
                    'message': '数据库连接失败'
                }
            
            cursor = conn.cursor()
            
            new_count = 0
            updated_count = 0
            
            for ipo in ipos:
                try:
                    # 解析数据
                    code = ipo.get('code', '')
                    name = ipo.get('name', '')
                    exchange = ipo.get('exchange', '')
                    currency = ipo.get('currency')
                    start_date_str = ipo.get('start_date', '')
                    filing_date_str = ipo.get('filing_date')
                    amended_date_str = ipo.get('amended_date')
                    price_from = ipo.get('price_from', 0)
                    price_to = ipo.get('price_to', 0)
                    offer_price = ipo.get('offer_price', 0)
                    shares = ipo.get('shares', 0)
                    deal_type = ipo.get('deal_type', '')
                    
                    # 处理日期
                    start_date_parsed = None
                    if start_date_str:
                        try:
                            start_date_parsed = datetime.strptime(start_date_str, "%Y-%m-%d").date()
                        except:
                            pass
                    
                    filing_date_parsed = None
                    if filing_date_str:
                        try:
                            filing_date_parsed = datetime.strptime(filing_date_str, "%Y-%m-%d").date()
                        except:
                            pass
                    
                    amended_date_parsed = None
                    if amended_date_str:
                        try:
                            amended_date_parsed = datetime.strptime(amended_date_str, "%Y-%m-%d").date()
                        except:
                            pass
                    
                    # 检查是否已存在（使用 code 和 start_date 作为唯一标识）
                    cursor.execute("""
                        SELECT id FROM ipo_calendar 
                        WHERE code = %s AND start_date = %s
                    """, (code, start_date_parsed))
                    
                    existing = cursor.fetchone()
                    
                    if existing:
                        # 更新现有记录
                        cursor.execute("""
                            UPDATE ipo_calendar SET
                                name = %s,
                                exchange = %s,
                                currency = %s,
                                filing_date = %s,
                                amended_date = %s,
                                price_from = %s,
                                price_to = %s,
                                offer_price = %s,
                                shares = %s,
                                deal_type = %s,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE code = %s AND start_date = %s
                        """, (
                            name, exchange, currency, filing_date_parsed, amended_date_parsed,
                            price_from, price_to, offer_price, shares, deal_type,
                            code, start_date_parsed
                        ))
                        updated_count += 1
                    else:
                        # 插入新记录
                        cursor.execute("""
                            INSERT INTO ipo_calendar (
                                code, name, exchange, currency, start_date,
                                filing_date, amended_date, price_from, price_to,
                                offer_price, shares, deal_type
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            code, name, exchange, currency, start_date_parsed,
                            filing_date_parsed, amended_date_parsed, price_from, price_to,
                            offer_price, shares, deal_type
                        ))
                        new_count += 1
                    
                except Exception as e:
                    logger.error(f"存储 IPO 数据失败: {code}, 错误: {str(e)}")
                    continue
            
            conn.commit()
            cursor.close()
            conn.close()
            
            total_count = len(ipos)
            message = f"成功处理 {total_count} 条 IPO 记录（新增: {new_count}, 更新: {updated_count}）"
            logger.info(message)
            
            return {
                'success': True,
                'message': message,
                'data': {
                    'total_count': total_count,
                    'new_count': new_count,
                    'updated_count': updated_count,
                    'start_date': start_date,
                    'end_date': end_date
                }
            }
            
        except Exception as e:
            error_msg = f"存储 IPO 数据失败: {str(e)}"
            logger.error(error_msg, exc_info=True)
            if conn:
                conn.rollback()
                conn.close()
            return {
                'success': False,
                'message': error_msg
            }


def create_ipo_calendar_service() -> IPOCalendarService:
    """创建 IPO 日历服务实例"""
    return IPOCalendarService()

