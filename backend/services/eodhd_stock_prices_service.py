# -*- coding: utf-8 -*-
"""
EODHD Stock Prices Service - 实时股价数据获取服务

通过EODHD API获取用户watchlist中所有股票的实时价格数据
数据来源：https://eodhd.com/api/real-time/AAPL.US?s=VTI,EUR.FOREX&api_token=xxx&fmt=json

主要功能：
1. 获取用户watchlist中的所有ticker
2. 批量请求EODHD实时价格API
3. 解析并存储到stock_prices_realtime表
4. 支持多ticker批量查询（提高API效率）
"""

import os
import json
import time
import asyncio
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Set
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import psycopg2.extras

from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from config.settings import config

logger = get_logger("eodhd_stock_prices_service")


class EODHDStockPricesService:
    """EODHD实时股价数据服务"""
    
    def __init__(self):
        """初始化EODHD股价服务"""
        logger.info("开始初始化EODHD股价服务")
        
        try:
            # 获取API密钥
            self.api_token = config.get('EODHD_API_TOKEN')
            if not self.api_token:
                logger.error("EODHD_API_TOKEN 未在环境变量中设置")
                raise ValueError("EODHD_API_TOKEN 未在环境变量中设置")
            
            logger.info("API密钥获取成功", token_length=len(self.api_token))
            self.base_url = "https://eodhd.com/api/real-time"
            logger.info("设置API基础URL", base_url=self.base_url)
            
            # 数据库连接配置
            db_config = {
                'db_host': config.get('DB_HOST', 'localhost'),
                'db_port': config.get('DB_PORT', '5432'),
                'db_name': config.get('DB_NAME', 'rc_database'),
                'db_user': config.get('DB_USER', 'postgres'),
                'db_password': config.get('DB_PASSWORD', '')
            }
            self.db_manager = DatabaseManager(db_config)
            logger.info("数据库管理器配置完成")
            
            # 配置参数
            self.batch_size = int(config.get('EODHD_BATCH_SIZE', 50))  # 每次批量请求的股票数量
            self.request_delay = float(config.get('EODHD_REQUEST_DELAY', 0.1))  # 请求间隔
            self.max_retries = int(config.get('EODHD_MAX_RETRIES_ON_RATE_LIMIT', 3))
            
            # 创建优化的HTTP会话
            self.session = self._create_robust_session()
            logger.info("HTTP会话创建完成")
            
            logger.info("EODHD股价服务初始化完成",
                       batch_size=self.batch_size,
                       request_delay=self.request_delay,
                       max_retries=self.max_retries)
            
        except Exception as e:
            logger.error(f"初始化EODHD股价服务失败: {str(e)}")
            raise
    
    def _create_robust_session(self) -> requests.Session:
        """创建具有重试机制的HTTP会话"""
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 设置请求头
        session.headers.update({
            'User-Agent': 'RC-Project/1.0',
            'Accept': 'application/json'
        })
        
        return session
    
    def _safe_api_request(self, url: str, timeout: int = 30) -> Optional[List[Dict[str, Any]]]:
        """安全的API请求"""
        try:
            logger.debug(f"请求URL: {url[:100]}...")
            
            response = self.session.get(url, timeout=timeout)
            response.raise_for_status()
            
            data = response.json()
            logger.debug(f"API响应成功，返回{len(data) if isinstance(data, list) else 1}条数据")
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API请求失败: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"JSON解析失败: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"未知错误: {str(e)}")
            return None
    
    def get_all_watchlist_tickers(self) -> Set[str]:
        """获取所有用户watchlist中的ticker，使用primaryticker字段"""
        try:
            query = """
            SELECT DISTINCT 
                CASE 
                    WHEN primaryticker IS NOT NULL AND primaryticker != '' 
                    THEN primaryticker 
                    ELSE symbol 
                END as ticker
            FROM target_tracking_config 
            WHERE enabled = true 
            AND (symbol IS NOT NULL AND symbol != '')
            """
            
            results = self.db_manager.fetch_all(query)
            tickers = {row[0] for row in results if row[0]}
            
            logger.info(f"获取到{len(tickers)}个watchlist ticker（使用primaryticker）", tickers_sample=list(tickers)[:5])
            return tickers
            
        except Exception as e:
            logger.error(f"获取watchlist ticker失败: {str(e)}")
            return set()
    
    def format_ticker_for_eodhd(self, ticker: str) -> str:
        """格式化ticker为EODHD API格式
        
        EODHD API要求格式为 SYMBOL.EXCHANGE
        如果ticker已经包含交易所后缀，直接返回
        否则根据ticker特征判断交易所
        """
        # 如果ticker已经包含交易所后缀，直接返回
        if '.' in ticker:
            return ticker
        
        # 根据ticker特征判断交易所
        ticker_upper = ticker.upper()
        
        # 香港股票：通常以数字开头，如 0700, 9988
        if ticker.isdigit() and len(ticker) == 4:
            return f"{ticker}.HK"
        
        # 中国A股：通常以数字开头，如 000001, 600036
        if ticker.isdigit() and len(ticker) == 6:
            if ticker.startswith(('000', '002', '300')):
                return f"{ticker}.SZ"  # 深圳
            elif ticker.startswith(('600', '601', '603', '605', '688')):
                return f"{ticker}.SH"  # 上海
        
        # 台湾股票：通常以数字开头，如 2330
        if ticker.isdigit() and len(ticker) == 4:
            return f"{ticker}.TW"
        
        # 日本股票：通常以数字开头，如 7203
        if ticker.isdigit() and len(ticker) == 4:
            return f"{ticker}.JP"
        
        # 默认为美国交易所
        return f"{ticker}.US"
    
    def fetch_stock_prices_batch(self, tickers: List[str]) -> List[Dict[str, Any]]:
        """批量获取股票价格数据
        
        使用EODHD API的批量查询功能：
        https://eodhd.com/api/real-time/AAPL.US?s=VTI,EUR.FOREX&api_token=xxx&fmt=json
        """
        if not tickers:
            return []
        
        try:
            # 格式化ticker
            formatted_tickers = [self.format_ticker_for_eodhd(ticker) for ticker in tickers]
            
            # 构建URL - 第一个ticker作为主ticker，其余作为s参数
            primary_ticker = formatted_tickers[0]
            additional_tickers = formatted_tickers[1:] if len(formatted_tickers) > 1 else []
            
            url = f"{self.base_url}/{primary_ticker}?api_token={self.api_token}&fmt=json"
            
            if additional_tickers:
                additional_str = ",".join(additional_tickers)
                url += f"&s={additional_str}"
            
            logger.debug(f"批量请求{len(formatted_tickers)}个ticker的价格数据")
            
            # 发送请求
            data = self._safe_api_request(url)
            
            if data is None:
                logger.warning(f"批量请求失败，tickers: {tickers[:5]}...")
                return []
            
            # 确保返回的是列表格式
            if isinstance(data, dict):
                data = [data]
            elif not isinstance(data, list):
                logger.warning(f"API返回格式异常: {type(data)}")
                return []
            
            logger.info(f"成功获取{len(data)}个ticker的价格数据")
            return data
            
        except Exception as e:
            logger.error(f"批量获取股票价格失败: {str(e)}, tickers: {tickers[:5]}...")
            return []
    
    def parse_price_data(self, price_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """解析单个股票价格数据"""
        try:
            code = price_data.get('code', '')
            if not code:
                logger.warning("价格数据缺少code字段")
                return None
            
            # 解析timestamp
            timestamp = price_data.get('timestamp')
            if timestamp:
                try:
                    last_updated = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                except (ValueError, TypeError):
                    last_updated = datetime.now(timezone.utc)
            else:
                last_updated = datetime.now(timezone.utc)
            
            # 安全解析数值字段，处理"NA"值
            def safe_float(value):
                if value is None or value == '' or str(value).upper() == 'NA':
                    return None
                try:
                    return float(value)
                except (ValueError, TypeError):
                    return None
            
            def safe_int(value):
                if value is None or value == '' or str(value).upper() == 'NA':
                    return None
                try:
                    return int(value)
                except (ValueError, TypeError):
                    return None
            
            parsed_data = {
                'code': code,
                'gmtoffset': safe_int(price_data.get('gmtoffset', 0)) or 0,
                'open': safe_float(price_data.get('open')),
                'high': safe_float(price_data.get('high')),
                'low': safe_float(price_data.get('low')),
                'close': safe_float(price_data.get('close')),
                'volume': safe_int(price_data.get('volume')),
                'previous_close': safe_float(price_data.get('previousClose')),
                'change_amount': safe_float(price_data.get('change')),
                'change_percent': safe_float(price_data.get('change_p')),
                'last_updated': last_updated
            }
            
            # 验证必要字段 - 如果所有价格字段都是None，说明这是无效数据
            price_fields = [parsed_data['open'], parsed_data['high'], parsed_data['low'], parsed_data['close']]
            if all(field is None for field in price_fields):
                logger.warning(f"股票{code}所有价格字段都无效，跳过保存")
                return None
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"解析价格数据失败: {str(e)}, data: {price_data}")
            return None
    
    def save_price_data(self, price_records: List[Dict[str, Any]]) -> Dict[str, int]:
        """保存价格数据到数据库"""
        if not price_records:
            return {'success_count': 0, 'failed_count': 0}
        
        success_count = 0
        failed_count = 0
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("无法获取数据库连接")
                return {'success_count': 0, 'failed_count': len(price_records)}
            
            cursor = conn.cursor()
            
            # 准备插入/更新SQL
            upsert_sql = """
            INSERT INTO stock_prices_realtime 
            (code, gmtoffset, open, high, low, close, volume, previous_close, 
             change_amount, change_percent, last_updated, created_at, updated_at)
            VALUES (%(code)s, %(gmtoffset)s, %(open)s, %(high)s, %(low)s, %(close)s, 
                    %(volume)s, %(previous_close)s, %(change_amount)s, %(change_percent)s, 
                    %(last_updated)s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (code) 
            DO UPDATE SET
                gmtoffset = EXCLUDED.gmtoffset,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                previous_close = EXCLUDED.previous_close,
                change_amount = EXCLUDED.change_amount,
                change_percent = EXCLUDED.change_percent,
                last_updated = EXCLUDED.last_updated,
                updated_at = CURRENT_TIMESTAMP
            """
            
            for record in price_records:
                try:
                    cursor.execute(upsert_sql, record)
                    success_count += 1
                except Exception as e:
                    logger.warning(f"保存股票{record.get('code', 'unknown')}价格数据失败: {str(e)}")
                    failed_count += 1
            
            conn.commit()
            logger.info(f"价格数据保存完成：成功{success_count}，失败{failed_count}")
            
        except Exception as e:
            logger.error(f"批量保存价格数据失败: {str(e)}")
            if conn:
                conn.rollback()
            failed_count = len(price_records)
            
        finally:
            if conn:
                conn.close()
        
        return {'success_count': success_count, 'failed_count': failed_count}
    
    def fetch_and_store_watchlist_prices(self) -> Dict[str, Any]:
        """获取并存储所有watchlist股票的价格数据"""
        logger.info("开始获取并存储watchlist股票价格数据")
        
        start_time = datetime.now()
        total_success = 0
        total_failed = 0
        
        try:
            # 1. 获取所有watchlist ticker
            tickers = self.get_all_watchlist_tickers()
            if not tickers:
                logger.warning("未找到任何watchlist ticker")
                return {
                    'total_tickers': 0,
                    'success_count': 0,
                    'failed_count': 0,
                    'execution_time': 0
                }
            
            tickers_list = list(tickers)
            logger.info(f"准备获取{len(tickers_list)}个ticker的价格数据")
            
            # 2. 批量处理ticker
            batch_count = 0
            for i in range(0, len(tickers_list), self.batch_size):
                batch_tickers = tickers_list[i:i + self.batch_size]
                batch_count += 1
                
                logger.info(f"处理第{batch_count}批，共{len(batch_tickers)}个ticker")
                
                # 获取价格数据
                price_data_list = self.fetch_stock_prices_batch(batch_tickers)
                
                if price_data_list:
                    # 解析价格数据
                    parsed_records = []
                    for price_data in price_data_list:
                        parsed = self.parse_price_data(price_data)
                        if parsed:
                            parsed_records.append(parsed)
                    
                    # 保存到数据库
                    if parsed_records:
                        result = self.save_price_data(parsed_records)
                        total_success += result['success_count']
                        total_failed += result['failed_count']
                
                # 请求间隔（避免API限制）
                if i + self.batch_size < len(tickers_list):
                    time.sleep(self.request_delay)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"watchlist股票价格数据获取完成：总计{len(tickers_list)}个ticker，"
                       f"成功{total_success}，失败{total_failed}，耗时{execution_time:.2f}秒")
            
            return {
                'total_tickers': len(tickers_list),
                'success_count': total_success,
                'failed_count': total_failed,
                'execution_time': execution_time
            }
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"获取watchlist股票价格数据失败: {str(e)}，耗时{execution_time:.2f}秒")
            return {
                'total_tickers': 0,
                'success_count': total_success,
                'failed_count': total_failed,
                'execution_time': execution_time,
                'error': str(e)
            }


def create_eodhd_stock_prices_service() -> EODHDStockPricesService:
    """创建EODHD股价服务实例"""
    return EODHDStockPricesService()


# 测试函数
async def test_service():
    """测试服务功能"""
    logger.info("开始测试EODHD股价服务")
    
    try:
        service = create_eodhd_stock_prices_service()
        result = service.fetch_and_store_watchlist_prices()
        
        logger.info("测试完成", result=result)
        return result
        
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        return {'error': str(e)}


if __name__ == "__main__":
    # 直接运行测试
    result = asyncio.run(test_service())
    print(f"测试结果: {result}")
