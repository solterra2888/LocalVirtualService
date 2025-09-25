# -*- coding: utf-8 -*-
"""
EODHD EOD Stock Prices Service - 历史股价数据获取服务

通过EODHD API获取用户watchlist中所有股票的历史价格数据（End of Day）
数据来源：https://eodhd.com/api/eod/{SYMBOL}?from={DATE}&to={DATE}&period=d&api_token={TOKEN}&fmt=json

主要功能：
1. 获取用户watchlist中的所有ticker
2. 批量请求EODHD历史价格API
3. 解析并存储到stock_prices_eod表
4. 支持日期范围查询和增量更新
5. 自动处理股票代码格式转换
"""

import os
import json
import time
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Set
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import psycopg2.extras

from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.config.exchange_config import ExchangeConfig, format_ticker_for_eodhd
from config.settings import config

logger = get_logger("eodhd_eod_stock_prices_service")


class EODHDEODStockPricesService:
    """EODHD历史股价数据服务"""
    
    def __init__(self):
        """初始化EODHD历史股价服务"""
        logger.info("开始初始化EODHD历史股价服务")
        
        try:
            # 获取API密钥
            self.api_token = config.get('EODHD_API_TOKEN')
            if not self.api_token:
                logger.error("EODHD_API_TOKEN 未在环境变量中设置")
                raise ValueError("EODHD_API_TOKEN 未在环境变量中设置")
            
            logger.info("API密钥获取成功", token_length=len(self.api_token))
            self.base_url = "https://eodhd.com/api/eod"
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
            
            logger.info("EODHD历史股价服务初始化完成",
                       batch_size=self.batch_size,
                       request_delay=self.request_delay,
                       max_retries=self.max_retries)
            
        except Exception as e:
            logger.error(f"初始化EODHD历史股价服务失败: {str(e)}")
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
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        })
        
        return session
    
    def get_watchlist_tickers(self) -> List[str]:
        """获取用户watchlist中的所有ticker"""
        try:
            logger.info("开始获取watchlist中的ticker列表")
            
            query = """
                SELECT DISTINCT primaryticker 
                FROM target_tracking_config 
                WHERE enabled = true AND primaryticker IS NOT NULL
                ORDER BY primaryticker
            """
            
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    tickers = [row[0] for row in cursor.fetchall()]
            
            logger.info(f"成功获取{len(tickers)}个ticker", tickers=tickers[:10])  # 只记录前10个
            return tickers
            
        except Exception as e:
            logger.error(f"获取watchlist ticker失败: {str(e)}")
            return []
    
    def get_latest_date_for_ticker(self, ticker: str) -> Optional[str]:
        """获取指定ticker在数据库中的最新日期"""
        try:
            query = """
                SELECT MAX(date) as latest_date 
                FROM stock_prices_eod 
                WHERE code = %s
            """
            
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (ticker,))
                    result = cursor.fetchone()
                    if result and result[0]:
                        return result[0].strftime('%Y-%m-%d')
                    return None
                    
        except Exception as e:
            logger.error(f"获取ticker {ticker} 最新日期失败: {str(e)}")
            return None
    
    def fetch_eod_data_for_ticker(self, ticker: str, from_date: str, to_date: str) -> Optional[List[Dict]]:
        """获取单个ticker的历史价格数据"""
        try:
            # 格式化ticker为EODHD格式
            formatted_ticker = format_ticker_for_eodhd(ticker)
            
            # 构建API URL
            url = f"{self.base_url}/{formatted_ticker}"
            params = {
                'from': from_date,
                'to': to_date,
                'period': 'd',  # 日线数据
                'api_token': self.api_token,
                'fmt': 'json'
            }
            
            logger.debug(f"请求EODHD API", url=url, params=params)
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if isinstance(data, list) and len(data) > 0:
                logger.debug(f"成功获取ticker {ticker} 的{len(data)}条历史数据")
                return data
            else:
                logger.warning(f"ticker {ticker} 没有返回数据")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"请求EODHD API失败 - ticker {ticker}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"获取ticker {ticker} 历史数据失败: {str(e)}")
            return None
    
    def store_eod_data(self, ticker: str, eod_data: List[Dict]) -> int:
        """存储历史价格数据到数据库"""
        if not eod_data:
            return 0
        
        try:
            logger.debug(f"开始存储ticker {ticker} 的{len(eod_data)}条历史数据")
            
            # 准备批量插入数据
            insert_data = []
            for item in eod_data:
                insert_data.append((
                    ticker,
                    item.get('date'),
                    item.get('open'),
                    item.get('high'),
                    item.get('low'),
                    item.get('close'),
                    item.get('volume'),
                    item.get('adjusted_close'),
                    item.get('split_coefficient', 1.0),
                    item.get('dividend_amount', 0.0),
                    'd'  # period
                ))
            
            # 使用ON CONFLICT处理重复数据
            query = """
                INSERT INTO stock_prices_eod 
                (code, date, open, high, low, close, volume, adjusted_close, split_coefficient, dividend_amount, period)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (code, date, period) 
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    adjusted_close = EXCLUDED.adjusted_close,
                    split_coefficient = EXCLUDED.split_coefficient,
                    dividend_amount = EXCLUDED.dividend_amount,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.executemany(query, insert_data)
                    conn.commit()
            
            logger.debug(f"成功存储ticker {ticker} 的{len(eod_data)}条历史数据")
            return len(eod_data)
            
        except Exception as e:
            logger.error(f"存储ticker {ticker} 历史数据失败: {str(e)}")
            return 0
    
    def fetch_and_store_watchlist_eod_prices(self, days_back: int = 60) -> Dict[str, Any]:
        """获取并存储watchlist中所有股票的历史价格数据"""
        start_time = time.time()
        logger.info(f"开始获取watchlist历史价格数据，回溯{days_back}天")
        
        try:
            # 获取watchlist中的ticker
            tickers = self.get_watchlist_tickers()
            if not tickers:
                logger.warning("watchlist中没有找到ticker")
                return {
                    'success': True,
                    'message': 'watchlist中没有ticker需要处理',
                    'total_tickers': 0,
                    'success_count': 0,
                    'failed_count': 0,
                    'total_records': 0,
                    'execution_time': time.time() - start_time
                }
            
            logger.info(f"开始处理{len(tickers)}个ticker的历史价格数据")
            
            # 计算日期范围
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            
            success_count = 0
            failed_count = 0
            total_records = 0
            
            # 批量处理ticker
            for i, ticker in enumerate(tickers):
                try:
                    logger.info(f"处理ticker {ticker} ({i+1}/{len(tickers)})")
                    
                    # 获取历史数据
                    eod_data = self.fetch_eod_data_for_ticker(ticker, start_date, end_date)
                    
                    if eod_data is not None:
                        # 存储数据
                        records_stored = self.store_eod_data(ticker, eod_data)
                        total_records += records_stored
                        success_count += 1
                        logger.info(f"ticker {ticker} 处理成功，存储了{records_stored}条记录")
                    else:
                        failed_count += 1
                        logger.warning(f"ticker {ticker} 处理失败")
                    
                    # 请求间隔
                    if i < len(tickers) - 1:  # 不是最后一个
                        time.sleep(self.request_delay)
                        
                except Exception as e:
                    failed_count += 1
                    logger.error(f"处理ticker {ticker} 时发生异常: {str(e)}")
            
            execution_time = time.time() - start_time
            
            result = {
                'success': True,
                'message': f'历史价格数据获取完成',
                'total_tickers': len(tickers),
                'success_count': success_count,
                'failed_count': failed_count,
                'total_records': total_records,
                'execution_time': execution_time,
                'date_range': f'{start_date} to {end_date}'
            }
            
            logger.info("历史价格数据获取任务完成", 
                       total_tickers=result['total_tickers'],
                       success_count=result['success_count'],
                       failed_count=result['failed_count'],
                       total_records=result['total_records'],
                       execution_time=result['execution_time'],
                       date_range=result['date_range'])
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"获取历史价格数据失败: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'execution_time': execution_time
            }
    
    def fetch_and_store_ticker_eod_prices(self, ticker: str, days_back: int = 60) -> Dict[str, Any]:
        """获取并存储指定ticker的历史价格数据"""
        start_time = time.time()
        logger.info(f"开始获取ticker {ticker} 的历史价格数据，回溯{days_back}天")
        
        try:
            # 计算日期范围
            end_date = datetime.now().strftime('%Y-%m-%d')
            start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
            
            # 获取历史数据
            eod_data = self.fetch_eod_data_for_ticker(ticker, start_date, end_date)
            
            if eod_data is None:
                return {
                    'success': False,
                    'error': f'无法获取ticker {ticker} 的历史数据',
                    'execution_time': time.time() - start_time
                }
            
            # 存储数据
            records_stored = self.store_eod_data(ticker, eod_data)
            
            execution_time = time.time() - start_time
            
            result = {
                'success': True,
                'message': f'ticker {ticker} 历史价格数据获取完成',
                'ticker': ticker,
                'records_stored': records_stored,
                'execution_time': execution_time,
                'date_range': f'{start_date} to {end_date}'
            }
            
            logger.info("单个ticker历史价格数据获取完成", 
                       ticker=result['ticker'],
                       records_stored=result['records_stored'],
                       execution_time=result['execution_time'],
                       date_range=result['date_range'])
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"获取ticker {ticker} 历史价格数据失败: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'execution_time': execution_time
            }


def create_eodhd_eod_stock_prices_service() -> EODHDEODStockPricesService:
    """创建EODHD历史股价服务实例"""
    return EODHDEODStockPricesService()
