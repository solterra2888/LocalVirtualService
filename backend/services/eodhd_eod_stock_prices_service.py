# -*- coding: utf-8 -*-
"""
EODHD EOD Stock Prices Service - 历史股价数据获取服务

通过EODHD API获取股票的历史价格数据（End of Day）
数据来源：https://eodhd.com/api/eod/{SYMBOL}?from={DATE}&to={DATE}&period=d&api_token={TOKEN}&fmt=json

主要功能：
1. 获取用户watchlist中的所有ticker（从target_tracking_config表）
2. 获取主题关联公司的所有ticker（从theme_company_associations表）
3. 批量请求EODHD历史价格API
4. 解析并存储到stock_prices_eod表
5. 支持日期范围查询和增量更新
6. 自动处理股票代码格式转换
"""

import os
import json
import time
import asyncio
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Set

from backend.utils.datetime_utils import utcnow as _utcnow
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import psycopg2.extras

from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.config.exchange_config import ExchangeConfig, format_ticker_for_eodhd
# 统一的环境变量加载
from backend.utils.env_loader import load_project_env

logger = get_logger("eodhd_eod_stock_prices_service")


class EODHDEODStockPricesService:
    """EODHD历史股价数据服务"""
    
    def __init__(self):
        """初始化EODHD历史股价服务"""
        logger.info("开始初始化EODHD历史股价服务")
        
        try:
            # 获取API密钥
            self.api_token = os.getenv('EODHD_API_TOKEN')
            if not self.api_token:
                logger.error("EODHD_API_TOKEN 未在环境变量中设置")
                raise ValueError("EODHD_API_TOKEN 未在环境变量中设置")
            
            logger.info("API密钥获取成功", token_length=len(self.api_token))
            self.base_url = "https://eodhd.com/api/eod"
            logger.info("设置API基础URL", base_url=self.base_url)
            
            # 获取标准化数据库连接配置
            from backend.utils.database_config import get_database_config
            db_config = get_database_config()
            self.db_manager = DatabaseManager(db_config)
            logger.info("数据库管理器配置完成")
            
            # 配置参数 - 批量并发处理
            self.batch_size = int(os.getenv('EODHD_EOD_BATCH_SIZE', '100'))  # 每批处理的ticker数量
            self.max_workers = int(os.getenv('EODHD_EOD_MAX_WORKERS', '8'))  # 并发线程数（降低以避免超时）
            self.request_delay = float(os.getenv('EODHD_REQUEST_DELAY', '0.1'))  # 请求间隔（增加以减少限流）
            self.max_retries = int(os.getenv('EODHD_MAX_RETRIES_ON_RATE_LIMIT', '3'))
            self.request_timeout = int(os.getenv('EODHD_REQUEST_TIMEOUT', '60'))  # 请求超时时间（秒）
            
            # 创建优化的HTTP会话
            self.session = self._create_robust_session()
            logger.info("HTTP会话创建完成")
            
            logger.info("EODHD历史股价服务初始化完成",
                       batch_size=self.batch_size,
                       max_workers=self.max_workers,
                       request_delay=self.request_delay,
                       request_timeout=self.request_timeout,
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
        
        # 🔧 修复：设置连接池大小以匹配并发线程数
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=max(30, self.max_workers + 10)  # 连接池大小至少30，或并发数+10
        )
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
            
            logger.info(f"成功获取{len(tickers)}个watchlist ticker", tickers=tickers[:10])  # 只记录前10个
            return tickers
            
        except Exception as e:
            logger.error(f"获取watchlist ticker失败: {str(e)}")
            return []
    
    def get_theme_company_tickers(self) -> List[str]:
        """获取theme_company_associations表中已验证的所有不重复ticker"""
        try:
            logger.info("开始获取theme_company_associations中的ticker列表（仅已验证）")
            
            query = """
                SELECT DISTINCT ticker 
                FROM theme_company_associations 
                WHERE ticker IS NOT NULL 
                  AND is_verified = true
                ORDER BY ticker
            """
            
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    tickers = [row[0] for row in cursor.fetchall()]
            
            logger.info(f"成功获取{len(tickers)}个已验证的theme_company_associations ticker", tickers=tickers[:10])  # 只记录前10个
            return tickers
            
        except Exception as e:
            logger.error(f"获取theme_company_associations ticker失败: {str(e)}")
            return []
    
    def get_latest_date_for_ticker(self, ticker: str) -> Optional[str]:
        """获取指定ticker在数据库中的最新日期"""
        try:
            query = """
                SELECT MAX(date) as latest_date 
                FROM stock_prices_eod 
                WHERE code = %s
                AND period = 'd'
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
    
    def fetch_eod_data_for_ticker(self, ticker: str, from_date: str, to_date: str, period: str = 'd') -> Optional[List[Dict]]:
        """
        获取单个ticker的历史价格数据（智能优化版，支持多周期）
        
        Args:
            ticker: 股票代码
            from_date: 开始日期
            to_date: 结束日期
            period: 数据周期 ('d'=日线, 'w'=周线, 'm'=月线)
        
        Returns:
            历史价格数据列表，失败返回None
        """
        try:
            # 🔍 智能优化：仅对日线数据检查最新日期，避免请求已停牌/退市的股票
            if period == 'd':
                latest_date = self.get_latest_date_for_ticker(ticker)
                if latest_date:
                    from datetime import datetime
                    try:
                        latest = datetime.strptime(latest_date, '%Y-%m-%d')
                        request_from = datetime.strptime(from_date, '%Y-%m-%d')
                        days_since_last_trade = (request_from - latest).days
                        
                        # 如果最新数据超过180天（6个月）前，很可能已停牌/退市
                        if days_since_last_trade > 180:
                            logger.debug(f"跳过ticker {ticker}：最新交易日{latest_date}距请求日期已{days_since_last_trade}天，可能已停牌/退市")
                            return []  # 返回空列表，视为正常（无新数据）
                    except ValueError:
                        pass  # 日期解析失败，继续正常流程
            
            # 格式化ticker为EODHD格式
            formatted_ticker = format_ticker_for_eodhd(ticker)
            
            # 构建API URL
            url = f"{self.base_url}/{formatted_ticker}"
            params = {
                'from': from_date,
                'to': to_date,
                'period': period,  # 支持多种周期：d/w/m
                'api_token': self.api_token,
                'fmt': 'json'
            }
            
            logger.debug(f"请求EODHD API (period={period})", url=url, params=params)
            
            response = self.session.get(url, params=params, timeout=self.request_timeout)
            response.raise_for_status()
            
            data = response.json()
            
            if isinstance(data, list):
                if len(data) > 0:
                    logger.debug(f"成功获取ticker {ticker} period={period} 的{len(data)}条历史数据")
                    return data
                else:
                    # 返回空列表是正常情况（该期间无交易），使用debug级别
                    logger.debug(f"ticker {ticker} period={period} 在日期范围{from_date}至{to_date}内无交易数据")
                    return []
            else:
                logger.warning(f"ticker {ticker} period={period} API返回非列表数据: {type(data)}")
                return []
                
        except requests.exceptions.RequestException as e:
            logger.error(f"请求EODHD API失败 - ticker {ticker} period={period}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"获取ticker {ticker} period={period} 历史数据失败: {str(e)}")
            return None
    
    def store_eod_data(self, ticker: str, eod_data: List[Dict], period: str = 'd') -> int:
        """
        存储历史价格数据到数据库
        
        Args:
            ticker: 股票代码
            eod_data: 历史价格数据列表
            period: 数据周期 ('d'=日线, 'w'=周线, 'm'=月线)
        
        Returns:
            成功存储的记录数
        """
        if not eod_data:
            return 0
        
        try:
            logger.debug(f"开始存储ticker {ticker} period={period} 的{len(eod_data)}条历史数据")
            
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
                    period  # 使用传入的period参数
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
            
            logger.debug(f"成功存储ticker {ticker} period={period} 的{len(eod_data)}条历史数据")
            return len(eod_data)
            
        except Exception as e:
            logger.error(f"存储ticker {ticker} period={period} 历史数据失败: {str(e)}")
            return 0
    
    def concurrent_fetch_eod_data(self, tickers: List[str], start_date: str, end_date: str, period: str = 'd', max_workers: int = None) -> Dict[str, List[Dict]]:
        """
        🚀 并发获取多个ticker的历史价格数据（性能优化版本，支持多周期）
        
        Args:
            tickers: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            period: 数据周期 ('d'=日线, 'w'=周线, 'm'=月线)
            max_workers: 最大并发线程数
        
        Returns:
            字典: {ticker: eod_data_list}
        """
        
        def fetch_single_ticker(ticker):
            """获取单个ticker的数据"""
            try:
                eod_data = self.fetch_eod_data_for_ticker(ticker, start_date, end_date, period)
                if eod_data is not None:
                    return (ticker, eod_data, True)
                else:
                    return (ticker, None, False)
            except Exception as e:
                logger.error(f"并发获取ticker {ticker} period={period} 失败: {str(e)}")
                return (ticker, None, False)
        
        # 使用线程池并发执行
        if max_workers is None:
            max_workers = self.max_workers
        
        results = {}
        successful_count = 0
        failed_count = 0
        
        logger.info(f"🚀 开始并发获取 {len(tickers)} 个ticker period={period} 的历史数据，并发数: {max_workers}")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_ticker = {
                executor.submit(fetch_single_ticker, ticker): ticker 
                for ticker in tickers
            }
            
            # 处理完成的任务
            for future in as_completed(future_to_ticker):
                ticker, eod_data, success = future.result()
                
                if success and eod_data:
                    results[ticker] = eod_data
                    successful_count += 1
                else:
                    results[ticker] = None
                    failed_count += 1
        
        logger.info(f"✅ 并发获取完成 period={period}: 成功 {successful_count}/{len(tickers)}, 失败 {failed_count}")
        
        return results
    
    def batch_store_eod_data(self, ticker_data_map: Dict[str, List[Dict]], period: str = 'd') -> Dict[str, int]:
        """
        🚀 真正的批量存储：使用execute_values一次性插入所有ticker的历史价格数据
        
        Args:
            ticker_data_map: {ticker: eod_data_list} 字典
            period: 数据周期 ('d'=日线, 'w'=周线, 'm'=月线)
        
        Returns:
            {ticker: records_count} 字典
        """
        if not ticker_data_map:
            return {}
        
        try:
            results = {}
            all_insert_data = []  # 收集所有数据
            
            # 1. 收集所有ticker的所有记录
            for ticker, eod_data in ticker_data_map.items():
                if not eod_data:
                    results[ticker] = 0
                    continue
                
                ticker_count = 0
                for item in eod_data:
                    all_insert_data.append((
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
                        period  # 使用传入的period参数
                    ))
                    ticker_count += 1
                
                results[ticker] = ticker_count
            
            if not all_insert_data:
                return results
            
            # 2. 🚀 使用execute_values一次性批量插入所有数据（关键优化！）
            upsert_sql = """
                INSERT INTO stock_prices_eod 
                (code, date, open, high, low, close, volume, adjusted_close, split_coefficient, dividend_amount, period)
                VALUES %s
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
                    # 🚀 使用execute_values批量插入，比executemany快得多
                    psycopg2.extras.execute_values(
                        cursor, 
                        upsert_sql, 
                        all_insert_data,
                        template=None, 
                        page_size=1000  # 每页1000条记录
                    )
                    conn.commit()
            
            logger.info(f"💾 批量存储完成 period={period}: {len(all_insert_data)}条记录 来自{len([k for k,v in ticker_data_map.items() if v])}个ticker")
            
            return results
            
        except Exception as e:
            logger.error(f"批量存储失败 period={period}: {str(e)}")
            # 失败时回退到逐个存储
            logger.warning("回退到逐个存储模式...")
            results = {}
            for ticker, eod_data in ticker_data_map.items():
                if eod_data:
                    records_stored = self.store_eod_data(ticker, eod_data, period)
                    results[ticker] = records_stored
                else:
                    results[ticker] = 0
            return results
    
    def fetch_and_store_watchlist_eod_prices(self, days_back: int = 60, periods: List[str] = None) -> Dict[str, Any]:
        """
        🚀 批量并发获取并存储watchlist和theme_company_associations中所有股票的历史价格数据（支持多周期）
        
        Args:
            days_back: 回溯天数
            periods: 数据周期列表 ['d', 'w', 'm']，默认获取所有周期
        
        Returns:
            执行结果字典
        """
        start_time = time.time()
        
        # 默认获取所有周期：日线、周线、月线
        if periods is None:
            periods = ['d', 'w', 'm']
        
        logger.info(f"🚀 开始批量并发获取历史价格数据，回溯{days_back}天，周期: {periods}")
        
        try:
            # 获取watchlist中的ticker
            watchlist_tickers = self.get_watchlist_tickers()
            logger.info(f"从watchlist获取{len(watchlist_tickers)}个ticker")
            
            # 获取theme_company_associations中的ticker
            theme_tickers = self.get_theme_company_tickers()
            logger.info(f"从theme_company_associations获取{len(theme_tickers)}个ticker")
            
            # 合并并去重ticker列表
            all_tickers_set = set(watchlist_tickers) | set(theme_tickers)
            tickers = sorted(list(all_tickers_set))
            
            logger.info(f"合并后共有{len(tickers)}个不重复ticker",
                       watchlist_count=len(watchlist_tickers),
                       theme_count=len(theme_tickers),
                       total_unique=len(tickers))
            
            if not tickers:
                logger.warning("没有找到需要处理的ticker")
                return {
                    'success': True,
                    'message': '没有ticker需要处理',
                    'total_tickers': 0,
                    'watchlist_tickers': 0,
                    'theme_tickers': 0,
                    'success_count': 0,
                    'failed_count': 0,
                    'total_records': 0,
                    'periods': periods,
                    'execution_time': time.time() - start_time
                }
            
            # 计算日期范围。用 utcnow() 避免 HK 午夜时 datetime.now() 给出明天的 HK 日期，
            # 导致 EODHD 对"今天"日期查询返回空数据（明天尚无收盘价）。
            _now = _utcnow()
            end_date = _now.strftime('%Y-%m-%d')
            start_date = (_now - timedelta(days=days_back)).strftime('%Y-%m-%d')
            
            # 🔄 遍历所有周期
            period_results = {}
            total_records_all_periods = 0
            
            for period in periods:
                logger.info(f"📊 开始处理周期 period={period}")
                period_start_time = time.time()
                
                logger.info(f"🚀 批量配置: batch_size={self.batch_size}, max_workers={self.max_workers}")
                
                total_success_count = 0
                total_failed_count = 0
                total_records = 0
                
                # 🚀 分批并发处理ticker
                for batch_start in range(0, len(tickers), self.batch_size):
                    batch_end = min(batch_start + self.batch_size, len(tickers))
                    batch_tickers = tickers[batch_start:batch_end]
                    batch_num = batch_start // self.batch_size + 1
                    total_batches = (len(tickers) + self.batch_size - 1) // self.batch_size
                    
                    logger.info(f"🚀 处理批次 {batch_num}/{total_batches} period={period}: {len(batch_tickers)} 个ticker ({batch_start+1}-{batch_end}/{len(tickers)})")
                    
                    batch_start_time = time.time()
                    
                    # 1. 🚀 并发获取API数据（传入period参数）
                    ticker_data_map = self.concurrent_fetch_eod_data(batch_tickers, start_date, end_date, period)
                    
                    batch_fetch_time = time.time() - batch_start_time
                    
                    # 2. 批量存储数据（传入period参数）
                    batch_save_start_time = time.time()
                    store_results = self.batch_store_eod_data(ticker_data_map, period)
                    batch_save_time = time.time() - batch_save_start_time
                    
                    # 统计本批次结果
                    batch_success = sum(1 for v in store_results.values() if v > 0)
                    batch_failed = len(batch_tickers) - batch_success
                    batch_records = sum(store_results.values())
                    
                    total_success_count += batch_success
                    total_failed_count += batch_failed
                    total_records += batch_records
                    
                    batch_total_time = time.time() - batch_start_time
                    
                    logger.info(f"✅ 批次 {batch_num}/{total_batches} period={period} 完成: "
                              f"成功 {batch_success}/{len(batch_tickers)}, "
                              f"失败 {batch_failed}, "
                              f"存储 {batch_records} 条记录, "
                              f"获取用时 {batch_fetch_time:.1f}s, "
                              f"存储用时 {batch_save_time:.1f}s, "
                              f"总用时 {batch_total_time:.1f}s "
                              f"(速度: {batch_success/batch_total_time:.1f} ticker/秒)")
                    
                    # 批次间短暂延迟
                    if batch_end < len(tickers):
                        time.sleep(0.5)
                
                period_execution_time = time.time() - period_start_time
                
                # 记录当前周期的结果
                period_results[period] = {
                    'success_count': total_success_count,
                    'failed_count': total_failed_count,
                    'total_records': total_records,
                    'execution_time': period_execution_time
                }
                
                total_records_all_periods += total_records
                
                logger.info(f"✅ 周期 period={period} 完成: "
                          f"成功 {total_success_count}/{len(tickers)}, "
                          f"失败 {total_failed_count}, "
                          f"存储 {total_records} 条记录, "
                          f"用时 {period_execution_time:.1f}s")
            
            execution_time = time.time() - start_time
            
            result = {
                'success': True,
                'message': f'🚀 批量并发历史价格数据获取完成（{len(periods)}个周期）',
                'total_tickers': len(tickers),
                'watchlist_tickers': len(watchlist_tickers),
                'theme_tickers': len(theme_tickers),
                'periods': periods,
                'period_results': period_results,
                'total_records': total_records_all_periods,
                'execution_time': execution_time,
                'date_range': f'{start_date} to {end_date}',
                'avg_speed': f'{len(tickers)*len(periods)/execution_time:.2f} ticker*period/秒' if execution_time > 0 else 'N/A'
            }
            
            logger.info("🚀 批量并发历史价格数据获取任务完成", 
                       total_tickers=result['total_tickers'],
                       watchlist_tickers=result['watchlist_tickers'],
                       theme_tickers=result['theme_tickers'],
                       periods=result['periods'],
                       period_results=result['period_results'],
                       total_records=result['total_records'],
                       execution_time=result['execution_time'],
                       avg_speed=result['avg_speed'],
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
            # 计算日期范围（UTC，避免 HK 午夜日期偏移）
            _now = _utcnow()
            end_date = _now.strftime('%Y-%m-%d')
            start_date = (_now - timedelta(days=days_back)).strftime('%Y-%m-%d')
            
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
