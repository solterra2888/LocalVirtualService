# -*- coding: utf-8 -*-
"""
EOD Fetch Service - 独立的EOD数据获取服务
与现有的EODHD调度器服务分离，专门用于按需获取EOD数据
"""

import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.config.exchange_config import format_ticker_for_eodhd
from config.settings import config

logger = get_logger("eod_fetch_service")


class EODFetchService:
    """EOD数据获取服务"""
    
    def __init__(self):
        """初始化EOD获取服务"""
        self.api_token = config.get('EODHD_API_TOKEN')
        if not self.api_token:
            raise ValueError("EODHD_API_TOKEN not configured")
            
        self.base_url = "https://eodhd.com/api/eod"
        
        # 数据库连接参数
        self.db_conn_params = {
            'db_host': config.get('DB_HOST', 'localhost'),
            'db_port': config.get('DB_PORT', '5432'),
            'db_name': config.get('DB_NAME', 'targettracking'),
            'db_user': config.get('DB_USER', 'postgres'),
            'db_password': config.get('DB_PASSWORD', '')
        }
        
        self.db_manager = DatabaseManager(self.db_conn_params)
        
        # 创建会话
        self.session = self._create_session()
    
    def _create_session(self):
        """创建带有重试机制的HTTP会话"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    async def fetch_and_store_eod_data_with_timeout(
        self, 
        ticker: str, 
        period: str = "d",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        timeout_seconds: int = 30
    ) -> Dict[str, Any]:
        """
        带超时控制的EOD数据获取和存储
        
        Args:
            ticker: 股票代码
            period: 周期 (d/w/m)
            from_date: 开始日期
            to_date: 结束日期
            timeout_seconds: 超时秒数
            
        Returns:
            Dict包含操作结果
        """
        try:
            import asyncio
            
            # 使用asyncio.wait_for设置超时
            result = await asyncio.wait_for(
                self.fetch_and_store_eod_data(
                    ticker=ticker,
                    period=period,
                    from_date=from_date,
                    to_date=to_date
                ),
                timeout=timeout_seconds
            )
            
            return result
            
        except asyncio.TimeoutError:
            logger.warning(f"EOD数据获取超时 ({timeout_seconds}秒)", ticker=ticker)
            raise
        except Exception as e:
            logger.error(f"EOD数据获取失败: {str(e)}", ticker=ticker)
            raise

    async def fetch_and_store_eod_data(
        self, 
        ticker: str, 
        period: str = "d",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        获取并存储EOD数据
        
        Args:
            ticker: 股票代码
            period: 周期 (d/w/m)
            from_date: 开始日期
            to_date: 结束日期
            
        Returns:
            Dict包含操作结果
        """
        try:
            logger.info(f"开始获取EOD数据", ticker=ticker, period=period)
            
            # 格式化ticker以适配EODHD API
            formatted_ticker = format_ticker_for_eodhd(ticker)
            
            # 构建API请求参数
            params = {
                'api_token': self.api_token,
                'period': period,
                'fmt': 'json'
            }
            
            if from_date:
                params['from'] = from_date
            if to_date:
                params['to'] = to_date
            
            # 请求EODHD API
            url = f"{self.base_url}/{formatted_ticker}"
            logger.info(f"请求EODHD API", url=url, params={k: v for k, v in params.items() if k != 'api_token'})
            
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if not data:
                return {
                    "success": False,
                    "message": f"No data returned for {ticker}",
                    "total_records": 0,
                    "fetched_records": 0,
                    "stored_records": 0
                }
            
            logger.info(f"从EODHD获取到{len(data)}条数据", ticker=ticker)
            
            # 检查是否有警告信息
            warning = None
            if len(data) > 0 and isinstance(data[0], dict) and 'warning' in data[0]:
                warning = data[0]['warning']
            
            # 存储数据到数据库
            stored_count = await self._store_eod_data(ticker, data, period)
            
            return {
                "success": True,
                "message": f"Successfully fetched and stored EOD data for {ticker}",
                "total_records": len(data),
                "fetched_records": len(data),
                "stored_records": stored_count,
                "warning": warning
            }
            
        except requests.exceptions.RequestException as e:
            logger.error(f"EODHD API请求失败: {str(e)}", ticker=ticker)
            return {
                "success": False,
                "message": f"Failed to fetch data from EODHD API: {str(e)}",
                "total_records": 0,
                "fetched_records": 0,
                "stored_records": 0
            }
        except Exception as e:
            logger.error(f"EOD数据获取失败: {str(e)}", ticker=ticker)
            return {
                "success": False,
                "message": f"Failed to fetch EOD data: {str(e)}",
                "total_records": 0,
                "fetched_records": 0,
                "stored_records": 0
            }
    
    async def _store_eod_data(self, ticker: str, data: List[Dict], period: str) -> int:
        """
        分批存储EOD数据到数据库，避免重复，优化大批量数据处理
        
        Args:
            ticker: 股票代码
            data: EOD数据列表
            period: 周期
            
        Returns:
            存储的记录数
        """
        if not data:
            return 0
        
        try:
            import time
            import psycopg2.extras
            
            start_time = time.time()
            data_count = len(data)
            
            logger.info(f"开始分批存储EOD数据", ticker=ticker, total_records=data_count)
            
            # 准备批量插入数据
            from datetime import datetime
            now = datetime.now()
            insert_data = []
            for item in data:
                insert_data.append((
                    ticker,
                    item.get('date'),
                    float(item.get('open', 0)) if item.get('open') is not None else None,
                    float(item.get('high', 0)) if item.get('high') is not None else None,
                    float(item.get('low', 0)) if item.get('low') is not None else None,
                    float(item.get('close', 0)) if item.get('close') is not None else None,
                    int(item.get('volume', 0)) if item.get('volume') is not None else None,
                    float(item.get('adjusted_close', 0)) if item.get('adjusted_close') is not None else None,
                    float(item.get('split_coefficient', 1.0)),
                    float(item.get('dividend_amount', 0.0)),
                    period,
                    now,  # created_at
                    now   # updated_at
                ))
            
            # 动态计算批次大小，避免超时
            batch_size = self._calculate_optimal_batch_size(data_count)
            total_batches = (data_count + batch_size - 1) // batch_size
            
            logger.info(f"使用批次大小: {batch_size}, 共分 {total_batches} 个批次处理")
            
            # 使用优化的bulk upsert SQL
            query = """
                INSERT INTO stock_prices_eod 
                (code, date, open, high, low, close, volume, adjusted_close, split_coefficient, dividend_amount, period, created_at, updated_at)
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
            
            total_processed = 0
            
            # 分批处理数据
            for i in range(0, len(insert_data), batch_size):
                batch_data = insert_data[i:i + batch_size]
                batch_num = i // batch_size + 1
                
                logger.debug(f"处理批次 {batch_num}/{total_batches}: {len(batch_data)} 条记录")
                
                try:
                    with self.db_manager.get_connection() as conn:
                        with conn.cursor() as cursor:
                            # 使用 execute_values 进行批量插入，性能更好
                            psycopg2.extras.execute_values(
                                cursor, 
                                query, 
                                batch_data,
                                template=None,
                                page_size=batch_size
                            )
                            conn.commit()
                            total_processed += len(batch_data)
                            
                            logger.debug(f"批次 {batch_num} 处理完成: {len(batch_data)} 条记录")
                            
                except Exception as batch_error:
                    logger.error(f"批次 {batch_num} 处理失败: {str(batch_error)}")
                    # 继续处理下一批次，不中断整个过程
                    continue
            
            # 性能统计
            end_time = time.time()
            duration = end_time - start_time
            records_per_second = total_processed / duration if duration > 0 else 0
            
            logger.info(f"EOD数据存储完成", 
                       ticker=ticker, 
                       total_records=data_count,
                       processed_records=total_processed,
                       duration=f"{duration:.2f}s",
                       records_per_second=f"{records_per_second:.0f}")
            
            return total_processed
            
        except Exception as e:
            logger.error(f"存储EOD数据失败: {str(e)}", ticker=ticker)
            raise
    
    def _calculate_optimal_batch_size(self, total_records: int) -> int:
        """
        动态计算最优批次大小，避免超时
        
        Args:
            total_records: 总记录数
            
        Returns:
            最优批次大小
        """
        try:
            # 根据记录数量动态调整批次大小
            if total_records <= 1000:
                return min(500, total_records)
            elif total_records <= 5000:
                return 1000
            elif total_records <= 10000:
                return 2000
            else:
                # 对于非常大的数据集，使用较小的批次以避免超时
                return 1500
                
        except Exception:
            # 默认安全的批次大小
            return 1000
    
    async def get_stored_eod_data(
        self,
        ticker: str,
        period: str = "d",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        获取已存储的EOD数据
        
        Args:
            ticker: 股票代码
            period: 周期
            from_date: 开始日期
            to_date: 结束日期
            limit: 限制数量
            offset: 偏移量
            
        Returns:
            EOD数据列表
        """
        try:
            # 构建查询
            where_conditions = ["code = %s", "period = %s"]
            params = [ticker, period]
            
            if from_date:
                where_conditions.append("date >= %s")
                params.append(from_date)
            
            if to_date:
                where_conditions.append("date <= %s")
                params.append(to_date)
            
            where_clause = " AND ".join(where_conditions)
            
            query = f"""
                SELECT 
                    date, open, high, low, close, adjusted_close, volume,
                    split_coefficient, dividend_amount
                FROM stock_prices_eod 
                WHERE {where_clause}
                ORDER BY date DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
            if offset:
                query += f" OFFSET {offset}"
            
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, params)
                    rows = cursor.fetchall()
                    
                    # 转换为字典列表
                    columns = [desc[0] for desc in cursor.description]
                    data = [dict(zip(columns, row)) for row in rows]
                    
                    # 格式化数据
                    for item in data:
                        item['date'] = item['date'].strftime('%Y-%m-%d') if item['date'] else None
                        # 确保数值类型
                        for key in ['open', 'high', 'low', 'close', 'adjusted_close', 'split_coefficient', 'dividend_amount']:
                            if item[key] is not None:
                                item[key] = float(item[key])
                        if item['volume'] is not None:
                            item['volume'] = int(item['volume'])
            
            return data
            
        except Exception as e:
            logger.error(f"获取存储EOD数据失败: {str(e)}", ticker=ticker)
            raise
    
    async def get_eod_stats(self, ticker: str, period: str = "d") -> Dict[str, Any]:
        """
        获取EOD数据统计信息
        
        Args:
            ticker: 股票代码
            period: 周期
            
        Returns:
            统计信息字典
        """
        try:
            query = """
                SELECT 
                    COUNT(*) as total_records,
                    MIN(date) as earliest_date,
                    MAX(date) as latest_date,
                    MIN(low) as lowest_price,
                    MAX(high) as highest_price,
                    AVG(volume) as avg_volume,
                    SUM(volume) as total_volume
                FROM stock_prices_eod 
                WHERE code = %s AND period = %s
            """
            
            with self.db_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query, (ticker, period))
                    row = cursor.fetchone()
                    
                    if row:
                        return {
                            "ticker": ticker,
                            "period": period,
                            "total_records": row[0],
                            "earliest_date": row[1].strftime('%Y-%m-%d') if row[1] else None,
                            "latest_date": row[2].strftime('%Y-%m-%d') if row[2] else None,
                            "lowest_price": float(row[3]) if row[3] else None,
                            "highest_price": float(row[4]) if row[4] else None,
                            "avg_volume": int(row[5]) if row[5] else None,
                            "total_volume": int(row[6]) if row[6] else None
                        }
                    else:
                        return {
                            "ticker": ticker,
                            "period": period,
                            "total_records": 0,
                            "earliest_date": None,
                            "latest_date": None,
                            "lowest_price": None,
                            "highest_price": None,
                            "avg_volume": None,
                            "total_volume": None
                        }
            
        except Exception as e:
            logger.error(f"获取EOD统计失败: {str(e)}", ticker=ticker)
            raise
    
    def __del__(self):
        """清理资源"""
        if hasattr(self, 'session'):
            self.session.close()
