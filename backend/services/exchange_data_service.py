# -*- coding: utf-8 -*-
"""
EODHD Exchange Data Service
基于参考代码实现的交易所数据管理服务
"""

import os
import json
import time
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Callable
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
import psycopg2.extras

from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from config.settings import config

logger = get_logger("exchange_data_service")


class ExchangeDataService:
    """交易所数据管理服务"""
    
    def __init__(self):
        """初始化交易所数据服务"""
        logger.info("开始初始化交易所数据服务")
        
        # 获取API密钥
        self.api_key = config.get('EODHD_API_TOKEN') or os.getenv('EODHD_API_TOKEN')
        if not self.api_key:
            raise ValueError("EODHD_API_TOKEN未设置。请在配置文件或环境变量中设置API密钥。")
        
        # 数据库连接配置
        db_config = {
            'db_host': config.get('DB_HOST', 'localhost'),
            'db_port': config.get('DB_PORT', '5432'),
            'db_name': config.get('DB_NAME', 'targettracking'),
            'db_user': config.get('DB_USER', 'postgres'),
            'db_password': config.get('DB_PASSWORD', '')
        }
        self.db_manager = DatabaseManager(db_config)
        
        # 支持的交易所配置
        self.supported_exchanges = {
            # 美国交易所
            'US': {
                'name': 'USA Stocks',
                'country': 'USA',
                'timezone': 'America/New_York',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/US',
                'mic_code': 'XNAS,XNYS'
            },
            
            # 欧洲主要交易所
            'LSE': {
                'name': 'London Stock Exchange',
                'country': 'UK',
                'timezone': 'Europe/London',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/LSE',
                'mic_code': 'XLON'
            },
            'PA': {
                'name': 'Euronext Paris',
                'country': 'France',
                'timezone': 'Europe/Paris',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/PA',
                'mic_code': 'XPAR'
            },
            'AS': {
                'name': 'Euronext Amsterdam',
                'country': 'Netherlands',
                'timezone': 'Europe/Amsterdam',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/AS',
                'mic_code': 'XAMS'
            },
            'BR': {
                'name': 'Euronext Brussels',
                'country': 'Belgium',
                'timezone': 'Europe/Brussels',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/BR',
                'mic_code': 'XBRU'
            },
            'VX': {
                'name': 'Swiss Exchange (SIX)',
                'country': 'Switzerland',
                'timezone': 'Europe/Zurich',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/VX',
                'mic_code': 'XSWX'
            },
            'MC': {
                'name': 'Madrid Stock Exchange',
                'country': 'Spain',
                'timezone': 'Europe/Madrid',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/MC',
                'mic_code': 'BMEX'
            },
            'F': {
                'name': 'Frankfurt Stock Exchange',
                'country': 'Germany',
                'timezone': 'Europe/Berlin',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/F',
                'mic_code': 'XFRA'
            },
            
            # 亚太交易所
            'HK': {
                'name': 'Hong Kong Stock Exchange',
                'country': 'Hong Kong',
                'timezone': 'Asia/Hong_Kong',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/HK',
                'mic_code': 'XHKG'
            },
            'SHG': {
                'name': 'Shanghai Stock Exchange',
                'country': 'China',
                'timezone': 'Asia/Shanghai',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/SHG',
                'mic_code': 'XSHG'
            },
            'SHE': {
                'name': 'Shenzhen Stock Exchange',
                'country': 'China',
                'timezone': 'Asia/Shanghai',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/SHE',
                'mic_code': 'XSHE'
            },
            'KO': {
                'name': 'Korea Stock Exchange (KRX)',
                'country': 'South Korea',
                'timezone': 'Asia/Seoul',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/KO',
                'mic_code': 'XKRX'
            },
            'TW': {
                'name': 'Taiwan Stock Exchange',
                'country': 'Taiwan',
                'timezone': 'Asia/Taipei',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/TW',
                'mic_code': 'XTAI'
            },
            'SG': {
                'name': 'Singapore Exchange',
                'country': 'Singapore',
                'timezone': 'Asia/Singapore',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/SG',
                'mic_code': 'XSES'
            },
            'AU': {
                'name': 'Australian Securities Exchange',
                'country': 'Australia',
                'timezone': 'Australia/Sydney',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/AU',
                'mic_code': 'XASX'
            },
            
            # 北美其他交易所
            'TO': {
                'name': 'Toronto Stock Exchange',
                'country': 'Canada',
                'timezone': 'America/Toronto',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/TO',
                'mic_code': 'XTSE'
            },
            
            # 印度交易所
            'BSE': {
                'name': 'Bombay Stock Exchange',
                'country': 'India',
                'timezone': 'Asia/Kolkata',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/BSE',
                'mic_code': 'XBOM'
            },
            'NSE': {
                'name': 'National Stock Exchange of India',
                'country': 'India',
                'timezone': 'Asia/Kolkata',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/NSE',
                'mic_code': 'XNSE'
            },
            
            # 南美交易所
            'SA': {
                'name': 'São Paulo Stock Exchange (B3)',
                'country': 'Brazil',
                'timezone': 'America/Sao_Paulo',
                'api_endpoint': 'https://eodhd.com/api/exchange-symbol-list/SA',
                'mic_code': 'BVMF'
            }
        }
        
        # 初始化数据库表
        self._init_database()
        
        logger.info(f"交易所数据服务初始化完成 - 支持 {len(self.supported_exchanges)} 个交易所")
    
    def _init_database(self):
        """初始化数据库表结构"""
        try:
            # 创建交易所配置表
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS exchange_configs (
                    id SERIAL PRIMARY KEY,
                    exchange_code TEXT UNIQUE NOT NULL,
                    exchange_name TEXT NOT NULL,
                    country TEXT,
                    timezone TEXT,
                    mic_code TEXT,
                    is_enabled BOOLEAN DEFAULT TRUE,
                    last_update TIMESTAMP,
                    total_tickers INTEGER DEFAULT 0,
                    api_endpoint TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            self.db_manager.execute_query(create_table_sql)
            
            # 为companies表添加字段（如果不存在）
            try:
                self.db_manager.execute_query("ALTER TABLE companies ADD COLUMN IF NOT EXISTS data_source TEXT DEFAULT 'manual'")
                self.db_manager.execute_query("ALTER TABLE companies ADD COLUMN IF NOT EXISTS ticker_active BOOLEAN DEFAULT TRUE")
                self.db_manager.execute_query("ALTER TABLE companies ADD COLUMN IF NOT EXISTS last_ticker_update TIMESTAMP")
            except Exception:
                pass  # 字段已存在
            
            # 插入或更新交易所配置
            for code, config in self.supported_exchanges.items():
                upsert_sql = """
                    INSERT INTO exchange_configs 
                    (exchange_code, exchange_name, country, timezone, mic_code, api_endpoint, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (exchange_code) DO UPDATE SET
                        exchange_name = EXCLUDED.exchange_name,
                        country = EXCLUDED.country,
                        timezone = EXCLUDED.timezone,
                        mic_code = EXCLUDED.mic_code,
                        api_endpoint = EXCLUDED.api_endpoint,
                        updated_at = EXCLUDED.updated_at
                """
                self.db_manager.execute_query(upsert_sql, [
                    code, config['name'], config['country'], 
                    config['timezone'], config.get('mic_code'), 
                    config['api_endpoint'], datetime.now()
                ])
            
            logger.debug("数据库表结构初始化完成")
            
        except Exception as e:
            logger.error(f"数据库初始化失败: {str(e)}")
            raise
    
    def create_robust_session(self):
        """创建一个具有重试机制的会话"""
        session = requests.Session()
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def safe_api_request(self, url: str, session: requests.Session, max_retries: int = 3):
        """安全的API请求函数"""
        for attempt in range(max_retries):
            try:
                logger.debug(f"API请求 (第{attempt + 1}次): {url}")
                response = session.get(url, timeout=(10, 30))
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"请求错误 (第{attempt + 1}次): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                else:
                    logger.error("达到最大重试次数，API请求失败")
                    raise
    
    async def get_supported_exchanges(self) -> List[Dict[str, Any]]:
        """获取支持的交易所列表"""
        try:
            query = """
                SELECT exchange_code, exchange_name, country, timezone, mic_code, is_enabled, 
                       last_update, total_tickers, created_at
                FROM exchange_configs
                ORDER BY exchange_code
            """
            
            results = self.db_manager.fetch_all(query)
            
            exchanges = []
            for row in results:
                exchanges.append({
                    'code': row[0],
                    'name': row[1],
                    'country': row[2],
                    'timezone': row[3],
                    'mic_code': row[4],
                    'enabled': bool(row[5]),
                    'last_update': row[6].isoformat() if row[6] else None,
                    'total_tickers': row[7] or 0,
                    'created_at': row[8].isoformat() if row[8] else None
                })
            
            return exchanges
            
        except Exception as e:
            logger.error(f"获取交易所列表失败: {str(e)}")
            return []
    
    async def fetch_exchange_tickers(self, exchange_code: str, progress_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """获取指定交易所的ticker数据"""
        try:
            if exchange_code not in self.supported_exchanges:
                raise ValueError(f"不支持的交易所: {exchange_code}")
            
            exchange_config = self.supported_exchanges[exchange_code]
            
            logger.info(f"开始获取 {exchange_code} 交易所的ticker数据")
            
            if progress_callback:
                await progress_callback(f"正在请求 {exchange_code} 数据...")
            
            # 构建API URL
            url = f"{exchange_config['api_endpoint']}?api_token={self.api_key}&fmt=json"
            
            # 创建会话并请求数据
            session = self.create_robust_session()
            try:
                tickers_data = self.safe_api_request(url, session)
            finally:
                session.close()
            
            if not isinstance(tickers_data, list):
                raise ValueError("API返回的数据格式不正确")
            
            if progress_callback:
                await progress_callback(f"获取到 {len(tickers_data)} 个ticker，开始保存...")
            
            # 保存到数据库
            success_count = await self._save_tickers_data(exchange_code, tickers_data, progress_callback)
            
            logger.info(f"{exchange_code} ticker数据获取完成，成功保存 {success_count} 个")
            
            return {
                'success': True,
                'exchange_code': exchange_code,
                'total_fetched': len(tickers_data),
                'success_count': success_count,
                'message': f'成功获取 {success_count} 个ticker'
            }
            
        except Exception as e:
            error_msg = f"获取 {exchange_code} ticker数据失败: {str(e)}"
            logger.error(error_msg)
            if progress_callback:
                await progress_callback(error_msg)
            return {'success': False, 'message': error_msg}
    
    async def _save_tickers_data(self, exchange_code: str, tickers_data: List[Dict[str, Any]], 
                               progress_callback: Optional[Callable] = None) -> int:
        """保存ticker数据到数据库 - 优化批量插入版本"""
        success_count = 0
        
        try:
            # 准备批量插入的数据
            insert_data = []
            
            logger.info(f"开始准备 {len(tickers_data)} 个ticker的批量插入数据")
            if progress_callback:
                await progress_callback(f"正在准备 {len(tickers_data)} 个ticker的批量插入数据...")
            
            for i, ticker in enumerate(tickers_data):
                try:
                    # 每100个ticker报告一次进度
                    if progress_callback and i % 100 == 0:
                        await progress_callback(f"正在处理第 {i+1}/{len(tickers_data)} 个ticker...")
                    
                    # 准备插入数据
                    ticker_data = (
                        ticker.get('Code'),
                        ticker.get('Name'),
                        ticker.get('Exchange', exchange_code),
                        ticker.get('Type'),
                        ticker.get('Currency', 'USD'),
                        ticker.get('Country'),
                        ticker.get('Isin'),
                        exchange_code.lower(),  # 数据来源
                        True,  # ticker活跃状态
                        datetime.now(),  # 最后更新时间
                        datetime.now(),  # 创建时间
                        datetime.now()   # 更新时间
                    )
                    
                    insert_data.append(ticker_data)
                    
                except Exception as e:
                    logger.warning(f"准备ticker数据失败 {ticker.get('Code', 'Unknown')}: {str(e)}")
                    continue
            
            if not insert_data:
                logger.warning("没有有效的ticker数据可以插入")
                return 0
            
            # 使用批量插入优化性能
            if progress_callback:
                await progress_callback(f"开始批量插入 {len(insert_data)} 个ticker到数据库...")
            
            logger.info(f"使用批量插入保存 {len(insert_data)} 个ticker")
            
            # 使用批量操作
            batch_size = 1000  # 每批次处理1000条记录
            total_batches = (len(insert_data) + batch_size - 1) // batch_size
            
            for batch_num in range(total_batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(insert_data))
                batch_data = insert_data[start_idx:end_idx]
                
                if progress_callback:
                    await progress_callback(f"正在处理第 {batch_num + 1}/{total_batches} 批次 ({len(batch_data)} 条记录)...")
                
                # 批量插入SQL
                upsert_sql = """
                    INSERT INTO companies (
                        code, name, exchange, type, currencycode, countryname, isin,
                        data_source, ticker_active, last_ticker_update, created_at, updated_at
                    ) VALUES %s
                    ON CONFLICT (code) DO UPDATE SET
                        name = EXCLUDED.name,
                        exchange = EXCLUDED.exchange,
                        type = EXCLUDED.type,
                        currencycode = EXCLUDED.currencycode,
                        countryname = EXCLUDED.countryname,
                        isin = EXCLUDED.isin,
                        data_source = EXCLUDED.data_source,
                        ticker_active = EXCLUDED.ticker_active,
                        last_ticker_update = EXCLUDED.last_ticker_update,
                        updated_at = EXCLUDED.updated_at
                """
                
                try:
                    # 使用psycopg2的execute_values进行批量插入
                    conn = self.db_manager.get_connection()
                    if not conn:
                        raise Exception("无法获取数据库连接")
                    
                    cursor = conn.cursor()
                    try:
                        psycopg2.extras.execute_values(
                            cursor, upsert_sql, batch_data, 
                            template=None, page_size=100
                        )
                        conn.commit()
                        success_count += len(batch_data)
                        
                    finally:
                        conn.close()
                        
                    logger.debug(f"批次 {batch_num + 1} 插入完成: {len(batch_data)} 条记录")
                    
                except Exception as e:
                    logger.error(f"批次 {batch_num + 1} 插入失败: {str(e)}")
                    # 如果批量插入失败，回退到单条插入
                    logger.info("批量插入失败，回退到单条插入模式")
                    for ticker_data in batch_data:
                        try:
                            single_upsert_sql = """
                                INSERT INTO companies (
                                    code, name, exchange, type, currencycode, countryname, isin,
                                    data_source, ticker_active, last_ticker_update, created_at, updated_at
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (code) DO UPDATE SET
                                    name = EXCLUDED.name,
                                    exchange = EXCLUDED.exchange,
                                    type = EXCLUDED.type,
                                    currencycode = EXCLUDED.currencycode,
                                    countryname = EXCLUDED.countryname,
                                    isin = EXCLUDED.isin,
                                    data_source = EXCLUDED.data_source,
                                    ticker_active = EXCLUDED.ticker_active,
                                    last_ticker_update = EXCLUDED.last_ticker_update,
                                    updated_at = EXCLUDED.updated_at
                            """
                            self.db_manager.execute_query(single_upsert_sql, ticker_data)
                            success_count += 1
                        except Exception as single_e:
                            logger.warning(f"单条插入失败 {ticker_data[0]}: {str(single_e)}")
                            continue
            
            # 更新交易所配置中的统计信息
            update_sql = """
                UPDATE exchange_configs 
                SET last_update = %s, total_tickers = %s, updated_at = %s
                WHERE exchange_code = %s
            """
            self.db_manager.execute_query(update_sql, [datetime.now(), success_count, datetime.now(), exchange_code])
            
            if progress_callback:
                await progress_callback(f"{exchange_code} 数据保存完成")
            
            logger.info(f"批量插入完成！成功保存 {success_count}/{len(tickers_data)} 个ticker到数据库")
            return success_count
            
        except Exception as e:
            logger.error(f"保存ticker数据失败: {str(e)}")
            raise
    
    async def get_tickers_by_exchange(self, exchange_code: str = None, 
                                    ticker_type: str = None, search_text: str = None,
                                    limit: int = 1000) -> List[Dict[str, Any]]:
        """根据条件获取ticker列表"""
        try:
            # 构建查询
            query = """
                SELECT code, name, exchange, type, currencycode, countryname, 
                       isin, data_source, ticker_active, last_ticker_update,
                       sector, industry
                FROM companies 
                WHERE 1=1
            """
            params = []
            
            # 交易所筛选
            if exchange_code and exchange_code != "所有交易所":
                query += " AND exchange = %s"
                params.append(exchange_code)
            
            # 类型筛选
            if ticker_type and ticker_type != "所有类型":
                query += " AND type = %s"
                params.append(ticker_type)
            
            # 搜索文本
            if search_text:
                query += " AND (code ILIKE %s OR name ILIKE %s)"
                search_pattern = f"%{search_text}%"
                params.extend([search_pattern, search_pattern])
            
            # 排序和限制
            query += " ORDER BY code"
            if limit is not None and limit > 0:
                query += " LIMIT %s"
                params.append(limit)
            
            results = self.db_manager.fetch_all(query, params)
            
            tickers = []
            for row in results:
                tickers.append({
                    'code': row[0],
                    'name': row[1],
                    'exchange': row[2],
                    'type': row[3],
                    'currency': row[4],
                    'country': row[5],
                    'isin': row[6],
                    'data_source': row[7],
                    'ticker_active': bool(row[8]) if row[8] is not None else True,
                    'last_update': row[9].isoformat() if row[9] else None,
                    'sector': row[10],
                    'industry': row[11]
                })
            
            logger.debug(f"获取ticker列表完成，共 {len(tickers)} 个结果")
            return tickers
            
        except Exception as e:
            logger.error(f"获取ticker列表失败: {str(e)}")
            return []
    
    async def get_ticker_statistics(self, exchange_code: str = None) -> Dict[str, Any]:
        """获取ticker统计信息"""
        try:
            stats = {}
            
            # 总数统计
            if exchange_code:
                query = "SELECT COUNT(*) as total FROM companies WHERE exchange = %s"
                result = self.db_manager.fetch_one(query, [exchange_code])
            else:
                query = "SELECT COUNT(*) as total FROM companies"
                result = self.db_manager.fetch_one(query)
            
            stats['total'] = result[0] if result else 0
            
            # 按类型统计
            type_query = "SELECT type, COUNT(*) as count FROM companies"
            type_params = []
            
            if exchange_code:
                type_query += " WHERE exchange = %s"
                type_params.append(exchange_code)
            
            type_query += " GROUP BY type ORDER BY count DESC"
            
            results = self.db_manager.fetch_all(type_query, type_params if type_params else None)
            stats['by_type'] = {row[0] or 'Unknown': row[1] for row in results}
            
            # 按交易所统计（如果不是特定交易所查询）
            if not exchange_code:
                query = """
                    SELECT exchange, COUNT(*) as count 
                    FROM companies 
                    GROUP BY exchange 
                    ORDER BY count DESC
                """
                results = self.db_manager.fetch_all(query)
                stats['by_exchange'] = {row[0] or 'Unknown': row[1] for row in results}
            
            # 数据源统计
            source_query = "SELECT data_source, COUNT(*) as count FROM companies"
            source_params = []
            
            if exchange_code:
                source_query += " WHERE exchange = %s"
                source_params.append(exchange_code)
            
            source_query += " GROUP BY data_source ORDER BY count DESC"
            
            results = self.db_manager.fetch_all(source_query, source_params if source_params else None)
            stats['by_source'] = {row[0] or 'manual': row[1] for row in results}
            
            return stats
            
        except Exception as e:
            logger.error(f"获取统计信息失败: {str(e)}")
            return {'total': 0, 'by_type': {}, 'by_exchange': {}, 'by_source': {}}
    
    async def clear_exchange_data(self, exchange_code: str) -> Dict[str, Any]:
        """清除指定交易所的数据"""
        try:
            logger.warning(f"开始清除 {exchange_code} 交易所数据")
            
            # 删除该交易所的ticker数据
            delete_sql = """
                DELETE FROM companies 
                WHERE exchange = %s AND data_source = %s
            """
            deleted_count = self.db_manager.execute_query(delete_sql, [exchange_code, exchange_code.lower()])
            
            # 重置交易所配置统计
            update_sql = """
                UPDATE exchange_configs 
                SET last_update = NULL, total_tickers = 0, updated_at = %s
                WHERE exchange_code = %s
            """
            self.db_manager.execute_query(update_sql, [datetime.now(), exchange_code])
            
            result = {
                'success': True,
                'exchange_code': exchange_code,
                'deleted_count': deleted_count,
                'message': f'成功清除 {exchange_code} 的 {deleted_count} 个ticker'
            }
            
            logger.warning(f"清除 {exchange_code} 数据完成：{deleted_count} 个ticker")
            return result
            
        except Exception as e:
            error_msg = f"清除 {exchange_code} 数据失败: {str(e)}"
            logger.error(error_msg)
            return {'success': False, 'message': error_msg}
    
    async def get_available_types(self, exchange_code: str = None) -> List[str]:
        """获取可用的股票类型列表"""
        try:
            query = "SELECT DISTINCT type FROM companies WHERE type IS NOT NULL"
            params = []
            
            if exchange_code and exchange_code != "所有交易所":
                query += " AND exchange = %s"
                params.append(exchange_code)
            
            query += " ORDER BY type"
            
            results = self.db_manager.fetch_all(query, params if params else None)
            return [row[0] for row in results]
            
        except Exception as e:
            logger.error(f"获取类型列表失败: {str(e)}")
            return []
    
    async def get_available_exchanges(self) -> List[str]:
        """获取可用的交易所列表"""
        try:
            query = """
                SELECT DISTINCT exchange FROM companies 
                WHERE exchange IS NOT NULL 
                ORDER BY exchange
            """
            results = self.db_manager.fetch_all(query)
            return [row[0] for row in results]
            
        except Exception as e:
            logger.error(f"获取交易所列表失败: {str(e)}")
            return []


# 全局服务实例
_exchange_service = None

async def get_exchange_service() -> ExchangeDataService:
    """获取交易所数据服务单例"""
    global _exchange_service
    if _exchange_service is None:
        _exchange_service = ExchangeDataService()
    return _exchange_service
