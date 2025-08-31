# -*- coding: utf-8 -*-
"""
EODHD Fundamentals Data Service
基于EODHD API的公司基本面数据获取和管理服务
"""

import os
import json
import time
import requests
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from config.settings import config

logger = get_logger("eodhd_fundamentals_service")


class EODHDFundamentalsService:
    """EODHD基本面数据服务类"""
    
    def __init__(self):
        """初始化EODHD基本面数据服务"""
        logger.info("开始初始化EODHD基本面数据服务")
        
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
        
        # 支持的交易所 - 仅NYSE和NASDAQ
        self.supported_exchanges = ['NYSE', 'NASDAQ']
        
        # 支持的基本面数据类型
        self.supported_data_types = ['General', 'Holders', 'outstandingShares', 'Earnings']
        
        # 初始化数据库表
        self._init_database()
        
        # 创建HTTP会话
        self.session = self._create_robust_session()
        
        logger.info("EODHD基本面数据服务初始化完成")
    
    def _create_robust_session(self):
        """创建一个具有重试机制和超时设置的会话"""
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
            'User-Agent': 'ReverseCapital-Fundamentals/1.0'
        })
        
        return session
    
    def _init_database(self):
        """初始化数据库表结构"""
        try:
            # 创建基本面数据表
            create_table_sql = """
                CREATE TABLE IF NOT EXISTS company_fundamentals (
                    id SERIAL PRIMARY KEY,
                    ticker_code TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    data_type TEXT NOT NULL,
                    data_content JSONB,
                    fetch_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker_code, exchange, data_type)
                );
                
                CREATE INDEX IF NOT EXISTS idx_company_fundamentals_ticker ON company_fundamentals(ticker_code);
                CREATE INDEX IF NOT EXISTS idx_company_fundamentals_exchange ON company_fundamentals(exchange);
                CREATE INDEX IF NOT EXISTS idx_company_fundamentals_type ON company_fundamentals(data_type);
                CREATE INDEX IF NOT EXISTS idx_company_fundamentals_date ON company_fundamentals(fetch_date);
            """
            self.db_manager.execute_query(create_table_sql)
            logger.info("基本面数据表初始化完成")
            
        except Exception as e:
            logger.error(f"数据库表初始化失败: {str(e)}")
            raise
    
    async def fetch_and_store_fundamentals(self, exchanges: List[str], data_types: List[str] = None) -> Dict[str, Any]:
        """
        批量获取并存储基本面数据
        
        Args:
            exchanges: 交易所列表 (只支持NYSE和NASDAQ)
            data_types: 数据类型列表，如果未提供则使用所有支持的类型
        
        Returns:
            执行结果统计
        """
        logger.info(f"开始批量获取基本面数据 - 交易所: {exchanges}, 数据类型: {data_types}")
        
        # 验证交易所
        valid_exchanges = [ex for ex in exchanges if ex in self.supported_exchanges]
        if not valid_exchanges:
            logger.warning(f"没有有效的交易所，支持的交易所: {self.supported_exchanges}")
            return {
                'success': False,
                'message': f'不支持的交易所，仅支持: {", ".join(self.supported_exchanges)}',
                'data': None
            }
        
        # 使用所有支持的数据类型如果未提供
        if not data_types:
            data_types = self.supported_data_types.copy()
        
        # 验证数据类型
        valid_data_types = [dt for dt in data_types if dt in self.supported_data_types]
        if not valid_data_types:
            logger.warning(f"没有有效的数据类型，支持的类型: {self.supported_data_types}")
            return {
                'success': False,
                'message': f'不支持的数据类型，仅支持: {", ".join(self.supported_data_types)}',
                'data': None
            }
        
        total_success = 0
        total_failed = 0
        results = []
        
        try:
            # 获取指定交易所的活跃ticker列表
            for exchange in valid_exchanges:
                logger.info(f"处理交易所: {exchange}")
                
                tickers = await self._get_tickers_by_exchange(exchange)
                if not tickers:
                    logger.warning(f"交易所 {exchange} 没有找到活跃的ticker")
                    continue
                
                logger.info(f"交易所 {exchange} 找到 {len(tickers)} 个ticker")
                
                # 为每个ticker获取基本面数据
                exchange_success = 0
                exchange_failed = 0
                
                for ticker in tickers[:10]:  # 限制数量以避免API限制，正式环境可以调整
                    try:
                        ticker_results = await self._fetch_ticker_fundamentals(
                            ticker, exchange, valid_data_types
                        )
                        
                        if ticker_results['success']:
                            exchange_success += ticker_results['success_count']
                            exchange_failed += ticker_results['failed_count']
                        else:
                            exchange_failed += len(valid_data_types)
                        
                        # 添加短暂延迟以避免API限制
                        await asyncio.sleep(0.1)
                        
                    except Exception as e:
                        logger.error(f"获取ticker {ticker} 基本面数据失败: {str(e)}")
                        exchange_failed += len(valid_data_types)
                        continue
                
                results.append({
                    'exchange': exchange,
                    'success_count': exchange_success,
                    'failed_count': exchange_failed,
                    'total_tickers': len(tickers)
                })
                
                total_success += exchange_success
                total_failed += exchange_failed
            
            success_message = f"基本面数据获取完成 - 成功: {total_success}, 失败: {total_failed}"
            logger.info(success_message)
            
            return {
                'success': True,
                'message': success_message,
                'data': {
                    'total_success': total_success,
                    'total_failed': total_failed,
                    'exchanges_processed': len(valid_exchanges),
                    'data_types_processed': len(valid_data_types),
                    'exchange_results': results
                }
            }
            
        except Exception as e:
            error_msg = f"批量获取基本面数据失败: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'message': error_msg,
                'data': None
            }
    
    async def _get_tickers_by_exchange(self, exchange: str) -> List[str]:
        """获取指定交易所的活跃ticker列表"""
        try:
            # 对于EODHD API，NYSE和NASDAQ都使用US交易所代码，但数据库中仍保持原有的exchange标记
            query = """
                SELECT DISTINCT code 
                FROM companies 
                WHERE UPPER(exchange) = UPPER(%s) 
                AND ticker_active = true 
                ORDER BY code
                LIMIT 10
            """
            
            result = self.db_manager.fetch_all(query, (exchange,))
            tickers = [row[0] for row in result] if result else []
            
            logger.info(f"交易所 {exchange} 找到 {len(tickers)} 个活跃ticker (限制为10个用于测试)")
            return tickers
            
        except Exception as e:
            logger.error(f"获取交易所 {exchange} ticker列表失败: {str(e)}")
            return []
    
    async def _fetch_ticker_fundamentals(self, ticker: str, exchange: str, data_types: List[str]) -> Dict[str, Any]:
        """获取单个ticker的基本面数据"""
        logger.info(f"获取ticker {ticker} ({exchange}) 的基本面数据")
        
        success_count = 0
        failed_count = 0
        
        try:
            # 将NYSE和NASDAQ映射为EODHD API所需的US格式
            api_exchange = 'US' if exchange.upper() in ['NYSE', 'NASDAQ'] else exchange
            
            for data_type in data_types:
                try:
                    # 构建API URL - 使用正确的交易所代码
                    api_url = f"https://eodhd.com/api/fundamentals/{ticker}.{api_exchange}"
                    
                    params = {
                        'api_token': self.api_key,
                        'fmt': 'json'
                    }
                    
                    # 根据数据类型添加过滤器
                    if data_type != 'General':  # General获取全部数据，其他类型只获取特定部分
                        params['filter'] = data_type
                    
                    # 发起API请求
                    response = self.session.get(api_url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        data_content = response.json()
                        
                        # 如果是General类型，只保存General部分
                        if data_type == 'General' and isinstance(data_content, dict):
                            data_content = data_content.get('General', {})
                        
                        # 存储到数据库
                        await self._store_fundamentals_data(ticker, exchange, data_type, data_content)
                        success_count += 1
                        logger.info(f"成功获取并存储 {ticker} ({exchange}) 的 {data_type} 数据")
                        
                    else:
                        logger.warning(f"API请求失败 {ticker} ({exchange}->{api_exchange}) {data_type}: {response.status_code}")
                        failed_count += 1
                    
                    # 短暂延迟
                    await asyncio.sleep(0.05)
                    
                except Exception as e:
                    logger.error(f"处理 {ticker} ({exchange}) {data_type} 数据失败: {str(e)}")
                    failed_count += 1
                    continue
            
            return {
                'success': success_count > 0,
                'success_count': success_count,
                'failed_count': failed_count
            }
            
        except Exception as e:
            logger.error(f"获取ticker {ticker} 基本面数据失败: {str(e)}")
            return {
                'success': False,
                'success_count': 0,
                'failed_count': len(data_types)
            }
    
    async def _store_fundamentals_data(self, ticker: str, exchange: str, data_type: str, data_content: Dict[str, Any]):
        """存储基本面数据到数据库"""
        try:
            query = """
                INSERT INTO company_fundamentals (ticker_code, exchange, data_type, data_content, fetch_date, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker_code, exchange, data_type)
                DO UPDATE SET 
                    data_content = EXCLUDED.data_content,
                    fetch_date = EXCLUDED.fetch_date,
                    updated_at = EXCLUDED.updated_at
            """
            
            current_time = datetime.now()
            
            self.db_manager.execute_query(
                query,
                (ticker, exchange, data_type, json.dumps(data_content), current_time, current_time)
            )
            
            logger.debug(f"存储基本面数据成功: {ticker} ({exchange}) - {data_type}")
            
        except Exception as e:
            logger.error(f"存储基本面数据失败 {ticker} ({exchange}) - {data_type}: {str(e)}")
            raise


# 全局服务实例
_fundamentals_service = None


async def get_fundamentals_service() -> EODHDFundamentalsService:
    """获取基本面数据服务实例"""
    global _fundamentals_service
    if _fundamentals_service is None:
        _fundamentals_service = EODHDFundamentalsService()
    return _fundamentals_service
