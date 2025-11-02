# -*- coding: utf-8 -*-
"""
市场数据服务 - 定期获取并存储市场数据（指数、外汇、大宗商品、债券、板块）
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.utils.database_config import get_database_config

logger = get_logger("market_data_service")


# 市场数据映射配置
INDEX_MAP = {
    "S&P 500": ("SPY.US", "index", "us_equity"),
    "Nasdaq 100": ("QQQ.US", "index", "us_equity"),
    "Dow Jones": ("DIA.US", "index", "us_equity"),
    "STOXX 50": ("FEZ.US", "index", "eu_equity"),
    "DAX (Germany)": ("EWG.US", "index", "eu_equity"),
    "CAC 40 (France)": ("EWQ.US", "index", "eu_equity"),
    "Nikkei 225": ("EWJ.US", "index", "asia_equity"),
    "Hang Seng": ("2800.HK", "index", "asia_equity"),
}

FOREX_MAP = {
    "USD/CNH": ("USDCNH.FOREX", "forex", "major_pair"),
    "EUR/USD": ("EURUSD.FOREX", "forex", "major_pair"),
    "GBP/USD": ("GBPUSD.FOREX", "forex", "major_pair"),
    "USD/JPY": ("USDJPY.FOREX", "forex", "major_pair"),
}

COMMODITY_MAP = {
    "Gold": ("XAUUSD.FOREX", "commodity", "precious_metal"),
    "Silver": ("XAGUSD.FOREX", "commodity", "precious_metal"),
    "Copper": ("HG.US", "commodity", "industrial_metal"),
    "Brent Oil (BNO ETF)": ("BNO.US", "commodity", "energy"),
    "Natural Gas": ("NG.US", "commodity", "energy"),
}

BOND_YIELD_MAP = {
    "3-Month Treasury": ("US3M.INDX", "bond", "us_treasury"),
    "1-Year Treasury": ("US1Y.INDX", "bond", "us_treasury"),
    "5-Year Treasury": ("US5Y.INDX", "bond", "us_treasury"),
    "10-Year Treasury": ("US10Y.INDX", "bond", "us_treasury"),
}

SECTOR_MAP = {
    "Technology": ("XLK.US", "sector", "us_sector"),
    "Financials": ("XLF.US", "sector", "us_sector"),
    "Health Care": ("XLV.US", "sector", "us_sector"),
    "Consumer Discretionary": ("XLY.US", "sector", "us_sector"),
    "Industrials": ("XLI.US", "sector", "us_sector"),
    "Consumer Staples": ("XLP.US", "sector", "us_sector"),
    "Energy": ("XLE.US", "sector", "us_sector"),
    "Utilities": ("XLU.US", "sector", "us_sector"),
    "Real Estate": ("XLRE.US", "sector", "us_sector"),
    "Materials": ("XLB.US", "sector", "us_sector"),
    "Communication Services": ("XLC.US", "sector", "us_sector"),
}


class MarketDataService:
    """市场数据服务类"""
    
    def __init__(self):
        """初始化市场数据服务"""
        logger.info("开始初始化市场数据服务")
        
        try:
            # 获取API密钥
            self.api_token = os.getenv('EODHD_API_TOKEN')
            if not self.api_token:
                logger.error("EODHD_API_TOKEN 未在环境变量中设置")
                raise ValueError("EODHD_API_TOKEN 未在环境变量中设置")
            
            logger.info("API密钥获取成功", token_length=len(self.api_token))
            
            # API配置
            self.realtime_base_url = "https://eodhd.com/api/real-time"
            self.eod_base_url = "https://eodhd.com/api/eod"
            
            # 获取数据库连接配置
            db_config = get_database_config()
            self.db_manager = DatabaseManager(db_config)
            logger.info("数据库管理器配置完成")
            
            # 创建HTTP会话
            self.session = self._create_robust_session()
            logger.info("HTTP会话创建完成")
            
            logger.info("市场数据服务初始化完成")
            
        except Exception as e:
            logger.error(f"初始化市场数据服务失败: {str(e)}")
            raise
    
    def _create_robust_session(self) -> requests.Session:
        """创建具有重试机制的HTTP会话"""
        session = requests.Session()
        
        # 配置重试策略
        retry_strategy = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # 设置请求头
        session.headers.update({
            'User-Agent': 'RC-MarketData/1.0',
            'Accept': 'application/json'
        })
        
        return session
    
    def safe_float(self, value):
        """安全转换为float，处理NA值"""
        if value is None or value == '' or str(value).upper() == 'NA':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    def safe_int(self, value):
        """安全转换为int，处理NA值"""
        if value is None or value == '' or str(value).upper() == 'NA':
            return None
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    def fetch_realtime_data(self, ticker: str) -> Optional[Dict[str, Any]]:
        """获取实时数据"""
        try:
            url = f"{self.realtime_base_url}/{ticker}"
            params = {
                'api_token': self.api_token,
                'fmt': 'json'
            }
            
            logger.debug(f"请求实时数据: {ticker}")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.debug(f"成功获取 {ticker} 的实时数据")
            return data
                
        except Exception as e:
            logger.error(f"获取 {ticker} 实时数据失败: {str(e)}")
            return None
    
    def fetch_eod_data(self, ticker: str, days: int = 1) -> Optional[List[Dict[str, Any]]]:
        """获取EOD历史数据"""
        try:
            from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            url = f"{self.eod_base_url}/{ticker}"
            params = {
                'api_token': self.api_token,
                'fmt': 'json',
                'from': from_date
            }
            
            logger.debug(f"请求EOD数据: {ticker} (最近{days}天)")
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            logger.debug(f"成功获取 {ticker} 的{len(data)}条EOD数据")
            return data
                
        except Exception as e:
            logger.error(f"获取 {ticker} EOD数据失败: {str(e)}")
            return None
    
    def parse_realtime_data(self, code: str, name: str, asset_type: str, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """解析实时数据"""
        try:
            if not data or 'error' in data:
                return None
            
            # 解析时间戳
            timestamp = data.get('timestamp')
            if timestamp and timestamp != "NA":
                try:
                    data_datetime = datetime.fromtimestamp(int(timestamp), tz=timezone.utc)
                except (ValueError, TypeError):
                    data_datetime = datetime.now(timezone.utc)
            else:
                data_datetime = datetime.now(timezone.utc)
            
            parsed_data = {
                'code': code,
                'asset_type': asset_type,
                'asset_name': name,
                'open': self.safe_float(data.get('open')),
                'high': self.safe_float(data.get('high')),
                'low': self.safe_float(data.get('low')),
                'close': self.safe_float(data.get('close')),
                'volume': self.safe_int(data.get('volume')),
                'previous_close': self.safe_float(data.get('previousClose')),
                'change_amount': self.safe_float(data.get('change')),
                'change_percent': self.safe_float(data.get('change_p')),
                'data_timestamp': self.safe_int(timestamp) if timestamp != "NA" else None,
                'data_datetime': data_datetime,
                'gmtoffset': self.safe_int(data.get('gmtoffset', 0)) or 0,
            }
            
            # 验证必要字段
            price_fields = [parsed_data['close'], parsed_data['previous_close']]
            if all(field is None for field in price_fields):
                logger.warning(f"资产{code}所有价格字段都无效，跳过")
                return None
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"解析{code}实时数据失败: {str(e)}")
            return None
    
    def parse_eod_data(self, code: str, asset_type: str, eod_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """解析EOD数据"""
        parsed_records = []
        
        try:
            for record in eod_data:
                parsed = {
                    'code': code,
                    'date': record.get('date'),
                    'asset_type': asset_type,
                    'open': self.safe_float(record.get('open')),
                    'high': self.safe_float(record.get('high')),
                    'low': self.safe_float(record.get('low')),
                    'close': self.safe_float(record.get('close')),
                    'adjusted_close': self.safe_float(record.get('adjusted_close')),
                    'volume': self.safe_int(record.get('volume')),
                }
                
                if parsed['date'] and parsed['close'] is not None:
                    parsed_records.append(parsed)
            
            logger.debug(f"解析{code}的{len(parsed_records)}条EOD数据")
            return parsed_records
            
        except Exception as e:
            logger.error(f"解析{code}EOD数据失败: {str(e)}")
            return []
    
    def store_realtime_data(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """存储实时数据到数据库"""
        if not records:
            return {'success_count': 0, 'failed_count': 0}
        
        success_count = 0
        failed_count = 0
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("无法获取数据库连接")
                return {'success_count': 0, 'failed_count': len(records)}
            
            cursor = conn.cursor()
            
            # UPSERT SQL
            upsert_sql = """
            INSERT INTO market_data_realtime 
            (code, asset_type, asset_name, open, high, low, close, volume, 
             previous_close, change_amount, change_percent, data_timestamp, 
             data_datetime, gmtoffset, created_at, updated_at)
            VALUES (%(code)s, %(asset_type)s, %(asset_name)s, %(open)s, %(high)s, %(low)s, %(close)s, 
                    %(volume)s, %(previous_close)s, %(change_amount)s, %(change_percent)s, 
                    %(data_timestamp)s, %(data_datetime)s, %(gmtoffset)s, 
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (code) 
            DO UPDATE SET
                asset_type = EXCLUDED.asset_type,
                asset_name = EXCLUDED.asset_name,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume,
                previous_close = EXCLUDED.previous_close,
                change_amount = EXCLUDED.change_amount,
                change_percent = EXCLUDED.change_percent,
                data_timestamp = EXCLUDED.data_timestamp,
                data_datetime = EXCLUDED.data_datetime,
                gmtoffset = EXCLUDED.gmtoffset,
                updated_at = CURRENT_TIMESTAMP
            """
            
            for record in records:
                try:
                    cursor.execute(upsert_sql, record)
                    success_count += 1
                except Exception as e:
                    logger.warning(f"保存{record.get('code', 'unknown')}实时数据失败: {str(e)}")
                    failed_count += 1
            
            conn.commit()
            logger.info(f"实时数据保存完成：成功{success_count}，失败{failed_count}")
            
        except Exception as e:
            logger.error(f"批量保存实时数据失败: {str(e)}")
            if conn:
                conn.rollback()
            failed_count = len(records)
            
        finally:
            if conn:
                conn.close()
        
        return {'success_count': success_count, 'failed_count': failed_count}
    
    def store_eod_data(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """存储EOD数据到数据库"""
        if not records:
            return {'success_count': 0, 'failed_count': 0}
        
        success_count = 0
        failed_count = 0
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("无法获取数据库连接")
                return {'success_count': 0, 'failed_count': len(records)}
            
            cursor = conn.cursor()
            
            # UPSERT SQL
            upsert_sql = """
            INSERT INTO market_data_eod 
            (code, date, asset_type, open, high, low, close, adjusted_close, volume,
             created_at, updated_at)
            VALUES (%(code)s, %(date)s, %(asset_type)s, %(open)s, %(high)s, %(low)s, %(close)s,
                    %(adjusted_close)s, %(volume)s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (code, date) 
            DO UPDATE SET
                asset_type = EXCLUDED.asset_type,
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                adjusted_close = EXCLUDED.adjusted_close,
                volume = EXCLUDED.volume,
                updated_at = CURRENT_TIMESTAMP
            """
            
            for record in records:
                try:
                    cursor.execute(upsert_sql, record)
                    success_count += 1
                except Exception as e:
                    logger.warning(f"保存{record.get('code', 'unknown')} {record.get('date')}的EOD数据失败: {str(e)}")
                    failed_count += 1
            
            conn.commit()
            logger.info(f"EOD数据保存完成：成功{success_count}，失败{failed_count}")
            
        except Exception as e:
            logger.error(f"批量保存EOD数据失败: {str(e)}")
            if conn:
                conn.rollback()
            failed_count = len(records)
            
        finally:
            if conn:
                conn.close()
        
        return {'success_count': success_count, 'failed_count': failed_count}
    
    def fetch_and_store_market_data(self, 
                                    asset_types: Optional[List[str]] = None,
                                    fetch_realtime: bool = True,
                                    fetch_eod: bool = True,
                                    eod_days: int = 5) -> Dict[str, Any]:
        """
        主要业务逻辑：获取并存储市场数据
        
        Args:
            asset_types: 要获取的资产类型列表 ['index', 'forex', 'commodity', 'bond', 'sector']
            fetch_realtime: 是否获取实时数据
            fetch_eod: 是否获取EOD历史数据
            eod_days: 获取最近几天的EOD数据
        
        Returns:
            Dict包含执行结果
        """
        logger.info("开始获取并存储市场数据", 
                   asset_types=asset_types, 
                   fetch_realtime=fetch_realtime, 
                   fetch_eod=fetch_eod)
        
        start_time = datetime.now()
        
        try:
            # 如果未指定资产类型，则获取所有类型
            if not asset_types:
                asset_types = ['index', 'forex', 'commodity', 'bond', 'sector']
            
            # 合并所有资产
            all_assets = {}
            
            if 'index' in asset_types:
                all_assets.update({name: (code, 'index', cat) for name, (code, _, cat) in INDEX_MAP.items()})
            if 'forex' in asset_types:
                all_assets.update({name: (code, 'forex', cat) for name, (code, _, cat) in FOREX_MAP.items()})
            if 'commodity' in asset_types:
                all_assets.update({name: (code, 'commodity', cat) for name, (code, _, cat) in COMMODITY_MAP.items()})
            if 'bond' in asset_types:
                all_assets.update({name: (code, 'bond', cat) for name, (code, _, cat) in BOND_YIELD_MAP.items()})
            if 'sector' in asset_types:
                all_assets.update({name: (code, 'sector', cat) for name, (code, _, cat) in SECTOR_MAP.items()})
            
            logger.info(f"准备获取{len(all_assets)}个资产的数据")
            
            realtime_stats = {'success': 0, 'failed': 0}
            eod_stats = {'success': 0, 'failed': 0}
            
            # 获取实时数据
            if fetch_realtime:
                logger.info("开始获取实时数据")
                realtime_records = []
                
                for name, (code, asset_type, category) in all_assets.items():
                    data = self.fetch_realtime_data(code)
                    if data:
                        parsed = self.parse_realtime_data(code, name, asset_type, data)
                        if parsed:
                            realtime_records.append(parsed)
                    time.sleep(0.1)  # 避免API限流
                
                # 存储实时数据
                if realtime_records:
                    result = self.store_realtime_data(realtime_records)
                    realtime_stats['success'] = result['success_count']
                    realtime_stats['failed'] = result['failed_count']
            
            # 获取EOD数据
            if fetch_eod:
                logger.info("开始获取EOD数据")
                all_eod_records = []
                
                for name, (code, asset_type, category) in all_assets.items():
                    eod_data = self.fetch_eod_data(code, days=eod_days)
                    if eod_data:
                        parsed_records = self.parse_eod_data(code, asset_type, eod_data)
                        all_eod_records.extend(parsed_records)
                    time.sleep(0.1)
                
                # 存储EOD数据
                if all_eod_records:
                    result = self.store_eod_data(all_eod_records)
                    eod_stats['success'] = result['success_count']
                    eod_stats['failed'] = result['failed_count']
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"市场数据获取完成：实时数据成功{realtime_stats['success']}/失败{realtime_stats['failed']}，"
                       f"EOD数据成功{eod_stats['success']}/失败{eod_stats['failed']}，耗时{execution_time:.2f}秒")
            
            return {
                'success': True,
                'message': f"成功获取市场数据：实时{realtime_stats['success']}条，历史{eod_stats['success']}条",
                'data': {
                    'total_assets': len(all_assets),
                    'realtime': realtime_stats,
                    'eod': eod_stats,
                    'execution_time': execution_time
                }
            }
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            error_msg = f"获取市场数据失败: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'message': error_msg,
                'data': {
                    'execution_time': execution_time
                }
            }


def create_market_data_service() -> MarketDataService:
    """创建市场数据服务实例"""
    return MarketDataService()
