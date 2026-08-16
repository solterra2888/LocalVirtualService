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
from concurrent.futures import ThreadPoolExecutor, as_completed
import psycopg2.extras

from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.utils.database_config import get_database_config

logger = get_logger("market_data_service")


# 市场数据映射配置
INDEX_MAP = {
    "S&P 500": ("SPY.US", "index", "us_equity"),
    "S&P 500 Index": ("GSPC.INDX", "index", "us_equity"),
    "Nasdaq 100": ("QQQ.US", "index", "us_equity"),
    "Nasdaq 100 Index": ("NDX.INDX", "index", "us_equity"),
    "Nasdaq Composite Index": ("IXIC.INDX", "index", "us_equity"),
    "PHLX Semiconductor Index": ("SOX.INDX", "index", "us_equity"),
    "Dow Jones": ("DIA.US", "index", "us_equity"),
    "STOXX 50": ("FEZ.US", "index", "eu_equity"),
    "DAX (Germany)": ("EWG.US", "index", "eu_equity"),
    "DAX Index": ("GDAXI.INDX", "index", "eu_equity"),
    "CAC 40 (France)": ("EWQ.US", "index", "eu_equity"),
    "Nikkei 225": ("EWJ.US", "index", "asia_equity"),
    "Nikkei 225 Index": ("N225.INDX", "index", "asia_equity"),
    "KOSPI": ("KS11.INDX", "index", "asia_equity"),
    "Hang Seng": ("2800.HK", "index", "asia_equity"),
    "SSE 50": ("SSE50.INDX", "index", "china_equity"),
    "CSI 300": ("CSI300.INDX", "index", "china_equity"),
    "CSI 500": ("CSI500.INDX", "index", "china_equity"),
    "CSI 1000": ("CSI1000.INDX", "index", "china_equity"),
    "VIX": ("VIX.INDX", "index", "volatility"),
}

FOREX_MAP = {
    "USD/CNH": ("USDCNH.FOREX", "forex", "major_pair"),
    "EUR/USD": ("EURUSD.FOREX", "forex", "major_pair"),
    "GBP/USD": ("GBPUSD.FOREX", "forex", "major_pair"),
    "USD/JPY": ("USDJPY.FOREX", "forex", "major_pair"),
}

COMMODITY_MAP = {
    "Gold (GLD ETF)": ("GLD.US", "commodity", "precious_metal"),
    "Silver (SLV ETF)": ("SLV.US", "commodity", "precious_metal"),
    "Copper (CPER ETF)": ("CPER.US", "commodity", "industrial_metal"),
    "WTI Crude Oil (USO ETF)": ("USO.US", "commodity", "energy"),
    "Brent Oil (BNO ETF)": ("BNO.US", "commodity", "energy"),
    "Natural Gas (UNG ETF)": ("UNG.US", "commodity", "energy"),
}

BOND_YIELD_MAP = {
    "3-Month Treasury": ("US3M.INDX", "bond", "us_treasury"),
    "1-Year Treasury": ("US1Y.INDX", "bond", "us_treasury"),
    "2-Year Treasury": ("US2Y.INDX", "bond", "us_treasury"),
    "5-Year Treasury": ("US5Y.INDX", "bond", "us_treasury"),
    "10-Year Treasury": ("US10Y.INDX", "bond", "us_treasury"),
    "30-Year Treasury": ("US30Y.INDX", "bond", "us_treasury"),
}

# DMR Market Movers 图表依赖的 ETF（SHY/IEF/TLT/... 与 ARKK/SMH/SOXX/... 等）。
# 历史上这些只做过一次性手动回填（2026-06-11），未注册前 market_data 定时任务不覆盖，
# 导致 DMR 行点击展开的 K 线图停在回填日。注册到独立的 'etf' 类型：
# - 不影响 market_weekly_report（其 SQL 显式过滤 index/sector/forex/commodity/bond）
# - asset_types=None 时随默认列表一起采集
ETF_MAP = {
    "2Y Treasury ETF": ("SHY.US", "etf", "us_bond_etf"),
    "7-10Y Treasury ETF": ("IEF.US", "etf", "us_bond_etf"),
    "20Y Treasury ETF": ("TLT.US", "etf", "us_bond_etf"),
    "US Dollar ETF": ("UUP.US", "etf", "us_macro_etf"),
    "Russell 2000 ETF": ("IWM.US", "etf", "us_equity_etf"),
    "S&P 500 Equal Weight ETF": ("RSP.US", "etf", "us_equity_etf"),
    "High Yield Credit ETF": ("HYG.US", "etf", "us_credit_etf"),
    "IG Credit ETF": ("LQD.US", "etf", "us_credit_etf"),
    "ARK Innovation ETF": ("ARKK.US", "etf", "thematic_etf"),
    "ARK Autonomous ETF": ("ARKQ.US", "etf", "thematic_etf"),
    "AI & Robotics ETF": ("BOTZ.US", "etf", "thematic_etf"),
    "Cybersecurity ETF (BUG)": ("BUG.US", "etf", "thematic_etf"),
    "Cybersecurity ETF (CIBR)": ("CIBR.US", "etf", "thematic_etf"),
    "Cloud Computing ETF": ("CLOU.US", "etf", "thematic_etf"),
    "Autonomous Driving ETF": ("DRIV.US", "etf", "thematic_etf"),
    "FinTech ETF": ("FINX.US", "etf", "thematic_etf"),
    "Cybersecurity ETF (HACK)": ("HACK.US", "etf", "thematic_etf"),
    "Clean Energy ETF": ("ICLN.US", "etf", "thematic_etf"),
    "Lithium & Battery ETF": ("LIT.US", "etf", "thematic_etf"),
    "E-Commerce ETF": ("ONLN.US", "etf", "thematic_etf"),
    "Water Resources ETF": ("PHO.US", "etf", "thematic_etf"),
    "Robotics ETF": ("ROBO.US", "etf", "thematic_etf"),
    "Cloud Computing ETF (SKYY)": ("SKYY.US", "etf", "thematic_etf"),
    "Semiconductor ETF (SMH)": ("SMH.US", "etf", "thematic_etf"),
    "Semiconductor ETF (SOXX)": ("SOXX.US", "etf", "thematic_etf"),
    "Semiconductor ETF (SOXQ)": ("SOXQ.US", "etf", "thematic_etf"),
    "Solar ETF": ("TAN.US", "etf", "thematic_etf"),
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
            
            # 🚀 并发配置
            self.max_workers = int(os.getenv("MARKET_DATA_MAX_WORKERS", "10"))
            
            # 创建HTTP会话
            self.session = self._create_robust_session()
            logger.info(f"HTTP会话创建完成 (max_workers={self.max_workers})")
            
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
        
        # 🔧 增加连接池大小以支持并发请求
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=10,
            pool_maxsize=max(20, self.max_workers + 10)
        )
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
            # 用 utcnow 而非 datetime.now: 服务器在 HK (UTC+8), 跨日时段 datetime.now()
            # 取本地日期会比 UTC 早 1 天, 导致 EODHD 返回不全/空数据.
            from backend.utils.datetime_utils import utcnow
            from_date = (utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
            
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
    
    # ==== 🚀 并发获取方法 ====
    
    def concurrent_fetch_realtime_data(self, assets: List[tuple]) -> Dict[str, Dict[str, Any]]:
        """
        并发获取多个资产的实时数据
        
        Args:
            assets: 资产列表，每个元素为 (name, code, asset_type, category)
            
        Returns:
            Dict[name, data_dict]: {资产名称: 实时数据字典}
        """
        results = {}
        
        def fetch_single_realtime(asset_info):
            """获取单个资产的实时数据"""
            name, code, asset_type, category = asset_info
            try:
                data = self.fetch_realtime_data(code)
                if data:
                    return (name, {"code": code, "asset_type": asset_type, "category": category, "data": data})
            except Exception as e:
                logger.error(f"并发获取{name}实时数据失败: {str(e)}")
            return (name, None)
        
        # 使用线程池并发获取
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(fetch_single_realtime, asset): asset for asset in assets}
            
            for future in as_completed(futures):
                try:
                    name, data = future.result()
                    if data:
                        results[name] = data
                except Exception as e:
                    asset = futures[future]
                    logger.error(f"并发获取{asset[0]}实时数据异常: {str(e)}")
        
        return results
    
    def concurrent_fetch_eod_data(self, assets: List[tuple], days: int = 1) -> Dict[str, List[Dict[str, Any]]]:
        """
        并发获取多个资产的EOD数据
        
        Args:
            assets: 资产列表，每个元素为 (name, code, asset_type, category)
            days: 获取最近N天的数据
            
        Returns:
            Dict[name, eod_data_list]: {资产名称: EOD数据列表}
        """
        results = {}
        
        def fetch_single_eod(asset_info):
            """获取单个资产的EOD数据"""
            name, code, asset_type, category = asset_info
            try:
                data = self.fetch_eod_data(code, days)
                if data:
                    return (name, {"code": code, "asset_type": asset_type, "category": category, "data": data})
            except Exception as e:
                logger.error(f"并发获取{name} EOD数据失败: {str(e)}")
            return (name, None)
        
        # 使用线程池并发获取
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(fetch_single_eod, asset): asset for asset in assets}
            
            for future in as_completed(futures):
                try:
                    name, data = future.result()
                    if data:
                        results[name] = data
                except Exception as e:
                    asset = futures[future]
                    logger.error(f"并发获取{asset[0]} EOD数据异常: {str(e)}")
        
        return results
    
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
        """🚀 批量存储实时数据到数据库 (使用execute_values优化)"""
        if not records:
            return {'success_count': 0, 'failed_count': 0}
        
        success_count = 0
        failed_count = 0
        conn = None
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("无法获取数据库连接")
                return {'success_count': 0, 'failed_count': len(records)}
            
            cursor = conn.cursor()
            
            # 准备批量插入的数据
            insert_data = []
            for record in records:
                insert_data.append((
                    record.get('code'),
                    record.get('asset_type'),
                    record.get('asset_name'),
                    record.get('open'),
                    record.get('high'),
                    record.get('low'),
                    record.get('close'),
                    record.get('volume'),
                    record.get('previous_close'),
                    record.get('change_amount'),
                    record.get('change_percent'),
                    record.get('data_timestamp'),
                    record.get('data_datetime'),
                    record.get('gmtoffset')
                ))
            
            # 🚀 使用execute_values批量UPSERT
            upsert_sql = """
                INSERT INTO market_data_realtime 
                (code, asset_type, asset_name, open, high, low, close, volume, 
                 previous_close, change_amount, change_percent, data_timestamp, 
                 data_datetime, gmtoffset, created_at, updated_at)
                VALUES %s
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
            
            psycopg2.extras.execute_values(
                cursor,
                upsert_sql,
                insert_data,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                page_size=100
            )
            
            conn.commit()
            success_count = len(records)
            logger.info(f"💾 实时数据批量保存完成：{success_count}条记录")
            
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
        """🚀 批量存储EOD数据到数据库 (使用execute_values优化)"""
        if not records:
            return {'success_count': 0, 'failed_count': 0}
        
        success_count = 0
        failed_count = 0
        conn = None
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("无法获取数据库连接")
                return {'success_count': 0, 'failed_count': len(records)}
            
            cursor = conn.cursor()
            
            # 准备批量插入的数据
            insert_data = []
            for record in records:
                insert_data.append((
                    record.get('code'),
                    record.get('date'),
                    record.get('asset_type'),
                    record.get('open'),
                    record.get('high'),
                    record.get('low'),
                    record.get('close'),
                    record.get('adjusted_close'),
                    record.get('volume')
                ))
            
            # 🚀 使用execute_values批量UPSERT
            upsert_sql = """
                INSERT INTO market_data_eod 
                (code, date, asset_type, open, high, low, close, adjusted_close, volume,
                 created_at, updated_at)
                VALUES %s
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
            
            psycopg2.extras.execute_values(
                cursor,
                upsert_sql,
                insert_data,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
                page_size=100
            )
            
            conn.commit()
            success_count = len(records)
            logger.info(f"💾 EOD数据批量保存完成：{success_count}条记录")
            
        except Exception as e:
            logger.error(f"批量保存EOD数据失败: {str(e)}")
            if conn:
                conn.rollback()
            failed_count = len(records)
            
        finally:
            if conn:
                conn.close()
        
        return {'success_count': success_count, 'failed_count': failed_count}
    
    # ==== 经济事件数据 ====
    
    def _ensure_economic_events_table(self, cursor) -> None:
        """确保economic_events表存在"""
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS economic_events (
            id SERIAL PRIMARY KEY,
            date VARCHAR(20) NOT NULL,
            time VARCHAR(20) NOT NULL DEFAULT '',
            country VARCHAR(10) NOT NULL DEFAULT 'US',
            event TEXT NOT NULL,
            period VARCHAR(50),
            actual VARCHAR(100),
            previous VARCHAR(100),
            forecast VARCHAR(100),
            change_val NUMERIC,
            change_percentage NUMERIC,
            impact SMALLINT,
            comparison VARCHAR(100),
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # 确保唯一约束存在
        cursor.execute("""
        DO $$ 
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint 
                WHERE conname = 'economic_events_unique_constraint'
            ) THEN
                ALTER TABLE economic_events 
                ADD CONSTRAINT economic_events_unique_constraint 
                UNIQUE (date, time, country, event);
            END IF;
        END $$;
        """)
        
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_economic_events_date
        ON economic_events (date)
        """)
        cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_economic_events_country
        ON economic_events (country)
        """)
    
    def _parse_event_impact(self, impact_value: Any) -> Optional[int]:
        """解析impact字段为整数"""
        if impact_value is None:
            return None
        
        if isinstance(impact_value, (int, float)):
            impact_int = int(impact_value)
            if impact_int < 1:
                return None
            return min(impact_int, 3)
        
        impact_str = str(impact_value).strip()
        if not impact_str:
            return None
        
        if set(impact_str) == {'*'}:
            return min(len(impact_str), 3)
        
        lower = impact_str.lower()
        mapping = {
            '3': 3,
            'high': 3,
            'very high': 3,
            '重要': 3,
            '2': 2,
            'medium': 2,
            'moderate': 2,
            'medium-high': 2,
            '1': 1,
            'low': 1,
        }
        if lower in mapping:
            return mapping[lower]
        
        if 'high' in lower:
            return 3
        if 'medium' in lower or 'moderate' in lower:
            return 2
        if 'low' in lower:
            return 1
        
        return None
    
    def _normalize_economic_event(self, event: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """规范化经济事件数据，确保字段完整"""
        if not event:
            return None
        
        date_raw = event.get('date') or event.get('event_date')
        if not date_raw:
            return None
        date_str = str(date_raw).strip()
        if not date_str:
            return None
        
        time_str = str(event.get('time') or '').strip()
        country = str(event.get('country') or 'US').upper()
        event_name = str(event.get('event') or event.get('name') or '').strip()
        if not event_name:
            return None
        
        period = str(event.get('period') or '').strip() or None
        
        def normalize_text_field(value: Any) -> Optional[str]:
            if value is None:
                return None
            value_str = str(value).strip()
            return value_str if value_str else None
        
        actual = normalize_text_field(event.get('actual'))
        previous = normalize_text_field(event.get('previous'))
        forecast = normalize_text_field(event.get('forecast') or event.get('estimate'))
        
        change_val_source = (
            event.get('change_val') or
            event.get('change') or
            event.get('calculated_change')
        )
        change_val = self.safe_float(change_val_source)
        
        change_pct_source = (
            event.get('change_percentage') or
            event.get('calculated_change_pct') or
            event.get('change_pct')
        )
        change_pct = self.safe_float(change_pct_source)
        
        impact = self._parse_event_impact(event.get('impact'))
        comparison = normalize_text_field(event.get('comparison'))
        
        return {
            'date': date_str,
            'time': time_str or '',
            'country': country or 'US',
            'event': event_name,
            'period': period,
            'actual': actual,
            'previous': previous,
            'forecast': forecast,
            'change_val': change_val,
            'change_percentage': change_pct,
            'impact': impact,
            'comparison': comparison
        }
    
    def insert_economic_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        插入或更新单条经济事件数据
        
        Returns:
            {
                'id': Optional[int],
                'inserted': bool
            }
        """
        result = {'id': None, 'inserted': False}
        normalized = self._normalize_economic_event(event_data)
        if not normalized:
            logger.debug("跳过无效的经济事件记录: %s", event_data)
            return result
        
        conn = None
        cursor = None
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("无法获取数据库连接")
                return result
            
            cursor = conn.cursor()
            self._ensure_economic_events_table(cursor)
            
            insert_sql = """
            INSERT INTO economic_events
            (date, time, country, event, period, actual, previous, forecast,
             change_val, change_percentage, impact, comparison)
            VALUES
            (%(date)s, %(time)s, %(country)s, %(event)s, %(period)s, %(actual)s,
             %(previous)s, %(forecast)s, %(change_val)s, %(change_percentage)s,
             %(impact)s, %(comparison)s)
            ON CONFLICT ON CONSTRAINT economic_events_unique_constraint
            DO UPDATE SET
                period = EXCLUDED.period,
                actual = EXCLUDED.actual,
                previous = EXCLUDED.previous,
                forecast = EXCLUDED.forecast,
                change_val = EXCLUDED.change_val,
                change_percentage = EXCLUDED.change_percentage,
                impact = EXCLUDED.impact,
                comparison = EXCLUDED.comparison,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id, (xmax = 0) AS inserted_flag;
            """
            
            cursor.execute(insert_sql, normalized)
            fetch_result = cursor.fetchone()
            conn.commit()
            
            if fetch_result:
                result['id'] = fetch_result[0]
                result['inserted'] = bool(fetch_result[1])
            
            return result
        
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"插入经济事件失败: {str(e)}")
            return result
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def batch_insert_economic_events(self, events: List[Dict[str, Any]]) -> Dict[str, int]:
        """
        批量插入经济事件数据
        
        Returns:
            {
                'processed': int,
                'inserted': int,
                'updated': int
            }
        """
        if not events:
            return {'processed': 0, 'inserted': 0, 'updated': 0}
        
        normalized_records = []
        for event in events:
            normalized = self._normalize_economic_event(event)
            if normalized:
                normalized_records.append(normalized)
        
        if not normalized_records:
            logger.warning("没有可插入的经济事件数据")
            return {'processed': 0, 'inserted': 0, 'updated': 0, 'total_before_dedup': 0, 'duplicates_removed': 0}
        
        # 记录去重前的数量
        total_before_dedup = len(normalized_records)
        
        # 去重：基于 (date, time, country, event) 的唯一性
        # 保留最后一个重复记录（通常是最新的数据）
        unique_records = {}
        for record in normalized_records:
            key = (record['date'], record['time'], record['country'], record['event'])
            unique_records[key] = record
        
        normalized_records = list(unique_records.values())
        duplicates_removed = total_before_dedup - len(normalized_records)
        logger.info(f"去重: 原始{total_before_dedup}条 → 去重后{len(normalized_records)}条 (移除{duplicates_removed}条重复)")
        
        if not normalized_records:
            logger.warning("去重后没有可插入的经济事件数据")
            return {'processed': 0, 'inserted': 0, 'updated': 0, 'total_before_dedup': total_before_dedup, 'duplicates_removed': duplicates_removed}
        
        conn = None
        cursor = None
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("无法获取数据库连接")
                return {'processed': 0, 'inserted': 0, 'updated': 0}
            
            cursor = conn.cursor()
            self._ensure_economic_events_table(cursor)
            
            insert_sql = """
            INSERT INTO economic_events
            (date, time, country, event, period, actual, previous, forecast,
             change_val, change_percentage, impact, comparison)
            VALUES %s
            ON CONFLICT ON CONSTRAINT economic_events_unique_constraint
            DO UPDATE SET
                period = EXCLUDED.period,
                actual = EXCLUDED.actual,
                previous = EXCLUDED.previous,
                forecast = EXCLUDED.forecast,
                change_val = EXCLUDED.change_val,
                change_percentage = EXCLUDED.change_percentage,
                impact = EXCLUDED.impact,
                comparison = EXCLUDED.comparison,
                updated_at = CURRENT_TIMESTAMP
            RETURNING (xmax = 0) AS inserted_flag;
            """
            
            values = [
                (
                    record['date'],
                    record['time'],
                    record['country'],
                    record['event'],
                    record['period'],
                    record['actual'],
                    record['previous'],
                    record['forecast'],
                    record['change_val'],
                    record['change_percentage'],
                    record['impact'],
                    record['comparison']
                )
                for record in normalized_records
            ]
            
            # 使用 execute_values 进行批量插入
            # 注意：execute_values 的 fetch 参数对 RETURNING 支持有限
            psycopg2.extras.execute_values(
                cursor,
                insert_sql,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                page_size=100  # 分批处理，避免单次过大
            )
            
            # 获取返回结果
            try:
                results = cursor.fetchall()
            except Exception as fetch_error:
                logger.warning(f"获取RETURNING结果失败: {fetch_error}, 将使用备选方案统计")
                results = []
            
            conn.commit()
            
            # 统计插入和更新数量
            if results:
                inserted = sum(1 for row in results if row and row[0])
                processed = len(results)
                updated = processed - inserted
            else:
                # 如果无法获取RETURNING结果，假设全部成功插入
                processed = len(values)
                inserted = processed
                updated = 0
                logger.info(f"无法统计具体插入/更新数量，假设全部插入成功：{processed}条")
            
            logger.info(f"经济事件批量写入完成：新增{inserted}条，更新{updated}条，总计{processed}条")
            return {
                'processed': processed, 
                'inserted': inserted, 
                'updated': updated,
                'total_before_dedup': total_before_dedup,
                'duplicates_removed': duplicates_removed
            }
        
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"批量插入经济事件失败: {str(e)}")
            return {
                'processed': 0, 
                'inserted': 0, 
                'updated': 0,
                'total_before_dedup': total_before_dedup if 'total_before_dedup' in locals() else 0,
                'duplicates_removed': duplicates_removed if 'duplicates_removed' in locals() else 0
            }
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def update_economic_event(self, event_data: Dict[str, Any]) -> bool:
        """显式更新经济事件记录"""
        normalized = self._normalize_economic_event(event_data)
        if not normalized:
            return False
        
        conn = None
        cursor = None
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("无法获取数据库连接")
                return False
            
            cursor = conn.cursor()
            self._ensure_economic_events_table(cursor)
            
            update_sql = """
            UPDATE economic_events
            SET period = %(period)s,
                actual = %(actual)s,
                previous = %(previous)s,
                forecast = %(forecast)s,
                change_val = %(change_val)s,
                change_percentage = %(change_percentage)s,
                impact = %(impact)s,
                comparison = %(comparison)s,
                updated_at = CURRENT_TIMESTAMP
            WHERE date = %(date)s
              AND time = %(time)s
              AND country = %(country)s
              AND event = %(event)s
            """
            
            cursor.execute(update_sql, normalized)
            conn.commit()
            return cursor.rowcount > 0
        
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"更新经济事件失败: {str(e)}")
            return False
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def delete_old_events(self, retain_days: int = 365) -> int:
        """删除指定天数之前的旧经济事件数据"""
        if retain_days <= 0:
            logger.warning("保留天数必须大于0，已忽略删除操作")
            return 0
        
        from backend.utils.datetime_utils import utc_today
        cutoff_date = (utc_today() - timedelta(days=retain_days)).strftime('%Y-%m-%d')
        
        conn = None
        cursor = None
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("无法获取数据库连接")
                return 0
            
            cursor = conn.cursor()
            self._ensure_economic_events_table(cursor)
            
            delete_sql = """
            DELETE FROM economic_events
            WHERE TO_DATE(date, 'YYYY-MM-DD') < %s
            """
            cursor.execute(delete_sql, (cutoff_date,))
            deleted = cursor.rowcount
            conn.commit()
            
            logger.info(f"删除了 {deleted} 条{retain_days}天前的经济事件数据")
            return deleted
        
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"清理旧经济事件数据失败: {str(e)}")
            return 0
        
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def fetch_and_store_market_data(self, 
                                    asset_types: Optional[List[str]] = None,
                                    fetch_realtime: bool = True,
                                    fetch_eod: bool = True,
                                    eod_days: int = 5) -> Dict[str, Any]:
        """
        🚀 主要业务逻辑：并发获取并批量存储市场数据
        
        Args:
            asset_types: 要获取的资产类型列表 ['index', 'forex', 'commodity', 'bond', 'sector', 'etf']
            fetch_realtime: 是否获取实时数据
            fetch_eod: 是否获取EOD历史数据
            eod_days: 获取最近几天的EOD数据
        
        Returns:
            Dict包含执行结果
        """
        logger.info("🚀 开始并发获取并批量存储市场数据", 
                   asset_types=asset_types, 
                   fetch_realtime=fetch_realtime, 
                   fetch_eod=fetch_eod,
                   max_workers=self.max_workers)
        
        start_time = datetime.now()
        
        try:
            # 如果未指定资产类型，则获取所有类型
            if not asset_types:
                asset_types = ['index', 'forex', 'commodity', 'bond', 'sector', 'etf']

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
            if 'etf' in asset_types:
                all_assets.update({name: (code, 'etf', cat) for name, (code, _, cat) in ETF_MAP.items()})
            
            total_assets = len(all_assets)
            logger.info(f"准备并发获取{total_assets}个资产的数据 (max_workers={self.max_workers})")
            
            # 转换为列表格式 [(name, code, asset_type, category), ...]
            assets_list = [(name, code, asset_type, category) 
                          for name, (code, asset_type, category) in all_assets.items()]
            
            realtime_stats = {'success': 0, 'failed': 0}
            eod_stats = {'success': 0, 'failed': 0}
            
            # 🚀 并发获取实时数据
            if fetch_realtime:
                realtime_fetch_start = datetime.now()
                logger.info(f"开始并发获取实时数据 (资产数={total_assets}, 并发数={self.max_workers})")
                
                # 使用并发方法获取数据
                concurrent_results = self.concurrent_fetch_realtime_data(assets_list)
                
                # 解析数据
                realtime_records = []
                for name, result_data in concurrent_results.items():
                    parsed = self.parse_realtime_data(
                        result_data['code'], 
                        name, 
                        result_data['asset_type'], 
                        result_data['data']
                    )
                    if parsed:
                        realtime_records.append(parsed)
                
                # 🚀 批量存储实时数据
                if realtime_records:
                    storage_start = datetime.now()
                    result = self.store_realtime_data(realtime_records)
                    storage_time = (datetime.now() - storage_start).total_seconds()
                    
                    realtime_stats['success'] = result['success_count']
                    realtime_stats['failed'] = result['failed_count']
                    
                    fetch_time = (datetime.now() - realtime_fetch_start).total_seconds()
                    logger.info(f"✅ 实时数据完成: 获取用时{fetch_time:.2f}s, 存储用时{storage_time:.2f}s, "
                              f"成功{realtime_stats['success']}, 失败{realtime_stats['failed']}")
            
            # 🚀 并发获取EOD数据
            if fetch_eod:
                eod_fetch_start = datetime.now()
                logger.info(f"开始并发获取EOD数据 (资产数={total_assets}, 天数={eod_days}, 并发数={self.max_workers})")
                
                # 使用并发方法获取数据
                concurrent_results = self.concurrent_fetch_eod_data(assets_list, days=eod_days)
                
                # 解析数据
                all_eod_records = []
                for name, result_data in concurrent_results.items():
                    parsed_records = self.parse_eod_data(
                        result_data['code'], 
                        result_data['asset_type'], 
                        result_data['data']
                    )
                    all_eod_records.extend(parsed_records)
                
                # 🚀 批量存储EOD数据
                if all_eod_records:
                    storage_start = datetime.now()
                    result = self.store_eod_data(all_eod_records)
                    storage_time = (datetime.now() - storage_start).total_seconds()
                    
                    eod_stats['success'] = result['success_count']
                    eod_stats['failed'] = result['failed_count']
                    
                    fetch_time = (datetime.now() - eod_fetch_start).total_seconds()
                    logger.info(f"✅ EOD数据完成: 获取用时{fetch_time:.2f}s, 存储用时{storage_time:.2f}s, "
                              f"成功{eod_stats['success']}, 失败{eod_stats['failed']}")
            
            execution_time = (datetime.now() - start_time).total_seconds()
            speed = total_assets / execution_time if execution_time > 0 else 0
            
            logger.info(f"🎉 市场数据获取完成：实时数据成功{realtime_stats['success']}/失败{realtime_stats['failed']}，"
                       f"EOD数据成功{eod_stats['success']}/失败{eod_stats['failed']}，"
                       f"总耗时{execution_time:.2f}秒，平均速度{speed:.1f}资产/秒")
            
            return {
                'success': True,
                'message': f"成功获取市场数据：实时{realtime_stats['success']}条，历史{eod_stats['success']}条",
                'data': {
                    'total_assets': total_assets,
                    'realtime': realtime_stats,
                    'eod': eod_stats,
                    'execution_time': execution_time,
                    'speed': f"{speed:.2f} assets/sec"
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
