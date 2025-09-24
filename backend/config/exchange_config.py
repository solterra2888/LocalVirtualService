# -*- coding: utf-8 -*-
"""
Exchange Configuration - 全球交易所代码统一配置

这个模块提供了全球主要交易所的代码映射表，确保与EODHD等数据提供商的规范一致。
支持股票代码自动识别和格式化功能。

主要功能：
1. 提供EODHD官方exchange code映射
2. 支持股票代码自动识别交易所
3. 提供交易所元数据（国家、货币、时区等）
4. 统一的ticker格式化功能
"""

from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


@dataclass
class ExchangeInfo:
    """交易所信息数据类"""
    code: str                    # EODHD官方代码
    name: str                    # 交易所全名
    country: str                 # 国家
    currency: str                # 交易货币
    country_iso2: str            # ISO2国家代码
    country_iso3: str            # ISO3国家代码
    mic_code: Optional[str] = None  # MIC代码
    timezone: Optional[str] = None  # 时区


class ExchangeType(Enum):
    """交易所类型枚举"""
    STOCK = "stock"
    FOREX = "forex"
    CRYPTO = "crypto"
    COMMODITY = "commodity"


class ExchangeConfig:
    """全球交易所配置类"""
    
    # EODHD官方交易所代码映射表
    # 基于EODHD官方文档和API返回的交易所列表
    EXCHANGE_MAPPING = {
        # 美国交易所
        'US': ExchangeInfo(
            code='US',
            name='United States',
            country='United States',
            currency='USD',
            country_iso2='US',
            country_iso3='USA',
            mic_code='XNYS',  # NYSE
            timezone='America/New_York'
        ),
        'NYSE': ExchangeInfo(
            code='US',
            name='New York Stock Exchange',
            country='United States',
            currency='USD',
            country_iso2='US',
            country_iso3='USA',
            mic_code='XNYS',
            timezone='America/New_York'
        ),
        'NASDAQ': ExchangeInfo(
            code='US',
            name='NASDAQ',
            country='United States',
            currency='USD',
            country_iso2='US',
            country_iso3='USA',
            mic_code='XNAS',
            timezone='America/New_York'
        ),
        'AMEX': ExchangeInfo(
            code='US',
            name='American Stock Exchange',
            country='United States',
            currency='USD',
            country_iso2='US',
            country_iso3='USA',
            mic_code='XASE',
            timezone='America/New_York'
        ),
        
        # 中国交易所
        'SHG': ExchangeInfo(
            code='SHG',
            name='Shanghai Stock Exchange',
            country='China',
            currency='CNY',
            country_iso2='CN',
            country_iso3='CHN',
            mic_code='XSHG',
            timezone='Asia/Shanghai'
        ),
        'SHE': ExchangeInfo(
            code='SHE',
            name='Shenzhen Stock Exchange',
            country='China',
            currency='CNY',
            country_iso2='CN',
            country_iso3='CHN',
            mic_code='XSHE',
            timezone='Asia/Shanghai'
        ),
        
        # 香港交易所
        'HK': ExchangeInfo(
            code='HK',
            name='Hong Kong Exchanges and Clearing',
            country='Hong Kong',
            currency='HKD',
            country_iso2='HK',
            country_iso3='HKG',
            mic_code='XHKG',
            timezone='Asia/Hong_Kong'
        ),
        
        # 台湾交易所
        'TWO': ExchangeInfo(
            code='TWO',
            name='Taiwan OTC Exchange',
            country='Taiwan',
            currency='TWD',
            country_iso2='TW',
            country_iso3='TWN',
            mic_code='ROCO',
            timezone='Asia/Taipei'
        ),
        'TW': ExchangeInfo(
            code='TWO',
            name='Taiwan Stock Exchange',
            country='Taiwan',
            currency='TWD',
            country_iso2='TW',
            country_iso3='TWN',
            mic_code='XTAI',
            timezone='Asia/Taipei'
        ),
        
        # 日本交易所
        'JP': ExchangeInfo(
            code='JP',
            name='Tokyo Stock Exchange',
            country='Japan',
            currency='JPY',
            country_iso2='JP',
            country_iso3='JPN',
            mic_code='XTKS',
            timezone='Asia/Tokyo'
        ),
        
        # 英国交易所
        'LSE': ExchangeInfo(
            code='LSE',
            name='London Stock Exchange',
            country='United Kingdom',
            currency='GBP',
            country_iso2='GB',
            country_iso3='GBR',
            mic_code='XLON',
            timezone='Europe/London'
        ),
        'AIM': ExchangeInfo(
            code='AIM',
            name='Alternative Investment Market',
            country='United Kingdom',
            currency='GBP',
            country_iso2='GB',
            country_iso3='GBR',
            mic_code='XAMS',
            timezone='Europe/London'
        ),
        
        # 欧洲交易所
        'PA': ExchangeInfo(
            code='PA',
            name='Euronext Paris',
            country='France',
            currency='EUR',
            country_iso2='FR',
            country_iso3='FRA',
            mic_code='XPAR',
            timezone='Europe/Paris'
        ),
        'AS': ExchangeInfo(
            code='AS',
            name='Euronext Amsterdam',
            country='Netherlands',
            currency='EUR',
            country_iso2='NL',
            country_iso3='NLD',
            mic_code='XAMS',
            timezone='Europe/Amsterdam'
        ),
        'BR': ExchangeInfo(
            code='BR',
            name='Euronext Brussels',
            country='Belgium',
            currency='EUR',
            country_iso2='BE',
            country_iso3='BEL',
            mic_code='XBRU',
            timezone='Europe/Brussels'
        ),
        'VX': ExchangeInfo(
            code='VX',
            name='SIX Swiss Exchange',
            country='Switzerland',
            currency='CHF',
            country_iso2='CH',
            country_iso3='CHE',
            mic_code='XSWX',
            timezone='Europe/Zurich'
        ),
        
        # 加拿大交易所
        'TO': ExchangeInfo(
            code='TO',
            name='Toronto Stock Exchange',
            country='Canada',
            currency='CAD',
            country_iso2='CA',
            country_iso3='CAN',
            mic_code='XTSE',
            timezone='America/Toronto'
        ),
        'TSX': ExchangeInfo(
            code='TO',
            name='Toronto Stock Exchange',
            country='Canada',
            currency='CAD',
            country_iso2='CA',
            country_iso3='CAN',
            mic_code='XTSE',
            timezone='America/Toronto'
        ),
        
        # 澳大利亚交易所
        'AU': ExchangeInfo(
            code='AU',
            name='Australian Securities Exchange',
            country='Australia',
            currency='AUD',
            country_iso2='AU',
            country_iso3='AUS',
            mic_code='XASX',
            timezone='Australia/Sydney'
        ),
        'ASX': ExchangeInfo(
            code='AU',
            name='Australian Securities Exchange',
            country='Australia',
            currency='AUD',
            country_iso2='AU',
            country_iso3='AUS',
            mic_code='XASX',
            timezone='Australia/Sydney'
        ),
        
        # 印度交易所
        'NSE': ExchangeInfo(
            code='NSE',
            name='National Stock Exchange of India',
            country='India',
            currency='INR',
            country_iso2='IN',
            country_iso3='IND',
            mic_code='XNSE',
            timezone='Asia/Kolkata'
        ),
        'BSE': ExchangeInfo(
            code='BSE',
            name='Bombay Stock Exchange',
            country='India',
            currency='INR',
            country_iso2='IN',
            country_iso3='IND',
            mic_code='XBOM',
            timezone='Asia/Kolkata'
        ),
        
        # 韩国交易所
        'KR': ExchangeInfo(
            code='KR',
            name='Korea Exchange',
            country='South Korea',
            currency='KRW',
            country_iso2='KR',
            country_iso3='KOR',
            mic_code='XKRX',
            timezone='Asia/Seoul'
        ),
        
        # 新加坡交易所
        'SG': ExchangeInfo(
            code='SG',
            name='Singapore Exchange',
            country='Singapore',
            currency='SGD',
            country_iso2='SG',
            country_iso3='SGP',
            mic_code='XSES',
            timezone='Asia/Singapore'
        ),
        
        # 巴西交易所
        'SA': ExchangeInfo(
            code='SA',
            name='B3 - Brasil Bolsa Balcão',
            country='Brazil',
            currency='BRL',
            country_iso2='BR',
            country_iso3='BRA',
            mic_code='BVMF',
            timezone='America/Sao_Paulo'
        ),
        
        # 墨西哥交易所
        'MX': ExchangeInfo(
            code='MX',
            name='Bolsa Mexicana de Valores',
            country='Mexico',
            currency='MXN',
            country_iso2='MX',
            country_iso3='MEX',
            mic_code='XMEX',
            timezone='America/Mexico_City'
        ),
        
        # 俄罗斯交易所
        'MCX': ExchangeInfo(
            code='MCX',
            name='Moscow Exchange',
            country='Russia',
            currency='RUB',
            country_iso2='RU',
            country_iso3='RUS',
            mic_code='MISX',
            timezone='Europe/Moscow'
        ),
        
        # 南非交易所
        'JO': ExchangeInfo(
            code='JO',
            name='Johannesburg Stock Exchange',
            country='South Africa',
            currency='ZAR',
            country_iso2='ZA',
            country_iso3='ZAF',
            mic_code='XJSE',
            timezone='Africa/Johannesburg'
        ),
    }
    
    # 股票代码模式识别规则
    # 用于自动识别股票代码对应的交易所
    TICKER_PATTERNS = {
        # 中国A股 - 6位数字
        'SHG': {
            'patterns': ['600', '601', '603', '605', '688'],  # 上海主板、科创板
            'length': 6,
            'description': '上海证券交易所'
        },
        'SHE': {
            'patterns': ['000', '002', '300'],  # 深圳主板、中小板、创业板
            'length': 6,
            'description': '深圳证券交易所'
        },
        
        # 香港股票 - 4位数字，以0开头
        'HK': {
            'patterns': ['0'],  # 香港股票通常以0开头
            'length': 4,
            'description': '香港交易所'
        },
        
        # 台湾股票 - 4位数字，以2开头
        'TWO': {
            'patterns': ['2'],  # 台湾股票通常以2开头
            'length': 4,
            'description': '台湾证券交易所'
        },
        
        # 日本股票 - 4位数字，以7开头
        'JP': {
            'patterns': ['7'],  # 日本股票通常以7开头
            'length': 4,
            'description': '东京证券交易所'
        },
        
        # 韩国股票 - 6位数字
        'KR': {
            'patterns': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'],  # 6位数字
            'length': 6,
            'description': '韩国交易所'
        },
        
        # 印度股票 - 6位数字
        'NSE': {
            'patterns': ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'],  # 6位数字
            'length': 6,
            'description': '印度国家证券交易所'
        },
    }
    
    # 支持的基本面数据交易所（EODHD基本面API支持）
    FUNDAMENTALS_SUPPORTED_EXCHANGES = [
        'US', 'NYSE', 'NASDAQ', 'AMEX',  # 美国
        'SHG', 'SHE',                     # 中国
        'HK',                             # 香港
        'TWO', 'TW',                     # 台湾
        'JP',                             # 日本
        'LSE', 'AIM',                    # 英国
        'PA', 'AS', 'BR', 'VX',          # 欧洲
        'TO', 'TSX',                     # 加拿大
        'AU', 'ASX',                     # 澳大利亚
        'NSE', 'BSE',                    # 印度
        'KR',                             # 韩国
        'SG',                             # 新加坡
        'SA',                             # 巴西
        'MX',                             # 墨西哥
        'MCX',                            # 俄罗斯
        'JO',                             # 南非
    ]
    
    # 支持实时价格数据的交易所
    REALTIME_PRICES_SUPPORTED_EXCHANGES = [
        'US', 'NYSE', 'NASDAQ', 'AMEX',  # 美国
        'SHG', 'SHE',                     # 中国
        'HK',                             # 香港
        'TWO', 'TW',                     # 台湾
        'JP',                             # 日本
        'LSE', 'AIM',                    # 英国
        'PA', 'AS', 'BR', 'VX',          # 欧洲
        'TO', 'TSX',                     # 加拿大
        'AU', 'ASX',                     # 澳大利亚
        'NSE', 'BSE',                    # 印度
        'KR',                             # 韩国
        'SG',                             # 新加坡
        'SA',                             # 巴西
        'MX',                             # 墨西哥
        'MCX',                            # 俄罗斯
        'JO',                             # 南非
    ]
    
    @classmethod
    def get_exchange_info(cls, exchange_code: str) -> Optional[ExchangeInfo]:
        """获取交易所信息"""
        return cls.EXCHANGE_MAPPING.get(exchange_code.upper())
    
    @classmethod
    def get_eodhd_code(cls, exchange_code: str) -> str:
        """获取EODHD官方代码"""
        exchange_info = cls.get_exchange_info(exchange_code)
        if exchange_info:
            return exchange_info.code
        return exchange_code.upper()
    
    @classmethod
    def format_ticker_for_eodhd(cls, ticker: str) -> str:
        """
        格式化ticker为EODHD API格式
        
        Args:
            ticker: 股票代码，可能包含或不包含交易所后缀
            
        Returns:
            格式化后的ticker，格式为 SYMBOL.EXCHANGE
        """
        if not ticker:
            return ticker
            
        ticker = ticker.strip().upper()
        
        # 如果ticker已经包含交易所后缀，直接返回
        if '.' in ticker:
            symbol, exchange = ticker.split('.', 1)
            eodhd_exchange = cls.get_eodhd_code(exchange)
            return f"{symbol}.{eodhd_exchange}"
        
        # 根据ticker特征自动识别交易所
        detected_exchange = cls._detect_exchange_by_ticker(ticker)
        if detected_exchange:
            return f"{ticker}.{detected_exchange}"
        
        # 默认为美国交易所
        return f"{ticker}.US"
    
    @classmethod
    def _detect_exchange_by_ticker(cls, ticker: str) -> Optional[str]:
        """
        根据ticker特征自动检测交易所
        
        Args:
            ticker: 股票代码
            
        Returns:
            检测到的交易所代码，如果无法检测则返回None
        """
        if not ticker.isdigit():
            return None
            
        ticker_length = len(ticker)
        
        # 遍历所有模式规则
        for exchange_code, pattern_info in cls.TICKER_PATTERNS.items():
            if ticker_length == pattern_info['length']:
                # 检查是否匹配任何模式
                for pattern in pattern_info['patterns']:
                    if ticker.startswith(pattern):
                        return cls.get_eodhd_code(exchange_code)
        
        return None
    
    @classmethod
    def get_supported_exchanges(cls, data_type: str = 'realtime') -> List[str]:
        """
        获取支持指定数据类型的交易所列表
        
        Args:
            data_type: 数据类型 ('realtime', 'fundamentals')
            
        Returns:
            支持的交易所代码列表
        """
        if data_type == 'fundamentals':
            return cls.FUNDAMENTALS_SUPPORTED_EXCHANGES.copy()
        elif data_type == 'realtime':
            return cls.REALTIME_PRICES_SUPPORTED_EXCHANGES.copy()
        else:
            return list(cls.EXCHANGE_MAPPING.keys())
    
    @classmethod
    def is_exchange_supported(cls, exchange_code: str, data_type: str = 'realtime') -> bool:
        """
        检查交易所是否支持指定数据类型
        
        Args:
            exchange_code: 交易所代码
            data_type: 数据类型 ('realtime', 'fundamentals')
            
        Returns:
            是否支持
        """
        supported_exchanges = cls.get_supported_exchanges(data_type)
        eodhd_code = cls.get_eodhd_code(exchange_code)
        return eodhd_code in supported_exchanges
    
    @classmethod
    def get_exchanges_by_country(cls, country_iso2: str) -> List[str]:
        """
        根据国家代码获取交易所列表
        
        Args:
            country_iso2: ISO2国家代码
            
        Returns:
            该国家的交易所代码列表
        """
        exchanges = []
        for exchange_code, exchange_info in cls.EXCHANGE_MAPPING.items():
            if exchange_info.country_iso2.upper() == country_iso2.upper():
                exchanges.append(exchange_code)
        return exchanges
    
    @classmethod
    def get_exchanges_by_currency(cls, currency: str) -> List[str]:
        """
        根据货币代码获取交易所列表
        
        Args:
            currency: 货币代码
            
        Returns:
            使用该货币的交易所代码列表
        """
        exchanges = []
        for exchange_code, exchange_info in cls.EXCHANGE_MAPPING.items():
            if exchange_info.currency.upper() == currency.upper():
                exchanges.append(exchange_code)
        return exchanges
    
    @classmethod
    def validate_ticker_format(cls, ticker: str) -> Tuple[bool, str]:
        """
        验证ticker格式是否正确
        
        Args:
            ticker: 股票代码
            
        Returns:
            (是否有效, 错误信息)
        """
        if not ticker:
            return False, "Ticker不能为空"
            
        ticker = ticker.strip().upper()
        
        # 检查是否包含交易所后缀
        if '.' not in ticker:
            return False, "Ticker必须包含交易所后缀，如: AAPL.US"
            
        symbol, exchange = ticker.split('.', 1)
        
        # 检查交易所是否支持
        if not cls.get_exchange_info(exchange):
            return False, f"不支持的交易所代码: {exchange}"
            
        # 检查符号是否为空
        if not symbol:
            return False, "股票符号不能为空"
            
        return True, ""


# 便捷函数
def format_ticker_for_eodhd(ticker: str) -> str:
    """格式化ticker为EODHD API格式的便捷函数"""
    return ExchangeConfig.format_ticker_for_eodhd(ticker)


def get_exchange_info(exchange_code: str) -> Optional[ExchangeInfo]:
    """获取交易所信息的便捷函数"""
    return ExchangeConfig.get_exchange_info(exchange_code)


def is_exchange_supported(exchange_code: str, data_type: str = 'realtime') -> bool:
    """检查交易所是否支持的便捷函数"""
    return ExchangeConfig.is_exchange_supported(exchange_code, data_type)


def validate_ticker_format(ticker: str) -> Tuple[bool, str]:
    """验证ticker格式的便捷函数"""
    return ExchangeConfig.validate_ticker_format(ticker)


