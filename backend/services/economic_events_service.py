#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
经济事件数据服务

基于EODHD API的经济事件数据获取和管理服务
支持从数据库读取已存储的经济事件数据
"""

import requests
import json
import os
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backend.utils.logger import info, error, warning, get_logger
# 统一的环境变量加载
from backend.utils.env_loader import load_project_env
from backend.database.db_manager import DatabaseManager
from config.settings import config
import psycopg2.extras


class EconomicEventsService:
    """经济事件数据服务类"""
    
    def __init__(self, api_token: str = None):
        """
        初始化API客户端
        
        Args:
            api_token: EODHD API密钥，如果不提供则从环境变量读取
        """
        self.logger = get_logger(__name__)
        self.base_url = "https://eodhd.com/api"
        
        # 获取API密钥
        self.api_token = api_token or os.getenv('EODHD_API_TOKEN')
        if not self.api_token:
            warning("EODHD_API_TOKEN未设置，数据可能受限", source="economic_events")
            self.api_token = "demo"
        
        # 设置默认请求参数
        self.session = self.create_robust_session()
        
        # 初始化数据库管理器
        db_config = config.get_db_config()
        self.db_manager = DatabaseManager({
            'db_host': db_config['host'],
            'db_port': db_config['port'],
            'db_name': db_config['name'],
            'db_user': db_config['user'],
            'db_password': db_config['password']
        })
        
        info("经济事件API服务初始化完成", source="economic_events")
    
    def create_robust_session(self):
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
            'User-Agent': 'Universal Glance/1.0'
        })
        
        return session
    
    def safe_api_request(self, url: str, params: dict, max_retries: int = 3) -> Optional[Dict]:
        """安全的API请求函数，处理SSL错误和各种异常"""
        session = self.session
        
        # 打印完整的EODHD API请求信息
        info(f"EODHD API请求详情:", source="economic_events")
        info(f"  URL: {url}", source="economic_events")
        info(f"  参数: {json.dumps(params, indent=2, ensure_ascii=False)}", source="economic_events")
        
        for attempt in range(max_retries):
            try:
                info(f"API请求 (第{attempt + 1}次): {url}", source="economic_events")
                
                response = session.get(url, params=params, timeout=(10, 30))
                response.raise_for_status()
                return response.json()
                
            except requests.exceptions.SSLError as e:
                warning(f"SSL错误 (第{attempt + 1}次): {e}", source="economic_events")
                if attempt < max_retries - 1:
                    # 重新创建session以解决SSL连接问题
                    session.close()
                    session = self.create_robust_session()
                    self.session = session
                    time.sleep(2 ** attempt)  # 指数退避
                else:
                    error("达到最大重试次数，SSL错误持续存在", source="economic_events")
                    raise
                    
            except requests.exceptions.RequestException as e:
                warning(f"请求错误 (第{attempt + 1}次): {e}", source="economic_events")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # 指数退避
                else:
                    error("达到最大重试次数，API请求失败", source="economic_events")
                    raise
            except json.JSONDecodeError as e:
                error(f"JSON解析失败: {e}", source="economic_events")
                raise Exception(f"响应数据解析失败: {str(e)}")
        
        return None

    def get_economic_events(
        self, 
        from_date: Union[str, datetime] = None,
        to_date: Union[str, datetime] = None,
        country: str = None,
        offset: int = 0,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        获取经济事件数据
        
        Args:
            from_date: 开始日期 (YYYY-MM-DD 或 datetime对象)
            to_date: 结束日期 (YYYY-MM-DD 或 datetime对象)
            country: 国家代码 (ISO 3166格式，2个字符)
            offset: 偏移量 (0-1000)
            limit: 限制数量 (0-1000)
        
        Returns:
            经济事件数据列表
        """
        info("开始获取经济事件数据", source="economic_events")
        
        # 构建请求参数
        params = {
            'api_token': self.api_token,
            'fmt': 'json'
        }
        
        # 处理日期参数
        if from_date:
            if isinstance(from_date, datetime):
                params['from'] = from_date.strftime('%Y-%m-%d')
            else:
                params['from'] = from_date
        
        if to_date:
            if isinstance(to_date, datetime):
                params['to'] = to_date.strftime('%Y-%m-%d')
            else:
                params['to'] = to_date
        
        # 添加其他参数
        if country:
            params['country'] = country
        if offset > 0:
            params['offset'] = offset
        if limit != 50:
            params['limit'] = limit
        
        try:
            # 使用安全的API请求方法
            url = f"{self.base_url}/economic-events"
            data = self.safe_api_request(url, params)
            
            if data is None:
                error("API请求返回空数据", source="economic_events")
                return []
            
            if isinstance(data, list):
                info(f"成功获取 {len(data)} 条经济事件数据", source="economic_events")
                return data
            else:
                warning(f"API返回非预期格式: {type(data)}", source="economic_events")
                return []
                
        except Exception as e:
            error(f"获取经济事件数据失败: {str(e)}", source="economic_events")
            raise Exception(f"获取经济事件数据失败: {str(e)}")
    
    def get_recent_events(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        获取最近几天的经济事件
        
        Args:
            days: 天数
        
        Returns:
            经济事件数据列表
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        return self.get_economic_events(
            from_date=start_date,
            to_date=end_date,
            limit=100
        )
    
    def get_upcoming_events(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        获取未来几天的经济事件
        
        Args:
            days: 天数
        
        Returns:
            经济事件数据列表
        """
        start_date = datetime.now()
        end_date = start_date + timedelta(days=days)
        
        return self.get_economic_events(
            from_date=start_date,
            to_date=end_date,
            limit=100
        )
    
    def get_events_by_country(self, country_code: str, days: int = 30) -> List[Dict[str, Any]]:
        """
        获取特定国家的经济事件
        
        Args:
            country_code: 国家代码 (ISO 3166格式)
            days: 天数范围
        
        Returns:
            经济事件数据列表
        """
        end_date = datetime.now() + timedelta(days=days//2)
        start_date = end_date - timedelta(days=days)
        
        return self.get_economic_events(
            from_date=start_date,
            to_date=end_date,
            country=country_code,
            limit=200
        )
    

    
    def get_supported_countries(self) -> List[str]:
        """
        获取支持的国家代码列表
        
        Returns:
            国家代码列表
        """
        # 常见的国家代码
        return [
            'US', 'CN', 'JP', 'DE', 'GB', 'FR', 'IT', 'CA', 'AU', 'KR',
            'IN', 'BR', 'RU', 'MX', 'ES', 'NL', 'CH', 'SE', 'BE', 'NO',
            'AT', 'FI', 'DK', 'PL', 'IE', 'PT', 'CZ', 'NZ', 'IL', 'SG'
        ]
    

    
    def format_event_data(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        格式化事件数据
        
        Args:
            event: 原始事件数据
        
        Returns:
            格式化后的事件数据
        """
        # 解析日期时间字符串
        date_str = event.get('date', '')
        time_str = ''
        date_only = ''
        
        if date_str:
            try:
                # 解析完整的日期时间字符串 "2025-08-04 23:50:00"
                if ' ' in date_str:
                    date_only, time_str = date_str.split(' ', 1)
                else:
                    date_only = date_str
            except:
                date_only = date_str
        
        formatted = {
            'date': date_only,
            'time': time_str,
            'country': event.get('country', ''),
            'event': event.get('type', ''),  # API返回的是'type'字段
            'currency': '',  # EODHD API 不返回currency字段，可能需要从其他地方获取
            'actual': event.get('actual', ''),
            'previous': event.get('previous', ''),
            'forecast': event.get('estimate', ''),  # API返回的是'estimate'字段
            'impact': '',  # EODHD API 不返回impact字段
            'unit': '',   # EODHD API 不返回unit字段
            'period': event.get('period', ''),
            'change': event.get('change', ''),
            'change_percentage': event.get('change_percentage', '')
        }
        
        # 计算变化值
        if formatted['actual'] and formatted['previous']:
            try:
                actual_val = float(formatted['actual'])
                previous_val = float(formatted['previous'])
                change = actual_val - previous_val
                change_pct = (change / previous_val) * 100 if previous_val != 0 else 0
                formatted['calculated_change'] = round(change, 4)
                formatted['calculated_change_pct'] = round(change_pct, 2)
            except (ValueError, TypeError):
                formatted['calculated_change'] = None
                formatted['calculated_change_pct'] = None
        
        return formatted
    
    def get_economic_events_from_db(
        self,
        from_date: Union[str, datetime] = None,
        to_date: Union[str, datetime] = None,
        country: str = None,
        offset: int = 0,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        从数据库获取经济事件数据
        
        Args:
            from_date: 开始日期 (YYYY-MM-DD 或 datetime对象)
            to_date: 结束日期 (YYYY-MM-DD 或 datetime对象)
            country: 国家代码 (ISO 3166格式，2个字符)，可以是逗号分隔的多个国家
            offset: 偏移量
            limit: 限制数量
        
        Returns:
            经济事件数据列表
        """
        info(f"从数据库获取经济事件数据: from_date={from_date}, to_date={to_date}, country={country}", source="economic_events")
        
        # 构建SQL查询
        query = "SELECT * FROM economic_events WHERE 1=1"
        params = []
        
        # 处理日期参数
        if from_date:
            if isinstance(from_date, datetime):
                from_date_str = from_date.strftime('%Y-%m-%d')
            else:
                from_date_str = from_date
            query += " AND date >= %s"
            params.append(from_date_str)
        
        if to_date:
            if isinstance(to_date, datetime):
                to_date_str = to_date.strftime('%Y-%m-%d')
            else:
                to_date_str = to_date
            query += " AND date <= %s"
            params.append(to_date_str)
        
        # 处理国家参数（支持多个国家）
        if country:
            # 分割国家代码（支持逗号分隔）
            countries = [c.strip() for c in country.split(',') if c.strip()]
            if countries:
                placeholders = ','.join(['%s'] * len(countries))
                query += f" AND country IN ({placeholders})"
                params.extend(countries)
        
        # 添加排序、分页
        query += " ORDER BY date DESC, time DESC NULLS LAST"
        query += " LIMIT %s OFFSET %s"
        params.extend([limit, offset])
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                error("无法连接到数据库", source="economic_events")
                return []
            
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(query, params)
            results = cursor.fetchall()
            conn.close()
            
            # 转换为字典列表
            events = []
            for row in results:
                event = dict(row)
                # 转换为前端期望的格式
                formatted_event = {
                    'date': event.get('date', ''),
                    'time': event.get('time', ''),
                    'country': event.get('country', ''),
                    'event': event.get('event', ''),
                    'period': event.get('period', ''),
                    'actual': event.get('actual', ''),
                    'previous': event.get('previous', ''),
                    'forecast': event.get('forecast', ''),
                    'change_val': event.get('change_val'),
                    'change_percentage': event.get('change_percentage'),
                    'impact': event.get('impact', 1),
                    'comparison': event.get('comparison', '')
                }
                events.append(formatted_event)
            
            info(f"从数据库获取到 {len(events)} 条经济事件数据", source="economic_events")
            return events
            
        except Exception as e:
            error(f"从数据库获取经济事件数据失败: {str(e)}", source="economic_events")
            return []
    
    def get_available_countries_from_db(self) -> List[Dict[str, str]]:
        """
        从数据库获取可用的国家列表
        
        Returns:
            国家列表，格式为 [{'code': 'US', 'name': 'United States'}, ...]
        """
        info("从数据库获取可用国家列表", source="economic_events")
        
        query = """
            SELECT DISTINCT country 
            FROM economic_events 
            WHERE country IS NOT NULL AND country != ''
            ORDER BY country
        """
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                error("无法连接到数据库", source="economic_events")
                return []
            
            cursor = conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            conn.close()
            
            # 国家代码到国家名称的映射
            country_names = {
                'US': 'United States', 'CN': 'China', 'JP': 'Japan', 
                'DE': 'Germany', 'GB': 'United Kingdom', 'FR': 'France',
                'IT': 'Italy', 'CA': 'Canada', 'AU': 'Australia', 'KR': 'South Korea',
                'IN': 'India', 'BR': 'Brazil', 'RU': 'Russia', 'MX': 'Mexico',
                'ES': 'Spain', 'NL': 'Netherlands', 'CH': 'Switzerland', 'SE': 'Sweden',
                'BE': 'Belgium', 'NO': 'Norway', 'AT': 'Austria', 'FI': 'Finland',
                'DK': 'Denmark', 'PL': 'Poland', 'IE': 'Ireland', 'PT': 'Portugal',
                'CZ': 'Czech Republic', 'NZ': 'New Zealand', 'IL': 'Israel', 'SG': 'Singapore',
                'HK': 'Hong Kong', 'TW': 'Taiwan', 'TH': 'Thailand', 'MY': 'Malaysia',
                'ID': 'Indonesia', 'PH': 'Philippines', 'VN': 'Vietnam', 'SA': 'Saudi Arabia',
                'AE': 'United Arab Emirates', 'ZA': 'South Africa', 'EG': 'Egypt', 'NG': 'Nigeria',
                'AR': 'Argentina', 'CL': 'Chile', 'CO': 'Colombia', 'PE': 'Peru',
                'TR': 'Turkey', 'GR': 'Greece', 'HU': 'Hungary', 'RO': 'Romania'
            }
            
            countries = []
            for row in results:
                country_code = row[0]
                country_name = country_names.get(country_code, country_code)
                countries.append({
                    'code': country_code,
                    'name': country_name
                })
            
            info(f"从数据库获取到 {len(countries)} 个国家", source="economic_events")
            return countries
            
        except Exception as e:
            error(f"从数据库获取国家列表失败: {str(e)}", source="economic_events")
            return []


