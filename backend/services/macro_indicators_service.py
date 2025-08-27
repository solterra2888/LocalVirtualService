#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
宏观经济指标数据服务

基于EODHD API的宏观经济指标数据获取和管理服务
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
from backend.database.db_manager import DatabaseManager
from config.settings import config


class MacroIndicatorsService:
    """EODHD宏观经济指标数据服务类"""
    
    # 所有可用的宏观经济指标
    AVAILABLE_INDICATORS = {
        'real_interest_rate': 'Real interest rate (%)',
        'population_total': 'Population, total',
        'population_growth_annual': 'Population growth (annual %)',
        'inflation_consumer_prices_annual': 'Inflation, consumer prices (annual %)',
        'consumer_price_index': 'Consumer Price Index (2010 = 100)',
        'gdp_current_usd': 'GDP (current US$)',
        'gdp_per_capita_usd': 'GDP per capita (current US$)',
        'gdp_growth_annual': 'GDP growth (annual %)',
        'debt_percent_gdp': 'Debt in percent of GDP (annual %)',
        'net_trades_goods_services': 'Net trades in goods and services (current US$)',
        'inflation_gdp_deflator_annual': 'Inflation, GDP deflator (annual %)',
        'agriculture_value_added_percent_gdp': 'Agriculture, value added (% of GDP)',
        'industry_value_added_percent_gdp': 'Industry, value added (% of GDP)',
        'services_value_added_percent_gdp': 'Services, etc., value added (% of GDP)',
        'exports_of_goods_services_percent_gdp': 'Exports of goods and services (% of GDP)',
        'imports_of_goods_services_percent_gdp': 'Imports of goods and services (% of GDP)',
        'gross_capital_formation_percent_gdp': 'Gross capital formation (% of GDP)',
        'net_migration': 'Net migration (absolute value)',
        'gni_usd': 'GNI, Atlas method (current US$)',
        'gni_per_capita_usd': 'GNI per capita, Atlas method (current US$)',
        'gni_ppp_usd': 'GNI, PPP (current international $)',
        'gni_per_capita_ppp_usd': 'GNI per capita, PPP (current international $)',
        'income_share_lowest_twenty': 'Income share held by lowest 20% (in %)',
        'life_expectancy': 'Life expectancy at birth, total (years)',
        'fertility_rate': 'Fertility rate, total (births per woman)',
        'prevalence_hiv_total': 'Prevalence of HIV, total (% of population ages 15-49)',
        'co2_emissions_tons_per_capita': 'CO2 emissions (metric tons per capita)',
        'surface_area_km': 'Surface area (sq. km)',
        'poverty_poverty_lines_percent_population': 'Poverty headcount ratio at national poverty lines (% of population)',
        'revenue_excluding_grants_percent_gdp': 'Revenue, excluding grants (% of GDP)',
        'cash_surplus_deficit_percent_gdp': 'Cash surplus/deficit (% of GDP)',
        'startup_procedures_register': 'Start-up procedures to register a business (number)',
        'market_cap_domestic_companies_percent_gdp': 'Market capitalization of listed domestic companies (% of GDP)',
        'mobile_subscriptions_per_hundred': 'Mobile cellular subscriptions (per 100 people)',
        'internet_users_per_hundred': 'Internet users (per 100 people)',
        'high_technology_exports_percent_total': 'High-technology exports (% of manufactured exports)',
        'merchandise_trade_percent_gdp': 'Merchandise trade (% of GDP)',
        'total_debt_service_percent_gni': 'Total debt service (% of GNI)',
        'unemployment_total_percent': 'Unemployment total (% of labor force)'
    }
    
    # 常用国家代码（ISO 3166 Alpha-3格式）
    COMMON_COUNTRIES = {
        'USA': 'United States',
        'CHN': 'China',
        'JPN': 'Japan',
        'DEU': 'Germany',
        'GBR': 'United Kingdom',
        'FRA': 'France',
        'ITA': 'Italy',
        'BRA': 'Brazil',
        'CAN': 'Canada',
        'RUS': 'Russia',
        'IND': 'India',
        'KOR': 'South Korea',
        'ESP': 'Spain',
        'AUS': 'Australia',
        'MEX': 'Mexico',
        'IDN': 'Indonesia',
        'NLD': 'Netherlands',
        'SAU': 'Saudi Arabia',
        'TUR': 'Turkey',
        'TWN': 'Taiwan'
    }
    
    def __init__(self, api_token: str = None):
        """
        初始化API客户端
        
        Args:
            api_token: EODHD API密钥，如果不提供则从环境变量读取
        """
        self.logger = get_logger(__name__)
        self.base_url = "https://eodhd.com/api"
        
        # 获取API密钥
        self.api_token = api_token or config.get("EODHD_API_TOKEN") or config.get("API_KEY", "demo")
        if self.api_token == "demo":
            warning("使用演示API密钥，数据可能受限", source="macro_indicators")
        
        # 设置默认请求参数
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Universal Glance/1.0'
        })
        
        # 初始化数据库连接
        self._init_database()
        
        info("宏观经济指标API服务初始化完成", source="macro_indicators")
    
    def _init_database(self):
        """初始化数据库连接"""
        try:
            # 获取数据库连接参数
            db_conn_params = {
                'db_host': config.get('DB_HOST', 'localhost'),
                'db_port': config.get('DB_PORT', '5432'),
                'db_name': config.get('DB_NAME', 'targettracking'),
                'db_user': config.get('DB_USER', 'postgres'),
                'db_password': config.get('DB_PASSWORD', '')
            }
            self.db_manager = DatabaseManager(db_conn_params)
            info("宏观指标数据库连接初始化完成", source="macro_indicators")
        except Exception as e:
            error(f"数据库初始化失败: {e}", source="macro_indicators")
            self.db_manager = None
    
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
        
        # 创建适配器
        adapter = HTTPAdapter(max_retries=retry_strategy)
        
        # 挂载适配器
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def safe_api_request(self, url: str, session: requests.Session, timeout: int = 30) -> Optional[Dict]:
        """
        安全的API请求，包含错误处理
        
        Args:
            url: 请求URL
            session: 请求会话
            timeout: 超时时间
            
        Returns:
            API响应数据或None
        """
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
            
        except requests.exceptions.Timeout:
            error(f"API请求超时: {url}", source="macro_indicators")
            return None
        except requests.exceptions.RequestException as e:
            error(f"API请求失败: {e}", source="macro_indicators")
            return None
        except json.JSONDecodeError as e:
            error(f"JSON解析失败: {e}", source="macro_indicators")
            return None
    
    def get_macro_indicator(
        self, 
        country: str,
        indicator: str = 'gdp_current_usd',
        fmt: str = 'json'
    ) -> List[Dict[str, Any]]:
        """
        获取指定国家的宏观经济指标数据
        
        Args:
            country: 国家代码 (ISO 3166 Alpha-3，如 USA, CHN)
            indicator: 指标名称
            fmt: 返回格式 ('json' 或 'csv')
            
        Returns:
            List[Dict]: 宏观经济指标数据列表
        """
        start_time = time.time()
        
        try:
            # 构建请求URL
            url = f"{self.base_url}/macro-indicator/{country}?indicator={indicator}&fmt={fmt}&api_token={self.api_token}"
            
            info(f"获取宏观指标: {country} - {indicator}", source="macro_indicators")
            
            # 使用会话发起请求
            session = self.create_robust_session()
            data = self.safe_api_request(url, session)
            
            if not data:
                self._log_fetch_attempt(country, indicator, 'failed', 0, '获取数据失败', time.time() - start_time)
                return []
            
            # 处理数据
            processed_data = self._process_macro_data(data, country, indicator)
            
            # 保存到数据库
            saved_count = self._save_macro_data(processed_data, country, indicator)
            
            self._log_fetch_attempt(country, indicator, 'success', saved_count, None, time.time() - start_time)
            
            return processed_data
            
        except Exception as e:
            error(f"获取宏观指标失败 {country}-{indicator}: {e}", source="macro_indicators")
            self._log_fetch_attempt(country, indicator, 'error', 0, str(e), time.time() - start_time)
            return []
    
    def _process_macro_data(self, raw_data: List[Dict], country: str, indicator: str) -> List[Dict[str, Any]]:
        """
        处理从API获取的原始宏观经济指标数据
        
        Args:
            raw_data: API返回的原始数据
            country: 国家代码
            indicator: 指标代码
            
        Returns:
            List[Dict]: 处理后的数据列表
        """
        processed_data = []
        
        try:
            for item in raw_data:
                # 处理日期，提取年份
                date_str = str(item.get('Date', ''))
                year = 0
                
                if date_str:
                    try:
                        # 如果是完整日期格式（如 2024-12-31），提取年份
                        if '-' in date_str:
                            year = int(date_str.split('-')[0])
                        else:
                            # 如果是年份格式，直接转换
                            year = int(date_str)
                    except (ValueError, IndexError):
                        warning(f"无法解析日期格式: {date_str}", source="macro_indicators")
                        year = 0
                
                processed_item = {
                    'country_code': country.upper(),
                    'country_name': self.COMMON_COUNTRIES.get(country.upper(), country),
                    'indicator': indicator,
                    'indicator_name': self.AVAILABLE_INDICATORS.get(indicator, indicator),
                    'year': year,
                    'value': float(item.get('Value', 0)) if item.get('Value') is not None else None,
                    'date_str': date_str,
                    'fetch_timestamp': datetime.now()
                }
                
                processed_data.append(processed_item)
            
            info(f"处理了 {len(processed_data)} 条宏观指标数据", source="macro_indicators")
            
        except Exception as e:
            error(f"处理宏观指标数据失败: {e}", source="macro_indicators")
        
        return processed_data
    
    def _save_macro_data(self, data: List[Dict], country: str, indicator: str) -> int:
        """
        保存宏观经济指标数据到PostgreSQL数据库（优化版批量操作）
        
        Args:
            data: 处理后的宏观经济指标数据
            country: 国家代码
            indicator: 指标代码
            
        Returns:
            int: 保存的记录数量
        """
        if not self.db_manager:
            warning("数据库连接不可用，跳过数据保存", source="macro_indicators")
            return 0
            
        if not data:
            return 0
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return 0
            
            try:
                # 开始事务
                conn.autocommit = False
                
                with conn.cursor() as cursor:
                    # 准备批量插入的数据
                    insert_data = []
                    for record in data:
                        insert_data.append((
                            record['country_code'],
                            record['country_name'],
                            record['indicator'],
                            record['indicator_name'],
                            record['year'],
                            record['value'],
                            record['date_str'],
                            record['fetch_timestamp']
                        ))
                    
                    # 使用批量 UPSERT 操作
                    sql = """
                        INSERT INTO macro_indicators 
                        (country_code, country_name, indicator, indicator_name, year, value, date_str, fetch_timestamp, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (country_code, indicator, year) 
                        DO UPDATE SET 
                            country_name = EXCLUDED.country_name,
                            indicator_name = EXCLUDED.indicator_name,
                            value = EXCLUDED.value,
                            date_str = EXCLUDED.date_str,
                            fetch_timestamp = EXCLUDED.fetch_timestamp,
                            updated_at = CURRENT_TIMESTAMP
                    """
                    
                    cursor.executemany(sql, insert_data)
                    saved_count = cursor.rowcount
                    
                    # 提交事务
                    conn.commit()
                    info(f"成功批量保存宏观指标数据 {saved_count} 条", source="macro_indicators")
                    return saved_count
                    
            except Exception as e:
                # 回滚事务
                conn.rollback()
                raise e
            finally:
                conn.close()
            
        except Exception as e:
            error(f"批量保存宏观指标数据失败: {e}", source="macro_indicators")
            return 0
    
    def batch_save_macro_data(self, data_dict: Dict[str, List[Dict]], country: str) -> Dict[str, int]:
        """
        批量保存多个指标的宏观经济数据（最高性能版本）
        
        Args:
            data_dict: 按指标分组的数据字典 {indicator: [data_records]}
            country: 国家代码
            
        Returns:
            Dict[str, int]: 每个指标保存的记录数 {indicator: count}
        """
        if not self.db_manager:
            warning("数据库连接不可用，跳过数据保存", source="macro_indicators")
            return {}
            
        if not data_dict:
            return {}
            
        results = {}
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return {}
            
            try:
                # 开始事务
                conn.autocommit = False
                
                with conn.cursor() as cursor:
                    # 合并所有指标的数据进行一次性批量插入
                    all_insert_data = []
                    indicator_counts = {}
                    
                    for indicator, records in data_dict.items():
                        indicator_counts[indicator] = len(records)
                        for record in records:
                            all_insert_data.append((
                                record['country_code'],
                                record['country_name'],
                                record['indicator'],
                                record['indicator_name'],
                                record['year'],
                                record['value'],
                                record['date_str'],
                                record['fetch_timestamp']
                            ))
                    
                    if all_insert_data:
                        # 使用单次批量 UPSERT 操作处理所有数据
                        sql = """
                            INSERT INTO macro_indicators 
                            (country_code, country_name, indicator, indicator_name, year, value, date_str, fetch_timestamp, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                            ON CONFLICT (country_code, indicator, year) 
                            DO UPDATE SET 
                                country_name = EXCLUDED.country_name,
                                indicator_name = EXCLUDED.indicator_name,
                                value = EXCLUDED.value,
                                date_str = EXCLUDED.date_str,
                                fetch_timestamp = EXCLUDED.fetch_timestamp,
                                updated_at = CURRENT_TIMESTAMP
                        """
                        
                        cursor.executemany(sql, all_insert_data)
                        total_saved = cursor.rowcount
                        
                        # 按比例分配每个指标的保存数量
                        total_input = sum(indicator_counts.values())
                        for indicator, input_count in indicator_counts.items():
                            proportion = input_count / total_input if total_input > 0 else 0
                            results[indicator] = int(total_saved * proportion)
                    
                    # 提交事务
                    conn.commit()
                    info(f"批量保存{country}宏观指标数据完成：{results}", source="macro_indicators")
                    return results
                    
            except Exception as e:
                # 回滚事务
                conn.rollback()
                raise e
            finally:
                conn.close()
            
        except Exception as e:
            error(f"批量保存{country}宏观指标数据失败: {e}", source="macro_indicators")
            return {}
    
    def _log_fetch_attempt(self, country: str, indicator: str, status: str, 
                          records_count: int, error_message: Optional[str], 
                          response_time: float):
        """记录API获取尝试到PostgreSQL数据库"""
        if not self.db_manager:
            return
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return
                
            with conn.cursor() as cursor:
                sql = """
                    INSERT INTO macro_indicators_fetch_logs 
                    (country_code, indicator, fetch_time, status, records_count, error_message, api_response_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                
                cursor.execute(sql, (
                    country, indicator, datetime.now(), status, records_count, error_message, response_time
                ))
                
                conn.commit()
            
        except Exception as e:
            error(f"记录获取日志失败: {e}", source="macro_indicators")
        finally:
            if conn:
                conn.close()
    
    def get_all_indicators_for_country(
        self,
        country: str,
        progress_callback: Optional[callable] = None
    ) -> List[Dict[str, Any]]:
        """
        获取指定国家的所有宏观经济指标数据（优化版批量保存）
        
        Args:
            country: 国家代码
            progress_callback: 进度回调函数
            
        Returns:
            List[Dict]: 所有指标数据的合并列表
        """
        all_data = []
        data_by_indicator = {}  # 按指标分组收集数据
        indicators = list(self.AVAILABLE_INDICATORS.keys())
        total_indicators = len(indicators)
        
        start_time = time.time()
        successful_count = 0
        failed_count = 0
        
        info(f"开始获取 {country} 的所有 {total_indicators} 个指标数据", source="macro_indicators")
        
        # 第一阶段：收集所有API数据，不保存到数据库
        for i, indicator in enumerate(indicators):
            if progress_callback:
                progress_callback(i, total_indicators, f"获取 {indicator} 数据... ({i+1}/{total_indicators})")
            
            try:
                # 获取单个指标的原始数据（不保存到数据库）
                indicator_data = self._fetch_single_indicator(country, indicator)
                if indicator_data:
                    all_data.extend(indicator_data)
                    data_by_indicator[indicator] = indicator_data
                    successful_count += 1
                else:
                    failed_count += 1
                    
                # 避免API请求过于频繁
                time.sleep(0.1)
                
            except Exception as e:
                error(f"获取 {country} {indicator} 数据失败: {e}", source="macro_indicators")
                failed_count += 1
                continue
        
        # 第二阶段：批量保存所有数据到数据库
        if data_by_indicator:
            info(f"开始批量保存 {country} 的 {len(data_by_indicator)} 个指标数据", source="macro_indicators")
            saved_results = self.batch_save_macro_data(data_by_indicator, country)
            info(f"批量保存完成，结果: {saved_results}", source="macro_indicators")
        
        total_time = time.time() - start_time
        
        # 记录总体获取日志
        self._log_fetch_attempt(
            country, 
            'all_indicators', 
            'success' if successful_count > 0 else 'failed',
            len(all_data),
            f"成功: {successful_count}, 失败: {failed_count}" if failed_count > 0 else None,
            total_time
        )
        
        info(f"完成获取 {country} 所有指标数据: 成功 {successful_count} 个, 失败 {failed_count} 个, 总数据 {len(all_data)} 条", source="macro_indicators")
        
        if progress_callback:
            progress_callback(total_indicators, total_indicators, f"完成! 获取到 {len(all_data)} 条数据")
        
        return all_data
    
    def _fetch_single_indicator(
        self,
        country: str,
        indicator: str,
        fmt: str = 'json'
    ) -> List[Dict[str, Any]]:
        """
        获取单个指标数据但不保存到数据库（用于批量操作）
        
        Args:
            country: 国家代码
            indicator: 指标代码
            fmt: 返回格式
            
        Returns:
            List[Dict]: 处理后的指标数据
        """
        try:
            # 构建请求URL
            url = f"{self.base_url}/macro-indicator/{country}?indicator={indicator}&fmt={fmt}&api_token={self.api_token}"
            
            info(f"获取宏观指标: {country} - {indicator}", source="macro_indicators")
            
            # 使用会话发起请求
            session = self.create_robust_session()
            data = self.safe_api_request(url, session)
            
            if not data:
                return []
            
            # 处理数据但不保存到数据库
            processed_data = self._process_macro_data(data, country, indicator)
            return processed_data
            
        except Exception as e:
            error(f"获取宏观指标失败 {country}-{indicator}: {e}", source="macro_indicators")
            return []
    
    def get_stored_data(
        self,
        country: str = None,
        indicator: str = None,
        year_from: int = None,
        year_to: int = None,
        limit: int = 1000
    ) -> List[Dict[str, Any]]:
        """
        从PostgreSQL数据库获取存储的宏观经济指标数据
        
        Args:
            country: 国家代码，为None时获取所有国家
            indicator: 指标代码，为None时获取所有指标
            year_from: 起始年份
            year_to: 结束年份
            limit: 返回记录数限制
            
        Returns:
            List[Dict]: 宏观经济指标数据列表
        """
        if not self.db_manager:
            warning("数据库连接不可用", source="macro_indicators")
            return []
            
        try:
            # 构建查询条件
            conditions = []
            params = []
            
            if country:
                conditions.append("country_code = %s")
                params.append(country)
            
            if indicator:
                conditions.append("indicator = %s")
                params.append(indicator)
            
            if year_from:
                conditions.append("year >= %s")
                params.append(year_from)
            
            if year_to:
                conditions.append("year <= %s")
                params.append(year_to)
            
            where_clause = ""
            if conditions:
                where_clause = "WHERE " + " AND ".join(conditions)
            
            query = f"""
                SELECT * FROM macro_indicators 
                {where_clause}
                ORDER BY country_code, indicator, year DESC
                LIMIT %s
            """
            params.append(limit)
            
            conn = self.db_manager.get_connection()
            if not conn:
                return []
            
            try:
                from psycopg2.extras import RealDictCursor
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute(query, tuple(params))
                    results = cursor.fetchall()
                    
                    # 转换为字典列表
                    data = []
                    for row in results:
                        data.append(dict(row))
                    
                    info(f"从数据库获取到 {len(data)} 条宏观指标数据", source="macro_indicators")
                    return data
            finally:
                conn.close()
            
        except Exception as e:
            error(f"获取存储数据失败: {e}", source="macro_indicators")
            return []
    
    def get_available_countries(self) -> List[Dict[str, str]]:
        """
        获取PostgreSQL数据库中可用的国家列表
        
        Returns:
            List[Dict]: 国家列表，包含代码和名称
        """
        if not self.db_manager:
            # 如果数据库不可用，返回常用国家列表
            return [{"code": code, "name": name} for code, name in self.COMMON_COUNTRIES.items()]
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return [{"code": code, "name": name} for code, name in self.COMMON_COUNTRIES.items()]
            
            try:
                from psycopg2.extras import RealDictCursor
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT DISTINCT country_code, country_name 
                        FROM macro_indicators 
                        ORDER BY country_name
                    """
                    
                    cursor.execute(query)
                    results = cursor.fetchall()
                    
                    return [{"code": row["country_code"], "name": row["country_name"]} for row in results]
            finally:
                conn.close()
            
        except Exception as e:
            error(f"获取可用国家列表失败: {e}", source="macro_indicators")
            return [{"code": code, "name": name} for code, name in self.COMMON_COUNTRIES.items()]
    
    def get_available_indicators_for_country(self, country: str) -> List[Dict[str, str]]:
        """
        获取指定国家可用的指标列表（PostgreSQL）
        
        Args:
            country: 国家代码
            
        Returns:
            List[Dict]: 指标列表，包含代码和名称
        """
        if not self.db_manager:
            warning("数据库连接不可用", source="macro_indicators")
            return []
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return []
            
            try:
                from psycopg2.extras import RealDictCursor
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT DISTINCT indicator, indicator_name, 
                               COUNT(*) as data_points,
                               MIN(year) as earliest_year,
                               MAX(year) as latest_year
                        FROM macro_indicators 
                        WHERE country_code = %s
                        GROUP BY indicator, indicator_name
                        ORDER BY indicator_name
                    """
                    
                    cursor.execute(query, (country,))
                    results = cursor.fetchall()
                    
                    return [dict(row) for row in results]
            finally:
                conn.close()
            
        except Exception as e:
            error(f"获取国家 {country} 可用指标列表失败: {e}", source="macro_indicators")
            return []

    def list_all_indicators(self) -> Dict[str, str]:
        """返回所有可用的指标列表"""
        return self.AVAILABLE_INDICATORS

    def list_common_countries(self) -> Dict[str, str]:
        """返回常用国家列表"""
        return self.COMMON_COUNTRIES
