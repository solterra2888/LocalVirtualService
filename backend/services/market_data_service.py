#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
市场数据管理服务

负责经济事件数据的数据库存储和管理
"""

import json
from datetime import datetime
from typing import List, Dict, Any, Optional

from backend.utils.logger import info, error, warning, get_logger
from backend.database.db_manager import DatabaseManager
from config.settings import config


class MarketDataService:
    """市场数据管理服务类"""
    
    def __init__(self):
        """初始化数据库连接"""
        self.logger = get_logger(__name__)
        
        # 初始化数据库连接
        self._init_database()
        
        info("市场数据服务初始化完成", source="market_data")
    
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
            info("市场数据数据库连接初始化完成", source="market_data")
        except Exception as e:
            error(f"数据库初始化失败: {e}", source="market_data")
            self.db_manager = None
    
    def verify_tables(self):
        """验证所需的表是否存在"""
        if not self.db_manager:
            warning("数据库连接不可用", source="market_data")
            return False
            
        required_tables = [
            'economic_events', 'macro_indicators', 'macro_indicators_fetch_logs'
        ]
        
        try:
            for table_name in required_tables:
                conn = self.db_manager.get_connection()
                if not conn:
                    return False
                
                try:
                    with conn.cursor() as cursor:
                        exists_sql = """
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_schema = 'public' 
                                AND table_name = %s
                            );
                        """
                        cursor.execute(exists_sql, (table_name,))
                        result = cursor.fetchone()
                        
                        if not result or not result[0]:
                            error(f"必需的表 '{table_name}' 不存在", source="market_data")
                            return False
                finally:
                    conn.close()
            
            info("所有必需的市场数据表都存在", source="market_data")
            return True
            
        except Exception as e:
            error(f"验证数据表失败: {e}", source="market_data")
            return False
    
    def insert_economic_event(self, event_data: Dict[str, Any]) -> int:
        """
        插入经济事件数据
        
        Args:
            event_data: 经济事件数据
        
        Returns:
            插入记录的ID
        """
        if not self.db_manager:
            warning("数据库连接不可用", source="market_data")
            return 0
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return 0
            
            try:
                with conn.cursor() as cursor:
                    # 检查是否已存在相同的事件（基于日期、时间、国家、事件名称）
                    check_sql = """
                        SELECT id FROM economic_events 
                        WHERE date = %s AND time = %s AND country = %s AND event = %s
                    """
                    cursor.execute(check_sql, (
                        event_data.get('date'),
                        event_data.get('time'),
                        event_data.get('country'),
                        event_data.get('event')
                    ))
                    
                    existing_record = cursor.fetchone()
                    
                    if existing_record:
                        # 记录已存在，返回0表示未插入新记录
                        return 0
                    
                    # 插入新记录
                    insert_sql = """
                        INSERT INTO economic_events (
                            date, time, country, event, period, actual, previous, 
                            forecast, calculated_change, calculated_change_pct, impact
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """
                    
                    cursor.execute(insert_sql, (
                        event_data.get('date'),
                        event_data.get('time'),
                        event_data.get('country'),
                        event_data.get('event'),
                        event_data.get('period'),
                        event_data.get('actual'),
                        event_data.get('previous'),
                        event_data.get('forecast'),
                        event_data.get('calculated_change'),
                        event_data.get('calculated_change_pct'),
                        event_data.get('impact', 1)  # 默认1星
                    ))
                    
                    event_id = cursor.fetchone()[0]
                    conn.commit()
                    
                    info(f"插入经济事件数据成功: ID={event_id}", source="market_data")
                    return event_id
                    
            finally:
                conn.close()
                
        except Exception as e:
            error(f"插入经济事件数据失败: {str(e)}", source="market_data")
            return 0
    
    def update_economic_event(self, event_data: Dict[str, Any]) -> bool:
        """
        更新经济事件数据
        
        Args:
            event_data: 经济事件数据
        
        Returns:
            是否更新成功
        """
        if not self.db_manager:
            warning("数据库连接不可用", source="market_data")
            return False
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return False
            
            try:
                with conn.cursor() as cursor:
                    # 更新记录
                    update_sql = """
                        UPDATE economic_events 
                        SET period = %s, actual = %s, previous = %s, 
                            forecast = %s, calculated_change = %s, 
                            calculated_change_pct = %s, impact = %s
                        WHERE date = %s AND time = %s AND country = %s AND event = %s
                    """
                    
                    cursor.execute(update_sql, (
                        event_data.get('period'),
                        event_data.get('actual'),
                        event_data.get('previous'),
                        event_data.get('forecast'),
                        event_data.get('calculated_change'),
                        event_data.get('calculated_change_pct'),
                        event_data.get('impact', 1),
                        event_data.get('date'),
                        event_data.get('time'),
                        event_data.get('country'),
                        event_data.get('event')
                    ))
                    
                    updated_rows = cursor.rowcount
                    conn.commit()
                    
                    if updated_rows > 0:
                        info(f"更新经济事件数据成功: {updated_rows} 条记录", source="market_data")
                        return True
                    else:
                        return False
                    
            finally:
                conn.close()
                
        except Exception as e:
            error(f"更新经济事件数据失败: {str(e)}", source="market_data")
            return False
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return 0
            
            try:
                with conn.cursor() as cursor:
                    # 准备数据
                    sql = """
                        INSERT INTO economic_events 
                        (date, time, country, event, period, actual, previous, forecast, 
                         change_val, change_percentage, raw_data)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """
                    
                    cursor.execute(sql, (
                        event_data.get('date', ''),
                        event_data.get('time', ''),
                        event_data.get('country', ''),
                        event_data.get('event', ''),
                        event_data.get('period', ''),
                        event_data.get('actual', ''),
                        event_data.get('previous', ''),
                        event_data.get('forecast', ''),
                        event_data.get('calculated_change'),
                        event_data.get('calculated_change_pct'),
                        json.dumps(event_data, ensure_ascii=False)
                    ))
                    
                    result = cursor.fetchone()
                    event_id = result[0] if result else 0
                    
                    conn.commit()
                    
                    info(f"插入经济事件数据成功，ID: {event_id}", source="market_data")
                    return event_id
            finally:
                conn.close()
            
        except Exception as e:
            error(f"插入经济事件数据失败: {str(e)}", source="market_data")
            return 0
    
    def batch_insert_economic_events(self, events_data: List[Dict[str, Any]]) -> int:
        """
        批量插入/更新经济事件数据（使用UPSERT）
        
        Args:
            events_data: 经济事件数据列表
        
        Returns:
            处理的记录数
        """
        if not events_data:
            return 0
        
        if not self.db_manager:
            warning("数据库连接不可用", source="market_data")
            return 0
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return 0
            
            try:
                # 开始事务
                conn.autocommit = False
                
                with conn.cursor() as cursor:
                    # 准备批量UPSERT的数据
                    insert_data = []
                    for event_data in events_data:
                        insert_data.append((
                            event_data.get('date', ''),
                            event_data.get('time', ''),
                            event_data.get('country', ''),
                            event_data.get('event', ''),
                            event_data.get('period', ''),
                            event_data.get('actual', ''),
                            event_data.get('previous', ''),
                            event_data.get('forecast', ''),
                            event_data.get('calculated_change'),
                            event_data.get('calculated_change_pct'),
                            json.dumps(event_data, ensure_ascii=False)
                        ))
                    
                    # 使用批量 UPSERT 操作
                    sql = """
                        INSERT INTO economic_events 
                        (date, time, country, event, period, actual, previous, forecast, 
                         change_val, change_percentage, raw_data, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (date, event, country) 
                        DO UPDATE SET 
                            time = EXCLUDED.time,
                            period = EXCLUDED.period,
                            actual = EXCLUDED.actual,
                            previous = EXCLUDED.previous,
                            forecast = EXCLUDED.forecast,
                            change_val = EXCLUDED.change_val,
                            change_percentage = EXCLUDED.change_percentage,
                            raw_data = EXCLUDED.raw_data,
                            updated_at = CURRENT_TIMESTAMP
                    """
                    
                    cursor.executemany(sql, insert_data)
                    processed_count = cursor.rowcount
                    
                    # 提交事务
                    conn.commit()
                    info(f"批量UPSERT完成，处理 {processed_count} 条记录", source="market_data")
                    return processed_count
                    
            except Exception as e:
                # 回滚事务
                conn.rollback()
                raise e
            finally:
                conn.close()
            
        except Exception as e:
            error(f"批量UPSERT经济事件数据失败: {str(e)}", source="market_data")
            return 0
    
    def event_exists(self, event_data: Dict[str, Any]) -> bool:
        """
        检查经济事件是否已存在
        注意：使用UPSERT后，此方法主要用于兼容性，实际批量操作不再需要
        
        Args:
            event_data: 经济事件数据
        
        Returns:
            是否已存在
        """
        if not self.db_manager:
            return False
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return False
            
            try:
                with conn.cursor() as cursor:
                    sql = """
                        SELECT EXISTS (
                            SELECT 1 FROM economic_events 
                            WHERE date = %s AND event = %s AND country = %s
                        )
                    """
                    
                    cursor.execute(sql, (
                        event_data.get('date', ''),
                        event_data.get('event', ''),
                        event_data.get('country', '')
                    ))
                    
                    result = cursor.fetchone()
                    return result[0] if result else False
            finally:
                conn.close()
            
        except Exception as e:
            error(f"检查事件存在性失败: {str(e)}", source="market_data")
            return False
    
    def get_economic_events(
        self,
        from_date: str = None,
        to_date: str = None,
        country: str = None,
        event_filter: str = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        查询经济事件数据
        
        Args:
            from_date: 开始日期
            to_date: 结束日期
            country: 国家代码
            event_filter: 事件名称过滤
            limit: 限制数量
            offset: 偏移量
        
        Returns:
            经济事件数据列表
        """
        if not self.db_manager:
            warning("数据库连接不可用", source="market_data")
            return []
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return []
            
            try:
                from psycopg2.extras import RealDictCursor
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # 构建查询条件
                    conditions = []
                    params = []
                    
                    if from_date:
                        conditions.append("date::date >= %s::date")
                        params.append(from_date)
                    
                    if to_date:
                        conditions.append("date::date <= %s::date")
                        params.append(to_date)
                    
                    if country:
                        # 支持多国家查询（逗号分隔的国家代码）
                        if ',' in country:
                            # 多个国家，使用 IN 查询
                            country_codes = [c.strip() for c in country.split(',') if c.strip()]
                            if country_codes:
                                placeholders = ','.join(['%s'] * len(country_codes))
                                conditions.append(f"country IN ({placeholders})")
                                params.extend(country_codes)
                        else:
                            # 单个国家
                            conditions.append("country = %s")
                            params.append(country)
                    
                    if event_filter:
                        conditions.append("event LIKE %s")
                        params.append(f"%{event_filter}%")
                    
                    # 构建查询语句
                    where_clause = ""
                    if conditions:
                        where_clause = "WHERE " + " AND ".join(conditions)
                    
                    query = f'''
                        SELECT * FROM economic_events 
                        {where_clause}
                        ORDER BY date::date DESC, time DESC
                        LIMIT %s OFFSET %s
                    '''
                    
                    params.extend([limit, offset])
                    
                    cursor.execute(query, params)
                    rows = cursor.fetchall()
                    
                    # 转换为字典列表
                    events = []
                    for row in rows:
                        event = dict(row)
                        # 解析raw_data
                        if event.get('raw_data'):
                            try:
                                raw_data = json.loads(event['raw_data'])
                                event.update(raw_data)
                            except json.JSONDecodeError:
                                pass
                        events.append(event)
                    
                    info(f"查询到 {len(events)} 条经济事件数据", source="market_data")
                    return events
            finally:
                conn.close()
            
        except Exception as e:
            error(f"查询经济事件数据失败: {str(e)}", source="market_data")
            return []
    
    def get_countries_list(self) -> List[str]:
        """
        获取数据库中的国家列表
        
        Returns:
            国家代码列表
        """
        if not self.db_manager:
            return []
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return []
            
            try:
                from psycopg2.extras import RealDictCursor
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = '''
                        SELECT DISTINCT country FROM economic_events 
                        WHERE country IS NOT NULL AND country != ''
                        ORDER BY country
                    '''
                    
                    cursor.execute(query)
                    rows = cursor.fetchall()
                    countries = [row['country'] for row in rows]
                    return countries
            finally:
                conn.close()
            
        except Exception as e:
            error(f"获取国家列表失败: {str(e)}", source="market_data")
            return []
    
    def get_events_statistics(self) -> Dict[str, Any]:
        """
        获取经济事件统计信息
        
        Returns:
            统计信息字典
        """
        if not self.db_manager:
            return {}
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return {}
            
            try:
                # 使用RealDictCursor来获取字典格式结果
                from psycopg2.extras import RealDictCursor
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    
                    # 总记录数
                    cursor.execute("SELECT COUNT(*) as count FROM economic_events")
                    total_result = cursor.fetchone()
                    total_events = total_result['count'] if total_result else 0
                    
                    # 按国家统计（前10个国家用于显示，同时获取总数）
                    cursor.execute('''
                        SELECT country, COUNT(*) as count 
                        FROM economic_events 
                        WHERE country IS NOT NULL AND country != ''
                        GROUP BY country 
                        ORDER BY count DESC 
                        LIMIT 10
                    ''')
                    country_rows = cursor.fetchall()
                    country_stats = {row['country']: row['count'] for row in country_rows}
                    
                    # 获取总的国家数量
                    cursor.execute('''
                        SELECT COUNT(DISTINCT country) as total_countries
                        FROM economic_events 
                        WHERE country IS NOT NULL AND country != ''
                    ''')
                    total_countries_result = cursor.fetchone()
                    total_countries = total_countries_result['total_countries'] if total_countries_result else 0
                    
                    # 按日期统计（最近30天）
                    cursor.execute('''
                        SELECT date, COUNT(*) as count 
                        FROM economic_events 
                        WHERE date::date >= CURRENT_DATE - INTERVAL '30 days'
                        GROUP BY date 
                        ORDER BY date::date DESC 
                        LIMIT 30
                    ''')
                    date_rows = cursor.fetchall()
                    date_stats = {row['date']: row['count'] for row in date_rows}
                    
                    # 最新数据时间
                    cursor.execute("SELECT MAX(date::date) as latest_date FROM economic_events")
                    latest_result = cursor.fetchone()
                    latest_date = str(latest_result['latest_date']) if latest_result and latest_result['latest_date'] else None
                    
                    statistics = {
                        'total_events': total_events,
                        'country_stats': country_stats,
                        'date_stats': date_stats,
                        'latest_date': latest_date,
                        'total_countries': total_countries
                    }
                    
                    return statistics
            finally:
                conn.close()
            
        except Exception as e:
            error(f"获取统计信息失败: {str(e)}", source="market_data")
            # 返回默认统计数据以防止API验证错误
            return {
                'total_events': 0,
                'country_stats': {},
                'date_stats': {},
                'latest_date': None,
                'total_countries': 0
            }
    
    def delete_old_events(self, days: int = 365) -> int:
        """
        删除指定天数之前的旧事件数据
        
        Args:
            days: 保留天数
        
        Returns:
            删除的记录数
        """
        if not self.db_manager:
            return 0
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return 0
            
            try:
                with conn.cursor() as cursor:
                    delete_sql = '''
                        DELETE FROM economic_events 
                        WHERE date::date < CURRENT_DATE - INTERVAL '%s days'
                    '''
                    
                    cursor.execute(delete_sql, (days,))
                    deleted_count = cursor.rowcount
                    
                    conn.commit()
                    
                    info(f"删除了 {deleted_count} 条旧经济事件数据", source="market_data")
                    return deleted_count
            finally:
                conn.close()
            
        except Exception as e:
            error(f"删除旧数据失败: {str(e)}", source="market_data")
            return 0
    
    def get_available_countries(self) -> List[Dict[str, str]]:
        """
        获取经济事件数据库中可用的国家列表
        
        Returns:
            List[Dict]: 国家列表，包含代码和名称
        """
        if not self.db_manager:
            return []
            
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return []
            
            try:
                from psycopg2.extras import RealDictCursor
                from backend.utils.country_mapping import get_country_name
                
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    query = """
                        SELECT DISTINCT country as code
                        FROM economic_events 
                        WHERE country IS NOT NULL AND country != ''
                        ORDER BY country
                    """
                    
                    cursor.execute(query)
                    results = cursor.fetchall()
                    
                    # 使用映射获取国家名称
                    countries = []
                    for row in results:
                        code = row["code"]
                        name = get_country_name(code)
                        countries.append({"code": code, "name": name})
                    
                    return countries
            finally:
                conn.close()
            
        except Exception as e:
            error(f"获取经济事件国家列表失败: {e}", source="market_data")
            return []
