"""
图表事件管理服务
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from backend.utils.logger import get_logger, info, error, warning

logger = get_logger("chart_events_service")


class ChartEventsService:
    """图表事件管理服务类"""

    def __init__(self, db_manager):
        self.db_manager = db_manager

    def create_event(self, user_id: int, event_data: Dict[str, Any]) -> int:
        """
        创建新的图表事件
        
        Args:
            user_id: 用户ID
            event_data: 事件数据
        
        Returns:
            新创建事件的ID
        """
        if not self.db_manager:
            warning("数据库连接不可用", source="chart_events")
            return 0

        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return 0

            with conn.cursor() as cursor:
                insert_sql = """
                    INSERT INTO chart_events (
                        user_id, symbol, event_date, event_type, title, 
                        description, position, related_url, related_file_id
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """
                
                cursor.execute(insert_sql, (
                    user_id,
                    event_data.get('symbol'),
                    event_data.get('event_date'),
                    event_data.get('event_type'),
                    event_data.get('title'),
                    event_data.get('description'),
                    event_data.get('position', 'middle'),
                    event_data.get('related_url'),
                    event_data.get('related_file_id')
                ))
                
                result = cursor.fetchone()
                conn.commit()
                
                event_id = result[0] if result else 0
                info(f"创建图表事件成功: user_id={user_id}, event_id={event_id}, symbol={event_data.get('symbol')}", source="chart_events")
                return event_id

        except Exception as e:
            error(f"创建图表事件失败: {str(e)}", source="chart_events")
            return 0

    def get_events_by_symbol(self, user_id: int, symbol: str) -> List[Dict[str, Any]]:
        """
        获取指定股票的所有事件
        
        Args:
            user_id: 用户ID
            symbol: 股票代码（可能是primaryTicker或原始symbol）
            
        Returns:
            事件列表
        """
        if not self.db_manager:
            warning("数据库连接不可用", source="chart_events")
            return []

        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return []

            with conn.cursor() as cursor:
                # 生成可能的symbol变体，确保向后兼容
                symbol_variants = self._get_symbol_variants(symbol)
                
                # 使用IN查询来匹配所有可能的symbol变体
                placeholders = ','.join(['%s'] * len(symbol_variants))
                select_sql = f"""
                    SELECT id, symbol, event_date, event_type, title, description, 
                           position, related_url, related_file_id, created_at, updated_at
                    FROM chart_events 
                    WHERE user_id = %s AND symbol IN ({placeholders})
                    ORDER BY event_date ASC, created_at ASC
                """
                
                cursor.execute(select_sql, [user_id] + symbol_variants)
                rows = cursor.fetchall()
                
                events = []
                for row in rows:
                    events.append({
                        'id': str(row[0]),  # 转换为字符串以匹配前端
                        'symbol': row[1],
                        'date': row[2].strftime('%Y-%m-%d') if row[2] else None,
                        'type': row[3],
                        'title': row[4],
                        'description': row[5],
                        'position': row[6],
                        'relatedUrl': row[7],
                        'relatedFileId': row[8],
                        'createdAt': row[9].isoformat() if row[9] else None,
                        'updatedAt': row[10].isoformat() if row[10] else None
                    })
                
                info(f"获取图表事件: user_id={user_id}, symbol={symbol}, variants={symbol_variants}, count={len(events)}", source="chart_events")
                return events

        except Exception as e:
            error(f"获取图表事件失败: {str(e)}", source="chart_events")
            return []

    def _get_symbol_variants(self, symbol: str) -> List[str]:
        """
        生成可能的symbol变体，确保向后兼容
        
        Args:
            symbol: 输入的symbol（可能是primaryTicker或原始symbol）
            
        Returns:
            可能的symbol变体列表
        """
        if not symbol:
            return []
        
        variants = [symbol]  # 原始symbol
        
        # 如果symbol包含交易所后缀（如.US, .HK等），也添加不带后缀的版本
        if '.' in symbol:
            base_symbol = symbol.split('.')[0]
            variants.append(base_symbol)
        
        # 如果symbol不包含交易所后缀，添加常见的交易所后缀
        else:
            # 添加常见的交易所后缀
            exchanges = ['.US', '.HK', '.SZ', '.SH', '.TW', '.JP', '.NASDAQ', '.NYSE']
            for exchange in exchanges:
                variants.append(f"{symbol}{exchange}")
        
        # 去重并保持顺序
        seen = set()
        unique_variants = []
        for variant in variants:
            if variant not in seen:
                seen.add(variant)
                unique_variants.append(variant)
        
        return unique_variants

    def update_event(self, user_id: int, event_id: int, event_data: Dict[str, Any]) -> bool:
        """
        更新图表事件
        
        Args:
            user_id: 用户ID
            event_id: 事件ID
            event_data: 更新的事件数据
            
        Returns:
            是否更新成功
        """
        if not self.db_manager:
            warning("数据库连接不可用", source="chart_events")
            return False

        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return False

            with conn.cursor() as cursor:
                # 检查事件是否属于当前用户
                check_sql = "SELECT id FROM chart_events WHERE id = %s AND user_id = %s"
                cursor.execute(check_sql, (event_id, user_id))
                
                if not cursor.fetchone():
                    warning(f"事件不存在或无权限: event_id={event_id}, user_id={user_id}", source="chart_events")
                    return False
                
                update_sql = """
                    UPDATE chart_events 
                    SET event_date = %s, event_type = %s, title = %s, description = %s,
                        position = %s, related_url = %s, related_file_id = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s AND user_id = %s
                """
                
                cursor.execute(update_sql, (
                    event_data.get('event_date'),
                    event_data.get('event_type'),
                    event_data.get('title'),
                    event_data.get('description'),
                    event_data.get('position', 'middle'),
                    event_data.get('related_url'),
                    event_data.get('related_file_id'),
                    event_id,
                    user_id
                ))
                
                conn.commit()
                info(f"更新图表事件成功: user_id={user_id}, event_id={event_id}", source="chart_events")
                return True

        except Exception as e:
            error(f"更新图表事件失败: {str(e)}", source="chart_events")
            return False

    def delete_event(self, user_id: int, event_id: int) -> bool:
        """
        删除图表事件
        
        Args:
            user_id: 用户ID
            event_id: 事件ID
            
        Returns:
            是否删除成功
        """
        if not self.db_manager:
            warning("数据库连接不可用", source="chart_events")
            return False

        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return False

            with conn.cursor() as cursor:
                delete_sql = "DELETE FROM chart_events WHERE id = %s AND user_id = %s"
                cursor.execute(delete_sql, (event_id, user_id))
                
                deleted_count = cursor.rowcount
                conn.commit()
                
                if deleted_count > 0:
                    info(f"删除图表事件成功: user_id={user_id}, event_id={event_id}", source="chart_events")
                    return True
                else:
                    warning(f"事件不存在或无权限: event_id={event_id}, user_id={user_id}", source="chart_events")
                    return False

        except Exception as e:
            error(f"删除图表事件失败: {str(e)}", source="chart_events")
            return False

    def delete_events_by_symbol(self, user_id: int, symbol: str) -> int:
        """
        删除指定用户的指定股票的所有事件（用于级联删除）
        
        Args:
            user_id: 用户ID
            symbol: 股票代码
            
        Returns:
            删除的事件数量
        """
        if not self.db_manager:
            warning("数据库连接不可用", source="chart_events")
            return 0

        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return 0

            with conn.cursor() as cursor:
                # 生成可能的symbol变体，确保删除所有相关事件
                symbol_variants = self._get_symbol_variants(symbol)
                
                # 使用IN查询来删除所有可能的symbol变体
                placeholders = ','.join(['%s'] * len(symbol_variants))
                delete_sql = f"""
                    DELETE FROM chart_events 
                    WHERE user_id = %s AND symbol IN ({placeholders})
                """
                
                cursor.execute(delete_sql, [user_id] + symbol_variants)
                deleted_count = cursor.rowcount
                conn.commit()
                
                if deleted_count > 0:
                    info(f"级联删除图表事件: user_id={user_id}, symbol={symbol}, variants={symbol_variants}, deleted={deleted_count}", source="chart_events")
                else:
                    info(f"没有需要删除的图表事件: user_id={user_id}, symbol={symbol}", source="chart_events")
                
                return deleted_count

        except Exception as e:
            error(f"删除股票事件失败: symbol={symbol}, error={str(e)}", source="chart_events")
            return 0

    def get_user_events_summary(self, user_id: int) -> Dict[str, Any]:
        """
        获取用户事件统计摘要
        
        Args:
            user_id: 用户ID
            
        Returns:
            统计信息
        """
        if not self.db_manager:
            warning("数据库连接不可用", source="chart_events")
            return {}

        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return {}

            with conn.cursor() as cursor:
                # 总事件数
                cursor.execute("SELECT COUNT(*) FROM chart_events WHERE user_id = %s", (user_id,))
                total_events = cursor.fetchone()[0]
                
                # 按类型统计
                cursor.execute("""
                    SELECT event_type, COUNT(*) 
                    FROM chart_events 
                    WHERE user_id = %s 
                    GROUP BY event_type
                """, (user_id,))
                type_stats = dict(cursor.fetchall())
                
                # 按股票统计
                cursor.execute("""
                    SELECT symbol, COUNT(*) 
                    FROM chart_events 
                    WHERE user_id = %s 
                    GROUP BY symbol 
                    ORDER BY COUNT(*) DESC 
                    LIMIT 10
                """, (user_id,))
                symbol_stats = dict(cursor.fetchall())
                
                return {
                    'total_events': total_events,
                    'by_type': type_stats,
                    'by_symbol': symbol_stats
                }

        except Exception as e:
            error(f"获取事件统计失败: {str(e)}", source="chart_events")
            return {}

    def export_user_events(self, user_id: int) -> List[Dict[str, Any]]:
        """
        导出用户的所有事件
        
        Args:
            user_id: 用户ID
            
        Returns:
            所有事件数据
        """
        if not self.db_manager:
            warning("数据库连接不可用", source="chart_events")
            return []

        try:
            conn = self.db_manager.get_connection()
            if not conn:
                return []

            with conn.cursor() as cursor:
                select_sql = """
                    SELECT id, symbol, event_date, event_type, title, description, 
                           position, related_url, related_file_id, created_at, updated_at
                    FROM chart_events 
                    WHERE user_id = %s
                    ORDER BY symbol, event_date ASC
                """
                
                cursor.execute(select_sql, (user_id,))
                rows = cursor.fetchall()
                
                events = []
                for row in rows:
                    events.append({
                        'id': str(row[0]),
                        'symbol': row[1],
                        'date': row[2].strftime('%Y-%m-%d') if row[2] else None,
                        'type': row[3],
                        'title': row[4],
                        'description': row[5],
                        'position': row[6],
                        'relatedUrl': row[7],
                        'relatedFileId': row[8],
                        'createdAt': row[9].isoformat() if row[9] else None,
                        'updatedAt': row[10].isoformat() if row[10] else None
                    })
                
                info(f"导出用户事件: user_id={user_id}, count={len(events)}", source="chart_events")
                return events

        except Exception as e:
            error(f"导出用户事件失败: {str(e)}", source="chart_events")
            return []


# 全局服务实例
_chart_events_service = None


def get_chart_events_service():
    """获取图表事件服务实例"""
    global _chart_events_service
    if _chart_events_service is None:
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        
        # 获取标准化数据库连接参数
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        _chart_events_service = ChartEventsService(db_manager)
    return _chart_events_service

