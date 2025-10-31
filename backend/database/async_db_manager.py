"""
异步数据库管理器包装器
将同步的DatabaseManager包装为异步接口，以便在异步代码中使用
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, List, Any, Dict
from backend.database.db_manager import DatabaseManager


class AsyncDatabaseManager:
    """异步数据库管理器包装器"""
    
    def __init__(self, db_conn_params: dict, max_workers: int = 10):
        """
        初始化异步数据库管理器
        
        Args:
            db_conn_params: 数据库连接参数
            max_workers: 线程池最大工作线程数
        """
        self.db_manager = DatabaseManager(db_conn_params)
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
    
    async def fetch_one(self, query: str, params=None) -> Optional[Dict]:
        """
        异步执行查询并获取一行结果
        
        Args:
            query: SQL查询语句（使用%s作为占位符）
            params: 查询参数（tuple或list）
        
        Returns:
            查询结果（字典格式）或None
        """
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor,
            self._fetch_one_sync,
            query,
            params
        )
        return result
    
    def _fetch_one_sync(self, query: str, params) -> Optional[Dict]:
        """同步执行查询（内部方法）"""
        # 使用带字段名的查询
        import psycopg2.extras
        conn = self.db_manager.get_connection()
        if not conn:
            return None
        
        try:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            result = cursor.fetchone()
            
            # 如果是修改语句，需要commit
            if query.strip().upper().startswith(('INSERT', 'UPDATE', 'DELETE')):
                conn.commit()
            
            conn.close()
            return dict(result) if result else None
        except Exception as e:
            print(f"查询失败: {e}")
            if conn:
                try:
                    conn.close()
                except:
                    pass
            return None
    
    async def fetch_all(self, query: str, params=None) -> List[Dict]:
        """
        异步执行查询并获取所有结果
        
        Args:
            query: SQL查询语句（使用%s作为占位符）
            params: 查询参数（tuple或list）
        
        Returns:
            查询结果列表（字典格式）
        """
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            self.executor,
            self._fetch_all_sync,
            query,
            params
        )
        return results
    
    def _fetch_all_sync(self, query: str, params) -> List[Dict]:
        """同步执行查询（内部方法）"""
        import psycopg2.extras
        conn = self.db_manager.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            results = cursor.fetchall()
            conn.close()
            return [dict(row) for row in results] if results else []
        except Exception as e:
            print(f"查询失败: {e}")
            if conn:
                try:
                    conn.close()
                except:
                    pass
            return []
    
    async def execute(self, query: str, params=None) -> str:
        """
        异步执行SQL语句（INSERT/UPDATE/DELETE）
        
        Args:
            query: SQL语句（使用%s作为占位符）
            params: 参数（tuple或list）
        
        Returns:
            执行结果信息
        """
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            self.executor,
            self._execute_sync,
            query,
            params
        )
        return result
    
    def _execute_sync(self, query: str, params) -> str:
        """同步执行SQL（内部方法）"""
        conn = self.db_manager.get_connection()
        if not conn:
            return "Connection failed"
        
        try:
            cursor = conn.cursor()
            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            conn.commit()
            rowcount = cursor.rowcount
            conn.close()
            return f"UPDATE {rowcount}"
        except Exception as e:
            if conn:
                try:
                    conn.rollback()
                    conn.close()
                except:
                    pass
            raise e
    
    def get_connection_context(self):
        """
        提供与同步DatabaseManager兼容的连接上下文管理器
        这是一个同步方法，用于与现有代码兼容
        """
        return self.db_manager.get_connection_context()
    
    def __del__(self):
        """清理线程池"""
        try:
            self.executor.shutdown(wait=False)
        except:
            pass

