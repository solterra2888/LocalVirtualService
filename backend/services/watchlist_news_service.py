# -*- coding: utf-8 -*-
"""
Watchlist News服务 - 基于用户watchlist获取新闻
"""

import os
import requests
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.services.eodhd_news_service import EODHDNewsService
from config.settings import config

# 获取日志实例
logger = get_logger("watchlist_news_service")


class WatchlistNewsService:
    """基于用户Watchlist的新闻获取服务"""
    
    def __init__(self):
        """初始化Watchlist新闻服务"""
        logger.info("开始初始化Watchlist新闻服务")
        
        try:
            # 数据库连接配置
            logger.info("准备数据库连接配置")
            db_config = {
                'db_host': config.get('DB_HOST', 'localhost'),
                'db_port': config.get('DB_PORT', '5432'),
                'db_name': config.get('DB_NAME', 'targettracking'),
                'db_user': config.get('DB_USER', 'postgres'),
                'db_password': config.get('DB_PASSWORD', '')
            }
            logger.info("数据库配置准备完成", 
                        host=db_config['db_host'], 
                        port=db_config['db_port'],
                        database=db_config['db_name'],
                        user=db_config['db_user'],
                        password_set=bool(db_config['db_password']))
            
            # 创建数据库管理器
            logger.info("创建数据库管理器")
            self.db_manager = DatabaseManager(db_config)
            logger.info("数据库管理器创建完成")
            
            # 创建EODHD新闻服务实例用于实际的新闻获取
            logger.info("创建EODHD新闻服务实例")
            self.eodhd_service = EODHDNewsService()
            logger.info("EODHD新闻服务实例创建完成")
            
            # 测试数据库连接
            logger.info("测试数据库连接")
            test_conn = self.db_manager.get_connection()
            if test_conn:
                test_conn.close()
                logger.info("数据库连接测试成功")
            else:
                logger.warning("数据库连接测试失败，但服务仍可继续")
            
            logger.info("Watchlist新闻服务初始化完成")
            
        except Exception as e:
            logger.error("Watchlist新闻服务初始化失败", exception=str(e))
            raise
    
    def get_all_active_tickers(self) -> List[str]:
        """
        获取所有用户watchlist中的活跃股票代码
        
        Returns:
            List[str]: primaryticker列表
        """
        logger.info("开始获取所有活跃的watchlist股票代码")
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("数据库连接失败")
                return []
            
            cursor = conn.cursor()
            
            # 查询所有启用的watchlist项目的primaryticker
            query = """
                SELECT DISTINCT primaryticker 
                FROM target_tracking_config 
                WHERE enabled = true 
                AND primaryticker IS NOT NULL 
                AND primaryticker != ''
                ORDER BY primaryticker
            """
            
            cursor.execute(query)
            results = cursor.fetchall()
            
            # 提取primaryticker列表
            tickers = [row[0] for row in results if row[0]]
            
            cursor.close()
            conn.close()
            
            logger.info("成功获取watchlist股票代码", 
                       ticker_count=len(tickers), 
                       tickers=tickers[:10] if len(tickers) > 10 else tickers)  # 只显示前10个
            
            return tickers
            
        except Exception as e:
            logger.error("获取watchlist股票代码失败", exception=str(e))
            return []
    
    def get_user_watchlist_tickers(self, user_id: int) -> List[str]:
        """
        获取指定用户watchlist中的股票代码
        
        Args:
            user_id: 用户ID
            
        Returns:
            List[str]: 该用户的primaryticker列表
        """
        logger.info("开始获取用户watchlist股票代码", user_id=user_id)
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("数据库连接失败")
                return []
            
            cursor = conn.cursor()
            
            # 查询指定用户的启用watchlist项目的primaryticker
            query = """
                SELECT DISTINCT primaryticker 
                FROM target_tracking_config 
                WHERE user_id = %s 
                AND enabled = true 
                AND primaryticker IS NOT NULL 
                AND primaryticker != ''
                ORDER BY primaryticker
            """
            
            cursor.execute(query, [user_id])
            results = cursor.fetchall()
            
            # 提取primaryticker列表
            tickers = [row[0] for row in results if row[0]]
            
            cursor.close()
            conn.close()
            
            logger.info("成功获取用户watchlist股票代码", 
                       user_id=user_id,
                       ticker_count=len(tickers), 
                       tickers=tickers[:10] if len(tickers) > 10 else tickers)  # 只显示前10个
            
            return tickers
            
        except Exception as e:
            logger.error("获取用户watchlist股票代码失败", user_id=user_id, exception=str(e))
            return []
    
    def fetch_and_store_watchlist_news(self, max_results: Optional[int] = None, 
                                     recent_days: Optional[int] = None,
                                     user_id: Optional[int] = None) -> Dict[str, Any]:
        """
        获取watchlist相关新闻并存储到数据库
        
        Args:
            max_results: 最大结果数
            recent_days: 最近天数
            user_id: 用户ID，如果提供则只获取该用户的watchlist，否则获取所有用户的
            
        Returns:
            包含详细统计信息的字典
        """
        logger.info("开始watchlist新闻获取和存储流程", 
                   max_results=max_results, 
                   recent_days=recent_days,
                   user_id=user_id)
        
        try:
            # 步骤1：获取股票代码列表
            if user_id:
                tickers = self.get_user_watchlist_tickers(user_id)
                logger.info("获取指定用户watchlist股票代码", user_id=user_id, ticker_count=len(tickers))
            else:
                tickers = self.get_all_active_tickers()
                logger.info("获取所有用户watchlist股票代码", ticker_count=len(tickers))
            
            if not tickers:
                logger.warning("未找到任何watchlist股票代码")
                return {
                    'success_count': 0,
                    'existing_count': 0,
                    'failed_count': 0,
                    'total_count': 0,
                    'fetched_count': 0,
                    'tickers_count': 0,
                    'tickers': []
                }
            
            # 步骤2：处理日期范围参数
            start_date = None
            end_date = None
            
            if recent_days is not None:
                # 计算日期范围
                current_date = datetime.now()
                end_date = current_date.strftime('%Y-%m-%d')
                start_date = (current_date - timedelta(days=recent_days - 1)).strftime('%Y-%m-%d')
                
                logger.info("根据recent_days参数计算日期范围", 
                           recent_days=recent_days,
                           start_date=start_date, 
                           end_date=end_date)
            
            # 步骤3：将股票代码列表转换为逗号分隔的字符串
            symbols_string = ','.join(tickers)
            logger.info("准备调用EODHD新闻服务", 
                       symbols_count=len(tickers),
                       symbols_preview=symbols_string[:100] + "..." if len(symbols_string) > 100 else symbols_string)
            
            # 步骤4：调用EODHD新闻服务获取新闻
            result = self.eodhd_service.fetch_and_store_news(
                symbols=symbols_string,
                topic=None,  # watchlist模式不使用topic
                start_date=start_date,
                end_date=end_date,
                max_results=max_results,
                topic_filter=None,  # watchlist模式不使用topic_filter
                keywords=None  # watchlist模式不使用keywords
            )
            
            # 添加watchlist特有的统计信息
            result.update({
                'tickers_count': len(tickers),
                'tickers': tickers,
                'user_id': user_id,
                'service_type': 'watchlist_news'
            })
            
            logger.info("watchlist新闻获取和存储流程完成", 
                       **{k: v for k, v in result.items() if k != 'tickers'})  # 不打印完整的tickers列表
            
            return result
            
        except Exception as e:
            logger.error("watchlist新闻获取和存储流程失败", exception=str(e))
            raise Exception(f"获取和存储watchlist新闻时发生错误: {str(e)}")
    
    def get_watchlist_statistics(self) -> Dict[str, Any]:
        """
        获取watchlist统计信息
        
        Returns:
            包含统计信息的字典
        """
        logger.info("开始获取watchlist统计信息")
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("数据库连接失败")
                return {}
            
            cursor = conn.cursor()
            
            # 统计总用户数
            cursor.execute("SELECT COUNT(DISTINCT user_id) FROM target_tracking_config WHERE enabled = true")
            total_users = cursor.fetchone()[0] or 0
            
            # 统计总股票数
            cursor.execute("""
                SELECT COUNT(DISTINCT primaryticker) 
                FROM target_tracking_config 
                WHERE enabled = true AND primaryticker IS NOT NULL AND primaryticker != ''
            """)
            total_tickers = cursor.fetchone()[0] or 0
            
            # 统计总watchlist项目数
            cursor.execute("SELECT COUNT(*) FROM target_tracking_config WHERE enabled = true")
            total_items = cursor.fetchone()[0] or 0
            
            # 按用户统计
            cursor.execute("""
                SELECT user_id, COUNT(*) as item_count
                FROM target_tracking_config 
                WHERE enabled = true 
                GROUP BY user_id 
                ORDER BY item_count DESC
                LIMIT 10
            """)
            user_stats = cursor.fetchall()
            
            # 按股票统计
            cursor.execute("""
                SELECT primaryticker, COUNT(*) as user_count
                FROM target_tracking_config 
                WHERE enabled = true AND primaryticker IS NOT NULL AND primaryticker != ''
                GROUP BY primaryticker 
                ORDER BY user_count DESC
                LIMIT 10
            """)
            ticker_stats = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            statistics = {
                'total_users': total_users,
                'total_unique_tickers': total_tickers,
                'total_watchlist_items': total_items,
                'top_users_by_items': [{'user_id': row[0], 'item_count': row[1]} for row in user_stats],
                'top_tickers_by_users': [{'ticker': row[0], 'user_count': row[1]} for row in ticker_stats]
            }
            
            logger.info("成功获取watchlist统计信息", **statistics)
            
            return statistics
            
        except Exception as e:
            logger.error("获取watchlist统计信息失败", exception=str(e))
            return {}
    
    def validate_watchlist_data(self) -> Dict[str, Any]:
        """
        验证watchlist数据的完整性
        
        Returns:
            验证结果字典
        """
        logger.info("开始验证watchlist数据完整性")
        
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("数据库连接失败")
                return {"valid": False, "error": "数据库连接失败"}
            
            cursor = conn.cursor()
            
            issues = []
            
            # 检查缺失primaryticker的记录
            cursor.execute("""
                SELECT COUNT(*) FROM target_tracking_config 
                WHERE enabled = true AND (primaryticker IS NULL OR primaryticker = '')
            """)
            missing_tickers = cursor.fetchone()[0] or 0
            
            if missing_tickers > 0:
                issues.append(f"有 {missing_tickers} 个启用的watchlist项目缺少primaryticker")
            
            # 检查无效的股票代码格式
            cursor.execute("""
                SELECT DISTINCT primaryticker FROM target_tracking_config 
                WHERE enabled = true AND primaryticker IS NOT NULL AND primaryticker != ''
                AND primaryticker !~ '^[A-Z0-9]+\\.(US|HK|SHE|SHG)$'
            """)
            invalid_tickers = cursor.fetchall()
            
            if invalid_tickers:
                issues.append(f"发现 {len(invalid_tickers)} 个可能无效的股票代码格式: {[row[0] for row in invalid_tickers[:5]]}")
            
            # 检查重复的用户-股票组合
            cursor.execute("""
                SELECT user_id, symbol, COUNT(*) as count
                FROM target_tracking_config 
                WHERE enabled = true 
                GROUP BY user_id, symbol 
                HAVING COUNT(*) > 1
            """)
            duplicates = cursor.fetchall()
            
            if duplicates:
                issues.append(f"发现 {len(duplicates)} 个重复的用户-股票组合")
            
            cursor.close()
            conn.close()
            
            validation_result = {
                "valid": len(issues) == 0,
                "issues": issues,
                "missing_tickers_count": missing_tickers,
                "invalid_tickers_count": len(invalid_tickers) if invalid_tickers else 0,
                "duplicate_combinations_count": len(duplicates) if duplicates else 0
            }
            
            logger.info("watchlist数据验证完成", **validation_result)
            
            return validation_result
            
        except Exception as e:
            logger.error("验证watchlist数据失败", exception=str(e))
            return {"valid": False, "error": str(e)}
