# -*- coding: utf-8 -*-
"""
Futu Investment Themes Service - 富途投资主题数据获取服务

通过调用爬虫模块获取富途投资主题和股票数据
数据来源：https://www.futunn.com/en/quote/us/concepts

主要功能：
1. 调用爬虫模块（backend.scrapers.futu_scraper）获取主题和股票数据
2. 解析爬取的数据
3. 存储到数据库（futu_investment_themes 和 futu_theme_stocks 表）
4. 支持港股(HK)和美股(US)市场
"""

import os
import json
import subprocess
import time
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional
from pathlib import Path

from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger

logger = get_logger("futu_themes_service")


class FutuThemesService:
    """富途投资主题数据服务"""
    
    def __init__(self):
        """初始化富途主题服务"""
        logger.info("开始初始化富途投资主题服务")
        
        try:
            # 获取标准化数据库连接配置
            from backend.utils.database_config import get_database_config
            db_config = get_database_config()
            self.db_manager = DatabaseManager(db_config)
            logger.info("数据库管理器配置完成")
            
            logger.info("富途投资主题服务初始化完成")
            
        except Exception as e:
            logger.error(f"初始化富途投资主题服务失败: {str(e)}")
            raise
    
    def run_scraper(self, markets: Optional[List[str]] = None, 
                   max_pages: Optional[int] = None,
                   scrape_stocks: bool = True) -> Optional[Dict[str, Any]]:
        """
        运行爬虫脚本获取数据
        
        Args:
            markets: 要爬取的市场列表，如 ['HK', 'US']，None表示爬取所有市场
            max_pages: 每个市场最大爬取页数，None表示爬取所有页面
            scrape_stocks: 是否爬取股票列表
            
        Returns:
            Dict: 爬取的数据，格式为 {market: [themes]}，失败返回None
        """
        try:
            logger.info(f"开始运行爬虫，市场: {markets or 'ALL'}, 最大页数: {max_pages or 'ALL'}")
            
            # 直接导入爬虫模块（从 backend.scrapers 包）
            from backend.scrapers import scrape_all_markets
            
            # 调用爬取函数
            logger.info("开始爬取数据...")
            start_time = time.time()
            
            all_markets_data = scrape_all_markets(
                markets_to_scrape=markets,
                max_consecutive_empty=3,
                max_pages=max_pages,
                scrape_stocks=scrape_stocks,
                max_themes_with_stocks=None  # 爬取所有主题的股票
            )
            
            elapsed_time = time.time() - start_time
            logger.info(f"爬取完成，耗时: {elapsed_time:.2f}秒")
            
            # 统计爬取结果
            total_themes = sum(len(themes) for themes in all_markets_data.values())
            total_stocks = sum(
                sum(len(theme.get('stocks', [])) for theme in themes)
                for themes in all_markets_data.values()
            )
            
            logger.info(f"爬取统计: {total_themes}个主题, {total_stocks}只股票")
            
            return all_markets_data
            
        except Exception as e:
            logger.error(f"运行爬虫失败: {str(e)}")
            return None
    
    def parse_theme_data(self, theme_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        解析单个主题数据
        
        Args:
            theme_data: 主题原始数据
            
        Returns:
            Dict: 解析后的主题数据，失败返回None
        """
        try:
            # 提取主题基本信息
            parsed_data = {
                'theme_id': theme_data.get('theme_id', ''),
                'theme_name': theme_data.get('theme_name', ''),
                'market': theme_data.get('market', ''),
                'theme_url': theme_data.get('theme_url', ''),
                'change_percent': theme_data.get('change_percent', ''),
                'gainers': theme_data.get('gainers', ''),
                'losers': theme_data.get('losers', ''),
                'unchanged': theme_data.get('unchanged', ''),
                'top_stock': theme_data.get('top_stock', ''),
                'top_stock_change': theme_data.get('top_stock_change', ''),
                'stock_count': theme_data.get('stock_count', 0),
                'scraped_at': theme_data.get('scraped_at', datetime.now(timezone.utc).isoformat())
            }
            
            # 提取股票列表
            stocks = theme_data.get('stocks', [])
            parsed_data['stocks'] = stocks
            
            return parsed_data
            
        except Exception as e:
            logger.error(f"解析主题数据失败: {str(e)}, data: {theme_data}")
            return None
    
    def save_theme_data(self, theme_data: Dict[str, Any]) -> Optional[int]:
        """
        保存主题数据到数据库
        
        Args:
            theme_data: 解析后的主题数据
            
        Returns:
            int: 保存的主题ID，失败返回None
        """
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                logger.error("无法获取数据库连接")
                return None
            
            cursor = conn.cursor()
            
            try:
                # 插入主题数据（使用ON CONFLICT更新）
                # 修改：只按 theme_id + market 去重，不按日期，避免同一主题在不同日期重复存储
                insert_theme_sql = """
                INSERT INTO futu_investment_themes 
                (theme_id, theme_name, market, theme_url, change_percent, 
                 gainers, losers, unchanged, top_stock, top_stock_change, 
                 stock_count, scraped_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (theme_id, market)
                DO UPDATE SET
                    theme_name = EXCLUDED.theme_name,
                    theme_url = EXCLUDED.theme_url,
                    change_percent = EXCLUDED.change_percent,
                    gainers = EXCLUDED.gainers,
                    losers = EXCLUDED.losers,
                    unchanged = EXCLUDED.unchanged,
                    top_stock = EXCLUDED.top_stock,
                    top_stock_change = EXCLUDED.top_stock_change,
                    stock_count = EXCLUDED.stock_count,
                    scraped_at = EXCLUDED.scraped_at,
                    updated_at = CURRENT_TIMESTAMP,
                    -- 关键：保留用户配置的状态，不被爬虫覆盖
                    is_deleted = futu_investment_themes.is_deleted,
                    is_enabled_for_matching = futu_investment_themes.is_enabled_for_matching
                RETURNING id
                """
                
                cursor.execute(insert_theme_sql, (
                    theme_data['theme_id'],
                    theme_data['theme_name'],
                    theme_data['market'],
                    theme_data['theme_url'],
                    theme_data['change_percent'],
                    theme_data['gainers'],
                    theme_data['losers'],
                    theme_data['unchanged'],
                    theme_data['top_stock'],
                    theme_data['top_stock_change'],
                    theme_data['stock_count'],
                    theme_data['scraped_at']
                ))
                
                theme_id = cursor.fetchone()[0]
                
                # 插入股票数据
                if theme_data.get('stocks'):
                    # 先删除该主题的旧股票数据
                    cursor.execute(
                        "DELETE FROM futu_theme_stocks WHERE theme_id = %s",
                        (theme_id,)
                    )
                    
                    # 批量插入新股票数据
                    insert_stock_sql = """
                    INSERT INTO futu_theme_stocks 
                    (theme_id, symbol, stock_name, stock_url, market)
                    VALUES (%s, %s, %s, %s, %s)
                    """
                    
                    stock_records = [
                        (
                            theme_id,
                            stock['symbol'],
                            stock['stock_name'],
                            stock.get('stock_url', ''),
                            stock.get('market', theme_data['market'])
                        )
                        for stock in theme_data['stocks']
                    ]
                    
                    cursor.executemany(insert_stock_sql, stock_records)
                    logger.debug(f"保存了{len(stock_records)}只股票到主题{theme_id}")
                
                conn.commit()
                logger.info(f"成功保存主题: {theme_data['theme_name']} (ID: {theme_id})")
                return theme_id
                
            except Exception as e:
                conn.rollback()
                logger.error(f"保存主题数据失败: {str(e)}")
                return None
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"保存主题数据失败: {str(e)}")
            return None
    
    def fetch_and_store_themes(self, markets: Optional[List[str]] = None,
                               max_pages: Optional[int] = None,
                               scrape_stocks: bool = True) -> Dict[str, Any]:
        """
        获取并存储富途投资主题数据
        
        Args:
            markets: 要爬取的市场列表，如 ['HK', 'US']
            max_pages: 每个市场最大爬取页数
            scrape_stocks: 是否爬取股票列表
            
        Returns:
            Dict: 执行结果统计
        """
        logger.info("开始获取并存储富途投资主题数据")
        
        start_time = datetime.now()
        total_success = 0
        total_failed = 0
        total_stocks = 0
        
        try:
            # 1. 运行爬虫获取数据
            all_markets_data = self.run_scraper(
                markets=markets,
                max_pages=max_pages,
                scrape_stocks=scrape_stocks
            )
            
            if not all_markets_data:
                logger.error("爬虫未返回任何数据")
                return {
                    'total_themes': 0,
                    'success_count': 0,
                    'failed_count': 0,
                    'total_stocks': 0,
                    'execution_time': 0,
                    'error': '爬虫未返回任何数据'
                }
            
            # 2. 处理每个市场的数据
            for market, themes_list in all_markets_data.items():
                logger.info(f"处理{market}市场的{len(themes_list)}个主题")
                
                for theme_data in themes_list:
                    # 解析主题数据
                    parsed_theme = self.parse_theme_data(theme_data)
                    
                    if parsed_theme:
                        # 保存到数据库
                        theme_id = self.save_theme_data(parsed_theme)
                        
                        if theme_id:
                            total_success += 1
                            total_stocks += len(parsed_theme.get('stocks', []))
                        else:
                            total_failed += 1
                    else:
                        total_failed += 1
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"富途投资主题数据获取完成：总计{total_success + total_failed}个主题，"
                       f"成功{total_success}，失败{total_failed}，"
                       f"共{total_stocks}只股票，"
                       f"耗时{execution_time:.2f}秒")
            
            return {
                'total_themes': total_success + total_failed,
                'success_count': total_success,
                'failed_count': total_failed,
                'total_stocks': total_stocks,
                'execution_time': execution_time,
                'markets': list(all_markets_data.keys())
            }
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"获取富途投资主题数据失败: {str(e)}，耗时{execution_time:.2f}秒")
            return {
                'total_themes': 0,
                'success_count': total_success,
                'failed_count': total_failed,
                'total_stocks': total_stocks,
                'execution_time': execution_time,
                'error': str(e)
            }
    
    def get_themes_by_market(self, market: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        按市场获取主题列表
        
        Args:
            market: 市场代码 (HK, US)
            limit: 返回数量限制
            
        Returns:
            List[Dict]: 主题列表
        """
        try:
            query = """
            SELECT 
                id, theme_id, theme_name, market, theme_url, 
                change_percent, gainers, losers, unchanged,
                top_stock, top_stock_change, stock_count, 
                scraped_at, created_at, updated_at
            FROM futu_investment_themes
            WHERE market = %s
            ORDER BY scraped_at DESC, ABS(REPLACE(change_percent, '%', '')::DECIMAL) DESC
            LIMIT %s
            """
            
            results = self.db_manager.fetch_all(query, [market, limit])
            
            themes = []
            for row in results:
                theme = {
                    'id': row[0],
                    'theme_id': row[1],
                    'theme_name': row[2],
                    'market': row[3],
                    'theme_url': row[4],
                    'change_percent': row[5],
                    'gainers': row[6],
                    'losers': row[7],
                    'unchanged': row[8],
                    'top_stock': row[9],
                    'top_stock_change': row[10],
                    'stock_count': row[11],
                    'scraped_at': row[12].isoformat() if row[12] else None,
                    'created_at': row[13].isoformat() if row[13] else None,
                    'updated_at': row[14].isoformat() if row[14] else None
                }
                themes.append(theme)
            
            return themes
            
        except Exception as e:
            logger.error(f"获取市场主题列表失败: {str(e)}")
            return []
    
    def get_theme_stocks(self, theme_id: int) -> List[Dict[str, Any]]:
        """
        获取主题关联的股票列表
        
        Args:
            theme_id: 主题数据库ID
            
        Returns:
            List[Dict]: 股票列表
        """
        try:
            query = """
            SELECT id, symbol, stock_name, stock_url, market, created_at
            FROM futu_theme_stocks
            WHERE theme_id = %s
            ORDER BY symbol
            """
            
            results = self.db_manager.fetch_all(query, [theme_id])
            
            stocks = []
            for row in results:
                stock = {
                    'id': row[0],
                    'symbol': row[1],
                    'stock_name': row[2],
                    'stock_url': row[3],
                    'market': row[4],
                    'created_at': row[5].isoformat() if row[5] else None
                }
                stocks.append(stock)
            
            return stocks
            
        except Exception as e:
            logger.error(f"获取主题股票列表失败: {str(e)}")
            return []


def create_futu_themes_service() -> FutuThemesService:
    """创建富途投资主题服务实例"""
    return FutuThemesService()


# 测试函数
def test_service():
    """测试服务功能"""
    logger.info("开始测试富途投资主题服务")
    
    try:
        service = create_futu_themes_service()
        
        # 测试爬取港股市场前2页数据
        result = service.fetch_and_store_themes(
            markets=['HK'],
            max_pages=2,
            scrape_stocks=True
        )
        
        logger.info("测试完成", result=result)
        return result
        
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        return {'error': str(e)}


if __name__ == "__main__":
    # 直接运行测试
    result = test_service()
    print(f"测试结果: {result}")

