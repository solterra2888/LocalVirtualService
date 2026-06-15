#!/usr/bin/env python3
"""
Finviz Screener Service
基于参考代码实现的 Finviz 筛选数据管理服务
"""

import os
import csv
import io
import asyncio
import time
from datetime import datetime
from typing import Optional, Dict, Any
import requests
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager

from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
# 统一的环境变量加载
from backend.utils.env_loader import load_project_env

logger = get_logger("finviz_screener_service")


class FinvizScreenerService:
    """Finviz筛选数据管理服务"""
    
    def __init__(self):
        """初始化Finviz筛选数据服务"""
        logger.info("开始初始化Finviz筛选数据服务")
        
        # 获取API密钥
        self.api_key = os.getenv('finviz_api_key')
        if not self.api_key:
            logger.warning("finviz_api_key未设置。某些功能可能不可用。")
        
        # 获取标准化数据库连接配置
        from backend.utils.database_config import get_psycopg2_config
        self.db_config = get_psycopg2_config()
        
        # 初始化连接池
        try:
            self.connection_pool = ThreadedConnectionPool(
                minconn=2,
                maxconn=10,
                **self.db_config
            )
            logger.info("Finviz数据库连接池初始化成功")
        except Exception as e:
            logger.error(f"Finviz数据库连接池初始化失败: {e}")
            self.connection_pool = None
        
        logger.info("Finviz筛选数据服务初始化完成")
    
    @contextmanager
    def get_connection(self):
        """从连接池获取连接的上下文管理器"""
        if not self.connection_pool:
            raise Exception("连接池未初始化")
        
        connection = None
        try:
            connection = self.connection_pool.getconn()
            yield connection
        except Exception as e:
            if connection:
                connection.rollback()
            raise e
        finally:
            if connection:
                self.connection_pool.putconn(connection)
    
    async def download_finviz_data(self, csv_file_path="screener_all.csv") -> bool:
        """下载 Finviz screener 数据"""
        if not self.api_key:
            logger.error("未找到 finviz_api_key 环境变量")
            return False
        
        # Finviz screener 导出 URL
        url = f"https://elite.finviz.com/export.ashx?v=152&c=0,1,2,79,3,4,5,129,6,7,8,9,10,11,12,13,75,16,17,18,19,20,21,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,47,46,138,139,140,48,49,50,51,52,53,54,57,58,134,59,68,60,61,63,64,67,65,66,78&auth={self.api_key}"
        
        try:
            logger.info("正在下载 Finviz screener 数据...")
            start_time = time.time()
            
            # 使用 asyncio 进行异步 HTTP 请求
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                lambda: requests.get(url, timeout=60)
            )
            response.raise_for_status()
            
            # 保存为 CSV 文件
            with open(csv_file_path, "wb") as f:
                f.write(response.content)
            
            download_time = time.time() - start_time
            logger.info(f"数据下载成功，保存为 {csv_file_path}")
            logger.info(f"文件大小: {len(response.content)} 字节，耗时: {download_time:.2f}秒")
            
            return True
            
        except requests.exceptions.RequestException as e:
            logger.error(f"下载失败: {e}")
            return False
        except Exception as e:
            logger.error(f"保存文件失败: {e}")
            return False
    
    def create_table_if_not_exists(self) -> bool:
        """创建表（如果不存在）"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS finviz_screener (
            id SERIAL PRIMARY KEY,
            ticker VARCHAR(20) NOT NULL,
            company VARCHAR(255),
            index_name VARCHAR(100),
            sector VARCHAR(100),
            industry VARCHAR(255),
            country VARCHAR(100),
            exchange VARCHAR(20),
            market_cap NUMERIC,
            pe_ratio NUMERIC,
            forward_pe NUMERIC,
            peg NUMERIC,
            ps_ratio NUMERIC,
            pb_ratio NUMERIC,
            pcash_ratio NUMERIC,
            pfree_cash_flow NUMERIC,
            dividend NUMERIC,
            eps_ttm NUMERIC,
            eps_growth_this_year VARCHAR(20),
            eps_growth_next_year VARCHAR(20),
            eps_growth_past_5_years VARCHAR(20),
            eps_growth_next_5_years VARCHAR(20),
            sales_growth_past_5_years VARCHAR(20),
            shares_outstanding NUMERIC,
            shares_float NUMERIC,
            insider_ownership VARCHAR(20),
            insider_transactions VARCHAR(20),
            institutional_ownership VARCHAR(20),
            institutional_transactions VARCHAR(20),
            short_float VARCHAR(20),
            short_ratio NUMERIC,
            return_on_assets VARCHAR(20),
            return_on_equity VARCHAR(20),
            return_on_invested_capital VARCHAR(20),
            current_ratio NUMERIC,
            quick_ratio NUMERIC,
            lt_debt_equity NUMERIC,
            total_debt_equity NUMERIC,
            gross_margin VARCHAR(20),
            operating_margin VARCHAR(20),
            profit_margin VARCHAR(20),
            performance_week VARCHAR(20),
            performance_month VARCHAR(20),
            performance_quarter VARCHAR(20),
            performance_half_year VARCHAR(20),
            performance_ytd VARCHAR(20),
            performance_year VARCHAR(20),
            performance_3_years VARCHAR(20),
            performance_5_years VARCHAR(20),
            performance_10_years VARCHAR(20),
            beta NUMERIC,
            average_true_range NUMERIC,
            volatility_week VARCHAR(20),
            volatility_month VARCHAR(20),
            sma_20_day NUMERIC,
            sma_50_day NUMERIC,
            sma_200_day NUMERIC,
            week_52_high NUMERIC,
            week_52_low NUMERIC,
            week_52_range VARCHAR(50),
            rsi_14 NUMERIC,
            earnings_date TIMESTAMP,
            change_from_open VARCHAR(20),
            gap VARCHAR(20),
            average_volume NUMERIC,
            relative_volume NUMERIC,
            volume NUMERIC,
            price NUMERIC,
            change_percent VARCHAR(20),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            snapshot_date DATE NOT NULL DEFAULT CURRENT_DATE
        );
        
        -- 创建索引以提高查询性能
        CREATE INDEX IF NOT EXISTS idx_finviz_screener_ticker ON finviz_screener(ticker);
        CREATE INDEX IF NOT EXISTS idx_finviz_screener_sector ON finviz_screener(sector);
        CREATE INDEX IF NOT EXISTS idx_finviz_screener_created_at ON finviz_screener(created_at);
        CREATE INDEX IF NOT EXISTS idx_finviz_screener_snapshot_date ON finviz_screener(snapshot_date);
        CREATE INDEX IF NOT EXISTS idx_finviz_screener_ticker_date ON finviz_screener(ticker, snapshot_date);
        """
        
        try:
            with self.get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    connection.commit()
                    logger.info("Finviz表和索引创建/验证成功")
                    return True
        except Exception as e:
            logger.error(f"创建Finviz表失败: {e}")
            return False
    
    def clean_csv_data_for_copy(self, csv_file_path: str) -> str:
        """清理CSV数据并准备用于COPY命令的格式"""
        cleaned_data = io.StringIO()
        
        try:
            with open(csv_file_path, 'r', encoding='utf-8') as file:
                csv_reader = csv.DictReader(file)
                
                for row in csv_reader:
                    # 清理数据并按照表结构顺序输出
                    cleaned_values = []
                    
                    # 按照表结构的顺序处理字段
                    field_mappings = [
                        ('ticker', 'Ticker'),
                        ('company', 'Company'),
                        ('index_name', 'Index'),
                        ('sector', 'Sector'),
                        ('industry', 'Industry'),
                        ('country', 'Country'),
                        ('exchange', 'Exchange'),
                        ('market_cap', 'Market Cap'),
                        ('pe_ratio', 'P/E'),
                        ('forward_pe', 'Forward P/E'),
                        ('peg', 'PEG'),
                        ('ps_ratio', 'P/S'),
                        ('pb_ratio', 'P/B'),
                        ('pcash_ratio', 'P/Cash'),
                        ('pfree_cash_flow', 'P/Free Cash Flow'),
                        ('dividend', 'Dividend'),
                        ('eps_ttm', 'EPS (ttm)'),
                        ('eps_growth_this_year', 'EPS Growth This Year'),
                        ('eps_growth_next_year', 'EPS Growth Next Year'),
                        ('eps_growth_past_5_years', 'EPS Growth Past 5 Years'),
                        ('eps_growth_next_5_years', 'EPS Growth Next 5 Years'),
                        ('sales_growth_past_5_years', 'Sales Growth Past 5 Years'),
                        ('shares_outstanding', 'Shares Outstanding'),
                        ('shares_float', 'Shares Float'),
                        ('insider_ownership', 'Insider Ownership'),
                        ('insider_transactions', 'Insider Transactions'),
                        ('institutional_ownership', 'Institutional Ownership'),
                        ('institutional_transactions', 'Institutional Transactions'),
                        ('short_float', 'Short Float'),
                        ('short_ratio', 'Short Ratio'),
                        ('return_on_assets', 'Return on Assets'),
                        ('return_on_equity', 'Return on Equity'),
                        ('return_on_invested_capital', 'Return on Invested Capital'),
                        ('current_ratio', 'Current Ratio'),
                        ('quick_ratio', 'Quick Ratio'),
                        ('lt_debt_equity', 'LT Debt/Equity'),
                        ('total_debt_equity', 'Total Debt/Equity'),
                        ('gross_margin', 'Gross Margin'),
                        ('operating_margin', 'Operating Margin'),
                        ('profit_margin', 'Profit Margin'),
                        ('performance_week', 'Performance (Week)'),
                        ('performance_month', 'Performance (Month)'),
                        ('performance_quarter', 'Performance (Quarter)'),
                        ('performance_half_year', 'Performance (Half Year)'),
                        ('performance_ytd', 'Performance (YTD)'),
                        ('performance_year', 'Performance (Year)'),
                        ('performance_3_years', 'Performance (3 Years)'),
                        ('performance_5_years', 'Performance (5 Years)'),
                        ('performance_10_years', 'Performance (10 Years)'),
                        ('beta', 'Beta'),
                        ('average_true_range', 'Average True Range'),
                        ('volatility_week', 'Volatility (Week)'),
                        ('volatility_month', 'Volatility (Month)'),
                        ('sma_20_day', '20-Day Simple Moving Average'),
                        ('sma_50_day', '50-Day Simple Moving Average'),
                        ('sma_200_day', '200-Day Simple Moving Average'),
                        ('week_52_high', '52-Week High'),
                        ('week_52_low', '52-Week Low'),
                        ('week_52_range', '52-Week Range'),
                        ('rsi_14', 'Relative Strength Index (14)'),
                        ('earnings_date', 'Earnings Date'),
                        ('change_from_open', 'Change from Open'),
                        ('gap', 'Gap'),
                        ('average_volume', 'Average Volume'),
                        ('relative_volume', 'Relative Volume'),
                        ('volume', 'Volume'),
                        ('price', 'Price'),
                        ('change_percent', 'Change')
                    ]
                    
                    for db_field, csv_field in field_mappings:
                        value = row.get(csv_field, '')
                        
                        # 清理数据
                        if not value or value.strip() == '' or value.strip() == '-':
                            cleaned_values.append('\\N')  # PostgreSQL NULL值
                        else:
                            value = value.strip().strip('"').strip('%')
                            
                            # 处理数字字段
                            if db_field in ['market_cap', 'pe_ratio', 'forward_pe', 'peg', 'ps_ratio', 
                                          'pb_ratio', 'pcash_ratio', 'pfree_cash_flow', 'dividend', 
                                          'eps_ttm', 'shares_outstanding', 'shares_float', 'short_ratio',
                                          'current_ratio', 'quick_ratio', 'lt_debt_equity', 'total_debt_equity',
                                          'beta', 'average_true_range', 'sma_20_day', 'sma_50_day', 
                                          'sma_200_day', 'week_52_high', 'week_52_low', 'rsi_14',
                                          'average_volume', 'relative_volume', 'volume', 'price']:
                                try:
                                    if ',' in value:
                                        value = value.replace(',', '')
                                    float(value)  # 验证是否为数字
                                    cleaned_values.append(value)
                                except:
                                    cleaned_values.append('\\N')
                            else:
                                # 字符串字段，转义特殊字符
                                value = value.replace('\\', '\\\\').replace('\t', '\\t').replace('\n', '\\n').replace('\r', '\\r')
                                cleaned_values.append(value)
                    
                    # 写入清理后的数据（制表符分隔）
                    cleaned_data.write('\t'.join(cleaned_values) + '\n')
            
            return cleaned_data.getvalue()
            
        except Exception as e:
            logger.error(f"清理CSV数据失败: {e}")
            return ""
    
    async def import_csv_data_optimized(self, csv_file_path: str) -> bool:
        """使用COPY命令高效导入CSV数据"""
        try:
            start_time = time.time()
            
            # 清理数据
            logger.info("正在清理和准备数据...")
            clean_start = time.time()
            
            # 在执行器中运行同步的数据清理
            loop = asyncio.get_event_loop()
            cleaned_data = await loop.run_in_executor(
                None, 
                self.clean_csv_data_for_copy, 
                csv_file_path
            )
            
            if not cleaned_data:
                return False
            clean_time = time.time() - clean_start
            logger.info(f"数据清理完成，耗时: {clean_time:.2f}秒")
            
            with self.get_connection() as connection:
                with connection.cursor() as cursor:
                    # 清空现有数据
                    truncate_start = time.time()
                    cursor.execute("TRUNCATE TABLE finviz_screener RESTART IDENTITY")
                    truncate_time = time.time() - truncate_start
                    logger.info(f"清空表完成，耗时: {truncate_time:.2f}秒")
                    
                    # 使用COPY命令批量导入
                    copy_start = time.time()
                    
                    # 定义COPY命令的列顺序
                    columns = [
                        'ticker', 'company', 'index_name', 'sector', 'industry', 'country', 'exchange',
                        'market_cap', 'pe_ratio', 'forward_pe', 'peg', 'ps_ratio', 'pb_ratio', 'pcash_ratio',
                        'pfree_cash_flow', 'dividend', 'eps_ttm', 'eps_growth_this_year', 'eps_growth_next_year',
                        'eps_growth_past_5_years', 'eps_growth_next_5_years', 'sales_growth_past_5_years',
                        'shares_outstanding', 'shares_float', 'insider_ownership', 'insider_transactions',
                        'institutional_ownership', 'institutional_transactions', 'short_float', 'short_ratio',
                        'return_on_assets', 'return_on_equity', 'return_on_invested_capital', 'current_ratio',
                        'quick_ratio', 'lt_debt_equity', 'total_debt_equity', 'gross_margin', 'operating_margin',
                        'profit_margin', 'performance_week', 'performance_month', 'performance_quarter',
                        'performance_half_year', 'performance_ytd', 'performance_year', 'performance_3_years',
                        'performance_5_years', 'performance_10_years', 'beta', 'average_true_range',
                        'volatility_week', 'volatility_month', 'sma_20_day', 'sma_50_day', 'sma_200_day',
                        'week_52_high', 'week_52_low', 'week_52_range', 'rsi_14', 'earnings_date',
                        'change_from_open', 'gap', 'average_volume', 'relative_volume', 'volume', 'price', 'change_percent'
                    ]
                    
                    copy_sql = f"COPY finviz_screener ({', '.join(columns)}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N')"
                    
                    cursor.copy_expert(copy_sql, io.StringIO(cleaned_data))
                    connection.commit()
                    
                    copy_time = time.time() - copy_start
                    
                    # 获取导入的行数
                    cursor.execute("SELECT COUNT(*) FROM finviz_screener")
                    total_rows = cursor.fetchone()[0]
                    
                    total_time = time.time() - start_time
                    
                    logger.info(f"COPY命令执行完成，耗时: {copy_time:.2f}秒")
                    logger.info(f"数据导入完成，总共导入 {total_rows} 行数据")
                    logger.info(f"总耗时: {total_time:.2f}秒，平均速度: {total_rows/total_time:.0f} 行/秒")
                    
                    return True
                    
        except Exception as e:
            logger.error(f"导入数据失败: {e}")
            return False
    
    async def fetch_and_store_finviz_data(self) -> Dict[str, Any]:
        """获取并存储Finviz筛选数据的完整流程"""
        try:
            total_start_time = time.time()
            csv_file_path = f"finviz_screener_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            logger.info("开始Finviz筛选数据获取和存储流程")
            
            # 1. 下载数据
            if not await self.download_finviz_data(csv_file_path):
                return {
                    'success': False,
                    'message': 'Finviz数据下载失败',
                    'data': None
                }
            
            # 2. 检查CSV文件是否存在
            if not os.path.exists(csv_file_path):
                return {
                    'success': False,
                    'message': f'CSV文件不存在: {csv_file_path}',
                    'data': None
                }
            
            # 3. 创建表（如果不存在）
            if not self.create_table_if_not_exists():
                return {
                    'success': False,
                    'message': '创建Finviz表失败',
                    'data': None
                }
            
            # 4. 导入数据
            if not await self.import_csv_data_optimized(csv_file_path):
                return {
                    'success': False,
                    'message': '导入Finviz数据失败',
                    'data': None
                }
            
            # 5. 更新表统计信息
            with self.get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("ANALYZE finviz_screener")
                    connection.commit()
                    
                    # 获取最终统计信息
                    cursor.execute("SELECT COUNT(*) FROM finviz_screener")
                    total_rows = cursor.fetchone()[0]
                    
                    cursor.execute("SELECT MAX(created_at) FROM finviz_screener")
                    latest_update = cursor.fetchone()[0]
            
            # 6. 清理临时文件
            try:
                os.remove(csv_file_path)
                logger.info(f"已清理临时文件: {csv_file_path}")
            except:
                logger.warning(f"清理临时文件失败: {csv_file_path}")
            
            total_time = time.time() - total_start_time
            success_message = f"Finviz筛选数据获取成功，导入 {total_rows} 行数据，总耗时: {total_time:.2f}秒"
            logger.info(success_message)
            
            return {
                'success': True,
                'message': success_message,
                'data': {
                    'total_rows': total_rows,
                    'latest_update': latest_update.isoformat() if latest_update else None,
                    'execution_time': total_time
                }
            }
            
        except Exception as e:
            error_msg = f"Finviz筛选数据获取流程失败: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'message': error_msg,
                'data': None
            }
    
    def close_connections(self):
        """关闭所有连接"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("Finviz数据库连接已关闭")


# 全局服务实例
_finviz_service_instance = None


async def get_finviz_service() -> FinvizScreenerService:
    """获取Finviz筛选数据服务实例"""
    global _finviz_service_instance
    if _finviz_service_instance is None:
        _finviz_service_instance = FinvizScreenerService()
    return _finviz_service_instance
