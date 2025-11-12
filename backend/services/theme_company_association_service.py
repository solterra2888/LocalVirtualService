# -*- coding: utf-8 -*-
"""
主题-公司关联服务
用于管理投资主题与股票代码的关联关系
"""

from typing import Dict, List, Optional, Any, Tuple
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger

logger = get_logger("theme_company_association_service")


class ThemeCompanyAssociationService:
    """主题-公司关联服务"""
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        初始化服务
        
        Args:
            db_config: 数据库配置
        """
        self.db_manager = DatabaseManager(db_config)
        logger.info("主题-公司关联服务初始化完成")
    
    def validate_and_complete_tickers(
        self,
        tickers: List[str],
        market: str
    ) -> Dict[str, Any]:
        """
        验证并补全 ticker 代码
        
        Args:
            tickers: 股票代码列表（简写），如 ['NVDA', 'AMD', 'INTC']
            market: 市场代码 ('US', 'HK', 'CN')
            
        Returns:
            {
                "valid": [
                    {
                        "input": "NVDA",
                        "full_ticker": "NVDA.US",
                        "company_name": "NVIDIA Corporation",
                        "sector": "Technology",
                        "exists": True
                    },
                    ...
                ],
                "invalid": [
                    {
                        "input": "XXXX",
                        "error": "Ticker not found in companies table"
                    }
                ]
            }
        """
        if not tickers:
            return {"valid": [], "invalid": []}
        
        if not market or market not in ['US', 'HK', 'CN']:
            return {
                "valid": [],
                "invalid": [{"input": "all", "error": f"Invalid market: {market}"}]
            }
        
        # 确定市场后缀
        suffix = f".{market}"
        
        valid_tickers = []
        invalid_tickers = []
        
        conn = self.db_manager.get_connection()
        if not conn:
            logger.error("数据库连接失败")
            return {"valid": [], "invalid": [{"input": "all", "error": "Database connection failed"}]}
        
        try:
            cursor = conn.cursor()
            
            for ticker_input in tickers:
                ticker_clean = ticker_input.strip().upper()
                if not ticker_clean:
                    continue
                
                # 补全完整的 ticker
                full_ticker = f"{ticker_clean}{suffix}"
                
                # 从 companies 表查询（仅使用 primaryticker 进行验证，避免交易所歧义）
                cursor.execute("""
                    SELECT 
                        name,
                        sector,
                        industry,
                        gicsector
                    FROM companies
                    WHERE primaryticker = %s
                    LIMIT 1
                """, (full_ticker,))
                
                result = cursor.fetchone()
                
                if result:
                    valid_tickers.append({
                        "input": ticker_clean,
                        "full_ticker": full_ticker,
                        "company_name": result[0] or ticker_clean,
                        "sector": result[1] or "Unknown",
                        "industry": result[2],
                        "gicsector": result[3],
                        "exists": True
                    })
                    logger.debug(f"✅ Ticker 验证成功: {full_ticker} - {result[0]}")
                else:
                    # Ticker 不存在于 companies 表
                    invalid_tickers.append({
                        "input": ticker_clean,
                        "full_ticker": full_ticker,
                        "error": "Ticker primaryticker not found in companies table",
                        "exists": False
                    })
                    logger.warning(f"⚠️ Ticker 未找到: {full_ticker}")
            
            cursor.close()
            conn.close()
            
            logger.info(f"Ticker 验证完成: {len(valid_tickers)} 有效, {len(invalid_tickers)} 无效")
            
            return {
                "valid": valid_tickers,
                "invalid": invalid_tickers
            }
            
        except Exception as e:
            logger.error(f"Ticker 验证失败: {str(e)}")
            if conn:
                conn.close()
            # 返回所有 ticker 为无效，并包含 full_ticker
            suffix = f".{market}"
            return {
                "valid": [],
                "invalid": [
                    {
                        "input": ticker.strip().upper(),
                        "full_ticker": f"{ticker.strip().upper()}{suffix}",
                        "error": f"Validation error: {str(e)}",
                        "exists": False
                    }
                    for ticker in tickers if ticker.strip()
                ]
            }
    
    def create_associations(
        self,
        theme_id: str,
        tickers: List[str],
        market: str,
        created_by: str,
        allow_unverified: bool = True
    ) -> Dict[str, Any]:
        """
        创建主题-公司关联
        
        Args:
            theme_id: 主题ID
            tickers: 股票代码列表（简写）
            market: 市场代码
            created_by: 创建者用户名
            allow_unverified: 是否允许创建未验证的关联
            
        Returns:
            {
                "success": bool,
                "created_count": int,
                "skipped_count": int,
                "details": [...]
            }
        """
        if not tickers:
            return {
                "success": True,
                "created_count": 0,
                "skipped_count": 0,
                "details": []
            }
        
        # 先验证 tickers
        validation_result = self.validate_and_complete_tickers(tickers, market)
        
        # 检查验证结果
        if not validation_result or ("valid" not in validation_result and "invalid" not in validation_result):
            logger.error(f"Ticker 验证返回无效结果: {validation_result}")
            return {
                "success": False,
                "created_count": 0,
                "skipped_count": len(tickers),
                "error": "Ticker validation failed"
            }
        
        conn = self.db_manager.get_connection()
        if not conn:
            logger.error("数据库连接失败")
            return {
                "success": False,
                "created_count": 0,
                "skipped_count": len(tickers),
                "error": "Database connection failed"
            }
        
        try:
            cursor = conn.cursor()
            
            created_count = 0
            skipped_count = 0
            details = []
            
            # 处理有效的 tickers
            for ticker_info in validation_result["valid"]:
                full_ticker = ticker_info["full_ticker"]
                company_name = ticker_info["company_name"]
                
                try:
                    cursor.execute("""
                        INSERT INTO theme_company_associations (
                            theme_id, ticker, market, 
                            association_weight, source, 
                            is_verified, company_name, 
                            created_by
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (theme_id, ticker) DO NOTHING
                    """, (
                        theme_id, 
                        full_ticker, 
                        market,
                        1.0,  # 默认权重
                        'manual',
                        True,  # 已验证
                        company_name,
                        created_by
                    ))
                    
                    if cursor.rowcount > 0:
                        created_count += 1
                        details.append({
                            "ticker": full_ticker,
                            "status": "created",
                            "verified": True
                        })
                        logger.debug(f"✅ 创建关联: {theme_id} - {full_ticker}")
                    else:
                        skipped_count += 1
                        details.append({
                            "ticker": full_ticker,
                            "status": "already_exists",
                            "verified": True
                        })
                        logger.debug(f"⚠️ 关联已存在: {theme_id} - {full_ticker}")
                        
                except Exception as e:
                    logger.error(f"插入关联失败: {full_ticker} - {str(e)}")
                    skipped_count += 1
                    details.append({
                        "ticker": full_ticker,
                        "status": "error",
                        "error": str(e)
                    })
            
            # 处理无效的 tickers（如果允许创建未验证的关联）
            if allow_unverified:
                for ticker_info in validation_result["invalid"]:
                    full_ticker = ticker_info["full_ticker"]
                    
                    try:
                        cursor.execute("""
                            INSERT INTO theme_company_associations (
                                theme_id, ticker, market, 
                                association_weight, source, 
                                is_verified, company_name, 
                                created_by
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (theme_id, ticker) DO NOTHING
                        """, (
                            theme_id, 
                            full_ticker, 
                            market,
                            1.0,
                            'manual',
                            False,  # 未验证
                            None,
                            created_by
                        ))
                        
                        if cursor.rowcount > 0:
                            created_count += 1
                            details.append({
                                "ticker": full_ticker,
                                "status": "created",
                                "verified": False,
                                "warning": "Ticker not found in companies table"
                            })
                            logger.warning(f"⚠️ 创建未验证关联: {theme_id} - {full_ticker}")
                        else:
                            skipped_count += 1
                            details.append({
                                "ticker": full_ticker,
                                "status": "already_exists",
                                "verified": False
                            })
                            
                    except Exception as e:
                        logger.error(f"插入未验证关联失败: {full_ticker} - {str(e)}")
                        skipped_count += 1
                        details.append({
                            "ticker": full_ticker,
                            "status": "error",
                            "error": str(e)
                        })
            else:
                # 不允许创建未验证关联，直接跳过
                skipped_count += len(validation_result["invalid"])
                for ticker_info in validation_result["invalid"]:
                    details.append({
                        "ticker": ticker_info["full_ticker"],
                        "status": "skipped",
                        "reason": "Ticker not verified and allow_unverified=False"
                    })
            
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"关联创建完成: {created_count} 创建, {skipped_count} 跳过")
            
            return {
                "success": True,
                "created_count": created_count,
                "skipped_count": skipped_count,
                "details": details
            }
            
        except Exception as e:
            logger.error(f"创建关联失败: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return {
                "success": False,
                "created_count": 0,
                "skipped_count": len(tickers),
                "error": str(e)
            }
    
    def get_associations_by_theme(self, theme_id: str) -> List[Dict[str, Any]]:
        """
        获取主题关联的所有公司
        
        Args:
            theme_id: 主题ID
            
        Returns:
            关联公司列表
        """
        conn = self.db_manager.get_connection()
        if not conn:
            logger.error("数据库连接失败")
            return []
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    ticker,
                    market,
                    association_weight,
                    is_verified,
                    company_name,
                    source,
                    created_at
                FROM theme_company_associations
                WHERE theme_id = %s
                ORDER BY association_weight DESC, created_at DESC
            """, (theme_id,))
            
            associations = []
            for row in cursor.fetchall():
                associations.append({
                    "ticker": row[0],
                    "market": row[1],
                    "association_weight": float(row[2]) if row[2] else 1.0,
                    "is_verified": row[3],
                    "company_name": row[4],
                    "source": row[5],
                    "created_at": row[6].isoformat() if row[6] else None
                })
            
            cursor.close()
            conn.close()
            
            logger.debug(f"查询到 {len(associations)} 个关联: theme_id={theme_id}")
            return associations
            
        except Exception as e:
            logger.error(f"查询关联失败: {str(e)}")
            if conn:
                conn.close()
            return []
    
    def delete_associations_by_theme(self, theme_id: str) -> int:
        """
        删除主题的所有关联
        
        Args:
            theme_id: 主题ID
            
        Returns:
            删除的关联数量
        """
        conn = self.db_manager.get_connection()
        if not conn:
            logger.error("数据库连接失败")
            return 0
        
        try:
            cursor = conn.cursor()
            cursor.execute("""
                DELETE FROM theme_company_associations
                WHERE theme_id = %s
            """, (theme_id,))
            
            deleted_count = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"删除关联: theme_id={theme_id}, count={deleted_count}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"删除关联失败: {str(e)}")
            if conn:
                conn.rollback()
                conn.close()
            return 0

