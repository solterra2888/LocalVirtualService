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
    
    def _extract_market_from_ticker(self, full_ticker: str) -> str:
        """
        从完整的ticker中提取市场代码
        
        Args:
            full_ticker: 完整的ticker，如 'NVDA.US', '688256.SHG', '000977.SHE'
            
        Returns:
            市场代码 ('US', 'HK', 'CN')
        """
        if '.' not in full_ticker:
            return 'US'  # 默认为US
        
        _, exchange_suffix = full_ticker.rsplit('.', 1)
        exchange_suffix = exchange_suffix.upper()
        
        # 映射交易所代码到市场代码
        exchange_to_market = {
            'US': 'US', 'NASDAQ': 'US', 'NYSE': 'US', 'AMEX': 'US',
            'HK': 'HK', 'HKEX': 'HK',
            'CN': 'CN', 'SH': 'CN', 'SZ': 'CN',
            'SHG': 'CN', 'SHE': 'CN',  # EODHD交易所代码
        }
        
        return exchange_to_market.get(exchange_suffix, 'US')
    
    def validate_and_complete_tickers(
        self,
        tickers: List[str],
        market: str
    ) -> Dict[str, Any]:
        """
        验证并补全 ticker 代码
        
        Args:
            tickers: 股票代码列表，可以是简写（如 'NVDA'）或完整代码（如 'NVDA.US', '688256.SHG'）
            market: 默认市场代码 ('US', 'HK', 'CN')，仅用于没有后缀的ticker
            
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
        
        valid_tickers = []
        invalid_tickers = []
        
        conn = self.db_manager.get_connection()
        if not conn:
            logger.error("数据库连接失败")
            return {"valid": [], "invalid": [{"input": "all", "error": "Database connection failed"}]}
        
        try:
            cursor = conn.cursor()
            
            for ticker_input in tickers:
                ticker_clean = ticker_input.strip()
                if not ticker_clean:
                    continue
                
                # 检查ticker是否已包含交易所后缀
                if '.' in ticker_clean:
                    # 已包含后缀，直接使用（如 NVDA.US, 688256.SHG, 000977.SHE）
                    full_ticker = ticker_clean.upper()
                else:
                    # 没有后缀，使用提供的market参数添加后缀
                    full_ticker = f"{ticker_clean.upper()}.{market}"
                
                # 从 companies 表查询（仅使用 primaryticker 进行验证）
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
            return {
                "valid": [],
                "invalid": [
                    {
                        "input": ticker.strip(),
                        "full_ticker": ticker.strip().upper() if '.' in ticker else f"{ticker.strip().upper()}.{market}",
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
        allow_unverified: bool = True,
        defer_eod_backfill: bool = False,
    ) -> Dict[str, Any]:
        """
        创建主题-公司关联

        Args:
            theme_id: 主题ID
            tickers: 股票代码列表（可以是简写或完整代码，如 'NVDA' 或 'NVDA.US'）
            market: 默认市场代码（仅用于没有后缀的ticker）
            created_by: 创建者用户名
            allow_unverified: 是否允许创建未验证的关联
            defer_eod_backfill: 为 True 时跳过即时 EOD 派发（由调用方统一回填）。
                批量上传路径应传 True，避免每个 theme 各触发若干 fetch_single 任务
                堵满 data_fetching 队列，阻塞 generate_all_theme_reports。

        Returns:
            {
                "success": bool,
                "created_count": int,
                "skipped_count": int,
                "created_verified_count": int,   # 新增已验证 ticker 数
                "created_unverified_count": int, # 新增未验证 ticker 数
                "details": [...]
            }
        """
        if not tickers:
            return {
                "success": True,
                "created_count": 0,
                "skipped_count": 0,
                "created_verified_count": 0,
                "created_unverified_count": 0,
                "details": []
            }
        
        # 先验证 tickers（现在支持完整ticker和简写ticker）
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
            created_tickers = []  # 已验证的新增 ticker，commit 后派发批量 EOD 回填
            created_unverified = []  # 未验证的新增 ticker，批量任务扫不到，单独单 ticker 兜底
            
            # 处理有效的 tickers
            for ticker_info in validation_result["valid"]:
                full_ticker = ticker_info["full_ticker"]
                company_name = ticker_info["company_name"]
                
                # 从 full_ticker 中提取实际的市场代码
                ticker_market = self._extract_market_from_ticker(full_ticker)
                
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
                        ticker_market,
                        1.0,  # 默认权重
                        'manual',
                        True,  # 已验证
                        company_name,
                        created_by
                    ))
                    
                    if cursor.rowcount > 0:
                        created_count += 1
                        created_tickers.append(full_ticker)
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
                    
                    # 从 full_ticker 中提取实际的市场代码
                    ticker_market = self._extract_market_from_ticker(full_ticker)
                    
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
                            ticker_market,
                            1.0,
                            'manual',
                            False,  # 未验证
                            None,
                            created_by
                        ))
                        
                        if cursor.rowcount > 0:
                            created_count += 1
                            created_unverified.append(full_ticker)
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

            # 对新增 ticker 异步触发首次 EOD 历史回填，保证 Theme Report 的 1Y（回溯 364 日历日）
            # 有足够历史，避免走 partial 分支失真。失败绝不影响已提交的关联与返回值。
            #
            # 策略（避免任务风暴堵塞 data_fetching 队列）：
            # - 已验证 ticker：派发 1 个批量任务 fetch_eodhd_eod_stock_prices_task，它内部读取
            #   theme_company_associations 中所有 is_verified=true 的 ticker（含本次新增）一次并发拉取。
            #   1 个任务替代原先 N 个单 ticker 任务（历史教训：838 个单 ticker 任务曾把 Theme Report
            #   堵在队列后排队 4.5 分钟）。days_back=400 覆盖 1Y（364 日历日）+ 余量，只补日线。
            # - 未验证 ticker：批量任务只读 is_verified=true 扫不到它们，逐个单 ticker 兜底
            #   （通常为 companies 表缺失的冷门 ticker，数量极少，不构成风暴）。
            # - defer_eod_backfill=True 时整体跳过即时派发，由批量上传任务在全批结束后统一回填。
            if not defer_eod_backfill:
                if created_tickers:
                    try:
                        from backend.tasks.data_tasks import (
                            fetch_eodhd_eod_stock_prices_task,
                            fetch_single_ticker_eod_history_task,
                        )
                        # 少量新增（≤2）：逐个单 ticker 精准回填，避免触发批量任务重拉全部 theme ticker。
                        # 批量新增（≥3）：1 个批量任务，内部并发拉取所有 is_verified ticker，
                        #   避免 N 个单任务风暴（历史教训：838 个单任务曾把 Theme Report 堵排队 4.5 分钟）。
                        if len(created_tickers) <= 2:
                            for _tk in created_tickers:
                                try:
                                    _task = fetch_single_ticker_eod_history_task.apply_async(
                                        args=[_tk], kwargs={'days_back': None}
                                    )
                                    logger.info(f"📥 已派发首次 EOD 回填(少量精准): {_tk} (task_id={_task.id})")
                                except Exception as dispatch_err:
                                    logger.warning(f"⚠️ 派发 EOD 回填失败 {_tk}: {dispatch_err}")
                        else:
                            _task = fetch_eodhd_eod_stock_prices_task.apply_async(
                                kwargs={'days_back': 400, 'periods': ['d']}
                            )
                            logger.info(
                                f"📥 已派发批量首次 EOD 回填: 覆盖 {len(created_tickers)} 个已验证新 ticker "
                                f"(task_id={_task.id})"
                            )
                    except Exception as dispatch_err:
                        logger.warning(f"⚠️ 派发首次 EOD 回填失败: {dispatch_err}")

                if created_unverified:
                    try:
                        from backend.tasks.data_tasks import fetch_single_ticker_eod_history_task
                        for _tk in created_unverified:
                            try:
                                _task = fetch_single_ticker_eod_history_task.apply_async(
                                    args=[_tk], kwargs={'days_back': None}
                                )
                                logger.info(f"📥 已派发未验证 ticker 单独 EOD 回填: {_tk} (task_id={_task.id})")
                            except Exception as dispatch_err:
                                logger.warning(f"⚠️ 派发 EOD 回填失败 {_tk}: {dispatch_err}")
                    except Exception as import_err:
                        logger.warning(f"⚠️ 单 ticker EOD 回填任务导入失败: {import_err}")
            else:
                if created_tickers or created_unverified:
                    logger.info(
                        f"📥 EOD 回填已延迟（defer_eod_backfill=True）: "
                        f"{len(created_tickers)} 个已验证, {len(created_unverified)} 个未验证，"
                        f"由调用方统一派发"
                    )

            logger.info(f"关联创建完成: {created_count} 创建, {skipped_count} 跳过")

            return {
                "success": True,
                "created_count": created_count,
                "skipped_count": skipped_count,
                "created_verified_count": len(created_tickers),
                "created_unverified_count": len(created_unverified),
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
                "created_verified_count": 0,
                "created_unverified_count": 0,
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

