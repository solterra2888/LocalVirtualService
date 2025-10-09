# -*- coding: utf-8 -*-
"""
新闻LLM分析服务
用于自动分析新闻内容，调用Prompt模板和LLM
"""

import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime
from backend.database.db_manager import DatabaseManager
from backend.core.llm_framework import llm_service
from backend.utils.logger import get_logger

logger = get_logger("news_analysis_service")


class NewsAnalysisService:
    """新闻LLM分析服务"""
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        初始化服务
        
        Args:
            db_config: 数据库配置
        """
        self.db_manager = DatabaseManager(db_config)
        logger.info("新闻分析服务初始化完成")
    
    def get_unanalyzed_news(
        self, 
        limit: int = 10,
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        获取未分析的新闻
        
        Args:
            limit: 最大返回数量
            symbols: 筛选特定股票代码的新闻
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            未分析的新闻列表
        """
        conn = self.db_manager.get_connection()
        if not conn:
            logger.error("数据库连接失败")
            return []
        
        try:
            cursor = conn.cursor()
            
            # 构建查询条件
            where_clauses = ["(analysis_status = 'pending' OR analysis_status IS NULL)"]
            params = []
            
            # 添加股票代码筛选
            if symbols:
                where_clauses.append("""
                    id IN (
                        SELECT DISTINCT ns.news_id 
                        FROM news_symbols ns
                        JOIN news_symbols_dict nsd ON ns.symbol_id = nsd.id
                        WHERE nsd.symbol_code = ANY(%s)
                    )
                """)
                params.append(symbols)
            
            # 添加日期筛选
            if start_date:
                where_clauses.append("date >= %s")
                params.append(start_date)
            
            if end_date:
                where_clauses.append("date <= %s")
                params.append(end_date)
            
            where_sql = " AND ".join(where_clauses)
            
            # 查询新闻
            query = f"""
                SELECT id, date, title, content, link, source
                FROM news
                WHERE {where_sql}
                ORDER BY date DESC
                LIMIT %s
            """
            params.append(limit)
            
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            news_list = []
            for row in rows:
                news_item = {
                    'id': row[0],
                    'date': row[1].isoformat() if row[1] else None,
                    'title': row[2],
                    'content': row[3],
                    'link': row[4],
                    'source': row[5],
                    'symbols': []
                }
                
                # 获取相关股票代码
                cursor.execute("""
                    SELECT nsd.symbol_code
                    FROM news_symbols ns
                    JOIN news_symbols_dict nsd ON ns.symbol_id = nsd.id
                    WHERE ns.news_id = %s
                """, (row[0],))
                symbols = [s[0] for s in cursor.fetchall()]
                news_item['symbols'] = symbols
                
                news_list.append(news_item)
            
            cursor.close()
            conn.close()
            
            logger.info(f"获取到 {len(news_list)} 条未分析的新闻")
            return news_list
            
        except Exception as e:
            logger.error(f"获取未分析新闻失败: {str(e)}")
            if conn:
                conn.close()
            return []
    
    async def analyze_news(
        self, 
        news_id: int,
        template_code: str = "news_summary_v1",
        user_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        分析单条新闻
        
        Args:
            news_id: 新闻ID
            template_code: Prompt模板代码
            user_id: 用户ID（可选）
            
        Returns:
            分析结果
        """
        logger.info(f"开始分析新闻", news_id=news_id, template_code=template_code)
        
        conn = self.db_manager.get_connection()
        if not conn:
            return {"success": False, "error": "数据库连接失败"}
        
        try:
            # 1. 获取新闻详情
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, title, content, source
                FROM news
                WHERE id = %s
            """, (news_id,))
            
            row = cursor.fetchone()
            if not row:
                return {"success": False, "error": f"新闻不存在: {news_id}"}
            
            news_data = {
                'id': row[0],
                'title': row[1],
                'content': row[2],
                'source': row[3]
            }
            
            # 获取相关股票代码
            cursor.execute("""
                SELECT nsd.symbol_code
                FROM news_symbols ns
                JOIN news_symbols_dict nsd ON ns.symbol_id = nsd.id
                WHERE ns.news_id = %s
            """, (news_id,))
            symbols = [s[0] for s in cursor.fetchall()]
            symbols_str = ', '.join(symbols) if symbols else ''
            
            # 2. 更新状态为processing
            cursor.execute("""
                UPDATE news 
                SET analysis_status = 'processing'
                WHERE id = %s
            """, (news_id,))
            conn.commit()
            
            # 3. 准备变量数据
            variables = {
                "title": news_data['title'],
                "content": news_data['content'] or '',
                "symbols": symbols_str
            }
            
            # 根据模板添加额外变量
            if template_code == "news_summary_v1":
                variables["max_length"] = 100
            
            # 4. 调用LLM分析
            logger.info(f"调用LLM分析新闻", news_id=news_id, template_code=template_code)
            
            try:
                response = await llm_service.chat_completion_with_template(
                    template_code=template_code,
                    variables=variables,
                    user_id=user_id
                )
                
                # 5. 保存分析结果
                import json
                # 将分析结果封装为JSON对象
                analysis_data = {
                    "content": response.content,
                    "template_code": template_code,
                    "provider": response.provider if hasattr(response, 'provider') else None,
                    "model": response.model if hasattr(response, 'model') else None,
                    "tokens": response.usage.get('total_tokens') if hasattr(response, 'usage') and response.usage else None
                }
                
                cursor.execute("""
                    UPDATE news 
                    SET 
                        analysis_result = %s::jsonb,
                        analyzed_at = %s,
                        analysis_template_code = %s,
                        analysis_status = 'completed',
                        analysis_error = NULL,
                        is_analyzed = TRUE
                    WHERE id = %s
                """, (
                    json.dumps(analysis_data),
                    datetime.now(),
                    template_code,
                    news_id
                ))
                conn.commit()
                
                logger.info(f"新闻分析完成", news_id=news_id, success=True)
                
                return {
                    "success": True,
                    "news_id": news_id,
                    "analysis": response.content,
                    "parsed_data": response.parsed_data if hasattr(response, 'parsed_data') else None
                }
                
            except Exception as llm_error:
                # LLM调用失败
                error_msg = str(llm_error)
                logger.error(f"LLM分析失败", news_id=news_id, error=error_msg)
                
                # 更新状态为failed
                cursor.execute("""
                    UPDATE news 
                    SET 
                        analysis_status = 'failed',
                        analysis_error = %s
                    WHERE id = %s
                """, (error_msg, news_id))
                conn.commit()
                
                return {
                    "success": False,
                    "news_id": news_id,
                    "error": error_msg
                }
            
        except Exception as e:
            logger.error(f"分析新闻失败", news_id=news_id, exception=str(e))
            return {
                "success": False,
                "news_id": news_id,
                "error": str(e)
            }
        finally:
            if conn:
                conn.close()
    
    async def batch_analyze_news(
        self,
        limit: int = 10,
        template_code: str = "news_summary_v1",
        symbols: Optional[List[str]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        user_id: Optional[int] = None,
        use_concurrent: bool = False,
        max_concurrent: int = 3
    ) -> Dict[str, Any]:
        """
        批量分析新闻
        
        Args:
            limit: 最大分析数量
            template_code: Prompt模板代码
            symbols: 筛选特定股票代码
            start_date: 开始日期
            end_date: 结束日期
            user_id: 用户ID
            use_concurrent: 是否使用并发（注意LLM API限流）
            max_concurrent: 最大并发数
            
        Returns:
            批量分析结果统计
        """
        logger.info(f"开始批量分析新闻", 
                   limit=limit, 
                   template_code=template_code,
                   use_concurrent=use_concurrent)
        
        # 1. 获取未分析的新闻
        unanalyzed_news = self.get_unanalyzed_news(
            limit=limit,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date
        )
        
        if not unanalyzed_news:
            logger.info("没有需要分析的新闻")
            return {
                "total": 0,
                "success": 0,
                "failed": 0,
                "results": []
            }
        
        # 2. 批量分析
        results = []
        success_count = 0
        failed_count = 0
        
        if use_concurrent:
            # 并发分析（注意API限流）
            logger.info(f"使用并发模式分析，最大并发数: {max_concurrent}")
            
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def analyze_with_semaphore(news):
                async with semaphore:
                    return await self.analyze_news(
                        news_id=news['id'],
                        template_code=template_code,
                        user_id=user_id
                    )
            
            tasks = [analyze_with_semaphore(news) for news in unanalyzed_news]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # 统计结果
            for result in results:
                if isinstance(result, dict) and result.get('success'):
                    success_count += 1
                else:
                    failed_count += 1
        else:
            # 顺序分析（更安全）
            logger.info("使用顺序模式分析")
            
            for news in unanalyzed_news:
                try:
                    result = await self.analyze_news(
                        news_id=news['id'],
                        template_code=template_code,
                        user_id=user_id
                    )
                    results.append(result)
                    
                    if result.get('success'):
                        success_count += 1
                    else:
                        failed_count += 1
                        
                    # 添加延迟，避免API限流
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"分析新闻异常", news_id=news['id'], error=str(e))
                    failed_count += 1
                    results.append({
                        "success": False,
                        "news_id": news['id'],
                        "error": str(e)
                    })
        
        summary = {
            "total": len(unanalyzed_news),
            "success": success_count,
            "failed": failed_count,
            "template_code": template_code,
            "results": results
        }
        
        logger.info(f"批量分析完成", **{k: v for k, v in summary.items() if k != 'results'})
        return summary
    
    def get_analysis_statistics(self) -> Dict[str, Any]:
        """
        获取分析统计信息
        
        Returns:
            统计信息
        """
        conn = self.db_manager.get_connection()
        if not conn:
            return {}
        
        try:
            cursor = conn.cursor()
            
            # 统计各状态的新闻数量
            cursor.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE analysis_status = 'pending' OR analysis_status IS NULL) as pending,
                    COUNT(*) FILTER (WHERE analysis_status = 'processing') as processing,
                    COUNT(*) FILTER (WHERE analysis_status = 'completed') as completed,
                    COUNT(*) FILTER (WHERE analysis_status = 'failed') as failed,
                    COUNT(*) as total
                FROM news
            """)
            
            row = cursor.fetchone()
            stats = {
                "pending": row[0],
                "processing": row[1],
                "completed": row[2],
                "failed": row[3],
                "total": row[4]
            }
            
            cursor.close()
            conn.close()
            
            return stats
            
        except Exception as e:
            logger.error(f"获取统计信息失败: {str(e)}")
            if conn:
                conn.close()
            return {}


