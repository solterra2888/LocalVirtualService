# -*- coding: utf-8 -*-
"""
新闻LLM分析服务
用于自动分析新闻内容，调用Prompt模板和LLM
支持基于向量检索的投资主题匹配
"""

import asyncio
import json
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
        # 内容审查失败统计
        self.content_filter_stats = {
            'total': 0,
            'input_blocked': 0,
            'output_blocked': 0,
            'all_providers_failed': 0
        }
        # 初始化投资主题向量服务
        self.theme_embedding_service = None
        self._init_theme_service(db_config)
        
        logger.info("新闻分析服务初始化完成")
    
    def _init_theme_service(self, db_config: Dict[str, Any]):
        """初始化主题向量服务"""
        try:
            from backend.services.investment_theme_embedding_service import InvestmentThemeEmbeddingService
            self.theme_embedding_service = InvestmentThemeEmbeddingService(db_config)
            logger.info("投资主题向量服务已启用")
        except Exception as e:
            logger.warning(f"投资主题向量服务初始化失败: {str(e)}, 将不支持主题匹配功能")
    
    def _is_content_filter_error(self, error_msg: str) -> Dict[str, Any]:
        """
        检测是否为内容审查错误
        
        Returns:
            包含错误类型信息的字典
        """
        error_lower = error_msg.lower()
        
        result = {
            'is_filter_error': False,
            'filter_type': None,
            'retryable': False
        }
        
        # 检测各种内容审查错误
        if 'data_inspection_failed' in error_lower or 'inappropriate content' in error_lower:
            result['is_filter_error'] = True
            if 'input' in error_lower:
                result['filter_type'] = 'input'
                result['retryable'] = False  # 输入内容问题，重试也无效
            elif 'output' in error_lower:
                result['filter_type'] = 'output'
                result['retryable'] = True  # 输出内容问题，可能重试成功
        
        elif '不安全或敏感内容' in error_msg or 'contentFilter' in error_msg:
            result['is_filter_error'] = True
            result['filter_type'] = 'content_safety'
            result['retryable'] = False
        
        elif 'all providers failed' in error_lower:
            result['is_filter_error'] = True
            result['filter_type'] = 'all_failed'
            result['retryable'] = False
        
        return result
    
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
        user_id: Optional[int] = None,
        enable_theme_matching: bool = True
    ) -> Dict[str, Any]:
        """
        分析单条新闻
        
        Args:
            news_id: 新闻ID
            template_code: Prompt模板代码
            user_id: 用户ID（可选）
            enable_theme_matching: 是否启用投资主题匹配（基于向量检索）
            
        Returns:
            分析结果，包含主题匹配信息
        """
        logger.info(f"开始分析新闻", news_id=news_id, template_code=template_code, enable_theme_matching=enable_theme_matching)
        
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
            
            # 4. 调用LLM分析（带智能重试和模板切换）
            logger.info(f"调用LLM分析新闻", news_id=news_id, template_code=template_code)
            
            max_retries = 2  # 最多重试2次
            retry_delay = 1  # 重试延迟（秒）
            current_template = template_code
            
            for attempt in range(max_retries + 1):
                try:
                    if attempt > 0:
                        logger.info(f"重试分析新闻", news_id=news_id, attempt=attempt, template=current_template)
                        await asyncio.sleep(retry_delay * attempt)  # 递增延迟
                    
                    response = await llm_service.chat_completion_with_template(
                        template_code=current_template,
                        variables=variables,
                        user_id=user_id
                    )
                    
                    # 5. 保存分析结果（支持多模板合并）
                    # 🔥 新逻辑：先读取现有的 analysis_result，然后合并新结果
                    cursor.execute("""
                        SELECT analysis_result, analysis_template_code
                        FROM news
                        WHERE id = %s
                    """, (news_id,))
                    existing_row = cursor.fetchone()
                    existing_result = existing_row[0] if existing_row and existing_row[0] else {}
                    existing_templates = existing_row[1] if existing_row and existing_row[1] else None
                    
                    # 如果 existing_result 是旧格式（非字典），转换为新格式
                    if existing_result and not isinstance(existing_result, dict):
                        existing_result = {}
                    
                    # 如果 existing_result 是旧的单模板格式（有 'content' 字段），转换为多模板格式
                    if 'content' in existing_result and 'template_code' in existing_result:
                        old_template = existing_result.get('template_code', 'unknown')
                        old_data = existing_result.copy()
                        existing_result = {old_template: old_data}
                    
                    # 构建当前模板的分析数据
                    current_analysis_data = {
                        "content": response.content,
                        "template_code": template_code,
                        "provider": response.provider if hasattr(response, 'provider') else None,
                        "model": response.model if hasattr(response, 'model') else None,
                        "tokens": response.usage.get('total_tokens') if hasattr(response, 'usage') and response.usage else None,
                        "retry_count": attempt,
                        "analyzed_at": datetime.now().isoformat()
                    }
                    
                    # 🔥 合并：将当前模板的结果添加到字典中
                    existing_result[template_code] = current_analysis_data
                    
                    # 🔥 更新 analysis_template_code 为所有已分析的模板列表
                    analyzed_templates = list(existing_result.keys())
                    template_code_str = ','.join(analyzed_templates)  # 存储为逗号分隔的字符串
                    
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
                        json.dumps(existing_result),
                        datetime.now(),
                        template_code_str,
                        news_id
                    ))
                    conn.commit()
                    
                    logger.info(f"新闻分析完成", news_id=news_id, success=True, retry_count=attempt)
                    
                    # 🔥 新增：投资主题匹配（基于向量检索）
                    theme_matches = []
                    if enable_theme_matching and self.theme_embedding_service:
                        try:
                            # 构建新闻文本（标题 + 内容摘要）
                            news_text = news_data['title']
                            if news_data['content']:
                                # 取前500个字符作为摘要
                                content_preview = news_data['content'][:500]
                                news_text = f"{news_data['title']}. {content_preview}"
                            
                            # 从股票代码提取市场信息
                            market = self.theme_embedding_service.extract_market_from_symbols(symbols)
                            
                            logger.info(f"开始投资主题匹配", news_id=news_id, market=market)
                            
                            # 执行向量检索
                            theme_matches = await self.theme_embedding_service.match_news_to_themes(
                                news_text=news_text,
                                news_id=news_id,
                                market=market,
                                top_k=5,
                                min_similarity=0.60,
                                save_to_db=True  # 保存匹配结果到数据库
                            )
                            
                            if theme_matches:
                                logger.info(
                                    f"投资主题匹配完成", 
                                    news_id=news_id, 
                                    matches_count=len(theme_matches),
                                    top_theme=theme_matches[0]['theme_name'] if theme_matches else None
                                )
                            else:
                                logger.info(f"未找到匹配的投资主题", news_id=news_id)
                                
                        except Exception as theme_error:
                            logger.error(f"投资主题匹配失败", news_id=news_id, error=str(theme_error))
                            # 主题匹配失败不影响整体分析结果
                    
                    return {
                        "success": True,
                        "news_id": news_id,
                        "analysis": response.content,
                        "parsed_data": response.parsed_data if hasattr(response, 'parsed_data') else None,
                        "retry_count": attempt,
                        "theme_matches": theme_matches  # 返回匹配的主题
                    }
                    
                except Exception as llm_error:
                    error_msg = str(llm_error)
                    
                    # 检测是否为内容审查错误
                    filter_info = self._is_content_filter_error(error_msg)
                    
                    if filter_info['is_filter_error']:
                        # 更新统计
                        self.content_filter_stats['total'] += 1
                        if filter_info['filter_type'] == 'input':
                            self.content_filter_stats['input_blocked'] += 1
                        elif filter_info['filter_type'] == 'output':
                            self.content_filter_stats['output_blocked'] += 1
                        elif filter_info['filter_type'] == 'all_failed':
                            self.content_filter_stats['all_providers_failed'] += 1
                        
                        logger.warning(
                            f"内容审查失败",
                            news_id=news_id,
                            filter_type=filter_info['filter_type'],
                            attempt=attempt,
                            template=current_template
                        )
                        
                        # 智能模板切换策略
                        if attempt == 1 and current_template == 'news_summary_v1':
                            # 第二次尝试时切换到保守模板
                            current_template = 'news_summary_safe_v1'
                            logger.info(
                                f"切换到保守模板",
                                news_id=news_id,
                                new_template=current_template
                            )
                            continue  # 继续重试，不退出循环
                        
                        # 如果不可重试或已达到最大重试次数，直接失败
                        if not filter_info['retryable'] or attempt >= max_retries:
                            break
                    else:
                        # 非内容审查错误，记录并判断是否重试
                        logger.error(f"LLM分析失败", news_id=news_id, error=error_msg, attempt=attempt)
                        
                        # 如果是最后一次尝试，退出循环
                        if attempt >= max_retries:
                            break
                    
                    # 保存最后一次的错误信息
                    last_error = error_msg
            
            # 所有重试都失败了
            logger.error(f"新闻分析最终失败", news_id=news_id, error=last_error, total_attempts=max_retries + 1)
            
            # 保存错误信息到数据库
            error_detail = {
                'error': last_error,
                'filter_info': self._is_content_filter_error(last_error),
                'total_attempts': max_retries + 1
            }
            
            cursor.execute("""
                UPDATE news 
                SET 
                    analysis_status = 'failed',
                    analysis_error = %s
                WHERE id = %s
            """, (json.dumps(error_detail), news_id))
            conn.commit()
            
            return {
                "success": False,
                "news_id": news_id,
                "error": last_error,
                "filter_info": error_detail['filter_info']
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
            
            for i, news in enumerate(unanalyzed_news):
                try:
                    # 记录进度
                    progress = int((i + 1) / len(unanalyzed_news) * 100)
                    logger.info(f"🔄 分析进度: {i+1}/{len(unanalyzed_news)} ({progress}%) - 新闻ID: {news['id']}")
                    
                    result = await self.analyze_news(
                        news_id=news['id'],
                        template_code=template_code,
                        user_id=user_id
                    )
                    results.append(result)
                    
                    if result.get('success'):
                        success_count += 1
                        logger.info(f"✅ 新闻 {news['id']} 分析成功")
                    else:
                        failed_count += 1
                        logger.warning(f"❌ 新闻 {news['id']} 分析失败: {result.get('error', 'unknown')}")
                        
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
        
        # 记录详细的批量分析完成摘要
        success_rate = (summary['success'] / summary['total'] * 100) if summary['total'] > 0 else 0
        
        logger.info(f"📊 [news_analysis_service] 批量分析完成:")
        logger.info(f"   📈 处理统计: 总计 {summary['total']} 条新闻")
        logger.info(f"   ✅ 成功分析: {summary['success']} 条 ({success_rate:.1f}%)")
        logger.info(f"   ❌ 分析失败: {summary['failed']} 条")
        logger.info(f"   🔧 使用模板: {summary['template_code']}")
        
        # 如果有失败的分析，记录失败原因统计
        if summary['failed'] > 0:
            failed_results = [r for r in summary['results'] if not r.get('success', False)]
            error_types = {}
            for failed_result in failed_results:
                error_msg = failed_result.get('error', 'unknown')
                if 'content_filter' in error_msg.lower() or 'inappropriate' in error_msg.lower():
                    error_types['内容审查'] = error_types.get('内容审查', 0) + 1
                elif 'timeout' in error_msg.lower():
                    error_types['超时'] = error_types.get('超时', 0) + 1
                elif 'rate limit' in error_msg.lower():
                    error_types['限流'] = error_types.get('限流', 0) + 1
                elif 'all providers failed' in error_msg.lower():
                    error_types['所有提供商失败'] = error_types.get('所有提供商失败', 0) + 1
                elif 'data_inspection_failed' in error_msg.lower():
                    error_types['数据检查失败'] = error_types.get('数据检查失败', 0) + 1
                else:
                    error_types['其他'] = error_types.get('其他', 0) + 1
            
            if error_types:
                logger.info(f"   🔍 失败原因统计: {error_types}")
                
            # 将失败原因统计添加到summary中
            summary['error_types'] = error_types
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
                "total": row[4],
                "content_filter_stats": self.content_filter_stats.copy()
            }
            
            # 获取失败新闻的错误类型统计
            cursor.execute("""
                SELECT 
                    analysis_error,
                    COUNT(*) as count
                FROM news
                WHERE analysis_status = 'failed'
                  AND analysis_error IS NOT NULL
                GROUP BY analysis_error
                ORDER BY count DESC
                LIMIT 10
            """)
            
            error_stats = []
            for row in cursor.fetchall():
                try:
                    error_data = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                    if isinstance(error_data, dict):
                        filter_type = error_data.get('filter_info', {}).get('filter_type', 'unknown')
                    else:
                        filter_type = 'legacy_error'
                    error_stats.append({
                        'filter_type': filter_type,
                        'count': row[1]
                    })
                except:
                    error_stats.append({
                        'filter_type': 'parse_error',
                        'count': row[1]
                    })
            
            stats['failed_error_types'] = error_stats
            
            cursor.close()
            conn.close()
            
            return stats
            
        except Exception as e:
            logger.error(f"获取统计信息失败: {str(e)}")
            if conn:
                conn.close()
            return {}
    
    def get_failed_news_for_retry(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        获取可重试的失败新闻
        
        Args:
            limit: 最大返回数量
            
        Returns:
            失败新闻列表
        """
        conn = self.db_manager.get_connection()
        if not conn:
            return []
        
        try:
            cursor = conn.cursor()
            
            query = """
                SELECT id, date, title, analysis_error
                FROM news
                WHERE analysis_status = 'failed'
                ORDER BY date DESC
                LIMIT %s
            """
            
            cursor.execute(query, (limit,))
            rows = cursor.fetchall()
            
            failed_news = []
            for row in rows:
                news_item = {
                    'id': row[0],
                    'date': row[1].isoformat() if row[1] else None,
                    'title': row[2],
                    'error': row[3]
                }
                
                # 解析错误信息，判断是否可重试
                try:
                    if isinstance(row[3], str):
                        error_data = json.loads(row[3])
                        filter_info = error_data.get('filter_info', {})
                        news_item['retryable'] = filter_info.get('retryable', False)
                        news_item['filter_type'] = filter_info.get('filter_type', 'unknown')
                    else:
                        news_item['retryable'] = False
                        news_item['filter_type'] = 'unknown'
                except:
                    news_item['retryable'] = False
                    news_item['filter_type'] = 'parse_error'
                
                failed_news.append(news_item)
            
            cursor.close()
            conn.close()
            
            logger.info(f"获取到 {len(failed_news)} 条失败的新闻")
            return failed_news
            
        except Exception as e:
            logger.error(f"获取失败新闻失败: {str(e)}")
            if conn:
                conn.close()
            return []


