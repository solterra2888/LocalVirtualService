# -*- coding: utf-8 -*-
"""
新闻分析相关Celery任务
用于大批量新闻的并发LLM分析处理
"""

import asyncio
from celery import current_task
from backend.celery_app import app
from backend.utils.database_config import get_database_config
from backend.utils.logger import info, error


@app.task(bind=True, name='analyze_news_batch', queue='ai_processing')
def analyze_news_batch_task(
    self, 
    news_ids: list,
    template_code: str = "news_summary_v1",
    user_id: int = None
):
    """
    Celery异步新闻批量分析任务
    
    适用场景：
    - 大批量新闻（100条以上）需要并发分析
    - 需要分布式处理的场景
    - 需要任务状态跟踪和重试机制
    
    Args:
        news_ids: 新闻ID列表
        template_code: Prompt模板代码
        user_id: 用户ID（可选）
    """
    
    def update_progress(status=None, progress=None, current_news_id=None, log_message=None, **kwargs):
        """更新任务进度到Redis"""
        try:
            meta = {
                'status': status or 'processing',
                'progress': progress or 0,
                'current_news_id': current_news_id,
                'message': log_message or '',
                'news_analyzed': kwargs.get('news_analyzed', 0),
                'news_failed': kwargs.get('news_failed', 0),
                'total_news': len(news_ids)
            }
            
            current_task.update_state(
                state='PROGRESS',
                meta=meta
            )
            
            if log_message:
                info(f"Task {self.request.id}: {log_message}", source="celery_news_task")
                
        except Exception as e:
            error(f"更新任务状态失败: {str(e)}", source="celery_news_task")
    
    try:
        info(f"开始执行新闻批量分析任务 {self.request.id}, 新闻数量: {len(news_ids)}", 
             source="celery_news_task")
        
        # 导入新闻分析服务
        from backend.services.news_analysis_service import NewsAnalysisService
        
        db_config = get_database_config()
        news_analysis_service = NewsAnalysisService(db_config)
        
        # 更新初始状态
        update_progress(
            status="initializing",
            progress=0,
            log_message=f"正在初始化新闻分析任务，共{len(news_ids)}条新闻..."
        )
        
        # 执行异步分析
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        news_analyzed = 0
        news_failed = 0
        results = []
        
        try:
            for i, news_id in enumerate(news_ids):
                try:
                    # 分析单条新闻
                    result = loop.run_until_complete(
                        news_analysis_service.analyze_news(
                            news_id=news_id,
                            template_code=template_code,
                            user_id=user_id
                        )
                    )
                    
                    results.append(result)
                    
                    if result.get('success'):
                        news_analyzed += 1
                    else:
                        news_failed += 1
                    
                    # 更新进度
                    progress = int((i + 1) / len(news_ids) * 100)
                    update_progress(
                        status="processing",
                        progress=progress,
                        current_news_id=news_id,
                        log_message=f"正在分析新闻 {i+1}/{len(news_ids)}",
                        news_analyzed=news_analyzed,
                        news_failed=news_failed
                    )
                    
                    # 添加短暂延迟，避免API限流
                    loop.run_until_complete(asyncio.sleep(0.3))
                    
                except Exception as e:
                    error(f"分析新闻 {news_id} 失败: {str(e)}", source="celery_news_task")
                    news_failed += 1
                    results.append({
                        "success": False,
                        "news_id": news_id,
                        "error": str(e)
                    })
                    
        finally:
            loop.close()
        
        # 返回成功结果
        final_result = {
            'status': 'completed',
            'total_news': len(news_ids),
            'news_analyzed': news_analyzed,
            'news_failed': news_failed,
            'template_code': template_code,
            'results': results
        }
        
        # 记录详细的统计摘要
        success_rate = (final_result['news_analyzed'] / final_result['total_news'] * 100) if final_result['total_news'] > 0 else 0
        
        summary_info = {
            'status': final_result['status'],
            'total_news': final_result['total_news'],
            'news_analyzed': final_result['news_analyzed'],
            'news_failed': final_result['news_failed'],
            'success_rate': f"{success_rate:.1f}%",
            'template_code': final_result['template_code']
        }
        
        # 格式化输出更易读的摘要
        info(f"📊 [celery_news_task] 任务 {self.request.id} 执行完成:", source="celery_news_task")
        info(f"   📈 处理统计: 总计 {final_result['total_news']} 条新闻", source="celery_news_task")
        info(f"   ✅ 成功分析: {final_result['news_analyzed']} 条 ({success_rate:.1f}%)", source="celery_news_task")
        info(f"   ❌ 分析失败: {final_result['news_failed']} 条", source="celery_news_task")
        info(f"   🔧 使用模板: {final_result['template_code']}", source="celery_news_task")
        return final_result
        
    except Exception as e:
        error_msg = f"任务执行失败: {str(e)}"
        error(error_msg, source="celery_news_task")
        
        # 更新任务状态为失败
        update_progress(
            status="failed",
            progress=0,
            log_message=error_msg
        )
        
        # 抛出异常以便Celery重试机制处理
        raise


@app.task(bind=True, name='analyze_news_by_ids', queue='ai_processing')
def analyze_news_by_ids_task(
    self,
    news_ids: list,
    template_code: str = "news_summary_v1",
    max_concurrent: int = 8,
    enable_theme_matching: bool = True
):
    """
    🔥 新任务：分析指定ID的新闻列表（解决时序问题的最佳方案）
    
    适用场景：
    - 新闻插入后立即分析指定的新闻
    - 避免查询未分析新闻时的时序问题
    - 精确控制要分析的新闻
    
    Args:
        news_ids: 要分析的新闻ID列表
        template_code: Prompt模板代码
        max_concurrent: 最大并发数
    """
    
    try:
        info(f"开始执行指定ID新闻分析任务 {self.request.id}, 新闻数量: {len(news_ids)}", source="celery_news_task")
        
        if not news_ids:
            info("新闻ID列表为空，无需分析", source="celery_news_task")
            return {
                'total': 0,
                'success': 0,
                'failed': 0,
                'template_code': template_code,
                'results': []
            }
        
        from backend.services.news_analysis_service import NewsAnalysisService
        
        db_config = get_database_config()
        news_analysis_service = NewsAnalysisService(db_config)
        
        # 执行异步批量分析
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        results = []
        success_count = 0
        failed_count = 0
        
        try:
            # 使用并发分析指定ID的新闻
            semaphore = asyncio.Semaphore(max_concurrent)
            
            async def analyze_with_semaphore(news_id):
                async with semaphore:
                    return await news_analysis_service.analyze_news(
                        news_id=news_id,
                        template_code=template_code,
                        enable_theme_matching=enable_theme_matching
                    )
            
            tasks = [analyze_with_semaphore(news_id) for news_id in news_ids]
            results = loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            
            # 统计结果
            for result in results:
                if isinstance(result, dict) and result.get('success'):
                    success_count += 1
                else:
                    failed_count += 1
                    
        finally:
            loop.close()
        
        # 格式化输出更易读的摘要
        total = len(news_ids)
        success_rate = (success_count / total * 100) if total > 0 else 0
        
        info(f"📊 [celery_news_task] 任务 {self.request.id} 执行完成:", source="celery_news_task")
        info(f"   📈 处理统计: 总计 {total} 条新闻", source="celery_news_task")
        info(f"   ✅ 成功分析: {success_count} 条 ({success_rate:.1f}%)", source="celery_news_task")
        info(f"   ❌ 分析失败: {failed_count} 条", source="celery_news_task")
        info(f"   🔧 使用模板: {template_code}", source="celery_news_task")
        
        return {
            'total': total,
            'success': success_count,
            'failed': failed_count,
            'template_code': template_code,
            'results': results
        }
        
    except Exception as e:
        error_msg = f"任务执行失败: {str(e)}"
        error(error_msg, source="celery_news_task")
        raise


@app.task(bind=True, name='analyze_unanalyzed_news', queue='ai_processing')
def analyze_unanalyzed_news_task(
    self,
    limit: int = 1000,
    template_code: str = "news_summary_v1",
    symbols: list = None,
    start_date: str = None,
    end_date: str = None,
    max_concurrent: int = 5,
    enable_theme_matching: bool = True
):
    """
    Celery任务：分析所有未分析的新闻
    
    适用场景：
    - 定时批量分析未处理的新闻
    - 系统恢复后重新分析失败的新闻
    
    Args:
        limit: 最大分析数量
        template_code: Prompt模板代码
        symbols: 筛选特定股票代码
        start_date: 开始日期
        end_date: 结束日期
        max_concurrent: 最大并发数
    """
    
    try:
        info(f"开始执行未分析新闻批量分析任务 {self.request.id}", source="celery_news_task")
        
        from backend.services.news_analysis_service import NewsAnalysisService
        
        db_config = get_database_config()
        news_analysis_service = NewsAnalysisService(db_config)
        
        # 执行异步批量分析
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(
                news_analysis_service.batch_analyze_news(
                    limit=limit,
                    template_code=template_code,
                    symbols=symbols,
                    start_date=start_date,
                    end_date=end_date,
                    use_concurrent=True,  # Celery任务中使用并发
                    max_concurrent=max_concurrent,
                    enable_theme_matching=enable_theme_matching
                )
            )
        finally:
            loop.close()
        
        # 记录详细的统计摘要
        total = result.get('total', 0)
        success = result.get('success', 0)
        failed = result.get('failed', 0)
        template_code = result.get('template_code', 'unknown')
        success_rate = (success / total * 100) if total > 0 else 0
        error_types = result.get('error_types', {})
        
        # 格式化输出更易读的摘要
        info(f"📊 [celery_news_task] 任务 {self.request.id} 执行完成:", source="celery_news_task")
        info(f"   📈 处理统计: 总计 {total} 条新闻", source="celery_news_task")
        info(f"   ✅ 成功分析: {success} 条 ({success_rate:.1f}%)", source="celery_news_task")
        info(f"   ❌ 分析失败: {failed} 条", source="celery_news_task")
        info(f"   🔧 使用模板: {template_code}", source="celery_news_task")
        
        # 如果有失败的分析，记录失败原因统计
        if failed > 0 and error_types:
            info(f"   🔍 失败原因统计: {error_types}", source="celery_news_task")
        
        return result
        
    except Exception as e:
        error_msg = f"任务执行失败: {str(e)}"
        error(error_msg, source="celery_news_task")
        raise


