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
        
        info(f"任务 {self.request.id} 执行成功: {final_result}", source="celery_news_task")
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


@app.task(bind=True, name='analyze_unanalyzed_news', queue='ai_processing')
def analyze_unanalyzed_news_task(
    self,
    limit: int = 50,
    template_code: str = "news_summary_v1",
    symbols: list = None,
    start_date: str = None,
    end_date: str = None,
    max_concurrent: int = 5
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
                    max_concurrent=max_concurrent
                )
            )
        finally:
            loop.close()
        
        info(f"任务 {self.request.id} 执行成功: {result}", source="celery_news_task")
        return result
        
    except Exception as e:
        error_msg = f"任务执行失败: {str(e)}"
        error(error_msg, source="celery_news_task")
        raise


