# -*- coding: utf-8 -*-
"""
数据获取相关 Celery 任务
用于长时间运行的数据获取操作
"""

import asyncio
from celery import current_task
from backend.celery_app import app
from backend.utils.database_config import get_database_config
from backend.utils.logger import info, error


@app.task(bind=True, name='fetch_eodhd_fundamentals')
def fetch_eodhd_fundamentals_task(self, exchanges, data_types):
    """
    异步获取 EODHD 基本面数据 Celery 任务
    
    适用场景：
    - 长时间运行的基本面数据获取（5-15分钟）
    - 处理上千家公司的多种数据类型
    - 需要独立 worker 环境避免阻塞 APScheduler
    
    Args:
        exchanges: 交易所列表，如 ['NYSE', 'NASDAQ']
        data_types: 数据类型列表，如 ['General', 'Financials', 'Highlights']
    
    Returns:
        Dict: 包含执行结果
            {
                'success': bool,
                'message': str,
                'data': dict,
                'statistics': dict
            }
    """
    
    def update_progress(status=None, progress=None, current_exchange=None, log_message=None, **kwargs):
        """更新任务进度到 Redis"""
        try:
            meta = {
                'status': status or 'processing',
                'progress': progress or 0,
                'current_exchange': current_exchange,
                'message': log_message or '',
                'companies_processed': kwargs.get('companies_processed', 0),
                'companies_failed': kwargs.get('companies_failed', 0),
                'total_exchanges': len(exchanges)
            }
            
            current_task.update_state(
                state='PROGRESS',
                meta=meta
            )
            
            if log_message:
                info(f"Task {self.request.id}: {log_message}", source="celery_data_task")
                
        except Exception as e:
            error(f"更新任务状态失败: {str(e)}", source="celery_data_task")
    
    try:
        info(f"开始执行 EODHD 基本面数据获取任务", 
             task_id=self.request.id,
             exchanges=exchanges, 
             data_types=data_types,
             source="celery_data_task")
        
        update_progress(
            status='starting',
            progress=0,
            log_message=f"初始化基本面数据服务，准备获取 {len(exchanges)} 个交易所的数据"
        )
        
        # 导入服务
        from backend.services.eodhd_fundamentals_service import EODHDFundamentalsService
        service = EODHDFundamentalsService()
        
        # 定义进度回调函数
        def progress_callback(msg):
            info(f"进度: {msg}", source="celery_data_task")
            # 尝试从消息中提取进度信息
            if "处理" in msg or "获取" in msg:
                update_progress(log_message=msg)
        
        # 执行数据获取（异步方法需要在新的事件循环中运行）
        # 因为 Celery worker 默认不在 asyncio 事件循环中
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(
                service.fetch_exchange_fundamentals(
                    exchanges=exchanges,
                    data_types=data_types,
                    progress_callback=progress_callback
                )
            )
        finally:
            loop.close()
        
        # 处理结果
        if result.get('success'):
            total_companies = result.get('total_companies', 0)
            successful_companies = result.get('successful_companies', 0)
            failed_companies = result.get('failed_companies', 0)
            
            update_progress(
                status='completed',
                progress=100,
                log_message=f"完成！成功处理 {successful_companies}/{total_companies} 家公司",
                companies_processed=successful_companies,
                companies_failed=failed_companies
            )
            
            info(f"EODHD 基本面数据获取成功", 
                 task_id=self.request.id,
                 total=total_companies,
                 success=successful_companies,
                 failed=failed_companies,
                 result=result.get('message'),
                 source="celery_data_task")
        else:
            error(f"EODHD 基本面数据获取失败", 
                  task_id=self.request.id,
                  error=result.get('message'),
                  source="celery_data_task")
        
        return result
        
    except Exception as e:
        error_msg = f"EODHD 基本面数据获取任务异常: {str(e)}"
        error(error_msg, 
              task_id=self.request.id,
              exception=str(e),
              source="celery_data_task")
        
        update_progress(
            status='failed',
            progress=0,
            log_message=error_msg
        )
        
        # 返回错误结果而不是抛出异常，这样调度器可以记录详细的错误信息
        return {
            'success': False,
            'message': error_msg,
            'data': None
        }


@app.task(bind=True, name='fetch_eodhd_eod_stock_prices', queue='data_fetching')
def fetch_eodhd_eod_stock_prices_task(self, days_back=60):
    """
    异步获取 EODHD 历史股价数据 Celery 任务
    
    适用场景：
    - 获取 watchlist 中所有股票的历史价格数据
    - 执行时间 3-10分钟（取决于 watchlist 大小）
    - 避免阻塞 APScheduler
    
    Args:
        days_back: 回溯天数（默认: 60天）
    
    Returns:
        Dict: 包含执行结果
            {
                'success': bool,
                'message': str,
                'total_tickers': int,
                'success_count': int,
                'failed_count': int,
                'total_records': int,
                'execution_time': float
            }
    """
    
    def update_progress(status=None, progress=None, current_ticker=None, log_message=None, **kwargs):
        """更新任务进度到 Redis"""
        try:
            meta = {
                'status': status or 'processing',
                'progress': progress or 0,
                'current_ticker': current_ticker,
                'message': log_message or '',
                'tickers_processed': kwargs.get('tickers_processed', 0),
                'tickers_failed': kwargs.get('tickers_failed', 0),
                'records_stored': kwargs.get('records_stored', 0)
            }
            
            current_task.update_state(
                state='PROGRESS',
                meta=meta
            )
            
            if log_message:
                info(f"Task {self.request.id}: {log_message}", source="celery_data_task")
                
        except Exception as e:
            error(f"更新任务状态失败: {str(e)}", source="celery_data_task")
    
    try:
        info(f"开始执行 EODHD 历史股价数据获取任务", 
             task_id=self.request.id,
             days_back=days_back,
             source="celery_data_task")
        
        update_progress(
            status='starting',
            progress=0,
            log_message=f"初始化历史股价服务，准备获取最近 {days_back} 天的数据"
        )
        
        # 导入服务
        from backend.services.eodhd_eod_stock_prices_service import create_eodhd_eod_stock_prices_service
        service = create_eodhd_eod_stock_prices_service()
        
        # 执行数据获取（同步方法）
        info(f"开始获取历史股价数据", source="celery_data_task")
        result = service.fetch_and_store_watchlist_eod_prices(days_back)
        
        # 处理结果
        if result.get('error'):
            error_msg = f"EODHD历史股价数据获取失败: {result['error']}"
            error(error_msg, 
                  task_id=self.request.id,
                  source="celery_data_task")
            
            update_progress(
                status='failed',
                progress=0,
                log_message=error_msg
            )
            
            return {
                'success': False,
                'message': error_msg,
                'data': result
            }
        else:
            total_tickers = result.get('total_tickers', 0)
            success_count = result.get('success_count', 0)
            failed_count = result.get('failed_count', 0)
            total_records = result.get('total_records', 0)
            execution_time = result.get('execution_time', 0)
            
            success_msg = f"EODHD历史股价数据获取成功: 处理了{total_tickers}个ticker，成功{success_count}个，失败{failed_count}个，存储了{total_records}条记录，耗时{execution_time:.2f}秒"
            
            update_progress(
                status='completed',
                progress=100,
                log_message=success_msg,
                tickers_processed=success_count,
                tickers_failed=failed_count,
                records_stored=total_records
            )
            
            info(success_msg, 
                 task_id=self.request.id,
                 source="celery_data_task")
            
            return {
                'success': True,
                'message': success_msg,
                'total_tickers': total_tickers,
                'success_count': success_count,
                'failed_count': failed_count,
                'total_records': total_records,
                'execution_time': execution_time,
                'data': result
            }
        
    except Exception as e:
        error_msg = f"EODHD历史股价数据获取任务异常: {str(e)}"
        error(error_msg, 
              task_id=self.request.id,
              exception=str(e),
              source="celery_data_task")
        
        update_progress(
            status='failed',
            progress=0,
            log_message=error_msg
        )
        
        # 返回错误结果
        return {
            'success': False,
            'message': error_msg,
            'data': None
        }


@app.task(bind=True, name='fetch_earnings_calendar', queue='data_fetching')
def fetch_earnings_calendar_task(self, start_date=None, end_date=None, days=None, fetch_next_week=False):
    """
    异步获取财报日历数据 Celery 任务
    
    适用场景：
    - 大量财报数据获取（13000+条记录）
    - 执行时间 5-15分钟
    - 需要进度追踪和批量处理
    - 避免阻塞 APScheduler
    
    Args:
        start_date: 开始日期 (YYYY-MM-DD)，可选
        end_date: 结束日期 (YYYY-MM-DD)，可选
        days: 从今天开始的天数，可选（默认7天）
        fetch_next_week: 是否获取下周数据，可选（默认False）
    
    Returns:
        Dict: 包含执行结果
            {
                'success': bool,
                'message': str,
                'data': {
                    'total_count': int,
                    'new_count': int,
                    'updated_count': int,
                    'start_date': str,
                    'end_date': str
                }
            }
    """
    
    def update_progress(status=None, progress=None, current_batch=None, log_message=None, **kwargs):
        """更新任务进度到 Redis"""
        try:
            meta = {
                'status': status or 'processing',
                'progress': progress or 0,
                'current_batch': current_batch,
                'message': log_message or '',
                'records_processed': kwargs.get('records_processed', 0),
                'records_stored': kwargs.get('records_stored', 0),
                'new_records': kwargs.get('new_records', 0),
                'updated_records': kwargs.get('updated_records', 0)
            }
            
            current_task.update_state(
                state='PROGRESS',
                meta=meta
            )
            
            if log_message:
                info(f"Task {self.request.id}: {log_message}", source="celery_earnings_task")
                
        except Exception as e:
            error(f"更新任务状态失败: {str(e)}", source="celery_earnings_task")
    
    try:
        info(f"开始执行财报日历数据获取任务", 
             task_id=self.request.id,
             start_date=start_date,
             end_date=end_date,
             days=days,
             fetch_next_week=fetch_next_week,
             source="celery_earnings_task")
        
        update_progress(
            status='starting',
            progress=0,
            log_message="初始化财报日历服务..."
        )
        
        # 导入服务
        from backend.services.earnings_calendar_service import create_earnings_calendar_service
        service = create_earnings_calendar_service()
        
        # 决定使用哪种获取方式
        if fetch_next_week:
            update_progress(
                progress=5,
                log_message="获取下周财报数据..."
            )
            result = service.fetch_next_week_earnings()
        elif start_date and end_date:
            update_progress(
                progress=5,
                log_message=f"获取指定日期范围财报数据: {start_date} 到 {end_date}..."
            )
            result = service.fetch_earnings_calendar(start_date, end_date)
        elif days:
            update_progress(
                progress=5,
                log_message=f"获取未来 {days} 天财报数据..."
            )
            result = service.fetch_custom_period_earnings(days)
        else:
            update_progress(
                progress=5,
                log_message="获取未来 7 天财报数据（默认）..."
            )
            result = service.fetch_custom_period_earnings(7)
        
        # 处理结果
        if result.get('success'):
            data = result.get('data', {})
            total_count = data.get('total_count', 0)
            new_count = data.get('new_count', 0)
            updated_count = data.get('updated_count', 0)
            
            success_msg = f"财报日历数据获取成功: 总计 {total_count} 条，新增 {new_count} 条，更新 {updated_count} 条"
            
            update_progress(
                status='completed',
                progress=100,
                log_message=success_msg,
                records_processed=total_count,
                records_stored=total_count,
                new_records=new_count,
                updated_records=updated_count
            )
            
            info(success_msg, 
                 task_id=self.request.id,
                 total=total_count,
                 new=new_count,
                 updated=updated_count,
                 source="celery_earnings_task")
            
            return result
        else:
            error_msg = f"财报日历数据获取失败: {result.get('message', '未知错误')}"
            error(error_msg, 
                  task_id=self.request.id,
                  source="celery_earnings_task")
            
            update_progress(
                status='failed',
                progress=0,
                log_message=error_msg
            )
            
            return result
        
    except Exception as e:
        error_msg = f"财报日历数据获取任务异常: {str(e)}"
        error(error_msg, 
              task_id=self.request.id,
              exception=str(e),
              source="celery_earnings_task")
        
        update_progress(
            status='failed',
            progress=0,
            log_message=error_msg
        )
        
        # 返回错误结果
        return {
            'success': False,
            'message': error_msg,
            'data': None
        }


@app.task(bind=True, name='fetch_ipo_calendar', queue='data_fetching')
def fetch_ipo_calendar_task(self, start_date=None, end_date=None, days=None, fetch_next_week=False):
    """
    异步获取 IPO 日历数据 Celery 任务
    
    适用场景：
    - IPO 日历数据获取
    - 执行时间 1-5分钟
    - 需要进度追踪和批量处理
    - 避免阻塞 APScheduler
    
    注意：EODHD API 不支持 exchanges 参数，会返回所有交易所的数据。
    如果需要过滤特定交易所，可以在查询数据库时使用 WHERE 条件。
    
    Args:
        start_date: 开始日期 (YYYY-MM-DD)，可选
        end_date: 结束日期 (YYYY-MM-DD)，可选
        days: 从今天开始的天数，可选（默认30天）
        fetch_next_week: 是否获取下周数据，可选（默认False）
    
    Returns:
        Dict: 包含执行结果
            {
                'success': bool,
                'message': str,
                'data': {
                    'total_count': int,
                    'new_count': int,
                    'updated_count': int,
                    'start_date': str,
                    'end_date': str
                }
            }
    """
    
    def update_progress(status=None, progress=None, current_batch=None, log_message=None, **kwargs):
        """更新任务进度到 Redis"""
        try:
            meta = {
                'status': status or 'processing',
                'progress': progress or 0,
                'current_batch': current_batch,
                'message': log_message or '',
                'records_processed': kwargs.get('records_processed', 0),
                'records_stored': kwargs.get('records_stored', 0),
                'new_records': kwargs.get('new_records', 0),
                'updated_records': kwargs.get('updated_records', 0)
            }
            
            self.update_state(
                state='PROGRESS',
                meta=meta
            )
            
            if log_message:
                info(f"Task {self.request.id}: {log_message}", source="celery_ipo_task")
                
        except Exception as e:
            error(f"更新任务状态失败: {str(e)}", source="celery_ipo_task")
    
    try:
        info(f"开始执行 IPO 日历数据获取任务", 
             task_id=self.request.id,
             start_date=start_date,
             end_date=end_date,
             days=days,
             fetch_next_week=fetch_next_week,
             source="celery_ipo_task")
        
        update_progress(
            status='starting',
            progress=0,
            log_message="初始化 IPO 日历服务..."
        )
        
        # 导入服务
        from backend.services.ipo_calendar_service import create_ipo_calendar_service
        service = create_ipo_calendar_service()
        
        # 决定使用哪种获取方式
        if fetch_next_week:
            update_progress(
                progress=5,
                log_message="获取下周 IPO 数据..."
            )
            result = service.fetch_next_week_ipo()
        elif start_date and end_date:
            update_progress(
                progress=5,
                log_message=f"获取指定日期范围 IPO 数据: {start_date} 到 {end_date}..."
            )
            result = service.fetch_ipo_calendar(start_date, end_date)
        elif days:
            update_progress(
                progress=5,
                log_message=f"获取未来 {days} 天 IPO 数据..."
            )
            result = service.fetch_custom_period_ipo(days)
        else:
            update_progress(
                progress=5,
                log_message="获取未来 30 天 IPO 数据（默认）..."
            )
            result = service.fetch_custom_period_ipo(30)
        
        # 处理结果
        if result.get('success'):
            data = result.get('data', {})
            total_count = data.get('total_count', 0)
            new_count = data.get('new_count', 0)
            updated_count = data.get('updated_count', 0)
            
            success_msg = f"IPO 日历数据获取成功: 总计 {total_count} 条，新增 {new_count} 条，更新 {updated_count} 条"
            
            update_progress(
                status='completed',
                progress=100,
                log_message=success_msg,
                records_processed=total_count,
                records_stored=total_count,
                new_records=new_count,
                updated_records=updated_count
            )
            
            info(success_msg, 
                 task_id=self.request.id,
                 total=total_count,
                 new=new_count,
                 updated=updated_count,
                 source="celery_ipo_task")
            
            return result
        else:
            error_msg = f"IPO 日历数据获取失败: {result.get('message', '未知错误')}"
            error(error_msg, 
                  task_id=self.request.id,
                  source="celery_ipo_task")
            
            update_progress(
                status='failed',
                progress=0,
                log_message=error_msg
            )
            
            return result
        
    except Exception as e:
        error_msg = f"IPO 日历数据获取任务异常: {str(e)}"
        error(error_msg, 
              task_id=self.request.id,
              exception=str(e),
              source="celery_ipo_task")
        
        update_progress(
            status='failed',
            progress=0,
            log_message=error_msg
        )
        
        # 返回错误结果
        return {
            'success': False,
            'message': error_msg,
            'data': None
        }

