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



@app.task(bind=True, name='update_single_ticker_fundamentals', queue='data_fetching')
def update_single_ticker_fundamentals_task(self, symbol: str, exchange: str = 'US'):
    """
    异步更新单个 ticker 的 fundamentals 数据 Celery 任务
    
    适用场景：
    - 用户添加新股票到 watchlist 时立即更新基本面数据
    - 不阻塞用户操作，后台异步执行
    - 避免在 API 请求线程中执行耗时的 EODHD API 调用
    
    Args:
        symbol: 股票代码（如 AAPL）
        exchange: 交易所代码（如 US, HK, SHG）
    
    Returns:
        Dict: 包含执行结果
            {
                'success': bool,
                'message': str,
                'symbol': str,
                'exchange': str,
                'updated_components': list,
                'execution_time': float
            }
    """
    import time
    
    def update_progress(status=None, progress=None, log_message=None, **kwargs):
        """更新任务进度到 Redis"""
        try:
            meta = {
                'status': status or 'processing',
                'progress': progress or 0,
                'symbol': symbol,
                'exchange': exchange,
                'message': log_message or '',
                'updated_components': kwargs.get('updated_components', [])
            }
            
            current_task.update_state(
                state='PROGRESS',
                meta=meta
            )
            
            if log_message:
                info(f"Task {self.request.id}: {log_message}", source="celery_fundamentals_task")
                
        except Exception as e:
            error(f"更新任务状态失败: {str(e)}", source="celery_fundamentals_task")
    
    try:
        start_time = time.time()
        
        info(f"开始更新单个 ticker 的 fundamentals 数据: {symbol}.{exchange}", 
             task_id=self.request.id,
             symbol=symbol,
             exchange=exchange,
             source="celery_fundamentals_task")
        
        update_progress(
            status='processing',
            progress=10,
            log_message=f"正在初始化 fundamentals 服务..."
        )
        
        # 导入服务
        from backend.services.eodhd_fundamentals_service import EODHDFundamentalsService
        fundamentals_service = EODHDFundamentalsService()
        
        update_progress(
            status='processing',
            progress=30,
            log_message=f"正在从 EODHD API 获取 {symbol} 的完整基本面数据..."
        )
        
        # 获取完整的基本面数据
        full_data = fundamentals_service.fetch_company_full_data(symbol, exchange)
        
        if not full_data:
            error_msg = f"未能从 EODHD 获取 {symbol}.{exchange} 的基本面数据"
            error(error_msg, 
                  task_id=self.request.id,
                  symbol=symbol,
                  exchange=exchange,
                  source="celery_fundamentals_task")
            
            update_progress(
                status='failed',
                progress=0,
                log_message=error_msg
            )
            
            return {
                'success': False,
                'message': error_msg,
                'symbol': symbol,
                'exchange': exchange,
                'updated_components': [],
                'execution_time': time.time() - start_time
            }
        
        update_progress(
            status='processing',
            progress=60,
            log_message=f"获取成功，正在保存到数据库..."
        )
        
        # 构建保存数据的格式
        company_data = {
            'symbol': symbol,
            'exchange': exchange,
            'general_data': full_data.get('General', full_data),
            'address_data': full_data.get('General', full_data).get('AddressData') if full_data.get('General') else None,
            'officers_data': fundamentals_service._extract_officers_list(full_data.get('General', full_data).get('Officers', {})) if full_data.get('General') else [],
            'listings_data': full_data.get('General', full_data).get('Listings') if full_data.get('General') else None,
            'full_data': full_data
        }
        
        # 使用批量保存方法
        save_result = fundamentals_service.batch_save_companies_data([company_data])
        
        # 构建更新结果
        data_updated = {
            'company_info': save_result.get('companies_saved', 0) > 0,
            'address': save_result.get('addresses_saved', 0) > 0,
            'officers': save_result.get('officers_saved', 0) > 0,
            'financials': save_result.get('financials_saved', 0) > 0,
            'highlights': save_result.get('highlights_saved', 0) > 0,
            'valuations': save_result.get('valuations_saved', 0) > 0,
            'technicals': save_result.get('technicals_saved', 0) > 0,
            'earnings': save_result.get('earnings_saved', 0) > 0,
            'outstanding_shares': save_result.get('outstanding_shares_saved', 0) > 0,
            'analyst_ratings': save_result.get('analyst_ratings_saved', 0) > 0,
            'splits_dividends': save_result.get('splits_dividends_saved', 0) > 0,
            'insider_transactions': save_result.get('insider_transactions_saved', 0) > 0
        }
        
        updated_components = [k for k, v in data_updated.items() if v]
        execution_time = time.time() - start_time
        
        success_msg = f"成功更新 {symbol}.{exchange} 的基本面数据，更新了 {len(updated_components)} 个组件（耗时 {execution_time:.2f} 秒）"
        info(success_msg, 
             task_id=self.request.id,
             symbol=symbol,
             exchange=exchange,
             updated_components=updated_components,
             execution_time=execution_time,
             source="celery_fundamentals_task")
        
        update_progress(
            status='completed',
            progress=100,
            log_message=success_msg,
            updated_components=updated_components
        )
        
        return {
            'success': True,
            'message': success_msg,
            'symbol': symbol,
            'exchange': exchange,
            'updated_components': updated_components,
            'data_updated': data_updated,
            'execution_time': execution_time
        }
        
    except Exception as e:
        error_msg = f"更新 {symbol}.{exchange} 基本面数据任务异常: {str(e)}"
        error(error_msg, 
              task_id=self.request.id,
              symbol=symbol,
              exchange=exchange,
              exception=str(e),
              source="celery_fundamentals_task")
        
        update_progress(
            status='failed',
            progress=0,
            log_message=error_msg
        )
        
        return {
            'success': False,
            'message': error_msg,
            'symbol': symbol,
            'exchange': exchange,
            'updated_components': [],
            'execution_time': 0
        }


@app.task(bind=True, name='update_watchlist_fundamentals_batch', queue='data_fetching')
def update_watchlist_fundamentals_batch_task(self, data_types: list = None, skip_recent_hours: int = 24):
    """
    异步批量更新 watchlist 中所有股票的 fundamentals 数据 Celery 任务
    
    适用场景：
    - 定期更新所有 watchlist 股票的基本面数据
    - 确保用户 watchlist 中的数据保持最新
    - 避免在用户查询时才触发更新
    
    Args:
        data_types: 要更新的数据类型列表（默认全部12种）
        skip_recent_hours: 跳过最近N小时内已更新的股票（默认24小时）
    
    Returns:
        Dict: 包含执行结果
            {
                'success': bool,
                'message': str,
                'total_stocks': int,
                'updated_stocks': int,
                'skipped_stocks': int,
                'failed_stocks': int,
                'execution_time': float
            }
    """
    import time
    from datetime import datetime, timedelta
    
    def update_progress(status=None, progress=None, log_message=None, **kwargs):
        """更新任务进度到 Redis"""
        try:
            meta = {
                'status': status or 'processing',
                'progress': progress or 0,
                'message': log_message or '',
                'total_stocks': kwargs.get('total_stocks', 0),
                'updated_stocks': kwargs.get('updated_stocks', 0),
                'skipped_stocks': kwargs.get('skipped_stocks', 0),
                'failed_stocks': kwargs.get('failed_stocks', 0)
            }
            
            current_task.update_state(
                state='PROGRESS',
                meta=meta
            )
            
            if log_message:
                info(f"Task {self.request.id}: {log_message}", source="celery_watchlist_fundamentals_batch")
                
        except Exception as e:
            error(f"更新任务状态失败: {str(e)}", source="celery_watchlist_fundamentals_batch")
    
    try:
        start_time = time.time()
        
        # 设置默认数据类型（全部12种）
        if data_types is None:
            data_types = [
                'General', 'Financials', 'Highlights', 'Valuation', 'Technicals',
                'Holders', 'outstandingShares', 'Earnings', 'SharesStats',
                'AnalystRatings', 'SplitsDividends', 'InsiderTransactions'
            ]
        
        info(f"开始批量更新 watchlist fundamentals 数据", 
             task_id=self.request.id,
             data_types=data_types,
             skip_recent_hours=skip_recent_hours,
             source="celery_watchlist_fundamentals_batch")
        
        update_progress(
            status='starting',
            progress=5,
            log_message=f"正在从 target_tracking_config 表获取 watchlist 股票列表..."
        )
        
        # 从数据库获取所有 watchlist 股票（去重）
        from backend.database.db_manager import DatabaseManager
        from backend.utils.database_config import get_database_config
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        # 查询所有活跃的 watchlist 股票，并关联 companies 表获取最后更新时间
        query = f"""
        SELECT DISTINCT 
            ttc.symbol,
            c.exchange,
            c.updated_at as last_update
        FROM target_tracking_config ttc
        LEFT JOIN companies c ON ttc.symbol = c.code
        WHERE ttc.enabled = TRUE
        ORDER BY c.updated_at ASC NULLS FIRST
        """
        
        with db_manager.get_connection_context() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            watchlist_stocks = cursor.fetchall()
        
        total_stocks = len(watchlist_stocks)
        
        if total_stocks == 0:
            info("Watchlist 为空，无需更新", source="celery_watchlist_fundamentals_batch")
            return {
                'success': True,
                'message': 'Watchlist 为空，无需更新',
                'total_stocks': 0,
                'updated_stocks': 0,
                'skipped_stocks': 0,
                'failed_stocks': 0,
                'execution_time': time.time() - start_time
            }
        
        info(f"找到 {total_stocks} 只 watchlist 股票", 
             total=total_stocks,
             source="celery_watchlist_fundamentals_batch")
        
        update_progress(
            status='processing',
            progress=10,
            log_message=f"找到 {total_stocks} 只股票，开始批量更新...",
            total_stocks=total_stocks
        )
        
        # 过滤掉最近已更新的股票
        cutoff_time = datetime.now() - timedelta(hours=skip_recent_hours)
        stocks_to_update = []
        skipped_stocks = 0
        
        for symbol, exchange, last_update in watchlist_stocks:
            if last_update and last_update > cutoff_time:
                skipped_stocks += 1
                info(f"跳过最近已更新的股票: {symbol} (上次更新: {last_update})",
                     source="celery_watchlist_fundamentals_batch")
            else:
                # 如果 exchange 为空，默认使用 US
                stocks_to_update.append((symbol, exchange or 'US'))
        
        info(f"过滤后需要更新 {len(stocks_to_update)} 只股票，跳过 {skipped_stocks} 只",
             to_update=len(stocks_to_update),
             skipped=skipped_stocks,
             source="celery_watchlist_fundamentals_batch")
        
        if len(stocks_to_update) == 0:
            return {
                'success': True,
                'message': f'所有 {total_stocks} 只股票都在 {skip_recent_hours} 小时内更新过，无需更新',
                'total_stocks': total_stocks,
                'updated_stocks': 0,
                'skipped_stocks': skipped_stocks,
                'failed_stocks': 0,
                'execution_time': time.time() - start_time
            }
        
        update_progress(
            status='processing',
            progress=15,
            log_message=f"开始提交 {len(stocks_to_update)} 个更新任务...",
            total_stocks=total_stocks,
            skipped_stocks=skipped_stocks
        )
        
        # 批量提交单个 ticker 更新任务
        from backend.tasks.data_tasks import update_single_ticker_fundamentals_task
        from celery import group
        
        # 创建任务组（并发执行，但控制并发数）
        update_tasks = []
        for symbol, exchange in stocks_to_update:
            task = update_single_ticker_fundamentals_task.s(symbol, exchange)
            update_tasks.append(task)
        
        # 使用 group 并发执行（Celery 会自动控制并发数）
        job = group(update_tasks)
        result_group = job.apply_async()
        
        info(f"已提交 {len(update_tasks)} 个更新任务到 Celery",
             source="celery_watchlist_fundamentals_batch")
        
        update_progress(
            status='processing',
            progress=30,
            log_message=f"已提交 {len(update_tasks)} 个任务，等待执行完成...",
            total_stocks=total_stocks,
            skipped_stocks=skipped_stocks
        )
        
        # 等待所有任务完成（设置超时：每只股票最多2分钟）
        timeout = len(stocks_to_update) * 120  # 2分钟/股票
        try:
            results = result_group.get(timeout=timeout, propagate=False)
        except Exception as e:
            error(f"等待任务完成时出错: {str(e)}", source="celery_watchlist_fundamentals_batch")
            results = []
        
        # 统计结果
        updated_stocks = 0
        failed_stocks = 0
        
        for result in results:
            if isinstance(result, dict) and result.get('success'):
                updated_stocks += 1
            else:
                failed_stocks += 1
        
        execution_time = time.time() - start_time
        
        success_msg = (
            f"Watchlist fundamentals 批量更新完成: "
            f"总计 {total_stocks} 只，更新 {updated_stocks} 只，"
            f"跳过 {skipped_stocks} 只，失败 {failed_stocks} 只 "
            f"（耗时 {execution_time:.2f} 秒）"
        )
        
        info(success_msg,
             task_id=self.request.id,
             total=total_stocks,
             updated=updated_stocks,
             skipped=skipped_stocks,
             failed=failed_stocks,
             execution_time=execution_time,
             source="celery_watchlist_fundamentals_batch")
        
        update_progress(
            status='completed',
            progress=100,
            log_message=success_msg,
            total_stocks=total_stocks,
            updated_stocks=updated_stocks,
            skipped_stocks=skipped_stocks,
            failed_stocks=failed_stocks
        )
        
        return {
            'success': True,
            'message': success_msg,
            'total_stocks': total_stocks,
            'updated_stocks': updated_stocks,
            'skipped_stocks': skipped_stocks,
            'failed_stocks': failed_stocks,
            'execution_time': execution_time,
            'data_types': data_types
        }
        
    except Exception as e:
        error_msg = f"Watchlist fundamentals 批量更新任务异常: {str(e)}"
        error(error_msg,
              task_id=self.request.id,
              exception=str(e),
              source="celery_watchlist_fundamentals_batch")
        
        update_progress(
            status='failed',
            progress=0,
            log_message=error_msg
        )
        
        return {
            'success': False,
            'message': error_msg,
            'total_stocks': 0,
            'updated_stocks': 0,
            'skipped_stocks': 0,
            'failed_stocks': 0,
            'execution_time': 0
        }


@app.task(bind=True, name='fetch_single_ticker_eod_history', queue='data_fetching')
def fetch_single_ticker_eod_history_task(self, ticker: str, days_back: int = None):
    """
    异步获取单个 ticker 的全部历史 EOD 数据 Celery 任务
    
    适用场景：
    - 用户添加新股票到 watchlist 时立即获取历史数据
    - 不阻塞用户操作，后台异步执行
    
    Args:
        ticker: 股票代码（如 AAPL.US）
        days_back: 回溯天数（None 表示获取全部历史数据，通常为 10 年）
    
    Returns:
        Dict: 包含执行结果
            {
                'success': bool,
                'message': str,
                'ticker': str,
                'records_stored': int,
                'execution_time': float
            }
    """
    import time
    from datetime import datetime, timedelta
    
    def update_progress(status=None, progress=None, log_message=None, **kwargs):
        """更新任务进度到 Redis"""
        try:
            meta = {
                'status': status or 'processing',
                'progress': progress or 0,
                'ticker': ticker,
                'message': log_message or '',
                'records_stored': kwargs.get('records_stored', 0)
            }
            
            current_task.update_state(
                state='PROGRESS',
                meta=meta
            )
            
            if log_message:
                info(f"Task {self.request.id}: {log_message}", source="celery_single_ticker_task")
                
        except Exception as e:
            error(f"更新任务状态失败: {str(e)}", source="celery_single_ticker_task")
    
    try:
        start_time = time.time()
        
        # 如果未指定天数，默认获取 10 年历史数据（3650 天）
        if days_back is None:
            days_back = 3650
        
        info(f"开始获取单个 ticker 历史数据: {ticker}, 回溯 {days_back} 天", 
             task_id=self.request.id,
             ticker=ticker,
             days_back=days_back,
             source="celery_single_ticker_task")
        
        update_progress(
            status='processing',
            progress=10,
            log_message=f"正在获取 {ticker} 的历史数据..."
        )
        
        # 初始化服务
        from backend.services.eodhd_eod_stock_prices_service import EODHDEODStockPricesService
        
        eod_service = EODHDEODStockPricesService()
        
        update_progress(
            status='processing',
            progress=30,
            log_message=f"正在从 EODHD API 获取 {ticker} 数据..."
        )
        
        # 计算日期范围
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
        
        # 获取数据
        eod_data = eod_service.fetch_eod_data_for_ticker(ticker, start_date, end_date)
        
        if not eod_data:
            error_msg = f"未能从 EODHD 获取 {ticker} 的数据"
            error(error_msg, 
                  task_id=self.request.id,
                  ticker=ticker,
                  source="celery_single_ticker_task")
            
            update_progress(
                status='failed',
                progress=0,
                log_message=error_msg
            )
            
            return {
                'success': False,
                'message': error_msg,
                'ticker': ticker,
                'records_stored': 0,
                'execution_time': time.time() - start_time
            }
        
        update_progress(
            status='processing',
            progress=60,
            log_message=f"获取到 {len(eod_data)} 条数据，正在存储到数据库..."
        )
        
        # 存储数据
        records_stored = eod_service.store_eod_data(ticker, eod_data)
        
        execution_time = time.time() - start_time
        
        if records_stored > 0:
            success_msg = f"成功获取并存储 {ticker} 的 {records_stored} 条历史数据（耗时 {execution_time:.2f} 秒）"
            info(success_msg, 
                 task_id=self.request.id,
                 ticker=ticker,
                 records=records_stored,
                 execution_time=execution_time,
                 source="celery_single_ticker_task")
            
            update_progress(
                status='completed',
                progress=100,
                log_message=success_msg,
                records_stored=records_stored
            )
            
            return {
                'success': True,
                'message': success_msg,
                'ticker': ticker,
                'records_stored': records_stored,
                'execution_time': execution_time
            }
        else:
            error_msg = f"存储 {ticker} 数据失败"
            error(error_msg, 
                  task_id=self.request.id,
                  ticker=ticker,
                  source="celery_single_ticker_task")
            
            update_progress(
                status='failed',
                progress=0,
                log_message=error_msg
            )
            
            return {
                'success': False,
                'message': error_msg,
                'ticker': ticker,
                'records_stored': 0,
                'execution_time': execution_time
            }
        
    except Exception as e:
        error_msg = f"获取 {ticker} 历史数据任务异常: {str(e)}"
        error(error_msg, 
              task_id=self.request.id,
              ticker=ticker,
              exception=str(e),
              source="celery_single_ticker_task")
        
        update_progress(
            status='failed',
            progress=0,
            log_message=error_msg
        )
        
        return {
            'success': False,
            'message': error_msg,
            'ticker': ticker,
            'records_stored': 0,
            'execution_time': 0
        }
