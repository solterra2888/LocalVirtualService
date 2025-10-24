# -*- coding: utf-8 -*-
"""
文件处理相关Celery任务
"""
import asyncio
import os
from datetime import datetime
from celery import current_task
from backend.celery_app import app
from backend.workers.file_worker import FileProcessingWorker
from backend.utils.database_config import get_database_config
from backend.utils.logger import info, error

@app.task(bind=True, name='process_files_async', queue='file_processing')
def process_files_task(self, file_paths, task_info, custom_inputs, params, username):
    """
    异步文件处理任务
    
    Args:
        file_paths: 文件路径列表
        task_info: 任务信息字典
        custom_inputs: 自定义输入参数
        params: 处理参数
        username: 用户名
    """
    
    def update_progress(status=None, progress=None, current_file=None, log_message=None, **kwargs):
        """更新任务进度到Redis"""
        try:
            # 初始化日志列表（如果不存在）
            if not hasattr(update_progress, 'logs'):
                update_progress.logs = []
            
            # 如果有新的日志消息，添加到日志列表
            if log_message:
                log_entry = {
                    'timestamp': datetime.now().isoformat(),
                    'level': 'info',
                    'message': log_message,
                    'source': 'celery_task'
                }
                update_progress.logs.append(log_entry)
                # 限制日志数量，避免内存占用过多
                if len(update_progress.logs) > 50:
                    update_progress.logs = update_progress.logs[-50:]
            
            meta = {
                'status': status or 'processing',
                'progress': progress or 0,
                'current_file': current_file,
                'message': log_message or '',
                'files_processed': getattr(worker, 'files_processed', 0),
                'files_failed': getattr(worker, 'files_failed', 0),
                'total_files': len(file_paths),
                'username': username,
                'logs': update_progress.logs
            }
            
            current_task.update_state(
                state='PROGRESS',
                meta=meta
            )
            
            # 同时记录到日志
            if log_message:
                info(f"Task {self.request.id}: {log_message}", source="celery_task")
                
        except Exception as e:
            error(f"更新任务状态失败: {e}", source="celery_task")
    
    worker = None
    try:
        info(f"开始执行文件处理任务 {self.request.id}, 用户: {username}", source="celery_task")
        
        # 获取数据库连接参数
        db_conn_params = get_database_config()
        
        # 创建文件处理工作者
        worker = FileProcessingWorker(self.request.id, username, db_conn_params)
        worker.set_status_callback(update_progress)
        
        # 更新初始状态
        update_progress(
            status="initializing",
            progress=0,
            log_message="正在初始化文件处理任务..."
        )
        
        # 执行异步文件处理
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            # 获取API密钥
            api_key = params.get('llamaindex_api_key') or os.getenv('LLAMA_CLOUD_API_KEY')
            if not api_key:
                raise ValueError("LlamaIndex API密钥未配置")
            
            result = loop.run_until_complete(
                worker.process_files(
                    file_paths, 
                    task_info, 
                    custom_inputs, 
                    params, 
                    api_key
                )
            )
        finally:
            loop.close()
        
        # 返回成功结果
        final_result = {
            'status': 'completed',
            'files_processed': worker.files_processed,
            'files_failed': worker.files_failed,
            'total_files': len(file_paths),
            'processing_time': result.get('processing_time', 0),
            'successful_results': len(result.get('successful_results', [])),
            'final_report': result.get('final_report') is not None,
            'logs': getattr(update_progress, 'logs', [])
        }
        
        info(f"任务 {self.request.id} 执行成功: {final_result}", source="celery_task")
        return final_result
        
    except Exception as e:
        error_msg = f"任务执行失败: {str(e)}"
        error(f"Task {self.request.id}: {error_msg}", source="celery_task")
        
        # 更新失败状态（确保所有值都可以JSON序列化）
        failure_meta = {
            'error': str(error_msg),  # 确保是字符串
            'status': 'failed',
            'files_processed': int(getattr(worker, 'files_processed', 0)),
            'files_failed': int(getattr(worker, 'files_failed', 0)),
            'total_files': int(len(file_paths))
        }
        
        current_task.update_state(
            state='FAILURE',
            meta=failure_meta
        )
        
        # 重新抛出异常，但确保异常信息可以被序列化
        raise Exception(error_msg)


