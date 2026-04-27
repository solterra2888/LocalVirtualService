# -*- coding: utf-8 -*-
"""
内容自动归类相关Celery任务
"""
import asyncio
import os
from datetime import datetime
from celery import current_task
from backend.celery_app import app
from backend.services.content_classify_service import ContentClassifyService
from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import info, error, warning


@app.task(
    bind=True,
    name='classify_file_content',
    queue='file_processing',
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 3, 'countdown': 30},
    retry_backoff=True,
    retry_backoff_max=120,
    retry_jitter=True
)
def classify_file_content_task(self, file_id, username=None, force=False):
    """
    为文件内容自动归类的Celery任务

    Args:
        file_id: file_processing_details.id
        username: 用户名（必须，用于查询该用户的文件夹列表）
        force: 是否强制重新归类（即使已有归类）

    Returns:
        归类结果
    """
    _last_progress = [0.0]
    _last_status = ['processing']

    def update_progress(status=None, progress=None, log_message=None, **kwargs):
        try:
            if progress is not None:
                _last_progress[0] = progress
            if status is not None:
                _last_status[0] = status

            if not hasattr(update_progress, 'logs'):
                update_progress.logs = []

            if log_message:
                log_entry = {
                    'timestamp': datetime.now().isoformat(),
                    'level': 'info',
                    'message': log_message,
                    'source': 'celery_classify'
                }
                update_progress.logs.append(log_entry)
                if len(update_progress.logs) > 30:
                    update_progress.logs = update_progress.logs[-30:]

            meta = {
                'status': _last_status[0],
                'progress': _last_progress[0],
                'message': log_message or '',
                'file_id': file_id,
                'username': username,
                'logs': update_progress.logs
            }

            current_task.update_state(state='PROGRESS', meta=meta)

            if log_message:
                info(f"Classify {self.request.id}: {log_message}", source="celery_classify")

        except Exception as e:
            error(f"更新自动归类任务状态失败: {e}", source="celery_classify")

    try:
        auto_classify_enabled = os.getenv('AUTO_CLASSIFY_ENABLED', 'true').lower() == 'true'
        if not auto_classify_enabled and not force:
            info(f"自动归类功能已禁用，跳过 file_id={file_id}", source="celery_classify")
            return {
                'status': 'skipped',
                'message': '自动归类功能已禁用',
                'file_id': file_id
            }

        if not username:
            info(f"未提供用户名，跳过自动归类 file_id={file_id}", source="celery_classify")
            return {
                'status': 'skipped',
                'message': '未提供用户名',
                'file_id': file_id
            }

        min_content_length = int(os.getenv('AUTO_CLASSIFY_MIN_CONTENT_LENGTH', '50'))

        info(
            f"开始自动归类任务 {self.request.id}, file_id={file_id}, "
            f"user={username}, force={force}",
            source="celery_classify"
        )

        update_progress(
            status="initializing",
            progress=5,
            log_message=f"正在初始化自动归类任务... (file_id={file_id})"
        )

        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)

        classify_service = ContentClassifyService(
            db_manager=db_manager,
            log_callback=update_progress
        )

        update_progress(progress=10, log_message="正在读取文件内容...")

        file_info = classify_service.get_file_info(file_id)
        if not file_info:
            raise ValueError(f"文件不存在: file_id={file_id}")

        filename = file_info.get('filename', 'unknown')
        content_length = file_info.get('content_length', 0)

        if content_length < min_content_length:
            warning(
                f"文件内容太短，跳过自动归类: {filename}, length={content_length}",
                source="celery_classify"
            )
            return {
                'status': 'skipped',
                'message': f'文件内容太短（{content_length} < {min_content_length}），跳过自动归类',
                'file_id': file_id,
                'filename': filename
            }

        update_progress(
            progress=15,
            log_message=f"文件: {filename}, 内容长度: {content_length} 字符"
        )

        update_progress(
            status="classifying",
            progress=20,
            log_message="正在调用AI分析文件内容并归类..."
        )

        # PR-4 修复: username → user_id, 让 LLM usage_log 能正确归属用户
        from backend.utils.user_resolver import get_user_id_by_username
        user_id = get_user_id_by_username(username)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                classify_service.classify_file_content(
                    file_id=file_id,
                    username=username,
                    force=force,
                    user_id=user_id,
                )
            )
        finally:
            loop.close()

        if result.get('skipped'):
            update_progress(
                status="completed",
                progress=100,
                log_message=result.get('message', '已跳过')
            )
            return {
                'status': 'skipped',
                'file_id': file_id,
                'filename': filename,
                **result
            }

        folder_name = result.get('folder_name', 'Uncategorized')
        update_progress(
            status="completed",
            progress=100,
            log_message=f"自动归类完成！folder={folder_name}"
        )

        final_result = {
            'status': 'completed',
            'file_id': file_id,
            'filename': filename,
            'folder_id': result.get('folder_id'),
            'folder_name': folder_name,
            'processing_time': result.get('processing_time', 0),
            'ai_metadata': result.get('ai_metadata', {}),
            'logs': getattr(update_progress, 'logs', [])
        }

        info(
            f"自动归类任务 {self.request.id} 完成: {filename} -> {folder_name}, "
            f"耗时={result.get('processing_time', 0):.2f}秒",
            source="celery_classify"
        )

        return final_result

    except Exception as e:
        error_msg = f"自动归类任务失败: {str(e)}"
        error(f"Classify {self.request.id}: {error_msg}", source="celery_classify")

        return {
            'status': 'failed',
            'file_id': file_id,
            'filename': None,
            'error': error_msg,
            'processing_time': 0,
            'retry_count': self.request.retries
        }


@app.task(
    bind=True,
    name='batch_classify_files',
    queue='file_processing'
)
def batch_classify_files_task(self, file_ids, username=None, force=False):
    """
    批量为文件自动归类

    Args:
        file_ids: 文件ID列表
        username: 用户名
        force: 是否强制重新归类
    """
    info(
        f"开始批量自动归类任务 {self.request.id}, "
        f"files={len(file_ids)}, user={username}",
        source="celery_classify"
    )

    results = {
        'total': len(file_ids),
        'success': 0,
        'failed': 0,
        'skipped': 0,
        'details': []
    }

    for idx, file_id in enumerate(file_ids):
        try:
            progress = (idx + 1) / len(file_ids) * 100
            current_task.update_state(
                state='PROGRESS',
                meta={
                    'status': 'processing',
                    'progress': progress,
                    'current_file': file_id,
                    'processed': idx + 1,
                    'total': len(file_ids),
                    'results': results
                }
            )

            task_result = classify_file_content_task.apply_async(
                args=(file_id, username, force),
                countdown=1
            )

            result = task_result.get(timeout=300)

            if result.get('status') == 'completed':
                results['success'] += 1
            elif result.get('status') == 'skipped':
                results['skipped'] += 1
            else:
                results['failed'] += 1

            results['details'].append({
                'file_id': file_id,
                'status': result.get('status'),
                'folder_name': result.get('folder_name'),
                'message': result.get('message', '')
            })

        except Exception as e:
            error(f"批量自动归类-文件 {file_id} 失败: {str(e)}", source="celery_classify")
            results['failed'] += 1
            results['details'].append({
                'file_id': file_id,
                'status': 'failed',
                'error': str(e)
            })

    info(
        f"批量自动归类任务 {self.request.id} 完成: "
        f"成功={results['success']}, 失败={results['failed']}, 跳过={results['skipped']}",
        source="celery_classify"
    )

    return results
