# -*- coding: utf-8 -*-
"""
内容自动打标签相关Celery任务
"""
import asyncio
import os
from datetime import datetime
from celery import current_task
from backend.celery_app import app
from backend.services.content_tagging_service import ContentTaggingService
from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import info, error, warning


@app.task(
    bind=True,
    name='tag_file_content',
    queue='file_processing',
    autoretry_for=(Exception,),
    retry_kwargs={'max_retries': 3, 'countdown': 30},  # 3次重试，首次30秒后
    retry_backoff=True,  # 指数退避
    retry_backoff_max=120,  # 最大退避时间2分钟
    retry_jitter=True  # 添加随机抖动避免雪崩
)
def tag_file_content_task(
    self,
    file_id,
    username=None,
    additional_context="",
    force=False,
    template_id=None,
    template_variables=None,
):
    """
    为文件内容自动打标签的Celery任务

    Args:
        file_id: file_processing_details.id
        username: 用户名（可选）
        additional_context: 额外的上下文信息
        force: 是否强制重新打标签（即使已有标签）
        template_id: 运行时指定的模板 ID (PR-5);
            - 调用方 (上传弹窗 / Re-tag API) 显式选择的模板
            - 优先级最高: runtime > user_template_preferences[file_tagging] > content_tag_v1
        template_variables: 运行时模板变量 (PR-5); 与 additional_context 合并

    Returns:
        打标签结果
    """
    _last_progress = [0.0]
    _last_status = ['processing']
    
    def update_progress(status=None, progress=None, log_message=None, **kwargs):
        """更新任务进度到Redis"""
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
                    'source': 'celery_tagging'
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
            
            current_task.update_state(
                state='PROGRESS',
                meta=meta
            )
            
            if log_message:
                info(f"Tagging {self.request.id}: {log_message}", source="celery_tagging")
                
        except Exception as e:
            error(f"更新打标签任务状态失败: {e}", source="celery_tagging")
    
    try:
        # 检查是否启用自动打标签
        auto_tag_enabled = os.getenv('AUTO_TAG_ENABLED', 'true').lower() == 'true'
        if not auto_tag_enabled and not force:
            info(f"自动打标签功能已禁用，跳过 file_id={file_id}", source="celery_tagging")
            return {
                'status': 'skipped',
                'message': '自动打标签功能已禁用',
                'file_id': file_id
            }
        
        # 检查最小内容长度要求
        min_content_length = int(os.getenv('AUTO_TAG_MIN_CONTENT_LENGTH', '100'))
        
        info(
            f"开始打标签任务 {self.request.id}, file_id={file_id}, "
            f"user={username}, force={force}",
            source="celery_tagging"
        )
        
        update_progress(
            status="initializing",
            progress=5,
            log_message=f"正在初始化打标签任务... (file_id={file_id})"
        )
        
        # 获取数据库连接
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)

        # PR-5: 用 LLMTemplateResolver 解析模板 (runtime > preference > default)
        # 失败时降级到默认模板, 不阻断打标流程
        from backend.utils.user_resolver import get_user_id_by_username
        user_id = get_user_id_by_username(username)
        resolved_template_id = None
        resolved_template_code = None
        try:
            from backend.services.llm_template_resolver import (
                LLMTemplateResolver, ResolveError,
            )
            resolver = LLMTemplateResolver()
            resolved = resolver.resolve(
                module_code='file_tagging',
                user_id=user_id,
                runtime_template_id=template_id,
            )
            resolved_template_id = resolved.template_id
            resolved_template_code = resolved.template_code
            info(
                f"file_tagging 模板解析: source={resolved.source} "
                f"template_id={resolved.template_id} code={resolved.template_code}",
                source="celery_tagging",
            )
        except Exception as e:
            warning(
                f"模板解析失败, 降级到默认模板: {e}",
                source="celery_tagging",
            )

        # 创建打标签服务 (传入解析后的模板)
        tagging_service = ContentTaggingService(
            db_manager=db_manager,
            log_callback=update_progress,
            template_id=resolved_template_id,
            template_code=resolved_template_code,
        )

        update_progress(progress=10, log_message="正在读取文件内容...")
        
        # 获取文件信息
        file_info = tagging_service.get_file_content(file_id)
        if not file_info:
            raise ValueError(f"文件不存在: file_id={file_id}")
        
        filename = file_info.get('filename', 'unknown')
        content_length = file_info.get('content_length', 0)
        
        # 检查内容长度
        if content_length < min_content_length:
            warning(
                f"文件内容太短，跳过打标签: {filename}, length={content_length}",
                source="celery_tagging"
            )
            return {
                'status': 'skipped',
                'message': f'文件内容太短（{content_length} < {min_content_length}），跳过打标签',
                'file_id': file_id,
                'filename': filename
            }
        
        update_progress(
            progress=15,
            log_message=f"文件: {filename}, 内容长度: {content_length} 字符"
        )
        
        # 检查是否已有标签
        if not force:
            existing_tags = tagging_service.get_existing_tags(file_id)
            if existing_tags:
                info(f"文件已有标签，跳过: {filename}", source="celery_tagging")
                update_progress(
                    status="completed",
                    progress=100,
                    log_message=f"文件已有标签，跳过"
                )
                return {
                    'status': 'skipped',
                    'message': '文件已有标签',
                    'file_id': file_id,
                    'filename': filename,
                    'existing_tags': existing_tags
                }
        
        update_progress(
            status="tagging",
            progress=20,
            log_message="正在调用AI分析文件内容..."
        )
        
        # PR-4: user_id 已在上方解析(供 LLMTemplateResolver 使用), 这里直接复用
        # PR-5: 把运行时 template_variables 合并进 additional_context (若有)
        merged_context = additional_context or ""
        if template_variables:
            extras = []
            for k, v in template_variables.items():
                if k in ("file_content",):  # file_content 由 service 注入, 跳过
                    continue
                if v is None or v == "":
                    continue
                extras.append(f"{k}: {v}")
            if extras:
                merged_context = (
                    (merged_context + "\n" if merged_context else "") + "\n".join(extras)
                )

        # 执行打标签（异步）
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                tagging_service.tag_file_content(
                    file_id=file_id,
                    additional_context=merged_context,
                    force=force,
                    user_id=user_id,
                )
            )
        finally:
            loop.close()
        
        # 处理结果
        if result.get('skipped'):
            update_progress(
                status="completed",
                progress=100,
                log_message=result.get('message', '已跳过')
            )
            # PR-4 R3 修复: 与 classify_tasks 对齐, 显式标记 status='skipped'
            # (避免调用方需根据 'skipped' bool 字段判断状态)
            return {
                'status': 'skipped',
                'file_id': file_id,
                'filename': filename,
                **result,
            }
        
        update_progress(
            status="completed",
            progress=100,
            log_message=f"打标签完成！sector={result['tags']['sector']}, "
                       f"industry={result['tags']['industry']}, "
                       f"tickers={len(result['tags'].get('tickers', []))}"
        )
        
        final_result = {
            'status': 'completed',
            'file_id': file_id,
            'filename': filename,
            'tag_id': result['tag_id'],
            'tags': result['tags'],
            'processing_time': result['processing_time'],
            'ai_metadata': result.get('ai_metadata', {}),
            'logs': getattr(update_progress, 'logs', [])
        }
        
        info(
            f"打标签任务 {self.request.id} 完成: {filename}, "
            f"sector={result['tags']['sector']}, "
            f"耗时={result['processing_time']:.2f}秒",
            source="celery_tagging"
        )
        
        return final_result
        
    except Exception as e:
        error_msg = f"打标签任务失败: {str(e)}"
        error(f"Tagging {self.request.id}: {error_msg}", source="celery_tagging")
        
        # 返回失败结果而不是抛出异常（避免Celery序列化错误）
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
    name='batch_tag_files',
    queue='file_processing'
)
def batch_tag_files_task(
    self, file_ids, username=None, force=False,
    template_id=None, template_variables=None,
):
    """
    批量为文件打标签

    Args:
        file_ids: 文件ID列表
        username: 用户名
        force: 是否强制重新打标签
        template_id: 运行时指定的 file_tagging 模板 ID (PR-5); 应用到所有文件
        template_variables: 模板变量 (PR-5)

    Returns:
        批量处理结果
    """
    info(
        f"开始批量打标签任务 {self.request.id}, "
        f"files={len(file_ids)}, user={username}",
        source="celery_tagging"
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
            # 更新进度
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
            
            # 异步调用单个文件打标签任务
            # PR-5: 把 template_id / variables 透传给单文件任务
            task_result = tag_file_content_task.apply_async(
                args=(file_id, username, "", force),
                kwargs={
                    'template_id': template_id,
                    'template_variables': template_variables,
                },
                countdown=1  # 延迟1秒，避免并发过高
            )
            
            # 等待结果（设置超时）
            result = task_result.get(timeout=300)  # 5分钟超时
            
            if result.get('status') == 'completed':
                results['success'] += 1
            elif result.get('status') == 'skipped':
                results['skipped'] += 1
            else:
                results['failed'] += 1
            
            results['details'].append({
                'file_id': file_id,
                'status': result.get('status'),
                'message': result.get('message', '')
            })
            
        except Exception as e:
            error(f"批量打标签-文件 {file_id} 失败: {str(e)}", source="celery_tagging")
            results['failed'] += 1
            results['details'].append({
                'file_id': file_id,
                'status': 'failed',
                'error': str(e)
            })
    
    info(
        f"批量打标签任务 {self.request.id} 完成: "
        f"成功={results['success']}, 失败={results['failed']}, 跳过={results['skipped']}",
        source="celery_tagging"
    )
    
    return results
