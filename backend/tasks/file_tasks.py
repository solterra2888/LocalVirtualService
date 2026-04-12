# -*- coding: utf-8 -*-
"""
文件处理相关Celery任务
"""
import asyncio
import os
import json
import time
import uuid
from datetime import datetime
from celery import current_task
from backend.celery_app import app
from backend.workers.file_worker import FileProcessingWorker
from backend.utils.database_config import get_database_config
from backend.utils.logger import info, error, warning

# 导入打标签任务（延迟导入避免循环依赖）
def get_tagging_task():
    """延迟导入打标签任务"""
    from backend.tasks.tagging_tasks import tag_file_content_task
    return tag_file_content_task


def trigger_auto_tagging(file_detail_ids, username=None, delay=2):
    """
    触发自动打标签任务
    
    Args:
        file_detail_ids: 文件detail ID列表或单个ID
        username: 用户名
        delay: 延迟执行时间（秒），默认2秒，确保DB事务已提交
    """
    # 检查是否启用自动打标签
    auto_tag_enabled = os.getenv('AUTO_TAG_ENABLED', 'true').lower() == 'true'
    if not auto_tag_enabled:
        info("自动打标签功能已禁用", source="auto_tagging")
        return
    
    # 确保是列表
    if not isinstance(file_detail_ids, list):
        file_detail_ids = [file_detail_ids]
    
    if not file_detail_ids:
        return
    
    tag_task = get_tagging_task()
    
    # 异步调用每个文件的打标签任务
    for file_id in file_detail_ids:
        try:
            tag_task.apply_async(
                args=(file_id, username),
                countdown=delay
            )
            info(f"已触发自动打标签任务: file_id={file_id}, delay={delay}s", source="auto_tagging")
        except Exception as e:
            error(f"触发打标签任务失败: file_id={file_id}, error={str(e)}", source="auto_tagging")

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
    
    # 用可变对象保存上次进度，避免仅传 log_message 时把 progress 覆盖为 0
    _last_progress = [0.0]
    _last_status = ['processing']

    def update_progress(status=None, progress=None, current_file=None, log_message=None, **kwargs):
        """更新任务进度到Redis。未传 progress/status 时保留上次值，避免被覆盖为 0。"""
        try:
            if progress is not None:
                _last_progress[0] = progress
            if status is not None:
                _last_status[0] = status

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
                'status': status if status is not None else _last_status[0],
                'progress': _last_progress[0],
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
        task_user_id = params.get('user_id') if params else None
        worker = FileProcessingWorker(self.request.id, username, db_conn_params, user_id=task_user_id)
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
            api_key = os.getenv('LLAMA_CLOUD_API_KEY')
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
        
        # 🏷️ 触发自动打标签任务
        try:
            # 查询该任务创建的所有file_processing_details ID
            db_conn_params = get_database_config()
            from backend.database.db_manager import DatabaseManager
            db_manager = DatabaseManager(db_conn_params)
            conn = db_manager.get_connection()
            
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT id FROM file_processing_details
                    WHERE task_id = %s AND status = 'completed' AND content IS NOT NULL AND content != ''
                """, (self.request.id,))
                
                file_ids = [row[0] for row in cursor.fetchall()]
                conn.close()
                
                if file_ids:
                    info(f"触发自动打标签: {len(file_ids)} 个文件", source="celery_task")
                    trigger_auto_tagging(file_ids, username, delay=int(os.getenv('AUTO_TAG_DELAY', '2')))
        except Exception as e:
            error(f"触发自动打标签失败: {str(e)}", source="celery_task")
        
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
        
        # 返回失败结果而不是抛出异常（避免Celery序列化错误）
        return {
            'task_id': self.request.id,
            'status': 'failed',
            'error': error_msg,
            'files_processed': 0,
            'files_failed': len(file_paths) if 'file_paths' in locals() else 0
        }


@app.task(bind=True, name='analyze_audio_async', queue='file_processing')
def analyze_audio_task(self, execution_id, file_detail_id, template_id, template_variables, username):
    """
    异步音频AI分析任务

    从 file_processing_details 读取 asr_with_diarization 数据，
    调用 LocalLLMAnalysisService 使用指定模板分析音频内容，
    将分析报告写回 processing_stats.final_report。

    Args:
        execution_id: processing_stats.id
        file_detail_id: file_processing_details.id
        template_id: 模板ID
        template_variables: 额外模板变量
        username: 用户名
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
                update_progress.logs.append({
                    'timestamp': datetime.now().isoformat(),
                    'level': 'info',
                    'message': log_message,
                    'source': 'celery_audio_analysis'
                })
                if len(update_progress.logs) > 50:
                    update_progress.logs = update_progress.logs[-50:]
            current_task.update_state(
                state='PROGRESS',
                meta={
                    'status': _last_status[0],
                    'progress': _last_progress[0],
                    'message': log_message or '',
                    'execution_id': execution_id,
                    'username': username,
                    'logs': update_progress.logs
                }
            )
            if log_message:
                info(f"AudioAnalysis {self.request.id}: {log_message}", source="celery_audio_analysis")
        except Exception as e:
            error(f"更新音频分析任务状态失败: {e}", source="celery_audio_analysis")

    try:
        info(f"开始音频AI分析任务 {self.request.id}, execution_id={execution_id}, user={username}", source="celery_audio_analysis")
        db_conn_params = get_database_config()

        update_progress(status="initializing", progress=5, log_message="正在初始化音频分析任务...")

        # 1. 从数据库读取 ASR 数据
        from backend.database.db_manager import DatabaseManager
        db_manager = DatabaseManager(db_conn_params)
        conn = db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")

        cursor = conn.cursor()
        cursor.execute("""
            SELECT filename, content, asr_with_diarization
            FROM file_processing_details
            WHERE id = %s
        """, (file_detail_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            raise ValueError(f"找不到文件记录: file_detail_id={file_detail_id}")

        filename, content_text, asr_json = row[0], row[1], row[2]

        update_progress(progress=15, log_message=f"已读取文件数据: {filename}")

        # 2. 构建用于LLM分析的文本内容
        # 优先使用 asr_with_diarization 的结构化数据，否则使用 content
        file_content_for_llm = ""
        if asr_json:
            asr_data = asr_json if isinstance(asr_json, dict) else json.loads(asr_json)
            sentences = asr_data.get('sentences', [])
            speakers = {s['speaker_id']: s.get('label', f"Speaker {s['speaker_id']}") for s in asr_data.get('speakers', [])}

            lines = []
            for sent in sentences:
                spk_id = sent.get('speaker_id', 0)
                spk_label = speakers.get(spk_id, f"Speaker {spk_id}")
                begin_sec = sent.get('begin_time', 0) / 1000
                end_sec = sent.get('end_time', 0) / 1000
                begin_str = f"{int(begin_sec // 60):02d}:{int(begin_sec % 60):02d}"
                end_str = f"{int(end_sec // 60):02d}:{int(end_sec % 60):02d}"
                lines.append(f"[{begin_str}-{end_str}] {spk_label}: {sent.get('text', '')}")
            file_content_for_llm = "\n".join(lines)

            update_progress(progress=25, log_message=f"已格式化 {len(sentences)} 条ASR句子，{len(speakers)} 位说话人")
        elif content_text:
            file_content_for_llm = content_text
            update_progress(progress=25, log_message="使用已有转写文本内容进行分析")
        else:
            raise ValueError("该音频文件没有ASR数据或转写文本，无法进行AI分析")

        # 3. 调用 LocalLLMAnalysisService
        update_progress(status="ai_processing", progress=35, log_message="正在调用AI分析...")

        from backend.services.local_llm_analysis_service import LocalLLMAnalysisService
        llm_service = LocalLLMAnalysisService(
            template_id=template_id,
            log_callback=update_progress
        )

        # 准备变量
        variables = template_variables.copy() if template_variables else {}
        # file_content 由模板定义，此处注入 ASR 格式化文本
        # (variables 中若用户没有指定 additional_instructions，会自动用模板 default)

        # 构造 successful_results，复用 process_all_contents
        successful_results = [{
            'content': file_content_for_llm,
            'original_file': filename,
            'content_length': len(file_content_for_llm),
            'processing_time': 0
        }]

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            final_report, file_ai_times = loop.run_until_complete(
                llm_service.process_all_contents(
                    successful_results,
                    custom_variables=variables,
                    user_id=None
                )
            )
        finally:
            loop.close()

        update_progress(progress=85, log_message="AI分析完成，正在保存结果...")

        # 4. 将结果写回数据库
        total_tokens = sum(f.get('total_tokens', 0) for f in (file_ai_times or []))
        total_price = sum(f.get('total_price', 0) for f in (file_ai_times or []))
        total_ai_time = sum(f.get('ai_time', 0) for f in (file_ai_times or []))

        llm_model = None
        llm_provider = None
        if file_ai_times:
            for ft in file_ai_times:
                mu = ft.get('model_usage', [])
                if mu:
                    llm_model = mu[0].get('model_name')
                    llm_provider = ft.get('provider')
                    break

        conn = db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库保存分析结果")

        cursor = conn.cursor()
        # 更新 processing_stats
        cursor.execute("""
            UPDATE processing_stats
            SET final_report = %s,
                rc_agent_status = %s,
                total_ai_time = %s,
                total_tokens = %s,
                total_price = %s,
                task_name = %s,
                llm_model = %s,
                llm_provider = %s
            WHERE id = %s
        """, (
            final_report or '',
            'success' if final_report else 'failed',
            total_ai_time,
            total_tokens,
            total_price,
            '会议录音分析 v1',
            llm_model,
            llm_provider,
            execution_id
        ))

        # 更新 file_processing_details 的 ai_time, total_tokens, cost
        cursor.execute("""
            UPDATE file_processing_details
            SET ai_time = %s,
                total_tokens = %s,
                cost = %s
            WHERE id = %s
        """, (
            total_ai_time,
            total_tokens,
            total_price,
            file_detail_id
        ))

        conn.commit()
        conn.close()

        update_progress(status="completed", progress=100, log_message="音频AI分析完成！")

        final_result = {
            'status': 'completed',
            'execution_id': execution_id,
            'final_report': final_report is not None,
            'total_tokens': total_tokens,
            'total_price': total_price,
            'total_ai_time': total_ai_time,
            'llm_model': llm_model,
            'llm_provider': llm_provider,
            'logs': getattr(update_progress, 'logs', [])
        }
        info(f"音频分析任务 {self.request.id} 完成: {final_result}", source="celery_audio_analysis")
        
        # 🏷️ 触发自动打标签任务
        try:
            trigger_auto_tagging(file_detail_id, username, delay=int(os.getenv('AUTO_TAG_DELAY', '2')))
        except Exception as e:
            error(f"触发自动打标签失败: {str(e)}", source="celery_audio_analysis")
        
        return final_result

    except Exception as e:
        error_msg = f"音频分析任务失败: {str(e)}"
        error(f"AudioAnalysis {self.request.id}: {error_msg}", source="celery_audio_analysis")

        # 尝试更新数据库状态为失败
        try:
            db_conn_params = get_database_config()
            from backend.database.db_manager import DatabaseManager
            db_manager = DatabaseManager(db_conn_params)
            conn = db_manager.get_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE processing_stats
                    SET rc_agent_status = 'failed'
                    WHERE id = %s
                """, (execution_id,))
                conn.commit()
                conn.close()
        except Exception:
            pass

        current_task.update_state(
            state='FAILURE',
            meta={
                'error': str(error_msg),
                'status': 'failed',
                'execution_id': execution_id
            }
        )
        raise Exception(error_msg)


@app.task(bind=True, name='analyze_document_async', queue='file_processing')
def analyze_document_task(self, execution_id, file_detail_id, template_id, template_variables, username):
    """
    异步文档AI分析任务

    对已上传但未分析的文档执行：
    1. 如果 content 为空，先用 LlamaParse 解析文档
    2. 使用指定 Prompt Template 进行AI分析
    3. 将结果写回 processing_stats.final_report

    Args:
        execution_id: processing_stats.id
        file_detail_id: file_processing_details.id
        template_id: 模板ID
        template_variables: 额外模板变量
        username: 用户名
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
                update_progress.logs.append({
                    'timestamp': datetime.now().isoformat(),
                    'level': 'info',
                    'message': log_message,
                    'source': 'celery_doc_analysis'
                })
                if len(update_progress.logs) > 50:
                    update_progress.logs = update_progress.logs[-50:]
            current_task.update_state(
                state='PROGRESS',
                meta={
                    'status': _last_status[0],
                    'progress': _last_progress[0],
                    'message': log_message or '',
                    'execution_id': execution_id,
                    'username': username,
                    'logs': update_progress.logs
                }
            )
            if log_message:
                info(f"DocAnalysis {self.request.id}: {log_message}", source="celery_doc_analysis")
        except Exception as e:
            error(f"更新文档分析任务状态失败: {e}", source="celery_doc_analysis")

    try:
        info(f"开始文档AI分析任务 {self.request.id}, execution_id={execution_id}, user={username}", source="celery_doc_analysis")
        db_conn_params = get_database_config()

        update_progress(status="initializing", progress=5, log_message="正在初始化文档分析任务...")

        # 1. 从数据库读取文件信息
        from backend.database.db_manager import DatabaseManager
        db_manager = DatabaseManager(db_conn_params)
        conn = db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库")

        cursor = conn.cursor()
        cursor.execute("""
            SELECT filename, content, original_file_path, file_type, file_storage_type, oss_bucket
            FROM file_processing_details
            WHERE id = %s
        """, (file_detail_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            raise ValueError(f"找不到文件记录: file_detail_id={file_detail_id}")

        filename, content_text, original_file_path, file_type, file_storage_type, oss_bucket = row[0], row[1], row[2], row[3], row[4], row[5]

        update_progress(progress=10, log_message=f"已读取文件数据: {filename}")

        # 2. 如果 content 为空，需要先用 LlamaParse 解析文档
        file_content_for_llm = content_text.strip() if content_text else ""
        doc_processing_time = 0
        temp_file_path = None  # 用于存储临时下载的文件路径

        if not file_content_for_llm:
            if not original_file_path:
                raise ValueError(f"文档文件路径为空，无法进行解析")
            
            # 检查文件存储类型
            local_file_path = original_file_path
            
            if file_storage_type == 'oss':
                # 文件存储在 OSS，需要先下载到本地临时目录
                update_progress(progress=12, log_message=f"文件存储在 OSS，正在下载: {original_file_path}")
                
                from backend.services.oss_service import OSSService
                oss_service = OSSService()
                
                # 创建临时目录
                temp_dir = os.path.join(os.getcwd(), 'uploads', 'temp_oss_download')
                os.makedirs(temp_dir, exist_ok=True)
                
                # 生成临时文件路径（保留原始文件扩展名）
                import uuid
                file_ext = os.path.splitext(filename)[1]
                temp_file_path = os.path.join(temp_dir, f"{uuid.uuid4().hex}{file_ext}")
                
                # 从 OSS 下载文件
                try:
                    oss_service.download_to_file(original_file_path, temp_file_path)
                    local_file_path = temp_file_path
                    update_progress(progress=14, log_message=f"OSS 文件下载完成，临时路径: {os.path.basename(temp_file_path)}")
                except Exception as e:
                    raise ValueError(f"从 OSS 下载文件失败: {str(e)}")
            else:
                # 文件存储在本地
                if not os.path.exists(original_file_path):
                    raise ValueError(f"本地文档文件不存在: {original_file_path}，无法进行解析")

            update_progress(status="parsing", progress=15, log_message="文档内容为空，正在使用LlamaParse解析文档...")

            api_key = os.getenv('LLAMA_CLOUD_API_KEY')
            if not api_key:
                raise ValueError("LlamaIndex API密钥未配置，无法解析文档")

            from backend.core.llama_service import LlamaService
            llama_service = LlamaService(api_key=api_key, polling_interval=10)

            successful_results = []
            failed_files = []

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    llama_service.process_file(local_file_path, successful_results, failed_files, None)
                )
            finally:
                loop.close()
                # 清理临时下载的文件
                if temp_file_path and os.path.exists(temp_file_path):
                    try:
                        os.remove(temp_file_path)
                        info(f"已清理临时文件: {temp_file_path}", source="celery_doc_analysis")
                    except Exception as e:
                        error(f"清理临时文件失败: {e}", source="celery_doc_analysis")

            if successful_results:
                file_content_for_llm = successful_results[0].get('content', '')
                doc_processing_time = successful_results[0].get('processing_time', 0)
                update_progress(progress=55, log_message=f"文档解析完成，内容长度: {len(file_content_for_llm)} 字符")

                # 更新 file_processing_details 的 content
                conn = db_manager.get_connection()
                if conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        UPDATE file_processing_details
                        SET content = %s,
                            content_length = %s,
                            processing_time = %s,
                            status = 'completed'
                        WHERE id = %s
                    """, (file_content_for_llm, len(file_content_for_llm), doc_processing_time, file_detail_id))
                    conn.commit()
                    conn.close()
            else:
                detail = failed_files[0].get("error") if failed_files else ""
                if detail:
                    raise ValueError(f"LlamaParse 解析失败: {detail}")
                raise ValueError(f"LlamaParse解析文档失败: {filename}")
        else:
            update_progress(progress=55, log_message=f"使用已有文档内容进行分析，内容长度: {len(file_content_for_llm)} 字符")

        if not file_content_for_llm:
            raise ValueError("文档内容为空，无法进行AI分析")

        # 3. 调用 LocalLLMAnalysisService
        update_progress(status="ai_processing", progress=60, log_message="正在调用AI分析...")

        from backend.services.local_llm_analysis_service import LocalLLMAnalysisService
        llm_service = LocalLLMAnalysisService(
            template_id=template_id,
            log_callback=update_progress
        )

        variables = template_variables.copy() if template_variables else {}

        successful_results_for_llm = [{
            'content': file_content_for_llm,
            'original_file': filename,
            'content_length': len(file_content_for_llm),
            'processing_time': doc_processing_time
        }]

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            final_report, file_ai_times = loop.run_until_complete(
                llm_service.process_all_contents(
                    successful_results_for_llm,
                    custom_variables=variables,
                    user_id=None
                )
            )
        finally:
            loop.close()

        update_progress(progress=90, log_message="AI分析完成，正在保存结果...")

        # 4. 将结果写回数据库
        total_tokens = sum(f.get('total_tokens', 0) for f in (file_ai_times or []))
        total_price = sum(f.get('total_price', 0) for f in (file_ai_times or []))
        total_ai_time = sum(f.get('ai_time', 0) for f in (file_ai_times or []))

        llm_model = None
        llm_provider = None
        template_name = None
        if file_ai_times:
            for ft in file_ai_times:
                mu = ft.get('model_usage', [])
                if mu:
                    llm_model = mu[0].get('model_name')
                    llm_provider = ft.get('provider')
                    break

        # 获取模板名称作为 task_name
        try:
            from backend.services.llm_prompt_template_service import LLMPromptTemplateService
            ts = LLMPromptTemplateService()
            tpl = ts.get_template(template_id)
            if tpl:
                template_name = tpl.get('template_name')
        except Exception:
            pass

        conn = db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库保存分析结果")

        cursor = conn.cursor()
        cursor.execute("""
            UPDATE processing_stats
            SET final_report = %s,
                rc_agent_status = %s,
                total_ai_time = %s,
                total_file_processing_time = %s,
                total_tokens = %s,
                total_price = %s,
                task_name = %s,
                llm_model = %s,
                llm_provider = %s
            WHERE id = %s
        """, (
            final_report or '',
            'success' if final_report else 'failed',
            total_ai_time,
            doc_processing_time,
            total_tokens,
            total_price,
            template_name or f'Template ID:{template_id}',
            llm_model,
            llm_provider,
            execution_id
        ))

        cursor.execute("""
            UPDATE file_processing_details
            SET ai_time = %s,
                total_tokens = %s,
                cost = %s
            WHERE id = %s
        """, (
            total_ai_time,
            total_tokens,
            total_price,
            file_detail_id
        ))

        conn.commit()
        conn.close()

        update_progress(status="completed", progress=100, log_message="文档AI分析完成！")

        final_result = {
            'status': 'completed',
            'execution_id': execution_id,
            'final_report': final_report is not None,
            'total_tokens': total_tokens,
            'total_price': total_price,
            'total_ai_time': total_ai_time,
            'llm_model': llm_model,
            'llm_provider': llm_provider,
            'logs': getattr(update_progress, 'logs', [])
        }
        info(f"文档分析任务 {self.request.id} 完成: {final_result}", source="celery_doc_analysis")
        
        # 🏷️ 触发自动打标签任务
        try:
            trigger_auto_tagging(file_detail_id, username, delay=int(os.getenv('AUTO_TAG_DELAY', '2')))
        except Exception as e:
            error(f"触发自动打标签失败: {str(e)}", source="celery_doc_analysis")
        
        return final_result

    except Exception as e:
        error_msg = f"文档分析任务失败: {str(e)}"
        error(f"DocAnalysis {self.request.id}: {error_msg}", source="celery_doc_analysis")

        # 清理临时文件
        if 'temp_file_path' in locals() and temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
                info(f"已清理临时文件: {temp_file_path}", source="celery_doc_analysis")
            except Exception as cleanup_error:
                error(f"清理临时文件失败: {cleanup_error}", source="celery_doc_analysis")

        try:
            db_conn_params = get_database_config()
            from backend.database.db_manager import DatabaseManager
            db_manager = DatabaseManager(db_conn_params)
            conn = db_manager.get_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE processing_stats
                    SET rc_agent_status = 'failed'
                    WHERE id = %s
                """, (execution_id,))
                conn.commit()
                conn.close()
        except Exception:
            pass

        current_task.update_state(
            state='FAILURE',
            meta={
                'error': str(error_msg),
                'status': 'failed',
                'execution_id': execution_id
            }
        )
        raise Exception(error_msg)


@app.task(bind=True, name='reparse_document_async', queue='file_processing')
def reparse_document_task(self, file_id, params, username):
    """
    异步重新解析文档任务（Re-parse）
    
    用于文档解析失败或内容为空时，重新调用 LlamaParse 解析文档。
    解析成功后自动触发打标签任务。
    
    Args:
        file_id: file_processing_details.id
        params: LlamaParse 配置参数（language, fast_mode 等）
        username: 用户名
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
                update_progress.logs.append({
                    'timestamp': datetime.now().isoformat(),
                    'level': 'info',
                    'message': log_message,
                    'source': 'celery_reparse'
                })
                if len(update_progress.logs) > 50:
                    update_progress.logs = update_progress.logs[-50:]
            current_task.update_state(
                state='PROGRESS',
                meta={
                    'status': _last_status[0],
                    'progress': _last_progress[0],
                    'message': log_message or '',
                    'file_id': file_id,
                    'username': username,
                    'logs': update_progress.logs
                }
            )
            if log_message:
                info(f"Reparse {self.request.id}: {log_message}", source="celery_reparse")
        except Exception as e:
            error(f"更新重新解析任务状态失败: {e}", source="celery_reparse")
    
    temp_file_path = None
    
    try:
        info(f"开始文档重新解析任务 {self.request.id}, file_id={file_id}, user={username}", 
             source="celery_reparse")
        
        update_progress(status="initializing", progress=5, log_message="正在初始化重新解析任务...")
        
        # 1. 从数据库读取文件信息
        from backend.database.db_manager import DatabaseManager
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        conn = db_manager.get_connection()
        
        if not conn:
            raise ValueError("无法连接到数据库")
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT filename, original_file_path, file_type, file_storage_type, 
                   oss_bucket, processing_stats_id
            FROM file_processing_details
            WHERE id = %s
        """, (file_id,))
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            raise ValueError(f"找不到文件记录: file_id={file_id}")
        
        filename, original_file_path, file_type, file_storage_type, oss_bucket, processing_stats_id = row
        
        update_progress(progress=10, log_message=f"已读取文件信息: {filename}")
        
        if not original_file_path:
            raise ValueError(f"文件路径为空，无法重新解析")
        
        # 2. 更新数据库状态为 processing
        conn = db_manager.get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE file_processing_details
                SET status = 'processing', content = '', content_length = 0
                WHERE id = %s
            """, (file_id,))
            cursor.execute("""
                UPDATE processing_stats
                SET rc_agent_status = 'processing'
                WHERE id = %s
            """, (processing_stats_id,))
            conn.commit()
            conn.close()
        
        update_progress(progress=15, log_message="已更新状态为处理中...")
        
        # 3. 获取文件本地路径
        local_file_path = original_file_path
        
        if file_storage_type == 'oss':
            # 文件存储在 OSS，需要先下载到本地临时目录
            update_progress(progress=20, log_message=f"文件存储在 OSS，正在下载: {original_file_path}")
            
            from backend.services.oss_service import OSSService
            oss_service = OSSService()
            
            # 创建临时目录
            temp_dir = os.path.join(os.getcwd(), 'uploads', 'temp_reparse')
            os.makedirs(temp_dir, exist_ok=True)
            
            # 生成临时文件路径（保留原始文件扩展名）
            import uuid
            file_ext = os.path.splitext(filename)[1]
            temp_file_path = os.path.join(temp_dir, f"{uuid.uuid4().hex}{file_ext}")
            
            # 从 OSS 下载文件
            try:
                oss_service.download_to_file(original_file_path, temp_file_path)
                local_file_path = temp_file_path
                update_progress(progress=30, log_message=f"OSS 文件下载完成，临时路径: {os.path.basename(temp_file_path)}")
            except Exception as e:
                raise ValueError(f"从 OSS 下载文件失败: {str(e)}")
        else:
            # 文件存储在本地
            if not os.path.exists(original_file_path):
                raise ValueError(f"本地文件不存在: {original_file_path}")
            update_progress(progress=30, log_message=f"使用本地文件: {original_file_path}")
        
        # 4. 根据 processing_mode 选择解析服务
        processing_mode = params.get('processing_mode', 'llamaparse')
        
        parse_params = {
            'language': params.get('language', ['ch_sim', 'en']),
            'fast_mode': params.get('fast_mode', False),
            'disable_ocr': params.get('disable_ocr', False),
            'max_pages': params.get('max_pages', 10),
            'target_pages': params.get('target_pages', ''),
            'skip_diagonal_text': params.get('skip_diagonal_text', False),
            'skip_table_extraction': params.get('skip_table_extraction', False),
            'extract_images': params.get('extract_images', False),
            'use_vendor_multimodal_model': params.get('use_vendor_multimodal_model', False),
            'vendor_multimodal_api_key': params.get('vendor_multimodal_api_key', ''),
            'polling_interval': params.get('polling_interval', 10)
        }
        
        if processing_mode == 'quick':
            update_progress(status="parsing", progress=35, log_message="正在使用 Quick Process 解析文档...")
            from backend.core.quick_parse_service import QuickParseService
            parse_service = QuickParseService()
        else:
            update_progress(status="parsing", progress=35, log_message="正在使用 LlamaParse 解析文档...")
            api_key = os.getenv('LLAMA_CLOUD_API_KEY')
            if not api_key:
                raise ValueError("LlamaIndex API 密钥未配置，无法解析文档")
            from backend.core.llama_service import LlamaService
            parse_service = LlamaService(
                api_key=api_key,
                polling_interval=parse_params['polling_interval']
            )
        
        successful_results = []
        failed_files = []
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result = loop.run_until_complete(
                parse_service.process_file(local_file_path, successful_results, failed_files, parse_params)
            )
        finally:
            loop.close()
            # 清理临时下载的文件
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                    info(f"已清理临时文件: {temp_file_path}", source="celery_reparse")
                except Exception as e:
                    error(f"清理临时文件失败: {e}", source="celery_reparse")
        
        if not successful_results:
            mode_label = "Quick Process" if processing_mode == "quick" else "LlamaParse"
            detail = failed_files[0].get("error") if failed_files else ""
            if detail:
                raise ValueError(f"{mode_label} 解析失败: {detail}")
            raise ValueError(f"{mode_label} 解析文档失败: {filename}")
        
        file_content = successful_results[0].get('content', '')
        doc_processing_time = successful_results[0].get('processing_time', 0)
        
        if not file_content:
            raise ValueError("解析结果为空，无法保存")
        
        update_progress(progress=85, log_message=f"文档解析完成，内容长度: {len(file_content)} 字符")
        
        # 5. 更新数据库
        update_progress(progress=90, log_message="正在保存解析结果...")
        
        conn = db_manager.get_connection()
        if not conn:
            raise ValueError("无法连接到数据库保存结果")
        
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE file_processing_details
            SET content = %s,
                content_length = %s,
                processing_time = %s,
                status = 'completed'
            WHERE id = %s
        """, (file_content, len(file_content), doc_processing_time, file_id))
        
        cursor.execute("""
            UPDATE processing_stats
            SET rc_agent_status = 'completed',
                total_file_processing_time = %s
            WHERE id = %s
        """, (doc_processing_time, processing_stats_id))
        
        conn.commit()
        conn.close()
        
        update_progress(status="completed", progress=100, log_message="重新解析完成！")
        
        final_result = {
            'status': 'completed',
            'file_id': file_id,
            'content_length': len(file_content),
            'processing_time': doc_processing_time,
            'logs': getattr(update_progress, 'logs', [])
        }
        
        info(f"文档重新解析任务 {self.request.id} 完成: file_id={file_id}", source="celery_reparse")
        
        # 🏷️ 触发自动打标签任务
        try:
            trigger_auto_tagging(file_id, username, delay=int(os.getenv('AUTO_TAG_DELAY', '2')))
        except Exception as e:
            error(f"触发自动打标签失败: {str(e)}", source="celery_reparse")
        
        return final_result
    
    except Exception as e:
        error_msg = f"文档重新解析任务失败: {str(e)}"
        error(f"Reparse {self.request.id}: {error_msg}", source="celery_reparse")
        
        # 清理临时文件
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except Exception:
                pass
        
        # 更新数据库状态为失败
        try:
            db_conn_params = get_database_config()
            from backend.database.db_manager import DatabaseManager
            db_manager = DatabaseManager(db_conn_params)
            conn = db_manager.get_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE file_processing_details
                    SET status = 'failed', error_message = %s
                    WHERE id = %s
                """, (str(e), file_id))
                cursor.execute("""
                    UPDATE processing_stats
                    SET rc_agent_status = 'failed'
                    WHERE id = (SELECT processing_stats_id FROM file_processing_details WHERE id = %s)
                """, (file_id,))
                conn.commit()
                conn.close()
        except Exception:
            pass
        
        # 重新抛出异常，使 Celery 正确标记为 FAILURE 并序列化异常信息（避免 exc_type KeyError）
        raise RuntimeError(error_msg)


@app.task(bind=True, name='upload_and_transcribe_audio', queue='file_processing')
def upload_and_transcribe_audio_task(self, temp_file_path, filename, username, model='fun-asr', 
                                      language_hints=None, diarization_enabled=True):
    """
    异步上传音频文件到 OSS 并进行语音识别
    
    这个任务避免阻塞主 API 线程，适合处理大文件（200MB+）
    
    Args:
        temp_file_path: 临时文件路径
        filename: 原始文件名
        username: 用户名
        model: ASR 模型名称
        language_hints: 语言提示列表
        diarization_enabled: 是否启用说话人分离
    """
    _last_progress = [0.0]
    _last_status = ['processing']
    
    if language_hints is None:
        language_hints = ['zh', 'en']
    
    def update_progress(status=None, progress=None, log_message=None, **meta_extra):
        try:
            if progress is not None:
                _last_progress[0] = progress
            if status is not None:
                _last_status[0] = status
            if not hasattr(update_progress, 'logs'):
                update_progress.logs = []
            if log_message:
                update_progress.logs.append({
                    'timestamp': datetime.now().isoformat(),
                    'level': 'info',
                    'message': log_message,
                    'source': 'celery_audio_upload'
                })
                if len(update_progress.logs) > 50:
                    update_progress.logs = update_progress.logs[-50:]
            meta = {
                'status': _last_status[0],
                'progress': _last_progress[0],
                'message': log_message or '',
                'filename': filename,
                'username': username,
                'logs': update_progress.logs
            }
            for k, v in meta_extra.items():
                if v is not None:
                    meta[k] = v
            current_task.update_state(
                state='PROGRESS',
                meta=meta
            )
            if log_message:
                info(f"AudioUpload {self.request.id}: {log_message}", source="celery_audio_upload")
        except Exception as e:
            error(f"更新音频上传任务状态失败: {e}", source="celery_audio_upload")
    
    try:
        task_start_time = time.time()
        info(f"开始音频上传和转写任务 {self.request.id}, file={filename}, user={username}", 
             source="celery_audio_upload")
        
        update_progress(status="uploading", progress=5, log_message=f"开始上传音频文件: {filename}")
        
        # 1. 上传到 OSS
        from backend.services.oss_service import OSSService
        oss_service = OSSService()
        
        if not oss_service.is_configured():
            raise ValueError("OSS 服务未配置")
        
        # 检查文件是否存在
        if not os.path.exists(temp_file_path):
            raise ValueError(f"临时文件不存在: {temp_file_path}")
        
        file_size = os.path.getsize(temp_file_path)
        file_size_mb = file_size / (1024 * 1024)
        update_progress(progress=10, log_message=f"文件大小: {file_size_mb:.2f} MB")
        
        # 上传到 OSS
        upload_result = oss_service.upload_audio_file(
            file_path=temp_file_path,
            filename=filename,
            username=username
        )
        
        if not upload_result["success"]:
            err = upload_result.get("error") or "未知错误"
            raise Exception(f"上传到 OSS 失败: {str(err)}")
        
        # 优先使用签名 URL，否则 ASR 拉取私有桶会 403（FILE_403_FORBIDDEN）
        audio_url = upload_result.get("signed_url") or upload_result["public_url"]
        object_name = upload_result["object_name"]
        
        update_progress(status="transcribing", progress=30, 
                       log_message=f"文件已上传到 OSS，开始语音识别...")
        
        # 2. 调用 ASR 服务
        from backend.services.asr_service import ASRService
        asr_service = ASRService()
        
        if not asr_service.is_configured():
            raise ValueError("ASR 服务未配置")
        
        # 提交 ASR 任务（使用 signed_url 以便私有桶可被 ASR 拉取）
        submit_result = asr_service.submit_transcription_task(
            audio_url=audio_url,
            model=model,
            language_hints=language_hints,
            diarization_enabled=diarization_enabled
        )
        
        if not submit_result["success"]:
            raise ValueError(f"提交 ASR 任务失败: {submit_result['error']}")
        
        task_id = submit_result["task_id"]
        update_progress(
            progress=40,
            log_message=f"ASR 任务已提交: {task_id}",
            asr_wait_notice=True,
        )
        
        # 3. 等待 ASR 完成
        transcription_result = asr_service.get_transcription_results(task_id)
        
        if not transcription_result["success"]:
            raise ValueError(f"ASR 识别失败: {transcription_result['error']}")
        
        update_progress(progress=80, log_message="语音识别完成，正在保存结果...")
        
        # 4. 格式化结果
        # 仅保存带时间戳的转写结果，每段一行，用于写入 DB content
        formatted_texts = []
        if transcription_result.get("transcriptions"):
            for trans_data in transcription_result["transcriptions"]:
                formatted = asr_service.format_transcription_result(
                    trans_data,
                    include_timestamps=True,
                    include_words=False,
                    include_speaker=diarization_enabled,
                    timestamped_only=True
                )
                formatted_texts.append(formatted)
        
        final_report_text = "\n".join(formatted_texts) if formatted_texts else ""
        
        # 提取纯文本
        texts = []
        for trans in transcription_result["transcriptions"]:
            for transcript in trans.get('transcripts', []):
                text = transcript.get('text', '')
                if text:
                    texts.append(text)
        plain_text = "\n".join(texts)
        
        # 5. 构建结构化 ASR 数据
        asr_structured_data = None
        if transcription_result.get("transcriptions") and diarization_enabled:
            try:
                from backend.api.files import _build_asr_structured_data
                asr_structured_data = _build_asr_structured_data(transcription_result["transcriptions"])
            except Exception as e:
                warning(f"构建结构化 ASR 数据失败: {str(e)}", source="celery_audio_upload")
        
        update_progress(progress=90, log_message="正在保存到数据库...")
        
        # 记录 ASR 处理耗时（上传+转写），用于 Basic Information 展示
        elapsed_seconds = round(time.time() - task_start_time, 2)
        
        # 6. 保存到数据库
        db_conn_params = get_database_config()
        from backend.database.db_manager import DatabaseManager
        db_manager = DatabaseManager(db_conn_params)
        conn = db_manager.get_connection()
        
        if not conn:
            raise ValueError("无法连接到数据库")
        
        cursor = conn.cursor()
        
        # 创建 processing_stats 记录（单文件时 total_time = total_file_processing_time）
        cursor.execute("""
        INSERT INTO processing_stats 
        (timestamp, files_processed, files_failed, total_time, 
         total_file_processing_time, total_ai_time, task_name, 
         "user", total_tokens, total_price, upload_params, 
         final_report, rc_agent_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """, (
            datetime.now(),
            1,
            0,
            elapsed_seconds,
            elapsed_seconds,
            0,
            'ASR Transcription',
            username,
            0,
            0,
            json.dumps({
                "audio_url": audio_url,
                "model": model,
                "language_hints": language_hints,
                "diarization_enabled": diarization_enabled,
                "object_name": object_name
            }),
            '',
            'completed'
        ))
        
        processing_stats_id = cursor.fetchone()[0]
        
        # 创建 file_processing_details 记录
        # original_file_path 必须为 OSS object key（供 /api/files/serve 拉取），不可存签名 URL；
        # file_storage_type / oss_bucket 必须与文档上传流程一致，否则会被当作本地路径导致 404。
        oss_bucket_name = upload_result.get("bucket")
        cursor.execute("""
        INSERT INTO file_processing_details 
        (processing_stats_id, filename, file_type, file_size_mb, 
         processing_time, ai_time, content_length, status, 
         total_tokens, cost, content, created_at, task_id, original_file_path, asr_with_diarization,
         file_storage_type, oss_bucket)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """, (
            processing_stats_id,
            filename,
            'audio',
            file_size_mb,
            elapsed_seconds,
            0,
            len(final_report_text),
            'completed',
            0,
            0,
            final_report_text,
            datetime.now(),
            task_id,
            object_name,
            json.dumps(asr_structured_data) if asr_structured_data else None,
            'oss',
            oss_bucket_name,
        ))
        
        file_detail_id = cursor.fetchone()[0]
        
        conn.commit()
        conn.close()
        
        # 7. 清理临时文件（可选）
        try:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                info(f"已删除临时文件: {temp_file_path}", source="celery_audio_upload")
        except Exception as e:
            warning(f"删除临时文件失败: {str(e)}", source="celery_audio_upload")
        
        update_progress(status="completed", progress=100, log_message="音频上传和转写完成！")
        
        final_result = {
            'status': 'completed',
            'filename': filename,
            'task_id': task_id,
            'processing_stats_id': processing_stats_id,
            'file_detail_id': file_detail_id,
            'audio_url': audio_url,
            'text': plain_text,
            'text_length': len(plain_text),
            'logs': getattr(update_progress, 'logs', [])
        }
        
        info(f"音频上传和转写任务 {self.request.id} 完成: {filename}", source="celery_audio_upload")
        
        # 🏷️ 触发自动打标签任务
        try:
            trigger_auto_tagging(file_detail_id, username, delay=int(os.getenv('AUTO_TAG_DELAY', '2')))
        except Exception as e:
            error(f"触发自动打标签失败: {str(e)}", source="celery_audio_upload")
        
        return final_result
        
    except Exception as e:
        error_msg = f"音频上传和转写任务失败: {str(e)}"
        error(f"AudioUpload {self.request.id}: {error_msg}", source="celery_audio_upload")
        
        # 清理临时文件
        try:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
        except Exception:
            pass
        
        current_task.update_state(
            state='FAILURE',
            meta={
                'error': str(error_msg),
                'status': 'failed',
                'filename': filename
            }
        )
        raise Exception(error_msg)


@app.task(bind=True, name='upload_and_process_document_async', queue='file_processing')
def upload_and_process_document_task(self, temp_file_path, filename, username, 
                                      task_info=None, custom_inputs=None, params=None):
    """
    异步上传文档到 OSS 并处理（LlamaParse + AI 分析）
    
    避免阻塞主 API 线程，适合 OSS 服务器较远（如香港）的场景
    
    Args:
        temp_file_path: 本地临时文件路径
        filename: 原始文件名
        username: 用户名
        task_info: 任务信息（analysis_mode, task_type, template_id等）
        custom_inputs: 自定义输入参数
        params: 处理参数（sector, industry, ticker等）
    """
    _last_progress = [0.0]
    _last_status = ['processing']
    
    task_info = task_info or {}
    custom_inputs = custom_inputs or {}
    params = params or {}
    
    def update_progress(status=None, progress=None, log_message=None, **kwargs):
        try:
            if progress is not None:
                _last_progress[0] = progress
            if status is not None:
                _last_status[0] = status
            if not hasattr(update_progress, 'logs'):
                update_progress.logs = []
            if log_message:
                update_progress.logs.append({
                    'timestamp': datetime.now().isoformat(),
                    'level': 'info',
                    'message': log_message,
                    'source': 'celery_doc_upload'
                })
                if len(update_progress.logs) > 50:
                    update_progress.logs = update_progress.logs[-50:]
            current_task.update_state(
                state='PROGRESS',
                meta={
                    'status': _last_status[0],
                    'progress': _last_progress[0],
                    'message': log_message or '',
                    'filename': filename,
                    'username': username,
                    'logs': update_progress.logs
                }
            )
            if log_message:
                info(f"DocUpload {self.request.id}: {log_message}", source="celery_doc_upload")
        except Exception as e:
            error(f"更新文档上传任务状态失败: {e}", source="celery_doc_upload")
    
    try:
        info(f"开始文档上传和处理任务 {self.request.id}, file={filename}, user={username}", 
             source="celery_doc_upload")
        
        update_progress(status="uploading", progress=5, log_message=f"开始上传文档: {filename}")
        
        # 1. 上传到 OSS
        from backend.services.oss_service import OSSService
        oss_service = OSSService()
        
        if not oss_service.is_configured():
            raise ValueError("OSS 服务未配置")
        
        if not os.path.exists(temp_file_path):
            raise ValueError(f"临时文件不存在: {temp_file_path}")
        
        file_size = os.path.getsize(temp_file_path)
        file_size_mb = file_size / (1024 * 1024)
        update_progress(progress=10, log_message=f"文件大小: {file_size_mb:.2f} MB")
        
        # 上传到 OSS
        file_category = oss_service.get_file_category(filename)
        upload_result = oss_service.upload_file(
            file_path=temp_file_path,
            filename=filename,
            username=username,
            file_category=file_category
        )
        
        if not upload_result["success"]:
            err = upload_result.get("error") or "未知错误"
            raise Exception(f"上传到 OSS 失败: {str(err)}")
        
        oss_object_name = upload_result["object_name"]
        oss_bucket = upload_result["bucket"]
        
        update_progress(status="uploaded", progress=30, 
                       log_message=f"文件已上传到 OSS，正在创建数据库记录...")
        
        # 🔥 立即创建数据库记录，状态为 'uploaded'，这样前端可以立即看到新文件
        from backend.database.db_manager import DatabaseManager
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        conn = db_manager.get_connection()
        
        if not conn:
            raise ValueError("无法连接到数据库")
        
        cursor = conn.cursor()
        
        # 推断文件类型
        from backend.processors.file_processor import FileProcessor
        file_processor = FileProcessor()
        file_ext = os.path.splitext(filename)[1].lower()
        
        ext_to_type = {
            '.pdf': 'pdf',
            '.docx': 'word', '.doc': 'word',
            '.xlsx': 'excel', '.xls': 'excel',
            '.csv': 'csv',
            '.jpg': 'image', '.jpeg': 'image', '.png': 'image', '.gif': 'image', '.bmp': 'image'
        }
        file_type = ext_to_type.get(file_ext, 'other')
        
        # 创建 processing_stats 记录（状态为 processing）
        cursor.execute("""
        INSERT INTO processing_stats 
        (timestamp, files_processed, files_failed, total_time, 
         total_file_processing_time, total_ai_time, task_name, 
         "user", total_tokens, total_price, upload_params, 
         final_report, rc_agent_status)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """, (
            datetime.now(),
            0,  # files_processed
            0,  # files_failed
            0,  # total_time
            0,  # total_file_processing_time
            0,  # total_ai_time
            'Document Upload & Processing',  # task_name
            username,
            0,  # total_tokens
            0,  # total_price
            json.dumps(params),  # upload_params
            '',  # final_report
            'processing'  # rc_agent_status = 'processing'
        ))
        
        processing_stats_id = cursor.fetchone()[0]
        
        # 创建 file_processing_details 记录（状态为 processing）
        cursor.execute("""
        INSERT INTO file_processing_details 
        (processing_stats_id, filename, file_type, file_size_mb, 
         processing_time, ai_time, content_length, status, 
         total_tokens, cost, content, created_at, task_id, original_file_path,
         file_storage_type, oss_bucket)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        """, (
            processing_stats_id,
            filename,  # 原始文件名
            file_type,
            file_size_mb,
            0,  # processing_time
            0,  # ai_time
            0,  # content_length
            'processing',  # status = 'processing'
            0,  # total_tokens
            0,  # cost
            '',  # content（尚未解析）
            datetime.now(),
            self.request.id,  # task_id
            oss_object_name,  # original_file_path（OSS object_name）
            'oss',  # file_storage_type
            oss_bucket  # oss_bucket
        ))
        
        file_detail_id = cursor.fetchone()[0]
        
        conn.commit()
        conn.close()
        
        update_progress(progress=35, 
                       log_message=f"数据库记录已创建 (ID: {file_detail_id})，开始解析文档...")
        
        # 2. 创建 Worker 并传递已有的 processing_stats_id
        worker = FileProcessingWorker(self.request.id, username, db_conn_params)
        worker.set_status_callback(update_progress)
        worker.existing_processing_stats_id = processing_stats_id  # 传递已创建的记录ID
        worker.existing_file_detail_id = file_detail_id  # 传递已创建的文件记录ID
        
        # 组装 file_paths（使用 OSS object_name）
        file_paths = [oss_object_name]
        
        # 获取 API 密钥（Quick Process 模式不需要 LlamaIndex API 密钥）
        processing_mode = params.get('processing_mode', 'llamaparse')
        if processing_mode == 'quick':
            api_key = None
        else:
            api_key = os.getenv('LLAMA_CLOUD_API_KEY')
            if not api_key:
                raise ValueError("LlamaIndex API 密钥未配置")
        
        # 3. 异步执行文件处理
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
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
        
        update_progress(status="completed", progress=100, 
                       log_message="处理完成")
        
        # 4. 清理临时文件
        try:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                info(f"已清理临时文件: {temp_file_path}", source="celery_doc_upload")
        except Exception as del_err:
            warning(f"清理临时文件失败: {del_err}", source="celery_doc_upload")
        
        # 5. 触发自动打标签
        try:
            # 查询该任务创建的所有 file_processing_details ID
            from backend.database.db_manager import DatabaseManager
            db_manager = DatabaseManager(db_conn_params)
            conn = db_manager.get_connection()
            
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT id FROM file_processing_details
                    WHERE task_id = %s AND status = 'completed' AND content IS NOT NULL AND content != ''
                """, (self.request.id,))
                
                file_ids = [row[0] for row in cursor.fetchall()]
                conn.close()
                
                if file_ids:
                    info(f"触发自动打标签: {len(file_ids)} 个文件", source="celery_doc_upload")
                    trigger_auto_tagging(file_ids, username=username, delay=2)
        except Exception as e:
            error(f"触发自动打标签失败: {str(e)}", source="celery_doc_upload")
        
        info(f"文档上传和处理任务完成 {self.request.id}, file={filename}", 
             source="celery_doc_upload")
        
        return {
            'task_id': self.request.id,
            'status': 'completed',
            'filename': filename,
            'oss_object_name': oss_object_name,
            'result': result
        }
        
    except Exception as e:
        error_msg = f"文档上传或处理失败: {str(e)}"
        error(f"{error_msg}, task_id={self.request.id}", source="celery_doc_upload")
        
        # 清理临时文件
        try:
            if temp_file_path and os.path.exists(temp_file_path):
                os.remove(temp_file_path)
        except Exception:
            pass
        
        # 更新失败状态（确保所有值都可以 JSON 序列化）
        failure_meta = {
            'error': str(error_msg),
            'status': 'failed',
            'filename': filename
        }
        
        current_task.update_state(
            state='FAILURE',
            meta=failure_meta
        )
        
        # 返回失败结果而不是抛出异常（避免Celery序列化错误）
        return {
            'task_id': self.request.id,
            'status': 'failed',
            'error': error_msg,
            'files_processed': 0,
            'files_failed': len(file_paths) if 'file_paths' in locals() else 0
        }


@app.task(bind=True, name='process_web_clip_async', queue='file_processing')
def process_web_clip_task(self, file_detail_id, processing_stats_id, html_content, username):
    """
    异步处理 Web Clip：从 HTML 中提取纯文本，写入 DB content 字段，
    以便后续 AI 分析和自动打标签。

    Args:
        file_detail_id: file_processing_details.id
        processing_stats_id: processing_stats.id
        html_content: 完整 HTML 文档内容
        username: 用户名
    """
    try:
        info(f"开始 Web Clip 文本提取任务 {self.request.id}, file_detail_id={file_detail_id}",
             source="celery_web_clip")

        current_task.update_state(
            state='PROGRESS',
            meta={'status': 'processing', 'progress': 30, 'message': 'Extracting text from HTML...'}
        )

        import re
        from html import unescape

        text = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</(?:p|div|h[1-6]|li|tr|blockquote|section|article)>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = unescape(text)
        lines = [line.strip() for line in text.splitlines()]
        plain_text = '\n'.join(line for line in lines if line)

        if len(plain_text) > 500000:
            plain_text = plain_text[:500000]

        current_task.update_state(
            state='PROGRESS',
            meta={'status': 'saving', 'progress': 80, 'message': 'Saving extracted text...'}
        )

        from backend.database.db_manager import DatabaseManager
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        conn = db_manager.get_connection()

        if not conn:
            raise ValueError("无法连接到数据库")

        cursor = conn.cursor()
        cursor.execute("""
            UPDATE file_processing_details
            SET content = %s,
                content_length = %s,
                status = 'completed'
            WHERE id = %s
        """, (plain_text, len(plain_text), file_detail_id))

        cursor.execute("""
            UPDATE processing_stats
            SET rc_agent_status = 'completed'
            WHERE id = %s
        """, (processing_stats_id,))

        conn.commit()
        conn.close()

        info(f"Web Clip 文本提取完成: file_detail_id={file_detail_id}, content_length={len(plain_text)}",
             source="celery_web_clip")

        trigger_auto_tagging(file_detail_id, username, delay=int(os.getenv('AUTO_TAG_DELAY', '2')))

        return {
            'status': 'completed',
            'file_detail_id': file_detail_id,
            'content_length': len(plain_text),
        }

    except Exception as e:
        error_msg = f"Web Clip 文本提取失败: {str(e)}"
        error(f"WebClip {self.request.id}: {error_msg}", source="celery_web_clip")

        try:
            db_conn_params = get_database_config()
            from backend.database.db_manager import DatabaseManager
            db_manager = DatabaseManager(db_conn_params)
            conn = db_manager.get_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE file_processing_details
                    SET status = 'failed', error_message = %s
                    WHERE id = %s
                """, (str(e), file_detail_id))
                cursor.execute("""
                    UPDATE processing_stats
                    SET rc_agent_status = 'failed'
                    WHERE id = %s
                """, (processing_stats_id,))
                conn.commit()
                conn.close()
        except Exception:
            pass

        current_task.update_state(
            state='FAILURE',
            meta={'error': str(error_msg), 'status': 'failed'}
        )
        raise Exception(error_msg)


@app.task(bind=True, name='scrape_and_process_web_link', queue='file_processing')
def scrape_and_process_web_link_task(self, url, username):
    """
    服务端网页抓取：抓取 URL → Readability 提取正文 → 图片 base64 内嵌 → 上传 OSS → 写入 DB。
    替代 Chrome Extension 在浏览器端完成的全部工作。
    """
    import re as _re
    from backend.services.web_scraper_service import WebScraperService
    from backend.services.oss_service import OSSService
    from backend.database.db_manager import DatabaseManager

    processing_stats_id = None
    file_detail_id = None

    try:
        # 防御性清理：若 Redis 中残留该 task_id 的损坏 FAILURE 状态（如上次崩溃后重新投递），
        # 先删除再写入，避免 update_state 内部读取旧状态时级联 ValueError。
        try:
            from backend.celery_app import app as _celery_app
            _celery_app.backend.delete(self.request.id)
        except Exception:
            pass

        # ── 10→40%: 抓取网页 + 提取正文（httpx 先试，内容不足自动 Playwright fallback）──
        current_task.update_state(
            state='PROGRESS',
            meta={'status': 'processing', 'progress': 10, 'message': 'Fetching web page...'}
        )
        info(f"Web Link task {self.request.id}: fetching {url}", source="celery_web_link")

        def _on_status(msg):
            current_task.update_state(
                state='PROGRESS',
                meta={'status': 'processing', 'progress': 25, 'message': msg}
            )

        title, article_html, used_playwright = WebScraperService.smart_fetch(url, on_status=_on_status)

        if used_playwright:
            info(f"Web Link task {self.request.id}: used Playwright for {url}", source="celery_web_link")

        current_task.update_state(
            state='PROGRESS',
            meta={'status': 'processing', 'progress': 40, 'message': 'Article content extracted.'}
        )

        if not article_html or len(article_html.strip()) < 20:
            raise ValueError("Failed to extract article content from the page. The page may require login or have anti-scraping protection.")

        # ── 50%: 下载图片并内嵌 ──
        def _img_progress(done, total):
            pct = 50 + int(20 * done / max(total, 1))
            current_task.update_state(
                state='PROGRESS',
                meta={'status': 'processing', 'progress': pct, 'message': f'Downloading images ({done}/{total})...'}
            )

        current_task.update_state(
            state='PROGRESS',
            meta={'status': 'processing', 'progress': 50, 'message': 'Downloading and embedding images...'}
        )
        article_html = WebScraperService.inline_images(article_html, url, on_progress=_img_progress)

        # ── 70%: 构建完整 HTML 并上传 OSS ──
        current_task.update_state(
            state='PROGRESS',
            meta={'status': 'processing', 'progress': 70, 'message': 'Uploading to storage...'}
        )
        full_html = WebScraperService.build_full_html(title, url, article_html)
        html_bytes = full_html.encode('utf-8')
        file_size_mb = len(html_bytes) / (1024 * 1024)

        oss_service = OSSService()
        if not oss_service.is_configured():
            raise ValueError("OSS service not configured")

        slug = _re.sub(r'[^\w\u4e00-\u9fff\-]', '', title.replace(' ', '-'))
        slug = slug[:80] or 'web-link'
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{slug}_{timestamp}.html"
        date_str = datetime.now().strftime("%Y-%m-%d")
        oss_object_name = f"{oss_service.web_clip_prefix}{username}/{date_str}/{filename}"

        upload_result = oss_service.upload_bytes(
            object_name=oss_object_name,
            data=html_bytes,
            content_type='text/html; charset=utf-8',
            content_disposition='inline'
        )
        if not upload_result["success"]:
            raise ValueError(f"OSS upload failed: {upload_result.get('error')}")

        # ── 80%: 写入 DB ──
        current_task.update_state(
            state='PROGRESS',
            meta={'status': 'processing', 'progress': 80, 'message': 'Saving to database...'}
        )
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        conn = db_manager.get_connection()
        if not conn:
            raise ValueError("Database connection failed")

        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO processing_stats
            (timestamp, files_processed, files_failed, total_time,
             total_file_processing_time, total_ai_time, task_name,
             "user", total_tokens, total_price, upload_params,
             final_report, rc_agent_status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            datetime.now(), 1, 0, 0, 0, 0,
            'Web Link',
            username,
            0, 0,
            json.dumps({"source_url": url, "title": title}),
            '', 'processing'
        ))
        processing_stats_id = cursor.fetchone()[0]

        cursor.execute("""
            INSERT INTO file_processing_details
            (processing_stats_id, filename, file_type, file_size_mb,
             processing_time, ai_time, content_length, status,
             total_tokens, cost, content, created_at, task_id,
             original_file_path, file_storage_type, oss_bucket)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (
            processing_stats_id,
            filename,
            'web',
            file_size_mb,
            0, 0, 0,
            'processing',
            0, 0, '',
            datetime.now(),
            self.request.id,
            oss_object_name,
            'oss',
            upload_result.get("bucket")
        ))
        file_detail_id = cursor.fetchone()[0]
        conn.commit()
        conn.close()

        # ── 90%: 提取纯文本 ──
        current_task.update_state(
            state='PROGRESS',
            meta={'status': 'processing', 'progress': 90, 'message': 'Extracting text content...'}
        )
        plain_text = WebScraperService.html_to_plain_text(full_html)

        conn2 = db_manager.get_connection()
        if conn2:
            cursor2 = conn2.cursor()
            cursor2.execute("""
                UPDATE file_processing_details
                SET content = %s, content_length = %s, status = 'completed'
                WHERE id = %s
            """, (plain_text, len(plain_text), file_detail_id))
            cursor2.execute("""
                UPDATE processing_stats
                SET rc_agent_status = 'completed'
                WHERE id = %s
            """, (processing_stats_id,))
            conn2.commit()
            conn2.close()

        # ── 100%: 完成 + 自动打标签 ──
        info(f"Web Link task completed: url={url}, file_detail_id={file_detail_id}, "
             f"content_length={len(plain_text)}", source="celery_web_link")

        trigger_auto_tagging(file_detail_id, username, delay=int(os.getenv('AUTO_TAG_DELAY', '2')))

        return {
            'status': 'completed',
            'file_detail_id': file_detail_id,
            'content_length': len(plain_text),
            'title': title,
            'source_url': url,
        }

    except Exception as e:
        error_msg = f"Web Link scraping failed: {str(e)}"
        error(f"WebLink {self.request.id}: {error_msg}", source="celery_web_link")

        try:
            import re as _re_err
            db_conn_params = get_database_config()
            from backend.database.db_manager import DatabaseManager as _DM
            db_manager = _DM(db_conn_params)
            conn = db_manager.get_connection()
            if conn:
                cursor = conn.cursor()

                if file_detail_id:
                    cursor.execute("""
                        UPDATE file_processing_details
                        SET status = 'failed', error_message = %s
                        WHERE id = %s
                    """, (str(e), file_detail_id))
                if processing_stats_id:
                    cursor.execute("""
                        UPDATE processing_stats
                        SET rc_agent_status = 'failed'
                        WHERE id = %s
                    """, (processing_stats_id,))

                # 如果还没创建 DB 记录就失败了（如反爬/登录墙在 smart_fetch 中触发），
                # 仍创建记录并标记 failed，这样文件卡片能显示 'Failed' 状态
                if not processing_stats_id:
                    from urllib.parse import urlparse as _urlparse
                    domain = _urlparse(url).netloc or 'unknown'
                    cursor.execute("""
                        INSERT INTO processing_stats
                        (timestamp, files_processed, files_failed, total_time,
                         total_file_processing_time, total_ai_time, task_name,
                         "user", total_tokens, total_price, upload_params,
                         final_report, rc_agent_status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        datetime.now(), 0, 1, 0, 0, 0,
                        'Web Link',
                        username,
                        0, 0,
                        json.dumps({"source_url": url}),
                        '', 'failed'
                    ))
                    processing_stats_id = cursor.fetchone()[0]

                    slug = _re_err.sub(r'[^\w\u4e00-\u9fff\-]', '', domain.replace('.', '-'))
                    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
                    fail_filename = f"WebLink-{slug}_{timestamp_str}.html"

                    cursor.execute("""
                        INSERT INTO file_processing_details
                        (processing_stats_id, filename, file_type, file_size_mb,
                         processing_time, ai_time, content_length, status,
                         total_tokens, cost, content, created_at, task_id,
                         error_message)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        processing_stats_id,
                        fail_filename,
                        'web',
                        0,
                        0, 0, 0,
                        'failed',
                        0, 0, '',
                        datetime.now(),
                        self.request.id,
                        str(e),
                    ))
                    file_detail_id = cursor.fetchone()[0]

                conn.commit()
                conn.close()
        except Exception:
            pass

        current_task.update_state(
            state='PROGRESS',
            meta={'status': 'failed', 'progress': 0, 'message': error_msg}
        )
        return {'status': 'failed', 'error': error_msg}


def _find_node_binary() -> str:
    """查找系统中的 Node.js 可执行文件路径"""
    import shutil
    for candidate in [
        os.getenv('NODE_BINARY'),
        '/usr/local/bin/node',
        '/usr/bin/node',
    ]:
        if candidate and os.path.isfile(candidate):
            return candidate
    found = shutil.which('node')
    return found or 'node'


def _find_ffmpeg_binary() -> str | None:
    """查找系统中的 ffmpeg 可执行文件路径"""
    import shutil
    conda_prefix = os.getenv('CONDA_PREFIX')
    for candidate in [
        os.getenv('FFMPEG_BINARY'),
        os.path.join(conda_prefix, 'bin', 'ffmpeg') if conda_prefix else None,
        '/root/miniconda3/envs/guanhe/bin/ffmpeg',
        '/usr/local/bin/ffmpeg',
        '/usr/bin/ffmpeg',
    ]:
        if candidate and os.path.isfile(candidate):
            return candidate
    return shutil.which('ffmpeg')


def _build_ydl_opts(out_template: str) -> dict:
    """
    构建 yt-dlp 配置字典，统一处理代理、Cookie 认证等选项。

    支持以下环境变量：
      YOUTUBE_COOKIES_FILE  Netscape 格式 cookies.txt 的绝对路径（首选认证方式，
                            可绕过 YouTube 反机器人检测）
      YOUTUBE_PO_TOKEN      YouTube PO Token（yt-dlp 2024.11+ 支持，
                            配合 YOUTUBE_VISITOR_DATA 使用）
      YOUTUBE_VISITOR_DATA  与 YOUTUBE_PO_TOKEN 配套的 Visitor Data 字符串
      YOUTUBE_PROXY / HTTPS_PROXY / HTTP_PROXY  代理地址
    """
    opts = {
        'format': 'bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best',
        'postprocessors': [{
            'key': 'FFmpegExtractAudio',
            'preferredcodec': 'mp3',
            'preferredquality': '128',
        }],
        'outtmpl': out_template,
        'quiet': True,
        'no_warnings': True,
        'socket_timeout': 30,
        'retries': 5,
        'fragment_retries': 5,
        'retry_sleep_functions': {'http': lambda n: 2 ** n},
        # 启用 Node.js 作为 EJS（外部 JS 挑战求解器）运行时，
        # 解决 YouTube n-challenge / signature solving 失败问题
        'js_runtimes': {'node': {'path': _find_node_binary()}},
    }

    ffmpeg_path = _find_ffmpeg_binary()
    if ffmpeg_path:
        opts['ffmpeg_location'] = os.path.dirname(ffmpeg_path)

    proxy = (os.getenv('YOUTUBE_PROXY') or
             os.getenv('HTTPS_PROXY') or
             os.getenv('HTTP_PROXY'))
    if proxy:
        opts['proxy'] = proxy

    cookies_file = os.getenv('YOUTUBE_COOKIES_FILE')
    if cookies_file and os.path.isfile(cookies_file):
        opts['cookiefile'] = cookies_file

    po_token = os.getenv('YOUTUBE_PO_TOKEN')
    visitor_data = os.getenv('YOUTUBE_VISITOR_DATA')
    if po_token and visitor_data:
        opts['extractor_args'] = {
            'youtube': {
                'po_token': [f'web+{po_token}'],
                'visitor_data': [visitor_data],
            }
        }

    return opts


@app.task(bind=True, name='upload_and_process_youtube_task', queue='file_processing')
def upload_and_process_youtube_task(self, youtube_url: str, username: str = 'guest'):
    """
    异步处理 YouTube 链接，获取转录并保存到数据库
    
    Args:
        youtube_url: YouTube 视频链接
        username: 用户名
        
    Returns:
        处理结果字典
    """
    from backend.services.youtube_service import YouTubeService
    from backend.database.db_manager import DatabaseManager
    
    task_id = self.request.id
    
    # 更新任务状态的辅助函数（kwargs 会并入 Celery meta，供前端弹窗等）
    def update_progress(status=None, progress=None, log_message=None, **meta_extra):
        try:
            state_data = {
                'status': status or 'processing',
                'progress': progress if progress is not None else 0,
                'current_file': youtube_url,
                'files_processed': 0,
                'files_failed': 1 if (status == 'failed') else 0,
                'total_files': 1,
                'logs': []
            }
            if status == 'failed' and log_message:
                state_data['message'] = log_message
            if log_message:
                state_data['logs'].append({
                    'timestamp': datetime.now().isoformat(),
                    'level': 'error' if status == 'failed' else 'info',
                    'message': log_message
                })
            for k, v in meta_extra.items():
                if v is not None:
                    state_data[k] = v
            current_task.update_state(
                state='PROGRESS',
                meta=state_data
            )
        except Exception as e:
            error(f"更新进度失败: {str(e)}", source="youtube_task")
    
    stats_id = None
    file_id = None
    try:
        task_start_time = time.time()
        info(f"开始处理 YouTube 链接: url={youtube_url}, task_id={task_id}", 
             source="youtube_task")
        
        # 更新进度：开始处理
        update_progress(status='processing', progress=10, 
                       log_message='Validating YouTube URL...')
        
        # 验证 URL 并提取 video_id
        video_id = YouTubeService.extract_video_id(youtube_url)
        if not video_id:
            error_msg = "Invalid YouTube URL"
            error(error_msg, source="youtube_task")
            update_progress(status='failed', progress=0, 
                           log_message=f'Error: {error_msg}')
            return {
                'task_id': task_id,
                'status': 'failed',
                'error': error_msg
            }
        
        info(f"提取 video_id: {video_id}", source="youtube_task")
        update_progress(progress=20, log_message=f'Video ID: {video_id}')
        
        # ── Step 1: 尝试通过 YouTube API 获取字幕 ─────────────────────────
        update_progress(progress=30, log_message='Fetching YouTube transcript...')

        transcript_result = None
        use_asr = False
        try:
            transcript_result = YouTubeService.fetch_transcript(youtube_url)
        except ImportError as e:
            # youtube-transcript-api 未安装 → 直接失败
            error_msg = str(e)
            error(error_msg, source="youtube_task")
            update_progress(status='failed', progress=0, log_message=f'Error: {error_msg}')
            return {'task_id': task_id, 'status': 'failed', 'error': error_msg}
        except Exception as e:
            # 无字幕（NoTranscriptFound / TranscriptsDisabled 等） → 切换到 Fun-ASR
            warning(f"No transcript via YouTube API ({e}), switching to Fun-ASR", source="youtube_task")
            use_asr = True

        if transcript_result:
            info(f"转录获取成功: {len(transcript_result['transcript'])} segments, language={transcript_result['language']}",
                 source="youtube_task")
            update_progress(progress=60,
                           log_message=f"Transcript fetched: {len(transcript_result['transcript'])} segments "
                                       f"in {transcript_result['language']}")
        else:
            update_progress(
                progress=35,
                log_message='No transcript available — switching to Fun-ASR transcription...',
                asr_fallback_notice=True,
            )

        # ── Step 2: 保存基础记录到数据库 ──────────────────────────────────
        update_progress(progress=70 if transcript_result else 37,
                       log_message='Saving to database...')

        # 有字幕时记录当前耗时；走 Fun-ASR 时在 Step 3 完成后再更新
        elapsed_seconds = round(time.time() - task_start_time, 2) if transcript_result else 0

        db_manager = DatabaseManager(get_database_config())
        filename = f"YouTube_{video_id}"

        try:
            with db_manager.get_connection_context() as conn:
                cursor = conn.cursor()

                full_text = YouTubeService.get_full_text(transcript_result) if transcript_result else ''
                asr_json = json.dumps(transcript_result) if transcript_result else None
                db_status = 'completed' if transcript_result else 'processing'
                # 仅在有官方字幕且已写入正文时记 completed；走 Fun-ASR 时在 ASR 成功后再记 completed
                rc_agent_status_init = 'completed' if transcript_result else 'processing'

                cursor.execute("""
                    INSERT INTO processing_stats (
                        timestamp, files_processed, files_failed,
                        "user", task_name, total_time,
                        total_file_processing_time, total_ai_time,
                        total_tokens, total_price, upload_params,
                        final_report, rc_agent_status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (
                    datetime.now().isoformat(), 1, 0, username,
                    'YouTube Processing', elapsed_seconds, elapsed_seconds, 0, 0, 0,
                    json.dumps({
                        "youtube_url": youtube_url,
                        "video_id": video_id,
                        "language": transcript_result.get('language') if transcript_result else None,
                        "is_generated": transcript_result.get('is_generated', False) if transcript_result else False,
                        "asr_used": use_asr,
                    }),
                    '', rc_agent_status_init
                ))
                stats_id = cursor.fetchone()[0]

                cursor.execute("""
                    INSERT INTO file_processing_details (
                        processing_stats_id, filename, file_type,
                        file_size_mb, status, content, asr_with_diarization,
                        created_at, task_id, original_file_path,
                        processing_time, content_length
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    stats_id, filename, 'youtube', 0, db_status,
                    full_text, asr_json,
                    datetime.now(), task_id, youtube_url,
                    elapsed_seconds, len(full_text)
                ))
                file_id = cursor.fetchone()[0]
                info(f"数据保存成功: file_id={file_id}, filename={filename}", source="youtube_task")

        except Exception as e:
            error_msg = f"数据库保存失败: {str(e)}"
            error(error_msg, source="youtube_task")
            update_progress(status='failed', progress=0, log_message=f'Error: {error_msg}')
            return {'task_id': task_id, 'status': 'failed', 'error': error_msg}

        # ── Step 3: Fun-ASR 流程（仅当无字幕时） ──────────────────────────
        if use_asr:
            import shutil, tempfile
            temp_dir = None
            try:
                try:
                    import yt_dlp
                except ImportError:
                    raise ImportError("yt-dlp 未安装，请执行: pip install yt-dlp")

                # 下载音频
                update_progress(progress=42, log_message='Downloading YouTube audio via yt-dlp...')
                temp_dir = tempfile.mkdtemp(prefix='yt_upload_asr_')
                out_template = os.path.join(temp_dir, f'youtube_{video_id}.%(ext)s')
                ydl_opts = _build_ydl_opts(out_template)
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([youtube_url])

                downloaded = [os.path.join(temp_dir, f)
                              for f in os.listdir(temp_dir) if f.startswith(f'youtube_{video_id}')]
                if not downloaded:
                    raise FileNotFoundError(f"yt-dlp failed to produce audio for {video_id}")
                temp_audio_path = downloaded[0]
                size_mb = os.path.getsize(temp_audio_path) / 1024 / 1024
                update_progress(progress=55, log_message=f'Audio downloaded: {size_mb:.1f} MB')

                # 上传 OSS
                from backend.services.oss_service import OSSService
                oss_service = OSSService()
                if not oss_service.is_configured():
                    raise ValueError("OSS 服务未配置")
                update_progress(progress=60, log_message='Uploading audio to OSS...')
                upload_res = oss_service.upload_audio_file(
                    file_path=temp_audio_path,
                    filename=os.path.basename(temp_audio_path),
                    username='system'
                )
                if not upload_res['success']:
                    raise Exception(f"OSS upload failed: {upload_res.get('error')}")
                audio_url = upload_res.get('signed_url') or upload_res['public_url']
                update_progress(progress=65, log_message='Uploaded to OSS, submitting to Fun-ASR...')

                # 提交 Fun-ASR
                from backend.services.asr_service import ASRService
                asr_service = ASRService()
                if not asr_service.is_configured():
                    raise ValueError("ASR 服务未配置（缺少 DASHSCOPE_API_KEY）")
                submit_res = asr_service.submit_transcription_task(
                    audio_url=audio_url, language_hints=['zh', 'en'], diarization_enabled=True
                )
                if not submit_res['success']:
                    raise ValueError(f"Fun-ASR submit failed: {submit_res['error']}")
                asr_task_id = submit_res['task_id']
                update_progress(progress=68, log_message=f'Fun-ASR task submitted: {asr_task_id}')

                # 等待结果
                update_progress(progress=70, log_message='Waiting for Fun-ASR results...')
                transcription_result = asr_service.get_transcription_results(asr_task_id)
                if not transcription_result['success']:
                    raise ValueError(f"Fun-ASR failed: {transcription_result['error']}")
                update_progress(progress=83, log_message='ASR complete, processing results...')

                # 格式化
                from backend.api.files import _build_asr_structured_data
                text_parts = []
                for trans_data in transcription_result['transcriptions']:
                    fmt = asr_service.format_transcription_result(
                        trans_data, include_timestamps=True, include_speaker=True, timestamped_only=True
                    )
                    if fmt:
                        text_parts.append(fmt)
                full_text = '\n'.join(text_parts)
                asr_structured = _build_asr_structured_data(transcription_result['transcriptions'])

                # 更新数据库记录（含处理耗时，用于 Basic Information 展示）
                update_progress(progress=87, log_message='Saving ASR transcript to database...')
                asr_elapsed_seconds = round(time.time() - task_start_time, 2)
                with db_manager.get_connection_context() as conn:
                    cursor = conn.cursor()
                    cursor.execute("""
                        UPDATE file_processing_details
                        SET content = %s, asr_with_diarization = %s,
                            content_length = %s, status = 'completed',
                            processing_time = %s
                        WHERE id = %s
                    """, (full_text,
                          json.dumps(asr_structured) if asr_structured else None,
                          len(full_text), asr_elapsed_seconds, file_id))
                    cursor.execute("""
                        UPDATE processing_stats
                        SET total_time = %s, total_file_processing_time = %s,
                            rc_agent_status = 'completed'
                        WHERE id = %s
                    """, (asr_elapsed_seconds, asr_elapsed_seconds, stats_id))
                info(f"Fun-ASR 转写完成并保存: file_id={file_id}", source="youtube_task")

            except Exception as asr_err:
                error(f"Fun-ASR pipeline failed for file_id={file_id}: {asr_err}", source="youtube_task")
                # ASR 失败时标记文件与 execution 为 failed（避免 rc_agent 仍为 completed/processing）
                try:
                    with db_manager.get_connection_context() as conn:
                        cur = conn.cursor()
                        cur.execute(
                            "UPDATE file_processing_details SET status='failed' WHERE id=%s", (file_id,)
                        )
                        if stats_id is not None:
                            cur.execute(
                                "UPDATE processing_stats SET rc_agent_status='failed' WHERE id=%s",
                                (stats_id,),
                            )
                except Exception:
                    pass
                update_progress(status='failed', progress=0,
                               log_message=f'Fun-ASR error: {asr_err}')
                return {'task_id': task_id, 'status': 'failed', 'error': str(asr_err)}
            finally:
                if temp_dir and os.path.exists(temp_dir):
                    try:
                        shutil.rmtree(temp_dir, ignore_errors=True)
                    except Exception:
                        pass

        # ── Step 4: 触发自动打标签 ─────────────────────────────────────────
        update_progress(progress=92, log_message='Triggering auto-tagging...')
        try:
            trigger_auto_tagging([file_id], username, delay=2)
            info(f"已触发自动打标签: file_id={file_id}", source="youtube_task")
        except Exception as e:
            error(f"触发自动打标签失败: {str(e)}", source="youtube_task")

        update_progress(status='completed', progress=100,
                       log_message='YouTube processing completed!')
        info(f"YouTube 处理任务完成: task_id={task_id}, video_id={video_id}", source="youtube_task")

        return {
            'task_id': task_id,
            'status': 'completed',
            'video_id': video_id,
            'file_id': file_id,
            'filename': filename,
            'asr_used': use_asr,
            'segments_count': len(transcript_result['transcript']) if transcript_result else 0,
            'language': transcript_result['language'] if transcript_result else 'asr',
        }
        
    except Exception as e:
        error_msg = f"YouTube 处理失败: {str(e)}"
        error(f"{error_msg}, task_id={task_id}", source="youtube_task")
        # 若已创建 processing_stats 但未在子流程中标记失败，补记 failed
        if stats_id is not None:
            try:
                dbm = DatabaseManager(get_database_config())
                with dbm.get_connection_context() as conn:
                    conn.cursor().execute(
                        "UPDATE processing_stats SET rc_agent_status='failed' WHERE id=%s AND rc_agent_status <> 'failed'",
                        (stats_id,),
                    )
            except Exception:
                pass
        
        # 更新失败状态
        failure_meta = {
            'error': str(error_msg),
            'status': 'failed',
            'current_file': youtube_url
        }
        
        try:
            current_task.update_state(
                state='PROGRESS',
                meta={
                    'status': 'failed',
                    'progress': 0,
                    'current_file': youtube_url,
                    'files_processed': 0,
                    'files_failed': 1,
                    'total_files': 1,
                    'error': error_msg,
                    'logs': [{'timestamp': datetime.now().isoformat(),
                               'level': 'error', 'message': error_msg}]
                }
            )
        except Exception:
            pass

        return {
            'task_id': task_id,
            'status': 'failed',
            'error': error_msg
        }


@app.task(bind=True, name='transcribe_youtube_feed_task', queue='file_processing')
def transcribe_youtube_feed_task(self, content_db_id: int, video_url: str, video_id: str):
    """
    对 Feed 中没有字幕的 YouTube 视频，使用 yt-dlp 下载音频后经 Fun-ASR 生成 transcript。

    Args:
        content_db_id: subscription_content 的主键 id
        video_url:     YouTube 视频完整 URL
        video_id:      YouTube video ID（用于临时文件命名和日志）
    """
    import shutil
    import tempfile

    task_id = self.request.id
    temp_dir = None

    def update_progress(status='processing', progress=0, log_message=None):
        try:
            meta = {
                'status': status,
                'progress': progress,
                'current_file': video_url,
                'files_processed': 0,
                'files_failed': 0,
                'total_files': 1,
                'logs': []
            }
            if log_message:
                meta['logs'].append({
                    'timestamp': datetime.now().isoformat(),
                    'level': 'info',
                    'message': log_message
                })
                info(f"YouTubeFeedASR {task_id}: {log_message}", source="youtube_feed_asr")
            current_task.update_state(state='PROGRESS', meta=meta)
        except Exception:
            pass

    try:
        info(f"开始 YouTube Feed ASR 转写: content_db_id={content_db_id}, video_id={video_id}",
             source="youtube_feed_asr")

        # ── 1. 检查 yt-dlp ────────────────────────────────────────
        try:
            import yt_dlp
        except ImportError:
            raise ImportError(
                "yt-dlp 未安装，请在服务器上执行: pip install yt-dlp"
            )

        # ── 2. 下载音频到临时文件 ──────────────────────────────────
        update_progress(progress=10, log_message='Downloading YouTube audio via yt-dlp...')

        temp_dir = tempfile.mkdtemp(prefix='yt_feed_asr_')
        out_template = os.path.join(temp_dir, f'youtube_{video_id}.%(ext)s')

        ydl_opts = _build_ydl_opts(out_template)
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])

        # 找到下载好的文件
        downloaded_files = [
            os.path.join(temp_dir, f)
            for f in os.listdir(temp_dir)
            if f.startswith(f'youtube_{video_id}')
        ]
        if not downloaded_files:
            raise FileNotFoundError(f"yt-dlp 未生成音频文件，video_id={video_id}")

        temp_audio_path = downloaded_files[0]
        file_size_mb = os.path.getsize(temp_audio_path) / 1024 / 1024
        update_progress(progress=30,
                       log_message=f'Audio downloaded: {os.path.basename(temp_audio_path)} ({file_size_mb:.1f} MB)')

        # ── 3. 上传到 OSS ──────────────────────────────────────────
        update_progress(progress=40, log_message='Uploading audio to OSS...')
        from backend.services.oss_service import OSSService
        oss_service = OSSService()
        if not oss_service.is_configured():
            raise ValueError("OSS 服务未配置，无法上传音频")

        upload_result = oss_service.upload_audio_file(
            file_path=temp_audio_path,
            filename=os.path.basename(temp_audio_path),
            username='system'
        )
        if not upload_result['success']:
            raise Exception(f"OSS 上传失败: {upload_result.get('error')}")

        # Fun-ASR 需通过公网可下载的 URL 拉取；私有桶时签名 URL 可能不被支持导致 FILE_DOWNLOAD_FAILED，可设置 OSS_ASR_PUBLIC_READ=true 使上传对象临时公网可读并用 public_url
        use_asr_public = os.getenv('OSS_ASR_PUBLIC_READ', '').strip().lower() in ('1', 'true', 'yes')
        if use_asr_public and upload_result.get('public_url'):
            audio_url = upload_result['public_url']
        else:
            audio_url = upload_result.get('signed_url') or upload_result.get('public_url')
        if not audio_url:
            raise ValueError(
                "OSS 未返回可访问 URL。若桶为私有，请设置 OSS_ASR_PUBLIC_READ=true 以便 Fun-ASR 通过公网 URL 下载。"
            )
        update_progress(progress=50, log_message=f'Uploaded to OSS, submitting to Fun-ASR...')

        # ── 4. 提交 Fun-ASR ────────────────────────────────────────
        from backend.services.asr_service import ASRService
        asr_service = ASRService()
        if not asr_service.is_configured():
            raise ValueError("ASR 服务未配置（缺少 DASHSCOPE_API_KEY）")

        submit_result = asr_service.submit_transcription_task(
            audio_url=audio_url,
            language_hints=['zh', 'en'],
            diarization_enabled=True
        )
        if not submit_result['success']:
            raise ValueError(f"提交 Fun-ASR 任务失败: {submit_result['error']}")

        asr_task_id = submit_result['task_id']
        update_progress(progress=60, log_message=f'Fun-ASR task submitted: {asr_task_id}')

        # ── 5. 等待 ASR 完成 ───────────────────────────────────────
        update_progress(progress=65, log_message='Waiting for Fun-ASR result (may take several minutes)...')
        transcription_result = asr_service.get_transcription_results(asr_task_id)
        if not transcription_result['success']:
            raise ValueError(f"Fun-ASR 识别失败: {transcription_result['error']}")
        if not transcription_result.get('transcriptions'):
            raise ValueError(
                "Fun-ASR 未返回有效转写结果（可能因文件下载失败等）。请重试或检查 OSS/网络。"
            )

        update_progress(progress=80, log_message='ASR complete, processing results...')

        # ── 6. 格式化结果 ──────────────────────────────────────────
        from backend.api.files import _build_asr_structured_data

        # 纯文本（带说话人 + 时间戳）
        text_parts = []
        for trans_data in transcription_result['transcriptions']:
            formatted = asr_service.format_transcription_result(
                trans_data,
                include_timestamps=True,
                include_speaker=True,
                timestamped_only=True
            )
            if formatted:
                text_parts.append(formatted)
        full_text = '\n'.join(text_parts)

        # 结构化数据（含说话人分离）
        asr_structured = _build_asr_structured_data(transcription_result['transcriptions'])
        transcript_data = {
            'source': 'asr',
            'language': 'zh',
            'is_generated': True,
            'asr_data': asr_structured
        }

        # ── 7. 写回数据库 ──────────────────────────────────────────
        update_progress(progress=90, log_message='Saving transcript to database...')
        from backend.services.subscription_content_service import SubscriptionContentService
        content_service = SubscriptionContentService()
        content_service.update_transcript(content_db_id, full_text, transcript_data)

        info(f"YouTube Feed ASR 完成: content_db_id={content_db_id}, video_id={video_id}",
             source="youtube_feed_asr")
        update_progress(status='completed', progress=100, log_message='Transcription completed!')

        return {
            'task_id': task_id,
            'status': 'completed',
            'content_db_id': content_db_id,
            'video_id': video_id,
        }

    except Exception as e:
        error_msg = f"YouTube Feed ASR 失败: {str(e)}"
        error(f"{error_msg}, content_db_id={content_db_id}", source="youtube_feed_asr")
        # ⚠️  不能同时调用 update_state(FAILURE) + return dict：
        # FAILURE 状态要求 meta 中有 exc_type，否则 Celery 在合并状态时会抛
        # ValueError("Exception information must include the exception type")。
        # 统一用 PROGRESS 状态上报错误信息，再 return 结果字典即可被前端轮询识别。
        try:
            current_task.update_state(
                state='PROGRESS',
                meta={
                    'status': 'failed',
                    'progress': 0,
                    'current_file': video_url,
                    'files_processed': 0,
                    'files_failed': 1,
                    'total_files': 1,
                    'error': error_msg,
                    'logs': [{
                        'timestamp': datetime.now().isoformat(),
                        'level': 'error',
                        'message': error_msg
                    }]
                }
            )
        except Exception:
            pass
        return {'task_id': task_id, 'status': 'failed', 'error': error_msg}

    finally:
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass


@app.task(bind=True, name='transcribe_youtube_file_asr_task', queue='file_processing')
def transcribe_youtube_file_asr_task(self, file_id: int, youtube_url: str, video_id: str,
                                     max_duration_seconds: int = 0):
    """
    对 Upload YouTube Links 中已保存（无字幕）的视频，触发 Fun-ASR 转写。

    Args:
        file_id:              file_processing_details 表主键
        youtube_url:          YouTube 视频完整 URL
        video_id:             YouTube video ID
        max_duration_seconds: 只转写前 N 秒（0 = 完整视频，暂未使用）
    """
    import shutil, tempfile
    from backend.database.db_manager import DatabaseManager

    task_id = self.request.id
    temp_dir = None

    def update_progress(status='processing', progress=0, log_message=None):
        try:
            meta = {
                'status': status,
                'progress': progress,
                'current_file': youtube_url,
                'files_processed': 0,
                'files_failed': 0,
                'total_files': 1,
                'logs': [],
            }
            if log_message:
                meta['logs'].append({
                    'timestamp': datetime.now().isoformat(),
                    'level': 'info',
                    'message': log_message,
                })
            current_task.update_state(state='PROGRESS', meta=meta)
        except Exception:
            pass

    try:
        info(f"transcribe_youtube_file_asr_task 开始: file_id={file_id}, task_id={task_id}",
             source="youtube_file_asr")
        update_progress(progress=10, log_message=f'Starting Fun-ASR for file_id={file_id}...')

        db_manager = DatabaseManager(get_database_config())

        # 标记数据库记录为 processing
        try:
            with db_manager.get_connection_context() as conn:
                conn.cursor().execute(
                    "UPDATE file_processing_details SET status='processing' WHERE id=%s",
                    (file_id,)
                )
        except Exception as e:
            warning(f"DB status update failed: {e}", source="youtube_file_asr")

        # ── 下载音频 ─────────────────────────────────────────────────────
        try:
            import yt_dlp
        except ImportError:
            raise ImportError("yt-dlp 未安装，请执行: pip install yt-dlp")

        update_progress(progress=15, log_message='Downloading YouTube audio via yt-dlp...')
        temp_dir = tempfile.mkdtemp(prefix='yt_file_asr_')
        out_template = os.path.join(temp_dir, f'youtube_{video_id}.%(ext)s')
        ydl_opts = _build_ydl_opts(out_template)
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([youtube_url])

        downloaded = [os.path.join(temp_dir, f)
                      for f in os.listdir(temp_dir) if f.startswith(f'youtube_{video_id}')]
        if not downloaded:
            raise FileNotFoundError(f"yt-dlp failed to produce audio for {video_id}")
        temp_audio_path = downloaded[0]
        size_mb = os.path.getsize(temp_audio_path) / 1024 / 1024
        update_progress(progress=40, log_message=f'Audio downloaded: {size_mb:.1f} MB')

        # ── 上传 OSS ──────────────────────────────────────────────────────
        from backend.services.oss_service import OSSService
        oss_service = OSSService()
        if not oss_service.is_configured():
            raise ValueError("OSS 服务未配置")
        update_progress(progress=50, log_message='Uploading audio to OSS...')
        upload_res = oss_service.upload_audio_file(
            file_path=temp_audio_path,
            filename=os.path.basename(temp_audio_path),
            username='system'
        )
        if not upload_res['success']:
            raise Exception(f"OSS upload failed: {upload_res.get('error')}")
        use_asr_public = os.getenv('OSS_ASR_PUBLIC_READ', '').strip().lower() in ('1', 'true', 'yes')
        if use_asr_public and upload_res.get('public_url'):
            audio_url = upload_res['public_url']
        else:
            audio_url = upload_res.get('signed_url') or upload_res.get('public_url')
        if not audio_url:
            raise ValueError(
                "OSS 未返回可访问 URL。若桶为私有，请设置 OSS_ASR_PUBLIC_READ=true 以便 Fun-ASR 通过公网 URL 下载。"
            )
        update_progress(progress=58, log_message='Uploaded to OSS, submitting to Fun-ASR...')

        # ── 提交 Fun-ASR ──────────────────────────────────────────────────
        from backend.services.asr_service import ASRService
        asr_service = ASRService()
        if not asr_service.is_configured():
            raise ValueError("ASR 服务未配置（缺少 DASHSCOPE_API_KEY）")
        submit_res = asr_service.submit_transcription_task(
            audio_url=audio_url, language_hints=['zh', 'en'], diarization_enabled=True
        )
        if not submit_res['success']:
            raise ValueError(f"Fun-ASR submit failed: {submit_res['error']}")
        asr_task_id = submit_res['task_id']
        update_progress(progress=63, log_message=f'Fun-ASR task submitted: {asr_task_id}')

        # ── 等待结果 ──────────────────────────────────────────────────────
        update_progress(progress=65, log_message='Waiting for Fun-ASR results...')
        transcription_result = asr_service.get_transcription_results(asr_task_id)
        if not transcription_result['success']:
            raise ValueError(f"Fun-ASR failed: {transcription_result['error']}")
        if not transcription_result.get('transcriptions'):
            raise ValueError(
                "Fun-ASR 未返回有效转写结果（可能因文件下载失败等）。请重试或检查 OSS/网络。"
            )
        update_progress(progress=83, log_message='ASR complete, processing results...')

        # ── 格式化 ────────────────────────────────────────────────────────
        from backend.api.files import _build_asr_structured_data
        text_parts = []
        for trans_data in transcription_result['transcriptions']:
            fmt = asr_service.format_transcription_result(
                trans_data, include_timestamps=True, include_speaker=True, timestamped_only=True
            )
            if fmt:
                text_parts.append(fmt)
        full_text = '\n'.join(text_parts)
        asr_structured = _build_asr_structured_data(transcription_result['transcriptions'])

        # ── 更新数据库 ────────────────────────────────────────────────────
        update_progress(progress=88, log_message='Saving ASR transcript to database...')
        with db_manager.get_connection_context() as conn:
            conn.cursor().execute("""
                UPDATE file_processing_details
                SET content = %s, asr_with_diarization = %s,
                    content_length = %s, status = 'completed'
                WHERE id = %s
            """, (full_text,
                  json.dumps(asr_structured) if asr_structured else None,
                  len(full_text), file_id))
        info(f"Fun-ASR 转写完成并保存: file_id={file_id}", source="youtube_file_asr")

        update_progress(status='completed', progress=100,
                       log_message='ASR transcription completed!')
        return {
            'task_id': task_id,
            'status': 'completed',
            'file_id': file_id,
            'video_id': video_id,
            'content_length': len(full_text),
        }

    except Exception as e:
        error_msg = f"YouTube File ASR 失败: {str(e)}"
        error(f"{error_msg}, file_id={file_id}", source="youtube_file_asr")
        try:
            with db_manager.get_connection_context() as conn:
                conn.cursor().execute(
                    "UPDATE file_processing_details SET status='failed' WHERE id=%s", (file_id,)
                )
        except Exception:
            pass
        try:
            current_task.update_state(
                state='PROGRESS',
                meta={
                    'status': 'failed', 'progress': 0,
                    'current_file': youtube_url,
                    'files_processed': 0, 'files_failed': 1, 'total_files': 1,
                    'error': error_msg,
                    'logs': [{'timestamp': datetime.now().isoformat(),
                               'level': 'error', 'message': error_msg}]
                }
            )
        except Exception:
            pass
        return {'task_id': task_id, 'status': 'failed', 'error': error_msg}

    finally:
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir, ignore_errors=True)
            except Exception:
                pass
