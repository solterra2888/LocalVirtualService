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
            SELECT filename, content, original_file_path, file_type
            FROM file_processing_details
            WHERE id = %s
        """, (file_detail_id,))
        row = cursor.fetchone()
        conn.close()

        if not row:
            raise ValueError(f"找不到文件记录: file_detail_id={file_detail_id}")

        filename, content_text, original_file_path, file_type = row[0], row[1], row[2], row[3]

        update_progress(progress=10, log_message=f"已读取文件数据: {filename}")

        # 2. 如果 content 为空，需要先用 LlamaParse 解析文档
        file_content_for_llm = content_text.strip() if content_text else ""
        doc_processing_time = 0

        if not file_content_for_llm:
            if not original_file_path or not os.path.exists(original_file_path):
                raise ValueError(f"文档文件不存在: {original_file_path}，无法进行解析")

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
                    llama_service.process_file(original_file_path, successful_results, failed_files, None)
                )
            finally:
                loop.close()

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
        return final_result

    except Exception as e:
        error_msg = f"文档分析任务失败: {str(e)}"
        error(f"DocAnalysis {self.request.id}: {error_msg}", source="celery_doc_analysis")

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
                    'source': 'celery_audio_upload'
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
                info(f"AudioUpload {self.request.id}: {log_message}", source="celery_audio_upload")
        except Exception as e:
            error(f"更新音频上传任务状态失败: {e}", source="celery_audio_upload")
    
    try:
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
            raise ValueError(f"上传到 OSS 失败: {upload_result['error']}")
        
        audio_url = upload_result["public_url"]
        object_name = upload_result["object_name"]
        
        update_progress(status="transcribing", progress=30, 
                       log_message=f"文件已上传到 OSS，开始语音识别...")
        
        # 2. 调用 ASR 服务
        from backend.services.asr_service import ASRService
        asr_service = ASRService()
        
        if not asr_service.is_configured():
            raise ValueError("ASR 服务未配置")
        
        # 提交 ASR 任务
        submit_result = asr_service.submit_transcription_task(
            audio_url=audio_url,
            model=model,
            language_hints=language_hints,
            diarization_enabled=diarization_enabled
        )
        
        if not submit_result["success"]:
            raise ValueError(f"提交 ASR 任务失败: {submit_result['error']}")
        
        task_id = submit_result["task_id"]
        update_progress(progress=40, log_message=f"ASR 任务已提交: {task_id}")
        
        # 3. 等待 ASR 完成
        transcription_result = asr_service.get_transcription_results(task_id)
        
        if not transcription_result["success"]:
            raise ValueError(f"ASR 识别失败: {transcription_result['error']}")
        
        update_progress(progress=80, log_message="语音识别完成，正在保存结果...")
        
        # 4. 格式化结果
        formatted_texts = []
        if transcription_result.get("transcriptions"):
            for trans_data in transcription_result["transcriptions"]:
                formatted = asr_service.format_transcription_result(
                    trans_data,
                    include_timestamps=True,
                    include_words=False,
                    include_speaker=diarization_enabled
                )
                formatted_texts.append(formatted)
        
        final_report_text = "\n\n".join(formatted_texts) if formatted_texts else ""
        
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
        
        # 6. 保存到数据库
        db_conn_params = get_database_config()
        from backend.database.db_manager import DatabaseManager
        db_manager = DatabaseManager(db_conn_params)
        conn = db_manager.get_connection()
        
        if not conn:
            raise ValueError("无法连接到数据库")
        
        cursor = conn.cursor()
        
        # 创建 processing_stats 记录
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
            0,
            0,
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
        cursor.execute("""
        INSERT INTO file_processing_details 
        (processing_stats_id, filename, file_type, file_size_mb, 
         processing_time, ai_time, content_length, status, 
         total_tokens, cost, content, created_at, task_id, original_file_path, asr_with_diarization)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            processing_stats_id,
            filename,
            'audio',
            file_size_mb,
            0,
            0,
            len(final_report_text),
            'completed',
            0,
            0,
            final_report_text,
            datetime.now(),
            task_id,
            audio_url,
            json.dumps(asr_structured_data) if asr_structured_data else None
        ))
        
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
            'audio_url': audio_url,
            'text': plain_text,
            'text_length': len(plain_text),
            'logs': getattr(update_progress, 'logs', [])
        }
        
        info(f"音频上传和转写任务 {self.request.id} 完成: {filename}", source="celery_audio_upload")
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
