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
        
        # 4. 调用 LlamaParse 解析文档
        update_progress(status="parsing", progress=35, log_message="正在使用 LlamaParse 解析文档...")
        
        api_key = params.get('llamaindex_api_key') or os.getenv('LLAMA_CLOUD_API_KEY')
        if not api_key:
            raise ValueError("LlamaIndex API 密钥未配置，无法解析文档")
        
        from backend.core.llama_service import LlamaService
        
        # 构建 LlamaParse 参数
        llama_params = {
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
        
        llama_service = LlamaService(
            api_key=api_key,
            polling_interval=llama_params['polling_interval']
        )
        
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
                    info(f"已清理临时文件: {temp_file_path}", source="celery_reparse")
                except Exception as e:
                    error(f"清理临时文件失败: {e}", source="celery_reparse")
        
        if not successful_results:
            raise ValueError(f"LlamaParse 解析文档失败: {filename}")
        
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
        
        current_task.update_state(
            state='FAILURE',
            meta={
                'error': str(error_msg),
                'status': 'failed',
                'file_id': file_id
            }
        )
        
        # 返回失败结果而不是抛出异常（避免Celery序列化错误）
        return {
            'task_id': self.request.id,
            'status': 'failed',
            'error': error_msg,
            'file_id': file_id
        }


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
            err = upload_result.get("error") or "未知错误"
            raise Exception(f"上传到 OSS 失败: {str(err)}")
        
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
        RETURNING id
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
        
        # 获取 API 密钥
        api_key = params.get('llamaindex_api_key') or os.getenv('LLAMA_CLOUD_API_KEY')
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
    
    # 更新任务状态的辅助函数
    def update_progress(status=None, progress=None, log_message=None):
        try:
            state_data = {
                'status': status or 'processing',
                'progress': progress if progress is not None else 0,
                'current_file': youtube_url,
                'files_processed': 0,
                'files_failed': 0,
                'total_files': 1,
                'logs': []
            }
            
            if log_message:
                state_data['logs'].append({
                    'timestamp': datetime.now().isoformat(),
                    'level': 'info',
                    'message': log_message
                })
            
            current_task.update_state(
                state='PROGRESS',
                meta=state_data
            )
        except Exception as e:
            error(f"更新进度失败: {str(e)}", source="youtube_task")
    
    try:
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
        
        # 获取转录
        update_progress(progress=30, log_message='Fetching YouTube transcript...')
        
        try:
            result = YouTubeService.fetch_transcript(youtube_url)
        except ImportError as e:
            error_msg = str(e)
            error(error_msg, source="youtube_task")
            update_progress(status='failed', progress=0, 
                           log_message=f'Error: {error_msg}')
            return {
                'task_id': task_id,
                'status': 'failed',
                'error': error_msg
            }
        except Exception as e:
            error_msg = f"Failed to fetch transcript: {str(e)}"
            error(error_msg, source="youtube_task")
            update_progress(status='failed', progress=0, 
                           log_message=f'Error: {error_msg}')
            return {
                'task_id': task_id,
                'status': 'failed',
                'error': error_msg
            }
        
        info(f"转录获取成功: {len(result['transcript'])} segments, language={result['language']}", 
             source="youtube_task")
        update_progress(progress=60, 
                       log_message=f"Transcript fetched: {len(result['transcript'])} segments in {result['language']}")
        
        # 保存到数据库
        update_progress(progress=70, log_message='Saving to database...')
        
        db_manager = DatabaseManager(get_database_config())
        
        try:
            with db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                
                # 插入 processing_stats
                cursor.execute("""
                    INSERT INTO processing_stats (
                        timestamp, files_processed, files_failed, 
                        "user", task_name, total_time,
                        total_file_processing_time, total_ai_time,
                        total_tokens, total_price, upload_params,
                        final_report, rc_agent_status
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, (
                    datetime.now().isoformat(),
                    1,  # files_processed
                    0,  # files_failed
                    username,
                    'YouTube Processing',
                    0,  # total_time (YouTube 处理很快)
                    0,  # total_file_processing_time
                    0,  # total_ai_time
                    0,  # total_tokens
                    0,  # total_price
                    json.dumps({
                        "youtube_url": youtube_url,
                        "video_id": video_id,
                        "language": result.get('language'),
                        "is_generated": result.get('is_generated', False)
                    }),  # upload_params
                    '',  # final_report
                    'completed'  # rc_agent_status
                ))
                stats_id = cursor.fetchone()[0]
                
                # 拼接完整文本
                full_text = YouTubeService.get_full_text(result)
                
                # 插入 file_processing_details
                filename = f"YouTube_{video_id}"
                cursor.execute("""
                    INSERT INTO file_processing_details (
                        processing_stats_id, filename, file_type, 
                        file_size_mb, status, content, asr_with_diarization,
                        created_at, task_id, original_file_path,
                        processing_time, content_length
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """, (
                    stats_id,
                    filename,
                    'youtube',  # file_type
                    0,  # file_size_mb (YouTube 没有文件大小)
                    'completed',
                    full_text,  # content: 完整文本
                    json.dumps(result),  # asr_with_diarization: 存储完整的 YouTube 数据
                    datetime.now(),
                    task_id,
                    youtube_url,  # original_file_path: 存储 YouTube URL
                    0,  # processing_time
                    len(full_text)  # content_length
                ))
                file_id = cursor.fetchone()[0]
                
                info(f"数据保存成功: file_id={file_id}, filename={filename}", 
                     source="youtube_task")
                
        except Exception as e:
            error_msg = f"数据库保存失败: {str(e)}"
            error(error_msg, source="youtube_task")
            update_progress(status='failed', progress=0, 
                           log_message=f'Error: {error_msg}')
            return {
                'task_id': task_id,
                'status': 'failed',
                'error': error_msg
            }
        
        # 触发自动打标签
        update_progress(progress=90, log_message='Triggering auto-tagging...')
        try:
            trigger_auto_tagging([file_id], username, delay=2)
            info(f"已触发自动打标签: file_id={file_id}", source="youtube_task")
        except Exception as e:
            error(f"触发自动打标签失败: {str(e)}", source="youtube_task")
        
        # 更新进度：完成
        update_progress(status='completed', progress=100,
                       log_message='YouTube processing completed!')
        
        info(f"YouTube 处理任务完成: task_id={task_id}, video_id={video_id}", 
             source="youtube_task")
        
        return {
            'task_id': task_id,
            'status': 'completed',
            'video_id': video_id,
            'file_id': file_id,
            'filename': filename,
            'segments_count': len(result['transcript']),
            'language': result['language']
        }
        
    except Exception as e:
        error_msg = f"YouTube 处理失败: {str(e)}"
        error(f"{error_msg}, task_id={task_id}", source="youtube_task")
        
        # 更新失败状态
        failure_meta = {
            'error': str(error_msg),
            'status': 'failed',
            'current_file': youtube_url
        }
        
        current_task.update_state(
            state='FAILURE',
            meta=failure_meta
        )
        
        return {
            'task_id': task_id,
            'status': 'failed',
            'error': error_msg
        }
