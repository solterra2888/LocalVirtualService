# -*- coding: utf-8 -*-
"""
文件处理工作者模块
"""

import asyncio
import time
import os
import json
from typing import Dict, List, Any, Callable, Optional
from datetime import datetime

# 统一的环境变量加载
from backend.utils.env_loader import load_project_env

from backend.core.llama_service import LlamaService
from backend.core.rc_agent_service import RCAgentService
from backend.processors.file_processor import FileProcessor
from backend.database.db_manager import DatabaseManager


class FileProcessingWorker:
    """文件处理工作者"""
    
    def __init__(self, task_id: str, username: str, db_conn_params: Dict[str, Any]):
        """
        初始化文件处理工作者
        
        Args:
            task_id: 任务ID
            username: 用户名
            db_conn_params: 数据库连接参数
        """
        self.task_id = task_id
        self.username = username
        self.db_conn_params = db_conn_params
        self.db_manager = DatabaseManager(db_conn_params)
        self.file_processor = FileProcessor()
        
        # 处理统计
        self.files_processed = 0
        self.files_failed = 0
        self.total_files = 0
        self.start_time = time.time()
        
        # 状态回调
        self.status_callback: Optional[Callable] = None
        
        # 是否停止标志
        self.should_stop = False
        
        # 已存在的数据库记录ID（用于上传完成后立即创建记录的场景）
        self.existing_processing_stats_id: Optional[int] = None
        self.existing_file_detail_id: Optional[int] = None

    async def _progress_ticker(self, progress_ref: List[float], max_pct: float, interval: float = 6, increment: float = 8):
        """在长时间等待（如文档解析）期间缓慢增加进度显示，避免长时间停在 10%。"""
        while not self.should_stop:
            await asyncio.sleep(interval)
            if self.should_stop:
                break
            progress_ref[0] = min(progress_ref[0] + increment, max_pct)
            self.update_status(progress=progress_ref[0])
    
    def set_status_callback(self, callback: Callable):
        """设置状态更新回调函数"""
        self.status_callback = callback
    
    def _is_oss_object_key(self, file_path: str) -> bool:
        """
        判断是否为 OSS 对象键（云端路径）
        
        Args:
            file_path: 文件路径
            
        Returns:
            bool: 是否为 OSS 对象键
        """
        if not file_path or os.path.isabs(file_path):
            return False
        # OSS 对象键格式：documents/...、audio/...、images/...（图片上传后也存 OSS，需先下载再解析）
        return (
            file_path.startswith("documents/")
            or file_path.startswith("audio/")
            or file_path.startswith("images/")
        )
    
    def stop(self):
        """停止处理"""
        self.should_stop = True
    
    def update_status(self, status: str = None, progress: float = None, 
                     current_file: str = None, log_message: str = None):
        """更新状态"""
        if self.status_callback:
            self.status_callback(
                status=status,
                progress=progress,
                current_file=current_file,
                log_message=log_message
            )
    
    async def process_files(
        self,
        file_paths: List[str],
        task_info: Dict[str, Any],
        custom_inputs: Dict[str, Any],
        params: Dict[str, Any],
        api_key: str
    ) -> Dict[str, Any]:
        """
        处理文件列表
        
        Args:
            file_paths: 文件路径列表
            task_info: 任务信息
            custom_inputs: 自定义输入参数
            params: 处理参数
            api_key: API密钥
            
        Returns:
            处理结果统计
        """
        self.total_files = len(file_paths)
        # 保存处理参数，用于后续标签设置
        self.params = params
        
        try:
            self.update_status("initializing", 0.0, log_message="正在初始化处理服务...")
            
            # 初始化服务
            llama_service = LlamaService(
                api_key=api_key,
                polling_interval=params.get('polling_interval', 10)
            )
            
            use_ai_agent = params.get('use_ai_agent', True)
            analysis_mode = params.get('analysis_mode', 'remote_agent')  # 'remote_agent' 或 'local_llm'
            
            rc_agent_service = None
            llm_analysis_service = None
            
            if use_ai_agent:
                if analysis_mode == 'local_llm':
                    # 使用本地LLM+Template方式
                    template_id = params.get('template_id')
                    template_code = params.get('template_code')
                    
                    if not template_id and not template_code:
                        self.update_status(log_message="⚠️ 警告: 未指定模板，将跳过AI分析")
                    else:
                        from backend.services.local_llm_analysis_service import LocalLLMAnalysisService
                        llm_analysis_service = LocalLLMAnalysisService(
                            template_id=template_id,
                            template_code=template_code,
                            log_callback=self.update_status
                        )
                        self.update_status(log_message=f"✅ 已初始化本地LLM分析服务 (模板ID: {template_id or template_code})")
                else:
                    # 使用远程Dify Agent方式（现有逻辑）
                    if task_info.get('token'):
                        base_url = params.get('ai_agent_server_url', os.getenv('RC_AGENT_BASE_URL', 'http://localhost:81/v1/chat-messages'))
                        rc_agent_service = RCAgentService(rc_agent_token=task_info.get('token'), base_url=base_url, log_callback=self.update_status)
            
            self.update_status("processing", 5.0, log_message="开始处理文件...")
            
            # 🔍 立即更新数据库中已存在的 uploaded 记录，标记为正在处理（写入 task_id）
            # 这样前端可以立即知道文件正在处理，防止重复提交
            try:
                conn = self.db_manager.get_connection()
                if conn and file_paths:
                    cursor = conn.cursor()
                    first_file_path = file_paths[0]
                    
                    # 查找对应的 uploaded 记录
                    cursor.execute("""
                    SELECT fpd.id, fpd.processing_stats_id
                    FROM file_processing_details fpd
                    JOIN processing_stats ps ON fpd.processing_stats_id = ps.id
                    WHERE fpd.original_file_path = %s 
                      AND ps.rc_agent_status = 'uploaded'
                      AND ps."user" = %s
                    ORDER BY fpd.id DESC
                    LIMIT 1
                    """, (first_file_path, self.username))
                    
                    result = cursor.fetchone()
                    if result:
                        file_detail_id = result[0]
                        # 更新 file_processing_details 的 task_id
                        cursor.execute("""
                        UPDATE file_processing_details
                        SET task_id = %s,
                            status = 'processing'
                        WHERE id = %s
                        """, (self.task_id, file_detail_id))
                        
                        # 同时更新 processing_stats 的状态
                        cursor.execute("""
                        UPDATE processing_stats
                        SET rc_agent_status = 'processing'
                        WHERE id = %s
                        """, (result[1],))
                        
                        conn.commit()
                        self.update_status(log_message=f"✅ 已标记文件为处理中状态 (task_id: {self.task_id})")
                    conn.close()
            except Exception as e:
                self.update_status(log_message=f"⚠️ 更新文件状态失败: {str(e)}")
            
            # 处理文件
            successful_results = []
            failed_files = []
            temp_files_to_cleanup = []  # 记录需要清理的临时文件
            temp_to_original_mapping = {}  # 映射临时文件路径到原始 OSS 路径
            
            for i, file_path in enumerate(file_paths):
                if self.should_stop:
                    self.update_status("stopped", log_message="用户请求停止处理")
                    break
                
                try:
                    file_name = os.path.basename(file_path)
                    extraction_max = 75.0  # 文档解析阶段进度上限，留 80+ 给 AI 阶段
                    progress_ref = [10.0 + (i / len(file_paths)) * (extraction_max - 10.0)]
                    self.update_status(
                        current_file=file_name,
                        progress=progress_ref[0],
                        log_message=f"正在处理文件: {file_name}"
                    )
                    
                    # 🔧 检查是否为 OSS 文件（object_name 格式：documents/admin/2026-02-12/xxx.docx）
                    local_file_path = file_path
                    temp_file_path = None
                    
                    if self._is_oss_object_key(file_path):
                        # 文件存储在 OSS，需要先下载到本地临时目录
                        self.update_status(log_message=f"检测到 OSS 文件，正在下载: {file_name}")
                        
                        from backend.services.oss_service import OSSService
                        oss_service = OSSService()
                        
                        if not oss_service.is_configured():
                            raise ValueError("OSS 服务未配置，但文件路径指向 OSS 对象")
                        
                        # 创建临时目录
                        temp_dir = os.path.join(os.getcwd(), 'uploads', 'temp_oss_processing')
                        os.makedirs(temp_dir, exist_ok=True)
                        
                        # 生成临时文件路径（保留原始文件扩展名）
                        import uuid
                        file_ext = os.path.splitext(file_name)[1]
                        temp_file_path = os.path.join(temp_dir, f"{uuid.uuid4().hex}{file_ext}")
                        
                        # 从 OSS 下载文件
                        try:
                            oss_service.download_to_file(file_path, temp_file_path)
                            local_file_path = temp_file_path
                            temp_files_to_cleanup.append(temp_file_path)
                            temp_to_original_mapping[temp_file_path] = file_path  # 记录映射关系
                            self.update_status(log_message=f"OSS 文件下载完成: {file_name}")
                        except Exception as e:
                            raise ValueError(f"从 OSS 下载文件失败: {str(e)}")
                    elif not os.path.exists(file_path):
                        # 如果不是 OSS 文件且本地文件不存在，报错
                        raise ValueError(f"文件不存在: {file_path}")
                    
                    # 文档解析可能很久，启动进度 ticker，避免长时间停在 10%
                    ticker = asyncio.create_task(
                        self._progress_ticker(progress_ref, max_pct=extraction_max, interval=6, increment=8)
                    )
                    try:
                        # 准备LlamaService参数
                        llama_params = {
                            "language": params.get('language', ['ch_sim', 'en']),
                            "fast_mode": params.get('fast_mode', False),
                            "disable_ocr": params.get('disable_ocr', False),
                            "max_pages": params.get('max_pages', 10),
                            "target_pages": params.get('target_pages', ''),
                            "skip_diagonal_text": params.get('skip_diagonal_text', False),
                            "skip_table_extraction": params.get('skip_table_extraction', False),
                            "extract_images": params.get('extract_images', False),
                            "use_vendor_multimodal_model": params.get('use_vendor_multimodal_model', False),
                            "vendor_multimodal_api_key": params.get('vendor_multimodal_api_key', ''),
                            "polling_interval": params.get('polling_interval', 10)
                        }
                        
                        # 使用LlamaService处理文件（使用本地文件路径）
                        result = await llama_service.process_file(local_file_path, successful_results, failed_files, llama_params)
                    finally:
                        ticker.cancel()
                        try:
                            await ticker
                        except asyncio.CancelledError:
                            pass
                    
                    # 单个文件完成后更新到真实进度（解析阶段结束，即将进入 AI 或保存）
                    self.update_status(progress=10.0 + ((i + 1) / len(file_paths)) * (extraction_max - 10.0))
                    
                    if result:
                        # LlamaService返回的是元组 (file_info, processing_time)
                        file_info, processing_time = result
                        
                        # 检查文件是否真的处理成功
                        if file_info.get('is_success', False):
                            # LlamaService已经将结果添加到successful_results中
                            self.files_processed += 1
                            self.update_status(log_message=f"文件 {file_name} 处理成功")
                        else:
                            # 处理失败，记录错误
                            error_msg = file_info.get('error', '处理失败')
                            failed_files.append({"file_path": file_path, "error": error_msg})
                            self.files_failed += 1
                            self.update_status(log_message=f"文件 {file_name} 处理失败: {error_msg}")
                    else:
                        failed_files.append({"file_path": file_path, "error": "处理失败"})
                        self.files_failed += 1
                        self.update_status(log_message=f"文件 {file_name} 处理失败")
                        
                except Exception as e:
                    failed_files.append({"file_path": file_path, "error": str(e)})
                    self.files_failed += 1
                    self.update_status(log_message=f"文件 {os.path.basename(file_path)} 处理出错: {str(e)}")
            
            # 🔧 修正：将 successful_results 中的 original_file 从 local_file_path 改回原始 file_path（OSS object_name）
            # 因为数据库中需要保存 OSS 路径用于后续预览
            for result_item in successful_results:
                original_file = result_item.get('original_file', '')
                # 使用映射表查找原始 OSS 路径
                if original_file in temp_to_original_mapping:
                    result_item['original_file'] = temp_to_original_mapping[original_file]
            
            # 🧹 清理所有临时下载的文件
            if temp_files_to_cleanup:
                self.update_status(log_message=f"正在清理 {len(temp_files_to_cleanup)} 个临时文件...")
                for temp_path in temp_files_to_cleanup:
                    try:
                        if os.path.exists(temp_path):
                            os.remove(temp_path)
                    except Exception as e:
                        self.update_status(log_message=f"清理临时文件失败: {str(e)}")
            
            # 如果有成功的结果，进行AI分析
            final_report = None
            file_ai_times = []
            ai_analysis_success = True  # 跟踪AI分析处理是否成功
            
            if successful_results and (rc_agent_service or llm_analysis_service) and not self.should_stop:
                try:
                    if analysis_mode == 'local_llm' and llm_analysis_service:
                        # 使用本地LLM分析
                        self.update_status("ai_processing", 80.0, log_message="正在使用本地LLM分析...")
                        
                        # 准备模板变量
                        template_variables = params.get('template_variables', {})
                        
                        # 调用本地LLM分析服务
                        final_report, file_ai_times = await llm_analysis_service.process_all_contents(
                            successful_results,
                            custom_variables=template_variables,
                            user_id=getattr(self, 'user_id', None),
                            provider=params.get('llm_provider'),
                            model=params.get('llm_model')
                        )
                    elif rc_agent_service:
                        # 使用远程RC Agent分析（现有逻辑）
                        self.update_status("ai_processing", 80.0, log_message="正在发送到RC Agent处理...")
                        agent_type = task_info.get('agent_type', 'chatbot')
                        final_report, file_ai_times = rc_agent_service.process_all_contents(
                            successful_results, 
                            custom_inputs,
                            agent_type
                        )
                    
                    # 检查AI分析是否真的返回了结果
                    if final_report and file_ai_times:
                        mode_name = "本地LLM" if analysis_mode == 'local_llm' else "RC Agent"
                        self.update_status(log_message=f"{mode_name}处理完成")
                    else:
                        ai_analysis_success = False
                        mode_name = "本地LLM" if analysis_mode == 'local_llm' else "RC Agent"
                        self.update_status(log_message=f"{mode_name}处理完成但未返回有效结果")
                        
                except Exception as e:
                    ai_analysis_success = False
                    error_msg = str(e)
                    mode_name = "本地LLM" if analysis_mode == 'local_llm' else "RC Agent"
                    self.update_status(log_message=f"{mode_name}处理失败: {error_msg}")
                    
                    # 如果是远程Agent的认证错误，提供更友好的错误信息
                    if analysis_mode == 'remote_agent':
                        if "401" in error_msg or "unauthorized" in error_msg.lower():
                            self.update_status(log_message="RC Agent认证失败，请检查token配置")
                        elif "connection" in error_msg.lower() or "timeout" in error_msg.lower():
                            self.update_status(log_message="RC Agent服务器连接失败，请检查网络连接")
                    
                    # 确保在失败时也有默认值
                    final_report = None
                    file_ai_times = []
            
            # 保存处理结果到数据库
            if not self.should_stop:
                self.update_status("saving", 95.0, log_message="正在保存处理结果...")
                
                # 确定AI分析状态
                if not successful_results or (not rc_agent_service and not llm_analysis_service):
                    # 没有成功的文件或没有AI分析服务，跳过AI分析
                    rc_status = 'skipped'
                elif ai_analysis_success and final_report:
                    # AI分析处理成功且有报告
                    rc_status = 'success'
                elif not ai_analysis_success:
                    # AI分析处理失败
                    rc_status = 'failed'
                else:
                    # 其他情况（如部分成功）
                    rc_status = 'failed'
                
                await self.save_processing_results(
                    task_info=task_info,
                    params=params,
                    custom_inputs=custom_inputs,
                    successful_results=successful_results,
                    failed_files=failed_files,
                    final_report=final_report,
                    file_ai_times=file_ai_times,
                    rc_agent_status=rc_status
                )
            
            if self.should_stop:
                self.update_status("stopped", log_message="处理已停止")
            else:
                # 根据处理结果决定最终状态
                mode_name = "本地LLM" if analysis_mode == 'local_llm' else "AI Agent"
                
                if not ai_analysis_success:
                    # AI分析处理失败，但文件处理成功，标记为部分成功
                    self.update_status("completed", 100.0, log_message=f"文件处理成功，但{mode_name}处理失败")
                elif successful_results and (rc_agent_service or llm_analysis_service) and not final_report:
                    # 有成功的文件处理结果，需要AI分析，但未获得报告内容
                    self.update_status("completed", 100.0, log_message=f"文件处理成功，但{mode_name}未返回有效结果")
                elif failed_files and not successful_results:
                    # 所有文件都处理失败
                    self.update_status("failed", 100.0, log_message="任务处理失败：所有文件处理失败")
                else:
                    # 任务成功完成
                    self.update_status("completed", 100.0, log_message="文件处理完成")
            
            return {
                "files_processed": self.files_processed,
                "files_failed": self.files_failed,
                "total_files": self.total_files,
                "successful_results": successful_results,
                "failed_files": failed_files,
                "final_report": final_report,
                "processing_time": time.time() - self.start_time
            }
            
        except Exception as e:
            self.update_status("failed", log_message=f"处理过程中发生错误: {str(e)}")
            raise
    
    async def save_processing_results(
        self,
        task_info: Dict[str, Any],
        params: Dict[str, Any],
        custom_inputs: Dict[str, Any],
        successful_results: List[Dict[str, Any]],
        failed_files: List[Dict[str, Any]],
        final_report: str = None,
        file_ai_times: List[Dict[str, Any]] = None,
        rc_agent_status: str = 'pending'
    ):
        """
        保存处理结果到数据库
        
        Args:
            task_info: 任务信息
            params: 处理参数
            custom_inputs: 自定义输入参数
            successful_results: 成功处理的结果
            failed_files: 失败的文件
            final_report: 最终报告
            file_ai_times: 文件AI处理时间
        """
        try:
            conn = self.db_manager.get_connection()
            if not conn:
                self.update_status(log_message="无法连接到数据库，跳过保存结果")
                return
            
            cursor = conn.cursor()
            
            # 计算总时间
            total_time = time.time() - self.start_time
            
            # 计算文件处理时间和RC Agent处理时间
            total_file_processing_time = sum(
                result.get('processing_time', 0) for result in successful_results
            )
            
            total_ai_time = sum(
                file_time.get('ai_time', 0) for file_time in (file_ai_times or [])
            )
            
            # 计算总tokens和总费用
            total_tokens = sum(
                file_time.get('total_tokens', 0) for file_time in (file_ai_times or [])
            )
            
            total_price = sum(
                file_time.get('total_price', 0) for file_time in (file_ai_times or [])
            )
            
            # 提取模型信息（从第一个文件的model_usage中）
            # 只记录实际的 LLM 模型和提供商，不将 "Dify Agent" 等平台名作为 Analysis Model
            # （Agent Name 已单独展示 RC Agent 名称，"Analysis Model" 应显示底层模型如 gpt-4、claude 等）
            llm_model = None
            llm_provider = None
            if file_ai_times:
                for file_time in file_ai_times:
                    model_usage_list = file_time.get('model_usage', [])
                    if model_usage_list and len(model_usage_list) > 0:
                        llm_model = model_usage_list[0].get('model_name')
                        # 本地 LLM 时 provider 在 file_time 中；远程 Agent 时 Dify 可能通过 model_usage 返回
                        llm_provider = file_time.get('provider')
                        break
            
            # 准备上传参数（转换为JSON字符串）
            upload_params = json.dumps({
                "task_type": task_info.get('name', ''),
                "custom_inputs": custom_inputs,
                "processing_params": params
            })
            
            # 🔍 优先使用预先创建的记录ID，否则查找已存在的记录
            processing_stats_id = self.existing_processing_stats_id
            
            if processing_stats_id:
                self.update_status(log_message=f"使用预创建的处理记录(ID: {processing_stats_id})，正在更新...")
            elif successful_results:
                # 如果没有预创建的ID，尝试通过 original_file_path 查找
                first_file_path = successful_results[0].get('original_file')
                if first_file_path:
                    cursor.execute("""
                    SELECT ps.id
                    FROM processing_stats ps
                    JOIN file_processing_details fpd ON ps.id = fpd.processing_stats_id
                    WHERE fpd.original_file_path = %s 
                      AND ps.rc_agent_status IN ('uploaded', 'processing')
                      AND ps."user" = %s
                    ORDER BY ps.timestamp DESC
                    LIMIT 1
                    """, (first_file_path, self.username))
                    
                    existing_record = cursor.fetchone()
                    if existing_record:
                        processing_stats_id = existing_record[0]
                        self.update_status(log_message=f"找到已存在的上传记录(ID: {processing_stats_id})，将更新该记录")
            
            if processing_stats_id:
                # 🔄 更新已存在的记录
                cursor.execute("""
                UPDATE processing_stats
                SET timestamp = %s,
                    files_processed = %s,
                    files_failed = %s,
                    total_time = %s,
                    total_file_processing_time = %s,
                    total_ai_time = %s,
                    task_name = %s,
                    total_tokens = %s,
                    total_price = %s,
                    upload_params = %s,
                    final_report = %s,
                    rc_agent_status = %s,
                    llm_model = %s,
                    llm_provider = %s
                WHERE id = %s
                """, (
                    datetime.now(),
                    self.files_processed,
                    self.files_failed,
                    total_time,
                    total_file_processing_time,
                    total_ai_time,
                    task_info.get('name', ''),
                    total_tokens,
                    total_price,
                    upload_params,
                    final_report or "",
                    rc_agent_status,
                    llm_model,
                    llm_provider,
                    processing_stats_id
                ))
                
                self.update_status(log_message="已更新现有处理记录")
            else:
                # ➕ 如果没有找到已有记录，创建新记录
                cursor.execute("""
                INSERT INTO processing_stats 
                (timestamp, files_processed, files_failed, total_time, 
                 total_file_processing_time, total_ai_time, task_name, 
                 "user", total_tokens, total_price, upload_params, 
                 final_report, rc_agent_status, llm_model, llm_provider)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """, (
                    datetime.now(),
                    self.files_processed,
                    self.files_failed,
                    total_time,
                    total_file_processing_time,
                    total_ai_time,
                    task_info.get('name', ''),
                    self.username,
                    total_tokens,
                    total_price,
                    upload_params,
                    final_report or "",
                    rc_agent_status,
                    llm_model,
                    llm_provider
                ))
                
                processing_stats_id = cursor.fetchone()[0]
                self.update_status(log_message="已创建新的处理记录")
            
            # 保存或更新文件处理详情
            for result in successful_results:
                original_file_path = result['original_file']
                
                # 查找对应的AI时间信息
                ai_info = None
                if file_ai_times:
                    ai_info = next(
                        (ft for ft in file_ai_times if ft['file_path'] == original_file_path),
                        None
                    )
                
                # 🔥 如果有预创建的 file_detail_id，则更新该记录
                if self.existing_file_detail_id:
                    cursor.execute("""
                    UPDATE file_processing_details
                    SET processing_time = %s,
                        ai_time = %s,
                        content_length = %s,
                        status = %s,
                        total_tokens = %s,
                        cost = %s,
                        content = %s
                    WHERE id = %s
                    RETURNING id
                    """, (
                        result.get('processing_time', 0),
                        ai_info.get('ai_time', 0) if ai_info else 0,
                        len(result.get('content', '')),
                        'completed',
                        ai_info.get('total_tokens', 0) if ai_info else 0,
                        ai_info.get('total_price', 0) if ai_info else 0,
                        result.get('content', ''),
                        self.existing_file_detail_id
                    ))
                    
                    file_processing_id = cursor.fetchone()[0]
                    self.update_status(log_message=f"已更新文件处理记录 (ID: {file_processing_id})")
                else:
                    # 否则，按原逻辑插入新记录
                    # 🔧 如果是 OSS 文件，需要特殊处理文件信息
                    storage_type = 'local'
                    oss_bucket = None
                    
                    if self._is_oss_object_key(original_file_path):
                        storage_type = 'oss'
                        
                        # OSS 文件：从数据库查询已有的文件信息（保留原始文件名）
                        cursor.execute("""
                            SELECT file_type, file_size_mb, filename, oss_bucket
                            FROM file_processing_details
                            WHERE original_file_path = %s
                            ORDER BY created_at DESC
                            LIMIT 1
                        """, (original_file_path,))
                        existing_file_info = cursor.fetchone()
                        
                        if existing_file_info:
                            # 使用已有的文件信息（保留原始文件名）
                            file_info = {
                                'file_type': existing_file_info[0],
                                'file_size_mb': existing_file_info[1],
                                'file_name': existing_file_info[2]
                            }
                            oss_bucket = existing_file_info[3]
                            self.update_status(log_message=f"使用原始文件名: {existing_file_info[2]}")
                        else:
                            # 从 OSS object_name 中提取原始文件名
                            oss_file_name = os.path.basename(original_file_path)
                            
                            import re
                            match = re.match(r'^[0-9a-f]{8}_\d{8}_\d{6}_(.+)$', oss_file_name)
                            if match:
                                file_name = match.group(1)
                                self.update_status(log_message=f"从 OSS 路径提取原始文件名: {file_name}")
                            else:
                                file_name = oss_file_name
                                self.update_status(log_message=f"⚠️ 无法提取原始文件名，使用: {file_name}")
                            
                            file_ext = os.path.splitext(file_name)[1].lower()
                            
                            ext_to_type = {
                                '.pdf': 'pdf',
                                '.docx': 'word', '.doc': 'word',
                                '.xlsx': 'excel', '.xls': 'excel',
                                '.csv': 'csv',
                                '.jpg': 'image', '.jpeg': 'image', '.png': 'image', '.gif': 'image', '.bmp': 'image'
                            }
                            file_type = ext_to_type.get(file_ext, 'other')
                            
                            file_info = {
                                'file_name': file_name,
                                'file_type': file_type,
                                'file_size_mb': 0
                            }
                            
                            oss_bucket = os.getenv('OSS_BUCKET_NAME')
                    else:
                        # 本地文件：使用 file_processor 获取文件信息
                        file_info = self.file_processor.get_file_info(original_file_path)
                    
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
                        file_info['file_name'],
                        file_info['file_type'],
                        file_info['file_size_mb'],
                        result.get('processing_time', 0),
                        ai_info.get('ai_time', 0) if ai_info else 0,
                        len(result.get('content', '')),
                        'completed',
                        ai_info.get('total_tokens', 0) if ai_info else 0,
                        ai_info.get('total_price', 0) if ai_info else 0,
                        result.get('content', ''),
                        datetime.now(),
                        self.task_id,
                        result['original_file'],
                        storage_type,
                        oss_bucket
                    ))
                    
                    file_processing_id = cursor.fetchone()[0]
                
                # 标签功能已移除 - 改用自动打标签（content_tag_v1）在任务完成后处理
            
            # 保存或更新失败文件的记录
            for failed_file in failed_files:
                try:
                    # 🔥 如果有预创建的 file_detail_id，则更新该记录为失败状态
                    if self.existing_file_detail_id:
                        cursor.execute("""
                        UPDATE file_processing_details
                        SET status = %s,
                            error_message = %s
                        WHERE id = %s
                        """, (
                            'failed',
                            failed_file['error'],
                            self.existing_file_detail_id
                        ))
                        self.update_status(log_message=f"已更新文件为失败状态 (ID: {self.existing_file_detail_id})")
                    else:
                        # 否则，插入新的失败记录
                        file_info = self.file_processor.get_file_info(failed_file['file_path'])
                        cursor.execute("""
                        INSERT INTO file_processing_details 
                        (processing_stats_id, filename, file_type, file_size_mb, 
                         processing_time, ai_time, content_length, status, 
                         total_tokens, cost, error_message, created_at, task_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            processing_stats_id,
                            file_info['file_name'],
                            file_info['file_type'],
                            file_info['file_size_mb'],
                            0,
                            0,
                            0,
                            'failed',
                            0,
                            0,
                            failed_file.get('error', ''),
                            datetime.now(),
                            self.task_id
                        ))
                except Exception as e:
                    # 如果获取文件信息失败，使用文件路径
                    if self.existing_file_detail_id:
                        # 如果有预创建的记录，更新为失败状态
                        cursor.execute("""
                        UPDATE file_processing_details
                        SET status = %s,
                            error_message = %s
                        WHERE id = %s
                        """, (
                            'failed',
                            failed_file.get('error', ''),
                            self.existing_file_detail_id
                        ))
                    else:
                        # 否则插入新的失败记录
                        cursor.execute("""
                        INSERT INTO file_processing_details 
                        (processing_stats_id, filename, file_type, file_size_mb, 
                         processing_time, ai_time, content_length, status, 
                         total_tokens, cost, error_message, created_at, task_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            processing_stats_id,
                            os.path.basename(failed_file['file_path']),
                            'unknown',
                            0,
                            0,
                        0,
                        0,
                        'failed',
                        0,
                        0,
                        failed_file.get('error', ''),
                        datetime.now(),
                        self.task_id
                    ))
            
            conn.commit()
            self.update_status(log_message="处理结果已保存到数据库")
            
        except Exception as e:
            if conn:
                conn.rollback()
            self.update_status(log_message=f"保存处理结果时出错: {str(e)}")
            raise
        finally:
            if conn:
                conn.close() 