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
    
    def set_status_callback(self, callback: Callable):
        """设置状态更新回调函数"""
        self.status_callback = callback
    
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
                        base_url = params.get('ai_agent_server_url', os.getenv('RC_AGENT_BASE_URL', 'http://111.230.48.119:81/v1/chat-messages'))
                        rc_agent_service = RCAgentService(rc_agent_token=task_info.get('token'), base_url=base_url, log_callback=self.update_status)
            
            self.update_status("processing", 5.0, log_message="开始处理文件...")
            
            # 处理文件
            successful_results = []
            failed_files = []
            
            for i, file_path in enumerate(file_paths):
                if self.should_stop:
                    self.update_status("stopped", log_message="用户请求停止处理")
                    break
                
                try:
                    file_name = os.path.basename(file_path)
                    self.update_status(
                        current_file=file_name,
                        progress=10.0 + (i / len(file_paths)) * 70.0,
                        log_message=f"正在处理文件: {file_name}"
                    )
                    
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
                    
                    # 使用LlamaService处理文件
                    result = await llama_service.process_file(file_path, successful_results, failed_files, llama_params)
                    
                    if result:
                        # LlamaService返回的是元组 (file_info, processing_time)
                        file_info, processing_time = result
                        
                        # LlamaService已经将结果添加到successful_results中，
                        # 这里只需要更新计数器和状态
                        self.files_processed += 1
                        self.update_status(log_message=f"文件 {file_name} 处理成功")
                    else:
                        failed_files.append({"file_path": file_path, "error": "处理失败"})
                        self.files_failed += 1
                        self.update_status(log_message=f"文件 {file_name} 处理失败")
                        
                except Exception as e:
                    failed_files.append({"file_path": file_path, "error": str(e)})
                    self.files_failed += 1
                    self.update_status(log_message=f"文件 {os.path.basename(file_path)} 处理出错: {str(e)}")
            
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
                        
                        # 如果有标签信息，添加到模板变量中
                        if params.get('sector'):
                            template_variables['sector'] = params.get('sector', '')
                        if params.get('industry'):
                            template_variables['industry'] = params.get('industry', '')
                        if params.get('ticker'):
                            template_variables['ticker'] = params.get('ticker', '')
                        
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
            llm_model = None
            llm_provider = None
            if file_ai_times:
                for file_time in file_ai_times:
                    model_usage_list = file_time.get('model_usage', [])
                    if model_usage_list and len(model_usage_list) > 0:
                        llm_model = model_usage_list[0].get('model_name')
                        # 如果是本地LLM分析，provider信息在file_time中
                        llm_provider = file_time.get('provider')
                        break
            
            # 如果是远程Agent模式，尝试从params中获取分析模式信息
            analysis_mode = params.get('analysis_mode', 'remote_agent')
            if analysis_mode == 'remote_agent':
                llm_provider = 'Dify Agent'
            
            # 准备上传参数（转换为JSON字符串）
            upload_params = json.dumps({
                "task_type": task_info.get('name', ''),
                "custom_inputs": custom_inputs,
                "processing_params": params
            })
            
            # 插入处理统计记录
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
            
            # 插入文件处理详情
            for result in successful_results:
                file_info = self.file_processor.get_file_info(result['original_file'])
                
                # 查找对应的AI时间信息
                ai_info = None
                if file_ai_times:
                    ai_info = next(
                        (ft for ft in file_ai_times if ft['file_path'] == result['original_file']),
                        None
                    )
                
                cursor.execute("""
                INSERT INTO file_processing_details 
                (processing_stats_id, filename, file_type, file_size_mb, 
                 processing_time, ai_time, content_length, status, 
                 total_tokens, cost, content, created_at, task_id, original_file_path)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    result['original_file']  # 保存原始文件路径
                ))
                
                # 获取插入的文件ID
                file_processing_id = cursor.fetchone()[0]
                
                # 如果处理参数中包含标签信息，自动设置标签
                if (hasattr(self, 'params') and self.params and 
                    self.params.get('sector') and self.params.get('industry')):
                    try:
                        cursor.execute("""
                        INSERT INTO file_tags (file_id, sector, industry, ticker, tagged_by)
                        VALUES (%s, %s, %s, %s, %s)
                        """, (
                            file_processing_id,
                            self.params.get('sector'),
                            self.params.get('industry'),
                            self.params.get('ticker'),
                            getattr(self, 'user_id', None)  # 如果有用户ID
                        ))
                        print(f"✅ 已为文件 {file_info['file_name']} 设置标签: {self.params.get('sector')} -> {self.params.get('industry')}")
                    except Exception as tag_error:
                        print(f"⚠️ 为文件 {file_info['file_name']} 设置标签失败: {str(tag_error)}")
                        # 标签设置失败不应该影响文件处理
            
            # 插入失败文件的记录
            for failed_file in failed_files:
                try:
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