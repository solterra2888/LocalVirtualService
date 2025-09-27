# -*- coding: utf-8 -*-
"""
文件处理API
"""

import os
import uuid
import asyncio
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from fastapi import APIRouter, File, UploadFile, HTTPException, status, Depends, BackgroundTasks

from backend.schemas.file import (
    FileUploadResponse,
    FileProcessRequest,
    FileProcessResponse,
    FileDetailResponse,
    FileType,
    ProcessingParamsRequest
)
from backend.schemas.file_tag import (
    FileTagCreate,
    FileTagUpdate,
    FileTagResponse,
    FileTagBatchCreate,
    FileTagStats,
    FileWithTag
)
from backend.auth.jwt_utils import get_current_user
from backend.processors.file_processor import FileProcessor
from backend.processors.task_manager import TaskManager
from backend.database.db_manager import DatabaseManager
from backend.core.llama_service import LlamaService
from backend.core.rc_agent_service import RCAgentService
from backend.workers.file_worker import FileProcessingWorker
from backend.utils.logger import info, error, warning, debug
from backend.utils.database_config import get_database_config

# 统一的环境变量加载
from backend.utils.env_loader import load_project_env

router = APIRouter()

# 上传文件目录
UPLOAD_DIR = "uploads"
os.makedirs(UPLOAD_DIR, exist_ok=True)

# 全局任务状态存储
task_statuses = {}


def safe_file_type_conversion(file_type_str: str) -> FileType:
    """
    安全地将文件类型字符串转换为FileType枚举
    
    Args:
        file_type_str: 文件类型字符串
        
    Returns:
        FileType枚举值
    """
    if not file_type_str:
        return FileType.OTHER
    
    # 转换为小写进行匹配
    file_type_lower = file_type_str.lower()
    
    # 创建映射字典
    type_mapping = {
        'pdf': FileType.PDF,
        'excel': FileType.EXCEL,
        'xlsx': FileType.EXCEL,
        'xls': FileType.EXCEL,
        'doc': FileType.DOC,
        'docx': FileType.DOC,
        'image': FileType.IMAGE,
        'jpg': FileType.IMAGE,
        'jpeg': FileType.IMAGE,
        'png': FileType.IMAGE,
        'gif': FileType.IMAGE,
        'bmp': FileType.IMAGE,
    }
    
    return type_mapping.get(file_type_lower, FileType.OTHER)


def get_file_type(filename: str) -> FileType:
    """根据文件名获取文件类型"""
    ext = filename.lower().split('.')[-1]
    if ext == 'pdf':
        return FileType.PDF
    elif ext in ['xlsx', 'xls']:
        return FileType.EXCEL
    elif ext in ['doc', 'docx']:
        return FileType.DOC
    elif ext in ['jpg', 'jpeg', 'png', 'gif', 'bmp']:
        return FileType.IMAGE
    else:
        return FileType.OTHER




@router.post("/upload", response_model=List[FileUploadResponse], summary="上传文件")
async def upload_files(
    files: List[UploadFile] = File(...),
    current_user=Depends(get_current_user)
):
    """
    上传文件
    
    - **files**: 要上传的文件列表
    """
    if not files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="请选择要上传的文件"
        )
    
    file_processor = FileProcessor()
    uploaded_files = []
    
    for file in files:
        if not file.filename:
            continue
            
        # 检查文件类型
        if not file_processor.is_supported_file(file.filename):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"不支持的文件类型: {file.filename}"
            )
        
        # 生成唯一文件名
        file_id = str(uuid.uuid4())[:8]  # 只取前8位
        # 分离文件名和扩展名
        name, ext = os.path.splitext(file.filename)
        filename = f"{name}_{file_id}{ext}"
        file_path = os.path.join(UPLOAD_DIR, filename)
        
        # 保存文件
        with open(file_path, "wb") as buffer:
            content = await file.read()
            buffer.write(content)
        
        # 获取文件信息
        file_info = file_processor.get_file_info(file_path)
        file_size = len(content)
        file_type = get_file_type(file.filename)
        
        uploaded_files.append(
            FileUploadResponse(
                filename=file.filename,
                file_path=file_path,
                file_size=file_size,
                file_type=file_type,
                upload_time=datetime.now()
            )
        )
    
    return uploaded_files


@router.get("/supported-types", summary="获取支持的文件类型")
async def get_supported_file_types():
    """获取支持的文件类型列表"""
    file_processor = FileProcessor()
    return {
        "supported_extensions": file_processor.get_supported_extensions(),
        "descriptions": file_processor.get_file_extension_description()
    }


@router.post("/validate", summary="验证文件")
async def validate_files(
    file_paths: List[str],
    current_user=Depends(get_current_user)
):
    """
    验证文件列表
    
    - **file_paths**: 文件路径列表
    """
    file_processor = FileProcessor()
    
    # 验证文件
    valid_files, invalid_files = file_processor.validate_files(file_paths)
    
    # 获取文件类型统计
    file_type_summary = file_processor.get_file_type_summary(valid_files)
    
    return {
        "valid_files": valid_files,
        "invalid_files": invalid_files,
        "summary": file_type_summary
    }


@router.post("/process", response_model=FileProcessResponse, summary="处理文件")
async def process_files(
    request: FileProcessRequest,
    background_tasks: BackgroundTasks,
    current_user=Depends(get_current_user)
):
    """
    处理文件
    
    - **file_paths**: 文件路径列表
    - **task_type**: RC Agent
    - **custom_inputs**: 自定义输入参数
    - **params**: 处理参数
    """
    if not request.file_paths:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="请选择要处理的文件"
        )
    
    # 验证文件存在
    file_processor = FileProcessor()
    valid_files, invalid_files = file_processor.validate_files(request.file_paths)
    
    if invalid_files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"存在无效文件: {[f['path'] for f in invalid_files]}"
        )
    
    # 获取数据库连接参数
    db_conn_params = get_database_config()
    
    # 获取任务信息
    task_manager = TaskManager(current_user.username, current_user.is_admin, db_conn_params)
    task_manager.load_agent_credentials()
    
    # 验证任务选择
    is_valid, message, task_info = task_manager.validate_task_selection(
        request.task_type, 
        request.params.get('use_ai_agent', True)
    )
    
    if not is_valid:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=message
        )
    
    # 生成任务ID
    task_id = str(uuid.uuid4())
    
    # 初始化任务状态
    task_statuses[task_id] = {
        "status": "starting",
        "progress": 0.0,
        "current_file": None,
        "files_processed": 0,
        "files_failed": 0,
        "total_files": len(valid_files),
        "logs": [],
        "start_time": time.time()
    }
    
    # 创建后台任务处理文件
    background_tasks.add_task(
        process_files_background,
        task_id=task_id,
        file_paths=valid_files,
        task_info=task_info,
        custom_inputs=request.custom_inputs,
        params=request.params,
        username=current_user.username,
        db_conn_params=db_conn_params
    )
    
    return FileProcessResponse(
        task_id=task_id,
        message="文件处理任务已启动",
        files_count=len(valid_files)
    )


async def process_files_background(
    task_id: str,
    file_paths: List[str],
    task_info: dict,
    custom_inputs: dict,
    params: dict,
    username: str,
    db_conn_params: dict
):
    """后台处理文件"""
    def update_task_status(status: str = None, progress: float = None, 
                          current_file: str = None, log_message: str = None, 
                          log_level: str = "info"):
        """更新任务状态"""
        if task_id in task_statuses:
            if status:
                task_statuses[task_id]["status"] = status
            if progress is not None:
                task_statuses[task_id]["progress"] = progress
            if current_file:
                task_statuses[task_id]["current_file"] = current_file
            if log_message:
                task_statuses[task_id]["logs"].append({
                    "timestamp": datetime.now().isoformat(),
                    "level": log_level,
                    "message": log_message
                })
                # 同时写入统一日志系统
                log_func = {"debug": debug, "info": info, "warning": warning, "error": error}.get(log_level, info)
                log_func(log_message, source="task", task_id=task_id)
    
    try:
        update_task_status("initializing", 0.0, log_message="正在初始化处理服务...")
        
        # 获取API密钥 - 优先使用用户提供的密钥，否则使用配置中的默认密钥
        api_key = params.get('llamaindex_api_key') or os.getenv('LLAMA_CLOUD_API_KEY')
        if not api_key:
            update_task_status("failed", log_message="LlamaIndex API密钥未配置")
            return
        
        # 初始化服务
        llama_service = LlamaService(
            api_key=api_key, 
            polling_interval=params.get('polling_interval', 10),
            log_callback=update_task_status
        )
        
        use_ai_agent = params.get('use_ai_agent', True)
        rc_agent_service = None
        if use_ai_agent and task_info.get('token'):
            token = task_info.get('token')
            base_url = params.get('ai_agent_server_url', os.getenv('RC_AGENT_BASE_URL', 'http://111.230.48.119:81/v1/chat-messages'))
            # update_task_status(log_message=f"🔑 使用RC Agent Token: {token[:20]}... (长度: {len(token)})")
            # update_task_status(log_message=f"🌐 使用RC Agent服务器地址: {base_url}")
            rc_agent_service = RCAgentService(rc_agent_token=token, base_url=base_url, log_callback=update_task_status)
        
        update_task_status("processing", 5.0, log_message="开始处理文件...")
        
        # 处理文件
        successful_results = []
        failed_files = []
        
        for i, file_path in enumerate(file_paths):
            try:
                file_name = os.path.basename(file_path)
                update_task_status(
                    current_file=file_name,
                    progress=10.0 + (i / len(file_paths)) * 70.0,
                    # log_message=f"正在处理文件: {file_name}"
                )
                
                # 使用LlamaService处理文件
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
                
                # 创建临时的结果收集器
                results_collector = []
                file_failed_files = []
                
                result_info, processing_time = await llama_service.process_file(
                    file_path, results_collector, file_failed_files, llama_params
                )
                
                if result_info and result_info.get('is_success', False):
                    successful_results.extend(results_collector)
                    task_statuses[task_id]["files_processed"] += 1
                    update_task_status(log_message=f"文件 {file_name} 处理成功")
                else:
                    error_message = result_info.get('error', '处理失败') if result_info else '处理失败'
                    failed_files.append({"file_path": file_path, "error": error_message})
                    task_statuses[task_id]["files_failed"] += 1
                    update_task_status(log_message=f"文件 {file_name} 处理失败: {error_message}", log_level="error")
                    
            except Exception as e:
                error_message = str(e)
                failed_files.append({"file_path": file_path, "error": error_message})
                task_statuses[task_id]["files_failed"] += 1
                update_task_status(log_message=f"文件 {os.path.basename(file_path)} 处理出错: {error_message}", log_level="error")
        
        # 如果有成功的结果，发送到RC Agent
        final_report = None
        file_ai_times = []
        rc_agent_success = True  # 跟踪RC Agent处理是否成功
        
        if successful_results and rc_agent_service:
            try:
                update_task_status("ai_processing", 80.0, log_message="正在发送到RC Agent处理...")
                agent_type = task_info.get('agent_type', 'chatbot')
                final_report, file_ai_times = rc_agent_service.process_all_contents(successful_results, custom_inputs, agent_type)
                update_task_status(log_message="RC Agent处理完成")
            except Exception as e:
                rc_agent_success = False
                update_task_status(log_message=f"RC Agent处理失败: {str(e)}")
                # 确保在失败时也有默认值
                final_report = None
                file_ai_times = []
        
        # 保存处理结果到数据库
        update_task_status("saving", 95.0, log_message="正在保存处理结果...")
        
        # 确定RC Agent状态
        if not successful_results or not rc_agent_service:
            # 没有成功的文件或没有RC Agent服务，跳过RC Agent处理
            rc_status = 'skipped'
        elif rc_agent_success and final_report:
            # RC Agent处理成功且有报告
            rc_status = 'success'
        elif not rc_agent_success:
            # RC Agent处理失败
            rc_status = 'failed'
        else:
            # 其他情况（如部分成功）
            rc_status = 'failed'
        
        try:
            # 保存处理结果到数据库
            db_manager = DatabaseManager(db_conn_params)
            conn = db_manager.get_connection()
            
            if conn:
                cursor = conn.cursor()
                
                # 计算总时间
                total_time = time.time() - task_statuses[task_id]["start_time"]
                
                # 计算文件处理时间和AI处理时间
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
                
                # 准备上传参数
                upload_params = {
                    "task_type": task_info.get('name', ''),
                    "custom_inputs": custom_inputs,
                    "processing_params": params
                }
                
                # 插入处理统计记录
                cursor.execute("""
                INSERT INTO processing_stats 
                (timestamp, files_processed, files_failed, total_time, 
                 total_file_processing_time, total_ai_time, task_name, 
                 "user", total_tokens, total_price, upload_params, 
                 final_report, rc_agent_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """, (
                    datetime.now().isoformat(),
                    len(successful_results),
                    len(failed_files),
                    total_time,
                    total_file_processing_time,
                    total_ai_time,
                    task_info.get('name', ''),
                    username,
                    total_tokens,
                    total_price,
                    json.dumps(upload_params, ensure_ascii=False),
                    final_report or "",
                    rc_status
                ))
                
                processing_stats_id = cursor.fetchone()[0]
                
                # 插入文件处理详情
                file_processor = FileProcessor()
                for result in successful_results:
                    try:
                        file_info = file_processor.get_file_info(result['original_file'])
                        
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
                            task_id,
                            result['original_file']  # 保存原始文件路径
                        ))
                        
                        # 获取插入的文件ID
                        file_processing_id = cursor.fetchone()[0]
                        
                        # 如果处理参数中包含标签信息，自动设置标签
                        if (params.get('sector') and params.get('industry')):
                            try:
                                # 获取用户ID
                                user_id = None
                                if username:
                                    cursor.execute("SELECT id FROM users WHERE username = %s", (username,))
                                    user_result = cursor.fetchone()
                                    if user_result:
                                        user_id = user_result[0]
                                
                                cursor.execute("""
                                INSERT INTO file_tags (file_id, sector, industry, ticker, tagged_by)
                                VALUES (%s, %s, %s, %s, %s)
                                """, (
                                    file_processing_id,
                                    params.get('sector'),
                                    params.get('industry'),
                                    params.get('ticker'),
                                    user_id
                                ))
                                update_task_status(log_message=f"✅ 已为文件 {file_info['file_name']} 设置标签: {params.get('sector')} -> {params.get('industry')}")
                            except Exception as tag_error:
                                update_task_status(log_message=f"⚠️ 为文件 {file_info['file_name']} 设置标签失败: {str(tag_error)}", log_level="warning")
                                # 标签设置失败不应该影响文件处理
                    except Exception as e:
                        # 如果获取文件信息失败，使用基本信息
                        cursor.execute("""
                        INSERT INTO file_processing_details 
                        (processing_stats_id, filename, file_type, file_size_mb, 
                         processing_time, ai_time, content_length, status, 
                         total_tokens, cost, content, created_at, task_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """, (
                            processing_stats_id,
                            os.path.basename(result['original_file']),
                            'unknown',
                            0,
                            result.get('processing_time', 0),
                            0,
                            len(result.get('content', '')),
                            'completed',
                            0,
                            0,
                            result.get('content', ''),
                            datetime.now(),
                            task_id
                        ))
                        
                        # 获取插入的文件ID
                        file_processing_id = cursor.fetchone()[0]
                        
                        # 如果处理参数中包含标签信息，自动设置标签
                        if (params.get('sector') and params.get('industry')):
                            try:
                                # 获取用户ID
                                user_id = None
                                if username:
                                    cursor.execute("SELECT id FROM users WHERE username = %s", (username,))
                                    user_result = cursor.fetchone()
                                    if user_result:
                                        user_id = user_result[0]
                                
                                cursor.execute("""
                                INSERT INTO file_tags (file_id, sector, industry, ticker, tagged_by)
                                VALUES (%s, %s, %s, %s, %s)
                                """, (
                                    file_processing_id,
                                    params.get('sector'),
                                    params.get('industry'),
                                    params.get('ticker'),
                                    user_id
                                ))
                                update_task_status(log_message=f"✅ 已为文件 {os.path.basename(result['original_file'])} 设置标签: {params.get('sector')} -> {params.get('industry')}")
                            except Exception as tag_error:
                                update_task_status(log_message=f"⚠️ 为文件 {os.path.basename(result['original_file'])} 设置标签失败: {str(tag_error)}", log_level="warning")
                                # 标签设置失败不应该影响文件处理
                
                # 插入失败的文件记录
                for failed_file in failed_files:
                    try:
                        file_info = file_processor.get_file_info(failed_file['file_path'])
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
                            task_id
                        ))
                    except Exception as e:
                        # 如果获取文件信息失败，使用基本信息
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
                            task_id
                        ))
                
                conn.commit()
                update_task_status(log_message="处理结果已保存到数据库")
                
            else:
                update_task_status(log_message="无法连接到数据库，跳过保存结果")
                
        except Exception as e:
            update_task_status(log_message=f"保存处理结果时出错: {str(e)}")
            if conn:
                conn.rollback()
        finally:
            if conn:
                conn.close()
        
        # 根据处理结果决定最终状态
        if not rc_agent_success:
            # RC Agent处理失败
            update_task_status("failed", 100.0, log_message="任务处理失败：RC Agent处理出错")
        elif successful_results and rc_agent_service and not final_report:
            # 有成功的LlamaIndex结果，需要RC Agent处理，但未获得报告内容
            update_task_status("failed", 100.0, log_message="任务处理失败：RC Agent未返回有效结果")
        elif failed_files and not successful_results:
            # 所有文件都处理失败
            update_task_status("failed", 100.0, log_message="任务处理失败：所有文件处理失败")
        else:
            # 任务成功完成
            update_task_status("completed", 100.0, log_message="文件处理完成")
        
    except Exception as e:
        update_task_status("failed", log_message=f"处理过程中发生错误: {str(e)}")


@router.get("/tasks/{task_id}/status", summary="获取任务状态")
async def get_task_status(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    获取任务处理状态
    
    - **task_id**: 任务ID
    """
    if task_id not in task_statuses:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="任务不存在"
        )
    
    return task_statuses[task_id]


@router.post("/tasks/{task_id}/stop", summary="停止任务")
async def stop_task(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    停止正在执行的任务
    
    - **task_id**: 任务ID
    """
    if task_id not in task_statuses:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="任务不存在"
        )
    
    task_statuses[task_id]["status"] = "stopped"
    task_statuses[task_id]["logs"].append({
        "timestamp": datetime.now().isoformat(),
        "level": "info",
        "message": "用户请求停止任务"
    })
    
    return {"message": "任务已停止"}


@router.get("/tasks/{task_id}/files", response_model=List[FileDetailResponse], summary="获取任务文件详情")
async def get_task_files(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    获取任务的文件处理详情
    
    - **task_id**: 任务ID
    """
    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="无法连接到数据库"
        )
    
    cursor = conn.cursor()
    try:
        cursor.execute("""
        SELECT id, filename, file_type, file_size_mb, processing_time,
               ai_time, content_length, status, total_tokens, cost,
               error_message, created_at, original_file_path
        FROM file_processing_details 
        WHERE task_id = %s
        ORDER BY id
        """, (task_id,))
        
        files_data = cursor.fetchall()
        
        return [
            FileDetailResponse(
                id=file_data[0],
                task_id=task_id,
                filename=file_data[1],
                file_type=safe_file_type_conversion(file_data[2]),
                file_size=file_data[3] or 0,
                processing_time=file_data[4],
                ai_time=file_data[5],
                content_length=file_data[6],
                status=file_data[7] or 'pending',
                total_tokens=file_data[8],
                cost=file_data[9],
                error_message=file_data[10],
                created_at=file_data[11],
                original_file_path=file_data[12] if len(file_data) > 12 else None
            )
            for file_data in files_data
        ]
        
    finally:
        conn.close()


@router.get("/original/{file_id}", summary="获取原始文件信息")
async def get_original_file_info(
    file_id: int,
    current_user=Depends(get_current_user)
):
    """
    获取原始文件信息
    
    - **file_id**: 文件详情ID
    """
    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="无法连接到数据库"
        )
    
    cursor = conn.cursor()
    try:
        cursor.execute("""
        SELECT filename, file_type, original_file_path, file_size_mb
        FROM file_processing_details 
        WHERE id = %s
        """, (file_id,))
        
        result = cursor.fetchone()
        
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="文件记录未找到"
            )
        
        filename, file_type, original_file_path, file_size_mb = result
        
        if not original_file_path or not os.path.exists(original_file_path):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="原始文件不存在"
            )
        
        # 构建可访问的URL - 根据环境动态调整
        def build_file_url(file_path: str) -> str:
            """根据部署环境构建正确的文件URL"""
            # 获取相对文件名
            if file_path.startswith("uploads/"):
                relative_path = file_path[8:]  # 去掉 "uploads/" 前缀
            else:
                relative_path = os.path.basename(file_path)
            
            # 检查是否为开发环境
            # 开发环境特征：DEBUG=True 或 ENVIRONMENT=development
            is_development = (
                os.getenv('DEBUG', 'false').lower() == 'true' or
                os.getenv('ENVIRONMENT', 'production').lower() == 'development' or
                os.getenv('APP_ENV', 'production').lower() == 'development'
            )
            
            if is_development:
                # 开发环境：需要包含后端服务器地址
                backend_host = os.getenv('BACKEND_HOST', 'localhost')
                backend_port = os.getenv('BACKEND_PORT', '8000')
                return f"http://{backend_host}:{backend_port}/uploads/{relative_path}"
            else:
                # 生产环境：相对路径即可
                return f"/uploads/{relative_path}"
        
        file_url = build_file_url(original_file_path)
        
        return {
            "filename": filename,
            "file_type": file_type,
            "file_size_mb": file_size_mb,
            "original_file_path": original_file_path,
            "file_url": file_url
        }
        
    finally:
        conn.close()


@router.delete("/uploads/{filename}", summary="删除上传的文件")
async def delete_uploaded_file(
    filename: str,
    current_user=Depends(get_current_user)
):
    """
    删除上传的文件
    
    - **filename**: 文件名
    """
    file_path = os.path.join(UPLOAD_DIR, filename)
    
    if not os.path.exists(file_path):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="文件不存在"
        )
    
    try:
        os.remove(file_path)
        return {"message": "文件删除成功"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"文件删除失败: {str(e)}"
        )


@router.post("/reorder", summary="重新排序文件")
async def reorder_files(
    file_paths: List[str],
    current_user=Depends(get_current_user)
):
    """
    重新排序文件列表
    
    - **file_paths**: 重新排序后的文件路径列表
    """
    # 验证文件存在
    file_processor = FileProcessor()
    valid_files, invalid_files = file_processor.validate_files(file_paths)
    
    if invalid_files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"存在无效文件: {[f['path'] for f in invalid_files]}"
        )
    
    return {
        "message": "文件排序成功",
        "ordered_files": valid_files
    }


@router.get("/processing-params", summary="获取处理参数选项")
async def get_processing_params():
    """获取文件处理的参数选项和默认值"""
    return {
        "language_options": [
            {"value": ["ch_sim", "en"], "label": "中文简体和英文"},
            {"value": ["ch_sim"], "label": "仅中文简体"},
            {"value": ["en"], "label": "仅英文"},
            {"value": ["ch_tra", "en"], "label": "中文繁体和英文"},
            {"value": ["ch_tra"], "label": "仅中文繁体"}
        ],
        "boolean_options": [
            {"key": "fast_mode", "label": "快速模式", "default": False},
            {"key": "disable_ocr", "label": "禁用OCR", "default": False},
            {"key": "skip_diagonal_text", "label": "跳过对角线文本", "default": False},
            {"key": "skip_table_extraction", "label": "跳过表格提取", "default": False},
            {"key": "extract_images", "label": "提取图像", "default": False},
            {"key": "use_vendor_multimodal_model", "label": "使用供应商多模态模型", "default": False}
        ],
        "numeric_options": [
            {"key": "max_pages", "label": "最大页数", "default": 10, "min": 1, "max": 1000},
            {"key": "polling_interval", "label": "轮询间隔(秒)", "default": 10, "min": 1, "max": 60}
        ],
        "text_options": [
            {"key": "target_pages", "label": "目标页面", "placeholder": "例如: 0,1,2 或 0-2,6-22"},
            {"key": "vendor_multimodal_api_key", "label": "供应商多模态API密钥", "type": "password"},
            {"key": "llamaindex_api_key", "label": "LlamaIndex API密钥", "type": "password"}
        ],
        "defaults": {
            "llamaindex_api_key": os.getenv('LLAMA_CLOUD_API_KEY'),
            "ai_agent_server_url": os.getenv('RC_AGENT_BASE_URL', 'http://111.230.48.119:81/v1/chat-messages')
        }
    }


@router.get("/sectors", summary="获取所有行业分类")
async def get_sectors(current_user=Depends(get_current_user)):
    """
    获取所有行业分类（sector）及其包含的子行业（industry）
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 查询所有sector和industry的组合
        query = """
        SELECT DISTINCT 
            sector, 
            industry,
            COUNT(*) as company_count
        FROM companies 
        WHERE sector IS NOT NULL 
        AND sector != '' 
        AND industry IS NOT NULL 
        AND industry != ''
        AND ticker_active = true
        GROUP BY sector, industry
        ORDER BY sector, company_count DESC
        """
        
        results = db_manager.fetch_all(query)
        
        # 按sector分组整理数据
        sectors_dict = {}
        for row in results:
            sector = row[0]
            industry = row[1]
            company_count = row[2]
            
            if sector not in sectors_dict:
                sectors_dict[sector] = {
                    "value": sector,
                    "label": sector,
                    "children": []
                }
            
            sectors_dict[sector]["children"].append({
                "value": industry,
                "label": f"{industry} ({company_count})"
            })
        
        # 转换为列表并按sector名称排序
        sectors = list(sectors_dict.values())
        sectors.sort(key=lambda x: x["label"])
        
        return sectors
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取行业分类失败: {str(e)}"
        )


@router.get("/tickers", summary="搜索股票代码")
async def search_tickers(
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    search: Optional[str] = None,
    current_user=Depends(get_current_user)
):
    """
    根据行业分类和搜索关键词搜索股票代码
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 构建查询条件
        where_conditions = ["ticker_active = true"]
        params = []
        
        if sector:
            where_conditions.append("sector = %s")
            params.append(sector)
        
        if industry:
            where_conditions.append("industry = %s")
            params.append(industry)
        
        if search:
            where_conditions.append("(code ILIKE %s OR name ILIKE %s)")
            params.extend([f"%{search}%", f"%{search}%"])
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
        SELECT 
            id,
            code,
            name,
            exchange,
            sector,
            industry
        FROM companies 
        WHERE {where_clause}
        ORDER BY name
        LIMIT 100
        """
        
        results = db_manager.fetch_all(query, params)
        
        tickers = []
        for row in results:
            ticker = {
                "id": row[0],
                "code": row[1],
                "name": row[2],
                "exchange": row[3],
                "sector": row[4],
                "industry": row[5],
                "label": f"{row[1]} - {row[2]} ({row[3]})",
                "value": row[1]
            }
            tickers.append(ticker)
        
        return tickers
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"搜索股票代码失败: {str(e)}"
        )


@router.post("/{file_id}/tags", response_model=FileTagResponse, summary="为文件设置标签")
async def create_file_tag(
    file_id: int,
    tag_data: FileTagCreate,
    current_user=Depends(get_current_user)
):
    """
    为指定文件设置标签
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 检查文件是否存在
        file_query = "SELECT id FROM files WHERE id = %s"
        file_result = db_manager.fetch_one(file_query, [file_id])
        if not file_result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="文件不存在"
            )
        
        # 检查是否已有标签，如果有则更新，否则创建
        existing_tag_query = "SELECT id FROM file_tags WHERE file_id = %s"
        existing_tag = db_manager.fetch_one(existing_tag_query, [file_id])
        
        if existing_tag:
            # 更新现有标签
            update_query = """
            UPDATE file_tags 
            SET sector = %s, industry = %s, ticker = %s, 
                tagged_by = %s, tagged_at = CURRENT_TIMESTAMP, 
                updated_at = CURRENT_TIMESTAMP
            WHERE file_id = %s
            RETURNING id, file_id, sector, industry, ticker, 
                     tagged_by, tagged_at, created_at, updated_at
            """
            result = db_manager.fetch_one(update_query, [
                tag_data.sector, tag_data.industry, tag_data.ticker,
                current_user.id, file_id
            ])
        else:
            # 创建新标签
            insert_query = """
            INSERT INTO file_tags (file_id, sector, industry, ticker, tagged_by)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, file_id, sector, industry, ticker, 
                     tagged_by, tagged_at, created_at, updated_at
            """
            result = db_manager.fetch_one(insert_query, [
                file_id, tag_data.sector, tag_data.industry, 
                tag_data.ticker, current_user.id
            ])
        
        return FileTagResponse(
            id=result[0],
            file_id=result[1],
            sector=result[2],
            industry=result[3],
            ticker=result[4],
            tagged_by=result[5],
            tagged_at=result[6],
            created_at=result[7],
            updated_at=result[8]
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"设置文件标签失败: {str(e)}"
        )


@router.get("/{file_id}/tags", response_model=FileTagResponse, summary="获取文件标签")
async def get_file_tag(
    file_id: int,
    current_user=Depends(get_current_user)
):
    """
    获取指定文件的标签
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        query = """
        SELECT id, file_id, sector, industry, ticker, 
               tagged_by, tagged_at, created_at, updated_at
        FROM file_tags 
        WHERE file_id = %s
        """
        result = db_manager.fetch_one(query, [file_id])
        
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="文件标签不存在"
            )
        
        return FileTagResponse(
            id=result[0],
            file_id=result[1],
            sector=result[2],
            industry=result[3],
            ticker=result[4],
            tagged_by=result[5],
            tagged_at=result[6],
            created_at=result[7],
            updated_at=result[8]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取文件标签失败: {str(e)}"
        )


@router.put("/{file_id}/tags", response_model=FileTagResponse, summary="更新文件标签")
async def update_file_tag(
    file_id: int,
    tag_data: FileTagUpdate,
    current_user=Depends(get_current_user)
):
    """
    更新指定文件的标签
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 检查标签是否存在
        check_query = "SELECT id FROM file_tags WHERE file_id = %s"
        existing = db_manager.fetch_one(check_query, [file_id])
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="文件标签不存在"
            )
        
        update_query = """
        UPDATE file_tags 
        SET sector = %s, industry = %s, ticker = %s, 
            tagged_by = %s, tagged_at = CURRENT_TIMESTAMP, 
            updated_at = CURRENT_TIMESTAMP
        WHERE file_id = %s
        RETURNING id, file_id, sector, industry, ticker, 
                 tagged_by, tagged_at, created_at, updated_at
        """
        result = db_manager.fetch_one(update_query, [
            tag_data.sector, tag_data.industry, tag_data.ticker,
            current_user.id, file_id
        ])
        
        return FileTagResponse(
            id=result[0],
            file_id=result[1],
            sector=result[2],
            industry=result[3],
            ticker=result[4],
            tagged_by=result[5],
            tagged_at=result[6],
            created_at=result[7],
            updated_at=result[8]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新文件标签失败: {str(e)}"
        )


@router.delete("/{file_id}/tags", summary="删除文件标签")
async def delete_file_tag(
    file_id: int,
    current_user=Depends(get_current_user)
):
    """
    删除指定文件的标签
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 检查标签是否存在
        check_query = "SELECT id FROM file_tags WHERE file_id = %s"
        existing = db_manager.fetch_one(check_query, [file_id])
        
        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="文件标签不存在"
            )
        
        delete_query = "DELETE FROM file_tags WHERE file_id = %s"
        db_manager.execute(delete_query, [file_id])
        
        return {"message": "文件标签删除成功"}
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除文件标签失败: {str(e)}"
        )


@router.post("/batch-tag", summary="批量设置文件标签")
async def batch_create_file_tags(
    batch_data: FileTagBatchCreate,
    current_user=Depends(get_current_user)
):
    """
    批量为多个文件设置相同标签
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        success_count = 0
        failed_files = []
        
        for file_id in batch_data.file_ids:
            try:
                # 检查文件是否存在
                file_query = "SELECT id FROM files WHERE id = %s"
                file_result = db_manager.fetch_one(file_query, [file_id])
                if not file_result:
                    failed_files.append({"file_id": file_id, "reason": "文件不存在"})
                    continue
                
                # 检查是否已有标签
                existing_tag_query = "SELECT id FROM file_tags WHERE file_id = %s"
                existing_tag = db_manager.fetch_one(existing_tag_query, [file_id])
                
                if existing_tag:
                    # 更新现有标签
                    update_query = """
                    UPDATE file_tags 
                    SET sector = %s, industry = %s, ticker = %s, 
                        tagged_by = %s, tagged_at = CURRENT_TIMESTAMP, 
                        updated_at = CURRENT_TIMESTAMP
                    WHERE file_id = %s
                    """
                    db_manager.execute(update_query, [
                        batch_data.sector, batch_data.industry, batch_data.ticker,
                        current_user.id, file_id
                    ])
                else:
                    # 创建新标签
                    insert_query = """
                    INSERT INTO file_tags (file_id, sector, industry, ticker, tagged_by)
                    VALUES (%s, %s, %s, %s, %s)
                    """
                    db_manager.execute(insert_query, [
                        file_id, batch_data.sector, batch_data.industry, 
                        batch_data.ticker, current_user.id
                    ])
                
                success_count += 1
                
            except Exception as e:
                failed_files.append({"file_id": file_id, "reason": str(e)})
        
        return {
            "message": f"批量标签设置完成",
            "success_count": success_count,
            "failed_count": len(failed_files),
            "failed_files": failed_files
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"批量设置文件标签失败: {str(e)}"
        )


@router.get("/by-tags", response_model=List[FileWithTag], summary="按标签查询文件")
async def get_files_by_tags(
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    ticker: Optional[str] = None,
    current_user=Depends(get_current_user)
):
    """
    根据标签查询文件
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 构建查询条件
        where_conditions = []
        params = []
        
        if sector:
            where_conditions.append("ft.sector = %s")
            params.append(sector)
        
        if industry:
            where_conditions.append("ft.industry = %s")
            params.append(industry)
        
        if ticker:
            where_conditions.append("ft.ticker = %s")
            params.append(ticker)
        
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
        
        query = f"""
        SELECT 
            f.id, f.filename, f.file_path, f.file_size, f.file_type, f.created_at,
            ft.id as tag_id, ft.sector, ft.industry, ft.ticker, 
            ft.tagged_by, ft.tagged_at, ft.created_at as tag_created_at, ft.updated_at as tag_updated_at
        FROM files f
        LEFT JOIN file_tags ft ON f.id = ft.file_id
        WHERE {where_clause}
        ORDER BY f.created_at DESC
        """
        
        results = db_manager.fetch_all(query, params)
        
        files_with_tags = []
        for row in results:
            file_data = {
                "id": row[0],
                "filename": row[1],
                "file_path": row[2],
                "file_size": row[3],
                "file_type": row[4],
                "created_at": row[5]
            }
            
            tag_data = None
            if row[6]:  # 如果有标签ID
                tag_data = {
                    "id": row[6],
                    "file_id": row[0],
                    "sector": row[7],
                    "industry": row[8],
                    "ticker": row[9],
                    "tagged_by": row[10],
                    "tagged_at": row[11],
                    "created_at": row[12],
                    "updated_at": row[13]
                }
            
            files_with_tags.append(FileWithTag(**file_data, tag=tag_data))
        
        return files_with_tags
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"按标签查询文件失败: {str(e)}"
        )


@router.get("/tag-stats", response_model=FileTagStats, summary="获取文件标签统计")
async def get_file_tag_stats(current_user=Depends(get_current_user)):
    """
    获取文件标签的统计信息
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        # 总文件数
        total_files_query = "SELECT COUNT(*) FROM files"
        total_files = db_manager.fetch_one(total_files_query)[0]
        
        # 已标签文件数
        tagged_files_query = "SELECT COUNT(DISTINCT file_id) FROM file_tags"
        tagged_files = db_manager.fetch_one(tagged_files_query)[0]
        
        # 未标签文件数
        untagged_files = total_files - tagged_files
        
        # 行业分类统计
        sectors_query = """
        SELECT sector, COUNT(*) as count 
        FROM file_tags 
        GROUP BY sector 
        ORDER BY count DESC
        """
        sectors_results = db_manager.fetch_all(sectors_query)
        sectors = [{"name": row[0], "count": row[1]} for row in sectors_results]
        
        # 子行业分类统计
        industries_query = """
        SELECT industry, COUNT(*) as count 
        FROM file_tags 
        GROUP BY industry 
        ORDER BY count DESC
        """
        industries_results = db_manager.fetch_all(industries_query)
        industries = [{"name": row[0], "count": row[1]} for row in industries_results]
        
        # 股票代码统计
        tickers_query = """
        SELECT ticker, COUNT(*) as count 
        FROM file_tags 
        WHERE ticker IS NOT NULL 
        GROUP BY ticker 
        ORDER BY count DESC
        """
        tickers_results = db_manager.fetch_all(tickers_query)
        tickers = [{"name": row[0], "count": row[1]} for row in tickers_results]
        
        return FileTagStats(
            total_files=total_files,
            tagged_files=tagged_files,
            untagged_files=untagged_files,
            sectors=sectors,
            industries=industries,
            tickers=tickers
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取文件标签统计失败: {str(e)}"
        ) 