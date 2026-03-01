# -*- coding: utf-8 -*-
"""
文件处理API - 使用Celery异步任务队列
"""

import os
import uuid
import json
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from pydantic import BaseModel
from fastapi import APIRouter, File, UploadFile, HTTPException, status, Depends, Body, Form
from fastapi.responses import FileResponse, StreamingResponse

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
from backend.utils.logger import info, error, warning, debug
from backend.utils.database_config import get_database_config
from backend.services.llm_prompt_template_service import LLMPromptTemplateService
from backend.services.oss_service import OSSService
from backend.services.asr_service import ASRService

# Celery相关导入
from celery.result import AsyncResult
from backend.celery_app import app as celery_app
from backend.tasks.file_tasks import (
    process_files_task, 
    analyze_audio_task, 
    analyze_document_task,
    upload_and_transcribe_audio_task,
    upload_and_process_document_task
)

# 统一的环境变量加载
from backend.utils.env_loader import load_project_env

router = APIRouter()

# 上传文件目录
UPLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


def _extract_filename_from_audio_url(audio_url: str) -> str:
    """
    从 audio_url 中提取原始文件名
    
    处理签名 URL 的情况：
    - 移除查询参数（?OSSAccessKeyId=...）
    - URL 解码（%E9%A6%99... → 香）
    - 提取文件名部分
    
    Args:
        audio_url: 音频文件 URL（可能包含查询参数）
    
    Returns:
        原始文件名
    
    Examples:
        - 输入: "https://bucket.oss.com/audio%2Fadmin%2F20260207_165504_%E9%A6%99.aac?OSSAccessKeyId=..."
        - 输出: "20260207_165504_香.aac"
    """
    from urllib.parse import unquote, urlparse
    
    try:
        # 移除查询参数
        url_without_params = audio_url.split('?')[0]
        
        # URL 解码
        decoded_url = unquote(url_without_params)
        
        # 提取最后的文件名部分
        filename = decoded_url.split('/')[-1]
        
        return filename
    except Exception as e:
        # 如果提取失败，返回 URL 的最后部分
        warning(f"从 URL 提取文件名失败: {str(e)}", source="api")
        return audio_url.split('/')[-1].split('?')[0]


def _build_asr_structured_data(transcriptions: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    构建结构化的 ASR 数据，用于前端说话人分离可视化
    
    Args:
        transcriptions: ASR 转写结果列表
    
    Returns:
        结构化的 ASR 数据，包含说话人列表和句子列表
    """
    if not transcriptions or len(transcriptions) == 0:
        return None
    
    # 提取第一个转写结果（通常只有一个）
    transcript = transcriptions[0]
    
    # 获取音频属性
    properties = {}
    if 'properties' in transcript:
        props = transcript['properties']
        properties = {
            'duration_ms': props.get('original_duration_in_milliseconds', 0),
            'audio_format': props.get('audio_format', ''),
            'channels': props.get('channels', []),
            'sampling_rate': props.get('original_sampling_rate', 0)
        }
    
    # 提取句子信息
    sentences = []
    speaker_ids = set()
    
    if 'transcripts' in transcript:
        for trans in transcript['transcripts']:
            for sent in trans.get('sentences', []):
                speaker_id = sent.get('speaker_id')
                if speaker_id is not None:
                    speaker_ids.add(speaker_id)
                
                sentence_data = {
                    'sentence_id': sent.get('sentence_id'),
                    'begin_time': sent.get('begin_time', 0),
                    'end_time': sent.get('end_time', 0),
                    'text': sent.get('text', ''),
                    'speaker_id': speaker_id
                }
                sentences.append(sentence_data)
    
    # 按时间排序
    sentences.sort(key=lambda x: x['begin_time'])
    
    # 构建说话人列表
    speakers = [{'speaker_id': sid, 'label': f'speaker {sid}'} for sid in sorted(speaker_ids)]
    
    # 构建最终数据结构
    structured_data = {
        'properties': properties,
        'speakers': speakers,
        'speaker_count': len(speakers),
        'sentences': sentences,
        'total_sentences': len(sentences)
    }
    
    return structured_data


def safe_file_type_conversion(file_type_str: str) -> FileType:
    """
    安全地将字符串转换为FileType枚举
    """
    try:
        return FileType(file_type_str)
    except ValueError:
        return FileType.OTHER


@router.post("/upload", response_model=List[FileUploadResponse], summary="上传文件（支持 OSS）")
async def upload_files(
    files: List[UploadFile] = File(...),
    upload_to_oss: bool = True,  # 新增：是否上传到 OSS（默认开启）
    current_user=Depends(get_current_user)
):
    """
    上传文件到服务器（支持 OSS 或本地存储）
    
    - **files**: 要上传的文件列表（当前版本仅支持单个文件）
    - **upload_to_oss**: 是否上传到 OSS（默认 True，推荐）
        - True: 上传到阿里云 OSS（稳定预览，多实例部署友好）
        - False: 上传到本地 uploads/ 目录（兼容模式）
    
    注意：
    - OSS 上传需要在 .env 中配置 OSS_ACCESS_KEY_ID 等参数
    - 如果 OSS 未配置，会自动回退到本地存储
    """
    if not files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="please select the files to upload"
        )
    
    # 验证只能上传单个文件
    if len(files) > 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="only one file is supported, please select one file at a time"
        )
    
    file_processor = FileProcessor()
    oss_service = OSSService() if upload_to_oss else None
    uploaded_files = []
    failed_files = []
    
    # 检查 OSS 是否配置（如果启用 OSS）
    oss_enabled = False
    if upload_to_oss and oss_service and oss_service.is_configured():
        oss_enabled = True
        info(f"OSS 已启用，文件将上传到 OSS", source="api", user=current_user.username)
    elif upload_to_oss:
        warning(f"OSS 未配置，文件将上传到本地存储", source="api", user=current_user.username)
    
    for file in files:
        temp_file_path = None
        storage_type = 'local'
        oss_bucket = None
        oss_object_name = None
        
        try:
            if not file.filename:
                failed_files.append({"filename": "unknown file", "error": "file name is empty"})
                continue
            
            # 检查文件类型
            if not file_processor.is_supported_file(file.filename):
                failed_files.append({
                    "filename": file.filename, 
                    "error": f"不支持的文件类型: {os.path.splitext(file.filename)[1]}"
                })
                continue
            
            # 1. 先保存到临时文件
            file_id = str(uuid.uuid4())[:8]
            name, ext = os.path.splitext(file.filename)
            temp_filename = f"temp_{file_id}_{name}{ext}"
            temp_file_path = os.path.join(UPLOAD_DIR, temp_filename)
            
            content = await file.read()
            file_size = len(content)
            
            with open(temp_file_path, "wb") as buffer:
                buffer.write(content)
            
            # 获取文件信息
            file_info = file_processor.get_file_info(temp_file_path)
            file_type = safe_file_type_conversion(file_info["file_type"])
            
            # 2. 判断是否上传到 OSS
            final_file_path = temp_file_path  # 默认使用临时文件路径
            
            if oss_enabled:
                # 判断文件类别（用于 OSS 目录）
                file_category = oss_service.get_file_category(file.filename)
                
                # 上传到 OSS
                upload_result = oss_service.upload_file(
                    file_path=temp_file_path,
                    filename=file.filename,
                    username=current_user.username,
                    file_category=file_category,
                    expires=3600  # 签名 URL 1 小时有效
                )
                
                if upload_result["success"]:
                    # OSS 上传成功
                    storage_type = 'oss'
                    oss_bucket = upload_result["bucket"]
                    oss_object_name = upload_result["object_name"]
                    final_file_path = oss_object_name  # 存储 object_name
                    
                    # 删除临时文件
                    try:
                        os.remove(temp_file_path)
                        temp_file_path = None
                    except Exception as del_err:
                        warning(f"删除临时文件失败: {str(del_err)}")
                    
                    info(f"文件已上传到 OSS: {file.filename} -> {oss_object_name}", 
                         source="api", user=current_user.username)
                else:
                    # OSS 上传失败，回退到本地
                    warning(f"OSS 上传失败，使用本地存储: {upload_result.get('error', 'unknown error')}", 
                           source="api", user=current_user.username)
                    storage_type = 'local'
                    final_file_path = temp_file_path
            
            uploaded_files.append(FileUploadResponse(
                filename=file.filename,
                file_path=final_file_path,  # OSS: object_name, Local: /uploads/xxx
                file_size=file_size,
                file_type=file_type,
                upload_time=datetime.now()
            ))
            
            # 额外存储 storage_type 和 oss_bucket（用于数据库写入）
            uploaded_files[-1]._storage_type = storage_type  # type: ignore
            uploaded_files[-1]._oss_bucket = oss_bucket  # type: ignore
            
            info(f"文件上传成功: {file.filename} (storage: {storage_type})", 
                 source="api", user=current_user.username)
            
        except Exception as e:
            error(f"文件上传失败: {file.filename}, error: {str(e)}", 
                  source="api", user=current_user.username)
            
            # 清理临时文件
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except Exception:
                    pass
            
            failed_files.append({
                "filename": file.filename,
                "error": str(e)
            })
    
    # 如果有失败的文件，返回错误信息
    if failed_files and not uploaded_files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"all files upload failed: {[f['filename'] for f in failed_files]}"
        )
    
    # 为成功上传的文件创建数据库记录
    if uploaded_files:
        try:
            db_conn_params = get_database_config()
            db_manager = DatabaseManager(db_conn_params)
            conn = db_manager.get_connection()
            
            if conn:
                cursor = conn.cursor()
                
                # 创建processing_stats记录（状态为uploaded）
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
                    len(uploaded_files),  # files_processed = 上传的文件数
                    len(failed_files),    # files_failed = 失败的文件数
                    0,  # total_time
                    0,  # total_file_processing_time
                    0,  # total_ai_time
                    'File Upload Only',  # task_name
                    current_user.username,
                    0,  # total_tokens
                    0,  # total_price
                    json.dumps({"upload_only": True}),  # upload_params
                    '',  # final_report
                    'uploaded'  # rc_agent_status = 'uploaded' 表示仅上传
                ))
                
                processing_stats_id = cursor.fetchone()[0]
                
                # 为每个上传的文件创建file_processing_details记录
                for uploaded_file in uploaded_files:
                    # 获取文件大小（OSS 文件需要从 uploaded_file.file_size）
                    storage_type = getattr(uploaded_file, '_storage_type', 'local')
                    oss_bucket = getattr(uploaded_file, '_oss_bucket', None)
                    
                    if storage_type == 'local':
                        file_info = file_processor.get_file_info(uploaded_file.file_path)
                        file_size_mb = file_info['file_size_mb']
                    else:
                        # OSS 文件，从 uploaded_file.file_size 计算
                        file_size_mb = uploaded_file.file_size / (1024 * 1024)
                    
                    cursor.execute("""
                    INSERT INTO file_processing_details 
                    (processing_stats_id, filename, file_type, file_size_mb, 
                     processing_time, ai_time, content_length, status, 
                     total_tokens, cost, content, created_at, task_id, original_file_path,
                     file_storage_type, oss_bucket)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        processing_stats_id,
                        uploaded_file.filename,
                        uploaded_file.file_type.value,
                        file_size_mb,
                        0,  # processing_time
                        0,  # ai_time
                        0,  # content_length
                        'uploaded',  # status = 'uploaded' 表示仅上传
                        0,  # total_tokens
                        0,  # cost
                        '',  # content
                        datetime.now(),
                        None,  # task_id (仅上传没有task_id)
                        uploaded_file.file_path,  # original_file_path (OSS: object_name, Local: /uploads/xxx)
                        storage_type,  # 新增：存储类型
                        oss_bucket  # 新增：OSS bucket 名称
                    ))
                
                conn.commit()
                info(f"created {len(uploaded_files)} database records for files", source="api", user=current_user.username)
                conn.close()
        except Exception as e:
            error(f"failed to create database records: {str(e)}", source="api", user=current_user.username)
            # 数据库记录失败不影响文件上传，只记录错误
    
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
    current_user=Depends(get_current_user)
):
    """
    提交文件处理任务到队列（异步处理）
    
    - **file_paths**: 文件路径列表
    - **task_type**: RC Agent类型
    - **custom_inputs**: 自定义输入参数
    - **params**: 处理参数
    """
    if not request.file_paths:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="请选择要处理的文件"
        )
    
    # 验证只能处理单个文件
    if len(request.file_paths) > 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="当前版本只支持处理单个文件，请一次只选择一个文件"
        )
    
    # 验证文件存在（快速验证，保留在API层）
    file_processor = FileProcessor()
    valid_files, invalid_files = file_processor.validate_files(request.file_paths)
    
    if invalid_files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"存在无效文件: {[f['path'] for f in invalid_files]}"
        )
    
    # 获取数据库连接参数
    db_conn_params = get_database_config()
    
    # 根据分析模式进行不同的验证
    task_info = {}
    analysis_mode = request.analysis_mode
    
    if analysis_mode == 'local_llm':
        # 本地LLM模式：验证模板
        if not request.template_id and not request.template_code:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="使用本地LLM模式时，必须指定template_id或template_code"
            )
        
        # 获取模板名称
        template_name = None
        try:
            template_service = LLMPromptTemplateService(db_conn_params)
            if request.template_id:
                template = template_service.get_template(request.template_id)
                if template:
                    template_name = template.get('template_name')
            elif request.template_code:
                template = template_service.get_template_by_code(request.template_code)
                if template:
                    template_name = template.get('template_name')
        except Exception as e:
            warning(f"获取模板名称失败: {str(e)}", source="api", user=current_user.username)
        
        # 使用模板名称作为task_name，如果没有获取到则使用fallback
        task_name = template_name if template_name else (request.template_code or f'ID:{request.template_id}')
        
        # 构造task_info用于传递给worker
        task_info = {
            'name': task_name,
            'token': None,  # 本地LLM不需要token
            'agent_type': 'local_llm'
        }
        
        info(
            f"使用本地LLM模式: template_id={request.template_id}, "
            f"template_code={request.template_code}, template_name={template_name}",
            source="api",
            user=current_user.username
        )
    else:
        # 远程Agent模式：验证任务选择
        task_manager = TaskManager(current_user.username, current_user.is_admin, db_conn_params)
        task_manager.load_agent_credentials()
        
        is_valid, message, task_info = task_manager.validate_task_selection(
            request.task_type, 
            request.params.get('use_ai_agent', True)
        )
        
        if not is_valid:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=message
            )
    
    try:
        # 准备处理参数，添加分析模式相关参数
        processing_params = request.params.copy()
        processing_params['analysis_mode'] = analysis_mode
        
        if analysis_mode == 'local_llm':
            processing_params['template_id'] = request.template_id
            processing_params['template_code'] = request.template_code
            processing_params['template_variables'] = request.template_variables
            processing_params['llm_provider'] = request.llm_provider
            processing_params['llm_model'] = request.llm_model
        
        # 🚀 提交到Celery任务队列（立即返回）
        task = process_files_task.delay(
            file_paths=valid_files,
            task_info=task_info,
            custom_inputs=request.custom_inputs,
            params=processing_params,
            username=current_user.username
        )
        
        info(f"文件处理任务已提交到队列: {task.id}", source="api", user=current_user.username)
        
        return FileProcessResponse(
            task_id=task.id,  # 使用Celery任务ID
            message="文件处理任务已提交到队列，正在异步处理",
            files_count=len(valid_files)
        )
        
    except Exception as e:
        error(f"提交任务到队列失败: {str(e)}", source="api", user=current_user.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="任务提交失败，请稍后重试"
        )


@router.get("/tasks/{task_id}/status", summary="获取任务状态")
async def get_task_status(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    获取任务执行状态（从Redis读取）
    
    - **task_id**: Celery任务ID
    """
    try:
        # 从Redis获取任务状态
        task_result = AsyncResult(task_id, app=celery_app)
        
        if task_result.state == 'PENDING':
            return {
                "task_id": task_id,
                "status": "pending",
                "progress": 0,
                "message": "任务等待处理中...",
                "current_file": None,
                "files_processed": 0,
                "total_files": 0,
                "logs": []
            }
        elif task_result.state == 'PROGRESS':
            info = task_result.info or {}
            return {
                "task_id": task_id,
                "status": info.get("status", "processing"),
                "progress": info.get("progress", 0),
                "message": info.get("message", "正在处理..."),
                "current_file": info.get("current_file"),
                "files_processed": info.get("files_processed", 0),
                "files_failed": info.get("files_failed", 0),
                "total_files": info.get("total_files", 0),
                "logs": info.get("logs", [])
            }
        elif task_result.state == 'SUCCESS':
            result = task_result.result or {}
            return {
                "task_id": task_id,
                "status": "completed",
                "progress": 100,
                "message": "处理完成",
                "files_processed": result.get("files_processed", 0),
                "files_failed": result.get("files_failed", 0),
                "total_files": result.get("total_files", 0),
                "processing_time": result.get("processing_time", 0),
                "result": result,
                "logs": result.get("logs", [])
            }
        else:  # FAILURE or other states
            # 尝试获取失败信息，如果获取失败则使用默认信息
            try:
                info = task_result.info or {}
                if isinstance(info, Exception):
                    # 如果info本身是异常对象，转换为字典
                    info = {"error": str(info)}
                elif isinstance(info, str):
                    # 如果info是字符串，转换为字典
                    info = {"error": info}
            except Exception as info_error:
                # 如果无法获取info（比如异常反序列化失败），使用默认值
                info = {"error": f"任务失败，无法获取详细错误信息"}
            
            return {
                "task_id": task_id,
                "status": "failed",
                "progress": 0,
                "message": str(info.get("error", "处理失败")),
                "files_processed": info.get("files_processed", 0),
                "files_failed": info.get("files_failed", 0),
                "total_files": info.get("total_files", 0),
                "logs": info.get("logs", [])
            }
            
    except Exception as e:
        err_msg = str(e)
        # Celery 后端存储的失败任务若缺少 exc_type，解码会抛出此错误；返回 200 并标记为失败，避免前端一直 500
        if "Exception information must include the exception type" in err_msg or (isinstance(e, ValueError) and "exception type" in err_msg.lower()):
            error(f"任务结果无法解码（后端异常信息不完整）: {err_msg}", source="api", task_id=task_id)
            return {
                "task_id": task_id,
                "status": "failed",
                "progress": 0,
                "message": "任务执行失败（可能是上传到 OSS 或 LlamaParse 处理时出错），无法获取详细错误。请重试或联系管理员。",
                "files_processed": 0,
                "files_failed": 0,
                "total_files": 0,
                "logs": []
            }
        error(f"获取任务状态失败: {err_msg}", source="api", task_id=task_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="获取任务状态失败"
        )


@router.post("/tasks/{task_id}/cancel", summary="取消任务")
async def cancel_task(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    取消正在执行的任务
    
    - **task_id**: Celery任务ID
    """
    try:
        celery_app.control.revoke(task_id, terminate=True)
        info(f"任务 {task_id} 已被用户 {current_user.username} 取消", source="api")
        
        return {"message": f"任务 {task_id} 已被取消"}
        
    except Exception as e:
        error(f"取消任务失败: {str(e)}", source="api", task_id=task_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="取消任务失败"
        )


@router.get("/tasks/{task_id}/files", response_model=List[FileDetailResponse], summary="获取任务文件详情")
async def get_task_files(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    获取任务处理的文件详情
    
    - **task_id**: 任务ID (Celery任务ID)
    """
    try:
        # 从数据库获取文件处理详情
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="failed to connect to database"
            )
        
        cursor = conn.cursor()
        
        # 查询文件处理详情
        cursor.execute("""
        SELECT id, filename, file_type, file_size_mb, processing_time,
               ai_time, content_length, status, total_tokens, cost,
               content, created_at, original_file_path
        FROM file_processing_details 
        WHERE task_id = %s
        ORDER BY created_at DESC
        """, (task_id,))
        
        files = []
        for row in cursor.fetchall():
            files.append(FileDetailResponse(
                id=row[0],
                task_id=task_id,  # 添加缺失的task_id字段
                filename=row[1],
                file_type=safe_file_type_conversion(row[2]),
                file_size=row[3],  # 修正字段名：file_size_mb -> file_size
                processing_time=row[4],
                ai_time=row[5],
                content_length=row[6],
                status=row[7],
                total_tokens=row[8],
                cost=row[9],
                error_message=row[10],  # 修正字段名：content -> error_message
                created_at=row[11]
            ))
        
        conn.close()

        return files
        
    except Exception as e:
        error(f"failed to get task file details: {str(e)}", source="api", task_id=task_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="failed to get file details"
        )


@router.get("/list", response_model=List[FileDetailResponse], summary="获取文件列表")
async def get_files(
    skip: int = 0,
    limit: int = 100,
    file_type: Optional[str] = None,
    current_user=Depends(get_current_user)
):
    """
    获取文件处理历史列表（按用户权限过滤）
    
    - admin 可看全部
    - 非 admin 只能看自己上传的 + 被分享的
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="failed to connect to database"
            )
        
        cursor = conn.cursor()
        
        conditions = []
        params = []
        
        if file_type:
            conditions.append("fpd.file_type = %s")
            params.append(file_type)
        
        conditions.append("""(
            ps."user" = %s
            OR EXISTS (SELECT 1 FROM file_shares fs WHERE fs.file_id = fpd.id AND fs.shared_to_user_id = %s)
        )""")
        params.extend([current_user.username, current_user.user_id])
        access_type_expr = """CASE WHEN ps."user" = %s THEN 'owner' ELSE 'shared' END"""
        shared_by_expr = """(
            SELECT u.username FROM file_shares fs2
            JOIN users u ON u.id = fs2.shared_by_user_id
            WHERE fs2.file_id = fpd.id AND fs2.shared_to_user_id = %s
            ORDER BY fs2.created_at DESC LIMIT 1
        )"""
        
        where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        select_params = [current_user.username, current_user.user_id]
        
        query = f"""
            SELECT fpd.id, fpd.filename, fpd.file_type, fpd.file_size_mb, fpd.processing_time, 
                   fpd.ai_time, fpd.content_length, fpd.status, fpd.total_tokens, fpd.cost, 
                   fpd.content, fpd.created_at, fpd.original_file_path,
                   {access_type_expr} as access_type,
                   {shared_by_expr} as shared_by
            FROM file_processing_details fpd
            JOIN processing_stats ps ON fpd.processing_stats_id = ps.id
            {where_clause}
            ORDER BY fpd.created_at DESC
            LIMIT %s OFFSET %s
        """
        
        cursor.execute(query, select_params + params + [limit, skip])
        
        files = []
        for row in cursor.fetchall():
            files.append(FileDetailResponse(
                id=row[0],
                filename=row[1],
                file_type=safe_file_type_conversion(row[2]),
                file_size_mb=row[3],
                processing_time=row[4],
                ai_time=row[5],
                content_length=row[6],
                status=row[7],
                total_tokens=row[8],
                cost=row[9],
                content=row[10],
                created_at=row[11],
                original_file_path=row[12],
                access_type=row[13],
                shared_by=row[14]
            ))
        
        conn.close()

        return files
        
    except Exception as e:
        error(f"获取文件列表失败: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="获取文件列表失败"
        )


@router.get("/stats", summary="获取处理统计")
async def get_processing_stats(
    current_user=Depends(get_current_user)
):
    """
    获取文件处理统计信息
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="无法连接到数据库"
            )
        
        cursor = conn.cursor()
        
        # 查询统计信息
        cursor.execute("""
        SELECT 
                COUNT(*) as total_files,
                COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_files,
                COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_files,
                AVG(processing_time) as avg_processing_time,
                AVG(ai_time) as avg_ai_time,
                SUM(total_tokens) as total_tokens,
                SUM(cost) as total_cost
            FROM file_processing_details
        """)
        
        stats = cursor.fetchone()
        
        conn.close()
        
        return {
            "total_files": stats[0] or 0,
            "completed_files": stats[1] or 0,
            "failed_files": stats[2] or 0,
            "avg_processing_time": float(stats[3]) if stats[3] else 0,
            "avg_ai_time": float(stats[4]) if stats[4] else 0,
            "total_tokens": stats[5] or 0,
            "total_cost": float(stats[6]) if stats[6] else 0
        }
        
    except Exception as e:
        error(f"获取处理统计失败: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="获取统计信息失败"
        )


# 文件标签相关API
@router.post("/tags", response_model=FileTagResponse, summary="创建文件标签")
async def create_file_tag(
    tag: FileTagCreate,
    current_user=Depends(get_current_user)
):
    """
    为文件创建标签
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="无法连接到数据库"
            )
        
        cursor = conn.cursor()
        
        # 插入标签
        cursor.execute("""
            INSERT INTO file_tags (file_id, sector, industry, ticker, tagged_by)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, file_id, sector, industry, ticker, tagged_by, created_at
        """, (
            tag.file_id,
            tag.sector,
            tag.industry,
            tag.ticker,
            current_user.username
        ))
        
        result = cursor.fetchone()
        conn.commit()
        conn.close()
        
        return FileTagResponse(
            id=result[0],
            file_id=result[1],
            sector=result[2],
            industry=result[3],
            ticker=result[4],
            tagged_by=result[5],
            created_at=result[6]
        )
        
    except Exception as e:
        error(f"创建文件标签失败: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="创建标签失败"
        )


@router.get("/tags/stats", response_model=FileTagStats, summary="获取标签统计")
async def get_tag_stats(
    current_user=Depends(get_current_user)
):
    """
    获取文件标签统计信息
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="无法连接到数据库"
            )
        
        cursor = conn.cursor()
        
        # 查询标签统计
        cursor.execute("""
            SELECT 
                COUNT(*) as total_tags,
                COUNT(DISTINCT sector) as unique_sectors,
                COUNT(DISTINCT industry) as unique_industries,
                COUNT(DISTINCT ticker) as unique_tickers
            FROM file_tags
        """)
        
        stats = cursor.fetchone()
        
        conn.close()
        
        return FileTagStats(
            total_tags=stats[0] or 0,
            unique_sectors=stats[1] or 0,
            unique_industries=stats[2] or 0,
            unique_tickers=stats[3] or 0
        )
        
    except Exception as e:
        error(f"获取标签统计失败: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="获取标签统计失败"
        )


@router.get("/tags/files", response_model=List[FileWithTag], summary="获取带标签的文件")
async def get_files_with_tags(
    sector: Optional[str] = None,
    industry: Optional[str] = None,
    ticker: Optional[str] = None,
    current_user=Depends(get_current_user)
):
    """
    获取带标签的文件列表
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="无法连接到数据库"
            )
        
        cursor = conn.cursor()
        
        # 构建查询条件
        where_clause = "WHERE 1=1"
        params = []
        
        if sector:
            where_clause += " AND ft.sector = %s"
            params.append(sector)
        if industry:
            where_clause += " AND ft.industry = %s"
            params.append(industry)
        if ticker:
            where_clause += " AND ft.ticker = %s"
            params.append(ticker)
        
        # 查询带标签的文件
        query = f"""
            SELECT 
                fpd.id, fpd.filename, fpd.file_type, fpd.file_size_mb,
                fpd.processing_time, fpd.ai_time, fpd.content_length,
                fpd.status, fpd.total_tokens, fpd.cost, fpd.created_at,
                ft.sector, ft.industry, ft.ticker, ft.tagged_by
            FROM file_processing_details fpd
            JOIN file_tags ft ON fpd.id = ft.file_id
            {where_clause}
            ORDER BY fpd.created_at DESC
        """
        
        cursor.execute(query, params)
        
        files = []
        for row in cursor.fetchall():
            files.append(FileWithTag(
                id=row[0],
                filename=row[1],
                file_type=safe_file_type_conversion(row[2]),
                file_size_mb=row[3],
                processing_time=row[4],
                ai_time=row[5],
                content_length=row[6],
                status=row[7],
                total_tokens=row[8],
                cost=row[9],
                created_at=row[10],
                sector=row[11],
                industry=row[12],
                ticker=row[13],
                tagged_by=row[14]
            ))
        
        conn.close()
        
        return files
        
    except Exception as e:
        error(f"获取带标签文件失败: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="获取带标签文件失败"
        )


@router.get("/processing-params", summary="获取处理参数选项")
async def get_processing_params():
    """获取文件处理的参数选项和默认值"""
    return {
        "language_options": [
            {"value": ["ch_sim", "en"], "label": "中文简体和英文"},
            {"value": ["ch_sim"], "label": "仅中文简体"},
            {"value": ["en"], "label": "仅英文"},
            {"value": ["ch_tra"], "label": "仅中文繁体"}
        ],
        "default_language": ["ch_sim", "en"],
        "fast_mode_options": [
            {"value": True, "label": "快速模式"},
            {"value": False, "label": "标准模式"}
        ],
        "default_fast_mode": False,
        "max_pages_options": [
            {"value": 5, "label": "5页"},
            {"value": 10, "label": "10页"},
            {"value": 20, "label": "20页"},
            {"value": 50, "label": "50页"},
            {"value": 100, "label": "100页"}
        ],
        "default_max_pages": 10
    }


@router.delete("/uploads/{filename}", summary="删除上传的文件")
async def delete_uploaded_file(
    filename: str,
    current_user=Depends(get_current_user)
):
    """
    删除上传的文件
    
    - **filename**: 文件名
    """
    try:
        file_path = os.path.join(UPLOAD_DIR, filename)
        
        if os.path.exists(file_path):
            os.remove(file_path)
            info(f"文件删除成功: {filename}", source="api", user=current_user.username)
            return {"message": f"文件 {filename} 已删除"}
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="文件不存在"
            )
                
    except Exception as e:
        error(f"删除文件失败: {str(e)}", source="api", user=current_user.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="删除文件失败"
        )


@router.get("/original/{file_id}", summary="获取原始文件信息（支持 OSS）")
async def get_original_file_info(
    file_id: str,
    current_user=Depends(get_current_user)
):
    """
    获取原始文件信息（支持 OSS 和本地存储）
    
    - **file_id**: 文件ID
    
    返回：
    - OSS 文件：返回带签名的临时 URL（1小时有效）
    - 本地文件：返回 /serve/{file_id} 端点
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="无法连接到数据库"
            )
        
        cursor = conn.cursor()
        
        access = _check_file_access(cursor, int(file_id), current_user.user_id, current_user.username, current_user.is_admin)
        if not access:
            conn.close()
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No access to this file")
        
        cursor.execute("""
            SELECT filename, file_type, file_size_mb, original_file_path, created_at, 
                   content, asr_with_diarization, file_storage_type, oss_bucket
            FROM file_processing_details 
            WHERE id = %s
        """, (file_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            filename = result[0]
            file_type = result[1]
            file_size_mb = result[2]
            original_file_path = result[3]
            created_at = result[4]
            content = result[5]
            asr_with_diarization = result[6]
            storage_type = result[7] or 'local'  # 兼容旧数据
            oss_bucket = result[8]
            
            # 根据存储类型生成 file_url
            if storage_type == 'oss':
                # OSS 文件：根据配置生成 URL
                oss_service = OSSService()
                if oss_service.is_configured():
                    # 如果配置了自定义域名，使用公共 URL（可以预览）
                    if oss_service.custom_domain:
                        from urllib.parse import quote
                        encoded_object_name = quote(original_file_path, safe='/')
                        # 注意：如果域名未配置 SSL 证书，使用 HTTP；否则使用 HTTPS
                        file_url = f"http://{oss_service.custom_domain}/{encoded_object_name}"
                        info(f"使用 OSS 自定义域名: {oss_service.custom_domain}", source="api")
                    else:
                        # 否则使用签名 URL（默认域名会强制下载）
                        file_url = oss_service.get_signed_url(
                            object_name=original_file_path,  # original_file_path 存的是 object_name
                            expires=3600  # 1 小时
                        )
                        if not file_url:
                            # 签名 URL 生成失败，回退到 serve 接口
                            warning(f"OSS 签名 URL 生成失败，使用 serve 接口: file_id={file_id}")
                            file_url = f"/api/files/serve/{file_id}"
                else:
                    # OSS 未配置，使用 serve 接口
                    warning(f"OSS 未配置，使用 serve 接口: file_id={file_id}")
                    file_url = f"/api/files/serve/{file_id}"
            else:
                # 本地文件：使用 serve 接口
                import os
                if os.getenv('ENV', 'development') == 'development':
                    # 本地开发：直接指向后端端口8000
                    file_url = f"http://localhost:8000/api/files/serve/{file_id}"
                else:
                    # 生产环境：使用相对路径
                    file_url = f"/api/files/serve/{file_id}"
            
            # 解析 asr_with_diarization JSON 数据
            asr_data = None
            if asr_with_diarization:
                try:
                    if isinstance(asr_with_diarization, str):
                        asr_data = json.loads(asr_with_diarization)
                    else:
                        asr_data = asr_with_diarization  # 已经是dict（JSONB类型）
                except Exception as e:
                    warning(f"解析 asr_with_diarization 数据失败: {str(e)}", source="api", file_id=file_id)
            
            return {
                "file_id": file_id,
                "filename": filename,
                "file_type": file_type,
                "file_size_mb": file_size_mb,
                "original_file_path": original_file_path,
                "file_url": file_url,  # 返回HTTP可访问的URL（OSS: 签名URL, Local: /serve）
                "storage_type": storage_type,  # 新增：存储类型
                "oss_bucket": oss_bucket,  # 新增：OSS bucket
                "created_at": created_at,
                "content": content,  # 转写文本内容
                "asr_with_diarization": asr_data  # 结构化的ASR数据（说话人分离）
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="文件不存在"
        )
        
    except Exception as e:
        error(f"获取文件信息失败: {str(e)}", source="api", file_id=file_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="获取文件信息失败"
        )


@router.patch("/details/{file_id}/content", summary="更新文件处理详情中的 content（Markdown）")
async def update_file_detail_content(
    file_id: str,
    content: str = Body(..., embed=True),
    current_user: dict = Depends(get_current_user)
):
    """
    更新 file_processing_details 表中的 content 字段（用于编辑 Content 标签页的 Markdown 内容）。
    仅允许该文件所属任务执行记录的用户（或管理员）更新。
    """
    try:
        db_manager = DatabaseManager(get_database_config())
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="无法连接到数据库"
            )
        cursor = conn.cursor()
        # 获取文件所属的 processing_stats_id，并校验归属
        cursor.execute("""
            SELECT fpd.id, fpd.processing_stats_id
            FROM file_processing_details fpd
            WHERE fpd.id = %s
        """, (file_id,))
        row = cursor.fetchone()
        if not row:
            conn.close()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="文件不存在"
            )
        processing_stats_id = row[1]
        cursor.execute("""
            SELECT "user" FROM processing_stats WHERE id = %s
        """, (processing_stats_id,))
        owner = cursor.fetchone()
        if not owner:
            conn.close()
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="任务执行记录不存在"
            )
        if owner[0] != current_user.username:
            conn.close()
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="权限不足，只能编辑自己的文件内容"
            )
        cursor.execute("""
            UPDATE file_processing_details
            SET content = %s
            WHERE id = %s
        """, (content, file_id))
        conn.commit()
        conn.close()
        info(f"更新文件 content 成功: file_id={file_id}", source="api", user=current_user.username)
        return {"success": True, "message": "Content updated"}
    except HTTPException:
        raise
    except Exception as e:
        error(f"更新文件 content 失败: {str(e)}", source="api", file_id=file_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="更新内容失败"
        )


@router.patch("/{file_id}/speaker/{speaker_id}", summary="更新说话人名称")
async def update_speaker_label(
    file_id: str,
    speaker_id: int,
    new_label: str,
    current_user: dict = Depends(get_current_user)
):
    """
    更新音频文件中指定说话人的标签名称
    
    - **file_id**: 文件ID
    - **speaker_id**: 说话人ID
    - **new_label**: 新的标签名称
    """
    try:
        db_manager = DatabaseManager(get_database_config())
        
        # 1. 获取当前的 asr_with_diarization 数据
        query = """
            SELECT asr_with_diarization 
            FROM file_processing_details 
            WHERE id = %s
        """
        result = db_manager.fetch_one(query, (file_id,))
        
        if not result or not result[0]:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="文件不存在或无ASR数据"
            )
        
        # 2. 解析 JSON 数据
        asr_data = result[0]
        if isinstance(asr_data, str):
            asr_data = json.loads(asr_data)
        
        # 3. 更新 speakers 列表中的 label
        speaker_found = False
        if 'speakers' in asr_data:
            for speaker in asr_data['speakers']:
                if speaker.get('speaker_id') == speaker_id:
                    speaker['label'] = new_label
                    speaker_found = True
                    break
        
        if not speaker_found:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"说话人 ID {speaker_id} 不存在"
            )
        
        # 4. 更新数据库
        update_query = """
            UPDATE file_processing_details 
            SET asr_with_diarization = %s
            WHERE id = %s
        """
        success = db_manager.execute_query(
            update_query,
            (json.dumps(asr_data), file_id)
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="数据库更新失败"
            )
        
        info(f"更新说话人标签成功: file_id={file_id}, speaker_id={speaker_id}, new_label={new_label}", source="api")
        
        return {
            "success": True,
            "message": "说话人标签更新成功",
            "speaker_id": speaker_id,
            "new_label": new_label
        }
        
    except HTTPException:
        raise
    except Exception as e:
        error(f"更新说话人标签失败: {str(e)}", source="api", file_id=file_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新说话人标签失败: {str(e)}"
        )


@router.get("/serve/{file_id}", summary="提供文件访问（支持 OSS 重定向）")
async def serve_file(
    file_id: str,
    token: str = None
):
    """
    提供文件访问（支持 OSS 和本地文件）
    
    - **file_id**: 文件ID
    - **token**: 可选，认证token（query参数）
    
    行为：
    - OSS 文件：重定向到 OSS 签名 URL
    - 本地文件：直接返回文件流
    """
    # 验证token
    from backend.auth.jwt_utils import verify_token
    
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="需要认证token。请在URL中添加 ?token=YOUR_TOKEN"
        )
    
    try:
        payload = verify_token(token)
        if not payload:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="无效的token"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"token验证失败: {str(e)}"
        )
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        
        conn = db_manager.get_connection()
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="无法连接到数据库"
            )
        
        cursor = conn.cursor()
        
        access = _check_file_access(
            cursor, int(file_id),
            payload.user_id, payload.username, payload.is_admin
        )
        if not access:
            conn.close()
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No access to this file")
        
        cursor.execute("""
            SELECT filename, file_type, original_file_path, file_storage_type, oss_bucket
            FROM file_processing_details 
            WHERE id = %s
        """, (file_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="文件不存在"
            )
        
        filename, file_type, file_path, storage_type, oss_bucket = result
        storage_type = storage_type or 'local'  # 兼容旧数据
        
        # 如果是 OSS 文件，重定向到 OSS URL
        if storage_type == 'oss':
            oss_service = OSSService()
            if oss_service.is_configured():
                # 如果配置了自定义域名，使用公共 URL
                if oss_service.custom_domain:
                    from urllib.parse import quote
                    from fastapi.responses import RedirectResponse
                    encoded_object_name = quote(file_path, safe='/')
                    redirect_url = f"https://{oss_service.custom_domain}/{encoded_object_name}"
                    info(f"重定向到 OSS 自定义域名: {oss_service.custom_domain}", source="api")
                    return RedirectResponse(url=redirect_url)
                else:
                    # 否则使用签名 URL
                    signed_url = oss_service.get_signed_url(
                        object_name=file_path,  # file_path 存的是 object_name
                        expires=3600  # 1 小时
                    )
                    if signed_url:
                        # 重定向到 OSS 签名 URL
                        from fastapi.responses import RedirectResponse
                        return RedirectResponse(url=signed_url)
                    else:
                        error(f"OSS 签名 URL 生成失败: file_id={file_id}")
                        raise HTTPException(
                            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="OSS 文件访问失败"
                        )
            else:
                error(f"OSS 未配置，但文件在 OSS: file_id={file_id}")
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="OSS 服务未配置"
                )
        
        # 本地文件：检查文件是否存在
        if not file_path or not os.path.exists(file_path):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="文件未找到"
            )
        
        # 根据文件类型设置MIME type（支持非音频文件预览：pdf/doc/docx/excel/xlsx/xls/csv 等）
        mime_types = {
            'pdf': 'application/pdf',
            'excel': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'xls': 'application/vnd.ms-excel',
            'csv': 'text/csv',  # CSV 使用 text/csv
            'doc': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'word': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'image': 'image/jpeg',
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'png': 'image/png',
            'gif': 'image/gif',
            'bmp': 'image/bmp'
        }
        
        media_type = mime_types.get(file_type.lower(), 'application/octet-stream')
        
        # 对文件名进行URL编码以支持中文等非ASCII字符
        # 使用RFC 2231标准的filename*参数
        from urllib.parse import quote
        encoded_filename = quote(filename)
        
        # 使用FileResponse直接返回文件
        # 对于PDF和图片，使用inline模式让浏览器直接显示
        # 对于其他文件类型，使用attachment模式让浏览器下载
        if file_type.lower() in ['pdf', 'jpg', 'jpeg', 'png', 'gif', 'bmp', 'image']:
            # inline模式 - 浏览器直接显示
            return FileResponse(
                path=file_path,
                media_type=media_type,
                headers={
                    "Content-Disposition": f"inline; filename*=UTF-8''{encoded_filename}"
                }
            )
        else:
            # attachment模式 - 浏览器下载
            return FileResponse(
                path=file_path,
                media_type=media_type,
                headers={
                    "Content-Disposition": f"attachment; filename*=UTF-8''{encoded_filename}"
                }
            )
        
    except HTTPException:
        raise
    except Exception as e:
        error(f"提供文件失败: {str(e)}", source="api", file_id=file_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="提供文件失败"
        )


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
        raise HTTPException(status_code=500, detail=f"获取行业分类失败: {str(e)}")


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
        
        # 添加排序逻辑
        order_by_clause = "ORDER BY name"
        if search:
            order_by_clause = f"""
            ORDER BY 
                CASE 
                    WHEN UPPER(code) = UPPER(%s) THEN 1
                    WHEN UPPER(name) = UPPER(%s) THEN 2
                    WHEN UPPER(code) LIKE UPPER(%s) THEN 3
                    WHEN UPPER(name) LIKE UPPER(%s) THEN 4
                    ELSE 5
                END,
                name
            """
            params.extend([search.upper(), search.upper(), f"%{search}%", f"%{search}%"])
        
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
        {order_by_clause}
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


# ===========================================
# 音频文件上传 API（用于 ASR 语音识别）
# ===========================================

@router.post("/upload-audio", summary="上传音频文件并异步处理")
async def upload_audio(
    file: UploadFile = File(...),
    current_user=Depends(get_current_user)
):
    """
    上传音频文件到阿里云 OSS 并异步进行语音识别
    
    ⚠️ 重要：此端点使用异步 Celery 任务处理，不会阻塞主线程
    - 适合处理大文件（200MB+）
    - 立即返回任务 ID，不等待上传和识别完成
    - 前端通过轮询或 WebSocket 获取进度
    
    支持的音频格式：
    - aac, amr, avi, flac, flv, m4a, mkv, mov
    - mp3, mp4, mpeg, ogg, opus, wav, webm, wma, wmv
    
    文件要求：
    - 最大文件大小：2GB
    - 最长时长：12小时
    
    返回：
    - task_id: Celery 任务 ID（用于查询进度）
    - filename: 文件名
    - file_size: 文件大小（字节）
    """
    try:
        # 检查文件名
        if not file.filename:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="文件名为空"
            )
        
        # 初始化 OSS 服务（只做基本检查）
        oss_service = OSSService()
        
        # 检查 OSS 是否配置
        if not oss_service.is_configured():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="OSS 服务未配置。请在 .env 文件中配置 OSS_ACCESS_KEY_ID 和 OSS_ACCESS_KEY_SECRET"
            )
        
        # 检查文件格式
        if not oss_service.is_audio_file(file.filename):
            ext = os.path.splitext(file.filename)[1]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"不支持的音频格式: {ext}。支持的格式: {', '.join(sorted(OSSService.SUPPORTED_AUDIO_FORMATS))}"
            )
        
        # 使用流式写入临时文件，避免一次性加载整个文件到内存
        temp_file_id = str(uuid.uuid4())[:8]
        name, ext = os.path.splitext(file.filename)
        temp_filename = f"{name}_{temp_file_id}{ext}"
        temp_file_path = os.path.join(UPLOAD_DIR, temp_filename)
        
        # 流式写入文件（分块读取，不占用大量内存）
        file_size = 0
        chunk_size = 1024 * 1024  # 1MB chunks
        
        with open(temp_file_path, "wb") as buffer:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                buffer.write(chunk)
                file_size += len(chunk)
        
        # 验证文件大小
        if file_size > OSSService.MAX_FILE_SIZE:
            # 删除临时文件
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            
            size_gb = file_size / (1024 * 1024 * 1024)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"音频文件过大: {size_gb:.2f}GB。最大支持 2GB"
            )
        
        info(f"音频文件已保存到临时目录: {file.filename} ({file_size / (1024 * 1024):.2f} MB)", 
             source="api", user=current_user.username)
        
        # 提交异步任务到 Celery
        task = upload_and_transcribe_audio_task.apply_async(
            args=[temp_file_path, file.filename, current_user.username],
            kwargs={
                'model': 'fun-asr',
                'language_hints': ['zh', 'en'],
                'diarization_enabled': True
            },
            queue='file_processing'
        )
        
        info(f"音频上传任务已提交: task_id={task.id}, file={file.filename}", 
             source="api", user=current_user.username)
        
        # 立即返回任务 ID，不等待完成
        return {
            "success": True,
            "message": "音频文件已接收，正在后台处理",
            "task_id": task.id,
            "filename": file.filename,
            "file_size": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2),
            "status": "processing",
            "note": "请使用 task_id 查询处理进度（GET /api/files/tasks/{task_id}/status）"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        error(f"上传音频文件失败: {str(e)}", source="api", user=current_user.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"上传音频文件失败: {str(e)}"
        )


@router.post("/upload-and-process-document", summary="上传文档并异步处理（推荐，避免 OSS 慢连接阻塞）")
async def upload_and_process_document(
    file: UploadFile = File(...),
    analysis_mode: str = Form('local_llm'),
    task_type: str = Form(None),
    template_id: Optional[int] = Form(None),
    template_variables: str = Form('{}'),  # JSON string
    custom_inputs: str = Form('{}'),  # JSON string
    params: str = Form('{}'),  # JSON string (sector, industry, ticker, etc.)
    current_user=Depends(get_current_user)
):
    """
    上传文档文件到 OSS 并异步处理（LlamaParse + AI 分析）
    
    ⚠️ 推荐使用此端点：OSS 服务器在香港，直接上传会阻塞请求
    - 使用 Celery 异步任务，立即返回 task_id
    - 后台上传到 OSS + 可选 AI 分析
    - 不阻塞主线程
    
    支持的文档格式：
    - PDF, Word (doc/docx), Excel (xls/xlsx), PowerPoint (ppt/pptx)
    - Text (txt, md), Images (jpg, png, gif, bmp, webp)
    
    参数：
    - file: 文件（必需）
    - analysis_mode: 'local_llm' 或 'remote_agent'（可选，默认 local_llm）
    - task_type: remote_agent 时的任务类型
    - template_id: local_llm 时的模板 ID
    - template_variables: 模板变量（JSON 字符串）
    - custom_inputs: 自定义输入（JSON 字符串）
    - params: 处理参数，包含 sector/industry/ticker 等（JSON 字符串）
    
    返回：
    - task_id: Celery 任务 ID（用于查询进度）
    - filename: 文件名
    - file_size: 文件大小（字节）
    """
    try:
        # 检查文件名
        if not file.filename:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="文件名为空"
            )
        
        # 检查 OSS 配置
        oss_service = OSSService()
        if not oss_service.is_configured():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="OSS 服务未配置。请在 .env 文件中配置 OSS_ACCESS_KEY_ID 和 OSS_ACCESS_KEY_SECRET"
            )
        
        # 检查文件格式
        file_processor = FileProcessor()
        if not file_processor.is_supported_file(file.filename):
            ext = os.path.splitext(file.filename)[1]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"不支持的文件类型: {ext}"
            )
        
        # 解析 JSON 参数
        try:
            template_vars = json.loads(template_variables) if template_variables else {}
            custom_inputs_dict = json.loads(custom_inputs) if custom_inputs else {}
            params_dict = json.loads(params) if params else {}
        except json.JSONDecodeError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"JSON 参数解析失败: {str(e)}"
            )
        
        # 流式写入临时文件
        temp_file_id = str(uuid.uuid4())[:8]
        name, ext = os.path.splitext(file.filename)
        temp_filename = f"{name}_{temp_file_id}{ext}"
        temp_file_path = os.path.join(UPLOAD_DIR, temp_filename)
        
        file_size = 0
        chunk_size = 1024 * 1024  # 1MB chunks
        
        with open(temp_file_path, "wb") as buffer:
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                buffer.write(chunk)
                file_size += len(chunk)
        
        info(f"文档文件已保存到临时目录: {file.filename} ({file_size / (1024 * 1024):.2f} MB)", 
             source="api", user=current_user.username)
        
        # 组装任务信息
        task_info = {
            'analysis_mode': analysis_mode,
            'task_type': task_type or 'local_llm_template',
            'template_id': template_id,
            'template_variables': template_vars
        }
        
        # 提交异步任务到 Celery
        task = upload_and_process_document_task.apply_async(
            args=[temp_file_path, file.filename, current_user.username],
            kwargs={
                'task_info': task_info,
                'custom_inputs': custom_inputs_dict,
                'params': params_dict
            },
            queue='file_processing'
        )
        
        info(f"文档上传和处理任务已提交: task_id={task.id}, file={file.filename}", 
             source="api", user=current_user.username)
        
        # 立即返回任务 ID
        return {
            "success": True,
            "message": "文档文件已接收，正在后台上传并处理",
            "task_id": task.id,
            "filename": file.filename,
            "file_size": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2),
            "status": "processing",
            "note": "请使用 task_id 查询处理进度（GET /api/files/tasks/{task_id}/status）"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        error(f"上传文档文件失败: {str(e)}", source="api", user=current_user.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"上传文档文件失败: {str(e)}"
        )


@router.get("/audio/supported-formats", summary="获取支持的音频格式")
async def get_supported_audio_formats():
    """
    获取支持的音频格式列表（用于 Fun-ASR）
    """
    return {
        "supported_formats": sorted(list(OSSService.SUPPORTED_AUDIO_FORMATS)),
        "max_file_size_gb": 2,
        "max_duration_hours": 12,
        "description": "支持的音频格式（Fun-ASR 兼容）"
    }


@router.post("/audio/transcribe", summary="语音识别 - Fun-ASR")
async def transcribe_audio(
    request: Dict[str, Any],
    current_user=Depends(get_current_user)
):
    """
    调用 Fun-ASR 进行语音识别
    
    请求体:
    {
        "audio_url": "https://...",  # 必填：音频文件的公网 URL
        "model": "fun-asr",           # 可选：模型名称，默认 fun-asr
        "language_hints": ["zh", "en"],  # 可选：语言提示
        "wait_for_result": true,      # 可选：是否等待结果，默认 true
        "diarization_enabled": true   # 可选：是否启用说话人分离，默认 true
    }
    
    返回:
    {
        "success": true,
        "task_id": "xxx",
        "status": "COMPLETED",
        "transcriptions": [...],  # 详细转写结果（包含说话人信息）
        "text": "转写文本",       # 合并的文本
        "formatted_text": "格式化的文本"  # 包含时间戳和说话人的格式化文本
    }
    """
    try:
        # 获取请求参数
        audio_url = request.get('audio_url')
        model = request.get('model', 'fun-asr')
        language_hints = request.get('language_hints', ['zh', 'en'])
        wait_for_result = request.get('wait_for_result', True)
        diarization_enabled = request.get('diarization_enabled', True)
        
        # 验证必填参数
        if not audio_url:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="缺少必填参数: audio_url"
            )
        
        info(f"开始语音识别: audio_url={audio_url}, model={model}", 
             source="api", user=current_user.username)
        
        # 初始化 ASR 服务
        asr_service = ASRService()
        
        # 检查 ASR 服务是否配置
        if not asr_service.is_configured():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="ASR 服务未配置。请设置 DASHSCOPE_API_KEY 环境变量并安装 dashscope SDK (pip install dashscope)"
            )
        
        # 调用语音识别
        result = asr_service.transcribe_audio(
            audio_url=audio_url,
            model=model,
            language_hints=language_hints,
            wait_for_result=wait_for_result,
            diarization_enabled=diarization_enabled
        )
        
        if not result["success"]:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"语音识别失败: {result.get('error', 'Unknown error')}"
            )
        
        # 格式化结果：API 返回用完整格式，写入 DB 用仅带时间戳的格式
        formatted_texts = []
        content_for_db_parts = []
        if result.get("transcriptions"):
            for trans_data in result["transcriptions"]:
                formatted = asr_service.format_transcription_result(
                    trans_data,
                    include_timestamps=True,
                    include_words=False,
                    include_speaker=diarization_enabled
                )
                formatted_texts.append(formatted)
                part = asr_service.format_transcription_result(
                    trans_data,
                    include_timestamps=True,
                    include_words=False,
                    include_speaker=diarization_enabled,
                    timestamped_only=True
                )
                if part:
                    content_for_db_parts.append(part)
        
        final_report_text = "\n\n".join(formatted_texts) if formatted_texts else result.get("text", "")
        content_for_db = "\n".join(content_for_db_parts) if content_for_db_parts else result.get("text", "")
        
        # 构建结构化的 ASR 数据（用于前端可视化）
        asr_structured_data = None
        if result.get("transcriptions") and diarization_enabled:
            try:
                asr_structured_data = _build_asr_structured_data(result["transcriptions"])
            except Exception as e:
                warning(f"构建结构化 ASR 数据失败: {str(e)}", source="api", user=current_user.username)
        
        # 记录到数据库（可选）
        try:
            db_conn_params = get_database_config()
            db_manager = DatabaseManager(db_conn_params)
            conn = db_manager.get_connection()
            
            if conn:
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
                    1 if result["success"] else 0,
                    0 if result["success"] else 1,
                    0,
                    0,
                    0,
                    'ASR Transcription',
                    current_user.username,
                    0,
                    0,
                    json.dumps({
                        "audio_url": audio_url,
                        "model": model,
                        "language_hints": language_hints,
                        "diarization_enabled": diarization_enabled
                    }),
                    '',  # final_report 保留为空，等待未来的 AI Agent 填充总结内容
                    'completed' if result["success"] else 'failed'
                ))
                
                processing_stats_id = cursor.fetchone()[0]
                
                # 从 audio_url 中提取原始文件名（处理 URL 编码和查询参数）
                original_filename = _extract_filename_from_audio_url(audio_url)
                
                # 创建 file_processing_details 记录
                cursor.execute("""
                INSERT INTO file_processing_details 
                (processing_stats_id, filename, file_type, file_size_mb, 
                 processing_time, ai_time, content_length, status, 
                 total_tokens, cost, content, created_at, task_id, original_file_path, asr_with_diarization)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    processing_stats_id,
                    original_filename,  # 使用提取的原始文件名，而不是 URL
                    'audio',
                    0,
                    0,
                    0,
                    len(content_for_db),
                    'completed' if result["success"] else 'failed',
                    0,
                    0,
                    content_for_db,  # 仅保存带时间戳的转写结果，每段一行
                    datetime.now(),
                    result.get("task_id"),
                    audio_url,
                    json.dumps(asr_structured_data) if asr_structured_data else None
                ))
                
                conn.commit()
                conn.close()
                
                info(f"ASR 结果已记录到数据库", source="api", user=current_user.username)
        except Exception as db_error:
            warning(f"记录 ASR 结果到数据库失败: {str(db_error)}", source="api", user=current_user.username)
        
        # 返回结果
        response_data = {
            "success": result["success"],
            "task_id": result["task_id"],
            "status": result["status"],
            "text": result.get("text", ""),
            "formatted_text": "\n\n".join(formatted_texts) if formatted_texts else "",
            "transcriptions": result.get("transcriptions", [])
        }
        
        info(f"语音识别完成: task_id={result['task_id']}, text_length={len(result.get('text', ''))}", 
             source="api", user=current_user.username)
        
        return response_data
        
    except HTTPException:
        raise
    except Exception as e:
        error(f"语音识别失败: {str(e)}", source="api", user=current_user.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"语音识别失败: {str(e)}"
        )


@router.post("/audio/transcribe-async", summary="提交语音识别任务（异步）")
async def transcribe_audio_async(
    request: Dict[str, Any],
    current_user=Depends(get_current_user)
):
    """
    提交语音识别任务（异步，不等待结果）
    
    请求体:
    {
        "audio_url": "https://...",
        "model": "fun-asr",
        "language_hints": ["zh", "en"],
        "diarization_enabled": true  # 可选：是否启用说话人分离，默认 true
    }
    
    返回:
    {
        "success": true,
        "task_id": "xxx",
        "message": "任务已提交"
    }
    """
    try:
        audio_url = request.get('audio_url')
        model = request.get('model', 'fun-asr')
        language_hints = request.get('language_hints', ['zh', 'en'])
        diarization_enabled = request.get('diarization_enabled', True)
        
        if not audio_url:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="缺少必填参数: audio_url"
            )
        
        # 初始化 ASR 服务
        asr_service = ASRService()
        
        if not asr_service.is_configured():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="ASR 服务未配置"
            )
        
        # 提交任务（不等待结果）
        result = asr_service.submit_transcription_task(
            audio_url=audio_url,
            model=model,
            language_hints=language_hints,
            diarization_enabled=diarization_enabled
        )
        
        if not result["success"]:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"提交任务失败: {result.get('error')}"
            )
        
        return {
            "success": True,
            "task_id": result["task_id"],
            "message": "语音识别任务已提交，请使用 task_id 查询结果"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"提交任务失败: {str(e)}"
        )


@router.get("/audio/transcribe/{task_id}", summary="查询语音识别结果")
async def get_transcription_result(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    查询语音识别任务结果
    
    返回:
    {
        "success": true,
        "task_id": "xxx",
        "status": "COMPLETED",
        "transcriptions": [...],
        "text": "转写文本"
    }
    """
    try:
        # 初始化 ASR 服务
        asr_service = ASRService()
        
        if not asr_service.is_configured():
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="ASR 服务未配置"
            )
        
        # 获取结果
        result = asr_service.get_transcription_results(task_id)
        
        if not result["success"]:
            # 如果任务还在进行中，返回状态
            if result["status"] in ["PENDING", "RUNNING"]:
                return {
                    "success": True,
                    "task_id": task_id,
                    "status": result["status"],
                    "message": "任务处理中，请稍后查询"
                }
            else:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"获取结果失败: {result.get('error')}"
                )
        
        # 格式化结果（包含说话人信息）
        formatted_texts = []
        if result.get("transcriptions"):
            for trans_data in result["transcriptions"]:
                formatted = asr_service.format_transcription_result(
                    trans_data,
                    include_timestamps=True,
                    include_words=False,
                    include_speaker=True  # 总是包含说话人信息（如果有）
                )
                formatted_texts.append(formatted)
        
        # 提取文本
        texts = []
        for trans in result.get("transcriptions", []):
            for transcript in trans.get('transcripts', []):
                text = transcript.get('text', '')
                if text:
                    texts.append(text)
        
        return {
            "success": True,
            "task_id": task_id,
            "status": result["status"],
            "text": "\n".join(texts),
            "formatted_text": "\n\n".join(formatted_texts),
            "transcriptions": result.get("transcriptions", [])
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"查询结果失败: {str(e)}"
        )


@router.post("/audio/analyze", summary="音频AI分析 - 分析ASR转写内容")
async def analyze_audio(
    request: Dict[str, Any],
    current_user=Depends(get_current_user)
):
    """
    对已完成语音识别的音频文件进行AI分析。
    
    从 file_processing_details.asr_with_diarization 读取ASR数据，
    使用指定的 Prompt Template（默认 audio_analysis_v1）进行分析，
    分析结果保存到 processing_stats.final_report。
    
    请求体:
    {
        "execution_id": 123,           # 必填：processing_stats.id
        "template_id": 32,             # 可选：模板ID（默认使用 audio_analysis_v1）
        "template_variables": {},       # 可选：额外模板变量
    }
    
    返回:
    {
        "success": true,
        "task_id": "celery-task-id",
        "message": "音频分析任务已提交"
    }
    """
    try:
        execution_id = request.get('execution_id')
        template_id = request.get('template_id')
        template_variables = request.get('template_variables', {})
        
        if not execution_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="缺少必填参数: execution_id"
            )
        
        # 获取数据库连接
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
            # 验证 execution 存在且属于当前用户
            cursor.execute("""
                SELECT id, "user", rc_agent_status, task_name
                FROM processing_stats
                WHERE id = %s
            """, (execution_id,))
            ps_row = cursor.fetchone()
            
            if not ps_row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"执行记录不存在: {execution_id}"
                )
            
            if ps_row[1] != current_user.username:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="权限不足，只能分析自己的记录"
                )
            
            if ps_row[2] == 'analyzing':
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="该记录正在分析中，请等待完成"
                )
            
            # 获取对应的 file_processing_details（音频文件）
            cursor.execute("""
                SELECT id, filename, file_type, asr_with_diarization, content
                FROM file_processing_details
                WHERE processing_stats_id = %s
                ORDER BY id ASC
                LIMIT 1
            """, (execution_id,))
            file_row = cursor.fetchone()
            
            if not file_row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="找不到对应的文件记录"
                )
            
            file_detail_id = file_row[0]
            file_type = file_row[2]
            asr_data = file_row[3]
            content = file_row[4]
            
            # 验证有 ASR 数据或转写内容
            if not asr_data and not content:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="该文件没有ASR数据或转写内容，请先进行语音识别"
                )
            
            # 如果未指定模板，使用默认的 audio_analysis_v1
            if not template_id:
                template_service = LLMPromptTemplateService()
                template = template_service.get_template_by_code('audio_analysis_v1')
                if template:
                    template_id = template['id']
                else:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="默认音频分析模板 audio_analysis_v1 不存在，请联系管理员创建"
                    )
            
            # 更新状态为 analyzing
            cursor.execute("""
                UPDATE processing_stats
                SET rc_agent_status = 'analyzing'
                WHERE id = %s
            """, (execution_id,))
            conn.commit()
            
        finally:
            conn.close()
        
        # 提交 Celery 任务
        task = analyze_audio_task.delay(
            execution_id=execution_id,
            file_detail_id=file_detail_id,
            template_id=template_id,
            template_variables=template_variables,
            username=current_user.username
        )
        
        info(
            f"音频AI分析任务已提交: task_id={task.id}, execution_id={execution_id}",
            source="api", user=current_user.username
        )
        
        return {
            "success": True,
            "task_id": task.id,
            "execution_id": execution_id,
            "message": "音频AI分析任务已提交，正在异步处理"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        error(f"提交音频分析任务失败: {str(e)}", source="api", user=current_user.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"提交音频分析任务失败: {str(e)}"
        )


@router.get("/audio/analyze/{task_id}/status", summary="查询音频AI分析任务状态")
async def get_audio_analysis_status(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    查询音频AI分析的Celery任务状态
    """
    try:
        task_result = AsyncResult(task_id, app=celery_app)
        
        if task_result.state == 'PENDING':
            return {
                "task_id": task_id,
                "status": "pending",
                "progress": 0,
                "message": "任务等待处理中..."
            }
        elif task_result.state == 'PROGRESS':
            task_info = task_result.info or {}
            return {
                "task_id": task_id,
                "status": task_info.get("status", "processing"),
                "progress": task_info.get("progress", 0),
                "message": task_info.get("message", "正在分析..."),
                "execution_id": task_info.get("execution_id"),
                "logs": task_info.get("logs", [])
            }
        elif task_result.state == 'SUCCESS':
            result = task_result.result or {}
            return {
                "task_id": task_id,
                "status": "completed",
                "progress": 100,
                "message": "分析完成",
                "execution_id": result.get("execution_id"),
                "total_tokens": result.get("total_tokens", 0),
                "total_price": result.get("total_price", 0),
                "llm_model": result.get("llm_model"),
                "llm_provider": result.get("llm_provider"),
                "logs": result.get("logs", [])
            }
        else:
            task_info = task_result.info or {}
            return {
                "task_id": task_id,
                "status": "failed",
                "progress": 0,
                "message": str(task_info.get("error", "分析失败")),
                "execution_id": task_info.get("execution_id"),
                "logs": task_info.get("logs", [])
            }
    
    except Exception as e:
        error(f"获取音频分析状态失败: {str(e)}", source="api", task_id=task_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="获取分析状态失败"
        )


@router.post("/document/analyze", summary="文档AI分析 - 对已上传文档进行AI分析")
async def analyze_document(
    request: Dict[str, Any],
    current_user=Depends(get_current_user)
):
    """
    对已上传但未经AI分析的文档进行AI分析。
    
    如果文档未经LlamaParse解析（content为空），会先自动解析，再进行AI分析。
    分析结果保存到 processing_stats.final_report。
    
    请求体:
    {
        "execution_id": 123,           # 必填：processing_stats.id
        "template_id": 32,             # 必填：模板ID
        "template_variables": {},       # 可选：额外模板变量
    }
    """
    try:
        execution_id = request.get('execution_id')
        template_id = request.get('template_id')
        template_variables = request.get('template_variables', {})
        
        if not execution_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="缺少必填参数: execution_id"
            )
        
        if not template_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="缺少必填参数: template_id（文档分析需要选择模板）"
            )
        
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
            # 验证 execution 存在且属于当前用户
            cursor.execute("""
                SELECT id, "user", rc_agent_status, task_name
                FROM processing_stats
                WHERE id = %s
            """, (execution_id,))
            ps_row = cursor.fetchone()
            
            if not ps_row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"执行记录不存在: {execution_id}"
                )
            
            if ps_row[1] != current_user.username:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="权限不足，只能分析自己的记录"
                )
            
            if ps_row[2] == 'analyzing':
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="该记录正在分析中，请等待完成"
                )
            
            # 获取对应的 file_processing_details（文档文件）
            cursor.execute("""
                SELECT id, filename, file_type, content, original_file_path
                FROM file_processing_details
                WHERE processing_stats_id = %s
                ORDER BY id ASC
                LIMIT 1
            """, (execution_id,))
            file_row = cursor.fetchone()
            
            if not file_row:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="找不到对应的文件记录"
                )
            
            file_detail_id = file_row[0]
            content = file_row[3]
            original_file_path = file_row[4]
            
            # 验证有内容或有原始文件可以解析
            if not content and not original_file_path:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="该文件没有内容且没有原始文件路径，无法进行分析"
                )
            
            # 更新状态为 analyzing
            cursor.execute("""
                UPDATE processing_stats
                SET rc_agent_status = 'analyzing'
                WHERE id = %s
            """, (execution_id,))
            conn.commit()
            
        finally:
            conn.close()
        
        # 提交 Celery 任务
        task = analyze_document_task.delay(
            execution_id=execution_id,
            file_detail_id=file_detail_id,
            template_id=template_id,
            template_variables=template_variables,
            username=current_user.username
        )
        
        info(
            f"文档AI分析任务已提交: task_id={task.id}, execution_id={execution_id}, template_id={template_id}",
            source="api", user=current_user.username
        )
        
        return {
            "success": True,
            "task_id": task.id,
            "execution_id": execution_id,
            "message": "文档AI分析任务已提交，正在异步处理"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        error(f"提交文档分析任务失败: {str(e)}", source="api", user=current_user.username)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"提交文档分析任务失败: {str(e)}"
        )


@router.get("/document/analyze/{task_id}/status", summary="查询文档AI分析任务状态")
async def get_document_analysis_status(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    查询文档AI分析的Celery任务状态
    """
    try:
        task_result = AsyncResult(task_id, app=celery_app)
        
        if task_result.state == 'PENDING':
            return {
                "task_id": task_id,
                "status": "pending",
                "progress": 0,
                "message": "任务等待处理中..."
            }
        elif task_result.state == 'PROGRESS':
            task_info = task_result.info or {}
            return {
                "task_id": task_id,
                "status": task_info.get("status", "processing"),
                "progress": task_info.get("progress", 0),
                "message": task_info.get("message", "正在分析..."),
                "execution_id": task_info.get("execution_id"),
                "logs": task_info.get("logs", [])
            }
        elif task_result.state == 'SUCCESS':
            result = task_result.result or {}
            return {
                "task_id": task_id,
                "status": "completed",
                "progress": 100,
                "message": "文档分析完成",
                "execution_id": result.get("execution_id"),
                "total_tokens": result.get("total_tokens", 0),
                "total_price": result.get("total_price", 0),
                "llm_model": result.get("llm_model"),
                "llm_provider": result.get("llm_provider"),
                "logs": result.get("logs", [])
            }
        else:
            task_info = task_result.info or {}
            return {
                "task_id": task_id,
                "status": "failed",
                "progress": 0,
                "message": str(task_info.get("error", "文档分析失败")),
                "execution_id": task_info.get("execution_id"),
                "logs": task_info.get("logs", [])
            }
    
    except Exception as e:
        error(f"获取文档分析状态失败: {str(e)}", source="api", task_id=task_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="获取文档分析状态失败"
        )


@router.post("/document/reparse", summary="重新解析文档 - Re-parse Document")
async def reparse_document(
    request: Dict[str, Any],
    current_user=Depends(get_current_user)
):
    """
    重新解析文档（Re-parse）
    
    用于文档解析失败或内容为空时，重新调用 LlamaParse 解析文档。
    解析成功后自动触发打标签任务。
    
    请求参数:
    - file_id: file_processing_details.id（必填）
    - params: LlamaParse 配置参数（可选，包含 language、fast_mode、max_pages 等）
    
    返回:
    - task_id: Celery 任务 ID
    - file_id: 文件 ID
    - message: 提示消息
    """
    from backend.tasks.file_tasks import reparse_document_task
    
    try:
        file_id = request.get('file_id')
        if not file_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="缺少 file_id 参数"
            )
        
        params = request.get('params', {})
        username = current_user.username if current_user else None
        
        # 验证文件是否存在
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="数据库连接失败"
            )
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, filename, status
            FROM file_processing_details
            WHERE id = %s
        """, (file_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"文件不存在: file_id={file_id}"
            )
        
        filename = row[1]
        current_status = row[2]
        
        info(f"提交重新解析任务: file_id={file_id}, filename={filename}, user={username}", 
             source="api")
        
        # 提交 Celery 任务
        task = reparse_document_task.apply_async(
            args=(file_id, params, username),
            countdown=1
        )
        
        info(f"重新解析任务已提交: task_id={task.id}, file_id={file_id}", source="api")
        
        return {
            "success": True,
            "task_id": task.id,
            "file_id": file_id,
            "filename": filename,
            "message": f"已提交重新解析任务: {filename}",
            "note": f"请使用 task_id 查询处理进度（GET /api/files/document/reparse/{task.id}/status）"
        }
    
    except HTTPException:
        raise
    except Exception as e:
        error(f"提交重新解析任务失败: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"提交重新解析任务失败: {str(e)}"
        )


@router.get("/document/reparse/{task_id}/status", summary="查询文档重新解析任务状态")
async def get_reparse_status(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    查询文档重新解析的 Celery 任务状态
    """
    try:
        task_result = AsyncResult(task_id, app=celery_app)
        
        if task_result.state == 'PENDING':
            return {
                "task_id": task_id,
                "status": "pending",
                "progress": 0,
                "message": "任务等待处理中..."
            }
        elif task_result.state == 'PROGRESS':
            task_info = task_result.info or {}
            return {
                "task_id": task_id,
                "status": task_info.get("status", "processing"),
                "progress": task_info.get("progress", 0),
                "message": task_info.get("message", "正在重新解析..."),
                "file_id": task_info.get("file_id"),
                "logs": task_info.get("logs", [])
            }
        elif task_result.state == 'SUCCESS':
            result = task_result.result or {}
            return {
                "task_id": task_id,
                "status": "completed",
                "progress": 100,
                "message": "重新解析完成",
                "file_id": result.get("file_id"),
                "content_length": result.get("content_length", 0),
                "processing_time": result.get("processing_time", 0),
                "logs": result.get("logs", [])
            }
        else:
            task_info = task_result.info or {}
            return {
                "task_id": task_id,
                "status": "failed",
                "progress": 0,
                "message": str(task_info.get("error", "重新解析失败")),
                "file_id": task_info.get("file_id"),
                "logs": task_info.get("logs", [])
            }
    
    except Exception as e:
        error(f"获取重新解析状态失败: {str(e)}", source="api", task_id=task_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="获取重新解析状态失败"
        )


@router.post("/upload-youtube", summary="上传YouTube链接")
async def upload_youtube_link(
    request: Dict[str, Any] = Body(...),
    current_user = Depends(get_current_user)
):
    """
    上传 YouTube 链接并异步处理
    
    请求参数:
    - youtube_url: YouTube 视频链接（必填）
    
    返回:
    - task_id: Celery 任务 ID
    - video_id: YouTube 视频 ID
    - message: 提示消息
    """
    from backend.services.youtube_service import YouTubeService
    from backend.tasks.file_tasks import upload_and_process_youtube_task
    
    try:
        youtube_url = request.get('youtube_url')
        if not youtube_url:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="缺少 youtube_url 参数"
            )
        
        username = current_user.username if hasattr(current_user, 'username') else 'guest'
        
        # 验证 YouTube URL 格式
        video_id = YouTubeService.extract_video_id(youtube_url)
        if not video_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid YouTube URL. Please provide a valid YouTube video link."
            )
        
        info(f"接收到 YouTube 链接: url={youtube_url}, video_id={video_id}, user={username}", 
             source="api")
        
        # 启动异步任务
        task = upload_and_process_youtube_task.apply_async(
            args=(youtube_url, username),
            queue='file_processing'
        )
        
        info(f"YouTube 处理任务已启动: task_id={task.id}, video_id={video_id}", 
             source="api")
        
        return {
            "task_id": task.id,
            "video_id": video_id,
            "message": "YouTube processing started. You can track the progress in the task status."
        }
        
    except HTTPException:
        raise
    except Exception as e:
        error(f"YouTube 上传失败: {str(e)}", source="api")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process YouTube link: {str(e)}"
        )

class FileASRRequest(BaseModel):
    max_duration_seconds: int = 0  # 0 = 完整视频；>0 = 只转写前 N 秒


@router.post("/{file_id}/transcribe-asr", summary="对 YouTube 文件触发 Fun-ASR 转写")
async def trigger_file_youtube_asr(
    file_id: int,
    body: FileASRRequest = FileASRRequest(),
    current_user=Depends(get_current_user)
):
    """
    对 Upload YouTube Links 保存的视频（无字幕），触发 Fun-ASR 转写任务。
    - max_duration_seconds: 只转写前 N 秒（0 = 完整视频）
    - 任务进度可通过 GET /api/files/tasks/{task_id}/status 查询。
    """
    try:
        db_conn_params = get_database_config()
        db_manager = DatabaseManager(db_conn_params)
        conn = db_manager.get_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT id, file_type, original_file_path FROM file_processing_details WHERE id = %s",
            (file_id,)
        )
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail="File not found")

        _, file_type, youtube_url = row
        if file_type != 'youtube':
            raise HTTPException(
                status_code=400,
                detail="Only YouTube files can be transcribed via ASR"
            )

        from backend.services.youtube_service import YouTubeService
        video_id = YouTubeService.extract_video_id(youtube_url) or f"file{file_id}"

        from backend.tasks.file_tasks import transcribe_youtube_file_asr_task
        task = transcribe_youtube_file_asr_task.apply_async(
            args=(file_id, youtube_url, video_id, max(0, body.max_duration_seconds)),
            queue='file_processing'
        )

        info(f"YouTube File ASR task dispatched: file_id={file_id}, video_id={video_id}, task_id={task.id}",
             source="api")

        return {
            "task_id": task.id,
            "file_id": file_id,
            "video_id": video_id,
            "message": "ASR transcription started. Use task_id to poll progress."
        }

    except HTTPException:
        raise
    except Exception as e:
        error(f"触发 YouTube File ASR 失败: {str(e)}", source="api")
        raise HTTPException(status_code=500, detail=str(e))


# ===========================================
# 文件分享 API
# ===========================================

def _check_file_access(cursor, file_id: int, user_id: int, username: str, is_admin: bool) -> str:
    """
    检查用户对文件的访问权限。
    返回 'owner' | 'shared' | None
    """
    cursor.execute("""
        SELECT ps."user"
        FROM file_processing_details fpd
        JOIN processing_stats ps ON fpd.processing_stats_id = ps.id
        WHERE fpd.id = %s
    """, (file_id,))
    row = cursor.fetchone()
    if not row:
        return None
    if row[0] == username:
        return 'owner'
    cursor.execute(
        "SELECT 1 FROM file_shares WHERE file_id = %s AND shared_to_user_id = %s",
        (file_id, user_id)
    )
    if cursor.fetchone():
        return 'shared'
    return None


@router.post("/{file_id}/share", summary="分享文件给指定用户")
async def share_file(
    file_id: int,
    body: Dict[str, Any] = Body(...),
    current_user=Depends(get_current_user)
):
    """
    分享文件给指定用户（支持再分享）

    Body: { "user_ids": [2, 5] }
    """
    user_ids = body.get("user_ids", [])
    if not user_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="user_ids is required")

    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor()
    try:
        access = _check_file_access(cursor, file_id, current_user.user_id, current_user.username, current_user.is_admin)
        if not access:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No access to this file")

        shared_count = 0
        skipped = []
        for uid in user_ids:
            if uid == current_user.user_id:
                skipped.append({"user_id": uid, "reason": "cannot share to yourself"})
                continue
            cursor.execute("SELECT id, username FROM users WHERE id = %s", (uid,))
            target = cursor.fetchone()
            if not target:
                skipped.append({"user_id": uid, "reason": "user not found"})
                continue
            cursor.execute("""
                INSERT INTO file_shares (file_id, shared_by_user_id, shared_to_user_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (file_id, shared_to_user_id) DO NOTHING
            """, (file_id, current_user.user_id, uid))
            if cursor.rowcount > 0:
                shared_count += 1

        conn.commit()
        info(f"File {file_id} shared to {shared_count} users by {current_user.username}", source="api")
        return {"shared_count": shared_count, "skipped": skipped}

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        error(f"Share file failed: {str(e)}", source="api")
        raise HTTPException(status_code=500, detail=f"Share failed: {str(e)}")
    finally:
        conn.close()


@router.delete("/{file_id}/share", summary="取消文件分享")
async def unshare_file(
    file_id: int,
    body: Dict[str, Any] = Body(...),
    current_user=Depends(get_current_user)
):
    """
    取消文件分享（仅 owner 或 admin）

    Body: { "user_ids": [5] }
    """
    user_ids = body.get("user_ids", [])
    if not user_ids:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="user_ids is required")

    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor()
    try:
        access = _check_file_access(cursor, file_id, current_user.user_id, current_user.username, current_user.is_admin)
        if access != 'owner':
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Only file owner can revoke sharing"
            )

        removed = 0
        for uid in user_ids:
            cursor.execute(
                "DELETE FROM file_shares WHERE file_id = %s AND shared_to_user_id = %s",
                (file_id, uid)
            )
            removed += cursor.rowcount

        conn.commit()
        info(f"File {file_id}: removed {removed} shares by {current_user.username}", source="api")
        return {"removed_count": removed}

    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        error(f"Unshare file failed: {str(e)}", source="api")
        raise HTTPException(status_code=500, detail=f"Unshare failed: {str(e)}")
    finally:
        conn.close()


@router.get("/{file_id}/shares", summary="查看文件的分享列表")
async def get_file_shares(
    file_id: int,
    current_user=Depends(get_current_user)
):
    """
    查看某文件当前的所有分享记录
    """
    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor()
    try:
        access = _check_file_access(cursor, file_id, current_user.user_id, current_user.username, current_user.is_admin)
        if not access:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No access to this file")

        cursor.execute("""
            SELECT fs.id, fs.file_id,
                   fs.shared_by_user_id, u_by.username as shared_by_username,
                   fs.shared_to_user_id, u_to.username as shared_to_username,
                   fs.created_at
            FROM file_shares fs
            JOIN users u_by ON u_by.id = fs.shared_by_user_id
            JOIN users u_to ON u_to.id = fs.shared_to_user_id
            WHERE fs.file_id = %s
            ORDER BY fs.created_at DESC
        """, (file_id,))

        shares = []
        for row in cursor.fetchall():
            shares.append({
                "id": row[0],
                "file_id": row[1],
                "shared_by_user_id": row[2],
                "shared_by_username": row[3],
                "shared_to_user_id": row[4],
                "shared_to_username": row[5],
                "created_at": row[6].isoformat() if row[6] else None
            })

        return {"file_id": file_id, "shares": shares}

    except HTTPException:
        raise
    except Exception as e:
        error(f"Get file shares failed: {str(e)}", source="api")
        raise HTTPException(status_code=500, detail=f"Get shares failed: {str(e)}")
    finally:
        conn.close()
