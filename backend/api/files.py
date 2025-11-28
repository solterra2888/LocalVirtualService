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
from fastapi import APIRouter, File, UploadFile, HTTPException, status, Depends

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

# Celery相关导入
from celery.result import AsyncResult
from backend.celery_app import app as celery_app
from backend.tasks.file_tasks import process_files_task

# 统一的环境变量加载
from backend.utils.env_loader import load_project_env

router = APIRouter()

# 上传文件目录
UPLOAD_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)


def safe_file_type_conversion(file_type_str: str) -> FileType:
    """
    安全地将字符串转换为FileType枚举
    """
    try:
        return FileType(file_type_str)
    except ValueError:
        return FileType.OTHER


@router.post("/upload", response_model=List[FileUploadResponse], summary="上传文件")
async def upload_files(
    files: List[UploadFile] = File(...),
    current_user=Depends(get_current_user)
):
    """
    上传文件到服务器
    
    - **files**: 要上传的文件列表
    """
    if not files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="请选择要上传的文件"
        )
    
    file_processor = FileProcessor()
    uploaded_files = []
    failed_files = []
    
    for file in files:
        try:
            if not file.filename:
                failed_files.append({"filename": "未知文件", "error": "文件名为空"})
                continue
            
            # 检查文件类型
            if not file_processor.is_supported_file(file.filename):
                failed_files.append({
                    "filename": file.filename, 
                    "error": f"不支持的文件类型: {os.path.splitext(file.filename)[1]}"
                })
                continue
            
            # 生成唯一文件名
            file_id = str(uuid.uuid4())[:8]  # 只取前8位
            # 分离文件名和扩展名
            name, ext = os.path.splitext(file.filename)
            filename = f"{name}_{file_id}{ext}"
            file_path = os.path.join(UPLOAD_DIR, filename)
            
            # 保存文件
            content = await file.read()
            with open(file_path, "wb") as buffer:
                buffer.write(content)
            
            # 获取文件信息
            file_info = file_processor.get_file_info(file_path)
            file_size = len(content)
            file_type = safe_file_type_conversion(file_info["file_type"])
            
            uploaded_files.append(FileUploadResponse(
                filename=file.filename,
                file_path=file_path,
                file_size=file_size,
                file_type=file_type,
                upload_time=datetime.now()
            ))
            
            info(f"文件上传成功: {file.filename} -> {file_path}", source="api", user=current_user.username)
            
        except Exception as e:
            error(f"文件上传失败: {file.filename}, 错误: {str(e)}", source="api", user=current_user.username)
            failed_files.append({
                "filename": file.filename,
                "error": str(e)
            })
    
    # 如果有失败的文件，返回错误信息
    if failed_files and not uploaded_files:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"所有文件上传失败: {[f['filename'] for f in failed_files]}"
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
            info = task_result.info or {}
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
        error(f"获取任务状态失败: {str(e)}", source="api", task_id=task_id)
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
                detail="无法连接到数据库"
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
        error(f"获取任务文件详情失败: {str(e)}", source="api", task_id=task_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="获取文件详情失败"
        )


@router.get("/list", response_model=List[FileDetailResponse], summary="获取文件列表")
async def get_files(
    skip: int = 0,
    limit: int = 100,
    file_type: Optional[str] = None,
    current_user=Depends(get_current_user)
):
    """
    获取文件处理历史列表
    
    - **skip**: 跳过记录数
    - **limit**: 限制返回记录数
    - **file_type**: 文件类型过滤
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
        
        if file_type:
            where_clause += " AND file_type = %s"
            params.append(file_type)
        
        # 查询文件列表
        query = f"""
            SELECT id, filename, file_type, file_size_mb, processing_time, 
                   ai_time, content_length, status, total_tokens, cost, 
                   content, created_at, original_file_path
            FROM file_processing_details 
            {where_clause}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """
        params.extend([limit, skip])
        
        cursor.execute(query, params)
        
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
                original_file_path=row[12]
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


@router.get("/original/{file_id}", summary="获取原始文件信息")
async def get_original_file_info(
    file_id: str,
    current_user=Depends(get_current_user)
):
    """
    获取原始文件信息
    
    - **file_id**: 文件ID
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
        
        # 查询文件信息
        cursor.execute("""
            SELECT filename, file_type, file_size_mb, original_file_path, created_at
            FROM file_processing_details 
            WHERE id = %s
        """, (file_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                "file_id": file_id,
                "filename": result[0],
                "file_type": result[1],
                "file_size_mb": result[2],
                "original_file_path": result[3],
                "created_at": result[4]
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