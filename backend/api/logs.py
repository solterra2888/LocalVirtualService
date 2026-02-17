# -*- coding: utf-8 -*-
"""
系统日志API路由
"""

import os
from typing import Optional, Literal
from fastapi import APIRouter, HTTPException, Depends, Query
from pydantic import BaseModel

from backend.auth.jwt_utils import get_current_admin_user
from backend.schemas.user import TokenData

router = APIRouter()


class LogResponse(BaseModel):
    """日志响应模型"""
    filename: str
    content: str
    lines: int
    size_kb: float


LOG_FILES = {
    "app": "logs/app.log",
    "celery_worker": "logs/celery_worker.log",
    "celery_worker_data_fetching": "logs/celery_worker_data_fetching.log",
}


def get_log_file_path(log_type: str) -> str:
    """获取日志文件的绝对路径"""
    if log_type not in LOG_FILES:
        raise HTTPException(status_code=400, detail=f"不支持的日志类型: {log_type}")
    
    # 获取项目根目录
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    log_path = os.path.join(project_root, LOG_FILES[log_type])
    
    if not os.path.exists(log_path):
        raise HTTPException(status_code=404, detail=f"日志文件不存在: {LOG_FILES[log_type]}")
    
    return log_path


@router.get("/api/logs/list", summary="获取可用日志列表")
async def list_logs(current_user: TokenData = Depends(get_current_admin_user)):
    """
    获取可用的日志文件列表（需要管理员权限）
    """
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    available_logs = []
    for log_type, log_file in LOG_FILES.items():
        log_path = os.path.join(project_root, log_file)
        if os.path.exists(log_path):
            size_bytes = os.path.getsize(log_path)
            size_kb = size_bytes / 1024
            available_logs.append({
                "type": log_type,
                "filename": log_file,
                "size_kb": round(size_kb, 2),
                "exists": True
            })
        else:
            available_logs.append({
                "type": log_type,
                "filename": log_file,
                "size_kb": 0,
                "exists": False
            })
    
    return {"logs": available_logs}


@router.get("/api/logs/{log_type}", response_model=LogResponse, summary="读取日志内容")
async def read_log(
    log_type: Literal["app", "celery_worker", "celery_worker_data_fetching"],
    lines: Optional[int] = Query(None, description="读取最后N行，不提供则读取全部"),
    current_user: TokenData = Depends(get_current_admin_user)
):
    """
    读取指定日志文件的内容（需要管理员权限）
    
    - **log_type**: 日志类型 (app, celery_worker 或 celery_worker_data_fetching)
    - **lines**: 可选，读取最后N行
    """
    log_path = get_log_file_path(log_type)
    
    try:
        # 获取文件大小
        size_bytes = os.path.getsize(log_path)
        size_kb = size_bytes / 1024
        
        # 读取文件内容
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            if lines and lines > 0:
                # 读取最后N行
                all_lines = f.readlines()
                content_lines = all_lines[-lines:]
                content = ''.join(content_lines)
                total_lines = len(all_lines)
            else:
                # 读取全部内容
                content = f.read()
                total_lines = content.count('\n') + 1
        
        return LogResponse(
            filename=LOG_FILES[log_type],
            content=content,
            lines=total_lines,
            size_kb=round(size_kb, 2)
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取日志文件失败: {str(e)}")


@router.delete("/api/logs/{log_type}/clear", summary="清空日志文件")
async def clear_log(
    log_type: Literal["app", "celery_worker", "celery_worker_data_fetching"],
    current_user: TokenData = Depends(get_current_admin_user)
):
    """
    清空指定的日志文件（需要管理员权限）
    
    - **log_type**: 日志类型 (app, celery_worker 或 celery_worker_data_fetching)
    """
    log_path = get_log_file_path(log_type)
    
    try:
        # 清空文件内容但保留文件
        with open(log_path, 'w', encoding='utf-8') as f:
            f.write("")
        
        return {
            "message": f"日志文件 {LOG_FILES[log_type]} 已清空",
            "filename": LOG_FILES[log_type]
        }
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"清空日志文件失败: {str(e)}")
