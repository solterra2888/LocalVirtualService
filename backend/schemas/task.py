# -*- coding: utf-8 -*-
"""
任务相关数据模型
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

from backend.utils.datetime_utils import UtcDateTime


class TaskExecutionResponse(BaseModel):
    """任务执行响应模型"""
    id: str
    username: str
    task_name: str
    created_at: UtcDateTime
    files_processed: int
    files_failed: int
    total_time: float
    file_processing_time: float
    ai_processing_time: float
    total_tokens: Optional[int] = None
    total_cost: Optional[float] = None
    rc_agent_status: Optional[str] = None
    first_filename: Optional[str] = None
    first_file_type: Optional[str] = None
    llm_model: Optional[str] = None
    llm_provider: Optional[str] = None
    first_task_id: Optional[str] = None
    upload_params: Optional[dict] = None
    access_type: Optional[str] = None
    shared_by: Optional[str] = None
    share_info: Optional[dict] = None
    ticker_tags: Optional[List[str]] = Field(
        None,
        description="该 execution 下所有文件 file_tags.ticker 合并去重后的代码列表（与移动端卡片一致）",
    )
    folder_id: Optional[int] = Field(None, description="文件夹 ID（首个文件的 folder_id）")
    folder_name: Optional[str] = Field(None, description="文件夹名称")
    first_display_name: Optional[str] = Field(None, description="AI 自动生成的文件显示名称")
    
    class Config:
        from_attributes = True


class TaskLogResponse(BaseModel):
    """任务日志响应模型"""
    timestamp: UtcDateTime
    level: str
    message: str


class TaskStatusResponse(BaseModel):
    """任务状态响应模型"""
    task_id: str
    status: str
    progress: float
    current_file: Optional[str] = None
    logs: List[TaskLogResponse] = [] 