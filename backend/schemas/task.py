# -*- coding: utf-8 -*-
"""
任务相关数据模型
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

from backend.utils.datetime_utils import UtcDateTime


class TaskTypeCreateRequest(BaseModel):
    """RC Agent创建请求模型"""
    name: str = Field(..., min_length=1, max_length=100, description="任务名称")
    token: str = Field(..., min_length=1, description="RC Agent令牌")
    remarks: Optional[str] = Field(None, max_length=500, description="备注")
    input_fields: List[Dict[str, Any]] = Field(default_factory=list, description="自定义输入字段")
    agent_type: str = Field(default="chatbot", description="Agent类型", pattern="^(chatbot|workflow)$")


class TaskTypeUpdateRequest(BaseModel):
    """RC Agent更新请求模型"""
    name: Optional[str] = Field(None, min_length=1, max_length=100, description="任务名称")
    token: Optional[str] = Field(None, min_length=1, description="RC Agent令牌")
    remarks: Optional[str] = Field(None, max_length=500, description="备注")
    input_fields: Optional[List[Dict[str, Any]]] = Field(None, description="自定义输入字段")
    agent_type: Optional[str] = Field(None, description="Agent类型", pattern="^(chatbot|workflow)$")


class TaskTypeResponse(BaseModel):
    """RC Agent响应模型"""
    id: str
    name: str
    token: str
    creator: str
    created_at: Optional[UtcDateTime] = None
    remarks: Optional[str] = None
    input_fields: List[Dict[str, Any]]
    is_default: bool = False
    agent_type: str = "chatbot"
    
    class Config:
        from_attributes = True


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