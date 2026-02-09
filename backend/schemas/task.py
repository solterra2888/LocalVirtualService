# -*- coding: utf-8 -*-
"""
任务相关数据模型
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


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
    created_at: Optional[datetime] = None
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
    created_at: datetime
    files_processed: int
    files_failed: int
    total_time: float
    file_processing_time: float
    ai_processing_time: float
    total_tokens: Optional[int] = None
    total_cost: Optional[float] = None
    rc_agent_status: Optional[str] = None
    first_filename: Optional[str] = None  # 第一个文件的名称
    first_file_type: Optional[str] = None  # 第一个文件的类型（用于列表筛选）
    llm_model: Optional[str] = None  # LLM模型名称
    llm_provider: Optional[str] = None  # LLM提供商
    first_task_id: Optional[str] = None  # 第一个文件的task_id（用于判断是否正在处理）
    
    class Config:
        from_attributes = True


class TaskLogResponse(BaseModel):
    """任务日志响应模型"""
    timestamp: datetime
    level: str
    message: str


class TaskStatusResponse(BaseModel):
    """任务状态响应模型"""
    task_id: str
    status: str
    progress: float
    current_file: Optional[str] = None
    logs: List[TaskLogResponse] = [] 