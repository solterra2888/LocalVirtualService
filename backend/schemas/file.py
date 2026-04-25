# -*- coding: utf-8 -*-
"""
文件相关数据模型
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

from backend.utils.datetime_utils import UtcDateTime


class FileType(str, Enum):
    """文件类型枚举"""
    PDF = "pdf"
    EXCEL = "excel"
    CSV = "csv"  # CSV 单独分类（不同于 Excel）
    IMAGE = "image"
    DOC = "doc"
    WEB = "web"
    OTHER = "other"


class WebClipRequest(BaseModel):
    """Chrome Extension 网页剪藏请求"""
    title: str = Field(..., min_length=1, max_length=500, description="网页标题")
    html: str = Field(..., min_length=1, description="已内嵌 base64 图片的 HTML 内容")
    source_url: str = Field(..., min_length=1, description="原始网页 URL")


class WebLinkRequest(BaseModel):
    """Web Link 网页抓取请求"""
    url: str = Field(..., min_length=1, max_length=2000, description="目标网页 URL")


class ProcessingStatus(str, Enum):
    """处理状态枚举"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"


class TaskStatus(str, Enum):
    """任务状态枚举"""
    STARTING = "starting"
    INITIALIZING = "initializing"
    PROCESSING = "processing"
    AI_PROCESSING = "ai_processing"
    SAVING = "saving"
    COMPLETED = "completed"
    FAILED = "failed"
    STOPPED = "stopped"


class FileUploadResponse(BaseModel):
    """文件上传响应模型"""
    filename: str
    file_path: str
    file_size: int
    file_type: FileType
    upload_time: UtcDateTime


class FileProcessRequest(BaseModel):
    """文件处理请求模型"""
    file_paths: List[str] = Field(..., description="文件路径列表")
    task_type: str = Field(..., description="AI Agent或模板标识")
    custom_inputs: Dict[str, Any] = Field(default_factory=dict, description="自定义输入参数")
    params: Dict[str, Any] = Field(default_factory=dict, description="处理参数")
    
    # 新增：分析模式相关字段
    analysis_mode: str = Field('remote_agent', description="分析模式: 'remote_agent' 或 'local_llm'")
    template_id: Optional[int] = Field(None, description="Prompt模板ID（local_llm模式使用）")
    template_code: Optional[str] = Field(None, description="Prompt模板代码（local_llm模式使用）")
    template_variables: Dict[str, Any] = Field(default_factory=dict, description="模板变量（local_llm模式使用）")
    llm_provider: Optional[str] = Field(None, description="指定LLM提供商（local_llm模式，可选）")
    llm_model: Optional[str] = Field(None, description="指定LLM模型（local_llm模式，可选）")


class FileProcessResponse(BaseModel):
    """文件处理响应模型"""
    task_id: str
    message: str
    files_count: int


class FileDetailResponse(BaseModel):
    """文件详情响应模型"""
    id: int
    task_id: str
    filename: str
    file_type: FileType
    file_size: float
    processing_time: Optional[float] = None
    ai_time: Optional[float] = None
    content_length: Optional[int] = None
    status: ProcessingStatus
    total_tokens: Optional[int] = None
    cost: Optional[float] = None
    error_message: Optional[str] = None
    # 全系统时区契约: 使用 UtcDateTime, 序列化时自动带 UTC tz (...+00:00)
    # 详见 backend/utils/datetime_utils.py 与 docs/instructions/DAILY_MODULE.md §13
    created_at: UtcDateTime
    original_file_path: Optional[str] = None
    access_type: Optional[str] = None
    shared_by: Optional[str] = None
    display_name: Optional[str] = None

    class Config:
        from_attributes = True


class ProcessingParamsRequest(BaseModel):
    """处理参数请求模型"""
    language: List[str] = Field(default=["ch_sim", "en"], description="解析语言")
    fast_mode: bool = Field(default=False, description="快速模式")
    disable_ocr: bool = Field(default=False, description="禁用OCR")
    max_pages: int = Field(default=10, ge=1, le=1000, description="最大页数")
    target_pages: Optional[str] = Field(None, description="目标页面")
    skip_diagonal_text: bool = Field(default=False, description="跳过对角线文本")
    skip_table_extraction: bool = Field(default=False, description="跳过表格提取")
    extract_images: bool = Field(default=False, description="提取图像")
    use_vendor_multimodal_model: bool = Field(default=False, description="使用供应商多模态模型")
    vendor_multimodal_api_key: Optional[str] = Field(None, description="供应商多模态API密钥")
    polling_interval: int = Field(default=10, ge=1, le=60, description="轮询间隔（秒）")
    use_ai_agent: bool = Field(default=True, description="使用AI Agent处理")


class TaskLogResponse(BaseModel):
    """任务日志响应模型"""
    timestamp: str
    level: str
    message: str


class TaskStatusResponse(BaseModel):
    """任务状态响应模型"""
    status: TaskStatus
    progress: float
    current_file: Optional[str] = None
    files_processed: int
    files_failed: int
    total_files: int
    logs: List[TaskLogResponse]
    start_time: float


class FileValidationResponse(BaseModel):
    """文件验证响应模型"""
    valid_files: List[str]
    invalid_files: List[Dict[str, str]]
    summary: Dict[str, Any]


class FileInfoResponse(BaseModel):
    """文件信息响应模型"""
    file_name: str
    file_path: str
    file_type: str
    file_size_mb: float
    file_extension: str
    is_supported: bool


class ProcessingParamsOptions(BaseModel):
    """处理参数选项模型"""
    language_options: List[Dict[str, Any]]
    boolean_options: List[Dict[str, Any]]
    numeric_options: List[Dict[str, Any]]
    text_options: List[Dict[str, Any]]


class SupportedFileTypesResponse(BaseModel):
    """支持的文件类型响应模型"""
    supported_extensions: List[str]
    descriptions: Dict[str, str]


class FileReorderRequest(BaseModel):
    """文件重新排序请求模型"""
    file_paths: List[str] = Field(..., description="重新排序后的文件路径列表")


class FileReorderResponse(BaseModel):
    """文件重新排序响应模型"""
    message: str
    ordered_files: List[str]


class TaskExecutionDetailResponse(BaseModel):
    """任务执行详情响应模型"""
    execution: Dict[str, Any]
    files: List[Dict[str, Any]] 