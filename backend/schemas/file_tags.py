# -*- coding: utf-8 -*-
"""
文件标签相关Schema定义
"""
from pydantic import BaseModel, Field, validator
from typing import List, Dict, Any, Optional
from datetime import datetime

from backend.utils.datetime_utils import UtcDateTime


class FileTagsBase(BaseModel):
    """文件标签基础Schema"""
    sector: str = Field(..., description="行业分类（GICS一级分类）")
    industry: str = Field(..., description="子行业分类")
    tickers: List[str] = Field(default_factory=list, description="股票代码列表")
    additional_tags: Dict[str, Any] = Field(default_factory=dict, description="扩展标签")
    confidence: Optional[float] = Field(None, ge=0, le=1, description="置信度（0-1）")
    reasoning: Optional[str] = Field(None, description="推理说明")


class FileTagsCreate(FileTagsBase):
    """创建文件标签Schema"""
    file_id: int = Field(..., description="文件ID（file_processing_details.id）")


class FileTagsUpdate(BaseModel):
    """更新文件标签Schema（所有字段都是可选的）"""
    sector: Optional[str] = Field(None, description="行业分类")
    sector_en: Optional[str] = Field(None, description="英文行业分类")
    industry: Optional[str] = Field(None, description="子行业分类")
    industry_en: Optional[str] = Field(None, description="英文子行业分类")
    tickers: Optional[List[str]] = Field(None, description="股票代码列表")
    concepts: Optional[List[str]] = Field(None, description="投资概念/主题列表")
    additional_tags: Optional[Dict[str, Any]] = Field(None, description="扩展标签")
    confidence: Optional[float] = Field(None, ge=0, le=1, description="置信度")
    reasoning: Optional[str] = Field(None, description="推理说明")


class FileTagsResponse(BaseModel):
    """文件标签响应Schema"""
    id: int
    file_id: int
    sector: str
    sector_en: Optional[str] = Field(None, description="英文行业分类（GICS标准）")
    industry: str
    industry_en: Optional[str] = Field(None, description="英文子行业分类（GICS标准）")
    tickers: List[str]  # 前端使用数组
    ticker_raw: Optional[str] = Field(None, description="原始ticker字段值（JSON字符串）")
    concepts: List[str] = Field(default_factory=list, description="投资概念/主题列表")
    additional_tags: Dict[str, Any]
    confidence: Optional[float]
    reasoning: Optional[str]
    tagged_by: Optional[int]
    tagged_at: Optional[UtcDateTime]
    created_at: Optional[UtcDateTime]
    updated_at: Optional[UtcDateTime]

    class Config:
        from_attributes = True


class FileTagsWithFileInfo(FileTagsResponse):
    """带文件信息的标签响应Schema"""
    filename: Optional[str] = None
    file_type: Optional[str] = None
    processing_stats_id: Optional[int] = None


class RetagRequest(BaseModel):
    """重新打标签请求Schema"""
    file_id: int = Field(..., description="文件ID")
    force: bool = Field(default=False, description="是否强制重新打标签（即使已有标签）")


class BatchRetagRequest(BaseModel):
    """批量重新打标签请求Schema"""
    file_ids: List[int] = Field(..., description="文件ID列表")
    force: bool = Field(default=False, description="是否强制重新打标签")


class TaggingTaskStatus(BaseModel):
    """打标签任务状态Schema"""
    task_id: str
    status: str  # pending, processing, completed, failed
    progress: float
    message: Optional[str] = None
    file_id: Optional[int] = None
    error: Optional[str] = None
    result: Optional[FileTagsResponse] = None


class FileTagsStats(BaseModel):
    """标签统计Schema"""
    total_tagged_files: int
    sectors: Dict[str, int]  # sector -> count
    industries: Dict[str, int]  # industry -> count
    top_tickers: List[Dict[str, Any]]  # [{ticker: str, count: int}]
    avg_confidence: Optional[float]
