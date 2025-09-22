# -*- coding: utf-8 -*-
"""
文件标签相关的数据模型
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field


class FileTagBase(BaseModel):
    """文件标签基础模型"""
    sector: str = Field(..., description="行业分类")
    industry: str = Field(..., description="子行业分类")
    ticker: Optional[str] = Field(None, description="股票代码（可选）")


class FileTagCreate(FileTagBase):
    """创建文件标签的请求模型"""
    file_id: int = Field(..., description="文件ID")


class FileTagUpdate(FileTagBase):
    """更新文件标签的请求模型"""
    pass


class FileTagResponse(FileTagBase):
    """文件标签响应模型"""
    id: int = Field(..., description="标签ID")
    file_id: int = Field(..., description="文件ID")
    tagged_by: Optional[int] = Field(None, description="标签创建者用户ID")
    tagged_at: datetime = Field(..., description="标签创建时间")
    created_at: datetime = Field(..., description="记录创建时间")
    updated_at: datetime = Field(..., description="记录更新时间")
    
    class Config:
        from_attributes = True


class FileTagBatchCreate(BaseModel):
    """批量创建文件标签的请求模型"""
    file_ids: list[int] = Field(..., description="文件ID列表")
    sector: str = Field(..., description="行业分类")
    industry: str = Field(..., description="子行业分类")
    ticker: Optional[str] = Field(None, description="股票代码（可选）")


class FileTagStats(BaseModel):
    """文件标签统计模型"""
    total_files: int = Field(..., description="总文件数")
    tagged_files: int = Field(..., description="已标签文件数")
    untagged_files: int = Field(..., description="未标签文件数")
    sectors: list[dict] = Field(..., description="行业分类统计")
    industries: list[dict] = Field(..., description="子行业分类统计")
    tickers: list[dict] = Field(..., description="股票代码统计")


class FileWithTag(BaseModel):
    """带标签的文件信息模型"""
    id: int = Field(..., description="文件ID")
    filename: str = Field(..., description="文件名")
    file_path: str = Field(..., description="文件路径")
    file_size: int = Field(..., description="文件大小")
    file_type: str = Field(..., description="文件类型")
    created_at: datetime = Field(..., description="文件创建时间")
    tag: Optional[FileTagResponse] = Field(None, description="文件标签")
    
    class Config:
        from_attributes = True
