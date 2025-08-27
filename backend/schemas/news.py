# -*- coding: utf-8 -*-
"""
新闻相关的数据模型
"""

from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class NewsResponse(BaseModel):
    """新闻响应模型"""
    id: int = Field(..., description="新闻ID")
    date: Optional[str] = Field(None, description="新闻发布日期")
    title: str = Field(..., description="新闻标题")
    content: Optional[str] = Field(None, description="新闻内容")
    link: Optional[str] = Field(None, description="新闻链接")
    source: Optional[str] = Field(None, description="新闻来源")
    sentiment_polarity: Optional[float] = Field(None, description="情感极性")
    sentiment_neg: Optional[float] = Field(None, description="负面情感")
    sentiment_neu: Optional[float] = Field(None, description="中性情感")
    sentiment_pos: Optional[float] = Field(None, description="正面情感")
    created_at: Optional[str] = Field(None, description="创建时间")
    updated_at: Optional[str] = Field(None, description="更新时间")
    symbols: List[str] = Field(default_factory=list, description="相关股票代码")
    tags: List[str] = Field(default_factory=list, description="相关标签")


class NewsListResponse(BaseModel):
    """新闻列表响应模型"""
    news: List[NewsResponse] = Field(..., description="新闻列表")
    total: int = Field(..., description="总数量")
    page: int = Field(..., description="当前页码")
    page_size: int = Field(..., description="每页数量")
    total_pages: int = Field(..., description="总页数")


class NewsTopicResponse(BaseModel):
    """新闻主题响应模型"""
    id: int = Field(..., description="主题ID")
    topic_name: str = Field(..., description="主题名称")
    description: Optional[str] = Field(None, description="主题描述")
    is_active: bool = Field(..., description="是否激活")
    color: Optional[str] = Field(None, description="主题颜色")
    created_at: Optional[str] = Field(None, description="创建时间")
    updated_at: Optional[str] = Field(None, description="更新时间")


class NewsSymbolResponse(BaseModel):
    """新闻股票代码响应模型"""
    symbol_code: str = Field(..., description="股票代码")
    news_count: int = Field(..., description="相关新闻数量")


class NewsTagResponse(BaseModel):
    """新闻标签响应模型"""
    tag_name: str = Field(..., description="标签名称")
    news_count: int = Field(..., description="相关新闻数量")


class NewsSearchRequest(BaseModel):
    """新闻搜索请求模型"""
    page: int = Field(1, ge=1, description="页码")
    page_size: int = Field(20, ge=1, le=100, description="每页数量")
    topic_id: Optional[int] = Field(None, description="主题ID过滤")
    symbol_code: Optional[str] = Field(None, description="股票代码过滤")
    start_date: Optional[str] = Field(None, description="开始日期 (YYYY-MM-DD)")
    end_date: Optional[str] = Field(None, description="结束日期 (YYYY-MM-DD)")
    search_text: Optional[str] = Field(None, description="搜索文本")


class NewsTopicKeywordResponse(BaseModel):
    """新闻主题关键词响应模型"""
    id: int = Field(..., description="关键词ID")
    topic_id: int = Field(..., description="主题ID")
    keyword: str = Field(..., description="关键词")
    weight: float = Field(..., description="权重")
    created_at: Optional[str] = Field(None, description="创建时间") 