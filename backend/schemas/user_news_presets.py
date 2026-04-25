# -*- coding: utf-8 -*-
"""
用户新闻筛选预设 Pydantic Schemas
"""

from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from datetime import datetime

from backend.utils.datetime_utils import UtcDateTime


class DimensionOperators(BaseModel):
    """各维度内部逻辑操作符配置"""
    news_value: Optional[str] = Field(None, description="news_value维度内部逻辑: OR或AND")
    categories: Optional[str] = Field(None, description="categories维度内部逻辑: OR或AND")
    event_types: Optional[str] = Field(None, description="event_types维度内部逻辑: OR或AND")
    impact_scopes: Optional[str] = Field(None, description="impact_scopes维度内部逻辑: OR或AND")
    urgency: Optional[str] = Field(None, description="urgency维度内部逻辑: OR或AND")
    related_sectors: Optional[str] = Field(None, description="related_sectors维度内部逻辑: OR或AND")
    related_countries: Optional[str] = Field(None, description="related_countries维度内部逻辑: OR或AND")
    source_types: Optional[str] = Field(None, description="source_types维度内部逻辑: OR或AND")
    scheduler_sources: Optional[str] = Field(None, description="scheduler_sources维度内部逻辑: OR或AND")


class FilterConfig(BaseModel):
    """筛选条件配置"""
    # 新闻价值分类 (新增)
    news_value: Optional[List[str]] = Field(None, description="新闻价值类型: fact(事实性新闻), opinion(观点性文章)")
    
    # 主题分类维度
    categories: Optional[List[str]] = Field(None, description="主要主题分类 (political, financial, technology...)")
    event_types: Optional[List[str]] = Field(None, description="事件类型列表")
    impact_scopes: Optional[List[str]] = Field(None, description="影响范围列表: company_specific, sector_wide, market_wide, global")
    urgency: Optional[List[str]] = Field(None, description="时效性列表: urgent, timely, routine")
    related_sectors: Optional[List[str]] = Field(None, description="相关行业列表")
    related_countries: Optional[List[str]] = Field(None, description="相关国家/地区列表")
    
    # 来源筛选
    source_types: Optional[List[str]] = Field(None, description="来源类型: eodhd_auto_tag, vector_retrieval, llm_classification")
    scheduler_sources: Optional[List[str]] = Field(None, description="调度器来源: eodhd_news, watchlist_news, cctv_news")
    
    # 🔥 新增：Ticker过滤
    ticker_symbols: Optional[List[str]] = Field(None, description="指定ticker符号列表，如: ['AAPL.US', '0700.HK']")
    use_watchlist: Optional[bool] = Field(None, description="是否使用用户watchlist中的ticker")
    watchlist_groups: Optional[List[str]] = Field(None, description="使用特定watchlist分组，如: ['Technology', 'Healthcare']")
    ticker_filter_mode: Optional[str] = Field(None, description="Ticker过滤模式: 'include'(包含) | 'exclude'(排除)")
    
    # 其他选项
    min_confidence: Optional[float] = Field(None, ge=0.0, le=1.0, description="最低置信度")
    only_primary: Optional[bool] = Field(None, description="只显示主要主题")
    market: Optional[str] = Field(None, description="市场: HK/US/CN/GLOBAL")
    
    # 🔥 新增：各维度内部逻辑操作符（默认 OR）
    dimension_operators: Optional[DimensionOperators] = Field(None, description="各维度内部逻辑操作符配置")
    
    # 逻辑关系（暂时保留，未来可能用到）
    custom_logic: Optional[str] = Field(None, description="多条件逻辑: AND | OR")
    
    @validator('ticker_filter_mode')
    def validate_ticker_filter_mode(cls, v):
        if v and v not in ['include', 'exclude']:
            raise ValueError('ticker_filter_mode must be either "include" or "exclude"')
        return v
    
    @validator('watchlist_groups')
    def validate_watchlist_groups(cls, v, values):
        if v and not values.get('use_watchlist'):
            raise ValueError('watchlist_groups requires use_watchlist to be True')
        return v
    
    class Config:
        # 允许额外字段，防止未知字段被丢弃
        extra = 'allow'
        schema_extra = {
            "example": {
                "news_value": ["fact"],
                "categories": ["financial", "technology"],
                "event_types": ["earnings_report", "merger_acquisition"],
                "urgency": ["urgent", "timely"],
                "impact_scopes": ["company_specific"],
                "related_countries": ["CN", "JP"],
                "min_confidence": 0.7,
                "only_primary": True,
                "dimension_operators": {
                    "news_value": "OR",
                    "categories": "OR",
                    "related_countries": "AND"
                },
                "ticker_symbols": ["AAPL.US", "TSLA.US"],
                "use_watchlist": False,
                "ticker_filter_mode": "include"
            }
        }


class CreatePresetRequest(BaseModel):
    """创建预设请求"""
    preset_name: str = Field(..., min_length=1, max_length=100, description="预设名称")
    description: Optional[str] = Field(None, max_length=500, description="预设说明")
    icon: Optional[str] = Field(None, max_length=50, description="图标（Emoji或图标代码）")
    filter_config: FilterConfig = Field(..., description="筛选条件")
    scope_type: str = Field("global", description="应用范围: global | symbol_specific")
    symbol_code: Optional[str] = Field(None, max_length=20, description="股票代码（仅symbol_specific时需要）")
    is_default: bool = Field(False, description="是否设为默认")
    is_favorite: bool = Field(False, description="是否收藏")
    is_system_preset: bool = Field(False, description="是否创建为系统预设（仅admin可用）")
    
    @validator('scope_type')
    def validate_scope_type(cls, v):
        if v not in ['global', 'symbol_specific']:
            raise ValueError('scope_type must be either "global" or "symbol_specific"')
        return v
    
    @validator('symbol_code')
    def validate_symbol_code(cls, v, values):
        if values.get('scope_type') == 'symbol_specific' and not v:
            raise ValueError('symbol_code is required when scope_type is "symbol_specific"')
        return v
    
    class Config:
        schema_extra = {
            "example": {
                "preset_name": "我的财报追踪",
                "description": "追踪财报和业绩相关的重要新闻",
                "icon": "📊",
                "filter_config": {
                    "event_types": ["earnings_report"],
                    "urgency": ["urgent", "timely"],
                    "min_confidence": 0.7
                },
                "scope_type": "symbol_specific",
                "symbol_code": "1810.HK",
                "is_default": True,
                "is_favorite": True
            }
        }


class UpdatePresetRequest(BaseModel):
    """更新预设请求"""
    preset_name: Optional[str] = Field(None, min_length=1, max_length=100, description="预设名称")
    description: Optional[str] = Field(None, max_length=500, description="预设说明")
    icon: Optional[str] = Field(None, max_length=50, description="图标")
    filter_config: Optional[FilterConfig] = Field(None, description="筛选条件")
    is_default: Optional[bool] = Field(None, description="是否设为默认")
    is_favorite: Optional[bool] = Field(None, description="是否收藏")
    sort_order: Optional[int] = Field(None, description="显示顺序")


class PresetResponse(BaseModel):
    """预设响应"""
    id: int
    user_id: Optional[int]  # 系统预设的 user_id 为 None
    preset_name: str
    preset_type: str
    description: Optional[str]
    icon: Optional[str]
    filter_config: Dict[str, Any]
    scope_type: str
    symbol_code: Optional[str]
    is_default: bool
    is_favorite: bool
    sort_order: int
    usage_count: int
    last_used_at: Optional[UtcDateTime]
    created_at: Optional[UtcDateTime]
    updated_at: Optional[UtcDateTime]
    
    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "id": 1,
                "user_id": 1,  # 用户自定义预设有 user_id，系统预设为 None
                "preset_name": "⭐ 重要新闻",
                "preset_type": "custom",
                "description": "紧急和及时的高相关度新闻",
                "icon": "⭐",
                "filter_config": {
                    "urgency": ["urgent", "timely"],
                    "min_confidence": 0.7,
                    "only_primary": True
                },
                "scope_type": "global",
                "symbol_code": None,
                "is_default": True,
                "is_favorite": False,
                "sort_order": 0,
                "usage_count": 15,
                "last_used_at": "2025-11-02T10:30:00",
                "created_at": "2025-11-01T08:00:00",
                "updated_at": "2025-11-02T10:30:00"
            }
        }


class PresetListResponse(BaseModel):
    """预设列表响应"""
    success: bool
    total: int
    presets: List[PresetResponse]


class PresetOperationResponse(BaseModel):
    """预设操作响应"""
    success: bool
    message: str
    preset: Optional[PresetResponse] = None


