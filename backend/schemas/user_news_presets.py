# -*- coding: utf-8 -*-
"""
用户新闻筛选预设 Pydantic Schemas
"""

from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from datetime import datetime


class FilterConfig(BaseModel):
    """筛选条件配置"""
    event_types: Optional[List[str]] = Field(None, description="事件类型列表")
    urgency: Optional[List[str]] = Field(None, description="时效性列表: urgent, timely, routine")
    impact_scopes: Optional[List[str]] = Field(None, description="影响范围列表: company_specific, sector_wide, market_wide, global")
    min_confidence: Optional[float] = Field(0.6, ge=0.0, le=1.0, description="最低置信度")
    only_primary: Optional[bool] = Field(False, description="只显示主要主题")
    source_types: Optional[List[str]] = Field(None, description="来源类型: eodhd_auto_tag, vector_retrieval, llm_classification")
    custom_logic: Optional[str] = Field("AND", description="多条件逻辑: AND | OR")
    
    class Config:
        schema_extra = {
            "example": {
                "event_types": ["earnings_report", "merger_acquisition"],
                "urgency": ["urgent", "timely"],
                "impact_scopes": ["company_specific"],
                "min_confidence": 0.7,
                "only_primary": True,
                "custom_logic": "AND"
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
    user_id: int
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
    last_used_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime
    
    class Config:
        orm_mode = True
        schema_extra = {
            "example": {
                "id": 1,
                "user_id": 1,
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


