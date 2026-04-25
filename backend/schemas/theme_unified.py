"""
Unified Theme System Schemas
统一主题系统的数据模型定义
"""
from datetime import datetime

from backend.utils.datetime_utils import UtcDateTime, utcnow as _utcnow_aware
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator, ConfigDict
from enum import Enum


# ============================================================
# Enums
# ============================================================

class ThemeSourceType(str, Enum):
    """主题来源类型"""
    EODHD_AUTO_TAG = "eodhd_auto_tag"
    VECTOR_RETRIEVAL = "vector_retrieval"
    LLM_CLASSIFICATION = "llm_classification"
    MANUAL = "manual"
    HYBRID = "hybrid"  # 混合/融合来源


class ThemeCategory(str, Enum):
    """主题分类"""
    TECHNOLOGY = "Technology"
    HEALTHCARE = "Healthcare"
    ENERGY = "Energy"
    FINANCE = "Finance"
    CONSUMER = "Consumer"
    INDUSTRIALS = "Industrials"
    MATERIALS = "Materials"
    REAL_ESTATE = "Real Estate"
    UTILITIES = "Utilities"
    TELECOMMUNICATIONS = "Telecommunications"
    OTHER = "Other"


class Market(str, Enum):
    """市场"""
    US = "US"
    HK = "HK"
    CN = "CN"
    GLOBAL = "GLOBAL"


class PeriodType(str, Enum):
    """统计周期"""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    ALL_TIME = "all_time"


# ============================================================
# Theme Master Data Schemas
# ============================================================

class ThemeMasterDataBase(BaseModel):
    """主题主数据基础模型"""
    canonical_name: str = Field(..., max_length=255, description="标准化名称")
    display_name: str = Field(..., max_length=255, description="显示名称")
    category: Optional[ThemeCategory] = Field(None, description="主题分类")
    sub_category: Optional[str] = Field(None, max_length=100, description="子分类")
    description: Optional[str] = Field(None, description="描述")
    aliases: List[str] = Field(default_factory=list, description="同义词列表")
    parent_theme_id: Optional[int] = Field(None, description="父主题ID")
    icon: Optional[str] = Field(None, max_length=50, description="图标")
    color: Optional[str] = Field(None, max_length=20, description="颜色")
    priority: int = Field(default=0, description="优先级")
    is_active: bool = Field(default=True, description="是否激活")
    is_system: bool = Field(default=False, description="是否系统预定义")


class ThemeMasterDataCreate(ThemeMasterDataBase):
    """创建主题主数据"""
    created_by: Optional[str] = Field(None, max_length=100)


class ThemeMasterDataUpdate(BaseModel):
    """更新主题主数据"""
    display_name: Optional[str] = Field(None, max_length=255)
    category: Optional[ThemeCategory] = None
    sub_category: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    aliases: Optional[List[str]] = None
    parent_theme_id: Optional[int] = None
    icon: Optional[str] = Field(None, max_length=50)
    color: Optional[str] = Field(None, max_length=20)
    priority: Optional[int] = None
    is_active: Optional[bool] = None
    updated_by: Optional[str] = Field(None, max_length=100)


class ThemeMasterData(ThemeMasterDataBase):
    """主题主数据完整模型"""
    id: int
    usage_count: int = 0
    last_used_at: Optional[UtcDateTime] = None
    created_at: UtcDateTime
    updated_at: UtcDateTime
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    
    model_config = ConfigDict(from_attributes=True)


# ============================================================
# News Themes Unified Schemas
# ============================================================

class NewsThemeUnifiedBase(BaseModel):
    """统一主题基础模型"""
    news_id: int = Field(..., description="新闻ID")
    theme_name: str = Field(..., max_length=255, description="原始主题名称")
    source_type: ThemeSourceType = Field(..., description="来源类型")
    source_id: Optional[str] = Field(None, max_length=255, description="原始来源ID")
    source_table: Optional[str] = Field(None, max_length=100, description="原始数据表名")
    confidence_score: float = Field(default=0.5, ge=0, le=1, description="置信度")
    market: Optional[Market] = Field(None, description="市场")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="元数据")
    is_active: bool = Field(default=True, description="是否激活")
    is_primary: bool = Field(default=False, description="是否主要主题")


class NewsThemeUnifiedCreate(NewsThemeUnifiedBase):
    """创建统一主题"""
    theme_master_id: Optional[int] = None
    quality_score: Optional[float] = Field(None, ge=0, le=1)
    relevance_score: Optional[float] = Field(None, ge=0, le=1)
    rank_in_news: Optional[int] = None
    weight: float = Field(default=1.0, ge=0, le=2)


class NewsThemeUnifiedUpdate(BaseModel):
    """更新统一主题"""
    theme_master_id: Optional[int] = None
    confidence_score: Optional[float] = Field(None, ge=0, le=1)
    quality_score: Optional[float] = Field(None, ge=0, le=1)
    relevance_score: Optional[float] = Field(None, ge=0, le=1)
    rank_in_news: Optional[int] = None
    weight: Optional[float] = Field(None, ge=0, le=2)
    is_active: Optional[bool] = None
    is_verified: Optional[bool] = None
    is_primary: Optional[bool] = None
    verification_note: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class NewsThemeUnified(NewsThemeUnifiedBase):
    """统一主题完整模型"""
    id: int
    theme_master_id: Optional[int] = None
    quality_score: Optional[float] = None
    relevance_score: Optional[float] = None
    rank_in_news: Optional[int] = None
    weight: float = 1.0
    is_verified: bool = False
    verified_by: Optional[int] = None
    verified_at: Optional[UtcDateTime] = None
    verification_note: Optional[str] = None
    created_at: UtcDateTime
    updated_at: UtcDateTime
    
    model_config = ConfigDict(from_attributes=True)


class NewsThemeUnifiedWithMaster(NewsThemeUnified):
    """带主数据的统一主题"""
    master_data: Optional[ThemeMasterData] = None


# ============================================================
# Theme Verification Schemas
# ============================================================

class ThemeVerification(BaseModel):
    """主题验证"""
    theme_record_id: int = Field(..., description="主题记录ID")
    is_verified: bool = Field(..., description="验证结果")
    verification_note: Optional[str] = Field(None, description="验证说明")
    verified_by: int = Field(..., description="验证人ID")


class BatchThemeVerification(BaseModel):
    """批量主题验证"""
    theme_record_ids: List[int] = Field(..., description="主题记录ID列表")
    is_verified: bool = Field(..., description="验证结果")
    verification_note: Optional[str] = Field(None, description="验证说明")
    verified_by: int = Field(..., description="验证人ID")


# ============================================================
# Theme Fusion Schemas
# ============================================================

class ThemeFusionResult(BaseModel):
    """主题融合结果"""
    theme_name: str = Field(..., description="主题名称")
    fusion_score: float = Field(..., ge=0, le=1, description="融合得分")
    confidence_score: float = Field(..., ge=0, le=1, description="平均置信度")
    quality_score: Optional[float] = Field(None, ge=0, le=1, description="平均质量分数")
    source_count: int = Field(..., description="来源数量")
    sources: List[ThemeSourceType] = Field(..., description="来源列表")
    markets: List[Market] = Field(default_factory=list, description="市场列表")
    is_verified: bool = Field(default=False, description="是否已验证")
    is_primary: bool = Field(default=False, description="是否主要主题")


class NewsBestThemes(BaseModel):
    """新闻最佳主题"""
    news_id: int
    themes: List[ThemeFusionResult] = Field(..., description="融合后的主题列表")
    total_count: int = Field(..., description="主题总数")


# ============================================================
# Theme Statistics Schemas
# ============================================================

class ThemeTrending(BaseModel):
    """主题热度"""
    theme_name: str
    display_name: Optional[str] = None
    category: Optional[str] = None
    icon: Optional[str] = None
    color: Optional[str] = None
    news_count: int = Field(..., description="新闻总数")
    news_count_7d: int = Field(..., description="最近7天新闻数")
    news_count_30d: int = Field(..., description="最近30天新闻数")
    active_days: int = Field(..., description="活跃天数")
    avg_confidence: Optional[float] = None
    avg_quality: Optional[float] = None
    source_distribution: Dict[str, int] = Field(default_factory=dict)
    latest_news_date: Optional[UtcDateTime] = None
    earliest_news_date: Optional[UtcDateTime] = None
    
    model_config = ConfigDict(from_attributes=True)


class ThemeAggregationStats(BaseModel):
    """主题聚合统计"""
    theme_name: str
    period_type: PeriodType
    period_start: UtcDateTime
    period_end: UtcDateTime
    news_count: int = 0
    avg_confidence: Optional[float] = None
    avg_quality: Optional[float] = None
    source_distribution: Dict[str, int] = Field(default_factory=dict)
    market_distribution: Dict[str, int] = Field(default_factory=dict)
    
    model_config = ConfigDict(from_attributes=True)


# ============================================================
# Query Schemas
# ============================================================

class ThemeQuery(BaseModel):
    """主题查询参数"""
    news_id: Optional[int] = Field(None, description="新闻ID")
    theme_name: Optional[str] = Field(None, description="主题名称（模糊匹配）")
    source_type: Optional[ThemeSourceType] = Field(None, description="来源类型")
    market: Optional[Market] = Field(None, description="市场")
    category: Optional[ThemeCategory] = Field(None, description="分类")
    min_confidence: Optional[float] = Field(None, ge=0, le=1, description="最小置信度")
    min_quality: Optional[float] = Field(None, ge=0, le=1, description="最小质量分数")
    is_verified: Optional[bool] = Field(None, description="是否已验证")
    is_active: Optional[bool] = Field(True, description="是否激活")
    limit: int = Field(default=50, ge=1, le=500, description="返回数量")
    offset: int = Field(default=0, ge=0, description="偏移量")


class ThemeSyncRequest(BaseModel):
    """主题同步请求"""
    news_ids: List[int] = Field(..., description="要同步的新闻ID列表")
    source_types: List[ThemeSourceType] = Field(
        default_factory=lambda: [
            ThemeSourceType.EODHD_AUTO_TAG,
            ThemeSourceType.VECTOR_RETRIEVAL,
            ThemeSourceType.LLM_CLASSIFICATION
        ],
        description="要同步的来源类型"
    )
    force_resync: bool = Field(default=False, description="强制重新同步")


class ThemeSyncResult(BaseModel):
    """主题同步结果"""
    success: bool
    synced_count: int = Field(..., description="同步成功的数量")
    failed_count: int = Field(default=0, description="失败数量")
    details: Dict[str, Any] = Field(default_factory=dict, description="详细信息")
    errors: List[str] = Field(default_factory=list, description="错误列表")


# ============================================================
# Response Schemas
# ============================================================

class ThemeListResponse(BaseModel):
    """主题列表响应"""
    items: List[NewsThemeUnified]
    total: int
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=500)
    has_more: bool


class ThemeMasterListResponse(BaseModel):
    """主题主数据列表响应"""
    items: List[ThemeMasterData]
    total: int
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=50, ge=1, le=500)
    has_more: bool


class ThemeTrendingResponse(BaseModel):
    """主题热度排行响应"""
    items: List[ThemeTrending]
    total: int
    period: str = Field(default="90d", description="统计周期")
    updated_at: UtcDateTime = Field(default_factory=_utcnow_aware)


# ============================================================
# Utility Schemas
# ============================================================

class ThemeNormalizationResult(BaseModel):
    """主题标准化结果"""
    original_name: str
    canonical_name: str
    matched: bool = Field(..., description="是否匹配到主数据")
    theme_master_id: Optional[int] = None
    confidence: float = Field(default=1.0, ge=0, le=1, description="标准化置信度")


class ThemeQualityReport(BaseModel):
    """主题质量报告"""
    theme_name: str
    news_count: int
    source_distribution: Dict[str, int]
    avg_confidence_by_source: Dict[str, float]
    consistency_score: float = Field(..., ge=0, le=1, description="一致性得分")
    quality_issues: List[str] = Field(default_factory=list, description="质量问题")
    recommendations: List[str] = Field(default_factory=list, description="改进建议")



