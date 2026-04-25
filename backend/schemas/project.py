# -*- coding: utf-8 -*-
"""
投资项目管理相关数据模型
"""

from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from enum import Enum

from backend.utils.datetime_utils import UtcDateTime


# ==================== 枚举类型 ====================

class ProjectType(str, Enum):
    """项目类型"""
    TICKER = "ticker"
    INDUSTRY = "industry"
    SECTOR = "sector"


class ProjectStatus(str, Enum):
    """项目状态"""
    ACTIVE = "active"
    PAUSED = "paused"
    COMPLETED = "completed"
    ARCHIVED = "archived"


class ProjectPriority(int, Enum):
    """项目优先级"""
    LOW = 1
    MEDIUM = 2
    HIGH = 3


class MemberRole(str, Enum):
    """成员角色"""
    OWNER = "owner"
    ADMIN = "admin"
    MEMBER = "member"
    VIEWER = "viewer"


class MemberStatus(str, Enum):
    """成员状态"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    INVITED = "invited"


class MilestoneType(str, Enum):
    """里程碑类型"""
    MEETING_NOTES = "meeting_notes"
    RESEARCH_REPORT = "research_report"
    DECISION = "decision"
    UPDATE = "update"
    OTHER = "other"


class DecisionType(str, Enum):
    """决策类型"""
    BUY = "buy"
    SELL = "sell"
    HOLD = "hold"
    WATCH = "watch"
    PASS = "pass"


class FileCategory(str, Enum):
    """文件分类"""
    MEETING_NOTES = "meeting_notes"
    REPORT = "report"
    CONTRACT = "contract"
    PRESENTATION = "presentation"
    OTHER = "other"


# ==================== 投资项目相关模型 ====================

class ProjectBase(BaseModel):
    """项目基础模型"""
    project_name: str = Field(..., min_length=1, max_length=200, description="项目名称")
    project_type: ProjectType = Field(..., description="项目类型")
    tickers: Optional[List[str]] = Field(default_factory=list, description="股票代码列表（支持多个）")
    industry: Optional[str] = Field(None, max_length=100, description="行业")
    sector: Optional[str] = Field(None, max_length=100, description="板块")
    region: Optional[str] = Field(None, max_length=50, description="区域")
    status: ProjectStatus = Field(ProjectStatus.ACTIVE, description="项目状态")
    priority: ProjectPriority = Field(ProjectPriority.MEDIUM, description="优先级")
    description: Optional[str] = Field(None, description="项目描述")
    current_position: Optional[str] = Field(None, description="当前持仓情况")
    start_date: Optional[date] = Field(None, description="项目开始日期")

    @validator('tickers')
    def validate_tickers(cls, v):
        """验证tickers：去重、去空、转大写"""
        if v is None:
            return []
        # 去除空字符串、去重、转大写
        cleaned = list(set([ticker.strip().upper() for ticker in v if ticker and ticker.strip()]))
        return cleaned
    
    @validator('sector')
    def validate_project_target(cls, v, values):
        """验证项目必须指定tickers、industry或sector之一"""
        tickers = values.get('tickers', [])
        industry = values.get('industry')
        if not tickers and not industry and not v:
            raise ValueError("Project must have at least one ticker, industry, or sector")
        return v


class ProjectCreate(ProjectBase):
    """创建项目请求"""
    pass


class ProjectUpdate(BaseModel):
    """更新项目请求"""
    project_name: Optional[str] = Field(None, min_length=1, max_length=200)
    status: Optional[ProjectStatus] = None
    priority: Optional[ProjectPriority] = None
    tickers: Optional[List[str]] = None
    description: Optional[str] = None
    current_position: Optional[str] = None
    
    @validator('tickers')
    def validate_tickers_update(cls, v):
        """验证tickers：去重、去空、转大写"""
        if v is None:
            return None
        # 去除空字符串、去重、转大写
        cleaned = list(set([ticker.strip().upper() for ticker in v if ticker and ticker.strip()]))
        return cleaned if cleaned else None


class ProjectResponse(ProjectBase):
    """项目响应模型"""
    id: int
    project_code: str
    owner_id: int
    created_by: int
    created_at: UtcDateTime
    updated_at: UtcDateTime
    
    # 扩展信息（可选）
    member_count: Optional[int] = None
    news_count: Optional[int] = None
    file_count: Optional[int] = None
    milestone_count: Optional[int] = None
    
    class Config:
        from_attributes = True


# ==================== 项目成员相关模型 ====================

class MemberBase(BaseModel):
    """成员基础模型"""
    role: MemberRole = Field(MemberRole.MEMBER, description="角色")
    can_edit: bool = Field(True, description="是否可以编辑")
    can_invite: bool = Field(False, description="是否可以邀请其他成员")


class MemberInvite(BaseModel):
    """邀请成员请求"""
    user_ids: List[int] = Field(..., min_items=1, description="用户ID列表")
    role: MemberRole = Field(MemberRole.MEMBER, description="角色")
    can_edit: bool = Field(True, description="是否可以编辑")
    can_invite: bool = Field(False, description="是否可以邀请")
    message: Optional[str] = Field(None, description="邀请消息")


class MemberUpdate(BaseModel):
    """更新成员请求"""
    role: Optional[MemberRole] = None
    can_edit: Optional[bool] = None
    can_invite: Optional[bool] = None
    status: Optional[MemberStatus] = None


class MemberResponse(MemberBase):
    """成员响应模型"""
    id: int
    project_id: int
    user_id: int
    username: Optional[str] = None  # 关联查询获取
    status: MemberStatus
    invited_by: Optional[int] = None
    invited_at: Optional[UtcDateTime] = None
    joined_at: UtcDateTime
    created_at: UtcDateTime
    updated_at: UtcDateTime
    
    class Config:
        from_attributes = True


# ==================== 项目里程碑相关模型 ====================

class MilestoneBase(BaseModel):
    """里程碑基础模型"""
    milestone_type: MilestoneType = Field(..., description="里程碑类型")
    title: str = Field(..., min_length=1, max_length=200, description="标题")
    content: Optional[str] = Field(None, description="内容（支持Markdown）")
    file_ids: Optional[List[int]] = Field(default_factory=list, description="关联的文件ID列表")
    decision_type: Optional[DecisionType] = Field(None, description="决策类型")
    decision_rationale: Optional[str] = Field(None, description="决策理由")
    milestone_date: Optional[date] = Field(None, description="里程碑日期")


class MilestoneCreate(MilestoneBase):
    """创建里程碑请求"""
    pass


class MilestoneUpdate(BaseModel):
    """更新里程碑请求"""
    milestone_type: Optional[MilestoneType] = None
    title: Optional[str] = Field(None, min_length=1, max_length=200)
    content: Optional[str] = None
    file_ids: Optional[List[int]] = None
    decision_type: Optional[DecisionType] = None
    decision_rationale: Optional[str] = None
    milestone_date: Optional[date] = None


class MilestoneResponse(MilestoneBase):
    """里程碑响应模型"""
    id: int
    project_id: int
    created_by: int
    created_by_username: Optional[str] = None  # 关联查询获取
    created_at: UtcDateTime
    updated_at: UtcDateTime
    
    class Config:
        from_attributes = True


# ==================== 项目新闻跟踪相关模型 ====================

class NewsTrackingBase(BaseModel):
    """新闻跟踪基础模型"""
    relevance_score: Optional[int] = Field(None, ge=1, le=5, description="相关性评分 1-5")
    is_important: bool = Field(False, description="是否标记为重要")
    user_notes: Optional[str] = Field(None, description="用户批注")


class NewsTrackingCreate(NewsTrackingBase):
    """添加新闻到项目请求"""
    news_id: int = Field(..., description="新闻ID")


class NewsTrackingUpdate(BaseModel):
    """更新新闻跟踪请求"""
    relevance_score: Optional[int] = Field(None, ge=1, le=5)
    is_important: Optional[bool] = None
    user_notes: Optional[str] = None


class NewsTrackingResponse(NewsTrackingBase):
    """新闻跟踪响应模型"""
    id: int
    project_id: int
    news_id: int
    added_by: int
    added_by_username: Optional[str] = None
    added_at: UtcDateTime
    updated_at: UtcDateTime
    
    # 新闻详情（可选，从news表关联查询）
    news_title: Optional[str] = None
    news_date: Optional[str] = None
    news_source: Optional[str] = None
    
    class Config:
        from_attributes = True


# ==================== 项目文件相关模型 ====================

class ProjectFileBase(BaseModel):
    """项目文件基础模型"""
    file_name: str = Field(..., max_length=255, description="文件名")
    file_type: Optional[str] = Field(None, max_length=50, description="文件类型")
    category: Optional[FileCategory] = Field(None, description="文件分类")
    description: Optional[str] = Field(None, description="文件描述")
    milestone_id: Optional[int] = Field(None, description="关联的里程碑ID")


class ProjectFileCreate(ProjectFileBase):
    """创建项目文件请求"""
    file_path: str = Field(..., description="文件路径")
    file_size: Optional[int] = Field(None, ge=0, description="文件大小（字节）")


class ProjectFileResponse(ProjectFileBase):
    """项目文件响应模型"""
    id: int
    project_id: int
    file_path: str
    file_size: Optional[int] = None
    uploaded_by: int
    uploaded_by_username: Optional[str] = None
    uploaded_at: UtcDateTime
    
    class Config:
        from_attributes = True


# ==================== 项目统计和概览相关模型 ====================

class ProjectStatistics(BaseModel):
    """项目统计信息"""
    total_members: int = 0
    active_members: int = 0
    total_news: int = 0
    important_news: int = 0
    total_files: int = 0
    total_milestones: int = 0
    latest_milestone_date: Optional[date] = None
    latest_news_added_at: Optional[UtcDateTime] = None


class ProjectActivity(BaseModel):
    """项目活动记录"""
    activity_type: str = Field(..., description="活动类型")
    description: str = Field(..., description="活动描述")
    user_id: int
    username: Optional[str] = None
    created_at: UtcDateTime


class ProjectDashboard(BaseModel):
    """项目仪表盘数据"""
    project: ProjectResponse
    statistics: ProjectStatistics
    recent_activities: List[ProjectActivity] = Field(default_factory=list)
    ticker_performance: Optional[Dict[str, Any]] = None
    upcoming_events: List[Dict[str, Any]] = Field(default_factory=list)


# ==================== 自动新闻抓取相关模型 ====================

class AutoFetchNewsRequest(BaseModel):
    """自动抓取新闻请求"""
    recent_days: int = Field(7, ge=1, le=90, description="抓取最近N天的新闻")
    auto_analyze: bool = Field(True, description="是否自动分析新闻")
    min_relevance: Optional[int] = Field(None, ge=1, le=5, description="最低相关性评分")
    limit: Optional[int] = Field(50, ge=1, le=200, description="最大抓取数量")


class AutoFetchNewsResponse(BaseModel):
    """自动抓取新闻响应"""
    success: bool
    total_found: int
    total_added: int
    already_tracked: int
    message: str
    news_ids: List[int] = Field(default_factory=list)


# ==================== 列表查询相关模型 ====================

class ProjectListQuery(BaseModel):
    """项目列表查询参数"""
    status: Optional[ProjectStatus] = None
    project_type: Optional[ProjectType] = None
    priority: Optional[ProjectPriority] = None
    search: Optional[str] = None  # 搜索项目名称或代码
    created_by_me: Optional[bool] = None  # 只看我创建的
    joined_by_me: Optional[bool] = None  # 只看我参与的
    page: int = Field(1, ge=1)
    page_size: int = Field(20, ge=1, le=100)
    sort_by: Optional[str] = Field("created_at", description="排序字段")
    sort_order: Optional[str] = Field("desc", description="排序方向: asc, desc")


class ProjectListResponse(BaseModel):
    """项目列表响应"""
    total: int
    page: int
    page_size: int
    items: List[ProjectResponse]

