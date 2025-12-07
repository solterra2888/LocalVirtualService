/**
 * Investment Project Management Type Definitions
 */

// ==================== 枚举类型 ====================

export enum ProjectType {
  TICKER = 'ticker',
  INDUSTRY = 'industry',
  SECTOR = 'sector'
}

export enum ProjectStatus {
  ACTIVE = 'active',
  PAUSED = 'paused',
  COMPLETED = 'completed',
  ARCHIVED = 'archived'
}

export enum ProjectPriority {
  LOW = 1,
  MEDIUM = 2,
  HIGH = 3
}

export enum MemberRole {
  OWNER = 'owner',
  ADMIN = 'admin',
  MEMBER = 'member',
  VIEWER = 'viewer'
}

export enum MemberStatus {
  ACTIVE = 'active',
  INACTIVE = 'inactive',
  INVITED = 'invited'
}

export enum MilestoneType {
  MEETING_NOTES = 'meeting_notes',
  RESEARCH_REPORT = 'research_report',
  DECISION = 'decision',
  UPDATE = 'update',
  OTHER = 'other'
}

export enum DecisionType {
  BUY = 'buy',
  SELL = 'sell',
  HOLD = 'hold',
  WATCH = 'watch',
  PASS = 'pass'
}

export enum FileCategory {
  MEETING_NOTES = 'meeting_notes',
  REPORT = 'report',
  CONTRACT = 'contract',
  PRESENTATION = 'presentation',
  OTHER = 'other'
}

// ==================== 投资项目相关类型 ====================

export interface InvestmentProject {
  id: number;
  project_code: string;
  project_name: string;
  project_type: ProjectType;
  tickers?: string[]; // 支持多个ticker，不分主次
  industry?: string;
  sector?: string;
  region?: string;
  status: ProjectStatus;
  priority: ProjectPriority;
  description?: string;
  current_position?: string;
  created_by: number;
  owner_id: number;
  start_date?: string;
  created_at: string;
  updated_at: string;
  
  // 扩展信息（从统计查询获取）
  member_count?: number;
  news_count?: number;
  file_count?: number;
  milestone_count?: number;
}

export interface ProjectCreateRequest {
  project_name: string;
  project_type: ProjectType;
  tickers?: string[]; // 支持多个ticker
  industry?: string;
  sector?: string;
  region?: string;
  status?: ProjectStatus;
  priority?: ProjectPriority;
  description?: string;
  current_position?: string;
  start_date?: string;
}

export interface ProjectUpdateRequest {
  project_name?: string;
  status?: ProjectStatus;
  priority?: ProjectPriority;
  tickers?: string[]; // 支持更新多个ticker
  description?: string;
  current_position?: string;
}

export interface ProjectListQuery {
  status?: ProjectStatus;
  project_type?: ProjectType;
  priority?: ProjectPriority;
  search?: string;
  created_by_me?: boolean;
  joined_by_me?: boolean;
  page?: number;
  page_size?: number;
  sort_by?: string;
  sort_order?: 'asc' | 'desc';
}

export interface ProjectListResponse {
  total: number;
  page: number;
  page_size: number;
  items: InvestmentProject[];
}

// ==================== 项目成员相关类型 ====================

export interface ProjectMember {
  id: number;
  project_id: number;
  user_id: number;
  username?: string;
  role: MemberRole;
  can_edit: boolean;
  can_invite: boolean;
  status: MemberStatus;
  invited_by?: number;
  invited_at?: string;
  joined_at: string;
  created_at: string;
  updated_at: string;
}

export interface MemberInviteRequest {
  user_ids: number[];
  role: MemberRole;
  can_edit: boolean;
  can_invite: boolean;
  message?: string;
}

export interface MemberUpdateRequest {
  role?: MemberRole;
  can_edit?: boolean;
  can_invite?: boolean;
  status?: MemberStatus;
}

// ==================== 项目里程碑相关类型 ====================

export interface ProjectMilestone {
  id: number;
  project_id: number;
  milestone_type: MilestoneType;
  title: string;
  content?: string;
  file_ids?: number[];
  decision_type?: DecisionType;
  decision_rationale?: string;
  milestone_date?: string;
  created_by: number;
  created_by_username?: string;
  created_at: string;
  updated_at: string;
}

export interface MilestoneCreateRequest {
  milestone_type: MilestoneType;
  title: string;
  content?: string;
  file_ids?: number[];
  decision_type?: DecisionType;
  decision_rationale?: string;
  milestone_date?: string;
}

export interface MilestoneUpdateRequest {
  milestone_type?: MilestoneType;
  title?: string;
  content?: string;
  file_ids?: number[];
  decision_type?: DecisionType;
  decision_rationale?: string;
  milestone_date?: string;
}

// ==================== 项目新闻跟踪相关类型 ====================

export interface ProjectNewsTracking {
  id: number;
  project_id: number;
  news_id: number;
  relevance_score?: number;
  is_important: boolean;
  user_notes?: string;
  added_by: number;
  added_by_username?: string;
  added_at: string;
  updated_at: string;
  
  // 新闻详情（关联查询）
  news_title?: string;
  news_date?: string;
  news_source?: string;
}

export interface NewsTrackingCreateRequest {
  news_id: number;
  relevance_score?: number;
  is_important?: boolean;
  user_notes?: string;
}

export interface NewsTrackingUpdateRequest {
  relevance_score?: number;
  is_important?: boolean;
  user_notes?: string;
}

export interface AutoFetchNewsRequest {
  recent_days?: number;
  auto_analyze?: boolean;
  min_relevance?: number;
  limit?: number;
}

export interface AutoFetchNewsResponse {
  success: boolean;
  total_found: number;
  total_added: number;
  already_tracked: number;
  message: string;
  news_ids: number[];
}

// ==================== 项目文件相关类型 ====================

export interface ProjectFile {
  id: number;
  project_id: number;
  file_name: string;
  file_path: string;
  file_type?: string;
  file_size?: number;
  category?: FileCategory;
  description?: string;
  milestone_id?: number;
  uploaded_by: number;
  uploaded_by_username?: string;
  uploaded_at: string;
}

export interface ProjectFileCreateRequest {
  file_name: string;
  file_path: string;
  file_type?: string;
  file_size?: number;
  category?: FileCategory;
  description?: string;
  milestone_id?: number;
}

// ==================== 项目统计和概览相关类型 ====================

export interface ProjectStatistics {
  total_members: number;
  active_members: number;
  total_news: number;
  important_news: number;
  total_files: number;
  total_milestones: number;
  latest_milestone_date?: string;
  latest_news_added_at?: string;
}

export interface ProjectActivity {
  activity_type: string;
  description: string;
  user_id: number;
  username?: string;
  created_at: string;
}

export interface ProjectDashboard {
  project: InvestmentProject;
  statistics: ProjectStatistics;
  recent_activities: ProjectActivity[];
  ticker_performance?: any;
  upcoming_events: any[];
}

// ==================== 权限相关类型 ====================

export interface ProjectPermissions {
  can_view: boolean;
  can_edit: boolean;
  can_invite: boolean;
  can_manage: boolean;
  can_delete: boolean;
  can_transfer: boolean;
}

export interface UserProjectPermissions {
  role?: MemberRole;
  permissions: ProjectPermissions;
}

// ==================== 辅助类型 ====================

export interface ProjectCardData extends InvestmentProject {
  // 用于展示的额外字段
  is_owner?: boolean;
  my_role?: MemberRole;
}

// 标签选项
export interface ProjectStatusOption {
  value: ProjectStatus;
  label: string;
  color: string;
}

export interface ProjectTypeOption {
  value: ProjectType;
  label: string;
  icon: string;
}

export interface ProjectPriorityOption {
  value: ProjectPriority;
  label: string;
  color: string;
}

// 常量定义
export const PROJECT_STATUS_OPTIONS: ProjectStatusOption[] = [
  { value: ProjectStatus.ACTIVE, label: 'Active', color: 'success' },
  { value: ProjectStatus.PAUSED, label: 'Paused', color: 'warning' },
  { value: ProjectStatus.COMPLETED, label: 'Completed', color: 'default' },
  { value: ProjectStatus.ARCHIVED, label: 'Archived', color: 'error' }
];

export const PROJECT_TYPE_OPTIONS: ProjectTypeOption[] = [
  { value: ProjectType.TICKER, label: 'Ticker Research', icon: '📊' },
  { value: ProjectType.INDUSTRY, label: 'Industry Research', icon: '🏭' },
  { value: ProjectType.SECTOR, label: 'Sector Research', icon: '📈' }
];

export const PROJECT_PRIORITY_OPTIONS: ProjectPriorityOption[] = [
  { value: ProjectPriority.HIGH, label: 'High', color: 'error' },
  { value: ProjectPriority.MEDIUM, label: 'Medium', color: 'warning' },
  { value: ProjectPriority.LOW, label: 'Low', color: 'default' }
];

export const MEMBER_ROLE_OPTIONS = [
  { value: MemberRole.OWNER, label: 'Owner', description: 'All permissions' },
  { value: MemberRole.ADMIN, label: 'Admin', description: 'Can manage events and members' },
  { value: MemberRole.MEMBER, label: 'Member', description: 'Can edit content' },
  { value: MemberRole.VIEWER, label: 'Viewer', description: 'View only' }
];

export const MILESTONE_TYPE_OPTIONS = [
  { value: MilestoneType.MEETING_NOTES, label: 'Meeting Notes', icon: '📝' },
  { value: MilestoneType.RESEARCH_REPORT, label: 'Research Report', icon: '📊' },
  { value: MilestoneType.DECISION, label: 'Investment Decision', icon: '✅' },
  { value: MilestoneType.UPDATE, label: 'Progress Update', icon: '🔄' },
  { value: MilestoneType.OTHER, label: 'Other', icon: '📌' }
];

export const DECISION_TYPE_OPTIONS = [
  { value: DecisionType.BUY, label: 'Buy', color: 'success' },
  { value: DecisionType.SELL, label: 'Sell', color: 'error' },
  { value: DecisionType.HOLD, label: 'Hold', color: 'primary' },
  { value: DecisionType.WATCH, label: 'Watch', color: 'warning' },
  { value: DecisionType.PASS, label: 'Pass', color: 'default' }
];

