/**
 * Investment Project Management API Services
 */

import { api } from './api';
import {
  InvestmentProject,
  ProjectCreateRequest,
  ProjectUpdateRequest,
  ProjectListQuery,
  ProjectListResponse,
  ProjectMember,
  MemberInviteRequest,
  MemberUpdateRequest,
  ProjectStatistics,
  UserProjectPermissions
} from '../types/project';

const API_BASE_URL = '/projects';

// ==================== 项目CRUD ====================

/**
 * 获取项目列表
 */
export async function listProjects(
  query: ProjectListQuery = {}
): Promise<ProjectListResponse> {
  return await api.get<ProjectListResponse>(`${API_BASE_URL}/`, query);
}

/**
 * 创建项目
 */
export async function createProject(
  data: ProjectCreateRequest
): Promise<InvestmentProject> {
  return await api.post<InvestmentProject>(`${API_BASE_URL}/`, data);
}

/**
 * 获取项目详情
 */
export async function getProject(
  projectId: number,
  includeStats: boolean = true
): Promise<InvestmentProject> {
  return await api.get<InvestmentProject>(
    `${API_BASE_URL}/${projectId}`,
    { include_stats: includeStats }
  );
}

/**
 * 更新项目
 */
export async function updateProject(
  projectId: number,
  data: ProjectUpdateRequest
): Promise<InvestmentProject> {
  return await api.put<InvestmentProject>(`${API_BASE_URL}/${projectId}`, data);
}

/**
 * 删除项目（归档）
 */
export async function deleteProject(
  projectId: number
): Promise<{ success: boolean; message: string }> {
  return await api.delete<{ success: boolean; message: string }>(
    `${API_BASE_URL}/${projectId}`
  );
}

// ==================== 项目成员管理 ====================

/**
 * 获取项目成员列表
 */
export async function getProjectMembers(
  projectId: number
): Promise<ProjectMember[]> {
  return await api.get<ProjectMember[]>(`${API_BASE_URL}/${projectId}/members`);
}

/**
 * 邀请成员
 */
export async function inviteMembers(
  projectId: number,
  data: MemberInviteRequest
): Promise<{ success: boolean; message: string; invited_count: number }> {
  return await api.post<{ success: boolean; message: string; invited_count: number }>(
    `${API_BASE_URL}/${projectId}/members/invite`,
    data
  );
}

/**
 * 更新成员信息
 */
export async function updateMember(
  projectId: number,
  userId: number,
  data: MemberUpdateRequest
): Promise<{ success: boolean; message: string }> {
  return await api.put<{ success: boolean; message: string }>(
    `${API_BASE_URL}/${projectId}/members/${userId}`,
    data
  );
}

/**
 * 移除成员
 */
export async function removeMember(
  projectId: number,
  userId: number
): Promise<{ success: boolean; message: string }> {
  return await api.delete<{ success: boolean; message: string }>(
    `${API_BASE_URL}/${projectId}/members/${userId}`
  );
}

// ==================== 项目权限查询 ====================

/**
 * 获取当前用户在项目中的权限
 */
export async function getMyPermissions(
  projectId: number
): Promise<UserProjectPermissions> {
  return await api.get<UserProjectPermissions>(
    `${API_BASE_URL}/${projectId}/permissions`
  );
}

// ==================== 项目统计 ====================

/**
 * 获取项目统计信息
 */
export async function getProjectStatistics(
  projectId: number
): Promise<ProjectStatistics> {
  return await api.get<ProjectStatistics>(
    `${API_BASE_URL}/${projectId}/statistics`
  );
}

// ==================== 辅助函数 ====================

/**
 * 获取项目状态显示文本
 */
export function getProjectStatusText(status: string): string {
  const statusMap: Record<string, string> = {
    active: 'Active',
    paused: 'Paused',
    completed: 'Completed',
    archived: 'Archived'
  };
  return statusMap[status] || status;
}

/**
 * 获取项目类型显示文本
 */
export function getProjectTypeText(type: string): string {
  const typeMap: Record<string, string> = {
    ticker: 'Ticker Research',
    industry: 'Industry Research',
    sector: 'Sector Research'
  };
  return typeMap[type] || type;
}

/**
 * 获取优先级显示文本
 */
export function getPriorityText(priority: number): string {
  const priorityMap: Record<number, string> = {
    1: 'Low',
    2: 'Medium',
    3: 'High'
  };
  return priorityMap[priority] || String(priority);
}

/**
 * 获取成员角色显示文本
 */
export function getMemberRoleText(role: string): string {
  const roleMap: Record<string, string> = {
    owner: 'Owner',
    admin: 'Admin',
    member: 'Member',
    viewer: 'Viewer'
  };
  return roleMap[role] || role;
}

/**
 * 格式化项目标识（tickers/industry/sector）
 */
export function getProjectIdentifier(project: InvestmentProject): string {
  if (project.tickers && project.tickers.length > 0) {
    return project.tickers.join(', ');
  }
  if (project.industry) {
    return project.industry;
  }
  if (project.sector) {
    return project.sector;
  }
  return project.project_code;
}

/**
 * 检查是否是项目负责人
 */
export function isProjectOwner(
  project: InvestmentProject,
  userId: number
): boolean {
  return project.owner_id === userId || project.created_by === userId;
}

