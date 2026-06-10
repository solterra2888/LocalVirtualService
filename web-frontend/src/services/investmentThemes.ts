/**
 * Investment Theme Vector Retrieval API Service
 */

import { api } from './api';
import {
  ThemeMatch,
  ThemeMatchRequest,
  InitializeThemesResponse,
  NewsThemesResponse,
  ThemeListResponse,
  TaskStatusResponse,
  ThemeAssociationsResponse,
} from '../types/investmentThemes';

/**
 * Match news to investment themes
 */
export async function matchNewsToThemes(
  request: ThemeMatchRequest
): Promise<ThemeMatch[]> {
  return await api.post<ThemeMatch[]>('/investment-themes/match', request);
}

/**
 * Initialize investment theme vectors
 */
export async function initializeThemeEmbeddings(
  forceUpdate: boolean = false
): Promise<InitializeThemesResponse> {
  return await api.post<InitializeThemesResponse>(
    '/investment-themes/initialize',
    { force_update: forceUpdate }
  );
}


/**
 * Get matched themes for news
 */
export async function getNewsThemes(newsId: number): Promise<ThemeMatch[]> {
  const response = await api.get<NewsThemesResponse>(
    `/investment-themes/news/${newsId}/themes`
  );
  return response.themes;
}

/**
 * List all investment themes
 */
export async function listThemes(
  limit: number = 100,
  offset: number = 0,
  search?: string,
  tags?: string[],
  createdBy?: string[]
): Promise<ThemeListResponse> {
  const params: Record<string, any> = { limit, offset };
  if (search !== undefined && search !== null && search !== '') {
    params.search = search;
  }

  // FastAPI 的 Query(List[str]) 需要多个同名参数，如 ?group=AI&group=Market%20Weekly
  // 注意：后端参数名仍为 'group'，但实际用于筛选 theme_tags 表中的 tag_name
  // axios 默认会将数组转换为 group[]=crypto&group[]=market%20weekly 或其他格式
  // 我们需要手动构建 URL 参数来确保格式正确
  const hasTags = tags !== undefined && tags !== null && tags.length > 0;
  const hasUsers = createdBy !== undefined && createdBy !== null && createdBy.length > 0;

  if (hasTags || hasUsers) {
    // 手动构建查询字符串，确保每个 group 和 created_by 都是独立的参数
    const queryParams = new URLSearchParams();
    queryParams.append('limit', limit.toString());
    queryParams.append('offset', offset.toString());
    if (search) {
      queryParams.append('search', search);
    }
    // 为每个 tag 添加一个独立的 'group' 参数（后端参数名仍为 'group'）
    if (hasTags) {
      tags.forEach(tag => {
        queryParams.append('group', tag);
      });
    }
    // 为每个 created_by 添加一个独立的参数（多个同名参数）
    if (hasUsers) {
      createdBy.forEach(user => {
        queryParams.append('created_by', user);
      });
    }

    const url = `/investment-themes/themes?${queryParams.toString()}`;
    // URL 已经包含查询参数，不再传递 params 对象
    return await api.get<ThemeListResponse>(url, undefined);
  }

  return await api.get<ThemeListResponse>('/investment-themes/themes', params);
}

/**
 * Get all unique groups (for filter dropdown)
 */
export async function getAllGroups(): Promise<{
  success: boolean;
  groups: string[];
  count: number;
}> {
  return await api.get<{
    success: boolean;
    groups: string[];
    count: number;
  }>('/investment-themes/groups');
}

/**
 * Get all unique users (for filter dropdown, admin only)
 */
export async function getAllUsers(): Promise<{
  success: boolean;
  users: string[];
  count: number;
}> {
  return await api.get<{
    success: boolean;
    users: string[];
    count: number;
  }>('/investment-themes/users');
}

/**
 * Get theme-company associations for a theme
 */
export async function getThemeAssociations(
  themeId: string
): Promise<ThemeAssociationsResponse> {
  return await api.get<ThemeAssociationsResponse>(
    `/investment-themes/${themeId}/associations`
  );
}

/**
 * Export current user's themes to Excel file
 */
export async function exportThemes(): Promise<Blob> {
  const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || '/api';
  const token = localStorage.getItem('access_token');

  const response = await fetch(`${API_BASE_URL}/investment-themes/export`, {
    method: 'GET',
    headers: {
      ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
    },
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ detail: 'Export failed' }));
    throw new Error(error.detail || 'Export failed');
  }

  return await response.blob();
}

/**
 * Verify ticker for a theme (Admin only)
 * If ticker not in database, fetch from EODHD and save
 */
export async function verifyTicker(
  themeId: string,
  ticker: string
): Promise<{
  success: boolean;
  message: string;
  ticker: string;
  is_verified: boolean;
  company_info?: {
    id?: number;
    code: string;
    name: string;
    exchange: string;
    primaryticker: string;
  };
  data_updated?: Record<string, boolean>;
  execution_time?: number;
}> {
  return await api.post(`/investment-themes/${themeId}/verify-ticker`, {
    theme_id: themeId,
    ticker: ticker
  });
}

/**
 * Update single theme vector
 */
export async function updateThemeEmbedding(
  themeId: string,
  themeName: string
): Promise<{ success: boolean; message: string }> {
  return await api.put('/investment-themes/update-theme', {
    theme_id: themeId,
    theme_name: themeName,
  });
}

/**
 * Batch fill theme descriptions (using LLM generation - returns Celery task)
 */
export async function fillThemeDescriptions(
  forceRegenerate: boolean = false
): Promise<InitializeThemesResponse> {
  return await api.post<InitializeThemesResponse>(
    '/investment-themes/fill-descriptions',
    {
      force_regenerate: forceRegenerate,
    }
  );
}

/**
 * Get Celery task status
 */
export async function getTaskStatus(
  taskId: string
): Promise<TaskStatusResponse> {
  return await api.get<TaskStatusResponse>(`/investment-themes/task-status/${taskId}`);
}

/**
 * Poll task until completion
 */
export async function pollTaskUntilComplete(
  taskId: string,
  onProgress?: (status: TaskStatusResponse) => void,
  pollInterval: number = 2000,
  timeout: number = 600000 // 10 minutes
): Promise<TaskStatusResponse> {
  const startTime = Date.now();
  
  while (true) {
    const status = await getTaskStatus(taskId);
    
    console.log(`🔄 Task ${taskId} status: ${status.status}`, status);
    
    // Notify progress
    if (onProgress) {
      onProgress(status);
    }
    
    // Check if completed
    if (status.status === 'SUCCESS' || status.status === 'FAILURE') {
      console.log(`✅ Task ${taskId} completed with status: ${status.status}`);
      return status;
    }
    
    // Check timeout
    if (Date.now() - startTime > timeout) {
      console.error(`❌ Task ${taskId} timeout after ${timeout}ms`);
      throw new Error('Task timeout');
    }
    
    // Wait before next poll
    await new Promise(resolve => setTimeout(resolve, pollInterval));
  }
}

/**
 * Update theme enabled/disabled status (Admin only)
 */
export async function updateThemeEnabledStatus(
  themeId: string,
  isEnabled: boolean
): Promise<{ success: boolean; message: string }> {
  return await api.patch(`/investment-themes/${themeId}/enabled`, {
    is_enabled: isEnabled,
  });
}

/**
 * Batch update theme enabled/disabled status (Admin only)
 */
export async function batchUpdateThemeEnabledStatus(
  themeIds: string[],
  isEnabled: boolean
): Promise<{
  success: boolean;
  total: number;
  success_count: number;
  failed_count: number;
  results: Array<{
    theme_id: string;
    success: boolean;
    message: string;
  }>;
}> {
  return await api.post('/investment-themes/batch-enabled', {
    theme_ids: themeIds,
    is_enabled: isEnabled,
  });
}

/**
 * Update theme description (Admin only)
 */
export async function updateThemeDescription(
  themeId: string,
  description: string,
  version?: number,
  group?: string
): Promise<{
  success: boolean;
  message: string;
  description_saved: boolean;
  embedding_updated: boolean;
  error?: string;
}> {
  return await api.patch(`/investment-themes/${themeId}/description`, {
    description,
    group,
    version,
  });
}

/**
 * Delete theme (Admin only, hard delete)
 */
export async function deleteTheme(
  themeId: string
): Promise<{
  success: boolean;
  message: string;
  deleted_matches_count: number;
  deleted_embeddings: boolean;
}> {
  return await api.delete(`/investment-themes/${themeId}`);
}

/**
 * Batch delete themes (Admin only, hard delete)
 */
export async function batchDeleteThemes(
  themeIds: string[]
): Promise<{
  success: boolean;
  message: string;
  deleted_count: number;
  failed_count: number;
  total_matches_deleted: number;
  details: Array<{
    theme_id: string;
    theme_name?: string;
    success: boolean;
    matches_deleted?: number;
    message: string;
  }>;
}> {
  return await api.post('/investment-themes/batch-delete', { theme_ids: themeIds });
}

/**
 * Generate theme description using LLM (Admin only)
 */
export async function generateThemeDescription(
  themeName: string,
  category: string,
  group?: string,
  market?: string,
  tickers?: string[]
): Promise<{
  success: boolean;
  description: string;
  message: string;
  error?: string;
}> {
  return await api.post('/investment-themes/generate-description', {
    group: group,
    theme_name: themeName,
    category: category,
    market: market,
    tickers: tickers,
  });
}

/**
 * Create new theme (Admin only)
 */
export async function createTheme(
  themeName: string,
  category: string,
  description: string,
  group?: string,
  themeId?: string,
  market?: string,
  tickers?: string[]
): Promise<{
  success: boolean;
  message: string;
  theme_id: string;
  theme_saved: boolean;
  embedding_created: boolean;
  associations_created?: number;
  conflict?: boolean;
  existing_theme?: {
    db_theme_id: number;
    theme_id: string;
    theme_name: string;
    category: string;
    description: string;
    group?: string;
    created_by: string;
    tickers: string[];
  };
  new_tickers?: string[];
  error?: string;
}> {
  return await api.post('/investment-themes/create', {
    group: group,
    theme_name: themeName,
    category: category,
    description,
    theme_id: themeId,
    market: market,
    tickers: tickers,
  });
}

/**
 * Update theme with conflict resolution
 * Called when user resolves a theme conflict by selecting tickers
 */
export async function updateThemeWithConflictResolution(
  themeId: string,
  themeName: string,
  category: string,
  description: string,
  selectedTickers: string[],
  group?: string
): Promise<{
  success: boolean;
  message: string;
  theme_id: string;
  theme_saved: boolean;
  embedding_created: boolean;
  associations_updated?: number;
  error?: string;
}> {
  return await api.post(`/investment-themes/${themeId}/update-with-conflict-resolution`, {
    theme_name: themeName,
    category: category,
    description: description,
    selected_tickers: selectedTickers,
    group: group,
  });
}

/**
 * Batch resolve theme conflicts
 * Submit all conflict resolutions at once and process in Celery
 */
export async function batchResolveConflicts(
  conflictResolutions: Record<string, {
    theme_name: string;
    category: string;
    description: string;
    selected_tickers: string[];
    group?: string;
  }>
): Promise<{
  success: boolean;
  message: string;
  task_id: string;
}> {
  return await api.post('/investment-themes/batch-resolve-conflicts', {
    conflict_resolutions: conflictResolutions
  });
}

/**
 * Update theme tickers and regenerate description/vector
 */
export async function updateThemeTickers(
  themeId: string,
  tickers: string[],
  themeName: string,
  category: string,
  group?: string,
  market?: string
): Promise<{
  success: boolean;
  message: string;
  theme_id: string;
  description_updated: boolean;
  embedding_updated: boolean;
  old_associations_deleted: number;
  new_associations_created: number;
}> {
  return await api.post('/investment-themes/update-theme-tickers', {
    theme_id: themeId,
    tickers: tickers,
    theme_name: themeName,
    category: category,
    group: group,
    market: market,
  });
}

/**
 * Batch upload themes from CSV/Excel file (Admin only)
 */
export async function batchUploadThemes(
  file: File,
  group?: string,
  market: string = 'US'
): Promise<{
  success: boolean;
  message: string;
  task_id: string;
  theme_names: string[];
  preview: {
    theme_names: string[];
    total_themes: number;
    total_tickers: number;
    themes: Record<string, { categories: Record<string, number>; total_tickers: number }>;
    categories?: Record<string, number>;
    categories_sample?: string[];
  };
  status_url: string;
}> {
  const formData = new FormData();
  formData.append('file', file);
  if (group) {
    formData.append('group', group);
  }
  formData.append('market', market);
  
  // 使用 fetch 直接调用，因为需要特殊处理 multipart/form-data
  const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || '/api';
  const token = localStorage.getItem('access_token');
  
  const response = await fetch(`${API_BASE_URL}/investment-themes/batch-upload`, {
    method: 'POST',
    headers: {
      ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
    },
    body: formData,
  });
  
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.detail?.message || error.detail || 'Batch upload failed');
  }
  
  return await response.json();
}

/**
 * Get available tags and their counts
 */
export async function getAvailableTags(): Promise<{
  success: boolean;
  tags: Array<{ tag_name: string; count: number }>;
}> {
  return await api.get('/investment-themes/available-tags');
}

/**
 * Get tags for a specific theme
 */
export async function getThemeTags(themeId: string): Promise<{
  success: boolean;
  theme_id: string;
  tags: Array<{ tag_name: string; created_at?: string; created_by?: string }>;
}> {
  return await api.get(`/investment-themes/${themeId}/tags`);
}

/**
 * Batch add or remove tags for themes
 */
export async function batchTagThemes(
  themeIds: string[],
  tags: string[],
  action: 'add' | 'remove'
): Promise<{
  success: boolean;
  message: string;
  affected_count: number;
}> {
  return await api.post('/investment-themes/batch-tag', {
    theme_ids: themeIds,
    tags,
    action,
  });
}

/**
 * Add a single tag to a theme
 */
export async function addTagToTheme(
  themeId: string,
  tagName: string
): Promise<{
  success: boolean;
  message: string;
  theme_id: string;
  tag_name: string;
}> {
  return await api.post(`/investment-themes/${themeId}/tags`, {
    tag_name: tagName,
  });
}

/**
 * Remove a single tag from a theme
 */
export async function removeTagFromTheme(
  themeId: string,
  tagName: string
): Promise<{
  success: boolean;
  message: string;
  theme_id: string;
  tag_name: string;
}> {
  return await api.delete(`/investment-themes/${themeId}/tags/${tagName}`);
}

