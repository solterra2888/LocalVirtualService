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
  offset: number = 0
): Promise<ThemeListResponse> {
  const params: Record<string, any> = { limit, offset };

  return await api.get<ThemeListResponse>('/investment-themes/themes', params);
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
    
    // Notify progress
    if (onProgress) {
      onProgress(status);
    }
    
    // Check if completed
    if (status.status === 'SUCCESS' || status.status === 'FAILURE') {
      return status;
    }
    
    // Check timeout
    if (Date.now() - startTime > timeout) {
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
  version?: number
): Promise<{
  success: boolean;
  message: string;
  description_saved: boolean;
  embedding_updated: boolean;
  error?: string;
}> {
  return await api.patch(`/investment-themes/${themeId}/description`, {
    description,
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
  themeName: string
): Promise<{
  success: boolean;
  description: string;
  message: string;
  error?: string;
}> {
  return await api.post('/investment-themes/generate-description', {
    theme_name: themeName,
  });
}

/**
 * Create new theme (Admin only)
 */
export async function createTheme(
  themeName: string,
  description: string,
  themeId?: string
): Promise<{
  success: boolean;
  message: string;
  theme_id: string;
  theme_saved: boolean;
  embedding_created: boolean;
  error?: string;
}> {
  return await api.post('/investment-themes/create', {
    theme_name: themeName,
    description,
    theme_id: themeId,
  });
}

