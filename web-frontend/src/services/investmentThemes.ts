/**
 * Investment Theme Vector Retrieval API Service
 */

import axios from 'axios';
import {
  ThemeMatch,
  ThemeMatchRequest,
  InitializeThemesResponse,
  NewsThemesResponse,
  ThemeListResponse,
  TaskStatusResponse,
} from '../types/investmentThemes';

const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || 'http://localhost:8000/api';

/**
 * Match news to investment themes
 */
export async function matchNewsToThemes(
  request: ThemeMatchRequest
): Promise<ThemeMatch[]> {
  const response = await axios.post<ThemeMatch[]>(
    `${API_BASE_URL}/investment-themes/match`,
    request
  );
  return response.data;
}

/**
 * Initialize investment theme vectors
 */
export async function initializeThemeEmbeddings(
  forceUpdate: boolean = false
): Promise<InitializeThemesResponse> {
  const response = await axios.post<InitializeThemesResponse>(
    `${API_BASE_URL}/investment-themes/initialize`,
    { force_update: forceUpdate }
  );
  return response.data;
}


/**
 * Get matched themes for news
 */
export async function getNewsThemes(newsId: number): Promise<ThemeMatch[]> {
  const response = await axios.get<NewsThemesResponse>(
    `${API_BASE_URL}/investment-themes/news/${newsId}/themes`
  );
  return response.data.themes;
}

/**
 * List all investment themes
 */
export async function listThemes(
  market?: string,
  limit: number = 100,
  offset: number = 0
): Promise<ThemeListResponse> {
  const params: Record<string, any> = { limit, offset };
  if (market) {
    params.market = market;
  }

  const response = await axios.get<ThemeListResponse>(
    `${API_BASE_URL}/investment-themes/themes`,
    { params }
  );
  return response.data;
}

/**
 * Update single theme vector
 */
export async function updateThemeEmbedding(
  themeId: string,
  themeName: string,
  market: string
): Promise<{ success: boolean; message: string }> {
  const response = await axios.put(
    `${API_BASE_URL}/investment-themes/update-theme`,
    {
      theme_id: themeId,
      theme_name: themeName,
      market,
    }
  );
  return response.data;
}

/**
 * Batch fill theme descriptions (using LLM generation - returns Celery task)
 */
export async function fillThemeDescriptions(
  forceRegenerate: boolean = false,
  market?: string
): Promise<InitializeThemesResponse> {
  const response = await axios.post<InitializeThemesResponse>(
    `${API_BASE_URL}/investment-themes/fill-descriptions`,
    {
      force_regenerate: forceRegenerate,
      market,
    }
  );
  return response.data;
}

/**
 * Get Celery task status
 */
export async function getTaskStatus(
  taskId: string
): Promise<TaskStatusResponse> {
  const response = await axios.get<TaskStatusResponse>(
    `${API_BASE_URL}/investment-themes/task-status/${taskId}`
  );
  return response.data;
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

