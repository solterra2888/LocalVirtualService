/**
 * Investment Theme Vector Retrieval API Service
 */

import axios from 'axios';
import {
  ThemeMatch,
  ThemeMatchRequest,
  ThemeStatistics,
  InitializeThemesResponse,
  NewsThemesResponse,
  ThemeListResponse,
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
 * Get investment theme statistics
 */
export async function getThemeStatistics(): Promise<ThemeStatistics> {
  const response = await axios.get<{ success: boolean; statistics: ThemeStatistics }>(
    `${API_BASE_URL}/investment-themes/statistics`
  );
  return response.data.statistics;
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
 * Batch fill theme descriptions (using LLM generation)
 */
export async function fillThemeDescriptions(
  forceRegenerate: boolean = false,
  market?: string
): Promise<{ success: boolean; message: string; statistics: any }> {
  const response = await axios.post(
    `${API_BASE_URL}/investment-themes/fill-descriptions`,
    {
      force_regenerate: forceRegenerate,
      market,
    }
  );
  return response.data;
}

