/**
 * 投资主题向量检索API服务
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
 * 匹配新闻到投资主题
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
 * 初始化投资主题向量
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
 * 获取投资主题统计信息
 */
export async function getThemeStatistics(): Promise<ThemeStatistics> {
  const response = await axios.get<{ success: boolean; statistics: ThemeStatistics }>(
    `${API_BASE_URL}/investment-themes/statistics`
  );
  return response.data.statistics;
}

/**
 * 获取新闻的匹配主题
 */
export async function getNewsThemes(newsId: number): Promise<ThemeMatch[]> {
  const response = await axios.get<NewsThemesResponse>(
    `${API_BASE_URL}/investment-themes/news/${newsId}/themes`
  );
  return response.data.themes;
}

/**
 * 列出所有投资主题
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
 * 更新单个主题向量
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

