/**
 * Unified Theme System API Service
 * 统一主题系统API服务
 */

import { api } from './api';
import type {
  FusedTheme,
  TrendingThemesResponse,
  ThemeMasterData
} from '../types/unifiedThemes';

const BASE_URL = '/themes-unified';

export interface ThemeSyncResult {
  success: boolean;
  synced_count: number;
  failed_count: number;
  details: Record<string, number>;
  errors: string[];
}

export interface FusedThemesResponse {
  news_id: number;
  themes: FusedTheme[];
  total_sources: number;
  generated_at: string;
}

/**
 * 获取trending主题列表
 */
export const getTrendingThemes = async (params?: {
  days?: number;
  min_news_count?: number;
  limit?: number;
}): Promise<TrendingThemesResponse> => {
  const response = await api.get(`${BASE_URL}/trending`, { params });
  return response.data;
};

/**
 * 获取指定新闻的融合主题
 */
export const getNewsFusedThemes = async (
  newsId: number,
  params?: {
    min_confidence?: number;
    top_n?: number;
  }
): Promise<FusedThemesResponse> => {
  const response = await api.get(`${BASE_URL}/news/${newsId}/fused-themes`, { params });
  return response.data;
};

/**
 * 批量获取多条新闻的融合主题
 */
export const getBatchNewsFusedThemes = async (
  newsIds: number[],
  params?: {
    min_confidence?: number;
    top_n?: number;
  }
): Promise<Record<number, FusedThemesResponse>> => {
  const response = await api.post(`${BASE_URL}/news/fused-themes/batch`, {
    news_ids: newsIds,
    ...params
  });
  return response.data;
};

/**
 * 触发最近N天的新闻主题同步
 */
export const syncRecentNews = async (days: number = 7): Promise<ThemeSyncResult> => {
  const response = await api.post(`${BASE_URL}/sync/recent?days=${days}`);
  return response.data;
};

/**
 * 获取主题列表（master data）
 */
export const listMasterThemes = async (params?: {
  is_active?: boolean;
  category?: string;
  limit?: number;
  offset?: number;
}): Promise<{ items: ThemeMasterData[]; total: number }> => {
  const response = await api.get(`${BASE_URL}/master-themes`, { params });
  return response.data;
};

/**
 * 搜索主题
 */
export const searchThemes = async (query: string): Promise<string[]> => {
  const response = await api.get(`${BASE_URL}/search`, {
    params: { query, limit: 20 }
  });
  return response.data.themes || [];
};

const unifiedThemesAPI = {
  getTrendingThemes,
  getNewsFusedThemes,
  getBatchNewsFusedThemes,
  syncRecentNews,
  listMasterThemes,
  searchThemes
};

export default unifiedThemesAPI;

