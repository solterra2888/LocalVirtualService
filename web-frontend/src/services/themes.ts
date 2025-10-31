import { api } from './api';
import type {
  ThemeFilterRequest,
  NewsFilterResponse,
  TrendingThemesResponse,
  DimensionValuesResponse
} from '../types/themes';

/**
 * 主题相关API服务
 */
export const themesAPI = {
  /**
   * 多维度筛选新闻
   */
  async filterNews(request: ThemeFilterRequest): Promise<NewsFilterResponse> {
    return api.post<NewsFilterResponse>('/themes/filter/news', request);
  },

  /**
   * 获取热门主题
   */
  async getTrendingThemes(
    dimension: string = 'primary_theme',
    days: number = 7,
    topK: number = 10,
    sourceType?: string,
    minConfidence: number = 0.7
  ): Promise<TrendingThemesResponse> {
    return api.get<TrendingThemesResponse>('/themes/trending', {
      dimension,
      days,
      top_k: topK,
      source_type: sourceType,
      min_confidence: minConfidence
    });
  },

  /**
   * 获取指定维度的所有可能值
   */
  async getDimensionValues(
    dimension: string,
    days?: number,
    minCount: number = 1
  ): Promise<DimensionValuesResponse> {
    return api.get<DimensionValuesResponse>('/themes/dimensions', {
      dimension,
      days,
      min_count: minCount
    });
  },

  /**
   * 获取标准分类列表
   */
  async getStandardCategories() {
    return api.get('/themes/categories');
  },

  /**
   * 获取Primary Theme统计
   */
  async getPrimaryThemeStats(days: number = 30) {
    return api.get('/themes/primary/stats', { days });
  }
};
