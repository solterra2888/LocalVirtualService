import { api } from './api';
import {
  NewsItem,
  NewsListResponse,
  NewsTopic,
  NewsSymbol,
  NewsSearchParams,
  FetchNewsParams,
  FetchNewsResponse,
  EODHDTestResponse,
  NewsTheme,
  CreateThemeParams,
  UpdateThemeParams,
  ThemeKeywordParams
} from '../types/news';

// 新闻API服务
export const newsAPI = {
  // 获取新闻列表
  getNewsList: async (params: NewsSearchParams = {}): Promise<NewsListResponse> => {
    const queryParams = new URLSearchParams();
    
    if (params.page) queryParams.append('page', params.page.toString());
    if (params.page_size) queryParams.append('page_size', params.page_size.toString());
    if (params.topic_id) queryParams.append('topic_id', params.topic_id.toString());
    if (params.symbol_code) queryParams.append('symbol_code', params.symbol_code);
    if (params.start_date) queryParams.append('start_date', params.start_date);
    if (params.end_date) queryParams.append('end_date', params.end_date);
    if (params.search_text) queryParams.append('search_text', params.search_text);

    const url = `/news/${queryParams.toString() ? '?' + queryParams.toString() : ''}`;
    return api.get<NewsListResponse>(url);
  },

  // 获取新闻详情
  getNewsDetail: async (newsId: number): Promise<NewsItem> => {
    return api.get<NewsItem>(`/news/${newsId}`);
  },

  // 获取新闻主题列表
  getTopics: async (): Promise<NewsTopic[]> => {
    return api.get<NewsTopic[]>('/news/topics');
  },

  // 获取新闻相关股票代码
  getSymbols: async (): Promise<NewsSymbol[]> => {
    return api.get<NewsSymbol[]>('/news/symbols');
  },

  // 搜索新闻（带参数）
  searchNews: async (searchParams: NewsSearchParams): Promise<NewsListResponse> => {
    return newsAPI.getNewsList(searchParams);
  },

  // 按主题获取新闻
  getNewsByTopic: async (topicId: number, page: number = 1, pageSize: number = 20): Promise<NewsListResponse> => {
    return newsAPI.getNewsList({
      topic_id: topicId,
      page,
      page_size: pageSize
    });
  },

  // 按股票代码获取新闻
  getNewsBySymbol: async (symbolCode: string, page: number = 1, pageSize: number = 20): Promise<NewsListResponse> => {
    return newsAPI.getNewsList({
      symbol_code: symbolCode,
      page,
      page_size: pageSize
    });
  },

  // 按日期范围获取新闻
  getNewsByDateRange: async (
    startDate: string,
    endDate: string,
    page: number = 1,
    pageSize: number = 20
  ): Promise<NewsListResponse> => {
    return newsAPI.getNewsList({
      start_date: startDate,
      end_date: endDate,
      page,
      page_size: pageSize
    });
  },

  // 全文搜索新闻
  searchNewsText: async (
    searchText: string,
    page: number = 1,
    pageSize: number = 20
  ): Promise<NewsListResponse> => {
    return newsAPI.getNewsList({
      search_text: searchText,
      page,
      page_size: pageSize
    });
  },

  // ============ EODHD新闻获取API ============

  // 从EODHD获取新闻
  fetchNewsFromEODHD: async (params: FetchNewsParams): Promise<FetchNewsResponse> => {
    const queryParams = new URLSearchParams();
    
    if (params.symbols) queryParams.append('symbols', params.symbols);
    if (params.topic) queryParams.append('topic', params.topic);
    if (params.start_date) queryParams.append('start_date', params.start_date);
    if (params.end_date) queryParams.append('end_date', params.end_date);
    if (params.max_results) queryParams.append('max_results', params.max_results.toString());
    if (params.topic_filter) queryParams.append('topic_filter', params.topic_filter);
    if (params.keywords) queryParams.append('keywords', params.keywords);

    return api.post<FetchNewsResponse>(`/news/fetch?${queryParams.toString()}`);
  },

  // 测试EODHD API连接
  testEODHDConnection: async (): Promise<EODHDTestResponse> => {
    return api.get<EODHDTestResponse>('/news/test-eodhd');
  },

  // ============ 主题管理API ============

  // 获取所有主题
  getThemes: async (): Promise<{ success: boolean; data: NewsTheme[]; total: number }> => {
    return api.get('/news/themes');
  },

  // 创建新主题
  createTheme: async (params: CreateThemeParams): Promise<{ success: boolean; message: string; data: { theme_id: number } }> => {
    const queryParams = new URLSearchParams();
    
    queryParams.append('topic_name', params.topic_name);
    if (params.description) queryParams.append('description', params.description);
    if (params.keywords) queryParams.append('keywords', params.keywords);
    if (params.color) queryParams.append('color', params.color);

    return api.post(`/news/themes?${queryParams.toString()}`);
  },

  // 更新主题
  updateTheme: async (themeId: number, params: UpdateThemeParams): Promise<{ success: boolean; message: string }> => {
    const queryParams = new URLSearchParams();
    
    if (params.topic_name) queryParams.append('topic_name', params.topic_name);
    if (params.description !== undefined) queryParams.append('description', params.description);
    if (params.is_active !== undefined) queryParams.append('is_active', params.is_active.toString());
    if (params.color) queryParams.append('color', params.color);

    return api.put(`/news/themes/${themeId}?${queryParams.toString()}`);
  },

  // 删除主题
  deleteTheme: async (themeId: number): Promise<{ success: boolean; message: string }> => {
    return api.delete(`/news/themes/${themeId}`);
  },

  // 为主题添加关键词
  addThemeKeywords: async (themeId: number, params: ThemeKeywordParams): Promise<{ success: boolean; message: string }> => {
    const queryParams = new URLSearchParams();
    
    queryParams.append('keywords', params.keywords);
    if (params.weights) queryParams.append('weights', params.weights);

    return api.post(`/news/themes/${themeId}/keywords?${queryParams.toString()}`);
  },

  // 删除主题关键词
  removeThemeKeywords: async (themeId: number, keywords: string): Promise<{ success: boolean; message: string }> => {
    const queryParams = new URLSearchParams();
    queryParams.append('keywords', keywords);

    return api.delete(`/news/themes/${themeId}/keywords?${queryParams.toString()}`);
  },

  // 初始化默认主题
  initializeDefaultThemes: async (): Promise<{ success: boolean; message: string; data: { created_count: number } }> => {
    return api.post('/news/themes/initialize');
  },

  // 重置主题到默认状态
  resetThemeToDefault: async (themeId: number): Promise<{ success: boolean; message: string; data: any }> => {
    return api.post(`/news/themes/${themeId}/reset`);
  },

};

// 新闻工具函数
export const newsUtils = {
  // 格式化日期
  formatNewsDate: (dateString?: string): string => {
    if (!dateString) return '';
    try {
      const date = new Date(dateString);
      return date.toLocaleDateString('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
      });
    } catch {
      return dateString;
    }
  },

  // 获取情感分析标签
  getSentimentLabel: (item: NewsItem): { label: string; color: string } => {
    const polarity = item.sentiment_polarity;
    if (polarity === undefined || polarity === null) {
      return { label: 'Unkown', color: '#d9d9d9' };
    }
    
    if (polarity > 0.1) {
      return { label: 'Positive', color: '#52c41a' };
    } else if (polarity < -0.1) {
      return { label: 'Negative', color: '#ff4d4f' };
    } else {
      return { label: 'Neutral', color: '#faad14' };
    }
  },

  // 获取情感分析分数的颜色
  getSentimentColor: (score?: number): string => {
    if (score === undefined || score === null) return '#d9d9d9';
    if (score > 0.6) return '#52c41a';
    if (score > 0.3) return '#faad14';
    return '#ff4d4f';
  },

  // 截取新闻内容摘要
  getContentSummary: (content?: string, maxLength: number = 150): string => {
    if (!content) return '';
    if (content.length <= maxLength) return content;
    return content.substring(0, maxLength) + '...';
  },

  // 获取主题颜色
  getTopicColor: (topic: NewsTopic): string => {
    return topic.color || '#1890ff';
  },

  // 格式化股票代码显示
  formatSymbolCode: (symbolCode: string): string => {
    return symbolCode.toUpperCase();
  },

  // 创建新闻链接
  createNewsLink: (newsItem: NewsItem): string => {
    return newsItem.link || '#';
  },

  // 检查新闻是否有外部链接
  hasExternalLink: (newsItem: NewsItem): boolean => {
    return !!(newsItem.link && newsItem.link.startsWith('http'));
  },

  // 获取新闻来源显示名称
  getSourceDisplayName: (source?: string): string => {
    if (!source) return '未知来源';
    return source;
  }
};