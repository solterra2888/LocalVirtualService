import { api } from './api';
import type { NewsAnalysisResult } from '../types/news';

// 类型定义
export interface CompanyInfo {
  id: number;
  companyname: string;
  primaryticker: string;
  sector: string;
  industry: string;
  gicsector?: string;
  gicindustry?: string;
  gicsubindustry?: string;
  market_cap?: string;
  price?: number;
  country?: string;
}

export interface SectorInfo {
  sector: string;
  company_count: number;
  companies: CompanyInfo[];
}

export interface DashboardNewsItem {
  id: number;
  title: string;
  content?: string;
  date?: string;
  source?: string;
  link?: string;
  sentiment_polarity?: number;
  sentiment_pos?: number;
  sentiment_neg?: number;
  sentiment_neu?: number;
  symbols: string[];
  tags: string[];
  scheduler_source?: string;  // 调度器来源
  updated_at?: string;  // 更新时间
  analysis_result?: NewsAnalysisResult;
}

export interface DashboardAnalysisResult {
  id: number;
  filename: string;
  content: string;
  created_at: string;
  file_type: string;
  status: string;
  total_tokens: number;
  cost: number;
  task_id?: string;
}

export interface DashboardSearchParams {
  sector?: string;
  ticker?: string;
  limit?: number;
  offset?: number;
}

// Watchlist相关类型定义
export interface WatchlistItem {
  watchlist_id: number;
  symbol: string;
  custom_name?: string;
  priority: number;
  sort_order?: number;
  notes?: string;
  added_at?: string;
  primaryticker?: string;
  company?: string;
  sector?: string;
  industry?: string;
  exchange?: string;
  company_id?: number;
}

export interface WatchlistGroup {
  group_name: string;
  items: WatchlistItem[];
}

export interface StockSearchResult {
  ticker: string;
  company: string;
  sector: string;
  industry: string;
  exchange?: string;
  company_id?: number;
}

export interface ResearchReport {
  id: number;
  file_name: string;
  file_path: string;
  file_size: number;
  file_type: string;
  uploaded_at: string;
  status: string;
  sector: string;
  industry: string;
  ticker: string;
  tagged_at: string;
}

export interface StockPriceData {
  code: string;
  open?: number;
  high?: number;
  low?: number;
  close?: number;
  volume?: number;
  previous_close?: number;
  change_amount?: number;
  change_percent?: number;
  last_updated?: string;
  updated_at?: string;
}

// API服务
export const dashboardAPI = {
  // 获取所有行业信息
  getSectors: async (): Promise<SectorInfo[]> => {
    try {
      return await api.get<SectorInfo[]>('/dashboard/sectors');
    } catch (error) {
      console.error('获取行业信息失败:', error);
      throw error;
    }
  },

  // 获取公司信息
  getCompanies: async (params?: { sector?: string; search?: string }): Promise<CompanyInfo[]> => {
    try {
      return await api.get<CompanyInfo[]>('/dashboard/companies', params);
    } catch (error) {
      console.error('获取公司信息失败:', error);
      throw error;
    }
  },

  // 获取特定公司信息
  getCompanyInfo: async (ticker: string): Promise<CompanyInfo> => {
    try {
      return await api.get<CompanyInfo>(`/dashboard/companies/${ticker}`);
    } catch (error) {
      console.error('获取公司详情失败:', error);
      throw error;
    }
  },

  // 获取公司相关新闻
  getCompanyNews: async (ticker: string, params?: { limit?: number; offset?: number }): Promise<DashboardNewsItem[]> => {
    try {
      return await api.get<DashboardNewsItem[]>(`/dashboard/companies/${ticker}/news`, params);
    } catch (error) {
      console.error('获取公司新闻失败:', error);
      throw error;
    }
  },

  // 获取行业相关新闻
  getSectorNews: async (sector: string, params?: { limit?: number; offset?: number }): Promise<DashboardNewsItem[]> => {
    try {
      return await api.get<DashboardNewsItem[]>(`/dashboard/sectors/${sector}/news`, params);
    } catch (error) {
      console.error('Failed to get sector news:', error);
      throw error;
    }
  },

  // 获取公司AI分析结果
  getCompanyAnalysis: async (ticker: string, params?: { limit?: number }): Promise<DashboardAnalysisResult[]> => {
    try {
      return await api.get<DashboardAnalysisResult[]>(`/dashboard/companies/${ticker}/analysis`, params);
    } catch (error) {
      console.error('Failed to get company analysis:', error);
      throw error;
    }
  },

  // 获取行业AI分析结果
  getSectorAnalysis: async (sector: string, params?: { limit?: number }): Promise<DashboardAnalysisResult[]> => {
    try {
      return await api.get<DashboardAnalysisResult[]>(`/dashboard/sectors/${sector}/analysis`, params);
    } catch (error) {
      console.error('Failed to get sector analysis:', error);
      throw error;
    }
  },

  // 获取公司相关研报文件
  getCompanyReports: async (ticker: string, params?: { limit?: number }): Promise<DashboardAnalysisResult[]> => {
    try {
      return await api.get<DashboardAnalysisResult[]>(`/dashboard/companies/${ticker}/reports`, params);
    } catch (error) {
      console.error('Failed to get company reports:', error);
      throw error;
    }
  },

  // 获取行业相关研报文件
  getSectorReports: async (sector: string, params?: { limit?: number }): Promise<DashboardAnalysisResult[]> => {
    try {
      return await api.get<DashboardAnalysisResult[]>(`/dashboard/sectors/${sector}/reports`, params);
    } catch (error) {
      console.error('Failed to get sector reports:', error);
      throw error;
    }
  },

  // 搜索公司
  searchCompanies: async (query: string): Promise<CompanyInfo[]> => {
    try {
      return await api.get<CompanyInfo[]>('/dashboard/search/companies', { q: query });
    } catch (error) {
      console.error('Failed to search companies:', error);
      throw error;
    }
  },

  // 获取文件内容
  getFileContent: async (fileId: number): Promise<{ content: string; filename: string }> => {
    try {
      return await api.get<{ content: string; filename: string }>(`/dashboard/files/${fileId}/content`);
    } catch (error) {
      console.error('Failed to get file content:', error);
      throw error;
    }
  },

  // 下载原始文件
  downloadFile: async (fileId: number): Promise<Blob> => {
    try {
      return await api.download(`/dashboard/files/${fileId}/download`);
    } catch (error) {
      console.error('Failed to download file:', error);
      throw error;
    }
  },

  // === Watchlist相关API ===

  // 获取用户的watchlist
  getUserWatchlist: async (): Promise<WatchlistGroup[]> => {
    try {
      return await api.get<WatchlistGroup[]>('/dashboard/watchlist');
    } catch (error) {
      console.error('Failed to get user watchlist:', error);
      throw error;
    }
  },

  // 添加股票到watchlist
  addToWatchlist: async (params: {
    symbol: string;
    name?: string;
    group_name?: string;
    priority?: number;
    notes?: string;
  }): Promise<{ message: string; watchlist_id: number; symbol: string; company_name: string }> => {
    try {
      return await api.post('/dashboard/watchlist', params);
    } catch (error) {
      console.error('Failed to add to watchlist:', error);
      throw error;
    }
  },

  // 更新watchlist项
  updateWatchlistItem: async (watchlistId: number, params: {
    priority?: number;
    notes?: string;
  }): Promise<{ message: string; watchlist_id: number; symbol: string; priority: number; notes?: string }> => {
    try {
      return await api.put(`/dashboard/watchlist/${watchlistId}`, params);
    } catch (error) {
      console.error('Failed to update watchlist item:', error);
      throw error;
    }
  },

  // 从watchlist移除股票
  removeFromWatchlist: async (watchlistId: number): Promise<{ message: string; symbol: string }> => {
    try {
      return await api.delete(`/dashboard/watchlist/${watchlistId}`);
    } catch (error) {
      console.error('Failed to remove from watchlist:', error);
      throw error;
    }
  },

  // 调整watchlist排序
  reorderWatchlist: async (items: Array<{ watchlist_id: number; sort_order: number; group_name?: string | null }>): Promise<{
    message: string;
    updated: number;
  }> => {
    try {
      return await api.put('/dashboard/watchlist/reorder', { items });
    } catch (error) {
      console.error('Failed to reorder watchlist:', error);
      throw error;
    }
  },

  // 搜索股票（用于添加到watchlist）
  searchStocks: async (query: string, limit?: number): Promise<StockSearchResult[]> => {
    try {
      return await api.get<StockSearchResult[]>('/dashboard/search/stocks', { 
        q: query, 
        limit: limit || 20 
      });
    } catch (error) {
      console.error('Failed to search stocks:', error);
      throw error;
    }
  },

  // 获取ticker关联的研究报告
  getTickerResearchReports: async (ticker: string): Promise<{
    ticker: string;
    count: number;
    reports: ResearchReport[];
  }> => {
    try {
      return await api.get(`/dashboard/watchlist/${ticker}/research-reports`);
    } catch (error) {
      console.error('Failed to get research reports:', error);
      throw error;
    }
  },

  // 获取股票价格数据
  getStockPrices: async (symbols: string[]): Promise<{
    success: boolean;
    data: StockPriceData[];
    count: number;
  }> => {
    try {
      const symbolsStr = symbols.join(',');
      return await api.get('/dashboard/stock-prices', { symbols: symbolsStr });
    } catch (error) {
      console.error('Failed to get stock prices:', error);
      throw error;
    }
  },

  // 获取watchlist股票的价格数据
  getWatchlistStockPrices: async (): Promise<{
    success: boolean;
    data: StockPriceData[];
    count: number;
    message?: string;
  }> => {
    try {
      return await api.get('/dashboard/watchlist/prices');
    } catch (error) {
      console.error('Failed to get watchlist stock prices:', error);
      throw error;
    }
  },

  // 获取股票历史数据用于蜡烛图
  getStockHistoricalData: async (symbol: string, days: number = 365): Promise<{
    success: boolean;
    data: Array<{
      date: string;
      open: number;
      high: number;
      low: number;
      close: number;
      volume: number;
    }>;
    count: number;
    symbol: string;
    date_range: {
      start: string;
      end: string;
    };
  }> => {
    try {
      // 使用正式端点，已修复period='d'过滤，确保只返回日数据
      return await api.get(`/dashboard/stock-historical-data/${symbol}`, { days });
    } catch (error) {
      console.error('Failed to get stock historical data:', error);
      throw error;
    }
  }
};

export default dashboardAPI;
