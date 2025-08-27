import { api } from './api';

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
  getCompanyNews: async (ticker: string, params?: { limit?: number }): Promise<DashboardNewsItem[]> => {
    try {
      return await api.get<DashboardNewsItem[]>(`/dashboard/companies/${ticker}/news`, params);
    } catch (error) {
      console.error('获取公司新闻失败:', error);
      throw error;
    }
  },

  // 获取行业相关新闻
  getSectorNews: async (sector: string, params?: { limit?: number }): Promise<DashboardNewsItem[]> => {
    try {
      return await api.get<DashboardNewsItem[]>(`/dashboard/sectors/${sector}/news`, params);
    } catch (error) {
      console.error('获取行业新闻失败:', error);
      throw error;
    }
  },

  // 获取公司AI分析结果
  getCompanyAnalysis: async (ticker: string, params?: { limit?: number }): Promise<DashboardAnalysisResult[]> => {
    try {
      return await api.get<DashboardAnalysisResult[]>(`/dashboard/companies/${ticker}/analysis`, params);
    } catch (error) {
      console.error('获取公司AI分析失败:', error);
      throw error;
    }
  },

  // 获取行业AI分析结果
  getSectorAnalysis: async (sector: string, params?: { limit?: number }): Promise<DashboardAnalysisResult[]> => {
    try {
      return await api.get<DashboardAnalysisResult[]>(`/dashboard/sectors/${sector}/analysis`, params);
    } catch (error) {
      console.error('获取行业AI分析失败:', error);
      throw error;
    }
  },

  // 获取公司相关研报文件
  getCompanyReports: async (ticker: string, params?: { limit?: number }): Promise<DashboardAnalysisResult[]> => {
    try {
      return await api.get<DashboardAnalysisResult[]>(`/dashboard/companies/${ticker}/reports`, params);
    } catch (error) {
      console.error('获取公司研报失败:', error);
      throw error;
    }
  },

  // 获取行业相关研报文件
  getSectorReports: async (sector: string, params?: { limit?: number }): Promise<DashboardAnalysisResult[]> => {
    try {
      return await api.get<DashboardAnalysisResult[]>(`/dashboard/sectors/${sector}/reports`, params);
    } catch (error) {
      console.error('获取行业研报失败:', error);
      throw error;
    }
  },

  // 搜索公司
  searchCompanies: async (query: string): Promise<CompanyInfo[]> => {
    try {
      return await api.get<CompanyInfo[]>('/dashboard/search/companies', { q: query });
    } catch (error) {
      console.error('搜索公司失败:', error);
      throw error;
    }
  },

  // 获取文件内容
  getFileContent: async (fileId: number): Promise<{ content: string; filename: string }> => {
    try {
      return await api.get<{ content: string; filename: string }>(`/dashboard/files/${fileId}/content`);
    } catch (error) {
      console.error('获取文件内容失败:', error);
      throw error;
    }
  },

  // 下载原始文件
  downloadFile: async (fileId: number): Promise<Blob> => {
    try {
      return await api.download(`/dashboard/files/${fileId}/download`);
    } catch (error) {
      console.error('下载文件失败:', error);
      throw error;
    }
  }
};

export default dashboardAPI;
