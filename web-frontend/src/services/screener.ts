import api from './api';

// 筛选器配置接口
export interface ScreenerFilters {
  exchange?: string;
  marketCap?: string;
  earningsDate?: string;
  price?: string;
  index?: string;
  dividendYield?: string;
  averageVolume?: string;
  targetPrice?: string;
  sector?: string;
  shortFloat?: string;
  relativeVolume?: string;
  ipoDate?: string;
  industry?: string;
  analystRecom?: string;
  currentVolume?: string;
  sharesOutstanding?: string;
  country?: string;
  optionShort?: string;
  trades?: string;
  float?: string;
}

// 股票数据接口
export interface ScreenerStockData {
  id: number;
  ticker: string;
  company: string;
  index_name?: string;
  sector: string;
  industry: string;
  country: string;
  exchange?: string;
  market_cap: number | null;
  pe_ratio: number | null;
  forward_pe: number | null;
  peg: number | null;
  ps_ratio: number | null;
  pb_ratio: number | null;
  pcash_ratio: number | null;
  pfree_cash_flow: number | null;
  dividend: number | null;
  eps_ttm: number | null;
  eps_growth_this_year?: string;
  eps_growth_next_year?: string;
  eps_growth_past_5_years?: string;
  eps_growth_next_5_years?: string;
  sales_growth_past_5_years?: string;
  shares_outstanding: number | null;
  shares_float: number | null;
  insider_ownership?: string;
  insider_transactions?: string;
  institutional_ownership?: string;
  institutional_transactions?: string;
  short_float?: string;
  short_ratio: number | null;
  return_on_assets?: string;
  return_on_equity?: string;
  return_on_invested_capital?: string;
  current_ratio: number | null;
  quick_ratio: number | null;
  lt_debt_equity: number | null;
  total_debt_equity: number | null;
  gross_margin?: string;
  operating_margin?: string;
  profit_margin?: string;
  performance_week?: string;
  performance_month?: string;
  performance_quarter?: string;
  performance_half_year?: string;
  performance_ytd?: string;
  performance_year?: string;
  performance_3_years?: string;
  performance_5_years?: string;
  performance_10_years?: string;
  beta: number | null;
  average_true_range: number | null;
  volatility_week?: string;
  volatility_month?: string;
  sma_20_day: number | null;
  sma_50_day: number | null;
  sma_200_day: number | null;
  week_52_high: number | null;
  week_52_low: number | null;
  week_52_range?: string;
  rsi_14: number | null;
  earnings_date?: string;
  change_from_open?: string;
  gap?: string;
  average_volume: number | null;
  relative_volume: number | null;
  volume: number | null;
  price: number | null;
  change_percent?: string;
  created_at?: string;
  updated_at?: string;
  snapshot_date?: string;
}

// 筛选器响应接口
export interface ScreenerResponse {
  data: ScreenerStockData[];
  total: number;
  page: number;
  pageSize: number;
  hasMore: boolean;
}

// 筛选器查询参数
export interface ScreenerQueryParams extends ScreenerFilters {
  view?: 'overview' | 'valuation' | 'financial' | 'ownership' | 'performance' | 'technical';
  page?: number;
  pageSize?: number;
  sortBy?: string;
  sortOrder?: 'asc' | 'desc';
}

class ScreenerAPI {
  // 获取筛选器数据
  async getScreenerData(params: ScreenerQueryParams): Promise<ScreenerResponse> {
    const response = await api.get('/screener/data', { params });
    return response.data;
  }

  // 获取筛选器统计信息
  async getScreenerStats(filters: ScreenerFilters): Promise<{
    totalStocks: number;
    sectorDistribution: Array<{ sector: string; count: number; percentage: number }>;
    countryDistribution: Array<{ country: string; count: number; percentage: number }>;
    marketCapDistribution: Array<{ range: string; count: number; percentage: number }>;
  }> {
    const response = await api.get('/screener/stats', { params: filters });
    return response.data;
  }

  // 获取行业列表
  async getSectors(): Promise<string[]> {
    const response = await api.get('/screener/sectors');
    return response.data;
  }

  // 获取细分行业列表
  async getIndustries(sector?: string): Promise<string[]> {
    const response = await api.get('/screener/industries', {
      params: sector ? { sector } : {}
    });
    return response.data;
  }

  // 获取国家列表
  async getCountries(): Promise<string[]> {
    const response = await api.get('/screener/countries');
    return response.data;
  }

  // 获取交易所列表
  async getExchanges(): Promise<string[]> {
    const response = await api.get('/screener/exchanges');
    return response.data;
  }

  // 保存筛选器预设
  async savePreset(name: string, filters: ScreenerFilters, view: string): Promise<{ id: number }> {
    const response = await api.post('/screener/presets', {
      name,
      filters,
      view
    });
    return response.data;
  }

  // 获取用户的筛选器预设
  async getPresets(): Promise<Array<{
    id: number;
    name: string;
    filters: ScreenerFilters;
    view: string;
    created_at: string;
    updated_at: string;
  }>> {
    const response = await api.get('/screener/presets');
    return response.data;
  }

  // 删除筛选器预设
  async deletePreset(id: number): Promise<void> {
    await api.delete(`/api/screener/presets/${id}`);
  }

  // 导出筛选器数据
  async exportData(
    params: ScreenerQueryParams,
    format: 'csv' | 'excel' = 'csv'
  ): Promise<Blob> {
    const response = await api.get('/screener/export', {
      params: { ...params, format },
      responseType: 'blob'
    });
    return response.data;
  }

  // 获取单个股票的详细信息
  async getStockDetail(ticker: string): Promise<ScreenerStockData> {
    const response = await api.get(`/api/screener/stocks/${ticker}`);
    return response.data;
  }

  // 获取股票的历史筛选数据
  async getStockHistory(
    ticker: string,
    days: number = 30
  ): Promise<Array<ScreenerStockData & { snapshot_date: string }>> {
    const response = await api.get(`/api/screener/stocks/${ticker}/history`, {
      params: { days }
    });
    return response.data;
  }

  // 创建股票提醒
  async createAlert(params: {
    ticker: string;
    condition: string; // 如 'price > 100' 或 'pe_ratio < 20'
    email?: boolean;
    push?: boolean;
  }): Promise<{ id: number }> {
    const response = await api.post('/screener/alerts', params);
    return response.data;
  }

  // 获取用户的提醒列表
  async getAlerts(): Promise<Array<{
    id: number;
    ticker: string;
    condition: string;
    email: boolean;
    push: boolean;
    active: boolean;
    triggered_at?: string;
    created_at: string;
  }>> {
    const response = await api.get('/screener/alerts');
    return response.data;
  }

  // 删除提醒
  async deleteAlert(id: number): Promise<void> {
    await api.delete(`/api/screener/alerts/${id}`);
  }

  // 更新数据（手动触发数据更新）
  async updateData(): Promise<{ 
    status: string; 
    message: string; 
    updated_count: number;
    last_update: string;
  }> {
    const response = await api.post('/screener/update');
    return response.data;
  }

  // 获取数据更新状态
  async getUpdateStatus(): Promise<{
    last_update: string;
    total_stocks: number;
    is_updating: boolean;
    update_progress?: number;
  }> {
    const response = await api.get('/screener/update-status');
    return response.data;
  }
}

export const screenerAPI = new ScreenerAPI();
export default screenerAPI;
