// Markets API 服务
import { api } from './api';

// ===== 类型定义 =====

export interface EconomicEvent {
  date?: string;
  time?: string;
  country?: string;
  event?: string;
  period?: string;
  actual?: string | number;
  previous?: string | number;
  forecast?: string | number;
  /** 对比口径: null/undefined=指数级 / 'yoy' 同比 / 'mom' 环比 */
  comparison?: string | null;
  calculated_change?: number;
  calculated_change_pct?: number;
  impact?: number;
  /** 事件类别 slug（interest-rate / inflation / ...，见 /markets/economic-events/categories） */
  category?: string;
}

/** 经济事件类别（Category 筛选） */
export interface EconomicEventCategory {
  slug: string;
  name: string;
}

export interface MacroIndicator {
  country_code: string;
  country_name: string;
  indicator: string;
  indicator_name: string;
  year: number;
  value?: number;
  date_str?: string;
  fetch_timestamp?: string;
}

export interface MarketStats {
  total_events: number;
  latest_date?: string;
  country_stats: Record<string, number>;
  date_stats: Record<string, number>;
  total_countries: number;
}

export interface CountryInfo {
  code: string;
  name: string;
}

export interface IndicatorInfo {
  indicator: string;
  indicator_name: string;
  data_points: number;
  earliest_year: number;
  latest_year: number;
}

export interface EconomicEventParams {
  from_date?: string;
  to_date?: string;
  country?: string;
  /** 对比口径: 'index'=指数级(默认) / 'yoy' / 'mom' / 'all'=全部 */
  comparison?: string;
  /** 重要性等级多选, 逗号分隔, 如 '2,3' */
  impacts?: string;
  /** 类别 slug 多选, 逗号分隔, 如 'interest-rate,inflation' */
  categories?: string;
  /** 事件名精确匹配（日历行历史序列查询） */
  event?: string;
  /** 排序方向: 'desc'=新到旧(默认) / 'asc'=旧到新（日历视图） */
  order?: string;
  limit?: number;
  offset?: number;
}

export interface MacroIndicatorParams {
  country?: string | string[];
  indicator?: string | string[];
  year_from?: number;
  year_to?: number;
  limit?: number;
}

export interface FetchResponse {
  message: string;
  total_fetched?: number;
  new_records?: number;
  country?: string;
  indicator?: string;
  total_records?: number;
}

export interface SupportedCountries {
  economic_events: string[];
  macro_indicators: Record<string, string>;
}

export interface CleanupResponse {
  message: string;
  deleted_count: number;
  retention_days: number;
}

// ===== 财报日历类型 =====

export interface EarningsCalendarItem {
  code: string;
  company_name?: string | null;
  logo_url?: string | null;
  report_date?: string | null;
  /** BeforeMarket / AfterMarket / 其他或空 */
  before_after_market?: string | null;
  currency?: string | null;
  eps_actual?: number | null;
  eps_estimate?: number | null;
  eps_difference?: number | null;
  eps_surprise_percent?: number | null;
  reported: boolean;
  fetch_date?: string | null;
}

export interface EarningsCalendarMeta {
  total: number;
  limit: number;
  offset: number;
  scope: 'watchlist' | 'theme' | 'us' | 'hk';
  from_date?: string | null;
  to_date?: string | null;
  latest_fetch_date?: string | null;
  latest_update_at?: string | null;
}

export interface EarningsCalendarData {
  items: EarningsCalendarItem[];
  meta: EarningsCalendarMeta;
}

export interface EarningsCalendarParams {
  from_date?: string;
  to_date?: string;
  scope?: 'watchlist' | 'theme' | 'us' | 'hk';
  before_after?: 'before' | 'after';
  search?: string;
  reported?: 'yes' | 'no';
  /** 最小市值筛选(USD 美元, 港股已按汇率换算), 如 1e10 = $10B */
  min_market_cap?: number;
  limit?: number;
  offset?: number;
}

// ===== 财报日历月视图类型 =====

export interface EarningsMonthDayTopItem {
  code: string;
  company_name?: string | null;
  logo_url?: string | null;
  eps_estimate?: number | null;
  eps_actual?: number | null;
  eps_surprise_percent?: number | null;
  reported: boolean;
  market_cap?: number | null;
}

export interface EarningsMonthDaySummary {
  date?: string | null;
  count: number;
  before_count: number;
  after_count: number;
  top: EarningsMonthDayTopItem[];
}

export interface EarningsMonthSummary {
  month: string;
  scope: 'watchlist' | 'theme' | 'us' | 'hk';
  total: number;
  days: EarningsMonthDaySummary[];
}

// ===== 财报日历公司面板类型 =====

export interface EarningsPanelCompany {
  code: string;
  name?: string | null;
  exchange?: string | null;
  sector?: string | null;
  industry?: string | null;
  country?: string | null;
  description?: string | null;
  web_url?: string | null;
  logo_url?: string | null;
  employees?: number | null;
  currency?: string | null;
}

export interface EarningsPanelHighlights {
  market_cap?: number | null;
  pe_ratio?: number | null;
  dividend_yield?: number | null;
  eps_ttm?: number | null;
  eps_est_current_quarter?: number | null;
  eps_est_next_quarter?: number | null;
  eps_est_current_year?: number | null;
  eps_est_next_year?: number | null;
  revenue_ttm?: number | null;
  profit_margin?: number | null;
  target_price?: number | null;
  most_recent_quarter?: string | null;
  forward_pe?: number | null;
  ps_ratio?: number | null;
  peg_ratio?: number | null;
  dividend_share?: number | null;
  revenue_growth_yoy?: number | null;
  earnings_growth_yoy?: number | null;
}

export interface EarningsPanelNext {
  report_date?: string | null;
  before_after_market?: string | null;
  eps_estimate?: number | null;
  revenue_estimate?: number | null;
}

export interface EarningsPanelHistoryQuarter {
  report_date?: string | null;
  fiscal_period?: string | null;
  before_after_market?: string | null;
  eps_actual?: number | null;
  eps_estimate?: number | null;
  surprise_percent?: number | null;
}

export interface EarningsPanelRevenueQuarter {
  period?: string | null;
  revenue?: number | null;
}

export interface EarningsPanelDividendQuarter {
  period?: string | null;
  amount?: number | null;
}

export interface EarningsPanelTrend {
  period?: string | null;
  period_date?: string | null;
  growth?: number | null;
  eps_avg?: number | null;
  eps_high?: number | null;
  eps_low?: number | null;
  analysts?: number | null;
  revenue_avg?: number | null;
  revenue_analysts?: number | null;
  revisions_up_30d?: number | null;
  revisions_down_30d?: number | null;
}

export interface EarningsPanelRatings {
  rating?: number | null;
  target_price?: number | null;
  strong_buy?: number | null;
  buy?: number | null;
  hold?: number | null;
  sell?: number | null;
  strong_sell?: number | null;
}

export interface EarningsPanelTechnicals {
  beta?: number | null;
  week52_high?: number | null;
  week52_low?: number | null;
  ma50?: number | null;
  ma200?: number | null;
  short_volume?: number | null;
  short_percent?: number | null;
  short_as_of?: string | null;
}

export interface EarningsPanelPrice {
  last?: number | null;
  change_percent?: number | null;
  as_of?: string | null;
  volume?: number | null;
  previous_close?: number | null;
}

export interface EarningsCompanyPanel {
  found: boolean;
  code: string;
  company?: EarningsPanelCompany;
  highlights?: EarningsPanelHighlights | null;
  next_earnings?: EarningsPanelNext | null;
  earnings_history?: EarningsPanelHistoryQuarter[];
  revenue_history?: EarningsPanelRevenueQuarter[];
  revenue_currency?: string | null;
  dividend_history?: EarningsPanelDividendQuarter[];
  dividend_currency?: string | null;
  earnings_trend?: EarningsPanelTrend | null;
  analyst_ratings?: EarningsPanelRatings | null;
  technicals?: EarningsPanelTechnicals | null;
  price?: EarningsPanelPrice | null;
  eps_beat_count?: number | null;
  eps_est_quarters?: number | null;
}

// ===== 财报电话会 AI Recap 类型 =====

export interface EarningsRecapResult {
  status: 'ok' | 'no_transcript' | 'unsupported' | 'error' | 'quota_exceeded';
  code: string;
  period?: string | null;
  call_date?: string | null;
  source?: string | null;
  source_url?: string | null;
  recap_md?: string | null;
  model?: string | null;
  cached?: boolean;
  generated_at?: string | null;
  message?: string | null;
  stale?: boolean;
  latest_report_date?: string | null;
}

// ===== Earnings Feed 类型（右栏默认态） =====

export interface EarningsFeedItem {
  code: string;
  company_name?: string | null;
  logo_url?: string | null;
  report_date?: string | null;
  before_after_market?: string | null;
  eps_estimate?: number | null;
  eps_actual?: number | null;
  eps_surprise_percent?: number | null;
  reported: boolean;
  price?: number | null;
  change_percent?: number | null;
  revenue?: number | null;
  revenue_period?: string | null;
}

// ===== EDGAR 财报资料(Slides/Report 外链) =====

export interface EarningsMaterials {
  found: boolean;
  code: string;
  cik?: number;
  eightk?: {
    filing_date: string;
    press_release?: string | null;
    slides?: string | null;
  };
  quarterly?: {
    form: string;
    filing_date: string;
    url: string;
  };
}

export interface EarningsFeedData {
  items: EarningsFeedItem[];
  meta: {
    total: number;
    reported: number;
    scope: string;
    from_date?: string | null;
    to_date?: string | null;
  };
}

// ===== API 方法 =====

export const marketsAPI = {

  // === 财报日历相关 ===

  /**
   * 获取财报日历数据
   */
  getEarningsCalendar: async (params?: EarningsCalendarParams): Promise<EarningsCalendarData> => {
    try {
      const result = await api.get<EarningsCalendarData>('/markets/earnings-calendar', params);
      return result;
    } catch (error) {
      console.error('获取财报日历数据失败:', error);
      throw error;
    }
  },

  /**
   * 获取财报日历月度汇总（Month View）
   */
  getEarningsCalendarMonthSummary: async (
    month: string,
    scope?: 'watchlist' | 'theme' | 'us' | 'hk',
    search?: string,
    minMarketCap?: number,
    /** 每日市值 Top N(月视图格子 logo 方块尽量铺满, 默认取满上限由前端按格子容量裁剪) */
    topN?: number
  ): Promise<EarningsMonthSummary> => {
    try {
      const result = await api.get<EarningsMonthSummary>('/markets/earnings-calendar/month-summary', {
        month,
        scope,
        search: search || undefined,
        min_market_cap: minMarketCap,
        top_n: topN,
      });
      return result;
    } catch (error) {
      console.error('获取财报月度汇总失败:', error);
      throw error;
    }
  },

  /**
   * 获取 Ticker 财务信息面板（财报日历右侧详情）
   */
  /**
   * 获取 EDGAR 财报资料链接(Report/Slides/10-Q, 免费官方源)
   */
  getEarningsMaterials: async (code: string): Promise<EarningsMaterials> => {
    try {
      const result = await api.get<EarningsMaterials>('/markets/earnings-calendar/company-materials', { code });
      return result;
    } catch (error) {
      console.error('获取 EDGAR 资料链接失败:', error);
      throw error;
    }
  },

  getEarningsCompanyPanel: async (code: string): Promise<EarningsCompanyPanel> => {
    try {
      const result = await api.get<EarningsCompanyPanel>('/markets/earnings-calendar/company-panel', {
        code,
      });
      return result;
    } catch (error) {
      console.error('获取公司面板失败:', error);
      throw error;
    }
  },

  /**
   * 生成/获取财报电话会 AI Recap（按需抓取 transcript + LLM 分析, 结果永久缓存）
   */
  generateEarningsRecap: async (code: string): Promise<EarningsRecapResult> => {
    try {
      const result = await api.post<EarningsRecapResult>(
        `/markets/earnings-calendar/company-recap?code=${encodeURIComponent(code)}`
      );
      return result;
    } catch (error) {
      console.error('生成公司 Recap 失败:', error);
      throw error;
    }
  },

  /**
   * 只读获取已缓存的 Recap（面板打开时自动恢复用; 204/失败返回 null, 不触发生成）
   */
  getEarningsRecapCached: async (code: string): Promise<EarningsRecapResult | null> => {
    try {
      const result = await api.get<EarningsRecapResult | null>(
        `/markets/earnings-calendar/company-recap?code=${encodeURIComponent(code)}`
      );
      return result || null;
    } catch (error) {
      console.error('读取公司 Recap 缓存失败:', error);
      return null;
    }
  },

  /**
   * 获取 Earnings Feed（右栏默认事件流）
   */
  getEarningsCalendarFeed: async (params?: {
    from_date?: string;
    to_date?: string;
    scope?: 'watchlist' | 'theme' | 'us' | 'hk';
    search?: string;
    min_market_cap?: number;
    limit?: number;
  }): Promise<EarningsFeedData> => {
    try {
      const result = await api.get<EarningsFeedData>('/markets/earnings-calendar/feed', params);
      return result;
    } catch (error) {
      console.error('获取 Earnings Feed 失败:', error);
      throw error;
    }
  },

  // === 经济事件相关 ===
  
  /**
   * 获取经济事件数据
   */
  getEconomicEvents: async (params?: EconomicEventParams): Promise<EconomicEvent[]> => {
    try {
      console.log('正在获取经济事件数据...', params);
      const result = await api.get<EconomicEvent[]>('/markets/economic-events', params);
      console.log('经济事件数据获取成功:', result.length, '条');
      return result;
    } catch (error) {
      console.error('获取经济事件数据失败:', error);
      throw error;
    }
  },



  // === 宏观指标相关 ===
  
  /**
   * 获取宏观指标数据
   */
  getMacroIndicators: async (params?: MacroIndicatorParams): Promise<MacroIndicator[]> => {
    try {
      console.log('正在获取宏观指标数据...', params);
      const result = await api.get<MacroIndicator[]>('/markets/macro-indicators', params);
      console.log('宏观指标数据获取成功:', result.length, '条');
      return result;
    } catch (error) {
      console.error('获取宏观指标数据失败:', error);
      throw error;
    }
  },



  // === 统计和配置相关 ===
  
  /**
   * 获取市场数据统计
   */
  getMarketStatistics: async (): Promise<MarketStats> => {
    try {
      console.log('正在获取市场数据统计...');
      const result = await api.get<MarketStats>('/markets/statistics');
      console.log('市场数据统计获取成功:', result);
      return result;
    } catch (error) {
      console.error('获取市场数据统计失败:', error);
      throw error;
    }
  },

  /**
   * 获取可用国家列表
   */
  getAvailableCountries: async (): Promise<CountryInfo[]> => {
    try {
      console.log('正在获取可用国家列表...');
      const result = await api.get<CountryInfo[]>('/markets/countries');
      console.log('可用国家列表获取成功:', result.length, '个国家');
      return result;
    } catch (error) {
      console.error('获取可用国家列表失败:', error);
      throw error;
    }
  },

  /**
   * 获取经济事件可用国家列表
   */
  getEconomicEventsCountries: async (): Promise<CountryInfo[]> => {
    try {
      console.log('正在获取经济事件国家列表...');
      const result = await api.get<CountryInfo[]>('/markets/economic-events/countries');
      console.log('经济事件国家列表获取成功:', result.length, '个国家');
      return result;
    } catch (error) {
      console.error('获取经济事件国家列表失败:', error);
      throw error;
    }
  },

  /**
   * 获取经济事件类别定义（Category 筛选）
   */
  getEconomicEventCategories: async (): Promise<EconomicEventCategory[]> => {
    try {
      const result = await api.get<EconomicEventCategory[]>('/markets/economic-events/categories');
      return result;
    } catch (error) {
      console.error('获取经济事件类别列表失败:', error);
      throw error;
    }
  },

  /**
   * 获取可用指标列表
   */
  getAvailableIndicators: async (country?: string): Promise<{
    country: string | null;
    indicators: IndicatorInfo[];
  }> => {
    try {
      console.log('正在获取可用指标列表...', country);
      const params = country ? { country } : {};
      const result = await api.get('/markets/indicators', params);
      console.log('可用指标列表获取成功:', result.indicators.length, '个指标');
      return result;
    } catch (error) {
      console.error('获取可用指标列表失败:', error);
      throw error;
    }
  },

  /**
   * 获取支持的国家代码
   */
  getSupportedCountries: async (): Promise<SupportedCountries> => {
    try {
      console.log('正在获取支持的国家代码...');
      const result = await api.get<SupportedCountries>('/markets/supported-countries');
      console.log('支持的国家代码获取成功:', result);
      return result;
    } catch (error) {
      console.error('获取支持的国家代码失败:', error);
      throw error;
    }
  },

  // === 管理功能 ===
  
  /**
   * 清理旧的经济事件数据（需要管理员权限）
   */
  cleanupOldEconomicEvents: async (days: number = 365): Promise<CleanupResponse> => {
    try {
      console.log('正在清理旧的经济事件数据...', days, '天前');
      const result = await api.delete<CleanupResponse>(`/markets/economic-events/cleanup?days=${days}`);
      console.log('旧数据清理成功:', result);
      return result;
    } catch (error) {
      console.error('清理旧数据失败:', error);
      throw error;
    }
  },

  // === 便利方法 ===
  
  /**
   * 获取最近的经济事件
   */
  getRecentEconomicEvents: async (days: number = 7): Promise<EconomicEvent[]> => {
    const toDate = new Date().toISOString().split('T')[0];
    const fromDate = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    
    return marketsAPI.getEconomicEvents({
      from_date: fromDate,
      to_date: toDate,
      limit: 100
    });
  },

  /**
   * 获取即将到来的经济事件
   */
  getUpcomingEconomicEvents: async (days: number = 7): Promise<EconomicEvent[]> => {
    const fromDate = new Date().toISOString().split('T')[0];
    const toDate = new Date(Date.now() + days * 24 * 60 * 60 * 1000).toISOString().split('T')[0];
    
    return marketsAPI.getEconomicEvents({
      from_date: fromDate,
      to_date: toDate,
      limit: 100
    });
  },

  /**
   * 获取指定国家的最新宏观指标
   */
  getCountryMacroIndicators: async (country: string, limit: number = 100): Promise<MacroIndicator[]> => {
    return marketsAPI.getMacroIndicators({
      country,
      limit
    });
  },

  /**
   * 获取指定指标的跨国比较数据
   */
  getIndicatorComparison: async (indicator: string, yearFrom?: number, yearTo?: number): Promise<MacroIndicator[]> => {
    return marketsAPI.getMacroIndicators({
      indicator,
      year_from: yearFrom,
      year_to: yearTo,
      limit: 1000
    });
  },

  /**
   * 导出宏观指标数据为CSV
   */
  exportMacroIndicatorsCSV: async (params?: {
    country?: string | string[];
    indicator?: string | string[];
    year_from?: number;
    year_to?: number;
  }): Promise<void> => {
    try {
      console.log('正在导出宏观指标CSV...', params);
      
      // 构建查询参数
      const queryParams = new URLSearchParams();
      
      if (params?.country) {
        if (Array.isArray(params.country)) {
          queryParams.append('country', params.country.join(','));
        } else {
          queryParams.append('country', params.country);
        }
      }
      
      if (params?.indicator) {
        if (Array.isArray(params.indicator)) {
          queryParams.append('indicator', params.indicator.join(','));
        } else {
          queryParams.append('indicator', params.indicator);
        }
      }
      
      if (params?.year_from) {
        queryParams.append('year_from', params.year_from.toString());
      }
      
      if (params?.year_to) {
        queryParams.append('year_to', params.year_to.toString());
      }
      
      // 发起下载请求
      const response = await fetch(`/api/markets/macro-indicators/export?${queryParams.toString()}`, {
        method: 'GET',
        headers: {
          'Authorization': `Bearer ${localStorage.getItem('access_token')}`,
        },
      });
      
      if (!response.ok) {
        throw new Error(`导出失败: ${response.status} ${response.statusText}`);
      }
      
      // 获取文件名
      const contentDisposition = response.headers.get('Content-Disposition');
      let filename = 'macro_indicators.csv';
      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename=(.+)/);
        if (filenameMatch) {
          filename = filenameMatch[1];
        }
      }
      
      // 下载文件
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      
      console.log('宏观指标CSV导出成功:', filename);
    } catch (error) {
      console.error('导出宏观指标CSV失败:', error);
      throw error;
    }
  }
};

export default marketsAPI;
