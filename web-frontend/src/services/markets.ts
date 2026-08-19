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

// ===== API 方法 =====

export const marketsAPI = {
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
