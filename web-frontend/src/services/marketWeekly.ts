/**
 * Market Weekly Report API Service
 */

import apiClient from './api';
import {
  MarketWeeklyResponse,
  WatchlistStocksResponse,
  MarketWeeklyReport,
  WatchlistStock
} from '../types/marketWeekly';

class MarketWeeklyAPI {
  private readonly CACHE_KEY = 'market_weekly_report';
  private readonly CACHE_TTL = 3600000; // 1小时（毫秒）

  /**
   * 从localStorage获取缓存
   */
  private getCachedReport(period: string): MarketWeeklyReport | null {
    try {
      const cacheKey = `${this.CACHE_KEY}_${period}`;
      const cached = localStorage.getItem(cacheKey);
      if (!cached) return null;

      const { data, timestamp } = JSON.parse(cached);
      const now = Date.now();
      
      // 检查是否过期
      if (now - timestamp > this.CACHE_TTL) {
        localStorage.removeItem(cacheKey);
        return null;
      }

      console.log(`[MarketWeeklyAPI] Using cached report (age: ${Math.round((now - timestamp) / 1000)}s)`);
      return data;
    } catch (error) {
      console.error('Error reading cache:', error);
      return null;
    }
  }

  /**
   * 保存报告到localStorage缓存
   */
  private saveCachedReport(period: string, data: MarketWeeklyReport): void {
    try {
      const cacheKey = `${this.CACHE_KEY}_${period}`;
      const cacheData = {
        data,
        timestamp: Date.now()
      };
      localStorage.setItem(cacheKey, JSON.stringify(cacheData));
      console.log(`[MarketWeeklyAPI] Report cached for period: ${period}`);
    } catch (error) {
      console.error('Error saving cache:', error);
      // localStorage可能已满，忽略错误
    }
  }

  /**
   * 清除缓存
   */
  clearCache(period: string = 'latest'): void {
    try {
      const cacheKey = `${this.CACHE_KEY}_${period}`;
      localStorage.removeItem(cacheKey);
      console.log(`[MarketWeeklyAPI] Cache cleared for period: ${period}`);
    } catch (error) {
      console.error('Error clearing cache:', error);
    }
  }

  /**
   * 获取Market Weekly报告数据
   * @param period - 报告周期，'latest' 表示最新一周
   * @param useCache - 是否使用缓存，默认true
   */
  async getReport(period: string = 'latest', useCache: boolean = true): Promise<MarketWeeklyReport> {
    // 尝试从缓存获取
    if (useCache) {
      const cached = this.getCachedReport(period);
      if (cached) {
        // 后台刷新（不阻塞UI）
        this.getReport(period, false).then((freshData) => {
          this.saveCachedReport(period, freshData);
        }).catch((error) => {
          console.warn('Background refresh failed:', error);
        });
        return cached;
      }
    }

    try {
      const response = await apiClient.get<MarketWeeklyResponse>(
        '/market-weekly/report',
        {
          params: { period, use_cache: useCache }
        }
      );

      if (response.data.success) {
        const reportData = response.data.data;
        
        // 保存到缓存
        if (useCache) {
          this.saveCachedReport(period, reportData);
        }
        
        return reportData;
      } else {
        throw new Error(response.data.message || 'Failed to fetch Market Weekly report');
      }
    } catch (error) {
      console.error('Error fetching Market Weekly report:', error);
      throw error;
    }
  }

  /**
   * 获取用户的Watchlist股票列表
   * @param userId - 用户ID
   */
  async getWatchlistStocks(userId?: number): Promise<WatchlistStock[]> {
    try {
      const response = await apiClient.get<WatchlistStocksResponse>(
        '/market-weekly/watchlist-stocks',
        {
          params: userId ? { user_id: userId } : {}
        }
      );

      if (response.data.success) {
        return response.data.data;
      } else {
        throw new Error(response.data.message || 'Failed to fetch watchlist stocks');
      }
    } catch (error) {
      console.error('Error fetching watchlist stocks:', error);
      throw error;
    }
  }

  /**
   * 刷新报告数据（触发重新生成）
   * @param period - 报告周期，默认'latest'
   */
  async refreshReport(period: string = 'latest'): Promise<{ success: boolean; message: string }> {
    try {
      // 清除前端缓存
      this.clearCache(period);
      
      const response = await apiClient.post(
        '/market-weekly/refresh',
        null,
        {
          params: { period }
        }
      );

      return {
        success: response.data.success,
        message: response.data.message || 'Report refreshed successfully'
      };
    } catch (error) {
      console.error('Error refreshing report:', error);
      throw error;
    }
  }

  /**
   * 获取图表图片（base64）
   * @param symbol - 股票代码
   * @param label - 图表标题
   * @param days - 天数
   */
  async getChartImage(symbol: string, label?: string, days: number = 90): Promise<string> {
    try {
      const response = await apiClient.get(
        `/market-weekly/chart/${symbol}`,
        {
          params: {
            label: label || symbol,
            days
          }
        }
      );

      if (response.data.success) {
        return response.data.data;
      } else {
        throw new Error(response.data.message || 'Failed to fetch chart image');
      }
    } catch (error) {
      console.error(`Error fetching chart image for ${symbol}:`, error);
      throw error;
    }
  }
}

// 导出单例
export const marketWeeklyAPI = new MarketWeeklyAPI();

export default marketWeeklyAPI;

