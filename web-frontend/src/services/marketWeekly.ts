/**
 * Market Weekly Report API Service
 */

import apiClient from './api';
import {
  MarketWeeklyResponse,
  MarketWeeklyReport
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
   * 批量获取OHLC原始数据（用于前端图表渲染）- 支持请求取消
   * @param symbols - 股票代码列表 [{ symbol: 'SPY.US', label: 'S&P 500' }, ...]
   * @param days - 天数
   * @param signal - AbortSignal 用于取消请求
   * @param dataSource - 数据源: 'market' (indices/sectors/forex/commodities/bonds) or 'stock' (个股) or 'auto'
   */
  async getOHLCDataInBatch(
    symbols: Array<{ symbol: string; label?: string }>,
    days: number = 90,
    signal?: AbortSignal,
    dataSource: 'market' | 'stock' | 'auto' = 'auto'
  ): Promise<{
    status: 'completed' | 'cancelled';
    processed: number;
    total: number;
    results: Array<{
      symbol: string;
      label: string;
      data: {
        ohlc: Array<{ time: number; open: number; high: number; low: number; close: number; volume: number }>;
        ema20: Array<{ time: number; value: number }>;
        ema50: Array<{ time: number; value: number }>;
        ema200: Array<{ time: number; value: number }>;
      } | null;
      success: boolean;
      error: string | null;
    }>;
  }> {
    try {
      console.log(`[MarketWeeklyAPI] Batch OHLC request: ${symbols.length} datasets`);
      
      const response = await apiClient.post(
        '/market-weekly/ohlc-data/batch',
        {
          symbols: symbols.map(s => ({
            symbol: s.symbol,
            label: s.label || s.symbol
          })),
          days,
          data_source: dataSource // 🔥 指定数据源
        },
        {
          signal // 🔥 传入AbortSignal用于取消请求
        }
      );

      if (response.data.success || response.data.status === 'cancelled') {
        const { status, processed, total, results } = response.data;
        
        if (status === 'cancelled') {
          console.log(`[MarketWeeklyAPI] Batch OHLC request cancelled. Processed ${processed}/${total}`);
        } else {
          console.log(`[MarketWeeklyAPI] Batch OHLC request completed. ${processed}/${total} datasets`);
        }
        
        return {
          status,
          processed,
          total,
          results: results || []
        };
      } else {
        throw new Error(response.data.message || 'Failed to fetch batch OHLC data');
      }
    } catch (error: any) {
      // 如果是取消错误，不要抛出异常
      if (error.name === 'CanceledError' || error.code === 'ERR_CANCELED') {
        console.log('[MarketWeeklyAPI] Batch OHLC request cancelled by client');
        return {
          status: 'cancelled',
          processed: 0,
          total: symbols.length,
          results: []
        };
      }
      
      console.error('Error fetching batch OHLC data:', error);
      throw error;
    }
  }

  /**
   * 批量获取图表图片（base64）- 支持请求取消
   * 
   * ⚠️ 已过时：建议使用 getOHLCDataInBatch 获取原始数据，在前端渲染图表
   * 
   * @param symbols - 股票代码列表 [{ symbol: 'SPY.US', label: 'S&P 500' }, ...]
   * @param days - 天数
   * @param signal - AbortSignal 用于取消请求
   */
  async getChartsInBatch(
    symbols: Array<{ symbol: string; label?: string }>,
    days: number = 90,
    signal?: AbortSignal
  ): Promise<{
    status: 'completed' | 'cancelled';
    processed: number;
    total: number;
    results: Array<{
      symbol: string;
      label: string;
      data: string | null;
      success: boolean;
      error: string | null;
    }>;
  }> {
    try {
      console.log(`[MarketWeeklyAPI] Batch request: ${symbols.length} charts`);
      
      const response = await apiClient.post(
        '/market-weekly/charts/batch',
        {
          symbols: symbols.map(s => ({
            symbol: s.symbol,
            label: s.label || s.symbol
          })),
          days
        },
        {
          signal // 🔥 传入AbortSignal用于取消请求
        }
      );

      if (response.data.success || response.data.status === 'cancelled') {
        const { status, processed, total, results } = response.data;
        
        if (status === 'cancelled') {
          console.log(`[MarketWeeklyAPI] Batch request cancelled. Processed ${processed}/${total}`);
        } else {
          console.log(`[MarketWeeklyAPI] Batch request completed. ${processed}/${total} charts`);
        }
        
        return {
          status,
          processed,
          total,
          results: results || []
        };
      } else {
        throw new Error(response.data.message || 'Failed to fetch batch charts');
      }
    } catch (error: any) {
      // 如果是取消错误，不要抛出异常
      if (error.name === 'CanceledError' || error.code === 'ERR_CANCELED') {
        console.log('[MarketWeeklyAPI] Batch request cancelled by client');
        return {
          status: 'cancelled',
          processed: 0,
          total: symbols.length,
          results: []
        };
      }
      
      console.error('Error fetching batch charts:', error);
      throw error;
    }
  }

  /**
   * 获取图表图片（base64）- 单个图表
   * @param symbol - 股票代码
   * @param label - 图表标题
   * @param days - 天数
   * 
   * Note: 对于多个图表，建议使用 getChartsInBatch 方法
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

