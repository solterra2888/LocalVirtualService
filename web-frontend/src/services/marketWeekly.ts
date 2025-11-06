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
  /**
   * 获取Market Weekly报告数据
   * @param period - 报告周期，'latest' 表示最新一周
   */
  async getReport(period: string = 'latest'): Promise<MarketWeeklyReport> {
    try {
      const response = await apiClient.get<MarketWeeklyResponse>(
        '/market-weekly/report',
        {
          params: { period }
        }
      );

      if (response.data.success) {
        return response.data.data;
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
   */
  async refreshReport(): Promise<{ success: boolean; message: string }> {
    try {
      const response = await apiClient.post(
        '/market-weekly/refresh'
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
}

// 导出单例
export const marketWeeklyAPI = new MarketWeeklyAPI();

export default marketWeeklyAPI;

