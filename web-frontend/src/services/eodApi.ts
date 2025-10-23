// EODHD API service for fetching End-of-Day stock data
// Based on: https://eodhd.com/api/eod/{SYMBOL}?from={DATE}&to={DATE}&period={PERIOD}&api_token={TOKEN}&fmt=json

import { api, default as apiClient } from './api';

export interface EODData {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  adjusted_close: number;
  volume: number;
  warning?: string;
}

export interface EODFetchParams {
  ticker: string;
  fromDate?: string;
  toDate?: string;
  period: 'd' | 'w' | 'm';
}

export interface EODFetchResponse {
  data: EODData[];
  success: boolean;
  error?: string;
  warning?: string;
}

class EODApiService {
  private baseUrl = '/eod-fetch'; // Backend API endpoint (without /api prefix since api client adds it)

  /**
   * Fetch EOD data for a given ticker
   * @param params - Fetch parameters
   * @returns Promise<EODFetchResponse>
   */
  async fetchEODData(params: EODFetchParams): Promise<EODFetchResponse> {
    try {
      const requestBody = {
        ticker: params.ticker,
        period: params.period,
        from_date: params.fromDate,
        to_date: params.toDate,
        limit: 100 // Default limit for display
      };

      const result = await api.post(`${this.baseUrl}/fetch`, requestBody);

      return {
        data: result.data || [],
        success: result.success,
        warning: result.warning
      };
    } catch (error) {
      console.error('EOD API fetch error:', error);
      return {
        data: [],
        success: false,
        error: error instanceof Error ? error.message : 'Unknown error occurred'
      };
    }
  }

  /**
   * Export EOD data to Excel
   * @param params - Export parameters
   * @returns Promise<Blob>
   */
  async exportEODData(params: EODFetchParams): Promise<Blob> {
    try {
      const requestBody = {
        ticker: params.ticker,
        period: params.period,
        from_date: params.fromDate,
        to_date: params.toDate
      };

      console.log('Exporting EOD data with params:', requestBody);

      // Use the unified API client for consistency with blob response
      const response = await apiClient.post(`${this.baseUrl}/export`, requestBody, {
        responseType: 'blob'
      });

      console.log('Export response received, blob size:', response.data.size);
      
      if (response.data.size === 0) {
        throw new Error('导出的文件为空，请检查是否有数据');
      }

      return response.data;
    } catch (error) {
      console.error('Export EOD data error:', error);
      throw error;
    }
  }

  /**
   * Get stored EOD data with pagination
   * @param params - Fetch parameters with pagination
   * @returns Promise<EODData[]>
   */
  async getStoredEODData(params: EODFetchParams & { limit?: number; offset?: number }): Promise<EODData[]> {
    const queryParams: Record<string, any> = {
      period: params.period
    };

    if (params.fromDate) queryParams.from_date = params.fromDate;
    if (params.toDate) queryParams.to_date = params.toDate;
    if (params.limit) queryParams.limit = params.limit;
    if (params.offset) queryParams.offset = params.offset;

    return await api.get(`${this.baseUrl}/data/${params.ticker}`, queryParams);
  }

  /**
   * Validate ticker format
   * @param ticker - Ticker symbol to validate
   * @returns boolean
   */
  validateTicker(ticker: string): boolean {
    const tickerPattern = /^[A-Za-z0-9]+\.[A-Za-z]{2,4}$/;
    return tickerPattern.test(ticker);
  }

  /**
   * Get available period options
   */
  getPeriodOptions() {
    return [
      { value: 'd', label: 'Daily', description: 'Daily price data' },
      { value: 'w', label: 'Weekly', description: 'Weekly aggregated data' },
      { value: 'm', label: 'Monthly', description: 'Monthly aggregated data' }
    ];
  }
}

// Export singleton instance
export const eodApiService = new EODApiService();
export default eodApiService;
