import apiClient from './api';

export interface MarketWeeklyReportRecord {
  id: number;
  user_id: string;
  week_start: string;
  week_end: string;
  oss_object_name: string | null;
  share_token: string | null;
  signed_url: string | null;
  status: 'pending' | 'generating' | 'completed' | 'failed';
  metadata: Record<string, any>;
  created_at: string | null;
  updated_at: string | null;
  error_message: string | null;
}

export interface GenerateResponse {
  success: boolean;
  message: string;
  task_id: string;
  user_id: string;
  week_end: string;
}

class MarketWeeklyReportAPI {
  /**
   * 获取用户报告历史列表
   */
  async list(limit: number = 20): Promise<MarketWeeklyReportRecord[]> {
    const response = await apiClient.get(`/market-weekly-report/list`, {
      params: { limit },
    });
    return response.data.data || [];
  }

  /**
   * 获取最新一份已完成的报告
   */
  async getLatest(): Promise<MarketWeeklyReportRecord | null> {
    try {
      const response = await apiClient.get(`/market-weekly-report/latest`);
      return response.data.data || null;
    } catch (error: any) {
      if (error.response?.status === 404) {
        return null;
      }
      throw error;
    }
  }

  /**
   * 触发报告生成
   */
  async generate(weekEnd?: string): Promise<GenerateResponse> {
    const response = await apiClient.post(`/market-weekly-report/generate`, {
      week_end: weekEnd,
    });
    return response.data;
  }

  /**
   * 获取分享链接 URL
   */
  getSharedUrl(token: string): string {
    return `/api/market-weekly-report/shared/${token}`;
  }
}

export const marketWeeklyReportAPI = new MarketWeeklyReportAPI();