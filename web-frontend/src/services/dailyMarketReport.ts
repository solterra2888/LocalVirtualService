import apiClient from './api';

export interface DailyMarketReportRecord {
  id: number;
  report_date: string;
  file_path: string | null;
  metadata: Record<string, any>;
  status: 'generating' | 'completed' | 'failed';
  error_message: string | null;
  created_at: string | null;
  share_token: string | null;
}

export interface GenerateResponse {
  success: boolean;
  message: string;
  celery_task_id: string;
  user_id: string;
}

class DailyMarketReportAPI {
  /**
   * 获取用户报告历史列表
   */
  async list(limit: number = 20): Promise<DailyMarketReportRecord[]> {
    const response = await apiClient.get(`/daily-market-report/list`, {
      params: { limit },
    });
    return response.data || [];
  }

  /**
   * 获取最新一份已完成的报告
   */
  async getLatest(): Promise<DailyMarketReportRecord | null> {
    try {
      const response = await apiClient.get(`/daily-market-report/latest`);
      return response.data || null;
    } catch (error: any) {
      if (error.response?.status === 404) {
        return null;
      }
      throw error;
    }
  }

  /**
   * 触发报告生成（当天单用户报告）
   */
  async generate(reportDate?: string): Promise<GenerateResponse> {
    const response = await apiClient.post(`/daily-market-report/generate`, {
      report_date: reportDate,
    });
    return response.data;
  }

  /**
   * 获取分享链接 URL（iframe src / 新窗口打开）
   */
  getSharedUrl(token: string): string {
    return `/api/daily-market-report/shared/${token}`;
  }
}

export const dailyMarketReportAPI = new DailyMarketReportAPI();
