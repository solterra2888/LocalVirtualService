import apiClient from './api';

export interface ClosingBellReportRecord {
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
}

class ClosingBellReportAPI {
  /**
   * 获取最新一份已完成的报告
   */
  async getLatest(): Promise<ClosingBellReportRecord | null> {
    try {
      const response = await apiClient.get(`/closing-bell-report/latest`);
      return response.data || null;
    } catch (error: any) {
      if (error.response?.status === 404) {
        return null;
      }
      throw error;
    }
  }

  /**
   * 获取报告历史列表
   */
  async list(limit: number = 20): Promise<ClosingBellReportRecord[]> {
    const response = await apiClient.get(`/closing-bell-report/list`, {
      params: { limit },
    });
    return response.data || [];
  }

  /**
   * 触发报告生成（内部转发 CNBC Closing Bell Summary 任务，报告为其后置钩子）
   */
  async generate(reportDate?: string, force: boolean = false): Promise<GenerateResponse> {
    const response = await apiClient.post(`/closing-bell-report/generate`, {
      report_date: reportDate,
      force,
    });
    return response.data;
  }

  /**
   * 获取分享链接 URL（iframe src / 新窗口打开）
   */
  getSharedUrl(token: string): string {
    return `/api/closing-bell-report/shared/${token}`;
  }
}

export const closingBellReportAPI = new ClosingBellReportAPI();
