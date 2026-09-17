import apiClient from './api';

export interface CloseBriefReportRecord {
  id: number;
  report_date: string;
  file_path: string | null;
  metadata: Record<string, any>;
  status: 'generating' | 'completed' | 'failed';
  error_message: string | null;
  created_at: string | null;
  share_token: string | null;
}

export interface CloseBriefTodaySummary {
  report_date: string;
  status: string;
  video_title: string | null;
  placeholder_reason: string | null;
  updated_at: string | null;
}

export interface CloseBriefStatus {
  as_of: string;
  latest_report: CloseBriefReportRecord | null;
  today_summary: CloseBriefTodaySummary | null;
  today_report: CloseBriefReportRecord | null;
}

export interface GenerateResponse {
  success: boolean;
  message: string;
  celery_task_id: string;
}

class CloseBriefReportAPI {
  /**
   * 获取最新一份已完成的报告
   */
  async getLatest(): Promise<CloseBriefReportRecord | null> {
    try {
      const response = await apiClient.get(`/close-brief-report/latest`);
      return response.data || null;
    } catch (error: any) {
      if (error.response?.status === 404) {
        return null;
      }
      throw error;
    }
  }

  /**
   * 当天摘要/报告状态（含最新已完成报告）
   */
  async getStatus(): Promise<CloseBriefStatus> {
    const response = await apiClient.get(`/close-brief-report/status`);
    return response.data;
  }

  /**
   * 获取报告历史列表
   */
  async list(limit: number = 20): Promise<CloseBriefReportRecord[]> {
    const response = await apiClient.get(`/close-brief-report/list`, {
      params: { limit },
    });
    return response.data || [];
  }

  /**
   * 触发报告生成（内部转发 The Close Summary 任务，报告为其后置钩子）
   */
  async generate(reportDate?: string, force: boolean = false): Promise<GenerateResponse> {
    const response = await apiClient.post(`/close-brief-report/generate`, {
      report_date: reportDate,
      force,
    });
    return response.data;
  }

  /**
   * 获取分享链接 URL（iframe src / 新窗口打开）
   */
  getSharedUrl(token: string): string {
    return `/api/close-brief-report/shared/${token}`;
  }
}

export const closeBriefReportAPI = new CloseBriefReportAPI();
