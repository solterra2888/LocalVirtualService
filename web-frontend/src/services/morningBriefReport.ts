import apiClient from './api';

export interface MorningBriefReportRecord {
  id: number;
  report_date: string;
  file_path: string | null;
  oss_object_name: string | null;
  metadata: Record<string, any>;
  status: 'generating' | 'completed' | 'failed';
  error_message: string | null;
  created_at: string | null;
  share_token: string | null;
  updated_at: string | null;
}

export interface MorningBriefStatus {
  as_of: string;
  latest_report: MorningBriefReportRecord | null;
  today_report: MorningBriefReportRecord | null;
}

/**
 * Morning Brief（facts-report 晨报）API 客户端 — 只读。
 *
 * 该报告由后端 Scheduler 每日定时生成（LLM agent 管线，40-60 分钟），
 * 刻意不提供手动触发接口：管线极重，不允许用户从前端发起。
 */
class MorningBriefReportAPI {
  /** 获取最新一份已完成的报告 */
  async getLatest(): Promise<MorningBriefReportRecord | null> {
    try {
      const response = await apiClient.get(`/morning-brief-report/latest`);
      return response.data || null;
    } catch (error: any) {
      if (error.response?.status === 404) {
        return null;
      }
      throw error;
    }
  }

  /** 今日报告状态 + 最新已完成报告 */
  async getStatus(): Promise<MorningBriefStatus> {
    const response = await apiClient.get(`/morning-brief-report/status`);
    return response.data;
  }

  /** 报告历史列表 */
  async list(limit: number = 20): Promise<MorningBriefReportRecord[]> {
    const response = await apiClient.get(`/morning-brief-report/list`, {
      params: { limit },
    });
    return response.data || [];
  }

  /** 分享链接 URL（iframe src / 新窗口打开） */
  getSharedUrl(token: string): string {
    return `/api/morning-brief-report/shared/${token}`;
  }
}

export const morningBriefReportAPI = new MorningBriefReportAPI();
