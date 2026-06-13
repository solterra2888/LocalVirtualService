/**
 * YouTube Daily Report API 服务
 */

import { api } from './api';

// ── 类型定义 ───────────────────────────────────────────────────────────────

export interface YouTubeDailyReport {
  id: number;
  report_date: string;
  report_content?: string;
  status: 'pending' | 'generating' | 'completed' | 'failed';
  error_message?: string;
  metadata?: {
    video_count?: number;
    channel_count?: number;
    top_channels?: string[];
    generation_time_seconds?: number;
    window_start?: string;
    window_end?: string;
    video_references?: Array<{
      ref_id: string;
      url: string;
      author: string;
    }>;
    generated_at_new_york?: string;
    generated_at_beijing?: string;
    [key: string]: any;
  };
  // 新增字段: HTML 存储 + 分享
  oss_object_name?: string | null;
  html_content?: string | null;
  share_token?: string | null;
  created_at?: string;
  updated_at?: string;
  generated_at_new_york?: string;
  generated_at_beijing?: string;
}

export interface YouTubeReportListResponse {
  reports: YouTubeDailyReport[];
  total: number;
  limit: number;
  offset: number;
}

interface ApiResponse<T = any> {
  success: boolean;
  data?: T;
  message?: string;
  error?: string;
}

// ── API 方法 ──────────────────────────────────────────────────────────────

export const youtubeDailyReportsAPI = {
  async listReports(
    limit: number = 30,
    offset: number = 0
  ): Promise<ApiResponse<YouTubeReportListResponse>> {
    try {
      const data = await api.get<ApiResponse<YouTubeReportListResponse>>(
        '/reports/youtube-daily',
        { limit, offset, _t: Date.now() }
      );
      return data;
    } catch (error: any) {
      console.error('Failed to list YouTube daily reports:', error);
      return { success: false, error: error.message || 'Failed to list reports' };
    }
  },

  async getLatestReport(): Promise<ApiResponse<YouTubeDailyReport | null>> {
    try {
      const data = await api.get<ApiResponse<YouTubeDailyReport | null>>(
        '/reports/youtube-daily/latest',
        { _t: Date.now() }
      );
      return data;
    } catch (error: any) {
      console.error('Failed to get latest report:', error);
      return { success: false, error: error.message || 'Failed to get latest report' };
    }
  },

  async getReportByDate(reportDate: string): Promise<ApiResponse<YouTubeDailyReport | null>> {
    try {
      const data = await api.get<ApiResponse<YouTubeDailyReport | null>>(
        `/reports/youtube-daily/${reportDate}`,
        { _t: Date.now() }
      );
      return data;
    } catch (error: any) {
      console.error('Failed to get report:', error);
      return { success: false, error: error.message || 'Failed to get report' };
    }
  },

  async generateReport(
    force: boolean = false,
    reportDate?: string
  ): Promise<ApiResponse<{ task_id: string; report_date: string; window_end?: string | null }>> {
    try {
      const body: { force: boolean; report_date?: string } = { force };
      if (reportDate) body.report_date = reportDate;
      const data = await api.post<ApiResponse<{ task_id: string; report_date: string; window_end?: string | null }>>(
        '/reports/youtube-daily/generate',
        body
      );
      return data;
    } catch (error: any) {
      console.error('Failed to generate report:', error);
      return { success: false, error: error.message || 'Failed to generate report' };
    }
  },
};
