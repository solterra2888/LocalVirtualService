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
    video_references?: Array<{
      ref_id: string;
      url: string;
      author: string;
    }>;
    [key: string]: any;
  };
  created_at?: string;
  updated_at?: string;
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
        { limit, offset }
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
        '/reports/youtube-daily/latest'
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
        `/reports/youtube-daily/${reportDate}`
      );
      return data;
    } catch (error: any) {
      console.error('Failed to get report:', error);
      return { success: false, error: error.message || 'Failed to get report' };
    }
  },

  async generateReport(
    reportDate: string,
    force: boolean = false
  ): Promise<ApiResponse<{ task_id: string; report_date: string }>> {
    try {
      const data = await api.post<ApiResponse<{ task_id: string; report_date: string }>>(
        '/reports/youtube-daily/generate',
        { report_date: reportDate, force }
      );
      return data;
    } catch (error: any) {
      console.error('Failed to generate report:', error);
      return { success: false, error: error.message || 'Failed to generate report' };
    }
  },
};
