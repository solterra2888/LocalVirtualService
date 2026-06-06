/**
 * Twitter Daily Report API 服务
 */

import { api } from './api';

// ── 类型定义 ───────────────────────────────────────────────────────────────

export interface TwitterDailyReport {
  id: number;
  report_date: string;
  report_content?: string;
  status: 'pending' | 'generating' | 'completed' | 'failed';
  error_message?: string;
  metadata?: {
    tweet_count?: number;
    author_count?: number;
    top_authors?: string[];
    key_topics?: string[];
    generation_time_seconds?: number;
    window_start?: string;
    window_end?: string;
    tweet_references?: Array<{
      ref_id: string;
      url: string;
      author: string;
    }>;
    generated_at_new_york?: string;
    generated_at_beijing?: string;
    [key: string]: any;
  };
  oss_object_name?: string;
  html_content?: string;
  share_token?: string;
  created_at?: string;
  updated_at?: string;
  generated_at_new_york?: string;
  generated_at_beijing?: string;
}

export interface TwitterReportListResponse {
  reports: TwitterDailyReport[];
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

export const twitterDailyReportsAPI = {
  async listReports(
    limit: number = 30,
    offset: number = 0
  ): Promise<ApiResponse<TwitterReportListResponse>> {
    try {
      const data = await api.get<ApiResponse<TwitterReportListResponse>>(
        '/reports/twitter-daily',
        { limit, offset }
      );
      return data;
    } catch (error: any) {
      console.error('Failed to list Twitter daily reports:', error);
      return { success: false, error: error.message || 'Failed to list reports' };
    }
  },

  async getLatestReport(): Promise<ApiResponse<TwitterDailyReport | null>> {
    try {
      const data = await api.get<ApiResponse<TwitterDailyReport | null>>(
        '/reports/twitter-daily/latest'
      );
      return data;
    } catch (error: any) {
      console.error('Failed to get latest report:', error);
      return { success: false, error: error.message || 'Failed to get latest report' };
    }
  },

  async getReportByDate(reportDate: string): Promise<ApiResponse<TwitterDailyReport | null>> {
    try {
      const data = await api.get<ApiResponse<TwitterDailyReport | null>>(
        `/reports/twitter-daily/${reportDate}`
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
        '/reports/twitter-daily/generate',
        body
      );
      return data;
    } catch (error: any) {
      console.error('Failed to generate report:', error);
      return { success: false, error: error.message || 'Failed to generate report' };
    }
  },

  async getAutoSettings(): Promise<ApiResponse<{ auto_enabled: boolean }>> {
    try {
      const data = await api.get<ApiResponse<{ auto_enabled: boolean }>>(
        '/reports/twitter-daily/settings/auto'
      );
      return data;
    } catch (error: any) {
      console.error('Failed to get auto settings:', error);
      return { success: false, error: error.message || 'Failed to get settings' };
    }
  },

  async updateAutoSettings(autoEnabled: boolean): Promise<ApiResponse<{ auto_enabled: boolean }>> {
    try {
      const data = await api.put<ApiResponse<{ auto_enabled: boolean }>>(
        '/reports/twitter-daily/settings/auto',
        { auto_enabled: autoEnabled }
      );
      return data;
    } catch (error: any) {
      console.error('Failed to update auto settings:', error);
      return { success: false, error: error.message || 'Failed to update settings' };
    }
  },
};
