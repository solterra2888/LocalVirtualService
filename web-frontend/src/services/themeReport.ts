/**
 * Theme Report API Service
 */

import apiClient from './api';

export interface ThemeReportRecord {
  id: number;
  report_date: string;        // 'YYYY-MM-DD'
  oss_object_name: string | null;
  signed_url: string | null;  // 24h inline OSS URL
  share_token: string | null; // permanent public share token
  theme_count: number;
  category_count: number;
  constituent_count: number;
  metadata: Record<string, unknown>;
  status: 'pending' | 'generating' | 'completed' | 'failed';
  error_message: string | null;
  created_at: string | null;
  /** Latest trading day used in report return calculations (from metadata). */
  price_as_of_date: string | null;
  /** Max EOD date across tagged tickers in stock_prices_eod. */
  latest_eod_date: string | null;
  /** True when latest_eod_date is newer than price_as_of_date. */
  is_stale: boolean;
  generated_at_new_york?: string | null;
  generated_at_beijing?: string | null;
  scheduler_timezone?: string | null;
  scheduler_timezone_label?: string | null;
  generated_at_scheduler_tz?: string | null;
}

export interface ThemeReportSingleResponse {
  success: boolean;
  data: ThemeReportRecord;
}

export interface GenerateResponse {
  success: boolean;
  message: string;
  task_id: string;
  user_id: string;
  report_date: string;
}

class ThemeReportAPI {
  /** Get the latest completed report for the current user. */
  async getLatest(): Promise<ThemeReportRecord | null> {
    try {
      const resp = await apiClient.get<ThemeReportSingleResponse>(
        '/theme-report/latest'
      );
      return resp.data.data ?? null;
    } catch (err: unknown) {
      // 404 means no report yet — return null instead of throwing
      if ((err as { response?: { status?: number } })?.response?.status === 404) {
        return null;
      }
      throw err;
    }
  }

  /** Trigger async generation for the current user. */
  async generate(reportDate?: string): Promise<GenerateResponse> {
    const resp = await apiClient.post<GenerateResponse>(
      '/theme-report/generate',
      reportDate ? { report_date: reportDate } : {}
    );
    return resp.data;
  }
}

export const themeReportAPI = new ThemeReportAPI();
