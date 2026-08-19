// 央行 Dashboard API 服务 (Fed FOMC)
import { api } from './api';

// ===== 类型定义 =====

export interface Dissenter {
  name: string;
  direction: 'hawkish' | 'dovish' | 'unknown';
}

export interface DecisionCard {
  meeting_date?: string | null;
  decision_date?: string | null;
  rate_low_bp?: number | null;
  rate_high_bp?: number | null;
  decision_direction?: 'hike' | 'cut' | 'hold' | null;
  change_bp?: number | null;
  vote_for?: number | null;
  vote_against?: number | null;
  dissenters: Dissenter[];
  source_url?: string | null;
  previous?: {
    meeting_date: string;
    rate_high_bp: number;
  } | null;
  eodhd_cross_check?: {
    actual: number | null;
    forecast: number | null;
    previous: number | null;
    source: string;
  } | null;
}

export interface SummaryInfo {
  document_id?: number | null;
  doc_type?: 'statement' | 'minutes' | null;
  meeting_date?: string | null;
  title?: string | null;
  source_url?: string | null;
  status?: 'generating' | 'completed' | 'failed' | null;
  template_id?: string | null;
  model?: string | null;
  summary_content?: string | null;
  prev_meeting_date?: string | null;
  generated_at?: string | null;
}

export interface UpdateItem {
  id: number;
  title: string;
  link: string;
  category?: string | null;
  is_monetary_policy: boolean;
  pub_date?: string | null;
}

export interface RecentMeeting {
  meeting_date: string;
  decision_direction: string | null;
  change_bp: number | null;
  vote_for: number | null;
  vote_against: number | null;
  rate_high_bp: number | null;
}

export interface DashboardData {
  decision: DecisionCard | null;
  statement_summary: SummaryInfo | null;
  minutes_summary: SummaryInfo | null;
  recent_meetings: RecentMeeting[];
  updates: UpdateItem[];
  updates_total: number;
  probabilities: null; // V1 缓做
}

export interface UpdatesPage {
  items: UpdateItem[];
  total: number;
  page: number;
  page_size: number;
}

export interface DocumentDetail {
  id: number;
  doc_type: 'statement' | 'minutes';
  meeting_date?: string | null;
  title: string;
  source_url: string;
  published_at?: string | null;
  raw_content?: string | null;
  word_count?: number | null;
  fetch_status: 'pending' | 'fetched' | 'failed';
  fetched_at?: string | null;
  summary?: {
    template_id: string;
    model: string | null;
    status: string;
    summary_content: string | null;
    updated_at: string | null;
  } | null;
}

export type RefreshType =
  | 'poll' | 'fetch' | 'summarize' | 'backfill' | 'retry';

// ===== API =====

export const fomcAPI = {
  /** 央行 Dashboard 聚合数据 */
  getDashboard: async (): Promise<DashboardData> => {
    return api.get<DashboardData>('/fomc/dashboard');
  },

  /** 文档原文 + 概括（原文对照用） */
  getDocument: async (documentId: number): Promise<DocumentDetail> => {
    return api.get<DocumentDetail>(`/fomc/documents/${documentId}`);
  },

  /** Fed updates 流（分页） */
  getUpdates: async (params?: {
    page?: number;
    page_size?: number;
    monetary_only?: boolean;
  }): Promise<UpdatesPage> => {
    return api.get<UpdatesPage>('/fomc/updates', params);
  },

  /** 手动刷新（派发 Celery 任务） */
  refresh: async (type: RefreshType, documentId?: number, force?: boolean): Promise<{
    task_id: string | null;
    dispatched: string;
  }> => {
    return api.post<{ task_id: string | null; dispatched: string }>(
      '/fomc/refresh', { type, document_id: documentId, force: !!force });
  },
};
