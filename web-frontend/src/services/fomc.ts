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

export interface ProbLatestSnapshot {
  asof_date: string;
  p_cut: number | null;
  p_hold: number | null;
  p_hike: number | null;
  implied_rate: number | null;
  effr: number | null;
  futures_contract: string | null;
  futures_price: number | null;
  method: string | null;
}

export interface ProbTrendPoint {
  date: string;
  p_cut: number | null;
  p_hold: number | null;
  p_hike: number | null;
}

export interface NextMeetingProb {
  meeting_date: string;
  days_away: number;
  is_sep: boolean;
  latest: ProbLatestSnapshot | null;
  trend: ProbTrendPoint[];
}

export interface ExpectationVsActual {
  meeting_date: string;
  actual_direction: 'hike' | 'cut' | 'hold' | null;
  actual_change_bp: number | null;
  implied_direction: 'hike' | 'cut' | 'hold' | null;
  implied_p: { cut: number; hold: number; hike: number } | null;
  asof_date: string | null;
  hit: boolean | null;
  method: string | null;
}

export interface ProbabilitiesPayload {
  asof_date: string | null;
  next_meetings: NextMeetingProb[];
  expectations_vs_actual: ExpectationVsActual[];
  method_note: string;
}

export interface DashboardData {
  decision: DecisionCard | null;
  statement_summary: SummaryInfo | null;
  minutes_summary: SummaryInfo | null;
  recent_meetings: RecentMeeting[];
  updates: UpdateItem[];
  updates_total: number;
  probabilities: ProbabilitiesPayload | null;
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
  | 'poll' | 'fetch' | 'summarize' | 'backfill' | 'retry'
  | 'probabilities' | 'probabilities_backfill';

export interface ProbabilityTrendPoint {
  date: string;
  p_cut: number | null;
  p_hold: number | null;
  p_hike: number | null;
  implied_rate: number | null;
}

export interface ProbabilityTrend {
  meeting_date: string;
  days: number;
  series: ProbabilityTrendPoint[];
}

// ===== API =====

export const fomcAPI = {
  /** 央行 Dashboard 聚合数据 */
  getDashboard: async (): Promise<DashboardData> => {
    return api.get<DashboardData>('/fomc/dashboard');
  },

  /** 单会议概率完整趋势（折线图数据源） */
  getProbabilityTrend: async (meetingDate: string, days = 90): Promise<ProbabilityTrend> => {
    return api.get<ProbabilityTrend>('/fomc/probabilities', {
      meeting_date: meetingDate,
      days,
    });
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
