// Form 13F API 服务 (SEC 季度持仓)
import { api } from './api';

// ===== 类型定义 =====

export interface FilerSummary {
  id: number;
  name: string;
  latest_quarter?: string | null;
  latest_total_value?: number | null;
}

export interface PresetItem extends FilerSummary {
  label: string;
}

export interface ManifestData {
  quarters: string[];
  presets: PresetItem[];
  top_filers: FilerSummary[];
}

export interface FilerSearchItem extends FilerSummary {
  cik?: string | null;
  is_preset: boolean;
}

export interface FilerSearchPage {
  items: FilerSearchItem[];
  total: number;
  page: number;
  page_size: number;
}

export interface HoldingItem {
  issuer: string;
  title?: string | null;
  cusip: string;
  putcall?: string | null;
  value: number;
  shares?: number | null;
  /** 上一报告期全量持仓中同 (cusip,putcall) 的股数；无上期或不持有则为 null */
  prev_shares?: number | null;
  pct: number;
}

export interface QuarterSnapshot {
  quarter: string;
  total_value?: number | null;
  position_count?: number | null;
  sector_alloc: Record<string, number>;
  top20_holdings: HoldingItem[];
}

export interface FilerDetail {
  id: number;
  name: string;
  cik?: string | null;
  is_preset: boolean;
  quarters: QuarterSnapshot[];
}

export type RefreshType = 'poll' | 'import' | 'reaggregate' | 'backfill' | 'retry';

// ===== API =====

export const form13fAPI = {
  /** 仪表盘清单：季度列表 + 预设机构 + 规模 TopN 摘要 */
  getManifest: async (topN = 200): Promise<ManifestData> => {
    return api.get<ManifestData>('/form13f/manifest', { top_n: topN });
  },

  /** 机构搜索（分页，按组合规模降序） */
  searchFilers: async (q: string, page = 1, pageSize = 20): Promise<FilerSearchPage> => {
    return api.get<FilerSearchPage>('/form13f/filers', {
      q, page, page_size: pageSize,
    });
  },

  /** 机构详情：全季度快照（行业分布 / top20） */
  getFilerDetail: async (filerId: number): Promise<FilerDetail> => {
    return api.get<FilerDetail>(`/form13f/filers/${filerId}`);
  },

  /** 手动刷新（派发 Celery 任务） */
  refresh: async (type: RefreshType, datasetId?: number): Promise<{
    task_id: string | null;
    dispatched: string;
  }> => {
    return api.post<{ task_id: string | null; dispatched: string }>(
      '/form13f/refresh', { type, dataset_id: datasetId });
  },
};
