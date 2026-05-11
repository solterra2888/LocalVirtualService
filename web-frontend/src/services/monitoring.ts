import { api } from './api';

// ---------------------------------------------------------------------------
// Backend DTOs (mirror backend/api/monitoring.py)
// ---------------------------------------------------------------------------

export interface QueueInfo {
  name: string;
  depth: number;              // -1 表示探测失败
  error?: string | null;
}

export interface WorkerInfo {
  name: string;
  online: boolean;
  queues: string[];           // 订阅队列列表（可能为空）
  active_count: number;
  reserved_count: number;
  scheduled_count: number;
  processed_total?: number | null;
  pool_max_concurrency?: number | null;
  pool_processes?: number | null;
}

export interface QueuesResponse {
  queues: QueueInfo[];
  total_depth: number;
  fetched_at: number;         // unix seconds
}

export interface WorkersResponse {
  workers: WorkerInfo[];
  online_count: number;
  fetched_at: number;
  inspect_cache_age_sec: number;
}

export interface LogFileStats {
  type: string;
  filename: string;
  size_human: string;
  size_kb: number;
  mtime?: number | null;
  exists: boolean;
}

export interface SummaryResponse {
  queues: QueueInfo[];
  total_depth: number;
  workers: WorkerInfo[];
  online_count: number;
  active_task_count: number;
  fetched_at: number;
  inspect_cache_age_sec: number;
  log_files: LogFileStats[];
  recent_errors_1h: number;
}

export interface PurgeResponse {
  queue: string;
  removed: number;
  message: string;
}

// ---------------------------------------------------------------------------
// API calls
// ---------------------------------------------------------------------------

/** 一次性拿全部监控数据（队列 + worker + 汇总）。首屏 / 5s 轮询主接口 */
export const getMonitoringSummary = async (
  forceRefresh: boolean = false,
): Promise<SummaryResponse> => {
  return api.get<SummaryResponse>('/monitoring/summary', {
    force_refresh: forceRefresh,
  });
};

/** 仅队列深度，极轻量（<1ms），可用于单独高频刷新 */
export const getQueueDepths = async (): Promise<QueuesResponse> => {
  return api.get<QueuesResponse>('/monitoring/queues');
};

/** 仅 worker 列表与心跳 */
export const getWorkers = async (
  forceRefresh: boolean = false,
): Promise<WorkersResponse> => {
  return api.get<WorkersResponse>('/monitoring/workers', {
    force_refresh: forceRefresh,
  });
};

/**
 * 清空指定队列（admin + 二次确认）
 *
 * 后端会校验: queueName 必须是 KNOWN_QUEUES 里登记的，且 confirm === queueName
 */
export const purgeQueue = async (queueName: string): Promise<PurgeResponse> => {
  return api.post<PurgeResponse>(
    `/monitoring/queues/${encodeURIComponent(queueName)}/purge?confirm=${encodeURIComponent(queueName)}`,
  );
};
