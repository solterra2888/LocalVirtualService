import apiClient from './api';
import { api } from './api';

// ---------------------------------------------------------------------------
// Response types (mirror backend/api/logs.py)
// ---------------------------------------------------------------------------
export interface LogFileInfo {
  type: string;
  filename: string;
  size_kb: number;
  size_human: string;
  exists: boolean;
  mtime?: number;
}

export interface LogListResponse {
  logs: LogFileInfo[];
}

export interface LogContentResponse {
  filename: string;
  content: string;
  file_total_lines: number;
  returned_lines: number;
  size_kb: number;
  size_human: string;
  truncated: boolean;
}

// ---------------------------------------------------------------------------
// API calls
// ---------------------------------------------------------------------------

/** 获取可用日志文件列表 */
export const getLogList = async (): Promise<LogListResponse> =>
  api.get<LogListResponse>('/logs/list');

/** 读取日志内容，支持 tail、级别过滤、关键字搜索 */
export const readLog = async (
  logType: string,
  options?: { lines?: number | null; level?: string; q?: string },
): Promise<LogContentResponse> => {
  const params: Record<string, string | number> = {};
  if (options?.lines && options.lines > 0) params.lines = options.lines;
  if (options?.level) params.level = options.level;
  if (options?.q) params.q = options.q;
  return api.get<LogContentResponse>(`/logs/${logType}`, params);
};

/**
 * 下载完整日志文件。
 * 使用 Axios blob 响应 + 客户端触发，确保 Bearer token 被携带。
 */
export const downloadLog = async (logType: string): Promise<void> => {
  // baseURL 已是 /api，路径不要再带 /api 前缀（否则会请求 /api/api/logs/...）
  const response = await apiClient.get(`/logs/${logType}/download`, {
    responseType: 'blob',
  });
  const blob: Blob = response.data;
  const filename =
    (response.headers['content-disposition'] || '')
      .match(/filename="?([^"]+)"?/)?.[1] ?? `${logType}.log`;
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
};

/**
 * 清空日志文件。
 * 需要 confirm=logType 作为二次确认（与 purge queue 安全机制一致）。
 */
export const clearLog = async (logType: string): Promise<{ message: string; filename: string }> =>
  api.delete(`/logs/${logType}/clear`, { params: { confirm: logType } });
