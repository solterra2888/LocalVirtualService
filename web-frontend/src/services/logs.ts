import { api } from './api';

export interface LogInfo {
  type: string;
  filename: string;
  size_kb: number;
  exists: boolean;
}

export interface LogListResponse {
  logs: LogInfo[];
}

export interface LogContentResponse {
  filename: string;
  content: string;
  lines: number;
  size_kb: number;
}

export type LogType = 'app' | 'celery_worker';

/**
 * 获取可用日志列表
 */
export const getLogList = async (): Promise<LogListResponse> => {
  return api.get<LogListResponse>('/logs/list');
};

/**
 * 读取日志内容
 * @param logType 日志类型
 * @param lines 读取最后N行，不提供则读取全部
 */
export const readLog = async (logType: LogType, lines?: number): Promise<LogContentResponse> => {
  const params = lines ? { lines } : {};
  return api.get<LogContentResponse>(`/logs/${logType}`, params);
};

/**
 * 清空日志文件
 * @param logType 日志类型
 */
export const clearLog = async (logType: LogType): Promise<{ message: string; filename: string }> => {
  return api.delete(`/logs/${logType}/clear`);
};
