export type LogLevel = 'info' | 'warning' | 'error' | 'debug' | 'success';

export interface LogEntry {
  id: string;
  timestamp: string;
  level: LogLevel;
  message: string;
  source?: string; // 日志来源，如 'file-processing', 'user-management' 等
  details?: any; // 额外的详细信息
}

export interface LoggerState {
  logs: LogEntry[];
  maxLogs: number;
  autoScroll: boolean;
  showModal: boolean;
  showFloating: boolean;
  minimizedFloating: boolean;
}

export interface LoggerActions {
  addLog: (log: Omit<LogEntry, 'id' | 'timestamp'>) => void;
  clearLogs: () => void;
  downloadLogs: (filename?: string) => void;
  setAutoScroll: (enabled: boolean) => void;
  setShowModal: (show: boolean) => void;
  setShowFloating: (show: boolean) => void;
  setMinimizedFloating: (minimized: boolean) => void;
  getLogsBySource: (source: string) => LogEntry[];
  getLogsByLevel: (level: LogLevel) => LogEntry[];
}

export interface LoggerConfig {
  maxLogs?: number;
  defaultSource?: string;
  enableFloating?: boolean;
  enableModal?: boolean;
  autoScroll?: boolean;
} 