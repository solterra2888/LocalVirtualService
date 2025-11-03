// 调度器相关类型定义

export interface SchedulerTask {
  id: string;
  name: string;
  type: 'economic_events' | 'macro_indicators' | 'news';
  enabled: boolean;
  schedule: TaskSchedule;
  lastRun?: string;
  nextRun?: string;
  status: TaskStatus;
  successCount: number;
  failCount: number;
  avgDuration: number;
  description?: string;
  timeout?: number;
  retryCount?: number;
  params?: Record<string, any>;
}

export interface TaskSchedule {
  type: 'cron' | 'interval';
  cron?: string;
  interval?: number;
  timezone: string;
}

export type TaskStatus = 'running' | 'success' | 'failed' | 'idle' | 'paused';

export interface TaskExecution {
  id: string;
  taskId: string;
  taskName: string;
  startTime: string;
  endTime?: string;
  status: 'running' | 'success' | 'failed' | 'timeout' | 'cancelled';
  duration?: number;
  details?: string;
  recordsProcessed?: number;
  errorMessage?: string;
  logs?: string[];
}

export interface SchedulerConfig {
  maxConcurrentTasks: number;
  defaultTimeout: number;
  logLevel: 'DEBUG' | 'INFO' | 'WARNING' | 'ERROR';
  healthCheckInterval: number;
  metricsRetentionDays: number;
  alertChannels: AlertChannel[];
}

export interface AlertChannel {
  type: 'log' | 'webhook' | 'email';
  url?: string;
  email?: string;
  enabled: boolean;
}

export interface SchedulerStats {
  totalTasks: number;
  enabledTasks: number;
  runningTasks: number;
  todayExecutions: number;
  successRate: number;
  avgExecutionTime: number;
}

export interface TaskMetrics {
  taskId: string;
  taskName: string;
  executionCount: number;
  successCount: number;
  failureCount: number;
  avgDuration: number;
  lastExecution?: string;
  successRate: number;
}

// API 请求/响应类型
export interface CreateTaskRequest {
  name: string;
  type: SchedulerTask['type'];
  schedule: TaskSchedule;
  enabled?: boolean;
  description?: string;
  timeout?: number;
  retryCount?: number;
  params?: Record<string, any>;
}

export interface UpdateTaskRequest extends Partial<CreateTaskRequest> {
  id: string;
}

export interface SchedulerApiResponse<T = any> {
  success: boolean;
  data?: T;
  message?: string;
  error?: string;
}

export interface ExecutionHistoryParams {
  taskId?: string;
  status?: TaskExecution['status'];
  startDate?: string;
  endDate?: string;
  page?: number;
  pageSize?: number;
}

export interface ExecutionHistoryResponse {
  executions: TaskExecution[];
  total: number;
  page: number;
  pageSize: number;
  totalPages: number;
}

// 表单数据类型
export interface TaskFormData {
  name: string;
  type: SchedulerTask['type'];
  enabled: boolean;
  scheduleType: 'cron' | 'interval';
  cron?: string;
  interval?: number;
  timezone: string;
  description?: string;
  timeout?: number;
  retryCount?: number;
}

// 系统资源监控
export interface SystemResources {
  cpu: {
    usage: number;
    cores: number;
  };
  memory: {
    used: number;
    total: number;
    usage: number;
  };
  disk: {
    used: number;
    total: number;
    usage: number;
  };
  network: {
    bytesIn: number;
    bytesOut: number;
  };
}

export interface SchedulerHealth {
  status: 'healthy' | 'degraded' | 'down';
  uptime: number;
  version: string;
  lastCheck: string;
  services: {
    database: 'up' | 'down';
    scheduler: 'up' | 'down';
    api: 'up' | 'down';
  };
}


