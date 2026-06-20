// 定时任务调度器服务
import { api } from './api';
import apiClient from './api';

// 类型定义
export interface ScheduledTask {
  id: string;
  name: string;
  type: 'eodhd_news' | 'watchlist_news' | 'cctv_news' | 'economic_events' | 'macro_indicators' | 'eodhd_fundamentals' | 'finviz_screener' | 'eodhd_stock_prices' | 'futu_themes' | 'market_data' | 'earnings_calendar' | 'ipo_calendar' | 'watchlist_fundamentals' | 'twitter_subscription' | 'youtube_subscription' | 'podcast_subscription' | 'news_feed' | 'zhishixingqiu_subscription' | 'twitter_author_summary' | 'youtube_author_summary' | 'theme_report' | 'market_weekly_report' | 'daily_market_report' | 'the_close_summary' | 'cnbc_closing_bell' | 'zhishixingqiu_summary';
  schedule_type: 'cron' | 'interval';
  cron_expression?: string;
  interval_minutes?: number;
  timezone: string;
  enabled: boolean;
  params: Record<string, any>;
  created_at: string;
  last_run?: string;
  next_run?: string;
  success_count: number;
  fail_count: number;
  status?: 'running' | 'paused' | 'disabled';
}

export interface CreateCronTaskParams {
  name: string;
  task_type: string;
  cron_expression: string;
  timezone?: string;
  enabled?: boolean;
  params?: Record<string, any>;
}

export interface CreateIntervalTaskParams {
  name: string;
  task_type: string;
  interval_minutes: number;
  timezone?: string;
  enabled?: boolean;
  params?: Record<string, any>;
}

export interface CreateEODHDNewsTaskParams {
  name?: string;
  cron_expression?: string;
  timezone?: string;
  enabled?: boolean;
  max_results?: number;
  symbols?: string;
  topic?: string;
  topic_filter?: string;
  keywords?: string;
  recent_days?: number;
  // LLM 新闻分析相关参数
  auto_analyze?: boolean;
  use_celery?: boolean;
  analysis_template?: string;
  analysis_batch_size?: number;
  max_concurrent?: number;
}

export interface CreateEODHDFundamentalsTaskParams {
  name?: string;
  cron_expression?: string;
  timezone?: string;
  enabled?: boolean;
  exchanges?: string[];
  data_types?: string[];
}

export interface CreateFinvizScreenerTaskParams {
  name?: string;
  cron_expression?: string;
  timezone?: string;
  enabled?: boolean;
}

export interface CreateWatchlistNewsTaskParams {
  name?: string;
  cron_expression?: string;
  timezone?: string;
  enabled?: boolean;
  max_results?: number;
  recent_days?: number;
  user_id?: number;
  // LLM 新闻分析相关参数
  auto_analyze?: boolean;
  use_celery?: boolean;
  analysis_template?: string;
  analysis_batch_size?: number;
  max_concurrent?: number;
}

export interface CreateCCTVNewsTaskParams {
  name?: string;
  cron_expression?: string;
  timezone?: string;
  enabled?: boolean;
  // 爬取参数
  date?: string;  // YYYYMMDD格式
  max_news?: number;
  // 🔥 LLM分析参数
  auto_analyze?: boolean;
  analysis_templates?: string[];  // 多模板支持
  max_concurrent?: number;
  enable_theme_matching?: boolean;
}

export interface CreateEODHDStockPricesTaskParams {
  name?: string;
  cron_expression?: string;
  timezone?: string;
  enabled?: boolean;
}

export interface CreateEODHDEODStockPricesTaskParams {
  name?: string;
  cron_expression?: string;
  timezone?: string;
  enabled?: boolean;
  days_back?: number;
}

export interface CreateFutuThemesTaskParams {
  name?: string;
  cron_expression?: string;
  timezone?: string;
  enabled?: boolean;
  markets?: string[];
  max_pages?: number;
  scrape_stocks?: boolean;
}

export interface CreateMarketDataTaskParams {
  name?: string;
  cron_expression?: string;
  timezone?: string;
  enabled?: boolean;
  asset_types?: string[];
  fetch_realtime?: boolean;
  fetch_eod?: boolean;
  eod_days?: number;
}

export interface CreateEarningsCalendarTaskParams {
  name?: string;
  cron_expression?: string;
  timezone?: string;
  enabled?: boolean;
  start_date?: string;
  end_date?: string;
  days?: number;
  fetch_next_week?: boolean;
}

export interface CreateIPOCalendarTaskParams {
  name?: string;
  cron_expression?: string;
  timezone?: string;
  enabled?: boolean;
  start_date?: string;
  end_date?: string;
  days?: number;
  fetch_next_week?: boolean;
  // 注意：EODHD API 不支持 exchanges 参数，已移除
  // 如果需要过滤特定交易所，可以在查询数据库时使用 WHERE 条件
}

export interface CreateWatchlistFundamentalsTaskParams {
  name?: string;
  cron_expression?: string;
  timezone?: string;
  enabled?: boolean;
  data_types?: string[];
  skip_recent_hours?: number;
}

export interface CronPreset {
  name: string;
  expression: string;
  description: string;
}

export interface TimezoneOption {
  value: string;
  label: string;
  description: string;
}

// API响应类型
export interface APIResponse<T> {
  success: boolean;
  message: string;
  data?: T;
}

export interface TaskListResponse extends APIResponse<ScheduledTask[]> {}

export interface TaskCreateResponse extends APIResponse<{
  task_id: string;
  task_config: ScheduledTask;
}> {}

export interface CronPresetsResponse extends APIResponse<CronPreset[]> {}

export interface ExecutionLog {
  id: string;
  task_id: string;
  task_name: string;
  task_type: string;
  started_at: string;
  finished_at?: string;
  duration_ms?: number;
  status: 'running' | 'success' | 'failed' | 'timeout' | 'cancelled' | 'skipped';
  result_message?: string;
  result_data?: any;
  error_message?: string;
  error_traceback?: string;
  execution_params?: any;
}

export interface ExecutionSummaryDailyStat {
  date: string;
  total_runs: number;
  success_count: number;
  failure_count: number;
  running_count: number;
  timeout_count: number;
  cancelled_count: number;
  skipped_count: number;
  avg_duration_ms?: number | null;
  success_rate?: number | null;
}

export interface ExecutionSummaryTask {
  task_type: string;
  task_name: string;
  daily_stats: ExecutionSummaryDailyStat[];
  totals: {
    total_runs: number;
    success_count: number;
    failure_count: number;
    running_count: number;
    timeout_count: number;
    cancelled_count: number;
    skipped_count: number;
    success_rate?: number | null;
  };
}

export interface ExecutionSummaryData {
  timezone: string;
  days: number;
  start: string;
  end: string;
  tasks: ExecutionSummaryTask[];
}

export interface ExecutionSummaryResponse extends APIResponse<ExecutionSummaryData> {}

export interface ExecutionLogsResponse extends APIResponse<{
  logs: ExecutionLog[];
  total: number;
  limit: number;
  offset: number;
}> {}

export interface TimezonePresetsResponse extends APIResponse<TimezoneOption[]> {}

// 调度器API服务类
export class SchedulerAPI {
  
  // ============ 任务管理API ============
  
  /**
   * 获取所有定时任务
   */
  async getTasks(): Promise<TaskListResponse> {
    try {
      const response = await api.get('/scheduler/tasks');
      return {
        success: true,
        message: '获取任务列表成功',
        data: response.data || response
      };
    } catch (error: any) {
      console.error('获取任务列表失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '获取任务列表失败',
        data: []
      };
    }
  }

  /**
   * 创建Cron定时任务
   */
  async createCronTask(params: CreateCronTaskParams): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/cron', params);
      return {
        success: true,
        message: response.data?.message || '创建定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建Cron定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建定时任务失败'
      };
    }
  }

  /**
   * 创建间隔定时任务
   */
  async createIntervalTask(params: CreateIntervalTaskParams): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/interval', params);
      return {
        success: true,
        message: response.data?.message || '创建定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建间隔定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建定时任务失败'
      };
    }
  }

  /**
   * 启用任务
   */
  async enableTask(taskId: string): Promise<APIResponse<null>> {
    try {
      const response = await api.post(`/scheduler/tasks/${taskId}/enable`);
      return {
        success: true,
        message: response.data?.message || '任务已启用'
      };
    } catch (error: any) {
      console.error('启用任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '启用任务失败'
      };
    }
  }

  /**
   * 禁用任务
   */
  async disableTask(taskId: string): Promise<APIResponse<null>> {
    try {
      const response = await api.post(`/scheduler/tasks/${taskId}/disable`);
      return {
        success: true,
        message: response.data?.message || '任务已禁用'
      };
    } catch (error: any) {
      console.error('禁用任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '禁用任务失败'
      };
    }
  }

  /**
   * 删除任务
   */
  async deleteTask(taskId: string): Promise<APIResponse<null>> {
    try {
      const response = await api.delete(`/scheduler/tasks/${taskId}`);
      return {
        success: true,
        message: response.data?.message || '任务已删除'
      };
    } catch (error: any) {
      console.error('删除任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '删除任务失败'
      };
    }
  }

  // ============ EODHD新闻任务专用API ============

  /**
   * 创建EODHD新闻定时任务
   */
  async createEODHDNewsTask(params: CreateEODHDNewsTaskParams): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/eodhd-news', params);
      return {
        success: true,
        message: response.data?.message || '创建EODHD新闻定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建EODHD新闻定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建EODHD新闻定时任务失败'
      };
    }
  }

  /**
   * 创建 News Feed (My Feeds) 定时任务
   */
  async createNewsFeedTask(params: {
    name: string;
    cron_expression: string;
    timezone: string;
    enabled: boolean;
    max_results?: number;
    recent_days?: number;
  }): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/news-feed', params);
      return {
        success: true,
        message: response.data?.message || '创建 News Feed 定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建 News Feed 定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建 News Feed 定时任务失败'
      };
    }
  }

  /**
   * 创建EODHD基本面数据定时任务
   */
  async createEODHDFundamentalsTask(params: CreateEODHDFundamentalsTaskParams): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/eodhd-fundamentals', params);
      return {
        success: true,
        message: response.data?.message || '创建EODHD基本面数据定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建EODHD基本面数据定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建EODHD基本面数据定时任务失败'
      };
    }
  }

  /**
   * 手动执行EODHD基本面数据更新
   */
  async executeFundamentalsUpdate(params: { exchanges: string[]; data_types: string[] }): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/execute/eodhd-fundamentals', params);
      return {
        success: true,
        message: response.data?.message || 'EODHD基本面数据更新已启动',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('执行EODHD基本面数据更新失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '执行EODHD基本面数据更新失败'
      };
    }
  }

  /**
   * 获取任务详细状态（包含执行统计）
   */
  async getTaskStatus(taskId: string): Promise<APIResponse<any>> {
    try {
      const response = await api.get(`/scheduler/tasks/${taskId}/status`);
      return {
        success: true,
        data: response.data,
        message: '获取任务状态成功'
      };
    } catch (error: any) {
      console.error('获取任务状态失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '获取任务状态失败'
      };
    }
  }

  /**
   * 获取任务执行历史
   */
  async getTaskHistory(taskId: string, limit: number = 10): Promise<APIResponse<any[]>> {
    try {
      const response = await api.get(`/scheduler/tasks/${taskId}/history?limit=${limit}`);
      return {
        success: true,
        data: response.data || [],
        message: '获取任务历史成功'
      };
    } catch (error: any) {
      console.error('获取任务历史失败:', error);
      return {
        success: false,
        data: [],
        message: error.response?.data?.detail || error.message || '获取任务历史失败'
      };
    }
  }

  /**
   * 创建Finviz筛选数据定时任务
   */
  async createFinvizScreenerTask(params: CreateFinvizScreenerTaskParams): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/finviz-screener', params);
      return {
        success: true,
        message: response.data?.message || '创建Finviz筛选数据定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建Finviz筛选数据定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建Finviz筛选数据定时任务失败'
      };
    }
  }

  /**
   * 创建EODHD股票价格定时任务
   */
  async createEODHDStockPricesTask(params: CreateEODHDStockPricesTaskParams): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/eodhd-stock-prices', params);
      return {
        success: true,
        message: response.data?.message || '创建EODHD股票价格定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建EODHD股票价格定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建EODHD股票价格定时任务失败'
      };
    }
  }

  /**
   * 创建EODHD历史股价定时任务
   */
  async createEODHDEODStockPricesTask(params: CreateEODHDEODStockPricesTaskParams): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/eodhd-eod-stock-prices', params);
      return {
        success: true,
        message: response.data?.message || '创建EODHD历史股价定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建EODHD历史股价定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建EODHD历史股价定时任务失败'
      };
    }
  }

  /**
   * 创建EODHD每周历史回填定时任务（兜底，400天日线）
   * 每周六 05:30 纽约时间执行，补齐新 ticker 首次回填偶发遗漏的历史缺口，
   * 保证 Theme Report 的 1Y（回溯 364 日历日）有足够历史。
   */
  async createEODHDEODStockPricesWeeklyBackfillTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/eodhd-eod-stock-prices/weekly-backfill');
      return {
        success: true,
        message: response.data?.message || '创建每周EOD历史回填定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建每周EOD历史回填定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建每周EOD历史回填定时任务失败'
      };
    }
  }

  /**
   * 创建默认EODHD新闻定时任务
   */
  async createDefaultEODHDNewsTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/eodhd-news/default');
      return {
        success: true,
        message: response.data?.message || '创建默认EODHD新闻定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建默认EODHD新闻定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建默认EODHD新闻定时任务失败'
      };
    }
  }

  /**
   * 创建Watchlist新闻定时任务
   */
  async createWatchlistNewsTask(params: CreateWatchlistNewsTaskParams): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/watchlist-news', params);
      return {
        success: true,
        message: response.data?.message || '创建Watchlist新闻定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建Watchlist新闻定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建Watchlist新闻定时任务失败'
      };
    }
  }

  /**
   * 创建默认Watchlist新闻定时任务
   */
  async createDefaultWatchlistNewsTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/watchlist-news/default');
      return {
        success: true,
        message: response.data?.message || '创建默认Watchlist新闻定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建默认Watchlist新闻定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建默认Watchlist新闻定时任务失败'
      };
    }
  }

  /**
   * 创建CCTV新闻定时任务
   */
  async createCCTVNewsTask(params: CreateCCTVNewsTaskParams): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/cctv-news', params);
      return {
        success: true,
        message: response.data?.message || '创建CCTV新闻定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建CCTV新闻定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建CCTV新闻定时任务失败'
      };
    }
  }

  /**
   * 创建默认CCTV新闻定时任务
   */
  async createDefaultCCTVNewsTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/cctv-news/default');
      return {
        success: true,
        message: response.data?.message || '创建默认CCTV新闻定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建默认CCTV新闻定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建默认CCTV新闻定时任务失败'
      };
    }
  }

  /**
   * 创建默认EODHD股票价格定时任务
   */
  async createDefaultEODHDStockPricesTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/eodhd-stock-prices/default');
      return {
        success: true,
        message: response.data?.message || '创建默认EODHD股票价格定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建默认EODHD股票价格定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建默认EODHD股票价格定时任务失败'
      };
    }
  }

  /**
   * 创建Futu投资主题定时任务
   */
  async createFutuThemesTask(params: CreateFutuThemesTaskParams): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/futu-themes', params);
      return {
        success: true,
        message: response.data?.message || '创建Futu投资主题定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建Futu投资主题定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建Futu投资主题定时任务失败'
      };
    }
  }

  /**
   * 创建默认Futu投资主题定时任务
   */
  async createDefaultFutuThemesTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/futu-themes/default');
      return {
        success: true,
        message: response.data?.message || '创建默认Futu投资主题定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建默认Futu投资主题定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建默认Futu投资主题定时任务失败'
      };
    }
  }

  /**
   * 创建市场数据定时任务
   */
  async createMarketDataTask(params: CreateMarketDataTaskParams): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/market-data', params);
      return {
        success: true,
        message: response.data?.message || '创建市场数据定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建市场数据定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建市场数据定时任务失败'
      };
    }
  }

  /**
   * 创建默认市场数据定时任务
   */
  async createDefaultMarketDataTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/market-data/default');
      return {
        success: true,
        message: response.data?.message || '创建默认市场数据定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建默认市场数据定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建默认市场数据定时任务失败'
      };
    }
  }

  /**
   * 创建财报日历定时任务
   */
  async createEarningsCalendarTask(params: CreateEarningsCalendarTaskParams): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/earnings-calendar', params);
      return {
        success: true,
        message: response.data?.message || '创建财报日历定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建财报日历定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建财报日历定时任务失败'
      };
    }
  }

  /**
   * 创建默认财报日历定时任务
   */
  async createDefaultEarningsCalendarTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/earnings-calendar/default');
      return {
        success: true,
        message: response.data?.message || '创建默认财报日历定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建默认财报日历定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建默认财报日历定时任务失败'
      };
    }
  }

  /**
   * 创建 IPO 日历定时任务
   */
  async createIPOCalendarTask(params: CreateIPOCalendarTaskParams): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/ipo-calendar', params);
      return {
        success: true,
        message: response.data?.message || '创建 IPO 日历定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建 IPO 日历定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建 IPO 日历定时任务失败'
      };
    }
  }

  /**
   * 创建默认 IPO 日历定时任务
   */
  async createDefaultIPOCalendarTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/ipo-calendar/default');
      return {
        success: true,
        message: response.data?.message || '创建默认 IPO 日历定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建默认 IPO 日历定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建默认 IPO 日历定时任务失败'
      };
    }
  }

  /**
   * 创建 Watchlist Fundamentals 定时任务
   */
  async createWatchlistFundamentalsTask(params: CreateWatchlistFundamentalsTaskParams): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/watchlist-fundamentals', params);
      return {
        success: true,
        message: response.data?.message || '创建 Watchlist Fundamentals 定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建 Watchlist Fundamentals 定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建 Watchlist Fundamentals 定时任务失败'
      };
    }
  }

  /**
   * 创建默认 Watchlist Fundamentals 定时任务
   */
  async createDefaultWatchlistFundamentalsTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/watchlist-fundamentals/default');
      return {
        success: true,
        message: response.data?.message || '创建默认 Watchlist Fundamentals 定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建默认 Watchlist Fundamentals 定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建默认 Watchlist Fundamentals 定时任务失败'
      };
    }
  }

  /**
   * 创建 Twitter 作者摘要定时任务（阶段 A）
   */
  async createTwitterAuthorSummaryTask(params: {
    name?: string;
    cron_expression?: string;
    timezone?: string;
    enabled?: boolean;
    chain_daily_reports?: boolean;
    force?: boolean;
  }): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/twitter-author-summary', params);
      return {
        success: true,
        message: response.data?.message || '创建 Twitter 作者摘要定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建 Twitter 作者摘要定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建 Twitter 作者摘要定时任务失败'
      };
    }
  }

  /**
   * 创建默认 Twitter 作者摘要定时任务
   */
  async createDefaultTwitterAuthorSummaryTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/twitter-author-summary/default');
      return {
        success: true,
        message: response.data?.message || '创建默认 Twitter 作者摘要定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建默认 Twitter 作者摘要定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建默认 Twitter 作者摘要定时任务失败'
      };
    }
  }

  /**
   * 创建 YouTube 频道摘要定时任务（阶段 A）
   */
  async createYouTubeAuthorSummaryTask(params: {
    name?: string;
    cron_expression?: string;
    timezone?: string;
    enabled?: boolean;
    chain_daily_reports?: boolean;
    force?: boolean;
  }): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/youtube-author-summary', params);
      return {
        success: true,
        message: response.data?.message || '创建 YouTube 频道摘要定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建 YouTube 频道摘要定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建 YouTube 频道摘要定时任务失败'
      };
    }
  }

  /**
   * 创建默认 YouTube 频道摘要定时任务
   */
  async createDefaultYouTubeAuthorSummaryTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/youtube-author-summary/default');
      return {
        success: true,
        message: response.data?.message || '创建默认 YouTube 频道摘要定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建默认 YouTube 频道摘要定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建默认 YouTube 频道摘要定时任务失败'
      };
    }
  }

  /**
   * 创建 Theme Report 定时任务
   */
  async createThemeReportTask(params: {
    name?: string;
    cron_expression?: string;
    timezone?: string;
    enabled?: boolean;
    report_date?: string;
    user_id?: string;
  }): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/theme-report', params);
      return {
        success: true,
        message: response.data?.message || '创建 Theme Report 定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建 Theme Report 定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建 Theme Report 定时任务失败'
      };
    }
  }

  /**
   * 创建默认 Theme Report 定时任务
   */
  async createDefaultThemeReportTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/theme-report/default');
      return {
        success: true,
        message: response.data?.message || '创建默认 Theme Report 定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建默认 Theme Report 定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建默认 Theme Report 定时任务失败'
      };
    }
  }

  /**
   * 创建 Market Weekly Report 定时任务
   */
  async createMarketWeeklyReportTask(params: {
    name?: string;
    cron_expression?: string;
    timezone?: string;
    enabled?: boolean;
    user_id?: string;
    week_end?: string;
  }): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/market-weekly-report', params);
      return {
        success: true,
        message: response.data?.message || '创建 Market Weekly Report 定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建 Market Weekly Report 定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建 Market Weekly Report 定时任务失败'
      };
    }
  }

  /**
   * 创建默认 Market Weekly Report 定时任务
   */
  async createDefaultMarketWeeklyReportTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/market-weekly-report/default');
      return {
        success: true,
        message: response.data?.message || '创建默认 Market Weekly Report 定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建默认 Market Weekly Report 定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建默认 Market Weekly Report 定时任务失败'
      };
    }
  }

  /**
   * 创建 Daily Market Report 定时任务
   */
  async createDailyMarketReportTask(params: {
    name?: string;
    cron_expression?: string;
    timezone?: string;
    enabled?: boolean;
    report_date?: string;
    user_id?: string;
  }): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/daily-market-report', params);
      return {
        success: true,
        message: response.data?.message || '创建 Daily Market Report 定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建 Daily Market Report 定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建 Daily Market Report 定时任务失败'
      };
    }
  }

  /**
   * 创建默认 Daily Market Report 定时任务
   */
  async createDefaultDailyMarketReportTask(): Promise<TaskCreateResponse> {
    try {
      const response = await api.post('/scheduler/tasks/daily-market-report/default');
      return {
        success: true,
        message: response.data?.message || '创建默认 Daily Market Report 定时任务成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('创建默认 Daily Market Report 定时任务失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '创建默认 Daily Market Report 定时任务失败'
      };
    }
  }

  // ============ 预设配置API ============

  /**
   * 获取Cron表达式预设
   */
  async getCronPresets(): Promise<CronPresetsResponse> {
    try {
      const response = await api.get('/scheduler/presets/cron-expressions');
      return {
        success: true,
        message: '获取Cron表达式预设成功',
        data: response.data?.data || []
      };
    } catch (error: any) {
      console.error('获取Cron表达式预设失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '获取Cron表达式预设失败',
        data: []
      };
    }
  }

  /**
   * Get timezone options
   */
  async getTimezonePresets(): Promise<TimezonePresetsResponse> {
    try {
      const response = await api.get('/scheduler/presets/timezones');
      console.log('Timezone API raw response:', response);
      
      // Process data based on backend API response structure
      const timezoneData = response.data;
      console.log('Processed timezone data:', timezoneData);
      
      return {
        success: true,
        message: 'Successfully retrieved timezone options',
        data: timezoneData
      };
    } catch (error: any) {
      console.error('Failed to get timezone options:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || 'Failed to get timezone options',
        data: []
      };
    }
  }

  // ============ 任务日志API ============



  // ============ 数据库表查看API ============

  /**
   * 查看数据库表数据
   */
  async getDatabaseTable(tableName: string, limit: number = 50): Promise<APIResponse<any>> {
    try {
      console.log(`发起API请求: /scheduler/debug/database-tables?table_name=${tableName}&limit=${limit}`);
      const response = await api.get(`/scheduler/debug/database-tables?table_name=${tableName}&limit=${limit}`);
      console.log('API原始响应:', response);
      
      return {
        success: true,
        message: '获取数据库表数据成功',
        data: response.data || response  // 兼容不同的响应结构
      };
    } catch (error: any) {
      console.error('获取数据库表数据失败:', error);
      console.error('错误详情:', {
        status: error.response?.status,
        statusText: error.response?.statusText,
        data: error.response?.data,
        message: error.message
      });
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '获取数据库表数据失败',
        data: null
      };
    }
  }

  /**
   * 查看表结构
   */
  async getTableStructure(): Promise<APIResponse<any>> {
    try {
      const response = await api.get('/scheduler/debug/table-structure');
      return {
        success: true,
        message: '获取表结构成功',
        data: response.data?.data
      };
    } catch (error: any) {
      console.error('获取表结构失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '获取表结构失败'
      };
    }
  }

  // ============ 实用工具方法 ============

  /**
   * 验证Cron表达式格式
   */
  validateCronExpression(expression: string): { valid: boolean; message?: string } {
    if (!expression || typeof expression !== 'string') {
      return { valid: false, message: 'Cron表达式不能为空' };
    }

    const parts = expression.trim().split(/\s+/);
    if (parts.length !== 5) {
      return { valid: false, message: 'Cron表达式必须包含5个部分：分 时 日 月 星期' };
    }

    // 基本格式验证
    const [minute, hour, day, month, dayOfWeek] = parts;
    
    // 简单的格式检查
    const isValidPart = (part: string, min: number, max: number) => {
      if (part === '*') return true;
      if (part.includes('/')) {
        const [base, step] = part.split('/');
        if (base === '*' && !isNaN(Number(step))) return true;
      }
      if (part.includes('-')) {
        const [start, end] = part.split('-');
        const startNum = Number(start);
        const endNum = Number(end);
        return !isNaN(startNum) && !isNaN(endNum) && startNum >= min && endNum <= max;
      }
      if (part.includes(',')) {
        return part.split(',').every(p => {
          const num = Number(p);
          return !isNaN(num) && num >= min && num <= max;
        });
      }
      const num = Number(part);
      return !isNaN(num) && num >= min && num <= max;
    };

    if (!isValidPart(minute, 0, 59)) {
      return { valid: false, message: '分钟值无效（0-59）' };
    }
    if (!isValidPart(hour, 0, 23)) {
      return { valid: false, message: '小时值无效（0-23）' };
    }
    if (!isValidPart(day, 1, 31)) {
      return { valid: false, message: '日期值无效（1-31）' };
    }
    if (!isValidPart(month, 1, 12)) {
      return { valid: false, message: '月份值无效（1-12）' };
    }
    if (!isValidPart(dayOfWeek, 0, 6)) {
      return { valid: false, message: '星期值无效（0-6，其中0表示周日）' };
    }

    return { valid: true };
  }

  /**
   * 获取Cron表达式的人性化描述
   */
  describeCronExpression(expression: string): string {
    const { valid } = this.validateCronExpression(expression);
    if (!valid) return '无效的Cron表达式';

    const [minute, hour] = expression.split(/\s+/);

    // 简单的描述生成
    if (expression === '* * * * *') return '每分钟执行';
    if (expression === '*/5 * * * *') return '每5分钟执行';
    if (expression === '*/15 * * * *') return '每15分钟执行';
    if (expression === '*/30 * * * *') return '每30分钟执行';
    if (expression === '0 * * * *') return '每小时执行';
    if (expression === '0 9 * * 1-5') return '工作日上午9点执行';
    if (expression === '0 17 * * 1-5') return '工作日下午5点执行';
    if (expression === '0 10 * * 1') return '每周一上午10点执行';
    if (expression === '0 9 1 * *') return '每月1号上午9点执行';

    return `${minute === '*' ? '每分钟' : `${minute}分`} ${hour === '*' ? '每小时' : `${hour}时`} 执行`;
  }

  /**
   * 获取任务执行日志
   */
  async getTaskLogs(taskId: string, limit: number = 50, offset: number = 0): Promise<ExecutionLogsResponse> {
    try {
      const response = await api.get<ExecutionLogsResponse>(`/scheduler/tasks/${taskId}/logs`, {
        limit,
        offset
      });
      return response;
    } catch (error) {
      console.error('获取任务执行日志失败:', error);
      throw error;
    }
  }

  /**
   * 获取所有任务执行日志
   */
  async getAllExecutionLogs(limit: number = 50, offset: number = 0, taskTypes?: string[]): Promise<ExecutionLogsResponse> {
    try {
      const params: any = {
        limit,
        offset
      };
      
      // 如果提供了 task_types，转换为逗号分隔的字符串
      if (taskTypes && taskTypes.length > 0) {
        params.task_types = taskTypes.join(',');
      }
      
      const response = await api.get<ExecutionLogsResponse>('/scheduler/execution-logs', params);
      return response;
    } catch (error) {
      console.error('获取所有任务执行日志失败:', error);
      throw error;
    }
  }

  /**
   * 获取执行日志统计
   */
  async getExecutionSummary(days: number = 7, timezone: string = 'America/New_York'): Promise<ExecutionSummaryResponse> {
    try {
      const response = await api.get<ExecutionSummaryResponse>('/scheduler/execution-logs/summary', {
        days,
        timezone
      });
      return response;
    } catch (error: any) {
      console.error('获取执行日志统计失败:', error);
      return {
        success: false,
        message: error.response?.data?.detail || error.message || '获取执行日志统计失败'
      };
    }
  }

  /**
   * 下载执行日志CSV
   */
  async downloadExecutionLogsCSV(taskId?: string, limit: number = 1000, offset: number = 0): Promise<void> {
    try {
      console.log('开始下载CSV，参数:', { taskId, limit, offset });
      
      const params = new URLSearchParams();
      if (taskId) {
        params.append('task_id', taskId);
      }
      params.append('limit', limit.toString());
      params.append('offset', offset.toString());

      const url = `/scheduler/execution-logs/download?${params.toString()}`;
      console.log('下载URL:', url);

      // 直接使用axios实例，避免extractData处理
      const response = await apiClient.get(url, {
        responseType: 'blob'
      });

      console.log('下载响应:', {
        status: response.status,
        headers: response.headers,
        dataSize: response.data?.size
      });

      // 创建下载链接
      const blob = new Blob([response.data], { type: 'text/csv;charset=utf-8;' });
      const link = document.createElement('a');
      const url_blob = URL.createObjectURL(blob);
      
      // 从响应头获取文件名，如果没有则使用默认名称
      const contentDisposition = response.headers['content-disposition'];
      let filename = 'execution_logs.csv';
      if (contentDisposition) {
        const filenameMatch = contentDisposition.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
        if (filenameMatch && filenameMatch[1]) {
          filename = filenameMatch[1].replace(/['"]/g, '');
        }
      }
      
      console.log('下载文件名:', filename);
      
      link.setAttribute('href', url_blob);
      link.setAttribute('download', filename);
      link.style.visibility = 'hidden';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url_blob);
      
      console.log('CSV下载完成');
    } catch (error: any) {
      console.error('下载执行日志CSV失败:', error);
      console.error('错误详情:', {
        message: error?.message,
        status: error?.response?.status,
        statusText: error?.response?.statusText,
        data: error?.response?.data,
        headers: error?.response?.headers
      });
      throw error;
    }
  }

  /**
   * 清空执行日志
   */
  async clearExecutionLogs(): Promise<APIResponse<null>> {
    try {
      console.log('开始清空执行日志');
      const response = await api.delete('/scheduler/execution-logs/clear');
      console.log('清空执行日志成功');
      return {
        success: true,
        message: response.message || '执行日志已清空'
      };
    } catch (error: any) {
      console.error('清空执行日志失败:', error);
      return {
        success: false,
        message: error?.response?.data?.detail || error?.message || '清空执行日志失败'
      };
    }
  }

  /**
   * 格式化任务状态
   */
  formatTaskStatus(task: ScheduledTask): {
    status: string;
    color: string;
    description: string;
  } {
    if (!task.enabled) {
      return {
        status: '已禁用',
        color: 'default',
        description: '任务已被禁用'
      };
    }

    if (task.status === 'running') {
      return {
        status: '运行中',
        color: 'processing',
        description: '任务正在执行中'
      };
    }

    if (task.next_run) {
      const nextRun = new Date(task.next_run);
      const now = new Date();
      if (nextRun > now) {
        return {
          status: '等待中',
          color: 'warning',
          description: `下次运行时间：${nextRun.toLocaleString()}`
        };
      }
    }

    if (task.last_run) {
      const lastRun = new Date(task.last_run);
      const successRate = task.success_count / (task.success_count + task.fail_count);
      
      if (successRate >= 0.9) {
        return {
          status: '正常',
          color: 'success',
          description: `上次运行：${lastRun.toLocaleString()}`
        };
      } else if (successRate >= 0.5) {
        return {
          status: '部分失败',
          color: 'warning',
          description: `成功率：${(successRate * 100).toFixed(1)}%`
        };
      } else {
        return {
          status: '异常',
          color: 'error',
          description: `成功率过低：${(successRate * 100).toFixed(1)}%`
        };
      }
    }

    return {
      status: '未知',
      color: 'default',
      description: '任务状态未知'
    };
  }
}

// 导出单例实例
export const schedulerAPI = new SchedulerAPI();
