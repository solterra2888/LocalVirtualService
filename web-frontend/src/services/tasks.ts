import apiClient from './api';
import { api } from './api';

export interface TaskExecution {
  id: string;
  username: string;
  task_name: string;
  created_at: string;
  files_processed: number;
  files_failed: number;
  total_time: number;
  file_processing_time: number;
  ai_processing_time: number;
  total_tokens?: number;
  total_cost?: number;
  first_filename?: string;
  first_file_type?: string;
  rc_agent_status?: string;
  llm_model?: string;
  llm_provider?: string;
  first_task_id?: string;
  upload_params?: any;
  access_type?: string;
  shared_by?: string;
  share_info?: {
    shared_by: { user_id: number; username: string; avatar_url?: string | null };
    shared_to: Array<{ user_id: number; username: string; avatar_url?: string | null }>;
  } | null;
  /** Aggregated from file_tags.ticker for all files in this execution (list + detail). */
  ticker_tags?: string[] | null;
  folder_id?: number | null;
  folder_name?: string | null;
  first_display_name?: string | null;
}

export interface TaskExecutionDetail {
  execution: {
    id: string;
    username: string;
    task_name: string;
    created_at: string;
    files_processed: number;
      files_failed: number;
  total_time: number;
  file_processing_time: number;
  ai_processing_time: number;
  total_tokens?: number;
  total_cost?: number;
  upload_params?: any;
    final_report?: string;
  };
  files: Array<{
    id: number;
    filename: string;
    file_type: string;
    file_size_mb: number;
      processing_time?: number;
  ai_time?: number;
  content_length?: number;
    status: string;
    total_tokens?: number;
    cost?: number;
    error_message?: string;
    content?: string;
    created_at: string;
    original_file_path?: string;
  }>;
}

export interface PaginationParams {
  page?: number;
  page_size?: number;
  search_type?: 'file_name' | 'content';
  q?: string;
  ticker?: string;
  folder_id?: string;
  /** 按文件类型筛选；后端对 document 会展开为 pdf/word/excel/csv/image 等文档大类 */
  file_type?: string;
}

export const tasksAPI = {
  // 获取任务执行历史（支持分页：page, page_size）
  getTaskExecutions: async (params?: PaginationParams): Promise<TaskExecution[]> => {
    try {
      const result = await api.get<TaskExecution[]>('/tasks/executions', params);
      console.log('任务执行历史获取成功:', result);
      return result;
    } catch (error) {
      console.error('获取任务执行历史失败:', error);
      throw error;
    }
  },

  // 获取任务执行详情
  getTaskExecution: async (executionId: string): Promise<TaskExecution> => {
    return api.get<TaskExecution>(`/tasks/executions/${executionId}`);
  },

  // 获取任务执行的详细信息
  getTaskExecutionDetails: async (executionId: string): Promise<TaskExecutionDetail> => {
    return api.get<TaskExecutionDetail>(`/tasks/executions/${executionId}/details`);
  },

  // 删除任务执行记录
  deleteTaskExecution: async (executionId: string): Promise<{ message: string }> => {
    return api.delete<{ message: string }>(`/tasks/executions/${executionId}`);
  },

  // 重命名执行记录的首个文件
  renameExecutionFile: async (
    executionId: string,
    filename: string
  ): Promise<{ message: string; filename: string }> => {
    return api.patch<{ message: string; filename: string }>(
      `/tasks/executions/${executionId}/rename`,
      { filename }
    );
  },

  // 更新任务执行的 AI 分析报告内容
  updateExecutionFinalReport: async (
    executionId: string,
    finalReport: string
  ): Promise<{ success: boolean; message: string }> => {
    return api.patch<{ success: boolean; message: string }>(
      `/tasks/executions/${executionId}/final-report`,
      { final_report: finalReport }
    );
  },

  /**
   * 下载 AI 分析报告的**文字版** PDF（服务端渲染：文字可选中/可搜索，支持中文）。
   * 与前端截图的图片版 Export PDF 互补。
   */
  downloadExecutionReportPdf: async (executionId: string): Promise<string> => {
    const response = await apiClient.get(
      `/tasks/executions/${executionId}/report-pdf`,
      { responseType: 'blob' }
    );
    const disposition: string = response.headers['content-disposition'] || '';
    // 优先 RFC 5987（filename*=UTF-8''...），回退 filename="..."
    const filename =
      decodeURIComponent(disposition.match(/filename\*=UTF-8''([^;]+)/)?.[1] ?? '') ||
      disposition.match(/filename="?([^";]+)"?/)?.[1] ||
      'ai-analysis-report.pdf';
    const blob: Blob = response.data;
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    return filename;
  },

  // 健康检查
  healthCheck: async (): Promise<{ status: string; message: string }> => {
    try {
      console.log('正在进行后端健康检查...');
      const result = await api.get<{ status: string; message: string }>('/health');
      console.log('后端健康检查成功:', result);
      return result;
    } catch (error) {
      console.error('后端健康检查失败:', error);
      throw error;
    }
  },

  // 测试认证状态
  testAuth: async (): Promise<any> => {
    try {
      console.log('正在测试认证状态...');
      const result = await api.get('/auth/verify');
      console.log('认证状态测试成功:', result);
      return result;
    } catch (error) {
      console.error('认证状态测试失败:', error);
      throw error;
    }
  },

  // 格式化任务执行时间
  formatExecutionTime: (seconds: number): string => {
    if (seconds < 60) {
      return `${seconds.toFixed(1)}秒`;
    } else if (seconds < 3600) {
      const minutes = Math.floor(seconds / 60);
      const remainingSeconds = (seconds % 60).toFixed(1);
      return `${minutes}分${remainingSeconds}秒`;
    } else {
      const hours = Math.floor(seconds / 3600);
      const minutes = Math.floor((seconds % 3600) / 60);
      return `${hours}小时${minutes}分钟`;
    }
  },

  // 计算任务成功率
  calculateSuccessRate: (execution: TaskExecution): number => {
    if (execution.files_processed === 0) return 0;
    const totalFiles = execution.files_processed + execution.files_failed;
    return (execution.files_processed / totalFiles) * 100;
  },

  // 格式化费用
  formatCost: (cost?: number): string => {
    if (!cost) return '¥0.0000';
    return `¥${cost.toFixed(4)}`;
  },

  // 格式化Token数量
  formatTokens: (tokens?: number): string => {
    if (!tokens) return '0';
    if (tokens >= 1000000) {
      return `${(tokens / 1000000).toFixed(1)}M`;
    } else if (tokens >= 1000) {
      return `${(tokens / 1000).toFixed(1)}K`;
    }
    return tokens.toLocaleString();
  },

  // 获取任务状态颜色
  getTaskStatusColor: (status: string): string => {
    switch (status.toLowerCase()) {
      case 'completed':
        return 'green';
      case 'failed':
        return 'red';
      case 'processing':
        return 'blue';
      case 'stopped':
        return 'orange';
      default:
        return 'default';
    }
  },

  // 获取任务状态文本
  getTaskStatusText: (status: string): string => {
    switch (status.toLowerCase()) {
      case 'completed':
        return '已完成';
      case 'failed':
        return '失败';
      case 'processing':
        return '处理中';
      case 'stopped':
        return '已停止';
      case 'pending':
        return '等待中';
      default:
        return '未知';
    }
  },

  // 计算处理效率
  calculateProcessingEfficiency: (execution: TaskExecution): {
    avgFileTime: number;
    avgAiTime: number;
    efficiency: string;
  } => {
    const totalFiles = execution.files_processed + execution.files_failed;
    const avgFileTime = totalFiles > 0 ? execution.file_processing_time / totalFiles : 0;
    const avgAiTime = totalFiles > 0 ? execution.ai_processing_time / totalFiles : 0;
    
    let efficiency = '一般';
    if (avgFileTime < 10) {
      efficiency = '高效';
    } else if (avgFileTime < 30) {
      efficiency = '良好';
    } else if (avgFileTime < 60) {
      efficiency = '一般';
    } else {
      efficiency = '较慢';
    }
    
    return {
      avgFileTime,
      avgAiTime,
      efficiency
    };
  },

  // 导出任务数据
  exportTaskData: async (taskIds: string[]): Promise<Blob> => {
    const response = await fetch('/api/tasks/export', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${localStorage.getItem('access_token')}`
      },
      body: JSON.stringify({ task_ids: taskIds })
    });
    
    if (!response.ok) {
      throw new Error('导出任务数据失败');
    }
    
    return response.blob();
  }
}; 