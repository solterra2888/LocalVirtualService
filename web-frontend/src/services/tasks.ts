import { api } from './api';

// 本地类型定义
export interface TaskType {
  id: string;
  name: string;
  token: string;
  creator: string;
  created_at: string;
  remarks?: string;
  input_fields: Array<{
    name: string;
    max_length: number;
  }>;
  is_default: boolean;
  agent_type: 'chatbot' | 'workflow';
}

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

export interface TaskCreateRequest {
  name: string;
  token: string;
  remarks?: string;
  input_fields?: Array<{
    name: string;
    max_length: number;
  }>;
  agent_type?: 'chatbot' | 'workflow';
}

export interface TaskUpdateRequest {
  name?: string;
  token?: string;
  remarks?: string;
  input_fields?: Array<{
    name: string;
    max_length: number;
  }>;
  agent_type?: 'chatbot' | 'workflow';
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
}

export const tasksAPI = {
  // 获取RC Agent列表
  getTaskTypes: async (): Promise<TaskType[]> => {
    try {
      console.log('正在获取RC Agent列表...');
      const result = await api.get<TaskType[]>('/tasks/types');
      console.log('RC Agent获取成功:', result);
      return result;
    } catch (error) {
      console.error('获取RC Agent失败:', error);
      throw error;
    }
  },

  // 创建RC Agent
  createTaskType: async (request: TaskCreateRequest): Promise<TaskType> => {
    try {
      console.log('正在创建RC Agent:', request);
      const result = await api.post<TaskType>('/tasks/types', request);
      console.log('RC Agent创建成功:', result);
      return result;
    } catch (error) {
      console.error('创建RC Agent失败:', error);
      throw error;
    }
  },

  // 更新RC Agent
  updateTaskType: async (taskId: string, request: TaskUpdateRequest): Promise<TaskType> => {
    try {
      console.log('正在更新RC Agent:', taskId, request);
      const result = await api.put<TaskType>(`/tasks/types/${taskId}`, request);
      console.log('RC Agent更新成功:', result);
      return result;
    } catch (error) {
      console.error('更新RC Agent失败:', error);
      throw error;
    }
  },

  // 删除RC Agent
  deleteTaskType: async (taskId: string): Promise<{ message: string }> => {
    try {
      console.log('正在删除RC Agent:', taskId);
      const result = await api.delete<{ message: string }>(`/tasks/types/${taskId}`);
      console.log('RC Agent删除成功:', result);
      return result;
    } catch (error) {
      console.error('删除RC Agent失败:', error);
      throw error;
    }
  },

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

  // 获取默认RC Agent
  getDefaultTaskType: async (): Promise<TaskType | null> => {
    const taskTypes = await tasksAPI.getTaskTypes();
    return taskTypes.find(task => task.is_default) || null;
  },

  // 验证RC Agent名称是否唯一
  validateTaskTypeName: async (name: string, excludeId?: string): Promise<boolean> => {
    const taskTypes = await tasksAPI.getTaskTypes();
    return !taskTypes.some(task => task.name === name && task.id !== excludeId);
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