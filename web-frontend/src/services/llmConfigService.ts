import apiClient from './api';

// LLM Provider interfaces
export interface LLMProvider {
  id: string;
  name: string;
  displayName: string;
  enabled: boolean;
  apiKey: string;
  baseUrl: string;
  models: string[];
  defaultModel: string;
  status: 'connected' | 'disconnected' | 'testing' | 'error';
  description: string;
  icon: string;
  maxTokens: number;
  temperature: number;
  topP: number;
  timeout: number;  // API request timeout in seconds
}

export interface LLMConfig {
  defaultProvider: string;
  fallbackEnabled: boolean;
  fallbackOrder: string[];
  globalSettings: {
    // Global Settings 只负责系统级策略，不包含参数默认值
    // timeout, retryCount 等参数由各个 Provider 配置提供
    enableLogging: boolean;
    enableMonitoring: boolean;
  };
  providers: LLMProvider[];
}

export interface LLMTestResponse {
  success: boolean;
  provider: string;
  model: string;
  responseTime: number;
  message?: string;
  error?: string;
}

export interface LLMUsageStats {
  provider: string;
  totalRequests: number;
  successRate: number;
  avgResponseTime: number;
  totalTokens: number;
  totalCost: number;
  currency: string;
}

// LLM Configuration Service
class LLMConfigService {
  // Get LLM configuration
  async getConfig(): Promise<LLMConfig> {
    try {
      const response = await apiClient.get('/llm/config');
      return response.data;
    } catch (error) {
      console.error('Failed to get LLM config:', error);
      throw error;
    }
  }

  // Save LLM configuration
  async saveConfig(config: LLMConfig): Promise<boolean> {
    try {
      const response = await apiClient.post('/llm/config', config);
      return response.data.success;
    } catch (error) {
      console.error('Failed to save LLM config:', error);
      throw error;
    }
  }

  // Test connection to a specific provider
  async testProvider(
    providerId: string, 
    apiKey: string, 
    baseUrl: string, 
    defaultModel: string, 
    maxTokens: number, 
    temperature: number, 
    topP: number
  ): Promise<LLMTestResponse> {
    try {
      const response = await apiClient.post('/llm/test', {
        providerId,
        apiKey,
        baseUrl,
        defaultModel,
        maxTokens,
        temperature,
        topP
      });
      return response.data;
    } catch (error) {
      console.error('Failed to test provider:', error);
      throw error;
    }
  }

  // Get usage statistics
  async getUsageStats(timeRange: '24h' | '7d' | '30d' = '30d'): Promise<LLMUsageStats[]> {
    try {
      const response = await apiClient.get(`/llm/stats?range=${timeRange}`);
      return response.data;
    } catch (error) {
      console.error('Failed to get usage stats:', error);
      throw error;
    }
  }

  // Get available models for a provider
  async getProviderModels(providerId: string, apiKey: string, baseUrl: string): Promise<string[]> {
    try {
      const response = await apiClient.post('/llm/models', {
        providerId,
        apiKey,
        baseUrl
      });
      return response.data.models;
    } catch (error) {
      console.error('Failed to get provider models:', error);
      throw error;
    }
  }

  // Get provider status
  async getProviderStatus(providerId: string): Promise<{ status: string; lastChecked: string }> {
    try {
      const response = await apiClient.get(`/llm/status/${providerId}`);
      return response.data;
    } catch (error) {
      console.error('Failed to get provider status:', error);
      throw error;
    }
  }

  // Reset provider configuration
  async resetProvider(providerId: string): Promise<boolean> {
    try {
      const response = await apiClient.post(`/llm/reset/${providerId}`);
      return response.data.success;
    } catch (error) {
      console.error('Failed to reset provider:', error);
      throw error;
    }
  }

  // Export configuration
  async exportConfig(): Promise<Blob> {
    try {
      const response = await apiClient.get('/llm/export', {
        responseType: 'blob'
      });
      return response.data;
    } catch (error) {
      console.error('Failed to export config:', error);
      throw error;
    }
  }

  // Import configuration
  async importConfig(file: File): Promise<boolean> {
    try {
      const formData = new FormData();
      formData.append('config', file);
      
      const response = await apiClient.post('/llm/import', formData, {
        headers: {
          'Content-Type': 'multipart/form-data'
        }
      });
      return response.data.success;
    } catch (error) {
      console.error('Failed to import config:', error);
      throw error;
    }
  }

  // ============ 增强的监控统计 API（基于 llm_prompt_usage_logs 表）============

  // 获取监控仪表板数据
  async getMonitoringDashboard(timeRange: string = '30d', providerId?: string, templateId?: number): Promise<any> {
    try {
      let url = `/llm/monitoring/dashboard?range=${timeRange}`;
      if (providerId) {
        url += `&provider_id=${providerId}`;
      }
      if (templateId) {
        url += `&template_id=${templateId}`;
      }
      const response = await apiClient.get(url);
      return response.data;
    } catch (error) {
      console.error('Failed to get monitoring dashboard:', error);
      throw error;
    }
  }

  // 获取时间序列数据
  async getTimeSeriesData(timeRange: string = '30d', providerId?: string, templateId?: number, interval?: string): Promise<any> {
    try {
      let url = `/llm/monitoring/time-series?range=${timeRange}`;
      if (providerId) {
        url += `&provider_id=${providerId}`;
      }
      if (templateId) {
        url += `&template_id=${templateId}`;
      }
      if (interval) {
        url += `&interval=${interval}`;
      }
      const response = await apiClient.get(url);
      return response.data;
    } catch (error) {
      console.error('Failed to get time series data:', error);
      throw error;
    }
  }

  // 获取按模型分组的统计
  async getModelStats(timeRange: string = '30d', providerId?: string, templateId?: number): Promise<any> {
    try {
      let url = `/llm/monitoring/model-stats?range=${timeRange}`;
      if (providerId) {
        url += `&provider_id=${providerId}`;
      }
      if (templateId) {
        url += `&template_id=${templateId}`;
      }
      const response = await apiClient.get(url);
      return response.data;
    } catch (error) {
      console.error('Failed to get model stats:', error);
      throw error;
    }
  }

  // 获取按模板分组的统计（新增）
  async getTemplateStats(timeRange: string = '30d', providerId?: string): Promise<any> {
    try {
      let url = `/llm/monitoring/template-stats?range=${timeRange}`;
      if (providerId) {
        url += `&provider_id=${providerId}`;
      }
      const response = await apiClient.get(url);
      return response.data;
    } catch (error) {
      console.error('Failed to get template stats:', error);
      throw error;
    }
  }

  // 获取错误分析
  async getErrorAnalysis(timeRange: string = '30d', providerId?: string, templateId?: number): Promise<any> {
    try {
      let url = `/llm/monitoring/error-analysis?range=${timeRange}`;
      if (providerId) {
        url += `&provider_id=${providerId}`;
      }
      if (templateId) {
        url += `&template_id=${templateId}`;
      }
      const response = await apiClient.get(url);
      return response.data;
    } catch (error) {
      console.error('Failed to get error analysis:', error);
      throw error;
    }
  }

  // 获取详细日志
  async getDetailedLogs(
    timeRange: string = '30d',
    providerId?: string,
    templateId?: number,
    successFilter?: boolean,
    limit: number = 100,
    offset: number = 0
  ): Promise<any> {
    try {
      let url = `/llm/monitoring/logs?range=${timeRange}&limit=${limit}&offset=${offset}`;
      if (providerId) {
        url += `&provider_id=${providerId}`;
      }
      if (templateId) {
        url += `&template_id=${templateId}`;
      }
      if (successFilter !== undefined) {
        url += `&success_filter=${successFilter}`;
      }
      const response = await apiClient.get(url);
      return response.data;
    } catch (error) {
      console.error('Failed to get detailed logs:', error);
      throw error;
    }
  }
}

export const llmConfigService = new LLMConfigService();
