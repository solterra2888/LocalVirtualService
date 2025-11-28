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
}

export const llmConfigService = new LLMConfigService();
