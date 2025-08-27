import axios, { AxiosInstance, AxiosResponse, AxiosError } from 'axios';
import { message } from 'antd';

// API基础配置
const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || '/api';

// 创建axios实例
const apiClient: AxiosInstance = axios.create({
  baseURL: API_BASE_URL,
  timeout: 120000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// 请求拦截器 - 添加认证token
apiClient.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('access_token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    console.log('API Request:', {
      url: config.url,
      method: config.method,
      baseURL: config.baseURL,
      hasToken: !!token
    });
    return config;
  },
  (error) => {
    console.error('Request interceptor error:', error);
    return Promise.reject(error);
  }
);

// 响应拦截器 - 处理错误和token过期
apiClient.interceptors.response.use(
  (response: AxiosResponse) => {
    console.log('API Response:', {
      url: response.config.url,
      status: response.status,
      data: response.data
    });
    return response;
  },
  (error: AxiosError) => {
    console.error('API Error:', {
      url: error.config?.url,
      status: error.response?.status,
      statusText: error.response?.statusText,
      data: error.response?.data,
      message: error.message
    });

    if (error.response?.status === 401) {
      // Token过期或无效，跳转到登录页
      localStorage.removeItem('access_token');
      localStorage.removeItem('user');
      window.location.href = '/login';
      message.error('登录已过期，请重新登录');
    } else if (error.response?.status === 403) {
      message.error('权限不足');
    } else if (error.response?.status === 404) {
      const errorMessage = (error.response?.data as any)?.detail || '请求的资源不存在';
      message.error(errorMessage);
    } else if (error.response?.status && error.response.status >= 500) {
      const errorMessage = (error.response?.data as any)?.detail || '服务器内部错误';
      message.error(`服务器错误: ${errorMessage}`);
      console.error('Server Error Details:', error.response?.data);
    } else if (error.message === 'Network Error') {
      message.error('网络连接失败，请检查网络连接和服务器状态');
    } else if (error.code === 'ECONNABORTED') {
      message.error('请求超时，请稍后重试');
    } else {
      // 显示具体的错误信息
      const errorMessage = (error.response?.data as any)?.detail || error.message || '未知错误';
      message.error(`请求失败: ${errorMessage}`);
    }
    return Promise.reject(error);
  }
);

// API响应数据提取器
export const extractData = <T>(response: AxiosResponse<T>): T => {
  return response.data;
};

// 通用API方法
export const api = {
  get: <T = any>(url: string, params?: any): Promise<T> => 
    apiClient.get(url, { params }).then(extractData),
  
  post: <T = any>(url: string, data?: any): Promise<T> => 
    apiClient.post(url, data).then(extractData),
  
  put: <T = any>(url: string, data?: any): Promise<T> => 
    apiClient.put(url, data).then(extractData),
  
  delete: <T = any>(url: string): Promise<T> => 
    apiClient.delete(url).then(extractData),
  
  // 文件上传专用方法
  upload: <T = any>(url: string, formData: FormData): Promise<T> => 
    apiClient.post(url, formData, {
      headers: {
        'Content-Type': 'multipart/form-data',
      },
    }).then(extractData),
  
  // 文件下载专用方法
  download: async (url: string): Promise<Blob> => {
    const response = await apiClient.get(url, {
      responseType: 'blob',
    });
    return response.data;
  },
};

// Exchanges API
export const exchangesApi = {
  // 获取交易所列表
  getExchanges: () => api.get('/exchanges/'),
  
  // 获取ticker列表
  getTickers: (params?: {
    exchange?: string;
    ticker_type?: string;
    search?: string;
    limit?: number;
  }) => api.get('/exchanges/tickers', params),
  
  // 获取统计信息
  getStatistics: (params?: { exchange?: string }) => 
    api.get('/exchanges/statistics', params),
  
  // 获取可用类型
  getAvailableTypes: (params?: { exchange?: string }) => 
    api.get('/exchanges/types', params),
  
  // 获取可用交易所
  getAvailableExchanges: () => api.get('/exchanges/available'),
  
  // 获取交易所数据
  fetchExchangeData: (exchangeCodes: string[]) => 
    api.post('/exchanges/fetch', { exchange_codes: exchangeCodes }),
  
  // 清除交易所数据
  clearExchangeData: (exchangeCode: string) => 
    api.delete(`/exchanges/${exchangeCode}/clear`),
  
  // 调试API
  getExchangeConfigs: () => api.get('/exchanges/debug/config'),
};

export default apiClient; 