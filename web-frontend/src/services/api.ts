import axios, { AxiosInstance, AxiosResponse, AxiosError } from 'axios';
import { message } from 'antd';
import {
  get401UserMessage,
  isSessionReplaced401Detail,
  showSessionReplacedModal,
} from '../utils/authSession';
import { formatFastAPIDetail } from '../utils/errorHandler';

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
  (error: any) => {
    // 🔥 检测是否为请求取消错误（用户主动取消或页面卸载）
    if (axios.isCancel(error) || error?.code === 'ERR_CANCELED' || error?.message === 'canceled') {
      console.log('Request cancelled:', error?.config?.url);
      return Promise.reject(error); // 不显示错误提示，直接拒绝
    }

    console.error('API Error:', {
      url: error.config?.url,
      status: error.response?.status,
      statusText: error.response?.statusText,
      data: error.response?.data,
      message: error.message
    });

    if (error.response?.status === 401) {
      // Token 过期、无效，或账号已在其它设备登录（后者用英文 Modal 提示）
      localStorage.removeItem('access_token');
      localStorage.removeItem('user');
      const detail = (error.response?.data as any)?.detail;
      const msg = get401UserMessage(detail);
      const goLogin = () => {
        window.location.href = '/login';
      };
      if (isSessionReplaced401Detail(detail)) {
        showSessionReplacedModal(msg, goLogin);
      } else {
        message.error(msg);
        goLogin();
      }
    } else if (error.response?.status === 403) {
      message.error('permission denied');
    } else if (error.response?.status === 404) {
      // 不在这里显示404错误，让组件自己处理
      console.warn('404 Error:', error.response?.data);
    } else if (error.response?.status === 409) {
      // 不在这里显示409错误，让组件自己处理（如watchlist重复添加）
      console.warn('409 Conflict:', error.response?.data);
    } else if (error.response?.status === 400) {
      // 不在这里显示400错误，让调用方根据 detail 自行展示（如创建订阅时 Twitter 用户不存在）
      console.warn('400 Bad Request:', error.response?.data);
    } else if (error.response?.status && error.response.status >= 500) {
      const raw = (error.response?.data as any)?.detail;
      const errorMessage = raw != null ? formatFastAPIDetail(raw) : 'server internal error';
      message.error(`server error: ${errorMessage}`);
      console.error('Server Error Details:', error.response?.data);
    } else if (error.message === 'Network Error') {
      // 网络连接失败提示已移除
      console.warn('Network Error:', error);
    } else if (error.code === 'ECONNABORTED') {
      message.error('request timeout, please try again later');
    } else {
      // 显示具体的错误信息；若后端已返回 detail，只展示该文案，避免与业务层重复并保持一条提示
      const detail = (error.response?.data as any)?.detail;
      const errorMessage =
        detail != null
          ? formatFastAPIDetail(detail)
          : error.message || 'unknown error';
      message.error(error.response?.data != null ? errorMessage : `request failed: ${errorMessage}`);
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
  
  patch: <T = any>(url: string, data?: any): Promise<T> => 
    apiClient.patch(url, data).then(extractData),
  
  delete: <T = any>(url: string, config?: any): Promise<T> => 
    apiClient.delete(url, config).then(extractData),
  
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


export default apiClient; 