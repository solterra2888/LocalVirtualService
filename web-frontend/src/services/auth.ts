import { api } from './api';
import { LoginRequest, LoginResponse, User } from '../types';

export const authAPI = {
  // 用户登录
  login: async (credentials: LoginRequest): Promise<LoginResponse> => {
    const response = await api.post<LoginResponse>('/auth/login', credentials);
    
    // 保存token和用户信息到localStorage
    localStorage.setItem('access_token', response.access_token);
    localStorage.setItem('user', JSON.stringify(response.user));
    
    return response;
  },

  // 用户登出
  logout: async (): Promise<void> => {
    try {
      await api.post('/auth/logout');
    } finally {
      // 无论API调用成功与否，都清除本地存储
      localStorage.removeItem('access_token');
      localStorage.removeItem('user');
    }
  },

  // 获取当前用户信息
  getCurrentUser: async (): Promise<User> => {
    return api.get<User>('/auth/me');
  },

  // 验证token有效性
  verifyToken: async (): Promise<{ valid: boolean; username: string; is_admin: boolean }> => {
    return api.get('/auth/verify');
  },

  // 从localStorage获取保存的用户信息
  getSavedUser: (): User | null => {
    const userStr = localStorage.getItem('user');
    if (userStr) {
      try {
        return JSON.parse(userStr);
      } catch {
        localStorage.removeItem('user');
      }
    }
    return null;
  },

  // 从localStorage获取保存的token
  getSavedToken: (): string | null => {
    return localStorage.getItem('access_token');
  },

  // 检查是否已登录
  isAuthenticated: (): boolean => {
    const token = localStorage.getItem('access_token');
    const user = localStorage.getItem('user');
    return !!(token && user);
  }
}; 