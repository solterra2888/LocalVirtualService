// 认证状态管理
import { create } from 'zustand';
import {
  get401UserMessage,
  isSessionReplaced401Detail,
  showSessionReplacedModal,
} from '../utils/authSession';

interface User {
  id: number;
  username: string;
  is_admin: boolean;
  created_at: string;
  last_login?: string;
  avatar_url?: string;
}

interface LoginRequest {
  username: string;
  password: string;
}

interface AuthState {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  loading: boolean;
  isInitialized: boolean;
  
  login: (credentials: LoginRequest) => Promise<void>;
  logout: () => Promise<void>;
  initializeAuth: () => Promise<void>;
  setLoading: (loading: boolean) => void;
  updateUser: (updates: Partial<User>) => void;
}

export const useAuthStore = create<AuthState>((set, get) => ({
  user: null,
  token: null,
  isAuthenticated: false,
  loading: false,
  isInitialized: false,

  login: async (credentials: LoginRequest) => {
    set({ loading: true });
    try {
      const apiBaseURL = process.env.REACT_APP_API_BASE_URL || '/api';
      const response = await fetch(`${apiBaseURL}/auth/login`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(credentials),
      });
      
      if (!response.ok) {
        throw new Error('登录失败');
      }
      
      const data = await response.json();
      
      // 保存token和用户信息
      localStorage.setItem('access_token', data.access_token);
      localStorage.setItem('user', JSON.stringify(data.user));
      
      console.log('登录成功，设置认证状态:', { user: data.user, token: data.access_token ? 'exists' : 'none' });
      
      set({
        user: data.user,
        token: data.access_token,
        isAuthenticated: true,
        loading: false,
        isInitialized: true
      });
    } catch (error) {
      set({ loading: false });
      throw error;
    }
  },

  logout: async () => {
    localStorage.removeItem('access_token');
    localStorage.removeItem('user');
    set({
      user: null,
      token: null,
      isAuthenticated: false,
      isInitialized: true
    });
  },

  initializeAuth: async () => {
    set({ isInitialized: false });
    
    const token = localStorage.getItem('access_token');
    const userStr = localStorage.getItem('user');
    
    console.log('初始化认证状态:', { token: token ? 'exists' : 'none', userStr: userStr ? 'exists' : 'none' });
    
    if (token && userStr) {
      try {
        const user = JSON.parse(userStr);
        
        // 验证token是否仍然有效
        console.log('开始验证token...');
        const apiBaseURL = process.env.REACT_APP_API_BASE_URL || '/api';
        const verifyResponse = await fetch(`${apiBaseURL}/auth/verify`, {
          method: 'GET',
          headers: {
            'Authorization': `Bearer ${token}`,
            'Content-Type': 'application/json',
          },
        });
        
        console.log('验证响应状态:', verifyResponse.status);
        
        if (verifyResponse.ok) {
          const verifyData = await verifyResponse.json();
          console.log('Token验证成功，验证数据:', verifyData);

          let freshUser = user;
          try {
            const meResponse = await fetch(`${apiBaseURL}/auth/me`, {
              method: 'GET',
              headers: {
                'Authorization': `Bearer ${token}`,
                'Content-Type': 'application/json',
              },
            });
            if (meResponse.ok) {
              freshUser = await meResponse.json();
              localStorage.setItem('user', JSON.stringify(freshUser));
            }
          } catch (e) {
            console.warn('获取最新用户信息失败，使用缓存:', e);
          }

          set({
            user: freshUser,
            token,
            isAuthenticated: true,
            isInitialized: true
          });
        } else {
          const text = await verifyResponse.text();
          let detail: unknown;
          try {
            detail = JSON.parse(text).detail;
          } catch {
            console.log('Token验证失败，清除认证信息. 状态:', verifyResponse.status, 'body:', text);
          }
          console.log('Token验证失败，清除认证信息. 状态:', verifyResponse.status, 'detail:', detail);
          localStorage.removeItem('access_token');
          localStorage.removeItem('user');
          set({
            user: null,
            token: null,
            isAuthenticated: false,
            isInitialized: true
          });
          if (isSessionReplaced401Detail(detail)) {
            showSessionReplacedModal(get401UserMessage(detail), () => {
              window.location.href = '/login';
            });
          }
        }
      } catch (error) {
        console.error('Token验证或解析用户信息失败:', error);
        localStorage.removeItem('access_token');
        localStorage.removeItem('user');
        set({
          user: null,
          token: null,
          isAuthenticated: false,
          isInitialized: true
        });
      }
    } else {
      console.log('没有找到有效的认证信息');
      set({
        user: null,
        token: null,
        isAuthenticated: false,
        isInitialized: true
      });
    }
  },

  setLoading: (loading: boolean) => {
    set({ loading });
  },

  updateUser: (updates: Partial<User>) => {
    const current = get().user;
    if (current) {
      const updated = { ...current, ...updates };
      localStorage.setItem('user', JSON.stringify(updated));
      set({ user: updated });
    }
  }
})); 