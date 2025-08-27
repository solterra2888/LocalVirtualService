// 认证状态管理
import { create } from 'zustand';

interface User {
  id: number;
  username: string;
  is_admin: boolean;
  created_at: string;
  last_login?: string;
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
      const response = await fetch('/api/auth/login', {
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
        const verifyResponse = await fetch('/api/auth/verify', {
          method: 'GET',
          headers: {
            'Authorization': `Bearer ${token}`,
            'Content-Type': 'application/json',
          },
        });
        
        console.log('验证响应状态:', verifyResponse.status);
        
        if (verifyResponse.ok) {
          const verifyData = await verifyResponse.json();
          console.log('Token验证成功，恢复用户状态:', user, '验证数据:', verifyData);
          set({
            user,
            token,
            isAuthenticated: true,
            isInitialized: true
          });
        } else {
          const errorData = await verifyResponse.text();
          console.log('Token验证失败，清除认证信息. 状态:', verifyResponse.status, '错误:', errorData);
          localStorage.removeItem('access_token');
          localStorage.removeItem('user');
          set({
            user: null,
            token: null,
            isAuthenticated: false,
            isInitialized: true
          });
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
  }
})); 