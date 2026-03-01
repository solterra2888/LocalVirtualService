import { api } from './api';
import { User, CreateUserRequest, UpdateUserRequest } from '../types';

// 用户API服务
export const usersApi = {
  // 获取用户列表
  async getUsers(page: number = 1, pageSize: number = 20): Promise<User[]> {
    try {
      return await api.get<User[]>('/users/', { page, page_size: pageSize });
    } catch (error) {
      console.error('获取用户列表失败:', error);
      throw error;
    }
  },

  // 创建用户
  async createUser(userData: CreateUserRequest): Promise<User> {
    try {
      return await api.post<User>('/users/', userData);
    } catch (error) {
      console.error('创建用户失败:', error);
      throw error;
    }
  },

  // 获取用户详情
  async getUser(userId: number): Promise<User> {
    try {
      return await api.get<User>(`/users/${userId}`);
    } catch (error) {
      console.error('获取用户详情失败:', error);
      throw error;
    }
  },

  // 更新用户
  async updateUser(userId: number, userData: UpdateUserRequest): Promise<User> {
    try {
      return await api.put<User>(`/users/${userId}`, userData);
    } catch (error) {
      console.error('更新用户失败:', error);
      throw error;
    }
  },

  // 删除用户
  async deleteUser(userId: number): Promise<void> {
    try {
      return await api.delete<void>(`/users/${userId}`);
    } catch (error) {
      console.error('删除用户失败:', error);
      throw error;
    }
  },

  // 修改用户密码
  async changePassword(userId: number, newPassword: string): Promise<void> {
    try {
      return await api.put<void>(`/users/${userId}/password`, { new_password: newPassword });
    } catch (error) {
      console.error('修改密码失败:', error);
      throw error;
    }
  },

  async getMyProfile(): Promise<User> {
    return api.get<User>('/users/me/profile');
  },

  async updateMyProfile(data: { username?: string; avatar_url?: string }): Promise<User> {
    return api.put<User>('/users/me/profile', data);
  },

  async uploadAvatar(file: File): Promise<{ avatar_url: string }> {
    const formData = new FormData();
    formData.append('file', file);
    return api.upload<{ avatar_url: string }>('/users/me/avatar', formData);
  }
}; 