/**
 * 用户新闻筛选预设 API 服务
 */

import { api } from './api';
import {
  NewsFilterPreset,
  CreatePresetRequest,
  UpdatePresetRequest,
  PresetListResponse,
  PresetOperationResponse,
  GetPresetsParams
} from '../types/newsPresets';

export const newsPresetsAPI = {
  /**
   * 获取用户预设列表
   */
  getPresets: async (params?: GetPresetsParams): Promise<PresetListResponse> => {
    const queryParams = new URLSearchParams();
    
    if (params?.scope_type) {
      queryParams.append('scope_type', params.scope_type);
    }
    if (params?.symbol_code) {
      queryParams.append('symbol_code', params.symbol_code);
    }
    if (params?.include_system !== undefined) {
      queryParams.append('include_system', params.include_system.toString());
    }
    
    const url = `/user/news-presets/${queryParams.toString() ? '?' + queryParams.toString() : ''}`;
    return api.get<PresetListResponse>(url);
  },

  /**
   * 获取系统预设列表
   */
  getSystemPresets: async (): Promise<PresetListResponse> => {
    return api.get<PresetListResponse>('/user/news-presets/system-presets');
  },

  /**
   * 创建新预设
   */
  createPreset: async (request: CreatePresetRequest): Promise<PresetOperationResponse> => {
    return api.post<PresetOperationResponse>('/user/news-presets/', request);
  },

  /**
   * 更新预设
   */
  updatePreset: async (presetId: number, request: UpdatePresetRequest): Promise<PresetOperationResponse> => {
    return api.put<PresetOperationResponse>(`/user/news-presets/${presetId}`, request);
  },

  /**
   * 删除预设
   */
  deletePreset: async (presetId: number): Promise<PresetOperationResponse> => {
    return api.delete<PresetOperationResponse>(`/user/news-presets/${presetId}`);
  },

  /**
   * 使用预设（更新使用统计）
   */
  usePreset: async (presetId: number): Promise<{ success: boolean; message: string }> => {
    return api.post<{ success: boolean; message: string }>(`/user/news-presets/${presetId}/use`, {});
  },

  /**
   * 复制预设
   */
  duplicatePreset: async (presetId: number, newName: string): Promise<PresetOperationResponse> => {
    const queryParams = new URLSearchParams({ new_name: newName });
    return api.post<PresetOperationResponse>(`/user/news-presets/${presetId}/duplicate?${queryParams.toString()}`, {});
  },

  /**
   * 设置为默认预设
   */
  setAsDefault: async (presetId: number): Promise<PresetOperationResponse> => {
    return newsPresetsAPI.updatePreset(presetId, { is_default: true });
  },

  /**
   * 切换收藏状态
   */
  toggleFavorite: async (preset: NewsFilterPreset): Promise<PresetOperationResponse> => {
    return newsPresetsAPI.updatePreset(preset.id, { is_favorite: !preset.is_favorite });
  }
};


