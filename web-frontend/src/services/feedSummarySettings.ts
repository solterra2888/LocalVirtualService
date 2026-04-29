/**
 * Feed Summary Settings API 服务 (PR-N1/N3)
 *
 * 用户对 Feeds 自动个性化摘要的开关 / 模板偏好.
 * 后端: backend/api/feed_summary_settings.py
 */

import { api } from './api';

export type FeedSummaryChannel = 'twitter' | 'youtube';

export interface FeedSummarySettingItem {
  id: number | null;
  channel_type: FeedSummaryChannel;
  enabled: boolean;
  template_id: number | null;
  min_length: number | null;
  created_at: string | null;
  updated_at: string | null;
}

export interface FeedSummarySettingUpdate {
  enabled: boolean;
  template_id?: number | null;
  min_length?: number | null;
}

export interface FeedSummarySettingsListResponse {
  items: FeedSummarySettingItem[];
}

export const feedSummarySettingsAPI = {
  /**
   * 获取当前用户全部渠道 (twitter / youtube) 设置.
   * 未配置过的渠道返回默认 disabled 行.
   */
  list: async (): Promise<FeedSummarySettingsListResponse> => {
    return api.get<FeedSummarySettingsListResponse>('/feeds/summary-settings');
  },

  /** 获取单渠道设置 */
  get: async (channel: FeedSummaryChannel): Promise<FeedSummarySettingItem> => {
    return api.get<FeedSummarySettingItem>(
      `/feeds/summary-settings/${channel}`,
    );
  },

  /** 创建或更新单渠道设置 (UPSERT) */
  update: async (
    channel: FeedSummaryChannel,
    payload: FeedSummarySettingUpdate,
  ): Promise<FeedSummarySettingItem> => {
    return api.put<FeedSummarySettingItem>(
      `/feeds/summary-settings/${channel}`,
      payload,
    );
  },
};
