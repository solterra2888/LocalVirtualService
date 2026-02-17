/**
 * 订阅管理服务 - 管理Twitter、YouTube、知识星球等多渠道订阅
 */

import { api } from './api';

// 类型定义
export interface Subscription {
  id: number;
  user_id: number;
  channel_type: string;
  channel_identifier: string;
  channel_name?: string;
  channel_description?: string;
  enabled: boolean;
  priority: number;
  fetch_interval_minutes: number;
  max_items_per_fetch: number;
  filter_config: Record<string, any>;
  metadata: Record<string, any>;
  created_at: string;
  updated_at: string;
  last_fetched_at?: string;
  next_fetch_at?: string;
  total_items_fetched: number;
  last_fetch_items_count: number;
  consecutive_failures: number;
  unread_count?: number; // 🔥 新增：未读内容数量
}

export interface CreateSubscriptionRequest {
  channel_type: string;
  channel_identifier: string;
  channel_name?: string;
  channel_description?: string;
  enabled?: boolean;
  filter_config?: Record<string, any>;
  metadata?: Record<string, any>;
}

export interface UpdateSubscriptionRequest {
  channel_name?: string;
  channel_description?: string;
  enabled?: boolean;
  filter_config?: Record<string, any>;
  metadata?: Record<string, any>;
}

export interface SubscriptionStats {
  total_subscriptions: number;
  enabled_subscriptions: number;
  disabled_subscriptions: number;
  by_channel_type: Record<string, number>;
  total_items_fetched: number;
  recent_failures: number;
}

export interface FetchHistory {
  id: number;
  subscription_id: number;
  fetch_time: string;
  execution_duration_ms?: number;
  items_fetched: number;
  items_new: number;
  items_duplicated: number;
  items_filtered: number;
  status: string;
  error_message?: string;
  error_code?: string;
}

export interface ApiResponse<T = any> {
  success: boolean;
  data?: T;
  message?: string;
  error?: string;
}

export interface SubscriptionContent {
  id: number;
  channel_type: string;
  content_id: string;
  title?: string;
  content: string;
  author_name?: string;
  author_identifier: string;
  url?: string;
  media_urls?: string[];
  published_at?: string;
  fetched_at?: string;
  engagement_count: number;
  view_count: number;
  metadata: Record<string, any>;
  is_read: boolean;
  read_at?: string;
  heat_level?: number; // 热度等级：1-灰色(低), 2-蓝色(中), 3-橙色(高), 4-红色(超高)
  // Twitter 特定字段
  is_retweet?: boolean;
  retweeted_author?: string;
  retweeted_content?: string;
  retweeted_url?: string;
  is_quote?: boolean;
  quoted_author?: string;
  quoted_content?: string;
  quoted_url?: string;
  // Reply 特定字段
  is_reply?: boolean;
  in_reply_to_id?: string;
  in_reply_to_username?: string;
  in_reply_to_user_id?: string;
}

export interface SubscriptionContentsResponse {
  contents: SubscriptionContent[];
  total: number;
  limit: number;
  offset: number;
}

// 订阅管理 API
export const subscriptionsAPI = {
  /**
   * 获取当前用户的所有订阅
   */
  async getUserSubscriptions(
    channelType?: string,
    enabled?: boolean
  ): Promise<ApiResponse<Subscription[]>> {
    try {
      const params: any = {};
      if (channelType) params.channel_type = channelType;
      if (enabled !== undefined) params.enabled = enabled;

      const data = await api.get<Subscription[]>('/subscriptions', params);

      return {
        success: true,
        data,
      };
    } catch (error: any) {
      console.error('Failed to get subscriptions:', error);
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to retrieve subscriptions',
      };
    }
  },

  /**
   * 获取单个订阅详情
   */
  async getSubscription(subscriptionId: number): Promise<ApiResponse<Subscription>> {
    try {
      const data = await api.get<Subscription>(`/subscriptions/${subscriptionId}`);

      return {
        success: true,
        data,
      };
    } catch (error: any) {
      console.error('Failed to get subscription:', error);
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to retrieve subscription',
      };
    }
  },

  /**
   * 创建新订阅
   */
  async createSubscription(
    request: CreateSubscriptionRequest
  ): Promise<ApiResponse<Subscription>> {
    try {
      const data = await api.post<Subscription>('/subscriptions/', request);

      return {
        success: true,
        data,
      };
    } catch (error: any) {
      console.error('Failed to create subscription:', error);
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to create subscription',
      };
    }
  },

  /**
   * 更新订阅
   */
  async updateSubscription(
    subscriptionId: number,
    request: UpdateSubscriptionRequest
  ): Promise<ApiResponse<Subscription>> {
    try {
      const data = await api.put<Subscription>(`/subscriptions/${subscriptionId}`, request);

      return {
        success: true,
        data,
      };
    } catch (error: any) {
      console.error('Failed to update subscription:', error);
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to update subscription',
      };
    }
  },

  /**
   * 删除订阅
   */
  async deleteSubscription(subscriptionId: number): Promise<ApiResponse> {
    try {
      const data = await api.delete(`/subscriptions/${subscriptionId}`);

      return {
        success: true,
        data,
      };
    } catch (error: any) {
      console.error('Failed to delete subscription:', error);
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to delete subscription',
      };
    }
  },

  /**
   * 获取订阅统计数据
   */
  async getSubscriptionStats(): Promise<ApiResponse<SubscriptionStats>> {
    try {
      const data = await api.get<SubscriptionStats>('/subscriptions/stats/overview');

      return {
        success: true,
        data,
      };
    } catch (error: any) {
      console.error('Failed to get subscription stats:', error);
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to retrieve statistics',
      };
    }
  },

  /**
   * 获取订阅的获取历史记录
   */
  async getSubscriptionHistory(
    subscriptionId: number,
    limit: number = 50
  ): Promise<ApiResponse<FetchHistory[]>> {
    try {
      const data = await api.get<FetchHistory[]>(
        `/subscriptions/${subscriptionId}/history`,
        { limit }
      );

      return {
        success: true,
        data,
      };
    } catch (error: any) {
      console.error('Failed to get subscription history:', error);
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to retrieve history',
      };
    }
  },

  /**
   * @deprecated Test Fetch 功能已弃用
   * 
   * 原因：新架构在创建订阅时自动智能处理：
   * - 如果有内容：复用现有数据（< 1秒）
   * - 如果无内容：自动调用API获取
   * - Scheduler 定期自动更新内容
   * 
   * 如需强制刷新，请使用 Scheduler 的立即执行功能
   */
  async testFetchSubscription(subscriptionId: number): Promise<ApiResponse> {
    console.warn('testFetchSubscription is deprecated. Content is automatically fetched on subscription creation.');
    return {
      success: false,
      error: 'Test Fetch is deprecated. Content is automatically managed by the system.',
    };
  },

  /**
   * 获取订阅的内容列表
   */
  async getSubscriptionContents(
    subscriptionId: number,
    limit: number = 50,
    offset: number = 0,
    onlyUnread: boolean = false
  ): Promise<ApiResponse<SubscriptionContentsResponse>> {
    try {
      const params: any = { limit, offset };
      if (onlyUnread) params.only_unread = true;

      const data = await api.get<SubscriptionContentsResponse>(
        `/subscriptions/${subscriptionId}/contents`,
        params
      );

      return {
        success: true,
        data,
      };
    } catch (error: any) {
      console.error('Failed to get subscription contents:', error);
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to retrieve contents',
      };
    }
  },

  /**
   * 标记内容为已读/未读
   */
  async markContentAsRead(
    contentId: number,
    isRead: boolean = true
  ): Promise<ApiResponse> {
    try {
      const data = await api.post(`/subscriptions/contents/${contentId}/mark-read`, {
        is_read: isRead,
      });

      return {
        success: true,
        data,
      };
    } catch (error: any) {
      console.error('Failed to mark content as read:', error);
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to mark as read',
      };
    }
  },
};

/**
 * 根据推文 ID 获取推文详情（用于显示被回复的原推文）
 */
export const getTweetById = async (tweetId: string, userId: number): Promise<SubscriptionContent | null> => {
  try {
    const response = await api.get<SubscriptionContent | null>(
      `/subscriptions/contents/tweet/${tweetId}`,
      { user_id: userId }  // 直接传 params 对象，不需要包在 params 里
    );
    return response as SubscriptionContent | null;
  } catch (error) {
    console.error('Failed to get tweet by ID:', error);
    return null;
  }
};
