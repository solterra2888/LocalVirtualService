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
  channel_avatar?: string; // 频道头像 URL
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

/**
 * Feeds 单条内容的 AI 自动打标 + 摘要 (Phase 3).
 *
 * 由 /api/subscriptions/{id}/contents 返回时作为 SubscriptionContent.tags 字段携带,
 * 未打标 → null. 手动获取单条详情走 /api/subscriptions/contents/{id}/tags.
 *
 * 设计文档: docs/instructions/FEEDS_AUTO_TAGGING_PLAN.md
 */
export interface TagPerson {
  name: string;
  name_en?: string;
  role?: string;
  affiliation?: string | null;
}

export interface SubscriptionContentTagsLite {
  sector: string;
  sector_en?: string;
  industry: string;
  industry_en?: string;
  tickers: string[];
  concepts: string[];
  summary?: string | null;
  confidence?: number | null;
  tagged_at?: string | null;
  people?: TagPerson[] | null;
  countries?: string[] | null;
  tag_schema_version?: string | null;
  additional_tags?: Record<string, any> | null;
}

export interface SubscriptionContentTagsFull extends SubscriptionContentTagsLite {
  id: number;
  subscription_content_id: number;
  reasoning?: string | null;
  channel_type: string;
  tagged_by?: number | null;
  created_at?: string | null;
  updated_at?: string | null;
}

export interface SubscriptionContent {
  id: number;
  channel_type: string;
  content_id: string;
  title?: string;
  content: string;
  author_name?: string;
  author_identifier: string;
  author_avatar?: string; // 作者头像 URL (Twitter profilePicture)
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
  // YouTube: 仅是否有 transcript（正文不返回，减轻加载）
  has_transcript?: boolean;
  // RSS/News 特定字段
  categories?: string[];
  // Phase 3 新增: AI 自动打标 + 摘要 (未打标为 null, 避免前端把 undefined 当已打标处理)
  tags?: SubscriptionContentTagsLite | null;

  // PR-7 新增: per-user 个性化 summary 覆盖层
  /**
   * - 'personalized': 用户已生成自定义 summary, 前端用 personalized_summary 显示
   * - 'global'      : 用户未个性化, 用 tags?.summary (全局 baseline)
   * - 'none'        : 极少数情况下后端附加失败时的兜底
   */
  summary_source?: 'personalized' | 'global' | 'none';
  personalized_summary?: string | null;
  personalized_reasoning?: string | null;
  personalized_template_id?: number | null;
  personalized_summary_id?: number | null;
  personalized_tagged_at?: string | null;
}

export interface FeedTaggingTaskStatus {
  task_id: string;
  status: string;
  message?: string;
  content_id?: number;
}

export interface FeedDistinctTickerItem {
  ticker: string;
  content_count: number;
}

export interface FeedDistinctConceptItem {
  concept: string;
  content_count: number;
}

export interface SubscriptionContentsResponse {
  contents: SubscriptionContent[];
  total: number;
  limit: number;
  offset: number;
}

/** GET /subscriptions/contents/:id/transcript — subscription_content.transcript only */
export interface SubscriptionContentTranscriptPayload {
  transcript: string;
}

/** GET /subscriptions/:id/digest */
export interface SubscriptionDigestPayload {
  subscription_id: number;
  channel_name: string;
  channel_type: string;
  days: number;
  since: string;
  show_filter: string | null;
  items_count: number;
  skipped_count: number;
  total_chars: number;
  truncated: boolean;
  text: string;
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

      const data = await api.get<Subscription[]>('/subscriptions/', params);

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
    onlyUnread: boolean = false,
    publishedAfter?: string
  ): Promise<ApiResponse<SubscriptionContentsResponse>> {
    try {
      const params: any = { limit, offset };
      if (onlyUnread) params.only_unread = true;
      if (publishedAfter) params.published_after = publishedAfter;

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
   * 触发 YouTube 视频 Fun-ASR 转写（针对无字幕视频）
   */
  /**
   * 按需加载单条内容的字幕/转写全文（列表仅返回 has_transcript）
   */
  async getSubscriptionContentTranscript(
    contentDbId: number
  ): Promise<ApiResponse<SubscriptionContentTranscriptPayload>> {
    try {
      const data = await api.get<SubscriptionContentTranscriptPayload>(
        `/subscriptions/contents/${contentDbId}/transcript`
      );
      return { success: true, data };
    } catch (error: any) {
      console.error('Failed to load subscription content transcript:', error);
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to load transcript',
      };
    }
  },

  async triggerYouTubeASR(contentDbId: number): Promise<{
    success: boolean;
    task_id?: string;
    video_id?: string;
    message?: string;
    error?: string;
  }> {
    try {
      const data = await api.post<{
        task_id: string;
        video_id: string;
        content_db_id: number;
        message: string;
      }>(`/subscriptions/contents/${contentDbId}/transcribe`, {});
      return { success: true, ...data };
    } catch (error: any) {
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to trigger ASR transcription',
      };
    }
  },

  /**
   * 获取订阅在过去 N 天内出现的 distinct show_name 列表（YouTube only）。
   * 用于 Digest 配置面板的 Program filter，避免依赖前端已加载的有限列表。
   */
  async getSubscriptionShows(
    subscriptionId: number,
    days: 1 | 3 | 7,
  ): Promise<ApiResponse<{ shows: string[] }>> {
    try {
      const data = await api.get<{ shows: string[] }>(
        `/subscriptions/${subscriptionId}/shows`,
        { days },
      );
      return { success: true, data };
    } catch (error: any) {
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to fetch shows',
      };
    }
  },

  /**
   * 获取订阅 N 天内主要内容拼接文本（Digest），供用户复制给 LLM 插件。
   */
  async getSubscriptionDigest(
    subscriptionId: number,
    days: 1 | 3 | 7,
    options?: {
      showFilter?: string | null;
      maxTotalChars?: number;
    },
  ): Promise<ApiResponse<SubscriptionDigestPayload>> {
    try {
      const params: Record<string, any> = { days };
      if (options?.showFilter) params.show_filter = options.showFilter;
      if (options?.maxTotalChars) params.max_total_chars = options.maxTotalChars;
      const data = await api.get<SubscriptionDigestPayload>(
        `/subscriptions/${subscriptionId}/digest`,
        params,
      );
      return { success: true, data };
    } catch (error: any) {
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to fetch digest',
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

  // ─── Feeds 自动打标 + 摘要 (Phase 3) ──────────────────────────────
  // 服务端: backend/api/subscription_content_tags.py
  // 设计文档: docs/instructions/FEEDS_AUTO_TAGGING_PLAN.md §7 §8

  /**
   * 获取单条 feed content 的完整 AI 标签 (含 reasoning / additional_tags 等).
   *
   * 注意: 列表接口 `/subscriptions/{id}/contents` 返回的 tags 字段是
   * SubscriptionContentTagsLite (精简版), 不需要独立 fetch. 只有详情视图
   * 需要 reasoning / ai_metadata 时才调这个.
   */
  async getContentTags(
    contentId: number
  ): Promise<ApiResponse<SubscriptionContentTagsFull>> {
    try {
      const data = await api.get<SubscriptionContentTagsFull>(
        `/subscriptions/contents/${contentId}/tags`
      );
      return { success: true, data };
    } catch (error: any) {
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to load content tags',
      };
    }
  },

  /**
   * 异步重新为单条 feed content 打标 + 生成摘要.
   * 默认 force=true (手动触发即重跑), 返回 task_id 供前端轮询进度.
   */
  async retagContent(
    contentId: number,
    options?: { force?: boolean; additionalContext?: string }
  ): Promise<ApiResponse<FeedTaggingTaskStatus>> {
    try {
      const data = await api.post<FeedTaggingTaskStatus>(
        `/subscriptions/contents/${contentId}/retag`,
        {
          force: options?.force ?? true,
          additional_context: options?.additionalContext ?? '',
        }
      );
      return { success: true, data };
    } catch (error: any) {
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to re-tag content',
      };
    }
  },

  // ─── PR-7: per-user 个性化 summary ─────────────────────────────────
  /**
   * 异步为当前用户生成 (或刷新) 该 feed 的个性化 summary.
   *
   * - 不影响其他用户视图 (subscription_content_user_summaries 单表 per-user)
   * - 模板解析: runtime template_id > user_template_preferences[feed_summary] > 抛错
   *   (feed_summary 无系统默认; 用户必须先在 Settings 设偏好或当场选模板)
   * - 返回 task_id, 前端可轮询; 推荐做法是 1.5s 后刷新内容列表读最新 summary
   */
  async personalizeContentSummary(
    contentId: number,
    options?: {
      templateId?: number | null;
      templateVariables?: Record<string, any>;
      force?: boolean;
    }
  ): Promise<ApiResponse<FeedTaggingTaskStatus>> {
    try {
      const data = await api.post<FeedTaggingTaskStatus>(
        `/subscriptions/contents/${contentId}/personalize`,
        {
          template_id: options?.templateId ?? null,
          template_variables: options?.templateVariables ?? null,
          force: options?.force ?? false,
        }
      );
      return { success: true, data };
    } catch (error: any) {
      return {
        success: false,
        error:
          error.response?.data?.detail ||
          'Failed to personalize content summary',
      };
    }
  },

  /** 取消当前用户对该 feed 的个性化 (回退到全局 summary) */
  async deletePersonalizedSummary(
    contentId: number
  ): Promise<ApiResponse<{ success: boolean; deleted: boolean }>> {
    try {
      const data = await api.delete<{ success: boolean; deleted: boolean }>(
        `/subscriptions/contents/${contentId}/personalized-summary`
      );
      return { success: true, data };
    } catch (error: any) {
      return {
        success: false,
        error:
          error.response?.data?.detail ||
          'Failed to remove personalized summary',
      };
    }
  },

  /**
   * 手动编辑 feed content 的 AI 标签 (sector/industry/tickers/concepts/summary/etc).
   */
  async updateContentTags(
    contentId: number,
    updates: Partial<SubscriptionContentTagsLite & {
      reasoning?: string;
      additional_tags?: Record<string, any>;
    }>
  ): Promise<ApiResponse<SubscriptionContentTagsFull>> {
    try {
      const data = await api.put<SubscriptionContentTagsFull>(
        `/subscriptions/contents/${contentId}/tags`,
        updates
      );
      return { success: true, data };
    } catch (error: any) {
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to update content tags',
      };
    }
  },

  /**
   * 删除某条内容的 AI 标签 (下次 retag 时会重新生成).
   */
  async deleteContentTags(contentId: number): Promise<ApiResponse> {
    try {
      const data = await api.delete(`/subscriptions/contents/${contentId}/tags`);
      return { success: true, data };
    } catch (error: any) {
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to delete content tags',
      };
    }
  },

  /**
   * 批量重新打标 (派发一串 tag_feed_content_task, countdown 梯度派发).
   */
  async batchRetagContents(
    contentIds: number[],
    force: boolean = true
  ): Promise<
    ApiResponse<{ task_id: string; status: string; total: number; message?: string }>
  > {
    try {
      const data = await api.post<{
        task_id: string;
        status: string;
        total: number;
        message?: string;
      }>('/subscriptions/contents/batch-retag', {
        content_ids: contentIds,
        force,
      });
      return { success: true, data };
    } catch (error: any) {
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to batch re-tag',
      };
    }
  },

  /**
   * 获取 feed 标签里所有去重 ticker (前端筛选自动补全用).
   */
  async getDistinctFeedTickers(params?: {
    search?: string;
    channelType?: string;
  }): Promise<ApiResponse<FeedDistinctTickerItem[]>> {
    try {
      const qs: Record<string, string> = {};
      if (params?.search) qs.search = params.search;
      if (params?.channelType) qs.channel_type = params.channelType;
      const data = await api.get<FeedDistinctTickerItem[]>(
        '/subscriptions/contents/tags/distinct-tickers',
        qs
      );
      return { success: true, data };
    } catch (error: any) {
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to load distinct tickers',
      };
    }
  },

  /**
   * 获取 feed 标签里所有去重 concept.
   */
  async getDistinctFeedConcepts(params?: {
    search?: string;
    channelType?: string;
  }): Promise<ApiResponse<FeedDistinctConceptItem[]>> {
    try {
      const qs: Record<string, string> = {};
      if (params?.search) qs.search = params.search;
      if (params?.channelType) qs.channel_type = params.channelType;
      const data = await api.get<FeedDistinctConceptItem[]>(
        '/subscriptions/contents/tags/distinct-concepts',
        qs
      );
      return { success: true, data };
    } catch (error: any) {
      return {
        success: false,
        error: error.response?.data?.detail || 'Failed to load distinct concepts',
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
