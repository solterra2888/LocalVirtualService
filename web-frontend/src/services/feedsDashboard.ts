/**
 * Feeds Dashboard service — 按主题聚合的重要信息视图 (/feeds-dashboard).
 *
 * 后端: backend/api/feeds_dashboard.py (prefix=/api/feeds-dashboard)
 * 数据链路: feed_topics + feed_topic_daily_items (聚合器零 LLM 写入)
 *          + subscription_content(_tags), 读路径零 LLM
 *
 * 条目对象经 toDailyFeedItem() 适配后可直接喂给 DailyPreviewDrawer
 * (复用 /api/daily/items/feed_<id> 详情与原文渲染器)。
 */

import { api } from './api';
import { DailyFeedItem, DailyItemDetail } from './daily';

// 与 backend/schemas/feeds_dashboard.py 严格对齐
export type FeedsImportance = 'high' | 'medium' | 'low';
export type FeedsMinImportance = 'high' | 'medium' | 'low';

export interface FeedsDashboardItem {
  id: string;                    // 'feed_<subscription_content_id>'
  source: 'feed';
  source_subtype: string;        // 与 Daily 口径一致: twitter/news/youtube_feed/...
  channel_type: string;          // DB 原始 channel_type
  title: string;
  summary: string | null;
  author_name: string | null;
  author_identifier: string | null;
  author_avatar: string | null;
  tickers: string[];
  importance: FeedsImportance;
  score: number;
  published_at: string;          // ISO 8601
  url: string | null;
  /** 是否在当前用户订阅范围内; server 视图下 false → 点击走原文外链 (抽屉详情有用户鉴权) */
  in_user_scope: boolean;
  navigate: {
    type: 'feed_article' | 'external_url' | 'execution';
    subscription_id?: number | null;
    content_id?: number | null;
    external_url?: string | null;
  } | null;
}

export interface FeedsDashboardTopic {
  topic_id: number;
  topic_key: string;
  name_cn: string;
  name_en: string | null;
  category: string | null;
  needs_review: boolean;
  item_count: number;
  high_count: number;
  medium_count: number;
  top_tickers: string[];
  items: FeedsDashboardItem[];
  has_more: boolean;
}

export interface FeedsDashboardResponse {
  date: string;
  tz: string;
  min_importance: string | null;
  topics: FeedsDashboardTopic[];
  total_items: number;
  unique_content_count: number;
  aggregated_at: string | null;
}

export interface FeedsDashboardTopicItemsResponse {
  topic_id: number;
  topic_name: string;
  date: string;
  items: FeedsDashboardItem[];
  total: number;
  has_more: boolean;
  limit: number;
  offset: number;
}

export type FeedsDashboardScope = 'mine' | 'server';

export interface FeedsDashboardQuery {
  date?: string;                 // YYYY-MM-DD 单日 (与 days 二选一)
  days?: number;                 // 最近 N 天: 1=当天(默认) 3=近3天 7=近一周
  min_importance?: FeedsMinImportance;
  channels?: string[];
  items_per_topic?: number;
  scope?: FeedsDashboardScope;   // mine=我的订阅(默认) | server=全站内容池
}

export interface FeedsDashboardGenerateRequest {
  date?: string;
  days?: number;
  force?: boolean;
}

/** buildQueryString 接受的全部可序列化字段 (含 /items 端点专用的词/主题过滤参数) */
type QueryWithStringExtras = FeedsDashboardQuery & {
  word?: string;
  word_type?: string;
  topic_id?: number;
};

/**
 * 构造 FastAPI 兼容的查询字符串 (数组重复 key 不带方括号, 与 services/daily.ts 同口径).
 * ⚠ 新增查询字段时必须在此同步序列化, 否则会被静默丢弃 (词过滤曾因此失效).
 */
function buildQueryString(query: QueryWithStringExtras): string {
  const params = new URLSearchParams();
  if (query.date) params.append('date', query.date);
  if (query.days !== undefined) params.append('days', String(query.days));
  if (query.min_importance) params.append('min_importance', query.min_importance);
  if (query.items_per_topic !== undefined) params.append('items_per_topic', String(query.items_per_topic));
  if (query.word) params.append('word', query.word);
  if (query.word_type) params.append('word_type', query.word_type);
  if (query.topic_id !== undefined) params.append('topic_id', String(query.topic_id));
  if (query.scope) params.append('scope', query.scope);
  (query.channels || []).forEach((c) => params.append('channels', c));
  return params.toString();
}

export async function getFeedsDashboard(query: FeedsDashboardQuery = {}): Promise<FeedsDashboardResponse> {
  const qs = buildQueryString(query);
  // api.get 已解包 data; 查询串手工构造 (数组重复 key 不带方括号, FastAPI 兼容)
  return api.get<FeedsDashboardResponse>(`/feeds-dashboard/${qs ? `?${qs}` : ''}`);
}

export async function getFeedsDashboardTopicItems(
  topicId: number,
  query: FeedsDashboardQuery & { limit?: number; offset?: number } = {},
): Promise<FeedsDashboardTopicItemsResponse> {
  const { limit, offset, ...rest } = query;
  const params = new URLSearchParams(buildQueryString(rest));
  if (limit !== undefined) params.append('limit', String(limit));
  if (offset !== undefined) params.append('offset', String(offset));
  return api.get<FeedsDashboardTopicItemsResponse>(
    `/feeds-dashboard/topics/${topicId}/items?${params.toString()}`,
  );
}

export async function generateFeedsDashboard(
  req: FeedsDashboardGenerateRequest = {},
): Promise<{ success: boolean; celery_task_id: string | null; message: string }> {
  return api.post('/feeds-dashboard/generate', req);
}

// ─── 词云 (当日热词, 点击过滤) ───────────────────────────────

export type FeedsWordType = 'concept' | 'ticker';
export type FeedsCloudWordType = 'concept' | 'ticker' | 'watchlist';

export interface FeedsDashboardWord {
  word: string;
  count: number;
  type: FeedsCloudWordType;
  /** 折并到的主题 id (点击走 topic 过滤, 命中该主题全部变体); 散词/ticker 为 null */
  topic_id?: number | null;
}

export interface FeedsDashboardWordCloudResponse {
  date: string;
  words: FeedsDashboardWord[];
  watchlist: FeedsDashboardWord[];
  unique_content_count: number;
}

export async function getFeedsDashboardWordCloud(
  query: FeedsDashboardQuery = {},
): Promise<FeedsDashboardWordCloudResponse> {
  const qs = buildQueryString(query);
  return api.get<FeedsDashboardWordCloudResponse>(`/feeds-dashboard/wordcloud${qs ? `?${qs}` : ''}`);
}

export interface FeedsDashboardItemsResponse {
  date: string;
  word: string | null;
  word_type: FeedsWordType | null;
  items: FeedsDashboardItem[];
  total: number;
  has_more: boolean;
  limit: number;
  offset: number;
}

export async function getFeedsDashboardItems(
  query: FeedsDashboardQuery & {
    word?: string;
    word_type?: FeedsWordType;
    topic_id?: number;
    limit?: number;
    offset?: number;
  } = {},
): Promise<FeedsDashboardItemsResponse> {
  const { limit, offset, ...rest } = query;
  const params = new URLSearchParams(buildQueryString(rest));
  if (limit !== undefined) params.append('limit', String(limit));
  if (offset !== undefined) params.append('offset', String(offset));
  return api.get<FeedsDashboardItemsResponse>(
    `/feeds-dashboard/items${params.toString() ? `?${params.toString()}` : ''}`,
  );
}

/**
 * Dashboard 条目 → DailyFeedItem (DailyPreviewDrawer 兼容).
 *
 * Drawer 内部按 item.id 调 /api/daily/items/{id} 拉详情, 因此只需保证
 * 复合 ID 形如 'feed_<id>' 且必填字段齐全; Dashboard 侧不展示的字段填空值。
 */
/** DailyFeedItem + Dashboard 侧的 scope 标记 (抽屉详情路由用, 见 getFeedsDashboardItemDetail) */
export interface FeedsDashboardDrawerItem extends DailyFeedItem {
  in_user_scope?: boolean;
}

export function toDailyFeedItem(item: FeedsDashboardItem): FeedsDashboardDrawerItem {
  return {
    id: item.id,
    source: 'feed',
    source_subtype: item.source_subtype as DailyFeedItem['source_subtype'],
    title: item.title,
    summary: item.summary,
    snippet: null,
    author_name: item.author_name,
    author_identifier: item.author_identifier,
    author_avatar: item.author_avatar,
    sector: null,
    industry: null,
    tickers: item.tickers,
    concepts: [],
    confidence: null,
    published_at: item.published_at,
    reading_time_min: null,
    in_user_scope: item.in_user_scope,
    navigate: {
      type: (item.navigate?.type as DailyFeedItem['navigate']['type']) || 'feed_article',
      subscription_id: item.navigate?.subscription_id ?? null,
      content_id: item.navigate?.content_id ?? null,
      external_url: item.navigate?.external_url ?? item.url,
    },
    shared_by_username: null,
    shared_by_user_id: null,
    shared_by_avatar_url: null,
    shared_to: [],
  };
}

/**
 * 全站视图条目详情 (Drawer 预览用): 订阅外内容在 /api/daily/items 会因用户鉴权 404,
 * 走本端点 (后端复用 Daily 装配逻辑, 由 FEEDS_DASHBOARD_SERVER_SCOPE_ENABLED 门控)。
 */
export async function getFeedsDashboardItemDetail(
  contentDbId: number,
): Promise<DailyItemDetail> {
  return api.get<DailyItemDetail>(`/feeds-dashboard/items/${contentDbId}/detail`);
}
