/**
 * Daily 聚合视图 service.
 *
 * 把 Upload Files (file_processing_details + file_tags) 与 Feeds 订阅
 * (subscription_content + subscription_content_tags) 合并成同一时间轴卡片流，
 * 作为登录后的默认落地页 (/daily)。
 *
 * 后端: backend/api/daily.py — endpoints: GET /api/daily/feed, GET /api/daily/stats
 * 设计参考: AI Notes Daily 的 "Summaries" 页面
 */

import { api } from './api';

// 与 backend/schemas/daily.py 严格对齐
export type DailySource = 'upload' | 'feed';

export type DailySourceSubtype =
  | 'document'    // PDF / Word / Excel / CSV (Upload)
  | 'image'       // Image (Upload)
  | 'audio'       // Audio (Upload)
  | 'youtube'     // Upload YouTube Link
  | 'web'         // Upload Web Link
  | 'text'        // My Notes / Thoughts (Upload)
  | 'podcast'     // Apple Podcast Episode (Upload)
  | 'twitter'     // Feed Twitter/X
  | 'news'        // Feed News
  | 'youtube_feed'; // Feed YouTube Subscription

export type DailyNavigateType = 'execution' | 'feed_article' | 'external_url';

export interface DailyNavigate {
  type: DailyNavigateType;
  execution_id?: number | null;     // Upload → processing_stats.id
  subscription_id?: number | null;  // Feed → subscription_list.id
  content_id?: number | null;       // Feed → subscription_content.id
  external_url?: string | null;     // 原始外链
}

export interface DailyShareRecipient {
  user_id: number;
  username: string;
  avatar_url: string | null;
}

export interface DailyFeedItem {
  id: string;                  // 复合 ID: upload_<id> 或 feed_<id>
  source: DailySource;
  source_subtype: DailySourceSubtype;
  title: string;
  summary: string | null;
  snippet: string | null;
  author_name: string | null;
  author_identifier: string | null;
  sector: string | null;
  industry: string | null;
  tickers: string[];
  concepts: string[];
  confidence: number | null;
  published_at: string;        // ISO 8601
  reading_time_min: number | null;
  navigate: DailyNavigate;

  /**
   * Upload 共享标记：当前卡片是他人分享给当前用户的文件时非 null。
   * Feed 卡片 / 当前用户自己上传的文件 = null。
   */
  shared_by_username: string | null;
  shared_by_user_id: number | null;
  /** OSS 签名 URL 或外链; 无头像或非共享卡片时为 null */
  shared_by_avatar_url: string | null;
  /** owner 分享给的用户列表；被分享者视角为空数组 */
  shared_to: DailyShareRecipient[];
}

export interface DailyFeedResponse {
  items: DailyFeedItem[];
  total: number;
  has_more: boolean;
  limit: number;
  offset: number;
}

export interface DailySourceCount {
  source_subtype: DailySourceSubtype;
  count: number;
}

export interface DailyStatsResponse {
  total: number;
  by_source: DailySourceCount[];
}

export interface DailyFeedQuery {
  date_from?: string;             // ISO 8601
  date_to?: string;               // ISO 8601
  source_subtypes?: DailySourceSubtype[];
  q?: string;
  tickers?: string[];
  limit?: number;
  offset?: number;
}

/**
 * 构造 FastAPI 兼容的查询字符串.
 *
 * axios 默认把数组序列化为 ?source_subtypes[]=a&source_subtypes[]=b（带方括号），
 * 但 FastAPI Query(List[str]) 要求重复 key 不带括号：?source_subtypes=a&source_subtypes=b.
 * 因此这里手动用 URLSearchParams 构造，而不是直接传 params 对象给 axios.
 */
function buildQueryString(query: DailyFeedQuery): string {
  const params = new URLSearchParams();
  if (query.q)         params.append('q', query.q);
  if (query.date_from) params.append('date_from', query.date_from);
  if (query.date_to)   params.append('date_to', query.date_to);
  if (query.limit  !== undefined) params.append('limit',  String(query.limit));
  if (query.offset !== undefined) params.append('offset', String(query.offset));
  query.source_subtypes?.forEach(st => params.append('source_subtypes', st));
  query.tickers?.forEach(t => params.append('tickers', t));
  return params.toString();
}

/**
 * 拉取聚合信息流（按 published_at 倒序）。
 * 前端按 published_at 的 YYYY-MM-DD 分组渲染卡片。
 */
export async function listDailyFeed(query: DailyFeedQuery = {}): Promise<DailyFeedResponse> {
  const qs = buildQueryString(query);
  return api.get<DailyFeedResponse>(`/daily/feed?${qs}`);
}

/**
 * 顶部统计：返回当前过滤条件下每个来源子类型的卡片数。
 */
export async function getDailyStats(
  query: Omit<DailyFeedQuery, 'source_subtypes' | 'limit' | 'offset'> = {}
): Promise<DailyStatsResponse> {
  const qs = buildQueryString(query);
  return api.get<DailyStatsResponse>(`/daily/stats?${qs}`);
}

/**
 * 已知所有合法 source_subtype，前端过滤 chip 渲染顺序参考此数组。
 */
export const ALL_DAILY_SOURCE_SUBTYPES: DailySourceSubtype[] = [
  'document',
  'image',
  'audio',
  'youtube',
  'web',
  'text',
  'podcast',
  'twitter',
  'news',
  'youtube_feed',
];

// ─── Preset (per-user) ─────────────────────────────────────────────
//
// 每个用户可在 Daily 页面通过齿轮入口配置一份 preset, 限制信息流只展示
// 选中的 source_subtype。语义约定:
//   - source_subtypes = []   → 未设置 / 等同于"展示全部"
//   - source_subtypes = [..] → 仅展示这些子类型 (chip 行也只显示这些)
//
// 后端: backend/api/daily.py — endpoints: GET/PUT /api/daily/preset

export interface DailyPresetResponse {
  source_subtypes: DailySourceSubtype[];
}

export interface DailyPresetUpdate {
  source_subtypes: DailySourceSubtype[];
}

/** 读取当前用户的 Daily preset; 未设置时返回 source_subtypes: []. */
export async function getDailyPreset(): Promise<DailyPresetResponse> {
  return api.get<DailyPresetResponse>('/daily/preset');
}

/** 保存当前用户的 Daily preset; 传空数组等同清除 preset. */
export async function updateDailyPreset(
  payload: DailyPresetUpdate
): Promise<DailyPresetResponse> {
  return api.put<DailyPresetResponse>('/daily/preset', payload);
}

// ─── Item detail (Drawer 预览用) ───────────────────────────────────
//
// 卡片点击后, 前端不再跳出到 Sources/Feeds, 而是在 Daily 内打开右侧抽屉预览.
// 抽屉里上半部分摘要/标签/作者/时间复用 DailyFeedItem (列表已有), 下半部分
// "原文区" 按 source_subtype 差异化呈现, 数据由本接口提供.
//
// 注意:
//   - 二进制资源 (image/audio/pdf 的 file_url + audio/youtube 的 asr) 不在
//     此返回, 由前端按需调 /api/files/original/{file_id} (复用 files 模块).
//   - YouTube transcript 同样懒加载 /api/subscriptions/contents/{id}/transcript.
//
// 后端: backend/api/daily.py — endpoint: GET /api/daily/items/{id}

export interface DailyItemDetailUpload {
  file_id: number;
  /** file_processing_details.file_type 原始值 (pdf/word/excel/csv/image/audio/youtube/web/text) */
  file_type_raw: string | null;
  display_name: string | null;
  filename: string | null;
  /** 本地路径 / OSS object key / 外链 URL (兜底); 预览资源直链请用 /api/files/original */
  original_file_path: string | null;
  /** Markdown 正文 / OCR 文本 / 网页正文 / 笔记 */
  content: string | null;
  /** processing_stats.final_report (执行级 AI 分析报告) */
  final_report: string | null;
}

export interface DailyItemDetailFeedTags {
  sector: string | null;
  sector_en?: string | null;
  industry: string | null;
  industry_en?: string | null;
  tickers: string[];
  concepts: string[];
  summary?: string | null;
  confidence?: number | null;
  tagged_at?: string | null;
}

export interface DailyItemDetailFeed {
  content_db_id: number;
  channel_type: string;             // twitter / youtube / news / rss
  content_id: string | null;        // tweet_id / video_id / news guid
  title: string | null;
  content: string | null;           // 正文 (twitter 推文 / youtube 描述 / news 全文)
  author_name: string | null;
  author_identifier: string | null;
  url: string | null;
  media_urls: string[] | null;
  metadata: Record<string, any> | null;
  categories: string[] | null;

  // Twitter 特定字段
  is_retweet: boolean | null;
  retweeted_author: string | null;
  retweeted_content: string | null;
  retweeted_url: string | null;
  is_quote: boolean | null;
  quoted_author: string | null;
  quoted_content: string | null;
  quoted_url: string | null;
  is_reply: boolean | null;
  in_reply_to_id: string | null;
  in_reply_to_username: string | null;
  in_reply_to_user_id: string | null;
  heat_level: number | null;

  /** YouTube 是否有 transcript; 正文需懒加载 /subscriptions/contents/:id/transcript */
  has_transcript: boolean;

  /** AI 标签 lite (与列表行 tags 同形, 未打标 = null) */
  tags: DailyItemDetailFeedTags | null;

  /**
   * PR-D1: per-user 个性化摘要 (subscription_content_user_summaries.summary).
   * 抽屉 "AI 深度分析" 段消费; null = 该用户对本条 feed 还没生成个性化.
   * 由 personalize_feed_summary_task 写入 (用户开启自动 fan-out 后随 tag 完成自动生成).
   */
  personalized_summary?: string | null;
  /** 生成时使用的 llm_prompt_templates.id (feed_summary / youtube_summary 模块) */
  personalized_template_id?: number | null;
  /** ISO 8601 时间戳 (subscription_content_user_summaries.tagged_at) */
  personalized_at?: string | null;
}

export interface DailyItemDetail extends DailyFeedItem {
  upload: DailyItemDetailUpload | null;
  feed: DailyItemDetailFeed | null;
}

/** 拉取单条卡片完整详情 (Drawer 预览主调用). */
export async function getDailyItemDetail(itemId: string): Promise<DailyItemDetail> {
  return api.get<DailyItemDetail>(`/daily/items/${encodeURIComponent(itemId)}`);
}

// ─── Calendar (日期导航栏) ────────────────────────────────────────
//
// GET /api/daily/calendar：返回最近 N 天每天的卡片总数与 source_subtype 分布。
// 前端左侧日期导航栏 DailyDateSidebar 消费本接口。
// 随 baseQuery（searchQuery / activeSubtypes / tickers）联动，与信息流过滤口径一致。

export interface DailyCalendarDaySource {
  source_subtype: DailySourceSubtype;
  count: number;
}

export interface DailyCalendarDay {
  /** YYYY-MM-DD，按用户时区切桶 */
  date: string;
  /** 当天总条数，0 = 空白日 */
  total: number;
  /** 各 source_subtype 分布，按 count 降序，0 条类型省略 */
  by_source: DailyCalendarDaySource[];
}

export interface DailyCalendarResponse {
  /** 实际使用的时区 (IANA) */
  tz: string;
  /** 实际返回的天数 */
  days: number;
  /**
   * 按日期倒序排列的日历数据（含 0 条空白日），长度 == days。
   * 第一条为今天，最后一条为 N 天前。
   */
  items: DailyCalendarDay[];
  /** 窗口内单日最大条数，用于活跃度等级归一化 */
  max_total: number;
  /** 窗口内总条数 */
  total_in_window: number;
}

export interface DailyCalendarQuery {
  /** IANA 时区名，如 Asia/Hong_Kong；默认 UTC */
  tz?: string;
  /** 从今天往回看的天数，1-180，默认 30 */
  days?: number;
  source_subtypes?: DailySourceSubtype[];
  q?: string;
  tickers?: string[];
}

/**
 * 拉取日历统计（左侧日期导航栏用）。
 * 随 baseQuery 联动——过滤条件改变时日期栏的各日数字也会同步更新。
 */
export async function getDailyCalendar(
  query: DailyCalendarQuery = {}
): Promise<DailyCalendarResponse> {
  const params = new URLSearchParams();
  if (query.tz)   params.append('tz',   query.tz);
  if (query.days  !== undefined) params.append('days',  String(query.days));
  if (query.q)    params.append('q',    query.q);
  query.source_subtypes?.forEach(st => params.append('source_subtypes', st));
  query.tickers?.forEach(t => params.append('tickers', t));
  return api.get<DailyCalendarResponse>(`/daily/calendar?${params.toString()}`);
}
