import type { SubscriptionContent } from '../../services/subscriptions';
import { substackHtmlToPlainText } from '../../utils/substackHtmlUtils';
import { getSubstackContentKind } from './SubstackFeedBadges';
import { weiboSafeAvatarSrc } from './weiboSafeImage';

/** 列表/详情作者头像（Twitter / 微博 / Substack / 知识星球）。 */
export function getFeedAuthorAvatarSrc(c: {
  channel_type?: string;
  source_subtype?: string;
  author_avatar?: string | null;
}): string | undefined {
  if (!c.author_avatar) return undefined;
  const kind = c.channel_type ?? c.source_subtype;
  if (kind === 'twitter' || kind === 'weibo' || kind === 'substack' || kind === 'zhishixingqiu') {
    return weiboSafeAvatarSrc(c.author_avatar);
  }
  return undefined;
}

export function feedListShowsAuthorAvatar(c: {
  channel_type?: string;
  source_subtype?: string;
  author_avatar?: string | null;
}): boolean {
  return Boolean(getFeedAuthorAvatarSrc(c));
}

/** New items notification (Web Audio API) */
export function playNewItemsNotificationSound() {
  try {
    const Ctx = window.AudioContext || (window as unknown as { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
    if (!Ctx) return;
    const ctx = new Ctx();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.frequency.value = 800;
    osc.type = 'sine';
    gain.gain.setValueAtTime(0.25, ctx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.15);
    osc.start(ctx.currentTime);
    osc.stop(ctx.currentTime + 0.15);
  } catch {
    // ignore
  }
}

/** Collapse whitespace/newlines for list previews so line-clamp works on YouTube descriptions. */
export function collapseWhitespaceForPreview(s: string): string {
  return (s || '').replace(/\s+/g, ' ').trim();
}

/** Twitter handle: trim and strip one leading @ before API storage. */
export function normalizeTwitterUsername(raw: string): string {
  const t = (raw || '').trim();
  return t.startsWith('@') ? t.slice(1).trim() : t;
}

/**
 * Channels whose items carry no `title` — the body text IS the headline
 * (e.g. Twitter tweets, Weibo posts). Kept as a Set so a new titleless
 * channel is a one-line add.
 */
const TITLELESS_CHANNELS = new Set(['twitter', 'weibo']);

function getSubstackPlainBody(c: SubscriptionContent): string {
  if (c.channel_type !== 'substack') return (c.content || '').trim();
  if (getSubstackContentKind(c) === 'note') return (c.content || '').trim();
  return substackHtmlToPlainText(c.content || '');
}

/** 列表卡片摘要行：Substack 文章去掉 HTML 标签 */
export function getFeedListBodyExcerpt(c: SubscriptionContent): string {
  if (c.channel_type === 'substack' && getSubstackContentKind(c) === 'post') {
    return getSubstackPlainBody(c);
  }
  return (c.content || '').trim();
}

/**
 * List card primary line: YouTube/News use `title`; titleless channels
 * (Twitter, Weibo) fall back to body text.
 */
export function getFeedListHeadline(c: SubscriptionContent): string {
  const t = (c.title || '').trim();
  if (t) return t;
  if (TITLELESS_CHANNELS.has(c.channel_type)) {
    const body = (c.content || '').trim().replace(/\s+/g, ' ');
    if (body) return body;
    return c.author_name || (c.author_identifier ? `@${c.author_identifier}` : 'Post');
  }
  return t || 'Untitled';
}

/**
 * Whether to show the secondary excerpt line (`content`). For Twitter without `title`, the headline
 * already shows the tweet — skip the duplicate line.
 */
export function showFeedListSecondaryExcerpt(c: SubscriptionContent): boolean {
  if (TITLELESS_CHANNELS.has(c.channel_type) && !(c.title || '').trim()) return false;
  if (c.channel_type === 'substack' && getSubstackContentKind(c) === 'post') {
    return Boolean(getSubstackPlainBody(c));
  }
  return Boolean((c.content || '').trim());
}

/**
 * Twitter 多数无 title，列表主行实为推文正文 — 应用正文排版，勿用标题字重/字号。
 */
export function feedListPrimaryLineIsTweetBody(c: SubscriptionContent): boolean {
  return TITLELESS_CHANNELS.has(c.channel_type) && !(c.title || '').trim();
}

/** 与 Daily 模块一致：`max(1, ceil(chars / 400))`。见 `backend/api/daily.py` WORDS_PER_MINUTE */
const READING_CHARS_PER_MINUTE = 400;

/**
 * 用列表接口已有字段估算阅读分钟数（与 Daily `reading_time_min` 同一公式）。
 * Twitter / 微博 等无标题渠道合并正文与嵌入的 retweet / quote 文本；其余渠道仅用 `content`
 *（列表不含 transcript，详情页字幕另行展示）。
 */
export function estimateSubscriptionContentReadingMinutes(
  c: SubscriptionContent,
): number | null {
  let len = (c.content || '').length;
  if (c.channel_type === 'substack' && getSubstackContentKind(c) === 'post') {
    len = getSubstackPlainBody(c).length;
  } else if (TITLELESS_CHANNELS.has(c.channel_type)) {
    len += (c.retweeted_content || '').length;
    len += (c.quoted_content || '').length;
  }
  if (len <= 0) return null;
  return Math.max(1, Math.ceil(len / READING_CHARS_PER_MINUTE));
}

/**
 * Per-channel engagement metric keys, normalized into a common shape.
 * Twitter: like_count / retweet_count / reply_count / quote_count.
 * Weibo:   attitudes_count (赞) / reposts_count (转发) / comments_count (评论) — no quote.
 * Returns null when no metric is present, so callers can skip the whole badge row.
 */
export type FeedEngagementCounts = {
  like?: number;
  retweet?: number;
  reply?: number;
  quote?: number;
};

export function getFeedEngagementCounts(c: SubscriptionContent): FeedEngagementCounts | null {
  const md = (c.metadata || {}) as Record<string, unknown>;
  const isWeibo = c.channel_type === 'weibo';
  const rawLike = isWeibo ? md.attitudes_count : md.like_count;
  const rawRetweet = isWeibo ? md.reposts_count : md.retweet_count;
  const rawReply = isWeibo ? md.comments_count : md.reply_count;
  const rawQuote = isWeibo ? undefined : md.quote_count;
  const result: FeedEngagementCounts = {
    like: typeof rawLike === 'number' ? rawLike : undefined,
    retweet: typeof rawRetweet === 'number' ? rawRetweet : undefined,
    reply: typeof rawReply === 'number' ? rawReply : undefined,
    quote: typeof rawQuote === 'number' ? rawQuote : undefined,
  };
  if (
    result.like == null &&
    result.retweet == null &&
    result.reply == null &&
    result.quote == null
  ) {
    return null;
  }
  return result;
}

/** Heat level badge config for subscription content */
export function getHeatLevelConfig(heatLevel?: number) {
  switch (heatLevel) {
    case 1:
      return { color: '#8c8c8c', text: 'Low heat', bgColor: 'rgba(140, 140, 140, 0.1)' };
    case 2:
      return { color: '#1890ff', text: 'Medium heat', bgColor: 'rgba(24, 144, 255, 0.1)' };
    case 3:
      return { color: '#fa8c16', text: 'High heat', bgColor: 'rgba(250, 140, 22, 0.1)' };
    case 4:
      return { color: '#f5222d', text: 'Very high heat', bgColor: 'rgba(245, 34, 45, 0.1)' };
    default:
      return { color: '#8c8c8c', text: 'Low heat', bgColor: 'rgba(140, 140, 140, 0.1)' };
  }
}

/** Client-side filter for feed article lists (title, body, author, tags, etc.). */
export function subscriptionContentMatchesSearchQuery(c: SubscriptionContent, rawQuery: string): boolean {
  const q = rawQuery.trim().toLowerCase();
  if (!q) return true;
  const hay: string[] = [
    c.title,
    c.content,
    getFeedListBodyExcerpt(c),
    c.author_name,
    c.author_identifier,
    getFeedListHeadline(c),
    c.personalized_summary ?? undefined,
    c.tags?.summary ?? undefined,
    ...(c.tags?.tickers ?? []),
    ...(c.tags?.concepts ?? []),
    c.retweeted_content,
    c.quoted_content,
  ].filter((s): s is string => Boolean(s && String(s).trim()));
  return hay.some((s) => String(s).toLowerCase().includes(q));
}
