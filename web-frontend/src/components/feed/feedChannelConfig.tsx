import React from 'react';
import {
  AppstoreOutlined,
  TwitterOutlined,
  YoutubeOutlined,
  GlobalOutlined,
  ReadOutlined,
  StarOutlined,
  CustomerServiceOutlined,
} from '@ant-design/icons';

/**
 * 微博官方品牌图标（Bootstrap Icons: sina-weibo, MPL-2.0）。
 * antd icons 无 WeiboOutlined，故 inline SVG；fill=currentColor 继承父色，与 antd Icon 行为一致。
 */
export const WeiboIcon: React.FC<{ style?: React.CSSProperties }> = ({ style }) => (
  <svg
    viewBox="0 0 16 16"
    fill="currentColor"
    xmlns="http://www.w3.org/2000/svg"
    style={{ fontSize: '1em', ...style }}
    aria-hidden
  >
    <path d="M10.878 1.093a4.23 4.23 0 0 1 4.031 1.305 4.22 4.22 0 0 1 .886 4.14v.001a.612.612 0 0 1-1.166-.377 3.01 3.01 0 0 0-3.495-3.873.611.611 0 1 1-.256-1.196" />
    <path d="M3.753 9.465c.548-1.11 1.972-1.74 3.233-1.411 1.304.338 1.971 1.568 1.437 2.764-.541 1.221-2.095 1.875-3.416 1.449-1.271-.411-1.812-1.67-1.254-2.802m2.658.567c.16.066.365-.009.458-.168.088-.16.03-.34-.129-.397-.156-.062-.353.013-.446.168-.09.154-.041.333.117.397m-1.607 1.314c.413.188.963.009 1.219-.4.252-.413.12-.883-.296-1.062-.41-.172-.94.005-1.194.402-.256.4-.135.874.271 1.06" />
    <path d="m12.014 7.238.005.001c.919.285 1.941.974 1.939 2.188 0 2.007-2.895 4.535-7.246 4.535C3.393 13.962 0 12.352 0 9.708c0-1.385.876-2.985 2.384-4.493C4.4 3.199 6.751 2.28 7.634 3.165c.39.392.427 1.065.177 1.87-.132.405.38.182.38.182 1.63-.682 3.051-.722 3.57.02.278.397.252.951-.004 1.594-.116.293.035.34.257.407m-10.4 3.101c.172 1.738 2.46 2.936 5.109 2.674 2.647-.26 4.656-1.883 4.482-3.623-.17-1.738-2.458-2.937-5.107-2.674-2.647.263-4.656 1.883-4.484 3.623m11.681-6.484a2.06 2.06 0 0 0-1.962-.634.526.526 0 1 0 .219 1.031 1.008 1.008 0 0 1 1.17 1.296.528.528 0 0 0 1.005.325 2.06 2.06 0 0 0-.432-2.018" />
  </svg>
);

export const CHANNEL_TYPES = [
  { value: 'twitter', label: 'Twitter', icon: <TwitterOutlined />, color: '#1DA1F2' },
  { value: 'youtube', label: 'YouTube', icon: <YoutubeOutlined />, color: '#FF0000', disabled: false },
  { value: 'podcast', label: 'Podcast', icon: <CustomerServiceOutlined />, color: '#8B5CF6', disabled: false },
  { value: 'zhishixingqiu', label: '知识星球', icon: <StarOutlined />, color: '#FF6B00', disabled: false },
  { value: 'substack', label: 'Substack', icon: <ReadOutlined />, color: '#FF6719', disabled: false },
  { value: 'weibo', label: '微博', icon: <WeiboIcon />, color: '#E6162D', disabled: false },
  { value: 'rss', label: 'RSS Feed', icon: <GlobalOutlined />, color: '#FF8A00', disabled: true },
];

/**
 * EODHD 可订阅新闻源列表。
 * 前端 "Add Feed" 弹窗中，每个 source 作为独立的可选 tile 展示，
 * 选中后以 channel_type='news'、channel_identifier=<source name> 创建订阅。
 */
export interface NewsSourceConfig {
  identifier: string;
  label: string;
  color: string;
  /** Hostname (reference only; not used for runtime icon loading). */
  iconDomain: string;
  /**
   * Static asset under `public/feed-news-icons/` (served with the app, no external URL).
   * Replace with brand favicons as needed; use `placeholder.svg` until then.
   */
  iconLocalFile: string;
  /** EODHD 暂不提供：灰显 + Coming soon，且不可订阅 */
  disabled?: boolean;
}

export const NEWS_SOURCES: NewsSourceConfig[] = [
  { identifier: 'Yahoo Finance', label: 'Yahoo Finance', color: '#6001D2', iconDomain: 'finance.yahoo.com', iconLocalFile: 'yahoo-finance.ico' },
  { identifier: 'Seeking Alpha', label: 'Seeking Alpha', color: '#F57C00', iconDomain: 'seekingalpha.com', iconLocalFile: 'seeking-alpha.ico' },
  { identifier: 'CNBC', label: 'CNBC', color: '#005594', iconDomain: 'cnbc.com', iconLocalFile: 'cnbc.ico' },
  { identifier: 'Nasdaq', label: 'Nasdaq', color: '#0096D6', iconDomain: 'nasdaq.com', iconLocalFile: 'nasdaq.ico' },
  { identifier: 'Fxstreet', label: 'FXStreet', color: '#1B5E20', iconDomain: 'fxstreet.com', iconLocalFile: 'fxstreet.ico' },
  { identifier: 'Globenewswire', label: 'GlobeNewsWire', color: '#2196F3', iconDomain: 'globenewswire.com', iconLocalFile: 'globenewswire.ico' },
  { identifier: 'U.Today', label: 'U.Today', color: '#FF5722', iconDomain: 'u.today', iconLocalFile: 'u-today.ico' },
  { identifier: 'CCTV', label: 'CCTV 新闻联播', color: '#CC0000', iconDomain: 'tv.cctv.com', iconLocalFile: 'cctv.ico' },
  { identifier: 'Actionforex', label: 'ActionForex', color: '#00897B', iconDomain: 'actionforex.com', iconLocalFile: 'actionforex.ico', disabled: true },
  { identifier: 'Dailyhodl', label: 'Daily Hodl', color: '#FFC107', iconDomain: 'dailyhodl.com', iconLocalFile: 'dailyhodl.ico', disabled: true },
  { identifier: 'Thecurrencyanalytics', label: 'Currency Analytics', color: '#7B1FA2', iconDomain: 'thecurrencyanalytics.com', iconLocalFile: 'thecurrencyanalytics.ico', disabled: true },
  { identifier: 'Bloomberg', label: 'Bloomberg', color: '#1A1A1A', iconDomain: 'bloomberg.com', iconLocalFile: 'bloomberg.ico', disabled: true },
  { identifier: 'Reuters', label: 'Reuters', color: '#FF6600', iconDomain: 'reuters.com', iconLocalFile: 'reuters.ico', disabled: true },
  { identifier: 'MarketWatch', label: 'MarketWatch', color: '#00A651', iconDomain: 'marketwatch.com', iconLocalFile: 'marketwatch.ico', disabled: true },
  { identifier: 'Wall Street Journal', label: 'WSJ', color: '#0274B6', iconDomain: 'wsj.com', iconLocalFile: 'wsj.ico', disabled: true },
  { identifier: 'Financial Times', label: 'Financial Times', color: '#265761', iconDomain: 'ft.com', iconLocalFile: 'ft.ico', disabled: true },
  { identifier: 'Forbes', label: 'Forbes', color: '#B10000', iconDomain: 'forbes.com', iconLocalFile: 'forbes.ico', disabled: true },
  { identifier: 'Business Insider', label: 'Business Insider', color: '#0066CC', iconDomain: 'businessinsider.com', iconLocalFile: 'business-insider.ico', disabled: true },
];

const FEED_NEWS_ICONS_BASE = `${(process.env.PUBLIC_URL || '').replace(/\/$/, '')}/feed-news-icons`;

/** Local static icon URL for a news source (served from `public/feed-news-icons/`, no external fetch). */
export function getNewsSourceFaviconUrl(identifier: string): string | undefined {
  const source = NEWS_SOURCES.find(
    (s) => s.identifier.toLowerCase() === identifier.toLowerCase()
  );
  if (!source?.iconLocalFile) return undefined;
  return `${FEED_NEWS_ICONS_BASE}/${encodeURIComponent(source.iconLocalFile)}`;
}

/** My Feeds 侧栏：与 Sources 文件类型栏一致的频道类型筛选 */
export type FeedChannelTypeFilterKey = 'all' | 'twitter' | 'youtube' | 'podcast' | 'news' | 'zhishixingqiu' | 'substack' | 'weibo';

export const FEED_CHANNEL_TYPE_FILTER_TABS: {
  key: FeedChannelTypeFilterKey;
  label: string;
  icon: React.ReactElement;
}[] = [
  { key: 'all', label: 'All types', icon: <AppstoreOutlined /> },
  { key: 'twitter', label: 'Twitter', icon: <TwitterOutlined /> },
  { key: 'youtube', label: 'YouTube', icon: <YoutubeOutlined /> },
  { key: 'podcast', label: 'Podcast', icon: <CustomerServiceOutlined /> },
  { key: 'news', label: 'News', icon: <ReadOutlined /> },
  { key: 'zhishixingqiu', label: '知识星球', icon: <StarOutlined /> },
  { key: 'substack', label: 'Substack', icon: <ReadOutlined /> },
  { key: 'weibo', label: '微博', icon: <WeiboIcon /> },
];

export function matchesFeedChannelTypeFilter(
  channelType: string,
  filter: FeedChannelTypeFilterKey
): boolean {
  if (filter === 'all') return true;
  return channelType === filter;
}

export function getChannelIcon(channelType: string) {
  const config = CHANNEL_TYPES.find((c) => c.value === channelType);
  if (config) return config.icon;
  if (channelType === 'news') return <ReadOutlined />;
  if (channelType === 'zhishixingqiu') return <StarOutlined />;
  if (channelType === 'substack') return <ReadOutlined />;
  if (channelType === 'podcast') return <CustomerServiceOutlined />;
  return <GlobalOutlined />;
}

/** Avatar 背景色，与 CHANNEL_TYPES 品牌色一致 */
export function getChannelColor(channelType: string): string {
  const config = CHANNEL_TYPES.find((c) => c.value === channelType);
  if (config) return config.color;
  if (channelType === 'news') return '#8c8c8c';
  if (channelType === 'podcast') return '#8B5CF6';
  return '#8c8c8c';
}

/**
 * 根据 news source identifier 获取颜色。
 * 用于订阅列表 Avatar 的 background-color。
 */
export function getNewsSourceColor(identifier: string): string {
  const source = NEWS_SOURCES.find(
    (s) => s.identifier.toLowerCase() === identifier.toLowerCase()
  );
  return source?.color || '#8c8c8c';
}

/**
 * 根据 news source identifier 获取显示名称。
 */
export function getNewsSourceLabel(identifier: string): string {
  const source = NEWS_SOURCES.find(
    (s) => s.identifier.toLowerCase() === identifier.toLowerCase()
  );
  return source?.label || identifier;
}
