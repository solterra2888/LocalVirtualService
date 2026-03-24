import React from 'react';
import {
  AppstoreOutlined,
  TwitterOutlined,
  YoutubeOutlined,
  GlobalOutlined,
  ReadOutlined,
} from '@ant-design/icons';

export const CHANNEL_TYPES = [
  { value: 'twitter', label: 'Twitter', icon: <TwitterOutlined />, color: '#1DA1F2' },
  { value: 'youtube', label: 'YouTube', icon: <YoutubeOutlined />, color: '#FF0000', disabled: false },
  { value: 'rss', label: 'RSS Feed', icon: <GlobalOutlined />, color: '#orange', disabled: true },
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
  /** Hostname for brand favicon (Google s2 favicon service). */
  iconDomain: string;
  /** EODHD 暂不提供：灰显 + Coming soon，且不可订阅 */
  disabled?: boolean;
}

export const NEWS_SOURCES: NewsSourceConfig[] = [
  { identifier: 'Yahoo Finance', label: 'Yahoo Finance', color: '#6001D2', iconDomain: 'finance.yahoo.com' },
  { identifier: 'Seeking Alpha', label: 'Seeking Alpha', color: '#F57C00', iconDomain: 'seekingalpha.com' },
  { identifier: 'CNBC', label: 'CNBC', color: '#005594', iconDomain: 'cnbc.com' },
  { identifier: 'Nasdaq', label: 'Nasdaq', color: '#0096D6', iconDomain: 'nasdaq.com' },
  { identifier: 'Fxstreet', label: 'FXStreet', color: '#1B5E20', iconDomain: 'fxstreet.com' },
  { identifier: 'Globenewswire', label: 'GlobeNewsWire', color: '#2196F3', iconDomain: 'globenewswire.com' },
  { identifier: 'U.Today', label: 'U.Today', color: '#FF5722', iconDomain: 'u.today' },
  { identifier: 'Actionforex', label: 'ActionForex', color: '#00897B', iconDomain: 'actionforex.com', disabled: true },
  { identifier: 'Dailyhodl', label: 'Daily Hodl', color: '#FFC107', iconDomain: 'dailyhodl.com', disabled: true },
  { identifier: 'Thecurrencyanalytics', label: 'Currency Analytics', color: '#7B1FA2', iconDomain: 'thecurrencyanalytics.com', disabled: true },
  { identifier: 'Bloomberg', label: 'Bloomberg', color: '#1A1A1A', iconDomain: 'bloomberg.com', disabled: true },
  { identifier: 'Reuters', label: 'Reuters', color: '#FF6600', iconDomain: 'reuters.com', disabled: true },
  { identifier: 'MarketWatch', label: 'MarketWatch', color: '#00A651', iconDomain: 'marketwatch.com', disabled: true },
  { identifier: 'Wall Street Journal', label: 'WSJ', color: '#0274B6', iconDomain: 'wsj.com', disabled: true },
  { identifier: 'Financial Times', label: 'Financial Times', color: '#265761', iconDomain: 'ft.com', disabled: true },
  { identifier: 'Forbes', label: 'Forbes', color: '#B10000', iconDomain: 'forbes.com', disabled: true },
  { identifier: 'Business Insider', label: 'Business Insider', color: '#0066CC', iconDomain: 'businessinsider.com', disabled: true },
];

/** Favicon URL for a configured news source (used in picker tiles and list avatars). */
export function getNewsSourceFaviconUrl(identifier: string): string | undefined {
  const source = NEWS_SOURCES.find(
    (s) => s.identifier.toLowerCase() === identifier.toLowerCase()
  );
  if (!source?.iconDomain) return undefined;
  return `https://www.google.com/s2/favicons?domain=${encodeURIComponent(source.iconDomain)}&sz=64`;
}

/** My Feeds 侧栏：与 Sources 文件类型栏一致的频道类型筛选 */
export type FeedChannelTypeFilterKey = 'all' | 'twitter' | 'youtube' | 'news';

export const FEED_CHANNEL_TYPE_FILTER_TABS: {
  key: FeedChannelTypeFilterKey;
  label: string;
  icon: React.ReactElement;
}[] = [
  { key: 'all', label: 'All types', icon: <AppstoreOutlined /> },
  { key: 'twitter', label: 'Twitter', icon: <TwitterOutlined /> },
  { key: 'youtube', label: 'YouTube', icon: <YoutubeOutlined /> },
  { key: 'news', label: 'News', icon: <ReadOutlined /> },
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
  return <GlobalOutlined />;
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
