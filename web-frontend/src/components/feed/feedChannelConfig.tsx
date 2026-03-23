import React from 'react';
import {
  AppstoreOutlined,
  TwitterOutlined,
  YoutubeOutlined,
  GlobalOutlined,
} from '@ant-design/icons';

export const CHANNEL_TYPES = [
  { value: 'twitter', label: 'Twitter', icon: <TwitterOutlined />, color: '#1DA1F2' },
  { value: 'youtube', label: 'YouTube', icon: <YoutubeOutlined />, color: '#FF0000', disabled: false },
  { value: 'rss', label: 'RSS Feed', icon: <GlobalOutlined />, color: '#orange', disabled: true },
];

/** My Feeds 侧栏：与 Sources 文件类型栏一致的频道类型筛选 */
export type FeedChannelTypeFilterKey = 'all' | 'twitter' | 'youtube' | 'rss';

export const FEED_CHANNEL_TYPE_FILTER_TABS: {
  key: FeedChannelTypeFilterKey;
  label: string;
  icon: React.ReactElement;
}[] = [
  { key: 'all', label: 'All types', icon: <AppstoreOutlined /> },
  { key: 'twitter', label: 'Twitter', icon: <TwitterOutlined /> },
  { key: 'youtube', label: 'YouTube', icon: <YoutubeOutlined /> },
  { key: 'rss', label: 'RSS', icon: <GlobalOutlined /> },
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
  return config?.icon || <GlobalOutlined />;
}
