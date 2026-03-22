import React from 'react';
import { TwitterOutlined, YoutubeOutlined, GlobalOutlined } from '@ant-design/icons';

export const CHANNEL_TYPES = [
  { value: 'twitter', label: 'Twitter', icon: <TwitterOutlined />, color: '#1DA1F2' },
  { value: 'youtube', label: 'YouTube', icon: <YoutubeOutlined />, color: '#FF0000', disabled: false },
  { value: 'rss', label: 'RSS Feed', icon: <GlobalOutlined />, color: '#orange', disabled: true },
];

export function getChannelIcon(channelType: string) {
  const config = CHANNEL_TYPES.find((c) => c.value === channelType);
  return config?.icon || <GlobalOutlined />;
}
