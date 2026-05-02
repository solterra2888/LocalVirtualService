/**
 * Shared Twitter original content renderer.
 *
 * Renders tweet body, RT / reply / quote markers, media grid, and external link.
 * Used by:
 *   · Daily  → DailyOriginalView (feed/twitter)
 *   · (future) Feeds → can replace FeedTweetContent for simpler embed needs
 *
 * Note: FeedArticleBody currently uses the richer FeedTweetContent (Twitter widgets.js
 * embed, full reply-fetch). This component covers the read-only / drawer use case.
 */

import React from 'react';
import { Space, Typography } from 'antd';
import { LinkOutlined, MessageOutlined, RetweetOutlined } from '@ant-design/icons';
import { DAILY_THEME_COLORS } from '../../../daily/dailyTheme';

const { Text } = Typography;

export interface TwitterOriginalViewProps {
  content?: string | null;
  url?: string | null;
  media_urls?: string[] | null;
  is_retweet?: boolean;
  retweeted_author?: string | null;
  is_reply?: boolean;
  in_reply_to_username?: string | null;
  is_quote?: boolean;
  quoted_content?: string | null;
  quoted_author?: string | null;
  themeMode: 'light' | 'dark';
}

const TwitterOriginalView: React.FC<TwitterOriginalViewProps> = ({
  content,
  url,
  media_urls,
  is_retweet,
  retweeted_author,
  is_reply,
  in_reply_to_username,
  is_quote,
  quoted_content,
  quoted_author,
  themeMode,
}) => {
  const colors = DAILY_THEME_COLORS[themeMode];

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {is_retweet && retweeted_author && (
        <Text type="secondary" style={{ fontSize: 12 }}>
          <RetweetOutlined /> Retweeted from{' '}
          <Text strong style={{ color: colors.text, fontSize: 12 }}>
            @{retweeted_author}
          </Text>
        </Text>
      )}
      {is_reply && in_reply_to_username && (
        <Text type="secondary" style={{ fontSize: 12 }}>
          <MessageOutlined /> Replying to{' '}
          <Text strong style={{ color: colors.text, fontSize: 12 }}>
            @{in_reply_to_username}
          </Text>
        </Text>
      )}

      {content && (
        <Text
          style={{
            color: colors.text,
            fontSize: 14,
            lineHeight: 1.7,
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word',
            display: 'block',
          }}
        >
          {content}
        </Text>
      )}

      {is_quote && quoted_content && (
        <div
          style={{
            borderLeft: `3px solid ${colors.accent}`,
            background:
              themeMode === 'dark' ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.02)',
            borderRadius: 6,
            padding: '10px 12px',
          }}
        >
          {quoted_author && (
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>
              Quoted @{quoted_author}
            </Text>
          )}
          <Text
            style={{
              color: colors.textSecondary,
              fontSize: 13,
              lineHeight: 1.6,
              whiteSpace: 'pre-wrap',
            }}
          >
            {quoted_content}
          </Text>
        </div>
      )}

      {media_urls && media_urls.length > 0 && (
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: media_urls.length === 1 ? '1fr' : '1fr 1fr',
            gap: 8,
          }}
        >
          {media_urls.slice(0, 4).map((u, idx) => (
            <a key={idx} href={u} target="_blank" rel="noopener noreferrer">
              <img
                src={u}
                alt=""
                style={{
                  width: '100%',
                  maxHeight: 280,
                  objectFit: 'cover',
                  borderRadius: 8,
                  border: `1px solid ${colors.divider}`,
                  display: 'block',
                }}
              />
            </a>
          ))}
        </div>
      )}

      {url && (
        <a
          href={url}
          target="_blank"
          rel="noopener noreferrer"
          style={{ color: colors.accent, fontSize: 13, display: 'inline-flex', alignItems: 'center', gap: 6 }}
        >
          <LinkOutlined /> View on Twitter
        </a>
      )}
    </Space>
  );
};

export default TwitterOriginalView;
