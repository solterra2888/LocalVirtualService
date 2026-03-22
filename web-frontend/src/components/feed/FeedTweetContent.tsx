import React from 'react';
import { Avatar, Card, Space, Typography } from 'antd';
import {
  CommentOutlined,
  LinkOutlined,
  MessageOutlined,
  RetweetOutlined,
} from '@ant-design/icons';
import type { SubscriptionContent } from '../../services/subscriptions';
import type { FeedThemeColors } from './feedTheme';

const { Text, Paragraph } = Typography;

export interface FeedTweetContentProps {
  content: SubscriptionContent;
  themeColors: FeedThemeColors;
  isDark: boolean;
  originalTweet: SubscriptionContent | null;
  originalTweetLoading: boolean;
  tweetContainerRef: React.RefObject<HTMLDivElement>;
}

/**
 * Twitter body: reply / retweet / quote / plain — shared by PC Feed Reader and mobile Feeds.
 */
const FeedTweetContent: React.FC<FeedTweetContentProps> = ({
  content,
  themeColors,
  isDark,
  originalTweet,
  originalTweetLoading,
  tweetContainerRef,
}) => {
  return (
    <div>
      {content.is_reply && content.in_reply_to_username && (
        <div style={{ marginBottom: 16 }}>
          <Paragraph style={{ color: themeColors.text, marginBottom: 12 }}>{content.content}</Paragraph>

          <Card
            size="small"
            style={{
              marginBottom: 12,
              backgroundColor: themeColors.hoverBg,
              borderColor: themeColors.cardBorder,
              borderLeft: `3px solid #657786`,
              borderRadius: 12,
              border: `1px solid ${themeColors.cardBorder}`,
              boxShadow: isDark ? '0 4px 16px rgba(0,0,0,0.2)' : '0 2px 8px rgba(0,0,0,0.06)',
            }}
            bodyStyle={{ padding: 12 }}
          >
            <Space direction="vertical" size={12} style={{ width: '100%' }}>
              <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                <Space>
                  <CommentOutlined style={{ color: '#657786' }} />
                  <Text type="secondary">Replying to @{content.in_reply_to_username}</Text>
                </Space>

                {originalTweetLoading && (
                  <Space size={4}>
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      Loading...
                    </Text>
                  </Space>
                )}
              </Space>

              {content.in_reply_to_id && content.in_reply_to_username && !originalTweetLoading && (
                <div>
                  {originalTweet ? (
                    <div
                      style={{
                        backgroundColor: themeColors.cardBg,
                        border: `1px solid ${themeColors.cardBorder}`,
                        borderRadius: 8,
                        padding: 12,
                      }}
                    >
                      <Space direction="vertical" size={8} style={{ width: '100%' }}>
                        <Space>
                          <Avatar size="small" style={{ backgroundColor: '#1DA1F2' }}>
                            {originalTweet.author_name?.[0] || 'T'}
                          </Avatar>
                          <Text strong style={{ color: themeColors.text }}>
                            {originalTweet.author_name}
                          </Text>
                          <Text type="secondary" style={{ fontSize: 12 }}>
                            @{originalTweet.author_identifier}
                          </Text>
                        </Space>

                        <Text style={{ color: themeColors.text }}>{originalTweet.content}</Text>

                        {originalTweet.media_urls && originalTweet.media_urls.length > 0 && (
                          <div style={{ marginTop: 8 }}>
                            {originalTweet.media_urls.map((url, index) => (
                              <img
                                key={index}
                                src={url}
                                alt="media"
                                style={{
                                  maxWidth: '100%',
                                  borderRadius: 8,
                                  marginBottom: index < originalTweet.media_urls!.length - 1 ? 8 : 0,
                                }}
                              />
                            ))}
                          </div>
                        )}

                        <a
                          href={originalTweet.url}
                          target="_blank"
                          rel="noopener noreferrer"
                          style={{ fontSize: 12 }}
                        >
                          <LinkOutlined /> View on Twitter
                        </a>
                      </Space>
                    </div>
                  ) : (
                    <div
                      ref={tweetContainerRef}
                      style={{
                        backgroundColor: themeColors.cardBg,
                        border: `1px solid ${themeColors.cardBorder}`,
                        borderRadius: 8,
                        padding: 12,
                        minHeight: '200px',
                      }}
                    />
                  )}
                </div>
              )}
            </Space>
          </Card>
        </div>
      )}

      {content.is_retweet && content.retweeted_content && (
        <Card
          size="small"
          style={{
            marginBottom: 12,
            backgroundColor: themeColors.hoverBg,
            border: `1px solid ${themeColors.cardBorder}`,
            borderRadius: 12,
            boxShadow: isDark ? '0 4px 16px rgba(0,0,0,0.2)' : '0 2px 8px rgba(0,0,0,0.06)',
          }}
        >
          <Space direction="vertical" size={4} style={{ width: '100%' }}>
            <Space>
              <RetweetOutlined style={{ color: '#00ba7c' }} />
              <Text type="secondary">Retweeted from @{content.retweeted_author}</Text>
            </Space>
            <Text style={{ color: themeColors.text }}>{content.retweeted_content}</Text>
            {content.retweeted_url && (
              <a href={content.retweeted_url} target="_blank" rel="noopener noreferrer">
                <LinkOutlined /> View original tweet
              </a>
            )}
          </Space>
        </Card>
      )}

      {content.is_quote && content.quoted_content && (
        <div style={{ marginBottom: 16 }}>
          <Paragraph style={{ color: themeColors.text, marginBottom: 12 }}>{content.content}</Paragraph>

          <Card
            size="small"
            style={{
              marginBottom: 12,
              backgroundColor: themeColors.hoverBg,
              border: `1px solid ${themeColors.cardBorder}`,
              borderLeft: '3px solid #1DA1F2',
              borderRadius: 12,
              boxShadow: isDark ? '0 4px 16px rgba(0,0,0,0.2)' : '0 2px 8px rgba(0,0,0,0.06)',
            }}
          >
            <Space direction="vertical" size={8} style={{ width: '100%' }}>
              <Space>
                <MessageOutlined style={{ color: '#1DA1F2' }} />
                <Text type="secondary">Quoted from @{content.quoted_author}</Text>
              </Space>

              <div
                style={{
                  backgroundColor: themeColors.cardBg,
                  border: `1px solid ${themeColors.cardBorder}`,
                  borderRadius: 8,
                  padding: 12,
                }}
              >
                <Text style={{ color: themeColors.text }}>{content.quoted_content}</Text>
              </div>

              {content.quoted_url && (
                <a href={content.quoted_url} target="_blank" rel="noopener noreferrer" style={{ fontSize: 12 }}>
                  <LinkOutlined /> View quoted tweet
                </a>
              )}
            </Space>
          </Card>
        </div>
      )}

      {!content.is_retweet && !content.is_reply && !content.is_quote && (
        <Paragraph style={{ color: themeColors.text }}>{content.content}</Paragraph>
      )}
    </div>
  );
};

export default FeedTweetContent;
