import React, { useEffect, useRef, useState } from 'react';
import {
  App,
  Avatar,
  Button,
  Divider,
  Space,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import dayjs from 'dayjs';
import {
  AlignLeftOutlined,
  CheckOutlined,
  CommentOutlined,
  EyeOutlined,
  LikeOutlined,
  LinkOutlined,
  MessageOutlined,
  ReadOutlined,
  RetweetOutlined,
  TwitterOutlined,
  YoutubeOutlined,
} from '@ant-design/icons';
import { getNewsSourceColor } from './feedChannelConfig';
import { useFileProcessing } from '../../contexts/FileProcessingContext';
import FeedTweetContent from './FeedTweetContent';
import type { FeedThemeColors } from './feedTheme';
import type { SubscriptionContent } from '../../services/subscriptions';
import { subscriptionsAPI, getTweetById } from '../../services/subscriptions';

const { Text } = Typography;

export interface FeedArticleBodyProps {
  selectedContent: SubscriptionContent | null;
  themeColors: FeedThemeColors;
  isDark: boolean;
  themeMode: 'light' | 'dark';
  /** When set, syncs transcript flag into parent (e.g. PC list selection) */
  onTranscriptReady?: (contentId: number) => void;
}

/**
 * Full article / tweet / YouTube detail column — shared by FeedReaderPage and mobile Feeds.
 */
const FeedArticleBody: React.FC<FeedArticleBodyProps> = ({
  selectedContent,
  themeColors,
  isDark,
  themeMode,
  onTranscriptReady,
}) => {
  const { message } = App.useApp();
  const tweetContainerRef = useRef<HTMLDivElement>(null);
  const [originalTweet, setOriginalTweet] = useState<SubscriptionContent | null>(null);
  const [originalTweetLoading, setOriginalTweetLoading] = useState(false);

  const [asrTaskId, setAsrTaskId] = useState<string | null>(null);
  const [asrProgress, setAsrProgress] = useState(0);
  const [asrStatus, setAsrStatus] = useState<'idle' | 'processing' | 'completed' | 'failed'>('idle');

  const { addTask, tasks } = useFileProcessing();

  useEffect(() => {
    const loadOriginalTweet = async () => {
      if (selectedContent?.is_reply && selectedContent?.in_reply_to_id) {
        setOriginalTweetLoading(true);
        try {
          const tweet = await getTweetById(selectedContent.in_reply_to_id, 1);
          setOriginalTweet(tweet);
        } catch (error) {
          console.error('Failed to load original tweet:', error);
          setOriginalTweet(null);
        } finally {
          setOriginalTweetLoading(false);
        }
      } else {
        setOriginalTweet(null);
      }
    };

    loadOriginalTweet();
  }, [selectedContent]);

  useEffect(() => {
    if (
      !originalTweetLoading &&
      !originalTweet &&
      selectedContent?.is_reply &&
      selectedContent?.in_reply_to_id &&
      tweetContainerRef.current
    ) {
      const el = tweetContainerRef.current;
      el.innerHTML = '';

      const loadTwitterWidget = () => {
        const twttr = (window as unknown as { twttr?: { widgets?: { createTweet: Function } } }).twttr;
        if (twttr?.widgets) {
          twttr.widgets.createTweet(selectedContent.in_reply_to_id, el, {
            theme: themeMode === 'dark' ? 'dark' : 'light',
            conversation: 'none',
            cards: 'visible',
          });
        } else {
          const script = document.createElement('script');
          script.src = 'https://platform.twitter.com/widgets.js';
          script.async = true;
          script.onload = () => {
            const w = (window as unknown as { twttr?: { widgets?: { createTweet: Function } } }).twttr;
            if (w?.widgets) {
              w.widgets.createTweet(selectedContent.in_reply_to_id, el, {
                theme: themeMode === 'dark' ? 'dark' : 'light',
                conversation: 'none',
                cards: 'visible',
              });
            }
          };
          document.body.appendChild(script);
        }
      };

      loadTwitterWidget();
    }
  }, [selectedContent, originalTweet, originalTweetLoading, themeMode]);

  const handleTriggerASR = async () => {
    if (!selectedContent || asrStatus === 'processing') return;

    setAsrStatus('processing');
    setAsrProgress(5);

    const result = await subscriptionsAPI.triggerYouTubeASR(selectedContent.id);
    if (!result.success || !result.task_id) {
      message.error(result.error || 'Failed to start ASR transcription');
      setAsrStatus('failed');
      return;
    }

    const taskId = result.task_id;
    const videoTitle = selectedContent.title || `YouTube: ${selectedContent.content_id || 'video'}`;
    setAsrTaskId(taskId);
    addTask(taskId, videoTitle, 'youtube');
    message.info('ASR transcription started — progress shown in the top bar');
  };

  useEffect(() => {
    if (!asrTaskId || asrStatus !== 'processing') return;

    const task = tasks.get(asrTaskId);
    if (!task) return;

    const s = task.status.status;
    const resultStatus = (task.status as { result?: { status?: string; error?: string } }).result?.status;
    const isCompleted = s === 'completed' && resultStatus !== 'failed';
    const isFailed = s === 'failed' || (s === 'completed' && resultStatus === 'failed');

    if (isCompleted) {
      setAsrStatus('completed');
      setAsrProgress(100);
      message.success('Transcription completed!');
      if (selectedContent) {
        onTranscriptReady?.(selectedContent.id);
      }
    } else if (isFailed) {
      setAsrStatus('failed');
      const errMsg =
        (task.status as { result?: { error?: string }; message?: string }).result?.error ||
        task.status.message ||
        'Transcription failed';
      message.error(`ASR failed: ${errMsg}`);
    } else {
      setAsrProgress(task.status.progress || 0);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- message API stable; avoid re-running ASR listener
  }, [tasks, asrTaskId, asrStatus, selectedContent, onTranscriptReady]);

  useEffect(() => {
    setAsrStatus('idle');
    setAsrProgress(0);
    setAsrTaskId(null);
  }, [selectedContent?.id]);

  if (!selectedContent) {
    return null;
  }

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <div>
        <Space>
          <Avatar
            icon={
              selectedContent.channel_type === 'youtube'
                ? <YoutubeOutlined />
                : selectedContent.channel_type === 'news'
                ? <ReadOutlined />
                : <TwitterOutlined />
            }
            style={{
              backgroundColor:
                selectedContent.channel_type === 'youtube'
                  ? '#FF0000'
                  : selectedContent.channel_type === 'news'
                  ? getNewsSourceColor(selectedContent.author_identifier)
                  : '#1DA1F2',
            }}
          />
          <div>
            <div>
              <Text strong style={{ color: themeColors.text }}>
                {selectedContent.author_name || selectedContent.author_identifier}
              </Text>
            </div>
            <Text type="secondary" style={{ fontSize: 12 }}>
              {selectedContent.published_at ? dayjs(selectedContent.published_at).format('MMM D, YYYY') : 'Unknown'}
            </Text>
          </div>
        </Space>
      </div>

      {selectedContent.channel_type === 'news' && selectedContent.title && (
        <Text strong style={{ color: themeColors.text, fontSize: 18, lineHeight: '1.5', display: 'block', marginTop: 8 }}>
          {selectedContent.title}
        </Text>
      )}

      {selectedContent.channel_type === 'news' && (
        <>
          <Text style={{ color: themeColors.textSecondary, fontSize: 14, whiteSpace: 'pre-wrap', lineHeight: '1.7' }}>
            {selectedContent.content}
          </Text>

          {selectedContent.metadata?.symbols && (selectedContent.metadata.symbols as string[]).length > 0 && (
            <div style={{ marginTop: 12 }}>
              <Text type="secondary" style={{ fontSize: 12, marginRight: 8 }}>Symbols:</Text>
              {(selectedContent.metadata.symbols as string[]).map((sym) => (
                <Tag key={sym} color="blue" style={{ fontSize: 11 }}>{sym}</Tag>
              ))}
            </div>
          )}

          {selectedContent.categories && selectedContent.categories.length > 0 && (
            <div style={{ marginTop: 8 }}>
              <Text type="secondary" style={{ fontSize: 12, marginRight: 8 }}>Tags:</Text>
              {selectedContent.categories.map((tag) => (
                <Tag key={tag} style={{ fontSize: 11 }}>{tag}</Tag>
              ))}
            </div>
          )}

          {selectedContent.metadata?.sentiment && (selectedContent.metadata.sentiment as { polarity?: number }).polarity != null && (
            <div style={{ marginTop: 8 }}>
              <Text type="secondary" style={{ fontSize: 12, marginRight: 8 }}>Sentiment:</Text>
              <Tag color={
                (selectedContent.metadata.sentiment as { polarity: number }).polarity > 0.1
                  ? 'green'
                  : (selectedContent.metadata.sentiment as { polarity: number }).polarity < -0.1
                  ? 'red'
                  : 'default'
              }>
                {(selectedContent.metadata.sentiment as { polarity: number }).polarity > 0.1
                  ? 'Positive'
                  : (selectedContent.metadata.sentiment as { polarity: number }).polarity < -0.1
                  ? 'Negative'
                  : 'Neutral'}
                {' '}({(selectedContent.metadata.sentiment as { polarity: number }).polarity.toFixed(2)})
              </Tag>
            </div>
          )}
        </>
      )}

      {selectedContent.channel_type === 'youtube' && selectedContent.content_id && (
        <div
          style={{
            position: 'relative',
            width: '100%',
            paddingTop: '56.25%',
            borderRadius: 12,
            overflow: 'hidden',
            backgroundColor: '#000',
          }}
        >
          <iframe
            style={{
              position: 'absolute',
              top: 0,
              left: 0,
              width: '100%',
              height: '100%',
              border: 'none',
              borderRadius: 12,
            }}
            src={`https://www.youtube.com/embed/${selectedContent.content_id}`}
            title={selectedContent.title || 'YouTube Video'}
            allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
            allowFullScreen
          />
        </div>
      )}

      {selectedContent.channel_type === 'youtube' && selectedContent.title && (
        <Text strong style={{ color: themeColors.text, fontSize: 18, lineHeight: '1.5', display: 'block' }}>
          {selectedContent.title}
        </Text>
      )}

      {selectedContent.channel_type === 'youtube' && (
        <div style={{ marginTop: 0 }}>
          <Text style={{ color: themeColors.textSecondary, fontSize: 14, whiteSpace: 'pre-wrap' }}>
            {selectedContent.content}
          </Text>
        </div>
      )}
      {selectedContent.channel_type !== 'youtube' && selectedContent.channel_type !== 'news' && (
        <div style={{ marginTop: 16 }}>
          <FeedTweetContent
            content={selectedContent}
            themeColors={themeColors}
            isDark={isDark}
            originalTweet={originalTweet}
            originalTweetLoading={originalTweetLoading}
            tweetContainerRef={tweetContainerRef}
          />
        </div>
      )}

      {selectedContent.channel_type !== 'youtube' && selectedContent.media_urls && selectedContent.media_urls.length > 0 && (
        <div>
          {selectedContent.media_urls.map((url, idx) => (
            <img key={idx} src={url} alt="" style={{ width: '100%', borderRadius: 12, marginBottom: 12 }} />
          ))}
        </div>
      )}

      {selectedContent.channel_type === 'youtube' && selectedContent.metadata && (
        <Space wrap size={16}>
          {typeof selectedContent.metadata.view_count === 'number' && (
            <Tooltip title="观看数">
              <Space size={4}>
                <EyeOutlined style={{ color: themeColors.textSecondary }} />
                <Text type="secondary" style={{ fontSize: 13 }}>
                  {selectedContent.metadata.view_count.toLocaleString()}
                </Text>
              </Space>
            </Tooltip>
          )}
          {typeof selectedContent.metadata.like_count === 'number' && (
            <Tooltip title="点赞数">
              <Space size={4}>
                <LikeOutlined style={{ color: '#eb2f96' }} />
                <Text type="secondary" style={{ fontSize: 13 }}>
                  {selectedContent.metadata.like_count.toLocaleString()}
                </Text>
              </Space>
            </Tooltip>
          )}
          {typeof selectedContent.metadata.comment_count === 'number' && (
            <Tooltip title="评论数">
              <Space size={4}>
                <CommentOutlined style={{ color: '#1890ff' }} />
                <Text type="secondary" style={{ fontSize: 13 }}>
                  {selectedContent.metadata.comment_count.toLocaleString()}
                </Text>
              </Space>
            </Tooltip>
          )}
        </Space>
      )}

      {selectedContent.channel_type !== 'youtube' && (
        <Space wrap size={16} style={{ marginTop: 16 }}>
          {selectedContent.metadata && (
            <>
              {typeof selectedContent.metadata.like_count === 'number' && (
                <Tooltip title="点赞">
                  <Space size={4}>
                    <LikeOutlined style={{ color: '#eb2f96' }} />
                    <Text type="secondary" style={{ fontSize: 13 }}>
                      {selectedContent.metadata.like_count}
                    </Text>
                  </Space>
                </Tooltip>
              )}
              {typeof selectedContent.metadata.retweet_count === 'number' && (
                <Tooltip title="转发">
                  <Space size={4}>
                    <RetweetOutlined style={{ color: '#52c41a' }} />
                    <Text type="secondary" style={{ fontSize: 13 }}>
                      {selectedContent.metadata.retweet_count}
                    </Text>
                  </Space>
                </Tooltip>
              )}
              {typeof selectedContent.metadata.reply_count === 'number' && (
                <Tooltip title="回复">
                  <Space size={4}>
                    <CommentOutlined style={{ color: '#1890ff' }} />
                    <Text type="secondary" style={{ fontSize: 13 }}>
                      {selectedContent.metadata.reply_count}
                    </Text>
                  </Space>
                </Tooltip>
              )}
              {typeof selectedContent.metadata.quote_count === 'number' && selectedContent.metadata.quote_count > 0 && (
                <Tooltip title="引用">
                  <Space size={4}>
                    <MessageOutlined style={{ color: '#1890ff' }} />
                    <Text type="secondary" style={{ fontSize: 13 }}>
                      {selectedContent.metadata.quote_count}
                    </Text>
                  </Space>
                </Tooltip>
              )}
            </>
          )}
        </Space>
      )}

      {selectedContent.url && (
        <div style={{ marginTop: 8 }}>
          <a
            href={selectedContent.url}
            target="_blank"
            rel="noopener noreferrer"
            style={{ color: themeColors.selectedBorder }}
          >
            <Space>
              <LinkOutlined />
              {selectedContent.channel_type === 'youtube'
                ? 'Watch on YouTube'
                : selectedContent.channel_type === 'news'
                ? 'Read Original Article'
                : 'View on Twitter'}
            </Space>
          </a>
        </div>
      )}

      {selectedContent.channel_type === 'youtube' && (
        <>
          <Divider style={{ borderColor: themeColors.cardBorder, margin: '8px 0' }} />
          {selectedContent.has_transcript ? (
            <div
              style={{
                borderRadius: 8,
                padding: '10px 16px',
                display: 'flex',
                alignItems: 'center',
                gap: 8,
                backgroundColor: isDark ? 'rgba(82, 196, 26, 0.08)' : '#f6ffed',
                border: `1px solid ${isDark ? 'rgba(82, 196, 26, 0.25)' : '#b7eb8f'}`,
              }}
            >
              <CheckOutlined style={{ color: '#52c41a', fontSize: 14 }} />
              <Text style={{ color: themeColors.text, fontSize: 13 }}>
                Transcript available
              </Text>
            </div>
          ) : (
            <div
              style={{
                border: `1px dashed ${themeColors.cardBorder}`,
                borderRadius: 8,
                padding: '12px 16px',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: 12,
                flexWrap: 'wrap',
              }}
            >
              <Space>
                <AlignLeftOutlined style={{ color: themeColors.textSecondary }} />
                <Text style={{ color: themeColors.textSecondary, fontSize: 13 }}>
                  {asrStatus === 'processing'
                    ? `Transcribing… ${asrProgress}%`
                    : asrStatus === 'completed'
                    ? 'Transcription complete — please refresh'
                    : asrStatus === 'failed'
                    ? 'Transcription failed'
                    : 'No transcript available'}
                </Text>
                {asrStatus === 'failed' && (
                  <Tag color="red" style={{ fontSize: 11 }}>
                    Failed
                  </Tag>
                )}
              </Space>
              {asrStatus !== 'completed' && (
                <Tooltip title="Use Fun-ASR to generate transcript (costs ASR tokens)">
                  <Button
                    size="small"
                    type="primary"
                    loading={asrStatus === 'processing'}
                    disabled={asrStatus === 'processing'}
                    onClick={handleTriggerASR}
                    style={{ fontSize: 12, borderRadius: 6 }}
                  >
                    {asrStatus === 'failed' ? 'Retry ASR' : 'AI Transcribe'}
                  </Button>
                </Tooltip>
              )}
            </div>
          )}
        </>
      )}
    </Space>
  );
};

export default FeedArticleBody;
