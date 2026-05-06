/**
 * YouTube Feed 原始内容块 + 独立 Transcript 面板.
 *
 * YoutubeFeedOriginalBlock:
 *   · YouTube embed iframe (从 contentId 或解析 URL)
 *   · 视频标题 / 描述 (超长时折叠)
 *   · Transcript 区 (可选: showTranscript=true 且 hasTranscript 时懒加载纯文本)
 *
 * YoutubeFeedTranscriptPanel:
 *   · 仅字幕区 (Daily 抽屉「Transcript」Tab 与 Original 拆分)
 *
 * 使用方:
 *   · FeedArticleBody      (Feeds: Original Tab 仅 embed+描述; Transcript Tab 用 YoutubeFeedTranscriptPanel)
 *   · DailyOriginalView    (Daily 抽屉 Original Tab, showTranscript=false 时无字幕)
 *   · DailyPreviewDrawer   (Transcript Tab → YoutubeFeedTranscriptPanel)
 */

import React from 'react';
import { Button, Space, Spin, Typography } from 'antd';
import { AlignLeftOutlined, LinkOutlined } from '@ant-design/icons';
import { subscriptionsAPI } from '../../services/subscriptions';

const { Text } = Typography;

const DESCRIPTION_PEEK_CHARS = 300;

function extractYouTubeId(url: string | null | undefined): string | null {
  if (!url) return null;
  const m = url.match(/(?:v=|youtu\.be\/|embed\/)([a-zA-Z0-9_-]{11})/);
  return m ? m[1] : null;
}

export type YoutubeFeedChromeColors = {
  text: string;
  textSecondary: string;
  textMuted: string;
  divider: string;
  link: string;
};

export interface YoutubeFeedTranscriptPanelProps {
  contentDbId: number;
  hasTranscript: boolean;
  themeMode: 'light' | 'dark';
  colors: YoutubeFeedChromeColors;
  /** When true and there is no transcript, do not show the "No transcript available" line. */
  suppressEmptyHint?: boolean;
}

/** 仅展示 YouTube Feed 字幕（懒加载 subscription_content.transcript） */
export const YoutubeFeedTranscriptPanel: React.FC<YoutubeFeedTranscriptPanelProps> = ({
  contentDbId,
  hasTranscript,
  themeMode,
  colors,
  suppressEmptyHint = false,
}) => {
  const [transcriptPlain, setTranscriptPlain] = React.useState<string | null>(null);
  const [transcriptLoading, setTranscriptLoading] = React.useState(false);
  const [transcriptError, setTranscriptError] = React.useState<string | null>(null);

  React.useEffect(() => {
    if (!hasTranscript) {
      setTranscriptPlain(null);
      setTranscriptError(null);
      return;
    }
    let cancelled = false;
    setTranscriptLoading(true);
    setTranscriptPlain(null);
    setTranscriptError(null);
    subscriptionsAPI.getSubscriptionContentTranscript(contentDbId).then((res) => {
      if (cancelled) return;
      setTranscriptLoading(false);
      if (!res.success || !res.data) {
        setTranscriptError(res.error ?? 'Failed to load transcript');
        return;
      }
      const text = (res.data.transcript ?? '').trim();
      if (text) {
        setTranscriptPlain(text);
      } else {
        setTranscriptError(res.error ?? 'Failed to load transcript');
      }
    });
    return () => {
      cancelled = true;
    };
  }, [contentDbId, hasTranscript]);

  return (
    <div>
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 6,
          marginBottom: 6,
        }}
      >
        <AlignLeftOutlined style={{ color: colors.textMuted, fontSize: 12 }} />
        <Text
          style={{
            fontSize: 11,
            letterSpacing: 0.5,
            textTransform: 'uppercase',
            color: colors.textMuted,
          }}
        >
          Transcript
        </Text>
      </div>

      {!hasTranscript && !suppressEmptyHint && (
        <Text type="secondary" style={{ fontSize: 12 }}>
          <AlignLeftOutlined style={{ marginRight: 4 }} />
          No transcript available for this video.
        </Text>
      )}

      {hasTranscript && transcriptLoading && (
        <div style={{ textAlign: 'center', padding: '12px 0' }}>
          <Spin size="small" />
        </div>
      )}

      {hasTranscript && !transcriptLoading && transcriptError && (
        <Text type="secondary" style={{ fontSize: 12 }}>
          {transcriptError}
        </Text>
      )}

      {hasTranscript && !transcriptLoading && transcriptPlain && (
        <div
          style={{
            background:
              themeMode === 'dark' ? 'rgba(255,255,255,0.03)' : '#fafafa',
            border: `1px solid ${colors.divider}`,
            borderRadius: 8,
            padding: '12px 16px',
            fontSize: 13,
            lineHeight: 1.7,
            color: colors.text,
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word',
          }}
        >
          {transcriptPlain}
        </div>
      )}
    </div>
  );
};

export interface YoutubeFeedOriginalBlockProps {
  /** YouTube video ID (e.g. "dQw4w9WgXcQ"). Primary source for embed. */
  contentId?: string | null;
  /** YouTube URL, used to derive video ID when contentId is absent. */
  url?: string;
  /** Video title shown below embed. */
  title?: string | null;
  /** Optional heading override (e.g. trimmed title). Defaults to title. */
  headingTitle?: string | null;
  /** Video description / stored content body. */
  description?: string;
  /** Database row ID — used for transcript API call. */
  contentDbId: number;
  /** Whether a transcript exists for this video. */
  hasTranscript: boolean;
  metadata?: Record<string, unknown> | null;
  themeMode: 'light' | 'dark';
  colors: YoutubeFeedChromeColors;
  /** Extra top margin for the embed (e.g. when shown below an AI summary card). */
  embedTopMargin?: number;
  /** When true, suppresses the "no transcript available" info row. */
  noTranscriptHint?: boolean;
  /** When false, transcript is not shown here (e.g. Daily drawer uses a separate Transcript tab). Default true. */
  showTranscript?: boolean;
}

const YoutubeFeedOriginalBlock: React.FC<YoutubeFeedOriginalBlockProps> = ({
  contentId,
  url,
  title,
  headingTitle,
  description = '',
  contentDbId,
  hasTranscript,
  themeMode,
  colors,
  embedTopMargin = 0,
  noTranscriptHint = false,
  showTranscript = true,
}) => {
  const videoId = contentId || extractYouTubeId(url);

  const [descExpanded, setDescExpanded] = React.useState(false);

  const descTrimmed = (description ?? '').trim();
  const descOverflow = descTrimmed.length > DESCRIPTION_PEEK_CHARS;
  const visibleDesc = descExpanded || !descOverflow
    ? descTrimmed
    : descTrimmed.slice(0, DESCRIPTION_PEEK_CHARS) + '…';

  const displayTitle = headingTitle || title;

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {/* ── YouTube embed ── */}
      {videoId ? (
        <div
          style={{
            position: 'relative',
            width: '100%',
            paddingTop: '56.25%',
            borderRadius: 12,
            overflow: 'hidden',
            backgroundColor: '#000',
            marginTop: embedTopMargin || undefined,
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
            }}
            src={`https://www.youtube.com/embed/${videoId}`}
            title={displayTitle || 'YouTube video'}
            allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
            allowFullScreen
          />
        </div>
      ) : url ? (
        <a
          href={url}
          target="_blank"
          rel="noopener noreferrer"
          style={{
            color: colors.link,
            fontSize: 13,
            display: 'inline-flex',
            alignItems: 'center',
            gap: 6,
          }}
        >
          <LinkOutlined /> Watch on YouTube
        </a>
      ) : null}

      {/* ── Title / heading ── */}
      {displayTitle && (
        <Text
          strong
          style={{ color: colors.text, fontSize: 15, lineHeight: '1.5', display: 'block' }}
        >
          {displayTitle}
        </Text>
      )}

      {/* ── Description ── */}
      {descTrimmed && (
        <div>
          <Text
            style={{
              color: colors.textSecondary,
              fontSize: 13,
              lineHeight: 1.65,
              whiteSpace: 'pre-wrap',
              wordBreak: 'break-word',
              display: 'block',
            }}
          >
            {visibleDesc}
          </Text>
          {descOverflow && (
            <Button
              type="link"
              size="small"
              style={{ padding: '2px 0', fontSize: 12 }}
              onClick={() => setDescExpanded((v) => !v)}
            >
              {descExpanded ? 'Show less' : 'Show more'}
            </Button>
          )}
        </div>
      )}

      {showTranscript && (
        <YoutubeFeedTranscriptPanel
          contentDbId={contentDbId}
          hasTranscript={hasTranscript}
          themeMode={themeMode}
          colors={colors}
          suppressEmptyHint={noTranscriptHint}
        />
      )}
    </Space>
  );
};

export default YoutubeFeedOriginalBlock;
