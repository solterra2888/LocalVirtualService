/**
 * YouTube Feed 原始内容块.
 *
 * 功能:
 *   · YouTube embed iframe (从 contentId 或解析 URL)
 *   · 视频标题 / 描述 (超长时折叠)
 *   · Transcript 区 (hasTranscript=true 时自动拉取；优先展示 transcript_data 分段 + 时间戳，ASR 含说话人)
 *   · 无字幕提示 (noTranscriptHint=false 时显示)
 *
 * 使用方:
 *   · FeedArticleBody      (Feeds 右侧面板, channel_type='youtube')
 *   · DailyOriginalView    (Daily 抽屉, source_subtype='youtube_feed')
 */

import React from 'react';
import { Button, Space, Spin, Typography } from 'antd';
import { AlignLeftOutlined, LinkOutlined } from '@ant-design/icons';
import { subscriptionsAPI } from '../../services/subscriptions';
import {
  formatTranscriptTime,
  rowsFromSubscriptionTranscriptData,
  type YoutubeFeedTranscriptRow,
} from './youtubeFeedTranscriptDisplay';

const { Text } = Typography;

const DESCRIPTION_PEEK_CHARS = 300;
const TRANSCRIPT_PEEK_CHARS = 1500;
/** Collapsed transcript: max segment rows when showing structured caption / ASR data. */
const TRANSCRIPT_PEEK_ROWS = 48;

function extractYouTubeId(url: string | null | undefined): string | null {
  if (!url) return null;
  const m = url.match(/(?:v=|youtu\.be\/|embed\/)([a-zA-Z0-9_-]{11})/);
  return m ? m[1] : null;
}

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
  colors: {
    text: string;
    textSecondary: string;
    textMuted: string;
    divider: string;
    link: string;
  };
  /** Extra top margin for the embed (e.g. when shown below an AI summary card). */
  embedTopMargin?: number;
  /** When true, suppresses the "no transcript available" info row. */
  noTranscriptHint?: boolean;
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
}) => {
  const videoId = contentId || extractYouTubeId(url);

  const [transcriptPlain, setTranscriptPlain] = React.useState<string | null>(null);
  const [transcriptRows, setTranscriptRows] = React.useState<YoutubeFeedTranscriptRow[] | null>(
    null
  );
  const [transcriptLoading, setTranscriptLoading] = React.useState(false);
  const [transcriptError, setTranscriptError] = React.useState<string | null>(null);
  const [transcriptExpanded, setTranscriptExpanded] = React.useState(false);
  const [descExpanded, setDescExpanded] = React.useState(false);

  React.useEffect(() => {
    if (!hasTranscript) {
      setTranscriptPlain(null);
      setTranscriptRows(null);
      setTranscriptError(null);
      return;
    }
    let cancelled = false;
    setTranscriptLoading(true);
    setTranscriptPlain(null);
    setTranscriptRows(null);
    setTranscriptError(null);
    setTranscriptExpanded(false);
    subscriptionsAPI.getSubscriptionContentTranscript(contentDbId).then((res) => {
      if (cancelled) return;
      setTranscriptLoading(false);
      if (!res.success || !res.data) {
        setTranscriptError(res.error ?? 'Failed to load transcript');
        return;
      }
      const structured = rowsFromSubscriptionTranscriptData(res.data.transcript_data);
      if (structured && structured.length > 0) {
        setTranscriptRows(structured);
        setTranscriptPlain(null);
      } else if (res.data.transcript?.trim()) {
        setTranscriptRows(null);
        setTranscriptPlain(res.data.transcript);
      } else {
        setTranscriptError(res.error ?? 'Failed to load transcript');
      }
    });
    return () => {
      cancelled = true;
    };
  }, [contentDbId, hasTranscript]);

  const descTrimmed = (description ?? '').trim();
  const descOverflow = descTrimmed.length > DESCRIPTION_PEEK_CHARS;
  const visibleDesc = descExpanded || !descOverflow
    ? descTrimmed
    : descTrimmed.slice(0, DESCRIPTION_PEEK_CHARS) + '…';

  const plainOverflow =
    (transcriptPlain?.length ?? 0) > TRANSCRIPT_PEEK_CHARS;
  const visiblePlainTranscript =
    transcriptExpanded || !plainOverflow
      ? (transcriptPlain ?? '')
      : (transcriptPlain ?? '').slice(0, TRANSCRIPT_PEEK_CHARS) + '…';

  const rowOverflow = (transcriptRows?.length ?? 0) > TRANSCRIPT_PEEK_ROWS;
  const visibleTranscriptRows =
    transcriptExpanded || !rowOverflow
      ? transcriptRows ?? []
      : (transcriptRows ?? []).slice(0, TRANSCRIPT_PEEK_ROWS);

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

      {/* ── Transcript ── */}
      {hasTranscript && (
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

          {transcriptLoading && (
            <div style={{ textAlign: 'center', padding: '12px 0' }}>
              <Spin size="small" />
            </div>
          )}

          {!transcriptLoading && transcriptError && (
            <Text type="secondary" style={{ fontSize: 12 }}>
              {transcriptError}
            </Text>
          )}

          {!transcriptLoading && transcriptRows && transcriptRows.length > 0 && (
            <>
              <div
                style={{
                  background:
                    themeMode === 'dark' ? 'rgba(255,255,255,0.03)' : '#fafafa',
                  border: `1px solid ${colors.divider}`,
                  borderRadius: 8,
                  padding: '10px 12px',
                  maxHeight: transcriptExpanded ? 'none' : 320,
                  overflowY: 'auto',
                }}
              >
                {visibleTranscriptRows.map((row, idx) => (
                  <div
                    key={row.key}
                    style={{
                      display: 'grid',
                      gridTemplateColumns: '52px 1fr',
                      columnGap: 10,
                      alignItems: 'start',
                      padding: '6px 0',
                      borderBottom:
                        idx === visibleTranscriptRows.length - 1
                          ? 'none'
                          : `1px solid ${colors.divider}`,
                      fontSize: 13,
                      lineHeight: 1.55,
                      color: colors.text,
                    }}
                  >
                    <Text
                      style={{
                        fontSize: 11,
                        fontVariantNumeric: 'tabular-nums',
                        color: colors.textMuted,
                        paddingTop: 1,
                      }}
                    >
                      {formatTranscriptTime(row.startSec)}
                    </Text>
                    <div style={{ minWidth: 0, wordBreak: 'break-word' }}>
                      {row.speakerLabel && (
                        <Text
                          strong
                          style={{
                            fontSize: 11,
                            color: colors.textSecondary,
                            display: 'block',
                            marginBottom: 2,
                          }}
                        >
                          {row.speakerLabel}
                        </Text>
                      )}
                      <span>{row.text}</span>
                    </div>
                  </div>
                ))}
              </div>
              {rowOverflow && (
                <Button
                  type="link"
                  size="small"
                  style={{ padding: '4px 0', fontSize: 12 }}
                  onClick={() => setTranscriptExpanded((v) => !v)}
                >
                  {transcriptExpanded ? 'Show less' : 'Show full transcript'}
                </Button>
              )}
            </>
          )}

          {!transcriptLoading && transcriptPlain && (
            <>
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
                  maxHeight: transcriptExpanded ? 'none' : 320,
                  overflowY: 'auto',
                }}
              >
                {visiblePlainTranscript}
              </div>
              {plainOverflow && (
                <Button
                  type="link"
                  size="small"
                  style={{ padding: '4px 0', fontSize: 12 }}
                  onClick={() => setTranscriptExpanded((v) => !v)}
                >
                  {transcriptExpanded ? 'Show less' : 'Show full transcript'}
                </Button>
              )}
            </>
          )}
        </div>
      )}

      {/* ── No transcript hint ── */}
      {!hasTranscript && !noTranscriptHint && (
        <Text type="secondary" style={{ fontSize: 12 }}>
          <AlignLeftOutlined style={{ marginRight: 4 }} />
          No transcript available for this video.
        </Text>
      )}
    </Space>
  );
};

export default YoutubeFeedOriginalBlock;
