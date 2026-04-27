/**
 * Daily 抽屉预览 — 原文区分发器.
 *
 * 抽屉里 "上半部 = 通用 (标题/作者/AI Summary/标签)" + "下半部 = 类型专属原文区".
 * 这个文件只负责"下半部分", 按 source_subtype 路由到 9 种渲染器:
 *
 *   document   → DocumentOriginalView   (PDF / Word / Excel / CSV: PDF iframe + 解析 Markdown)
 *   image      → ImageOriginalView      (img 内嵌 + OCR Markdown)
 *   audio      → AudioOriginalView      (audio 播放器 + ASR 文本浏览)
 *   youtube    → YoutubeUploadOriginalView (YouTube embed + ASR 文本)
 *   web        → WebOriginalView        (解析 Markdown + Open original)
 *   text       → TextNoteOriginalView   (笔记 Markdown)
 *   twitter    → TwitterOriginalView    (推文文本 + media + 转推/引用 / View on Twitter)
 *   news       → NewsOriginalView       (纯文本 + Read original)
 *   youtube_feed → YoutubeFeedOriginalView (YouTube embed + 描述 + Load transcript)
 *
 * 资源 (image/audio/pdf 的 file_url + audio/youtube 的 ASR JSON) 通过
 *   filesAPI.getOriginalFileInfo(file_id)
 * 在抽屉打开时按需懒加载 (与现 Sources 详情页同口径). YouTube transcript
 * 同样懒加载 subscriptionsAPI.getSubscriptionContentTranscript(content_id).
 *
 * 这个组件 **完全只读**, 不复用 FeedArticleBody 里的 Re-tag / ASR Trigger 等
 * 写入操作; 用户若要编辑 / 触发更深功能, 走顶栏的 "Open in Sources/Feeds".
 */

import React from 'react';
import { App, Button, Empty, Space, Spin, Tag, Tooltip, Typography } from 'antd';
import {
  DownloadOutlined,
  FileTextOutlined,
  LinkOutlined,
  MessageOutlined,
  RetweetOutlined,
} from '@ant-design/icons';
import MarkdownRenderer from '../MarkdownRenderer';
import { filesAPI } from '../../services/files';
import { subscriptionsAPI } from '../../services/subscriptions';
import type {
  DailyItemDetail,
  DailyItemDetailFeed,
  DailyItemDetailUpload,
  DailySourceSubtype,
} from '../../services/daily';
import {
  DAILY_THEME_COLORS,
  type DailyThemeColors,
} from './dailyTheme';
import {
  extractYouTubeId,
  flattenAsr,
  formatTimestampMs,
  type FlatTranscriptLine,
} from './dailyPreviewUtils';

const { Text } = Typography;

/** 一次预览里 ASR / transcript 默认折叠的最大行数, 多余的让用户点击展开. */
const TRANSCRIPT_PEEK_LINES = 30;

interface DailyOriginalViewProps {
  detail: DailyItemDetail;
  themeMode: 'light' | 'dark';
}

/** 顶层分发器: 按 source_subtype 路由到具体渲染器 */
const DailyOriginalView: React.FC<DailyOriginalViewProps> = ({ detail, themeMode }) => {
  const colors = DAILY_THEME_COLORS[themeMode];
  const subtype: DailySourceSubtype = detail.source_subtype;

  if (detail.source === 'upload' && detail.upload) {
    switch (subtype) {
      case 'document':
        return <DocumentOriginalView upload={detail.upload} colors={colors} themeMode={themeMode} />;
      case 'image':
        return <ImageOriginalView upload={detail.upload} colors={colors} themeMode={themeMode} />;
      case 'audio':
        return <AudioOriginalView upload={detail.upload} colors={colors} themeMode={themeMode} />;
      case 'youtube':
        return (
          <YoutubeUploadOriginalView upload={detail.upload} colors={colors} themeMode={themeMode} />
        );
      case 'web':
        return <WebOriginalView upload={detail.upload} colors={colors} themeMode={themeMode} />;
      case 'text':
        return <TextNoteOriginalView upload={detail.upload} colors={colors} themeMode={themeMode} />;
    }
  }

  if (detail.source === 'feed' && detail.feed) {
    switch (subtype) {
      case 'twitter':
        return <TwitterOriginalView feed={detail.feed} colors={colors} themeMode={themeMode} />;
      case 'news':
        return <NewsOriginalView feed={detail.feed} colors={colors} themeMode={themeMode} />;
      case 'youtube_feed':
        return (
          <YoutubeFeedOriginalView feed={detail.feed} colors={colors} themeMode={themeMode} />
        );
    }
  }

  return (
    <Empty
      description={
        <Text style={{ color: colors.textMuted }}>No original content available</Text>
      }
      style={{ padding: '40px 0' }}
    />
  );
};

export default DailyOriginalView;

// ─── 共用小组件 ────────────────────────────────────────────────────

/** 段落标题 (例: Original · Markdown / Transcript / OCR …) */
const SectionLabel: React.FC<{ children: React.ReactNode; colors: DailyThemeColors }> = ({
  children,
  colors,
}) => (
  <Text
    type="secondary"
    style={{
      fontSize: 11,
      letterSpacing: 0.6,
      textTransform: 'uppercase',
      color: colors.textMuted,
      display: 'block',
      marginBottom: 6,
    }}
  >
    {children}
  </Text>
);

/** Markdown 内容卡片化容器 (浅灰底 + 圆角, 与抽屉外观一致) */
const MarkdownPanel: React.FC<{
  content: string;
  themeMode: 'light' | 'dark';
  colors: DailyThemeColors;
}> = ({ content, themeMode, colors }) => (
  <div
    style={{
      background: themeMode === 'dark' ? 'rgba(255,255,255,0.03)' : '#fafafa',
      border: `1px solid ${colors.divider}`,
      borderRadius: 8,
      padding: '12px 16px',
    }}
  >
    <MarkdownRenderer content={content} variant={themeMode} />
  </div>
);

/** 外链跳转条 (Read on Twitter / Open original page / Watch on YouTube …) */
const ExternalLinkRow: React.FC<{
  href: string;
  label: string;
  colors: DailyThemeColors;
}> = ({ href, label, colors }) => (
  <a
    href={href}
    target="_blank"
    rel="noopener noreferrer"
    style={{
      color: colors.accent,
      fontSize: 13,
      display: 'inline-flex',
      alignItems: 'center',
      gap: 6,
    }}
  >
    <LinkOutlined /> {label}
  </a>
);

// ─── Upload: hooks for /files/original lazy-load ───────────────────

interface OriginalFile {
  file_url: string;
  file_type: string;
  filename: string;
  asr_with_diarization?: any;
}

/** 抽屉里需要 file_url / asr 的子类型, 共享同一个懒加载 hook. */
function useOriginalFileInfo(fileId: number | null | undefined, enabled: boolean) {
  const [data, setData] = React.useState<OriginalFile | null>(null);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  React.useEffect(() => {
    if (!enabled || !fileId) {
      setData(null);
      setError(null);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);
    (async () => {
      try {
        const r = await filesAPI.getOriginalFileInfo(fileId);
        if (!cancelled) setData(r as OriginalFile);
      } catch (e: any) {
        if (!cancelled) setError(e?.response?.data?.detail || e?.message || 'Failed to load original');
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [fileId, enabled]);

  return { data, loading, error };
}

// ─── Upload renderers ──────────────────────────────────────────────

interface UploadProps {
  upload: DailyItemDetailUpload;
  colors: DailyThemeColors;
  themeMode: 'light' | 'dark';
}

/** document: PDF/Word/Excel/CSV — 解析 Markdown + 下载按钮 */
const DocumentOriginalView: React.FC<UploadProps> = ({ upload, colors, themeMode }) => {
  // PDF 二进制预览 iframe 体验在 OSS 默认域名下不稳定 (Sources 那边踩过坑),
  // 这里抽屉只做 "看摘要 + 解析 Markdown" 轻量预览;
  // 用户要看原始 PDF 走 "Open in Sources" 即可.
  const { data, loading } = useOriginalFileInfo(upload.file_id, true);
  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      <div
        style={{
          display: 'flex',
          gap: 12,
          alignItems: 'center',
          justifyContent: 'space-between',
          flexWrap: 'wrap',
        }}
      >
        <Space size={8} align="center">
          <FileTextOutlined style={{ color: colors.textMuted }} />
          <Text style={{ color: colors.text, fontSize: 13 }}>
            {upload.display_name || upload.filename || 'Document'}
          </Text>
          {upload.file_type_raw && (
            <Tag style={{ fontSize: 10, marginInlineEnd: 0 }}>
              {upload.file_type_raw.toUpperCase()}
            </Tag>
          )}
        </Space>
        {data?.file_url && (
          <Tooltip title="Open / download the original file">
            <Button
              size="small"
              icon={<DownloadOutlined />}
              href={data.file_url}
              target="_blank"
              rel="noopener noreferrer"
            >
              Open file
            </Button>
          </Tooltip>
        )}
        {!data && loading && <Spin size="small" />}
      </div>

      {upload.content ? (
        <>
          <SectionLabel colors={colors}>Parsed content</SectionLabel>
          <MarkdownPanel content={upload.content} themeMode={themeMode} colors={colors} />
        </>
      ) : (
        <Text type="secondary" style={{ fontSize: 12 }}>
          No parsed content yet — open the file in Sources to view the original.
        </Text>
      )}
    </Space>
  );
};

/** image: 图片预览 + OCR */
const ImageOriginalView: React.FC<UploadProps> = ({ upload, colors, themeMode }) => {
  const { data, loading } = useOriginalFileInfo(upload.file_id, true);
  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {loading && !data ? (
        <div style={{ textAlign: 'center', padding: '40px 0' }}>
          <Spin />
        </div>
      ) : data?.file_url ? (
        <a href={data.file_url} target="_blank" rel="noopener noreferrer">
          <img
            src={data.file_url}
            alt={upload.display_name || upload.filename || 'image'}
            style={{
              maxWidth: '100%',
              maxHeight: 480,
              display: 'block',
              margin: '0 auto',
              borderRadius: 8,
              border: `1px solid ${colors.divider}`,
              objectFit: 'contain',
              background: themeMode === 'dark' ? '#0f1626' : '#fafafa',
            }}
          />
        </a>
      ) : (
        <Empty description="Image not available" />
      )}

      {upload.content && (
        <>
          <SectionLabel colors={colors}>OCR / extracted text</SectionLabel>
          <MarkdownPanel content={upload.content} themeMode={themeMode} colors={colors} />
        </>
      )}
    </Space>
  );
};

/** audio: 音频播放器 + ASR diarization 文本 (折叠式) */
const AudioOriginalView: React.FC<UploadProps> = ({ upload, colors, themeMode }) => {
  const { data, loading } = useOriginalFileInfo(upload.file_id, true);
  const [expanded, setExpanded] = React.useState(false);
  const lines = React.useMemo<FlatTranscriptLine[]>(
    () => flattenAsr(data?.asr_with_diarization),
    [data?.asr_with_diarization]
  );
  const visible = expanded ? lines : lines.slice(0, TRANSCRIPT_PEEK_LINES);

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {data?.file_url && (
        <audio
          controls
          src={data.file_url}
          style={{ width: '100%', borderRadius: 8 }}
        />
      )}
      {!data && loading && (
        <div style={{ textAlign: 'center', padding: 16 }}>
          <Spin size="small" />
        </div>
      )}

      {lines.length > 0 ? (
        <>
          <SectionLabel colors={colors}>Transcript</SectionLabel>
          <div
            style={{
              background: themeMode === 'dark' ? 'rgba(255,255,255,0.03)' : '#fafafa',
              border: `1px solid ${colors.divider}`,
              borderRadius: 8,
              padding: '12px 16px',
              maxHeight: expanded ? 'none' : 360,
              overflow: 'auto',
              fontSize: 13,
              lineHeight: 1.7,
              color: colors.text,
            }}
          >
            {visible.map((l, idx) => (
              <div
                key={idx}
                style={{ display: 'flex', gap: 10, marginBottom: 4, alignItems: 'flex-start' }}
              >
                {(l.beginMs !== undefined || l.speakerId !== undefined) && (
                  <span
                    style={{
                      flexShrink: 0,
                      color: colors.textMuted,
                      fontSize: 11,
                      fontFamily:
                        'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
                      minWidth: 56,
                    }}
                  >
                    {l.speakerId !== undefined ? `S${l.speakerId} ` : ''}
                    {formatTimestampMs(l.beginMs)}
                  </span>
                )}
                <span>{l.text}</span>
              </div>
            ))}
          </div>
          {lines.length > TRANSCRIPT_PEEK_LINES && (
            <Button type="link" size="small" onClick={() => setExpanded((v) => !v)}>
              {expanded ? 'Show less' : `Show all ${lines.length} lines`}
            </Button>
          )}
        </>
      ) : upload.content ? (
        <>
          <SectionLabel colors={colors}>Transcript / Notes</SectionLabel>
          <MarkdownPanel content={upload.content} themeMode={themeMode} colors={colors} />
        </>
      ) : (
        <Text type="secondary" style={{ fontSize: 12 }}>
          No transcript available — open the file in Sources for full diarization view.
        </Text>
      )}
    </Space>
  );
};

/** youtube (upload): YouTube embed + transcript */
const YoutubeUploadOriginalView: React.FC<UploadProps> = ({ upload, colors, themeMode }) => {
  const { data, loading } = useOriginalFileInfo(upload.file_id, true);
  const videoId = React.useMemo(
    () => extractYouTubeId(upload.original_file_path) || extractYouTubeId(data?.file_url || null),
    [upload.original_file_path, data?.file_url]
  );
  const [expanded, setExpanded] = React.useState(false);
  const lines = React.useMemo<FlatTranscriptLine[]>(
    () => flattenAsr(data?.asr_with_diarization),
    [data?.asr_with_diarization]
  );
  const visible = expanded ? lines : lines.slice(0, TRANSCRIPT_PEEK_LINES);

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {videoId ? (
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
            }}
            src={`https://www.youtube.com/embed/${videoId}`}
            title={upload.display_name || 'YouTube video'}
            allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
            allowFullScreen
          />
        </div>
      ) : null}

      {upload.original_file_path && (
        <ExternalLinkRow
          href={upload.original_file_path}
          label="Watch on YouTube"
          colors={colors}
        />
      )}

      {loading && !data && <Spin size="small" />}

      {lines.length > 0 ? (
        <>
          <SectionLabel colors={colors}>Transcript</SectionLabel>
          <div
            style={{
              background: themeMode === 'dark' ? 'rgba(255,255,255,0.03)' : '#fafafa',
              border: `1px solid ${colors.divider}`,
              borderRadius: 8,
              padding: '12px 16px',
              maxHeight: expanded ? 'none' : 320,
              overflow: 'auto',
              fontSize: 13,
              lineHeight: 1.7,
              color: colors.text,
              whiteSpace: 'pre-wrap',
            }}
          >
            {visible.map((l, idx) => (
              <span key={idx}>
                {l.text}
                {idx < visible.length - 1 ? ' ' : ''}
              </span>
            ))}
          </div>
          {lines.length > TRANSCRIPT_PEEK_LINES && (
            <Button type="link" size="small" onClick={() => setExpanded((v) => !v)}>
              {expanded ? 'Show less' : `Show all ${lines.length} segments`}
            </Button>
          )}
        </>
      ) : upload.content ? (
        <>
          <SectionLabel colors={colors}>Description / Notes</SectionLabel>
          <MarkdownPanel content={upload.content} themeMode={themeMode} colors={colors} />
        </>
      ) : null}
    </Space>
  );
};

/** web (upload): 解析 Markdown + Open original */
const WebOriginalView: React.FC<UploadProps> = ({ upload, colors, themeMode }) => {
  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {upload.original_file_path && upload.original_file_path.startsWith('http') && (
        <ExternalLinkRow
          href={upload.original_file_path}
          label="Open original page"
          colors={colors}
        />
      )}
      {upload.content ? (
        <>
          <SectionLabel colors={colors}>Extracted article</SectionLabel>
          <MarkdownPanel content={upload.content} themeMode={themeMode} colors={colors} />
        </>
      ) : (
        <Text type="secondary" style={{ fontSize: 12 }}>
          No extracted content yet.
        </Text>
      )}
    </Space>
  );
};

/** text (My Notes / Thoughts): Markdown 渲染 */
const TextNoteOriginalView: React.FC<UploadProps> = ({ upload, colors, themeMode }) => {
  if (!upload.content) {
    return <Empty description="Empty note" />;
  }
  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      <SectionLabel colors={colors}>Note</SectionLabel>
      <MarkdownPanel content={upload.content} themeMode={themeMode} colors={colors} />
    </Space>
  );
};

// ─── Feed renderers ────────────────────────────────────────────────

interface FeedProps {
  feed: DailyItemDetailFeed;
  colors: DailyThemeColors;
  themeMode: 'light' | 'dark';
}

/** twitter: 推文文本 + media + 转推/引用/回复 marker */
const TwitterOriginalView: React.FC<FeedProps> = ({ feed, colors, themeMode }) => {
  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {feed.is_retweet && feed.retweeted_author && (
        <Text type="secondary" style={{ fontSize: 12 }}>
          <RetweetOutlined /> Retweeted from{' '}
          <Text strong style={{ color: colors.text, fontSize: 12 }}>
            @{feed.retweeted_author}
          </Text>
        </Text>
      )}
      {feed.is_reply && feed.in_reply_to_username && (
        <Text type="secondary" style={{ fontSize: 12 }}>
          <MessageOutlined /> Replying to{' '}
          <Text strong style={{ color: colors.text, fontSize: 12 }}>
            @{feed.in_reply_to_username}
          </Text>
        </Text>
      )}

      {feed.content && (
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
          {feed.content}
        </Text>
      )}

      {feed.is_quote && feed.quoted_content && (
        <div
          style={{
            borderLeft: `3px solid ${colors.accent}`,
            paddingLeft: 12,
            background:
              themeMode === 'dark' ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.02)',
            borderRadius: 6,
            padding: '10px 12px',
          }}
        >
          {feed.quoted_author && (
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>
              Quoted @{feed.quoted_author}
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
            {feed.quoted_content}
          </Text>
        </div>
      )}

      {feed.media_urls && feed.media_urls.length > 0 && (
        <div
          style={{
            display: 'grid',
            gridTemplateColumns: feed.media_urls.length === 1 ? '1fr' : '1fr 1fr',
            gap: 8,
          }}
        >
          {feed.media_urls.slice(0, 4).map((url, idx) => (
            <a key={idx} href={url} target="_blank" rel="noopener noreferrer">
              <img
                src={url}
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

      {feed.url && <ExternalLinkRow href={feed.url} label="View on Twitter" colors={colors} />}
    </Space>
  );
};

/** news: 纯文本正文 + Tickers / Categories / Sentiment + Read original */
const NewsOriginalView: React.FC<FeedProps> = ({ feed, colors }) => {
  const symbols: string[] = Array.isArray(feed.metadata?.symbols)
    ? (feed.metadata!.symbols as string[])
    : [];
  const sentiment = feed.metadata?.sentiment as
    | { polarity?: number }
    | undefined;
  const polarity = sentiment?.polarity;
  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {feed.content ? (
        <Text
          style={{
            color: colors.text,
            fontSize: 14,
            lineHeight: 1.75,
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-word',
            display: 'block',
          }}
        >
          {feed.content}
        </Text>
      ) : (
        <Text type="secondary" style={{ fontSize: 12 }}>
          Article body not stored — read on the source.
        </Text>
      )}

      {symbols.length > 0 && (
        <div style={{ display: 'flex', gap: 6, alignItems: 'center', flexWrap: 'wrap' }}>
          <Text type="secondary" style={{ fontSize: 11 }}>
            Tickers:
          </Text>
          {symbols.map((sym) => (
            <Tag key={sym} style={{ fontSize: 11, margin: 0 }}>
              {sym}
            </Tag>
          ))}
        </div>
      )}

      {feed.categories && feed.categories.length > 0 && (
        <div style={{ display: 'flex', gap: 6, alignItems: 'center', flexWrap: 'wrap' }}>
          <Text type="secondary" style={{ fontSize: 11 }}>
            Categories:
          </Text>
          {feed.categories.map((c) => (
            <Tag key={c} style={{ fontSize: 11, margin: 0 }}>
              {c}
            </Tag>
          ))}
        </div>
      )}

      {typeof polarity === 'number' && (
        <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
          <Text type="secondary" style={{ fontSize: 11 }}>
            Sentiment:
          </Text>
          <Tag
            color={polarity > 0.1 ? 'green' : polarity < -0.1 ? 'red' : 'default'}
            style={{ fontSize: 11, margin: 0 }}
          >
            {polarity > 0.1 ? 'Positive' : polarity < -0.1 ? 'Negative' : 'Neutral'} (
            {polarity.toFixed(2)})
          </Tag>
        </div>
      )}

      {feed.url && (
        <ExternalLinkRow href={feed.url} label="Read original article" colors={colors} />
      )}
    </Space>
  );
};

/** youtube_feed: YouTube embed + 描述 + Load transcript (lazy) + Watch on YouTube */
const YoutubeFeedOriginalView: React.FC<FeedProps> = ({ feed, colors, themeMode }) => {
  const { message } = App.useApp();
  const videoId = feed.content_id || extractYouTubeId(feed.url);
  const [transcript, setTranscript] = React.useState<string | null>(null);
  const [loadingTranscript, setLoadingTranscript] = React.useState(false);
  const [transcriptLoaded, setTranscriptLoaded] = React.useState(false);
  const [descExpanded, setDescExpanded] = React.useState(false);

  const handleLoadTranscript = async () => {
    if (!feed.content_db_id) return;
    setLoadingTranscript(true);
    try {
      const r = await subscriptionsAPI.getSubscriptionContentTranscript(feed.content_db_id);
      if (r.success && r.data?.transcript) {
        setTranscript(r.data.transcript);
      } else {
        message.warning(r.error || 'Transcript not available');
      }
    } catch (e: any) {
      message.error(e?.response?.data?.detail || 'Failed to load transcript');
    } finally {
      setLoadingTranscript(false);
      setTranscriptLoaded(true);
    }
  };

  const stats = feed.metadata || {};
  const viewCount = typeof stats.view_count === 'number' ? (stats.view_count as number) : null;
  const likeCount = typeof stats.like_count === 'number' ? (stats.like_count as number) : null;

  const description = feed.content || '';
  const descCollapsedMaxHeight = 96;

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {videoId ? (
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
            }}
            src={`https://www.youtube.com/embed/${videoId}`}
            title={feed.title || 'YouTube video'}
            allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
            allowFullScreen
          />
        </div>
      ) : null}

      {(viewCount !== null || likeCount !== null) && (
        <Space size={16} style={{ color: colors.textMuted, fontSize: 12 }}>
          {viewCount !== null && <span>{viewCount.toLocaleString()} views</span>}
          {likeCount !== null && <span>{likeCount.toLocaleString()} likes</span>}
        </Space>
      )}

      {description && (
        <div>
          <SectionLabel colors={colors}>Description</SectionLabel>
          <div
            style={{
              maxHeight: descExpanded ? 'none' : descCollapsedMaxHeight,
              overflow: 'hidden',
              position: 'relative',
            }}
          >
            <Text
              style={{
                color: colors.textSecondary,
                fontSize: 13,
                lineHeight: 1.6,
                whiteSpace: 'pre-wrap',
                display: 'block',
              }}
            >
              {description}
            </Text>
          </div>
          {description.length > 240 && (
            <Button
              type="link"
              size="small"
              style={{ padding: 0 }}
              onClick={() => setDescExpanded((v) => !v)}
            >
              {descExpanded ? 'Less' : 'More'}
            </Button>
          )}
        </div>
      )}

      <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
        {feed.has_transcript && !transcriptLoaded && (
          <Button
            size="small"
            icon={<FileTextOutlined />}
            loading={loadingTranscript}
            onClick={handleLoadTranscript}
          >
            Load transcript
          </Button>
        )}
        {!feed.has_transcript && (
          <Text type="secondary" style={{ fontSize: 12 }}>
            No transcript yet — generate it from the Feeds page (AI Transcribe).
          </Text>
        )}
        {feed.url && <ExternalLinkRow href={feed.url} label="Watch on YouTube" colors={colors} />}
      </div>

      {transcript && (
        <>
          <SectionLabel colors={colors}>Transcript</SectionLabel>
          <div
            style={{
              background: themeMode === 'dark' ? 'rgba(255,255,255,0.03)' : '#fafafa',
              border: `1px solid ${colors.divider}`,
              borderRadius: 8,
              padding: '12px 16px',
              maxHeight: 360,
              overflow: 'auto',
              fontSize: 13,
              lineHeight: 1.7,
              color: colors.text,
              whiteSpace: 'pre-wrap',
              fontFamily:
                'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
            }}
          >
            {transcript}
          </div>
        </>
      )}
    </Space>
  );
};
