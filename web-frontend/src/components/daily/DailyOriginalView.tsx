/**
 * Daily 抽屉预览 — 原文区分发器.
 *
 * 抽屉里 "上半部 = 通用 (标题/作者/AI Summary/标签)" + "下半部 = 类型专属原文区".
 * 这个文件只负责"下半部分", 按 source_subtype 路由到 9 种渲染器:
 *
 *   document   → DocumentOriginalView   (PDF / Word / Excel / CSV: PDF iframe + 解析 Markdown)
 *   image      → ImageOriginalView      (img 内嵌 + OCR Markdown)
 *   audio      → AudioOriginalView      (audio 播放器 + ASR 文本浏览)
 *   youtube    → YoutubeUploadOriginalView (嵌入 + 标题 + 描述; 有 Transcript Tab 时字幕在 Tab 内)
 *   web        → WebOriginalView        (解析 Markdown + Open original)
 *   text       → TextNoteOriginalView   (笔记 Markdown)
 *   twitter    → TwitterOriginalView    (推文文本 + media + 转推/引用 / View on Twitter)
 *   news       → NewsOriginalView       (纯文本 + Read original)
 *   youtube_feed → YoutubeFeedOriginalBlock (YouTube embed + 描述; 字幕见抽屉 Transcript Tab)
 *
 * 资源 (image/audio/pdf 的 file_url + audio/youtube 的 ASR JSON) 通过
 *   filesAPI.getOriginalFileInfo(file_id)
 * 在抽屉打开时按需懒加载 (与现 Sources 详情页同口径). YouTube Feed 的 transcript
 * 由 DailyPreviewDrawer 的 Transcript Tab 内 YoutubeFeedTranscriptPanel /
 * DailyYoutubeUploadTranscriptPanel 展示.
 *
 * 这个组件 **完全只读**, 不复用 FeedArticleBody 里的 Re-tag / ASR Trigger 等
 * 写入操作; 用户若要编辑 / 触发更深功能, 走顶栏的 "Open in Sources/Feeds".
 */

import React from 'react';
import { Button, Empty, Space, Spin, Tag, Tooltip, Typography } from 'antd';
import {
  AlignLeftOutlined,
  DownloadOutlined,
  FileTextOutlined,
  LinkOutlined,
} from '@ant-design/icons';
import MarkdownRenderer from '../MarkdownRenderer';
import { filesAPI } from '../../services/files';
import YoutubeFeedOriginalBlock from '../feed/YoutubeFeedOriginalBlock';
import TwitterOriginalView from '../shared/content-viewer/original-views/TwitterOriginalView';
import NewsOriginalView from '../shared/content-viewer/original-views/NewsOriginalView';
import type {
  DailyItemDetail,
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
  /** Daily 抽屉: YouTube Feed / YouTube Upload 字幕单独 Tab，不在 Original 内嵌 */
  splitYoutubeTranscriptTab?: boolean;
}

/** 顶层分发器: 按 source_subtype 路由到具体渲染器 */
const DailyOriginalView: React.FC<DailyOriginalViewProps> = ({
  detail,
  themeMode,
  splitYoutubeTranscriptTab = false,
}) => {
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
          <YoutubeUploadOriginalView
            upload={detail.upload}
            colors={colors}
            themeMode={themeMode}
            omitTranscript={splitYoutubeTranscriptTab}
          />
        );
      case 'web':
        return <WebOriginalView upload={detail.upload} colors={colors} themeMode={themeMode} />;
      case 'text':
        return <TextNoteOriginalView upload={detail.upload} colors={colors} themeMode={themeMode} />;
    }
  }

  if (detail.source === 'feed' && detail.feed) {
    const f = detail.feed;
    switch (subtype) {
      case 'twitter':
        return (
          <TwitterOriginalView
            content={f.content}
            url={f.url}
            media_urls={f.media_urls}
            is_retweet={f.is_retweet ?? undefined}
            retweeted_author={f.retweeted_author}
            is_reply={f.is_reply ?? undefined}
            in_reply_to_username={f.in_reply_to_username}
            is_quote={f.is_quote ?? undefined}
            quoted_content={f.quoted_content}
            quoted_author={f.quoted_author}
            themeMode={themeMode}
          />
        );
      case 'news':
        return (
          <NewsOriginalView
            content={f.content}
            url={f.url}
            symbols={Array.isArray(f.metadata?.symbols) ? (f.metadata!.symbols as string[]) : []}
            categories={f.categories ?? []}
            sentiment={f.metadata?.sentiment as { polarity?: number } | null}
            themeMode={themeMode}
          />
        );
      case 'youtube_feed':
        return (
          <YoutubeFeedOriginalBlock
            contentId={detail.feed.content_id}
            url={detail.feed.url ?? undefined}
            title={detail.feed.title}
            description={detail.feed.content || ''}
            contentDbId={detail.feed.content_db_id}
            hasTranscript={detail.feed.has_transcript}
            metadata={detail.feed.metadata}
            themeMode={themeMode}
            showTranscript={!splitYoutubeTranscriptTab}
            colors={{
              text: colors.text,
              textSecondary: colors.textSecondary,
              textMuted: colors.textMuted,
              divider: colors.divider,
              link: colors.accent,
            }}
          />
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

/** Daily 抽屉 Transcript Tab — Upload YouTube 的 ASR 全文（与 Upload 原文区字幕块同样式） */
export function DailyYoutubeUploadTranscriptPanel({
  upload,
  themeMode,
  colors,
}: UploadProps) {
  const { data, loading } = useOriginalFileInfo(upload.file_id, true);
  const lines = React.useMemo<FlatTranscriptLine[]>(
    () => flattenAsr(data?.asr_with_diarization),
    [data?.asr_with_diarization]
  );
  const transcriptPlain = React.useMemo(() => {
    if (lines.length === 0) return '';
    return lines.map((l) => l.text).join('\n');
  }, [lines]);

  if (loading && !data) {
    return (
      <div style={{ textAlign: 'center', padding: '12px 0' }}>
        <Spin size="small" />
      </div>
    );
  }

  if (transcriptPlain) {
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
      </div>
    );
  }

  return (
    <Text type="secondary" style={{ fontSize: 12 }}>
      No transcript available — open the file in Sources for full diarization view.
    </Text>
  );
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

/** youtube (upload): 与 Feeds `YoutubeFeedOriginalBlock` 一致的嵌入 / 标题 / 描述 / 字幕区视觉 */
const YT_UPLOAD_DESCRIPTION_PEEK = 300;

/** youtube (upload): YouTube embed + 标题 + 描述 + 可选内嵌字幕（omitTranscript 时仅前三项） */
const YoutubeUploadOriginalView: React.FC<
  UploadProps & { omitTranscript?: boolean }
> = ({ upload, colors, themeMode, omitTranscript = false }) => {
  const { data, loading } = useOriginalFileInfo(upload.file_id, true);
  const videoId = React.useMemo(
    () => extractYouTubeId(upload.original_file_path) || extractYouTubeId(data?.file_url || null),
    [upload.original_file_path, data?.file_url]
  );

  const urlForLink = (upload.original_file_path ?? '').trim() || (data?.file_url ?? '').trim() || null;

  const [descExpanded, setDescExpanded] = React.useState(false);
  const descTrimmed = (upload.content ?? '').trim();
  const descOverflow = descTrimmed.length > YT_UPLOAD_DESCRIPTION_PEEK;
  const visibleDesc =
    descExpanded || !descOverflow
      ? descTrimmed
      : descTrimmed.slice(0, YT_UPLOAD_DESCRIPTION_PEEK) + '…';

  const lines = React.useMemo<FlatTranscriptLine[]>(
    () => flattenAsr(data?.asr_with_diarization),
    [data?.asr_with_diarization]
  );

  const transcriptPlain = React.useMemo(() => {
    if (lines.length === 0) return '';
    return lines.map((l) => l.text).join('\n');
  }, [lines]);

  const displayTitle =
    (upload.display_name ?? upload.filename ?? '').trim() || null;

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
            title={displayTitle || 'YouTube video'}
            allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
            allowFullScreen
          />
        </div>
      ) : urlForLink ? (
        <a
          href={urlForLink}
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
          <LinkOutlined /> Watch on YouTube
        </a>
      ) : null}

      {loading && !data && <Spin size="small" />}

      {displayTitle && (
        <Text
          strong
          style={{ color: colors.text, fontSize: 15, lineHeight: '1.5', display: 'block' }}
        >
          {displayTitle}
        </Text>
      )}

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

      {transcriptPlain && !omitTranscript ? (
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
        </div>
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

// TwitterOriginalView and NewsOriginalView are now shared components.
// See: components/shared/content-viewer/original-views/
