/**
 * Daily 抽屉预览 — 主容器.
 *
 * 卡片点击 → 打开本抽屉, 在 Daily 页面内就地预览, 不再跳出到 Sources/Feeds.
 *
 * 布局 (自上而下):
 *   ┌──────────────────────────────────────────────────────┐
 *   │ Header: SourceBadge · ‹ Prev / Next › · Close        │
 *   │ Title (h2)                                           │
 *   │ 作者 · 时间 · ⏱ X min read                            │
 *   ├──────────────────────────────────────────────────────┤
 *   │ AI Summary block (摘要 + sector/industry + tickers)   │
 *   │   - 缺失 summary 时降级提示并直接展开下方原文区        │
 *   ├──────────────────────────────────────────────────────┤
 *   │ Original | AI Analysis | Transcript (YouTube Feed / Upload YT Link) — Segmented Tab   │
 *   │   Transcript → YoutubeFeedTranscriptPanel (feed) / DailyYoutubeUploadTranscriptPanel (upload)   │
 *   └──────────────────────────────────────────────────────┘
 *
 * 键盘:
 *   - Esc       关闭
 *   - ↑ / k     上一条
 *   - ↓ / j     下一条
 *
 * 移动端 bottom-sheet: 两侧浮动 < >（与 Mobile Feeds 全文页一致）切换上一条/下一条。
 *
 * 设计取舍详见 docs/instructions/DAILY_MODULE.md §1.
 */

import React from 'react';
import { createPortal } from 'react-dom';
import {
  Button,
  Drawer,
  Empty,
  Segmented,
  Skeleton,
  Space,
  Spin,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import {
  ArrowDownOutlined,
  ArrowUpOutlined,
  BulbOutlined,
  CloseOutlined,
  GlobalOutlined,
  StockOutlined,
  TeamOutlined,
} from '@ant-design/icons';
import {
  DailyFeedItem,
  DailyItemDetail,
  getDailyItemDetail,
} from '../../services/daily';
import SourceBadge from './SourceBadge';
import { DAILY_THEME_COLORS } from './dailyTheme';
import DailyOriginalView, { DailyYoutubeUploadTranscriptPanel } from './DailyOriginalView';
import DailyAIAnalysisSection from './DailyAIAnalysisSection';
import AISummarySection from '../shared/content-viewer/AISummarySection';
import CollapsibleOriginalSection from '../shared/content-viewer/CollapsibleOriginalSection';
import { YoutubeFeedTranscriptPanel } from '../feed/YoutubeFeedOriginalBlock';
import {
  type FeedArticleViewMode,
  adjacentFeedArticleViewMode,
} from '../feed/feedArticleViewMode';
import { formatTime as fmtTimeInTz } from '../../utils/datetime';
import { useDashboardSettings } from '../../contexts/DashboardSettingsContext';
import ReportTickerTags from '../ReportTickerTags';
import { aiTagChipStyle } from '../shared/aiTagChipStyles';
import './DailyPreviewDrawer.css';

const { Title, Text } = Typography;

export interface DailyPreviewDrawerProps {
  /** 当前预览的卡片列表条目 (列表已有), null = 关闭 */
  item: DailyFeedItem | null;
  /** 整个 Daily 列表 (用于 Prev/Next 切换); 索引按 ID 解析 */
  items: DailyFeedItem[];
  themeMode: 'light' | 'dark';
  /** 移动端走 placement="bottom", 桌面端走 placement="right" */
  placement?: 'right' | 'bottom';
  onClose: () => void;
  /** 切换上一/下一条; 由父维护 item state, 父也可以拒绝 (例: 已经到首/末) */
  onNavigate?: (next: DailyFeedItem) => void;
}

const DESKTOP_DRAWER_WIDTH = 720;

const DailyPreviewDrawer: React.FC<DailyPreviewDrawerProps> = ({
  item,
  items,
  themeMode,
  placement = 'right',
  onClose,
  onNavigate,
}) => {
  const colors = DAILY_THEME_COLORS[themeMode];
  const { userTimezone } = useDashboardSettings();

  const [detail, setDetail] = React.useState<DailyItemDetail | null>(null);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  // 视图模式: Original | AI Analysis | Transcript（YouTube Feed 与 Upload YT Link 显示第三 Tab）
  const [viewMode, setViewMode] = React.useState<FeedArticleViewMode>('original');

  const showTranscriptTab =
    item?.source_subtype === 'youtube_feed' ||
    (item?.source === 'upload' && item?.source_subtype === 'youtube');
  const analysisTabEnabled =
    loading || !!(detail?.upload?.final_report || detail?.feed?.personalized_summary);

  React.useEffect(() => {
    if (!showTranscriptTab && viewMode === 'transcript') {
      setViewMode('original');
    }
  }, [item?.id, showTranscriptTab, viewMode]);

  // 防止过期请求覆盖最新选中
  const requestSeqRef = React.useRef(0);

  // 当前条目索引 (用于 Prev/Next 启用判断)
  const currentIndex = React.useMemo(
    () => (item ? items.findIndex((x) => x.id === item.id) : -1),
    [item, items]
  );
  const hasPrev = currentIndex > 0;
  const hasNext = currentIndex >= 0 && currentIndex < items.length - 1;
  const isMobile = placement === 'bottom';

  // 切换 item 时拉取详情 (viewMode 不重置，跨 item 保持用户选择)
  React.useEffect(() => {
    if (!item) {
      setDetail(null);
      setError(null);
      return;
    }
    const seq = ++requestSeqRef.current;
    setLoading(true);
    setError(null);
    setDetail(null);
    (async () => {
      try {
        const d = await getDailyItemDetail(item.id);
        if (seq !== requestSeqRef.current) return;
        setDetail(d);
      } catch (e: any) {
        if (seq !== requestSeqRef.current) return;
        setError(e?.response?.data?.detail || e?.message || 'Failed to load detail');
      } finally {
        if (seq === requestSeqRef.current) setLoading(false);
      }
    })();
  }, [item?.id]); // eslint-disable-line react-hooks/exhaustive-deps -- item identity tied to id

  // 移动端 bottom-sheet: 切换条目后正文区滚回顶部
  React.useEffect(() => {
    if (!item?.id || !isMobile) return;
    requestAnimationFrame(() => {
      const body = document.querySelector('.daily-preview-drawer-mobile .ant-drawer-body');
      if (body instanceof HTMLElement) {
        body.scrollTop = 0;
      }
    });
  }, [item?.id, isMobile]);

  // 键盘快捷键 (与 Feeds 阅读器对齐):
  //   Esc — 关闭抽屉
  //   ↑ / ↓ / K / J — 上一条 / 下一条
  //   ← / → — Original / AI Analysis / Transcript（焦点在顶栏 Segmented 内时交给组件）
  React.useEffect(() => {
    if (!item) return;
    const handler = (e: KeyboardEvent) => {
      if (e.defaultPrevented) return;
      const target = e.target as HTMLElement | null;
      if (!target) return;

      if (target.closest('.ant-modal') || target.closest('.ant-image-preview-wrap')) return;

      const tag = target.tagName?.toUpperCase();
      const isEditable =
        tag === 'INPUT' ||
        tag === 'TEXTAREA' ||
        tag === 'SELECT' ||
        target.isContentEditable;
      if (isEditable && e.key !== 'Escape') return;

      if (e.key === 'Escape') {
        e.preventDefault();
        onClose();
        return;
      }

      // ← / → : 在可用 Tab 间循环
      if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
        if (target.closest('.ant-segmented')) return;
        e.preventDefault();
        const dir: 1 | -1 = e.key === 'ArrowRight' ? 1 : -1;
        setViewMode((prev) =>
          adjacentFeedArticleViewMode(prev, dir, analysisTabEnabled, showTranscriptTab)
        );
        return;
      }

      if ((e.key === 'ArrowUp' || e.key === 'k' || e.key === 'K') && hasPrev) {
        e.preventDefault();
        onNavigate?.(items[currentIndex - 1]);
      } else if (
        (e.key === 'ArrowDown' || e.key === 'j' || e.key === 'J') &&
        hasNext
      ) {
        e.preventDefault();
        onNavigate?.(items[currentIndex + 1]);
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [
    item,
    hasPrev,
    hasNext,
    currentIndex,
    items,
    onClose,
    onNavigate,
    detail,
    loading,
    analysisTabEnabled,
    showTranscriptTab,
  ]);

  const drawerSegmentedOptions = React.useMemo(
    () => {
      const opts: { label: string; value: FeedArticleViewMode; disabled?: boolean }[] = [
        { label: 'Original', value: 'original' },
        {
          label: 'AI Analysis',
          value: 'analysis',
          disabled: !analysisTabEnabled,
        },
      ];
      if (showTranscriptTab) {
        opts.push({ label: 'Transcript', value: 'transcript' });
      }
      return opts;
    },
    [analysisTabEnabled, showTranscriptTab]
  );

  React.useEffect(() => {
    if (!analysisTabEnabled && viewMode === 'analysis') {
      setViewMode('original');
    }
  }, [analysisTabEnabled, viewMode]);

  // 渲染顶栏: 第一行 = 来源/导航/关闭; 第二行 = 视图模式 Tab
  const headerNode = (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
      {/* 第一行: 来源徽章 + 导航 + 关闭 */}
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 8 }}>
        <Space size={8} align="center">
          {item && <SourceBadge subtype={item.source_subtype} themeMode={themeMode} />}
          {!isMobile && (
            <Text type="secondary" style={{ fontSize: 11 }}>
              {currentIndex >= 0 ? `${currentIndex + 1} / ${items.length}` : ''}
            </Text>
          )}
        </Space>
        <Space size={4}>
          {!isMobile && (
            <>
              <Tooltip title="Previous (↑ / K)" mouseEnterDelay={0.4}>
                <Button
                  size="small"
                  type="text"
                  icon={<ArrowUpOutlined />}
                  disabled={!hasPrev}
                  onClick={() => hasPrev && onNavigate?.(items[currentIndex - 1])}
                />
              </Tooltip>
              <Tooltip title="Next (↓ / J)" mouseEnterDelay={0.4}>
                <Button
                  size="small"
                  type="text"
                  icon={<ArrowDownOutlined />}
                  disabled={!hasNext}
                  onClick={() => hasNext && onNavigate?.(items[currentIndex + 1])}
                />
              </Tooltip>
            </>
          )}
          <Tooltip title="Close (Esc)" mouseEnterDelay={0.4}>
            <Button size="small" type="text" icon={<CloseOutlined />} onClick={onClose} />
          </Tooltip>
        </Space>
      </div>

      {/* 第二行: 视图模式切换 Tab */}
      <Segmented
        value={viewMode}
        onChange={(v) => setViewMode(v as FeedArticleViewMode)}
        options={drawerSegmentedOptions}
        style={{ width: '100%' }}
        block
      />
    </div>
  );

  // 抽屉 body: 标题/作者/时间 + AI Summary + Original 区
  // 在没拉到 detail 之前, 用列表条目里已有的字段先把 above-the-fold 渲染出来
  // (避免抽屉打开时空白闪烁)
  const renderBody = () => {
    if (!item) return null;
    const showSummary = !!(detail?.summary || item.summary);
    const summaryText = detail?.summary || item.summary;
    const tickers = detail?.tickers || item.tickers;
    const concepts = detail?.concepts || item.concepts;
    const sector = detail?.sector || item.sector;
    const industry = detail?.industry || item.industry;
    const authorName = detail?.author_name || item.author_name;
    const people = detail?.feed?.tags?.people ?? detail?.upload?.tags?.people ?? null;
    const countries = detail?.feed?.tags?.countries ?? detail?.upload?.tags?.countries ?? null;
    const tagSchemaVersion =
      detail?.feed?.tags?.tag_schema_version ?? detail?.upload?.tags?.tag_schema_version ?? null;
    const tweetSignal = detail?.feed?.tags?.additional_tags?.tweet_signal as
      | { stance?: string; signal_type?: string; importance?: string }
      | undefined;
    const analysisContent =
      detail?.upload?.final_report || detail?.feed?.personalized_summary || null;

    return (
      <Space direction="vertical" size={16} style={{ width: '100%' }}>
        {/* 标题 + 作者 / 时间 */}
        <div>
          <Title
            level={isMobile ? 5 : 4}
            style={{
              margin: '0 0 8px',
              color: colors.text,
              fontWeight: 600,
              lineHeight: 1.4,
              wordBreak: 'break-word',
            }}
          >
            {detail?.title || item.title}
          </Title>
          <Space size={8} wrap style={{ color: colors.textMuted, fontSize: 12 }}>
            {authorName && <span>{authorName}</span>}
            {authorName && <span style={{ opacity: 0.5 }}>·</span>}
            <span>{fmtTimeInTz(item.published_at, userTimezone)}</span>
            {item.reading_time_min ? (
              <>
                <span style={{ opacity: 0.5 }}>·</span>
                <span>{item.reading_time_min} min read</span>
              </>
            ) : null}
          </Space>
        </div>

        <AISummarySection
          themeMode={themeMode}
          colors={{
            text: colors.text,
            textMuted: colors.textMuted,
            divider: colors.divider,
            cardBg: colors.cardBg,
            accent: colors.accent,
          }}
          headerMeta={
            !showSummary ? (
              <Text type="secondary" style={{ fontSize: 11 }}>
                · pending — original below
              </Text>
            ) : undefined
          }
        >
          {showSummary && summaryText && (
            <Text
              style={{
                color: colors.text,
                fontSize: 13,
                lineHeight: 1.65,
                display: 'block',
                whiteSpace: 'pre-wrap',
              }}
            >
              {summaryText}
            </Text>
          )}

          {(sector || industry) && (
            <div style={{ marginTop: 8 }}>
              <Space size={[8, 8]} wrap>
                {sector && (
                  <Tag bordered={false} style={aiTagChipStyle('sector', themeMode)}>
                    {sector}
                  </Tag>
                )}
                {industry && industry !== sector && (
                  <Tag bordered={false} style={aiTagChipStyle('sector', themeMode)}>
                    {industry}
                  </Tag>
                )}
              </Space>
            </div>
          )}

          {tickers && tickers.length > 0 && (
            <div
              style={{
                marginTop: 8,
                display: 'flex',
                alignItems: 'center',
                gap: 6,
                flexWrap: 'wrap',
              }}
            >
              <Text type="secondary" style={{ fontSize: 11, flexShrink: 0 }}>
                <StockOutlined /> Tickers:
              </Text>
              <ReportTickerTags
                tickers={tickers}
                tagBordered={false}
                tagStyle={aiTagChipStyle('ticker', themeMode)}
                overflowTagStyle={aiTagChipStyle('overflow', themeMode)}
                maxVisible={6}
              />
            </div>
          )}

          {concepts && concepts.length > 0 && (
            <div
              style={{
                marginTop: 8,
                display: 'flex',
                alignItems: 'center',
                gap: 6,
                flexWrap: 'wrap',
              }}
            >
              <Text type="secondary" style={{ fontSize: 11, flexShrink: 0 }}>
                <BulbOutlined /> Concepts:
              </Text>
              {concepts.slice(0, 8).map((c) => (
                <Tag key={c} bordered={false} style={aiTagChipStyle('concept', themeMode)}>
                  {c}
                </Tag>
              ))}
            </div>
          )}

          {people && people.length > 0 && (
            <div
              style={{
                marginTop: 8,
                display: 'flex',
                alignItems: 'center',
                gap: 6,
                flexWrap: 'wrap',
              }}
            >
              <Text type="secondary" style={{ fontSize: 11, flexShrink: 0 }}>
                <TeamOutlined /> People:
              </Text>
              {people.slice(0, 5).map((p, i) => (
                <Tooltip
                  key={i}
                  title={[p.name_en, p.role, p.affiliation].filter(Boolean).join(' · ')}
                  mouseEnterDelay={0.3}
                >
                  <Tag bordered={false} style={aiTagChipStyle('person', themeMode)}>
                    {p.name || p.name_en}
                  </Tag>
                </Tooltip>
              ))}
              {people.length > 5 && (
                <Tag bordered={false} style={aiTagChipStyle('overflow', themeMode)}>
                  +{people.length - 5}
                </Tag>
              )}
            </div>
          )}

          {countries && countries.length > 0 && (
            <div
              style={{
                marginTop: 8,
                display: 'flex',
                alignItems: 'center',
                gap: 6,
                flexWrap: 'wrap',
              }}
            >
              <Text type="secondary" style={{ fontSize: 11, flexShrink: 0 }}>
                <GlobalOutlined /> Countries:
              </Text>
              {countries.map((c) => (
                <Tag key={c} bordered={false} style={aiTagChipStyle('country', themeMode)}>
                  {c}
                </Tag>
              ))}
            </div>
          )}

          {tagSchemaVersion === 'tweet_signal_v1' && tweetSignal && (
            <div style={{ marginTop: 8, display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
              {tweetSignal.signal_type && (
                <Tag bordered={false} style={aiTagChipStyle('signal-meta', themeMode)}>
                  {tweetSignal.signal_type}
                </Tag>
              )}
              {tweetSignal.stance && tweetSignal.stance !== 'none' && (
                <Tag
                  bordered={false}
                  style={aiTagChipStyle(
                    tweetSignal.stance === 'bullish' ? 'signal-bullish' :
                    tweetSignal.stance === 'bearish' ? 'signal-bearish' : 'signal-neutral',
                    themeMode
                  )}
                >
                  {tweetSignal.stance}
                </Tag>
              )}
              {tweetSignal.importance && (
                <Tag
                  bordered={false}
                  style={aiTagChipStyle(
                    tweetSignal.importance === 'high' ? 'signal-bearish' :
                    tweetSignal.importance === 'medium' ? 'signal-bullish' : 'signal-neutral',
                    themeMode
                  )}
                >
                  {tweetSignal.importance}
                </Tag>
              )}
            </div>
          )}
        </AISummarySection>

        {/* Original / AI Analysis / Transcript — 根据 Tab 切换 */}
        {viewMode === 'analysis' ? (
          <DailyAIAnalysisSection
            content={analysisContent}
            themeMode={themeMode}
          />
        ) : viewMode === 'transcript' ? (
          <CollapsibleOriginalSection>
            {loading && !detail ? (
              <div style={{ padding: 8 }}>
                <Skeleton active paragraph={{ rows: 4 }} />
              </div>
            ) : error ? (
              <Empty
                description={
                  <Text style={{ color: colors.textMuted }}>
                    {error || 'Failed to load transcript'}
                  </Text>
                }
              />
            ) : detail?.feed && item?.source_subtype === 'youtube_feed' ? (
              <YoutubeFeedTranscriptPanel
                contentDbId={detail.feed.content_db_id}
                hasTranscript={detail.feed.has_transcript}
                themeMode={themeMode}
                colors={{
                  text: colors.text,
                  textSecondary: colors.textSecondary,
                  textMuted: colors.textMuted,
                  divider: colors.divider,
                  link: colors.accent,
                }}
              />
            ) : detail?.upload &&
              item?.source === 'upload' &&
              item?.source_subtype === 'youtube' ? (
              <DailyYoutubeUploadTranscriptPanel
                upload={detail.upload}
                themeMode={themeMode}
                colors={colors}
              />
            ) : (
              <Spin />
            )}
          </CollapsibleOriginalSection>
        ) : (
          <CollapsibleOriginalSection>
            {loading && !detail ? (
              <div style={{ padding: 8 }}>
                <Skeleton active paragraph={{ rows: 4 }} />
              </div>
            ) : error ? (
              <Empty
                description={
                  <Text style={{ color: colors.textMuted }}>
                    {error || 'Failed to load original content'}
                  </Text>
                }
              />
            ) : detail ? (
              <DailyOriginalView
                detail={detail}
                themeMode={themeMode}
                splitYoutubeTranscriptTab
              />
            ) : (
              <Spin />
            )}
          </CollapsibleOriginalSection>
        )}
      </Space>
    );
  };

  const showMobileNavFabs =
    isMobile && !!item && currentIndex >= 0 && items.length > 0 && !!onNavigate;

  const dailyNavFabPortalStyle = React.useMemo(
    () =>
      ({
        ['--nav-fab-text' as string]: colors.text,
        ['--nav-fab-muted' as string]: colors.textMuted,
      }) as React.CSSProperties,
    [colors.text, colors.textMuted]
  );

  const dailyNavPortal =
    showMobileNavFabs &&
    typeof document !== 'undefined' &&
    createPortal(
      <div
        className="daily-preview-nav-portal"
        data-theme={themeMode}
        style={dailyNavFabPortalStyle}
      >
        <div className="daily-preview-nav-fabs" aria-label="Daily item navigation">
          <span className="daily-preview-nav-fab-tooltip-wrap">
            <button
              type="button"
              className="daily-preview-nav-fab"
              disabled={!hasPrev}
              onClick={() => hasPrev && onNavigate?.(items[currentIndex - 1])}
              aria-label="Previous item"
            >
              {'<'}
            </button>
          </span>
          <span className="daily-preview-nav-fab-tooltip-wrap">
            <button
              type="button"
              className="daily-preview-nav-fab"
              disabled={!hasNext}
              onClick={() => hasNext && onNavigate?.(items[currentIndex + 1])}
              aria-label="Next item"
            >
              {'>'}
            </button>
          </span>
        </div>
      </div>,
      document.body
    );

  return (
    <>
      <Drawer
        open={!!item}
        onClose={onClose}
        placement={placement}
        width={placement === 'right' ? DESKTOP_DRAWER_WIDTH : undefined}
        height={placement === 'bottom' ? '92vh' : undefined}
        closable={false}
        destroyOnClose
        rootClassName={isMobile ? 'daily-preview-drawer-mobile' : undefined}
        title={headerNode}
        styles={{
          header: {
            background: colors.cardBg,
            borderBottom: `1px solid ${colors.divider}`,
            padding: '10px 16px',
          },
          body: {
            background: colors.pageBg,
            padding: isMobile ? '14px 16px 32px' : '20px 24px 40px',
          },
          wrapper: { boxShadow: '-8px 0 24px rgba(0,0,0,0.08)' },
        }}
      >
        {renderBody()}
      </Drawer>

      {dailyNavPortal}
    </>
  );
};

export default DailyPreviewDrawer;
