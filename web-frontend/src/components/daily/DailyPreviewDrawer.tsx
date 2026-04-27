/**
 * Daily 抽屉预览 — 主容器.
 *
 * 卡片点击 → 打开本抽屉, 在 Daily 页面内就地预览, 不再跳出到 Sources/Feeds.
 *
 * 布局 (自上而下):
 *   ┌──────────────────────────────────────────────────────┐
 *   │ Header: SourceBadge · ‹ Prev / Next › · Open in...   │
 *   │ Title (h2)                                           │
 *   │ 作者 · 时间 · ⏱ X min read                            │
 *   ├──────────────────────────────────────────────────────┤
 *   │ AI Summary block (摘要 + sector/industry + tickers)   │
 *   │   - 缺失 summary 时降级提示并直接展开下方原文区        │
 *   ├──────────────────────────────────────────────────────┤
 *   │ Original Content (按 source_subtype 分发, 见            │
 *   │ DailyOriginalView)                                    │
 *   └──────────────────────────────────────────────────────┘
 *
 * 键盘:
 *   - Esc       关闭
 *   - ↑ / k     上一条
 *   - ↓ / j     下一条
 *
 * 设计取舍详见 docs/instructions/DAILY_MODULE.md §1.
 */

import React from 'react';
import {
  App,
  Button,
  Drawer,
  Empty,
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
  CloseOutlined,
  ExportOutlined,
  RobotOutlined,
} from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
import {
  DailyFeedItem,
  DailyItemDetail,
  getDailyItemDetail,
} from '../../services/daily';
import SourceBadge from './SourceBadge';
import { DAILY_THEME_COLORS } from './dailyTheme';
import DailyOriginalView from './DailyOriginalView';
import { formatTime as fmtTimeInTz } from '../../utils/datetime';
import { useDashboardSettings } from '../../contexts/DashboardSettingsContext';
import ReportTickerTags, { PC_INFO_TAGS_TAG_STYLE } from '../ReportTickerTags';

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
  const navigate = useNavigate();
  const { message } = App.useApp();

  const [detail, setDetail] = React.useState<DailyItemDetail | null>(null);
  const [loading, setLoading] = React.useState(false);
  const [error, setError] = React.useState<string | null>(null);

  // 防止过期请求覆盖最新选中
  const requestSeqRef = React.useRef(0);

  // 当前条目索引 (用于 Prev/Next 启用判断)
  const currentIndex = React.useMemo(
    () => (item ? items.findIndex((x) => x.id === item.id) : -1),
    [item, items]
  );
  const hasPrev = currentIndex > 0;
  const hasNext = currentIndex >= 0 && currentIndex < items.length - 1;

  // 切换 item 时拉详情
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

  // 键盘快捷键 (Esc / ↑ / ↓ / J / K)
  React.useEffect(() => {
    if (!item) return;
    const handler = (e: KeyboardEvent) => {
      // 表单输入中按键不要劫持
      const target = e.target as HTMLElement | null;
      const tag = target?.tagName?.toUpperCase();
      const isEditable =
        tag === 'INPUT' ||
        tag === 'TEXTAREA' ||
        (target as any)?.isContentEditable;
      if (isEditable && e.key !== 'Escape') return;

      if (e.key === 'Escape') {
        e.preventDefault();
        onClose();
      } else if ((e.key === 'ArrowUp' || e.key === 'k' || e.key === 'K') && hasPrev) {
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
  }, [item, hasPrev, hasNext, currentIndex, items, onClose, onNavigate]);

  const isMobile = placement === 'bottom';

  // 顶栏 "Open in Sources/Feeds" 行为 = 旧的跳转逻辑 (作为深度查看入口保留)
  const handleOpenInSource = React.useCallback(() => {
    if (!item) return;
    const nav = item.navigate;
    if (nav.type === 'execution' && nav.execution_id) {
      navigate(`/files?execution=${nav.execution_id}`);
      onClose();
      return;
    }
    if (nav.type === 'feed_article' && nav.subscription_id && nav.content_id) {
      if (isMobile) {
        // 移动端有现成深链 path
        navigate(`/feeds/article/${nav.subscription_id}/${nav.content_id}`);
      } else {
        // 桌面 FeedReaderPage 暂不支持 query 深链, 用 state 传过去
        navigate('/feeds', {
          state: {
            dailyTarget: {
              subscriptionId: nav.subscription_id,
              contentId: nav.content_id,
            },
          },
        });
      }
      onClose();
      return;
    }
    if (nav.external_url) {
      window.open(nav.external_url, '_blank', 'noopener,noreferrer');
      return;
    }
    message.info('No navigation target available');
  }, [item, navigate, onClose, message, isMobile]);

  // 渲染顶栏 (Drawer.title 槽位会自带关闭按钮 X, 这里我们换掉默认 closable 自定义)
  const headerNode = (
    <div
      style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: 8,
        flexWrap: 'wrap',
      }}
    >
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
        {item && (
          <Tooltip
            title={
              item.source === 'upload'
                ? 'Open in Sources for full editor & AI tools'
                : item.navigate.type === 'external_url'
                ? 'Open original link in new tab'
                : 'Open in Feeds for full reader'
            }
            mouseEnterDelay={0.4}
          >
            <Button size="small" icon={<ExportOutlined />} onClick={handleOpenInSource}>
              {isMobile ? 'Open' : 'Open in ' + (item.source === 'upload' ? 'Sources' : 'Feeds')}
            </Button>
          </Tooltip>
        )}
        <Tooltip title="Close (Esc)" mouseEnterDelay={0.4}>
          <Button size="small" type="text" icon={<CloseOutlined />} onClick={onClose} />
        </Tooltip>
      </Space>
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

        {/* AI Summary 块 */}
        <div
          style={{
            borderRadius: 10,
            padding: '12px 14px',
            background:
              themeMode === 'dark'
                ? 'rgba(33, 150, 243, 0.07)'
                : 'rgba(33, 150, 243, 0.06)',
            border: `1px solid ${colors.divider}`,
            borderLeft: `3px solid ${colors.accent}`,
          }}
        >
          <Space size={6} align="center" style={{ marginBottom: showSummary ? 8 : 0 }}>
            <RobotOutlined style={{ color: colors.accent, fontSize: 14 }} />
            <Text strong style={{ color: colors.text, fontSize: 13 }}>
              AI Summary
            </Text>
            {!showSummary && (
              <Text type="secondary" style={{ fontSize: 11 }}>
                · pending — showing original content below
              </Text>
            )}
          </Space>

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
              <Space size={[4, 4]} wrap>
                {sector && (
                  <Tag color="blue" style={{ fontSize: 11, margin: 0 }}>
                    {sector}
                  </Tag>
                )}
                {industry && industry !== sector && (
                  <Tag color="cyan" style={{ fontSize: 11, margin: 0 }}>
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
                Tickers:
              </Text>
              <ReportTickerTags
                tickers={tickers}
                tagColor="magenta"
                tagStyle={PC_INFO_TAGS_TAG_STYLE}
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
                Concepts:
              </Text>
              {concepts.slice(0, 8).map((c) => (
                <Tag key={c} style={{ fontSize: 11, margin: 0 }}>
                  {c}
                </Tag>
              ))}
            </div>
          )}
        </div>

        {/* Original 区 */}
        <div>
          <Text
            type="secondary"
            style={{
              fontSize: 11,
              letterSpacing: 0.6,
              textTransform: 'uppercase',
              color: colors.textMuted,
              display: 'block',
              marginBottom: 8,
            }}
          >
            Original
          </Text>
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
            <DailyOriginalView detail={detail} themeMode={themeMode} />
          ) : (
            <Spin />
          )}
        </div>
      </Space>
    );
  };

  return (
    <Drawer
      open={!!item}
      onClose={onClose}
      placement={placement}
      width={placement === 'right' ? DESKTOP_DRAWER_WIDTH : undefined}
      height={placement === 'bottom' ? '92vh' : undefined}
      closable={false}
      destroyOnClose
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
  );
};

export default DailyPreviewDrawer;
