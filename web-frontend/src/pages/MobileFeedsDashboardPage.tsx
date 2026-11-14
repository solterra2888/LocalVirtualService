/**
 * Feeds Dashboard (移动端 /feeds-dashboard).
 *
 * 桌面页的移动形态重构 (数据层与 FeedsDashboardPage 同源, 只换 UI):
 *   - 吸顶紧凑筛选栏: 日期 + 重要性 / 范围 + 热词·刷新·生成, 滚动中常驻可及
 *   - Watchlist 命中 chips 横滑行: 有动静的票一眼可见, 点按 = ticker 词过滤
 *   - 词云收进底部抽屉 (WordCloudPanel 复用), 主内容直接从主题卡片开始
 *   - 主题卡片「View all →」钻入主题详情视图 (状态切换, 无限滚动), 返回键回首页
 *   - 条目预览复用 DailyPreviewDrawer (bottom-sheet), Prev/Next 拉全量列表与桌面同口径
 *
 * 自动刷新 / 阅读中不打扰 / scope localStorage 持久化: 与桌面页行为一致。
 */

import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  App,
  Button,
  ConfigProvider,
  Drawer,
  Empty,
  Segmented,
  Skeleton,
  Spin,
  Tooltip,
  theme as antdTheme,
} from 'antd';
import {
  CaretDownOutlined,
  CaretUpOutlined,
  FireOutlined,
  LeftOutlined,
  ReloadOutlined,
  RocketOutlined,
} from '@ant-design/icons';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import { useAuthStore } from '../store/authStore';
import { DAILY_THEME_COLORS } from '../components/daily/dailyTheme';
import DailyPreviewDrawer from '../components/daily/DailyPreviewDrawer';
import WordCloudPanel, {
  SelectedWord,
  WordCloudWord,
} from '../components/feedsDashboard/WordCloudPanel';
import { getChannelColor, getChannelIcon } from '../components/feed/feedChannelConfig';
import {
  FeedsDashboardDrawerItem,
  FeedsDashboardItem,
  FeedsDashboardItemsResponse,
  FeedsDashboardResponse,
  FeedsDashboardScope,
  FeedsDashboardTopic,
  FeedsImportance,
  FeedsMinImportance,
  generateFeedsDashboard,
  getFeedsDashboard,
  getFeedsDashboardItemDetail,
  getFeedsDashboardItems,
  getFeedsDashboardTopicItems,
  getFeedsDashboardWordCloud,
  toDailyFeedItem,
} from '../services/feedsDashboard';
import { formatTime } from '../utils/datetime';
import { DailyFeedItem, getDailyItemDetail } from '../services/daily';
import { MOBILE_TASK_PROGRESS_BAR_HEIGHT } from '../components/mobile/MobileTaskProgressBar';
import '../styles/mobilePageLoading.css';

// importance 徽标配色 (与桌面页一致, 跨明暗主题可读)
const IMPORTANCE_META: Record<FeedsImportance, { label: string; color: string }> = {
  high: { label: 'High', color: '#cf1322' },
  medium: { label: 'Med', color: '#d48806' },
  low: { label: 'Low', color: '#8c8c8c' },
};

const IMPORTANCE_FILTER_OPTIONS = [
  { label: 'All', value: 'all' },
  { label: 'Med+', value: 'medium' },
  { label: 'High', value: 'high' },
];

/** 视图范围: 我的订阅 / 全站内容池 (与桌面页共用同一 localStorage key) */
const SCOPE_STORAGE_KEY = 'feeds-dashboard-scope';
const SCOPE_OPTIONS = [
  { label: 'My Feeds', value: 'mine' },
  { label: 'Server', value: 'server' },
];

function loadStoredScope(): FeedsDashboardScope {
  try {
    const v = localStorage.getItem(SCOPE_STORAGE_KEY);
    return v === 'server' ? 'server' : 'mine';
  } catch {
    return 'mine';
  }
}

/** 日期范围 (移动端紧凑标签): 当天 (默认) / 近3天 / 近一周 */
const DATE_RANGE_OPTIONS = [
  { label: 'Today', value: 1 },
  { label: '3D', value: 3 },
  { label: '7D', value: 7 },
];

/** 自动刷新周期 (与桌面页一致; 后端快照每 15 分钟聚合) */
const AUTO_REFRESH_INTERVAL_MS = 5 * 60 * 1000;

/** 主题详情分页大小 */
const DETAIL_PAGE_SIZE = 20;

const MobileFeedsDashboardPageInner: React.FC = () => {
  const { themeMode, userTimezone } = useDashboardSettings();
  const { tasks } = useFileProcessing();
  const isAdmin = !!useAuthStore((s) => s.user?.is_admin);
  const colors = DAILY_THEME_COLORS[themeMode];
  const { message } = App.useApp();

  /** 顶栏 56px + 可选任务进度条 (与 MobileLayout / MobileDailyPage 对齐) */
  const mobileStickyTop = tasks.size > 0 ? 56 + MOBILE_TASK_PROGRESS_BAR_HEIGHT : 56;

  const [rangeDays, setRangeDays] = useState<number>(1);
  const [minImportance, setMinImportance] = useState<FeedsMinImportance | 'all'>('all');
  const [scope, setScope] = useState<FeedsDashboardScope>(loadStoredScope);

  const [data, setData] = useState<FeedsDashboardResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // 词云 (底部抽屉) + 选中词过滤的扁平列表 (主区切换视图)
  const [cloudOpen, setCloudOpen] = useState(false);
  const [cloudWords, setCloudWords] = useState<WordCloudWord[]>([]);
  const [cloudWatchlist, setCloudWatchlist] = useState<WordCloudWord[]>([]);
  const [cloudLoading, setCloudLoading] = useState(false);
  const [selectedWord, setSelectedWord] = useState<SelectedWord | null>(null);
  const [wordResult, setWordResult] = useState<FeedsDashboardItemsResponse | null>(null);
  const [wordLoading, setWordLoading] = useState(false);

  // 折叠的主题 (默认全部展开)
  const [collapsedTopics, setCollapsedTopics] = useState<Set<number>>(new Set());

  // 主题详情钻入视图 (状态切换, 同 MobileDashboardPage selectedStock 模式)
  const [topicDetail, setTopicDetail] = useState<FeedsDashboardTopic | null>(null);
  const [detailItems, setDetailItems] = useState<FeedsDashboardItem[]>([]);
  const [detailTotal, setDetailTotal] = useState(0);
  const [detailHasMore, setDetailHasMore] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const detailOffsetRef = useRef(0);
  const detailSeqRef = useRef(0);
  const detailLoadMoreRef = useRef<() => void>(() => {});
  const detailSentinelRef = useRef<HTMLDivElement | null>(null);

  // Drawer 预览
  const [activeItem, setActiveItem] = useState<DailyFeedItem | null>(null);
  const [drawerNavItems, setDrawerNavItems] = useState<DailyFeedItem[]>([]);
  const drawerNavSeqRef = useRef(0);
  const [generating, setGenerating] = useState(false);

  // 防重入 + 最近刷新时间 (聚焦时判断是否需要刷)
  const loadInFlightRef = useRef(false);
  const lastRefreshAtRef = useRef(0);

  const load = useCallback(
    async (opts?: { silent?: boolean }) => {
      if (loadInFlightRef.current) return;
      loadInFlightRef.current = true;
      if (!opts?.silent) setLoading(true);
      setError(null);
      // 切换过滤切片 = 新的词频空间, 清除词选择回主题视图
      setSelectedWord(null);
      setWordResult(null);
      setCloudLoading(true);
      const query = {
        days: rangeDays,
        min_importance: minImportance === 'all' ? undefined : minImportance,
        scope,
      };
      try {
        const [res, cloud] = await Promise.all([
          getFeedsDashboard({ ...query, items_per_topic: 5 }),
          getFeedsDashboardWordCloud(query).catch(() => null),
        ]);
        setData(res);
        setCloudWords(cloud?.words || []);
        setCloudWatchlist(cloud?.watchlist || []);
        lastRefreshAtRef.current = Date.now();
      } catch (e: any) {
        setError(e?.response?.data?.detail || e?.message || 'Failed to load');
      } finally {
        setLoading(false);
        setCloudLoading(false);
        loadInFlightRef.current = false;
      }
    },
    [rangeDays, minImportance, scope],
  );

  // 自动刷新: 每 5 分钟静默刷新; 切回标签页时若已过期立即刷。
  // 阅读状态不打扰 — 抽屉打开 / 词过滤中 / 主题详情中 / 加载中 跳过本次。
  useEffect(() => {
    const shouldSkip = () =>
      document.visibilityState !== 'visible' ||
      !!activeItem ||
      !!selectedWord ||
      !!topicDetail ||
      loading;

    const tick = () => {
      if (!shouldSkip()) void load({ silent: true });
    };

    const onVisibilityChange = () => {
      if (
        document.visibilityState === 'visible' &&
        Date.now() - lastRefreshAtRef.current > AUTO_REFRESH_INTERVAL_MS &&
        !shouldSkip()
      ) {
        void load({ silent: true });
      }
    };

    const id = window.setInterval(tick, AUTO_REFRESH_INTERVAL_MS);
    document.addEventListener('visibilitychange', onVisibilityChange);
    return () => {
      window.clearInterval(id);
      document.removeEventListener('visibilitychange', onVisibilityChange);
    };
  }, [load, activeItem, selectedWord, topicDetail, loading]);

  // 词云/Watchlist chip 点击: 选词 → 拉过滤列表; 再点/清除 → 回主题视图
  const handleSelectWord = useCallback(
    async (word: SelectedWord | null) => {
      setSelectedWord(word);
      setWordResult(null);
      if (!word) return;
      setWordLoading(true);
      try {
        const res = await getFeedsDashboardItems({
          days: rangeDays,
          min_importance: minImportance === 'all' ? undefined : minImportance,
          scope,
          // 折并词走 topic_id 过滤 (命中该主题全部变体); 散词/ticker 走精确词过滤
          ...(word.topicId
            ? { topic_id: word.topicId }
            : { word: word.word, word_type: word.type }),
          limit: 30,
          offset: 0,
        });
        setWordResult(res);
      } catch (e: any) {
        message.error(e?.response?.data?.detail || 'Filter failed');
        setSelectedWord(null);
      } finally {
        setWordLoading(false);
      }
    },
    [rangeDays, minImportance, scope, message],
  );

  // 词过滤视图「继续加载」
  const loadMoreByWord = useCallback(async () => {
    if (!selectedWord || !wordResult) return;
    setWordLoading(true);
    try {
      const res = await getFeedsDashboardItems({
        days: rangeDays,
        min_importance: minImportance === 'all' ? undefined : minImportance,
        scope,
        ...(selectedWord.topicId
          ? { topic_id: selectedWord.topicId }
          : { word: selectedWord.word, word_type: selectedWord.type }),
        limit: 30,
        offset: wordResult.items.length,
      });
      setWordResult((prev) =>
        prev
          ? { ...res, items: prev.items.concat(res.items) }
          : res,
      );
    } catch (e: any) {
      message.error(e?.response?.data?.detail || 'Failed to load more');
    } finally {
      setWordLoading(false);
    }
  }, [rangeDays, minImportance, scope, selectedWord, wordResult, message]);

  useEffect(() => {
    void load();
  }, [load]);

  // ─── 主题详情钻入 ────────────────────────────────────────

  const openTopicDetail = useCallback((topic: FeedsDashboardTopic) => {
    ++detailSeqRef.current; // 使在途分页请求失效
    // 种子 = 首页卡片已拿到的 top5, 后续无限滚动续拉
    setTopicDetail(topic);
    setDetailItems(topic.items);
    setDetailTotal(topic.item_count);
    setDetailHasMore(topic.has_more);
    setDetailLoading(false);
    detailOffsetRef.current = topic.items.length;
    window.scrollTo({ top: 0 });
  }, []);

  const closeTopicDetail = useCallback(() => {
    ++detailSeqRef.current; // 使在途分页请求失效
    setTopicDetail(null);
    setDetailItems([]);
    setDetailTotal(0);
    setDetailHasMore(false);
    setDetailLoading(false);
    window.scrollTo({ top: 0 });
  }, []);

  const loadDetailMore = useCallback(async () => {
    if (!topicDetail || detailLoading || !detailHasMore) return;
    const seq = detailSeqRef.current;
    setDetailLoading(true);
    try {
      const res = await getFeedsDashboardTopicItems(topicDetail.topic_id, {
        days: rangeDays,
        min_importance: minImportance === 'all' ? undefined : minImportance,
        scope,
        limit: DETAIL_PAGE_SIZE,
        offset: detailOffsetRef.current,
      });
      if (seq !== detailSeqRef.current) return;
      setDetailItems((prev) => prev.concat(res.items));
      setDetailTotal(res.total);
      setDetailHasMore(res.has_more);
      detailOffsetRef.current += res.items.length;
    } catch {
      // 静默: 哨兵会随滚动重试
    } finally {
      if (seq === detailSeqRef.current) setDetailLoading(false);
    }
  }, [topicDetail, detailLoading, detailHasMore, rangeDays, minImportance, scope]);

  useEffect(() => {
    detailLoadMoreRef.current = loadDetailMore;
  });

  // 无限滚动哨兵 + 滚动兜底 (与 MobileDailyPage 同模式)
  useEffect(() => {
    const el = detailSentinelRef.current;
    if (!el || !topicDetail) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting) detailLoadMoreRef.current();
      },
      { rootMargin: '400px', threshold: 0 },
    );
    observer.observe(el);
    return () => observer.disconnect();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [!!topicDetail, detailItems.length > 0]);

  useEffect(() => {
    const handleScroll = () => {
      const sentinel = detailSentinelRef.current;
      if (!sentinel) return;
      const rect = sentinel.getBoundingClientRect();
      if (rect.top <= window.innerHeight + 400) {
        detailLoadMoreRef.current();
      }
    };
    document.addEventListener('scroll', handleScroll, { capture: true, passive: true });
    return () => document.removeEventListener('scroll', handleScroll, true);
  }, []);

  // admin 手动生成快照 (近 7 天, 覆盖范围查询)
  const handleGenerate = useCallback(async () => {
    setGenerating(true);
    try {
      const res = await generateFeedsDashboard({ days: 7 });
      message.success(res.message || 'Aggregation task submitted');
      // 聚合是异步 Celery 任务, 延迟后自动刷新一次
      setTimeout(() => void load({ silent: true }), 8000);
    } catch (e: any) {
      message.error(e?.response?.data?.detail || 'Failed to trigger');
    } finally {
      setGenerating(false);
    }
  }, [load, message]);

  // Drawer 的 Prev/Next 导航集: 点击条目时按其所属上下文 (主题 / 词过滤) 异步拉取
  // 完整列表 (与列表头计数同口径), 先用单击条目占位开抽屉, 全量列表到达后替换 —
  // 与桌面页同口径 (词过滤前30 / 主题 top5 只是已加载窗口)。
  const loadDrawerNav = useCallback(
    async (clicked: FeedsDashboardItem, topicId?: number) => {
      const seq = ++drawerNavSeqRef.current;
      try {
        const filters = {
          days: rangeDays,
          min_importance: minImportance === 'all' ? undefined : minImportance,
          scope,
        };
        const list: FeedsDashboardItem[] = [];
        let offset = 0;
        let hasMore = true;
        while (hasMore && list.length < 500) {
          // 词过滤视图 → /items (word 或折并 topic_id); 主题视图 → /topics/{id}/items
          const res =
            !topicId && selectedWord
              ? await getFeedsDashboardItems({
                  ...filters,
                  ...(selectedWord.topicId
                    ? { topic_id: selectedWord.topicId }
                    : { word: selectedWord.word, word_type: selectedWord.type }),
                  limit: 100,
                  offset,
                })
              : await getFeedsDashboardTopicItems(topicId as number, {
                  ...filters,
                  limit: 100,
                  offset,
                });
          list.push(...res.items);
          offset += res.items.length;
          hasMore = res.has_more;
        }
        if (seq !== drawerNavSeqRef.current) return; // 用户已点击其他条目, 丢弃
        setDrawerNavItems(list.map(toDailyFeedItem));
      } catch {
        // 导航集加载失败: 保持占位单条 (抽屉仍可看当前条目详情)
      }
    },
    [rangeDays, minImportance, scope, selectedWord],
  );

  // 抽屉详情路由: 订阅外条目 (全站视图) 走 feeds-dashboard 全站端点 —
  // /api/daily/items 有用户范围鉴权, 订阅外内容会 404
  const drawerFetchDetail = useCallback((item: DailyFeedItem) => {
    if ((item as FeedsDashboardDrawerItem).in_user_scope === false) {
      const contentDbId = Number(item.id.slice('feed_'.length));
      return getFeedsDashboardItemDetail(contentDbId);
    }
    return getDailyItemDetail(item.id);
  }, []);

  const toggleTopic = useCallback((topicId: number) => {
    setCollapsedTopics((prev) => {
      const next = new Set(prev);
      if (next.has(topicId)) next.delete(topicId);
      else next.add(topicId);
      return next;
    });
  }, []);

  // 条目点击: 一律抽屉就地预览; topicId 用于抽屉 Prev/Next 拉取该主题完整列表
  const handleItemClick = (item: FeedsDashboardItem, topicId?: number) => {
    const converted = toDailyFeedItem(item);
    setDrawerNavItems([converted]); // 占位: 先保证抽屉可开、Prev/Next 禁用
    setActiveItem(converted);
    void loadDrawerNav(item, topicId);
  };

  // ─── 渲染 ────────────────────────────────────────────────

  const cardStyle: React.CSSProperties = {
    background: colors.cardBg,
    border: `1px solid ${colors.cardBorder}`,
    borderRadius: 8,
    padding: 12,
    marginBottom: 12,
  };

  const importanceBadge = (imp: FeedsImportance) => {
    const meta = IMPORTANCE_META[imp] || IMPORTANCE_META.low;
    return (
      <span
        style={{
          flexShrink: 0,
          marginTop: 3,
          fontSize: 11,
          lineHeight: '16px',
          padding: '0 5px',
          borderRadius: 4,
          color: meta.color,
          border: `1px solid ${meta.color}`,
          fontWeight: 600,
        }}
      >
        {meta.label}
      </span>
    );
  };

  const renderItemRow = (item: FeedsDashboardItem, topicId?: number) => (
    <div
      key={item.id}
      style={{
        display: 'flex',
        alignItems: 'flex-start',
        gap: 10,
        padding: '9px 8px',
        borderRadius: 6,
        cursor: 'pointer',
      }}
      onClick={() => handleItemClick(item, topicId)}
    >
      {importanceBadge(item.importance)}
      <span style={{ marginTop: 3, color: getChannelColor(item.channel_type), flexShrink: 0 }}>
        {getChannelIcon(item.channel_type)}
      </span>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div
          style={{
            color: colors.text,
            fontSize: 13,
            fontWeight: 500,
            overflow: 'hidden',
            textOverflow: 'ellipsis',
            whiteSpace: 'nowrap',
          }}
        >
          {item.title}
        </div>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 8,
            marginTop: 2,
            fontSize: 12,
            color: colors.textSecondary,
            flexWrap: 'wrap',
          }}
        >
          <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 140 }}>
            {item.author_name || item.author_identifier || ''}
          </span>
          <span>{formatTime(item.published_at, userTimezone)}</span>
          {item.tickers.slice(0, 3).map((t) => (
            <span
              key={t}
              style={{
                background: colors.tickerBg,
                color: colors.tickerFg,
                padding: '0 5px',
                borderRadius: 4,
                fontSize: 11,
              }}
            >
              {t}
            </span>
          ))}
        </div>
      </div>
    </div>
  );

  const renderTopic = (topic: FeedsDashboardTopic) => {
    const collapsed = collapsedTopics.has(topic.topic_id);
    return (
      <div key={topic.topic_id} style={cardStyle}>
        {/* 主题头 */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 8,
            flexWrap: 'wrap',
            cursor: 'pointer',
            paddingBottom: collapsed ? 0 : 10,
            borderBottom: collapsed ? 'none' : `1px solid ${colors.divider}`,
          }}
          onClick={() => toggleTopic(topic.topic_id)}
        >
          <FireOutlined style={{ color: topic.high_count > 0 ? '#cf1322' : colors.accent }} />
          <span style={{ fontSize: 15, fontWeight: 600, color: colors.text }}>
            {topic.name_cn}
          </span>
          <span style={{ fontSize: 12, color: colors.textSecondary }}>
            {topic.item_count} · H{topic.high_count}/M{topic.medium_count}
          </span>
          <span style={{ flex: 1 }} />
          {topic.top_tickers.slice(0, 3).map((t) => (
            <span
              key={t}
              style={{
                background: colors.tickerBg,
                color: colors.tickerFg,
                padding: '0 6px',
                borderRadius: 4,
                fontSize: 11,
              }}
            >
              {t}
            </span>
          ))}
          {collapsed ? (
            <CaretDownOutlined style={{ color: colors.textSecondary }} />
          ) : (
            <CaretUpOutlined style={{ color: colors.textSecondary }} />
          )}
        </div>

        {/* 条目列表 (top5) */}
        {!collapsed && (
          <div style={{ paddingTop: 4 }}>
            {topic.items.map((it) => renderItemRow(it, topic.topic_id))}
            {topic.has_more && (
              <Button
                type="link"
                size="small"
                style={{ paddingInline: 8 }}
                onClick={() => openTopicDetail(topic)}
              >
                View all {topic.item_count} →
              </Button>
            )}
          </div>
        )}
      </div>
    );
  };

  // Watchlist 命中 chips (有动静的票, 按命中数降序; 点按 = ticker 词过滤)
  const watchlistHits = cloudWatchlist
    .filter((w) => w.count > 0)
    .sort((a, b) => b.count - a.count);

  const notGenerated = !!data && !data.aggregated_at && data.topics.length === 0;

  return (
    <div style={{ background: colors.pageBg, minHeight: '100%', padding: '12px 12px 40px' }}>
      {topicDetail ? (
        /* ─── 主题详情视图 (钻入, 隐藏筛选栏) ─── */
        <>
          <div
            style={{
              position: 'sticky',
              top: mobileStickyTop,
              zIndex: 20,
              background: colors.pageBg,
              paddingBottom: 8,
              marginBottom: 4,
              backdropFilter: 'blur(8px)',
            }}
          >
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <Button
                type="text"
                size="small"
                aria-label="Back to topics"
                icon={<LeftOutlined style={{ fontSize: 16 }} />}
                onClick={closeTopicDetail}
              />
              <FireOutlined
                style={{ color: topicDetail.high_count > 0 ? '#cf1322' : colors.accent }}
              />
              <span style={{ fontSize: 16, fontWeight: 600, color: colors.text }}>
                {topicDetail.name_cn}
              </span>
              <span style={{ fontSize: 12, color: colors.textSecondary }}>
                {detailTotal} items
              </span>
            </div>
          </div>
          <div style={cardStyle}>
            {detailItems.map((it) => renderItemRow(it, topicDetail.topic_id))}
            {detailLoading && (
              <Skeleton active title={false} paragraph={{ rows: 2 }} style={{ padding: 8 }} />
            )}
            {!detailHasMore && detailItems.length > 0 && (
              <div
                style={{
                  textAlign: 'center',
                  color: colors.textMuted,
                  fontSize: 11,
                  padding: '12px 0',
                }}
              >
                — {detailTotal} items —
              </div>
            )}
          </div>
          <div ref={detailSentinelRef} style={{ height: 24 }} />
        </>
      ) : (
        /* ─── 首页视图 ─── */
        <>
          {/* 吸顶紧凑筛选栏 */}
          <div
            style={{
              position: 'sticky',
              top: mobileStickyTop,
              zIndex: 20,
              background: colors.pageBg,
              paddingBottom: 8,
              marginBottom: 4,
              backdropFilter: 'blur(8px)',
            }}
          >
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 8,
                marginBottom: 8,
              }}
            >
              <Segmented
                value={rangeDays}
                onChange={(v) => setRangeDays(v as number)}
                options={DATE_RANGE_OPTIONS}
                size="small"
              />
              <Segmented
                value={minImportance}
                onChange={(v) => setMinImportance(v as FeedsMinImportance | 'all')}
                options={IMPORTANCE_FILTER_OPTIONS}
                size="small"
              />
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <Segmented
                value={scope}
                onChange={(v) => {
                  const next = v as FeedsDashboardScope;
                  setScope(next);
                  try {
                    localStorage.setItem(SCOPE_STORAGE_KEY, next);
                  } catch {
                    /* 忽略隐私模式等存储异常 */
                  }
                }}
                options={SCOPE_OPTIONS}
                size="small"
              />
              <span style={{ flex: 1 }} />
              <Tooltip title="Keywords & tickers">
                <Button
                  size="small"
                  aria-label="Keywords and tickers"
                  icon={
                    <FireOutlined
                      style={{ color: selectedWord ? colors.accent : undefined }}
                    />
                  }
                  onClick={() => setCloudOpen(true)}
                />
              </Tooltip>
              <Tooltip title="Refresh">
                <Button
                  size="small"
                  aria-label="Refresh"
                  icon={<ReloadOutlined />}
                  onClick={() => void load()}
                  loading={loading}
                />
              </Tooltip>
              {isAdmin && (
                <Tooltip title="Manually aggregate the last 7 days (auto-runs every 15 min)">
                  <Button
                    size="small"
                    aria-label="Generate snapshot"
                    icon={<RocketOutlined />}
                    onClick={() => void handleGenerate()}
                    loading={generating}
                  />
                </Tooltip>
              )}
            </div>
          </div>

          {/* Watchlist 命中 chips 横滑行 (无命中不占位) */}
          {watchlistHits.length > 0 && (
            <div
              style={{
                display: 'flex',
                gap: 6,
                overflowX: 'auto',
                paddingBottom: 6,
                marginBottom: 6,
                WebkitOverflowScrolling: 'touch',
              }}
            >
              {watchlistHits.map((w) => {
                const active =
                  selectedWord?.type === 'ticker' && selectedWord?.word === w.word;
                return (
                  <button
                    key={w.word}
                    onClick={() =>
                      void handleSelectWord(
                        active ? null : { word: w.word, type: 'ticker' },
                      )
                    }
                    style={{
                      flexShrink: 0,
                      display: 'inline-flex',
                      alignItems: 'center',
                      gap: 4,
                      padding: '2px 8px',
                      borderRadius: 10,
                      fontSize: 12,
                      border: `1px solid ${active ? colors.accent : colors.cardBorder}`,
                      background: active ? colors.accent : colors.tickerBg,
                      color: active ? '#fff' : colors.tickerFg,
                      minHeight: 24,
                    }}
                  >
                    {w.word}
                    <span
                      style={{
                        fontSize: 10,
                        background: active ? 'rgba(255,255,255,0.25)' : colors.accent,
                        color: active ? '#fff' : '#fff',
                        borderRadius: 8,
                        padding: '0 5px',
                      }}
                    >
                      {w.count}
                    </span>
                  </button>
                );
              })}
            </div>
          )}

          {/* 统计行 */}
          {data && (
            <div style={{ fontSize: 12, color: colors.textSecondary, marginBottom: 12 }}>
              {data.topics.length} topics · {data.unique_content_count} items
              {data.aggregated_at
                ? ` · snapshot ${formatTime(data.aggregated_at, userTimezone)}`
                : ' · no snapshot for selected range'}
              {' · auto 5 min'}
            </div>
          )}

          {/* 主区: 词过滤视图 / 主题卡片 */}
          {loading ? (
            <div className="mobile-page-loading-wrap" aria-busy="true" aria-live="polite">
              <Spin size="large" />
            </div>
          ) : error ? (
            <Empty description={error} style={{ padding: 48 }} />
          ) : selectedWord ? (
            <div style={cardStyle}>
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 8,
                  flexWrap: 'wrap',
                  paddingBottom: 10,
                  borderBottom: `1px solid ${colors.divider}`,
                }}
              >
                <span
                  style={{
                    background:
                      selectedWord.type === 'ticker' ? colors.tickerBg : colors.sectionHeader,
                    color: selectedWord.type === 'ticker' ? colors.tickerFg : colors.text,
                    padding: '2px 8px',
                    borderRadius: 4,
                    fontSize: 13,
                    fontWeight: 600,
                  }}
                >
                  {selectedWord.word}
                </span>
                <span style={{ fontSize: 13, color: colors.text, fontWeight: 600 }}>
                  {wordResult ? `${wordResult.total} related items` : '…'}
                </span>
                <span style={{ flex: 1 }} />
                <Button
                  type="link"
                  size="small"
                  onClick={() => void handleSelectWord(null)}
                >
                  × Clear
                </Button>
              </div>
              <div style={{ paddingTop: 4 }}>
                {wordResult && wordResult.items.length === 0 && !wordLoading ? (
                  <Empty
                    style={{ padding: 32 }}
                    description={
                      <span style={{ color: colors.textSecondary }}>
                        No matching items for “{selectedWord.word}” in the selected range
                      </span>
                    }
                  />
                ) : (
                  <>
                    {(wordResult?.items || []).map((it) => renderItemRow(it))}
                    {wordLoading && (
                      <Skeleton active title={false} paragraph={{ rows: 2 }} style={{ padding: 8 }} />
                    )}
                    {wordResult?.has_more && !wordLoading && (
                      <Button
                        type="link"
                        size="small"
                        style={{ paddingInline: 8 }}
                        onClick={() => void loadMoreByWord()}
                      >
                        Load more ({wordResult.items.length}/{wordResult.total})
                      </Button>
                    )}
                  </>
                )}
              </div>
            </div>
          ) : notGenerated ? (
            <Empty
              style={{ padding: 48 }}
              description={
                <span style={{ color: colors.textSecondary }}>
                  No topic snapshot for the selected range yet
                  {isAdmin
                    ? ' — tap 🚀 to aggregate now'
                    : ' — wait for the scheduled task or ask an admin to trigger it'}
                </span>
              }
            />
          ) : !data || data.topics.length === 0 ? (
            <Empty
              style={{ padding: 48 }}
              description={
                <span style={{ color: colors.textSecondary }}>
                  No content under current filters (items in the selected range may all be low importance)
                </span>
              }
            />
          ) : (
            data.topics.map(renderTopic)
          )}
        </>
      )}

      {/* 词云底部抽屉: 选中词后关闭并进入词过滤视图 */}
      <Drawer
        open={cloudOpen}
        onClose={() => setCloudOpen(false)}
        placement="bottom"
        height="70%"
        title="Keywords · Watchlist · Tickers"
        styles={{ body: { padding: 12, overflowY: 'auto' } }}
      >
        <WordCloudPanel
          words={cloudWords}
          watchlistWords={cloudWatchlist}
          selected={selectedWord}
          onSelect={(w) => {
            void handleSelectWord(w);
            setCloudOpen(false);
          }}
          themeMode={themeMode}
          loading={cloudLoading}
          maxWords={50}
        />
      </Drawer>

      {/* 条目预览抽屉 (复用 Daily; 订阅外条目详情走全站端点) */}
      <DailyPreviewDrawer
        item={activeItem}
        items={drawerNavItems}
        themeMode={themeMode}
        placement="bottom"
        onClose={() => setActiveItem(null)}
        onNavigate={(next) => setActiveItem(next)}
        fetchDetail={drawerFetchDetail}
      />
    </div>
  );
};

const MobileFeedsDashboardPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = DAILY_THEME_COLORS[themeMode];

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: themeColors.cardBg,
            colorBgElevated: themeColors.cardBg,
            colorBorderSecondary: themeColors.cardBorder,
            colorPrimary: '#2196F3',
            colorText: themeColors.text,
            colorTextSecondary: themeColors.textSecondary,
            colorFillQuaternary: themeColors.inputBg,
            colorBgLayout: themeColors.pageBg,
            borderRadius: 8,
            borderRadiusLG: 12,
          },
        }),
      }}
    >
      <App>
        <MobileFeedsDashboardPageInner />
      </App>
    </ConfigProvider>
  );
};

export default MobileFeedsDashboardPage;
