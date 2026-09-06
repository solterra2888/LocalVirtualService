/**
 * Feeds Dashboard — 按主题聚合的重要信息视图 (/feeds-dashboard).
 *
 * 解决「订阅内容看不完」: 把 auto-tag 产物按「主题 × 重要性」重组。
 *   - 数据: GET /api/feeds-dashboard (feed_topics + feed_topic_daily_items, 读路径零 LLM)
 *   - 条目点击 → DailyPreviewDrawer 就地预览原文 (复用 Daily 的 9 种渲染器),
 *     顶栏 "Open in Feeds" 深链到订阅阅读器
 *   - importance: twitter 来自 tweet_signal LLM 判断, 其余渠道规则评分
 *
 * 后端: backend/api/feeds_dashboard.py + backend/services/feed_topic_aggregation_service.py
 * 生成: task_scheduling feeds_topic_summary 每 2h 聚合; admin 可手动触发 (本页按钮)。
 */

import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  Button,
  Empty,
  message,
  Segmented,
  Skeleton,
  Tag,
  Tooltip,
} from 'antd';
import {
  CaretDownOutlined,
  CaretUpOutlined,
  FireOutlined,
  ReloadOutlined,
  RocketOutlined,
} from '@ant-design/icons';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useDeviceDetection } from '../hooks/useDeviceDetection';
import { useAuthStore } from '../store/authStore';
import { DAILY_THEME_COLORS } from '../components/daily/dailyTheme';
import DailyPreviewDrawer from '../components/daily/DailyPreviewDrawer';
import WordCloudPanel, {
  SelectedWord,
  WordCloudWord,
} from '../components/feedsDashboard/WordCloudPanel';
import {
  getChannelColor,
  getChannelIcon,
} from '../components/feed/feedChannelConfig';
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

// importance 徽标配色 (跨明暗主题可读)
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

/** 视图范围: 我的订阅 / 全站内容池 (localStorage 持久化) */
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

/** 日期范围: 当天 (默认) / 近3天 / 近一周 */
const DATE_RANGE_OPTIONS = [
  { label: 'Today', value: 1 },
  { label: '3 Days', value: 3 },
  { label: '7 Days', value: 7 },
];

/** 页面自动刷新周期 (后端快照每 15 分钟聚合一次) */
const AUTO_REFRESH_INTERVAL_MS = 5 * 60 * 1000;

interface TopicExtra {
  items: FeedsDashboardItem[];
  total: number;
  loading: boolean;
  exhausted: boolean;
}

const FeedsDashboardPage: React.FC = () => {
  const { themeMode, userTimezone } = useDashboardSettings();
  const { isMobile } = useDeviceDetection();
  const isAdmin = !!useAuthStore((s) => s.user?.is_admin);
  const colors = DAILY_THEME_COLORS[themeMode];

  const [rangeDays, setRangeDays] = useState<number>(1);
  const [minImportance, setMinImportance] = useState<FeedsMinImportance | 'all'>('all');
  const [scope, setScope] = useState<FeedsDashboardScope>(loadStoredScope);

  const [data, setData] = useState<FeedsDashboardResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // 词云 (左侧) + 选中词过滤的扁平列表 (右侧切换视图)
  const [cloudWords, setCloudWords] = useState<WordCloudWord[]>([]);
  const [cloudWatchlist, setCloudWatchlist] = useState<WordCloudWord[]>([]);
  const [cloudLoading, setCloudLoading] = useState(false);
  const [selectedWord, setSelectedWord] = useState<SelectedWord | null>(null);
  const [wordResult, setWordResult] = useState<FeedsDashboardItemsResponse | null>(null);
  const [wordLoading, setWordLoading] = useState(false);

  // 展开的「查看全部」附加条目 (topic_id → 追加列表)
  const [extraByTopic, setExtraByTopic] = useState<Record<number, TopicExtra>>({});
  // 折叠的主题 (默认全部展开)
  const [collapsedTopics, setCollapsedTopics] = useState<Set<number>>(new Set());

  // Drawer 预览
  const [activeItem, setActiveItem] = useState<DailyFeedItem | null>(null);
  const [generating, setGenerating] = useState(false);

  // 防重入 (自动刷新与手动刷新并发时丢弃后到者) + 最近刷新时间 (聚焦时判断是否需要刷)
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
        setExtraByTopic({});
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
  // 阅读状态不打扰 — 抽屉打开 / 词过滤中 / 加载中 跳过本次, 等下一个周期。
  useEffect(() => {
    const shouldSkip = () =>
      document.visibilityState !== 'visible' ||
      !!activeItem ||
      !!selectedWord ||
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
  }, [load, activeItem, selectedWord, loading]);

  // 词云点击: 选词 → 拉过滤列表; 再点/清除 → 回主题视图
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
    [rangeDays, minImportance, scope],
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
  }, [rangeDays, minImportance, scope, selectedWord, wordResult]);

  useEffect(() => {
    void load();
  }, [load]);

  // 拉取某主题的追加条目 (点击「查看全部 N 条」)
  const loadMore = useCallback(
    async (topic: FeedsDashboardTopic) => {
      const extra = extraByTopic[topic.topic_id];
      const offset = topic.items.length + (extra?.items.length || 0);
      setExtraByTopic((prev) => ({
        ...prev,
        [topic.topic_id]: {
          items: extra?.items || [],
          total: extra?.total ?? topic.item_count,
          loading: true,
          exhausted: extra?.exhausted || false,
        },
      }));
      try {
        const res = await getFeedsDashboardTopicItems(topic.topic_id, {
          days: rangeDays,
          min_importance: minImportance === 'all' ? undefined : minImportance,
          scope,
          limit: 20,
          offset,
        });
        setExtraByTopic((prev) => {
          const cur = prev[topic.topic_id];
          const merged = (cur?.items || []).concat(res.items);
          return {
            ...prev,
            [topic.topic_id]: {
              items: merged,
              total: res.total,
              loading: false,
              exhausted: !res.has_more,
            },
          };
        });
      } catch (e: any) {
        setExtraByTopic((prev) => ({
          ...prev,
          [topic.topic_id]: { ...prev[topic.topic_id], loading: false },
        }));
        message.error(e?.response?.data?.detail || 'Failed to load more');
      }
    },
    [rangeDays, minImportance, scope, extraByTopic],
  );

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
  }, [load]);

  // Drawer 的 Prev/Next 导航集: 点击条目时按其所属上下文 (主题 / 词过滤) 异步拉取
  // 完整列表 (与列表头计数同口径), 先用单击条目占位开抽屉, 全量列表到达后替换 —
  // 否则抽屉只能翻"页面已加载"的条目 (主题 top5 / 词过滤前30), 与显示条数不一致。
  const [drawerNavItems, setDrawerNavItems] = useState<DailyFeedItem[]>([]);
  const drawerNavSeqRef = useRef(0);

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

  // ─── 渲染 ────────────────────────────────────────────────

  const cardStyle: React.CSSProperties = {
    background: colors.cardBg,
    border: `1px solid ${colors.cardBorder}`,
    borderRadius: 8,
    padding: isMobile ? 12 : 16,
    marginBottom: 12,
  };

  const itemRowStyle: React.CSSProperties = {
    display: 'flex',
    alignItems: 'flex-start',
    gap: 10,
    padding: '9px 8px',
    borderRadius: 6,
    cursor: 'pointer',
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

  // 条目点击: 一律抽屉就地预览; topicId 用于抽屉 Prev/Next 拉取该主题完整列表
  const handleItemClick = (item: FeedsDashboardItem, topicId?: number) => {
    const converted = toDailyFeedItem(item);
    setDrawerNavItems([converted]); // 占位: 先保证抽屉可开、Prev/Next 禁用
    setActiveItem(converted);
    void loadDrawerNav(item, topicId);
  };

  const renderItemRow = (item: FeedsDashboardItem, topicId?: number) => (
    <div
      key={item.id}
      style={itemRowStyle}
      onClick={() => handleItemClick(item, topicId)}
      onMouseEnter={(e) => {
        e.currentTarget.style.background = colors.cardHoverBg;
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.background = 'transparent';
      }}
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
          <span style={{ overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 180 }}>
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
        {item.summary && !isMobile && (
          <div
            style={{
              marginTop: 2,
              fontSize: 12,
              color: colors.textSecondary,
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            {item.summary}
          </div>
        )}
      </div>
    </div>
  );

  const renderTopic = (topic: FeedsDashboardTopic) => {
    const extra = extraByTopic[topic.topic_id];
    const collapsed = collapsedTopics.has(topic.topic_id);
    const visibleCount = topic.items.length + (extra?.items.length || 0);
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
          {topic.name_en && !isMobile && (
            <span style={{ fontSize: 12, color: colors.textMuted }}>{topic.name_en}</span>
          )}
          {topic.category && <Tag style={{ marginInlineEnd: 0 }}>{topic.category}</Tag>}
          {topic.needs_review && <Tag color="orange" style={{ marginInlineEnd: 0 }}>New</Tag>}
          <span style={{ fontSize: 12, color: colors.textSecondary }}>
            {topic.item_count} items · High {topic.high_count} / Med {topic.medium_count}
          </span>
          {topic.top_tickers.slice(0, isMobile ? 3 : 6).map((t) => (
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
          <span style={{ flex: 1 }} />
          {collapsed ? (
            <CaretDownOutlined style={{ color: colors.textSecondary }} />
          ) : (
            <CaretUpOutlined style={{ color: colors.textSecondary }} />
          )}
        </div>

        {/* 条目列表 */}
        {!collapsed && (
          <div style={{ paddingTop: 4 }}>
            {topic.items.map((it) => renderItemRow(it, topic.topic_id))}
            {(extra?.items.length || 0) > 0 &&
              extra?.items.map((it) => renderItemRow(it, topic.topic_id))}
            {extra?.loading && (
              <Skeleton active title={false} paragraph={{ rows: 2 }} style={{ padding: 8 }} />
            )}
            {topic.has_more && !(extra && (extra.loading || extra.exhausted)) && (
              <Button
                type="link"
                size="small"
                style={{ paddingInline: 8 }}
                onClick={() => void loadMore(topic)}
              >
                View all {topic.item_count}{visibleCount > topic.items.length ? ` (showing ${visibleCount})` : ''}
              </Button>
            )}
            {(extra?.items.length || 0) > 0 && !extra?.exhausted && extra && !extra.loading && (
              <Button
                type="link"
                size="small"
                style={{ paddingInline: 8 }}
                onClick={() => void loadMore(topic)}
              >
                Load more
              </Button>
            )}
          </div>
        )}
      </div>
    );
  };

  const notGenerated = !!data && !data.aggregated_at && data.topics.length === 0;

  return (
    <div
      className={isMobile ? undefined : 'feeds-dashboard-scroll-root'}
      style={
        isMobile
          ? { padding: 12, background: colors.pageBg, minHeight: '100%' }
          : {
              // 桌面: 满高双栏 (与左侧 Nav 侧栏等高), 根容器禁滚, 两栏各自内部滚动
              // 配套 App.css: .layout-content:has(.feeds-dashboard-scroll-root) { overflow: hidden }
              height: '100%',
              minHeight: 0,
              overflow: 'hidden',
              display: 'flex',
              flexDirection: 'column',
              background: colors.pageBg,
            }
      }
    >
      {/* 顶部: 标题 + 过滤器 */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: 12,
          flexWrap: 'wrap',
          marginBottom: 12,
          padding: isMobile ? 0 : '16px 24px 0',
        }}
      >
        <span style={{ fontSize: 18, fontWeight: 700, color: colors.text }}>
          Feeds Dashboard
        </span>
        {scope === 'server' && <Tag color="blue">Server-wide</Tag>}
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
        <span style={{ flex: 1 }} />
        <Tooltip title="Refresh">
          <Button
            size="small"
            icon={<ReloadOutlined />}
            onClick={() => void load()}
            loading={loading}
          />
        </Tooltip>
        {isAdmin && (
          <Tooltip title="Manually aggregate the last 7 days (auto-runs every 15 min)">
            <Button
              size="small"
              icon={<RocketOutlined />}
              onClick={() => void handleGenerate()}
              loading={generating}
            >
              Generate Snapshot
            </Button>
          </Tooltip>
        )}
      </div>

      {/* 统计行 */}
      {data && (
        <div
          style={{
            fontSize: 12,
            color: colors.textSecondary,
            marginBottom: 12,
            padding: isMobile ? 0 : '0 24px',
          }}
        >
          {data.topics.length} topics · {data.unique_content_count} items
          {data.aggregated_at
            ? ` · snapshot updated ${formatTime(data.aggregated_at, userTimezone)}`
            : ' · no snapshot for selected range'}
          {' · auto-refreshes every 5 min'}
        </div>
      )}

      {/* 主体: 左词云 + 右内容
          桌面: 满高双栏 (flex:1 + minHeight:0), 两栏各自 overflowY 滚动, 高度与 Nav 侧栏一致
          移动: 上下堆叠, 随页面正常滚动 */}
      <div
        style={{
          display: 'flex',
          gap: 12,
          flexDirection: isMobile ? 'column' : 'row',
          alignItems: isMobile ? 'flex-start' : 'stretch',
          flex: isMobile ? undefined : 1,
          minHeight: isMobile ? undefined : 0,
          padding: isMobile ? 0 : '0 24px 24px',
        }}
      >
        {/* 左: 词云筛选面板 (满高, 内部滚动) */}
        <div
          style={{
            width: isMobile ? '100%' : 320,
            flexShrink: 0,
            ...(isMobile ? {} : { minHeight: 0, overflowY: 'auto' }),
          }}
        >
          <WordCloudPanel
            words={cloudWords}
            watchlistWords={cloudWatchlist}
            selected={selectedWord}
            onSelect={(w) => void handleSelectWord(w)}
            themeMode={themeMode}
            loading={cloudLoading}
            maxWords={isMobile ? 30 : undefined}
            style={isMobile ? undefined : { minHeight: '100%' }}
          />
        </div>

        {/* 右: 主题视图 / 词过滤视图 (满高, 内部滚动) */}
        <div
          style={{
            flex: 1,
            minWidth: 0,
            ...(isMobile ? {} : { minHeight: 0, overflowY: 'auto' }),
          }}
        >
          {loading ? (
            <div style={{ padding: 8 }}>
              <Skeleton active paragraph={{ rows: 10 }} />
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
                  × Clear filter
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
                    ? ' — click "Generate Snapshot" to aggregate now'
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
        </div>
      </div>

      {/* 条目预览抽屉 (复用 Daily; 订阅外条目详情走全站端点) */}
      <DailyPreviewDrawer
        item={activeItem}
        items={drawerNavItems}
        themeMode={themeMode}
        placement={isMobile ? 'bottom' : 'right'}
        onClose={() => setActiveItem(null)}
        onNavigate={(next) => setActiveItem(next)}
        fetchDetail={drawerFetchDetail}
      />
    </div>
  );
};

export default FeedsDashboardPage;
