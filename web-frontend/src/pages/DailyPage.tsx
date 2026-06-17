/**
 * Daily 页面 (桌面端 /daily).
 *
 * 登录后默认落地页. 把 Upload Files 与 Feeds 订阅按时间轴聚合, 卡片视图
 * (参考 AI Notes Daily 的 Summaries 视图).
 *
 * 功能:
 *   - 顶部搜索框 (标题/摘要/正文) + 来源筛选 chip + 实时统计
 *   - 主区按日期 (YYYY-MM-DD) 分组的卡片列表
 *   - 滚动到底部自动加载下一页
 *   - 卡片点击根据 source 跳转: Upload → /files?execution=<id>, Feed → /feeds (传 state)
 *
 * 数据源: GET /api/daily/feed (UNION ALL of file_processing_details + subscription_content)
 * 详见 backend/api/daily.py
 */

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { App, Button, ConfigProvider, Empty, Input, Spin, Tooltip, Typography, message, theme as antdTheme } from 'antd';
import { QuestionCircleOutlined, SearchOutlined, SettingOutlined } from '@ant-design/icons';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import {
  ALL_DAILY_SOURCE_SUBTYPES,
  DailyFeedItem,
  DailySourceCount,
  DailySourceSubtype,
  getDailyPreset,
  getDailyStats,
  listDailyFeed,
} from '../services/daily';
import DailyCard from '../components/daily/DailyCard';
import DailyTimelineItem from '../components/daily/DailyTimelineItem';
import SourceBadge from '../components/daily/SourceBadge';
import DailyPresetModal from '../components/daily/DailyPresetModal';
import DailyPreviewDrawer from '../components/daily/DailyPreviewDrawer';
import DailyDateSidebar from '../components/daily/DailyDateSidebar';
import '../components/daily/dailySubtleScrollbar.css';
import { SOURCE_BADGE_DISPLAY_ORDER } from '../components/daily/sourceBadgeConfig';
import { DAILY_THEME_COLORS } from '../components/daily/dailyTheme';
import { dateKeyInTz, formatDateLabel } from '../utils/datetime';

const { Title, Text } = Typography;

const PAGE_SIZE = 30;
const SEARCH_DEBOUNCE_MS = 350;

interface DateGroup {
  dateKey: string; // YYYY-MM-DD
  dateLabel: string; // April 25, 2026
  items: DailyFeedItem[];
}

// 日期分组 / 标题统一走 utils/datetime, 由用户时区决定 "自然日"
// 防止"服务器在 HK (UTC+8) 时美国用户看到错位 1 天"的问题.
const groupByDate = (items: DailyFeedItem[], tz: string): DateGroup[] => {
  const groups: DateGroup[] = [];
  const map = new Map<string, DateGroup>();
  for (const it of items) {
    const key = dateKeyInTz(it.published_at, tz);
    let g = map.get(key);
    if (!g) {
      g = { dateKey: key, dateLabel: formatDateLabel(it.published_at, tz), items: [] };
      map.set(key, g);
      groups.push(g);
    }
    g.items.push(it);
  }
  return groups;
};

const DailyPageInner: React.FC = () => {
  const { themeMode, userTimezone } = useDashboardSettings();
  const colors = DAILY_THEME_COLORS[themeMode];

  // ─── 状态 ─────────────────────────────────────────────
  const [items, setItems] = useState<DailyFeedItem[]>([]);
  const [stats, setStats] = useState<DailySourceCount[]>([]);
  const [total, setTotal] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);

  const [searchInput, setSearchInput] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [activeSubtypes, setActiveSubtypes] = useState<DailySourceSubtype[]>([]);

  // ─── Preset (per-user persistent filter) ─────────────────────────
  // null = 还没拉到 preset, 暂不发起 feed 请求 (避免首次拉了 "全部" 又被 preset 覆盖)
  // []   = 用户未设置 preset, 等同 "展示全部" (与 backend 语义对齐)
  const [presetSubtypes, setPresetSubtypes] = useState<DailySourceSubtype[] | null>(null);
  const [presetModalOpen, setPresetModalOpen] = useState(false);

  // 抽屉预览 (卡片点击不再跳出, 而是就地预览; 详见 docs §1)
  const [previewItem, setPreviewItem] = useState<DailyFeedItem | null>(null);

  // ─── 日期导航栏 (DailyDateSidebar) ──────────────────────────
  // activeDateKey: 当前视口内第一个可见日期分组的 key（YYYY-MM-DD），
  // 由 IntersectionObserver 监听主区各 .daily-date-anchor 元素维护。
  const [activeDateKey, setActiveDateKey] = useState<string | null>(null);
  const dateAnchorObserverRef = useRef<IntersectionObserver | null>(null);

  // 右侧内容区独立滚动容器：与侧栏完全解耦，滚动条不出现在页面顶级
  const contentScrollRef = useRef<HTMLDivElement | null>(null);

  const offsetRef = useRef(0);
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const requestSeqRef = useRef(0); // 防止过期请求覆盖最新数据
  // 永远指向最新 loadMore，避免因闭包过期导致 observer 失效
  const loadMoreRef = useRef<() => void>(() => {});

  // 搜索防抖
  useEffect(() => {
    const t = window.setTimeout(() => setSearchQuery(searchInput.trim()), SEARCH_DEBOUNCE_MS);
    return () => window.clearTimeout(t);
  }, [searchInput]);

  // 首次挂载: 优先拉用户 preset, 拉到后再触发 feed 加载
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await getDailyPreset();
        if (!cancelled) setPresetSubtypes(r.source_subtypes);
      } catch {
        // 拉不到 preset 不阻塞 Daily 体验, 兜底为空 (= 展示全部)
        if (!cancelled) setPresetSubtypes([]);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // 模式 A: preset 限制范围, chip 临时收窄
  //   - activeSubtypes 非空 → 当前临时筛选 (一定是 preset 子集, 由 chip 行限制保证)
  //   - activeSubtypes 为空 → 用 presetSubtypes (空数组等同 "全部")
  const effectiveSubtypes: DailySourceSubtype[] | undefined = useMemo(() => {
    if (activeSubtypes.length > 0) return activeSubtypes;
    if (presetSubtypes && presetSubtypes.length > 0) return presetSubtypes;
    return undefined;
  }, [activeSubtypes, presetSubtypes]);

  const baseQuery = useMemo(
    () => ({
      q: searchQuery || undefined,
      source_subtypes: effectiveSubtypes,
    }),
    [searchQuery, effectiveSubtypes]
  );

  // ─── 加载首页 + 统计 ─────────────────────────────────
  const loadFirstPage = useCallback(async () => {
    if (presetSubtypes === null) return; // 等 preset 拉到再加载
    const seq = ++requestSeqRef.current;
    setLoading(true);
    try {
      const [feed, statsResp] = await Promise.all([
        listDailyFeed({ ...baseQuery, limit: PAGE_SIZE, offset: 0 }),
        getDailyStats({ q: baseQuery.q }), // 统计不按 subtype 筛, 让用户看清各类总量
      ]);
      if (seq !== requestSeqRef.current) return; // 过期了
      setItems(feed.items);
      setTotal(feed.total);
      setHasMore(feed.has_more);
      offsetRef.current = feed.items.length;
      setStats(statsResp.by_source);
    } catch (e: any) {
      if (seq !== requestSeqRef.current) return;
      message.error(e?.message || 'Failed to load Daily feed');
    } finally {
      if (seq === requestSeqRef.current) setLoading(false);
    }
  }, [baseQuery, presetSubtypes]);

  useEffect(() => {
    loadFirstPage();
  }, [loadFirstPage]);

  // ─── 加载下一页 ──────────────────────────────────────
  const loadMore = useCallback(async () => {
    if (loadingMore || !hasMore) return;
    const seq = requestSeqRef.current;
    setLoadingMore(true);
    try {
      const resp = await listDailyFeed({
        ...baseQuery,
        limit: PAGE_SIZE,
        offset: offsetRef.current,
      });
      if (seq !== requestSeqRef.current) return;
      setItems((prev) => [...prev, ...resp.items]);
      setHasMore(resp.has_more);
      offsetRef.current += resp.items.length;
    } catch {
      // 静默, 顶部 message 已经会被全局拦截器处理
    } finally {
      if (seq === requestSeqRef.current) setLoadingMore(false);
    }
  }, [baseQuery, loadingMore, hasMore]);

  // 保持 ref 始终指向最新的 loadMore（避免 useEffect 因依赖变化频繁重连）
  useEffect(() => {
    loadMoreRef.current = loadMore;
  });

  // IntersectionObserver：items 从空到非空时挂载，root 指向内容滚动容器
  useEffect(() => {
    const el = sentinelRef.current;
    const container = contentScrollRef.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting) loadMoreRef.current();
      },
      { root: container, rootMargin: '400px', threshold: 0 }
    );
    observer.observe(el);
    return () => observer.disconnect();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [items.length > 0]);

  // 滚动兜底：监听内容容器的 scroll 事件
  useEffect(() => {
    const container = contentScrollRef.current;
    if (!container) return;
    const handleScroll = () => {
      const sentinel = sentinelRef.current;
      if (!sentinel) return;
      const containerRect = container.getBoundingClientRect();
      const rect = sentinel.getBoundingClientRect();
      if (rect.top <= containerRect.bottom + 400) {
        loadMoreRef.current();
      }
    };
    container.addEventListener('scroll', handleScroll, { passive: true });
    return () => container.removeEventListener('scroll', handleScroll);
  }, []); // 稳定，只挂载/卸载一次

  // ─── 日期锚点 IntersectionObserver ──────────────────────────
  // 监听主区每个日期分组的 .daily-date-anchor 元素，追踪当前视口最顶可见的日期。
  // rootMargin '0px 0px -70% 0px' → 进入视口上方 30% 时触发 active。
  useEffect(() => {
    if (dateAnchorObserverRef.current) {
      dateAnchorObserverRef.current.disconnect();
    }
    const observer = new IntersectionObserver(
      (entries) => {
        const visible = entries
          .filter((e) => e.isIntersecting)
          .sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top);
        if (visible.length > 0) {
          const key = (visible[0].target as HTMLElement).dataset.dateKey;
          if (key) setActiveDateKey(key);
        }
      },
      { root: contentScrollRef.current, rootMargin: '0px 0px -70% 0px', threshold: 0 }
    );
    dateAnchorObserverRef.current = observer;
    document.querySelectorAll<HTMLElement>('.daily-date-anchor').forEach((el) =>
      observer.observe(el)
    );
    return () => observer.disconnect();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [items.length]);

  // ─── 点击日期侧栏跳转主区 ────────────────────────────────────
  // 用 #daily-scroll-anchor-{dateKey} 定位节点，在列表滚动容器内 scrollTo；
  // 标题+搜索在容器外，锚点相对列表区顶部对齐即可。
  const DESKTOP_SCROLL_ANCHOR_GAP = 8;
  const scrollToAnchor = useCallback((dateKey: string) => {
    const anchor = document.getElementById(`daily-scroll-anchor-${dateKey}`);
    const container = contentScrollRef.current;
    if (!anchor || !container) return false;
    const containerRect = container.getBoundingClientRect();
    const anchorRect = anchor.getBoundingClientRect();
    const target =
      container.scrollTop + anchorRect.top - containerRect.top - DESKTOP_SCROLL_ANCHOR_GAP;
    container.scrollTo({ top: Math.max(0, target), behavior: 'smooth' });
    setActiveDateKey(dateKey);
    return true;
  }, []);

  const handleJumpToDate = useCallback(
    async (dateKey: string) => {
      // 先尝试滚动到已加载的锚点
      if (scrollToAnchor(dateKey)) return;

      // 未命中 → 拉取该日数据补加载
      const seq = ++requestSeqRef.current;
      setLoading(true);
      try {
        const resp = await listDailyFeed({
          ...baseQuery,
          date_from: `${dateKey}T00:00:00Z`,
          date_to: `${dateKey}T23:59:59Z`,
          limit: 100,
          offset: 0,
        });
        if (seq !== requestSeqRef.current) return;
        if (resp.items.length > 0) {
          setItems((prev) => {
            const merged = [...resp.items, ...prev];
            const seen = new Set<string>();
            const deduped = merged.filter((it) => {
              if (seen.has(it.id)) return false;
              seen.add(it.id);
              return true;
            });
            deduped.sort(
              (a, b) =>
                new Date(b.published_at).getTime() - new Date(a.published_at).getTime()
            );
            return deduped;
          });
          // 等 DOM 更新后再滚动
          requestAnimationFrame(() => scrollToAnchor(dateKey));
        }
      } catch {
        // 静默处理
      } finally {
        if (seq === requestSeqRef.current) setLoading(false);
      }
    },
    [baseQuery]
  );

  // ─── 卡片点击跳转 ────────────────────────────────────
  // 卡片点击 → 在 Daily 页面内打开右侧抽屉预览, 不再跳出到 Sources/Feeds.
  // 顶栏 "Open in Sources/Feeds" 按钮保留旧的跳转能力 (DailyPreviewDrawer 内部实现).
  const handleCardClick = useCallback((item: DailyFeedItem) => {
    setPreviewItem(item);
  }, []);

  // ─── 来源 chip 切换 ──────────────────────────────────
  const toggleSubtype = useCallback((sub: DailySourceSubtype) => {
    setActiveSubtypes((prev) => {
      if (prev.includes(sub)) return prev.filter((x) => x !== sub);
      return [...prev, sub];
    });
  }, []);

  const clearFilters = () => {
    setActiveSubtypes([]);
    setSearchInput('');
    setSearchQuery('');
  };

  // ─── Preset 保存回调 ─────────────────────────────────
  // 模式 A 约定: 临时 chip 选择必须落在新 preset 范围内, 否则视为脏选, 清除.
  // (preset 为空数组时无需限制 — 表示 "全部")
  const handlePresetSaved = useCallback((next: DailySourceSubtype[]) => {
    setPresetSubtypes(next);
    // 保存 preset 后清空 chip 临时筛选。否则若此前只点了例如 Twitter，
    // activeSubtypes 仍为 ['twitter']，effectiveSubtypes 只会传 twitter，
    // 新勾选的 podcast 等类型永远不会进 /daily/feed 请求。
    setActiveSubtypes([]);
    setPresetModalOpen(false);
  }, []);

  // ─── 渲染 ────────────────────────────────────────────
  const grouped = useMemo(() => groupByDate(items, userTimezone), [items, userTimezone]);
  const statsMap = useMemo(() => {
    const map = new Map<DailySourceSubtype, number>();
    for (const s of stats) map.set(s.source_subtype, s.count);
    return map;
  }, [stats]);

  // 可见 chip 集合: preset 非空时仅显示 preset 内类型, 否则显示全部 (=未设置)
  const visibleSubtypeOrder = useMemo(() => {
    const base = SOURCE_BADGE_DISPLAY_ORDER.filter((s) =>
      ALL_DAILY_SOURCE_SUBTYPES.includes(s)
    );
    if (!presetSubtypes || presetSubtypes.length === 0) return base;
    return base.filter((s) => presetSubtypes.includes(s));
  }, [presetSubtypes]);

  const presetActive = !!(presetSubtypes && presetSubtypes.length > 0);

  return (
    <div
      className="daily-page-scroll-root"
      style={{
        background: colors.pageBg,
        height: '100%',
        minHeight: 0,
        overflow: 'hidden',
      }}
    >
      {/* 两栏布局：左侧日期导航栏 + 右侧主内容区
          主内容左缘对齐：paddingLeft + 220(sidebar) + gap = 常数（原 32+220+16），避免列表区整体漂移 */}
      <div
        style={{
          maxWidth: 1280,
          margin: '0 auto',
          display: 'flex',
          gap: 50,
          height: '100%',
          overflow: 'hidden',
          padding: '24px 32px 0',
        }}
      >
        {/* 左侧日期导航栏 */}
        <DailyDateSidebar
          activeDateKey={activeDateKey}
          baseQuery={{
            tz: userTimezone,
            source_subtypes: effectiveSubtypes,
            q: searchQuery || undefined,
          }}
          onJumpToDate={handleJumpToDate}
          variant="desktop"
        />

        {/* 右侧：标题+搜索+筛选固定，仅列表区滚动 */}
        <div
          style={{
            flex: 1,
            minWidth: 0,
            height: '100%',
            minHeight: 0,
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden',
          }}
        >
          <div style={{ flexShrink: 0 }}>
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'space-between',
                gap: 16,
                marginBottom: 16,
              }}
            >
              <Title level={3} style={{ margin: 0, color: colors.text, fontWeight: 600 }}>
                Daily
              </Title>
              <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                <Text style={{ color: colors.textMuted, fontSize: 13 }}>
                  {total.toLocaleString()} {total === 1 ? 'item' : 'items'}
                  {(activeSubtypes.length > 0 || searchQuery) && ' (filtered)'}
                </Text>
                <Tooltip
                  title={
                    presetActive
                      ? `Preset: ${presetSubtypes!.length} types · click to edit`
                      : 'Configure Daily preset'
                  }
                  mouseEnterDelay={0.4}
                >
                  <Button
                    type="text"
                    size="small"
                    aria-label="Configure Daily preset"
                    onClick={() => setPresetModalOpen(true)}
                    icon={
                      <SettingOutlined
                        style={{
                          fontSize: 16,
                          color: presetActive ? colors.accent : colors.textMuted,
                        }}
                      />
                    }
                  />
                </Tooltip>
              </div>
            </div>

            <div style={{ marginBottom: 8 }}>
              <Input
                allowClear
                size="large"
                prefix={<SearchOutlined style={{ color: colors.textMuted, fontSize: 14 }} />}
                suffix={
                  <Tooltip
                    title={
                      <div style={{ fontSize: 12, lineHeight: 1.7 }}>
                        <div style={{ fontWeight: 600, marginBottom: 4 }}>搜索语法</div>
                        <div><span style={{ opacity: 0.6 }}>空格</span> → AND，每个词都要出现</div>
                        <div><span style={{ opacity: 0.6 }}>OR（大写）</span> → 任一词命中即可</div>
                        <div><span style={{ opacity: 0.6 }}>"短语"</span> → 精确子串匹配</div>
                        <div style={{ marginTop: 6, borderTop: '1px solid rgba(255,255,255,0.15)', paddingTop: 6, opacity: 0.75 }}>
                          示例：<br />
                          <code style={{ fontSize: 11 }}>美国 访华</code> — 同时含两词<br />
                          <code style={{ fontSize: 11 }}>美国 OR 访华 OR Trump</code> — 含任一<br />
                          <code style={{ fontSize: 11 }}>"Air Force One"</code> — 精确短语
                        </div>
                      </div>
                    }
                    overlayStyle={{ maxWidth: 280 }}
                    placement="bottomRight"
                  >
                    <QuestionCircleOutlined style={{ color: colors.textMuted, cursor: 'pointer', fontSize: 14 }} />
                  </Tooltip>
                }
                placeholder="Search... (space=AND, OR=any, &quot;phrase&quot;=exact)"
                value={searchInput}
                onChange={(e) => setSearchInput(e.target.value)}
                style={{
                  background: colors.inputBg,
                  borderColor: colors.cardBorder,
                  color: colors.text,
                  marginBottom: 10,
                  fontSize: 13,
                }}
              />

              <div
                style={{
                  display: 'flex',
                  flexWrap: 'wrap',
                  alignItems: 'center',
                  gap: 8,
                }}
              >
                {visibleSubtypeOrder.map((sub) => {
                  const cnt = statsMap.get(sub) ?? 0;
                  if (cnt === 0 && !activeSubtypes.includes(sub)) return null;
                  return (
                    <SourceBadge
                      key={sub}
                      subtype={sub}
                      themeMode={themeMode}
                      clickable
                      active={activeSubtypes.includes(sub)}
                      onClick={() => toggleSubtype(sub)}
                    />
                  );
                })}
                {(activeSubtypes.length > 0 || searchQuery) && (
                  <span
                    role="button"
                    tabIndex={0}
                    onClick={clearFilters}
                    style={{
                      fontSize: 12,
                      color: colors.textMuted,
                      cursor: 'pointer',
                      marginLeft: 6,
                      textDecoration: 'underline',
                      userSelect: 'none',
                    }}
                  >
                    Clear
                  </span>
                )}
              </div>
            </div>
          </div>

          <div
            ref={contentScrollRef}
            className={`daily-subtle-scrollbar${themeMode === 'dark' ? ' daily-subtle-scrollbar--dark' : ''}`}
            style={{ flex: 1, minHeight: 0, overflowY: 'auto', paddingBottom: 60 }}
          >
        {/* 内容主区 */}
        {loading ? (
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', minHeight: '100%' }}>
            <Spin size="large" />
          </div>
        ) : items.length === 0 ? (
          <Empty
            description={
              <span style={{ color: colors.textMuted }}>
                {searchQuery
                  ? <>No results for <strong>"{searchQuery}"</strong> — try fewer words, or join terms with <strong>OR</strong></>
                  : activeSubtypes.length
                    ? 'No items match your filters'
                    : 'No items yet — upload a file or subscribe to a feed to start your Daily.'}
              </span>
            }
            style={{ padding: '60px 0' }}
          />
        ) : (
          <>
            {grouped.map((group) => (
              <div key={group.dateKey} style={{ marginBottom: 28 }}>
                {/* 非 sticky 滚动定位锚点，scrollToAnchor() 依此算坐标 */}
                <div id={`daily-scroll-anchor-${group.dateKey}`} style={{ height: 0 }} />
                {/* 日期分隔锚点：sticky 固定在顶部，data-date-key 供 IntersectionObserver 读取 */}
                <div
                  className="daily-date-anchor"
                  data-date-key={group.dateKey}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 12,
                    margin: '0 0 12px',
                    padding: '6px 12px',
                    background: colors.dateAnchorBg,
                    borderLeft: `3px solid ${colors.dateLine}`,
                    borderRadius: 4,
                  }}
                >
                  <Text strong style={{ color: colors.dateAnchorText, fontSize: 14 }}>
                    {group.dateLabel}
                  </Text>
                  <Text style={{ color: colors.textMuted, fontSize: 12 }}>
                    {group.items.length} {group.items.length === 1 ? 'article' : 'articles'}
                  </Text>
                </div>

                {/* 当日卡片列表（左侧时间轴，参考 AIHOT） */}
                <div style={{ display: 'flex', flexDirection: 'column' }}>
                  {group.items.map((it, idx) => (
                    <DailyTimelineItem
                      key={it.id}
                      themeMode={themeMode}
                      publishedAt={it.published_at}
                      isFirst={idx === 0}
                      isLast={idx === group.items.length - 1}
                    >
                      <DailyCard
                        item={it}
                        themeMode={themeMode}
                        onClick={handleCardClick}
                        onTickerClick={(t) => setSearchInput(t.replace(/\.US$/, ''))}
                      />
                    </DailyTimelineItem>
                  ))}
                </div>
              </div>
            ))}

            {/* 滚动加载哨兵 */}
            <div ref={sentinelRef} style={{ height: 24 }} />
            {loadingMore && (
              <div style={{ textAlign: 'center', padding: 16 }}>
                <Spin />
              </div>
            )}
            {!hasMore && items.length > 0 && (
              <div
                style={{
                  textAlign: 'center',
                  color: colors.textMuted,
                  fontSize: 12,
                  padding: '20px 0 40px',
                }}
              >
                — End of feed —
              </div>
            )}
          </>
        )}
          </div>
        </div>
        </div>

      {/* Preset 配置弹窗 */}
      <DailyPresetModal
        open={presetModalOpen}
        value={presetSubtypes ?? []}
        themeMode={themeMode}
        onSaved={handlePresetSaved}
        onCancel={() => setPresetModalOpen(false)}
      />

      {/* 卡片点击预览抽屉 */}
      <DailyPreviewDrawer
        item={previewItem}
        items={items}
        themeMode={themeMode}
        placement="right"
        onClose={() => setPreviewItem(null)}
        onNavigate={(next) => setPreviewItem(next)}
      />
    </div>
  );
};

const DailyPage: React.FC = () => {
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
        <DailyPageInner />
      </App>
    </ConfigProvider>
  );
};

export default DailyPage;
