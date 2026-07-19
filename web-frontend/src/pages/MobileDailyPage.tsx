/**
 * Daily 页面 (移动端 /daily).
 *
 * 单列卡片信息流, 按日期分组. 标题 + 搜索 + 来源 chip 整块 sticky（顶栏下，有任务时含任务条高度）.
 * 跳转策略与桌面端一致, 但 Feed 跳转用移动端的深链路由
 * /feeds/article/:subscriptionId/:contentId.
 */

import React, { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { App, Button, ConfigProvider, Drawer, Empty, Input, Spin, Typography, message, theme as antdTheme } from 'antd';
import { CalendarOutlined, SearchOutlined, SettingOutlined } from '@ant-design/icons';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import {
  ALL_DAILY_SOURCE_SUBTYPES,
  DailyFeedItem,
  DailySourceCount,
  DailySourceSubtype,
  getDailyPreset,
  getDailyStats,
  listDailyFeed,
  readDailyPresetCache,
  writeDailyPresetCache,
} from '../services/daily';
import DailyCard from '../components/daily/DailyCard';
import SourceBadge from '../components/daily/SourceBadge';
import DailyPresetModal from '../components/daily/DailyPresetModal';
import DailyPreviewDrawer from '../components/daily/DailyPreviewDrawer';
import DailyDateSidebar from '../components/daily/DailyDateSidebar';
import { SOURCE_BADGE_DISPLAY_ORDER } from '../components/daily/sourceBadgeConfig';
import { DAILY_THEME_COLORS } from '../components/daily/dailyTheme';
import { dateKeyInTz, formatDateLabel } from '../utils/datetime';
import '../styles/mobilePageLoading.css';
import { MOBILE_TASK_PROGRESS_BAR_HEIGHT } from '../components/mobile/MobileTaskProgressBar';

const { Title, Text } = Typography;

const PAGE_SIZE = 25;
const SEARCH_DEBOUNCE_MS = 350;

interface DateGroup {
  dateKey: string;
  dateLabel: string;
  items: DailyFeedItem[];
}

// 日期分组按用户时区算"自然日", 与卡片显示时间口径一致.
// 服务器在 HK (UTC+8); 美国用户用 server local 会错位 1 天.
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

const MobileDailyPageInner: React.FC = () => {
  const { themeMode, userTimezone } = useDashboardSettings();
  const { tasks } = useFileProcessing();
  const colors = DAILY_THEME_COLORS[themeMode];

  /** 顶栏 56px + 可选任务进度条，与 MobileLayout / MobileTaskProgressBar 对齐 */
  const mobileStickyTop =
    tasks.size > 0 ? 56 + MOBILE_TASK_PROGRESS_BAR_HEIGHT : 56;
  const mobilePageHeaderRef = useRef<HTMLDivElement | null>(null);
  const [mobileHeaderHeight, setMobileHeaderHeight] = useState(160);

  useLayoutEffect(() => {
    const el = mobilePageHeaderRef.current;
    if (!el) return;
    const measure = () => setMobileHeaderHeight(el.getBoundingClientRect().height);
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, []);

  const [items, setItems] = useState<DailyFeedItem[]>([]);
  const [stats, setStats] = useState<DailySourceCount[]>([]);
  const [total, setTotal] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [loading, setLoading] = useState(false);
  const [loadingMore, setLoadingMore] = useState(false);

  const [searchInput, setSearchInput] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [activeSubtypes, setActiveSubtypes] = useState<DailySourceSubtype[]>([]);

  // Preset (per-user persistent filter) — null = 还没拉到, [] = 未设置 (=全部)
  const [presetSubtypes, setPresetSubtypes] = useState<DailySourceSubtype[] | null>(
    () => readDailyPresetCache(),
  );
  const [presetModalOpen, setPresetModalOpen] = useState(false);

  // 抽屉预览 (卡片点击不再跳转, 而是就地打开 bottom-sheet 预览; 详见 docs §1)
  const [previewItem, setPreviewItem] = useState<DailyFeedItem | null>(null);

  // 日期导航栏 bottom-sheet
  const [calendarOpen, setCalendarOpen] = useState(false);
  const [activeDateKey, setActiveDateKey] = useState<string | null>(null);

  const offsetRef = useRef(0);
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const requestSeqRef = useRef(0);
  const loadMoreRef = useRef<() => void>(() => {});

  useEffect(() => {
    const t = window.setTimeout(() => setSearchQuery(searchInput.trim()), SEARCH_DEBOUNCE_MS);
    return () => window.clearTimeout(t);
  }, [searchInput]);

  // 首次挂载: 有缓存则立即拉 feed；后台刷新 preset
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await getDailyPreset();
        if (cancelled) return;
        writeDailyPresetCache(r.source_subtypes);
        setPresetSubtypes((prev) => {
          // null = "尚未拉到 preset" 与 [] = "已拉到, 用户未设置" 是两种语义,
          // 不能用 `prev ?? []` 合并 —— 否则首次拿到 [] 会判为"无变化"返回 null,
          // loadFirstPage 的 null gate 永不解开, 首页永远不加载.
          if (prev === null) return r.source_subtypes;
          const prevKey = JSON.stringify(prev);
          const nextKey = JSON.stringify(r.source_subtypes);
          return prevKey === nextKey ? prev : r.source_subtypes;
        });
      } catch {
        if (!cancelled && presetSubtypes === null) setPresetSubtypes([]);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // 模式 A: chip 临时收窄优先, 否则套 preset (空数组 = 全部)
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

  const loadFirstPage = useCallback(async () => {
    if (presetSubtypes === null) return;
    const seq = ++requestSeqRef.current;
    setLoading(true);
    try {
      const feed = await listDailyFeed({ ...baseQuery, limit: PAGE_SIZE, offset: 0 });
      if (seq !== requestSeqRef.current) return;
      setItems(feed.items);
      setTotal(feed.total);
      setHasMore(feed.has_more);
      offsetRef.current = feed.items.length;
      getDailyStats({ q: baseQuery.q })
        .then((statsResp) => {
          if (seq === requestSeqRef.current) setStats(statsResp.by_source);
        })
        .catch(() => {});
    } catch (e: any) {
      if (seq !== requestSeqRef.current) return;
      message.error(e?.message || 'Failed to load Daily');
    } finally {
      if (seq === requestSeqRef.current) setLoading(false);
    }
  }, [baseQuery, presetSubtypes]);

  useEffect(() => {
    loadFirstPage();
  }, [loadFirstPage]);

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
      // 静默
    } finally {
      if (seq === requestSeqRef.current) setLoadingMore(false);
    }
  }, [baseQuery, loadingMore, hasMore]);

  useEffect(() => {
    loadMoreRef.current = loadMore;
  });

  useEffect(() => {
    const el = sentinelRef.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting) loadMoreRef.current();
      },
      { rootMargin: '400px', threshold: 0 }
    );
    observer.observe(el);
    return () => observer.disconnect();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [items.length > 0]);

  useEffect(() => {
    const handleScroll = () => {
      const sentinel = sentinelRef.current;
      if (!sentinel) return;
      const rect = sentinel.getBoundingClientRect();
      if (rect.top <= window.innerHeight + 400) {
        loadMoreRef.current();
      }
    };
    document.addEventListener('scroll', handleScroll, { capture: true, passive: true });
    return () => document.removeEventListener('scroll', handleScroll, true);
  }, []);

  // 卡片点击 → 在 Daily 页面内打开底部抽屉预览, 不再跳出到 Sources/Feeds.
  // 顶栏 "Open" 按钮保留旧的跳转能力 (DailyPreviewDrawer 内部实现).
  const handleCardClick = useCallback((item: DailyFeedItem) => {
    setPreviewItem(item);
  }, []);

  // iOS Safari 兼容的 scrollY 读取
  const getScrollY = () =>
    window.scrollY ?? window.pageYOffset ?? document.documentElement.scrollTop ?? 0;

  const scrollToMobileAnchor = useCallback(
    (dateKey: string): boolean => {
      const anchor = document.getElementById(`mobile-scroll-anchor-${dateKey}`);
      if (!anchor) return false;
      const rect = anchor.getBoundingClientRect();
      const offset = mobileStickyTop + mobileHeaderHeight + 8;
      const target = getScrollY() + rect.top - offset;
      window.scrollTo({ top: Math.max(0, target), behavior: 'smooth' });
      setActiveDateKey(dateKey);
      return true;
    },
    [mobileStickyTop, mobileHeaderHeight]
  );

  // 点击日期导航栏 → 关闭 bottom-sheet，滚动到对应日期锚点
  // 若当天数据尚未加载（anchor 不存在），先拉取该日数据再跳转（与桌面端对齐）
  const handleJumpToDate = useCallback((dateKey: string) => {
    setCalendarOpen(false);
    // 等 Drawer 关闭动画结束（300ms）再滚动，避免 rect 被遮挡影响
    setTimeout(async () => {
      // 优先命中已加载的锚点
      if (scrollToMobileAnchor(dateKey)) return;

      // anchor 不存在 → 该日数据未加载，按日期拉取补插入
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
          requestAnimationFrame(() => scrollToMobileAnchor(dateKey));
        }
      } catch {
        // 静默处理
      } finally {
        if (seq === requestSeqRef.current) setLoading(false);
      }
    }, 320);
  }, [baseQuery, scrollToMobileAnchor]);

  const toggleSubtype = useCallback((sub: DailySourceSubtype) => {
    setActiveSubtypes((prev) =>
      prev.includes(sub) ? prev.filter((x) => x !== sub) : [...prev, sub]
    );
  }, []);

  // Preset 保存回调: 同步状态 + 清除落在新 preset 之外的临时 chip 选择
  const handlePresetSaved = useCallback((next: DailySourceSubtype[]) => {
    writeDailyPresetCache(next);
    setPresetSubtypes(next);
    // 与桌面端一致：保存 preset 后清空 chip 临时筛选，避免仍只传部分 source_subtypes
    setActiveSubtypes([]);
    setPresetModalOpen(false);
  }, []);

  const grouped = useMemo(() => groupByDate(items, userTimezone), [items, userTimezone]);
  const statsMap = useMemo(() => {
    const map = new Map<DailySourceSubtype, number>();
    for (const s of stats) map.set(s.source_subtype, s.count);
    return map;
  }, [stats]);

  // chip 行: preset 非空时仅显示 preset 内类型, 否则显示全部
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
      style={{
        background: colors.pageBg,
        minHeight: '100%',
        padding: '12px 12px 40px',
      }}
    >
      {/* 标题 + 搜索 + chip：整块 sticky，滚动时与顶栏（及任务条）对齐 */}
      <div
        ref={mobilePageHeaderRef}
        style={{
          position: 'sticky',
          top: mobileStickyTop,
          zIndex: 20,
          background: colors.pageBg,
          paddingBottom: 8,
          paddingTop: 4,
          marginBottom: 4,
          backdropFilter: 'blur(8px)',
        }}
      >
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            marginBottom: 10,
          }}
        >
          <Title level={4} style={{ margin: 0, color: colors.text, fontWeight: 600 }}>
            Daily
          </Title>
          <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
            <Text style={{ color: colors.textMuted, fontSize: 12 }}>
              {total.toLocaleString()} items
            </Text>
            <Button
              type="text"
              size="small"
              aria-label="Open date navigator"
              onClick={() => setCalendarOpen(true)}
              icon={
                <CalendarOutlined
                  style={{ fontSize: 16, color: activeDateKey ? colors.accent : colors.textMuted }}
                />
              }
            />
            <Button
              type="text"
              size="small"
              aria-label="Configure Daily preset"
              onClick={() => setPresetModalOpen(true)}
              icon={
                <SettingOutlined
                  style={{ fontSize: 16, color: presetActive ? colors.accent : colors.textMuted }}
                />
              }
            />
          </div>
        </div>

        <Input
          allowClear
          prefix={<SearchOutlined style={{ color: colors.textMuted }} />}
          placeholder="Search... (space=AND, OR=any)"
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
          style={{
            background: colors.inputBg,
            borderColor: colors.cardBorder,
            color: colors.text,
            marginBottom: 8,
          }}
        />

        {/* 横向滚动 chip 行 */}
        <div
          style={{
            display: 'flex',
            gap: 6,
            overflowX: 'auto',
            paddingBottom: 4,
            WebkitOverflowScrolling: 'touch',
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
              compact
              active={activeSubtypes.includes(sub)}
              count={cnt}
              onClick={() => toggleSubtype(sub)}
          />
          );
        })}
        </div>
      </div>

      {loading ? (
        <div className="mobile-page-loading-wrap" aria-busy="true" aria-live="polite">
          <Spin size="large" />
        </div>
      ) : items.length === 0 ? (
        <Empty
          description={
            <span style={{ color: colors.textMuted }}>
              {searchQuery || activeSubtypes.length
                ? 'No items match your filters'
                : 'No items yet'}
            </span>
          }
          style={{ padding: '60px 0' }}
        />
      ) : (
        <>
          {grouped.map((group) => (
            <div key={group.dateKey} style={{ marginBottom: 20 }}>
              {/* 非 sticky 滚动定位锚点，handleJumpToDate 依此算坐标 */}
              <div id={`mobile-scroll-anchor-${group.dateKey}`} style={{ height: 0 }} />
              <div
                className="mobile-daily-date-anchor"
                data-date-key={group.dateKey}
                style={{
                  padding: '4px 10px',
                  background: activeDateKey === group.dateKey
                    ? (colors.accent + '22')
                    : colors.dateAnchorBg,
                  borderLeft: `3px solid ${activeDateKey === group.dateKey ? colors.accent : colors.dateLine}`,
                  borderRadius: 4,
                  marginBottom: 8,
                }}
              >
                <Text strong style={{ color: colors.dateAnchorText, fontSize: 13 }}>
                  {group.dateLabel}
                </Text>
                <Text style={{ color: colors.textMuted, fontSize: 11, marginLeft: 8 }}>
                  {group.items.length}
                </Text>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                {group.items.map((it) => (
                  <DailyCard
                    key={it.id}
                    item={it}
                    themeMode={themeMode}
                    onClick={handleCardClick}
                  />
                ))}
              </div>
            </div>
          ))}
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
                fontSize: 11,
                padding: '20px 0',
              }}
            >
              — End of feed —
            </div>
          )}
        </>
      )}

      {/* 日期导航 bottom-sheet */}
      <Drawer
        open={calendarOpen}
        onClose={() => setCalendarOpen(false)}
        placement="bottom"
        height="auto"
        title="Date Navigator"
        styles={{ body: { padding: 0 } }}
      >
        <DailyDateSidebar
          activeDateKey={activeDateKey}
          baseQuery={{
            tz: userTimezone,
            source_subtypes: effectiveSubtypes,
            q: searchQuery || undefined,
          }}
          onJumpToDate={handleJumpToDate}
          variant="mobile"
        />
      </Drawer>

      {/* Preset 配置弹窗 (移动端复用同一组件) */}
      <DailyPresetModal
        open={presetModalOpen}
        value={presetSubtypes ?? []}
        themeMode={themeMode}
        onSaved={handlePresetSaved}
        onCancel={() => setPresetModalOpen(false)}
      />

      {/* 卡片点击预览抽屉 (底部弹层) */}
      <DailyPreviewDrawer
        item={previewItem}
        items={items}
        themeMode={themeMode}
        placement="bottom"
        onClose={() => setPreviewItem(null)}
        onNavigate={(next) => setPreviewItem(next)}
      />
    </div>
  );
};

const MobileDailyPage: React.FC = () => {
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
        <MobileDailyPageInner />
      </App>
    </ConfigProvider>
  );
};

export default MobileDailyPage;
