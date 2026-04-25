/**
 * Daily 页面 (移动端 /daily).
 *
 * 单列卡片信息流, 按日期分组. 顶部固定搜索 + 横向滚动来源 chip.
 * 跳转策略与桌面端一致, 但 Feed 跳转用移动端的深链路由
 * /feeds/article/:subscriptionId/:contentId.
 */

import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { App, Button, ConfigProvider, Empty, Input, Spin, Typography, message, theme as antdTheme } from 'antd';
import { SearchOutlined, SettingOutlined } from '@ant-design/icons';
import { useNavigate } from 'react-router-dom';
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
import SourceBadge from '../components/daily/SourceBadge';
import DailyPresetModal from '../components/daily/DailyPresetModal';
import { SOURCE_BADGE_DISPLAY_ORDER } from '../components/daily/sourceBadgeConfig';
import { DAILY_THEME_COLORS } from '../components/daily/dailyTheme';
import { dateKeyInTz, formatDateLabel } from '../utils/datetime';

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
  const navigate = useNavigate();
  const colors = DAILY_THEME_COLORS[themeMode];

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
  const [presetSubtypes, setPresetSubtypes] = useState<DailySourceSubtype[] | null>(null);
  const [presetModalOpen, setPresetModalOpen] = useState(false);

  const offsetRef = useRef(0);
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const requestSeqRef = useRef(0);
  const loadMoreRef = useRef<() => void>(() => {});

  useEffect(() => {
    const t = window.setTimeout(() => setSearchQuery(searchInput.trim()), SEARCH_DEBOUNCE_MS);
    return () => window.clearTimeout(t);
  }, [searchInput]);

  // 首次挂载: 优先拉 preset, 拉到再加载 feed
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const r = await getDailyPreset();
        if (!cancelled) setPresetSubtypes(r.source_subtypes);
      } catch {
        if (!cancelled) setPresetSubtypes([]);
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
    if (presetSubtypes === null) return; // 等 preset 拉到
    const seq = ++requestSeqRef.current;
    setLoading(true);
    try {
      const [feed, statsResp] = await Promise.all([
        listDailyFeed({ ...baseQuery, limit: PAGE_SIZE, offset: 0 }),
        getDailyStats({ q: baseQuery.q }),
      ]);
      if (seq !== requestSeqRef.current) return;
      setItems(feed.items);
      setTotal(feed.total);
      setHasMore(feed.has_more);
      offsetRef.current = feed.items.length;
      setStats(statsResp.by_source);
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

  const handleCardClick = useCallback(
    (item: DailyFeedItem) => {
      const nav = item.navigate;
      if (nav.type === 'execution' && nav.execution_id) {
        navigate(`/files?execution=${nav.execution_id}`);
        return;
      }
      if (nav.type === 'feed_article' && nav.subscription_id && nav.content_id) {
        // 移动端有现成深链
        navigate(`/feeds/article/${nav.subscription_id}/${nav.content_id}`);
        return;
      }
      if (nav.external_url) {
        window.open(nav.external_url, '_blank', 'noopener,noreferrer');
        return;
      }
      message.info('No navigation target available');
    },
    [navigate]
  );

  const toggleSubtype = useCallback((sub: DailySourceSubtype) => {
    setActiveSubtypes((prev) =>
      prev.includes(sub) ? prev.filter((x) => x !== sub) : [...prev, sub]
    );
  }, []);

  // Preset 保存回调: 同步状态 + 清除落在新 preset 之外的临时 chip 选择
  const handlePresetSaved = useCallback((next: DailySourceSubtype[]) => {
    setPresetSubtypes(next);
    setActiveSubtypes((prev) =>
      next.length === 0 ? prev : prev.filter((s) => next.includes(s))
    );
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
        <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
          <Text style={{ color: colors.textMuted, fontSize: 12 }}>
            {total.toLocaleString()} items
          </Text>
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
        placeholder="Search tickers, themes..."
        value={searchInput}
        onChange={(e) => setSearchInput(e.target.value)}
        style={{
          background: colors.inputBg,
          borderColor: colors.cardBorder,
          color: colors.text,
          marginBottom: 10,
        }}
      />

      {/* 横向滚动 chip 行 */}
      <div
        style={{
          display: 'flex',
          gap: 6,
          overflowX: 'auto',
          paddingBottom: 8,
          marginBottom: 12,
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

      {loading ? (
        <div style={{ textAlign: 'center', padding: '60px 0' }}>
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
              <div
                style={{
                  padding: '4px 10px',
                  background: colors.dateAnchorBg,
                  borderLeft: `3px solid ${colors.dateLine}`,
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

      {/* Preset 配置弹窗 (移动端复用同一组件) */}
      <DailyPresetModal
        open={presetModalOpen}
        value={presetSubtypes ?? []}
        themeMode={themeMode}
        onSaved={handlePresetSaved}
        onCancel={() => setPresetModalOpen(false)}
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
