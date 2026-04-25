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
import { Button, Empty, Input, Spin, Tooltip, Typography, message } from 'antd';
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

const DailyPage: React.FC = () => {
  const { themeMode, userTimezone } = useDashboardSettings();
  const navigate = useNavigate();
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

  // IntersectionObserver：items 从空到非空时挂载，之后保持稳定
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
  }, [items.length > 0]); // 只在"有无数据"切换时重连，不依赖 loadMore 引用

  // 滚动兜底：覆盖 Ant Design Content 内部滚动容器（window.scroll 监听不到）
  useEffect(() => {
    const handleScroll = () => {
      const sentinel = sentinelRef.current;
      if (!sentinel) return;
      const rect = sentinel.getBoundingClientRect();
      if (rect.top <= window.innerHeight + 400) {
        loadMoreRef.current();
      }
    };
    // capture: true → 能捕获任意子容器的 scroll 冒泡
    document.addEventListener('scroll', handleScroll, { capture: true, passive: true });
    return () => document.removeEventListener('scroll', handleScroll, true);
  }, []); // 稳定，只挂载/卸载一次

  // ─── 卡片点击跳转 ────────────────────────────────────
  const handleCardClick = useCallback(
    (item: DailyFeedItem) => {
      const nav = item.navigate;
      if (nav.type === 'execution' && nav.execution_id) {
        navigate(`/files?execution=${nav.execution_id}`);
        return;
      }
      if (nav.type === 'feed_article' && nav.subscription_id && nav.content_id) {
        // 桌面 FeedReaderPage 暂不支持深链 query 参数, 优先把 state 传过去, 失败兜底外链
        navigate('/feeds', {
          state: {
            dailyTarget: {
              subscriptionId: nav.subscription_id,
              contentId: nav.content_id,
            },
          },
        });
        return;
      }
      if (nav.external_url) {
        window.open(nav.external_url, '_blank', 'noopener,noreferrer');
        return;
      }
      message.info('No navigation target available for this item');
    },
    [navigate]
  );

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
    setActiveSubtypes((prev) =>
      next.length === 0 ? prev : prev.filter((s) => next.includes(s))
    );
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
      style={{
        background: colors.pageBg,
        minHeight: '100%',
        padding: '24px 32px 60px',
      }}
    >
      {/* 顶部标题区 */}
      <div style={{ maxWidth: 880, margin: '0 auto' }}>
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
                      color: presetActive ? '#1890ff' : colors.textMuted,
                    }}
                  />
                }
              />
            </Tooltip>
          </div>
        </div>

        {/* 搜索栏 */}
        <Input
          allowClear
          size="large"
          prefix={<SearchOutlined style={{ color: colors.textMuted }} />}
          placeholder="Search tickers, themes, titles or content..."
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
          style={{
            background: colors.inputBg,
            borderColor: colors.cardBorder,
            color: colors.text,
            marginBottom: 14,
          }}
        />

        {/* 来源筛选 chip 行 */}
        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            alignItems: 'center',
            gap: 8,
            marginBottom: 24,
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
                count={cnt}
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

        {/* 内容主区 */}
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
                  : 'No items yet — upload a file or subscribe to a feed to start your Daily.'}
              </span>
            }
            style={{ padding: '60px 0' }}
          />
        ) : (
          <>
            {grouped.map((group) => (
              <div key={group.dateKey} style={{ marginBottom: 28 }}>
                {/* 日期分隔锚点 */}
                <div
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

                {/* 当日卡片列表 */}
                <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
                  {group.items.map((it) => (
                    <DailyCard
                      key={it.id}
                      item={it}
                      themeMode={themeMode}
                      onClick={handleCardClick}
                      onTickerClick={(t) => setSearchInput(t.replace(/\.US$/, ''))}
                    />
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

      {/* Preset 配置弹窗 */}
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

export default DailyPage;
