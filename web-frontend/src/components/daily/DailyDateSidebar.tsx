/**
 * DailyDateSidebar — 日期导航栏.
 *
 * 功能:
 *  - 显示最近 N 天（默认 30）的每日内容统计
 *  - 每行：活跃度色块 + 日期标签 + 总数 + per-subtype 迷你彩条
 *  - 点击日期 → 调用 onJumpToDate(dateKey) 让主区平滑滚动到对应锚点
 *  - activeDateKey 高亮当前视口内的日期（由父组件通过 IntersectionObserver 维护）
 *  - 底部 "Show more" 按钮渐进加载更早历史
 *  - variant="mobile"：bottom-sheet 内的紧凑布局
 *
 * 业界对标：Readwise Daily Review 侧栏、GitHub contributions heatmap
 */

import React, { useCallback, useEffect, useRef, useState } from 'react';
import { Button, Skeleton, Tooltip, Typography } from 'antd';
import { CalendarOutlined, ReloadOutlined } from '@ant-design/icons';
import { useDashboardSettings } from '../../contexts/DashboardSettingsContext';
import {
  DailyCalendarDay,
  DailyCalendarResponse,
  DailyCalendarQuery,
  getDailyCalendar,
} from '../../services/daily';
import { DAILY_THEME_COLORS } from './dailyTheme';
import { SOURCE_BADGE_STYLES } from './sourceBadgeConfig';
import { todayInTz, yesterdayInTz } from '../../utils/datetime';
import './dailySubtleScrollbar.css';

const { Text } = Typography;

// ─── 活跃度等级色块（仿 GitHub contributions，0-4 级）─────────────

const ACTIVITY_LEVELS = 5; // 0 = 空白, 1-4 = 逐渐加深

/** 根据当天总数与窗口最大值计算活跃度等级 0-4 */
function activityLevel(count: number, maxTotal: number): number {
  if (count === 0 || maxTotal === 0) return 0;
  return Math.min(4, Math.ceil((count / maxTotal) * 4));
}

const ACTIVITY_COLORS_LIGHT = [
  '#f0f0f0', // 0: 空白
  '#bae7ff', // 1: 很淡
  '#69c0ff', // 2: 中淡
  '#1890ff', // 3: 中深
  '#003a8c', // 4: 深
];

const ACTIVITY_COLORS_DARK = [
  'rgba(255,255,255,0.07)', // 0
  'rgba(33,150,243,0.25)',  // 1
  'rgba(33,150,243,0.45)',  // 2
  'rgba(33,150,243,0.70)',  // 3
  '#2196F3',                // 4
];

// ─── 日期格式化工具 ──────────────────────────────────────────────

function formatSidebarDate(dateStr: string, today: string, yesterday: string): string {
  if (dateStr === today) return 'Today';
  if (dateStr === yesterday) return 'Yesterday';
  // "Mon May 10" style
  const d = new Date(dateStr + 'T00:00:00');
  return d.toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' });
}

// ─── Props ──────────────────────────────────────────────────────

export interface DailyDateSidebarProps {
  /** 当前视口内锚定的日期 key (YYYY-MM-DD)，由父组件 IntersectionObserver 更新 */
  activeDateKey: string | null;
  /** 主区当前过滤条件，侧栏统计与其联动 */
  baseQuery: DailyCalendarQuery;
  /** 点击日期时触发，父组件负责滚动到对应锚点 */
  onJumpToDate: (dateKey: string) => void;
  /** mobile: 全宽紧凑模式（用于 bottom-sheet）；desktop: 默认固定宽度栏 */
  variant?: 'desktop' | 'mobile';
}

const INITIAL_DAYS = 30;
const INCREMENT_DAYS = 30;
const MAX_DAYS = 180;

const DailyDateSidebar: React.FC<DailyDateSidebarProps> = ({
  activeDateKey,
  baseQuery,
  onJumpToDate,
  variant = 'desktop',
}) => {
  const { themeMode, userTimezone } = useDashboardSettings();
  const colors = DAILY_THEME_COLORS[themeMode];
  const isDark = themeMode === 'dark';

  const activityColors = isDark ? ACTIVITY_COLORS_DARK : ACTIVITY_COLORS_LIGHT;

  const [days, setDays] = useState(INITIAL_DAYS);
  const [data, setData] = useState<DailyCalendarResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(false);

  const tz = userTimezone || 'UTC';
  const today = todayInTz(tz);
  const yesterday = yesterdayInTz(tz);

  const fetchCalendar = useCallback(async (numDays: number) => {
    setLoading(true);
    setError(false);
    try {
      const resp = await getDailyCalendar({
        tz: userTimezone || 'UTC',
        days: numDays,
        source_subtypes: baseQuery.source_subtypes,
        q: baseQuery.q,
        tickers: baseQuery.tickers,
      });
      setData(resp);
    } catch {
      setError(true);
    } finally {
      setLoading(false);
    }
  }, [userTimezone, baseQuery.source_subtypes, baseQuery.q, baseQuery.tickers]);

  // 当 baseQuery 或 days 变化时重拉
  useEffect(() => {
    fetchCalendar(days);
  }, [fetchCalendar, days]);

  const handleShowMore = useCallback(() => {
    setDays(prev => Math.min(prev + INCREMENT_DAYS, MAX_DAYS));
  }, []);

  const isMobile = variant === 'mobile';

  // ─── 渲染单天行 ───────────────────────────────────────────────

  const renderDayRow = (day: DailyCalendarDay) => {
    const level = activityLevel(day.total, data?.max_total ?? 1);
    const isActive = activeDateKey === day.date;
    const isEmpty = day.total === 0;
    /** 当天无条目时仍保留「今天」可读性，不与历史空日混用淡化样式 */
    const isTodayRow = day.date === today;
    const fadeEmptyRow = isEmpty && !isTodayRow;
    const label = formatSidebarDate(day.date, today, yesterday);

    const activitySwatchBg =
      isTodayRow && level === 0
        ? isDark
          ? 'rgba(33,150,243,0.28)'
          : '#91caff'
        : activityColors[level];

    const rowStyle: React.CSSProperties = {
      display: 'flex',
      flexDirection: 'column',
      gap: 3,
      padding: isMobile ? '7px 12px' : '6px 10px',
      borderRadius: 7,
      cursor: isEmpty ? 'default' : 'pointer',
      background: isActive
        ? isDark ? 'rgba(33,150,243,0.18)' : '#e6f4ff'
        : 'transparent',
      borderLeft: isActive
        ? `3px solid ${colors.accent}`
        : '3px solid transparent',
      transition: 'background 0.15s, border-color 0.15s',
      opacity: fadeEmptyRow ? 0.45 : 1,
    };

    const topRow = (
      <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
        {/* 活跃度色块 */}
        <div
          style={{
            width: 10,
            height: 10,
            borderRadius: 2,
            flexShrink: 0,
            background: activitySwatchBg,
          }}
        />
        {/* 日期标签 */}
        <Text
          style={{
            fontSize: 12,
            color: isActive ? colors.accent : fadeEmptyRow ? colors.textMuted : colors.text,
            fontWeight: isActive ? 600 : isTodayRow ? 500 : 400,
            flex: 1,
            overflow: 'hidden',
            whiteSpace: 'nowrap',
            textOverflow: 'ellipsis',
          }}
        >
          {label}
        </Text>
        {/* 总数 */}
        {day.total > 0 && (
          <Text
            style={{
              fontSize: 11,
              color: isActive ? colors.accent : colors.textSecondary,
              fontWeight: isActive ? 600 : 400,
              flexShrink: 0,
            }}
          >
            {day.total}
          </Text>
        )}
      </div>
    );

    // 迷你彩条（per-subtype 分布，按 sourceBadgeConfig 颜色）
    const miniBar =
      day.by_source.length > 0 ? (
        <Tooltip
          title={day.by_source.map(s => {
            const cfg = SOURCE_BADGE_STYLES[s.source_subtype];
            return `${cfg?.fullName ?? s.source_subtype}: ${s.count}`;
          }).join(' · ')}
          placement={isMobile ? 'top' : 'right'}
          mouseEnterDelay={0.3}
        >
          <div
            style={{
              display: 'flex',
              height: 4,
              borderRadius: 2,
              overflow: 'hidden',
              marginLeft: 16,
            }}
          >
            {day.by_source.map((s) => {
              const cfg = SOURCE_BADGE_STYLES[s.source_subtype];
              const color = isDark ? cfg?.dark?.bg : cfg?.light?.bg;
              const fgColor = isDark ? cfg?.dark?.fg : cfg?.light?.fg;
              const widthPct = (s.count / day.total) * 100;
              return (
                <div
                  key={s.source_subtype}
                  style={{
                    width: `${widthPct}%`,
                    background: fgColor || color || colors.accent,
                    flexShrink: 0,
                  }}
                />
              );
            })}
          </div>
        </Tooltip>
      ) : null;

    return (
      <div
        key={day.date}
        style={rowStyle}
        onClick={() => onJumpToDate(day.date)}
        role={isEmpty ? undefined : 'button'}
        tabIndex={isEmpty ? -1 : 0}
        onKeyDown={(e) => {
          if (!isEmpty && (e.key === 'Enter' || e.key === ' ')) {
            e.preventDefault();
            onJumpToDate(day.date);
          }
        }}
        aria-current={isActive ? 'date' : undefined}
      >
        {topRow}
        {miniBar}
      </div>
    );
  };

  // ─── 分节：This week / Earlier ────────────────────────────────

  const items = data?.items ?? [];
  const thisWeekItems = items.filter((d) => d.date >= yesterday || d.date === today);
  const earlierItems = items.filter((d) => d.date < yesterday);

  const sectionLabel = (label: string) => (
    <Text
      style={{
        fontSize: 10,
        fontWeight: 600,
        letterSpacing: '0.08em',
        textTransform: 'uppercase',
        color: colors.textMuted,
        display: 'block',
        padding: isMobile ? '8px 12px 2px' : '8px 10px 2px',
      }}
    >
      {label}
    </Text>
  );

  // ─── Sidebar 外层样式 ─────────────────────────────────────────

  const containerStyle: React.CSSProperties = isMobile
    ? {
        width: '100%',
        background: colors.pageBg,
        overflowY: 'auto',
        maxHeight: '70vh',
        paddingBottom: 16,
      }
    : {
        width: 220,
        minWidth: 220,
        flexShrink: 0,
        background: colors.pageBg,
        borderRight: `1px solid ${colors.divider}`,
        overflowY: 'auto',
        paddingBottom: 24,
        height: '100%',
      };

  return (
    <div
      className={`daily-subtle-scrollbar${isDark ? ' daily-subtle-scrollbar--dark' : ''}`}
      style={containerStyle}
    >
      {/* 标题行（仅 desktop 显示）*/}
      {!isMobile && (
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 6,
            padding: '16px 10px 8px',
          }}
        >
          <CalendarOutlined style={{ color: colors.accent, fontSize: 13 }} />
          <Text style={{ fontSize: 12, fontWeight: 600, color: colors.textSecondary }}>
            Timeline
          </Text>
          {loading && (
            <ReloadOutlined
              spin
              style={{ fontSize: 11, color: colors.textMuted, marginLeft: 'auto' }}
            />
          )}
        </div>
      )}

      {/* 错误 / 重试 */}
      {error && (
        <div style={{ padding: '8px 10px' }}>
          <Button
            size="small"
            type="text"
            style={{ fontSize: 11, color: colors.textMuted }}
            onClick={() => fetchCalendar(days)}
          >
            Retry
          </Button>
        </div>
      )}

      {/* 加载骨架 */}
      {loading && !data && (
        <div style={{ padding: '4px 10px' }}>
          {[...Array(8)].map((_, i) => (
            <Skeleton.Input
              key={i}
              active
              size="small"
              style={{ width: '100%', marginBottom: 6, height: 28 }}
            />
          ))}
        </div>
      )}

      {/* 日历列表 */}
      {data && (
        <>
          {thisWeekItems.length > 0 && (
            <>
              {sectionLabel('This week')}
              {thisWeekItems.map(renderDayRow)}
            </>
          )}

          {earlierItems.length > 0 && (
            <>
              {sectionLabel('Earlier')}
              {earlierItems.map(renderDayRow)}
            </>
          )}

          {/* Show more */}
          {days < MAX_DAYS && (
            <div style={{ padding: isMobile ? '8px 12px' : '8px 10px' }}>
              <Button
                type="text"
                size="small"
                onClick={handleShowMore}
                loading={loading}
                style={{
                  fontSize: 11,
                  color: colors.textMuted,
                  padding: '0 4px',
                }}
              >
                Show 30 more days
              </Button>
            </div>
          )}

          {/* Total summary */}
          {data.total_in_window > 0 && (
            <div style={{ padding: isMobile ? '4px 12px 8px' : '4px 10px 8px' }}>
              <Text style={{ fontSize: 10, color: colors.textMuted }}>
                {data.total_in_window.toLocaleString()} items in {data.days} days
              </Text>
            </div>
          )}
        </>
      )}
    </div>
  );
};

export default DailyDateSidebar;
