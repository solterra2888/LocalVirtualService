import React from 'react';
import { Tooltip } from 'antd';
import dayjs, { Dayjs } from 'dayjs';
import type { EarningsCalendarItem } from '../../services/markets';
import './WeekBoard.css';

interface WeekBoardProps {
  /** 财报条目（页面已按窗口拉取，组件内部只展示 weekStart 所在周） */
  data: EarningsCalendarItem[];
  /** 本周周一 */
  weekStart: Dayjs;
  isDark: boolean;
}

const SunIcon: React.FC = () => (
  <svg className="ec-section-icon" viewBox="0 0 24 24" aria-hidden="true">
    <circle cx="12" cy="12" r="4" stroke="currentColor" strokeWidth="1.5" fill="none" />
    <line x1="12" y1="2" x2="12" y2="5" stroke="currentColor" strokeWidth="1.5" />
    <line x1="12" y1="19" x2="12" y2="22" stroke="currentColor" strokeWidth="1.5" />
    <line x1="4.22" y1="4.22" x2="6.34" y2="6.34" stroke="currentColor" strokeWidth="1.5" />
    <line x1="17.66" y1="17.66" x2="19.78" y2="19.78" stroke="currentColor" strokeWidth="1.5" />
    <line x1="2" y1="12" x2="5" y2="12" stroke="currentColor" strokeWidth="1.5" />
    <line x1="19" y1="12" x2="22" y2="12" stroke="currentColor" strokeWidth="1.5" />
    <line x1="4.22" y1="19.78" x2="6.34" y2="17.66" stroke="currentColor" strokeWidth="1.5" />
    <line x1="17.66" y1="6.34" x2="19.78" y2="4.22" stroke="currentColor" strokeWidth="1.5" />
  </svg>
);

const MoonIcon: React.FC = () => (
  <svg className="ec-section-icon" viewBox="0 0 24 24" aria-hidden="true">
    <path
      d="M20 13.5A7.5 7.5 0 0 1 11.5 5 6.5 6.5 0 1 0 20 13.5Z"
      stroke="currentColor"
      strokeWidth="1.5"
      fill="none"
    />
  </svg>
);

const formatEps = (v?: number | null): string =>
  v === null || v === undefined ? '—' : v.toFixed(2);

/** 单张财报卡片：未公布显示预期值，已公布显示实际值 + 惊喜率着色 */
const EarningsCard: React.FC<{ item: EarningsCalendarItem }> = ({ item }) => {
  const beat = item.reported && (item.eps_surprise_percent ?? 0) >= 0;

  const epsLine = item.reported ? (
    <span>
      <span className="actual">
        {formatEps(item.eps_actual)}
        {item.eps_surprise_percent !== null && item.eps_surprise_percent !== undefined && (
          <span className={beat ? 'ec-beat' : 'ec-miss'}>
            {' '}
            {beat ? '▲' : '▼'}
            {Math.abs(item.eps_surprise_percent).toFixed(1)}%
          </span>
        )}
      </span>
    </span>
  ) : (
    <span className="est">
      {item.eps_estimate !== null && item.eps_estimate !== undefined
        ? `est ${item.eps_estimate.toFixed(2)}`
        : 'est —'}
    </span>
  );

  return (
    <Tooltip
      title={`${item.code} — ${item.company_name || ''}${
        item.reported && item.eps_actual !== null && item.eps_actual !== undefined
          ? ` · EPS ${item.eps_actual.toFixed(2)}`
          : ''
      }`}
    >
      <div className="ec-card">
        <div className="ec-logo">
          {item.logo_url ? (
            <img
              src={item.logo_url}
              alt={item.code}
              onError={(e) => {
                e.currentTarget.style.display = 'none';
              }}
            />
          ) : (
            <span className="ec-logo-text">{item.code}</span>
          )}
        </div>
        <div className="ec-ticker">{item.code}</div>
        <div className="ec-eps">{epsLine}</div>
      </div>
    </Tooltip>
  );
};

const WeekSection: React.FC<{
  title: string;
  icon: React.ReactNode;
  items: EarningsCalendarItem[];
}> = ({ title, icon, items }) => (
  <div className={`ec-section ${title === 'Before Open' ? 'before-open' : 'after-close'}`}>
    <div className="ec-section-header">
      {icon}
      <span className="ec-section-title">{title}</span>
    </div>
    {items.length > 0 ? (
      <div className="ec-list">
        {items.map((item) => (
          <EarningsCard key={`${item.code}-${item.report_date}`} item={item} />
        ))}
      </div>
    ) : (
      <div className="ec-empty">
        <span>📋</span>
        <span>No Earnings</span>
      </div>
    )}
  </div>
);

export const WeekBoard: React.FC<WeekBoardProps> = ({ data, weekStart, isDark }) => {
  const todayStr = dayjs().format('YYYY-MM-DD');

  // 周一至周五，每天按盘前/盘后分组（与周报 EarningsCalendarWeek 相同口径）
  const days = Array.from({ length: 5 }, (_, i) => {
    const date = weekStart.add(i, 'day');
    const dateStr = date.format('YYYY-MM-DD');
    const dayItems = data.filter((it) => it.report_date === dateStr);
    const when = (it: EarningsCalendarItem) => (it.before_after_market || '').toUpperCase();
    return {
      key: dateStr,
      dayName: date.format('ddd').toUpperCase(),
      dayNumber: date.format('D'),
      isToday: dateStr === todayStr,
      beforeOpen: dayItems.filter((it) => when(it).includes('BEFORE')),
      afterClose: dayItems.filter((it) => !when(it).includes('BEFORE')),
    };
  });

  return (
    <div className={`ec-board${isDark ? ' dark' : ''}`}>
      {days.map((day) => (
        <div key={day.key} className="ec-day-col">
          <div className={`ec-day-header${day.isToday ? ' is-today' : ''}`}>
            <div className="ec-day-name">{day.dayName}</div>
            <div className="ec-day-number">{day.dayNumber}</div>
            {day.isToday && <span className="ec-today-tag">TODAY</span>}
          </div>
          <WeekSection title="Before Open" icon={<SunIcon />} items={day.beforeOpen} />
          <WeekSection title="After Close" icon={<MoonIcon />} items={day.afterClose} />
        </div>
      ))}
    </div>
  );
};

export default WeekBoard;
