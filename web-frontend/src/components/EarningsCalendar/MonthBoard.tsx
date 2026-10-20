import React from 'react';
import { Tooltip } from 'antd';
import dayjs, { Dayjs } from 'dayjs';
import type { EarningsMonthSummary, EarningsMonthDayTopItem } from '../../services/markets';
import './MonthBoard.css';

interface MonthBoardProps {
  data: EarningsMonthSummary | null;
  /** 月视图当前月份第一天 */
  monthStart: Dayjs;
  isDark: boolean;
  /** 点击某天: 页面跳转到该天的周视图 */
  onDayClick: (date: string) => void;
}

const formatCap = (cap?: number | null): string => {
  if (cap === null || cap === undefined) return '';
  if (cap >= 1e12) return `$${(cap / 1e12).toFixed(1)}T`;
  if (cap >= 1e9) return `$${(cap / 1e9).toFixed(0)}B`;
  if (cap >= 1e6) return `$${(cap / 1e6).toFixed(0)}M`;
  return '';
};

const chipTooltip = (item: EarningsMonthDayTopItem): string => {
  const parts = [item.code, item.company_name || ''];
  const cap = formatCap(item.market_cap);
  if (cap) parts.push(cap);
  if (item.reported) {
    parts.push(`EPS ${item.eps_actual !== null && item.eps_actual !== undefined ? item.eps_actual.toFixed(2) : '—'}`);
    if (item.eps_surprise_percent !== null && item.eps_surprise_percent !== undefined) {
      parts.push(`${item.eps_surprise_percent >= 0 ? '+' : ''}${item.eps_surprise_percent.toFixed(1)}%`);
    }
  } else if (item.eps_estimate !== null && item.eps_estimate !== undefined) {
    parts.push(`est ${item.eps_estimate.toFixed(2)}`);
  }
  return parts.join(' · ');
};

const TopChip: React.FC<{ item: EarningsMonthDayTopItem }> = ({ item }) => (
  <Tooltip title={chipTooltip(item)}>
    <span className={`ecm-chip${item.reported ? ' reported' : ''}`}>
      {item.logo_url ? (
        <img
          src={item.logo_url}
          alt=""
          onError={(e) => {
            e.currentTarget.style.display = 'none';
          }}
        />
      ) : null}
      {item.code}
    </span>
  </Tooltip>
);

export const MonthBoard: React.FC<MonthBoardProps> = ({ data, monthStart, isDark, onDayClick }) => {
  const todayStr = dayjs().format('YYYY-MM-DD');
  const daysByDate = new Map((data?.days ?? []).map((d) => [d.date ?? '', d]));

  // 覆盖整月的周网格: 从本月所在周的周一开始
  const firstCell = (() => {
    const first = monthStart.startOf('day');
    const dow = first.day(); // 0=周日
    return first.subtract(dow === 0 ? 6 : dow - 1, 'day');
  })();
  // 行数按月动态计算（对齐 EarningsHub: 只展示工作日, 周末财报在 Table 视图可见）
  const daysBeforeMonth = monthStart.diff(firstCell, 'day');
  const weeks = Math.ceil((daysBeforeMonth + monthStart.daysInMonth()) / 7);
  const cells = Array.from({ length: weeks * 5 }, (_, i) =>
    firstCell.add(Math.floor(i / 5) * 7 + (i % 5), 'day')
  );

  const maxCount = Math.max(1, ...(data?.days ?? []).map((d) => d.count));
  const weekdays = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'];

  return (
    <div className={`ecm-board${isDark ? ' dark' : ''}`}>
      <div className="ecm-month-total">
        {data ? `${data.total} companies reporting in ${monthStart.format('MMMM YYYY')}` : ''}
      </div>
      <div className="ecm-grid">
        {weekdays.map((w) => (
          <div key={w} className="ecm-weekday">
            {w}
          </div>
        ))}
        {cells.map((cell) => {
          const dateStr = cell.format('YYYY-MM-DD');
          const day = daysByDate.get(dateStr);
          const inMonth = cell.month() === monthStart.month() && cell.year() === monthStart.year();
          const isToday = dateStr === todayStr;
          // 热度底色: 家数越多越深 (最多 22% 透明度, 不压内容)
          const heat = day ? Math.min(0.22, 0.06 + (day.count / maxCount) * 0.18) : 0;

          return (
            <div
              key={dateStr}
              className={[
                'ecm-cell',
                inMonth ? '' : 'out-month',
                isToday ? 'is-today' : '',
                day && inMonth ? 'clickable' : '',
              ].join(' ')}
              style={
                heat > 0
                  ? { background: `rgba(var(--ecm-heat), ${heat.toFixed(3)})` }
                  : undefined
              }
              onClick={day && inMonth ? () => onDayClick(dateStr) : undefined}
            >
              <div className="ecm-cell-header">
                <span className="ecm-day-number">{cell.format('D')}</span>
                {day ? <span className="ecm-count-badge">{day.count}</span> : null}
              </div>
              {day && day.top.length > 0 ? (
                <div className="ecm-chips">
                  {day.top.slice(0, 4).map((item) => (
                    <TopChip key={item.code} item={item} />
                  ))}
                  {day.count > day.top.length ? (
                    <Tooltip title={`${day.count - day.top.length} more · click to see all`}>
                      <span className="ecm-chip ecm-chip-overflow">+{day.count - day.top.length}</span>
                    </Tooltip>
                  ) : null}
                </div>
              ) : null}
            </div>
          );
        })}
      </div>
    </div>
  );
};

export default MonthBoard;
