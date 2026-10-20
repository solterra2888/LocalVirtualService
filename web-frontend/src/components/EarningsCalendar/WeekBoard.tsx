import React, { useState } from 'react';
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
  /** 点击卡片打开公司详情面板 */
  onSelect?: (code: string) => void;
}

/** ticker 哈希 → 色相, 保证同一代码的头像颜色稳定 */
const hueOf = (code: string): number => {
  let h = 0;
  for (let i = 0; i < code.length; i++) {
    h = (h * 31 + code.charCodeAt(i)) % 360;
  }
  return h;
};

/** 无 Logo 时的首字母方块（颜色由 ticker 哈希生成，随主题适配） */
const TickerAvatar: React.FC<{ code: string; isDark: boolean }> = ({ code, isDark }) => {
  const h = hueOf(code);
  const style: React.CSSProperties = isDark
    ? { background: `hsl(${h}, 32%, 30%)`, color: `hsl(${h}, 65%, 78%)` }
    : { background: `hsl(${h}, 60%, 91%)`, color: `hsl(${h}, 55%, 32%)` };
  return (
    <span className="ec-avatar" style={style}>
      {code.charAt(0).toUpperCase()}
    </span>
  );
};

/** 卡片上的显示代码: 去掉 .HK 后缀避免溢出(完整代码在 Tooltip) */
const displayCode = (code: string): string => code.replace(/\.(HK|US)$/i, '');

const whenLabel = (v?: string | null): string => {
  const s = (v || '').toUpperCase();
  if (s.includes('BEFORE')) return 'BMO';
  if (s.includes('AFTER')) return 'AMC';
  return '';
};

/** 单张财报卡片（EarningsHub 风格: 方形 logo + 代码; 已公布加 beat/miss 圆点） */
const EarningsCard: React.FC<{
  item: EarningsCalendarItem;
  isDark: boolean;
  onSelect?: (code: string) => void;
}> = ({ item, isDark, onSelect }) => {
  const [logoFailed, setLogoFailed] = useState(false);
  const beat = item.reported && (item.eps_surprise_percent ?? 0) >= 0;
  const showImg = item.logo_url && !logoFailed;

  const tooltipParts = [item.code, item.company_name || ''];
  const when = whenLabel(item.before_after_market);
  if (when) tooltipParts.push(when);
  if (item.reported) {
    if (item.eps_actual !== null && item.eps_actual !== undefined) {
      tooltipParts.push(`EPS ${item.eps_actual.toFixed(2)}`);
    }
    if (item.eps_estimate !== null && item.eps_estimate !== undefined) {
      tooltipParts.push(`est ${item.eps_estimate.toFixed(2)}`);
    }
    if (item.eps_surprise_percent !== null && item.eps_surprise_percent !== undefined) {
      tooltipParts.push(`${item.eps_surprise_percent >= 0 ? '+' : ''}${item.eps_surprise_percent.toFixed(1)}%`);
    }
  } else if (item.eps_estimate !== null && item.eps_estimate !== undefined) {
    tooltipParts.push(`est ${item.eps_estimate.toFixed(2)}`);
  }

  return (
    <Tooltip title={tooltipParts.join(' · ')}>
      <div
        className={`ec-card${onSelect ? ' clickable' : ''}`}
        onClick={onSelect ? () => onSelect(item.code) : undefined}
      >
        {item.reported && <span className={`ec-dot${beat ? ' beat' : ' miss'}`} />}
        <div className="ec-logo">
          {showImg ? (
            <img
              src={item.logo_url as string}
              alt={item.code}
              onError={() => setLogoFailed(true)}
            />
          ) : (
            <TickerAvatar code={item.code} isDark={isDark} />
          )}
        </div>
        <div className="ec-ticker">{displayCode(item.code)}</div>
      </div>
    </Tooltip>
  );
};

export const WeekBoard: React.FC<WeekBoardProps> = ({ data, weekStart, isDark, onSelect }) => {
  const todayStr = dayjs().format('YYYY-MM-DD');

  // 周一至周五, 每天一个卡片框(不分盘前/盘后, 发布时点在卡片 Tooltip 的 BMO/AMC)
  const days = Array.from({ length: 5 }, (_, i) => {
    const date = weekStart.add(i, 'day');
    const dateStr = date.format('YYYY-MM-DD');
    return {
      key: dateStr,
      dayName: date.format('ddd').toUpperCase(),
      dayNumber: date.format('D'),
      isToday: dateStr === todayStr,
      items: data.filter((it) => it.report_date === dateStr),
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
          <div className="ec-section">
            {day.items.length > 0 ? (
              <div className="ec-list">
                {day.items.map((item) => (
                  <EarningsCard
                    key={`${item.code}-${item.report_date}`}
                    item={item}
                    isDark={isDark}
                    onSelect={onSelect}
                  />
                ))}
              </div>
            ) : (
              <div className="ec-empty">
                <span>📋</span>
                <span>No Earnings</span>
              </div>
            )}
          </div>
        </div>
      ))}
    </div>
  );
};

export default WeekBoard;
