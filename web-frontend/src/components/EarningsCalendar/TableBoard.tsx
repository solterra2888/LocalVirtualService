import React, { useMemo, useState } from 'react';
import dayjs from 'dayjs';
import type { EarningsCalendarItem } from '../../services/markets';
import './TableBoard.css';

interface TableBoardProps {
  /** 财报条目（页面已按周窗口拉取，组件按 report_date 分组展示） */
  items: EarningsCalendarItem[];
  isDark: boolean;
  /** 点击行打开公司详情面板(附带所点事件的 report_date, 供面板判断该事件是否已公布) */
  onSelect: (code: string, reportDate?: string | null) => void;
}

/** ticker 哈希 → 色相, 保证同一代码的头像颜色稳定（与 WeekBoard 同规则） */
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
    <span className="etb-avatar" style={style}>
      {code.charAt(0).toUpperCase()}
    </span>
  );
};

/** 行内显示代码: 去掉 .HK/.US 后缀(完整代码在 title 提示) */
const displayCode = (code: string): string => code.replace(/\.(HK|US)$/i, '');

const fmtEps = (v?: number | null): string =>
  v === null || v === undefined ? '—' : v.toFixed(2);

/** 发布时点 → 分段: 盘前 / 盘后 / 未指明 */
const sectionOf = (v?: string | null): 'before' | 'after' | 'unspecified' => {
  const s = (v || '').toUpperCase();
  if (s.includes('BEFORE')) return 'before';
  if (s.includes('AFTER')) return 'after';
  return 'unspecified';
};

/** 日内分段定义(顺序即渲染顺序; unspecified 仅在有数据时渲染)
 *  图标加 U+FE0E 强制文本呈现, 防止 macOS 把 ☀/☾ 渲染成彩色 emoji */
const SECTIONS = [
  { key: 'before', icon: '☀︎', label: 'Before Open' },
  { key: 'after', icon: '☾︎', label: 'After Close' },
  { key: 'unspecified', icon: '·', label: 'Unspecified' },
] as const;

type SectionKey = (typeof SECTIONS)[number]['key'];

/** 单行（EarningsHub List 风格紧凑行; 全灰配色: surprise 用 ▲▼ 符号区分, 不用红绿） */
const Row: React.FC<{
  item: EarningsCalendarItem;
  isDark: boolean;
  onSelect: (code: string, reportDate?: string | null) => void;
}> = ({ item, isDark, onSelect }) => {
  const [logoFailed, setLogoFailed] = useState(false);
  const showImg = item.logo_url && !logoFailed;
  const surprise = item.eps_surprise_percent;

  const epsBlock = item.reported ? (
    <>
      {item.eps_estimate !== null && item.eps_estimate !== undefined && (
        <span className="etb-est">{fmtEps(item.eps_estimate)}e → </span>
      )}
      <span className="etb-actual">{fmtEps(item.eps_actual)}</span>
    </>
  ) : (
    <span className="etb-est">est {fmtEps(item.eps_estimate)}</span>
  );

  return (
    <div className="etb-row" onClick={() => onSelect(item.code, item.report_date)}>
      <div className="etb-logo">
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
      <span className="etb-ticker" title={item.code}>
        {displayCode(item.code)}
      </span>
      <span className="etb-name" title={item.company_name || undefined}>
        {item.company_name || '—'}
      </span>
      <div className="etb-eps">{epsBlock}</div>
      <div className="etb-surprise">
        {item.reported && surprise !== null && surprise !== undefined ? (
          <span className="etb-surprise-value">
            {surprise >= 0 ? '▲' : '▼'} {Math.abs(surprise).toFixed(1)}%
          </span>
        ) : (
          <span className="etb-pill">Pending</span>
        )}
      </div>
    </div>
  );
};

/** Table 视图板: 日期分组 + 盘前/盘后分段（结构对齐 EarningsHub List View） */
export const TableBoard: React.FC<TableBoardProps> = ({ items, isDark, onSelect }) => {
  // 按 report_date 升序分组, 日内按盘前/盘后分段, 段内按 ticker 字母序
  const days = useMemo(() => {
    const map = new Map<string, EarningsCalendarItem[]>();
    for (const it of items) {
      if (!it.report_date) continue;
      const arr = map.get(it.report_date);
      if (arr) arr.push(it);
      else map.set(it.report_date, [it]);
    }
    return Array.from(map.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([date, list]) => ({
        date,
        total: list.length,
        reportedCount: list.filter((it) => it.reported).length,
        sections: SECTIONS.map((sec) => ({
          ...sec,
          items: list
            .filter((it) => sectionOf(it.before_after_market) === (sec.key as SectionKey))
            .sort((a, b) => a.code.localeCompare(b.code)),
        })).filter((sec) => sec.items.length > 0),
      }));
  }, [items]);

  const todayStr = dayjs().format('YYYY-MM-DD');

  return (
    <div className={`etb-board ec-scroll-hidden${isDark ? ' dark' : ''}`}>
      {days.map((day) => {
        const isToday = day.date === todayStr;
        return (
          <div key={day.date} className="etb-day">
            <div className={`etb-day-header${isToday ? ' today' : ''}`}>
              <span className="etb-day-title">
                {dayjs(day.date).format('ddd · MMM D').toUpperCase()}
                {isToday && <span className="etb-pill">Today</span>}
              </span>
              <span className="etb-day-stats">
                {day.total} {day.total > 1 ? 'reports' : 'report'}
                {day.reportedCount > 0 ? ` · ${day.reportedCount} reported` : ''}
              </span>
            </div>
            {day.sections.map((sec) => (
              <div key={sec.key} className="etb-section">
                <div className="etb-section-header">
                  <span className="etb-section-icon">{sec.icon}</span>
                  <span>{sec.label}</span>
                  <span className="etb-section-count">· {sec.items.length}</span>
                </div>
                {sec.items.map((item) => (
                  <Row
                    key={`${item.code}-${item.report_date}`}
                    item={item}
                    isDark={isDark}
                    onSelect={onSelect}
                  />
                ))}
              </div>
            ))}
          </div>
        );
      })}
    </div>
  );
};

export default TableBoard;
