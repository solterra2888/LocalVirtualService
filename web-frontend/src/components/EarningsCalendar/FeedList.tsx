import React from 'react';
import { Avatar } from 'antd';
import dayjs from 'dayjs';
import type { EarningsFeedItem } from '../../services/markets';
import './FeedList.css';

interface FeedListProps {
  items: EarningsFeedItem[];
  isDark: boolean;
  onSelect: (code: string, reportDate?: string | null) => void;
}

const fmtB = (v?: number | null): string => {
  if (v === null || v === undefined) return '—';
  if (Math.abs(v) >= 1e12) return `$${(v / 1e12).toFixed(2)}T`;
  if (Math.abs(v) >= 1e9) return `$${(v / 1e9).toFixed(2)}B`;
  if (Math.abs(v) >= 1e6) return `$${(v / 1e6).toFixed(0)}M`;
  return `$${v.toFixed(0)}`;
};

const fmtEps = (v?: number | null): string =>
  v === null || v === undefined ? '—' : v.toFixed(2);

/** 相对日期: Today / Tomorrow / Yesterday / 3d ago / in 3d */
const relDate = (d?: string | null): string => {
  if (!d) return '';
  const days = dayjs(d).startOf('day').diff(dayjs().startOf('day'), 'day');
  if (days === 0) return 'Today';
  if (days === 1) return 'Tomorrow';
  if (days === -1) return 'Yesterday';
  if (days < 0) return `${-days}d ago`;
  return `in ${days}d`;
};

const whenLabel = (v?: string | null): string => {
  const s = (v || '').toUpperCase();
  if (s.includes('BEFORE')) return 'BMO';
  if (s.includes('AFTER')) return 'AMC';
  return '';
};

const FeedCard: React.FC<{ item: EarningsFeedItem; isDark: boolean; onSelect: (code: string, reportDate?: string | null) => void }> = ({
  item,
  isDark,
  onSelect,
}) => {
  const up = item.change_percent !== null && item.change_percent !== undefined && item.change_percent >= 0;
  const beat = item.reported && (item.eps_surprise_percent ?? 0) >= 0;
  const when = whenLabel(item.before_after_market);

  return (
    <div
      className="ecf-card"
      style={{ background: isDark ? 'rgba(148,163,184,0.08)' : 'rgba(0,0,0,0.025)' }}
      onClick={() => onSelect(item.code, item.report_date)}
    >
      <div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
        {item.logo_url ? (
          <Avatar src={item.logo_url} size={28} shape="square" />
        ) : (
          <Avatar size={28} shape="square">
            {item.code.charAt(0)}
          </Avatar>
        )}
        <div style={{ minWidth: 0, flex: 1 }}>
          <div style={{ fontWeight: 700, fontSize: 13, lineHeight: 1.2 }}>
            {item.code}
            <span style={{ fontWeight: 400, opacity: 0.6, marginLeft: 6, fontSize: 12 }}>
              {(item.company_name || '').slice(0, 24)}
            </span>
          </div>
          <div style={{ fontSize: 11, opacity: 0.65, marginTop: 1 }}>
            {relDate(item.report_date)}
            {item.report_date ? ` · ${dayjs(item.report_date).format('MMM D')}` : ''}
            {when ? ` · ${when}` : ''}
          </div>
        </div>
        {item.price !== null && item.price !== undefined && (
          <div style={{ textAlign: 'right', flexShrink: 0 }}>
            <div style={{ fontSize: 12, fontWeight: 600 }}>{item.price.toFixed(2)}</div>
            <div style={{ fontSize: 11, color: up ? '#52c41a' : '#f5222d' }}>
              {up ? '+' : ''}
              {item.change_percent?.toFixed(2)}%
            </div>
          </div>
        )}
      </div>

      {/* Results: REV + EPS（对齐 EarningsHub 卡片结构） */}
      <div className="ecf-results">
        {item.revenue !== null && item.revenue !== undefined && (
          <div className="ecf-row">
            <span className="ecf-label">REV</span>
            <span>
              {item.reported ? (
                <>
                  <b>{fmtB(item.revenue)}</b>
                  <span className="ecf-est"> latest q</span>
                </>
              ) : (
                <span className="ecf-est">{fmtB(item.revenue)} last q</span>
              )}
            </span>
          </div>
        )}
        <div className="ecf-row">
          <span className="ecf-label">EPS</span>
          {item.reported ? (
            <span>
              {item.eps_estimate !== null && item.eps_estimate !== undefined && (
                <span className="ecf-est">{fmtEps(item.eps_estimate)}e → </span>
              )}
              <b>{fmtEps(item.eps_actual)}</b>
              {item.eps_surprise_percent !== null && item.eps_surprise_percent !== undefined && (
                <b style={{ marginLeft: 4, color: beat ? '#52c41a' : '#f5222d' }}>
                  {beat ? '+' : ''}
                  {item.eps_surprise_percent.toFixed(1)}%
                </b>
              )}
            </span>
          ) : (
            <span className="ecf-est">
              {item.eps_estimate !== null && item.eps_estimate !== undefined
                ? `est ${fmtEps(item.eps_estimate)}`
                : 'est —'}
            </span>
          )}
        </div>
      </div>
    </div>
  );
};

export const FeedList: React.FC<FeedListProps> = ({ items, isDark, onSelect }) => {
  if (items.length === 0) {
    return (
      <div style={{ padding: '60px 12px', textAlign: 'center', opacity: 0.55, fontSize: 13 }}>
        No earnings in the current window
      </div>
    );
  }
  return (
    <div>
      {items.map((item) => (
        <FeedCard
          key={`${item.code}-${item.report_date}`}
          item={item}
          isDark={isDark}
          onSelect={onSelect}
        />
      ))}
    </div>
  );
};

export default FeedList;
