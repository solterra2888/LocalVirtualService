import React, { useEffect, useState } from 'react';
import {
  Avatar,
  Button,
  Empty,
  Segmented,
  Skeleton,
  Spin,
  Table,
  Tabs,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import { GlobalOutlined, ThunderboltOutlined } from '@ant-design/icons';
import dayjs from 'dayjs';
import {
  Bar,
  BarChart,
  Cell,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip as ChartTooltip,
  XAxis,
  YAxis,
} from 'recharts';
import type {
  EarningsCompanyPanel,
  EarningsPanelCompany,
  EarningsPanelDividendQuarter,
  EarningsPanelHistoryQuarter,
  EarningsPanelRevenueQuarter,
  EarningsRecapResult,
} from '../../services/markets';
import { marketsAPI } from '../../services/markets';
import MarkdownRenderer from '../MarkdownRenderer';
import './CompanyPanel.css';

const { Text, Paragraph } = Typography;

interface CompanyPanelProps {
  code: string | null;
  data: EarningsCompanyPanel | null;
  loading: boolean;
  isDark: boolean;
  /** 用户所点财报事件的 report_date; 未来日期(未公布)时隐藏 AI Recap 区块 */
  reportDate?: string | null;
}

const fmtCap = (v?: number | null): string => {
  if (v === null || v === undefined) return '—';
  if (v >= 1e12) return `$${(v / 1e12).toFixed(2)}T`;
  if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `$${(v / 1e6).toFixed(0)}M`;
  return `$${v}`;
};

const fmtNum = (v?: number | null, digits = 2): string =>
  v === null || v === undefined ? '—' : v.toFixed(digits);

const fmtPct = (v?: number | null, digits = 1): string =>
  v === null || v === undefined ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(digits)}%`;

/** 量的缩写: 股数/成交量 → 12.2M / 345.1K */
const fmtQty = (v?: number | null): string => {
  if (v === null || v === undefined) return '—';
  if (Math.abs(v) >= 1e9) return `${(v / 1e9).toFixed(2)}B`;
  if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (Math.abs(v) >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
  return `${v}`;
};

/** 比率型同比(0.164 → +16.4%) */
const fmtYoy = (v?: number | null): string =>
  v === null || v === undefined ? '' : `${v >= 0 ? '+' : ''}${(v * 100).toFixed(1)}%`;

/** 财季标签: fiscal_period(如 2026-09-30) → "Q4'26" */
const quarterLabel = (fp?: string | null): string => {
  if (!fp) return '-';
  const d = dayjs(fp);
  if (!d.isValid()) return fp;
  return `Q${Math.floor(d.month() / 3) + 1}'${d.format('YY')}`;
};

const whenLabel = (v?: string | null): string => {
  const s = (v || '').toUpperCase();
  if (s.includes('BEFORE')) return 'BMO';
  if (s.includes('AFTER')) return 'AMC';
  return '';
};

const SectionTitle: React.FC<{ children: React.ReactNode }> = ({ children }) => (
  <div style={{ fontWeight: 600, fontSize: 13, margin: '18px 0 8px' }}>{children}</div>
);

/** EPS 历史: 预估(灰) vs 实际(beat绿/miss红), 未来季度只画预估值 */
const EpsChart: React.FC<{ history: EarningsPanelHistoryQuarter[]; isDark: boolean }> = ({
  history,
  isDark,
}) => {
  const data = history.map((q) => ({
    name: quarterLabel(q.fiscal_period),
    estimate: q.eps_estimate ?? 0,
    actual: q.eps_actual ?? 0,
    reported: q.eps_actual !== null && q.eps_actual !== undefined,
    beat: (q.surprise_percent ?? 0) >= 0,
  }));
  const axisColor = isDark ? 'rgba(255,255,255,0.45)' : 'rgba(0,0,0,0.45)';

  return (
    <ResponsiveContainer width="100%" height={200}>
      <BarChart data={data} barGap={2} margin={{ top: 4, right: 4, bottom: 0, left: -18 }}>
        <CartesianGrid stroke={isDark ? '#364152' : '#f0f0f0'} vertical={false} />
        <XAxis dataKey="name" tick={{ fontSize: 10, fill: axisColor }} interval={0} angle={-40} textAnchor="end" height={44} />
        <YAxis tick={{ fontSize: 10, fill: axisColor }} />
        <ChartTooltip
          formatter={(value: number | string, name: string) => [Number(value).toFixed(2), name]}
        />
        <Bar dataKey="estimate" name="Est" fill={isDark ? '#4b5563' : '#d9d9d9'} radius={[2, 2, 0, 0]} />
        <Bar dataKey="actual" name="Actual" radius={[2, 2, 0, 0]}>
          {data.map((d, i) => (
            <Cell
              key={i}
              fill={!d.reported ? 'transparent' : d.beat ? '#52c41a' : '#f5222d'}
              stroke={!d.reported ? 'transparent' : undefined}
            />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
};

/** 季度营收柱状图（币种随报表本币, 非美元标注币种代码） */
const RevChart: React.FC<{
  history: EarningsPanelRevenueQuarter[];
  currency?: string | null;
  isDark: boolean;
}> = ({ history, currency, isDark }) => {
  const isUsd = !currency || currency === 'USD' || currency === '$';
  const fmtRev = (v: number): string => {
    const n =
      Math.abs(v) >= 1e12
        ? `${(v / 1e12).toFixed(2)}T`
        : Math.abs(v) >= 1e9
          ? `${(v / 1e9).toFixed(2)}B`
          : `${(v / 1e6).toFixed(0)}M`;
    return isUsd ? `$${n}` : `${n} ${currency}`;
  };
  const data = history.map((q) => ({
    name: quarterLabel(q.period),
    value: q.revenue ?? 0,
  }));
  const axisColor = isDark ? 'rgba(255,255,255,0.45)' : 'rgba(0,0,0,0.45)';

  return (
    <ResponsiveContainer width="100%" height={200}>
      <BarChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: -12 }}>
        <CartesianGrid stroke={isDark ? '#364152' : '#f0f0f0'} vertical={false} />
        <XAxis dataKey="name" tick={{ fontSize: 10, fill: axisColor }} interval={0} angle={-40} textAnchor="end" height={44} />
        <YAxis
          tick={{ fontSize: 10, fill: axisColor }}
          tickFormatter={(v: number) => (isUsd ? `$${v / 1e9}B` : `${v / 1e9}B`)}
        />
        <ChartTooltip formatter={(value: number | string) => [fmtRev(Number(value)), 'Revenue']} />
        <Bar dataKey="value" radius={[2, 2, 0, 0]}>
          {data.map((d, i) => (
            <Cell key={i} fill={i === data.length - 1 ? (isDark ? '#4096ff' : '#1890ff') : isDark ? '#2f5da8' : '#91caff'} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
};

/** 季度股息支付柱状图（cash flow 口径, 币种随报表本币） */
const DivChart: React.FC<{
  history: EarningsPanelDividendQuarter[];
  currency?: string | null;
  isDark: boolean;
}> = ({ history, currency, isDark }) => {
  const isUsd = !currency || currency === 'USD' || currency === '$';
  const fmtDiv = (v: number): string => {
    const n =
      Math.abs(v) >= 1e9
        ? `${(v / 1e9).toFixed(2)}B`
        : Math.abs(v) >= 1e6
          ? `${(v / 1e6).toFixed(0)}M`
          : `${v}`;
    return isUsd ? `$${n}` : `${n} ${currency}`;
  };
  const data = history.map((q) => ({
    name: quarterLabel(q.period),
    value: q.amount ?? 0,
  }));
  const axisColor = isDark ? 'rgba(255,255,255,0.45)' : 'rgba(0,0,0,0.45)';

  return (
    <ResponsiveContainer width="100%" height={200}>
      <BarChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: -12 }}>
        <CartesianGrid stroke={isDark ? '#364152' : '#f0f0f0'} vertical={false} />
        <XAxis dataKey="name" tick={{ fontSize: 10, fill: axisColor }} interval={0} angle={-40} textAnchor="end" height={44} />
        <YAxis
          tick={{ fontSize: 10, fill: axisColor }}
          tickFormatter={(v: number) => (isUsd ? `$${v / 1e9}B` : `${v / 1e9}B`)}
        />
        <ChartTooltip formatter={(value: number | string) => [fmtDiv(Number(value)), 'Dividends Paid']} />
        <Bar dataKey="value" radius={[2, 2, 0, 0]}>
          {data.map((d, i) => (
            <Cell
              key={i}
              fill={
                i === data.length - 1
                  ? isDark
                    ? '#4096ff'
                    : '#1890ff'
                  : isDark
                    ? '#1e7f9c'
                    : '#87e8de'
              }
            />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  );
};

export const CompanyPanel: React.FC<CompanyPanelProps> = ({ code, data, loading, isDark, reportDate }) => {
  const [chartMode, setChartMode] = useState<'eps' | 'revenue'>('eps');
  const [recap, setRecap] = useState<EarningsRecapResult | null>(null);
  const [recapLoading, setRecapLoading] = useState(false);

  // Earnings Tab 柱状图下方的子分区(对齐 EarningsHub): Overview=最新一期+AI分析 / History=历史数据
  const [earnSection, setEarnSection] = useState<'overview' | 'history'>('overview');
  const [activeTab, setActiveTab] = useState<string>('earnings');

  // 切换 ticker 时重置 Recap 状态（Tab 保持, 便于连续对比同一维度）
  useEffect(() => {
    setRecap(null);
    setRecapLoading(false);
  }, [code]);

  const fetchRecap = async () => {
    if (!code || recapLoading) return;
    setRecapLoading(true);
    try {
      const result = await marketsAPI.generateEarningsRecap(code);
      setRecap(result);
    } catch {
      setRecap({ status: 'error', code, message: '请求失败, 请稍后重试' });
    } finally {
      setRecapLoading(false);
    }
  };

  if (loading && !data) {
    return (
      <div style={{ padding: 8 }}>
        <Skeleton active avatar paragraph={{ rows: 6 }} />
      </div>
    );
  }
  if (!data || !data.found) {
    return (
      <Empty
        style={{ padding: '80px 0' }}
        description={code ? `No fundamentals data for ${code}` : 'Select a ticker'}
      />
    );
  }

  const c = (data.company ?? {}) as EarningsPanelCompany;
  const h = data.highlights ?? null;
  const next = data.next_earnings ?? null;
  const trend = data.earnings_trend ?? null;
  const ratings = data.analyst_ratings ?? null;
  const tech = data.technicals ?? null;
  const price = data.price ?? null;
  const history = data.earnings_history ?? [];
  const revenueHistory = data.revenue_history ?? [];

  // 最新已公布季度(取有 actual 的最后一行, 跳过只有预估的未来季) + 对应营收, 供 Overview 结果卡片
  const latestQ = history.length
    ? [...history].reverse().find((q) => q.eps_actual !== null && q.eps_actual !== undefined) ?? null
    : null;
  const latestBeat = (latestQ?.surprise_percent ?? 0) >= 0;
  const latestRev = latestQ
    ? revenueHistory.find((r) => r.period === latestQ.fiscal_period) ?? null
    : null;
  const dividendHistory = data.dividend_history ?? [];
  const up = price?.change_percent !== null && price?.change_percent !== undefined && price.change_percent >= 0;

  const ratingTotal =
    ratings
      ? (ratings.strong_buy ?? 0) + (ratings.buy ?? 0) + (ratings.hold ?? 0) + (ratings.sell ?? 0) + (ratings.strong_sell ?? 0)
      : 0;

  const ratingSegments = ratings
    ? [
        { label: 'Strong Buy', count: ratings.strong_buy ?? 0, color: '#389e0d' },
        { label: 'Buy', count: ratings.buy ?? 0, color: '#73d13d' },
        { label: 'Hold', count: ratings.hold ?? 0, color: '#d9d9d9' },
        { label: 'Sell', count: ratings.sell ?? 0, color: '#ff7875' },
        { label: 'Strong Sell', count: ratings.strong_sell ?? 0, color: '#cf1322' },
      ]
    : [];

  // 股息 TTM 汇总（最近 4 季 vs 之前 4 季）
  const divAmounts = dividendHistory.map((d) => d.amount ?? 0);
  const divTtm = divAmounts.slice(-4).reduce((a, b) => a + b, 0);
  const divPriorTtm = divAmounts.length >= 8 ? divAmounts.slice(-8, -4).reduce((a, b) => a + b, 0) : null;

  // ── 各 Tab 内容 ────────────────────────────────────────────────

  const earningsTab = (
    <>
      {/* 下次财报 */}
      <SectionTitle>Next Earnings</SectionTitle>
      {next?.report_date ? (
        <div>
          <div style={{ display: 'flex', gap: 16, alignItems: 'baseline' }}>
            <span style={{ fontSize: 20, fontWeight: 700 }}>
              {dayjs(next.report_date).format('ddd, MMM D, YYYY')}
            </span>
            {whenLabel(next.before_after_market) && (
              <Tag color={whenLabel(next.before_after_market) === 'BMO' ? 'orange' : 'geekblue'}>
                {whenLabel(next.before_after_market)}
              </Tag>
            )}
          </div>
          {(next.eps_estimate != null || next.revenue_estimate != null) && (
            <Text type="secondary" style={{ fontSize: 12 }}>
              {next.eps_estimate != null && <>est EPS {next.eps_estimate.toFixed(2)}</>}
              {next.eps_estimate != null && next.revenue_estimate != null && ' · '}
              {next.revenue_estimate != null && <>est REV {fmtCap(next.revenue_estimate)}</>}
            </Text>
          )}
        </div>
      ) : (
        <Text type="secondary">No upcoming earnings date in calendar</Text>
      )}

      {/* EPS / Revenue 历史图表 */}
      {(history.length > 0 || revenueHistory.length > 0) && (
        <>
          <div className="ecp-chart-header">
            <span className="ecp-chart-title">
              Quarterly History
              {chartMode === 'revenue' && data.revenue_currency && data.revenue_currency !== 'USD' && (
                <span className="ecp-currency-tag">({data.revenue_currency})</span>
              )}
            </span>
            {history.length > 0 && revenueHistory.length > 0 && (
              <Segmented
                size="small"
                options={[
                  { label: 'EPS', value: 'eps' },
                  { label: 'Revenue', value: 'revenue' },
                ]}
                value={chartMode}
                onChange={(v) => setChartMode(v as 'eps' | 'revenue')}
              />
            )}
          </div>
          {chartMode === 'eps' && (data.eps_est_quarters ?? 0) > 0 && (
            <div className="ecp-beat-note">
              Beat estimates <b>{data.eps_beat_count}</b> of {data.eps_est_quarters} quarters
            </div>
          )}
          {chartMode === 'eps' && history.length > 0 ? (
            <EpsChart history={history} isDark={isDark} />
          ) : revenueHistory.length > 0 ? (
            <RevChart history={revenueHistory} currency={data.revenue_currency} isDark={isDark} />
          ) : null}
        </>
      )}

      {/* 柱状图下方分区(对齐 EarningsHub): Overview=最新一期结果卡片+AI分析 / History=历史数据 */}
      {history.length > 0 && (
        <Segmented
          size="small"
          style={{ marginTop: 14 }}
          options={[
            { label: 'Overview', value: 'overview' },
            { label: 'History', value: 'history' },
          ]}
          value={earnSection}
          onChange={(v) => setEarnSection(v as 'overview' | 'history')}
        />
      )}

      {earnSection === 'overview' && history.length > 0 && (
        <>
          {/* 最新一期 Earnings 结果卡片 */}
          {latestQ && latestQ.eps_actual !== null && latestQ.eps_actual !== undefined ? (
            <div className={`ecp-latest-card${latestBeat ? ' beat' : ' miss'}`}>
              <div className="ecp-latest-head">
                <span className="ecp-latest-q">{quarterLabel(latestQ.fiscal_period)}</span>
                <span className="ecp-latest-date">
                  {latestQ.report_date ? dayjs(latestQ.report_date).format('MMM D, YY') : ''}
                </span>
              </div>
              <div className="ecp-latest-row">
                <span className="ecp-latest-label">EPS</span>
                <span className="ecp-latest-big">{latestQ.eps_actual.toFixed(2)}</span>
                {latestQ.eps_estimate != null && (
                  <span className="ecp-latest-sub">vs est {latestQ.eps_estimate.toFixed(2)}</span>
                )}
                {latestQ.surprise_percent != null && (
                  <span
                    className="ecp-latest-pct"
                    style={{ color: latestBeat ? '#16a34a' : '#dc2626' }}
                  >
                    {latestBeat ? '+' : ''}
                    {latestQ.surprise_percent.toFixed(1)}%
                  </span>
                )}
              </div>
              {latestRev?.revenue != null && (
                <div className="ecp-latest-row">
                  <span className="ecp-latest-label">REV</span>
                  <span className="ecp-latest-mid">{fmtCap(latestRev.revenue)}</span>
                </div>
              )}
            </div>
          ) : (
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginTop: 10 }}>
              No reported earnings yet
            </Text>
          )}

          {/* AI Recap: 按需抓取 transcript 并生成分析（美股 only）
              所点事件在未来(未公布)时隐藏 —— 此时"最新电话会"只是上一季旧内容 */}
          {code && !code.includes('.') && (!reportDate || reportDate <= dayjs().format('YYYY-MM-DD')) && (
        <>
          <SectionTitle>
            AI Recap{' '}
            <Tooltip title="AI generated, may be inaccurate — 基于财报电话会 transcript 自动生成">
              <Tag style={{ fontSize: 10, lineHeight: '16px', padding: '0 4px' }}>AI</Tag>
            </Tooltip>
          </SectionTitle>
          {!recap && !recapLoading && (
            <Button
              className="app-ctl-btn"
              icon={<ThunderboltOutlined />}
              onClick={fetchRecap}
              style={{ width: '100%' }}
            >
              生成最新电话会 Recap
            </Button>
          )}
          {recapLoading && (
            <div style={{ padding: '18px 0', textAlign: 'center' }}>
              <Spin />
              <div style={{ marginTop: 10, fontSize: 12, opacity: 0.65 }}>
                正在抓取 transcript 并分析（约 10-30 秒）...
              </div>
            </div>
          )}
          {recap && !recapLoading && (
            <div>
              <div
                style={{
                  display: 'flex',
                  gap: 6,
                  alignItems: 'center',
                  flexWrap: 'wrap',
                  marginBottom: 6,
                  fontSize: 11,
                }}
              >
                {recap.period && <Tag color="blue">{recap.period}</Tag>}
                {recap.call_date && (
                  <Text type="secondary">{dayjs(recap.call_date).format('MMM D, YYYY')}</Text>
                )}
                {recap.cached && <Tag>cached</Tag>}
                {recap.source_url && (
                  <a href={recap.source_url} target="_blank" rel="noreferrer" style={{ fontSize: 11 }}>
                    transcript ↗
                  </a>
                )}
              </div>
              {recap.stale && (
                <div
                  style={{
                    fontSize: 11,
                    color: '#d46b08',
                    background: isDark ? 'rgba(250,173,20,0.08)' : '#fff7e6',
                    border: `1px solid ${isDark ? 'rgba(250,173,20,0.25)' : '#ffd591'}`,
                    borderRadius: 6,
                    padding: '4px 8px',
                    marginBottom: 8,
                  }}
                >
                  数据源未覆盖最新季度：这是 {recap.period} 的电话会
                  {recap.latest_report_date
                    ? `，最近已公布财报为 ${dayjs(recap.latest_report_date).format('YYYY-MM-DD')}`
                    : ''}
                  ，点"重新获取"可在新 transcript 上线后自动更新
                </div>
              )}
              {recap.status === 'ok' && recap.recap_md ? (
                <MarkdownRenderer
                  content={recap.recap_md}
                  variant={isDark ? 'dark' : 'light'}
                  style={{ fontSize: 12 }}
                />
              ) : (
                <Text type="secondary" style={{ fontSize: 12 }}>
                  {recap.message || '未找到 transcript'}
                </Text>
              )}
              <Button
                size="small"
                type="link"
                onClick={fetchRecap}
                style={{ padding: 0, marginTop: 4, fontSize: 11 }}
              >
                重新获取
              </Button>
            </div>
          )}
        </>
      )}
        </>
      )}

      {/* History 分区: 全部历史季度 */}
      {earnSection === 'history' && history.length > 0 && (
        <Table
          size="small"
          pagination={false}
          dataSource={[...history].reverse()}
          rowKey={(r) => `${r.report_date}-${r.fiscal_period}`}
          style={{ marginTop: 12 }}
        >
          <Table.Column
            title="Quarter"
            dataIndex="fiscal_period"
            render={(v: string) => quarterLabel(v)}
          />
          <Table.Column
            title="Date"
            dataIndex="report_date"
            render={(v: string) => (v ? dayjs(v).format('MMM D, YY') : '—')}
          />
          <Table.Column<EarningsPanelHistoryQuarter>
            title="EPS"
            key="eps"
            render={(_, r) =>
              r.eps_actual !== null && r.eps_actual !== undefined ? (
                <span>{r.eps_actual.toFixed(2)}</span>
              ) : (
                <span style={{ opacity: 0.55 }}>{r.eps_estimate?.toFixed(2) ?? '—'}e</span>
              )
            }
          />
          <Table.Column<EarningsPanelHistoryQuarter>
            title="Surprise"
            key="surprise"
            align="right"
            render={(_, r) => {
              if (r.surprise_percent === null || r.surprise_percent === undefined) {
                return <Text type="secondary">—</Text>;
              }
              const beat = r.surprise_percent >= 0;
              return (
                <Text strong style={{ color: beat ? '#52c41a' : '#f5222d' }}>
                  {beat ? '+' : ''}
                  {r.surprise_percent.toFixed(1)}%
                </Text>
              );
            }}
          />
        </Table>
      )}
    </>
  );

  const infoTab = (
    <>
      {/* 关键指标（对齐 EarningsHub Highlights: 估值 + TTM 经营 + 股息） */}
      <div className="ecp-stats">
        {[
          { label: 'Mkt Cap', value: fmtCap(h?.market_cap), sub: null as string | null, subUp: null as boolean | null },
          { label: 'P/E', value: fmtNum(h?.pe_ratio, 1), sub: null, subUp: null },
          { label: 'Fwd P/E', value: fmtNum(h?.forward_pe, 1), sub: null, subUp: null },
          { label: 'P/S', value: fmtNum(h?.ps_ratio, 1), sub: null, subUp: null },
          { label: 'PEG', value: fmtNum(h?.peg_ratio, 1), sub: null, subUp: null },
          { label: 'Target', value: fmtNum(h?.target_price), sub: null, subUp: null },
          {
            label: 'REV TTM',
            value: fmtCap(h?.revenue_ttm),
            sub: fmtYoy(h?.revenue_growth_yoy) || null,
            subUp: (h?.revenue_growth_yoy ?? 0) >= 0,
          },
          {
            label: 'EPS TTM',
            value: fmtNum(h?.eps_ttm),
            sub: fmtYoy(h?.earnings_growth_yoy) || null,
            subUp: (h?.earnings_growth_yoy ?? 0) >= 0,
          },
          {
            label: 'Div',
            value: h?.dividend_share != null ? `$${h.dividend_share.toFixed(2)}` : '—',
            sub: h?.dividend_yield != null && h.dividend_yield > 0 ? `${(h.dividend_yield * 100).toFixed(2)}%` : null,
            subUp: null,
          },
        ].map((s) => (
          <div key={s.label} className="ecp-stat">
            <div className="ecp-stat-label">{s.label}</div>
            <div className="ecp-stat-value">{s.value}</div>
            {s.sub && (
              <div className={`ecp-stat-sub${s.subUp === null ? '' : s.subUp ? ' up' : ' down'}`}>{s.sub}</div>
            )}
          </div>
        ))}
      </div>

      {/* 52 周区间 */}
      {tech && (
        <>
          <SectionTitle>52-Week Range</SectionTitle>
          <Text type="secondary" style={{ fontSize: 12 }}>
            {fmtNum(tech.week52_low)} — {fmtNum(tech.week52_high)}
            {tech.beta != null ? ` · Beta ${tech.beta.toFixed(2)}` : ''}
          </Text>
        </>
      )}

      {/* 统计与做空（对齐 EarningsHub Statistics / Short Interest） */}
      {(() => {
        const cells = [
          { label: 'Volume', value: price?.volume != null ? fmtQty(price.volume) : null },
          { label: 'Prev Close', value: price?.previous_close != null ? price.previous_close.toFixed(2) : null },
          { label: 'Employees', value: c.employees != null ? fmtQty(c.employees) : null },
          { label: 'Short Vol', value: tech?.short_volume ? fmtQty(tech.short_volume) : null },
          {
            label: 'Short % Float',
            value: tech?.short_percent != null && tech.short_percent > 0 ? `${(tech.short_percent * 100).toFixed(2)}%` : null,
          },
          { label: 'Short As Of', value: tech?.short_as_of ?? null },
        ].filter((s) => s.value !== null);
        if (cells.length === 0) return null;
        return (
          <>
            <SectionTitle>Statistics</SectionTitle>
            <div className="ecp-stats" style={{ marginTop: 8 }}>
              {cells.map((s) => (
                <div key={s.label} className="ecp-stat">
                  <div className="ecp-stat-label">{s.label}</div>
                  <div className="ecp-stat-value">{s.value}</div>
                </div>
              ))}
            </div>
          </>
        );
      })()}
    </>
  );

  const analystsTab = (
    <>
      {/* 评级 */}
      {ratings && ratingTotal > 0 ? (
        <>
          <SectionTitle>Analyst Ratings</SectionTitle>
          <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginTop: 8 }}>
            <div style={{ fontWeight: 700, fontSize: 18 }}>{fmtNum(ratings.rating, 1)}</div>
            <Text type="secondary" style={{ fontSize: 12 }}>
              {ratings.rating !== null && ratings.rating !== undefined
                ? ratings.rating >= 4
                  ? 'Strong Buy'
                  : ratings.rating >= 3
                    ? 'Buy'
                    : ratings.rating >= 2.5
                      ? 'Hold'
                      : 'Sell'
                : '—'}
              {ratings.target_price ? ` · target $${ratings.target_price.toFixed(0)}` : ''}
            </Text>
          </div>
          <Tooltip title={ratingSegments.map((s) => `${s.label}: ${s.count}`).join(' · ')}>
            <div style={{ display: 'flex', height: 8, borderRadius: 4, overflow: 'hidden', marginTop: 8 }}>
              {ratingSegments.map((s) =>
                s.count > 0 ? (
                  <div key={s.label} style={{ width: `${(s.count / ratingTotal) * 100}%`, background: s.color }} />
                ) : null,
              )}
            </div>
          </Tooltip>
        </>
      ) : (
        <Empty style={{ padding: '40px 0' }} description="No analyst ratings data" />
      )}

      {/* 一致预期 */}
      {trend ? (
        <>
          <SectionTitle>Consensus ({quarterLabel(trend.period_date ?? trend.period)})</SectionTitle>
          <div style={{ fontSize: 12, display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 6 }}>
            <Text type="secondary">
              EPS avg <b style={{ color: 'inherit' }}>{fmtNum(trend.eps_avg)}</b> (range{' '}
              {fmtNum(trend.eps_low)} ~ {fmtNum(trend.eps_high)})
            </Text>
            <Text type="secondary">Analysts: {trend.analysts ?? '—'}</Text>
            <Text type="secondary">Revenue est: {fmtCap(trend.revenue_avg)}</Text>
            <Text type="secondary">
              Revisions 30d:{' '}
              <span style={{ color: '#52c41a' }}>↑{trend.revisions_up_30d ?? 0}</span>{' '}
              <span style={{ color: '#f5222d' }}>↓{trend.revisions_down_30d ?? 0}</span>
            </Text>
          </div>
        </>
      ) : null}
    </>
  );

  const dividendsTab = (
    <>
      <div className="ecp-stats" style={{ marginTop: 4 }}>
        {[
          {
            label: 'Per Share',
            value: h?.dividend_share != null ? `$${h.dividend_share.toFixed(2)}` : '—',
          },
          {
            label: 'Yield',
            value:
              h?.dividend_yield != null && h.dividend_yield > 0
                ? `${(h.dividend_yield * 100).toFixed(2)}%`
                : '—',
          },
          { label: 'TTM Paid', value: divTtm > 0 ? fmtCap(divTtm) : '—' },
          {
            label: 'YoY',
            value:
              divPriorTtm && divPriorTtm > 0 && divTtm > 0
                ? fmtPct(((divTtm - divPriorTtm) / divPriorTtm) * 100)
                : '—',
          },
        ].map((s) => (
          <div key={s.label} className="ecp-stat">
            <div className="ecp-stat-label">{s.label}</div>
            <div className="ecp-stat-value">{s.value}</div>
          </div>
        ))}
      </div>

      {dividendHistory.length > 0 ? (
        <>
          <div className="ecp-chart-header">
            <span className="ecp-chart-title">
              Quarterly Dividends Paid
              {data.dividend_currency && data.dividend_currency !== 'USD' && (
                <span className="ecp-currency-tag">({data.dividend_currency})</span>
              )}
            </span>
          </div>
          <DivChart history={dividendHistory} currency={data.dividend_currency} isDark={isDark} />
          <Text type="secondary" style={{ fontSize: 11 }}>
            cash flow 口径的公司分红总额（非每股）；币种随报表本币
          </Text>
        </>
      ) : (
        <Empty style={{ padding: '40px 0' }} description="No dividend history data" />
      )}
    </>
  );

  return (
    <div className={isDark ? 'ecp-panel ecp-dark' : 'ecp-panel'} style={{ padding: '4px 8px 16px' }}>
      {/* 公司头部 */}
      <div style={{ display: 'flex', gap: 12, alignItems: 'center' }}>
        {c.logo_url ? (
          <Avatar src={c.logo_url} size={48} shape="square" />
        ) : (
          <Avatar size={48} shape="square">
            {(c.code || '?').charAt(0)}
          </Avatar>
        )}
        <div style={{ minWidth: 0 }}>
          <div style={{ fontWeight: 700, fontSize: 16, lineHeight: 1.3 }}>
            {c.name || data.code}
            {c.web_url && (
              <a href={c.web_url} target="_blank" rel="noreferrer" style={{ marginLeft: 6, fontSize: 12 }}>
                <GlobalOutlined />
              </a>
            )}
          </div>
          <Text type="secondary" style={{ fontSize: 12 }}>
            {c.code} · {c.exchange || '—'}
            {c.sector ? ` · ${c.sector}` : ''}
          </Text>
        </div>
        {price?.last !== null && price?.last !== undefined && (
          <div style={{ marginLeft: 'auto', textAlign: 'right' }}>
            <div style={{ fontWeight: 700, fontSize: 16 }}>{price.last.toFixed(2)}</div>
            <Text style={{ fontSize: 12, color: up ? '#52c41a' : '#f5222d' }}>
              {up ? '▲' : '▼'} {fmtPct(price.change_percent)}
            </Text>
          </div>
        )}
      </div>

      {c.description ? (
        <Paragraph
          type="secondary"
          ellipsis={{
            rows: 2,
            expandable: 'collapsible',
            symbol: (expanded: boolean) => (expanded ? 'less' : 'more'),
          }}
          style={{ fontSize: 12, marginTop: 10, marginBottom: 0 }}
        >
          {c.description}
        </Paragraph>
      ) : null}

      {/* Tab 分类（对齐 EarningsHub quote 页导航） */}
      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        size="small"
        style={{ marginTop: 8 }}
        items={[
          { key: 'earnings', label: 'Earnings', children: earningsTab },
          { key: 'info', label: 'Info', children: infoTab },
          { key: 'analysts', label: 'Analysts', children: analystsTab },
          { key: 'dividends', label: 'Dividends', children: dividendsTab },
        ]}
      />
    </div>
  );
};

export default CompanyPanel;
