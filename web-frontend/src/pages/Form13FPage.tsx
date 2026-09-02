import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import {
  Card, Typography, Button, Space, message, Tag, Tooltip, Spin, Empty,
  Table, Select, Tabs,
} from 'antd';
import {
  ReloadOutlined, SearchOutlined, RiseOutlined, FallOutlined,
} from '@ant-design/icons';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip as RTooltip,
  ResponsiveContainer, Legend,
} from 'recharts';
import {
  form13fAPI, ManifestData, FilerDetail, QuarterSnapshot,
  HoldingItem, Top5ChangeItem, FilerSummary,
} from '../services/form13f';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

const { Title, Text } = Typography;

// ===== 行业/资产类别（与后端 form13f_sector_map 分类一致）=====

const SECTOR_ORDER = [
  'Healthcare', 'Basic Materials', 'Consumer Cyclical', 'Industrials', 'Technology',
  'Real Estate', 'Financial Services', 'Consumer Defensive', 'Utilities',
  'Communication Services', 'Energy',
  'Broad Emerging Market', 'Latin American Equity', 'Asia-Pacific (Emerging)',
  'Broad Developed World', 'Precious Metals', 'High-Yield/Junk Bond',
  'Investment Grade Corporate Bond', 'Government Bond', 'Large Cap Blend',
  'Other',
];

const SECTOR_COLORS: Record<string, string> = {
  'Healthcare': '#1b3a6b',
  'Basic Materials': '#25598f',
  'Consumer Cyclical': '#2f75a8',
  'Industrials': '#3f8fb5',
  'Technology': '#56a6b8',
  'Real Estate': '#74bcae',
  'Financial Services': '#93c9a2',
  'Consumer Defensive': '#b4dba2',
  'Utilities': '#cfe8a1',
  'Communication Services': '#dce87f',
  'Energy': '#b6c94f',
  'Broad Emerging Market': '#a3132c',
  'Latin American Equity': '#c0392b',
  'Asia-Pacific (Emerging)': '#d95f4b',
  'Broad Developed World': '#e8836d',
  'Precious Metals': '#f2a488',
  'High-Yield/Junk Bond': '#f7c1a3',
  'Investment Grade Corporate Bond': '#f9d6c3',
  'Government Bond': '#f5e3d7',
  'Large Cap Blend': '#e64961',
  'Other': '#5d6773',
};

// 图例用短名（避免长类别名换行），Tooltip 仍显示完整名
const LEGEND_SHORT: Record<string, string> = {
  'Broad Emerging Market': 'EM Equity',
  'Broad Developed World': 'Dev. World',
  'High-Yield/Junk Bond': 'High-Yield',
  'Investment Grade Corporate Bond': 'IG Corp Bond',
};

const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    barBg: 'rgba(22, 119, 255, 0.08)',
    posColor: '#cf1322',
    negColor: '#389e0d',
  },
  dark: {
    pageBg: '#1a202c',
    cardBg: '#1a2234',
    cardBorder: '#364152',
    text: '#e0e6f1',
    textSecondary: 'rgba(255, 255, 255, 0.5)',
    barBg: 'rgba(22, 119, 255, 0.15)',
    posColor: '#ff7875',
    negColor: '#95de64',
  },
} as const;

// ===== 格式化 =====

const fmtMoney = (v?: number | null): string => {
  if (v === null || v === undefined) return '—';
  const a = Math.abs(v);
  if (a >= 1e9) return (v / 1e9).toFixed(2) + 'B';
  if (a >= 1e6) return (v / 1e6).toFixed(1) + 'M';
  if (a >= 1e3) return (v / 1e3).toFixed(0) + 'K';
  return String(v);
};

const fmtFull = (v?: number | null): string =>
  v === null || v === undefined ? '—' : v.toLocaleString('en-US');

const fmtQuarter = (q: string): string =>
  q.replace(/^(\d{4})Q(\d)$/, '$1 Q$2');

/** 季度快照列表 → 堆叠柱状图行数据（透视 quarter × sector） */
const pivotSectors = (quarters: QuarterSnapshot[]) => {
  const present = new Set<string>();
  quarters.forEach((qs) => Object.keys(qs.sector_alloc).forEach((k) => present.add(k)));
  const ordered = SECTOR_ORDER.filter((s) => present.has(s));
  const rows = quarters.map((qs) => {
    const row: Record<string, string | number> = { quarter: fmtQuarter(qs.quarter) };
    ordered.forEach((sec) => {
      row[sec] = +(qs.sector_alloc[sec] || 0).toFixed(2);
    });
    return row;
  });
  // Other 放最后绘制 → 堆叠最顶层（与 MacroMicro 布局一致）
  const drawOrder = ordered.filter((s) => s !== 'Other')
    .concat(ordered.includes('Other') ? ['Other'] : []);
  return { rows, sectors: ordered, drawOrder };
};

// ===== 主组件 =====

const Form13FPage: React.FC = () => {
  const { isAuthenticated } = useAuthStore();
  const { themeMode } = useDashboardSettings();
  const colors = THEME_COLORS[themeMode];

  const [manifest, setManifest] = useState<ManifestData | null>(null);
  const [filerId, setFilerId] = useState<number | null>(null);
  const [detail, setDetail] = useState<FilerDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [selectedQuarter, setSelectedQuarter] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);

  // 机构选择器：远程搜索
  const [options, setOptions] = useState<FilerSummary[]>([]);
  const [searching, setSearching] = useState(false);
  const searchTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (!isAuthenticated) return;
    (async () => {
      try {
        const m = await form13fAPI.getManifest();
        setManifest(m);
        // 预设机构作为初始选项；默认选第一个
        setOptions(m.presets.map((p) => ({ id: p.id, name: p.label })));
        const first = m.presets.find((p) => p.id != null);
        if (first) setFilerId(first.id);
      } catch (error) {
        console.error('Failed to load 13F manifest:', error);
        message.error('加载 13F 清单失败');
      }
    })();
  }, [isAuthenticated]);

  useEffect(() => {
    if (filerId == null) return;
    (async () => {
      setDetailLoading(true);
      try {
        const d = await form13fAPI.getFilerDetail(filerId);
        setDetail(d);
        const qs = d.quarters.map((x) => x.quarter);
        setSelectedQuarter(qs.length ? qs[qs.length - 1] : null);
      } catch (error) {
        console.error('Failed to load filer detail:', error);
        message.error('加载机构详情失败');
      } finally {
        setDetailLoading(false);
      }
    })();
  }, [filerId]);

  const handleSearch = useCallback((q: string) => {
    if (searchTimer.current) clearTimeout(searchTimer.current);
    if (!q.trim()) return;
    searchTimer.current = setTimeout(async () => {
      setSearching(true);
      try {
        const r = await form13fAPI.searchFilers(q, 1, 20);
        setOptions(r.items.map((it) => ({ id: it.id, name: it.name })));
      } catch (error) {
        console.error('13F filer search failed:', error);
      } finally {
        setSearching(false);
      }
    }, 300);
  }, []);

  const handleRefresh = useCallback(async () => {
    setRefreshing(true);
    try {
      await form13fAPI.refresh('poll');
      message.success('已派发 SEC 轮询任务，稍后刷新查看');
    } catch (error) {
      message.error('派发刷新失败');
    } finally {
      setTimeout(() => setRefreshing(false), 4000);
    }
  }, []);

  const chart = useMemo(() => pivotSectors(detail?.quarters || []), [detail]);
  const currentSnap = useMemo(
    () => detail?.quarters.find((q) => q.quarter === selectedQuarter) || null,
    [detail, selectedQuarter]);

  // Top20 表：与上一季度同 (cusip,putcall) 的股数对比
  const prevShares = useMemo(() => {
    if (!detail || !selectedQuarter) return {};
    const qs = detail.quarters.map((x) => x.quarter);
    const idx = qs.indexOf(selectedQuarter);
    if (idx <= 0) return null; // 首季度无对比（null 表示有上下文但无上期）
    const prev = detail.quarters[idx - 1];
    const map: Record<string, number> = {};
    prev.top20_holdings.forEach((h) => {
      map[`${h.cusip}|${h.putcall || ''}`] = h.shares || 0;
    });
    return map;
  }, [detail, selectedQuarter]);

  const holdingColumns = useMemo(() => [
    {
      title: '#', width: 44, align: 'center' as const,
      render: (_: unknown, __: HoldingItem, i: number) => (
        <Text style={{ color: colors.textSecondary }}>{i + 1}</Text>
      ),
    },
    {
      title: 'Issuer', dataIndex: 'issuer', key: 'issuer', ellipsis: true,
      render: (v: string, r: HoldingItem) => (
        <Space size={4}>
          <span style={{ color: colors.text }}>{v}</span>
          {r.title && r.title !== 'COM' && (
            <Text type="secondary" style={{ fontSize: 11 }}>{r.title}</Text>
          )}
          {r.putcall === 'Call' && <Tag color="volcano" style={{ marginInlineEnd: 0 }}>Call</Tag>}
          {r.putcall === 'P' && <Tag color="geekblue" style={{ marginInlineEnd: 0 }}>Put</Tag>}
        </Space>
      ),
    },
    { title: 'CUSIP', dataIndex: 'cusip', width: 100, render: (v: string) => (
      <Text type="secondary" style={{ fontSize: 12 }}>{v}</Text>) },
    { title: 'Value ($)', dataIndex: 'value', align: 'right' as const, width: 120,
      render: (v: number) => <span style={{ color: colors.text, fontVariantNumeric: 'tabular-nums' }}>{fmtFull(v)}</span> },
    {
      title: '% of portfolio', dataIndex: 'pct', align: 'right' as const, width: 130,
      render: (v: number) => (
        <div style={{ position: 'relative', width: '100%', minWidth: 90 }}>
          <div style={{
            position: 'absolute', inset: 0, borderRadius: 2,
            background: colors.barBg,
            width: `${Math.min(v * 6, 100)}%`,
          }} />
          <span style={{ position: 'relative', fontVariantNumeric: 'tabular-nums' }}>
            {v.toFixed(2)}%
          </span>
        </div>
      ),
    },
    { title: 'Shares', dataIndex: 'shares', align: 'right' as const, width: 120,
      render: (v: number | null) => (
        <span style={{ fontVariantNumeric: 'tabular-nums' }}>{fmtFull(v)}</span>) },
    {
      title: 'Δ Shares (QoQ)', key: 'delta', align: 'right' as const, width: 140,
      render: (_: unknown, r: HoldingItem) => {
        if (prevShares === null) return <Text type="secondary">—</Text>;
        const key = `${r.cusip}|${r.putcall || ''}`;
        const ps = prevShares[key];
        if (ps === undefined) return <Tag color="red" style={{ marginInlineEnd: 0 }}>NEW</Tag>;
        const d = (r.shares || 0) - ps;
        if (d === 0) return <Text type="secondary">0</Text>;
        const pct = ps > 0 ? (d / ps) * 100 : 100;
        return (
          <span style={{ color: d > 0 ? colors.posColor : colors.negColor, fontVariantNumeric: 'tabular-nums' }}>
            {d > 0 ? '+' : ''}{fmtMoney(d)} ({pct >= 0 ? '+' : ''}{pct.toFixed(1)}%)
          </span>
        );
      },
    },
  ], [colors, prevShares]);

  const changeColumns = useMemo(() => [
    {
      title: 'Issuer', dataIndex: 'issuer', key: 'issuer', ellipsis: true,
      render: (v: string, r: Top5ChangeItem) => (
        <Space size={4}>
          <span style={{ color: colors.text }}>{v}</span>
          {r.kind === 'new' && <Tag color="red" style={{ marginInlineEnd: 0 }}>NEW</Tag>}
          {r.kind === 'exited' && <Tag color="green" style={{ marginInlineEnd: 0 }}>EXITED</Tag>}
        </Space>
      ),
    },
    {
      title: 'Δ Value ($)', dataIndex: 'dv', align: 'right' as const, width: 110,
      render: (v: number) => (
        <span style={{ color: v > 0 ? colors.posColor : colors.negColor, fontVariantNumeric: 'tabular-nums' }}>
          {v > 0 ? '+' : '−'}${fmtMoney(Math.abs(v))}
        </span>),
    },
    {
      title: 'Δ %', dataIndex: 'spct', align: 'right' as const, width: 90,
      render: (v: number, r: Top5ChangeItem) => (
        <span style={{ color: r.dv > 0 ? colors.posColor : colors.negColor, fontVariantNumeric: 'tabular-nums' }}>
          {v > 0 ? '+' : ''}{v.toFixed(1)}%
        </span>),
    },
    {
      title: 'Now ($)', dataIndex: 'value', align: 'right' as const, width: 90,
      render: (v: number) => (
        <Text type="secondary" style={{ fontVariantNumeric: 'tabular-nums' }}>
          {v > 0 ? '$' + fmtMoney(v) : '—'}
        </Text>),
    },
  ], [colors]);

  const latestQ = detail?.quarters.length
    ? detail.quarters[detail.quarters.length - 1] : null;

  if (!isAuthenticated) {
    return <div style={{ padding: 24 }}><Spin /></div>;
  }

  return (
    <div style={{ padding: 24, background: colors.pageBg, minHeight: '100%' }}>
      {/* ── 顶栏：标题 + 机构选择 + 刷新 ── */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap', marginBottom: 16 }}>
        <div>
          <Title level={4} style={{ margin: 0, color: colors.text }}>Form 13F Filings</Title>
          <Text type="secondary" style={{ fontSize: 12 }}>
            数据来源：SEC Form 13F Data Sets · 季度持仓披露{manifest?.quarters.length ? `（${manifest.quarters[0]} – ${manifest.quarters[manifest.quarters.length - 1]}）` : ''}
          </Text>
        </div>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
          <Select
            showSearch
            value={filerId}
            placeholder="搜索机构（9,400+ filers）…"
            style={{ minWidth: 300 }}
            filterOption={false}
            onSearch={handleSearch}
            onSelect={(v: number) => setFilerId(v)}
            notFoundContent={searching ? <Spin size="small" /> : null}
            options={options.map((o) => ({
              value: o.id,
              label: o.name,
            }))}
            suffixIcon={<SearchOutlined />}
          />
          <Tooltip title="轮询 SEC 数据页：发现新季度数据集并自动下载导入">
            <Button icon={<ReloadOutlined />} loading={refreshing} onClick={handleRefresh}>
              刷新
            </Button>
          </Tooltip>
        </div>
      </div>

      {/* 预设机构快捷胶囊 */}
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginBottom: 16 }}>
        {(manifest?.presets || []).map((p) => (
          <Button
            key={p.id}
            size="small"
            type={p.id === filerId ? 'primary' : 'default'}
            onClick={() => p.id != null && setFilerId(p.id)}
            disabled={p.id == null}
            style={p.id == null ? { opacity: 0.4 } : undefined}
          >
            {p.label}
          </Button>
        ))}
      </div>

      <Spin spinning={detailLoading}>
        {/* ── % of sector 季度趋势 ── */}
        <Card size="small" style={{ marginBottom: 16, background: colors.cardBg, borderColor: colors.cardBorder }}>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginBottom: 4 }}>
            <Text strong style={{ fontSize: 15, color: colors.text }}>
              {detail?.name || '—'}
            </Text>
            <Tooltip title="组合中各行业/资产类别的市值占比（% of sector），按季度展示。基于该机构当季全部 13F 持仓计算。">
              <Text type="secondary" style={{ cursor: 'help' }}>ⓘ</Text>
            </Tooltip>
            {currentSnap && (
              <Text type="secondary" style={{ fontSize: 12 }}>
                {fmtQuarter(currentSnap.quarter)} 组合市值 ${fmtMoney(currentSnap.total_value)} · {fmtFull(currentSnap.position_count)} 条持仓
              </Text>
            )}
          </div>
          {chart.rows.length ? (
            <ResponsiveContainer width="100%" height={380}>
              <BarChart data={chart.rows} margin={{ top: 8, right: 8, bottom: 0, left: 0 }}>
                <CartesianGrid vertical={false} stroke={themeMode === 'dark' ? '#2d3a4e' : '#eceff2'} />
                <XAxis dataKey="quarter" tick={{ fontSize: 11, fill: colors.textSecondary }} />
                <YAxis
                  domain={[0, 100]} interval={25}
                  tickFormatter={(v: number) => String(v)}
                  tick={{ fontSize: 11, fill: colors.textSecondary }}
                  width={36}
                />
                <RTooltip
                  formatter={(v: number | string, name: string) => [
                    `${Number(v).toFixed(2)}%`, LEGEND_SHORT[name] ? name : name,
                  ]}
                  contentStyle={{
                    background: themeMode === 'dark' ? '#1a2234' : '#fff',
                    border: `1px solid ${colors.cardBorder}`,
                    color: colors.text,
                  }}
                />
                <Legend
                  wrapperStyle={{ fontSize: 11, paddingTop: 8 }}
                  formatter={(v: string) => LEGEND_SHORT[v] || v}
                />
                {chart.drawOrder.map((sec) => (
                  <Bar
                    key={sec}
                    dataKey={sec}
                    stackId="sector"
                    fill={SECTOR_COLORS[sec] || '#999'}
                    maxBarSize={64}
                  />
                ))}
              </BarChart>
            </ResponsiveContainer>
          ) : (
            <Empty description="暂无快照数据" style={{ padding: 40 }} />
          )}
        </Card>

        {/* ── Top 20 largest holdings ── */}
        <Card size="small" style={{ marginBottom: 16, background: colors.cardBg, borderColor: colors.cardBorder }}>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginBottom: 8 }}>
            <Text strong style={{ fontSize: 15, color: colors.text }}>Top 20 largest holdings</Text>
            <Tooltip title="该机构报告的全部持仓中市值最大的 20 只证券。% of portfolio = 持仓市值 / 当季组合总市值；Δ Shares 为相对上一报告期的持仓股数变动。">
              <Text type="secondary" style={{ cursor: 'help' }}>ⓘ</Text>
            </Tooltip>
            {detail && detail.quarters.length > 1 && (
              <Tabs
                size="small"
                activeKey={selectedQuarter || undefined}
                onChange={setSelectedQuarter}
                style={{ marginLeft: 'auto' }}
                items={[...detail.quarters].reverse().map((q) => ({
                  key: q.quarter,
                  label: fmtQuarter(q.quarter),
                }))}
              />
            )}
          </div>
          <Table<HoldingItem>
            size="small"
            rowKey={(r) => `${r.cusip}|${r.putcall || ''}`}
            columns={holdingColumns}
            dataSource={currentSnap?.top20_holdings || []}
            pagination={false}
            locale={{ emptyText: <Empty description="无数据" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
          />
        </Card>

        {/* ── Top 5 buys / sells ── */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16 }}>
          {[
            {
              title: 'Top 5 buys',
              icon: <RiseOutlined style={{ color: colors.posColor }} />,
              tip: '当季市值增加幅度最大的 5 只证券（含新建仓）。',
              items: latestQ?.top5_buys || [],
            },
            {
              title: 'Top 5 sells',
              icon: <FallOutlined style={{ color: colors.negColor }} />,
              tip: '当季市值减少幅度最大的 5 只证券（含清仓）。',
              items: latestQ?.top5_sells || [],
            },
          ].map(({ title, icon, tip, items }) => (
            <Card
              key={title}
              size="small"
              style={{ background: colors.cardBg, borderColor: colors.cardBorder }}
              title={(
                <Space size={6}>
                  {icon}
                  <span>{title}</span>
                  <Text type="secondary" style={{ fontSize: 12, fontWeight: 400 }}>
                    {latestQ ? `· ${fmtQuarter(latestQ.quarter)}` : ''}
                  </Text>
                  <Tooltip title={tip}>
                    <Text type="secondary" style={{ cursor: 'help', fontSize: 12 }}>ⓘ</Text>
                  </Tooltip>
                </Space>
              )}
            >
              <Table<Top5ChangeItem>
                size="small"
                rowKey={(r) => `${r.cusip}|${r.putcall || ''}`}
                columns={changeColumns}
                dataSource={items}
                pagination={false}
                locale={{ emptyText: <Empty description="无数据" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
              />
            </Card>
          ))}
        </div>
      </Spin>

      <div style={{ textAlign: 'center', marginTop: 24 }}>
        <Text type="secondary" style={{ fontSize: 12 }}>
          Form 13F 是美国 SEC 要求管理超过 1 亿美元资产的机构投资管理人提交的季度持仓报告。
          数据来自 <a href="https://www.sec.gov/data-research/sec-markets-data/form-13f-data-sets" target="_blank" rel="noreferrer">SEC Form 13F Data Sets</a>，仅供研究参考。
        </Text>
      </div>
    </div>
  );
};

export default Form13FPage;
