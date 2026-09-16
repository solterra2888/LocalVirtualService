import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import type { CSSProperties } from 'react';
import {
  Card, Typography, Button, Space, message, Tooltip, Spin, Empty,
  Table, Select, Tabs,
} from 'antd';
import {
  ReloadOutlined, SearchOutlined, RiseOutlined, FallOutlined,
} from '@ant-design/icons';
import {
  form13fAPI, ManifestData, FilerDetail,
  HoldingItem, Top5ChangeItem, FilerSummary,
} from '../services/form13f';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import Form13FSectorChart from '../components/form13f/Form13FSectorChart';

const { Title, Text } = Typography;

// ===== 行业/资产类别（与后端 form13f_sector_map 分类一致）=====
// 图表配色与交互见 Form13FSectorChart.tsx

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

/** Feeds「Brief digest」微型标签：大写、字距、10px / 600。 */
type StatusKind = 'new' | 'exited' | 'put' | 'call';

const STATUS_COPY: Record<StatusKind, string> = {
  new: 'New',
  exited: 'Exited',
  put: 'Put',
  call: 'Call',
};

type StatusColors = {
  posColor: string;
  negColor: string;
  textSecondary: string;
};

const StatusLabel: React.FC<{
  kind: StatusKind;
  themeMode: 'light' | 'dark';
  colors: StatusColors;
}> = ({ kind, themeMode, colors }) => {
  const color =
    kind === 'new' ? colors.posColor
      : kind === 'exited' ? colors.negColor
        : (themeMode === 'dark' ? colors.textSecondary : '#3d4a5c');
  const style: CSSProperties = {
    fontSize: 10,
    letterSpacing: 1.1,
    textTransform: 'uppercase',
    fontWeight: 600,
    color,
    lineHeight: '12px',
    flexShrink: 0,
  };
  return <span style={style}>{STATUS_COPY[kind]}</span>;
};

const isPut = (putcall?: string | null) =>
  putcall === 'Put' || putcall === 'P';
const isCall = (putcall?: string | null) =>
  putcall === 'Call' || putcall === 'C';

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
      setDetail(null);
      setSelectedQuarter(null);
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
      message.success('已派发 13F 同步任务，稍后刷新查看');
    } catch (error) {
      message.error('派发刷新失败');
    } finally {
      setTimeout(() => setRefreshing(false), 4000);
    }
  }, []);

  const currentSnap = useMemo(
    () => detail?.quarters.find((q) => q.quarter === selectedQuarter) || null,
    [detail, selectedQuarter]);
  const latestQ = detail?.quarters.length
    ? detail.quarters[detail.quarters.length - 1] : null;
  const quarterRange = useMemo(() => {
    const qs = detail?.quarters.map((q) => q.quarter) || [];
    if (qs.length < 2) return qs[0] ? fmtQuarter(qs[0]) : '';
    return `${fmtQuarter(qs[0])} – ${fmtQuarter(qs[qs.length - 1])}`;
  }, [detail]);
  const maxHoldingPct = useMemo(() => {
    const pcts = (currentSnap?.top20_holdings || []).map((h) => h.pct);
    return Math.max(...pcts, 0.01);
  }, [currentSnap]);

  // 当前选中季是否有上一报告期（首季 Δ Shares 显示 —，其余才能标 NEW）
  const hasPrevQuarter = useMemo(() => {
    if (!detail || !selectedQuarter) return false;
    return detail.quarters.map((x) => x.quarter).indexOf(selectedQuarter) > 0;
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
          {isCall(r.putcall) && (
            <StatusLabel kind="call" themeMode={themeMode} colors={colors} />
          )}
          {isPut(r.putcall) && (
            <StatusLabel kind="put" themeMode={themeMode} colors={colors} />
          )}
        </Space>
      ),
    },
    { title: 'Value ($)', dataIndex: 'value', align: 'right' as const, width: 120,
      render: (v: number) => <span style={{ color: colors.text, fontVariantNumeric: 'tabular-nums' }}>{fmtFull(v)}</span> },
    {
      title: '% of portfolio', dataIndex: 'pct', align: 'right' as const, width: 130,
      render: (v: number) => (
        <div style={{ position: 'relative', width: '100%', minWidth: 90 }}>
          <div style={{
            position: 'absolute', inset: 0, borderRadius: 2,
            background: colors.barBg,
            width: `${(v / maxHoldingPct * 100).toFixed(1)}%`,
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
        if (!hasPrevQuarter) return <Text type="secondary">—</Text>;
        const ps = r.prev_shares;
        if (ps == null || ps === 0) {
          return (
            <StatusLabel kind="new" themeMode={themeMode} colors={colors} />
          );
        }
        const d = (r.shares || 0) - ps;
        if (d === 0) return <Text type="secondary">0</Text>;
        const pct = (d / ps) * 100;
        return (
          <span style={{ color: d > 0 ? colors.posColor : colors.negColor, fontVariantNumeric: 'tabular-nums' }}>
            {d > 0 ? '+' : ''}{fmtMoney(d)} ({pct >= 0 ? '+' : ''}{pct.toFixed(1)}%)
          </span>
        );
      },
    },
  ], [colors, hasPrevQuarter, maxHoldingPct, themeMode]);

  const changeColumns = useMemo(() => [
    {
      title: 'Issuer', dataIndex: 'issuer', key: 'issuer', ellipsis: true,
      render: (v: string, r: Top5ChangeItem) => (
        <Space size={4}>
          <span style={{ color: colors.text }}>{v}</span>
          {isCall(r.putcall) && (
            <StatusLabel kind="call" themeMode={themeMode} colors={colors} />
          )}
          {isPut(r.putcall) && (
            <StatusLabel kind="put" themeMode={themeMode} colors={colors} />
          )}
          {r.kind === 'new' && (
            <StatusLabel kind="new" themeMode={themeMode} colors={colors} />
          )}
          {r.kind === 'exited' && (
            <StatusLabel kind="exited" themeMode={themeMode} colors={colors} />
          )}
        </Space>
      ),
    },
    {
      title: '% of portfolio', key: 'pct', align: 'right' as const, width: 150,
      render: (_: unknown, r: Top5ChangeItem) => {
        if (r.pct == null) return <Text type="secondary">—</Text>;
        const dpct = r.dpct ?? 0;
        return (
          <span style={{ fontVariantNumeric: 'tabular-nums' }}>
            {r.pct.toFixed(2)}%{' '}
            <span style={{ color: dpct >= 0 ? colors.posColor : colors.negColor }}>
              ({dpct >= 0 ? '+' : ''}{dpct.toFixed(2)}%)
            </span>
          </span>
        );
      },
    },
    {
      title: 'Change in shares', key: 'spct', align: 'right' as const, width: 140,
      render: (_: unknown, r: Top5ChangeItem) => {
        if (r.kind === 'new' || r.spct == null) {
          return (
            <StatusLabel kind="new" themeMode={themeMode} colors={colors} />
          );
        }
        if (r.kind === 'exited') {
          return (
            <StatusLabel kind="exited" themeMode={themeMode} colors={colors} />
          );
        }
        return (
          <span style={{
            color: r.spct >= 0 ? colors.posColor : colors.negColor,
            fontVariantNumeric: 'tabular-nums',
          }}>
            {r.spct >= 0 ? '+' : ''}{r.spct.toFixed(2)}%
          </span>
        );
      },
    },
  ], [colors, themeMode]);

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
            数据来源：SEC EDGAR 13F-HR · 关注机构{manifest?.quarters.length ? `（${manifest.quarters[0]} – ${manifest.quarters[manifest.quarters.length - 1]}）` : ''}
          </Text>
        </div>
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 8 }}>
          <Select
            showSearch
            value={filerId}
            placeholder="搜索关注机构…"
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
          <Tooltip title="同步关注机构的最新 13F-HR">
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
            {latestQ && (
              <Text type="secondary" style={{ fontSize: 12 }}>
                {fmtQuarter(latestQ.quarter)} 组合市值 ${fmtMoney(latestQ.total_value)} · {fmtFull(latestQ.position_count)} 条持仓
              </Text>
            )}
            {quarterRange && (
              <Text type="secondary" style={{ fontSize: 12, marginLeft: 'auto' }}>
                {quarterRange}
              </Text>
            )}
          </div>
          {detail?.quarters.length ? (
            <Form13FSectorChart
              quarters={detail.quarters}
              themeMode={themeMode}
              onQuarterClick={setSelectedQuarter}
            />
          ) : (
            <Empty description="暂无快照数据" style={{ padding: 40 }} />
          )}
        </Card>

        {detail && detail.quarters.length > 1 && (
          <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 8 }}>
            <Tabs
              size="small"
              activeKey={selectedQuarter || undefined}
              onChange={setSelectedQuarter}
              items={[...detail.quarters].reverse().map((q) => ({
                key: q.quarter,
                label: fmtQuarter(q.quarter),
              }))}
            />
          </div>
        )}

        {/* ── Top 5 buys / sells ── */}
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 16 }}>
          {[
            {
              title: 'Top 5 buys',
              icon: <RiseOutlined style={{ color: colors.posColor }} />,
              tip: '实际加仓或新建的证券中，投组占比上升最多的 5 只。股数未变（仅因股价/组合规模漂移）不计入。排序按 Δ(% of portfolio)。',
              items: currentSnap?.top5_buys || [],
            },
            {
              title: 'Top 5 sells',
              icon: <FallOutlined style={{ color: colors.negColor }} />,
              tip: '实际减仓或清仓的证券中，投组占比下降最多的 5 只。股数未变不计入。排序按 Δ(% of portfolio)。',
              items: currentSnap?.top5_sells || [],
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
                    {currentSnap ? `· ${fmtQuarter(currentSnap.quarter)}` : ''}
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

        {/* ── Top 20 largest holdings ── */}
        <Card size="small" style={{ marginBottom: 16, background: colors.cardBg, borderColor: colors.cardBorder }}>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginBottom: 8 }}>
            <Text strong style={{ fontSize: 15, color: colors.text }}>Top 20 largest holdings</Text>
            <Tooltip title="该机构报告的全部持仓中市值最大的 20 只证券。% of portfolio = 持仓市值 / 当季组合总市值；Δ Shares 为相对上一报告期全量持仓（不限上季 Top 20）的股数变动。">
              <Text type="secondary" style={{ cursor: 'help' }}>ⓘ</Text>
            </Tooltip>
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
      </Spin>

      <div style={{ textAlign: 'center', marginTop: 24 }}>
        <Text type="secondary" style={{ fontSize: 12 }}>
          Form 13F 是美国 SEC 要求管理超过 1 亿美元资产的机构投资管理人提交的季度持仓报告。
          数据来自 <a href="https://www.sec.gov/cgi-bin/browse-edgar" target="_blank" rel="noreferrer">SEC EDGAR 13F-HR</a>（关注机构），仅供研究参考。
        </Text>
      </div>
    </div>
  );
};

export default Form13FPage;
