import React, { useState, useEffect, useMemo } from 'react';
import type { CSSProperties } from 'react';
import {
  Card, Typography, Button, Space, message, Tooltip, Spin, Empty,
  Table, Tabs, Tag,
} from 'antd';
import {
  form13fAPI, ManifestData, FilerDetail,
  HoldingItem,
} from '../services/form13f';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import Form13FSectorChart from '../components/form13f/Form13FSectorChart';
import { aiTagChipStyle, type AiTagChipVariant } from '../components/shared/aiTagChipStyles';

const { Text } = Typography;

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

/** 持仓行内 New/Put/Call 标签：全站标签胶囊规范（aiTagChipStyle，见 UI_STYLE_SPEC.md 二） */
type StatusKind = 'new' | 'put' | 'call';

const STATUS_COPY: Record<StatusKind, string> = {
  new: 'New',
  put: 'Put',
  call: 'Call',
};

const STATUS_VARIANT: Record<StatusKind, AiTagChipVariant> = {
  new: 'signal-neutral',   // 新建持仓 = 中性状态高亮
  put: 'signal-bearish',   // Put = 看跌工具
  call: 'signal-bullish',  // Call = 看涨工具
};

const StatusLabel: React.FC<{
  kind: StatusKind;
  themeMode: 'light' | 'dark';
}> = ({ kind, themeMode }) => (
  <Tag bordered={false} style={aiTagChipStyle(STATUS_VARIANT[kind], themeMode)}>
    {STATUS_COPY[kind]}
  </Tag>
);

const isPut = (putcall?: string | null) =>
  putcall === 'Put' || putcall === 'P';
const isCall = (putcall?: string | null) =>
  putcall === 'Call' || putcall === 'C';

const TIP_BOX: CSSProperties = { maxWidth: 360, fontSize: 12, lineHeight: 1.65 };

const TipLine: React.FC<{ label: string; children: React.ReactNode }> = ({ label, children }) => (
  <div style={{ marginTop: 4 }}>
    <span style={{ fontWeight: 600 }}>{label}</span>
    {children}
  </div>
);

const TOP20_TIP = (
  <div style={TIP_BOX}>
    该机构当季 13F 申报中市值最大的 20 只证券（含期权等全部 infotable 行）。
    <TipLine label="% of portfolio：">持仓市值 / 当季 13F 申报总市值。</TipLine>
    <TipLine label="Δ Shares (QoQ)：">相对上一报告期同一 CUSIP + Put/Call 的股数变动（对照全量持仓，不限上季 Top 20）。上期无持仓标 New。</TipLine>
  </div>
);

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

  useEffect(() => {
    if (!isAuthenticated) return;
    (async () => {
      try {
        const m = await form13fAPI.getManifest();
        setManifest(m);
        // 预设机构作为初始选项；默认选第一个
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
      title: '#', width: 36, align: 'center' as const,
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
            <Tag bordered={false} style={aiTagChipStyle('signal-meta', themeMode)}>
              {r.title}
            </Tag>
          )}
          {isCall(r.putcall) && (
            <StatusLabel kind="call" themeMode={themeMode} />
          )}
          {isPut(r.putcall) && (
            <StatusLabel kind="put" themeMode={themeMode} />
          )}
        </Space>
      ),
    },
    { title: 'Value ($)', dataIndex: 'value', align: 'right' as const, width: 150,
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
    { title: 'Shares', dataIndex: 'shares', align: 'right' as const, width: 140,
      render: (v: number | null) => (
        <span style={{ fontVariantNumeric: 'tabular-nums' }}>{fmtFull(v)}</span>) },
    {
      title: 'Δ Shares (QoQ)', key: 'delta', align: 'right' as const, width: 150,
      render: (_: unknown, r: HoldingItem) => {
        if (!hasPrevQuarter) return <Text type="secondary">—</Text>;
        const ps = r.prev_shares;
        if (ps == null || ps === 0) {
          return (
            <StatusLabel kind="new" themeMode={themeMode} />
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

  if (!isAuthenticated) {
    return <div style={{ padding: 24 }}><Spin /></div>;
  }

  return (
    <div style={{ padding: 24, background: colors.pageBg, minHeight: '100%' }}>
      {/* 预设机构快捷胶囊: 选项多(20 家), 用可换行胶囊切换组 .app-ctl-pills(Segmented 放不下) */}
      <div className="app-ctl-pills" style={{ marginBottom: 16 }}>
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

        {/* ── Top 20 largest holdings ── */}
        <Card size="small" style={{ marginBottom: 16, background: colors.cardBg, borderColor: colors.cardBorder }}>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8, marginBottom: 8 }}>
            <Text strong style={{ fontSize: 15, color: colors.text }}>Top 20 largest holdings</Text>
            <Tooltip title={TOP20_TIP} overlayInnerStyle={{ maxWidth: 400 }}>
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
          Form 13F 是美国 SEC 要求管理超过 1 亿美元资产的机构投资管理人提交的季度持仓报告，
          披露股权资产（股票、ETF）多头，不含期货与固定收益。
          数据来自 <a href="https://www.sec.gov/cgi-bin/browse-edgar" target="_blank" rel="noreferrer">SEC EDGAR 13F-HR</a>（关注机构），仅供研究参考。
          Top 20 展示申报表中市值最大的 20 条（含期权等）。
        </Text>
      </div>
    </div>
  );
};

export default Form13FPage;
