/* =========================================================================
   移动版 Form 13F（/form13f，手机端由 TerminalRoutes 按 isMobile 换入）
   与桌面 Form13FPage 同口径：预设机构切换 + 行业分布季度趋势（复用
   Form13FSectorChart compact 模式）+ 季度选择 + Top 20 持仓。
   移动适配：机构切换用单行横向滑动胶囊条；Tabs 季度选择换 Select；
   Top 20 六列表格换两行式列表（信息不裁剪）。
   共享逻辑/色板见 components/form13f/form13fShared.tsx。
   ========================================================================= */
import React, { useState, useEffect, useMemo, useRef } from 'react';
import { Button, Card, Empty, Select, Spin, Tag, Typography, message } from 'antd';
import {
  form13fAPI, ManifestData, FilerDetail, HoldingItem,
} from '../services/form13f';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import Form13FSectorChart from '../components/form13f/Form13FSectorChart';
import { aiTagChipStyle } from '../components/shared/aiTagChipStyles';
import {
  THEME_COLORS, Form13fThemeColors, fmtMoney, fmtFull, fmtQuarter,
  StatusLabel, isPut, isCall,
} from '../components/form13f/form13fShared';
import './MobileForm13FPage.css';

const { Text } = Typography;

/* Δ Shares 渲染（与桌面 Δ Shares (QoQ) 列同规则）：
   无上一报告期 → —；上期无持仓 → New；持平 → 0；否则红绿 +绝对量(±%) */
const DeltaShares: React.FC<{
  holding: HoldingItem;
  hasPrevQuarter: boolean;
  colors: Form13fThemeColors;
  themeMode: 'light' | 'dark';
}> = ({ holding, hasPrevQuarter, colors, themeMode }) => {
  if (!hasPrevQuarter) return <Text type="secondary">—</Text>;
  const ps = holding.prev_shares;
  if (ps == null || ps === 0) return <StatusLabel kind="new" themeMode={themeMode} />;
  const d = (holding.shares || 0) - ps;
  if (d === 0) return <Text type="secondary">0</Text>;
  const pct = (d / ps) * 100;
  return (
    <span style={{ color: d > 0 ? colors.posColor : colors.negColor, fontVariantNumeric: 'tabular-nums' }}>
      {d > 0 ? '+' : ''}{fmtMoney(d)} ({pct >= 0 ? '+' : ''}{pct.toFixed(1)}%)
    </span>
  );
};

const MobileForm13FPage: React.FC = () => {
  const { isAuthenticated } = useAuthStore();
  const { themeMode } = useDashboardSettings();
  const colors = THEME_COLORS[themeMode];

  const [manifest, setManifest] = useState<ManifestData | null>(null);
  const [filerId, setFilerId] = useState<number | null>(null);
  const [detail, setDetail] = useState<FilerDetail | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [selectedQuarter, setSelectedQuarter] = useState<string | null>(null);

  // 选中机构胶囊自动滚入视野（20 家胶囊条横向滑动）
  const activePillRef = useRef<HTMLButtonElement | null>(null);
  useEffect(() => {
    activePillRef.current?.scrollIntoView({
      inline: 'center', block: 'nearest', behavior: 'smooth',
    });
  }, [filerId]);

  useEffect(() => {
    if (!isAuthenticated) return;
    (async () => {
      try {
        const m = await form13fAPI.getManifest();
        setManifest(m);
        // 预设机构作为初始选项；默认选第一个（与桌面同口径）
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

  if (!isAuthenticated) {
    return <div className="m13f-loading"><Spin /></div>;
  }

  const cardStyle = { background: colors.cardBg, border: `1px solid ${colors.cardBorder}` };
  const holdings = currentSnap?.top20_holdings || [];

  return (
    <div className="m13f-page" style={{ background: colors.pageBg, minHeight: '100%' }}>
      {/* 预设机构快捷胶囊：单行横向滑动（桌面为可换行胶囊组 .app-ctl-pills） */}
      <div className="m13f-pill-strip" role="tablist" aria-label="预设机构">
        {(manifest?.presets || []).map((p) => (
          <Button
            key={p.id}
            ref={p.id === filerId ? activePillRef : undefined}
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
        <Card size="small" className="m13f-card" style={cardStyle} styles={{ body: { padding: 12 } }}>
          <div className="m13f-title-row">
            <Text strong style={{ fontSize: 15, color: colors.text }}>
              {detail?.name || '—'}
            </Text>
            {quarterRange && (
              <Text type="secondary" style={{ fontSize: 12 }}>{quarterRange}</Text>
            )}
          </div>
          {latestQ && (
            <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>
              {fmtQuarter(latestQ.quarter)} 组合市值 ${fmtMoney(latestQ.total_value)} · {fmtFull(latestQ.position_count)} 条持仓
            </Text>
          )}
          {detail?.quarters.length ? (
            <Form13FSectorChart
              quarters={detail.quarters}
              themeMode={themeMode}
              onQuarterClick={setSelectedQuarter}
              height={300}
              compact
            />
          ) : (
            <Empty description="暂无快照数据" style={{ padding: 32 }} />
          )}
        </Card>

        {/* ── 季度选择（桌面 Tabs 在窄屏放不下，换 Select；点图表柱同样可切换）── */}
        {detail && detail.quarters.length > 1 && (
          <div className="m13f-quarter-row">
            <Text type="secondary" style={{ fontSize: 12 }}>季度</Text>
            <Select
              size="small"
              style={{ minWidth: 116 }}
              value={selectedQuarter || undefined}
              onChange={setSelectedQuarter}
              options={[...detail.quarters].reverse().map((q) => ({
                value: q.quarter,
                label: fmtQuarter(q.quarter),
              }))}
            />
          </div>
        )}

        {/* ── Top 20 largest holdings（两行式列表承载桌面 6 列信息）── */}
        <Card size="small" className="m13f-card" style={cardStyle} styles={{ body: { padding: '4px 12px' } }}>
          <div style={{ padding: '8px 0 2px' }}>
            <Text strong style={{ fontSize: 15, color: colors.text }}>Top 20 largest holdings</Text>
          </div>
          {holdings.length ? (
            <div className="m13f-holding-list">
              {holdings.map((h, i) => (
                <div
                  key={`${h.cusip}|${h.putcall || ''}`}
                  className="m13f-holding-row"
                  style={{ borderTopColor: colors.cardBorder }}
                >
                  <div className="m13f-holding-main">
                    <Text style={{ color: colors.textSecondary, fontSize: 12, flex: '0 0 auto' }}>
                      {i + 1}
                    </Text>
                    <div className="m13f-holding-issuer-wrap">
                      <span className="m13f-holding-issuer" style={{ color: colors.text }}>
                        {h.issuer}
                      </span>
                      {h.title && h.title !== 'COM' && (
                        <Tag bordered={false} style={aiTagChipStyle('signal-meta', themeMode)}>
                          {h.title}
                        </Tag>
                      )}
                      {isCall(h.putcall) && <StatusLabel kind="call" themeMode={themeMode} />}
                      {isPut(h.putcall) && <StatusLabel kind="put" themeMode={themeMode} />}
                    </div>
                    <span className="m13f-holding-value" style={{ color: colors.text }}>
                      ${fmtFull(h.value)}
                    </span>
                  </div>
                  <div className="m13f-holding-sub">
                    <div className="m13f-holding-pct">
                      <div style={{
                        position: 'absolute', left: 0, top: 2, bottom: 2, borderRadius: 2,
                        background: colors.barBg,
                        width: `${(h.pct / maxHoldingPct * 100).toFixed(1)}%`,
                      }} />
                      <span style={{ position: 'relative', paddingLeft: 6, fontVariantNumeric: 'tabular-nums', fontSize: 12, color: colors.text }}>
                        {h.pct.toFixed(2)}%
                      </span>
                    </div>
                    <div className="m13f-holding-shares" style={{ fontSize: 12, color: colors.textSecondary }}>
                      <div>{fmtFull(h.shares)} 股</div>
                      <DeltaShares holding={h} hasPrevQuarter={hasPrevQuarter} colors={colors} themeMode={themeMode} />
                    </div>
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <Empty description="无数据" image={Empty.PRESENTED_IMAGE_SIMPLE} style={{ padding: 24 }} />
          )}
        </Card>
      </Spin>

      <div className="m13f-footer">
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

export default MobileForm13FPage;
