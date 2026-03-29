import React, { useEffect, useState, useMemo, useRef } from 'react';
import { Card, Spin, Empty, Tag } from 'antd';
import { IBReportData } from './IBTradingReportView';
import TradeReviewChart, { TradeMarker } from './TradeReviewChart';
import { dashboardAPI } from '../services/dashboard';

interface PriceDataPoint {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface TickerGroup {
  symbol: string;
  currency: string;
  trades: TradeMarker[];
}

interface TickerPriceState {
  data: PriceDataPoint[];
  loading: boolean;
  error: string | null;
}

interface Props {
  data: IBReportData;
  isDark: boolean;
  themeColors: any;
}

function parseNumStr(val: string | undefined): number {
  if (!val) return 0;
  const s = val.replace(/,/g, '').trim();
  const n = Number(s);
  return isNaN(n) ? 0 : n;
}

/** MTM 表列名：英文 / 中文 IB 导出 */
const MTM_SYM_KEYS = ['Symbol', '代码'] as const;
const MTM_COMM_KEYS = [
  'Mark-to-Market P/L Commissions',
  '按市值计盈亏 佣金',
  '按市值计盈亏佣金',
] as const;
const MTM_TOTAL_KEYS = [
  'Mark-to-Market P/L Total',
  '按市值计盈亏 总数',
  '按市值计盈亏总数',
] as const;

function pickMtmCell(row: Record<string, string>, keys: readonly string[]): string | undefined {
  for (const k of keys) {
    const v = row[k];
    if (v !== undefined && String(v).trim() !== '') return v;
  }
  return undefined;
}

function fmtMtmDisplay(raw: string | undefined): string {
  if (raw === undefined || raw === null) return '—';
  const t = String(raw).trim();
  if (t === '' || t === '--') return '—';
  const s = t.replace(/,/g, '');
  const n = Number(s);
  if (isNaN(n)) return t;
  return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function mtmTagColor(raw: string | undefined): 'green' | 'red' | 'default' {
  if (raw === undefined || raw === null) return 'default';
  const t = String(raw).trim();
  if (t === '' || t === '--') return 'default';
  const n = Number(t.replace(/,/g, ''));
  if (isNaN(n)) return 'default';
  if (n > 0) return 'green';
  if (n < 0) return 'red';
  return 'default';
}

function getMtmStockRows(tables: IBReportData['tables']): Record<string, string>[] {
  const split = tables['按市值计算的表现总结_股票'];
  if (split?.length) return split;
  const legacy = tables['按市值计算的表现总结'];
  if (!legacy?.length) return [];
  return legacy.filter((r) => {
    const cat = (r['Asset Category'] || r['资产分类'] || '').trim();
    return cat === 'Stocks' || cat === '股票';
  });
}

/** symbol → MTM 佣金 / 总数（与 Mark-to-Market Performance 股票表对齐） */
function buildMtmPlBySymbol(tables: IBReportData['tables']): Map<string, { commissions: string; total: string }> {
  const map = new Map<string, { commissions: string; total: string }>();
  const rows = getMtmStockRows(tables);
  for (const row of rows) {
    const sym = pickMtmCell(row, MTM_SYM_KEYS)?.trim();
    if (!sym) continue;
    const commissions = pickMtmCell(row, MTM_COMM_KEYS) ?? '';
    const total = pickMtmCell(row, MTM_TOTAL_KEYS) ?? '';
    map.set(sym, { commissions, total });
  }
  return map;
}

/**
 * 复盘仅展示股票：EODHD 无法覆盖期权/期货/外汇/基金等，按 IB「资产分类」过滤。
 * 先排除明确非股票，再匹配股票关键词；未知分类保守排除。
 */
export function isStockAssetClass(raw: string | undefined): boolean {
  const s = (raw || '').trim();
  if (!s) return false;

  if (
    /期权|期货|外汇|基金|债券|结构性|权证|涡轮|商品|金属|能源|指数|利差|货币|现货金|差价/i.test(s) ||
    /\b(OPT|FOP|FUT|FX|BOND|FUND|CFD|CMDTY|FOR|BAG|COIN|CRYPTO|CMD|IOPT)\b/i.test(s)
  ) {
    return false;
  }
  // IB 外汇/现金类常见写法（与股票 STK 区分）
  if (/\bCASH\b/i.test(s) && !/股票/i.test(s)) return false;

  if (/股票|正股|普通股/i.test(s)) return true;
  if (/\b(STK|STOCK|EQUITY|EQUITIES)\b/i.test(s)) return true;

  return false;
}


function groupOrdersByTicker(orders: Record<string, string>[]): TickerGroup[] {
  const map = new Map<string, TickerGroup>();

  for (const order of orders) {
    if (!isStockAssetClass(order.asset_class)) continue;

    const sym = (order.symbol || '').trim();
    if (!sym) continue;

    const qty = parseNumStr(order.quantity);
    const price = parseNumStr(order.trade_price);
    const currency = (order.currency || 'USD').trim();
    const dateRaw = (order.trade_time || '').trim();
    const datePart = dateRaw.split(',')[0]?.trim() || dateRaw.split(' ')[0] || dateRaw;

    const isBuy = qty > 0;
    const marker: TradeMarker = {
      date: datePart,
      type: isBuy ? 'buy' : 'sell',
      price,
      quantity: qty,
    };

    if (!map.has(sym)) {
      map.set(sym, {
        symbol: sym,
        currency,
        trades: [],
      });
    }

    const group = map.get(sym)!;
    group.trades.push(marker);
  }

  return Array.from(map.values()).sort((a, b) => b.trades.length - a.trades.length);
}

const TradeReviewView: React.FC<Props> = ({ data, isDark, themeColors }) => {
  const tickerGroups = useMemo(() => groupOrdersByTicker(data.orders), [data.orders]);
  const mtmPlBySymbol = useMemo(() => buildMtmPlBySymbol(data.tables), [data.tables]);
  const stockOrderStats = useMemo(() => {
    const total = data.orders.length;
    const stock = data.orders.filter((o) => isStockAssetClass(o.asset_class)).length;
    return { total, stock };
  }, [data.orders]);
  const [priceStates, setPriceStates] = useState<Map<string, TickerPriceState>>(new Map());
  const batchFetchedRef = useRef<string>('');

  useEffect(() => {
    if (!tickerGroups.length) return;
    const key = tickerGroups.map((g) => g.symbol).sort().join(',');
    if (batchFetchedRef.current === key) return;
    batchFetchedRef.current = key;

    const initStates = new Map<string, TickerPriceState>();
    tickerGroups.forEach((g) => initStates.set(g.symbol, { data: [], loading: true, error: null }));
    setPriceStates(initStates);

    const tickers = tickerGroups.map((g) => ({ symbol: g.symbol, currency: g.currency }));

    dashboardAPI
      .batchGetStockHistoricalData(tickers, 365, true)
      .then((resp) => {
        setPriceStates((prev) => {
          const next = new Map(prev);
          for (const g of tickerGroups) {
            const r = resp.results?.[g.symbol];
            if (r && r.success && r.data.length > 0) {
              next.set(g.symbol, { data: r.data, loading: false, error: null });
            } else {
              next.set(g.symbol, { data: [], loading: false, error: `No price data for ${g.symbol}` });
            }
          }
          return next;
        });
      })
      .catch(() => {
        setPriceStates((prev) => {
          const next = new Map(prev);
          for (const g of tickerGroups) {
            next.set(g.symbol, { data: [], loading: false, error: `Failed to load prices` });
          }
          return next;
        });
      });
  }, [tickerGroups]);

  if (!tickerGroups.length) {
    const desc =
      stockOrderStats.total === 0
        ? '暂无成交订单'
        : '本报告无股票类成交（复盘仅展示股票；期权/期货/外汇等已排除）';
    return <Empty description={desc} image={Empty.PRESENTED_IMAGE_SIMPLE} />;
  }

  const cardStyle: React.CSSProperties = {
    marginBottom: 12,
    borderRadius: 8,
    ...(isDark ? { background: '#1a2234', borderColor: 'rgba(255,255,255,0.06)' } : {}),
  };

  return (
    <div
      style={{
        padding: '12px 16px',
        overflowY: 'auto',
        height: '100%',
        color: themeColors.text,
      }}
    >
      <div style={{ marginBottom: 12 }}>
        <h3 style={{ margin: 0, fontSize: 15, fontWeight: 600 }}>
          Trade Review
        </h3>
        <div style={{ fontSize: 12, color: themeColors.textSecondary, marginTop: 2 }}>
          仅股票 &middot; {tickerGroups.length} 标的 &middot; {stockOrderStats.stock} 笔股票成交
          {stockOrderStats.total > stockOrderStats.stock && (
            <span>（共 {stockOrderStats.total} 笔订单，已排除非股票）</span>
          )}
          {data.period && <span> &middot; {data.period}</span>}
        </div>
      </div>

      {tickerGroups.map((group) => {
        const ps = priceStates.get(group.symbol);
        const mtm = mtmPlBySymbol.get(group.symbol);
        const mtmCcy = (data.base_currency || 'USD').trim().toUpperCase();
        return (
          <Card
            key={group.symbol}
            size="small"
            style={cardStyle}
            title={
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'space-between',
                  gap: 8,
                  flexWrap: 'wrap',
                  width: '100%',
                }}
              >
                <span style={{ fontWeight: 600, fontSize: 13 }}>{group.symbol}</span>
                {mtm && (
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
                    <Tag
                      color={mtmTagColor(mtm.commissions)}
                      style={{ margin: 0, fontSize: 11, lineHeight: '18px' }}
                    >
                      Comm {fmtMtmDisplay(mtm.commissions)}
                      {mtmCcy ? ` ${mtmCcy}` : ''}
                    </Tag>
                    <Tag
                      color={mtmTagColor(mtm.total)}
                      style={{ margin: 0, fontSize: 11, lineHeight: '18px' }}
                    >
                      Tot {fmtMtmDisplay(mtm.total)}
                      {mtmCcy ? ` ${mtmCcy}` : ''}
                    </Tag>
                  </div>
                )}
              </div>
            }
          >
            {ps?.loading ? (
              <div style={{ height: 200, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <Spin size="small" />
                <span style={{ marginLeft: 8, fontSize: 12, color: themeColors.textSecondary }}>
                  Loading price data...
                </span>
              </div>
            ) : (
              <TradeReviewChart
                symbol={group.symbol}
                trades={group.trades}
                priceData={ps?.data || []}
                loading={false}
                error={ps?.error || null}
                isDark={isDark}
                height={260}
              />
            )}
          </Card>
        );
      })}
    </div>
  );
};

export default TradeReviewView;
