import React, { useEffect, useLayoutEffect, useRef, useCallback, useState } from 'react';
import { Tooltip, Spin } from 'antd';
import {
  createChart,
  IChartApi,
  Time,
  CandlestickData as LWCandlestickData,
  HistogramData,
} from 'lightweight-charts';

/** Matches `chart.addCandlestickSeries()` so `priceToCoordinate` is typed (avoids ISeriesApi generic mismatch). */
type CandlestickSeriesApi = ReturnType<IChartApi['addCandlestickSeries']>;

export interface TradeMarker {
  date: string;
  type: 'buy' | 'sell';
  price: number;
  quantity: number;
}

interface CandlestickDataPoint {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface TradeReviewChartProps {
  symbol: string;
  trades: TradeMarker[];
  priceData: CandlestickDataPoint[];
  loading?: boolean;
  error?: string | null;
  isDark: boolean;
  height?: number;
}

const CHART_THEME = {
  light: {
    layout: { background: { color: '#ffffff' }, textColor: '#333' },
    grid: { vertLines: { color: '#f0f0f0' }, horzLines: { color: '#f0f0f0' } },
    rightPriceScale: { borderColor: '#cccccc' },
    timeScale: { borderColor: '#cccccc' },
    containerBorder: '#e0e0e0',
    candlestick: {
      upColor: 'rgba(0, 0, 0, 0)',
      downColor: '#EA6D61',
      borderUpColor: '#999999',
      borderDownColor: '#EA6D61',
      wickUpColor: '#999999',
      wickDownColor: '#EA6D61',
    },
  },
  dark: {
    layout: { background: { color: '#1a2234' }, textColor: '#e0e6f1' },
    grid: { vertLines: { color: '#364152' }, horzLines: { color: '#364152' } },
    rightPriceScale: { borderColor: '#4c5263' },
    timeScale: { borderColor: '#4c5263' },
    containerBorder: '#4c5263',
    candlestick: {
      upColor: '#38a169',
      downColor: '#e53e3e',
      borderUpColor: '#38a169',
      borderDownColor: '#e53e3e',
      wickUpColor: '#38a169',
      wickDownColor: '#e53e3e',
    },
  },
} as const;

const MARKER_COLORS = {
  buy: { bg: '#38a169', border: '#2f855a', text: '#fff' },
  sell: { bg: '#e53e3e', border: '#c53030', text: '#fff' },
};

/**
 * IB 成交时间可能是 "2025-03-27 10:30"、"2025-03-27, 10:30"、"Mar 27, 2025, 10:30" 等。
 * 不能只用 split(' ')[0]（会得到 "Mar"），需统一成 YYYY-MM-DD 再与 OHLC 的 date 对齐。
 */
function normalizeTradeDateToYmd(raw: string): string | null {
  const s = raw.trim();
  if (!s) return null;

  let m = s.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (m) return `${m[1]}-${m[2]}-${m[3]}`;

  m = s.match(/^(\d{4})[/-](\d{1,2})[/-](\d{1,2})/);
  if (m) {
    const y = m[1];
    const mo = m[2].padStart(2, '0');
    const day = m[3].padStart(2, '0');
    return `${y}-${mo}-${day}`;
  }

  m = s.match(/(\d{4})年(\d{1,2})月(\d{1,2})日?/);
  if (m) {
    const y = m[1];
    const mo = m[2].padStart(2, '0');
    const day = m[3].padStart(2, '0');
    return `${y}-${mo}-${day}`;
  }

  const parsed = Date.parse(s.replace(/,/g, ' '));
  if (!Number.isNaN(parsed)) {
    const d = new Date(parsed);
    const y = d.getFullYear();
    const mo = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${y}-${mo}-${day}`;
  }
  return null;
}

function tradeYmdToUnixSec(ymd: string): number {
  return Math.floor(new Date(ymd + 'T00:00:00').getTime() / 1000);
}

/** 无当日 K 线时（EOD 滞后），用不晚于成交日的最近一根 K 线做横轴定位 */
function lastBarOnOrBefore(
  sortedPriceData: CandlestickDataPoint[],
  tradeYmd: string,
): CandlestickDataPoint | null {
  let last: CandlestickDataPoint | null = null;
  for (const row of sortedPriceData) {
    if (row.date <= tradeYmd) last = row;
    else break;
  }
  return last;
}

const TradeReviewChart: React.FC<TradeReviewChartProps> = ({
  symbol,
  trades,
  priceData,
  loading = false,
  error = null,
  isDark,
  height = 300,
}) => {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candlestickSeriesRef = useRef<CandlestickSeriesApi | null>(null);
  const volumeSeriesRef = useRef<ReturnType<IChartApi['addHistogramSeries']> | null>(null);
  const calculateMarkerPositionsRef = useRef<() => void>(() => {});
  /** 在 chart 创建后注册，unmount 或重建前必须先执行，否则 logical range 订阅会泄漏且标记不更新 */
  const chartInteractionCleanupRef = useRef<(() => void) | null>(null);
  const [markerPositions, setMarkerPositions] = useState<
    Map<string, { left: number; top: number; marker: TradeMarker }>
  >(new Map());

  const theme = isDark ? CHART_THEME.dark : CHART_THEME.light;

  const calculateMarkerPositions = useCallback(() => {
    if (!chartRef.current || !candlestickSeriesRef.current || !priceData.length || !trades.length) {
      setMarkerPositions(new Map());
      return;
    }

    const chart = chartRef.current;
    const series = candlestickSeriesRef.current;
    const timeScale = chart.timeScale();
        const cw = chartContainerRef.current?.clientWidth ?? 0;
        const containerWidth = cw > 0 ? cw : 320;
    const leftAxisWidth = 10;
    const rightAxisWidth = 50;
    const plotAreaStartX = leftAxisWidth;
    const plotAreaEndX = containerWidth - rightAxisWidth;

    const positions = new Map<string, { left: number; top: number; marker: TradeMarker }>();
    const sortedBars = [...priceData].sort((a, b) => a.date.localeCompare(b.date));

    trades.forEach((trade, idx) => {
      const tradeYmd = normalizeTradeDateToYmd(trade.date);
      if (!tradeYmd) return;
      const bar = lastBarOnOrBefore(sortedBars, tradeYmd);
      if (!bar) return;

      const timestamp = tradeYmdToUnixSec(bar.date) as Time;
      const xCoordinate = timeScale.timeToCoordinate(timestamp);
      if (xCoordinate === null || xCoordinate < plotAreaStartX || xCoordinate > plotAreaEndX) return;

      const yCoordinate = series.priceToCoordinate(trade.price);
      if (yCoordinate === null) return;

      const key = `${idx}-${trade.date}-${trade.type}`;
      positions.set(key, {
        left: xCoordinate,
        top: yCoordinate,
        marker: trade,
      });
    });

    setMarkerPositions(positions);
  }, [priceData, trades]);

  useLayoutEffect(() => {
    calculateMarkerPositionsRef.current = calculateMarkerPositions;
  });

  // Initialize chart
  useEffect(() => {
    if (!chartContainerRef.current || !priceData.length) return;

    const runChartInteractionCleanup = () => {
      chartInteractionCleanupRef.current?.();
      chartInteractionCleanupRef.current = null;
    };
    runChartInteractionCleanup();

    if (chartRef.current) {
      chartRef.current.remove();
      chartRef.current = null;
      candlestickSeriesRef.current = null;
      volumeSeriesRef.current = null;
    }

    let timerId: ReturnType<typeof setTimeout> | null = null;
    let retryCount = 0;
    const maxRetries = 20;

    const tryInit = () => {
      retryCount++;
      if (!chartContainerRef.current) {
        if (retryCount < maxRetries) timerId = setTimeout(tryInit, 100);
        return;
      }

      const container = chartContainerRef.current;
      const containerWidth = container.clientWidth || container.offsetWidth || 600;
      if (containerWidth <= 0) {
        if (retryCount < maxRetries) timerId = setTimeout(tryInit, 100);
        return;
      }

      const chart = createChart(container, {
        width: containerWidth,
        height,
        autoSize: false,
        /** 锁定走势图：禁止拖拽平移与滚轮/触控缩放 */
        handleScroll: false,
        handleScale: false,
        layout: theme.layout,
        grid: theme.grid,
        rightPriceScale: { borderColor: theme.rightPriceScale.borderColor },
        timeScale: {
          borderColor: theme.timeScale.borderColor,
          timeVisible: false,
          secondsVisible: false,
        },
        crosshair: {
          vertLine: { color: theme.rightPriceScale.borderColor, labelBackgroundColor: theme.containerBorder },
          horzLine: { color: theme.timeScale.borderColor, labelBackgroundColor: theme.containerBorder },
        },
      });

      chartRef.current = chart;

      const candlestickSeries = chart.addCandlestickSeries(theme.candlestick);
      candlestickSeriesRef.current = candlestickSeries;

      const volumeSeries = chart.addHistogramSeries({
        priceFormat: { type: 'volume' },
        priceScaleId: '',
        scaleMargins: { top: 0.85, bottom: 0 },
      } as any);
      volumeSeriesRef.current = volumeSeries;

      // Set data
      const dataMap = new Map<number, CandlestickDataPoint>();
      priceData.forEach((item) => {
        const timestamp = Math.floor(new Date(item.date + 'T00:00:00').getTime() / 1000);
        dataMap.set(timestamp, item);
      });

      const sortedEntries = Array.from(dataMap.entries()).sort((a, b) => a[0] - b[0]);

      const candlestickData: LWCandlestickData[] = sortedEntries.map(([ts, item]) => ({
        time: ts as Time,
        open: item.open,
        high: item.high,
        low: item.low,
        close: item.close,
      }));

      const volumeData: HistogramData[] = sortedEntries.map(([ts, item]) => ({
        time: ts as Time,
        value: item.volume,
        color: item.close >= item.open
          ? (isDark ? 'rgba(56, 161, 105, 0.3)' : 'rgba(0, 0, 0, 0.1)')
          : (isDark ? 'rgba(229, 62, 62, 0.3)' : 'rgba(234, 109, 97, 0.3)'),
      }));

      candlestickSeries.setData(candlestickData);
      volumeSeries.setData(volumeData);

      // Default visible range: 3 months，右端对齐「最后成交日」；EOD 若滞后，时间轴需多出空白（setVisibleRange 的 to 会被夹到最后一根 K）
      if (trades.length > 0) {
        const tradeYmds = trades
          .map((t) => normalizeTradeDateToYmd(t.date))
          .filter((ymd): ymd is string => !!ymd);
        const maxTradeYmd = tradeYmds.length ? tradeYmds.reduce((a, b) => (a > b ? a : b)) : '';
        const tradeMs = tradeYmds.map((ymd) => new Date(ymd + 'T00:00:00').getTime());
        const lastBarMs =
          sortedEntries.length > 0 ? sortedEntries[sortedEntries.length - 1][0] * 1000 : 0;
        const maxTradeMs = tradeMs.length ? Math.max(...tradeMs) : 0;
        const endAnchorMs = Math.max(lastBarMs, maxTradeMs);
        const threeMonthsMs = 90 * 24 * 60 * 60 * 1000;
        const paddingMs = 7 * 24 * 60 * 60 * 1000;
        const rangeTo = (Math.floor((endAnchorMs + paddingMs) / 1000)) as Time;
        const rangeFrom = (Math.floor((endAnchorMs + paddingMs - threeMonthsMs) / 1000)) as Time;
        chart.timeScale().setVisibleRange({ from: rangeFrom, to: rangeTo });

        const lastBarYmd = sortedEntries.length ? sortedEntries[sortedEntries.length - 1][1].date : '';
        if (maxTradeYmd && lastBarYmd && lastBarYmd < maxTradeYmd) {
          const lr = chart.timeScale().getVisibleLogicalRange();
          if (lr) {
            const calDays = Math.ceil(
              (new Date(maxTradeYmd + 'T12:00:00').getTime() - new Date(lastBarYmd + 'T12:00:00').getTime()) /
                (24 * 60 * 60 * 1000),
            );
            const padBars = Math.min(20, Math.max(1, calDays));
            chart.timeScale().setVisibleLogicalRange({ from: lr.from, to: lr.to + padBars });
          }
        }
      } else {
        chart.timeScale().fitContent();
      }

      // 与 CandlestickChart 一致：必须在 chart 创建后再订阅，否则首次 effect 时 chartRef 为空会永远不订阅
      const scheduleRecalc = () => {
        requestAnimationFrame(() => calculateMarkerPositionsRef.current());
      };
      const onVisibleRangeChange = () => scheduleRecalc();
      chart.timeScale().subscribeVisibleLogicalRangeChange(onVisibleRangeChange);
      chart.timeScale().subscribeVisibleTimeRangeChange(onVisibleRangeChange);
      const onTimeScaleSizeChange = () => scheduleRecalc();
      chart.timeScale().subscribeSizeChange(onTimeScaleSizeChange);

      chartInteractionCleanupRef.current = () => {
        try {
          chart.timeScale().unsubscribeVisibleLogicalRangeChange(onVisibleRangeChange);
          chart.timeScale().unsubscribeVisibleTimeRangeChange(onVisibleRangeChange);
          chart.timeScale().unsubscribeSizeChange(onTimeScaleSizeChange);
        } catch {
          /* chart already removed */
        }
      };

      setTimeout(() => scheduleRecalc(), 100);
      setTimeout(() => scheduleRecalc(), 300);
    };

    timerId = setTimeout(tryInit, 50);

    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        const newWidth = chartContainerRef.current.clientWidth || chartContainerRef.current.offsetWidth;
        if (newWidth > 0) {
          chartRef.current.applyOptions({ width: newWidth, height });
          requestAnimationFrame(() => calculateMarkerPositionsRef.current());
        }
      }
    };

    window.addEventListener('resize', handleResize);

    let resizeObserver: ResizeObserver | null = null;
    if (typeof ResizeObserver !== 'undefined' && chartContainerRef.current) {
      resizeObserver = new ResizeObserver(() => handleResize());
      resizeObserver.observe(chartContainerRef.current);
    }

    return () => {
      if (timerId) clearTimeout(timerId);
      resizeObserver?.disconnect();
      window.removeEventListener('resize', handleResize);
      chartInteractionCleanupRef.current?.();
      chartInteractionCleanupRef.current = null;
      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
      }
      candlestickSeriesRef.current = null;
      volumeSeriesRef.current = null;
    };
  }, [symbol, priceData, trades, height, isDark]);

  // Theme update
  useEffect(() => {
    if (!chartRef.current || !candlestickSeriesRef.current) return;
    chartRef.current.applyOptions({
      layout: theme.layout,
      grid: theme.grid,
      rightPriceScale: { borderColor: theme.rightPriceScale.borderColor },
      timeScale: { borderColor: theme.timeScale.borderColor },
      crosshair: {
        vertLine: { color: theme.rightPriceScale.borderColor, labelBackgroundColor: theme.containerBorder },
        horzLine: { color: theme.timeScale.borderColor, labelBackgroundColor: theme.containerBorder },
      },
    });
    (candlestickSeriesRef.current as unknown as { applyOptions: (opts: any) => void }).applyOptions(theme.candlestick);
  }, [isDark]);

  if (loading) {
    return (
      <div style={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <Spin size="small" />
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center', color: isDark ? '#e0e6f1' : '#666', fontSize: 12 }}>
        {error}
      </div>
    );
  }

  if (!priceData.length) {
    return (
      <div style={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center', color: isDark ? '#a0aec0' : '#999', fontSize: 12 }}>
        No price data available for {symbol}
      </div>
    );
  }

  return (
    <div style={{ position: 'relative', width: '100%' }}>
      <div ref={chartContainerRef} style={{ width: '100%', height }} />
      {/* B/S trade markers overlay */}
      {markerPositions.size > 0 && (
        <div
          style={{
            position: 'absolute',
            top: 0,
            left: 0,
            width: '100%',
            height: '100%',
            pointerEvents: 'none',
            zIndex: 5,
          }}
        >
          {Array.from(markerPositions.entries()).map(([key, { left, top, marker }]) => {
            const colors = MARKER_COLORS[marker.type];
            const label = marker.type === 'buy' ? 'B' : 'S';
            const absQty = Math.abs(marker.quantity);
            return (
              <Tooltip
                key={key}
                title={
                  <div style={{ fontSize: 11 }}>
                    <div style={{ fontWeight: 'bold', marginBottom: 2 }}>{symbol}</div>
                    <div>Price: {marker.price.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}</div>
                    <div>Qty: {absQty.toLocaleString()}</div>
                    <div>Date: {marker.date}</div>
                  </div>
                }
                placement={marker.type === 'buy' ? 'bottom' : 'top'}
                overlayStyle={{ maxWidth: 220 }}
              >
                <div
                  style={{
                    position: 'absolute',
                    left: `${left}px`,
                    top: `${top}px`,
                    transform: marker.type === 'buy' ? 'translate(-50%, 4px)' : 'translate(-50%, -100%) translateY(-4px)',
                    cursor: 'default',
                    pointerEvents: 'auto',
                    zIndex: 10,
                  }}
                >
                  <div
                    style={{
                      width: 22,
                      height: 22,
                      borderRadius: '50%',
                      background: colors.bg,
                      border: `2px solid ${colors.border}`,
                      boxShadow: '0 1px 4px rgba(0,0,0,0.3)',
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      fontSize: 11,
                      fontWeight: 700,
                      color: colors.text,
                      lineHeight: 1,
                    }}
                  >
                    {label}
                  </div>
                </div>
              </Tooltip>
            );
          })}
        </div>
      )}
    </div>
  );
};

export default TradeReviewChart;
