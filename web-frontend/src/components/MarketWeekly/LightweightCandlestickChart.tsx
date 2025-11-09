import React, { useEffect, useRef } from 'react';
import { createChart, IChartApi, ISeriesApi, CandlestickData, Time } from 'lightweight-charts';

interface OHLCData {
  ohlc: Array<{ time: number; open: number; high: number; low: number; close: number; volume: number }>;
  ema20: Array<{ time: number; value: number }>;
  ema50: Array<{ time: number; value: number }>;
  ema200: Array<{ time: number; value: number }>;
}

interface LightweightCandlestickChartProps {
  data: OHLCData;
  label: string;
  height?: number;
  disableInteractions?: boolean; // 是否禁用交互（拖动、缩放等）
}

const LightweightCandlestickChart: React.FC<LightweightCandlestickChartProps> = ({
  data,
  label,
  height = 400,
  disableInteractions = true
}) => {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candlestickSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const ema20SeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const ema50SeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const ema200SeriesRef = useRef<ISeriesApi<'Line'> | null>(null);

  useEffect(() => {
    if (!chartContainerRef.current) return;

    const container = chartContainerRef.current;

    const hideWatermark = () => {
      const svgElements = container.querySelectorAll('svg');
      svgElements.forEach((svg) => {
        const paths = svg.querySelectorAll('path');
        paths.forEach((path) => {
          const d = path.getAttribute('d');
          if (d && (d.includes('M2 0H0v10h6v9h21.4') || d.includes('M14 7V2H2v6h6v9h4V7h2Z'))) {
            const target = path.closest('svg') as HTMLElement | null;
            if (target) {
              target.style.display = 'none';
            }
          }
        });
      });

      const allElements = container.querySelectorAll('*');
      allElements.forEach((el) => {
        if (el.textContent && el.textContent.includes('TradingView')) {
          (el as HTMLElement).style.display = 'none';
        }
      });
    };

    hideWatermark();

    const observer = new MutationObserver(() => hideWatermark());
    observer.observe(container, { childList: true, subtree: true });

    const interval = setInterval(hideWatermark, 1000);

    return () => {
      observer.disconnect();
      clearInterval(interval);
    };
  }, []);

  useEffect(() => {
    if (!chartContainerRef.current) return;

    // 创建图表
    const chart = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: height,
      layout: {
        background: { color: '#FFFFFF' },
        textColor: '#333333',
      },
      grid: {
        vertLines: { color: '#E5E5E5' },
        horzLines: { color: '#E5E5E5' },
      },
      crosshair: {
        mode: disableInteractions ? 0 : 1, // 0 = 隐藏十字线, 1 = 正常十字线
      },
      rightPriceScale: {
        borderColor: '#CCCCCC',
      },
      timeScale: {
        borderColor: '#CCCCCC',
        timeVisible: true,
        secondsVisible: false,
      },
      // 🔥 禁用交互功能
      handleScroll: !disableInteractions, // 禁用滚轮缩放
      handleScale: !disableInteractions,  // 禁用触摸缩放
      kineticScroll: {
        touch: !disableInteractions,      // 禁用触摸惯性滚动
        mouse: !disableInteractions,      // 禁用鼠标惯性滚动
      },
    });

    chartRef.current = chart;

    // 创建Candlestick系列
    const candlestickSeries = chart.addCandlestickSeries({
      upColor: 'rgba(0, 0, 0, 0)',
      downColor: '#EA6D61',
      borderUpColor: '#999999',
      borderDownColor: '#EA6D61',
      wickUpColor: '#999999',
      wickDownColor: '#EA6D61',
    });

    candlestickSeriesRef.current = candlestickSeries;

    // 创建EMA系列
    const ema20Series = chart.addLineSeries({
      color: 'rgba(33, 150, 243, 0.3)',
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    ema20SeriesRef.current = ema20Series;

    const ema50Series = chart.addLineSeries({
      color: 'rgba(255, 152, 0, 0.3)',
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    ema50SeriesRef.current = ema50Series;

    const ema200Series = chart.addLineSeries({
      color: 'rgba(156, 39, 176, 0.3)',
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
    });
    ema200SeriesRef.current = ema200Series;

    // 设置数据
    if (data.ohlc && data.ohlc.length > 0) {
      const candleData = data.ohlc.map(d => ({
        time: d.time as Time,
        open: d.open,
        high: d.high,
        low: d.low,
        close: d.close,
      }));
      candlestickSeries.setData(candleData);

      if (data.ema20 && data.ema20.length > 0) {
        const ema20Data = data.ema20.map(d => ({
          time: d.time as Time,
          value: d.value,
        }));
        ema20Series.setData(ema20Data);
      }

      if (data.ema50 && data.ema50.length > 0) {
        const ema50Data = data.ema50.map(d => ({
          time: d.time as Time,
          value: d.value,
        }));
        ema50Series.setData(ema50Data);
      }

      if (data.ema200 && data.ema200.length > 0) {
        const ema200Data = data.ema200.map(d => ({
          time: d.time as Time,
          value: d.value,
        }));
        ema200Series.setData(ema200Data);
      }

      // 自动调整可见范围
      chart.timeScale().fitContent();
    }

    // 响应式调整
    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        chartRef.current.applyOptions({
          width: chartContainerRef.current.clientWidth,
        });
      }
    };

    window.addEventListener('resize', handleResize);

    // 清理函数
    return () => {
      window.removeEventListener('resize', handleResize);
      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
      }
    };
}, [data, height, disableInteractions]);

  return (
    <div style={{ width: '100%', margin: '20px 0' }}>
      <div
        ref={chartContainerRef}
        style={{
          width: '100%',
          position: 'relative',
          border: '1px solid #e8e8e8',
          borderRadius: '4px',
        }}
      >
        <div
          style={{
            position: 'absolute',
            top: '8px',
            left: '12px',
            display: 'flex',
            gap: '12px',
            fontSize: '11px',
            color: '#555',
            background: 'rgba(255, 255, 255, 0.85)',
            padding: '4px 8px',
            borderRadius: '4px',
            border: '1px solid rgba(0, 0, 0, 0.08)',
            zIndex: 2,
          }}
        >
          <span style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <span style={{ width: '10px', height: '2px', background: 'rgba(33, 150, 243, 0.3)', display: 'inline-block' }} />
            EMA 20
          </span>
          <span style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <span style={{ width: '10px', height: '2px', background: 'rgba(255, 152, 0, 0.3)', display: 'inline-block' }} />
            EMA 50
          </span>
          <span style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <span style={{ width: '10px', height: '2px', background: 'rgba(156, 39, 176, 0.3)', display: 'inline-block' }} />
            EMA 200
          </span>
        </div>
      </div>
      <div style={{ marginTop: '8px', fontSize: '12px', color: '#666', textAlign: 'center' }}>
        {label} - 90 Day Chart with EMAs (20, 50, 200)
      </div>
    </div>
  );
};

export default LightweightCandlestickChart;

