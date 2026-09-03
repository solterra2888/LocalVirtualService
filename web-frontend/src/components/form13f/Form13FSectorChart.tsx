import React, { useEffect, useMemo, useRef } from 'react';
import * as echarts from 'echarts';
import type { EChartsOption } from 'echarts';
import { QuarterSnapshot } from '../../services/form13f';

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

const LEGEND_SHORT: Record<string, string> = {
  'Broad Emerging Market': 'EM Equity',
  'Broad Developed World': 'Dev. World',
  'High-Yield/Junk Bond': 'High-Yield',
  'Investment Grade Corporate Bond': 'IG Corp Bond',
};

const fmtQuarter = (q: string): string =>
  q.replace(/^(\d{4})Q(\d)$/, '$1 Q$2');

const BAR_OPACITY = 0.65;

const withAlpha = (hex: string, alpha: number): string => {
  const h = hex.replace('#', '');
  const r = parseInt(h.slice(0, 2), 16);
  const g = parseInt(h.slice(2, 4), 16);
  const b = parseInt(h.slice(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
};

interface Form13FSectorChartProps {
  quarters: QuarterSnapshot[];
  themeMode: 'light' | 'dark';
  onQuarterClick?: (quarter: string) => void;
}

const Form13FSectorChart: React.FC<Form13FSectorChartProps> = ({
  quarters,
  themeMode,
  onQuarterClick,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<echarts.ECharts | null>(null);

  const chartData = useMemo(() => {
    const qs = quarters.map((q) => q.quarter);
    const sectors = SECTOR_ORDER.filter((sec) =>
      quarters.some((q) => (q.sector_alloc[sec] || 0) >= 0.05));
    const drawOrder = sectors.filter((s) => s !== 'Other')
      .concat(sectors.includes('Other') ? ['Other'] : []);
    return { qs, sectors, drawOrder };
  }, [quarters]);

  const option = useMemo((): EChartsOption => {
    const isDark = themeMode === 'dark';
    const textSecondary = isDark ? 'rgba(255, 255, 255, 0.5)' : '#5b6472';
    const splitLine = isDark ? '#2d3a4e' : '#eceff2';
    const axisLine = isDark ? '#364152' : '#d5dae0';
    const tooltipBg = isDark ? '#1a2234' : '#fff';
    const tooltipBorder = isDark ? '#364152' : '#e5e8ec';
    const tooltipText = isDark ? '#e0e6f1' : '#1f2733';

    const series = chartData.drawOrder.map((cat) => {
      const baseColor = SECTOR_COLORS[cat] || '#999';
      return {
        name: cat,
        type: 'bar' as const,
        stack: 'total',
        barWidth: '46%',
        emphasis: {
          focus: 'series' as const,
          itemStyle: { color: baseColor, opacity: 1 },
        },
        itemStyle: { color: withAlpha(baseColor, BAR_OPACITY) },
        data: chartData.qs.map((q) => {
          const snap = quarters.find((x) => x.quarter === q);
          return +(snap?.sector_alloc[cat] || 0).toFixed(2);
        }),
      };
    });

    return {
      animationDuration: 300,
      grid: { left: 44, right: 16, top: 16, bottom: 128 },
      legend: {
        type: 'plain',
        bottom: 0,
        left: 10,
        right: 10,
        itemWidth: 10,
        itemHeight: 10,
        itemGap: 8,
        textStyle: { fontSize: 10.5, color: textSecondary },
        formatter: (name: string) => LEGEND_SHORT[name] || name,
        data: chartData.sectors,
      },
      tooltip: {
        trigger: 'item',
        backgroundColor: tooltipBg,
        borderColor: tooltipBorder,
        textStyle: { color: tooltipText, fontSize: 12 },
        formatter: (params) => {
          const p = Array.isArray(params) ? params[0] : params;
          if (!p || p.dataIndex == null) return '';
          const quarter = fmtQuarter(chartData.qs[p.dataIndex] || '');
          const sector = String(p.seriesName || '');
          const value = Number(p.value ?? 0).toFixed(2);
          return `${quarter}<br/>${sector}: ${value}%`;
        },
      },
      xAxis: {
        type: 'category',
        data: chartData.qs.map(fmtQuarter),
        axisLine: { lineStyle: { color: axisLine } },
        axisTick: { show: false },
        axisLabel: { color: textSecondary, fontSize: 11 },
      },
      yAxis: {
        type: 'value',
        max: 100,
        interval: 25,
        name: '% of sector',
        nameTextStyle: { color: textSecondary, fontSize: 11, align: 'right', padding: [0, 6, 4, 0] },
        axisLabel: { color: textSecondary, formatter: '{value}' },
        splitLine: { lineStyle: { color: splitLine } },
      },
      series,
    };
  }, [chartData, quarters, themeMode]);

  useEffect(() => {
    if (!containerRef.current) return;
    const chart = echarts.init(containerRef.current);
    chartRef.current = chart;

    const onResize = () => chart.resize();
    window.addEventListener('resize', onResize);
    const ro = new ResizeObserver(onResize);
    ro.observe(containerRef.current);

    return () => {
      window.removeEventListener('resize', onResize);
      ro.disconnect();
      chart.dispose();
      chartRef.current = null;
    };
  }, []);

  useEffect(() => {
    chartRef.current?.setOption(option, true);
  }, [option]);

  useEffect(() => {
    const chart = chartRef.current;
    if (!chart || !onQuarterClick) return;

    const handler = (params: { componentType?: string; dataIndex?: number }) => {
      if (params.componentType !== 'series' || params.dataIndex == null) return;
      const quarter = chartData.qs[params.dataIndex];
      if (quarter) onQuarterClick(quarter);
    };
    chart.on('click', handler);
    return () => { chart.off('click', handler); };
  }, [chartData.qs, onQuarterClick]);

  if (!quarters.length) return null;

  return <div ref={containerRef} style={{ width: '100%', height: 420 }} />;
};

export default Form13FSectorChart;
