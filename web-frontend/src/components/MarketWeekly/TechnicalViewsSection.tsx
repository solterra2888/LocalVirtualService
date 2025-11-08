import React, { useState, useEffect, useRef } from 'react';
import { Collapse, Empty, Spin, Alert } from 'antd';
import { marketWeeklyAPI } from '../../services/marketWeekly';
import LightweightCandlestickChart from './LightweightCandlestickChart';
import type { AssetInfo } from '../../types/marketWeekly';

// OHLC数据类型
interface OHLCData {
  ohlc: Array<{ time: number; open: number; high: number; low: number; close: number; volume: number }>;
  ema20: Array<{ time: number; value: number }>;
  ema50: Array<{ time: number; value: number }>;
  ema200: Array<{ time: number; value: number }>;
}

// 图表数据类型
interface ChartData {
  symbol: string;
  label: string;
  data: OHLCData | null;
  success: boolean;
  error: string | null;
}

// 图表组件（接收预加载的OHLC数据）
interface ChartComponentProps {
  symbol: string;
  label: string;
  chartData: ChartData | null;
  loading: boolean;
}

const ChartComponent: React.FC<ChartComponentProps> = ({ symbol, label, chartData, loading }) => {
  if (loading) {
    return (
      <div style={{ textAlign: 'center', padding: '40px 0' }}>
        <Spin size="large" tip="Loading chart data..." />
      </div>
    );
  }

  if (!chartData) {
    return (
      <Empty
        description="Preparing chart data... please wait a moment"
        style={{ padding: '40px 0' }}
      />
    );
  }

  if (chartData.error) {
    // 如果是 404 错误，显示友好提示
    const is404 = chartData.error.includes('404') || 
                   chartData.error.includes('not found') || 
                   chartData.error.includes('No data available');
    
    if (is404) {
      return (
        <Empty
          description={`No historical data available for ${label}`}
          style={{ padding: '40px 0' }}
        />
      );
    }
    
    return (
      <Alert
        message="Chart Error"
        description={chartData.error}
        type="warning"
        showIcon
        style={{ margin: '20px 0' }}
      />
    );
  }

  if (!chartData.data) {
    return (
      <Empty
        description="No chart data available"
        style={{ padding: '40px 0' }}
      />
    );
  }

  // 使用 Lightweight Charts 渲染
  return <LightweightCandlestickChart data={chartData.data} label={label} height={400} />;
};

interface TechnicalViewsSectionProps {
  assets: AssetInfo[];
  sectionId: string;
  title: string;
  groupByRegion?: boolean;
}

// 普通资产列表（不分组）- 内部使用（支持批量加载和请求取消）
const SimpleTechnicalSectionInternal: React.FC<TechnicalViewsSectionProps> = ({
  assets,
  sectionId,
  title
}) => {
  const [chartsData, setChartsData] = useState<Map<string, ChartData>>(new Map());
  const [loading, setLoading] = useState<boolean>(true);
  const abortControllerRef = useRef<AbortController | null>(null);

  useEffect(() => {
    const loadAllCharts = async () => {
      // 取消之前的请求（如果存在）
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }

      // 创建新的 AbortController
      abortControllerRef.current = new AbortController();

      try {
        setLoading(true);

        // 准备批量请求的symbols
        const symbols = assets.map(asset => ({
          symbol: asset.primary_ticker || asset.symbol,
          label: asset.name
        }));

        console.log(`[TechnicalViews] Loading ${symbols.length} charts for ${title}...`);

        // 🔥 批量获取所有OHLC数据
        const result = await marketWeeklyAPI.getOHLCDataInBatch(
          symbols,
          90,
          abortControllerRef.current.signal
        );

        // 将结果转换为Map，方便查找
        const dataMap = new Map<string, ChartData>();
        result.results.forEach(chart => {
          dataMap.set(chart.symbol, chart);
        });

        setChartsData(dataMap);
        console.log(`[TechnicalViews] Loaded ${result.processed}/${result.total} charts for ${title}`);
      } catch (error: any) {
        if (error.name === 'CanceledError' || error.code === 'ERR_CANCELED') {
          console.log(`[TechnicalViews] Chart loading cancelled for ${title}`);
        } else {
          console.error(`[TechnicalViews] Error loading charts for ${title}:`, error);
        }
      } finally {
        setLoading(false);
      }
    };

    if (assets && assets.length > 0) {
      loadAllCharts();
    }

    // 🔥 清理函数：组件卸载时取消请求
    return () => {
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
        console.log(`[TechnicalViews] Cleanup: Cancelled chart loading for ${title}`);
      }
    };
  }, [assets, title]);

  if (!assets || assets.length === 0) {
    return (
      <Empty description="No data available" style={{ padding: '40px 0' }} />
    );
  }

  // 默认展开所有面板
  const defaultActiveKeys = assets.map(asset => asset.symbol);

  return (
    <Collapse
      defaultActiveKey={defaultActiveKeys}
      bordered={false}
      style={{ background: 'transparent' }}
    >
      {assets.map((asset) => {
        const primaryTicker = asset.primary_ticker || asset.symbol;
        const chartData = chartsData.get(primaryTicker) || null;
        
        return (
          <Collapse.Panel
            key={asset.symbol}
            header={<span style={{ fontWeight: 600, fontSize: '13px' }}>{asset.name}</span>}
          >
            <ChartComponent
              symbol={asset.symbol}
              label={asset.name}
              chartData={chartData}
              loading={loading}
            />
          </Collapse.Panel>
        );
      })}
    </Collapse>
  );
};

// 按区域分组的指数显示 - 内部使用（支持批量加载和请求取消）
const EquityIndicesTechnicalSectionInternal: React.FC<TechnicalViewsSectionProps> = ({
  assets,
  sectionId,
  groupByRegion = true
}) => {
  const [chartsData, setChartsData] = useState<Map<string, ChartData>>(new Map());
  const [loading, setLoading] = useState<boolean>(true);
  const abortControllerRef = useRef<AbortController | null>(null);

  useEffect(() => {
    const loadAllCharts = async () => {
      // 取消之前的请求（如果存在）
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
      }

      // 创建新的 AbortController
      abortControllerRef.current = new AbortController();

      try {
        setLoading(true);

        // 准备批量请求的symbols
        const symbols = assets.map(asset => ({
          symbol: asset.primary_ticker || asset.symbol,
          label: asset.name
        }));

        console.log(`[TechnicalViews] Loading ${symbols.length} charts for equity indices...`);

        // 🔥 批量获取所有OHLC数据
        const result = await marketWeeklyAPI.getOHLCDataInBatch(
          symbols,
          90,
          abortControllerRef.current.signal
        );

        // 将结果转换为Map，方便查找
        const dataMap = new Map<string, ChartData>();
        result.results.forEach(chart => {
          dataMap.set(chart.symbol, chart);
        });

        setChartsData(dataMap);
        console.log(`[TechnicalViews] Loaded ${result.processed}/${result.total} charts for equity indices`);
      } catch (error: any) {
        if (error.name === 'CanceledError' || error.code === 'ERR_CANCELED') {
          console.log('[TechnicalViews] Chart loading cancelled for equity indices');
        } else {
          console.error('[TechnicalViews] Error loading charts for equity indices:', error);
        }
      } finally {
        setLoading(false);
      }
    };

    if (assets && assets.length > 0) {
      loadAllCharts();
    }

    // 🔥 清理函数：组件卸载时取消请求
    return () => {
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
        console.log('[TechnicalViews] Cleanup: Cancelled chart loading for equity indices');
      }
    };
  }, [assets]);

  // 按区域分组
  const regions = {
    US: assets.filter(a => a.region === 'US'),
    Europe: assets.filter(a => a.region === 'Europe'),
    Asia: assets.filter(a => a.region === 'Asia'),
    Other: assets.filter(a => !['US', 'Europe', 'Asia'].includes(a.region || ''))
  };

  // 默认展开所有区域
  const defaultActiveKeys = Object.keys(regions).filter(region => regions[region as keyof typeof regions].length > 0);

  return (
    <Collapse
      defaultActiveKey={defaultActiveKeys}
      bordered={false}
      style={{ background: 'transparent' }}
    >
      {Object.entries(regions).map(([region, regionAssets]) => {
        if (regionAssets.length === 0) return null;
        
        return (
          <Collapse.Panel
            key={region}
            header={<span style={{ fontWeight: 600, fontSize: '13px' }}>{region} Markets</span>}
          >
            {regionAssets.map((asset) => {
              const primaryTicker = asset.primary_ticker || asset.symbol;
              const chartData = chartsData.get(primaryTicker) || null;
              
              return (
                <div key={asset.symbol} style={{ marginBottom: '40px' }}>
                  <h5 style={{ fontSize: '13px', fontWeight: 600, color: '#666', marginBottom: '12px' }}>
                    {asset.name}
                  </h5>
                  <ChartComponent
                    symbol={asset.symbol}
                    label={asset.name}
                    chartData={chartData}
                    loading={loading}
                  />
                </div>
              );
            })}
          </Collapse.Panel>
        );
      })}
    </Collapse>
  );
};

// 统一的默认导出组件
const TechnicalViewsSection: React.FC<TechnicalViewsSectionProps> = ({
  assets,
  sectionId,
  title,
  groupByRegion = false
}) => {
  return (
    <div id={sectionId} className="subsection">
      <h3 className="subsection-title">{title}</h3>
      {groupByRegion ? (
        <EquityIndicesTechnicalSectionInternal
          assets={assets}
          sectionId={sectionId}
          title={title}
          groupByRegion={true}
        />
      ) : (
        <SimpleTechnicalSectionInternal
          assets={assets}
          sectionId={sectionId}
          title={title}
          groupByRegion={false}
        />
      )}
    </div>
  );
};

export default TechnicalViewsSection;

// 也导出具名组件供单独使用
export { SimpleTechnicalSectionInternal as SimpleTechnicalSection };
export { EquityIndicesTechnicalSectionInternal as EquityIndicesTechnicalSection };
