import React, { useState, useEffect, useRef } from 'react';
import { Empty, Spin, Alert } from 'antd';
import { marketWeeklyAPI } from '../../services/marketWeekly';
import LightweightCandlestickChart from './LightweightCandlestickChart';
import './TechnicalViewsSection.css';

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

// 股票信息类型
interface StockInfo {
  ticker: string;
  company_name: string;
  theme_name: string;
  theme_id: string;
  category: string;
  market: string;
}

// My Themes数据类型
interface MyThemesStocks {
  us_stocks: StockInfo[];
  china_stocks: StockInfo[];
}

// 图表组件
interface ChartComponentProps {
  symbol: string;
  label: string;
  chartData: ChartData | null;
  loading: boolean;
  chartHeight?: number;
}

const ChartComponent: React.FC<ChartComponentProps> = ({
  symbol,
  label,
  chartData,
  loading,
  chartHeight = 320
}) => {
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

  return <LightweightCandlestickChart data={chartData.data} label={label} height={chartHeight} />;
};

// My Themes股票列表组件（支持按市场分组）
interface MyThemesSectionProps {
  myThemesStocks: MyThemesStocks;
  sectionId: string;
}

const MyThemesSection: React.FC<MyThemesSectionProps> = ({
  myThemesStocks,
  sectionId
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

        // 合并所有股票ticker（US + China）
        const allStocks = [...myThemesStocks.us_stocks, ...myThemesStocks.china_stocks];
        
        if (allStocks.length === 0) {
          setLoading(false);
          return;
        }

        // 准备批量请求的symbols
        const symbols = allStocks.map(stock => ({
          symbol: stock.ticker,
          label: stock.company_name
        }));

        console.log(`[MyThemes] Loading ${symbols.length} charts...`);

        // 🔥 批量获取所有OHLC数据（从 stock_prices_eod 获取）
        const result = await marketWeeklyAPI.getOHLCDataInBatch(
          symbols,
          90,
          abortControllerRef.current.signal,
          'stock'  // 🔥 My Themes 个股使用 stock_prices_eod
        );

        // 将结果转换为Map，方便查找
        const dataMap = new Map<string, ChartData>();
        result.results.forEach(chart => {
          dataMap.set(chart.symbol, chart);
        });

        setChartsData(dataMap);
        console.log(`[MyThemes] Loaded ${result.processed}/${result.total} charts`);
      } catch (error: any) {
        if (error.name === 'CanceledError' || error.code === 'ERR_CANCELED') {
          console.log('[MyThemes] Chart loading cancelled');
        } else {
          console.error('[MyThemes] Error loading charts:', error);
        }
      } finally {
        setLoading(false);
      }
    };

    loadAllCharts();

    // 🔥 清理函数：组件卸载时取消请求
    return () => {
      if (abortControllerRef.current) {
        abortControllerRef.current.abort();
        console.log('[MyThemes] Cleanup: Cancelled chart loading');
      }
    };
  }, [myThemesStocks]);

  if (!myThemesStocks.us_stocks?.length && !myThemesStocks.china_stocks?.length) {
    return (
      <div id={sectionId} className="subsection">
        <h3 className="subsection-title">My Themes</h3>
        <Empty 
          description="No My Themes stocks configured in Market Weekly" 
          style={{ padding: '40px 0' }} 
        />
      </div>
    );
  }

  return (
    <div id={sectionId} className="subsection">
      <h3 className="subsection-title">My Themes</h3>
      
      <div className="technical-region-groups">
        {/* US Stock Section */}
        {myThemesStocks.us_stocks && myThemesStocks.us_stocks.length > 0 && (
          <div className="technical-region-block">
            <h4 className="technical-region-title">US Stock</h4>
            <div className="technical-grid">
              {myThemesStocks.us_stocks.map((stock) => {
                const chartData = chartsData.get(stock.ticker) || null;

                return (
                  <div key={stock.ticker} className="technical-card">
                    <h5 className="technical-card-subtitle">
                      {stock.company_name}
                      <span style={{ fontSize: '0.85em', color: '#666', marginLeft: '8px' }}>
                        ({stock.theme_name})
                      </span>
                    </h5>
                    <div className="technical-card-content">
                      <ChartComponent
                        symbol={stock.ticker}
                        label={stock.company_name}
                        chartData={chartData}
                        loading={loading}
                        chartHeight={320}
                      />
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}

        {/* China Stock Section */}
        {myThemesStocks.china_stocks && myThemesStocks.china_stocks.length > 0 && (
          <div className="technical-region-block">
            <h4 className="technical-region-title">China Stock</h4>
            <div className="technical-grid">
              {myThemesStocks.china_stocks.map((stock) => {
                const chartData = chartsData.get(stock.ticker) || null;

                return (
                  <div key={stock.ticker} className="technical-card">
                    <h5 className="technical-card-subtitle">
                      {stock.company_name}
                      <span style={{ fontSize: '0.85em', color: '#666', marginLeft: '8px' }}>
                        ({stock.theme_name})
                      </span>
                    </h5>
                    <div className="technical-card-content">
                      <ChartComponent
                        symbol={stock.ticker}
                        label={stock.company_name}
                        chartData={chartData}
                        loading={loading}
                        chartHeight={320}
                      />
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default MyThemesSection;

