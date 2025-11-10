import React, { useEffect, useState, useRef, useCallback } from 'react';
import { Spin, message } from 'antd';
import { createChart, IChartApi, ISeriesApi, CandlestickData, Time } from 'lightweight-charts';
import { dashboardAPI } from '../../../services/dashboard';
import './MobileStockChart.css';

interface MobileStockChartProps {
  symbol: string;
  primaryTicker?: string;
}

/**
 * 移动端股票图表组件
 * 使用lightweight-charts显示日K线图
 */
const MobileStockChart: React.FC<MobileStockChartProps> = ({ symbol, primaryTicker }) => {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candlestickSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  // 使用useCallback确保loadChartData函数引用稳定
  const loadChartData = useCallback(async () => {
    if (!candlestickSeriesRef.current) {
      console.log('[MobileStockChart] Candlestick series not ready yet');
      return;
    }

    try {
      const tickerForHistory = primaryTicker || symbol;
      console.log('[MobileStockChart] Loading data for', symbol, 'using ticker:', tickerForHistory, 'period: 1Y');
      setLoading(true);
      setError(null);
      
      const response = await dashboardAPI.getStockHistoricalData(tickerForHistory, 365);
      
      console.log('[MobileStockChart] Received data:', {
        success: response.success,
        dataLength: response.data?.length,
        dateRange: response.date_range
      });
      
      if (response.success && response.data && response.data.length > 0) {
        console.log('[MobileStockChart] Historical data count:', response.data.length);
        
        const chartData: CandlestickData<Time>[] = response.data.map(item => ({
          time: item.date as Time,
          open: item.open,
          high: item.high,
          low: item.low,
          close: item.close,
        }));

        if (chartData.length === 0) {
          throw new Error('No data points available for selected period');
        }

        console.log('[MobileStockChart] Setting chart data, count:', chartData.length);
        candlestickSeriesRef.current.setData(chartData);
        
        // 自动调整视图
        if (chartRef.current) {
          chartRef.current.timeScale().fitContent();
        }
        
        console.log('[MobileStockChart] Chart data loaded successfully');
      } else {
        const errorMsg = 'No historical data available for this stock';
        console.warn('[MobileStockChart]', errorMsg, 'symbol:', symbol, 'ticker tried:', tickerForHistory);
        setError(errorMsg);
      }
    } catch (error: any) {
      console.error('[MobileStockChart] Failed to load chart data:', error);
      
      // 格式化错误信息
      let errorMessage = 'Failed to load chart data';
      if (error?.message) {
        errorMessage = error.message;
      } else if (error?.response?.data?.detail) {
        errorMessage = error.response.data.detail;
      } else if (typeof error === 'string') {
        errorMessage = error;
      }
      
      console.error('[MobileStockChart] Error details:', errorMessage);
      setError(errorMessage);
      message.error(errorMessage);
    } finally {
      setLoading(false);
    }
  }, [symbol, primaryTicker]);

  // 初始化图表
  useEffect(() => {
    console.log('[MobileStockChart] Initializing chart for', symbol);
    
    if (!chartContainerRef.current) {
      console.warn('[MobileStockChart] Chart container ref not ready');
      return;
    }

    const containerWidth = chartContainerRef.current.clientWidth;
    console.log('[MobileStockChart] Container width:', containerWidth);

    // 创建图表
    const chart = createChart(chartContainerRef.current, {
      width: containerWidth || 320,
      height: 240,
      layout: {
        background: { color: '#ffffff' },
        textColor: '#333',
      },
      grid: {
        vertLines: { color: '#f0f0f0' },
        horzLines: { color: '#f0f0f0' },
      },
      timeScale: {
        borderColor: '#d1d4dc',
        timeVisible: true,
        secondsVisible: false,
      },
      rightPriceScale: {
        borderColor: '#d1d4dc',
      },
    });

    chartRef.current = chart;
    console.log('[MobileStockChart] Chart created');

    // 创建蜡烛图系列
    const candlestickSeries = chart.addCandlestickSeries({
      upColor: '#cf1322',
      downColor: '#3f8600',
      borderUpColor: '#cf1322',
      borderDownColor: '#3f8600',
      wickUpColor: '#cf1322',
      wickDownColor: '#3f8600',
    });

    candlestickSeriesRef.current = candlestickSeries;
    console.log('[MobileStockChart] Candlestick series added');

    // 响应式调整
    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        const newWidth = chartContainerRef.current.clientWidth;
        chartRef.current.applyOptions({
          width: newWidth || 320,
          height: 240
        });
        console.log('[MobileStockChart] Chart resized to:', newWidth);
      }
    };

    window.addEventListener('resize', handleResize);

    return () => {
      console.log('[MobileStockChart] Cleaning up chart');
      window.removeEventListener('resize', handleResize);
      if (chartRef.current) {
        chart.remove();
        chartRef.current = null;
        candlestickSeriesRef.current = null;
      }
    };
  }, [symbol]); // 在symbol变化时重新初始化

  // 加载数据
  useEffect(() => {
    console.log('[MobileStockChart] Triggering data load for', symbol, 'primaryTicker:', primaryTicker);
    loadChartData();
  }, [symbol, primaryTicker, loadChartData]);

  // 隐藏 TradingView 水印
  useEffect(() => {
    if (!chartContainerRef.current) return;

    const hideWatermark = () => {
      if (!chartContainerRef.current) return;

      const svgElements = chartContainerRef.current.querySelectorAll('svg');
      svgElements.forEach((svg) => {
        const paths = svg.querySelectorAll('path');
        paths.forEach((path) => {
          const d = path.getAttribute('d');
          if (
            d &&
            (d.includes('M2 0H0v10h6v9h21.4') ||
              d.includes('M14 7V2H2v6h6v9h4V7h2Z'))
          ) {
            const parent = path.closest('svg') || path.parentElement;
            if (parent) {
              (parent as HTMLElement).style.display = 'none';
            }
          }
        });
      });

      const allElements = chartContainerRef.current.querySelectorAll('*');
      allElements.forEach((el) => {
        if (el.textContent && el.textContent.includes('TradingView')) {
          (el as HTMLElement).style.display = 'none';
        }
      });
    };

    hideWatermark();

    const observer =
      typeof MutationObserver !== 'undefined'
        ? new MutationObserver(() => hideWatermark())
        : null;

    if (observer && chartContainerRef.current) {
      observer.observe(chartContainerRef.current, {
        childList: true,
        subtree: true,
      });
    }

    const interval = window.setInterval(hideWatermark, 1000);

    return () => {
      if (observer) {
        observer.disconnect();
      }
      window.clearInterval(interval);
    };
  }, [symbol]);

  return (
    <div className="mobile-stock-chart">
      <div className="chart-wrapper">
        {loading && (
          <div className="chart-loading">
            <Spin tip="Loading chart..." />
          </div>
        )}
        <div 
          ref={chartContainerRef} 
          className="chart-container"
          style={{ 
            minHeight: '240px',
            display: error && !loading ? 'none' : 'block'
          }}
        />
        {error && !loading && (
          <div className="chart-error" style={{
            padding: '40px 20px',
            textAlign: 'center',
            color: '#999',
            fontSize: '14px'
          }}>
            {error}
          </div>
        )}
      </div>
    </div>
  );
};

export default MobileStockChart;

