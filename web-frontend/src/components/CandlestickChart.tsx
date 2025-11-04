import React, { useState, useEffect, useCallback, useRef, useLayoutEffect } from 'react';
import { Card, Spin, Alert, Typography, Space, Button } from 'antd';
import { 
  createChart, 
  IChartApi, 
  ISeriesApi,
  Time,
  CandlestickData as LWCandlestickData,
  HistogramData
} from 'lightweight-charts';
import { ReloadOutlined } from '@ant-design/icons';
import { dashboardAPI, type StockPriceData } from '../services/dashboard';

const { Title } = Typography;

interface CandlestickDataPoint {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface CandlestickChartProps {
  symbol: string;
  companyName?: string;
  primaryTicker?: string;
  realTimePriceData?: StockPriceData;
  onRefreshPrice?: () => void;
  priceLoading?: boolean;
  width?: number;
  height?: number;
  // 支持传入静态数据，用于离线测试
  staticData?: CandlestickDataPoint[];
  // 是否使用模拟数据（当 API 失败时）
  useMockData?: boolean;
}

// 默认加载5年数据，初始显示最近1年
const DEFAULT_DATA_DAYS = 1825; // 5年
const INITIAL_DISPLAY_MONTHS = 12; // 初始显示最近1年

// 生成模拟数据用于离线测试
const generateMockData = (days: number = 90): CandlestickDataPoint[] => {
  const data: CandlestickDataPoint[] = [];
  const today = new Date();
  let basePrice = 100;
  
  for (let i = days; i >= 0; i--) {
    const date = new Date(today);
    date.setDate(date.getDate() - i);
    
    // 模拟价格波动
    const change = (Math.random() - 0.5) * 4; // -2 到 +2 的随机波动
    const open = basePrice;
    const close = open + change;
    const high = Math.max(open, close) + Math.random() * 2;
    const low = Math.min(open, close) - Math.random() * 2;
    const volume = Math.floor(Math.random() * 1000000) + 500000;
    
    basePrice = close; // 下一天的开盘价等于今天的收盘价
    
    data.push({
      date: date.toISOString().split('T')[0],
      open,
      high,
      low,
      close,
      volume
    });
  }
  
  return data;
};

const CandlestickChart: React.FC<CandlestickChartProps> = ({ 
  symbol, 
  companyName,
  primaryTicker,
  realTimePriceData,
  onRefreshPrice,
  priceLoading = false,
  width,
  height = 400,
  staticData,
  useMockData = false
}) => {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candlestickSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null);
  const ema20SeriesRef = useRef<ISeriesApi<'Line'> | null>(null);
  const ema50SeriesRef = useRef<ISeriesApi<'Line'> | null>(null);


  const [data, setData] = useState<CandlestickDataPoint[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const hasSetInitialRange = useRef<boolean>(false);

  // 初始化图表的辅助函数
  const initializeChart = useCallback(() => {
    if (!chartContainerRef.current) {
      console.warn('Chart container ref is not available');
      return false;
    }

    // 如果图表已存在，先清理
    if (chartRef.current) {
      console.log('Removing existing chart before creating new one');
      chartRef.current.remove();
      chartRef.current = null;
      candlestickSeriesRef.current = null;
      volumeSeriesRef.current = null;
      ema20SeriesRef.current = null;
      ema50SeriesRef.current = null;
    }

    // 确保容器有尺寸
    const containerWidth = width || chartContainerRef.current.clientWidth || chartContainerRef.current.offsetWidth || 800;
    const containerHeight = height || chartContainerRef.current.clientHeight || chartContainerRef.current.offsetHeight || 400;

    if (containerWidth <= 0 || containerHeight <= 0) {
      console.warn('Chart container has invalid dimensions:', { 
        containerWidth, 
        containerHeight,
        clientWidth: chartContainerRef.current.clientWidth,
        clientHeight: chartContainerRef.current.clientHeight,
        offsetWidth: chartContainerRef.current.offsetWidth,
        offsetHeight: chartContainerRef.current.offsetHeight
      });
      return false;
    }

    try {
      console.log('Creating chart with dimensions:', { containerWidth, containerHeight });
      
      // 确保容器有明确的尺寸样式，并且是可见的
      if (chartContainerRef.current) {
        const container = chartContainerRef.current;
        container.style.width = `${containerWidth}px`;
        container.style.height = `${containerHeight}px`;
        container.style.display = 'block';
        container.style.visibility = 'visible';
        container.style.overflow = 'hidden';
        container.style.position = 'relative';
        
        // 验证容器是否真的可见
        const computedStyle = window.getComputedStyle(container);
        const isVisible = computedStyle.display !== 'none' && 
                         computedStyle.visibility !== 'hidden' &&
                         computedStyle.opacity !== '0';
        
        console.log('Container visibility check:', {
          display: computedStyle.display,
          visibility: computedStyle.visibility,
          opacity: computedStyle.opacity,
          isVisible,
          width: containerWidth,
          height: containerHeight
        });
        
        if (!isVisible) {
          console.error('Container is not visible!');
          return false;
        }
      }
      
      console.log('Creating chart with container:', {
        element: chartContainerRef.current,
        width: containerWidth,
        height: containerHeight
      });
      
      const chart = createChart(chartContainerRef.current, {
        width: containerWidth,
        height: containerHeight,
        autoSize: false, // 禁用自动调整，使用固定尺寸
        layout: {
          background: { color: '#ffffff' },
          textColor: '#333',
        },
        grid: {
          vertLines: { color: '#f0f0f0' },
          horzLines: { color: '#f0f0f0' },
        },
        rightPriceScale: {
          borderColor: '#cccccc',
        },
        timeScale: {
          borderColor: '#cccccc',
          timeVisible: true,
          secondsVisible: false,
        },
      });

      chartRef.current = chart;
      
      // 验证图表是否创建了DOM元素（使用多个检查点）
      const checkChartDOM = (attempt: number = 1) => {
        if (!chartContainerRef.current) return;
        
        const svgElements = chartContainerRef.current.querySelectorAll('svg');
        const canvasElements = chartContainerRef.current.querySelectorAll('canvas');
        const divElements = chartContainerRef.current.querySelectorAll('div');
        
        console.log(`🔍 Chart DOM check (attempt ${attempt}):`, {
          svgCount: svgElements.length,
          canvasCount: canvasElements.length,
          divCount: divElements.length,
          totalChildren: chartContainerRef.current.children.length,
          containerHTML: chartContainerRef.current.innerHTML.substring(0, 200)
        });
        
        if (svgElements.length === 0 && canvasElements.length === 0 && divElements.length === 0) {
          if (attempt < 5) {
            // 如果还没有DOM元素，继续检查
            setTimeout(() => checkChartDOM(attempt + 1), 100);
          } else {
            console.error('❌ Chart created but no DOM elements found after 5 attempts!');
            console.error('Container element:', chartContainerRef.current);
            console.error('Container computed styles:', window.getComputedStyle(chartContainerRef.current));
            console.error('Chart instance:', chart);
            // 尝试重新创建图表
            console.log('Attempting to recreate chart...');
            if (chartRef.current) {
              chartRef.current.remove();
            }
            setTimeout(() => {
              if (chartContainerRef.current) {
                const newChart = createChart(chartContainerRef.current, {
                  width: containerWidth,
                  height: containerHeight,
                  autoSize: false,
                  layout: {
                    background: { color: '#ffffff' },
                    textColor: '#333',
                  },
                });
                chartRef.current = newChart;
                console.log('Chart recreated:', newChart);
              }
            }, 100);
          }
        } else {
          console.log('✅ Chart DOM elements found:', {
            svg: svgElements.length,
            canvas: canvasElements.length,
            div: divElements.length
          });
        }
      };

      // 立即检查，然后每100ms检查一次，最多5次
      checkChartDOM(1);

      // 验证图表是否真的创建了DOM元素
      const checkInitialDOM = () => {
        if (!chartContainerRef.current) return;
        const svgElements = chartContainerRef.current.querySelectorAll('svg');
        const canvasElements = chartContainerRef.current.querySelectorAll('canvas');
        const divElements = chartContainerRef.current.querySelectorAll('div');
        console.log('🔍 Initial DOM check after createChart:', {
          svgCount: svgElements.length,
          canvasCount: canvasElements.length,
          divCount: divElements.length,
          totalChildren: chartContainerRef.current.children.length
        });
      };
      // 立即检查
      setTimeout(checkInitialDOM, 10);
      
      // 创建 K 线图系列
      const candlestickSeries = chart.addCandlestickSeries({
        upColor: '#26a69a',
        downColor: '#ef5350',
        borderVisible: false,
        wickUpColor: '#26a69a',
        wickDownColor: '#ef5350',
      });
      candlestickSeriesRef.current = candlestickSeries;
      
      // 添加一个空的占位数据点，确保图表渲染
      // lightweight-charts 可能需要至少一个数据点才能渲染
      const placeholderData = [{
        time: Math.floor(Date.now() / 1000) as Time,
        open: 100,
        high: 100,
        low: 100,
        close: 100
      }];
      candlestickSeries.setData(placeholderData);
      console.log('Added placeholder data to trigger chart rendering');

      // 创建成交量系列（显示在图表下方）
      // 使用空字符串的 priceScaleId 会在图表底部创建独立的成交量面板
      const volumeSeries = chart.addHistogramSeries({
        priceFormat: {
          type: 'volume',
        },
        priceScaleId: '', // 空字符串表示独立的底部价格刻度
        scaleMargins: {
          top: 0.85, // 成交量占据底部15%的空间
          bottom: 0,
        },
      } as any);
      
      volumeSeriesRef.current = volumeSeries;
      console.log('Volume series created - will display at bottom of chart');

      // 创建 EMA 20 系列
      const ema20Series = chart.addLineSeries({
        color: '#2196F3',
        lineWidth: 2,
        title: 'EMA 20',
      });
      ema20SeriesRef.current = ema20Series;

      // 创建 EMA 50 系列
      const ema50Series = chart.addLineSeries({
        color: '#FF9800',
        lineWidth: 2,
        title: 'EMA 50',
      });
      ema50SeriesRef.current = ema50Series;

      console.log('✅ Chart initialized successfully');
      console.log('Chart instance:', chart);
      console.log('Chart container element:', chartContainerRef.current);
      console.log('Chart container children:', chartContainerRef.current?.children.length);
      
      // 触发图表就绪事件，让数据更新逻辑处理
      setTimeout(() => {
        console.log('Dispatching chartReady event');
        const event = new Event('chartReady');
        window.dispatchEvent(event);
      }, 100);
      
      return true;
    } catch (error) {
      console.error('Error creating chart:', error);
      return false;
    }
  }, [width, height]);

  // 提取数据更新逻辑为独立函数（必须在 useLayoutEffect 之前定义）
  const updateChartData = useCallback(() => {
    if (!data.length || !chartRef.current || !candlestickSeriesRef.current) {
      return;
    }

    console.log('Updating chart with data:', data.length, 'records');

    // 先对数据进行排序和去重处理
    // 使用 Map 来去重，保留最后一个（最新的）数据
    const dataMap = new Map<number, CandlestickDataPoint>();
    
    data.forEach((item) => {
      const timestamp = Math.floor(new Date(item.date).getTime() / 1000);
      // 如果已存在，保留最新的数据
      dataMap.set(timestamp, item);
    });
    
    // 转换为数组并按时间戳排序
    const sortedData = Array.from(dataMap.values())
      .sort((a, b) => {
        const timestampA = Math.floor(new Date(a.date).getTime() / 1000);
        const timestampB = Math.floor(new Date(b.date).getTime() / 1000);
        return timestampA - timestampB;
      });

    // 转换数据格式
    const candlestickData: LWCandlestickData<Time>[] = sortedData.map((item) => {
      const date = new Date(item.date);
      const timestamp = Math.floor(date.getTime() / 1000) as Time;
      
      return {
        time: timestamp,
        open: item.open,
        high: item.high,
        low: item.low,
        close: item.close,
      };
    });

    const volumeData: HistogramData<Time>[] = sortedData.map((item) => {
      const date = new Date(item.date);
      const timestamp = Math.floor(date.getTime() / 1000) as Time;
      const isUp = item.close >= item.open;
      
      return {
        time: timestamp,
        value: item.volume,
        color: isUp ? '#26a69a80' : '#ef535080',
      };
    });

    // 计算 EMA
    const closes = sortedData.map((d) => d.close);
    const ema20 = calculateEMA(closes, 20);
    const ema50 = calculateEMA(closes, 50);

    const ema20Data = sortedData.map((item, index) => {
      const date = new Date(item.date);
      const timestamp = Math.floor(date.getTime() / 1000) as Time;
      return {
        time: timestamp,
        value: ema20[index] || null,
      };
    }).filter((d) => d.value !== null) as any[];

    const ema50Data = sortedData.map((item, index) => {
      const date = new Date(item.date);
      const timestamp = Math.floor(date.getTime() / 1000) as Time;
      return {
        time: timestamp,
        value: ema50[index] || null,
      };
    }).filter((d) => d.value !== null) as any[];

    // 设置数据
    try {
      console.log('=== Setting chart data ===');
      console.log('Chart instance:', chartRef.current);
      console.log('Candlestick series:', candlestickSeriesRef.current);
      console.log('Data to set:', {
        candlestick: candlestickData.length,
        volume: volumeData.length,
        ema20: ema20Data.length,
        ema50: ema50Data.length
      });
      
      candlestickSeriesRef.current.setData(candlestickData);
      console.log('✅ Candlestick data set:', candlestickData.length, 'points');

      if (volumeSeriesRef.current) {
        volumeSeriesRef.current.setData(volumeData);
        console.log('Volume data set:', volumeData.length, 'points');
      }

      if (ema20SeriesRef.current && ema20Data.length > 0) {
        ema20SeriesRef.current.setData(ema20Data as any);
        console.log('EMA20 data set:', ema20Data.length, 'points');
      }

      if (ema50SeriesRef.current && ema50Data.length > 0) {
        ema50SeriesRef.current.setData(ema50Data as any);
        console.log('EMA50 data set:', ema50Data.length, 'points');
      }

      // 初始只显示最近几个月的数据，但保留所有数据供用户缩放查看
      // 只在首次加载时设置初始可见范围，避免覆盖用户的手动缩放
      if (sortedData.length > 0 && !hasSetInitialRange.current) {
        const latestTimestampNum = Math.floor(new Date(sortedData[sortedData.length - 1].date).getTime() / 1000);
        const latestTimestamp = latestTimestampNum as Time;
        // 计算初始显示范围的起始时间（最近N个月）
        const monthsAgo = INITIAL_DISPLAY_MONTHS;
        const daysAgo = monthsAgo * 30; // 近似值
        const startTimestamp = (latestTimestampNum - daysAgo * 24 * 60 * 60) as Time;
        
        // 设置可见范围
        chartRef.current.timeScale().setVisibleRange({
          from: startTimestamp,
          to: latestTimestamp,
        });
        
        hasSetInitialRange.current = true;
        console.log('Chart updated successfully, showing last', monthsAgo, 'months initially');
      } else if (sortedData.length > 0) {
        // 后续数据更新时，只调整到最新数据，不重置可见范围
        console.log('Chart updated successfully, maintaining current view');
      } else {
        // 如果没有数据，使用fitContent
        chartRef.current.timeScale().fitContent();
        console.log('Chart updated successfully');
      }
      
      // 验证图表是否真的渲染了
      setTimeout(() => {
        if (chartContainerRef.current) {
          const svgElements = chartContainerRef.current.querySelectorAll('svg');
          const canvasElements = chartContainerRef.current.querySelectorAll('canvas');
          console.log('🔍 Chart rendering check:', {
            containerExists: !!chartContainerRef.current,
            containerWidth: chartContainerRef.current.clientWidth,
            containerHeight: chartContainerRef.current.clientHeight,
            svgCount: svgElements.length,
            canvasCount: canvasElements.length,
            hasChildren: chartContainerRef.current.children.length > 0
          });
          
          if (svgElements.length === 0 && canvasElements.length === 0) {
            console.error('❌ Chart container has no SVG or Canvas elements! Chart may not be rendering.');
          } else {
            console.log('✅ Chart elements found in container');
          }
        }
      }, 500);
    } catch (error) {
      console.error('❌ Error updating chart data:', error);
    }
  }, [data]);

  // 监听图表初始化完成事件
  useEffect(() => {
    const handleChartReady = () => {
      if (data.length > 0 && chartRef.current && candlestickSeriesRef.current) {
        updateChartData();
      }
    };

    window.addEventListener('chartReady', handleChartReady);
    return () => window.removeEventListener('chartReady', handleChartReady);
  }, [data, updateChartData]);

  // 初始化图表
  useEffect(() => {
    console.log('=== Chart initialization effect triggered ===');
    console.log('Symbol:', symbol);
    console.log('Container ref available:', !!chartContainerRef.current);
    console.log('Current chart ref:', !!chartRef.current);
    
    // 如果图表已存在，先清理
    if (chartRef.current) {
      console.log('Chart already exists, cleaning up first');
      chartRef.current.remove();
      chartRef.current = null;
      candlestickSeriesRef.current = null;
      volumeSeriesRef.current = null;
      ema20SeriesRef.current = null;
      ema50SeriesRef.current = null;
    }
    
    // 使用更可靠的方式等待容器准备好
    let retryCount = 0;
    const maxRetries = 30; // 最多尝试30次，每次100ms，总共3秒
    let timerId: NodeJS.Timeout | null = null;
    
    const tryInitChart = () => {
      retryCount++;
      console.log(`[${symbol}] Attempting to initialize chart (attempt ${retryCount}/${maxRetries})`);
      
      if (!chartContainerRef.current) {
        console.warn(`[${symbol}] Container ref not available`);
        if (retryCount < maxRetries) {
          timerId = setTimeout(tryInitChart, 100);
        } else {
          console.error(`[${symbol}] Failed to get container ref after ${maxRetries} attempts`);
        }
        return;
      }

      const container = chartContainerRef.current;
      const containerWidth = width || container.clientWidth || container.offsetWidth || 800;
      const containerHeight = height || container.clientHeight || container.offsetHeight || 400;
      
      if (retryCount <= 3 || retryCount % 5 === 0) {
        console.log(`[${symbol}] Container dimensions (attempt ${retryCount}):`, {
          clientWidth: container.clientWidth,
          clientHeight: container.clientHeight,
          offsetWidth: container.offsetWidth,
          offsetHeight: container.offsetHeight,
          computedWidth: containerWidth,
          computedHeight: containerHeight
        });
      }

      // 如果容器尺寸无效，延迟重试
      if (containerWidth <= 0 || containerHeight <= 0) {
        console.warn(`[${symbol}] Container has invalid dimensions:`, { containerWidth, containerHeight });
        if (retryCount < maxRetries) {
          timerId = setTimeout(tryInitChart, 100);
        } else {
          console.error(`[${symbol}] Container dimensions invalid after ${maxRetries} attempts`);
        }
        return;
      }

      // 尝试初始化
      console.log(`[${symbol}] Calling initializeChart function...`);
      const success = initializeChart();
      if (success) {
        console.log(`[${symbol}] ✅ Chart initialized successfully on attempt ${retryCount}`);
      } else {
        console.warn(`[${symbol}] Chart initialization returned false on attempt ${retryCount}`);
        if (retryCount < maxRetries) {
          timerId = setTimeout(tryInitChart, 100);
        } else {
          console.error(`[${symbol}] Chart initialization failed after ${maxRetries} attempts`);
        }
      }
    };

    // 延迟执行，确保DOM已渲染
    timerId = setTimeout(() => {
      tryInitChart();
    }, 100);

    // 响应式调整
    const handleResize = () => {
      if (chartContainerRef.current && chartRef.current) {
        const newWidth = chartContainerRef.current.clientWidth || chartContainerRef.current.offsetWidth;
        const newHeight = chartContainerRef.current.clientHeight || chartContainerRef.current.offsetHeight;
        if (newWidth > 0 && newHeight > 0) {
          console.log('Resizing chart to:', { newWidth, newHeight });
          chartRef.current.applyOptions({
            width: newWidth,
            height: newHeight,
          });
        }
      }
    };

    window.addEventListener('resize', handleResize);

    return () => {
      if (timerId) {
        clearTimeout(timerId);
      }
      window.removeEventListener('resize', handleResize);
      if (chartRef.current) {
        console.log('Cleaning up chart on unmount');
        chartRef.current.remove();
        chartRef.current = null;
      }
      // 清理系列引用
      candlestickSeriesRef.current = null;
      volumeSeriesRef.current = null;
      ema20SeriesRef.current = null;
      ema50SeriesRef.current = null;
    };
  }, [symbol, width, height, initializeChart]); // 当 symbol 改变时也重新初始化

  // 计算移动平均线
  const calculateEMA = (data: number[], period: number): number[] => {
    const ema: number[] = [];
    const multiplier = 2 / (period + 1);
    
    // 第一个 EMA 值是 SMA
    let sum = 0;
    for (let i = 0; i < period && i < data.length; i++) {
      sum += data[i];
    }
    ema[period - 1] = sum / period;

    // 计算后续的 EMA
    for (let i = period; i < data.length; i++) {
      ema[i] = (data[i] - ema[i - 1]) * multiplier + ema[i - 1];
    }

    return ema;
  };


  // 更新图表数据
  useEffect(() => {
    if (!data.length) {
      console.log('No data available for chart update');
      return;
    }

    // 如果图表还没初始化，等待并重试
    if (!chartRef.current || !candlestickSeriesRef.current) {
      console.warn('Chart not initialized yet, waiting for initialization...', {
        hasChart: !!chartRef.current,
        hasSeries: !!candlestickSeriesRef.current
      });
      
      // 延迟重试，最多等待2秒
      let retryCount = 0;
      const maxRetries = 10; // 每次200ms，最多2秒
      
      const retryTimer = setInterval(() => {
        retryCount++;
        
        if (chartRef.current && candlestickSeriesRef.current) {
          clearInterval(retryTimer);
          console.log('Chart initialized, updating data now');
          updateChartData();
        } else if (retryCount >= maxRetries) {
          clearInterval(retryTimer);
          console.error('Chart initialization timeout, giving up');
        }
      }, 200);
      
      return () => clearInterval(retryTimer);
    }

    updateChartData();
  }, [data, updateChartData]);

  const fetchHistoricalData = useCallback(async () => {
    // 如果提供了静态数据，直接使用
    if (staticData) {
      setData(staticData);
      return;
    }

    // 如果启用了模拟数据，直接生成
    if (useMockData) {
      const mockData = generateMockData(DEFAULT_DATA_DAYS);
      setData(mockData);
      return;
    }

    setLoading(true);
    setError(null);
    
    try {
      const tickerToUse = primaryTicker || symbol;
      console.log('Fetching historical data for symbol:', symbol, 'using ticker:', tickerToUse, 'days:', DEFAULT_DATA_DAYS);
      const response = await dashboardAPI.getStockHistoricalData(tickerToUse, DEFAULT_DATA_DAYS);
      console.log('API Response:', response);
      
      if (response.success && response.data) {
        setData(response.data);
        console.log('Data loaded successfully:', response.data.length, 'records');
      } else {
        setError('Failed to fetch historical data');
        console.error('API returned no data:', response);
        // API 失败时，如果启用了模拟数据选项，使用模拟数据
        if (useMockData) {
          const mockData = generateMockData(DEFAULT_DATA_DAYS);
          setData(mockData);
          setError(null); // 清除错误，因为使用了模拟数据
        }
      }
    } catch (err) {
      setError('Error loading chart data');
      console.error('Error fetching historical data:', err);
      // API 失败时，如果启用了模拟数据选项，使用模拟数据
      if (useMockData) {
        const mockData = generateMockData(DEFAULT_DATA_DAYS);
        setData(mockData);
        setError(null); // 清除错误，因为使用了模拟数据
      }
    } finally {
      setLoading(false);
    }
  }, [symbol, primaryTicker, staticData, useMockData]);

  useEffect(() => {
    if (symbol) {
      console.log('Symbol changed, fetching data for:', symbol);
      // 重置初始范围标志，以便新symbol加载时重新设置初始可见范围
      hasSetInitialRange.current = false;
      fetchHistoricalData();
    } else {
      console.warn('No symbol provided');
    }
  }, [symbol, fetchHistoricalData]);


  // 格式化价格变化颜色
  const getPriceChangeColor = (changePercent?: number) => {
    if (!changePercent) return '#8c8c8c';
    return changePercent >= 0 ? '#52c41a' : '#f5222d';
  };

  if (loading && !data.length) {
    return (
      <Card 
        title={
          <Title level={5} style={{ margin: 0 }}>
            📊 Candlestick Chart - {companyName || symbol}
          </Title>
        }
        style={{ marginBottom: '16px' }}
      >
        <div style={{ height: '400px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <Spin size="large" />
        </div>
      </Card>
    );
  }

  if (error && !data.length) {
    return (
      <Card 
        title={
          <Title level={5} style={{ margin: 0 }}>
            📊 Candlestick Chart - {companyName || symbol}
          </Title>
        }
        style={{ marginBottom: '16px' }}
      >
        <Alert
          message="Chart Error"
          description={error}
          type="error"
          showIcon
        />
      </Card>
    );
  }

  return (
    <Card 
      title={
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Title level={4} style={{ margin: 0 }}>
            {companyName || symbol} ({symbol})
          </Title>
          <Space>
            {onRefreshPrice && (
              <Button
                type="text"
                size="small"
                icon={<ReloadOutlined />}
                onClick={onRefreshPrice}
                loading={priceLoading}
                title="Refresh Price"
              />
            )}
          </Space>
        </div>
      }
      style={{ marginBottom: '16px' }}
    >
      {/* Real-time Price Information */}
      {realTimePriceData && (
        <div style={{ marginBottom: '16px', padding: '12px', background: '#f5f5f5', borderRadius: '4px' }}>
          <Space size="large">
            <div>
              <Typography.Text strong style={{ fontSize: '12px' }}>Current Price</Typography.Text>
              <br />
              <Typography.Text style={{ fontSize: '16px', fontWeight: 'bold' }}>
                {realTimePriceData.close ? `$${realTimePriceData.close.toFixed(2)}` : 'N/A'}
              </Typography.Text>
            </div>
            <div>
              <Typography.Text strong style={{ fontSize: '12px' }}>Price Change %</Typography.Text>
              <br />
              <Typography.Text 
                style={{ 
                  fontSize: '16px', 
                  fontWeight: 'bold',
                  color: realTimePriceData.change_percent !== undefined 
                    ? getPriceChangeColor(realTimePriceData.change_percent) 
                    : '#8c8c8c'
                }}
              >
                {realTimePriceData.change_percent !== undefined 
                  ? `${realTimePriceData.change_percent >= 0 ? '+' : ''}${realTimePriceData.change_percent.toFixed(2)}%` 
                  : 'N/A'
                }
              </Typography.Text>
            </div>
            <div>
              <Typography.Text strong style={{ fontSize: '12px' }}>Volume</Typography.Text>
              <br />
              <Typography.Text style={{ fontSize: '14px' }}>
                {realTimePriceData.volume ? realTimePriceData.volume.toLocaleString() : 'N/A'}
              </Typography.Text>
            </div>
          </Space>
        </div>
      )}

      <div style={{ position: 'relative', width: '100%', height: `${height}px` }}>
        {loading && (
          <div style={{
            position: 'absolute',
            top: 0,
            left: 0,
            right: 0,
            bottom: 0,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            background: 'rgba(255, 255, 255, 0.8)',
            zIndex: 10
          }}>
            <Spin size="large" />
          </div>
        )}
        <div 
          ref={chartContainerRef}
          style={{ 
            width: '100%', 
            height: `${height}px`,
            minWidth: '400px',
            minHeight: '300px',
            position: 'relative',
            backgroundColor: '#ffffff',
            display: 'block',
            overflow: 'hidden',
            visibility: 'visible'
          }} 
          id="candlestick-chart-container"
        />
        {!data.length && !loading && !chartRef.current && (
          <div style={{ 
            position: 'absolute',
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            color: '#8c8c8c'
          }}>
            <p>No historical data available</p>
          </div>
        )}
      </div>
    </Card>
  );
};

export default CandlestickChart;
