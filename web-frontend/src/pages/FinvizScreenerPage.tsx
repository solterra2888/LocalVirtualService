import React, { useState, useEffect } from 'react';
import {
  Layout,
  Card,
  Row,
  Col,
  Select,
  Tabs,
  Table,
  Pagination,
  Button,
  Space,
  Typography,
  message
} from 'antd';
import {
  ReloadOutlined,
  HeartOutlined,
  BellOutlined,
  LeftOutlined,
  RightOutlined
} from '@ant-design/icons';
import { useGlobalLogger } from '../contexts/LoggerContext';
import { screenerAPI, type ScreenerStockData, type ScreenerFilters } from '../services/screener';

const { Content } = Layout;
const { Title, Text } = Typography;
const { Option } = Select;

// 筛选器配置
const FILTER_OPTIONS = {
  // 第1列
  exchange: [
    { label: 'Any', value: '' },
    { label: 'NASDAQ', value: 'NASDAQ' },
    { label: 'NYSE', value: 'NYSE' },
    { label: 'AMEX', value: 'AMEX' },
    { label: 'CBOE', value: 'CBOE' }
  ],
  marketCap: [
    { label: 'Any', value: '' },
    { label: 'Nano ($0-50M)', value: 'nano' },
    { label: 'Micro ($50M-300M)', value: 'micro' },
    { label: 'Small ($300M-2B)', value: 'small' },
    { label: 'Mid ($2B-10B)', value: 'mid' },
    { label: 'Large ($10B-100B)', value: 'large' },
    { label: 'Mega ($100B+)', value: 'mega' }
  ],
  earningsDate: [
    { label: 'Any', value: '' },
    { label: 'Today', value: 'today' },
    { label: 'This Week', value: 'thisweek' },
    { label: 'Next Week', value: 'nextweek' },
    { label: 'This Month', value: 'thismonth' },
    { label: 'Next Month', value: 'nextmonth' }
  ],
  price: [
    { label: 'Any', value: '' },
    { label: 'Under $1', value: 'under1' },
    { label: '$1-$5', value: '1to5' },
    { label: '$5-$10', value: '5to10' },
    { label: '$10-$20', value: '10to20' },
    { label: '$20-$50', value: '20to50' },
    { label: 'Over $50', value: 'over50' }
  ],

  // 第2列
  index: [
    { label: 'Any', value: '' },
    { label: 'S&P 500', value: 'sp500' },
    { label: 'NASDAQ 100', value: 'nasdaq100' },
    { label: 'Dow Jones', value: 'dji' },
    { label: 'Russell 2000', value: 'russell2000' }
  ],
  dividendYield: [
    { label: 'Any', value: '' },
    { label: 'None (0%)', value: 'none' },
    { label: 'Positive (>0%)', value: 'positive' },
    { label: 'High (>5%)', value: 'high' },
    { label: 'Very High (>10%)', value: 'veryhigh' }
  ],
  averageVolume: [
    { label: 'Any', value: '' },
    { label: 'Under 50K', value: 'under50k' },
    { label: 'Under 100K', value: 'under100k' },
    { label: 'Under 500K', value: 'under500k' },
    { label: 'Under 750K', value: 'under750k' },
    { label: 'Under 1M', value: 'under1m' },
    { label: 'Over 200K', value: 'over200k' },
    { label: 'Over 400K', value: 'over400k' },
    { label: 'Over 750K', value: 'over750k' },
    { label: 'Over 1M', value: 'over1m' }
  ],
  targetPrice: [
    { label: 'Any', value: '' },
    { label: '50% Above Price', value: 'above50' },
    { label: '40% Above Price', value: 'above40' },
    { label: '30% Above Price', value: 'above30' },
    { label: '20% Above Price', value: 'above20' },
    { label: '10% Above Price', value: 'above10' },
    { label: 'Above Price', value: 'above' },
    { label: 'Below Price', value: 'below' }
  ],

  // 第3列
  sector: [
    { label: 'Any', value: '' },
    { label: 'Basic Materials', value: 'Basic Materials' },
    { label: 'Communication Services', value: 'Communication Services' },
    { label: 'Consumer Cyclical', value: 'Consumer Cyclical' },
    { label: 'Consumer Defensive', value: 'Consumer Defensive' },
    { label: 'Energy', value: 'Energy' },
    { label: 'Financial', value: 'Financial' },
    { label: 'Healthcare', value: 'Healthcare' },
    { label: 'Industrials', value: 'Industrials' },
    { label: 'Real Estate', value: 'Real Estate' },
    { label: 'Technology', value: 'Technology' },
    { label: 'Utilities', value: 'Utilities' }
  ],
  shortFloat: [
    { label: 'Any', value: '' },
    { label: 'Low (<5%)', value: 'low' },
    { label: 'High (>20%)', value: 'high' }
  ],
  relativeVolume: [
    { label: 'Any', value: '' },
    { label: 'Over 1.5', value: 'over1.5' },
    { label: 'Over 2', value: 'over2' },
    { label: 'Over 3', value: 'over3' },
    { label: 'Over 5', value: 'over5' },
    { label: 'Over 10', value: 'over10' }
  ],
  ipoDate: [
    { label: 'Any', value: '' },
    { label: 'Today', value: 'today' },
    { label: 'Yesterday', value: 'yesterday' },
    { label: 'In the last week', value: 'lastweek' },
    { label: 'In the last month', value: 'lastmonth' },
    { label: 'In the last quarter', value: 'lastquarter' },
    { label: 'In the last year', value: 'lastyear' },
    { label: 'More than a year ago', value: 'morethanyear' }
  ],

  // 第4列
  industry: [
    { label: 'Any', value: '' },
    // 这里可以添加更多细分行业
  ],
  analystRecom: [
    { label: 'Any', value: '' },
    { label: 'Strong Buy (1)', value: '1' },
    { label: 'Buy (2)', value: '2' },
    { label: 'Hold (3)', value: '3' },
    { label: 'Sell (4)', value: '4' },
    { label: 'Strong Sell (5)', value: '5' }
  ],
  currentVolume: [
    { label: 'Any', value: '' },
    { label: 'Under 50K', value: 'under50k' },
    { label: 'Under 100K', value: 'under100k' },
    { label: 'Under 500K', value: 'under500k' },
    { label: 'Under 750K', value: 'under750k' },
    { label: 'Under 1M', value: 'under1m' },
    { label: 'Over 200K', value: 'over200k' },
    { label: 'Over 400K', value: 'over400k' },
    { label: 'Over 750K', value: 'over750k' },
    { label: 'Over 1M', value: 'over1m' }
  ],
  sharesOutstanding: [
    { label: 'Any', value: '' },
    { label: 'Under 1M', value: 'under1m' },
    { label: 'Under 5M', value: 'under5m' },
    { label: 'Under 10M', value: 'under10m' },
    { label: 'Under 20M', value: 'under20m' },
    { label: 'Under 50M', value: 'under50m' },
    { label: 'Under 100M', value: 'under100m' },
    { label: 'Over 100M', value: 'over100m' }
  ],

  // 第5列
  country: [
    { label: 'Any', value: '' },
    { label: 'USA', value: 'USA' },
    { label: 'China', value: 'China' },
    { label: 'Canada', value: 'Canada' },
    { label: 'UK', value: 'UK' },
    { label: 'Germany', value: 'Germany' },
    { label: 'Japan', value: 'Japan' }
  ],
  optionShort: [
    { label: 'Any', value: '' },
    { label: 'Optionable', value: 'optionable' },
    { label: 'Shortable', value: 'shortable' }
  ],
  trades: [
    { label: 'Any', value: '' }
  ],
  float: [
    { label: 'Any', value: '' },
    { label: 'Under 1M', value: 'under1m' },
    { label: 'Under 5M', value: 'under5m' },
    { label: 'Under 10M', value: 'under10m' },
    { label: 'Under 20M', value: 'under20m' },
    { label: 'Under 50M', value: 'under50m' },
    { label: 'Under 100M', value: 'under100m' },
    { label: 'Over 100M', value: 'over100m' }
  ]
};

// 数据视图类型
type DataView = 'overview' | 'valuation' | 'financial' | 'ownership' | 'performance' | 'technical';

// 筛选器状态接口（继承API接口）
interface FilterState extends ScreenerFilters {
  exchange: string;
  marketCap: string;
  earningsDate: string;
  price: string;
  index: string;
  dividendYield: string;
  averageVolume: string;
  targetPrice: string;
  sector: string;
  shortFloat: string;
  relativeVolume: string;
  ipoDate: string;
  industry: string;
  analystRecom: string;
  currentVolume: string;
  sharesOutstanding: string;
  country: string;
  optionShort: string;
  trades: string;
  float: string;
}

const FinvizScreenerPage: React.FC = () => {
  const logger = useGlobalLogger();
  const [loading, setLoading] = useState(false);
  const [dataView, setDataView] = useState<DataView>('overview');
  const [stockData, setStockData] = useState<ScreenerStockData[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize] = useState(20);

  const [refreshInterval, setRefreshInterval] = useState('off');

  // 筛选器状态
  const [filters, setFilters] = useState<FilterState>({
    exchange: '',
    marketCap: '',
    earningsDate: '',
    price: '',
    index: '',
    dividendYield: '',
    averageVolume: '',
    targetPrice: '',
    sector: '',
    shortFloat: '',
    relativeVolume: '',
    ipoDate: '',
    industry: '',
    analystRecom: '',
    currentVolume: '',
    sharesOutstanding: '',
    country: '',
    optionShort: '',
    trades: '',
    float: ''
  });

  // 初始化加载数据
  useEffect(() => {
    const initialize = async () => {
      await loadScreenerData();
      logger.logInfo('Finviz Screener页面已加载', 'screener');
    };
    initialize();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // 监听数据视图变化
  useEffect(() => {
    loadScreenerData();
  }, [dataView, currentPage, filters]); // eslint-disable-line react-hooks/exhaustive-deps

  // 自动刷新
  useEffect(() => {
    let intervalId: NodeJS.Timeout;
    
    if (refreshInterval !== 'off') {
      const interval = refreshInterval === '10s' ? 10000 : 60000;
      intervalId = setInterval(() => {
        loadScreenerData(true);
      }, interval);
    }

    return () => {
      if (intervalId) {
        clearInterval(intervalId);
      }
    };
  }, [refreshInterval]); // eslint-disable-line react-hooks/exhaustive-deps

  const loadScreenerData = async (isRefresh = false) => {
    if (!isRefresh) {
      setLoading(true);
    }
    
    try {
      const response = await screenerAPI.getScreenerData({
        view: dataView,
        page: currentPage,
        pageSize: pageSize,
        ...filters
      });

      setStockData(response.data);
      setTotalCount(response.total);
      
      logger.logInfo(`加载筛选器数据: ${dataView} 视图, 第${currentPage}页, 共${response.total}条记录`, 'screener');
    } catch (error) {
      // 如果API失败，使用模拟数据保证界面正常运行
      logger.logWarning('API调用失败，使用模拟数据', 'screener', error);
      
      const mockData: ScreenerStockData[] = [
        {
          id: 1,
          ticker: 'AAPL',
          company: 'Apple Inc',
          sector: 'Technology',
          industry: 'Consumer Electronics',
          country: 'USA',
          market_cap: 3240000000000,
          pe_ratio: 29.15,
          price: 124.32,
          change_percent: '+1.07%',
          volume: 435154,
          forward_pe: null,
          peg: null,
          ps_ratio: null,
          pb_ratio: null,
          pcash_ratio: null,
          pfree_cash_flow: null,
          dividend: null,
          eps_ttm: null,
          shares_outstanding: null,
          shares_float: null,
          short_ratio: null,
          current_ratio: null,
          quick_ratio: null,
          lt_debt_equity: null,
          total_debt_equity: null,
          beta: null,
          average_true_range: null,
          sma_20_day: null,
          sma_50_day: null,
          sma_200_day: null,
          week_52_high: null,
          week_52_low: null,
          rsi_14: null,
          average_volume: null,
          relative_volume: null
        },
        {
          id: 2,
          ticker: 'MSFT',
          company: 'Microsoft Corporation',
          sector: 'Technology',
          industry: 'Software',
          country: 'USA',
          market_cap: 2850000000000,
          pe_ratio: 35.22,
          price: 378.85,
          change_percent: '+0.95%',
          volume: 289643,
          forward_pe: null,
          peg: null,
          ps_ratio: null,
          pb_ratio: null,
          pcash_ratio: null,
          pfree_cash_flow: null,
          dividend: null,
          eps_ttm: null,
          shares_outstanding: null,
          shares_float: null,
          short_ratio: null,
          current_ratio: null,
          quick_ratio: null,
          lt_debt_equity: null,
          total_debt_equity: null,
          beta: null,
          average_true_range: null,
          sma_20_day: null,
          sma_50_day: null,
          sma_200_day: null,
          week_52_high: null,
          week_52_low: null,
          rsi_14: null,
          average_volume: null,
          relative_volume: null
        }
      ];

      setStockData(mockData);
      setTotalCount(10359);
      
      message.warning('使用模拟数据，请检查网络连接或联系管理员');
    } finally {
      if (!isRefresh) {
        setLoading(false);
      }
    }
  };

  const handleFilterChange = (filterKey: keyof FilterState, value: string) => {
    setFilters(prev => ({
      ...prev,
      [filterKey]: value
    }));
    setCurrentPage(1); // 重置到第一页
  };

  const handleRefresh = () => {
    loadScreenerData(true);
    message.success('数据已刷新');
  };

  const handleSavePortfolio = () => {
    message.info('保存为投资组合功能开发中...');
  };

  const handleCreateAlert = () => {
    message.info('创建提醒功能开发中...');
  };

  // 根据数据视图获取表格列配置
  const getTableColumns = () => {
    const baseColumns = [
      {
        title: 'No.',
        dataIndex: 'id',
        key: 'id',
        width: 60,
        render: (value: number, record: ScreenerStockData, index: number) => 
          (currentPage - 1) * pageSize + index + 1
      },
      {
        title: 'Ticker',
        dataIndex: 'ticker',
        key: 'ticker',
        width: 80,
        sorter: true,
        render: (ticker: string) => (
          <Button 
            type="link" 
            style={{ fontWeight: 'bold', padding: 0, height: 'auto' }}
            onClick={() => {
              // TODO: 实现股票详情页面跳转
              console.log('查看股票详情:', ticker);
            }}
          >
            {ticker}
          </Button>
        )
      },
      {
        title: 'Company',
        dataIndex: 'company',
        key: 'company',
        width: 200,
        ellipsis: true
      },
      {
        title: 'Sector',
        dataIndex: 'sector',
        key: 'sector',
        width: 120,
        render: (sector: string) => (
          <Button 
            type="link" 
            style={{ padding: 0, height: 'auto' }}
            onClick={() => {
              // TODO: 实现行业筛选
              console.log('筛选行业:', sector);
            }}
          >
            {sector}
          </Button>
        )
      },
      {
        title: 'Industry',
        dataIndex: 'industry',
        key: 'industry',
        width: 150,
        ellipsis: true,
        render: (industry: string) => (
          <Button 
            type="link" 
            style={{ padding: 0, height: 'auto' }}
            onClick={() => {
              // TODO: 实现细分行业筛选
              console.log('筛选细分行业:', industry);
            }}
          >
            {industry}
          </Button>
        )
      },
      {
        title: 'Country',
        dataIndex: 'country',
        key: 'country',
        width: 80
      }
    ];

    // 根据不同视图添加特定列
    switch (dataView) {
      case 'overview':
        return [
          ...baseColumns,
          {
            title: 'Market Cap',
            dataIndex: 'market_cap',
            key: 'market_cap',
            width: 100,
            sorter: true,
            render: (marketCap: number | null) => {
              if (!marketCap) return '-';
              if (marketCap >= 1e12) return `$${(marketCap / 1e12).toFixed(2)}T`;
              if (marketCap >= 1e9) return `$${(marketCap / 1e9).toFixed(2)}B`;
              if (marketCap >= 1e6) return `$${(marketCap / 1e6).toFixed(2)}M`;
              return `$${marketCap.toLocaleString()}`;
            }
          },
          {
            title: 'P/E',
            dataIndex: 'pe_ratio',
            key: 'pe_ratio',
            width: 80,
            sorter: true,
            render: (pe: number | null) => pe ? pe.toFixed(2) : '-'
          },
          {
            title: 'Price',
            dataIndex: 'price',
            key: 'price',
            width: 80,
            sorter: true,
            render: (price: number | null) => price ? `$${price.toFixed(2)}` : '-'
          },
          {
            title: 'Change',
            dataIndex: 'change_percent',
            key: 'change_percent',
            width: 80,
            render: (change: string | null) => {
              if (!change) return '-';
              return (
                <span style={{ 
                  color: change.startsWith('+') ? '#52c41a' : change.startsWith('-') ? '#f5222d' : '#000' 
                }}>
                  {change}
                </span>
              );
            }
          },
          {
            title: 'Volume',
            dataIndex: 'volume',
            key: 'volume',
            width: 100,
            sorter: true,
            render: (volume: number | null) => volume ? volume.toLocaleString() : '-'
          }
        ];
      
      case 'valuation':
        return [
          ...baseColumns,
          {
            title: 'Market Cap',
            dataIndex: 'market_cap',
            key: 'market_cap',
            width: 100,
            render: (marketCap: number | null) => {
              if (!marketCap) return '-';
              if (marketCap >= 1e12) return `$${(marketCap / 1e12).toFixed(2)}T`;
              if (marketCap >= 1e9) return `$${(marketCap / 1e9).toFixed(2)}B`;
              if (marketCap >= 1e6) return `$${(marketCap / 1e6).toFixed(2)}M`;
              return `$${marketCap.toLocaleString()}`;
            }
          },
          {
            title: 'P/E',
            dataIndex: 'pe_ratio',
            key: 'pe_ratio',
            width: 80,
            render: (pe: number | null) => pe ? pe.toFixed(2) : '-'
          },
          {
            title: 'Forward P/E',
            dataIndex: 'forward_pe',
            key: 'forward_pe',
            width: 100,
            render: (forwardPe: number | null) => forwardPe ? forwardPe.toFixed(2) : '-'
          },
          {
            title: 'PEG',
            dataIndex: 'peg',
            key: 'peg',
            width: 80,
            render: (peg: number | null) => peg ? peg.toFixed(2) : '-'
          },
          {
            title: 'P/S',
            dataIndex: 'ps_ratio',
            key: 'ps_ratio',
            width: 80,
            render: (ps: number | null) => ps ? ps.toFixed(2) : '-'
          },
          {
            title: 'P/B',
            dataIndex: 'pb_ratio',
            key: 'pb_ratio',
            width: 80,
            render: (pb: number | null) => pb ? pb.toFixed(2) : '-'
          }
        ];
      
      case 'financial':
        return [
          ...baseColumns,
          {
            title: 'Market Cap',
            dataIndex: 'market_cap',
            key: 'market_cap',
            width: 100,
            render: (marketCap: number | null) => {
              if (!marketCap) return '-';
              if (marketCap >= 1e12) return `$${(marketCap / 1e12).toFixed(2)}T`;
              if (marketCap >= 1e9) return `$${(marketCap / 1e9).toFixed(2)}B`;
              if (marketCap >= 1e6) return `$${(marketCap / 1e6).toFixed(2)}M`;
              return `$${marketCap.toLocaleString()}`;
            }
          },
          {
            title: 'EPS TTM',
            dataIndex: 'eps_ttm',
            key: 'eps_ttm',
            width: 100,
            render: (eps: number | null) => eps ? eps.toFixed(2) : '-'
          },
          {
            title: 'EPS Growth This Year',
            dataIndex: 'eps_growth_this_year',
            key: 'eps_growth_this_year',
            width: 120,
            render: (growth: string | null) => growth || '-'
          },
          {
            title: 'EPS Growth Next Year',
            dataIndex: 'eps_growth_next_year',
            key: 'eps_growth_next_year',
            width: 120,
            render: (growth: string | null) => growth || '-'
          },
          {
            title: 'Sales Growth 5Y',
            dataIndex: 'sales_growth_past_5_years',
            key: 'sales_growth_past_5_years',
            width: 120,
            render: (growth: string | null) => growth || '-'
          },
          {
            title: 'ROA',
            dataIndex: 'return_on_assets',
            key: 'return_on_assets',
            width: 80,
            render: (roa: string | null) => roa || '-'
          },
          {
            title: 'ROE',
            dataIndex: 'return_on_equity',
            key: 'return_on_equity',
            width: 80,
            render: (roe: string | null) => roe || '-'
          },
          {
            title: 'Current Ratio',
            dataIndex: 'current_ratio',
            key: 'current_ratio',
            width: 100,
            render: (ratio: number | null) => ratio ? ratio.toFixed(2) : '-'
          },
          {
            title: 'Quick Ratio',
            dataIndex: 'quick_ratio',
            key: 'quick_ratio',
            width: 100,
            render: (ratio: number | null) => ratio ? ratio.toFixed(2) : '-'
          },
          {
            title: 'LT Debt/Equity',
            dataIndex: 'lt_debt_equity',
            key: 'lt_debt_equity',
            width: 120,
            render: (ratio: number | null) => ratio ? ratio.toFixed(2) : '-'
          },
          {
            title: 'Gross Margin',
            dataIndex: 'gross_margin',
            key: 'gross_margin',
            width: 100,
            render: (margin: string | null) => margin || '-'
          },
          {
            title: 'Operating Margin',
            dataIndex: 'operating_margin',
            key: 'operating_margin',
            width: 120,
            render: (margin: string | null) => margin || '-'
          },
          {
            title: 'Profit Margin',
            dataIndex: 'profit_margin',
            key: 'profit_margin',
            width: 120,
            render: (margin: string | null) => margin || '-'
          }
        ];
      
      case 'ownership':
        return [
          ...baseColumns,
          {
            title: 'Market Cap',
            dataIndex: 'market_cap',
            key: 'market_cap',
            width: 100,
            render: (marketCap: number | null) => {
              if (!marketCap) return '-';
              if (marketCap >= 1e12) return `$${(marketCap / 1e12).toFixed(2)}T`;
              if (marketCap >= 1e9) return `$${(marketCap / 1e9).toFixed(2)}B`;
              if (marketCap >= 1e6) return `$${(marketCap / 1e6).toFixed(2)}M`;
              return `$${marketCap.toLocaleString()}`;
            }
          },
          {
            title: 'Shares Outstanding',
            dataIndex: 'shares_outstanding',
            key: 'shares_outstanding',
            width: 120,
            render: (shares: number | null) => shares ? (shares / 1e6).toFixed(2) + 'M' : '-'
          },
          {
            title: 'Shares Float',
            dataIndex: 'shares_float',
            key: 'shares_float',
            width: 100,
            render: (shares: number | null) => shares ? (shares / 1e6).toFixed(2) + 'M' : '-'
          },
          {
            title: 'Insider Ownership',
            dataIndex: 'insider_ownership',
            key: 'insider_ownership',
            width: 120,
            render: (ownership: string | null) => ownership ? ownership + '%' : '-'
          },
          {
            title: 'Institutional Ownership',
            dataIndex: 'institutional_ownership',
            key: 'institutional_ownership',
            width: 140,
            render: (ownership: string | null) => ownership ? ownership + '%' : '-'
          },
          {
            title: 'Short Float',
            dataIndex: 'short_float',
            key: 'short_float',
            width: 100,
            render: (shortFloat: string | null) => shortFloat ? shortFloat + '%' : '-'
          },
          {
            title: 'Short Ratio',
            dataIndex: 'short_ratio',
            key: 'short_ratio',
            width: 100,
            render: (ratio: number | null) => ratio ? ratio.toFixed(2) : '-'
          },
          {
            title: 'Insider Transactions',
            dataIndex: 'insider_transactions',
            key: 'insider_transactions',
            width: 140,
            render: (transactions: string | null) => transactions || '-'
          },
          {
            title: 'Institutional Transactions',
            dataIndex: 'institutional_transactions',
            key: 'institutional_transactions',
            width: 160,
            render: (transactions: string | null) => transactions || '-'
          }
        ];
      
      case 'performance':
        return [
          ...baseColumns,
          {
            title: 'Market Cap',
            dataIndex: 'market_cap',
            key: 'market_cap',
            width: 100,
            render: (marketCap: number | null) => {
              if (!marketCap) return '-';
              if (marketCap >= 1e12) return `$${(marketCap / 1e12).toFixed(2)}T`;
              if (marketCap >= 1e9) return `$${(marketCap / 1e9).toFixed(2)}B`;
              if (marketCap >= 1e6) return `$${(marketCap / 1e6).toFixed(2)}M`;
              return `$${marketCap.toLocaleString()}`;
            }
          },
          {
            title: 'Perf Week',
            dataIndex: 'performance_week',
            key: 'performance_week',
            width: 100,
            render: (perf: string | null) => {
              if (!perf) return '-';
              const value = parseFloat(perf);
              return (
                <span style={{ 
                  color: value > 0 ? '#52c41a' : value < 0 ? '#f5222d' : '#000' 
                }}>
                  {perf}%
                </span>
              );
            }
          },
          {
            title: 'Perf Month',
            dataIndex: 'performance_month',
            key: 'performance_month',
            width: 100,
            render: (perf: string | null) => {
              if (!perf) return '-';
              const value = parseFloat(perf);
              return (
                <span style={{ 
                  color: value > 0 ? '#52c41a' : value < 0 ? '#f5222d' : '#000' 
                }}>
                  {perf}%
                </span>
              );
            }
          },
          {
            title: 'Perf Quarter',
            dataIndex: 'performance_quarter',
            key: 'performance_quarter',
            width: 100,
            render: (perf: string | null) => {
              if (!perf) return '-';
              const value = parseFloat(perf);
              return (
                <span style={{ 
                  color: value > 0 ? '#52c41a' : value < 0 ? '#f5222d' : '#000' 
                }}>
                  {perf}%
                </span>
              );
            }
          },
          {
            title: 'Perf Half Y',
            dataIndex: 'performance_half_year',
            key: 'performance_half_year',
            width: 100,
            render: (perf: string | null) => {
              if (!perf) return '-';
              const value = parseFloat(perf);
              return (
                <span style={{ 
                  color: value > 0 ? '#52c41a' : value < 0 ? '#f5222d' : '#000' 
                }}>
                  {perf}%
                </span>
              );
            }
          },
          {
            title: 'Perf Year',
            dataIndex: 'performance_year',
            key: 'performance_year',
            width: 100,
            render: (perf: string | null) => {
              if (!perf) return '-';
              const value = parseFloat(perf);
              return (
                <span style={{ 
                  color: value > 0 ? '#52c41a' : value < 0 ? '#f5222d' : '#000' 
                }}>
                  {perf}%
                </span>
              );
            }
          },
          {
            title: 'Perf YTD',
            dataIndex: 'performance_ytd',
            key: 'performance_ytd',
            width: 100,
            render: (perf: string | null) => {
              if (!perf) return '-';
              const value = parseFloat(perf);
              return (
                <span style={{ 
                  color: value > 0 ? '#52c41a' : value < 0 ? '#f5222d' : '#000' 
                }}>
                  {perf}%
                </span>
              );
            }
          },
          {
            title: 'Perf 3Y',
            dataIndex: 'performance_3_years',
            key: 'performance_3_years',
            width: 100,
            render: (perf: string | null) => {
              if (!perf) return '-';
              const value = parseFloat(perf);
              return (
                <span style={{ 
                  color: value > 0 ? '#52c41a' : value < 0 ? '#f5222d' : '#000' 
                }}>
                  {perf}%
                </span>
              );
            }
          },
          {
            title: 'Perf 5Y',
            dataIndex: 'performance_5_years',
            key: 'performance_5_years',
            width: 100,
            render: (perf: string | null) => {
              if (!perf) return '-';
              const value = parseFloat(perf);
              return (
                <span style={{ 
                  color: value > 0 ? '#52c41a' : value < 0 ? '#f5222d' : '#000' 
                }}>
                  {perf}%
                </span>
              );
            }
          },
          {
            title: 'Perf 10Y',
            dataIndex: 'performance_10_years',
            key: 'performance_10_years',
            width: 100,
            render: (perf: string | null) => {
              if (!perf) return '-';
              const value = parseFloat(perf);
              return (
                <span style={{ 
                  color: value > 0 ? '#52c41a' : value < 0 ? '#f5222d' : '#000' 
                }}>
                  {perf}%
                </span>
              );
            }
          }
        ];
      
      case 'technical':
        return [
          ...baseColumns,
          {
            title: 'Market Cap',
            dataIndex: 'market_cap',
            key: 'market_cap',
            width: 100,
            render: (marketCap: number | null) => {
              if (!marketCap) return '-';
              if (marketCap >= 1e12) return `$${(marketCap / 1e12).toFixed(2)}T`;
              if (marketCap >= 1e9) return `$${(marketCap / 1e9).toFixed(2)}B`;
              if (marketCap >= 1e6) return `$${(marketCap / 1e6).toFixed(2)}M`;
              return `$${marketCap.toLocaleString()}`;
            }
          },
          {
            title: 'Beta',
            dataIndex: 'beta',
            key: 'beta',
            width: 80,
            render: (beta: number | null) => beta ? beta.toFixed(2) : '-'
          },
          {
            title: 'ATR',
            dataIndex: 'average_true_range',
            key: 'average_true_range',
            width: 80,
            render: (atr: number | null) => atr ? atr.toFixed(2) : '-'
          },
          {
            title: 'SMA20',
            dataIndex: 'sma_20_day',
            key: 'sma_20_day',
            width: 80,
            render: (sma: number | null) => sma ? sma.toFixed(2) : '-'
          },
          {
            title: 'SMA50',
            dataIndex: 'sma_50_day',
            key: 'sma_50_day',
            width: 80,
            render: (sma: number | null) => sma ? sma.toFixed(2) : '-'
          },
          {
            title: 'SMA200',
            dataIndex: 'sma_200_day',
            key: 'sma_200_day',
            width: 80,
            render: (sma: number | null) => sma ? sma.toFixed(2) : '-'
          },
          {
            title: '52W High',
            dataIndex: 'week_52_high',
            key: 'week_52_high',
            width: 80,
            render: (high: number | null) => high ? `$${high.toFixed(2)}` : '-'
          },
          {
            title: '52W Low',
            dataIndex: 'week_52_low',
            key: 'week_52_low',
            width: 80,
            render: (low: number | null) => low ? `$${low.toFixed(2)}` : '-'
          },
          {
            title: '52W Range',
            dataIndex: 'week_52_range',
            key: 'week_52_range',
            width: 100,
            render: (range: string | null) => range || '-'
          },
          {
            title: 'RSI(14)',
            dataIndex: 'rsi_14',
            key: 'rsi_14',
            width: 80,
            render: (rsi: number | null) => rsi ? rsi.toFixed(2) : '-'
          },
          {
            title: 'Vol Week',
            dataIndex: 'volatility_week',
            key: 'volatility_week',
            width: 100,
            render: (vol: string | null) => vol || '-'
          },
          {
            title: 'Vol Month',
            dataIndex: 'volatility_month',
            key: 'volatility_month',
            width: 100,
            render: (vol: string | null) => vol || '-'
          },
          {
            title: 'Avg Volume',
            dataIndex: 'average_volume',
            key: 'average_volume',
            width: 100,
            render: (volume: number | null) => volume ? (volume / 1e6).toFixed(2) + 'M' : '-'
          },
          {
            title: 'Rel Volume',
            dataIndex: 'relative_volume',
            key: 'relative_volume',
            width: 100,
            render: (relVol: number | null) => relVol ? relVol.toFixed(2) : '-'
          }
        ];
      
      default:
        return baseColumns;
    }
  };

  return (
    <div style={{ height: '100%', background: '#f5f5f5' }}>
      <Layout style={{ height: '100%' }}>
        <Content style={{ background: '#fff', height: '100%', overflow: 'auto' }}>
          <div style={{ padding: '16px' }}>
            {/* 页面标题区域 */}
            <div style={{ marginBottom: '20px' }}>
              <Title level={2} style={{ margin: 0, color: '#1890ff' }}>
                Stock Screener
              </Title>
              <Text type="secondary">
                Find stocks that match your investment criteria
              </Text>
            </div>

            {/* 筛选器区域 */}
            <Card style={{ marginBottom: '20px' }}>
              <Row gutter={[16, 16]}>
                {/* 第1列筛选器 */}
                <Col span={4}>
                  <Space direction="vertical" style={{ width: '100%' }} size="small">
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Exchange</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.exchange}
                        onChange={(value) => handleFilterChange('exchange', value)}
                      >
                        {FILTER_OPTIONS.exchange.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Market Cap.</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.marketCap}
                        onChange={(value) => handleFilterChange('marketCap', value)}
                      >
                        {FILTER_OPTIONS.marketCap.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Earnings Date</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.earningsDate}
                        onChange={(value) => handleFilterChange('earningsDate', value)}
                      >
                        {FILTER_OPTIONS.earningsDate.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Price $</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.price}
                        onChange={(value) => handleFilterChange('price', value)}
                      >
                        {FILTER_OPTIONS.price.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                  </Space>
                </Col>

                {/* 第2列筛选器 */}
                <Col span={4}>
                  <Space direction="vertical" style={{ width: '100%' }} size="small">
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Index</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.index}
                        onChange={(value) => handleFilterChange('index', value)}
                      >
                        {FILTER_OPTIONS.index.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Dividend Yield</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.dividendYield}
                        onChange={(value) => handleFilterChange('dividendYield', value)}
                      >
                        {FILTER_OPTIONS.dividendYield.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Average Volume</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.averageVolume}
                        onChange={(value) => handleFilterChange('averageVolume', value)}
                      >
                        {FILTER_OPTIONS.averageVolume.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Target Price</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.targetPrice}
                        onChange={(value) => handleFilterChange('targetPrice', value)}
                      >
                        {FILTER_OPTIONS.targetPrice.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                  </Space>
                </Col>

                {/* 第3列筛选器 */}
                <Col span={4}>
                  <Space direction="vertical" style={{ width: '100%' }} size="small">
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Sector</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.sector}
                        onChange={(value) => handleFilterChange('sector', value)}
                      >
                        {FILTER_OPTIONS.sector.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Short Float</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.shortFloat}
                        onChange={(value) => handleFilterChange('shortFloat', value)}
                      >
                        {FILTER_OPTIONS.shortFloat.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Relative Volume</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.relativeVolume}
                        onChange={(value) => handleFilterChange('relativeVolume', value)}
                      >
                        {FILTER_OPTIONS.relativeVolume.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>IPO Date</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.ipoDate}
                        onChange={(value) => handleFilterChange('ipoDate', value)}
                      >
                        {FILTER_OPTIONS.ipoDate.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                  </Space>
                </Col>

                {/* 第4列筛选器 */}
                <Col span={4}>
                  <Space direction="vertical" style={{ width: '100%' }} size="small">
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Industry</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.industry}
                        onChange={(value) => handleFilterChange('industry', value)}
                      >
                        <Option value="">Any</Option>
                      </Select>
                    </div>
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Analyst Recom.</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.analystRecom}
                        onChange={(value) => handleFilterChange('analystRecom', value)}
                      >
                        {FILTER_OPTIONS.analystRecom.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Current Volume</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.currentVolume}
                        onChange={(value) => handleFilterChange('currentVolume', value)}
                      >
                        {FILTER_OPTIONS.currentVolume.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Shares Outstanding</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.sharesOutstanding}
                        onChange={(value) => handleFilterChange('sharesOutstanding', value)}
                      >
                        {FILTER_OPTIONS.sharesOutstanding.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                  </Space>
                </Col>

                {/* 第5列筛选器 */}
                <Col span={4}>
                  <Space direction="vertical" style={{ width: '100%' }} size="small">
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Country</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.country}
                        onChange={(value) => handleFilterChange('country', value)}
                      >
                        {FILTER_OPTIONS.country.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Option/Short</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.optionShort}
                        onChange={(value) => handleFilterChange('optionShort', value)}
                      >
                        {FILTER_OPTIONS.optionShort.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Trades</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.trades}
                        onChange={(value) => handleFilterChange('trades', value)}
                      >
                        <Option value="">Any</Option>
                      </Select>
                    </div>
                    <div>
                      <Text strong style={{ fontSize: '12px' }}>Float</Text>
                      <Select
                        size="small"
                        style={{ width: '100%' }}
                        value={filters.float}
                        onChange={(value) => handleFilterChange('float', value)}
                      >
                        {FILTER_OPTIONS.float.map(option => (
                          <Option key={option.value} value={option.value}>
                            {option.label}
                          </Option>
                        ))}
                      </Select>
                    </div>
                  </Space>
                </Col>

                {/* 操作按钮区域 */}
                <Col span={4}>
                  <Space direction="vertical" style={{ width: '100%' }} size="small">
                    <Button
                      type="primary"
                      size="small"
                      style={{ width: '100%' }}
                      onClick={() => loadScreenerData()}
                    >
                      Apply Filters
                    </Button>
                    <Button
                      size="small"
                      style={{ width: '100%' }}
                      onClick={() => setFilters({
                        exchange: '',
                        marketCap: '',
                        earningsDate: '',
                        price: '',
                        index: '',
                        dividendYield: '',
                        averageVolume: '',
                        targetPrice: '',
                        sector: '',
                        shortFloat: '',
                        relativeVolume: '',
                        ipoDate: '',
                        industry: '',
                        analystRecom: '',
                        currentVolume: '',
                        sharesOutstanding: '',
                        country: '',
                        optionShort: '',
                        trades: '',
                        float: ''
                      })}
                    >
                      Clear All
                    </Button>
                  </Space>
                </Col>
              </Row>
            </Card>

            {/* 数据视图标签 */}
            <div style={{ marginBottom: '16px' }}>
              <Tabs
                activeKey={dataView}
                onChange={(key) => setDataView(key as DataView)}
                size="large"
                tabBarExtraContent={
                  <Space>
                    <Button
                      type="link"
                      size="small"
                      icon={<HeartOutlined />}
                      onClick={handleSavePortfolio}
                    >
                      save as portfolio
                    </Button>
                    <Button
                      type="link"
                      size="small"
                      icon={<BellOutlined />}
                      onClick={handleCreateAlert}
                    >
                      create alert
                    </Button>
                    <Text type="secondary" style={{ fontSize: '12px', marginRight: '8px' }}>
                      Refresh:
                    </Text>
                    <Select
                      size="small"
                      value={refreshInterval}
                      onChange={setRefreshInterval}
                      style={{ width: '80px' }}
                    >
                      <Option value="10s">10s</Option>
                      <Option value="1min">1min</Option>
                      <Option value="off">off</Option>
                    </Select>
                    <Button
                      type="link"
                      size="small"
                      icon={<ReloadOutlined />}
                      onClick={handleRefresh}
                    />
                  </Space>
                }
              >
                <Tabs.TabPane tab="Overview" key="overview" />
                <Tabs.TabPane tab="Valuation" key="valuation" />
                <Tabs.TabPane tab="Financial" key="financial" />
                <Tabs.TabPane tab="Ownership" key="ownership" />
                <Tabs.TabPane tab="Performance" key="performance" />
                <Tabs.TabPane tab="Technical" key="technical" />
              </Tabs>
            </div>

            {/* 数据统计和分页控制 */}
            <div style={{ 
              display: 'flex', 
              justifyContent: 'space-between', 
              alignItems: 'center',
              marginBottom: '16px'
            }}>
              <div>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  #{(currentPage - 1) * pageSize + 1} / {totalCount.toLocaleString()} Total
                </Text>
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <Button
                  size="small"
                  icon={<LeftOutlined />}
                  disabled={currentPage === 1}
                  onClick={() => setCurrentPage(Math.max(1, currentPage - 1))}
                />
                <Text style={{ fontSize: '12px' }}>
                  Page {currentPage} / {Math.ceil(totalCount / pageSize)}
                </Text>
                <Button
                  size="small"
                  icon={<RightOutlined />}
                  disabled={currentPage >= Math.ceil(totalCount / pageSize)}
                  onClick={() => setCurrentPage(currentPage + 1)}
                />
              </div>
            </div>

            {/* 数据表格 */}
            <Table
              loading={loading}
              dataSource={stockData}
              columns={getTableColumns()}
              pagination={false}
              size="small"
              rowKey="id"
              scroll={{ x: 'max-content' }}
              style={{ 
                background: '#fff',
                border: '1px solid #f0f0f0'
              }}
            />

            {/* 底部分页 */}
            <div style={{ 
              display: 'flex', 
              justifyContent: 'center', 
              marginTop: '16px' 
            }}>
              <Pagination
                current={currentPage}
                pageSize={pageSize}
                total={totalCount}
                onChange={setCurrentPage}
                showSizeChanger={false}
                showQuickJumper
                showTotal={(total, range) => 
                  `${range[0]}-${range[1]} of ${total} items`
                }
              />
            </div>
          </div>
        </Content>
      </Layout>
    </div>
  );
};

export default FinvizScreenerPage;
