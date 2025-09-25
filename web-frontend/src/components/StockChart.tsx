import React, { useState, useEffect } from 'react';
import { Card, Spin, Alert, Typography, Select, Space, Row, Col, Button, Skeleton, Tag } from 'antd';
import { 
  ComposedChart,
  Line,
  Bar,
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer
} from 'recharts';
import { ReloadOutlined, DeleteOutlined } from '@ant-design/icons';
import { dashboardAPI, type StockPriceData } from '../services/dashboard';

const { Title } = Typography;
const { Option } = Select;

interface CandlestickData {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

interface StockChartProps {
  symbol: string;
  companyName?: string;
  primaryTicker?: string;
  realTimePriceData?: StockPriceData;
  onRefreshPrice?: () => void;
  priceLoading?: boolean;
  onRemoveFromWatchlist?: () => void;
  sector?: string;
  industry?: string;
  exchange?: string;
  onCompanyNameClick?: () => void;
}

// 时间范围选项
const TIME_RANGES = [
  { label: '3 Months', value: 90, days: 90 },
  { label: '6 Months', value: 180, days: 180 },
  { label: '1 Year', value: 365, days: 365 },
  { label: '3 Years', value: 1095, days: 1095 },
  { label: '5 Years', value: 1825, days: 1825 }
];

const StockChart: React.FC<StockChartProps> = ({ 
  symbol, 
  companyName,
  primaryTicker,
  realTimePriceData,
  onRefreshPrice,
  priceLoading = false,
  onRemoveFromWatchlist,
  sector,
  industry,
  exchange,
  onCompanyNameClick
}) => {
  const [data, setData] = useState<CandlestickData[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [selectedRange, setSelectedRange] = useState<number>(90); // 默认3个月
  const [yAxisDomain, setYAxisDomain] = useState<[number, number] | undefined>(undefined);

  useEffect(() => {
    if (symbol) {
      fetchHistoricalData();
    }
  }, [symbol, primaryTicker, selectedRange]);

  // 计算动态Y轴范围
  const calculateYAxisDomain = (data: CandlestickData[]): [number, number] => {
    if (!data || data.length === 0) return [0, 100];
    
    const prices = data.flatMap(d => [d.open, d.high, d.low, d.close]).filter(p => p != null);
    if (prices.length === 0) return [0, 100];
    
    const minPrice = Math.min(...prices);
    const maxPrice = Math.max(...prices);
    const priceRange = maxPrice - minPrice;
    
    // 根据价格范围调整边距，小范围股票使用更小的边距
    const margin = priceRange < 10 ? priceRange * 0.05 : priceRange * 0.1;
    const min = Math.max(0, minPrice - margin);
    const max = maxPrice + margin;
    
    return [min, max];
  };

  // 计算价格变化统计
  const getPriceStats = () => {
    if (!data || data.length < 2) return null;
    
    const firstPrice = data[0].close;
    const lastPrice = data[data.length - 1].close;
    const change = lastPrice - firstPrice;
    const changePercent = (change / firstPrice) * 100;
    
    return {
      firstPrice,
      lastPrice,
      change,
      changePercent
    };
  };

  // 格式化价格变化颜色
  const getPriceChangeColor = (changePercent?: number) => {
    if (!changePercent) return '#8c8c8c';
    return changePercent >= 0 ? '#52c41a' : '#f5222d';
  };

  // 当数据变化时重新计算Y轴范围
  useEffect(() => {
    if (data && data.length > 0) {
      const domain = calculateYAxisDomain(data);
      setYAxisDomain(domain);
    }
  }, [data]);

  const fetchHistoricalData = async () => {
    setLoading(true);
    setError(null);
    
    try {
      // Use primaryTicker if available, otherwise use symbol
      const tickerToUse = primaryTicker || symbol;
      console.log('Fetching historical data for symbol:', symbol, 'using ticker:', tickerToUse, 'days:', selectedRange);
      const response = await dashboardAPI.getStockHistoricalData(tickerToUse, selectedRange);
      console.log('API Response:', response);
      
      if (response.success && response.data) {
        setData(response.data);
        console.log('Data loaded successfully:', response.data.length, 'records');
      } else {
        setError('Failed to fetch historical data');
        console.error('API returned no data:', response);
      }
    } catch (err) {
      setError('Error loading chart data');
      console.error('Error fetching historical data:', err);
    } finally {
      setLoading(false);
    }
  };

  // Custom tooltip component
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div style={{
          backgroundColor: '#fff',
          border: '1px solid #ccc',
          borderRadius: '4px',
          padding: '8px',
          boxShadow: '0 2px 8px rgba(0,0,0,0.15)'
        }}>
          <p style={{ margin: '0 0 4px 0', fontWeight: 'bold' }}>{label}</p>
          <p style={{ margin: '0 0 2px 0', color: '#52c41a' }}>
            Open: ${data.open?.toFixed(2)}
          </p>
          <p style={{ margin: '0 0 2px 0', color: '#f5222d' }}>
            High: ${data.high?.toFixed(2)}
          </p>
          <p style={{ margin: '0 0 2px 0', color: '#1890ff' }}>
            Low: ${data.low?.toFixed(2)}
          </p>
          <p style={{ margin: '0 0 2px 0', color: '#722ed1' }}>
            Close: ${data.close?.toFixed(2)}
          </p>
          <p style={{ margin: '0', color: '#8c8c8c' }}>
            Volume: {data.volume?.toLocaleString()}
          </p>
        </div>
      );
    }
    return null;
  };

  if (loading) {
    return (
      <Card 
        title={
          <Title level={5} style={{ margin: 0 }}>
            📈 Price Chart - {companyName || symbol}
          </Title>
        }
        style={{ marginBottom: '16px' }}
      >
        <div style={{ height: '300px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <Spin size="large" />
        </div>
      </Card>
    );
  }

  if (error) {
    return (
      <Card 
        title={
          <Title level={5} style={{ margin: 0 }}>
            📈 Price Chart - {companyName || symbol}
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

  if (!data || data.length === 0) {
    return (
      <Card 
        title={
          <Title level={5} style={{ margin: 0 }}>
            📈 Price Chart - {companyName || symbol}
          </Title>
        }
        style={{ marginBottom: '16px' }}
      >
        <div style={{ height: '300px', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
          <p style={{ color: '#8c8c8c' }}>No historical data available</p>
        </div>
      </Card>
    );
  }

  return (
    <Card 
      title={
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div style={{ flex: 1 }}>
            <div style={{ marginBottom: '8px', paddingTop: '8px' }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: '8px', flexWrap: 'wrap' }}>
                <Title level={4} style={{ margin: 0 }}>
                  {onCompanyNameClick ? (
                    <span 
                      style={{ 
                        cursor: 'pointer', 
                        color: '#1890ff',
                        textDecoration: 'underline'
                      }}
                      onClick={onCompanyNameClick}
                      title="Click to view fundamentals overview"
                    >
                      {companyName || symbol}
                    </span>
                  ) : (
                    companyName || symbol
                  )}
                  {' '}({symbol})
                </Title>
                <Space size="small">
                  {exchange && <Tag color="default" style={{ fontSize: '11px', fontWeight: 'normal' }}>{exchange}</Tag>}
                  {sector && <Tag color="default" style={{ fontSize: '11px', fontWeight: 'normal' }}>{sector}</Tag>}
                  {industry && <Tag color="default" style={{ fontSize: '11px', fontWeight: 'normal' }}>{industry}</Tag>}
                </Space>
              </div>
            </div>
            {/* Real-time Price Information */}
            {realTimePriceData && (
              <div style={{ marginTop: '8px' }}>
                <Row gutter={16}>
                  <Col span={8}>
                    <div>
                      <Typography.Text strong style={{ fontSize: '12px' }}>Current Price</Typography.Text>
                      <br />
                      {priceLoading ? (
                        <Skeleton.Input style={{ width: 80, height: 20 }} size="small" />
                      ) : (
                        <Typography.Text style={{ fontSize: '16px', fontWeight: 'bold' }}>
                          {realTimePriceData.close ? `$${realTimePriceData.close.toFixed(2)}` : 'N/A'}
                        </Typography.Text>
                      )}
                    </div>
                  </Col>
                  <Col span={8}>
                    <div>
                      <Typography.Text strong style={{ fontSize: '12px' }}>Price Change %</Typography.Text>
                      <br />
                      {priceLoading ? (
                        <Skeleton.Input style={{ width: 60, height: 20 }} size="small" />
                      ) : (
                        <Typography.Text 
                          style={{ 
                            fontSize: '16px', 
                            fontWeight: 'bold',
                            color: realTimePriceData.change_percent !== undefined ? getPriceChangeColor(realTimePriceData.change_percent) : '#8c8c8c'
                          }}
                        >
                          {realTimePriceData.change_percent !== undefined 
                            ? `${realTimePriceData.change_percent >= 0 ? '+' : ''}${realTimePriceData.change_percent.toFixed(2)}%` 
                            : 'N/A'
                          }
                        </Typography.Text>
                      )}
                    </div>
                  </Col>
                  <Col span={8}>
                    <div>
                      <Typography.Text strong style={{ fontSize: '12px' }}>Volume</Typography.Text>
                      <br />
                      {priceLoading ? (
                        <Skeleton.Input style={{ width: 100, height: 20 }} size="small" />
                      ) : (
                        <Typography.Text style={{ fontSize: '14px' }}>
                          {realTimePriceData.volume ? realTimePriceData.volume.toLocaleString() : 'N/A'}
                        </Typography.Text>
                      )}
                    </div>
                  </Col>
                </Row>
                
                {/* Update timestamp */}
                {realTimePriceData.updated_at && (
                  <div style={{ 
                    marginTop: '8px',
                    fontSize: '10px',
                    color: '#999',
                    fontStyle: 'italic',
                    textAlign: 'right',
                    width: '100%'
                  }}>
                    Updated at {new Date(realTimePriceData.updated_at).toLocaleString()}
                  </div>
                )}
              </div>
            )}
          </div>
          <Space>
            <Select
              value={selectedRange}
              onChange={setSelectedRange}
              size="small"
              style={{ width: 120 }}
            >
              {TIME_RANGES.map(range => (
                <Option key={range.value} value={range.days}>
                  {range.label}
                </Option>
              ))}
            </Select>
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
            {onRemoveFromWatchlist && (
              <Button
                type="text"
                danger
                size="small"
                icon={<DeleteOutlined />}
                onClick={onRemoveFromWatchlist}
                title="Remove from watchlist"
              />
            )}
          </Space>
        </div>
      }
      style={{ marginBottom: '16px' }}
    >
      <div style={{ height: '400px' }}>
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 20, right: 30, left: 20, bottom: 5 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
            <XAxis 
              dataKey="date" 
              tick={{ fontSize: 12 }}
              tickFormatter={(value) => {
                const date = new Date(value);
                return `${date.getMonth() + 1}/${date.getDate()}`;
              }}
            />
            <YAxis 
              yAxisId="price"
              orientation="left"
              domain={yAxisDomain}
              tick={{ fontSize: 12 }}
              tickFormatter={(value) => `$${value.toFixed(0)}`}
            />
            <YAxis 
              yAxisId="volume"
              orientation="right"
              tick={{ fontSize: 12 }}
              tickFormatter={(value) => `${(value / 1000000).toFixed(1)}M`}
            />
            <Tooltip content={<CustomTooltip />} />
            
            {/* Volume bars */}
            <Bar 
              yAxisId="volume"
              dataKey="volume" 
              fill="#1890ff" 
              opacity={0.3}
              name="Volume"
            />
            
            {/* Price lines */}
            <Line 
              yAxisId="price"
              type="monotone" 
              dataKey="close" 
              stroke="#52c41a" 
              strokeWidth={3}
              dot={false}
              name="Close Price"
              activeDot={{ r: 4, stroke: '#52c41a', strokeWidth: 2 }}
            />
            <Line 
              yAxisId="price"
              type="monotone" 
              dataKey="high" 
              stroke="#f5222d" 
              strokeWidth={1.5}
              dot={false}
              name="High Price"
              strokeDasharray="2 2"
            />
            <Line 
              yAxisId="price"
              type="monotone" 
              dataKey="low" 
              stroke="#1890ff" 
              strokeWidth={1.5}
              dot={false}
              name="Low Price"
              strokeDasharray="2 2"
            />
          </ComposedChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
};

export default StockChart;