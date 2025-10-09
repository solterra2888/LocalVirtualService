import React, { useState, useEffect, useCallback } from 'react';
import { Card, Spin, Alert, Typography, Select, Space, Row, Col, Button, Skeleton, Tag, message, Dropdown, Menu } from 'antd';
import { 
  ComposedChart,
  Line,
  Bar,
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer,
  ReferenceDot
} from 'recharts';
import { ReloadOutlined, DeleteOutlined, PlusOutlined, SettingOutlined, EyeOutlined, EyeInvisibleOutlined } from '@ant-design/icons';
import { dashboardAPI, type StockPriceData } from '../services/dashboard';
import EventEditModal from './EventEditModal';
import EventDot from './EventDot';
import { useChartEvents } from '../hooks/useChartEvents';
import { ChartEvent, EventFormData } from '../types/events';

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
  
  // 事件标记相关状态
  const [showEvents, setShowEvents] = useState<boolean>(true);
  const [eventModalVisible, setEventModalVisible] = useState<boolean>(false);
  const [editingEvent, setEditingEvent] = useState<ChartEvent | null>(null);
  const [clickedDate, setClickedDate] = useState<string | null>(null);
  
  
  // 使用事件管理hook - 优先使用primaryTicker作为事件symbol
  // 这样确保事件保存到数据库时使用标准化的股票代码，与系统其他部分保持一致
  const eventSymbol = primaryTicker || symbol;
  const {
    events,
    loading: eventsLoading,
    addEvent,
    updateEvent
  } = useChartEvents(eventSymbol);

  const fetchHistoricalData = useCallback(async () => {
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
  }, [symbol, primaryTicker, selectedRange]);

  useEffect(() => {
    if (symbol) {
      fetchHistoricalData();
    }
  }, [symbol, fetchHistoricalData]);

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


  // 事件处理函数
  const handleAddEvent = useCallback(() => {
    setEditingEvent(null);
    setEventModalVisible(true);
  }, []);

  const handleEditEvent = useCallback((event: ChartEvent) => {
    setEditingEvent(event);
    setEventModalVisible(true);
  }, []);


  const handleSaveEvent = useCallback(async (eventData: EventFormData) => {
    try {
      if (editingEvent) {
        await updateEvent(editingEvent.id, eventData);
        message.success('Event updated successfully');
      } else {
        await addEvent(eventData);
        message.success('Event added successfully');
      }
      setEventModalVisible(false);
      setEditingEvent(null);
      setClickedDate(null);
    } catch (error) {
      message.error('Failed to save event');
    }
  }, [editingEvent, updateEvent, addEvent]);

  // 处理图表点击事件
  const handleChartClick = useCallback((event: any) => {
    if (!event || !event.activeLabel) return;
    
    const clickedDateStr = event.activeLabel;
    setClickedDate(clickedDateStr);
    setEditingEvent(null);
    setEventModalVisible(true);
  }, []);

  // 获取事件标记的Y坐标值（价格）
  const getEventYValue = useCallback((event: ChartEvent, chartData: CandlestickData[]) => {
    const dataPoint = chartData.find(d => d.date === event.date);
    if (!dataPoint || !yAxisDomain) return null;

    const [minPrice, maxPrice] = yAxisDomain;
    const priceRange = maxPrice - minPrice;
    
    switch (event.position) {
      case 'top':
        return maxPrice - (priceRange * 0.1); // 距离顶部10%
      case 'bottom':
        return minPrice + (priceRange * 0.1); // 距离底部10%
      default: // middle
        return (dataPoint.high + dataPoint.low) / 2; // 当日平均价格
    }
  }, [yAxisDomain]);

  // Custom tooltip component
  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      const eventsOnDate = showEvents ? events.filter(e => e.date === label) : [];
      
      // 计算涨跌幅 (基于开盘价)
      const calculateChangePercent = (open: number, close: number) => {
        if (!open || !close || open === 0) return null;
        return ((close - open) / open) * 100;
      };
      
      const changePercent = calculateChangePercent(data.open, data.close);
      const changeAmount = data.open && data.close ? data.close - data.open : null;
      
      return (
        <div style={{
          backgroundColor: '#fff',
          border: '1px solid #ccc',
          borderRadius: '4px',
          padding: '8px',
          boxShadow: '0 2px 8px rgba(0,0,0,0.15)',
          maxWidth: '300px'
        }}>
          <p style={{ margin: '0 0 4px 0', fontWeight: 'bold' }}>{label}</p>
          <p style={{ margin: '0 0 2px 0', color: '#333' }}>
            Open: ${data.open?.toFixed(2)}
          </p>
          <p style={{ margin: '0 0 2px 0', color: '#333' }}>
            High: ${data.high?.toFixed(2)}
          </p>
          <p style={{ margin: '0 0 2px 0', color: '#333' }}>
            Low: ${data.low?.toFixed(2)}
          </p>
          <p style={{ margin: '0 0 2px 0', color: '#333' }}>
            Close: ${data.close?.toFixed(2)}
          </p>
          {changePercent !== null && changeAmount !== null && (
            <p style={{ 
              margin: '0 0 2px 0', 
              color: changePercent >= 0 ? '#52c41a' : '#f5222d',
              fontWeight: 'bold'
            }}>
              Change: {changeAmount >= 0 ? '+' : ''}${changeAmount.toFixed(2)} ({changePercent >= 0 ? '+' : ''}{changePercent.toFixed(2)}%)
            </p>
          )}
          <p style={{ margin: '0', color: '#333' }}>
            Volume: {data.volume?.toLocaleString()}
          </p>
          
          {eventsOnDate.length > 0 && (
            <div style={{ borderTop: '1px solid #f0f0f0', paddingTop: '8px', marginTop: '8px' }}>
              <p style={{ margin: '0 0 4px 0', fontSize: '12px', fontWeight: 'bold', color: '#1890ff' }}>
                📅 Events Today ({eventsOnDate.length})
              </p>
              {eventsOnDate.slice(0, 3).map(event => (
                <p key={event.id} style={{ margin: '0 0 2px 0', fontSize: '11px', color: '#666' }}>
                  • {event.title}
                </p>
              ))}
              {eventsOnDate.length > 3 && (
                <p style={{ margin: '0', fontSize: '10px', color: '#999' }}>
                  ...and {eventsOnDate.length - 3} more events
                </p>
              )}
            </div>
          )}
          
          <div style={{ borderTop: '1px solid #f0f0f0', paddingTop: '6px', marginTop: '6px' }}>
            <p style={{ margin: '0', fontSize: '10px', color: '#999', fontStyle: 'italic' }}>
              💡 Click here to add event marker
            </p>
          </div>
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
            
            {/* 事件管理按钮 */}
            <Dropdown
              overlay={
                <Menu>
                  <Menu.Item 
                    key="add"
                    icon={<PlusOutlined />}
                    onClick={handleAddEvent}
                  >
                    Add Event
                  </Menu.Item>
                  <Menu.Item 
                    key="toggle"
                    icon={showEvents ? <EyeInvisibleOutlined /> : <EyeOutlined />}
                    onClick={() => setShowEvents(!showEvents)}
                  >
                    {showEvents ? 'Hide Events' : 'Show Events'}
                  </Menu.Item>
                  <Menu.Divider />
                  <Menu.Item key="stats" disabled>
                    Total Events: {events.length}
                  </Menu.Item>
                </Menu>
              }
              trigger={['click']}
              placement="bottomRight"
            >
              <Button
                type="text"
                size="small"
                icon={<SettingOutlined />}
                title="Event Management"
                style={{ 
                  position: 'relative',
                }}
              >
                {events.length > 0 && (
                  <span style={{
                    position: 'absolute',
                    top: -2,
                    right: -2,
                    background: '#ff4d4f',
                    color: 'white',
                    borderRadius: '50%',
                    width: 16,
                    height: 16,
                    fontSize: '10px',
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    lineHeight: 1
                  }}>
                    {events.length > 99 ? '99+' : events.length}
                  </span>
                )}
              </Button>
            </Dropdown>
            
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
          <ComposedChart 
            data={data} 
            margin={{ top: 20, right: 30, left: 20, bottom: 5 }}
            onClick={handleChartClick}
          >
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
            
            {/* 事件标记 - 放在最后以确保在所有线条上方 */}
            {showEvents && events.map(event => {
              const eventData = data.find(d => d.date === event.date);
              if (!eventData) return null;
              
              const yValue = getEventYValue(event, data);
              if (yValue === null) return null;
              
              return (
                <ReferenceDot
                  key={`event-${event.id}`}
                  x={event.date}
                  y={yValue}
                  yAxisId="price"
                  r={0} // 设置为0，让自定义组件控制大小
                  fill="transparent"
                  stroke="transparent"
                  shape={(props: any) => (
                    <EventDot 
                      cx={props.cx} 
                      cy={props.cy} 
                      event={event} 
                      onClick={handleEditEvent} 
                    />
                  )}
                />
              );
            })}
          </ComposedChart>
        </ResponsiveContainer>
      </div>
      
      {/* 事件编辑模态框 */}
      <EventEditModal
        visible={eventModalVisible}
        onCancel={() => {
          setEventModalVisible(false);
          setEditingEvent(null);
          setClickedDate(null);
        }}
        onSave={handleSaveEvent}
        event={editingEvent}
        defaultDate={clickedDate || undefined}
        loading={eventsLoading}
      />
    </Card>
  );
};

export default StockChart;