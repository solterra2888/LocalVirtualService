import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  Typography,
  Table,
  Button,
  Space,
  DatePicker,
  Select,
  Input,
  Row,
  Col,
  message,
  Tag,
  Tooltip,
  ConfigProvider,
  theme as antdTheme
} from 'antd';
import {
  ReloadOutlined,
  CalendarOutlined
} from '@ant-design/icons';
import dayjs, { Dayjs } from 'dayjs';
import { marketsAPI, EconomicEvent, CountryInfo } from '../services/markets';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import CountryFlag from '../components/CountryFlag';

const { Title, Text } = Typography;
const { RangePicker } = DatePicker;
const { Option } = Select;

const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    neutralTagBg: 'rgba(128, 128, 128, 0.1)',
    neutralTagColor: '#666',
    neutralTagBorder: 'rgba(128, 128, 128, 0.3)',
  },
  dark: {
    pageBg: '#1a202c',
    cardBg: '#1a2234',
    cardBorder: '#364152',
    text: '#e0e6f1',
    textSecondary: 'rgba(255, 255, 255, 0.5)',
    neutralTagBg: 'rgba(148, 163, 184, 0.15)',
    neutralTagColor: '#94a3b8',
    neutralTagBorder: 'rgba(148, 163, 184, 0.35)',
  },
} as const;

const EconomicEventsPage: React.FC = () => {
  const { isAuthenticated } = useAuthStore();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const [loading, setLoading] = useState(false);
  const [events, setEvents] = useState<EconomicEvent[]>([]);
  const [countries, setCountries] = useState<CountryInfo[]>([]);
  const [dateRange, setDateRange] = useState<[Dayjs, Dayjs]>([
    dayjs().subtract(7, 'days'),
    dayjs().add(30, 'days')
  ]);
  const [selectedCountries, setSelectedCountries] = useState<string[]>(['US', 'CN', 'HK']);
  const [searchText, setSearchText] = useState<string>('');
  const [selectedImpact, setSelectedImpact] = useState<number | undefined>(undefined);

  // 获取经济事件数据
  const fetchEvents = useCallback(async () => {
    setLoading(true);
    try {
      const params = {
        from_date: dateRange[0].format('YYYY-MM-DD'),
        to_date: dateRange[1].format('YYYY-MM-DD'),
        country: selectedCountries.length > 0 ? selectedCountries.join(',') : undefined,
        limit: 500
      };
      
      const data = await marketsAPI.getEconomicEvents(params);
      setEvents(data);
    } catch (error) {
      console.error('Failed to get economic event data:', error);
      message.error('Failed to retrieve economic event data');
    } finally {
      setLoading(false);
    }
  }, [dateRange, selectedCountries]);

  // 获取经济事件国家列表
  const fetchCountries = useCallback(async () => {
    try {
      const countryList = await marketsAPI.getEconomicEventsCountries();
      setCountries(countryList);
    } catch (error) {
      console.error('Failed to get economic event countries list:', error);
      message.error('Failed to retrieve country list');
    }
  }, []);



  // 初始加载
  useEffect(() => {
    if (isAuthenticated) {
      fetchEvents();
      fetchCountries();
    }
  }, [fetchEvents, fetchCountries, isAuthenticated]);

  // 筛选事件数据
  const filteredEvents = events.filter(event => {
    // 国家筛选 - 确保只显示选中国家的数据（或关系）
    if (selectedCountries.length > 0 && !selectedCountries.includes(event.country || '')) {
      return false;
    }
    
    // 搜索文本筛选
    if (searchText) {
      const searchLower = searchText.toLowerCase();
      const matchesSearch = (
        event.event?.toLowerCase().includes(searchLower) ||
        event.country?.toLowerCase().includes(searchLower)
      );
      if (!matchesSearch) return false;
    }
    
    // 重要性筛选
    if (selectedImpact !== undefined && event.impact !== selectedImpact) {
      return false;
    }
    
    return true;
  });

  // 表格列定义
  const columns = [
    {
      title: 'Date',
      dataIndex: 'date',
      key: 'date',
      width: 90,
      fixed: 'left' as const,
      render: (date: string) => date ? dayjs(date).format('MM-DD') : '-'
    },
    {
      title: 'Time',
      dataIndex: 'time',
      key: 'time',
      width: 70,
      fixed: 'left' as const,
    },
    {
      title: 'Country / Region',
      dataIndex: 'country',
      key: 'country',
      width: 110,
      fixed: 'left' as const,
      render: (country: string) => country ? <CountryFlag countryCode={country} /> : '-'
    },
    {
      title: 'Impact',
      dataIndex: 'impact',
      key: 'impact',
      width: 120,
      fixed: 'left' as const,
      render: (impact: number) => {
        if (!impact || impact < 1 || impact > 3) return '-';
        
        const colors = ['', '#52c41a', '#faad14', '#f5222d']; // 1星绿色，2星橙色，3星红色
        const impactColor = colors[impact];
        
        return (
          <div style={{ 
            fontWeight: 'bold',
            fontSize: '14px',
            minWidth: '70px',
            textAlign: 'center',
            letterSpacing: '1px',
            color: impactColor
          }}>
            {'★'.repeat(impact)}
          </div>
        );
      }
    },
    {
      title: 'Status',
      key: 'status',
      width: 100,
      fixed: 'left' as const,
      render: (_: any, record: EconomicEvent) => {
        const eventDate = new Date(record.date || '');
        const now = new Date();
        const isFuture = eventDate > now;
        const hasData = (record.actual !== null && record.actual !== undefined && record.actual !== '') || 
                       (record.previous !== null && record.previous !== undefined && record.previous !== '') || 
                       (record.forecast !== null && record.forecast !== undefined && record.forecast !== '');
        
        if (isFuture) {
          return <Tag color="orange">Upcoming</Tag>;
        } else if (hasData) {
          return <Tag color="green">Published</Tag>;
        } else {
          return <Tag color="red">Missing Data</Tag>;
        }
      }
    },
    {
      title: 'Event',
      dataIndex: 'event',
      key: 'event',
      width: 400,
      ellipsis: { showTitle: false },
      render: (event: string) => (
        <Tooltip title={event} placement="topLeft">
          {event || '-'}
        </Tooltip>
      )
    },
    {
      title: 'Actual',
      dataIndex: 'actual',
      key: 'actual',
      width: 90,
      render: (actual: string | number, record: EconomicEvent) => {
        if (actual !== null && actual !== undefined && actual !== '') return String(actual);
        // 检查是否是未来事件
        const eventDate = new Date(record.date || '');
        const now = new Date();
        if (eventDate > now) {
          return <Tag color="orange">Pending</Tag>;
        }
        return <Tag color="red">Not Published</Tag>;
      }
    },
    {
      title: 'Forecast',
      dataIndex: 'forecast',
      key: 'forecast',
      width: 90,
      render: (forecast: string | number) => {
        if (forecast !== null && forecast !== undefined && forecast !== '') return String(forecast);
        return (
          <Tag 
            style={{ 
              backgroundColor: colors.neutralTagBg, 
              color: colors.neutralTagColor,
              border: `1px solid ${colors.neutralTagBorder}`
            }}
          >
            No Forecast
          </Tag>
        );
      }
    },
    {
      title: 'Previous',
      dataIndex: 'previous',
      key: 'previous',
      width: 90,
      render: (previous: string | number) => {
        if (previous !== null && previous !== undefined && previous !== '') return String(previous);
        return (
          <Tag 
            style={{ 
              backgroundColor: colors.neutralTagBg, 
              color: colors.neutralTagColor,
              border: `1px solid ${colors.neutralTagBorder}`
            }}
          >
            No Previous
          </Tag>
        );
      }
    },
    // {
    //   title: 'Change %',
    //   dataIndex: 'calculated_change_pct',
    //   key: 'calculated_change_pct',
    //   width: 90,
    //   render: (changePct: number) => {
    //     if (changePct === null || changePct === undefined) return '-';
    //     const color = changePct > 0 ? 'green' : changePct < 0 ? 'red' : 'default';
    //     return <Text type={color as any}>{changePct.toFixed(2)}%</Text>;
    //   }
    // }
  ];

  // 如果未认证，显示登录提示
  if (!isAuthenticated) {
    return (
      <ConfigProvider
        theme={{
          algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
          ...(isDark && {
            token: {
              colorBgContainer: colors.cardBg,
              colorBorderSecondary: colors.cardBorder,
            },
          }),
        }}
      >
        <div style={{ padding: '24px', textAlign: 'center', backgroundColor: colors.pageBg, minHeight: '100%' }}>
          <Card>
            <Title level={3}>Login Required</Title>
            <Text type="secondary">
              Please login to access economic events data
            </Text>
          </Card>
        </div>
      </ConfigProvider>
    );
  }

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: colors.cardBg,
            colorBorderSecondary: colors.cardBorder,
          },
        }),
      }}
    >
      <div style={{ padding: '24px', backgroundColor: colors.pageBg, minHeight: '100%' }}>
        <div style={{ marginBottom: '24px' }}>
          <Title level={2} style={{ color: colors.text }}>
            <CalendarOutlined /> Economic Events
          </Title>
          <Text style={{ color: colors.textSecondary }}>
            Real-time economic events, 没有 https://tradingeconomics.com/calendar 全面，未来会改进...
          </Text>
        </div>

        {/* 控制面板 */}
        <Card style={{ marginBottom: 16 }}>
          <Row gutter={[16, 16]} align="middle">
          <Col xs={24} sm={12} md={8} lg={6}>
            <Space direction="vertical" size="small" style={{ width: '100%' }}>
              <Text strong>Date Range:</Text>
              <RangePicker
                value={dateRange}
                onChange={(dates) => dates && setDateRange(dates as [Dayjs, Dayjs])}
                format="YYYY-MM-DD"
                style={{ width: '100%' }}
              />
            </Space>
          </Col>
          <Col xs={24} sm={12} md={8} lg={6}>
            <Space direction="vertical" size="small" style={{ width: '100%' }}>
              <Text strong>Countries:</Text>
              <Select
                mode="multiple"
                style={{ width: '100%' }}
                placeholder="Select Countries"
                allowClear
                value={selectedCountries}
                onChange={setSelectedCountries}
                showSearch
                filterOption={(input, option) => {
                  const country = countries.find(c => c.code === option?.value);
                  if (!country) return false;
                  return country.name.toLowerCase().includes(input.toLowerCase()) ||
                         country.code.toLowerCase().includes(input.toLowerCase());
                }}
                loading={countries.length === 0}
                maxTagCount="responsive"
                tagRender={(props) => {
                  const { value, closable, onClose } = props;
                  return (
                    <Tag
                      closable={closable}
                      onClose={onClose}
                      style={{ marginRight: 3 }}
                    >
                      <CountryFlag countryCode={value} showName={false} />
                    </Tag>
                  );
                }}
              >
                {countries.map(country => (
                  <Option key={country.code} value={country.code}>
                    <CountryFlag countryCode={country.code} showName={true} />
                  </Option>
                ))}
              </Select>
            </Space>
          </Col>
          <Col xs={24} sm={12} md={8} lg={6}>
            <Space direction="vertical" size="small" style={{ width: '100%' }}>
              <Text strong>Impact:</Text>
              <Select
                style={{ width: '100%' }}
                placeholder="All Impact"
                allowClear
                value={selectedImpact}
                onChange={setSelectedImpact}
              >
                <Option value={3}>
                  <Tag color="red">★★★ High</Tag>
                </Option>
                <Option value={2}>
                  <Tag color="orange">★★ Medium</Tag>
                </Option>
                <Option value={1}>
                  <Tag color="green">★ Low</Tag>
                </Option>
              </Select>
            </Space>
          </Col>
          <Col xs={24} sm={12} md={8} lg={6}>
            <Space direction="vertical" size="small" style={{ width: '100%' }}>
              <Text strong>Search:</Text>
              <Input.Search
                placeholder="Search events or countries"
                value={searchText}
                onChange={(e) => setSearchText(e.target.value)}
                style={{ width: '100%' }}
              />
            </Space>
          </Col>
          <Col xs={24} sm={24} md={24} lg={24}>
            <Space>
              <Button
                type="primary"
                icon={<ReloadOutlined />}
                onClick={fetchEvents}
                loading={loading}
              >
                Refresh
              </Button>
            </Space>
          </Col>
          </Row>
        </Card>

        {/* 数据表格 */}
        <Card title={`Economic Events Data (${filteredEvents.length} records${searchText || selectedImpact !== undefined ? ' - filtered' : ' - showing latest 500'})`}>
          <Table
            columns={columns}
            dataSource={filteredEvents.map((item, index) => ({ ...item, key: index }))}
            loading={loading}
            size="small"
            scroll={{ x: 1000 }}
            pagination={{
              total: filteredEvents.length,
              pageSize: 50,
              showSizeChanger: true,
              showQuickJumper: true,
              showTotal: (total, range) => `Showing ${range[0]}-${range[1]} of ${total} records`
            }}
          />
        </Card>
      </div>
    </ConfigProvider>
  );
};

export default EconomicEventsPage;
