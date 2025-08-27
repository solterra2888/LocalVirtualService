import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  Typography,
  Table,
  Button,
  Space,
  Select,
  Row,
  Col,
  message,
  Tooltip,
  InputNumber
} from 'antd';
import {
  ReloadOutlined,
  LineChartOutlined
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { marketsAPI, MacroIndicator, CountryInfo } from '../services/markets';
import { useAuthStore } from '../store/authStore';
import CountryFlag from '../components/CountryFlag';

const { Title, Text } = Typography;
const { Option } = Select;

const MacroIndicatorsPage: React.FC = () => {
  const { isAuthenticated } = useAuthStore();
  const [loading, setLoading] = useState(false);
  const [indicators, setIndicators] = useState<MacroIndicator[]>([]);
  const [countries, setCountries] = useState<CountryInfo[]>([]);
  const [selectedCountry, setSelectedCountry] = useState<string>('USA');
  const [selectedIndicator, setSelectedIndicator] = useState<string>('');
  const [yearRange, setYearRange] = useState<[number, number]>([2020, 2024]);

  // 获取宏观指标数据
  const fetchIndicators = useCallback(async () => {
    if (!selectedCountry) return;
    
    setLoading(true);
    try {
      const params = {
        country: selectedCountry,
        indicator: selectedIndicator || undefined,
        year_from: yearRange[0],
        year_to: yearRange[1],
        limit: 1000
      };
      
      const data = await marketsAPI.getMacroIndicators(params);
      setIndicators(data);
    } catch (error) {
      console.error('获取宏观指标失败:', error);
      message.error('Failed to retrieve macro indicator data');
    } finally {
      setLoading(false);
    }
  }, [selectedCountry, selectedIndicator, yearRange]);

  // 获取国家列表
  const fetchCountries = useCallback(async () => {
    try {
      const countryList = await marketsAPI.getAvailableCountries();
      setCountries(countryList);
    } catch (error) {
      console.error('获取国家列表失败:', error);
      message.error('Failed to retrieve country list');
    }
  }, []);

  // 初始加载
  useEffect(() => {
    if (isAuthenticated) {
      fetchIndicators();
      fetchCountries();
    }
  }, [fetchIndicators, fetchCountries, isAuthenticated]);

  // 表格列定义
  const indicatorColumns = [
    {
      title: 'Year',
      dataIndex: 'year',
      key: 'year',
      width: 80,
      sorter: (a: MacroIndicator, b: MacroIndicator) => a.year - b.year,
    },
    {
      title: 'Indicator Name',
      dataIndex: 'indicator_name',
      key: 'indicator_name',
      ellipsis: { showTitle: false },
      render: (name: string) => (
        <Tooltip title={name} placement="topLeft">
          {name}
        </Tooltip>
      )
    },
    {
      title: 'Value',
      dataIndex: 'value',
      key: 'value',
      width: 150,
      render: (value: number) => {
        if (value === null || value === undefined) return '-';
        
        // 格式化大数值
        if (Math.abs(value) >= 1e12) {
          return `${(value / 1e12).toFixed(2)}T`;
        } else if (Math.abs(value) >= 1e9) {
          return `${(value / 1e9).toFixed(2)}B`;
        } else if (Math.abs(value) >= 1e6) {
          return `${(value / 1e6).toFixed(2)}M`;
        } else {
          return value.toLocaleString(undefined, { maximumFractionDigits: 2 });
        }
      }
    },
    {
      title: 'Country/Region',
      dataIndex: 'country_name',
      key: 'country_name',
      width: 120,
      render: (name: string, record: MacroIndicator) => (
        <CountryFlag countryCode={record.country_code} showName={true} />
      )
    },
    {
      title: 'Fetch Time',
      dataIndex: 'fetch_timestamp',
      key: 'fetch_timestamp',
      width: 150,
      render: (timestamp: string) => timestamp ? dayjs(timestamp).format('YYYY-MM-DD HH:mm') : '-'
    }
  ];

  // 如果未认证，显示登录提示
  if (!isAuthenticated) {
    return (
      <div style={{ padding: '24px', textAlign: 'center' }}>
        <Card>
          <Title level={3}>Login Required</Title>
          <Text type="secondary">
            Please login to access macro indicators data
          </Text>
        </Card>
      </div>
    );
  }

  return (
    <div style={{ padding: '24px' }}>
      <div style={{ marginBottom: '24px' }}>
        <Title level={2}>
          <LineChartOutlined /> Macro Indicators
        </Title>
        <Text type="secondary">
          Global macroeconomic indicators and economic data analysis
        </Text>
      </div>

      {/* 控制面板 */}
      <Card style={{ marginBottom: 16 }}>
        <Row gutter={[16, 16]} align="middle">
          <Col xs={24} sm={12} md={8} lg={6}>
            <Space direction="vertical" size="small" style={{ width: '100%' }}>
              <Text strong>Country:</Text>
              <Select
                style={{ width: '100%' }}
                placeholder="Select Country"
                value={selectedCountry}
                onChange={setSelectedCountry}
                showSearch
                filterOption={(input, option) => {
                  const country = countries.find(c => c.code === option?.value);
                  if (!country) return false;
                  return country.name.toLowerCase().includes(input.toLowerCase()) ||
                         country.code.toLowerCase().includes(input.toLowerCase());
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
              <Text strong>Indicator:</Text>
              <Select
                style={{ width: '100%' }}
                placeholder="Select Indicator (Optional)"
                allowClear
                value={selectedIndicator}
                onChange={setSelectedIndicator}
              >
                <Option value="gdp_current_usd">GDP (Current USD)</Option>
                <Option value="gdp_per_capita_usd">GDP Per Capita</Option>
                <Option value="inflation_consumer_prices_annual">Inflation Rate</Option>
                <Option value="unemployment_total_percent">Unemployment Rate</Option>
                <Option value="population_total">Total Population</Option>
              </Select>
            </Space>
          </Col>
          <Col xs={24} sm={12} md={8} lg={6}>
            <Space direction="vertical" size="small" style={{ width: '100%' }}>
              <Text strong>Year Range:</Text>
              <Space>
                <InputNumber
                  min={1960}
                  max={2030}
                  value={yearRange[0]}
                  onChange={(value) => value && setYearRange([value, yearRange[1]])}
                  style={{ width: 80 }}
                />
                <Text>-</Text>
                <InputNumber
                  min={1960}
                  max={2030}
                  value={yearRange[1]}
                  onChange={(value) => value && setYearRange([yearRange[0], value])}
                  style={{ width: 80 }}
                />
              </Space>
            </Space>
          </Col>
          <Col xs={24} sm={12} md={8} lg={6}>
            <Space direction="vertical" size="small" style={{ width: '100%' }}>
              <Text strong>Actions:</Text>
              <Button
                type="primary"
                icon={<ReloadOutlined />}
                onClick={fetchIndicators}
                loading={loading}
              >
                Refresh
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* 数据表格 */}
      <Card title={`Macro Indicators Data (${indicators.length} records)`}>
        <Table
          columns={indicatorColumns}
          dataSource={indicators.map((item, index) => ({ ...item, key: index }))}
          loading={loading}
          size="small"
          scroll={{ x: 800 }}
          pagination={{
            total: indicators.length,
            pageSize: 50,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => `Showing ${range[0]}-${range[1]} of ${total} records`
          }}
        />
      </Card>
    </div>
  );
};

export default MacroIndicatorsPage;
