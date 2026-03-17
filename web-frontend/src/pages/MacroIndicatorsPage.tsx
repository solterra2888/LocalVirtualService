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
  InputNumber,
  Modal,
  Checkbox,
  Divider,
  ConfigProvider,
  theme as antdTheme
} from 'antd';
import {
  ReloadOutlined,
  LineChartOutlined,
  ExportOutlined
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { marketsAPI, MacroIndicator, CountryInfo, IndicatorInfo } from '../services/markets';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { getModalStyles } from '../utils/modalStyles';
import CountryFlag from '../components/CountryFlag';

const { Title, Text } = Typography;
const { Option } = Select;

const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
  },
  dark: {
    pageBg: '#1a202c',
    cardBg: '#1a2234',
    cardBorder: '#364152',
    text: '#e0e6f1',
    textSecondary: 'rgba(255, 255, 255, 0.5)',
  },
} as const;

const MacroIndicatorsPage: React.FC = () => {
  const { isAuthenticated } = useAuthStore();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const [loading, setLoading] = useState(false);
  const [indicators, setIndicators] = useState<MacroIndicator[]>([]);
  const [countries, setCountries] = useState<CountryInfo[]>([]);
  const [availableIndicators, setAvailableIndicators] = useState<IndicatorInfo[]>([]);
  const [selectedCountry, setSelectedCountry] = useState<string>('USA');
  const [selectedIndicator, setSelectedIndicator] = useState<string>('');
  const [yearRange, setYearRange] = useState<[number, number]>([2020, 2024]);

  // 导出相关状态
  const [exportModalVisible, setExportModalVisible] = useState(false);
  const [exportLoading, setExportLoading] = useState(false);
  const [exportCountries, setExportCountries] = useState<string[]>([]);
  const [exportIndicators, setExportIndicators] = useState<string[]>([]);
  const [exportYearRange, setExportYearRange] = useState<[number, number]>([2020, 2024]);

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

  // 获取可用指标列表
  const fetchAvailableIndicators = useCallback(async () => {
    try {
      const result = await marketsAPI.getAvailableIndicators();
      setAvailableIndicators(result.indicators);
    } catch (error) {
      console.error('获取可用指标列表失败:', error);
      message.error('Failed to retrieve available indicators');
    }
  }, []);

  // 导出CSV
  const handleExportCSV = async () => {
    setExportLoading(true);
    try {
      const params: any = {};
      
      // 只有当选择了国家时才添加参数
      if (exportCountries.length > 0) {
        params.country = exportCountries;
      }
      
      // 只有当选择了指标时才添加参数
      if (exportIndicators.length > 0) {
        params.indicator = exportIndicators;
      }
      
      // 添加年份范围
      params.year_from = exportYearRange[0];
      params.year_to = exportYearRange[1];
      
      await marketsAPI.exportMacroIndicatorsCSV(params);
      message.success('CSV export completed successfully');
      setExportModalVisible(false);
    } catch (error) {
      console.error('导出CSV失败:', error);
      message.error('Failed to export CSV');
    } finally {
      setExportLoading(false);
    }
  };

  // 重置导出选项
  const resetExportOptions = () => {
    setExportCountries([]);
    setExportIndicators([]);
    setExportYearRange([2020, 2024]);
  };

  // 初始加载
  useEffect(() => {
    if (isAuthenticated) {
      fetchIndicators();
      fetchCountries();
      fetchAvailableIndicators();
    }
  }, [fetchIndicators, fetchCountries, fetchAvailableIndicators, isAuthenticated]);

  // 表格列定义
  const indicatorColumns = [
    {
      title: 'Year',
      dataIndex: 'year',
      key: 'year',
      width: 70,
      fixed: 'left' as const,
      sorter: (a: MacroIndicator, b: MacroIndicator) => a.year - b.year,
    },
    {
      title: 'Indicator Name',
      dataIndex: 'indicator_name',
      key: 'indicator_name',
      width: 300,
      ellipsis: { showTitle: false },
      render: (name: string) => (
        <Tooltip title={name} placement="topLeft">
          <Text style={{ fontSize: '12px' }}>{name}</Text>
        </Tooltip>
      )
    },
    {
      title: 'Value',
      dataIndex: 'value',
      key: 'value',
      width: 120,
      align: 'left' as const,
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
      width: 140,
      render: (name: string, record: MacroIndicator) => (
        <CountryFlag countryCode={record.country_code} showName={true} />
      )
    },
    {
      title: 'Fetch Time',
      dataIndex: 'fetch_timestamp',
      key: 'fetch_timestamp',
      width: 130,
      render: (timestamp: string) => timestamp ? dayjs(timestamp).format('MM-DD HH:mm') : '-'
    }
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
              Please login to access macro indicators data
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
            <LineChartOutlined /> Macro Indicators
          </Title>
          <Text style={{ color: colors.textSecondary }}>
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
                showSearch
                filterOption={(input, option) => {
                  const indicator = availableIndicators.find(i => i.indicator === option?.value);
                  if (!indicator) return false;
                  return indicator.indicator_name.toLowerCase().includes(input.toLowerCase()) ||
                         indicator.indicator.toLowerCase().includes(input.toLowerCase());
                }}
              >
                {availableIndicators.map(indicator => (
                  <Option key={indicator.indicator} value={indicator.indicator}>
                    {indicator.indicator_name}
                  </Option>
                ))}
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
              <Space>
                <Button
                  type="primary"
                  icon={<ReloadOutlined />}
                  onClick={fetchIndicators}
                  loading={loading}
                >
                  Refresh
                </Button>
                <Button
                  icon={<ExportOutlined />}
                  onClick={() => setExportModalVisible(true)}
                >
                  Export
                </Button>
              </Space>
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
            scroll={{ x: 760 }}
            pagination={{
              total: indicators.length,
              pageSize: 50,
              showSizeChanger: true,
              showQuickJumper: true,
              showTotal: (total, range) => `Showing ${range[0]}-${range[1]} of ${total} records`
            }}
          />
        </Card>

        {/* 导出CSV弹窗 */}
        <Modal
        title="Export Macro Indicators CSV"
        open={exportModalVisible}
        onOk={handleExportCSV}
        onCancel={() => setExportModalVisible(false)}
        confirmLoading={exportLoading}
        width={600}
        okText="Export"
        cancelText="Cancel"
        styles={getModalStyles(themeMode)}
      >
        <div style={{ marginBottom: 16 }}>
          <Text type="secondary">
            Select countries, indicators, and year range for export. Leave all fields empty to export all data.
          </Text>
        </div>

        <Divider orientation="left">Export Options</Divider>

        {/* 国家选择 */}
        <div style={{ marginBottom: 16 }}>
          <Text strong>Countries (Multi-select):</Text>
          <div style={{ marginTop: 8 }}>
            <Checkbox.Group
              options={countries.map(country => ({
                label: (
                  <CountryFlag countryCode={country.code} showName={true} />
                ),
                value: country.code
              }))}
              value={exportCountries}
              onChange={(checkedValues) => setExportCountries(checkedValues as string[])}
            />
          </div>
        </div>

        {/* 指标选择 */}
        <div style={{ marginBottom: 16 }}>
          <Text strong>Indicators (Multi-select):</Text>
          <div style={{ marginTop: 8 }}>
            <Checkbox.Group
              options={availableIndicators.map(indicator => ({
                label: indicator.indicator_name,
                value: indicator.indicator
              }))}
              value={exportIndicators}
              onChange={(checkedValues) => setExportIndicators(checkedValues as string[])}
            />
          </div>
        </div>

        {/* 年份范围 */}
        <div style={{ marginBottom: 16 }}>
          <Text strong>Year Range:</Text>
          <div style={{ marginTop: 8 }}>
            <Space>
              <InputNumber
                min={1960}
                max={2030}
                value={exportYearRange[0]}
                onChange={(value) => value && setExportYearRange([value, exportYearRange[1]])}
                style={{ width: 100 }}
                placeholder="From"
              />
              <Text>-</Text>
              <InputNumber
                min={1960}
                max={2030}
                value={exportYearRange[1]}
                onChange={(value) => value && setExportYearRange([exportYearRange[0], value])}
                style={{ width: 100 }}
                placeholder="To"
              />
            </Space>
          </div>
        </div>

        <Divider />

        <div style={{ textAlign: 'center' }}>
          <Button onClick={resetExportOptions} style={{ marginRight: 8 }}>
            Reset Options
          </Button>
          <Text type="secondary">
            {exportCountries.length === 0 && exportIndicators.length === 0 
              ? 'Will export all macro indicators data'
              : `Will export ${exportCountries.length || 'all'} countries, ${exportIndicators.length || 'all'} indicators`
            }
          </Text>
        </div>
      </Modal>
      </div>
    </ConfigProvider>
  );
};

export default MacroIndicatorsPage;
