import React, { useState, useEffect } from 'react';
import {
  Card,
  Form,
  Input,
  DatePicker,
  Select,
  Button,
  Space,
  Typography,
  Alert,
  Row,
  Col,
  message,
  Spin,
  Table,
  Tag,
  Statistic,
  Radio
} from 'antd';
import {
  DownloadOutlined,
  SearchOutlined,
  InfoCircleOutlined,
  BarChartOutlined,
  DatabaseOutlined
} from '@ant-design/icons';
import { RangePickerProps } from 'antd/es/date-picker';
import dayjs, { Dayjs } from 'dayjs';
import { eodApiService, EODData, EODFetchParams } from '../services/eodApi';

const { Title, Text, Paragraph } = Typography;
const { Option } = Select;
const { RangePicker } = DatePicker;

interface EODFetchProps {
  onDataFetched?: (data: EODData[]) => void;
}

const EODFetch: React.FC<EODFetchProps> = ({ onDataFetched }) => {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [eodData, setEodData] = useState<EODData[]>([]);
  const [selectedPeriod, setSelectedPeriod] = useState<string>('d');
  const [fetchInfo, setFetchInfo] = useState<{
    ticker: string;
    period: string;
    dateRange: string;
    recordCount: number;
    totalRecords: number;
    storedRecords: number;
  } | null>(null);

  // Period options from service
  const periodOptions = eodApiService.getPeriodOptions();

  // Initialize selectedPeriod to match form's initial value
  useEffect(() => {
    setSelectedPeriod('d');
  }, []);

  // Disable future dates
  const disabledDate: RangePickerProps['disabledDate'] = (current) => {
    return current && current > dayjs().endOf('day');
  };

  // Validate ticker format using service
  const validateTicker = (_: any, value: string) => {
    // Skip validation if value is empty (handled by required rule)
    if (!value) {
      return Promise.resolve();
    }
    
    if (!eodApiService.validateTicker(value)) {
      return Promise.reject(new Error('Invalid ticker format. Use format like AAPL.US or 1810.HK'));
    }
    
    return Promise.resolve();
  };

  // Handle form submission
  const handleSubmit = async (values: {
    ticker: string;
    dateRange?: [Dayjs, Dayjs];
    period: string;
  }) => {
    setLoading(true);
    try {
      // Prepare API parameters
      const params: EODFetchParams = {
        ticker: values.ticker.toUpperCase(),
        period: values.period as 'd' | 'w' | 'm'
      };

      // Add date range if provided
      if (values.dateRange && values.dateRange.length === 2) {
        params.fromDate = values.dateRange[0].format('YYYY-MM-DD');
        params.toDate = values.dateRange[1].format('YYYY-MM-DD');
      }

      // Fetch data from API
      const response = await eodApiService.fetchEODData(params);

      if (!response.success) {
        throw new Error(response.error || 'Failed to fetch data');
      }

      setEodData(response.data);
      setFetchInfo({
        ticker: values.ticker,
        period: values.period, // 保存实际的period值（d/w/m），而不是label
        dateRange: values.dateRange 
          ? `${values.dateRange[0].format('YYYY-MM-DD')} to ${values.dateRange[1].format('YYYY-MM-DD')}`
          : 'All available data',
        recordCount: response.data.length,
        totalRecords: 0, // Will be updated by actual API response
        storedRecords: 0  // Will be updated by actual API response
      });

      if (response.warning) {
        message.warning(response.warning);
      } else {
        message.success(`Successfully fetched ${response.data.length} records for ${values.ticker}`);
      }
      
      if (onDataFetched) {
        onDataFetched(response.data);
      }
    } catch (error) {
      message.error('Failed to fetch EOD data. Please try again.');
      console.error('EOD fetch error:', error);
    } finally {
      setLoading(false);
    }
  };

  // Handle Excel export
  const handleExport = async () => {
    if (!fetchInfo) {
      message.error('No data to export. Please fetch data first.');
      return;
    }

    setExporting(true);
    try {
      const formValues = form.getFieldsValue();
      
      // 确保使用正确的查询参数
      const params: EODFetchParams = {
        ticker: fetchInfo.ticker,
        period: fetchInfo.period || formValues.period || 'd', // 优先使用fetchInfo中的period
        fromDate: formValues.dateRange?.[0]?.format('YYYY-MM-DD'),
        toDate: formValues.dateRange?.[1]?.format('YYYY-MM-DD')
      };

      console.log('导出参数:', params); // 调试日志
      console.log('fetchInfo:', fetchInfo); // 调试fetchInfo
      console.log('formValues:', formValues); // 调试表单值

      const blob = await eodApiService.exportEODData(params);
      
      // Create download link
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      
      // Generate filename
      const dateStr = formValues.dateRange 
        ? `_${params.fromDate}_${params.toDate}`
        : '';
      a.download = `${fetchInfo.ticker}_${params.period}_EOD_data${dateStr}.xlsx`;
      
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      
      message.success('Data exported successfully!');
    } catch (error) {
      message.error('Failed to export data. Please try again.');
      console.error('Export error:', error);
    } finally {
      setExporting(false);
    }
  };

  // Table columns for EOD data
  const columns = [
    {
      title: 'Date',
      dataIndex: 'date',
      key: 'date',
      width: 120,
      sorter: (a: EODData, b: EODData) => new Date(a.date).getTime() - new Date(b.date).getTime(),
    },
    {
      title: 'Open',
      dataIndex: 'open',
      key: 'open',
      width: 100,
      render: (value: number) => `$${value.toFixed(2)}`,
      sorter: (a: EODData, b: EODData) => a.open - b.open,
    },
    {
      title: 'High',
      dataIndex: 'high',
      key: 'high',
      width: 100,
      render: (value: number) => `$${value.toFixed(2)}`,
      sorter: (a: EODData, b: EODData) => a.high - b.high,
    },
    {
      title: 'Low',
      dataIndex: 'low',
      key: 'low',
      width: 100,
      render: (value: number) => `$${value.toFixed(2)}`,
      sorter: (a: EODData, b: EODData) => a.low - b.low,
    },
    {
      title: 'Close',
      dataIndex: 'close',
      key: 'close',
      width: 100,
      render: (value: number) => `$${value.toFixed(2)}`,
      sorter: (a: EODData, b: EODData) => a.close - b.close,
    },
    {
      title: 'Adjusted Close',
      dataIndex: 'adjusted_close',
      key: 'adjusted_close',
      width: 130,
      render: (value: number) => `$${value.toFixed(2)}`,
      sorter: (a: EODData, b: EODData) => a.adjusted_close - b.adjusted_close,
    },
    {
      title: 'Volume',
      dataIndex: 'volume',
      key: 'volume',
      width: 120,
      render: (value: number) => value.toLocaleString(),
      sorter: (a: EODData, b: EODData) => a.volume - b.volume,
    },
    {
      title: 'Warning',
      dataIndex: 'warning',
      key: 'warning',
      width: 200,
      render: (warning: string) => warning ? (
        <Tag color="orange" icon={<InfoCircleOutlined />}>
          {warning}
        </Tag>
      ) : null,
    },
  ];

  // Calculate statistics
  const calculateStats = () => {
    if (eodData.length === 0) return null;

    const prices = eodData.map(d => d.close);
    const volumes = eodData.map(d => d.volume);
    
    return {
      highestPrice: Math.max(...prices),
      lowestPrice: Math.min(...prices),
      avgVolume: volumes.reduce((a, b) => a + b, 0) / volumes.length,
      totalVolume: volumes.reduce((a, b) => a + b, 0),
      priceChange: prices[0] ? ((prices[prices.length - 1] - prices[0]) / prices[0] * 100) : 0
    };
  };

  const stats = calculateStats();

  return (
    <div style={{ padding: '24px' }}>
      <Card>
        <Title level={2} style={{ marginBottom: 24 }}>
          <DatabaseOutlined style={{ marginRight: 8 }} />
          EOD Data Fetch
        </Title>
        
        <Paragraph type="secondary" style={{ marginBottom: 24 }}>
          Fetch End-of-Day stock price data from EODHD API. Enter a ticker symbol with exchange code 
          (e.g., AAPL.US, 1810.HK) and select your preferred time period and date range.
        </Paragraph>

        <Form
          form={form}
          layout="vertical"
          onFinish={handleSubmit}
          initialValues={{
            period: 'd'
          }}
        >
          <Row gutter={[16, 16]}>
            <Col xs={24} sm={24} md={8}>
              <Form.Item
                name="ticker"
                label="Ticker Symbol"
                rules={[
                  { required: true, message: 'Please enter a ticker symbol' },
                  { validator: validateTicker }
                ]}
                tooltip="Enter ticker with exchange code (e.g., AAPL.US, 1810.HK)"
              >
                <Input
                  placeholder="e.g., AAPL.US"
                  prefix={<SearchOutlined />}
                  size="large"
                  style={{ textTransform: 'uppercase' }}
                />
              </Form.Item>
            </Col>
            
            <Col xs={24} sm={24} md={8}>
              <Form.Item
                name="period"
                label="Period"
                rules={[{ required: true, message: 'Please select a period' }]}
                tooltip="Select data aggregation period"
              >
                <Radio.Group 
                  size="large"
                  style={{ width: '100%', display: 'flex' }}
                  optionType="button"
                  buttonStyle="solid"
                  value={selectedPeriod}
                  onChange={(e) => {
                    setSelectedPeriod(e.target.value);
                    form.setFieldsValue({ period: e.target.value });
                  }}
                >
                  {periodOptions.map(option => (
                    <Radio.Button 
                      key={option.value} 
                      value={option.value}
                      style={{ flex: 1, textAlign: 'center' }}
                    >
                      {option.label}
                    </Radio.Button>
                  ))}
                </Radio.Group>
                <div style={{ marginTop: 8, fontSize: '12px', color: '#666', textAlign: 'center', minHeight: '16px' }}>
                  <Text type="secondary">
                    {periodOptions.find(p => p.value === selectedPeriod)?.description || 'Select a period to see description'}
                  </Text>
                </div>
              </Form.Item>
            </Col>
            
            <Col xs={24} sm={24} md={8}>
              <Form.Item
                name="dateRange"
                label="Date Range (Optional)"
                tooltip="Leave empty to fetch all available data"
              >
                <RangePicker
                  style={{ width: '100%' }}
                  size="large"
                  disabledDate={disabledDate}
                  placeholder={['Start Date', 'End Date']}
                />
              </Form.Item>
            </Col>
          </Row>

          <Form.Item>
            <Space>
              <Button
                type="primary"
                htmlType="submit"
                icon={<DownloadOutlined />}
                loading={loading}
                size="large"
              >
                Fetch EOD Data
              </Button>
              <Button
                onClick={handleExport}
                icon={<DownloadOutlined />}
                loading={exporting}
                disabled={!fetchInfo}
                size="large"
              >
                Export Excel
              </Button>
              <Button
                onClick={() => {
                  form.resetFields();
                  setEodData([]);
                  setFetchInfo(null);
                }}
                size="large"
              >
                Reset
              </Button>
            </Space>
          </Form.Item>
        </Form>

        {loading && (
          <div style={{ textAlign: 'center', padding: '40px' }}>
            <Spin size="large" />
            <div style={{ marginTop: 16 }}>
              <Text>Fetching EOD data from EODHD API...</Text>
            </div>
          </div>
        )}

        {fetchInfo && (
          <Alert
            message={`Successfully fetched ${fetchInfo.recordCount} records for ${fetchInfo.ticker}`}
            description={`Period: ${periodOptions.find(p => p.value === fetchInfo.period)?.label || fetchInfo.period} | Date Range: ${fetchInfo.dateRange}`}
            type="success"
            showIcon
            style={{ marginTop: 16 }}
          />
        )}

        {stats && (
          <Card size="small" style={{ marginTop: 16 }}>
            <Title level={4}>Data Statistics</Title>
            <Row gutter={16}>
              <Col span={6}>
                <Statistic
                  title="Highest Price"
                  value={stats.highestPrice}
                  prefix="$"
                  precision={2}
                />
              </Col>
              <Col span={6}>
                <Statistic
                  title="Lowest Price"
                  value={stats.lowestPrice}
                  prefix="$"
                  precision={2}
                />
              </Col>
              <Col span={6}>
                <Statistic
                  title="Average Volume"
                  value={stats.avgVolume}
                  precision={0}
                />
              </Col>
              <Col span={6}>
                <Statistic
                  title="Price Change"
                  value={stats.priceChange}
                  suffix="%"
                  precision={2}
                  valueStyle={{ color: stats.priceChange >= 0 ? '#3f8600' : '#cf1322' }}
                />
              </Col>
            </Row>
          </Card>
        )}

        {eodData.length > 0 && (
          <Card size="small" style={{ marginTop: 16 }}>
            <Title level={4}>
              <BarChartOutlined style={{ marginRight: 8 }} />
              EOD Data Table
            </Title>
            <Table
              columns={columns}
              dataSource={eodData}
              rowKey="date"
              pagination={{
                pageSize: 20,
                showSizeChanger: true,
                showQuickJumper: true,
                showTotal: (total, range) => 
                  `${range[0]}-${range[1]} of ${total} records`
              }}
              scroll={{ x: 800 }}
              size="small"
            />
          </Card>
        )}
      </Card>
    </div>
  );
};

export default EODFetch;
