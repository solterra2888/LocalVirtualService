import React, { useState, useEffect } from 'react';
import {
  Modal,
  Steps,
  Form,
  Input,
  Select,
  Button,
  Space,
  Card,
  Typography,
  Alert,
  Row,
  Col,
  InputNumber,
  Tooltip,
  message
} from 'antd';
import {
  ClockCircleOutlined,
  InfoCircleOutlined,
  CheckCircleOutlined,
  ScheduleOutlined,
  BulbOutlined,
  BarChartOutlined,
  QuestionCircleOutlined,
  DatabaseOutlined
} from '@ant-design/icons';
import { schedulerAPI, CreateEODHDNewsTaskParams, TimezoneOption } from '../services/scheduler';
import CronExpressionGuide from './CronExpressionGuide';

const { Title, Text } = Typography;
const { Option } = Select;

interface DataSourceConfigModalProps {
  open: boolean;
  onCancel: () => void;
  onSuccess: (taskId: string) => void;
  dataSourceType: 'eodhd_news' | 'eodhd_events' | 'eodhd_macro' | 'eodhd_exchange' | 'eodhd_fundamentals' | 'finviz_screener';
  dataSourceName: string;
}

interface ConfigFormData {
  // 调度配置
  cronExpression: string;
  timezone: string;
  
  // 任务参数
  taskParams?: Record<string, any>;
}

const DataSourceConfigModal: React.FC<DataSourceConfigModalProps> = ({
  open,
  onCancel,
  onSuccess,
  dataSourceType,
  dataSourceName
}) => {
  const [currentStep, setCurrentStep] = useState(0);
  const [form] = Form.useForm<ConfigFormData>();
  const [loading, setLoading] = useState(false);

  const [timezoneOptions, setTimezoneOptions] = useState<TimezoneOption[]>([]);
  const [formData, setFormData] = useState<Partial<ConfigFormData>>({});
  const [showCronGuide, setShowCronGuide] = useState(false);

  // 数据源配置映射
  const dataSourceConfigs = {
    eodhd_news: {
      icon: <InfoCircleOutlined style={{ color: '#1890ff' }} />,
      params: ['max_results', 'symbols', 'topic', 'keywords'],
      defaultSchedule: '*/15 * * * *',
      defaultName: 'EODHD News Auto Fetch',
      description: 'Get latest stock news and market information from EODHD API'
    },
    eodhd_events: {
      icon: <ScheduleOutlined style={{ color: '#52c41a' }} />,
      params: ['limit'],
      defaultSchedule: '0 9 * * 1-5',
      defaultName: 'Economic Events Auto Fetch',
      description: 'Get economic calendar event data including GDP, CPI and other important indicators'
    },
    eodhd_macro: {
      icon: <BarChartOutlined style={{ color: '#722ed1' }} />,
      params: ['country', 'indicator'],
      defaultSchedule: '0 10 1 * *',
      defaultName: 'Macro Indicators Auto Fetch',
      description: 'Get macroeconomic indicator data such as inflation rate, unemployment rate, etc.'
    },
    eodhd_exchange: {
      icon: <DatabaseOutlined style={{ color: '#722ed1' }} />,
      params: ['exchanges'],
      defaultSchedule: '0 2 * * 0',
      defaultName: 'EODHD Exchange Data Auto Fetch',
      description: 'Get exchange ticker symbols and company data from EODHD API for fundamentals analysis'
    },
    eodhd_fundamentals: {
      icon: <BarChartOutlined style={{ color: '#13c2c2' }} />,
      params: ['exchanges', 'data_types'],
      defaultSchedule: '0 6 * * 0',
      defaultName: 'EODHD Fundamentals Auto Fetch',
      description: 'Get comprehensive company fundamentals data from EODHD API for multiple markets'
    },
    finviz_screener: {
      icon: <DatabaseOutlined style={{ color: '#fa8c16' }} />,
      params: [],
      defaultSchedule: '0 8 * * 1-5',
      defaultName: 'Finviz Screener Data Auto Fetch',
      description: 'Get comprehensive stock screener data from Finviz with technical and fundamental metrics (no parameters required)'
    }
  };

  const currentConfig = dataSourceConfigs[dataSourceType];

  const loadPresets = async () => {
    try {
      console.log('Loading timezone configuration...');
      const timezonesResponse = await schedulerAPI.getTimezonePresets();
      console.log('Timezone API response:', timezonesResponse);
      
      if (timezonesResponse.success) {
        console.log('Setting timezone options:', timezonesResponse.data);
        setTimezoneOptions(timezonesResponse.data || []);
      } else {
        console.error('Timezone API returned failure:', timezonesResponse.message);
      }
    } catch (error) {
      console.error('Failed to load timezone configuration:', error);
    }
  };

  const resetModal = () => {
    // EODHD Fundamentals不需要schedule configuration，直接跳到参数配置
    const startStep = dataSourceType === 'eodhd_fundamentals' ? 1 : 0;
    setCurrentStep(startStep);
    setFormData({});
    form.resetFields();
    
    // 设置默认值（不显示cron表达式）
    form.setFieldsValue({
      timezone: 'America/New_York'  // 默认选择美国纽约时区
    });
  };

  useEffect(() => {
    if (open) {
      loadPresets();
      resetModal();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const handleNext = async () => {
    try {
      const values = await form.validateFields();
      const newFormData = { ...formData, ...values };
      setFormData(newFormData);
      
      // EODHD Fundamentals跳过schedule configuration，直接提交
      if (dataSourceType === 'eodhd_fundamentals') {
        await handleSubmit(newFormData);
      } else if (currentStep < 1) {
        setCurrentStep(currentStep + 1);
      } else {
        await handleSubmit(newFormData);
      }
    } catch (error) {
      console.error('表单验证失败:', error);
    }
  };

  const handlePrev = () => {
    setCurrentStep(currentStep - 1);
  };

  const handleSubmit = async (data: Partial<ConfigFormData>) => {
    try {
      setLoading(true);
      
      let result;
      if (dataSourceType === 'eodhd_fundamentals') {
        // EODHD基本面数据手动更新逻辑（不需要cron表达式）
        const fundamentalsParams = {
          exchanges: data.taskParams?.exchanges || ['NYSE', 'NASDAQ'],
          data_types: data.taskParams?.data_types || ['General', 'Financials', 'Highlights', 'Valuation', 'Technicals', 'Holders', 'outstandingShares', 'Earnings', 'SharesStats', 'AnalystRatings', 'SplitsDividends', 'InsiderTransactions']
        };
        result = await schedulerAPI.executeFundamentalsUpdate(fundamentalsParams);
      } else {
        const params: CreateEODHDNewsTaskParams = {
          name: currentConfig.defaultName,
          cron_expression: data.cronExpression!,
          timezone: data.timezone!,
          enabled: true, // 新创建的任务默认启用，用户可通过主页面switch控制
          ...data.taskParams
        };

        if (dataSourceType === 'eodhd_news') {
          result = await schedulerAPI.createEODHDNewsTask(params);
        } else if (dataSourceType === 'finviz_screener') {
          // Finviz筛选数据任务的专用创建逻辑
          const finvizParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true
          };
          result = await schedulerAPI.createFinvizScreenerTask(finvizParams);
        } else {
          // 其他任务类型的创建逻辑
          const taskType = dataSourceType === 'eodhd_events' ? 'economic_events' : 
                          dataSourceType === 'eodhd_macro' ? 'macro_indicators' : 
                          dataSourceType === 'eodhd_exchange' ? 'exchange_data' : 
                          dataSourceType;
          
          result = await schedulerAPI.createCronTask({
            name: currentConfig.defaultName,
            task_type: taskType,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true, // 新创建的任务默认启用，用户可通过主页面switch控制
            params: data.taskParams
          });
        }
      }

      if (result.success) {
        const successMessage = dataSourceType === 'eodhd_fundamentals' 
          ? 'Data update initiated successfully!' 
          : 'Data acquisition strategy configuration successful!';
        message.success(successMessage);
        onSuccess(result.data?.task_id || '');
        handleCancel();
      } else {
        message.error(result.message || (dataSourceType === 'eodhd_fundamentals' ? 'Data update failed' : 'Configuration failed'));
      }
    } catch (error) {
      console.error('Configuration failed:', error);
      message.error('Configuration failed');
    } finally {
      setLoading(false);
    }
  };

  const handleCancel = () => {
    resetModal();
    onCancel();
  };

  const renderStepContent = () => {
    switch (currentStep) {
      case 0:
        return renderScheduleConfigStep();
      case 1:
        return renderTaskParamsStep();
      default:
        return null;
    }
  };

  const renderScheduleConfigStep = () => (
    <div>
      <div style={{ textAlign: 'center', marginBottom: 24 }}>
        <Title level={4}>
          <ClockCircleOutlined /> Schedule Configuration
        </Title>
        <Text type="secondary">
          Set the execution time and frequency for data acquisition for {dataSourceName}
        </Text>
      </div>

      {/* Data source information card */}
      <Card 
        style={{ marginBottom: 24, backgroundColor: '#f9f9f9' }}
        size="small"
      >
        <Row align="middle">
          <Col span={2}>
            {currentConfig.icon}
          </Col>
          <Col span={22}>
            <div>
              <Text strong>{dataSourceName}</Text>
              <div style={{ marginTop: 4 }}>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  {currentConfig.description}
                </Text>
              </div>
            </div>
          </Col>
        </Row>
      </Card>

      <Form.Item
        label={
          <Space>
            Cron Expression
            <Button
              type="text"
              size="small"
              icon={<QuestionCircleOutlined />}
              onClick={() => setShowCronGuide(true)}
              style={{ padding: 0, height: 'auto' }}
            >
              Usage Guide
            </Button>
          </Space>
        }
        name="cronExpression"
        rules={[
          { required: true, message: 'Please enter a Cron expression' },
          {
            validator: (_, value) => {
              if (!value) return Promise.resolve();
              const { valid, message: msg } = schedulerAPI.validateCronExpression(value);
              return valid ? Promise.resolve() : Promise.reject(new Error(msg));
            }
          }
        ]}
        extra="Format: Minutes Hours Day Month Week (e.g., */15 * * * * means execute every 15 minutes)"
      >
        <Input 
          placeholder="Enter Cron expression, e.g., */15 * * * *"
          suffix={
            <Tooltip title="Format: Minutes Hours Day Month Week">
              <InfoCircleOutlined />
            </Tooltip>
          }
        />
      </Form.Item>

      <Form.Item
        label="Timezone"
        name="timezone"
        rules={[{ required: true, message: 'Please select a timezone' }]}
      >
        <Select placeholder="Select timezone" showSearch>
          {timezoneOptions.map(tz => (
            <Option key={tz.value} value={tz.value}>
              {tz.label}
            </Option>
          ))}
        </Select>
      </Form.Item>




    </div>
  );

  const renderTaskParamsStep = () => {
    const renderParamFields = () => {
      switch (dataSourceType) {
        case 'eodhd_news':
          return (
            <>
              <Form.Item
                label="Max Results"
                name={['taskParams', 'max_results']}
                extra="Maximum number of news items to fetch per time, up to 1000"
              >
                <InputNumber 
                  min={1} 
                  max={1000} 
                  placeholder="100"
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Stock Symbols"
                name={['taskParams', 'symbols']}
                extra="Specify stock symbols, separated by commas, leave empty to get all"
              >
                <Input placeholder="AAPL,MSFT,GOOGL" />
              </Form.Item>
              
              <Form.Item
                label="Topic Tags"
                name={['taskParams', 'topic']}
                extra="News topic classification"
              >
                <Select placeholder="Select topic" allowClear>
                  <Option value="technology">Technology</Option>
                  <Option value="finance">Finance</Option>
                  <Option value="economics">Economics</Option>
                  <Option value="earnings">Earnings</Option>
                </Select>
              </Form.Item>
              
              <Form.Item
                label="Keywords"
                name={['taskParams', 'keywords']}
                extra="News content keywords, separated by commas"
              >
                <Input placeholder="AI,blockchain,cryptocurrency" />
              </Form.Item>
            </>
          );
          
        case 'eodhd_events':
          return (
            <>
              <Form.Item
                label="Data Limit"
                name={['taskParams', 'limit']}
                extra="Maximum number of events to fetch per time, up to 1000"
              >
                <InputNumber 
                  min={1} 
                  max={1000} 
                  placeholder="100"
                  style={{ width: '100%' }}
                />
              </Form.Item>
            </>
          );
          
        case 'eodhd_macro':
          return (
            <>
              <Form.Item
                label="Country"
                name={['taskParams', 'country']}
                rules={[{ required: true, message: 'Please select a country' }]}
                extra="Specify which countries to get macro indicators for, multiple selection allowed"
              >
                <Select 
                  placeholder="Select country" 
                  mode="multiple"
                  showSearch
                  optionLabelProp="label"
                  filterOption={(input, option) => {
                    const label = String(option?.label || '');
                    return label.toLowerCase().includes(input.toLowerCase());
                  }}
                >
                  <Option value="USA" label="🇺🇸 United States (USA)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇺🇸</span>
                      <span>United States (USA)</span>
                    </div>
                  </Option>
                  <Option value="CHN" label="🇨🇳 China (CHN)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇳</span>
                      <span>China (CHN)</span>
                    </div>
                  </Option>
                  <Option value="JPN" label="🇯🇵 Japan (JPN)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇯🇵</span>
                      <span>Japan (JPN)</span>
                    </div>
                  </Option>
                  <Option value="DEU" label="��🇪 Germany (DEU)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇩🇪</span>
                      <span>Germany (DEU)</span>
                    </div>
                  </Option>
                  <Option value="GBR" label="🇬🇧 United Kingdom (GBR)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇬🇧</span>
                      <span>United Kingdom (GBR)</span>
                    </div>
                  </Option>
                  <Option value="FRA" label="🇫🇷 France (FRA)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇫🇷</span>
                      <span>France (FRA)</span>
                    </div>
                  </Option>
                  <Option value="ITA" label="🇮🇹 Italy (ITA)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇹</span>
                      <span>Italy (ITA)</span>
                    </div>
                  </Option>
                  <Option value="BRA" label="🇧🇷 Brazil (BRA)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇧🇷</span>
                      <span>Brazil (BRA)</span>
                    </div>
                  </Option>
                  <Option value="CAN" label="🇨🇦 Canada (CAN)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇦</span>
                      <span>Canada (CAN)</span>
                    </div>
                  </Option>
                  <Option value="RUS" label="🇷🇺 Russia (RUS)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇷🇺</span>
                      <span>Russia (RUS)</span>
                    </div>
                  </Option>
                  <Option value="IND" label="🇮🇳 India (IND)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇳</span>
                      <span>India (IND)</span>
                    </div>
                  </Option>
                  <Option value="KOR" label="🇰🇷 South Korea (KOR)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇰🇷</span>
                      <span>South Korea (KOR)</span>
                    </div>
                  </Option>
                  <Option value="ESP" label="🇪🇸 Spain (ESP)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇪🇸</span>
                      <span>Spain (ESP)</span>
                    </div>
                  </Option>
                  <Option value="AUS" label="🇦🇺 Australia (AUS)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇦🇺</span>
                      <span>Australia (AUS)</span>
                    </div>
                  </Option>
                  <Option value="MEX" label="🇲🇽 Mexico (MEX)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇲🇽</span>
                      <span>Mexico (MEX)</span>
                    </div>
                  </Option>
                  <Option value="IDN" label="🇮🇩 Indonesia (IDN)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇩</span>
                      <span>Indonesia (IDN)</span>
                    </div>
                  </Option>
                  <Option value="NLD" label="🇳🇱 Netherlands (NLD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇳🇱</span>
                      <span>Netherlands (NLD)</span>
                    </div>
                  </Option>
                  <Option value="SAU" label="🇸🇦 Saudi Arabia (SAU)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇸🇦</span>
                      <span>Saudi Arabia (SAU)</span>
                    </div>
                  </Option>
                  <Option value="TUR" label="🇹🇷 Turkey (TUR)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇹🇷</span>
                      <span>Turkey (TUR)</span>
                    </div>
                  </Option>
                  <Option value="TWN" label="🇹🇼 Taiwan (TWN)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇹🇼</span>
                      <span>Taiwan (TWN)</span>
                    </div>
                  </Option>
                </Select>
              </Form.Item>
              
              <Form.Item
                label="Indicator Type"
                name={['taskParams', 'indicator']}
                extra="Specify macro indicator types, multiple selection allowed, leave empty to get all indicators"
              >
                <Select 
                  placeholder="Select indicator type" 
                  mode="multiple"
                  allowClear
                  showSearch
                  optionLabelProp="label"
                  filterOption={(input, option) => {
                    const label = String(option?.label || '');
                    return label.toLowerCase().includes(input.toLowerCase());
                  }}
                >
                  <Option value="real_interest_rate" label="📊 Real Interest Rate (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📊</span>
                      <span>Real Interest Rate (%)</span>
                    </div>
                  </Option>
                  <Option value="population_total" label="�� Total Population">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👥</span>
                      <span>Total Population</span>
                    </div>
                  </Option>
                  <Option value="population_growth_annual" label="📈 Annual Population Growth (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📈</span>
                      <span>Annual Population Growth (%)</span>
                    </div>
                  </Option>
                  <Option value="inflation_consumer_prices_annual" label="💰 Annual Consumer Price Inflation (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💰</span>
                      <span>Annual Consumer Price Inflation (%)</span>
                    </div>
                  </Option>
                  <Option value="consumer_price_index" label="📊 Consumer Price Index (2010=100)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📊</span>
                      <span>Consumer Price Index (2010=100)</span>
                    </div>
                  </Option>
                  <Option value="gdp_current_usd" label="💵 GDP (Current Price USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💵</span>
                      <span>GDP (Current Price USD)</span>
                    </div>
                  </Option>
                  <Option value="gdp_per_capita_usd" label="👤 Per Capita GDP (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👤</span>
                      <span>Per Capita GDP (USD)</span>
                    </div>
                  </Option>
                  <Option value="gdp_growth_annual" label="📈 Annual GDP Growth (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📈</span>
                      <span>Annual GDP Growth (%)</span>
                    </div>
                  </Option>
                  <Option value="debt_percent_gdp" label="💳 Debt to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💳</span>
                      <span>Debt to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="net_trades_goods_services" label="🔄 Net Trade in Goods and Services (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🔄</span>
                      <span>Net Trade in Goods and Services (USD)</span>
                    </div>
                  </Option>
                  <Option value="inflation_gdp_deflator_annual" label="📊 Annual GDP Deflator Inflation (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📊</span>
                      <span>Annual GDP Deflator Inflation (%)</span>
                    </div>
                  </Option>
                  <Option value="agriculture_value_added_percent_gdp" label="🌾 Agriculture Value Added to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🌾</span>
                      <span>Agriculture Value Added to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="industry_value_added_percent_gdp" label="🏭 Industry Value Added to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🏭</span>
                      <span>Industry Value Added to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="services_value_added_percent_gdp" label="💼 Services Value Added to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💼</span>
                      <span>Services Value Added to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="exports_of_goods_services_percent_gdp" label="📤 Export of Goods and Services to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📤</span>
                      <span>Export of Goods and Services to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="imports_of_goods_services_percent_gdp" label="📥 Import of Goods and Services to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📥</span>
                      <span>Import of Goods and Services to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="gross_capital_formation_percent_gdp" label="🏗️ Gross Capital Formation to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🏗️</span>
                      <span>Gross Capital Formation to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="net_migration" label="🚶 Net Migration">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🚶</span>
                      <span>Net Migration</span>
                    </div>
                  </Option>
                  <Option value="gni_usd" label="💵 Gross National Income (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💵</span>
                      <span>Gross National Income (USD)</span>
                    </div>
                  </Option>
                  <Option value="gni_per_capita_usd" label="👤 Per Capita Gross National Income (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👤</span>
                      <span>Per Capita Gross National Income (USD)</span>
                    </div>
                  </Option>
                  <Option value="gni_ppp_usd" label="💱 Purchasing Power Parity Gross National Income (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💱</span>
                      <span>Purchasing Power Parity Gross National Income (USD)</span>
                    </div>
                  </Option>
                  <Option value="gni_per_capita_ppp_usd" label="👤 Per Capita Purchasing Power Parity Gross National Income (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👤</span>
                      <span>Per Capita Purchasing Power Parity Gross National Income (USD)</span>
                    </div>
                  </Option>
                  <Option value="income_share_lowest_twenty" label="📊 Income Share of Lowest 20% (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📊</span>
                      <span>Income Share of Lowest 20% (%)</span>
                    </div>
                  </Option>
                  <Option value="life_expectancy" label="👴 Life Expectancy (Years)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👴</span>
                      <span>Life Expectancy (Years)</span>
                    </div>
                  </Option>
                  <Option value="fertility_rate" label="👶 Fertility Rate (Births per Woman)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👶</span>
                      <span>Fertility Rate (Births per Woman)</span>
                    </div>
                  </Option>
                  <Option value="prevalence_hiv_total" label="🩺 HIV Prevalence (15-49 Years %)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🩺</span>
                      <span>HIV Prevalence (15-49 Years %)</span>
                    </div>
                  </Option>
                  <Option value="co2_emissions_tons_per_capita" label="🌍 CO2 Emissions per Capita (Tons)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🌍</span>
                      <span>CO2 Emissions per Capita (Tons)</span>
                    </div>
                  </Option>
                  <Option value="surface_area_km" label="🗺️ Surface Area (Square Kilometers)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🗺️</span>
                      <span>Surface Area (Square Kilometers)</span>
                    </div>
                  </Option>
                  <Option value="poverty_poverty_lines_percent_population" label="🏚️ Poverty Line Population Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🏚️</span>
                      <span>Poverty Line Population Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="revenue_excluding_grants_percent_gdp" label="💰 Revenue to GDP Ratio (Excluding Grants %)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💰</span>
                      <span>Revenue to GDP Ratio (Excluding Grants %)</span>
                    </div>
                  </Option>
                  <Option value="cash_surplus_deficit_percent_gdp" label="💸 Cash Surplus/Deficit to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💸</span>
                      <span>Cash Surplus/Deficit to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="startup_procedures_register" label="🏢 Number of Startup Procedures">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🏢</span>
                      <span>Number of Startup Procedures</span>
                    </div>
                  </Option>
                  <Option value="market_cap_domestic_companies_percent_gdp" label="📈 Domestic Listed Companies Market Cap to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📈</span>
                      <span>Domestic Listed Companies Market Cap to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="mobile_subscriptions_per_hundred" label="📱 Mobile Phone Subscriptions per 100 People">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📱</span>
                      <span>Mobile Phone Subscriptions per 100 People</span>
                    </div>
                  </Option>
                  <Option value="internet_users_per_hundred" label="🌐 Internet Users per 100 People">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🌐</span>
                      <span>Internet Users per 100 People</span>
                    </div>
                  </Option>
                  <Option value="high_technology_exports_percent_total" label="🔬 High-Tech Exports to Manufactured Goods Exports Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🔬</span>
                      <span>High-Tech Exports to Manufactured Goods Exports Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="merchandise_trade_percent_gdp" label="📦 Merchandise Trade to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📦</span>
                      <span>Merchandise Trade to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="total_debt_service_percent_gni" label="�� Total Debt Service to GNI Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💳</span>
                      <span>Total Debt Service to GNI Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="unemployment_total_percent" label="👷 Total Unemployment Rate (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👷</span>
                      <span>Total Unemployment Rate (%)</span>
                    </div>
                  </Option>
                </Select>
              </Form.Item>
            </>
          );
          
        case 'eodhd_exchange':
        case 'eodhd_fundamentals':
          return (
            <>
              <Form.Item
                label="Exchanges"
                name={['taskParams', 'exchanges']}
                rules={[{ required: true, message: 'Please select at least one exchange' }]}
                extra={dataSourceType === 'eodhd_fundamentals' ? 
                  "Select exchanges to fetch fundamentals data from (Supports US, China, Hong Kong, Taiwan, Japan, UK markets)" : 
                  "Select exchanges to fetch ticker data from. Based on EODHD Exchanges API documentation."}
              >
                <Select 
                  placeholder="Select exchanges" 
                  mode="multiple"
                  showSearch
                  optionLabelProp="label"
                  filterOption={(input, option) => {
                    const label = String(option?.label || '');
                    return label.toLowerCase().includes(input.toLowerCase());
                  }}
                >
                  {/* 根据数据源类型显示不同的交易所选项 */}
                  {dataSourceType === 'eodhd_fundamentals' ? (
                    <>
                      {/* EODHD Fundamentals 支持多个国家和地区的交易所 */}
                      <Option value="NYSE" label="🏛️ New York Stock Exchange (NYSE)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏛️</span>
                          <span>New York Stock Exchange (NYSE)</span>
                        </div>
                      </Option>
                      <Option value="NASDAQ" label="💻 NASDAQ">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>💻</span>
                          <span>NASDAQ Global Market</span>
                        </div>
                      </Option>
                      <Option value="AMEX" label="🏦 American Stock Exchange (AMEX)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏦</span>
                          <span>American Stock Exchange (AMEX)</span>
                        </div>
                      </Option>
                      <Option value="SHG" label="🏢 Shanghai Stock Exchange (SHG)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏢</span>
                          <span>Shanghai Stock Exchange (SHG)</span>
                        </div>
                      </Option>
                      <Option value="SHE" label="🏪 Shenzhen Stock Exchange (SHE)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏪</span>
                          <span>Shenzhen Stock Exchange (SHE)</span>
                        </div>
                      </Option>
                      <Option value="HK" label="🏙️ Hong Kong Stock Exchange (HK)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏙️</span>
                          <span>Hong Kong Stock Exchange (HK)</span>
                        </div>
                      </Option>
                      <Option value="TWO" label="🏝️ Taiwan Stock Exchange (TWO)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏝️</span>
                          <span>Taiwan Stock Exchange (TWO)</span>
                        </div>
                      </Option>
                      <Option value="JP" label="🗾 Tokyo Stock Exchange (JP)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🗾</span>
                          <span>Tokyo Stock Exchange (JP)</span>
                        </div>
                      </Option>
                      <Option value="LSE" label="🇬🇧 London Stock Exchange (LSE)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🇬🇧</span>
                          <span>London Stock Exchange (LSE)</span>
                        </div>
                      </Option>
                      <Option value="AIM" label="💡 London AIM Market (AIM)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>💡</span>
                          <span>London AIM Market (AIM)</span>
                        </div>
                      </Option>
                    </>
                  ) : (
                    <>
                      {/* 其他数据源的完整交易所列表 */}
                      <Option value="US" label="🇺🇸 USA Stocks (NYSE, NASDAQ, etc.)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🇺🇸</span>
                          <span>USA Stocks (NYSE, NASDAQ, NYSE ARCA, OTC markets)</span>
                        </div>
                      </Option>
                      
                      {/* European Exchanges */}
                      <Option value="LSE" label="🇬🇧 London Stock Exchange">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🇬🇧</span>
                          <span>London Stock Exchange (LSE)</span>
                        </div>
                      </Option>
                  <Option value="PA" label="🇫🇷 Euronext Paris">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇫🇷</span>
                      <span>Euronext Paris (PA)</span>
                    </div>
                  </Option>
                  <Option value="AS" label="🇳🇱 Euronext Amsterdam">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇳🇱</span>
                      <span>Euronext Amsterdam (AS)</span>
                    </div>
                  </Option>
                  <Option value="BR" label="🇧🇪 Euronext Brussels">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇧🇪</span>
                      <span>Euronext Brussels (BR)</span>
                    </div>
                  </Option>
                  <Option value="VX" label="🇨🇭 Swiss Exchange (SIX)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇭</span>
                      <span>Swiss Exchange (SIX) (VX)</span>
                    </div>
                  </Option>
                  <Option value="MC" label="🇪🇸 Madrid Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇪🇸</span>
                      <span>Madrid Stock Exchange (MC)</span>
                    </div>
                  </Option>
                  <Option value="F" label="🇩🇪 Frankfurt Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇩🇪</span>
                      <span>Frankfurt Stock Exchange (F)</span>
                    </div>
                  </Option>
                  
                  {/* Asia-Pacific Exchanges */}
                  <Option value="HK" label="🇭🇰 Hong Kong Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇭🇰</span>
                      <span>Hong Kong Stock Exchange (HK)</span>
                    </div>
                  </Option>
                  <Option value="SHG" label="🇨🇳 Shanghai Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇳</span>
                      <span>Shanghai Stock Exchange (SHG)</span>
                    </div>
                  </Option>
                  <Option value="SHE" label="🇨🇳 Shenzhen Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇳</span>
                      <span>Shenzhen Stock Exchange (SHE)</span>
                    </div>
                  </Option>
                  <Option value="KO" label="🇰🇷 Korea Stock Exchange (KRX)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇰🇷</span>
                      <span>Korea Stock Exchange (KRX) (KO)</span>
                    </div>
                  </Option>
                  <Option value="TW" label="🇹🇼 Taiwan Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇹🇼</span>
                      <span>Taiwan Stock Exchange (TW)</span>
                    </div>
                  </Option>
                  <Option value="SG" label="🇸🇬 Singapore Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇸🇬</span>
                      <span>Singapore Exchange (SG)</span>
                    </div>
                  </Option>
                  <Option value="AU" label="🇦🇺 Australian Securities Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇦🇺</span>
                      <span>Australian Securities Exchange (AU)</span>
                    </div>
                  </Option>
                  
                  {/* North America Other */}
                  <Option value="TO" label="🇨🇦 Toronto Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇦</span>
                      <span>Toronto Stock Exchange (TO)</span>
                    </div>
                  </Option>
                  
                  {/* Indian Exchanges */}
                  <Option value="BSE" label="🇮🇳 Bombay Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇳</span>
                      <span>Bombay Stock Exchange (BSE)</span>
                    </div>
                  </Option>
                  <Option value="NSE" label="🇮🇳 National Stock Exchange of India">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇳</span>
                      <span>National Stock Exchange of India (NSE)</span>
                    </div>
                  </Option>
                  
                      {/* South American Exchanges */}
                      <Option value="SA" label="🇧🇷 São Paulo Stock Exchange (B3)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🇧🇷</span>
                          <span>São Paulo Stock Exchange (B3) (SA)</span>
                        </div>
                      </Option>
                    </>
                  )}
                </Select>
              </Form.Item>
              
              {/* Data Types selection for EODHD Fundamentals Low Frequency */}
              {dataSourceType === 'eodhd_fundamentals' && (
                <Form.Item
                  label="Data Types"
                  name={['taskParams', 'data_types']}
                  rules={[]}
                  extra="Select data types to fetch. Choose from 12 comprehensive data categories for complete company analysis."
                  initialValue={['General', 'Financials', 'Highlights', 'Valuation', 'Technicals', 'Holders', 'outstandingShares', 'Earnings', 'SharesStats', 'AnalystRatings', 'SplitsDividends', 'InsiderTransactions']}
                >
                  <Select 
                    placeholder="Select data types" 
                    mode="multiple"
                    style={{ width: '100%' }}
                    disabled={false}
                  >
                    <Option value="General" label="📊 General">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>📊</span>
                        <span>General (Company information, address, officers)</span>
                      </div>
                    </Option>
                    <Option value="Financials" label="💰 Financials">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>💰</span>
                        <span>Financials (Balance sheet, income statement, cash flow)</span>
                      </div>
                    </Option>
                    <Option value="Highlights" label="⭐ Highlights">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>⭐</span>
                        <span>Highlights (Key metrics: market cap, P/E, earnings, margins)</span>
                      </div>
                    </Option>
                    <Option value="Valuation" label="📈 Valuation">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>📈</span>
                        <span>Valuation (P/E ratios, enterprise value, price multiples)</span>
                      </div>
                    </Option>
                    <Option value="Technicals" label="📊 Technicals">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>📊</span>
                        <span>Technicals (Beta, 52-week range, moving averages, short data)</span>
                      </div>
                    </Option>
                    <Option value="Holders" label="👥 Holders">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>👥</span>
                        <span>Holders (Institutional holdings, fund holdings, ownership data)</span>
                      </div>
                    </Option>
                    <Option value="outstandingShares" label="📈 Outstanding Shares">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>📈</span>
                        <span>Outstanding Shares (Annual/quarterly shares outstanding data)</span>
                      </div>
                    </Option>
                    <Option value="Earnings" label="💸 Earnings">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>💸</span>
                        <span>Earnings (Annual earnings, history, trend, estimates)</span>
                      </div>
                    </Option>
                    <Option value="SharesStats" label="📊 Shares Stats">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>📊</span>
                        <span>Shares Stats (Outstanding, float, short interest, insider %)</span>
                      </div>
                    </Option>
                    <Option value="AnalystRatings" label="🎯 Analyst Ratings">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>🎯</span>
                        <span>Analyst Ratings (Strong buy/buy/hold/sell, target price)</span>
                      </div>
                    </Option>
                    <Option value="SplitsDividends" label="💰 Splits & Dividends">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>💰</span>
                        <span>Splits & Dividends (Dividend rate/yield, split history)</span>
                      </div>
                    </Option>
                    <Option value="InsiderTransactions" label="🔐 Insider Transactions">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>🔐</span>
                        <span>Insider Transactions (Executive trading activity)</span>
                      </div>
                    </Option>
                  </Select>
                </Form.Item>
              )}
            </>
          );
          
        default:
          return null;
      }
    };

    return (
      <div>
        <div style={{ textAlign: 'center', marginBottom: 24 }}>
          <Title level={4}>
            <BulbOutlined /> Parameter Configuration
          </Title>
          <Text type="secondary">
            Configure specific data acquisition parameters for {dataSourceName}
          </Text>
        </div>

        {renderParamFields()}

        <Alert
          message="Parameter Description"
          description="All parameters are optional, leave empty for default configuration. The task will be automatically enabled after creation, and you can control its enable/disable status through the switch on the main page, or modify parameters through the edit function."
          type="info"
          showIcon
        />
      </div>
    );
  };

  const steps = dataSourceType === 'eodhd_fundamentals' ? [
    {
      title: 'Parameter Configuration',
      description: 'Configure data types and exchanges',
      icon: <BulbOutlined />
    }
  ] : [
    {
      title: 'Schedule Configuration',
      description: 'Set execution time',
      icon: <ClockCircleOutlined />
    },
    {
      title: 'Parameter Configuration',
      description: 'Configure acquisition parameters',
      icon: <BulbOutlined />
    }
  ];

  return (
    <Modal
      title={
        <Space>
          {currentConfig.icon}
          {dataSourceType === 'eodhd_fundamentals' 
            ? `Update Data for ${dataSourceName}` 
            : `Configure Data Acquisition Strategy for ${dataSourceName}`}
        </Space>
      }
      open={open}
      onCancel={handleCancel}
      width={800}
      footer={
        <div style={{ textAlign: 'right' }}>
          <Space>
            <Button onClick={handleCancel}>
              Cancel
            </Button>
            {currentStep > 0 && dataSourceType !== 'eodhd_fundamentals' && (
              <Button onClick={handlePrev}>
                Previous Step
              </Button>
            )}
            <Button 
              type="primary" 
              onClick={handleNext}
              loading={loading}
              icon={(
                dataSourceType === 'eodhd_fundamentals' || 
                currentStep === 1
              ) ? <CheckCircleOutlined /> : undefined}
            >
              {dataSourceType === 'eodhd_fundamentals' 
                ? 'Update Data' 
                : (currentStep === 1 ? 'Create Strategy' : 'Next Step')}
            </Button>
          </Space>
        </div>
      }
      destroyOnClose
    >
      <div style={{ padding: '20px 0' }}>
        <Steps 
          current={currentStep} 
          style={{ marginBottom: 32 }}
          items={steps}
        />

        <Form
          form={form}
          layout="vertical"
          size="middle"
        >
          {renderStepContent()}
        </Form>
      </div>
      
      {/* Cron expression usage guide */}
      <CronExpressionGuide
        open={showCronGuide}
        onCancel={() => setShowCronGuide(false)}
      />
    </Modal>
  );
};

export default DataSourceConfigModal;
