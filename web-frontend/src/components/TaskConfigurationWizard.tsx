import React, { useState, useEffect } from 'react';
import {
  Modal,
  Steps,
  Form,
  Input,
  Select,
  Switch,
  Button,
  Space,
  Card,
  Typography,
  Alert,
  Row,
  Col,
  InputNumber,
  Tooltip,
  Tag,
  Divider,
  message
} from 'antd';
import {
  SettingOutlined,
  ClockCircleOutlined,
  InfoCircleOutlined,
  CheckCircleOutlined,
  ScheduleOutlined,
  RocketOutlined,
  BulbOutlined,
  BarChartOutlined
} from '@ant-design/icons';
import { schedulerAPI, CreateEODHDNewsTaskParams, CronPreset, TimezoneOption } from '../services/scheduler';

const { Title, Text } = Typography;
const { Option } = Select;

interface TaskConfigurationWizardProps {
  open: boolean;
  onCancel: () => void;
  onSuccess: (taskId: string) => void;
}

interface WizardFormData {
  // 第一步：任务类型选择
  taskType: 'eodhd_news' | 'economic_events' | 'macro_indicators';
  
  // 第二步：基本配置
  name: string;
  description?: string;
  enabled: boolean;
  
  // 第三步：调度配置
  scheduleType: 'cron' | 'preset';
  cronExpression: string;
  timezone: string;
  
  // 第四步：任务参数
  taskParams: Record<string, any>;
}

const TaskConfigurationWizard: React.FC<TaskConfigurationWizardProps> = ({
  open,
  onCancel,
  onSuccess
}) => {
  const [currentStep, setCurrentStep] = useState(0);
  const [form] = Form.useForm<WizardFormData>();
  const [loading, setLoading] = useState(false);
  const [cronPresets, setCronPresets] = useState<CronPreset[]>([]);
  const [timezoneOptions, setTimezoneOptions] = useState<TimezoneOption[]>([]);
  const [formData, setFormData] = useState<Partial<WizardFormData>>({});

  const taskTypeOptions: Array<{
    value: 'eodhd_news' | 'economic_events' | 'macro_indicators';
    label: string;
    description: string;
    icon: React.ReactNode;
    params: string[];
    defaultSchedule: string;
    recommended?: boolean;
  }> = [
    {
      value: 'eodhd_news',
      label: 'EODHD News Data',
      description: 'Get latest stock news and market information from EODHD API',
      icon: <InfoCircleOutlined style={{ color: '#1890ff' }} />,
      params: ['max_results', 'symbols', 'topic', 'keywords'],
      defaultSchedule: '*/15 * * * *',
      recommended: true
    },
    {
      value: 'economic_events',
      label: 'Economic Events Data',
      description: 'Get economic calendar event data including GDP, CPI and other important indicators',
      icon: <ScheduleOutlined style={{ color: '#52c41a' }} />,
      params: ['limit'],
      defaultSchedule: '0 9 * * 1-5'
    },
    {
      value: 'macro_indicators',
      label: 'Macroeconomic Indicators',
      description: 'Get macroeconomic indicator data such as inflation rate, unemployment rate, etc.',
      icon: <BarChartOutlined style={{ color: '#722ed1' }} />,
      params: ['country', 'indicator'],
      defaultSchedule: '0 10 1 * *'
    }
  ];

  useEffect(() => {
    if (open) {
      loadPresets();
      resetWizard();
    }
  }, [open]); // eslint-disable-line react-hooks/exhaustive-deps

  const loadPresets = async () => {
    try {
      const [cronResult, timezoneResult] = await Promise.all([
        schedulerAPI.getCronPresets(),
        schedulerAPI.getTimezonePresets()
      ]);
      
      if (cronResult.success && cronResult.data) {
        setCronPresets(cronResult.data);
      }
      
      if (timezoneResult.success && timezoneResult.data) {
        setTimezoneOptions(timezoneResult.data);
      }
    } catch (error) {
      console.error('Failed to load preset configuration:', error);
    }
  };

  const resetWizard = () => {
    setCurrentStep(0);
    setFormData({});
    form.resetFields();
    
    // 设置默认时区
    form.setFieldsValue({
      timezone: 'America/New_York'  // 默认选择美国纽约时区
    });
  };

  const handleNext = async () => {
    try {
      const values = await form.validateFields();
      const newFormData = { ...formData, ...values };
      setFormData(newFormData);
      
      if (currentStep < 3) {
        setCurrentStep(currentStep + 1);
        
        // 自动填充下一步的默认值（不显示cron表达式）
        if (currentStep === 0 && values.taskType) {
          form.setFieldsValue({
            name: getDefaultTaskName(values.taskType),
            enabled: true,
            timezone: 'America/New_York',
            scheduleType: 'preset'
          });
        }
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

  const handleSubmit = async (data: Partial<WizardFormData>) => {
    try {
      setLoading(true);
      
      const params: CreateEODHDNewsTaskParams = {
        name: data.name!,
        cron_expression: data.cronExpression!,
        timezone: data.timezone!,
        enabled: data.enabled!,
        ...data.taskParams
      };

      let result;
      if (data.taskType === 'eodhd_news') {
        result = await schedulerAPI.createEODHDNewsTask(params);
      } else {
        // 其他任务类型的创建逻辑
        result = await schedulerAPI.createCronTask({
          name: data.name!,
          task_type: data.taskType!,
          cron_expression: data.cronExpression!,
          timezone: data.timezone!,
          enabled: data.enabled!,
          params: data.taskParams
        });
      }

      if (result.success) {
        message.success('Task created successfully!');
        onSuccess(result.data?.task_id || '');
        handleCancel();
      } else {
        message.error(result.message || 'Task creation failed');
      }
    } catch (error) {
      console.error('Task creation failed:', error);
      message.error('Task creation failed');
    } finally {
      setLoading(false);
    }
  };

  const handleCancel = () => {
    resetWizard();
    onCancel();
  };

  const getDefaultTaskName = (taskType: string) => {
    const names = {
      eodhd_news: 'EODHD News Auto Fetch',
      economic_events: 'Economic Events Auto Fetch',
      macro_indicators: 'Macro Indicators Auto Fetch'
    };
    return names[taskType as keyof typeof names] || 'Auto Task';
  };

  const renderStepContent = () => {
    switch (currentStep) {
      case 0:
        return renderTaskTypeStep();
      case 1:
        return renderBasicConfigStep();
      case 2:
        return renderScheduleConfigStep();
      case 3:
        return renderTaskParamsStep();
      default:
        return null;
    }
  };

  const renderTaskTypeStep = () => (
    <div>
      <div style={{ textAlign: 'center', marginBottom: 24 }}>
        <Title level={4}>
          <RocketOutlined /> Select Task Type
        </Title>
        <Text type="secondary">
          Select the type of automation task you want to create, different types of tasks have different data sources and configuration options
        </Text>
      </div>

      <Form.Item
        name="taskType"
        rules={[{ required: true, message: 'Please select a task type' }]}
      >
        <div>
          {taskTypeOptions.map(option => (
            <Card
              key={option.value}
              hoverable
              style={{ 
                marginBottom: 16,
                border: formData.taskType === option.value ? '2px solid #1890ff' : '1px solid #d9d9d9'
              }}
              onClick={() => {
                form.setFieldsValue({ taskType: option.value });
                setFormData({ ...formData, taskType: option.value });
              }}
            >
              <Row align="middle">
                <Col span={2}>
                  {option.icon}
                </Col>
                <Col span={18}>
                  <div>
                    <Space>
                      <Text strong>{option.label}</Text>
                      {option.recommended && <Tag color="gold">Recommended</Tag>}
                    </Space>
                    <div style={{ marginTop: 4 }}>
                      <Text type="secondary" style={{ fontSize: '12px' }}>
                        {option.description}
                      </Text>
                    </div>
                  </div>
                </Col>
                <Col span={4} style={{ textAlign: 'right' }}>
                  <Text type="secondary" style={{ fontSize: '11px' }}>
                    Default: {option.defaultSchedule}
                  </Text>
                </Col>
              </Row>
            </Card>
          ))}
        </div>
      </Form.Item>

      <Alert
        message="Task Type Description"
        description="After selecting a task type, the wizard will provide you with corresponding configuration options and recommended settings. You can customize specific parameters in subsequent steps."
        type="info"
        showIcon
      />
    </div>
  );

  const renderBasicConfigStep = () => (
    <div>
      <div style={{ textAlign: 'center', marginBottom: 24 }}>
        <Title level={4}>
          <SettingOutlined /> Basic Configuration
        </Title>
        <Text type="secondary">
          Set basic information for the task, including name, description, and enabled status
        </Text>
      </div>

      <Form.Item
        label="Task Name"
        name="name"
        rules={[{ required: true, message: 'Please enter a task name' }]}
      >
        <Input placeholder="Enter task name" />
      </Form.Item>

      <Form.Item
        label="Task Description"
        name="description"
      >
        <Input.TextArea 
          placeholder="Enter task description (optional)"
          rows={3}
        />
      </Form.Item>

      <Form.Item
        label="Enabled Status"
        name="enabled"
        valuePropName="checked"
        extra="Whether to enable this task immediately after creation"
      >
        <Switch 
          checkedChildren="Enabled" 
          unCheckedChildren="Disabled"
        />
      </Form.Item>

      {formData.taskType && (
        <Alert
          message={`${taskTypeOptions.find(t => t.value === formData.taskType)?.label} Task`}
          description={taskTypeOptions.find(t => t.value === formData.taskType)?.description}
          type="info"
          showIcon
        />
      )}
    </div>
  );

  const renderScheduleConfigStep = () => (
    <div>
      <div style={{ textAlign: 'center', marginBottom: 24 }}>
        <Title level={4}>
          <ClockCircleOutlined /> Schedule Configuration
        </Title>
        <Text type="secondary">
          Set the execution time and frequency of the task, supporting Cron expressions and preset options
        </Text>
      </div>

      <Form.Item
        label="Schedule Type"
        name="scheduleType"
        rules={[{ required: true, message: 'Please select a schedule type' }]}
      >
        <Select placeholder="Select schedule type">
          <Option value="preset">Use preset template</Option>
          <Option value="cron">Custom Cron expression</Option>
        </Select>
      </Form.Item>

      {form.getFieldValue('scheduleType') === 'preset' && (
        <Form.Item
          label="Preset Template"
          name="cronExpression"
          rules={[{ required: true, message: 'Please select a preset template' }]}
        >
          <Select placeholder="Select preset template">
            {cronPresets.map(preset => (
              <Option key={preset.expression} value={preset.expression}>
                <div>
                  <Text strong>{preset.name}</Text>
                  <br />
                  <Text type="secondary" style={{ fontSize: '11px' }}>
                    {preset.description}
                  </Text>
                </div>
              </Option>
            ))}
          </Select>
        </Form.Item>
      )}

      {form.getFieldValue('scheduleType') === 'cron' && (
        <Form.Item
          label="Cron Expression"
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
      )}

      <Form.Item
        label="Timezone"
        name="timezone"
        rules={[{ required: true, message: 'Please select a timezone' }]}
      >
        <Select placeholder="Select timezone" showSearch>
          {timezoneOptions.map(tz => (
            <Option key={tz.value} value={tz.value}>
              {tz.label} ({tz.description})
            </Option>
          ))}
        </Select>
      </Form.Item>

      {form.getFieldValue('cronExpression') && (
        <Alert
          message="Schedule Preview"
          description={`This task will execute every ${schedulerAPI.describeCronExpression(form.getFieldValue('cronExpression'))}`}
          type="success"
          showIcon
        />
      )}
    </div>
  );

  const renderTaskParamsStep = () => {
    const taskType = formData.taskType;
    const taskConfig = taskTypeOptions.find(t => t.value === taskType);

    return (
      <div>
        <div style={{ textAlign: 'center', marginBottom: 24 }}>
          <Title level={4}>
            <BulbOutlined /> Task Parameters
          </Title>
          <Text type="secondary">
            Configure specific parameters and options for {taskConfig?.label}
          </Text>
        </div>

        {taskType === 'eodhd_news' && (
          <>
            <Form.Item
              label="Max Results"
              name={['taskParams', 'max_results']}
              extra="Maximum number of news items to fetch per time, up to 1000"
            >
              <InputNumber min={1} max={1000} placeholder="Enter max results" style={{ width: '100%' }} />
            </Form.Item>

            <Form.Item
              label="Stock Symbols"
              name={['taskParams', 'symbols']}
              extra="Separate multiple codes with commas, e.g., AAPL.US,TSLA.US"
            >
              <Input placeholder="Enter stock symbols (optional)" />
            </Form.Item>

            <Form.Item
              label="Topic Tags"
              name={['taskParams', 'topic']}
              extra="EODHD API topic parameter, e.g., technology, politics"
            >
              <Input placeholder="Enter topic tags (optional)" />
            </Form.Item>

            <Form.Item
              label="Keywords"
              name={['taskParams', 'keywords']}
              extra="Custom keywords, separated by commas"
            >
              <Input placeholder="Enter keywords (optional)" />
            </Form.Item>
          </>
        )}

        {taskType === 'economic_events' && (
          <>
            <Form.Item
              label="Result Limit"
              name={['taskParams', 'limit']}
              extra="Maximum number of events to fetch per time, up to 1000"
            >
              <InputNumber min={1} max={1000} placeholder="Enter result limit" style={{ width: '100%' }} />
            </Form.Item>
          </>
        )}

        {taskType === 'macro_indicators' && (
          <>
            <Form.Item
              label="Country Code"
              name={['taskParams', 'country']}
              rules={[{ required: true, message: 'Please select a country' }]}
              extra="Specify which countries to get macroeconomic indicators for, can be multiple"
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
                <Option value="USA" label="🇺🇸 USA">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇺🇸</span>
                    <span>USA</span>
                  </div>
                </Option>
                <Option value="CHN" label="🇨🇳 China">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇨🇳</span>
                    <span>China</span>
                  </div>
                </Option>
                <Option value="JPN" label="🇯🇵 Japan">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇯🇵</span>
                    <span>Japan</span>
                  </div>
                </Option>
                <Option value="DEU" label="🇩🇪 Germany">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇩🇪</span>
                    <span>Germany</span>
                  </div>
                </Option>
                <Option value="GBR" label="🇬🇧 UK">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇬🇧</span>
                    <span>UK</span>
                  </div>
                </Option>
                <Option value="FRA" label="🇫🇷 France">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇫🇷</span>
                    <span>France</span>
                  </div>
                </Option>
                <Option value="ITA" label="🇮🇹 Italy">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇮🇹</span>
                    <span>Italy</span>
                  </div>
                </Option>
                <Option value="BRA" label="🇧🇷 Brazil">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇧🇷</span>
                    <span>Brazil</span>
                  </div>
                </Option>
                <Option value="CAN" label="🇨🇦 Canada">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇨🇦</span>
                    <span>Canada</span>
                  </div>
                </Option>
                <Option value="RUS" label="🇷🇺 Russia">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇷🇺</span>
                    <span>Russia</span>
                  </div>
                </Option>
                <Option value="IND" label="🇮🇳 India">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇮🇳</span>
                    <span>India</span>
                  </div>
                </Option>
                <Option value="KOR" label="🇰🇷 South Korea">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇰🇷</span>
                    <span>South Korea</span>
                  </div>
                </Option>
                <Option value="ESP" label="🇪🇸 Spain">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇪🇸</span>
                    <span>Spain</span>
                  </div>
                </Option>
                <Option value="AUS" label="🇦🇺 Australia">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇦🇺</span>
                    <span>Australia</span>
                  </div>
                </Option>
                <Option value="MEX" label="🇲🇽 Mexico">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇲🇽</span>
                    <span>Mexico</span>
                  </div>
                </Option>
                <Option value="IDN" label="🇮🇩 Indonesia">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇮🇩</span>
                    <span>Indonesia</span>
                  </div>
                </Option>
                <Option value="NLD" label="🇳🇱 Netherlands">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇳🇱</span>
                    <span>Netherlands</span>
                  </div>
                </Option>
                <Option value="SAU" label="🇸🇦 Saudi Arabia">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇸🇦</span>
                    <span>Saudi Arabia</span>
                  </div>
                </Option>
                <Option value="TUR" label="🇹🇷 Turkey">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇹🇷</span>
                    <span>Turkey</span>
                  </div>
                </Option>
                <Option value="TWN" label="🇹🇼 Taiwan">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🇹🇼</span>
                    <span>Taiwan</span>
                  </div>
                </Option>
              </Select>
            </Form.Item>

            <Form.Item
              label="Indicator Code"
              name={['taskParams', 'indicator']}
              extra="Specify the type of macroeconomic indicator, can be multiple, leave empty to get all indicators"
            >
              <Select 
                placeholder="Select indicator (optional)" 
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
                <Option value="population_total" label="👥 Total Population">
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
                <Option value="gdp_current_usd" label="💵 GDP (Current USD)">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>💵</span>
                    <span>GDP (Current USD)</span>
                  </div>
                </Option>
                <Option value="gdp_per_capita_usd" label="👤 GDP per Capita (USD)">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>👤</span>
                    <span>GDP per Capita (USD)</span>
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
                <Option value="gni_per_capita_usd" label="👤 GNI per Capita (USD)">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>👤</span>
                    <span>GNI per Capita (USD)</span>
                  </div>
                </Option>
                <Option value="gni_ppp_usd" label="💱 GNI at Purchasing Power Parity (USD)">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>💱</span>
                    <span>GNI at Purchasing Power Parity (USD)</span>
                  </div>
                </Option>
                <Option value="gni_per_capita_ppp_usd" label="👤 GNI per Capita at Purchasing Power Parity (USD)">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>👤</span>
                    <span>GNI per Capita at Purchasing Power Parity (USD)</span>
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
                <Option value="high_technology_exports_percent_total" label="🔬 High-Tech Exports to Total Manufactured Goods Exports Ratio (%)">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>🔬</span>
                    <span>High-Tech Exports to Total Manufactured Goods Exports Ratio (%)</span>
                  </div>
                </Option>
                <Option value="merchandise_trade_percent_gdp" label="📦 Merchandise Trade to GDP Ratio (%)">
                  <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span>📦</span>
                    <span>Merchandise Trade to GDP Ratio (%)</span>
                  </div>
                </Option>
                <Option value="total_debt_service_percent_gni" label="💳 Total Debt Service to GNI Ratio (%)">
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
        )}

        <Divider />

        <Alert
          message="Configuration Overview"
          description={
            <div>
              <p><strong>Task Type:</strong>{taskConfig?.label}</p>
              <p><strong>Task Name:</strong>{formData.name}</p>
              <p><strong>Schedule Configuration:</strong>{form.getFieldValue('cronExpression') && schedulerAPI.describeCronExpression(form.getFieldValue('cronExpression'))}</p>
              <p><strong>Timezone:</strong>{form.getFieldValue('timezone')}</p>
              <p><strong>Enabled Status:</strong>{formData.enabled ? 'Enabled' : 'Disabled'}</p>
            </div>
          }
          type="info"
          showIcon
        />
      </div>
    );
  };

  const steps = [
    {
      title: 'Task Type',
      description: 'Select Task Type',
      icon: <RocketOutlined />
    },
    {
      title: 'Basic Configuration',
      description: 'Set Basic Information',
      icon: <SettingOutlined />
    },
    {
      title: 'Schedule Configuration',
      description: 'Set Execution Time',
      icon: <ClockCircleOutlined />
    },
    {
      title: 'Task Parameters',
      description: 'Configure Task Parameters',
      icon: <BulbOutlined />
    }
  ];

  return (
    <Modal
      title={
        <Space>
          <ScheduleOutlined />
          Task Configuration Wizard
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
            {currentStep > 0 && (
              <Button onClick={handlePrev}>
                Previous Step
              </Button>
            )}
            <Button 
              type="primary" 
              onClick={handleNext}
              loading={loading}
              icon={currentStep === 3 ? <CheckCircleOutlined /> : undefined}
            >
              {currentStep === 3 ? 'Create Task' : 'Next Step'}
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
    </Modal>
  );
};

export default TaskConfigurationWizard;
