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
  QuestionCircleOutlined
} from '@ant-design/icons';
import { schedulerAPI, CreateEODHDNewsTaskParams, TimezoneOption } from '../services/scheduler';
import CronExpressionGuide from './CronExpressionGuide';

const { Title, Text } = Typography;
const { Option } = Select;

interface DataSourceConfigModalProps {
  open: boolean;
  onCancel: () => void;
  onSuccess: (taskId: string) => void;
  dataSourceType: 'eodhd_news' | 'eodhd_events' | 'eodhd_macro';
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
      description: '从EODHD API获取最新的股票新闻和市场信息'
    },
    eodhd_events: {
      icon: <ScheduleOutlined style={{ color: '#52c41a' }} />,
      params: ['limit'],
      defaultSchedule: '0 9 * * 1-5',
      defaultName: 'Economic Events Auto Fetch',
      description: '获取经济日历事件数据，包括GDP、CPI等重要指标'
    },
    eodhd_macro: {
      icon: <BarChartOutlined style={{ color: '#722ed1' }} />,
      params: ['country', 'indicator'],
      defaultSchedule: '0 10 1 * *',
      defaultName: 'Macro Indicators Auto Fetch',
      description: '获取宏观经济指标数据，如通胀率、失业率等'
    }
  };

  const currentConfig = dataSourceConfigs[dataSourceType];

  const loadPresets = async () => {
    try {
      console.log('开始加载时区配置...');
      const timezonesResponse = await schedulerAPI.getTimezonePresets();
      console.log('时区API响应:', timezonesResponse);
      
      if (timezonesResponse.success) {
        console.log('设置时区选项:', timezonesResponse.data);
        setTimezoneOptions(timezonesResponse.data || []);
      } else {
        console.error('时区API返回失败:', timezonesResponse.message);
      }
    } catch (error) {
      console.error('加载时区配置失败:', error);
    }
  };

  const resetModal = () => {
    setCurrentStep(0);
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
      
      if (currentStep < 1) {
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
      
      const params: CreateEODHDNewsTaskParams = {
        name: currentConfig.defaultName,
        cron_expression: data.cronExpression!,
        timezone: data.timezone!,
        enabled: true, // 新创建的任务默认启用，用户可通过主页面switch控制
        ...data.taskParams
      };

      let result;
      if (dataSourceType === 'eodhd_news') {
        result = await schedulerAPI.createEODHDNewsTask(params);
      } else {
        // 其他任务类型的创建逻辑
        const taskType = dataSourceType === 'eodhd_events' ? 'economic_events' : 
                        dataSourceType === 'eodhd_macro' ? 'macro_indicators' : 
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

      if (result.success) {
        message.success('数据获取策略配置成功！');
        onSuccess(result.data?.task_id || '');
        handleCancel();
      } else {
        message.error(result.message || '配置失败');
      }
    } catch (error) {
      console.error('配置失败:', error);
      message.error('配置失败');
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
          <ClockCircleOutlined /> 调度配置
        </Title>
        <Text type="secondary">
          为 {dataSourceName} 设置数据获取的执行时间和频率
        </Text>
      </div>

      {/* 数据源信息卡片 */}
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
            Cron表达式
            <Button
              type="text"
              size="small"
              icon={<QuestionCircleOutlined />}
              onClick={() => setShowCronGuide(true)}
              style={{ padding: 0, height: 'auto' }}
            >
              使用说明
            </Button>
          </Space>
        }
        name="cronExpression"
        rules={[
          { required: true, message: '请输入Cron表达式' },
          {
            validator: (_, value) => {
              if (!value) return Promise.resolve();
              const { valid, message: msg } = schedulerAPI.validateCronExpression(value);
              return valid ? Promise.resolve() : Promise.reject(new Error(msg));
            }
          }
        ]}
        extra="格式：分 时 日 月 星期（例如：*/15 * * * * 表示每15分钟执行一次）"
      >
        <Input 
          placeholder="输入Cron表达式，如: */15 * * * *"
          suffix={
            <Tooltip title="格式：分 时 日 月 星期">
              <InfoCircleOutlined />
            </Tooltip>
          }
        />
      </Form.Item>

      <Form.Item
        label="时区"
        name="timezone"
        rules={[{ required: true, message: '请选择时区' }]}
      >
        <Select placeholder="选择时区" showSearch>
          {timezoneOptions.map(tz => (
            <Option key={tz.value} value={tz.value}>
              {tz.label}
            </Option>
          ))}
        </Select>
      </Form.Item>



      <Alert
        message="推荐配置"
        description={`建议使用 "${currentConfig.defaultSchedule}" 表达式，在纽约时间执行，适合获取美股相关数据。任务创建后将自动启用，您可以通过主页面的开关控制启用/禁用状态。`}
        type="info"
        showIcon
      />
    </div>
  );

  const renderTaskParamsStep = () => {
    const renderParamFields = () => {
      switch (dataSourceType) {
        case 'eodhd_news':
          return (
            <>
              <Form.Item
                label="最大结果数"
                name={['taskParams', 'max_results']}
                extra="每次获取的最大新闻条数，最多1000条"
              >
                <InputNumber 
                  min={1} 
                  max={1000} 
                  placeholder="100"
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="股票代码"
                name={['taskParams', 'symbols']}
                extra="指定股票代码，多个用逗号分隔，留空获取所有"
              >
                <Input placeholder="AAPL,MSFT,GOOGL" />
              </Form.Item>
              
              <Form.Item
                label="主题标签"
                name={['taskParams', 'topic']}
                extra="新闻主题分类"
              >
                <Select placeholder="选择主题" allowClear>
                  <Option value="technology">科技</Option>
                  <Option value="finance">金融</Option>
                  <Option value="economics">经济</Option>
                  <Option value="earnings">财报</Option>
                </Select>
              </Form.Item>
              
              <Form.Item
                label="关键词"
                name={['taskParams', 'keywords']}
                extra="新闻内容关键词，多个用逗号分隔"
              >
                <Input placeholder="AI,blockchain,cryptocurrency" />
              </Form.Item>
            </>
          );
          
        case 'eodhd_events':
          return (
            <>
              <Form.Item
                label="数据条数限制"
                name={['taskParams', 'limit']}
                extra="每次获取的最大事件数，最多1000条"
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
                label="国家"
                name={['taskParams', 'country']}
                rules={[{ required: true, message: '请选择国家' }]}
                extra="指定获取哪个国家的宏观指标"
              >
                <Select placeholder="选择国家">
                  <Option value="USA">美国</Option>
                  <Option value="CHN">中国</Option>
                  <Option value="JPN">日本</Option>
                  <Option value="GBR">英国</Option>
                  <Option value="DEU">德国</Option>
                </Select>
              </Form.Item>
              
              <Form.Item
                label="指标类型"
                name={['taskParams', 'indicator']}
                extra="指定宏观指标类型，留空获取所有"
              >
                <Select placeholder="选择指标类型" allowClear>
                  <Option value="GDP">GDP</Option>
                  <Option value="inflation_rate">通胀率</Option>
                  <Option value="unemployment_rate">失业率</Option>
                  <Option value="interest_rate">利率</Option>
                  <Option value="trade_balance">贸易平衡</Option>
                </Select>
              </Form.Item>
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
            <BulbOutlined /> 参数配置
          </Title>
          <Text type="secondary">
            为 {dataSourceName} 配置具体的数据获取参数
          </Text>
        </div>

        {renderParamFields()}

        <Alert
          message="参数说明"
          description="所有参数都是可选的，留空将使用默认配置。任务创建后将自动启用，您可以通过主页面开关控制启用/禁用状态，也可以通过编辑功能修改参数。"
          type="info"
          showIcon
        />
      </div>
    );
  };

  const steps = [
    {
      title: '调度配置',
      description: '设置执行时间',
      icon: <ClockCircleOutlined />
    },
    {
      title: '参数配置',
      description: '配置获取参数',
      icon: <BulbOutlined />
    }
  ];

  return (
    <Modal
      title={
        <Space>
          {currentConfig.icon}
          配置 {dataSourceName} 数据获取策略
        </Space>
      }
      open={open}
      onCancel={handleCancel}
      width={800}
      footer={
        <div style={{ textAlign: 'right' }}>
          <Space>
            <Button onClick={handleCancel}>
              取消
            </Button>
            {currentStep > 0 && (
              <Button onClick={handlePrev}>
                上一步
              </Button>
            )}
            <Button 
              type="primary" 
              onClick={handleNext}
              loading={loading}
              icon={currentStep === 1 ? <CheckCircleOutlined /> : undefined}
            >
              {currentStep === 1 ? '创建策略' : '下一步'}
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
      
      {/* Cron表达式使用说明 */}
      <CronExpressionGuide
        open={showCronGuide}
        onCancel={() => setShowCronGuide(false)}
      />
    </Modal>
  );
};

export default DataSourceConfigModal;
