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
      label: 'EODHD 新闻数据',
      description: '从EODHD API获取最新的股票新闻和市场信息',
      icon: <InfoCircleOutlined style={{ color: '#1890ff' }} />,
      params: ['max_results', 'symbols', 'topic', 'keywords'],
      defaultSchedule: '*/15 * * * *',
      recommended: true
    },
    {
      value: 'economic_events',
      label: '经济事件数据',
      description: '获取经济日历事件数据，包括GDP、CPI等重要指标',
      icon: <ScheduleOutlined style={{ color: '#52c41a' }} />,
      params: ['limit'],
      defaultSchedule: '0 9 * * 1-5'
    },
    {
      value: 'macro_indicators',
      label: '宏观经济指标',
      description: '获取宏观经济指标数据，如通胀率、失业率等',
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
      console.error('加载预设配置失败:', error);
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
        message.success('任务创建成功！');
        onSuccess(result.data?.task_id || '');
        handleCancel();
      } else {
        message.error(result.message || '任务创建失败');
      }
    } catch (error) {
      console.error('创建任务失败:', error);
      message.error('创建任务失败');
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
          <RocketOutlined /> 选择任务类型
        </Title>
        <Text type="secondary">
          选择您要创建的自动化任务类型，不同类型的任务具有不同的数据源和配置选项
        </Text>
      </div>

      <Form.Item
        name="taskType"
        rules={[{ required: true, message: '请选择任务类型' }]}
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
                      {option.recommended && <Tag color="gold">推荐</Tag>}
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
                    默认: {option.defaultSchedule}
                  </Text>
                </Col>
              </Row>
            </Card>
          ))}
        </div>
      </Form.Item>

      <Alert
        message="任务类型说明"
        description="选择任务类型后，向导将为您提供相应的配置选项和推荐设置。您可以在后续步骤中自定义具体参数。"
        type="info"
        showIcon
      />
    </div>
  );

  const renderBasicConfigStep = () => (
    <div>
      <div style={{ textAlign: 'center', marginBottom: 24 }}>
        <Title level={4}>
          <SettingOutlined /> 基本配置
        </Title>
        <Text type="secondary">
          设置任务的基本信息，包括名称、描述和启用状态
        </Text>
      </div>

      <Form.Item
        label="任务名称"
        name="name"
        rules={[{ required: true, message: '请输入任务名称' }]}
      >
        <Input placeholder="输入任务名称" />
      </Form.Item>

      <Form.Item
        label="任务描述"
        name="description"
      >
        <Input.TextArea 
          placeholder="输入任务描述（可选）"
          rows={3}
        />
      </Form.Item>

      <Form.Item
        label="启用状态"
        name="enabled"
        valuePropName="checked"
        extra="创建后是否立即启用此任务"
      >
        <Switch 
          checkedChildren="启用" 
          unCheckedChildren="禁用"
        />
      </Form.Item>

      {formData.taskType && (
        <Alert
          message={`${taskTypeOptions.find(t => t.value === formData.taskType)?.label} 任务`}
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
          <ClockCircleOutlined /> 调度配置
        </Title>
        <Text type="secondary">
          设置任务的执行时间和频率，支持Cron表达式和预设选项
        </Text>
      </div>

      <Form.Item
        label="调度方式"
        name="scheduleType"
        rules={[{ required: true, message: '请选择调度方式' }]}
      >
        <Select placeholder="选择调度方式">
          <Option value="preset">使用预设模板</Option>
          <Option value="cron">自定义Cron表达式</Option>
        </Select>
      </Form.Item>

      {form.getFieldValue('scheduleType') === 'preset' && (
        <Form.Item
          label="预设模板"
          name="cronExpression"
          rules={[{ required: true, message: '请选择预设模板' }]}
        >
          <Select placeholder="选择预设模板">
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
          label="Cron表达式"
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
      )}

      <Form.Item
        label="时区"
        name="timezone"
        rules={[{ required: true, message: '请选择时区' }]}
      >
        <Select placeholder="选择时区" showSearch>
          {timezoneOptions.map(tz => (
            <Option key={tz.value} value={tz.value}>
              {tz.label} ({tz.description})
            </Option>
          ))}
        </Select>
      </Form.Item>

      {form.getFieldValue('cronExpression') && (
        <Alert
          message="调度预览"
          description={`该任务将${schedulerAPI.describeCronExpression(form.getFieldValue('cronExpression'))}`}
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
            <BulbOutlined /> 任务参数
          </Title>
          <Text type="secondary">
            配置 {taskConfig?.label} 的具体参数和选项
          </Text>
        </div>

        {taskType === 'eodhd_news' && (
          <>
            <Form.Item
              label="最大结果数"
              name={['taskParams', 'max_results']}
              extra="每次获取的最大新闻条数，最多1000条"
            >
              <InputNumber min={1} max={1000} placeholder="请输入最大结果数" style={{ width: '100%' }} />
            </Form.Item>

            <Form.Item
              label="股票代码"
              name={['taskParams', 'symbols']}
              extra="多个代码用逗号分隔，如：AAPL.US,TSLA.US"
            >
              <Input placeholder="输入股票代码（可选）" />
            </Form.Item>

            <Form.Item
              label="主题标签"
              name={['taskParams', 'topic']}
              extra="EODHD API主题参数，如：technology, politics"
            >
              <Input placeholder="输入主题标签（可选）" />
            </Form.Item>

            <Form.Item
              label="关键词"
              name={['taskParams', 'keywords']}
              extra="自定义关键词，用逗号分隔"
            >
              <Input placeholder="输入关键词（可选）" />
            </Form.Item>
          </>
        )}

        {taskType === 'economic_events' && (
          <>
            <Form.Item
              label="结果限制"
              name={['taskParams', 'limit']}
              extra="每次获取的最大事件数，最多1000条"
            >
              <InputNumber min={1} max={1000} placeholder="请输入结果限制数" style={{ width: '100%' }} />
            </Form.Item>
          </>
        )}

        {taskType === 'macro_indicators' && (
          <>
            <Form.Item
              label="国家代码"
              name={['taskParams', 'country']}
              rules={[{ required: true, message: '请选择国家' }]}
            >
              <Select placeholder="选择国家">
                <Option value="USA">美国 (USA)</Option>
                <Option value="CHN">中国 (CHN)</Option>
                <Option value="GBR">英国 (GBR)</Option>
                <Option value="DEU">德国 (DEU)</Option>
                <Option value="JPN">日本 (JPN)</Option>
              </Select>
            </Form.Item>

            <Form.Item
              label="指标代码"
              name={['taskParams', 'indicator']}
              extra="留空获取所有指标"
            >
              <Select placeholder="选择指标（可选）" allowClear>
                <Option value="gdp_current_usd">GDP (现价美元)</Option>
                <Option value="gdp_per_capita_usd">人均GDP</Option>
                <Option value="gdp_growth_annual">GDP年增长率</Option>
                <Option value="inflation_consumer_prices_annual">通胀率</Option>
                <Option value="unemployment_total">失业率</Option>
              </Select>
            </Form.Item>
          </>
        )}

        <Divider />

        <Alert
          message="配置总览"
          description={
            <div>
              <p><strong>任务类型：</strong>{taskConfig?.label}</p>
              <p><strong>任务名称：</strong>{formData.name}</p>
              <p><strong>调度配置：</strong>{form.getFieldValue('cronExpression') && schedulerAPI.describeCronExpression(form.getFieldValue('cronExpression'))}</p>
              <p><strong>时区：</strong>{form.getFieldValue('timezone')}</p>
              <p><strong>启用状态：</strong>{formData.enabled ? '启用' : '禁用'}</p>
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
      title: '任务类型',
      description: '选择任务类型',
      icon: <RocketOutlined />
    },
    {
      title: '基本配置',
      description: '设置基本信息',
      icon: <SettingOutlined />
    },
    {
      title: '调度配置',
      description: '设置执行时间',
      icon: <ClockCircleOutlined />
    },
    {
      title: '任务参数',
      description: '配置任务参数',
      icon: <BulbOutlined />
    }
  ];

  return (
    <Modal
      title={
        <Space>
          <ScheduleOutlined />
          任务配置向导
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
              icon={currentStep === 3 ? <CheckCircleOutlined /> : undefined}
            >
              {currentStep === 3 ? '创建任务' : '下一步'}
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
