import React, { useState, useEffect } from 'react';
import {
  Modal,
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
  Switch,
  Tabs,
  Tag,
  Tooltip,
  message
} from 'antd';
import {
  ScheduleOutlined,
  InfoCircleOutlined,
  ClockCircleOutlined,
  SettingOutlined,
  PlayCircleOutlined
} from '@ant-design/icons';
import { schedulerAPI, CreateEODHDNewsTaskParams, CronPreset, TimezoneOption } from '../services/scheduler';

const { Title, Text, Paragraph } = Typography;
const { Option } = Select;
const { TextArea } = Input;

interface CronScheduleModalProps {
  open: boolean;
  onCancel: () => void;
  onSuccess: (taskId: string) => void;
  taskType?: 'eodhd_news' | 'economic_events' | 'macro_indicators';
  defaultParams?: Record<string, any>;
}

const CronScheduleModal: React.FC<CronScheduleModalProps> = ({
  open,
  onCancel,
  onSuccess,
  taskType = 'eodhd_news',
  defaultParams = {}
}) => {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [cronPresets, setCronPresets] = useState<CronPreset[]>([]);
  const [timezoneOptions, setTimezoneOptions] = useState<TimezoneOption[]>([]);
  // Removed scheduleType state as we only use cron expressions now
  const [customCron, setCustomCron] = useState('');
  const [cronDescription, setCronDescription] = useState('');

  // 加载预设数据
  useEffect(() => {
    if (open) {
      loadPresets();
      // 设置默认值（不显示在输入框中）
      form.setFieldsValue({
        name: getDefaultTaskName(),
        enabled: true,
        timezone: 'America/New_York',  // 默认选择美国纽约时区
        ...defaultParams
      });
      // 不设置默认的cron表达式，让用户自己填写
      setCustomCron('');
      setCronDescription('');
    }
  }, [open, taskType]);

  // 加载预设配置
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

  // 获取默认任务名称
  const getDefaultTaskName = () => {
    const taskNames = {
      eodhd_news: 'EODHD News Auto Fetch',
      economic_events: 'Economic Events Auto Fetch',
      macro_indicators: 'Macro Indicators Auto Fetch'
    };
    return taskNames[taskType];
  };

  // 更新Cron描述
  const updateCronDescription = (expression: string) => {
    const description = schedulerAPI.describeCronExpression(expression);
    setCronDescription(description);
  };

  // 处理Cron表达式变化
  const handleCronChange = (value: string) => {
    setCustomCron(value);
    updateCronDescription(value);
    form.setFieldsValue({ cron_expression: value });
  };

  // 处理预设选择
  const handlePresetSelect = (expression: string) => {
    handleCronChange(expression);
  };

  // 创建定时任务
  const handleSubmit = async () => {
    try {
      setLoading(true);
      const values = await form.validateFields();

      let result;
      
      if (taskType === 'eodhd_news') {
        // EODHD新闻任务的特殊处理
        const eodhdParams: CreateEODHDNewsTaskParams = {
          name: values.name,
          cron_expression: values.cron_expression,
          timezone: values.timezone,
          enabled: values.enabled,
          max_results: values.max_results,
          symbols: values.symbols,
          topic: values.topic,
          topic_filter: values.topic_filter,
          keywords: values.keywords
        };

        // Using direct cron expression input

        result = await schedulerAPI.createEODHDNewsTask(eodhdParams);
      } else {
        // 其他类型任务的通用处理
        result = await schedulerAPI.createCronTask({
          name: values.name,
          task_type: taskType,
          cron_expression: values.cron_expression,
          timezone: values.timezone,
          enabled: values.enabled,
          params: {
            ...defaultParams,
            ...values.params
          }
        });
      }

      if (result.success && result.data) {
        message.success(result.message);
        onSuccess(result.data.task_id);
        handleCancel();
      } else {
        message.error(result.message);
      }
    } catch (error: any) {
      console.error('创建定时任务失败:', error);
      message.error('创建定时任务失败');
    } finally {
      setLoading(false);
    }
  };

  // 取消操作
  const handleCancel = () => {
    form.resetFields();
    setCustomCron('');
    setCronDescription('');
    // No schedule type to reset
    onCancel();
  };

  // 快速创建默认EODHD新闻任务
  const handleCreateDefault = async () => {
    try {
      setLoading(true);
      const result = await schedulerAPI.createDefaultEODHDNewsTask();
      
      if (result.success && result.data) {
        message.success(result.message);
        onSuccess(result.data.task_id);
        handleCancel();
      } else {
        message.error(result.message);
      }
    } catch (error) {
      console.error('创建默认任务失败:', error);
      message.error('创建默认任务失败');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Modal
      title={
        <Space>
          <ScheduleOutlined />
          Configure Data Fetch Strategy - Cron表达式定期请求
        </Space>
      }
      open={open}
      onCancel={handleCancel}
      width={900}
      footer={[
        <Button key="cancel" onClick={handleCancel}>
          取消
        </Button>,
        taskType === 'eodhd_news' && (
          <Button
            key="default"
            type="dashed"
            icon={<PlayCircleOutlined />}
            onClick={handleCreateDefault}
            loading={loading}
          >
            创建默认策略（每15分钟）
          </Button>
        ),
        <Button
          key="submit"
          type="primary"
          icon={<ScheduleOutlined />}
          onClick={handleSubmit}
          loading={loading}
        >
          创建定时任务
        </Button>
      ]}
      destroyOnClose
    >
      <div style={{ marginBottom: 16 }}>
        <Alert
          message="定期数据获取策略配置"
          description={
            <div>
              <p>• 基于Cron表达式实现精确的定时数据获取</p>
              <p>• 支持纽约时间、北京时间等多时区配置</p>
              <p>• 自动处理任务失败重试和日志记录</p>
              <p>• 推荐新闻数据使用每15分钟更新策略</p>
            </div>
          }
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
        />
      </div>

      <Form form={form} layout="vertical">
        <Tabs defaultActiveKey="basic">
          {/* 基础配置 */}
          <Tabs.TabPane
            tab={<span><SettingOutlined />基础配置</span>}
            key="basic"
          >
            <Row gutter={16}>
              <Col span={12}>
                <Form.Item
                  name="name"
                  label="任务名称"
                  rules={[{ required: true, message: '请输入任务名称' }]}
                >
                  <Input placeholder="输入任务名称" />
                </Form.Item>
              </Col>
              <Col span={12}>
                <Form.Item
                  name="timezone"
                  label="时区"
                  rules={[{ required: true, message: '请选择时区' }]}
                >
                  <Select placeholder="选择时区">
                    {timezoneOptions.map(tz => (
                      <Option key={tz.value} value={tz.value}>
                        <Tooltip title={tz.description}>
                          {tz.label}
                        </Tooltip>
                      </Option>
                    ))}
                  </Select>
                </Form.Item>
              </Col>
              <Col span={24}>
                <Alert
                  message="调度方式"
                  description="使用自定义Cron表达式进行精确的定时任务调度"
                  type="info"
                  showIcon
                  style={{ marginBottom: 16 }}
                />
              </Col>
            </Row>
          </Tabs.TabPane>

          {/* 调度配置 */}
          <Tabs.TabPane
            tab={<span><ClockCircleOutlined />调度配置</span>}
            key="schedule"
          >
            <div>
                <Form.Item
                  name="cron_expression"
                  label="Cron表达式"
                  rules={[
                    { required: true, message: '请输入Cron表达式' },
                    {
                      validator: (_, value) => {
                        if (!value) return Promise.resolve();
                        const { valid, message: errorMsg } = schedulerAPI.validateCronExpression(value);
                        return valid ? Promise.resolve() : Promise.reject(new Error(errorMsg));
                      }
                    }
                  ]}
                  extra={
                    <div>
                      <Text type="secondary">格式：分 时 日 月 星期 (如：*/15 * * * * 表示每15分钟)</Text>
                      {cronDescription && (
                        <div style={{ marginTop: 4 }}>
                          <Tag color="blue">{cronDescription}</Tag>
                        </div>
                      )}
                    </div>
                  }
                >
                  <Input
                    placeholder="如：*/15 * * * *"
                    value={customCron}
                    onChange={(e) => handleCronChange(e.target.value)}
                  />
                </Form.Item>

                <Card title="常用Cron表达式预设" size="small" style={{ marginTop: 16 }}>
                  <Row gutter={[8, 8]}>
                    {cronPresets.map((preset, index) => (
                      <Col key={index} span={8}>
                        <Button
                          size="small"
                          block
                          onClick={() => handlePresetSelect(preset.expression)}
                          style={{ textAlign: 'left', height: 'auto', padding: '4px 8px' }}
                        >
                          <div>
                            <div style={{ fontWeight: 'bold', fontSize: '12px' }}>
                              {preset.name}
                            </div>
                            <div style={{ fontSize: '10px', color: '#666' }}>
                              {preset.expression}
                            </div>
                            <div style={{ fontSize: '10px', color: '#999' }}>
                              {preset.description}
                            </div>
                          </div>
                        </Button>
                      </Col>
                    ))}
                  </Row>
                </Card>
            </div>

            <Form.Item name="enabled" label="启用状态" valuePropName="checked">
              <Switch checkedChildren="启用" unCheckedChildren="禁用" />
            </Form.Item>
          </Tabs.TabPane>

          {/* 任务参数 */}
          {taskType === 'eodhd_news' && (
            <Tabs.TabPane
              tab={<span><InfoCircleOutlined />EODHD参数</span>}
              key="params"
            >
              <Alert
                message="EODHD新闻获取参数"
                description="配置新闻获取的具体参数，留空则使用默认设置"
                type="info"
                showIcon
                style={{ marginBottom: 16 }}
              />

              <Row gutter={16}>
                <Col span={12}>
                  <Form.Item
                    name="max_results"
                    label="最大结果数"
                    extra="每次获取的最大新闻条数，最多1000条"
                  >
                    <InputNumber
                      min={1}
                      max={1000}
                      placeholder="请输入最大结果数"
                      style={{ width: '100%' }}
                    />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item
                    name="symbols"
                    label="股票代码"
                    tooltip="多个代码用逗号分隔，如：AAPL.US,TSLA.US"
                  >
                    <Input placeholder="如：AAPL.US,TSLA.US" />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item
                    name="topic"
                    label="题材标签"
                  >
                    <Input placeholder="如：technology, politics" />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item
                    name="topic_filter"
                    label="主题过滤器"
                  >
                    <Input placeholder="使用数据库中的主题进行过滤" />
                  </Form.Item>
                </Col>
                <Col span={24}>
                  <Form.Item
                    name="keywords"
                    label="关键词"
                    tooltip="多个关键词用逗号分隔"
                  >
                    <Input placeholder="如：inflation,interest rate,GDP" />
                  </Form.Item>
                </Col>
              </Row>
            </Tabs.TabPane>
          )}
        </Tabs>
      </Form>
    </Modal>
  );
};

export default CronScheduleModal;
