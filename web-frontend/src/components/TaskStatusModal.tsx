import React, { useState, useEffect } from 'react';
import {
  Modal,
  Card,
  Descriptions,
  Progress,
  Badge,
  Typography,
  Space,
  Alert,
  Collapse,
  Row,
  Col,
  Statistic,
  Table,
  Tag
} from 'antd';
import {
  CheckCircleOutlined,
  ClockCircleOutlined,
  ExclamationCircleOutlined,
  InfoCircleOutlined,
  BarChartOutlined,
  DatabaseOutlined
} from '@ant-design/icons';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { getModalStyles } from '../utils/modalStyles';

const { Title, Text } = Typography;
const { Panel } = Collapse;

interface TaskStatusModalProps {
  open: boolean;
  onCancel: () => void;
  taskData?: {
    id: string;
    name: string;
    type: string;
    status: 'running' | 'completed' | 'failed' | 'scheduled';
    progress?: number;
    statistics?: {
      total_companies?: number;
      successful_companies?: number;
      failed_companies?: number;
      success_rate?: number;
      processed_exchanges?: number;
      data_types?: string[];
      exchange_details?: Record<string, any>;
    };
    last_execution?: {
      start_time: string;
      end_time?: string;
      duration?: number;
      message?: string;
    };
    next_execution?: string;
    cron_expression?: string;
  };
}

const TaskStatusModal: React.FC<TaskStatusModalProps> = ({
  open,
  onCancel,
  taskData
}) => {
  const { themeMode } = useDashboardSettings();
  const [refreshing, setRefreshing] = useState(false);

  useEffect(() => {
    if (open && taskData?.status === 'running') {
      // 如果任务正在运行，定期刷新状态
      const interval = setInterval(() => {
        setRefreshing(true);
        // 这里可以调用API刷新任务状态
        setTimeout(() => setRefreshing(false), 1000);
      }, 5000);

      return () => clearInterval(interval);
    }
  }, [open, taskData?.status]);

  const getStatusConfig = (status: string) => {
    switch (status) {
      case 'running':
        return {
          color: 'processing',
          icon: <ClockCircleOutlined />,
          text: '运行中'
        };
      case 'completed':
        return {
          color: 'success',
          icon: <CheckCircleOutlined />,
          text: '已完成'
        };
      case 'failed':
        return {
          color: 'error',
          icon: <ExclamationCircleOutlined />,
          text: '失败'
        };
      case 'scheduled':
        return {
          color: 'default',
          icon: <ClockCircleOutlined />,
          text: '已调度'
        };
      default:
        return {
          color: 'default',
          icon: <InfoCircleOutlined />,
          text: '未知'
        };
    }
  };

  const formatDataTypes = (dataTypes: string[]) => {
    const typeMap: Record<string, { label: string; color: string }> = {
      'General': { label: '基本信息', color: 'blue' },
      'Financials': { label: '财务报表', color: 'green' },
      'Highlights': { label: '关键指标', color: 'orange' },
      'Valuation': { label: '估值数据', color: 'purple' },
      'Technicals': { label: '技术指标', color: 'cyan' }
    };

    return dataTypes.map(type => (
      <Tag key={type} color={typeMap[type]?.color || 'default'}>
        {typeMap[type]?.label || type}
      </Tag>
    ));
  };

  const renderExchangeTable = () => {
    if (!taskData?.statistics?.exchange_details) {
      return null;
    }

    const columns = [
      {
        title: '交易所',
        dataIndex: 'exchange',
        key: 'exchange',
        render: (exchange: string) => (
          <Space>
            <DatabaseOutlined />
            <strong>{exchange}</strong>
          </Space>
        )
      },
      {
        title: '总公司数',
        dataIndex: 'total',
        key: 'total',
        align: 'center' as const
      },
      {
        title: '成功',
        dataIndex: 'successful',
        key: 'successful',
        align: 'center' as const,
        render: (value: number) => (
          <Badge count={value} style={{ backgroundColor: '#52c41a' }} />
        )
      },
      {
        title: '失败',
        dataIndex: 'failed',
        key: 'failed',
        align: 'center' as const,
        render: (value: number) => (
          <Badge count={value} style={{ backgroundColor: value > 0 ? '#ff4d4f' : '#d9d9d9' }} />
        )
      },
      {
        title: '成功率',
        dataIndex: 'success_rate',
        key: 'success_rate',
        align: 'center' as const,
        render: (rate: number) => (
          <Progress 
            type="circle" 
            size={40} 
            percent={rate} 
            format={percent => `${percent?.toFixed(1)}%`}
            strokeColor={rate >= 90 ? '#52c41a' : rate >= 70 ? '#faad14' : '#ff4d4f'}
          />
        )
      },
      {
        title: '状态',
        dataIndex: 'status',
        key: 'status',
        align: 'center' as const,
        render: (status: string, record: any) => {
          if (record.successful > 0 && record.failed === 0) {
            return <Badge status="success" text="完成" />;
          } else if (record.successful > 0 && record.failed > 0) {
            return <Badge status="warning" text="部分成功" />;
          } else if (record.failed > 0) {
            return <Badge status="error" text="失败" />;
          }
          return <Badge status="default" text="等待" />;
        }
      }
    ];

    const dataSource = Object.entries(taskData.statistics.exchange_details).map(([exchange, data]: [string, any]) => ({
      key: exchange,
      exchange,
      total: data.total || 0,
      successful: data.successful || 0,
      failed: data.failed || 0,
      success_rate: data.total > 0 ? ((data.successful || 0) / data.total * 100) : 0
    }));

    return (
      <Table
        columns={columns}
        dataSource={dataSource}
        pagination={false}
        size="small"
        bordered
      />
    );
  };

  if (!taskData) {
    return null;
  }

  const statusConfig = getStatusConfig(taskData.status);

  return (
    <Modal
      title={
        <Space>
          <BarChartOutlined />
          任务执行状态 - {taskData.name}
        </Space>
      }
      open={open}
      onCancel={onCancel}
      width={900}
      footer={null}
      destroyOnClose
      styles={{
        ...getModalStyles(themeMode),
        body: { ...getModalStyles(themeMode).body, maxHeight: '70vh', overflow: 'auto' },
      }}
    >
      <div>
        {/* 任务基本信息 */}
        <Card size="small" style={{ marginBottom: 16 }}>
          <Descriptions 
            column={2} 
            size="small"
            labelStyle={{ fontWeight: 'bold' }}
          >
            <Descriptions.Item label="任务状态">
              <Space>
                {statusConfig.icon}
                <Badge 
                  status={statusConfig.color as any} 
                  text={statusConfig.text}
                />
              </Space>
            </Descriptions.Item>
            <Descriptions.Item label="任务类型">
              {taskData.type === 'eodhd_fundamentals' ? '基本面数据获取' : taskData.type}
            </Descriptions.Item>
            <Descriptions.Item label="Cron表达式">
              <code>{taskData.cron_expression || 'N/A'}</code>
            </Descriptions.Item>
            <Descriptions.Item label="下次执行">
              {taskData.next_execution || 'N/A'}
            </Descriptions.Item>
          </Descriptions>
        </Card>

        {/* 执行进度 */}
        {taskData.status === 'running' && (
          <Card size="small" style={{ marginBottom: 16 }}>
            <Title level={5}>
              <ClockCircleOutlined /> 执行进度
            </Title>
            <Progress 
              percent={taskData.progress || 0} 
              status="active"
              format={percent => `${percent}%`}
            />
            {refreshing && (
              <Text type="secondary" style={{ fontSize: '12px' }}>
                正在刷新状态...
              </Text>
            )}
          </Card>
        )}

        {/* 统计信息 */}
        {taskData.statistics && (
          <Card size="small" style={{ marginBottom: 16 }}>
            <Title level={5}>
              <BarChartOutlined /> 执行统计
            </Title>
            
            <Row gutter={16} style={{ marginBottom: 16 }}>
              <Col span={6}>
                <Statistic
                  title="总公司数"
                  value={taskData.statistics.total_companies || 0}
                  prefix={<DatabaseOutlined />}
                />
              </Col>
              <Col span={6}>
                <Statistic
                  title="成功数量"
                  value={taskData.statistics.successful_companies || 0}
                  valueStyle={{ color: '#3f8600' }}
                  prefix={<CheckCircleOutlined />}
                />
              </Col>
              <Col span={6}>
                <Statistic
                  title="失败数量"
                  value={taskData.statistics.failed_companies || 0}
                  valueStyle={{ color: '#cf1322' }}
                  prefix={<ExclamationCircleOutlined />}
                />
              </Col>
              <Col span={6}>
                <Statistic
                  title="成功率"
                  value={taskData.statistics.success_rate || 0}
                  precision={1}
                  suffix="%"
                  valueStyle={{ 
                    color: (taskData.statistics.success_rate || 0) >= 90 ? '#3f8600' : 
                           (taskData.statistics.success_rate || 0) >= 70 ? '#fa8c16' : '#cf1322'
                  }}
                />
              </Col>
            </Row>

            {/* 数据类型标签 */}
            {taskData.statistics.data_types && (
              <div style={{ marginBottom: 16 }}>
                <Text strong>数据类型: </Text>
                <Space wrap>
                  {formatDataTypes(taskData.statistics.data_types)}
                </Space>
              </div>
            )}

            {/* 处理的交易所 */}
            {taskData.statistics.processed_exchanges && (
              <div>
                <Text strong>处理交易所: </Text>
                <Badge count={taskData.statistics.processed_exchanges} style={{ backgroundColor: '#1890ff' }} />
              </div>
            )}
          </Card>
        )}

        {/* 详细结果 */}
        <Collapse>
          {/* 交易所详情 */}
          {taskData.statistics?.exchange_details && (
            <Panel 
              header={
                <Space>
                  <DatabaseOutlined />
                  交易所处理详情
                </Space>
              } 
              key="exchanges"
            >
              {renderExchangeTable()}
            </Panel>
          )}

          {/* 执行历史 */}
          {taskData.last_execution && (
            <Panel 
              header={
                <Space>
                  <ClockCircleOutlined />
                  执行历史
                </Space>
              } 
              key="history"
            >
              <Descriptions column={1} size="small">
                <Descriptions.Item label="开始时间">
                  {taskData.last_execution.start_time}
                </Descriptions.Item>
                {taskData.last_execution.end_time && (
                  <Descriptions.Item label="结束时间">
                    {taskData.last_execution.end_time}
                  </Descriptions.Item>
                )}
                {taskData.last_execution.duration && (
                  <Descriptions.Item label="执行时长">
                    {taskData.last_execution.duration}秒
                  </Descriptions.Item>
                )}
                {taskData.last_execution.message && (
                  <Descriptions.Item label="执行消息">
                    <Alert
                      message={taskData.last_execution.message}
                      type={taskData.status === 'failed' ? 'error' : 'info'}
                      showIcon
                      style={{ marginTop: 8 }}
                    />
                  </Descriptions.Item>
                )}
              </Descriptions>
            </Panel>
          )}
        </Collapse>
      </div>
    </Modal>
  );
};

export default TaskStatusModal;


