import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  Table,
  Tag,
  Space,
  Typography,
  Button,
  Select,
  Row,
  Col,
  Statistic,
  Progress,
  Empty,
  message,
  Modal,
  Descriptions
} from 'antd';
import {
  ClockCircleOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
  ReloadOutlined,
  CalendarOutlined,
  BarChartOutlined,
  InfoCircleOutlined,
  PlayCircleOutlined,
  TrophyOutlined,
  DownloadOutlined,
  DeleteOutlined
} from '@ant-design/icons';
import dayjs, { Dayjs } from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import { schedulerAPI, ExecutionLog, ScheduledTask } from '../services/scheduler';

dayjs.extend(relativeTime);

const { Text } = Typography;
const { Option } = Select;

// Use ExecutionLog type imported from scheduler service

interface TaskExecutionHistoryProps {
  taskId?: string;
  showTaskFilter?: boolean;
  height?: number;
  refreshTrigger?: number; // Add refresh trigger prop
}

const TaskExecutionHistory: React.FC<TaskExecutionHistoryProps> = ({
  taskId,
  showTaskFilter = true,
  height = 600,
  refreshTrigger
}) => {
  const [logs, setLogs] = useState<ExecutionLog[]>([]);
  const [loading, setLoading] = useState(false);
  const [selectedTaskId, setSelectedTaskId] = useState<string | undefined>(taskId);
  const [dateRange] = useState<[Dayjs, Dayjs]>([
    dayjs().subtract(7, 'day'),
    dayjs()
  ]);
  const [statusFilter, setStatusFilter] = useState<string>('all');
  const [selectedLog, setSelectedLog] = useState<ExecutionLog | null>(null);
  const [detailVisible, setDetailVisible] = useState(false);
  const [tasks, setTasks] = useState<ScheduledTask[]>([]);

  const loadTasks = useCallback(async () => {
    try {
      const response = await schedulerAPI.getTasks();
      if (response.success && response.data) {
        setTasks(response.data);
      } else {
        console.error('Failed to get task list:', response.message);
        message.error('Failed to get task list');
      }
    } catch (error) {
      console.error('Failed to get task list:', error);
      message.error('Failed to get task list');
    }
  }, []);

  const loadExecutionLogs = useCallback(async () => {
    try {
      setLoading(true);
      
      let response;
      if (selectedTaskId && selectedTaskId !== 'all') {
        // Get execution logs for specific task
        response = await schedulerAPI.getTaskLogs(selectedTaskId, 50, 0);
      } else {
        // Get execution logs for all tasks
        response = await schedulerAPI.getAllExecutionLogs(50, 0);
      }

      if (response.success && response.data) {
        let filteredLogs = response.data.logs;
        
        // Filter by status
        if (statusFilter && statusFilter !== 'all') {
          filteredLogs = filteredLogs.filter((log: ExecutionLog) => log.status === statusFilter);
        }

        // Filter by date range
        if (dateRange && dateRange.length === 2) {
          const [startDate, endDate] = dateRange;
          filteredLogs = filteredLogs.filter((log: ExecutionLog) => {
            const logDate = dayjs(log.started_at);
            return logDate.isAfter(startDate.startOf('day')) && logDate.isBefore(endDate.endOf('day'));
          });
        }

        setLogs(filteredLogs);
      } else {
        message.error(response.message || 'Failed to get execution history');
        setLogs([]);
      }
      
    } catch (error) {
      console.error('Failed to load execution logs:', error);
      message.error('Failed to load execution logs');
      setLogs([]);
    } finally {
      setLoading(false);
    }
  }, [selectedTaskId, statusFilter, dateRange]);

  useEffect(() => {
    loadExecutionLogs();
  }, [selectedTaskId, dateRange, statusFilter, loadExecutionLogs]);

  useEffect(() => {
    loadTasks();
  }, [loadTasks]);

  // Refresh data when refreshTrigger changes
  useEffect(() => {
    if (refreshTrigger !== undefined) {
      loadExecutionLogs();
      loadTasks();
    }
  }, [refreshTrigger, loadExecutionLogs, loadTasks]);

  const getStatusIcon = (status: string) => {
    const statusConfig = {
      success: { color: '#00b96b', icon: <CheckCircleOutlined /> },
      failed: { color: '#ff7875', icon: <ExclamationCircleOutlined /> },
      error: { color: '#ff7875', icon: <ExclamationCircleOutlined /> },
      running: { color: '#40a9ff', icon: <PlayCircleOutlined /> },
      timeout: { color: '#fa8c16', icon: <ClockCircleOutlined /> },
      cancelled: { color: '#8c8c8c', icon: <InfoCircleOutlined /> }
    };
    
    const config = statusConfig[status as keyof typeof statusConfig] || statusConfig.error;
    return (
      <span style={{ color: config.color, fontSize: '16px' }}>
        {config.icon}
      </span>
    );
  };

  const formatDuration = (durationMs?: number) => {
    if (!durationMs) return '-';
    const seconds = Math.floor(durationMs / 1000);
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    
    if (minutes > 0) {
      return `${minutes}m${remainingSeconds}s`;
    }
    return `${remainingSeconds}s`;
  };

  const getTaskTypeIcon = (taskType: string) => {
    switch (taskType) {
      case 'eodhd_news':
        return <InfoCircleOutlined style={{ color: '#1890ff' }} />;
      case 'economic_events':
        return <CalendarOutlined style={{ color: '#52c41a' }} />;
      case 'macro_indicators':
        return <BarChartOutlined style={{ color: '#722ed1' }} />;
      default:
        return <ClockCircleOutlined />;
    }
  };

  const handleViewDetail = (log: ExecutionLog) => {
    setSelectedLog(log);
    setDetailVisible(true);
  };

  const handleDownloadCSV = async () => {
    try {
      console.log('Starting CSV download, task ID:', selectedTaskId);
      message.loading('Generating CSV file...', 0);
      await schedulerAPI.downloadExecutionLogsCSV(selectedTaskId, 1000, 0);
      message.destroy();
      message.success('CSV file downloaded successfully');
    } catch (error: any) {
      message.destroy();
      console.error('CSV download failed:', error);
      console.error('Error details:', {
        message: error?.message,
        response: error?.response,
        status: error?.response?.status,
        data: error?.response?.data
      });
      message.error(`CSV download failed: ${error?.response?.data?.detail || error?.message}`);
    }
  };

  const handleClearLogs = async () => {
    try {
      // Show confirmation dialog
      const confirmed = await new Promise<boolean>((resolve) => {
        Modal.confirm({
          title: 'Confirm Clear',
          content: 'Are you sure you want to clear all task execution logs? This operation cannot be undone!',
          okText: 'Confirm Clear',
          cancelText: 'Cancel',
          okType: 'danger',
          onOk: () => resolve(true),
          onCancel: () => resolve(false),
        });
      });

      if (!confirmed) {
        return;
      }

      message.loading('Clearing execution logs...', 0);
      const result = await schedulerAPI.clearExecutionLogs();
      message.destroy();

      if (result.success) {
        message.success('Execution logs cleared');
        // Reload log data
        await loadExecutionLogs();
      } else {
        message.error(`Clear failed: ${result.message}`);
      }
    } catch (error: any) {
      message.destroy();
      console.error('Failed to clear logs:', error);
      message.error(`Failed to clear logs: ${error?.message || 'Unknown error'}`);
    }
  };

  // Statistics data
  const stats = {
    total: logs.length,
    success: logs.filter(log => log.status === 'success').length,
    failed: logs.filter(log => ['failed', 'error'].includes(log.status)).length,
    avgDuration: logs.length > 0 
      ? logs.filter(log => log.duration_ms).reduce((acc, log) => acc + (log.duration_ms || 0), 0) / logs.filter(log => log.duration_ms).length
      : 0,
    successRate: logs.length > 0 
      ? (logs.filter(log => log.status === 'success').length / logs.length * 100)
      : 0
  };

  const columns = [
    {
      title: 'Task Info',
      key: 'task',
      render: (log: ExecutionLog) => (
        <Space>
          {getTaskTypeIcon(log.task_type)}
          <div>
            <div>
              <Text strong style={{ fontSize: '12px' }}>{log.task_name}</Text>
            </div>
            <div>
              <Text type="secondary" style={{ fontSize: '11px' }}>
                {log.task_type.replace('_', ' ').toUpperCase()}
              </Text>
            </div>
          </div>
        </Space>
      )
    },
    {
      title: 'Execution Time',
      key: 'time',
      render: (log: ExecutionLog) => (
        <div>
          <div>
            <Text style={{ fontSize: '11px' }}>
              {dayjs(log.started_at).format('MM-DD HH:mm:ss')}
            </Text>
          </div>
          <div>
            <Text type="secondary" style={{ fontSize: '10px' }}>
              {dayjs(log.started_at).fromNow()}
            </Text>
          </div>
        </div>
      ),
      sorter: (a: ExecutionLog, b: ExecutionLog) => 
        dayjs(a.started_at).valueOf() - dayjs(b.started_at).valueOf()
    },
    {
      title: 'Duration',
      dataIndex: 'duration_ms',
      key: 'duration',
      render: (duration: number) => (
        <Text style={{ fontSize: '11px' }}>
          {formatDuration(duration)}
        </Text>
      ),
      sorter: (a: ExecutionLog, b: ExecutionLog) => (a.duration_ms || 0) - (b.duration_ms || 0)
    },
    {
      title: 'Status',
      dataIndex: 'status',
      key: 'status',
      align: 'center' as const,
      render: (status: string) => getStatusIcon(status),
      filters: [
        { text: 'Success', value: 'success' },
        { text: 'Failed', value: 'failed' },
        { text: 'Error', value: 'error' },
        { text: 'Running', value: 'running' },
        { text: 'Timeout', value: 'timeout' },
        { text: 'Cancelled', value: 'cancelled' }
      ],
      onFilter: (value: any, record: ExecutionLog) => record.status === value
    },
    {
      title: 'Result',
      key: 'result',
      render: (log: ExecutionLog) => (
        <div style={{ maxWidth: 200 }}>
          {log.status === 'success' ? (
            <Text style={{ fontSize: '11px', color: '#52c41a' }}>
              {log.result_message || 'Execution successful'}
            </Text>
          ) : log.status === 'running' ? (
            <Text style={{ fontSize: '11px', color: '#1890ff' }}>
              {log.result_message || 'Task is running...'}
            </Text>
          ) : log.status === 'cancelled' ? (
            <Text style={{ fontSize: '11px', color: '#8c8c8c' }}>
              {log.error_message || log.result_message || 'Task was cancelled'}
            </Text>
          ) : log.status === 'timeout' ? (
            <Text style={{ fontSize: '11px', color: '#fa8c16' }}>
              {log.error_message || 'Execution timeout'}
            </Text>
          ) : (
            <Text style={{ fontSize: '11px', color: '#ff4d4f' }}>
              {log.error_message || 'Execution failed'}
            </Text>
          )}
        </div>
      )
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (log: ExecutionLog) => (
        <Button
          type="text"
          size="small"
          icon={<InfoCircleOutlined />}
          onClick={() => handleViewDetail(log)}
        >
          Details
        </Button>
      )
    }
  ];

  return (
    <div>
      {/* Filters */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Row gutter={[16, 16]} align="middle">
          {showTaskFilter && (
            <Col xs={24} sm={8}>
              <Space>
                <Text strong>Task:</Text>
                <Select
                  placeholder="Select task"
                  style={{ width: 200 }}
                  allowClear
                  value={selectedTaskId}
                  onChange={setSelectedTaskId}
                  loading={loading}
                >
                  <Option value="all">All Tasks</Option>
                  {tasks.map((task) => (
                    <Option key={task.id} value={task.id}>
                      {task.name}
                    </Option>
                  ))}
                </Select>
              </Space>
            </Col>
          )}
          
          <Col xs={24} sm={8}>
            <Space>
              <Text strong>Status:</Text>
              <Select
                value={statusFilter}
                onChange={setStatusFilter}
                style={{ width: 100 }}
              >
                <Option value="all">All</Option>
                <Option value="success">Success</Option>
                <Option value="failed">Failed</Option>
                <Option value="error">Error</Option>
                <Option value="running">Running</Option>
              </Select>
            </Space>
          </Col>
          
          <Col xs={24} sm={8}>
            <Space>
              <Button
                icon={<ReloadOutlined />}
                onClick={loadExecutionLogs}
                loading={loading}
                size="small"
              >
                Refresh
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* Statistics cards */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="Total Executions"
              value={stats.total}
              prefix={<ClockCircleOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="Success Count"
              value={stats.success}
              prefix={<CheckCircleOutlined />}
              valueStyle={{ color: '#52c41a' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="Failure Count"
              value={stats.failed}
              prefix={<ExclamationCircleOutlined />}
              valueStyle={{ color: '#ff4d4f' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="Success Rate"
              value={stats.successRate.toFixed(1)}
              suffix="%"
              prefix={<TrophyOutlined />}
              valueStyle={{ 
                color: stats.successRate >= 90 ? '#52c41a' : 
                       stats.successRate >= 70 ? '#faad14' : '#ff4d4f' 
              }}
            />
          </Card>
        </Col>
      </Row>

      {/* Success rate progress bar */}
      {stats.total > 0 && (
        <Card size="small" style={{ marginBottom: 16 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
            <Text strong>Execution Success Rate:</Text>
            <Progress
              percent={stats.successRate}
              status={stats.successRate >= 90 ? 'success' : stats.successRate >= 70 ? 'active' : 'exception'}
              strokeColor={stats.successRate >= 90 ? '#52c41a' : stats.successRate >= 70 ? '#faad14' : '#ff4d4f'}
              style={{ flex: 1 }}
            />
            <Text style={{ fontSize: '12px' }}>
              Average Duration: {formatDuration(stats.avgDuration)}
            </Text>
          </div>
        </Card>
      )}

      {/* Execution logs table */}
      <Card 
        title={
          <Space>
            <BarChartOutlined />
            Task Execution History
            {selectedTaskId && (
              <Tag color="blue">
                Task filtering enabled
              </Tag>
            )}
          </Space>
        }
        extra={
          <Space>
            <Text type="secondary" style={{ fontSize: '12px' }}>
              Total {logs.length} records
            </Text>
            <Button
              icon={<DownloadOutlined />}
              onClick={handleDownloadCSV}
              size="small"
              type="primary"
            >
              Download CSV
            </Button>
            <Button
              icon={<DeleteOutlined />}
              onClick={handleClearLogs}
              size="small"
              danger
            >
              Clear
            </Button>
          </Space>
        }
      >
        {logs.length > 0 ? (
          <Table
            columns={columns}
            dataSource={logs}
            rowKey="id"
            loading={loading}
            pagination={{
              pageSize: 10,
              showSizeChanger: true,
              showQuickJumper: true,
              showTotal: (total, range) => `${range[0]}-${range[1]} records, total ${total}`
            }}
            scroll={{ y: height - 200 }}
            size="small"
          />
        ) : (
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description={
              <Space direction="vertical">
                <Text type="secondary">No execution records</Text>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  {selectedTaskId ? 'No execution records for this task' : 'Please select a task or adjust filter conditions'}
                </Text>
              </Space>
            }
          />
        )}
      </Card>

      {/* Details modal */}
      <Modal
        title={
          <Space>
            <InfoCircleOutlined />
            Execution Details
          </Space>
        }
        open={detailVisible}
        onCancel={() => setDetailVisible(false)}
        footer={null}
        width={700}
      >
        {selectedLog && (
          <Descriptions column={1} bordered size="small">
            <Descriptions.Item label="Task Name">
              <Space>
                {getTaskTypeIcon(selectedLog.task_type)}
                {selectedLog.task_name}
              </Space>
            </Descriptions.Item>
            <Descriptions.Item label="Task Type">
              <Tag color="blue">
                {selectedLog.task_type.replace('_', ' ').toUpperCase()}
              </Tag>
            </Descriptions.Item>
            <Descriptions.Item label="Execution Status">
              {getStatusIcon(selectedLog.status)}
            </Descriptions.Item>
            <Descriptions.Item label="Start Time">
              {dayjs(selectedLog.started_at).format('YYYY-MM-DD HH:mm:ss')}
            </Descriptions.Item>
            <Descriptions.Item label="End Time">
              {selectedLog.finished_at 
                ? dayjs(selectedLog.finished_at).format('YYYY-MM-DD HH:mm:ss')
                : '-'
              }
            </Descriptions.Item>
            <Descriptions.Item label="Execution Duration">
              {formatDuration(selectedLog.duration_ms)}
            </Descriptions.Item>
            {selectedLog.result_message && (
              <Descriptions.Item label="Execution Result">
                <Text style={{ color: '#52c41a' }}>
                  {selectedLog.result_message}
                </Text>
              </Descriptions.Item>
            )}
            {selectedLog.result_data && (
              <Descriptions.Item label="Result Data">
                <pre style={{ fontSize: '11px', margin: 0, whiteSpace: 'pre-wrap' }}>
                  {JSON.stringify(selectedLog.result_data, null, 2)}
                </pre>
              </Descriptions.Item>
            )}
            {selectedLog.error_message && (
              <Descriptions.Item label="Error Message">
                <Text style={{ color: '#ff4d4f' }}>
                  {selectedLog.error_message}
                </Text>
              </Descriptions.Item>
            )}
            {selectedLog.error_traceback && (
              <Descriptions.Item label="Error Stack">
                <pre style={{ fontSize: '10px', margin: 0, whiteSpace: 'pre-wrap', color: '#ff4d4f' }}>
                  {selectedLog.error_traceback}
                </pre>
              </Descriptions.Item>
            )}
            {selectedLog.execution_params && (
              <Descriptions.Item label="Execution Parameters">
                <pre style={{ fontSize: '11px', margin: 0, whiteSpace: 'pre-wrap' }}>
                  {JSON.stringify(selectedLog.execution_params, null, 2)}
                </pre>
              </Descriptions.Item>
            )}
          </Descriptions>
        )}
      </Modal>
    </div>
  );
};

export default TaskExecutionHistory;

