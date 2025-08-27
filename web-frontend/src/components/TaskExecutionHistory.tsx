import React, { useState, useEffect } from 'react';
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

// 使用从scheduler service导入的ExecutionLog类型

interface TaskExecutionHistoryProps {
  taskId?: string;
  showTaskFilter?: boolean;
  height?: number;
}

const TaskExecutionHistory: React.FC<TaskExecutionHistoryProps> = ({
  taskId,
  showTaskFilter = true,
  height = 600
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

  useEffect(() => {
    loadExecutionLogs();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedTaskId, dateRange, statusFilter]);

  useEffect(() => {
    loadTasks();
  }, []);

  const loadTasks = async () => {
    try {
      const response = await schedulerAPI.getTasks();
      if (response.success && response.data) {
        setTasks(response.data);
      } else {
        console.error('获取任务列表失败:', response.message);
        message.error('获取任务列表失败');
      }
    } catch (error) {
      console.error('获取任务列表失败:', error);
      message.error('获取任务列表失败');
    }
  };

  const loadExecutionLogs = async () => {
    try {
      setLoading(true);
      
      let response;
      if (selectedTaskId && selectedTaskId !== 'all') {
        // 获取特定任务的执行日志
        response = await schedulerAPI.getTaskLogs(selectedTaskId, 50, 0);
      } else {
        // 获取所有任务的执行日志
        response = await schedulerAPI.getAllExecutionLogs(50, 0);
      }

      if (response.success && response.data) {
        let filteredLogs = response.data.logs;
        
        // 根据状态过滤
        if (statusFilter && statusFilter !== 'all') {
          filteredLogs = filteredLogs.filter((log: ExecutionLog) => log.status === statusFilter);
        }

        // 根据日期范围过滤
        if (dateRange && dateRange.length === 2) {
          const [startDate, endDate] = dateRange;
          filteredLogs = filteredLogs.filter((log: ExecutionLog) => {
            const logDate = dayjs(log.started_at);
            return logDate.isAfter(startDate.startOf('day')) && logDate.isBefore(endDate.endOf('day'));
          });
        }

        setLogs(filteredLogs);
      } else {
        message.error(response.message || '获取执行历史失败');
        setLogs([]);
      }
      
    } catch (error) {
      console.error('加载执行日志失败:', error);
      message.error('加载执行日志失败');
      setLogs([]);
    } finally {
      setLoading(false);
    }
  };

  const getStatusTag = (status: string) => {
    const statusConfig = {
      success: { color: 'success', icon: <CheckCircleOutlined />, text: '成功' },
      failed: { color: 'error', icon: <ExclamationCircleOutlined />, text: '失败' },
      error: { color: 'error', icon: <ExclamationCircleOutlined />, text: '错误' },
      running: { color: 'processing', icon: <PlayCircleOutlined />, text: '运行中' }
    };
    
    const config = statusConfig[status as keyof typeof statusConfig] || statusConfig.error;
    return (
      <Tag color={config.color} icon={config.icon}>
        {config.text}
      </Tag>
    );
  };

  const formatDuration = (durationMs?: number) => {
    if (!durationMs) return '-';
    const seconds = Math.floor(durationMs / 1000);
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    
    if (minutes > 0) {
      return `${minutes}分${remainingSeconds}秒`;
    }
    return `${remainingSeconds}秒`;
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
      console.log('开始下载CSV，任务ID:', selectedTaskId);
      message.loading('正在生成CSV文件...', 0);
      await schedulerAPI.downloadExecutionLogsCSV(selectedTaskId, 1000, 0);
      message.destroy();
      message.success('CSV文件下载成功');
    } catch (error: any) {
      message.destroy();
      console.error('下载CSV失败:', error);
      console.error('错误详情:', {
        message: error?.message,
        response: error?.response,
        status: error?.response?.status,
        data: error?.response?.data
      });
      message.error(`下载CSV失败: ${error?.response?.data?.detail || error?.message}`);
    }
  };

  const handleClearLogs = async () => {
    try {
      // 显示确认对话框
      const confirmed = await new Promise<boolean>((resolve) => {
        Modal.confirm({
          title: '确认清空',
          content: '确定要清空所有任务执行日志吗？此操作不可恢复！',
          okText: '确认清空',
          cancelText: '取消',
          okType: 'danger',
          onOk: () => resolve(true),
          onCancel: () => resolve(false),
        });
      });

      if (!confirmed) {
        return;
      }

      message.loading('正在清空执行日志...', 0);
      const result = await schedulerAPI.clearExecutionLogs();
      message.destroy();

      if (result.success) {
        message.success('执行日志已清空');
        // 重新加载日志数据
        await loadExecutionLogs();
      } else {
        message.error(`清空失败: ${result.message}`);
      }
    } catch (error: any) {
      message.destroy();
      console.error('清空日志失败:', error);
      message.error(`清空日志失败: ${error?.message || '未知错误'}`);
    }
  };

  // 统计数据
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
      title: '任务信息',
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
      title: '执行时间',
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
      title: '耗时',
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
      title: '状态',
      dataIndex: 'status',
      key: 'status',
      render: (status: string) => getStatusTag(status),
      filters: [
        { text: '成功', value: 'success' },
        { text: '失败', value: 'failed' },
        { text: '错误', value: 'error' },
        { text: '运行中', value: 'running' }
      ],
      onFilter: (value: any, record: ExecutionLog) => record.status === value
    },
    {
      title: '结果',
      key: 'result',
      render: (log: ExecutionLog) => (
        <div style={{ maxWidth: 200 }}>
          {log.status === 'success' ? (
            <Text style={{ fontSize: '11px', color: '#52c41a' }}>
              {log.result_message || '执行成功'}
            </Text>
          ) : (
            <Text style={{ fontSize: '11px', color: '#ff4d4f' }}>
              {log.error_message || '执行失败'}
            </Text>
          )}
        </div>
      )
    },
    {
      title: '操作',
      key: 'actions',
      render: (log: ExecutionLog) => (
        <Button
          type="text"
          size="small"
          icon={<InfoCircleOutlined />}
          onClick={() => handleViewDetail(log)}
        >
          详情
        </Button>
      )
    }
  ];

  return (
    <div>
      {/* 过滤器 */}
      <Card size="small" style={{ marginBottom: 16 }}>
        <Row gutter={[16, 16]} align="middle">
          {showTaskFilter && (
            <Col xs={24} sm={8}>
              <Space>
                <Text strong>任务:</Text>
                <Select
                  placeholder="选择任务"
                  style={{ width: 200 }}
                  allowClear
                  value={selectedTaskId}
                  onChange={setSelectedTaskId}
                  loading={loading}
                >
                  <Option value="all">所有任务</Option>
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
              <Text strong>状态:</Text>
              <Select
                value={statusFilter}
                onChange={setStatusFilter}
                style={{ width: 100 }}
              >
                <Option value="all">全部</Option>
                <Option value="success">成功</Option>
                <Option value="failed">失败</Option>
                <Option value="error">错误</Option>
                <Option value="running">运行中</Option>
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
                刷新
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* 统计卡片 */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="总执行次数"
              value={stats.total}
              prefix={<ClockCircleOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="成功次数"
              value={stats.success}
              prefix={<CheckCircleOutlined />}
              valueStyle={{ color: '#52c41a' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="失败次数"
              value={stats.failed}
              prefix={<ExclamationCircleOutlined />}
              valueStyle={{ color: '#ff4d4f' }}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="成功率"
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

      {/* 成功率进度条 */}
      {stats.total > 0 && (
        <Card size="small" style={{ marginBottom: 16 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
            <Text strong>执行成功率:</Text>
            <Progress
              percent={stats.successRate}
              status={stats.successRate >= 90 ? 'success' : stats.successRate >= 70 ? 'active' : 'exception'}
              strokeColor={stats.successRate >= 90 ? '#52c41a' : stats.successRate >= 70 ? '#faad14' : '#ff4d4f'}
              style={{ flex: 1 }}
            />
            <Text style={{ fontSize: '12px' }}>
              平均耗时: {formatDuration(stats.avgDuration)}
            </Text>
          </div>
        </Card>
      )}

      {/* 执行日志表格 */}
      <Card 
        title={
          <Space>
            <BarChartOutlined />
            任务执行历史
            {selectedTaskId && (
              <Tag color="blue">
                任务过滤已启用
              </Tag>
            )}
          </Space>
        }
        extra={
          <Space>
            <Text type="secondary" style={{ fontSize: '12px' }}>
              共 {logs.length} 条记录
            </Text>
            <Button
              icon={<DownloadOutlined />}
              onClick={handleDownloadCSV}
              size="small"
              type="primary"
            >
              下载CSV
            </Button>
            <Button
              icon={<DeleteOutlined />}
              onClick={handleClearLogs}
              size="small"
              danger
            >
              清空
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
              showTotal: (total, range) => `${range[0]}-${range[1]} 条记录，共 ${total} 条`
            }}
            scroll={{ y: height - 200 }}
            size="small"
          />
        ) : (
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description={
              <Space direction="vertical">
                <Text type="secondary">暂无执行记录</Text>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  {selectedTaskId ? '该任务暂无执行记录' : '请选择任务或调整过滤条件'}
                </Text>
              </Space>
            }
          />
        )}
      </Card>

      {/* 详情模态框 */}
      <Modal
        title={
          <Space>
            <InfoCircleOutlined />
            执行详情
          </Space>
        }
        open={detailVisible}
        onCancel={() => setDetailVisible(false)}
        footer={null}
        width={700}
      >
        {selectedLog && (
          <Descriptions column={1} bordered size="small">
            <Descriptions.Item label="任务名称">
              <Space>
                {getTaskTypeIcon(selectedLog.task_type)}
                {selectedLog.task_name}
              </Space>
            </Descriptions.Item>
            <Descriptions.Item label="任务类型">
              <Tag color="blue">
                {selectedLog.task_type.replace('_', ' ').toUpperCase()}
              </Tag>
            </Descriptions.Item>
            <Descriptions.Item label="执行状态">
              {getStatusTag(selectedLog.status)}
            </Descriptions.Item>
            <Descriptions.Item label="开始时间">
              {dayjs(selectedLog.started_at).format('YYYY-MM-DD HH:mm:ss')}
            </Descriptions.Item>
            <Descriptions.Item label="结束时间">
              {selectedLog.finished_at 
                ? dayjs(selectedLog.finished_at).format('YYYY-MM-DD HH:mm:ss')
                : '-'
              }
            </Descriptions.Item>
            <Descriptions.Item label="执行耗时">
              {formatDuration(selectedLog.duration_ms)}
            </Descriptions.Item>
            {selectedLog.result_message && (
              <Descriptions.Item label="执行结果">
                <Text style={{ color: '#52c41a' }}>
                  {selectedLog.result_message}
                </Text>
              </Descriptions.Item>
            )}
            {selectedLog.result_data && (
              <Descriptions.Item label="结果数据">
                <pre style={{ fontSize: '11px', margin: 0, whiteSpace: 'pre-wrap' }}>
                  {JSON.stringify(selectedLog.result_data, null, 2)}
                </pre>
              </Descriptions.Item>
            )}
            {selectedLog.error_message && (
              <Descriptions.Item label="错误信息">
                <Text style={{ color: '#ff4d4f' }}>
                  {selectedLog.error_message}
                </Text>
              </Descriptions.Item>
            )}
            {selectedLog.error_traceback && (
              <Descriptions.Item label="错误堆栈">
                <pre style={{ fontSize: '10px', margin: 0, whiteSpace: 'pre-wrap', color: '#ff4d4f' }}>
                  {selectedLog.error_traceback}
                </pre>
              </Descriptions.Item>
            )}
            {selectedLog.execution_params && (
              <Descriptions.Item label="执行参数">
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

