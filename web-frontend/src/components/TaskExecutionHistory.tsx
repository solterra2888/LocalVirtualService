import React, { useState, useEffect, useCallback, useMemo } from 'react';
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
  Empty,
  message,
  Modal,
  Descriptions,
  Radio,
  Skeleton
} from 'antd';
import {
  ClockCircleOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
  CalendarOutlined,
  BarChartOutlined,
  InfoCircleOutlined,
  PlayCircleOutlined,
  TrophyOutlined,
  DownloadOutlined,
  DeleteOutlined,
  LineChartOutlined
} from '@ant-design/icons';
import {
  ResponsiveContainer,
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip as RechartsTooltip,
  Legend,
  BarChart,
  Bar
} from 'recharts';
import dayjs, { Dayjs } from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import {
  schedulerAPI,
  ExecutionLog,
  ExecutionSummaryData,
  ExecutionSummaryDailyStat
} from '../services/scheduler';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { getModalStyles } from '../utils/modalStyles';
import { aiTagChipStyle } from './shared/aiTagChipStyles';
import DatabaseTableViewer from './DatabaseTableViewer';

dayjs.extend(relativeTime);

const { Text } = Typography;

/** Daily statistics 表：每个单元格 success / failure / total_runs */
type OverviewSftCell = { success: number; failure: number; total: number };

interface ExecutionOverviewTableRow {
  key: string;
  taskLabel: string;
  cells: Record<string, OverviewSftCell>;
}

const metricLabelMap: Record<'success_count' | 'total_runs' | 'failure_count', string> = {
  success_count: 'Success Count',
  total_runs: 'Total Runs',
  failure_count: 'Failure Count'
};

const colorPalette = [
  '#1890ff',
  '#52c41a',
  '#fa8c16',
  '#722ed1',
  '#13c2c2',
  '#eb2f96',
  '#fa541c',
  '#2f54eb',
  '#a0d911',
  '#9254de'
];

// Use ExecutionLog type imported from scheduler service

interface TaskExecutionHistoryProps {
  height?: number;
  refreshTrigger?: number; // Add refresh trigger prop
}

const TaskExecutionHistory: React.FC<TaskExecutionHistoryProps> = ({
  height = 600,
  refreshTrigger
}) => {
  const { themeMode } = useDashboardSettings();
  const [logs, setLogs] = useState<ExecutionLog[]>([]);
  const [loading, setLoading] = useState(false);
  const [dateRange] = useState<[Dayjs, Dayjs]>([
    dayjs().subtract(7, 'day'),
    dayjs()
  ]);
  const [statusFilter, setStatusFilter] = useState<string>('all');
  const [selectedLog, setSelectedLog] = useState<ExecutionLog | null>(null);
  const [detailVisible, setDetailVisible] = useState(false);
  const [summaryData, setSummaryData] = useState<ExecutionSummaryData | null>(null);
  const [summaryLoading, setSummaryLoading] = useState(false);
  const [summaryRange, setSummaryRange] = useState<'7d' | '30d'>('7d');
  const [chartType, setChartType] = useState<'line' | 'bar'>('line');
  const [chartMetric, setChartMetric] = useState<'success_count' | 'total_runs' | 'failure_count'>('success_count');
  const [selectedSummaryTasks, setSelectedSummaryTasks] = useState<string[]>([]);

  const taskColorMap = useMemo(() => {
    if (!summaryData) return {};
    const map: Record<string, string> = {};
    summaryData.tasks.forEach((task, index) => {
      map[task.task_type] = colorPalette[index % colorPalette.length];
    });
    return map;
  }, [summaryData]);

  const summaryTaskOptions = useMemo(() => {
    if (!summaryData) return [];
    return summaryData.tasks.map((task) => ({
      label: task.task_name || task.task_type,
      value: task.task_type
    }));
  }, [summaryData]);

  const selectedSummaryTaskData = useMemo(() => {
    if (!summaryData || selectedSummaryTasks.length === 0) return [];
    return summaryData.tasks.filter((task) => selectedSummaryTasks.includes(task.task_type));
  }, [summaryData, selectedSummaryTasks]);

  const aggregatedSummaryTotals = useMemo(() => {
    if (!summaryData) return null;
    const totals = summaryData.tasks.reduce(
      (acc, task) => {
        acc.totalRuns += task.totals?.total_runs || 0;
        acc.success += task.totals?.success_count || 0;
        acc.failure += task.totals?.failure_count || 0;
        return acc;
      },
      { totalRuns: 0, success: 0, failure: 0 }
    );
    const successRate = totals.totalRuns > 0 ? (totals.success / totals.totalRuns) * 100 : 0;
    return {
      ...totals,
      successRate
    };
  }, [summaryData]);

  const { chartData, hasSummaryChartData } = useMemo(() => {
    if (!summaryData || selectedSummaryTasks.length === 0) {
      return { chartData: [], hasSummaryChartData: false };
    }

    const dateMap: Record<string, any> = {};
    const selectedSet = new Set(selectedSummaryTasks);

    summaryData.tasks.forEach((task) => {
      if (!selectedSet.has(task.task_type)) {
        return;
      }
      task.daily_stats.forEach((stat) => {
        if (!dateMap[stat.date]) {
          dateMap[stat.date] = {
            date: stat.date,
            dateLabel: dayjs(stat.date).format('MM-DD'),
            meta: {}
          };
        }
        const metricValue =
          chartMetric === 'total_runs'
            ? stat.total_runs
            : chartMetric === 'failure_count'
            ? stat.failure_count
            : stat.success_count;
        dateMap[stat.date][task.task_type] = metricValue;
        dateMap[stat.date].meta[task.task_type] = stat;
      });
    });

    const sorted = Object.values(dateMap).sort(
      (a, b) => dayjs(a.date).valueOf() - dayjs(b.date).valueOf()
    );

    return {
      chartData: sorted,
      hasSummaryChartData: sorted.length > 0
    };
  }, [summaryData, selectedSummaryTasks, chartMetric]);

  /** 最近 3 个日历日：行 = 任务，列 = 日期（与原表行列互换） */
  const recentOverviewDays = useMemo(() => {
    if (!hasSummaryChartData || chartData.length === 0) return [];
    return chartData.slice(-3) as Record<string, any>[];
  }, [chartData, hasSummaryChartData]);

  const executionOverviewTableRows = useMemo((): ExecutionOverviewTableRow[] => {
    if (!hasSummaryChartData || selectedSummaryTaskData.length === 0) return [];
    return selectedSummaryTaskData.map((task) => {
      const cells: Record<string, OverviewSftCell> = {};
      recentOverviewDays.forEach((dayRow: Record<string, any>) => {
        const dateKey = String(dayRow.date);
        const stat = dayRow.meta?.[task.task_type] as ExecutionSummaryDailyStat | undefined;
        cells[dateKey] = {
          success: stat?.success_count ?? 0,
          failure: stat?.failure_count ?? 0,
          total: stat?.total_runs ?? 0
        };
      });
      return {
        key: task.task_type,
        taskLabel: task.task_name || task.task_type,
        cells
      };
    });
  }, [hasSummaryChartData, selectedSummaryTaskData, recentOverviewDays]);

  const executionOverviewTableColumns = useMemo(() => {
    const taskCol = {
      title: 'Task',
      dataIndex: 'taskLabel',
      key: 'taskLabel',
      fixed: 'left' as const,
      width: 200,
      ellipsis: true
    };

    const dateCols = recentOverviewDays.map((dayRow: Record<string, any>) => {
      const dateKey = String(dayRow.date);
      return {
        title: (
          <span style={{ lineHeight: 1.35 }}>
            {dayjs(dayRow.date).format('YYYY-MM-DD')}
            <br />
            <Text type="secondary" style={{ fontSize: 12, fontWeight: 400 }}>
              success / failure / total
            </Text>
          </span>
        ),
        key: dateKey,
        align: 'right' as const,
        width: 150,
        render: (_: unknown, record: ExecutionOverviewTableRow) => {
          const c = record.cells[dateKey];
          if (!c) return '0 / 0 / 0';
          return `${c.success} / ${c.failure} / ${c.total}`;
        },
        sorter: (a: ExecutionOverviewTableRow, b: ExecutionOverviewTableRow) =>
          (a.cells[dateKey]?.total ?? 0) - (b.cells[dateKey]?.total ?? 0)
      };
    });

    return [taskCol, ...dateCols];
  }, [recentOverviewDays]);

  const renderChartTooltip = useCallback(
    ({ active, payload }: any) => {
      if (!active || !payload || payload.length === 0) {
        return null;
      }

      const dataPoint = payload[0].payload;
      const meta = dataPoint.meta || {};
      const formattedDate = dayjs(dataPoint.date).format('YYYY-MM-DD');

      return (
        <div
          style={{
            background: '#fff',
            padding: 12,
            borderRadius: 8,
            boxShadow: '0 4px 12px rgba(0,0,0,0.15)',
            minWidth: 200
          }}
        >
          <div style={{ fontWeight: 600, marginBottom: 8 }}>{formattedDate}</div>
          <div style={{ fontSize: 12, color: '#8c8c8c', marginBottom: 8 }}>
            Metric: {metricLabelMap[chartMetric]}
          </div>
          {payload.map((item: any) => {
            const taskKey = item.dataKey;
            const taskMeta = meta[taskKey];
            return (
              <div key={taskKey} style={{ marginBottom: 8 }}>
                <div
                  style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    fontWeight: 500,
                    color: item.color || '#333'
                  }}
                >
                  <span>{item.name}</span>
                  <span>{item.value ?? 0}</span>
                </div>
                {taskMeta && (
                  <div style={{ fontSize: 11, color: '#595959', marginTop: 4 }}>
                    <div>Total Runs: {taskMeta.total_runs}</div>
                    <div>Success: {taskMeta.success_count}</div>
                    {taskMeta.failure_count > 0 && (
                      <div style={{ color: '#ff4d4f' }}>Failure: {taskMeta.failure_count}</div>
                    )}
                    {taskMeta.skipped_count > 0 && (
                      <div style={{ color: '#8c8c8c' }}>Skipped (dup): {taskMeta.skipped_count}</div>
                    )}
                    {typeof taskMeta.success_rate === 'number' && (
                      <div>Success Rate: {(taskMeta.success_rate * 100).toFixed(1)}%</div>
                    )}
                    {typeof taskMeta.avg_duration_ms === 'number' && (
                      <div>
                        Avg Duration: {formatDuration(Math.round(taskMeta.avg_duration_ms))}
                      </div>
                    )}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      );
    },
    [chartMetric]
  );

  const loadExecutionSummary = useCallback(
    async (range: '7d' | '30d') => {
      try {
        setSummaryLoading(true);
        const days = range === '30d' ? 30 : 7;
        const browserTimezone =
          typeof Intl !== 'undefined' && Intl.DateTimeFormat
            ? Intl.DateTimeFormat().resolvedOptions().timeZone || 'America/New_York'
            : 'America/New_York';

        const response = await schedulerAPI.getExecutionSummary(days, browserTimezone);
        if (response.success && response.data) {
          const tasksData = (response.data.tasks || []).filter(
            (task) => task.task_type !== 'futu_themes'
          );
          const filteredSummary = {
            ...response.data,
            tasks: tasksData
          };
          setSummaryData(filteredSummary);
          setSelectedSummaryTasks((prev) => {
            const retained = prev.filter((taskType) =>
              tasksData.some((task) => task.task_type === taskType)
            );
            if (retained.length > 0) {
              return retained;
            }
            if (tasksData.length === 0) {
              return [];
            }
            return filteredSummary.tasks.map((task) => task.task_type);
          });
        } else {
          setSummaryData(null);
          message.error(response.message || 'Failed to get execution summary');
        }
      } catch (error) {
        console.error('Failed to load execution summary:', error);
        setSummaryData(null);
        message.error('Failed to load execution summary');
      } finally {
        setSummaryLoading(false);
      }
    },
    []
  );

  const loadExecutionLogs = useCallback(async () => {
    try {
      setLoading(true);
      
      // Prepare task types for filtering (exclude futu_themes)
      let taskTypesToFetch = selectedSummaryTasks.length > 0 
        ? selectedSummaryTasks.filter((type) => type !== 'futu_themes')
        : undefined;
      
      // Get execution logs with task type filtering at backend
      // Increase limit to 500 to get more records when filtering by task type
      const response = await schedulerAPI.getAllExecutionLogs(500, 0, taskTypesToFetch);

      if (response.success && response.data) {
        let filteredLogs = response.data.logs.filter(
          (log: ExecutionLog) => log.task_type !== 'futu_themes'
        );
        
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
  }, [selectedSummaryTasks, statusFilter, dateRange]);

  useEffect(() => {
    loadExecutionLogs();
  }, [selectedSummaryTasks, dateRange, statusFilter, loadExecutionLogs]);

  useEffect(() => {
    loadExecutionSummary(summaryRange);
  }, [summaryRange, loadExecutionSummary]);

  // Refresh data when refreshTrigger changes
  useEffect(() => {
    if (refreshTrigger !== undefined) {
      loadExecutionLogs();
      loadExecutionSummary(summaryRange);
    }
  }, [refreshTrigger, loadExecutionLogs, loadExecutionSummary, summaryRange]);

  const getStatusIcon = (status: string) => {
    const statusConfig = {
      success: { color: '#00b96b', icon: <CheckCircleOutlined /> },
      failed: { color: '#ff7875', icon: <ExclamationCircleOutlined /> },
      error: { color: '#ff7875', icon: <ExclamationCircleOutlined /> },
      running: { color: '#40a9ff', icon: <PlayCircleOutlined /> },
      timeout: { color: '#fa8c16', icon: <ClockCircleOutlined /> },
      cancelled: { color: '#8c8c8c', icon: <InfoCircleOutlined /> },
      skipped: { color: '#bfbfbf', icon: <InfoCircleOutlined /> }
    };
    
    const config = statusConfig[status as keyof typeof statusConfig] || statusConfig.error;
    return (
      <span style={{ color: config.color, fontSize: '12px' }}>
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
      console.log('Starting CSV download, selected tasks:', selectedSummaryTasks);
      message.loading('Generating CSV file...', 0);
      // Download all tasks if none selected, otherwise download first selected task
      const taskId = selectedSummaryTasks.length > 0 ? selectedSummaryTasks[0] : undefined;
      await schedulerAPI.downloadExecutionLogsCSV(taskId, 1000, 0);
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

  const columns = [
    {
      title: 'Scheduler',
      key: 'scheduler',
      fixed: 'left' as const,
      width: 220,
      ellipsis: true,
      render: (_: unknown, log: ExecutionLog) => {
        const name = log.task_name || log.task_id;
        return (
          <Text strong ellipsis={{ tooltip: name }} style={{ fontSize: '12px' }}>
            {name}
          </Text>
        );
      },
      sorter: (a: ExecutionLog, b: ExecutionLog) =>
        (a.task_name || '').localeCompare(b.task_name || '', undefined, { sensitivity: 'base' })
    },
    {
      title: 'Start Time',
      dataIndex: 'started_at',
      key: 'started_at',
      width: 160,
      render: (startedAt: string) => (
        <Text style={{ fontSize: '12px' }}>
          {startedAt ? dayjs(startedAt).format('YYYY-MM-DD HH:mm:ss') : '-'}
        </Text>
      ),
      sorter: (a: ExecutionLog, b: ExecutionLog) =>
        dayjs(a.started_at).valueOf() - dayjs(b.started_at).valueOf()
    },
    {
      title: 'Duration',
      dataIndex: 'duration_ms',
      key: 'duration',
      render: (duration: number) => (
        <Text style={{ fontSize: '12px' }}>
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
        { text: 'Cancelled', value: 'cancelled' },
        { text: 'Skipped (dup)', value: 'skipped' }
      ],
      onFilter: (value: any, record: ExecutionLog) => record.status === value
    },
    {
      title: 'Details',
      key: 'details',
      render: (log: ExecutionLog) => (
        <Button
          type="text"
          size="small"
          icon={<InfoCircleOutlined />}
          onClick={() => handleViewDetail(log)}
          style={{ fontSize: 12 }}
        >
          Details
        </Button>
      )
    }
  ];

  return (
    <div>
      <Card
        size="small"
        style={{ marginBottom: 16 }}
        bodyStyle={{ overflow: 'visible' }}
        title={
          <Space>
            {chartType === 'line' ? <LineChartOutlined /> : <BarChartOutlined />}
            Execution Overview
          </Space>
        }
      >
        <Space wrap size="middle" style={{ marginBottom: 16 }}>
          <Radio.Group
            value={summaryRange}
            onChange={(e) => setSummaryRange(e.target.value as '7d' | '30d')}
            size="small"
          >
            <Radio.Button value="7d">Past 7 Days</Radio.Button>
            <Radio.Button value="30d">Past 30 Days</Radio.Button>
          </Radio.Group>
          <Radio.Group
            value={chartType}
            onChange={(e) => setChartType(e.target.value as 'line' | 'bar')}
            size="small"
          >
            <Radio.Button value="line">
              <LineChartOutlined /> Line
            </Radio.Button>
            <Radio.Button value="bar">
              <BarChartOutlined /> Bar
            </Radio.Button>
          </Radio.Group>
          <Select
            mode="multiple"
            allowClear
            placeholder="Select Tasks"
            value={selectedSummaryTasks}
            onChange={(value) => setSelectedSummaryTasks(value as string[])}
            options={summaryTaskOptions}
            style={{ minWidth: 350 }}
            size="small"
            maxTagCount="responsive"
          />
          <Select
            value={statusFilter}
            onChange={setStatusFilter}
            options={[
              { label: 'All Status', value: 'all' },
              { label: 'Success', value: 'success' },
              { label: 'Failed', value: 'failed' },
              { label: 'Error', value: 'error' },
              { label: 'Running', value: 'running' },
              { label: 'Timeout', value: 'timeout' },
              { label: 'Cancelled', value: 'cancelled' }
            ]}
            style={{ width: 120 }}
            size="small"
          />
          <Select
            value={chartMetric}
            onChange={(value) =>
              setChartMetric(value as 'success_count' | 'total_runs' | 'failure_count')
            }
            options={[
              { label: 'Success Count', value: 'success_count' },
              { label: 'Total Runs', value: 'total_runs' },
              { label: 'Failure Count', value: 'failure_count' }
            ]}
            style={{ width: 160 }}
            size="small"
          />
          {summaryData && (
            <Tag bordered={false} style={aiTagChipStyle('ticker', themeMode)}>
              Timezone: {summaryData.timezone}
            </Tag>
          )}
        </Space>

        {aggregatedSummaryTotals && (
          <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
            <Col xs={12} sm={6}>
              <Statistic
                title="Total Runs"
                value={aggregatedSummaryTotals.totalRuns}
                prefix={<ClockCircleOutlined />}
              />
            </Col>
            <Col xs={12} sm={6}>
              <Statistic
                title="Success"
                value={aggregatedSummaryTotals.success}
                prefix={<CheckCircleOutlined />}
                valueStyle={{ color: '#52c41a' }}
              />
            </Col>
            <Col xs={12} sm={6}>
              <Statistic
                title="Failures"
                value={aggregatedSummaryTotals.failure}
                prefix={<ExclamationCircleOutlined />}
                valueStyle={{ color: '#ff4d4f' }}
              />
            </Col>
            <Col xs={12} sm={6}>
              <Statistic
                title="Success Rate"
                value={aggregatedSummaryTotals.successRate.toFixed(1)}
                suffix="%"
                prefix={<TrophyOutlined />}
                valueStyle={{
                  color:
                    aggregatedSummaryTotals.successRate >= 90
                      ? '#52c41a'
                      : aggregatedSummaryTotals.successRate >= 70
                      ? '#faad14'
                      : '#ff4d4f'
                }}
              />
            </Col>
          </Row>
        )}

        {summaryLoading ? (
          <Skeleton active paragraph={{ rows: 6 }} />
        ) : hasSummaryChartData ? (
          <>
            <div style={{ width: '100%', height: 320 }}>
              <ResponsiveContainer width="100%" height="100%">
                {chartType === 'line' ? (
                  <LineChart data={chartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="dateLabel" />
                    <YAxis
                      allowDecimals={false}
                      label={{
                        value: metricLabelMap[chartMetric],
                        angle: -90,
                        position: 'insideLeft'
                      }}
                    />
                    <RechartsTooltip
                      content={renderChartTooltip}
                      wrapperStyle={{ zIndex: 2000 }}
                      allowEscapeViewBox={{ x: false, y: true }}
                    />
                    <Legend wrapperStyle={{ fontSize: 9 }} />
                    {selectedSummaryTaskData.map((task) => (
                      <Line
                        key={task.task_type}
                        type="monotone"
                        dataKey={task.task_type}
                        name={task.task_name || task.task_type}
                        stroke={taskColorMap[task.task_type] || '#1890ff'}
                        strokeWidth={2}
                        dot={{ r: 2 }}
                        activeDot={{ r: 4 }}
                      />
                    ))}
                  </LineChart>
                ) : (
                  <BarChart data={chartData}>
                    <CartesianGrid strokeDasharray="3 3" />
                    <XAxis dataKey="dateLabel" />
                    <YAxis
                      allowDecimals={false}
                      label={{
                        value: metricLabelMap[chartMetric],
                        angle: -90,
                        position: 'insideLeft'
                      }}
                    />
                    <RechartsTooltip
                      content={renderChartTooltip}
                      wrapperStyle={{ zIndex: 2000 }}
                      allowEscapeViewBox={{ x: true, y: true }}
                    />
                    <Legend wrapperStyle={{ fontSize: 9 }} />
                    {selectedSummaryTaskData.map((task) => (
                      <Bar
                        key={task.task_type}
                        dataKey={task.task_type}
                        name={task.task_name || task.task_type}
                        stackId="runs"
                        fill={taskColorMap[task.task_type] || '#1890ff'}
                        radius={[4, 4, 0, 0]}
                      />
                    ))}
                  </BarChart>
                )}
              </ResponsiveContainer>
            </div>
            <div style={{ marginTop: 16 }}>
              <Text strong style={{ display: 'block', marginBottom: 8 }}>
                Daily statistics — last 3 days (success / failure / total runs per cell)
              </Text>
              <Table
                size="small"
                columns={executionOverviewTableColumns}
                dataSource={executionOverviewTableRows}
                pagination={false}
                scroll={{ x: 'max-content' }}
              />
            </div>
          </>
        ) : (
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description={
              <Space direction="vertical">
                <Text type="secondary">No execution summary data</Text>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  Select schedulers or adjust the time range
                </Text>
              </Space>
            }
          />
        )}
        <div style={{ marginTop: 24 }}>
          <DatabaseTableViewer refreshTrigger={refreshTrigger} />
        </div>
      </Card>

      {/* Execution logs table */}
      <Card 
        title={
          <Space>
            <BarChartOutlined />
            Task Execution History
            {selectedSummaryTasks.length > 0 && (
              <Tag bordered={false} style={aiTagChipStyle('ticker', themeMode)}>
                {selectedSummaryTasks.length} task(s) selected
              </Tag>
            )}
            {statusFilter !== 'all' && (
              <Tag bordered={false} style={aiTagChipStyle('sector', themeMode)}>
                Status: {statusFilter}
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
              type="primary"
              size="small"
              icon={<DownloadOutlined />}
              onClick={handleDownloadCSV}
              title="Download CSV"
              aria-label="Download CSV"
            />
            <Button
              danger
              size="small"
              icon={<DeleteOutlined />}
              onClick={handleClearLogs}
              title="Clear execution logs"
              aria-label="Clear execution logs"
            />
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
            scroll={{ x: 'max-content', y: height - 200 }}
            size="small"
          />
        ) : (
          <Empty
            image={Empty.PRESENTED_IMAGE_SIMPLE}
            description={
              <Space direction="vertical">
                <Text type="secondary">No execution records</Text>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  {selectedSummaryTasks.length > 0 
                    ? 'No execution records for selected tasks' 
                    : 'Please select tasks or adjust filter conditions'}
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
        styles={getModalStyles(themeMode)}
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
              <Tag bordered={false} style={aiTagChipStyle('concept', themeMode)}>
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

