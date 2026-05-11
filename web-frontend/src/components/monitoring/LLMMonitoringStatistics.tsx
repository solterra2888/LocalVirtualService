import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  Button,
  Select,
  message,
  Row,
  Col,
  Typography,
  Space,
  Statistic,
  Spin,
  Tooltip,
  Table,
  Segmented,
  Progress,
  Alert,
  Tag,
} from 'antd';
import { ReloadOutlined, DownloadOutlined } from '@ant-design/icons';
import {
  LineChart,
  Line,
  AreaChart,
  Area,
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip as RechartsTooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from 'recharts';
import { llmConfigService, LLMConfig } from '../../services/llmConfigService';
import { LLMProviderBrandIcon } from '../LLMProviderBrandIcon';

const { Text } = Typography;
const { Option } = Select;

export interface LLMMonitoringStatisticsProps {
  /** When false, no data is fetched and nothing is rendered (keeps mounted state when toggling). */
  active: boolean;
}

/**
 * LLM usage monitoring dashboard (moved from LLMConfigPage "Monitoring Statistics" tab).
 * Loads config and monitoring APIs only while `active` is true.
 */
const LLMMonitoringStatistics: React.FC<LLMMonitoringStatisticsProps> = ({ active }) => {
  const [config, setConfig] = useState<LLMConfig | null>(null);
  const [monitoringTimeRange, setMonitoringTimeRange] = useState<string>('30d');
  const [monitoringProvider, setMonitoringProvider] = useState<string | undefined>(undefined);
  const [monitoringLoading, setMonitoringLoading] = useState<boolean>(false);
  const [dashboardData, setDashboardData] = useState<any>(null);
  const [timeSeriesData, setTimeSeriesData] = useState<any[]>([]);
  const [modelStatsData, setModelStatsData] = useState<any[]>([]);
  const [templateStatsData, setTemplateStatsData] = useState<any[]>([]);
  const [errorAnalysisData, setErrorAnalysisData] = useState<any[]>([]);
  const [detailedLogs, setDetailedLogs] = useState<any[]>([]);
  const [logsTotal, setLogsTotal] = useState<number>(0);
  const [logsPage, setLogsPage] = useState<number>(1);
  const [logsPageSize] = useState<number>(20);

  useEffect(() => {
    if (!active) return;
    let cancelled = false;
    (async () => {
      try {
        const data = await llmConfigService.getConfig();
        if (cancelled) return;
        const configWithStatus: LLMConfig = {
          ...data,
          providers: data.providers.map((p) => ({
            ...p,
            status: 'disconnected' as const,
          })),
        };
        setConfig(configWithStatus);
      } catch (error) {
        console.error('Failed to load LLM config for monitoring:', error);
        message.error('加载 LLM 配置失败');
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [active]);

  const loadMonitoringData = useCallback(async () => {
    if (!config) return;

    setMonitoringLoading(true);
    try {
      const [dashboard, timeSeries, modelStats, templateStats, errorAnalysis, logs] = await Promise.all([
        llmConfigService.getMonitoringDashboard(monitoringTimeRange, monitoringProvider),
        llmConfigService.getTimeSeriesData(monitoringTimeRange, monitoringProvider),
        llmConfigService.getModelStats(monitoringTimeRange, monitoringProvider),
        llmConfigService.getTemplateStats(monitoringTimeRange, monitoringProvider),
        llmConfigService.getErrorAnalysis(monitoringTimeRange, monitoringProvider),
        llmConfigService.getDetailedLogs(
          monitoringTimeRange,
          monitoringProvider,
          undefined,
          undefined,
          logsPageSize,
          (logsPage - 1) * logsPageSize
        ),
      ]);

      setDashboardData(dashboard);
      setTimeSeriesData(timeSeries.data || []);
      setModelStatsData(modelStats.data || []);
      setTemplateStatsData(templateStats.data || []);
      setErrorAnalysisData(errorAnalysis.data || []);
      setDetailedLogs(logs.data || []);
      setLogsTotal(logs.total || 0);
    } catch (error) {
      console.error('Failed to load monitoring data:', error);
      message.error('加载监控数据失败');
    } finally {
      setMonitoringLoading(false);
    }
  }, [config, monitoringTimeRange, monitoringProvider, logsPage, logsPageSize]);

  useEffect(() => {
    if (!active || !config) return;
    loadMonitoringData();
  }, [active, config, loadMonitoringData]);

  if (!active) {
    return null;
  }

  if (!config) {
    return (
      <div style={{ textAlign: 'center', padding: 48 }}>
        <Spin size="large" />
      </div>
    );
  }

  return (
    <Spin spinning={monitoringLoading}>
      <Card style={{ marginBottom: 16 }}>
        <Row gutter={16} align="middle">
          <Col xs={24} sm={8}>
            <Space wrap>
              <Text strong>时间范围:</Text>
              <Segmented
                options={[
                  { label: '24小时', value: '1d' },
                  { label: '7天', value: '7d' },
                  { label: '30天', value: '30d' },
                ]}
                value={monitoringTimeRange}
                onChange={(value) => setMonitoringTimeRange(value as string)}
              />
            </Space>
          </Col>
          <Col xs={24} sm={8}>
            <Space wrap>
              <Text strong>提供商:</Text>
              <Select
                style={{ width: 200, maxWidth: '100%' }}
                placeholder="All Providers"
                allowClear
                value={monitoringProvider}
                onChange={setMonitoringProvider}
                optionLabelProp="label"
              >
                {config.providers.map((p) => {
                  const optLabel = (
                    <Space size={8} align="center">
                      <LLMProviderBrandIcon providerId={p.id} fallbackIcon={p.icon} size={16} />
                      {p.displayName}
                    </Space>
                  );
                  return (
                    <Option key={p.id} value={p.id} label={optLabel}>
                      {optLabel}
                    </Option>
                  );
                })}
              </Select>
            </Space>
          </Col>
          <Col xs={24} sm={8} style={{ textAlign: 'right' }}>
            <Space wrap>
              <Button icon={<ReloadOutlined />} onClick={loadMonitoringData}>
                刷新
              </Button>
              <Button icon={<DownloadOutlined />} type="primary">
                导出报告
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {dashboardData && (
        <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
          <Col xs={24} sm={12} lg={6}>
            <Card>
              <Statistic
                title="总请求数"
                value={dashboardData.summary.total_requests}
                suffix="次"
                valueStyle={{ color: '#1890ff' }}
              />
            </Card>
          </Col>
          <Col xs={24} sm={12} lg={6}>
            <Card>
              <Statistic
                title="平均响应时间"
                value={(dashboardData.summary.avg_response_time / 1000).toFixed(2)}
                suffix="秒"
                valueStyle={{ color: '#52c41a' }}
              />
            </Card>
          </Col>
          <Col xs={24} sm={12} lg={6}>
            <Card>
              <Statistic
                title="成功率"
                value={dashboardData.summary.avg_success_rate.toFixed(1)}
                suffix="%"
                valueStyle={{
                  color: dashboardData.summary.avg_success_rate > 95 ? '#3f8600' : '#ff4d4f',
                }}
              />
            </Card>
          </Col>
          <Col xs={24} sm={12} lg={6}>
            <Card>
              <Statistic
                title="总成本"
                value={dashboardData.summary.total_cost.toFixed(2)}
                suffix={dashboardData.summary.currency}
                valueStyle={{ color: '#fa8c16' }}
              />
            </Card>
          </Col>
        </Row>
      )}

      {timeSeriesData.length > 0 ? (
        <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
          <Col xs={24} lg={12}>
            <Card title="请求趋势" size="small">
              <ResponsiveContainer width="100%" height={250}>
                <AreaChart data={timeSeriesData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="timestamp" tickFormatter={(value) => new Date(value).toLocaleDateString()} />
                  <YAxis />
                  <RechartsTooltip />
                  <Area
                    type="monotone"
                    dataKey="requests"
                    stroke="#1890ff"
                    fill="#1890ff"
                    fillOpacity={0.6}
                    name="请求数"
                  />
                </AreaChart>
              </ResponsiveContainer>
            </Card>
          </Col>
          <Col xs={24} lg={12}>
            <Card title="成功率趋势" size="small">
              <ResponsiveContainer width="100%" height={250}>
                <LineChart data={timeSeriesData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="timestamp" tickFormatter={(value) => new Date(value).toLocaleDateString()} />
                  <YAxis domain={[0, 100]} />
                  <RechartsTooltip />
                  <Line type="monotone" dataKey="success_rate" stroke="#52c41a" strokeWidth={2} name="成功率 (%)" />
                </LineChart>
              </ResponsiveContainer>
            </Card>
          </Col>
          <Col xs={24} lg={12}>
            <Card title="响应时间趋势" size="small">
              <ResponsiveContainer width="100%" height={250}>
                <LineChart data={timeSeriesData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="timestamp" tickFormatter={(value) => new Date(value).toLocaleDateString()} />
                  <YAxis />
                  <RechartsTooltip />
                  <Line
                    type="monotone"
                    dataKey="avg_response_time"
                    stroke="#fa8c16"
                    strokeWidth={2}
                    name="平均响应时间 (ms)"
                  />
                </LineChart>
              </ResponsiveContainer>
            </Card>
          </Col>
          <Col xs={24} lg={12}>
            <Card title="Token使用趋势" size="small">
              <ResponsiveContainer width="100%" height={250}>
                <BarChart data={timeSeriesData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="timestamp" tickFormatter={(value) => new Date(value).toLocaleDateString()} />
                  <YAxis />
                  <RechartsTooltip />
                  <Bar dataKey="total_tokens" fill="#722ed1" name="Token数量" />
                </BarChart>
              </ResponsiveContainer>
            </Card>
          </Col>
        </Row>
      ) : (
        <Alert
          style={{ marginBottom: 24 }}
          message="暂无数据"
          description="当前时间范围内没有使用记录，请使用LLM服务后再查看统计数据"
          type="info"
          showIcon
        />
      )}

      {templateStatsData.length > 0 && (
        <Card title="按模板统计" size="small" style={{ marginBottom: 16 }}>
          <Table
            dataSource={templateStatsData}
            pagination={false}
            size="small"
            rowKey={(record) => `template-${record.template_id}`}
            columns={[
              {
                title: '模板ID',
                dataIndex: 'template_id',
                key: 'template_id',
                width: 100,
              },
              {
                title: '模板名称',
                dataIndex: 'template_name',
                key: 'template_name',
              },
              {
                title: '请求数',
                dataIndex: 'totalRequests',
                key: 'totalRequests',
                sorter: (a, b) => a.totalRequests - b.totalRequests,
                render: (value) => value.toLocaleString(),
              },
              {
                title: '成功率',
                dataIndex: 'successRate',
                key: 'successRate',
                sorter: (a, b) => a.successRate - b.successRate,
                render: (value) => (
                  <Progress
                    percent={value}
                    size="small"
                    status={value > 95 ? 'success' : value > 80 ? 'normal' : 'exception'}
                  />
                ),
              },
              {
                title: '平均响应时间',
                dataIndex: 'avgResponseTime',
                key: 'avgResponseTime',
                sorter: (a, b) => a.avgResponseTime - b.avgResponseTime,
                render: (value) => `${value.toFixed(0)}ms`,
              },
              {
                title: 'Token总数',
                dataIndex: 'totalTokens',
                key: 'totalTokens',
                sorter: (a, b) => a.totalTokens - b.totalTokens,
                render: (value) => value.toLocaleString(),
              },
              {
                title: '总成本',
                dataIndex: 'totalCost',
                key: 'totalCost',
                sorter: (a, b) => a.totalCost - b.totalCost,
                render: (value) => `¥${value.toFixed(2)}`,
              },
            ]}
          />
        </Card>
      )}

      {modelStatsData.length > 0 && (
        <Card title="按模型统计" size="small" style={{ marginBottom: 16 }}>
          <Table
            dataSource={modelStatsData}
            pagination={false}
            size="small"
            rowKey={(record) => `${record.provider}-${record.model}`}
            columns={[
              {
                title: '提供商',
                dataIndex: 'provider',
                key: 'provider',
                render: (text: string) => {
                  const provider = config.providers.find((p) => p.id === text);
                  return (
                    <Space align="center">
                      <LLMProviderBrandIcon providerId={text} fallbackIcon={provider?.icon || '🤖'} size={16} />
                      <span>{provider?.displayName || text}</span>
                    </Space>
                  );
                },
              },
              {
                title: '模型',
                dataIndex: 'model',
                key: 'model',
              },
              {
                title: '请求数',
                dataIndex: 'totalRequests',
                key: 'totalRequests',
                sorter: (a, b) => a.totalRequests - b.totalRequests,
                render: (value) => value.toLocaleString(),
              },
              {
                title: '成功率',
                dataIndex: 'successRate',
                key: 'successRate',
                sorter: (a, b) => a.successRate - b.successRate,
                render: (value) => (
                  <Progress
                    percent={value}
                    size="small"
                    status={value > 95 ? 'success' : value > 80 ? 'normal' : 'exception'}
                  />
                ),
              },
              {
                title: '平均响应时间',
                dataIndex: 'avgResponseTime',
                key: 'avgResponseTime',
                sorter: (a, b) => a.avgResponseTime - b.avgResponseTime,
                render: (value) => `${value.toFixed(0)}ms`,
              },
              {
                title: 'Token总数',
                dataIndex: 'totalTokens',
                key: 'totalTokens',
                sorter: (a, b) => a.totalTokens - b.totalTokens,
                render: (value) => value.toLocaleString(),
              },
              {
                title: '总成本',
                dataIndex: 'totalCost',
                key: 'totalCost',
                sorter: (a, b) => a.totalCost - b.totalCost,
                render: (value) => `¥${value.toFixed(2)}`,
              },
            ]}
          />
        </Card>
      )}

      {errorAnalysisData.length > 0 && (
        <Card title="错误分析" size="small" style={{ marginBottom: 16 }}>
          <Row gutter={[16, 16]}>
            <Col xs={24} lg={12}>
              <Table
                dataSource={errorAnalysisData}
                pagination={false}
                size="small"
                rowKey="error_type"
                columns={[
                  {
                    title: '错误类型',
                    dataIndex: 'error_type',
                    key: 'error_type',
                    render: (text: string) => <Tag color="error">{text}</Tag>,
                  },
                  {
                    title: '次数',
                    dataIndex: 'count',
                    key: 'count',
                    sorter: (a, b) => a.count - b.count,
                  },
                  {
                    title: '占比',
                    dataIndex: 'percentage',
                    key: 'percentage',
                    render: (value) => `${value.toFixed(1)}%`,
                  },
                  {
                    title: '最后发生',
                    dataIndex: 'last_occurrence',
                    key: 'last_occurrence',
                    render: (value) => new Date(value).toLocaleString(),
                  },
                ]}
              />
            </Col>
            <Col xs={24} lg={12}>
              <ResponsiveContainer width="100%" height={250}>
                <PieChart>
                  <Pie
                    data={errorAnalysisData}
                    dataKey="count"
                    nameKey="error_type"
                    cx="50%"
                    cy="50%"
                    outerRadius={80}
                    label={(entry: any) => `${entry.error_type}: ${entry.percentage.toFixed(1)}%`}
                  >
                    {errorAnalysisData.map((entry, index) => (
                      <Cell key={`cell-${entry.error_type}-${index}`} fill={['#ff4d4f', '#ff7a45', '#ffa940'][index % 3]} />
                    ))}
                  </Pie>
                  <RechartsTooltip />
                </PieChart>
              </ResponsiveContainer>
            </Col>
          </Row>
        </Card>
      )}

      {detailedLogs.length > 0 && (
        <Card title="详细日志" size="small" extra={<Text type="secondary">共 {logsTotal} 条记录</Text>}>
          <Table
            dataSource={detailedLogs}
            pagination={{
              current: logsPage,
              pageSize: logsPageSize,
              total: logsTotal,
              onChange: (page) => setLogsPage(page),
              showSizeChanger: false,
            }}
            size="small"
            rowKey="id"
            scroll={{ x: 1200 }}
            columns={[
              {
                title: '时间',
                dataIndex: 'created_at',
                key: 'created_at',
                width: 180,
                render: (value) => new Date(value).toLocaleString(),
              },
              {
                title: '模板ID',
                dataIndex: 'template_id',
                key: 'template_id',
                width: 100,
                render: (value) => value || '-',
              },
              {
                title: '提供商',
                dataIndex: 'provider',
                key: 'provider',
                width: 120,
                render: (text: string) => {
                  const provider = config.providers.find((p) => p.id === text);
                  return (
                    <Space size={6} align="center">
                      <LLMProviderBrandIcon providerId={text} fallbackIcon={provider?.icon || '🤖'} size={14} />
                      <span>{provider?.displayName || text}</span>
                    </Space>
                  );
                },
              },
              {
                title: '模型',
                dataIndex: 'model',
                key: 'model',
                width: 150,
              },
              {
                title: '状态',
                dataIndex: 'success',
                key: 'success',
                width: 100,
                render: (value) => (
                  <Tag color={value ? 'success' : 'error'}>{value ? '成功' : '失败'}</Tag>
                ),
              },
              {
                title: '响应时间',
                dataIndex: 'response_time_ms',
                key: 'response_time_ms',
                width: 100,
                render: (value) => `${value}ms`,
              },
              {
                title: 'Tokens',
                dataIndex: 'tokens_used',
                key: 'tokens_used',
                width: 100,
              },
              {
                title: '成本',
                dataIndex: 'cost',
                key: 'cost',
                width: 100,
                render: (value) => `¥${value?.toFixed(4) || 0}`,
              },
              {
                title: '用户ID',
                dataIndex: 'user_id',
                key: 'user_id',
                width: 100,
                render: (text) => text || '-',
              },
              {
                title: '错误信息',
                dataIndex: 'error_message',
                key: 'error_message',
                render: (text) =>
                  text ? (
                    <Tooltip title={text}>
                      <Text type="danger" ellipsis style={{ maxWidth: 200 }}>
                        {text}
                      </Text>
                    </Tooltip>
                  ) : (
                    '-'
                  ),
              },
            ]}
          />
        </Card>
      )}
    </Spin>
  );
};

export default LLMMonitoringStatistics;
