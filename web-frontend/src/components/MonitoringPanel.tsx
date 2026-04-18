import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Card, Col, Row, Space, Table, Tag, Button, Select, Typography,
  Statistic, Spin, Modal, Input, message, Tooltip,
} from 'antd';
import {
  ReloadOutlined, DeleteOutlined, ExclamationCircleOutlined,
  CloudServerOutlined, DatabaseOutlined, FireOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

import {
  getMonitoringSummary, purgeQueue,
  QueueInfo, WorkerInfo, SummaryResponse,
} from '../services/monitoring';

const { Text } = Typography;

// ---------------------------------------------------------------------------
// 主题色（跟 SystemLogsPage 保持一致）
// ---------------------------------------------------------------------------
interface ThemeColors {
  pageBg: string;
  cardBg: string;
  cardBorder: string;
  text: string;
  textSecondary: string;
  codeBlockBg: string;
}

interface MonitoringPanelProps {
  isDark: boolean;
  colors: ThemeColors;
  cardStyle: React.CSSProperties;
}

const REFRESH_OPTIONS = [
  { label: '3s', value: 3 },
  { label: '5s', value: 5 },
  { label: '10s', value: 10 },
  { label: '30s', value: 30 },
  { label: '手动', value: 0 },
];

// ---------------------------------------------------------------------------
// 队列深度颜色映射
// ---------------------------------------------------------------------------
const depthColor = (depth: number): { color: string; status: string } => {
  if (depth < 0) return { color: '#8c8c8c', status: 'error' };
  if (depth === 0) return { color: '#52c41a', status: 'ok' };
  if (depth <= 10) return { color: '#faad14', status: 'warn' };
  return { color: '#f5222d', status: 'critical' };
};

const MonitoringPanel: React.FC<MonitoringPanelProps> = ({ isDark, colors, cardStyle }) => {
  const [data, setData] = useState<SummaryResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [refreshSec, setRefreshSec] = useState<number>(5);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [purgeTarget, setPurgeTarget] = useState<QueueInfo | null>(null);
  const [purgeInput, setPurgeInput] = useState<string>('');
  const [purgeLoading, setPurgeLoading] = useState<boolean>(false);
  const timerRef = useRef<number | null>(null);

  // ---------- 加载数据 ----------
  const loadSummary = useCallback(async (force: boolean = false) => {
    setLoading(true);
    try {
      const resp = await getMonitoringSummary(force);
      setData(resp);
      setLastUpdated(new Date());
    } catch (e) {
      console.error(e);
      // 错误提示由 api.ts 响应拦截器统一处理
    } finally {
      setLoading(false);
    }
  }, []);

  // 首次挂载 + 自动刷新
  useEffect(() => {
    loadSummary();
    if (refreshSec > 0) {
      timerRef.current = window.setInterval(() => {
        loadSummary();
      }, refreshSec * 1000);
    }
    return () => {
      if (timerRef.current !== null) {
        window.clearInterval(timerRef.current);
        timerRef.current = null;
      }
    };
  }, [loadSummary, refreshSec]);

  // ---------- 清队列弹窗 ----------
  const openPurge = (q: QueueInfo) => {
    setPurgeTarget(q);
    setPurgeInput('');
  };

  const handlePurgeConfirm = async () => {
    if (!purgeTarget) return;
    if (purgeInput !== purgeTarget.name) {
      message.error('输入的队列名与目标不匹配');
      return;
    }
    setPurgeLoading(true);
    try {
      const resp = await purgeQueue(purgeTarget.name);
      message.success(resp.message);
      setPurgeTarget(null);
      await loadSummary(true);
    } catch (e) {
      console.error(e);
    } finally {
      setPurgeLoading(false);
    }
  };

  // ---------- 表格列定义 ----------
  const queueColumns: ColumnsType<QueueInfo> = useMemo(() => [
    {
      title: '队列名',
      dataIndex: 'name',
      key: 'name',
      render: (name: string) => <Text code style={{ color: colors.text }}>{name}</Text>,
    },
    {
      title: '积压任务数',
      dataIndex: 'depth',
      key: 'depth',
      width: 160,
      sorter: (a, b) => a.depth - b.depth,
      render: (depth: number, record: QueueInfo) => {
        if (depth < 0) {
          return <Tooltip title={record.error || 'Redis 探测失败'}><Tag color="default">N/A</Tag></Tooltip>;
        }
        const { color } = depthColor(depth);
        return (
          <Space>
            <div style={{
              width: 8, height: 8, borderRadius: '50%',
              backgroundColor: color, display: 'inline-block',
            }} />
            <Text strong style={{ color, fontSize: 15 }}>{depth}</Text>
          </Space>
        );
      },
    },
    {
      title: '操作',
      key: 'actions',
      width: 120,
      render: (_: any, record: QueueInfo) => (
        <Button
          size="small" danger icon={<DeleteOutlined />}
          disabled={record.depth <= 0}
          onClick={() => openPurge(record)}
        >
          清空
        </Button>
      ),
    },
  ], [colors.text]);

  const workerColumns: ColumnsType<WorkerInfo> = useMemo(() => [
    {
      title: 'Worker 名称',
      dataIndex: 'name',
      key: 'name',
      render: (name: string, rec: WorkerInfo) => (
        <Space>
          <div style={{
            width: 8, height: 8, borderRadius: '50%',
            backgroundColor: rec.online ? '#52c41a' : '#8c8c8c',
          }} />
          <Text style={{ color: colors.text, fontFamily: 'monospace', fontSize: 12 }}>
            {name}
          </Text>
        </Space>
      ),
    },
    {
      title: '订阅队列',
      dataIndex: 'queues',
      key: 'queues',
      render: (queues: string[]) => queues.length
        ? <Space wrap size={[4, 4]}>{queues.map(q => <Tag key={q} color="blue">{q}</Tag>)}</Space>
        : <Text type="secondary" style={{ fontSize: 12 }}>（空闲）</Text>,
    },
    {
      title: '活跃',
      dataIndex: 'active_count',
      key: 'active_count',
      width: 70,
      align: 'center',
      render: (v: number) => (
        <Tag color={v > 0 ? 'processing' : 'default'}>{v}</Tag>
      ),
    },
    {
      title: '预留',
      dataIndex: 'reserved_count',
      key: 'reserved_count',
      width: 70,
      align: 'center',
      render: (v: number) => <span style={{ color: colors.textSecondary }}>{v}</span>,
    },
    {
      title: '延时',
      dataIndex: 'scheduled_count',
      key: 'scheduled_count',
      width: 70,
      align: 'center',
      render: (v: number) => <span style={{ color: colors.textSecondary }}>{v}</span>,
    },
    {
      title: '已处理',
      dataIndex: 'processed_total',
      key: 'processed_total',
      width: 110,
      align: 'right',
      render: (v?: number | null) => <span style={{ color: colors.textSecondary }}>
        {v == null ? '—' : v.toLocaleString()}
      </span>,
    },
    {
      title: '并发',
      dataIndex: 'pool_max_concurrency',
      key: 'pool_max_concurrency',
      width: 70,
      align: 'center',
      render: (v?: number | null, rec?: WorkerInfo) => {
        const actual = rec?.pool_processes;
        if (v == null) return '—';
        return actual != null && actual !== v
          ? <Tooltip title={`最大 ${v} / 当前 ${actual}`}>{actual}/{v}</Tooltip>
          : v;
      },
    },
  ], [colors.text, colors.textSecondary]);

  // ---------- Render ----------
  const totalDepth = data?.total_depth ?? 0;
  const onlineCount = data?.online_count ?? 0;
  const activeTaskCount = data?.active_task_count ?? 0;
  const cacheAge = data?.inspect_cache_age_sec ?? 0;

  return (
    <Spin spinning={loading && !data}>
      {/* 控制栏 */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }} align="middle">
        <Col>
          <Space>
            <Text style={{ color: colors.text }}>自动刷新:</Text>
            <Select
              style={{ width: 100 }}
              value={refreshSec}
              onChange={setRefreshSec}
              options={REFRESH_OPTIONS}
            />
            <Button
              type="primary" icon={<ReloadOutlined />}
              onClick={() => loadSummary(true)} loading={loading}
            >
              立即刷新
            </Button>
          </Space>
        </Col>
        <Col flex="auto" style={{ textAlign: 'right' }}>
          <Space split={<span style={{ color: colors.textSecondary }}>·</span>}>
            {lastUpdated && (
              <Text type="secondary" style={{ fontSize: 12 }}>
                上次更新: {lastUpdated.toLocaleTimeString()}
              </Text>
            )}
            <Tooltip title="inspect() 结果在后端有 3s TTL 内存缓存">
              <Text type="secondary" style={{ fontSize: 12 }}>
                Worker 快照延迟: {cacheAge}s
              </Text>
            </Tooltip>
          </Space>
        </Col>
      </Row>

      {/* 总览统计 */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} sm={12} md={8}>
          <Card style={cardStyle} bodyStyle={{ padding: 20 }}>
            <Statistic
              title={<Space><CloudServerOutlined />在线 Worker</Space>}
              value={onlineCount}
              valueStyle={{ color: onlineCount > 0 ? '#52c41a' : '#f5222d' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={8}>
          <Card style={cardStyle} bodyStyle={{ padding: 20 }}>
            <Statistic
              title={<Space><DatabaseOutlined />总积压任务</Space>}
              value={totalDepth}
              valueStyle={{ color: depthColor(totalDepth).color }}
              suffix={totalDepth > 10 ? <Tag color="error" style={{ marginLeft: 8 }}>需关注</Tag> : null}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={8}>
          <Card style={cardStyle} bodyStyle={{ padding: 20 }}>
            <Statistic
              title={<Space><FireOutlined />当前活跃任务</Space>}
              value={activeTaskCount}
              valueStyle={{ color: activeTaskCount > 0 ? '#1890ff' : colors.textSecondary }}
            />
          </Card>
        </Col>
      </Row>

      {/* 队列表 */}
      <Card
        title={<Space><DatabaseOutlined />Redis 队列深度</Space>}
        style={{ ...cardStyle, marginBottom: 16 }}
        bodyStyle={{ padding: 0 }}
      >
        <Table<QueueInfo>
          dataSource={data?.queues || []}
          columns={queueColumns}
          rowKey="name"
          size="small"
          pagination={false}
          locale={{ emptyText: '暂无数据' }}
        />
      </Card>

      {/* Worker 表 */}
      <Card
        title={<Space><CloudServerOutlined />Celery Worker ({onlineCount} 在线)</Space>}
        style={cardStyle}
        bodyStyle={{ padding: 0 }}
      >
        <Table<WorkerInfo>
          dataSource={data?.workers || []}
          columns={workerColumns}
          rowKey="name"
          size="small"
          pagination={false}
          locale={{ emptyText: '没有 worker 响应心跳。请确认 Celery worker 已启动。' }}
        />
      </Card>

      {/* 清队列二次确认弹窗 */}
      <Modal
        title={<Space><ExclamationCircleOutlined style={{ color: '#f5222d' }} />危险操作: 清空队列</Space>}
        open={!!purgeTarget}
        onOk={handlePurgeConfirm}
        onCancel={() => setPurgeTarget(null)}
        okText="确认清空"
        cancelText="取消"
        okButtonProps={{
          danger: true,
          disabled: purgeInput !== (purgeTarget?.name || ''),
          loading: purgeLoading,
        }}
        destroyOnClose
      >
        <div style={{ marginBottom: 12 }}>
          <Text>
            将删除队列 <Text code>{purgeTarget?.name}</Text> 里所有（
            <Text strong style={{ color: '#f5222d' }}>{purgeTarget?.depth}</Text>
            ）条未消费的消息，<Text strong>此操作不可撤销</Text>。
          </Text>
        </div>
        <div style={{ marginBottom: 8 }}>
          <Text type="secondary">为防止误操作，请输入完整的队列名以确认:</Text>
        </div>
        <Input
          placeholder={purgeTarget?.name}
          value={purgeInput}
          onChange={(e) => setPurgeInput(e.target.value)}
          onPressEnter={handlePurgeConfirm}
          autoFocus
        />
      </Modal>
    </Spin>
  );
};

export default MonitoringPanel;
