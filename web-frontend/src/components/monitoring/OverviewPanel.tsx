import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  Card, Col, Row, Statistic, Tag, Space, Typography, Spin, Button, Tooltip, Table,
} from 'antd';
import {
  CloudServerOutlined,
  DatabaseOutlined,
  FireOutlined,
  WarningOutlined,
  ReloadOutlined,
  FileTextOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import { getMonitoringSummary, LogFileStats, SummaryResponse } from '../../services/monitoring';

dayjs.extend(relativeTime);

const { Text } = Typography;

interface OverviewPanelProps {
  isDark: boolean;
  colors: {
    text: string;
    textSecondary: string;
    cardBg: string;
    cardBorder: string;
    codeBlockBg: string;
  };
  cardStyle: React.CSSProperties;
  active?: boolean;
}

const AUTO_REFRESH_SEC = 30;

const OverviewPanel: React.FC<OverviewPanelProps> = ({ colors, cardStyle, active = true }) => {
  const [data, setData] = useState<SummaryResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const timerRef = useRef<number | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const resp = await getMonitoringSummary(false);
      setData(resp);
      setLastUpdated(new Date());
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!active) {
      if (timerRef.current !== null) {
        window.clearInterval(timerRef.current);
        timerRef.current = null;
      }
      return;
    }
    load();
    timerRef.current = window.setInterval(load, AUTO_REFRESH_SEC * 1000);
    return () => {
      if (timerRef.current !== null) {
        window.clearInterval(timerRef.current);
        timerRef.current = null;
      }
    };
  }, [active, load]);

  const totalDepth = data?.total_depth ?? 0;
  const onlineCount = data?.online_count ?? 0;
  const activeTaskCount = data?.active_task_count ?? 0;
  const recentErrors = data?.recent_errors_1h ?? 0;

  const depthColor = totalDepth > 10 ? '#f5222d' : totalDepth > 0 ? '#faad14' : '#52c41a';

  // Log file columns
  const logColumns: ColumnsType<LogFileStats> = [
    {
      title: 'Log File',
      dataIndex: 'filename',
      key: 'filename',
      render: (name: string) => (
        <Text code style={{ color: colors.text, fontSize: 12 }}>
          {name}
        </Text>
      ),
    },
    {
      title: 'Size',
      dataIndex: 'size_human',
      key: 'size_human',
      width: 100,
      align: 'right',
      render: (v: string, rec: LogFileStats) => {
        const isLarge = rec.size_kb > 50 * 1024;
        return (
          <Text style={{ color: isLarge ? '#f5222d' : colors.textSecondary, fontSize: 12 }}>
            {rec.exists ? v : '—'}
          </Text>
        );
      },
    },
    {
      title: 'Modified',
      dataIndex: 'mtime',
      key: 'mtime',
      width: 140,
      align: 'right',
      render: (mtime?: number | null) =>
        mtime ? (
          <Tooltip title={dayjs.unix(mtime).format('YYYY-MM-DD HH:mm:ss')}>
            <Text type="secondary" style={{ fontSize: 12 }}>
              {dayjs.unix(mtime).fromNow()}
            </Text>
          </Tooltip>
        ) : (
          <Text type="secondary" style={{ fontSize: 12 }}>—</Text>
        ),
    },
  ];

  return (
    <Spin spinning={loading && !data}>
      {/* Header row */}
      <Row gutter={[0, 0]} align="middle" style={{ marginBottom: 16 }}>
        <Col flex="auto" />
        <Col>
          <Space>
            {lastUpdated && (
              <Text type="secondary" style={{ fontSize: 12 }}>
                <ClockCircleOutlined style={{ marginRight: 4 }} />
                {lastUpdated.toLocaleTimeString()}
              </Text>
            )}
            <Button
              size="small"
              icon={<ReloadOutlined />}
              onClick={() => load()}
              loading={loading}
            >
              Refresh
            </Button>
          </Space>
        </Col>
      </Row>

      {/* KPI cards */}
      <Row gutter={[16, 16]} style={{ marginBottom: 16 }}>
        <Col xs={24} sm={12} md={6}>
          <Card style={cardStyle} bodyStyle={{ padding: 20 }}>
            <Statistic
              title={<Space><CloudServerOutlined />Workers Online</Space>}
              value={onlineCount}
              valueStyle={{ color: onlineCount > 0 ? '#52c41a' : '#f5222d' }}
              suffix={
                onlineCount === 0
                  ? <Tag color="error" style={{ marginLeft: 8 }}>Offline</Tag>
                  : undefined
              }
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={6}>
          <Card style={cardStyle} bodyStyle={{ padding: 20 }}>
            <Statistic
              title={<Space><DatabaseOutlined />Queue Backlog</Space>}
              value={totalDepth}
              valueStyle={{ color: depthColor }}
              suffix={
                totalDepth > 10
                  ? <Tag color="error" style={{ marginLeft: 8 }}>High</Tag>
                  : undefined
              }
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={6}>
          <Card style={cardStyle} bodyStyle={{ padding: 20 }}>
            <Statistic
              title={<Space><FireOutlined />Active Tasks</Space>}
              value={activeTaskCount}
              valueStyle={{ color: activeTaskCount > 0 ? '#1890ff' : colors.textSecondary }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} md={6}>
          <Card style={cardStyle} bodyStyle={{ padding: 20 }}>
            <Statistic
              title={<Space><WarningOutlined />Errors (1h)</Space>}
              value={recentErrors}
              valueStyle={{ color: recentErrors > 0 ? '#f5222d' : '#52c41a' }}
              suffix={
                recentErrors > 0
                  ? <Tag color="error" style={{ marginLeft: 8 }}>app.log</Tag>
                  : undefined
              }
            />
          </Card>
        </Col>
      </Row>

      {/* Log file overview */}
      <Card
        title={<Space><FileTextOutlined />Log Files</Space>}
        style={{ ...cardStyle, marginBottom: 16 }}
        bodyStyle={{ padding: 0 }}
        extra={
          <Text type="secondary" style={{ fontSize: 12 }}>
            Files &gt;50 MB are highlighted in red
          </Text>
        }
      >
        <Table<LogFileStats>
          dataSource={data?.log_files ?? []}
          columns={logColumns}
          rowKey="type"
          size="small"
          pagination={false}
          locale={{ emptyText: '暂无数据' }}
        />
      </Card>
    </Spin>
  );
};

export default OverviewPanel;
