import React, { useState, useEffect, useCallback } from 'react';
import {
  Layout, Typography, DatePicker, Button, Spin, Empty, Tag, Space,
  List, App, Tooltip, ConfigProvider, theme as antdTheme,
} from 'antd';
import {
  ReloadOutlined, FileTextOutlined,
  CalendarOutlined, CheckCircleOutlined,
  CloseCircleOutlined, ClockCircleOutlined, LoadingOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import MarkdownRenderer from '../components/MarkdownRenderer';
import { FEED_THEME_COLORS } from '../components/feed/feedTheme';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import {
  twitterDailyReportsAPI,
  TwitterDailyReport,
} from '../services/twitterDailyReports';

const { Content } = Layout;
const { Title, Text } = Typography;

const STATUS_MAP: Record<string, { color: string; icon: React.ReactNode; label: string }> = {
  completed: { color: 'success', icon: <CheckCircleOutlined />, label: 'Completed' },
  generating: { color: 'processing', icon: <LoadingOutlined />, label: 'Generating' },
  failed: { color: 'error', icon: <CloseCircleOutlined />, label: 'Failed' },
  pending: { color: 'default', icon: <ClockCircleOutlined />, label: 'Pending' },
};

const TwitterDailyReportPageInner: React.FC = () => {
  const { message } = App.useApp();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = FEED_THEME_COLORS[themeMode];

  const [reports, setReports] = useState<TwitterDailyReport[]>([]);
  const [selectedReport, setSelectedReport] = useState<TwitterDailyReport | null>(null);
  const [loading, setLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [selectedDate, setSelectedDate] = useState<dayjs.Dayjs>(dayjs().subtract(1, 'day'));

  // ── Load reports list ──────────────────────────────────────────────────
  const loadReports = useCallback(async () => {
    setLoading(true);
    try {
      const res = await twitterDailyReportsAPI.listReports(60, 0);
      if (res.success && res.data) {
        setReports(res.data.reports);
        if (res.data.reports.length > 0 && !selectedReport) {
          const latest = res.data.reports[0];
          setSelectedDate(dayjs(latest.report_date));
          loadReportDetail(latest.report_date);
        }
      }
    } catch {
      message.error('Failed to load report list');
    } finally {
      setLoading(false);
    }
  }, []);

  const loadReportDetail = useCallback(async (dateStr: string) => {
    setDetailLoading(true);
    try {
      const res = await twitterDailyReportsAPI.getReportByDate(dateStr);
      if (res.success) {
        setSelectedReport(res.data ?? null);
      }
    } catch {
      message.error('Failed to load report');
    } finally {
      setDetailLoading(false);
    }
  }, []);

  useEffect(() => { loadReports(); }, [loadReports]);

  // ── Date selection ─────────────────────────────────────────────────────
  const handleDateChange = (date: dayjs.Dayjs | null) => {
    if (!date) return;
    setSelectedDate(date);
    loadReportDetail(date.format('YYYY-MM-DD'));
  };

  const handleSelectReport = (report: TwitterDailyReport) => {
    setSelectedDate(dayjs(report.report_date));
    if (report.report_content) {
      setSelectedReport(report);
    } else {
      loadReportDetail(report.report_date);
    }
  };

  // ── Styling ────────────────────────────────────────────────────────────
  const panelBg = themeColors.pageBg;
  const borderColor = themeColors.cardBorder;
  const sidebarBg = themeColors.cardBg;

  return (
    <Layout style={{ height: '100%', background: 'transparent' }}>
      {/* Sidebar: report list */}
      <Layout.Sider
        width={280}
        style={{
          background: sidebarBg,
          borderRight: `1px solid ${borderColor}`,
          overflow: 'auto',
          height: '100%',
        }}
      >
        <div style={{ padding: '16px 12px 8px' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
            <Title level={5} style={{ margin: 0 }}>
              <FileTextOutlined style={{ marginRight: 6 }} />Daily Reports
            </Title>
            <Tooltip title="Refresh">
              <Button size="small" type="text" icon={<ReloadOutlined />} onClick={loadReports} loading={loading} />
            </Tooltip>
          </div>

          <DatePicker
            value={selectedDate}
            onChange={handleDateChange}
            style={{ width: '100%', marginBottom: 12 }}
            allowClear={false}
          />
        </div>

        <List
          loading={loading}
          dataSource={reports}
          locale={{ emptyText: <Empty description="No reports yet" image={Empty.PRESENTED_IMAGE_SIMPLE} /> }}
          renderItem={(item) => {
            const status = STATUS_MAP[item.status] || STATUS_MAP.pending;
            const isSelected = selectedReport?.id === item.id;
            const meta = item.metadata;
            return (
              <List.Item
                onClick={() => handleSelectReport(item)}
                style={{
                  padding: '10px 12px',
                  cursor: 'pointer',
                  background: isSelected ? themeColors.selectedItemBg : 'transparent',
                  borderBottom: `1px solid ${borderColor}`,
                  transition: 'background 0.2s',
                }}
              >
                <div style={{ width: '100%' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <Text strong style={{ fontSize: 13 }}>
                      <CalendarOutlined style={{ marginRight: 4 }} />
                      {item.report_date}
                    </Text>
                    <Tag color={status.color} icon={status.icon} style={{ fontSize: 11, marginRight: 0 }}>
                      {status.label}
                    </Tag>
                  </div>
                  {meta && item.status === 'completed' && (
                    <Text type="secondary" style={{ fontSize: 11, display: 'block', marginTop: 2 }}>
                      {meta.tweet_count} tweets &middot; {meta.author_count} authors
                      {meta.generation_time_seconds != null && ` · ${meta.generation_time_seconds}s`}
                    </Text>
                  )}
                  {item.status === 'failed' && item.error_message && (
                    <Text type="danger" style={{ fontSize: 11, display: 'block', marginTop: 2 }} ellipsis>
                      {item.error_message}
                    </Text>
                  )}
                </div>
              </List.Item>
            );
          }}
        />
      </Layout.Sider>

      {/* Main: report content */}
      <Content style={{ overflow: 'auto', padding: 24, background: panelBg }}>
        {detailLoading ? (
          <div style={{ textAlign: 'center', paddingTop: 120 }}>
            <Spin size="large" tip="Loading report..." />
          </div>
        ) : selectedReport?.report_content ? (
          <div style={{ maxWidth: 900, margin: '0 auto' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
              <Space>
                <Tag color={STATUS_MAP[selectedReport.status]?.color}>
                  {selectedReport.report_date}
                </Tag>
                {selectedReport.metadata?.tweet_count != null && (
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    {selectedReport.metadata.tweet_count} tweets &middot;{' '}
                    {selectedReport.metadata.author_count} authors
                  </Text>
                )}
              </Space>
            </div>
            <MarkdownRenderer
              content={selectedReport.report_content}
              variant={isDark ? 'dark' : 'light'}
            />
          </div>
        ) : selectedReport?.status === 'failed' ? (
          <div style={{ textAlign: 'center', paddingTop: 120 }}>
            <Empty description={selectedReport.error_message || 'Report generation failed'} />
          </div>
        ) : (
          <div style={{ textAlign: 'center', paddingTop: 120 }}>
            <Empty description="Select a date to view the report. Reports are generated automatically by the scheduler." />
          </div>
        )}
      </Content>
    </Layout>
  );
};

const TwitterDailyReportPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = FEED_THEME_COLORS[themeMode];

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: themeColors.cardBg,
            colorBgElevated: themeColors.cardBg,
            colorBorderSecondary: themeColors.cardBorder,
            colorPrimary: '#2196F3',
            colorText: themeColors.text,
            colorTextSecondary: themeColors.textSecondary,
            colorFillQuaternary: themeColors.inputBg ?? '#1E202B',
            colorBgLayout: themeColors.pageBg,
            borderRadius: 8,
            borderRadiusLG: 12,
          },
        }),
      }}
    >
      <App>
        <TwitterDailyReportPageInner />
      </App>
    </ConfigProvider>
  );
};

export default TwitterDailyReportPage;
