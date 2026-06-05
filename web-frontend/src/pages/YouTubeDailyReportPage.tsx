import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Layout, Typography, Spin, Empty, Space,
  List, App, Tooltip, ConfigProvider, theme as antdTheme, DatePicker, Button, Switch,
} from 'antd';
import {
  VideoCameraOutlined,
  CalendarOutlined,
  LoadingOutlined,
  ThunderboltOutlined,
  RobotOutlined,
  ShareAltOutlined,
  ReloadOutlined,
  LinkOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { DailyReportStatusTag } from '../components/reports/DailyReportStatusTag';
import { FEED_THEME_COLORS } from '../components/feed/feedTheme';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import {
  youtubeDailyReportsAPI,
  YouTubeDailyReport,
} from '../services/youtubeDailyReports';
import { useDailyReportStatusPoll } from '../hooks/useDailyReportStatusPoll';

const { Content } = Layout;
const { Title, Text } = Typography;

const YouTubeDailyReportPageInner: React.FC = () => {
  const { message } = App.useApp();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = FEED_THEME_COLORS[themeMode];

  const [reports, setReports] = useState<YouTubeDailyReport[]>([]);
  const [selectedReport, setSelectedReport] = useState<YouTubeDailyReport | null>(null);
  const [loading, setLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [autoEnabled, setAutoEnabled] = useState<boolean>(false);
  const [autoToggling, setAutoToggling] = useState(false);
  const [selectedDate, setSelectedDate] = useState<dayjs.Dayjs>(() => dayjs().subtract(1, 'day'));

  const selectedReportDateRef = useRef<string | null>(null);
  selectedReportDateRef.current = selectedReport?.report_date ?? null;
  const { startPoll, clearPoll } = useDailyReportStatusPoll();

  // ── Auto settings ──────────────────────────────────────────────────────
  const loadAutoSettings = useCallback(async () => {
    try {
      const res = await youtubeDailyReportsAPI.getAutoSettings();
      if (res.success && res.data) setAutoEnabled(res.data.auto_enabled);
    } catch { /* silently ignore */ }
  }, []);

  const handleAutoToggle = useCallback(async (checked: boolean) => {
    setAutoToggling(true);
    try {
      const res = await youtubeDailyReportsAPI.updateAutoSettings(checked);
      if (res.success && res.data) {
        setAutoEnabled(res.data.auto_enabled);
        message.success(checked ? 'Auto-generation enabled' : 'Auto-generation disabled');
      } else {
        message.error(res.error || 'Failed to update setting');
      }
    } catch {
      message.error('Failed to update setting');
    } finally {
      setAutoToggling(false);
    }
  }, [message]);

  // ── Reports list ───────────────────────────────────────────────────────
  const loadReportDetail = useCallback(async (dateStr: string) => {
    setDetailLoading(true);
    try {
      const res = await youtubeDailyReportsAPI.getReportByDate(dateStr);
      if (res.success) {
        setSelectedReport(res.data ?? null);
        if (res.data?.report_date) {
          setSelectedDate(dayjs(res.data.report_date));
        }
      }
    } catch {
      message.error('Failed to load report');
    } finally {
      setDetailLoading(false);
    }
  }, [message]);

  const loadReports = useCallback(async (options?: { quiet?: boolean }) => {
    const quiet = options?.quiet === true;
    if (!quiet) setLoading(true);
    try {
      const res = await youtubeDailyReportsAPI.listReports(60, 0);
      if (res.success && res.data) {
        setReports(res.data.reports);
        if (res.data.reports.length > 0 && !selectedReport) {
          loadReportDetail(res.data.reports[0].report_date);
        }
      }
    } catch {
      if (!quiet) message.error('Failed to load report list');
    } finally {
      if (!quiet) setLoading(false);
    }
  }, [selectedReport, loadReportDetail, message]);

  useEffect(() => {
    loadReports();
    loadAutoSettings();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // ── Generate ───────────────────────────────────────────────────────────
  const handleGenerate = useCallback(async () => {
    const dateStr = dayjs().subtract(1, 'day').format('YYYY-MM-DD');
    clearPoll();
    setGenerating(true);
    try {
      const res = await youtubeDailyReportsAPI.generateReport(dateStr, true);
      if (!res.success) {
        message.error(res.error || 'Failed to trigger report generation');
        setGenerating(false);
        return;
      }
      message.success(`Report generation started for ${dateStr}. It may take a few minutes.`);
      await loadReports();
      await loadReportDetail(dateStr);
      startPoll(dateStr, {
        fetchReport: youtubeDailyReportsAPI.getReportByDate,
        refreshList: () => loadReports({ quiet: true }),
        syncDetailIfViewing: () => selectedReportDateRef.current === dateStr,
        setDetail: (r) => setSelectedReport(r as YouTubeDailyReport),
        onDone: () => setGenerating(false),
      });
    } catch {
      message.error('Failed to trigger report generation');
      setGenerating(false);
    }
  }, [loadReports, loadReportDetail, message, clearPoll, startPoll]);

  const handleSelectReport = (report: YouTubeDailyReport) => {
    setSelectedDate(dayjs(report.report_date));
    if (report.report_content || report.share_token) {
      setSelectedReport(report);
    } else {
      loadReportDetail(report.report_date);
    }
  };

  const handleDateChange = (date: dayjs.Dayjs | null) => {
    if (!date) return;
    setSelectedDate(date);
    loadReportDetail(date.format('YYYY-MM-DD'));
  };

  // ── Styling ────────────────────────────────────────────────────────────
  const panelBg = themeColors.pageBg;
  const borderColor = themeColors.cardBorder;
  const sidebarBg = themeColors.cardBg;
  const cardBg = themeColors.cardBg;

  // ── Share Handler ───────────────────────────────────────────────────────
  const handleShare = useCallback(async () => {
    if (!selectedReport?.share_token) {
      message.warning('This report does not have a share link yet. Please wait for generation to complete.');
      return;
    }
    const shareUrl = `${window.location.origin}/api/reports/youtube-daily/shared/${selectedReport.share_token}`;
    try {
      await navigator.clipboard.writeText(shareUrl);
      message.success('Share link copied to clipboard');
    } catch {
      message.info(`Share link: ${shareUrl}`);
    }
  }, [selectedReport, message]);

  // ── iframe key for force refresh ───────────────────────────────────────
  const [iframeKey, setIframeKey] = useState(0);

  return (
    <Layout style={{ height: '100%', background: 'transparent' }}>
      {/* Sidebar - 报告列表选择器 */}
      <Layout.Sider
        width={240}
        style={{
          background: sidebarBg,
          borderRight: `1px solid ${borderColor}`,
          overflow: 'auto',
          height: '100%',
        }}
      >
        <div style={{ padding: '16px 12px 8px' }}>
          <Title level={5} style={{ margin: 0, marginBottom: 10 }}>
            <VideoCameraOutlined style={{ marginRight: 6 }} />Reports
          </Title>

          <Text strong style={{ display: 'block', marginBottom: 6, fontSize: 12, color: themeColors.text }}>
            <CalendarOutlined style={{ marginRight: 6 }} />
            Select date
          </Text>
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
                    <DailyReportStatusTag status={item.status} dark={isDark} compact />
                  </div>
                  {meta && item.status === 'completed' && (
                    <Text type="secondary" style={{ fontSize: 11, display: 'block', marginTop: 2 }}>
                      {meta.video_count} videos &middot; {meta.channel_count} channels
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

      {/* Main content - 参考 Theme Report 布局 */}
      <Content style={{ overflow: 'hidden', padding: 24, background: panelBg }}>
        <div style={{
          background: cardBg,
          border: `1px solid ${borderColor}`,
          borderRadius: 8,
          overflow: 'hidden',
          height: '100%',
          display: 'flex',
          flexDirection: 'column',
        }}>
          {/* Toolbar - 参考 Theme Report */}
          <div style={{
            padding: '14px 16px',
            borderBottom: `1px solid ${borderColor}`,
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
          }}>
            <div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <Text strong style={{ fontSize: 14 }}>
                  {selectedReport?.report_date || 'YouTube Daily Report'}
                </Text>
                {selectedReport && (
                  <DailyReportStatusTag status={selectedReport.status} dark={isDark} />
                )}
                {selectedReport?.metadata?.video_count != null && (
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    {selectedReport.metadata.video_count} videos &middot; {selectedReport.metadata.channel_count} channels
                  </Text>
                )}
              </div>
            </div>
            <Space size="small">
              {/* Auto-generate Switch */}
              <Tooltip title={autoEnabled ? 'Auto-generation ON — click to disable' : 'Auto-generation OFF — click to enable'}>
                <div style={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 6,
                  padding: '4px 8px',
                  borderRadius: 6,
                  background: autoEnabled
                    ? (isDark ? 'rgba(255,0,0,0.12)' : 'rgba(255,0,0,0.08)')
                    : (isDark ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.04)'),
                }}>
                  <RobotOutlined style={{ color: autoEnabled ? '#FF0000' : themeColors.textSecondary, fontSize: 12 }} />
                  <Switch
                    size="small"
                    checked={autoEnabled}
                    loading={autoToggling}
                    onChange={handleAutoToggle}
                  />
                </div>
              </Tooltip>

              {/* Generate */}
              <Tooltip title="Generate yesterday's report">
                <Button
                  type="primary"
                  size="small"
                  icon={<ThunderboltOutlined />}
                  loading={generating}
                  onClick={handleGenerate}
                  style={{ background: '#FF0000', borderColor: '#FF0000' }}
                />
              </Tooltip>

              {/* Refresh */}
              <Tooltip title="Refresh report">
                <Button
                  size="small"
                  icon={<ReloadOutlined />}
                  onClick={() => setIframeKey(prev => prev + 1)}
                />
              </Tooltip>

              {/* Open in new tab */}
              {selectedReport?.share_token && (
                <Tooltip title="Open in new tab">
                  <Button
                    size="small"
                    icon={<LinkOutlined />}
                    href={`/api/reports/youtube-daily/shared/${selectedReport.share_token}`}
                    target="_blank"
                    rel="noopener noreferrer"
                  />
                </Tooltip>
              )}

              {/* Share */}
              <Tooltip title={selectedReport?.share_token ? 'Copy share link' : 'No report to share'}>
                <Button
                  size="small"
                  icon={<ShareAltOutlined />}
                  disabled={!selectedReport?.share_token}
                  onClick={handleShare}
                />
              </Tooltip>
            </Space>
          </div>

          {/* Content */}
          <div style={{ flex: 1, overflow: 'hidden' }}>
            {detailLoading ? (
              <div style={{ textAlign: 'center', paddingTop: 120 }}>
                <Spin size="large" tip="Loading report..." />
              </div>
            ) : selectedReport?.share_token ? (
              <iframe
                key={iframeKey}
                src={`/api/reports/youtube-daily/shared/${selectedReport.share_token}`}
                title="YouTube Daily Report"
                style={{
                  width: '100%',
                  height: '100%',
                  border: 'none',
                  display: 'block',
                }}
              />
            ) : selectedReport?.status === 'failed' ? (
              <div style={{ textAlign: 'center', paddingTop: 120 }}>
                <Empty description={selectedReport.error_message || 'Report generation failed'} />
              </div>
            ) : (
              <div style={{ textAlign: 'center', paddingTop: 120 }}>
                <Empty description="Select a report from the list, or click Generate to create yesterday's report." />
              </div>
            )}
          </div>
        </div>
      </Content>
    </Layout>
  );
};

const YouTubeDailyReportPage: React.FC = () => {
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
            colorPrimary: '#FF0000',
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
        <YouTubeDailyReportPageInner />
      </App>
    </ConfigProvider>
  );
};

export default YouTubeDailyReportPage;