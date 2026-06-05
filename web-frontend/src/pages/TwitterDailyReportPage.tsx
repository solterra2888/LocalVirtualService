import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Layout, Typography, Spin, Empty, Space,
  List, App, Tooltip, ConfigProvider, theme as antdTheme, Switch, DatePicker, Button,
} from 'antd';
import {
  FileTextOutlined,
  CalendarOutlined,
  LoadingOutlined,
  PlayCircleOutlined,
  RobotOutlined,
  ShareAltOutlined,
  ReloadOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { DailyReportStatusTag } from '../components/reports/DailyReportStatusTag';
import { FEED_THEME_COLORS } from '../components/feed/feedTheme';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import {
  twitterDailyReportsAPI,
  TwitterDailyReport,
} from '../services/twitterDailyReports';
import { useDailyReportStatusPoll } from '../hooks/useDailyReportStatusPoll';

const { Content } = Layout;
const { Title, Text } = Typography;

const TwitterDailyReportPageInner: React.FC = () => {
  const { message } = App.useApp();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = FEED_THEME_COLORS[themeMode];

  const [reports, setReports] = useState<TwitterDailyReport[]>([]);
  const [selectedReport, setSelectedReport] = useState<TwitterDailyReport | null>(null);
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
      const res = await twitterDailyReportsAPI.getAutoSettings();
      if (res.success && res.data) setAutoEnabled(res.data.auto_enabled);
    } catch { /* silently ignore */ }
  }, []);

  const handleAutoToggle = useCallback(async (checked: boolean) => {
    setAutoToggling(true);
    try {
      const res = await twitterDailyReportsAPI.updateAutoSettings(checked);
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
      const res = await twitterDailyReportsAPI.getReportByDate(dateStr);
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
      const res = await twitterDailyReportsAPI.listReports(60, 0);
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
  const handleGenerate = useCallback(async (force: boolean = false) => {
    const dateStr = dayjs().subtract(1, 'day').format('YYYY-MM-DD');
    clearPoll();
    setGenerating(true);
    try {
      const res = await twitterDailyReportsAPI.generateReport(dateStr, force);
      if (!res.success) {
        message.error(res.error || 'Failed to trigger report generation');
        setGenerating(false);
        return;
      }
      message.success(`Report generation started for ${dateStr}. It may take a few minutes.`);
      await loadReports();
      await loadReportDetail(dateStr);
      startPoll(dateStr, {
        fetchReport: twitterDailyReportsAPI.getReportByDate,
        refreshList: () => loadReports({ quiet: true }),
        syncDetailIfViewing: () => selectedReportDateRef.current === dateStr,
        setDetail: (r) => setSelectedReport(r as TwitterDailyReport),
        onDone: () => setGenerating(false),
      });
    } catch {
      message.error('Failed to trigger report generation');
      setGenerating(false);
    }
  }, [loadReports, loadReportDetail, message, clearPoll, startPoll]);

  const handleSelectReport = (report: TwitterDailyReport) => {
    setSelectedDate(dayjs(report.report_date));
    if (report.report_content) {
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

  // ── Share Handler ───────────────────────────────────────────────────────
  const handleShare = useCallback(async () => {
    if (!selectedReport?.share_token) {
      message.warning('This report does not have a share link yet. Please wait for generation to complete.');
      return;
    }
    const shareUrl = `${window.location.origin}/api/reports/twitter-daily/shared/${selectedReport.share_token}`;
    try {
      await navigator.clipboard.writeText(shareUrl);
      message.success('Share link copied to clipboard');
    } catch {
      message.error('Failed to copy share link');
    }
  }, [selectedReport, message]);

  // ── iframe key for force refresh ───────────────────────────────────────
  const [iframeKey, setIframeKey] = useState(0);

  return (
    <Layout style={{ height: '100%', background: 'transparent' }}>
      {/* Sidebar */}
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
          {/* Header row: title + Generate */}
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 10 }}>
            <Title level={5} style={{ margin: 0, flex: 1, whiteSpace: 'nowrap' }}>
              <FileTextOutlined style={{ marginRight: 6 }} />Daily Reports
            </Title>
            <Tooltip title="Generate yesterday's report">
              <button
                type="button"
                className="feed-toolbar-icon-btn"
                onClick={() => handleGenerate(true)}
                disabled={generating}
                aria-label="Generate yesterday's report"
              >
                <span className="feed-toolbar-icon-btn__icon">
                  {generating ? <LoadingOutlined spin /> : <PlayCircleOutlined />}
                </span>
              </button>
            </Tooltip>
          </div>

          {/* Auto-generate toggle */}
          <Tooltip title={autoEnabled ? 'Auto-generation ON — click to disable' : 'Auto-generation OFF — click to enable'}>
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                gap: 8,
                marginBottom: 10,
                padding: '6px 8px',
                borderRadius: 6,
                background: autoEnabled
                  ? (isDark ? 'rgba(33,150,243,0.12)' : 'rgba(33,150,243,0.08)')
                  : (isDark ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.04)'),
              }}
            >
              <RobotOutlined style={{ color: autoEnabled ? '#2196F3' : themeColors.textSecondary, fontSize: 13 }} />
              <Text style={{ fontSize: 12, flex: 1, color: autoEnabled ? '#2196F3' : themeColors.textSecondary }}>
                Auto-generate
              </Text>
              <Switch
                size="small"
                checked={autoEnabled}
                loading={autoToggling}
                onChange={handleAutoToggle}
              />
            </div>
          </Tooltip>

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

      {/* Main content */}
      <Content style={{ overflow: 'hidden', padding: 0, background: panelBg }}>
        {detailLoading ? (
          <div style={{ textAlign: 'center', paddingTop: 120 }}>
            <Spin size="large" tip="Loading report..." />
          </div>
        ) : selectedReport?.share_token ? (
          <div style={{ width: '100%', height: '100%', display: 'flex', flexDirection: 'column' }}>
            {/* Header with status and share button */}
            <div style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              padding: '12px 16px',
              background: themeColors.cardBg,
              borderBottom: `1px solid ${borderColor}`,
            }}>
              <Space align="center" size="middle">
                <Text strong style={{ fontSize: 13 }}>{selectedReport.report_date}</Text>
                <DailyReportStatusTag status={selectedReport.status} dark={isDark} />
                {selectedReport.metadata?.tweet_count != null && (
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    {selectedReport.metadata.tweet_count} tweets &middot;{' '}
                    {selectedReport.metadata.author_count} authors
                  </Text>
                )}
              </Space>
              <Space>
                <Tooltip title="Refresh report">
                  <Button
                    size="small"
                    icon={<ReloadOutlined />}
                    onClick={() => setIframeKey(prev => prev + 1)}
                  />
                </Tooltip>
                <Tooltip title="Copy share link">
                  <Button
                    size="small"
                    icon={<ShareAltOutlined />}
                    onClick={handleShare}
                  />
                </Tooltip>
              </Space>
            </div>
            {/* iframe for HTML report */}
            <div style={{ flex: 1, overflow: 'hidden' }}>
              <iframe
                key={iframeKey}
                src={`/api/reports/twitter-daily/shared/${selectedReport.share_token}`}
                title="Twitter Daily Report"
                style={{
                  width: '100%',
                  height: '100%',
                  border: 'none',
                  display: 'block',
                }}
              />
            </div>
          </div>
        ) : selectedReport?.status === 'failed' ? (
          <div style={{ textAlign: 'center', paddingTop: 120 }}>
            <Empty description={selectedReport.error_message || 'Report generation failed'} />
          </div>
        ) : (
          <div style={{ textAlign: 'center', paddingTop: 120 }}>
            <Empty description="Select a report from the list, or use the play button next to the title to generate." />
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
