import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  App,
  Button,
  ConfigProvider,
  DatePicker,
  Drawer,
  Empty,
  List,
  Space,
  Spin,
  Tag,
  Typography,
  theme as antdTheme,
} from 'antd';
import {
  CalendarOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  ClockCircleOutlined,
  LoadingOutlined,
  MenuOutlined,
  ReloadOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import MarkdownRenderer from '../components/MarkdownRenderer';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { FEED_THEME_COLORS } from '../components/feed/feedTheme';
import {
  twitterDailyReportsAPI,
  TwitterDailyReport,
} from '../services/twitterDailyReports';
import './MobileFeedsPage.css';

const { Text } = Typography;

const STATUS_MAP: Record<string, { color: string; icon: React.ReactNode; label: string }> = {
  completed: { color: 'success', icon: <CheckCircleOutlined />, label: 'Completed' },
  generating: { color: 'processing', icon: <LoadingOutlined />, label: 'Generating' },
  failed: { color: 'error', icon: <CloseCircleOutlined />, label: 'Failed' },
  pending: { color: 'default', icon: <ClockCircleOutlined />, label: 'Pending' },
};

/**
 * Mobile Reports: Twitter Daily Report with left drawer (date + list), Feeds-like toolbar.
 */
const MobileReportsPageInner: React.FC = () => {
  const { message } = App.useApp();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = FEED_THEME_COLORS[themeMode];

  const [reports, setReports] = useState<TwitterDailyReport[]>([]);
  const [selectedReport, setSelectedReport] = useState<TwitterDailyReport | null>(null);
  const [loading, setLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [selectedDate, setSelectedDate] = useState<dayjs.Dayjs>(() => dayjs().subtract(1, 'day'));
  const [drawerOpen, setDrawerOpen] = useState(false);

  const pollIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const stopPolling = useCallback(() => {
    if (pollIntervalRef.current) {
      clearInterval(pollIntervalRef.current);
      pollIntervalRef.current = null;
    }
  }, []);

  useEffect(() => () => stopPolling(), [stopPolling]);

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
  }, [message]);

  const loadReports = useCallback(async () => {
    setLoading(true);
    try {
      const res = await twitterDailyReportsAPI.listReports(60, 0);
      if (res.success && res.data) {
        setReports(res.data.reports);
      }
    } catch {
      message.error('Failed to load report list');
    } finally {
      setLoading(false);
    }
  }, [message]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setDetailLoading(true);
      try {
        const res = await twitterDailyReportsAPI.listReports(60, 0);
        if (cancelled || !res.success || !res.data) return;
        setReports(res.data.reports);
        if (res.data.reports.length > 0) {
          const latest = res.data.reports[0];
          setSelectedDate(dayjs(latest.report_date));
          const detail = await twitterDailyReportsAPI.getReportByDate(latest.report_date);
          if (!cancelled && detail.success) {
            setSelectedReport(detail.data ?? null);
          }
        } else {
          setSelectedReport(null);
        }
      } catch {
        if (!cancelled) message.error('Failed to load report list');
      } finally {
        if (!cancelled) {
          setLoading(false);
          setDetailLoading(false);
        }
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [message]);

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
    setDrawerOpen(false);
  };

  const handleGenerate = async (force = false) => {
    setGenerating(true);
    stopPolling();
    try {
      const dateStr = selectedDate.format('YYYY-MM-DD');
      const res = await twitterDailyReportsAPI.generateReport(dateStr, force);
      if (res.success) {
        if (res.data?.skipped) {
          message.info(res.message || 'Report already exists');
          loadReportDetail(dateStr);
          setGenerating(false);
          return;
        }
        message.info('Report generation started, please wait...');

        let elapsed = 0;
        const MAX_POLL = 600_000;
        const INTERVAL = 5_000;
        pollIntervalRef.current = setInterval(async () => {
          elapsed += INTERVAL;
          if (elapsed > MAX_POLL) {
            stopPolling();
            setGenerating(false);
            message.warning('Generation timed out. Please refresh later.');
            return;
          }
          try {
            const detail = await twitterDailyReportsAPI.getReportByDate(dateStr);
            const status = detail.data?.status;
            if (status === 'completed') {
              stopPolling();
              setSelectedReport(detail.data ?? null);
              await loadReports();
              setGenerating(false);
              message.success('Report generated successfully');
            } else if (status === 'failed') {
              stopPolling();
              setSelectedReport(detail.data ?? null);
              await loadReports();
              setGenerating(false);
              message.error(detail.data?.error_message || 'Generation failed');
            }
          } catch {
            /* ignore poll errors */
          }
        }, INTERVAL);
      } else {
        message.error(res.error || 'Generation failed');
        setGenerating(false);
      }
    } catch {
      message.error('Generation failed');
      setGenerating(false);
    }
  };

  const borderColor = themeColors.cardBorder;

  return (
    <div
      className="mobile-feeds-page"
      data-theme={themeMode}
      style={
        {
          display: 'flex',
          flexDirection: 'column',
          minHeight: '100%',
          background: themeColors.pageBg,
          ['--mobile-feeds-page-bg' as string]: themeColors.pageBg,
          ['--mobile-feeds-card-bg' as string]: themeColors.cardBg,
          ['--mobile-feeds-border' as string]: themeColors.cardBorder,
          ['--mobile-feeds-accent' as string]: themeColors.selectedBorder,
          ['--mobile-feeds-selected-bg' as string]: themeColors.selectedItemBg,
          ['--mobile-feeds-text' as string]: themeColors.text,
          ['--mobile-feeds-text-secondary' as string]: themeColors.textSecondary,
        } as React.CSSProperties
      }
    >
      <div className="mobile-feeds-toolbar">
        <Button
          type="text"
          icon={<MenuOutlined style={{ fontSize: 18 }} />}
          onClick={() => setDrawerOpen(true)}
          aria-label="Pick date"
          style={{ color: themeColors.textSecondary }}
        />
        <div className="mobile-feeds-toolbar-title-block">
          <div className="mobile-feeds-toolbar-title" style={{ color: themeColors.text }}>
            Twitter Daily Report
          </div>
          <div className="mobile-feeds-toolbar-sub" style={{ color: themeColors.textSecondary }}>
            {selectedDate.format('YYYY-MM-DD')}
            {selectedReport?.status && (
              <>
                {' · '}
                {(STATUS_MAP[selectedReport.status] || STATUS_MAP.pending).label}
              </>
            )}
          </div>
        </div>
        <Space size={4}>
          <Button
            type="text"
            icon={<ReloadOutlined />}
            onClick={() => loadReports()}
            loading={loading}
            style={{ color: themeColors.textSecondary }}
            aria-label="Refresh list"
          />
          <Button
            type="primary"
            size="small"
            shape="round"
            icon={<ThunderboltOutlined />}
            onClick={() => handleGenerate(false)}
            loading={generating}
            aria-label="Generate report"
          />
        </Space>
      </div>

      <div
        style={{
          flex: 1,
          minHeight: 0,
          overflow: 'auto',
          padding: `12px max(12px, env(safe-area-inset-left, 0px)) max(28px, env(safe-area-inset-bottom, 0px)) max(12px, env(safe-area-inset-right, 0px))`,
          background: themeColors.cardBg,
        }}
      >
        {detailLoading ? (
          <div className="mobile-feeds-spin-wrap">
            <Spin size="large" tip="Loading report..." />
          </div>
        ) : selectedReport?.report_content ? (
          <div style={{ maxWidth: 720, margin: '0 auto' }}>
            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'center',
                marginBottom: 14,
                flexWrap: 'wrap',
                gap: 8,
              }}
            >
              <Space wrap>
                <Tag color={STATUS_MAP[selectedReport.status]?.color}>
                  {selectedReport.report_date}
                </Tag>
                {selectedReport.metadata?.tweet_count != null && (
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    {selectedReport.metadata.tweet_count} tweets · {selectedReport.metadata.author_count} authors
                  </Text>
                )}
              </Space>
              <Button
                size="small"
                icon={<ReloadOutlined />}
                onClick={() => handleGenerate(true)}
                loading={generating}
              >
                Regenerate
              </Button>
            </div>
            <MarkdownRenderer
              content={selectedReport.report_content}
              variant={isDark ? 'dark' : 'light'}
            />
          </div>
        ) : selectedReport?.status === 'failed' ? (
          <div style={{ padding: '48px 16px', textAlign: 'center' }}>
            <Empty description={selectedReport.error_message || 'Report generation failed'}>
              <Button type="primary" onClick={() => handleGenerate(true)}>
                Retry
              </Button>
            </Empty>
          </div>
        ) : (
          <div style={{ padding: '48px 16px', textAlign: 'center' }}>
            <Empty
              description={
                <span style={{ color: themeColors.textSecondary }}>
                  No report for this date — open the menu to pick another day, or generate one.
                </span>
              }
            >
              <Button
                type="primary"
                icon={<ThunderboltOutlined />}
                onClick={() => handleGenerate(false)}
                loading={generating}
              >
                Generate for {selectedDate.format('YYYY-MM-DD')}
              </Button>
            </Empty>
          </div>
        )}
      </div>

      <Drawer
        title={<span style={{ color: themeColors.text }}>Daily reports</span>}
        placement="left"
        width="min(100%, 360px)"
        onClose={() => setDrawerOpen(false)}
        open={drawerOpen}
        rootClassName={isDark ? 'mobile-feeds-drawer--dark' : 'mobile-feeds-drawer--light'}
        styles={{
          content: { background: themeColors.cardBg },
          header: {
            background: themeColors.cardBg,
            borderBottom: `1px solid ${borderColor}`,
          },
          body: {
            background: themeColors.cardBg,
            paddingTop: 8,
            paddingLeft: 0,
            paddingRight: 0,
            paddingBottom: 8,
            overflowY: 'auto',
          },
          ...(isDark ? { mask: { background: 'rgba(0, 0, 0, 0.65)' } } : {}),
        }}
      >
        <div className="mobile-feeds-drawer-body-pad">
          <div style={{ marginBottom: 12 }}>
            <Text strong style={{ display: 'block', marginBottom: 8, color: themeColors.text }}>
              <CalendarOutlined style={{ marginRight: 6 }} />
              Select date
            </Text>
            <DatePicker
              value={selectedDate}
              onChange={(d) => {
                if (d) {
                  handleDateChange(d);
                  setDrawerOpen(false);
                }
              }}
              style={{ width: '100%' }}
              allowClear={false}
            />
          </div>

          <Button
            type="primary"
            icon={<ThunderboltOutlined />}
            onClick={() => {
              handleGenerate(false);
            }}
            loading={generating}
            block
            style={{ marginBottom: 16 }}
          >
            {generating ? 'Generating…' : 'Generate report'}
          </Button>

          <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 8 }}>
            History
          </Text>

          <List
            loading={loading}
            dataSource={reports}
            locale={{
              emptyText: (
                <Empty
                  description={
                    <span style={{ color: themeColors.textSecondary }}>No reports yet</span>
                  }
                  image={Empty.PRESENTED_IMAGE_SIMPLE}
                />
              ),
            }}
            renderItem={(item) => {
              const status = STATUS_MAP[item.status] || STATUS_MAP.pending;
              const isSelected = selectedReport?.id === item.id;
              const meta = item.metadata;
              return (
                <List.Item style={{ padding: '6px 0', border: 'none' }}>
                  <div
                    className={`mobile-feeds-drawer-sub${
                      isSelected ? ' mobile-feeds-drawer-sub--selected' : ''
                    }`}
                    style={{
                      width: '100%',
                      cursor: 'pointer',
                      backgroundColor: isSelected ? themeColors.selectedItemBg : 'transparent',
                    }}
                    onClick={() => handleSelectReport(item)}
                    role="button"
                    tabIndex={0}
                    onKeyDown={(e) => e.key === 'Enter' && handleSelectReport(item)}
                  >
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div
                        style={{
                          display: 'flex',
                          justifyContent: 'space-between',
                          alignItems: 'center',
                          gap: 8,
                        }}
                      >
                        <Text strong style={{ fontSize: 14, color: themeColors.text }}>
                          {item.report_date}
                        </Text>
                        <Tag color={status.color} icon={status.icon} style={{ margin: 0, fontSize: 11 }}>
                          {status.label}
                        </Tag>
                      </div>
                      {meta && item.status === 'completed' && (
                        <Text type="secondary" style={{ fontSize: 11, display: 'block', marginTop: 4 }}>
                          {meta.tweet_count} tweets · {meta.author_count} authors
                        </Text>
                      )}
                      {item.status === 'failed' && item.error_message && (
                        <Text type="danger" style={{ fontSize: 11, display: 'block', marginTop: 4 }} ellipsis>
                          {item.error_message}
                        </Text>
                      )}
                    </div>
                  </div>
                </List.Item>
              );
            }}
          />
        </div>
      </Drawer>
    </div>
  );
};

const MobileReportsPage: React.FC = () => {
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
        <MobileReportsPageInner />
      </App>
    </ConfigProvider>
  );
};

export default MobileReportsPage;
