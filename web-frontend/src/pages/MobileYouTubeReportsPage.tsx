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
  Tooltip,
  Typography,
  theme as antdTheme,
} from 'antd';
import {
  CalendarOutlined,
  LoadingOutlined,
  MenuOutlined,
  PlayCircleOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import MarkdownRenderer from '../components/MarkdownRenderer';
import { DailyReportStatusTag } from '../components/reports/DailyReportStatusTag';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { FEED_THEME_COLORS } from '../components/feed/feedTheme';
import {
  youtubeDailyReportsAPI,
  YouTubeDailyReport,
} from '../services/youtubeDailyReports';
import { useDailyReportStatusPoll } from '../hooks/useDailyReportStatusPoll';
import './MobileFeedsPage.css';
import '../styles/mobilePageLoading.css';

const { Text } = Typography;

/**
 * Mobile Reports: YouTube Daily Report with left drawer (date + list), Feeds-like toolbar.
 */
const MobileYouTubeReportsPageInner: React.FC = () => {
  const { message } = App.useApp();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = FEED_THEME_COLORS[themeMode];

  const [reports, setReports] = useState<YouTubeDailyReport[]>([]);
  const [selectedReport, setSelectedReport] = useState<YouTubeDailyReport | null>(null);
  const [loading, setLoading] = useState(false);
  const [detailLoading, setDetailLoading] = useState(false);
  const [selectedDate, setSelectedDate] = useState<dayjs.Dayjs>(() => dayjs().subtract(1, 'day'));
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [generating, setGenerating] = useState(false);

  const selectedReportDateRef = useRef<string | null>(null);
  selectedReportDateRef.current = selectedReport?.report_date ?? null;
  const { startPoll, clearPoll, startLatestWatch, clearLatestWatch, syncLatestSnapshot } =
    useDailyReportStatusPoll();

  const loadReportDetail = useCallback(async (dateStr: string) => {
    setDetailLoading(true);
    try {
      const res = await youtubeDailyReportsAPI.getReportByDate(dateStr);
      if (res.success) {
        setSelectedReport(res.data ?? null);
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
      }
    } catch {
      if (!quiet) message.error('Failed to load report list');
    } finally {
      if (!quiet) setLoading(false);
    }
  }, [message]);

  const handleGenerate = useCallback(async () => {
    clearPoll();
    setGenerating(true);
    try {
      const res = await youtubeDailyReportsAPI.generateReport(false);
      if (!res.success) {
        message.error(res.error || 'Failed to trigger report generation');
        setGenerating(false);
        return;
      }
      const dateStr = res.data?.report_date ?? dayjs().format('YYYY-MM-DD');
      message.success(`Report generation started (past 24h). It may take a few minutes.`);
      setSelectedDate(dayjs(dateStr));
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
  }, [message, loadReports, loadReportDetail, clearPoll, startPoll]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      setDetailLoading(true);
      try {
        const res = await youtubeDailyReportsAPI.listReports(60, 0);
        if (cancelled || !res.success || !res.data) return;
        setReports(res.data.reports);
        if (res.data.reports.length > 0) {
          const latest = res.data.reports[0];
          setSelectedDate(dayjs(latest.report_date));
          const detail = await youtubeDailyReportsAPI.getReportByDate(latest.report_date);
          if (!cancelled && detail.success) {
            const data = detail.data ?? null;
            setSelectedReport(data);
            if (data) syncLatestSnapshot(data);
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
  }, [message, syncLatestSnapshot]);

  useEffect(() => {
    startLatestWatch({
      fetchLatest: youtubeDailyReportsAPI.getLatestReport,
      onUpdate: (r) => {
        void loadReports({ quiet: true });
        if (selectedReportDateRef.current === r.report_date) {
          setSelectedReport(r as YouTubeDailyReport);
        }
      },
    });
    return () => clearLatestWatch();
  }, [startLatestWatch, clearLatestWatch, loadReports]);

  const handleDateChange = (date: dayjs.Dayjs | null) => {
    if (!date) return;
    setSelectedDate(date);
    loadReportDetail(date.format('YYYY-MM-DD'));
  };

  const handleSelectReport = (report: YouTubeDailyReport) => {
    setSelectedDate(dayjs(report.report_date));
    if (report.report_content) {
      setSelectedReport(report);
    } else {
      loadReportDetail(report.report_date);
    }
    setDrawerOpen(false);
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
            YouTube Daily Report
          </div>
          <div
            className="mobile-feeds-toolbar-sub"
            style={{ color: themeColors.textSecondary }}
          >
            {selectedDate.format('YYYY-MM-DD')}
          </div>
        </div>
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            gap: 6,
            flexShrink: 0,
          }}
        >
          <button
            type="button"
            className="feed-toolbar-icon-btn"
            onClick={() => void handleGenerate()}
            disabled={generating}
            aria-label="Generate yesterday's report"
            title="Generate yesterday's report"
          >
            <span className="feed-toolbar-icon-btn__icon">
              {generating ? <LoadingOutlined spin /> : <PlayCircleOutlined />}
            </span>
          </button>
        </div>
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
          <div className="mobile-page-loading-wrap" aria-busy="true" aria-live="polite">
            <Spin size="large" />
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
              <Space wrap align="center">
                <Text strong style={{ fontSize: 13, color: themeColors.text }}>
                  {selectedReport.report_date}
                </Text>
                {selectedReport.metadata?.video_count != null && (
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    {selectedReport.metadata.video_count} videos · {selectedReport.metadata.channel_count} channels
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
          <div style={{ padding: '48px 16px', textAlign: 'center' }}>
            <Empty description={selectedReport.error_message || 'Report generation failed'} />
          </div>
        ) : (
          <div style={{ padding: '48px 16px', textAlign: 'center' }}>
            <Empty
              description={
                <span style={{ color: themeColors.textSecondary }}>
                  No report for this date. Use the play button in the toolbar to generate yesterday&apos;s report.
                </span>
              }
            />
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
                        <DailyReportStatusTag status={item.status} dark={isDark} compact />
                      </div>
                      {meta && item.status === 'completed' && (
                        <Text type="secondary" style={{ fontSize: 11, display: 'block', marginTop: 4 }}>
                          {meta.video_count} videos · {meta.channel_count} channels
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

const MobileYouTubeReportsPage: React.FC = () => {
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
        <MobileYouTubeReportsPageInner />
      </App>
    </ConfigProvider>
  );
};

export default MobileYouTubeReportsPage;
