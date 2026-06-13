import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Button,
  Tooltip,
  Empty,
  Spin,
  ConfigProvider,
  theme as antdTheme,
  App,
} from 'antd';
import {
  ReloadOutlined,
  LinkOutlined,
  ThunderboltOutlined,
  ShareAltOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { FEED_THEME_COLORS } from '../components/feed/feedTheme';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import {
  youtubeDailyReportsAPI,
  YouTubeDailyReport,
} from '../services/youtubeDailyReports';
import { useDailyReportStatusPoll } from '../hooks/useDailyReportStatusPoll';

const YouTubeDailyReportPageInner: React.FC = () => {
  const { message } = App.useApp();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = FEED_THEME_COLORS[themeMode];

  const [latest, setLatest] = useState<YouTubeDailyReport | null>(null);
  const [loading, setLoading] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [iframeKey, setIframeKey] = useState(0);

  const reportDateRef = useRef<string | null>(null);
  reportDateRef.current = latest?.report_date ?? null;
  const { startPoll, clearPoll, startLatestWatch, clearLatestWatch, syncLatestSnapshot } =
    useDailyReportStatusPoll();

  // ── Load latest report ───────────────────────────────────────────────────
  const loadData = useCallback(async (options?: { quiet?: boolean }) => {
    const quiet = options?.quiet === true;
    if (!quiet) setLoading(true);
    try {
      const res = await youtubeDailyReportsAPI.getLatestReport();
      if (res.success && res.data) {
        setLatest(res.data);
        syncLatestSnapshot(res.data);
      } else {
        setLatest(null);
        syncLatestSnapshot(null);
      }
    } catch (err) {
      console.error('Failed to load YouTube Daily Report:', err);
      if (!quiet) message.error('Failed to load report');
    } finally {
      if (!quiet) setLoading(false);
    }
  }, [message, syncLatestSnapshot]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  useEffect(() => {
    startLatestWatch({
      fetchLatest: youtubeDailyReportsAPI.getLatestReport,
      onUpdate: (r) => {
        setLatest(r as YouTubeDailyReport);
        if (r.status === 'completed') {
          setIframeKey((k) => k + 1);
        }
      },
    });
    return () => clearLatestWatch();
  }, [startLatestWatch, clearLatestWatch]);

  // ── Generate ───────────────────────────────────────────────────────────
  const handleGenerate = useCallback(async () => {
    clearPoll();
    setGenerating(true);
    try {
      const res = await youtubeDailyReportsAPI.generateReport(true);
      if (!res.success) {
        const errorMsg = res.error || 'Failed to trigger report generation';
        message.error(errorMsg);
        setGenerating(false);
        return;
      }
      const dateStr = res.data?.report_date ?? dayjs().format('YYYY-MM-DD');
      message.success(`Report generation started (past 24h). It may take a few minutes.`);
      startPoll(dateStr, {
        fetchReport: youtubeDailyReportsAPI.getReportByDate,
        refreshList: loadData,
        syncDetailIfViewing: () => reportDateRef.current === dateStr,
        setDetail: (r) => {
          setLatest(r as YouTubeDailyReport);
          // Auto refresh iframe when report is completed
          if (r.status === 'completed') {
            setIframeKey(k => k + 1);
          }
        },
        onDone: () => setGenerating(false),
      });
    } catch {
      message.error('Failed to trigger report generation');
      setGenerating(false);
    }
  }, [loadData, message, clearPoll, startPoll]);

  const handleRefresh = async () => {
    await loadData({ quiet: true });
    setIframeKey((k) => k + 1);
    message.success('Refreshed');
  };

  const handleShare = async () => {
    if (!latest?.share_token) {
      message.warning('This report does not have a share link yet.');
      return;
    }
    const url = `${window.location.origin}/api/reports/youtube-daily/shared/${latest.share_token}`;
    try {
      await navigator.clipboard.writeText(url);
      message.success('Share link copied to clipboard');
    } catch {
      message.info(`Share link: ${url}`);
    }
  };

  // ── Styles ────────────────────────────────────────────────────────────
  const bgColor = themeColors.pageBg;
  const cardBg = themeColors.cardBg;
  const borderColor = themeColors.cardBorder;
  return (
    <div style={{ padding: '12px 16px', background: bgColor, flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
      <div style={{
        background: cardBg,
        border: `1px solid ${borderColor}`,
        borderRadius: 8,
        overflow: 'hidden',
        position: 'relative',
        flex: 1,
        minHeight: 0,
        display: 'flex',
        flexDirection: 'column',
      }}>
        {/* Floating toolbar - right top corner */}
        <div style={{
          position: 'absolute',
          top: 16,
          right: 16,
          zIndex: 10,
          display: 'flex',
          alignItems: 'center',
          gap: 8,
          background: isDark ? 'rgba(26,29,39,0.9)' : 'rgba(255,255,255,0.9)',
          padding: '8px 12px',
          borderRadius: 6,
          boxShadow: isDark ? '0 2px 8px rgba(0,0,0,0.3)' : '0 2px 8px rgba(0,0,0,0.1)',
        }}>
          <Tooltip title="Generate report for the past 24 hours">
            <Button
              type="primary"
              size="small"
              icon={<ThunderboltOutlined />}
              loading={generating}
              onClick={handleGenerate}
              style={{ background: '#FF5C5C', borderColor: '#FF5C5C' }}
            />
          </Tooltip>

          <Tooltip title="Refresh">
            <Button
              size="small"
              icon={<ReloadOutlined />}
              onClick={handleRefresh}
              loading={loading}
            />
          </Tooltip>

          {latest?.share_token && (
            <Tooltip title="Open in new tab">
              <Button
                size="small"
                icon={<LinkOutlined />}
                href={`/api/reports/youtube-daily/shared/${latest.share_token}`}
                target="_blank"
                rel="noopener noreferrer"
              />
            </Tooltip>
          )}

          <Tooltip title={latest?.share_token ? 'Copy share link' : 'No report to share'}>
            <Button
              size="small"
              icon={<ShareAltOutlined />}
              disabled={!latest?.share_token}
              onClick={handleShare}
            />
          </Tooltip>
        </div>

        {/* Content */}
        {loading ? (
          <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Spin tip="Loading…" />
          </div>
        ) : generating || latest?.status === 'generating' || latest?.status === 'pending' ? (
          <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center' }}>
            <Spin size="large" tip="Report is being generated. Please wait…" />
            <div style={{ marginTop: 16, color: themeColors.textSecondary }}>
              This usually takes 1-2 minutes.
            </div>
          </div>
        ) : latest?.share_token ? (
          <iframe
            key={iframeKey}
            src={`/api/reports/youtube-daily/shared/${latest.share_token}?t=${iframeKey}`}
            title="YouTube Daily Report"
            style={{
              width: '100%',
              height: '100%',
              minHeight: 500,
              border: 'none',
              display: 'block',
            }}
          />
        ) : latest?.html_content ? (
          <div
            style={{
              width: '100%',
              height: '100%',
              minHeight: 500,
              overflow: 'auto',
              padding: 24,
            }}
            dangerouslySetInnerHTML={{ __html: latest.html_content }}
          />
        ) : latest?.status === 'failed' ? (
          <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Empty
              description={
                <>
                  <div>Report generation failed</div>
                  {latest.error_message && (
                    <div style={{ marginTop: 8, color: themeColors.textSecondary, fontSize: 12 }}>
                      Reason: {latest.error_message}
                    </div>
                  )}
                </>
              }
            />
          </div>
        ) : latest?.metadata?.video_count === 0 || latest?.metadata?.channel_count === 0 ? (
          <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Empty
              description={
                <>
                  <div>No videos available for this date</div>
                  <div style={{ marginTop: 8, color: themeColors.textSecondary, fontSize: 12 }}>
                    There were no videos from your subscribed channels on {latest?.report_date}.
                    <br />
                    Please check your <a href="/settings/subscriptions">YouTube subscriptions</a> or try a different date.
                  </div>
                </>
              }
            />
          </div>
        ) : (
          <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Empty
              description={
                latest
                  ? `Report status: ${latest.status}${latest.error_message ? ` — ${latest.error_message}` : ''}`
                  : 'No report yet. Click Generate to create a report for the past 24 hours.'
              }
            />
          </div>
        )}
      </div>
    </div>
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
            colorPrimary: '#FF5C5C',
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
      <App style={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0, height: '100%' }}>
        <YouTubeDailyReportPageInner />
      </App>
    </ConfigProvider>
  );
};

export default YouTubeDailyReportPage;