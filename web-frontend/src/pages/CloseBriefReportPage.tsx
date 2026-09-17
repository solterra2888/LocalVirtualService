import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Button,
  Tooltip,
  message,
  Empty,
  Spin,
  Typography,
  Space,
  Tag,
} from 'antd';
import {
  ReloadOutlined,
  LinkOutlined,
  ThunderboltOutlined,
  ShareAltOutlined,
} from '@ant-design/icons';
import {
  closeBriefReportAPI,
  CloseBriefReportRecord,
  CloseBriefStatus,
} from '../services/closeBriefReport';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

const { Text } = Typography;

const PLACEHOLDER_LABELS: Record<string, string> = {
  no_video_found: 'No The Close episode in the current window',
  no_transcript: 'Episode found but transcript is missing',
  episode_not_yet_published: 'Episode usually lands around 22:00 Beijing — not in yet',
};

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const CloseBriefReportPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';

  const [status, setStatus] = useState<CloseBriefStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [iframeKey, setIframeKey] = useState(0);
  const staleRefreshTriggered = useRef(false);

  const displayReport: CloseBriefReportRecord | null =
    status?.today_report?.status === 'completed' && status.today_report.share_token
      ? status.today_report
      : status?.latest_report || null;

  const applyStatus = useCallback((next: CloseBriefStatus) => {
    setStatus(next);
    return next;
  }, []);

  const maybeTriggerStaleRefresh = useCallback((report: CloseBriefReportRecord | null) => {
    if (
      !report ||
      report.status !== 'completed' ||
      !report.share_token ||
      staleRefreshTriggered.current
    ) {
      return;
    }
    const createdAt = report.created_at ? new Date(report.created_at) : null;
    if (createdAt && Date.now() - createdAt.getTime() > 24 * 60 * 60 * 1000) {
      staleRefreshTriggered.current = true;
      closeBriefReportAPI.generate().catch((err) => {
        console.error('Failed to queue stale report refresh:', err);
        staleRefreshTriggered.current = false;
      });
    }
  }, []);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const next = applyStatus(await closeBriefReportAPI.getStatus());
      maybeTriggerStaleRefresh(next.latest_report);
    } catch (err) {
      console.error('Failed to load Close Brief Report:', err);
      message.error('Failed to load report');
    } finally {
      setLoading(false);
    }
  }, [applyStatus, maybeTriggerStaleRefresh]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const pollUntilSettled = useCallback(async (queuedAt: number) => {
    while (Date.now() - queuedAt < 120000) {
      await sleep(3000);
      try {
        const next = applyStatus(await closeBriefReportAPI.getStatus());
        const todayDone =
          next.today_report?.status === 'completed' &&
          next.today_report.report_date === next.as_of;
        if (todayDone) {
          setIframeKey((k) => k + 1);
          return 'completed';
        }
        const updatedAt = next.today_summary?.updated_at
          ? new Date(next.today_summary.updated_at).getTime()
          : 0;
        if (updatedAt >= queuedAt - 2000 && next.today_summary?.placeholder_reason) {
          return next.today_summary.placeholder_reason;
        }
        if (next.today_report?.status === 'failed' && updatedAt >= queuedAt - 2000) {
          return 'failed';
        }
      } catch (err) {
        console.error('Failed to poll Close Brief status:', err);
      }
    }
    return 'timeout';
  }, [applyStatus]);

  const handleGenerate = async () => {
    setGenerating(true);
    try {
      const queuedAt = Date.now();
      const result = await closeBriefReportAPI.generate();
      message.info(
        `Generation queued (${result.celery_task_id.slice(0, 8)}…). Waiting for the latest episode.`
      );
      const outcome = await pollUntilSettled(queuedAt);
      if (outcome === 'completed') {
        message.success('Latest The Close Report is ready');
      } else if (outcome === 'timeout') {
        message.warning('Still running. Click refresh in a minute.');
      } else if (PLACEHOLDER_LABELS[outcome]) {
        message.warning(PLACEHOLDER_LABELS[outcome]);
      } else if (outcome === 'failed') {
        message.error('Report generation failed');
      }
    } catch (err) {
      console.error('Failed to trigger generation:', err);
      message.error('Failed to queue generation');
    } finally {
      setGenerating(false);
    }
  };

  const handleRefresh = async () => {
    await loadData();
    setIframeKey((k) => k + 1);
    message.success('Refreshed');
  };

  const handleShare = async () => {
    if (!displayReport?.share_token) return;
    const url = `${window.location.origin}/api/close-brief-report/shared/${displayReport.share_token}`;
    try {
      await navigator.clipboard.writeText(url);
      message.success('Share link copied to clipboard');
    } catch {
      message.info(`Share link: ${url}`);
    }
  };

  const bgColor = isDark ? '#0f1117' : '#f5f7fa';
  const cardBg = isDark ? '#1a1d27' : '#ffffff';
  const borderColor = isDark ? '#2e3347' : '#e8ecf2';
  const todayReason = status?.today_summary?.placeholder_reason;
  const showingStale =
    !!displayReport &&
    !!status?.as_of &&
    displayReport.report_date !== status.as_of;

  return (
    <div style={{ padding: '12px 16px', background: bgColor, flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>
      <div style={{
        background: cardBg,
        border: `1px solid ${borderColor}`,
        borderRadius: 8,
        overflow: 'hidden',
        flex: 1,
        minHeight: 0,
        display: 'flex',
        flexDirection: 'column',
      }}>
        <div style={{
          padding: '14px 16px',
          borderBottom: `1px solid ${borderColor}`,
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          gap: 12,
        }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
              <Text strong>
                {displayReport?.report_date
                  ? `Report ${displayReport.report_date}`
                  : 'Latest Report'}
              </Text>
              {showingStale && status?.as_of && (
                <Tag>Showing last completed · today {status.as_of}</Tag>
              )}
              {generating && <Tag color="processing">Generating…</Tag>}
              {todayReason && (
                <Tag color="warning">{PLACEHOLDER_LABELS[todayReason] || todayReason}</Tag>
              )}
              {status?.today_summary?.video_title && !todayReason && (
                <Tag color="blue">{status.today_summary.video_title}</Tag>
              )}
              {displayReport?.status === 'failed' && (
                <Tag color="error">Failed</Tag>
              )}
            </div>
          </div>
          <Space>
            <Tooltip title="Generate the latest episode (rolling 24h window)">
              <Button
                type="primary"
                size="small"
                icon={<ThunderboltOutlined />}
                loading={generating}
                onClick={handleGenerate}
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

            {displayReport?.share_token && (
              <Tooltip title="Open in new tab">
                <Button
                  size="small"
                  icon={<LinkOutlined />}
                  href={`/api/close-brief-report/shared/${displayReport.share_token}`}
                  target="_blank"
                  rel="noopener noreferrer"
                />
              </Tooltip>
            )}
            <Tooltip title={displayReport?.share_token ? 'Copy share link' : 'No report to share'}>
              <Button
                size="small"
                icon={<ShareAltOutlined />}
                disabled={!displayReport?.share_token}
                onClick={handleShare}
              />
            </Tooltip>
          </Space>
        </div>

        {loading ? (
          <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Spin tip="Loading…" />
          </div>
        ) : displayReport?.share_token ? (
          <iframe
            key={iframeKey}
            src={`/api/close-brief-report/shared/${displayReport.share_token}`}
            title="The Close Report"
            style={{
              width: '100%',
              height: '100%',
              minHeight: 500,
              border: 'none',
              display: 'block',
            }}
          />
        ) : (
          <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Empty
              description={
                todayReason ? (
                  PLACEHOLDER_LABELS[todayReason]
                ) : (
                  <>
                    <div>No completed reports yet.</div>
                    <div style={{ marginTop: 8, fontSize: 13 }}>
                      Click the <ThunderboltOutlined /> button to generate the latest report.
                    </div>
                  </>
                )
              }
            />
          </div>
        )}
      </div>
    </div>
  );
};

export default CloseBriefReportPage;
