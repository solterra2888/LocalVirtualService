import React, { useState, useEffect, useCallback } from 'react';
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
  ShareAltOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons';
import {
  morningBriefReportAPI,
  MorningBriefReportRecord,
  MorningBriefStatus,
} from '../services/morningBriefReport';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

const { Text } = Typography;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Morning Brief（facts-report 事实/观点分离版晨报）页面 — 只读。
 *
 * 生成由后端 Scheduler 每日 08:00（北京，周二至周六）定时触发；
 * 本页不提供任何手动生成按钮（管线极重，见服务层注释）。
 * 当日 generating 时自动轮询，完成即刷新。
 */
const MorningBriefReportPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';

  const [status, setStatus] = useState<MorningBriefStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [iframeKey, setIframeKey] = useState(0);

  const displayReport: MorningBriefReportRecord | null =
    status?.today_report?.status === 'completed' && status.today_report.share_token
      ? status.today_report
      : status?.latest_report || null;

  const todayGenerating = status?.today_report?.status === 'generating';
  const todayFailed = status?.today_report?.status === 'failed';

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      setStatus(await morningBriefReportAPI.getStatus());
    } catch (err) {
      console.error('Failed to load Morning Brief Report:', err);
      message.error('Failed to load report');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  // 当日生成中 → 自动轮询（最长 2 小时窗口内的轻量轮询，30s 间隔）
  useEffect(() => {
    if (!todayGenerating) {
      return;
    }
    let cancelled = false;
    const poll = async () => {
      while (!cancelled) {
        await sleep(30000);
        if (cancelled) {
          return;
        }
        try {
          const next = await morningBriefReportAPI.getStatus();
          if (cancelled) {
            return;
          }
          setStatus(next);
          if (next.today_report?.status !== 'generating') {
            setIframeKey((k) => k + 1);
            if (next.today_report?.status === 'completed') {
              message.success('Morning Brief is ready');
            } else if (next.today_report?.status === 'failed') {
              message.error('Morning Brief generation failed');
            }
            return;
          }
        } catch (err) {
          console.error('Failed to poll Morning Brief status:', err);
        }
      }
    };
    poll();
    return () => {
      cancelled = true;
    };
  }, [todayGenerating]);

  const handleRefresh = async () => {
    await loadData();
    setIframeKey((k) => k + 1);
    message.success('Refreshed');
  };

  const handleShare = async () => {
    if (!displayReport?.share_token) return;
    const url = `${window.location.origin}/api/morning-brief-report/shared/${displayReport.share_token}`;
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
                  ? `Morning Brief ${displayReport.report_date}`
                  : 'Morning Brief'}
              </Text>
              {showingStale && status?.as_of && (
                <Tag>Showing last completed · today {status.as_of}</Tag>
              )}
              {todayGenerating && (
                <Tag icon={<ClockCircleOutlined />} color="processing">
                  Generating · scheduler runs 08:00 Beijing
                </Tag>
              )}
              {todayFailed && (
                <Tag color="error">
                  Failed {status?.today_report?.error_message ? `· ${status.today_report.error_message.slice(0, 80)}` : ''}
                </Tag>
              )}
            </div>
          </div>
          <Space>
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
                  href={`/api/morning-brief-report/shared/${displayReport.share_token}`}
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
            src={`/api/morning-brief-report/shared/${displayReport.share_token}`}
            title="Morning Brief"
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
                todayGenerating ? (
                  <div>
                    <div>Today&apos;s Morning Brief is being generated.</div>
                    <div style={{ marginTop: 8, fontSize: 13 }}>
                      The LLM pipeline takes 40-60 minutes; this page refreshes automatically.
                    </div>
                  </div>
                ) : (
                  'No completed reports yet.'
                )
              }
            />
          </div>
        )}
      </div>
    </div>
  );
};

export default MorningBriefReportPage;
