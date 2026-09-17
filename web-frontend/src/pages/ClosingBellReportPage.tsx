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
import { closingBellReportAPI, ClosingBellReportRecord } from '../services/closingBellReport';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

const { Text } = Typography;

const ClosingBellReportPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';

  const [latest, setLatest] = useState<ClosingBellReportRecord | null>(null);
  const [loading, setLoading] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [iframeKey, setIframeKey] = useState(0);
  const staleRefreshTriggered = useRef(false);

  // 报告陈旧时自动触发刷新（>24h；任务内部幂等——摘要已存在则跳过，
  // 仅在后置钩子中按新鲜度补渲染报告，代价极低）
  const maybeTriggerStaleRefresh = useCallback((report: ClosingBellReportRecord | null) => {
    if (
      !report ||
      report.status !== 'completed' ||
      !report.share_token ||
      staleRefreshTriggered.current
    ) {
      return;
    }
    const createdAt = report.created_at ? new Date(report.created_at) : null;
    if (createdAt && (Date.now() - createdAt.getTime()) > 24 * 60 * 60 * 1000) {
      staleRefreshTriggered.current = true;
      closingBellReportAPI.generate().catch((err) => {
        console.error('Failed to queue stale report refresh:', err);
        staleRefreshTriggered.current = false;
      });
    }
  }, []);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const lat = await closingBellReportAPI.getLatest();
      setLatest(lat);
      maybeTriggerStaleRefresh(lat);
    } catch (err) {
      console.error('Failed to load Closing Bell Report:', err);
      message.error('Failed to load report');
    } finally {
      setLoading(false);
    }
  }, [maybeTriggerStaleRefresh]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  const handleGenerate = async () => {
    setGenerating(true);
    try {
      const result = await closingBellReportAPI.generate();
      message.success(
        `Report generation queued (task: ${result.celery_task_id.slice(0, 8)}…). Refresh in a minute.`
      );
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
    if (!latest?.share_token) return;
    const url = `${window.location.origin}/api/closing-bell-report/shared/${latest.share_token}`;
    try {
      await navigator.clipboard.writeText(url);
      message.success('Share link copied to clipboard');
    } catch {
      message.info(`Share link: ${url}`);
    }
  };

  // ── Styles ─────────────────────────────────────────────────
  const bgColor = isDark ? '#0f1117' : '#f5f7fa';
  const cardBg = isDark ? '#1a1d27' : '#ffffff';
  const borderColor = isDark ? '#2e3347' : '#e8ecf2';

  return (
    <div style={{ padding: '12px 16px', background: bgColor, flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }}>

      {/* Latest report */}
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
        }}>
          <div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <Text strong>Latest Report</Text>
              {latest?.status === 'generating' && (
                <Tag color="processing">Generating…</Tag>
              )}
              {latest?.status === 'failed' && (
                <Tag color="error">Failed</Tag>
              )}
            </div>
          </div>
          <Space>
            <Tooltip title="Generate today's report">
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

            {latest?.share_token && (
              <Tooltip title="Open in new tab">
                <Button
                  size="small"
                  icon={<LinkOutlined />}
                  href={`/api/closing-bell-report/shared/${latest.share_token}`}
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
          </Space>
        </div>

        {loading ? (
          <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Spin tip="Loading…" />
          </div>
        ) : latest?.share_token ? (
          <iframe
            key={iframeKey}
            src={`/api/closing-bell-report/shared/${latest.share_token}`}
            title="Closing Bell Report"
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
                latest
                  ? `Report status: ${latest.status}${latest.error_message ? ` — ${latest.error_message}` : ''}`
                  : <>
                      <div>No completed reports yet.</div>
                      <div style={{ marginTop: 8, fontSize: 13 }}>
                        Click the <ThunderboltOutlined /> button to generate today's report.
                        The scheduler also generates one each business day after the
                        CNBC Closing Bell summary completes.
                      </div>
                    </>
              }
            />
          </div>
        )}
      </div>
    </div>
  );
};

export default ClosingBellReportPage;
