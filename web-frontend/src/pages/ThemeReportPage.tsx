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
import { themeReportAPI, ThemeReportRecord } from '../services/themeReport';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

const { Text } = Typography;

const ThemeReportPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';

  const [latest, setLatest] = useState<ThemeReportRecord | null>(null);
  const [loading, setLoading] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [iframeKey, setIframeKey] = useState(0);
  const staleRefreshTriggered = useRef(false);

  const maybeTriggerStaleRefresh = useCallback((report: ThemeReportRecord | null) => {
    if (
      !report ||
      report.status !== 'completed' ||
      !report.is_stale ||
      staleRefreshTriggered.current
    ) {
      return;
    }
    staleRefreshTriggered.current = true;
    themeReportAPI.generate().catch((err) => {
      console.error('Failed to queue stale report refresh:', err);
      staleRefreshTriggered.current = false;
    });
  }, []);

  const loadData = useCallback(async () => {
    setLoading(true);
    try {
      const lat = await themeReportAPI.getLatest();
      setLatest(lat);
      maybeTriggerStaleRefresh(lat);
    } catch (err) {
      console.error('Failed to load Theme Report:', err);
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
      const result = await themeReportAPI.generate();
      message.success(
        `Report generation queued (task: ${result.task_id.slice(0, 8)}…). Refresh in a minute.`
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
    setIframeKey(k => k + 1);
    message.success('Refreshed');
  };

  const handleShare = async () => {
    if (!latest?.share_token) return;
    const url = `${window.location.origin}/api/theme-report/shared/${latest.share_token}`;
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
    <div style={{ padding: '24px 32px', background: bgColor, minHeight: '100%' }}>

      {/* Latest report */}
      <div style={{
        background: cardBg,
        border: `1px solid ${borderColor}`,
        borderRadius: 8,
        overflow: 'hidden',
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
              {latest?.is_stale && (
                <Tag color="processing">数据更新中…</Tag>
              )}
            </div>
          </div>
          <Space>
            <Tooltip title="Regenerate report">
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
                  href={`/api/theme-report/shared/${latest.share_token}`}
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
          <div style={{ padding: 48, textAlign: 'center' }}>
            <Spin tip="Loading…" />
          </div>
        ) : latest?.share_token ? (
          <iframe
            key={iframeKey}
            src={`/api/theme-report/shared/${latest.share_token}`}
            title="Theme Performance Report"
            style={{
              width: '100%',
              height: 'calc(100vh - 280px)',
              minHeight: 500,
              border: 'none',
              display: 'block',
            }}
          />
        ) : (
          <Empty
            style={{ padding: 48 }}
            description={
              latest
                ? `Report status: ${latest.status}${latest.error_message ? ` — ${latest.error_message}` : ''}`
                : <>
                    <div>No completed reports yet.</div>
                    <div style={{ marginTop: 8, fontSize: 13 }}>
                      Go to <a href="/settings/themes">Investment Theme Settings</a> to configure your themes,
                      then tag them with <Text code>Theme Report</Text> to include in the report.
                    </div>
                  </>
            }
          />
        )}
      </div>
    </div>
  );
};

export default ThemeReportPage;
