import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Button,
  Tooltip,
  message,
  Empty,
  Spin,
  Typography,
  Space,
  DatePicker,
  Tag,
} from 'antd';
import {
  ReloadOutlined,
  LinkOutlined,
  ThunderboltOutlined,
  ShareAltOutlined,
} from '@ant-design/icons';
import type { Dayjs } from 'dayjs';
import { themeReportAPI, ThemeReportRecord } from '../services/themeReport';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

const { Text, Title } = Typography;

const ThemeReportPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';

  const [latest, setLatest] = useState<ThemeReportRecord | null>(null);
  const [loading, setLoading] = useState(false);
  const [generating, setGenerating] = useState(false);
  const [selectedDate, setSelectedDate] = useState<Dayjs | null>(null);
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
      const dateStr = selectedDate ? selectedDate.format('YYYY-MM-DD') : undefined;
      const result = await themeReportAPI.generate(dateStr);
      const label = dateStr ?? 'today';
      message.success(
        `Report generation queued for ${label} (task: ${result.task_id.slice(0, 8)}…). Refresh in a minute.`
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

      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 20 }}>
        <div>
          <Title level={4} style={{ margin: 0 }}>Theme Report</Title>
          <Text type="secondary" style={{ fontSize: 13 }}>
            Static HTML reports (Theme Details: categories &amp; constituents) saved to OSS.
            Tag your themes with <Text code>Theme Report</Text> to include them.
          </Text>
        </div>

        <Space wrap>
          {/* Date picker + Generate — grouped visually */}
          <Space.Compact>
            <DatePicker
              value={selectedDate}
              onChange={setSelectedDate}
              placeholder="Today (default)"
              format="YYYY-MM-DD"
              allowClear
              style={{ width: 160 }}
            />
            <Tooltip title={selectedDate ? `Generate report for ${selectedDate.format('YYYY-MM-DD')}` : 'Generate report for today'}>
              <Button
                type="primary"
                icon={<ThunderboltOutlined />}
                loading={generating}
                onClick={handleGenerate}
              >
                Generate
              </Button>
            </Tooltip>
          </Space.Compact>

          <Button
            icon={<ReloadOutlined />}
            onClick={handleRefresh}
            loading={loading}
          >
            Refresh
          </Button>
        </Space>
      </div>

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
            <div style={{ display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: 8 }}>
              <Text strong>Latest Report</Text>
              {latest?.is_stale && (
                <Tag color="processing">数据更新中…</Tag>
              )}
            </div>
            {latest && (
              <>
                <Text type="secondary" style={{ display: 'block', fontSize: 12, marginTop: 4 }}>
                  {latest.report_date} · {latest.theme_count} themes · {latest.category_count} categories · {latest.constituent_count} constituents
                </Text>
                {(latest.price_as_of_date ||
                  latest.generated_at_new_york ||
                  latest.generated_at_beijing ||
                  latest.scheduler_timezone_label) && (
                  <Text type="secondary" style={{ display: 'block', fontSize: 12, marginTop: 2 }}>
                    {latest.price_as_of_date && `Data as of ${latest.price_as_of_date}`}
                  </Text>
                )}
                {(latest.generated_at_new_york || latest.generated_at_beijing) && (
                  <Text type="secondary" style={{ display: 'block', fontSize: 12, marginTop: 2 }}>
                    {latest.generated_at_new_york && `Generated ${latest.generated_at_new_york}`}
                    {latest.generated_at_new_york && latest.generated_at_beijing && ' · '}
                    {latest.generated_at_beijing && latest.generated_at_beijing}
                  </Text>
                )}
                {latest.scheduler_timezone_label && (
                  <Text type="secondary" style={{ display: 'block', fontSize: 12, marginTop: 2 }}>
                    Schedule timezone: {latest.scheduler_timezone_label}
                    {latest.generated_at_scheduler_tz &&
                      ` · ${latest.generated_at_scheduler_tz}`}
                  </Text>
                )}
              </>
            )}
          </div>
          <Space>
            {latest?.share_token && (
              <Button
                size="small"
                icon={<LinkOutlined />}
                href={`/api/theme-report/shared/${latest.share_token}`}
                target="_blank"
                rel="noopener noreferrer"
              >
                Open in new tab
              </Button>
            )}
            <Tooltip title={latest?.share_token ? 'Copy public share link (no login required)' : 'No completed report to share'}>
              <Button
                size="small"
                icon={<ShareAltOutlined />}
                disabled={!latest?.share_token}
                onClick={handleShare}
              >
                Share
              </Button>
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
                : 'No completed reports yet. Tag your themes with "Theme Report", then click Generate.'
            }
          />
        )}
      </div>
    </div>
  );
};

export default ThemeReportPage;
