import React, { useState, useEffect, useCallback, useRef } from 'react';
import {
  Card,
  Space,
  ConfigProvider,
  theme as antdTheme,
  Typography,
  Button,
  Select,
  InputNumber,
  message,
  Spin,
  Input,
  Tabs,
  Modal,
  Tag,
} from 'antd';
import {
  FileTextOutlined,
  ReloadOutlined,
  ClearOutlined,
  DownloadOutlined,
  DashboardOutlined,
  UnorderedListOutlined,
  LineChartOutlined,
  AppstoreOutlined,
  SearchOutlined,
  ExclamationCircleOutlined,
} from '@ant-design/icons';
import { useSearchParams } from 'react-router-dom';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useDeviceDetection } from '../hooks/useDeviceDetection';
import { getLogList, readLog, clearLog, downloadLog, LogFileInfo } from '../services/logs';
import MonitoringPanel from '../components/MonitoringPanel';
import TaskExecutionHistory from '../components/TaskExecutionHistory';
import LLMMonitoringStatistics from '../components/monitoring/LLMMonitoringStatistics';
import OverviewPanel from '../components/monitoring/OverviewPanel';

const { Title, Text } = Typography;
const { TextArea } = Input;

const MONITORING_TABS = ['overview', 'logs', 'infrastructure', 'scheduler', 'llm'] as const;
type MonitoringTabKey = (typeof MONITORING_TABS)[number];

const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    codeBlockBg: '#f5f5f5',
    selectedBorder: '#1890ff',
    inputBg: undefined as string | undefined,
    hoverBg: undefined as string | undefined,
  },
  dark: {
    pageBg: '#1E202B',
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    text: '#FFFFFF',
    textSecondary: '#A0A3AD',
    codeBlockBg: '#1E202B',
    selectedBorder: '#2196F3',
    inputBg: '#1E202B',
    hoverBg: 'rgba(255, 255, 255, 0.06)',
  },
} as const;

const CARD_STYLE_BEHANCE = {
  borderRadius: 12,
  boxShadow: '0 4px 24px rgba(0, 0, 0, 0.12)',
  border: '1px solid #f0f0f0',
};
const CARD_STYLE_BEHANCE_DARK = {
  borderRadius: 12,
  boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)',
  border: '1px solid rgba(255, 255, 255, 0.06)',
};

const LOG_LEVEL_OPTIONS = [
  { label: 'All', value: '' },
  { label: 'ERROR', value: 'ERROR' },
  { label: 'WARNING', value: 'WARNING' },
  { label: 'INFO', value: 'INFO' },
  { label: 'DEBUG', value: 'DEBUG' },
];

const AUTO_REFRESH_OPTIONS = [
  { label: 'Off', value: 0 },
  { label: '10s', value: 10 },
  { label: '30s', value: 30 },
  { label: '60s', value: 60 },
];

/** Color-code log levels inline using simple span replacement */
function colorizeLogContent(content: string, isDark: boolean): React.ReactNode {
  if (!content) return null;
  // Split on lines and render each with color
  const lines = content.split('\n');
  return (
    <>
      {lines.map((line, i) => {
        let color: string | undefined;
        if (line.includes('[ERROR]') || line.includes('/ERROR]')) color = isDark ? '#ff7875' : '#cf1322';
        else if (line.includes('[WARNING]') || line.includes('/WARNING]')) color = isDark ? '#ffc53d' : '#d46b08';
        else if (line.includes('[DEBUG]') || line.includes('/DEBUG]')) color = isDark ? '#8c8c8c' : '#595959';
        return (
          <React.Fragment key={i}>
            <span style={color ? { color } : undefined}>{line}</span>
            {i < lines.length - 1 && '\n'}
          </React.Fragment>
        );
      })}
    </>
  );
}

const MonitoringPage: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const { isMobile } = useDeviceDetection();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const cardStyle = isDark ? CARD_STYLE_BEHANCE_DARK : CARD_STYLE_BEHANCE;

  const rawTab = searchParams.get('tab');
  const activeKey: MonitoringTabKey = MONITORING_TABS.includes(rawTab as MonitoringTabKey)
    ? (rawTab as MonitoringTabKey)
    : 'overview';

  const handleTabChange = (key: string) => {
    const k = MONITORING_TABS.includes(key as MonitoringTabKey) ? (key as MonitoringTabKey) : 'overview';
    if (k === 'overview') {
      setSearchParams({}, { replace: true });
    } else {
      setSearchParams({ tab: k }, { replace: true });
    }
  };

  // ─── Log list (dynamic from API) ────────────────────────────────────────
  const [logFileList, setLogFileList] = useState<LogFileInfo[]>([]);
  const [selectedLogType, setSelectedLogType] = useState<string>('app');

  useEffect(() => {
    getLogList()
      .then((resp) => {
        const available = resp.logs.filter((l) => l.exists);
        setLogFileList(available);
        if (available.length > 0 && !available.find((l) => l.type === selectedLogType)) {
          setSelectedLogType(available[0].type);
        }
      })
      .catch(console.error);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ─── Log viewer state ────────────────────────────────────────────────────
  const [logContent, setLogContent] = useState<string>('');
  const [logInfo, setLogInfo] = useState<{
    file_total_lines: number;
    returned_lines: number;
    size_human: string;
    truncated: boolean;
  } | null>(null);
  const [logLoading, setLogLoading] = useState<boolean>(false);
  const [linesToRead, setLinesToRead] = useState<number | null>(1000);
  const [levelFilter, setLevelFilter] = useState<string>('');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [autoRefreshSec, setAutoRefreshSec] = useState<number>(0);
  const autoRefreshRef = useRef<number | null>(null);

  // ─── Clear log confirmation state ───────────────────────────────────────
  const [clearModalOpen, setClearModalOpen] = useState(false);
  const [clearConfirmInput, setClearConfirmInput] = useState('');
  const [clearLoading, setClearLoading] = useState(false);

  const loadLogContent = useCallback(async () => {
    setLogLoading(true);
    try {
      const response = await readLog(selectedLogType, {
        lines: linesToRead || undefined,
        level: levelFilter || undefined,
        q: searchQuery || undefined,
      });
      setLogContent(response.content);
      setLogInfo({
        file_total_lines: response.file_total_lines,
        returned_lines: response.returned_lines,
        size_human: response.size_human,
        truncated: response.truncated,
      });
    } catch (error) {
      message.error('Failed to load log content');
      console.error(error);
    } finally {
      setLogLoading(false);
    }
  }, [selectedLogType, linesToRead, levelFilter, searchQuery]);

  // Initial load when tab becomes active
  useEffect(() => {
    if (activeKey !== 'logs') return;
    loadLogContent();
  }, [activeKey, loadLogContent]);

  // Auto-refresh interval
  useEffect(() => {
    if (autoRefreshRef.current !== null) {
      window.clearInterval(autoRefreshRef.current);
      autoRefreshRef.current = null;
    }
    if (activeKey === 'logs' && autoRefreshSec > 0) {
      autoRefreshRef.current = window.setInterval(() => {
        loadLogContent();
      }, autoRefreshSec * 1000);
    }
    return () => {
      if (autoRefreshRef.current !== null) {
        window.clearInterval(autoRefreshRef.current);
        autoRefreshRef.current = null;
      }
    };
  }, [activeKey, autoRefreshSec, loadLogContent]);

  const handleClearLog = async () => {
    if (clearConfirmInput !== selectedLogType) {
      message.error('Input does not match the log type');
      return;
    }
    setClearLoading(true);
    try {
      await clearLog(selectedLogType);
      message.success('Log has been cleared');
      setClearModalOpen(false);
      setClearConfirmInput('');
      loadLogContent();
    } catch (error) {
      message.error('Failed to clear log');
      console.error(error);
    } finally {
      setClearLoading(false);
    }
  };

  const handleDownloadLog = async () => {
    try {
      await downloadLog(selectedLogType);
      message.success('Download started');
    } catch (error) {
      message.error('Failed to download log');
      console.error(error);
    }
  };

  const llmTabActive = activeKey === 'llm';
  const infrastructureTabActive = activeKey === 'infrastructure';
  const schedulerTabActive = activeKey === 'scheduler';
  const overviewTabActive = activeKey === 'overview';

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        components: {
          Table: {
            cellFontSize: 12,
            cellFontSizeMD: 12,
            cellFontSizeSM: 12,
          },
        },
        ...(isDark && {
          token: {
            colorBgContainer: colors.cardBg,
            colorBgElevated: colors.cardBg,
            colorBorderSecondary: colors.cardBorder,
            colorPrimary: '#2196F3',
            colorText: colors.text,
            colorTextSecondary: colors.textSecondary,
            colorFillQuaternary: colors.inputBg ?? '#1E202B',
            colorBgLayout: colors.pageBg,
            borderRadius: 8,
            borderRadiusLG: 12,
          },
        }),
      }}
    >
      <div
        style={{
          padding: isMobile ? '12px 12px 28px' : '12px 40px 32px',
          backgroundColor: colors.pageBg,
          minHeight: '100%',
          boxSizing: 'border-box',
        }}
      >
        <div style={{ marginBottom: isMobile ? 14 : 20 }}>
          <Title level={isMobile ? 3 : 2} style={{ margin: 0, marginBottom: 4, color: colors.text }}>
            <DashboardOutlined style={{ marginRight: 8 }} />
            Monitoring
          </Title>
          <Text style={{ margin: 0, display: 'block', color: colors.textSecondary }}>
            System overview, logs, infrastructure queues, scheduler history, and LLM statistics (admin only)
          </Text>
        </div>

        <Tabs
          activeKey={activeKey}
          onChange={handleTabChange}
          items={[
            // ── Overview ──────────────────────────────────────────────────
            {
              key: 'overview',
              label: (
                <Space>
                  <AppstoreOutlined />
                  Overview
                </Space>
              ),
              children: (
                <OverviewPanel
                  isDark={isDark}
                  colors={colors}
                  cardStyle={cardStyle}
                  active={overviewTabActive}
                />
              ),
            },

            // ── System Logs ──────────────────────────────────────────────
            {
              key: 'logs',
              label: (
                <Space>
                  <FileTextOutlined />
                  System Logs
                </Space>
              ),
              children: (
                <Card
                  style={{ ...cardStyle, overflow: 'hidden' }}
                  bodyStyle={{ padding: isMobile ? '14px 12px' : '20px 24px' }}
                >
                  {/* Controls row */}
                  <div style={{ marginBottom: 16 }}>
                    <Space
                      wrap
                      size={isMobile ? 'small' : 'middle'}
                      direction={isMobile ? 'vertical' : 'horizontal'}
                      style={{ width: isMobile ? '100%' : undefined }}
                    >
                      {/* Log type selector (dynamic) */}
                      <Space size="small">
                        <span style={{ color: colors.text }}>Log File:</span>
                        <Select
                          style={{ width: isMobile ? '100%' : 240 }}
                          value={selectedLogType}
                          onChange={(v) => { setSelectedLogType(v); }}
                          options={logFileList.map((l) => ({
                            label: `${l.type} (${l.size_human})`,
                            value: l.type,
                          }))}
                          placeholder="Loading..."
                        />
                      </Space>

                      {/* Lines to read */}
                      <Space size="small">
                        <span style={{ color: colors.text }}>Read Lines:</span>
                        <InputNumber
                          style={{ width: isMobile ? '100%' : 110 }}
                          value={linesToRead}
                          onChange={setLinesToRead}
                          min={0}
                          max={50000}
                          placeholder="All"
                        />
                      </Space>

                      {/* Level filter */}
                      <Space size="small">
                        <span style={{ color: colors.text }}>Level:</span>
                        <Select
                          style={{ width: 120 }}
                          value={levelFilter}
                          onChange={setLevelFilter}
                          options={LOG_LEVEL_OPTIONS}
                        />
                      </Space>

                      {/* Keyword search */}
                      <Input
                        placeholder="Search keyword"
                        prefix={<SearchOutlined />}
                        value={searchQuery}
                        onChange={(e) => setSearchQuery(e.target.value)}
                        onPressEnter={loadLogContent}
                        allowClear
                        style={{ width: isMobile ? '100%' : 180 }}
                      />

                      {/* Auto-refresh */}
                      <Space size="small">
                        <span style={{ color: colors.text }}>Auto Refresh:</span>
                        <Select
                          style={{ width: 80 }}
                          value={autoRefreshSec}
                          onChange={setAutoRefreshSec}
                          options={AUTO_REFRESH_OPTIONS}
                        />
                      </Space>
                    </Space>

                    <Space wrap size="small" style={{ marginTop: 10 }}>
                      <Button
                        type="primary"
                        icon={<ReloadOutlined />}
                        onClick={loadLogContent}
                        loading={logLoading}
                        block={isMobile}
                      >
                        Refresh
                      </Button>

                      <Button
                        icon={<DownloadOutlined />}
                        onClick={handleDownloadLog}
                        disabled={!logContent}
                        block={isMobile}
                      >
                        Download
                      </Button>

                      <Button
                        danger
                        icon={<ClearOutlined />}
                        onClick={() => { setClearConfirmInput(''); setClearModalOpen(true); }}
                        disabled={logLoading}
                        block={isMobile}
                      >
                        Clear Log
                      </Button>
                    </Space>
                  </div>

                  {/* File info badges */}
                  {logInfo && (
                    <Space wrap style={{ marginBottom: 12 }} size="small">
                      <Tag color="default">
                        {logInfo.returned_lines.toLocaleString()} lines shown
                      </Tag>
                      <Tag color="default">
                        {logInfo.file_total_lines.toLocaleString()} total lines
                      </Tag>
                      <Tag color="default">{logInfo.size_human}</Tag>
                      {logInfo.truncated && (
                        <Tag color="warning">Truncated at 50 000 lines</Tag>
                      )}
                    </Space>
                  )}

                  {/* Log viewer */}
                  <Spin spinning={logLoading}>
                    <div
                      style={{
                        fontFamily: 'Monaco, Consolas, "Courier New", monospace',
                        fontSize: 12,
                        backgroundColor: colors.codeBlockBg,
                        color: colors.text,
                        border: `1px solid ${colors.cardBorder}`,
                        borderRadius: 6,
                        padding: '10px 14px',
                        overflowY: 'auto',
                        whiteSpace: 'pre-wrap',
                        wordBreak: 'break-all',
                        height: isMobile ? 340 : 520,
                        lineHeight: '1.6',
                      }}
                    >
                      {logContent
                        ? colorizeLogContent(logContent, isDark)
                        : <span style={{ color: colors.textSecondary }}>No log content loaded. Click Refresh.</span>
                      }
                    </div>
                  </Spin>
                </Card>
              ),
            },

            // ── Infrastructure ────────────────────────────────────────────
            {
              key: 'infrastructure',
              label: (
                <Space>
                  <DashboardOutlined />
                  Infrastructure
                </Space>
              ),
              children: (
                <MonitoringPanel
                  isDark={isDark}
                  colors={colors}
                  cardStyle={cardStyle}
                  active={infrastructureTabActive}
                />
              ),
            },

            // ── Scheduler Logs ────────────────────────────────────────────
            {
              key: 'scheduler',
              label: (
                <Space>
                  <UnorderedListOutlined />
                  Scheduler Logs
                </Space>
              ),
              children: (
                <Card
                  style={{ ...cardStyle, overflow: 'hidden' }}
                  bodyStyle={{ padding: isMobile ? '14px 12px' : '20px 24px' }}
                >
                  <TaskExecutionHistory
                    height={isMobile ? 480 : 600}
                    active={schedulerTabActive}
                  />
                </Card>
              ),
            },

            // ── LLM Monitoring ────────────────────────────────────────────
            {
              key: 'llm',
              label: (
                <Space>
                  <LineChartOutlined />
                  LLM Monitoring
                </Space>
              ),
              children: (
                <div style={{ marginTop: 0 }}>
                  <LLMMonitoringStatistics active={llmTabActive} />
                </div>
              ),
            },
          ]}
        />
      </div>

      {/* ── Clear Log confirmation modal ──────────────────────────────────── */}
      <Modal
        title={
          <Space>
            <ExclamationCircleOutlined style={{ color: '#f5222d' }} />
            Danger: Clear Log File
          </Space>
        }
        open={clearModalOpen}
        onOk={handleClearLog}
        onCancel={() => { setClearModalOpen(false); setClearConfirmInput(''); }}
        okText="Confirm Clear"
        cancelText="Cancel"
        okButtonProps={{
          danger: true,
          disabled: clearConfirmInput !== selectedLogType,
          loading: clearLoading,
        }}
        destroyOnClose
      >
        <div style={{ marginBottom: 12 }}>
          <Text>
            This will permanently truncate <Text code>{selectedLogType}</Text>. This action
            cannot be undone and will destroy any unread log data.
          </Text>
        </div>
        <div style={{ marginBottom: 8 }}>
          <Text type="secondary">
            To confirm, type the log type name exactly:
          </Text>
        </div>
        <Input
          placeholder={selectedLogType}
          value={clearConfirmInput}
          onChange={(e) => setClearConfirmInput(e.target.value)}
          onPressEnter={handleClearLog}
          autoFocus
        />
      </Modal>
    </ConfigProvider>
  );
};

export default MonitoringPage;
