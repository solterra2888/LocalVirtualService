import React, { useState, useEffect, useCallback } from 'react';
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
} from 'antd';
import {
  FileTextOutlined,
  ReloadOutlined,
  ClearOutlined,
  DownloadOutlined,
  DashboardOutlined,
  UnorderedListOutlined,
  LineChartOutlined,
} from '@ant-design/icons';
import { useSearchParams } from 'react-router-dom';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useDeviceDetection } from '../hooks/useDeviceDetection';
import { readLog, clearLog, LogType } from '../services/logs';
import MonitoringPanel from '../components/MonitoringPanel';
import TaskExecutionHistory from '../components/TaskExecutionHistory';
import LLMMonitoringStatistics from '../components/monitoring/LLMMonitoringStatistics';

const { Title, Text } = Typography;
const { TextArea } = Input;

const MONITORING_TABS = ['logs', 'infrastructure', 'scheduler', 'llm'] as const;
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
    : 'logs';

  const handleTabChange = (key: string) => {
    const k = MONITORING_TABS.includes(key as MonitoringTabKey) ? (key as MonitoringTabKey) : 'logs';
    if (k === 'logs') {
      setSearchParams({}, { replace: true });
    } else {
      setSearchParams({ tab: k }, { replace: true });
    }
  };

  const [selectedLogType, setSelectedLogType] = useState<LogType>('app');
  const [logContent, setLogContent] = useState<string>('');
  const [logInfo, setLogInfo] = useState<{ lines: number; size_kb: number } | null>(null);
  const [logLoading, setLogLoading] = useState<boolean>(false);
  const [linesToRead, setLinesToRead] = useState<number | null>(1000);

  const loadLogContent = useCallback(async () => {
    setLogLoading(true);
    try {
      const response = await readLog(selectedLogType, linesToRead || undefined);
      setLogContent(response.content);
      setLogInfo({
        lines: response.lines,
        size_kb: response.size_kb,
      });
      message.success(`already loaded ${response.lines} lines of log`);
    } catch (error) {
      message.error('failed to load log content');
      console.error(error);
    } finally {
      setLogLoading(false);
    }
  }, [selectedLogType, linesToRead]);

  useEffect(() => {
    if (activeKey !== 'logs') return;
    loadLogContent();
  }, [activeKey, loadLogContent]);

  const handleClearLog = async () => {
    try {
      await clearLog(selectedLogType);
      message.success('Log has been cleared');
      loadLogContent();
    } catch (error) {
      message.error('failed to clear log');
      console.error(error);
    }
  };

  const handleDownloadLog = () => {
    const blob = new Blob([logContent], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${selectedLogType}_${new Date().toISOString()}.log`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
    message.success('Log has been downloaded');
  };

  const llmTabActive = activeKey === 'llm';

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
            System logs, infrastructure queues, scheduler execution history, and LLM usage statistics (admin only)
          </Text>
        </div>

        <Tabs
          activeKey={activeKey}
          onChange={handleTabChange}
          items={[
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
                  <div style={{ marginBottom: 16 }}>
                    <Space
                      wrap
                      size={isMobile ? 'small' : 'middle'}
                      direction={isMobile ? 'vertical' : 'horizontal'}
                      style={{ width: isMobile ? '100%' : undefined }}
                    >
                      <span style={{ color: colors.text }}>Log Type:</span>
                      <Select
                        style={{ width: isMobile ? '100%' : 200 }}
                        value={selectedLogType}
                        onChange={setSelectedLogType}
                        options={[
                          { label: 'App 日志 (app.log)', value: 'app' },
                          { label: 'Celery Worker 日志 (celery_worker.log)', value: 'celery_worker' },
                        ]}
                      />

                      <span style={{ color: colors.text, ...(isMobile ? {} : { marginLeft: 16 }) }}>Read Lines:</span>
                      <InputNumber
                        style={{ width: isMobile ? '100%' : 120 }}
                        value={linesToRead}
                        onChange={setLinesToRead}
                        min={0}
                        placeholder="All"
                      />
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        (0 or leave empty to read all)
                      </Text>

                      <Space wrap size="small" style={{ width: isMobile ? '100%' : undefined }}>
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
                          onClick={handleClearLog}
                          disabled={logLoading}
                          block={isMobile}
                        >
                          Clear Log
                        </Button>
                      </Space>
                    </Space>
                  </div>

                  {logInfo && (
                    <div style={{ marginBottom: 12 }}>
                      <Space>
                        <Text type="secondary">
                          Total Lines: <strong>{logInfo.lines}</strong>
                        </Text>
                        <Text type="secondary">
                          File Size: <strong>{logInfo.size_kb.toFixed(2)} KB</strong>
                        </Text>
                      </Space>
                    </div>
                  )}

                  <Spin spinning={logLoading}>
                    <TextArea
                      value={logContent}
                      readOnly
                      style={{
                        fontFamily: 'Monaco, Consolas, "Courier New", monospace',
                        fontSize: 12,
                        backgroundColor: colors.codeBlockBg,
                        color: colors.text,
                        border: `1px solid ${colors.cardBorder}`,
                      }}
                      rows={isMobile ? 16 : 30}
                      placeholder="The log content will be displayed here..."
                    />
                  </Spin>
                </Card>
              ),
            },
            {
              key: 'infrastructure',
              label: (
                <Space>
                  <DashboardOutlined />
                  Infrastructure
                </Space>
              ),
              children: <MonitoringPanel isDark={isDark} colors={colors} cardStyle={cardStyle} />,
            },
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
                  <TaskExecutionHistory height={isMobile ? 480 : 600} />
                </Card>
              ),
            },
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
    </ConfigProvider>
  );
};

export default MonitoringPage;
