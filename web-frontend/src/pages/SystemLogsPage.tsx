import React, { useState, useEffect, useCallback } from 'react';
import { Card, Space, ConfigProvider, theme as antdTheme, Typography, Button, Select, InputNumber, message, Spin, Input } from 'antd';
import { FileTextOutlined, ReloadOutlined, ClearOutlined, DownloadOutlined } from '@ant-design/icons';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { readLog, clearLog, LogType } from '../services/logs';

const { Title, Text } = Typography;
const { TextArea } = Input;

// Theme colors per FRONTEND_STYLE_GUIDE §2: Light/Dark + Behance
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

// Behance-style: rounded corners, soft shadow, minimal border
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

const SystemLogsPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const cardStyle = isDark ? CARD_STYLE_BEHANCE_DARK : CARD_STYLE_BEHANCE;

  const [selectedLogType, setSelectedLogType] = useState<LogType>('app');
  const [logContent, setLogContent] = useState<string>('');
  const [logInfo, setLogInfo] = useState<{ lines: number; size_kb: number } | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [linesToRead, setLinesToRead] = useState<number | null>(1000);

  // 加载日志内容
  const loadLogContent = useCallback(async () => {
    setLoading(true);
    try {
      const response = await readLog(selectedLogType, linesToRead || undefined);
      setLogContent(response.content);
      setLogInfo({
        lines: response.lines,
        size_kb: response.size_kb,
      });
      message.success(`已加载 ${response.lines} 行日志`);
    } catch (error) {
      message.error('加载日志内容失败');
      console.error(error);
    } finally {
      setLoading(false);
    }
  }, [selectedLogType, linesToRead]);

  useEffect(() => {
    loadLogContent();
  }, [loadLogContent]);

  // 清空日志
  const handleClearLog = async () => {
    try {
      await clearLog(selectedLogType);
      message.success('Log has been cleared');
      loadLogContent(); // 重新加载
    } catch (error) {
      message.error('清空日志失败');
      console.error(error);
    }
  };

  // 下载日志
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

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
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
          padding: '12px 40px 32px',
          backgroundColor: colors.pageBg,
          minHeight: '100%',
        }}
      >
        <div style={{ marginBottom: 20 }}>
          <Title level={2} style={{ margin: 0, marginBottom: 4, color: colors.text }}>
            <FileTextOutlined style={{ marginRight: 8 }} />
            system logs
          </Title>
          <Text style={{ margin: 0, display: 'block', color: colors.textSecondary }}>
            View and manage system log files (only visible to administrators)
          </Text>
        </div>

        <Card
          style={{
            ...cardStyle,
            overflow: 'hidden',
          }}
          bodyStyle={{
            padding: '20px 24px',
          }}
        >
          {/* 控制栏 */}
          <div style={{ marginBottom: 16 }}>
            <Space wrap>
              <span style={{ color: colors.text }}>Log Type:</span>
              <Select
                style={{ width: 200 }}
                value={selectedLogType}
                onChange={setSelectedLogType}
                options={[
                  { label: 'App 日志 (app.log)', value: 'app' },
                  { label: 'Celery Worker 日志 (celery_worker.log)', value: 'celery_worker' },
                ]}
              />
              
              <span style={{ color: colors.text, marginLeft: 16 }}>Read Lines:</span>
              <InputNumber
                style={{ width: 120 }}
                value={linesToRead}
                onChange={setLinesToRead}
                min={0}
                placeholder="All"
              />
              <Text type="secondary" style={{ fontSize: 12 }}>
                (0 or leave empty to read all)
              </Text>

              <Button
                type="primary"
                icon={<ReloadOutlined />}
                onClick={loadLogContent}
                loading={loading}
              >
                Refresh
              </Button>

              <Button
                icon={<DownloadOutlined />}
                onClick={handleDownloadLog}
                disabled={!logContent}
              >
                Download
              </Button>

              <Button
                danger
                icon={<ClearOutlined />}
                onClick={handleClearLog}
                disabled={loading}
              >
                Clear Log
              </Button>
            </Space>
          </div>

          {/* 日志信息 */}
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

          {/* 日志内容 */}
          <Spin spinning={loading}>
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
              rows={30}
              placeholder="The log content will be displayed here..."
            />
          </Spin>
        </Card>
      </div>
    </ConfigProvider>
  );
};

export default SystemLogsPage;
