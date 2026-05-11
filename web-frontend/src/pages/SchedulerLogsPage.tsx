import React from 'react';
import { Card, ConfigProvider, theme as antdTheme, Typography } from 'antd';
import { UnorderedListOutlined } from '@ant-design/icons';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import TaskExecutionHistory from '../components/TaskExecutionHistory';

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

// Behance-style: rounded corners, soft shadow, minimal border (per FRONTEND_STYLE_GUIDE §3)
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

const { Title, Text } = Typography;

const SchedulerLogsPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const cardStyle = isDark ? CARD_STYLE_BEHANCE_DARK : CARD_STYLE_BEHANCE;

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
          padding: '12px 40px 32px',
          backgroundColor: colors.pageBg,
          minHeight: '100%',
        }}
      >
        <div style={{ marginBottom: 20 }}>
          <Title level={2} style={{ margin: 0, marginBottom: 4, color: colors.text }}>
            <UnorderedListOutlined style={{ marginRight: 8 }} />
            Scheduler Logs
          </Title>
          <Text style={{ margin: 0, display: 'block', color: colors.textSecondary }}>
            Execution overview, daily statistics, scheduled task configuration, and execution history
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
          <TaskExecutionHistory height={600} />
        </Card>
      </div>
    </ConfigProvider>
  );
};

export default SchedulerLogsPage;
