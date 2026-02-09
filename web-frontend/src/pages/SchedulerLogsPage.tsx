import React, { useState } from 'react';
import { Card, Tabs, Space, ConfigProvider, theme as antdTheme } from 'antd';
import { ScheduleOutlined, BarChartOutlined } from '@ant-design/icons';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import DatabaseTableViewer from '../components/DatabaseTableViewer';
import TaskExecutionHistory from '../components/TaskExecutionHistory';

type SchedulerLogsTabKey = 'task_management' | 'execution_history';

const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
  },
  dark: {
    pageBg: '#1a202c',
    cardBg: '#1a2234',
    cardBorder: '#364152',
  },
} as const;

const SchedulerLogsPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const [activeTab, setActiveTab] = useState<SchedulerLogsTabKey>('execution_history');

  const handleTabChange = (key: string) => {
    const tabKey = key as SchedulerLogsTabKey;
    setActiveTab(tabKey);
  };

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: colors.cardBg,
            colorBorderSecondary: colors.cardBorder,
          },
        }),
      }}
    >
    <div style={{ padding: '24px', backgroundColor: colors.pageBg, minHeight: '100%' }}>
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        <Card>
          <Tabs
          activeKey={activeTab}
          onChange={handleTabChange}
          items={[
            {
              key: 'execution_history',
              label: (
                <Space>
                  <BarChartOutlined />
                  Execution History
                </Space>
              ),
              children: (
                <TaskExecutionHistory
                  height={600}
                />
              )
            },
            {
              key: 'task_management',
              label: (
                <Space>
                  <ScheduleOutlined />
                  Task Management
                </Space>
              ),
              children: (
                <DatabaseTableViewer />
              )
            }
          ]}
        />
      </Card>
    </Space>
    </div>
    </ConfigProvider>
  );
};

export default SchedulerLogsPage;


