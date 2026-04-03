import React from 'react';
import { Tabs } from 'antd';
import { ProfileOutlined, VideoCameraOutlined, BarChartOutlined } from '@ant-design/icons';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

const reportTabs = [
  { key: '/reports/twitter', label: 'Twitter Report', icon: <ProfileOutlined /> },
  { key: '/reports/youtube', label: 'YouTube Report', icon: <VideoCameraOutlined /> },
  { key: '/reports/market-weekly', label: 'Market Weekly', icon: <BarChartOutlined /> },
];

const ReportsLayout: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { themeMode } = useDashboardSettings();

  const activeKey = reportTabs.find(t => location.pathname.startsWith(t.key))?.key ?? reportTabs[0].key;

  const isDark = themeMode === 'dark';

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      <Tabs
        activeKey={activeKey}
        onChange={(key) => navigate(key)}
        className="reports-layout-tabs"
        items={reportTabs.map(tab => ({
          key: tab.key,
          label: (
            <span>
              {tab.icon}
              <span style={{ marginLeft: 8 }}>{tab.label}</span>
            </span>
          ),
        }))}
        style={{
          padding: '0 24px',
          marginBottom: 0,
          background: isDark ? '#1a202c' : '#fff',
          borderBottom: `1px solid ${isDark ? '#364152' : '#f0f0f0'}`,
          flexShrink: 0,
        }}
        tabBarStyle={{
          marginBottom: 0,
        }}
      />
      <div style={{ flex: 1, overflow: 'auto' }}>
        <Outlet />
      </div>
    </div>
  );
};

export default ReportsLayout;
