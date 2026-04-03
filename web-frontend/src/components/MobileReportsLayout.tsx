import React from 'react';
import { ProfileOutlined, VideoCameraOutlined, BarChartOutlined } from '@ant-design/icons';
import { Outlet, useLocation, useNavigate } from 'react-router-dom';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import { MOBILE_TASK_PROGRESS_BAR_HEIGHT } from './mobile/MobileTaskProgressBar';

const HEADER_HEIGHT = 56;
const BOTTOM_NAV_HEIGHT = 60;

const reportTabs = [
  { key: '/reports/twitter', label: 'Twitter', icon: <ProfileOutlined /> },
  { key: '/reports/youtube', label: 'YouTube', icon: <VideoCameraOutlined /> },
  { key: '/reports/market-weekly', label: 'Market Weekly', icon: <BarChartOutlined /> },
];

const MobileReportsLayout: React.FC = () => {
  const navigate = useNavigate();
  const location = useLocation();
  const { themeMode } = useDashboardSettings();
  const { tasks } = useFileProcessing();

  const activeKey = reportTabs.find(t => location.pathname.startsWith(t.key))?.key ?? reportTabs[0].key;

  const isDark = themeMode === 'dark';
  const chromeHeight = HEADER_HEIGHT + BOTTOM_NAV_HEIGHT + (tasks.size > 0 ? MOBILE_TASK_PROGRESS_BAR_HEIGHT : 0);

  return (
    <div style={{ height: `calc(100dvh - ${chromeHeight}px)`, display: 'flex', flexDirection: 'column' }}>
      <div
        style={{
          display: 'flex',
          gap: 0,
          overflowX: 'auto',
          WebkitOverflowScrolling: 'touch',
          borderBottom: `1px solid ${isDark ? '#364152' : '#f0f0f0'}`,
          background: isDark ? '#1a2234' : '#fff',
          flexShrink: 0,
        }}
      >
        {reportTabs.map(tab => {
          const isActive = tab.key === activeKey;
          return (
            <div
              key={tab.key}
              onClick={() => navigate(tab.key)}
              style={{
                flex: 1,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                gap: 6,
                padding: '10px 12px',
                fontSize: 13,
                fontWeight: isActive ? 600 : 400,
                color: isActive
                  ? '#1890ff'
                  : isDark ? '#94a3b8' : 'rgba(0,0,0,0.55)',
                borderBottom: isActive ? '2px solid #1890ff' : '2px solid transparent',
                cursor: 'pointer',
                whiteSpace: 'nowrap',
                transition: 'all 0.2s',
              }}
            >
              {tab.icon}
              <span>{tab.label}</span>
            </div>
          );
        })}
      </div>
      <div style={{ flex: 1, minHeight: 0, overflow: 'auto' }}>
        <Outlet />
      </div>
    </div>
  );
};

export default MobileReportsLayout;
