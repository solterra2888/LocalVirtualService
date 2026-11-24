import React from 'react';
import { Button, Typography } from 'antd';
import { ArrowLeftOutlined } from '@ant-design/icons';
import { useNavigate, useLocation } from 'react-router-dom';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

const { Text } = Typography;

const REPORT_TITLE_MAP: Record<string, string> = {
  '/reports/twitter': 'Twitter Daily Report',
  '/reports/youtube': 'YouTube Daily Report',
  '/reports/theme': 'Theme Performance Report',
  '/reports/daily-market': 'Daily Market Report',
};

const ReportsDetailLayout: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const navigate = useNavigate();
  const location = useLocation();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';

  const title = REPORT_TITLE_MAP[location.pathname] || 'Report';

  const headerBg = isDark ? '#1a1d27' : '#ffffff';
  const borderColor = isDark ? '#2e3347' : '#e8ecf2';

  return (
    <div style={{ height: '100%', display: 'flex', flexDirection: 'column', background: isDark ? '#0f1117' : '#f5f7fa' }}>
      {/* Header with back button */}
      <div
        style={{
          background: headerBg,
          borderBottom: `1px solid ${borderColor}`,
          padding: '10px 16px',
          display: 'flex',
          alignItems: 'center',
          gap: 12,
          flexShrink: 0,
        }}
      >
        <Button
          type="text"
          size="small"
          icon={<ArrowLeftOutlined />}
          onClick={() => navigate('/reports')}
          style={{
            color: isDark ? '#94a3b8' : '#595959',
            fontSize: 13,
          }}
        >
          Back
        </Button>
        <Text
          strong
          style={{
            color: isDark ? '#e0e6ed' : '#262626',
            fontSize: 14,
          }}
        >
          {title}
        </Text>
      </div>
      {/* Content - flex container to allow child to expand */}
      <div style={{ flex: '1 1 0%', minHeight: 0, overflow: 'hidden', display: 'flex' }}>
        {children}
      </div>
    </div>
  );
};

export default ReportsDetailLayout;