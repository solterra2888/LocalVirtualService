import React from 'react';
import { Button, Typography } from 'antd';
import { ArrowLeftOutlined } from '@ant-design/icons';
import { useNavigate, useLocation } from 'react-router-dom';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import { MOBILE_TASK_PROGRESS_BAR_HEIGHT } from './mobile/MobileTaskProgressBar';

const { Text } = Typography;

const HEADER_HEIGHT = 56;
const BOTTOM_NAV_HEIGHT = 60;

const REPORT_TITLE_MAP: Record<string, string> = {
  '/reports/twitter': 'Twitter Daily Report',
  '/reports/youtube': 'YouTube Daily Report',
  '/reports/theme': 'Theme Performance Report',
  '/reports/daily-market': 'Daily Market Report',
};

/**
 * 移动端 Reports 详情壳：顶部「返回」按钮 + 报告标题，内容区 flex 撑满以承载
 * （复用的）桌面版报告页（iframe + 工具栏）。对齐桌面 ReportsDetailLayout 的结构，
 * 但按手机 chrome（顶部 56 + 底部导航 60 + 可选任务进度条）计算高度。
 */
const MobileReportsDetailLayout: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const navigate = useNavigate();
  const location = useLocation();
  const { themeMode } = useDashboardSettings();
  const { tasks } = useFileProcessing();
  const isDark = themeMode === 'dark';

  const title = REPORT_TITLE_MAP[location.pathname] || 'Report';
  const chromeHeight =
    HEADER_HEIGHT + BOTTOM_NAV_HEIGHT + (tasks.size > 0 ? MOBILE_TASK_PROGRESS_BAR_HEIGHT : 0);

  const headerBg = isDark ? '#1a1d27' : '#ffffff';
  const borderColor = isDark ? '#2e3347' : '#e8ecf2';

  return (
    <div
      style={{
        height: `calc(100dvh - ${chromeHeight}px)`,
        display: 'flex',
        flexDirection: 'column',
        background: isDark ? '#0f1117' : '#f5f7fa',
      }}
    >
      {/* Header with back button */}
      <div
        style={{
          background: headerBg,
          borderBottom: `1px solid ${borderColor}`,
          padding: '8px 12px',
          display: 'flex',
          alignItems: 'center',
          gap: 8,
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
      {/* Content - flex container to allow child report page to expand */}
      <div style={{ flex: '1 1 0%', minHeight: 0, overflow: 'hidden', display: 'flex' }}>
        {children}
      </div>
    </div>
  );
};

export default MobileReportsDetailLayout;
