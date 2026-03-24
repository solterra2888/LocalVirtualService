import React from 'react';
import { Outlet } from 'react-router-dom';
import { App, ConfigProvider, theme as antdTheme } from 'antd';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { MobileFeedsProvider } from '../contexts/MobileFeedsContext';
import { FEED_THEME_COLORS } from '../components/feed/feedTheme';
import './MobileFeedsPage.css';

/**
 * Mobile Feeds shell: theme + subscription feed context + nested routes (list | article).
 */
const MobileFeedsPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = FEED_THEME_COLORS[themeMode];

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: themeColors.cardBg,
            colorBgElevated: themeColors.cardBg,
            colorBorderSecondary: themeColors.cardBorder,
            colorPrimary: '#2196F3',
            colorText: themeColors.text,
            colorTextSecondary: themeColors.textSecondary,
            colorFillQuaternary: themeColors.inputBg ?? '#1E202B',
            colorBgLayout: themeColors.pageBg,
            borderRadius: 8,
            borderRadiusLG: 12,
          },
        }),
      }}
    >
      <App>
        <MobileFeedsProvider>
          <div
            className="mobile-feeds-page"
            data-theme={themeMode}
            style={
              {
                background: themeColors.pageBg,
                ['--mobile-feeds-page-bg' as string]: themeColors.pageBg,
                ['--mobile-feeds-card-bg' as string]: themeColors.cardBg,
                ['--mobile-feeds-border' as string]: themeColors.cardBorder,
                ['--mobile-feeds-accent' as string]: themeColors.selectedBorder,
                ['--mobile-feeds-selected-bg' as string]: themeColors.selectedItemBg,
                ['--mobile-feeds-box-muted' as string]: themeColors.boxMuted,
                ['--mobile-feeds-text' as string]: themeColors.text,
                ['--mobile-feeds-text-secondary' as string]: themeColors.textSecondary,
              } as React.CSSProperties
            }
          >
            <Outlet />
          </div>
        </MobileFeedsProvider>
      </App>
    </ConfigProvider>
  );
};

export default MobileFeedsPage;
