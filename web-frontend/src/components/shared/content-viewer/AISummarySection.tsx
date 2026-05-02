/**
 * Shared AI Summary card — institutional “Brief digest” shell (navy–gold accent).
 *
 * Daily / Feeds both pass theme tokens via `colors` + optional header slots.
 */

import React from 'react';
import { LineChartOutlined } from '@ant-design/icons';
import { Space, Typography } from 'antd';
import { getFinanceInsightShellStyles } from './financeInsightCardShell';

const { Text } = Typography;

export interface AISummarySectionThemeColors {
  text: string;
  textMuted: string;
  divider: string;
  /** Dark mode card fill; light mode uses #fafafa to match Daily */
  cardBg: string;
  accent: string;
}

export interface AISummarySectionProps {
  themeMode: 'light' | 'dark';
  colors: AISummarySectionThemeColors;
  /** Inline meta after the brief line (pending hint, tagged_at, low-conf tag, …) */
  headerMeta?: React.ReactNode;
  /** Right side of header row (e.g. Feeds Re-tag) */
  headerActions?: React.ReactNode;
  children: React.ReactNode;
}

const AISummarySection: React.FC<AISummarySectionProps> = ({
  themeMode,
  colors,
  headerMeta,
  headerActions,
  children,
}) => {
  const shell = getFinanceInsightShellStyles(themeMode, colors);

  return (
    <div style={shell.wrapper}>
      <div style={shell.accentBar} />
      <div style={{ padding: '12px 16px 16px' }}>
        <div style={shell.headerRow}>
          <Space size={8} align="center" wrap>
            <LineChartOutlined style={{ color: shell.iconColor, fontSize: 15 }} />
            <Text style={shell.labelText}>Brief digest</Text>
            {headerMeta}
          </Space>
          {headerActions ? (
            <Space size={8} align="center" wrap style={{ flexShrink: 0 }}>
              {headerActions}
            </Space>
          ) : null}
        </div>
        {children}
      </div>
    </div>
  );
};

export default AISummarySection;
