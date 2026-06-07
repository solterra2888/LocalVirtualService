import React from 'react';
import { Result, Typography } from 'antd';
import { LineChartOutlined } from '@ant-design/icons';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

const { Title, Paragraph } = Typography;

const DailyMarketReportPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';

  const bgColor = isDark ? '#0f1117' : '#f5f7fa';
  const cardBg = isDark ? '#1a1d27' : '#ffffff';
  const borderColor = isDark ? '#2e3347' : '#e8ecf2';

  return (
    <div
      style={{
        padding: '24px 32px',
        background: bgColor,
        minHeight: '100%',
        display: 'flex',
        flexDirection: 'column',
      }}
    >
      <div
        style={{
          background: cardBg,
          border: `1px solid ${borderColor}`,
          borderRadius: 12,
          padding: 48,
          flex: 1,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
        }}
      >
        <Result
          icon={<LineChartOutlined style={{ color: '#1890ff' }} />}
          title={
            <Title level={4} style={{ color: isDark ? '#e0e6ed' : '#262626' }}>
              Daily Market Report
            </Title>
          }
          subTitle={
            <div style={{ textAlign: 'center' }}>
              <Paragraph style={{ color: isDark ? '#8a8a9a' : '#595959' }}>
                This feature is under development and will be available soon.
              </Paragraph>
              <Paragraph style={{ color: isDark ? '#6a6a7a' : '#8c8c8c', fontSize: 13 }}>
                Daily Market Report will provide:
              </Paragraph>
              <ul style={{ color: isDark ? '#6a6a7a' : '#8c8c8c', fontSize: 13, textAlign: 'left', marginTop: 16 }}>
                <li>Daily market analysis and key highlights</li>
                <li>Major index movements and sector performance</li>
                <li>Notable news and events impacting the markets</li>
                <li>Trending stocks and volume analysis</li>
              </ul>
            </div>
          }
        />
      </div>
    </div>
  );
};

export default DailyMarketReportPage;