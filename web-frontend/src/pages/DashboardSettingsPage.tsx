import React from 'react';
import { Card, Select, Typography, Space } from 'antd';
import { BgColorsOutlined } from '@ant-design/icons';
import { useDashboardSettings, type DashboardThemeMode } from '../contexts/DashboardSettingsContext';

const { Title, Text } = Typography;

const DashboardSettingsPage: React.FC = () => {
  const { themeMode, setThemeMode } = useDashboardSettings();

  return (
    <div style={{ padding: 24, maxWidth: 560 }}>
      <Title level={4} style={{ marginBottom: 24 }}>
        Theme
      </Title>
      <Card title={<span><BgColorsOutlined /> Theme</span>}>
        <Space direction="vertical" size="middle" style={{ width: '100%' }}>
          <div>
            <Text type="secondary" style={{ display: 'block', marginBottom: 8 }}>
              Light / Dark mode for all modules (Watchlist Dashboard, charts, cards, etc.)
            </Text>
            <Select
              value={themeMode}
              onChange={(v) => setThemeMode(v as DashboardThemeMode)}
              style={{ width: 160 }}
              options={[
                { value: 'light', label: 'Light' },
                { value: 'dark', label: 'Dark' },
              ]}
            />
          </div>
        </Space>
      </Card>
    </div>
  );
};

export default DashboardSettingsPage;
