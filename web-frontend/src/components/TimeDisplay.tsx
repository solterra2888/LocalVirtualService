import React, { useState, useEffect } from 'react';
import { Typography, Space, Tooltip } from 'antd';
import { ClockCircleOutlined } from '@ant-design/icons';

const { Text } = Typography;

interface TimeDisplayProps {
  compact?: boolean;
}

const TimeDisplay: React.FC<TimeDisplayProps> = ({ compact = false }) => {
  const [currentTime, setCurrentTime] = useState({
    us: new Date(),
    china: new Date()
  });

  useEffect(() => {
    const updateTime = () => {
      const now = new Date();
      setCurrentTime({
        us: now,
        china: now
      });
    };

    updateTime();
    const interval = setInterval(updateTime, 1000);
    return () => clearInterval(interval);
  }, []);

  const formatTime = (date: Date, timezone: string) => {
    return date.toLocaleTimeString('en-US', {
      timeZone: timezone,
      hour12: false,
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  const formatDate = (date: Date, timezone: string) => {
    return date.toLocaleDateString('en-US', {
      timeZone: timezone,
      month: 'short',
      day: 'numeric',
      weekday: 'short'
    });
  };

  if (compact) {
    return (
      <Space size="small">
        <ClockCircleOutlined style={{ color: '#8c8c8c' }} />
        <Space direction="vertical" size={0} style={{ lineHeight: 1 }}>
          <Text style={{ fontSize: '12px', color: '#8c8c8c' }}>
            US: {formatTime(currentTime.us, 'America/New_York')}
          </Text>
          <Text style={{ fontSize: '12px', color: '#8c8c8c' }}>
            CN: {formatTime(currentTime.china, 'Asia/Shanghai')}
          </Text>
        </Space>
      </Space>
    );
  }

  return (
    <Space size="small">
      <ClockCircleOutlined style={{ color: '#8c8c8c' }} />
      <Space direction="vertical" size={0} style={{ lineHeight: 1.2 }}>
        <Tooltip title={`${formatDate(currentTime.us, 'America/New_York')} - New York Time`}>
          <Text style={{ fontSize: '13px', color: '#8c8c8c' }}>
            🇺🇸 {formatTime(currentTime.us, 'America/New_York')}
          </Text>
        </Tooltip>
        <Tooltip title={`${formatDate(currentTime.china, 'Asia/Shanghai')} - Beijing Time`}>
          <Text style={{ fontSize: '13px', color: '#8c8c8c' }}>
            🇨🇳 {formatTime(currentTime.china, 'Asia/Shanghai')}
          </Text>
        </Tooltip>
      </Space>
    </Space>
  );
};

export default TimeDisplay;
