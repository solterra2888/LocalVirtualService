import React, { useState, useEffect } from 'react';
import { Typography, Space, Tooltip } from 'antd';
import { ClockCircleOutlined } from '@ant-design/icons';
import './TimeDisplay.css';

const { Text } = Typography;

const US_TZ = 'America/New_York';
const CN_TZ = 'Asia/Shanghai';

interface TimeDisplayProps {
  compact?: boolean;
  variant?: 'default' | 'sidebar';
}

const TimeDisplay: React.FC<TimeDisplayProps> = ({ compact = false, variant = 'default' }) => {
  const [currentTime, setCurrentTime] = useState({
    us: new Date(),
    china: new Date(),
  });

  useEffect(() => {
    const updateTime = () => {
      const now = new Date();
      setCurrentTime({ us: now, china: now });
    };

    updateTime();
    const interval = setInterval(updateTime, 1000);
    return () => clearInterval(interval);
  }, []);

  const formatTime = (date: Date, timezone: string) =>
    date.toLocaleTimeString('en-US', {
      timeZone: timezone,
      hour12: false,
      hour: '2-digit',
      minute: '2-digit',
    });

  const formatDate = (date: Date, timezone: string) =>
    date.toLocaleDateString('en-US', {
      timeZone: timezone,
      month: 'short',
      day: 'numeric',
      weekday: 'short',
    });

  const usTime = formatTime(currentTime.us, US_TZ);
  const cnTime = formatTime(currentTime.china, CN_TZ);
  const usDate = formatDate(currentTime.us, US_TZ);
  const cnDate = formatDate(currentTime.china, CN_TZ);

  if (variant === 'sidebar') {
    const expandedContent = (
      <div className="time-display-sidebar">
        <Tooltip title={`${usDate} — New York`}>
          <div className="time-display-sidebar-row">
            <span className="time-display-sidebar-label">US</span>
            <span className="time-display-sidebar-value">{usTime}</span>
          </div>
        </Tooltip>
        <Tooltip title={`${cnDate} — Beijing`}>
          <div className="time-display-sidebar-row">
            <span className="time-display-sidebar-label">CN</span>
            <span className="time-display-sidebar-value">{cnTime}</span>
          </div>
        </Tooltip>
      </div>
    );

    if (compact) {
      return (
        <Tooltip
          title={
            <div className="time-display-sidebar-tooltip">
              <div>US {usTime}</div>
              <div>CN {cnTime}</div>
            </div>
          }
          placement="right"
        >
          <div className="time-display-sidebar-collapsed" aria-label={`US ${usTime}, CN ${cnTime}`}>
            <ClockCircleOutlined />
          </div>
        </Tooltip>
      );
    }

    return expandedContent;
  }

  if (compact) {
    return (
      <Space size="small">
        <ClockCircleOutlined style={{ color: '#8c8c8c' }} />
        <Space direction="vertical" size={0} style={{ lineHeight: 1 }}>
          <Text style={{ fontSize: '12px', color: '#8c8c8c' }}>US: {usTime}</Text>
          <Text style={{ fontSize: '12px', color: '#8c8c8c' }}>CN: {cnTime}</Text>
        </Space>
      </Space>
    );
  }

  return (
    <Space size="small">
      <ClockCircleOutlined style={{ color: '#8c8c8c' }} />
      <Space direction="vertical" size={0} style={{ lineHeight: 1.2 }}>
        <Tooltip title={`${usDate} - New York Time`}>
          <Text style={{ fontSize: '13px', color: '#8c8c8c' }}>🇺🇸 {usTime}</Text>
        </Tooltip>
        <Tooltip title={`${cnDate} - Beijing Time`}>
          <Text style={{ fontSize: '13px', color: '#8c8c8c' }}>🇨🇳 {cnTime}</Text>
        </Tooltip>
      </Space>
    </Space>
  );
};

export default TimeDisplay;
