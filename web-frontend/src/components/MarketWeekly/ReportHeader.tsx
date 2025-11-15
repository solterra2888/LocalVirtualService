import React from 'react';
import { Typography, Space, Button, Tooltip } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import type { ReportMeta } from '../../types/marketWeekly';
import './ReportHeader.css';

const { Title, Text } = Typography;

interface ReportHeaderProps {
  meta: ReportMeta;
  onRefresh?: () => void;
  refreshing?: boolean;
}

const ReportHeader: React.FC<ReportHeaderProps> = ({ meta, onRefresh, refreshing = false }) => {
  // 格式化日期为中文格式：2025年1月15日（周三）
  const formatDateChinese = (dateStr: string, includeWeekday: boolean = true) => {
    const date = new Date(dateStr);
    const year = date.getFullYear();
    const month = date.getMonth() + 1;
    const day = date.getDate();
    const weekdays = ['周日', '周一', '周二', '周三', '周四', '周五', '周六'];
    const weekday = weekdays[date.getDay()];
    
    if (includeWeekday) {
      return `${year}年${month}月${day}日（${weekday}）`;
    }
    return `${year}年${month}月${day}日`;
  };

  // 格式化生成时间（显示中国和美国时区）
  const formatGeneratedTime = (dateStr: string) => {
    const date = new Date(dateStr);
    
    // 中国时区 (Asia/Shanghai, UTC+8)
    const chinaFormatter = new Intl.DateTimeFormat('en-US', {
      timeZone: 'Asia/Shanghai',
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
      timeZoneName: 'short'
    });
    
    const chinaParts = chinaFormatter.formatToParts(date);
    const chinaDate = chinaParts.filter(p => p.type !== 'timeZoneName').map(p => p.value).join('');
    const chinaTz = chinaParts.find(p => p.type === 'timeZoneName')?.value || 'CST';
    
    // 美国东部时区 (America/New_York, EST/EDT)
    const usFormatter = new Intl.DateTimeFormat('en-US', {
      timeZone: 'America/New_York',
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      hour12: true,
      timeZoneName: 'short'
    });
    
    const usParts = usFormatter.formatToParts(date);
    const usDate = usParts.filter(p => p.type !== 'timeZoneName').map(p => p.value).join('');
    const usTz = usParts.find(p => p.type === 'timeZoneName')?.value || 'EST';
    
    return (
      <span>
        <span>China ({chinaTz}): {chinaDate}</span>
        <span style={{ margin: '0 8px', color: '#ccc' }}>|</span>
        <span>US ({usTz}): {usDate}</span>
      </span>
    );
  };

  return (
    <div className="market-weekly-header">
      <div className="header-content">
        <div className="title-section">
          <Title level={1} className="report-title">
            Market Weekly
          </Title>
          <Text className="report-subtitle">
            Global Markets Analysis & Performance Review
          </Text>
          <div style={{ marginTop: '8px', marginBottom: '4px' }}>
            <Text style={{ fontSize: '11px', color: '#666', fontStyle: 'italic' }}>
              数据截至：{formatDateChinese(meta.week_end)} • 周度对比：{formatDateChinese(meta.week_start, false)} 至 {formatDateChinese(meta.week_end, false)}（基于数据库中最新的交易日）
            </Text>
          </div>
          <div style={{ marginTop: '8px' }}>
            <Text style={{ fontSize: '11px', color: '#999' }}>
              Report Generated: 
            </Text>
            <span style={{ fontSize: '11px', color: '#999', marginLeft: '4px' }}>
              {formatGeneratedTime(meta.generated_at)}
            </span>
          </div>
        </div>

        {onRefresh && (
          <div className="refresh-section">
            <Tooltip title="Refresh report data">
              <Button
                type="text"
                icon={<ReloadOutlined spin={refreshing} />}
                onClick={onRefresh}
                disabled={refreshing}
                className="refresh-button"
              >
                Refresh
              </Button>
            </Tooltip>
          </div>
        )}
      </div>

      <div className="header-divider" />
    </div>
  );
};

export default ReportHeader;

