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
  // 格式化日期显示
  const formatDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', {
      year: 'numeric',
      month: 'long',
      day: 'numeric'
    });
  };

  // 格式化生成时间
  const formatGeneratedTime = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleString('en-US', {
      year: 'numeric',
      month: 'short',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
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
        </div>

        <div className="period-section">
          <div className="period-box">
            <Space direction="vertical" size="small" style={{ width: '100%' }}>
              <div>
                <Text strong style={{ fontSize: '12px', color: '#666' }}>
                  Reporting Period
                </Text>
              </div>
              <div>
                <Text style={{ fontSize: '16px', fontWeight: 600 }}>
                  {formatDate(meta.week_start)} to {formatDate(meta.week_end)}
                </Text>
              </div>
              <div>
                <Text style={{ fontSize: '11px', color: '#999' }}>
                  Report Generated: {formatGeneratedTime(meta.generated_at)}
                </Text>
              </div>
            </Space>
          </div>

          {onRefresh && (
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
          )}
        </div>
      </div>

      <div className="header-divider" />
    </div>
  );
};

export default ReportHeader;

