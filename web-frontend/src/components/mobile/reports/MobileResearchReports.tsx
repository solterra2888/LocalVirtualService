import React from 'react';
import { List, Typography, Tag, Space, Button, message } from 'antd';
import { DownloadOutlined, CalendarOutlined } from '@ant-design/icons';
import type { ResearchReport } from '../../../services/dashboard';
import { dashboardAPI } from '../../../services/dashboard';
import './MobileResearchReports.css';

const { Text } = Typography;

interface MobileResearchReportsProps {
  reports: ResearchReport[];
}

/**
 * 移动端研究报告列表组件
 */
const MobileResearchReports: React.FC<MobileResearchReportsProps> = ({ reports }) => {
  const formatDate = (dateStr?: string) => {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric'
    });
  };

  const formatFileSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  const getFileIcon = (fileType: string) => {
    const type = fileType.toLowerCase();
    if (type.includes('pdf')) return '📄';
    if (type.includes('word') || type.includes('doc')) return '📝';
    if (type.includes('excel') || type.includes('xls')) return '📊';
    return '📁';
  };

  const handleDownload = async (report: ResearchReport) => {
    try {
      message.loading({ content: 'Downloading...', key: 'download' });
      const blob = await dashboardAPI.downloadFile(report.id);
      
      // 创建下载链接
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = report.file_name;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
      
      message.success({ content: 'Downloaded successfully', key: 'download' });
    } catch (error) {
      console.error('Failed to download file:', error);
      message.error({ content: 'Failed to download file', key: 'download' });
    }
  };

  return (
    <div className="mobile-research-reports">
      <List
        dataSource={reports}
        renderItem={(report) => (
          <div className="report-card">
            <div className="report-icon">
              <span style={{ fontSize: '32px' }}>
                {getFileIcon(report.file_type)}
              </span>
            </div>

            <div className="report-content">
              <div className="report-title">
                <Text strong style={{ fontSize: '15px', lineHeight: 1.4 }}>
                  {report.file_name}
                </Text>
              </div>

              <div className="report-meta">
                <Space size={12} wrap>
                  <Space size={4}>
                    <CalendarOutlined style={{ color: '#8c8c8c', fontSize: '12px' }} />
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      {formatDate(report.uploaded_at)}
                    </Text>
                  </Space>
                  <Text type="secondary" style={{ fontSize: '12px' }}>
                    {formatFileSize(report.file_size)}
                  </Text>
                </Space>
              </div>

              {report.ticker && (
                <div className="report-tags">
                  <Tag style={{ margin: 0 }}>{report.ticker}</Tag>
                  {report.sector && (
                    <Tag style={{ margin: 0 }}>{report.sector}</Tag>
                  )}
                </div>
              )}
            </div>

            <div className="report-action">
              <Button
                type="text"
                icon={<DownloadOutlined />}
                onClick={() => handleDownload(report)}
                size="large"
                style={{ color: '#1890ff' }}
              />
            </div>
          </div>
        )}
      />
    </div>
  );
};

export default MobileResearchReports;

