import React from 'react';
import { Avatar, Card, message, Skeleton, Tooltip } from 'antd';
import {
  CalendarOutlined,
  FileExcelOutlined,
  FileImageOutlined,
  FilePdfOutlined,
  FileTextOutlined,
  FileWordOutlined,
  SoundOutlined,
  VideoCameraOutlined,
} from '@ant-design/icons';
import type { ResearchReport } from '../../services/dashboard';
import ReportTickerTags, { parseReportTickers } from '../ReportTickerTags';
import { getExecutionStatusDisplay } from '../../utils/rcAgentStatus';
import '../../pages/MobileSourcesPage.css';
import './WatchlistResearchReportCards.css';

export interface WatchlistResearchCardTheme {
  cardBg: string;
  cardBorder: string;
  text: string;
  textSecondary: string;
}

function getFileTypeIconDisplay(firstFileType: string | undefined): {
  tooltip: string;
  icon: React.ReactElement;
  color: string;
} {
  const t = (firstFileType || '').toLowerCase();
  if (['pdf'].includes(t) || t.includes('pdf')) {
    return { tooltip: 'PDF', icon: <FilePdfOutlined />, color: '#f5222d' };
  }
  if (['doc', 'docx', 'word'].includes(t) || t.includes('word') || t.includes('msword')) {
    return { tooltip: 'Word', icon: <FileWordOutlined />, color: '#1890ff' };
  }
  if (['excel', 'xlsx', 'xls'].includes(t) || t.includes('excel') || t.includes('spreadsheet')) {
    return { tooltip: 'Excel', icon: <FileExcelOutlined />, color: '#52c41a' };
  }
  if (['audio'].includes(t) || t.startsWith('audio/')) {
    return { tooltip: 'Audio', icon: <SoundOutlined />, color: '#722ed1' };
  }
  if (['youtube'].includes(t)) {
    return { tooltip: 'YouTube', icon: <VideoCameraOutlined />, color: '#ff4d4f' };
  }
  if (['image', 'jpg', 'jpeg', 'png', 'gif', 'bmp'].includes(t) || t.startsWith('image/')) {
    return { tooltip: 'Image', icon: <FileImageOutlined />, color: '#fa8c16' };
  }
  const label = (firstFileType || 'File').toUpperCase();
  return { tooltip: label, icon: <FileTextOutlined />, color: '#8c8c8c' };
}

function formatFileSize(bytes: number) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

export interface WatchlistResearchReportCardsProps {
  loading?: boolean;
  reports?: ResearchReport[];
  themeColors: WatchlistResearchCardTheme;
}

const WatchlistResearchReportCards: React.FC<WatchlistResearchReportCardsProps> = ({
  loading,
  reports = [],
  themeColors,
}) => {
  const openSourcesForReport = (report: ResearchReport) => {
    if (!report.execution_id) {
      message.warning('该文件尚未关联到 Sources 中的来源任务，请从 Sources 页面手动打开。');
      return;
    }
    const base = `${window.location.origin}/files?execution=${encodeURIComponent(report.execution_id)}`;
    window.open(base, '_blank', 'noopener,noreferrer');
  };

  if (loading) {
    return (
      <div className="watchlist-research-sources-grid">
        {Array.from({ length: 6 }).map((_, i) => (
          <Card
            key={i}
            className="watchlist-research-sources-skeleton-card"
            style={{
              background: themeColors.cardBg,
              borderColor: themeColors.cardBorder,
            }}
            styles={{ body: { padding: 12 } }}
          >
            <Skeleton active paragraph={{ rows: 3 }} title={{ width: '85%' }} />
          </Card>
        ))}
      </div>
    );
  }

  return (
    <div className="watchlist-research-sources-grid">
      {reports.map(report => {
        const fileTypeDisplay = getFileTypeIconDisplay(report.file_type);
        const statusDisplay = report.rc_agent_status
          ? getExecutionStatusDisplay({
              rc_agent_status: report.rc_agent_status,
              first_file_type: report.file_type,
            })
          : { label: 'Processed', tooltip: 'File processing complete' };
        const openable = !!report.execution_id;

        return (
          <div
            key={report.id}
            role={openable ? 'button' : undefined}
            tabIndex={openable ? 0 : undefined}
            className={`mobile-sources-card${openable ? ' mobile-sources-card--openable' : ''}`}
            style={{
              background: themeColors.cardBg,
              borderColor: themeColors.cardBorder,
            }}
            onClick={() => openable && openSourcesForReport(report)}
            onKeyDown={e => {
              if (!openable) return;
              if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                openSourcesForReport(report);
              }
            }}
          >
            <div className="mobile-sources-card-header">
              <div className="mobile-sources-card-title" style={{ color: themeColors.text }}>
                {report.file_name}
              </div>
            </div>
            <div className="mobile-sources-card-meta" style={{ color: themeColors.textSecondary }}>
              <div className="mobile-sources-meta-row">
                <CalendarOutlined />
                <span>
                  {report.uploaded_at ? new Date(report.uploaded_at).toLocaleString() : '—'}
                </span>
                <span aria-hidden> · </span>
                <span>{formatFileSize(report.file_size)}</span>
              </div>
              {report.tagged_at && (
                <div className="mobile-sources-meta-row" style={{ fontSize: 10 }}>
                  Tagged: {new Date(report.tagged_at).toLocaleDateString()}
                </div>
              )}
            </div>
            {report.industry ? (
              <div className="watchlist-research-card-industry" style={{ color: themeColors.textSecondary }}>
                {report.industry}
              </div>
            ) : null}
            <div className="mobile-sources-card-footer">
              <div className="mobile-sources-card-footer-left">
                <Tooltip title={fileTypeDisplay.tooltip}>
                  <span
                    className="mobile-sources-file-type-icon"
                    style={{ color: fileTypeDisplay.color }}
                    aria-label={fileTypeDisplay.tooltip}
                  >
                    {fileTypeDisplay.icon}
                  </span>
                </Tooltip>
                <div className="mobile-sources-status-wrap">
                  <Tooltip title={statusDisplay.tooltip}>
                    <span
                      className="mobile-sources-status-text"
                      style={{ color: themeColors.textSecondary }}
                      aria-label={statusDisplay.label}
                    >
                      {statusDisplay.label}
                    </span>
                  </Tooltip>
                </div>
              </div>
              {report.share_info && (
                <div className="mobile-sources-card-footer-share">
                  <Tooltip title={`Shared by ${report.share_info.shared_by.username}`}>
                    {report.share_info.shared_by.avatar_url ? (
                      <Avatar size={18} src={report.share_info.shared_by.avatar_url} />
                    ) : (
                      <Avatar size={18} style={{ backgroundColor: '#722ed1', fontSize: 10 }}>
                        {report.share_info.shared_by.username.charAt(0).toUpperCase()}
                      </Avatar>
                    )}
                  </Tooltip>
                  <span
                    className="mobile-sources-share-divider"
                    style={{ color: themeColors.textSecondary }}
                    aria-hidden
                  >
                    |
                  </span>
                  <Avatar.Group
                    size={18}
                    max={{
                      count: 2,
                      style: { backgroundColor: '#722ed1', fontSize: 9 },
                    }}
                  >
                    {report.share_info.shared_to.map(u => (
                      <Tooltip key={u.user_id} title={u.username}>
                        {u.avatar_url ? (
                          <Avatar size={18} src={u.avatar_url} />
                        ) : (
                          <Avatar size={18} style={{ backgroundColor: '#1890ff', fontSize: 10 }}>
                            {u.username.charAt(0).toUpperCase()}
                          </Avatar>
                        )}
                      </Tooltip>
                    ))}
                  </Avatar.Group>
                </div>
              )}
            </div>
            {parseReportTickers(report.ticker).length > 0 && (
              <div className="mobile-sources-card-tickers">
                <ReportTickerTags tickers={report.ticker} maxVisible={4} />
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
};

export default WatchlistResearchReportCards;
