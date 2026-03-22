import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  App,
  Avatar,
  ConfigProvider,
  Spin,
  Tooltip,
  theme as antdTheme,
} from 'antd';
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
import type { ResearchReport } from '../../../services/dashboard';
import { tasksAPI, TaskExecution } from '../../../services/tasks';
import { useDashboardSettings } from '../../../contexts/DashboardSettingsContext';
import MobileSourceFileDetail from '../../MobileSourceFileDetail';
import ReportTickerTags, { parseReportTickers } from '../../ReportTickerTags';
import { getExecutionStatusDisplay } from '../../../utils/rcAgentStatus';
/** Full-screen detail layout (`.mobile-source-file-detail`) — same as Mobile Sources page */
import '../../../pages/MobileSourcesPage.css';
import './MobileResearchReports.css';

/** Matches `MobileSourcesPage` THEME_COLORS for `MobileSourceFileDetail` themeColors. */
const DETAIL_THEME = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    textMuted: 'rgba(0, 0, 0, 0.45)',
    codeBlockBg: '#f5f5f5',
    selectedBorder: '#1890ff',
    background: '#f5f5f5',
  },
  dark: {
    pageBg: '#1E202B',
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    text: '#FFFFFF',
    textSecondary: '#A0A3AD',
    textMuted: '#A0A3AD',
    codeBlockBg: '#1E202B',
    selectedBorder: '#2196F3',
    background: '#151b26',
    inputBg: '#1E202B',
  },
} as const;

/** Same as PC Sources / MobileSourcesPage `getFileTypeIconDisplay` — colored vector icons */
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

interface MobileResearchReportsProps {
  reports: ResearchReport[];
}

const MobileResearchReportsInner: React.FC<MobileResearchReportsProps> = ({ reports }) => {
  const { message: appMessage } = App.useApp();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = DETAIL_THEME[themeMode];

  const [detailExecution, setDetailExecution] = useState<TaskExecution | null>(null);
  const detailExecutionRef = useRef<TaskExecution | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);

  useEffect(() => {
    detailExecutionRef.current = detailExecution;
  }, [detailExecution]);

  const handleDetailRefreshList = useCallback(async () => {
    const id = detailExecutionRef.current?.id;
    if (!id) return;
    try {
      const fresh = await tasksAPI.getTaskExecution(id);
      setDetailExecution(fresh);
    } catch {
      setDetailExecution(null);
    }
  }, []);

  const formatFileSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  const handleReportCardClick = async (report: ResearchReport) => {
    const execId = report.execution_id;
    if (!execId) {
      appMessage.warning('This report is not linked to an analysis yet.');
      return;
    }
    setDetailLoading(true);
    try {
      const execution = await tasksAPI.getTaskExecution(execId);
      setDetailExecution(execution);
    } catch (e) {
      console.error(e);
      appMessage.error('Failed to load report detail');
    } finally {
      setDetailLoading(false);
    }
  };

  return (
    <div className="mobile-research-reports-outer">
      {detailLoading && (
        <div className="mobile-research-reports-detail-loading" aria-busy="true" aria-live="polite">
          <Spin size="large" />
        </div>
      )}
      <div className="mobile-research-reports">
        {reports.map(report => {
          const openable = !!report.execution_id;
          const fileTypeDisplay = getFileTypeIconDisplay(report.file_type);
          const statusDisplay = report.rc_agent_status
            ? getExecutionStatusDisplay({
                rc_agent_status: report.rc_agent_status,
                first_file_type: report.file_type,
              })
            : { label: 'Processed', tooltip: 'File processing complete' };
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
              onClick={() => openable && void handleReportCardClick(report)}
              onKeyDown={e => {
                if (!openable) return;
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault();
                  void handleReportCardClick(report);
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
                    {report.uploaded_at
                      ? new Date(report.uploaded_at).toLocaleString()
                      : '—'}
                  </span>
                  <span aria-hidden> · </span>
                  <span>{formatFileSize(report.file_size)}</span>
                </div>
              </div>
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
                        onClick={e => e.stopPropagation()}
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
                  <ReportTickerTags tickers={report.ticker} maxVisible={3} />
                </div>
              )}
            </div>
          );
        })}
      </div>

      {detailExecution && (
        <div className="mobile-research-reports-detail-root">
          <MobileSourceFileDetail
            execution={detailExecution}
            themeColors={themeColors}
            isDark={isDark}
            onClose={() => setDetailExecution(null)}
            onRefreshList={handleDetailRefreshList}
          />
        </div>
      )}
    </div>
  );
};

const MobileResearchReports: React.FC<MobileResearchReportsProps> = (props) => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = DETAIL_THEME[themeMode];

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
            colorBgLayout: themeColors.pageBg,
            borderRadius: 8,
            borderRadiusLG: 12,
          },
        }),
      }}
    >
      <App>
        <MobileResearchReportsInner {...props} />
      </App>
    </ConfigProvider>
  );
};

export default MobileResearchReports;
