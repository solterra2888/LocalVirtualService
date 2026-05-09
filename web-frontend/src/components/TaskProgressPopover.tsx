import React from 'react';
import { Progress, Tag, Space } from 'antd';
import {
  FileTextOutlined,
  SoundOutlined,
  YoutubeOutlined,
  RobotOutlined,
  LoadingOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  AppleOutlined,
} from '@ant-design/icons';
import type { TaskInfo } from '../contexts/FileProcessingContext';

interface TaskProgressPopoverProps {
  tasks: Map<string, TaskInfo>;
  themeMode: 'light' | 'dark';
  /** 手机 Drawer 内：全宽、略紧凑 */
  mobileLayout?: boolean;
}

const getStatusIcon = (status: string, isDark?: boolean) => {
  switch (status) {
    case 'completed': return <CheckCircleOutlined style={{ color: '#52c41a' }} />;
    case 'failed': return <CloseCircleOutlined style={{ color: '#ff4d4f' }} />;
    default: return <LoadingOutlined style={isDark ? { color: '#1890ff' } : undefined} />;
  }
};

const getStatusText = (status: string, currentFile?: string, fileType?: string, progress?: number) => {
  // Feed AI Analysis 状态文案
  if (fileType === 'feed_analysis') {
    if (status === 'completed') return 'AI Analysis ready';
    if (status === 'failed') return 'AI Analysis failed';
    return 'Generating AI Analysis…';
  }
  // 自动 AI 分析的中间态: NavBar 在主任务结束 + analyze_*_task 还在跑时显示
  if (status === 'ai_processing') {
    return 'AI analyzing…';
  }
  if (fileType === 'youtube') {
    if (status === 'completed') return 'Completed';
    if (status === 'failed') return 'Failed';
    if (progress !== undefined && progress >= 37 && progress < 92) {
      return 'Fun-ASR transcribing...';
    }
    return 'Fetching YouTube transcript...';
  }

  const statusMap: Record<string, string> = {
    pending: 'Waiting...',
    initializing: 'Initializing...',
    processing: 'Processing document...',
    ai_processing: 'AI analyzing...',
    saving: 'Saving results...',
    uploading: 'Uploading audio...',
    transcribing: 'Transcribing audio...',
    completed: 'Completed',
    failed: 'Failed',
  };

  if (currentFile && status === 'processing') {
    const shortName = currentFile.length > 30
      ? currentFile.substring(0, 27) + '...'
      : currentFile;
    return `Processing: ${shortName}`;
  }

  return statusMap[status] || status;
};

export const TaskProgressPopover: React.FC<TaskProgressPopoverProps> = ({ tasks, themeMode, mobileLayout }) => {
  const isDark = themeMode === 'dark';
  const taskArray = Array.from(tasks.values()).sort((a, b) => b.startTime - a.startTime);

  if (taskArray.length === 0) {
    return (
      <div style={{ 
        padding: mobileLayout ? '10px 12px' : '12px 16px',
        color: isDark ? '#94a3b8' : '#666',
        fontSize: mobileLayout ? '12px' : '13px'
      }}>
        No tasks in progress
      </div>
    );
  }

  return (
    <div style={{ 
      width: mobileLayout ? '100%' : 380, 
      maxWidth: '100%',
      maxHeight: mobileLayout ? 'min(420px, 55vh)' : 400, 
      overflowY: 'auto',
      background: isDark ? '#1a2234' : '#fff'
    }}>
      {/* Header */}
      <div style={{ 
        padding: mobileLayout ? '10px 12px' : '12px 16px',
        borderBottom: `1px solid ${isDark ? '#364152' : '#f0f0f0'}`,
        fontWeight: 600,
        fontSize: mobileLayout ? '13px' : '14px',
        color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.85)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between'
      }}>
        <span>Processing Tasks</span>
        <Tag color="blue">{taskArray.length}</Tag>
      </div>

      {/* Task List */}
      <div style={{ padding: mobileLayout ? '6px 0' : '8px 0' }}>
        {taskArray.map((task, index) => (
          <div
            key={task.taskId}
            style={{
              padding: mobileLayout ? '10px 12px' : '12px 16px',
              borderBottom: index < taskArray.length - 1 
                ? `1px solid ${isDark ? '#364152' : '#f0f0f0'}` 
                : 'none'
            }}
          >
            {/* File Name & Icon */}
            <div style={{ 
              display: 'flex', 
              alignItems: 'center', 
              gap: 8,
              marginBottom: 8 
            }}>
              {task.fileType === 'audio' ? (
                <SoundOutlined style={{ fontSize: 16, color: '#1890ff' }} />
              ) : task.fileType === 'youtube' ? (
                <YoutubeOutlined style={{ fontSize: 16, color: '#ff4444' }} />
              ) : task.fileType === 'podcast' ? (
                <AppleOutlined style={{ fontSize: 16, color: '#7224d8' }} />
              ) : task.fileType === 'feed_analysis' ? (
                <RobotOutlined style={{ fontSize: 16, color: '#722ed1' }} />
              ) : (
                <FileTextOutlined style={{ fontSize: 16, color: '#1890ff' }} />
              )}
              <span style={{ 
                flex: 1,
                fontSize: '13px',
                fontWeight: 500,
                color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.85)',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap'
              }}>
                {task.fileName}
              </span>
              {getStatusIcon(task.status.status, isDark)}
            </div>

            {/* Progress Bar */}
            <Progress
              percent={Math.round(task.status.progress)}
              status={task.status.status === 'failed' ? 'exception' : task.status.status === 'completed' ? 'success' : 'active'}
              size="small"
              strokeColor={
                task.fileType === 'feed_analysis'
                  ? (isDark ? '#b37feb' : '#722ed1')
                  : task.fileType === 'podcast'
                    ? (isDark ? '#a855f0' : '#7224d8')
                  : (isDark ? '#1890ff' : undefined)
              }
              style={{ marginBottom: 6 }}
            />

            {/* Status Text */}
            <div style={{ 
              fontSize: '12px',
              color: isDark ? '#94a3b8' : '#666',
              display: 'flex',
              alignItems: 'center',
              gap: 4
            }}>
              {getStatusText(task.status.status, task.status.current_file, task.fileType, task.status.progress)}
            </div>

            {/* Stats */}
            {(task.status.files_processed > 0 || (task.status.files_failed || 0) > 0) && (
              <div style={{ 
                marginTop: 6,
                fontSize: '11px',
                color: isDark ? '#94a3b8' : '#999'
              }}>
                <Space size={4} split="|">
                  <span>✓ {task.status.files_processed}</span>
                  {(task.status.files_failed || 0) > 0 && (
                    <span style={{ color: '#ff4d4f' }}>✗ {task.status.files_failed}</span>
                  )}
                </Space>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
};
