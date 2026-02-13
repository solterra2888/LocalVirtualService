import React from 'react';
import { Progress, Tag, Space } from 'antd';
import { 
  FileTextOutlined, 
  SoundOutlined, 
  LoadingOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined 
} from '@ant-design/icons';
import type { TaskInfo } from '../contexts/FileProcessingContext';

interface TaskProgressPopoverProps {
  tasks: Map<string, TaskInfo>;
  themeMode: 'light' | 'dark';
}

const getStatusIcon = (status: string, isDark?: boolean) => {
  switch (status) {
    case 'completed': return <CheckCircleOutlined style={{ color: '#52c41a' }} />;
    case 'failed': return <CloseCircleOutlined style={{ color: '#ff4d4f' }} />;
    default: return <LoadingOutlined style={isDark ? { color: '#1890ff' } : undefined} />;
  }
};

const getStatusText = (status: string, currentFile?: string) => {
  const statusMap: Record<string, string> = {
    pending: 'Waiting...',
    initializing: 'Initializing...',
    processing: 'Processing document...',
    ai_processing: 'AI analyzing...',
    saving: 'Saving results...',
    uploading: 'Uploading...',
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

export const TaskProgressPopover: React.FC<TaskProgressPopoverProps> = ({ tasks, themeMode }) => {
  const isDark = themeMode === 'dark';
  const taskArray = Array.from(tasks.values()).sort((a, b) => b.startTime - a.startTime);

  if (taskArray.length === 0) {
    return (
      <div style={{ 
        padding: '12px 16px',
        color: isDark ? '#94a3b8' : '#666',
        fontSize: '13px'
      }}>
        No tasks in progress
      </div>
    );
  }

  return (
    <div style={{ 
      width: 380, 
      maxHeight: 400, 
      overflowY: 'auto',
      background: isDark ? '#1a2234' : '#fff'
    }}>
      {/* Header */}
      <div style={{ 
        padding: '12px 16px',
        borderBottom: `1px solid ${isDark ? '#364152' : '#f0f0f0'}`,
        fontWeight: 600,
        fontSize: '14px',
        color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.85)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between'
      }}>
        <span>Processing Tasks</span>
        <Tag color="blue">{taskArray.length}</Tag>
      </div>

      {/* Task List */}
      <div style={{ padding: '8px 0' }}>
        {taskArray.map((task, index) => (
          <div
            key={task.taskId}
            style={{
              padding: '12px 16px',
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
              status={task.status.status === 'failed' ? 'exception' : 'active'}
              size="small"
              strokeColor={isDark ? '#1890ff' : undefined}
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
              {getStatusText(task.status.status, task.status.current_file)}
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
