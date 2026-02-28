import React, { useState } from 'react';
import { Modal, Input, Button, message, Space, ConfigProvider, theme as antdTheme, Alert } from 'antd';
import { LinkOutlined, PlayCircleOutlined } from '@ant-design/icons';
import { filesAPI } from '../services/files';
import { getErrorMessage } from '../utils/errorHandler';
import { useFileProcessing } from '../contexts/FileProcessingContext';

interface UploadYouTubeLinkModalProps {
  visible: boolean;
  onCancel: () => void;
  themeMode: 'light' | 'dark';
}

const UploadYouTubeLinkModal: React.FC<UploadYouTubeLinkModalProps> = ({ visible, onCancel, themeMode }) => {
  const isDark = themeMode === 'dark';
  const [youtubeUrl, setYoutubeUrl] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const { addTask, updateTaskStatus } = useFileProcessing();

  // 验证 YouTube URL 格式
  const validateYouTubeUrl = (url: string): boolean => {
    if (!url.trim()) return false;
    
    const patterns = [
      /(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/|youtube\.com\/shorts\/)([a-zA-Z0-9_-]{11})/,
      /youtube\.com\/watch\?.*v=([a-zA-Z0-9_-]{11})/,
      /youtube\.com\/shorts\/([a-zA-Z0-9_-]{11})/,
    ];
    
    return patterns.some(pattern => pattern.test(url));
  };

  // 处理开始处理
  const handleStartProcessing = async () => {
    if (!youtubeUrl.trim()) {
      message.warning('Please enter a YouTube URL');
      return;
    }

    if (!validateYouTubeUrl(youtubeUrl)) {
      message.error('Invalid YouTube URL. Please provide a valid YouTube video link.');
      return;
    }

    setIsProcessing(true);
    const url = youtubeUrl.trim();

    // 关闭 Modal
    handleClose();

    try {
      // 调用 API
      const response = await filesAPI.uploadYouTubeLink(url);
      
      if (!response.task_id) {
        throw new Error('No task ID returned');
      }

      const taskId = response.task_id;
      const videoId = response.video_id || 'unknown';

      // 添加任务到全局 context
      addTask(taskId, `YouTube_${videoId}`, 'youtube');

      // 初始状态
      updateTaskStatus(taskId, {
        status: 'processing',
        progress: 10,
        current_file: url,
        files_processed: 0,
        files_failed: 0,
        total_files: 1,
        logs: [],
      });

      message.info('YouTube processing started. Fetching transcript (or falling back to Fun-ASR)...');

      // 轮询任务状态
      const pollInterval = setInterval(async () => {
        try {
          const statusResult = await filesAPI.getTaskStatus(taskId);
          
          const taskStatus = statusResult.status;
          
          if (taskStatus === 'pending' || taskStatus === 'processing') {
            // 更新进度
            updateTaskStatus(taskId, {
              status: taskStatus,
              progress: statusResult.progress || 0,
              current_file: url,
              files_processed: statusResult.files_processed || 0,
              files_failed: statusResult.files_failed || 0,
              total_files: statusResult.total_files || 1,
              logs: statusResult.logs || [],
            });
          } else if (taskStatus === 'completed') {
            // 任务完成
            clearInterval(pollInterval);
            updateTaskStatus(taskId, {
              status: 'completed',
              progress: 100,
              current_file: url,
              files_processed: 1,
              files_failed: 0,
              total_files: 1,
              logs: statusResult.logs || [],
            });
            message.success('YouTube transcript fetched successfully!');
          } else if (taskStatus === 'failed') {
            // 任务失败
            clearInterval(pollInterval);
            const errorMsg = statusResult.message || 'Task failed';
            updateTaskStatus(taskId, {
              status: 'failed',
              progress: 0,
              current_file: url,
              files_processed: 0,
              files_failed: 1,
              total_files: 1,
              logs: statusResult.logs || [],
            });
            message.error(`Processing failed: ${errorMsg}`);
          }
        } catch (pollError: any) {
          console.error('Error polling task status:', pollError);
        }
      }, 2000); // 每 2 秒轮询一次

      // 设置超时（最多 5 分钟，YouTube 处理通常很快）
      setTimeout(() => {
        clearInterval(pollInterval);
      }, 300000);

    } catch (error: any) {
      const errorMessage = getErrorMessage(error, 'Failed to process YouTube link');
      message.error(errorMessage);
      setIsProcessing(false);
    }
  };

  const handleClose = () => {
    setYoutubeUrl('');
    setIsProcessing(false);
    onCancel();
  };

  const modalStyles = isDark
    ? {
        header: {
          background: '#1a2234',
          color: '#e0e6f1',
          borderBottom: '1px solid #364152',
        } as React.CSSProperties,
        content: {
          background: '#1a2234',
          boxShadow: '0 6px 16px rgba(0,0,0,0.4)',
        } as React.CSSProperties,
        body: {
          background: '#1a2234',
          color: '#e0e6f1',
        } as React.CSSProperties,
      }
    : undefined;

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: '#212936',
            colorBorder: '#364152',
            colorBorderSecondary: '#364152',
            colorText: '#e0e6f1',
            colorTextSecondary: '#94a3b8',
            colorPrimary: '#1890ff',
          },
        }),
      }}
    >
      <Modal
        title={
          <Space>
            <LinkOutlined style={{ fontSize: '18px' }} />
            <span>Upload YouTube Link</span>
          </Space>
        }
        open={visible}
        onCancel={handleClose}
        width={600}
        centered
        footer={null}
        maskClosable
        closable
        maskStyle={{
          background: isDark ? 'rgba(0, 0, 0, 0.6)' : undefined,
          backdropFilter: 'blur(6px)',
          WebkitBackdropFilter: 'blur(6px)',
        }}
        styles={modalStyles}
      >
        <div style={{ padding: '8px 0' }}>
          {/* 说明信息 */}
          <Alert
            message="Paste a YouTube video link to extract its transcript"
            description="Supports regular videos, shorts, and embedded links. If no caption track is available, Fun-ASR will automatically transcribe the audio."
            type="info"
            showIcon
            style={{
              marginBottom: '20px',
              background: isDark ? '#1e3a5f' : '#e6f7ff',
              border: `1px solid ${isDark ? '#2b5a8f' : '#91d5ff'}`,
              color: isDark ? '#91d5ff' : 'rgba(0, 0, 0, 0.85)',
            }}
          />

          {/* URL 输入框 */}
          <div style={{ marginBottom: '20px' }}>
            <label
              style={{
                display: 'block',
                marginBottom: '8px',
                fontSize: '14px',
                fontWeight: 500,
                color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
              }}
            >
              YouTube URL
            </label>
            <Input
              placeholder="https://www.youtube.com/watch?v=..."
              value={youtubeUrl}
              onChange={(e) => setYoutubeUrl(e.target.value)}
              onPressEnter={handleStartProcessing}
              prefix={<LinkOutlined style={{ color: isDark ? '#94a3b8' : '#bfbfbf' }} />}
              size="large"
              disabled={isProcessing}
              style={{
                background: isDark ? '#212936' : '#fff',
                borderColor: isDark ? '#364152' : '#d9d9d9',
                color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
              }}
            />
          </div>

          {/* 支持的格式提示 */}
          <div
            style={{
              fontSize: '12px',
              color: isDark ? '#64748b' : 'rgba(0, 0, 0, 0.45)',
              marginBottom: '20px',
            }}
          >
            Supported formats:
            <ul style={{ margin: '4px 0 0 20px', padding: 0 }}>
              <li>https://www.youtube.com/watch?v=VIDEO_ID</li>
              <li>https://youtu.be/VIDEO_ID</li>
              <li>https://www.youtube.com/shorts/VIDEO_ID</li>
            </ul>
          </div>

          {/* 底部按钮 */}
          <div
            style={{
              display: 'flex',
              justifyContent: 'flex-end',
              alignItems: 'center',
              marginTop: '24px',
              gap: '8px',
            }}
          >
            <Button onClick={handleClose} disabled={isProcessing}>
              Cancel
            </Button>
            <Button
              type="primary"
              onClick={handleStartProcessing}
              icon={<PlayCircleOutlined />}
              disabled={!youtubeUrl.trim() || isProcessing}
              loading={isProcessing}
            >
              Start Processing
            </Button>
          </div>
        </div>
      </Modal>
    </ConfigProvider>
  );
};

export default UploadYouTubeLinkModal;
