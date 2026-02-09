import React, { useState } from 'react';
import { Modal, Upload, Button, message, Space, ConfigProvider, theme as antdTheme, Tag } from 'antd';
import { InboxOutlined, SoundOutlined } from '@ant-design/icons';
import { filesAPI } from '../services/files';
import { getErrorMessage } from '../utils/errorHandler';
import { useFileProcessing } from '../contexts/FileProcessingContext';

interface UploadAudioModalProps {
  visible: boolean;
  onCancel: () => void;
  themeMode: 'light' | 'dark';
}

interface AudioFile {
  uid: string;
  name: string;
  size: number;
  format: string;
  file: File;
}

const { Dragger } = Upload;

const UploadAudioModal: React.FC<UploadAudioModalProps> = ({ visible, onCancel, themeMode }) => {
  const isDark = themeMode === 'dark';
  const [audioFile, setAudioFile] = useState<AudioFile | null>(null);
  const { addTask, updateTaskStatus } = useFileProcessing();

  const formatFileSize = (bytes: number): string => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  // 处理文件选择
  const handleBeforeUpload = (file: File) => {
    if (audioFile) {
      message.warning('Upload Audio Files supports a maximum of 1 file. Please remove the current file before adding another.');
      return false;
    }

    if (!filesAPI.isAudioFile(file.name)) {
      message.error('please upload audio files. supported formats: mp3, wav, m4a, aac, flac, ogg, etc.');
      return false;
    }

    const maxSize = 1024 * 1024 * 1024; // 1GB
    if (file.size > maxSize) {
      message.error('File size exceeds 1GB limit and cannot be processed. Please select a smaller file.');
      return false;
    }

    setAudioFile({
      uid: `${Date.now()}_${file.name}`,
      name: file.name,
      size: file.size,
      format: filesAPI.getFileExtension(file.name).toUpperCase(),
      file,
    });

    return false;
  };

  // 上传并自动发送到 Fun-ASR，进度显示在 NavBar
  const handleUpload = async () => {
    if (!audioFile) {
      message.warning('no file to upload');
      return;
    }

    const filename = audioFile.name;

    onCancel();
    setAudioFile(null);

    (async () => {
      try {
        // 上传音频文件（立即返回任务 ID，不阻塞）
        const uploadResult = await filesAPI.uploadAudioFile(audioFile.file);
        if (!uploadResult.success || !uploadResult.task_id) {
          throw new Error(uploadResult.message || 'upload failed');
        }

        const taskId = uploadResult.task_id;

        // 添加任务到全局 context
        addTask(taskId, filename, 'audio');

        // 初始状态
        updateTaskStatus(taskId, {
          status: 'uploading',
          progress: 5,
          current_file: filename,
          files_processed: 0,
          files_failed: 0,
          total_files: 1,
          logs: [],
        });

        message.info('Audio file submitted, processing in background...');

        // 轮询任务状态
        const pollInterval = setInterval(async () => {
          try {
            const statusResult = await filesAPI.getTaskStatus(taskId);
            
            // 根据后端返回的 status 字段判断任务状态
            const taskStatus = statusResult.status;
            
            if (taskStatus === 'pending' || taskStatus === 'processing' || 
                taskStatus === 'uploading' || taskStatus === 'transcribing') {
              // 更新进度
              updateTaskStatus(taskId, {
                status: taskStatus,
                progress: statusResult.progress || 0,
                current_file: filename,
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
                current_file: filename,
                files_processed: 1,
                files_failed: 0,
                total_files: 1,
                logs: statusResult.logs || [],
              });
              message.success('Upload and speech recognition completed!');
            } else if (taskStatus === 'failed') {
              // 任务失败
              clearInterval(pollInterval);
              const errorMsg = statusResult.message || 'Task failed';
              updateTaskStatus(taskId, {
                status: 'failed',
                progress: 0,
                current_file: filename,
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

        // 设置超时（最多 10 分钟）
        setTimeout(() => {
          clearInterval(pollInterval);
        }, 600000);

      } catch (error: any) {
        const errorMessage = getErrorMessage(error, 'Upload failed');
        message.error(errorMessage);
      }
    })();
  };

  const handleRemoveFile = () => {
    setAudioFile(null);
  };

  const handleClose = () => {
    setAudioFile(null);
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
            <SoundOutlined style={{ fontSize: '18px' }} />
            <span>Upload Audio Files</span>
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
          {/* 拖拽上传区域 - 仅在没有文件时显示 */}
          {!audioFile && (
            <Dragger
              multiple={false}
              maxCount={1}
              beforeUpload={handleBeforeUpload}
              showUploadList={false}
              accept="audio/*,.mp3,.wav,.m4a,.aac,.flac,.ogg,.opus,.wma,.amr"
              style={{
                background: isDark ? '#212936' : '#fafafa',
                border: `2px dashed ${isDark ? '#364152' : '#d9d9d9'}`,
                borderRadius: '8px',
              }}
            >
              <p className="ant-upload-drag-icon">
                <InboxOutlined style={{ fontSize: '48px', color: '#1890ff' }} />
              </p>
              <p className="ant-upload-text" style={{ 
                fontSize: '16px', 
                color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
                marginTop: '16px'
              }}>
                click or drag audio file to this area
              </p>
              <p className="ant-upload-hint" style={{ 
                fontSize: '13px', 
                color: isDark ? '#94a3b8' : 'rgba(0, 0, 0, 0.45)',
                marginTop: '8px'
              }}>
                supported formats: mp3, wav, m4a, aac, flac, ogg, opus, wma, etc.
              </p>
              <p className="ant-upload-hint" style={{ 
                fontSize: '12px', 
                color: isDark ? '#64748b' : 'rgba(0, 0, 0, 0.35)',
                marginTop: '4px'
              }}>
                maximum 1 file, maximum file size: 1GB
              </p>
            </Dragger>
          )}

          {/* 已选文件 - 简单信息 */}
          {audioFile && (
            <div style={{
              padding: '16px',
              background: isDark ? '#212936' : '#fafafa',
              borderRadius: '8px',
              border: `1px solid ${isDark ? '#364152' : '#e0e0e0'}`,
              marginBottom: '16px'
            }}>
              <div>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '8px' }}>
                  <span style={{ 
                    fontSize: '14px', 
                    fontWeight: 500,
                    color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
                    wordBreak: 'break-all'
                  }}>
                    {audioFile.name}
                  </span>
                  <Button type="text" danger size="small" onClick={handleRemoveFile}>
                    Remove
                  </Button>
                </div>
                <Space size={4} wrap>
                  <Tag color="cyan" style={{ fontSize: '11px' }}>{audioFile.format}</Tag>
                  <Tag color="default" style={{ fontSize: '11px' }}>{formatFileSize(audioFile.size)}</Tag>
                </Space>
              </div>
            </div>
          )}

          {/* 底部按钮 */}
          <div style={{ 
            display: 'flex', 
            justifyContent: 'flex-end', 
            alignItems: 'center',
            marginTop: '16px',
            gap: '8px'
          }}>
            <Button onClick={handleClose}>
              Cancel
            </Button>
            {audioFile && (
              <Button
                type="primary"
                onClick={handleUpload}
                icon={<SoundOutlined />}
              >
                Upload
              </Button>
            )}
          </div>
        </div>
      </Modal>
    </ConfigProvider>
  );
};

export default UploadAudioModal;
