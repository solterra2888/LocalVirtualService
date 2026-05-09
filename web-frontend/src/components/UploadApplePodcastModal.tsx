import React, { useState } from 'react';
import { Modal, Input, Button, message, notification, Space, ConfigProvider, theme as antdTheme, Alert, Typography } from 'antd';
import { AppleOutlined, SoundOutlined, LockOutlined, GlobalOutlined } from '@ant-design/icons';
import { filesAPI } from '../services/files';
import { getErrorMessage } from '../utils/errorHandler';
import { getModalStyles } from '../utils/modalStyles';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import UploadAnalysisTemplatePicker, {
  UploadAnalysisTemplatePickerValue,
} from './UploadAnalysisTemplatePicker';

interface UploadApplePodcastModalProps {
  visible: boolean;
  onCancel: () => void;
  themeMode: 'light' | 'dark';
}

const APPLE_PODCAST_URL_RE = /^https?:\/\/podcasts\.apple\.com\/.+\?.*\bi=\d+/i;

const UploadApplePodcastModal: React.FC<UploadApplePodcastModalProps> = ({
  visible,
  onCancel,
  themeMode,
}) => {
  const isDark = themeMode === 'dark';
  const [podcastUrl, setPodcastUrl] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const [analyzePicker, setAnalyzePicker] = useState<UploadAnalysisTemplatePickerValue>({});
  const [tagPicker, setTagPicker] = useState<UploadAnalysisTemplatePickerValue>({});
  const { addTask, updateTaskStatus } = useFileProcessing();

  const validateUrl = (url: string): boolean => {
    if (!url.trim()) return false;
    return APPLE_PODCAST_URL_RE.test(url.trim());
  };

  const handleStartProcessing = async () => {
    if (!podcastUrl.trim()) {
      message.warning('Please enter an Apple Podcasts episode URL');
      return;
    }
    if (!validateUrl(podcastUrl)) {
      message.error(
        'Invalid Apple Podcasts URL. Please paste an episode link containing ?i=<episode_id>, e.g. https://podcasts.apple.com/us/podcast/.../id...?i=...',
      );
      return;
    }

    setIsProcessing(true);
    const url = podcastUrl.trim();
    const analyzeOptions = {
      auto_analyze_template_id: analyzePicker.templateId ?? null,
      auto_analyze_template_variables: analyzePicker.templateVariables || {},
      auto_analyze_enabled: analyzePicker.enabled !== false,
      auto_tag_template_id: tagPicker.templateId ?? null,
      auto_tag_template_variables: tagPicker.templateVariables || {},
      auto_tag_enabled: tagPicker.enabled !== false,
    };

    handleClose();

    try {
      const response = await filesAPI.uploadApplePodcastLink(url, analyzeOptions);

      if (!response.task_id) {
        throw new Error('No task ID returned from server');
      }

      const taskId = response.task_id;
      const episodeId = response.episode_id || 'unknown';
      const displayName = `ApplePodcast_${episodeId}`;

      addTask(taskId, displayName, 'podcast', undefined);
      updateTaskStatus(taskId, {
        status: 'processing',
        progress: 5,
        current_file: url,
        files_processed: 0,
        files_failed: 0,
        total_files: 1,
        logs: [],
      });

      message.info(
        'Apple Podcast processing started. Downloading and transcribing audio via Fun-ASR…',
      );

      const pollInterval = setInterval(async () => {
        try {
          const statusResult = await filesAPI.getTaskStatus(taskId);
          const taskStatus = statusResult.status;

          if (taskStatus === 'pending' || taskStatus === 'processing') {
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
            message.success('Apple Podcast transcribed successfully!');
          } else if (taskStatus === 'failed') {
            clearInterval(pollInterval);
            const errorMsg = statusResult.message || 'Task failed';
            const errorType = (statusResult as any).error_type as string | undefined;
            updateTaskStatus(taskId, {
              status: 'failed',
              progress: 0,
              current_file: url,
              files_processed: 0,
              files_failed: 1,
              total_files: 1,
              logs: statusResult.logs || [],
            });
            if (errorType === 'subscriber_only') {
              notification.error({
                message: 'Podcast 下载失败',
                description: (
                  <div style={{ fontSize: '13px', lineHeight: '1.6' }}>
                    <p style={{ marginBottom: 8 }}>该集 Podcast 无法自动下载，常见原因：</p>
                    <ul style={{ paddingLeft: 18, margin: 0 }}>
                      <li style={{ marginBottom: 4 }}>
                        <LockOutlined style={{ marginRight: 4, color: '#faad14' }} />
                        <strong>付费/订阅专属内容</strong>：该集仅对付费会员开放，
                        Apple Podcasts 不公开音频链接
                      </li>
                      <li style={{ marginBottom: 4 }}>
                        <GlobalOutlined style={{ marginRight: 4, color: '#1890ff' }} />
                        <strong>地区限制</strong>：该集在当前服务器所在地区不可访问
                      </li>
                    </ul>
                    <p style={{ marginTop: 8, color: '#888' }}>
                      建议确认该集在 Apple Podcasts 中可免费公开收听后再尝试。
                    </p>
                  </div>
                ),
                duration: 12,
                placement: 'topRight',
              });
            } else {
              message.error(`Processing failed: ${errorMsg}`);
            }
          }
        } catch (pollError: any) {
          console.error('Error polling Apple Podcast task status:', pollError);
        }
      }, 3000);

      // 最长等待 15 分钟（长节目 ASR 需要时间）
      setTimeout(() => {
        clearInterval(pollInterval);
      }, 900000);
    } catch (error: any) {
      const errorMessage = getErrorMessage(error, 'Failed to process Apple Podcast link');
      message.error(errorMessage);
      setIsProcessing(false);
    }
  };

  const handleClose = () => {
    setPodcastUrl('');
    setIsProcessing(false);
    setAnalyzePicker({});
    setTagPicker({});
    onCancel();
  };

  const modalStyles = getModalStyles(themeMode);

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
            <AppleOutlined style={{ fontSize: '18px', color: '#7224d8' }} />
            <span>Upload Apple Podcast Link</span>
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
          <Alert
            message="Paste an Apple Podcasts episode link to transcribe its audio"
            description={
              <div>
                <p style={{ margin: '0 0 6px' }}>
                  The episode audio will be downloaded and transcribed via Fun-ASR. Only episode
                  links containing <code>?i=&lt;episode_id&gt;</code> are supported.
                </p>
                <p style={{ margin: 0, color: isDark ? '#faad14' : '#d48806' }}>
                  <LockOutlined style={{ marginRight: 4 }} />
                  <strong>Note:</strong> Subscriber-only or region-restricted episodes cannot be
                  downloaded. Please ensure the episode is freely available before submitting.
                </p>
              </div>
            }
            type="info"
            showIcon
            style={{
              marginBottom: '20px',
              background: isDark ? '#1e3a5f' : '#e6f7ff',
              border: `1px solid ${isDark ? '#2b5a8f' : '#91d5ff'}`,
              color: isDark ? '#91d5ff' : 'rgba(0, 0, 0, 0.85)',
            }}
          />

          <div style={{ marginBottom: '16px' }}>
            <Typography.Text
              style={{
                fontSize: '13px',
                color: isDark ? '#94a3b8' : '#666',
                display: 'block',
                marginBottom: '6px',
              }}
            >
              Apple Podcasts Episode URL
            </Typography.Text>
            <Input
              placeholder="https://podcasts.apple.com/us/podcast/.../id...?i=..."
              value={podcastUrl}
              onChange={(e) => setPodcastUrl(e.target.value)}
              onPressEnter={handleStartProcessing}
              prefix={<SoundOutlined style={{ color: isDark ? '#94a3b8' : '#bfbfbf' }} />}
              size="large"
              style={{
                background: isDark ? '#1a2332' : '#fff',
                borderColor: isDark ? '#364152' : '#d9d9d9',
                color: isDark ? '#e0e6f1' : undefined,
              }}
            />
            {podcastUrl.trim() && !validateUrl(podcastUrl) && (
              <Typography.Text
                style={{ fontSize: '12px', color: '#ff4d4f', marginTop: '4px', display: 'block' }}
              >
                URL must be an Apple Podcasts episode link containing ?i=&lt;episode_id&gt;
              </Typography.Text>
            )}
          </div>

          {/* Apple Podcast 单集：转写后的深度分析与「上传 YouTube 链接」共用 youtube_analysis 模块 */}
          <div style={{ marginBottom: '12px' }}>
            <UploadAnalysisTemplatePicker
              mode="audio-default"
              moduleCode="youtube_analysis"
              label="YouTube Analysis Template"
              textColor={isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)'}
              textSecondaryColor={isDark ? '#94a3b8' : 'rgba(0, 0, 0, 0.45)'}
              value={analyzePicker}
              onChange={setAnalyzePicker}
            />
          </div>

          {/* File tagging template picker */}
          <div style={{ marginBottom: '20px' }}>
            <UploadAnalysisTemplatePicker
              moduleCode="file_tagging"
              value={tagPicker}
              onChange={setTagPicker}
            />
          </div>

          <Button
            type="primary"
            size="large"
            block
            loading={isProcessing}
            onClick={handleStartProcessing}
            disabled={!podcastUrl.trim() || !validateUrl(podcastUrl)}
            icon={<AppleOutlined style={{ color: '#7224d8' }} />}
          >
            {isProcessing ? 'Processing…' : 'Start Processing'}
          </Button>
        </div>
      </Modal>
    </ConfigProvider>
  );
};

export default UploadApplePodcastModal;
