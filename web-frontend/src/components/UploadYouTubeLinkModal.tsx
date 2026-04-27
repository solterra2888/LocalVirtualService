import React, { useState, useRef } from 'react';
import { Modal, Input, Button, message, Space, ConfigProvider, theme as antdTheme, Alert, Typography } from 'antd';
import { LinkOutlined, PlayCircleOutlined, SoundOutlined, ClockCircleOutlined } from '@ant-design/icons';
import { filesAPI } from '../services/files';
import { getErrorMessage } from '../utils/errorHandler';
import { getModalStyles } from '../utils/modalStyles';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import UploadAnalysisTemplatePicker, {
  UploadAnalysisTemplatePickerValue,
} from './UploadAnalysisTemplatePicker';

interface UploadYouTubeLinkModalProps {
  visible: boolean;
  onCancel: () => void;
  themeMode: 'light' | 'dark';
}

const UploadYouTubeLinkModal: React.FC<UploadYouTubeLinkModalProps> = ({ visible, onCancel, themeMode }) => {
  const isDark = themeMode === 'dark';
  const [youtubeUrl, setYoutubeUrl] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const [asrModalOpen, setAsrModalOpen] = useState(false);
  const [analyzePicker, setAnalyzePicker] = useState<UploadAnalysisTemplatePickerValue>({});
  // PR-5: file_tagging picker
  const [tagPicker, setTagPicker] = useState<UploadAnalysisTemplatePickerValue>({});
  const asrNotifiedRef = useRef(false); // 每次上传只弹一次
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
    const youtubeAnalyzeOptions = {
      auto_analyze_template_id: analyzePicker.templateId ?? null,
      auto_analyze_template_variables: analyzePicker.templateVariables || {},
      // PR-5: file_tagging picker
      auto_tag_template_id: tagPicker.templateId ?? null,
      auto_tag_template_variables: tagPicker.templateVariables || {},
    };

    // 重置 ASR 通知标记（每次上传独立）
    asrNotifiedRef.current = false;

    // 关闭 Modal
    handleClose();

    try {
      // 调用 API, 字幕/ASR 完成后由 finalize_youtube_file_task 自动并行触发 4 路 LLM
      const response = await filesAPI.uploadYouTubeLink(url, youtubeAnalyzeOptions);
      
      if (!response.task_id) {
        throw new Error('No task ID returned');
      }

      const taskId = response.task_id;
      const videoId = response.video_id || 'unknown';
      // 新路径: 后端立即建好 file_processing_details, 返回 file_id.
      // 若存在 file_id → 直接按 file_id 轮询数据库, 跨字幕→ASR 链路都能拿到真实终态.
      const fileId = response.file_id;

      addTask(taskId, `YouTube_${videoId}`, 'youtube', fileId ?? undefined);

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

      const pollInterval = setInterval(async () => {
        try {
          // 优先使用 file_id 轮询数据库真实状态;
          // 回退路径 (YOUTUBE_UPLOAD_USE_LOCAL=false) 没有 file_id, 继续走 Celery task 轮询.
          if (fileId) {
            const st = await filesAPI.getUploadYouTubeStatus(fileId);
            // 仅当 rc_agent_status 也已终态（finalize 已执行）才视为真正完成；
            // 否则继续以 processing 上报，确保列表刷新在 rc_agent_status 更新后触发。
            const rcFinalized = !!st.rc_agent_status && st.rc_agent_status !== 'processing';

            // ASR 已触发（无字幕，已切换 Fun-ASR）→ 弹一次等待提示
            if (st.asr_triggered && !asrNotifiedRef.current) {
              asrNotifiedRef.current = true;
              setAsrModalOpen(true);
            }

            if (st.status === 'processing' || ((st.status === 'completed' || st.status === 'failed') && !rcFinalized)) {
              updateTaskStatus(taskId, {
                status: 'processing',
                progress: st.progress,
                current_file: url,
                files_processed: 0,
                files_failed: 0,
                total_files: 1,
                logs: [],
              });
            } else if (st.status === 'completed' && rcFinalized) {
              clearInterval(pollInterval);
              updateTaskStatus(taskId, {
                status: 'completed',
                progress: 100,
                current_file: url,
                files_processed: 1,
                files_failed: 0,
                total_files: 1,
                logs: [],
              });
              const via = st.caption_source === 'asr'
                ? 'Fun-ASR'
                : st.caption_source === 'caption_ytdlp'
                  ? 'yt-dlp caption'
                  : 'YouTube caption';
              message.success(`YouTube transcript ready (${via})!`);
            } else if (st.status === 'failed' && rcFinalized) {
              clearInterval(pollInterval);
              updateTaskStatus(taskId, {
                status: 'failed',
                progress: 0,
                current_file: url,
                files_processed: 0,
                files_failed: 1,
                total_files: 1,
                logs: [],
              });
              message.error(`Processing failed: ${st.finalize_reason || st.message}`);
            }
            return;
          }

          // 回退路径: 沿用旧的 task_id 轮询
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
            message.success('YouTube transcript fetched successfully!');
          } else if (taskStatus === 'failed') {
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
      }, 2000);

      // 超时提高到 15 分钟 (ASR 长视频需要更多时间)
      setTimeout(() => {
        clearInterval(pollInterval);
      }, 900000);

    } catch (error: any) {
      const errorMessage = getErrorMessage(error, 'Failed to process YouTube link');
      message.error(errorMessage);
      setIsProcessing(false);
    }
  };

  const handleClose = () => {
    setYoutubeUrl('');
    setIsProcessing(false);
    setAnalyzePicker({});
    setTagPicker({});
    onCancel();
  };

  const handleAsrModalClose = () => setAsrModalOpen(false);

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

          {/* PR-A: YouTube 走 audio_analysis 模块 (转写后做音频/视频内容分析) */}
          <div style={{ marginBottom: '20px' }}>
            <UploadAnalysisTemplatePicker
              value={analyzePicker}
              onChange={setAnalyzePicker}
              mode="audio-default"
              moduleCode="audio_analysis"
              label="Audio Analysis Template"
              textColor={isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)'}
              textSecondaryColor={isDark ? '#94a3b8' : 'rgba(0, 0, 0, 0.45)'}
              disabled={isProcessing}
            />
          </div>

          {/* PR-5: 自动打标模板 (可选, 选中后用自定义模板代替默认 content_tag_v1) */}
          <div style={{ marginBottom: '20px' }}>
            <UploadAnalysisTemplatePicker
              value={tagPicker}
              onChange={setTagPicker}
              mode="optional"
              moduleCode="file_tagging"
              label="AI Tagging Template"
              hint="Optional. Toggle on to use your own tagging template (sector / industry / tickers / concepts)."
              textColor={isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)'}
              textSecondaryColor={isDark ? '#94a3b8' : 'rgba(0, 0, 0, 0.45)'}
              disabled={isProcessing}
            />
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
      {/* ASR 等待提示弹窗 —— 仅在视频无字幕、Fun-ASR 已触发时弹出一次 */}
      <Modal
        open={asrModalOpen}
        onCancel={handleAsrModalClose}
        onOk={handleAsrModalClose}
        okText="Got it"
        cancelButtonProps={{ style: { display: 'none' } }}
        width={460}
        centered
        title={
          <Space>
            <SoundOutlined style={{ color: '#faad14', fontSize: 18 }} />
            <span>ASR Transcription Started</span>
          </Space>
        }
        maskStyle={{
          background: isDark ? 'rgba(0,0,0,0.6)' : undefined,
          backdropFilter: 'blur(6px)',
          WebkitBackdropFilter: 'blur(6px)',
        }}
        styles={modalStyles}
      >
        <div style={{ padding: '8px 0 4px' }}>
          <Alert
            type="warning"
            showIcon
            icon={<ClockCircleOutlined />}
            message="No subtitles available — switching to Fun-ASR"
            description={
              <Typography.Text style={{ color: isDark ? '#94a3b8' : undefined }}>
                This video doesn't have a caption track. Fun-ASR is now transcribing the audio.
                <br /><br />
                <strong>Estimated wait:</strong> 5–30 minutes depending on video length.
                <br /><br />
                You can close this tab — we'll update the file automatically when transcription is complete.
              </Typography.Text>
            }
            style={{
              background: isDark ? '#2d2000' : '#fffbe6',
              border: `1px solid ${isDark ? '#614700' : '#ffe58f'}`,
            }}
          />
        </div>
      </Modal>
    </ConfigProvider>
  );
};

export default UploadYouTubeLinkModal;
