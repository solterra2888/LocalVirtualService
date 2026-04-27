import React, { useState } from 'react';
import { Modal, Input, Button, message, Space, ConfigProvider, theme as antdTheme, Alert } from 'antd';
import { GlobalOutlined, SendOutlined, ChromeOutlined, WarningOutlined } from '@ant-design/icons';
import { filesAPI } from '../services/files';
import { getErrorMessage } from '../utils/errorHandler';
import { getModalStyles } from '../utils/modalStyles';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import UploadAnalysisTemplatePicker, {
  UploadAnalysisTemplatePickerValue,
} from './UploadAnalysisTemplatePicker';

interface UploadWebLinkModalProps {
  visible: boolean;
  onCancel: () => void;
  themeMode: 'light' | 'dark';
}

const UploadWebLinkModal: React.FC<UploadWebLinkModalProps> = ({ visible, onCancel, themeMode }) => {
  const isDark = themeMode === 'dark';
  const [webUrl, setWebUrl] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const [scrapeBlockedOpen, setScrapeBlockedOpen] = useState(false);
  const [analyzePicker, setAnalyzePicker] = useState<UploadAnalysisTemplatePickerValue>({});
  // PR-5: file_tagging picker
  const [tagPicker, setTagPicker] = useState<UploadAnalysisTemplatePickerValue>({});
  const { addTask, updateTaskStatus } = useFileProcessing();

  const validateUrl = (url: string): boolean => {
    if (!url.trim()) return false;
    try {
      const parsed = new URL(url.trim());
      return parsed.protocol === 'http:' || parsed.protocol === 'https:';
    } catch {
      return false;
    }
  };

  const getDomainFromUrl = (url: string): string => {
    try {
      return new URL(url).hostname;
    } catch {
      return 'unknown';
    }
  };

  const handleStartProcessing = async () => {
    if (!webUrl.trim()) {
      message.warning('Please enter a web page URL');
      return;
    }

    if (!validateUrl(webUrl)) {
      message.error('Invalid URL. Please provide a valid HTTP/HTTPS link.');
      return;
    }

    setIsProcessing(true);
    const url = webUrl.trim();
    const webAnalyzeOptions = {
      auto_analyze_template_id: analyzePicker.templateId ?? null,
      auto_analyze_template_variables: analyzePicker.templateVariables || {},
      // PR-5: file_tagging picker
      auto_tag_template_id: tagPicker.templateId ?? null,
      auto_tag_template_variables: tagPicker.templateVariables || {},
    };

    handleClose();

    try {
      // 抓取完成后由后端自动并行触发 4 路 LLM (analyze 仅当用户选了模板)
      const response = await filesAPI.uploadWebLink(url, webAnalyzeOptions);

      if (!response.task_id) {
        throw new Error('No task ID returned');
      }

      const taskId = response.task_id;
      const domain = getDomainFromUrl(url);

      addTask(taskId, `WebLink_${domain}`, 'weblink');

      updateTaskStatus(taskId, {
        status: 'processing',
        progress: 10,
        current_file: url,
        files_processed: 0,
        files_failed: 0,
        total_files: 1,
        logs: [],
      });

      message.info('Web page scraping started. Extracting article content...');

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
            message.success('Web page captured successfully!');
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

            const isAccessBlocked = errorMsg.includes('login') ||
              errorMsg.includes('subscription') ||
              errorMsg.includes('anti-scraping') ||
              errorMsg.includes('anti-bot') ||
              errorMsg.includes('Unable to fetch') ||
              errorMsg.includes('Web Clipper') ||
              errorMsg.includes('blocked page');

            if (isAccessBlocked) {
              setScrapeBlockedOpen(true);
            } else {
              message.error(errorMsg, 8);
            }
          }
        } catch (pollError: any) {
          console.error('Error polling task status:', pollError);
        }
      }, 2000);

      setTimeout(() => {
        clearInterval(pollInterval);
      }, 300000);

    } catch (error: any) {
      const errorMessage = getErrorMessage(error, 'Failed to process Web Link');
      message.error(errorMessage);
      setIsProcessing(false);
    }
  };

  const handleClose = () => {
    setWebUrl('');
    setIsProcessing(false);
    setAnalyzePicker({});
    setTagPicker({});
    onCancel();
  };

  const modalStyles = getModalStyles(themeMode);
  const scrapeBlockedModalStyles = {
    ...modalStyles,
    body: {
      ...modalStyles.body,
      padding: '20px clamp(16px, 4vw, 24px) 8px',
      maxHeight: 'min(70vh, 100%)',
      overflow: 'auto' as const,
    },
    footer: {
      ...modalStyles.footer,
      padding: '12px clamp(16px, 4vw, 24px) 20px',
    },
  };

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
            <GlobalOutlined style={{ fontSize: '18px' }} />
            <span>Web Link</span>
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
            message="Paste a web page link to extract its content"
            description="The server will fetch the page, extract the main article, and embed images. Only publicly accessible pages are supported (no login required)."
            type="info"
            showIcon
            style={{
              marginBottom: '20px',
              background: isDark ? '#1e3a5f' : '#e6f7ff',
              border: `1px solid ${isDark ? '#2b5a8f' : '#91d5ff'}`,
              color: isDark ? '#91d5ff' : 'rgba(0, 0, 0, 0.85)',
            }}
          />

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
              Web Page URL
            </label>
            <Input
              placeholder="https://example.com/article..."
              value={webUrl}
              onChange={(e) => setWebUrl(e.target.value)}
              onPressEnter={handleStartProcessing}
              prefix={<GlobalOutlined style={{ color: isDark ? '#94a3b8' : '#bfbfbf' }} />}
              size="large"
              disabled={isProcessing}
              style={{
                background: isDark ? '#212936' : '#fff',
                borderColor: isDark ? '#364152' : '#d9d9d9',
                color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
              }}
            />
          </div>

          <div
            style={{
              fontSize: '12px',
              color: isDark ? '#64748b' : 'rgba(0, 0, 0, 0.45)',
              marginBottom: '20px',
            }}
          >
            Supported:
            <ul style={{ margin: '4px 0 0 20px', padding: 0 }}>
              <li>News articles, blog posts, research reports</li>
              <li>Any publicly accessible web page</li>
            </ul>
            Not supported:
            <ul style={{ margin: '4px 0 0 20px', padding: 0 }}>
              <li>Pages requiring login or authentication</li>
              <li>Pages with heavy anti-scraping protection</li>
            </ul>
          </div>

          {/* AI 分析模板 (可选, 选中后抓取完成自动触发 analyze_document_task) */}
          <div style={{ marginBottom: '20px' }}>
            <UploadAnalysisTemplatePicker
              value={analyzePicker}
              onChange={setAnalyzePicker}
              mode="optional"
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
              icon={<SendOutlined />}
              disabled={!webUrl.trim() || isProcessing}
              loading={isProcessing}
            >
              Start Scraping
            </Button>
          </div>
        </div>
      </Modal>

      <Modal
        title={
          <Space align="start" size={10} wrap>
            <WarningOutlined
              style={{
                fontSize: 18,
                color: isDark ? '#faad14' : '#d48806',
                marginTop: 2,
              }}
            />
            <span style={{ lineHeight: 1.4 }}>Unable to scrape this page</span>
          </Space>
        }
        open={scrapeBlockedOpen}
        onCancel={() => setScrapeBlockedOpen(false)}
        centered
        width="min(520px, calc(100vw - 24px))"
        maskClosable
        destroyOnClose
        maskStyle={{
          background: isDark ? 'rgba(0, 0, 0, 0.6)' : undefined,
          backdropFilter: 'blur(6px)',
          WebkitBackdropFilter: 'blur(6px)',
        }}
        styles={scrapeBlockedModalStyles}
        footer={
          <div
            style={{
              display: 'flex',
              justifyContent: 'flex-end',
              gap: 8,
              flexWrap: 'wrap',
            }}
          >
            <Button type="primary" onClick={() => setScrapeBlockedOpen(false)}>
              Got it
            </Button>
          </div>
        }
      >
        <div
          style={{
            color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.85)',
            fontSize: 14,
            lineHeight: 1.6,
            wordBreak: 'break-word',
          }}
        >
          <p style={{ margin: 0 }}>
            This page likely requires login/subscription or has anti-scraping protection.
          </p>
          <Alert
            type="info"
            showIcon
            icon={<ChromeOutlined />}
            style={{
              marginTop: 14,
              background: isDark ? '#1e3a5f' : '#e6f7ff',
              border: `1px solid ${isDark ? '#2b5a8f' : '#91d5ff'}`,
              color: isDark ? '#91d5ff' : 'rgba(0, 0, 0, 0.85)',
            }}
            message="Try using Web Clipper instead"
            description="Install the GuanHe Web Clipper Chrome extension. It runs in your browser where you're already logged in, and can capture pages that the server cannot access."
          />
        </div>
      </Modal>
    </ConfigProvider>
  );
};

export default UploadWebLinkModal;
