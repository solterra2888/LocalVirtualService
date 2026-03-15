

import React, { useState } from 'react';
import { Modal, Upload, Button, Form, Select, message, Space, Tag, Divider, Input, Switch, InputNumber, Row, Col, Collapse, ConfigProvider, Segmented, theme as antdTheme } from 'antd';
import { InboxOutlined, PlayCircleOutlined, DeleteOutlined, SettingOutlined, UploadOutlined, ThunderboltOutlined, CloudUploadOutlined } from '@ant-design/icons';
import { filesAPI } from '../services/files';
import { useNavigate } from 'react-router-dom';
import { getErrorMessage } from '../utils/errorHandler';
import { useFileProcessing } from '../contexts/FileProcessingContext';

const { Option } = Select;

interface UploadFilesModalProps {
  visible: boolean;
  onCancel: () => void;
  themeMode: 'light' | 'dark';
}

interface ProcessingParams {
  processing_mode: 'quick' | 'llamaparse';
  language: string[];
  fast_mode: boolean;
  disable_ocr: boolean;
  max_pages: number;
  target_pages: string;
  skip_diagonal_text: boolean;
  skip_table_extraction: boolean;
  extract_images: boolean;
  use_vendor_multimodal_model: boolean;
  vendor_multimodal_api_key: string;
  polling_interval: number;
  use_ai_agent: boolean;
  llamaindex_api_key: string;
  ai_agent_server_url: string;
}

// 移除未使用的接口：FileItem, TaskStatus, ProcessingResult

const INPUT_CONTROL_HEIGHT = 32;

/* 与 FRONTEND_STYLE_GUIDE 一致 */
const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    codeBlockBg: '#f5f5f5',
    selectedBorder: '#1890ff',
    inputBg: undefined as string | undefined,
  },
  dark: {
    pageBg: '#1E202B',
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    text: '#FFFFFF',
    textSecondary: '#A0A3AD',
    codeBlockBg: '#1E202B',
    selectedBorder: '#2196F3',
    inputBg: '#1E202B',
  },
} as const;

const CARD_STYLE_BEHANCE = {
  borderRadius: 12,
  boxShadow: '0 4px 24px rgba(0, 0, 0, 0.12)',
  border: '1px solid rgba(255, 255, 255, 0.06)',
};

const CARD_STYLE_BEHANCE_DARK = {
  borderRadius: 12,
  boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)',
  border: '1px solid rgba(255, 255, 255, 0.06)',
};

const UploadFilesModal: React.FC<UploadFilesModalProps> = ({ visible, onCancel, themeMode }) => {
  const navigate = useNavigate();
  const [form] = Form.useForm();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const { addTask } = useFileProcessing();

  const inputStyle: React.CSSProperties = {
    width: '100%',
    height: INPUT_CONTROL_HEIGHT,
    ...(isDark && colors.inputBg ? { backgroundColor: colors.inputBg } : {}),
  };
  
  // 文件相关状态：选中文件仅存本地，点击 Start Processing 时才上传
  const [selectedFile, setSelectedFile] = useState<File | null>(null);

  // 行业和股票选择功能已移除 - 改用自动打标签（content_tag_v1）
  // AI分析功能已移除 - 改为在FilesPage右侧面板通过"Analyze Document"/"Analyze Audio"按钮触发
  
  // 参数设置 - 仅保留文档处理相关参数
  const [processingParams, setProcessingParams] = useState<ProcessingParams>({
    processing_mode: 'quick',
    language: ['ch_sim', 'en'],
    fast_mode: false,
    disable_ocr: false,
    max_pages: 20,
    target_pages: '',
    skip_diagonal_text: false,
    skip_table_extraction: false,
    extract_images: false,
    use_vendor_multimodal_model: false,
    vendor_multimodal_api_key: '',
    polling_interval: 10,
    use_ai_agent: false,
    llamaindex_api_key: '',
    ai_agent_server_url: ''
  });

  // 加载初始数据 - 不再需要加载任务类型和模板
  // AI分析功能已移至FilesPage右侧面板

  /** 选中/拖入文件时仅加入列表，不发起上传；仅接受支持格式 */
  const handleFileSelect = (file: File) => {
    if (selectedFile) {
      message.warning('Only one file at a time. Please remove the current file first.');
      return false;
    }
    if (!filesAPI.validateFileType(file)) {
      const exts = filesAPI.getSupportedFileExtensions().join(', ');
      message.warning(`Unsupported file type. Supported: ${exts}`);
      return false;
    }
    setSelectedFile(file);
    if (filesAPI.isImageFile(file.name)) {
      setProcessingParams(prev => ({ ...prev, processing_mode: 'llamaparse' }));
      message.info('Image files require LlamaParse for OCR processing. Switched automatically.');
    }
    return false; // 阻止 Ant Design 默认上传
  };

  /** 移除已选文件（未上传，无需调后端） */
  const handleClearSelectedFile = () => {
    setSelectedFile(null);
    setProcessingParams(prev => ({ ...prev, processing_mode: 'quick' }));
  };

  const handleStartProcessing = async () => {
    if (!selectedFile) {
      message.warning('Please select a file first');
      return;
    }

    // 先保存参数，再立即关闭窗口，上传与处理在 Celery 后台进行
    const file = selectedFile;
    const params = { ...processingParams };

    resetAndClose();

    try {
      // 调用新的异步端点：立即返回，后台上传到 OSS + 文档解析（不含AI分析）
      const response = await filesAPI.uploadAndProcessDocument(file, {
        analysis_mode: 'local_llm', // 保留字段但不启用AI分析
        task_type: 'document_upload', // 仅上传和解析
        template_id: undefined, // 不使用模板
        template_variables: {},
        custom_inputs: {},
        params: {
          ...params,
          use_ai_agent: false, // 明确禁用AI分析
        },
      });

      addTask(response.task_id, file.name, 'document');
      message.success('File upload and processing started. Use "Analyze Document" in the right panel for AI analysis.');
    } catch (error: any) {
      const errorMessage = getErrorMessage(error, 'Failed to start upload and processing');
      message.error(errorMessage);
      console.error('Failed to start upload and processing:', error);
    }
  };

  const handleClose = () => {
    resetAndClose();
  };

  const resetAndClose = () => {
    setSelectedFile(null);
    setProcessingParams({
      processing_mode: 'quick',
      language: ['ch_sim', 'en'],
      fast_mode: false,
      disable_ocr: false,
      max_pages: 20,
      target_pages: '',
      skip_diagonal_text: false,
      skip_table_extraction: false,
      extract_images: false,
      use_vendor_multimodal_model: false,
      vendor_multimodal_api_key: '',
      polling_interval: 10,
      use_ai_agent: false,
      llamaindex_api_key: '',
      ai_agent_server_url: ''
    });
    form.resetFields();
    onCancel();
  };

  const modalStyles = isDark
    ? {
        header: {
          background: colors.cardBg,
          color: colors.text,
          borderBottom: `1px solid ${colors.cardBorder}`,
        } as React.CSSProperties,
        content: {
          background: colors.cardBg,
          borderRadius: CARD_STYLE_BEHANCE_DARK.borderRadius,
          boxShadow: CARD_STYLE_BEHANCE_DARK.boxShadow,
          border: CARD_STYLE_BEHANCE_DARK.border,
        } as React.CSSProperties,
        body: {
          background: colors.cardBg,
          color: colors.text,
          maxHeight: '85vh',
          overflowY: 'auto' as const,
        } as React.CSSProperties,
      }
    : {
        content: {
          borderRadius: CARD_STYLE_BEHANCE.borderRadius,
          boxShadow: CARD_STYLE_BEHANCE.boxShadow,
          border: `1px solid ${colors.cardBorder}`,
        } as React.CSSProperties,
      };

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: colors.cardBg,
            colorBgElevated: colors.cardBg,
            colorBorderSecondary: colors.cardBorder,
            colorPrimary: '#2196F3',
            colorText: colors.text,
            colorTextSecondary: colors.textSecondary,
            colorFillQuaternary: colors.inputBg ?? '#1E202B',
            colorBgLayout: colors.pageBg,
            borderRadius: 8,
            borderRadiusLG: 12,
          },
        }),
      }}
    >
      <Modal
        classNames={{ content: 'upload-files-modal' }}
        title={
          <Space>
            <UploadOutlined />
            <span>Upload & Process Files</span>
          </Space>
        }
        open={visible}
        onCancel={handleClose}
        footer={null}
        width={800}
        centered
        closable={true}
        maskClosable={true}
        maskStyle={{
          background: isDark ? 'rgba(0, 0, 0, 0.6)' : 'rgba(0, 0, 0, 0.45)',
          backdropFilter: 'blur(6px)',
          WebkitBackdropFilter: 'blur(6px)',
        }}
        styles={modalStyles}
        bodyStyle={{
          maxHeight: '85vh',
          overflowY: 'auto',
        }}
      >
      <Form form={form} layout="vertical" style={{ padding: '12px 0' }}>
        {/* 文件上传区域：选中不上传，点击 Start Processing 才上传 */}
        {!selectedFile ? (
          <Upload.Dragger
            accept={filesAPI.getAcceptFileTypes()}
            beforeUpload={handleFileSelect}
            showUploadList={false}
            style={{
              background: colors.codeBlockBg,
              border: `1px dashed ${colors.cardBorder}`,
              borderRadius: 8,
            }}
          >
            <p className="ant-upload-drag-icon">
              <InboxOutlined style={{ color: colors.selectedBorder }} />
            </p>
            <p className="ant-upload-text" style={{ color: colors.text }}>
              Click or drag file to this area to upload
            </p>
            <p className="ant-upload-hint" style={{ color: colors.textSecondary }}>
              Supported: PDF, Excel (.xlsx, .xls), CSV, Word (.doc, .docx), Images (.jpg, .jpeg, .png, .gif, .bmp)
            </p>
          </Upload.Dragger>
        ) : (
          <div
            style={{
              padding: '16px 20px',
              background: colors.codeBlockBg,
              border: `1px solid ${colors.cardBorder}`,
              borderRadius: 8,
              marginBottom: '16px',
            }}
          >
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
              <Tag color="blue">
                {selectedFile.name}
                <span style={{ marginLeft: 8, opacity: 0.8 }}>
                  ({(selectedFile.size / 1024).toFixed(1)} KB)
                </span>
              </Tag>
              <Button
                type="text"
                danger
                size="small"
                icon={<DeleteOutlined />}
                onClick={handleClearSelectedFile}
              />
            </div>
          </div>
        )}

        <Divider style={{ margin: '16px 0', borderColor: colors.cardBorder }} />

        {/* Processing Mode Selector */}
        <div style={{ marginTop: '8px', marginBottom: '8px' }}>
          <div style={{
            fontSize: '14px',
            fontWeight: 600,
            marginBottom: '12px',
            color: colors.text,
          }}>
            Document Processing
          </div>
          <Segmented
            block
            value={processingParams.processing_mode}
            onChange={(value) => {
              if (value === 'quick' && selectedFile && filesAPI.isImageFile(selectedFile.name)) {
                message.warning('Image files require LlamaParse for OCR processing.');
                return;
              }
              setProcessingParams(prev => ({ ...prev, processing_mode: value as 'quick' | 'llamaparse' }));
            }}
            options={[
              {
                label: (
                  <div style={{ padding: '4px 0' }}>
                    <Space>
                      <ThunderboltOutlined />
                      <span style={{ fontWeight: 500 }}>Quick Process</span>
                    </Space>
                    <div style={{ fontSize: 11, opacity: 0.65, marginTop: 2 }}>
                      Fast local parsing for simple documents
                    </div>
                  </div>
                ),
                value: 'quick',
                disabled: !!(selectedFile && filesAPI.isImageFile(selectedFile.name)),
              },
              {
                label: (
                  <div style={{ padding: '4px 0' }}>
                    <Space>
                      <CloudUploadOutlined />
                      <span style={{ fontWeight: 500 }}>LlamaParse</span>
                    </Space>
                    <div style={{ fontSize: 11, opacity: 0.65, marginTop: 2 }}>
                      Cloud parsing with OCR for complex docs
                    </div>
                  </div>
                ),
                value: 'llamaparse',
              },
            ]}
            style={{ marginBottom: '12px' }}
          />

          {processingParams.processing_mode === 'quick' ? (
            <div style={{
              marginBottom: '12px',
              padding: '8px 12px',
              background: colors.codeBlockBg,
              borderRadius: 8,
              fontSize: '12px',
              color: colors.textSecondary,
            }}>
              Uses lightweight local libraries to convert documents to Markdown. Best for text-based PDFs, Word, and Excel files. Does not support OCR or scanned documents.
            </div>
          ) : (
            <>
              <div style={{
                marginBottom: '12px',
                padding: '8px 12px',
                background: colors.codeBlockBg,
                borderRadius: 8,
                fontSize: '12px',
                color: colors.textSecondary,
              }}>
                By default only 20 pages are processed. You can change this and other options in Advanced Parameter Settings below.
              </div>
              <Collapse
                ghost
                style={{ marginBottom: '16px' }}
                items={[
                  {
                    key: '1',
                    label: (
                      <Space>
                        <SettingOutlined />
                        <span style={{ fontWeight: 500 }}>Advanced Parameter Settings</span>
                      </Space>
                    ),
                    children: (
                      <div style={{ paddingTop: '8px' }}>
                        <Row gutter={[12, 12]}>
                        <Col span={12}>
                          <Form.Item label="Language Settings" style={{ marginBottom: '12px' }}>
                            <Select
                              mode="multiple"
                              value={processingParams.language}
                              onChange={(value) => setProcessingParams(prev => ({ ...prev, language: value }))}
                              placeholder="Select languages"
                              style={inputStyle}
                            >
                              <Option value="ch_sim">Simplified Chinese</Option>
                              <Option value="en">English</Option>
                            </Select>
                          </Form.Item>
                        </Col>
                        <Col span={12}>
                          <Form.Item label="Max Pages" style={{ marginBottom: '12px' }}>
                            <InputNumber
                              value={processingParams.max_pages}
                              onChange={(value) => setProcessingParams(prev => ({ ...prev, max_pages: value || 20 }))}
                              min={1}
                              max={100}
                              style={inputStyle}
                            />
                          </Form.Item>
                        </Col>
                      </Row>

                      <Row gutter={[12, 12]}>
                        <Col span={12}>
                          <Form.Item label="Polling Interval (seconds)" style={{ marginBottom: '12px' }}>
                            <InputNumber
                              value={processingParams.polling_interval}
                              onChange={(value) => setProcessingParams(prev => ({ ...prev, polling_interval: value || 10 }))}
                              min={5}
                              max={60}
                              style={inputStyle}
                            />
                          </Form.Item>
                        </Col>
                        <Col span={12}>
                          <Form.Item label="Target Pages" style={{ marginBottom: '12px' }}>
                            <Input
                              value={processingParams.target_pages}
                              onChange={(e) => setProcessingParams(prev => ({ ...prev, target_pages: e.target.value }))}
                              placeholder="e.g., 1-5, 7, 10"
                              style={inputStyle}
                            />
                          </Form.Item>
                        </Col>
                      </Row>

                      <Divider style={{ margin: '12px 0', borderColor: colors.cardBorder }} />

                      <Row gutter={[12, 12]}>
                        <Col span={12}>
                          <Form.Item label="Fast Mode" style={{ marginBottom: '8px' }}>
                            <Switch
                              size="small"
                              checked={processingParams.fast_mode}
                              onChange={(checked) => setProcessingParams(prev => ({ ...prev, fast_mode: checked }))}
                            />
                          </Form.Item>
                        </Col>
                        <Col span={12}>
                          <Form.Item label="Disable OCR" style={{ marginBottom: '8px' }}>
                            <Switch
                              size="small"
                              checked={processingParams.disable_ocr}
                              onChange={(checked) => setProcessingParams(prev => ({ ...prev, disable_ocr: checked }))}
                            />
                          </Form.Item>
                        </Col>
                      </Row>

                      <Row gutter={[12, 12]}>
                        <Col span={8}>
                          <Form.Item label="Skip Diagonal Text" style={{ marginBottom: '8px' }}>
                            <Switch
                              size="small"
                              checked={processingParams.skip_diagonal_text}
                              onChange={(checked) => setProcessingParams(prev => ({ ...prev, skip_diagonal_text: checked }))}
                            />
                          </Form.Item>
                        </Col>
                        <Col span={8}>
                          <Form.Item label="Skip Table Extraction" style={{ marginBottom: '8px' }}>
                            <Switch
                              size="small"
                              checked={processingParams.skip_table_extraction}
                              onChange={(checked) => setProcessingParams(prev => ({ ...prev, skip_table_extraction: checked }))}
                            />
                          </Form.Item>
                        </Col>
                        <Col span={8}>
                          <Form.Item label="Extract Images" style={{ marginBottom: '8px' }}>
                            <Switch
                              size="small"
                              checked={processingParams.extract_images}
                              onChange={(checked) => setProcessingParams(prev => ({ ...prev, extract_images: checked }))}
                            />
                          </Form.Item>
                        </Col>
                      </Row>

                      <Row gutter={[12, 12]}>
                        <Col span={12}>
                          <Form.Item label="Use Multimodal Model" style={{ marginBottom: '8px' }}>
                            <Switch
                              size="small"
                              checked={processingParams.use_vendor_multimodal_model}
                              onChange={(checked) => setProcessingParams(prev => ({ ...prev, use_vendor_multimodal_model: checked }))}
                            />
                          </Form.Item>
                        </Col>
                        <Col span={12}>
                          <Form.Item label="Multimodal API Key" style={{ marginBottom: '8px' }}>
                            <Input
                              value={processingParams.vendor_multimodal_api_key}
                              onChange={(e) => setProcessingParams(prev => ({ ...prev, vendor_multimodal_api_key: e.target.value }))}
                              placeholder="Enter API key"
                              disabled={!processingParams.use_vendor_multimodal_model}
                              style={inputStyle}
                            />
                          </Form.Item>
                        </Col>
                      </Row>

                      <Row gutter={[12, 12]}>
                        <Col span={24}>
                          <Form.Item label="LlamaIndex API Key" style={{ marginBottom: '8px' }}>
                            <Input.Password
                              value={processingParams.llamaindex_api_key}
                              onChange={(e) => setProcessingParams(prev => ({ ...prev, llamaindex_api_key: e.target.value }))}
                              placeholder="Enter LlamaIndex API key"
                              style={inputStyle}
                            />
                          </Form.Item>
                        </Col>
                      </Row>
                    </div>
                  ),
                },
              ]}
              />
            </>
          )}
        </div>

        {/* 操作按钮 */}
        <div style={{ marginTop: '20px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Button
            onClick={handleClose}
            style={{
              borderRadius: 8,
              ...(isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : {}),
            }}
          >
            Close
          </Button>
          <Space>
            {/* 选中文件后显示 Start Processing；点击后上传并解析文档 */}
            {selectedFile && (
              <Button
                type="primary"
                icon={<PlayCircleOutlined />}
                onClick={handleStartProcessing}
                style={{
                  borderRadius: 10,
                  fontWeight: 500,
                  height: 36,
                  paddingLeft: 16,
                  paddingRight: 16,
                  ...(isDark ? { background: '#2196F3', borderColor: '#2196F3' } : {}),
                }}
              >
                Start Processing
              </Button>
            )}

            {/* 始终显示 Go to Files Page 按钮 */}
            <Button
              onClick={() => {
                onCancel();
                navigate('/files');
              }}
              style={{
                borderRadius: 8,
                ...(isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : {}),
              }}
            >
              Go to Files Page
            </Button>
          </Space>
        </div>
      </Form>
    </Modal>
    </ConfigProvider>
  );
};

export default UploadFilesModal;
