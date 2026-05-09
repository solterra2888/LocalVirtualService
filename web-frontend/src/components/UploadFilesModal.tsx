

import React, { useState } from 'react';
import { Modal, Upload, Button, Form, Select, message, Space, Divider, Input, Switch, InputNumber, Row, Col, Collapse, ConfigProvider, Segmented, theme as antdTheme, Typography, Tooltip } from 'antd';
import { InboxOutlined, PlayCircleOutlined, DeleteOutlined, SettingOutlined, UploadOutlined, ThunderboltOutlined, CloudUploadOutlined, FilePdfOutlined, FileWordOutlined, FileExcelOutlined, FileImageOutlined } from '@ant-design/icons';
import { filesAPI } from '../services/files';
import { getErrorMessage } from '../utils/errorHandler';
import { getModalStyles } from '../utils/modalStyles';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import UploadAnalysisTemplatePicker, {
  UploadAnalysisTemplatePickerValue,
} from './UploadAnalysisTemplatePicker';

const { Option } = Select;

interface UploadFilesModalProps {
  visible: boolean;
  onCancel: () => void;
  themeMode: 'light' | 'dark';
  /** 手机端：Document Processing 下不展示 Quick/LlamaParse 副标题与说明段落 */
  compactDocumentProcessing?: boolean;
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

const UploadFilesModal: React.FC<UploadFilesModalProps> = ({
  visible,
  onCancel,
  themeMode,
  compactDocumentProcessing = false,
}) => {
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

  // 上传完成后由后端自动并行触发的 AI 分析模板, None 表示跳过 analyze
  const [analyzePicker, setAnalyzePicker] = useState<UploadAnalysisTemplatePickerValue>({});
  // PR-5: file_tagging 模板, None 表示走默认 content_tag_v1 (或用户偏好)
  const [tagPicker, setTagPicker] = useState<UploadAnalysisTemplatePickerValue>({});

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
      // 调用新的异步端点: 立即返回, 后台上传到 OSS + 文档解析,
      // 并在 content 写入完成后自动触发 4 路并行 LLM (含 analyze, 仅当用户选了模板).
      const response = await filesAPI.uploadAndProcessDocument(file, {
        analysis_mode: 'local_llm',
        task_type: 'document_upload',
        template_id: analyzePicker.templateId, // 用户在 picker 选的模板, 上传完成后后端 trigger_auto_analyze
        template_variables: analyzePicker.templateVariables || {},
        // 方案 A: AI Analysis 开关
        auto_analyze_enabled: analyzePicker.enabled !== false,
        custom_inputs: {},
        params: {
          ...params,
          use_ai_agent: false,
        },
        // PR-5: file_tagging picker 选择, 上传完成后后端 trigger_auto_tagging
        auto_tag_template_id: tagPicker.templateId,
        auto_tag_template_variables: tagPicker.templateVariables || {},
        // 方案 A: AI Tagging 开关
        auto_tag_enabled: tagPicker.enabled !== false,
      });

      addTask(response.task_id, file.name, 'document');
      message.success(
        analyzePicker.templateId
          ? 'File upload started. AI analysis will run automatically once parsing completes.'
          : 'File upload and processing started. Use "Analyze Document" in the right panel for AI analysis.'
      );
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
    setAnalyzePicker({});
    setTagPicker({});
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
      ai_agent_server_url: ''
    });
    form.resetFields();
    onCancel();
  };

  const modalStyles = {
    ...getModalStyles(themeMode),
    body: { ...getModalStyles(themeMode).body, maxHeight: '85vh', overflowY: 'auto' as const },
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
          /** Quick / LlamaParse：轨道与选中块对比度（默认 track≈elevated，暗色下几乎分不清） */
          components: {
            Segmented: {
              trackBg: 'rgba(255, 255, 255, 0.06)',
              itemColor: 'rgba(255, 255, 255, 0.55)',
              itemHoverColor: 'rgba(255, 255, 255, 0.92)',
              itemHoverBg: 'rgba(255, 255, 255, 0.1)',
              itemSelectedBg: 'rgba(33, 150, 243, 0.42)',
              itemSelectedColor: '#f0f8ff',
              itemActiveBg: 'rgba(255, 255, 255, 0.12)',
              boxShadowTertiary: '0 0 0 1px rgba(120, 190, 255, 0.45), 0 2px 8px rgba(0, 0, 0, 0.35)',
            },
          },
        }),
      }}
    >
      <Modal
        classNames={{ content: isDark ? 'upload-files-modal upload-files-modal--dark' : 'upload-files-modal' }}
        title={
          <Space>
            <UploadOutlined />
            <span>Upload & Process Files</span>
          </Space>
        }
        open={visible}
        onCancel={handleClose}
        footer={null}
        width={compactDocumentProcessing ? 'calc(100vw - 32px)' : 800}
        centered
        closable={true}
        maskClosable={true}
        maskStyle={{
          background: isDark ? 'rgba(0, 0, 0, 0.6)' : 'rgba(0, 0, 0, 0.45)',
          backdropFilter: 'blur(6px)',
          WebkitBackdropFilter: 'blur(6px)',
        }}
        styles={modalStyles}
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
            <div
              className="upload-files-modal-supported-icons"
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                flexWrap: 'wrap',
                gap: 10,
                marginTop: 8,
              }}
            >
              <span style={{ fontSize: 12, color: colors.textSecondary, marginRight: 4 }}>Supported:</span>
              <Tooltip title="PDF">
                <span style={{ color: '#f5222d', fontSize: 18, lineHeight: 1, cursor: 'default' }} aria-label="PDF">
                  <FilePdfOutlined />
                </span>
              </Tooltip>
              <Tooltip title="Excel (.xlsx, .xls), CSV">
                <span style={{ color: '#52c41a', fontSize: 18, lineHeight: 1, cursor: 'default' }} aria-label="Excel and CSV">
                  <FileExcelOutlined />
                </span>
              </Tooltip>
              <Tooltip title="Word (.doc, .docx)">
                <span style={{ color: '#1890ff', fontSize: 18, lineHeight: 1, cursor: 'default' }} aria-label="Word">
                  <FileWordOutlined />
                </span>
              </Tooltip>
              <Tooltip title="Images (.jpg, .jpeg, .png, .gif, .bmp)">
                <span style={{ color: '#fa8c16', fontSize: 18, lineHeight: 1, cursor: 'default' }} aria-label="Images">
                  <FileImageOutlined />
                </span>
              </Tooltip>
            </div>
          </Upload.Dragger>
        ) : (
          <div
            style={{
              padding: '16px 20px',
              background: colors.codeBlockBg,
              border: `1px solid ${colors.cardBorder}`,
              borderRadius: 8,
              marginBottom: '16px',
              maxWidth: '100%',
              boxSizing: 'border-box',
            }}
          >
            <div
              style={{
                display: 'flex',
                justifyContent: 'space-between',
                alignItems: 'flex-start',
                gap: 8,
                width: '100%',
                minWidth: 0,
              }}
            >
              <div style={{ flex: 1, minWidth: 0, overflow: 'hidden' }}>
                <Typography.Text
                  ellipsis={{ tooltip: selectedFile.name }}
                  style={{
                    display: 'block',
                    width: '100%',
                    color: isDark ? '#69b7ff' : '#1677ff',
                    fontWeight: 500,
                  }}
                >
                  {selectedFile.name}
                </Typography.Text>
                <span style={{ fontSize: 12, opacity: 0.75, color: colors.textSecondary }}>
                  {(selectedFile.size / 1024).toFixed(1)} KB
                </span>
              </div>
              <Button
                type="text"
                danger
                size="small"
                icon={<DeleteOutlined />}
                onClick={handleClearSelectedFile}
                style={{ flexShrink: 0 }}
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
            marginBottom: '16px',
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
                label: compactDocumentProcessing ? (
                  <Space style={{ padding: '4px 0' }}>
                    <ThunderboltOutlined />
                    <span style={{ fontWeight: 500 }}>Quick Process</span>
                  </Space>
                ) : (
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
                label: compactDocumentProcessing ? (
                  <Space style={{ padding: '4px 0' }}>
                    <CloudUploadOutlined />
                    <span style={{ fontWeight: 500 }}>LlamaParse</span>
                  </Space>
                ) : (
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
            !compactDocumentProcessing ? (
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
            ) : null
          ) : (
            <>
              {!compactDocumentProcessing && (
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
              )}
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

                    </div>
                  ),
                },
              ]}
              />
            </>
          )}
        </div>

        {/* 主操作：置于 AI Analysis Template 之上；无选中文件时禁用（与 YouTube 弹窗一致） */}
        <Divider style={{ margin: '20px 0 16px', borderColor: colors.cardBorder }} />
        <div
          style={{
            marginBottom: 20,
            display: 'flex',
            justifyContent: compactDocumentProcessing ? 'flex-start' : 'flex-end',
            alignItems: 'center',
            flexWrap: 'wrap',
            gap: 8,
          }}
        >
          <Button
            type="primary"
            icon={<PlayCircleOutlined />}
            onClick={handleStartProcessing}
            disabled={!selectedFile}
          >
            Start Processing
          </Button>
        </div>

        {/* AI 分析模板 (可选, 选中后上传完成自动并行触发 analyze_document_task) */}
        <Divider style={{ margin: '16px 0', borderColor: colors.cardBorder }} />
        <UploadAnalysisTemplatePicker
          value={analyzePicker}
          onChange={setAnalyzePicker}
          mode="optional"
          textColor={colors.text}
          textSecondaryColor={colors.textSecondary}
        />

        {/* PR-5: 自动打标模板 (可选, 选中后用自定义模板代替默认 content_tag_v1) */}
        <Divider style={{ margin: '16px 0', borderColor: colors.cardBorder }} />
        <UploadAnalysisTemplatePicker
          value={tagPicker}
          onChange={setTagPicker}
          mode="optional"
          moduleCode="file_tagging"
          label="AI Tagging Template"
          hint="Optional. Toggle on to use your own tagging template (sector / industry / tickers / concepts)."
          textColor={colors.text}
          textSecondaryColor={colors.textSecondary}
        />
      </Form>
    </Modal>
    </ConfigProvider>
  );
};

export default UploadFilesModal;
