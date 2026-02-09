import React, { useState, useEffect, useCallback, useRef } from 'react';
import { Modal, Upload, Button, Form, Select, message, Space, Tag, Divider, Cascader, AutoComplete, Input, Switch, InputNumber, Row, Col, Collapse, ConfigProvider, theme as antdTheme } from 'antd';
import { InboxOutlined, PlayCircleOutlined, DeleteOutlined, SettingOutlined, ExclamationCircleOutlined, ReloadOutlined, LoadingOutlined, FileTextOutlined, RobotOutlined, UploadOutlined } from '@ant-design/icons';
import { filesAPI } from '../services/files';
import { tasksAPI } from '../services/tasks';
import { useNavigate } from 'react-router-dom';
import { getErrorMessage } from '../utils/errorHandler';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import TemplateSelector from './TemplateSelector';
import TemplateVariablesEditor from './TemplateVariablesEditor';

const { Option } = Select;

interface UploadFilesModalProps {
  visible: boolean;
  onCancel: () => void;
  themeMode: 'light' | 'dark';
}

interface ProcessingParams {
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

interface FileItem {
  id: string;
  name: string;
  path: string;
  size: number;
  fileId?: number;
  status?: 'ready' | 'pending' | 'processing' | 'completed' | 'failed';
  error_message?: string;
}

interface TaskStatus {
  status: string;
  progress: number;
  current_file?: string;
  files_processed: number;
  files_failed: number;
  total_files: number;
  logs: Array<{
    timestamp: string;
    level: string;
    message: string;
  }>;
}

interface ProcessingResult {
  task_id: string;
  files_processed: number;
  files_failed: number;
  total_files: number;
  processing_time: number;
}

const INPUT_CONTROL_HEIGHT = 32;

const UploadFilesModal: React.FC<UploadFilesModalProps> = ({ visible, onCancel, themeMode }) => {
  const navigate = useNavigate();
  const [form] = Form.useForm();
  const isDark = themeMode === 'dark';
  const { addTask } = useFileProcessing();

  const inputStyle: React.CSSProperties = { width: '100%', height: INPUT_CONTROL_HEIGHT };
  
  // 文件相关状态
  const [files, setFiles] = useState<FileItem[]>([]);
  const [uploadingFiles, setUploadingFiles] = useState(false);
  const tickerSearchDebounceRef = useRef<NodeJS.Timeout | null>(null);

  // 行业和股票选择
  const [sectors, setSectors] = useState<any[]>([]);
  const [tickerOptions, setTickerOptions] = useState<any[]>([]);
  const [selectedSector, setSelectedSector] = useState<string>('');
  const [selectedIndustry, setSelectedIndustry] = useState<string>('');
  const [selectedTicker, setSelectedTicker] = useState<string>('');
  const [tickerSearchValue, setTickerSearchValue] = useState<string>('');
  const [loadingSectors, setLoadingSectors] = useState(false);
  const [loadingTickers, setLoadingTickers] = useState(false);
  
  // 分析模式
  const [analysisMode, setAnalysisMode] = useState<'remote_agent' | 'local_llm'>('local_llm');
  const [taskTypes, setTaskTypes] = useState<any[]>([]);
  const [selectedTaskType, setSelectedTaskType] = useState<string>('');
  const [customInputs, setCustomInputs] = useState<Record<string, string>>({});
  const [selectedTemplateId, setSelectedTemplateId] = useState<number | undefined>(undefined);
  const [templateVariables, setTemplateVariables] = useState<Record<string, any>>({});
  const [taskTypesLoadAttempts, setTaskTypesLoadAttempts] = useState(0);
  const [lastTaskTypesError, setLastTaskTypesError] = useState(0);
  const maxTaskTypesRetries = 3;
  
  // 参数设置
  const [processingParams, setProcessingParams] = useState<ProcessingParams>({
    language: ['ch_sim', 'en'],
    fast_mode: false,
    disable_ocr: false,
    max_pages: 10,
    target_pages: '',
    skip_diagonal_text: false,
    skip_table_extraction: false,
    extract_images: false,
    use_vendor_multimodal_model: false,
    vendor_multimodal_api_key: '',
    polling_interval: 10,
    use_ai_agent: true,
    llamaindex_api_key: '',
    ai_agent_server_url: ''
  });
  
  // AI Analysis 启用开关
  const [enableAIAnalysis, setEnableAIAnalysis] = useState(true);

  // 加载初始数据
  useEffect(() => {
    if (visible) {
      loadSectorOptions();
      loadTaskTypes();
      loadTemplates();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- loadTaskTypes/loadTemplates stable; avoid re-running on their identity change
  }, [visible]);

  // 根据sector/industry加载tickers
  useEffect(() => {
    if (selectedSector && selectedIndustry) {
      loadTickerOptions();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- loadTickerOptions depends on selectedSector/Industry, avoid loop
  }, [selectedSector, selectedIndustry]);

  // 卸载时清除 ticker 搜索防抖定时器
  useEffect(() => {
    return () => {
      if (tickerSearchDebounceRef.current) {
        clearTimeout(tickerSearchDebounceRef.current);
      }
    };
  }, []);

  const loadSectorOptions = async () => {
    setLoadingSectors(true);
    try {
      const sectorsData = await filesAPI.getSectors();
      setSectors(sectorsData);
    } catch (error) {
      console.error('Failed to load sectors:', error);
    } finally {
      setLoadingSectors(false);
    }
  };

  const loadTickerOptions = async (searchValue?: string) => {
    try {
      setLoadingTickers(true);
      const tickers = await filesAPI.searchTickers({
        search: searchValue || '',
        sector: selectedSector,
        industry: selectedIndustry
      });
      // 后端返回 code/name/value/label，兼容旧字段 ticker/company_name
      const formattedOptions = tickers.map((ticker: any) => {
        const code = ticker.code ?? ticker.ticker ?? ticker.value;
        const name = ticker.name ?? ticker.company_name ?? '';
        const label = ticker.label ?? `${code} - ${name}`;
        return {
          value: code,
          label,
          sector: ticker.sector,
          industry: ticker.industry,
          company_name: name
        };
      });
      setTickerOptions(formattedOptions);
    } catch (error) {
      console.error('Failed to load tickers:', error);
    } finally {
      setLoadingTickers(false);
    }
  };

  const loadTaskTypes = async (forceRetry = false) => {
    if (!forceRetry && taskTypesLoadAttempts >= maxTaskTypesRetries) {
      return;
    }

    try {
      const data = await tasksAPI.getTaskTypes();
      setTaskTypes(data);
      const defaultTask = data.find((t: any) => t.is_default);
      if (defaultTask) {
        setSelectedTaskType(defaultTask.id);
      }
      setTaskTypesLoadAttempts(0);
      setLastTaskTypesError(0);
    } catch (error) {
      console.error('Failed to load task types:', error);
      const now = Date.now();
      if (now - lastTaskTypesError > 5000) {
        setTaskTypesLoadAttempts(prev => prev + 1);
        setLastTaskTypesError(now);
        
        if (taskTypesLoadAttempts < maxTaskTypesRetries - 1) {
          setTimeout(() => loadTaskTypes(), 2000);
        }
      }
    }
  };

  const loadTemplates = async () => {
    try {
      // Templates are loaded via TemplateSelector component
      // No need to load manually
    } catch (error) {
      console.error('Failed to load templates:', error);
    }
  };

  // 处理行业选择（Cascader）
  const handleSectorIndustryChange = (value: any) => {
    if (value && value.length === 2) {
      setSelectedSector(value[0]);
      setSelectedIndustry(value[1]);
      setSelectedTicker('');
      setTickerSearchValue('');
    } else {
      setSelectedSector('');
      setSelectedIndustry('');
      setSelectedTicker('');
      setTickerSearchValue('');
    }
  };

  // Ticker 模糊搜索防抖
  const handleTickerSearch = useCallback((searchValue: string) => {
    setTickerSearchValue(searchValue);
    if (tickerSearchDebounceRef.current) {
      clearTimeout(tickerSearchDebounceRef.current);
      tickerSearchDebounceRef.current = null;
    }
    if (!searchValue.trim()) {
      setTickerOptions([]);
      return;
    }
    tickerSearchDebounceRef.current = setTimeout(() => {
      tickerSearchDebounceRef.current = null;
      loadTickerOptions(searchValue.trim());
    }, 300);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- loadTickerOptions uses sector/industry from closure
  }, [selectedSector, selectedIndustry]);

  // Ticker 选择处理（选中后显示完整 label，如 "AAPL - Apple Inc"）
  const handleTickerSelect = useCallback((value: string, option: any) => {
    setSelectedTicker(value);
    setTickerSearchValue(option?.label ?? value);
    if (option?.sector && option?.industry) {
      setSelectedSector(option.sector);
      setSelectedIndustry(option.industry);
    }
  }, []);

  // Task Type 变更处理
  const handleTaskTypeChange = (taskId: string) => {
    setSelectedTaskType(taskId);
    const task = taskTypes.find(t => t.id === taskId);
    if (task && task.input_fields) {
      const newInputs: Record<string, string> = {};
      task.input_fields.forEach((field: any) => {
        newInputs[field.name] = customInputs[field.name] || '';
      });
      setCustomInputs(newInputs);
    }
  };

  // 渲染自定义输入字段
  const renderCustomInputs = () => {
    const selectedTask = taskTypes.find(t => t.id === selectedTaskType);
    if (!selectedTask || !selectedTask.input_fields || selectedTask.input_fields.length === 0) {
      return null;
    }

    return (
      <Form.Item label="Additional Inputs" style={{ marginTop: 16 }}>
        <Space direction="vertical" style={{ width: '100%' }}>
          {selectedTask.input_fields.map((field: any) => (
            <Input.TextArea
              key={field.name}
              placeholder={`${field.name} (max ${field.max_length} characters)`}
              value={customInputs[field.name] || ''}
              onChange={(e) => {
                const value = e.target.value.slice(0, field.max_length);
                setCustomInputs(prev => ({ ...prev, [field.name]: value }));
              }}
              maxLength={field.max_length}
              rows={3}
              showCount
            />
          ))}
        </Space>
      </Form.Item>
    );
  };

  const handleUpload = async (file: File) => {
    if (files.length > 0) {
      message.warning('Only one file can be uploaded at a time. Please delete the existing file first.');
      return false;
    }

    setUploadingFiles(true);
    try {
      const uploadedFiles = await filesAPI.uploadFiles([file]);
      const newFile: FileItem = {
        id: `${Date.now()}`,
        name: uploadedFiles[0].filename,
        path: uploadedFiles[0].file_path,
        size: uploadedFiles[0].file_size,
        fileId: (uploadedFiles[0] as any).id,
      };
      setFiles([newFile]);

      // 如果已选择标签，自动设置
      if (selectedSector && selectedIndustry && newFile.fileId) {
        try {
          await filesAPI.batchCreateFileTags({
            file_ids: [newFile.fileId],
            sector: selectedSector,
            industry: selectedIndustry,
            ticker: selectedTicker || undefined,
          });
          message.success('File uploaded and tagged successfully');
        } catch (tagError) {
          message.warning('File uploaded but tagging failed');
        }
      } else {
        message.success('File uploaded successfully');
      }
    } catch (error: any) {
      const errorMessage = getErrorMessage(error, 'Failed to upload file');
      message.error(errorMessage);
    } finally {
      setUploadingFiles(false);
    }

    return false;
  };

  const handleRemoveFile = async (fileId: string) => {
    const file = files.find(f => f.id === fileId);
    if (!file) return;

    try {
      const filename = file.path.split('/').pop() || file.name;
      await filesAPI.deleteUploadedFile(filename);
      setFiles([]);
      message.success('File deleted successfully');
    } catch (error: any) {
      const errorMessage = getErrorMessage(error, 'Failed to delete file');
      message.error(errorMessage);
    }
  };

  const handleStartProcessing = async () => {
    // 验证必填项
    if (files.length === 0) {
      message.warning('Please upload a file first');
      return;
    }

    if (analysisMode === 'remote_agent' && !selectedTaskType) {
      message.warning('Please select an AI Agent');
      return;
    }

    if (analysisMode === 'local_llm' && !selectedTemplateId) {
      message.warning('Please select a Prompt Template');
      return;
    }

    try {
      message.info('Starting file processing...');

      const response = await filesAPI.processFiles({
        file_paths: files.map(f => f.path),
        task_type: analysisMode === 'local_llm' ? 'local_llm_template' : selectedTaskType,
        custom_inputs: customInputs,
        params: {
          ...processingParams,
          sector: selectedSector,
          industry: selectedIndustry,
          ticker: selectedTicker,
        },
        analysis_mode: analysisMode,
        template_id: analysisMode === 'local_llm' ? selectedTemplateId : undefined,
        template_code: undefined,
        template_variables: analysisMode === 'local_llm' ? templateVariables : {},
        llm_provider: undefined,
        llm_model: undefined,
      });

      // 添加任务到全局进度显示
      addTask(response.task_id, files[0].name, 'document');
      message.success('File processing task started. Check progress in NavBar.');
      
      // 立即关闭弹窗
      resetAndClose();
    } catch (error: any) {
      const errorMessage = getErrorMessage(error, 'Failed to start processing');
      message.error(errorMessage);
      console.error('Failed to start processing:', error);
    }
  };

  const handleClose = () => {
    resetAndClose();
  };

  const resetAndClose = () => {
    // 清理状态
    setFiles([]);
    setSelectedSector('');
    setSelectedIndustry('');
    setSelectedTicker('');
    setAnalysisMode('local_llm');
    setEnableAIAnalysis(true); // 重置AI Analysis开关
    setProcessingParams({
      language: ['ch_sim', 'en'],
      fast_mode: false,
      disable_ocr: false,
      max_pages: 10,
      target_pages: '',
      skip_diagonal_text: false,
      skip_table_extraction: false,
      extract_images: false,
      use_vendor_multimodal_model: false,
      vendor_multimodal_api_key: '',
      polling_interval: 10,
      use_ai_agent: true,
      llamaindex_api_key: '',
      ai_agent_server_url: ''
    });
    form.resetFields();
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
          maxHeight: '85vh',
          overflowY: 'auto' as const,
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
          background: isDark ? 'rgba(0, 0, 0, 0.6)' : undefined,
          backdropFilter: 'blur(6px)',
          WebkitBackdropFilter: 'blur(6px)',
        }}
        styles={modalStyles}
        bodyStyle={
          modalStyles
            ? undefined
            : {
                maxHeight: '85vh',
                overflowY: 'auto',
              }
        }
      >
      <Form form={form} layout="vertical" style={{ padding: '8px 0' }}>
        {/* 文件上传区域 */}
        {files.length === 0 ? (
          <Upload.Dragger
            beforeUpload={handleUpload}
            showUploadList={false}
            disabled={uploadingFiles}
            style={{
              background: isDark ? '#212936' : '#fafafa',
              border: `1px dashed ${isDark ? '#364152' : '#d9d9d9'}`,
            }}
          >
            <p className="ant-upload-drag-icon">
              <InboxOutlined style={{ color: isDark ? '#1890ff' : undefined }} />
            </p>
            <p className="ant-upload-text" style={{ color: isDark ? '#e0e6f1' : undefined }}>
              Click or drag file to this area to upload
            </p>
            <p className="ant-upload-hint" style={{ color: isDark ? '#94a3b8' : undefined }}>
              Support PDF, Excel, Word, Image and more (for AI Analysis)
            </p>
          </Upload.Dragger>
        ) : (
          <div
            style={{
              padding: '16px',
              background: isDark ? '#212936' : '#fafafa',
              borderRadius: '8px',
              marginBottom: '16px',
            }}
          >
            {files.map((file) => (
              <div key={file.id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <Tag color="blue">{file.name}</Tag>
                <Button
                  type="text"
                  danger
                  size="small"
                  icon={<DeleteOutlined />}
                  onClick={() => handleRemoveFile(file.id)}
                />
              </div>
            ))}
          </div>
        )}

        <Divider style={{ margin: '16px 0' }} />

        {/* AI Analysis 启用开关 */}
        <Form.Item 
          label={
            <span style={{ fontWeight: 600, fontSize: '14px' }}>
              Enable AI Analysis & Document Processing
            </span>
          }
          tooltip="Turn off to upload files only without AI analysis or document processing"
          style={{ marginBottom: '16px' }}
        >
          <Switch
            checked={enableAIAnalysis}
            onChange={(checked) => {
              setEnableAIAnalysis(checked);
              if (!checked) {
                // 当禁用AI Analysis时，清除相关选择
                setSelectedTaskType('');
                setSelectedTemplateId(undefined);
                setTemplateVariables({});
              }
            }}
            checkedChildren="Enabled"
            unCheckedChildren="Disabled"
          />
          {!enableAIAnalysis && (
            <div style={{
              marginTop: '8px',
              padding: '8px 12px',
              backgroundColor: isDark ? 'rgba(250, 173, 20, 0.15)' : '#fffbe6',
              border: `1px solid ${isDark ? 'rgba(250, 173, 20, 0.45)' : '#ffe58f'}`,
              borderRadius: '6px',
              fontSize: '12px',
              color: '#faad14'
            }}>
              Files will be uploaded only. No AI analysis or document processing will be performed.
            </div>
          )}
        </Form.Item>

        {/* AI Analysis */}
        {enableAIAnalysis && (
        <div style={{ marginBottom: '16px' }}>
          <div style={{
            fontSize: '14px',
            fontWeight: 600,
            marginBottom: '12px',
            color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
          }}>
            AI Analysis
          </div>
          <Form.Item label="Analysis Mode" style={{ marginBottom: '12px' }}>
            <Select
              value={analysisMode}
              onChange={(value) => {
                setAnalysisMode(value);
                if (value === 'local_llm') {
                  setSelectedTaskType('');
                } else {
                  setSelectedTemplateId(undefined);
                  setTemplateVariables({});
                }
              }}
              style={inputStyle}
            >
              <Option value="local_llm">
                <Space>
                  <FileTextOutlined />
                  <span>Local AI Agent</span>
                </Space>
              </Option>
              <Option value="remote_agent">
                <Space>
                  <RobotOutlined />
                  <span>Remote AI Agent (Dify)</span>
                </Space>
              </Option>
            </Select>
          </Form.Item>

          {/* Agent/Template Selection */}
          {analysisMode === 'remote_agent' ? (
          <>
            <Form.Item label="AI Agent Selection">
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
                {taskTypes.map((task, index) => {
                  const tagColors = ['blue', 'green', 'orange', 'red', 'purple', 'cyan', 'magenta', 'volcano', 'geekblue', 'gold'];
                  const color = tagColors[index % tagColors.length];
                  const isSelected = selectedTaskType === task.id;
                  
                  return (
                    <Tag
                      key={task.id}
                      color={isSelected ? color : undefined}
                      style={{
                        cursor: 'pointer',
                        border: isSelected ? '2px solid #1890ff' : `1px solid ${isDark ? '#364152' : '#d9d9d9'}`,
                        backgroundColor: isSelected ? undefined : (isDark ? '#212936' : '#f5f5f5'),
                        fontWeight: isSelected ? 'bold' : 'normal',
                        padding: '4px 12px'
                      }}
                      onClick={() => handleTaskTypeChange(task.id)}
                    >
                      {task.name} {task.is_default && '[Default]'}
                    </Tag>
                  );
                })}
              </div>
              {taskTypes.length === 0 && (
                <div style={{ textAlign: 'center', padding: '20px' }}>
                  {taskTypesLoadAttempts >= maxTaskTypesRetries ? (
                    <div>
                      <div style={{ color: '#ff4d4f', marginBottom: '10px' }}>
                        <ExclamationCircleOutlined /> RC Agent加载失败
                      </div>
                      <Button 
                        type="primary" 
                        size="small" 
                        onClick={() => {
                          setTaskTypesLoadAttempts(0);
                          setLastTaskTypesError(0);
                          loadTaskTypes(true);
                        }}
                        icon={<ReloadOutlined />}
                      >
                        重新加载
                      </Button>
                    </div>
                  ) : taskTypesLoadAttempts > 0 ? (
                    <div style={{ color: '#faad14' }}>
                      <LoadingOutlined /> 正在重试加载RC Agent... ({taskTypesLoadAttempts}/{maxTaskTypesRetries})
                    </div>
                  ) : (
                    <div style={{ color: isDark ? '#94a3b8' : '#999' }}>
                      No AI Agents available
                    </div>
                  )}
                </div>
              )}
            </Form.Item>
            {renderCustomInputs()}
          </>
        ) : (
          <>
            <Form.Item label="Select Prompt Template" required>
              <TemplateSelector
                categoryCode="file_analysis"
                value={selectedTemplateId}
                onChange={(templateId, template) => {
                  setSelectedTemplateId(templateId);
                  setTemplateVariables({});
                }}
                style={inputStyle}
              />
            </Form.Item>
            {selectedTemplateId && (
              <Form.Item label="Template Parameters Configuration">
                <TemplateVariablesEditor
                  templateId={selectedTemplateId}
                  values={templateVariables}
                  onChange={(values) => setTemplateVariables(values)}
                />
              </Form.Item>
            )}
          </>
        )}
        </div>
        )}

        {/* Industry & Ticker Selection (after AI Analysis) */}
        {enableAIAnalysis && (
        <Form.Item
          label="Industry & Ticker Selection"
          tooltip="Optional. Select sector and industry for analysis context and file tagging"
          style={{ marginBottom: '12px' }}
        >
          <Space direction="vertical" style={{ width: '100%' }} size="small">
            <Cascader
              options={sectors}
              value={selectedSector && selectedIndustry ? [selectedSector, selectedIndustry] : []}
              onChange={handleSectorIndustryChange}
              placeholder="Select sector and industry (optional)"
              loading={loadingSectors}
              showSearch={{
                filter: (inputValue, path) => {
                  return path.some(option =>
                    option.label.toLowerCase().includes(inputValue.toLowerCase())
                  );
                }
              }}
              style={inputStyle}
              dropdownStyle={{ fontSize: '11px' }}
            />

            <AutoComplete
              value={tickerSearchValue}
              options={tickerOptions.map(ticker => ({
                value: ticker.value,
                label: ticker.label,
                sector: ticker.sector,
                industry: ticker.industry
              }))}
              onSearch={handleTickerSearch}
              onSelect={handleTickerSelect}
              placeholder="Search ticker by code or name (global search)"
              style={inputStyle}
              filterOption={false}
              notFoundContent={loadingTickers ? 'Loading...' : 'No tickers found'}
              dropdownStyle={{ fontSize: '11px' }}
            />

            {(selectedSector && selectedIndustry) && (
              <div style={{
                padding: '8px 12px',
                backgroundColor: isDark ? 'rgba(82, 196, 26, 0.15)' : '#f6ffed',
                border: `1px solid ${isDark ? 'rgba(82, 196, 26, 0.45)' : '#b7eb8f'}`,
                borderRadius: '6px',
                fontSize: '12px',
                color: '#52c41a'
              }}>
                <strong>Selected:</strong> {selectedSector} → {selectedIndustry}
                {selectedTicker && <><br /><strong>Ticker:</strong> {selectedTicker}</>}
                {!selectedTicker && <><br /><em>No ticker selected (optional)</em></>}
              </div>
            )}
          </Space>
        </Form.Item>
        )}

        {/* LlamaParse / Document Processing - only when AI Analysis is enabled */}
        {enableAIAnalysis && (
        <div style={{ marginTop: '8px', marginBottom: '8px' }}>
          <div style={{
            fontSize: '14px',
            fontWeight: 600,
            marginBottom: '8px',
            color: isDark ? '#e0e6f1' : 'rgba(0, 0, 0, 0.88)',
          }}>
            Document Processing (LlamaParse Server)
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
                          onChange={(value) => setProcessingParams(prev => ({ ...prev, max_pages: value || 10 }))}
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

                  <Row gutter={[12, 12]}>
                    <Col span={24}>
                      <Form.Item label="RC Agent Server URL" style={{ marginBottom: '12px' }}>
                        <Input
                          value={processingParams.ai_agent_server_url}
                          onChange={(e) => setProcessingParams(prev => ({ ...prev, ai_agent_server_url: e.target.value }))}
                          placeholder="Enter RC Agent server URL"
                          style={inputStyle}
                        />
                      </Form.Item>
                    </Col>
                  </Row>

                  <Divider style={{ margin: '12px 0' }} />

                  <Row gutter={[12, 12]}>
                    <Col span={8}>
                      <Form.Item label="Fast Mode" style={{ marginBottom: '8px' }}>
                        <Switch
                          size="small"
                          checked={processingParams.fast_mode}
                          onChange={(checked) => setProcessingParams(prev => ({ ...prev, fast_mode: checked }))}
                        />
                      </Form.Item>
                    </Col>
                    <Col span={8}>
                      <Form.Item label="Disable OCR" style={{ marginBottom: '8px' }}>
                        <Switch
                          size="small"
                          checked={processingParams.disable_ocr}
                          onChange={(checked) => setProcessingParams(prev => ({ ...prev, disable_ocr: checked }))}
                        />
                      </Form.Item>
                    </Col>
                    <Col span={8}>
                      <Form.Item label="Use RC Agent" style={{ marginBottom: '8px' }}>
                        <Switch
                          size="small"
                          checked={processingParams.use_ai_agent}
                          onChange={(checked) => setProcessingParams(prev => ({ ...prev, use_ai_agent: checked }))}
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
        </div>
        )}

        {/* 提示信息 */}
        {files.length > 0 && (
          <div
            style={{
              padding: '12px 16px',
              background: isDark ? (enableAIAnalysis ? 'rgba(24, 144, 255, 0.1)' : 'rgba(82, 196, 26, 0.1)') : (enableAIAnalysis ? '#e6f7ff' : '#f6ffed'),
              border: `1px solid ${isDark ? (enableAIAnalysis ? 'rgba(24, 144, 255, 0.3)' : 'rgba(82, 196, 26, 0.3)') : (enableAIAnalysis ? '#91d5ff' : '#b7eb8f')}`,
              borderRadius: '8px',
              marginBottom: '16px',
            }}
          >
            <p style={{ margin: 0, fontSize: '13px', color: isDark ? (enableAIAnalysis ? '#91d5ff' : '#95de64') : (enableAIAnalysis ? '#1890ff' : '#52c41a') }}>
              {enableAIAnalysis 
                ? 'File uploaded successfully. Click "Start Processing" to analyze. Progress will be shown in NavBar.'
                : 'File uploaded successfully. You can now close this window or upload another file.'
              }
            </p>
          </div>
        )}

        {/* 操作按钮 */}
        <div style={{ marginTop: '16px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Button onClick={handleClose}>
            Close
          </Button>
          <Space>
            {/* 显示 Start Processing (仅当启用AI Analysis且有文件时) */}
            {files.length > 0 && enableAIAnalysis && (
              <Button
                type="primary"
                icon={<PlayCircleOutlined />}
                onClick={handleStartProcessing}
                disabled={
                  (analysisMode === 'remote_agent' && !selectedTaskType) ||
                  (analysisMode === 'local_llm' && !selectedTemplateId)
                }
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
