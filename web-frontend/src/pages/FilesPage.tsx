import React, { useState, useEffect, useRef, useCallback } from 'react';
import {
  Card,
  Upload,
  Button,
  Progress,
  Typography,
  Space,
  Tag,
  Row,
  Col,
  Select,
  Form,
  Input,
  Switch,
  InputNumber,
  Divider,
  message,
  Modal,
  Table,
  Tooltip,
  Badge,
  Cascader,
  AutoComplete
} from 'antd';
import {
  InboxOutlined,
  PlayCircleOutlined,
  DeleteOutlined,
  UpOutlined,
  DownOutlined,
  StopOutlined,
  SettingOutlined,
  EyeOutlined,
  ReloadOutlined,
  FileTextOutlined,
  FilePdfOutlined,
  FileExcelOutlined,
  FileWordOutlined,
  FileImageOutlined,
  ClockCircleOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
  ClearOutlined,
  DownloadOutlined,
  RobotOutlined,
  FolderOutlined,
  TagOutlined,
  LoadingOutlined
} from '@ant-design/icons';
import { filesAPI, SectorOption, TickerOption } from '../services/files';
import { tasksAPI, TaskExecutionDetail } from '../services/tasks';
import { getErrorMessage } from '../utils/errorHandler';
import { useGlobalLogger } from '../contexts/LoggerContext';
import MarkdownRenderer from '../components/MarkdownRenderer';
import { downloadElementAsPDF } from '../utils/pdfDownload';
import TemplateSelector from '../components/TemplateSelector';
import TemplateVariablesEditor from '../components/TemplateVariablesEditor';
import promptTemplateService, { PromptTemplate } from '../services/promptTemplateService';

const { Text } = Typography;
const { Dragger } = Upload;
const { Option } = Select;

interface FileItem {
  id: string;
  name: string;
  path: string;
  size: number;
  type: string;
  status: 'ready' | 'pending' | 'processing' | 'completed' | 'failed';
  error_message?: string;
  fileId?: number;  // 实际的数据库文件ID
  tag?: {
    sector: string;
    industry: string;
    ticker?: string;
  };
}

interface TaskType {
  id: string;
  name: string;
  token: string;
  input_fields: Array<{
    name: string;
    max_length: number;
  }>;
  is_default: boolean;
}

interface TaskExecution {
  id: string;
  username: string;
  task_name: string;
  created_at: string;
  files_processed: number;
  files_failed: number;
  total_time: number;
  file_processing_time: number;
  ai_processing_time: number;
  total_tokens?: number;
  total_cost?: number;
  rc_agent_status?: string;
  first_filename?: string;  // 第一个文件的名称
  llm_model?: string;  // LLM模型名称
  llm_provider?: string;  // LLM提供商
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
  sector?: string;
  industry?: string;
  ticker?: string;
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

// 新增处理结果接口
interface ProcessingResult {
  task_id: string;
  files_processed: number;
  files_failed: number;
  total_files: number;
  processing_time: number;
  final_report?: string;
}

const FilesPage: React.FC = () => {
  const [files, setFiles] = useState<FileItem[]>([]);
  const [taskTypes, setTaskTypes] = useState<TaskType[]>([]);
  const [selectedTaskType, setSelectedTaskType] = useState<string>('');
  const [customInputs, setCustomInputs] = useState<Record<string, string>>({});
  const [processing, setProcessing] = useState(false);
  const [currentTaskId, setCurrentTaskId] = useState<string | null>(null);
  const [lastCompletedTaskId, setLastCompletedTaskId] = useState<string | null>(null);
  const [taskStatus, setTaskStatus] = useState<TaskStatus | null>(null);
  
  // 新增：分析模式相关状态
  const [analysisMode, setAnalysisMode] = useState<'remote_agent' | 'local_llm'>('local_llm');
  const [selectedTemplateId, setSelectedTemplateId] = useState<number | undefined>(undefined);
  const [templateVariables, setTemplateVariables] = useState<Record<string, any>>({});
  const [showParams, setShowParams] = useState(false);
  const [allTemplates, setAllTemplates] = useState<PromptTemplate[]>([]);
  const [refreshingStatus, setRefreshingStatus] = useState(false);
  const [stoppingTask, setStoppingTask] = useState(false);
  const [updatingStatus, setUpdatingStatus] = useState(false);
  const [updatingFiles, setUpdatingFiles] = useState(false);
  
  // 行业和股票选择相关状态
  const [sectors, setSectors] = useState<SectorOption[]>([]);
  const [selectedSector, setSelectedSector] = useState<string>('');
  const [selectedIndustry, setSelectedIndustry] = useState<string>('');
  const [tickerOptions, setTickerOptions] = useState<TickerOption[]>([]);
  const [selectedTicker, setSelectedTicker] = useState<string>('');
  const [tickerSearchValue, setTickerSearchValue] = useState<string>('');
  const [loadingSectors, setLoadingSectors] = useState(false);
  const [loadingTickers, setLoadingTickers] = useState(false);
  const [showBatchTagModal, setShowBatchTagModal] = useState(false);
  const [batchTagging, setBatchTagging] = useState(false);
  
  // 初始化日志系统
  const logger = useGlobalLogger();
  
  // 日志容器引用
  const logContainerRef = useRef<HTMLDivElement>(null);
  
  // 根据文件类型获取图标
  const getFileTypeIcon = (type: string) => {
    switch (type) {
      case 'pdf':
        return <FilePdfOutlined style={{ color: '#f5222d' }} />;
      case 'excel':
        return <FileExcelOutlined style={{ color: '#52c41a' }} />;
      case 'doc':
        return <FileWordOutlined style={{ color: '#1890ff' }} />;
      case 'image':
        return <FileImageOutlined style={{ color: '#fa8c16' }} />;
      default:
        return <FileTextOutlined style={{ color: '#8c8c8c' }} />;
    }
  };
  
  // 新增状态管理
  const [processingResult, setProcessingResult] = useState<ProcessingResult | null>(null);
  const [showResult, setShowResult] = useState(false);
  
  // 使用ref避免重新渲染导致的循环
  const hasShownCompletionMessageRef = useRef(false);
  
  // 执行历史相关状态
  const [taskExecutions, setTaskExecutions] = useState<TaskExecution[]>([]);
  const [executionDetailVisible, setExecutionDetailVisible] = useState(false);
  const [selectedExecution, setSelectedExecution] = useState<TaskExecution | null>(null);
  const [executionDetailData, setExecutionDetailData] = useState<TaskExecutionDetail | null>(null);
  const [executionDetailLoading, setExecutionDetailLoading] = useState(false);
  const [selectedExecutionIds, setSelectedExecutionIds] = useState<string[]>([]);
  const [deleteExecutionVisible, setDeleteExecutionVisible] = useState(false);
  const [executionToDelete, setExecutionToDelete] = useState<TaskExecution | null>(null);
  const [deletingExecutions, setDeletingExecutions] = useState(false);
  
  // AI Analysis 相关状态
  const [aiAnalysisVisible, setAiAnalysisVisible] = useState(false);
  const [aiAnalysisData, setAiAnalysisData] = useState<TaskExecutionDetail | null>(null);
  const [aiAnalysisLoading, setAiAnalysisLoading] = useState(false);
  const [pdfDownloading, setPdfDownloading] = useState(false);
  
  
  // 文件内容查看相关状态
  const [fileContentVisible, setFileContentVisible] = useState(false);
  const [selectedFileContent, setSelectedFileContent] = useState<any>(null);
  
  // 文件选择相关状态
  const [fileSelectionVisible, setFileSelectionVisible] = useState(false);
  const [availableFiles, setAvailableFiles] = useState<any[]>([]);
  const [selectedExecutionForFiles, setSelectedExecutionForFiles] = useState<any>(null);
  
  const [processingParams, setProcessingParams] = useState<ProcessingParams>({
    language: ['ch_sim', 'en'],
    fast_mode: false,
    disable_ocr: false,
    max_pages: 50,
    target_pages: '',
    skip_diagonal_text: false,
    skip_table_extraction: false,
    extract_images: false,
    use_vendor_multimodal_model: false,
    vendor_multimodal_api_key: '',
    polling_interval: 8, // 调整为与实际轮询间隔一致（秒）
    use_ai_agent: true,
    llamaindex_api_key: '', // 将通过API加载默认值
    ai_agent_server_url: process.env.REACT_APP_RC_AGENT_BASE_URL || 'http://111.230.48.119:81/v1/chat-messages'
    });
    const [form] = Form.useForm();

  // 自动滚动到底部
  useEffect(() => {
    if (logger.autoScroll && logContainerRef.current) {
      logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
    }
  }, [logger.logs, logger.autoScroll]);

  // 加载任务执行历史的函数 - 将在状态声明后重新定义

  // 加载API密钥等关键配置
  const loadDefaultConfig = useCallback(async () => {
    try {
      const response = await filesAPI.getProcessingParams();
      if (response.defaults) {
        setProcessingParams(prev => ({
          ...prev,
          llamaindex_api_key: response.defaults?.llamaindex_api_key || '',
          ai_agent_server_url: response.defaults?.ai_agent_server_url || prev.ai_agent_server_url
        }));
      }
    } catch (error: any) {
      console.error('加载默认配置失败:', error);
      logger.logWarning('加载默认配置失败，使用硬编码默认值', 'file-processing');
    }
  }, [logger, setProcessingParams]);

  // 加载所有模板
  const loadAllTemplates = useCallback(async () => {
    try {
      const templates = await promptTemplateService.listTemplates({
        active_only: true,
      });
      setAllTemplates(templates);
    } catch (error: any) {
      console.error('加载模板列表失败:', error);
    }
  }, []);
  
  // 重置所有左侧输入项为原始状态
  const resetAllInputs = useCallback(() => {
    // 重置文件列表
    setFiles([]);
    
    // 重置任务类型和自定义输入
    setSelectedTaskType('');
    setCustomInputs({});
    
    // 重置分析模式相关
    setAnalysisMode('local_llm');
    setSelectedTemplateId(undefined);
    setTemplateVariables({});
    
    // 重置行业和股票选择
    setSelectedSector('');
    setSelectedIndustry('');
    setSelectedTicker('');
    setTickerSearchValue('');
    setTickerOptions([]);
    
    // 重置处理参数为默认值
    setProcessingParams({
      language: ['ch_sim', 'en'],
      fast_mode: false,
      disable_ocr: false,
      max_pages: 50,
      target_pages: '',
      skip_diagonal_text: false,
      skip_table_extraction: false,
      extract_images: false,
      use_vendor_multimodal_model: false,
      vendor_multimodal_api_key: '',
      polling_interval: 8,
      use_ai_agent: true,
      llamaindex_api_key: '',
      ai_agent_server_url: process.env.REACT_APP_RC_AGENT_BASE_URL || 'http://111.230.48.119:81/v1/chat-messages'
    });
    
    // 重新加载默认配置（包括API密钥等）
    loadDefaultConfig();
    
    logger.logInfo('所有输入项已重置为原始状态', 'file-processing');
  }, [logger, loadDefaultConfig]);

  // 获取显示的Agent名称（将template ID映射为template名称）
  const getDisplayAgentName = useCallback((taskName: string): string => {
    if (!taskName) return '';
    
    // 如果已经是template名称（不是纯数字或ID:xxx格式），直接返回
    // 但是仍然尝试匹配，以防万一
    
    // 尝试解析以下几种格式：
    // 1. 纯数字：如 "123"
    // 2. ID:xxx格式：如 "ID:123"
    let templateId: number | null = null;
    
    // 检查是否是ID:xxx格式
    const idMatch = taskName.match(/^ID:(\d+)$/);
    if (idMatch) {
      templateId = parseInt(idMatch[1], 10);
    } else {
      // 尝试直接解析为数字
      const parsed = parseInt(taskName, 10);
      if (!isNaN(parsed) && parsed.toString() === taskName) {
        templateId = parsed;
      }
    }
    
    // 如果解析到了template ID，尝试在templates列表中查找
    if (templateId !== null && allTemplates.length > 0) {
      const template = allTemplates.find(t => t.id === templateId);
      if (template) {
        return template.template_name;
      }
    }
    
    // 如果找不到对应的template，返回原始的taskName
    return taskName;
  }, [allTemplates]);

  // 添加错误重试状态管理
  const [taskTypesLoadAttempts, setTaskTypesLoadAttempts] = useState(0);
  const [lastTaskTypesError, setLastTaskTypesError] = useState<number>(0);
  const maxTaskTypesRetries = 3; // 最大重试次数
  const retryInterval = 30000; // 重试间隔30秒
  
  // 任务执行历史错误重试状态管理
  const [taskHistoryLoadAttempts, setTaskHistoryLoadAttempts] = useState(0);
  const [lastTaskHistoryError, setLastTaskHistoryError] = useState<number>(0);
  const maxTaskHistoryRetries = 3; // 最大重试次数
  const taskHistoryRetryInterval = 30000; // 重试间隔30秒

  // 加载任务执行历史的函数
  const loadTaskExecutionsFunc = useCallback(async (showError = true, isAutoRefresh = false) => {
    const now = Date.now();
    
    // 如果最近有错误且未超过重试间隔，则跳过（除非是手动刷新）
    if (!isAutoRefresh && lastTaskHistoryError > 0 && (now - lastTaskHistoryError) < taskHistoryRetryInterval) {
      console.log('跳过任务执行历史加载，等待重试间隔...');
      return;
    }
    
    // 如果超过最大重试次数，则停止尝试（除非是手动刷新）
    if (!isAutoRefresh && taskHistoryLoadAttempts >= maxTaskHistoryRetries) {
      console.log('任务执行历史加载失败次数过多，停止尝试');
      return;
    }

    try {
      console.log('开始加载任务执行历史...');
      const executions = await tasksAPI.getTaskExecutions();
      setTaskExecutions(executions);
      
      // 重置错误状态
      setTaskHistoryLoadAttempts(0);
      setLastTaskHistoryError(0);
    } catch (error: any) {
      console.error('加载任务执行历史失败:', error);
      
      // 更新错误状态（只有在非自动刷新时）
      if (!isAutoRefresh) {
        setTaskHistoryLoadAttempts(prev => prev + 1);
        setLastTaskHistoryError(now);
      }
      
      let errorMessage = '加载任务执行历史失败';
      if (error.response?.status === 401) {
        errorMessage += ' - 认证失败，请重新登录';
      } else if (error.response?.status === 403) {
        errorMessage += ' - 权限不足';
      } else if (error.response?.status >= 500) {
        errorMessage += ' - 服务器内部错误';
      } else if (error.message === 'Network Error') {
        errorMessage += ' - Network connection failed';
      } else {
        errorMessage += ` - ${error.response?.data?.detail || error.message}`;
      }
      
      // 只有在显示错误且重试次数未达到上限时才显示错误消息
      if (showError && taskHistoryLoadAttempts < maxTaskHistoryRetries && !isAutoRefresh) {
        // 添加重试信息
        if (taskHistoryLoadAttempts < maxTaskHistoryRetries - 1) {
          errorMessage += ` (${taskHistoryLoadAttempts + 1}/${maxTaskHistoryRetries})`;
        } else {
          errorMessage += ' - 已达到最大重试次数，请检查网络连接';
        }
        
        message.error(errorMessage);
        logger.logError(errorMessage, 'file-processing', error);
      } else if (taskHistoryLoadAttempts >= maxTaskHistoryRetries && !isAutoRefresh) {
        // 最后一次重试失败，显示最终错误消息
        message.error('任务执行历史加载失败，请检查网络连接或联系管理员');
        logger.logError('任务执行历史加载失败，已达到最大重试次数', 'file-processing', error);
      }
    }
  }, [logger, setTaskExecutions, taskHistoryLoadAttempts, lastTaskHistoryError, maxTaskHistoryRetries, taskHistoryRetryInterval]);

  // loadTaskTypes 函数定义需要移到这里
  const loadTaskTypes = useCallback(async (showError = true) => {
    const now = Date.now();
    
    // 如果最近有错误且未超过重试间隔，则跳过
    if (lastTaskTypesError > 0 && (now - lastTaskTypesError) < retryInterval) {
      console.log('跳过RC Agent加载，等待重试间隔...');
      return;
    }
    
    // 如果超过最大重试次数，则停止尝试
    if (taskTypesLoadAttempts >= maxTaskTypesRetries) {
      console.log('RC Agent加载失败次数过多，停止尝试');
      return;
    }

    try {
      console.log('开始加载RC Agent...');
      const types = await tasksAPI.getTaskTypes();
      setTaskTypes(types);
      
      // 重置错误状态
      setTaskTypesLoadAttempts(0);
      setLastTaskTypesError(0);
      
      // 不再自动选择默认任务，让用户手动选择
      // 如果用户之前选择了任务且该任务仍然存在，则保持选择
      if (selectedTaskType && types.find(t => t.id === selectedTaskType)) {
        // 保持当前选择
      } else if (types.length > 0) {
        // 如果没有选择或选择的任务不存在，清空选择
        setSelectedTaskType('');
      }
    } catch (error: any) {
      console.error('failed to load RC Agent:', error);
      
      // 更新错误状态
      setTaskTypesLoadAttempts(prev => prev + 1);
      setLastTaskTypesError(now);
      
      // 只有在显示错误且重试次数未达到上限时才显示错误消息
      if (showError && taskTypesLoadAttempts < maxTaskTypesRetries) {
        // 显示详细的错误信息
        let errorMessage = 'failed to load RC Agent';
        if (error.response?.status === 401) {
          errorMessage += ' - authentication failed, please login again';
        } else if (error.response?.status === 403) {
          errorMessage += ' - not enough permissions';
        } else if (error.response?.status === 404) {
          errorMessage += ' - API endpoint not found';
        } else if (error.response?.status >= 500) {
          errorMessage += ' - server internal error';
        } else if (error.message === 'Network Error') {
          errorMessage += ' - network connection failed';
        } else {
          errorMessage += ` - ${error.response?.data?.detail || error.message}`;
        }
        
        // 添加重试信息
        if (taskTypesLoadAttempts < maxTaskTypesRetries - 1) {
          errorMessage += ` (${taskTypesLoadAttempts + 1}/${maxTaskTypesRetries})`;
        } else {
          errorMessage += ' - maximum number of retries reached, please check network connection';
        }
        
        message.error(errorMessage);
        logger.logError(errorMessage, 'file-processing', error);
      } else if (taskTypesLoadAttempts >= maxTaskTypesRetries) {
        // 最后一次重试失败，显示最终错误消息
        message.error('failed to load RC Agent, please check network connection or contact administrator');
        logger.logError('failed to load RC Agent, maximum number of retries reached', 'file-processing', error);
      }
    }
  }, [logger, setSelectedTaskType, setTaskTypes, selectedTaskType, taskTypesLoadAttempts, lastTaskTypesError, maxTaskTypesRetries, retryInterval]);

  // 加载RC Agent和执行历史
  useEffect(() => {
    loadTaskTypes();
    loadTaskExecutionsFunc();
    loadDefaultConfig();
    loadAllTemplates();
    
    // 设置定时刷新执行历史列表
    const refreshInterval = setInterval(() => {
      // 静默刷新，不显示错误消息，标记为自动刷新
      loadTaskExecutionsFunc(false, true).catch(() => {
        // 静默处理错误，避免在自动刷新时显示错误消息
      });
    }, 30000); // 每30秒自动刷新一次执行历史列表
    
    return () => {
      clearInterval(refreshInterval);
    };
  }, [loadDefaultConfig, loadTaskExecutionsFunc, loadTaskTypes, loadAllTemplates]);

  // 加载行业分类数据
  useEffect(() => {
    const loadSectors = async () => {
      try {
        setLoadingSectors(true);
        const sectorsData = await filesAPI.getSectors();
        setSectors(sectorsData);
      } catch (error) {
        console.error('failed to load sectors:', error);
        message.error('failed to load sectors');
      } finally {
        setLoadingSectors(false);
      }
    };

    loadSectors();
  }, []);

  // 当选择行业后加载股票代码
  useEffect(() => {
    const loadTickers = async () => {
      if (!selectedSector || !selectedIndustry) {
        setTickerOptions([]);
        return;
      }

      try {
        setLoadingTickers(true);
        const tickers = await filesAPI.searchTickers({
          sector: selectedSector,
          industry: selectedIndustry
        });
        setTickerOptions(tickers);
      } catch (error) {
        console.error('failed to load tickers:', error);
        message.error('failed to load tickers');
      } finally {
        setLoadingTickers(false);
      }
    };

    loadTickers();
  }, [selectedSector, selectedIndustry]);

  // 更新处理参数中的行业和股票信息
  useEffect(() => {
    setProcessingParams(prev => ({
      ...prev,
      sector: selectedSector,
      industry: selectedIndustry,
      ticker: selectedTicker
    }));
  }, [selectedSector, selectedIndustry, selectedTicker]);

  // 自动更新文件状态的函数
  const updateFileStatuses = useCallback(async (taskId: string) => {
    if (updatingFiles) {
      console.log('skipping file status update - previous request is still in progress');
      return; // 防止并发请求
    }
    
    setUpdatingFiles(true);
    try {
      const taskFiles = await filesAPI.getTaskFiles(taskId);
      
      setFiles(prev => prev.map(file => {
        const taskFile = taskFiles.find(tf => tf.filename === file.name);
        if (taskFile) {
          return {
            ...file,
            status: taskFile.status as 'ready' | 'pending' | 'processing' | 'completed' | 'failed',
            error_message: taskFile.error_message
          };
        }
        return file;
      }));
    } catch (error) {
      console.error('failed to update file status:', error);
    } finally {
      setUpdatingFiles(false);
    }
  }, [updatingFiles]);

  // 优化的任务状态更新函数
  const updateTaskStatus = useCallback(async () => {
    if (!currentTaskId || updatingStatus) {
      if (updatingStatus) {
        console.log('skipping task status update - previous request is still in progress');
      }
      return; // 防止并发请求
    }
    
    setUpdatingStatus(true);
    try {
      const status = await filesAPI.getTaskStatus(currentTaskId);
      
      // 同步日志到新的日志系统
      if (status.logs && status.logs.length > 0) {
        // 定义关键日志的关键词，只显示这些关键节点的日志
        const keyLogPatterns = [
          /文件.*处理成功/,
          /文件.*处理失败/,
          /✅.*LLM分析完成/,
          /❌.*失败/,
          /批量处理完成/,
          /处理结果已保存/,
          /文件处理完成/,
          /任务.*启动/,
          /任务.*完成/,
          /任务.*失败/,
          /错误/,
          /error/i,
          /failed/i,
          /completed/i,
        ];
        
        // 过滤日志，只保留关键节点
        const filteredLogs = status.logs.filter((log) => {
          const message = log.message || '';
          return keyLogPatterns.some(pattern => pattern.test(message));
        });
        
        // 获取当前任务的已存在日志的消息内容集合
        const existingMessages = new Set(
          logger.logs
            .filter(log => log.source === 'file-processing' && log.details?.taskId === currentTaskId)
            .map(log => log.message)
        );
        
        // 对于关键日志，只添加第一次出现的，通过消息内容去重
        filteredLogs.forEach((log) => {
          const message = log.message || '';
          
          // 检查消息是否已存在
          if (!existingMessages.has(message)) {
            logger.actions.addLog({
              level: log.level as any,
              message: log.message,
              source: 'file-processing',
              details: { taskId: currentTaskId }
            });
            // 立即添加到集合中，避免同一轮询中的重复
            existingMessages.add(message);
          }
        });
      }
      
      setTaskStatus(status);
      
      if (status.status === 'completed' || status.status === 'failed') {
        setProcessing(false);
        
        // 自动更新文件状态
        await updateFileStatuses(currentTaskId);
        
        // 设置处理结果
        setProcessingResult({
          task_id: currentTaskId,
          files_processed: status.files_processed,
          files_failed: status.files_failed,
          total_files: status.total_files,
          processing_time: 0, // 这里可以从任务开始时间计算
        });
        
        setLastCompletedTaskId(currentTaskId);
        setCurrentTaskId(null);
        setShowResult(true);
        
        // 重置完成消息标志，为下次任务做准备
        hasShownCompletionMessageRef.current = false;
        
        // 显示任务完成消息
        if (status.status === 'completed') {
          message.success('task processing completed');
          // 处理成功时，重置所有左侧输入项为原始状态
          resetAllInputs();
          // 清空处理结果和显示状态，使界面回到初始状态
          setProcessingResult(null);
          setShowResult(false);
        } else {
          message.error('task processing failed');
          logger.logError('task processing failed', 'file-processing');
          // 处理失败时，保持原样（继续显示reprocess和clear file list）
        }
        
        // 任务完成后刷新执行历史列表
        try {
          const executions = await tasksAPI.getTaskExecutions();
          setTaskExecutions(executions);
        } catch (error) {
          console.error('failed to refresh execution history:', error);
          logger.logError('failed to refresh execution history', 'file-processing', error);
        }
        
        // 延迟一段时间后再次刷新，确保数据同步
        setTimeout(async () => {
          try {
            const executions = await tasksAPI.getTaskExecutions();
            setTaskExecutions(executions);
          } catch (error) {
            console.error('failed to refresh execution history:', error);
            logger.logError('failed to refresh execution history', 'file-processing', error);
          }
        }, 5000); // 等待5秒后再次刷新，确保数据完全同步
      }
    } catch (error) {
      console.error('failed to get task status:', error);
      logger.logError(`failed to get task status: ${error}`, 'file-processing');
    } finally {
      setUpdatingStatus(false);
    }
  }, [currentTaskId, updateFileStatuses, logger, updatingStatus, resetAllInputs]);

  // 定时更新任务状态
  useEffect(() => {
    let interval: NodeJS.Timeout;
    if (processing && currentTaskId) {
      interval = setInterval(() => {
        updateTaskStatus();
      }, 3000); // 优化轮询间隔为3秒，提高响应速度
    }
    return () => {
      if (interval) {
        clearInterval(interval);
      }
    };
  }, [processing, currentTaskId, updateTaskStatus]);

  // 开始处理时自动显示浮动日志窗口
  useEffect(() => {
    if (processing && !logger.showFloating) {
      logger.actions.setShowFloating(true);
    }
  }, [processing, logger.showFloating, logger.actions]);



  const handleViewExecution = async (execution: TaskExecution) => {
    try {
      setSelectedExecution(execution);
      setExecutionDetailVisible(true);
      setExecutionDetailLoading(true);
      
      logger.logInfo(`view task execution details: ${execution.task_name}`, 'file-processing');
      
      // 获取包含文件详情的完整执行信息
      const details = await tasksAPI.getTaskExecutionDetails(execution.id);
      setExecutionDetailData(details);
      
    } catch (error) {
      message.error('failed to get execution details');
      logger.logError(`failed to get execution details: ${execution.task_name}`, 'file-processing', error);
      setExecutionDetailVisible(false);
    } finally {
      setExecutionDetailLoading(false);
    }
  };

  const handleViewAiAnalysis = async (execution: TaskExecution) => {
    try {
      setAiAnalysisLoading(true);
      setAiAnalysisVisible(true);
      
      logger.logInfo(`view AI analysis report: ${execution.task_name}`, 'file-processing');
      
      const details = await tasksAPI.getTaskExecutionDetails(execution.id);
      console.log('AI Analysis Data:', details);
      console.log('Final Report:', details?.execution?.final_report);
      setAiAnalysisData(details);
      
    } catch (error) {
      message.error('Failed to load AI analysis report');
      logger.logError(`Failed to load AI analysis report: ${execution.task_name}`, 'file-processing', error);
      setAiAnalysisVisible(false);
    } finally {
      setAiAnalysisLoading(false);
    }
  };

  const handleDownloadPdf = async () => {
    if (!aiAnalysisData?.execution?.final_report) {
      message.error('No report content available for download');
      return;
    }

    try {
      setPdfDownloading(true);
      
      const filename = `AI_Analysis_Report_${aiAnalysisData.execution.task_name}_${new Date().toISOString().split('T')[0]}.pdf`;
      
      logger.logInfo(`Starting PDF download: ${filename}`, 'file-processing');
      
      // 等待一小段时间确保MarkdownRenderer完全渲染
      await new Promise(resolve => setTimeout(resolve, 500));
      
      await downloadElementAsPDF('ai-analysis-content', {
        filename,
        title: `AI Analysis Report - ${aiAnalysisData.execution.task_name}`,
        author: 'Reverse Capital',
        subject: 'AI Analysis Report'
      });
      
      message.success('PDF report downloaded successfully');
      logger.logInfo(`PDF report downloaded successfully: ${filename}`, 'file-processing');
      
    } catch (error) {
      message.error('PDF download failed, please try again');
      logger.logError(`PDF download failed: ${error}`, 'file-processing', error);
    } finally {
      setPdfDownloading(false);
    }
  };


  const handleViewFileContent = (fileData: any) => {
    setSelectedFileContent(fileData);
    setFileContentVisible(true);
    logger.logInfo(`view AI processed content: ${fileData.filename}`, 'file-processing');
  };

  const handleViewOriginalFile = async (fileData: any) => {
    try {
      const originalFileInfo = await filesAPI.getOriginalFileInfo(fileData.id);
      logger.logInfo(`view original file: ${fileData.filename}`, 'file-processing');
      
      // 根据文件类型决定如何打开
      const fileType = originalFileInfo.file_type.toLowerCase();
      
      // 处理文件URL - 确保在不同环境下都能正确访问
      const fileUrl = originalFileInfo.file_url;
      
      if (fileType === 'pdf') {
        // PDF文件在新窗口打开
        window.open(fileUrl, '_blank');
      } else if (['jpg', 'jpeg', 'png', 'gif', 'bmp', 'image'].includes(fileType)) {
        // 图片文件在modal中显示
        setSelectedFileContent({
          ...fileData,
          isOriginalFile: true,
          originalFileInfo: {
            ...originalFileInfo,
            file_url: fileUrl  // 确保使用处理后的URL
          }
        });
        setFileContentVisible(true);
      } else {
        // 其他文件类型（Excel, Word等）提供下载
        const link = document.createElement('a');
        link.href = fileUrl;
        link.download = originalFileInfo.filename;
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        message.success(`downloading file: ${originalFileInfo.filename}`);
      }
    } catch (error) {
      console.error('Failed to get original file info:', error);
      message.error('failed to get original file info');
    }
  };

  const handleShowOriginalFiles = async (execution: any) => {
    try {
      logger.logInfo(`view task original files: ${execution.task_name}`, 'file-processing');
      
      // 获取该执行任务的文件详情
      const details = await tasksAPI.getTaskExecutionDetails(execution.id);
      
      // 过滤出有原始文件路径的文件
      const filesWithOriginal = details.files.filter(file => file.original_file_path);
      
      if (filesWithOriginal.length === 0) {
        message.info('no original files available for this task');
        return;
      }
      
      if (filesWithOriginal.length === 1) {
        // 只有一个文件，直接打开
        await handleViewOriginalFile(filesWithOriginal[0]);
      } else {
        // 多个文件，显示选择对话框
        setAvailableFiles(filesWithOriginal);
        setSelectedExecutionForFiles(execution);
        setFileSelectionVisible(true);
      }
    } catch (error) {
      console.error('Failed to get execution files:', error);
      message.error('failed to get execution files');
    }
  };

  const handleDeleteExecution = (execution: TaskExecution) => {
    setExecutionToDelete(execution);
    setDeleteExecutionVisible(true);
    logger.logInfo(`prepare to delete task execution: ${execution.task_name}`, 'file-processing');
  };

  const confirmDeleteExecution = async () => {
    if (!executionToDelete) return;
    
    try {
      setDeletingExecutions(true);
      
      // 先调用API删除
      await tasksAPI.deleteTaskExecution(executionToDelete.id);
      
      // 只有API调用成功后才更新本地状态
      setTaskExecutions(prev => prev.filter(exec => exec.id !== executionToDelete.id));
      
      message.success('Execution record deleted successfully');
      
      setDeleteExecutionVisible(false);
      setExecutionToDelete(null);
      
    } catch (error: any) {
      console.error('failed to delete execution record:', error);
      const errorMessage = getErrorMessage(error, 'Failed to delete execution record');
      message.error(errorMessage);
      logger.logError(errorMessage, 'file-processing', error);
      
      // API失败时不更新本地状态，保持原有数据
    } finally {
      setDeletingExecutions(false);
    }
  };

  const handleBatchDeleteExecutions = async () => {
    if (selectedExecutionIds.length === 0) {
      message.warning('Please select records to delete first');
      return;
    }
    
    Modal.confirm({
      title: 'Batch Delete Confirmation',
      content: `Are you sure you want to delete the selected ${selectedExecutionIds.length} execution record(s)? This action cannot be undone.`,
      okText: 'Confirm Delete',
      cancelText: 'Cancel',
      okType: 'danger',
      onOk: async () => {
        try {
          setDeletingExecutions(true);
          
          // 并行删除所有选中的记录
          await Promise.all(
            selectedExecutionIds.map(id => tasksAPI.deleteTaskExecution(id))
          );
          
          // 只有所有API调用都成功后才更新本地状态
          setTaskExecutions(prev => prev.filter(exec => !selectedExecutionIds.includes(exec.id)));
          setSelectedExecutionIds([]);
          
          message.success(`Successfully deleted ${selectedExecutionIds.length} execution record(s)`);
          
        } catch (error: any) {
          console.error('failed to batch delete execution records:', error);
          const errorMessage = getErrorMessage(error, 'Failed to batch delete execution records');
          message.error(errorMessage);
          logger.logError(errorMessage, 'file-processing', error);
          
          // API失败时不更新本地状态，保持选中状态让用户可以重试
        } finally {
          setDeletingExecutions(false);
        }
      }
    });
  };

  const refreshFileStatus = async () => {
    if (refreshingStatus) return;
    
    setRefreshingStatus(true);
    try {
      if (lastCompletedTaskId) {
        await updateFileStatuses(lastCompletedTaskId);
        message.success('file status refreshed');
      } else {
        message.info('no tasks to refresh');
        logger.logWarning('no tasks to refresh', 'file-processing');
      }
    } catch (error) {
      console.error('failed to refresh file status:', error);
      message.error('failed to refresh file status, please try again');
      logger.logError('failed to refresh file status', 'file-processing', error);
    } finally {
      setRefreshingStatus(false);
    }
  };

  // 批量文件上传状态
  const [pendingFiles, setPendingFiles] = useState<File[]>([]);
  const [uploadingFiles, setUploadingFiles] = useState(false);

  const handleUpload = (file: any) => {
    // 限制只能上传一份文件
    if (files.length > 0) {
      message.warning('只能上传一份文件，请先删除现有文件后再上传');
      return false; // 阻止上传
    }
    // 收集待上传的文件，不立即上传
    setPendingFiles(prev => [...prev, file]);
    return false; // 阻止自动上传
  };

  // 处理批量文件上传
  const processPendingFiles = useCallback(async (filesToUpload: File[]) => {
    if (filesToUpload.length === 0) return;
    
    setUploadingFiles(true);
    try {
      const uploadedFiles = await filesAPI.uploadFiles(filesToUpload);
      const newFiles = uploadedFiles.map((f, index) => ({
        id: `${Date.now()}-${index}`,
        name: f.filename,
        path: f.file_path,
        size: f.file_size,
        type: f.file_type,
        status: 'ready' as const,
        fileId: (f as any).id // 添加实际的文件ID，用于标签设置
      }));
      
      setFiles(prev => [...prev, ...newFiles]);
      
      // 如果用户已选择标签，自动为新上传的文件设置标签
      if (selectedSector && selectedIndustry) {
        try {
          // 从响应中获取真实的文件ID
          const fileIds = uploadedFiles.map((f: any) => f.id).filter(id => id);
          if (fileIds.length > 0) {
            await filesAPI.batchCreateFileTags({
              file_ids: fileIds,
              sector: selectedSector,
              industry: selectedIndustry,
              ticker: selectedTicker || undefined
            });
            message.success(`successfully uploaded ${uploadedFiles.length} files and set tags`);
            logger.logInfo(`set tags for ${fileIds.length} files: ${selectedSector} - ${selectedIndustry}${selectedTicker ? ` - ${selectedTicker}` : ''}`, 'file-processing');
          } else {
            message.success(`successfully uploaded ${uploadedFiles.length} files`);
          }
        } catch (tagError: any) {
          console.error('failed to set file tags:', tagError);
          const tagErrorMessage = getErrorMessage(tagError, 'failed to set file tags');
          message.warning(`file uploaded successfully, but tags setting failed: ${tagErrorMessage}`);
          logger.logWarning(`failed to set file tags: ${tagErrorMessage}`, 'file-processing', tagError);
        }
      } else {
        message.success(`Successfully uploaded ${uploadedFiles.length} files`);
        logger.logInfo('file uploaded successfully', 'file-processing');
      }
      
    } catch (error: any) {
      console.error('failed to upload files:', error);
      const errorMessage = getErrorMessage(error, 'failed to upload files');
      message.error(errorMessage);
      logger.logError(errorMessage, 'file-processing', error);
    } finally {
      setUploadingFiles(false);
    }
  }, [logger, setFiles, setUploadingFiles, selectedSector, selectedIndustry, selectedTicker]);

  // 当有新文件添加到待上传列表时，延迟处理以支持批量上传
  useEffect(() => {
    if (pendingFiles.length > 0) {
      const timer = setTimeout(() => {
        processPendingFiles([...pendingFiles]);
        setPendingFiles([]);
      }, 100); // 100ms 延迟，允许多个文件同时选择

      return () => clearTimeout(timer);
    }
  }, [pendingFiles, processPendingFiles]);

  const handleRemoveFile = async (fileId: string) => {
    const file = files.find(f => f.id === fileId);
    if (!file) return;

    // 如果正在处理，不允许删除
    if (processing) {
      message.warning('Cannot delete file while processing');
      return;
    }

    try {
      // 从服务器删除文件
      const filename = file.path.split('/').pop() || file.name;
      await filesAPI.deleteUploadedFile(filename);
      
      // 从本地状态中移除文件
      setFiles(prev => prev.filter(f => f.id !== fileId));
      message.success(`File ${file.name} deleted successfully`);
      logger.logInfo(`remove file: ${file.name}`, 'file-processing');
    } catch (error: any) {
      console.error('Failed to delete file:', error);
      const errorMessage = getErrorMessage(error, 'Failed to delete file');
      message.error(errorMessage);
      logger.logError(`Failed to delete file ${file.name}: ${errorMessage}`, 'file-processing', error);
    }
  };

  const handleMoveFile = (fileId: string, direction: 'up' | 'down') => {
    setFiles(prev => {
      const index = prev.findIndex(f => f.id === fileId);
      if (index === -1) return prev;
      
      const newFiles = [...prev];
      const targetIndex = direction === 'up' ? index - 1 : index + 1;
      
      if (targetIndex >= 0 && targetIndex < newFiles.length) {
        [newFiles[index], newFiles[targetIndex]] = [newFiles[targetIndex], newFiles[index]];
        logger.logInfo(`file position adjusted: ${newFiles[targetIndex].name} ${direction === 'up' ? 'up' : 'down'}`, 'file-processing');
      }
      
      return newFiles;
    });
  };

  const handleStartProcessing = async () => {
    // 根据分析模式进行不同的验证
    if (analysisMode === 'remote_agent') {
      if (!selectedTaskType) {
        message.error('please select RC Agent');
        logger.logWarning('no RC Agent selected', 'file-processing');
        return;
      }
    } else if (analysisMode === 'local_llm') {
      if (!selectedTemplateId) {
        message.error('please select Prompt Template');
        logger.logWarning('no template selected', 'file-processing');
        return;
      }
    }

    if (!selectedSector || !selectedIndustry) {
      message.error('please select industry classification');
      logger.logWarning('no industry classification selected', 'file-processing');
      return;
    }
    
    try {
      setProcessing(true);
      setLastCompletedTaskId(null); // 清空上次的任务ID
      setProcessingResult(null);   // 清空处理结果
      setShowResult(false);        // 隐藏结果显示
      hasShownCompletionMessageRef.current = false; // 重置完成消息标志
      
      // 清空当前任务状态
      setCurrentTaskId(null);
      setTaskStatus(null);
      
      logger.logInfo(`tranforming to markdown, please wait 30-60 seconds`, 'file-processing');
      
      // 将所有文件状态设置为ready
      setFiles(prev => prev.map(file => ({ ...file, status: 'ready' as const })));
      
      // 清空文件错误信息
      setFiles(prev => prev.map(file => ({ ...file, error_message: undefined })));
      
      const response = await filesAPI.processFiles({
        file_paths: files.map(f => f.path),
        task_type: analysisMode === 'local_llm' ? 'local_llm_template' : selectedTaskType,
        custom_inputs: customInputs,
        params: processingParams,
        // 新增：分析模式相关参数
        analysis_mode: analysisMode,
        template_id: analysisMode === 'local_llm' ? selectedTemplateId : undefined,
        template_code: undefined, // 可选：如果需要使用template_code
        template_variables: analysisMode === 'local_llm' ? templateVariables : {},
        llm_provider: undefined, // 可选：指定LLM提供商
        llm_model: undefined, // 可选：指定LLM模型
      });
      
      setCurrentTaskId(response.task_id);
      message.success('file processing task started');
      
      // 任务启动后立即刷新执行历史列表
      setTimeout(() => {
        loadTaskExecutionsFunc(true, false); // 手动刷新，显示错误
      }, 1000); // 延迟1秒后刷新，确保任务状态已更新
    } catch (error: any) {
      setProcessing(false);
      console.error('failed to start file processing:', error);
      const errorMessage = getErrorMessage(error, 'failed to start file processing');
      message.error(errorMessage);
      logger.logError(`failed to start file processing: ${errorMessage}`, 'file-processing', error);
    }
  };

  const handleStopProcessing = async () => {
    if (!currentTaskId || stoppingTask) return;
    
    setStoppingTask(true);
    try {
      await filesAPI.stopTask(currentTaskId);
      message.info('stop task request sent');
      logger.logInfo('stop task request sent', 'file-processing');
      
      // 停止任务后，等待一段时间再刷新执行历史列表
      setTimeout(async () => {
        try {
          const executions = await tasksAPI.getTaskExecutions();
          setTaskExecutions(executions);
        } catch (error) {
          console.error('failed to refresh execution history after task stopped:', error);
          logger.logError('failed to refresh execution history after task stopped', 'file-processing', error);
        }
      }, 3000); // 等待3秒后刷新，确保任务状态已更新
      
    } catch (error: any) {
      console.error('failed to stop task:', error);
      const errorMessage = getErrorMessage(error, 'failed to stop task');
      message.error(errorMessage);
      logger.logError(errorMessage, 'file-processing', error);
    } finally {
      setStoppingTask(false);
    }
  };

  const handleTaskTypeChange = (taskId: string) => {
    setSelectedTaskType(taskId);
    const taskType = taskTypes.find(t => t.id === taskId);
    if (taskType) {
      // 重置自定义输入
      const newCustomInputs: Record<string, string> = {};
      taskType.input_fields.forEach(field => {
        newCustomInputs[field.name] = '';
      });
      setCustomInputs(newCustomInputs);
      logger.logInfo(`switch RC Agent: ${taskType.name}`, 'file-processing');
    }
  };

  // 新增：重新处理函数
  const handleReprocess = () => {
    setShowResult(false);
    setProcessingResult(null);
    setFiles(prev => prev.map(file => ({ ...file, status: 'ready' as const })));
    logger.logInfo('reprocess files', 'file-processing');
    
          // 重新处理时也刷新执行历史列表
      setTimeout(() => {
        loadTaskExecutionsFunc(true, false); // 手动刷新，显示错误
      }, 1000); // 延迟1秒后刷新，确保状态已更新
  };

  // 新增：清除文件列表函数
  const handleClearFiles = () => {
    setFiles([]);
    setShowResult(false);
    setProcessingResult(null);
    setLastCompletedTaskId(null);
    logger.logInfo('clear file list', 'file-processing');
    
          // 清除文件列表时也刷新执行历史列表
      setTimeout(() => {
        loadTaskExecutionsFunc(true, false); // 手动刷新，显示错误
      }, 1000); // 延迟1秒后刷新，确保状态已更新
  };

  // 行业选择处理函数
  const handleSectorIndustryChange = (value: string[]) => {
    if (value && value.length >= 2) {
      setSelectedSector(value[0]);
      setSelectedIndustry(value[1]);
      
      // 如果当前选择的ticker不属于新选择的sector/industry，则清空ticker选择
      if (selectedTicker) {
        const currentTicker = tickerOptions.find(t => t.value === selectedTicker);
        if (currentTicker && (currentTicker.sector !== value[0] || currentTicker.industry !== value[1])) {
          setSelectedTicker('');
          setTickerSearchValue('');
        }
      }
    } else {
      setSelectedSector('');
      setSelectedIndustry('');
      setSelectedTicker('');
      setTickerSearchValue('');
    }
  };

  // 股票代码搜索处理函数
  const handleTickerSearch = async (searchValue: string) => {
    setTickerSearchValue(searchValue);
    
    if (!searchValue.trim()) {
      setTickerOptions([]);
      return;
    }

    try {
      setLoadingTickers(true);
      // 搜索时不受sector/industry限制，允许全局搜索
      const tickers = await filesAPI.searchTickers({
        search: searchValue
      });
      setTickerOptions(tickers);
    } catch (error) {
      console.error('failed to search tickers:', error);
      message.error('failed to search tickers');
    } finally {
      setLoadingTickers(false);
    }
  };

  // 股票代码选择处理函数
  const handleTickerSelect = (value: string, option: any) => {
    setSelectedTicker(value);
    setTickerSearchValue(option.label);
    
    // 从tickerOptions中查找对应的ticker信息
    const selectedTickerInfo = tickerOptions.find(ticker => ticker.value === value);
    console.log('Selected ticker info:', selectedTickerInfo);
    
    if (selectedTickerInfo && selectedTickerInfo.sector && selectedTickerInfo.industry) {
      console.log('Setting sector and industry:', selectedTickerInfo.sector, selectedTickerInfo.industry);
      setSelectedSector(selectedTickerInfo.sector);
      setSelectedIndustry(selectedTickerInfo.industry);
      message.success(`automatically selected industry: ${selectedTickerInfo.sector} → ${selectedTickerInfo.industry}`);
    } else {
      console.log('No sector/industry info found for ticker:', value);
    }
  };

  // 批量标签处理函数
  const handleBatchTag = async () => {
    try {
      setBatchTagging(true);
      
      // 获取所有已上传文件的ID
      const fileIds = files
        .filter(f => f.fileId)
        .map(f => f.fileId!)
        .filter(id => id);
      
      if (fileIds.length === 0) {
        message.warning('no files to set tags');
        return;
      }
      
      await filesAPI.batchCreateFileTags({
        file_ids: fileIds,
        sector: selectedSector,
        industry: selectedIndustry,
        ticker: selectedTicker || undefined
      });
      
      // 更新本地文件状态
      setFiles(prev => prev.map(f => ({
        ...f,
        tag: f.fileId ? {
          sector: selectedSector,
          industry: selectedIndustry,
          ticker: selectedTicker || undefined
        } : f.tag
      })));
      
      message.success(`successfully set tags for ${fileIds.length} files`);
      setShowBatchTagModal(false);
      
    } catch (error: any) {
      console.error('failed to batch set tags:', error);
      message.error(`failed to batch set tags: ${error.message || 'unknown error'}`);
    } finally {
      setBatchTagging(false);
    }
  };

  // 获取当前界面状态
  const getUIState = () => {
    if (processing) return 'processing';
    if (showResult && processingResult) return 'completed';
    if (files.length === 0) return 'empty';
    return 'ready';
  };

  const renderCustomInputs = () => {
    const selectedTask = taskTypes.find(t => t.id === selectedTaskType);
    if (!selectedTask || !selectedTask.input_fields.length) {
      return null;
    }

    return (
      <div style={{ marginTop: 16 }}>
        <Form layout="vertical">
          {selectedTask.input_fields.map(field => (
            <Form.Item
              key={field.name}
              label={field.name}
            >
              <Input.TextArea
                value={customInputs[field.name] || ''}
                onChange={(e) => setCustomInputs(prev => ({
                  ...prev,
                  [field.name]: e.target.value
                }))}
                placeholder={`enter ${field.name}`}
                maxLength={field.max_length}
                showCount
                rows={2}
                disabled={processing}
              />
            </Form.Item>
          ))}
        </Form>
      </div>
    );
  };

  const renderProcessingParams = () => (
    <Form form={form} layout="vertical">
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item label="Language Settings">
            <Select
              mode="multiple"
              value={processingParams.language}
              onChange={(value) => setProcessingParams(prev => ({
                ...prev,
                language: value
              }))}
              placeholder="Select languages"
            >
              <Option value="ch_sim">Simplified Chinese</Option>
              <Option value="en">English</Option>
            </Select>
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item label="Max Pages">
            <InputNumber
              value={processingParams.max_pages}
              onChange={(value) => setProcessingParams(prev => ({
                ...prev,
                max_pages: value || 10
              }))}
              min={1}
              max={100}
              style={{ width: '100%' }}
            />
          </Form.Item>
        </Col>
      </Row>
      
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item label="Polling Interval (seconds)">
            <InputNumber
              value={processingParams.polling_interval}
              onChange={(value) => setProcessingParams(prev => ({
                ...prev,
                polling_interval: value || 10
              }))}
              min={5}
              max={60}
              style={{ width: '100%' }}
            />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item label="Target Pages">
            <Input
              value={processingParams.target_pages}
              onChange={(e) => setProcessingParams(prev => ({
                ...prev,
                target_pages: e.target.value
              }))}
              placeholder="e.g., 1-5, 7, 10"
            />
          </Form.Item>
        </Col>
      </Row>
      
      <Row gutter={16}>
        <Col span={24}>
          <Form.Item label="RC Agent Server URL">
            <Input
              value={processingParams.ai_agent_server_url}
              onChange={(e) => setProcessingParams(prev => ({
                ...prev,
                ai_agent_server_url: e.target.value
              }))}
              placeholder="Enter RC Agent server URL"
            />
          </Form.Item>
        </Col>
      </Row>
      
      <Divider />
      
      <Row gutter={16}>
        <Col span={8}>
          <Form.Item label="Fast Mode">
            <Switch
              checked={processingParams.fast_mode}
              onChange={(checked) => setProcessingParams(prev => ({
                ...prev,
                fast_mode: checked
              }))}
            />
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item label="Disable OCR">
            <Switch
              checked={processingParams.disable_ocr}
              onChange={(checked) => setProcessingParams(prev => ({
                ...prev,
                disable_ocr: checked
              }))}
            />
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item label="Use RC Agent">
            <Switch
                          checked={processingParams.use_ai_agent}
            onChange={(checked) => setProcessingParams(prev => ({
              ...prev,
              use_ai_agent: checked
            }))}
            />
          </Form.Item>
        </Col>
      </Row>
      
      <Row gutter={16}>
        <Col span={8}>
          <Form.Item label="Skip Diagonal Text">
            <Switch
              checked={processingParams.skip_diagonal_text}
              onChange={(checked) => setProcessingParams(prev => ({
                ...prev,
                skip_diagonal_text: checked
              }))}
            />
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item label="Skip Table Extraction">
            <Switch
              checked={processingParams.skip_table_extraction}
              onChange={(checked) => setProcessingParams(prev => ({
                ...prev,
                skip_table_extraction: checked
              }))}
            />
          </Form.Item>
        </Col>
        <Col span={8}>
          <Form.Item label="Extract Images">
            <Switch
              checked={processingParams.extract_images}
              onChange={(checked) => setProcessingParams(prev => ({
                ...prev,
                extract_images: checked
              }))}
            />
          </Form.Item>
        </Col>
      </Row>
      
      <Row gutter={16}>
        <Col span={12}>
          <Form.Item label="Use Multimodal Model">
            <Switch
              checked={processingParams.use_vendor_multimodal_model}
              onChange={(checked) => setProcessingParams(prev => ({
                ...prev,
                use_vendor_multimodal_model: checked
              }))}
            />
          </Form.Item>
        </Col>
        <Col span={12}>
          <Form.Item label="Multimodal API Key">
            <Input
              value={processingParams.vendor_multimodal_api_key}
              onChange={(e) => setProcessingParams(prev => ({
                ...prev,
                vendor_multimodal_api_key: e.target.value
              }))}
              placeholder="Enter API key"
              disabled={!processingParams.use_vendor_multimodal_model}
            />
          </Form.Item>
        </Col>
      </Row>
      <Row gutter={16}>
        <Col span={24}>
          <Form.Item label="LlamaIndex API Key">
            <Input.Password
              value={processingParams.llamaindex_api_key}
              onChange={(e) => setProcessingParams(prev => ({
                ...prev,
                llamaindex_api_key: e.target.value
              }))}
              placeholder="Enter LlamaIndex API key"
            />
          </Form.Item>
        </Col>
      </Row>
    </Form>
  );

  const getStatusText = (status: string) => {
    switch (status) {
      case 'ready':
      case 'pending':
        return 'Pending';
      case 'starting':
        return 'Starting';
      case 'initializing':
        return 'Initializing';
      case 'processing':
        return 'Processing';
      case 'ai_processing':
        return 'AI Processing';
      case 'saving':
        return 'Saving';
      case 'completed':
        return 'Completed';
      case 'failed':
        return 'Failed';
      case 'stopped':
        return 'Stopped';
      default:
        return 'Pending';
    }
  };



  const fileColumns = [
    {
      title: 'File Name',
      dataIndex: 'name',
      key: 'name',
      render: (text: string, record: FileItem) => (
        <Space>
          {getFileTypeIcon(record.type)}
          {text}
        </Space>
      )
    },
    {
      title: 'Type',
      dataIndex: 'type',
      key: 'type',
      render: (type: string) => {
        switch (type) {
          case 'pdf':
            return <Tag color="red">PDF</Tag>;
          case 'excel':
            return <Tag color="green">Excel</Tag>;
          case 'doc':
            return <Tag color="blue">Word</Tag>;
          case 'image':
            return <Tag color="orange">Image</Tag>;
          default:
            return <Tag color="default">{type.toUpperCase()}</Tag>;
        }
      }
    },
    {
      title: 'Size',
      dataIndex: 'size',
      key: 'size',
      render: (size: number) => filesAPI.formatFileSize(size)
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 80,
      render: (_: any, record: FileItem) => (
        <Tooltip title="Delete File">
          <Button
            type="text"
            danger
            icon={<DeleteOutlined />}
            onClick={() => handleRemoveFile(record.id)}
            disabled={processing}
            size="small"
          />
        </Tooltip>
      )
    }
  ];

  const executionColumns = [
    {
      title: 'File Name',
      key: 'file_name',
      width: 350,
      ellipsis: true,
      render: (_: any, record: TaskExecution) => {
        // 如果有first_filename则显示，否则显示文件数量
        if (record.first_filename) {
          return (
            <Tooltip title={record.first_filename} placement="topLeft">
              <div style={{ 
                color: '#1890ff', 
                cursor: 'pointer',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
                maxWidth: '400px',
                fontSize: '13px'
              }}>
                <FileTextOutlined style={{ marginRight: 6, fontSize: '14px' }} />
                {record.first_filename}
              </div>
            </Tooltip>
          );
        } else {
          // 如果没有文件名，显示文件数量作为占位
          const total = record.files_processed + record.files_failed;
          return (
            <div style={{ color: '#999', fontSize: '11px' }}>
              <FolderOutlined style={{ marginRight: 6, fontSize: '11px' }} />
              {total} file{total > 1 ? 's' : ''}
            </div>
          );
        }
      }
    },
    {
      title: 'Agent Name',
      dataIndex: 'task_name',
      key: 'task_name',
      width: 180,
      render: (task_name: string, record: TaskExecution) => {
        const displayName = getDisplayAgentName(task_name);
        return (
          <div>
            <Tooltip title={displayName} placement="topLeft">
              <div style={{ 
                fontWeight: 500,
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
                fontSize: '11px'
              }}>
                {displayName}
              </div>
            </Tooltip>
            <div style={{ fontSize: '11px', color: '#999' }}>
              {new Date(record.created_at).toLocaleString()}
            </div>
          </div>
        );
      }
    },
    {
      title: 'Files',
      key: 'files_info',
      width: 120,
      render: (_: any, record: TaskExecution) => {
        const status = record.rc_agent_status;
        const isAnalyzed = status === 'success';
        
        return (
          <Space direction="vertical" size={0}>
            <div style={{ fontSize: '11px' }}>
              <span style={{ color: '#999' }}>Processed: </span>
              <span style={{ color: '#52c41a', fontWeight: 500 }}>
                {record.files_processed}
              </span>
            </div>
            <div style={{ fontSize: '11px' }}>
              <span style={{ color: '#999' }}>Analyzed: </span>
              <span style={{ fontWeight: 500 }}>
                {isAnalyzed ? 'True' : 'False'}
              </span>
            </div>
          </Space>
        );
      }
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 200,
      render: (_: any, record: TaskExecution) => (
        <Space size="small">
          <Tooltip title="AI Analysis">
            <Button
              type="text"
              icon={<RobotOutlined />}
              onClick={() => handleViewAiAnalysis(record)}
              size="small"
            />
          </Tooltip>
          <Tooltip title="View Details">
            <Button
              type="text"
              icon={<EyeOutlined />}
              onClick={() => handleViewExecution(record)}
              size="small"
            />
          </Tooltip>
          <Tooltip title="Original Files">
            <Button
              type="text"
              icon={<FileTextOutlined />}
              onClick={() => handleShowOriginalFiles(record)}
              size="small"
            />
          </Tooltip>
          <Tooltip title="Delete">
            <Button
              type="text"
              danger
              icon={<DeleteOutlined />}
              onClick={() => handleDeleteExecution(record)}
              size="small"
              loading={deletingExecutions}
            />
          </Tooltip>
        </Space>
      )
    }
  ];

  const renderExecutionDetail = () => (
    <Modal
      title="Execution Details"
      open={executionDetailVisible}
      onCancel={() => {
        setExecutionDetailVisible(false);
        setExecutionDetailData(null);
      }}
      footer={null}
      width={1200}
      centered
    >
      {executionDetailLoading ? (
        <div style={{ textAlign: 'center', padding: '40px 0' }}>
          <Progress type="circle" percent={50} showInfo={false} />
          <div style={{ marginTop: 16 }}>Loading Execution Details...</div>
        </div>
      ) : selectedExecution && (
        <div>
          {/* 基础信息 */}
          <Card size="small" title="Basic Information" style={{ marginBottom: 16 }}>
            <Row gutter={16}>
              <Col span={12}>
                <div><strong>Agent Name:</strong> {getDisplayAgentName(selectedExecution.task_name)}</div>
              </Col>
              <Col span={12}>
                <div><strong>User:</strong> {selectedExecution.username}</div>
              </Col>
            </Row>
            <Row gutter={16} style={{ marginTop: 16 }}>
              <Col span={24}>
                <div><strong>Time:</strong> {new Date(selectedExecution.created_at).toLocaleString()}</div>
              </Col>
            </Row>
            <Row gutter={16} style={{ marginTop: 16 }}>
              <Col span={8}>
                <div><strong>Number of Files:</strong> {selectedExecution.files_processed}</div>
              </Col>
              <Col span={8}>
                <div><strong>Number of Failed Files:</strong> {selectedExecution.files_failed}</div>
              </Col>
              <Col span={8}>
                <div><strong>Success Rate:</strong> {selectedExecution.files_processed > 0 ? 
                  `${((selectedExecution.files_processed / (selectedExecution.files_processed + selectedExecution.files_failed)) * 100).toFixed(1)}%` : 
                  '0%'
                }</div>
              </Col>
            </Row>
            <Row gutter={16} style={{ marginTop: 16 }}>
              <Col span={8}>
                <div><strong>Processing Time:</strong> {selectedExecution.total_time.toFixed(2)}秒</div>
              </Col>
              <Col span={8}>
                <div><strong>File Processing Time:</strong> {selectedExecution.file_processing_time.toFixed(2)}秒</div>
              </Col>
              <Col span={8}>
                <div><strong>RC Agent Processing Time:</strong> {selectedExecution.ai_processing_time.toFixed(2)}秒</div>
              </Col>
            </Row>
            <Row gutter={16} style={{ marginTop: 16 }}>
              <Col span={12}>
                <div><strong>Token Consumption:</strong> {selectedExecution.total_tokens ? selectedExecution.total_tokens.toLocaleString() : '-'}</div>
              </Col>
              <Col span={12}>
                <div><strong>Approximate Cost:</strong> {selectedExecution.total_cost ? `¥${selectedExecution.total_cost.toFixed(4)}` : '-'}</div>
              </Col>
            </Row>
            <Row gutter={16} style={{ marginTop: 16 }}>
              <Col span={12}>
                <div><strong>RC Agent Status:</strong> 
                  {selectedExecution.rc_agent_status ? (
                    (() => {
                      const statusConfig = {
                        success: { color: 'success', text: 'Success' },
                        failed: { color: 'error', text: 'Failed' },
                        pending: { color: 'processing', text: 'Pending' },
                        skipped: { color: 'default', text: 'Skipped' }
                      };
                      const config = statusConfig[selectedExecution.rc_agent_status as keyof typeof statusConfig] || { color: 'default', text: selectedExecution.rc_agent_status };
                      return <Tag color={config.color} style={{ marginLeft: 8 }}>{config.text}</Tag>;
                    })()
                  ) : (
                    <Tag color="default" style={{ marginLeft: 8 }}>Unknown</Tag>
                  )}
                </div>
              </Col>
            </Row>
            {(selectedExecution.llm_model || selectedExecution.llm_provider) && (
              <Row gutter={16} style={{ marginTop: 16 }}>
                <Col span={24}>
                  <div><strong>Analysis Model:</strong> 
                    {selectedExecution.llm_provider && (
                      <Tag color="blue" style={{ marginLeft: 8 }}>
                        {selectedExecution.llm_provider}
                      </Tag>
                    )}
                    {selectedExecution.llm_model && (
                      <Tag color="cyan" style={{ marginLeft: 4 }}>
                        {selectedExecution.llm_model}
                      </Tag>
                    )}
                  </div>
                </Col>
              </Row>
            )}
          </Card>

          {/* 文件处理详情 */}
          {executionDetailData?.files && executionDetailData.files.length > 0 ? (
            <Card size="small" title="File Processing Details">
              <Table
                dataSource={executionDetailData.files}
                rowKey="id"
                size="small"
                scroll={{ y: 400 }}
                pagination={false}
                columns={[
                  {
                    title: 'File Name',
                    dataIndex: 'filename',
                    key: 'filename',
                    width: 300,
                    ellipsis: false,
                    render: (filename, record) => (
                      <div style={{ wordBreak: 'break-all', whiteSpace: 'normal' }}>
                        {record.content ? (
                          <Button 
                            type="link" 
                            style={{ 
                              padding: 0, 
                              height: 'auto', 
                              textAlign: 'left',
                              whiteSpace: 'normal',
                              wordBreak: 'break-all'
                            }}
                            onClick={() => handleViewFileContent(record)}
                          >
                            {filename}
                          </Button>
                        ) : (
                          <span>{filename}</span>
                        )}
                      </div>
                    )
                  },
                  {
                    title: 'Type',
                    dataIndex: 'file_type',
                    key: 'file_type',
                    width: 80,
                    render: (file_type) => {
                      const typeMapping = {
                        'pdf': { color: 'red', text: 'PDF' },
                        'excel': { color: 'green', text: 'Excel' },
                        'doc': { color: 'blue', text: 'Word' },
                        'image': { color: 'orange', text: 'Image' }
                      };
                      const config = typeMapping[file_type as keyof typeof typeMapping] || { color: 'default', text: file_type?.toUpperCase() || 'Unknown' };
                      return <Tag color={config.color}>{config.text}</Tag>;
                    }
                  },
                  {
                    title: 'Size (MB)',
                    dataIndex: 'file_size_mb',
                    key: 'file_size_mb',
                    width: 100,
                    render: (size) => size?.toFixed(2)
                  },
                  {
                    title: 'Processing Time (s)',
                    dataIndex: 'processing_time',
                    key: 'processing_time',
                    width: 120,
                    render: (time) => time?.toFixed(2) || '-'
                  },
                  {
                    title: 'AI Time (s)',
                    dataIndex: 'ai_time',
                    key: 'ai_time',
                    width: 100,
                    render: (time) => time?.toFixed(2) || '-'
                  },
                  {
                    title: 'Tokens',
                    dataIndex: 'total_tokens',
                    key: 'total_tokens',
                    width: 100,
                    render: (tokens) => tokens ? tokens.toLocaleString() : '-'
                  },
                  {
                    title: 'Cost (¥)',
                    dataIndex: 'cost',
                    key: 'cost',
                    width: 100,
                    render: (cost) => cost ? cost.toFixed(4) : '-'
                  },
                  {
                    title: 'Actions',
                    key: 'actions',
                    width: 120,
                    render: (_, record) => (
                      <Space>
                        {record.content && (
                          <Button
                            type="text"
                            size="small"
                            icon={<RobotOutlined />}
                            onClick={() => handleViewFileContent(record)}
                            title="View AI Content"
                          >
                          </Button>
                        )}
                        {record.original_file_path && (
                          <Button
                            type="text"
                            size="small"
                            icon={<FileTextOutlined />}
                            onClick={() => handleViewOriginalFile(record)}
                            title="View Original File"
                          >
                          </Button>
                        )}
                      </Space>
                    )
                  }
                ]}
              />
            </Card>
          ) : (
            <Card size="small" title="File Processing Details">
              <div style={{ textAlign: 'center', padding: '40px 0' }}>
                <ExclamationCircleOutlined style={{ fontSize: '48px', color: '#faad14' }} />
                <div style={{ marginTop: 16, fontSize: '16px' }}>
                  No file processing details available for this execution.
                </div>
              </div>
            </Card>
          )}
        </div>
      )}
    </Modal>
  );

  const renderAiAnalysisModal = () => {
    console.log('Rendering AI Analysis Modal');
    console.log('aiAnalysisData:', aiAnalysisData);
    console.log('final_report exists:', !!aiAnalysisData?.execution?.final_report);
    console.log('final_report length:', aiAnalysisData?.execution?.final_report?.length);
    
    return (
      <Modal
        title="AI Analysis Report"
        open={aiAnalysisVisible}
        onCancel={() => {
          setAiAnalysisVisible(false);
          setAiAnalysisData(null);
        }}
        footer={[
          <Button key="cancel" onClick={() => {
            setAiAnalysisVisible(false);
            setAiAnalysisData(null);
          }}>
            Close
          </Button>,
          <Button 
            key="download" 
            type="primary" 
            icon={<DownloadOutlined />}
            loading={pdfDownloading}
            onClick={handleDownloadPdf}
            disabled={!aiAnalysisData?.execution?.final_report || pdfDownloading}
          >
            {pdfDownloading ? 'Generating...' : 'Download PDF'}
          </Button>
        ]}
        width={1000}
        centered
      >
      {aiAnalysisLoading ? (
        <div style={{ textAlign: 'center', padding: '40px 0' }}>
          <Progress type="circle" percent={50} showInfo={false} />
          <div style={{ marginTop: 16 }}>Loading AI Analysis...</div>
        </div>
      ) : aiAnalysisData?.execution?.final_report ? (
        <div>
          <div style={{ marginBottom: 16 }}>
            <Text strong>Report for: </Text>
            <Tag color="blue">{getDisplayAgentName(aiAnalysisData.execution.task_name)}</Tag>
            <Text style={{ marginLeft: 16 }}>
              <Text strong>Generated: </Text>
              {new Date(aiAnalysisData.execution.created_at).toLocaleString()}
            </Text>
          </div>
          <Divider />
          <div 
            id="ai-analysis-content"
            style={{ 
              background: '#ffffff', 
              padding: '20px', 
              borderRadius: '8px', 
              border: '1px solid #e1e4e8',
              maxHeight: '60vh',
              overflow: 'auto'
            }}
          >
            <MarkdownRenderer 
              content={aiAnalysisData.execution.final_report}
              style={{
                fontSize: '14px',
                lineHeight: '1.6'
              }}
            />
          </div>
        </div>
      ) : (
        <div style={{ textAlign: 'center', padding: '40px 0' }}>
          <ExclamationCircleOutlined style={{ fontSize: '48px', color: '#faad14' }} />
          <div style={{ marginTop: 16, fontSize: '16px' }}>
            No AI analysis report available for this execution.
          </div>
        </div>
      )}
    </Modal>
    );
  };


  const renderFileContentModal = () => (
    <Modal
      title={
        <Space>
          {selectedFileContent?.isOriginalFile ? <FolderOutlined /> : <FileTextOutlined />}
          <span>
            {selectedFileContent?.isOriginalFile ? 'Original File' : 'AI Processed Content'} - {selectedFileContent?.filename || 'Unknown'}
          </span>
        </Space>
      }
      open={fileContentVisible}
      onCancel={() => {
        setFileContentVisible(false);
        setSelectedFileContent(null);
      }}
      footer={[
        <Button key="close" onClick={() => {
          setFileContentVisible(false);
          setSelectedFileContent(null);
        }}>
          Close
        </Button>
      ]}
      width={1000}
      centered
      style={{ top: 20 }}
    >
      {selectedFileContent ? (
        <div>
          <div style={{ marginBottom: 16, padding: '12px', backgroundColor: '#f5f5f5', borderRadius: '6px' }}>
            <Row gutter={16}>
              <Col span={8}>
                <Text strong>File Type:</Text> {(() => {
                  const typeMapping = {
                    'pdf': { color: 'red', text: 'PDF' },
                    'excel': { color: 'green', text: 'Excel' },
                    'doc': { color: 'blue', text: 'Word' },
                    'image': { color: 'orange', text: 'Image' }
                  };
                  const config = typeMapping[selectedFileContent.file_type as keyof typeof typeMapping] || { color: 'default', text: selectedFileContent.file_type?.toUpperCase() || 'Unknown' };
                  return <Tag color={config.color}>{config.text}</Tag>;
                })()}
              </Col>
              <Col span={8}>
                <Text strong>Size:</Text> {selectedFileContent.file_size_mb?.toFixed(2)} MB
              </Col>
              <Col span={8}>
                <Text strong>Status:</Text> 
                <Tag color={selectedFileContent.status === 'completed' ? 'success' : 'error'}>
                  {selectedFileContent.status}
                </Tag>
              </Col>
            </Row>
          </div>
          <Divider style={{ margin: '16px 0' }} />
          <div style={{ 
            maxHeight: '500px', 
            overflow: 'auto',
            border: '1px solid #d9d9d9',
            borderRadius: '6px',
            padding: '16px',
            backgroundColor: '#fafafa'
          }}>
            {selectedFileContent.isOriginalFile && selectedFileContent.originalFileInfo ? (
              // 原始文件显示
              (() => {
                const fileType = selectedFileContent.originalFileInfo.file_type.toLowerCase();
                if (['jpg', 'jpeg', 'png', 'gif', 'bmp', 'image'].includes(fileType)) {
                  // 显示图片
                  return (
                    <div style={{ textAlign: 'center' }}>
                      <img 
                        src={selectedFileContent.originalFileInfo.file_url} 
                        alt={selectedFileContent.filename}
                        style={{ 
                          maxWidth: '100%', 
                          maxHeight: '400px',
                          objectFit: 'contain'
                        }}
                        onError={(e) => {
                          const target = e.target as HTMLImageElement;
                          target.style.display = 'none';
                          target.parentElement!.innerHTML = `
                            <div style="text-align: center; padding: 40px 0;">
                              <div style="font-size: 48px; color: #faad14;">⚠️</div>
                              <div style="margin-top: 16px; font-size: 16px; color: #666;">
                                Failed to load image
                              </div>
                            </div>
                          `;
                        }}
                      />
                    </div>
                  );
                } else {
                  // 其他文件类型提示下载
                  return (
                    <div style={{ textAlign: 'center', padding: '40px 0' }}>
                      <FileTextOutlined style={{ fontSize: '48px', color: '#1890ff' }} />
                      <div style={{ marginTop: 16, fontSize: '16px', color: '#666' }}>
                        This file type cannot be previewed online.
                      </div>
                      <div style={{ marginTop: 8 }}>
                        <Button 
                          type="primary" 
                          icon={<DownloadOutlined />}
                          onClick={() => {
                            const link = document.createElement('a');
                            link.href = selectedFileContent.originalFileInfo.file_url;
                            link.download = selectedFileContent.originalFileInfo.filename;
                            document.body.appendChild(link);
                            link.click();
                            document.body.removeChild(link);
                          }}
                        >
                          Download File
                        </Button>
                      </div>
                    </div>
                  );
                }
              })()
            ) : selectedFileContent.content ? (
              // AI处理后的内容
              <pre style={{ 
                whiteSpace: 'pre-wrap', 
                wordWrap: 'break-word',
                margin: 0,
                fontFamily: 'Monaco, Menlo, "Ubuntu Mono", Consolas, source-code-pro, monospace',
                fontSize: '13px',
                lineHeight: '1.5',
                color: '#333'
              }}>
                {selectedFileContent.content}
              </pre>
            ) : (
              <div style={{ textAlign: 'center', padding: '40px 0' }}>
                <ExclamationCircleOutlined style={{ fontSize: '48px', color: '#faad14' }} />
                <div style={{ marginTop: 16, fontSize: '16px', color: '#666' }}>
                  No content available for this file
                </div>
              </div>
            )}
          </div>
        </div>
      ) : (
        <div style={{ textAlign: 'center', padding: '40px 0' }}>
          <ExclamationCircleOutlined style={{ fontSize: '48px', color: '#faad14' }} />
          <div style={{ marginTop: 16, fontSize: '16px' }}>
            No file data available
          </div>
        </div>
      )}
    </Modal>
  );

  return (
    <div>
      <div className="page-content" style={{ marginTop: 16 }}>
        {/* 主体区域：左侧参数配置，右侧执行历史 */}
        <Row gutter={24}>
          {/* 左侧：输入参数区域 - 1/3宽度 */}
          <Col span={8}>
            {/* 文件上传和处理按钮 */}
            {getUIState() !== 'completed' && (
              <div style={{ marginBottom: 16 }}>
                <Space style={{ width: '100%' }} size="middle">
                  <Upload
                    beforeUpload={handleUpload}
                    showUploadList={false}
                    disabled={processing || uploadingFiles}
                    style={{ flex: 1 }}
                  >
                    <Button
                      type="primary"
                      icon={<InboxOutlined />}
                      loading={uploadingFiles}
                      disabled={processing || uploadingFiles}
                      size="large"
                      style={{ width: '100%' }}
                    >
                      {uploadingFiles ? 'Uploading Files...' : 'Upload Files'}
                    </Button>
                  </Upload>
                  
                  {getUIState() === 'ready' && files.length > 0 && (
                    <Button
                      type="primary"
                      icon={<PlayCircleOutlined />}
                      onClick={handleStartProcessing}
                      loading={processing}
                      disabled={
                        files.length === 0 || 
                        !selectedSector || 
                        !selectedIndustry ||
                        (analysisMode === 'remote_agent' && !selectedTaskType) ||
                        (analysisMode === 'local_llm' && !selectedTemplateId)
                      }
                      size="large"
                      style={{ 
                        width: '160px',
                        backgroundColor: '#52c41a',
                        borderColor: '#52c41a'
                      }}
                    >
                      Start Processing
                    </Button>
                  )}
                </Space>
                {!uploadingFiles && (
                  <div style={{ 
                    marginTop: 8, 
                    fontSize: '12px', 
                    color: '#999', 
                    textAlign: 'left' 
                  }}>
                    Support PDF, Excel, Word, Image and more
                  </div>
                )}
              </div>
            )}

            {/* 行业和股票选择 */}
            <Card title="Industry & Ticker Selection" className="mb-12" size="small">
              <Form layout="vertical" size="small">
                <Form.Item 
                  label="Sector & Industry" 
                  required
                  tooltip="Select sector and industry for analysis context"
                >
                  <Cascader
                    options={sectors}
                    value={selectedSector && selectedIndustry ? [selectedSector, selectedIndustry] : []}
                    onChange={handleSectorIndustryChange}
                    placeholder="Select sector and industry"
                    loading={loadingSectors}
                    disabled={processing}
                    showSearch={{
                      filter: (inputValue, path) => {
                        return path.some(option => 
                          option.label.toLowerCase().includes(inputValue.toLowerCase())
                        );
                      }
                    }}
                    style={{ width: '100%' }}
                  />
                </Form.Item>

                <Form.Item 
                  label="Ticker" 
                  tooltip="Select or search for a specific ticker (optional)"
                >
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
                    disabled={processing}
                    style={{ width: '100%' }}
                    filterOption={false}
                    notFoundContent={loadingTickers ? 'Loading...' : 'No tickers found'}
                  />
                </Form.Item>

                {(selectedSector && selectedIndustry) && (
                  <div style={{ 
                    padding: '8px 12px', 
                    backgroundColor: '#f6ffed', 
                    border: '1px solid #b7eb8f',
                    borderRadius: '6px',
                    fontSize: '12px',
                    color: '#52c41a'
                  }}>
                    <strong>Selected:</strong> {selectedSector} → {selectedIndustry}
                    {selectedTicker && <><br /><strong>Ticker:</strong> {selectedTicker}</>}
                    {!selectedTicker && <><br /><em>No ticker selected (optional)</em></>}
                  </div>
                )}
              </Form>
            </Card>

            {/* 文件列表 */}
            {files.length > 0 && (
              <Card
                title={
                  <Space>
                    <FileTextOutlined />
                    File List ({files.length})
                    {getUIState() === 'completed' && (
                      <Badge 
                        count={files.filter(f => f.status === 'completed').length} 
                        style={{ backgroundColor: '#52c41a' }} 
                      />
                    )}
                  </Space>
                }
                extra={
                  <Space>
                    {getUIState() === 'completed' && (
                      <Button
                        icon={<ReloadOutlined />}
                        onClick={refreshFileStatus}
                        loading={refreshingStatus}
                        size="small"
                      >
                        Refresh Status
                      </Button>
                    )}
                  </Space>
                }
                className="mb-16"
                size="small"
              >
                <Table
                  dataSource={files}
                  columns={fileColumns}
                  rowKey="id"
                  pagination={false}
                  size="small"
                />
              </Card>
            )}

            {/* 处理进度 */}
            {processing && taskStatus && (
              <Card title="Processing Progress" className="mb-12" size="small">
                <div style={{ marginBottom: 16 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 8 }}>
                    <span>Current Status: {getStatusText(taskStatus.status)}</span>
                    <span>{taskStatus.progress.toFixed(1)}%</span>
                  </div>
                  <Progress
                    percent={taskStatus.progress}
                    status={taskStatus.status === 'failed' ? 'exception' : 'active'}
                    showInfo={false}
                  />
                </div>
                
                {taskStatus.current_file && (
                  <div style={{ marginBottom: 16 }}>
                    <Text>Current File: {taskStatus.current_file}</Text>
                  </div>
                )}
                
                <div>
                  <Space>
                    <span>Processed: {taskStatus.files_processed}</span>
                    <span>Failed: {taskStatus.files_failed}</span>
                    <span>Total: {taskStatus.total_files}</span>
                  </Space>
                </div>
              </Card>
            )}

            {/* 分析模式选择 */}
            <Card title="Analysis Mode" className="mb-12" size="small">
              <Form layout="vertical" size="small">
                <Form.Item label="Analysis Mode" style={{ marginBottom: 0 }}>
                  <Select
                    value={analysisMode}
                    onChange={(value) => {
                      setAnalysisMode(value);
                      // 切换模式时清除相关选择
                      if (value === 'local_llm') {
                        setSelectedTaskType('');
                      } else {
                        setSelectedTemplateId(undefined);
                        setTemplateVariables({});
                      }
                    }}
                    disabled={processing}
                    style={{ width: '100%' }}
                  >
                    <Option value="local_llm">
                      <Space>
                        <FileTextOutlined />
                        <Text>Local AI Agent</Text>
                      </Space>
                    </Option>
                    <Option value="remote_agent">
                      <Space>
                        <RobotOutlined />
                        <Text>Remote AI Agent (Dify)</Text>
                      </Space>
                    </Option>
                  </Select>
                </Form.Item>
              </Form>
            </Card>

            {/* 根据模式显示不同的选择器 */}
            {analysisMode === 'remote_agent' ? (
              // 远程Agent模式 - 显示AI Agent选择
              <Card title="AI Agent Selection" className="mb-12" size="small">
              <Form layout="vertical" size="small">
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
                  {taskTypes.map((task, index) => {
                    const colors = ['blue', 'green', 'orange', 'red', 'purple', 'cyan', 'magenta', 'volcano', 'geekblue', 'gold'];
                    const color = colors[index % colors.length];
                    const isSelected = selectedTaskType === task.id;
                    
                    return (
                      <Tag
                        key={task.id}
                        color={isSelected ? color : undefined}
                        style={{
                          cursor: processing ? 'not-allowed' : 'pointer',
                          border: isSelected ? `2px solid ${color === 'blue' ? '#1890ff' : color === 'green' ? '#52c41a' : color === 'orange' ? '#fa8c16' : color === 'red' ? '#f5222d' : color === 'purple' ? '#722ed1' : color === 'cyan' ? '#13c2c2' : color === 'magenta' ? '#eb2f96' : color === 'volcano' ? '#fa541c' : color === 'geekblue' ? '#2f54eb' : '#fadb14'}` : '1px solid #d9d9d9',
                          backgroundColor: isSelected ? undefined : '#fafafa',
                          fontWeight: isSelected ? 'bold' : 'normal',
                          opacity: processing ? 0.6 : 1
                        }}
                        onClick={() => !processing && handleTaskTypeChange(task.id)}
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
                      <div style={{ color: '#999' }}>
                        No AI Agents available
                      </div>
                    )}
                  </div>
                )}
              </Form>
              
              {renderCustomInputs()}
            </Card>
            ) : (
              // 本地LLM模式 - 显示模板选择和变量编辑
              <Card title="Local LLM Agent" className="mb-12" size="small">
                <Form layout="vertical" size="small">
                  <Form.Item label="Select Prompt Template" required>
                    <TemplateSelector
                      categoryCode="file_analysis"
                      value={selectedTemplateId}
                      onChange={(templateId, template) => {
                        setSelectedTemplateId(templateId);
                        // 清空之前的变量
                        setTemplateVariables({});
                      }}
                      disabled={processing}
                    />
                  </Form.Item>
                  
                  {selectedTemplateId && (
                    <Form.Item label="Template Parameters Configuration">
                      <TemplateVariablesEditor
                        templateId={selectedTemplateId}
                        values={templateVariables}
                        onChange={(values) => setTemplateVariables(values)}
                        disabled={processing}
                      />
                    </Form.Item>
                  )}
                </Form>
              </Card>
            )}

            {/* 操作按钮 - 根据状态显示不同的操作 */}
            <Card title="Operation Control" className="mb-12" size="small">
              <Space style={{ width: '100%' }} direction="vertical">
                {getUIState() === 'ready' && (
                  <Button
                    icon={<SettingOutlined />}
                    onClick={() => setShowParams(true)}
                    disabled={processing}
                    block
                  >
                    Parameter Settings
                  </Button>
                )}

                {getUIState() === 'processing' && (
                  <>
                    <Button
                      icon={<StopOutlined />}
                      onClick={handleStopProcessing}
                      block
                      danger
                      loading={stoppingTask}
                      disabled={stoppingTask}
                    >
                      Stop Processing
                    </Button>
                    
                    <Button
                      icon={<EyeOutlined />}
                      onClick={() => logger.actions.setShowFloating(true)}
                      block
                    >
                      View Real-time Logs
                    </Button>
                  </>
                )}

                {getUIState() === 'completed' && processingResult && (
                  <>
                    <Button
                      type="primary"
                      icon={<ReloadOutlined />}
                      onClick={handleReprocess}
                      block
                    >
                      Reprocess
                    </Button>
                    
                    <Button
                      icon={<EyeOutlined />}
                      onClick={() => logger.actions.setShowFloating(true)}
                      block
                      disabled={!logger.logs || logger.logs.length === 0}
                    >
                      View Processing Logs
                    </Button>
                    
                    <Button
                      icon={<ClearOutlined />}
                      onClick={handleClearFiles}
                      block
                    >
                      Clear File List
                    </Button>
                  </>
                )}

                {getUIState() === 'empty' && (
                  <>
                    <Button
                      icon={<SettingOutlined />}
                      onClick={() => setShowParams(true)}
                      block
                    >
                      Parameter Settings
                    </Button>
                  </>
                )}
              </Space>
            </Card>
          </Col>

          {/* 右侧：执行历史区域 - 2/3宽度 */}
          <Col span={16}>
            <Card 
              title="Execution History" 
              size="small"
              extra={
                selectedExecutionIds.length > 0 && (
                  <Space>
                    <span>Selected {selectedExecutionIds.length} item(s)</span>
                    <Button
                      type="primary"
                      danger
                      icon={<DeleteOutlined />}
                      onClick={handleBatchDeleteExecutions}
                      loading={deletingExecutions}
                      size="small"
                    >
                      Batch Delete
                    </Button>
                  </Space>
                )
              }
            >
              <Table
                columns={executionColumns}
                dataSource={taskExecutions}
                rowKey="id"
                size="small"
                rowSelection={{
                  selectedRowKeys: selectedExecutionIds,
                  onChange: (selectedRowKeys: React.Key[]) => {
                    setSelectedExecutionIds(selectedRowKeys as string[]);
                  },
                  onSelectAll: (selected, selectedRows, changeRows) => {
                    if (selected) {
                      const newSelectedIds = [...selectedExecutionIds, ...changeRows.map(row => row.id)];
                      setSelectedExecutionIds(Array.from(new Set(newSelectedIds)));
                    } else {
                      const changeIds = changeRows.map(row => row.id);
                      setSelectedExecutionIds(selectedExecutionIds.filter(id => !changeIds.includes(id)));
                    }
                  }
                }}
                pagination={{
                  pageSize: 15,
                  showSizeChanger: true,
                  showQuickJumper: true,
                  showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} items`,
                  pageSizeOptions: ['10', '15', '20', '30', '50']
                }}
                scroll={{ x: 'max-content' }}
              />
            </Card>
          </Col>
        </Row>

        {/* 参数设置对话框 */}
        <Modal
          title="Processing Parameter Settings"
          open={showParams}
          onOk={() => setShowParams(false)}
          onCancel={() => setShowParams(false)}
          width={800}
          centered
        >
          {renderProcessingParams()}
        </Modal>

        {/* 执行详情对话框 */}
        {renderExecutionDetail()}

        {/* AI分析报告对话框 */}
        {renderAiAnalysisModal()}

        {/* 删除确认对话框 */}
        <Modal
          title="Delete Confirmation"
          open={deleteExecutionVisible}
          onOk={confirmDeleteExecution}
          onCancel={() => {
            setDeleteExecutionVisible(false);
            setExecutionToDelete(null);
          }}
          okText="Confirm Delete"
          cancelText="Cancel"
          okType="danger"
          confirmLoading={deletingExecutions}
          centered
        >
          {executionToDelete && (
            <div>
              <p>Are you sure you want to delete the following execution record? This action cannot be undone.</p>
              <div style={{ background: '#f5f5f5', padding: '12px', borderRadius: '4px', margin: '12px 0' }}>
                <p><strong>Task Name:</strong> {getDisplayAgentName(executionToDelete.task_name)}</p>
                <p><strong>User:</strong> {executionToDelete.username}</p>
                <p><strong>Execution Time:</strong> {new Date(executionToDelete.created_at).toLocaleString()}</p>
                <p><strong>Files Processed:</strong> {executionToDelete.files_processed} successful, {executionToDelete.files_failed} failed</p>
              </div>
            </div>
          )}
        </Modal>

        {renderFileContentModal()}

        {/* 文件选择对话框 */}
        <Modal
          title={
            <Space>
              <FileTextOutlined />
              <span>Select Original File to View</span>
              {selectedExecutionForFiles && (
                <Tag color="blue">{getDisplayAgentName(selectedExecutionForFiles.task_name)}</Tag>
              )}
            </Space>
          }
          open={fileSelectionVisible}
          onCancel={() => {
            setFileSelectionVisible(false);
            setAvailableFiles([]);
            setSelectedExecutionForFiles(null);
          }}
          footer={[
            <Button key="close" onClick={() => {
              setFileSelectionVisible(false);
              setAvailableFiles([]);
              setSelectedExecutionForFiles(null);
            }}>
              Close
            </Button>
          ]}
          width={800}
          centered
        >
          <div style={{ marginBottom: 16 }}>
            <Text type="secondary">
              This task contains {availableFiles.length} file(s) with original content. Select a file to view or download:
            </Text>
          </div>
          
          <Table
            dataSource={availableFiles}
            rowKey="id"
            size="small"
            pagination={false}
            scroll={{ y: 300 }}
            columns={[
              {
                title: 'File Name',
                dataIndex: 'filename',
                key: 'filename',
                ellipsis: true,
                render: (filename) => (
                  <Text strong>{filename}</Text>
                )
              },
              {
                title: 'Type',
                dataIndex: 'file_type',
                key: 'file_type',
                width: 80,
                render: (file_type) => {
                  const typeMapping = {
                    'pdf': { color: 'red', text: 'PDF' },
                    'excel': { color: 'green', text: 'Excel' },
                    'doc': { color: 'blue', text: 'Word' },
                    'image': { color: 'orange', text: 'Image' }
                  };
                  const config = typeMapping[file_type as keyof typeof typeMapping] || { color: 'default', text: file_type?.toUpperCase() || 'Unknown' };
                  return <Tag color={config.color}>{config.text}</Tag>;
                }
              },
              {
                title: 'Size (MB)',
                dataIndex: 'file_size_mb',
                key: 'file_size_mb',
                width: 100,
                render: (size) => size?.toFixed(2)
              },
              {
                title: 'Action',
                key: 'action',
                width: 120,
                render: (_, record) => (
                  <Button
                    type="primary"
                    size="small"
                    icon={<EyeOutlined />}
                    onClick={() => {
                      setFileSelectionVisible(false);
                      handleViewOriginalFile(record);
                    }}
                  >
                    View
                  </Button>
                )
              }
            ]}
          />
        </Modal>

        {/* 批量标签设置模态框 */}
        <Modal
          title="Batch Tag Files"
          open={showBatchTagModal}
          onOk={handleBatchTag}
          onCancel={() => setShowBatchTagModal(false)}
          confirmLoading={batchTagging}
          centered
        >
          <div>
            <p>将为以下文件设置标签：</p>
            <div style={{ marginBottom: 16, padding: '8px 12px', backgroundColor: '#f6ffed', border: '1px solid #b7eb8f', borderRadius: '6px' }}>
              <Space direction="vertical" size="small">
                <div>
                  <Text strong>Sector:</Text> <Tag color="blue">{selectedSector}</Tag>
                </div>
                <div>
                  <Text strong>Industry:</Text> <Tag color="green">{selectedIndustry}</Tag>
                </div>
                {selectedTicker && (
                  <div>
                    <Text strong>Ticker:</Text> <Tag color="orange">{selectedTicker}</Tag>
                  </div>
                )}
              </Space>
            </div>
            <div style={{ maxHeight: '200px', overflow: 'auto', border: '1px solid #d9d9d9', borderRadius: '4px', padding: '8px' }}>
              {files.filter(f => f.fileId).map(file => (
                <div key={file.id} style={{ padding: '4px 0', display: 'flex', alignItems: 'center' }}>
                  {getFileTypeIcon(file.type)}
                  <Text style={{ marginLeft: 8 }}>{file.name}</Text>
                </div>
              ))}
            </div>
            {files.filter(f => f.fileId).length === 0 && (
              <div style={{ textAlign: 'center', color: '#999', padding: '20px' }}>
                没有可以设置标签的文件
              </div>
            )}
          </div>
        </Modal>
      </div>
    </div>
  );
};

export default FilesPage; 