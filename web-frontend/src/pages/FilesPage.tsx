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
  AutoComplete,
  ConfigProvider,
  Dropdown,
  Checkbox,
  theme as antdTheme
} from 'antd';
import type { MenuProps } from 'antd';
import {
  InboxOutlined,
  PlayCircleOutlined,
  DeleteOutlined,
  StopOutlined,
  SettingOutlined,
  EyeOutlined,
  ReloadOutlined,
  FileTextOutlined,
  FilePdfOutlined,
  FileExcelOutlined,
  FileWordOutlined,
  FileImageOutlined,
  ExclamationCircleOutlined,
  ClearOutlined,
  DownloadOutlined,
  RobotOutlined,
  FolderOutlined,
  LoadingOutlined,
  EllipsisOutlined,
  EditOutlined,
  SoundOutlined
} from '@ant-design/icons';
import { filesAPI, SectorOption, TickerOption } from '../services/files';
import { tasksAPI, TaskExecutionDetail } from '../services/tasks';
import { getErrorMessage } from '../utils/errorHandler';
import { useGlobalLogger } from '../contexts/LoggerContext';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import MarkdownRenderer from '../components/MarkdownRenderer';
import { downloadElementAsPDF } from '../utils/pdfDownload';
import TemplateSelector from '../components/TemplateSelector';
import TemplateVariablesEditor from '../components/TemplateVariablesEditor';
import promptTemplateService, { PromptTemplate } from '../services/promptTemplateService';

const { Text } = Typography;
const { Option } = Select;

const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    textMuted: '#999',
    boxMuted: '#f5f5f5',
    successBg: '#f6ffed',
    successBorder: '#b7eb8f',
  },
  dark: {
    pageBg: '#1a202c',
    cardBg: '#1a2234',
    cardBorder: '#364152',
    text: '#e0e6f1',
    textSecondary: 'rgba(255, 255, 255, 0.5)',
    textMuted: '#94a3b8',
    boxMuted: '#212936',
    successBg: 'rgba(82, 196, 26, 0.15)',
    successBorder: 'rgba(82, 196, 26, 0.45)',
  },
} as const;

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
  first_file_type?: string;  // 第一个文件的类型（用于列表筛选）
  llm_model?: string;  // LLM模型名称
  llm_provider?: string;  // LLM提供商
  first_task_id?: string;  // 第一个文件的task_id（用于判断是否正在处理）
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
  files_failed?: number;
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
  files_failed?: number;
  total_files: number;
  processing_time: number;
  final_report?: string;
}

const FilesPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const { addTask, updateTaskStatus: updateGlobalTaskStatus } = useFileProcessing();
  const isDark = themeMode === 'dark';
  const themeColors = THEME_COLORS[themeMode];
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
  const [batchMode, setBatchMode] = useState(false);
  const [deleteExecutionVisible, setDeleteExecutionVisible] = useState(false);
  const [executionToDelete, setExecutionToDelete] = useState<TaskExecution | null>(null);
  const [deletingExecutions, setDeletingExecutions] = useState(false);
  const [renameModalVisible, setRenameModalVisible] = useState(false);
  const [executionToRename, setExecutionToRename] = useState<TaskExecution | null>(null);
  const [renameInputValue, setRenameInputValue] = useState('');
  const [renameLoading, setRenameLoading] = useState(false);
  
  // AI Analysis 相关状态
  const [aiAnalysisVisible, setAiAnalysisVisible] = useState(false);
  const [aiAnalysisData, setAiAnalysisData] = useState<TaskExecutionDetail | null>(null);
  const [aiAnalysisLoading, setAiAnalysisLoading] = useState(false);
  const [pdfDownloading, setPdfDownloading] = useState(false);
  
  // AI分析相关状态（音频+文档）
  const [audioAnalyzingIds, setAudioAnalyzingIds] = useState<Set<string>>(new Set());
  const [docAnalyzingIds, setDocAnalyzingIds] = useState<Set<string>>(new Set());
  const [aiAnalysisExecution, setAiAnalysisExecution] = useState<TaskExecution | null>(null);
  const [modalSelectedTemplateId, setModalSelectedTemplateId] = useState<number | undefined>(undefined);
  
  // 新布局相关状态
  const [selectedExecutionForView, setSelectedExecutionForView] = useState<TaskExecution | null>(null);
  const [viewMode, setViewMode] = useState<'original' | 'analysis'>('original');
  const [selectedFileForView, setSelectedFileForView] = useState<any>(null);
  const [originalFileInfoForTabs, setOriginalFileInfoForTabs] = useState<{ filename?: string; file_type?: string } | null>(null);
  const [fileTypeFilter, setFileTypeFilter] = useState<'all' | 'pdf' | 'doc' | 'excel' | 'image' | 'audio'>('all');
  const [searchType, setSearchType] = useState<'file_name' | 'content'>('file_name');
  const [searchQuery, setSearchQuery] = useState('');
  
  
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

  // 切换 execution 时清空 Original 标签栏的文件信息（会由 OriginalContentView 重新加载）
  useEffect(() => {
    setOriginalFileInfoForTabs(null);
  }, [selectedExecutionForView?.id]);

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

  // 分页与无限滚动
  const EXECUTIONS_PAGE_SIZE = 20;
  const [executionsPage, setExecutionsPage] = useState(1);
  const [hasMoreExecutions, setHasMoreExecutions] = useState(true);
  const [loadingMoreExecutions, setLoadingMoreExecutions] = useState(false);
  const filesListScrollRef = useRef<HTMLDivElement>(null);

  // 加载任务执行历史的函数
  const loadTaskExecutionsFunc = useCallback(async (showError = true, isAutoRefresh = false, append = false) => {
    if (append && (!hasMoreExecutions || loadingMoreExecutions)) return;
    
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
      const pageToLoad = append ? executionsPage + 1 : 1;
      if (append) setLoadingMoreExecutions(true);
      
      const executions = await tasksAPI.getTaskExecutions({
        page: pageToLoad,
        page_size: EXECUTIONS_PAGE_SIZE,
        ...(searchType === 'content' && searchQuery.trim() ? { search_type: 'content', q: searchQuery.trim() } : {}),
      });
      
      if (append) {
        setTaskExecutions(prev => [...prev, ...executions]);
        setExecutionsPage(pageToLoad);
      } else {
        setTaskExecutions(executions);
        setExecutionsPage(1);
      }
      setHasMoreExecutions(executions.length === EXECUTIONS_PAGE_SIZE);
      
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
    } finally {
      if (append) setLoadingMoreExecutions(false);
    }
  }, [logger, taskHistoryLoadAttempts, lastTaskHistoryError, maxTaskHistoryRetries, taskHistoryRetryInterval, executionsPage, hasMoreExecutions, loadingMoreExecutions, searchType, searchQuery]);

  // 无限滚动：接近底部时加载更多
  // 使用 ref 来避免 loadTaskExecutionsFunc 变化导致的重复绑定
  const hasMoreExecutionsRef = useRef(hasMoreExecutions);
  const loadingMoreExecutionsRef = useRef(loadingMoreExecutions);
  const loadTaskExecutionsFuncForScrollRef = useRef(loadTaskExecutionsFunc);
  
  useEffect(() => {
    hasMoreExecutionsRef.current = hasMoreExecutions;
    loadingMoreExecutionsRef.current = loadingMoreExecutions;
    loadTaskExecutionsFuncForScrollRef.current = loadTaskExecutionsFunc;
  }, [hasMoreExecutions, loadingMoreExecutions, loadTaskExecutionsFunc]);

  const handleFilesListScroll = useCallback(() => {
    const el = filesListScrollRef.current;
    if (!el || !hasMoreExecutionsRef.current || loadingMoreExecutionsRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = el;
    const threshold = 80;
    if (scrollTop + clientHeight >= scrollHeight - threshold) {
      loadTaskExecutionsFuncForScrollRef.current(false, false, true);
    }
  }, []); // 空依赖数组，函数只创建一次

  useEffect(() => {
    const el = filesListScrollRef.current;
    if (!el) return;
    el.addEventListener('scroll', handleFilesListScroll);
    return () => el.removeEventListener('scroll', handleFilesListScroll);
  }, [handleFilesListScroll]);

  // content 搜索：切换为 content 或搜索词变化时重新拉取列表（搜索词防抖）
  // 使用 ref 避免 loadTaskExecutionsFunc 变化导致的重复执行
  const searchTypeRef = useRef(searchType);
  const searchQueryRef = useRef(searchQuery);
  
  useEffect(() => {
    searchTypeRef.current = searchType;
    searchQueryRef.current = searchQuery;
  }, [searchType, searchQuery]);

  useEffect(() => {
    if (searchType !== 'content') return;
    const delay = searchQuery.trim() ? 400 : 0;
    const t = setTimeout(() => {
      loadTaskExecutionsFuncRef.current(false, false, false);
    }, delay);
    return () => clearTimeout(t);
  }, [searchType, searchQuery]); // 只依赖搜索相关的状态，不依赖函数

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

  // 初始化加载：仅在组件挂载时执行一次
  useEffect(() => {
    loadTaskTypes();
    loadTaskExecutionsFunc();
    loadDefaultConfig();
    loadAllTemplates();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // 空依赖数组，只在组件挂载时执行一次

  // 定时刷新执行历史列表：使用 ref 避免依赖项导致的重复执行
  const loadTaskExecutionsFuncRef = useRef(loadTaskExecutionsFunc);
  useEffect(() => {
    loadTaskExecutionsFuncRef.current = loadTaskExecutionsFunc;
  }, [loadTaskExecutionsFunc]);

  useEffect(() => {
    const refreshInterval = setInterval(() => {
      // 使用 ref 中的最新函数，避免依赖项变化
      loadTaskExecutionsFuncRef.current(false, true).catch(() => {
        // 静默处理错误，避免在自动刷新时显示错误消息
      });
    }, 30000); // 每30秒自动刷新一次执行历史列表
    
    return () => {
      clearInterval(refreshInterval);
    };
  }, []); // 空依赖数组，定时器只创建一次

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
        
        // 任务完成后刷新执行历史列表（重置为第一页）
        // 使用 ref 避免依赖项问题
        loadTaskExecutionsFuncRef.current(false, true, false).catch(() => {});
        
        // 延迟一段时间后再次刷新，确保数据同步
        setTimeout(() => {
          loadTaskExecutionsFuncRef.current(false, true, false).catch(() => {});
        }, 5000); // 等待5秒后再次刷新，确保数据完全同步
      }
    } catch (error) {
      console.error('failed to get task status:', error);
      logger.logError(`failed to get task status: ${error}`, 'file-processing');
    } finally {
      setUpdatingStatus(false);
    }
  }, [currentTaskId, updateFileStatuses, logger, updatingStatus, resetAllInputs]); // 移除 loadTaskExecutionsFunc 依赖

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
      setAiAnalysisExecution(execution);
      
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


  // 音频AI分析 - 触发分析
  const handleAnalyzeAudio = async (record: TaskExecution) => {
    const executionId = Number(record.id);
    const fileName = record.first_filename || record.task_name || 'Audio file';
    
    try {
      setAudioAnalyzingIds(prev => new Set(prev).add(record.id));
      message.loading({ content: 'Submitting audio analysis task...', key: 'audioAnalyze', duration: 0 });

      const result = await filesAPI.analyzeAudio({ execution_id: executionId });

      if (result.success && result.task_id) {
        message.loading({ content: 'AI analyzing audio...', key: 'audioAnalyze', duration: 0 });
        logger.logInfo(`Audio analysis task submitted: ${result.task_id}`, 'audio-analysis');

        // 添加任务到全局进度显示
        addTask(result.task_id, fileName, 'audio');

        // 开始轮询任务状态
        const pollInterval = setInterval(async () => {
          try {
            const statusResult = await filesAPI.getAudioAnalysisStatus(result.task_id);
            
            // 更新全局任务状态
            updateGlobalTaskStatus(result.task_id, {
              status: statusResult.status,
              progress: statusResult.progress || 0,
              current_file: fileName,
              files_processed: 0,
              files_failed: 0,
              total_files: 1,
              logs: []
            });
            
            if (statusResult.status === 'completed') {
              clearInterval(pollInterval);
              message.destroy('audioAnalyze');
              message.success('Audio AI analysis completed!');
              setAudioAnalyzingIds(prev => {
                const next = new Set(prev);
                next.delete(record.id);
                return next;
              });
              // 刷新执行列表
              loadTaskExecutionsFunc(false);
              // 如果当前正在查看该记录的AI Analysis，刷新报告数据
              try {
                const details = await tasksAPI.getTaskExecutionDetails(record.id);
                setAiAnalysisData(details);
              } catch (_) { /* ignore */ }
            } else if (statusResult.status === 'failed') {
              clearInterval(pollInterval);
              message.destroy('audioAnalyze');
              message.error(`Audio analysis failed: ${statusResult.message}`);
              setAudioAnalyzingIds(prev => {
                const next = new Set(prev);
                next.delete(record.id);
                return next;
              });
              loadTaskExecutionsFunc(false);
            }
          } catch (err) {
            console.error('Failed to poll audio analysis status:', err);
          }
        }, 8000);

        // 安全超时：5分钟后停止轮询
        setTimeout(() => {
          clearInterval(pollInterval);
          setAudioAnalyzingIds(prev => {
            const next = new Set(prev);
            next.delete(record.id);
            return next;
          });
        }, 300000);
      }
    } catch (error: any) {
      message.destroy('audioAnalyze');
      const errorMsg = getErrorMessage(error, 'Failed to start audio analysis');
      message.error(errorMsg);
      setAudioAnalyzingIds(prev => {
        const next = new Set(prev);
        next.delete(record.id);
        return next;
      });
    }
  };

  // 文档AI分析 - 触发分析（需要指定模板ID）
  const handleAnalyzeDocument = async (record: TaskExecution, templateId: number) => {
    const executionId = Number(record.id);
    const fileName = record.first_filename || record.task_name || 'Document';
    
    try {
      setDocAnalyzingIds(prev => new Set(prev).add(record.id));
      message.loading({ content: 'Submitting document analysis task...', key: 'docAnalyze', duration: 0 });

      const result = await filesAPI.analyzeDocument({
        execution_id: executionId,
        template_id: templateId,
      });

      if (result.success && result.task_id) {
        message.loading({ content: 'AI analyzing document...', key: 'docAnalyze', duration: 0 });
        logger.logInfo(`Document analysis task submitted: ${result.task_id}`, 'doc-analysis');

        // 添加任务到全局进度显示
        addTask(result.task_id, fileName, 'document');

        const pollInterval = setInterval(async () => {
          try {
            const statusResult = await filesAPI.getDocumentAnalysisStatus(result.task_id);
            
            // 更新全局任务状态
            updateGlobalTaskStatus(result.task_id, {
              status: statusResult.status,
              progress: statusResult.progress || 0,
              current_file: fileName,
              files_processed: 0,
              files_failed: 0,
              total_files: 1,
              logs: []
            });
            
            if (statusResult.status === 'completed') {
              clearInterval(pollInterval);
              message.destroy('docAnalyze');
              message.success('Document AI analysis completed!');
              setDocAnalyzingIds(prev => {
                const next = new Set(prev);
                next.delete(record.id);
                return next;
              });
              loadTaskExecutionsFunc(false);
              // 如果当前正在查看该记录的AI Analysis，刷新报告数据
              try {
                const details = await tasksAPI.getTaskExecutionDetails(record.id);
                setAiAnalysisData(details);
              } catch (_) { /* ignore */ }
            } else if (statusResult.status === 'failed') {
              clearInterval(pollInterval);
              message.destroy('docAnalyze');
              message.error(`Document analysis failed: ${statusResult.message}`);
              setDocAnalyzingIds(prev => {
                const next = new Set(prev);
                next.delete(record.id);
                return next;
              });
              loadTaskExecutionsFunc(false);
            }
          } catch (err) {
            console.error('Failed to poll document analysis status:', err);
          }
        }, 8000);

        // 安全超时：10分钟后停止轮询（文档可能需要LlamaParse+AI分析，时间更长）
        setTimeout(() => {
          clearInterval(pollInterval);
          setDocAnalyzingIds(prev => {
            const next = new Set(prev);
            next.delete(record.id);
            return next;
          });
        }, 600000);
      }
    } catch (error: any) {
      message.destroy('docAnalyze');
      const errorMsg = getErrorMessage(error, 'Failed to start document analysis');
      message.error(errorMsg);
      setDocAnalyzingIds(prev => {
        const next = new Set(prev);
        next.delete(record.id);
        return next;
      });
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
    } catch (error: any) {
      console.error('Failed to get original file info:', error);
      
      // 提供更友好的错误信息
      if (error.response?.status === 404) {
        message.error(`File not found (ID: ${fileData.id}). The file may have been deleted or the database record is missing.`);
        logger.logError(`File not found in database: ${fileData.filename} (ID: ${fileData.id})`, 'file-processing', error);
      } else {
        const errorMsg = getErrorMessage(error, 'Failed to load original file');
        message.error(errorMsg);
        logger.logError(`Failed to load original file: ${fileData.filename}`, 'file-processing', error);
      }
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

  const handleOpenRename = (execution: TaskExecution) => {
    setExecutionToRename(execution);
    setRenameInputValue(execution.first_filename || `${execution.files_processed} file(s)`);
    setRenameModalVisible(true);
  };

  const handleConfirmRename = async () => {
    if (!executionToRename || !renameInputValue.trim()) return;
    try {
      setRenameLoading(true);
      await tasksAPI.renameExecutionFile(executionToRename.id, renameInputValue.trim());
      setTaskExecutions(prev =>
        prev.map(exec =>
          exec.id === executionToRename.id
            ? { ...exec, first_filename: renameInputValue.trim() }
            : exec
        )
      );
      if (selectedExecutionForView?.id === executionToRename.id) {
        setSelectedExecutionForView(prev =>
          prev ? { ...prev, first_filename: renameInputValue.trim() } : null
        );
      }
      message.success('Renamed successfully');
      setRenameModalVisible(false);
      setExecutionToRename(null);
      setRenameInputValue('');
    } catch (error: any) {
      const errMsg = error?.response?.data?.detail || getErrorMessage(error, 'Rename failed');
      message.error(errMsg);
    } finally {
      setRenameLoading(false);
    }
  };

  const confirmDeleteExecution = async () => {
    if (!executionToDelete) return;
    
    try {
      setDeletingExecutions(true);
      
      // 先调用API删除
      await tasksAPI.deleteTaskExecution(executionToDelete.id);
      
      // 只有API调用成功后才更新本地状态
      setTaskExecutions(prev => prev.filter(exec => exec.id !== executionToDelete.id));
      setSelectedExecutionIds(prev => prev.filter(id => id !== executionToDelete.id));
      if (selectedExecutionForView?.id === executionToDelete.id) {
        setSelectedExecutionForView(null);
      }
      
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
      title: 'Batch Delete Warning',
      content: (
        <div>
          <p>
            <strong>Are you sure you want to permanently delete the selected {selectedExecutionIds.length} file(s)?</strong>
          </p>
          <p style={{ color: '#ff4d4f', marginTop: 8, marginBottom: 0 }}>
            Warning: The deleted files and all AI-analyzed content will be permanently removed and cannot be recovered.
          </p>
        </div>
      ),
      okText: 'Confirm Delete',
      cancelText: 'Cancel',
      okType: 'danger',
      onOk: async () => {
        try {
          setDeletingExecutions(true);

          // 限制并发：每批最多 10 个请求，避免 200+ 并发导致后端连接池耗尽或 500 错误
          const BATCH_SIZE = 10;
          const ids = [...selectedExecutionIds];
          const succeeded: string[] = [];
          const failed: { id: string; error: string }[] = [];

          for (let i = 0; i < ids.length; i += BATCH_SIZE) {
            const batch = ids.slice(i, i + BATCH_SIZE);
            const results = await Promise.allSettled(
              batch.map(id => tasksAPI.deleteTaskExecution(id))
            );
            results.forEach((result, idx) => {
              const id = batch[idx];
              if (result.status === 'fulfilled') {
                succeeded.push(id);
              } else {
                failed.push({ id, error: (result.reason as Error)?.message || String(result.reason) });
              }
            });
          }

          const idsToDelete = succeeded;
          setTaskExecutions(prev => prev.filter(exec => !idsToDelete.includes(exec.id)));
          setSelectedExecutionIds([]);
          if (selectedExecutionForView && idsToDelete.includes(selectedExecutionForView.id)) {
            setSelectedExecutionForView(null);
          }

          if (failed.length > 0) {
            message.warning(`Deleted ${succeeded.length} of ${ids.length} records. ${failed.length} failed.`);
            logger.logError(`Batch delete partial failure: ${failed.length}/${ids.length} failed`, 'file-processing', failed);
          } else {
            message.success(`Successfully deleted ${idsToDelete.length} execution record(s)`);
          }
          
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

  // 单文件上传状态
  const [uploadingFiles, setUploadingFiles] = useState(false);

  const handleUpload = async (file: any) => {
    // 限制只能上传一份文件
    if (files.length > 0) {
      message.warning('只能上传一份文件，请先删除现有文件后再上传');
      return false; // 阻止上传
    }
    
    setUploadingFiles(true);
    try {
      // 只上传单个文件
      const uploadedFiles = await filesAPI.uploadFiles([file]);
      const uploadedFile = uploadedFiles[0];
      
      const newFile = {
        id: `${Date.now()}`,
        name: uploadedFile.filename,
        path: uploadedFile.file_path,
        size: uploadedFile.file_size,
        type: uploadedFile.file_type,
        status: 'ready' as const,
        fileId: (uploadedFile as any).id // 添加实际的文件ID，用于标签设置
      };
      
      setFiles([newFile]); // 直接设置为单个文件
      
      // 如果用户已选择标签，自动为新上传的文件设置标签
      if (selectedSector && selectedIndustry && newFile.fileId) {
        try {
          await filesAPI.batchCreateFileTags({
            file_ids: [newFile.fileId],
            sector: selectedSector,
            industry: selectedIndustry,
            ticker: selectedTicker || undefined
          });
          message.success('files uploaded successfully and tags set');
          logger.logInfo(`set tags for files: ${selectedSector} - ${selectedIndustry}${selectedTicker ? ` - ${selectedTicker}` : ''}`, 'file-processing');
        } catch (tagError: any) {
          console.error('failed to set file tags:', tagError);
          const tagErrorMessage = getErrorMessage(tagError, 'failed to set file tags');
          message.warning(`files uploaded successfully, but tags setting failed: ${tagErrorMessage}`);
          logger.logWarning(`failed to set file tags: ${tagErrorMessage}`, 'file-processing', tagError);
        }
      } else {
        message.success('files uploaded successfully');
        logger.logInfo('file uploaded successfully', 'file-processing');
      }
      
    } catch (error: any) {
      console.error('failed to upload file:', error);
      const errorMessage = getErrorMessage(error, 'failed to upload file');
      message.error(errorMessage);
      logger.logError(errorMessage, 'file-processing', error);
    } finally {
      setUploadingFiles(false);
    }
    
    return false; // 阻止自动上传
  };

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
      message.success(`file ${file.name} deleted successfully`);
      logger.logInfo(`remove file: ${file.name}`, 'file-processing');
    } catch (error: any) {
      console.error('Failed to delete file:', error);
      const errorMessage = getErrorMessage(error, 'Failed to delete file');
      message.error(errorMessage);
      logger.logError(`Failed to delete file ${file.name}: ${errorMessage}`, 'file-processing', error);
    }
  };

  const _handleMoveFile = (fileId: string, direction: 'up' | 'down') => {
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
        message.error('please select reverse capital agent');
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
      // 添加任务到全局进度显示
      if (files.length > 0) {
        addTask(response.task_id, files[0].name, 'document');
      }
      message.success('file processing task started');
      
      // 任务启动后立即刷新执行历史列表
      setTimeout(() => {
        loadTaskExecutionsFuncRef.current(true, false); // 手动刷新，显示错误
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
      
      // 停止任务后，等待一段时间再刷新执行历史列表（重置为第一页）
      setTimeout(() => {
        loadTaskExecutionsFuncRef.current(false, true, false).catch(() => {});
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
        loadTaskExecutionsFuncRef.current(true, false); // 手动刷新，显示错误
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
        loadTaskExecutionsFuncRef.current(true, false); // 手动刷新，显示错误
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
            <div style={{ color: themeColors.textMuted, fontSize: '11px' }}>
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
            <div style={{ fontSize: '11px', color: themeColors.textMuted }}>
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
        const agentStatus = record.rc_agent_status;
        const isAnalyzed = agentStatus === 'success';
        const isUploadOnly = agentStatus === 'uploaded';
        const isAnalyzing = agentStatus === 'analyzing' || audioAnalyzingIds.has(record.id) || docAnalyzingIds.has(record.id);
        
        return (
          <Space direction="vertical" size={0}>
            <div style={{ fontSize: '11px' }}>
              <span style={{ color: themeColors.textMuted }}>
                {isUploadOnly ? 'Uploaded: ' : 'Processed: '}
              </span>
              <span style={{ color: isUploadOnly ? '#1890ff' : '#52c41a', fontWeight: 500 }}>
                {record.files_processed}
              </span>
            </div>
            {(!isUploadOnly || isAnalyzing) && (
              <div style={{ fontSize: '11px' }}>
                <span style={{ color: themeColors.textMuted }}>Analyzed: </span>
                <span style={{ fontWeight: 500, color: isAnalyzing ? '#faad14' : isAnalyzed ? '#52c41a' : undefined }}>
                  {isAnalyzing ? 'Analyzing...' : isAnalyzed ? 'True' : 'False'}
                </span>
              </div>
            )}
          </Space>
        );
      }
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 220,
      render: (_: any, record: TaskExecution) => {
        const isUploadOnly = record.rc_agent_status === 'uploaded';
        const isAudioFile = record.first_file_type === 'audio';
        const isAudioAnalyzing = record.rc_agent_status === 'analyzing' || audioAnalyzingIds.has(record.id);
        const isDocAnalyzing = record.rc_agent_status === 'analyzing' || docAnalyzingIds.has(record.id);
        const isAnalyzing = isAudioAnalyzing || isDocAnalyzing;
        const hasAnalysis = record.rc_agent_status === 'success';
        // 音频文件且ASR已完成但未进行AI分析
        const canAnalyzeAudio = isAudioFile && !hasAnalysis && !isAudioAnalyzing && record.rc_agent_status === 'completed';
        // 文档分析：需要区分"仅上传"和"正在处理"
        // 1. 如果first_task_id为null且rc_agent_status为uploaded，说明仅上传未处理，允许分析
        // 2. 如果rc_agent_status为failed，允许重新分析
        // 3. 如果有task_id或状态为processing/initializing等，说明正在处理，禁止重复分析
        const isProcessingDoc = record.rc_agent_status === 'processing' || 
                                record.rc_agent_status === 'initializing' ||
                                (record.first_task_id != null && record.rc_agent_status === 'uploaded');
        const canAnalyzeDoc = !isAudioFile && !hasAnalysis && !isDocAnalyzing && !isProcessingDoc && (
          record.rc_agent_status === 'uploaded' ||
          record.rc_agent_status === 'failed'
        );
        
        return (
        <Space size="small">
          {/* 音频文件专属：Analyze 按钮 */}
          {isAudioFile && (canAnalyzeAudio || isAudioAnalyzing) && (
            <Tooltip title={isAudioAnalyzing ? "AI Analyzing..." : "AI Analyze Audio"}>
              <Button
                type="text"
                icon={isAudioAnalyzing ? <LoadingOutlined /> : <SoundOutlined />}
                onClick={() => !isAudioAnalyzing && handleAnalyzeAudio(record)}
                size="small"
                loading={isAudioAnalyzing}
                style={{ color: isAudioAnalyzing ? '#faad14' : '#1890ff' }}
              />
            </Tooltip>
          )}
          <Tooltip title={isUploadOnly ? "AI Analysis (click to analyze)" : "AI Analysis"}>
            <Button
              type="text"
              icon={isDocAnalyzing ? <LoadingOutlined /> : <RobotOutlined />}
              onClick={() => handleViewAiAnalysis(record)}
              size="small"
              disabled={isAudioFile && !hasAnalysis && !isAudioAnalyzing}
              style={{ 
                opacity: (isAudioFile && !hasAnalysis && !isAudioAnalyzing) ? 0.3 : 1,
                color: isDocAnalyzing ? '#faad14' : undefined
              }}
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
        );
      }
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
                <div><strong>Status:</strong> 
                  {selectedExecution.rc_agent_status ? (
                    (() => {
                      const statusConfig = {
                        success: { color: 'success', text: 'AI Analysis Complete' },
                        failed: { color: 'error', text: 'Failed' },
                        pending: { color: 'processing', text: 'Pending' },
                        skipped: { color: 'default', text: 'Skipped' },
                        uploaded: { color: 'blue', text: 'Upload Only' },
                        analyzing: { color: 'warning', text: 'AI Analyzing...' },
                        completed: { color: 'cyan', text: 'ASR Complete' }
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
            className={isDark ? 'ai-analysis-content ai-analysis-content--dark' : 'ai-analysis-content'}
            style={{ 
              background: isDark ? '#151b26' : '#ffffff', 
              padding: '20px', 
              borderRadius: '12px', 
              border: isDark ? '1px solid #364152' : '1px solid #e1e4e8',
              maxHeight: '60vh',
              overflow: 'auto',
              boxShadow: isDark ? '0 2px 12px rgba(0,0,0,0.2)' : '0 1px 3px rgba(0,0,0,0.04)'
            }}
          >
            <MarkdownRenderer 
              content={aiAnalysisData.execution.final_report}
              variant={isDark ? 'dark' : 'light'}
              style={{
                fontSize: '14px',
                lineHeight: '1.6',
                color: isDark ? themeColors.text : undefined
              }}
            />
          </div>
        </div>
      ) : (() => {
        const isAudioModal = aiAnalysisExecution?.first_file_type === 'audio';
        
        // 音频分析：只有转写完成(completed)或失败(failed)时才允许AI分析
        const canAnalyzeAudioModal = isAudioModal && 
          (aiAnalysisExecution?.rc_agent_status === 'completed' || aiAnalysisExecution?.rc_agent_status === 'failed');
        
        // 文档分析：需要区分"仅上传"和"正在处理"
        // 1. 如果first_task_id为null且rc_agent_status为uploaded，说明仅上传未处理，允许分析
        // 2. 如果rc_agent_status为failed，允许重新分析
        // 3. 如果有task_id或状态为processing/initializing等，说明正在处理，禁止重复分析
        const isProcessingDocModal = aiAnalysisExecution?.rc_agent_status === 'processing' || 
                                      aiAnalysisExecution?.rc_agent_status === 'initializing' ||
                                      (aiAnalysisExecution?.first_task_id != null && aiAnalysisExecution?.rc_agent_status === 'uploaded');
        const canAnalyzeDocModal = !isAudioModal && aiAnalysisExecution && !isProcessingDocModal && (
          aiAnalysisExecution?.rc_agent_status === 'uploaded' ||
          aiAnalysisExecution?.rc_agent_status === 'failed'
        );
        
        const isAudioAnalyzingModal = aiAnalysisExecution ? audioAnalyzingIds.has(aiAnalysisExecution.id) : false;
        const isDocAnalyzingModal = aiAnalysisExecution ? docAnalyzingIds.has(aiAnalysisExecution.id) : false;
        const isAnalyzingModal = isAudioAnalyzingModal || isDocAnalyzingModal;

        return (
          <div style={{ textAlign: 'center', padding: '40px 0' }}>
            {isAnalyzingModal ? (
              <>
                <LoadingOutlined style={{ fontSize: '48px', color: '#1890ff' }} />
                <div style={{ marginTop: 16, fontSize: '16px' }}>
                  {isAudioModal ? 'AI is analyzing your audio transcription...' : 'AI is processing and analyzing your document...'}
                </div>
                <div style={{ marginTop: 8, fontSize: '13px', color: '#999' }}>
                  {isAudioModal
                    ? 'This may take a moment. The report will appear here once completed.'
                    : 'The document will be parsed and analyzed. This may take a few minutes.'}
                </div>
              </>
            ) : (
              <>
                <ExclamationCircleOutlined style={{ fontSize: '48px', color: '#faad14' }} />
                <div style={{ marginTop: 16, fontSize: '16px' }}>
                  No AI analysis report available for this execution.
                </div>
                {canAnalyzeAudioModal && aiAnalysisExecution && (
                  <Button
                    type="primary"
                    icon={<SoundOutlined />}
                    size="large"
                    onClick={() => handleAnalyzeAudio(aiAnalysisExecution)}
                    style={{ 
                      marginTop: '20px',
                      height: '44px',
                      paddingInline: '28px',
                      fontSize: '15px',
                      borderRadius: '8px',
                    }}
                  >
                    Analyze Audio
                  </Button>
                )}
                {canAnalyzeDocModal && aiAnalysisExecution && (
                  <div style={{ maxWidth: '420px', margin: '20px auto 0', textAlign: 'left' }}>
                    <div style={{ marginBottom: '12px', color: '#999', fontSize: '13px', textAlign: 'center' }}>
                      Select a prompt template and click "Analyze" to start AI analysis
                    </div>
                    <TemplateSelector
                      categoryCode="file_analysis"
                      value={modalSelectedTemplateId}
                      onChange={(value) => setModalSelectedTemplateId(value)}
                      placeholder="Select analysis template"
                      style={{ marginBottom: '16px' }}
                    />
                    <div style={{ textAlign: 'center' }}>
                      <Button
                        type="primary"
                        icon={<RobotOutlined />}
                        size="large"
                        disabled={!modalSelectedTemplateId}
                        onClick={() => modalSelectedTemplateId && handleAnalyzeDocument(aiAnalysisExecution, modalSelectedTemplateId)}
                        style={{ 
                          marginTop: '4px',
                          height: '44px',
                          paddingInline: '28px',
                          fontSize: '15px',
                          borderRadius: '8px',
                        }}
                      >
                        Analyze Document
                      </Button>
                    </div>
                  </div>
                )}
              </>
            )}
          </div>
        );
      })()}
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
          <div style={{ marginBottom: 16, padding: '12px', backgroundColor: themeColors.boxMuted, borderRadius: '6px' }}>
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
            backgroundColor: themeColors.boxMuted
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

  // 按 file_type 筛选列表（image 包含 jpg/jpeg/png/gif/bmp/image）
  const matchesFileType = (firstFileType: string | undefined, tab: typeof fileTypeFilter): boolean => {
    const t = firstFileType?.toLowerCase() || '';
    if (tab === 'all') return true;
    if (tab === 'image') return ['image', 'jpg', 'jpeg', 'png', 'gif', 'bmp'].includes(t);
    return t === tab;
  };
  const filteredByType = taskExecutions.filter(exec => matchesFileType(exec.first_file_type, fileTypeFilter));
  const filteredExecutions = searchType === 'file_name' && searchQuery.trim()
    ? filteredByType.filter(exec => (exec.first_filename || '').toLowerCase().includes(searchQuery.trim().toLowerCase()))
    : filteredByType;

  const FILE_TYPE_TABS: { key: typeof fileTypeFilter; label: string }[] = [
    { key: 'all', label: 'All' },
    { key: 'pdf', label: 'PDF' },
    { key: 'doc', label: 'Doc' },
    { key: 'excel', label: 'Excel' },
    { key: 'image', label: 'Image' },
    { key: 'audio', label: 'Audio' },
  ];

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: themeColors.cardBg,
            colorBorderSecondary: themeColors.cardBorder,
          },
        }),
      }}
    >
      <div className="files-page-layout" style={{ backgroundColor: themeColors.pageBg }}>
        <div className="files-page-inner">
          {/* 左侧：Behance 风格文件列表 */}
          <div className="files-list-panel">
            <div className="files-list-header">
              <Space size={8}>
                <FileTextOutlined />
                <span>File List</span>
                <span style={{ fontWeight: 400, fontSize: '13px', opacity: 0.7 }}>({filteredExecutions.length})</span>
              </Space>
              <Space size={2}>
                <Button
                  type={batchMode ? 'primary' : 'text'}
                  size="small"
                  onClick={() => {
                    setBatchMode(prev => !prev);
                    if (batchMode) setSelectedExecutionIds([]);
                  }}
                  className="files-list-batch-btn"
                >
                  Batch
                </Button>
                {batchMode && (
                  <>
                    <Button
                      type="text"
                      size="small"
                      onClick={() => {
                        setSelectedExecutionIds(filteredExecutions.map(e => e.id));
                      }}
                      className="files-list-batch-btn"
                    >
                      All
                    </Button>
                    <Button
                      type="text"
                      size="small"
                      onClick={() => {
                        setBatchMode(false);
                        setSelectedExecutionIds([]);
                      }}
                      className="files-list-batch-btn"
                    >
                      Done
                    </Button>
                  </>
                )}
              </Space>
            </div>
            <div className="files-list-type-tabs">
              {FILE_TYPE_TABS.map(({ key, label }) => (
                <button
                  key={key}
                  type="button"
                  className={`files-list-type-tab ${fileTypeFilter === key ? 'is-active' : ''}`}
                  onClick={() => setFileTypeFilter(key)}
                >
                  {label}
                </button>
              ))}
            </div>
            <div className="files-list-search-bar">
              <Select
                value={searchType}
                onChange={(v) => setSearchType(v)}
                size="small"
                className="files-list-search-type"
                dropdownClassName="files-list-search-type-dropdown"
                options={[
                  { value: 'file_name', label: 'Title' },
                  { value: 'content', label: 'Content' },
                ]}
              />
              <Input
                size="small"
                placeholder={searchType === 'file_name' ? 'Search by title' : 'Search by content'}
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                allowClear
                className="files-list-search-input"
              />
            </div>
            {batchMode && selectedExecutionIds.length > 0 && (
              <div className="files-list-batch-bar">
                <span className="files-list-batch-count">{selectedExecutionIds.length} selected</span>
                <Button
                  type="primary"
                  danger
                  size="small"
                  icon={<DeleteOutlined />}
                  loading={deletingExecutions}
                  onClick={handleBatchDeleteExecutions}
                  className="files-list-batch-bar-btn"
                >
                  Batch Delete
                </Button>
                <Button
                  type="text"
                  size="small"
                  onClick={() => setSelectedExecutionIds([])}
                  className="files-list-batch-bar-btn"
                >
                  Clear
                </Button>
              </div>
            )}
            <div
              ref={filesListScrollRef}
              className="files-list-scroll"
            >
              {filteredExecutions.map(execution => {
                const isSelected = selectedExecutionForView?.id === execution.id;
                const isUploadOnly = execution.rc_agent_status === 'uploaded';
                const isAudioFile = execution.first_file_type === 'audio';
                const canAnalyze = isUploadOnly || execution.rc_agent_status === 'failed';
                const isDocAnalyzing = docAnalyzingIds.has(execution.id);
                const isAudioAnalyzing = audioAnalyzingIds.has(execution.id);
                const isItemAnalyzing = execution.rc_agent_status === 'analyzing' || isDocAnalyzing || isAudioAnalyzing;
                const cardMenuItems: MenuProps['items'] = [
                  // 文档文件：未分析时显示 Analyze 入口
                  ...(!isAudioFile && canAnalyze && !isItemAnalyzing ? [{
                    key: 'analyze',
                    icon: <RobotOutlined />,
                    label: 'AI Analyze',
                    onClick: (e: any) => {
                      e.domEvent.stopPropagation();
                      setSelectedExecutionForView(execution);
                      setViewMode('analysis');
                    },
                  }] : []),
                  {
                    key: 'view-details',
                    icon: <EyeOutlined />,
                    label: 'View Details',
                    onClick: (e) => {
                      e.domEvent.stopPropagation();
                      handleViewExecution(execution);
                    },
                  },
                  {
                    key: 'rename',
                    icon: <EditOutlined />,
                    label: 'Rename',
                    onClick: (e) => {
                      e.domEvent.stopPropagation();
                      handleOpenRename(execution);
                    },
                  },
                  {
                    key: 'delete',
                    icon: <DeleteOutlined />,
                    label: 'Delete',
                    danger: true,
                    onClick: (e) => {
                      e.domEvent.stopPropagation();
                      handleDeleteExecution(execution);
                    },
                  },
                ];
                const isChecked = selectedExecutionIds.includes(execution.id);
                return (
                  <div
                    key={execution.id}
                    className={`files-list-item ${isSelected ? 'is-selected' : ''} ${isChecked ? 'is-checked' : ''}`}
                    onClick={() => {
                      setSelectedExecutionForView(execution);
                      setViewMode('original');
                      setSelectedFileForView(null);
                    }}
                  >
                    <div className="files-list-item-header">
                      {batchMode && (
                        <Checkbox
                          checked={isChecked}
                          onChange={(e) => {
                            e.stopPropagation();
                            if (e.target.checked) {
                              setSelectedExecutionIds(prev => [...prev, execution.id]);
                            } else {
                              setSelectedExecutionIds(prev => prev.filter(id => id !== execution.id));
                            }
                          }}
                          onClick={(e) => e.stopPropagation()}
                          className="files-list-item-checkbox"
                        />
                      )}
                      <div className="files-list-item-title">
                        {execution.first_filename || `${execution.files_processed} file(s)`}
                      </div>
                      <Dropdown
                        menu={{ items: cardMenuItems }}
                        trigger={['click']}
                        placement="bottomRight"
                      >
                        <Button
                          type="text"
                          size="small"
                          icon={<EllipsisOutlined />}
                          className="files-list-item-more-btn"
                          onClick={(e) => e.stopPropagation()}
                        />
                      </Dropdown>
                    </div>
                    <div className="files-list-item-meta">
                      {new Date(execution.created_at).toLocaleString()}
                    </div>
                    <div className="files-list-item-tags">
                      <Tag
                        color={isItemAnalyzing ? 'warning' : isUploadOnly ? 'blue' : (execution.rc_agent_status === 'success' ? 'success' : 'default')}
                        style={{ margin: 0, fontSize: '11px' }}
                      >
                        {isItemAnalyzing ? 'Analyzing...' : isUploadOnly ? 'Uploaded' : (execution.rc_agent_status === 'success' ? 'Analyzed' : 'Not Analyzed')}
                      </Tag>
                    </div>
                  </div>
                );
              })}
              {filteredExecutions.length === 0 && (
                <div style={{ textAlign: 'center', padding: '48px 20px', color: themeColors.textMuted }}>
                  <FileTextOutlined style={{ fontSize: '48px', marginBottom: '16px', display: 'block' }} />
                  <div>{taskExecutions.length === 0 ? 'no file records' : `no ${fileTypeFilter} files`}</div>
                </div>
              )}
              {loadingMoreExecutions && (
                <div style={{ textAlign: 'center', padding: '16px', color: themeColors.textMuted }}>
                  <LoadingOutlined spin style={{ marginRight: 8 }} />
                  Loading...
                </div>
              )}
            </div>
          </div>

          {/* 右侧：内容展示区 */}
          <div className="files-detail-panel">
            {selectedExecutionForView ? (
              <>
                <div className="files-detail-tabs">
                  <div
                    className={`files-detail-tab ${viewMode === 'original' ? 'is-active' : ''}`}
                    onClick={() => setViewMode('original')}
                  >
                    Original
                  </div>
                  <div
                    className={`files-detail-tab ${viewMode === 'analysis' ? 'is-active' : ''}`}
                    onClick={() => {
                      setViewMode('analysis');
                    }}
                  >
                    AI Analysis
                  </div>
                  <div style={{ flex: 1 }} />
                  {viewMode === 'original' && originalFileInfoForTabs && (
                    <Space size={12} style={{ fontSize: 13, fontWeight: 400, color: themeColors.textSecondary }}>
                      <span title={originalFileInfoForTabs.filename} style={{ maxWidth: 200, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {originalFileInfoForTabs.filename}
                      </span>
                      <Tag color="blue" style={{ margin: 0 }}>{originalFileInfoForTabs.file_type?.toUpperCase() || 'Unknown'}</Tag>
                    </Space>
                  )}
                  {viewMode === 'analysis' && aiAnalysisData?.execution?.final_report && (
                    <Button
                      icon={<DownloadOutlined />}
                      onClick={handleDownloadPdf}
                      loading={pdfDownloading}
                      size="small"
                      className={isDark ? 'files-export-pdf-btn files-export-pdf-btn--dark' : 'files-export-pdf-btn'}
                    >
                      Export PDF
                    </Button>
                  )}
                </div>

                <div className="files-detail-content">
                  {viewMode === 'original' ? (
                    <OriginalContentView
                      execution={selectedExecutionForView}
                      themeColors={themeColors}
                      isDark={isDark}
                      onFileSelect={setSelectedFileForView}
                      selectedFile={selectedFileForView}
                      onFileInfoLoaded={setOriginalFileInfoForTabs}
                    />
                  ) : (
                    <AIAnalysisView
                      execution={selectedExecutionForView}
                      themeColors={themeColors}
                      isDark={isDark}
                      aiAnalysisData={aiAnalysisData}
                      aiAnalysisLoading={aiAnalysisLoading}
                      onLoadAnalysis={async () => {
                        try {
                          setAiAnalysisLoading(true);
                          const details = await tasksAPI.getTaskExecutionDetails(selectedExecutionForView.id);
                          setAiAnalysisData(details);
                        } catch (error) {
                          message.error('Failed to load AI analysis report');
                        } finally {
                          setAiAnalysisLoading(false);
                        }
                      }}
                      onAnalyzeAudio={handleAnalyzeAudio}
                      onAnalyzeDocument={handleAnalyzeDocument}
                      audioAnalyzing={audioAnalyzingIds.has(selectedExecutionForView.id)}
                      docAnalyzing={docAnalyzingIds.has(selectedExecutionForView.id)}
                    />
                  )}
                </div>
              </>
            ) : (
              <div className="files-detail-empty">
                <div className="files-detail-empty-inner">
                  <EyeOutlined />
                  <div>Please select a file from the left to view content</div>
                </div>
              </div>
            )}
          </div>
        </div>

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

        {/* 重命名对话框 */}
        <Modal
          title="Rename File"
          open={renameModalVisible}
          onOk={handleConfirmRename}
          okButtonProps={{ disabled: !renameInputValue.trim() }}
          onCancel={() => {
            setRenameModalVisible(false);
            setExecutionToRename(null);
            setRenameInputValue('');
          }}
          okText="Save"
          cancelText="Cancel"
          confirmLoading={renameLoading}
          centered
          destroyOnClose
        >
          {executionToRename && (
            <div>
              <p style={{ marginBottom: 8 }}>New file name:</p>
              <Input
                value={renameInputValue}
                onChange={(e) => setRenameInputValue(e.target.value)}
                placeholder="Enter new file name"
                onPressEnter={handleConfirmRename}
              />
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
            <p>Will set tags for the following files:</p>
            <div style={{ marginBottom: 16, padding: '8px 12px', backgroundColor: themeColors.successBg, border: `1px solid ${themeColors.successBorder}`, borderRadius: '6px' }}>
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
              <div style={{ textAlign: 'center', color: themeColors.textMuted, padding: '20px' }}>
                No files available to set tags
              </div>
            )}
          </div>
        </Modal>
      </div>
    </ConfigProvider>
  );
};

// 原文内容查看组件（仅支持单文件，不再显示文件列表）
const OriginalContentView: React.FC<{
  execution: TaskExecution;
  themeColors: any;
  isDark: boolean;
  onFileSelect: (file: any) => void;
  selectedFile: any;
  onFileInfoLoaded?: (info: { filename?: string; file_type?: string }) => void;
}> = ({ execution, themeColors, isDark, onFileSelect, onFileInfoLoaded }) => {
  const [loading, setLoading] = useState(true);
  const [fileContent, setFileContent] = useState<any>(null);
  const [loadError, setLoadError] = useState<string | null>(null);

  useEffect(() => {
    const loadAndShowFile = async () => {
      try {
        setLoading(true);
        setLoadError(null);
        const details = await tasksAPI.getTaskExecutionDetails(execution.id);
        const files = details.files || [];
        if (files.length === 0) {
          setLoadError('No original file');
          return;
        }
        const file = files[0];
        onFileSelect(file);
        const originalFileInfo = await filesAPI.getOriginalFileInfo(file.id);
        
        // 在URL中添加token参数（用于iframe认证）
        const token = localStorage.getItem('access_token');
        if (token && originalFileInfo.file_url) {
          const separator = originalFileInfo.file_url.includes('?') ? '&' : '?';
          originalFileInfo.file_url = `${originalFileInfo.file_url}${separator}token=${token}`;
        }
        
        console.log('🔍 [DEBUG] Original File Info:', originalFileInfo);
        console.log('🔍 [DEBUG] File URL with token:', originalFileInfo.file_url);
        setFileContent(originalFileInfo);
        onFileInfoLoaded?.({ filename: originalFileInfo.filename, file_type: originalFileInfo.file_type });
      } catch (error) {
        console.error('Failed to load file:', error);
        setLoadError('Failed to load file content');
        message.error('Failed to load file content');
      } finally {
        setLoading(false);
      }
    };

    loadAndShowFile();
  }, [execution.id, onFileSelect, onFileInfoLoaded]);

  if (loading) {
    return (
      <div style={{ textAlign: 'center', padding: '40px', color: themeColors.text }}>
        <LoadingOutlined style={{ fontSize: '32px', color: '#1890ff' }} />
        <div style={{ marginTop: '16px', color: themeColors.textSecondary }}>Loading...</div>
      </div>
    );
  }

  if (loadError || !fileContent) {
    return (
      <div style={{ 
        textAlign: 'center', 
        padding: '40px',
        color: themeColors.textMuted 
      }}>
        <FileTextOutlined style={{ fontSize: '48px', marginBottom: '16px', color: themeColors.textMuted }} />
        <div>{loadError || 'No original file'}</div>
      </div>
    );
  }

  return (
    <div style={{ width: '100%', flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', color: themeColors.text }}>
      {(() => {
        const fileType = fileContent.file_type.toLowerCase();
        console.log('🔍 [DEBUG] Rendering file type:', fileType);
        console.log('🔍 [DEBUG] File URL for iframe:', fileContent.file_url);
        
        if (fileType === 'pdf') {
          return (
            <div style={{ 
              border: `1px solid ${themeColors.cardBorder}`,
              borderRadius: '8px',
              overflow: 'hidden',
              flex: 1,
              minHeight: 0
            }}>
              <iframe
                src={fileContent.file_url}
                style={{ width: '100%', height: '100%', border: 'none' }}
                title={fileContent.filename}
                onLoad={() => console.log('✅ [DEBUG] iframe loaded successfully')}
                onError={(e) => console.error('❌ [DEBUG] iframe load error:', e)}
              />
            </div>
          );
        } else if (['doc', 'docx', 'word'].includes(fileType)) {
          // Word文档预览：使用Microsoft Office Online Viewer
          // 需要文件URL是公网可访问的（生产环境有效）
          // 对于本地开发，提供替代方案
          const isLocalDev = fileContent.file_url.includes('localhost');
          
          if (isLocalDev) {
            // 本地开发环境：显示提示和下载按钮
            return (
              <div style={{ 
                flex: 1,
                minHeight: 0,
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                justifyContent: 'center',
                padding: '40px',
                border: `1px solid ${themeColors.cardBorder}`,
                borderRadius: '8px',
                color: themeColors.text
              }}>
                <FileWordOutlined style={{ fontSize: '48px', color: '#1890ff', marginBottom: '16px' }} />
                <div style={{ marginBottom: '8px', fontSize: '16px', fontWeight: 500 }}>
                  {fileContent.filename}
                </div>
                <div style={{ marginBottom: '16px', color: themeColors.textSecondary }}>
                  Word document preview is available in production environment<br/>
                  Please download and view locally for development
                </div>
                <Button 
                  type="primary" 
                  icon={<DownloadOutlined />}
                  onClick={() => {
                    const link = document.createElement('a');
                    link.href = fileContent.file_url;
                    link.download = fileContent.filename;
                    document.body.appendChild(link);
                    link.click();
                    document.body.removeChild(link);
                  }}
                >
                  Download Word File
                </Button>
              </div>
            );
          } else {
            // 生产环境：使用Microsoft Office Online Viewer
            const encodedUrl = encodeURIComponent(fileContent.file_url);
            const officeViewerUrl = `https://view.officeapps.live.com/op/embed.aspx?src=${encodedUrl}`;
            
            return (
              <div style={{ 
                border: `1px solid ${themeColors.cardBorder}`,
                borderRadius: '8px',
                overflow: 'hidden',
                flex: 1,
                minHeight: 0
              }}>
                <iframe
                  src={officeViewerUrl}
                  style={{ width: '100%', height: '100%', border: 'none' }}
                  title={fileContent.filename}
                  onLoad={() => console.log('✅ [DEBUG] Word viewer loaded successfully')}
                  onError={(e) => console.error('❌ [DEBUG] Word viewer load error:', e)}
                />
              </div>
            );
          }
        } else if (['excel', 'xlsx', 'xls'].includes(fileType)) {
          // Excel文档预览：同样使用Microsoft Office Online Viewer
          const isLocalDev = fileContent.file_url.includes('localhost');
          
          if (isLocalDev) {
            return (
              <div style={{ 
                flex: 1,
                minHeight: 0,
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                justifyContent: 'center',
                padding: '40px',
                border: `1px solid ${themeColors.cardBorder}`,
                borderRadius: '8px',
                color: themeColors.text
              }}>
                <FileExcelOutlined style={{ fontSize: '48px', color: '#52c41a', marginBottom: '16px' }} />
                <div style={{ marginBottom: '8px', fontSize: '16px', fontWeight: 500 }}>
                  {fileContent.filename}
                </div>
                <div style={{ marginBottom: '16px', color: themeColors.textSecondary }}>
                  Excel document preview is available in production environment<br/>
                  Please download and view locally for development
                </div>
                <Button 
                  type="primary" 
                  icon={<DownloadOutlined />}
                  onClick={() => {
                    const link = document.createElement('a');
                    link.href = fileContent.file_url;
                    link.download = fileContent.filename;
                    document.body.appendChild(link);
                    link.click();
                    document.body.removeChild(link);
                  }}
                >
                  Download Excel File
                </Button>
              </div>
            );
          } else {
            const encodedUrl = encodeURIComponent(fileContent.file_url);
            const officeViewerUrl = `https://view.officeapps.live.com/op/embed.aspx?src=${encodedUrl}`;
            
            return (
              <div style={{ 
                border: `1px solid ${themeColors.cardBorder}`,
                borderRadius: '8px',
                overflow: 'hidden',
                flex: 1,
                minHeight: 0
              }}>
                <iframe
                  src={officeViewerUrl}
                  style={{ width: '100%', height: '100%', border: 'none' }}
                  title={fileContent.filename}
                  onLoad={() => console.log('✅ [DEBUG] Excel viewer loaded successfully')}
                  onError={(e) => console.error('❌ [DEBUG] Excel viewer load error:', e)}
                />
              </div>
            );
          }
        } else if (['jpg', 'jpeg', 'png', 'gif', 'bmp', 'image'].includes(fileType)) {
          return (
            <div style={{ flex: 1, minHeight: 0, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
              <img 
                src={fileContent.file_url} 
                alt={fileContent.filename}
                style={{ 
                  maxWidth: '100%', 
                  maxHeight: '100%',
                  objectFit: 'contain',
                  border: `1px solid ${themeColors.cardBorder}`,
                  borderRadius: '8px'
                }}
              />
            </div>
          );
        } else if (fileType === 'audio' && fileContent.asr_with_diarization) {
          // 音频文件：显示说话人分离可视化界面
          return <AudioDiarizationView fileContent={fileContent} themeColors={themeColors} isDark={isDark} />;
        } else {
          return (
            <div style={{ 
              flex: 1,
              minHeight: 0,
              display: 'flex',
              flexDirection: 'column',
              alignItems: 'center',
              justifyContent: 'center',
              padding: '40px',
              border: `1px solid ${themeColors.cardBorder}`,
              borderRadius: '8px',
              color: themeColors.text
            }}>
              <FileTextOutlined style={{ fontSize: '48px', color: '#1890ff', marginBottom: '16px' }} />
              <div style={{ marginBottom: '16px', color: themeColors.textSecondary }}>
                This file type cannot be previewed online
              </div>
              <Button 
                type="primary" 
                icon={<DownloadOutlined />}
                onClick={() => {
                  const link = document.createElement('a');
                  link.href = fileContent.file_url;
                  link.download = fileContent.filename;
                  document.body.appendChild(link);
                  link.click();
                  document.body.removeChild(link);
                }}
              >
                Download File
              </Button>
            </div>
          );
        }
      })()}
    </div>
  );
};

// 音频说话人分离可视化组件
const AudioDiarizationView: React.FC<{
  fileContent: any;
  themeColors: any;
  isDark: boolean;
  onSpeakerUpdated?: () => void;
}> = ({ fileContent, themeColors, isDark, onSpeakerUpdated }) => {
  const [localAsrData, setLocalAsrData] = useState(fileContent.asr_with_diarization);
  const [editingSpeaker, setEditingSpeaker] = useState<{ id: number; currentLabel: string } | null>(null);
  const [newSpeakerLabel, setNewSpeakerLabel] = useState('');
  const [savingLabel, setSavingLabel] = useState(false);
  
  const asrData = localAsrData;
  
  if (!asrData || !asrData.sentences || asrData.sentences.length === 0) {
    return (
      <div style={{ 
        textAlign: 'center', 
        padding: '40px',
        color: themeColors.textMuted 
      }}>
        <FileTextOutlined style={{ fontSize: '48px', marginBottom: '16px', color: themeColors.textMuted }} />
        <div>No speaker diarization data available</div>
      </div>
    );
  }
  
  // 格式化时间（毫秒转为 MM:SS 格式）
  const formatTime = (milliseconds: number) => {
    const seconds = Math.floor(milliseconds / 1000);
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    return `${minutes.toString().padStart(2, '0')}:${remainingSeconds.toString().padStart(2, '0')}`;
  };
  
  // 格式化时间（毫秒转为 MM:SS.mmm 格式，更精确）
  const formatTimeDetailed = (milliseconds: number) => {
    const seconds = Math.floor(milliseconds / 1000);
    const ms = milliseconds % 1000;
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    return `${minutes.toString().padStart(2, '0')}:${remainingSeconds.toString().padStart(2, '0')}.${Math.floor(ms / 100)}`;
  };
  
  // 说话人颜色映射
  const speakerColors = [
    '#1890ff', // 蓝色
    '#52c41a', // 绿色
    '#fa8c16', // 橙色
    '#eb2f96', // 粉色
    '#722ed1', // 紫色
    '#13c2c2', // 青色
    '#faad14', // 金色
    '#f5222d', // 红色
  ];
  
  const getSpeakerColor = (speakerId: number) => {
    return speakerColors[speakerId % speakerColors.length];
  };

  const handleDownloadDialogueTxt = () => {
    const lines = asrData.sentences.map((s: any) => {
      const speakerLabel = asrData.speakers.find((sp: any) => sp.speaker_id === s.speaker_id)?.label || `Speaker ${s.speaker_id}`;
      const timeRange = `${formatTimeDetailed(s.begin_time)} - ${formatTimeDetailed(s.end_time)}`;
      return `${speakerLabel} [${timeRange}]:\n${s.text || ''}`;
    });
    const content = lines.join('\n\n');
    const blob = new Blob([content], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${(fileContent.filename || 'dialogue').replace(/\.[^.]+$/, '')}_对话.txt`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleEditSpeaker = (speaker: any) => {
    setEditingSpeaker({ id: speaker.speaker_id, currentLabel: speaker.label });
    setNewSpeakerLabel(speaker.label);
  };

  const handleCancelEdit = () => {
    setEditingSpeaker(null);
    setNewSpeakerLabel('');
  };

  const handleSaveSpeakerLabel = async () => {
    if (!editingSpeaker || !newSpeakerLabel.trim()) {
      message.warning('Please input a valid speaker name');
      return;
    }

    setSavingLabel(true);
    try {
      await filesAPI.updateSpeakerLabel(fileContent.file_id, editingSpeaker.id, newSpeakerLabel.trim());
      
      // 更新本地状态
      const updatedAsrData = { ...asrData };
      const speakerToUpdate = updatedAsrData.speakers.find((sp: any) => sp.speaker_id === editingSpeaker.id);
      if (speakerToUpdate) {
        speakerToUpdate.label = newSpeakerLabel.trim();
      }
      setLocalAsrData(updatedAsrData);
      
      message.success('Speaker name updated successfully');
      handleCancelEdit();
      
      // 通知父组件刷新
      if (onSpeakerUpdated) {
        onSpeakerUpdated();
      }
    } catch (error: any) {
      message.error(`Update failed: ${error.response?.data?.detail || error.message}`);
    } finally {
      setSavingLabel(false);
    }
  };
  
  return (
    <div style={{ 
      flex: 1, 
      minHeight: 0, 
      overflow: 'auto',
      padding: '20px',
      border: `1px solid ${themeColors.cardBorder}`,
      borderRadius: '8px',
      backgroundColor: isDark ? '#1a1d2e' : '#fafafa'
    }}>
      {/* 头部信息 */}
      <div style={{ marginBottom: '20px', display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: '16px', flexWrap: 'wrap' }}>
        <div>
          <div style={{ 
            fontSize: '16px', 
            fontWeight: 600, 
            color: themeColors.text,
            marginBottom: '12px'
          }}>
            {fileContent.filename}
          </div>
          <div style={{ display: 'flex', gap: '16px', fontSize: '13px', color: themeColors.textSecondary }}>
            <span>duration: {formatTime(asrData.properties.duration_ms)}</span>
            <span>speaker count: {asrData.speaker_count}</span>
            <span>sentence count: {asrData.total_sentences}</span>
          </div>
        </div>
        <Button
          type="primary"
          icon={<DownloadOutlined />}
          onClick={handleDownloadDialogueTxt}
          size="small"
        >
          download
        </Button>
      </div>
      
      {/* 说话人列表 */}
      <div style={{ marginBottom: '20px' }}>
        <div style={{ 
          fontSize: '12px', 
          fontWeight: 500, 
          color: themeColors.text,
          marginBottom: '8px'
        }}>
          speaker list
        </div>
        <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
          {asrData.speakers.map((speaker: any) => (
            <Tag 
              key={speaker.speaker_id}
              color={getSpeakerColor(speaker.speaker_id)}
              style={{ 
                margin: 0,
                padding: '4px 12px',
                fontSize: '12px',
                fontWeight: 500,
                cursor: 'pointer',
                transition: 'all 0.2s'
              }}
              onClick={() => handleEditSpeaker(speaker)}
              onMouseEnter={(e) => {
                e.currentTarget.style.opacity = '0.8';
                e.currentTarget.style.transform = 'translateY(-1px)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.opacity = '1';
                e.currentTarget.style.transform = 'translateY(0)';
              }}
            >
              {speaker.label} <EditOutlined style={{ fontSize: '10px', marginLeft: '4px' }} />
            </Tag>
          ))}
        </div>
      </div>
      
      <Divider style={{ margin: '16px 0', borderColor: themeColors.cardBorder }} />
      
      {/* 时间轴 */}
      <div style={{ 
        fontSize: '11px', 
        fontWeight: 500, 
        color: themeColors.text,
        marginBottom: '12px'
      }}>
        time axis
      </div>
      
      <div style={{ 
        display: 'flex', 
        flexDirection: 'column', 
        gap: '12px' 
      }}>
        {asrData.sentences.map((sentence: any, index: number) => {
          const speakerColor = getSpeakerColor(sentence.speaker_id);
          const timeRange = `${formatTimeDetailed(sentence.begin_time)} - ${formatTimeDetailed(sentence.end_time)}`;
          
          return (
            <div
              key={sentence.sentence_id || index}
              style={{
                display: 'flex',
                gap: '12px',
                padding: '12px',
                backgroundColor: isDark ? '#252a3e' : '#ffffff',
                borderLeft: `4px solid ${speakerColor}`,
                borderRadius: '6px',
                boxShadow: isDark 
                  ? '0 1px 3px rgba(0,0,0,0.3)' 
                  : '0 1px 2px rgba(0,0,0,0.06)',
                transition: 'all 0.2s ease',
                cursor: 'pointer'
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = 'translateX(4px)';
                e.currentTarget.style.boxShadow = isDark
                  ? '0 2px 8px rgba(0,0,0,0.4)'
                  : '0 2px 8px rgba(0,0,0,0.1)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.transform = 'translateX(0)';
                e.currentTarget.style.boxShadow = isDark
                  ? '0 1px 3px rgba(0,0,0,0.3)'
                  : '0 1px 2px rgba(0,0,0,0.06)';
              }}
            >
              {/* 说话人标签 */}
              <div style={{ 
                minWidth: '80px',
                display: 'flex',
                alignItems: 'flex-start',
                paddingTop: '2px'
              }}>
                <Tag 
                  color={speakerColor}
                  style={{ 
                    margin: 0,
                    fontSize: '12px',
                    fontWeight: 500,
                    padding: '2px 8px'
                  }}
                >
                  {asrData.speakers.find((sp: any) => sp.speaker_id === sentence.speaker_id)?.label || `speaker ${sentence.speaker_id}`}
                </Tag>
              </div>
              
              {/* 时间和文本 */}
              <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ 
                  fontSize: '12px', 
                  color: themeColors.textSecondary,
                  marginBottom: '4px',
                  fontFamily: 'monospace'
                }}>
                  {timeRange}
                </div>
                <div style={{ 
                  fontSize: '12px', 
                  color: themeColors.text,
                  lineHeight: '1.6',
                  wordBreak: 'break-word'
                }}>
                  {sentence.text}
                </div>
              </div>
            </div>
          );
        })}
      </div>

      {/* 编辑说话人名称的 Modal */}
      <Modal
        title="Modify speaker name"
        open={!!editingSpeaker}
        onOk={handleSaveSpeakerLabel}
        onCancel={handleCancelEdit}
        okText="save"
        cancelText="cancel"
        confirmLoading={savingLabel}
      >
        <div style={{ marginTop: '16px' }}>
          <div style={{ marginBottom: '8px', fontSize: '14px', color: themeColors.textSecondary }}>
            Current name: <strong>{editingSpeaker?.currentLabel}</strong>
          </div>
          <Input
            placeholder="input new speaker name"
            value={newSpeakerLabel}
            onChange={(e) => setNewSpeakerLabel(e.target.value)}
            onPressEnter={handleSaveSpeakerLabel}
            autoFocus
          />
        </div>
      </Modal>
    </div>
  );
};

// AI Analysis 查看组件
const AIAnalysisView: React.FC<{
  execution: TaskExecution;
  themeColors: any;
  isDark: boolean;
  aiAnalysisData: TaskExecutionDetail | null;
  aiAnalysisLoading: boolean;
  onLoadAnalysis: () => Promise<void>;
  onAnalyzeAudio?: (record: TaskExecution) => void;
  onAnalyzeDocument?: (record: TaskExecution, templateId: number) => void;
  audioAnalyzing?: boolean;
  docAnalyzing?: boolean;
}> = ({ execution, themeColors, isDark, aiAnalysisData, aiAnalysisLoading, onLoadAnalysis, onAnalyzeAudio, onAnalyzeDocument, audioAnalyzing, docAnalyzing }) => {
  const [selectedTemplateId, setSelectedTemplateId] = useState<number | undefined>(undefined);

  useEffect(() => {
    if (!aiAnalysisData || aiAnalysisData.execution.id !== execution.id) {
      onLoadAnalysis();
    }
  }, [execution.id, aiAnalysisData, onLoadAnalysis]);

  if (aiAnalysisLoading) {
    return (
      <div style={{ textAlign: 'center', padding: '40px' }}>
        <LoadingOutlined style={{ fontSize: '32px', color: '#1890ff' }} />
        <div style={{ marginTop: '16px', color: themeColors.textSecondary }}>Loading AI analysis report...</div>
      </div>
    );
  }

  // 正在分析中：显示分析进度（音频或文档）
  if (audioAnalyzing || docAnalyzing) {
    const isAudio = execution.first_file_type === 'audio';
    return (
      <div style={{ textAlign: 'center', padding: '40px' }}>
        <LoadingOutlined style={{ fontSize: '48px', color: '#1890ff', marginBottom: '16px' }} />
        <div style={{ color: themeColors.text, fontSize: '16px', fontWeight: 500, marginBottom: '8px' }}>
          {isAudio ? 'AI Analyzing Audio...' : 'AI Analyzing Document...'}
        </div>
        <div style={{ color: themeColors.textSecondary, fontSize: '13px' }}>
          {isAudio
            ? 'The AI is analyzing your audio transcription. This may take a moment.'
            : 'The AI is processing and analyzing your document. This may take a few minutes.'}
        </div>
      </div>
    );
  }

  if (!aiAnalysisData?.execution?.final_report) {
    const isAudioFile = execution.first_file_type === 'audio';
    const canAnalyzeAudio = isAudioFile && onAnalyzeAudio && 
      (execution.rc_agent_status === 'completed' || execution.rc_agent_status === 'failed');
    // 文档分析：需要区分"仅上传"和"正在处理"
    // 1. 如果first_task_id为null且rc_agent_status为uploaded，说明仅上传未处理，允许分析
    // 2. 如果rc_agent_status为failed，允许重新分析
    // 3. 如果有task_id或状态为processing/initializing等，说明正在处理，禁止重复分析
    const isProcessingDoc = execution.rc_agent_status === 'processing' || 
                            execution.rc_agent_status === 'initializing' ||
                            (execution.first_task_id != null && execution.rc_agent_status === 'uploaded');
    const canAnalyzeDoc = !isAudioFile && onAnalyzeDocument && !isProcessingDoc && (
      execution.rc_agent_status === 'uploaded' ||
      execution.rc_agent_status === 'failed'
    );

    return (
      <div style={{ textAlign: 'center', padding: '40px' }}>
        <ExclamationCircleOutlined style={{ fontSize: '48px', color: '#faad14', marginBottom: '16px' }} />
        <div style={{ color: themeColors.textSecondary, marginBottom: (canAnalyzeAudio || canAnalyzeDoc) ? '20px' : 0 }}>
          This file has no AI analysis report
        </div>
        {canAnalyzeAudio && (
          <Button
            type="primary"
            icon={<SoundOutlined />}
            size="large"
            onClick={() => onAnalyzeAudio(execution)}
            style={{ 
              marginTop: '8px',
              height: '44px',
              paddingInline: '28px',
              fontSize: '15px',
              borderRadius: '8px',
            }}
          >
            Analyze Audio
          </Button>
        )}
        {canAnalyzeDoc && (
          <div style={{ maxWidth: '420px', margin: '0 auto', textAlign: 'left' }}>
            <div style={{ marginBottom: '12px', color: themeColors.textSecondary, fontSize: '13px', textAlign: 'center' }}>
              Select a prompt template and click "Analyze" to start AI analysis
            </div>
            <TemplateSelector
              categoryCode="file_analysis"
              value={selectedTemplateId}
              onChange={(value) => setSelectedTemplateId(value)}
              placeholder="Select analysis template"
              style={{ marginBottom: '16px' }}
            />
            <div style={{ textAlign: 'center' }}>
              <Button
                type="primary"
                icon={<RobotOutlined />}
                size="large"
                disabled={!selectedTemplateId}
                onClick={() => selectedTemplateId && onAnalyzeDocument(execution, selectedTemplateId)}
                style={{ 
                  marginTop: '4px',
                  height: '44px',
                  paddingInline: '28px',
                  fontSize: '15px',
                  borderRadius: '8px',
                }}
              >
                Analyze Document
              </Button>
            </div>
          </div>
        )}
      </div>
    );
  }

  return (
    <div>
      <div style={{ 
        marginBottom: '16px', 
        padding: '12px',
        backgroundColor: themeColors.boxMuted,
        borderRadius: '6px'
      }}>
        <Row gutter={16}>
          <Col span={12}>
            <Text strong>Analysis Time:</Text> {new Date(aiAnalysisData.execution.created_at).toLocaleString()}
          </Col>
          <Col span={12}>
            <Text strong>Token Usage:</Text> {aiAnalysisData.execution.total_tokens?.toLocaleString() || '-'}
          </Col>
        </Row>
      </div>
      
      <div 
        id="ai-analysis-content"
        className={isDark ? 'ai-analysis-content ai-analysis-content--dark' : 'ai-analysis-content'}
        style={{ 
          background: isDark ? '#151b26' : '#ffffff', 
          padding: '24px', 
          borderRadius: '12px', 
          border: `1px solid ${themeColors.cardBorder}`,
          boxShadow: isDark ? '0 2px 12px rgba(0,0,0,0.2)' : '0 1px 3px rgba(0,0,0,0.04)',
        }}
      >
        <MarkdownRenderer 
          content={aiAnalysisData.execution.final_report}
          variant={isDark ? 'dark' : 'light'}
          style={{
            fontSize: '14px',
            lineHeight: '1.8',
            color: themeColors.text
          }}
        />
      </div>
    </div>
  );
};

export default FilesPage; 