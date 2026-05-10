/* eslint-disable @typescript-eslint/no-unused-vars */
import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import {
  Card,
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
  ConfigProvider,
  Dropdown,
  Checkbox,
  App,
  Descriptions,
  Avatar,
  Segmented,
  AutoComplete,
  theme as antdTheme
} from 'antd';
import type { MenuProps } from 'antd';
import {
  DeleteOutlined,
  EyeOutlined,
  FileTextOutlined,
  FilePdfOutlined,
  FileExcelOutlined,
  FileWordOutlined,
  FileImageOutlined,
  ExclamationCircleOutlined,
  DownloadOutlined,
  RobotOutlined,
  FolderOutlined,
  LoadingOutlined,
  EllipsisOutlined,
  EditOutlined,
  SearchOutlined,
  SoundOutlined,
  DoubleLeftOutlined,
  DoubleRightOutlined,
  TagsOutlined,
  CheckCircleOutlined,
  CheckOutlined,
  ReloadOutlined,
  LinkOutlined,
  VideoCameraOutlined,
  AppstoreOutlined,
  CloseOutlined,
  ShareAltOutlined,
  UsergroupAddOutlined,
  UserOutlined,
  ThunderboltOutlined,
  CloudUploadOutlined,
  PlusOutlined,
  FolderAddOutlined,
  InboxOutlined,
  DownOutlined,
  UpOutlined,
  GlobalOutlined,
  InfoCircleOutlined,
  PrinterOutlined,
  AppleOutlined,
} from '@ant-design/icons';
import { filesAPI, SectorOption, TickerOption, FolderResponse } from '../services/files';
import { tasksAPI, TaskExecutionDetail } from '../services/tasks';
import { fileTagsService } from '../services/fileTags';
import { getErrorMessage } from '../utils/errorHandler';
import { appendServeAuthToken } from '../utils/appendServeAuthToken';
import { getModalStyles } from '../utils/modalStyles';
import {
  getExecutionStatusDisplay,
  getRcAgentStatusTagColor,
  normalizeRcAgentStatusForAnalyze,
} from '../utils/rcAgentStatus';
import { useSearchParams } from 'react-router-dom';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import { useDeviceDetection } from '../hooks/useDeviceDetection';
import MarkdownRenderer from '../components/MarkdownRenderer';
import IBTradingReportView, { isIBReportContent, IBReportData } from '../components/IBTradingReportView';
import TradeReviewView from '../components/TradeReviewView';
import { downloadElementAsPDF } from '../utils/pdfDownload';
import ReportTickerTags, { parseReportTickers } from '../components/ReportTickerTags';
import { aiTagChipStyle, AI_TAG_CHIP_BASE } from '../components/shared/aiTagChipStyles';
import TemplateSelector from '../components/TemplateSelector';
import TemplateVariablesEditor from '../components/TemplateVariablesEditor';
import promptTemplateService, { PromptTemplate } from '../services/promptTemplateService';
import OriginalPdfJsViewer from '../components/OriginalPdfJsViewer';
import { TASK_EXECUTIONS_REFRESH_EVENT } from '../components/CreateTextNoteModal';
import UploadAnalysisTemplatePicker, {
  UploadAnalysisTemplatePickerValue,
} from '../components/UploadAnalysisTemplatePicker';

const { Text } = Typography;
const { Option } = Select;
const { TextArea } = Input;

/** 转写搜索高亮（Original 面板） */
function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function highlightTranscriptText(text: string, query: string, markStyle: React.CSSProperties): React.ReactNode {
  const q = query.trim();
  if (!q) return text;
  const parts = text.split(new RegExp(`(${escapeRegExp(q)})`, 'gi'));
  return parts.map((part, i) =>
    part.toLowerCase() === q.toLowerCase() ? (
      <mark key={i} style={markStyle}>
        {part}
      </mark>
    ) : (
      <React.Fragment key={i}>{part}</React.Fragment>
    )
  );
}

/** Original 面板：与后端枚举 / MIME 对齐，避免 application/pdf 等落入「无法在线预览」分支 */
function isOriginalPdfFile(fileType: string | undefined | null, filename: string | undefined | null): boolean {
  const ft = (fileType || '').toLowerCase();
  if (ft === 'pdf' || ft === 'application/pdf') return true;
  return /\.pdf$/i.test(filename || '');
}

/**
 * 阿里云 OSS 虚拟主机默认域名且不像已签名 URL 时，浏览器常被 Content-Disposition 强制下载，iframe 无法内嵌预览。
 * 签名参数不止 OSSAccessKeyId（STS / V4 等），需一并识别；走 `/api/files/serve/` 代理的 URL 一律视为可预览。
 */
function isUnsignedOssDefaultDomainObjectUrl(fileUrl: string): boolean {
  if (!fileUrl) return false;
  if (fileUrl.includes('/api/files/serve/')) return false;
  try {
    const u = new URL(fileUrl, typeof window !== 'undefined' ? window.location.origin : 'http://localhost');
    const host = u.hostname.toLowerCase();
    if (!host.includes('aliyuncs.com') && !host.includes('aliyuncs.net')) return false;
    const q = u.search.slice(1).toLowerCase();
    if (!q) return true;
    if (
      q.includes('ossaccesskeyid') ||
      q.includes('signature=') ||
      q.includes('x-oss-signature') ||
      q.includes('x-oss-credential') ||
      q.includes('security-token')
    ) {
      return false;
    }
    return true;
  } catch {
    return false;
  }
}

/** iframe/embed 使用绝对 URL，避免部分手机 WebView 对相对路径解析异常 */
function absoluteUrlForEmbeddedFile(fileUrl: string): string {
  if (!fileUrl) return fileUrl;
  if (fileUrl.startsWith('http://') || fileUrl.startsWith('https://')) return fileUrl;
  if (typeof window === 'undefined') return fileUrl;
  if (fileUrl.startsWith('//')) return `${window.location.protocol}${fileUrl}`;
  if (fileUrl.startsWith('/')) return `${window.location.origin}${fileUrl}`;
  return fileUrl;
}

// 与 FRONTEND_STYLE_GUIDE 一致：语义化色板 + Behance 风格
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
    successBg: '#f6ffed',
    successBorder: '#b7eb8f',
    textMuted: '#999',
    boxMuted: '#f5f5f5',
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
    hoverBg: 'rgba(255, 255, 255, 0.06)',
    successBg: 'rgba(82, 196, 26, 0.15)',
    successBorder: 'rgba(82, 196, 26, 0.45)',
    textMuted: '#A0A3AD',
    boxMuted: '#1E202B',
  },
} as const;

// Behance 风格：圆角 12px、柔和阴影、细边框
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

// Modal Behance 样式（Dark 模式与 CARD_STYLE_BEHANCE 一致）

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
  first_filename?: string;
  first_file_type?: string;
  llm_model?: string;
  llm_provider?: string;
  first_task_id?: string;
  upload_params?: any;
  access_type?: string;
  shared_by?: string;
  share_info?: {
    shared_by: { user_id: number; username: string; avatar_url?: string | null };
    shared_to: Array<{ user_id: number; username: string; avatar_url?: string | null }>;
  } | null;
  ticker_tags?: string[] | null;
  folder_id?: number | null;
  folder_name?: string | null;
  first_display_name?: string | null;
}

interface ProcessingParams {
  processing_mode?: 'quick' | 'llamaparse';
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

const AUDIO_LIKE_TYPES: ReadonlyArray<string> = ['audio', 'podcast'];

const FilesPageInner: React.FC = () => {
  const [searchParams, setSearchParams] = useSearchParams();
  const executionFromUrl = searchParams.get('execution');
  const { themeMode } = useDashboardSettings();
  const { addTask, updateTaskStatus: updateGlobalTaskStatus, registerSourceListRefresh } = useFileProcessing();
  const { modal } = App.useApp();
  const isDark = themeMode === 'dark';
  const themeColors = THEME_COLORS[themeMode];
  const [files, setFiles] = useState<FileItem[]>([]);
  const [processing, setProcessing] = useState(false);
  const [currentTaskId, setCurrentTaskId] = useState<string | null>(null);
  const [lastCompletedTaskId, setLastCompletedTaskId] = useState<string | null>(null);
  const [, setTaskStatus] = useState<TaskStatus | null>(null);
  
  const [selectedTemplateId, setSelectedTemplateId] = useState<number | undefined>(undefined);
  const [templateVariables, setTemplateVariables] = useState<Record<string, any>>({});
  const [, setShowParams] = useState(false);
  const [allTemplates, setAllTemplates] = useState<PromptTemplate[]>([]);
  const [refreshingStatus, setRefreshingStatus] = useState(false);
  const [stoppingTask, setStoppingTask] = useState(false);
  const [updatingStatus, setUpdatingStatus] = useState(false);
  const [updatingFiles, setUpdatingFiles] = useState(false);
  
  // File sharing state
  const [shareModalVisible, setShareModalVisible] = useState(false);
  const [shareTargetExecution, setShareTargetExecution] = useState<TaskExecution | null>(null);
  const [shareableUsers, setShareableUsers] = useState<Array<{ id: number; username: string }>>([]);
  const [selectedShareUserIds, setSelectedShareUserIds] = useState<number[]>([]);
  const [sharingInProgress, setSharingInProgress] = useState(false);
  const [currentShares, setCurrentShares] = useState<Array<{ id: number; shared_to_user_id: number; shared_to_username: string; shared_by_username: string; created_at: string }>>([]);
  
  // 行业和股票选择功能已移除 - 改用自动打标签（content_tag_v1）
  
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
  const [selectedExecutionIds, setSelectedExecutionIds] = useState<string[]>([]);
  const [batchMode, setBatchMode] = useState(false);
  const [deleteExecutionVisible, setDeleteExecutionVisible] = useState(false);
  const [executionToDelete, setExecutionToDelete] = useState<TaskExecution | null>(null);
  const [deletingExecutions, setDeletingExecutions] = useState(false);
  const [renameModalVisible, setRenameModalVisible] = useState(false);
  const [executionToRename, setExecutionToRename] = useState<TaskExecution | null>(null);
  const [renameStem, setRenameStem] = useState('');
  const [renameExtension, setRenameExtension] = useState('');
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
  // Phase 2: 中间弹窗 Analyze 区用 picker value 替代旧 selectedTemplateId
  const [modalAnalyzeDocPicker, setModalAnalyzeDocPicker] = useState<UploadAnalysisTemplatePickerValue>({});
  // 保留 backward compat aliases (部分旧路径仍引用)
  const modalSelectedTemplateId = modalAnalyzeDocPicker.templateId;
  const modalTemplateVariables = modalAnalyzeDocPicker.templateVariables || {};
  const [modalAudioAnalyzePicker, setModalAudioAnalyzePicker] = useState<UploadAnalysisTemplatePickerValue>({});
  
  // 新布局相关状态
  const [selectedExecutionForView, setSelectedExecutionForView] = useState<TaskExecution | null>(null);
  const [rightPanelView, setRightPanelView] = useState<'info' | 'analysis' | 'content' | 'review'>('info');
  const [ibReportDataForReview, setIbReportDataForReview] = useState<IBReportData | null>(null);
  const [rightPanelCollapsed, setRightPanelCollapsed] = useState(false);
  const [selectedFileForView, setSelectedFileForView] = useState<any>(null);
  const [originalFileInfoForTabs, setOriginalFileInfoForTabs] = useState<{ filename?: string; file_type?: string } | null>(null);
  const [contentDetailData, setContentDetailData] = useState<TaskExecutionDetail | null>(null);
  const [contentDetailLoading, setContentDetailLoading] = useState(false);
  const [searchType, setSearchType] = useState<'file_name' | 'content' | 'ticker'>('file_name');
  const [searchQuery, setSearchQuery] = useState('');
  const [tickerFilter, setTickerFilter] = useState<string | undefined>(undefined);
  const [tickerSearchOptions, setTickerSearchOptions] = useState<Array<{ value: string; label: string }>>([]);
  /** 方案 B：高级搜索（Title / Content / Ticker）默认折叠 */
  const [searchPanelExpanded, setSearchPanelExpanded] = useState(false);
  
  // Folder state
  const [folders, setFolders] = useState<FolderResponse[]>([]);
  const [selectedFolderId, setSelectedFolderId] = useState<string | undefined>(undefined);
  const [creatingFolder, setCreatingFolder] = useState(false);
  const [newFolderName, setNewFolderName] = useState('');
  const [editingFolderId, setEditingFolderId] = useState<number | null>(null);
  const [editingFolderName, setEditingFolderName] = useState('');
  const [moveToFolderVisible, setMoveToFolderVisible] = useState(false);
  const [moveTargetExecution, setMoveTargetExecution] = useState<any>(null);
  /** 文件夹区：折叠块 + 内部滚动，默认收缩 */
  const [folderPanelExpanded, setFolderPanelExpanded] = useState(false);

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
    });
    const [form] = Form.useForm();

  // 切换 execution 时清空 Original 标签栏的文件信息及 Content 数据（会由对应视图重新加载）
  useEffect(() => {
    setOriginalFileInfoForTabs(null);
    setContentDetailData(null);
    setIbReportDataForReview(null);
    if (rightPanelView === 'review') setRightPanelView('info');
  }, [selectedExecutionForView?.id]);

  // 活动报告（IB）：右侧面板不展示 AI Analysis / Markdown；若当前在对应 Tab 则切回 Info
  useEffect(() => {
    if (ibReportDataForReview && (rightPanelView === 'analysis' || rightPanelView === 'content')) {
      setRightPanelView('info');
    }
  }, [ibReportDataForReview, rightPanelView]);

  // 加载任务执行历史的函数 - 将在状态声明后重新定义

  // 加载API密钥等关键配置
  const loadDefaultConfig = useCallback(async () => {
    try {
      await filesAPI.getProcessingParams();
    } catch (error: any) {
      console.error('加载默认配置失败:', error);
    }
  }, []);

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
    
    setSelectedTemplateId(undefined);
    setTemplateVariables({});
    setModalAnalyzeDocPicker({});
    
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
    });
    
    // 重新加载默认配置（包括API密钥等）
    loadDefaultConfig();
  }, [loadDefaultConfig]);

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

  // 任务执行历史错误重试状态管理
  const [taskHistoryLoadAttempts, setTaskHistoryLoadAttempts] = useState(0);
  const [lastTaskHistoryError, setLastTaskHistoryError] = useState<number>(0);
  const maxTaskHistoryRetries = 3; // 最大重试次数
  const taskHistoryRetryInterval = 30000; // 重试间隔30秒

  // 文件类型筛选（'podcast' | 'audio' | 'youtube' | 'document' | '' = 全部）
  const [fileTypeFilter, setFileTypeFilter] = useState<string>('');

  // 分页与无限滚动
  const EXECUTIONS_PAGE_SIZE = 20;
  const [executionsPage, setExecutionsPage] = useState(1);
  const [hasMoreExecutions, setHasMoreExecutions] = useState(true);
  const [loadingMoreExecutions, setLoadingMoreExecutions] = useState(false);
  const filesListScrollRef = useRef<HTMLDivElement>(null);
  /** 文件类型筛选横条：与 FeedReader 节目条一致，竖滚轮映射为横向滚动 */
  const fileTypeStripRef = useRef<HTMLDivElement>(null);
  /** Watchlist `/files?execution=` 进入后：左侧列表需滚动到已定位的条目 */
  const pendingScrollToExecutionFromDeepLinkRef = useRef(false);
  const scrollDeepLinkTargetExecutionIdRef = useRef<string | null>(null);
  const deepLinkScrollAppendAttemptsRef = useRef(0);

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
        ...(searchType === 'ticker' && tickerFilter ? { ticker: tickerFilter } : {}),
        ...(selectedFolderId ? { folder_id: selectedFolderId } : {}),
        ...(fileTypeFilter ? { file_type: fileTypeFilter } : {}),
      });
      
      if (append) {
        setTaskExecutions(prev => [...prev, ...executions]);
        setExecutionsPage(pageToLoad);
      } else {
        setTaskExecutions(executions);
        setExecutionsPage(1);
        // 刷新 Basic Information：若当前选中的 execution 在新列表中，用最新数据更新
        if (selectedExecutionForView) {
          const freshExecution = executions.find((e: TaskExecution) => e.id === selectedExecutionForView.id);
          if (freshExecution) {
            setSelectedExecutionForView(freshExecution);
          }
        }
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
      } else if (taskHistoryLoadAttempts >= maxTaskHistoryRetries && !isAutoRefresh) {
        // 最后一次重试失败，显示最终错误消息
        message.error('任务执行历史加载失败，请检查网络连接或联系管理员');
      }
    } finally {
      if (append) setLoadingMoreExecutions(false);
    }
  }, [taskHistoryLoadAttempts, lastTaskHistoryError, maxTaskHistoryRetries, taskHistoryRetryInterval, executionsPage, hasMoreExecutions, loadingMoreExecutions, searchType, searchQuery, tickerFilter, selectedExecutionForView, selectedFolderId, fileTypeFilter]);

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

  const tickerSearchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // ticker filter change: small delay ensures the ref has been updated with
  // the new loadTaskExecutionsFunc that captures the latest tickerFilter
  const isTickerMountRef = useRef(true);
  useEffect(() => {
    if (isTickerMountRef.current) { isTickerMountRef.current = false; return; }
    const t = setTimeout(() => {
      loadTaskExecutionsFuncRef.current(false, false, false);
    }, 50);
    return () => clearTimeout(t);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tickerFilter]);

  // ── Folder helpers ────────────────────────────────────────
  const loadFolders = useCallback(async () => {
    try {
      const data = await filesAPI.getFolders();
      setFolders(data);
    } catch (e) {
      console.error('Failed to load folders:', e);
    }
  }, []);

  const handleCreateFolder = useCallback(async () => {
    const name = newFolderName.trim();
    if (!name) return;
    try {
      await filesAPI.createFolder(name);
      setNewFolderName('');
      setCreatingFolder(false);
      await loadFolders();
    } catch (e: any) {
      message.error(e?.response?.data?.detail || 'Failed to create folder');
    }
  }, [newFolderName, loadFolders]);

  const handleRenameFolder = useCallback(async (folderId: number) => {
    const name = editingFolderName.trim();
    if (!name) return;
    try {
      await filesAPI.updateFolder(folderId, { name });
      setEditingFolderId(null);
      setEditingFolderName('');
      await loadFolders();
    } catch (e: any) {
      message.error(e?.response?.data?.detail || 'Failed to rename folder');
    }
  }, [editingFolderName, loadFolders]);

  const handleDeleteFolder = useCallback(async (folderId: number) => {
    try {
      await filesAPI.deleteFolder(folderId);
      if (selectedFolderId === String(folderId)) {
        setSelectedFolderId(undefined);
      }
      await loadFolders();
      loadTaskExecutionsFuncRef.current(false, false, false).catch(() => {});
    } catch (e: any) {
      message.error(e?.response?.data?.detail || 'Failed to delete folder');
    }
  }, [selectedFolderId, loadFolders]);

  const handleMoveToFolder = useCallback(async (executionId: string, folderId: number | null) => {
    try {
      await filesAPI.moveExecutionToFolder(Number(executionId), folderId);
      await loadFolders();
      loadTaskExecutionsFuncRef.current(false, false, false).catch(() => {});
      message.success('Moved successfully');
    } catch (e: unknown) {
      const err = e as { response?: unknown };
      if (!err?.response) {
        message.error(getErrorMessage(e, 'Failed to move file'));
      }
    }
  }, [loadFolders]);

  // 初始化加载：仅在组件挂载时执行一次
  useEffect(() => {
    loadTaskExecutionsFunc();
    loadDefaultConfig();
    loadAllTemplates();
    loadFolders();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // 空依赖数组，只在组件挂载时执行一次

  // 定时刷新执行历史列表：使用 ref 避免依赖项导致的重复执行
  const loadTaskExecutionsFuncRef = useRef(loadTaskExecutionsFunc);
  useEffect(() => {
    loadTaskExecutionsFuncRef.current = loadTaskExecutionsFunc;
  }, [loadTaskExecutionsFunc]);

  // 浮动菜单「My Notes / Thoughts」保存成功后刷新左侧列表（CreateTextNoteModal 派发事件）
  useEffect(() => {
    const onRefresh = (ev: Event) => {
      loadTaskExecutionsFuncRef.current(false, false, false).catch(() => {});
    };
    window.addEventListener(TASK_EXECUTIONS_REFRESH_EVENT, onRefresh);
    return () => window.removeEventListener(TASK_EXECUTIONS_REFRESH_EVENT, onRefresh);
  }, []);

  // Refresh executions when folder filter changes (server-side pagination must match filters)
  const isFolderMountRef = useRef(true);
  useEffect(() => {
    if (isFolderMountRef.current) { isFolderMountRef.current = false; return; }
    const t = setTimeout(() => {
      loadTaskExecutionsFuncRef.current(false, false, false).catch(() => {});
    }, 50);
    return () => clearTimeout(t);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedFolderId]);

  // Refresh executions when file_type filter changes
  const isFileTypeMountRef = useRef(true);
  useEffect(() => {
    if (isFileTypeMountRef.current) { isFileTypeMountRef.current = false; return; }
    const t = setTimeout(() => {
      loadTaskExecutionsFuncRef.current(false, false, false).catch(() => {});
    }, 50);
    return () => clearTimeout(t);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [fileTypeFilter]);

  /** 文件类型 chip 横条：与 FeedReader Programme 条一致，deltaY → scrollLeft；触控板横向手势走 deltaX 分支不拦截 */
  useEffect(() => {
    const el = fileTypeStripRef.current;
    if (!el) return;

    const onWheel = (e: WheelEvent) => {
      if (el.scrollWidth <= el.clientWidth + 1) return;

      if (!e.shiftKey && Math.abs(e.deltaX) > Math.abs(e.deltaY)) {
        return;
      }

      const scrollDelta = e.shiftKey ? e.deltaY + e.deltaX : e.deltaY;
      const maxScroll = el.scrollWidth - el.clientWidth;
      const prev = el.scrollLeft;
      const next = Math.max(0, Math.min(maxScroll, prev + scrollDelta));

      if (next !== prev) {
        el.scrollLeft = next;
        e.preventDefault();
      }
    };

    el.addEventListener('wheel', onWheel, { passive: false });
    return () => el.removeEventListener('wheel', onWheel);
  }, []);

  /** Watchlist 等外链：`/files?execution=<processing_stats_id>` 打开并选中对应来源任务，并重置筛选以便列表能显示该条 */
  useEffect(() => {
    if (!executionFromUrl) return;
    let cancelled = false;
    (async () => {
      try {
        const execution = await tasksAPI.getTaskExecution(executionFromUrl);
        if (cancelled) return;
        setSearchQuery('');
        setTickerFilter(undefined);
        setSearchType('file_name');
        setSelectedExecutionForView(execution);
        setRightPanelView('info');
        setSelectedFileForView(null);
        scrollDeepLinkTargetExecutionIdRef.current = execution.id;
        pendingScrollToExecutionFromDeepLinkRef.current = true;
        deepLinkScrollAppendAttemptsRef.current = 0;
        loadTaskExecutionsFuncRef.current(false, false, false).catch(() => {});
        setSearchParams(
          prev => {
            const next = new URLSearchParams(prev);
            next.delete('execution');
            return next;
          },
          { replace: true }
        );
      } catch (e) {
        console.error(e);
        message.error('无法打开链接对应的来源任务');
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [executionFromUrl, setSearchParams]);

  /** 外链进入后：左侧列表滚动到已选中的条目（必要时自动加载更多分页直至出现） */
  useEffect(() => {
    if (!pendingScrollToExecutionFromDeepLinkRef.current) return;
    const targetId = scrollDeepLinkTargetExecutionIdRef.current;
    if (!targetId || selectedExecutionForView?.id !== targetId) {
      pendingScrollToExecutionFromDeepLinkRef.current = false;
      scrollDeepLinkTargetExecutionIdRef.current = null;
      deepLinkScrollAppendAttemptsRef.current = 0;
      return;
    }
    const scrollEl = filesListScrollRef.current;
    if (!scrollEl) return;

    const tryScroll = (): boolean => {
      const row = scrollEl.querySelector(`[data-execution-id="${CSS.escape(targetId)}"]`);
      if (row) {
        row.scrollIntoView({ block: 'center', behavior: 'smooth' });
        return true;
      }
      return false;
    };

    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        if (tryScroll()) {
          pendingScrollToExecutionFromDeepLinkRef.current = false;
          scrollDeepLinkTargetExecutionIdRef.current = null;
          deepLinkScrollAppendAttemptsRef.current = 0;
          return;
        }
        if (hasMoreExecutions && !loadingMoreExecutions && deepLinkScrollAppendAttemptsRef.current < 25) {
          deepLinkScrollAppendAttemptsRef.current += 1;
          loadTaskExecutionsFuncRef.current(false, false, true).catch(() => {});
        } else {
          pendingScrollToExecutionFromDeepLinkRef.current = false;
          scrollDeepLinkTargetExecutionIdRef.current = null;
          deepLinkScrollAppendAttemptsRef.current = 0;
        }
      });
    });
  }, [
    selectedExecutionForView?.id,
    taskExecutions,
    loadingMoreExecutions,
    hasMoreExecutions,
  ]);

  // 注册 Source 列表刷新：Upload Files 弹窗点击 Start Processing 关闭后或任务完成时会触发
  useEffect(() => {
    const unregister = registerSourceListRefresh(() => {
      loadTaskExecutionsFuncRef.current(false, true, false).catch(() => {});
    });
    return unregister;
  }, [registerSourceListRefresh]);

  // 仅在有“进行中”任务时定期刷新执行历史，避免完成后无意义的轮询（含 details/file-tags）
  const hasExecutionInProgress = processing ||
    audioAnalyzingIds.size > 0 ||
    docAnalyzingIds.size > 0 ||
    taskExecutions.some((e) => ['processing', 'analyzing', 'initializing'].includes(e.rc_agent_status || ''));
  useEffect(() => {
    if (!hasExecutionInProgress) return;
    const refreshInterval = setInterval(() => {
      loadTaskExecutionsFuncRef.current(false, true).catch(() => {});
    }, 30000);
    return () => clearInterval(refreshInterval);
  }, [hasExecutionInProgress]);

  // 稳定的回调，避免 ExecutionDetailsPanel 因父组件重渲染而重复请求 details/file-tags
  const onExecutionRefresh = useCallback(() => {
    loadTaskExecutionsFuncRef.current(false, true, false).catch(() => {});
  }, []);

  // 行业和股票选择相关的useEffect已移除 - 改用自动打标签

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
          // 处理失败时，保持原样（继续显示reprocess和clear file list）
        }
        
        // 任务完成后刷新执行历史列表（重置为第一页）
        // 使用 ref 避免依赖项问题
        loadTaskExecutionsFuncRef.current(false, true, false).catch(() => {});
        
        // 延迟再次刷新：DB 写入与后台 auto_name_v1 可能晚于任务状态 completed
        const postAsyncDelays = [5000, 15000, 40000];
        postAsyncDelays.forEach((ms) => {
          setTimeout(() => {
            loadTaskExecutionsFuncRef.current(false, true, false).catch(() => {});
          }, ms);
        });
      }
    } catch (error) {
      console.error('failed to get task status:', error);
    } finally {
      setUpdatingStatus(false);
    }
  }, [currentTaskId, updateFileStatuses, updatingStatus, resetAllInputs]); // 移除 loadTaskExecutionsFunc 依赖

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

  const handleViewAiAnalysis = async (execution: TaskExecution) => {
    try {
      setAiAnalysisLoading(true);
      setAiAnalysisVisible(true);
      setAiAnalysisExecution(execution);
      setModalAudioAnalyzePicker({});
      
      
      const details = await tasksAPI.getTaskExecutionDetails(execution.id);
      console.log('AI Analysis Data:', details);
      console.log('Final Report:', details?.execution?.final_report);
      setAiAnalysisData(details);
      
    } catch (error) {
      message.error('Failed to load AI analysis report');
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
      
      
      // 等待一小段时间确保MarkdownRenderer完全渲染
      await new Promise(resolve => setTimeout(resolve, 500));
      
      await downloadElementAsPDF('ai-analysis-content', {
        filename,
        title: `AI Analysis Report - ${aiAnalysisData.execution.task_name}`,
        author: process.env.REACT_APP_APP_NAME || 'Reverse Capital',
        subject: 'AI Analysis Report'
      });
      
      message.success('PDF report downloaded successfully');
      
    } catch (error) {
      message.error('PDF download failed, please try again');
    } finally {
      setPdfDownloading(false);
    }
  };


  // 音频AI分析 - 触发分析
  const handleAnalyzeAudio = async (
    record: TaskExecution,
    templateVariables?: Record<string, any>,
    templateId?: number,
  ) => {
    const executionId = Number(record.id);
    const fileName = record.first_display_name || record.first_filename || record.task_name || 'Audio file';
    
    try {
      setAudioAnalyzingIds(prev => new Set(prev).add(record.id));
      message.loading({ content: 'Submitting audio analysis task...', key: 'audioAnalyze', duration: 0 });

      // 过滤空值的 template_variables
      const filteredVars = templateVariables
        ? Object.fromEntries(Object.entries(templateVariables).filter(([_, v]) => v !== undefined && v !== ''))
        : undefined;

      const result = await filesAPI.analyzeAudio({
        execution_id: executionId,
        ...(templateId != null ? { template_id: templateId } : {}),
        ...(filteredVars && Object.keys(filteredVars).length > 0 ? { template_variables: filteredVars } : {}),
      });

      if (result.success && result.task_id) {
        message.loading({ content: 'AI analyzing audio...', key: 'audioAnalyze', duration: 0 });

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

  // 文档AI分析 - 触发分析（需要指定模板ID，可选 template_variables）
  const handleAnalyzeDocument = async (record: TaskExecution, templateId: number, templateVariables?: Record<string, any>) => {
    const executionId = Number(record.id);
    const fileName = record.first_display_name || record.first_filename || record.task_name || 'Document';
    
    try {
      setDocAnalyzingIds(prev => new Set(prev).add(record.id));
      message.loading({ content: 'Submitting document analysis task...', key: 'docAnalyze', duration: 0 });

      const result = await filesAPI.analyzeDocument({
        execution_id: executionId,
        template_id: templateId,
        template_variables: templateVariables ?? {},
      });

      if (result.success && result.task_id) {
        message.destroy('docAnalyze'); // 关闭 "Submitting..."，进度由 NavBar Processing Tasks 展示

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
  };

  const handleViewOriginalFile = async (fileData: any) => {
    try {
      const originalFileInfo = await filesAPI.getOriginalFileInfo(fileData.id);
      
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
      } else {
        const errorMsg = getErrorMessage(error, 'Failed to load original file');
        message.error(errorMsg);
      }
    }
  };

  const handleShowOriginalFiles = async (execution: any) => {
    try {
      
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
  };

  const handleOpenShareModal = async (execution: TaskExecution) => {
    setShareTargetExecution(execution);
    setSelectedShareUserIds([]);
    setShareModalVisible(true);
    try {
      const [users, sharesData] = await Promise.all([
        filesAPI.getShareableUsers(),
        (async () => {
          const detailResp = await tasksAPI.getTaskExecutionDetails(execution.id);
          const fileId = detailResp?.files?.[0]?.id;
          if (fileId) return filesAPI.getFileShares(fileId);
          return { file_id: 0, shares: [] };
        })()
      ]);
      setShareableUsers(users);
      setCurrentShares(sharesData.shares as any);
    } catch (err) {
      console.error('Failed to load share data:', err);
      setShareableUsers([]);
      setCurrentShares([]);
    }
  };

  const handleConfirmShare = async () => {
    if (!shareTargetExecution) return;
    if (selectedShareUserIds.length === 0) {
      setShareModalVisible(false);
      loadTaskExecutionsFuncRef.current(false, true, false).catch(() => {});
      return;
    }
    setSharingInProgress(true);
    try {
      const detailResp = await tasksAPI.getTaskExecutionDetails(shareTargetExecution.id);
      const fileId = detailResp?.files?.[0]?.id;
      if (!fileId) {
        message.error('No file found for this execution');
        return;
      }
      const result = await filesAPI.shareFile(fileId, selectedShareUserIds);
      message.success(`Shared to ${result.shared_count} user(s)`);
      if (result.skipped?.length > 0) {
        result.skipped.forEach(s => message.warning(`Skipped user ${s.user_id}: ${s.reason}`));
      }
      setShareModalVisible(false);
      loadTaskExecutionsFuncRef.current(false, true, false).catch(() => {});
    } catch (err) {
      message.error('Failed to share file');
      console.error(err);
    } finally {
      setSharingInProgress(false);
    }
  };

  const handleRemoveShare = async (fileId: number, userId: number) => {
    try {
      await filesAPI.unshareFile(fileId, [userId]);
      setCurrentShares(prev => prev.filter(s => s.shared_to_user_id !== userId));
      message.success('Share removed');
      loadTaskExecutionsFuncRef.current(false, true, false).catch(() => {});
    } catch (err) {
      message.error('Failed to remove share');
    }
  };

  const handleOpenRename = (execution: TaskExecution) => {
    setExecutionToRename(execution);
    const raw = execution.first_filename || `${execution.files_processed} file(s)`;
    const { stem, extension } = filesAPI.splitFilenameStemAndExtension(raw);
    setRenameStem(stem);
    setRenameExtension(extension);
    setRenameModalVisible(true);
  };

  const handleConfirmRename = async () => {
    if (!executionToRename || !renameStem.trim()) return;
    const newFilename = `${renameStem.trim()}${renameExtension}`;
    try {
      setRenameLoading(true);
      await tasksAPI.renameExecutionFile(executionToRename.id, newFilename);
      setTaskExecutions(prev =>
        prev.map(exec =>
          exec.id === executionToRename.id
            ? { ...exec, first_filename: newFilename, first_display_name: null }
            : exec
        )
      );
      if (selectedExecutionForView?.id === executionToRename.id) {
        setSelectedExecutionForView(prev =>
          prev ? { ...prev, first_filename: newFilename, first_display_name: null } : null
        );
      }
      message.success('Renamed successfully');
      setRenameModalVisible(false);
      setExecutionToRename(null);
      setRenameStem('');
      setRenameExtension('');
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
    
    modal.confirm({
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
      centered: true,
      styles: getModalStyles(themeMode),
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
          setBatchMode(false); // 恢复 batch 为原始状态，隐藏 All 和 Done
          if (selectedExecutionForView && idsToDelete.includes(selectedExecutionForView.id)) {
            setSelectedExecutionForView(null);
          }

          if (failed.length > 0) {
            message.warning(`Deleted ${succeeded.length} of ${ids.length} records. ${failed.length} failed.`);
          } else {
            message.success(`Successfully deleted ${idsToDelete.length} execution record(s)`);
          }
          
        } catch (error: any) {
          console.error('failed to batch delete execution records:', error);
          const errorMessage = getErrorMessage(error, 'Failed to batch delete execution records');
          message.error(errorMessage);
          
          // API失败时不更新本地状态，保持选中状态让用户可以重试
        } finally {
          setDeletingExecutions(false);
        }
      }
    });
  };

  const _refreshFileStatus = async () => {
    if (refreshingStatus) return;
    
    setRefreshingStatus(true);
    try {
      if (lastCompletedTaskId) {
        await updateFileStatuses(lastCompletedTaskId);
        message.success('file status refreshed');
      } else {
        message.info('no tasks to refresh');
      }
    } catch (error) {
      console.error('failed to refresh file status:', error);
      message.error('failed to refresh file status, please try again');
    } finally {
      setRefreshingStatus(false);
    }
  };

  // 单文件上传状态
  const [, setUploadingFiles] = useState(false);

  const _handleUpload = async (file: any) => {
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
      
      // 文件上传成功，自动打标签会在文件处理完成后自动触发
      message.success('files uploaded successfully');
      
    } catch (error: any) {
      console.error('failed to upload file:', error);
      const errorMessage = getErrorMessage(error, 'failed to upload file');
      // 避免与 api 拦截器的 toast 重复：拦截器已对带 response 的请求展示过错误
      if (!error.response) message.error(errorMessage);
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
    } catch (error: any) {
      console.error('Failed to delete file:', error);
      const errorMessage = getErrorMessage(error, 'Failed to delete file');
      message.error(errorMessage);
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
      }
      
      return newFiles;
    });
  };

  const _handleStartProcessing = async () => {
    if (!selectedTemplateId) {
      message.error('please select Prompt Template');
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
      
      
      // 将所有文件状态设置为ready
      setFiles(prev => prev.map(file => ({ ...file, status: 'ready' as const })));
      
      // 清空文件错误信息
      setFiles(prev => prev.map(file => ({ ...file, error_message: undefined })));
      
      const response = await filesAPI.processFiles({
        file_paths: files.map(f => f.path),
        task_type: 'local_llm_template',
        params: {
          ...processingParams,
          use_ai_agent: true,
          analysis_mode: 'local_llm',
        },
        analysis_mode: 'local_llm',
        template_id: selectedTemplateId,
        template_code: undefined, // 可选：如果需要使用template_code
        template_variables: templateVariables,
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
    }
  };

  const _handleStopProcessing = async () => {
    if (!currentTaskId || stoppingTask) return;
    
    setStoppingTask(true);
    try {
      await filesAPI.stopTask(currentTaskId);
      message.info('stop task request sent');
      
      // 停止任务后，等待一段时间再刷新执行历史列表（重置为第一页）
      setTimeout(() => {
        loadTaskExecutionsFuncRef.current(false, true, false).catch(() => {});
      }, 3000); // 等待3秒后刷新，确保任务状态已更新
      
    } catch (error: any) {
      console.error('failed to stop task:', error);
      const errorMessage = getErrorMessage(error, 'failed to stop task');
      message.error(errorMessage);
    } finally {
      setStoppingTask(false);
    }
  };

  // 新增：重新处理函数
  const _handleReprocess = () => {
    setShowResult(false);
    setProcessingResult(null);
    setFiles(prev => prev.map(file => ({ ...file, status: 'ready' as const })));
    
          // 重新处理时也刷新执行历史列表
      setTimeout(() => {
        loadTaskExecutionsFuncRef.current(true, false); // 手动刷新，显示错误
      }, 1000); // 延迟1秒后刷新，确保状态已更新
  };

  // 新增：清除文件列表函数
  const _handleClearFiles = () => {
    setFiles([]);
    setShowResult(false);
    setProcessingResult(null);
    setLastCompletedTaskId(null);
    
          // 清除文件列表时也刷新执行历史列表
      setTimeout(() => {
        loadTaskExecutionsFuncRef.current(true, false); // 手动刷新，显示错误
      }, 1000); // 延迟1秒后刷新，确保状态已更新
  };

  // 手动标签相关函数已移除 - 改用自动打标签（content_tag_v1）

  // 获取当前界面状态
  const _getUIState = () => {
    if (processing) return 'processing';
    if (showResult && processingResult) return 'completed';
    if (files.length === 0) return 'empty';
    return 'ready';
  };

  const _renderProcessingParams = () => (
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
      
      <Divider />
      
      <Row gutter={16}>
        <Col span={12}>
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
        <Col span={12}>
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
    </Form>
  );

  const _getStatusText = (status: string) => {
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



  const _fileColumns = [
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

  const _executionColumns = [
    {
      title: 'File Name',
      key: 'file_name',
      width: 350,
      ellipsis: true,
      render: (_: any, record: TaskExecution) => {
        const isShared = record.access_type === 'shared';
        const displayName = record.first_display_name || record.first_filename;
        if (displayName) {
          return (
            <Tooltip title={`${displayName}${isShared && record.shared_by ? ` (shared by ${record.shared_by})` : ''}`} placement="topLeft">
              <div style={{ 
                color: '#1890ff', 
                cursor: 'pointer',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
                maxWidth: '400px',
                fontSize: '13px',
                display: 'flex',
                alignItems: 'center',
                gap: 4
              }}>
                <FileTextOutlined style={{ marginRight: 6, fontSize: '14px', flexShrink: 0 }} />
                <span style={{ overflow: 'hidden', textOverflow: 'ellipsis' }}>{displayName}</span>
                {isShared && <Tag color="purple" style={{ marginLeft: 4, fontSize: 10, lineHeight: '16px', padding: '0 4px', flexShrink: 0 }}>Shared</Tag>}
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
        const isAudioFile = AUDIO_LIKE_TYPES.includes(record.first_file_type ?? '');
        const isYouTubeFile = record.first_file_type === 'youtube';
        const isAudioAnalyzing = record.rc_agent_status === 'analyzing' || audioAnalyzingIds.has(record.id);
        const isDocAnalyzing = record.rc_agent_status === 'analyzing' || docAnalyzingIds.has(record.id);
        const _isAnalyzing = isAudioAnalyzing || isDocAnalyzing;
        const hasAnalysis = record.rc_agent_status === 'success';
        // 音频文件且ASR已完成但未进行AI分析（YouTube 不需要 AI 分析）；旧 skipped 视为 completed
        const canAnalyzeAudio =
          isAudioFile &&
          !hasAnalysis &&
          !isAudioAnalyzing &&
          normalizeRcAgentStatusForAnalyze(record.rc_agent_status) === 'completed';
        // 文档分析：需要区分"仅上传"和"正在处理"
        // 1. 如果first_task_id为null且rc_agent_status为uploaded，说明仅上传未处理，允许分析
        // 2. 如果rc_agent_status为failed，允许重新分析
        // 3. 如果有task_id或状态为processing/initializing等，说明正在处理，禁止重复分析
        const isProcessingDoc = record.rc_agent_status === 'processing' || 
                                record.rc_agent_status === 'initializing' ||
                                (record.first_task_id != null && record.rc_agent_status === 'uploaded');
        const rcForAnalyze = normalizeRcAgentStatusForAnalyze(record.rc_agent_status);
        // 允许分析的状态：uploaded、failed、completed（含旧数据 skipped，已映射为 completed）
        const _canAnalyzeDoc = !isAudioFile && !hasAnalysis && !isDocAnalyzing && !isProcessingDoc && (
          rcForAnalyze === 'uploaded' ||
          rcForAnalyze === 'failed' ||
          rcForAnalyze === 'completed'
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
          <Tooltip title="Original Files">
            <Button
              type="text"
              icon={<FileTextOutlined />}
              onClick={() => handleShowOriginalFiles(record)}
              size="small"
            />
          </Tooltip>
          <Tooltip title="Share">
            <Button
              type="text"
              icon={<ShareAltOutlined />}
              onClick={() => handleOpenShareModal(record)}
              size="small"
              style={{ color: '#722ed1' }}
            />
          </Tooltip>
          {record.access_type !== 'shared' && (
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
          )}
        </Space>
        );
      }
    }
  ];

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
          setModalAudioAnalyzePicker({});
        }}
        styles={getModalStyles(themeMode)}
        footer={[
          <Button key="cancel" onClick={() => {
            setAiAnalysisVisible(false);
            setAiAnalysisData(null);
            setModalAudioAnalyzePicker({});
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
        const isAudioModal = AUDIO_LIKE_TYPES.includes(aiAnalysisExecution?.first_file_type ?? '');
        
        // 音频分析：只有转写完成(completed，含旧 skipped)或失败(failed)时才允许AI分析
        const stModal = aiAnalysisExecution?.rc_agent_status;
        const canAnalyzeAudioModal =
          isAudioModal &&
          (normalizeRcAgentStatusForAnalyze(stModal) === 'completed' || stModal === 'failed');
        
        // 文档分析：需要区分"仅上传"和"正在处理"
        // 1. 如果first_task_id为null且rc_agent_status为uploaded，说明仅上传未处理，允许分析
        // 2. 如果rc_agent_status为failed，允许重新分析
        // 3. 如果有task_id或状态为processing/initializing等，说明正在处理，禁止重复分析
        const isProcessingDocModal = aiAnalysisExecution?.rc_agent_status === 'processing' || 
                                      aiAnalysisExecution?.rc_agent_status === 'initializing' ||
                                      (aiAnalysisExecution?.first_task_id != null && aiAnalysisExecution?.rc_agent_status === 'uploaded');
        const rcModal = normalizeRcAgentStatusForAnalyze(aiAnalysisExecution?.rc_agent_status);
        const canAnalyzeDocModal = !isAudioModal && aiAnalysisExecution && !isProcessingDocModal && (
          rcModal === 'uploaded' ||
          rcModal === 'failed' ||
          rcModal === 'completed'
        );
        
        // 同时识别两种 analyzing 来源:
        // 1) 用户在右侧面板手动 Re-analyze 时, FilesPage 把 id 加入 *AnalyzingIds
        // 2) 自动并行链路 (trigger_auto_analyze) 写入 rc_agent_status='analyzing'
        const rcAnalyzingModal = aiAnalysisExecution?.rc_agent_status === 'analyzing';
        const isAudioAnalyzingModal = aiAnalysisExecution
          ? audioAnalyzingIds.has(aiAnalysisExecution.id) || (isAudioModal && rcAnalyzingModal)
          : false;
        const isDocAnalyzingModal = aiAnalysisExecution
          ? docAnalyzingIds.has(aiAnalysisExecution.id) || (!isAudioModal && rcAnalyzingModal)
          : false;
        const isAnalyzingModal = isAudioAnalyzingModal || isDocAnalyzingModal;

        const modalAudioTemplateModule =
          aiAnalysisExecution?.first_file_type === 'podcast' ? 'youtube_analysis' : 'audio_analysis';

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
              <div style={{
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                justifyContent: 'center',
                padding: '40px',
                textAlign: 'center',
              }}>
                <ExclamationCircleOutlined style={{ fontSize: '48px', color: '#faad14', marginBottom: '16px' }} />
                <div style={{ marginBottom: '20px', fontSize: '16px' }}>
                  No AI analysis report available for this execution.
                </div>
                {canAnalyzeAudioModal && aiAnalysisExecution && (
                  <div style={{ maxWidth: '480px', textAlign: 'center' }}>
                    <div style={{ marginBottom: '14px', color: '#999', fontSize: '13px', textAlign: 'left' }}>
                      {modalAudioTemplateModule === 'youtube_analysis'
                        ? 'Apple Podcast episodes use the same deep-analysis template as Upload YouTube Link (youtube_analysis). Use Edit my template to adjust prompts, set variables when needed, then click Analyze Audio.'
                        : 'Choose your private audio analysis template (optional). Use Edit my template to adjust prompts, set variables when needed, then click Analyze Audio.'}
                    </div>
                    <UploadAnalysisTemplatePicker
                      value={modalAudioAnalyzePicker}
                      onChange={setModalAudioAnalyzePicker}
                      mode="audio-default"
                      moduleCode={modalAudioTemplateModule}
                      label={
                        modalAudioTemplateModule === 'youtube_analysis'
                          ? 'YouTube analysis template'
                          : 'Audio analysis template'
                      }
                      hint={
                        modalAudioTemplateModule === 'youtube_analysis'
                          ? 'When the switch is on, your private youtube_analysis template is used (same module as Upload YouTube Link). Toggle off for the system default.'
                          : 'When the switch is on, your private audio template is used. Toggle off to use the system default.'
                      }
                      showVariablesEditor
                    />
                    <Button
                      type="primary"
                      icon={<SoundOutlined />}
                      onClick={() =>
                        handleAnalyzeAudio(
                          aiAnalysisExecution,
                          modalAudioAnalyzePicker.templateVariables || {},
                          modalAudioAnalyzePicker.enabled !== false ? modalAudioAnalyzePicker.templateId : undefined,
                        )
                      }
                      style={{
                        height: '40px',
                        paddingInline: '24px',
                        fontSize: '14px',
                        borderRadius: '8px',
                        marginTop: 16,
                      }}
                    >
                      Analyze Audio
                    </Button>
                  </div>
                )}
                {canAnalyzeDocModal && aiAnalysisExecution && (
                  <div style={{ maxWidth: '460px', marginTop: '20px', textAlign: 'left' }}>
                    <div style={{ marginBottom: '12px', color: '#999', fontSize: '13px' }}>
                      {aiAnalysisExecution.rc_agent_status === 'failed'
                        ? 'The previous AI analysis failed. Turn on your private template below, adjust variables if needed, then run analysis again.'
                        : 'Choose your private document analysis template below: enable the switch, set any template variables, then click Analyze Document.'}
                    </div>
                    <UploadAnalysisTemplatePicker
                      value={modalAnalyzeDocPicker}
                      onChange={setModalAnalyzeDocPicker}
                      mode="optional"
                      moduleCode="file_analysis"
                      label="Document analysis template"
                      hint="When the switch is on, your private template is used. Fill in variables if your template defines them, then click Analyze Document."
                      showVariablesEditor
                    />
                    <div style={{ marginTop: '16px', textAlign: 'center' }}>
                      <Button
                        type="primary"
                        icon={<RobotOutlined />}
                        disabled={!modalAnalyzeDocPicker.templateId || modalAnalyzeDocPicker.enabled === false}
                        onClick={() => {
                          if (modalAnalyzeDocPicker.templateId != null) {
                            handleAnalyzeDocument(aiAnalysisExecution, modalAnalyzeDocPicker.templateId, modalAnalyzeDocPicker.templateVariables || {});
                          }
                        }}
                        style={{
                          height: '40px',
                          paddingInline: '24px',
                          fontSize: '14px',
                          borderRadius: '8px',
                        }}
                      >
                        Analyze Document
                      </Button>
                    </div>
                  </div>
                )}
              </div>
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
      styles={getModalStyles(themeMode)}
    >
      {selectedFileContent ? (
        <div>
          <div style={{ marginBottom: 16, padding: '12px', backgroundColor: themeColors.codeBlockBg, borderRadius: 8 }}>
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
            border: `1px solid ${themeColors.cardBorder}`,
            borderRadius: 8,
            padding: '16px',
            backgroundColor: themeColors.codeBlockBg
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
                    <div style={{
                      display: 'flex',
                      flexDirection: 'column',
                      alignItems: 'center',
                      justifyContent: 'center',
                      padding: '40px',
                      textAlign: 'center',
                    }}>
                      <FileTextOutlined style={{ fontSize: '48px', color: '#1890ff', marginBottom: '16px' }} />
                      <div style={{ marginBottom: '20px', fontSize: '16px', color: '#666' }}>
                        This file type cannot be previewed online.
                      </div>
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
                        style={{
                          height: '40px',
                          paddingInline: '24px',
                          fontSize: '14px',
                          borderRadius: '8px',
                        }}
                      >
                        Download File
                      </Button>
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
                color: themeColors.text
              }}>
                {selectedFileContent.content}
              </pre>
            ) : (
              <div style={{
                display: 'flex',
                flexDirection: 'column',
                alignItems: 'center',
                justifyContent: 'center',
                padding: '40px',
                textAlign: 'center',
              }}>
                <ExclamationCircleOutlined style={{ fontSize: '48px', color: '#faad14', marginBottom: '16px' }} />
                <div style={{ fontSize: '16px', color: '#666' }}>
                  No content available for this file
                </div>
              </div>
            )}
          </div>
        </div>
      ) : (
        <div style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          padding: '40px',
          textAlign: 'center',
        }}>
          <ExclamationCircleOutlined style={{ fontSize: '48px', color: '#faad14', marginBottom: '16px' }} />
          <div style={{ fontSize: '16px' }}>
            No file data available
          </div>
        </div>
      )}
    </Modal>
  );

  const filteredExecutions = searchType === 'file_name' && searchQuery.trim()
    ? taskExecutions.filter(exec => (exec.first_display_name || exec.first_filename || '').toLowerCase().includes(searchQuery.trim().toLowerCase()))
    : taskExecutions;

  const hasActiveSourcesSearch =
    (searchType !== 'ticker' && searchQuery.trim().length > 0) ||
    (searchType === 'ticker' && Boolean(tickerFilter));

  const hasAnySearchPanelFilter = hasActiveSourcesSearch;

  const clearSourcesSearchFilters = () => {
    setSearchQuery('');
    setTickerFilter(undefined);
    setTickerSearchOptions([]);
  };

  const folderFilterSummary = useMemo(() => {
    if (!selectedFolderId) return 'All';
    if (selectedFolderId === 'uncategorized') return 'Uncategorized';
    const f = folders.find(x => String(x.id) === selectedFolderId);
    return f ? f.name : selectedFolderId;
  }, [selectedFolderId, folders]);

  /** 与移动端 Sources 卡片一致：彩色类型图标 + 文案状态（非 Tag） */
  const getFileTypeIconDisplay = (firstFileType: string | undefined): {
    tooltip: string;
    icon: React.ReactElement;
    color: string;
  } => {
    const t = (firstFileType || '').toLowerCase();
    if (['pdf'].includes(t) || t.includes('pdf')) {
      return { tooltip: 'PDF', icon: <FilePdfOutlined />, color: '#f5222d' };
    }
    if (['doc', 'docx', 'word'].includes(t) || t.includes('word') || t.includes('msword')) {
      return { tooltip: 'Word', icon: <FileWordOutlined />, color: '#1890ff' };
    }
    if (['excel', 'xlsx', 'xls'].includes(t) || t.includes('excel') || t.includes('spreadsheet')) {
      return { tooltip: 'Excel', icon: <FileExcelOutlined />, color: '#52c41a' };
    }
    if (['audio'].includes(t) || t.startsWith('audio/')) {
      return { tooltip: 'Audio', icon: <SoundOutlined />, color: '#722ed1' };
    }
    if (['podcast'].includes(t)) {
      return { tooltip: 'Podcast', icon: <AppleOutlined />, color: '#7224d8' };
    }
    if (['youtube'].includes(t)) {
      return { tooltip: 'YouTube', icon: <VideoCameraOutlined />, color: '#ff4d4f' };
    }
    if (['image', 'jpg', 'jpeg', 'png', 'gif', 'bmp'].includes(t) || t.startsWith('image/')) {
      return { tooltip: 'Image', icon: <FileImageOutlined />, color: '#fa8c16' };
    }
    if (['web'].includes(t)) {
      return { tooltip: 'Web Clip', icon: <GlobalOutlined />, color: '#13c2c2' };
    }
    if (['text'].includes(t)) {
      return { tooltip: 'Text Note', icon: <EditOutlined />, color: '#2f54eb' };
    }
    const label = (firstFileType || 'File').toUpperCase();
    return { tooltip: label, icon: <FileTextOutlined />, color: '#8c8c8c' };
  };

  return (
      <div className="files-page-layout" style={{ backgroundColor: themeColors.pageBg }}>
        <div className="files-page-inner">
          {/* 左侧：Behance 风格文件列表 */}
          <div className="files-list-panel">
            <div className="files-list-header">
              <Space size={8} className="files-list-header-lead">
                <FileTextOutlined style={{ fontSize: 14, opacity: 0.85 }} />
                <span>Sources</span>
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
            {/* Folders：折叠块 + 内部滚动；file type + search 在 Search 面板 */}
            <div
              className={`files-folder-panel${isDark ? ' files-folder-panel--dark' : ''}`}
              style={{ borderBottom: `1px solid ${themeColors.cardBorder}` }}
            >
              <button
                type="button"
                className="files-folder-panel-header"
                onClick={() => setFolderPanelExpanded(v => !v)}
                aria-expanded={folderPanelExpanded}
                style={{ color: themeColors.text }}
              >
                <FolderOutlined style={{ fontSize: 14, opacity: 0.85 }} />
                <span className="files-folder-panel-title">Folders</span>
                <span
                  className="files-folder-panel-summary"
                  style={{ color: themeColors.textSecondary }}
                  title={folderFilterSummary}
                >
                  {folderFilterSummary}
                </span>
                {folderPanelExpanded ? (
                  <UpOutlined style={{ fontSize: 10, opacity: 0.65, marginLeft: 'auto' }} />
                ) : (
                  <DownOutlined style={{ fontSize: 10, opacity: 0.65, marginLeft: 'auto' }} />
                )}
              </button>
              {folderPanelExpanded && (
                <div className="files-folder-panel-body">
                  <div className="files-folder-strip-scroll">
                    <button
                      type="button"
                      className={`files-folder-chip${!selectedFolderId ? ' is-active' : ''}`}
                      onClick={() => setSelectedFolderId(undefined)}
                      style={{
                        background: !selectedFolderId ? (isDark ? 'rgba(33,150,243,0.18)' : 'rgba(24,144,255,0.08)') : 'transparent',
                        color: !selectedFolderId ? (isDark ? '#64B5F6' : '#1890ff') : themeColors.textSecondary,
                        border: !selectedFolderId ? `1px solid ${isDark ? 'rgba(33,150,243,0.35)' : 'rgba(24,144,255,0.25)'}` : '1px solid transparent',
                      }}
                    >
                      <AppstoreOutlined style={{ fontSize: 12, marginRight: 4 }} />All
                    </button>
                    <button
                      type="button"
                      className={`files-folder-chip${selectedFolderId === 'uncategorized' ? ' is-active' : ''}`}
                      onClick={() => setSelectedFolderId(selectedFolderId === 'uncategorized' ? undefined : 'uncategorized')}
                      style={{
                        background: selectedFolderId === 'uncategorized' ? (isDark ? 'rgba(33,150,243,0.18)' : 'rgba(24,144,255,0.08)') : 'transparent',
                        color: selectedFolderId === 'uncategorized' ? (isDark ? '#64B5F6' : '#1890ff') : themeColors.textSecondary,
                        border: selectedFolderId === 'uncategorized' ? `1px solid ${isDark ? 'rgba(33,150,243,0.35)' : 'rgba(24,144,255,0.25)'}` : '1px solid transparent',
                      }}
                    >
                      <InboxOutlined style={{ fontSize: 12, marginRight: 4 }} />Uncategorized
                    </button>
                    {folders.map(folder => {
                      const isActive = selectedFolderId === String(folder.id);
                      return (
                        <Dropdown
                          key={folder.id}
                          trigger={['contextMenu']}
                          menu={{
                            items: [
                              { key: 'rename', label: 'Rename', icon: <EditOutlined /> },
                              { key: 'delete', label: 'Delete', icon: <DeleteOutlined />, danger: true },
                            ],
                            onClick: ({ key }) => {
                              if (key === 'rename') {
                                setEditingFolderId(folder.id);
                                setEditingFolderName(folder.name);
                              } else if (key === 'delete') {
                                Modal.confirm({
                                  title: 'Delete folder?',
                                  content: `Files in "${folder.name}" will be moved to Uncategorized.`,
                                  okText: 'Delete',
                                  okButtonProps: { danger: true },
                                  onOk: () => handleDeleteFolder(folder.id),
                                });
                              }
                            },
                          }}
                        >
                          <button
                            type="button"
                            className={`files-folder-chip${isActive ? ' is-active' : ''}`}
                            onClick={() => setSelectedFolderId(isActive ? undefined : String(folder.id))}
                            style={{
                              background: isActive ? (isDark ? 'rgba(33,150,243,0.18)' : 'rgba(24,144,255,0.08)') : 'transparent',
                              color: isActive ? (isDark ? '#64B5F6' : '#1890ff') : themeColors.textSecondary,
                              border: isActive ? `1px solid ${isDark ? 'rgba(33,150,243,0.35)' : 'rgba(24,144,255,0.25)'}` : '1px solid transparent',
                            }}
                          >
                            {editingFolderId === folder.id ? (
                              <Input
                                size="small"
                                value={editingFolderName}
                                onChange={e => setEditingFolderName(e.target.value)}
                                onPressEnter={() => handleRenameFolder(folder.id)}
                                onBlur={() => handleRenameFolder(folder.id)}
                                autoFocus
                                style={{ width: 80, height: 20, fontSize: 12, padding: '0 4px' }}
                                onClick={e => e.stopPropagation()}
                              />
                            ) : (
                              <>
                                <FolderOutlined style={{ fontSize: 12, marginRight: 4 }} />
                                {folder.name}
                                <span style={{ marginLeft: 4, opacity: 0.5, fontSize: 11 }}>({folder.file_count})</span>
                              </>
                            )}
                          </button>
                        </Dropdown>
                      );
                    })}
                    {creatingFolder ? (
                      <Input
                        size="small"
                        placeholder="Folder name"
                        value={newFolderName}
                        onChange={e => setNewFolderName(e.target.value)}
                        onPressEnter={handleCreateFolder}
                        onBlur={() => { if (!newFolderName.trim()) setCreatingFolder(false); else handleCreateFolder(); }}
                        autoFocus
                        style={{ width: 100, height: 24, fontSize: 12, borderRadius: 12 }}
                      />
                    ) : (
                      <button
                        type="button"
                        className="files-folder-chip files-folder-chip-add"
                        onClick={() => setCreatingFolder(true)}
                        style={{ color: themeColors.textSecondary }}
                      >
                        <PlusOutlined style={{ fontSize: 11 }} />
                      </button>
                    )}
                  </div>
                </div>
              )}
            </div>
            <div
              className={`files-folder-panel${isDark ? ' files-folder-panel--dark' : ''}`}
              style={{ borderBottom: `1px solid ${themeColors.cardBorder}` }}
            >
              <div className="files-search-panel-header-row">
                <button
                  type="button"
                  className="files-folder-panel-header"
                  onClick={() => setSearchPanelExpanded(v => !v)}
                  aria-expanded={searchPanelExpanded}
                  style={{ color: themeColors.text }}
                >
                  <SearchOutlined style={{ fontSize: 14, opacity: 0.85 }} />
                  <span className="files-folder-panel-title">Search</span>
                  {hasAnySearchPanelFilter && (
                    <span className="files-search-active-dot" title="Filters active" />
                  )}
                  {!searchPanelExpanded && hasAnySearchPanelFilter && (
                    <span
                      className="files-folder-panel-summary files-search-filter-preview"
                      style={{ color: themeColors.textSecondary }}
                      title={searchType === 'ticker' ? `Ticker: ${tickerFilter}` : `${searchType === 'file_name' ? 'Title' : 'Content'}: ${searchQuery.trim()}`}
                    >
                      {searchType === 'file_name' && 'Title: '}
                      {searchType === 'content' && 'Content: '}
                      {searchType === 'ticker' && 'Ticker: '}
                      <span className="files-search-filter-preview-value">
                        {searchType === 'ticker' ? tickerFilter : (searchQuery.trim() || '…')}
                      </span>
                    </span>
                  )}
                  {searchPanelExpanded ? (
                    <UpOutlined style={{ fontSize: 10, opacity: 0.65, marginLeft: 'auto' }} />
                  ) : (
                    <DownOutlined style={{ fontSize: 10, opacity: 0.65, marginLeft: 'auto' }} />
                  )}
                </button>
                {hasAnySearchPanelFilter && (
                  <Button
                    type="link"
                    size="small"
                    className="files-search-clear-link"
                    onClick={(e) => {
                      e.stopPropagation();
                      clearSourcesSearchFilters();
                    }}
                  >
                    Clear
                  </Button>
                )}
              </div>
              {searchPanelExpanded && (
            <div className="files-search-panel-body">
            <div
              className={`files-list-search-bar files-list-search-bar--expandable${isDark ? ' files-list-search-bar--dark' : ''}`}
              style={
                {
                  '--files-sources-search-border': themeColors.cardBorder,
                  '--files-sources-search-bg': themeColors.cardBg,
                } as React.CSSProperties
              }
            >
              <div className="files-list-search-stack">
                <div className="files-list-search-mode-row">
                  <Segmented
                    block
                    size="small"
                    className="files-list-search-segmented files-list-search-segmented-three"
                    value={searchType}
                    onChange={(v) => {
                      const next = v as 'file_name' | 'content' | 'ticker';
                      setSearchType(next);
                      if (next === 'ticker') {
                        setSearchQuery('');
                      } else {
                        setTickerFilter(undefined);
                        setTickerSearchOptions([]);
                      }
                    }}
                    options={[
                      { label: 'Title', value: 'file_name' },
                      { label: 'Content', value: 'content' },
                      { label: 'Ticker', value: 'ticker' },
                    ]}
                  />
                </div>
                <div className="files-list-search-field-row">
                  {searchType === 'ticker' ? (
                    <AutoComplete
                      size="small"
                      placeholder="Search ticker..."
                      options={tickerSearchOptions}
                      value={tickerFilter}
                      onSearch={(text) => {
                        if (tickerSearchTimerRef.current) clearTimeout(tickerSearchTimerRef.current);
                        if (!text.trim()) {
                          setTickerSearchOptions([]);
                          return;
                        }
                        tickerSearchTimerRef.current = setTimeout(() => {
                          fileTagsService
                            .getDistinctTickers(text.trim())
                            .then((results) => {
                              setTickerSearchOptions(
                                results.map((r) => ({
                                  value: r.ticker,
                                  label: `${r.ticker} (${r.file_count})`,
                                })),
                              );
                            })
                            .catch(() => {});
                        }, 300);
                      }}
                      onSelect={(val: string) => setTickerFilter(val)}
                      onChange={(val: string) => {
                        if (!val) {
                          setTickerFilter(undefined);
                          setTickerSearchOptions([]);
                        }
                      }}
                      allowClear
                      className="files-list-search-input files-list-search-ticker"
                      classNames={{ popup: { root: 'files-list-ticker-autocomplete-popup' } }}
                    />
                  ) : (
                    <Input
                      size="small"
                      placeholder={searchType === 'file_name' ? 'Search by title' : 'Search in content'}
                      value={searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                      allowClear
                      className="files-list-search-input"
                      prefix={<SearchOutlined style={{ color: themeColors.textSecondary }} />}
                    />
                  )}
                </div>
              </div>
            </div>
            </div>
              )}
            </div>
            {/* File-type filter chips：单行横滚；与 FeedReader 节目条相同 wheel 逻辑（竖滚轮映射横向） */}
            <div
              ref={fileTypeStripRef}
              className="files-file-type-strip-scroll"
              style={{
                padding: '4px 8px',
                borderBottom: `1px solid ${themeColors.cardBorder}`,
              }}
            >
              {[
                { label: 'All', value: '' },
                { label: 'Document', value: 'document' },
                { label: 'Audio', value: 'audio' },
                { label: 'YouTube', value: 'youtube' },
                { label: 'Podcast', value: 'podcast' },
                { label: 'Web', value: 'web' },
                { label: 'Note', value: 'text' },
              ].map(({ label, value }) => {
                const isActive = fileTypeFilter === value;
                return (
                  <button
                    key={value || 'all'}
                    type="button"
                    className={`files-folder-chip${isActive ? ' is-active' : ''}`}
                    onClick={() => setFileTypeFilter(isActive ? '' : value)}
                    style={{
                      background: isActive ? (isDark ? 'rgba(33,150,243,0.18)' : 'rgba(24,144,255,0.08)') : 'transparent',
                      color: isActive ? (isDark ? '#64B5F6' : '#1890ff') : themeColors.textSecondary,
                      border: isActive ? `1px solid ${isDark ? 'rgba(33,150,243,0.35)' : 'rgba(24,144,255,0.25)'}` : '1px solid transparent',
                      whiteSpace: 'nowrap',
                      flexShrink: 0,
                    }}
                  >
                    {label}
                  </button>
                );
              })}
            </div>
            {batchMode && selectedExecutionIds.length > 0 && (
              <div className="files-list-batch-bar">
                <span className="files-list-batch-count">{selectedExecutionIds.length} selected</span>
                <Dropdown
                  menu={{
                    items: [
                      { key: 'move-root', icon: <InboxOutlined />, label: 'Uncategorized',
                        onClick: async () => {
                          try {
                            await filesAPI.batchMoveToFolder(selectedExecutionIds.map(Number), null);
                            await loadFolders();
                            loadTaskExecutionsFuncRef.current(false, false, false).catch(() => {});
                            message.success('Moved to Uncategorized');
                          } catch (e: unknown) {
                            const err = e as { response?: unknown };
                            if (!err?.response) {
                              message.error(getErrorMessage(e, 'Move failed'));
                            }
                          }
                        },
                      },
                      ...folders.map(f => ({
                        key: `bmove-${f.id}`, icon: <FolderOutlined />, label: f.name,
                        onClick: async () => {
                          try {
                            await filesAPI.batchMoveToFolder(selectedExecutionIds.map(Number), f.id);
                            await loadFolders();
                            loadTaskExecutionsFuncRef.current(false, false, false).catch(() => {});
                            message.success(`Moved to ${f.name}`);
                          } catch (e: unknown) {
                            const err = e as { response?: unknown };
                            if (!err?.response) {
                              message.error(getErrorMessage(e, 'Move failed'));
                            }
                          }
                        },
                      })),
                    ],
                  }}
                  trigger={['click']}
                >
                  <Button
                    type="default"
                    size="small"
                    icon={<FolderOutlined />}
                    className="files-list-batch-bar-btn"
                  >
                    Move to
                  </Button>
                </Dropdown>
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
                const isFailed = execution.rc_agent_status === 'failed';
                const isAudioFile = AUDIO_LIKE_TYPES.includes(execution.first_file_type ?? '');
                const canAnalyze = isUploadOnly || execution.rc_agent_status === 'failed';
                const fileTypeDisplay = getFileTypeIconDisplay(execution.first_file_type);
                const isDocAnalyzing = docAnalyzingIds.has(execution.id);
                const isAudioAnalyzing = audioAnalyzingIds.has(execution.id);
                const isItemAnalyzing = execution.rc_agent_status === 'analyzing' || isDocAnalyzing || isAudioAnalyzing;
                const statusDisplay = getExecutionStatusDisplay(execution);
                const statusLabel = isItemAnalyzing ? 'Analyzing…' : statusDisplay.label;
                const statusTooltip = isItemAnalyzing ? 'AI analyzing…' : statusDisplay.tooltip;
                const isSharedFile = execution.access_type === 'shared';
                const cardMenuItems: MenuProps['items'] = [
                  // 文档文件：未分析时显示 Analyze 入口
                  ...(!isAudioFile && canAnalyze && !isItemAnalyzing ? [{
                    key: 'analyze',
                    icon: <RobotOutlined />,
                    label: 'AI Analyze',
                    onClick: (e: any) => {
                      e.domEvent.stopPropagation();
                      setSelectedExecutionForView(execution);
                    },
                  }] : []),
                  {
                    key: 'share',
                    icon: <ShareAltOutlined />,
                    label: 'Share',
                    onClick: (e: any) => {
                      e.domEvent.stopPropagation();
                      handleOpenShareModal(execution);
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
                    key: 'move-to-folder',
                    icon: <FolderOutlined />,
                    label: 'Move to Folder',
                    children: [
                      {
                        key: 'move-to-root',
                        icon: <InboxOutlined />,
                        label: 'Uncategorized',
                        onClick: (e: any) => { e.domEvent.stopPropagation(); handleMoveToFolder(execution.id, null); },
                      },
                      ...folders.map(f => ({
                        key: `move-to-${f.id}`,
                        icon: <FolderOutlined />,
                        label: f.name,
                        onClick: (e: any) => { e.domEvent.stopPropagation(); handleMoveToFolder(execution.id, f.id); },
                      })),
                    ],
                  },
                  ...(!isSharedFile ? [{
                    key: 'delete',
                    icon: <DeleteOutlined />,
                    label: 'Delete',
                    danger: true,
                    onClick: (e: any) => {
                      e.domEvent.stopPropagation();
                      handleDeleteExecution(execution);
                    },
                  }] : []),
                ];
                const isChecked = selectedExecutionIds.includes(execution.id);
                return (
                  <div
                    key={execution.id}
                    data-execution-id={execution.id}
                    className={`files-list-item ${isSelected ? 'is-selected' : ''} ${isChecked ? 'is-checked' : ''}`}
                    onClick={() => {
                      setSelectedExecutionForView(execution);
                      setRightPanelView('info');
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
                        {execution.first_display_name || execution.first_filename || `${execution.files_processed} file(s)`}
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
                    <div className="files-list-item-tags" style={{ justifyContent: 'space-between' }}>
                      <div className="files-list-item-type-status">
                        <Tooltip title={fileTypeDisplay.tooltip}>
                          <span
                            className="files-list-item-file-type-icon"
                            style={{ color: fileTypeDisplay.color }}
                            aria-label={fileTypeDisplay.tooltip}
                          >
                            {fileTypeDisplay.icon}
                          </span>
                        </Tooltip>
                        <div className="files-list-item-status-wrap">
                          <Tooltip title={statusTooltip}>
                            <span
                              className="files-list-item-status-text"
                              style={{ color: themeColors.textSecondary }}
                              aria-label={statusLabel}
                              onClick={e => e.stopPropagation()}
                            >
                              {statusLabel}
                            </span>
                          </Tooltip>
                        </div>
                      </div>
                      {execution.share_info && (
                        <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexShrink: 0 }}>
                          <Tooltip title={execution.share_info.shared_by.username}>
                            {execution.share_info.shared_by.avatar_url ? (
                              <Avatar size={20} src={execution.share_info.shared_by.avatar_url} />
                            ) : (
                              <Avatar size={20} style={{ backgroundColor: '#722ed1', fontSize: 10 }}>
                                {execution.share_info.shared_by.username.charAt(0).toUpperCase()}
                              </Avatar>
                            )}
                          </Tooltip>
                          <span style={{ color: '#d9d9d9', fontSize: 14, lineHeight: '20px', userSelect: 'none' }}>|</span>
                          <Avatar.Group size={20} max={{ count: 3, style: { backgroundColor: '#722ed1', fontSize: 10 } }}>
                            {execution.share_info.shared_to.map(u => (
                              <Tooltip key={u.user_id} title={u.username}>
                                {u.avatar_url ? (
                                  <Avatar size={20} src={u.avatar_url} />
                                ) : (
                                  <Avatar size={20} style={{ backgroundColor: '#1890ff', fontSize: 10 }}>
                                    {u.username.charAt(0).toUpperCase()}
                                  </Avatar>
                                )}
                              </Tooltip>
                            ))}
                          </Avatar.Group>
                        </div>
                      )}
                    </div>
                    {parseReportTickers(execution.ticker_tags).length > 0 && (
                      <div className="files-list-item-tickers">
                        <ReportTickerTags tickers={execution.ticker_tags} maxVisible={3} />
                      </div>
                    )}
                  </div>
                );
              })}
              {filteredExecutions.length === 0 && (
                <div style={{ textAlign: 'center', padding: '48px 20px', color: themeColors.textMuted }}>
                  <FileTextOutlined style={{ fontSize: '48px', marginBottom: '16px', display: 'block' }} />
                  <div>{taskExecutions.length === 0 ? 'no file records' : 'no files match the current filter'}</div>
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
                <div className={`files-detail-split ${rightPanelCollapsed ? 'files-detail-split--right-collapsed' : ''}`}>
                  {/* 左侧：Original + Content */}
                  <div className="files-detail-left">
                    <div className="files-detail-left-tabs">
                      <div className="files-detail-tab is-active">Original</div>
                      <div style={{ flex: 1 }} />
                    </div>
                    <div className="files-detail-left-content">
                      <OriginalContentView
                        execution={selectedExecutionForView}
                        themeColors={themeColors}
                        isDark={isDark}
                        onFileSelect={setSelectedFileForView}
                        selectedFile={selectedFileForView}
                        onFileInfoLoaded={setOriginalFileInfoForTabs}
                        onTranscriptPersisted={async () => {
                          if (!selectedExecutionForView) return;
                          try {
                            const details = await tasksAPI.getTaskExecutionDetails(selectedExecutionForView.id);
                            setContentDetailData(details);
                          } catch {
                            message.error('Failed to refresh Markdown content');
                          }
                        }}
                        onTextNoteContentSaved={async () => {
                          if (!selectedExecutionForView) return;
                          try {
                            const details = await tasksAPI.getTaskExecutionDetails(selectedExecutionForView.id);
                            setContentDetailData(details);
                            loadTaskExecutionsFuncRef.current(false, true, false).catch(() => {});
                          } catch {
                            message.error('Failed to refresh Markdown content');
                          }
                        }}
                        onIBReportDetected={setIbReportDataForReview}
                      />
                    </div>
                  </div>
                  {/* 右侧：Info / AI Analysis / Markdown / Trade Review（可收缩）；活动报告仅 Info + Trade Review */}
                  <div className={`files-detail-right ${rightPanelCollapsed ? 'files-detail-right--collapsed' : ''}`}>
                    {rightPanelCollapsed ? (
                      <div className="files-detail-right-collapsed-bar">
                        <Tooltip title="Expand panel">
                          <span
                            className="files-detail-right-expand-btn"
                            onClick={() => setRightPanelCollapsed(false)}
                            role="button"
                            tabIndex={0}
                            onKeyDown={(e) => e.key === 'Enter' && setRightPanelCollapsed(false)}
                          >
                            <DoubleLeftOutlined />
                          </span>
                        </Tooltip>
                      </div>
                    ) : (
                      <>
                        <div className="files-detail-right-header">
                          <div
                            className={`files-detail-tab ${rightPanelView === 'info' ? 'is-active' : ''}`}
                            onClick={() => setRightPanelView('info')}
                            role="tab"
                            aria-label="Info"
                          >
                            <InfoCircleOutlined />
                            <span className="files-detail-tab-label">Info</span>
                          </div>
                          {!ibReportDataForReview && (
                            <div
                              className={`files-detail-tab ${rightPanelView === 'analysis' ? 'is-active' : ''}`}
                              onClick={() => setRightPanelView('analysis')}
                              role="tab"
                              aria-label="AI Analysis"
                            >
                              <RobotOutlined />
                              <span className="files-detail-tab-label">AI Analysis</span>
                            </div>
                          )}
                          {!ibReportDataForReview && (
                            <div
                              className={`files-detail-tab ${rightPanelView === 'content' ? 'is-active' : ''}`}
                              onClick={() => setRightPanelView('content')}
                              role="tab"
                              aria-label="Markdown"
                            >
                              <FileTextOutlined />
                              <span className="files-detail-tab-label">Markdown</span>
                            </div>
                          )}
                          {ibReportDataForReview && (
                            <div
                              className={`files-detail-tab ${rightPanelView === 'review' ? 'is-active' : ''}`}
                              onClick={() => setRightPanelView('review')}
                              role="tab"
                              aria-label="Trade Review"
                            >
                              <ThunderboltOutlined />
                              <span className="files-detail-tab-label">Trade Review</span>
                            </div>
                          )}
                          <div style={{ flex: 1 }} />
                          <Tooltip title="Collapse panel">
                            <span
                              className="files-detail-right-collapse-btn"
                              onClick={() => setRightPanelCollapsed(true)}
                              role="button"
                              tabIndex={0}
                              onKeyDown={(e) => e.key === 'Enter' && setRightPanelCollapsed(true)}
                            >
                              <DoubleRightOutlined />
                            </span>
                          </Tooltip>
                        </div>
                        <div className="files-detail-right-content">
                          {rightPanelView === 'info' && selectedExecutionForView && (
                            <ExecutionDetailsPanel
                              execution={selectedExecutionForView}
                              themeColors={themeColors}
                              isDark={isDark}
                              getDisplayAgentName={getDisplayAgentName}
                              onExecutionRefresh={onExecutionRefresh}
                            />
                          )}
                          {rightPanelView === 'analysis' && !ibReportDataForReview && (
                            <AIAnalysisView
                              execution={selectedExecutionForView}
                              themeColors={themeColors}
                              isDark={isDark}
                              aiAnalysisData={aiAnalysisData}
                              aiAnalysisLoading={aiAnalysisLoading}
                              onDownloadPdf={handleDownloadPdf}
                              pdfDownloading={pdfDownloading}
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
                              onAnalysisSaved={(newContent) => {
                                setAiAnalysisData(prev => prev ? {
                                  ...prev,
                                  execution: { ...prev.execution, final_report: newContent }
                                } : null);
                              }}
                              audioAnalyzing={
                                audioAnalyzingIds.has(selectedExecutionForView.id) ||
                                (AUDIO_LIKE_TYPES.includes(selectedExecutionForView.first_file_type ?? '') &&
                                  selectedExecutionForView.rc_agent_status === 'analyzing')
                              }
                              docAnalyzing={
                                docAnalyzingIds.has(selectedExecutionForView.id) ||
                                (!AUDIO_LIKE_TYPES.includes(selectedExecutionForView.first_file_type ?? '') &&
                                  selectedExecutionForView.rc_agent_status === 'analyzing')
                              }
                            />
                          )}
                          {rightPanelView === 'content' && !ibReportDataForReview && (
                            <ContentMarkdownView
                              execution={selectedExecutionForView}
                              themeColors={themeColors}
                              isDark={isDark}
                              contentDetailData={contentDetailData}
                              contentDetailLoading={contentDetailLoading}
                              onLoadContent={async () => {
                                if (!selectedExecutionForView) return;
                                try {
                                  setContentDetailLoading(true);
                                  const details = await tasksAPI.getTaskExecutionDetails(selectedExecutionForView.id);
                                  setContentDetailData(details);
                                  loadTaskExecutionsFuncRef.current(false, true, false).catch(() => {});
                                } catch (error) {
                                  message.error('Failed to load content');
                                } finally {
                                  setContentDetailLoading(false);
                                }
                              }}
                              onContentSaved={(fileId, newContent) => {
                                setContentDetailData(prev => {
                                  if (!prev) return prev;
                                  return {
                                    ...prev,
                                    files: prev.files.map(f =>
                                      f.id === fileId ? { ...f, content: newContent } : f
                                    ),
                                  };
                                });
                                loadTaskExecutionsFuncRef.current(false, true, false).catch(() => {});
                              }}
                              selectedFileForView={selectedFileForView}
                            />
                          )}
                          {rightPanelView === 'review' && ibReportDataForReview && (
                            <TradeReviewView
                              data={ibReportDataForReview}
                              isDark={isDark}
                              themeColors={themeColors}
                            />
                          )}
                        </div>
                      </>
                    )}
                  </div>
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
          styles={getModalStyles(themeMode)}
        >
          {executionToDelete && (
            <p>Are you sure you want to delete the following execution record? This action cannot be undone.</p>
          )}
        </Modal>

        {/* 重命名对话框 */}
        <Modal
          title="Rename File"
          open={renameModalVisible}
          onOk={handleConfirmRename}
          okButtonProps={{ disabled: !renameStem.trim() }}
          onCancel={() => {
            setRenameModalVisible(false);
            setExecutionToRename(null);
            setRenameStem('');
            setRenameExtension('');
          }}
          okText="Save"
          cancelText="cancel"
          confirmLoading={renameLoading}
          centered
          destroyOnClose
          styles={getModalStyles(themeMode)}
        >
          {executionToRename && (
            <div>
              <p style={{ marginBottom: 8 }}>New file name:</p>
              <Input
                value={renameStem}
                onChange={(e) => setRenameStem(e.target.value)}
                addonAfter={renameExtension || undefined}
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
          styles={getModalStyles(themeMode)}
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

        {/* 批量标签设置模态框已移除 - 改用自动打标签（content_tag_v1） */}

        {/* Share File Modal */}
        <Modal
          title={<><ShareAltOutlined /> Share File</>}
          open={shareModalVisible}
          onCancel={() => setShareModalVisible(false)}
          onOk={handleConfirmShare}
          okText={selectedShareUserIds.length > 0 ? 'Share' : 'Done'}
          cancelText="cancel"
          confirmLoading={sharingInProgress}
          width={520}
          centered
          styles={getModalStyles(themeMode)}
        >
          <div style={{ marginBottom: 16 }}>
            <Text strong>File: </Text>
            <Text>{shareTargetExecution?.first_display_name || shareTargetExecution?.first_filename || 'Unknown'}</Text>
          </div>

          <div style={{ marginBottom: 16 }}>
            <Text strong style={{ display: 'block', marginBottom: 8 }}>Share to users:</Text>
            <Select
              mode="multiple"
              placeholder="Select users to share with"
              style={{ width: '100%' }}
              value={selectedShareUserIds}
              onChange={setSelectedShareUserIds}
              optionFilterProp="label"
              options={shareableUsers
                .filter(u => !currentShares.some(s => s.shared_to_user_id === u.id))
                .map(u => ({ value: u.id, label: u.username }))}
            />
          </div>

          {currentShares.length > 0 && (
            <div>
              <Text strong style={{ display: 'block', marginBottom: 8 }}>Currently shared with:</Text>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {currentShares.map(share => (
                  <Tag
                    key={share.shared_to_user_id}
                    closable={shareTargetExecution?.access_type !== 'shared'}
                    onClose={(e) => {
                      e.preventDefault();
                      const detailFetch = async () => {
                        const detail = await tasksAPI.getTaskExecutionDetails(shareTargetExecution!.id);
                        const fid = detail?.files?.[0]?.id;
                        if (fid) handleRemoveShare(fid, share.shared_to_user_id);
                      };
                      detailFetch();
                    }}
                    color="purple"
                  >
                    <UsergroupAddOutlined style={{ marginRight: 4 }} />
                    {share.shared_to_username}
                  </Tag>
                ))}
              </div>
            </div>
          )}
        </Modal>
      </div>
  );
};

// 外层：ConfigProvider + App，供 modal.confirm 继承主题
const FilesPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = THEME_COLORS[themeMode];

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: themeColors.cardBg,
            colorBgElevated: themeColors.cardBg,
            colorBorderSecondary: themeColors.cardBorder,
            colorPrimary: '#2196F3',
            colorText: themeColors.text,
            colorTextSecondary: themeColors.textSecondary,
            colorFillQuaternary: themeColors.inputBg ?? '#1E202B',
            colorBgLayout: themeColors.pageBg,
            borderRadius: 8,
            borderRadiusLG: 12,
          },
        }),
      }}
    >
      <App>
        <FilesPageInner />
      </App>
    </ConfigProvider>
  );
};

// 右侧 Info Tab：仅展示 Basic Information（不含 File Processing Details）
export const ExecutionDetailsPanel: React.FC<{
  execution: TaskExecution;
  themeColors: any;
  isDark?: boolean;
  getDisplayAgentName: (name: string) => string;
  onExecutionRefresh?: () => void;  // 当检测到 processing/AI 完成时，供父组件刷新 Basic Information
}> = ({ execution, themeColors, isDark = false, getDisplayAgentName, onExecutionRefresh }) => {
  const tagThemeMode = isDark ? 'dark' : 'light';
  const [fileId, setFileId] = useState<number | null>(null);
  const [fileStatus, setFileStatus] = useState<string | null>(null);
  const [fileErrorMessage, setFileErrorMessage] = useState<string | null>(null);
  const [fileTags, setFileTags] = useState<any>(null);
  const [tagsLoading, setTagsLoading] = useState(false);
  const [editTagsVisible, setEditTagsVisible] = useState(false);
  const [tagForm] = Form.useForm();

  const statusDisplay = getExecutionStatusDisplay(execution);
  const statusItem = {
    color: getRcAgentStatusTagColor(execution.rc_agent_status),
    text: statusDisplay.label,
    tooltip: statusDisplay.tooltip,
  };

  // 加载文件ID、状态和标签信息
  const fileStatusRef = useRef<string | null>(null);
  useEffect(() => {
    // 🔥 关键修复：切换 execution 时立即清空旧数据，避免显示上一个文件的 tags
    setFileId(null);
    setFileStatus(null);
    fileStatusRef.current = null;
    setFileErrorMessage(null);
    setFileTags(null);
    setTagsLoading(false);
    
    let pollInterval: NodeJS.Timeout | null = null;
    let pollCount = 0;
    const MAX_POLL_COUNT = 48; // 最多轮询4分钟（5秒 * 48 = 240秒）
    let isMounted = true;
    const displayNameRefreshTimeouts: NodeJS.Timeout[] = [];
    
    const loadFileAndTags = async () => {
      if (!isMounted) return;
      
      try {
        const details = await tasksAPI.getTaskExecutionDetails(execution.id);
        const files = details?.files || [];
        if (files.length > 0) {
          const firstFile = files[0];
          const firstFileId = firstFile.id;
          const firstFileStatus = firstFile.status;
          const firstFileError = firstFile.error_message;
          
          if (!isMounted) return;
          
          // 当 file processing 或 AI 分析完成时，通知父组件刷新 Basic Information
          const prevStatus = fileStatusRef.current;
          if (prevStatus === 'processing' && (firstFileStatus === 'completed' || firstFileStatus === 'failed')) {
            onExecutionRefresh?.();
            // 主任务已完成，但 auto_name_v1 等 Celery 任务稍后写入 display_name，再刷新左侧卡片列表
            [8000, 22000, 50000].forEach((ms) => {
              displayNameRefreshTimeouts.push(
                setTimeout(() => {
                  if (isMounted) onExecutionRefresh?.();
                }, ms)
              );
            });
          }
          fileStatusRef.current = firstFileStatus;
          
          setFileId(firstFileId);
          setFileStatus(firstFileStatus);
          setFileErrorMessage(firstFileError || null);
          
          // 当文件处理完成时，尝试加载标签
          if (firstFileStatus === 'completed') {
            const tagsResponse = await fetch(`/api/file-tags/${firstFileId}`, {
              headers: {
                'Authorization': `Bearer ${localStorage.getItem('access_token')}`
              }
            });
            
            if (!isMounted) return;
            
            if (tagsResponse.ok) {
              const tagsData = await tagsResponse.json();
              setFileTags(tagsData);
              setTagsLoading(false);
              
              // 标签已生成，停止轮询
              if (pollInterval) {
                clearInterval(pollInterval);
                pollInterval = null;
              }
            } else if (tagsResponse.status === 404) {
              // 标签还没生成，但文件已处理完成
              setFileTags(null);
              setTagsLoading(false);
              
              // 继续轮询等待标签生成（如果还没开始轮询）
              if (!pollInterval && pollCount < MAX_POLL_COUNT) {
                pollCount++;
                setTimeout(() => {
                  if (isMounted) {
                    loadFileAndTags();
                  }
                }, 5000);
              }
            }
          } else if (firstFileStatus === 'failed') {
            // 🔥 文件处理失败，清空 tags 并停止轮询
            setFileTags(null);
            setTagsLoading(false);
            if (pollInterval) {
              clearInterval(pollInterval);
              pollInterval = null;
            }
          } else if (firstFileStatus === 'processing') {
            // 文件正在处理中，继续轮询
            setTagsLoading(true);
            if (pollCount < MAX_POLL_COUNT) {
              pollCount++;
              setTimeout(() => {
                if (isMounted) {
                  loadFileAndTags();
                }
              }, 5000);
            }
          }
        }
      } catch (error) {
        console.error('Failed to load file details:', error);
        if (isMounted) {
          setTagsLoading(false);
        }
      }
    };
    
    loadFileAndTags();
    
    // 清理函数：组件卸载时停止轮询
    return () => {
      isMounted = false;
      displayNameRefreshTimeouts.forEach(clearTimeout);
      if (pollInterval) {
        clearInterval(pollInterval);
      }
    };
  }, [execution.id, onExecutionRefresh]);

  // 当 rc_agent_status 为 processing/analyzing 时，轮询以在 AI 分析完成后刷新 Basic Information
  useEffect(() => {
    const isAiProcessing = execution.rc_agent_status === 'processing' || execution.rc_agent_status === 'analyzing';
    if (!isAiProcessing) return;
    let isMounted = true;
    const postAsyncTimers: NodeJS.Timeout[] = [];
    const pollRcAgent = async () => {
      if (!isMounted) return;
      try {
        const fresh = await tasksAPI.getTaskExecution(execution.id);
        if (!isMounted) return;
        if (fresh.rc_agent_status === 'success' || fresh.rc_agent_status === 'failed') {
          onExecutionRefresh?.();
          [8000, 22000, 50000].forEach((ms) => {
            postAsyncTimers.push(
              setTimeout(() => {
                if (isMounted) onExecutionRefresh?.();
              }, ms)
            );
          });
          return;
        }
        setTimeout(pollRcAgent, 5000);
      } catch {
        if (isMounted) setTimeout(pollRcAgent, 5000);
      }
    };
    const t = setTimeout(pollRcAgent, 5000);
    return () => {
      isMounted = false;
      clearTimeout(t);
      postAsyncTimers.forEach(clearTimeout);
    };
  }, [execution.id, execution.rc_agent_status, onExecutionRefresh]);

  const loadTags = async (fid: number) => {
    setTagsLoading(true);
    try {
      const response = await fetch(`/api/file-tags/${fid}`, {
        headers: {
          'Authorization': `Bearer ${localStorage.getItem('access_token')}`
        }
      });
      
      if (response.ok) {
        const data = await response.json();
        setFileTags(data);
      } else if (response.status === 404) {
        setFileTags(null);
      }
    } catch (error) {
      console.error('Failed to load tags:', error);
    } finally {
      setTagsLoading(false);
    }
  };

  const handleEditTags = () => {
    if (!fileTags) return;
    
    tagForm.setFieldsValue({
      sector: fileTags.sector,
      sector_en: fileTags.sector_en || '',
      industry: fileTags.industry,
      industry_en: fileTags.industry_en || '',
      tickers: fileTags.tickers || [],
      concepts: fileTags.concepts || [],
    });
    setEditTagsVisible(true);
  };

  const handleSaveTags = async () => {
    if (!fileId) return;
    
    try {
      const values = await tagForm.validateFields();
      const payload = {
        ...values,
        confidence: fileTags?.confidence,
        reasoning: fileTags?.reasoning,
      };
      const response = await fetch(`/api/file-tags/${fileId}`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${localStorage.getItem('access_token')}`
        },
        body: JSON.stringify(payload)
      });
      
      if (response.ok) {
        const updated = await response.json();
        setFileTags(updated);
        setEditTagsVisible(false);
        message.success('Tags updated successfully');
      } else {
        message.error('Failed to update tags');
      }
    } catch (error) {
      console.error('Failed to save tags:', error);
      message.error('Failed to save tags');
    }
  };

  // PR-5: Re-tag with template 模态框状态
  const [retagModalOpen, setRetagModalOpen] = useState(false);
  const [retagPicker, setRetagPicker] = useState<UploadAnalysisTemplatePickerValue>({});

  const handleRetagFile = async (
    templateOverride?: { templateId?: number; templateVariables?: Record<string, any> }
  ) => {
    if (!fileId) return;

    try {
      // 记录当前的updated_at，用于判断标签是否已更新
      const previousUpdatedAt = fileTags?.updated_at;

      const body: any = { file_id: fileId, force: true };
      // PR-5: 用户在 Re-tag with template 弹窗里选了自定义模板时透传
      if (templateOverride?.templateId) {
        body.template_id = templateOverride.templateId;
        body.template_variables = templateOverride.templateVariables || {};
      }

      const response = await fetch(`/api/file-tags/retag`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${localStorage.getItem('access_token')}`
        },
        body: JSON.stringify(body)
      });
      
      if (response.ok) {
        message.success('Tagging task submitted, please wait...');
        setTagsLoading(true);
        
        // 轮询检查标签是否已更新
        let pollCount = 0;
        const MAX_POLL = 30; // 最多轮询30次
        const POLL_INTERVAL = 3000; // 每3秒检查一次
        
        const pollForUpdatedTags = async () => {
          pollCount++;
          try {
            const tagsResponse = await fetch(`/api/file-tags/${fileId}`, {
              headers: {
                'Authorization': `Bearer ${localStorage.getItem('access_token')}`
              }
            });
            
            if (tagsResponse.ok) {
              const tagsData = await tagsResponse.json();
              // 检查标签是否已更新（updated_at 变化说明重新打标签完成）
              if (!previousUpdatedAt || tagsData.updated_at !== previousUpdatedAt) {
                setFileTags(tagsData);
                setTagsLoading(false);
                message.success('Tags updated successfully');
                return; // 标签已更新，停止轮询
              }
            }
          } catch (error) {
            console.error('Failed to poll tags:', error);
          }
          
          // 未更新，继续轮询
          if (pollCount < MAX_POLL) {
            setTimeout(pollForUpdatedTags, POLL_INTERVAL);
          } else {
            // 超时，尝试最后加载一次
            setTagsLoading(false);
            loadTags(fileId);
            message.info('Tagging may still be in progress, tags will update when ready');
          }
        };
        
        // 延迟3秒后开始轮询（给后端一些处理时间）
        setTimeout(pollForUpdatedTags, POLL_INTERVAL);
      } else {
        message.error('Failed to submit tagging task');
      }
    } catch (error) {
      console.error('Failed to retag:', error);
      message.error('Failed to retag file');
    }
  };

  return (
    <div style={{ color: themeColors.text }}>
      <Card size="small" title="Basic Information" style={{ marginBottom: 16 }}>
        <Descriptions
          column={1}
          layout="horizontal"
          size="small"
          labelStyle={{ fontWeight: 600, color: themeColors.textSecondary, width: 160 }}
          contentStyle={{ color: themeColors.text }}
        >
          <Descriptions.Item label="Agent Name">{getDisplayAgentName(execution.task_name)}</Descriptions.Item>
          <Descriptions.Item label="User">{execution.username}</Descriptions.Item>
          <Descriptions.Item label="Document Name">{execution.first_display_name || execution.first_filename || '-'}</Descriptions.Item>
          <Descriptions.Item label="Document Type">
            {execution.first_file_type ? (
              <Tag color="blue" bordered={false} style={AI_TAG_CHIP_BASE}>
                {execution.first_file_type.toUpperCase()}
              </Tag>
            ) : (
              '-'
            )}
          </Descriptions.Item>
          <Descriptions.Item label="Folder Type">
            <Tooltip title="Automatic folder assignment from auto-classify (classify_file_content_task). Shows Uncategorized when the model did not match any of your folders.">
              {execution.folder_name ? (
                <Tag color="geekblue" bordered={false} style={AI_TAG_CHIP_BASE}>
                  {execution.folder_name}
                </Tag>
              ) : (
                <span style={{ color: themeColors.textMuted }}>Uncategorized</span>
              )}
            </Tooltip>
          </Descriptions.Item>
          <Descriptions.Item label="Time">{new Date(execution.created_at).toLocaleString()}</Descriptions.Item>
          <Descriptions.Item label="Processing Time">{execution.total_time.toFixed(2)}秒</Descriptions.Item>
          <Descriptions.Item label="File Processing Time">{execution.file_processing_time.toFixed(2)}秒</Descriptions.Item>
          <Descriptions.Item label="AI Processing Time">{execution.ai_processing_time.toFixed(2)}秒</Descriptions.Item>
          <Descriptions.Item label="Token Consumption">
            {execution.total_tokens ? execution.total_tokens.toLocaleString() : '-'}
          </Descriptions.Item>
          <Descriptions.Item label="Approximate Cost">
            {execution.total_cost ? `¥${execution.total_cost.toFixed(4)}` : '-'}
          </Descriptions.Item>
          <Descriptions.Item label="Status">
            <Tooltip title={statusItem.tooltip}>
              <Tag color={statusItem.color as any} bordered={false} style={AI_TAG_CHIP_BASE}>
                {statusItem.text}
              </Tag>
            </Tooltip>
          </Descriptions.Item>
          {(execution.llm_model || execution.llm_provider) && (
            <Descriptions.Item label="Analysis Model">
              <Space size={4}>
                {execution.llm_provider && (
                  <Tag color="blue" bordered={false} style={AI_TAG_CHIP_BASE}>
                    {execution.llm_provider}
                  </Tag>
                )}
                {execution.llm_model && (
                  <Tag color="cyan" bordered={false} style={AI_TAG_CHIP_BASE}>
                    {execution.llm_model}
                  </Tag>
                )}
              </Space>
            </Descriptions.Item>
          )}
        </Descriptions>
      </Card>

      {/* Tags Section */}
      <Card 
        size="small" 
        title={
          <Space>
            <TagsOutlined />
            <span>Tags</span>
          </Space>
        }
        extra={
          fileTags && (
            <Space size="small">
              <Tooltip title="Edit tags">
                <Button
                  type="link"
                  size="small"
                  icon={<EditOutlined />}
                  onClick={handleEditTags}
                  aria-label="Edit tags"
                />
              </Tooltip>
              <Tooltip title="Retag">
                <Button
                  type="link"
                  size="small"
                  icon={<ReloadOutlined />}
                  onClick={() => handleRetagFile()}
                  aria-label="Retag"
                />
              </Tooltip>
              <Tooltip title="Re-tag with template">
                <Button
                  type="link"
                  size="small"
                  icon={<RobotOutlined />}
                  onClick={() => {
                    setRetagPicker({});
                    setRetagModalOpen(true);
                  }}
                  aria-label="Re-tag with template"
                />
              </Tooltip>
            </Space>
          )
        }
        loading={tagsLoading}
        style={{ marginBottom: 16 }}
      >
        {!fileTags ? (
          <div style={{ textAlign: 'center', padding: '20px', color: themeColors.textSecondary }}>
            <TagsOutlined style={{ fontSize: 32, marginBottom: 8, opacity: 0.3 }} />
            
            {/* 根据文件状态显示不同的提示 */}
            {fileStatus === 'processing' ? (
              <>
                <div style={{ marginBottom: 8 }}>Document is being processed...</div>
                <div style={{ fontSize: 12, color: themeColors.textMuted }}>
                  Tags will be generated automatically after processing completes
                </div>
                <LoadingOutlined style={{ fontSize: 20, marginTop: 12, color: '#1890ff' }} />
              </>
            ) : fileStatus === 'failed' ? (
              <>
                <div style={{ marginBottom: 8, color: '#ff4d4f', fontWeight: 'bold' }}>
                  Document processing failed
                </div>
                {fileErrorMessage && (
                  <div style={{ 
                    marginBottom: 12, 
                    fontSize: 12, 
                    color: '#ff7875',
                    padding: '8px 12px',
                    background: 'rgba(255, 77, 79, 0.1)',
                    borderRadius: 4,
                    border: '1px solid rgba(255, 77, 79, 0.2)'
                  }}>
                    <div style={{ fontWeight: 'bold', marginBottom: 4 }}>Error Details:</div>
                    <div style={{ wordBreak: 'break-word' }}>{fileErrorMessage}</div>
                  </div>
                )}
                <div style={{ fontSize: 12, color: themeColors.textMuted }}>
                  Cannot generate tags for failed documents
                </div>
              </>
            ) : (
              <div>No tags available</div>
            )}
          </div>
        ) : (
          <Descriptions
            column={1}
            layout="horizontal"
            size="small"
            labelStyle={{ fontWeight: 600, color: themeColors.textSecondary, width: 160 }}
            contentStyle={{ color: themeColors.text }}
          >
            <Descriptions.Item label="Sector">
              <Space size={[4, 4]} wrap>
                <Tag bordered={false} style={aiTagChipStyle('sector', tagThemeMode)}>
                  {fileTags.sector}
                </Tag>
                {fileTags.sector_en && (
                  <Tag bordered={false} style={aiTagChipStyle('sector', tagThemeMode)}>
                    {fileTags.sector_en}
                  </Tag>
                )}
              </Space>
            </Descriptions.Item>
            <Descriptions.Item label="Industry">
              <Space size={[4, 4]} wrap>
                <Tag bordered={false} style={aiTagChipStyle('sector', tagThemeMode)}>
                  {fileTags.industry}
                </Tag>
                {fileTags.industry_en && (
                  <Tag bordered={false} style={aiTagChipStyle('sector', tagThemeMode)}>
                    {fileTags.industry_en}
                  </Tag>
                )}
              </Space>
            </Descriptions.Item>
            {fileTags.tickers && fileTags.tickers.length > 0 && (
              <Descriptions.Item label="Stock Tickers">
                <Space size={[4, 4]} wrap>
                  {fileTags.tickers.map((ticker: string) => (
                    <Tag key={ticker} bordered={false} style={aiTagChipStyle('ticker', tagThemeMode)}>
                      {ticker}
                    </Tag>
                  ))}
                </Space>
              </Descriptions.Item>
            )}
            {fileTags.concepts && fileTags.concepts.length > 0 && (
              <Descriptions.Item label="Concepts">
                <Space size={[4, 4]} wrap>
                  {fileTags.concepts.map((concept: string) => (
                    <Tag key={concept} bordered={false} style={aiTagChipStyle('concept', tagThemeMode)}>
                      {concept}
                    </Tag>
                  ))}
                </Space>
              </Descriptions.Item>
            )}
            {fileTags.confidence != null && (
              <Descriptions.Item label="Confidence">
                <Space size={4}>
                  <Tag
                    color={fileTags.confidence > 0.8 ? 'success' : fileTags.confidence > 0.5 ? 'warning' : 'error'}
                    bordered={false}
                    icon={
                      fileTags.confidence > 0.8 ? (
                        <CheckCircleOutlined style={{ fontSize: 10 }} />
                      ) : undefined
                    }
                    style={AI_TAG_CHIP_BASE}
                  >
                    {(fileTags.confidence * 100).toFixed(0)}%
                  </Tag>
                </Space>
              </Descriptions.Item>
            )}
            {fileTags.reasoning && (
              <Descriptions.Item label="Summary">
                <div style={{ 
                  maxHeight: 120, 
                  overflowY: 'auto',
                  padding: 8,
                  background: themeColors.background,
                  borderRadius: 4,
                  fontSize: 12,
                  lineHeight: 1.6
                }}>
                  {fileTags.reasoning}
                </div>
              </Descriptions.Item>
            )}
            {fileTags.updated_at && (
              <Descriptions.Item label="Last Updated">
                <Text type="secondary" style={{ fontSize: 12 }}>
                  {new Date(fileTags.updated_at).toLocaleString()}
                </Text>
              </Descriptions.Item>
            )}
          </Descriptions>
        )}
      </Card>

      {/* Edit Tags Modal - 自定义 overlay 风格（与 Upload Files / Re-analyze 一致） */}
      {editTagsVisible && (
        <div
          style={{
            position: 'fixed',
            inset: 0,
            zIndex: 1100,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            background: isDark ? 'rgba(0, 0, 0, 0.6)' : 'rgba(0, 0, 0, 0.45)',
            backdropFilter: 'blur(6px)',
            WebkitBackdropFilter: 'blur(6px)',
          }}
          onClick={() => setEditTagsVisible(false)}
        >
          <div
            className="edit-tags-modal-content upload-files-modal"
            style={{
              width: 600,
              maxWidth: '90vw',
              maxHeight: '85vh',
              overflowY: 'auto',
              background: themeColors.cardBg ?? (isDark ? '#1a2234' : '#fff'),
              color: themeColors.text,
              borderRadius: isDark ? CARD_STYLE_BEHANCE_DARK.borderRadius : CARD_STYLE_BEHANCE.borderRadius,
              boxShadow: isDark ? CARD_STYLE_BEHANCE_DARK.boxShadow : CARD_STYLE_BEHANCE.boxShadow,
              border: isDark ? CARD_STYLE_BEHANCE_DARK.border : `1px solid ${themeColors.cardBorder}`,
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div style={{
              padding: '16px 20px',
              borderBottom: `1px solid ${themeColors.cardBorder}`,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
            }}>
              <span style={{ fontWeight: 600, fontSize: 16 }}>Edit Tags</span>
              <Button
                type="text"
                size="small"
                icon={<CloseOutlined />}
                onClick={() => setEditTagsVisible(false)}
              />
            </div>
            <div style={{ padding: '20px' }}>
              <Form
                form={tagForm}
                layout="vertical"
              >
                <div style={{ display: 'flex', gap: 12 }}>
                  <Form.Item
                    name="sector"
                    label="Sector"
                    style={{ flex: 1 }}
                    rules={[{ required: true, message: 'Please enter sector' }]}
                  >
                    <Input placeholder="e.g., 信息技术, 医疗健康" />
                  </Form.Item>
                  <Form.Item
                    name="sector_en"
                    label="Sector (EN)"
                    style={{ flex: 1 }}
                  >
                    <Input placeholder="e.g., Information Technology, Healthcare" />
                  </Form.Item>
                </div>

                <div style={{ display: 'flex', gap: 12 }}>
                  <Form.Item
                    name="industry"
                    label="Industry"
                    style={{ flex: 1 }}
                    rules={[{ required: true, message: 'Please enter industry' }]}
                  >
                    <Input placeholder="e.g., 半导体, 生物制药" />
                  </Form.Item>
                  <Form.Item
                    name="industry_en"
                    label="Industry (EN)"
                    style={{ flex: 1 }}
                  >
                    <Input placeholder="e.g., Semiconductors, Pharmaceuticals" />
                  </Form.Item>
                </div>

                <Form.Item
                  name="tickers"
                  label="Stock Tickers"
                  tooltip="Enter stock codes, press Enter to add multiple"
                >
                  <Select
                    mode="tags"
                    placeholder="e.g., AAPL, 600519.SS, MSFT"
                    style={{ width: '100%' }}
                  />
                </Form.Item>

                <Form.Item
                  name="concepts"
                  label="Concepts / Tags"
                  tooltip="Enter concept tags, press Enter to add multiple"
                >
                  <Select
                    mode="tags"
                    placeholder="e.g., AI, 新能源, 芯片"
                    style={{ width: '100%' }}
                  />
                </Form.Item>

              </Form>
            </div>
            <div style={{
              padding: '12px 20px',
              borderTop: `1px solid ${themeColors.cardBorder}`,
              display: 'flex',
              justifyContent: 'flex-end',
              gap: 8,
            }}>
              <Tooltip title="Cancel">
                <Button icon={<CloseOutlined />} onClick={() => setEditTagsVisible(false)} aria-label="Cancel" />
              </Tooltip>
              <Tooltip title="Save">
                <Button type="primary" icon={<CheckOutlined />} onClick={handleSaveTags} aria-label="Save" />
              </Tooltip>
            </div>
          </div>
        </div>
      )}

      {/* PR-5: Re-tag with custom template 弹窗 */}
      <Modal
        title="Re-tag with Custom Template"
        open={retagModalOpen}
        onCancel={() => setRetagModalOpen(false)}
        onOk={() => {
          if (!retagPicker.templateId) {
            message.warning('Please select a template first.');
            return;
          }
          handleRetagFile({
            templateId: retagPicker.templateId,
            templateVariables: retagPicker.templateVariables,
          });
          setRetagModalOpen(false);
        }}
        okText="Re-tag"
        cancelText="Cancel"
        width={600}
        destroyOnClose
      >
        <UploadAnalysisTemplatePicker
          value={retagPicker}
          onChange={setRetagPicker}
          mode="optional"
          moduleCode="file_tagging"
          label="Re-tag Template"
          hint="Toggle on to re-tag this file using your private file_tagging template."
        />
      </Modal>
    </div>
  );
};

// Content 标签页：展示并编辑 file_processing_details 中的 content（Markdown）
const ContentMarkdownView: React.FC<{
  execution: TaskExecution;
  themeColors: any;
  isDark: boolean;
  contentDetailData: TaskExecutionDetail | null;
  contentDetailLoading: boolean;
  onLoadContent: () => Promise<void>;
  onContentSaved?: (fileId: number, newContent: string) => void;
  selectedFileForView: any;
}> = ({ execution, themeColors, isDark, contentDetailData, contentDetailLoading, onLoadContent, onContentSaved, selectedFileForView }) => {
  const themeMode = isDark ? 'dark' : 'light';
  const { addTask } = useFileProcessing();
  const [isEditing, setIsEditing] = useState(false);
  const [editContent, setEditContent] = useState('');
  const [saving, setSaving] = useState(false);
  const [fileStatus, setFileStatus] = useState<string | null>(null);
  
  // Re-parse 相关状态
  const [reparseModalVisible, setReparseModalVisible] = useState(false);
  const [reparseParams, setReparseParams] = useState<ProcessingParams>({
    processing_mode: 'quick',
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
  });
  const [reparsing, setReparsing] = useState(false);

  // 初始加载内容
  useEffect(() => {
    if (!contentDetailData || contentDetailData.execution.id !== execution.id) {
      onLoadContent();
    }
  }, [execution.id, contentDetailData, onLoadContent]);

  // 监控文件状态并自动刷新
  useEffect(() => {
    let pollCount = 0;
    const MAX_POLL_COUNT = 120; // 最多轮询10分钟（5秒 * 120 = 600秒）
    let isMounted = true;
    let previousStatus: string | null = null;
    
    const checkStatusAndRefresh = async () => {
      if (!isMounted) return;
      
      try {
        const details = await tasksAPI.getTaskExecutionDetails(execution.id);
        const files = details?.files || [];
        
        if (!isMounted) return;
        
        if (files.length > 0) {
          const firstFile = files[0];
          const currentStatus = firstFile.status;
          
          // 如果状态从processing变为completed，刷新内容
          if (previousStatus === 'processing' && currentStatus === 'completed') {
            await onLoadContent(); // 刷新Markdown内容
          }
          
          setFileStatus(currentStatus);
          previousStatus = currentStatus;
          
          // 如果文件还在处理中，继续轮询
          if (currentStatus === 'processing' && pollCount < MAX_POLL_COUNT) {
            pollCount++;
            setTimeout(() => {
              if (isMounted) {
                checkStatusAndRefresh();
              }
            }, 5000);
          }
        }
      } catch (error) {
        console.error('Failed to check file status:', error);
      }
    };
    
    checkStatusAndRefresh();
    
    // 清理函数
    return () => {
      isMounted = false;
    };
  }, [execution.id, onLoadContent]);

  if (contentDetailLoading) {
    return (
      <div style={{ textAlign: 'center', padding: '40px', color: themeColors.text }}>
        <LoadingOutlined style={{ fontSize: '32px', color: '#1890ff' }} />
        <div style={{ marginTop: '16px', color: themeColors.textSecondary }}>Loading content...</div>
      </div>
    );
  }

  const files = contentDetailData?.files ?? [];
  const fileToShow = selectedFileForView?.id
    ? files.find((f: { id: number }) => f.id === selectedFileForView.id)
    : files[0];
  const content = fileToShow?.content;
  const rawStr = content == null ? '' : typeof content === 'string' ? content : String(content);
  const ibData = isIBReportContent(rawStr);
  const contentStr = ibData ? ibData.markdown : rawStr;

  if (!fileToShow) {
    return (
      <div style={{ textAlign: 'center', padding: '40px', color: themeColors.textMuted }}>
        <FileTextOutlined style={{ fontSize: '48px', marginBottom: '16px', display: 'block' }} />
        <div>No file in this execution</div>
      </div>
    );
  }

  const handleStartEdit = () => {
    setEditContent(contentStr);
    setIsEditing(true);
  };

  const handleCancelEdit = () => {
    setIsEditing(false);
    setEditContent('');
  };

  const handleSave = async () => {
    try {
      setSaving(true);
      await filesAPI.updateFileDetailContent(fileToShow.id, editContent);
      onContentSaved?.(fileToShow.id, editContent);
      setIsEditing(false);
      setEditContent('');
      message.success('Content saved');
    } catch (error) {
      message.error('Failed to save content');
    } finally {
      setSaving(false);
    }
  };

  const isCurrentFileImage = !!(fileToShow && filesAPI.isImageFile(fileToShow.filename));

  const handleOpenReparseModal = () => {
    if (isCurrentFileImage) {
      setReparseParams(prev => ({ ...prev, processing_mode: 'llamaparse' }));
    }
    setReparseModalVisible(true);
  };

  const handleReparse = async () => {
    if (!fileToShow) {
      message.error('No file selected');
      return;
    }

    try {
      setReparsing(true);
      const response = await filesAPI.reparseDocument({
        file_id: fileToShow.id,
        params: reparseParams
      });

      message.success(`Re-parse task submitted: ${response.task_id}`);
      setReparseModalVisible(false);

      // 🔥 添加任务到全局进度显示（NavBar 的 Processing Tasks）
      addTask(response.task_id, response.filename || fileToShow.filename, 'document');

      // 开始轮询任务状态
      const pollInterval = setInterval(async () => {
        try {
          const status = await filesAPI.getReparseStatus(response.task_id);
          
          if (status.status === 'completed') {
            clearInterval(pollInterval);
            message.success('Document re-parsed successfully!');
            setReparsing(false);
            // 刷新内容
            await onLoadContent();
          } else if (status.status === 'failed') {
            clearInterval(pollInterval);
            message.error(`Re-parse failed: ${status.message}`);
            setReparsing(false);
          }
        } catch (error) {
          console.error('Failed to check reparse status:', error);
        }
      }, 5000);

      // 10分钟后停止轮询
      setTimeout(() => {
        clearInterval(pollInterval);
        setReparsing(false);
      }, 600000);

    } catch (error: any) {
      message.error(getErrorMessage(error, 'Failed to submit re-parse task'));
      setReparsing(false);
    }
  };

  // Re-parse Modal
  const reparseModalContent = (
    <Modal
      title="Re-parse Document - Configure Parameters"
      open={reparseModalVisible}
      onCancel={() => setReparseModalVisible(false)}
      onOk={handleReparse}
      okText="Start Re-parsing"
      cancelText="Cancel"
      confirmLoading={reparsing}
      width={700}
      centered
      styles={{
        ...getModalStyles(themeMode),
        body: { ...getModalStyles(themeMode).body, maxHeight: '70vh', overflowY: 'auto' },
      }}
    >
      <Segmented
        block
        value={reparseParams.processing_mode || 'llamaparse'}
        onChange={(value) => {
          if (value === 'quick' && isCurrentFileImage) {
            message.warning('Image files require LlamaParse for OCR processing.');
            return;
          }
          setReparseParams(prev => ({ ...prev, processing_mode: value as 'quick' | 'llamaparse' }));
        }}
        options={[
          {
            label: (
              <div style={{ padding: '4px 0' }}>
                <Space><ThunderboltOutlined /><span style={{ fontWeight: 500 }}>Quick Process</span></Space>
                <div style={{ fontSize: 11, opacity: 0.65, marginTop: 2 }}>Fast local parsing for simple documents</div>
              </div>
            ),
            value: 'quick',
            disabled: isCurrentFileImage,
          },
          {
            label: (
              <div style={{ padding: '4px 0' }}>
                <Space><CloudUploadOutlined /><span style={{ fontWeight: 500 }}>LlamaParse</span></Space>
                <div style={{ fontSize: 11, opacity: 0.65, marginTop: 2 }}>Cloud parsing with OCR for complex docs</div>
              </div>
            ),
            value: 'llamaparse',
          },
        ]}
        style={{ marginBottom: '12px', marginTop: '8px' }}
      />

      {reparseParams.processing_mode === 'quick' ? (
        <div style={{
          padding: '8px 12px',
          background: isDark ? '#1E202B' : '#f5f5f5',
          borderRadius: 8,
          fontSize: '12px',
          opacity: 0.65,
        }}>
          Uses lightweight local libraries to convert documents to Markdown. Best for text-based PDFs, Word, and Excel files. Does not support OCR or scanned documents.
        </div>
      ) : (
        <Form layout="vertical">
          <Divider style={{ margin: '12px 0' }} />
          <Row gutter={[12, 12]}>
            <Col span={12}>
              <Form.Item label="Language Settings">
                <Select
                  mode="multiple"
                  value={reparseParams.language}
                  onChange={(value) => setReparseParams(prev => ({ ...prev, language: value }))}
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
                  value={reparseParams.max_pages}
                  onChange={(value) => setReparseParams(prev => ({ ...prev, max_pages: value || 10 }))}
                  min={1}
                  max={100}
                  style={{ width: '100%' }}
                />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={[12, 12]}>
            <Col span={12}>
              <Form.Item label="Polling Interval (seconds)">
                <InputNumber
                  value={reparseParams.polling_interval}
                  onChange={(value) => setReparseParams(prev => ({ ...prev, polling_interval: value || 10 }))}
                  min={5}
                  max={60}
                  style={{ width: '100%' }}
                />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="Target Pages">
                <Input
                  value={reparseParams.target_pages}
                  onChange={(e) => setReparseParams(prev => ({ ...prev, target_pages: e.target.value }))}
                  placeholder="e.g., 1-5, 7, 10"
                />
              </Form.Item>
            </Col>
          </Row>

          <Divider style={{ margin: '12px 0' }} />

          <Row gutter={[12, 12]}>
            <Col span={12}>
              <Form.Item label="Fast Mode">
                <Switch
                  size="small"
                  checked={reparseParams.fast_mode}
                  onChange={(checked) => setReparseParams(prev => ({ ...prev, fast_mode: checked }))}
                />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="Disable OCR">
                <Switch
                  size="small"
                  checked={reparseParams.disable_ocr}
                  onChange={(checked) => setReparseParams(prev => ({ ...prev, disable_ocr: checked }))}
                />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={[12, 12]}>
            <Col span={8}>
              <Form.Item label="Skip Diagonal Text">
                <Switch
                  size="small"
                  checked={reparseParams.skip_diagonal_text}
                  onChange={(checked) => setReparseParams(prev => ({ ...prev, skip_diagonal_text: checked }))}
                />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="Skip Table Extraction">
                <Switch
                  size="small"
                  checked={reparseParams.skip_table_extraction}
                  onChange={(checked) => setReparseParams(prev => ({ ...prev, skip_table_extraction: checked }))}
                />
              </Form.Item>
            </Col>
            <Col span={8}>
              <Form.Item label="Extract Images">
                <Switch
                  size="small"
                  checked={reparseParams.extract_images}
                  onChange={(checked) => setReparseParams(prev => ({ ...prev, extract_images: checked }))}
                />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={[12, 12]}>
            <Col span={12}>
              <Form.Item label="Use Multimodal Model">
                <Switch
                  size="small"
                  checked={reparseParams.use_vendor_multimodal_model}
                  onChange={(checked) => setReparseParams(prev => ({ ...prev, use_vendor_multimodal_model: checked }))}
                />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="Multimodal API Key">
                <Input
                  value={reparseParams.vendor_multimodal_api_key}
                  onChange={(e) => setReparseParams(prev => ({ ...prev, vendor_multimodal_api_key: e.target.value }))}
                  placeholder="Enter API key"
                  disabled={!reparseParams.use_vendor_multimodal_model}
                />
              </Form.Item>
            </Col>
          </Row>

        </Form>
      )}
    </Modal>
  );

  if (isEditing) {
    return (
      <>
        {reparseModalContent}
        <div
          style={{
            flex: 1,
            minHeight: 0,
            display: 'flex',
            flexDirection: 'column',
            padding: '16px 20px',
            color: themeColors.text,
          }}
        >
        <Input.TextArea
          value={editContent}
          onChange={(e) => setEditContent(e.target.value)}
          placeholder="Markdown content..."
          style={{
            flex: 1,
            minHeight: 280,
            fontFamily: 'monospace',
            fontSize: 13,
            background: themeColors.codeBlockBg,
            borderColor: themeColors.cardBorder,
            color: themeColors.text,
          }}
          className={isDark ? 'files-content-textarea-dark' : ''}
        />
        <Space style={{ marginTop: 12 }}>
          <Tooltip title="Save">
            <Button type="primary" onClick={handleSave} loading={saving} icon={<CheckOutlined />} aria-label="Save" />
          </Tooltip>
          <Tooltip title="Cancel">
            <span>
              <Button onClick={handleCancelEdit} disabled={saving} icon={<CloseOutlined />} aria-label="Cancel" />
            </span>
          </Tooltip>
        </Space>
      </div>
      </>
    );
  }

  if (content == null || content === '') {
    return (
      <>
        {reparseModalContent}
        <div style={{
        flex: 1,
        minHeight: 0,
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        justifyContent: 'center',
        width: '100%',
      }}>
        <div style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          padding: '40px',
          textAlign: 'center',
          color: themeColors.textMuted,
        }}>
          <FileTextOutlined style={{ fontSize: '48px', marginBottom: '16px' }} />
          
          {fileStatus === 'processing' ? (
            <>
              <div style={{ marginBottom: '12px', fontSize: '16px' }}>Document is being processed...</div>
              <div style={{ fontSize: '13px', color: themeColors.textSecondary, marginBottom: '16px' }}>
                Content will be available after document parsing completes
              </div>
              <LoadingOutlined style={{ fontSize: '24px', color: '#1890ff' }} />
            </>
          ) : (
            <>
              <div style={{ marginBottom: '20px', fontSize: '16px' }}>No extracted content for {fileToShow.filename}</div>
              <Tooltip title="Re-parse document">
                <Button
                  type="primary"
                  icon={<ReloadOutlined />}
                  onClick={handleOpenReparseModal}
                  disabled={fileStatus === 'processing'}
                  aria-label="Re-parse document"
                  style={{
                    height: '40px',
                    width: '40px',
                    paddingInline: 0,
                    borderRadius: '8px',
                  }}
                />
              </Tooltip>
            </>
          )}
        </div>
      </div>
      </>
    );
  }

  // 检查是否可以编辑
  const isProcessing = fileStatus === 'processing';
  const canEdit = !isProcessing;

  return (
    <>
      {reparseModalContent}
      <div
      className="files-detail-content-markdown"
      style={{
        flex: 1,
        minHeight: 0,
        overflow: 'auto',
        padding: '16px 20px',
        color: themeColors.text,
      }}
    >
      <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: 8, alignItems: 'center', gap: 8 }}>
        {isProcessing && (
          <div style={{ fontSize: '12px', color: themeColors.textSecondary, marginRight: 'auto' }}>
            <LoadingOutlined style={{ marginRight: 6 }} />
            Document processing... (Edit disabled)
          </div>
        )}
        <Tooltip title={!canEdit ? 'Cannot edit while document is being processed' : 'Edit markdown'}>
          <span>
            <Button
              type="primary"
              ghost
              size="small"
              icon={<EditOutlined />}
              onClick={handleStartEdit}
              disabled={!canEdit}
              aria-label="Edit markdown"
            />
          </span>
        </Tooltip>
      </div>
      <MarkdownRenderer
        content={contentStr}
        variant={isDark ? 'dark' : 'light'}
      />
    </div>
    </>
  );
};

/** text 类型：Original 面板直接渲染 Markdown，支持 Edit 编辑保存 */
const OriginalTextNotePanel: React.FC<{
  fileId: number;
  content: string;
  filename: string;
  themeColors: any;
  isDark: boolean;
  onContentUpdated: (newContent: string) => void;
  /** 保存成功后通知父级，便于同步 Content 标签页等 */
  onSaved?: () => void | Promise<void>;
}> = ({ fileId, content, filename, themeColors, isDark, onContentUpdated, onSaved }) => {
  const [isEditing, setIsEditing] = useState(false);
  const [editContent, setEditContent] = useState('');
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    if (!isEditing) {
      setEditContent(content);
    }
  }, [content, isEditing]);

  const handleStartEdit = () => {
    setEditContent(content);
    setIsEditing(true);
  };

  const handleCancelEdit = () => {
    setIsEditing(false);
    setEditContent(content);
  };

  const handleSave = async () => {
    try {
      setSaving(true);
      await filesAPI.updateFileDetailContent(fileId, editContent);
      onContentUpdated(editContent);
      await onSaved?.();
      setIsEditing(false);
      message.success('Content saved');
    } catch {
      message.error('Failed to save content');
    } finally {
      setSaving(false);
    }
  };

  if (isEditing) {
    return (
      <div
        style={{
          flex: 1,
          minHeight: 0,
          display: 'flex',
          flexDirection: 'column',
          padding: '16px 20px',
          color: themeColors.text,
        }}
      >
        <Input.TextArea
          value={editContent}
          onChange={(e) => setEditContent(e.target.value)}
          placeholder="Markdown content..."
          style={{
            flex: 1,
            minHeight: 280,
            fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Consolas, monospace',
            fontSize: 13,
            lineHeight: 1.6,
            background: themeColors.codeBlockBg,
            borderColor: themeColors.cardBorder,
            color: themeColors.text,
          }}
          className={isDark ? 'files-content-textarea-dark' : ''}
        />
        <Space style={{ marginTop: 12 }}>
          <Tooltip title="Save">
            <Button type="primary" onClick={handleSave} loading={saving} icon={<CheckOutlined />} aria-label="Save" />
          </Tooltip>
          <Tooltip title="Cancel">
            <span>
              <Button onClick={handleCancelEdit} disabled={saving} icon={<CloseOutlined />} aria-label="Cancel" />
            </span>
          </Tooltip>
        </Space>
      </div>
    );
  }

  return (
    <div
      className="files-detail-content-markdown"
      style={{
        flex: 1,
        minHeight: 0,
        overflow: 'auto',
        padding: '16px 20px',
        color: themeColors.text,
      }}
    >
      <div
        style={{
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          marginBottom: 12,
          gap: 8,
          flexWrap: 'wrap',
        }}
      >
        <span style={{ fontWeight: 600, fontSize: 14, color: themeColors.text }}>{filename}</span>
        <Tooltip title="Edit">
          <Button type="primary" ghost size="small" icon={<EditOutlined />} onClick={handleStartEdit} aria-label="Edit" />
        </Tooltip>
      </div>
      {content.trim() ? (
        <MarkdownRenderer content={content} variant={isDark ? 'dark' : 'light'} />
      ) : (
        <div style={{ color: themeColors.textSecondary, fontSize: 14 }}>
          Empty note. Click Edit to add content.
        </div>
      )}
    </div>
  );
};

/** Web Link 快照：iframe 预览 + 仅此类可用的「打印」 */
const WebOriginalPreviewWithPrint: React.FC<{
  webEmbedSrc: string;
  filename?: string;
  themeColors: any;
  isDark: boolean;
}> = ({ webEmbedSrc, filename, themeColors, isDark }) => {
  const iframeRef = useRef<HTMLIFrameElement>(null);
  const [snapshotReady, setSnapshotReady] = useState(false);

  useEffect(() => {
    setSnapshotReady(false);
  }, [webEmbedSrc]);

  const handlePrint = useCallback(() => {
    const iframe = iframeRef.current;
    if (!webEmbedSrc) {
      message.warning('No snapshot URL to print.');
      return;
    }
    if (!snapshotReady || !iframe?.contentWindow) {
      message.warning('Snapshot is still loading. Try again in a moment.');
      return;
    }
    try {
      iframe.contentWindow.focus();
      iframe.contentWindow.print();
    } catch {
      try {
        const w = window.open(webEmbedSrc, '_blank');
        if (!w) {
          message.error('Unable to open print window. Check whether pop-ups are blocked.');
          return;
        }
        const runPrint = () => {
          try {
            w.focus();
            w.print();
          } catch {
            message.info('Opened the snapshot in a new tab. Use your browser’s Print command there.');
          }
        };
        if (w.document?.readyState === 'complete') runPrint();
        else w.addEventListener('load', runPrint, { once: true });
      } catch {
        message.error('Print is not available for this preview.');
      }
    }
  }, [webEmbedSrc, snapshotReady]);

  return (
    <div style={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', gap: 8 }}>
      <div style={{ display: 'flex', justifyContent: 'flex-end', flexShrink: 0 }}>
        <Tooltip title="Print the snapshot shown below">
          <Button
            type="default"
            size="small"
            icon={<PrinterOutlined />}
            onClick={handlePrint}
            disabled={!webEmbedSrc}
            aria-label="Print web snapshot"
          >
            Print
          </Button>
        </Tooltip>
      </div>
      <div
        className="original-web-frame-host"
        style={{
          position: 'relative',
          flex: 1,
          minHeight: 0,
          border: `1px solid ${themeColors.cardBorder}`,
          borderRadius: '8px',
          overflow: 'hidden',
          backgroundColor: themeColors.cardBg,
        }}
      >
        <iframe
          ref={iframeRef}
          src={webEmbedSrc}
          title={filename || 'Web snapshot'}
          onLoad={() => setSnapshotReady(true)}
          sandbox="allow-same-origin allow-modals"
          style={{
            position: 'absolute',
            inset: 0,
            width: '100%',
            height: '100%',
            border: 'none',
            colorScheme: isDark ? 'dark' : 'light',
            backgroundColor: isDark ? '#141414' : '#fff',
          }}
        />
      </div>
    </div>
  );
};

// 原文内容查看组件（仅支持单文件，不再显示文件列表）
export const OriginalContentView: React.FC<{
  execution: TaskExecution;
  themeColors: any;
  isDark: boolean;
  onFileSelect: (file: any) => void;
  selectedFile: any;
  onFileInfoLoaded?: (info: { filename?: string; file_type?: string }) => void;
  /** 转写保存后刷新 Markdown 等（Original 会在内部自行重新拉取 getOriginalFileInfo） */
  onTranscriptPersisted?: () => void | Promise<void>;
  /** text 笔记在 Original 中保存后，同步右侧 Content 等 */
  onTextNoteContentSaved?: () => void | Promise<void>;
  onIBReportDetected?: (data: IBReportData | null) => void;
}> = ({ execution, themeColors, isDark, onFileSelect, selectedFile, onFileInfoLoaded, onTranscriptPersisted, onTextNoteContentSaved, onIBReportDetected }) => {
  const { isMobile } = useDeviceDetection();
  const [loading, setLoading] = useState(true);
  const [fileContent, setFileContent] = useState<any>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [ibReportData, setIbReportData] = useState<ReturnType<typeof isIBReportContent>>(null);

  const handleTranscriptPersisted = useCallback(async () => {
    try {
      const details = await tasksAPI.getTaskExecutionDetails(execution.id);
      const file = details.files?.[0];
      if (file) {
        const originalFileInfo = await filesAPI.getOriginalFileInfo(file.id);
        const token = localStorage.getItem('access_token');
        if (originalFileInfo.file_url) {
          originalFileInfo.file_url = appendServeAuthToken(originalFileInfo.file_url, token);
        }
        setFileContent(originalFileInfo);
      }
    } catch (e) {
      console.error('Failed to refresh original file after transcript edit:', e);
    }
    await onTranscriptPersisted?.();
  }, [execution.id, onTranscriptPersisted]);

  useEffect(() => {
    const loadAndShowFile = async () => {
      try {
        setLoading(true);
        setLoadError(null);
        setIbReportData(null);
        const details = await tasksAPI.getTaskExecutionDetails(execution.id);
        const files = details.files || [];
        if (files.length === 0) {
          setLoadError('No original file');
          return;
        }
        const file = files[0];
        onFileSelect(file);

        const parsed = isIBReportContent(file.content);
        if (parsed) {
          setIbReportData(parsed);
          onIBReportDetected?.(parsed);
          onFileInfoLoaded?.({ filename: file.filename, file_type: file.file_type });
          return;
        }
        onIBReportDetected?.(null);

        const originalFileInfo = await filesAPI.getOriginalFileInfo(file.id);

        // 仅 serve 代理 URL 需要 token；OSS 签名 URL 不可追加额外 query
        const token = localStorage.getItem('access_token');
        if (originalFileInfo.file_url) {
          originalFileInfo.file_url = appendServeAuthToken(originalFileInfo.file_url, token);
        }

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

  const emptyStateWrapperStyle: React.CSSProperties = {
    flex: 1,
    minHeight: 0,
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    width: '100%',
  };

  if (loading) {
    return (
      <div style={emptyStateWrapperStyle}>
        <div style={{ textAlign: 'center', padding: '40px', color: themeColors.text }}>
          <LoadingOutlined style={{ fontSize: '32px', color: '#1890ff' }} />
          <div style={{ marginTop: '16px', color: themeColors.textSecondary }}>Loading...</div>
        </div>
      </div>
    );
  }

  if (ibReportData) {
    return (
      <div style={{ width: '100%', flex: 1, minHeight: 0, overflow: 'auto', color: themeColors.text }}>
        <IBTradingReportView data={ibReportData} isDark={isDark} />
      </div>
    );
  }

  if (loadError || !fileContent) {
    return (
      <div style={emptyStateWrapperStyle}>
        <div style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          padding: '40px',
          textAlign: 'center',
          color: themeColors.textMuted,
        }}>
          <FileTextOutlined style={{ fontSize: '48px', marginBottom: '16px', color: themeColors.textMuted }} />
          <div style={{ fontSize: '16px' }}>{loadError || 'No original file'}</div>
        </div>
      </div>
    );
  }

  return (
    <div style={{ width: '100%', flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', color: themeColors.text }}>
      {(() => {
        const fileType = (fileContent.file_type || '').toLowerCase();

        if (isOriginalPdfFile(fileContent.file_type, fileContent.filename)) {
          // PDF预览：仅当使用未签名的 OSS 默认域名时才无法预览（会被强制下载）
          // 带签名的 URL 已设置 response-content-disposition=inline，可正常预览
          const pdfEmbedSrc = absoluteUrlForEmbeddedFile(fileContent.file_url || '');

          if (isUnsignedOssDefaultDomainObjectUrl(fileContent.file_url || '')) {
            // OSS 默认域名会强制下载，显示提示和下载按钮
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
                <FilePdfOutlined style={{ fontSize: '48px', color: '#ff4d4f', marginBottom: '16px' }} />
                <div style={{ marginBottom: '8px', fontSize: '16px', fontWeight: 500 }}>
                  {fileContent.filename}
                </div>
                <div style={{ 
                  marginBottom: '16px', 
                  color: themeColors.textSecondary,
                  textAlign: 'center',
                  maxWidth: '500px'
                }}>
                  ⚠️ PDF 无法在浏览器内预览<br/>
                  <small>（中国大陆 OSS 默认域名暂不支持在线预览，会强制下载）</small><br/>
                  <small style={{ color: '#999' }}>如需预览，需配置已备案的自定义域名</small>
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
                  style={{ height: '40px', paddingInline: '24px', fontSize: '14px', borderRadius: '8px' }}
                >
                  Download PDF
                </Button>
              </div>
            );
          } else {
            // 桌面：iframe 内嵌。手机：多数 Android WebView/自带浏览器无法在 iframe/embed 内嵌 PDF（Chrome 常只给 Open 外链），
            // 使用 PDF.js 画布渲染，与浏览器内核无关、可滚动预览整份 PDF。
            return (
              <div
                style={{
                  flex: 1,
                  minHeight: 0,
                  display: 'flex',
                  flexDirection: 'column',
                }}
              >
                <div
                  className={isMobile ? 'original-pdf-frame-host' : undefined}
                  style={{
                    position: 'relative',
                    flex: 1,
                    minHeight: isMobile ? undefined : 0,
                    border: `1px solid ${themeColors.cardBorder}`,
                    borderRadius: '8px',
                    overflow: 'hidden',
                    display: isMobile ? 'flex' : undefined,
                    flexDirection: isMobile ? 'column' : undefined,
                  }}
                >
                  {isMobile ? (
                    <OriginalPdfJsViewer
                      fileUrl={pdfEmbedSrc}
                      style={{ flex: 1, minHeight: 0 }}
                      text={themeColors.text}
                      textMuted={themeColors.textSecondary}
                    />
                  ) : (
                    <iframe
                      src={pdfEmbedSrc}
                      title={fileContent.filename}
                      style={{
                        position: 'absolute',
                        inset: 0,
                        width: '100%',
                        height: '100%',
                        border: 'none',
                      }}
                    />
                  )}
                </div>
              </div>
            );
          }
        } else if (['doc', 'docx', 'word'].includes(fileType)) {
          // Word文档预览：使用Microsoft Office Online Viewer
          // 需要文件URL是公网可访问的绝对 HTTPS URL（Microsoft服务器需要能拉取该文件）
          // 相对路径、localhost、或 /api/serve/ 代理路径均无法被 Office Online Viewer 访问
          const canUseOfficeViewer = fileContent.file_url.startsWith('https://');
          
          if (!canUseOfficeViewer) {
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
                  Word document preview requires a public HTTPS file URL<br/>
                  Please download to view
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
                  style={{ height: '40px', paddingInline: '24px', fontSize: '14px', borderRadius: '8px' }}
                >
                  Download Word File
                </Button>
              </div>
            );
          } else {
            // URL 是公网可访问的 HTTPS 地址，使用 Microsoft Office Online Viewer
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
        } else if (fileType === 'csv') {
          // CSV 文件：直接下载（Office Online Viewer 不支持 CSV）
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
                CSV files cannot be previewed online<br/>
                Please download and view in Excel or text editor
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
                style={{ height: '40px', paddingInline: '24px', fontSize: '14px', borderRadius: '8px' }}
              >
                Download CSV File
              </Button>
            </div>
          );
        } else if (['excel', 'xlsx', 'xls'].includes(fileType)) {
          // Excel文档预览：同样使用Microsoft Office Online Viewer
          // 需要公网可访问的绝对 HTTPS URL
          const canUseOfficeViewer = fileContent.file_url.startsWith('https://');
          
          if (!canUseOfficeViewer) {
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
                  Excel document preview requires a public HTTPS file URL<br/>
                  Please download to view
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
                  style={{ height: '40px', paddingInline: '24px', fontSize: '14px', borderRadius: '8px' }}
                >
                  Download Excel File
                </Button>
              </div>
            );
          } else {
            // URL 是公网可访问的 HTTPS 地址，使用 Microsoft Office Online Viewer
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
        } else if (AUDIO_LIKE_TYPES.includes(fileType ?? '') && fileContent.asr_with_diarization) {
          // 音频 / Podcast 文件：显示说话人分离可视化界面（手机端内联原生播放条）
          return (
            <AudioDiarizationView
              fileContent={fileContent}
              themeColors={themeColors}
              isDark={isDark}
              onTranscriptPersisted={handleTranscriptPersisted}
              inlineAudioPlayer={isMobile}
            />
          );
        } else if (fileType === 'youtube' && fileContent.asr_with_diarization) {
          // YouTube 文件：显示转录界面（复用 AudioDiarizationView 组件）
          return (
            <AudioDiarizationView
              fileContent={fileContent}
              themeColors={themeColors}
              isDark={isDark}
              onTranscriptPersisted={handleTranscriptPersisted}
            />
          );
        } else if (fileType === 'text') {
          const raw = fileContent.content;
          const textContent = raw == null ? '' : typeof raw === 'string' ? raw : String(raw);
          const fromApi = Number(fileContent.file_id);
          const fromSelection = Number(selectedFile?.id);
          const resolvedFileId =
            Number.isFinite(fromApi) && fromApi > 0
              ? fromApi
              : Number.isFinite(fromSelection) && fromSelection > 0
                ? fromSelection
                : 0;
          if (!resolvedFileId) {
            return (
              <div style={{ padding: 24, color: themeColors.textMuted }}>
                Cannot edit: missing file id. Try reopening this source.
              </div>
            );
          }
          return (
            <OriginalTextNotePanel
              fileId={resolvedFileId}
              content={textContent}
              filename={fileContent.filename || 'Text Note'}
              themeColors={themeColors}
              isDark={isDark}
              onContentUpdated={(newContent) => {
                setFileContent((prev: any) => (prev ? { ...prev, content: newContent } : prev));
              }}
              onSaved={onTextNoteContentSaved}
            />
          );
        } else if (fileType === 'web') {
          const webEmbedSrc = absoluteUrlForEmbeddedFile(fileContent.file_url || '');
          return (
            <WebOriginalPreviewWithPrint
              webEmbedSrc={webEmbedSrc}
              filename={fileContent.filename}
              themeColors={themeColors}
              isDark={isDark}
            />
          );
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
                style={{ height: '40px', paddingInline: '24px', fontSize: '14px', borderRadius: '8px' }}
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

// 音频说话人分离可视化组件（支持 Audio 和 YouTube）
const AudioDiarizationView: React.FC<{
  fileContent: any;
  themeColors: any;
  isDark: boolean;
  onSpeakerUpdated?: () => void;
  /** 转写保存成功后：刷新 Original 数据与 Markdown 列表 */
  onTranscriptPersisted?: () => void | Promise<void>;
  /** 手机端：在正文顶部展示粘性原生 audio 控件，不依赖弹窗内播放 */
  inlineAudioPlayer?: boolean;
}> = ({ fileContent, themeColors, isDark, onSpeakerUpdated, onTranscriptPersisted, inlineAudioPlayer }) => {
  const themeMode = isDark ? 'dark' : 'light';
  const [localAsrData, setLocalAsrData] = useState(fileContent.asr_with_diarization);
  const [audioPlayerVisible, setAudioPlayerVisible] = useState(false);
  const [audioPlaybackFailed, setAudioPlaybackFailed] = useState(false);
  const [editingSpeaker, setEditingSpeaker] = useState<{ id: number; currentLabel: string } | null>(null);
  const [newSpeakerLabel, setNewSpeakerLabel] = useState('');
  const [savingLabel, setSavingLabel] = useState(false);
  const [transcriptSearch, setTranscriptSearch] = useState('');
  const [editingTranscript, setEditingTranscript] = useState<{
    kind: 'audio' | 'youtube';
    index: number;
    initialText: string;
  } | null>(null);
  const [editTranscriptDraft, setEditTranscriptDraft] = useState('');
  const [savingTranscript, setSavingTranscript] = useState(false);
  const [deletingSegment, setDeletingSegment] = useState<{ kind: 'audio' | 'youtube'; index: number } | null>(null);
  const [replaceFind, setReplaceFind] = useState('');
  const [replaceWith, setReplaceWith] = useState('');
  const [replacing, setReplacing] = useState(false);

  useEffect(() => {
    setLocalAsrData(fileContent.asr_with_diarization);
  }, [fileContent.asr_with_diarization]);

  useEffect(() => {
    setAudioPlaybackFailed(false);
  }, [fileContent.file_id, fileContent.file_url]);

  const orderedAudioSentences = useMemo(() => {
    const sents = localAsrData?.sentences;
    if (!Array.isArray(sents) || sents.length === 0) return [];
    return [...sents].sort(
      (a: any, b: any) =>
        (a.begin_time || 0) - (b.begin_time || 0) ||
        (a.sentence_id ?? 0) - (b.sentence_id ?? 0)
    );
  }, [localAsrData]);

  const highlightMarkStyle: React.CSSProperties = useMemo(
    () => ({
      backgroundColor: isDark ? 'rgba(250, 173, 20, 0.4)' : '#fff566',
      color: 'inherit',
      padding: '0 2px',
      borderRadius: 2,
    }),
    [isDark]
  );

  /** 转写行内 Edit / 关闭：更小、低对比，避免抢眼 */
  const compactTranscriptActionBtn: React.CSSProperties = useMemo(
    () => ({
      flexShrink: 0,
      padding: 0,
      width: 18,
      height: 18,
      minWidth: 18,
      display: 'inline-flex',
      alignItems: 'center',
      justifyContent: 'center',
      color: themeColors.textSecondary,
    }),
    [themeColors.textSecondary]
  );
  const compactTranscriptIconSize = 10;
  const compactCloseIconSize = 9;

  const asrData = localAsrData;
  
  // 检测是 YouTube 还是 Audio 格式
  const isYouTube = asrData && asrData.transcript && !asrData.sentences;
  const isAudio = asrData && asrData.sentences;
  
  // 如果没有有效数据，显示空状态
  if (!asrData || (!isYouTube && !isAudio)) {
    return (
      <div style={{ 
        textAlign: 'center', 
        padding: '40px',
        color: themeColors.textMuted 
      }}>
        <FileTextOutlined style={{ fontSize: '48px', marginBottom: '16px', color: themeColors.textMuted }} />
        <div>No transcript data available</div>
      </div>
    );
  }
  
  // 检查数据是否为空
  if (isAudio && (!asrData.sentences || asrData.sentences.length === 0)) {
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
  
  if (isYouTube && (!asrData.transcript || asrData.transcript.length === 0)) {
    return (
      <div style={{ 
        textAlign: 'center', 
        padding: '40px',
        color: themeColors.textMuted 
      }}>
        <FileTextOutlined style={{ fontSize: '48px', marginBottom: '16px', color: themeColors.textMuted }} />
        <div>No YouTube transcript data available</div>
      </div>
    );
  }
  
  // 格式化时间（毫秒转为 MM:SS 格式）- 用于 Audio
  const formatTime = (milliseconds: number) => {
    const seconds = Math.floor(milliseconds / 1000);
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    return `${minutes.toString().padStart(2, '0')}:${remainingSeconds.toString().padStart(2, '0')}`;
  };
  
  // 格式化时间（毫秒转为 MM:SS.mmm 格式，更精确）- 用于 Audio
  const formatTimeDetailed = (milliseconds: number) => {
    const seconds = Math.floor(milliseconds / 1000);
    const ms = milliseconds % 1000;
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = seconds % 60;
    return `${minutes.toString().padStart(2, '0')}:${remainingSeconds.toString().padStart(2, '0')}.${Math.floor(ms / 100)}`;
  };
  
  // 格式化时间（秒转为 MM:SS 格式）- 用于 YouTube
  const formatYouTubeTime = (seconds: number) => {
    const minutes = Math.floor(seconds / 60);
    const remainingSeconds = Math.floor(seconds % 60);
    return `${minutes.toString().padStart(2, '0')}:${remainingSeconds.toString().padStart(2, '0')}`;
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
    let content = '';
    
    if (isYouTube) {
      // YouTube 格式
      const lines = asrData.transcript.map((segment: any) => {
        const timestamp = formatYouTubeTime(segment.start);
        return `[${timestamp}] ${segment.text}`;
      });
      content = lines.join('\n');
    } else {
      // Audio 格式
      const lines = asrData.sentences.map((s: any) => {
        const speakerLabel = asrData.speakers.find((sp: any) => sp.speaker_id === s.speaker_id)?.label || `Speaker ${s.speaker_id}`;
        const timeRange = `${formatTimeDetailed(s.begin_time)} - ${formatTimeDetailed(s.end_time)}`;
        return `${speakerLabel} [${timeRange}]:\n${s.text || ''}`;
      });
      content = lines.join('\n\n');
    }
    
    const blob = new Blob([content], { type: 'text/plain;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    const filename = isYouTube ? 
      `${(fileContent.filename || 'youtube_transcript').replace(/\.[^.]+$/, '')}_transcript.txt` :
      `${(fileContent.filename || 'dialogue').replace(/\.[^.]+$/, '')}_对话.txt`;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleDownloadAudioFile = () => {
    const url = fileContent.file_url;
    if (!url) {
      message.warning('Audio download URL is not available');
      return;
    }
    const link = document.createElement('a');
    link.href = url;
    link.download = fileContent.filename || 'audio';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    message.success(`Downloading: ${fileContent.filename || 'audio'}`);
  };

  const handlePlayAudio = () => {
    if (!fileContent.file_url) {
      message.warning('Audio URL is not available, starting download instead');
      handleDownloadAudioFile();
      return;
    }
    setAudioPlaybackFailed(false);
    setAudioPlayerVisible(true);
  };

  const handleAudioPlaybackError = () => {
    setAudioPlaybackFailed(true);
    // 不在此处自动触发下载：部分浏览器/WebView 会误报 error，自动下载会造成「播不了就变下载」
    message.warning('Could not play this audio in the browser. Use Download audio or check the file format.');
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

  const openEditTranscript = (kind: 'audio' | 'youtube', index: number, initialText: string) => {
    setEditingTranscript({ kind, index, initialText });
    setEditTranscriptDraft(initialText);
  };

  const handleCancelTranscriptEdit = () => {
    setEditingTranscript(null);
    setEditTranscriptDraft('');
  };

  const handleSaveTranscriptEdit = async () => {
    if (!editingTranscript || fileContent.file_id == null) {
      message.error('Missing file id');
      return;
    }
    setSavingTranscript(true);
    try {
      await filesAPI.updateAsrSegmentText(fileContent.file_id, {
        text: editTranscriptDraft,
        segment_kind: editingTranscript.kind,
        index: editingTranscript.index,
      });
      message.success('Transcript updated');
      handleCancelTranscriptEdit();
      await onTranscriptPersisted?.();
    } catch (error: any) {
      message.error(`Save failed: ${error.response?.data?.detail || error.message}`);
    } finally {
      setSavingTranscript(false);
    }
  };

  const handleReplaceAll = async () => {
    const findTrim = replaceFind.trim();
    if (!findTrim) {
      message.warning('Please enter text to find');
      return;
    }
    if (fileContent.file_id == null) return;
    setReplacing(true);
    try {
      const res = await filesAPI.replaceAsrText(fileContent.file_id, {
        find: findTrim,
        replace: replaceWith,
      });
      const r = res as any;
      let count: number | null = null;
      const rawCount = r?.replace_count ?? r?.replaceCount;
      if (rawCount !== undefined && rawCount !== null) {
        const n = Number(rawCount);
        if (Number.isInteger(n) && n >= 0) count = n;
      }
      if (count === null && typeof r?.message === 'string') {
        const match = r.message.match(/Replaced\s+(\d+)\s+occurrence/);
        if (match) count = parseInt(match[1], 10);
      }
      if (count !== null) {
        message.success(count > 0 ? `Replaced ${count} occurrence(s)` : 'No matches found');
      } else {
        message.success('Replace completed');
      }
      await onTranscriptPersisted?.();
    } catch (error: any) {
      const detail = error.response?.data?.detail ?? error.message ?? 'Unknown error';
      message.error(`Replace failed: ${typeof detail === 'string' ? detail : JSON.stringify(detail)}`);
    } finally {
      setReplacing(false);
    }
  };

  const handleDeleteSegment = async (kind: 'audio' | 'youtube', index: number) => {
    if (fileContent.file_id == null) return;
    setDeletingSegment({ kind, index });
    try {
      await filesAPI.deleteAsrSegment(fileContent.file_id, { segment_kind: kind, index });
      message.success('Segment deleted');
      await onTranscriptPersisted?.();
    } catch (error: any) {
      message.error(`Delete failed: ${error.response?.data?.detail || error.message}`);
    } finally {
      setDeletingSegment(null);
    }
  };

  const normSearch = transcriptSearch.trim().toLowerCase();
  const ytFiltered =
    isYouTube && asrData?.transcript
      ? asrData.transcript
          .map((segment: any, index: number) => ({ segment, index }))
          .filter(
            ({ segment }: { segment: any }) =>
              !normSearch || (segment.text || '').toLowerCase().includes(normSearch)
          )
      : [];
  const audioFiltered =
    isAudio && orderedAudioSentences.length
      ? orderedAudioSentences
          .map((sentence: any, index: number) => ({ sentence, index }))
          .filter(
            ({ sentence }: { sentence: any }) =>
              !normSearch || (sentence.text || '').toLowerCase().includes(normSearch)
          )
      : [];

  // 渲染 YouTube 格式
  if (isYouTube) {
    const totalDuration = asrData.transcript.length > 0 
      ? asrData.transcript[asrData.transcript.length - 1].start + asrData.transcript[asrData.transcript.length - 1].duration 
      : 0;
    
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
        {/* 头部信息 - YouTube */}
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
            <div style={{ display: 'flex', gap: '16px', fontSize: '13px', color: themeColors.textSecondary, flexWrap: 'wrap' }}>
              <span>video ID: {asrData.video_id}</span>
              <span>language: {asrData.language}</span>
              <span>segments: {asrData.transcript.length}</span>
              <span>duration: ~{formatYouTubeTime(totalDuration)}</span>
            </div>
            {asrData.video_url && (
              <div style={{ marginTop: '8px' }}>
                <a 
                  href={asrData.video_url} 
                  target="_blank" 
                  rel="noopener noreferrer"
                  style={{ fontSize: '12px', color: '#1890ff' }}
                >
                  <LinkOutlined style={{ marginRight: '4px' }} />
                  Watch on YouTube
                </a>
              </div>
            )}
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
        
        <Divider style={{ margin: '16px 0', borderColor: themeColors.cardBorder }} />

        <div
          style={{
            marginBottom: '12px',
            display: 'flex',
            flexDirection: 'column',
            gap: 10,
            width: '100%',
          }}
        >
          <div
            style={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: '8px',
              alignItems: 'center',
              width: '100%',
            }}
          >
            <Input
              allowClear
              prefix={<SearchOutlined style={{ color: themeColors.textMuted, fontSize: 10 }} />}
              placeholder="Search transcript..."
              value={transcriptSearch}
              onChange={(e) => setTranscriptSearch(e.target.value)}
              style={{ flex: '1 1 220px', minWidth: 200, maxWidth: '100%', fontSize: 10 }}
            />
            {normSearch ? (
              <span style={{ fontSize: 10, color: themeColors.textSecondary }}>
                {ytFiltered.length} match{ytFiltered.length === 1 ? '' : 'es'}
              </span>
            ) : null}
          </div>
          <div
            style={{
              display: 'flex',
              flexWrap: 'wrap',
              gap: '8px',
              alignItems: 'center',
              width: '100%',
            }}
          >
            <span style={{ fontSize: 10, color: themeColors.textSecondary, whiteSpace: 'nowrap' }}>Replace:</span>
            <div
              style={{
                display: 'flex',
                flex: '1 1 180px',
                gap: 6,
                minWidth: 0,
                alignItems: 'center',
              }}
            >
              <Input
                placeholder="Find"
                value={replaceFind}
                onChange={(e) => setReplaceFind(e.target.value)}
                style={{ flex: 1, minWidth: 72, fontSize: 10 }}
                allowClear
              />
              <Input
                placeholder="Replace with"
                value={replaceWith}
                onChange={(e) => setReplaceWith(e.target.value)}
                style={{ flex: 1, minWidth: 72, fontSize: 10 }}
                allowClear
              />
            </div>
            <Button
              type="default"
              size="small"
              onClick={handleReplaceAll}
              loading={replacing}
              style={{ fontSize: 10, flexShrink: 0 }}
            >
              Replace all
            </Button>
          </div>
        </div>
        
        {/* YouTube 转录内容 */}
        <div style={{ 
          fontSize: '11px', 
          fontWeight: 500, 
          color: themeColors.text,
          marginBottom: '12px'
        }}>
          transcript
        </div>
        
        <div style={{ 
          display: 'flex', 
          flexDirection: 'column', 
          gap: '12px' 
        }}>
          {ytFiltered.map(({ segment, index }: { segment: any; index: number }) => (
            <div
              key={index}
              style={{
                display: 'flex',
                gap: '12px',
                padding: '12px',
                backgroundColor: isDark ? '#252a3e' : '#ffffff',
                borderLeft: '4px solid #1890ff',
                borderRadius: '6px',
                boxShadow: isDark 
                  ? '0 1px 3px rgba(0,0,0,0.3)' 
                  : '0 1px 2px rgba(0,0,0,0.06)',
                transition: 'all 0.2s ease',
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
              {/* 时间戳 */}
              <div style={{ 
                minWidth: '50px',
                display: 'flex',
                alignItems: 'flex-start',
                paddingTop: '2px'
              }}>
                <span
                  style={{
                    fontSize: '11px',
                    color: '#1890ff',
                    fontWeight: 500,
                    fontFamily: 'monospace'
                  }}
                >
                  {formatYouTubeTime(segment.start)}
                </span>
              </div>
              
              {/* 文本 + 编辑 */}
              <div style={{ flex: 1, minWidth: 0, display: 'flex', gap: 6, alignItems: 'flex-start' }}>
                <div style={{ 
                  fontSize: '12px', 
                  color: themeColors.text,
                  lineHeight: '1.6',
                  wordBreak: 'break-word',
                  flex: 1,
                }}>
                  {highlightTranscriptText(segment.text || '', transcriptSearch, highlightMarkStyle)}
                </div>
                <div style={{ display: 'flex', alignItems: 'flex-start', gap: 0, flexShrink: 0 }}>
                <Tooltip title="Edit this segment">
                  <Button
                    type="text"
                    size="small"
                    icon={<EditOutlined style={{ fontSize: compactTranscriptIconSize }} />}
                    onClick={() => openEditTranscript('youtube', index, segment.text || '')}
                    style={compactTranscriptActionBtn}
                  />
                </Tooltip>
                <Tooltip title="Delete this segment">
                  <Button
                    type="text"
                    size="small"
                    icon={<CloseOutlined style={{ fontSize: compactCloseIconSize }} />}
                    loading={deletingSegment?.kind === 'youtube' && deletingSegment?.index === index}
                    style={compactTranscriptActionBtn}
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDeleteSegment('youtube', index);
                    }}
                  />
                </Tooltip>
                </div>
              </div>
            </div>
          ))}
        </div>

        <Modal
          title="Edit transcript"
          open={!!editingTranscript}
          onOk={handleSaveTranscriptEdit}
          onCancel={handleCancelTranscriptEdit}
          okText="Save"
          cancelText="Cancel"
          confirmLoading={savingTranscript}
          centered
          width={560}
          zIndex={1100}
          styles={getModalStyles(themeMode)}
        >
          <div style={{ marginTop: 12 }}>
            <TextArea
              rows={6}
              value={editTranscriptDraft}
              onChange={(e) => setEditTranscriptDraft(e.target.value)}
              placeholder="Transcript text..."
              style={{ fontSize: 13 }}
            />
          </div>
        </Modal>
      </div>
    );
  }
  
  // 渲染 Audio 格式（原有逻辑）
  return (
    <div
      style={{
        flex: 1,
        minHeight: 0,
        display: 'flex',
        flexDirection: 'column',
        overflow: 'hidden',
      }}
    >
      <div
        style={{
          flex: 1,
          minHeight: 0,
          overflow: 'auto',
          WebkitOverflowScrolling: 'touch',
          padding: '20px',
          border: `1px solid ${themeColors.cardBorder}`,
          borderRadius: '8px',
          backgroundColor: isDark ? '#1a1d2e' : '#fafafa',
        }}
      >
        {inlineAudioPlayer && fileContent.file_url && (
          <div
            className="original-inline-audio-player"
            style={{
              position: 'sticky',
              top: 0,
              zIndex: 4,
              margin: '-20px -20px 16px -20px',
              padding: '12px 20px',
              backgroundColor: isDark ? '#232738' : '#f5f5f5',
              borderBottom: `1px solid ${themeColors.cardBorder}`,
            }}
          >
            {!audioPlaybackFailed ? (
              <audio
                key={fileContent.file_id ?? fileContent.file_url}
                src={fileContent.file_url}
                controls
                preload="metadata"
                playsInline
                onError={handleAudioPlaybackError}
                style={{ width: '100%', display: 'block' }}
              />
            ) : (
              <div style={{ color: themeColors.textSecondary, fontSize: 12 }}>
                Playback unavailable in browser. Use Download or open the file link.
              </div>
            )}
          </div>
        )}
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
        <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
          {asrData.speakers.map((speaker: any) => (
            <Tag 
              key={speaker.speaker_id}
              color={getSpeakerColor(speaker.speaker_id)}
              style={{ 
                margin: 0,
                padding: '4px 6px',
                fontSize: '10px',
                fontWeight: 500,
                lineHeight: 1.4,
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
              {speaker.label} <EditOutlined style={{ fontSize: '9px', marginLeft: '2px' }} />
            </Tag>
          ))}
        </div>
      </div>
      
      <Divider style={{ margin: '16px 0', borderColor: themeColors.cardBorder }} />

      <div
        style={{
          marginBottom: '12px',
          display: 'flex',
          flexDirection: 'column',
          gap: 10,
          width: '100%',
        }}
      >
        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: '8px',
            alignItems: 'center',
            width: '100%',
          }}
        >
          <Input
            allowClear
            prefix={<SearchOutlined style={{ color: themeColors.textMuted, fontSize: 10 }} />}
            placeholder="Search transcript..."
            value={transcriptSearch}
            onChange={(e) => setTranscriptSearch(e.target.value)}
            style={{ flex: '1 1 220px', minWidth: 200, maxWidth: '100%', fontSize: 10 }}
          />
          {normSearch ? (
            <span style={{ fontSize: 10, color: themeColors.textSecondary }}>
              {audioFiltered.length} match{audioFiltered.length === 1 ? '' : 'es'}
            </span>
          ) : null}
        </div>
        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: '8px',
            alignItems: 'center',
            width: '100%',
          }}
        >
          <span style={{ fontSize: 10, color: themeColors.textSecondary, whiteSpace: 'nowrap' }}>Replace:</span>
          <div
            style={{
              display: 'flex',
              flex: '1 1 180px',
              gap: 6,
              minWidth: 0,
              alignItems: 'center',
            }}
          >
            <Input
              placeholder="Find"
              value={replaceFind}
              onChange={(e) => setReplaceFind(e.target.value)}
              style={{ flex: 1, minWidth: 72, fontSize: 10 }}
              allowClear
            />
            <Input
              placeholder="Replace with"
              value={replaceWith}
              onChange={(e) => setReplaceWith(e.target.value)}
              style={{ flex: 1, minWidth: 72, fontSize: 10 }}
              allowClear
            />
          </div>
          <Tooltip title="Replace all">
            <Button
              type="default"
              size="small"
              icon={<ReloadOutlined style={{ fontSize: 10 }} />}
              onClick={handleReplaceAll}
              loading={replacing}
              style={{ fontSize: 10, flexShrink: 0 }}
            />
          </Tooltip>
          <Tooltip title="download transcript">
            <Button
              type="primary"
              icon={<DownloadOutlined style={{ fontSize: 10 }} />}
              onClick={handleDownloadDialogueTxt}
              size="small"
              style={{ fontSize: 10, flexShrink: 0 }}
            />
          </Tooltip>
          <Tooltip title="Play audio">
            <Button
              type="default"
              icon={<SoundOutlined style={{ fontSize: 10 }} />}
              onClick={handlePlayAudio}
              size="small"
              disabled={!fileContent.file_url}
              style={{ fontSize: 10, flexShrink: 0 }}
            />
          </Tooltip>
        </div>
      </div>
      
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
        {audioFiltered.map(({ sentence, index }: { sentence: any; index: number }) => {
          const speakerColor = getSpeakerColor(sentence.speaker_id);
          const timeRange = `${formatTimeDetailed(sentence.begin_time)} - ${formatTimeDetailed(sentence.end_time)}`;
          
          return (
            <div
              key={sentence.sentence_id ?? index}
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
              {/* 说话人（非标签形式） */}
              <div style={{ 
                minWidth: '64px',
                display: 'flex',
                alignItems: 'flex-start',
                paddingTop: '2px'
              }}>
                <span
                  style={{
                    fontSize: '11px',
                    color: speakerColor,
                    fontWeight: 500,
                  }}
                >
                  {asrData.speakers.find((sp: any) => sp.speaker_id === sentence.speaker_id)?.label || `speaker ${sentence.speaker_id}`}
                </span>
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
                <div style={{ display: 'flex', gap: 6, alignItems: 'flex-start' }}>
                  <div style={{ 
                    fontSize: '12px', 
                    color: themeColors.text,
                    lineHeight: '1.6',
                    wordBreak: 'break-word',
                    flex: 1,
                  }}>
                    {highlightTranscriptText(sentence.text || '', transcriptSearch, highlightMarkStyle)}
                  </div>
                  <div style={{ display: 'flex', alignItems: 'flex-start', gap: 0, flexShrink: 0 }}>
                  <Tooltip title="Edit this sentence">
                    <Button
                      type="text"
                      size="small"
                      icon={<EditOutlined style={{ fontSize: compactTranscriptIconSize }} />}
                      onClick={() => openEditTranscript('audio', index, sentence.text || '')}
                      style={compactTranscriptActionBtn}
                    />
                  </Tooltip>
                  <Tooltip title="Delete this sentence">
                    <Button
                      type="text"
                      size="small"
                      icon={<CloseOutlined style={{ fontSize: compactCloseIconSize }} />}
                      loading={deletingSegment?.kind === 'audio' && deletingSegment?.index === index}
                      style={compactTranscriptActionBtn}
                      onClick={(e) => {
                        e.stopPropagation();
                        handleDeleteSegment('audio', index);
                      }}
                    />
                  </Tooltip>
                  </div>
                </div>
              </div>
            </div>
          );
        })}
      </div>

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
        centered
        zIndex={1100}
        styles={getModalStyles(themeMode)}
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

      <Modal
        title="Edit transcript"
        open={!!editingTranscript}
        onOk={handleSaveTranscriptEdit}
        onCancel={handleCancelTranscriptEdit}
        okText="Save"
        cancelText="Cancel"
        confirmLoading={savingTranscript}
        centered
        width={560}
        zIndex={1100}
        styles={getModalStyles(themeMode)}
      >
        <div style={{ marginTop: 12 }}>
          <TextArea
            rows={6}
            value={editTranscriptDraft}
            onChange={(e) => setEditTranscriptDraft(e.target.value)}
            placeholder="Transcript text..."
            style={{ fontSize: 13 }}
          />
        </div>
      </Modal>

      <Modal
        title="Play audio"
        open={audioPlayerVisible}
        onCancel={() => setAudioPlayerVisible(false)}
        footer={[
          <Button key="download" icon={<DownloadOutlined />} onClick={handleDownloadAudioFile}>
            Download audio
          </Button>,
          <Button key="close" type="primary" onClick={() => setAudioPlayerVisible(false)}>
            Close
          </Button>,
        ]}
        centered
        width={640}
        zIndex={1100}
        styles={getModalStyles(themeMode)}
      >
        {!audioPlaybackFailed ? (
          <audio
            src={fileContent.file_url}
            controls
            autoPlay
            preload="metadata"
            playsInline
            onError={handleAudioPlaybackError}
            style={{ width: '100%' }}
          />
        ) : (
          <div style={{ color: themeColors.textSecondary }}>
            Browser playback is not supported for this audio. The file download has been started.
          </div>
        )}
      </Modal>
    </div>
  );
};

// AI Analysis 查看组件
export const AIAnalysisView: React.FC<{
  execution: TaskExecution;
  themeColors: any;
  isDark: boolean;
  aiAnalysisData: TaskExecutionDetail | null;
  aiAnalysisLoading: boolean;
  onLoadAnalysis: () => Promise<void>;
  onDownloadPdf?: () => void;
  pdfDownloading?: boolean;
  onAnalyzeAudio?: (
    record: TaskExecution,
    templateVariables?: Record<string, any>,
    templateId?: number,
  ) => void;
  onAnalyzeDocument?: (record: TaskExecution, templateId: number, templateVariables?: Record<string, any>) => void;
  onAnalysisSaved?: (newContent: string) => void;
  audioAnalyzing?: boolean;
  docAnalyzing?: boolean;
  /** Mobile: tighter header row */
  mobileCompactActions?: boolean;
}> = ({
  execution,
  themeColors,
  isDark,
  aiAnalysisData,
  aiAnalysisLoading,
  onLoadAnalysis,
  onDownloadPdf,
  pdfDownloading,
  onAnalyzeAudio,
  onAnalyzeDocument,
  onAnalysisSaved,
  audioAnalyzing,
  docAnalyzing,
  mobileCompactActions = false,
}) => {
  // Phase 2: 用 UploadAnalysisTemplatePicker value 取代旧 selectedTemplateId + templateVariables
  const [analyzeDocPicker, setAnalyzeDocPicker] = useState<UploadAnalysisTemplatePickerValue>({});
  const [analyzeAudioPicker, setAnalyzeAudioPicker] = useState<UploadAnalysisTemplatePickerValue>({});
  const [reAnalyzeDocPicker, setReAnalyzeDocPicker] = useState<UploadAnalysisTemplatePickerValue>({});
  const [reAnalyzeAudioPicker, setReAnalyzeAudioPicker] = useState<UploadAnalysisTemplatePickerValue>({});
  // 保留用于向下兼容已有逻辑 (部分地方直接用 selectedTemplateId)
  const selectedTemplateId = analyzeDocPicker.templateId;
  const templateVariables = analyzeDocPicker.templateVariables || {};
  const [isEditing, setIsEditing] = useState(false);
  const [editContent, setEditContent] = useState('');
  const [saving, setSaving] = useState(false);
  const [reAnalyzeModalVisible, setReAnalyzeModalVisible] = useState(false);

  const audioLikeAnalyzeModule: 'youtube_analysis' | 'audio_analysis' =
    execution.first_file_type === 'podcast' ? 'youtube_analysis' : 'audio_analysis';

  useEffect(() => {
    setAnalyzeDocPicker({});
    setAnalyzeAudioPicker({});
    setReAnalyzeDocPicker({});
    setReAnalyzeAudioPicker({});
  }, [execution.id]);

  /** 切换 execution 时重置，避免把上一条记录的 rc 状态当成「上一次」 */
  const prevRcAgentForAnalysisRef = useRef<string | undefined>(undefined);
  useEffect(() => {
    prevRcAgentForAnalysisRef.current = undefined;
  }, [execution.id]);

  const handleStartEdit = () => {
    setEditContent(aiAnalysisData?.execution?.final_report ?? '');
    setIsEditing(true);
  };

  const handleCancelEdit = () => {
    setIsEditing(false);
    setEditContent('');
  };

  const handleSave = async () => {
    try {
      setSaving(true);
      await tasksAPI.updateExecutionFinalReport(execution.id, editContent);
      onAnalysisSaved?.(editContent);
      setIsEditing(false);
      setEditContent('');
      message.success('AI analysis report saved');
    } catch (error) {
      message.error('Failed to save report');
    } finally {
      setSaving(false);
    }
  };

  useEffect(() => {
    if (!aiAnalysisData || aiAnalysisData.execution.id !== execution.id) {
      onLoadAnalysis();
    }
  }, [execution.id, aiAnalysisData, onLoadAnalysis]);

  // 自动并行 analyze（trigger_auto_analyze）完成后仅刷新左侧列表的 rc，不会重拉 details；
  // 在 analyzing → success/failed 时强制拉取 final_report，避免右侧面板停在空状态或 Loading 占位。
  useEffect(() => {
    const prev = prevRcAgentForAnalysisRef.current;
    const cur = execution.rc_agent_status;
    prevRcAgentForAnalysisRef.current = cur;
    if (prev === 'analyzing' && (cur === 'success' || cur === 'failed')) {
      onLoadAnalysis();
    }
  }, [execution.id, execution.rc_agent_status, onLoadAnalysis]);

  const emptyStateWrapperStyle: React.CSSProperties = {
    flex: 1,
    minHeight: 0,
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    width: '100%',
  };

  if (aiAnalysisLoading) {
    return (
      <div style={emptyStateWrapperStyle}>
        <div style={{ textAlign: 'center', padding: '40px' }}>
          <LoadingOutlined style={{ fontSize: '32px', color: '#1890ff' }} />
          <div style={{ marginTop: '16px', color: themeColors.textSecondary }}>Loading AI analysis report...</div>
        </div>
      </div>
    );
  }

  // 正在分析中：显示分析进度（音频 / Podcast 或文档）
  if (audioAnalyzing || docAnalyzing) {
    const isAudio = AUDIO_LIKE_TYPES.includes(execution.first_file_type ?? '');
    return (
      <div style={emptyStateWrapperStyle}>
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
      </div>
    );
  }

  if (!aiAnalysisData?.execution?.final_report) {
    const isAudioFile = AUDIO_LIKE_TYPES.includes(execution.first_file_type ?? '');
    const canAnalyzeAudio =
      isAudioFile &&
      onAnalyzeAudio &&
      (normalizeRcAgentStatusForAnalyze(execution.rc_agent_status) === 'completed' ||
        execution.rc_agent_status === 'failed');
    // 文档分析：需要区分"仅上传"和"正在处理"
    // 1. 如果first_task_id为null且rc_agent_status为uploaded，说明仅上传未处理，允许分析
    // 2. 如果rc_agent_status为failed，允许重新分析
    // 3. completed（含旧 skipped）表示已处理但未做AI或无报告，也应显示模板与可选参数
    // 4. 如果有task_id或状态为processing/initializing等，说明正在处理，禁止重复分析
    const isProcessingDoc = execution.rc_agent_status === 'processing' || 
                            execution.rc_agent_status === 'initializing' ||
                            (execution.first_task_id != null && execution.rc_agent_status === 'uploaded');
    const rcDoc = normalizeRcAgentStatusForAnalyze(execution.rc_agent_status);
    const canAnalyzeDoc = !isAudioFile && onAnalyzeDocument && !isProcessingDoc && (
      rcDoc === 'uploaded' ||
      rcDoc === 'failed' ||
      rcDoc === 'completed'
    );

    return (
      <div style={emptyStateWrapperStyle}>
        <div style={{
          display: 'flex',
          flexDirection: 'column',
          alignItems: 'center',
          justifyContent: 'center',
          padding: '40px',
          textAlign: 'center',
        }}>
          <ExclamationCircleOutlined style={{ fontSize: '48px', color: '#faad14', marginBottom: '16px' }} />

          {canAnalyzeAudio && (
            <div style={{ maxWidth: '480px', textAlign: 'center' }}>
              <div style={{ marginBottom: '14px', color: themeColors.textSecondary, fontSize: '13px', textAlign: 'left' }}>
                {audioLikeAnalyzeModule === 'youtube_analysis'
                  ? 'Apple Podcast episodes use the same deep-analysis template as Upload YouTube Link after transcription. Use Edit my template to customize prompts, set variables when the switch is on, then click Analyze Audio.'
                  : 'Choose your private audio analysis template (optional). Use Edit my template to customize prompts, set variables when the switch is on, then click Analyze Audio.'}
              </div>
              <UploadAnalysisTemplatePicker
                value={analyzeAudioPicker}
                onChange={setAnalyzeAudioPicker}
                mode="audio-default"
                moduleCode={audioLikeAnalyzeModule}
                label={
                  audioLikeAnalyzeModule === 'youtube_analysis'
                    ? 'YouTube analysis template'
                    : 'Audio analysis template'
                }
                hint={
                  audioLikeAnalyzeModule === 'youtube_analysis'
                    ? 'When the switch is on, your private youtube_analysis template is used (same module as Upload YouTube Link). Toggle off for the system default.'
                    : 'When the switch is on, your private audio template is used. Toggle off to use the system default.'
                }
                showVariablesEditor
                textColor={themeColors.text}
                textSecondaryColor={themeColors.textSecondary}
              />
              <Button
                type="primary"
                icon={<SoundOutlined />}
                onClick={() =>
                  onAnalyzeAudio?.(
                    execution,
                    analyzeAudioPicker.templateVariables || {},
                    analyzeAudioPicker.enabled !== false ? analyzeAudioPicker.templateId : undefined,
                  )
                }
                aria-label="Analyze audio"
                style={{
                  height: '40px',
                  paddingInline: '20px',
                  borderRadius: '8px',
                  marginTop: 16,
                }}
              >
                Analyze Audio
              </Button>
            </div>
          )}
          {canAnalyzeDoc && (
            <div style={{ maxWidth: '460px', marginTop: canAnalyzeAudio ? '20px' : 0, textAlign: 'left' }}>
              <div style={{ marginBottom: '12px', color: themeColors.textSecondary, fontSize: '13px' }}>
                {execution.rc_agent_status === 'failed'
                  ? 'The previous AI analysis failed. Turn on your private template below, adjust variables if needed, then run analysis again.'
                  : 'Choose your private document analysis template below: enable the switch, set any template variables, then click Analyze Document.'}
              </div>
              <UploadAnalysisTemplatePicker
                value={analyzeDocPicker}
                onChange={setAnalyzeDocPicker}
                mode="optional"
                moduleCode="file_analysis"
                label="Document analysis template"
                hint="When the switch is on, your private template is used. Fill in variables if your template defines them, then click Analyze Document."
                showVariablesEditor
              />
              <div style={{ marginTop: '16px', textAlign: 'center' }}>
                <Button
                  type="primary"
                  icon={<RobotOutlined />}
                  disabled={!analyzeDocPicker.templateId || analyzeDocPicker.enabled === false}
                  onClick={() => {
                    if (analyzeDocPicker.templateId != null) {
                      onAnalyzeDocument?.(execution, analyzeDocPicker.templateId, analyzeDocPicker.templateVariables || {});
                    }
                  }}
                  aria-label="Analyze document"
                  style={{
                    height: '40px',
                    paddingInline: '20px',
                    borderRadius: '8px',
                  }}
                >
                  Analyze Document
                </Button>
              </div>
            </div>
          )}
        </div>
      </div>
    );
  }

  const isAudioFile = AUDIO_LIKE_TYPES.includes(execution.first_file_type ?? '');
  const isProcessingDoc = execution.rc_agent_status === 'processing' ||
    execution.rc_agent_status === 'initializing' ||
    (execution.first_task_id != null && execution.rc_agent_status === 'uploaded');
  const canReAnalyzeDoc = !isAudioFile && onAnalyzeDocument && !isProcessingDoc && !docAnalyzing;
  const canReAnalyzeAudio = isAudioFile && onAnalyzeAudio && !audioAnalyzing;

  return (
    <div style={{ display: 'flex', flexDirection: 'column', flex: 1, minHeight: 0 }}>
      <div style={{ 
        flexShrink: 0,
        marginBottom: '16px', 
        padding: '12px',
        backgroundColor: themeColors.boxMuted,
        borderRadius: '6px'
      }}>
        {mobileCompactActions ? (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 10 }}>
            <div
              style={{
                fontSize: 12,
                lineHeight: 1.5,
                color: themeColors.text,
                wordBreak: 'break-word',
              }}
            >
              Analysis Time: {new Date(aiAnalysisData.execution.created_at).toLocaleString()}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-start', gap: 12, flexWrap: 'wrap' }}>
              {!isEditing && (canReAnalyzeDoc || canReAnalyzeAudio) && (
                <Button
                  type="default"
                  size="small"
                  icon={<ReloadOutlined />}
                  onClick={() => setReAnalyzeModalVisible(true)}
                  aria-label="Re-analyze"
                >
                  Re-analyze
                </Button>
              )}
              {!isEditing && (
                <Tooltip title="Edit">
                  <Button
                    type="primary"
                    ghost
                    size="small"
                    icon={<EditOutlined />}
                    onClick={handleStartEdit}
                    aria-label="Edit"
                  />
                </Tooltip>
              )}
              {onDownloadPdf && !isEditing && (
                <Tooltip title="Export PDF">
                  <Button
                    icon={<DownloadOutlined />}
                    onClick={onDownloadPdf}
                    loading={pdfDownloading}
                    size="small"
                    className={isDark ? 'files-export-pdf-btn files-export-pdf-btn--dark' : 'files-export-pdf-btn'}
                    aria-label="Export PDF"
                  />
                </Tooltip>
              )}
            </div>
          </div>
        ) : (
          <Row gutter={16} align="middle">
            <Col span={12}>
              <Text strong>Analysis Time:</Text> {new Date(aiAnalysisData.execution.created_at).toLocaleString()}
            </Col>
            <Col span={12} style={{ display: 'flex', alignItems: 'center', justifyContent: 'flex-end', gap: 12, flexWrap: 'wrap' }}>
              {!isEditing && (canReAnalyzeDoc || canReAnalyzeAudio) && (
                <Button
                  type="default"
                  size="small"
                  icon={<ReloadOutlined />}
                  onClick={() => setReAnalyzeModalVisible(true)}
                  aria-label="Re-analyze"
                >
                  Re-analyze
                </Button>
              )}
              {!isEditing && (
                <Tooltip title="Edit">
                  <Button type="primary" ghost size="small" icon={<EditOutlined />} onClick={handleStartEdit} aria-label="Edit" />
                </Tooltip>
              )}
              {onDownloadPdf && !isEditing && (
                <Tooltip title="Export PDF">
                  <Button
                    icon={<DownloadOutlined />}
                    onClick={onDownloadPdf}
                    loading={pdfDownloading}
                    size="small"
                    className={isDark ? 'files-export-pdf-btn files-export-pdf-btn--dark' : 'files-export-pdf-btn'}
                    aria-label="Export PDF"
                  />
                </Tooltip>
              )}
            </Col>
          </Row>
        )}
      </div>

      {reAnalyzeModalVisible && (
        <div
          style={{
            position: 'fixed',
            inset: 0,
            zIndex: 1100,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            background: isDark ? 'rgba(0, 0, 0, 0.6)' : 'rgba(0, 0, 0, 0.45)',
            backdropFilter: 'blur(6px)',
            WebkitBackdropFilter: 'blur(6px)',
          }}
          onClick={() => {
            setReAnalyzeModalVisible(false);
            setReAnalyzeDocPicker({});
            setReAnalyzeAudioPicker({});
          }}
        >
          <div
            className="re-analyze-modal-content upload-files-modal"
            style={{
              width: 480,
              maxWidth: '90vw',
              maxHeight: '85vh',
              overflowY: 'auto',
              background: themeColors.cardBg ?? (isDark ? '#1a2234' : '#fff'),
              color: themeColors.text,
              borderRadius: isDark ? CARD_STYLE_BEHANCE_DARK.borderRadius : CARD_STYLE_BEHANCE.borderRadius,
              boxShadow: isDark ? CARD_STYLE_BEHANCE_DARK.boxShadow : CARD_STYLE_BEHANCE.boxShadow,
              border: isDark ? CARD_STYLE_BEHANCE_DARK.border : `1px solid ${themeColors.cardBorder}`,
            }}
            onClick={(e) => e.stopPropagation()}
          >
            <div style={{
              padding: '16px 20px',
              borderBottom: `1px solid ${themeColors.cardBorder}`,
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
            }}>
              <Space>
                <ReloadOutlined style={{ color: themeColors.selectedBorder }} />
                <span style={{ fontWeight: 600, fontSize: 16 }}>Re-analyze with different template</span>
              </Space>
              <Button
                type="text"
                size="small"
                icon={<CloseOutlined />}
                onClick={() => {
                  setReAnalyzeModalVisible(false);
                  setReAnalyzeDocPicker({});
                  setReAnalyzeAudioPicker({});
                }}
              />
            </div>
            <div style={{ padding: '20px' }}>
              {canReAnalyzeDoc && (
                <div style={{ marginBottom: canReAnalyzeAudio ? 24 : 0 }}>
                  <div style={{ marginBottom: 12, color: themeColors.textSecondary, fontSize: 13 }}>
                    Re-run AI analysis on this document. Enable your private template, set variables if needed, then
                    confirm below.
                  </div>
                  <UploadAnalysisTemplatePicker
                    value={reAnalyzeDocPicker}
                    onChange={setReAnalyzeDocPicker}
                    mode="optional"
                    moduleCode="file_analysis"
                    label="Document analysis template"
                    hint="When the switch is on, your private template is used. Adjust variables if your template defines them, then click Re-analyze Document."
                    showVariablesEditor
                  />
                  <Button
                    type="primary"
                    icon={<RobotOutlined />}
                    disabled={!reAnalyzeDocPicker.templateId || reAnalyzeDocPicker.enabled === false}
                    block
                    style={{ marginTop: 16 }}
                    onClick={() => {
                      if (reAnalyzeDocPicker.templateId != null && onAnalyzeDocument) {
                        onAnalyzeDocument(execution, reAnalyzeDocPicker.templateId, reAnalyzeDocPicker.templateVariables || {});
                        setReAnalyzeModalVisible(false);
                        setReAnalyzeDocPicker({});
                      }
                    }}
                    aria-label="Re-analyze document"
                  >
                    Re-analyze Document
                  </Button>
                </div>
              )}
              {canReAnalyzeAudio && (
                <div>
                  <div style={{ marginBottom: 12, color: themeColors.textSecondary, fontSize: 13 }}>
                    {audioLikeAnalyzeModule === 'youtube_analysis'
                      ? 'Re-run analysis using your YouTube-style deep template (same module as Upload YouTube Link), adjust variables, then confirm.'
                      : 'Re-run audio analysis. Adjust your private template and variables below, then confirm.'}
                  </div>
                  <UploadAnalysisTemplatePicker
                    value={reAnalyzeAudioPicker}
                    onChange={setReAnalyzeAudioPicker}
                    mode="audio-default"
                    moduleCode={audioLikeAnalyzeModule}
                    label={
                      audioLikeAnalyzeModule === 'youtube_analysis'
                        ? 'YouTube analysis template'
                        : 'Audio analysis template'
                    }
                    hint={
                      audioLikeAnalyzeModule === 'youtube_analysis'
                        ? 'When the switch is on, your private youtube_analysis template is used. Toggle off for the system default.'
                        : 'When the switch is on, your private audio template is used. Toggle off to use the system default.'
                    }
                    showVariablesEditor
                    textColor={themeColors.text}
                    textSecondaryColor={themeColors.textSecondary}
                  />
                  <Button
                    type="primary"
                    icon={<SoundOutlined />}
                    block
                    style={{ marginTop: 16 }}
                    onClick={() => {
                      if (onAnalyzeAudio) {
                        onAnalyzeAudio(
                          execution,
                          reAnalyzeAudioPicker.templateVariables || {},
                          reAnalyzeAudioPicker.enabled !== false ? reAnalyzeAudioPicker.templateId : undefined,
                        );
                        setReAnalyzeModalVisible(false);
                        setReAnalyzeAudioPicker({});
                      }
                    }}
                    aria-label="Re-analyze audio"
                  >
                    Re-analyze Audio
                  </Button>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
      
      {isEditing ? (
        <div
          style={{
            flex: 1,
            minHeight: 0,
            display: 'flex',
            flexDirection: 'column',
            color: themeColors.text,
          }}
        >
          <Input.TextArea
            value={editContent}
            onChange={(e) => setEditContent(e.target.value)}
            placeholder="AI analysis report content (Markdown)..."
            style={{
              flex: 1,
              minHeight: 280,
              fontFamily: 'monospace',
              fontSize: 13,
              background: themeColors.codeBlockBg,
              borderColor: themeColors.cardBorder,
              color: themeColors.text,
            }}
            className={isDark ? 'files-content-textarea-dark' : ''}
          />
          <Space style={{ marginTop: 12 }}>
            <Tooltip title="Save">
              <Button
                type="primary"
                onClick={handleSave}
                loading={saving}
                icon={<CheckOutlined />}
                aria-label="Save"
              />
            </Tooltip>
            <Tooltip title="Cancel">
              <span>
                <Button onClick={handleCancelEdit} disabled={saving} icon={<CloseOutlined />} aria-label="Cancel" />
              </span>
            </Tooltip>
          </Space>
        </div>
      ) : (
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
      )}
    </div>
  );
};

export default FilesPage; 