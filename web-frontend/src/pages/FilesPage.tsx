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
  Tabs
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
  InfoCircleOutlined,
  WarningOutlined,
  RobotOutlined,
  FolderOutlined
} from '@ant-design/icons';
import { useAuthStore } from '../store/authStore';
import { filesAPI } from '../services/files';
import { tasksAPI, TaskExecutionDetail } from '../services/tasks';
import { getErrorMessage } from '../utils/errorHandler';
import { useGlobalLogger } from '../contexts/LoggerContext';

const { Title, Text } = Typography;
const { Dragger } = Upload;
const { Option } = Select;
const { TabPane } = Tabs;

interface FileItem {
  id: string;
  name: string;
  path: string;
  size: number;
  type: string;
  status: 'ready' | 'pending' | 'processing' | 'completed' | 'failed';
  error_message?: string;
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
  const [showParams, setShowParams] = useState(false);
  const [refreshingStatus, setRefreshingStatus] = useState(false);
  const [stoppingTask, setStoppingTask] = useState(false);
  const [updatingStatus, setUpdatingStatus] = useState(false);
  const [updatingFiles, setUpdatingFiles] = useState(false);
  
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
  
  
  // 文件内容查看相关状态
  const [fileContentVisible, setFileContentVisible] = useState(false);
  const [selectedFileContent, setSelectedFileContent] = useState<any>(null);
  const [fileContentLoading, setFileContentLoading] = useState(false);
  
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

  // 获取日志级别图标
  const getLogLevelIcon = (level: string) => {
    switch (level.toLowerCase()) {
      case 'error':
        return <ExclamationCircleOutlined style={{ color: '#ff4d4f' }} />;
      case 'warning':
        return <WarningOutlined style={{ color: '#faad14' }} />;
      case 'info':
        return <InfoCircleOutlined style={{ color: '#1890ff' }} />;
      default:
        return <InfoCircleOutlined style={{ color: '#1890ff' }} />;
    }
  };

  // 获取日志级别背景色
  const getLogLevelColor = (level: string) => {
    switch (level.toLowerCase()) {
      case 'error':
        return '#ffebe6';
      case 'warning':
        return '#fff7e6';
      case 'info':
        return '#e6f7ff';
      default:
        return '#f5f5f5';
    }
  };

  // 自动滚动到底部
  useEffect(() => {
    if (logger.autoScroll && logContainerRef.current) {
      logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
    }
  }, [logger.logs, logger.autoScroll]);

  // 加载任务执行历史的函数
  const loadTaskExecutionsFunc = async () => {
    try {
      console.log('开始加载任务执行历史...');
      const executions = await tasksAPI.getTaskExecutions();
      setTaskExecutions(executions);
    } catch (error: any) {
      console.error('加载任务执行历史失败:', error);
      
      let errorMessage = '加载任务执行历史失败';
      if (error.response?.status === 401) {
        errorMessage += ' - 认证失败，请重新登录';
      } else if (error.response?.status === 403) {
        errorMessage += ' - 权限不足';
      } else if (error.response?.status >= 500) {
        errorMessage += ' - 服务器内部错误';
      } else if (error.message === 'Network Error') {
        errorMessage += ' - 网络连接失败，请检查后端服务是否运行';
      } else {
        errorMessage += ` - ${error.response?.data?.detail || error.message}`;
      }
      
      // 只在非自动刷新时显示错误消息，避免频繁弹窗
      if (!error.message?.includes('auto-refresh')) {
        message.error(errorMessage);
      }
      logger.logError(errorMessage, 'file-processing', error);
    }
  };

  // 加载API密钥等关键配置
  const loadDefaultConfig = async () => {
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
  };

  // 加载RC Agent和执行历史
  useEffect(() => {
    loadTaskTypes();
    loadTaskExecutionsFunc();
    loadDefaultConfig();
    
    // 设置定时刷新执行历史列表
    const refreshInterval = setInterval(() => {
      // 添加标记，避免自动刷新时显示错误消息
      const originalError = Error.prototype.toString;
      Error.prototype.toString = function() {
        return this.message + ' - auto-refresh';
      };
      
      loadTaskExecutionsFunc();
      
      // 恢复原始错误处理
      setTimeout(() => {
        Error.prototype.toString = originalError;
      }, 100);
    }, 30000); // 每30秒自动刷新一次执行历史列表
    
    return () => {
      clearInterval(refreshInterval);
    };
  }, []);

  // 自动更新文件状态的函数
  const updateFileStatuses = useCallback(async (taskId: string) => {
    if (updatingFiles) {
      console.log('跳过文件状态更新 - 上一个请求仍在进行中');
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
      console.error('更新文件状态失败:', error);
    } finally {
      setUpdatingFiles(false);
    }
  }, [updatingFiles]);

  // 优化的任务状态更新函数
  const updateTaskStatus = useCallback(async () => {
    if (!currentTaskId || updatingStatus) {
      if (updatingStatus) {
        console.log('跳过任务状态更新 - 上一个请求仍在进行中');
      }
      return; // 防止并发请求
    }
    
    setUpdatingStatus(true);
    try {
      const status = await filesAPI.getTaskStatus(currentTaskId);
      
      // 同步日志到新的日志系统
      if (status.logs && status.logs.length > 0) {
        // 只添加新的日志条目，避免重复
        const existingLogIds = new Set(logger.logs.map(log => log.id));
        status.logs.forEach((log, index) => {
          const logId = `task-${currentTaskId}-${index}`;
          if (!existingLogIds.has(logId)) {
            logger.actions.addLog({
              level: log.level as any,
              message: log.message,
              source: 'file-processing',
              details: { taskId: currentTaskId }
            });
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
          message.success('任务处理完成');
        } else {
          message.error('任务处理失败');
          logger.logError('任务处理失败', 'file-processing');
        }
        
        // 任务完成后刷新执行历史列表
        try {
          const executions = await tasksAPI.getTaskExecutions();
          setTaskExecutions(executions);
        } catch (error) {
          console.error('刷新执行历史失败:', error);
          logger.logError('刷新执行历史失败', 'file-processing', error);
        }
        
        // 延迟一段时间后再次刷新，确保数据同步
        setTimeout(async () => {
          try {
            const executions = await tasksAPI.getTaskExecutions();
            setTaskExecutions(executions);
          } catch (error) {
            console.error('延迟刷新执行历史失败:', error);
            logger.logError('延迟刷新执行历史失败', 'file-processing', error);
          }
        }, 5000); // 等待5秒后再次刷新，确保数据完全同步
      }
    } catch (error) {
      console.error('获取任务状态失败:', error);
      logger.logError(`获取任务状态失败: ${error}`, 'file-processing');
    } finally {
      setUpdatingStatus(false);
    }
  }, [currentTaskId, updateFileStatuses, logger, updatingStatus]);

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

  const loadTaskTypes = async () => {
    try {
      console.log('开始加载RC Agent...');
      const types = await tasksAPI.getTaskTypes();
      setTaskTypes(types);
      // 设置默认RC Agent
      const defaultTask = types.find(t => t.is_default);
      if (defaultTask) {
        setSelectedTaskType(defaultTask.id);
      }
    } catch (error: any) {
      console.error('加载RC Agent失败:', error);
      
      // 显示详细的错误信息
      let errorMessage = '加载RC Agent失败';
      if (error.response?.status === 401) {
        errorMessage += ' - 认证失败，请重新登录';
      } else if (error.response?.status === 403) {
        errorMessage += ' - 权限不足';
      } else if (error.response?.status === 404) {
        errorMessage += ' - API端点不存在';
      } else if (error.response?.status >= 500) {
        errorMessage += ' - 服务器内部错误';
      } else if (error.message === 'Network Error') {
        errorMessage += ' - 网络连接失败，请检查后端服务是否运行';
      } else {
        errorMessage += ` - ${error.response?.data?.detail || error.message}`;
      }
      
      message.error(errorMessage);
      logger.logError(errorMessage, 'file-processing', error);
      
      // 在控制台输出详细错误信息
      console.error('详细错误信息:', {
        status: error.response?.status,
        statusText: error.response?.statusText,
        data: error.response?.data,
        config: error.config,
        message: error.message
      });
    }
  };



  const handleViewExecution = async (execution: TaskExecution) => {
    try {
      setSelectedExecution(execution);
      setExecutionDetailVisible(true);
      setExecutionDetailLoading(true);
      
      logger.logInfo(`查看任务执行详情: ${execution.task_name}`, 'file-processing');
      
      // 获取包含文件详情的完整执行信息
      const details = await tasksAPI.getTaskExecutionDetails(execution.id);
      setExecutionDetailData(details);
      
    } catch (error) {
      message.error('获取执行详情失败');
      logger.logError(`获取执行详情失败: ${execution.task_name}`, 'file-processing', error);
      setExecutionDetailVisible(false);
    } finally {
      setExecutionDetailLoading(false);
    }
  };

  const handleViewAiAnalysis = async (execution: TaskExecution) => {
    try {
      setAiAnalysisLoading(true);
      setAiAnalysisVisible(true);
      
      logger.logInfo(`获取AI分析报告: ${execution.task_name}`, 'file-processing');
      
      const details = await tasksAPI.getTaskExecutionDetails(execution.id);
      setAiAnalysisData(details);
      
    } catch (error) {
      message.error('获取AI分析报告失败');
      logger.logError(`获取AI分析报告失败: ${execution.task_name}`, 'file-processing', error);
      setAiAnalysisVisible(false);
    } finally {
      setAiAnalysisLoading(false);
    }
  };


  const handleViewFileContent = (fileData: any) => {
    setSelectedFileContent(fileData);
    setFileContentVisible(true);
    logger.logInfo(`查看AI处理内容: ${fileData.filename}`, 'file-processing');
  };

  const handleViewOriginalFile = async (fileData: any) => {
    try {
      const originalFileInfo = await filesAPI.getOriginalFileInfo(fileData.id);
      logger.logInfo(`查看原始文件: ${fileData.filename}`, 'file-processing');
      
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
        message.success(`正在下载文件: ${originalFileInfo.filename}`);
      }
    } catch (error) {
      console.error('Failed to get original file info:', error);
      message.error('获取原始文件信息失败');
    }
  };

  const handleShowOriginalFiles = async (execution: any) => {
    try {
      logger.logInfo(`查看任务原始文件: ${execution.task_name}`, 'file-processing');
      
      // 获取该执行任务的文件详情
      const details = await tasksAPI.getTaskExecutionDetails(execution.id);
      
      // 过滤出有原始文件路径的文件
      const filesWithOriginal = details.files.filter(file => file.original_file_path);
      
      if (filesWithOriginal.length === 0) {
        message.info('该任务没有可查看的原始文件');
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
      message.error('获取任务文件失败');
    }
  };

  const handleDeleteExecution = (execution: TaskExecution) => {
    setExecutionToDelete(execution);
    setDeleteExecutionVisible(true);
    logger.logInfo(`准备删除任务执行: ${execution.task_name}`, 'file-processing');
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
      console.error('删除执行记录失败:', error);
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
          console.error('批量删除执行记录失败:', error);
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
        message.success('文件状态已刷新');
      } else {
        message.info('没有可刷新的任务');
        logger.logWarning('没有可刷新的任务', 'file-processing');
      }
    } catch (error) {
      console.error('刷新文件状态失败:', error);
      message.error('刷新文件状态失败，请稍后重试');
      logger.logError('刷新文件状态失败', 'file-processing', error);
    } finally {
      setRefreshingStatus(false);
    }
  };

  // 批量文件上传状态
  const [pendingFiles, setPendingFiles] = useState<File[]>([]);
  const [uploadingFiles, setUploadingFiles] = useState(false);

  const handleUpload = (file: any) => {
    // 收集待上传的文件，不立即上传
    setPendingFiles(prev => [...prev, file]);
    return false; // 阻止自动上传
  };

  // 处理批量文件上传
  const processPendingFiles = async (filesToUpload: File[]) => {
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
        status: 'ready' as const
      }));
      setFiles(prev => [...prev, ...newFiles]);
      message.success(`成功上传 ${uploadedFiles.length} 个文件`);
    } catch (error: any) {
      console.error('文件上传失败:', error);
      const errorMessage = getErrorMessage(error, '文件上传失败');
      message.error(errorMessage);
      logger.logError(errorMessage, 'file-processing', error);
    } finally {
      setUploadingFiles(false);
    }
  };

  // 当有新文件添加到待上传列表时，延迟处理以支持批量上传
  useEffect(() => {
    if (pendingFiles.length > 0) {
      const timer = setTimeout(() => {
        processPendingFiles([...pendingFiles]);
        setPendingFiles([]);
      }, 100); // 100ms 延迟，允许多个文件同时选择

      return () => clearTimeout(timer);
    }
  }, [pendingFiles]);

  const handleRemoveFile = (fileId: string) => {
    const fileName = files.find(f => f.id === fileId)?.name;
    setFiles(prev => prev.filter(f => f.id !== fileId));
    logger.logInfo(`移除文件: ${fileName}`, 'file-processing');
  };

  const handleMoveFile = (fileId: string, direction: 'up' | 'down') => {
    setFiles(prev => {
      const index = prev.findIndex(f => f.id === fileId);
      if (index === -1) return prev;
      
      const newFiles = [...prev];
      const targetIndex = direction === 'up' ? index - 1 : index + 1;
      
      if (targetIndex >= 0 && targetIndex < newFiles.length) {
        [newFiles[index], newFiles[targetIndex]] = [newFiles[targetIndex], newFiles[index]];
        logger.logInfo(`文件位置调整: ${newFiles[targetIndex].name} ${direction === 'up' ? '上移' : '下移'}`, 'file-processing');
      }
      
      return newFiles;
    });
  };

  const handleStartProcessing = async () => {
    if (!selectedTaskType) {
      message.error('请选择RC Agent');
      logger.logWarning('未选择RC Agent', 'file-processing');
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
      
      logger.logInfo(`开始处理 ${files.length} 个文件`, 'file-processing');
      
      // 将所有文件状态设置为ready
      setFiles(prev => prev.map(file => ({ ...file, status: 'ready' as const })));
      
      // 清空文件错误信息
      setFiles(prev => prev.map(file => ({ ...file, error_message: undefined })));
      
      const response = await filesAPI.processFiles({
        file_paths: files.map(f => f.path),
        task_type: selectedTaskType,
        custom_inputs: customInputs,
        params: processingParams
      });
      
      setCurrentTaskId(response.task_id);
      message.success('文件处理任务已启动');
      
      // 任务启动后立即刷新执行历史列表
      setTimeout(() => {
        loadTaskExecutionsFunc();
      }, 1000); // 延迟1秒后刷新，确保任务状态已更新
    } catch (error: any) {
      setProcessing(false);
      console.error('启动文件处理失败:', error);
      const errorMessage = getErrorMessage(error, '启动文件处理失败');
      message.error(errorMessage);
      logger.logError(`启动文件处理失败: ${errorMessage}`, 'file-processing', error);
    }
  };

  const handleStopProcessing = async () => {
    if (!currentTaskId || stoppingTask) return;
    
    setStoppingTask(true);
    try {
      await filesAPI.stopTask(currentTaskId);
      message.info('已发送停止请求');
      logger.logInfo('已发送停止任务请求', 'file-processing');
      
      // 停止任务后，等待一段时间再刷新执行历史列表
      setTimeout(async () => {
        try {
          const executions = await tasksAPI.getTaskExecutions();
          setTaskExecutions(executions);
        } catch (error) {
          console.error('任务停止后刷新执行历史失败:', error);
          logger.logError('任务停止后刷新执行历史失败', 'file-processing', error);
        }
      }, 3000); // 等待3秒后刷新，确保任务状态已更新
      
    } catch (error: any) {
      console.error('停止任务失败:', error);
      const errorMessage = getErrorMessage(error, '停止任务失败');
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
      logger.logInfo(`切换RC Agent: ${taskType.name}`, 'file-processing');
    }
  };

  // 新增：重新处理函数
  const handleReprocess = () => {
    setShowResult(false);
    setProcessingResult(null);
    setFiles(prev => prev.map(file => ({ ...file, status: 'ready' as const })));
    logger.logInfo('重新处理文件', 'file-processing');
    
          // 重新处理时也刷新执行历史列表
      setTimeout(() => {
        loadTaskExecutionsFunc();
      }, 1000); // 延迟1秒后刷新，确保状态已更新
  };

  // 新增：清除文件列表函数
  const handleClearFiles = () => {
    setFiles([]);
    setShowResult(false);
    setProcessingResult(null);
    setLastCompletedTaskId(null);
    logger.logInfo('清空文件列表', 'file-processing');
    
          // 清除文件列表时也刷新执行历史列表
      setTimeout(() => {
        loadTaskExecutionsFunc();
      }, 1000); // 延迟1秒后刷新，确保状态已更新
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
                placeholder={`请输入${field.name}`}
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

  const getStatusIcon = (status: string) => {
    switch (status) {
      case 'ready':
      case 'pending':
        return <ClockCircleOutlined style={{ color: '#d9d9d9' }} />;
      case 'completed':
        return <CheckCircleOutlined style={{ color: '#52c41a' }} />;
      case 'failed':
        return <ExclamationCircleOutlined style={{ color: '#ff4d4f' }} />;
      case 'processing':
        return <ClockCircleOutlined style={{ color: '#1890ff' }} />;
      default:
        return <ClockCircleOutlined style={{ color: '#d9d9d9' }} />;
    }
  };

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
      render: (_: any, record: FileItem, index: number) => (
        <Space>
          <Tooltip title="Move Up">
            <Button
              type="text"
              icon={<UpOutlined />}
              onClick={() => handleMoveFile(record.id, 'up')}
              disabled={index === 0 || processing}
              size="small"
            />
          </Tooltip>
          <Tooltip title="Move Down">
            <Button
              type="text"
              icon={<DownOutlined />}
              onClick={() => handleMoveFile(record.id, 'down')}
              disabled={index === files.length - 1 || processing}
              size="small"
            />
          </Tooltip>
          <Tooltip title="Delete">
            <Button
              type="text"
              danger
              icon={<DeleteOutlined />}
              onClick={() => handleRemoveFile(record.id)}
              disabled={processing}
              size="small"
            />
          </Tooltip>
        </Space>
      )
    }
  ];

  const executionColumns = [
    {
      title: 'Agent Name',
      dataIndex: 'task_name',
      key: 'task_name'
    },
    {
      title: 'User',
      dataIndex: 'username',
      key: 'username'
    },
    {
      title: 'Time',
      dataIndex: 'created_at',
      key: 'created_at',
      render: (date: string) => new Date(date).toLocaleString()
    },
    {
      title: 'Files Status',
      key: 'files_stats',
      align: 'center' as const,
      render: (_: any, record: TaskExecution) => (
        <Space>
          <span style={{ color: '#00b96b', fontSize: '14px' }}>
            <CheckCircleOutlined /> {record.files_processed}
          </span>
          <span style={{ color: '#ff7875', fontSize: '14px' }}>
            <ExclamationCircleOutlined /> {record.files_failed}
          </span>
        </Space>
      )
    },
    {
      title: 'RC Agent Status',
      dataIndex: 'rc_agent_status',
      key: 'rc_agent_status',
      align: 'center' as const,
      render: (status: string) => {
        if (!status) return <span style={{ color: '#bfbfbf', fontSize: '16px' }}><ExclamationCircleOutlined /></span>;
        
        const statusConfig = {
          success: { color: '#00b96b', icon: <CheckCircleOutlined /> },
          failed: { color: '#ff7875', icon: <ExclamationCircleOutlined /> },
          pending: { color: '#40a9ff', icon: <ClockCircleOutlined /> },
          skipped: { color: '#bfbfbf', icon: <ExclamationCircleOutlined /> }
        };
        
        const config = statusConfig[status as keyof typeof statusConfig] || { color: '#bfbfbf', icon: <ExclamationCircleOutlined /> };
        return (
          <span style={{ color: config.color, fontSize: '16px' }}>
            {config.icon}
          </span>
        );
      }
    },
    {
      title: 'Processing Time',
      dataIndex: 'total_time',
      key: 'total_time',
      render: (time: number) => `${time?.toFixed(2)}s`
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (_: any, record: TaskExecution) => (
        <Space>
          <Button
            type="text"
            icon={<RobotOutlined />}
            onClick={() => handleViewAiAnalysis(record)}
            size="small"
          >
            AI Analysis
          </Button>
          <Button
            type="text"
            icon={<FileTextOutlined />}
            onClick={() => handleShowOriginalFiles(record)}
            size="small"
          >
            Original Files
          </Button>
          <Button
            type="text"
            icon={<EyeOutlined />}
            onClick={() => handleViewExecution(record)}
            size="small"
          >
            Details
          </Button>
          <Button
            type="text"
            danger
            icon={<DeleteOutlined />}
            onClick={() => handleDeleteExecution(record)}
            size="small"
            loading={deletingExecutions}
          >
          </Button>
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
                <div><strong>Agent Name:</strong> {selectedExecution.task_name}</div>
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

  const renderAiAnalysisModal = () => (
    <Modal
      title="AI Analysis Report"
      open={aiAnalysisVisible}
      onCancel={() => {
        setAiAnalysisVisible(false);
        setAiAnalysisData(null);
      }}
      footer={null}
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
            <Tag color="blue">{aiAnalysisData.execution.task_name}</Tag>
            <Text style={{ marginLeft: 16 }}>
              <Text strong>Generated: </Text>
              {new Date(aiAnalysisData.execution.created_at).toLocaleString()}
            </Text>
          </div>
          <Divider />
          <div style={{ 
            background: '#f6f8fa', 
            padding: '16px', 
            borderRadius: '8px', 
            border: '1px solid #e1e4e8',
            maxHeight: '60vh',
            overflow: 'auto'
          }}>
            <pre style={{ 
              whiteSpace: 'pre-wrap', 
              wordWrap: 'break-word',
              margin: 0,
              fontFamily: 'inherit',
              fontSize: '14px',
              lineHeight: '1.6'
            }}>
              {aiAnalysisData.execution.final_report}
            </pre>
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
      <div className="page-content">
        <Row gutter={24} style={{ marginTop: 16 }}>
          <Col span={16}>
            {/* 根据状态显示不同的内容 */}

            {/* 文件上传区域 - 只在非完成状态显示 */}
            {getUIState() !== 'completed' && (
              <Card title="File Upload" className="mb-16" size="small">
                <Dragger
                  beforeUpload={handleUpload}
                  showUploadList={false}
                  multiple
                  disabled={processing || uploadingFiles}
                  style={{ padding: '20px 0' }}
                >
                  <p className="ant-upload-drag-icon" style={{ marginBottom: 8 }}>
                    <InboxOutlined style={{ fontSize: '36px' }} />
                  </p>
                  <p className="ant-upload-text" style={{ marginBottom: 4 }}>
                    {uploadingFiles ? 'Uploading files...' : 'Click or drag files to this area to upload'}
                  </p>
                  <p className="ant-upload-hint" style={{ marginBottom: 0 }}>
                    {uploadingFiles ? 'Please wait while files are being uploaded' : 'Support PDF, Excel, Word, image formats and more. Multiple files supported.'}
                  </p>
                </Dragger>
              </Card>
            )}

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
                extra={getUIState() === 'completed' ? (
                  <Button
                    icon={<ReloadOutlined />}
                    onClick={refreshFileStatus}
                    loading={refreshingStatus}
                    size="small"
                  >
                    Refresh Status
                  </Button>
                ) : null}
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
          </Col>

          <Col span={8}>
            {/* AI Agent选择 - 始终显示 */}
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
                  <div style={{ textAlign: 'center', color: '#999', padding: '20px' }}>
                    No AI Agents available
                  </div>
                )}
              </Form>
              
              {renderCustomInputs()}
            </Card>

            {/* 操作按钮 - 根据状态显示不同的操作 */}
            <Card title="Operation Control" className="mb-12" size="small">
              <Space style={{ width: '100%' }} direction="vertical">
                {getUIState() === 'ready' && (
                  <>
                    <Button
                      type="primary"
                      icon={<PlayCircleOutlined />}
                      onClick={handleStartProcessing}
                      loading={processing}
                      disabled={files.length === 0 || !selectedTaskType}
                      block
                    >
                      Start Processing
                    </Button>
                    
                    <Button
                      icon={<SettingOutlined />}
                      onClick={() => setShowParams(true)}
                      disabled={processing}
                      block
                    >
                      Parameter Settings
                    </Button>
                  </>
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
        </Row>

        {/* 执行历史部分 */}
        <div style={{ marginTop: 12 }}>
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
                pageSize: 10,
                showSizeChanger: false,
                showQuickJumper: false,
                showTotal: (total, range) => `${range[0]}-${range[1]} of ${total} items`,
                simple: true
              }}
            />
          </Card>
        </div>

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
                <p><strong>Task Name:</strong> {executionToDelete.task_name}</p>
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
                <Tag color="blue">{selectedExecutionForFiles.task_name}</Tag>
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
      </div>
    </div>
  );
};

export default FilesPage; 