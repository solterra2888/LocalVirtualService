/**
 * Investment Themes Management Page
 * Display and manage investment theme vector retrieval functionality
 */

import React, { useState, useEffect } from 'react';
import {
  Card,
  Table,
  Button,
  Space,
  Tag,
  Typography,
  Row,
  Col,
  message,
  Modal,
  Input,
  Select,
  Divider,
  Progress,
  Tooltip,
  Dropdown,
  Menu,
  Alert,
  Switch,
  Form,
  Upload
} from 'antd';
import {
  ReloadOutlined,
  SearchOutlined,
  SyncOutlined,
  FundOutlined,
  SettingOutlined,
  ExperimentOutlined,
  WarningOutlined,
  InfoCircleOutlined,
  DownOutlined,
  EditOutlined,
  DeleteOutlined,
  CheckCircleOutlined,
  StopOutlined,
  PlusOutlined,
  RobotOutlined,
  UploadOutlined,
  InboxOutlined,
  DownloadOutlined
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

import {
  listThemes,
  initializeThemeEmbeddings,
  matchNewsToThemes,
  fillThemeDescriptions,
  pollTaskUntilComplete,
  updateThemeEnabledStatus,
  updateThemeDescription,
  deleteTheme,
  batchDeleteThemes,
  generateThemeDescription,
  createTheme,
  batchUploadThemes,
  getThemeAssociations,
  verifyTicker,
  getAllGroups,
  getAllUsers,
  updateThemeTickers,
  updateThemeWithConflictResolution
} from '../services/investmentThemes';
import {
  InvestmentTheme,
  ThemeMatch,
  ThemeAssociation,
  getDescriptionSourceLabel
} from '../types/investmentThemes';
import { getErrorMessage } from '../utils/errorHandler';
import { useAuthStore } from '../store/authStore';
import ThemeConflictModal from '../components/ThemeConflictModal';
import BatchThemeConflictModal from '../components/BatchThemeConflictModal';

const { Title, Text, Paragraph } = Typography;
const { TextArea, Search } = Input;

interface PageState {
  loading: boolean;
  themes: InvestmentTheme[];
  total: number;
  currentPage: number;
  pageSize: number;
  initializing: boolean;
  testModalVisible: boolean;
  testNewsText: string;
  testResults: ThemeMatch[];
  testLoading: boolean;
  editModalVisible: boolean;
  editingTheme: InvestmentTheme | null;
  editDescription: string;
  editGroup: string;
  editLoading: boolean;
  selectedRowKeys: React.Key[];
  createModalVisible: boolean;
  createStep: number;
  newThemeGroup: string;
  newThemeName: string;
  newThemeCategory: string;
  newThemeMarket: string;
  newThemeTickers: string;
  newThemeDescription: string;
  generatingDescription: boolean;
  creatingTheme: boolean;
  // 冲突解决相关
  conflictModalVisible: boolean;
  conflictExistingTheme: any;
  conflictNewTickers: string[];
  // 批量上传冲突
  batchConflictModalVisible: boolean;
  batchConflicts: any[];
  conflictResolving: boolean;
  // 批量上传相关
  batchUploadModalVisible: boolean;
  uploadingFile: boolean;
  uploadedFile: File | null;
  batchUploadGroup: string;  // Group 分组（必填）
  batchUploadMarket: string;  // market 选择
  uploadPreview: {
    theme_name: string;  // 主题名称（整个文件中唯一）
    total_themes: number;  // 要创建的主题数量（= category 数量）
    total_tickers: number;
    categories: Record<string, number>;  // category -> ticker count
    categories_sample: string[];  // 前5个category
  } | null;
  batchTaskId: string | null;
  batchTaskProgress: number;
  batchTaskStatus: string;
  batchTaskDetails: any[];
  // 主题关联查看
  associationModalVisible: boolean;
  associationLoading: boolean;
  associationTheme: InvestmentTheme | null;
    associationData: ThemeAssociation[];
    verifyingTickers: Set<string>;  // 正在验证的tickers
    searchKeyword: string;
    selectedGroups: string[];  // 选中的 Group 筛选值（支持多选）
    availableGroups: string[];  // 所有可用的 groups（从 API 获取）
    loadingGroups: boolean;  // 是否正在加载 groups
    selectedUsers: string[];  // 选中的 User 筛选值（支持多选，admin专用）
    availableUsers: string[];  // 所有可用的 users（从 API 获取）
    loadingUsers: boolean;  // 是否正在加载 users
}

const InvestmentThemesPage: React.FC = () => {
  const { user } = useAuthStore();
  const isAdmin = user?.is_admin === true;
  
  const [state, setState] = useState<PageState>({
    loading: false,
    themes: [],
    total: 0,
    currentPage: 1,
    pageSize: 50,
    initializing: false,
    testModalVisible: false,
    testNewsText: '',
    testResults: [],
    testLoading: false,
    editModalVisible: false,
    editingTheme: null,
    editDescription: '',
    editGroup: '',
    editLoading: false,
    selectedRowKeys: [],
    createModalVisible: false,
    createStep: 1,
    newThemeGroup: '',
    newThemeName: '',
    newThemeCategory: '',
    newThemeMarket: 'US',
    newThemeTickers: '',
    newThemeDescription: '',
    generatingDescription: false,
    creatingTheme: false,
    // 冲突解决相关
    conflictModalVisible: false,
    conflictExistingTheme: null,
    conflictNewTickers: [],
    conflictResolving: false,
    // 批量上传冲突
    batchConflictModalVisible: false,
    batchConflicts: [],
    // 批量上传相关
    batchUploadModalVisible: false,
    uploadingFile: false,
    uploadedFile: null,
    batchUploadGroup: '',  // 默认为空，必须用户填写
    batchUploadMarket: 'US',  // 默认 US 市场
    uploadPreview: null,
    batchTaskId: null,
    batchTaskProgress: 0,
    batchTaskStatus: '',
    batchTaskDetails: [],
    associationModalVisible: false,
    associationLoading: false,
    associationTheme: null,
    associationData: [],
    verifyingTickers: new Set(),
    searchKeyword: '',
    selectedGroups: [],
    availableGroups: [],
    loadingGroups: false,
    selectedUsers: [],
    availableUsers: [],
    loadingUsers: false
  });


  // Load theme list
  const loadThemes = async (page?: number, pageSize?: number, searchKeyword?: string, groups?: string[], users?: string[]) => {
    setState(prev => ({ ...prev, loading: true }));
    try {
      const currentPage = page || state.currentPage;
      const currentPageSize = pageSize || state.pageSize;
      const effectiveSearch = searchKeyword !== undefined ? searchKeyword : state.searchKeyword;
      const effectiveGroups = groups !== undefined ? groups : state.selectedGroups;
      const effectiveUsers = users !== undefined ? users : state.selectedUsers;
      const response = await listThemes(
        currentPageSize,
        (currentPage - 1) * currentPageSize,
        effectiveSearch,
        effectiveGroups.length > 0 ? effectiveGroups : undefined,
        isAdmin && effectiveUsers.length > 0 ? effectiveUsers : undefined
      );
      
      setState(prev => ({
        ...prev,
        themes: response.themes,
        total: response.total,
        currentPage: currentPage,
        pageSize: currentPageSize,
        loading: false
      }));
    } catch (error) {
      message.error(`Failed to load theme list: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, loading: false }));
    }
  };

  const handleSearchThemes = (value: string) => {
    const trimmed = value.trim();
    setState(prev => ({
      ...prev,
      searchKeyword: trimmed,
      currentPage: 1
    }));
    loadThemes(1, state.pageSize, trimmed, state.selectedGroups, state.selectedUsers);
  };
  
  const handleGroupFilterChange = (groups: string[]) => {
    setState(prev => ({
      ...prev,
      selectedGroups: groups,
      currentPage: 1
    }));
    loadThemes(1, state.pageSize, state.searchKeyword, groups, state.selectedUsers);
  };
  
  const handleUserFilterChange = (users: string[]) => {
    setState(prev => ({
      ...prev,
      selectedUsers: users,
      currentPage: 1
    }));
    loadThemes(1, state.pageSize, state.searchKeyword, state.selectedGroups, users);
  };

  const handleSearchInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const { value } = event.target;
    setState(prev => ({
      ...prev,
      searchKeyword: value
    }));

    if (value === '') {
      loadThemes(1, state.pageSize, '', state.selectedGroups, state.selectedUsers);
    }
  };
  
  // 加载所有可用的 groups（从 API 获取，不受分页限制）
  const loadAvailableGroups = async () => {
    if (state.loadingGroups || state.availableGroups.length > 0) {
      return; // 已加载或正在加载，跳过
    }
    
    setState(prev => ({ ...prev, loadingGroups: true }));
    try {
      const response = await getAllGroups();
      if (response.success) {
        setState(prev => ({
          ...prev,
          availableGroups: response.groups || [],
          loadingGroups: false
        }));
      } else {
        setState(prev => ({ ...prev, loadingGroups: false }));
      }
    } catch (error) {
      console.error('Failed to load groups:', error);
      setState(prev => ({ ...prev, loadingGroups: false }));
    }
  };
  
  // 加载所有可用的 users（从 API 获取，不受分页限制，仅admin用户）
  const loadAvailableUsers = async () => {
    if (!isAdmin) {
      return; // 非admin用户不需要加载
    }
    if (state.loadingUsers || state.availableUsers.length > 0) {
      return; // 已加载或正在加载，跳过
    }
    
    setState(prev => ({ ...prev, loadingUsers: true }));
    try {
      const response = await getAllUsers();
      if (response.success) {
        setState(prev => ({
          ...prev,
          availableUsers: response.users || [],
          loadingUsers: false
        }));
      } else {
        setState(prev => ({ ...prev, loadingUsers: false }));
      }
    } catch (error) {
      console.error('Failed to load users:', error);
      setState(prev => ({ ...prev, loadingUsers: false }));
    }
  };
  
  // 组件加载时获取所有 groups 和 users
  useEffect(() => {
    loadAvailableGroups();
    if (isAdmin) {
      loadAvailableUsers();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []); // 只在组件首次加载时执行

  const showAssociationsModal = async (theme: InvestmentTheme) => {
    setState(prev => ({
      ...prev,
      associationModalVisible: true,
      associationLoading: true,
      associationTheme: theme,
      associationData: []
    }));

    try {
      const response = await getThemeAssociations(theme.theme_id);
      setState(prev => ({
        ...prev,
        associationLoading: false,
        associationData: response.associations
      }));
    } catch (error) {
      message.error(`Failed to load theme associations: ${getErrorMessage(error)}`);
      setState(prev => ({
        ...prev,
        associationLoading: false
      }));
    }
  };

  const handleCloseAssociationsModal = () => {
    setState(prev => ({
      ...prev,
      associationModalVisible: false,
      associationLoading: false,
      associationTheme: null,
      associationData: [],
      verifyingTickers: new Set()
    }));
  };

  // 验证单个ticker
  const handleVerifyTicker = React.useCallback(async (ticker: string) => {
    console.log('🔍 handleVerifyTicker called with ticker:', ticker);
    console.log('🔍 Current associationTheme:', state.associationTheme);
    
    if (!state.associationTheme) {
      console.warn('⚠️ No associationTheme found, returning');
      return;
    }

    console.log('✅ Starting verification for:', ticker, 'theme_id:', state.associationTheme.theme_id);

    // 添加到验证中状态
    setState(prev => ({
      ...prev,
      verifyingTickers: new Set(prev.verifyingTickers).add(ticker)
    }));

    try {
      console.log('📡 Calling verifyTicker API...');
      const response = await verifyTicker(state.associationTheme.theme_id, ticker);
      console.log('📡 API response:', response);

      if (response.success) {
        message.success(response.message);

        // 更新association数据中的is_verified状态
        setState(prev => {
          const newVerifyingTickers = new Set(prev.verifyingTickers);
          newVerifyingTickers.delete(ticker);

          return {
            ...prev,
            associationData: prev.associationData.map(item =>
              item.ticker === ticker
                ? { ...item, is_verified: true }
                : item
            ),
            verifyingTickers: newVerifyingTickers
          };
        });

        // 刷新主题列表（更新verified_tickers计数）
        loadThemes();
      } else {
        message.error(response.message);

        // 从验证中状态移除
        setState(prev => {
          const newVerifyingTickers = new Set(prev.verifyingTickers);
          newVerifyingTickers.delete(ticker);
          return { ...prev, verifyingTickers: newVerifyingTickers };
        });
      }
    } catch (error) {
      message.error(`验证失败: ${getErrorMessage(error)}`);

      // 从验证中状态移除
      setState(prev => {
        const newVerifyingTickers = new Set(prev.verifyingTickers);
        newVerifyingTickers.delete(ticker);
        return { ...prev, verifyingTickers: newVerifyingTickers };
      });
    }
  }, [state.associationTheme, loadThemes]);

  const associationColumns = React.useMemo<ColumnsType<ThemeAssociation>>(() => [
    {
      title: 'Ticker',
      dataIndex: 'ticker',
      key: 'ticker',
      render: (ticker: string) => <Text strong>{ticker}</Text>
    },
    {
      title: 'Market',
      dataIndex: 'market',
      key: 'market',
      render: (market?: string | null) => market || '-'
    },
    {
      title: 'Verified',
      dataIndex: 'is_verified',
      key: 'is_verified',
      render: (isVerified?: boolean | null) => (
        <Tag color={isVerified ? 'green' : 'default'}>
          {isVerified ? 'Yes' : 'No'}
        </Tag>
      )
    },
    {
      title: 'Action',
      key: 'action',
      width: 120,
      render: (_: any, record: ThemeAssociation) => {
        if (record.is_verified) {
          return <Text type="secondary">-</Text>;
        }
        
        const isVerifying = state.verifyingTickers.has(record.ticker);
        
        return (
          <Tooltip title="Verify ticker and fetch data from EODHD if needed">
            <Button
              type="link"
              size="small"
              icon={isVerifying ? <SyncOutlined spin /> : <CheckCircleOutlined />}
              onClick={() => handleVerifyTicker(record.ticker)}
              loading={isVerifying}
              disabled={isVerifying}
            >
              {isVerifying ? 'Verifying' : 'Verify'}
            </Button>
          </Tooltip>
        );
      }
    }
  ], [state.verifyingTickers, handleVerifyTicker]);

  const associationDataSource = React.useMemo(
    () =>
      state.associationData.map((item, index) => ({
        key: `${item.ticker}-${index}`,
        ...item
      })),
    [state.associationData]
  );

  // 🌟 Sync Theme Data (merged functionality - using Celery async tasks)
  const handleSyncThemes = async () => {
    Modal.confirm({
      title: 'Sync Theme Data',
      icon: <SyncOutlined />,
      width: 600,
      content: (
        <div>
          <Alert
            message="This operation will complete the following tasks"
            description={
              <div style={{ marginTop: '12px' }}>
                <div style={{ marginBottom: '8px' }}>
                  Generate detailed descriptions for new themes (using LLM)
                </div>
                <div style={{ marginBottom: '8px' }}>
                  Generate vectors for new themes (incremental update, skip existing)
                </div>
              </div>
            }
            type="info"
            showIcon
            style={{ marginBottom: '16px' }}
          />
          <Paragraph type="secondary">
            Estimated Time: 2-10 minutes (background async execution using Celery)
          </Paragraph>
        </div>
      ),
      okText: 'Start Sync',
      cancelText: 'Cancel',
      onOk: async () => {
        try {
          // Step 1: Submit description generation task
          message.loading({ content: 'Step 1/2: Submitting theme description generation task...', key: 'sync', duration: 0 });
          const descTask = await fillThemeDescriptions(false);
          
          if (!descTask.success) {
            message.destroy('sync');
            message.error(`Failed to submit description task: ${descTask.message}`);
            return;
          }
          
          // Poll description task
          message.loading({ content: 'Step 1/2: Generating theme descriptions (0%)...', key: 'sync', duration: 0 });
          const descResult = await pollTaskUntilComplete(
            descTask.task_id,
            (status) => {
              const progressText = status.message || `Progress: ${status.progress}%`;
              message.loading({ 
                content: `Step 1/2: ${progressText}`, 
                key: 'sync', 
                duration: 0 
              });
            }
          );
          
          if (descResult.status === 'FAILURE') {
            message.destroy('sync');
            message.error(`Description generation failed: ${descResult.error || 'Unknown error'}`);
            return;
          }
          
          // Step 2: Submit vector generation task
          message.loading({ content: 'Step 2/2: Submitting theme vector generation task...', key: 'sync', duration: 0 });
          const vectorTask = await initializeThemeEmbeddings(false);
          
          if (!vectorTask.success) {
            message.destroy('sync');
            message.warning(`Descriptions generated, but failed to submit vector task: ${vectorTask.message}`);
            return;
          }
          
          // Poll vector task
          message.loading({ content: 'Step 2/2: Generating theme vectors (0%)...', key: 'sync', duration: 0 });
          const vectorResult = await pollTaskUntilComplete(
            vectorTask.task_id,
            (status) => {
              const progressText = status.message || `Progress: ${status.progress}%`;
              message.loading({ 
                content: `Step 2/2: ${progressText}`, 
                key: 'sync', 
                duration: 0 
              });
            }
          );
          
          message.destroy('sync');
          
          if (vectorResult.status === 'FAILURE') {
            message.warning(`Descriptions generated, but vector generation failed: ${vectorResult.error || 'Unknown error'}`);
          } else {
            const descStats = descResult.result?.statistics || descResult.result || {};
            const vectorStats = vectorResult.result?.statistics || vectorResult.result || {};
            message.success({
              content: `🎉 Sync completed! Descriptions: ${(descStats as any).processed || 0}/${(descStats as any).total || 0}, Vectors: ${(vectorStats as any).processed || 0}/${(vectorStats as any).total || 0}`,
              duration: 6,
            });
            
            // Refresh data
            setTimeout(() => {
              loadThemes();
            }, 2000);
          }
        } catch (error) {
          message.destroy('sync');
          message.error(`Sync failed: ${getErrorMessage(error)}`);
        }
      }
    });
  };

  // 🌟 Optimize: Enhance Description Quality (fill descriptions only)
  const handleOptimizeDescriptions = async () => {
    Modal.confirm({
      title: 'Optimize Description Quality',
      icon: <ExperimentOutlined />,
      width: 600,
      content: (
        <div>
          <Alert
            message="Use LLM to enhance theme description quality"
            description={
              <div style={{ marginTop: '12px' }}>
                <div style={{ marginBottom: '8px' }}>
                  Generate detailed descriptions for themes
                </div>
                <div>
                  Upgrade all to LLM-generated detailed descriptions
                </div>
              </div>
            }
            type="success"
            showIcon
            style={{ marginBottom: '16px' }}
          />
          <Paragraph type="secondary">
            Use Case: When description coverage is low or many simple template descriptions
          </Paragraph>
        </div>
      ),
      okText: 'Start Optimization',
      cancelText: 'Cancel',
      onOk: async () => {
        try {
          // Submit task
          message.loading({ content: 'Submitting description optimization task...', key: 'optimize', duration: 0 });
          const task = await fillThemeDescriptions(false);
          
          if (!task.success) {
            message.destroy('optimize');
            message.error(`Failed to submit task: ${task.message}`);
            return;
          }
          
          // Poll task
          message.loading({ content: 'Optimizing theme descriptions (0%)...', key: 'optimize', duration: 0 });
          const result = await pollTaskUntilComplete(
            task.task_id,
            (status) => {
              const progressText = status.message || `Progress: ${status.progress}%`;
              message.loading({ 
                content: `Optimizing: ${progressText}`, 
                key: 'optimize', 
                duration: 0 
              });
            }
          );
          
          message.destroy('optimize');
          
          if (result.status === 'FAILURE') {
            message.error(`Optimization failed: ${result.error || 'Unknown error'}`);
          } else {
            const stats = result.result?.statistics || result.result || {};
            message.success({
              content: `🎉 Optimization completed! Processed: ${(stats as any).processed || 0}/${(stats as any).total || 0}`,
              duration: 5,
            });
            
            // Refresh data
            setTimeout(() => {
              loadThemes();
            }, 2000);
          }
        } catch (error) {
          message.destroy('optimize');
          message.error(`Optimization failed: ${getErrorMessage(error)}`);
        }
      }
    });
  };

  // 🚨 Dangerous Operation: Force Update All Vectors
  const handleForceUpdateVectors = async () => {
    Modal.confirm({
      title: 'Warning: Force Update All Vectors',
      icon: <WarningOutlined style={{ color: '#ff4d4f' }} />,
      width: 650,
      content: (
        <div>
          <Alert
            message="This is a high-risk, high-cost operation"
            description={
              <div style={{ marginTop: '12px' }}>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  Will regenerate vectors for <Text strong style={{ color: '#ff4d4f' }}>all themes</Text> (150+)
                </div>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  Estimated Time: <Text strong>20-30 minutes</Text>
                </div>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  Will repeatedly call LLM to generate descriptions (consuming large API quota)
                </div>
              </div>
            }
            type="error"
            showIcon
            style={{ marginBottom: '16px' }}
          />
          <Divider />
          <Paragraph>
            <Text strong>Applicable Scenarios:</Text>
          </Paragraph>
          <ul style={{ paddingLeft: '20px', marginTop: '8px' }}>
            <li>Embedding model upgrade (e.g., text-embedding-v2 → v3)</li>
            <li>Vector dimension change</li>
          </ul>
          <Paragraph style={{ marginTop: '12px' }}>
            <Text strong>Not Applicable Scenarios:</Text>
          </Paragraph>
          <ul style={{ paddingLeft: '20px', marginTop: '8px' }}>
            <li>Daily maintenance (please use "Sync Theme Data")</li>
            <li>Adding few new themes (please use "Sync Theme Data")</li>
          </ul>
          <Alert
            message="Are you sure you want to continue?"
            type="warning"
            showIcon
            style={{ marginTop: '16px' }}
          />
        </div>
      ),
      okText: 'Yes, I am sure',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk: async () => {
        setState(prev => ({ ...prev, initializing: true }));
        try {
          // Submit task
          message.loading({ content: 'Submitting force update task...', key: 'forceUpdate', duration: 0 });
          const task = await initializeThemeEmbeddings(true);
          
          if (!task.success) {
            message.destroy('forceUpdate');
            message.error(`Failed to submit task: ${task.message}`);
            setState(prev => ({ ...prev, initializing: false }));
            return;
          }
          
          // Poll task
          message.loading({ content: 'Force updating all vectors (0%)...', key: 'forceUpdate', duration: 0 });
          const result = await pollTaskUntilComplete(
            task.task_id,
            (status) => {
              const progressText = status.message || `Progress: ${status.progress}%`;
              message.loading({ 
                content: `Force updating: ${progressText}`, 
                key: 'forceUpdate', 
                duration: 0 
              });
            }
          );
          
          message.destroy('forceUpdate');
          
          if (result.status === 'FAILURE') {
            message.error(`Update failed: ${result.error || 'Unknown error'}`);
          } else {
            const stats = result.result?.statistics || result.result || {};
            message.success(
              `Force update completed! Processed: ${(stats as any).processed || 0}, ` +
              `Failed: ${(stats as any).failed || 0}`
            );
            
            // Refresh data
            await loadThemes();
          }
        } catch (error) {
          message.destroy('forceUpdate');
          message.error(`Update failed: ${getErrorMessage(error)}`);
        } finally {
          setState(prev => ({ ...prev, initializing: false }));
        }
      }
    });
  };

  // 🚨 Dangerous Operation: Regenerate All Descriptions
  const handleForceRegenerateDescriptions = async () => {
    Modal.confirm({
      title: 'Warning: Regenerate All Descriptions',
      icon: <WarningOutlined style={{ color: '#ff4d4f' }} />,
      width: 650,
      content: (
        <div>
          <Alert
            message="This is a high-cost operation"
            description={
              <div style={{ marginTop: '12px' }}>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  Will call LLM to regenerate <Text strong style={{ color: '#ff4d4f' }}>all descriptions</Text> (150+)
                </div>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  Estimated Time: <Text strong>15-25 minutes</Text>
                </div>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  LLM API calling cost is high
                </div>
              </div>
            }
            type="error"
            showIcon
            style={{ marginBottom: '16px' }}
          />
          <Divider />
          <Paragraph>
            <Text strong>Applicable Scenarios:</Text>
          </Paragraph>
          <ul style={{ paddingLeft: '20px', marginTop: '8px' }}>
            <li>After major Prompt Template optimization</li>
            <li>Found historical description quality is generally poor</li>
          </ul>
          <Paragraph style={{ marginTop: '12px' }}>
            <Text strong>Not Applicable Scenarios:</Text>
          </Paragraph>
          <ul style={{ paddingLeft: '20px', marginTop: '8px' }}>
            <li>Only few descriptions missing (please use "Optimize Description Quality")</li>
            <li>Daily maintenance (please use "Optimize Description Quality")</li>
          </ul>
          <Alert
            message="Are you sure you want to continue?"
            type="warning"
            showIcon
            style={{ marginTop: '16px' }}
          />
        </div>
      ),
      okText: 'Yes, I am sure',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk: async () => {
        try {
          // Submit task
          message.loading({ content: 'Submitting force regenerate task...', key: 'forceRegen', duration: 0 });
          const task = await fillThemeDescriptions(true);
          
          if (!task.success) {
            message.destroy('forceRegen');
            message.error(`Failed to submit task: ${task.message}`);
            return;
          }
          
          // Poll task
          message.loading({ content: 'Regenerating all descriptions (0%)...', key: 'forceRegen', duration: 0 });
          const result = await pollTaskUntilComplete(
            task.task_id,
            (status) => {
              const progressText = status.message || `Progress: ${status.progress}%`;
              message.loading({ 
                content: `Regenerating: ${progressText}`, 
                key: 'forceRegen', 
                duration: 0 
              });
            }
          );
          
          message.destroy('forceRegen');
          
          if (result.status === 'FAILURE') {
            message.error(`Operation failed: ${result.error || 'Unknown error'}`);
          } else {
            const stats: any = result.result?.statistics || result.result || {};
            message.success({
              content: `🎉 All descriptions regenerated! Processed: ${stats.processed || 0}/${stats.total || 0}`,
              duration: 5,
            });
            
            setTimeout(() => {
              loadThemes();
            }, 2000);
          }
        } catch (error) {
          message.destroy('forceRegen');
          message.error(`Operation failed: ${getErrorMessage(error)}`);
        }
      }
    });
  };

  // Test theme matching
  const handleTestMatch = async () => {
    if (!state.testNewsText.trim()) {
      message.warning('Please enter news text');
      return;
    }

    setState(prev => ({ ...prev, testLoading: true }));
    try {
      const results = await matchNewsToThemes({
        news_text: state.testNewsText,
        top_k: 10,
        min_similarity: 0.5
      });

      setState(prev => ({
        ...prev,
        testResults: results,
        testLoading: false
      }));

      if (results.length === 0) {
        message.info('No matching themes found (similarity < 0.5)');
      } else {
        message.success(`Found ${results.length} matching themes`);
      }
    } catch (error) {
      message.error(`Matching failed: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, testLoading: false }));
    }
  };

  // Initial load (execute only once)
  useEffect(() => {
    loadThemes();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // View description details
  const showDescriptionModal = (theme: InvestmentTheme) => {
    const sourceLabel = getDescriptionSourceLabel(theme.description_source);
    Modal.info({
      title: `${theme.theme_name} Theme Description`,
      width: 800,
      okText: 'Close',
      content: (
        <div>
          <Paragraph>
            <Text strong>Description Source:</Text>
            <Tag color={sourceLabel.color}>{sourceLabel.text}</Tag>
          </Paragraph>
          {theme.last_manual_edit_at && (
            <Paragraph>
              <Text strong>Last Edited:</Text>
              <Text type="secondary">{new Date(theme.last_manual_edit_at).toLocaleString('en-US')} by {theme.last_manual_edit_by}</Text>
            </Paragraph>
          )}
          <Divider />
          <Paragraph style={{ fontSize: '14px', lineHeight: '1.8' }}>
            {theme.description_preview || <Text type="secondary">No description available</Text>}
          </Paragraph>
        </div>
      ),
    });
  };

  // Handle enable/disable theme toggle (Admin only)
  const handleToggleEnabled = async (theme: InvestmentTheme) => {
    if (!isAdmin) {
      message.error('Only administrators can perform this operation');
      return;
    }

    const newStatus = !theme.is_enabled_for_matching;
    
    try {
      await updateThemeEnabledStatus(theme.theme_id, newStatus);
      message.success(`Theme ${newStatus ? 'enabled' : 'disabled'}`);
      loadThemes(state.currentPage);
    } catch (error) {
      message.error(`Failed to update status: ${getErrorMessage(error)}`);
    }
  };

  // Handle edit description
  const handleEditDescription = (theme: InvestmentTheme) => {
    // Check if user has permission to edit
    if (!isAdmin && theme.created_by !== user?.username) {
      message.error('You can only edit themes you created');
      return;
    }

    setState(prev => ({
      ...prev,
      editModalVisible: true,
      editingTheme: theme,
      editDescription: theme.description_preview || '',
      editGroup: theme.group || ''
    }));
  };

  // Submit edit description
  const handleSubmitEdit = async () => {
    if (!state.editingTheme) return;

    if (!state.editDescription.trim()) {
      message.error('Description cannot be empty');
      return;
    }

    setState(prev => ({ ...prev, editLoading: true }));

    try {
      // 始终发送 group 值（即使是空字符串，表示清空）
      // 这样后端可以正确处理清空操作
      const result = await updateThemeDescription(
        state.editingTheme.theme_id,
        state.editDescription,
        state.editingTheme.version,
        state.editGroup.trim()  // 发送空字符串表示清空，非空字符串表示更新
      );

      if (result.description_saved) {
        if (result.embedding_updated) {
          message.success('Description, group, and vector updated successfully');
        } else {
          message.warning(`Description and group saved, but vector update failed: ${result.error || 'Unknown error'}`);
        }
        
        setState(prev => ({
          ...prev,
          editModalVisible: false,
          editingTheme: null,
          editDescription: '',
          editGroup: '',
          editLoading: false
        }));
        
        loadThemes(state.currentPage);
      } else {
        message.error(result.message || 'Update failed');
        setState(prev => ({ ...prev, editLoading: false }));
      }
    } catch (error) {
      message.error(`Update failed: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, editLoading: false }));
    }
  };

  // Handle delete theme
  const handleDeleteTheme = (theme: InvestmentTheme) => {
    // Check if user has permission to delete
    if (!isAdmin && theme.created_by !== user?.username) {
      message.error('You can only delete themes you created');
      return;
    }

    Modal.confirm({
      title: 'Confirm Delete Theme?',
      icon: <WarningOutlined style={{ color: '#ff4d4f' }} />,
      width: 600,
      content: (
        <div>
          <Alert
            type="error"
            message="⚠️ Permanent Deletion - This operation CANNOT be undone!"
            description="The theme will be permanently deleted from the database."
            style={{ marginBottom: 16 }}
          />
          <Paragraph>
            <Text strong>Theme ID:</Text> {theme.theme_id}
          </Paragraph>
          <Paragraph>
            <Text strong>Theme Name:</Text> {theme.theme_name}
          </Paragraph>
          <Paragraph type="danger">
            This will permanently delete:
            <ul>
              <li>Theme vector data (investment_theme_embeddings)</li>
              <li>All news matching records (news_theme_matches)</li>
            </ul>
          </Paragraph>
        </div>
      ),
      okText: 'Confirm Delete',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk: async () => {
        try {
          const result = await deleteTheme(theme.theme_id);
          
          if (result.success) {
            message.success(result.message + ` (deleted ${result.deleted_matches_count} matching records)`);
            loadThemes(state.currentPage);
          } else {
            message.error(result.message);
          }
        } catch (error: any) {
          const errorMsg = getErrorMessage(error);
          console.error('Delete theme error:', error);
          message.error(`Delete failed: ${errorMsg}`);
        }
      }
    });
  };

  // Handle batch delete themes
  const handleBatchDelete = () => {
    if (state.selectedRowKeys.length === 0) {
      message.warning('Please select themes to delete');
      return;
    }

    // Find selected themes details
    const selectedThemes = state.themes.filter(theme => 
      state.selectedRowKeys.includes(theme.theme_id)
    );
    
    // Check permissions for non-admin users
    if (!isAdmin) {
      const unauthorizedThemes = selectedThemes.filter(
        theme => theme.created_by !== user?.username
      );
      
      if (unauthorizedThemes.length > 0) {
        message.error(`You can only delete your own themes. ${unauthorizedThemes.length} theme(s) selected are not created by you.`);
        return;
      }
    }

    Modal.confirm({
      title: `Confirm Batch Delete ${selectedThemes.length} Themes?`,
      icon: <WarningOutlined style={{ color: '#ff4d4f' }} />,
      width: 700,
      content: (
        <div>
          <Alert
            type="error"
            message="⚠️ Permanent Batch Deletion - This operation CANNOT be undone!"
            description="All selected themes will be permanently deleted from the database."
            style={{ marginBottom: 16 }}
          />
          <Paragraph>
            <Text strong>Selected Themes: {selectedThemes.length}</Text>
          </Paragraph>
          <div style={{ maxHeight: '300px', overflowY: 'auto', marginBottom: 16 }}>
            {selectedThemes.map(theme => (
              <div key={theme.theme_id} style={{ padding: '8px', background: '#f5f5f5', marginBottom: '4px', borderRadius: '4px' }}>
                <Text strong>{theme.theme_name}</Text>
                {' '}
                <Text type="secondary">({theme.theme_id})</Text>
              </div>
            ))}
          </div>
          <Paragraph type="danger">
            This will permanently delete:
            <ul>
              <li>All selected theme vector data (investment_theme_embeddings)</li>
              <li>All related news matching records (news_theme_matches)</li>
            </ul>
          </Paragraph>
        </div>
      ),
      okText: 'Confirm Batch Delete',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk: async () => {
        try {
          const themeIds = selectedThemes.map(theme => theme.theme_id);

          const result = await batchDeleteThemes(themeIds);
          
          if (result.success || result.deleted_count > 0) {
            if (result.failed_count > 0) {
              Modal.info({
                title: 'Batch Delete Completed with Issues',
                width: 700,
                content: (
                  <div>
                    <Paragraph>
                      <Text type="success" strong>Successfully deleted: {result.deleted_count} themes</Text>
                    </Paragraph>
                    <Paragraph>
                      <Text type="danger" strong>Failed: {result.failed_count} themes</Text>
                    </Paragraph>
                    <Paragraph>
                      <Text>Total matching records deleted: {result.total_matches_deleted}</Text>
                    </Paragraph>
                    {result.details && result.details.filter(d => !d.success).length > 0 && (
                      <div>
                        <Divider />
                        <Text strong>Failed Items:</Text>
                        <ul>
                          {result.details.filter(d => !d.success).map((detail, idx) => (
                            <li key={idx}>
                              {detail.theme_name || detail.theme_id}: {detail.message}
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}
                  </div>
                )
              });
            } else {
              message.success(`Successfully deleted ${result.deleted_count} themes (${result.total_matches_deleted} matching records)`);
            }
            
            // Clear selection and reload
            setState(prev => ({ ...prev, selectedRowKeys: [] }));
            loadThemes(state.currentPage);
          } else {
            message.error(result.message);
          }
        } catch (error: any) {
          const errorMsg = getErrorMessage(error);
          console.error('Batch delete error:', error);
          message.error(`Batch delete failed: ${errorMsg}`);
        }
      }
    });
  };

  // Handle create theme - Step 1: Generate description
  const handleGenerateDescription = async () => {
    if (!state.newThemeName.trim()) {
      message.error('Please enter theme name');
      return;
    }
    
    if (!state.newThemeCategory.trim()) {
      message.error('Please enter category');
      return;
    }

    setState(prev => ({ ...prev, generatingDescription: true }));

    try {
      // 解析 tickers（逗号分隔）
      const tickersArray = state.newThemeTickers
        ? state.newThemeTickers.split(',').map(t => t.trim()).filter(Boolean)
        : undefined;
      
      const result = await generateThemeDescription(
        state.newThemeName, 
        state.newThemeCategory,
        state.newThemeGroup.trim() || undefined, // group is now optional
        undefined, // market parameter removed - users can input tickers with suffixes
        tickersArray
      );
      
      if (result.success) {
        setState(prev => ({
          ...prev,
          newThemeDescription: result.description,
          createStep: 2,
          generatingDescription: false
        }));
        message.success('Description generated successfully');
      } else {
        message.error(result.message || 'Failed to generate description');
        setState(prev => ({ ...prev, generatingDescription: false }));
      }
    } catch (error) {
      message.error(`Generate description failed: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, generatingDescription: false }));
    }
  };

  // Handle create theme - Step 2: Create theme with description
  const handleCreateTheme = async () => {
    if (!state.newThemeDescription.trim()) {
      message.error('Please enter description');
      return;
    }

    setState(prev => ({ ...prev, creatingTheme: true }));

    try {
      // 解析 tickers（逗号分隔）
      const tickersArray = state.newThemeTickers
        ? state.newThemeTickers.split(',').map(t => t.trim()).filter(Boolean)
        : undefined;
      
      console.log('📤 Creating theme with data:', {
        themeName: state.newThemeName,
        category: state.newThemeCategory,
        tickers: tickersArray,
        group: state.newThemeGroup
      });
      
      const result = await createTheme(
        state.newThemeName,
        state.newThemeCategory,
        state.newThemeDescription,
        state.newThemeGroup.trim() || undefined, // group is now optional
        undefined, // theme_id is auto-generated, no need to pass
        undefined, // market parameter removed - users can input tickers with suffixes
        tickersArray
      );
      
      console.log('📥 Create theme result:', result);
      
      // 检查是否有冲突
      if (result.conflict && result.existing_theme) {
        console.log('⚠️ Conflict detected! Showing conflict resolution modal');
        console.log('Existing theme:', result.existing_theme);
        console.log('New tickers:', result.new_tickers);
        
        // 显示冲突解决Modal
        setState(prev => ({
          ...prev,
          creatingTheme: false,
          conflictModalVisible: true,
          conflictExistingTheme: result.existing_theme,
          conflictNewTickers: result.new_tickers || []
        }));
        return;
      }
      
      if (result.theme_saved) {
        if (result.embedding_created) {
          message.success(`Theme '${state.newThemeName}' created successfully!`);
        } else {
          message.warning(`Theme saved, but vector generation failed: ${result.error || 'Unknown error'}`);
        }
        
        // Reset and close modal (removed newThemeId as it's auto-generated)
        setState(prev => ({
          ...prev,
          createModalVisible: false,
          createStep: 1,
          newThemeGroup: '',
          newThemeName: '',
          newThemeCategory: '',
          newThemeMarket: 'US',
          newThemeTickers: '',
          newThemeDescription: '',
          creatingTheme: false
        }));
        
        // Reload themes list
        loadThemes(state.currentPage);
      } else {
        message.error(result.message || 'Failed to create theme');
        setState(prev => ({ ...prev, creatingTheme: false }));
      }
    } catch (error: any) {
      console.error('❌ Create theme error:', error);
      console.error('Error response:', error.response);
      
      // 检查是否是冲突错误（可能API返回的是错误而不是正常响应）
      if (error.response?.data?.conflict && error.response?.data?.existing_theme) {
        console.log('⚠️ Conflict detected in error response! Showing conflict resolution modal');
        
        setState(prev => ({
          ...prev,
          creatingTheme: false,
          conflictModalVisible: true,
          conflictExistingTheme: error.response.data.existing_theme,
          conflictNewTickers: error.response.data.new_tickers || []
        }));
        return;
      }
      
      message.error(`Create theme failed: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, creatingTheme: false }));
    }
  };
  
  // Handle conflict resolution - user confirms selected tickers
  const handleConflictResolve = async (selectedTickers: string[]) => {
    setState(prev => ({ ...prev, conflictResolving: true }));
    
    try {
      const result = await updateThemeTickers(
        state.conflictExistingTheme.theme_id,
        selectedTickers,
        state.newThemeName,
        state.newThemeCategory,
        state.newThemeGroup.trim() || undefined,
        state.newThemeMarket
      );
      
      if (result.success) {
        message.success(result.message);
        
        // Close both modals and reset state
        setState(prev => ({
          ...prev,
          conflictModalVisible: false,
          conflictResolving: false,
          conflictExistingTheme: null,
          conflictNewTickers: [],
          createModalVisible: false,
          createStep: 1,
          newThemeGroup: '',
          newThemeName: '',
          newThemeCategory: '',
          newThemeMarket: 'US',
          newThemeTickers: '',
          newThemeDescription: ''
        }));
        
        // Reload themes list
        loadThemes(state.currentPage);
      } else {
        message.error('Failed to update theme');
        setState(prev => ({ ...prev, conflictResolving: false }));
      }
    } catch (error) {
      message.error(`Update failed: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, conflictResolving: false }));
    }
  };
  
  // Handle conflict resolution - user cancels
  const handleConflictCancel = () => {
    setState(prev => ({
      ...prev,
      conflictModalVisible: false,
      conflictExistingTheme: null,
      conflictNewTickers: []
    }));
  };

  // Handle batch conflict resolution
  const handleBatchConflictResolve = async (themeId: string, selectedTickers: string[]) => {
    try {
      console.log(`🔧 Resolving batch conflict for theme ${themeId}`, selectedTickers);
      
      // 找到对应的冲突主题
      const conflict = state.batchConflicts.find((c: any) => c.theme_id === themeId);
      if (!conflict) {
        throw new Error('Conflict not found');
      }
      
      // 调用冲突解决 API
      const result = await updateThemeWithConflictResolution(
        themeId,
        conflict.theme_name,
        conflict.category,
        conflict.existing_theme.description,
        selectedTickers,
        conflict.new_data.group
      );
      
      if (result.success) {
        message.success(`主题 "${conflict.theme_name}" 已更新`);
        // 刷新主题列表
        loadThemes(state.currentPage);
      } else {
        throw new Error(result.message || '更新失败');
      }
    } catch (error) {
      console.error('Failed to resolve batch conflict:', error);
      message.error(getErrorMessage(error));
      throw error;  // Re-throw so modal knows it failed
    }
  };

  const handleBatchConflictClose = () => {
    setState(prev => ({
      ...prev,
      batchConflictModalVisible: false,
      batchConflicts: []
    }));
  };

  // Handle cancel create
  const handleCancelCreate = () => {
    Modal.confirm({
      title: 'Confirm Cancel?',
      content: 'Unsaved changes will be lost.',
      okText: 'Yes, Cancel',
      cancelText: 'Continue Editing',
      onOk: () => {
        setState(prev => ({
          ...prev,
          createModalVisible: false,
          createStep: 1,
          newThemeGroup: '',
          newThemeName: '',
          newThemeCategory: '',
          newThemeMarket: 'US',
          newThemeTickers: '',
          newThemeDescription: '',
          generatingDescription: false,
          creatingTheme: false
        }));
      }
    });
  };

  // Batch Upload: Handle file selection
  const handleFileSelect = (file: File) => {
    // 验证文件格式
    const validExtensions = ['csv', 'xlsx', 'xls'];
    const extension = file.name.split('.').pop()?.toLowerCase();
    
    if (!extension || !validExtensions.includes(extension)) {
      message.error('Invalid file format. Only .csv, .xlsx, .xls are supported.');
      return false;
    }
    
    // 验证文件大小（10MB）
    if (file.size > 10 * 1024 * 1024) {
      message.error('File size exceeds 10MB limit.');
      return false;
    }
    
    setState(prev => ({
      ...prev,
      uploadedFile: file,
    }));
    
    return false; // 阻止自动上传
  };

  // Batch Upload: Start batch upload
  const handleStartBatchUpload = async () => {
    if (!state.uploadedFile) {
      message.error('No file selected');
      return;
    }
    
    setState(prev => ({ ...prev, uploadingFile: true }));
    
    try {
      const result = await batchUploadThemes(
        state.uploadedFile, 
        state.batchUploadGroup.trim() || undefined, // group is now optional
        state.batchUploadMarket
      );
      
      if (result.success) {
        message.success(result.message);
        
        setState(prev => ({
          ...prev,
          uploadingFile: false,
          batchTaskId: result.task_id,
          uploadPreview: result.preview,
        }));
        
        // 开始轮询任务进度
        startPollingBatchTask(result.task_id);
      } else {
        message.error('Upload failed');
        setState(prev => ({ ...prev, uploadingFile: false }));
      }
    } catch (error) {
      message.error(getErrorMessage(error));
      setState(prev => ({ ...prev, uploadingFile: false }));
    }
  };

  // Batch Upload: Poll task status
  const startPollingBatchTask = async (taskId: string) => {
    try {
      await pollTaskUntilComplete(
        taskId,
        (progress: any) => {
          setState(prev => ({
            ...prev,
            batchTaskProgress: progress.progress || 0,
            batchTaskStatus: progress.message || 'Processing...',
          }));
        }
      );

      // 任务完成，获取最终结果
      const finalStatus = await import('../services/investmentThemes').then(m => m.getTaskStatus(taskId));
      const result = finalStatus.result as any;
      
      if (result && result.success) {
        const conflictCount = result.statistics?.conflicts || 0;
        
        message.success(
          `Batch upload completed: ${result.statistics?.created || 0} created, ${result.statistics?.failed || 0} failed, ${result.statistics?.skipped || 0} skipped, ${conflictCount} conflicts`
        );
        
        setState(prev => ({
          ...prev,
          batchTaskDetails: result.details || [],
          batchTaskProgress: 100,
        }));
        
        // 检查是否有冲突需要解决
        if (conflictCount > 0 && result.conflicts && result.conflicts.length > 0) {
          console.log('📋 Batch upload conflicts detected:', result.conflicts);
          
          setState(prev => ({
            ...prev,
            batchConflictModalVisible: true,
            batchConflicts: result.conflicts
          }));
        }
        
        // 刷新主题列表
        loadThemes(state.currentPage);
      } else {
        const errorMsg = (result as any)?.error || 'Unknown error';
        message.error(`Batch upload failed: ${errorMsg}`);
      }
    } catch (error) {
      message.error(getErrorMessage(error));
    }
  };

  // Batch Upload: Close modal
  const handleCloseBatchUploadModal = () => {
    setState(prev => ({
      ...prev,
      batchUploadModalVisible: false,
      uploadedFile: null,
      batchUploadGroup: '',  // 重置为空
      batchUploadMarket: 'US',  // 重置为默认值
      uploadPreview: null,
      batchTaskId: null,
      batchTaskProgress: 0,
      batchTaskStatus: '',
      batchTaskDetails: [],
      uploadingFile: false,
    }));
  };

  const handleDownloadTemplate = () => {
    const headers = ['theme', 'category', 'ticker', 'company'];
    const sampleRows = [
      ['crypto', 'ETF', 'ARKB', 'ARK 21Shares Bitcoin ETF'],
      ['crypto', 'ETF', 'GBTC', 'Grayscale Bitcoin Trust (BTC)'],
      ['crypto', 'Exchange_Custody', 'COIN', 'Coinbase Global Inc'],
      ['crypto', 'Exchange_Custody', 'HOOD', 'Robinhood Markets Inc'],
      ['crypto', 'Miner_AI_Transition', 'IREN', 'Iris Energy Ltd'],
      ['crypto', 'Miner_AI_Transition', 'HIVE', 'HIVE Digital Technologies Ltd'],
      ['crypto', 'Payment', 'PYPL', 'PayPal Holdings Inc'],
      ['crypto', 'Treasury', 'MSTR', 'MicroStrategy Inc']
    ];

    // 将单元格值转义为CSV格式（处理包含逗号、引号或换行符的值）
    const escapeCSV = (value: string): string => {
      if (value.includes(',') || value.includes('"') || value.includes('\n')) {
        // 将值用双引号包裹，并将内部的双引号转义为两个双引号
        return `"${value.replace(/"/g, '""')}"`;
      }
      return value;
    };

    // 将行转换为CSV格式
    const createCSVRow = (row: string[]): string => {
      return row.map(escapeCSV).join(',');
    };

    // 生成CSV内容
    const csvContent = [
      createCSVRow(headers),
      ...sampleRows.map(createCSVRow)
    ].join('\n');

    // 添加BOM以支持Excel正确识别UTF-8编码
    const blob = new Blob(['\ufeff' + csvContent], {
      type: 'text/csv;charset=utf-8;'
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'investment_theme_batch_template.csv';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
  };

  // Table column definitions
  const columns: ColumnsType<InvestmentTheme> = [
    {
      title: 'Theme Name',
      dataIndex: 'theme_name',
      key: 'theme_name',
      ellipsis: true,
      render: (name: string) => <Text strong>{name}</Text>
    },
    {
      title: 'Category',
      dataIndex: 'category',
      key: 'category',
      render: (category?: string) =>
        category ? <Text>{category}</Text> : <Text type="secondary">Not set</Text>
    },
    {
      title: 'Group',
      dataIndex: 'group',
      key: 'group',
      width: 120,
      render: (group?: string) =>
        group ? <Tag color="blue">{group}</Tag> : <Text type="secondary">-</Text>
    },
    {
      title: 'Tickers',
      dataIndex: 'tickers',
      key: 'tickers',
      render: (tickers: string[] | undefined, record: InvestmentTheme) => {
        if (!tickers || tickers.length === 0) {
          return <Text type="secondary">None</Text>;
        }
        const displayTickers = tickers.slice(0, 5);
        const extraCount = tickers.length - displayTickers.length;
        return (
          <Space size={[0, 4]} wrap>
            {displayTickers.map(ticker => (
              <Tooltip title="View associated tickers" key={ticker}>
                <Tag
                  style={{ cursor: 'pointer' }}
                  onClick={() => showAssociationsModal(record)}
                >
                  {ticker}
                </Tag>
              </Tooltip>
            ))}
            {extraCount > 0 && (
              <Tooltip title="View remaining tickers">
                <Tag
                  style={{ cursor: 'pointer' }}
                  onClick={() => showAssociationsModal(record)}
                >
                  +{extraCount}
                </Tag>
              </Tooltip>
            )}
          </Space>
        );
      }
    },
    {
      title: 'Verified %',
      dataIndex: 'verified_tickers',
      key: 'verified_percent',
      render: (_: any, record: InvestmentTheme) => {
        const verified = record.verified_tickers ?? 0;
        const total = record.total_tickers ?? (record.tickers?.length ?? 0);
        const percent = total > 0 ? (verified / total) * 100 : 0;
        return (
          <Tooltip title={`${verified}/${total} verified`}>
            <Tag color={percent === 100 ? 'green' : percent === 0 ? 'default' : 'blue'}>
              {percent.toFixed(0)}%
            </Tag>
          </Tooltip>
        );
      }
    },
    {
      title: 'Description Preview',
      dataIndex: 'description_preview',
      key: 'description_preview',
      ellipsis: true,
      render: (text: string, record: InvestmentTheme) => (
        <Text 
          ellipsis 
          style={{ cursor: text ? 'pointer' : 'default', color: text ? undefined : '#999' }}
          onClick={() => text && showDescriptionModal(record)}
        >
          {text || 'Not generated'}
        </Text>
      )
    },
    {
      title: 'Vectorized',
      dataIndex: 'has_embedding',
      key: 'has_embedding',
      render: (hasEmbedding: boolean, record: InvestmentTheme) => (
        <Tooltip title={hasEmbedding ? `Embedding model: ${record.embedding_model || 'text-embedding-v3'}` : 'Not vectorized yet'}>
          <Tag color={hasEmbedding ? 'green' : 'default'} icon={hasEmbedding ? <CheckCircleOutlined /> : null}>
            {hasEmbedding ? 'Yes' : 'No'}
          </Tag>
        </Tooltip>
      )
    },
    {
      title: 'Created By',
      dataIndex: 'created_by',
      key: 'created_by',
      render: (createdBy?: string) => (
        createdBy ? <Text>{createdBy}</Text> : <Text type="secondary">-</Text>
      )
    },
    {
      title: 'Enabled',
      dataIndex: 'is_enabled_for_matching',
      key: 'is_enabled_for_matching',
      render: (_: any, record: InvestmentTheme) => (
        <Tooltip title={isAdmin ? "Click to toggle" : "View only"}>
          <Switch
            checked={record.is_enabled_for_matching ?? true}
            onChange={() => handleToggleEnabled(record)}
            disabled={!isAdmin}
            checkedChildren={<CheckCircleOutlined />}
            unCheckedChildren={<StopOutlined />}
          />
        </Tooltip>
      )
    },
    {
      title: 'Actions',
      key: 'actions',
      render: (_: any, record: InvestmentTheme) => {
        // Check if user can edit/delete this theme
        const canEdit = isAdmin || record.created_by === user?.username;
        
        return (
          <Space size="small">
            <Tooltip title={canEdit ? "Edit Description" : "You can only edit your own themes"}>
              <Button
                type="link"
                size="small"
                icon={<EditOutlined />}
                onClick={() => handleEditDescription(record)}
                disabled={!canEdit}
              />
            </Tooltip>
            <Tooltip title={canEdit ? "Delete Theme" : "You can only delete your own themes"}>
              <Button
                type="link"
                size="small"
                danger
                icon={<DeleteOutlined />}
                onClick={() => handleDeleteTheme(record)}
                disabled={!canEdit}
              />
            </Tooltip>
          </Space>
        );
      }
    }
  ];


  const uploadPreview = state.uploadPreview;

  return (
    <div style={{ padding: '24px' }}>
      {/* Page Title */}
      <div style={{ marginBottom: '24px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <div>
            <Title level={2}>
              <FundOutlined /> Investment Themes Settings
            </Title>
            <Paragraph type="secondary">
              Intelligent theme matching system based on Embedding technique, automatically associating news through semantic analysis. All users can create themes. You can edit and delete your own themes{isAdmin ? ', and admin can manage all themes' : ''}.
            </Paragraph>
          </div>
        </div>
      </div>


      {/* Action Buttons - Optimized Layout */}
      <Card style={{ marginBottom: '24px' }}>

         {/* Operations */}
         <div style={{ marginBottom: '16px' }}>
           <Space size="middle" wrap>
            {/* Create and Upload - Available to all users */}
            <Tooltip title="Create a new custom investment theme with LLM-generated description">
              <Button
                type="primary"
                icon={<PlusOutlined />}
                onClick={() => setState(prev => ({ ...prev, createModalVisible: true }))}
              >
                Create New Theme
              </Button>
            </Tooltip>
            
            <Tooltip title="Batch upload themes from CSV/Excel file">
              <Button
                type="default"
                icon={<UploadOutlined />}
                onClick={() => setState(prev => ({ ...prev, batchUploadModalVisible: true }))}
              >
                Batch Upload
              </Button>
            </Tooltip>
            
            <Tooltip title={state.selectedRowKeys.length === 0 ? "Select themes to delete" : `Delete selected themes${!isAdmin ? " (only your own)" : ""}`}>
              <Button
                danger
                icon={<DeleteOutlined />}
                onClick={handleBatchDelete}
                disabled={state.selectedRowKeys.length === 0}
              >
                Batch Delete {state.selectedRowKeys.length > 0 ? `(${state.selectedRowKeys.length})` : ''}
              </Button>
            </Tooltip>
            
            {/* Admin-only operations */}
            {isAdmin && (
              <>
                <Tooltip title="Generate descriptions and vectors for new themes, suitable after adding new themes">
                  <Button
                    type="default"
                    icon={<SyncOutlined />}
                    onClick={handleSyncThemes}
                  >
                    Sync Theme Data
                  </Button>
                </Tooltip>
                
                <Tooltip title="Use LLM to enhance description quality, recommended when coverage < 90%">
                  <Button
                    type="default"
                    icon={<ExperimentOutlined />}
                    onClick={handleOptimizeDescriptions}
                  >
                    Optimize Descriptions
                  </Button>
                </Tooltip>
                
                {/* Advanced Options Dropdown Menu */}
                <Dropdown
                  overlay={
                    <Menu>
                      <Menu.Item
                        key="force-vectors"
                        icon={<WarningOutlined style={{ color: '#ff4d4f' }} />}
                        onClick={handleForceUpdateVectors}
                      >
                        <Text type="danger">Force Update All Vectors</Text>
                        <div style={{ fontSize: '12px', color: '#999', marginTop: '4px' }}>
                          Only for model upgrade (20-30 mins)
                        </div>
                      </Menu.Item>
                      <Menu.Item
                        key="force-descriptions"
                        icon={<WarningOutlined style={{ color: '#ff4d4f' }} />}
                        onClick={handleForceRegenerateDescriptions}
                      >
                        <Text type="danger">Regenerate All Descriptions</Text>
                        <div style={{ fontSize: '12px', color: '#999', marginTop: '4px' }}>
                          Only after Prompt optimization (15-25 mins)
                        </div>
                      </Menu.Item>
                      <Menu.Divider />
                      <Menu.Item
                        key="logs"
                        icon={<InfoCircleOutlined />}
                        disabled
                      >
                        View Execution Logs (In Development)
                      </Menu.Item>
                    </Menu>
                  }
                  trigger={['click']}
                >
                  <Button
                    icon={<SettingOutlined />}
                  >
                    Advanced Options <DownOutlined />
                  </Button>
                </Dropdown>
              </>
            )}
            
            <Tooltip title="Enter news text to test theme matching in real-time">
              <Button
                icon={<SearchOutlined />}
                onClick={() => setState(prev => ({ ...prev, testModalVisible: true }))}
              >
                Test Match
              </Button>
            </Tooltip>
            
            <Button
              icon={<ReloadOutlined />}
              onClick={() => {
                loadThemes();
              }}
            >
              Refresh Data
            </Button>
          </Space>
        </div>

      </Card>

      {/* Theme List Table */}
      <Card
        title="Investment Themes List"
        extra={
          <Space size="middle" wrap>
            <Text type="secondary">Total: {state.total}</Text>
            <Search
              placeholder="Search theme or category"
              allowClear
              enterButton
              value={state.searchKeyword}
              onChange={handleSearchInputChange}
              onSearch={handleSearchThemes}
              style={{ width: 260 }}
            />
            <Space size="small">
              <Text type="secondary">Group:</Text>
              <Select
                mode="multiple"
                placeholder={state.loadingGroups ? "Loading groups..." : "Select groups"}
                value={state.selectedGroups}
                onChange={handleGroupFilterChange}
                style={{ minWidth: 200 }}
                size="small"
                allowClear
                maxTagCount="responsive"
                loading={state.loadingGroups}
                notFoundContent={state.loadingGroups ? "Loading..." : "No groups found"}
              >
                {state.availableGroups.map(group => (
                  <Select.Option key={group} value={group}>
                    {group}
                  </Select.Option>
                ))}
              </Select>
            </Space>
            {isAdmin && (
              <Space size="small">
                <Text type="secondary">User:</Text>
                <Select
                  mode="multiple"
                  placeholder={state.loadingUsers ? "Loading users..." : "Select users"}
                  value={state.selectedUsers}
                  onChange={handleUserFilterChange}
                  style={{ minWidth: 200 }}
                  size="small"
                  allowClear
                  maxTagCount="responsive"
                  loading={state.loadingUsers}
                  notFoundContent={state.loadingUsers ? "Loading..." : "No users found"}
                >
                  {state.availableUsers.map(user => (
                    <Select.Option key={user} value={user}>
                      {user}
                    </Select.Option>
                  ))}
                </Select>
              </Space>
            )}
          </Space>
        }
      >
        <Table
          columns={columns}
          dataSource={state.themes}
          rowKey="theme_id"
          loading={state.loading}
          rowSelection={{
            selectedRowKeys: state.selectedRowKeys,
            onChange: (selectedRowKeys: React.Key[]) => {
              setState(prev => ({ ...prev, selectedRowKeys }));
            },
            getCheckboxProps: (record: InvestmentTheme) => ({
              disabled: !isAdmin && record.created_by !== user?.username,
              title: !isAdmin && record.created_by !== user?.username ? 'You can only select your own themes' : undefined
            }),
            selections: [
              Table.SELECTION_ALL,
              Table.SELECTION_INVERT,
              Table.SELECTION_NONE,
            ],
          }}
          pagination={{
            current: state.currentPage,
            pageSize: state.pageSize,
            total: state.total,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total) => `Total ${total} themes`,
            onChange: (page, pageSize) => {
              // 清空选中项（改变页面大小或翻页时）
              setState(prev => ({ 
                ...prev, 
                currentPage: page, 
                pageSize: pageSize || 50,
                selectedRowKeys: []  // 清空选中项
              }));
              loadThemes(page, pageSize || 50, state.searchKeyword, state.selectedGroups, state.selectedUsers);  // 传递新的 pageSize 和筛选条件
            }
          }}
          size="small"
        />
      </Card>

      {/* Theme Associations Modal */}
      <Modal
        open={state.associationModalVisible}
        title={
          state.associationTheme
            ? `Theme Associations - ${state.associationTheme.theme_name || state.associationTheme.theme_id}`
            : 'Theme Associations'
        }
        onCancel={handleCloseAssociationsModal}
        footer={null}
        width={520}
      >
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
          message="Reminder"
          description={
            <span>
              Unverified tickers cannot join matching, screening, or attribution features. Please double‑check ticker accuracy and ensure the correct market suffix (e.g. <Text code>NVDA.US</Text>).
            </span>
          }
        />
        <Table
          columns={associationColumns}
          dataSource={associationDataSource}
          size="small"
          pagination={false}
          loading={state.associationLoading}
          locale={{ emptyText: state.associationLoading ? 'Loading...' : 'No associations found' }}
        />
      </Modal>

      {/* Test Match Modal */}
      <Modal
        title="Test Theme Matching"
        open={state.testModalVisible}
        onCancel={() => setState(prev => ({
          ...prev,
          testModalVisible: false,
          testNewsText: '',
          testResults: []
        }))}
        footer={null}
        width={800}
      >
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          <div>
            <Text strong>Enter News Text:</Text>
            <TextArea
              rows={4}
              placeholder="Please enter news title and content..."
              value={state.testNewsText}
              onChange={(e) => setState(prev => ({ ...prev, testNewsText: e.target.value }))}
              style={{ marginTop: '8px' }}
            />
          </div>


          <Button
            type="primary"
            icon={<SearchOutlined />}
            onClick={handleTestMatch}
            loading={state.testLoading}
            block
          >
            Match Themes
          </Button>

          {/* Match Results */}
          {state.testResults.length > 0 && (
            <div>
              <Divider />
              <Title level={5}>Match Results (Top {state.testResults.length}):</Title>
              <Space direction="vertical" style={{ width: '100%' }} size="small">
                {state.testResults.map((result, index) => (
                  <Card key={index} size="small">
                    <Row gutter={16} align="middle">
                      <Col span={2}>
                        <Text strong style={{ fontSize: '16px' }}>#{result.rank}</Text>
                      </Col>
                      <Col span={14}>
                        <Space direction="vertical" size={0} style={{ width: '100%' }}>
                          <Text strong>{result.theme_name}</Text>
                          {result.category && (
                            <Text type="secondary" style={{ fontSize: '12px' }}>
                              Category: {result.category}
                            </Text>
                          )}
                        </Space>
                      </Col>
                      <Col span={8}>
                        <Tooltip title={`Similarity: ${result.similarity.toFixed(4)}`}>
                          <Progress
                            percent={result.confidence * 100}
                            size="small"
                            format={(percent) => `${percent?.toFixed(0)}%`}
                            strokeColor={
                              result.confidence >= 0.8 ? '#52c41a' :
                              result.confidence >= 0.7 ? '#1890ff' : '#faad14'
                            }
                          />
                        </Tooltip>
                      </Col>
                    </Row>
                  </Card>
                ))}
              </Space>
            </div>
          )}
        </Space>
      </Modal>

      {/* Edit Description Modal */}
      <Modal
        title={`Edit Theme${state.editingTheme ? ` - ${state.editingTheme.theme_name}` : ''}`}
        open={state.editModalVisible}
        onCancel={() => setState(prev => ({ 
          ...prev, 
          editModalVisible: false, 
          editingTheme: null,
          editDescription: '',
          editGroup: ''
        }))}
        onOk={handleSubmitEdit}
        okText="Save & Re-vectorize"
        cancelText="Cancel"
        confirmLoading={state.editLoading}
        width={700}
      >
        {state.editingTheme && (
          <div>
            <Alert
              message="Important Notes"
              description={
                <ul style={{ marginBottom: 0, paddingLeft: '20px' }}>
                  <li>Description and group will be saved immediately</li>
                  <li>Vector will be regenerated automatically (may take a few seconds)</li>
                  <li>If vectorization fails, description and group are still saved</li>
                </ul>
              }
              type="info"
              showIcon
              style={{ marginBottom: 16 }}
            />
            <Space direction="vertical" style={{ width: '100%' }} size="small">
              <div>
                <Text strong>Theme ID:</Text> <Text code>{state.editingTheme.theme_id}</Text>
              </div>
              {state.editingTheme.last_manual_edit_by && (
                <div>
                  <Text strong>Last Edited By:</Text> <Text>{state.editingTheme.last_manual_edit_by}</Text>
                  {' at '}
                  <Text type="secondary">
                    {state.editingTheme.last_manual_edit_at 
                      ? new Date(state.editingTheme.last_manual_edit_at).toLocaleString('en-US') 
                      : '-'}
                  </Text>
                </div>
              )}
            </Space>
            <Divider />
            <Form layout="vertical">
              <Form.Item label="Group" help="Group name for classifying themes (e.g., AI, crypto, healthcare) - Optional">
                <Input
                  placeholder="e.g. AI, Energy (optional)"
                  value={state.editGroup}
                  onChange={(e) => setState(prev => ({ ...prev, editGroup: e.target.value }))}
                  maxLength={100}
                  showCount
                />
              </Form.Item>
              
              <Form.Item label="Description" required style={{ marginBottom: 0 }}>
                <TextArea
                  value={state.editDescription}
                  onChange={(e) => setState(prev => ({ ...prev, editDescription: e.target.value }))}
                  placeholder="Enter detailed theme description..."
                  autoSize={{ minRows: 6, maxRows: 12 }}
                  maxLength={2000}
                  showCount
                />
              </Form.Item>
            </Form>
          </div>
        )}
      </Modal>

      {/* Create New Theme Modal */}
      <Modal
        title={
          <Space>
            <PlusOutlined />
            <span>Create New Investment Theme</span>
            <Tag color="blue">Step {state.createStep}/2</Tag>
          </Space>
        }
        open={state.createModalVisible}
        onCancel={handleCancelCreate}
        footer={null}
        width={800}
        destroyOnClose
      >
        {state.createStep === 1 && (
          <div>

            <Form layout="vertical">
              <Form.Item label="Group" help="Group name for classifying themes (e.g., AI, crypto, healthcare) - Optional">
                <Input
                  placeholder="e.g. AI, Energy (optional)"
                  value={state.newThemeGroup}
                  onChange={(e) => setState(prev => ({ ...prev, newThemeGroup: e.target.value }))}
                  maxLength={100}
                  showCount
                />
              </Form.Item>
              
              <Form.Item label="Theme Name" required>
                <Input
                  placeholder="e.g. AI Healthcare, GS Power Up"
                  value={state.newThemeName}
                  onChange={(e) => setState(prev => ({ ...prev, newThemeName: e.target.value }))}
                  maxLength={200}
                  showCount
                />
              </Form.Item>
              
              <Form.Item label="Category" required>
                <Input
                  placeholder="e.g. AI Diagnostics, Uranium Mining"
                  value={state.newThemeCategory}
                  onChange={(e) => setState(prev => ({ ...prev, newThemeCategory: e.target.value }))}
                  maxLength={100}
                  showCount
                />
              </Form.Item>
              
              <Divider style={{ margin: '16px 0' }} />
              
              <Form.Item 
                label="Associated Stocks (Optional)" 
                help="Enter ticker symbols separated by commas. Supports US, HK, SH (Shanghai), SZ (Shenzhen) exchanges. Use full format like NVDA.US, 0700.HK, 688256.SHG, 000977.SHE. You can mix different exchanges in one theme."
              >
                <Input
                  placeholder="e.g. NVDA.US, AMD.US, 688256.SHG, 000977.SHE, 0700.HK"
                  value={state.newThemeTickers}
                  onChange={(e) => setState(prev => ({ ...prev, newThemeTickers: e.target.value }))}
                  maxLength={500}
                />
              </Form.Item>
              
              <Divider style={{ margin: '16px 0' }} />
              

            </Form>
            
            <div style={{ textAlign: 'right', marginTop: 24 }}>
              <Space>
                <Button onClick={handleCancelCreate}>
                  Cancel
                </Button>
                <Button
                  type="primary"
                  icon={<RobotOutlined />}
                  onClick={handleGenerateDescription}
                  loading={state.generatingDescription}
                  disabled={!state.newThemeName.trim() || !state.newThemeCategory.trim()}
                >
                  {state.generatingDescription ? 'Generating...' : 'Generate Description with AI'}
                </Button>
              </Space>
            </div>
          </div>
        )}
        
        {state.createStep === 2 && (
          <div>
            <Alert
              message="Step 2: Review & Edit Description"
              description={
                <div>
                  <div>AI has generated a description for your theme. You can edit it before creating.</div>
                  <div style={{ marginTop: 8 }}>
                    After confirmation, the theme will be created and vectorized automatically.
                  </div>
                </div>
              }
              type="success"
              showIcon
              style={{ marginBottom: 24 }}
            />
            
            <Space direction="vertical" style={{ width: '100%', marginBottom: 16 }} size="small">
              {state.newThemeGroup && (
                <div>
                  <Text strong>Group:</Text> <Text>{state.newThemeGroup}</Text>
                </div>
              )}
              <div>
                <Text strong>Theme Name:</Text> <Text>{state.newThemeName}</Text>
              </div>
              <div>
                <Text strong>Category:</Text> <Text>{state.newThemeCategory}</Text>
              </div>
              {state.newThemeTickers && (
                <div>
                  <Text strong>Associated Stocks:</Text> <Text>{state.newThemeTickers.split(',').map(t => t.trim()).filter(Boolean).join(', ')}</Text>
                </div>
              )}
              <div>
                <Text strong>Theme ID (Auto-generated):</Text> 
                <Text code style={{ marginLeft: 8 }}>
                  {`${user?.username || 'yourname'}_${state.newThemeName.toLowerCase().replace(/\s+/g, '_')}_${state.newThemeCategory.toLowerCase().replace(/\s+/g, '_')}`}
                </Text>
              </div>
            </Space>
            
            <Divider />
            
            <Form layout="vertical">
              <Form.Item 
                label={
                  <Space>
                    <span>Theme Description</span>
                    <Tag icon={<RobotOutlined />} color="purple">AI Generated</Tag>
                  </Space>
                } 
                required
              >
                <TextArea
                  value={state.newThemeDescription}
                  onChange={(e) => setState(prev => ({ ...prev, newThemeDescription: e.target.value }))}
                  placeholder="Edit the AI-generated description..."
                  autoSize={{ minRows: 8, maxRows: 15 }}
                  maxLength={2000}
                  showCount
                />
              </Form.Item>
            </Form>
            
            <div style={{ textAlign: 'right', marginTop: 24 }}>
              <Space>
                <Button 
                  onClick={() => setState(prev => ({ ...prev, createStep: 1 }))}
                  disabled={state.creatingTheme}
                >
                  Back
                </Button>
                <Button onClick={handleCancelCreate} disabled={state.creatingTheme}>
                  Cancel
                </Button>
                <Button
                  type="primary"
                  onClick={handleCreateTheme}
                  loading={state.creatingTheme}
                  disabled={!state.newThemeDescription.trim()}
                >
                  {state.creatingTheme ? 'Creating...' : 'Create Theme & Vectorize'}
                </Button>
              </Space>
            </div>
          </div>
        )}
      </Modal>

      {/* Theme Conflict Resolution Modal */}
      {state.conflictExistingTheme && (
        <ThemeConflictModal
          visible={state.conflictModalVisible}
          onCancel={handleConflictCancel}
          onConfirm={handleConflictResolve}
          existingTheme={state.conflictExistingTheme}
          newTickers={state.conflictNewTickers}
          loading={state.conflictResolving}
        />
      )}
      
      {/* 批量上传冲突解决 Modal */}
      <BatchThemeConflictModal
        visible={state.batchConflictModalVisible}
        conflicts={state.batchConflicts}
        onClose={handleBatchConflictClose}
        onResolve={handleBatchConflictResolve}
      />

      {/* Batch Upload Modal */}
      <Modal
        title={
          <Space>
            <UploadOutlined />
            <span>Batch Upload Investment Themes</span>
          </Space>
        }
        open={state.batchUploadModalVisible}
        onCancel={handleCloseBatchUploadModal}
        width={800}
        footer={
          state.batchTaskId ? (
            <Button onClick={handleCloseBatchUploadModal}>Close</Button>
          ) : (
            <>
              <Button onClick={handleCloseBatchUploadModal}>Cancel</Button>
              <Button
                type="primary"
                onClick={handleStartBatchUpload}
                loading={state.uploadingFile}
                disabled={!state.uploadedFile}
              >
                Start Upload
              </Button>
            </>
          )
        }
      >
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          {/* File Upload Area */}
          {!state.batchTaskId && (
            <>

              <Button
                icon={<DownloadOutlined />}
                onClick={handleDownloadTemplate}
                type="default"
              >
                Download CSV Template
              </Button>

              {/* Group Input */}
              <div style={{ marginBottom: 16 }}>
                <Text strong style={{ display: 'block', marginBottom: 8 }}>
                  Group <Text type="secondary">(Optional)</Text>
                </Text>
                <Input
                  placeholder="e.g. AI, crypto... (optional)"
                  value={state.batchUploadGroup}
                  onChange={(e) => setState(prev => ({ ...prev, batchUploadGroup: e.target.value }))}
                  maxLength={100}
                  disabled={state.uploadingFile || !!state.batchTaskId}
                  style={{ width: '100%' }}
                />
                <Text type="secondary" style={{ fontSize: 12 }}>
                  All themes in this batch will be assigned to this group (if provided)
                </Text>
              </div>

              {/* Market Selection */}
              <div>
                <Text strong style={{ marginRight: 8 }}>Market:</Text>
                <Select
                  value={state.batchUploadMarket}
                  onChange={(value) => setState(prev => ({ ...prev, batchUploadMarket: value }))}
                  style={{ width: 120 }}
                  disabled={state.uploadingFile || !!state.batchTaskId}
                >
                  <Select.Option value="US">US Market</Select.Option>
                  <Select.Option value="HK">HK Market</Select.Option>
                  <Select.Option value="CN">CN Market</Select.Option>
                </Select>
              </div>

              <Upload.Dragger
                name="file"
                multiple={false}
                beforeUpload={handleFileSelect}
                onRemove={() => setState(prev => ({ ...prev, uploadedFile: null }))}
                fileList={state.uploadedFile ? [state.uploadedFile as any] : []}
                accept=".csv,.xlsx,.xls"
              >
                <p className="ant-upload-drag-icon">
                  <InboxOutlined />
                </p>
                <p className="ant-upload-text">Click or drag file to this area to upload</p>
                <p className="ant-upload-hint">
                  Only CSV or Excel files are supported. The file will be validated before processing.
                </p>
              </Upload.Dragger>

              {state.uploadedFile && (
                <Alert
                  message={`File selected: ${state.uploadedFile.name} (${(state.uploadedFile.size / 1024).toFixed(2)} KB)`}
                  type="success"
                  showIcon
                />
              )}
            </>
          )}

          {/* Upload Preview */}
          {uploadPreview && (
            <>
              <Divider>Upload Preview</Divider>
              
              {/* Theme Name */}
              <Card size="small">
                <Text strong>Theme Name:</Text> <Tag color="blue">{uploadPreview.theme_name}</Tag>
                <br />
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  All themes will use this name with different categories
                </Text>
              </Card>
              
              <Row gutter={16}>
                <Col span={12}>
                  <Card size="small">
                    <Text strong>Themes to Create:</Text> {uploadPreview.total_themes}
                    <br />
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      (= Number of categories)
                    </Text>
                  </Card>
                </Col>
                <Col span={12}>
                  <Card size="small">
                    <Text strong>Total Tickers:</Text> {uploadPreview.total_tickers}
                  </Card>
                </Col>
              </Row>

              {/* Categories Distribution */}
              <Card size="small" title={`Categories Distribution (${uploadPreview.total_themes} categories → ${uploadPreview.total_themes} themes)`}>
                <Space wrap>
                  {Object.entries(uploadPreview.categories).map(([category, count]) => (
                    <Tag key={category} color="green">
                      {category}: {count} tickers
                    </Tag>
                  ))}
                </Space>
              </Card>

            </>
          )}

          {/* Progress Display */}
          {state.batchTaskId && (
            <>
              <Divider>Processing Progress</Divider>
              <Progress
                percent={state.batchTaskProgress}
                status={state.batchTaskProgress === 100 ? 'success' : 'active'}
              />
              <Text type="secondary">{state.batchTaskStatus}</Text>
            </>
          )}
        </Space>
      </Modal>
    </div>
  );
};

export default InvestmentThemesPage;

