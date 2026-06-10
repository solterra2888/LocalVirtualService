/**
 * Investment Themes Management Page
 * Display and manage investment theme vector retrieval functionality
 */

import React, { useState, useEffect, useMemo } from 'react';
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
  App,
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
  Upload,
  ConfigProvider,
  Collapse,
  theme as antdTheme
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
  DownloadOutlined,
  UserOutlined,
  CloseCircleOutlined,
  QuestionCircleOutlined
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
  getAllUsers,
  updateThemeTickers,
  updateThemeWithConflictResolution,
  exportThemes,
  getAvailableTags,
  batchTagThemes
} from '../services/investmentThemes';
import {
  InvestmentTheme,
  ThemeMatch,
  ThemeAssociation,
  getDescriptionSourceLabel
} from '../types/investmentThemes';
import { getErrorMessage } from '../utils/errorHandler';
import { getModalStyles } from '../utils/modalStyles';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import ThemeConflictModal from '../components/ThemeConflictModal';
import BatchThemeConflictModal from '../components/BatchThemeConflictModal';

const { Title, Text, Paragraph } = Typography;
const { TextArea, Search } = Input;

// Aligned with FRONTEND_STYLE_GUIDE: semantic colors for Light/Dark + Behance card style
const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    codeBlockBg: '#f5f5f5',
    selectedBorder: '#1890ff',
    inputBg: undefined as string | undefined, // only used in dark; present for type union
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
  editTags: string[];  // Current tags for the theme being edited
  editTagsToAdd: string;  // Comma-separated new tags to add
  editTickers: ThemeAssociation[];  // 当前的ticker列表（包含验证状态）
  editTickersToAdd: string;  // 要添加的新ticker（逗号分隔）
  editLoadingTickers: boolean;  // 是否正在加载ticker数据
  editLoading: boolean;
  selectedRowKeys: React.Key[];
  createModalVisible: boolean;
  createStep: number;
  newThemeTags: string[];  // Changed from newThemeGroup to newThemeTags (multi-select)
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
  batchUploadTags: string[];  // Tags 标签（可选，多选）
  batchUploadMarket: string;  // market 选择
  uploadPreview: {
    theme_names: string[];
    total_themes: number;  // 要创建的主题数量（= (theme, category) 组合数）
    total_tickers: number;
    themes: Record<string, { categories: Record<string, number>; total_tickers: number }>;
    categories?: Record<string, number>;
    categories_sample?: string[];
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
    selectedFilterTags: string[];  // 选中的 Tags 筛选值（用于过滤主题列表）
    selectedUsers: string[];  // 选中的 User 筛选值（支持多选，admin专用）
    availableUsers: string[];  // 所有可用的 users（从 API 获取）
    loadingUsers: boolean;  // 是否正在加载 users
    ownershipFilter: 'all' | 'own';  // 所有权筛选器：全部/自己创建
    // 批量打标签相关
    batchTagModalVisible: boolean;
    batchTagAction: 'add' | 'remove';
    selectedTags: string[];  // 批量打标签操作时选择的标签
    availableTags: Array<{ tag_name: string; count: number }>;
    loadingTags: boolean;
    batchTagging: boolean;
}

const InvestmentThemesPageInner: React.FC = () => {
  const { user } = useAuthStore();
  const { themeMode } = useDashboardSettings();
  const { modal } = App.useApp();
  const isAdmin = user?.is_admin === true;
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const cardStyle = isDark ? CARD_STYLE_BEHANCE_DARK : CARD_STYLE_BEHANCE;
  const modalStyles = getModalStyles(themeMode);

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
    editTags: [],
    editTagsToAdd: '',
    editTickers: [],
    editTickersToAdd: '',
    editLoadingTickers: false,
    editLoading: false,
    selectedRowKeys: [],
    createModalVisible: false,
    createStep: 1,
    newThemeTags: [],  // Changed to array for multi-select
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
    batchUploadTags: [],  // 默认为空数组，支持多标签
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
    selectedFilterTags: [],
    selectedUsers: [],
    availableUsers: [],
    loadingUsers: false,
    ownershipFilter: 'all',
    // 批量打标签相关
    batchTagModalVisible: false,
    batchTagAction: 'add',
    selectedTags: [],
    availableTags: [],
    loadingTags: false,
    batchTagging: false
  });


  // Load theme list
  const loadThemes = async (page?: number, pageSize?: number, searchKeyword?: string, tags?: string[], users?: string[]) => {
    setState(prev => ({ ...prev, loading: true }));
    try {
      const currentPage = page || state.currentPage;
      const currentPageSize = pageSize || state.pageSize;
      const effectiveSearch = searchKeyword !== undefined ? searchKeyword : state.searchKeyword;
      const effectiveTags = tags !== undefined ? tags : state.selectedFilterTags;
      const effectiveUsers = users !== undefined ? users : state.selectedUsers;
      const response = await listThemes(
        currentPageSize,
        (currentPage - 1) * currentPageSize,
        effectiveSearch,
        effectiveTags.length > 0 ? effectiveTags : undefined,
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
    loadThemes(1, state.pageSize, trimmed, state.selectedFilterTags, state.selectedUsers);
  };
  
  const handleTagFilterChange = (tags: string[]) => {
    setState(prev => ({
      ...prev,
      selectedFilterTags: tags,
      currentPage: 1
    }));
    loadThemes(1, state.pageSize, state.searchKeyword, tags, state.selectedUsers);
  };
  
  const handleUserFilterChange = (users: string[]) => {
    setState(prev => ({
      ...prev,
      selectedUsers: users,
      currentPage: 1
    }));
    loadThemes(1, state.pageSize, state.searchKeyword, state.selectedFilterTags, users);
  };

  const handleOwnershipFilterChange = (filter: 'all' | 'own') => {
    setState(prev => ({
      ...prev,
      ownershipFilter: filter,
      currentPage: 1
    }));
  };

  const handleSearchInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const { value } = event.target;
    setState(prev => ({
      ...prev,
      searchKeyword: value
    }));

    if (value === '') {
      loadThemes(1, state.pageSize, '', state.selectedFilterTags, state.selectedUsers);
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
  
  // 加载所有可用的标签（从 API 获取）
  const loadAvailableTags = async () => {
    setState(prev => ({ ...prev, loadingTags: true }));
    try {
      const response = await getAvailableTags();
      if (response.success) {
        setState(prev => ({
          ...prev,
          availableTags: response.tags || [],
          loadingTags: false
        }));
      } else {
        setState(prev => ({ ...prev, loadingTags: false }));
      }
    } catch (error) {
      console.error('Failed to load tags:', error);
      setState(prev => ({ ...prev, loadingTags: false }));
    }
  };
  
  // 🔥 Helper function: Batch tag themes
  const batchTagThemes = async (themeIds: string[], tagNames: string[], action: 'add' | 'remove') => {
    try {
      // Call batch tag API directly with tag names
      const { batchTagThemes: apiBatchTag } = await import('../services/investmentThemes');
      const result = await apiBatchTag(themeIds, tagNames, action);
      
      // Reload tags after operation
      await loadAvailableTags();
      
      return result;
    } catch (error) {
      console.error('Failed to batch tag themes:', error);
      throw error;
    }
  };
  
  // 🔥 Helper function: Update tags for a single theme (replace all existing tags)
  const updateThemeTags = async (themeId: string, tagNames: string[]) => {
    try {
      // First, get current tags for the theme
      const { getThemeTags } = await import('../services/investmentThemes');
      const currentTagsResponse = await getThemeTags(themeId);
      const currentTags = currentTagsResponse.tags.map(t => t.tag_name);
      
      // Find tags to remove and add
      const tagsToRemove = currentTags.filter(t => !tagNames.includes(t));
      const tagsToAdd = tagNames.filter(t => !currentTags.includes(t));
      
      // Remove old tags
      if (tagsToRemove.length > 0) {
        await batchTagThemes([themeId], tagsToRemove, 'remove');
      }
      
      // Add new tags
      if (tagsToAdd.length > 0) {
        await batchTagThemes([themeId], tagsToAdd, 'add');
      }
      
      // Reload tags
      await loadAvailableTags();
    } catch (error) {
      console.error('Failed to update theme tags:', error);
      throw error;
    }
  };
  
  // 组件加载时获取所有 users 和 tags
  useEffect(() => {
    loadAvailableTags();
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
    modal.confirm({
      title: 'Sync Theme Data',
      icon: <SyncOutlined />,
      width: 600,
      styles: modalStyles,
      okButtonProps: {
        style: { borderRadius: 10, fontWeight: 500, ...(isDark ? { background: '#2196F3', borderColor: '#2196F3' } : {}) },
      },
      cancelButtonProps: {
        style: isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : undefined,
      },
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
    modal.confirm({
      title: 'Optimize Description Quality',
      icon: <ExperimentOutlined />,
      width: 600,
      styles: modalStyles,
      okButtonProps: {
        style: { borderRadius: 10, fontWeight: 500, ...(isDark ? { background: '#2196F3', borderColor: '#2196F3' } : {}) },
      },
      cancelButtonProps: {
        style: isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : undefined,
      },
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
    modal.confirm({
      title: 'Warning: Force Update All Vectors',
      icon: <WarningOutlined style={{ color: '#ff4d4f' }} />,
      width: 650,
      styles: modalStyles,
      okButtonProps: {
        style: { borderRadius: 10, fontWeight: 500 },
      },
      cancelButtonProps: {
        style: isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : undefined,
      },
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
    modal.confirm({
      title: 'Warning: Regenerate All Descriptions',
      icon: <WarningOutlined style={{ color: '#ff4d4f' }} />,
      width: 650,
      styles: modalStyles,
      okButtonProps: {
        style: { borderRadius: 10, fontWeight: 500 },
      },
      cancelButtonProps: {
        style: isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : undefined,
      },
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
    modal.info({
      title: `${theme.theme_name} Theme Description`,
      width: 800,
      okText: 'Close',
      styles: modalStyles,
      okButtonProps: {
        style: {
          borderRadius: 10,
          fontWeight: 500,
          ...(isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : {}),
        },
      },
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
  const handleEditDescription = async (theme: InvestmentTheme) => {
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
      editTags: theme.tags || [],  // Load existing tags
      editTagsToAdd: '',
      editTickers: [],
      editTickersToAdd: '',
      editLoadingTickers: true
    }));

    // Load ticker associations
    try {
      const response = await getThemeAssociations(theme.theme_id);
      setState(prev => ({
        ...prev,
        editTickers: response.associations,
        editLoadingTickers: false
      }));
    } catch (error) {
      message.error(`Failed to load ticker associations: ${getErrorMessage(error)}`);
      setState(prev => ({
        ...prev,
        editLoadingTickers: false
      }));
    }
  };

  // Handle add new tickers
  const handleAddTickers = () => {
    if (!state.editTickersToAdd.trim()) {
      return;
    }

    const newTickers = state.editTickersToAdd
      .split(',')
      .map(t => t.trim())
      .filter(Boolean);

    if (newTickers.length === 0) {
      return;
    }

    // Check for duplicates
    const existingTickers = new Set(state.editTickers.map(t => t.ticker.toUpperCase()));
    const duplicates = newTickers.filter(t => existingTickers.has(t.toUpperCase()));
    
    if (duplicates.length > 0) {
      message.warning(`These tickers already exist: ${duplicates.join(', ')}`);
    }

    // Add non-duplicate tickers
    const tickersToAdd = newTickers.filter(t => !existingTickers.has(t.toUpperCase()));
    
    if (tickersToAdd.length > 0) {
      const newTickerObjects: ThemeAssociation[] = tickersToAdd.map(ticker => ({
        ticker: ticker,
        market: null,
        is_verified: false,
        association_weight: null
      }));

      setState(prev => ({
        ...prev,
        editTickers: [...prev.editTickers, ...newTickerObjects],
        editTickersToAdd: ''
      }));

      message.success(`Added ${tickersToAdd.length} ticker(s)`);
    }
  };

  // Handle remove ticker
  const handleRemoveTicker = (tickerToRemove: string) => {
    setState(prev => ({
      ...prev,
      editTickers: prev.editTickers.filter(t => t.ticker !== tickerToRemove)
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
      // Check if tickers have changed
      const originalTickers = (state.editingTheme.tickers || []).sort();
      const currentTickers = state.editTickers.map(t => t.ticker).sort();
      const tickersChanged = JSON.stringify(originalTickers) !== JSON.stringify(currentTickers);

      // If tickers changed, use updateThemeTickers API (which will update description, group, and tickers)
      if (tickersChanged) {
        // Infer market from tickers (check if any ticker has a market suffix)
        const tickers = state.editTickers.map(t => t.ticker);
        let inferredMarket = 'US'; // Default to US
        
        // Check for non-US markets
        for (const ticker of tickers) {
          if (ticker.includes('.HK')) {
            inferredMarket = 'HK';
            break;
          } else if (ticker.includes('.SHG') || ticker.includes('.SHE')) {
            inferredMarket = 'CN';
            break;
          }
        }
        
        const result = await updateThemeTickers(
          state.editingTheme.theme_id,
          tickers,
          state.editingTheme.theme_name,
          state.editingTheme.category || '',
          undefined,  // group parameter removed
          inferredMarket  // Use inferred market
        );

        if (result.success) {
          message.success('Theme updated successfully (tickers, description, and vector regenerated)');
          
          setState(prev => ({
            ...prev,
            editModalVisible: false,
            editingTheme: null,
            editDescription: '',
            editTags: [],
            editTagsToAdd: '',
            editTickers: [],
            editTickersToAdd: '',
            editLoading: false
          }));
          
          // Refresh with delay to ensure backend processing completes
          setTimeout(() => {
            loadThemes(state.currentPage);
          }, 1500);
        } else {
          message.error(result.message || 'Update failed');
          setState(prev => ({ ...prev, editLoading: false }));
        }
      } else {
        // Tickers not changed, only update description and group
        const result = await updateThemeDescription(
          state.editingTheme.theme_id,
          state.editDescription,
          state.editingTheme.version,
          undefined  // group parameter removed
        );

        // 🔥 Update tags separately after updating description
        await updateThemeTags(state.editingTheme.theme_id, state.editTags);

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
            editTags: [],
            editTagsToAdd: '',
            editTickers: [],
            editTickersToAdd: '',
            editLoading: false
          }));
          
          loadThemes(state.currentPage);
        } else {
          message.error(result.message || 'Update failed');
          setState(prev => ({ ...prev, editLoading: false }));
        }
      }
    } catch (error) {
      message.error(`Update failed: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, editLoading: false }));
    }
  };

  // Handle export themes to Excel
  const handleExportThemes = async () => {
    try {
      const blob = await exportThemes();
      const url = URL.createObjectURL(blob);

      // 从 Content-Disposition 提取文件名，或使用默认值
      const filename = `investment_themes_${user?.username}_${new Date().toISOString().slice(0, 10)}.xlsx`;

      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
      message.success('主题导出成功');
    } catch (error: any) {
      message.error(`导出失败: ${getErrorMessage(error)}`);
    }
  };

  // Handle delete theme
  const handleDeleteTheme = (theme: InvestmentTheme) => {
    // Check if user has permission to delete
    if (!isAdmin && theme.created_by !== user?.username) {
      message.error('You can only delete themes you created');
      return;
    }

    modal.confirm({
      title: 'Confirm Delete Theme?',
      icon: <WarningOutlined style={{ color: '#ff4d4f' }} />,
      width: 600,
      styles: modalStyles,
      okButtonProps: {
        style: { borderRadius: 10, fontWeight: 500 },
      },
      cancelButtonProps: {
        style: isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : undefined,
      },
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

    modal.confirm({
      title: `Confirm Batch Delete ${selectedThemes.length} Themes?`,
      icon: <WarningOutlined style={{ color: '#ff4d4f' }} />,
      width: 700,
      styles: modalStyles,
      okButtonProps: {
        style: { borderRadius: 10, fontWeight: 500 },
      },
      cancelButtonProps: {
        style: isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : undefined,
      },
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
              <div key={theme.theme_id} style={{ padding: '8px', background: colors.codeBlockBg, marginBottom: '4px', borderRadius: 8 }}>
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
              modal.info({
                title: 'Batch Delete Completed with Issues',
                width: 700,
                styles: modalStyles,
                okButtonProps: {
                  style: {
                    borderRadius: 10,
                    fontWeight: 500,
                    ...(isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : {}),
                  },
                },
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
        undefined, // group parameter removed - will use tags instead
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
        tags: state.newThemeTags
      });
      
      const result = await createTheme(
        state.newThemeName,
        state.newThemeCategory,
        state.newThemeDescription,
        undefined, // group parameter removed - will use tags instead
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
        // 🔥 Theme created, now add tags if any
        if (state.newThemeTags.length > 0) {
          try {
            await batchTagThemes([result.theme_id], state.newThemeTags, 'add');
            console.log(`✅ Tags added to theme ${result.theme_id}:`, state.newThemeTags);
          } catch (tagError) {
            console.error('Failed to add tags:', tagError);
            message.warning('主题创建成功，但添加标签失败');
          }
        }
        
        if (result.embedding_created) {
          message.success(`Theme '${state.newThemeName}' created successfully!${state.newThemeTags.length > 0 ? ` (${state.newThemeTags.length} tags added)` : ''}`);
        } else {
          message.warning(`Theme saved, but vector generation failed: ${result.error || 'Unknown error'}`);
        }
        
        // Reset and close modal
        setState(prev => ({
          ...prev,
          createModalVisible: false,
          createStep: 1,
          newThemeTags: [],  // Changed to empty array
          newThemeName: '',
          newThemeCategory: '',
          newThemeMarket: 'US',
          newThemeTickers: '',
          newThemeDescription: '',
          creatingTheme: false
        }));
        
        // Reload themes list with a delay to ensure backend processing completes
        setTimeout(() => {
          loadThemes(state.currentPage);
        }, 2000);
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
        undefined,  // group parameter removed
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
          newThemeTags: [],  // Changed to empty array
          newThemeName: '',
          newThemeCategory: '',
          newThemeMarket: 'US',
          newThemeTickers: '',
          newThemeDescription: ''
        }));
        
        // Reload themes list with delay to ensure backend updates complete
        setTimeout(() => {
          loadThemes(state.currentPage);
        }, 1500);
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
        // 刷新主题列表，添加延迟以确保后端更新完成
        setTimeout(() => {
          loadThemes(state.currentPage);
        }, 1500);
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
    
    // 关闭冲突modal时刷新列表，确保看到最新的数据
    setTimeout(() => {
      loadThemes(state.currentPage);
    }, 1000);
  };

  // Handle cancel create
  const handleCancelCreate = () => {
    modal.confirm({
      title: 'Confirm Cancel?',
      content: 'Unsaved changes will be lost.',
      okText: 'Yes, Cancel',
      cancelText: 'Continue Editing',
      styles: modalStyles,
      okButtonProps: {
        style: { borderRadius: 10, fontWeight: 500, ...(isDark ? { background: '#2196F3', borderColor: '#2196F3' } : {}) },
      },
      cancelButtonProps: {
        style: isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : undefined,
      },
      onOk: () => {
        setState(prev => ({
          ...prev,
          createModalVisible: false,
          createStep: 1,
          newThemeTags: [],  // Changed to empty array
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
        undefined, // group parameter removed - will use tags instead
        state.batchUploadMarket
      );
      
      if (result.success) {
        // 🔥 Batch upload initiated, store task ID and tags for later application
        setState(prev => ({
          ...prev,
          uploadingFile: false,
          batchTaskId: result.task_id,
          uploadPreview: result.preview,
        }));
        
        message.success(result.message);
        
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
        const createdCount = result.statistics?.created || 0;
        
        // 🔥 Apply tags to newly created themes if tags were selected
        if (state.batchUploadTags.length > 0 && result.theme_ids && result.theme_ids.length > 0) {
          try {
            await batchTagThemes(result.theme_ids, state.batchUploadTags, 'add');
            console.log(`✅ Tags added to ${result.theme_ids.length} batch uploaded themes:`, state.batchUploadTags);
            message.success(
              `Batch upload completed: ${createdCount} created (${state.batchUploadTags.length} tags added), ${result.statistics?.failed || 0} failed, ${result.statistics?.skipped || 0} skipped, ${conflictCount} conflicts`
            );
          } catch (tagError) {
            console.error('Failed to add tags to batch uploaded themes:', tagError);
            message.warning(
              `Batch upload completed: ${createdCount} created, but failed to add tags. ${result.statistics?.failed || 0} failed, ${result.statistics?.skipped || 0} skipped, ${conflictCount} conflicts`
            );
          }
        } else {
          message.success(
            `Batch upload completed: ${createdCount} created, ${result.statistics?.failed || 0} failed, ${result.statistics?.skipped || 0} skipped, ${conflictCount} conflicts`
          );
        }
        
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
        
        // 刷新主题列表，添加延迟以确保后端处理完成（LLM描述生成和向量化）
        setTimeout(() => {
          loadThemes(state.currentPage);
        }, 2000);
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
    // 如果任务已完成，关闭modal时刷新列表
    const shouldRefresh = state.batchTaskId && state.batchTaskProgress === 100;
    
    setState(prev => ({
      ...prev,
      batchUploadModalVisible: false,
      uploadedFile: null,
      batchUploadTags: [],  // 重置为空数组
      batchUploadMarket: 'US',  // 重置为默认值
      uploadPreview: null,
      batchTaskId: null,
      batchTaskProgress: 0,
      batchTaskStatus: '',
      batchTaskDetails: [],
      uploadingFile: false,
    }));
    
    // 任务完成后关闭modal时刷新列表，确保显示最新数据
    if (shouldRefresh) {
      setTimeout(() => {
        loadThemes(state.currentPage);
      }, 1000);
    }
  };

  // 批量打标签相关函数
  const handleOpenBatchTagModal = (action: 'add' | 'remove') => {
    if (state.selectedRowKeys.length === 0) {
      message.warning('请先选择要操作的主题');
      return;
    }
    
    setState(prev => ({
      ...prev,
      batchTagModalVisible: true,
      batchTagAction: action,
      selectedTags: []
    }));
    
    // 刷新可用标签列表
    loadAvailableTags();
  };
  
  const handleCloseBatchTagModal = () => {
    setState(prev => ({
      ...prev,
      batchTagModalVisible: false,
      selectedTags: [],
      batchTagging: false
    }));
  };
  
  const handleBatchTag = async () => {
    if (state.selectedTags.length === 0) {
      message.warning('请选择至少一个标签');
      return;
    }
    
    setState(prev => ({ ...prev, batchTagging: true }));
    
    try {
      const themeIds = state.selectedRowKeys.map(String);
      const result = await batchTagThemes(
        themeIds,
        state.selectedTags,
        state.batchTagAction
      );
      
      if (result.success) {
        message.success(result.message);
        handleCloseBatchTagModal();
        
        // 刷新主题列表
        loadThemes(state.currentPage);
        
        // 清空选中项
        setState(prev => ({ ...prev, selectedRowKeys: [] }));
      } else {
        message.error('操作失败');
      }
    } catch (error) {
      message.error(`操作失败: ${getErrorMessage(error)}`);
    } finally {
      setState(prev => ({ ...prev, batchTagging: false }));
    }
  };
  
  const handleDownloadTemplate = () => {
    const headers = ['theme', 'category', 'ticker', 'company'];
    const sampleRows = [
      ['crypto', 'ETF', 'ARKB', 'ARK 21Shares Bitcoin ETF'],
      ['crypto', 'Treasury', 'MSTR', 'MicroStrategy Inc'],
      ['AI', 'Infrastructure', 'NVDA', 'NVIDIA Corp'],
      ['AI', 'Application', 'MSFT', 'Microsoft Corp'],
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
      width: 280,
      ellipsis: true,
      render: (name: string, record: InvestmentTheme) => (
        <Text strong>{name}</Text>
      )
    },
    {
      title: 'Category',
      dataIndex: 'category',
      key: 'category',
      width: 180,
      render: (category?: string) =>
        category ? <Text>{category}</Text> : <Text type="secondary">Not set</Text>
    },
    {
      title: (
        <Space>
          <span>Tags</span>
          <Tooltip title="Tags are personal labels used to organize and categorize themes, primarily for Market Weekly and other reports. Tags are only visible to you.">
            <QuestionCircleOutlined style={{ color: '#1890ff', cursor: 'help' }} />
          </Tooltip>
        </Space>
      ),
      dataIndex: 'tags',
      key: 'tags',
      width: 200,
      render: (tags?: string[]) => {
        if (!tags || tags.length === 0) {
          return <Text type="secondary">-</Text>;
        }
        return (
          <Space size={[0, 4]} wrap>
            {tags.map(tag => (
              <Tag key={tag} color="blue">{tag}</Tag>
            ))}
          </Space>
        );
      }
    },
    {
      title: 'Tickers',
      dataIndex: 'tickers',
      key: 'tickers',
      render: (tickers: string[] | undefined, record: InvestmentTheme) => {
        if (!tickers || tickers.length === 0) {
          return <Text type="secondary">None</Text>;
        }
        return (
          <Tooltip title="View associated tickers">
            <Text
              style={{
                color: '#1890ff',
                textDecoration: 'underline',
                cursor: 'pointer'
              }}
              onClick={() => showAssociationsModal(record)}
            >
              {tickers.length}
            </Text>
          </Tooltip>
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
        const canEdit = record.can_edit ?? (isAdmin || record.created_by === user?.username);
        const canDelete = record.can_delete ?? canEdit;

        return (
          <Space size="small">
            <Tooltip title={canEdit ? "Edit Theme" : "只能编辑自己创建的主题"}>
              <Button
                type="link"
                size="small"
                icon={<EditOutlined />}
                onClick={() => handleEditDescription(record)}
                disabled={!canEdit}
              />
            </Tooltip>

            <Tooltip title={canDelete ? "Delete Theme" : "只能删除自己创建的主题"}>
              <Button
                type="link"
                size="small"
                danger
                icon={<DeleteOutlined />}
                onClick={() => handleDeleteTheme(record)}
                disabled={!canDelete}
              />
            </Tooltip>
          </Space>
        );
      }
    }
  ];


  const uploadPreview = state.uploadPreview;

  return (
    <div style={{ padding: '12px 40px 32px', backgroundColor: colors.pageBg, minHeight: '100%' }}>
      {/* Page Title */}
      <div style={{ marginBottom: 20 }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
          <div>
            <Title level={2} style={{ color: colors.text, marginTop: 0, marginBottom: 4 }}>
              <FundOutlined /> Investment Themes Settings
            </Title>
            <Paragraph type="secondary" style={{ color: colors.textSecondary, marginBottom: 0 }}>
              Intelligent theme matching system based on Embedding technique, automatically associating news through semantic analysis. All users can create themes. You can edit and delete your own themes{isAdmin ? ', and admin can manage all themes' : ''}.
            </Paragraph>
          </div>
        </div>
      </div>


      {/* Action Buttons - Optimized Layout */}
      <Card style={{ ...cardStyle, marginBottom: '24px' }}>

         {/* Operations */}
         <div style={{ marginBottom: '16px' }}>
           <Space size="middle" wrap>
            {/* Create and Upload - Available to all users */}
            <Tooltip title="Create a new custom investment theme with LLM-generated description">
              <Button
                type="primary"
                icon={<PlusOutlined />}
                onClick={() => setState(prev => ({ ...prev, createModalVisible: true }))}
                style={{
                  borderRadius: 10,
                  fontWeight: 500,
                  height: 36,
                  paddingLeft: 16,
                  paddingRight: 16,
                  ...(isDark ? { background: '#2196F3', borderColor: '#2196F3' } : {}),
                }}
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
            
            <Tooltip title={state.selectedRowKeys.length === 0 ? "Select themes to tag" : "Add tags to selected themes"}>
              <Button
                type="default"
                icon={<PlusOutlined />}
                onClick={() => handleOpenBatchTagModal('add')}
                disabled={state.selectedRowKeys.length === 0}
              >
                Add Tags {state.selectedRowKeys.length > 0 ? `(${state.selectedRowKeys.length})` : ''}
              </Button>
            </Tooltip>
            
            <Tooltip title={state.selectedRowKeys.length === 0 ? "Select themes to untag" : "Remove tags from selected themes"}>
              <Button
                type="default"
                icon={<CloseCircleOutlined />}
                onClick={() => handleOpenBatchTagModal('remove')}
                disabled={state.selectedRowKeys.length === 0}
              >
                Remove Tags {state.selectedRowKeys.length > 0 ? `(${state.selectedRowKeys.length})` : ''}
              </Button>
            </Tooltip>

            <Tooltip title="Export your themes to Excel file">
              <Button
                type="default"
                icon={<DownloadOutlined />}
                onClick={handleExportThemes}
              >
                Export
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
                        <div style={{ fontSize: '12px', color: colors.textSecondary, marginTop: '4px' }}>
                          Only for model upgrade (20-30 mins)
                        </div>
                      </Menu.Item>
                      <Menu.Item
                        key="force-descriptions"
                        icon={<WarningOutlined style={{ color: '#ff4d4f' }} />}
                        onClick={handleForceRegenerateDescriptions}
                      >
                        <Text type="danger">Regenerate All Descriptions</Text>
                        <div style={{ fontSize: '12px', color: colors.textSecondary, marginTop: '4px' }}>
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
        style={cardStyle}
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
              style={{ width: 260, ...(isDark ? { backgroundColor: colors.inputBg } : {}) }}
            />
            {isAdmin && (
              <Space size="small">
                <Text type="secondary">Ownership:</Text>
                <Select
                  value={state.ownershipFilter}
                  onChange={handleOwnershipFilterChange}
                  style={{ width: 120, ...(isDark ? { backgroundColor: colors.inputBg } : {}) }}
                  size="small"
                >
                  <Select.Option value="all">All</Select.Option>
                  <Select.Option value="own">My Own</Select.Option>
                </Select>
              </Space>
            )}
            <Space size="small">
              <Text type="secondary">Tags:</Text>
              <Select
                mode="multiple"
                placeholder={state.loadingTags ? "Loading tags..." : "Select tags"}
                value={state.selectedFilterTags}
                onChange={handleTagFilterChange}
                style={{ minWidth: 200, ...(isDark ? { backgroundColor: colors.inputBg } : {}) }}
                size="small"
                allowClear
                maxTagCount="responsive"
                loading={state.loadingTags}
                notFoundContent={state.loadingTags ? "Loading..." : "No tags found"}
                showSearch
                filterOption={(input, option) =>
                  (option?.children as unknown as string)?.toLowerCase().includes(input.toLowerCase())
                }
              >
                {state.availableTags.map(tag => (
                  <Select.Option key={tag.tag_name} value={tag.tag_name}>
                    {tag.tag_name} {tag.count > 0 ? `(${tag.count})` : ''}
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
                  style={{ minWidth: 200, ...(isDark ? { backgroundColor: colors.inputBg } : {}) }}
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
          dataSource={useMemo(() => {
            // Filter themes based on ownership filter
            if (state.ownershipFilter === 'all') {
              return state.themes;
            }
            return state.themes.filter(theme => {
              if (state.ownershipFilter === 'own') {
                return (theme as any).created_by === user?.username;
              }
              return true;
            });
          }, [state.themes, state.ownershipFilter, user?.username])}
          rowKey="theme_id"
          loading={state.loading}
          rowSelection={{
            selectedRowKeys: state.selectedRowKeys,
            onChange: (selectedRowKeys: React.Key[]) => {
              setState(prev => ({ ...prev, selectedRowKeys }));
            },
            // 允许用户勾选任何主题（包括分享的主题），以便进行标签操作
            // 具体的操作权限在执行时检查（例如删除只能删除自己的主题）
            selections: [
              Table.SELECTION_ALL,
              Table.SELECTION_INVERT,
              Table.SELECTION_NONE,
            ],
          }}
          pagination={{
            current: state.currentPage,
            pageSize: state.pageSize,
            total: useMemo(() => {
              if (state.ownershipFilter === 'all') {
                return state.total;
              }
              const filteredThemes = state.themes.filter(theme => {
                if (state.ownershipFilter === 'own') {
                  return (theme as any).created_by === user?.username;
                }
                return true;
              });
              return filteredThemes.length;
            }, [state.themes, state.ownershipFilter, state.total, user?.username]),
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
              loadThemes(page, pageSize || 50, state.searchKeyword, state.selectedFilterTags, state.selectedUsers);  // 传递新的 pageSize 和筛选条件
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
        styles={modalStyles}
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
        styles={modalStyles}
      >
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          <div>
            <Text strong>Enter News Text:</Text>
            <TextArea
              rows={4}
              placeholder="Please enter news title and content..."
              value={state.testNewsText}
              onChange={(e) => setState(prev => ({ ...prev, testNewsText: e.target.value }))}
              style={{ marginTop: '8px', ...(isDark ? { backgroundColor: colors.inputBg } : {}) }}
            />
          </div>


          <Button
            type="primary"
            icon={<SearchOutlined />}
            onClick={handleTestMatch}
            loading={state.testLoading}
            block
            style={{
              borderRadius: 10,
              fontWeight: 500,
              ...(isDark ? { background: '#2196F3', borderColor: '#2196F3' } : {}),
            }}
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
                  <Card key={index} size="small" style={cardStyle}>
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
        styles={modalStyles}
        onCancel={() => setState(prev => ({ 
          ...prev, 
          editModalVisible: false, 
          editingTheme: null,
          editDescription: '',
          editTags: [],
          editTagsToAdd: '',
          editTickers: [],
          editTickersToAdd: ''
        }))}
        onOk={handleSubmitEdit}
        okText="Save & Re-vectorize"
        cancelText="Cancel"
        confirmLoading={state.editLoading}
        width={700}
        okButtonProps={{
          style: {
            borderRadius: 10,
            fontWeight: 500,
            ...(isDark ? { background: '#2196F3', borderColor: '#2196F3' } : {}),
          },
        }}
        cancelButtonProps={{
          style: isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : undefined,
        }}
      >
        {state.editingTheme && (
          <div>
            <Alert
              message="Important Notes"
              description={
                <ul style={{ marginBottom: 0, paddingLeft: '20px' }}>
                  <li>Description and group will be saved immediately</li>
                  <li>Vector will be regenerated automatically (may take a few seconds)</li>
                  <li><strong>If tickers are changed:</strong> Description will be regenerated using LLM and new tickers will be auto-verified</li>
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
              {/* Tags Management Section */}
              <Form.Item label="My Tags" help="🏷️ Your personal tags for this theme. These tags are only visible to you and help organize themes for reports like Market Weekly.">
                <Space direction="vertical" style={{ width: '100%' }} size="small">
                  {/* Display current tags */}
                  {state.editTags.length > 0 ? (
                    <Space size={[0, 4]} wrap>
                      {state.editTags.map(tag => (
                        <Tag
                          key={tag}
                          closable
                          onClose={() => {
                            setState(prev => ({
                              ...prev,
                              editTags: prev.editTags.filter(t => t !== tag)
                            }));
                          }}
                          color="blue"
                        >
                          {tag}
                        </Tag>
                      ))}
                    </Space>
                  ) : (
                    <Text type="secondary">No tags assigned</Text>
                  )}
                  
                  {/* Add new tags */}
                  <Space.Compact style={{ width: '100%' }}>
                    <Select
                      mode="tags"
                      style={{ width: '100%' }}
                      placeholder="Select or type tags to add (e.g., Market Weekly)"
                      value={[]}
                      onChange={(values) => {
                        if (values.length > 0) {
                          // Add new unique tags
                          const newTags = values.filter(v => !state.editTags.includes(v));
                          if (newTags.length > 0) {
                            setState(prev => ({
                              ...prev,
                              editTags: [...prev.editTags, ...newTags]
                            }));
                          }
                        }
                      }}
                      loading={state.loadingTags}
                      maxTagCount="responsive"
                    >
                      {state.availableTags.map(tag => (
                        <Select.Option key={tag.tag_name} value={tag.tag_name}>
                          {tag.tag_name} ({tag.count})
                        </Select.Option>
                      ))}
                    </Select>
                  </Space.Compact>
                </Space>
              </Form.Item>
              
              <Form.Item label="Description" required>
                <TextArea
                  value={state.editDescription}
                  onChange={(e) => setState(prev => ({ ...prev, editDescription: e.target.value }))}
                  placeholder="Enter detailed theme description..."
                  autoSize={{ minRows: 6, maxRows: 12 }}
                  maxLength={2000}
                  showCount
                />
              </Form.Item>
              
              <Divider />
              
              <Form.Item 
                label="Associated Tickers" 
                help="Add or remove tickers. Changes will trigger description and vector regeneration with auto-verification."
              >
                {state.editLoadingTickers ? (
                  <div style={{ textAlign: 'center', padding: '20px' }}>
                    <SyncOutlined spin style={{ fontSize: '24px' }} />
                    <div style={{ marginTop: '8px' }}>Loading tickers...</div>
                  </div>
                ) : (
                  <>
                    {/* Current Tickers */}
                    <div style={{ marginBottom: '12px' }}>
                      <Text strong style={{ fontSize: '12px', color: colors.textSecondary }}>Current Tickers:</Text>
                      <div style={{ marginTop: '8px' }}>
                        {state.editTickers.length === 0 ? (
                          <Text type="secondary">No tickers associated</Text>
                        ) : (
                          <Space size={[0, 8]} wrap>
                            {state.editTickers.map((assoc) => (
                              <Tag
                                key={assoc.ticker}
                                closable
                                onClose={() => handleRemoveTicker(assoc.ticker)}
                                color={assoc.is_verified ? 'green' : 'default'}
                                icon={assoc.is_verified ? <CheckCircleOutlined /> : null}
                              >
                                {assoc.ticker}
                              </Tag>
                            ))}
                          </Space>
                        )}
                      </div>
                    </div>
                    
                    {/* Add New Tickers */}
                    <div>
                      <Text strong style={{ fontSize: '12px', color: colors.textSecondary }}>Add New Tickers:</Text>
                      <div style={{ marginTop: '8px', display: 'flex', gap: '8px' }}>
                        <Input
                          placeholder="e.g. NVDA.US, AMD.US, 0700.HK (comma-separated)"
                          value={state.editTickersToAdd}
                          onChange={(e) => setState(prev => ({ ...prev, editTickersToAdd: e.target.value }))}
                          onPressEnter={handleAddTickers}
                          style={{ flex: 1 }}
                        />
                        <Button
                          type="primary"
                          icon={<PlusOutlined />}
                          onClick={handleAddTickers}
                          disabled={!state.editTickersToAdd.trim()}
                        >
                          Add
                        </Button>
                      </div>
                    </div>
                  </>
                )}
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
        styles={modalStyles}
      >
        {state.createStep === 1 && (
          <div>

            <Form layout="vertical">
              <Form.Item label="My Tags" help="🏷️ Personal tags for organizing your themes (e.g., Market Weekly, Broker Report). These tags are private to you - other users cannot see them. You can tag any theme, including shared ones.">
                <Select
                  mode="tags"
                  style={{ width: '100%' }}
                  placeholder="Select or enter tags (e.g., Market Weekly, Broker Report)"
                  value={state.newThemeTags}
                  onChange={(values) => setState(prev => ({ ...prev, newThemeTags: values }))}
                  loading={state.loadingTags}
                  maxTagCount="responsive"
                >
                  {state.availableTags.map(tag => (
                    <Select.Option key={tag.tag_name} value={tag.tag_name}>
                      {tag.tag_name} ({tag.count})
                    </Select.Option>
                  ))}
                </Select>
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
              {state.newThemeTags.length > 0 && (
                <div>
                  <Text strong>Tags:</Text>{' '}
                  <Space size={[0, 4]} wrap>
                    {state.newThemeTags.map(tag => (
                      <Tag key={tag} color="blue">{tag}</Tag>
                    ))}
                  </Space>
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

      {/* Batch Tag Modal */}
      <Modal
        title={
          <Space>
            {state.batchTagAction === 'add' ? <PlusOutlined /> : <CloseCircleOutlined />}
            <span>{state.batchTagAction === 'add' ? '批量添加标签' : '批量移除标签'}</span>
          </Space>
        }
        open={state.batchTagModalVisible}
        onCancel={handleCloseBatchTagModal}
        onOk={handleBatchTag}
        confirmLoading={state.batchTagging}
        okText={state.batchTagAction === 'add' ? '添加标签' : '移除标签'}
        cancelText="取消"
        width={600}
        styles={modalStyles}
        okButtonProps={{
          style: {
            borderRadius: 10,
            fontWeight: 500,
            ...(isDark ? { background: '#2196F3', borderColor: '#2196F3' } : {}),
          },
        }}
        cancelButtonProps={{
          style: isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : undefined,
        }}
      >
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          <Alert
            message="提示"
            description={
              <div>
                <div>已选择 {state.selectedRowKeys.length} 个主题</div>
                <div style={{ marginTop: 8 }}>
                  {state.batchTagAction === 'add' 
                    ? '请选择要添加的标签。如果标签不存在，将自动创建。' 
                    : '请选择要移除的标签。'}
                </div>
              </div>
            }
            type="info"
            showIcon
          />
          
          <div>
            <Text strong style={{ display: 'block', marginBottom: 8 }}>
              {state.batchTagAction === 'add' ? '选择或输入标签' : '选择要移除的标签'}:
            </Text>
            <Select
              mode="tags"
              style={{ width: '100%' }}
              placeholder={state.batchTagAction === 'add' ? '选择现有标签或输入新标签（支持多选）' : '选择要移除的标签'}
              value={state.selectedTags}
              onChange={(values) => setState(prev => ({ ...prev, selectedTags: values }))}
              loading={state.loadingTags}
              disabled={state.batchTagging}
              maxTagCount="responsive"
            >
              {state.availableTags.map(tag => (
                <Select.Option key={tag.tag_name} value={tag.tag_name}>
                  {tag.tag_name} ({tag.count})
                </Select.Option>
              ))}
            </Select>
            <Text type="secondary" style={{ fontSize: 12, marginTop: 4, display: 'block' }}>
              {state.batchTagAction === 'add' 
                ? '支持输入新标签名称，如 "Market Weekly"、"Broker Report" 等' 
                : '只能移除现有标签'}
            </Text>
          </div>
          
          {state.availableTags.length > 0 && (
            <div>
              <Text strong style={{ display: 'block', marginBottom: 8 }}>
                当前可用标签:
              </Text>
              <Space wrap>
                {state.availableTags.slice(0, 10).map(tag => (
                  <Tag 
                    key={tag.tag_name} 
                    color="blue"
                    style={{ cursor: 'pointer' }}
                    onClick={() => {
                      if (!state.selectedTags.includes(tag.tag_name)) {
                        setState(prev => ({
                          ...prev,
                          selectedTags: [...prev.selectedTags, tag.tag_name]
                        }));
                      }
                    }}
                  >
                    {tag.tag_name} ({tag.count})
                  </Tag>
                ))}
                {state.availableTags.length > 10 && (
                  <Text type="secondary">...共 {state.availableTags.length} 个标签</Text>
                )}
              </Space>
            </div>
          )}
        </Space>
      </Modal>

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
        styles={modalStyles}
        footer={
          state.batchTaskId ? (
            <Button
              onClick={handleCloseBatchUploadModal}
              style={{
                borderRadius: 8,
                ...(isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : {}),
              }}
            >
              Close
            </Button>
          ) : (
            <>
              <Button
                onClick={handleCloseBatchUploadModal}
                style={{
                  borderRadius: 8,
                  ...(isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : {}),
                }}
              >
                Cancel
              </Button>
              <Button
                type="primary"
                onClick={handleStartBatchUpload}
                loading={state.uploadingFile}
                disabled={!state.uploadedFile}
                style={{
                  borderRadius: 10,
                  fontWeight: 500,
                  ...(isDark ? { background: '#2196F3', borderColor: '#2196F3' } : {}),
                }}
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

              {/* Tags Input */}
              <div style={{ marginBottom: 16 }}>
                <Text strong style={{ display: 'block', marginBottom: 8 }}>
                  My Tags <Text type="secondary">(Optional)</Text>
                </Text>
                <Select
                  mode="tags"
                  style={{ width: '100%' }}
                  placeholder="Your personal tags (e.g., Market Weekly, Broker Report)"
                  value={state.batchUploadTags}
                  onChange={(values) => setState(prev => ({ ...prev, batchUploadTags: values }))}
                  disabled={state.uploadingFile || !!state.batchTaskId}
                  loading={state.loadingTags}
                  maxTagCount="responsive"
                >
                  {state.availableTags.map(tag => (
                    <Select.Option key={tag.tag_name} value={tag.tag_name}>
                      {tag.tag_name} ({tag.count})
                    </Select.Option>
                  ))}
                </Select>
                <Text type="secondary" style={{ fontSize: 12 }}>
                  🏷️ All themes in this batch will be tagged with your personal tags. These are only visible to you.
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
                  CSV or Excel with columns: theme, category, ticker, company (optional).
                  Multiple theme names per file are supported; each (theme, category) pair creates one investment theme.
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
              
              {/* Theme Names */}
              <Card size="small" style={cardStyle}>
                <Text strong>Theme Names ({uploadPreview.theme_names.length}):</Text>
                <div style={{ marginTop: 8 }}>
                  <Space wrap size={[4, 8]}>
                    {uploadPreview.theme_names.map((name) => (
                      <Tag key={name} color="blue">{name}</Tag>
                    ))}
                  </Space>
                </div>
                <Text type="secondary" style={{ fontSize: '12px', display: 'block', marginTop: 8 }}>
                  Each (theme, category) pair will create one investment theme
                </Text>
              </Card>
              
              <Row gutter={16}>
                <Col span={12}>
                  <Card size="small" style={cardStyle}>
                    <Text strong>Records to Create:</Text> {uploadPreview.total_themes}
                    <br />
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      (= Number of theme + category groups)
                    </Text>
                  </Card>
                </Col>
                <Col span={12}>
                  <Card size="small" style={cardStyle}>
                    <Text strong>Total Tickers:</Text> {uploadPreview.total_tickers}
                  </Card>
                </Col>
              </Row>

              {/* Per-theme breakdown */}
              {uploadPreview.themes && Object.keys(uploadPreview.themes).length > 0 && (
                <Collapse
                  size="small"
                  items={Object.entries(uploadPreview.themes).map(([themeName, info]) => ({
                    key: themeName,
                    label: `${themeName} (${Object.keys(info.categories).length} categories, ${info.total_tickers} tickers)`,
                    children: (
                      <Space wrap>
                        {Object.entries(info.categories).map(([category, count]) => (
                          <Tag key={`${themeName}-${category}`} color="green">
                            {category}: {count} tickers
                          </Tag>
                        ))}
                      </Space>
                    ),
                  }))}
                />
              )}

              {/* Categories Distribution (aggregated, legacy fallback) */}
              {uploadPreview.categories && Object.keys(uploadPreview.categories).length > 0 && (
                <Card size="small" style={cardStyle} title="All Categories (aggregated)">
                  <Space wrap>
                    {Object.entries(uploadPreview.categories).map(([category, count]) => (
                      <Tag key={category} color="default">
                        {category}: {count} tickers
                      </Tag>
                    ))}
                  </Space>
                </Card>
              )}

            </>
          )}

          {/* Progress Display */}
          {state.batchTaskId && (
            <>
              <Divider>Processing: llm description generation, vectorization, ticker verification</Divider>
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

const InvestmentThemesPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
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
      <App>
        <InvestmentThemesPageInner />
      </App>
    </ConfigProvider>
  );
};

export default InvestmentThemesPage;

