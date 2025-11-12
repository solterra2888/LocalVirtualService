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
  verifyTicker
} from '../services/investmentThemes';
import {
  InvestmentTheme,
  ThemeMatch,
  ThemeAssociation,
  getDescriptionSourceLabel
} from '../types/investmentThemes';
import { getErrorMessage } from '../utils/errorHandler';
import { useAuthStore } from '../store/authStore';

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
  editLoading: boolean;
  selectedRowKeys: React.Key[];
  createModalVisible: boolean;
  createStep: number;
  newThemeName: string;
  newThemeCategory: string;
  newThemeId: string;
  newThemeMarket: string;
  newThemeTickers: string;
  newThemeDescription: string;
  generatingDescription: boolean;
  creatingTheme: boolean;
  helpModalVisible: boolean;
  // 批量上传相关
  batchUploadModalVisible: boolean;
  uploadingFile: boolean;
  uploadedFile: File | null;
  batchUploadMarket: string;  // 新增：market 选择
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
}

const InvestmentThemesPage: React.FC = () => {
  const { user } = useAuthStore();
  const isAdmin = user?.is_admin === true;
  
  const [state, setState] = useState<PageState>({
    loading: false,
    themes: [],
    total: 0,
    currentPage: 1,
    pageSize: 20,
    initializing: false,
    testModalVisible: false,
    testNewsText: '',
    testResults: [],
    testLoading: false,
    editModalVisible: false,
    editingTheme: null,
    editDescription: '',
    editLoading: false,
    selectedRowKeys: [],
    createModalVisible: false,
    createStep: 1,
    newThemeName: '',
    newThemeCategory: '',
    newThemeId: '',
    newThemeMarket: 'US',
    newThemeTickers: '',
    newThemeDescription: '',
    generatingDescription: false,
    creatingTheme: false,
    helpModalVisible: false,
    // 批量上传相关
    batchUploadModalVisible: false,
    uploadingFile: false,
    uploadedFile: null,
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
    searchKeyword: ''
  });


  // Load theme list
  const loadThemes = async (page?: number, pageSize?: number, searchKeyword?: string) => {
    setState(prev => ({ ...prev, loading: true }));
    try {
      const currentPage = page || state.currentPage;
      const currentPageSize = pageSize || state.pageSize;
      const effectiveSearch = searchKeyword !== undefined ? searchKeyword : state.searchKeyword;
      const response = await listThemes(
        currentPageSize,
        (currentPage - 1) * currentPageSize,
        effectiveSearch
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
    loadThemes(1, state.pageSize, trimmed);
  };

  const handleSearchInputChange = (event: React.ChangeEvent<HTMLInputElement>) => {
    const { value } = event.target;
    setState(prev => ({
      ...prev,
      searchKeyword: value
    }));

    if (value === '') {
      loadThemes(1, state.pageSize, '');
    }
  };

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

  // Handle edit description (Admin only)
  const handleEditDescription = (theme: InvestmentTheme) => {
    if (!isAdmin) {
      message.error('Only administrators can perform this operation');
      return;
    }

    setState(prev => ({
      ...prev,
      editModalVisible: true,
      editingTheme: theme,
      editDescription: theme.description_preview || ''
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
      const result = await updateThemeDescription(
        state.editingTheme.theme_id,
        state.editDescription,
        state.editingTheme.version
      );

      if (result.description_saved) {
        if (result.embedding_updated) {
          message.success('Description and vector updated successfully');
        } else {
          message.warning(`Description saved, but vector update failed: ${result.error || 'Unknown error'}`);
        }
        
        setState(prev => ({
          ...prev,
          editModalVisible: false,
          editingTheme: null,
          editDescription: '',
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

  // Handle delete theme (Admin only)
  const handleDeleteTheme = (theme: InvestmentTheme) => {
    if (!isAdmin) {
      message.error('Only administrators can perform this operation');
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

  // Handle batch delete themes (Admin only)
  const handleBatchDelete = () => {
    if (!isAdmin) {
      message.error('Only administrators can perform this operation');
      return;
    }

    if (state.selectedRowKeys.length === 0) {
      message.warning('Please select themes to delete');
      return;
    }

    // Find selected themes details
    const selectedThemes = state.themes.filter(theme => 
      state.selectedRowKeys.includes(theme.theme_id)
    );

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
        state.newThemeMarket || undefined,
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
      
      const result = await createTheme(
        state.newThemeName,
        state.newThemeCategory,
        state.newThemeDescription,
        state.newThemeId || undefined,
        state.newThemeMarket || undefined,
        tickersArray
      );
      
      if (result.theme_saved) {
        if (result.embedding_created) {
          message.success(`Theme '${state.newThemeName}' created successfully!`);
        } else {
          message.warning(`Theme saved, but vector generation failed: ${result.error || 'Unknown error'}`);
        }
        
        // Reset and close modal
        setState(prev => ({
          ...prev,
          createModalVisible: false,
          createStep: 1,
          newThemeName: '',
          newThemeCategory: '',
          newThemeId: '',
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
    } catch (error) {
      message.error(`Create theme failed: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, creatingTheme: false }));
    }
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
          newThemeName: '',
          newThemeCategory: '',
          newThemeId: '',
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
      const result = await batchUploadThemes(state.uploadedFile, state.batchUploadMarket);
      
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
        message.success(
          `Batch upload completed: ${result.statistics?.created || 0} created, ${result.statistics?.failed || 0} failed, ${result.statistics?.skipped || 0} skipped`
        );
        
        setState(prev => ({
          ...prev,
          batchTaskDetails: result.details || [],
          batchTaskProgress: 100,
        }));
        
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

    const createRow = (row: string[]) =>
      `<tr>${row.map(cell => `<td style="mso-number-format:'@';">${cell}</td>`).join('')}</tr>`;

    const tableHtml =
      `<table border="1"><thead>${createRow(headers)}</thead><tbody>${sampleRows.map(createRow).join('')}</tbody></table>`;

    const blob = new Blob(['\ufeff' + tableHtml], {
      type: 'application/vnd.ms-excel'
    });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = 'investment_theme_batch_template.xls';
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
      title: 'Created By',
      dataIndex: 'created_by',
      key: 'created_by',
      render: (createdBy?: string) =>
        createdBy ? <Text>{createdBy}</Text> : <Text type="secondary">Unknown</Text>
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
      title: 'Updated',
      dataIndex: 'updated_at',
      key: 'updated_at',
      render: (date: string) => date ? new Date(date).toLocaleString('en-US', { 
        month: 'short', 
        day: 'numeric', 
        hour: '2-digit', 
        minute: '2-digit' 
      }) : '-'
    },
    ...(isAdmin ? [{
      title: 'Actions',
      key: 'actions',
      render: (_: any, record: InvestmentTheme) => (
        <Space size="small">
          <Tooltip title="Edit Description">
            <Button
              type="link"
              size="small"
              icon={<EditOutlined />}
              onClick={() => handleEditDescription(record)}
            />
          </Tooltip>
          <Tooltip title="Delete Theme">
            <Button
              type="link"
              size="small"
              danger
              icon={<DeleteOutlined />}
              onClick={() => handleDeleteTheme(record)}
            />
          </Tooltip>
        </Space>
      )
    }] : [])
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
              Intelligent theme matching system based on Reverse Capital Embedding technique, automatically associating news with 150+ investment themes through semantic analysis
            </Paragraph>
          </div>
          <Button
            type="primary"
            ghost
            icon={<InfoCircleOutlined />}
            onClick={() => setState(prev => ({ ...prev, helpModalVisible: true }))}
            style={{ marginTop: '8px' }}
          >
            How to Use
          </Button>
        </div>
      </div>


      {/* Action Buttons - Optimized Layout */}
      <Card style={{ marginBottom: '24px' }}>

         {/* Operations */}
         <div style={{ marginBottom: '16px' }}>
           <Space size="middle" wrap>
            {isAdmin && (
              <>
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
                
                <Tooltip title="Delete selected themes (soft delete)">
                  <Button
                    danger
                    icon={<DeleteOutlined />}
                    onClick={handleBatchDelete}
                    disabled={state.selectedRowKeys.length === 0}
                  >
                    Batch Delete {state.selectedRowKeys.length > 0 ? `(${state.selectedRowKeys.length})` : ''}
                  </Button>
                </Tooltip>
              </>
            )}
            
            <Tooltip title="Generate descriptions and vectors for new themes, suitable after adding new themes">
              <Button
                type={isAdmin ? "default" : "primary"}
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
            
            <Tooltip title="Enter news text to test theme matching in real-time">
              <Button
                icon={<SearchOutlined />}
                onClick={() => setState(prev => ({ ...prev, testModalVisible: true }))}
              >
                Test Match
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
          <Space size="middle">
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
          </Space>
        }
      >
        <Table
          columns={columns}
          dataSource={state.themes}
          rowKey="theme_id"
          loading={state.loading}
          rowSelection={isAdmin ? {
            selectedRowKeys: state.selectedRowKeys,
            onChange: (selectedRowKeys: React.Key[]) => {
              setState(prev => ({ ...prev, selectedRowKeys }));
            },
            selections: [
              Table.SELECTION_ALL,
              Table.SELECTION_INVERT,
              Table.SELECTION_NONE,
            ],
          } : undefined}
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
                pageSize: pageSize || 20,
                selectedRowKeys: []  // 清空选中项
              }));
              loadThemes(page, pageSize || 20);  // 传递新的 pageSize
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
                        <Text strong>{result.theme_name}</Text>
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
        title={`Edit Theme Description${state.editingTheme ? ` - ${state.editingTheme.theme_name}` : ''}`}
        open={state.editModalVisible}
        onCancel={() => setState(prev => ({ 
          ...prev, 
          editModalVisible: false, 
          editingTheme: null,
          editDescription: '' 
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
                  <li>Description will be saved immediately</li>
                  <li>Vector will be regenerated automatically (may take a few seconds)</li>
                  <li>If vectorization fails, description is still saved</li>
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
            <Alert
              message="Step 1: Basic Information"
              description="Fill in the theme name, category, and optionally associate stock tickers."
              type="info"
              showIcon
              style={{ marginBottom: 24 }}
            />
            
            <Form layout="vertical">
              <Form.Item label="Theme Name" required>
                <Input
                  placeholder="e.g. AI Chips, Clean Energy, Quantum Computing"
                  value={state.newThemeName}
                  onChange={(e) => setState(prev => ({ ...prev, newThemeName: e.target.value }))}
                  maxLength={200}
                  showCount
                />
              </Form.Item>
              
              <Form.Item label="Category" required>
                <Input
                  placeholder="e.g. Technology, Healthcare, Finance, Energy"
                  value={state.newThemeCategory}
                  onChange={(e) => setState(prev => ({ ...prev, newThemeCategory: e.target.value }))}
                  maxLength={100}
                  showCount
                />
              </Form.Item>
              
              <Divider style={{ margin: '16px 0' }} />
              
              <Form.Item 
                label="Associated Stocks (Optional)" 
                help="Select market and enter ticker symbols to associate with this theme"
              >
                <Space direction="vertical" style={{ width: '100%' }} size="small">
                  <Select
                    value={state.newThemeMarket}
                    onChange={(value) => setState(prev => ({ ...prev, newThemeMarket: value }))}
                    style={{ width: 200 }}
                  >
                    <Select.Option value="US">🇺🇸 US Market</Select.Option>
                    <Select.Option value="HK">🇭🇰 HK Market</Select.Option>
                  </Select>
                  
                  <Input
                    placeholder={`Enter tickers separated by commas (e.g. ${state.newThemeMarket === 'US' ? 'NVDA, AMD, INTC' : '0700, 9988, 3690'})`}
                    value={state.newThemeTickers}
                    onChange={(e) => setState(prev => ({ ...prev, newThemeTickers: e.target.value }))}
                    maxLength={500}
                  />
                  
                  {state.newThemeTickers && (
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      Preview: {state.newThemeTickers.split(',').map(t => t.trim()).filter(Boolean).map(t => `${t}.${state.newThemeMarket}`).join(', ')}
                    </Text>
                  )}
                </Space>
              </Form.Item>
              
              <Divider style={{ margin: '16px 0' }} />
              
              <Form.Item 
                label="Theme ID (Optional)" 
                help="Leave empty to auto-generate (format: CUSTOM_TIMESTAMP)"
              >
                <Input
                  placeholder="e.g. AI_CHIP_US (optional)"
                  value={state.newThemeId}
                  onChange={(e) => setState(prev => ({ ...prev, newThemeId: e.target.value }))}
                  maxLength={50}
                />
              </Form.Item>
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
              <div>
                <Text strong>Theme Name:</Text> <Text>{state.newThemeName}</Text>
              </div>
              <div>
                <Text strong>Category:</Text> <Text>{state.newThemeCategory}</Text>
              </div>
              {state.newThemeTickers && (
                <div>
                  <Text strong>Associated Stocks:</Text> <Text>{state.newThemeTickers.split(',').map(t => t.trim()).filter(Boolean).map(t => `${t}.${state.newThemeMarket}`).join(', ')}</Text>
                </div>
              )}
              {state.newThemeId && (
                <div>
                  <Text strong>Theme ID:</Text> <Text code>{state.newThemeId}</Text>
                </div>
              )}
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

      {/* Help Modal */}
      <Modal
        title={
          <Space>
            <InfoCircleOutlined style={{ color: '#1890ff' }} />
            <span>Investment Themes Settings - User Guide</span>
          </Space>
        }
        open={state.helpModalVisible}
        onCancel={() => setState(prev => ({ ...prev, helpModalVisible: false }))}
        footer={null}
        width={900}
        style={{ top: 20 }}
      >
        <div 
          dangerouslySetInnerHTML={{ 
            __html: `
              <style>
                .help-guide {
                  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
                  line-height: 1.8;
                  color: #333;
                }
                .help-guide h3 {
                  color: #1890ff;
                  border-left: 4px solid #1890ff;
                  padding-left: 12px;
                  margin-top: 24px;
                  margin-bottom: 12px;
                }
                .help-guide h4 {
                  color: #52c41a;
                  margin-top: 16px;
                  margin-bottom: 8px;
                }
                .help-guide .flow-step {
                  background: #f0f9ff;
                  border: 2px solid #91d5ff;
                  border-radius: 8px;
                  padding: 16px;
                  margin: 12px 0;
                  position: relative;
                  padding-left: 50px;
                }
                .help-guide .flow-step::before {
                  content: counter(step-counter);
                  counter-increment: step-counter;
                  position: absolute;
                  left: 16px;
                  top: 16px;
                  background: #1890ff;
                  color: white;
                  width: 28px;
                  height: 28px;
                  border-radius: 50%;
                  display: flex;
                  align-items: center;
                  justify-content: center;
                  font-weight: bold;
                  font-size: 14px;
                }
                .help-guide .flow-container {
                  counter-reset: step-counter;
                }
                .help-guide .icon-badge {
                  display: inline-block;
                  background: #1890ff;
                  color: white;
                  padding: 2px 8px;
                  border-radius: 4px;
                  font-size: 12px;
                  font-weight: bold;
                  margin-right: 8px;
                }
                .help-guide .tip {
                  background: #fff7e6;
                  border-left: 4px solid #faad14;
                  padding: 12px;
                  margin: 12px 0;
                  border-radius: 4px;
                }
                .help-guide .warning {
                  background: #fff1f0;
                  border-left: 4px solid #ff4d4f;
                  padding: 12px;
                  margin: 12px 0;
                  border-radius: 4px;
                }
                .help-guide table {
                  width: 100%;
                  border-collapse: collapse;
                  margin: 12px 0;
                }
                .help-guide table th {
                  background: #f0f9ff;
                  padding: 10px;
                  text-align: left;
                  border: 1px solid #d9e7ff;
                  font-weight: bold;
                }
                .help-guide table td {
                  padding: 10px;
                  border: 1px solid #d9e7ff;
                }
                .help-guide .feature-card {
                  background: #f6ffed;
                  border: 2px solid #b7eb8f;
                  border-radius: 8px;
                  padding: 16px;
                  margin: 12px 0;
                }
                .help-guide .feature-card h4 {
                  margin-top: 0;
                }
                .help-guide ul {
                  padding-left: 24px;
                }
                .help-guide ul li {
                  margin: 8px 0;
                }
              </style>
              
              <div class="help-guide">
                <h3>📖 What is This System?</h3>
                <p>
                  This is an <strong>Intelligent Investment Theme Matching System</strong> that automatically associates financial news with 150+ investment themes 
                  (like "Electric Vehicles", "AI Chips", "Clean Energy", etc.) through semantic similarity analysis.
                </p>
                
                <h4>🚀 Key Advantages:</h4>
                <ul>
                  <li>⚡ <strong>Ultra Fast:</strong> 50-100ms response time (20x faster than LLM)</li>
                  <li>💰 <strong>Ultra Low Cost:</strong> ¥0.14 per 1000 news (99% cheaper than LLM)</li>
                  <li>🎯 <strong>High Accuracy:</strong> 82% accuracy rate (threshold 0.60)</li>
                  <li>🔄 <strong>Real-time Updates:</strong> New themes ready instantly after adding</li>
                </ul>

                <h3>🎯 How It Works (3 Simple Steps)</h3>
                <div class="flow-container">
                  <div class="flow-step">
                    <strong>Step 1: Generate Theme Descriptions</strong><br/>
                    Use AI (Qwen LLM) to generate detailed, 150-300 word descriptions for each investment theme.
                  </div>
                  <div class="flow-step">
                    <strong>Step 2: Generate Vectors</strong><br/>
                    Convert theme descriptions into 1024-dimensional vectors using Qwen Embedding API.
                  </div>
                  <div class="flow-step">
                    <strong>Step 3: Match News</strong><br/>
                    When news arrives, convert it to a vector and find the most similar themes using PostgreSQL pgvector.
                  </div>
                </div>

                <h3>🔧 Main Functions</h3>
                
                <div class="feature-card">
                  <span class="icon-badge">1️⃣</span>
                  <strong>Sync Theme Data</strong> (Most Common)
                  <p>Generate descriptions and vectors for submitted themes only. 
                  Perfect for daily maintenance after Futu crawler adds new themes.</p>
                  <div class="tip">💡 Use this: When you see new themes without descriptions</div>
                </div>

                <div class="feature-card">
                  <span class="icon-badge">2️⃣</span>
                  <strong>Optimize Descriptions</strong> (Recommended Monthly)
                  <p>Enhance all theme descriptions using LLM. Upgrade simple template descriptions to detailed AI-generated ones.</p>
                  <div class="tip">💡 Use this: When description coverage is low (&lt;90%)</div>
                </div>

                <div class="feature-card">
                  <span class="icon-badge">3️⃣</span>
                  <strong>Test Match</strong> (Debug & Verify)
                  <p>Enter sample news text to test theme matching in real-time. See which themes match and their similarity scores.</p>
                  <div class="tip">💡 Use this: To verify matching quality or debug issues</div>
                </div>

                <div class="feature-card">
                  <span class="icon-badge">⚠️</span>
                  <strong>Advanced Options</strong> (Use with Caution)
                  <p>Force update all vectors or regenerate all descriptions. Only use when embedding model upgrades or major Prompt changes.</p>
                  <div class="warning">⚠️ High cost and long execution time (15-30 minutes). Use sparingly!</div>
                </div>

                <h3>📊 Quick Reference</h3>
                <table>
                  <tr>
                    <th>Button</th>
                    <th>When to Use</th>
                    <th>Time</th>
                    <th>Cost</th>
                  </tr>
                  <tr>
                    <td>🔄 Sync Theme Data</td>
                    <td>New themes added by crawler</td>
                    <td>2-10 mins</td>
                    <td>Low</td>
                  </tr>
                  <tr>
                    <td>✨ Optimize Descriptions</td>
                    <td>Coverage &lt;90%</td>
                    <td>2-10 mins</td>
                    <td>Medium</td>
                  </tr>
                  <tr>
                    <td>🧪 Test Match</td>
                    <td>Debug or verify quality</td>
                    <td>Instant</td>
                    <td>Free</td>
                  </tr>
                  <tr>
                    <td>⚠️ Force Update Vectors</td>
                    <td>Embedding model upgrade</td>
                    <td>20-30 mins</td>
                    <td>High</td>
                  </tr>
                  <tr>
                    <td>⚠️ Regenerate All Descriptions</td>
                    <td>Prompt template major change</td>
                    <td>15-25 mins</td>
                    <td>High</td>
                  </tr>
                </table>

                <h3>💡 Best Practices</h3>
                <ul>
                  <li><strong>Regular Sync:</strong> Run "Sync Theme Data" weekly to keep themes up to date</li>
                  <li><strong>Check Coverage:</strong> Monitor description coverage in statistics - keep it above 90%</li>
                  <li><strong>Test First:</strong> Always use "Test Match" before deploying to production</li>
                  <li><strong>Batch Operations:</strong> Avoid running multiple heavy operations simultaneously</li>
                  <li><strong>Monitor Costs:</strong> Review API usage logs regularly</li>
                </ul>

                <h3>❓ Troubleshooting</h3>
                <div class="tip">
                  <strong>Q: Why are some themes showing "Not generated"?</strong><br/>
                  A: Click "Sync Theme Data" to generate descriptions and vectors for new themes.
                </div>
                <div class="tip">
                  <strong>Q: Matching quality seems low. What should I do?</strong><br/>
                  A: Try "Optimize Descriptions" to upgrade theme descriptions to AI-generated detailed ones.
                </div>
                <div class="tip">
                  <strong>Q: When should I use Advanced Options?</strong><br/>
                  A: Only when embedding model upgrades or major Prompt template changes. For normal operations, use "Sync Theme Data" and "Optimize Descriptions".
                </div>

                <div style="text-align: center; margin-top: 24px; padding-top: 16px; border-top: 2px solid #e8e8e8; color: #999;">
                  <p>💙 Powered by Qwen Embedding + PostgreSQL pgvector</p>
                </div>
              </div>
            `
          }}
        />
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
              <Alert
                message="File Format Requirements"
                description={
                  <ul style={{ marginBottom: 0, paddingLeft: 20 }}>
                    <li>Support: CSV (.csv) or Excel (.xlsx, .xls) files</li>
                    <li>Required columns: <code>theme</code>, <code>category</code>, <code>ticker</code></li>
                    <li>Optional column: <code>company</code> (company name)</li>
                    <li><strong>Important:</strong> <code>theme</code> column must have only ONE unique value throughout the file</li>
                    <li>Multiple <code>category</code> values → Multiple themes will be created</li>
                    <li>Example: theme='crypto' with 8 categories → Creates 8 themes</li>
                    <li>Maximum file size: 10MB</li>
                  </ul>
                }
                type="info"
                showIcon
              />

              <Button
                icon={<DownloadOutlined />}
                onClick={handleDownloadTemplate}
                type="default"
              >
                Download Excel Template
              </Button>

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

