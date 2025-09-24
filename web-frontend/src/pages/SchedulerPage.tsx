import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  Row,
  Col,
  Button,
  Space,
  Tag,
  Typography,
  Switch,
  Statistic,
  Alert,
  message,
  Modal,
  Form,
  Input,
  Select,
  Tooltip,
  Tabs,
  DatePicker,
  InputNumber,

  Table,
  Badge,
  Popconfirm
} from 'antd';
import {
  ReloadOutlined,
  SettingOutlined,
  SyncOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
  ClockCircleOutlined,
  DatabaseOutlined,
  GlobalOutlined,



  InfoCircleOutlined,
  CloudDownloadOutlined,
  TagOutlined,
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  BulbOutlined,
  ScheduleOutlined,
  FilterOutlined,
  BarChartOutlined
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { api } from '../services/api';
import { newsAPI } from '../services/news';
import { schedulerAPI, ScheduledTask } from '../services/scheduler';

import DatabaseTableViewer from '../components/DatabaseTableViewer';
import TaskExecutionHistory from '../components/TaskExecutionHistory';

import DataSourceConfigModal from '../components/DataSourceConfigModal';
import TaskStatusModal from '../components/TaskStatusModal';

const { Title, Text } = Typography;
const { RangePicker } = DatePicker;
const { Option } = Select;

// Data source configuration
interface DataSource {
  id: string;
  name: string;
  type: 'eodhd' | 'watchlist' | 'alpha_vantage' | 'finviz' | 'finviz_screener' | 'eodhd_events' | 'eodhd_macro' | 'eodhd_fundamentals' | 'eodhd_stock_prices' | 'other';
  description: string;
  enabled: boolean;
  status: 'idle' | 'running' | 'success' | 'error';
  lastRun?: string;
  lastSuccess?: string;
  quickActions: QuickAction[];
}

interface QuickAction {
  id: string;
  name: string;
  description: string;
  endpoint: string;
  params?: Record<string, any>;
  icon: React.ReactNode;
}

// News theme interface
interface NewsTheme {
  id: number;
  topic_name: string;
  description?: string;
  color?: string;
  is_active: boolean;
  keywords: string[];
}



const SchedulerPage: React.FC = () => {
  const [dataSources, setDataSources] = useState<DataSource[]>([
    {
      id: 'eodhd_news',
      name: 'EODHD News Data',
      type: 'eodhd',
      description: 'Get latest stock news from EODHD API',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_cron_schedule',
          name: 'Configure Data Fetch Strategy',
          description: 'Set up Cron-based periodic data fetching with NY timezone every 15 minutes',
          endpoint: '/scheduler/tasks/eodhd-news',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'watchlist_news',
      name: 'My Watchlist News',
      type: 'watchlist',
      description: 'Get news for stocks in your watchlist from EODHD API',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_watchlist_schedule',
          name: 'Configure Watchlist News Fetch',
          description: 'Set up Cron-based periodic news fetching for your watchlist stocks',
          endpoint: '/scheduler/tasks/watchlist-news',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'eodhd_stock_prices',
      name: 'EODHD Stock Prices',
      type: 'eodhd_stock_prices',
      description: 'Get real-time stock prices for watchlist stocks from EODHD API',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_stock_prices_schedule',
          name: 'Configure Stock Prices Fetch',
          description: 'Set up periodic real-time stock price fetching for watchlist stocks (every 5 minutes)',
          endpoint: '/scheduler/tasks/eodhd-stock-prices',
          params: {},
          icon: <BarChartOutlined />
        }
      ]
    },
    {
      id: 'eodhd_events',
      name: 'EODHD Economic Events',
      type: 'eodhd_events',
      description: 'Get economic calendar event data from EODHD API',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_events_schedule',
          name: 'Configure Data Fetch Strategy',
          description: 'Set up Cron-based periodic economic events fetching',
          endpoint: '/scheduler/tasks/economic-events',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'eodhd_macro',
      name: 'EODHD Macro Indicators',
      type: 'eodhd_macro',
      description: 'Get macroeconomic indicator data from EODHD API',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_macro_schedule',
          name: 'Configure Data Fetch Strategy',
          description: 'Set up Cron-based periodic macro indicators fetching',
          endpoint: '/scheduler/tasks/macro-indicators',
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'eodhd_fundamentals',
      name: 'EODHD Fundamentals',
      type: 'eodhd_fundamentals',
      description: 'Manually get company fundamentals data from EODHD',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'update_fundamentals_data',
          name: 'Update Data',
          description: 'Manually update fundamentals data for multiple markets with comprehensive data types',
          endpoint: '/scheduler/tasks/eodhd-fundamentals',
          params: {},
          icon: <CloudDownloadOutlined />
        }
      ]
    },
    {
      id: 'alpha_vantage',
      name: 'Alpha Vantage',
      type: 'alpha_vantage',
      description: 'Financial data API - stock prices, technical indicators, etc. (Coming soon)',
      enabled: false,
      status: 'idle',
      quickActions: [
        {
          id: 'fetch_stock_data',
          name: 'Configure Data Fetch Strategy',
          description: 'Get real-time stock prices and historical data',
          endpoint: '/alpha-vantage/stock-data',
          icon: <GlobalOutlined />
        }
      ]
    },
    {
      id: 'finviz_screener',
      name: 'Finviz Screener',
      type: 'finviz_screener',
      description: 'Comprehensive stock screener data from Finviz with technical and fundamental metrics',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_finviz_screener_schedule',
          name: 'Configure Screener Data Fetch',
          description: 'Set up periodic stock screener data fetching from Finviz',
          endpoint: '/scheduler/tasks/finviz-screener',
          params: {},
          icon: <FilterOutlined />
        }
      ]
    }
  ]);

  const [loading, setLoading] = useState(false);
  const [quickActionModal, setQuickActionModal] = useState<{
    visible: boolean;
    action: QuickAction | null;
    dataSource: DataSource | null;
  }>({
    visible: false,
    action: null,
    dataSource: null
  });
  
  // News related state
  const [themes, setThemes] = useState<NewsTheme[]>([]);

  const [themeModalVisible, setThemeModalVisible] = useState(false);
  const [selectedTheme, setSelectedTheme] = useState<NewsTheme | null>(null);
  const [themeForm] = Form.useForm();
  
  // Scheduled task related state
  const [scheduledTasks, setScheduledTasks] = useState<ScheduledTask[]>([]);

  const [dataSourceConfigVisible, setDataSourceConfigVisible] = useState(false);
  const [selectedDataSource, setSelectedDataSource] = useState<DataSource | null>(null);

  // Task status monitoring modal
  const [taskStatusVisible, setTaskStatusVisible] = useState(false);
  const [selectedTask, setSelectedTask] = useState<ScheduledTask | null>(null);
  
  // Database table viewing functionality moved to DatabaseTableViewer component
  
  const [form] = Form.useForm();
  
  // Active tab state for refresh functionality
  const [activeTab, setActiveTab] = useState<string>('scheduler_config');
  
  // Refresh triggers for child components
  const [taskManagementRefreshTrigger, setTaskManagementRefreshTrigger] = useState<number>(0);
  const [executionHistoryRefreshTrigger, setExecutionHistoryRefreshTrigger] = useState<number>(0);

  // Load scheduled task data when page loads
  useEffect(() => {
    loadScheduledTasks();
  }, []);

  // Handle tab change and refresh data
  const handleTabChange = (key: string) => {
    setActiveTab(key);
    
    // Refresh data when switching to specific tabs
    if (key === 'scheduler_config') {
      loadScheduledTasks();
    } else if (key === 'task_management') {
      setTaskManagementRefreshTrigger(prev => prev + 1);
    } else if (key === 'execution_history') {
      setExecutionHistoryRefreshTrigger(prev => prev + 1);
    }
  };

  // View task status
  // const handleViewTaskStatus = (task: ScheduledTask) => {
  //   setSelectedTask(task);
  //   setTaskStatusVisible(true);
  // };

  // Sync data source status with scheduled task status
  const syncDataSourceStatus = useCallback(() => {
    setDataSources(prev => prev.map(source => {
      // EODHD Fundamentals always shows as enabled because it only supports manual trigger
      if (source.id === 'eodhd_fundamentals') {
        return { ...source, enabled: true };
      }
      
      // Find corresponding scheduled tasks
      const relatedTasks = scheduledTasks.filter(task => {
        const taskType = task.type.toLowerCase();
        const sourceIdLower = source.id.toLowerCase();
        
        return (
          (sourceIdLower === 'eodhd_news' && taskType === 'eodhd_news') ||
          (sourceIdLower === 'watchlist_news' && taskType === 'watchlist_news') ||
          (sourceIdLower === 'eodhd_stock_prices' && taskType === 'eodhd_stock_prices') ||
          (sourceIdLower.includes('events') && taskType.includes('events')) ||
          (sourceIdLower.includes('macro') && (taskType.includes('macro') || taskType.includes('indicators'))) ||
          (sourceIdLower.includes('exchange') && taskType.includes('exchange')) ||
          (sourceIdLower.includes('fundamentals') && taskType.includes('fundamentals')) ||
          (sourceIdLower.includes('finviz_screener') && taskType.includes('finviz_screener'))
        );
      });
      
      // If there are related tasks, use the task's enabled status
      if (relatedTasks.length > 0) {
        // As long as one task is enabled, consider the data source enabled
        const hasEnabledTask = relatedTasks.some(task => task.enabled);
        return { ...source, enabled: hasEnabledTask };
      }
      
      // No related tasks, maintain original status (Coming soon data sources)
      return source;
    }));
  }, [scheduledTasks]);

  // Sync data source status when scheduled task list updates
  useEffect(() => {
    if (scheduledTasks.length > 0) {
      syncDataSourceStatus();
    }
  }, [scheduledTasks, syncDataSourceStatus]);

  // Get status tag
  const getStatusTag = (status: string) => {
    const statusConfig = {
      running: { color: 'processing', icon: <SyncOutlined spin />, text: 'Running' },
      success: { color: 'success', icon: <CheckCircleOutlined />, text: 'Success' },
      error: { color: 'error', icon: <ExclamationCircleOutlined />, text: 'Error' },
      idle: { color: 'default', icon: <ClockCircleOutlined />, text: 'Idle' }
    };
    
    const config = statusConfig[status as keyof typeof statusConfig];
    return (
      <Tag color={config.color} icon={config.icon}>
        {config.text}
      </Tag>
    );
  };

  // Get related scheduled tasks for a data source
  const getRelatedTasks = (sourceId: string) => {
    return scheduledTasks.filter(task => {
      const taskType = task.type.toLowerCase();
      const sourceIdLower = sourceId.toLowerCase();
      
      return (
        (sourceIdLower === 'eodhd_news' && taskType === 'eodhd_news') ||
        (sourceIdLower === 'watchlist_news' && taskType === 'watchlist_news') ||
        (sourceIdLower === 'eodhd_stock_prices' && taskType === 'eodhd_stock_prices') ||
        (sourceIdLower.includes('events') && taskType.includes('events')) ||
        (sourceIdLower.includes('macro') && (taskType.includes('macro') || taskType.includes('indicators'))) ||
        (sourceIdLower.includes('exchange') && taskType.includes('exchange')) ||
        (sourceIdLower.includes('fundamentals') && taskType.includes('fundamentals')) ||
        (sourceIdLower.includes('finviz_screener') && taskType.includes('finviz_screener'))
      );
    });
  };

  // Get cron expression info for a data source
  const getCronInfo = (sourceId: string) => {
    const relatedTasks = getRelatedTasks(sourceId);
    const enabledTasks = relatedTasks.filter(task => task.enabled);
    
    if (enabledTasks.length === 0) {
      return null;
    }
    
    // Get the first enabled task's cron expression
    const task = enabledTasks[0];
    const cronExpression = task.cron_expression;
    
    if (!cronExpression) {
      return null;
    }
    
    // Validate cron expression
    const validation = schedulerAPI.validateCronExpression(cronExpression);
    
    // Get human-readable description
    const description = schedulerAPI.describeCronExpression(cronExpression);
    
    return {
      expression: cronExpression,
      description: description,
      timezone: task.timezone,
      isValid: validation.valid,
      validationMessage: validation.message,
      params: task.params
    };
  };



  // Load theme data
  const loadThemes = async () => {
    try {
      const response = await newsAPI.getThemes();
      setThemes(response.data || []);
    } catch (error) {
      console.error('Failed to get themes:', error);
      message.error('Failed to get themes');
    }
  };

  // Load scheduled tasks
  const loadScheduledTasks = async () => {
    try {
      setLoading(true);
      const response = await schedulerAPI.getTasks();
      if (response.success && response.data) {
        setScheduledTasks(response.data);
      } else {
        message.error(response.message);
      }
    } catch (error) {
      console.error('Failed to load scheduled tasks:', error);
      message.error('Failed to load scheduled tasks');
    } finally {
      setLoading(false);
    }
  };

  // Toggle data source enabled status - by enabling/disabling corresponding scheduled tasks
  const toggleDataSource = async (sourceId: string, enabled: boolean) => {
    try {
      setLoading(true);
      
      // EODHD Fundamentals does not support status toggle, only manual trigger
      if (sourceId === 'eodhd_fundamentals') {
        message.info('EODHD Fundamentals only supports manual trigger updates, cannot be controlled by switch');
        return;
      }
      
      // Find corresponding scheduled tasks based on data source ID
      const relatedTasks = scheduledTasks.filter(task => {
        const taskType = task.type.toLowerCase();
        const sourceIdLower = sourceId.toLowerCase();
        
        // Mapping relationship: data source ID -> task type
        return (
          (sourceIdLower === 'eodhd_news' && taskType === 'eodhd_news') ||
          (sourceIdLower === 'watchlist_news' && taskType === 'watchlist_news') ||
          (sourceIdLower === 'eodhd_stock_prices' && taskType === 'eodhd_stock_prices') ||
          (sourceIdLower.includes('events') && taskType.includes('events')) ||
          (sourceIdLower.includes('macro') && (taskType.includes('macro') || taskType.includes('indicators'))) ||
          (sourceIdLower.includes('exchange') && !sourceIdLower.includes('fundamentals') && taskType.includes('exchange')) ||
          (sourceIdLower.includes('fundamentals') && taskType.includes('fundamentals')) ||
          (sourceIdLower.includes('finviz_screener') && taskType.includes('finviz_screener'))
        );
      });
      
      if (relatedTasks.length === 0) {
        message.warning(`No scheduled tasks found for ${sourceId}, please configure data fetch strategy first`);
        return;
      }
      
      // Batch enable/disable related tasks
      let successCount = 0;
      let errorCount = 0;
      
      for (const task of relatedTasks) {
        try {
          const result = enabled 
            ? await schedulerAPI.enableTask(task.id)
            : await schedulerAPI.disableTask(task.id);
          
          if (result.success) {
            successCount++;
          } else {
            errorCount++;
            console.error(`Task ${task.id} operation failed:`, result.message);
          }
        } catch (error) {
          errorCount++;
          console.error(`Task ${task.id} operation error:`, error);
        }
      }
      
      if (successCount > 0) {
        // Update frontend status
        setDataSources(prev => prev.map(source => 
          source.id === sourceId ? { ...source, enabled } : source
        ));
        
        // Reload task list to get latest status
        await loadScheduledTasks();
        
        const message_text = `${enabled ? 'Enabled' : 'Disabled'} successfully: ${successCount} tasks`;
        message.success(errorCount > 0 ? `${message_text}, ${errorCount} tasks failed` : message_text);
      } else {
        message.error(`Operation failed: all ${relatedTasks.length} tasks cannot be ${enabled ? 'enabled' : 'disabled'}`);
      }
      
    } catch (error) {
      console.error('Failed to toggle data source status:', error);
      message.error('Operation failed');
    } finally {
      setLoading(false);
    }
  };

  // Execute quick action
  const executeQuickAction = async (action: QuickAction, dataSource: DataSource, customParams?: any) => {
    try {
      setLoading(true);
      
      // Update data source status to running
      setDataSources(prev => prev.map(source => 
        source.id === dataSource.id ? { ...source, status: 'running' } : source
      ));

      const params = { ...action.params, ...customParams };
      let response;

      response = await api.post(action.endpoint, params);

      // Update data source status
      setDataSources(prev => prev.map(source => 
        source.id === dataSource.id ? { 
          ...source, 
          status: 'success',
          lastRun: dayjs().format('YYYY-MM-DD HH:mm:ss'),
          lastSuccess: dayjs().format('YYYY-MM-DD HH:mm:ss')
        } : source
      ));

      message.success(response.message || 'Operation successful');
      
    } catch (error: any) {
      console.error('Quick action error:', error);
      

      
      // Update data source status to error
      setDataSources(prev => prev.map(source => 
        source.id === dataSource.id ? { 
          ...source, 
          status: 'error',
          lastRun: dayjs().format('YYYY-MM-DD HH:mm:ss')
        } : source
      ));

      const errorMessage = error.response?.data?.detail || (error instanceof Error ? error.message : 'Operation failed');
      message.error(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  // Show quick action dialog
  const showQuickActionModal = (action: QuickAction, dataSource: DataSource) => {
    // If it's Cron scheduling configuration or fundamentals data update, decide which config component to use based on data source type
    if ((action.id.includes('configure') && action.id.includes('schedule')) || action.id === 'update_fundamentals_data') {
      // For EODHD related data sources, use dedicated configuration modal
      if (dataSource.id === 'eodhd_news' || dataSource.id === 'watchlist_news' || dataSource.id === 'eodhd_events' || dataSource.id === 'eodhd_macro' || dataSource.id === 'eodhd_fundamentals' || dataSource.id === 'finviz_screener' || dataSource.id === 'eodhd_stock_prices') {
        setSelectedDataSource(dataSource);
        setDataSourceConfigVisible(true);
      } else {
        // Other data sources do not support configuration yet
        message.info('Data source configuration feature coming soon');
      }
      return;
    }

    // If it's a simple operation, execute directly
    if (!action.params || Object.keys(action.params).length === 0) {
      executeQuickAction(action, dataSource);
      return;
    }

    // Show parameter configuration dialog
    setQuickActionModal({
      visible: true,
      action,
      dataSource
    });
    
    // If it's news fetching, set default values and load themes
    if (action.id === 'fetch_latest_news') {
      form.setFieldsValue({
        ...action.params,
        dateRange: [dayjs().subtract(7, 'day'), dayjs()]
      });
      loadThemes();
    } else {
      form.setFieldsValue(action.params || {});
    }
  };

  // Confirm execute quick action
  const confirmQuickAction = async () => {
    try {
      const values = await form.validateFields();
      await executeQuickAction(quickActionModal.action!, quickActionModal.dataSource!, values);
      setQuickActionModal({ visible: false, action: null, dataSource: null });
      form.resetFields();
    } catch (error) {
      console.error('Form validation error:', error);
    }
  };

  // Theme management functions
  const createTheme = async (values: any) => {
    try {
      const response = await newsAPI.createTheme(values);
      if (response.success) {
        message.success('Theme created successfully');
        loadThemes();
        setThemeModalVisible(false);
        themeForm.resetFields();
      } else {
        message.error(response.message || 'Creation failed');
      }
    } catch (error) {
      console.error('Failed to create theme:', error);
      message.error('Failed to create theme');
    }
  };

  const updateTheme = async (themeId: number, values: any) => {
    try {
      const response = await newsAPI.updateTheme(themeId, values);
      if (response.success) {
        message.success('Theme updated successfully');
        loadThemes();
        setThemeModalVisible(false);
        setSelectedTheme(null);
        themeForm.resetFields();
      } else {
        message.error(response.message || 'Update failed');
      }
    } catch (error) {
      console.error('Failed to update theme:', error);
      message.error('Failed to update theme');
    }
  };

  const deleteTheme = async (themeId: number) => {
    try {
      const response = await newsAPI.deleteTheme(themeId);
      if (response.success) {
        message.success('Theme deleted successfully');
        loadThemes();
      } else {
        message.error(response.message || 'Deletion failed');
      }
    } catch (error) {
      console.error('Failed to delete theme:', error);
      message.error('Failed to delete theme');
    }
  };

  const initializeDefaultThemes = async () => {
    try {
      const response = await newsAPI.initializeDefaultThemes();
      if (response.success) {
        message.success(`Initialization successful, created ${response.data.created_count} default themes`);
        loadThemes();
      } else {
        message.error(response.message || 'Initialization failed');
      }
    } catch (error) {
      console.error('Failed to initialize themes:', error);
      message.error('Failed to initialize themes');
    }
  };

  // Scheduled task management functions






  const handleDataSourceConfigSuccess = (taskId: string) => {
    setDataSourceConfigVisible(false);
    setSelectedDataSource(null);
    loadScheduledTasks(); // Reload task list
  };





  // Database table viewing functions moved to DatabaseTableViewer component

  // Refresh all data source status
  const refreshAllSources = async () => {
    setLoading(true);
    try {
      // Load scheduled task list
      await loadScheduledTasks();
      message.success('Status refreshed');
    } catch (error) {
      message.error('Refresh failed');
    } finally {
      setLoading(false);
    }
  };

  // Statistics data
  const enabledSources = dataSources.filter(s => s.enabled).length;
  const runningSources = dataSources.filter(s => s.status === 'running').length;
  const enabledTasks = scheduledTasks.filter(t => t.enabled).length;


  return (
    <div style={{ padding: '24px' }}>
      {/* Page title */}
      <div style={{ marginBottom: '24px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <Title level={2} style={{ margin: 0 }}>
            <DatabaseOutlined style={{ marginRight: '8px' }} />
            Data Source Management
          </Title>
          <Text type="secondary" style={{ margin: '8px 0 0 0', display: 'block' }}>
            Simplified data fetching scheduler - Manage EODHD, Alpha Vantage, Finviz and other data sources
          </Text>
        </div>
        <Space>
          <Button 
            icon={<ReloadOutlined />} 
            onClick={refreshAllSources}
            loading={loading}
          >
            Refresh Status
          </Button>
        </Space>
      </div>

      {/* Main content area - using Tabs layout */}
      <Card>
        <Tabs 
          activeKey={activeTab}
          onChange={handleTabChange}
          size="large"
          items={[
            {
              key: 'scheduler_config',
              label: (
                <Space>
                  <SettingOutlined />
                  Scheduler Configuration
                </Space>
              ),
              children: (
                <div>
                  {/* Statistics cards */}
                  <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
                    <Col xs={24} sm={8} md={6}>
                      <Card>
                        <Statistic
                          title="Total Data Sources"
                          value={dataSources.length}
                          prefix={<DatabaseOutlined />}
                          valueStyle={{ color: '#1890ff' }}
                        />
                      </Card>
                    </Col>
                    <Col xs={24} sm={8} md={6}>
                      <Card>
                        <Statistic
                          title="Enabled"
                          value={enabledSources}
                          prefix={<CheckCircleOutlined />}
                          valueStyle={{ color: '#52c41a' }}
                          suffix={`/ ${dataSources.length}`}
                        />
                      </Card>
                    </Col>
                    <Col xs={24} sm={8} md={6}>
                      <Card>
                        <Statistic
                          title="Running"
                          value={runningSources}
                          prefix={<SyncOutlined spin={runningSources > 0} />}
                          valueStyle={{ color: '#faad14' }}
                        />
                      </Card>
                    </Col>
                    <Col xs={24} sm={8} md={6}>
                      <Card>
                        <Statistic
                          title="Scheduled Tasks"
                          value={scheduledTasks.length}
                          prefix={<ScheduleOutlined />}
                          valueStyle={{ color: '#722ed1' }}
                          suffix={`(${enabledTasks} enabled)`}
                        />
                      </Card>
                    </Col>
                  </Row>

                  {/* Data source cards */}
                  <Row gutter={[16, 16]}>
                    {dataSources.map(source => (
                      <Col xs={24} lg={12} xl={8} key={source.id}>
                        <Card
                          title={
                            <Space>
                              <span>{source.name}</span>
                              {getStatusTag(source.status)}
                            </Space>
                          }
                          extra={
                            source.id === 'eodhd_fundamentals' ? null : (
                              <Tooltip title={
                                source.description.includes('Coming soon') 
                                  ? 'This data source is coming soon' 
                                  : !source.enabled && scheduledTasks.filter(task => {
                                      const taskType = task.type.toLowerCase();
                                      const sourceIdLower = source.id.toLowerCase();
                                      return (
                                        (sourceIdLower === 'eodhd_news' && taskType === 'eodhd_news') ||
                                        (sourceIdLower === 'watchlist_news' && taskType === 'watchlist_news') ||
                                        (sourceIdLower.includes('events') && taskType.includes('events')) ||
                                        (sourceIdLower.includes('macro') && (taskType.includes('macro') || taskType.includes('indicators')))
                                      );
                                    }).length === 0
                                  ? 'Please configure data fetch strategy first'
                                  : undefined
                              }>
                                <Switch
                                  checked={source.enabled}
                                  onChange={(checked) => toggleDataSource(source.id, checked)}
                                  disabled={source.description.includes('Coming soon') || loading}
                                  loading={loading && source.status === 'running'}
                                />
                              </Tooltip>
                            )
                          }
                          style={{ 
                            height: '100%',
                            opacity: source.enabled ? 1 : 0.6
                          }}
                        >
                          <div style={{ marginBottom: '16px' }}>
                            <Text type="secondary">{source.description}</Text>
                          </div>

                          {source.lastRun && (
                            <div style={{ marginBottom: '16px' }}>
                              <Text type="secondary" style={{ fontSize: '12px' }}>
                                Last Run: {source.lastRun}
                              </Text>
                            </div>
                          )}

                          <div style={{ marginBottom: '16px' }}>
                            <Text strong>Configuration:</Text>
                          </div>

                          {/* Cron Expression Display */}
                          {(() => {
                            const cronInfo = getCronInfo(source.id);
                            if (cronInfo) {
                              return (
                                <div style={{ 
                                  marginBottom: '16px', 
                                  padding: '12px', 
                                  backgroundColor: cronInfo.isValid ? '#f6f8fa' : '#fff2f0', 
                                  borderRadius: '6px', 
                                  border: `1px solid ${cronInfo.isValid ? '#e1e4e8' : '#ffccc7'}` 
                                }}>
                                  <div style={{ marginBottom: '8px' }}>
                                    <Space>
                                      <ScheduleOutlined style={{ color: cronInfo.isValid ? '#1890ff' : '#ff4d4f' }} />
                                      <Text strong style={{ fontSize: '13px' }}>Current Schedule:</Text>
                                      {cronInfo.isValid ? (
                                        <CheckCircleOutlined style={{ color: '#52c41a', fontSize: '12px' }} />
                                      ) : (
                                        <ExclamationCircleOutlined style={{ color: '#ff4d4f', fontSize: '12px' }} />
                                      )}
                                    </Space>
                                  </div>
                                  <div style={{ marginBottom: '4px' }}>
                                    <Space>
                                      <Text code style={{ 
                                        fontSize: '12px', 
                                        backgroundColor: '#fff', 
                                        padding: '2px 6px', 
                                        borderRadius: '3px',
                                        color: cronInfo.isValid ? '#000' : '#ff4d4f'
                                      }}>
                                        {cronInfo.expression}
                                      </Text>
                                      {cronInfo.params && Object.keys(cronInfo.params).length > 0 && (
                                        <Tooltip 
                                          title={
                                            <pre style={{ 
                                              margin: 0, 
                                              fontSize: '12px', 
                                              fontFamily: 'monospace',
                                              whiteSpace: 'pre-wrap',
                                              maxWidth: '300px'
                                            }}>
                                              {JSON.stringify(cronInfo.params, null, 2)}
                                            </pre>
                                          }
                                          placement="top"
                                        >
                                          <SettingOutlined 
                                            style={{ 
                                              color: '#1890ff', 
                                              fontSize: '12px',
                                              cursor: 'pointer'
                                            }} 
                                          />
                                        </Tooltip>
                                      )}
                                    </Space>
                                  </div>
                                  {!cronInfo.isValid && cronInfo.validationMessage && (
                                    <div style={{ marginBottom: '4px' }}>
                                      <Text type="danger" style={{ fontSize: '11px' }}>
                                        {cronInfo.validationMessage}
                                      </Text>
                                    </div>
                                  )}
                                  <div>
                                    <Text type="secondary" style={{ fontSize: '11px' }}>
                                      Timezone: {cronInfo.timezone}
                                    </Text>
                                  </div>
                                </div>
                              );
                            }
                            return null;
                          })()}

                          <Space direction="vertical" style={{ width: '100%' }}>
                            {source.quickActions.map(action => (
                              <Tooltip key={action.id} title={action.description}>
                                <Button
                                  block
                                  icon={action.icon}
                                  onClick={() => showQuickActionModal(action, source)}
                                  disabled={
                                    // Configuration buttons should always be clickable
                                    action.id.includes('configure') 
                                      ? false 
                                      : !source.enabled
                                  }
                                  loading={source.status === 'running'}
                                >
                                  {action.name}
                                </Button>
                              </Tooltip>
                            ))}
                          </Space>
                        </Card>
                      </Col>
                    ))}
                  </Row>
                </div>
              )
            },
            {
              key: 'task_management',
              label: (
                <Space>
                  <ScheduleOutlined />
                  Task Management
                </Space>
              ),
              children: (
                <DatabaseTableViewer refreshTrigger={taskManagementRefreshTrigger} />
              )
            },
            {
              key: 'execution_history',
              label: (
                <Space>
                  <BarChartOutlined />
                  Execution History
                </Space>
              ),
              children: (
                <TaskExecutionHistory 
                  showTaskFilter={true} 
                  height={600} 
                  refreshTrigger={executionHistoryRefreshTrigger} 
                />
              )
            }
          ]} 
        />
      </Card>





      {/* Data source specific configuration modal */}
      {selectedDataSource && (
        <DataSourceConfigModal
          open={dataSourceConfigVisible}
          onCancel={() => {
            setDataSourceConfigVisible(false);
            setSelectedDataSource(null);
          }}
          onSuccess={handleDataSourceConfigSuccess}
          dataSourceType={selectedDataSource.id as 'eodhd_news' | 'eodhd_events' | 'eodhd_macro' | 'eodhd_fundamentals'}
          dataSourceName={selectedDataSource.name}
        />
      )}

      {/* Quick action parameter configuration dialog */}
      <Modal
        title={
          <Space>
            <CloudDownloadOutlined />
            {quickActionModal.action?.name}
          </Space>
        }
        open={quickActionModal.visible}
        onOk={confirmQuickAction}
        onCancel={() => {
          setQuickActionModal({ visible: false, action: null, dataSource: null });
          form.resetFields();
        }}
        okText="Execute"
        cancelText="Cancel"
        width={800}
        destroyOnClose
      >
        {/* Progress bar */}


        {quickActionModal.action && (
          <div>
            <div style={{ marginBottom: '16px' }}>
              <Text type="secondary">{quickActionModal.action.description}</Text>
            </div>
            
            {quickActionModal.action.id === 'fetch_latest_news' ? (
              <Form form={form} layout="vertical">
                <Tabs defaultActiveKey="basic">
                  <Tabs.TabPane
                    tab={<span><SettingOutlined />Basic Settings</span>}
                    key="basic"
                  >
                    <Row gutter={16}>
                      <Col span={12}>
                        <Form.Item
                          name="symbols"
                          label="Stock Symbols"
                          tooltip="Enter stock symbols, multiple separated by commas, e.g.: AAPL.US,TSLA.US"
                        >
                          <Input placeholder="e.g.: AAPL.US,TSLA.US" />
                        </Form.Item>
                      </Col>
                      <Col span={12}>
                        <Form.Item
                          name="topic"
                          label="Topic Tags"
                          tooltip="EODHD API topic parameters, e.g.: technology, politics"
                        >
                          <Input placeholder="e.g.: technology, politics" />
                        </Form.Item>
                      </Col>
                      <Col span={24}>
                        <Form.Item
                          name="dateRange"
                          label="Date Range"
                          rules={[{ required: true, message: 'Please select date range' }]}
                        >
                          <RangePicker style={{ width: '100%' }} />
                        </Form.Item>
                      </Col>
                      <Col span={12}>
                        <Form.Item
                          name="max_results"
                          label="Max Results"
                          rules={[{ required: true, message: 'Please enter max results' }]}
                          extra="Maximum number of news articles to fetch, up to 1000"
                        >
                          <InputNumber min={1} max={1000} placeholder="100" style={{ width: '100%' }} />
                        </Form.Item>
                      </Col>
                    </Row>
                  </Tabs.TabPane>
                  
                  <Tabs.TabPane
                    tab={<span><InfoCircleOutlined />Advanced Settings</span>}
                    key="advanced"
                  >
                    <Row gutter={16}>
                      <Col span={12}>
                        <Form.Item
                          name="topic_filter"
                          label="Topic Filter"
                          tooltip="Filter using themes from database"
                        >
                          <Select placeholder="Select theme" allowClear>
                            {themes.filter(theme => theme.is_active).map(theme => (
                              <Option key={theme.id} value={theme.topic_name}>
                                <span style={{ color: theme.color || '#1890ff' }}>●</span> {theme.topic_name}
                              </Option>
                            ))}
                          </Select>
                        </Form.Item>
                      </Col>
                      <Col span={12}>
                        <Form.Item
                          name="keywords"
                          label="Custom Keywords"
                          tooltip="Custom keywords, multiple separated by commas"
                        >
                          <Input placeholder="e.g.: inflation,interest rate" />
                        </Form.Item>
                      </Col>
                      <Col span={24}>
                        <Alert
                          message="Advanced Filtering Instructions"
                          description={
                            <div>
                              <p>• <strong>Topic Filter</strong>: Smart filtering based on theme keywords in database</p>
                              <p>• <strong>Custom Keywords</strong>: Content matching using custom keywords</p>
                              <p>• <strong>Combined Usage</strong>: Can use multiple filter conditions simultaneously</p>
                            </div>
                          }
                          type="info"
                          showIcon
                          style={{ marginBottom: 16 }}
                        />
                      </Col>
                    </Row>
                  </Tabs.TabPane>
                  
                  <Tabs.TabPane
                    tab={<span><TagOutlined />Theme Management</span>}
                    key="themes"
                  >
                    <div style={{ marginBottom: 16 }}>
                      <Space wrap>
                        <Button
                          type="primary"
                          icon={<PlusOutlined />}
                          onClick={() => {
                            setSelectedTheme(null);
                            setThemeModalVisible(true);
                            themeForm.resetFields();
                          }}
                          size="small"
                        >
                          New Theme
                        </Button>
                        <Button
                          icon={<BulbOutlined />}
                          onClick={initializeDefaultThemes}
                          size="small"
                        >
                          Initialize Default Themes
                        </Button>
                        <Button
                          icon={<ReloadOutlined />}
                          onClick={loadThemes}
                          size="small"
                        >
                          Refresh
                        </Button>
                      </Space>
                    </div>

                    <Alert
                      message="Theme Management Instructions"
                      description="Themes are used for intelligent news classification and filtering. Each theme contains multiple keywords, and the system automatically adds corresponding tags to news based on these keywords."
                      type="info"
                      showIcon
                      style={{ marginBottom: 16 }}
                    />

                    <Table
                      size="small"
                      pagination={false}
                      dataSource={themes}
                      rowKey="id"
                      scroll={{ y: 300 }}
                      columns={[
                        {
                          title: 'Theme Name',
                          dataIndex: 'topic_name',
                          key: 'topic_name',
                          render: (text: string, record: NewsTheme) => (
                            <Space>
                              <Badge color={record.color || '#1890ff'} />
                              <Text strong style={{ fontSize: '12px' }}>{text}</Text>
                            </Space>
                          )
                        },
                        {
                          title: 'Keywords',
                          dataIndex: 'keywords',
                          key: 'keywords',
                          render: (keywords: string[]) => (
                            <div>
                              {keywords.slice(0, 3).map((keyword, index) => (
                                <Tag key={index} style={{ fontSize: '10px' }}>
                                  {keyword}
                                </Tag>
                              ))}
                              {keywords.length > 3 && (
                                <Text type="secondary" style={{ fontSize: '10px' }}>
                                  +{keywords.length - 3}
                                </Text>
                              )}
                            </div>
                          )
                        },
                        {
                          title: 'Status',
                          dataIndex: 'is_active',
                          key: 'is_active',
                          render: (active: boolean) => (
                            <Tag color={active ? 'green' : 'default'} style={{ fontSize: '10px' }}>
                              {active ? 'Active' : 'Disabled'}
                            </Tag>
                          )
                        },
                        {
                          title: 'Actions',
                          key: 'actions',
                          render: (record: NewsTheme) => (
                            <Space size="small">
                              <Button
                                type="link"
                                size="small"
                                icon={<EditOutlined />}
                                onClick={() => {
                                  setSelectedTheme(record);
                                  setThemeModalVisible(true);
                                  themeForm.setFieldsValue({
                                    topic_name: record.topic_name,
                                    description: record.description,
                                    color: record.color,
                                    is_active: record.is_active,
                                    keywords: record.keywords.join(', ')
                                  });
                                }}
                                style={{ fontSize: '10px', padding: '2px 4px' }}
                              />
                                                              <Popconfirm
                                  title="Are you sure you want to delete this theme?"
                                  onConfirm={() => deleteTheme(record.id)}
                                  okText="Confirm"
                                  cancelText="Cancel"
                                >
                                <Button
                                  type="link"
                                  size="small"
                                  icon={<DeleteOutlined />}
                                  danger
                                  style={{ fontSize: '10px', padding: '2px 4px' }}
                                />
                              </Popconfirm>
                            </Space>
                          )
                        }
                      ]}
                    />
                  </Tabs.TabPane>
                </Tabs>
              </Form>
            ) : (
              <Form form={form} layout="vertical">
                {quickActionModal.action.id === 'fetch_economic_events' && (
                  <>
                    <Form.Item
                      label="Result Limit"
                      name="limit"
                      rules={[{ required: true, type: 'number', min: 1, max: 1000 }]}
                    >
                      <Input type="number" placeholder="Maximum number of economic events to fetch" />
                    </Form.Item>
                    <Form.Item label="Start Date" name="from_date">
                      <Input type="date" placeholder="Optional, format: YYYY-MM-DD" />
                    </Form.Item>
                    <Form.Item label="End Date" name="to_date">
                      <Input type="date" placeholder="Optional, format: YYYY-MM-DD" />
                    </Form.Item>
                  </>
                )}
                
                {(quickActionModal.action.id === 'fetch_macro_indicators_usa' || 
                  quickActionModal.action.id === 'fetch_macro_indicators_chn') && (
                  <Form.Item label="Indicator Code" name="indicator">
                    <Select placeholder="Optional, select indicator (leave empty to get all indicators)">
                      <Select.Option value="gdp_current_usd">GDP (Current USD)</Select.Option>
                      <Select.Option value="gdp_per_capita_usd">GDP per Capita</Select.Option>
                      <Select.Option value="gdp_growth_annual">GDP Annual Growth Rate</Select.Option>
                      <Select.Option value="inflation_consumer_prices_annual">Consumer Price Inflation Rate</Select.Option>
                      <Select.Option value="unemployment_total">Unemployment Rate</Select.Option>
                      <Select.Option value="debt_percent_gdp">Debt as % of GDP</Select.Option>
                    </Select>
                  </Form.Item>
                )}
              </Form>
            )}
          </div>
        )}
      </Modal>

      {/* Theme editing modal */}
      <Modal
        title={selectedTheme ? "Edit Theme" : "New Theme"}
        open={themeModalVisible}
        onOk={() => themeForm.submit()}
        onCancel={() => {
          setThemeModalVisible(false);
          setSelectedTheme(null);
          themeForm.resetFields();
        }}
        okText="Save"
        cancelText="Cancel"
      >
        <Form
          form={themeForm}
          layout="vertical"
          onFinish={(values) => {
            const formattedValues = {
              ...values,
              keywords: values.keywords.split(',').map((k: string) => k.trim()).filter((k: string) => k)
            };
            
            if (selectedTheme) {
              updateTheme(selectedTheme.id, formattedValues);
            } else {
              createTheme(formattedValues);
            }
          }}
        >
          <Form.Item
            label="Theme Name"
            name="topic_name"
            rules={[{ required: true, message: 'Please enter theme name' }]}
          >
            <Input placeholder="Enter theme name" />
          </Form.Item>
          
          <Form.Item
            label="Description"
            name="description"
          >
            <Input.TextArea placeholder="Enter theme description" rows={3} />
          </Form.Item>
          
          <Form.Item
            label="Color"
            name="color"
          >
            <Input placeholder="#1890ff" />
          </Form.Item>
          
          <Form.Item
            label="Keywords"
            name="keywords"
            rules={[{ required: true, message: 'Please enter keywords' }]}
            tooltip="Multiple keywords separated by commas"
          >
            <Input.TextArea 
              placeholder="Enter keywords separated by commas, e.g.: technology,AI,machine learning" 
              rows={3}
            />
          </Form.Item>
          
          <Form.Item
            label="Status"
            name="is_active"
            valuePropName="checked"
            initialValue={true}
          >
            <Switch checkedChildren="Enabled" unCheckedChildren="Disabled" />
          </Form.Item>
        </Form>
      </Modal>

      {/* Task status monitoring modal */}
      <TaskStatusModal
        open={taskStatusVisible}
        onCancel={() => {
          setTaskStatusVisible(false);
          setSelectedTask(null);
        }}
        taskData={selectedTask ? {
          id: selectedTask.id,
          name: selectedTask.name,
          type: selectedTask.type,
          status: selectedTask.status === 'running' ? 'running' : 
                  selectedTask.status === 'paused' ? 'scheduled' :
                  selectedTask.status === 'disabled' ? 'scheduled' : 'scheduled',
          cron_expression: selectedTask.cron_expression,
          next_execution: selectedTask.next_run,
          statistics: undefined // ScheduledTask interface does not have last_execution property
        } : undefined}
      />
    </div>
  );
};

export default SchedulerPage;
