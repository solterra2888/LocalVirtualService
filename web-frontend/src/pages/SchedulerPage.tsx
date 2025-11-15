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
  Result,
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
  BarChartOutlined,
  LineChartOutlined,
  CalendarOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { api } from '../services/api';
import { newsAPI } from '../services/news';
import { schedulerAPI, ScheduledTask } from '../services/scheduler';
import { useAuthStore } from '../store/authStore';
import { useNavigate } from 'react-router-dom';

import DataSourceConfigModal from '../components/DataSourceConfigModal';
import TaskStatusModal from '../components/TaskStatusModal';

const { Title, Text } = Typography;
const { RangePicker } = DatePicker;
const { Option } = Select;

// Data source configuration
interface DataSource {
  id: string;
  name: string;
  type: 'eodhd' | 'watchlist' | 'alpha_vantage' | 'finviz' | 'finviz_screener' | 'eodhd_events' | 'eodhd_macro' | 'eodhd_fundamentals' | 'eodhd_stock_prices' | 'eodhd_eod_stock_prices' | 'market_data' | 'earnings_calendar' | 'ipo_calendar' | 'watchlist_fundamentals' | 'other';
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
  const { user } = useAuthStore();
  const navigate = useNavigate();
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
      id: 'eodhd_eod_stock_prices',
      name: 'EODHD EOD Stock Prices',
      type: 'eodhd_eod_stock_prices',
      description: 'Get historical EOD (End of Day) stock prices for watchlist and theme associated stocks from EODHD API',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_eod_stock_prices_schedule',
          name: 'Configure EOD Stock Prices Fetch',
          description: 'Set up periodic historical stock price fetching for watchlist stocks (daily at 9 AM)',
          endpoint: '/scheduler/tasks/eodhd-eod-stock-prices',
          params: {},
          icon: <BarChartOutlined />
        }
      ]
    },
    // futu_themes 数据源已删除 - 改为纯手动主题管理
    {
      id: 'market_data',
      name: 'Market Data (Indices, Forex, Commodities, Bonds, Sectors)',
      type: 'market_data',
      description: 'Get real-time and historical market data from EODHD API (32 assets: indices, forex, commodities, bond yields, sectors)',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_market_data_schedule',
          name: 'Configure Market Data Fetch',
          description: 'Set up periodic market data fetching (every 6 hours, realtime + EOD data)',
          endpoint: '/scheduler/tasks/market-data',
          params: {},
          icon: <LineChartOutlined />
        }
      ]
    },
    {
      id: 'earnings_calendar',
      name: 'Earnings Calendar',
      type: 'earnings_calendar',
      description: 'Get US earnings calendar from EODHD API (no S&P 500 filtering, all US stocks)',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_earnings_calendar_schedule',
          name: 'Configure Earnings Calendar Fetch',
          description: 'Set up periodic earnings calendar fetching (weekly on Sunday at 6 AM ET)',
          endpoint: '/scheduler/tasks/earnings-calendar',
          params: {},
          icon: <CalendarOutlined />
        }
      ]
    },
    {
      id: 'ipo_calendar',
      name: 'IPO Calendar',
      type: 'ipo_calendar',
      description: 'Get IPO calendar data from EODHD API for US and HK exchanges',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_ipo_calendar_schedule',
          name: 'Configure IPO Calendar Fetch',
          description: 'Set up periodic IPO calendar fetching (weekly on Sunday at 6 AM ET)',
          endpoint: '/scheduler/tasks/ipo-calendar',
          params: {},
          icon: <CalendarOutlined />
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
    },
    {
      id: 'watchlist_fundamentals',
      name: 'Watchlist Fundamentals',
      type: 'watchlist_fundamentals',
      description: 'Automatically update fundamentals data for all watchlist stocks (weekly)',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_watchlist_fundamentals_schedule',
          name: 'Configure Watchlist Fundamentals Update',
          description: 'Set up periodic fundamentals update for your watchlist stocks (weekly on Sunday 2 AM)',
          endpoint: '/scheduler/tasks/watchlist-fundamentals',
          params: {},
          icon: <DatabaseOutlined />
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
  
  // Load scheduled task data when page loads
  useEffect(() => {
    loadScheduledTasks();
  }, []);

  // Handle tab change and refresh data
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
          (sourceIdLower === 'eodhd_eod_stock_prices' && taskType === 'eodhd_eod_stock_prices') ||
          (sourceIdLower === 'market_data' && taskType === 'market_data') ||
          (sourceIdLower === 'earnings_calendar' && taskType === 'earnings_calendar') ||
          (sourceIdLower === 'ipo_calendar' && taskType === 'ipo_calendar') ||
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
        (sourceIdLower === 'eodhd_eod_stock_prices' && taskType === 'eodhd_eod_stock_prices') ||
        (sourceIdLower === 'futu_themes' && taskType === 'futu_themes') ||
        (sourceIdLower === 'market_data' && taskType === 'market_data') ||
        (sourceIdLower === 'earnings_calendar' && taskType === 'earnings_calendar') ||
        (sourceIdLower === 'ipo_calendar' && taskType === 'ipo_calendar') ||
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

  // Format task parameters for display
  const formatTaskParams = (sourceId: string, params: Record<string, any>) => {
    if (!params || Object.keys(params).length === 0) {
      return null;
    }

    const sourceIdLower = sourceId.toLowerCase();

    // Format earnings_calendar parameters
    if (sourceIdLower === 'earnings_calendar') {
      const parts: string[] = [];
      
      if (params.fetch_next_week) {
        parts.push('📅 Next Week');
      } else if (params.start_date && params.end_date) {
        parts.push(`📅 ${params.start_date} to ${params.end_date}`);
      } else if (params.days) {
        parts.push(`📅 Next ${params.days} days`);
      }
      
      return parts.length > 0 ? parts.join(', ') : null;
    }

    // Format eodhd_events (economic_events) parameters - 使用与 earnings_calendar 相同的格式
    if (sourceIdLower === 'eodhd_events') {
      const parts: string[] = [];
      
      if (params.fetch_next_week) {
        parts.push('📅 Next Week');
      } else if (params.start_date && params.end_date) {
        parts.push(`📅 ${params.start_date} to ${params.end_date}`);
      } else if (params.days) {
        parts.push(`📅 Next ${params.days} days`);
      }
      
      if (params.country) {
        // 支持数组或字符串
        const countries = Array.isArray(params.country) 
          ? params.country 
          : (typeof params.country === 'string' ? params.country.split(',').map(c => c.trim()) : [params.country]);
        
        if (countries.length > 0) {
          parts.push(`🌍 ${countries.join(', ')}`);
        }
      }
      
      return parts.length > 0 ? parts.join(' | ') : null;
    }

    // Format ipo_calendar parameters
    if (sourceIdLower === 'ipo_calendar') {
      const parts: string[] = [];
      
      if (params.fetch_next_week) {
        parts.push('📅 Next Week');
      } else if (params.start_date && params.end_date) {
        parts.push(`📅 ${params.start_date} to ${params.end_date}`);
      } else if (params.days) {
        parts.push(`📅 Next ${params.days} days`);
      }
      
      return parts.length > 0 ? parts.join(', ') : null;
    }

    // Format market_data parameters
    if (sourceIdLower === 'market_data') {
      const parts: string[] = [];
      
      if (params.asset_types && Array.isArray(params.asset_types) && params.asset_types.length > 0) {
        parts.push(`Types: ${params.asset_types.join(', ')}`);
      } else {
        parts.push('Types: All');
      }
      
      if (params.fetch_realtime) {
        parts.push('Realtime: ✅');
      }
      if (params.fetch_eod) {
        parts.push(`EOD: ${params.eod_days || 5} days`);
      }
      
      return parts.join(' | ');
    }

    // Default: return formatted JSON
    return JSON.stringify(params, null, 2);
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
        const filteredTasks = response.data.filter(task => task.type !== 'futu_themes');
        setScheduledTasks(filteredTasks);
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
          (sourceIdLower === 'eodhd_eod_stock_prices' && taskType === 'eodhd_eod_stock_prices') ||
          (sourceIdLower === 'market_data' && taskType === 'market_data') ||
          (sourceIdLower === 'earnings_calendar' && taskType === 'earnings_calendar') ||
          (sourceIdLower === 'ipo_calendar' && taskType === 'ipo_calendar') ||
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
      if (dataSource.id === 'eodhd_news' || dataSource.id === 'watchlist_news' || dataSource.id === 'eodhd_events' || dataSource.id === 'eodhd_macro' || dataSource.id === 'eodhd_fundamentals' || dataSource.id === 'finviz_screener' || dataSource.id === 'eodhd_stock_prices' || dataSource.id === 'eodhd_eod_stock_prices' || dataSource.id === 'market_data' || dataSource.id === 'earnings_calendar' || dataSource.id === 'ipo_calendar' || dataSource.id === 'watchlist_fundamentals') {
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

  if (!user?.is_admin) {
    return (
      <div style={{ padding: '24px' }}>
        <Card>
          <Result
            status="info"
            title="Scheduler configuration requires admin access"
            subTitle="Task Management and Execution History have moved to Scheduler Logs."
            extra={
              <Button type="primary" onClick={() => navigate('/scheduler/logs')}>
                Go to Scheduler Logs
              </Button>
            }
          />
        </Card>
      </div>
    );
  }


  return (
    <div style={{ padding: '24px' }}>
      {/* Page title */}
      <div style={{ marginBottom: '24px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <Title level={2} style={{ margin: 0 }}>
            <DatabaseOutlined style={{ marginRight: '8px' }} />
            Scheduler Management
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

      {/* Main content area - simplified without Tabs */}
      <Card>
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
                    <Tooltip
                      title={
                        source.description.includes('Coming soon') 
                          ? 'This data source is coming soon' 
                          : !source.enabled && scheduledTasks.filter(task => {
                              const taskType = task.type.toLowerCase();
                              const sourceIdLower = source.id.toLowerCase();
                              return (
                                (sourceIdLower === 'eodhd_news' && taskType === 'eodhd_news') ||
                                (sourceIdLower === 'watchlist_news' && taskType === 'watchlist_news') ||
                                (sourceIdLower === 'market_data' && taskType === 'market_data') ||
                                (sourceIdLower === 'earnings_calendar' && taskType === 'earnings_calendar') ||
                                (sourceIdLower === 'ipo_calendar' && taskType === 'ipo_calendar') ||
                                (sourceIdLower.includes('events') && taskType.includes('events')) ||
                                (sourceIdLower.includes('macro') && (taskType.includes('macro') || taskType.includes('indicators')))
                              );
                            }).length === 0
                          ? 'Please configure data fetch strategy first'
                          : undefined
                      }
                    >
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

                {/* Cron Expression Display with Configuration Icon */}
                {(() => {
                  const cronInfo = getCronInfo(source.id);
                  if (cronInfo) {
                    // Format configuration content for tooltip (excluding cron expression)
                    const formattedParams = formatTaskParams(source.id, cronInfo.params);
                    const configContent = (
                      <div style={{ maxWidth: '400px', color: '#fff' }}>
                        <div style={{ marginBottom: '10px' }}>
                          <Space>
                            <ScheduleOutlined style={{ color: '#1890ff' }} />
                            <span style={{ fontSize: '13px', fontWeight: 'bold' }}>Configuration Details</span>
                          </Space>
                        </div>
                        <div style={{ marginBottom: '8px', fontSize: '12px' }}>
                          Timezone: <span style={{ color: '#1890ff' }}>{cronInfo.timezone}</span>
                        </div>
                        {cronInfo.description && (
                          <div style={{ marginBottom: '8px', fontSize: '12px' }}>
                            Description: <span style={{ color: '#52c41a' }}>{cronInfo.description}</span>
                          </div>
                        )}
                        {formattedParams && (
                          <div style={{ marginTop: '10px', paddingTop: '10px', borderTop: '1px solid rgba(255,255,255,0.2)' }}>
                            <div style={{ fontSize: '12px', fontWeight: 'bold', marginBottom: '6px' }}>
                              Configuration Parameters:
                            </div>
                            <pre style={{ 
                              margin: 0, 
                              fontSize: '11px', 
                              fontFamily: 'monospace',
                              whiteSpace: 'pre-wrap',
                              color: '#1890ff',
                              backgroundColor: 'rgba(0,0,0,0.3)',
                              padding: '8px',
                              borderRadius: '4px',
                              maxHeight: '200px',
                              overflow: 'auto'
                            }}>
                              {formattedParams}
                            </pre>
                            {cronInfo.params && Object.keys(cronInfo.params).length > 0 && (
                              <div style={{ marginTop: '10px' }}>
                                <div style={{ fontSize: '11px', opacity: 0.7, marginBottom: '4px' }}>
                                  Full JSON:
                                </div>
                                <pre style={{ 
                                  margin: 0, 
                                  fontSize: '10px', 
                                  fontFamily: 'monospace',
                                  whiteSpace: 'pre-wrap',
                                  color: '#52c41a',
                                  backgroundColor: 'rgba(0,0,0,0.3)',
                                  padding: '6px',
                                  borderRadius: '4px',
                                  maxHeight: '150px',
                                  overflow: 'auto'
                                }}>
                                  {JSON.stringify(cronInfo.params, null, 2)}
                                </pre>
                              </div>
                            )}
                          </div>
                        )}
                      </div>
                    );

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
                        <div style={{ marginBottom: '8px', display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <Text code style={{ 
                            fontSize: '12px', 
                            backgroundColor: '#fff', 
                            padding: '2px 6px', 
                            borderRadius: '3px',
                            color: cronInfo.isValid ? '#000' : '#ff4d4f'
                          }}>
                            {cronInfo.expression}
                          </Text>
                          <Tooltip 
                            title={configContent}
                            placement="top"
                            overlayStyle={{ maxWidth: '450px' }}
                            overlayInnerStyle={{ 
                              backgroundColor: 'rgba(0,0,0,0.85)',
                              padding: '12px',
                              borderRadius: '6px'
                            }}
                          >
                            <SettingOutlined 
                              style={{ 
                                color: cronInfo.isValid ? '#1890ff' : '#ff4d4f', 
                                fontSize: '16px',
                                cursor: 'pointer',
                                transition: 'color 0.3s'
                              }} 
                            />
                          </Tooltip>
                        </div>
                        {!cronInfo.isValid && cronInfo.validationMessage && (
                          <div style={{ marginBottom: '4px' }}>
                            <Text type="danger" style={{ fontSize: '11px' }}>
                              {cronInfo.validationMessage}
                            </Text>
                          </div>
                        )}
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
          dataSourceType={selectedDataSource.id as 'eodhd_news' | 'watchlist_news' | 'eodhd_events' | 'eodhd_macro' | 'eodhd_fundamentals' | 'finviz_screener' | 'eodhd_stock_prices' | 'eodhd_eod_stock_prices' | 'market_data' | 'earnings_calendar' | 'ipo_calendar' | 'watchlist_fundamentals'}
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
                    tab={<span><TagOutlined />Highlight Setting</span>}
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
                      message="Keyword Highlight Instructions"
                      description="Create themes with keywords to automatically highlight matching words in news content. Each theme can have a custom color, and keywords will be highlighted with that color when found in the original news text."
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
