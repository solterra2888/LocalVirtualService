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
  Popconfirm,
  ConfigProvider,
  theme as antdTheme
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
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

import DataSourceConfigModal from '../components/DataSourceConfigModal';
import TaskStatusModal from '../components/TaskStatusModal';

const { Title, Text } = Typography;
const { RangePicker } = DatePicker;
const { Option } = Select;

// Theme colors per FRONTEND_STYLE_GUIDE: Light/Dark + Behance
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
    hoverBg: undefined as string | undefined,
    scheduleValidBg: '#f6f8fa',
    scheduleInvalidBg: '#fff2f0',
    scheduleValidBorder: '#e1e4e8',
    scheduleInvalidBorder: '#ffccc7',
    codeBlockText: '#000',
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
    scheduleValidBg: 'rgba(33, 150, 243, 0.08)',
    scheduleInvalidBg: 'rgba(244, 67, 54, 0.12)',
    scheduleValidBorder: 'rgba(255, 255, 255, 0.08)',
    scheduleInvalidBorder: 'rgba(244, 67, 54, 0.4)',
    codeBlockText: '#e0e6f1',
  },
} as const;

// Behance-style: rounded corners, soft shadow, minimal border (per FRONTEND_STYLE_GUIDE §3)
const CARD_STYLE_BEHANCE = {
  borderRadius: 12,
  boxShadow: '0 4px 24px rgba(0, 0, 0, 0.12)',
  border: '1px solid #f0f0f0',
};
const CARD_STYLE_BEHANCE_DARK = {
  borderRadius: 12,
  boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)',
  border: '1px solid rgba(255, 255, 255, 0.06)',
};

// Data source configuration
interface DataSource {
  id: string;
  name: string;
  type: 'eodhd' | 'watchlist' | 'cctv' | 'finviz' | 'finviz_screener' | 'eodhd_events' | 'eodhd_macro' | 'eodhd_fundamentals' | 'eodhd_stock_prices' | 'eodhd_eod_stock_prices' | 'market_data' | 'earnings_calendar' | 'ipo_calendar' | 'watchlist_fundamentals' | 'twitter_subscription' | 'youtube_subscription' | 'other';
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
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const cardStyle = isDark ? CARD_STYLE_BEHANCE_DARK : CARD_STYLE_BEHANCE;
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
      id: 'twitter_subscription',
      name: 'Twitter Subscription Feed',
      type: 'twitter_subscription',
      description: 'Fetch tweets from subscribed Twitter authors (supports multi-user subscriptions)',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_twitter_schedule',
          name: 'Configure Twitter Fetch Schedule',
          description: 'Set up periodic Twitter subscription fetching (default: every 30 minutes)',
          endpoint: '/scheduler/tasks/twitter-subscription',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'youtube_subscription',
      name: 'YouTube Subscription Feed',
      type: 'youtube_subscription',
      description: 'Fetch videos from subscribed YouTube channels via RSS (supports multi-user subscriptions)',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_youtube_schedule',
          name: 'Configure YouTube Fetch Schedule',
          description: 'Set up periodic YouTube subscription fetching (default: every 60 minutes)',
          endpoint: '/scheduler/tasks/youtube-subscription',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'cctv_news',
      name: 'CCTV News (新闻联播)',
      type: 'cctv',
      description: 'Scrape daily news from CCTV News (央视新闻联播) website',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_cctv_news_schedule',
          name: 'Configure CCTV News Fetch',
          description: 'Set up periodic CCTV news fetching (daily at 8 PM Beijing time)',
          endpoint: '/scheduler/tasks/cctv-news',
          params: {},
          icon: <GlobalOutlined />
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
          (sourceIdLower === 'cctv_news' && taskType === 'cctv_news') ||
          (sourceIdLower === 'eodhd_stock_prices' && taskType === 'eodhd_stock_prices') ||
          (sourceIdLower === 'eodhd_eod_stock_prices' && taskType === 'eodhd_eod_stock_prices') ||
          (sourceIdLower === 'market_data' && taskType === 'market_data') ||
          (sourceIdLower === 'earnings_calendar' && taskType === 'earnings_calendar') ||
          (sourceIdLower === 'ipo_calendar' && taskType === 'ipo_calendar') ||
          (sourceIdLower === 'twitter_subscription' && taskType === 'twitter_subscription') ||
          (sourceIdLower === 'youtube_subscription' && taskType === 'youtube_subscription') ||
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
        (sourceIdLower === 'cctv_news' && taskType === 'cctv_news') ||
        (sourceIdLower === 'eodhd_stock_prices' && taskType === 'eodhd_stock_prices') ||
        (sourceIdLower === 'eodhd_eod_stock_prices' && taskType === 'eodhd_eod_stock_prices') ||
        (sourceIdLower === 'futu_themes' && taskType === 'futu_themes') ||
        (sourceIdLower === 'market_data' && taskType === 'market_data') ||
        (sourceIdLower === 'earnings_calendar' && taskType === 'earnings_calendar') ||
        (sourceIdLower === 'ipo_calendar' && taskType === 'ipo_calendar') ||
        (sourceIdLower === 'twitter_subscription' && taskType === 'twitter_subscription') ||
        (sourceIdLower === 'youtube_subscription' && taskType === 'youtube_subscription') ||
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

    // Format cctv_news parameters
    if (sourceIdLower === 'cctv_news') {
      const parts: string[] = [];
      
      if (params.date) {
        parts.push(`📅 Date: ${params.date}`);
      }
      
      if (params.max_news) {
        parts.push(`📰 Max News: ${params.max_news}`);
      }
      
      if (params.auto_analyze !== undefined) {
        parts.push(`🤖 Auto Analyze: ${params.auto_analyze ? '✅' : '❌'}`);
      }
      
      if (params.analysis_templates && Array.isArray(params.analysis_templates) && params.analysis_templates.length > 0) {
        parts.push(`📋 Templates: ${params.analysis_templates.join(', ')}`);
      }
      
      if (params.max_concurrent) {
        parts.push(`⚡ Max Concurrent: ${params.max_concurrent}`);
      }
      
      if (params.enable_theme_matching !== undefined) {
        parts.push(`🏷️ Theme Matching: ${params.enable_theme_matching ? '✅' : '❌'}`);
      }
      
      return parts.length > 0 ? parts.join(' | ') : null;
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
          (sourceIdLower === 'cctv_news' && taskType === 'cctv_news') ||
          (sourceIdLower === 'twitter_subscription' && taskType === 'twitter_subscription') ||
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
      if (dataSource.id === 'eodhd_news' || dataSource.id === 'watchlist_news' || dataSource.id === 'cctv_news' || dataSource.id === 'twitter_subscription' || dataSource.id === 'youtube_subscription' || dataSource.id === 'eodhd_events' || dataSource.id === 'eodhd_macro' || dataSource.id === 'eodhd_fundamentals' || dataSource.id === 'finviz_screener' || dataSource.id === 'eodhd_stock_prices' || dataSource.id === 'eodhd_eod_stock_prices' || dataSource.id === 'market_data' || dataSource.id === 'earnings_calendar' || dataSource.id === 'ipo_calendar' || dataSource.id === 'watchlist_fundamentals') {
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
        <div style={{ padding: '12px 40px 32px', backgroundColor: colors.pageBg, minHeight: '100%' }}>
          <Card style={cardStyle} bodyStyle={{ padding: '20px 24px' }}>
            <Result
              status="info"
              title="Scheduler configuration requires admin access"
              subTitle="Task Management and Execution History have moved to Scheduler Logs."
              extra={
                <Button
                  type="primary"
                  onClick={() => navigate('/scheduler/logs')}
                  style={{
                    borderRadius: 20,
                    fontWeight: 500,
                    height: 40,
                    paddingLeft: 24,
                    paddingRight: 24,
                    background: isDark ? '#2196F3' : '#1890ff',
                    borderColor: isDark ? '#2196F3' : '#1890ff',
                    boxShadow: isDark 
                      ? '0 4px 16px rgba(33, 150, 243, 0.35)' 
                      : '0 4px 16px rgba(24, 144, 255, 0.3)',
                    transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
                  }}
                  onMouseEnter={(e) => {
                    const btn = e.currentTarget as HTMLButtonElement;
                    btn.style.transform = 'translateY(-2px)';
                    btn.style.boxShadow = isDark 
                      ? '0 6px 20px rgba(33, 150, 243, 0.45)' 
                      : '0 6px 20px rgba(24, 144, 255, 0.4)';
                  }}
                  onMouseLeave={(e) => {
                    const btn = e.currentTarget as HTMLButtonElement;
                    btn.style.transform = 'translateY(0)';
                    btn.style.boxShadow = isDark 
                      ? '0 4px 16px rgba(33, 150, 243, 0.35)' 
                      : '0 4px 16px rgba(24, 144, 255, 0.3)';
                  }}
                >
                  Go to Scheduler Logs
                </Button>
              }
            />
          </Card>
        </div>
      </ConfigProvider>
    );
  }


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
    <div style={{ padding: '12px 40px 32px', backgroundColor: colors.pageBg, minHeight: '100%' }}>
      {/* Page title */}
      <div style={{ marginBottom: 20, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <Title level={2} style={{ margin: 0, marginBottom: 4, color: colors.text }}>
            <DatabaseOutlined style={{ marginRight: '8px' }} />
            Scheduler Management
          </Title>
          <Text type="secondary" style={{ margin: 0, display: 'block', color: colors.textSecondary }}>
            Simplified data fetching scheduler - Manage EODHD, Finviz and other data sources
          </Text>
        </div>
        <Space>
          <Button
            icon={<ReloadOutlined />}
            onClick={refreshAllSources}
            loading={loading}
            style={{
              borderRadius: 20,
              fontWeight: 500,
              height: 36,
              paddingLeft: 16,
              paddingRight: 16,
              border: `1.5px solid ${colors.cardBorder}`,
              color: colors.text,
              background: 'transparent',
              transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
            }}
            onMouseEnter={(e) => {
              const btn = e.currentTarget as HTMLButtonElement;
              if (!btn.disabled) {
                btn.style.background = isDark ? 'rgba(255, 255, 255, 0.08)' : 'rgba(0, 0, 0, 0.04)';
                btn.style.transform = 'translateY(-1px)';
                btn.style.borderColor = isDark ? '#2196F3' : '#1890ff';
              }
            }}
            onMouseLeave={(e) => {
              const btn = e.currentTarget as HTMLButtonElement;
              if (!btn.disabled) {
                btn.style.background = 'transparent';
                btn.style.transform = 'translateY(0)';
                btn.style.borderColor = colors.cardBorder;
              }
            }}
          >
            Refresh Status
          </Button>
        </Space>
      </div>

      {/* Main content - Behance list container */}
      <div
        style={{
          ...cardStyle,
          marginBottom: 0,
          overflow: 'hidden',
        }}
      >
        {/* Stats bar - inline, no cards */}
        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: 24,
            padding: '20px 24px',
            borderBottom: `1px solid ${colors.cardBorder}`,
          }}
        >
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
            <span style={{ fontSize: 24, fontWeight: 600, color: isDark ? '#2196F3' : '#1890ff' }}>{dataSources.length}</span>
            <span style={{ fontSize: 13, color: colors.textSecondary }}>Data Sources</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
            <span style={{ fontSize: 24, fontWeight: 600, color: '#52c41a' }}>{enabledSources}</span>
            <span style={{ fontSize: 13, color: colors.textSecondary }}>Enabled</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
            <span style={{ fontSize: 24, fontWeight: 600, color: '#faad14' }}>{runningSources}</span>
            <span style={{ fontSize: 13, color: colors.textSecondary }}>Running</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
            <span style={{ fontSize: 24, fontWeight: 600, color: '#722ed1' }}>{scheduledTasks.length}</span>
            <span style={{ fontSize: 13, color: colors.textSecondary }}>Scheduled ({enabledTasks} on)</span>
          </div>
        </div>

        {/* Data source list - Behance list style */}
        <div style={{ padding: 0 }}>
          {dataSources.map((source, index) => {
            const cronInfo = getCronInfo(source.id);
            const formattedParams = cronInfo ? formatTaskParams(source.id, cronInfo.params) : null;
            const configContent = cronInfo ? (
              <div style={{ maxWidth: 400, color: '#fff' }}>
                <div style={{ marginBottom: 10 }}>
                  <Space>
                    <ScheduleOutlined style={{ color: '#2196F3' }} />
                    <span style={{ fontSize: 13, fontWeight: 'bold' }}>Configuration Details</span>
                  </Space>
                </div>
                <div style={{ marginBottom: 8, fontSize: 12 }}>
                  Timezone: <span style={{ color: '#2196F3' }}>{cronInfo.timezone}</span>
                </div>
                {cronInfo.description && (
                  <div style={{ marginBottom: 8, fontSize: 12 }}>
                    Description: <span style={{ color: '#52c41a' }}>{cronInfo.description}</span>
                  </div>
                )}
                {formattedParams && (
                  <div style={{ marginTop: 10, paddingTop: 10, borderTop: '1px solid rgba(255,255,255,0.2)' }}>
                    <div style={{ fontSize: 12, fontWeight: 'bold', marginBottom: 6 }}>Configuration Parameters:</div>
                    <pre style={{ margin: 0, fontSize: 11, fontFamily: 'monospace', whiteSpace: 'pre-wrap', color: '#2196F3', backgroundColor: 'rgba(0,0,0,0.3)', padding: 8, borderRadius: 8, maxHeight: 200, overflow: 'auto' }}>
                      {formattedParams}
                    </pre>
                    {cronInfo.params && Object.keys(cronInfo.params).length > 0 && (
                      <div style={{ marginTop: 10 }}>
                        <div style={{ fontSize: 11, opacity: 0.7, marginBottom: 4 }}>Full JSON:</div>
                        <pre style={{ margin: 0, fontSize: 10, fontFamily: 'monospace', whiteSpace: 'pre-wrap', color: '#52c41a', backgroundColor: 'rgba(0,0,0,0.3)', padding: 6, borderRadius: 4, maxHeight: 150, overflow: 'auto' }}>
                          {JSON.stringify(cronInfo.params, null, 2)}
                        </pre>
                      </div>
                    )}
                  </div>
                )}
              </div>
            ) : null;

            return (
              <div
                key={source.id}
                style={{
                  display: 'flex',
                  flexWrap: 'wrap',
                  alignItems: 'center',
                  gap: 16,
                  padding: '16px 24px',
                  borderBottom: index < dataSources.length - 1 ? `1px solid ${colors.cardBorder}` : 'none',
                  backgroundColor: 'transparent',
                  transition: 'background-color 0.2s',
                  opacity: source.enabled ? 1 : 0.65,
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.backgroundColor = isDark ? colors.hoverBg ?? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.02)';
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.backgroundColor = 'transparent';
                }}
              >
                {/* Left: name, status, description, schedule */}
                <div style={{ flex: '1 1 280px', minWidth: 0 }}>
                  <div style={{ display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: 8, marginBottom: 4 }}>
                    <span style={{ fontWeight: 600, fontSize: 15, color: colors.text }}>{source.name}</span>
                    {getStatusTag(source.status)}
                    {source.lastRun && (
                      <span style={{ fontSize: 12, color: colors.textSecondary }}>Last: {source.lastRun}</span>
                    )}
                  </div>
                  <div style={{ fontSize: 13, color: colors.textSecondary, marginBottom: cronInfo ? 8 : 0 }}>
                    {source.description}
                  </div>
                  {cronInfo && (
                    <div
                      style={{
                        display: 'inline-flex',
                        alignItems: 'center',
                        gap: 6,
                        padding: '6px 10px',
                        borderRadius: 8,
                        backgroundColor: cronInfo.isValid ? colors.scheduleValidBg : colors.scheduleInvalidBg,
                        border: `1px solid ${cronInfo.isValid ? colors.scheduleValidBorder : colors.scheduleInvalidBorder}`,
                      }}
                    >
                      <ScheduleOutlined style={{ color: cronInfo.isValid ? (isDark ? '#2196F3' : '#1890ff') : '#ff4d4f', fontSize: 12 }} />
                      <Text code style={{ fontSize: 11, backgroundColor: 'transparent', padding: 0, color: cronInfo.isValid ? colors.codeBlockText : '#ff4d4f' }}>
                        {cronInfo.expression}
                      </Text>
                      <Tooltip title={configContent} placement="top" overlayStyle={{ maxWidth: 450 }} overlayInnerStyle={{ backgroundColor: 'rgba(0,0,0,0.85)', padding: 12, borderRadius: 8 }}>
                        <SettingOutlined style={{ color: cronInfo.isValid ? (isDark ? '#2196F3' : '#1890ff') : '#ff4d4f', fontSize: 14, cursor: 'pointer' }} />
                      </Tooltip>
                    </div>
                  )}
                </div>
                {/* Right: primary action buttons + switch - Modern style */}
                <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexShrink: 0 }}>
                  {source.quickActions.map((action) => {
                    const isConfigButton = action.id.includes('configure') || action.id === 'update_fundamentals_data';
                    return (
                      <Tooltip key={action.id} title={isConfigButton ? action.name : action.description} placement="topRight">
                        <Button
                          type="default"
                          size="middle"
                          icon={isConfigButton ? <SettingOutlined /> : action.icon}
                          onClick={() => showQuickActionModal(action, source)}
                          disabled={action.id.includes('configure') ? false : !source.enabled}
                          loading={source.status === 'running'}
                          style={{
                            borderRadius: isConfigButton ? 8 : 20,
                            fontWeight: 500,
                            height: 36,
                            width: isConfigButton ? 36 : undefined,
                            paddingLeft: isConfigButton ? 0 : 16,
                            paddingRight: isConfigButton ? 0 : 16,
                            border: `1.5px solid ${isDark ? '#2196F3' : '#1890ff'}`,
                            color: isDark ? '#2196F3' : '#1890ff',
                            background: 'transparent',
                            transition: 'all 0.3s cubic-bezier(0.4, 0, 0.2, 1)',
                            boxShadow: 'none',
                          }}
                          onMouseEnter={(e) => {
                            const btn = e.currentTarget as HTMLButtonElement;
                            if (!btn.disabled) {
                              btn.style.background = isDark ? '#2196F3' : '#1890ff';
                              btn.style.color = '#fff';
                              btn.style.transform = 'translateY(-2px) scale(1.02)';
                              btn.style.boxShadow = isDark 
                                ? '0 6px 20px rgba(33, 150, 243, 0.4)' 
                                : '0 6px 20px rgba(24, 144, 255, 0.3)';
                            }
                          }}
                          onMouseLeave={(e) => {
                            const btn = e.currentTarget as HTMLButtonElement;
                            if (!btn.disabled) {
                              btn.style.background = 'transparent';
                              btn.style.color = isDark ? '#2196F3' : '#1890ff';
                              btn.style.transform = 'translateY(0) scale(1)';
                              btn.style.boxShadow = 'none';
                            }
                          }}
                        >
                          {isConfigButton ? null : action.name}
                        </Button>
                      </Tooltip>
                    );
                  })}
                  {source.id !== 'eodhd_fundamentals' && (
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
                                (sourceIdLower === 'cctv_news' && taskType === 'cctv_news') ||
                                (sourceIdLower === 'twitter_subscription' && taskType === 'twitter_subscription') ||
                                (sourceIdLower === 'youtube_subscription' && taskType === 'youtube_subscription') ||
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
                      <div
                        style={{
                          position: 'relative',
                          display: 'inline-block',
                        }}
                      >
                        <Switch
                          checked={source.enabled}
                          onChange={(checked) => toggleDataSource(source.id, checked)}
                          disabled={source.description.includes('Coming soon') || loading}
                          loading={loading && source.status === 'running'}
                          style={{
                            backgroundColor: source.enabled 
                              ? (isDark ? '#2196F3' : '#1890ff')
                              : undefined,
                            boxShadow: source.enabled 
                              ? `0 0 12px ${isDark ? 'rgba(33, 150, 243, 0.4)' : 'rgba(24, 144, 255, 0.3)'}`
                              : undefined,
                          }}
                        />
                      </div>
                    </Tooltip>
                  )}
                </div>
              </div>
            );
          })}
        </div>
      </div>





      {/* Data source specific configuration modal */}
      {selectedDataSource && (
        <DataSourceConfigModal
          open={dataSourceConfigVisible}
          onCancel={() => {
            setDataSourceConfigVisible(false);
            setSelectedDataSource(null);
          }}
          onSuccess={handleDataSourceConfigSuccess}
          dataSourceType={selectedDataSource.id as 'eodhd_news' | 'watchlist_news' | 'cctv_news' | 'eodhd_events' | 'eodhd_macro' | 'eodhd_fundamentals' | 'finviz_screener' | 'eodhd_stock_prices' | 'eodhd_eod_stock_prices' | 'market_data' | 'earnings_calendar' | 'ipo_calendar' | 'watchlist_fundamentals'}
          dataSourceName={selectedDataSource.name}
        />
      )}

      {/* Quick action parameter configuration dialog - Behance modal */}
      <Modal
        title={
          <Space style={{ color: colors.text }}>
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
        styles={{
          content: {
            borderRadius: 12,
            boxShadow: isDark ? '0 8px 32px rgba(0, 0, 0, 0.4)' : '0 4px 24px rgba(0, 0, 0, 0.12)',
            border: `1px solid ${colors.cardBorder}`,
            backgroundColor: colors.cardBg,
          },
          header: {
            borderBottomColor: colors.cardBorder,
            color: colors.text,
          },
          body: {
            color: colors.text,
          },
        }}
        okButtonProps={{
          style: {
            borderRadius: 10,
            fontWeight: 500,
            height: 38,
            paddingLeft: 24,
            paddingRight: 24,
            background: isDark ? '#2196F3' : '#1890ff',
            borderColor: isDark ? '#2196F3' : '#1890ff',
            boxShadow: isDark
              ? '0 4px 12px rgba(33, 150, 243, 0.3)'
              : '0 4px 12px rgba(24, 144, 255, 0.25)',
            transition: 'all 0.3s ease',
          },
        }}
        cancelButtonProps={{
          style: {
            borderRadius: 8,
            height: 38,
            paddingLeft: 20,
            paddingRight: 20,
            border: `1px solid ${colors.cardBorder}`,
            color: colors.textSecondary,
            background: 'transparent',
            transition: 'all 0.3s ease',
          },
        }}
      >
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
                          style={{
                            borderRadius: 16,
                            fontWeight: 500,
                            height: 32,
                            paddingLeft: 14,
                            paddingRight: 14,
                            background: isDark ? '#2196F3' : '#1890ff',
                            borderColor: isDark ? '#2196F3' : '#1890ff',
                            boxShadow: isDark 
                              ? '0 2px 8px rgba(33, 150, 243, 0.3)' 
                              : '0 2px 8px rgba(24, 144, 255, 0.25)',
                          }}
                        >
                          New Theme
                        </Button>
                        <Button
                          icon={<BulbOutlined />}
                          onClick={initializeDefaultThemes}
                          size="small"
                          style={{
                            borderRadius: 16,
                            height: 32,
                            paddingLeft: 14,
                            paddingRight: 14,
                            border: `1.5px solid ${colors.cardBorder}`,
                            color: colors.textSecondary,
                            background: 'transparent',
                          }}
                        >
                          Initialize Default Themes
                        </Button>
                        <Button
                          icon={<ReloadOutlined />}
                          onClick={loadThemes}
                          size="small"
                          style={{
                            borderRadius: 16,
                            height: 32,
                            paddingLeft: 14,
                            paddingRight: 14,
                            border: `1.5px solid ${colors.cardBorder}`,
                            color: colors.textSecondary,
                            background: 'transparent',
                          }}
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

      {/* Theme editing modal - Behance */}
      <Modal
        title={<span style={{ color: colors.text }}>{selectedTheme ? 'Edit Theme' : 'New Theme'}</span>}
        open={themeModalVisible}
        onOk={() => themeForm.submit()}
        onCancel={() => {
          setThemeModalVisible(false);
          setSelectedTheme(null);
          themeForm.resetFields();
        }}
        okText="Save"
        cancelText="Cancel"
        styles={{
          content: {
            borderRadius: 12,
            boxShadow: isDark ? '0 8px 32px rgba(0, 0, 0, 0.4)' : '0 4px 24px rgba(0, 0, 0, 0.12)',
            border: `1px solid ${colors.cardBorder}`,
            backgroundColor: colors.cardBg,
          },
          header: {
            borderBottomColor: colors.cardBorder,
            color: colors.text,
          },
          body: {
            color: colors.text,
          },
        }}
        okButtonProps={{
          style: {
            borderRadius: 10,
            fontWeight: 500,
            height: 38,
            paddingLeft: 24,
            paddingRight: 24,
            background: isDark ? '#2196F3' : '#1890ff',
            borderColor: isDark ? '#2196F3' : '#1890ff',
            boxShadow: isDark
              ? '0 4px 12px rgba(33, 150, 243, 0.3)'
              : '0 4px 12px rgba(24, 144, 255, 0.25)',
            transition: 'all 0.3s ease',
          },
        }}
        cancelButtonProps={{
          style: {
            borderRadius: 8,
            height: 38,
            paddingLeft: 20,
            paddingRight: 20,
            border: `1px solid ${colors.cardBorder}`,
            color: colors.textSecondary,
            background: 'transparent',
            transition: 'all 0.3s ease',
          },
        }}
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
    </ConfigProvider>
  );
};

export default SchedulerPage;
