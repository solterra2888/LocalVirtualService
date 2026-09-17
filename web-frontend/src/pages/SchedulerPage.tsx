import React, { useState, useEffect, useCallback, useMemo } from 'react';
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
  theme as antdTheme,
  Grid,
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
  FileTextOutlined,
  HolderOutlined,
} from '@ant-design/icons';
import {
  DndContext,
  type DragEndEvent,
  KeyboardSensor,
  PointerSensor,
  closestCenter,
  useSensor,
  useSensors,
} from '@dnd-kit/core';
import type { Modifier } from '@dnd-kit/core';
import {
  SortableContext,
  arrayMove,
  sortableKeyboardCoordinates,
  useSortable,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import dayjs from 'dayjs';
import { api } from '../services/api';
import { getModalStyles } from '../utils/modalStyles';
import { schedulerAPI, ScheduledTask } from '../services/scheduler';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

import DataSourceConfigModal from '../components/DataSourceConfigModal';
import TaskStatusModal from '../components/TaskStatusModal';

const { Title, Text, Paragraph } = Typography;
const { RangePicker } = DatePicker;
const { Option } = Select;
const { useBreakpoint } = Grid;

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
/** 数据获取类：拉取外部数据；报告生成类：预生成摘要/报告等 */
type SchedulerCategory = 'data_fetch' | 'report_generation';

interface DataSource {
  id: string;
  name: string;
  type: 'eodhd' | 'watchlist' | 'cctv' | 'finviz' | 'finviz_screener' | 'eodhd_events' | 'eodhd_macro' | 'eodhd_fundamentals' | 'eodhd_stock_prices' | 'eodhd_eod_stock_prices' | 'market_data' | 'earnings_calendar' | 'ipo_calendar' | 'watchlist_fundamentals' | 'twitter_subscription' | 'youtube_subscription' | 'podcast_subscription' | 'news_feed' | 'zhishixingqiu_subscription' | 'substack_subscription' | 'weibo_subscription' | 'twitter_author_summary' | 'youtube_author_summary' | 'theme_report' | 'market_weekly_report' | 'daily_market_report' | 'the_close_summary' | 'cnbc_closing_bell' | 'bloomberg_surveillance' | 'zhishixingqiu_summary' | 'substack_summary' | 'fed_dashboard' | 'form13f' | 'other';
  category: SchedulerCategory;
  description: string;
  enabled: boolean;
  status: 'idle' | 'running' | 'success' | 'error';
  lastRun?: string;
  lastSuccess?: string;
  sortOrder?: number;
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

/** dnd-kit 修饰器：限制拖拽只能沿垂直轴移动 */
const restrictToVerticalAxis: Modifier = ({ transform }) => {
  if (!transform) return transform;
  return { ...transform, x: 0 };
};

function getDefaultCategoryOrder(sources: DataSource[], category: SchedulerCategory): string[] {
  return sources
    .filter((s) => s.category === category)
    .sort((a, b) => (a.sortOrder ?? 0) - (b.sortOrder ?? 0))
    .map((s) => s.id);
}

function mergeCategoryOrder(saved: string[] | undefined, defaults: string[]): string[] {
  const defaultSet = new Set(defaults);
  const validSaved = (saved ?? []).filter((id) => defaultSet.has(id));
  const savedSet = new Set(validSaved);
  return [...validSaved, ...defaults.filter((id) => !savedSet.has(id))];
}

/** 返回静态目录的默认顺序（首屏兜底；挂载后从后端拉取用户偏好覆盖） */
function getDefaultCategoryOrders(sources: DataSource[]): Record<SchedulerCategory, string[]> {
  return {
    data_fetch: getDefaultCategoryOrder(sources, 'data_fetch'),
    report_generation: getDefaultCategoryOrder(sources, 'report_generation'),
  };
}

const INITIAL_SCHEDULER_DATA_SOURCES: DataSource[] = [
    {
      id: 'twitter_subscription',
      name: 'Twitter Subscription Feed',
      type: 'twitter_subscription',
      category: 'data_fetch',
      sortOrder: 10,
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
      category: 'data_fetch',
      sortOrder: 20,
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
      id: 'podcast_subscription',
      name: 'Podcast Subscription Feed',
      type: 'podcast_subscription',
      category: 'data_fetch',
      sortOrder: 30,
      description: 'Fetch episodes from subscribed Podcast RSS feeds and auto-transcribe recent episodes (supports multi-user subscriptions)',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_podcast_schedule',
          name: 'Configure Podcast Fetch Schedule',
          description: 'Set up periodic Podcast subscription fetching (default: every 60 minutes)',
          endpoint: '/scheduler/tasks/podcast-subscription',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'news_feed',
      name: 'News Feed (My Feeds)',
      type: 'news_feed',
      category: 'data_fetch',
      sortOrder: 40,
      description: 'Fetch EODHD news and distribute to My Feeds subscriptions (Yahoo Finance, CNBC, Seeking Alpha, etc.)',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_news_feed_schedule',
          name: 'Configure News Feed Schedule',
          description: 'Set up periodic news feed fetching (default: every 30 minutes)',
          endpoint: '/scheduler/tasks/news-feed',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'zhishixingqiu_subscription',
      name: '知识星球 Subscription Feed',
      type: 'zhishixingqiu_subscription',
      category: 'data_fetch',
      sortOrder: 50,
      description: '定时从订阅的知识星球抓取最新话题，写入 My Feeds（支持多用户共享同一星球内容，Cookie 集中管理）',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_zsxq_schedule',
          name: 'Configure ZSXQ Fetch Schedule',
          description: '配置知识星球定期抓取计划（默认每 30 分钟一次）',
          endpoint: '/scheduler/tasks/zhishixingqiu-subscription',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'weibo_subscription',
      name: 'Weibo Subscription Feed',
      type: 'weibo_subscription',
      category: 'data_fetch',
      sortOrder: 60,
      description: '定时从订阅的微博博主抓取最新博文，写入 My Feeds（支持多用户共享同一博主内容，Cookie 集中管理与轮换）',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_weibo_schedule',
          name: 'Configure Weibo Fetch Schedule',
          description: '配置微博定期抓取计划（默认每 30 分钟一次）',
          endpoint: '/scheduler/tasks/weibo-subscription',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'substack_subscription',
      name: 'Substack Subscription Feed',
      type: 'substack_subscription',
      category: 'data_fetch',
      sortOrder: 70,
      description: '定时从 Substack 订阅源抓取文章（源A 订阅 newsletter）+ 关注作者动态（源B Notes），写入 My Feeds（管理员共享 Cookie，订阅源取两源并集）',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_substack_schedule',
          name: 'Configure Substack Fetch Schedule',
          description: '配置 Substack 定期抓取计划（默认每 30 分钟一次）',
          endpoint: '/scheduler/tasks/substack-subscription',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'twitter_author_summary',
      name: 'Twitter Daily Report',
      type: 'twitter_author_summary',
      category: 'report_generation',
      sortOrder: 10,
      description: 'Schedule automatic Twitter Daily Report generation. Runs the full pipeline: pre-generates per-author summaries (shared across users), then synthesizes personalized daily reports for each subscriber.',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_twitter_author_summary_schedule',
          name: 'Configure Report Schedule',
          description: 'Set the schedule for Twitter Daily Report generation (default: daily at 5 AM CST). Runs Phase A (author summaries) → Phase B (user reports) automatically.',
          endpoint: '/scheduler/tasks/twitter-author-summary',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'youtube_author_summary',
      name: 'YouTube Daily Report',
      type: 'youtube_author_summary',
      category: 'report_generation',
      sortOrder: 20,
      description: 'Schedule automatic YouTube Daily Report generation. Runs the full pipeline: pre-generates per-channel summaries (shared across users), then synthesizes personalized daily reports for each subscriber.',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_youtube_author_summary_schedule',
          name: 'Configure Report Schedule',
          description: 'Set the schedule for YouTube Daily Report generation (default: daily at 6 AM CST). Runs Phase A (channel summaries) → Phase B (user reports) automatically.',
          endpoint: '/scheduler/tasks/youtube-author-summary',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'theme_report',
      name: 'Theme Report',
      type: 'theme_report',
      category: 'report_generation',
      sortOrder: 30,
      description: 'Pre-generate Theme Report static HTML for all users with Theme Report tagged themes (saved to OSS).',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_theme_report_schedule',
          name: 'Configure Theme Report Schedule',
          description: 'Set up periodic Theme Report generation (default: weekly Monday 2 PM CST)',
          endpoint: '/scheduler/tasks/theme-report',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'market_weekly_report',
      name: 'Market Weekly Report',
      type: 'market_weekly_report',
      category: 'report_generation',
      sortOrder: 40,
      description: 'Pre-generate Market Weekly Report static HTML for all users with Market Weekly tagged themes (indices, sectors, forex, commodities, bonds WoW performance, AI themes, economic calendar).',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_market_weekly_report_schedule',
          name: 'Configure Market Weekly Report Schedule',
          description: 'Set up periodic Market Weekly Report generation (default: weekly Friday 5 PM ET)',
          endpoint: '/scheduler/tasks/market-weekly-report',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'daily_market_report',
      name: 'Daily Market Report',
      type: 'daily_market_report',
      category: 'report_generation',
      sortOrder: 50,
      description: 'Pre-generate Daily Market Report (Home/Stockview/My Subscription/Market Overview) static HTML for all users.',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_daily_market_report_schedule',
          name: 'Configure Daily Market Report Schedule',
          description: 'Set up periodic Daily Market Report generation (default: weekdays-next-day 8 AM Beijing, after Bloomberg/CNBC close summaries)',
          endpoint: '/scheduler/tasks/daily-market-report',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'the_close_summary',
      name: 'Bloomberg The Close Summary',
      type: 'the_close_summary',
      category: 'report_generation',
      sortOrder: 60,
      description: 'Generate daily PM Brief summary from Bloomberg Television "The Close" episode transcript (8-section structured markdown).',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_the_close_summary_schedule',
          name: 'Configure The Close Summary Schedule',
          description: 'Set up periodic The Close PM Brief generation (default: weekdays 7 AM Beijing / 7 PM ET)',
          endpoint: '/scheduler/tasks/the-close-summary',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'cnbc_closing_bell',
      name: 'CNBC Closing Bell Summary',
      type: 'cnbc_closing_bell',
      category: 'report_generation',
      sortOrder: 70,
      description: 'Generate daily Stock News summary from CNBC "Closing Bell" episode transcript (6-section structured markdown).',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_cnbc_closing_bell_schedule',
          name: 'Configure CNBC Closing Bell Schedule',
          description: 'Set up periodic Closing Bell Stock News generation (default: weekdays 7 AM Beijing / 7 PM ET)',
          endpoint: '/scheduler/tasks/cnbc-closing-bell',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'bloomberg_surveillance',
      name: 'Bloomberg Surveillance Summary',
      type: 'bloomberg_surveillance',
      category: 'report_generation',
      sortOrder: 75,
      description: 'Generate daily AM Brief summary from Bloomberg "Surveillance" morning episode transcript (8-section structured markdown, multi-segment merge).',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_bloomberg_surveillance_schedule',
          name: 'Configure Bloomberg Surveillance Schedule',
          description: 'Set up periodic Surveillance AM Brief generation (default: weekdays 7 AM Beijing / 7 PM ET, after the morning episode uploads)',
          endpoint: '/scheduler/tasks/bloomberg-surveillance',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'zhishixingqiu_summary',
      name: 'Zhishixingqiu Daily Summary',
      type: 'zhishixingqiu_summary',
      category: 'report_generation',
      sortOrder: 80,
      description: 'Generate daily aggregated summary from all subscribed Zhishixingqiu groups, organized by group (global single report).',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_zhishixingqiu_summary_schedule',
          name: 'Configure Zhishixingqiu Summary Schedule',
          description: 'Set up periodic Zhishixingqiu daily summary generation (default: 10 PM Beijing)',
          endpoint: '/scheduler/tasks/zhishixingqiu-summary',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'substack_summary',
      name: 'Substack Daily Summary',
      type: 'substack_summary',
      category: 'report_generation',
      sortOrder: 90,
      description: 'Generate daily aggregated summary from all subscribed Substack publications (articles + notes), two-phase: per-publication LLM (cached) then per-user mechanical concat.',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_substack_summary_schedule',
          name: 'Configure Substack Summary Schedule',
          description: 'Set up periodic Substack daily summary generation (default: after fetch, Beijing day)',
          endpoint: '/scheduler/tasks/substack-summary',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'fed_dashboard',
      name: 'Fed Dashboard (FOMC)',
      type: 'fed_dashboard',
      category: 'data_fetch',
      sortOrder: 160,
      description: 'Poll Federal Reserve press RSS every 30 min: discover new FOMC statements/minutes, extract rate decisions (range/votes/dissents), generate AI summaries, and maintain the Fed updates feed.',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_fed_dashboard_schedule',
          name: 'Configure Fed Dashboard Poll',
          description: 'Set up periodic Fed RSS polling (default: every 30 min)',
          endpoint: '/scheduler/tasks/fed-dashboard',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'form13f',
      name: 'Form 13F (SEC)',
      type: 'form13f',
      category: 'data_fetch',
      sortOrder: 170,
      description: 'Poll 20 preset institution CIKs on weekdays: fetch 13F-HR filings from EDGAR, fold restatement/new-holdings amendments, import that filer\'s full holdings with sector classification, and compute snapshots (sector allocation / top 20 holdings / top 5 buys & sells).',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_form13f_schedule',
          name: 'Configure Form 13F Poll',
          description: 'Set up periodic 13F CIK polling (default: weekdays 21:00 UTC)',
          endpoint: '/scheduler/tasks/form13f',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'eodhd_stock_prices',
      name: 'EODHD Stock Prices',
      type: 'eodhd_stock_prices',
      category: 'data_fetch',
      sortOrder: 60,
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
      category: 'data_fetch',
      sortOrder: 70,
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
        },
        {
          id: 'create_weekly_backfill',
          name: 'Create Weekly Backfill (400d)',
          description: 'One-click create weekly Saturday 05:30 ET backfill task (400 days, daily periods). Guarantees 1Y history coverage for Theme Report; complements the daily fetch.',
          endpoint: '/scheduler/tasks/eodhd-eod-stock-prices/weekly-backfill',
          params: {},
          icon: <CalendarOutlined />
        }
      ]
    },
    // futu_themes 数据源已删除 - 改为纯手动主题管理
    {
      id: 'market_data',
      name: 'Market Data (Indices, Forex, Commodities, Bonds, Sectors)',
      type: 'market_data',
      category: 'data_fetch',
      sortOrder: 80,
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
      category: 'data_fetch',
      sortOrder: 90,
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
      category: 'data_fetch',
      sortOrder: 100,
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
      category: 'data_fetch',
      sortOrder: 110,
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
      category: 'data_fetch',
      sortOrder: 120,
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
      category: 'data_fetch',
      sortOrder: 130,
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
      category: 'data_fetch',
      sortOrder: 140,
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
      category: 'data_fetch',
      sortOrder: 150,
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
];

const SchedulerPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const cardStyle = isDark ? CARD_STYLE_BEHANCE_DARK : CARD_STYLE_BEHANCE;
  const screens = useBreakpoint();
  const isLgUp = !!screens?.lg;
  const [dataSources, setDataSources] = useState<DataSource[]>(INITIAL_SCHEDULER_DATA_SOURCES);
  const [categoryOrder, setCategoryOrder] = useState<Record<SchedulerCategory, string[]>>(() =>
    getDefaultCategoryOrders(INITIAL_SCHEDULER_DATA_SOURCES)
  );

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

  // 从后端加载当前用户的卡片排序偏好，合并覆盖静态默认顺序（跨设备同步）
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const remote = await schedulerAPI.getCardOrder();
      if (cancelled) return;
      // 后端无偏好（首次访问）时保持静态默认，避免无谓 re-render
      if (!remote.data_fetch?.length && !remote.report_generation?.length) return;
      setCategoryOrder(() => {
        const defaults = getDefaultCategoryOrders(INITIAL_SCHEDULER_DATA_SOURCES);
        return {
          data_fetch: mergeCategoryOrder(remote.data_fetch, defaults.data_fetch),
          report_generation: mergeCategoryOrder(remote.report_generation, defaults.report_generation),
        };
      });
    })();
    return () => { cancelled = true; };
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
          (sourceIdLower === 'podcast_subscription' && taskType === 'podcast_subscription') ||
          (sourceIdLower === 'news_feed' && taskType === 'news_feed') ||
          (sourceIdLower === 'zhishixingqiu_subscription' && taskType === 'zhishixingqiu_subscription') ||
          (sourceIdLower === 'substack_subscription' && taskType === 'substack_subscription') ||
          (sourceIdLower === 'weibo_subscription' && taskType === 'weibo_subscription') ||
          (sourceIdLower === 'twitter_author_summary' && taskType === 'twitter_author_summary') ||
          (sourceIdLower === 'youtube_author_summary' && taskType === 'youtube_author_summary') ||
          (sourceIdLower === 'theme_report' && taskType === 'theme_report') ||
          (sourceIdLower === 'market_weekly_report' && taskType === 'market_weekly_report') ||
          (sourceIdLower === 'daily_market_report' && taskType === 'daily_market_report') ||
          (sourceIdLower === 'the_close_summary' && taskType === 'the_close_summary') ||
          (sourceIdLower === 'cnbc_closing_bell' && taskType === 'cnbc_closing_bell') ||
          (sourceIdLower === 'bloomberg_surveillance' && taskType === 'bloomberg_surveillance') ||
          (sourceIdLower === 'zhishixingqiu_summary' && taskType === 'zhishixingqiu_summary') ||
          (sourceIdLower === 'substack_summary' && taskType === 'substack_summary') ||
          (sourceIdLower === 'fed_dashboard' && taskType === 'fed_dashboard') ||
          (sourceIdLower === 'form13f' && taskType === 'form13f') ||
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
        (sourceIdLower === 'podcast_subscription' && taskType === 'podcast_subscription') ||
        (sourceIdLower === 'news_feed' && taskType === 'news_feed') ||
        (sourceIdLower === 'zhishixingqiu_subscription' && taskType === 'zhishixingqiu_subscription') ||
        (sourceIdLower === 'substack_subscription' && taskType === 'substack_subscription') ||
        (sourceIdLower === 'weibo_subscription' && taskType === 'weibo_subscription') ||
        (sourceIdLower === 'twitter_author_summary' && taskType === 'twitter_author_summary') ||
        (sourceIdLower === 'youtube_author_summary' && taskType === 'youtube_author_summary') ||
        (sourceIdLower === 'theme_report' && taskType === 'theme_report') ||
        (sourceIdLower === 'market_weekly_report' && taskType === 'market_weekly_report') ||
        (sourceIdLower === 'daily_market_report' && taskType === 'daily_market_report') ||
        (sourceIdLower === 'the_close_summary' && taskType === 'the_close_summary') ||
        (sourceIdLower === 'cnbc_closing_bell' && taskType === 'cnbc_closing_bell') ||
        (sourceIdLower === 'bloomberg_surveillance' && taskType === 'bloomberg_surveillance') ||
        (sourceIdLower === 'zhishixingqiu_summary' && taskType === 'zhishixingqiu_summary') ||
        (sourceIdLower === 'substack_summary' && taskType === 'substack_summary') ||
        (sourceIdLower === 'fed_dashboard' && taskType === 'fed_dashboard') ||
        (sourceIdLower === 'form13f' && taskType === 'form13f') ||
        (sourceIdLower.includes('events') && taskType.includes('events')) ||
        (sourceIdLower.includes('macro') && (taskType.includes('macro') || taskType.includes('indicators'))) ||
        (sourceIdLower.includes('exchange') && taskType.includes('exchange')) ||
        (sourceIdLower.includes('fundamentals') && taskType.includes('fundamentals')) ||
        (sourceIdLower.includes('finviz_screener') && taskType.includes('finviz_screener'))
      );
    });
  };

  // Get the existing scheduled task for a data source (prefer enabled, then any with cron)
  const getExistingScheduledTask = (sourceId: string): ScheduledTask | undefined => {
    const relatedTasks = getRelatedTasks(sourceId);
    return (
      relatedTasks.find((task) => task.enabled && task.cron_expression) ||
      relatedTasks.find((task) => task.cron_expression)
    );
  };

  // Get cron expression info for a data source
  const getCronInfo = (sourceId: string) => {
    const task = getExistingScheduledTask(sourceId);
    if (!task) {
      return null;
    }
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
          (sourceIdLower === 'youtube_subscription' && taskType === 'youtube_subscription') ||
          (sourceIdLower === 'podcast_subscription' && taskType === 'podcast_subscription') ||
          (sourceIdLower === 'news_feed' && taskType === 'news_feed') ||
          (sourceIdLower === 'zhishixingqiu_subscription' && taskType === 'zhishixingqiu_subscription') ||
          (sourceIdLower === 'substack_subscription' && taskType === 'substack_subscription') ||
          (sourceIdLower === 'weibo_subscription' && taskType === 'weibo_subscription') ||
          (sourceIdLower === 'twitter_author_summary' && taskType === 'twitter_author_summary') ||
          (sourceIdLower === 'youtube_author_summary' && taskType === 'youtube_author_summary') ||
          (sourceIdLower === 'theme_report' && taskType === 'theme_report') ||
          (sourceIdLower === 'market_weekly_report' && taskType === 'market_weekly_report') ||
          (sourceIdLower === 'daily_market_report' && taskType === 'daily_market_report') ||
          (sourceIdLower === 'the_close_summary' && taskType === 'the_close_summary') ||
          (sourceIdLower === 'cnbc_closing_bell' && taskType === 'cnbc_closing_bell') ||
          (sourceIdLower === 'bloomberg_surveillance' && taskType === 'bloomberg_surveillance') ||
          (sourceIdLower === 'zhishixingqiu_summary' && taskType === 'zhishixingqiu_summary') ||
          (sourceIdLower === 'substack_summary' && taskType === 'substack_summary') ||
          (sourceIdLower === 'fed_dashboard' && taskType === 'fed_dashboard') ||
          (sourceIdLower === 'form13f' && taskType === 'form13f') ||
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

      // 刷新任务列表，确保新建/变更的定时任务立即可见（如 weekly-backfill 创建后）
      await loadScheduledTasks();

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
      if (dataSource.id === 'twitter_subscription' || dataSource.id === 'youtube_subscription' || dataSource.id === 'podcast_subscription' || dataSource.id === 'news_feed' || dataSource.id === 'zhishixingqiu_subscription' || dataSource.id === 'weibo_subscription' || dataSource.id === 'twitter_author_summary' || dataSource.id === 'youtube_author_summary' || dataSource.id === 'theme_report' || dataSource.id === 'market_weekly_report' || dataSource.id === 'daily_market_report' || dataSource.id === 'the_close_summary' || dataSource.id === 'cnbc_closing_bell' || dataSource.id === 'bloomberg_surveillance' || dataSource.id === 'zhishixingqiu_summary' || dataSource.id === 'substack_subscription' || dataSource.id === 'substack_summary' || dataSource.id === 'fed_dashboard' || dataSource.id === 'form13f' || dataSource.id === 'eodhd_events' || dataSource.id === 'eodhd_macro' || dataSource.id === 'eodhd_fundamentals' || dataSource.id === 'finviz_screener' || dataSource.id === 'eodhd_stock_prices' || dataSource.id === 'eodhd_eod_stock_prices' || dataSource.id === 'market_data' || dataSource.id === 'earnings_calendar' || dataSource.id === 'ipo_calendar' || dataSource.id === 'watchlist_fundamentals') {
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
    
    form.setFieldsValue(action.params || {});
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

  const orderSourcesByCategory = useCallback((category: SchedulerCategory) => {
    const byId = new Map(
      dataSources.filter((s) => s.category === category).map((s) => [s.id, s])
    );
    return categoryOrder[category]
      .map((id) => byId.get(id))
      .filter((s): s is DataSource => !!s);
  }, [categoryOrder, dataSources]);

  const dataFetchSources = useMemo(
    () => orderSourcesByCategory('data_fetch'),
    [orderSourcesByCategory]
  );
  const reportGenerationSources = useMemo(
    () => orderSourcesByCategory('report_generation'),
    [orderSourcesByCategory]
  );

  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 8 } }),
    useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates })
  );

  const handleCategoryDragEnd = (category: SchedulerCategory) => async (event: DragEndEvent) => {
    const { active, over } = event;
    if (!over || active.id === over.id) {
      return;
    }
    const prevSnapshot = categoryOrder; // 拖拽前快照（handler 每次渲染重建，闭包最新）
    const items = prevSnapshot[category];
    const oldIndex = items.indexOf(String(active.id));
    const newIndex = items.indexOf(String(over.id));
    if (oldIndex < 0 || newIndex < 0) {
      return;
    }
    const newItems = arrayMove(items, oldIndex, newIndex);
    setCategoryOrder({ ...prevSnapshot, [category]: newItems }); // 乐观更新

    // 落库（本 category 全量重编号 1..N）；失败回滚到拖拽前
    const payload = newItems.map((source_id, idx) => ({ source_id, sort_order: idx + 1 }));
    const resp = await schedulerAPI.reorderCards(category, payload);
    if (!resp.success) {
      setCategoryOrder(prevSnapshot);
      message.error('排序保存失败，已回滚');
    }
  };

  const SchedulerListItem: React.FC<{
    source: DataSource;
    index: number;
    columnLength: number;
  }> = ({ source, index, columnLength }) => {
    const {
      attributes,
      listeners,
      setNodeRef,
      transform,
      transition,
      isDragging,
    } = useSortable({ id: source.id });
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
        ref={setNodeRef}
        style={{
          display: 'flex',
          alignItems: 'stretch',
          minHeight: 118,
          transform: CSS.Transform.toString(transform),
          transition,
          opacity: isDragging ? 0.65 : source.enabled ? 1 : 0.65,
          backgroundColor: isDragging
            ? (isDark ? colors.hoverBg ?? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.02)')
            : 'transparent',
          borderBottom: index < columnLength - 1 ? `1px solid ${colors.cardBorder}` : 'none',
        }}
      >
        <Tooltip title="拖拽排序">
          <div
            {...attributes}
            {...listeners}
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'center',
              width: 36,
              flexShrink: 0,
              cursor: isDragging ? 'grabbing' : 'grab',
              color: colors.textSecondary,
              touchAction: 'none',
            }}
          >
            <HolderOutlined style={{ fontSize: 16 }} />
          </div>
        </Tooltip>
        <div
          style={{
            flex: 1,
            minWidth: 0,
            display: 'flex',
            flexWrap: 'wrap',
            alignItems: 'center',
            gap: 16,
            padding: '16px 24px 16px 0',
            transition: 'background-color 0.2s',
            minHeight: 118,
          }}
          onMouseEnter={(e) => {
            if (!isDragging) {
              e.currentTarget.parentElement!.style.backgroundColor = isDark
                ? colors.hoverBg ?? 'rgba(255,255,255,0.06)'
                : 'rgba(0,0,0,0.02)';
            }
          }}
          onMouseLeave={(e) => {
            if (!isDragging) {
              e.currentTarget.parentElement!.style.backgroundColor = 'transparent';
            }
          }}
        >
        <div style={{ flex: '1 1 280px', minWidth: 0 }}>
          <div style={{ display: 'flex', alignItems: 'center', flexWrap: 'wrap', gap: 8, marginBottom: 4 }}>
            <span style={{ fontWeight: 600, fontSize: 15, color: colors.text }}>{source.name}</span>
            {getStatusTag(source.status)}
            {source.lastRun && (
              <span style={{ fontSize: 12, color: colors.textSecondary }}>Last: {source.lastRun}</span>
            )}
          </div>
          <Paragraph
            type="secondary"
            ellipsis={{ rows: 2, tooltip: source.description }}
            style={{
              fontSize: 13,
              marginBottom: 8,
              lineHeight: 1.5,
              minHeight: 39,
            }}
          >
            {source.description}
          </Paragraph>
          {cronInfo ? (
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
              {cronInfo.timezone && (
                <>
                  <span style={{ color: colors.textSecondary, fontSize: 11, opacity: 0.6 }}>|</span>
                  <GlobalOutlined style={{ color: cronInfo.isValid ? colors.textSecondary : '#ff4d4f', fontSize: 11 }} />
                  <Text style={{ fontSize: 11, color: cronInfo.isValid ? colors.textSecondary : '#ff4d4f' }}>
                    {cronInfo.timezone}
                  </Text>
                </>
              )}
              <Tooltip title={configContent} placement="top" overlayStyle={{ maxWidth: 450 }} overlayInnerStyle={{ backgroundColor: 'rgba(0,0,0,0.85)', padding: 12, borderRadius: 8 }}>
                <SettingOutlined style={{ color: cronInfo.isValid ? (isDark ? '#2196F3' : '#1890ff') : '#ff4d4f', fontSize: 14, cursor: 'pointer' }} />
              </Tooltip>
            </div>
          ) : (
            <div style={{ minHeight: 34 }} aria-hidden />
          )}
        </div>
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
                        (sourceIdLower === 'podcast_subscription' && taskType === 'podcast_subscription') ||
                        (sourceIdLower === 'news_feed' && taskType === 'news_feed') ||
                        (sourceIdLower === 'zhishixingqiu_subscription' && taskType === 'zhishixingqiu_subscription') ||
          (sourceIdLower === 'substack_subscription' && taskType === 'substack_subscription') ||
          (sourceIdLower === 'weibo_subscription' && taskType === 'weibo_subscription') ||
                        (sourceIdLower === 'twitter_author_summary' && taskType === 'twitter_author_summary') ||
                        (sourceIdLower === 'youtube_author_summary' && taskType === 'youtube_author_summary') ||
                        (sourceIdLower === 'theme_report' && taskType === 'theme_report') ||
                        (sourceIdLower === 'market_weekly_report' && taskType === 'market_weekly_report') ||
                        (sourceIdLower === 'daily_market_report' && taskType === 'daily_market_report') ||
                        (sourceIdLower === 'the_close_summary' && taskType === 'the_close_summary') ||
                        (sourceIdLower === 'cnbc_closing_bell' && taskType === 'cnbc_closing_bell') ||
          (sourceIdLower === 'bloomberg_surveillance' && taskType === 'bloomberg_surveillance') ||
          (sourceIdLower === 'zhishixingqiu_summary' && taskType === 'zhishixingqiu_summary') ||
          (sourceIdLower === 'substack_summary' && taskType === 'substack_summary') ||
          (sourceIdLower === 'fed_dashboard' && taskType === 'fed_dashboard') ||
          (sourceIdLower === 'form13f' && taskType === 'form13f') ||
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
      </div>
    );
  };

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
            数据获取类与报告生成类任务分栏展示 — 管理行情、新闻、摘要与定时报告
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

        {/* Data source list — two columns: equal height via flex stretch + bottom spacer */}
        <div
          style={{
            display: 'flex',
            flexDirection: isLgUp ? 'row' : 'column',
            alignItems: 'stretch',
            margin: 0,
          }}
        >
          <div
            style={{
              flex: isLgUp ? '1 1 50%' : '1 1 auto',
              display: 'flex',
              flexDirection: 'column',
              minWidth: 0,
            }}
          >
            <div
              style={{
                padding: '14px 24px 10px',
                borderBottom: `1px solid ${colors.cardBorder}`,
                flexShrink: 0,
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <CloudDownloadOutlined style={{ color: isDark ? '#2196F3' : '#1890ff', fontSize: 16 }} />
                <span style={{ fontWeight: 600, fontSize: 15, color: colors.text }}>数据获取类</span>
              </div>
              <Text style={{ display: 'block', marginTop: 4, fontSize: 12, color: colors.textSecondary }}>
                定时拉取行情、新闻、订阅源、日历与基本面等外部数据 · 拖拽左侧把手可调整顺序
              </Text>
            </div>
            <div
              style={{
                flex: 1,
                display: 'flex',
                flexDirection: 'column',
                minHeight: 0,
              }}
            >
              <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
                <DndContext
                  sensors={sensors}
                  collisionDetection={closestCenter}
                  modifiers={[restrictToVerticalAxis]}
                  onDragEnd={handleCategoryDragEnd('data_fetch')}
                >
                  <SortableContext
                    items={dataFetchSources.map((source) => source.id)}
                    strategy={verticalListSortingStrategy}
                  >
                    {dataFetchSources.map((source, index) => (
                      <SchedulerListItem
                        key={source.id}
                        source={source}
                        index={index}
                        columnLength={dataFetchSources.length}
                      />
                    ))}
                  </SortableContext>
                </DndContext>
                <div style={{ flex: 1, minHeight: 0 }} aria-hidden />
              </div>
            </div>
          </div>
          <div
            style={{
              flex: isLgUp ? '1 1 50%' : '1 1 auto',
              display: 'flex',
              flexDirection: 'column',
              minWidth: 0,
              borderTop: isLgUp ? undefined : `1px solid ${colors.cardBorder}`,
              borderLeft: isLgUp ? `1px solid ${colors.cardBorder}` : undefined,
            }}
          >
            <div
              style={{
                padding: '14px 24px 10px',
                borderBottom: `1px solid ${colors.cardBorder}`,
                flexShrink: 0,
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <FileTextOutlined style={{ color: isDark ? '#52c41a' : '#389e0d', fontSize: 16 }} />
                <span style={{ fontWeight: 600, fontSize: 15, color: colors.text }}>报告生成类</span>
              </div>
              <Text style={{ display: 'block', marginTop: 4, fontSize: 12, color: colors.textSecondary }}>
                预生成作者/频道摘要，并可链接至日报等报告流程 · 拖拽左侧把手可调整顺序
              </Text>
            </div>
            <div
              style={{
                flex: 1,
                display: 'flex',
                flexDirection: 'column',
                minHeight: 0,
              }}
            >
              <div style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
                <DndContext
                  sensors={sensors}
                  collisionDetection={closestCenter}
                  modifiers={[restrictToVerticalAxis]}
                  onDragEnd={handleCategoryDragEnd('report_generation')}
                >
                  <SortableContext
                    items={reportGenerationSources.map((source) => source.id)}
                    strategy={verticalListSortingStrategy}
                  >
                    {reportGenerationSources.map((source, index) => (
                      <SchedulerListItem
                        key={source.id}
                        source={source}
                        index={index}
                        columnLength={reportGenerationSources.length}
                      />
                    ))}
                  </SortableContext>
                </DndContext>
                <div style={{ flex: 1, minHeight: 0 }} aria-hidden />
              </div>
            </div>
          </div>
        </div>
      </div>





      {/* Data source specific configuration modal */}
      {selectedDataSource && (() => {
        const existingTask = getExistingScheduledTask(selectedDataSource.id);
        return (
          <DataSourceConfigModal
            open={dataSourceConfigVisible}
            onCancel={() => {
              setDataSourceConfigVisible(false);
              setSelectedDataSource(null);
            }}
            onSuccess={handleDataSourceConfigSuccess}
            dataSourceType={selectedDataSource.id as 'eodhd_news' | 'watchlist_news' | 'cctv_news' | 'eodhd_events' | 'eodhd_macro' | 'eodhd_fundamentals' | 'finviz_screener' | 'eodhd_stock_prices' | 'eodhd_eod_stock_prices' | 'market_data' | 'earnings_calendar' | 'ipo_calendar' | 'watchlist_fundamentals' | 'twitter_subscription' | 'youtube_subscription' | 'podcast_subscription' | 'news_feed' | 'zhishixingqiu_subscription' | 'substack_subscription' | 'weibo_subscription' | 'twitter_author_summary' | 'youtube_author_summary' | 'theme_report' | 'market_weekly_report' | 'daily_market_report' | 'the_close_summary' | 'cnbc_closing_bell' | 'bloomberg_surveillance' | 'zhishixingqiu_summary' | 'substack_summary' | 'fed_dashboard' | 'form13f'}
            dataSourceName={selectedDataSource.name}
            initialCronExpression={existingTask?.cron_expression}
            initialTimezone={existingTask?.timezone}
          />
        );
      })()}

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
        styles={getModalStyles(themeMode)}
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
                          tooltip="Custom topic filter keyword"
                        >
                          <Input placeholder="e.g.: technology" />
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
