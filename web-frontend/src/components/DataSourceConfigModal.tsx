import React, { useState, useEffect } from 'react';
import {
  Modal,
  Form,
  Input,
  Select,
  Button,
  Space,
  Card,
  Typography,
  Row,
  Col,
  InputNumber,
  Tooltip,
  message,
  Switch,
  Divider
} from 'antd';
import {
  ClockCircleOutlined,
  InfoCircleOutlined,
  CheckCircleOutlined,
  ScheduleOutlined,
  BulbOutlined,
  BarChartOutlined,
  QuestionCircleOutlined,
  DatabaseOutlined,
  LineChartOutlined,
  CalendarOutlined,
  GlobalOutlined,
  MessageOutlined,
  TwitterOutlined,
  YoutubeOutlined,
  FileTextOutlined,
  StarOutlined,
  StockOutlined,
  CustomerServiceOutlined,
  ReadOutlined,
  BankOutlined,
  FundViewOutlined,
} from '@ant-design/icons';
import { schedulerAPI, CreateEODHDNewsTaskParams, TimezoneOption } from '../services/scheduler';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { getModalStyles } from '../utils/modalStyles';
import CronExpressionGuide from './CronExpressionGuide';

const { Text } = Typography;

const THEME_COLORS = {
  light: {
    cardSecondaryBg: '#f9f9f9',
    borderSecondary: '#f0f0f0',
    headingPrimary: '#1890ff',
  },
  dark: {
    cardSecondaryBg: '#252d3d',
    borderSecondary: '#364152',
    headingPrimary: '#60a5fa',
  },
} as const;
const { Option } = Select;

interface DataSourceConfigModalProps {
  open: boolean;
  onCancel: () => void;
  onSuccess: (taskId: string) => void;
  dataSourceType: 'eodhd_news' | 'watchlist_news' | 'cctv_news' | 'twitter_subscription' | 'youtube_subscription' | 'podcast_subscription' | 'news_feed' | 'zhishixingqiu_subscription' | 'substack_subscription' | 'weibo_subscription' | 'twitter_author_summary' | 'youtube_author_summary' | 'theme_report' | 'market_weekly_report' | 'daily_market_report' | 'the_close_summary' | 'cnbc_closing_bell' | 'bloomberg_surveillance' | 'zhishixingqiu_summary' | 'substack_summary' | 'fed_dashboard' | 'form13f' | 'eodhd_events' | 'eodhd_macro' | 'eodhd_fundamentals' | 'finviz_screener' | 'eodhd_stock_prices' | 'eodhd_eod_stock_prices' | 'futu_themes' | 'market_data' | 'earnings_calendar' | 'ipo_calendar' | 'watchlist_fundamentals';
  dataSourceName: string;
  /** 已有定时任务的 Cron，打开弹窗时预填 */
  initialCronExpression?: string;
  /** 已有定时任务的时区，打开弹窗时预填 */
  initialTimezone?: string;
}

interface ConfigFormData {
  // 调度配置
  cronExpression: string;
  timezone: string;
  
  // 任务参数
  taskParams?: Record<string, any>;

  // News Feed params
  maxResults?: number;
  recentDays?: number;
}

const DataSourceConfigModal: React.FC<DataSourceConfigModalProps> = ({
  open,
  onCancel,
  onSuccess,
  dataSourceType,
  dataSourceName,
  initialCronExpression,
  initialTimezone,
}) => {
  const { themeMode } = useDashboardSettings();
  const colors = THEME_COLORS[themeMode];
  const [form] = Form.useForm<ConfigFormData>();
  const [loading, setLoading] = useState(false);

  const [timezoneOptions, setTimezoneOptions] = useState<TimezoneOption[]>([]);
  const [showCronGuide, setShowCronGuide] = useState(false);

  // 数据源配置映射
  const dataSourceConfigs = {
    eodhd_news: {
      icon: <InfoCircleOutlined style={{ color: '#1890ff' }} />,
      params: ['max_results', 'recent_days', 'symbols', 'topic', 'keywords'],
      defaultSchedule: '*/15 * * * *',
      defaultName: 'EODHD News Auto Fetch',
      description: 'Get latest stock news and market information from EODHD API'
    },
    watchlist_news: {
      icon: <BulbOutlined style={{ color: '#f5222d' }} />,
      params: ['max_results', 'recent_days'],
      defaultSchedule: '*/15 * * * *',
      defaultName: 'My Watchlist News Auto Fetch',
      description: 'Get news for stocks in your watchlist automatically from EODHD API'
    },
    cctv_news: {
      icon: <GlobalOutlined style={{ color: '#ff4d4f' }} />,
      params: ['date', 'max_news'],
      defaultSchedule: '0 20 * * *',
      defaultName: 'CCTV News Auto Fetch',
      description: 'Scrape CCTV News (新闻联播) daily at 8 PM Beijing time (Asia/Shanghai timezone)'
    },
    twitter_subscription: {
      icon: <TwitterOutlined style={{ color: '#1DA1F2' }} />,
      params: [],  // No parameters needed - auto-aggregates all subscriptions
      defaultSchedule: '*/30 * * * *',
      defaultName: 'Twitter Subscription Feed',
      description: 'Automatically fetch tweets from all subscribed Twitter authors every 30 minutes'
    },
    youtube_subscription: {
      icon: <YoutubeOutlined style={{ color: '#FF0000' }} />,
      params: [],  // No parameters needed - auto-aggregates all subscriptions via RSS
      defaultSchedule: '0 * * * *',
      defaultName: 'YouTube Subscription Feed',
      description: 'Automatically fetch videos from all subscribed YouTube channels every hour (via RSS)'
    },
    podcast_subscription: {
      icon: <CustomerServiceOutlined style={{ color: '#722ed1' }} />,
      params: [],
      defaultSchedule: '0 * * * *',
      defaultName: 'Podcast Subscription Feed',
      description: 'Automatically fetch episodes from all subscribed Podcast RSS feeds every hour; ASR for episodes published within the last 48 hours'
    },
    weibo_subscription: {
      icon: <MessageOutlined style={{ color: '#E6162D' }} />,
      params: [],
      defaultSchedule: '*/30 * * * *',
      defaultName: 'Weibo Subscription Feed',
      description: '定时从订阅的微博博主抓取最新博文（默认每 30 分钟一次，Cookie 集中管理与轮换）'
    },
    eodhd_events: {
      icon: <ScheduleOutlined style={{ color: '#52c41a' }} />,
      params: ['days', 'lookback_days', 'fetch_next_week', 'start_date', 'end_date', 'country'],
      defaultSchedule: '0 12 * * *',
      defaultName: 'Economic Events Auto Fetch',
      description: 'Get economic calendar event data including GDP, CPI and other important indicators; daily run with lookback window to backfill actual values of published events'
    },
    eodhd_macro: {
      icon: <BarChartOutlined style={{ color: '#722ed1' }} />,
      params: ['country', 'indicator'],
      defaultSchedule: '0 10 1 * *',
      defaultName: 'Macro Indicators Auto Fetch',
      description: 'Get macroeconomic indicator data such as inflation rate, unemployment rate, etc.'
    },
    eodhd_fundamentals: {
      icon: <BarChartOutlined style={{ color: '#13c2c2' }} />,
      params: ['exchanges', 'data_types'],
      defaultSchedule: '0 6 * * 0',
      defaultName: 'EODHD Fundamentals Auto Fetch',
      description: 'Get comprehensive company fundamentals data from EODHD API for multiple markets'
    },
    finviz_screener: {
      icon: <DatabaseOutlined style={{ color: '#fa8c16' }} />,
      params: [],
      defaultSchedule: '0 8 * * 1-5',
      defaultName: 'Finviz Screener Data Auto Fetch',
      description: 'Get comprehensive stock screener data from Finviz with technical and fundamental metrics (no parameters required)'
    },
    eodhd_stock_prices: {
      icon: <BarChartOutlined style={{ color: '#1890ff' }} />,
      params: [],
      defaultSchedule: '*/5 * * * 1-5',
      defaultName: 'EODHD Stock Prices Auto Fetch',
      description: 'Get real-time stock prices for all watchlist stocks from EODHD API (every 5 minutes on weekdays)'
    },
    eodhd_eod_stock_prices: {
      icon: <BarChartOutlined style={{ color: '#52c41a' }} />,
      params: ['days_back'],
      defaultSchedule: '0 9 * * 1-5',
      defaultName: 'EODHD EOD Stock Prices Auto Fetch',
      description: 'Get historical end-of-day stock prices for all watchlist stocks from EODHD API (daily on weekdays)'
    },
    futu_themes: {
      icon: <DatabaseOutlined style={{ color: '#eb2f96' }} />,
      params: ['markets', 'max_pages', 'scrape_stocks'],
      defaultSchedule: '0 2 * * *',
      defaultName: 'Futu Themes Auto Fetch',
      description: 'Get investment themes and related stocks from Futu website (HK and US markets, daily at 2 AM HKT)'
    },
    market_data: {
      icon: <LineChartOutlined style={{ color: '#52c41a' }} />,
      params: ['asset_types', 'fetch_realtime', 'fetch_eod', 'eod_days'],
      defaultSchedule: '0 */6 * * *',
      defaultName: 'Market Data Auto Fetch',
      description: 'Get real-time and historical market data (indices, forex, commodities, bonds, sectors) every 6 hours'
    },
    earnings_calendar: {
      icon: <ClockCircleOutlined style={{ color: '#fa8c16' }} />,
      params: ['days', 'fetch_next_week', 'start_date', 'end_date'],
      defaultSchedule: '0 6 * * 0',
      defaultName: 'Earnings Calendar Auto Fetch',
      description: 'Get US earnings calendar data weekly (no S&P 500 filtering, all US stocks)'
    },
    ipo_calendar: {
      icon: <CalendarOutlined style={{ color: '#722ed1' }} />,
      params: ['days', 'fetch_next_week', 'start_date', 'end_date'],
      defaultSchedule: '0 6 * * 0',
      defaultName: 'IPO Calendar Auto Fetch',
      description: 'Get IPO calendar data weekly from EODHD API (all exchanges). Note: EODHD API does not support exchange filtering - all exchanges data will be returned.'
    },
    watchlist_fundamentals: {
      icon: <DatabaseOutlined style={{ color: '#1890ff' }} />,
      params: ['data_types', 'skip_recent_hours'],
      defaultSchedule: '0 2 * * 0',
      defaultName: 'Watchlist Fundamentals Weekly Update',
      description: 'Automatically update fundamentals data for all watchlist stocks (weekly on Sunday 2 AM, skips stocks updated within 24 hours)'
    },
    news_feed: {
      icon: <FileTextOutlined style={{ color: '#722ed1' }} />,
      params: ['max_results', 'recent_days'],
      defaultSchedule: '*/30 * * * *',
      defaultName: 'News Feed Auto Fetch',
      description: 'Fetch EODHD news and distribute to My Feeds subscriptions (Yahoo Finance, CNBC, Seeking Alpha, etc.) every 30 minutes'
    },
    zhishixingqiu_subscription: {
      icon: <StarOutlined style={{ color: '#FF6B00' }} />,
      params: [],
      defaultSchedule: '*/30 * * * *',
      defaultName: 'Zhishixingqiu Subscription Feed',
      description: 'Automatically fetch topics from all subscribed Knowledge Planet (知识星球) groups every 30 minutes'
    },
    substack_subscription: {
      icon: <ReadOutlined style={{ color: '#FF6719' }} />,
      params: [],
      defaultSchedule: '*/30 * * * *',
      defaultName: 'Substack Subscription Feed',
      description: 'Fetch Substack articles (subscribed newsletters) + notes (followed authors) every 30 minutes; admin-shared cookie, union of two sources'
    },
    twitter_author_summary: {
      icon: <TwitterOutlined style={{ color: '#1DA1F2' }} />,
      params: ['force'],
      defaultSchedule: '0 4 * * *',
      defaultName: 'Twitter Daily Report',
      description: 'Full report pipeline: generates per-author summaries (Phase A, shared across users), then synthesizes personalized Twitter Daily Reports (Phase B). Default: daily 04:00 Beijing, before Daily Market Report 06:00.'
    },
    youtube_author_summary: {
      icon: <YoutubeOutlined style={{ color: '#FF0000' }} />,
      params: ['force'],
      defaultSchedule: '30 4 * * *',
      defaultName: 'YouTube Daily Report',
      description: 'Full report pipeline: generates per-channel summaries (Phase A, shared across users), then synthesizes personalized YouTube Daily Reports (Phase B). Default: daily 04:30 Beijing, before Daily Market Report 06:00.'
    },
    theme_report: {
      icon: <BarChartOutlined style={{ color: '#52c41a' }} />,
      params: [],
      defaultSchedule: '0 14 * * 1',
      defaultName: 'Theme Report',
      description: 'Pre-generate Theme Report static HTML for all users with Theme Report tagged themes (weekly Monday 2 PM CST)'
    },
    market_weekly_report: {
      icon: <LineChartOutlined style={{ color: '#fa8c16' }} />,
      params: [],
      defaultSchedule: '0 17 * * 5',
      defaultName: 'Market Weekly Report',
      description: 'Pre-generate Market Weekly Report static HTML for all users with Market Weekly tagged themes (weekly Friday 5 PM ET)'
    },
    daily_market_report: {
      icon: <LineChartOutlined style={{ color: '#13c2c2' }} />,
      params: [],
      defaultSchedule: '0 6 * * 1-6',
      defaultName: 'Daily Market Report',
      description: 'Pre-generate Daily Market Report (Temperature/My Subscription/Market Overview) for all users. Default: Mon-Sat 06:00 Beijing, after Twitter/YouTube/Substack/Zhishixingqiu summaries.'
    },
    the_close_summary: {
      icon: <StockOutlined style={{ color: '#722ed1' }} />,
      params: [],
      defaultSchedule: '40 8,22 * * 2-6',
      defaultName: 'Bloomberg The Close Report',
      description: 'Generate daily PM Brief from Bloomberg The Close (Tue-Sat 08:40 catch-up and 22:40 same-day, Beijing). Identifies episodes by title or opening bumper when YouTube drops the show suffix.'
    },
    cnbc_closing_bell: {
      icon: <StockOutlined style={{ color: '#1890ff' }} />,
      params: [],
      defaultSchedule: '0 7 * * 2-6',
      defaultName: 'CNBC Closing Bell Report',
      description: 'Generate daily Stock News summary from CNBC "Closing Bell" transcript (weekdays 7 AM Beijing / 7 PM ET)'
    },
    bloomberg_surveillance: {
      icon: <StockOutlined style={{ color: '#2f54eb' }} />,
      params: [],
      defaultSchedule: '30 0 * * 2-6',
      defaultName: 'Bloomberg Surveillance Report',
      description: 'AM Brief from Bloomberg Surveillance (live 6-9 AM ET); once daily Tue-Sat 00:30 Beijing after YouTube VOD (previous US session)'
    },
    fed_dashboard: {
      icon: <BankOutlined style={{ color: '#1d4ed8' }} />,
      params: ['mode'],
      defaultSchedule: '*/30 * * * *',
      defaultName: 'Fed Dashboard RSS Poll',
      description: 'Poll Fed press RSS for FOMC statements and minutes, then fetch, extract decisions, and summarize (every 30 minutes UTC)'
    },
    form13f: {
      icon: <FundViewOutlined style={{ color: '#b45309' }} />,
      params: ['mode'],
      defaultSchedule: '0 21 * * 1-5',
      defaultName: 'Form 13F Quarterly Poll',
      description: 'Poll 20 preset institution CIKs on EDGAR for 13F-HR filings, fold amendments, import holdings, and compute snapshots (weekdays 21:00 UTC)'
    },
    zhishixingqiu_summary: {
      icon: <StockOutlined style={{ color: '#52c41a' }} />,
      params: [],
      defaultSchedule: '30 5 * * *',
      defaultName: 'Zhishixingqiu Daily Summary',
      description: 'Generate daily aggregated summary from all Zhishixingqiu groups (default: 05:30 Beijing, before Daily Market Report 06:00)'
    },
    substack_summary: {
      icon: <ReadOutlined style={{ color: '#FF6719' }} />,
      params: [],
      defaultSchedule: '0 5 * * *',
      defaultName: 'Substack Daily Summary',
      description: 'Two-phase daily summary: per-publication LLM (articles + notes, cached) then per-user mechanical concat (default: 05:00 Beijing, before Daily Market Report 06:00)'
    },
  };

  const currentConfig = dataSourceConfigs[dataSourceType];
  const hasParamFields = currentConfig.params.length > 0;

  const loadPresets = async () => {
    try {
      console.log('Loading timezone configuration...');
      const timezonesResponse = await schedulerAPI.getTimezonePresets();
      console.log('Timezone API response:', timezonesResponse);
      
      if (timezonesResponse.success) {
        console.log('Setting timezone options:', timezonesResponse.data);
        setTimezoneOptions(timezonesResponse.data || []);
      } else {
        console.error('Timezone API returned failure:', timezonesResponse.message);
      }
    } catch (error) {
      console.error('Failed to load timezone configuration:', error);
    }
  };

  const resetModal = () => {
    form.resetFields();
    form.setFieldsValue({
      timezone:
        initialTimezone ||
        (dataSourceType === 'bloomberg_surveillance'
          ? 'Asia/Shanghai'
          : 'America/New_York'),
      cronExpression: initialCronExpression || currentConfig.defaultSchedule,
    });
  };

  useEffect(() => {
    if (open) {
      loadPresets();
      resetModal();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, initialCronExpression, initialTimezone, dataSourceType]);

  const handleSubmit = async () => {
    let data: Partial<ConfigFormData>;
    try {
      data = await form.validateFields();
    } catch (error) {
      console.error('表单验证失败:', error);
      return;
    }

    try {
      setLoading(true);
      
      // 显示详细的处理状态信息
      if (dataSourceType === 'eodhd_news') {
        message.loading('Creating EODHD News strategy, this may take up to 2 minutes...', 0);
      }
      
      let result;
      if (dataSourceType === 'eodhd_fundamentals') {
        // EODHD基本面数据手动更新逻辑（不需要cron表达式）
        const fundamentalsParams = {
          exchanges: data.taskParams?.exchanges || ['NYSE', 'NASDAQ'],
          data_types: data.taskParams?.data_types || ['General', 'Financials', 'Highlights', 'Valuation', 'Technicals', 'Holders', 'outstandingShares', 'Earnings', 'SharesStats', 'AnalystRatings', 'SplitsDividends', 'InsiderTransactions']
        };
        result = await schedulerAPI.executeFundamentalsUpdate(fundamentalsParams);
      } else {
        const params: CreateEODHDNewsTaskParams = {
          name: currentConfig.defaultName,
          cron_expression: data.cronExpression!,
          timezone: data.timezone!,
          enabled: true, // 新创建的任务默认启用，用户可通过主页面switch控制
          ...data.taskParams
        };

        if (dataSourceType === 'eodhd_news') {
          result = await schedulerAPI.createEODHDNewsTask(params);
        } else if (dataSourceType === 'watchlist_news') {
          // Watchlist新闻任务的专用创建逻辑
          const watchlistParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            ...data.taskParams  // ✅ 展开所有参数，包括 LLM 分析参数
          };
          result = await schedulerAPI.createWatchlistNewsTask(watchlistParams);
        } else if (dataSourceType === 'cctv_news') {
          // CCTV新闻任务的专用创建逻辑
          const cctvNewsParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            // 爬取参数
            date: data.taskParams?.date || undefined,
            max_news: data.taskParams?.max_news || undefined,
            // 🔥 LLM分析参数
            auto_analyze: data.taskParams?.auto_analyze !== undefined ? data.taskParams.auto_analyze : true,
            analysis_templates: data.taskParams?.analysis_templates || ['news_cctv_summary_v1', 'news_classification'],
            max_concurrent: data.taskParams?.max_concurrent || 5,
            enable_theme_matching: data.taskParams?.enable_theme_matching !== undefined ? data.taskParams.enable_theme_matching : true
          };
          result = await schedulerAPI.createCCTVNewsTask(cctvNewsParams);
        } else if (dataSourceType === 'finviz_screener') {
          // Finviz筛选数据任务的专用创建逻辑
          const finvizParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true
          };
          result = await schedulerAPI.createFinvizScreenerTask(finvizParams);
        } else if (dataSourceType === 'eodhd_stock_prices') {
          // EODHD股票价格任务的专用创建逻辑
          const stockPricesParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true
          };
          result = await schedulerAPI.createEODHDStockPricesTask(stockPricesParams);
        } else if (dataSourceType === 'eodhd_eod_stock_prices') {
          // EODHD历史股价任务的专用创建逻辑
          const eodStockPricesParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            days_back: data.taskParams?.days_back || 60
          };
          result = await schedulerAPI.createEODHDEODStockPricesTask(eodStockPricesParams);
        } else if (dataSourceType === 'futu_themes') {
          // Futu投资主题任务的专用创建逻辑
          const futuThemesParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            markets: data.taskParams?.markets,
            max_pages: data.taskParams?.max_pages,
            scrape_stocks: data.taskParams?.scrape_stocks !== undefined ? data.taskParams.scrape_stocks : true
          };
          result = await schedulerAPI.createFutuThemesTask(futuThemesParams);
        } else if (dataSourceType === 'market_data') {
          // 市场数据任务的专用创建逻辑
          const marketDataParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            asset_types: data.taskParams?.asset_types,
            fetch_realtime: data.taskParams?.fetch_realtime !== undefined ? data.taskParams.fetch_realtime : true,
            fetch_eod: data.taskParams?.fetch_eod !== undefined ? data.taskParams.fetch_eod : true,
            eod_days: data.taskParams?.eod_days || 5
          };
          result = await schedulerAPI.createMarketDataTask(marketDataParams);
        } else if (dataSourceType === 'earnings_calendar') {
          // 财报日历任务的专用创建逻辑
          const earningsCalendarParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            days: data.taskParams?.days || 7,
            fetch_next_week: data.taskParams?.fetch_next_week || false,
            start_date: data.taskParams?.start_date,
            end_date: data.taskParams?.end_date
          };
          result = await schedulerAPI.createEarningsCalendarTask(earningsCalendarParams);
        } else if (dataSourceType === 'ipo_calendar') {
          // IPO 日历任务的专用创建逻辑
          // 注意：EODHD API 不支持 exchanges 参数，已移除
          const ipoCalendarParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            days: data.taskParams?.days || 30,
            fetch_next_week: data.taskParams?.fetch_next_week || false,
            start_date: data.taskParams?.start_date,
            end_date: data.taskParams?.end_date
          };
          result = await schedulerAPI.createIPOCalendarTask(ipoCalendarParams);
        } else if (dataSourceType === 'news_feed') {
          const newsFeedParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            max_results: data.maxResults || 200,
            recent_days: data.recentDays || 1,
          };
          result = await schedulerAPI.createNewsFeedTask(newsFeedParams);
        } else if (dataSourceType === 'twitter_author_summary') {
          const authorSummaryParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            chain_daily_reports: true,
            force: data.taskParams?.force || false,
          };
          result = await schedulerAPI.createTwitterAuthorSummaryTask(authorSummaryParams);
        } else if (dataSourceType === 'youtube_author_summary') {
          const authorSummaryParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            chain_daily_reports: true,
            force: data.taskParams?.force || false,
          };
          result = await schedulerAPI.createYouTubeAuthorSummaryTask(authorSummaryParams);
        } else if (dataSourceType === 'theme_report') {
          const themeReportParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
          };
          result = await schedulerAPI.createThemeReportTask(themeReportParams);
        } else if (dataSourceType === 'market_weekly_report') {
          const marketWeeklyReportParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
          };
          result = await schedulerAPI.createMarketWeeklyReportTask(marketWeeklyReportParams);
        } else if (dataSourceType === 'daily_market_report') {
          const dailyMarketReportParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
          };
          result = await schedulerAPI.createDailyMarketReportTask(dailyMarketReportParams);
        } else if (dataSourceType === 'the_close_summary') {
          const theCloseSummaryParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
          };
          result = await schedulerAPI.createCronTask({
            task_type: 'the_close_summary',
            ...theCloseSummaryParams,
          });
        } else if (dataSourceType === 'cnbc_closing_bell') {
          const cnbcClosingBellParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
          };
          result = await schedulerAPI.createCronTask({
            task_type: 'cnbc_closing_bell',
            ...cnbcClosingBellParams,
          });
        } else if (dataSourceType === 'bloomberg_surveillance') {
          const bloombergSurveillanceParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
          };
          result = await schedulerAPI.createCronTask({
            task_type: 'bloomberg_surveillance',
            ...bloombergSurveillanceParams,
          });
        } else if (dataSourceType === 'fed_dashboard') {
          result = await schedulerAPI.createCronTask({
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            task_type: 'fed_dashboard',
            params: { mode: data.taskParams?.mode || 'poll' },
          });
        } else if (dataSourceType === 'form13f') {
          result = await schedulerAPI.createCronTask({
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            task_type: 'form13f',
            params: { mode: data.taskParams?.mode || 'poll' },
          });
        } else if (dataSourceType === 'zhishixingqiu_summary') {
          const zhishixingqiuSummaryParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
          };
          result = await schedulerAPI.createCronTask({
            task_type: 'zhishixingqiu_summary',
            ...zhishixingqiuSummaryParams,
          });
        } else if (dataSourceType === 'watchlist_fundamentals') {
          // Watchlist Fundamentals 任务的专用创建逻辑
          const watchlistFundamentalsParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            data_types: data.taskParams?.data_types,  // undefined 表示全部12种
            skip_recent_hours: data.taskParams?.skip_recent_hours || 24
          };
          result = await schedulerAPI.createWatchlistFundamentalsTask(watchlistFundamentalsParams);
        } else {
          // 其他任务类型的创建逻辑
          const taskType = dataSourceType === 'eodhd_events' ? 'economic_events' : 
                          dataSourceType === 'eodhd_macro' ? 'macro_indicators' :
                          dataSourceType === 'twitter_subscription' ? 'twitter_subscription' :
                          dataSourceType === 'youtube_subscription' ? 'youtube_subscription' :
                          dataSourceType === 'podcast_subscription' ? 'podcast_subscription' :
                          dataSourceType === 'zhishixingqiu_subscription' ? 'zhishixingqiu_subscription' :
                          dataSourceType === 'weibo_subscription' ? 'weibo_subscription' :
                          dataSourceType;
          
          result = await schedulerAPI.createCronTask({
            name: currentConfig.defaultName,
            task_type: taskType,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true, // 新创建的任务默认启用，用户可通过主页面switch控制
            params: data.taskParams
          });
        }
      }

      // 关闭loading消息
      message.destroy();

      if (result.success) {
        const successMessage = dataSourceType === 'eodhd_fundamentals' 
          ? 'Data update initiated successfully!' 
          : 'Data acquisition strategy configuration successful!';
        message.success(successMessage);
        onSuccess(result.data?.task_id || '');
        handleCancel();
      } else {
        message.error(result.message || (dataSourceType === 'eodhd_fundamentals' ? 'Data update failed' : 'Configuration failed'));
      }
    } catch (error: any) {
      console.error('Configuration failed:', error);
      
      // 关闭loading消息
      message.destroy();
      
      // 提供更详细的错误信息
      let errorMessage = 'Configuration failed';
      if (error.code === 'ECONNABORTED' || error.message?.includes('timeout')) {
        errorMessage = 'Request timeout. The strategy creation is taking longer than expected. Please check the task list in a moment to see if it was created successfully.';
      } else if (error.response?.status === 504) {
        errorMessage = 'Gateway timeout. The strategy creation is in progress but taking longer than expected. Please check the task list in a moment.';
      } else if (error.response?.data?.detail) {
        errorMessage = error.response.data.detail;
      } else if (error.message) {
        errorMessage = error.message;
      }
      
      message.error(errorMessage, 8); // 显示8秒
    } finally {
      setLoading(false);
    }
  };

  const handleCancel = () => {
    resetModal();
    onCancel();
  };

  const renderParamFields = () => {
      switch (dataSourceType) {
        case 'eodhd_news':
          return (
            <>
              <Form.Item
                label="Max Results"
                name={['taskParams', 'max_results']}
                extra="Maximum number of news items to fetch per time, up to 1000"
              >
                <InputNumber 
                  min={1} 
                  max={1000} 
                  placeholder="100" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Recent Days"
                name={['taskParams', 'recent_days']}
                extra="Limit news to recent N days from current date. Leave empty to fetch all available news."
              >
                <InputNumber 
                  min={1} 
                  max={365} 
                  placeholder="Leave empty for all news" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Stock Symbols"
                name={['taskParams', 'symbols']}
                extra="Specify stock symbols, separated by commas, leave empty to get all"
              >
                <Input placeholder="AAPL,MSFT,GOOGL" />
              </Form.Item>
              
              <Form.Item
                label="Topic Tags"
                name={['taskParams', 'topic']}
                extra="News topic classification"
              >
                <Select placeholder="Select topic" allowClear>
                  <Option value="technology">Technology</Option>
                  <Option value="finance">Finance</Option>
                  <Option value="economics">Economics</Option>
                  <Option value="earnings">Earnings</Option>
                </Select>
              </Form.Item>
              
              <Form.Item
                label="Keywords"
                name={['taskParams', 'keywords']}
                extra="News content keywords, separated by commas"
              >
                <Input placeholder="AI,blockchain,cryptocurrency" />
              </Form.Item>
              
              <Form.Item
                label="Enable Auto Analysis"
                name={['taskParams', 'auto_analyze']}
                valuePropName="checked"
                extra="Automatically analyze news with LLM immediately after fetching. Recommended for real-time news analysis."
                initialValue={true}
              >
                <Switch 
                  checkedChildren="Enabled" 
                  unCheckedChildren="Disabled" 
                />
              </Form.Item>
              
              <Form.Item
                label="Use Celery (Async Processing)"
                name={['taskParams', 'use_celery']}
                valuePropName="checked"
                extra="Use Celery for async processing (STRONGLY RECOMMENDED for multi-user cloud applications to avoid blocking)"
                initialValue={true}
              >
                <Switch 
                  checkedChildren="Celery" 
                  unCheckedChildren="Direct" 
                />
              </Form.Item>
              
              <Form.Item
                label="Analysis Templates (LLM)"
                name={['taskParams', 'analysis_templates']}
                extra="Select one or more LLM prompt templates for text analysis (multiple templates will run in parallel)"
                initialValue={["news_summary_v1", "news_classification"]}
              >
                <Select 
                  mode="multiple"
                  placeholder="Select one or more analysis templates"
                  maxTagCount="responsive"
                >
                  <Option value="news_summary_v1">News Summary v1 (Default)</Option>
                  <Option value="news_sentiment_analysis">News Sentiment Analysis</Option>
                  <Option value="news_classification">News Classification</Option>
                </Select>
              </Form.Item>
              
              <Form.Item
                label="Analysis Batch Size"
                name={['taskParams', 'analysis_batch_size']}
                extra="Maximum number of news items to analyze per batch. Leave empty to analyze all new items."
                initialValue={1000}
              >
                <InputNumber 
                  min={1} 
                  max={5000} 
                  placeholder="1000" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Max Concurrent"
                name={['taskParams', 'max_concurrent']}
                extra="Number of concurrent LLM API calls (1-10). Higher values = faster but more API load."
                initialValue={8}
              >
                <InputNumber 
                  min={1} 
                  max={10} 
                  placeholder="8" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <div style={{ 
                borderTop: `1px solid ${colors.borderSecondary}`, 
                paddingTop: 24, 
                marginTop: 24, 
                marginBottom: 16 
              }}>
                <h4 style={{ marginBottom: 16, color: colors.headingPrimary }}>
                  🎯 Investment Theme Matching (Vector Search)
                </h4>
              </div>
              
              <Form.Item
                label="Enable Theme Matching"
                name={['taskParams', 'enable_theme_matching']}
                valuePropName="checked"
                extra="Automatically match news to 150+ investment themes using vector similarity search"
                initialValue={true}
              >
                <Switch 
                  checkedChildren="✓ Enabled" 
                  unCheckedChildren="✗ Disabled"
                />
              </Form.Item>
            </>
          );
          
        case 'watchlist_news':
          return (
            <>
              <Form.Item
                label="Max Results"
                name={['taskParams', 'max_results']}
                extra="Maximum number of news items to fetch per time, up to 1000"
              >
                <InputNumber 
                  min={1} 
                  max={1000} 
                  placeholder="100" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Recent Days"
                name={['taskParams', 'recent_days']}
                extra="Limit news to recent N days from current date. Leave empty to fetch all available news."
              >
                <InputNumber 
                  min={1} 
                  max={365} 
                  placeholder="Leave empty for all news" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Enable Auto Analysis"
                name={['taskParams', 'auto_analyze']}
                valuePropName="checked"
                extra="Automatically analyze news with LLM immediately after fetching. Recommended for real-time news analysis."
                initialValue={true}
              >
                <Switch 
                  checkedChildren="Enabled" 
                  unCheckedChildren="Disabled" 
                />
              </Form.Item>
              
              <Form.Item
                label="Use Celery (Async Processing)"
                name={['taskParams', 'use_celery']}
                valuePropName="checked"
                extra="Use Celery for async processing (STRONGLY RECOMMENDED for multi-user cloud applications to avoid blocking)"
                initialValue={true}
              >
                <Switch 
                  checkedChildren="Celery" 
                  unCheckedChildren="Direct" 
                />
              </Form.Item>
              
              <Form.Item
                label="Analysis Templates (LLM)"
                name={['taskParams', 'analysis_templates']}
                extra="Select one or more LLM prompt templates for text analysis (multiple templates will run in parallel)"
                initialValue={["news_summary_v1", "news_classification"]}
              >
                <Select 
                  mode="multiple"
                  placeholder="Select one or more analysis templates"
                  maxTagCount="responsive"
                >
                  <Option value="news_summary_v1">News Summary v1 (Default)</Option>
                  <Option value="news_sentiment_analysis">News Sentiment Analysis</Option>
                  <Option value="news_classification">News Classification</Option>
                </Select>
              </Form.Item>
              
              <Form.Item
                label="Analysis Batch Size"
                name={['taskParams', 'analysis_batch_size']}
                extra="Maximum number of news items to analyze per batch. Leave empty to analyze all new items."
                initialValue={1000}
              >
                <InputNumber 
                  min={1} 
                  max={5000} 
                  placeholder="1000" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Max Concurrent"
                name={['taskParams', 'max_concurrent']}
                extra="Number of concurrent LLM API calls (1-10). Higher values = faster but more API load."
                initialValue={8}
              >
                <InputNumber 
                  min={1} 
                  max={10} 
                  placeholder="8" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <div style={{ 
                borderTop: `1px solid ${colors.borderSecondary}`, 
                paddingTop: 24, 
                marginTop: 24, 
                marginBottom: 16 
              }}>
                <h4 style={{ marginBottom: 16, color: colors.headingPrimary }}>
                  🎯 Investment Theme Matching (Vector Search)
                </h4>
              </div>
              
              <Form.Item
                label="Enable Theme Matching"
                name={['taskParams', 'enable_theme_matching']}
                valuePropName="checked"
                extra="Automatically match news to 150+ investment themes using vector similarity search"
                initialValue={true}
              >
                <Switch 
                  checkedChildren="✓ Enabled" 
                  unCheckedChildren="✗ Disabled"
                />
              </Form.Item>
            </>
          );
          
        case 'cctv_news':
          return (
            <>
              <Divider orientation="left">Scraping Parameters (爬取参数)</Divider>
              
              <Form.Item
                label="Date (Optional)"
                name={['taskParams', 'date']}
                extra="Specify date (format: YYYYMMDD). Leave empty to scrape latest news."
              >
                <Input 
                  placeholder="e.g., 20251124 (leave empty for latest)" 
                  maxLength={8}
                />
              </Form.Item>
              
              <Form.Item
                label="Max News (Optional)"
                name={['taskParams', 'max_news']}
                extra="Maximum number of news items to scrape (1-100). Leave empty to scrape all."
              >
                <InputNumber 
                  min={1} 
                  max={100} 
                  placeholder="Leave empty for all" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Divider orientation="left">LLM Analysis Parameters (分析参数)</Divider>
              
              <Form.Item
                label="Auto Analyze"
                name={['taskParams', 'auto_analyze']}
                valuePropName="checked"
                initialValue={true}
                extra="Automatically trigger LLM analysis after scraping new news."
              >
                <Switch defaultChecked />
              </Form.Item>
              
              <Form.Item
                label="Analysis Templates"
                name={['taskParams', 'analysis_templates']}
                initialValue={['news_cctv_summary_v1', 'news_classification']}
                extra="Select multiple templates for parallel analysis. Default: CCTV Summary + Classification."
              >
                <Select
                  mode="multiple"
                  placeholder="Select analysis templates"
                  options={[
                    { label: 'CCTV Summary (央视新闻摘要)', value: 'news_cctv_summary_v1' },
                    { label: 'News Classification (新闻分类)', value: 'news_classification' },
                    { label: 'News Summary (通用摘要)', value: 'news_summary_v1' },
                  ]}
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Max Concurrent"
                name={['taskParams', 'max_concurrent']}
                initialValue={5}
                extra="Maximum concurrent LLM analysis (1-20). Recommended: 5."
              >
                <InputNumber 
                  min={1} 
                  max={20} 
                  placeholder="5" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Enable Theme Matching"
                name={['taskParams', 'enable_theme_matching']}
                valuePropName="checked"
                initialValue={true}
                extra="Automatically match news to investment themes after analysis."
              >
                <Switch defaultChecked />
              </Form.Item>
            </>
          );
          
        case 'twitter_subscription':
          return null;
        
        case 'twitter_author_summary':
        case 'youtube_author_summary':
          return (
            <>
              <Form.Item
                label={
                  <span>
                    Force Regenerate&nbsp;
                    <Tooltip title="When enabled, all cached author/channel summaries and daily reports will be regenerated from scratch, ignoring existing cached data. Useful after changing LLM model or prompt template.">
                      <QuestionCircleOutlined />
                    </Tooltip>
                  </span>
                }
                name={['taskParams', 'force']}
                valuePropName="checked"
                initialValue={false}
                style={{ marginTop: 16 }}
              >
                <Switch />
              </Form.Item>
            </>
          );

        case 'youtube_subscription':
          return null;

        case 'podcast_subscription':
          return null;

        case 'zhishixingqiu_subscription':
          return null;

        case 'substack_subscription':
          return null;

        case 'weibo_subscription':
          return null;

        case 'news_feed':
          return (
            <>

              <Row gutter={16}>
                <Col span={12}>
                  <Form.Item
                    name="maxResults"
                    label={
                      <Space>
                        Max Results
                        <Tooltip title="Maximum number of articles to fetch per run from EODHD API">
                          <QuestionCircleOutlined style={{ color: '#8c8c8c' }} />
                        </Tooltip>
                      </Space>
                    }
                    initialValue={200}
                  >
                    <InputNumber min={1} max={1000} style={{ width: '100%' }} />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item
                    name="recentDays"
                    label={
                      <Space>
                        Recent Days
                        <Tooltip title="Only fetch news published within the last N days">
                          <QuestionCircleOutlined style={{ color: '#8c8c8c' }} />
                        </Tooltip>
                      </Space>
                    }
                    initialValue={1}
                  >
                    <InputNumber min={1} max={7} style={{ width: '100%' }} />
                  </Form.Item>
                </Col>
              </Row>
            </>
          );

        case 'eodhd_events':
          return (
            <>
              
              <Form.Item
                label="Fetch Mode"
                name={['taskParams', 'fetch_next_week']}
                valuePropName="checked"
                extra="Enable to fetch next week's data specifically, or disable to use custom settings."
                initialValue={false}
              >
                <Switch 
                  checkedChildren="Next Week" 
                  unCheckedChildren="Custom Period" 
                />
              </Form.Item>
              
              <Form.Item
                label="Days Ahead"
                name={['taskParams', 'days']}
                extra="Number of days ahead to fetch (1-90 days). Default is 30 days."
                initialValue={30}
              >
                <InputNumber
                  min={1}
                  max={90}
                  placeholder="30"
                  style={{ width: '100%' }}
                />
              </Form.Item>

              <Form.Item
                label="Lookback Days"
                name={['taskParams', 'lookback_days']}
                extra="Days to look back to backfill actual values of published events (0-90). Default is 14. Ignored when explicit start/end dates are set."
                initialValue={14}
              >
                <InputNumber
                  min={0}
                  max={90}
                  placeholder="14"
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Start Date"
                name={['taskParams', 'start_date']}
                extra="Optional: Specific start date (YYYY-MM-DD format)"
              >
                <Input placeholder="e.g., 2025-11-10" />
              </Form.Item>
              
              <Form.Item
                label="End Date"
                name={['taskParams', 'end_date']}
                extra="Optional: Specific end date (YYYY-MM-DD format)"
              >
                <Input placeholder="e.g., 2025-11-17" />
              </Form.Item>
              
              <Form.Item
                label="Country Filter"
                name={['taskParams', 'country']}
                extra="Optional: Select one or more countries. Leave empty for all countries."
              >
                <Select 
                  mode="multiple"
                  placeholder="All countries" 
                  allowClear
                  showSearch
                  maxTagCount="responsive"
                  optionLabelProp="label"
                  filterOption={(input, option) => {
                    const label = String(option?.label || '');
                    return label.toLowerCase().includes(input.toLowerCase());
                  }}
                >
                  <Option value="US" label="🇺🇸 United States">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇺🇸</span>
                      <span>United States (US)</span>
                    </div>
                  </Option>
                  <Option value="CN" label="🇨🇳 China">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇳</span>
                      <span>China (CN)</span>
                    </div>
                  </Option>
                  <Option value="JP" label="🇯🇵 Japan">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇯🇵</span>
                      <span>Japan (JP)</span>
                    </div>
                  </Option>
                  <Option value="GB" label="🇬🇧 United Kingdom">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇬🇧</span>
                      <span>United Kingdom (GB)</span>
                    </div>
                  </Option>
                  <Option value="DE" label="🇩🇪 Germany">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇩🇪</span>
                      <span>Germany (DE)</span>
                    </div>
                  </Option>
                  <Option value="FR" label="🇫🇷 France">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇫🇷</span>
                      <span>France (FR)</span>
                    </div>
                  </Option>
                  <Option value="CA" label="🇨🇦 Canada">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇦</span>
                      <span>Canada (CA)</span>
                    </div>
                  </Option>
                  <Option value="AU" label="🇦🇺 Australia">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇦🇺</span>
                      <span>Australia (AU)</span>
                    </div>
                  </Option>
                  <Option value="KR" label="🇰🇷 South Korea">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇰🇷</span>
                      <span>South Korea (KR)</span>
                    </div>
                  </Option>
                  <Option value="IN" label="🇮🇳 India">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇳</span>
                      <span>India (IN)</span>
                    </div>
                  </Option>
                  <Option value="HK" label="🇭🇰 Hong Kong">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇭🇰</span>
                      <span>Hong Kong (HK)</span>
                    </div>
                  </Option>
                  <Option value="SG" label="🇸🇬 Singapore">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇸🇬</span>
                      <span>Singapore (SG)</span>
                    </div>
                  </Option>
                  <Option value="IT" label="🇮🇹 Italy">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇹</span>
                      <span>Italy (IT)</span>
                    </div>
                  </Option>
                  <Option value="ES" label="🇪🇸 Spain">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇪🇸</span>
                      <span>Spain (ES)</span>
                    </div>
                  </Option>
                  <Option value="BR" label="🇧🇷 Brazil">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇧🇷</span>
                      <span>Brazil (BR)</span>
                    </div>
                  </Option>
                  <Option value="RU" label="🇷🇺 Russia">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇷🇺</span>
                      <span>Russia (RU)</span>
                    </div>
                  </Option>
                  <Option value="MX" label="🇲🇽 Mexico">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇲🇽</span>
                      <span>Mexico (MX)</span>
                    </div>
                  </Option>
                  <Option value="NL" label="🇳🇱 Netherlands">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇳🇱</span>
                      <span>Netherlands (NL)</span>
                    </div>
                  </Option>
                  <Option value="CH" label="🇨🇭 Switzerland">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇭</span>
                      <span>Switzerland (CH)</span>
                    </div>
                  </Option>
                </Select>
              </Form.Item>
            </>
          );
          
        case 'eodhd_macro':
          return (
            <>
              <Form.Item
                label="Country"
                name={['taskParams', 'country']}
                rules={[{ required: true, message: 'Please select a country' }]}
                extra="Specify which countries to get macro indicators for, multiple selection allowed"
              >
                <Select 
                  placeholder="Select country" 
                  mode="multiple"
                  showSearch
                  optionLabelProp="label"
                  filterOption={(input, option) => {
                    const label = String(option?.label || '');
                    return label.toLowerCase().includes(input.toLowerCase());
                  }}
                >
                  <Option value="USA" label="🇺🇸 United States (USA)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇺🇸</span>
                      <span>United States (USA)</span>
                    </div>
                  </Option>
                  <Option value="CHN" label="🇨🇳 China (CHN)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇳</span>
                      <span>China (CHN)</span>
                    </div>
                  </Option>
                  <Option value="JPN" label="🇯🇵 Japan (JPN)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇯🇵</span>
                      <span>Japan (JPN)</span>
                    </div>
                  </Option>
                  <Option value="DEU" label="��🇪 Germany (DEU)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇩🇪</span>
                      <span>Germany (DEU)</span>
                    </div>
                  </Option>
                  <Option value="GBR" label="🇬🇧 United Kingdom (GBR)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇬🇧</span>
                      <span>United Kingdom (GBR)</span>
                    </div>
                  </Option>
                  <Option value="FRA" label="🇫🇷 France (FRA)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇫🇷</span>
                      <span>France (FRA)</span>
                    </div>
                  </Option>
                  <Option value="ITA" label="🇮🇹 Italy (ITA)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇹</span>
                      <span>Italy (ITA)</span>
                    </div>
                  </Option>
                  <Option value="BRA" label="🇧🇷 Brazil (BRA)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇧🇷</span>
                      <span>Brazil (BRA)</span>
                    </div>
                  </Option>
                  <Option value="CAN" label="🇨🇦 Canada (CAN)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇦</span>
                      <span>Canada (CAN)</span>
                    </div>
                  </Option>
                  <Option value="RUS" label="🇷🇺 Russia (RUS)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇷🇺</span>
                      <span>Russia (RUS)</span>
                    </div>
                  </Option>
                  <Option value="IND" label="🇮🇳 India (IND)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇳</span>
                      <span>India (IND)</span>
                    </div>
                  </Option>
                  <Option value="KOR" label="🇰🇷 South Korea (KOR)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇰🇷</span>
                      <span>South Korea (KOR)</span>
                    </div>
                  </Option>
                  <Option value="ESP" label="🇪🇸 Spain (ESP)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇪🇸</span>
                      <span>Spain (ESP)</span>
                    </div>
                  </Option>
                  <Option value="AUS" label="🇦🇺 Australia (AUS)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇦🇺</span>
                      <span>Australia (AUS)</span>
                    </div>
                  </Option>
                  <Option value="MEX" label="🇲🇽 Mexico (MEX)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇲🇽</span>
                      <span>Mexico (MEX)</span>
                    </div>
                  </Option>
                  <Option value="IDN" label="🇮🇩 Indonesia (IDN)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇩</span>
                      <span>Indonesia (IDN)</span>
                    </div>
                  </Option>
                  <Option value="NLD" label="🇳🇱 Netherlands (NLD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇳🇱</span>
                      <span>Netherlands (NLD)</span>
                    </div>
                  </Option>
                  <Option value="SAU" label="🇸🇦 Saudi Arabia (SAU)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇸🇦</span>
                      <span>Saudi Arabia (SAU)</span>
                    </div>
                  </Option>
                  <Option value="TUR" label="🇹🇷 Turkey (TUR)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇹🇷</span>
                      <span>Turkey (TUR)</span>
                    </div>
                  </Option>
                  <Option value="TWN" label="🇹🇼 Taiwan (TWN)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇹🇼</span>
                      <span>Taiwan (TWN)</span>
                    </div>
                  </Option>
                </Select>
              </Form.Item>
              
              <Form.Item
                label="Indicator Type"
                name={['taskParams', 'indicator']}
                extra="Specify macro indicator types, multiple selection allowed, leave empty to get all indicators"
              >
                <Select 
                  placeholder="Select indicator type" 
                  mode="multiple"
                  allowClear
                  showSearch
                  optionLabelProp="label"
                  filterOption={(input, option) => {
                    const label = String(option?.label || '');
                    return label.toLowerCase().includes(input.toLowerCase());
                  }}
                >
                  <Option value="real_interest_rate" label="📊 Real Interest Rate (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📊</span>
                      <span>Real Interest Rate (%)</span>
                    </div>
                  </Option>
                  <Option value="population_total" label="�� Total Population">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👥</span>
                      <span>Total Population</span>
                    </div>
                  </Option>
                  <Option value="population_growth_annual" label="📈 Annual Population Growth (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📈</span>
                      <span>Annual Population Growth (%)</span>
                    </div>
                  </Option>
                  <Option value="inflation_consumer_prices_annual" label="💰 Annual Consumer Price Inflation (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💰</span>
                      <span>Annual Consumer Price Inflation (%)</span>
                    </div>
                  </Option>
                  <Option value="consumer_price_index" label="📊 Consumer Price Index (2010=100)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📊</span>
                      <span>Consumer Price Index (2010=100)</span>
                    </div>
                  </Option>
                  <Option value="gdp_current_usd" label="💵 GDP (Current Price USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💵</span>
                      <span>GDP (Current Price USD)</span>
                    </div>
                  </Option>
                  <Option value="gdp_per_capita_usd" label="👤 Per Capita GDP (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👤</span>
                      <span>Per Capita GDP (USD)</span>
                    </div>
                  </Option>
                  <Option value="gdp_growth_annual" label="📈 Annual GDP Growth (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📈</span>
                      <span>Annual GDP Growth (%)</span>
                    </div>
                  </Option>
                  <Option value="debt_percent_gdp" label="💳 Debt to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💳</span>
                      <span>Debt to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="net_trades_goods_services" label="🔄 Net Trade in Goods and Services (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🔄</span>
                      <span>Net Trade in Goods and Services (USD)</span>
                    </div>
                  </Option>
                  <Option value="inflation_gdp_deflator_annual" label="📊 Annual GDP Deflator Inflation (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📊</span>
                      <span>Annual GDP Deflator Inflation (%)</span>
                    </div>
                  </Option>
                  <Option value="agriculture_value_added_percent_gdp" label="🌾 Agriculture Value Added to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🌾</span>
                      <span>Agriculture Value Added to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="industry_value_added_percent_gdp" label="🏭 Industry Value Added to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🏭</span>
                      <span>Industry Value Added to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="services_value_added_percent_gdp" label="💼 Services Value Added to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💼</span>
                      <span>Services Value Added to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="exports_of_goods_services_percent_gdp" label="📤 Export of Goods and Services to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📤</span>
                      <span>Export of Goods and Services to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="imports_of_goods_services_percent_gdp" label="📥 Import of Goods and Services to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📥</span>
                      <span>Import of Goods and Services to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="gross_capital_formation_percent_gdp" label="🏗️ Gross Capital Formation to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🏗️</span>
                      <span>Gross Capital Formation to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="net_migration" label="🚶 Net Migration">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🚶</span>
                      <span>Net Migration</span>
                    </div>
                  </Option>
                  <Option value="gni_usd" label="💵 Gross National Income (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💵</span>
                      <span>Gross National Income (USD)</span>
                    </div>
                  </Option>
                  <Option value="gni_per_capita_usd" label="👤 Per Capita Gross National Income (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👤</span>
                      <span>Per Capita Gross National Income (USD)</span>
                    </div>
                  </Option>
                  <Option value="gni_ppp_usd" label="💱 Purchasing Power Parity Gross National Income (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💱</span>
                      <span>Purchasing Power Parity Gross National Income (USD)</span>
                    </div>
                  </Option>
                  <Option value="gni_per_capita_ppp_usd" label="👤 Per Capita Purchasing Power Parity Gross National Income (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👤</span>
                      <span>Per Capita Purchasing Power Parity Gross National Income (USD)</span>
                    </div>
                  </Option>
                  <Option value="income_share_lowest_twenty" label="📊 Income Share of Lowest 20% (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📊</span>
                      <span>Income Share of Lowest 20% (%)</span>
                    </div>
                  </Option>
                  <Option value="life_expectancy" label="👴 Life Expectancy (Years)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👴</span>
                      <span>Life Expectancy (Years)</span>
                    </div>
                  </Option>
                  <Option value="fertility_rate" label="👶 Fertility Rate (Births per Woman)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👶</span>
                      <span>Fertility Rate (Births per Woman)</span>
                    </div>
                  </Option>
                  <Option value="prevalence_hiv_total" label="🩺 HIV Prevalence (15-49 Years %)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🩺</span>
                      <span>HIV Prevalence (15-49 Years %)</span>
                    </div>
                  </Option>
                  <Option value="co2_emissions_tons_per_capita" label="🌍 CO2 Emissions per Capita (Tons)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🌍</span>
                      <span>CO2 Emissions per Capita (Tons)</span>
                    </div>
                  </Option>
                  <Option value="surface_area_km" label="🗺️ Surface Area (Square Kilometers)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🗺️</span>
                      <span>Surface Area (Square Kilometers)</span>
                    </div>
                  </Option>
                  <Option value="poverty_poverty_lines_percent_population" label="🏚️ Poverty Line Population Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🏚️</span>
                      <span>Poverty Line Population Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="revenue_excluding_grants_percent_gdp" label="💰 Revenue to GDP Ratio (Excluding Grants %)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💰</span>
                      <span>Revenue to GDP Ratio (Excluding Grants %)</span>
                    </div>
                  </Option>
                  <Option value="cash_surplus_deficit_percent_gdp" label="💸 Cash Surplus/Deficit to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💸</span>
                      <span>Cash Surplus/Deficit to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="startup_procedures_register" label="🏢 Number of Startup Procedures">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🏢</span>
                      <span>Number of Startup Procedures</span>
                    </div>
                  </Option>
                  <Option value="market_cap_domestic_companies_percent_gdp" label="📈 Domestic Listed Companies Market Cap to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📈</span>
                      <span>Domestic Listed Companies Market Cap to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="mobile_subscriptions_per_hundred" label="📱 Mobile Phone Subscriptions per 100 People">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📱</span>
                      <span>Mobile Phone Subscriptions per 100 People</span>
                    </div>
                  </Option>
                  <Option value="internet_users_per_hundred" label="🌐 Internet Users per 100 People">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🌐</span>
                      <span>Internet Users per 100 People</span>
                    </div>
                  </Option>
                  <Option value="high_technology_exports_percent_total" label="🔬 High-Tech Exports to Manufactured Goods Exports Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🔬</span>
                      <span>High-Tech Exports to Manufactured Goods Exports Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="merchandise_trade_percent_gdp" label="📦 Merchandise Trade to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📦</span>
                      <span>Merchandise Trade to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="total_debt_service_percent_gni" label="�� Total Debt Service to GNI Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💳</span>
                      <span>Total Debt Service to GNI Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="unemployment_total_percent" label="👷 Total Unemployment Rate (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👷</span>
                      <span>Total Unemployment Rate (%)</span>
                    </div>
                  </Option>
                </Select>
              </Form.Item>
            </>
          );
          
        case 'eodhd_eod_stock_prices':
          return (
            <>
              <Form.Item
                label="Days Back"
                name={['taskParams', 'days_back']}
                extra="Number of days to look back for historical price data (1-365 days)"
                initialValue={60}
              >
                <InputNumber 
                  min={1} 
                  max={365} 
                  placeholder="60" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
            </>
          );
          
        case 'futu_themes':
          return (
            <>
              
              <Form.Item
                label="Markets"
                name={['taskParams', 'markets']}
                extra="Select markets to scrape. Leave empty to scrape all available markets (HK, US)."
              >
                <Select 
                  mode="multiple"
                  placeholder="Select markets (leave empty for all)"
                  allowClear
                >
                  <Option value="HK">🇭🇰 Hong Kong (HK)</Option>
                  <Option value="US">🇺🇸 United States (US)</Option>
                </Select>
              </Form.Item>
              
              <Form.Item
                label="Max Pages Per Market"
                name={['taskParams', 'max_pages']}
                extra="Maximum number of pages to scrape per market (1-100). Leave empty to scrape all pages. ⚡ Optimized for faster scraping."
              >
                <InputNumber 
                  min={1} 
                  max={100} 
                  placeholder="Leave empty for all pages" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Scrape Stocks"
                name={['taskParams', 'scrape_stocks']}
                valuePropName="checked"
                extra="Enable to scrape detailed stock list for each theme. Disable to only fetch theme metadata."
                initialValue={true}
              >
                <Switch 
                  checkedChildren="Enabled" 
                  unCheckedChildren="Disabled" 
                />
              </Form.Item>
            </>
          );
          
        case 'market_data':
          return (
            <>
              
              <Form.Item
                label="Asset Types"
                name={['taskParams', 'asset_types']}
                extra="Select asset types to fetch. Leave empty to fetch all types (recommended)."
              >
                <Select 
                  mode="multiple"
                  placeholder="Select asset types (leave empty for all)"
                  allowClear
                >
                  <Option value="index">📈 Indices (8 assets: S&P 500, Nasdaq, Dow Jones, etc.)</Option>
                  <Option value="forex">💱 Forex (4 pairs: USD/CNH, EUR/USD, GBP/USD, USD/JPY)</Option>
                  <Option value="commodity">🛢️ Commodities (5 assets: Gold, Silver, Copper, Oil, Natural Gas)</Option>
                  <Option value="bond">📊 Bond Yields (4 types: 3M, 1Y, 5Y, 10Y Treasury)</Option>
                  <Option value="sector">🏢 Sectors (11 sectors: Tech, Finance, Healthcare, etc.)</Option>
                </Select>
              </Form.Item>
              
              <Form.Item
                label="Fetch Realtime Data"
                name={['taskParams', 'fetch_realtime']}
                valuePropName="checked"
                extra="Enable to fetch current market prices and changes (recommended)."
                initialValue={true}
              >
                <Switch 
                  checkedChildren="Enabled" 
                  unCheckedChildren="Disabled" 
                />
              </Form.Item>
              
              <Form.Item
                label="Fetch EOD Historical Data"
                name={['taskParams', 'fetch_eod']}
                valuePropName="checked"
                extra="Enable to fetch end-of-day historical OHLC data (recommended)."
                initialValue={true}
              >
                <Switch 
                  checkedChildren="Enabled" 
                  unCheckedChildren="Disabled" 
                />
              </Form.Item>
              
              <Form.Item
                label="EOD Days"
                name={['taskParams', 'eod_days']}
                extra="Number of recent days to fetch EOD data (1-120 days)."
                initialValue={5}
              >
                <InputNumber 
                  min={1} 
                  max={120} 
                  placeholder="5" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
            </>
          );

        case 'earnings_calendar':
          return (
            <>
              
              <Form.Item
                label="Fetch Mode"
                name={['taskParams', 'fetch_next_week']}
                valuePropName="checked"
                extra="Enable to fetch next week's data specifically, or disable to use custom settings."
                initialValue={false}
              >
                <Switch 
                  checkedChildren="Next Week" 
                  unCheckedChildren="Custom Period" 
                />
              </Form.Item>
              
              <Form.Item
                label="Days Ahead"
                name={['taskParams', 'days']}
                extra="Number of days ahead to fetch (1-90 days). Default is 7 days."
                initialValue={7}
              >
                <InputNumber 
                  min={1} 
                  max={90} 
                  placeholder="7" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Start Date"
                name={['taskParams', 'start_date']}
                extra="Optional: Specific start date (YYYY-MM-DD format)"
              >
                <Input placeholder="e.g., 2025-11-10" />
              </Form.Item>
              
              <Form.Item
                label="End Date"
                name={['taskParams', 'end_date']}
                extra="Optional: Specific end date (YYYY-MM-DD format)"
              >
                <Input placeholder="e.g., 2025-11-17" />
              </Form.Item>
            </>
          );

        case 'ipo_calendar':
          return (
            <>
              
              <Form.Item
                label="Fetch Mode"
                name={['taskParams', 'fetch_next_week']}
                valuePropName="checked"
                extra="Enable to fetch next week's data specifically, or disable to use custom settings."
                initialValue={false}
              >
                <Switch 
                  checkedChildren="Next Week" 
                  unCheckedChildren="Custom Period" 
                />
              </Form.Item>
              
              <Form.Item
                label="Days Ahead"
                name={['taskParams', 'days']}
                extra="Number of days ahead to fetch (1-365 days). Default is 30 days."
                initialValue={30}
              >
                <InputNumber 
                  min={1} 
                  max={365} 
                  placeholder="30" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Start Date"
                name={['taskParams', 'start_date']}
                extra="Optional: Specific start date (YYYY-MM-DD format)"
              >
                <Input placeholder="e.g., 2025-11-10" />
              </Form.Item>
              
              <Form.Item
                label="End Date"
                name={['taskParams', 'end_date']}
                extra="Optional: Specific end date (YYYY-MM-DD format)"
              >
                <Input placeholder="e.g., 2025-12-10" />
              </Form.Item>
            </>
          );

        case 'watchlist_fundamentals':
          return (
            <>
              
              <Form.Item
                label="Data Types to Update"
                name={['taskParams', 'data_types']}
                extra="Select which data types to update. Leave empty to update all 12 types (General, Financials, Highlights, Valuation, Technicals, Holders, outstandingShares, Earnings, SharesStats, AnalystRatings, SplitsDividends, InsiderTransactions)."
              >
                <Select 
                  mode="multiple"
                  placeholder="Leave empty for all 12 data types"
                  allowClear
                  style={{ width: '100%' }}
                >
                  <Option value="General">📊 General (Company info, address, officers)</Option>
                  <Option value="Financials">💰 Financials (Balance sheet, income statement, cash flow)</Option>
                  <Option value="Highlights">⭐ Highlights (Key metrics: market cap, P/E, earnings)</Option>
                  <Option value="Valuation">📈 Valuation (P/E ratios, enterprise value)</Option>
                  <Option value="Technicals">📊 Technicals (Beta, 52-week range, MA)</Option>
                  <Option value="Holders">👥 Holders (Institutional holdings, ownership)</Option>
                  <Option value="outstandingShares">📈 Outstanding Shares (Shares data)</Option>
                  <Option value="Earnings">💸 Earnings (Annual earnings, estimates)</Option>
                  <Option value="SharesStats">📊 Shares Stats (Float, short interest)</Option>
                  <Option value="AnalystRatings">🎯 Analyst Ratings (Buy/sell, target price)</Option>
                  <Option value="SplitsDividends">💰 Splits & Dividends (Dividend, splits)</Option>
                  <Option value="InsiderTransactions">🔐 Insider Transactions (Executive trades)</Option>
                </Select>
              </Form.Item>

              <Form.Item
                label="Skip Recent Hours"
                name={['taskParams', 'skip_recent_hours']}
                extra="Skip stocks updated within the last N hours to avoid redundant API calls. Default is 24 hours."
                initialValue={24}
              >
                <InputNumber 
                  min={1} 
                  max={168} 
                  placeholder="24" 
                  style={{ width: '100%' }}
                  addonAfter="hours"
                />
              </Form.Item>
            </>
          );
          
        case 'eodhd_fundamentals':
          return (
            <>
              <Form.Item
                label="Exchanges"
                name={['taskParams', 'exchanges']}
                rules={[{ required: true, message: 'Please select at least one exchange' }]}
                extra="Select exchanges to fetch fundamentals data from (Supports US, China, Hong Kong, Taiwan, Japan, UK markets)"
              >
                <Select 
                  placeholder="Select exchanges" 
                  mode="multiple"
                  showSearch
                  optionLabelProp="label"
                  filterOption={(input, option) => {
                    const label = String(option?.label || '');
                    return label.toLowerCase().includes(input.toLowerCase());
                  }}
                >
                  {/* 根据数据源类型显示不同的交易所选项 */}
                  {dataSourceType === 'eodhd_fundamentals' ? (
                    <>
                      {/* EODHD Fundamentals 支持多个国家和地区的交易所 */}
                      <Option value="NYSE" label="🏛️ New York Stock Exchange (NYSE)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏛️</span>
                          <span>New York Stock Exchange (NYSE)</span>
                        </div>
                      </Option>
                      <Option value="NASDAQ" label="💻 NASDAQ">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>💻</span>
                          <span>NASDAQ Global Market</span>
                        </div>
                      </Option>
                      <Option value="AMEX" label="🏦 American Stock Exchange (AMEX)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏦</span>
                          <span>American Stock Exchange (AMEX)</span>
                        </div>
                      </Option>
                      <Option value="SHG" label="🏢 Shanghai Stock Exchange (SHG)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏢</span>
                          <span>Shanghai Stock Exchange (SHG)</span>
                        </div>
                      </Option>
                      <Option value="SHE" label="🏪 Shenzhen Stock Exchange (SHE)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏪</span>
                          <span>Shenzhen Stock Exchange (SHE)</span>
                        </div>
                      </Option>
                      <Option value="HK" label="🏙️ Hong Kong Stock Exchange (HK)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏙️</span>
                          <span>Hong Kong Stock Exchange (HK)</span>
                        </div>
                      </Option>
                      <Option value="TWO" label="🏝️ Taiwan Stock Exchange (TWO)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏝️</span>
                          <span>Taiwan Stock Exchange (TWO)</span>
                        </div>
                      </Option>
                      <Option value="JP" label="🗾 Tokyo Stock Exchange (JP)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🗾</span>
                          <span>Tokyo Stock Exchange (JP)</span>
                        </div>
                      </Option>
                      <Option value="LSE" label="🇬🇧 London Stock Exchange (LSE)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🇬🇧</span>
                          <span>London Stock Exchange (LSE)</span>
                        </div>
                      </Option>
                      <Option value="AIM" label="💡 London AIM Market (AIM)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>💡</span>
                          <span>London AIM Market (AIM)</span>
                        </div>
                      </Option>
                    </>
                  ) : (
                    <>
                      {/* 其他数据源的完整交易所列表 */}
                      <Option value="US" label="🇺🇸 USA Stocks (NYSE, NASDAQ, etc.)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🇺🇸</span>
                          <span>USA Stocks (NYSE, NASDAQ, NYSE ARCA, OTC markets)</span>
                        </div>
                      </Option>
                      
                      {/* European Exchanges */}
                      <Option value="LSE" label="🇬🇧 London Stock Exchange">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🇬🇧</span>
                          <span>London Stock Exchange (LSE)</span>
                        </div>
                      </Option>
                  <Option value="PA" label="🇫🇷 Euronext Paris">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇫🇷</span>
                      <span>Euronext Paris (PA)</span>
                    </div>
                  </Option>
                  <Option value="AS" label="🇳🇱 Euronext Amsterdam">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇳🇱</span>
                      <span>Euronext Amsterdam (AS)</span>
                    </div>
                  </Option>
                  <Option value="BR" label="🇧🇪 Euronext Brussels">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇧🇪</span>
                      <span>Euronext Brussels (BR)</span>
                    </div>
                  </Option>
                  <Option value="VX" label="🇨🇭 Swiss Exchange (SIX)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇭</span>
                      <span>Swiss Exchange (SIX) (VX)</span>
                    </div>
                  </Option>
                  <Option value="MC" label="🇪🇸 Madrid Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇪🇸</span>
                      <span>Madrid Stock Exchange (MC)</span>
                    </div>
                  </Option>
                  <Option value="F" label="🇩🇪 Frankfurt Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇩🇪</span>
                      <span>Frankfurt Stock Exchange (F)</span>
                    </div>
                  </Option>
                  
                  {/* Asia-Pacific Exchanges */}
                  <Option value="HK" label="🇭🇰 Hong Kong Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇭🇰</span>
                      <span>Hong Kong Stock Exchange (HK)</span>
                    </div>
                  </Option>
                  <Option value="SHG" label="🇨🇳 Shanghai Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇳</span>
                      <span>Shanghai Stock Exchange (SHG)</span>
                    </div>
                  </Option>
                  <Option value="SHE" label="🇨🇳 Shenzhen Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇳</span>
                      <span>Shenzhen Stock Exchange (SHE)</span>
                    </div>
                  </Option>
                  <Option value="KO" label="🇰🇷 Korea Stock Exchange (KRX)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇰🇷</span>
                      <span>Korea Stock Exchange (KRX) (KO)</span>
                    </div>
                  </Option>
                  <Option value="TW" label="🇹🇼 Taiwan Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇹🇼</span>
                      <span>Taiwan Stock Exchange (TW)</span>
                    </div>
                  </Option>
                  <Option value="SG" label="🇸🇬 Singapore Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇸🇬</span>
                      <span>Singapore Exchange (SG)</span>
                    </div>
                  </Option>
                  <Option value="AU" label="🇦🇺 Australian Securities Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇦🇺</span>
                      <span>Australian Securities Exchange (AU)</span>
                    </div>
                  </Option>
                  
                  {/* North America Other */}
                  <Option value="TO" label="🇨🇦 Toronto Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇦</span>
                      <span>Toronto Stock Exchange (TO)</span>
                    </div>
                  </Option>
                  
                  {/* Indian Exchanges */}
                  <Option value="BSE" label="🇮🇳 Bombay Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇳</span>
                      <span>Bombay Stock Exchange (BSE)</span>
                    </div>
                  </Option>
                  <Option value="NSE" label="🇮🇳 National Stock Exchange of India">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇳</span>
                      <span>National Stock Exchange of India (NSE)</span>
                    </div>
                  </Option>
                  
                      {/* South American Exchanges */}
                      <Option value="SA" label="🇧🇷 São Paulo Stock Exchange (B3)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🇧🇷</span>
                          <span>São Paulo Stock Exchange (B3) (SA)</span>
                        </div>
                      </Option>
                    </>
                  )}
                </Select>
              </Form.Item>
              
              {/* Data Types selection for EODHD Fundamentals Low Frequency */}
              {dataSourceType === 'eodhd_fundamentals' && (
                <Form.Item
                  label="Data Types"
                  name={['taskParams', 'data_types']}
                  rules={[]}
                  extra="Select data types to fetch. Choose from 12 comprehensive data categories for complete company analysis."
                  initialValue={['General', 'Financials', 'Highlights', 'Valuation', 'Technicals', 'Holders', 'outstandingShares', 'Earnings', 'SharesStats', 'AnalystRatings', 'SplitsDividends', 'InsiderTransactions']}
                >
                  <Select 
                    placeholder="Select data types" 
                    mode="multiple"
                    style={{ width: '100%' }}
                    disabled={false}
                  >
                    <Option value="General" label="📊 General">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>📊</span>
                        <span>General (Company information, address, officers)</span>
                      </div>
                    </Option>
                    <Option value="Financials" label="💰 Financials">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>💰</span>
                        <span>Financials (Balance sheet, income statement, cash flow)</span>
                      </div>
                    </Option>
                    <Option value="Highlights" label="⭐ Highlights">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>⭐</span>
                        <span>Highlights (Key metrics: market cap, P/E, earnings, margins)</span>
                      </div>
                    </Option>
                    <Option value="Valuation" label="📈 Valuation">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>📈</span>
                        <span>Valuation (P/E ratios, enterprise value, price multiples)</span>
                      </div>
                    </Option>
                    <Option value="Technicals" label="📊 Technicals">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>📊</span>
                        <span>Technicals (Beta, 52-week range, moving averages, short data)</span>
                      </div>
                    </Option>
                    <Option value="Holders" label="👥 Holders">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>👥</span>
                        <span>Holders (Institutional holdings, fund holdings, ownership data)</span>
                      </div>
                    </Option>
                    <Option value="outstandingShares" label="📈 Outstanding Shares">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>📈</span>
                        <span>Outstanding Shares (Annual/quarterly shares outstanding data)</span>
                      </div>
                    </Option>
                    <Option value="Earnings" label="💸 Earnings">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>💸</span>
                        <span>Earnings (Annual earnings, history, trend, estimates)</span>
                      </div>
                    </Option>
                    <Option value="SharesStats" label="📊 Shares Stats">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>📊</span>
                        <span>Shares Stats (Outstanding, float, short interest, insider %)</span>
                      </div>
                    </Option>
                    <Option value="AnalystRatings" label="🎯 Analyst Ratings">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>🎯</span>
                        <span>Analyst Ratings (Strong buy/buy/hold/sell, target price)</span>
                      </div>
                    </Option>
                    <Option value="SplitsDividends" label="💰 Splits & Dividends">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>💰</span>
                        <span>Splits & Dividends (Dividend rate/yield, split history)</span>
                      </div>
                    </Option>
                    <Option value="InsiderTransactions" label="🔐 Insider Transactions">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>🔐</span>
                        <span>Insider Transactions (Executive trading activity)</span>
                      </div>
                    </Option>
                  </Select>
                </Form.Item>
              )}
            </>
          );

        case 'market_weekly_report':
          return null;

        case 'the_close_summary':
          return null;

        case 'cnbc_closing_bell':
          return null;

        case 'bloomberg_surveillance':
          return null;

        case 'fed_dashboard':
          return (
            <Form.Item
              label="Mode"
              name={['taskParams', 'mode']}
              extra="poll: RSS discovery + fetch + summarize + retry failed docs. retry: only retry pending/failed documents."
              initialValue="poll"
            >
              <Select>
                <Option value="poll">Poll RSS (full chain)</Option>
                <Option value="retry">Retry pending/failed only</Option>
              </Select>
            </Form.Item>
          );

        case 'form13f':
          return (
            <Form.Item
              label="Mode"
              name={['taskParams', 'mode']}
              extra="poll: sync 20 preset CIKs from EDGAR (13F-HR + amendments + snapshots). retry: same as poll."
              initialValue="poll"
            >
              <Select>
                <Option value="poll">Sync preset CIKs</Option>
                <Option value="retry">Retry (same as poll)</Option>
              </Select>
            </Form.Item>
          );

        case 'zhishixingqiu_summary':
          return null;

        default:
          return null;
      }
  };

  return (
    <Modal
      title={
        <Space>
          {currentConfig.icon}
          {dataSourceType === 'eodhd_fundamentals'
            ? `Update Data for ${dataSourceName}`
            : `Configure Data Acquisition Strategy for ${dataSourceName}`}
        </Space>
      }
      open={open}
      onCancel={handleCancel}
      width={800}
      styles={getModalStyles(themeMode)}
      footer={
        <Space style={{ justifyContent: 'flex-end', display: 'flex', width: '100%' }}>
          <Button onClick={handleCancel}>Cancel</Button>
          <Button
            type="primary"
            onClick={handleSubmit}
            loading={loading}
            icon={<CheckCircleOutlined />}
          >
            {dataSourceType === 'eodhd_fundamentals' ? 'Update Data' : 'Create Strategy'}
          </Button>
        </Space>
      }
      destroyOnClose
    >
      <div style={{ maxHeight: '70vh', overflowY: 'auto', padding: '4px 0' }}>
        <Form form={form} layout="vertical" size="middle">
          <Card
            style={{ marginBottom: 16, backgroundColor: colors.cardSecondaryBg }}
            size="small"
          >
            <Row align="middle">
              <Col span={2}>{currentConfig.icon}</Col>
              <Col span={22}>
                <Text strong>{dataSourceName}</Text>
                <div style={{ marginTop: 4 }}>
                  <Text type="secondary" style={{ fontSize: '12px' }}>
                    {currentConfig.description}
                  </Text>
                </div>
              </Col>
            </Row>
          </Card>

          {dataSourceType !== 'eodhd_fundamentals' && (
            <Row gutter={16}>
              <Col span={14}>
                <Form.Item
                  label={
                    <Space>
                      Cron Expression
                      <Button
                        type="text"
                        size="small"
                        icon={<QuestionCircleOutlined />}
                        onClick={() => setShowCronGuide(true)}
                        style={{ padding: 0, height: 'auto' }}
                      >
                        Usage Guide
                      </Button>
                    </Space>
                  }
                  name="cronExpression"
                  rules={[
                    { required: true, message: 'Please enter a Cron expression' },
                    {
                      validator: (_, value) => {
                        if (!value) return Promise.resolve();
                        const { valid, message: msg } = schedulerAPI.validateCronExpression(value);
                        return valid ? Promise.resolve() : Promise.reject(new Error(msg));
                      },
                    },
                  ]}
                  extra="Format: Minutes Hours Day Month Week (e.g., */15 * * * * means execute every 15 minutes)"
                >
                  <Input
                    placeholder="Enter Cron expression, e.g., */15 * * * *"
                    suffix={
                      <Tooltip title="Format: Minutes Hours Day Month Week">
                        <InfoCircleOutlined />
                      </Tooltip>
                    }
                  />
                </Form.Item>
              </Col>
              <Col span={10}>
                <Form.Item
                  label="Timezone"
                  name="timezone"
                  rules={[{ required: true, message: 'Please select a timezone' }]}
                >
                  <Select placeholder="Select timezone" showSearch>
                    {timezoneOptions.map((tz) => (
                      <Option key={tz.value} value={tz.value}>
                        {tz.label}
                      </Option>
                    ))}
                  </Select>
                </Form.Item>
              </Col>
            </Row>
          )}

          {hasParamFields && dataSourceType !== 'eodhd_fundamentals' && (
            <Divider orientation="left">Task Parameters</Divider>
          )}

          {renderParamFields()}
        </Form>
      </div>

      <CronExpressionGuide
        open={showCronGuide}
        onCancel={() => setShowCronGuide(false)}
      />
    </Modal>
  );
};

export default DataSourceConfigModal;
