/* =========================================================================
   Scheduler 数据源卡片共享定义（桌面 SchedulerPage 与移动 MobileSchedulerPage 共用）
   - 类型 SchedulerCategory / DataSource / QuickAction
   - 静态卡片注册表 INITIAL_SCHEDULER_DATA_SOURCES（两类：data_fetch / report_generation）
   - 分类排序工具（配合后端卡片排序偏好接口）
   - isTaskRelatedToSource：数据源 ID → 关联定时任务判定（原 SchedulerPage 内三处
     重复内联谓词的统一抽取，取 toggleDataSource 版本——模糊规则覆盖其余版本的
     显式规则，对现有全部 source id 行为等价）
   ========================================================================= */
import React from 'react';
import {
  BarChartOutlined,
  CalendarOutlined,
  CloudDownloadOutlined,
  DatabaseOutlined,
  FilterOutlined,
  LineChartOutlined,
  ScheduleOutlined,
} from '@ant-design/icons';

// Data source configuration
/** 数据获取类：拉取外部数据；报告生成类：预生成摘要/报告等 */
export type SchedulerCategory = 'data_fetch' | 'report_generation';

export interface DataSource {
  id: string;
  name: string;
  type: 'eodhd' | 'watchlist' | 'cctv' | 'finviz' | 'finviz_screener' | 'eodhd_events' | 'eodhd_macro' | 'eodhd_fundamentals' | 'eodhd_stock_prices' | 'eodhd_eod_stock_prices' | 'market_data' | 'earnings_calendar' | 'ipo_calendar' | 'watchlist_fundamentals' | 'earnings_active_fundamentals' | 'fundamentals_rotation' | 'twitter_subscription' | 'youtube_subscription' | 'youtube_subscription_morning' | 'podcast_subscription' | 'news_feed' | 'zhishixingqiu_subscription' | 'substack_subscription' | 'weibo_subscription' | 'twitter_author_summary' | 'youtube_author_summary' | 'theme_report' | 'market_weekly_report' | 'daily_market_report' | 'the_close_summary' | 'cnbc_closing_bell' | 'bloomberg_surveillance' | 'bloomberg_surveillance_flash' | 'zhishixingqiu_summary' | 'substack_summary' | 'fed_dashboard' | 'form13f' | 'other';
  category: SchedulerCategory;
  description: string;
  enabled: boolean;
  status: 'idle' | 'running' | 'success' | 'error';
  lastRun?: string;
  lastSuccess?: string;
  sortOrder?: number;
  quickActions: QuickAction[];
}

export interface QuickAction {
  id: string;
  name: string;
  description: string;
  endpoint: string;
  params?: Record<string, any>;
  icon: React.ReactNode;
}

function getDefaultCategoryOrder(sources: DataSource[], category: SchedulerCategory): string[] {
  return sources
    .filter((s) => s.category === category)
    .sort((a, b) => (a.sortOrder ?? 0) - (b.sortOrder ?? 0))
    .map((s) => s.id);
}

export function mergeCategoryOrder(saved: string[] | undefined, defaults: string[]): string[] {
  const defaultSet = new Set(defaults);
  const validSaved = (saved ?? []).filter((id) => defaultSet.has(id));
  const savedSet = new Set(validSaved);
  return [...validSaved, ...defaults.filter((id) => !savedSet.has(id))];
}

/** 返回静态目录的默认顺序（首屏兜底；挂载后从后端拉取用户偏好覆盖） */
export function getDefaultCategoryOrders(sources: DataSource[]): Record<SchedulerCategory, string[]> {
  return {
    data_fetch: getDefaultCategoryOrder(sources, 'data_fetch'),
    report_generation: getDefaultCategoryOrder(sources, 'report_generation'),
  };
}

export const INITIAL_SCHEDULER_DATA_SOURCES: DataSource[] = [
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
      id: 'youtube_subscription_morning',
      name: 'YouTube Bloomberg 早间加密抓取',
      type: 'youtube_subscription_morning',
      category: 'data_fetch',
      sortOrder: 25,
      description: 'The Close 8:30 SLA:北京 06:05-08:55 每 10 分钟定向抓取 Bloomberg Television(订阅 41),补全量任务整点间隙',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_youtube_morning_schedule',
          name: 'Configure Morning Dense Fetch Schedule',
          description: 'Adjust the dense morning fetch window (default: every 10 minutes, 06:05-08:55 Beijing)',
          endpoint: '/scheduler/tasks/youtube-subscription-morning',
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
          description: 'Set the schedule for Twitter Daily Report generation (default: daily 04:00 Beijing, before Daily Market Report 06:00). Runs Phase A (author summaries) → Phase B (user reports) automatically.',
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
          description: 'Set the schedule for YouTube Daily Report generation (default: daily 04:30 Beijing, before Daily Market Report 06:00). Runs Phase A (channel summaries) → Phase B (user reports) automatically.',
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
          description: 'Set up periodic Daily Market Report generation (default: Mon-Sat 06:00 Beijing, after Twitter/YouTube/Substack/Zhishixingqiu summaries)',
          endpoint: '/scheduler/tasks/daily-market-report',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'the_close_summary',
      name: 'Bloomberg The Close Report',
      type: 'the_close_summary',
      category: 'report_generation',
      sortOrder: 60,
      description: 'Generate daily The Close Report (PM Brief) from Bloomberg Television "The Close" episode transcript.',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_the_close_summary_schedule',
          name: 'Configure The Close Report Schedule',
          description: 'Set up periodic The Close PM Brief generation (default: weekdays 7 AM Beijing / 7 PM ET)',
          endpoint: '/scheduler/tasks/the-close-summary',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'cnbc_closing_bell',
      name: 'CNBC Closing Bell Report',
      type: 'cnbc_closing_bell',
      category: 'report_generation',
      sortOrder: 70,
      description: 'Generate daily Closing Bell Report (Stock News) from CNBC "Closing Bell" episode transcript.',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_cnbc_closing_bell_schedule',
          name: 'Configure Closing Bell Report Schedule',
          description: 'Set up periodic Closing Bell Stock News generation (default: weekdays 7 AM Beijing / 7 PM ET)',
          endpoint: '/scheduler/tasks/cnbc-closing-bell',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'bloomberg_surveillance',
      name: 'Bloomberg Surveillance Report',
      type: 'bloomberg_surveillance',
      category: 'report_generation',
      sortOrder: 75,
      description: 'Generate daily Surveillance Report (AM Brief, full version) from Bloomberg "Surveillance" YouTube full-episode transcript.',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_bloomberg_surveillance_schedule',
          name: 'Configure Surveillance Report Schedule',
          description: 'Tue-Sat 07:30/08:30/12:30 Beijing, YouTube full-episode upgrade over the flash brief (07:30 EDT / 08:30 EST, 12:30 final)',
          endpoint: '/scheduler/tasks/bloomberg-surveillance',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'bloomberg_surveillance_flash',
      name: 'Bloomberg Surveillance Flash Report',
      type: 'bloomberg_surveillance_flash',
      category: 'report_generation',
      sortOrder: 76,
      description: 'Generate same-evening flash Surveillance Report from the official podcast highlight transcript (ready 21:36-22:06 Beijing).',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_bloomberg_surveillance_flash_schedule',
          name: 'Configure Surveillance Flash Report Schedule',
          description: 'Mon-Fri 22:30/23:30 Beijing from podcast highlight transcript; auto-adapts to DST (22:30 EDT / 23:30 EST)',
          endpoint: '/scheduler/tasks/bloomberg-surveillance-flash',
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
          description: 'Set up periodic Zhishixingqiu daily summary generation (default: 05:30 Beijing, before Daily Market Report 06:00)',
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
          description: 'Set up periodic Substack daily summary generation (default: 05:00 Beijing, before Daily Market Report 06:00)',
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
    },
    {
      id: 'earnings_active_fundamentals',
      name: 'Earnings Active Fundamentals',
      type: 'earnings_active_fundamentals',
      category: 'data_fetch',
      sortOrder: 155,
      description: 'Daily refresh for companies reporting earnings within [-7d, +21d] window (up to 800/day, ~8k EODHD calls). Keeps calendar-viewed tickers fresh around their earnings dates',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_earnings_active_fundamentals',
          name: 'Configure Earnings Active Fundamentals Refresh',
          description: 'Runs daily after the 18:00 earnings calendar fetch. Params: window_back / window_forward / stale_days / max_companies',
          endpoint: '/scheduler/tasks/earnings-active-fundamentals',
          params: {},
          icon: <DatabaseOutlined />
        }
      ]
    },
    {
      id: 'fundamentals_rotation',
      name: 'Fundamentals Rotation',
      type: 'fundamentals_rotation',
      category: 'data_fetch',
      sortOrder: 156,
      description: 'Nightly full rotation of US+HK active companies (~15k pool), oldest-first 3000/day ≈ 30% of EODHD daily quota, full cycle in ~5 days',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_fundamentals_rotation',
          name: 'Configure Fundamentals Rotation',
          description: 'Runs nightly at 01:30. Param: daily_limit (companies per day, each costs 10 EODHD calls)',
          endpoint: '/scheduler/tasks/fundamentals-rotation',
          params: {},
          icon: <DatabaseOutlined />
        }
      ]
    }
];

/**
 * 支持打开 DataSourceConfigModal 配置调度的数据源 ID 白名单
 * （桌面 SchedulerPage#showQuickActionModal 与移动 MobileSchedulerPage 同口径）。
 */
export const CONFIGURABLE_SOURCE_IDS: ReadonlySet<string> = new Set([
  'twitter_subscription',
  'youtube_subscription',
  'youtube_subscription_morning',
  'podcast_subscription',
  'news_feed',
  'zhishixingqiu_subscription',
  'weibo_subscription',
  'twitter_author_summary',
  'youtube_author_summary',
  'theme_report',
  'market_weekly_report',
  'daily_market_report',
  'the_close_summary',
  'cnbc_closing_bell',
  'bloomberg_surveillance',
  'bloomberg_surveillance_flash',
  'zhishixingqiu_summary',
  'substack_subscription',
  'substack_summary',
  'fed_dashboard',
  'form13f',
  'eodhd_events',
  'eodhd_macro',
  'eodhd_fundamentals',
  'finviz_screener',
  'eodhd_stock_prices',
  'eodhd_eod_stock_prices',
  'market_data',
  'earnings_calendar',
  'ipo_calendar',
  'watchlist_fundamentals',
]);

/**
 * 数据源 ID → 关联定时任务判定。
 * 取原 SchedulerPage#toggleDataSource 的映射版本（最完整超集：显式等值规则 +
 * events/macro/exchange/fundamentals/finviz_screener 模糊规则），桌面页与移动页
 * 的状态同步、批量启停均以此为准，避免两份映射漂移。
 */
export function isTaskRelatedToSource(sourceId: string, taskType: string): boolean {
  const sourceIdLower = sourceId.toLowerCase();
  const taskTypeLower = taskType.toLowerCase();

  return (
    (sourceIdLower === 'eodhd_news' && taskTypeLower === 'eodhd_news') ||
    (sourceIdLower === 'watchlist_news' && taskTypeLower === 'watchlist_news') ||
    (sourceIdLower === 'cctv_news' && taskTypeLower === 'cctv_news') ||
    (sourceIdLower === 'twitter_subscription' && taskTypeLower === 'twitter_subscription') ||
    (sourceIdLower === 'youtube_subscription' && taskTypeLower === 'youtube_subscription') ||
    (sourceIdLower === 'youtube_subscription_morning' && taskTypeLower === 'youtube_subscription_morning') ||
    (sourceIdLower === 'podcast_subscription' && taskTypeLower === 'podcast_subscription') ||
    (sourceIdLower === 'news_feed' && taskTypeLower === 'news_feed') ||
    (sourceIdLower === 'zhishixingqiu_subscription' && taskTypeLower === 'zhishixingqiu_subscription') ||
    (sourceIdLower === 'substack_subscription' && taskTypeLower === 'substack_subscription') ||
    (sourceIdLower === 'weibo_subscription' && taskTypeLower === 'weibo_subscription') ||
    (sourceIdLower === 'twitter_author_summary' && taskTypeLower === 'twitter_author_summary') ||
    (sourceIdLower === 'youtube_author_summary' && taskTypeLower === 'youtube_author_summary') ||
    (sourceIdLower === 'theme_report' && taskTypeLower === 'theme_report') ||
    (sourceIdLower === 'market_weekly_report' && taskTypeLower === 'market_weekly_report') ||
    (sourceIdLower === 'daily_market_report' && taskTypeLower === 'daily_market_report') ||
    (sourceIdLower === 'the_close_summary' && taskTypeLower === 'the_close_summary') ||
    (sourceIdLower === 'cnbc_closing_bell' && taskTypeLower === 'cnbc_closing_bell') ||
    (sourceIdLower === 'bloomberg_surveillance' && taskTypeLower === 'bloomberg_surveillance') ||
    (sourceIdLower === 'bloomberg_surveillance_flash' && taskTypeLower === 'bloomberg_surveillance_flash') ||
    (sourceIdLower === 'zhishixingqiu_summary' && taskTypeLower === 'zhishixingqiu_summary') ||
    (sourceIdLower === 'substack_summary' && taskTypeLower === 'substack_summary') ||
    (sourceIdLower === 'fed_dashboard' && taskTypeLower === 'fed_dashboard') ||
    (sourceIdLower === 'form13f' && taskTypeLower === 'form13f') ||
    (sourceIdLower === 'eodhd_stock_prices' && taskTypeLower === 'eodhd_stock_prices') ||
    (sourceIdLower === 'eodhd_eod_stock_prices' && taskTypeLower === 'eodhd_eod_stock_prices') ||
    (sourceIdLower === 'market_data' && taskTypeLower === 'market_data') ||
    (sourceIdLower === 'earnings_calendar' && taskTypeLower === 'earnings_calendar') ||
    (sourceIdLower === 'ipo_calendar' && taskTypeLower === 'ipo_calendar') ||
    (sourceIdLower.includes('events') && taskTypeLower.includes('events')) ||
    (sourceIdLower.includes('macro') && (taskTypeLower.includes('macro') || taskTypeLower.includes('indicators'))) ||
    (sourceIdLower.includes('exchange') && !sourceIdLower.includes('fundamentals') && taskTypeLower.includes('exchange')) ||
    (sourceIdLower.includes('fundamentals') && taskTypeLower.includes('fundamentals')) ||
    (sourceIdLower.includes('finviz_screener') && taskTypeLower.includes('finviz_screener'))
  );
}
