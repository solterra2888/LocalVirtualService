import React, { useState, useEffect } from 'react';
import {
  Card,
  List,
  Button,
  Space,
  Tag,
  Typography,
  Row,
  Col,
  message,
  Modal,
  Badge,
  Empty,
  Skeleton,
  Switch,
  Input,
  Tooltip,
  Slider,
  Popover,
  Drawer,
  ConfigProvider,
  theme as antdTheme,
  Tabs
} from 'antd';
import {
  CalendarOutlined,
  FilterOutlined,
  ExportOutlined,
  SettingOutlined,
  StarFilled,
  StarOutlined,
  SearchOutlined,
  CloseCircleOutlined,
  ControlOutlined,
  BookOutlined,
  HighlightOutlined,
  ColumnHeightOutlined,
  DeleteOutlined
} from '@ant-design/icons';
import dayjs from 'dayjs';

import { newsAPI, newsUtils } from '../services/news';
import {
  NewsItem,
  NewsListResponse,
  NewsTopic,
  NewsSymbol,
  NewsSearchParams,
  NewsTheme
} from '../types/news';

import ThemeManagementModal from '../components/ThemeManagementModal';
import ThemeFilterPanel from '../components/ThemeFilterPanel';
import PresetsManager from '../components/PresetsManager';
import MarkdownRenderer from '../components/MarkdownRenderer';
import DeleteNewsByDateModal from '../components/DeleteNewsByDateModal';
import { useAuthStore } from '../store/authStore';
import { getErrorMessage } from '../utils/errorHandler';
import { getModalStyles, MODAL_DARK_BG } from '../utils/modalStyles';
import { getAnalysisContent, getAllAnalysisResults, getPreferredAnalysisContent, getPreferredSummaryTemplate } from '../utils/newsAnalysisHelper';
import { 
  getClassificationFromAnalysis, 
  getClassificationTags, 
  hasValidClassification,
  getClassificationSummary,
  getRelatedSectors,
  getRelatedCountries,
  COUNTRIES_MAP
} from '../utils/newsClassificationHelper';
import {
  hasValidUnifiedThemes,
  groupThemesByCanonicalName,
  getBestThemeDisplay,
  getSourceTypeConfig,
  formatQualityScore,
  getQualityScoreColor
} from '../utils/unifiedThemeHelper';
import { themesAPI } from '../services/themes';
import { newsPresetsAPI } from '../services/newsPresets';
import type { ThemeFilterState, FilteredNews, ThemeDimensionOperators, DimensionOperatorsRequest } from '../types/themes';
import type { NewsFilterPreset } from '../types/newsPresets';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

const { Text } = Typography;
const { Search } = Input;

// 主题配色（与 FRONTEND_STYLE_GUIDE 一致，Behance 风格）
const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    codeBlockBg: '#f5f5f5',
    selectedBorder: '#1890ff',
    inputBg: '#fff',
    hoverBg: '#f5f5f5',
    borderSub: '#e8e8e8',
    siderBg: '#fff',
    headerBorder: '#f0f0f0',
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
    borderSub: 'rgba(255, 255, 255, 0.06)',
    siderBg: '#252d3d',
    headerBorder: 'rgba(255, 255, 255, 0.06)',
  },
} as const;

// Behance 卡片风格：圆角 12px、柔和阴影、细边框
const CARD_STYLE_BEHANCE = {
  borderRadius: 12,
  boxShadow: '0 4px 24px rgba(0, 0, 0, 0.12)',
};
const CARD_STYLE_BEHANCE_DARK = {
  borderRadius: 12,
  boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)',
};

const ANALYSIS_MODAL_BODY_HEIGHT = '68vh';

interface NewsPageState {
  loading: boolean;
  newsData: NewsListResponse | null;
  topics: NewsTopic[];
  symbols: NewsSymbol[];
  searchParams: NewsSearchParams;
  selectedNews: NewsItem | null;
  themeManagementModalVisible: boolean;
  // 主题筛选相关
  themeFilters: ThemeFilterState;
  filteredNewsData: FilteredNews[];
  filteredTotal: number;
  filteredOffset: number;
  filteredPageSize: number;
  hasMoreNews: boolean;
  loadingMore: boolean;
  // 主题关键词数据（用于高亮）
  themes: NewsTheme[];
  // 预设相关
  activePresetId: number | null;
  advancedFilterModalVisible: boolean;
  presetsManagerVisible: boolean;
  presets: NewsFilterPreset[];
  presetsLoading: boolean;
  // 新闻内容高度设置
  newsContentHeight: number;
  // Sidebar控制
  sidebarVisible: boolean;
  // 删除新闻Modal
  deleteNewsModalVisible: boolean;
  // 鼠标悬停相关
  hoveredNewsId: number | null;
  mousePosition: { x: number; y: number };
}

const NewsPage: React.FC = () => {
  const { user, isAuthenticated } = useAuthStore();
  const { themeMode } = useDashboardSettings();
  const colors = THEME_COLORS[themeMode];
  const isDark = themeMode === 'dark';
  const cardStyleBehance = isDark
    ? { ...CARD_STYLE_BEHANCE_DARK, backgroundColor: colors.cardBg, border: `1px solid ${colors.cardBorder}` }
    : { ...CARD_STYLE_BEHANCE, backgroundColor: colors.cardBg, border: `1px solid ${colors.cardBorder}` };
  const hideTooltipTimeoutRef = React.useRef<NodeJS.Timeout | null>(null);
  
  const [state, setState] = useState<NewsPageState>({
    loading: false,
    newsData: null,
    topics: [],
    symbols: [],
    searchParams: {
      page: 1,
      page_size: 20
    },
    selectedNews: null,
    themeManagementModalVisible: false,
    // 主题筛选相关
    themeFilters: {
      newsValue: [],
      categories: [],
      eventTypes: [],
      impactScopes: [],
      urgencyLevels: [],
      relatedSectors: [],
      relatedCountries: [],
      sourceTypes: [],
      schedulerSources: [],
      minConfidence: 0.5,
      onlyPrimary: false,
      dateRange: null,
      market: null,
      dimensionOperators: {},  // 🔥 新增：维度操作符
      searchText: undefined,  // 🔥 新增：搜索文本
      useWatchlist: false,  // 默认关闭watchlist过滤
      tickerFilterMode: 'include' as const  // 默认使用include模式
    },
    filteredNewsData: [],
    filteredTotal: 0,
    filteredOffset: 0,
    filteredPageSize: 20,
    hasMoreNews: true,
    loadingMore: false,
    themes: [],
    // 预设相关
    activePresetId: null,
    advancedFilterModalVisible: false,
    presetsManagerVisible: false,
    presets: [],
    presetsLoading: false,
    // 新闻内容高度设置
    newsContentHeight: 20,
    // Sidebar控制
    sidebarVisible: false,
    // 删除新闻Modal
    deleteNewsModalVisible: false,
    // 鼠标悬停相关
    hoveredNewsId: null,
    mousePosition: { x: 0, y: 0 }
  });

  // 加载预设列表
  const loadPresets = async () => {
    setState(prev => ({ ...prev, presetsLoading: true }));
    try {
      const response = await newsPresetsAPI.getPresets({ include_system: true });
      setState(prev => ({ ...prev, presets: response.presets, presetsLoading: false }));
    } catch (error: any) {
      console.error('Failed to load presets:', error);
      message.error('Failed to load presets');
      setState(prev => ({ ...prev, presetsLoading: false }));
    }
  };

  // 加载主题和股票代码数据
  const loadMetadata = async () => {
    try {
      console.log('start to load metadata...');
      const [topics, symbols, themesResponse] = await Promise.all([
        newsAPI.getTopics(),
        newsAPI.getSymbols(),
        newsAPI.getThemes()
      ]);
      console.log('loaded metadata successfully:', { 
        topics: topics.length, 
        symbols: symbols.length,
        themes: themesResponse.data.length 
      });
      setState(prev => ({
        ...prev,
        topics,
        symbols: symbols.slice(0, 50),
        themes: themesResponse.data.filter(t => t.is_active) // 只加载激活的主题
      }));
    } catch (error: any) {
      console.error('Failed to get metadata:', error);
      message.error(`Failed to get metadata: ${getErrorMessage(error)}`);
    }
  };

  // 加载主题筛选的新闻（支持追加模式）
  const loadFilteredNews = async (reset: boolean = true, filters?: ThemeFilterState) => {
    const startTime = Date.now();
    
    // 使用传入的 filters 或当前 state 中的 filters
    const activeFilters = filters || state.themeFilters;
    const offset = reset ? 0 : state.filteredOffset;
    const pageSize = state.filteredPageSize;
    
    console.log('🎯 [主题筛选] 开始加载筛选新闻', {
      filters: activeFilters,
      offset,
      pageSize,
      reset,
      timestamp: new Date().toISOString()
    });
    
    // 根据 reset 设置不同的 loading 状态
    if (reset) {
      setState(prev => ({ ...prev, loading: true }));
    } else {
      setState(prev => ({ ...prev, loadingMore: true }));
    }
    
    try {
      // 构建维度操作符请求对象
      const buildDimensionOperators = (): DimensionOperatorsRequest | undefined => {
        const ops = activeFilters.dimensionOperators;
        if (!ops) return undefined;
        
        const result: DimensionOperatorsRequest = {};
        if (ops.newsValue) result.news_value = ops.newsValue;
        if (ops.categories) result.categories = ops.categories;
        if (ops.eventTypes) result.event_types = ops.eventTypes;
        if (ops.impactScopes) result.impact_scopes = ops.impactScopes;
        if (ops.urgencyLevels) result.urgency = ops.urgencyLevels;
        if (ops.relatedSectors) result.related_sectors = ops.relatedSectors;
        if (ops.relatedCountries) result.related_countries = ops.relatedCountries;
        if (ops.sourceTypes) result.source_types = ops.sourceTypes;
        if (ops.schedulerSources) result.scheduler_sources = ops.schedulerSources;
        
        return Object.keys(result).length > 0 ? result : undefined;
      };

      // 构建请求参数（使用 offset 而不是 page）
      const request = {
        news_value: activeFilters.newsValue && activeFilters.newsValue.length > 0 ? activeFilters.newsValue : undefined,
        categories: activeFilters.categories.length > 0 ? activeFilters.categories : undefined,
        event_types: activeFilters.eventTypes.length > 0 ? activeFilters.eventTypes : undefined,
        impact_scopes: activeFilters.impactScopes.length > 0 ? activeFilters.impactScopes : undefined,
        urgency_levels: activeFilters.urgencyLevels.length > 0 ? activeFilters.urgencyLevels : undefined,
        related_sectors: activeFilters.relatedSectors.length > 0 ? activeFilters.relatedSectors : undefined,
        related_countries: activeFilters.relatedCountries.length > 0 ? activeFilters.relatedCountries : undefined,
        source_types: activeFilters.sourceTypes.length > 0 ? activeFilters.sourceTypes : undefined,
        scheduler_sources: activeFilters.schedulerSources.length > 0 ? activeFilters.schedulerSources : undefined,
        dimension_operators: buildDimensionOperators(),  // 🔥 新增：维度操作符
        // 🔥 新增：Ticker过滤参数
        ticker_symbols: activeFilters.tickerSymbols && activeFilters.tickerSymbols.length > 0 ? activeFilters.tickerSymbols : undefined,
        use_watchlist: activeFilters.useWatchlist || undefined,
        watchlist_groups: activeFilters.watchlistGroups && activeFilters.watchlistGroups.length > 0 ? activeFilters.watchlistGroups : undefined,
        ticker_filter_mode: activeFilters.tickerFilterMode || undefined,
        // 🔥 新增：搜索文本
        search_text: activeFilters.searchText && activeFilters.searchText.trim() ? activeFilters.searchText.trim() : undefined,
        min_confidence: activeFilters.minConfidence,
        only_primary: activeFilters.onlyPrimary,
        start_date: activeFilters.dateRange?.[0]?.format('YYYY-MM-DD'),
        end_date: activeFilters.dateRange?.[1]?.format('YYYY-MM-DD'),
        market: activeFilters.market || undefined,
        page: Math.floor(offset / pageSize) + 1,
        page_size: pageSize
      };
      
      // 🔥 记录ticker过滤和搜索参数（用于调试）
      if (request.use_watchlist || request.ticker_symbols) {
        console.log('🎯 [Ticker Filter] Request parameters:', {
          use_watchlist: request.use_watchlist,
          watchlist_groups: request.watchlist_groups,
          ticker_symbols: request.ticker_symbols,
          ticker_filter_mode: request.ticker_filter_mode
        });
      }
      if (request.search_text) {
        console.log('🔍 [Search] Searching for:', request.search_text);
      }
      
      const response = await themesAPI.filterNews(request);
      const loadTime = Date.now() - startTime;
      
      console.log('✅ [主题筛选] 筛选新闻加载成功', {
        total: response.total,
        page: response.page,
        page_size: response.page_size,
        total_pages: response.total_pages,
        news_count: response.news?.length || 0,
        load_time: `${loadTime}ms`,
        reset,
        ticker_filter_active: request.use_watchlist || (request.ticker_symbols && request.ticker_symbols.length > 0)
      });
      
      // 根据 reset 模式决定是替换还是追加数据
      setState(prev => ({
        ...prev,
        filteredNewsData: reset ? response.news : [...prev.filteredNewsData, ...response.news],
        filteredTotal: response.total,
        filteredOffset: reset ? pageSize : prev.filteredOffset + pageSize,
        filteredPageSize: response.page_size,
        hasMoreNews: response.news.length === pageSize,
        loading: false,
        loadingMore: false
      }));
    } catch (error) {
      const loadTime = Date.now() - startTime;
      
      console.error('❌ Failed to filter news by themes', {
        error,
        filters: activeFilters,
        load_time: `${loadTime}ms`,
        timestamp: new Date().toISOString()
      });
      
      message.error(`筛选新闻失败: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, loading: false, loadingMore: false }));
    }
  };

  // 处理主题筛选变化
  const handleThemeFilterChange = (filters: ThemeFilterState) => {
    setState(prev => ({ 
      ...prev, 
      themeFilters: filters,
      activePresetId: null // 筛选条件改变时清除预设激活状态
    }));
  };

  // 应用主题筛选（重置并加载）
  const handleApplyThemeFilter = (filters?: ThemeFilterState) => {
    // 重置 offset 和 hasMore 状态，并清除激活的预设
    setState(prev => ({ 
      ...prev, 
      filteredOffset: 0, 
      hasMoreNews: true,
      filteredNewsData: [],
      activePresetId: null // 手动修改筛选条件时清除预设激活状态
    }));
    loadFilteredNews(true, filters);
  };

  // 加载更多新闻
  const loadMoreNews = () => {
    if (!state.hasMoreNews || state.loadingMore) return;
    loadFilteredNews(false);
  };

  // 🔥 新增：应用预设
  const handleApplyPreset = async (preset: NewsFilterPreset) => {
    console.log('🎯 应用预设:', preset.preset_name, preset.filter_config);
    
    // 将 FilterConfig 转换为 ThemeFilterState
    const config = preset.filter_config;
    
    // 转换维度操作符
    const dimensionOperators: ThemeDimensionOperators = {};
    if (config.dimension_operators) {
      const ops = config.dimension_operators;
      if (ops.news_value) dimensionOperators.newsValue = ops.news_value;
      if (ops.categories) dimensionOperators.categories = ops.categories;
      if (ops.event_types) dimensionOperators.eventTypes = ops.event_types;
      if (ops.impact_scopes) dimensionOperators.impactScopes = ops.impact_scopes;
      if (ops.urgency) dimensionOperators.urgencyLevels = ops.urgency;
      if (ops.related_sectors) dimensionOperators.relatedSectors = ops.related_sectors;
      if (ops.related_countries) dimensionOperators.relatedCountries = ops.related_countries;
      if (ops.source_types) dimensionOperators.sourceTypes = ops.source_types;
      if (ops.scheduler_sources) dimensionOperators.schedulerSources = ops.scheduler_sources;
    }
    
    const newFilters: ThemeFilterState = {
      newsValue: config.news_value || [],
      categories: config.categories || [],
      eventTypes: config.event_types || [],
      impactScopes: config.impact_scopes || [],
      urgencyLevels: config.urgency || [],
      relatedSectors: config.related_sectors || [],
      relatedCountries: config.related_countries || [],
      sourceTypes: config.source_types || [],
      schedulerSources: config.scheduler_sources || [],
      minConfidence: config.min_confidence || 0.5,
      onlyPrimary: config.only_primary || false,
      dateRange: null, // 预设不保存日期范围
      market: config.market || null,
      dimensionOperators,  // 🔥 新增：维度操作符
      // 🔥 保留当前的watchlist相关设置（不受预设影响）
      useWatchlist: state.themeFilters.useWatchlist,
      tickerFilterMode: state.themeFilters.tickerFilterMode,
      tickerSymbols: state.themeFilters.tickerSymbols,
      watchlistGroups: state.themeFilters.watchlistGroups,
      searchText: state.themeFilters.searchText
    };

    // 更新筛选条件并应用
    setState(prev => ({
      ...prev,
      themeFilters: newFilters,
      activePresetId: preset.id,
      filteredOffset: 0,
      hasMoreNews: true,
      filteredNewsData: []
    }));

    // 如果不是系统预设，更新使用统计
    if (preset.id > 0 && preset.preset_type !== 'system') {
      try {
        await newsPresetsAPI.usePreset(preset.id);
      } catch (error) {
        console.error('Failed to update preset usage:', error);
      }
    }

    // 立即加载新闻
    loadFilteredNews(true, newFilters);
  };

  // 🔥 新增：创建预设

  // 🔥 新增：删除预设
  const handleDeletePreset = async (presetId: number) => {
    try {
      await newsPresetsAPI.deletePreset(presetId);
      message.success('Preset deleted successfully');
      await loadPresets();
      // 如果删除的是当前激活的预设，清除激活状态
      if (state.activePresetId === presetId) {
        setState(prev => ({ ...prev, activePresetId: null }));
      }
    } catch (error: any) {
      console.error('Failed to delete preset:', error);
      message.error('Failed to delete preset');
    }
  };

  // 导出筛选的新闻为CSV
  const handleExportToCSV = () => {
    if (state.filteredNewsData.length === 0) {
      message.warning('No news data to export. Please apply filters first.');
      return;
    }

    try {
      // 定义CSV列
      const headers = [
        'ID',
        'Date',
        'Title',
        'Content',
        'Analysis Result',
        'Updated At',
        'Source',
        'Link',
        'Themes'
      ];

      // 转换数据为CSV行
      const csvRows = [
        headers.join(','),
        ...state.filteredNewsData.map(item => {
          // 处理可能包含逗号、引号或换行符的字段
          const escapeCSV = (value: any): string => {
            if (value === null || value === undefined) return '';
            const str = String(value);
            // 如果包含逗号、引号或换行符，需要用引号包裹，并转义内部引号
            if (str.includes(',') || str.includes('"') || str.includes('\n')) {
              return `"${str.replace(/"/g, '""')}"`;
            }
            return str;
          };

          // 从analysis_result中提取news_cctv_summary_v1的content
          let analysisContent = '';
          if (item.analysis_result) {
            const cctvAnalysis = getAnalysisContent(item.analysis_result, 'news_cctv_summary_v1');
            if (cctvAnalysis && typeof cctvAnalysis.content === 'string') {
              analysisContent = cctvAnalysis.content;
            }
          }
          
          const themes = item.themes && item.themes.length > 0
            ? item.themes.map(t => `${t.theme_name}${t.display_name ? ` (${t.display_name})` : ''}`).join('; ')
            : '';

          return [
            item.id,
            escapeCSV(item.date),
            escapeCSV(item.title),
            escapeCSV(item.content || ''),
            escapeCSV(analysisContent),
            escapeCSV(item.updated_at || ''),
            escapeCSV(item.source),
            escapeCSV(item.link || ''),
            escapeCSV(themes)
          ].join(',');
        })
      ];

      // 创建CSV内容
      const csvContent = csvRows.join('\n');

      // 添加BOM以支持Excel正确显示中文
      const BOM = '\uFEFF';
      const blob = new Blob([BOM + csvContent], { type: 'text/csv;charset=utf-8;' });

      // 创建下载链接
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      
      // 生成文件名（包含时间戳）
      const timestamp = dayjs().format('YYYY-MM-DD_HH-mm-ss');
      link.download = `filtered_news_${timestamp}.csv`;
      
      // 触发下载
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      
      // 清理URL对象
      URL.revokeObjectURL(url);

      message.success(`Successfully exported ${state.filteredNewsData.length} news items to CSV.`);
    } catch (error) {
      console.error('Failed to export CSV:', error);
      message.error('Failed to export CSV. Please try again.');
    }
  };

  // 初始化数据
  useEffect(() => {
    if (isAuthenticated && user) {
      console.log('NewsPage: 用户已认证', { user: user.username, token: !!localStorage.getItem('access_token') });
      loadFilteredNews(true); // 初始加载时也加载筛选新闻（使用默认筛选条件）
      loadMetadata();
      loadPresets(); // 加载预设列表
    } else {
      console.log('NewsPage: 用户未认证或认证状态未初始化');
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isAuthenticated, user]);

  // 显示新闻详情（打开sidebar）
  const showNewsDetail = async (newsId: number) => {
    try {
      const newsDetail = await newsAPI.getNewsDetail(newsId);
      setState(prev => ({
        ...prev,
        selectedNews: newsDetail,
        sidebarVisible: true
      }));
    } catch (error) {
      console.error('Failed to get news detail:', error);
      message.error(`Failed to get news detail: ${getErrorMessage(error)}`);
    }
  };


  // 获取一句话总结（从 AI 分析中）
  const getOneSentenceSummary = (analysisResult: any): string | null => {
    if (!analysisResult) return null;
    
    // 尝试从 news_summary_v1 获取
    const summaryV1 = getAnalysisContent(analysisResult, 'news_summary_v1');
    if (summaryV1 && typeof summaryV1.content === 'string') {
      // 提取第一句话
      const match = summaryV1.content.match(/^[^\n]+/);
      return match ? match[0].trim() : null;
    }
    
    // 尝试从 news_cctv_summary_v1 获取
    const cctvSummary = getAnalysisContent(analysisResult, 'news_cctv_summary_v1');
    if (cctvSummary && typeof cctvSummary.content === 'string') {
      const match = cctvSummary.content.match(/^[^\n]+/);
      return match ? match[0].trim() : null;
    }
    
    return null;
  };

  // 获取市场影响评估
  const getMarketImpact = (analysisResult: any): { direction: string; score: number } | null => {
    const summaryResult = getAnalysisContent(analysisResult, 'news_summary_v1');
    if (!summaryResult || typeof summaryResult.content !== 'string') {
      return null;
    }
    
    const content = summaryResult.content;
    
    // 查找"市场影响评估"部分（支持中英文标题）
    const impactMatch = content.match(/\*\*(?:市场影响评估|Market Impact Assessment)\*\*([\s\S]*?)(?=\n\s*\d+\.\s*\*\*|$)/i);
    if (!impactMatch) return null;
    
    const impactText = impactMatch[1];
    
    // 提取方向 - 支持多种格式（中英文）
    let direction = '中性';
    // 中文格式1: "- 方向：中性偏正面"
    const directionMatch1 = impactText.match(/方向[：:]\s*(正面|中性偏正面|中性|中性偏负面|负面)/);
    // 中文格式2: "- 中性；影响程度" 或 "- 中性｜4分｜" (分号或竖线分隔)
    const directionMatch2 = impactText.match(/[-\s]*(正面|中性偏正面|中性偏负面|负面|中性)[；;｜|]/);
    // 中文格式3: 单独一行 "- 正面" 或 "- 负面"
    const directionMatch3 = impactText.match(/^[-\s]*(正面|中性偏正面|中性偏负面|负面|中性)\s*$/m);
    // 英文格式1: "Direction: Neutral/Positive/Negative"
    const directionMatch4 = impactText.match(/Direction[：:]\s*(Positive|Neutral|Negative)/i);
    // 英文格式2: "- Positive" / "- Negative" / "- Neutral"
    const directionMatch5 = impactText.match(/^[-\s]*(Positive|Neutral|Negative)\s*$/mi);
    
    if (directionMatch1) {
      direction = directionMatch1[1];
    } else if (directionMatch2) {
      direction = directionMatch2[1];
    } else if (directionMatch3) {
      direction = directionMatch3[1];
    } else if (directionMatch4) {
      // 将英文方向映射为中文
      const enDirection = directionMatch4[1].toLowerCase();
      if (enDirection === 'positive') direction = '正面';
      else if (enDirection === 'negative') direction = '负面';
      else direction = '中性';
    } else if (directionMatch5) {
      const enDirection = directionMatch5[1].toLowerCase();
      if (enDirection === 'positive') direction = '正面';
      else if (enDirection === 'negative') direction = '负面';
      else direction = '中性';
    }
    
    // 提取评分 - 支持多种格式
    let score = 0;
    // 格式1: "- 影响程度：4/5"
    const scoreMatch1 = impactText.match(/影响程度[：:]\s*(\d+)[/／]?5/);
    // 格式2: "- 程度：4分"
    const scoreMatch2 = impactText.match(/程度[：:]\s*(\d+)分/);
    // 格式3: "- 中性｜4分" (竖线分隔，支持中英文竖线)
    const scoreMatch3 = impactText.match(/[；;｜|]\s*(\d+)分/);
    // 格式4: "- 中性；4/5" (分号分隔)
    const scoreMatch4 = impactText.match(/[；;｜|]\s*(\d+)[/／]5/);
    // 英文格式1: "Impact Level: 4/5" 或 "Severity: 4/5"
    const scoreMatch5 = impactText.match(/(?:Impact Level|Severity)[：:]\s*(\d+)[/／]?5/i);
    // 英文格式2: "Score: 4"
    const scoreMatch6 = impactText.match(/Score[：:]\s*(\d+)/i);
    
    if (scoreMatch1) {
      score = parseInt(scoreMatch1[1], 10);
    } else if (scoreMatch2) {
      score = parseInt(scoreMatch2[1], 10);
    } else if (scoreMatch3) {
      score = parseInt(scoreMatch3[1], 10);
    } else if (scoreMatch4) {
      score = parseInt(scoreMatch4[1], 10);
    } else if (scoreMatch5) {
      score = parseInt(scoreMatch5[1], 10);
    } else if (scoreMatch6) {
      score = parseInt(scoreMatch6[1], 10);
    }
    
    // 如果没有找到评分但找到了方向，根据方向给一个默认分数
    if (score === 0 && direction) {
      if (direction.includes('正面') || direction.includes('负面')) {
        score = 3; // 默认中等影响
      }
    }
    
    return { direction, score };
  };

  // 获取市场影响颜色（支持 dark mode）
  const getMarketImpactColor = (direction: string) => {
    const lowerDirection = direction.toLowerCase();
    if (lowerDirection.includes('正面') || lowerDirection.includes('positive')) {
      return isDark
        ? { bg: 'rgba(16, 185, 129, 0.15)', text: '#34d399', border: 'rgba(16, 185, 129, 0.3)' }
        : { bg: '#d4edda', text: '#155724', border: '#c3e6cb' };
    }
    if (lowerDirection.includes('负面') || lowerDirection.includes('negative')) {
      return isDark
        ? { bg: 'rgba(239, 68, 68, 0.15)', text: '#f87171', border: 'rgba(239, 68, 68, 0.3)' }
        : { bg: '#f8d7da', text: '#721c24', border: '#f5c6cb' };
    }
    return isDark
      ? { bg: 'rgba(107, 114, 128, 0.2)', text: '#9ca3af', border: 'rgba(107, 114, 128, 0.3)' }
      : { bg: '#fff3cd', text: '#856404', border: '#ffeaa7' };
  };

  // 获取来源颜色（支持 dark mode）
  const getSourceColor = (source: string) => {
    const light: { [key: string]: string } = {
      finviz: '#1890ff',
      rss: '#52c41a',
      manual: '#722ed1',
      cctv: '#fa541c',
      default: '#8c8c8c',
    };
    const dark: { [key: string]: string } = {
      finviz: '#5dade2',
      rss: '#69db7c',
      manual: '#b794f6',
      cctv: '#f97316',
      default: '#94a3b8',
    };
    const palette = isDark ? dark : light;
    return palette[source?.toLowerCase()] || palette.default;
  };

  // 格式化时间为相对时间
  const formatTimeAgo = (dateStr: string) => {
    const date = dayjs(dateStr);
    const now = dayjs();
    const diffMinutes = now.diff(date, 'minute');
    const diffHours = now.diff(date, 'hour');
    const diffDays = now.diff(date, 'day');
    
    if (diffMinutes < 60) {
      return `${diffMinutes}m ago`;
    } else if (diffHours < 24) {
      return `${diffHours}h ago`;
    } else if (diffDays < 7) {
      return `${diffDays}d ago`;
    } else {
      return date.format('MMM D');
    }
  };

  // 获取 AI 分析内容用于 Tooltip
  const getAIAnalysisForTooltip = (analysisResult: any): string | null => {
    if (!analysisResult) return null;
    
    // 优先使用 news_summary_v1
    const summaryV1 = getAnalysisContent(analysisResult, 'news_summary_v1');
    if (summaryV1 && typeof summaryV1.content === 'string') {
      return summaryV1.content;
    }
    
    // 其次使用 news_cctv_summary_v1
    const cctvSummary = getAnalysisContent(analysisResult, 'news_cctv_summary_v1');
    if (cctvSummary && typeof cctvSummary.content === 'string') {
      return cctvSummary.content;
    }
    
    return null;
  };

  // 关键词高亮函数
  const highlightKeywords = (text: string): React.ReactNode => {
    if (!text || state.themes.length === 0) {
      return text;
    }

    // 收集所有关键词及其对应的主题颜色
    interface KeywordMatch {
      keyword: string;
      color: string;
      themeName: string;
    }
    
    const keywordMatches: KeywordMatch[] = [];
    
    state.themes.forEach(theme => {
      if (theme.keywords && theme.keywords.length > 0) {
        theme.keywords.forEach(keyword => {
          keywordMatches.push({
            keyword: keyword.trim(),
            color: theme.color || '#1890ff',
            themeName: theme.topic_name
          });
        });
      }
    });

    // 如果没有关键词，直接返回原文本
    if (keywordMatches.length === 0) {
      return text;
    }

    // 按关键词长度排序（长的优先），避免短词先匹配导致长词无法匹配
    keywordMatches.sort((a, b) => b.keyword.length - a.keyword.length);

    // 存储所有匹配位置和对应的主题信息
    interface MatchPosition {
      start: number;
      end: number;
      color: string;
      keyword: string;
      themeName: string;
    }

    const positions: MatchPosition[] = [];

    // 查找所有匹配位置
    keywordMatches.forEach(({ keyword, color, themeName }) => {
      if (!keyword) return;
      
      // 转义特殊字符，并使用单词边界 \b 进行精确匹配
      // \b 确保匹配的是完整的单词，不会匹配单词的一部分
      const escapedKeyword = keyword.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const regex = new RegExp(`\\b${escapedKeyword}\\b`, 'gi');
      let match;
      
      while ((match = regex.exec(text)) !== null) {
        const start = match.index;
        const end = start + match[0].length;
        
        // 检查是否与已有位置重叠
        const isOverlapping = positions.some(pos => 
          (start >= pos.start && start < pos.end) || 
          (end > pos.start && end <= pos.end) ||
          (start <= pos.start && end >= pos.end)
        );
        
        if (!isOverlapping) {
          positions.push({ start, end, color, keyword: match[0], themeName });
        }
      }
    });

    // 如果没有匹配，返回原文本
    if (positions.length === 0) {
      return text;
    }

    // 按开始位置排序
    positions.sort((a, b) => a.start - b.start);

    // 构建带高亮的文本
    const elements: React.ReactNode[] = [];
    let lastIndex = 0;

    positions.forEach((pos, index) => {
      // 添加高亮前的普通文本
      if (pos.start > lastIndex) {
        elements.push(
          <span key={`text-${index}`}>
            {text.substring(lastIndex, pos.start)}
          </span>
        );
      }

      // 添加高亮文本
      elements.push(
        <span
          key={`highlight-${index}`}
          style={{
            backgroundColor: pos.color,
            color: '#fff',
            padding: '2px 4px',
            borderRadius: '2px',
            fontWeight: 'bold'
          }}
          title={`Theme: ${pos.themeName}`}
        >
          {pos.keyword}
        </span>
      );

      lastIndex = pos.end;
    });

    // 添加剩余的文本
    if (lastIndex < text.length) {
      elements.push(
        <span key="text-end">
          {text.substring(lastIndex)}
        </span>
      );
    }

    return <>{elements}</>;
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
      <div style={{ padding: '12px 16px 32px', backgroundColor: colors.pageBg, minHeight: '100%' }}>
      <style>
        {`
          .ant-select-dropdown .ant-select-item {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-panel {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-header {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-content {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-cell {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-cell-inner {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-year-panel .ant-picker-cell-inner {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-month-panel .ant-picker-cell-inner {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-decade-panel .ant-picker-cell-inner {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-header-view {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-header-super-prev-btn,
          .ant-picker-dropdown .ant-picker-header-super-next-btn {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-header-prev-btn,
          .ant-picker-dropdown .ant-picker-header-next-btn {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-header-prev-btn .anticon,
          .ant-picker-dropdown .ant-picker-header-next-btn .anticon {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-today-btn {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-ok {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-ok .ant-btn {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-time-panel {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-time-panel .ant-picker-time-panel-cell {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-time-panel .ant-picker-time-panel-cell-inner {
            font-size: 12px !important;
          }
          .ant-select-selection-item {
            font-size: 12px !important;
          }
          .ant-select-selection-placeholder {
            font-size: 12px !important;
          }
          .ant-input::placeholder {
            font-size: 12px !important;
          }
          .ant-picker-input > input {
            font-size: 12px !important;
          }
          .ant-picker-separator {
            font-size: 12px !important;
          }
          .ant-picker-range {
            font-size: 12px !important;
          }
          .ant-picker-range .ant-picker-input {
            font-size: 12px !important;
          }
          .ant-picker-range .ant-picker-input > input {
            font-size: 12px !important;
          }
          .ant-picker-range .ant-picker-separator {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-panel-container {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-panel-container .ant-picker-panel {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-panel-container .ant-picker-panel .ant-picker-content {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-panel-container .ant-picker-panel .ant-picker-content .ant-picker-cell {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-panel-container .ant-picker-panel .ant-picker-content .ant-picker-cell .ant-picker-cell-inner {
            font-size: 12px !important;
          }
          .ant-input {
            height: 20px !important;
          }
          .ant-picker {
            height: 20px !important;
          }
          .ant-picker-input > input {
            height: 20px !important;
            line-height: 20px !important;
          }
          .ant-select {
            height: 20px !important;
          }
          .ant-select-selector {
            height: 20px !important;
            line-height: 20px !important;
          }
          .ant-select-selection-item {
            line-height: 20px !important;
          }
          .ant-select-selection-placeholder {
            line-height: 20px !important;
          }
          /* Tooltip 随主题（Behance 风格）：使用语义色 */
          .news-page-tooltip .ant-tooltip-inner {
            background-color: ${isDark ? colors.codeBlockBg : colors.cardBg} !important;
            color: ${colors.text} !important;
            border: 1px solid ${colors.cardBorder} !important;
            box-shadow: 0 4px 24px rgba(0, 0, 0, 0.12) !important;
          }
          .news-page-tooltip .ant-tooltip-inner,
          .news-page-tooltip .ant-tooltip-inner * {
            color: ${colors.text} !important;
          }
          .news-page-tooltip .ant-tooltip-arrow::before {
            background: ${isDark ? colors.codeBlockBg : colors.cardBg} !important;
            border: 1px solid ${colors.cardBorder} !important;
          }
          /* Slider marks字体大小 */
          .ant-slider-mark-text {
            font-size: 10px !important;
          }
        `}
      </style>
      
      {/* 主题筛选面板 */}
        <Row gutter={16} style={{ marginBottom: 16 }}>
          <Col span={24}>
            {/* 主题筛选模式下的新闻列表 */}
            <Card
              style={cardStyleBehance}
              bodyStyle={{ backgroundColor: colors.cardBg, padding: '20px 24px' }}
              headStyle={isDark ? { backgroundColor: colors.cardBg, borderBottomColor: colors.cardBorder, color: colors.text } : undefined}
              title={
                <Space>
                  <FilterOutlined />
                  <span style={{ color: colors.text }}>RC News</span>
                  <span
                    style={{
                      fontSize: '11px',
                      color: colors.selectedBorder,
                      backgroundColor: isDark ? 'rgba(33, 150, 243, 0.12)' : 'rgba(24, 144, 255, 0.1)',
                      borderRadius: '12px',
                      display: 'inline-flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      lineHeight: '1',
                      padding: '4px 8px',
                      minWidth: '24px',
                      height: '24px'
                    }}
                  >
                    {state.filteredTotal.toLocaleString()}
                  </span>
                </Space>
              }
              extra={
                <Space>
                  {/* 🔥 新增：搜索框 */}
                  <Search
                    placeholder="Search News by Title or Content..."
                    allowClear
                    enterButton={<SearchOutlined />}
                    size="small"
                    style={{ width: 400, ...(isDark && { backgroundColor: colors.inputBg }) }}
                    value={state.themeFilters.searchText || ''}
                    onChange={(e) => {
                      const searchText = e.target.value;
                      setState(prev => ({
                        ...prev,
                        themeFilters: {
                          ...prev.themeFilters,
                          searchText: searchText || undefined
                        }
                      }));
                    }}
                    onSearch={(value) => {
                      const searchText = value.trim();
                      if (searchText) {
                        message.info(`Searching for: ${searchText}`);
                      }
                      const newFilters = {
                        ...state.themeFilters,
                        searchText: searchText || undefined
                      };
                      setState(prev => ({
                        ...prev,
                        themeFilters: newFilters
                      }));
                      loadFilteredNews(true, newFilters);
                    }}
                    suffix={
                      state.themeFilters.searchText ? (
                        <CloseCircleOutlined
                          style={{ color: colors.textSecondary, cursor: 'pointer' }}
                          onClick={() => {
                            const newFilters = {
                              ...state.themeFilters,
                              searchText: undefined
                            };
                            setState(prev => ({
                              ...prev,
                              themeFilters: newFilters
                            }));
                            loadFilteredNews(true, newFilters);
                            message.info('Search cleared');
                          }}
                        />
                      ) : null
                    }
                  />
                  <Tooltip overlayClassName="news-page-tooltip" title="Presets Management">
                    <Button
                      icon={<BookOutlined />}
                      onClick={() => setState(prev => ({ ...prev, presetsManagerVisible: true }))}
                      size="small"
                    />
                  </Tooltip>
                  <Tooltip overlayClassName="news-page-tooltip" title="Advanced Filter">
                    <Button
                      icon={<ControlOutlined />}
                      onClick={() => setState(prev => ({ ...prev, advancedFilterModalVisible: true }))}
                      size="small"
                    />
                  </Tooltip>
                  <Tooltip overlayClassName="news-page-tooltip" title="Highlight Setting">
                    <Button
                      icon={<HighlightOutlined />}
                      onClick={() => setState(prev => ({ ...prev, themeManagementModalVisible: true }))}
                      size="small"
                    />
                  </Tooltip>
                  <Tooltip overlayClassName="news-page-tooltip" title="Export to CSV">
                    <Button
                      icon={<ExportOutlined />}
                      onClick={handleExportToCSV}
                      size="small"
                      disabled={state.filteredNewsData.length === 0}
                    />
                  </Tooltip>
                  {/* 删除新闻按钮 - 仅管理员可见 */}
                  {user?.is_admin && (
                    <Tooltip overlayClassName="news-page-tooltip" title="Delete News by Date Range (Admin Only)">
                      <Button
                        icon={<DeleteOutlined />}
                        onClick={() => setState(prev => ({ ...prev, deleteNewsModalVisible: true }))}
                        size="small"
                        danger
                      />
                    </Tooltip>
                  )}
                  {/* Height Setting */}
                  <Popover
                    content={
                      <div style={{ width: 200, fontSize: '10px' }}>
                        <div style={{ marginBottom: 8, fontSize: '10px' }}>
                          <Text strong style={{ fontSize: '10px' }}>News Content Height: {state.newsContentHeight}px</Text>
                        </div>
                        <Slider
                          min={20}
                          max={800}
                          value={state.newsContentHeight}
                          onChange={(value) => setState(prev => ({ ...prev, newsContentHeight: value }))}
                          marks={{
                            20: '20px',
                            200: '200px',
                            400: '400px',
                            800: '800px'
                          }}
                        />
                      </div>
                    }
                    title="Height Setting"
                    trigger="click"
                  >
                    <Tooltip overlayClassName="news-page-tooltip" title="Height Setting">
                      <Button
                        icon={<ColumnHeightOutlined />}
                        size="small"
                      />
                    </Tooltip>
                  </Popover>
                  {/* Watchlist开关 */}
                  <Tooltip overlayClassName="news-page-tooltip" title="Only show news from my watchlist">
                    <Switch
                      checked={state.themeFilters.useWatchlist || false}
                      onChange={(checked) => {
                        const newFilters = {
                          ...state.themeFilters,
                          useWatchlist: checked,
                          // 如果启用watchlist过滤，默认设置为include模式
                          tickerFilterMode: checked ? 'include' as const : state.themeFilters.tickerFilterMode,
                          // 如果关闭watchlist，清空相关设置
                          watchlistGroups: checked ? state.themeFilters.watchlistGroups : undefined
                        };
                        
                        setState(prev => ({ 
                          ...prev, 
                          themeFilters: newFilters 
                        }));
                        
                        // 自动应用过滤
                        if (checked) {
                          message.info('Loading news from your watchlist...');
                        } else {
                          message.info('Watchlist filter removed');
                        }
                        
                        // 重新加载过滤的新闻
                        loadFilteredNews(true, newFilters);
                      }}
                      checkedChildren={<StarFilled />}
                      unCheckedChildren={<StarOutlined />}
                    />
                  </Tooltip>
                </Space>
              }
            >
              {/* 🔥 新增：预设 Tags 展示 */}
              {state.presets.length > 0 && (
                <div style={{ 
                  marginBottom: '16px'
                }}>
                  <Space size={[6, 6]} wrap>
                    {/* 系统预设（左侧） */}
                    {state.presets
                      .filter(p => p.preset_type === 'system')
                      .map(preset => (
                        <Tag
                          key={preset.id}
                          color={preset.id === state.activePresetId ? 'blue' : 'default'}
                          style={{
                            cursor: 'pointer',
                            fontSize: '11px',
                            padding: '4px 8px',
                            margin: 0,
                            border: preset.id === state.activePresetId ? `2px solid ${colors.selectedBorder}` : `1px solid ${colors.cardBorder}`,
                            fontWeight: preset.id === state.activePresetId ? 600 : 400,
                            backgroundColor: preset.id === state.activePresetId ? (isDark ? 'rgba(33, 150, 243, 0.12)' : undefined) : (isDark ? colors.hoverBg : '#f0f0f0')
                          }}
                          onClick={() => handleApplyPreset(preset)}
                        >
                          <span>{preset.preset_name}</span>
                        </Tag>
                      ))}
                    
                    {/* 分隔线 */}
                    {state.presets.filter(p => p.preset_type === 'system').length > 0 && 
                     state.presets.filter(p => p.preset_type === 'custom').length > 0 && (
                      <div style={{ width: '1px', height: '20px', backgroundColor: colors.cardBorder, margin: '0 4px' }} />
                    )}
                    
                    {/* 收藏的自定义预设（右侧） */}
                    {state.presets
                      .filter(p => p.is_favorite && p.preset_type === 'custom')
                      .map(preset => (
                        <Tag
                          key={preset.id}
                          color={preset.id === state.activePresetId ? 'blue' : 'default'}
                          style={{
                            cursor: 'pointer',
                            fontSize: '11px',
                            padding: '4px 8px',
                            margin: 0,
                            border: preset.id === state.activePresetId ? `2px solid ${colors.selectedBorder}` : `1px solid ${colors.cardBorder}`,
                            fontWeight: preset.id === state.activePresetId ? 600 : 400,
                            backgroundColor: preset.id === state.activePresetId ? (isDark ? 'rgba(33, 150, 243, 0.12)' : undefined) : undefined
                          }}
                          onClick={() => handleApplyPreset(preset)}
                        >
                          <Space size={4}>
                            <StarFilled style={{ color: '#faad14', fontSize: '10px' }} />
                            <span>{preset.preset_name}</span>
                          </Space>
                        </Tag>
                      ))}
                    
                    {/* 其他自定义预设 */}
                    {state.presets
                      .filter(p => !p.is_favorite && p.preset_type === 'custom')
                      .slice(0, 5)
                      .map(preset => (
                        <Tag
                          key={preset.id}
                          color={preset.id === state.activePresetId ? 'blue' : 'default'}
                          style={{
                            cursor: 'pointer',
                            fontSize: '11px',
                            padding: '4px 8px',
                            margin: 0,
                            border: preset.id === state.activePresetId ? `2px solid ${colors.selectedBorder}` : `1px solid ${colors.cardBorder}`,
                            fontWeight: preset.id === state.activePresetId ? 600 : 400,
                            backgroundColor: preset.id === state.activePresetId ? (isDark ? 'rgba(33, 150, 243, 0.12)' : undefined) : undefined
                          }}
                          onClick={() => handleApplyPreset(preset)}
                        >
                          <span>{preset.preset_name}</span>
                        </Tag>
                      ))}
                  </Space>
                </div>
              )}

              {state.loading ? (
                <div style={{ textAlign: 'center', padding: '40px' }}>
                  <Skeleton active />
                </div>
              ) : state.filteredNewsData.length === 0 ? (
                <Empty description="No news found" />
              ) : (
                <>
                  {/* 新闻列表 - 紧凑卡片样式 */}
                  <div>
                    {state.filteredNewsData.map((item: FilteredNews) => {
                      const aiAnalysisContent = getAIAnalysisForTooltip(item.analysis_result);
                      
                      return (
                        <div
                          key={item.id}
                          style={{
                            padding: '12px 16px',
                            marginBottom: '8px',
                            borderRadius: 12,
                            border: `1px solid ${colors.cardBorder}`,
                            boxShadow: isDark ? '0 2px 8px rgba(0, 0, 0, 0.2)' : '0 2px 8px rgba(0, 0, 0, 0.06)',
                            cursor: 'pointer',
                            transition: 'background-color 0.2s ease, box-shadow 0.2s ease',
                            position: 'relative',
                            backgroundColor: colors.cardBg
                          }}
                          onClick={() => showNewsDetail(item.id)}
                          onMouseEnter={(e) => {
                            e.currentTarget.style.backgroundColor = colors.hoverBg;
                            e.currentTarget.style.boxShadow = isDark ? '0 4px 16px rgba(0, 0, 0, 0.3)' : '0 4px 16px rgba(0, 0, 0, 0.1)';
                          }}
                          onMouseLeave={(e) => {
                            e.currentTarget.style.backgroundColor = colors.cardBg;
                            e.currentTarget.style.boxShadow = isDark ? '0 2px 8px rgba(0, 0, 0, 0.2)' : '0 2px 8px rgba(0, 0, 0, 0.06)';
                          }}
                        >
                          {/* 标题 */}
                          <div style={{ fontSize: 14, marginBottom: 4, lineHeight: 1.4 }}>
                            {item.link ? (
                              <a
                                href={item.link}
                                target="_blank"
                                rel="noopener noreferrer"
                                style={{ color: colors.text, textDecoration: 'none' }}
                                onClick={(e) => e.stopPropagation()}
                                onMouseEnter={(e) => {
                                  if (aiAnalysisContent) {
                                    if (hideTooltipTimeoutRef.current) {
                                      clearTimeout(hideTooltipTimeoutRef.current);
                                      hideTooltipTimeoutRef.current = null;
                                    }
                                    setState(prev => ({
                                      ...prev,
                                      hoveredNewsId: item.id,
                                      mousePosition: { x: e.clientX, y: e.clientY }
                                    }));
                                  }
                                }}
                                onMouseMove={(e) => {
                                  if (aiAnalysisContent && state.hoveredNewsId === item.id) {
                                    setState(prev => ({
                                      ...prev,
                                      mousePosition: { x: e.clientX, y: e.clientY }
                                    }));
                                  }
                                }}
                                onMouseLeave={() => {
                                  if (hideTooltipTimeoutRef.current) {
                                    clearTimeout(hideTooltipTimeoutRef.current);
                                  }
                                  hideTooltipTimeoutRef.current = setTimeout(() => {
                                    setState(prev => ({ ...prev, hoveredNewsId: null }));
                                    hideTooltipTimeoutRef.current = null;
                                  }, 300);
                                }}
                              >
                                {item.title}
                              </a>
                            ) : (
                              <span 
                                style={{ color: colors.text }}
                                onMouseEnter={(e) => {
                                  if (aiAnalysisContent) {
                                    if (hideTooltipTimeoutRef.current) {
                                      clearTimeout(hideTooltipTimeoutRef.current);
                                      hideTooltipTimeoutRef.current = null;
                                    }
                                    setState(prev => ({
                                      ...prev,
                                      hoveredNewsId: item.id,
                                      mousePosition: { x: e.clientX, y: e.clientY }
                                    }));
                                  }
                                }}
                                onMouseMove={(e) => {
                                  if (aiAnalysisContent && state.hoveredNewsId === item.id) {
                                    setState(prev => ({
                                      ...prev,
                                      mousePosition: { x: e.clientX, y: e.clientY }
                                    }));
                                  }
                                }}
                                onMouseLeave={() => {
                                  if (hideTooltipTimeoutRef.current) {
                                    clearTimeout(hideTooltipTimeoutRef.current);
                                  }
                                  hideTooltipTimeoutRef.current = setTimeout(() => {
                                    setState(prev => ({ ...prev, hoveredNewsId: null }));
                                    hideTooltipTimeoutRef.current = null;
                                  }, 300);
                                }}
                              >
                                {item.title}
                              </span>
                            )}
                          </div>

                          {/* 一句话总结 - 仅有 AI 分析时显示 */}
                          {(() => {
                            const summary = getOneSentenceSummary(item.analysis_result);
                            if (!summary) return null;
                            return (
                              <div 
                                style={{ 
                                  fontSize: 12, 
                                  color: colors.textSecondary,
                                  marginBottom: 6,
                                  lineHeight: 1.5,
                                  paddingLeft: 2,
                                }}
                              >
                                {summary}
                              </div>
                            );
                          })()}

                          {/* Ticker标签 */}
                          {item.symbols && item.symbols.length > 0 && (
                            <div style={{ marginBottom: 6 }}>
                              <Space size={[4, 4]} wrap>
                                {item.symbols.map((symbol: string) => (
                                  <Tag key={symbol} color="blue" style={{ fontSize: '10px', margin: 0 }}>
                                    {symbol}
                                  </Tag>
                                ))}
                              </Space>
                            </div>
                          )}

                          {/* 元信息行：来源、时间、AI标识、市场影响 */}
                          <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                            <span style={{ color: getSourceColor(item.source), fontSize: 12 }}>
                              {newsUtils.getSourceDisplayName(item.source) || '—'}
                            </span>
                            <span style={{ color: colors.textSecondary, fontSize: 12 }}>
                              {formatTimeAgo(item.date)}
                            </span>
                            
                            {/* AI 分析标识 */}
                            {item.analysis_result && getAnalysisContent(item.analysis_result, 'news_summary_v1') && (
                              <span
                                style={{
                                  fontSize: 8,
                                  fontWeight: 600,
                                  padding: '0 3px',
                                  borderRadius: 2,
                                  background: isDark ? 'rgba(33, 150, 243, 0.12)' : 'rgba(24, 144, 255, 0.06)',
                                  color: colors.selectedBorder,
                                  border: `1px solid ${isDark ? 'rgba(33, 150, 243, 0.35)' : 'rgba(24, 144, 255, 0.35)'}`,
                                }}
                              >
                                AI
                              </span>
                            )}
                            
                            {/* 市场影响评估徽章 */}
                            {(() => {
                              const impact = getMarketImpact(item.analysis_result);
                              if (!impact) return null;
                              const impactColor = getMarketImpactColor(impact.direction);
                              // 简化方向显示
                              const shortDirection = impact.direction
                                .replace('中性偏正面', '偏正面')
                                .replace('中性偏负面', '偏负面');
                              return (
                                <span
                                  style={{
                                    fontSize: 8,
                                    padding: '0 3px',
                                    borderRadius: 2,
                                    backgroundColor: impactColor.bg,
                                    color: impactColor.text,
                                    border: `1px solid ${impactColor.border}`,
                                    fontWeight: 500,
                                    whiteSpace: 'nowrap',
                                  }}
                                >
                                  {shortDirection} {impact.score}/5
                                </span>
                              );
                            })()}
                          </div>
                        </div>
                      );
                    })}
                    
                    {/* 鼠标跟随的AI分析提示框 */}
                    {state.hoveredNewsId && (() => {
                      const hoveredItem = state.filteredNewsData.find(item => item.id === state.hoveredNewsId);
                      const aiAnalysisContent = hoveredItem ? getAIAnalysisForTooltip(hoveredItem.analysis_result) : null;
                      
                      if (!aiAnalysisContent) return null;
                      
                      // 智能定位计算
                      const tooltipWidth = 650;
                      const tooltipMaxHeight = 450;
                      const offset = 20;
                      const viewportWidth = window.innerWidth;
                      const viewportHeight = window.innerHeight;
                      
                      // 计算水平位置：优先显示在鼠标右侧，如果空间不够则显示在左侧
                      let left = state.mousePosition.x + offset;
                      if (left + tooltipWidth > viewportWidth - 20) {
                        left = state.mousePosition.x - tooltipWidth - offset;
                        if (left < 20) {
                          left = viewportWidth - tooltipWidth - 20;
                        }
                      }
                      
                      // 计算垂直位置：优先显示在鼠标下方，如果空间不够则显示在上方
                      let top = state.mousePosition.y + 10;
                      if (top + tooltipMaxHeight > viewportHeight - 20) {
                        top = state.mousePosition.y - tooltipMaxHeight - 10;
                        if (top < 20) {
                          top = viewportHeight - tooltipMaxHeight - 20;
                        }
                      }
                      
                      return (
                        <div
                          style={{
                            position: 'fixed',
                            left: Math.max(20, left),
                            top: Math.max(20, top),
                            width: tooltipWidth,
                            maxHeight: tooltipMaxHeight,
                            overflow: 'auto',
                            backgroundColor: colors.cardBg,
                            border: `1px solid ${colors.cardBorder}`,
                            boxShadow: isDark ? '0 8px 32px rgba(0, 0, 0, 0.4)' : '0 4px 24px rgba(0, 0, 0, 0.12)',
                            padding: '12px 16px',
                            borderRadius: 8,
                            zIndex: 9999,
                            pointerEvents: 'auto',
                            fontSize: '9pt',
                            color: colors.text,
                          }}
                          onMouseEnter={() => {
                            if (hideTooltipTimeoutRef.current) {
                              clearTimeout(hideTooltipTimeoutRef.current);
                              hideTooltipTimeoutRef.current = null;
                            }
                          }}
                          onMouseLeave={() => {
                            if (hideTooltipTimeoutRef.current) {
                              clearTimeout(hideTooltipTimeoutRef.current);
                              hideTooltipTimeoutRef.current = null;
                            }
                            setState(prev => ({ ...prev, hoveredNewsId: null }));
                          }}
                        >
                          <div style={{ 
                            fontWeight: 600, 
                            marginBottom: 8, 
                            fontSize: '10pt',
                            color: colors.text,
                            borderBottom: `1px solid ${colors.cardBorder}`,
                            paddingBottom: 6
                          }}>
                            AI Analysis
                          </div>
                          <div style={{ fontSize: '9pt', lineHeight: '1.6' }}>
                            <MarkdownRenderer 
                              content={aiAnalysisContent}
                              variant={isDark ? 'dark' : 'light'}
                            />
                          </div>
                        </div>
                      );
                    })()}
                  </div>

                  {/* Load More 按钮 */}
                  {state.hasMoreNews && (
                    <div style={{ textAlign: 'center', marginTop: '24px', marginBottom: '24px' }}>
                      <Button 
                        type="primary" 
                        loading={state.loadingMore}
                        onClick={loadMoreNews}
                        disabled={state.loadingMore}
                      >
                        {state.loadingMore ? 'Loading...' : 'Load More News'}
                      </Button>
                    </div>
                  )}
                  
                  {/* 没有更多数据提示 */}
                  {!state.hasMoreNews && state.filteredNewsData.length > 0 && (
                    <div style={{ textAlign: 'center', marginTop: '24px', marginBottom: '24px' }}>
                      <Text style={{ color: colors.textSecondary }}>All news loaded</Text>
                    </div>
                  )}
                </>
              )}
            </Card>
          </Col>
        </Row>

        {/* AI Analysis Details 弹窗 - 样式与 FinvizDashboard 一致 */}
        <Modal
          title={
            <Space>
              {state.selectedNews?.analysis_result && (
                <span
                  style={{
                    fontSize: 10,
                    fontWeight: 600,
                    padding: '1px 4px',
                    borderRadius: 2,
                    background: isDark ? 'rgba(33, 150, 243, 0.12)' : 'rgba(24, 144, 255, 0.06)',
                    color: colors.selectedBorder,
                    border: `1px solid ${isDark ? 'rgba(33, 150, 243, 0.35)' : 'rgba(24, 144, 255, 0.35)'}`,
                  }}
                >
                  AI
                </span>
              )}
              <span style={{ color: colors.text }}>
                {state.selectedNews?.analysis_result ? 'AI Analysis Details' : 'News Details'}
              </span>
            </Space>
          }
          open={state.sidebarVisible}
          centered
          onCancel={() => setState(prev => ({ ...prev, sidebarVisible: false, selectedNews: null }))}
          footer={[
            <Button
              key="close"
              onClick={() => setState(prev => ({ ...prev, sidebarVisible: false, selectedNews: null }))}
              style={isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : undefined}
            >
              Close
            </Button>,
            state.selectedNews?.link ? (
              <Button
                key="original"
                type="primary"
                style={{
                  borderRadius: 10,
                  fontWeight: 500,
                  ...(isDark ? { background: '#2196F3', borderColor: '#2196F3' } : {}),
                }}
                onClick={() => window.open(state.selectedNews?.link, '_blank')}
              >
                View Original
              </Button>
            ) : null,
          ].filter(Boolean)}
          width={800}
          className="analysis-details-modal"
          styles={{
            ...getModalStyles(themeMode),
            body: {
              ...getModalStyles(themeMode).body,
              height: ANALYSIS_MODAL_BODY_HEIGHT,
              minHeight: '480px',
              display: 'flex',
              flexDirection: 'column',
              overflow: 'hidden',
            },
            ...(isDark ? {
              wrapper: { backdropFilter: 'brightness(0.7)' },
              content: {
                backgroundColor: MODAL_DARK_BG,
                border: '1px solid rgba(255, 255, 255, 0.06)',
                boxShadow: '0 8px 32px rgba(0,0,0,0.5), 0 0 0 1px rgba(255,255,255,0.06)',
              },
              header: {
                backgroundColor: MODAL_DARK_BG,
                borderBottomColor: 'rgba(255, 255, 255, 0.06)',
              },
            } : {}),
          }}
        >
          {!state.selectedNews ? (
            <Empty description="Select a news item to view details" image={Empty.PRESENTED_IMAGE_SIMPLE} style={{ marginTop: '100px' }} />
          ) : (
            (() => {
              const item = state.selectedNews;
              const isDarkModal = isDark;
              const modalCardBg = isDarkModal ? colors.cardBg : '#f5f5f5';
              const modalHeadBg = isDarkModal ? 'rgba(22, 119, 255, 0.2)' : '#e6f7ff';
              const modalHeadBgAlt = isDarkModal ? colors.siderBg : '#f5f5f5';
              const modalHeadBgAlt2 = isDarkModal ? colors.siderBg : '#f0f5ff';
              const modalText = colors.text;
              const modalTextSecondary = colors.textSecondary;
              const modalSectionBg = isDarkModal ? colors.siderBg : '#fafafa';
              const modalBoxBg = isDarkModal ? colors.cardBg : '#fff';
              const modalBorder = colors.cardBorder;
              const modalLabelColor = colors.textSecondary;
              const modalMetaColor = colors.textSecondary;
              const modalContentColor = colors.text;
              const tabContentStyle = { padding: '4px 0', height: '100%', overflow: 'auto' as const };
              const tagLabelStyle = { fontSize: '11px', color: modalLabelColor, display: 'block' as const, marginBottom: '10px', fontWeight: 500 };
              const tagBoxStyle = { padding: '12px', backgroundColor: modalBoxBg, borderRadius: '6px', border: `1px solid ${modalBorder}` };
              const tagItemStyle = { fontSize: '11px', padding: '4px 10px', borderRadius: '4px', fontWeight: 500, border: 'none', boxShadow: '0 1px 2px rgba(0,0,0,0.1)' };

              const allResults = getAllAnalysisResults(item.analysis_result);
              const hasAnyAnalysis = Object.keys(allResults).length > 0;
              // 使用首选 summary 模板（CCTV 用 news_cctv_summary_v1，其他用 news_summary_v1 等）
              const preferredSummary = getPreferredSummaryTemplate(item.analysis_result);
              const summaryResult = preferredSummary ? preferredSummary.result : null;

              return (
                <div style={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
                  <Card size="small" style={{ flexShrink: 0, marginBottom: '16px', backgroundColor: modalCardBg, borderColor: modalBorder }}>
                    <Typography.Title level={5} style={{ marginBottom: '12px', color: modalText }}>
                      {item.title}
                    </Typography.Title>
                    <Space size="small" wrap>
                      <Text style={{ fontSize: '12px', color: modalTextSecondary }}>
                        <CalendarOutlined /> {item.date ? new Date(item.date).toLocaleString() : 'N/A'}
                      </Text>
                      <Text style={{ fontSize: '12px', color: modalTextSecondary }}>
                        Source: {newsUtils.getSourceDisplayName(item.source) || 'N/A'}
                      </Text>
                    </Space>
                  </Card>

                  {hasAnyAnalysis ? (
                    <Tabs
                      defaultActiveKey="ai"
                      size="small"
                      style={{ marginTop: '4px', flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}
                      items={[
                        {
                          key: 'ai',
                          label: 'AI Analysis',
                          children:
                            summaryResult && typeof summaryResult.content === 'string' ? (
                              <div style={tabContentStyle}>
                                <MarkdownRenderer
                                  content={summaryResult.content}
                                  variant={isDarkModal ? 'dark' : 'light'}
                                  style={{ fontSize: '12px', lineHeight: '1.6', color: modalText }}
                                />
                              </div>
                            ) : (
                              <Empty description="No AI analysis" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                            ),
                        },
                        {
                          key: 'original',
                          label: 'Original Content',
                          children: item.content ? (
                            <div style={{ fontSize: '13px', color: modalContentColor, lineHeight: '1.8', ...tabContentStyle }}>
                              {highlightKeywords(item.content)}
                            </div>
                          ) : (
                            <Empty description="No content" image={Empty.PRESENTED_IMAGE_SIMPLE} />
                          ),
                        },
                        {
                          key: 'tags',
                          label: 'Tags',
                          children: (
                            <div style={tabContentStyle}>
                              <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                                {item.symbols && item.symbols.length > 0 && (
                                  <div style={tagBoxStyle}>
                                    <Text strong style={tagLabelStyle}>Related Tickers</Text>
                                    <Space size={[6, 6]} wrap>
                                      {item.symbols.map(symbol => (
                                        <Tag key={symbol} color="blue" style={tagItemStyle}>{symbol}</Tag>
                                      ))}
                                    </Space>
                                  </div>
                                )}
                                {(() => {
                                  const classification = getClassificationFromAnalysis(item.analysis_result);
                                  if (!classification || !hasValidClassification(classification)) return null;
                                  const tags = getClassificationTags(classification);
                                  const relatedSectors = getRelatedSectors(classification);
                                  const relatedCountries = getRelatedCountries(classification);
                                  return (
                                    <>
                                      {tags.length > 0 && (
                                        <div style={{ padding: '12px', backgroundColor: modalBoxBg, borderRadius: '6px', border: `1px solid ${modalBorder}` }}>
                                          <Text strong style={{ fontSize: '11px', color: modalLabelColor, display: 'block', marginBottom: '10px', fontWeight: 500 }}>Classification Tags</Text>
                                          <Space size={[6, 6]} wrap>
                                            {tags.map(({ key, config, displayValue }) => (
                                              <Tag key={key} color={config.color} style={{ fontSize: '11px', padding: '4px 10px', borderRadius: '4px', fontWeight: 500, border: 'none', boxShadow: '0 1px 2px rgba(0,0,0,0.1)' }} title={`${config.label}: ${displayValue}`}>{displayValue}</Tag>
                                            ))}
                                          </Space>
                                        </div>
                                      )}
                                      {relatedSectors.length > 0 && (
                                        <div style={{ padding: '12px', backgroundColor: modalBoxBg, borderRadius: '6px', border: `1px solid ${modalBorder}` }}>
                                          <Text strong style={{ fontSize: '11px', color: modalLabelColor, display: 'block', marginBottom: '10px', fontWeight: 500 }}>Related Sectors</Text>
                                          <Space size={[6, 6]} wrap>
                                            {relatedSectors.map(sector => (
                                              <Tag key={sector} color="purple" style={{ fontSize: '11px', padding: '4px 10px', borderRadius: '4px', fontWeight: 500, border: 'none', boxShadow: '0 1px 2px rgba(0,0,0,0.1)' }}>{sector}</Tag>
                                            ))}
                                          </Space>
                                        </div>
                                      )}
                                      {relatedCountries.length > 0 && (
                                        <div style={{ padding: '12px', backgroundColor: modalBoxBg, borderRadius: '6px', border: `1px solid ${modalBorder}` }}>
                                          <Text strong style={{ fontSize: '11px', color: modalLabelColor, display: 'block', marginBottom: '10px', fontWeight: 500 }}>Related Countries</Text>
                                          <Space size={[6, 6]} wrap>
                                            {relatedCountries.map(country => (
                                              <Tag key={country} color="geekblue" style={{ fontSize: '11px', padding: '4px 10px', borderRadius: '4px', fontWeight: 500, border: 'none', boxShadow: '0 1px 2px rgba(0,0,0,0.1)' }} title={country}>{COUNTRIES_MAP[country] || country}</Tag>
                                            ))}
                                          </Space>
                                        </div>
                                      )}
                                    </>
                                  );
                                })()}
                                {(() => {
                                  if (!hasValidUnifiedThemes(item.unified_themes)) return null;
                                  const groupedThemes = groupThemesByCanonicalName(item.unified_themes || []);
                                  if (groupedThemes.size === 0) return null;
                                  return (
                                    <div style={{ padding: '12px', backgroundColor: modalBoxBg, borderRadius: '6px', border: `1px solid ${modalBorder}` }}>
                                      <div style={{ marginBottom: '10px' }}>
                                        <Text strong style={{ fontSize: '11px', color: modalLabelColor, fontWeight: 500, marginRight: '8px' }}>Unified Themes</Text>
                                        <Tag color="cyan" style={{ fontSize: '10px', padding: '2px 8px', borderRadius: '4px', fontWeight: 500, border: 'none' }}>Multi-Source</Tag>
                                      </div>
                                      <Space size={[6, 6]} wrap>
                                        {Array.from(groupedThemes.entries()).map(([canonicalName, themes]) => {
                                          const displayInfo = getBestThemeDisplay(themes);
                                          const qualityColor = getQualityScoreColor(displayInfo.maxQualityScore);
                                          return (
                                            <Tag
                                              key={canonicalName}
                                              color={qualityColor}
                                              style={{ fontSize: '11px', padding: '4px 10px', borderRadius: '4px', fontWeight: 500, border: 'none', boxShadow: '0 1px 2px rgba(0,0,0,0.1)', cursor: 'help' }}
                                              title={`${displayInfo.displayName}${displayInfo.description ? '\n' + displayInfo.description : ''}\n\nQuality: ${formatQualityScore(displayInfo.maxQualityScore)}\nConfidence: ${formatQualityScore(displayInfo.avgConfidence)}\nSources: ${displayInfo.sources.map(s => getSourceTypeConfig(s).label).join(', ')}${displayInfo.markets.length > 0 ? '\nMarkets: ' + displayInfo.markets.join(', ') : ''}${displayInfo.isVerified ? '\n✓ Verified' : ''}${displayInfo.isPrimary ? '\n⭐ Primary' : ''}`}
                                            >
                                              {displayInfo.displayName}
                                              {displayInfo.isVerified && <span style={{ marginLeft: '4px' }}>✓</span>}
                                              {displayInfo.isPrimary && <span style={{ marginLeft: '4px' }}>⭐</span>}
                                              {displayInfo.sources.length > 1 && <Badge count={displayInfo.sources.length} style={{ backgroundColor: '#52c41a', fontSize: '8px', height: '14px', lineHeight: '14px', minWidth: '14px', marginLeft: '4px' }} />}
                                            </Tag>
                                          );
                                        })}
                                      </Space>
                                      <div style={{ marginTop: '12px', paddingTop: '12px', borderTop: `1px solid ${modalBorder}` }}>
                                        <Text style={{ fontSize: '10px', color: modalMetaColor, marginRight: '8px' }}>Sources:</Text>
                                        <Space size={[4, 4]} wrap>
                                          {Array.from(new Set(item.unified_themes?.map(t => t.source_type) || [])).map(sourceType => {
                                            const config = getSourceTypeConfig(sourceType);
                                            return <Tag key={sourceType} color={config.color} style={{ fontSize: '10px', padding: '2px 8px', borderRadius: '4px', border: 'none' }}>{config.icon} {config.label}</Tag>;
                                          })}
                                        </Space>
                                      </div>
                                    </div>
                                  );
                                })()}
                              </Space>
                            </div>
                          ),
                        },
                        {
                          key: 'meta',
                          label: 'Metadata',
                          children: (
                            <div style={tabContentStyle}>
                              <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                                {summaryResult && (
                                  <div style={tagBoxStyle}>
                                    <Text strong style={tagLabelStyle}>AI 模型信息</Text>
                                    <Space size="small" style={{ fontSize: '12px', color: modalMetaColor }} wrap>
                                      {summaryResult.provider && <span>Provider: {summaryResult.provider}</span>}
                                      {summaryResult.model && <span>Model: {summaryResult.model}</span>}
                                      {summaryResult.tokens && <span>Tokens: {summaryResult.tokens.toLocaleString()}</span>}
                                      {summaryResult.temperature && <span>Temperature: {summaryResult.temperature}</span>}
                                    </Space>
                                  </div>
                                )}
                                {(() => {
                                  const classification = getClassificationFromAnalysis(item.analysis_result);
                                  if (!classification || !hasValidClassification(classification)) return null;
                                  const summary = getClassificationSummary(classification);
                                  if (!summary || (summary.confidence.theme === 0 && summary.confidence.event === 0)) return null;
                                  return (
                                    <div style={tagBoxStyle}>
                                      <Text strong style={tagLabelStyle}>Confidence</Text>
                                      <Space size="middle" wrap>
                                        {summary.confidence.theme > 0 && <Tag color="orange" style={tagItemStyle}>Theme: {(summary.confidence.theme * 100).toFixed(0)}%</Tag>}
                                        {summary.confidence.event > 0 && <Tag color="orange" style={tagItemStyle}>Event: {(summary.confidence.event * 100).toFixed(0)}%</Tag>}
                                      </Space>
                                    </div>
                                  );
                                })()}
                                {Object.keys(allResults).length > 0 && (
                                  <div style={tagBoxStyle}>
                                    <Text strong style={tagLabelStyle}>Analysis Template</Text>
                                    <Space size={[6, 6]} wrap>
                                      {Object.keys(allResults).map(templateCode => (
                                        <Tag key={templateCode} color="green" style={tagItemStyle}>{templateCode}</Tag>
                                      ))}
                                    </Space>
                                  </div>
                                )}
                                {Object.entries(allResults).map(([templateCode, result]) => {
                                  if (templateCode === 'news_summary_v1' && typeof result.content === 'string') return null;
                                  if (templateCode === 'news_classification') return null;
                                  return (
                                    <Card key={templateCode} title={templateCode} size="small" style={{ marginTop: '8px', borderColor: modalBorder }} headStyle={{ backgroundColor: modalHeadBgAlt2, fontWeight: 'bold', color: modalText, borderColor: modalBorder }}>
                                      <pre style={{ fontSize: '12px', whiteSpace: 'pre-wrap', wordBreak: 'break-word', margin: 0, color: modalText }}>{typeof result.content === 'string' ? result.content : JSON.stringify(result.content, null, 2)}</pre>
                                      <div style={{ marginTop: '12px', fontSize: '10px', color: modalMetaColor }}>
                                        <Space size="small" wrap>
                                          {result.provider && <span>Provider: {result.provider}</span>}
                                          {result.model && <span>Model: {result.model}</span>}
                                          {result.tokens && <span>Tokens: {result.tokens.toLocaleString()}</span>}
                                          {result.temperature && <span>Temperature: {result.temperature}</span>}
                                        </Space>
                                      </div>
                                    </Card>
                                  );
                                })}
                              </Space>
                            </div>
                          ),
                        },
                      ]}
                    />
                  ) : (
                    <Card title="News Content" size="small" style={{ borderColor: modalBorder }} headStyle={{ backgroundColor: modalHeadBgAlt, fontWeight: 'bold', color: modalText, borderColor: modalBorder }}>
                      <div style={{ fontSize: '14px', lineHeight: '1.8', color: modalText }}>
                        {item.content ? highlightKeywords(item.content) : 'No content available'}
                      </div>
                    </Card>
                  )}
                </div>
              );
            })()
          )}
        </Modal>

      {/* 🔥 新增：高级筛选对话框 */}
      <Modal
        title={
          <Space>
            <FilterOutlined />
            <span>Advanced Filter</span>
          </Space>
        }
        open={state.advancedFilterModalVisible}
        onCancel={() => setState(prev => ({ ...prev, advancedFilterModalVisible: false }))}
        footer={null}
        width={600}
        destroyOnClose
        styles={getModalStyles(themeMode)}
      >
        <ThemeFilterPanel
          onFilterChange={handleThemeFilterChange}
          onApply={(filters) => {
            handleApplyThemeFilter(filters);
            setState(prev => ({ ...prev, advancedFilterModalVisible: false, activePresetId: null }));
          }}
          onSaveAsPreset={() => {
            setState(prev => ({ 
              ...prev, 
              advancedFilterModalVisible: false,
              presetsManagerVisible: true 
            }));
          }}
          loading={state.loading}
        />
      </Modal>


      {/* 主题管理模态框 */}
      <ThemeManagementModal
        open={state.themeManagementModalVisible}
        onCancel={() => setState(prev => ({ ...prev, themeManagementModalVisible: false }))}
        onSuccess={() => {
          loadMetadata(); // 刷新主题数据
        }}
      />

      {/* 🔥 预设管理对话框 */}
      <PresetsManager
        visible={state.presetsManagerVisible}
        onClose={() => {
          setState(prev => ({ ...prev, presetsManagerVisible: false }));
          loadPresets(); // 关闭时重新加载预设列表
        }}
      />

      {/* 删除新闻Modal */}
      <DeleteNewsByDateModal
        visible={state.deleteNewsModalVisible}
        onClose={() => setState(prev => ({ ...prev, deleteNewsModalVisible: false }))}
        onSuccess={() => {
          // 删除成功后刷新新闻列表
          loadFilteredNews(true);
        }}
      />
      </div>
    </ConfigProvider>
  );
};

export default NewsPage;