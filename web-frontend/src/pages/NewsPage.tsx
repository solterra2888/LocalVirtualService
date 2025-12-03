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
  Skeleton
} from 'antd';
import {
  CalendarOutlined,
  RobotOutlined,
  FilterOutlined,
  ExportOutlined,
  SettingOutlined,
  StarFilled
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
import { useAuthStore } from '../store/authStore';
import { getErrorMessage } from '../utils/errorHandler';
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
import type { ThemeFilterState, FilteredNews } from '../types/themes';
import type { NewsFilterPreset, CreatePresetRequest, FilterConfig } from '../types/newsPresets';

const { Text } = Typography;

interface NewsPageState {
  loading: boolean;
  newsData: NewsListResponse | null;
  topics: NewsTopic[];
  symbols: NewsSymbol[];
  searchParams: NewsSearchParams;
  selectedNews: NewsItem | null;
  detailModalVisible: boolean;
  themeManagementModalVisible: boolean;
  expandedNewsItems: Set<number>;
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
}

const NewsPage: React.FC = () => {
  const { user, isAuthenticated } = useAuthStore();
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
    detailModalVisible: false,
    themeManagementModalVisible: false,
    expandedNewsItems: new Set(),
    // 主题筛选相关
    themeFilters: {
      categories: [],
      eventTypes: [],
      impactScopes: [],
      urgencyLevels: [],
      relatedSectors: [],
      relatedCountries: [],
      sourceTypes: [],
      schedulerSources: [],  // 🔥 新增
      minConfidence: 0.5,
      onlyPrimary: false,
      dateRange: null,
      market: null
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
    presetsLoading: false
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
      // 构建请求参数（使用 offset 而不是 page）
      const request = {
        categories: activeFilters.categories.length > 0 ? activeFilters.categories : undefined,
        event_types: activeFilters.eventTypes.length > 0 ? activeFilters.eventTypes : undefined,
        impact_scopes: activeFilters.impactScopes.length > 0 ? activeFilters.impactScopes : undefined,
        urgency_levels: activeFilters.urgencyLevels.length > 0 ? activeFilters.urgencyLevels : undefined,
        related_sectors: activeFilters.relatedSectors.length > 0 ? activeFilters.relatedSectors : undefined,
        related_countries: activeFilters.relatedCountries.length > 0 ? activeFilters.relatedCountries : undefined,
        source_types: activeFilters.sourceTypes.length > 0 ? activeFilters.sourceTypes : undefined,
        scheduler_sources: activeFilters.schedulerSources.length > 0 ? activeFilters.schedulerSources : undefined,  // 🔥 新增
        min_confidence: activeFilters.minConfidence,
        only_primary: activeFilters.onlyPrimary,
        start_date: activeFilters.dateRange?.[0]?.format('YYYY-MM-DD'),
        end_date: activeFilters.dateRange?.[1]?.format('YYYY-MM-DD'),
        market: activeFilters.market || undefined,
        page: Math.floor(offset / pageSize) + 1,
        page_size: pageSize
      };
      
      const response = await themesAPI.filterNews(request);
      const loadTime = Date.now() - startTime;
      
      console.log('✅ [主题筛选] 筛选新闻加载成功', {
        total: response.total,
        page: response.page,
        page_size: response.page_size,
        total_pages: response.total_pages,
        news_count: response.news?.length || 0,
        load_time: `${loadTime}ms`,
        reset
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
    const newFilters: ThemeFilterState = {
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
      market: config.market || null
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

  // 显示新闻详情
  const showNewsDetail = async (newsId: number) => {
    try {
      const newsDetail = await newsAPI.getNewsDetail(newsId);
      setState(prev => ({
        ...prev,
        selectedNews: newsDetail,
        detailModalVisible: true
      }));
    } catch (error) {
      console.error('Failed to get news detail:', error);
      message.error(`Failed to get news detail: ${getErrorMessage(error)}`);
    }
  };

  // 新闻展开/收缩功能
  const toggleNewsExpansion = (newsId: number) => {
    setState(prev => {
      const newSet = new Set(prev.expandedNewsItems);
      if (newSet.has(newsId)) {
        newSet.delete(newsId);
      } else {
        newSet.add(newsId);
      }
      return { ...prev, expandedNewsItems: newSet };
    });
  };

  const isNewsExpanded = (newsId: number) => state.expandedNewsItems.has(newsId);

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
    <div style={{ padding: '24px' }}>
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
          /* Tooltip样式覆盖 */
          .ant-tooltip-inner {
            background-color: rgba(255, 255, 255, 0.95) !important;
            color: #333 !important;
            border: 1px solid #d9d9d9 !important;
            box-shadow: 0 3px 6px -4px rgba(0, 0, 0, 0.12), 0 6px 16px 0 rgba(0, 0, 0, 0.08), 0 9px 28px 8px rgba(0, 0, 0, 0.05) !important;
          }
          .ant-tooltip-arrow::before {
            background-color: rgba(255, 255, 255, 0.95) !important;
            border: 1px solid #d9d9d9 !important;
          }
        `}
      </style>
      {/* 主题筛选面板 */}
        <Row gutter={16} style={{ marginBottom: 16 }}>
          <Col span={24}>
            {/* 主题筛选模式下的新闻列表 */}
            <Card
              title={
                <Space>
                  <FilterOutlined />
                  <span>RC News</span>
                  <span
                    style={{
                      fontSize: '11px',
                      color: '#1890ff',
                      backgroundColor: 'rgba(24, 144, 255, 0.1)',
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
                  <Button
                    icon={<SettingOutlined />}
                    onClick={() => setState(prev => ({ ...prev, presetsManagerVisible: true }))}
                    size="small"
                  >
                    Presets Management
                  </Button>
                  <Button
                    icon={<SettingOutlined />}
                    onClick={() => setState(prev => ({ ...prev, advancedFilterModalVisible: true }))}
                    size="small"
                  >
                    Advanced Filter
                  </Button>
                  <Button
                    icon={<FilterOutlined />}
                    onClick={() => setState(prev => ({ ...prev, themeManagementModalVisible: true }))}
                    size="small"
                  >
                    Highlight Setting
                  </Button>
                  <Button
                    icon={<ExportOutlined />}
                    onClick={handleExportToCSV}
                    size="small"
                    disabled={state.filteredNewsData.length === 0}
                  >
                    Export
                  </Button>
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
                            border: preset.id === state.activePresetId ? '2px solid #1890ff' : '1px solid #d9d9d9',
                            fontWeight: preset.id === state.activePresetId ? 600 : 400,
                            backgroundColor: preset.id === state.activePresetId ? undefined : '#f0f0f0'
                          }}
                          onClick={() => handleApplyPreset(preset)}
                        >
                          <span>{preset.preset_name}</span>
                        </Tag>
                      ))}
                    
                    {/* 分隔线 */}
                    {state.presets.filter(p => p.preset_type === 'system').length > 0 && 
                     state.presets.filter(p => p.preset_type === 'custom').length > 0 && (
                      <div style={{ width: '1px', height: '20px', backgroundColor: '#d9d9d9', margin: '0 4px' }} />
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
                            border: preset.id === state.activePresetId ? '2px solid #1890ff' : '1px solid #d9d9d9',
                            fontWeight: preset.id === state.activePresetId ? 600 : 400
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
                            border: preset.id === state.activePresetId ? '2px solid #1890ff' : '1px solid #d9d9d9',
                            fontWeight: preset.id === state.activePresetId ? 600 : 400
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
                  <div style={{ position: 'relative' }}>
                    {state.filteredNewsData.map((item: FilteredNews, index: number) => (
                      <div key={item.id} style={{ position: 'relative', marginBottom: '24px' }}>
                        {/* Timeline line */}
                        {index < state.filteredNewsData.length - 1 && (
                          <div
                            style={{
                              position: 'absolute',
                              left: '40px',
                              top: '60px',
                              bottom: '-24px',
                              width: '2px',
                              backgroundColor: '#d9d9d9',
                              zIndex: 1
                            }}
                          />
                        )}
                        
                        {/* Timeline dot */}
                        <div
                          style={{
                            position: 'absolute',
                            left: '36px',
                            top: '22px',
                            width: '8px',
                            height: '8px',
                            borderRadius: '50%',
                            backgroundColor: '#1890ff',
                            border: '1px solid #fff',
                            boxShadow: '0 0 0 1px #1890ff',
                            zIndex: 2
                          }}
                        />
                        
                        {/* Content */}
                        <div style={{ marginLeft: '60px', paddingLeft: '16px' }}>
                          <List.Item>
                            <List.Item.Meta
                              avatar={
                                <Text
                                  type="secondary"
                                  style={{
                                    fontSize: '10px',
                                    display: 'flex',
                                    flexWrap: 'wrap',
                                    alignItems: 'center',
                                    gap: '4px',
                                  }}
                                >
                                  {(() => {
                                    const segments = [
                                      <span
                                        key="date"
                                        style={{ display: 'inline-flex', alignItems: 'center', gap: '4px' }}
                                      >
                                        <CalendarOutlined />{' '}
                                        {item.date ? dayjs(item.date).format('YYYY-MM-DD HH:mm:ss') : 'N/A'}
                                      </span>,
                                      item.updated_at && (
                                        <span key="updated">
                                          Updated: {dayjs(item.updated_at).format('YYYY-MM-DD HH:mm:ss')}
                                        </span>
                                      ),
                                      item.source && (
                                        <span key="source">{newsUtils.getSourceDisplayName(item.source)}</span>
                                      ),
                                    ].filter(Boolean) as React.ReactNode[];

                                    return segments.map((segment, index) => (
                                      <React.Fragment key={index}>
                                        {index > 0 && <span style={{ color: '#d9d9d9' }}>|</span>}
                                        {segment}
                                      </React.Fragment>
                                    ));
                                  })()}
                                </Text>
                              }
                              title={
                                <Space>
                                  <span 
                                    style={{ 
                                      fontSize: '14px', 
                                      cursor: 'pointer',
                                      color: '#1890ff',
                                      textDecoration: 'underline',
                                      maxWidth: 'min(800px, 100%)',
                                      whiteSpace: 'normal',
                                      wordBreak: 'break-word',
                                      overflowWrap: 'anywhere',
                                    }}
                                    title={item.title}
                                    onClick={() => showNewsDetail(item.id)}
                                  >
                                    {item.title}
                                  </span>
                                </Space>
                              }
                              description={null}
                            />
                            
                            {/* Ticker标签 */}
                            {item.symbols && item.symbols.length > 0 && (
                              <div style={{ marginBottom: '8px' }}>
                                <Space size={[4, 4]} wrap>
                                  {item.symbols.map((symbol: string) => (
                                    <Tag key={symbol} color="default" style={{ fontSize: '10px', margin: 0 }}>
                                      {symbol}
                                    </Tag>
                                  ))}
                                </Space>
                              </div>
                            )}

                            {/* 新闻内容 - 支持展开/收起 */}
                            <div style={{ fontSize: '13px', marginBottom: '8px' }}>
                              {(() => {
                                const displayContent = getPreferredAnalysisContent(item.analysis_result, item.content);
                                return displayContent ? (
                                  <div>
                                    <div style={{ 
                                      maxHeight: isNewsExpanded(item.id) ? 'none' : '60px', 
                                      overflow: isNewsExpanded(item.id) ? 'visible' : 'hidden',
                                      position: 'relative'
                                    }}>
                                      <MarkdownRenderer 
                                        content={displayContent}
                                        style={{
                                          fontSize: '13px',
                                          lineHeight: '1.4'
                                        }}
                                      />
                                      {!isNewsExpanded(item.id) && (
                                        <div style={{
                                          position: 'absolute',
                                          bottom: 0,
                                          left: 0,
                                          right: 0,
                                          height: '20px',
                                          background: 'linear-gradient(transparent, white)',
                                          pointerEvents: 'none'
                                        }} />
                                      )}
                                    </div>
                                    <Button
                                      type="link"
                                      size="small"
                                      onClick={() => toggleNewsExpansion(item.id)}
                                      style={{ 
                                        padding: '4px 0', 
                                        fontSize: '12px',
                                        height: 'auto'
                                      }}
                                    >
                                      {isNewsExpanded(item.id) ? 'Collapse' : 'Expand'}
                                    </Button>
                                  </div>
                                ) : null;
                              })()}
                            </div>
                          </List.Item>
                        </div>
                      </div>
                    ))}
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
                      <Text type="secondary">All news loaded</Text>
                    </div>
                  )}
                </>
              )}
            </Card>
          </Col>
        </Row>

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


      {/* AI Analysis Details Modal */}
      <Modal
        title={
          <Space>
            <RobotOutlined style={{ color: '#1890ff' }} />
            <span>{state.selectedNews?.analysis_result ? 'AI Analysis Details' : 'News Details'}</span>
          </Space>
        }
        open={state.detailModalVisible}
        onCancel={() => setState(prev => ({ ...prev, detailModalVisible: false }))}
        footer={[
          <Button key="close" onClick={() => setState(prev => ({ ...prev, detailModalVisible: false }))}>
            Close
          </Button>,
          state.selectedNews?.link && (
            <Button
              key="original" 
              type="primary"
              onClick={() => window.open(state.selectedNews?.link, '_blank')}
            >
              View Original
            </Button>
          )
        ]}
        width={800}
      >
        {state.selectedNews && (
          <>
            <Card size="small" style={{ marginBottom: '16px', backgroundColor: '#f5f5f5' }}>
              <Typography.Title level={5} style={{ marginBottom: '12px' }}>
                {state.selectedNews.title}
              </Typography.Title>
              <Space size="small" wrap>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  <CalendarOutlined /> {state.selectedNews.date ? new Date(state.selectedNews.date).toLocaleString() : 'N/A'}
                </Text>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  Source: {newsUtils.getSourceDisplayName(state.selectedNews.source) || 'N/A'}
                </Text>
              </Space>
            </Card>

            {(() => {
              const allResults = getAllAnalysisResults(state.selectedNews.analysis_result);
              const hasAnyAnalysis = Object.keys(allResults).length > 0;
              // 🔥 使用优先级逻辑获取 summary 模板（支持 CCTV 和通用模板）
              const preferredSummary = getPreferredSummaryTemplate(state.selectedNews.analysis_result);

              if (hasAnyAnalysis) {
                return (
                  <>
                    {/* 优先显示 summary 模板的结果（支持多种 summary 模板） */}
                    {preferredSummary && typeof preferredSummary.result.content === 'string' && (
                      <>
                        <Card 
                          title={
                            <Space>
                              <span>AI Analysis</span>
                              <Tag color="blue" style={{ fontSize: '10px' }}>
                                {preferredSummary.templateCode}
                              </Tag>
                            </Space>
                          }
                          size="small"
                          headStyle={{ backgroundColor: '#e6f7ff', fontWeight: 'bold' }}
                        >
                          <MarkdownRenderer 
                            content={preferredSummary.result.content}
                            style={{
                              fontSize: '12px',
                              lineHeight: '1.6'
                            }}
                          />
                          <div style={{ marginTop: '12px', fontSize: '10px', color: '#999' }}>
                            <Space size="small">
                              {preferredSummary.result.provider && <span>Provider: {preferredSummary.result.provider}</span>}
                              {preferredSummary.result.model && <span>Model: {preferredSummary.result.model}</span>}
                              {preferredSummary.result.tokens && <span>Tokens: {preferredSummary.result.tokens.toLocaleString()}</span>}
                              {preferredSummary.result.temperature && <span>Temperature: {preferredSummary.result.temperature}</span>}
                            </Space>
                </div>
                        </Card>

                        {state.selectedNews.content && (
                          <Card 
                            title="Original Content" 
                            size="small"
                            style={{ marginTop: '16px' }}
                            headStyle={{ backgroundColor: '#f5f5f5' }}
                          >
                            <div style={{ fontSize: '13px', color: '#666', lineHeight: '1.8' }}>
                              {highlightKeywords(state.selectedNews.content)}
                            </div>
                          </Card>
                        )}

                        {/* Tags Section - 美化设计，不使用Card */}
                        <div style={{ 
                          marginTop: '20px',
                          padding: '16px',
                          backgroundColor: '#fafafa',
                          borderRadius: '8px',
                          border: '1px solid #e8e8e8'
                        }}>
                          <div style={{ 
                            marginBottom: '16px',
                            paddingBottom: '12px',
                            borderBottom: '2px solid #e8e8e8'
                          }}>
                            <Text strong style={{ 
                              fontSize: '13px', 
                              color: '#1890ff',
                              fontWeight: 600,
                              letterSpacing: '0.5px'
                            }}>
                              Tags & Metadata
                            </Text>
                          </div>

                          <Space direction="vertical" size="middle" style={{ width: '100%' }}>
                            {/* Related Tickers */}
                            {state.selectedNews.symbols && state.selectedNews.symbols.length > 0 && (
                              <div style={{ 
                                padding: '12px',
                                backgroundColor: '#fff',
                                borderRadius: '6px',
                                border: '1px solid #f0f0f0'
                              }}>
                                <Text strong style={{ 
                                  fontSize: '11px', 
                                  color: '#595959',
                                  display: 'block',
                                  marginBottom: '10px',
                                  fontWeight: 500
                                }}>
                                  Related Tickers
                                </Text>
                                <Space size={[6, 6]} wrap>
                                  {state.selectedNews.symbols.map(symbol => (
                                    <Tag 
                                      key={symbol} 
                                      color="blue" 
                                      style={{ 
                                        fontSize: '11px',
                                        padding: '4px 10px',
                                        borderRadius: '4px',
                                        fontWeight: 500,
                                        border: 'none',
                                        boxShadow: '0 1px 2px rgba(0,0,0,0.1)'
                                      }}
                                    >
                                      {symbol}
                                    </Tag>
                                  ))}
                                </Space>
                              </div>
                            )}

                            {/* Classification Tags */}
                            {(() => {
                              const classification = getClassificationFromAnalysis(state.selectedNews.analysis_result);
                              if (!classification || !hasValidClassification(classification)) {
                                return null;
                              }

                              const tags = getClassificationTags(classification);
                              const relatedSectors = getRelatedSectors(classification);
                              const relatedCountries = getRelatedCountries(classification);

                              return (
                                <>
                                  {/* 主要分类标签 */}
                                  {tags.length > 0 && (
                                    <div style={{ 
                                      padding: '12px',
                                      backgroundColor: '#fff',
                                      borderRadius: '6px',
                                      border: '1px solid #f0f0f0'
                                    }}>
                                      <Text strong style={{ 
                                        fontSize: '11px', 
                                        color: '#595959',
                                        display: 'block',
                                        marginBottom: '10px',
                                        fontWeight: 500
                                      }}>
                                        Classification Tags
                                      </Text>
                                      <Space size={[6, 6]} wrap>
                                        {tags.map(({ key, config, value, displayValue }) => (
                                          <Tag 
                                            key={key} 
                                            color={config.color} 
                                            style={{ 
                                              fontSize: '11px',
                                              padding: '4px 10px',
                                              borderRadius: '4px',
                                              fontWeight: 500,
                                              border: 'none',
                                              boxShadow: '0 1px 2px rgba(0,0,0,0.1)'
                                            }}
                                            title={`${config.label}: ${displayValue}`}
                                          >
                                            {displayValue}
                                          </Tag>
                                        ))}
                                      </Space>
                                    </div>
                                  )}

                                  {/* 相关行业 */}
                                  {relatedSectors.length > 0 && (
                                    <div style={{ 
                                      padding: '12px',
                                      backgroundColor: '#fff',
                                      borderRadius: '6px',
                                      border: '1px solid #f0f0f0'
                                    }}>
                                      <Text strong style={{ 
                                        fontSize: '11px', 
                                        color: '#595959',
                                        display: 'block',
                                        marginBottom: '10px',
                                        fontWeight: 500
                                      }}>
                                        Related Sectors
                                      </Text>
                                      <Space size={[6, 6]} wrap>
                                        {relatedSectors.map(sector => (
                                          <Tag 
                                            key={sector} 
                                            color="purple" 
                                            style={{ 
                                              fontSize: '11px',
                                              padding: '4px 10px',
                                              borderRadius: '4px',
                                              fontWeight: 500,
                                              border: 'none',
                                              boxShadow: '0 1px 2px rgba(0,0,0,0.1)'
                                            }}
                                          >
                                            {sector}
                                          </Tag>
                                        ))}
                                      </Space>
                                    </div>
                                  )}

                                  {/* 相关国家/地区 */}
                                  {relatedCountries.length > 0 && (
                                    <div style={{ 
                                      padding: '12px',
                                      backgroundColor: '#fff',
                                      borderRadius: '6px',
                                      border: '1px solid #f0f0f0'
                                    }}>
                                      <Text strong style={{ 
                                        fontSize: '11px', 
                                        color: '#595959',
                                        display: 'block',
                                        marginBottom: '10px',
                                        fontWeight: 500
                                      }}>
                                        Related Countries
                                      </Text>
                                      <Space size={[6, 6]} wrap>
                                        {relatedCountries.map(country => (
                                          <Tag 
                                            key={country} 
                                            color="geekblue" 
                                            style={{ 
                                              fontSize: '11px',
                                              padding: '4px 10px',
                                              borderRadius: '4px',
                                              fontWeight: 500,
                                              border: 'none',
                                              boxShadow: '0 1px 2px rgba(0,0,0,0.1)'
                                            }}
                                            title={country}
                                          >
                                            {COUNTRIES_MAP[country] || country}
                                          </Tag>
                                        ))}
                                      </Space>
                                    </div>
                                  )}
                                </>
                              );
                            })()}

                            {/* Analysis Template Tags */}
                            {(() => {
                              const allResults = getAllAnalysisResults(state.selectedNews.analysis_result);
                              const templateCodes = Object.keys(allResults);

                              if (templateCodes.length > 0) {
                                return (
                                  <div style={{ 
                                    padding: '12px',
                                    backgroundColor: '#fff',
                                    borderRadius: '6px',
                                    border: '1px solid #f0f0f0'
                                  }}>
                                    <Text strong style={{ 
                                      fontSize: '11px', 
                                      color: '#595959',
                                      display: 'block',
                                      marginBottom: '10px',
                                      fontWeight: 500
                                    }}>
                                      Analysis Template
                                    </Text>
                                    <Space size={[6, 6]} wrap>
                                      {templateCodes.map(templateCode => (
                                        <Tag 
                                          key={templateCode} 
                                          color="green" 
                                          style={{ 
                                            fontSize: '11px',
                                            padding: '4px 10px',
                                            borderRadius: '4px',
                                            fontWeight: 500,
                                            border: 'none',
                                            boxShadow: '0 1px 2px rgba(0,0,0,0.1)'
                                          }}
                                        >
                                          {templateCode}
                                        </Tag>
                                      ))}
                                    </Space>
                                  </div>
                                );
                              }
                              return null;
                            })()}

                            {/* Confidence Information */}
                            {(() => {
                              const classification = getClassificationFromAnalysis(state.selectedNews.analysis_result);
                              if (!classification || !hasValidClassification(classification)) {
                                return null;
                              }

                              const summary = getClassificationSummary(classification);
                              
                              if (summary && (summary.confidence.theme > 0 || summary.confidence.event > 0)) {
                                return (
                                  <div style={{ 
                                    padding: '12px',
                                    backgroundColor: '#fff',
                                    borderRadius: '6px',
                                    border: '1px solid #f0f0f0'
                                  }}>
                                    <Text strong style={{ 
                                      fontSize: '11px', 
                                      color: '#595959',
                                      display: 'block',
                                      marginBottom: '10px',
                                      fontWeight: 500
                                    }}>
                                      Confidence
                                    </Text>
                                    <Space size="middle" style={{ marginTop: '4px' }}>
                                      {summary.confidence.theme > 0 && (
                                        <Tag 
                                          color="orange" 
                                          style={{ 
                                            fontSize: '11px',
                                            padding: '4px 10px',
                                            borderRadius: '4px',
                                            fontWeight: 500,
                                            border: 'none',
                                            boxShadow: '0 1px 2px rgba(0,0,0,0.1)'
                                          }}
                                        >
                                          Theme: {(summary.confidence.theme * 100).toFixed(0)}%
                                        </Tag>
                                      )}
                                      {summary.confidence.event > 0 && (
                                        <Tag 
                                          color="orange" 
                                          style={{ 
                                            fontSize: '11px',
                                            padding: '4px 10px',
                                            borderRadius: '4px',
                                            fontWeight: 500,
                                            border: 'none',
                                            boxShadow: '0 1px 2px rgba(0,0,0,0.1)'
                                          }}
                                        >
                                          Event: {(summary.confidence.event * 100).toFixed(0)}%
                                        </Tag>
                                      )}
                                    </Space>
                                  </div>
                                );
                              }
                              return null;
                            })()}

                            {/* Unified Themes (统一主题) */}
                            {(() => {
                              if (!hasValidUnifiedThemes(state.selectedNews.unified_themes)) {
                                return null;
                              }

                              const groupedThemes = groupThemesByCanonicalName(state.selectedNews.unified_themes || []);
                              
                              if (groupedThemes.size === 0) {
                                return null;
                              }

                              return (
                                <div style={{ 
                                  padding: '12px',
                                  backgroundColor: '#fff',
                                  borderRadius: '6px',
                                  border: '1px solid #f0f0f0'
                                }}>
                                  <div style={{ marginBottom: '10px' }}>
                                    <Text strong style={{ 
                                      fontSize: '11px', 
                                      color: '#595959',
                                      fontWeight: 500,
                                      marginRight: '8px'
                                    }}>
                                      Unified Themes
                                    </Text>
                                    <Tag 
                                      color="cyan" 
                                      style={{ 
                                        fontSize: '10px',
                                        padding: '2px 8px',
                                        borderRadius: '4px',
                                        fontWeight: 500,
                                        border: 'none'
                                      }}
                                    >
                                      Multi-Source
                                    </Tag>
                                  </div>
                                  <Space size={[6, 6]} wrap>
                                    {Array.from(groupedThemes.entries()).map(([canonicalName, themes]) => {
                                      const displayInfo = getBestThemeDisplay(themes);
                                      const qualityColor = getQualityScoreColor(displayInfo.maxQualityScore);
                                      
                                      return (
                                        <Tag 
                                          key={canonicalName}
                                          color={qualityColor}
                                          style={{ 
                                            fontSize: '11px',
                                            padding: '4px 10px',
                                            borderRadius: '4px',
                                            fontWeight: 500,
                                            border: 'none',
                                            boxShadow: '0 1px 2px rgba(0,0,0,0.1)',
                                            cursor: 'help'
                                          }}
                                          title={`${displayInfo.displayName}${displayInfo.description ? '\n' + displayInfo.description : ''}\n\nQuality: ${formatQualityScore(displayInfo.maxQualityScore)}\nConfidence: ${formatQualityScore(displayInfo.avgConfidence)}\nSources: ${displayInfo.sources.map(s => getSourceTypeConfig(s).label).join(', ')}${displayInfo.markets.length > 0 ? '\nMarkets: ' + displayInfo.markets.join(', ') : ''}${displayInfo.isVerified ? '\n✓ Verified' : ''}${displayInfo.isPrimary ? '\n⭐ Primary' : ''}`}
                                        >
                                          {displayInfo.displayName}
                                          {displayInfo.isVerified && <span style={{ marginLeft: '4px' }}>✓</span>}
                                          {displayInfo.isPrimary && <span style={{ marginLeft: '4px' }}>⭐</span>}
                                          {displayInfo.sources.length > 1 && (
                                            <Badge 
                                              count={displayInfo.sources.length} 
                                              style={{ 
                                                backgroundColor: '#52c41a', 
                                                fontSize: '8px',
                                                height: '14px',
                                                lineHeight: '14px',
                                                minWidth: '14px',
                                                marginLeft: '4px'
                                              }}
                                            />
                                          )}
                                        </Tag>
                                      );
                                    })}
                                  </Space>
                                  {/* 来源统计 */}
                                  <div style={{ marginTop: '12px', paddingTop: '12px', borderTop: '1px solid #f0f0f0' }}>
                                    <Text style={{ fontSize: '10px', color: '#8c8c8c', marginRight: '8px' }}>
                                      Sources:
                                    </Text>
                                    <Space size={[4, 4]} wrap>
                                      {Array.from(new Set(state.selectedNews.unified_themes?.map(t => t.source_type) || [])).map(sourceType => {
                                        const config = getSourceTypeConfig(sourceType);
                                        return (
                                          <Tag 
                                            key={sourceType} 
                                            color={config.color} 
                                            style={{ 
                                              fontSize: '10px',
                                              padding: '2px 8px',
                                              borderRadius: '4px',
                                              border: 'none'
                                            }}
                                          >
                                            {config.icon} {config.label}
                                          </Tag>
                                        );
                                      })}
                                    </Space>
                                  </div>
                                </div>
                              );
                            })()}
                          </Space>
                        </div>
                      </>
                    )}

                    {/* 显示其他模板的结果 */}
                    {Object.entries(allResults).map(([templateCode, result]) => {
                      // 🔥 跳过已经显示的 summary 模板
                      if (preferredSummary && templateCode === preferredSummary.templateCode) return null;
                      // 跳过 news_classification（已经显示在 Tags 中）
                      if (templateCode === 'news_classification') return null;
                      
                      return (
                  <Card 
                          key={templateCode}
                          title={`AI Analysis (${templateCode})`} 
                    size="small"
                          style={{ marginTop: '16px' }}
                          headStyle={{ backgroundColor: '#f0f5ff', fontWeight: 'bold' }}
                        >
                          <pre style={{ 
                            fontSize: '12px', 
                            whiteSpace: 'pre-wrap', 
                            wordBreak: 'break-word',
                            margin: 0
                          }}>
                            {typeof result.content === 'string' 
                              ? result.content 
                              : JSON.stringify(result.content, null, 2)}
                          </pre>
                          <div style={{ marginTop: '12px', fontSize: '10px', color: '#999' }}>
                            <Space size="small">
                              {result.provider && <span>Provider: {result.provider}</span>}
                              {result.model && <span>Model: {result.model}</span>}
                              {result.tokens && <span>Tokens: {result.tokens.toLocaleString()}</span>}
                              {result.temperature && <span>Temperature: {result.temperature}</span>}
                            </Space>
                    </div>
                        </Card>
                      );
                    })}
                  </>
                );
              } else {
                return (
                  <Card 
                    title="News Content" 
                    size="small"
                    headStyle={{ backgroundColor: '#f5f5f5', fontWeight: 'bold' }}
                  >
                    <div style={{ fontSize: '14px', lineHeight: '1.8' }}>
                      {state.selectedNews.content 
                        ? highlightKeywords(state.selectedNews.content)
                        : 'No content available'}
                    </div>
                  </Card>
                );
              }
            })()}
          </>
        )}
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
    </div>
  );
};

export default NewsPage;