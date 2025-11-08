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
  FilterOutlined
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
import MarkdownRenderer from '../components/MarkdownRenderer';
import { useAuthStore } from '../store/authStore';
import { getErrorMessage } from '../utils/errorHandler';
import { getAnalysisContent, getAllAnalysisResults, getPreferredAnalysisContent, hasAnalysisForTemplate } from '../utils/newsAnalysisHelper';
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
  getThemeCategoryConfig,
  formatQualityScore,
  getQualityScoreColor
} from '../utils/unifiedThemeHelper';
import { themesAPI } from '../services/themes';
import type { ThemeFilterState, FilteredNews } from '../types/themes';

const { Text } = Typography;

// 辅助函数：获取情感颜色
const getSentimentColor = (polarity?: number) => {
  if (!polarity) return 'default';
  if (polarity > 0.1) return 'green';
  if (polarity < -0.1) return 'red';
  return 'orange';
};

// 辅助函数：获取情感文本
const getSentimentText = (polarity?: number) => {
  if (!polarity) return 'Neutral';
  if (polarity > 0.1) return 'Positive';
  if (polarity < -0.1) return 'Negative';
  return 'Neutral';
};

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
    themes: []
  });

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
    setState(prev => ({ ...prev, themeFilters: filters }));
  };

  // 应用主题筛选（重置并加载）
  const handleApplyThemeFilter = (filters?: ThemeFilterState) => {
    // 重置 offset 和 hasMore 状态
    setState(prev => ({ 
      ...prev, 
      filteredOffset: 0, 
      hasMoreNews: true,
      filteredNewsData: []
    }));
    loadFilteredNews(true, filters);
  };

  // 加载更多新闻
  const loadMoreNews = () => {
    if (!state.hasMoreNews || state.loadingMore) return;
    loadFilteredNews(false);
  };

  // 初始化数据
  useEffect(() => {
    if (isAuthenticated && user) {
      console.log('NewsPage: 用户已认证', { user: user.username, token: !!localStorage.getItem('access_token') });
      loadFilteredNews(true); // 初始加载时也加载筛选新闻（使用默认筛选条件）
      loadMetadata();
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
          <Col span={6}>
            <ThemeFilterPanel
              onFilterChange={handleThemeFilterChange}
              onApply={handleApplyThemeFilter}
              loading={state.loading}
            />
          </Col>
          <Col span={18}>
            {/* 主题筛选模式下的新闻列表 */}
            <Card
              title={
                <Space>
                  <FilterOutlined />
                  <span>Filtered Results</span>
                  <Badge 
                    count={state.filteredTotal} 
                    overflowCount={999}
                    style={{ 
                      fontSize: '10px',
                      height: '18px',
                      lineHeight: '18px',
                      padding: '0 6px',
                      minWidth: '18px',
                      backgroundColor: '#ff9c9c'
                    }}
                  />
                </Space>
              }
              extra={
                <Button
                  icon={<FilterOutlined />}
                  onClick={() => setState(prev => ({ ...prev, themeManagementModalVisible: true }))}
                  size="small"
                >
                  Highlight Setting
                </Button>
              }
            >
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
                          <List.Item
                            extra={
                              <Space direction="vertical" align="end">
                                <Space size={[4, 4]} wrap>
                                  {/* 情感标签 */}
                                  <Tag 
                                    color={getSentimentColor(item.sentiment_polarity)}
                                    style={{ fontSize: '8px', margin: 0 }}
                                  >
                                    {getSentimentText(item.sentiment_polarity)}
                                  </Tag>
                                  {/* AI分析标签 */}
                                  {hasAnalysisForTemplate(item.analysis_result, 'news_summary_v1') && (
                                    <Tag 
                                      color="blue" 
                                      icon={<RobotOutlined />} 
                                      style={{ fontSize: '8px', margin: 0 }}
                                    >
                                      AI分析
                                    </Tag>
                                  )}
                                </Space>
                              </Space>
                            }
                          >
                            <List.Item.Meta
                              avatar={
                                <Text 
                                  type="secondary" 
                                  style={{ fontSize: '10px', whiteSpace: 'nowrap' }}
                                >
                                  <CalendarOutlined />{' '}
                                  {item.date ? dayjs(item.date).format('YYYY-MM-DD HH:mm:ss') : 'N/A'}
                                  {item.updated_at && (
                                    <span style={{ marginLeft: '8px', color: '#999' }}>
                                      | Updated: {dayjs(item.updated_at).format('YYYY-MM-DD HH:mm:ss')}
                                    </span>
                                  )}
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
                                      maxWidth: '800px',
                                      overflow: 'hidden',
                                      textOverflow: 'ellipsis',
                                      whiteSpace: 'nowrap',
                                      display: 'inline-block'
                                    }}
                                    title={item.title}
                                    onClick={() => showNewsDetail(item.id)}
                                  >
                                    {item.title}
                                  </span>
                                  <Text type="secondary" style={{ fontSize: '12px' }}>
                                    - {newsUtils.getSourceDisplayName(item.source)}
                                  </Text>
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
              const summaryResult = getAnalysisContent(state.selectedNews.analysis_result, 'news_summary_v1');

              if (hasAnyAnalysis) {
                return (
                  <>
                    {/* 优先显示 news_summary_v1 的结果 */}
                    {summaryResult && typeof summaryResult.content === 'string' && (
                      <>
                        <Card 
                          title="AI Analysis (Summary)" 
                          size="small"
                          headStyle={{ backgroundColor: '#e6f7ff', fontWeight: 'bold' }}
                        >
                          <MarkdownRenderer 
                            content={summaryResult.content}
                            style={{
                              fontSize: '12px',
                              lineHeight: '1.6'
                            }}
                          />
                          <div style={{ marginTop: '12px', fontSize: '10px', color: '#999' }}>
                            <Space size="small">
                              {summaryResult.provider && <span>Provider: {summaryResult.provider}</span>}
                              {summaryResult.model && <span>Model: {summaryResult.model}</span>}
                              {summaryResult.tokens && <span>Tokens: {summaryResult.tokens.toLocaleString()}</span>}
                              {summaryResult.temperature && <span>Temperature: {summaryResult.temperature}</span>}
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

                        {/* Tags Card */}
                        <Card 
                          title="Tags" 
                          size="small"
                          style={{ marginTop: '16px' }}
                          headStyle={{ backgroundColor: '#f0f5ff', fontWeight: 'bold' }}
                        >
                          <Space direction="vertical" size="small" style={{ width: '100%' }}>
                            {/* Related Tickers */}
                            {state.selectedNews.symbols && state.selectedNews.symbols.length > 0 && (
                              <div>
                                <Text strong style={{ fontSize: '9px', color: '#666' }}>Related Tickers:</Text>
                                <Space size={[4, 4]} wrap style={{ marginTop: '12px' }}>
                                  {state.selectedNews.symbols.map(symbol => (
                                    <Tag key={symbol} color="default" style={{ fontSize: '9px' }}>
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
                                  <div>
                                    <Text strong style={{ fontSize: '9px', color: '#666' }}>Classification Tags:</Text>
                                    <Space size={[4, 4]} wrap style={{ marginTop: '12px' }}>
                                      {tags.map(({ key, config, value, displayValue }) => (
                                        <Tag 
                                          key={key} 
                                          color={config.color} 
                                          style={{ fontSize: '9px' }}
                                          title={`${config.label}: ${displayValue}`}
                                        >
                                          {displayValue}
                    </Tag>
                  ))}
                                    </Space>
                </div>

                                  {/* 相关行业 */}
                                  {relatedSectors.length > 0 && (
                                    <div>
                                      <Text strong style={{ fontSize: '9px', color: '#666' }}>Related Sectors:</Text>
                                      <Space size={[4, 4]} wrap style={{ marginTop: '12px' }}>
                                        {relatedSectors.map(sector => (
                                          <Tag key={sector} color="purple" style={{ fontSize: '9px' }}>
                                            {sector}
                    </Tag>
                  ))}
                                      </Space>
              </div>
            )}

                                  {/* 相关国家/地区 */}
                                  {relatedCountries.length > 0 && (
                                    <div>
                                      <Text strong style={{ fontSize: '9px', color: '#666' }}>Related Countries:</Text>
                                      <Space size={[4, 4]} wrap style={{ marginTop: '12px' }}>
                                        {relatedCountries.map(country => (
                                          <Tag 
                                            key={country} 
                                            color="geekblue" 
                                            style={{ fontSize: '9px' }}
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
            <div>
                                    <Text strong style={{ fontSize: '9px', color: '#666' }}>Analysis Template:</Text>
                                    <Space size={[4, 4]} wrap style={{ marginTop: '12px' }}>
                                      {templateCodes.map(templateCode => (
                                        <Tag key={templateCode} color="green" style={{ fontSize: '9px' }}>
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
                                  <div>
                                    <Text strong style={{ fontSize: '9px', color: '#666' }}>Confidence:</Text>
                                    <Space size="small" style={{ marginTop: '4px' }}>
                                      {summary.confidence.theme > 0 && (
                                        <Text style={{ fontSize: '9px' }}>
                                          Theme: {(summary.confidence.theme * 100).toFixed(0)}%
                                        </Text>
                                      )}
                                      {summary.confidence.event > 0 && (
                                        <Text style={{ fontSize: '9px' }}>
                                          Event: {(summary.confidence.event * 100).toFixed(0)}%
                                        </Text>
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
                                <div>
                                  <Text strong style={{ fontSize: '9px', color: '#666' }}>
                                    Unified Themes: 
                                    <Tag color="cyan" style={{ fontSize: '8px', marginLeft: '4px' }}>
                                      Multi-Source
                                    </Tag>
                                  </Text>
                                  <Space size={[4, 4]} wrap style={{ marginTop: '12px' }}>
                                    {Array.from(groupedThemes.entries()).map(([canonicalName, themes]) => {
                                      const displayInfo = getBestThemeDisplay(themes);
                                      const categoryConfig = getThemeCategoryConfig(displayInfo.category);
                                      const qualityColor = getQualityScoreColor(displayInfo.maxQualityScore);
                                      
                                      return (
                                        <Tag 
                                          key={canonicalName}
                                          color={qualityColor}
                                          style={{ fontSize: '9px', cursor: 'help' }}
                                          title={`${displayInfo.displayName}${displayInfo.description ? '\n' + displayInfo.description : ''}\n\nQuality: ${formatQualityScore(displayInfo.maxQualityScore)}\nConfidence: ${formatQualityScore(displayInfo.avgConfidence)}\nSources: ${displayInfo.sources.map(s => getSourceTypeConfig(s).label).join(', ')}${displayInfo.markets.length > 0 ? '\nMarkets: ' + displayInfo.markets.join(', ') : ''}${displayInfo.isVerified ? '\n✓ Verified' : ''}${displayInfo.isPrimary ? '\n⭐ Primary' : ''}`}
                                        >
                                          {categoryConfig.icon} {displayInfo.displayName}
                                          {displayInfo.isVerified && <span style={{ marginLeft: '2px' }}>✓</span>}
                                          {displayInfo.isPrimary && <span style={{ marginLeft: '2px' }}>⭐</span>}
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
                                  <div style={{ marginTop: '8px' }}>
                                    <Text style={{ fontSize: '8px', color: '#999' }}>
                                      Sources: {' '}
                                      {Array.from(new Set(state.selectedNews.unified_themes?.map(t => t.source_type) || [])).map(sourceType => {
                                        const config = getSourceTypeConfig(sourceType);
                                        return (
                                          <Tag key={sourceType} color={config.color} style={{ fontSize: '7px', marginRight: '2px' }}>
                                            {config.icon} {config.label}
                                          </Tag>
                                        );
                                      })}
                                    </Text>
                                  </div>
                                </div>
                              );
                            })()}
                          </Space>
                        </Card>
                      </>
                    )}

                    {/* 显示其他模板的结果 */}
                    {Object.entries(allResults).map(([templateCode, result]) => {
                      if (templateCode === 'news_summary_v1' && typeof result.content === 'string') return null; // 已经显示过了
                      if (templateCode === 'news_classification') return null; // 不显示新闻分类分析
                      
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
    </div>
  );
};

export default NewsPage;