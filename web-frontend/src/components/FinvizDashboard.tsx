import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import {
  DndContext,
  DragEndEvent,
  PointerSensor,
  closestCenter,
  useSensor,
  useSensors,
} from '@dnd-kit/core';
import type { Modifier } from '@dnd-kit/core';
import {
  SortableContext,
  arrayMove,
  useSortable,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import {
  Input,
  Tabs,
  Typography,
  Tag,
  Empty,
  Spin,
  message,
  Space,
  Card,
  Statistic,
  Row,
  Col,
  Button,
  Skeleton,
  Select,
  Modal,
  Dropdown,
  Divider,
  Segmented,
  Checkbox,
  Tooltip,
  Badge,
  ConfigProvider,
} from 'antd';
import { theme as antdTheme } from 'antd';
import type { MenuProps } from 'antd';
import MarkdownRenderer from './MarkdownRenderer';
import WatchlistResearchReportCards from './watchlist/WatchlistResearchReportCards';
import { getPreferredAnalysisContent, hasAnalysisForTemplate, getAnalysisContent, getAllAnalysisResults } from '../utils/newsAnalysisHelper';
import { MODAL_DARK_BG, MODAL_DARK_BORDER } from '../utils/modalStyles';
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
import {
  SearchOutlined, 
  FileTextOutlined,
  CalendarOutlined,
  PlusOutlined,
  HeartOutlined,
  BankOutlined,
  DashboardOutlined,
  InfoCircleOutlined,
  LineChartOutlined,
  DollarOutlined,
  BarChartOutlined,
  StarFilled,
  StarOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  ArrowUpOutlined,
  ArrowDownOutlined,
  MoreOutlined,
  EditOutlined,
  DeleteOutlined,
  CalculatorOutlined,
  HolderOutlined,
  UnorderedListOutlined
} from '@ant-design/icons';
import { useGlobalLogger } from '../contexts/LoggerContext';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { dashboardAPI, type DashboardNewsItem, type WatchlistGroup, type WatchlistItem, type ResearchReport, type StockPriceData, type CustomGroup } from '../services/dashboard';
import { fundamentalsAPI, fundamentalsUtils, type CompanyFundamentalsResponse } from '../services/fundamentals';
import { newsAPI } from '../services/news';
import type { NewsTheme, NewsUnifiedTheme } from '../types/news';
import AddToWatchlistModal from './AddToWatchlistModal';
import EditWatchlistItemModal from './EditWatchlistItemModal';
import AddToCustomGroupModal from './AddToCustomGroupModal';
import ManageCustomGroupsModal from './ManageCustomGroupsModal';
import StockChart from './StockChart';
import {
  EXCHANGE_FILTER_TABS,
  matchesExchangeFilter,
  resolveExchangeCode,
  resolveBaseSymbol,
  type ExchangeFilterValue
} from '../utils/watchlistExchange';
const { Title, Text, Paragraph } = Typography;
const { TabPane } = Tabs;
const { Option } = Select;

export type DashboardThemeMode = 'light' | 'dark';

// 主题配色（Light / Dark）- 与 FRONTEND_STYLE_GUIDE 统一，Behance 风格
const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    siderBg: '#fff',
    siderBorder: '#f0f0f0',
    contentBg: '#fff',
    headerBg: '#fff',
    headerBorder: '#f0f0f0',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    selectedBg: '#e6f7ff',
    selectedBorder: '#1890ff',
    editCheckedBg: '#f6ffed',
    editCheckedBorder: '#52c41a',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    codeBlockBg: '#f5f5f5',
    positive: '#cf1322',
    negative: '#3f8600',
  },
  dark: {
    pageBg: '#1E202B',
    siderBg: '#1a2234',
    siderBorder: 'rgba(255, 255, 255, 0.06)',
    contentBg: '#1E202B',
    headerBg: '#1a2234',
    headerBorder: 'rgba(255, 255, 255, 0.06)',
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    selectedBg: 'rgba(33, 150, 243, 0.12)',
    selectedBorder: '#2196F3',
    editCheckedBg: 'rgba(56, 161, 105, 0.18)',
    editCheckedBorder: '#38a169',
    text: '#FFFFFF',
    textSecondary: '#A0A3AD',
    codeBlockBg: '#1E202B',
    inputBg: '#1E202B',
    hoverBg: 'rgba(255, 255, 255, 0.06)',
    positive: '#38a169',
    negative: '#e53e3e',
  },
} as const;

// Behance 卡片风格（与 FRONTEND_STYLE_GUIDE 一致）
const CARD_STYLE_BEHANCE = {
  borderRadius: 12,
  boxShadow: '0 4px 24px rgba(0, 0, 0, 0.12)',
  border: '1px solid rgba(0, 0, 0, 0.06)',
} as const;

const CARD_STYLE_BEHANCE_DARK = {
  borderRadius: 12,
  boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)',
  border: '1px solid rgba(255, 255, 255, 0.06)',
} as const;

// 缓存配置
const CACHE_DURATION = 5 * 60 * 1000; // 5分钟缓存

// Priority filter options
const PRIORITY_FILTER_OPTIONS = [
  { label: 'All Priorities', value: 'all', stars: null },
  { label: 'High Priority', value: 3, stars: 3 },
  { label: 'Medium Priority', value: 2, stars: 2 },
  { label: 'Low Priority', value: 1, stars: 1 }
];

// 数据状态类型定义
interface WatchlistStockInfo extends WatchlistItem {
  // 添加一些前端特有的属性（如果需要）
}

interface EnrichedWatchlistItem extends WatchlistItem {
  groupName: string;
  priceData?: StockPriceData;
  resolvedExchange?: string;
  resolvedSymbol?: string;
}

interface SortableWatchlistRowProps {
  item: EnrichedWatchlistItem;
  isSelected: boolean;
  onSelect: (item: WatchlistStockInfo) => void;
  onAction: (action: string, item: WatchlistItem) => void;
  actionItems: MenuProps['items'];
  resolvePriceData: (item: WatchlistItem) => StockPriceData | undefined;
  renderStarsFn: (count: number, filled?: boolean) => React.ReactNode;
  formatPrice: (price?: number) => string;
  formatChange: (change?: number) => string;
  getChangeColor: (change?: number) => string;
  disabled?: boolean;
  isEditMode?: boolean;
  isChecked?: boolean;
  onCheckboxToggle?: (watchlistId: number) => void;
  themeMode?: DashboardThemeMode;
}

const SortableWatchlistRow: React.FC<SortableWatchlistRowProps> = ({
  item,
  isSelected,
  onSelect,
  onAction,
  actionItems,
  resolvePriceData,
  renderStarsFn,
  formatPrice,
  formatChange,
  getChangeColor,
  disabled = false,
  isEditMode = false,
  isChecked = false,
  onCheckboxToggle,
  themeMode = 'light',
}) => {
  const [hovered, setHovered] = useState(false);
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
    id: item.watchlist_id,
    disabled,
  });
  const colors = THEME_COLORS[themeMode];

  const transformStyle = CSS.Transform.toString(transform);

  const containerStyle: React.CSSProperties = {
    transform: transformStyle,
    transition,
    zIndex: isDragging ? 1000 : 'auto',
  };

  const priceData = item.priceData ?? resolvePriceData(item);
  const change = priceData?.change_percent;
  const hasChange = change !== undefined && change !== null;
  const displaySymbol = item.resolvedSymbol ?? resolveBaseSymbol(item.symbol);
  const changeColor = hasChange ? (change! >= 0 ? colors.positive : colors.negative) : colors.textSecondary;
  const displayName = item.custom_name || item.company || item.symbol;

  const isDark = themeMode === 'dark';
  const cardStyleBehance = isDark ? CARD_STYLE_BEHANCE_DARK : CARD_STYLE_BEHANCE;
  const cardBorder = isEditMode && isChecked ? colors.editCheckedBorder : isSelected ? colors.selectedBorder : colors.cardBorder;
  const hoverBg = (colors as { hoverBg?: string }).hoverBg;
  const cardBg = isEditMode && isChecked
    ? colors.editCheckedBg
    : isSelected
    ? colors.selectedBg
    : isDark && hovered && hoverBg
    ? hoverBg
    : colors.cardBg;
  const boxShadow = isEditMode && isChecked
    ? (isDark ? '0 4px 16px rgba(56, 161, 105, 0.2)' : '0 4px 16px rgba(82, 196, 26, 0.15)')
    : isSelected
    ? (isDark ? '0 6px 24px rgba(33, 150, 243, 0.25)' : '0 4px 24px rgba(24, 144, 255, 0.18)')
    : isDragging
    ? '0 8px 24px rgba(0, 0, 0, 0.25)'
    : cardStyleBehance.boxShadow;

  return (
    <div ref={setNodeRef} style={containerStyle} {...attributes}>
      <div
        onMouseEnter={() => setHovered(true)}
        onMouseLeave={() => setHovered(false)}
        onClick={() => {
          if (!disabled) {
            if (isEditMode && onCheckboxToggle) {
              onCheckboxToggle(item.watchlist_id);
            } else {
              onSelect(item as WatchlistStockInfo);
            }
          }
        }}
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '8px',
          padding: '8px 12px',
          height: '48px',
          borderRadius: cardStyleBehance.borderRadius,
          border: `1px solid ${cardBorder}`,
          ...(isSelected && isDark && { borderLeft: '3px solid #2196F3' }),
          background: cardBg,
          boxShadow,
          transition: 'all 0.2s ease',
          cursor: disabled ? 'not-allowed' : 'pointer',
          opacity: disabled && !isDragging ? 0.7 : 1,
        }}
      >
        {isEditMode && onCheckboxToggle && (
          <div onClick={(e) => e.stopPropagation()}>
            <Checkbox
              checked={isChecked}
              onChange={() => onCheckboxToggle(item.watchlist_id)}
            />
          </div>
        )}
        
        {!isEditMode && (
          <div
            {...(!disabled ? listeners : undefined)}
            onClick={event => event.stopPropagation()}
            style={{
              display: 'flex',
              alignItems: 'center',
              height: '100%',
              padding: '0 4px',
              color: colors.textSecondary,
              cursor: disabled ? 'not-allowed' : 'grab',
              userSelect: 'none',
            }}
          >
            <HolderOutlined />
          </div>
        )}

        <div style={{ display: 'flex', flexDirection: 'column', gap: '2px', flex: 1, minWidth: 0 }}>
          <Text
            style={{
              fontSize: '11px',
              fontWeight: 500,
              margin: 0,
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
              color: colors.text,
            }}
          >
            {displayName}
          </Text>
          <div style={{ display: 'flex', alignItems: 'center', gap: '4px', minWidth: 0 }}>
            <Text
              style={{
                fontSize: '11px',
                margin: 0,
                textTransform: 'uppercase',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
                color: colors.textSecondary,
              }}
            >
              {displaySymbol}
            </Text>
            {item.priority ? renderStarsFn(item.priority) : null}
          </div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: '2px' }}>
          <Text strong style={{ fontSize: '11px', color: changeColor, margin: 0 }}>
            {formatPrice(priceData?.close)}
          </Text>
          <div style={{ display: 'flex', alignItems: 'center', gap: '2px' }}>
            {hasChange &&
              (change! >= 0 ? (
                <ArrowUpOutlined style={{ color: colors.positive, fontSize: '11px' }} />
              ) : (
                <ArrowDownOutlined style={{ color: colors.negative, fontSize: '11px' }} />
              ))}
            <Text style={{ fontSize: '11px', color: changeColor, fontWeight: 500, margin: 0 }}>
              {formatChange(change)}
            </Text>
          </div>
        </div>

        <Dropdown
          trigger={['click']}
          menu={{
            items: actionItems ?? [],
            onClick: ({ key }) => onAction(key, item),
          }}
        >
          <Button
            type="text"
            shape="circle"
            size="small"
            icon={<MoreOutlined />}
            disabled={disabled}
            onClick={event => {
              event.stopPropagation();
            }}
          />
        </Dropdown>
      </div>
    </div>
  );
};

// 渲染星星图标的辅助函数
const renderStars = (count: number, filled: boolean = true) => {
  const stars = [];
  for (let i = 0; i < count; i++) {
    stars.push(
      filled ? (
        <StarFilled key={i} style={{ color: '#faad14', fontSize: '6px', opacity: 0.5 }} />
      ) : (
        <StarOutlined key={i} style={{ color: '#d9d9d9', fontSize: '6px', opacity: 0.3 }} />
      )
    );
  }
  return <Space size={1}>{stars}</Space>;
};

const restrictToVerticalAxis: Modifier = ({ transform }) => {
  if (!transform) {
    return transform;
  }

  return {
    ...transform,
    x: 0,
  };
};

const FinvizDashboard: React.FC = () => {
  const logger = useGlobalLogger();
  const { themeMode } = useDashboardSettings();
  
  // 状态管理
  const [selectedStock, setSelectedStock] = useState<WatchlistStockInfo | null>(null);
  const [searchValue, setSearchValue] = useState<string>('');
  const [priorityFilter, setPriorityFilter] = useState<number | string>('all');
  const [exchangeFilter, setExchangeFilter] = useState<ExchangeFilterValue>('all');
  const [activeTab, setActiveTab] = useState<string>('news');
  const [watchlistCollapsed, setWatchlistCollapsed] = useState<boolean>(false);
  
  // 数据状态
  const [watchlistGroups, setWatchlistGroups] = useState<WatchlistGroup[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  
  // 右侧内容数据
  const [newsData, setNewsData] = useState<DashboardNewsItem[]>([]);
  const [researchReports, setResearchReports] = useState<ResearchReport[]>([]);
  const [contentLoading, setContentLoading] = useState<boolean>(false);
  const [overviewData, setOverviewData] = useState<CompanyFundamentalsResponse | null>(null);
  const [overviewLoading, setOverviewLoading] = useState<boolean>(false);
  const [overviewError, setOverviewError] = useState<string | null>(null);
  const [overviewCache, setOverviewCache] = useState<Map<string, CompanyFundamentalsResponse>>(new Map());
  const [overviewLastError, setOverviewLastError] = useState<string | null>(null);
  
  // 分页相关状态
  const [newsOffset, setNewsOffset] = useState<number>(0);
  const [hasMoreNews, setHasMoreNews] = useState<boolean>(true);
  const [loadingMoreNews, setLoadingMoreNews] = useState<boolean>(false);
  const NEWS_PAGE_SIZE = 20; // 每页加载20条新闻
const ANALYSIS_MODAL_BODY_HEIGHT = '68vh'; // AI 分析详情弹窗固定高度，切换 tab 时尺寸与位置保持一致
  
  // 价格数据
  const [stockPrices, setStockPrices] = useState<Map<string, StockPriceData>>(new Map());
  const [pricesLoading, setPricesLoading] = useState<boolean>(false);
  const [isReordering, setIsReordering] = useState<boolean>(false);

  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: { distance: 8 },
    })
  );
  
  // 自动刷新相关状态
  const [isPageVisible, setIsPageVisible] = useState<boolean>(true);
  
  // 缓存机制
  const [newsCache, setNewsCache] = useState<Map<string, DashboardNewsItem[]>>(new Map());
  const [cacheTimestamps, setCacheTimestamps] = useState<Map<string, number>>(new Map());
  
  // 新闻展开状态管理
  const [expandedNewsItems, setExpandedNewsItems] = useState<Set<number>>(new Set());
  // 新闻列表最后更新时间（用于 "Updated HH:MM:SS PM"）
  const [lastNewsUpdate, setLastNewsUpdate] = useState<Date | null>(null);
  
  // 鼠标悬停显示AI分析的状态
  const [hoveredNewsId, setHoveredNewsId] = useState<number | null>(null);
  const [mousePosition, setMousePosition] = useState<{ x: number; y: number }>({ x: 0, y: 0 });
  const hideTooltipTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  
  // 主题关键词数据（用于高亮）
  const [themes, setThemes] = useState<NewsTheme[]>([]);
  
  // 新闻展开/收缩功能
  const toggleNewsExpansion = (newsId: number) => {
    setExpandedNewsItems(prev => {
      const newSet = new Set(prev);
      if (newSet.has(newsId)) {
        newSet.delete(newsId);
      } else {
        newSet.add(newsId);
      }
      return newSet;
    });
  };
  
  const isNewsExpanded = (newsId: number) => expandedNewsItems.has(newsId);
  
  // 关键词高亮函数
  const highlightKeywords = (text: string): React.ReactNode => {
    if (!text || themes.length === 0) {
      return text;
    }

    // 收集所有关键词及其对应的主题颜色
    interface KeywordMatch {
      keyword: string;
      color: string;
      themeName: string;
    }
    
    const keywordMatches: KeywordMatch[] = [];
    
    themes.forEach(theme => {
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
  
  
  // 缓存工具函数
  const getCacheKey = (type: 'stock' | 'group', identifier: string) => {
    return `${type}:${identifier}`;
  };
  
  const isCacheValid = (cacheKey: string): boolean => {
    const timestamp = cacheTimestamps.get(cacheKey);
    if (!timestamp) return false;
    return Date.now() - timestamp < CACHE_DURATION;
  };
  
  const getCachedNews = (cacheKey: string): DashboardNewsItem[] | null => {
    if (isCacheValid(cacheKey)) {
      return newsCache.get(cacheKey) || null;
    }
    return null;
  };
  
  const setCachedNews = useCallback((cacheKey: string, news: DashboardNewsItem[]) => {
    setNewsCache(prev => new Map(prev).set(cacheKey, news));
    setCacheTimestamps(prev => new Map(prev).set(cacheKey, Date.now()));
  }, []);
  
  // 清理过期缓存
  const cleanupExpiredCache = () => {
    const now = Date.now();
    setCacheTimestamps(prev => {
      const newTimestamps = new Map();
      prev.forEach((timestamp, key) => {
        if (now - timestamp < CACHE_DURATION) {
          newTimestamps.set(key, timestamp);
        }
      });
      return newTimestamps;
    });
    
    setNewsCache(prev => {
      const newCache = new Map();
      prev.forEach((news, key) => {
        const timestamp = cacheTimestamps.get(key);
        if (timestamp && now - timestamp < CACHE_DURATION) {
          newCache.set(key, news);
        }
      });
      return newCache;
    });
  };
  
  // 模态框状态
  const [addModalVisible, setAddModalVisible] = useState<boolean>(false);
  const [editModalVisible, setEditModalVisible] = useState<boolean>(false);
  const [editingItem, setEditingItem] = useState<WatchlistItem | null>(null);
  
  // 右键菜单状态
  const [analysisModalVisible, setAnalysisModalVisible] = useState<boolean>(false);
  const [selectedNewsForAnalysis, setSelectedNewsForAnalysis] = useState<DashboardNewsItem | null>(null);
  
  // 错误状态跟踪，防止重复显示相同的错误消息
  const [lastError, setLastError] = useState<string | null>(null);

  // 编辑模式状态（用于批量操作）
  const [isEditMode, setIsEditMode] = useState<boolean>(false);
  const [selectedWatchlistIds, setSelectedWatchlistIds] = useState<Set<number>>(new Set());
  const [addToGroupModalVisible, setAddToGroupModalVisible] = useState<boolean>(false);
  const [manageGroupsModalVisible, setManageGroupsModalVisible] = useState<boolean>(false);

  // 自定义组状态
  const [customGroups, setCustomGroups] = useState<CustomGroup[]>([]);
  const [selectedCustomGroupId, setSelectedCustomGroupId] = useState<number | null>(null);
  const [customGroupMemberIds, setCustomGroupMemberIds] = useState<Set<number>>(new Set());

  // 初始化数据
  useEffect(() => {
    loadUserWatchlist();
    loadThemes();
    loadCustomGroups();
    logger.logInfo('FinvizDashboard页面已加载', 'finviz-dashboard');
    
    // 定期清理过期缓存
    const cleanupInterval = setInterval(cleanupExpiredCache, 60000); // 每分钟清理一次
    
    return () => clearInterval(cleanupInterval);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // 加载自定义组
  const loadCustomGroups = async () => {
    try {
      const groups = await dashboardAPI.getCustomGroups();
      setCustomGroups(groups);
    } catch (error) {
      console.error('Failed to load custom groups:', error);
    }
  };

  // 当选中自定义组时，加载组成员
  useEffect(() => {
    if (selectedCustomGroupId) {
      loadCustomGroupMembers(selectedCustomGroupId);
    } else {
      setCustomGroupMemberIds(new Set());
    }
  }, [selectedCustomGroupId]);

  const loadCustomGroupMembers = async (groupId: number) => {
    try {
      const result = await dashboardAPI.getCustomGroupMembers(groupId);
      const memberIds = new Set(result.members.map(m => m.watchlist_id));
      setCustomGroupMemberIds(memberIds);
    } catch (error) {
      console.error('Failed to load custom group members:', error);
      message.error('Failed to load custom group members');
    }
  };

  // 页面可见性检测
  useEffect(() => {
    const handleVisibilityChange = () => {
      setIsPageVisible(!document.hidden);
    };

    document.addEventListener('visibilitychange', handleVisibilityChange);
    
    return () => {
      document.removeEventListener('visibilitychange', handleVisibilityChange);
    };
  }, []);

  // 加载价格数据
  useEffect(() => {
    if (watchlistGroups.length > 0) {
      loadWatchlistPrices();
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [watchlistGroups]);

  // 自动刷新价格数据
  useEffect(() => {
    let intervalId: NodeJS.Timeout;
    
    if (watchlistGroups.length > 0 && isPageVisible) {
      // 固定使用1分钟刷新间隔
      const intervalMs = 60000; // 1分钟
      
      intervalId = setInterval(() => {
        // 只有在页面可见时才执行刷新
        if (!document.hidden) {
          loadWatchlistPrices();
        }
      }, intervalMs);
    }

    return () => {
      if (intervalId) {
        clearInterval(intervalId);
      }
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [watchlistGroups, isPageVisible]);

  // 确保初次加载时自动选择第一个股票
  useEffect(() => {
    if (watchlistGroups.length > 0 && !selectedStock && !loading) {
      const firstGroup = watchlistGroups.find(group => group.items.length > 0);
      if (firstGroup && firstGroup.items.length > 0) {
        const firstStock = firstGroup.items[0] as WatchlistStockInfo;

        setSelectedStock(firstStock);
        setResearchReports([]);
        setNewsData([]);
        setActiveTab('news');

        const query =
          (firstStock.primaryticker && firstStock.primaryticker.trim()) ||
          (firstStock.symbol && firstStock.symbol.trim()) ||
          null;

        if (query) {
          const normalizedQuery = query.toUpperCase();
          if (overviewCache.has(normalizedQuery)) {
            setOverviewData(overviewCache.get(normalizedQuery) || null);
            setOverviewError(null);
            setOverviewLoading(false);
          } else {
            setOverviewData(null);
            setOverviewError(null);
            setOverviewLoading(true);
          }
        } else {
          setOverviewData(null);
          setOverviewError('No valid ticker available for overview');
          setOverviewLoading(false);
        }

        logger.logInfo(`自动选择第一个股票: ${firstStock.custom_name || firstStock.company} (${firstStock.symbol})`, 'finviz-dashboard');
      }
    }
  }, [watchlistGroups, loading, selectedStock, overviewCache, logger]);

  // 当选择改变时加载相关内容（添加防抖）
  useEffect(() => {
    if (selectedStock && activeTab === 'news') {
      const timer = setTimeout(() => {
        loadRelatedNews();
      }, 300); // 300ms 防抖
      
      return () => clearTimeout(timer);
    } else if (selectedStock && activeTab === 'reports') {
      const timer = setTimeout(() => {
        loadResearchReports(selectedStock.symbol);
      }, 300); // 300ms 防抖
      
      return () => clearTimeout(timer);
    } else if (selectedStock && activeTab === 'overview') {
      const timer = setTimeout(() => {
        loadStockOverview(selectedStock);
      }, 300); // 300ms 防抖
      
      return () => clearTimeout(timer);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedStock, activeTab]);

  const loadUserWatchlist = async () => {
    setLoading(true);
    try {
      const watchlistData = await dashboardAPI.getUserWatchlist();
      setWatchlistGroups(watchlistData);
      
      // 成功加载后清除错误状态
      setLastError(null);
      
      const totalStocks = watchlistData.reduce((sum, group) => sum + group.items.length, 0);
      logger.logInfo(`加载了 ${watchlistData.length} 个分组，${totalStocks} 支股票`, 'finviz-dashboard');
      
    } catch (error) {
      const errorMessage = 'Failed to load user watchlist';
      
      // 只有在错误消息与上次不同时才显示错误消息
      if (lastError !== errorMessage) {
        message.error(errorMessage);
        setLastError(errorMessage);
      }
      
      logger.logError('Failed to load user watchlist', 'finviz-dashboard', error);
    } finally {
      setLoading(false);
    }
  };

  const normalizeGroupName = useCallback((groupName?: string | null) => {
    if (!groupName || groupName.trim() === '') {
      return 'default';
    }
    return groupName;
  }, []);

  // 加载主题数据（用于关键词高亮）
  const loadThemes = async () => {
    try {
      const themesResponse = await newsAPI.getThemes();
      setThemes(themesResponse.data.filter(t => t.is_active)); // 只加载激活的主题
      logger.logInfo(`加载了 ${themesResponse.data.length} 个主题`, 'finviz-dashboard');
    } catch (error) {
      logger.logError('Failed to load themes', 'finviz-dashboard', error);
      // 主题加载失败不阻塞页面，不显示错误消息
    }
  };

  const loadWatchlistPrices = async () => {
    setPricesLoading(true);
    try {
      const response = await dashboardAPI.getWatchlistStockPrices();
      if (response.success && response.data) {
        // 将价格数据转换为Map，以股票代码为key
        const pricesMap = new Map<string, StockPriceData>();
        response.data.forEach(priceData => {
          // 保存完整的ticker（包含交易所后缀）
          pricesMap.set(priceData.code, priceData);
          
          // 也保存不带交易所后缀的版本，以便匹配watchlist中的symbol
          const symbolWithoutExchange = priceData.code.split('.')[0];
          pricesMap.set(symbolWithoutExchange, priceData);
        });
        setStockPrices(pricesMap);
        logger.logInfo(`加载了 ${response.data.length} 个股票的价格数据`, 'finviz-dashboard');
      }
    } catch (error) {
      logger.logError('Failed to load stock prices', 'finviz-dashboard', error);
      // 不显示错误消息，因为价格数据不是必需的
    } finally {
      setPricesLoading(false);
    }
  };

  const getOverviewQueryForStock = useCallback((stock: WatchlistStockInfo | null): string | null => {
    if (!stock) return null;
    if (stock.primaryticker && stock.primaryticker.trim()) {
      return stock.primaryticker.trim();
    }
    if (stock.symbol && stock.symbol.trim()) {
      return stock.symbol.trim();
    }
    return null;
  }, []);

  const loadStockOverview = useCallback(
    async (stock: WatchlistStockInfo) => {
      const query = getOverviewQueryForStock(stock);
      if (!query) {
        setOverviewData(null);
        const errorMessage = 'No valid ticker available for overview';
        setOverviewError(errorMessage);
        if (overviewLastError !== errorMessage) {
          message.warning(errorMessage);
          setOverviewLastError(errorMessage);
        }
        return;
      }

      const normalizedQuery = query.toUpperCase();
      if (overviewCache.has(normalizedQuery)) {
        setOverviewData(overviewCache.get(normalizedQuery) || null);
        setOverviewError(null);
        setOverviewLoading(false);
        return;
      }

      setOverviewLoading(true);
      setOverviewError(null);
      setOverviewData(null);
      try {
        const data = await fundamentalsAPI.searchCompany(query);
        setOverviewData(data);
        setOverviewCache(prev => {
          const next = new Map(prev);
          next.set(normalizedQuery, data);
          return next;
        });
        setOverviewLastError(null);
      } catch (error: unknown) {
        logger.logError('Failed to load company overview', 'finviz-dashboard', error);
        const errorMessage =
          error instanceof Error ? error.message : 'Failed to load company overview';
        setOverviewError(errorMessage);
        if (overviewLastError !== errorMessage) {
          message.error(errorMessage);
          setOverviewLastError(errorMessage);
        }
      } finally {
        setOverviewLoading(false);
      }
    },
    [overviewCache, overviewLastError, logger, getOverviewQueryForStock]
  );

  const resolvePriceData = useCallback((stock: WatchlistItem): StockPriceData | undefined => {
    let priceData = stockPrices.get(stock.symbol);

    const stockWithTicker = stock as any;
    if (!priceData && stockWithTicker?.primaryticker) {
      priceData = stockPrices.get(stockWithTicker.primaryticker);
    }

    if (!priceData && stock.symbol?.includes('.')) {
      const base = stock.symbol.split('.')[0];
      priceData = stockPrices.get(base);
    }

    if (!priceData) {
      const exchanges = ['.US', '.HK', '.SZ', '.SH', '.TW', '.JP'];
      for (const suffix of exchanges) {
        if (stock.symbol) {
          priceData = stockPrices.get(`${stock.symbol}${suffix}`);
          if (priceData) break;

          if (stock.symbol.includes('.')) {
            const base = stock.symbol.split('.')[0];
            priceData = stockPrices.get(`${base}${suffix}`);
            if (priceData) break;
          }
        }
      }
    }

    return priceData;
  }, [stockPrices]);

  const filteredStocks = useMemo<EnrichedWatchlistItem[]>(() => {
    const lowerSearch = searchValue.trim().toLowerCase();

    return watchlistGroups
      .flatMap<EnrichedWatchlistItem>(group =>
        group.items.map(item => {
          const priceData = resolvePriceData(item);
          const resolvedExchange = resolveExchangeCode(item, priceData);
          const resolvedSymbol = resolveBaseSymbol(item.symbol);

          return {
            ...item,
            groupName: group.group_name,
            priceData,
            resolvedExchange,
            resolvedSymbol
          };
        })
      )
      .filter(item => {
        // 自定义组过滤
        if (selectedCustomGroupId && !customGroupMemberIds.has(item.watchlist_id)) {
          return false;
        }

        if (priorityFilter !== 'all' && item.priority !== priorityFilter) {
          return false;
        }

        if (exchangeFilter !== 'all') {
          if (!matchesExchangeFilter(exchangeFilter, item.resolvedExchange)) {
            const baseSymbol = item.resolvedSymbol || resolveBaseSymbol(item.symbol);
            const isHongKongCode = exchangeFilter === 'hk' && /^\d{4}$/.test(baseSymbol);
            const isChinaCode = exchangeFilter === 'cn' && /^\d{6}$/.test(baseSymbol);
            if (!isHongKongCode && !isChinaCode) {
              return false;
            }
          }
        }

        if (!lowerSearch) {
          return true;
        }

        const groupName = item.groupName?.toLowerCase() ?? '';

        return (
          item.symbol.toLowerCase().includes(lowerSearch) ||
          (item.company && item.company.toLowerCase().includes(lowerSearch)) ||
          (item.custom_name && item.custom_name.toLowerCase().includes(lowerSearch)) ||
          (groupName && groupName.includes(lowerSearch))
        );
      });
  }, [watchlistGroups, priorityFilter, exchangeFilter, searchValue, resolvePriceData, selectedCustomGroupId, customGroupMemberIds]);

  const handleSelectStock = useCallback((stock: WatchlistStockInfo | null) => {
    if (!stock) {
      setSelectedStock(null);
      setNewsData([]);
      setResearchReports([]);
      setOverviewData(null);
      setOverviewError(null);
      setOverviewLoading(false);
      setActiveTab('news');
      return;
    }

    setSelectedStock(stock);
    setResearchReports([]);

    const query = getOverviewQueryForStock(stock);
    if (query) {
      const normalizedQuery = query.toUpperCase();
      if (overviewCache.has(normalizedQuery)) {
        setOverviewData(overviewCache.get(normalizedQuery) || null);
        setOverviewError(null);
        setOverviewLoading(false);
      } else {
        setOverviewData(null);
        setOverviewError(null);
        setOverviewLoading(true);
      }
    } else {
      setOverviewData(null);
      setOverviewError('No valid ticker available for overview');
      setOverviewLoading(false);
    }

    setActiveTab('news');
    logger.logInfo(`选择了股票: ${stock.custom_name || stock.company} (${stock.symbol})`, 'finviz-dashboard');
  }, [logger, overviewCache, getOverviewQueryForStock]);

  const formatPrice = (price?: number) => {
    if (price === undefined || price === null) return '--';
    return price.toFixed(2);
  };

  const formatChange = (change?: number) => {
    if (change === undefined || change === null) return '--';
    const sign = change >= 0 ? '+' : '';
    return `${sign}${change.toFixed(2)}%`;
  };

  const getChangeColor = (change?: number) => {
    if (change === undefined || change === null) return '#8c8c8c';
    return change >= 0 ? '#cf1322' : '#3f8600';
  };

  const watchlistActionItems = useMemo(() => ([
    {
      key: 'edit',
      label: 'Edit',
      icon: <EditOutlined />,
    },
    {
      key: 'delete',
      label: 'Delete',
      icon: <DeleteOutlined />,
      danger: true,
    },
  ]), []);

  const loadRelatedNews = async () => {
    if (!selectedStock) return;
    
    setNewsOffset(0);
    setHasMoreNews(true);
    
    const cacheKey = getCacheKey('stock', selectedStock.symbol);
    const cachedNews = getCachedNews(cacheKey);
    
    if (cachedNews) {
      setNewsData(cachedNews);
      setNewsOffset(NEWS_PAGE_SIZE);
      setHasMoreNews(cachedNews.length === NEWS_PAGE_SIZE);
      logger.logInfo(`使用缓存数据: ${cacheKey}`, 'finviz-dashboard');
      return;
    }
    
    setContentLoading(true);
    try {
      await loadStockNews(selectedStock.symbol, cacheKey, true);
    } catch (error) {
      message.error('Failed to load related news');
      logger.logError('Failed to load related news', 'finviz-dashboard', error);
    } finally {
      setContentLoading(false);
    }
  };

  const loadStockNews = useCallback(async (ticker: string, cacheKey?: string | null, reset: boolean = true) => {
    try {
      const offset = reset ? 0 : newsOffset;
      const news = await dashboardAPI.getCompanyNews(ticker, { 
        limit: NEWS_PAGE_SIZE, 
        offset: offset 
      });
      
      if (reset) {
        setNewsData(news);
        setNewsOffset(NEWS_PAGE_SIZE);
        setLastNewsUpdate(new Date());
      } else {
        setNewsData(prev => [...prev, ...news]);
        setNewsOffset(prev => prev + NEWS_PAGE_SIZE);
      }
      
      // 检查是否还有更多数据
      setHasMoreNews(news.length === NEWS_PAGE_SIZE);
      
      // 缓存数据（仅在重置时缓存）
      if (cacheKey && reset) {
        setCachedNews(cacheKey, news);
      }
      
      logger.logInfo(`加载股票新闻: ${ticker}, ${news.length} 条 (offset: ${offset})`, 'finviz-dashboard');
    } catch (error) {
      logger.logError(`加载股票新闻失败: ${ticker}`, 'finviz-dashboard', error);
      if (reset) {
        setNewsData([]);
      }
    }
  }, [newsOffset, NEWS_PAGE_SIZE, setCachedNews, logger]);

  // 加载更多新闻
  const loadMoreNews = useCallback(async () => {
    if (!hasMoreNews || loadingMoreNews || !selectedStock) return;
    
    setLoadingMoreNews(true);
    try {
      await loadStockNews(selectedStock.symbol, null, false);
    } catch (error) {
      logger.logError('Failed to load more news', 'finviz-dashboard', error);
      message.error('Failed to load more news');
    } finally {
      setLoadingMoreNews(false);
    }
  }, [hasMoreNews, loadingMoreNews, selectedStock, loadStockNews, logger]);

  // 滚动检测和自动加载更多新闻（使用右侧 Tab 内容区作为滚动容器）
  useEffect(() => {
    const handleScroll = () => {
      if (!hasMoreNews || loadingMoreNews || activeTab !== 'news') return;
      const scrollContainer = document.querySelector('.watchlist-detail-panel .ant-tabs-tabpane.ant-tabs-tabpane-active');
      if (!scrollContainer) return;
      const { scrollTop, scrollHeight, clientHeight } = scrollContainer;
      const threshold = 200;
      if (scrollTop + clientHeight >= scrollHeight - threshold) {
        loadMoreNews();
      }
    };
    const scrollContainer = document.querySelector('.watchlist-detail-panel .ant-tabs-tabpane.ant-tabs-tabpane-active');
    if (scrollContainer) {
      scrollContainer.addEventListener('scroll', handleScroll);
      return () => scrollContainer.removeEventListener('scroll', handleScroll);
    }
  }, [hasMoreNews, loadingMoreNews, activeTab, loadMoreNews]);

  // 加载研究报告
  const loadResearchReports = async (ticker: string) => {
    try {
      setContentLoading(true);
      const result = await dashboardAPI.getTickerResearchReports(ticker);
      setResearchReports(result.reports);
      logger.logInfo(`加载研究报告: ${ticker}, ${result.count} 个文件`, 'finviz-dashboard');
    } catch (error) {
      logger.logError(`加载研究报告失败: ${ticker}`, 'finviz-dashboard', error);
      setResearchReports([]);
      message.error('加载研究报告失败');
    } finally {
      setContentLoading(false);
    }
  };

  // 下载文件
  // 删除watchlist项
  const handleRemoveFromWatchlist = useCallback(async (watchlistId: number, symbol: string) => {
    try {
      const result = await dashboardAPI.removeFromWatchlist(watchlistId);
      message.success(result.message);
      
      // 重新加载watchlist
      await loadUserWatchlist();
      
      // 如果删除的是当前选中的股票，清空选择
      if (selectedStock?.watchlist_id === watchlistId) {
        setSelectedStock(null);
        setResearchReports([]);
        setNewsData([]);
        setOverviewData(null);
        setOverviewError(null);
        setOverviewLoading(false);
        setActiveTab('news');
      }
      
      logger.logInfo(`从watchlist移除: ${symbol}`, 'finviz-dashboard');
    } catch (error) {
      message.error('Failed to remove');
      logger.logError(`Failed to remove watchlist item: ${symbol}`, 'finviz-dashboard', error);
    }
  }, [selectedStock, logger]);

  const handleWatchlistAction = useCallback((action: string, item: WatchlistItem) => {
    if (action === 'edit') {
      setEditingItem(item);
      setEditModalVisible(true);
    } else if (action === 'delete') {
      Modal.confirm({
        title: 'Delete Watchlist Item',
        content: `Are you sure you want to remove ${item.symbol} from your watchlist?`,
        okText: 'Delete',
        okType: 'danger',
        cancelText: 'Cancel',
        centered: true,
        onOk: async () => {
          await handleRemoveFromWatchlist(item.watchlist_id, item.symbol);
        },
      });
    }
  }, [handleRemoveFromWatchlist]);

  const handleDragEnd = useCallback(async (event: DragEndEvent) => {
    const { active, over } = event;

    if (!over || active.id === over.id) {
      return;
    }

    const activeId = Number(active.id);
    const overId = Number(over.id);

    const flattened = watchlistGroups.flatMap(group =>
      group.items.map(item => ({
        ...item,
        group_name: normalizeGroupName(group.group_name),
        original_group_name: group.group_name,
      }))
    );

    const oldIndex = flattened.findIndex(item => item.watchlist_id === activeId);
    const newIndex = flattened.findIndex(item => item.watchlist_id === overId);

    if (oldIndex === -1 || newIndex === -1) {
      return;
    }

    const overItemGroupKey = flattened[newIndex].group_name;
    const overItemOriginalGroup =
      flattened[newIndex].original_group_name ?? flattened[newIndex].group_name;

    const previousGroups = watchlistGroups.map(group => ({
      group_name: group.group_name,
      items: group.items.map(item => ({ ...item })),
    }));
    const previousSelected = selectedStock;

    const reorderedFlatList = arrayMove(flattened, oldIndex, newIndex);

    // 更新被拖拽项的分组信息（若有变化）
    const movedItemIndex = reorderedFlatList.findIndex(item => item.watchlist_id === activeId);
    if (movedItemIndex !== -1) {
      reorderedFlatList[movedItemIndex] = {
        ...reorderedFlatList[movedItemIndex],
        group_name: overItemGroupKey,
        original_group_name: overItemOriginalGroup ?? overItemGroupKey,
      };
    }

    const groupOrder = watchlistGroups.map(group => ({
      key: normalizeGroupName(group.group_name),
      label: group.group_name,
    }));

    const groupIndexMap = new Map<string, number>();
    groupOrder.forEach(({ key }, index) => {
      if (!groupIndexMap.has(key)) {
        groupIndexMap.set(key, index);
      }
    });

    const updatedGroups: WatchlistGroup[] = groupOrder.map(({ label }) => ({
      group_name: label,
      items: [],
    }));

    reorderedFlatList.forEach(item => {
      const normalizedKey = item.group_name;
      let groupIndex = groupIndexMap.get(normalizedKey);

      if (groupIndex === undefined) {
        groupIndex = updatedGroups.length;
        groupIndexMap.set(normalizedKey, groupIndex);
        updatedGroups.push({
          group_name: item.original_group_name ?? normalizedKey,
          items: [],
        });
      }

      const { group_name: _normalized, original_group_name: _original, ...rest } = item;
      updatedGroups[groupIndex].items.push({ ...rest });
    });

    const reorderPayload: Array<{ watchlist_id: number; sort_order: number; group_name?: string | null }> = [];

    const updatedGroupsWithSortOrder = updatedGroups.map(group => {
      const itemsWithSortOrder = group.items.map((item, index) => {
        const updatedItem = { ...item, sort_order: index + 1 };

        const payloadGroupName =
          group.group_name && group.group_name !== 'default' ? group.group_name : null;

        reorderPayload.push({
          watchlist_id: item.watchlist_id,
          sort_order: index + 1,
          group_name: payloadGroupName,
        });

        return updatedItem;
      });

      return {
        ...group,
        items: itemsWithSortOrder,
      };
    });

    setWatchlistGroups(updatedGroupsWithSortOrder);

    if (selectedStock) {
      const updatedSelected = updatedGroupsWithSortOrder
        .flatMap(group => group.items)
        .find(item => item.watchlist_id === selectedStock.watchlist_id);

      if (updatedSelected) {
        setSelectedStock(updatedSelected as WatchlistStockInfo);
      }
    }

    try {
      setIsReordering(true);
      await dashboardAPI.reorderWatchlist(reorderPayload);
      logger.logInfo('Watchlist order updated', 'finviz-dashboard');
      message.success('Watchlist order updated');
    } catch (error) {
      setWatchlistGroups(previousGroups);
      if (previousSelected) {
        setSelectedStock(previousSelected);
      }
      logger.logError('Failed to reorder watchlist', 'finviz-dashboard', error);
      message.error('Failed to update watchlist order');
    } finally {
      setIsReordering(false);
    }
  }, [logger, normalizeGroupName, selectedStock, watchlistGroups]);

  const renderWatchlistList = () => {
    if (!loading && watchlistGroups.length === 0) {
      return (
        <Empty 
          description="No stocks in your watchlist"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
          style={{ margin: '40px 0' }}
        >
          <Button 
            type="primary" 
            icon={<PlusOutlined />}
            onClick={() => setAddModalVisible(true)}
            style={{
              borderRadius: 10,
              fontWeight: 500,
              height: 36,
              ...(themeMode === 'dark' ? { background: '#2196F3', borderColor: '#2196F3' } : {}),
            }}
          >
            Add First Stock
          </Button>
        </Empty>
      );
    }

    if (!loading && filteredStocks.length === 0) {
      return (
        <Empty 
          description="No stocks match your filters"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
          style={{ margin: '40px 0' }}
        />
      );
    }

    return (
      <DndContext
        sensors={sensors}
        collisionDetection={closestCenter}
        modifiers={[restrictToVerticalAxis]}
        onDragEnd={handleDragEnd}
      >
        <SortableContext
          items={filteredStocks.map(item => item.watchlist_id)}
          strategy={verticalListSortingStrategy}
        >
          <div
            style={{
              display: 'flex',
              flexDirection: 'column',
              gap: '6px',
              opacity: isReordering ? 0.7 : 1,
            }}
          >
            {filteredStocks.map(item => (
              <SortableWatchlistRow
                key={item.watchlist_id}
                item={item}
                isSelected={selectedStock?.watchlist_id === item.watchlist_id}
                onSelect={stock => handleSelectStock(stock)}
                onAction={handleWatchlistAction}
                actionItems={watchlistActionItems}
                resolvePriceData={resolvePriceData}
                renderStarsFn={renderStars}
                formatPrice={formatPrice}
                formatChange={formatChange}
                getChangeColor={getChangeColor}
                disabled={isReordering || loading}
                isEditMode={isEditMode}
                isChecked={selectedWatchlistIds.has(item.watchlist_id)}
                onCheckboxToggle={handleToggleSelectTicker}
                themeMode={themeMode}
              />
            ))}
          </div>
        </SortableContext>
      </DndContext>
    );
  };

  // 添加成功后的回调
  const handleAddSuccess = async () => {
    await loadUserWatchlist();
  };

  // 编辑成功后的回调
  const handleEditSuccess = async () => {
    await loadUserWatchlist();
    // 如果编辑的是当前选中的股票，更新选中股票的信息
    if (editingItem && selectedStock?.watchlist_id === editingItem.watchlist_id) {
      // 重新加载watchlist以获取更新后的数据
      const watchlistData = await dashboardAPI.getUserWatchlist();
      setWatchlistGroups(watchlistData);
      
      // 找到更新后的股票信息
      for (const group of watchlistData) {
        const updatedItem = group.items.find(item => item.watchlist_id === editingItem.watchlist_id);
        if (updatedItem) {
          setSelectedStock(updatedItem as WatchlistStockInfo);
          break;
        }
      }
    }
  };

  // 切换编辑模式
  const handleToggleEditMode = () => {
    setIsEditMode(!isEditMode);
    if (isEditMode) {
      // 退出编辑模式时清空选择
      setSelectedWatchlistIds(new Set());
    }
  };

  // 切换单个ticker的选中状态
  const handleToggleSelectTicker = (watchlistId: number) => {
    setSelectedWatchlistIds(prev => {
      const newSet = new Set(prev);
      if (newSet.has(watchlistId)) {
        newSet.delete(watchlistId);
      } else {
        newSet.add(watchlistId);
      }
      return newSet;
    });
  };

  // 全选/取消全选
  const handleToggleSelectAll = () => {
    if (selectedWatchlistIds.size === filteredStocks.length) {
      // 如果已全选，则取消全选
      setSelectedWatchlistIds(new Set());
    } else {
      // 否则全选
      const allIds = filteredStocks.map(item => item.watchlist_id);
      setSelectedWatchlistIds(new Set(allIds));
    }
  };

  // 打开添加到自定义组的弹窗
  const handleOpenAddToGroup = () => {
    if (selectedWatchlistIds.size === 0) {
      message.warning('Please select at least one ticker');
      return;
    }
    setAddToGroupModalVisible(true);
  };

  // 添加到自定义组成功的回调
  const handleAddToGroupSuccess = async () => {
    // 重新加载自定义组列表
    await loadCustomGroups();
    // 退出编辑模式并清空选择
    setIsEditMode(false);
    setSelectedWatchlistIds(new Set());
  };

  const getSentimentColor = (polarity?: number) => {
    if (!polarity) return 'default';
    if (polarity > 0.1) return 'green';
    if (polarity < -0.1) return 'red';
    return 'orange';
  };

  const getSentimentText = (polarity?: number) => {
    if (!polarity) return 'Neutral';
    if (polarity > 0.1) return 'Positive';
    if (polarity < -0.1) return 'Negative';
    return 'Neutral';
  };

  // Live Headlines / Finviz News 样式用：相对时间
  const formatTimeAgo = (dateStr?: string): string => {
    if (!dateStr) return '';
    const d = new Date(dateStr);
    const now = new Date();
    const diffMs = now.getTime() - d.getTime();
    const diffM = Math.floor(diffMs / 60000);
    const diffH = Math.floor(diffMs / 3600000);
    const diffD = Math.floor(diffMs / 86400000);
    if (diffM < 60) return `${diffM}m ago`;
    if (diffH < 24) return `${diffH}h ago`;
    return `${diffD}d ago`;
  };

  // 从 AI 分析中提取"一句话总结"（第一段，在核心要点之前的内容）
  const getOneSentenceSummary = (analysisResult: any, maxLength: number = 80): string | null => {
    const summaryResult = getAnalysisContent(analysisResult, 'news_summary_v1');
    if (!summaryResult || typeof summaryResult.content !== 'string') {
      return null;
    }
    
    const content = summaryResult.content.trim();
    // 找到第一个编号列表项（如 "1. **核心要点**"）之前的内容
    const firstListMatch = content.match(/^([\s\S]*?)(?=\n\s*1\.\s*\*\*)/);
    let summary = firstListMatch ? firstListMatch[1].trim() : '';
    
    // 如果没找到编号格式，尝试取第一段（到第一个双换行为止）
    if (!summary) {
      const firstParagraph = content.split(/\n\n/)[0];
      summary = firstParagraph?.trim() || '';
    }
    
    // 如果还是没有内容，返回 null
    if (!summary) return null;
    
    // 截断并添加省略号
    if (summary.length > maxLength) {
      summary = summary.substring(0, maxLength).trim() + '...';
    }
    
    return summary;
  };

  // 从 AI 分析中提取"市场影响评估"（方向和评分）
  interface MarketImpact {
    direction: string;  // 正面、中性偏正面、中性、中性偏负面、负面
    score: number;      // 1-5
  }
  
  const getMarketImpact = (analysisResult: any): MarketImpact | null => {
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
    const directionMatch3 = impactText.match(/[-\s]*(正面|中性偏正面|中性偏负面|负面|中性)\s*[\n\r]/);
    // 英文格式: "Direction: Neutral" 或 "Direction: Positive" 或 "Direction: Mixed"
    const directionMatchEn = impactText.match(/Direction[：:]\s*(Positive|Slightly Positive|Neutral|Mixed|Slightly Negative|Negative)(?:\s*\(|[\s\n\r])/i);
    
    if (directionMatch1) {
      direction = directionMatch1[1];
    } else if (directionMatch2) {
      direction = directionMatch2[1];
    } else if (directionMatch3) {
      direction = directionMatch3[1];
    } else if (directionMatchEn) {
      // 英文转中文
      const enDirection = directionMatchEn[1].toLowerCase();
      if (enDirection === 'positive') direction = '正面';
      else if (enDirection === 'slightly positive') direction = '中性偏正面';
      else if (enDirection === 'neutral') direction = '中性';
      else if (enDirection === 'mixed') direction = '中性';
      else if (enDirection === 'slightly negative') direction = '中性偏负面';
      else if (enDirection === 'negative') direction = '负面';
    }
    
    // 提取评分 - 支持多种格式（中英文）
    let score = 0;
    // 中文格式1: "影响程度：3分" 或 "影响程度：3/5" 或 "影响程度：3"（纯数字）或 "程度：3分"
    const scoreMatch1 = impactText.match(/(?:影响)?程度[：:]\s*(\d)(?:[分\/]|\s*[；;｜|]|\s*[\n\r]|\s*$)/);
    // 中文格式2: "正面；4分；" 或 "正面｜4分｜" (分号或竖线分隔)
    const scoreMatch2 = impactText.match(/[；;｜|]\s*(\d)\s*分[；;｜|]/);
    // 中文格式3: 单独一行 "- 4分" 或 "- 4 分"
    const scoreMatch3 = impactText.match(/[-\s]+(\d)\s*分\s*[\n\r]/);
    // 英文格式: "Impact degree: 3/5" 或 "Impact: 3/5" 或 "Severity: 4/5"
    const scoreMatchEn = impactText.match(/(?:Impact(?:\s+degree)?|Severity)[：:]\s*(\d)\s*[\/of]\s*5/i);
    
    if (scoreMatch1) {
      score = parseInt(scoreMatch1[1], 10);
    } else if (scoreMatch2) {
      score = parseInt(scoreMatch2[1], 10);
    } else if (scoreMatch3) {
      score = parseInt(scoreMatch3[1], 10);
    } else if (scoreMatchEn) {
      score = parseInt(scoreMatchEn[1], 10);
    }
    
    if (score === 0) return null;
    
    return { direction, score };
  };

  // 根据市场影响方向获取颜色
  const getMarketImpactColor = (direction: string): { bg: string; text: string; border: string } => {
    const isDark = themeMode === 'dark';
    
    if (direction.includes('正面') && !direction.includes('负面')) {
      // 正面 / 中性偏正面 → 绿色系
      return isDark 
        ? { bg: 'rgba(16, 185, 129, 0.15)', text: '#34d399', border: 'rgba(16, 185, 129, 0.3)' }
        : { bg: 'rgba(16, 185, 129, 0.1)', text: '#059669', border: 'rgba(16, 185, 129, 0.3)' };
    } else if (direction.includes('负面')) {
      // 负面 / 中性偏负面 → 红色系
      return isDark
        ? { bg: 'rgba(239, 68, 68, 0.15)', text: '#f87171', border: 'rgba(239, 68, 68, 0.3)' }
        : { bg: 'rgba(239, 68, 68, 0.1)', text: '#dc2626', border: 'rgba(239, 68, 68, 0.3)' };
    } else {
      // 中性 → 灰色系
      return isDark
        ? { bg: 'rgba(107, 114, 128, 0.2)', text: '#9ca3af', border: 'rgba(107, 114, 128, 0.3)' }
        : { bg: 'rgba(107, 114, 128, 0.1)', text: '#6b7280', border: 'rgba(107, 114, 128, 0.3)' };
    }
  };

  // 新闻来源颜色（CNBC 浅蓝，Bloomberg 橙黄；dark 模式下其余来源也使用可见色）
  const getSourceColor = (source?: string): string => {
    if (!source) return themeMode === 'dark' ? '#94a3b8' : colors.textSecondary;
    const s = source.toLowerCase();
    if (s.includes('cnbc')) return '#5dade2';
    if (s.includes('bloomberg')) return '#f5b041';
    if (s.includes('reuters')) return '#7dd3fc';
    if (s.includes('yahoo') || s.includes('finance')) return themeMode === 'dark' ? '#64748b' : colors.textSecondary;
    if (s.includes('marketwatch')) return '#fcd34d';
    if (s.includes('zero') || s.includes('hedge')) return '#f472b6';
    if (s.includes('fox')) return '#a78bfa';
    return themeMode === 'dark' ? '#94a3b8' : colors.textSecondary;
  };

  // 新闻列表骨架屏
  const renderNewsSkeleton = () => (
    <div>
      {Array.from({ length: 5 }).map((_, index) => (
        <Card key={index} style={{ marginBottom: '16px' }}>
          <Skeleton active paragraph={{ rows: 2 }} />
          <div style={{ marginTop: '12px' }}>
            <Skeleton.Input style={{ width: '60%' }} size="small" />
          </div>
        </Card>
      ))}
    </div>
  );

  // 获取AI Analysis的完整内容用于Tooltip显示
  const getAIAnalysisForTooltip = (analysisResult: any): string | null => {
    if (!analysisResult) return null;
    const summaryResult = getAnalysisContent(analysisResult, 'news_summary_v1');
    if (!summaryResult) return null;
    
    // 返回完整的AI分析内容
    return summaryResult.content || null;
  };

  const renderNewsTab = () => {
    const isDark = themeMode === 'dark';
    const liveHeadlinesHeaderBg = isDark ? '#1e3a5f' : '#b8d4e8';
    const liveHeadlinesHeaderFg = isDark ? '#fff' : '#1a3a5c';
    const liveHeadlinesHeaderFgMuted = isDark ? 'rgba(255,255,255,0.65)' : 'rgba(26,58,92,0.7)';
    const updateTimeStr = lastNewsUpdate
      ? lastNewsUpdate.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit', second: '2-digit', hour12: true })
      : '--:--:-- --';

    if (contentLoading) {
      return <div style={{ minHeight: 400 }}>{renderNewsSkeleton()}</div>;
    }

    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 500 }}>
        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            flex: 1,
            minWidth: 0,
            background: isDark ? colors.cardBg : '#fff',
            border: `1px solid ${colors.cardBorder}`,
            borderRadius: 8,
            overflow: 'hidden',
          }}
        >
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              padding: '10px 14px',
              background: liveHeadlinesHeaderBg,
              flexShrink: 0,
            }}
          >
            <Space>
              <UnorderedListOutlined style={{ color: liveHeadlinesHeaderFg, fontSize: 16 }} />
              <span style={{ color: liveHeadlinesHeaderFg, fontWeight: 600, fontSize: 14 }}>Live Headlines</span>
            </Space>
            <Text style={{ color: liveHeadlinesHeaderFgMuted, fontSize: 12 }}>
              Updated {updateTimeStr}
            </Text>
          </div>
          <div style={{ flex: 1, overflow: 'auto', padding: '12px' }}>
            {newsData.length === 0 ? (
              <Empty description="No related news" image={Empty.PRESENTED_IMAGE_SIMPLE} style={{ marginTop: 24 }} />
            ) : (
              <>
                {newsData.map((item) => {
                  const aiAnalysisContent = getAIAnalysisForTooltip(item.analysis_result);
                  
                  return (
                    <div
                      key={item.id}
                      className="live-headlines-card"
                      style={{
                        padding: '10px 12px',
                        marginBottom: '4px',
                        borderRadius: '8px',
                        borderBottom: `1px solid ${colors.cardBorder}`,
                        cursor: 'pointer',
                        transition: 'background-color 0.2s ease',
                        position: 'relative',
                      }}
                      onClick={() => {
                        setSelectedNewsForAnalysis(item);
                        setAnalysisModalVisible(true);
                      }}
                    >
                      <div style={{ fontSize: 14, marginBottom: 4, lineHeight: 1.4 }}>
                        {item.link ? (
                          <a
                            href={item.link}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="live-headlines-title"
                            style={{ color: colors.text, textDecoration: 'none' }}
                            onClick={(e) => e.stopPropagation()}
                            onMouseEnter={(e) => {
                              if (aiAnalysisContent) {
                                if (hideTooltipTimeoutRef.current) {
                                  clearTimeout(hideTooltipTimeoutRef.current);
                                  hideTooltipTimeoutRef.current = null;
                                }
                                setHoveredNewsId(item.id);
                                setMousePosition({ x: e.clientX, y: e.clientY });
                              }
                            }}
                            onMouseMove={(e) => {
                              if (aiAnalysisContent && hoveredNewsId === item.id) {
                                setMousePosition({ x: e.clientX, y: e.clientY });
                              }
                            }}
                            onMouseLeave={() => {
                              if (hideTooltipTimeoutRef.current) {
                                clearTimeout(hideTooltipTimeoutRef.current);
                              }
                              hideTooltipTimeoutRef.current = setTimeout(() => {
                                setHoveredNewsId(null);
                                hideTooltipTimeoutRef.current = null;
                              }, 300);
                            }}
                          >
                            {item.title}
                          </a>
                        ) : (
                          <span 
                            className="live-headlines-title" 
                            style={{ color: colors.text }}
                            onMouseEnter={(e) => {
                              if (aiAnalysisContent) {
                                if (hideTooltipTimeoutRef.current) {
                                  clearTimeout(hideTooltipTimeoutRef.current);
                                  hideTooltipTimeoutRef.current = null;
                                }
                                setHoveredNewsId(item.id);
                                setMousePosition({ x: e.clientX, y: e.clientY });
                              }
                            }}
                            onMouseMove={(e) => {
                              if (aiAnalysisContent && hoveredNewsId === item.id) {
                                setMousePosition({ x: e.clientX, y: e.clientY });
                              }
                            }}
                            onMouseLeave={() => {
                              if (hideTooltipTimeoutRef.current) {
                                clearTimeout(hideTooltipTimeoutRef.current);
                              }
                              hideTooltipTimeoutRef.current = setTimeout(() => {
                                setHoveredNewsId(null);
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
                              color: themeMode === 'dark' ? '#94a3b8' : '#64748b',
                              marginBottom: 6,
                              lineHeight: 1.5,
                              paddingLeft: 2,
                            }}
                          >
                            {summary}
                          </div>
                        );
                      })()}
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
                        <span style={{ color: getSourceColor(item.source), fontSize: 12 }}>{item.source || '—'}</span>
                        <span style={{ color: colors.textSecondary, fontSize: 12 }}>{formatTimeAgo(item.date)}</span>
                        {/* AI 分析标识 */}
                        {item.analysis_result && getAnalysisContent(item.analysis_result, 'news_summary_v1') && (
                          <span
                            style={{
                              fontSize: 8,
                              fontWeight: 600,
                              padding: '0 3px',
                              borderRadius: 2,
                              background: themeMode === 'dark' ? 'rgba(93, 173, 226, 0.08)' : 'rgba(24, 144, 255, 0.06)',
                              color: themeMode === 'dark' ? 'rgba(93, 173, 226, 0.75)' : 'rgba(24, 144, 255, 0.75)',
                              border: `1px solid ${themeMode === 'dark' ? 'rgba(93, 173, 226, 0.35)' : 'rgba(24, 144, 255, 0.35)'}`,
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
                {hoveredNewsId && (() => {
                  const hoveredItem = newsData.find(item => item.id === hoveredNewsId);
                  const aiAnalysisContent = hoveredItem ? getAIAnalysisForTooltip(hoveredItem.analysis_result) : null;
                  
                  if (!aiAnalysisContent) return null;
                  
                  // 智能定位计算
                  const tooltipWidth = 650;
                  const tooltipMaxHeight = 450;
                  const offset = 20;
                  const viewportWidth = window.innerWidth;
                  const viewportHeight = window.innerHeight;
                  
                  // 计算水平位置：优先显示在鼠标右侧，如果空间不够则显示在左侧
                  let left = mousePosition.x + offset;
                  if (left + tooltipWidth > viewportWidth - 20) {
                    // 右侧空间不够，显示在左侧
                    left = mousePosition.x - tooltipWidth - offset;
                    // 如果左侧也不够，则贴近右边缘
                    if (left < 20) {
                      left = viewportWidth - tooltipWidth - 20;
                    }
                  }
                  
                  // 计算垂直位置：优先显示在鼠标下方，如果空间不够则显示在上方
                  let top = mousePosition.y + 10;
                  if (top + tooltipMaxHeight > viewportHeight - 20) {
                    // 下方空间不够，显示在上方
                    top = mousePosition.y - tooltipMaxHeight - 10;
                    // 如果上方也不够，则贴近底部
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
                        backgroundColor: isDark ? '#1e293b' : '#ffffff',
                        border: `1px solid ${isDark ? 'rgba(255,255,255,0.2)' : '#e5e7eb'}`,
                        boxShadow: '0 4px 12px rgba(0, 0, 0, 0.15)',
                        padding: '12px 16px',
                        borderRadius: 8,
                        zIndex: 9999,
                        pointerEvents: 'auto',
                        fontSize: '9pt',
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
                        setHoveredNewsId(null);
                      }}
                    >
                      <div style={{ 
                        fontWeight: 600, 
                        marginBottom: 8, 
                        fontSize: '10pt',
                        color: isDark ? '#e0e6f1' : '#1f2937',
                        borderBottom: `1px solid ${isDark ? 'rgba(255,255,255,0.2)' : '#e5e7eb'}`,
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
              </>
            )}
            {hasMoreNews && newsData.length > 0 && (
              <div style={{ textAlign: 'center', padding: '12px 0' }}>
                <Button type="primary" size="small" loading={loadingMoreNews} onClick={loadMoreNews} disabled={loadingMoreNews}>
                  {loadingMoreNews ? 'Loading...' : 'Load More News'}
                </Button>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  };

  const renderOverviewTab = () => {
    const chartSection = renderStockDetails();

    if (overviewLoading) {
      return (
        <div>
          {chartSection}
          <div
            style={{
              display: 'flex',
              justifyContent: 'center',
              alignItems: 'center',
              minHeight: '240px'
            }}
          >
            <Spin tip="Loading overview..." />
          </div>
        </div>
      );
    }

    if (overviewError) {
      return (
        <div>
          {chartSection}
          <Card>
            <Empty description={overviewError} image={Empty.PRESENTED_IMAGE_SIMPLE} />
          </Card>
        </div>
      );
    }

    if (!overviewData || !overviewData.company) {
      return (
        <div>
          {chartSection}
          <Empty
            description="Select a stock to view company overview"
            image={Empty.PRESENTED_IMAGE_SIMPLE}
          />
        </div>
      );
    }

    const { company, highlights, valuations, analyst_ratings, technicals } = overviewData;
    const currencyCode = company.currencycode || 'USD';
    const overviewColors = THEME_COLORS[themeMode];

    return (
      <div>
        {chartSection}
        <Card
          title={
            <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
              <InfoCircleOutlined style={{ color: '#1890ff' }} />
              Basic Information
            </span>
          }
          style={{ marginBottom: 24 }}
        >
          <Row gutter={[24, 16]}>
            <Col xs={24} lg={12}>
              <div
                style={{
                  display: 'grid',
                  gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
                  gap: '16px'
                }}
              >
                <div>
                  <Text strong>Company Name:</Text>
                  <br />
                  <Text>{company.name || 'N/A'}</Text>
                </div>
                <div>
                  <Text strong>Stock Code:</Text>
                  <br />
                  <Text>{company.code || company.primaryticker || 'N/A'}</Text>
                </div>
                <div>
                  <Text strong>Exchange:</Text>
                  <br />
                  <Text>{company.exchange || 'N/A'}</Text>
                </div>
                <div>
                  <Text strong>Currency:</Text>
                  <br />
                  <Text>{currencyCode}</Text>
                </div>
                <div>
                  <Text strong>Country:</Text>
                  <br />
                  <Text>{company.countryname || 'N/A'}</Text>
                </div>
                <div>
                  <Text strong>Sector:</Text>
                  <br />
                  <Text>{company.sector || 'N/A'}</Text>
                </div>
                <div>
                  <Text strong>Industry:</Text>
                  <br />
                  <Text>{company.industry || 'N/A'}</Text>
                </div>
                <div>
                  <Text strong>Employees:</Text>
                  <br />
                  <Text>
                    {company.fulltimeemployees
                      ? company.fulltimeemployees.toLocaleString()
                      : 'N/A'}
                  </Text>
                </div>
                <div>
                  <Text strong>Type:</Text>
                  <br />
                  <Text>{company.type || 'N/A'}</Text>
                </div>
              </div>
            </Col>
            <Col xs={24} lg={12}>
              <div>
                <Text strong>Contact Information:</Text>
                <div style={{ marginTop: 8 }}>
                  {company.phone && (
                    <div style={{ marginBottom: 4 }}>
                      <Text>Phone: {company.phone}</Text>
                    </div>
                  )}
                  {company.weburl && (
                    <div style={{ marginBottom: 4 }}>
                      <Text>Website: </Text>
                      <a href={company.weburl} target="_blank" rel="noopener noreferrer">
                        {company.weburl}
                      </a>
                    </div>
                  )}
                  {company.address && (
                    <div>
                      <Text>Address: {company.address}</Text>
                    </div>
                  )}
                </div>
              </div>
            </Col>
            {company.description && (
              <Col span={24}>
                <Divider />
                <div>
                  <Text strong>Company Description:</Text>
                  <Paragraph style={{ marginTop: 8, fontSize: '14px', lineHeight: '1.6' }}>
                    {company.description}
                  </Paragraph>
                </div>
              </Col>
            )}
          </Row>
        </Card>

        {highlights && (
          <Card
            title={
              <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <BarChartOutlined style={{ color: '#52c41a' }} />
                Key Metrics
              </span>
            }
            style={{ marginBottom: 24 }}
          >
            <Row gutter={[16, 16]}>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Market Cap"
                  value={fundamentalsUtils.formatCurrency(highlights.marketcapitalization, currencyCode)}
                  prefix={<DollarOutlined />}
                  valueStyle={{ color: '#1890ff', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="P/E Ratio"
                  value={fundamentalsUtils.formatNumber(highlights.peratio)}
                  prefix={<LineChartOutlined />}
                  valueStyle={{
                    color: fundamentalsUtils.getRatioColor(highlights.peratio),
                    fontSize: '16px'
                  }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Revenue (TTM)"
                  value={fundamentalsUtils.formatCurrency(highlights.revenuettm, currencyCode)}
                  prefix={<BankOutlined />}
                  valueStyle={{ color: '#52c41a', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="EPS (TTM)"
                  value={fundamentalsUtils.formatNumber(highlights.earningsshare)}
                  valueStyle={{
                    color: fundamentalsUtils.getRatioColor(highlights.earningsshare),
                    fontSize: '16px'
                  }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Dividend Yield"
                  value={fundamentalsUtils.formatPercentage(highlights.dividendyield)}
                  valueStyle={{ color: '#faad14', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="ROE (TTM)"
                  value={fundamentalsUtils.formatPercentage(highlights.returnonequityttm)}
                  valueStyle={{
                    color: fundamentalsUtils.getRatioColor(highlights.returnonequityttm),
                    fontSize: '16px'
                  }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Profit Margin"
                  value={fundamentalsUtils.formatPercentage(highlights.profitmargin)}
                  valueStyle={{
                    color: fundamentalsUtils.getRatioColor(highlights.profitmargin),
                    fontSize: '16px'
                  }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Operating Margin"
                  value={fundamentalsUtils.formatPercentage(highlights.operatingmarginttm)}
                  valueStyle={{
                    color: fundamentalsUtils.getRatioColor(highlights.operatingmarginttm),
                    fontSize: '16px'
                  }}
                />
              </Col>
            </Row>
          </Card>
        )}

        {analyst_ratings && analyst_ratings.length > 0 && (
          <Card
            title={
              <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <StarOutlined style={{ color: '#722ed1' }} />
                Analyst Ratings
              </span>
            }
            style={{ marginBottom: 24 }}
          >
            <Row gutter={[16, 16]}>
              <Col span={24}>
                {analyst_ratings.map((rating, index) => {
                  const safeRating = rating?.rating || 'N/A';
                  const safeAnalystName = rating?.analyst_name || 'Unknown Analyst';
                  const safeTargetPrice = rating?.target_price;
                  const safeRatingDate = rating?.rating_date;
                  const safeBrokerage = rating?.brokerage;
                  const safeRatingValue = rating?.rating_value;
                  const safeTotalAnalysts = rating?.total_analysts || 0;
                  const safeRatingBreakdown = rating?.rating_breakdown;

                  return (
                    <div
                      key={`${safeAnalystName}-${index}`}
                      style={{
                        marginBottom: '16px',
                        padding: '16px',
                        border: `1px solid ${overviewColors.cardBorder}`,
                        borderRadius: '8px',
                        backgroundColor: overviewColors.cardBg
                      }}
                    >
                      <div
                        style={{
                          display: 'flex',
                          justifyContent: 'space-between',
                          alignItems: 'center',
                          marginBottom: 12,
                          gap: 16,
                          flexWrap: 'wrap'
                        }}
                      >
                        <div>
                          <Text strong style={{ fontSize: '16px' }}>
                            {safeAnalystName}
                          </Text>
                          {safeRatingValue && (
                            <Text type="secondary" style={{ marginLeft: 8, fontSize: '14px' }}>
                              (Rating: {safeRatingValue.toFixed(2)})
                            </Text>
                          )}
                        </div>
                        <Tag
                          color={fundamentalsUtils.getRatingColor(safeRating)}
                          style={{ fontSize: '14px', padding: '4px 12px' }}
                        >
                          {safeRating}
                        </Tag>
                      </div>

                      <Row gutter={[16, 8]} style={{ marginBottom: 12 }}>
                        <Col xs={24} sm={12}>
                          <Text strong>Target Price: </Text>
                          <Text>
                            {safeTargetPrice
                              ? fundamentalsUtils.formatCurrency(safeTargetPrice, currencyCode)
                              : 'N/A'}
                          </Text>
                        </Col>
                        <Col xs={24} sm={12}>
                          <Text strong>Date: </Text>
                          <Text>{fundamentalsUtils.formatDate(safeRatingDate)}</Text>
                        </Col>
                      </Row>

                      {safeRatingBreakdown && safeTotalAnalysts > 0 && (
                        <div style={{ marginTop: 12 }}>
                          <Text strong style={{ fontSize: '14px', marginBottom: 8, display: 'block' }}>
                            Rating Distribution ({safeTotalAnalysts} analysts):
                          </Text>
                          <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
                            {safeRatingBreakdown.strong_buy?.count > 0 && (
                              <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                <div
                                  style={{
                                    width: '12px',
                                    height: '12px',
                                    backgroundColor: '#52c41a',
                                    borderRadius: '2px'
                                  }}
                                ></div>
                                <Text style={{ fontSize: '12px' }}>
                                  Strong Buy: {safeRatingBreakdown.strong_buy.count} (
                                  {safeRatingBreakdown.strong_buy.percentage}%)
                                </Text>
                              </div>
                            )}
                            {safeRatingBreakdown.buy?.count > 0 && (
                              <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                <div
                                  style={{
                                    width: '12px',
                                    height: '12px',
                                    backgroundColor: '#73d13d',
                                    borderRadius: '2px'
                                  }}
                                ></div>
                                <Text style={{ fontSize: '12px' }}>
                                  Buy: {safeRatingBreakdown.buy.count} ({safeRatingBreakdown.buy.percentage}%)
                                </Text>
                              </div>
                            )}
                            {safeRatingBreakdown.hold?.count > 0 && (
                              <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                <div
                                  style={{
                                    width: '12px',
                                    height: '12px',
                                    backgroundColor: '#faad14',
                                    borderRadius: '2px'
                                  }}
                                ></div>
                                <Text style={{ fontSize: '12px' }}>
                                  Hold: {safeRatingBreakdown.hold.count} ({safeRatingBreakdown.hold.percentage}%)
                                </Text>
                              </div>
                            )}
                            {safeRatingBreakdown.sell?.count > 0 && (
                              <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                <div
                                  style={{
                                    width: '12px',
                                    height: '12px',
                                    backgroundColor: '#ff7a45',
                                    borderRadius: '2px'
                                  }}
                                ></div>
                                <Text style={{ fontSize: '12px' }}>
                                  Sell: {safeRatingBreakdown.sell.count} ({safeRatingBreakdown.sell.percentage}%)
                                </Text>
                              </div>
                            )}
                            {safeRatingBreakdown.strong_sell?.count > 0 && (
                              <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                <div
                                  style={{
                                    width: '12px',
                                    height: '12px',
                                    backgroundColor: '#ff4d4f',
                                    borderRadius: '2px'
                                  }}
                                ></div>
                                <Text style={{ fontSize: '12px' }}>
                                  Strong Sell: {safeRatingBreakdown.strong_sell.count} (
                                  {safeRatingBreakdown.strong_sell.percentage}%)
                                </Text>
                              </div>
                            )}
                          </div>
                        </div>
                      )}

                      {safeBrokerage && (
                        <div style={{ marginTop: 8, fontSize: '12px', color: overviewColors.textSecondary }}>
                          <Text>
                            {safeBrokerage === 'Multiple Brokers' ? safeBrokerage : `Firm: ${safeBrokerage}`}
                          </Text>
                        </div>
                      )}
                    </div>
                  );
                })}
              </Col>
            </Row>
          </Card>
        )}

        {valuations && (
          <Card
            title={
              <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <CalculatorOutlined style={{ color: '#faad14' }} />
                Valuation Metrics
              </span>
            }
            style={{ marginBottom: 24 }}
          >
            <Row gutter={[16, 16]}>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Trailing P/E"
                  value={fundamentalsUtils.formatNumber(valuations.trailingpe)}
                  valueStyle={{
                    color: fundamentalsUtils.getRatioColor(valuations.trailingpe),
                    fontSize: '16px'
                  }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Forward P/E"
                  value={fundamentalsUtils.formatNumber(valuations.forwardpe)}
                  valueStyle={{
                    color: fundamentalsUtils.getRatioColor(valuations.forwardpe),
                    fontSize: '16px'
                  }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Price/Sales (TTM)"
                  value={fundamentalsUtils.formatNumber(valuations.pricesalesttm)}
                  valueStyle={{
                    color: fundamentalsUtils.getRatioColor(valuations.pricesalesttm),
                    fontSize: '16px'
                  }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Price/Book (MRQ)"
                  value={fundamentalsUtils.formatNumber(valuations.pricebookmrq)}
                  valueStyle={{
                    color: fundamentalsUtils.getRatioColor(valuations.pricebookmrq),
                    fontSize: '16px'
                  }}
                />
              </Col>
              <Col xs={24} sm={12} lg={12}>
                <Statistic
                  title="Enterprise Value"
                  value={fundamentalsUtils.formatCurrency(valuations.enterprisevalue, currencyCode)}
                  valueStyle={{ color: '#1890ff', fontSize: '16px' }}
                />
              </Col>
              <Col xs={24} sm={12} lg={12}>
                <Statistic
                  title="EV/Revenue"
                  value={fundamentalsUtils.formatNumber(valuations.enterprisevaluerevenue)}
                  valueStyle={{
                    color: fundamentalsUtils.getRatioColor(valuations.enterprisevaluerevenue),
                    fontSize: '16px'
                  }}
                />
              </Col>
            </Row>
          </Card>
        )}

        {technicals && (
          <Card
            title={
              <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <DashboardOutlined style={{ color: '#13c2c2' }} />
                Technical Indicators
              </span>
            }
            style={{ marginBottom: 24 }}
          >
            <Row gutter={[16, 16]}>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Beta"
                  value={fundamentalsUtils.formatNumber(technicals.beta)}
                  valueStyle={{
                    color:
                      technicals.beta && technicals.beta > 1 ? '#ff4d4f' : '#52c41a',
                    fontSize: '16px'
                  }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="52-Week High"
                  value={fundamentalsUtils.formatCurrency(technicals['52WeekHigh'], currencyCode)}
                  valueStyle={{ color: '#52c41a', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="52-Week Low"
                  value={fundamentalsUtils.formatCurrency(technicals['52WeekLow'], currencyCode)}
                  valueStyle={{ color: '#ff4d4f', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="50-Day MA"
                  value={fundamentalsUtils.formatCurrency(technicals['50DayMA'], currencyCode)}
                  valueStyle={{ color: '#1890ff', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="200-Day MA"
                  value={fundamentalsUtils.formatCurrency(technicals['200DayMA'], currencyCode)}
                  valueStyle={{ color: '#1890ff', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Short Ratio"
                  value={fundamentalsUtils.formatNumber(technicals.shortratio)}
                  valueStyle={{ color: '#faad14', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Short Interest"
                  value={fundamentalsUtils.formatNumber(technicals.sharesshort)}
                  valueStyle={{ color: '#faad14', fontSize: '16px' }}
                />
              </Col>
              <Col xs={12} sm={8} lg={6}>
                <Statistic
                  title="Short % Float"
                  value={fundamentalsUtils.formatPercentage(technicals.shortpercent)}
                  valueStyle={{ color: '#faad14', fontSize: '16px' }}
                />
              </Col>
            </Row>
          </Card>
        )}
      </div>
    );
  };

  const renderResearchReportsTab = () => (
    <div>
      {contentLoading ? (
        <WatchlistResearchReportCards
          loading
          themeColors={THEME_COLORS[themeMode]}
        />
      ) : researchReports.length === 0 ? (
        <Empty 
          description="No research reports found for this ticker"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      ) : (
        <WatchlistResearchReportCards
          reports={researchReports}
          themeColors={THEME_COLORS[themeMode]}
        />
      )}
    </div>
  );

  const renderStockDetails = () => {
    if (!selectedStock) return null;

    // 获取该股票的价格数据
    // 首先尝试直接匹配symbol，然后尝试匹配带交易所后缀的版本
    let priceData = stockPrices.get(selectedStock.symbol);
    
    // 尝试使用primaryticker（如果存在）
    const stockWithTicker = selectedStock as any;
    if (!priceData && stockWithTicker.primaryticker) {
      priceData = stockPrices.get(stockWithTicker.primaryticker);
    }
    
    if (!priceData) {
      // 尝试常见的交易所后缀
      const exchanges = ['.US', '.HK', '.SZ', '.SH', '.TW', '.JP'];
      for (const exchange of exchanges) {
        priceData = stockPrices.get(`${selectedStock.symbol}${exchange}`);
        if (priceData) break;
      }
    }

    const stockTitle = selectedStock.custom_name || selectedStock.company || selectedStock.symbol;
    const cardStyle = themeMode === 'dark'
      ? { marginBottom: 24, ...CARD_STYLE_BEHANCE_DARK }
      : { marginBottom: 24, ...CARD_STYLE_BEHANCE };

    return (
      <Card
        title={
          <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <LineChartOutlined style={{ color: themeMode === 'dark' ? '#2196F3' : '#1890ff' }} />
            {stockTitle}
            <Text type="secondary" style={{ fontWeight: 400, fontSize: 14 }}>
              ({selectedStock.symbol})
            </Text>
          </span>
        }
        style={cardStyle}
      >
        <StockChart 
          key={`${selectedStock.symbol}-${selectedStock.watchlist_id}`}
          symbol={selectedStock.symbol}
          companyName={selectedStock.custom_name || selectedStock.company}
          primaryTicker={selectedStock.primaryticker}
          realTimePriceData={priceData}
          onRefreshPrice={loadWatchlistPrices}
          priceLoading={pricesLoading}
          sector={selectedStock.sector}
          industry={selectedStock.industry}
          exchange={selectedStock.exchange}
          themeMode={themeMode}
        />
      </Card>
    );
  };

  const colors = THEME_COLORS[themeMode];

  return (
    <ConfigProvider
      theme={{
        algorithm: themeMode === 'dark' ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(themeMode === 'dark' && {
          token: {
            colorBgContainer: colors.cardBg,
            colorBgElevated: colors.cardBg,
            colorBorderSecondary: colors.cardBorder,
            colorPrimary: '#2196F3',
            colorText: colors.text,
            colorTextSecondary: colors.textSecondary,
            colorFillQuaternary: (colors as { inputBg?: string }).inputBg ?? '#1E202B',
            colorBgLayout: colors.pageBg,
            borderRadius: 8,
            borderRadiusLG: 12,
          },
          components: { Card: { colorBgContainer: colors.cardBg } },
        }),
      }}
    >
    <div className="watchlist-page-layout" style={{ background: colors.pageBg }}>
      <div className="watchlist-page-inner">
        {/* 左侧：Watchlist 列表栏（固定卡片，仅列表区域滚动） */}
        <div
          className="watchlist-list-panel"
          style={{
            width: watchlistCollapsed ? 80 : 350,
            minWidth: watchlistCollapsed ? 80 : 350,
            background: colors.siderBg,
            border: `1px solid ${colors.siderBorder}`,
            ...(themeMode === 'dark' ? { boxShadow: CARD_STYLE_BEHANCE_DARK.boxShadow } : { boxShadow: CARD_STYLE_BEHANCE.boxShadow }),
          }}
        >
          <div
            className="watchlist-list-header"
            style={{
              borderBottomColor: colors.siderBorder,
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              flexDirection: watchlistCollapsed ? 'column' : 'row',
              gap: watchlistCollapsed ? '8px' : '0',
            }}
          >
            <Title 
              level={watchlistCollapsed ? 5 : 4} 
              style={{ 
                margin: 0,
                textAlign: watchlistCollapsed ? 'center' : 'left',
                fontSize: watchlistCollapsed ? '14px' : '20px',
                cursor: watchlistCollapsed ? 'pointer' : 'default',
                color: colors.text
              }}
              onClick={watchlistCollapsed ? () => setWatchlistCollapsed(false) : undefined}
            >
              {watchlistCollapsed ? (
                <HeartOutlined />
              ) : (
                <>
                  <HeartOutlined /> Watchlist
                </>
              )}
            </Title>
            {!watchlistCollapsed && (
              <Space size="small">
                {!isEditMode ? (
                  <>
                    <Tooltip title="Add - Add a new stock to your watchlist">
                      <Button 
                        type="primary" 
                        icon={<PlusOutlined />} 
                        size="small"
                        onClick={() => setAddModalVisible(true)}
                        style={{
                          borderRadius: 8,
                          fontWeight: 500,
                          ...(themeMode === 'dark' ? { background: '#2196F3', borderColor: '#2196F3' } : {}),
                        }}
                      />
                    </Tooltip>
                    <Tooltip title="Edit - Enter edit mode to select and manage multiple tickers">
                      <Button 
                        icon={<EditOutlined />} 
                        size="small"
                        onClick={handleToggleEditMode}
                        style={{
                          borderRadius: 8,
                          ...(themeMode === 'dark' ? { color: colors.textSecondary, borderColor: colors.cardBorder } : {}),
                        }}
                      />
                    </Tooltip>
                  </>
                ) : (
                  <>
                    <Button 
                      type="primary"
                      size="small"
                      onClick={handleOpenAddToGroup}
                      disabled={selectedWatchlistIds.size === 0}
                      style={{
                        borderRadius: 8,
                        fontWeight: 500,
                        ...(themeMode === 'dark' ? { background: '#2196F3', borderColor: '#2196F3' } : {}),
                      }}
                    >
                      Add to ({selectedWatchlistIds.size})
                    </Button>
                    <Button 
                      size="small"
                      onClick={handleToggleEditMode}
                      style={{
                        borderRadius: 8,
                        ...(themeMode === 'dark' ? { color: colors.textSecondary, borderColor: colors.cardBorder } : {}),
                      }}
                    >
                      Cancel
                    </Button>
                  </>
                )}
                <Button
                  type="text"
                  icon={<MenuFoldOutlined />}
                  onClick={() => setWatchlistCollapsed(true)}
                  style={{
                    fontSize: '16px',
                    width: 32,
                    height: 32,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center'
                  }}
                  title="收缩Watchlist"
                />
              </Space>
            )}
          </div>

          {!watchlistCollapsed && (
            <div
              className="watchlist-list-filters"
              style={{ borderBottomColor: colors.siderBorder }}
            >
              <Input
                placeholder="Search stocks..."
                prefix={<SearchOutlined />}
                value={searchValue}
                onChange={e => setSearchValue(e.target.value)}
                style={{
                  marginBottom: '12px',
                  borderRadius: 8,
                  ...(themeMode === 'dark' && (colors as { inputBg?: string }).inputBg
                    ? { backgroundColor: (colors as { inputBg?: string }).inputBg }
                    : {}),
                }}
                allowClear
              />
              {isEditMode && filteredStocks.length > 0 && (
                <div style={{ marginBottom: '12px' }}>
                  <Checkbox
                    checked={selectedWatchlistIds.size === filteredStocks.length && filteredStocks.length > 0}
                    indeterminate={selectedWatchlistIds.size > 0 && selectedWatchlistIds.size < filteredStocks.length}
                    onChange={handleToggleSelectAll}
                  >
                    Select All ({selectedWatchlistIds.size}/{filteredStocks.length})
                  </Checkbox>
                </div>
              )}
              <div style={{ display: 'flex', gap: '8px', marginBottom: '12px', alignItems: 'center' }}>
                <Segmented
                  size="small"
                  value={exchangeFilter}
                  onChange={value => setExchangeFilter(value as ExchangeFilterValue)}
                  options={EXCHANGE_FILTER_TABS}
                  block
                  style={{ flex: 1 }}
                />
                <Select
                  placeholder="Priority"
                  value={priorityFilter}
                  onChange={setPriorityFilter}
                  size="small"
                  style={{
                    width: 140,
                    border: 'none',
                    borderRadius: 8,
                    ...(themeMode === 'dark' && (colors as { inputBg?: string }).inputBg
                      ? { backgroundColor: (colors as { inputBg?: string }).inputBg }
                      : {}),
                  }}
                  variant="borderless"
                >
                  {PRIORITY_FILTER_OPTIONS.map(option => (
                    <Option key={option.value} value={option.value}>
                      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                        {option.stars ? renderStars(option.stars) : <span style={{ fontSize: '12px' }}>All Priorities</span>}
                      </div>
                    </Option>
                  ))}
                </Select>
              </div>
              <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                <Select
                  placeholder="Custom Groups"
                  value={selectedCustomGroupId}
                  onChange={setSelectedCustomGroupId}
                  size="small"
                  allowClear
                  style={{
                    flex: 1,
                    borderRadius: 8,
                    ...(themeMode === 'dark' && (colors as { inputBg?: string }).inputBg
                      ? { backgroundColor: (colors as { inputBg?: string }).inputBg }
                      : {}),
                  }}
                >
                  {customGroups.map(group => (
                    <Option key={group.id} value={group.id}>
                      <Space>
                        <div
                          style={{
                            width: 12,
                            height: 12,
                            borderRadius: '50%',
                            backgroundColor: group.color || '#1890ff',
                          }}
                        />
                        <span>{group.group_name}</span>
                        <Tag color="default" style={{ marginLeft: 'auto' }}>
                          {group.member_count || 0}
                        </Tag>
                      </Space>
                    </Option>
                  ))}
                </Select>
                <Button
                  type="text"
                  icon={<EditOutlined />}
                  size="small"
                  onClick={() => setManageGroupsModalVisible(true)}
                  title="Manage custom groups"
                  style={{ flexShrink: 0 }}
                />
              </div>
            </div>
          )}

          {!watchlistCollapsed && (
            <div className="watchlist-list-scroll">
              <Spin spinning={loading || pricesLoading}>
                {renderWatchlistList()}
              </Spin>
            </div>
          )}

          {watchlistCollapsed && (
            <div style={{ 
              padding: '16px 8px', 
              borderTop: `1px solid ${colors.siderBorder}`,
              textAlign: 'center',
              flexShrink: 0
            }}>
              <Button
                type="text"
                icon={<MenuUnfoldOutlined />}
                onClick={() => setWatchlistCollapsed(false)}
                title="展开Watchlist"
                style={{
                  fontSize: '16px',
                  width: 32,
                  height: 32,
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  margin: '0 auto'
                }}
              />
            </div>
          )}
        </div>

        {/* 右侧内容区域（固定卡片，仅 Tab 内容区滚动） */}
        <div
          className="watchlist-detail-panel"
          style={{
            background: colors.contentBg,
            border: `1px solid ${colors.siderBorder}`,
            ...(themeMode === 'dark' ? { boxShadow: CARD_STYLE_BEHANCE_DARK.boxShadow } : { boxShadow: CARD_STYLE_BEHANCE.boxShadow }),
          }}
        >
          <div className="watchlist-detail-inner" style={{ color: colors.text }}>
            {loading ? (
              <div style={{ 
                display: 'flex', 
                justifyContent: 'center', 
                alignItems: 'center', 
                flex: 1,
                minHeight: 200,
                flexDirection: 'column'
              }}>
                <Spin size="large" tip="Loading watchlist..." />
              </div>
            ) : selectedStock ? (
              <Tabs 
                activeKey={activeTab}
                onChange={(key) => {
                  setActiveTab(key);
                  if (key === 'reports' && selectedStock) {
                    loadResearchReports(selectedStock.symbol);
                  } else if (key === 'news') {
                    loadRelatedNews();
                  } else if (key === 'overview' && selectedStock) {
                    loadStockOverview(selectedStock);
                  }
                }}
                size="large"
              >
                <TabPane 
                  tab={
                    <span>
                      <FileTextOutlined />
                      News
                    </span>
                  } 
                  key="news"
                >
                  {renderNewsTab()}
                </TabPane>
                <TabPane
                  tab={
                    <span>
                      <DashboardOutlined />
                      Overview
                    </span>
                  }
                  key="overview"
                >
                  {renderOverviewTab()}
                </TabPane>
                <TabPane 
                  tab={
                    <span>
                      <FileTextOutlined />
                      Sources
                    </span>
                  } 
                  key="reports"
                >
                  {renderResearchReportsTab()}
                </TabPane>
              </Tabs>
            ) : (
              <div style={{ 
                display: 'flex', 
                justifyContent: 'center', 
                alignItems: 'center', 
                flex: 1,
                minHeight: 200,
                flexDirection: 'column'
              }}>
                <Empty
                  image={Empty.PRESENTED_IMAGE_SIMPLE}
                  description={
                    <span>
                      Please select a stock from the left<br />
                      to view related news and details
                    </span>
                  }
                />
              </div>
            )}
          </div>
        </div>
      </div>
    </div>

      {/* 添加股票到关注列表的模态框 */}
      <AddToWatchlistModal
        open={addModalVisible}
        onCancel={() => setAddModalVisible(false)}
        onSuccess={handleAddSuccess}
      />

      {/* 编辑watchlist项的模态框 */}
      <EditWatchlistItemModal
        open={editModalVisible}
        item={editingItem}
        onCancel={() => {
          setEditModalVisible(false);
          setEditingItem(null);
        }}
        onSuccess={handleEditSuccess}
      />

      {/* 添加到自定义组的模态框 */}
      <AddToCustomGroupModal
        visible={addToGroupModalVisible}
        selectedWatchlistIds={Array.from(selectedWatchlistIds)}
        onClose={() => setAddToGroupModalVisible(false)}
        onSuccess={handleAddToGroupSuccess}
      />

      {/* 管理自定义组的模态框 */}
      <ManageCustomGroupsModal
        visible={manageGroupsModalVisible}
        onClose={() => setManageGroupsModalVisible(false)}
        onUpdate={loadCustomGroups}
      />

      {/* AI分析详情模态框 */}
      <Modal
        title={
          <Space>
            {selectedNewsForAnalysis?.analysis_result && (
              <span
                style={{
                  fontSize: 10,
                  fontWeight: 600,
                  padding: '1px 4px',
                  borderRadius: 2,
                  background: themeMode === 'dark' ? 'rgba(93, 173, 226, 0.08)' : 'rgba(24, 144, 255, 0.06)',
                  color: themeMode === 'dark' ? 'rgba(93, 173, 226, 0.75)' : 'rgba(24, 144, 255, 0.75)',
                  border: `1px solid ${themeMode === 'dark' ? 'rgba(93, 173, 226, 0.35)' : 'rgba(24, 144, 255, 0.35)'}`,
                }}
              >
                AI
              </span>
            )}
            <span style={{ color: themeMode === 'dark' ? colors.text : undefined }}>
              {selectedNewsForAnalysis?.analysis_result ? 'AI Analysis Details' : 'News Details'}
            </span>
          </Space>
        }
        open={analysisModalVisible}
        centered
        onCancel={() => {
          setAnalysisModalVisible(false);
          setSelectedNewsForAnalysis(null);
        }}
        footer={[
          <Button key="close" onClick={() => {
            setAnalysisModalVisible(false);
            setSelectedNewsForAnalysis(null);
          }}>
            Close
          </Button>,
          selectedNewsForAnalysis?.link && (
            <Button 
              key="original" 
              type="primary"
              onClick={() => window.open(selectedNewsForAnalysis.link, '_blank')}
            >
              View Original
            </Button>
          )
        ]}
        width={800}
        className="analysis-details-modal"
        styles={{
          body: {
            height: ANALYSIS_MODAL_BODY_HEIGHT,
            minHeight: '480px',
            display: 'flex',
            flexDirection: 'column',
            overflow: 'hidden',
            ...(themeMode === 'dark' ? { backgroundColor: MODAL_DARK_BG } : {}),
          },
          ...(themeMode === 'dark' ? {
            wrapper: { backdropFilter: 'brightness(0.7)' },
            content: {
              backgroundColor: MODAL_DARK_BG,
              border: `1px solid ${MODAL_DARK_BORDER}`,
              boxShadow: '0 8px 32px rgba(0,0,0,0.5), 0 0 0 1px rgba(255,255,255,0.06)',
            },
            header: {
              backgroundColor: MODAL_DARK_BG,
              borderBottomColor: MODAL_DARK_BORDER,
            },
          } : {}),
        }}
      >
        {selectedNewsForAnalysis && (() => {
          const isDarkModal = themeMode === 'dark';
          const modalCardBg = isDarkModal ? colors.cardBg : '#f5f5f5';
          const modalHeadBg = isDarkModal ? 'rgba(22, 119, 255, 0.2)' : '#e6f7ff';
          const modalHeadBgAlt = isDarkModal ? colors.siderBg : '#f5f5f5';
          const modalHeadBgAlt2 = isDarkModal ? colors.siderBg : '#f0f5ff';
          const modalText = colors.text;
          const modalTextSecondary = colors.textSecondary;
          const modalSectionBg = isDarkModal ? colors.siderBg : '#fafafa';
          const modalBoxBg = isDarkModal ? colors.cardBg : '#fff';
          const modalBorder = colors.cardBorder;
          const modalLabelColor = isDarkModal ? 'rgba(255,255,255,0.65)' : '#595959';
          const modalMetaColor = isDarkModal ? 'rgba(255,255,255,0.45)' : '#999';
          const modalContentColor = isDarkModal ? 'rgba(255,255,255,0.85)' : '#666';
          const tabContentStyle = { padding: '4px 0', height: '100%', overflow: 'auto' as const };
          return (
          <div style={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
            <Card size="small" style={{ flexShrink: 0, marginBottom: '16px', backgroundColor: modalCardBg, borderColor: modalBorder }}>
              <Typography.Title level={5} style={{ marginBottom: '12px', color: modalText }}>
                {selectedNewsForAnalysis.title}
              </Typography.Title>
              <Space size="small" wrap>
                <Text style={{ fontSize: '12px', color: modalTextSecondary }}>
                  <CalendarOutlined /> {selectedNewsForAnalysis.date ? new Date(selectedNewsForAnalysis.date).toLocaleString() : 'N/A'}
                </Text>
                <Text style={{ fontSize: '12px', color: modalTextSecondary }}>
                  Source: {selectedNewsForAnalysis.source || 'N/A'}
                </Text>
              </Space>
            </Card>

            {(() => {
              const allResults = getAllAnalysisResults(selectedNewsForAnalysis.analysis_result);
              const hasAnyAnalysis = Object.keys(allResults).length > 0;
              const summaryResult = getAnalysisContent(selectedNewsForAnalysis.analysis_result, 'news_summary_v1');

              if (hasAnyAnalysis) {
                const tagLabelStyle = { fontSize: '11px', color: modalLabelColor, display: 'block' as const, marginBottom: '10px', fontWeight: 500 };
                const tagBoxStyle = { padding: '12px', backgroundColor: modalBoxBg, borderRadius: '6px', border: `1px solid ${modalBorder}` };
                const tagItemStyle = { fontSize: '11px', padding: '4px 10px', borderRadius: '4px', fontWeight: 500, border: 'none', boxShadow: '0 1px 2px rgba(0,0,0,0.1)' };

                return (
                  <Tabs
                    defaultActiveKey="ai"
                    size="small"
                    style={{ marginTop: '4px', flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}
                    items={[
                      {
                        key: 'ai',
                        label: 'AI Analysis',
                        children: summaryResult && typeof summaryResult.content === 'string' ? (
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
                        children: selectedNewsForAnalysis.content ? (
                          <div style={{ fontSize: '13px', color: modalContentColor, lineHeight: '1.8', ...tabContentStyle }}>
                            {highlightKeywords(selectedNewsForAnalysis.content)}
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
                              {selectedNewsForAnalysis.symbols && selectedNewsForAnalysis.symbols.length > 0 && (
                                <div style={tagBoxStyle}>
                                  <Text strong style={tagLabelStyle}>Related Tickers</Text>
                                  <Space size={[6, 6]} wrap>
                                    {selectedNewsForAnalysis.symbols.map(symbol => (
                                      <Tag key={symbol} color="blue" style={tagItemStyle}>{symbol}</Tag>
                                    ))}
                                  </Space>
                                </div>
                              )}
                              {/* Classification Tags */}
                            {(() => {
                              const classification = getClassificationFromAnalysis(selectedNewsForAnalysis.analysis_result);
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
                                      backgroundColor: modalBoxBg,
                                      borderRadius: '6px',
                                      border: `1px solid ${modalBorder}`
                                    }}>
                                      <Text strong style={{ 
                                        fontSize: '11px', 
                                        color: modalLabelColor,
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
                                      backgroundColor: modalBoxBg,
                                      borderRadius: '6px',
                                      border: `1px solid ${modalBorder}`
                                    }}>
                                      <Text strong style={{ 
                                        fontSize: '11px', 
                                        color: modalLabelColor,
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
                                      backgroundColor: modalBoxBg,
                                      borderRadius: '6px',
                                      border: `1px solid ${modalBorder}`
                                    }}>
                                      <Text strong style={{ 
                                        fontSize: '11px', 
                                        color: modalLabelColor,
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

                            {/* Unified Themes (统一主题) */}
                            {(() => {
                              if (!hasValidUnifiedThemes(selectedNewsForAnalysis.unified_themes)) {
                                return null;
                              }

                              const groupedThemes = groupThemesByCanonicalName(selectedNewsForAnalysis.unified_themes || []);
                              
                              if (groupedThemes.size === 0) {
                                return null;
                              }

                              return (
                                <div style={{ 
                                  padding: '12px',
                                  backgroundColor: modalBoxBg,
                                  borderRadius: '6px',
                                  border: `1px solid ${modalBorder}`
                                }}>
                                  <div style={{ marginBottom: '10px' }}>
                                    <Text strong style={{ 
                                      fontSize: '11px', 
                                      color: modalLabelColor,
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
                                  <div style={{ marginTop: '12px', paddingTop: '12px', borderTop: `1px solid ${modalBorder}` }}>
                                    <Text style={{ fontSize: '10px', color: modalMetaColor, marginRight: '8px' }}>
                                      Sources:
                                    </Text>
                                    <Space size={[4, 4]} wrap>
                                      {Array.from(new Set(selectedNewsForAnalysis.unified_themes?.map((t: NewsUnifiedTheme) => t.source_type) || [])).map((sourceType: string) => {
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
                                const classification = getClassificationFromAnalysis(selectedNewsForAnalysis.analysis_result);
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
                                  <Card key={templateCode} title={`${templateCode}`} size="small" style={{ marginTop: '8px', borderColor: modalBorder }} headStyle={{ backgroundColor: modalHeadBgAlt2, fontWeight: 'bold', color: modalText, borderColor: modalBorder }}>
                                    <pre style={{ fontSize: '12px', whiteSpace: 'pre-wrap', wordBreak: 'break-word', margin: 0, color: modalText }}>
                                      {typeof result.content === 'string' ? result.content : JSON.stringify(result.content, null, 2)}
                                    </pre>
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
                );
              } else {
                return (
                  <Card 
                    title="News Content" 
                    size="small"
                    style={{ borderColor: modalBorder }}
                    headStyle={{ backgroundColor: modalHeadBgAlt, fontWeight: 'bold', color: modalText, borderColor: modalBorder }}
                  >
                    <div style={{ fontSize: '14px', lineHeight: '1.8', color: modalText }}>
                      {selectedNewsForAnalysis.content 
                        ? highlightKeywords(selectedNewsForAnalysis.content)
                        : 'No content available'}
                    </div>
                  </Card>
                );
              }
            })()}
          </div>
          );
        })()}
      </Modal>
    </ConfigProvider>
  );
};

export default FinvizDashboard;
