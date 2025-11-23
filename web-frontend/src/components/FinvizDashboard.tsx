import React, { useState, useEffect, useCallback, useMemo } from 'react';
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
  Layout,
  Input,
  Tabs,
  List,
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
} from 'antd';
import type { MenuProps } from 'antd';
import MarkdownRenderer from './MarkdownRenderer';
import { getPreferredAnalysisContent, hasAnalysisForTemplate, getAnalysisContent, getAllAnalysisResults } from '../utils/newsAnalysisHelper';
import { 
  getClassificationFromAnalysis, 
  getClassificationTags, 
  hasValidClassification,
  getClassificationSummary
} from '../utils/newsClassificationHelper';
import {
  SearchOutlined, 
  FileTextOutlined,
  CalendarOutlined,
  PlusOutlined,
  HeartOutlined,
  BankOutlined,
  DownloadOutlined,
  EyeOutlined,
  DashboardOutlined,
  InfoCircleOutlined,
  LineChartOutlined,
  DollarOutlined,
  BarChartOutlined,
  FilePdfOutlined,
  FileExcelOutlined,
  FileWordOutlined,
  FileImageOutlined,
  StarFilled,
  StarOutlined,
  MenuFoldOutlined,
  MenuUnfoldOutlined,
  ArrowUpOutlined,
  ArrowDownOutlined,
  MoreOutlined,
  RobotOutlined,
  EditOutlined,
  DeleteOutlined,
  CalculatorOutlined,
  HolderOutlined
} from '@ant-design/icons';
import { useGlobalLogger } from '../contexts/LoggerContext';
import { dashboardAPI, type DashboardNewsItem, type WatchlistGroup, type WatchlistItem, type ResearchReport, type StockPriceData, type CustomGroup } from '../services/dashboard';
import { fundamentalsAPI, fundamentalsUtils, type CompanyFundamentalsResponse } from '../services/fundamentals';
import { filesAPI } from '../services/files';
import { newsAPI } from '../services/news';
import type { NewsTheme } from '../types/news';
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
const { Content, Sider } = Layout;
const { Title, Text, Paragraph } = Typography;
const { TabPane } = Tabs;
const { Option } = Select;

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
}) => {
  const { attributes, listeners, setNodeRef, transform, transition, isDragging } = useSortable({
    id: item.watchlist_id,
    disabled,
  });

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
  const changeColor = getChangeColor(change);
  const displayName = item.custom_name || item.company || item.symbol;

  return (
    <div ref={setNodeRef} style={containerStyle} {...attributes}>
      <div
        onClick={() => {
          if (!disabled) {
            if (isEditMode && onCheckboxToggle) {
              // 在编辑模式下，点击整个卡片切换复选框
              onCheckboxToggle(item.watchlist_id);
            } else {
              // 正常模式下，选择股票
              onSelect(item as WatchlistStockInfo);
            }
          }
        }}
        style={{
          display: 'flex',
          alignItems: 'center',
          gap: '12px',
          padding: '8px 12px',
          height: '60px',
          borderRadius: '10px',
          border: isEditMode && isChecked 
            ? '1px solid #52c41a' 
            : isSelected 
            ? '1px solid #91d5ff' 
            : '1px solid #f0f0f0',
          background: isEditMode && isChecked 
            ? '#f6ffed' 
            : isSelected 
            ? '#e6f7ff' 
            : '#ffffff',
          boxShadow: isEditMode && isChecked
            ? '0 2px 8px rgba(82, 196, 26, 0.15)'
            : isSelected
            ? '0 2px 8px rgba(24, 144, 255, 0.15)'
            : isDragging
            ? '0 4px 12px rgba(0, 0, 0, 0.12)'
            : '0 1px 2px rgba(0, 0, 0, 0.02)',
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
              color: '#bfbfbf',
              cursor: disabled ? 'not-allowed' : 'grab',
              userSelect: 'none',
            }}
          >
            <HolderOutlined />
          </div>
        )}

        <div style={{ display: 'flex', flexDirection: 'column', gap: '4px', flex: 1, minWidth: 0 }}>
          <Text
            style={{
              fontSize: '14px',
              fontWeight: 500,
              margin: 0,
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            {displayName}
          </Text>
          <div style={{ display: 'flex', alignItems: 'center', gap: '6px', minWidth: 0 }}>
            <Text
              type="secondary"
              style={{
                fontSize: '12px',
                margin: 0,
                textTransform: 'uppercase',
                overflow: 'hidden',
                textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
              }}
            >
              {displaySymbol}
            </Text>
            {item.priority ? renderStarsFn(item.priority) : null}
          </div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: '4px' }}>
          <Text strong style={{ fontSize: '14px', color: changeColor, margin: 0 }}>
            {formatPrice(priceData?.close)}
          </Text>
          <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
            {hasChange &&
              (change! >= 0 ? (
                <ArrowUpOutlined style={{ color: '#cf1322', fontSize: '12px' }} />
              ) : (
                <ArrowDownOutlined style={{ color: '#3f8600', fontSize: '12px' }} />
              ))}
            <Text style={{ fontSize: '12px', color: changeColor, fontWeight: 500, margin: 0 }}>
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
  
  // 状态管理
  const [selectedStock, setSelectedStock] = useState<WatchlistStockInfo | null>(null);
  const [searchValue, setSearchValue] = useState<string>('');
  const [priorityFilter, setPriorityFilter] = useState<number | string>('all');
  const [exchangeFilter, setExchangeFilter] = useState<ExchangeFilterValue>('all');
  const [activeTab, setActiveTab] = useState<string>('overview');
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
  const NEWS_PAGE_SIZE = 10; // 每页加载10条新闻
  
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
        setActiveTab('overview');

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
      setActiveTab('overview');
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

    setActiveTab('overview');
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

  // 滚动检测和自动加载更多新闻
  useEffect(() => {
    const handleScroll = () => {
      if (!hasMoreNews || loadingMoreNews || activeTab !== 'news') return;
      
      const scrollContainer = document.querySelector('.ant-layout-content');
      if (!scrollContainer) return;
      
      const { scrollTop, scrollHeight, clientHeight } = scrollContainer;
      const threshold = 200; // 距离底部200px时触发加载
      
      if (scrollTop + clientHeight >= scrollHeight - threshold) {
        loadMoreNews();
      }
    };

    const scrollContainer = document.querySelector('.ant-layout-content');
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
  const handleDownloadFile = async (report: ResearchReport) => {
    try {
      logger.logInfo(`开始下载文件: ${report.file_name}`, 'finviz-dashboard');
      
      // 使用dashboard API的downloadFile方法，这与AI Analysis download功能相同的方法
      const blob = await dashboardAPI.downloadFile(report.id);
      
      // 创建下载链接
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = report.file_name;
      link.style.visibility = 'hidden';
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
      URL.revokeObjectURL(url);
      
      message.success(`Downloading file: ${report.file_name}`);
      logger.logInfo(`下载文件成功: ${report.file_name}`, 'finviz-dashboard');
    } catch (error) {
      logger.logError(`下载文件失败: ${report.file_name}`, 'finviz-dashboard', error);
      message.error('Download failed');
    }
  };

  // 查看文件内容
  const handleViewFile = async (report: ResearchReport) => {
    try {
      const originalFileInfo = await filesAPI.getOriginalFileInfo(report.id);
      logger.logInfo(`查看文件: ${report.file_name}`, 'finviz-dashboard');
      
      const fileType = originalFileInfo.file_type.toLowerCase();
      
      if (fileType === 'pdf') {
        // PDF文件直接在新窗口打开
        window.open(originalFileInfo.file_url, '_blank');
      } else {
        // 非PDF文件显示黄色提示
        message.warning('Currently only PDF format files are supported for viewing.');
      }
    } catch (error) {
      logger.logError(`获取文件信息失败: ${report.file_name}`, 'finviz-dashboard', error);
      message.error('获取文件信息失败');
    }
  };

  // 获取文件类型图标
  const getFileTypeIcon = (fileType: string) => {
    switch (fileType.toLowerCase()) {
      case 'pdf':
        return <FilePdfOutlined style={{ color: '#ff4d4f' }} />;
      case 'excel':
        return <FileExcelOutlined style={{ color: '#52c41a' }} />;
      case 'doc':
        return <FileWordOutlined style={{ color: '#1890ff' }} />;
      case 'image':
        return <FileImageOutlined style={{ color: '#fa8c16' }} />;
      default:
        return <FileTextOutlined style={{ color: '#8c8c8c' }} />;
    }
  };

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
        setActiveTab('overview');
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
              gap: '8px',
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

  const renderNewsTab = () => (
    <div>
      {contentLoading ? (
        renderNewsSkeleton()
      ) : newsData.length === 0 ? (
        <Empty 
          description="No related news"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      ) : (
        <div style={{ position: 'relative' }}>
          {newsData.map((item, index) => (
            <div key={item.id} style={{ position: 'relative', marginBottom: '24px' }}>
              {/* Timeline line */}
              {index < newsData.length - 1 && (
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
                              {item.date
                                ? `${new Date(item.date).toLocaleDateString()} ${new Date(
                                    item.date
                                  ).toLocaleTimeString()}`
                                : 'N/A'}
                            </span>,
                            item.updated_at && (
                              <span key="updated">
                                Updated:{' '}
                                {`${new Date(item.updated_at).toLocaleDateString()} ${new Date(
                                  item.updated_at
                                ).toLocaleTimeString()}`}
                              </span>
                            ),
                            item.source && <span key="source">{item.source}</span>,
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
                          onClick={() => {
                            setSelectedNewsForAnalysis(item);
                            setAnalysisModalVisible(true);
                          }}
                        >
                          {item.title}
                        </span>
                      </Space>
                    }
                    description={null}
                  />
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
          
          {/* 加载更多按钮 */}
          {hasMoreNews && (
            <div style={{ textAlign: 'center', marginTop: '24px', marginBottom: '24px' }}>
              <Button 
                type="primary" 
                loading={loadingMoreNews}
                onClick={loadMoreNews}
                disabled={loadingMoreNews}
              >
                {loadingMoreNews ? 'Loading...' : 'Load More News'}
              </Button>
            </div>
          )}
          
          {/* 没有更多数据提示 */}
          {!hasMoreNews && newsData.length > 0 && (
            <div style={{ textAlign: 'center', marginTop: '24px', marginBottom: '24px' }}>
              <Text type="secondary">All news loaded</Text>
            </div>
          )}
        </div>
      )}
    </div>
  );

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
                        border: '1px solid #f0f0f0',
                        borderRadius: '8px',
                        backgroundColor: '#fafafa'
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
                        <div style={{ marginTop: 8, fontSize: '12px', color: '#666' }}>
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
        <div>
          {Array.from({ length: 3 }).map((_, index) => (
            <Card key={index} style={{ marginBottom: '16px' }}>
              <Skeleton active paragraph={{ rows: 2 }} />
            </Card>
          ))}
        </div>
      ) : researchReports.length === 0 ? (
        <Empty 
          description="No research reports found for this ticker"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      ) : (
        <List
          itemLayout="vertical"
          dataSource={researchReports}
          renderItem={report => (
            <List.Item
              key={report.id}
              extra={
                <Space direction="vertical" align="end">
                  <Space>
                    {getFileTypeIcon(report.file_type)}
                    <Tag color="blue">{report.file_type.toUpperCase()}</Tag>
                  </Space>
                  <Text type="secondary" style={{ fontSize: '12px' }}>
                    {(report.file_size / 1024 / 1024).toFixed(2)} MB
                  </Text>
                  <Text type="secondary" style={{ fontSize: '12px' }}>
                    <CalendarOutlined /> {new Date(report.uploaded_at).toLocaleDateString()}
                  </Text>
                </Space>
              }
            >
              <List.Item.Meta
                title={
                  <div style={{ fontSize: '14px', fontWeight: 'bold' }}>
                    {report.file_name}
                  </div>
                }
                description={
                  <Space wrap>
                    <Tag color="orange">{report.industry}</Tag>
                    <Tag color="blue">{report.ticker}</Tag>
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      Tagged: {new Date(report.tagged_at).toLocaleDateString()}
                    </Text>
                  </Space>
                }
              />
              <div style={{ marginTop: '8px' }}>
                <Space>
                  <Button 
                    type="text" 
                    size="small"
                    icon={<EyeOutlined />}
                    onClick={() => handleViewFile(report)}
                    title="查看文件"
                  />
                  <Button 
                    type="text" 
                    size="small"
                    icon={<DownloadOutlined />}
                    onClick={() => handleDownloadFile(report)}
                    title="下载文件"
                  />
                </Space>
              </div>
            </List.Item>
          )}
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

    return (
      <div style={{ marginBottom: 24 }}>
        {/* Candlestick Chart */}
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
        />
      </div>
    );
  };

  return (
    <div style={{ height: '100%', background: '#f5f5f5' }}>
      <Layout style={{ height: '100%' }}>
        {/* 左侧选择器 */}
        <Sider 
          width={watchlistCollapsed ? 80 : 350} 
          style={{ 
            background: '#fff', 
            borderRight: '1px solid #f0f0f0',
            height: '100%',
            overflowY: 'auto'
          }}
        >
          <div style={{ padding: watchlistCollapsed ? '16px 8px' : '16px' }}>
            <div style={{ 
              display: 'flex', 
              justifyContent: 'space-between', 
              alignItems: 'center', 
              marginBottom: '16px',
              flexDirection: watchlistCollapsed ? 'column' : 'row',
              gap: watchlistCollapsed ? '8px' : '0'
            }}>
              <Title 
                level={watchlistCollapsed ? 5 : 4} 
                style={{ 
                  margin: 0,
                  textAlign: watchlistCollapsed ? 'center' : 'left',
                  fontSize: watchlistCollapsed ? '14px' : '20px',
                  cursor: watchlistCollapsed ? 'pointer' : 'default'
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
                        />
                      </Tooltip>
                      <Tooltip title="Edit - Enter edit mode to select and manage multiple tickers">
                        <Button 
                          icon={<EditOutlined />} 
                          size="small"
                          onClick={handleToggleEditMode}
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
                      >
                        Add to ({selectedWatchlistIds.size})
                      </Button>
                      <Button 
                        size="small"
                        onClick={handleToggleEditMode}
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
              <>
                <Input
                  placeholder="Search stocks..."
                  prefix={<SearchOutlined />}
                  value={searchValue}
                  onChange={e => setSearchValue(e.target.value)}
                  style={{ marginBottom: '12px' }}
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
                
                <div style={{ display: 'flex', gap: '8px', marginBottom: '16px', alignItems: 'center' }}>
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
                    style={{ width: 140, border: 'none' }}
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

                <div style={{ marginBottom: '16px' }}>
                  <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                    <Select
                      placeholder="Custom Groups"
                      value={selectedCustomGroupId}
                      onChange={setSelectedCustomGroupId}
                      size="small"
                      allowClear
                      style={{ flex: 1 }}
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
              </>
            )}
            
            {!watchlistCollapsed && (
              <Spin spinning={loading || pricesLoading}>
                {renderWatchlistList()}
              </Spin>
            )}
            
            {watchlistCollapsed && (
              <div style={{ 
                padding: '16px 8px', 
                borderTop: '1px solid #f0f0f0',
                textAlign: 'center'
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
                    justifyContent: 'center'
                  }}
                />
              </div>
            )}
          </div>
        </Sider>

        {/* 右侧内容区域 */}
        <Content style={{ background: '#fff', margin: '0', height: '100%' }}>
          <div style={{ padding: '24px', height: '100%' }}>
            {loading ? (
              <div style={{ 
                display: 'flex', 
                justifyContent: 'center', 
                alignItems: 'center', 
                height: '50%',
                flexDirection: 'column'
              }}>
                <Spin size="large" tip="Loading watchlist..." />
              </div>
            ) : selectedStock ? (
              <>
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
                        <FileTextOutlined />
                        Research Report
                      </span>
                    } 
                    key="reports"
                  >
                    {renderResearchReportsTab()}
                  </TabPane>
                </Tabs>
              </>
            ) : (
              <div style={{ 
                display: 'flex', 
                justifyContent: 'center', 
                alignItems: 'center', 
                height: '50%',
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
        </Content>
      </Layout>
      
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
            <RobotOutlined style={{ color: '#1890ff' }} />
            <span>{selectedNewsForAnalysis?.analysis_result ? 'AI Analysis Details' : 'News Details'}</span>
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
      >
        {selectedNewsForAnalysis && (
          <>
            <Card size="small" style={{ marginBottom: '16px', backgroundColor: '#f5f5f5' }}>
              <Typography.Title level={5} style={{ marginBottom: '12px' }}>
                {selectedNewsForAnalysis.title}
              </Typography.Title>
              <Space size="small" wrap>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  <CalendarOutlined /> {selectedNewsForAnalysis.date ? new Date(selectedNewsForAnalysis.date).toLocaleString() : 'N/A'}
                </Text>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  Source: {selectedNewsForAnalysis.source || 'N/A'}
                </Text>
              </Space>
            </Card>


            {(() => {
              const allResults = getAllAnalysisResults(selectedNewsForAnalysis.analysis_result);
              const hasAnyAnalysis = Object.keys(allResults).length > 0;
              const summaryResult = getAnalysisContent(selectedNewsForAnalysis.analysis_result, 'news_summary_v1');

              if (hasAnyAnalysis) {
                return (
                  <>

                    {/* 优先显示 news_summary_v1 的结果 */}
                    {summaryResult && typeof summaryResult.content === 'string' && (
                      <>
                        <Card 
                          title="AI Analysis" 
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

                        {selectedNewsForAnalysis.content && (
                          <Card 
                            title="Original Content" 
                            size="small"
                            style={{ marginTop: '16px' }}
                            headStyle={{ backgroundColor: '#f5f5f5' }}
                          >
                            <div style={{ fontSize: '13px', color: '#666', lineHeight: '1.8' }}>
                              {highlightKeywords(selectedNewsForAnalysis.content)}
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
                                {selectedNewsForAnalysis.symbols && selectedNewsForAnalysis.symbols.length > 0 && (
                                  <div>
                                    <Text strong style={{ fontSize: '9px', color: '#666' }}>Related Tickers:</Text>
                                    <Space size={[4, 4]} wrap style={{ marginTop: '12px' }}>
                                      {selectedNewsForAnalysis.symbols.map(symbol => (
                                        <Tag key={symbol} color="default" style={{ fontSize: '9px' }}>
                                          {symbol}
                                        </Tag>
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

                                    </>
                                  );
                                })()}

                                {/* Analysis Template Tags */}
                                {(() => {
                                  const allResults = getAllAnalysisResults(selectedNewsForAnalysis.analysis_result);
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
                                  const classification = getClassificationFromAnalysis(selectedNewsForAnalysis.analysis_result);
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
                      {selectedNewsForAnalysis.content 
                        ? highlightKeywords(selectedNewsForAnalysis.content)
                        : 'No content available'}
                    </div>
                  </Card>
                );
              }
            })()}
          </>
        )}
      </Modal>

    </div>
  );
};

export default FinvizDashboard;
