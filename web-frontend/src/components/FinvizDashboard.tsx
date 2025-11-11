import React, { useState, useEffect, useCallback } from 'react';
import {
  Layout,
  Tree,
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
} from 'antd';
import MarkdownRenderer from './MarkdownRenderer';
import { getPreferredAnalysisContent, hasAnalysisForTemplate, getAnalysisContent, getAllAnalysisResults } from '../utils/newsAnalysisHelper';
import { 
  getClassificationFromAnalysis, 
  getClassificationTags, 
  hasValidClassification,
  getClassificationSummary,
  getRelatedSectors
} from '../utils/newsClassificationHelper';
import {
  SearchOutlined, 
  StockOutlined, 
  FileTextOutlined,
  CalendarOutlined,
  PlusOutlined,
  HeartOutlined,
  ExperimentOutlined,
  PhoneOutlined,
  ShoppingOutlined,
  SafetyOutlined,
  ThunderboltOutlined,
  BankOutlined,
  MedicineBoxOutlined,
  ToolOutlined,
  HomeOutlined,
  LaptopOutlined,
  SettingOutlined,
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
  RobotOutlined,
  EditOutlined,
  DeleteOutlined,
  CalculatorOutlined
} from '@ant-design/icons';
import { useGlobalLogger } from '../contexts/LoggerContext';
import { dashboardAPI, type DashboardNewsItem, type WatchlistGroup, type WatchlistItem, type ResearchReport, type StockPriceData } from '../services/dashboard';
import { fundamentalsAPI, fundamentalsUtils, type CompanyFundamentalsResponse } from '../services/fundamentals';
import { filesAPI } from '../services/files';
import { newsAPI } from '../services/news';
import type { NewsTheme } from '../types/news';
import AddToWatchlistModal from './AddToWatchlistModal';
import EditWatchlistItemModal from './EditWatchlistItemModal';
import StockChart from './StockChart';
import type { DataNode } from 'antd/es/tree';

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

// Helper function to get unique exchanges from watchlist data
const getUniqueExchanges = (watchlistGroups: WatchlistGroup[]): string[] => {
  const exchanges = new Set<string>();
  watchlistGroups.forEach(group => {
    group.items.forEach(item => {
      if (item.exchange) {
        exchanges.add(item.exchange);
      }
    });
  });
  return Array.from(exchanges).sort();
};

// Helper function to get unique sectors from watchlist data
const getUniqueSectors = (watchlistGroups: WatchlistGroup[]): string[] => {
  const sectors = new Set<string>();
  watchlistGroups.forEach(group => {
    group.items.forEach(item => {
      if (item.sector) {
        sectors.add(item.sector);
      }
    });
  });
  return Array.from(sectors).sort();
};

// 数据状态类型定义
interface WatchlistStockInfo extends WatchlistItem {
  // 添加一些前端特有的属性（如果需要）
}

// Sector图标映射函数
const getSectorIcon = (sector: string) => {
  const sectorLower = sector?.toLowerCase() || '';
  
  switch (sectorLower) {
    case 'basic materials':
      return <ExperimentOutlined style={{ color: '#8B4513' }} />;
    case 'communication services':
      return <PhoneOutlined style={{ color: '#1890ff' }} />;
    case 'consumer cyclical':
      return <ShoppingOutlined style={{ color: '#52c41a' }} />;
    case 'consumer defensive':
      return <SafetyOutlined style={{ color: '#faad14' }} />;
    case 'energy':
      return <ThunderboltOutlined style={{ color: '#ff7a45' }} />;
    case 'financial':
      return <BankOutlined style={{ color: '#722ed1' }} />;
    case 'healthcare':
      return <MedicineBoxOutlined style={{ color: '#f5222d' }} />;
    case 'industrials':
      return <ToolOutlined style={{ color: '#13c2c2' }} />;
    case 'real estate':
      return <HomeOutlined style={{ color: '#eb2f96' }} />;
    case 'technology':
      return <LaptopOutlined style={{ color: '#2f54eb' }} />;
    case 'utilities':
      return <SettingOutlined style={{ color: '#52c41a' }} />;
    default:
      return <HeartOutlined style={{ color: '#ff4d4f' }} />;
  }
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

const FinvizDashboard: React.FC = () => {
  const logger = useGlobalLogger();
  
  // 状态管理
  const [selectedKey, setSelectedKey] = useState<string>('');
  const [selectedType, setSelectedType] = useState<'group' | 'stock' | ''>('');
  const [selectedStock, setSelectedStock] = useState<WatchlistStockInfo | null>(null);
  const [selectedGroup, setSelectedGroup] = useState<string>('');
  const [searchValue, setSearchValue] = useState<string>('');
  const [priorityFilter, setPriorityFilter] = useState<number | string>('all');
  const [exchangeFilter, setExchangeFilter] = useState<string>('all');
  const [sectorFilter, setSectorFilter] = useState<string>('all');
  const [activeTab, setActiveTab] = useState<string>('overview');
  const [expandedKeys, setExpandedKeys] = useState<React.Key[]>([]);
  const [watchlistCollapsed, setWatchlistCollapsed] = useState<boolean>(false);
  
  // 数据状态
  const [watchlistGroups, setWatchlistGroups] = useState<WatchlistGroup[]>([]);
  const [treeData, setTreeData] = useState<DataNode[]>([]);
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
  const [contextMenuVisible, setContextMenuVisible] = useState<boolean>(false);
  const [contextMenuPosition, setContextMenuPosition] = useState<{ x: number; y: number }>({ x: 0, y: 0 });
  const [contextMenuItem, setContextMenuItem] = useState<WatchlistItem | null>(null);
  const [analysisModalVisible, setAnalysisModalVisible] = useState<boolean>(false);
  const [selectedNewsForAnalysis, setSelectedNewsForAnalysis] = useState<DashboardNewsItem | null>(null);
  
  // 错误状态跟踪，防止重复显示相同的错误消息
  const [lastError, setLastError] = useState<string | null>(null);

  // 初始化数据
  useEffect(() => {
    loadUserWatchlist();
    loadThemes();
    logger.logInfo('FinvizDashboard页面已加载', 'finviz-dashboard');
    
    // 定期清理过期缓存
    const cleanupInterval = setInterval(cleanupExpiredCache, 60000); // 每分钟清理一次
    
    return () => clearInterval(cleanupInterval);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

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

  // 搜索过滤
  useEffect(() => {
    filterTreeData();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchValue, priorityFilter, exchangeFilter, sectorFilter, watchlistGroups, stockPrices]);

  // 确保初次加载时自动选择第一个股票
  useEffect(() => {
    if (watchlistGroups.length > 0 && !selectedKey && !loading) {
      const firstGroup = watchlistGroups.find(group => group.items.length > 0);
      if (firstGroup && firstGroup.items.length > 0) {
        const firstStock = firstGroup.items[0];
        const stockKey = `stock-${firstStock.watchlist_id}`;
        
        console.log('Auto-selecting first stock:', {
          symbol: firstStock.symbol,
          company: firstStock.company,
          primaryticker: firstStock.primaryticker,
          watchlist_id: firstStock.watchlist_id
        });
        
        // 设置选中状态
        setSelectedKey(stockKey);
        setSelectedType('stock');
        setSelectedStock(firstStock);
        setSelectedGroup('');
        setActiveTab('overview');
        
        const query = getOverviewQueryForStock(firstStock);
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
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [watchlistGroups, loading]);

  useEffect(() => {
    if (selectedType !== 'stock') {
      setOverviewData(null);
      setOverviewError(null);
      setOverviewLoading(false);
    }
  }, [selectedType]);

  // 当选择改变时加载相关内容（添加防抖）
  useEffect(() => {
    if (selectedKey && activeTab === 'news') {
      const timer = setTimeout(() => {
        loadRelatedNews();
      }, 300); // 300ms 防抖
      
      return () => clearTimeout(timer);
    } else if (selectedKey && activeTab === 'reports' && selectedStock) {
      const timer = setTimeout(() => {
        loadResearchReports(selectedStock.symbol);
      }, 300); // 300ms 防抖
      
      return () => clearTimeout(timer);
    } else if (selectedKey && activeTab === 'overview' && selectedStock) {
      const timer = setTimeout(() => {
        loadStockOverview(selectedStock);
      }, 300); // 300ms 防抖
      
      return () => clearTimeout(timer);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedKey, selectedType, activeTab, selectedStock]);

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

  const getOverviewQueryForStock = (stock: WatchlistStockInfo | null): string | null => {
    if (!stock) return null;
    if (stock.primaryticker && stock.primaryticker.trim()) {
      return stock.primaryticker.trim();
    }
    if (stock.symbol && stock.symbol.trim()) {
      return stock.symbol.trim();
    }
    return null;
  };

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
    [overviewCache, overviewLastError, logger]
  );

  const filterTreeData = () => {
    const filteredGroups = watchlistGroups.filter(group => {
      // Apply all filters to items
      let filteredItems = group.items;
      
      // Filter by priority
      if (priorityFilter !== 'all') {
        filteredItems = filteredItems.filter(item => item.priority === priorityFilter);
      }
      
      // Filter by exchange
      if (exchangeFilter !== 'all') {
        filteredItems = filteredItems.filter(item => item.exchange === exchangeFilter);
      }
      
      // Filter by sector
      if (sectorFilter !== 'all') {
        filteredItems = filteredItems.filter(item => item.sector === sectorFilter);
      }
      
      // If no items match the filters, hide the entire group
      if (filteredItems.length === 0) {
        return false;
      }
      
      // Then filter by search value
      if (!searchValue) return true;
      
      const groupMatch = group.group_name.toLowerCase().includes(searchValue.toLowerCase());
      const stockMatch = filteredItems.some(item => 
        item.symbol.toLowerCase().includes(searchValue.toLowerCase()) ||
        (item.company && item.company.toLowerCase().includes(searchValue.toLowerCase())) ||
        (item.custom_name && item.custom_name.toLowerCase().includes(searchValue.toLowerCase())) ||
        (item.sector && item.sector.toLowerCase().includes(searchValue.toLowerCase()))
      );
      
      return groupMatch || stockMatch;
    });

    const treeNodes: DataNode[] = filteredGroups.map(group => {
      // Get filtered items based on all filters
      let filteredItems = group.items;
      
      // Apply all filters
      if (priorityFilter !== 'all') {
        filteredItems = filteredItems.filter(item => item.priority === priorityFilter);
      }
      
      if (exchangeFilter !== 'all') {
        filteredItems = filteredItems.filter(item => item.exchange === exchangeFilter);
      }
      
      if (sectorFilter !== 'all') {
        filteredItems = filteredItems.filter(item => item.sector === sectorFilter);
      }
      
      // Apply search filter to items
      if (searchValue) {
        filteredItems = filteredItems.filter(item => 
          item.symbol.toLowerCase().includes(searchValue.toLowerCase()) ||
          (item.company && item.company.toLowerCase().includes(searchValue.toLowerCase())) ||
          (item.custom_name && item.custom_name.toLowerCase().includes(searchValue.toLowerCase())) ||
          (item.sector && item.sector.toLowerCase().includes(searchValue.toLowerCase()))
        );
      }
      
      // 获取分组中股票的主要sector (based on filtered items)
      const sectors = filteredItems
        .map(item => item.sector)
        .filter((sector): sector is string => Boolean(sector))
        .reduce((acc, sector) => {
          acc[sector] = (acc[sector] || 0) + 1;
          return acc;
        }, {} as Record<string, number>);
      
      // 找到出现最频繁的sector
      const sectorKeys = Object.keys(sectors);
      const dominantSector = sectorKeys.length > 0 
        ? sectorKeys.reduce((a, b) => sectors[a] > sectors[b] ? a : b)
        : '';
      
      return {
        title: group.group_name,
        key: `group-${group.group_name}`,
        icon: getSectorIcon(dominantSector),
        children: filteredItems.map(item => {
          return {
            title: (
              <div style={{ 
                display: 'flex', 
                alignItems: 'center', 
                width: '100%',
                minHeight: '40px',
                padding: '4px 0'
              }}>
                <StockOutlined style={{ 
                  marginRight: '8px', 
                  color: '#1890ff', 
                  flexShrink: 0,
                  fontSize: '14px'
                }} />
                <div style={{ 
                  flex: 1, 
                  minWidth: 0,
                  display: 'flex',
                  flexDirection: 'column',
                  justifyContent: 'center'
                }}>
                  <div style={{ 
                    fontSize: '13px', 
                    fontWeight: 'bold', 
                    overflow: 'hidden', 
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                    lineHeight: '1.2'
                  }}>
                    {item.custom_name || item.company || item.symbol}
                  </div>
                  <div style={{ 
                    fontSize: '11px', 
                    color: '#666', 
                    fontFamily: 'monospace', 
                    letterSpacing: '0.5px',
                    lineHeight: '1.2',
                    display: 'flex',
                    alignItems: 'center',
                    gap: '4px'
                  }}>
                    <span>{item.symbol}</span>
                    {renderStars(item.priority)}
                  </div>
                </div>
              </div>
            ),
            key: `stock-${item.watchlist_id}`,
            isLeaf: true,
          };
        })
      };
    });

    setTreeData(treeNodes);
    
    // 设置所有分组节点为展开状态
    const groupKeys = filteredGroups.map(group => `group-${group.group_name}`);
    setExpandedKeys(groupKeys);
  };

  const handleTreeSelect = (selectedKeys: React.Key[], info: any) => {
    // 如果点击的是已选中的项目，保持选中状态不变
    if (selectedKeys.length === 0 && selectedKey) {
      // 重新设置选中状态，防止取消选择
      setSelectedKey(selectedKey);
      return;
    }
    
    if (selectedKeys.length === 0) {
      setSelectedKey('');
      setSelectedType('');
      setSelectedStock(null);
      setSelectedGroup('');
      setNewsData([]);
      setResearchReports([]);
      setOverviewData(null);
      setOverviewError(null);
      setOverviewLoading(false);
      setActiveTab('overview');
      return;
    }

    const key = selectedKeys[0] as string;
    setSelectedKey(key);

    if (key.startsWith('group-')) {
      setSelectedType('group');
      const groupName = key.replace('group-', '');
      setSelectedGroup(groupName);
      setSelectedStock(null);
      setOverviewData(null);
      setOverviewError(null);
      setOverviewLoading(false);
      setActiveTab('news');
      logger.logInfo(`选择了分组: ${groupName}`, 'finviz-dashboard');
    } else if (key.startsWith('stock-')) {
      setSelectedType('stock');
      const watchlistId = parseInt(key.replace('stock-', ''));
      
      // 在所有watchlist中查找对应的股票
      let foundStock: WatchlistStockInfo | null = null;
      for (const group of watchlistGroups) {
        foundStock = group.items.find(item => item.watchlist_id === watchlistId) || null;
        if (foundStock) break;
      }
      
      setSelectedStock(foundStock);
      setSelectedGroup('');
      setResearchReports([]); // 清空研究报告数据
      
      if (foundStock) {
        const query = getOverviewQueryForStock(foundStock);
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
      } else {
        setOverviewData(null);
        setOverviewError(null);
        setOverviewLoading(false);
        setActiveTab('news');
      }
      logger.logInfo(`选择了股票: ${foundStock?.custom_name || foundStock?.company} (${foundStock?.symbol})`, 'finviz-dashboard');
    }
  };

  const loadRelatedNews = async () => {
    if (!selectedKey) return;
    
    // 重置分页状态
    setNewsOffset(0);
    setHasMoreNews(true);
    
    // 生成缓存键
    const cacheKey = selectedType === 'stock' && selectedStock 
      ? getCacheKey('stock', selectedStock.symbol)
      : selectedType === 'group' && selectedGroup
      ? getCacheKey('group', selectedGroup)
      : null;
    
    // 检查缓存
    if (cacheKey) {
      const cachedNews = getCachedNews(cacheKey);
      if (cachedNews) {
        setNewsData(cachedNews);
        setNewsOffset(NEWS_PAGE_SIZE);
        setHasMoreNews(cachedNews.length === NEWS_PAGE_SIZE);
        logger.logInfo(`使用缓存数据: ${cacheKey}`, 'finviz-dashboard');
        return;
      }
    }
    
    setContentLoading(true);
    try {
      if (selectedType === 'stock' && selectedStock) {
        await loadStockNews(selectedStock.symbol, cacheKey, true);
      } else if (selectedType === 'group' && selectedGroup) {
        await loadGroupNews(selectedGroup, cacheKey, true);
      }
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

  const loadGroupNews = useCallback(async (groupName: string, cacheKey?: string | null, reset: boolean = true) => {
    try {
      // 获取该分组下所有股票的新闻
      const group = watchlistGroups.find(g => g.group_name === groupName);
      if (!group || group.items.length === 0) {
        setNewsData([]);
        return;
      }

      // 获取分组中第一个股票的新闻作为示例
      // 实际应用中可能需要合并多个股票的新闻
      const firstStock = group.items[0];
      if (firstStock.sector) {
        const offset = reset ? 0 : newsOffset;
        const news = await dashboardAPI.getSectorNews(firstStock.sector, { 
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
        
        logger.logInfo(`加载分组新闻: ${groupName}, ${news.length} 条 (offset: ${offset})`, 'finviz-dashboard');
      } else {
        setNewsData([]);
      }
    } catch (error) {
      logger.logError(`加载分组新闻失败: ${groupName}`, 'finviz-dashboard', error);
      if (reset) {
        setNewsData([]);
      }
    }
  }, [watchlistGroups, newsOffset, NEWS_PAGE_SIZE, setCachedNews, logger]);

  // 加载更多新闻
  const loadMoreNews = useCallback(async () => {
    if (!hasMoreNews || loadingMoreNews || !selectedKey) return;
    
    setLoadingMoreNews(true);
    try {
      if (selectedType === 'stock' && selectedStock) {
        await loadStockNews(selectedStock.symbol, null, false);
      } else if (selectedType === 'group' && selectedGroup) {
        await loadGroupNews(selectedGroup, null, false);
      }
    } catch (error) {
      logger.logError('Failed to load more news', 'finviz-dashboard', error);
      message.error('Failed to load more news');
    } finally {
      setLoadingMoreNews(false);
    }
  }, [hasMoreNews, loadingMoreNews, selectedKey, selectedType, selectedStock, selectedGroup, loadGroupNews, loadStockNews, logger]);

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
  const handleRemoveFromWatchlist = async (watchlistId: number, symbol: string) => {
    try {
      const result = await dashboardAPI.removeFromWatchlist(watchlistId);
      message.success(result.message);
      
      // 重新加载watchlist
      await loadUserWatchlist();
      
      // 如果删除的是当前选中的股票，清空选择
      if (selectedStock?.watchlist_id === watchlistId) {
        setSelectedKey('');
        setSelectedType('');
        setSelectedStock(null);
        setNewsData([]);
      }
      
      logger.logInfo(`从watchlist移除: ${symbol}`, 'finviz-dashboard');
    } catch (error) {
      message.error('Failed to remove');
      logger.logError(`Failed to remove watchlist item: ${symbol}`, 'finviz-dashboard', error);
    }
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

  // 处理右键点击
  const handleRightClick = (info: any) => {
    const { event, node } = info;
    event.preventDefault();
    event.stopPropagation();

    // 只处理股票节点（不是分组节点）
    if (node.key && node.key.toString().startsWith('stock-')) {
      const watchlistId = parseInt(node.key.toString().replace('stock-', ''));
      
      // 在所有watchlist中查找对应的股票
      let foundItem: WatchlistItem | null = null;
      for (const group of watchlistGroups) {
        foundItem = group.items.find(item => item.watchlist_id === watchlistId) || null;
        if (foundItem) break;
      }
      
      if (foundItem) {
        setContextMenuItem(foundItem);
        setContextMenuPosition({ x: event.clientX, y: event.clientY });
        setContextMenuVisible(true);
      }
    }
  };

  // 处理右键菜单项点击
  const handleContextMenuClick = ({ key }: { key: string }) => {
    setContextMenuVisible(false);
    
    if (!contextMenuItem) return;
    
    if (key === 'edit') {
      setEditingItem(contextMenuItem);
      setEditModalVisible(true);
    } else if (key === 'delete') {
      // 显示确认对话框
      Modal.confirm({
        title: 'Delete Watchlist Item',
        content: `Are you sure you want to remove ${contextMenuItem.symbol} from your watchlist?`,
        okText: 'Delete',
        okType: 'danger',
        cancelText: 'Cancel',
        centered: true,
        onOk: async () => {
          await handleRemoveFromWatchlist(contextMenuItem.watchlist_id, contextMenuItem.symbol);
        },
      });
    }
  };

  // 右键菜单配置
  const contextMenuItems = [
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
  ];

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
    if (overviewLoading) {
      return (
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
      );
    }

    if (overviewError) {
      return (
        <Card>
          <Empty description={overviewError} image={Empty.PRESENTED_IMAGE_SIMPLE} />
        </Card>
      );
    }

    if (!overviewData || !overviewData.company) {
      return (
        <Empty
          description="Select a stock to view company overview"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      );
    }

    const { company, highlights, valuations, analyst_ratings, technicals } = overviewData;
    const currencyCode = company.currencycode || 'USD';

    return (
      <div>
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
                    <Tag color="green">{report.sector}</Tag>
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
      <div>
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

  const renderGroupStats = () => {
    if (!selectedGroup) return null;

    const group = watchlistGroups.find(g => g.group_name === selectedGroup);
    if (!group) return null;

    const sectorCount = group.items.reduce((acc, item) => {
      const sector = item.sector || 'Unknown';
      acc[sector] = (acc[sector] || 0) + 1;
      return acc;
    }, {} as Record<string, number>);

    const topSector = Object.entries(sectorCount)
      .sort(([,a], [,b]) => b - a)[0]?.[0] || 'N/A';

    return (
      <Card size="small" style={{ marginBottom: '16px' }}>
        <Row gutter={16}>
          <Col span={8}>
            <Statistic 
              title="Stock Count" 
              value={group.items.length}
              valueStyle={{ fontSize: '16px' }}
            />
          </Col>
          <Col span={8}>
            <Statistic 
              title="Top Sector" 
              value={topSector}
              valueStyle={{ fontSize: '16px' }}
            />
          </Col>
          <Col span={8}>
            <Statistic 
              title="Sectors" 
              value={Object.keys(sectorCount).length}
              valueStyle={{ fontSize: '16px' }}
            />
          </Col>
        </Row>
      </Card>
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
                    <HeartOutlined /> My Watchlist
                  </>
                )}
              </Title>
              {!watchlistCollapsed && (
                <Button 
                  type="primary" 
                  icon={<PlusOutlined />} 
                  size="small"
                  onClick={() => setAddModalVisible(true)}
                  title="Add stock to watchlist"
                >
                  Add
                </Button>
              )}
              {!watchlistCollapsed && (
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
              )}
            </div>
            
            {!watchlistCollapsed && (
              <>
                <Input
                  placeholder="Search groups, stocks, or sectors..."
                  prefix={<SearchOutlined />}
                  value={searchValue}
                  onChange={e => setSearchValue(e.target.value)}
                  style={{ marginBottom: '12px' }}
                  allowClear
                />
                
                <div style={{ display: 'flex', gap: '8px', marginBottom: '16px' }}>
                  <Select
                    placeholder="Priority"
                    value={priorityFilter}
                    onChange={setPriorityFilter}
                    size="small"
                    style={{ flex: 1, border: 'none' }}
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
                  
                  <Select
                    placeholder="Exchange"
                    value={exchangeFilter}
                    onChange={setExchangeFilter}
                    size="small"
                    style={{ flex: 1, border: 'none' }}
                    variant="borderless"
                  >
                    <Option key="all" value="all">
                      <span style={{ fontSize: '12px' }}>All</span>
                    </Option>
                    {getUniqueExchanges(watchlistGroups).map(exchange => (
                      <Option key={exchange} value={exchange}>
                        <span style={{ fontSize: '12px' }}>{exchange}</span>
                      </Option>
                    ))}
                  </Select>
                  
                  <Select
                    placeholder="Sector"
                    value={sectorFilter}
                    onChange={setSectorFilter}
                    size="small"
                    style={{ flex: 1, border: 'none' }}
                    variant="borderless"
                  >
                    <Option key="all" value="all">
                      <span style={{ fontSize: '12px' }}>Sectors</span>
                    </Option>
                    {getUniqueSectors(watchlistGroups).map(sector => (
                      <Option key={sector} value={sector}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '4px', fontSize: '12px' }}>
                          {getSectorIcon(sector)}
                          <span style={{ fontSize: '12px' }}>{sector}</span>
                        </div>
                      </Option>
                    ))}
                  </Select>
                </div>
              </>
            )}
            
            {!watchlistCollapsed && (
              <Spin spinning={loading}>
                {watchlistGroups.length === 0 && !loading ? (
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
                ) : (
                  <div>
                    <Tree
                      showIcon
                      selectedKeys={selectedKey ? [selectedKey] : []}
                      expandedKeys={expandedKeys}
                      onExpand={setExpandedKeys}
                      treeData={treeData}
                      onSelect={handleTreeSelect}
                      onRightClick={handleRightClick}
                      style={{ background: 'transparent' }}
                    />
                    {contextMenuVisible && (
                      <Dropdown
                        menu={{ items: contextMenuItems, onClick: handleContextMenuClick }}
                        trigger={['click']}
                        open={contextMenuVisible}
                        onOpenChange={(visible) => {
                          setContextMenuVisible(visible);
                        }}
                      >
                        <div
                          style={{
                            position: 'fixed',
                            left: contextMenuPosition.x,
                            top: contextMenuPosition.y,
                            width: 1,
                            height: 1,
                            pointerEvents: 'none',
                            zIndex: 9999
                          }}
                        />
                      </Dropdown>
                    )}
                  </div>
                )}
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
            ) : selectedKey ? (
              <>
                {selectedType === 'group' && (
                  <div style={{ marginBottom: '24px' }}>
                    <Title level={3}>
                      {selectedGroup} 分组
                    </Title>
                  </div>
                )}

                {/* 详细信息卡片 */}
                {selectedType === 'stock' && renderStockDetails()}
                {selectedType === 'group' && renderGroupStats()}

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
                  {selectedType === 'stock' && (
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
                  )}
                  <TabPane 
                    tab={
                      <span>
                        <FileTextOutlined />
                        Related News
                      </span>
                    } 
                    key="news"
                  >
                    {renderNewsTab()}
                  </TabPane>
                  {selectedType === 'stock' && (
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
                  )}
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
                      Please select a stock or group from the left<br />
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
                                  const relatedSectors = getRelatedSectors(classification);

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
