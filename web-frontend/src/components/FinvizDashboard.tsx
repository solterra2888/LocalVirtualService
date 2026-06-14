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
import {
  SearchOutlined, 
  QuestionCircleOutlined,
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
import DailyCard from './daily/DailyCard';
import DailyTimelineItem from './daily/DailyTimelineItem';
import DailyPreviewDrawer from './daily/DailyPreviewDrawer';
import { listDailyFeed, type DailyFeedItem } from '../services/daily';
import { dateKeyInTz, formatDateLabel } from '../utils/datetime';
import { expandTickerForDaily } from '../utils/tickerFormat';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useScrollingClass, useScrollingClassOnElement } from '../hooks/useScrollingClass';
import '../styles/subtleScrollbar.css';
import { dashboardAPI, type WatchlistGroup, type WatchlistItem, type StockPriceData, type CustomGroup } from '../services/dashboard';
import { fundamentalsAPI, fundamentalsUtils, type CompanyFundamentalsResponse } from '../services/fundamentals';
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

// ─── news tab 日期分组 (复刻 DailyPage.groupByDate) ───────────────
interface NewsDateGroup {
  dateKey: string;   // YYYY-MM-DD (用户时区)
  dateLabel: string; // April 25, 2026
  items: DailyFeedItem[];
}

const groupNewsByDate = (items: DailyFeedItem[], tz: string): NewsDateGroup[] => {
  const groups: NewsDateGroup[] = [];
  const map = new Map<string, NewsDateGroup>();
  for (const it of items) {
    const key = dateKeyInTz(it.published_at, tz);
    let g = map.get(key);
    if (!g) {
      g = { dateKey: key, dateLabel: formatDateLabel(it.published_at, tz), items: [] };
      map.set(key, g);
      groups.push(g);
    }
    g.items.push(it);
  }
  return groups;
};

const FinvizDashboard: React.FC = () => {
  const { themeMode, userTimezone } = useDashboardSettings();
  const liveHeadlinesScrollRef = useScrollingClass<HTMLDivElement>();
  const panelScrollbarClass =
    themeMode === 'dark'
      ? 'subtle-scrollbar subtle-scrollbar--panel-dark'
      : 'subtle-scrollbar subtle-scrollbar--panel';

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
  const [newsData, setNewsData] = useState<DailyFeedItem[]>([]);
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
  const NEWS_SEARCH_DEBOUNCE_MS = 400; // news 搜索输入防抖
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
  
  // news 请求竞态保护 + 当前 ticker 缓存 (避免 loadMore 重算)
  const newsRequestSeqRef = useRef(0);
  const currentTickersRef = useRef<string[]>([]);
  // news 搜索: 输入框值(实时) / 防抖后查询值 / ref(供 loadStockNews 读取避免改签名)
  const [newsSearchInput, setNewsSearchInput] = useState('');
  const [newsSearchQuery, setNewsSearchQuery] = useState('');
  const newsSearchQueryRef = useRef('');
  
  // 新闻列表最后更新时间（用于 "Updated HH:MM:SS PM"）
  const [lastNewsUpdate, setLastNewsUpdate] = useState<Date | null>(null);
  
  
  

  

  
  

  
  // 模态框状态
  const [addModalVisible, setAddModalVisible] = useState<boolean>(false);
  const [editModalVisible, setEditModalVisible] = useState<boolean>(false);
  const [editingItem, setEditingItem] = useState<WatchlistItem | null>(null);
  
  // Daily 卡片预览抽屉 (news tab 点击卡片打开; null = 关闭)
  const [previewDailyItem, setPreviewDailyItem] = useState<DailyFeedItem | null>(null);
  
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
    loadCustomGroups();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  /** Overview Tab 面板滚动时显示弱化滚动条 */
  useEffect(() => {
    const root = document.querySelector(
      '.watchlist-page-layout .watchlist-detail-panel'
    );
    if (!root) return;

    const panes = root.querySelectorAll<HTMLElement>(
      '.ant-tabs-tabpane.watchlist-tab-pane-scroll'
    );
    const SCROLL_IDLE_MS = 700;
    const cleanups: Array<() => void> = [];

    panes.forEach((el) => {
      let idleTimer: number | undefined;
      const onScroll = () => {
        el.classList.add('is-scrolling');
        if (idleTimer !== undefined) {
          window.clearTimeout(idleTimer);
        }
        idleTimer = window.setTimeout(() => {
          el.classList.remove('is-scrolling');
        }, SCROLL_IDLE_MS);
      };
      el.addEventListener('scroll', onScroll, { passive: true });
      cleanups.push(() => {
        el.removeEventListener('scroll', onScroll);
        if (idleTimer !== undefined) {
          window.clearTimeout(idleTimer);
        }
        el.classList.remove('is-scrolling');
      });
    });

    return () => cleanups.forEach((fn) => fn());
  }, [selectedStock]);

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

      }
    }
  }, [watchlistGroups, loading, selectedStock, overviewCache]);

  // news 搜索防抖: 输入停止后写入 searchQuery + ref, 触发下方 reload effect
  useEffect(() => {
    const t = window.setTimeout(() => {
      const q = newsSearchInput.trim();
      newsSearchQueryRef.current = q;
      setNewsSearchQuery(q);
    }, NEWS_SEARCH_DEBOUNCE_MS);
    return () => window.clearTimeout(t);
  }, [newsSearchInput]);

  // 当选择改变时加载相关内容（添加防抖）
  useEffect(() => {
    if (selectedStock && activeTab === 'news') {
      const timer = setTimeout(() => {
        loadRelatedNews();
      }, 300); // 300ms 防抖
      
      return () => clearTimeout(timer);
    } else if (selectedStock && activeTab === 'overview') {
      const timer = setTimeout(() => {
        loadStockOverview(selectedStock);
      }, 300); // 300ms 防抖
      
      return () => clearTimeout(timer);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedStock, activeTab, newsSearchQuery]);

  const loadUserWatchlist = async () => {
    setLoading(true);
    try {
      const watchlistData = await dashboardAPI.getUserWatchlist();
      setWatchlistGroups(watchlistData);
      
      // 成功加载后清除错误状态
      setLastError(null);
      
    } catch (error) {
      const errorMessage = 'Failed to load user watchlist';
      
      // 只有在错误消息与上次不同时才显示错误消息
      if (lastError !== errorMessage) {
        message.error(errorMessage);
        setLastError(errorMessage);
      }
      
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
      }
    } catch (error) {
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
    [overviewCache, overviewLastError, getOverviewQueryForStock]
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
      setOverviewData(null);
      setOverviewError(null);
      setOverviewLoading(false);
      setActiveTab('news');
      return;
    }

    setSelectedStock(stock);

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
  }, [overviewCache, getOverviewQueryForStock]);

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
    setContentLoading(true);
    try {
      const tickers = expandTickerForDaily(selectedStock.symbol, selectedStock.exchange);
      currentTickersRef.current = tickers;
      await loadStockNews(tickers, true);
    } catch (error) {
      message.error('Failed to load related news');
    } finally {
      setContentLoading(false);
    }
  };

  const loadStockNews = useCallback(async (tickers: string[], reset: boolean = true) => {
    const seq = ++newsRequestSeqRef.current;
    try {
      const offset = reset ? 0 : newsOffset;
      const resp = await listDailyFeed({
        tickers,
        q: newsSearchQueryRef.current || undefined,
        limit: NEWS_PAGE_SIZE,
        offset,
      });
      // 丢弃过期响应 (切股竞态保护)
      if (seq !== newsRequestSeqRef.current) return;

      if (reset) {
        setNewsData(resp.items);
        setNewsOffset(resp.items.length);
        setLastNewsUpdate(new Date());
      } else {
        setNewsData(prev => [...prev, ...resp.items]);
        setNewsOffset(prev => prev + resp.items.length);
      }
      setHasMoreNews(resp.has_more);
    } catch (error) {
      if (reset && seq === newsRequestSeqRef.current) {
        setNewsData([]);
      }
    }
  }, [newsOffset, NEWS_PAGE_SIZE]);

  // 加载更多新闻
  const loadMoreNews = useCallback(async () => {
    if (!hasMoreNews || loadingMoreNews || contentLoading || !selectedStock) return;

    setLoadingMoreNews(true);
    try {
      await loadStockNews(currentTickersRef.current, false);
    } catch (error) {
      message.error('Failed to load more news');
    } finally {
      setLoadingMoreNews(false);
    }
  }, [hasMoreNews, loadingMoreNews, contentLoading, selectedStock, loadStockNews]);

  // 滚动检测和自动加载更多新闻（使用右侧 Tab 内容区作为滚动容器）
  useEffect(() => {
    const handleScroll = () => {
      if (!hasMoreNews || loadingMoreNews || activeTab !== 'news') return;
      const scrollContainer = document.querySelector('.watchlist-detail-panel .watchlist-tab-scroll');
      if (!scrollContainer) return;
      const { scrollTop, scrollHeight, clientHeight } = scrollContainer;
      const threshold = 200;
      if (scrollTop + clientHeight >= scrollHeight - threshold) {
        loadMoreNews();
      }
    };
    const scrollContainer = document.querySelector('.watchlist-detail-panel .watchlist-tab-scroll');
    if (scrollContainer) {
      scrollContainer.addEventListener('scroll', handleScroll);
      return () => scrollContainer.removeEventListener('scroll', handleScroll);
    }
  }, [hasMoreNews, loadingMoreNews, activeTab, loadMoreNews]);

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
        setNewsData([]);
        setOverviewData(null);
        setOverviewError(null);
        setOverviewLoading(false);
        setActiveTab('news');
      }
      
    } catch (error) {
      message.error('Failed to remove');
    }
  }, [selectedStock]);

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
      message.success('Watchlist order updated');
    } catch (error) {
      setWatchlistGroups(previousGroups);
      if (previousSelected) {
        setSelectedStock(previousSelected);
      }
      message.error('Failed to update watchlist order');
    } finally {
      setIsReordering(false);
    }
  }, [normalizeGroupName, selectedStock, watchlistGroups]);

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

  const renderNewsTab = () => {
    const searchBar = (
      <div style={{ flexShrink: 0, padding: '12px 12px 8px' }}>
        <Input
          allowClear
          prefix={<SearchOutlined style={{ color: colors.textSecondary, fontSize: 14 }} />}
          suffix={
            <Tooltip
              title={
                <div style={{ fontSize: 12, lineHeight: 1.7 }}>
                  <div style={{ fontWeight: 600, marginBottom: 4 }}>搜索语法</div>
                  <div><span style={{ opacity: 0.6 }}>空格</span> → AND，每个词都要出现</div>
                  <div><span style={{ opacity: 0.6 }}>OR（大写）</span> → 任一词命中即可</div>
                  <div><span style={{ opacity: 0.6 }}>「短语」</span> → 精确子串匹配</div>
                </div>
              }
              placement="bottomRight"
            >
              <QuestionCircleOutlined style={{ color: colors.textSecondary, cursor: 'pointer', fontSize: 14 }} />
            </Tooltip>
          }
          placeholder={`Search within ${selectedStock?.symbol ?? ''} (space=AND, OR=any, "phrase")`}
          value={newsSearchInput}
          onChange={(e) => setNewsSearchInput(e.target.value)}
          style={{
            background: colors.cardBg,
            borderColor: colors.cardBorder,
            color: colors.text,
            fontSize: 13,
          }}
        />
      </div>
    );

    if (contentLoading) {
      return (
        <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0, overflow: 'hidden' }}>
          {searchBar}
          <div style={{ flex: 1, minHeight: 0, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
            <Spin size="large" />
          </div>
        </div>
      );
    }

    const grouped = groupNewsByDate(newsData, userTimezone);
    const dateAnchorStyle: React.CSSProperties = {
      position: 'sticky',
      top: 0,
      zIndex: 2,
      display: 'flex',
      alignItems: 'baseline',
      gap: 8,
      padding: '8px 4px',
      marginBottom: 8,
      background: colors.cardBg,
      borderBottom: `1px solid ${colors.cardBorder}`,
    };

    return (
      <div style={{ display: 'flex', flexDirection: 'column', height: '100%', minHeight: 0, overflow: 'hidden' }}>
        {searchBar}
        <div
          ref={liveHeadlinesScrollRef}
          className={`watchlist-tab-scroll ${panelScrollbarClass}`}
          style={{ flex: 1, minHeight: 0, overflowY: 'auto', padding: '0 12px 12px' }}
        >
          {newsData.length === 0 ? (
            <Empty
              description={newsSearchQuery
                ? `No results for "${newsSearchQuery}" within ${selectedStock?.symbol ?? ''}`
                : selectedStock
                ? `No items for ${selectedStock.symbol}`
                : 'No items'}
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              style={{ marginTop: 40 }}
            />
          ) : (
            <>
              {grouped.map((g) => (
                <div key={g.dateKey} style={{ marginBottom: 20 }}>
                  <div style={dateAnchorStyle}>
                    <Text strong>{g.dateLabel}</Text>
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      {g.items.length} {g.items.length === 1 ? 'article' : 'articles'}
                    </Text>
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column' }}>
                    {g.items.map((it, idx) => (
                      <DailyTimelineItem
                        key={it.id}
                        themeMode={themeMode}
                        publishedAt={it.published_at}
                        isFirst={idx === 0}
                        isLast={idx === g.items.length - 1}
                      >
                        <DailyCard
                          item={it}
                          themeMode={themeMode}
                          onClick={(item) => setPreviewDailyItem(item)}
                        />
                      </DailyTimelineItem>
                    ))}
                  </div>
                </div>
              ))}
              {loadingMoreNews && (
                <div style={{ textAlign: 'center', padding: '16px 0' }}>
                  <Spin />
                </div>
              )}
              {!hasMoreNews && newsData.length > 0 && (
                <div style={{ textAlign: 'center', color: colors.textSecondary, fontSize: 12, padding: '16px 0 32px' }}>
                  — End of feed —
                </div>
              )}
            </>
          )}
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
                  if (key === 'news') {
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
                  className={panelScrollbarClass}
                  style={{ height: '100%', overflow: 'hidden' }}
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
                  className={`watchlist-tab-pane-scroll ${panelScrollbarClass}`}
                >
                  {renderOverviewTab()}
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

      {/* Daily 卡片预览抽屉 (news tab 点击卡片打开; item=null 时关闭) */}
      <DailyPreviewDrawer
        item={previewDailyItem}
        items={newsData}
        themeMode={themeMode}
        placement="right"
        onClose={() => setPreviewDailyItem(null)}
        onNavigate={(next) => setPreviewDailyItem(next)}
      />
    </ConfigProvider>
  );
};

export default FinvizDashboard;
