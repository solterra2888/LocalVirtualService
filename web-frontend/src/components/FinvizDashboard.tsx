import React, { useState, useEffect } from 'react';
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
  Select
} from 'antd';
import {
  SearchOutlined, 
  StockOutlined, 
  FileTextOutlined,
  CalendarOutlined,
  PlusOutlined,
  DeleteOutlined,
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
  FilePdfOutlined,
  FileExcelOutlined,
  FileWordOutlined,
  FileImageOutlined,
  StarFilled,
  StarOutlined,
  ReloadOutlined
} from '@ant-design/icons';
import { useGlobalLogger } from '../contexts/LoggerContext';
import { dashboardAPI, type DashboardNewsItem, type WatchlistGroup, type WatchlistItem, type ResearchReport, type StockPriceData } from '../services/dashboard';
import { filesAPI } from '../services/files';
import AddToWatchlistModal from './AddToWatchlistModal';
import FundamentalsOverviewModal from './FundamentalsOverviewModal';
import type { DataNode } from 'antd/es/tree';

const { Content, Sider } = Layout;
const { Title, Text, Paragraph } = Typography;
const { TabPane } = Tabs;
const { Option } = Select;

// 缓存配置
const CACHE_DURATION = 5 * 60 * 1000; // 5分钟缓存

// Priority filter options
const PRIORITY_FILTER_OPTIONS = [
  { label: 'All', value: null, stars: null },
  { label: 'High Priority', value: 3, stars: 3 },
  { label: 'Medium Priority', value: 2, stars: 2 },
  { label: 'Low Priority', value: 1, stars: 1 }
];

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

// Priority级别映射函数
const getPriorityLabel = (priority: number) => {
  switch (priority) {
    case 1:
      return 'Low';
    case 2:
      return 'Medium';
    case 3:
      return 'High';
    default:
      return `Level ${priority}`;
  }
};

// Priority级别颜色映射函数
const getPriorityColor = (priority: number) => {
  switch (priority) {
    case 1:
      return '#52c41a'; // Green for Low
    case 2:
      return '#faad14'; // Orange for Medium
    case 3:
      return '#f5222d'; // Red for High
    default:
      return '#8c8c8c'; // Gray for unknown
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
  const [priorityFilter, setPriorityFilter] = useState<number | null>(null);
  const [activeTab, setActiveTab] = useState<string>('news');
  const [expandedKeys, setExpandedKeys] = useState<React.Key[]>([]);
  
  // 数据状态
  const [watchlistGroups, setWatchlistGroups] = useState<WatchlistGroup[]>([]);
  const [treeData, setTreeData] = useState<DataNode[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  
  // 右侧内容数据
  const [newsData, setNewsData] = useState<DashboardNewsItem[]>([]);
  const [researchReports, setResearchReports] = useState<ResearchReport[]>([]);
  const [contentLoading, setContentLoading] = useState<boolean>(false);
  
  // 价格数据
  const [stockPrices, setStockPrices] = useState<Map<string, StockPriceData>>(new Map());
  const [pricesLoading, setPricesLoading] = useState<boolean>(false);
  
  
  // 缓存机制
  const [newsCache, setNewsCache] = useState<Map<string, DashboardNewsItem[]>>(new Map());
  const [cacheTimestamps, setCacheTimestamps] = useState<Map<string, number>>(new Map());
  
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
  
  const setCachedNews = (cacheKey: string, news: DashboardNewsItem[]) => {
    setNewsCache(prev => new Map(prev).set(cacheKey, news));
    setCacheTimestamps(prev => new Map(prev).set(cacheKey, Date.now()));
  };
  
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
  const [fundamentalsModalVisible, setFundamentalsModalVisible] = useState<boolean>(false);
  const [selectedTickerForFundamentals, setSelectedTickerForFundamentals] = useState<string>('');
  const [selectedCompanyNameForFundamentals, setSelectedCompanyNameForFundamentals] = useState<string>('');

  // 初始化数据
  useEffect(() => {
    loadUserWatchlist();
    logger.logInfo('FinvizDashboard页面已加载', 'finviz-dashboard');
    
    // 定期清理过期缓存
    const cleanupInterval = setInterval(cleanupExpiredCache, 60000); // 每分钟清理一次
    
    return () => clearInterval(cleanupInterval);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // 加载价格数据
  useEffect(() => {
    if (watchlistGroups.length > 0) {
      loadWatchlistPrices();
    }
  }, [watchlistGroups]);

  // 搜索过滤
  useEffect(() => {
    filterTreeData();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchValue, priorityFilter, watchlistGroups, stockPrices]);

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
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedKey, selectedType, activeTab, selectedStock]);

  const loadUserWatchlist = async () => {
    setLoading(true);
    try {
      const watchlistData = await dashboardAPI.getUserWatchlist();
      setWatchlistGroups(watchlistData);
      
      const totalStocks = watchlistData.reduce((sum, group) => sum + group.items.length, 0);
      logger.logInfo(`加载了 ${watchlistData.length} 个分组，${totalStocks} 支股票`, 'finviz-dashboard');
      
      // 如果有股票且当前没有选中任何股票，自动选择第一个股票
      if (totalStocks > 0 && !selectedKey) {
        const firstGroup = watchlistData.find(group => group.items.length > 0);
        if (firstGroup && firstGroup.items.length > 0) {
          const firstStock = firstGroup.items[0];
          const stockKey = `stock-${firstStock.watchlist_id}`;
          
          // 设置选中状态
          setSelectedKey(stockKey);
          setSelectedType('stock');
          setSelectedStock(firstStock);
          setSelectedGroup('');
          
          logger.logInfo(`自动选择第一个股票: ${firstStock.custom_name || firstStock.company} (${firstStock.symbol})`, 'finviz-dashboard');
        }
      }
      
    } catch (error) {
      message.error('Failed to load user watchlist');
      logger.logError('Failed to load user watchlist', 'finviz-dashboard', error);
    } finally {
      setLoading(false);
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

  const filterTreeData = () => {
    const filteredGroups = watchlistGroups.filter(group => {
      // First filter by priority if set
      let filteredItems = group.items;
      if (priorityFilter !== null) {
        filteredItems = group.items.filter(item => item.priority === priorityFilter);
      }
      
      // If no items match the priority filter, hide the entire group
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
      // Get filtered items based on priority and search
      let filteredItems = group.items;
      if (priorityFilter !== null) {
        filteredItems = group.items.filter(item => item.priority === priorityFilter);
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
      return;
    }

    const key = selectedKeys[0] as string;
    setSelectedKey(key);

    if (key.startsWith('group-')) {
      setSelectedType('group');
      const groupName = key.replace('group-', '');
      setSelectedGroup(groupName);
      setSelectedStock(null);
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
      logger.logInfo(`选择了股票: ${foundStock?.custom_name || foundStock?.company} (${foundStock?.symbol})`, 'finviz-dashboard');
    }
  };

  const loadRelatedNews = async () => {
    if (!selectedKey) return;
    
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
        logger.logInfo(`使用缓存数据: ${cacheKey}`, 'finviz-dashboard');
        return;
      }
    }
    
    setContentLoading(true);
    try {
      if (selectedType === 'stock' && selectedStock) {
        await loadStockNews(selectedStock.symbol, cacheKey);
      } else if (selectedType === 'group' && selectedGroup) {
        await loadGroupNews(selectedGroup, cacheKey);
      }
    } catch (error) {
      message.error('Failed to load related news');
      logger.logError('Failed to load related news', 'finviz-dashboard', error);
    } finally {
      setContentLoading(false);
    }
  };

  const loadStockNews = async (ticker: string, cacheKey?: string | null) => {
    try {
      const news = await dashboardAPI.getCompanyNews(ticker, { limit: 20 });
      setNewsData(news);
      
      // 缓存数据
      if (cacheKey) {
        setCachedNews(cacheKey, news);
      }
      
      logger.logInfo(`加载股票新闻: ${ticker}, ${news.length} 条`, 'finviz-dashboard');
    } catch (error) {
      logger.logError(`加载股票新闻失败: ${ticker}`, 'finviz-dashboard', error);
      setNewsData([]);
    }
  };

  const loadGroupNews = async (groupName: string, cacheKey?: string | null) => {
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
        const news = await dashboardAPI.getSectorNews(firstStock.sector, { limit: 20 });
        setNewsData(news);
        
        // 缓存数据
        if (cacheKey) {
          setCachedNews(cacheKey, news);
        }
        
        logger.logInfo(`加载分组新闻: ${groupName}, ${news.length} 条`, 'finviz-dashboard');
      } else {
        setNewsData([]);
      }
    } catch (error) {
      logger.logError(`加载分组新闻失败: ${groupName}`, 'finviz-dashboard', error);
      setNewsData([]);
    }
  };

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

  // 处理公司名称点击，显示基本面信息
  const handleCompanyNameClick = (ticker: string, companyName: string) => {
    setSelectedTickerForFundamentals(ticker);
    setSelectedCompanyNameForFundamentals(companyName);
    setFundamentalsModalVisible(true);
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
        <List
          itemLayout="vertical"
          dataSource={newsData}
          renderItem={item => (
            <List.Item
              key={item.id}
              extra={
                <Space direction="vertical" align="end">
                  <Tag color={getSentimentColor(item.sentiment_polarity)}>
                    {getSentimentText(item.sentiment_polarity)}
                  </Tag>
                  <Text type="secondary" style={{ fontSize: '12px' }}>
                    <CalendarOutlined /> {item.date ? new Date(item.date).toLocaleDateString() : 'N/A'}
                  </Text>
                </Space>
              }
            >
              <List.Item.Meta
                title={
                  <a 
                    href={item.link} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    style={{ fontSize: '14px' }}
                  >
                    {item.title}
                  </a>
                }
                description={
                  <Space>
                    <Text type="secondary" style={{ fontSize: '12px' }}>{item.source}</Text>
                    {item.symbols?.slice(0, 3).map(symbol => (
                      <Tag key={symbol} style={{ fontSize: '10px' }}>{symbol}</Tag>
                    ))}
                    {item.symbols && item.symbols.length > 3 && (
                      <Tag style={{ fontSize: '10px' }}>+{item.symbols.length - 3}</Tag>
                    )}
                  </Space>
                }
              />
              <Paragraph 
                ellipsis={{ rows: 2, expandable: true }}
                style={{ fontSize: '13px', marginBottom: '8px' }}
              >
                {item.content}
              </Paragraph>
            </List.Item>
          )}
        />
      )}
    </div>
  );

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
    
    // 格式化价格变化颜色
    const getPriceChangeColor = (changePercent?: number) => {
      if (!changePercent) return '#8c8c8c';
      return changePercent >= 0 ? '#52c41a' : '#f5222d';
    };

    // 格式化价格变化显示
    const formatPriceChange = (changeAmount?: number, changePercent?: number) => {
      if (!changeAmount || !changePercent) return 'N/A';
      const sign = changeAmount >= 0 ? '+' : '';
      return `${sign}$${changeAmount.toFixed(2)} (${sign}${changePercent.toFixed(2)}%)`;
    };

    return (
      <Card size="small" style={{ marginBottom: '16px', position: 'relative' }}>
        <Row gutter={16}>
          <Col span={8}>
            <div>
              <Text strong style={{ fontSize: '12px' }}>Price</Text>
              <br />
              {pricesLoading ? (
                <Skeleton.Input style={{ width: 80, height: 20 }} size="small" />
              ) : (
                <Text style={{ fontSize: '14px', fontWeight: 'bold' }}>
                  {priceData?.close ? `$${priceData.close.toFixed(2)}` : 'N/A'}
                </Text>
              )}
            </div>
          </Col>
          <Col span={8}>
            <div>
              <Text strong style={{ fontSize: '12px' }}>Price Change %</Text>
              <br />
              {pricesLoading ? (
                <Skeleton.Input style={{ width: 60, height: 20 }} size="small" />
              ) : (
                <Text 
                  style={{ 
                    fontSize: '14px', 
                    fontWeight: 'bold',
                    color: priceData?.change_percent !== undefined ? getPriceChangeColor(priceData.change_percent) : '#8c8c8c'
                  }}
                >
                  {priceData?.change_percent !== undefined 
                    ? `${priceData.change_percent >= 0 ? '+' : ''}${priceData.change_percent.toFixed(2)}%` 
                    : 'N/A'
                  }
                </Text>
              )}
            </div>
          </Col>
          <Col span={8}>
            <div>
              <Text strong style={{ fontSize: '12px' }}>Volume</Text>
              <br />
              {pricesLoading ? (
                <Skeleton.Input style={{ width: 100, height: 20 }} size="small" />
              ) : (
                <Text style={{ fontSize: '12px' }}>
                  {priceData?.volume ? priceData.volume.toLocaleString() : 'N/A'}
                </Text>
              )}
            </div>
          </Col>
        </Row>
        
        {/* 操作按钮区域 */}
        <Row style={{ marginTop: '12px', marginBottom: '20px' }}>
          <Col span={24} style={{ textAlign: 'right' }}>
            <Button 
              type="text" 
              danger 
              size="small"
              icon={<DeleteOutlined />}
              onClick={() => handleRemoveFromWatchlist(selectedStock.watchlist_id, selectedStock.symbol)}
              title="Remove from watchlist"
            />
          </Col>
        </Row>
        
        {/* 更新时间戳 - 右下角 */}
        {priceData?.updated_at && (
          <div style={{ 
            position: 'absolute', 
            bottom: '8px', 
            right: '12px',
            fontSize: '9px',
            color: '#999',
            fontStyle: 'italic'
          }}>
            Updated at {new Date(priceData.updated_at).toLocaleString()}
          </div>
        )}
        
        {selectedStock.notes && (
          <div style={{ marginTop: '12px', padding: '8px', backgroundColor: '#f9f9f9', borderRadius: '4px' }}>
            <Text type="secondary" style={{ fontSize: '12px' }}>Notes: </Text>
            <Text style={{ fontSize: '12px' }}>{selectedStock.notes}</Text>
          </div>
        )}
      </Card>
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
          width={350} 
          style={{ 
            background: '#fff', 
            borderRight: '1px solid #f0f0f0',
            height: '100%',
            overflowY: 'auto'
          }}
        >
          <div style={{ padding: '16px' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
              <Title level={4} style={{ margin: 0 }}>
                <HeartOutlined /> My Watchlist
              </Title>
              <Space>
                <Button 
                  icon={<ReloadOutlined />} 
                  size="small"
                  loading={pricesLoading}
                  onClick={loadWatchlistPrices}
                  title="Refresh stock prices"
                >
                  Prices
                </Button>
                <Button 
                  type="primary" 
                  icon={<PlusOutlined />} 
                  size="small"
                  onClick={() => setAddModalVisible(true)}
                  title="Add stock to watchlist"
                >
                  Add
                </Button>
              </Space>
            </div>
            
            <Input
              placeholder="Search groups, stocks, or sectors..."
              prefix={<SearchOutlined />}
              value={searchValue}
              onChange={e => setSearchValue(e.target.value)}
              style={{ marginBottom: '12px' }}
              allowClear
            />
            
            <Select
              placeholder="Filter by Priority"
              value={priorityFilter}
              onChange={setPriorityFilter}
              size="small"
              style={{ width: '100%', marginBottom: '16px' }}
              allowClear
            >
              {PRIORITY_FILTER_OPTIONS.map(option => (
                <Option key={option.value || 'all'} value={option.value}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: '4px', fontSize: '12px' }}>
                    <span style={{ fontSize: '12px' }}>{option.label}</span>
                    {option.stars && renderStars(option.stars)}
                  </div>
                </Option>
              ))}
            </Select>
            
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
                <Tree
                  showIcon
                  selectedKeys={selectedKey ? [selectedKey] : []}
                  expandedKeys={expandedKeys}
                  onExpand={setExpandedKeys}
                  treeData={treeData}
                  onSelect={handleTreeSelect}
                  style={{ background: 'transparent' }}
                />
              )}
            </Spin>
          </div>
        </Sider>

        {/* 右侧内容区域 */}
        <Content style={{ background: '#fff', margin: '0', height: '100%' }}>
          <div style={{ padding: '24px', height: '100%' }}>
            {selectedKey ? (
              <>
                <div style={{ marginBottom: '24px' }}>
                  <Title level={3}>
                    {selectedType === 'stock' 
                      ? (
                        <span>
                          <span 
                            style={{ 
                              cursor: 'pointer', 
                              color: '#1890ff',
                              textDecoration: 'underline'
                            }}
                            onClick={() => selectedStock && handleCompanyNameClick(selectedStock.symbol, selectedStock.company || selectedStock.symbol)}
                            title="Click to view fundamentals overview"
                          >
                            {selectedStock?.custom_name || selectedStock?.company}
                          </span>
                          {' '}({selectedStock?.symbol})
                        </span>
                      )
                      : `${selectedGroup} 分组`
                    }
                  </Title>
                  
                  {selectedType === 'stock' && selectedStock && (
                    <Space size="large" style={{ marginBottom: '16px' }}>
                      <Tag color="blue">{selectedStock.sector}</Tag>
                      <Tag color="green">{selectedStock.industry}</Tag>
                    </Space>
                  )}
                </div>

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
                    }
                  }}
                  size="large"
                >
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

      {/* 基本面信息概览模态框 */}
      <FundamentalsOverviewModal
        visible={fundamentalsModalVisible}
        onCancel={() => setFundamentalsModalVisible(false)}
        ticker={selectedTickerForFundamentals}
        companyName={selectedCompanyNameForFundamentals}
      />

    </div>
  );
};

export default FinvizDashboard;
