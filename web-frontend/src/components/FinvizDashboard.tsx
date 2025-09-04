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
  Skeleton
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
  SettingOutlined
} from '@ant-design/icons';
import { useGlobalLogger } from '../contexts/LoggerContext';
import { dashboardAPI, type DashboardNewsItem, type WatchlistGroup, type WatchlistItem } from '../services/dashboard';
import AddToWatchlistModal from './AddToWatchlistModal';
import type { DataNode } from 'antd/es/tree';

const { Content, Sider } = Layout;
const { Title, Text, Paragraph } = Typography;
const { TabPane } = Tabs;

// 缓存配置
const CACHE_DURATION = 5 * 60 * 1000; // 5分钟缓存

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

const FinvizDashboard: React.FC = () => {
  const logger = useGlobalLogger();
  
  // 状态管理
  const [selectedKey, setSelectedKey] = useState<string>('');
  const [selectedType, setSelectedType] = useState<'group' | 'stock' | ''>('');
  const [selectedStock, setSelectedStock] = useState<WatchlistStockInfo | null>(null);
  const [selectedGroup, setSelectedGroup] = useState<string>('');
  const [searchValue, setSearchValue] = useState<string>('');
  const [activeTab, setActiveTab] = useState<string>('news');
  const [expandedKeys, setExpandedKeys] = useState<React.Key[]>([]);
  
  // 数据状态
  const [watchlistGroups, setWatchlistGroups] = useState<WatchlistGroup[]>([]);
  const [treeData, setTreeData] = useState<DataNode[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  
  // 右侧内容数据
  const [newsData, setNewsData] = useState<DashboardNewsItem[]>([]);
  const [contentLoading, setContentLoading] = useState<boolean>(false);
  
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

  // 初始化数据
  useEffect(() => {
    loadUserWatchlist();
    logger.logInfo('FinvizDashboard页面已加载', 'finviz-dashboard');
    
    // 定期清理过期缓存
    const cleanupInterval = setInterval(cleanupExpiredCache, 60000); // 每分钟清理一次
    
    return () => clearInterval(cleanupInterval);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // 搜索过滤
  useEffect(() => {
    filterTreeData();
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [searchValue, watchlistGroups]);

  // 当选择改变时加载相关内容（添加防抖）
  useEffect(() => {
    if (selectedKey && activeTab === 'news') {
      const timer = setTimeout(() => {
        loadRelatedNews();
      }, 300); // 300ms 防抖
      
      return () => clearTimeout(timer);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedKey, selectedType, activeTab]);

  const loadUserWatchlist = async () => {
    setLoading(true);
    try {
      const watchlistData = await dashboardAPI.getUserWatchlist();
      setWatchlistGroups(watchlistData);
      
      const totalStocks = watchlistData.reduce((sum, group) => sum + group.items.length, 0);
      logger.logInfo(`加载了 ${watchlistData.length} 个分组，${totalStocks} 支股票`, 'finviz-dashboard');
      
    } catch (error) {
      message.error('Failed to load user watchlist');
      logger.logError('Failed to load user watchlist', 'finviz-dashboard', error);
    } finally {
      setLoading(false);
    }
  };

  const filterTreeData = () => {
    const filteredGroups = watchlistGroups.filter(group => {
      if (!searchValue) return true;
      
      const groupMatch = group.group_name.toLowerCase().includes(searchValue.toLowerCase());
      const stockMatch = group.items.some(item => 
        item.symbol.toLowerCase().includes(searchValue.toLowerCase()) ||
        (item.company && item.company.toLowerCase().includes(searchValue.toLowerCase())) ||
        (item.custom_name && item.custom_name.toLowerCase().includes(searchValue.toLowerCase())) ||
        (item.sector && item.sector.toLowerCase().includes(searchValue.toLowerCase()))
      );
      
      return groupMatch || stockMatch;
    });

    const treeNodes: DataNode[] = filteredGroups.map(group => {
      // 获取分组中股票的主要sector
      const sectors = group.items
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
        children: group.items
        .filter(item => {
          if (!searchValue) return true;
          return item.symbol.toLowerCase().includes(searchValue.toLowerCase()) ||
                 (item.company && item.company.toLowerCase().includes(searchValue.toLowerCase())) ||
                 (item.custom_name && item.custom_name.toLowerCase().includes(searchValue.toLowerCase())) ||
                 (item.sector && item.sector.toLowerCase().includes(searchValue.toLowerCase()));
        })
        .map(item => ({
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
                  lineHeight: '1.2'
                }}>
                  {item.symbol}
                </div>
              </div>
            </div>
          ),
          key: `stock-${item.watchlist_id}`,
          isLeaf: true,
        }))
      };
    });

    setTreeData(treeNodes);
    
    // 设置所有分组节点为展开状态
    const groupKeys = filteredGroups.map(group => `group-${group.group_name}`);
    setExpandedKeys(groupKeys);
  };

  const handleTreeSelect = (selectedKeys: React.Key[], info: any) => {
    if (selectedKeys.length === 0) {
      setSelectedKey('');
      setSelectedType('');
      setSelectedStock(null);
      setSelectedGroup('');
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

  const formatMarketCap = (marketCap?: number) => {
    if (!marketCap) return 'N/A';
    if (marketCap >= 1e9) {
      return `$${(marketCap / 1e9).toFixed(1)}B`;
    } else if (marketCap >= 1e6) {
      return `$${(marketCap / 1e6).toFixed(1)}M`;
    }
    return `$${marketCap.toFixed(0)}`;
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

  const renderStockDetails = () => {
    if (!selectedStock) return null;

    return (
      <Card size="small" style={{ marginBottom: '16px' }}>
        <Row gutter={16}>
          <Col span={6}>
            <Statistic 
              title="股价" 
              value={selectedStock.price || 0} 
              prefix="$"
              precision={2}
              valueStyle={{ 
                fontSize: '16px',
                color: selectedStock.change_percent?.startsWith('-') ? '#ff4d4f' : '#52c41a'
              }}
            />
          </Col>
          <Col span={6}>
            <Statistic 
              title="市值" 
              value={formatMarketCap(selectedStock.market_cap)} 
              valueStyle={{ fontSize: '16px' }}
            />
          </Col>
          <Col span={6}>
                          <div>
                <Text strong style={{ fontSize: '12px' }}>Sector</Text>
                <br />
                <Text style={{ fontSize: '14px' }}>{selectedStock.sector || 'N/A'}</Text>
              </div>
          </Col>
          <Col span={6}>
            <div style={{ textAlign: 'right' }}>
              <Space>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  Priority: {selectedStock.priority}
                </Text>
                <Button 
                  type="text" 
                  danger 
                  size="small"
                  icon={<DeleteOutlined />}
                  onClick={() => handleRemoveFromWatchlist(selectedStock.watchlist_id, selectedStock.symbol)}
                  title="Remove from watchlist"
                />
              </Space>
            </div>
          </Col>
        </Row>
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

    const totalValue = group.items
      .filter(item => item.price && item.market_cap)
      .reduce((sum, item) => sum + (item.market_cap || 0), 0);

    const avgPrice = group.items
      .filter(item => item.price)
      .reduce((sum, item) => sum + (item.price || 0), 0) / 
      group.items.filter(item => item.price).length;

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
              title="Total Market Cap" 
              value={formatMarketCap(totalValue)}
              valueStyle={{ fontSize: '16px' }}
            />
          </Col>
          <Col span={8}>
            <Statistic 
              title="Average Price" 
              value={avgPrice || 0}
              prefix="$"
              precision={2}
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
              <Button 
                type="primary" 
                icon={<PlusOutlined />} 
                size="small"
                onClick={() => setAddModalVisible(true)}
                title="Add stock to watchlist"
              >
                Add
              </Button>
            </div>
            
            <Input
              placeholder="Search groups, stocks, or sectors..."
              prefix={<SearchOutlined />}
              value={searchValue}
              onChange={e => setSearchValue(e.target.value)}
              style={{ marginBottom: '16px' }}
              allowClear
            />
            
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
                      ? `${selectedStock?.custom_name || selectedStock?.company} (${selectedStock?.symbol})`
                      : `${selectedGroup} 分组`
                    }
                  </Title>
                  
                  {selectedType === 'stock' && selectedStock && (
                    <Space size="large" style={{ marginBottom: '16px' }}>
                      <Tag color="blue">{selectedStock.sector}</Tag>
                      <Tag color="green">{selectedStock.industry}</Tag>
                      {selectedStock.change_percent && (
                        <Tag color={selectedStock.change_percent.startsWith('-') ? 'red' : 'green'}>
                          {selectedStock.change_percent}
                        </Tag>
                      )}
                    </Space>
                  )}
                </div>

                {/* 详细信息卡片 */}
                {selectedType === 'stock' && renderStockDetails()}
                {selectedType === 'group' && renderGroupStats()}

                <Tabs 
                  activeKey={activeTab}
                  onChange={setActiveTab}
                  size="large"
                >
                  <TabPane 
                    tab={
                      <span>
                        <FileTextOutlined />
                        Related News ({contentLoading ? '...' : newsData.length})
                      </span>
                    } 
                    key="news"
                  >
                    {renderNewsTab()}
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
    </div>
  );
};

export default FinvizDashboard;
