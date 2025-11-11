import React, { useState, useEffect } from 'react';
import { Spin, message, Select, Empty, Button, Modal, Tabs } from 'antd';
import { dashboardAPI, type WatchlistGroup, type WatchlistItem, type StockPriceData } from '../services/dashboard';
import MobileStockList from '../components/mobile/stock/MobileStockList';
import MobileStockDetail from '../components/mobile/stock/MobileStockDetail';
import MobileAddToWatchlistDrawer from '../components/mobile/stock/MobileAddToWatchlistDrawer';
import EditWatchlistItemModal from '../components/EditWatchlistItemModal';
import './MobileDashboardPage.css';
import { PlusOutlined } from '@ant-design/icons';

const { Option } = Select;

type PriorityFilterValue = 'all' | 3 | 2 | 1;

type ExchangeFilterValue = 'all' | 'us' | 'hk' | 'cn';

const PRIORITY_FILTER_OPTIONS: Array<{ label: string; value: PriorityFilterValue }> = [
  { label: 'All Priorities', value: 'all' },
  { label: 'High', value: 3 },
  { label: 'Medium', value: 2 },
  { label: 'Low', value: 1 }
];

const EXCHANGE_FILTER_TABS: Array<{ label: string; value: ExchangeFilterValue }> = [
  { label: 'All', value: 'all' },
  { label: 'US', value: 'us' },
  { label: 'HK', value: 'hk' },
  { label: 'CN', value: 'cn' }
];

const EXCHANGE_CODE_ALIASES: Record<string, string> = {
  NASDAQGS: 'NASDAQ',
  NASDAQGM: 'NASDAQ',
  NASDAQCM: 'NASDAQ',
  NASDAQ: 'NASDAQ',
  NAS: 'NASDAQ',
  NMS: 'NASDAQ',
  NYSEARCA: 'NYSE',
  NYSEAMERICAN: 'AMEX',
  NYSEMKT: 'AMEX',
  ARCA: 'NYSE',
  BATS: 'US',
  OTCQX: 'OTC',
  OTCQB: 'OTC',
  OTCMKTS: 'OTC',
  OTCM: 'OTC',
  PINK: 'OTC',
  HKEX: 'HK',
  HKG: 'HK',
  XHKG: 'HK',
  HK: 'HK',
  SHSE: 'SHG',
  SSE: 'SHG',
  XSHG: 'SHG',
  SZSE: 'SHE',
  XSHE: 'SHE',
  CNEQ: 'SHE',
  CNSZ: 'SHE',
  CNSH: 'SHG'
};

const EXCHANGE_GROUPS: Record<Exclude<ExchangeFilterValue, 'all'>, Set<string>> = {
  us: new Set([
    'US',
    'NYSE',
    'NASDAQ',
    'AMEX',
    'XNYS',
    'XNAS',
    'XASE',
    'NYSEARCA',
    'NYSEAMERICAN',
    'NYSEMKT',
    'ARCA',
    'OTC',
    'BATS'
  ]),
  hk: new Set(['HK', 'HKEX', 'HKG', 'XHKG']),
  cn: new Set(['SHG', 'SHE', 'SHSE', 'SSE', 'SZSE', 'XSHE', 'XSHG', 'CN'])
};

const normalizeExchangeCode = (code?: string | null): string | undefined => {
  if (!code) return undefined;
  const upper = code.trim().toUpperCase();
  if (!upper) return undefined;
  return EXCHANGE_CODE_ALIASES[upper] || upper;
};

const resolveBaseSymbol = (symbol: string): string => {
  if (!symbol) return symbol;
  const parts = symbol.split('.');
  return parts[0];
};

const resolveExchangeCode = (item: WatchlistItem, priceData?: StockPriceData): string | undefined => {
  const candidates = [
    item.exchange,
    item.symbol.includes('.') ? item.symbol.split('.')[1] : undefined,
    priceData?.code?.includes('.') ? priceData.code.split('.')[1] : undefined,
    priceData?.code
  ];

  for (const candidate of candidates) {
    const normalized = normalizeExchangeCode(candidate);
    if (normalized) {
      return normalized;
    }
  }

  return undefined;
};

interface StockWithPrice extends WatchlistItem {
  priceData?: StockPriceData;
  resolvedExchange?: string;
  resolvedSymbol?: string;
}

/**
 * 移动端Dashboard页面
 * 管理股票列表和详情页的显示
 */
const MobileDashboardPage: React.FC = () => {
  const [watchlistGroups, setWatchlistGroups] = useState<WatchlistGroup[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [pricesLoading, setPricesLoading] = useState<boolean>(false);
  const [stockPrices, setStockPrices] = useState<Map<string, StockPriceData>>(new Map());
  const [priorityFilter, setPriorityFilter] = useState<PriorityFilterValue>('all');
  const [exchangeFilter, setExchangeFilter] = useState<ExchangeFilterValue>('all');
  const [addDrawerOpen, setAddDrawerOpen] = useState<boolean>(false);
  const [actionsModalOpen, setActionsModalOpen] = useState<boolean>(false);
  const [actionStock, setActionStock] = useState<StockWithPrice | null>(null);
  const [editModalOpen, setEditModalOpen] = useState<boolean>(false);
  const [editingItem, setEditingItem] = useState<WatchlistItem | null>(null);
  
  // 详情页状态
  const [selectedStock, setSelectedStock] = useState<StockWithPrice | null>(null);

  // 加载watchlist数据
  useEffect(() => {
    loadWatchlist();
  }, []);

  // 加载价格数据
  useEffect(() => {
    if (watchlistGroups.length > 0) {
      loadPrices();
      
      // 定期刷新价格（每30秒）
      const interval = setInterval(loadPrices, 30000);
      return () => clearInterval(interval);
    }
  }, [watchlistGroups]);

  const loadWatchlist = async () => {
    try {
      setLoading(true);
      const data = await dashboardAPI.getUserWatchlist();
      setWatchlistGroups(data);
      
      // 调试信息：打印watchlist数据
      const totalStocks = data.reduce((sum, group) => sum + group.items.length, 0);
      console.log(`Loaded ${data.length} groups, ${totalStocks} stocks`);
      if (data.length > 0 && data[0].items.length > 0) {
        console.log('Sample stock:', data[0].items[0]);
      }
    } catch (error) {
      console.error('Failed to load watchlist:', error);
      message.error('Failed to load watchlist');
    } finally {
      setLoading(false);
    }
  };

  const loadPrices = async () => {
    try {
      setPricesLoading(true);
      const response = await dashboardAPI.getWatchlistStockPrices();
      
      if (response.success && response.data) {
        const pricesMap = new Map<string, StockPriceData>();
        response.data.forEach(priceData => {
          // 保存完整的ticker（包含交易所后缀）
          pricesMap.set(priceData.code, priceData);
          
          // 也保存不带交易所后缀的版本，以便匹配watchlist中的symbol
          const symbolWithoutExchange = priceData.code.split('.')[0];
          pricesMap.set(symbolWithoutExchange, priceData);
        });
        setStockPrices(pricesMap);
        
        // 调试信息
        console.log(`Loaded ${response.data.length} stock prices`);
        if (response.data.length > 0) {
          console.log('Sample price data:', response.data[0]);
          console.log('Price map keys (first 10):', Array.from(pricesMap.keys()).slice(0, 10));
        }
      }
    } catch (error) {
      console.error('Failed to load prices:', error);
      message.error('Failed to load stock prices');
    } finally {
      setPricesLoading(false);
    }
  };

  // 获取所有股票（合并价格数据）
  const getAllStocks = (): StockWithPrice[] => {
    const allStocks: StockWithPrice[] = [];
    
    watchlistGroups.forEach(group => {
      group.items.forEach(item => {
        // 尝试多种方式匹配价格数据
        let priceData = stockPrices.get(item.symbol);
        
        // 如果直接匹配失败，尝试不带交易所后缀的版本
        if (!priceData && item.symbol.includes('.')) {
          const symbolWithoutExchange = item.symbol.split('.')[0];
          priceData = stockPrices.get(symbolWithoutExchange);
        }
        
        // 如果还是没有，尝试添加交易所后缀（如果symbol不带后缀）
        if (!priceData && !item.symbol.includes('.') && item.exchange) {
          const symbolWithExchange = `${item.symbol}.${item.exchange}`;
          priceData = stockPrices.get(symbolWithExchange);
        }
        
        const resolvedExchange = resolveExchangeCode(item, priceData);
        const resolvedSymbol = resolveBaseSymbol(item.symbol);

        allStocks.push({
          ...item,
          priceData,
          resolvedExchange,
          resolvedSymbol
        });
      });
    });

    return allStocks;
  };

  // 根据优先级筛选股票
  const getFilteredStocks = (): StockWithPrice[] => {
    const stocks = getAllStocks();
    
    const filteredByPriority = priorityFilter === 'all'
      ? stocks
      : stocks.filter(stock => {
          const stockPriority = stock.priority ?? 0;
          return stockPriority === priorityFilter;
        });

    return filteredByPriority.filter(stock => {
      if (exchangeFilter === 'all') {
        return true;
      }

      const exchangeCode = stock.resolvedExchange;
      const exchangeGroup = EXCHANGE_GROUPS[exchangeFilter];

      if (exchangeCode && exchangeGroup.has(exchangeCode)) {
        return true;
      }

      const baseSymbol = stock.resolvedSymbol || resolveBaseSymbol(stock.symbol);

      if (exchangeFilter === 'hk' && /^\d{4}$/.test(baseSymbol)) {
        return true;
      }

      if (exchangeFilter === 'cn' && /^\d{6}$/.test(baseSymbol)) {
        return true;
      }

      return false;
    });
  };

  // 获取展示股票
  const getDisplayStocks = (): StockWithPrice[] => {
    return getFilteredStocks();
  };

  // 返回列表页
  const handleBackToList = () => {
    setSelectedStock(null);
  };

  // 选择股票
  const handleStockClick = (stock: StockWithPrice) => {
    setSelectedStock(stock);
  };

  const handleStockLongPress = (stock: StockWithPrice) => {
    setActionStock(stock);
    setActionsModalOpen(true);
  };

  const handleCloseActionsModal = () => {
    setActionsModalOpen(false);
    setActionStock(null);
  };

  const handleEditStock = () => {
    if (!actionStock) return;
    setEditingItem(actionStock);
    setEditModalOpen(true);
    setActionsModalOpen(false);
  };

  const handleDeleteStock = () => {
    if (!actionStock) return;
    const stockToRemove = actionStock;

    Modal.confirm({
      title: 'Remove from Watchlist',
      content: `Are you sure you want to remove ${stockToRemove.symbol} from your watchlist?`,
      okText: 'Delete',
      okType: 'danger',
      cancelText: 'Cancel',
      centered: true,
      async onOk() {
        try {
          const result = await dashboardAPI.removeFromWatchlist(stockToRemove.watchlist_id);
          message.success(result.message || 'Removed from watchlist');
          if (selectedStock?.watchlist_id === stockToRemove.watchlist_id) {
            setSelectedStock(null);
          }
          handleCloseActionsModal();
          await loadWatchlist();
        } catch (error) {
          console.error('Failed to remove watchlist item:', error);
          message.error('Failed to remove watchlist item');
          throw error;
        }
      }
    });
  };

  const handleEditSuccess = async () => {
    await loadWatchlist();
  };

  const handleEditCancel = () => {
    setEditModalOpen(false);
    setEditingItem(null);
  };

  if (loading) {
    return (
      <div className="mobile-loading-container">
        <Spin size="large" />
      </div>
    );
  }

  // 显示详情页
  if (selectedStock) {
    return (
      <MobileStockDetail
        stock={selectedStock}
        onBack={handleBackToList}
      />
    );
  }

  // 显示列表页
  const displayStocks = getDisplayStocks();

  return (
    <div className="mobile-dashboard-page">
      {/* 筛选栏 */}
      <div className="mobile-dashboard-filters">
        <div className="mobile-dashboard-filter-row">
          <Select<PriorityFilterValue>
            value={priorityFilter}
            onChange={(value) => setPriorityFilter(value as PriorityFilterValue)}
            size="large"
            className="mobile-dashboard-priority-select"
          >
            {PRIORITY_FILTER_OPTIONS.map(option => (
              <Option key={option.value} value={option.value}>
                {option.label}
              </Option>
            ))}
          </Select>
          <Button
            type="primary"
            icon={<PlusOutlined />}
            shape="circle"
            size="large"
            onClick={() => setAddDrawerOpen(true)}
            aria-label="Add Stock"
          />
        </div>
        <Tabs
          className="mobile-dashboard-exchange-tabs"
          size="small"
          activeKey={exchangeFilter}
          onChange={(key) => setExchangeFilter(key as ExchangeFilterValue)}
          items={EXCHANGE_FILTER_TABS.map(tab => ({
            key: tab.value,
            label: tab.label
          }))}
        />
      </div>

      {/* 股票列表 */}
      {displayStocks.length === 0 ? (
        <Empty
          description="No stocks in watchlist"
          style={{ marginTop: 60 }}
        />
      ) : (
        <MobileStockList
          stocks={displayStocks}
          onStockClick={handleStockClick}
          loading={pricesLoading}
          onLongPress={handleStockLongPress}
        />
      )}
      <MobileAddToWatchlistDrawer
        open={addDrawerOpen}
        onClose={() => setAddDrawerOpen(false)}
        onSuccess={() => {
          void loadWatchlist();
        }}
      />
      <Modal
        className="mobile-stock-actions-modal"
        title={actionStock?.symbol}
        open={actionsModalOpen}
        centered
        onCancel={handleCloseActionsModal}
        footer={null}
        destroyOnClose
      >
        <Button
          block
          onClick={handleEditStock}
          className="mobile-stock-actions-button"
          disabled={!actionStock}
        >
          Edit
        </Button>
        <Button
          danger
          block
          onClick={handleDeleteStock}
          className="mobile-stock-actions-button"
          disabled={!actionStock}
        >
          Delete
        </Button>
        <Button
          block
          onClick={handleCloseActionsModal}
          className="mobile-stock-actions-button"
          type="text"
        >
          Cancel
        </Button>
      </Modal>
      <EditWatchlistItemModal
        open={editModalOpen}
        item={editingItem}
        onCancel={handleEditCancel}
        onSuccess={handleEditSuccess}
      />
    </div>
  );
};

export default MobileDashboardPage;

