import React, { useState, useEffect } from 'react';
import { Spin, message, Select, Space, Empty } from 'antd';
import { dashboardAPI, type WatchlistGroup, type WatchlistItem, type StockPriceData } from '../services/dashboard';
import MobileStockList from '../components/mobile/stock/MobileStockList';
import MobileStockDetail from '../components/mobile/stock/MobileStockDetail';
import './MobileDashboardPage.css';

const { Option } = Select;

interface StockWithPrice extends WatchlistItem {
  priceData?: StockPriceData;
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
  
  const [sortBy, setSortBy] = useState<string>('change_desc'); // change_desc, change_asc, name
  
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
        
        allStocks.push({
          ...item,
          priceData
        });
      });
    });

    return allStocks;
  };

  // 排序股票
  const getSortedStocks = (): StockWithPrice[] => {
    const stocks = getAllStocks();
    
    switch (sortBy) {
      case 'change_desc':
        return stocks.sort((a, b) => {
          const changeA = a.priceData?.change_percent || 0;
          const changeB = b.priceData?.change_percent || 0;
          return changeB - changeA;
        });
      case 'change_asc':
        return stocks.sort((a, b) => {
          const changeA = a.priceData?.change_percent || 0;
          const changeB = b.priceData?.change_percent || 0;
          return changeA - changeB;
        });
      case 'name':
        return stocks.sort((a, b) => {
          return a.symbol.localeCompare(b.symbol);
        });
      default:
        return stocks;
    }
  };

  // 返回列表页
  const handleBackToList = () => {
    setSelectedStock(null);
  };

  // 选择股票
  const handleStockClick = (stock: StockWithPrice) => {
    setSelectedStock(stock);
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
  const sortedStocks = getSortedStocks();

  return (
    <div className="mobile-dashboard-page">
      {/* 筛选栏 */}
      <div className="mobile-dashboard-filters">
        <Space direction="vertical" size="small" style={{ width: '100%' }}>
          <Select
            value={sortBy}
            onChange={setSortBy}
            style={{ width: '100%' }}
            size="large"
          >
            <Option value="change_desc">Top Gainers</Option>
            <Option value="change_asc">Top Losers</Option>
            <Option value="name">By Name</Option>
          </Select>
        </Space>
      </div>

      {/* 股票列表 */}
      {sortedStocks.length === 0 ? (
        <Empty
          description="No stocks in watchlist"
          style={{ marginTop: 60 }}
        />
      ) : (
        <MobileStockList
          stocks={sortedStocks}
          onStockClick={handleStockClick}
          loading={pricesLoading}
        />
      )}
    </div>
  );
};

export default MobileDashboardPage;

