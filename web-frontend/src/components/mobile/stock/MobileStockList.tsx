import React from 'react';
import { List, Typography, Space } from 'antd';
import type { WatchlistItem, StockPriceData } from '../../../services/dashboard';
import { ArrowUpOutlined, ArrowDownOutlined } from '@ant-design/icons';
import './MobileStockList.css';

const { Text } = Typography;

interface StockWithPrice extends WatchlistItem {
  priceData?: StockPriceData;
}

interface MobileStockListProps {
  stocks: StockWithPrice[];
  onStockClick: (stock: StockWithPrice) => void;
  loading?: boolean;
}

/**
 * 移动端股票列表组件
 * 显示ticker代码、名称、价格、涨跌幅
 */
const MobileStockList: React.FC<MobileStockListProps> = ({
  stocks,
  onStockClick,
  loading = false
}) => {
  const formatPrice = (price?: number) => {
    if (price === undefined || price === null) return '--';
    return price.toFixed(3);
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

  return (
    <div className="mobile-stock-list">
      <List
        dataSource={stocks}
        loading={loading}
        renderItem={(stock) => {
          const change = stock.priceData?.change_percent || 0;
          const isPositive = change >= 0;
          
          return (
            <div 
              className="stock-list-item"
              onClick={() => onStockClick(stock)}
            >
              <div className="stock-item-left">
                <div className="stock-symbol">
                  <Text strong style={{ fontSize: '16px' }}>
                    {stock.symbol}
                  </Text>
                </div>
                <div className="stock-name">
                  <Text type="secondary" style={{ fontSize: '13px' }}>
                    {stock.custom_name || stock.company || stock.symbol}
                  </Text>
                </div>
              </div>

              <div className="stock-item-right">
                <div className="stock-price">
                  <Text 
                    strong 
                    style={{ 
                      fontSize: '16px',
                      color: getChangeColor(change)
                    }}
                  >
                    {formatPrice(stock.priceData?.close)}
                  </Text>
                </div>
                <div className="stock-change">
                  <Space size={4}>
                    {isPositive ? (
                      <ArrowUpOutlined style={{ fontSize: '12px', color: '#cf1322' }} />
                    ) : (
                      <ArrowDownOutlined style={{ fontSize: '12px', color: '#3f8600' }} />
                    )}
                    <Text 
                      style={{ 
                        fontSize: '13px',
                        color: getChangeColor(change),
                        fontWeight: 500
                      }}
                    >
                      {formatChange(change)}
                    </Text>
                  </Space>
                </div>
              </div>
            </div>
          );
        }}
      />
    </div>
  );
};

export default MobileStockList;

