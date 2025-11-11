import React, { useRef } from 'react';
import { List, Typography, Space } from 'antd';
import type { WatchlistItem, StockPriceData } from '../../../services/dashboard';
import { ArrowUpOutlined, ArrowDownOutlined, StarFilled, StarOutlined } from '@ant-design/icons';
import './MobileStockList.css';

const { Text } = Typography;

interface StockWithPrice extends WatchlistItem {
  priceData?: StockPriceData;
  resolvedExchange?: string;
  resolvedSymbol?: string;
}

interface MobileStockListProps {
  stocks: StockWithPrice[];
  onStockClick: (stock: StockWithPrice) => void;
  loading?: boolean;
  onLongPress?: (stock: StockWithPrice) => void;
}

/**
 * 移动端股票列表组件
 * 显示ticker代码、名称、价格、涨跌幅
 */
const MobileStockList: React.FC<MobileStockListProps> = ({
  stocks,
  onStockClick,
  loading = false,
  onLongPress
}) => {
  const longPressTimeoutRef = useRef<NodeJS.Timeout | null>(null);
  const longPressTriggeredRef = useRef<boolean>(false);

  const startLongPressTimer = (stock: StockWithPrice) => {
    if (!onLongPress) return;
    longPressTriggeredRef.current = false;
    longPressTimeoutRef.current = setTimeout(() => {
      longPressTriggeredRef.current = true;
      onLongPress(stock);
    }, 600);
  };

  const clearLongPressTimer = () => {
    if (longPressTimeoutRef.current) {
      clearTimeout(longPressTimeoutRef.current);
      longPressTimeoutRef.current = null;
    }
  };

  const renderPriorityStars = (priority?: number) => {
    if (!priority || priority <= 0) {
      return null;
    }

    const stars = [];
    for (let i = 0; i < Math.min(priority, 3); i++) {
      stars.push(
        <StarFilled
          key={i}
          style={{
            color: '#faad14',
            fontSize: '12px'
          }}
        />
      );
    }

    // 如果想参考电脑端的空星逻辑，可在这里补充StarOutlined
    if (priority < 3) {
      for (let i = priority; i < 3; i++) {
        stars.push(
          <StarOutlined
            key={`empty-${i}`}
            style={{
              color: '#d9d9d9',
              fontSize: '12px'
            }}
          />
        );
      }
    }

    return (
      <Space size={2} className="stock-priority-stars">
        {stars}
      </Space>
    );
  };

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

  const deriveExchangeCode = (stock: StockWithPrice): string | undefined => {
    if (stock.resolvedExchange) {
      return stock.resolvedExchange;
    }

    const candidates = [
      stock.exchange,
      stock.symbol.includes('.') ? stock.symbol.split('.')[1] : undefined,
      stock.priceData?.code?.includes('.') ? stock.priceData.code.split('.')[1] : undefined,
      stock.priceData?.code
    ];

    for (const candidate of candidates) {
      if (candidate && candidate.trim()) {
        return candidate.trim().toUpperCase();
      }
    }

    return undefined;
  };

  const deriveBaseSymbol = (stock: StockWithPrice): string => {
    if (stock.resolvedSymbol) {
      return stock.resolvedSymbol;
    }

    if (!stock.symbol) {
      return '';
    }

    return stock.symbol.includes('.') ? stock.symbol.split('.')[0] : stock.symbol;
  };

  return (
    <div className="mobile-stock-list">
      <List
        dataSource={stocks}
        loading={loading}
        renderItem={(stock) => {
          const change = stock.priceData?.change_percent || 0;
          const isPositive = change >= 0;
          const exchangeCode = deriveExchangeCode(stock);
          const displaySymbol = deriveBaseSymbol(stock);
          const handleClick = () => {
            if (longPressTriggeredRef.current) {
              longPressTriggeredRef.current = false;
              return;
            }
            onStockClick(stock);
          };

          const handleTouchStart = () => {
            startLongPressTimer(stock);
          };

          const handleTouchEnd = () => {
            clearLongPressTimer();
          };

          const handleTouchMove = () => {
            clearLongPressTimer();
          };

          const handleTouchCancel = () => {
            clearLongPressTimer();
          };
          
          return (
            <div 
              className="stock-list-item"
              onClick={handleClick}
              onTouchStart={handleTouchStart}
              onTouchEnd={handleTouchEnd}
              onTouchCancel={handleTouchCancel}
              onTouchMove={handleTouchMove}
              onContextMenu={(event) => {
                if (onLongPress) {
                  event.preventDefault();
                  onLongPress(stock);
                }
              }}
            >
              <div className="stock-item-left">
                <div className="stock-symbol">
                  <Text strong style={{ fontSize: '16px' }}>
                    {displaySymbol}
                  </Text>
                  {exchangeCode && (
                    <span className="stock-exchange-code">
                      {exchangeCode}
                    </span>
                  )}
                  {renderPriorityStars(stock.priority)}
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

