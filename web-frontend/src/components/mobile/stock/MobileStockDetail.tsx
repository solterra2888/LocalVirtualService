import React, { useState, useEffect, useCallback } from 'react';
import { Button, Tabs, Typography, Space, Spin, message } from 'antd';
import { ArrowLeftOutlined, ArrowUpOutlined, ArrowDownOutlined } from '@ant-design/icons';
import type { WatchlistItem, StockPriceData, DashboardNewsItem } from '../../../services/dashboard';
import { dashboardAPI } from '../../../services/dashboard';
import MobileStockChart from './MobileStockChart';
import MobileStockOverview from './MobileStockOverview';
import MobileNewsList from '../news/MobileNewsList';
import './MobileStockDetail.css';

const { Title, Text } = Typography;

interface StockWithPrice extends WatchlistItem {
  priceData?: StockPriceData;
}

interface MobileStockDetailProps {
  stock: StockWithPrice;
  onBack: () => void;
}

/**
 * 移动端股票详情页组件
 * 显示股票信息、图表、新闻和研报
 */
const MobileStockDetail: React.FC<MobileStockDetailProps> = ({ stock, onBack }) => {
  const [activeTab, setActiveTab] = useState<string>('news');
  const [newsData, setNewsData] = useState<DashboardNewsItem[]>([]);
  const [loading, setLoading] = useState<boolean>(false);

  const loadNews = useCallback(async () => {
    try {
      setLoading(true);
      const news = await dashboardAPI.getCompanyNews(stock.symbol, { limit: 20 });
      setNewsData(news);
    } catch (error) {
      console.error('Failed to load news:', error);
      message.error('Failed to load news');
    } finally {
      setLoading(false);
    }
  }, [stock.symbol]);

  useEffect(() => {
    if (activeTab === 'news') {
      loadNews();
    }
  }, [activeTab, loadNews]);

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

  const change = stock.priceData?.change_percent || 0;
  const isPositive = change >= 0;

  const tabItems = [
    {
      key: 'news',
      label: 'News',
      children: (
        <div className="mobile-tab-content">
          {/* 新闻列表 */}
          {loading ? (
            <div className="mobile-loading">
              <Spin />
            </div>
          ) : newsData.length === 0 ? (
            <div className="mobile-loading">
              <Text type="secondary">No news available</Text>
            </div>
          ) : (
            <MobileNewsList news={newsData} />
          )}
        </div>
      ),
    },
    {
      key: 'overview',
      label: 'Overview',
      children: (
        <div className="mobile-tab-content mobile-overview-content">
          <section className="mobile-overview-section mobile-chart-section">
            <MobileStockChart
              symbol={stock.symbol}
              primaryTicker={stock.primaryticker}
            />
          </section>
          <MobileStockOverview stock={stock} />
        </div>
      ),
    },
  ];

  return (
    <div className="mobile-stock-detail">
      {/* 顶部导航栏 */}
      <div className="mobile-stock-header">
        <Button
          type="text"
          icon={<ArrowLeftOutlined />}
          onClick={onBack}
          className="back-button"
        />
        <div className="header-title">
          <Text strong>{stock.custom_name || stock.company || stock.symbol}</Text>
        </div>
        <div style={{ width: 32 }} /> {/* Placeholder for centering */}
      </div>

      {/* 股票基本信息 */}
      <div className="mobile-stock-info">
        <div className="stock-info-header">
          <div>
            <Title level={4} style={{ margin: 0, fontSize: '22px', fontWeight: 600 }}>
              {stock.custom_name || stock.company || stock.symbol}
            </Title>
            <Text type="secondary" style={{ fontSize: '12px' }}>
              {stock.symbol}
            </Text>
          </div>
        </div>

        <div className="stock-info-price">
          <Space direction="vertical" size={4}>
            <div>
              <Text 
                style={{ 
                  fontSize: '28px', 
                  fontWeight: 'bold',
                  color: getChangeColor(change)
                }}
              >
                {formatPrice(stock.priceData?.close)}
              </Text>
            </div>
            <div>
              <Space size={8}>
                {isPositive ? (
                  <ArrowUpOutlined style={{ fontSize: '12px', color: '#cf1322' }} />
                ) : (
                  <ArrowDownOutlined style={{ fontSize: '12px', color: '#3f8600' }} />
                )}
                <Text 
                  style={{ 
                    fontSize: '14px',
                    fontWeight: 500,
                    color: getChangeColor(change)
                  }}
                >
                  {formatChange(change)}
                </Text>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  {stock.priceData?.last_updated ? 
                    new Date(stock.priceData.last_updated).toLocaleString(undefined, {
                      month: 'short',
                      day: 'numeric',
                      hour: '2-digit',
                      minute: '2-digit'
                    }) : 
                    'Not Open'
                  }
                </Text>
              </Space>
            </div>
          </Space>
        </div>
      </div>

      {/* Tabs */}
      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        items={tabItems}
        className="mobile-stock-tabs"
        size="large"
      />
    </div>
  );
};

export default MobileStockDetail;


