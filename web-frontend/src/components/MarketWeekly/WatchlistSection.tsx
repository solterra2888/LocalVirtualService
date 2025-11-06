import React, { useState, useEffect } from 'react';
import { Collapse, Skeleton, Alert, Empty } from 'antd';
import { useAuthStore } from '../../store/authStore';
import { marketWeeklyAPI } from '../../services/marketWeekly';
import TechnicalViewsSection from './TechnicalViewsSection';
import type { WatchlistStock } from '../../types/marketWeekly';

const { Panel } = Collapse;

interface WatchlistSectionProps {
  sectionId: string;
}

const WatchlistSection: React.FC<WatchlistSectionProps> = ({ sectionId }) => {
  const { user } = useAuthStore();
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [watchlistStocks, setWatchlistStocks] = useState<WatchlistStock[]>([]);

  useEffect(() => {
    const loadWatchlist = async () => {
      if (!user?.id) {
        setLoading(false);
        return;
      }

      try {
        setLoading(true);
        const stocks = await marketWeeklyAPI.getWatchlistStocks(user.id);
        setWatchlistStocks(stocks);
      } catch (err: any) {
        setError(err.message || 'Failed to load watchlist');
        console.error('Error loading watchlist:', err);
      } finally {
        setLoading(false);
      }
    };

    loadWatchlist();
  }, [user?.id]);

  if (loading) {
    return (
      <div id={sectionId} className="subsection">
        <h3 className="subsection-title">My Watchlist</h3>
        <Skeleton active paragraph={{ rows: 4 }} />
      </div>
    );
  }

  if (error) {
    return (
      <div id={sectionId} className="subsection">
        <h3 className="subsection-title">My Watchlist</h3>
        <Alert
          message="Error"
          description={error}
          type="error"
          showIcon
        />
      </div>
    );
  }

  if (!watchlistStocks || watchlistStocks.length === 0) {
    return (
      <div id={sectionId} className="subsection">
        <h3 className="subsection-title">My Watchlist</h3>
        <Empty
          description="Your watchlist is empty. Add stocks to your watchlist to see them here."
          style={{ padding: '40px 0' }}
        />
      </div>
    );
  }

  // 转换为AssetInfo格式
  const assets = watchlistStocks.map(stock => ({
    symbol: stock.symbol,
    name: stock.company_name,
    primary_ticker: stock.primary_ticker
  }));

  return (
    <TechnicalViewsSection
      assets={assets}
      title="My Watchlist"
      sectionId={sectionId}
      groupByRegion={false}
    />
  );
};

export default WatchlistSection;

