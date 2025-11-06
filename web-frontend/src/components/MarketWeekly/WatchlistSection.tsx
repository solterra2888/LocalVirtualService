import React, { useState, useEffect } from 'react';
import { Spin, Alert } from 'antd';
import { WatchlistTechnicalSection } from './TechnicalViewsSection';
import { marketWeeklyAPI } from '../../services/marketWeekly';
import type { WatchlistStock } from '../../types/marketWeekly';

interface WatchlistSectionProps {
  sectionId: string;
}

const WatchlistSection: React.FC<WatchlistSectionProps> = ({ sectionId }) => {
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [stocks, setStocks] = useState<WatchlistStock[]>([]);

  useEffect(() => {
    const loadWatchlist = async () => {
      try {
        setLoading(true);
        setError(null);
        // 不传user_id，API会使用当前登录用户
        const data = await marketWeeklyAPI.getWatchlistStocks();
        setStocks(data);
      } catch (err: any) {
        console.error('Error loading watchlist:', err);
        setError(err.message || 'Failed to load watchlist');
      } finally {
        setLoading(false);
      }
    };

    loadWatchlist();
  }, []);

  if (loading) {
    return (
      <div id={sectionId} className="subsection">
        <h3 className="subsection-title">My Watchlist</h3>
        <div style={{ textAlign: 'center', padding: '40px 0' }}>
          <Spin size="large" tip="Loading watchlist..." />
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div id={sectionId} className="subsection">
        <h3 className="subsection-title">My Watchlist</h3>
        <Alert
          message="Error Loading Watchlist"
          description={error}
          type="warning"
          showIcon
          style={{ margin: '20px 0' }}
        />
      </div>
    );
  }

  return <WatchlistTechnicalSection stocks={stocks} sectionId={sectionId} />;
};

export default WatchlistSection;
