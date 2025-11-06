import React, { useState, useEffect } from 'react';
import { Collapse, Skeleton, Empty, Spin, Alert } from 'antd';
import { marketWeeklyAPI } from '../../services/marketWeekly';
import type { AssetInfo, WatchlistStock } from '../../types/marketWeekly';

// 图表图片组件
interface ChartImageProps {
  symbol: string;
  primaryTicker: string;
  label: string;
  days?: number;
}

const ChartImage: React.FC<ChartImageProps> = ({ symbol, primaryTicker, label, days = 90 }) => {
  const [imageSrc, setImageSrc] = useState<string | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const loadImage = async () => {
      try {
        setLoading(true);
        setError(null);
        // 使用 primaryTicker 获取图表
        const imageBase64 = await marketWeeklyAPI.getChartImage(primaryTicker, label, days);
        setImageSrc(`data:image/png;base64,${imageBase64}`);
      } catch (err: any) {
        console.error(`Error loading chart image for ${symbol}:`, err);
        let errorMessage = 'Failed to load chart image';
        if (err.response?.data?.detail) {
          errorMessage = err.response.data.detail;
        } else if (err.message) {
          errorMessage = err.message;
        }
        setError(errorMessage);
      } finally {
        setLoading(false);
      }
    };

    loadImage();
  }, [symbol, primaryTicker, label, days]);

  if (loading) {
    return (
      <div style={{ textAlign: 'center', padding: '40px 0' }}>
        <Spin size="large" tip="Loading chart..." />
      </div>
    );
  }

  if (error) {
    // 如果是 404 错误，显示友好提示
    const is404 = error.includes('404') || error.includes('not found') || error.includes('No data available');
    
    if (is404) {
      return (
        <Empty
          description={`No historical data available for ${label}`}
          style={{ padding: '40px 0' }}
        />
      );
    }
    
    return (
      <Alert
        message="Chart Error"
        description={error}
        type="warning"
        showIcon
        style={{ margin: '20px 0' }}
      />
    );
  }

  if (!imageSrc) {
    return (
      <Empty
        description="No chart image available"
        style={{ padding: '40px 0' }}
      />
    );
  }

  return (
    <div style={{ width: '100%', margin: '20px 0' }}>
      <img
        src={imageSrc}
        alt={`${label} chart`}
        style={{
          width: '100%',
          height: 'auto',
          border: '1px solid #e8e8e8',
          borderRadius: '4px',
          display: 'block'
        }}
      />
    </div>
  );
};

interface TechnicalViewsSectionProps {
  assets: AssetInfo[];
  sectionId: string;
  title: string;
  groupByRegion?: boolean;
}

interface WatchlistSectionProps {
  stocks: WatchlistStock[];
  sectionId: string;
}

// 普通资产列表（不分组）- 内部使用
const SimpleTechnicalSectionInternal: React.FC<TechnicalViewsSectionProps> = ({
  assets,
  sectionId,
  title
}) => {
  if (!assets || assets.length === 0) {
    return (
      <Empty description="No data available" style={{ padding: '40px 0' }} />
    );
  }

  // 默认展开所有面板
  const defaultActiveKeys = assets.map(asset => asset.symbol);

  return (
    <Collapse
      defaultActiveKey={defaultActiveKeys}
      bordered={false}
      style={{ background: 'transparent' }}
    >
      {assets.map((asset) => (
        <Collapse.Panel
          key={asset.symbol}
          header={<span style={{ fontWeight: 600, fontSize: '13px' }}>{asset.name}</span>}
        >
          <ChartImage
            symbol={asset.symbol}
            primaryTicker={asset.primary_ticker || asset.symbol}
            label={asset.name}
            days={90}
          />
        </Collapse.Panel>
      ))}
    </Collapse>
  );
};

// 按区域分组的指数显示 - 内部使用
const EquityIndicesTechnicalSectionInternal: React.FC<TechnicalViewsSectionProps> = ({
  assets,
  sectionId,
  groupByRegion = true
}) => {
  // 按区域分组
  const regions = {
    US: assets.filter(a => a.region === 'US'),
    Europe: assets.filter(a => a.region === 'Europe'),
    Asia: assets.filter(a => a.region === 'Asia'),
    Other: assets.filter(a => !['US', 'Europe', 'Asia'].includes(a.region || ''))
  };

  // 默认展开所有区域
  const defaultActiveKeys = Object.keys(regions).filter(region => regions[region as keyof typeof regions].length > 0);

  return (
    <Collapse
      defaultActiveKey={defaultActiveKeys}
      bordered={false}
      style={{ background: 'transparent' }}
    >
      {Object.entries(regions).map(([region, regionAssets]) => {
        if (regionAssets.length === 0) return null;
        
        return (
          <Collapse.Panel
            key={region}
            header={<span style={{ fontWeight: 600, fontSize: '13px' }}>{region} Markets</span>}
          >
            {regionAssets.map((asset) => (
              <div key={asset.symbol} style={{ marginBottom: '40px' }}>
                <h5 style={{ fontSize: '13px', fontWeight: 600, color: '#666', marginBottom: '12px' }}>
                  {asset.name}
                </h5>
                <ChartImage
                  symbol={asset.symbol}
                  primaryTicker={asset.primary_ticker || asset.symbol}
                  label={asset.name}
                  days={90}
                />
              </div>
            ))}
          </Collapse.Panel>
        );
      })}
    </Collapse>
  );
};

// 统一的默认导出组件
const TechnicalViewsSection: React.FC<TechnicalViewsSectionProps> = ({
  assets,
  sectionId,
  title,
  groupByRegion = false
}) => {
  return (
    <div id={sectionId} className="subsection">
      <h3 className="subsection-title">{title}</h3>
      {groupByRegion ? (
        <EquityIndicesTechnicalSectionInternal
          assets={assets}
          sectionId={sectionId}
          title={title}
          groupByRegion={true}
        />
      ) : (
        <SimpleTechnicalSectionInternal
          assets={assets}
          sectionId={sectionId}
          title={title}
          groupByRegion={false}
        />
      )}
    </div>
  );
};

export default TechnicalViewsSection;

// 也导出具名组件供单独使用
export { SimpleTechnicalSectionInternal as SimpleTechnicalSection };
export { EquityIndicesTechnicalSectionInternal as EquityIndicesTechnicalSection };

// Watchlist股票显示的具名导出
export const WatchlistTechnicalSection: React.FC<WatchlistSectionProps> = ({
  stocks,
  sectionId
}) => {
  if (!stocks || stocks.length === 0) {
    return (
      <div id={sectionId} className="subsection">
        <h3 className="subsection-title">My Watchlist</h3>
        <Empty
          description="No stocks in your watchlist"
          style={{ padding: '40px 0' }}
        />
      </div>
    );
  }

  // 默认展开所有股票
  const defaultActiveKeys = stocks.map(stock => stock.symbol);

  return (
    <div id={sectionId} className="subsection">
      <h3 className="subsection-title">My Watchlist</h3>
      <Collapse
        defaultActiveKey={defaultActiveKeys}
        bordered={false}
        style={{ background: 'transparent' }}
      >
        {stocks.map((stock) => (
        <Collapse.Panel
          key={stock.symbol}
          header={
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
              <span style={{ fontWeight: 600, fontSize: '13px' }}>
                {stock.company_name} ({stock.symbol})
              </span>
            </div>
          }
        >
          <ChartImage
            symbol={stock.symbol}
            primaryTicker={stock.primary_ticker}
            label={stock.company_name}
            days={90}
          />
        </Collapse.Panel>
        ))}
      </Collapse>
    </div>
  );
};
