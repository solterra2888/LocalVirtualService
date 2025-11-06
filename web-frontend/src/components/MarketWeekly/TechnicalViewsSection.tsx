import React, { Suspense, lazy, useState, useEffect, useRef } from 'react';
import { Collapse, Skeleton } from 'antd';
import CandlestickChart from '../CandlestickChart';
import type { AssetInfo } from '../../types/marketWeekly';

const { Panel } = Collapse;

interface TechnicalViewsSectionProps {
  assets: AssetInfo[];
  title: string;
  sectionId: string;
  groupByRegion?: boolean;
}

// 懒加载的图表组件（带可见性检测）
const LazyChartPanel: React.FC<{ asset: AssetInfo }> = ({ asset }) => {
  const [isVisible, setIsVisible] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!ref.current) return;

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setIsVisible(true);
          observer.disconnect();
        }
      },
      { rootMargin: '300px' } // 提前300px开始加载
    );

    observer.observe(ref.current);
    return () => observer.disconnect();
  }, []);

  return (
    <div ref={ref} style={{ minHeight: isVisible ? 'auto' : '450px' }}>
      {isVisible ? (
        <Suspense fallback={<Skeleton active paragraph={{ rows: 8 }} />}>
          <CandlestickChart
            symbol={asset.symbol}
            companyName={asset.name}
            primaryTicker={asset.primary_ticker || asset.symbol}
            showHeader={false}
            showVolume={false}
            height={400}
          />
        </Suspense>
      ) : (
        <Skeleton active paragraph={{ rows: 8 }} />
      )}
    </div>
  );
};

const TechnicalViewsSection: React.FC<TechnicalViewsSectionProps> = ({
  assets,
  title,
  sectionId,
  groupByRegion = false
}) => {
  if (!assets || assets.length === 0) {
    return (
      <div id={sectionId} className="subsection">
        <h3 className="subsection-title">{title}</h3>
        <p style={{ color: '#999', fontStyle: 'italic' }}>No data available</p>
      </div>
    );
  }

  if (groupByRegion) {
    // 按区域分组
    const groupedAssets: Record<string, AssetInfo[]> = {};
    assets.forEach(asset => {
      const region = asset.region || 'Other';
      if (!groupedAssets[region]) {
        groupedAssets[region] = [];
      }
      groupedAssets[region].push(asset);
    });

    return (
      <div id={sectionId} className="subsection">
        <h3 className="subsection-title">{title}</h3>
        {Object.entries(groupedAssets).map(([region, regionAssets]) => (
          <div key={region} style={{ marginBottom: '24px' }}>
            <h4 style={{ fontSize: '14px', fontWeight: 600, color: '#34495E', marginBottom: '12px' }}>
              {region} Markets
            </h4>
            <Collapse
              accordion
              bordered={false}
              style={{ background: 'transparent' }}
            >
              {regionAssets.map(asset => (
                <Panel
                  header={`${asset.name} (${asset.symbol})`}
                  key={asset.symbol}
                  style={{ marginBottom: '8px' }}
                >
                  <LazyChartPanel asset={asset} />
                </Panel>
              ))}
            </Collapse>
          </div>
        ))}
      </div>
    );
  }

  // 不分组，直接展示
  return (
    <div id={sectionId} className="subsection">
      <h3 className="subsection-title">{title}</h3>
      <Collapse
        accordion
        bordered={false}
        style={{ background: 'transparent' }}
      >
        {assets.map(asset => (
          <Panel
            header={`${asset.name} (${asset.symbol})`}
            key={asset.symbol}
            style={{ marginBottom: '8px' }}
          >
            <LazyChartPanel asset={asset} />
          </Panel>
        ))}
      </Collapse>
    </div>
  );
};

export default TechnicalViewsSection;

