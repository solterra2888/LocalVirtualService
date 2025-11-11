import React, { useEffect, useMemo, useRef, useState } from 'react';
import { Empty, Spin, Tag, Typography, message } from 'antd';
import {
  BarChartOutlined,
  FundProjectionScreenOutlined,
  InfoCircleOutlined,
  LineChartOutlined,
  StarOutlined
} from '@ant-design/icons';
import type { WatchlistItem } from '../../../services/dashboard';
import type { CompanyFundamentalsResponse } from '../../../services/fundamentals';
import { fundamentalsAPI, fundamentalsUtils } from '../../../services/fundamentals';
import './MobileStockOverview.css';

const { Title, Text, Paragraph } = Typography;

interface MobileStockOverviewProps {
  stock: WatchlistItem;
}

type FundamentalsCache = Map<string, CompanyFundamentalsResponse>;

const getOverviewQuery = (stock: Pick<WatchlistItem, 'symbol' | 'primaryticker'>): string | null => {
  if (stock.primaryticker && stock.primaryticker.trim()) {
    return stock.primaryticker.trim();
  }
  if (stock.symbol && stock.symbol.trim()) {
    return stock.symbol.trim();
  }
  return null;
};

const hasValue = (value: string): boolean => value.trim().length > 0 && value.trim() !== 'N/A';

const MobileStockOverview: React.FC<MobileStockOverviewProps> = ({ stock }) => {
  const [overviewData, setOverviewData] = useState<CompanyFundamentalsResponse | null>(null);
  const [loading, setLoading] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);

  const cacheRef = useRef<FundamentalsCache>(new Map());
  const lastErrorRef = useRef<string | null>(null);

  useEffect(() => {
    const query = getOverviewQuery({
      primaryticker: stock.primaryticker,
      symbol: stock.symbol
    });
    if (!query) {
      const errorMessage = 'No valid ticker found for fundamentals overview';
      setOverviewData(null);
      setError(errorMessage);
      return;
    }

    const normalizedQuery = query.toUpperCase();
    if (cacheRef.current.has(normalizedQuery)) {
      setOverviewData(cacheRef.current.get(normalizedQuery) || null);
      setError(null);
      setLoading(false);
      return;
    }

    let cancelled = false;
    const fetchOverview = async () => {
      setLoading(true);
      setError(null);
      setOverviewData(null);

      try {
        const data = await fundamentalsAPI.searchCompany(query);
        if (cancelled) {
          return;
        }
        cacheRef.current.set(normalizedQuery, data);
        setOverviewData(data);
        setError(null);
        lastErrorRef.current = null;
      } catch (fetchError) {
        if (cancelled) {
          return;
        }
        const errorMessage =
          fetchError instanceof Error ? fetchError.message : 'Failed to load fundamentals overview';
        setError(errorMessage);
        if (lastErrorRef.current !== errorMessage) {
          message.error(errorMessage);
          lastErrorRef.current = errorMessage;
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    };

    fetchOverview();

    return () => {
      cancelled = true;
    };
  }, [stock.primaryticker, stock.symbol]);

  const company = overviewData?.company;
  const highlights = overviewData?.highlights;
  const valuations = overviewData?.valuations;
  const analystRatings = overviewData?.analyst_ratings ?? [];
  const technicals = overviewData?.technicals;

  const currencyCode = company?.currencycode || 'USD';

  const companyFacts = useMemo(() => {
    if (!company) return [];

    const items = [
      { label: 'Name', value: company.name || stock.custom_name || 'N/A' },
      { label: 'Ticker', value: company.code || company.primaryticker || stock.symbol || 'N/A' },
      { label: 'Exchange', value: company.exchange || stock.exchange || 'N/A' },
      { label: 'Currency', value: currencyCode },
      { label: 'Country', value: company.countryname || 'N/A' },
      { label: 'Sector', value: company.sector || stock.sector || 'N/A' },
      { label: 'Industry', value: company.industry || stock.industry || 'N/A' },
      {
        label: 'Employees',
        value: company.fulltimeemployees
          ? company.fulltimeemployees.toLocaleString()
          : 'N/A'
      },
      { label: 'Type', value: company.type || 'N/A' }
    ];

    return items.filter(item => hasValue(item.value));
  }, [company, currencyCode, stock.custom_name, stock.symbol, stock.exchange, stock.sector, stock.industry]);

  const highlightMetrics = useMemo(() => {
    if (!highlights) return [];

    const metrics = [
      {
        label: 'Market Cap',
        value: fundamentalsUtils.formatCurrency(highlights.marketcapitalization, currencyCode)
      },
      {
        label: 'P/E Ratio',
        value: fundamentalsUtils.formatNumber(highlights.peratio)
      },
      {
        label: 'PEG Ratio',
        value: fundamentalsUtils.formatNumber(highlights.pegratio)
      },
      {
        label: 'Revenue (TTM)',
        value: fundamentalsUtils.formatCurrency(highlights.revenuettm, currencyCode)
      },
      {
        label: 'EPS (TTM)',
        value: fundamentalsUtils.formatNumber(highlights.earningsshare)
      },
      {
        label: 'Dividend Yield',
        value: fundamentalsUtils.formatPercentage(highlights.dividendyield)
      },
      {
        label: 'ROE (TTM)',
        value: fundamentalsUtils.formatPercentage(highlights.returnonequityttm)
      },
      {
        label: 'Profit Margin',
        value: fundamentalsUtils.formatPercentage(highlights.profitmargin)
      }
    ];

    return metrics.filter(metric => hasValue(metric.value));
  }, [highlights, currencyCode]);

  const valuationMetrics = useMemo(() => {
    if (!valuations) return [];

    const metrics = [
      {
        label: 'Trailing P/E',
        value: fundamentalsUtils.formatNumber(valuations.trailingpe)
      },
      {
        label: 'Forward P/E',
        value: fundamentalsUtils.formatNumber(valuations.forwardpe)
      },
      {
        label: 'Price / Sales',
        value: fundamentalsUtils.formatNumber(valuations.pricesalesttm)
      },
      {
        label: 'Price / Book',
        value: fundamentalsUtils.formatNumber(valuations.pricebookmrq)
      },
      {
        label: 'EV / Revenue',
        value: fundamentalsUtils.formatNumber(valuations.enterprisevaluerevenue)
      },
      {
        label: 'EV / EBITDA',
        value: fundamentalsUtils.formatNumber(valuations.enterprisevalueebitda)
      },
      {
        label: 'Enterprise Value',
        value: fundamentalsUtils.formatCurrency(valuations.enterprisevalue, currencyCode)
      }
    ];

    return metrics.filter(metric => hasValue(metric.value));
  }, [valuations, currencyCode]);

  const technicalMetrics = useMemo(() => {
    if (!technicals) return [];

    const metrics = [
      {
        label: '52W High',
        value: fundamentalsUtils.formatCurrency(technicals['52WeekHigh'], currencyCode)
      },
      {
        label: '52W Low',
        value: fundamentalsUtils.formatCurrency(technicals['52WeekLow'], currencyCode)
      },
      {
        label: '50D MA',
        value: fundamentalsUtils.formatCurrency(technicals['50DayMA'], currencyCode)
      },
      {
        label: '200D MA',
        value: fundamentalsUtils.formatCurrency(technicals['200DayMA'], currencyCode)
      },
      {
        label: 'Beta',
        value: fundamentalsUtils.formatNumber(technicals.beta)
      },
      {
        label: 'Short Float',
        value: fundamentalsUtils.formatPercentage(technicals.shortpercent)
      },
      {
        label: 'Short Ratio',
        value: fundamentalsUtils.formatNumber(technicals.shortratio)
      }
    ];

    return metrics.filter(metric => hasValue(metric.value));
  }, [technicals, currencyCode]);

  if (loading) {
    return (
      <div className="mobile-overview-loading">
        <Spin tip="Loading overview..." />
      </div>
    );
  }

  if (error) {
    return (
      <div className="mobile-overview-empty">
        <Empty description={error} image={Empty.PRESENTED_IMAGE_SIMPLE} />
      </div>
    );
  }

  if (!overviewData || !company) {
    return (
      <div className="mobile-overview-empty">
        <Empty
          description="Select a stock to view fundamentals overview"
          image={Empty.PRESENTED_IMAGE_SIMPLE}
        />
      </div>
    );
  }

  const latestRatings = analystRatings.slice(0, 3);

  return (
    <div className="mobile-overview-wrapper">
      <section className="mobile-overview-section">
        <div className="mobile-overview-section-title">
          <InfoCircleOutlined />
          <Title level={5} style={{ margin: 0 }}>
            Company Profile
          </Title>
        </div>

        <div className="mobile-overview-grid">
          {companyFacts.map(item => (
            <div key={item.label} className="mobile-overview-item">
              <Text className="mobile-overview-label">{item.label}</Text>
              <Text className="mobile-overview-value">{item.value}</Text>
            </div>
          ))}
        </div>

        {company.description && (
          <Paragraph className="mobile-overview-description">
            {company.description}
          </Paragraph>
        )}

        <div className="mobile-overview-contact">
          {company.phone && (
            <Text className="mobile-overview-contact-item">Phone: {company.phone}</Text>
          )}
          {company.weburl && (
            <a
              className="mobile-overview-contact-item"
              href={company.weburl}
              target="_blank"
              rel="noopener noreferrer"
            >
              Website: {company.weburl}
            </a>
          )}
          {company.address && (
            <Text className="mobile-overview-contact-item">Address: {company.address}</Text>
          )}
        </div>
      </section>

      {highlightMetrics.length > 0 && (
        <section className="mobile-overview-section">
          <div className="mobile-overview-section-title">
            <BarChartOutlined />
            <Title level={5} style={{ margin: 0 }}>
              Key Metrics
            </Title>
          </div>

          <div className="mobile-overview-stat-grid">
            {highlightMetrics.map(metric => (
              <div key={metric.label} className="mobile-overview-stat-card">
                <Text className="mobile-overview-stat-label">{metric.label}</Text>
                <Text className="mobile-overview-stat-value">{metric.value}</Text>
              </div>
            ))}
          </div>
        </section>
      )}

      {valuationMetrics.length > 0 && (
        <section className="mobile-overview-section">
          <div className="mobile-overview-section-title">
            <FundProjectionScreenOutlined />
            <Title level={5} style={{ margin: 0 }}>
              Valuation
            </Title>
          </div>

          <div className="mobile-overview-stat-grid">
            {valuationMetrics.map(metric => (
              <div key={metric.label} className="mobile-overview-stat-card">
                <Text className="mobile-overview-stat-label">{metric.label}</Text>
                <Text className="mobile-overview-stat-value">{metric.value}</Text>
              </div>
            ))}
          </div>
        </section>
      )}

      {technicalMetrics.length > 0 && (
        <section className="mobile-overview-section">
          <div className="mobile-overview-section-title">
            <LineChartOutlined />
            <Title level={5} style={{ margin: 0 }}>
              Technical Snapshot
            </Title>
          </div>

          <div className="mobile-overview-stat-grid">
            {technicalMetrics.map(metric => (
              <div key={metric.label} className="mobile-overview-stat-card">
                <Text className="mobile-overview-stat-label">{metric.label}</Text>
                <Text className="mobile-overview-stat-value">{metric.value}</Text>
              </div>
            ))}
          </div>
        </section>
      )}

      {latestRatings.length > 0 && (
        <section className="mobile-overview-section">
          <div className="mobile-overview-section-title">
            <StarOutlined />
            <Title level={5} style={{ margin: 0 }}>
              Analyst Ratings
            </Title>
          </div>

          <div className="mobile-overview-ratings">
            {latestRatings.map((rating, index) => {
              const ratingKey = `${rating.analyst_name || 'analyst'}-${index}`;
              const ratingLabel = rating.rating || 'N/A';
              const targetPrice = fundamentalsUtils.formatCurrency(rating.target_price, currencyCode);
              const ratingDate = fundamentalsUtils.formatDate(rating.rating_date);
              return (
                <div key={ratingKey} className="mobile-overview-rating-card">
                  <div className="mobile-overview-rating-header">
                    <div>
                      <Text strong className="mobile-overview-rating-analyst">
                        {rating.analyst_name || rating.brokerage || 'Unknown Analyst'}
                      </Text>
                      {rating.brokerage && (
                        <Text type="secondary" className="mobile-overview-rating-brokerage">
                          {rating.brokerage}
                        </Text>
                      )}
                    </div>
                    <Tag color={fundamentalsUtils.getRatingColor(ratingLabel)}>{ratingLabel}</Tag>
                  </div>
                  <div className="mobile-overview-rating-meta">
                    <span>Target: {targetPrice}</span>
                    <span>Date: {ratingDate}</span>
                  </div>
                  {rating.rating_value !== undefined && rating.rating_value !== null && (
                    <Text type="secondary" className="mobile-overview-hint">
                      Rating score: {rating.rating_value.toFixed(2)}
                    </Text>
                  )}
                </div>
              );
            })}
          </div>
        </section>
      )}
    </div>
  );
};

export default MobileStockOverview;

