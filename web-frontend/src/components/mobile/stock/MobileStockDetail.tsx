import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import { Button, Tabs, Typography, Space, Spin, Empty, Input, Tooltip, message } from 'antd';
import { ArrowLeftOutlined, ArrowUpOutlined, ArrowDownOutlined, SearchOutlined, QuestionCircleOutlined } from '@ant-design/icons';
import type { WatchlistItem, StockPriceData } from '../../../services/dashboard';
import { listDailyFeed, type DailyFeedItem } from '../../../services/daily';
import { dateKeyInTz, formatDateLabel } from '../../../utils/datetime';
import { expandTickerForDaily } from '../../../utils/tickerFormat';
import { useDashboardSettings } from '../../../contexts/DashboardSettingsContext';
import DailyCard from '../../daily/DailyCard';
import DailyPreviewDrawer from '../../daily/DailyPreviewDrawer';
import { DAILY_THEME_COLORS } from '../../daily/dailyTheme';
import MobileStockChart from './MobileStockChart';
import MobileStockOverview from './MobileStockOverview';
import './MobileStockDetail.css';

const { Title, Text } = Typography;

const NEWS_PAGE_SIZE = 20;
const SEARCH_DEBOUNCE_MS = 400;

interface NewsDateGroup {
  dateKey: string;
  dateLabel: string;
  items: DailyFeedItem[];
}

// 按用户时区把 Daily feed 按自然日分组 (复刻 FinvizDashboard.groupNewsByDate)
const groupNewsByDate = (items: DailyFeedItem[], tz: string): NewsDateGroup[] => {
  const groups: NewsDateGroup[] = [];
  const map = new Map<string, NewsDateGroup>();
  for (const it of items) {
    const key = dateKeyInTz(it.published_at, tz);
    let g = map.get(key);
    if (!g) {
      g = { dateKey: key, dateLabel: formatDateLabel(it.published_at, tz), items: [] };
      map.set(key, g);
      groups.push(g);
    }
    g.items.push(it);
  }
  return groups;
};

interface StockWithPrice extends WatchlistItem {
  priceData?: StockPriceData;
}

interface MobileStockDetailProps {
  stock: StockWithPrice;
  onBack: () => void;
}

/**
 * 移动端股票详情页组件
 * News tab 展示该股票的 Daily feed (与电脑版 Watchlist > News 一致)
 */
const MobileStockDetail: React.FC<MobileStockDetailProps> = ({ stock, onBack }) => {
  const { themeMode, userTimezone } = useDashboardSettings();
  const colors = DAILY_THEME_COLORS[themeMode];

  const [activeTab, setActiveTab] = useState<string>('news');
  const [newsData, setNewsData] = useState<DailyFeedItem[]>([]);
  const [contentLoading, setContentLoading] = useState<boolean>(false);
  const [loadingMore, setLoadingMore] = useState<boolean>(false);
  const [hasMore, setHasMore] = useState<boolean>(true);
  const [newsOffset, setNewsOffset] = useState<number>(0);
  const [searchInput, setSearchInput] = useState<string>('');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [previewItem, setPreviewItem] = useState<DailyFeedItem | null>(null);

  // 竞态保护: 切股/切 tab 时丢弃过期响应
  const requestSeqRef = useRef<number>(0);
  // 缓存当前股票展开后的 ticker 变体, 供"加载更多"复用
  const currentTickersRef = useRef<string[]>([]);
  // 搜索词同步给 loadStockNews 读取, 避免闭包陈旧
  const searchQueryRef = useRef<string>('');
  // 无限滚动
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  const loadMoreRef = useRef<() => void>(() => {});

  const loadStockNews = useCallback(async (tickers: string[], reset: boolean = true) => {
    const seq = ++requestSeqRef.current;
    try {
      const offset = reset ? 0 : newsOffset;
      const resp = await listDailyFeed({
        tickers,
        q: searchQueryRef.current || undefined,
        limit: NEWS_PAGE_SIZE,
        offset,
      });
      if (seq !== requestSeqRef.current) return; // 过期响应丢弃
      if (reset) {
        setNewsData(resp.items);
        setNewsOffset(resp.items.length);
      } else {
        setNewsData((prev) => [...prev, ...resp.items]);
        setNewsOffset((prev) => prev + resp.items.length);
      }
      setHasMore(resp.has_more);
    } catch (error) {
      if (reset && seq === requestSeqRef.current) setNewsData([]);
      console.error('Failed to load daily feed:', error);
    }
  }, [newsOffset]);

  // 切股 / 搜索后的首屏(重置)加载入口
  const loadRelatedNews = useCallback(async () => {
    setNewsOffset(0);
    setHasMore(true);
    setContentLoading(true);
    try {
      const tickers = expandTickerForDaily(stock.symbol, stock.exchange);
      currentTickersRef.current = tickers;
      await loadStockNews(tickers, true);
    } catch (error) {
      console.error('Failed to load related news:', error);
      message.error('Failed to load news');
    } finally {
      setContentLoading(false);
    }
  }, [stock.symbol, stock.exchange, loadStockNews]);

  const loadMoreNews = useCallback(async () => {
    if (!hasMore || loadingMore || contentLoading || activeTab !== 'news') return;
    setLoadingMore(true);
    try {
      await loadStockNews(currentTickersRef.current, false);
    } catch (error) {
      console.error('Failed to load more news:', error);
    } finally {
      setLoadingMore(false);
    }
  }, [hasMore, loadingMore, contentLoading, activeTab, loadStockNews]);

  // loadRelatedNews 依赖 loadStockNews→newsOffset, 而每次加载都改 newsOffset 使其重建.
  // 用 ref 始终拿最新值, 不放进下方 effect 依赖, 否则: 首屏加载→改 offset→重建→重跑 effect→死循环.
  const loadRelatedNewsRef = useRef(loadRelatedNews);
  useEffect(() => {
    loadRelatedNewsRef.current = loadRelatedNews;
  });

  // 搜索防抖: 输入 → 400ms 后写入 searchQuery 触发重载
  useEffect(() => {
    const t = window.setTimeout(() => {
      const q = searchInput.trim();
      searchQueryRef.current = q;
      setSearchQuery(q);
    }, SEARCH_DEBOUNCE_MS);
    return () => window.clearTimeout(t);
  }, [searchInput]);

  // 进入 news tab / 切股 / 搜索 → 首屏(重置)加载; 离开 news tab 不加载
  useEffect(() => {
    if (activeTab !== 'news') return;
    const timer = window.setTimeout(() => {
      loadRelatedNewsRef.current();
    }, 300);
    return () => window.clearTimeout(timer);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, stock.symbol, stock.exchange, searchQuery]);

  // 无限滚动: 始终拿最新的 loadMoreNews
  useEffect(() => {
    loadMoreRef.current = loadMoreNews;
  });

  // sentinel 进入视口 → 加载下一页
  useEffect(() => {
    const el = sentinelRef.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting) loadMoreRef.current();
      },
      { rootMargin: '400px', threshold: 0 }
    );
    observer.observe(el);
    return () => observer.disconnect();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [newsData.length > 0]);

  // iOS Safari 兼容的 document 滚动兜底
  useEffect(() => {
    const handleScroll = () => {
      const sentinel = sentinelRef.current;
      if (!sentinel) return;
      const rect = sentinel.getBoundingClientRect();
      if (rect.top <= window.innerHeight + 400) {
        loadMoreRef.current();
      }
    };
    document.addEventListener('scroll', handleScroll, { capture: true, passive: true });
    return () => document.removeEventListener('scroll', handleScroll, true);
  }, []);

  const grouped = useMemo(() => groupNewsByDate(newsData, userTimezone), [newsData, userTimezone]);

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
        <div className="mobile-tab-content mobile-news-content">
          {/* 搜索框 */}
          <div style={{ padding: '8px 0 12px' }}>
            <Input
              allowClear
              prefix={<SearchOutlined style={{ color: colors.textSecondary, fontSize: 14 }} />}
              suffix={
                <Tooltip
                  title={
                    <div style={{ fontSize: 12, lineHeight: 1.7 }}>
                      <div style={{ fontWeight: 600, marginBottom: 4 }}>搜索语法</div>
                      <div><span style={{ opacity: 0.6 }}>空格</span> → AND，每个词都要出现</div>
                      <div><span style={{ opacity: 0.6 }}>OR（大写）</span> → 任一词命中即可</div>
                      <div><span style={{ opacity: 0.6 }}>「短语」</span> → 精确子串匹配</div>
                    </div>
                  }
                  placement="bottomRight"
                >
                  <QuestionCircleOutlined style={{ color: colors.textSecondary, cursor: 'pointer', fontSize: 14 }} />
                </Tooltip>
              }
              placeholder={`Search within ${stock.symbol} (space=AND, OR=any, "phrase")`}
              value={searchInput}
              onChange={(e) => setSearchInput(e.target.value)}
              style={{
                background: colors.inputBg,
                borderColor: colors.cardBorder,
                color: colors.text,
                fontSize: 13,
              }}
            />
          </div>

          {contentLoading ? (
            <div className="mobile-loading">
              <Spin />
            </div>
          ) : newsData.length === 0 ? (
            <Empty
              description={searchQuery
                ? `No results for "${searchQuery}" within ${stock.symbol}`
                : `No items for ${stock.symbol}`}
              image={Empty.PRESENTED_IMAGE_SIMPLE}
              style={{ marginTop: 40 }}
            />
          ) : (
            <>
              {grouped.map((g) => (
                <div key={g.dateKey} style={{ marginBottom: 16 }}>
                  <div
                    style={{
                      display: 'flex',
                      alignItems: 'baseline',
                      gap: 8,
                      padding: '8px 4px',
                      marginBottom: 8,
                      borderBottom: `1px solid ${colors.divider}`,
                    }}
                  >
                    <Text strong style={{ color: colors.dateAnchorText, fontSize: 13 }}>
                      {g.dateLabel}
                    </Text>
                    <Text style={{ color: colors.textMuted, fontSize: 11 }}>
                      {g.items.length} {g.items.length === 1 ? 'article' : 'articles'}
                    </Text>
                  </div>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                    {g.items.map((it) => (
                      <DailyCard
                        key={it.id}
                        item={it}
                        themeMode={themeMode}
                        onClick={(item) => setPreviewItem(item)}
                      />
                    ))}
                  </div>
                </div>
              ))}
              <div ref={sentinelRef} style={{ height: 24 }} />
              {loadingMore && (
                <div style={{ textAlign: 'center', padding: 16 }}>
                  <Spin />
                </div>
              )}
              {!hasMore && newsData.length > 0 && (
                <div style={{ textAlign: 'center', color: colors.textMuted, fontSize: 11, padding: '20px 0' }}>
                  — End of feed —
                </div>
              )}
            </>
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

      {/* 卡片点击预览抽屉 (底部弹层) */}
      <DailyPreviewDrawer
        item={previewItem}
        items={newsData}
        themeMode={themeMode}
        placement="bottom"
        onClose={() => setPreviewItem(null)}
        onNavigate={(next) => setPreviewItem(next)}
      />
    </div>
  );
};

export default MobileStockDetail;
