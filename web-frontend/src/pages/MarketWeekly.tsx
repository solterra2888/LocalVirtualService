import React, { useState, useEffect } from 'react';
import { Spin, Alert, message } from 'antd';
import TableOfContents from '../components/MarketWeekly/TableOfContents';
import ReportHeader from '../components/MarketWeekly/ReportHeader';
import CombinedWoWChart from '../components/MarketWeekly/CombinedWoWChart';
import WoWBarChart from '../components/MarketWeekly/WoWBarChart';
import RelativeLineChart from '../components/MarketWeekly/RelativeLineChart';
import TechnicalViewsSection from '../components/MarketWeekly/TechnicalViewsSection';
import MyThemesSection from '../components/MarketWeekly/MyThemesSection';
import { EconomicCalendarTable, IPOCalendarTable } from '../components/MarketWeekly/CalendarTables';
import { EarningsCalendarWeek } from '../components/MarketWeekly/EarningsCalendarWeek';
import { marketWeeklyAPI } from '../services/marketWeekly';
import type { MarketWeeklyReport } from '../types/marketWeekly';
import './MarketWeekly.css';

const MarketWeekly: React.FC = () => {
  const [loading, setLoading] = useState<boolean>(true);
  const [refreshing, setRefreshing] = useState<boolean>(false);
  const [error, setError] = useState<string | null>(null);
  const [reportData, setReportData] = useState<MarketWeeklyReport | null>(null);

  // 加载报告数据
  const loadReport = async (useCache: boolean = true) => {
    try {
      setLoading(true);
      setError(null);
      
      // 先尝试从缓存加载（如果启用缓存）
      if (useCache) {
        // 快速显示缓存数据
        const cachedData = await marketWeeklyAPI.getReport('latest', true);
        setReportData(cachedData);
        setLoading(false); // 先显示缓存数据
        
        // 后台刷新（不阻塞UI）
        try {
          const freshData = await marketWeeklyAPI.getReport('latest', false);
          setReportData(freshData);
        } catch (refreshError) {
          console.warn('Background refresh failed, using cached data:', refreshError);
        }
      } else {
        // 强制刷新，不使用缓存
        const data = await marketWeeklyAPI.getReport('latest', false);
        setReportData(data);
        setLoading(false);
      }
    } catch (err: any) {
      setError(err.message || 'Failed to load Market Weekly report');
      console.error('Error loading report:', err);
      setLoading(false);
    }
  };

  // 刷新报告
  const handleRefresh = async () => {
    try {
      setRefreshing(true);
      
      // 清除缓存
      marketWeeklyAPI.clearCache('latest');
      
      // 清除后端缓存
      await marketWeeklyAPI.refreshReport();
      
      // 重新加载（不使用缓存）
      await loadReport(false);
      
      message.success('Report refreshed successfully');
    } catch (err: any) {
      message.error('Failed to refresh report');
      console.error('Error refreshing report:', err);
    } finally {
      setRefreshing(false);
    }
  };

  // 初始加载
  useEffect(() => {
    loadReport();
  }, []);

  if (loading && !reportData) {
    return (
      <div className="market-weekly-loading">
        <Spin size="large" tip="Loading Market Weekly Report..." />
      </div>
    );
  }

  if (error && !reportData) {
    return (
      <div className="market-weekly-error">
        <Alert
          message="Error Loading Report"
          description={error}
          type="error"
          showIcon
        />
      </div>
    );
  }

  if (!reportData) {
    return (
      <div className="market-weekly-error">
        <Alert
          message="No Data Available"
          description="Market Weekly report data is not available at this time."
          type="warning"
          showIcon
        />
      </div>
    );
  }

  return (
    <div className="market-weekly-container">
      {/* 固定侧边栏目录 */}
      <TableOfContents />

      {/* 主内容区域 */}
      <div className="market-weekly-content">
        {/* 报告头部 */}
        <ReportHeader
          meta={reportData.meta}
          onRefresh={handleRefresh}
          refreshing={refreshing}
        />

        {/* 1. Executive Summary */}
        <section id="executive-summary" className="report-section">
          <h2 className="section-title">Executive Summary</h2>
          <div className="section-content">
            <p className="section-intro">
              Key highlights for week ending {new Date(reportData.meta.week_end).toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric' })}
            </p>

            <div className="summary-grid">
              <div className="summary-card">
                <h4>Top Performer</h4>
                <p className="performer-name">{reportData.executive_summary.top_performer.name}</p>
                <p className="performer-change positive">
                  +{reportData.executive_summary.top_performer.change.toFixed(2)}%
                </p>
              </div>

              <div className="summary-card">
                <h4>Bottom Performer</h4>
                <p className="performer-name">{reportData.executive_summary.bottom_performer.name}</p>
                <p className="performer-change negative">
                  {reportData.executive_summary.bottom_performer.change.toFixed(2)}%
                </p>
              </div>
            </div>

            <div className="regional-overview">
              <h4>Regional Overview</h4>
              <ul>
                <li>
                  <strong>US Markets:</strong> {reportData.executive_summary.regional_overview.us_markets}
                </li>
                <li>
                  <strong>European Markets:</strong> {reportData.executive_summary.regional_overview.eu_markets}
                </li>
                <li>
                  <strong>Asian Markets:</strong> {reportData.executive_summary.regional_overview.asia_markets}
                </li>
              </ul>
            </div>
          </div>
        </section>

        {/* 2. Weekly Performance */}
        <section id="weekly-performance" className="report-section">
          <h2 className="section-title">Weekly Performance</h2>

          {/* 2.1 Equity Indices */}
          <div id="equity-weekly" className="subsection">
            <h3 className="subsection-title">Equity Indices</h3>
            
            {/* Combined WoW Chart (2x2 Grid) */}
            <div className="chart-container">
              <CombinedWoWChart
                indicesData={reportData.weekly_performance.indices_wow}
                forexData={reportData.weekly_performance.forex_wow}
                commoditiesData={reportData.weekly_performance.commodities_wow}
                yieldsData={reportData.weekly_performance.yields_wow}
                weekStart={reportData.meta.week_start}
                weekEnd={reportData.meta.week_end}
              />
            </div>
            <p className="chart-caption">
              Week-over-week performance across all asset classes, comparing current week closing prices to previous week.
            </p>

            {/* Equity Indices 3-Month Relative Performance */}
            <div className="chart-container" style={{ marginTop: '2rem' }}>
              <RelativeLineChart
                data={reportData.weekly_performance.indices_relative}
                title="Equity Indices - 3 Month Relative Performance"
                height={400}
                showTitle={false}
              />
            </div>
            <p className="chart-caption">
              3-month indexed performance (base=100) showing cumulative trends across global equity markets.
            </p>
          </div>

          {/* 2.2 US Sectors */}
          <div id="sectors-weekly" className="subsection">
            <h3 className="subsection-title">US Sectors</h3>
            
            {/* US Sectors WoW Chart */}
            <div className="chart-container">
              <WoWBarChart
                data={reportData.weekly_performance.sectors_wow}
                title="US Sectors - Week-over-Week Performance"
                height={Math.max(300, reportData.weekly_performance.sectors_wow.length * 35)}
                showTitle={false}
              />
            </div>
            <p className="chart-caption">
              Week-over-week performance across US sectors via SPDR sector ETFs.
            </p>

            {/* US Sectors 3-Month Relative Performance */}
            <div className="chart-container" style={{ marginTop: '2rem' }}>
              <RelativeLineChart
                data={reportData.weekly_performance.sectors_relative}
                title="US Sectors - 3 Month Trends"
                height={400}
                showTitle={false}
              />
            </div>
            <p className="chart-caption">
              3-month sector rotation trends showing relative strength across industries.
            </p>
          </div>

          {/* 2.3 My Themes */}
          {((reportData.weekly_performance.us_ai_wow && reportData.weekly_performance.us_ai_wow.length > 0) ||
            (reportData.weekly_performance.china_ai_wow && reportData.weekly_performance.china_ai_wow.length > 0)) && (
            <div id="ai-weekly" className="subsection">
              <h3 className="subsection-title">My Themes</h3>
              <p className="section-caption" style={{ fontSize: '11px' }}>
                管理员可以在 Theme Settings 中自定义需要重点关注的 sectors 和 tickers。
              </p>

              {/* Combined US and China AI WoW Chart */}
              <div className="chart-container" style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem' }}>
                {/* US AI WoW */}
                {reportData.weekly_performance.us_ai_wow && reportData.weekly_performance.us_ai_wow.length > 0 && (
                  <div>
                    <h4 style={{ textAlign: 'center', marginBottom: '1rem', fontSize: '14px', fontWeight: 600, color: '#34495E' }}>
                      US
                    </h4>
                    <WoWBarChart
                      data={reportData.weekly_performance.us_ai_wow}
                      title="US Theme - Week-over-Week"
                      height={Math.max(300, reportData.weekly_performance.us_ai_wow.length * 35)}
                      showTitle={false}
                    />
                  </div>
                )}
                
                {/* China AI WoW */}
                {reportData.weekly_performance.china_ai_wow && reportData.weekly_performance.china_ai_wow.length > 0 && (
                  <div>
                    <h4 style={{ textAlign: 'center', marginBottom: '1rem', fontSize: '14px', fontWeight: 600, color: '#34495E' }}>
                      China
                    </h4>
                    <WoWBarChart
                      data={reportData.weekly_performance.china_ai_wow}
                      title="China Theme - Week-over-Week"
                      height={Math.max(300, reportData.weekly_performance.china_ai_wow.length * 35)}
                      showTitle={false}
                    />
                  </div>
                )}
              </div>
              <p className="chart-caption">
                Week-over-week performance of your selected themes by market. Each theme aggregates its constituent stocks from the database.
              </p>

              {/* US AI 3-Month Relative Performance */}
              {reportData.weekly_performance.us_ai_relative && reportData.weekly_performance.us_ai_relative.length > 0 && (
                <>
                  <div className="chart-container" style={{ marginTop: '2rem' }}>
                    <RelativeLineChart
                      data={reportData.weekly_performance.us_ai_relative}
                      title="US Theme - 3 Month Trends"
                      height={400}
                      showTitle={true}
                    />
                  </div>
                  <p className="chart-caption">
                    3-month US Theme performance trends (indexed to 100).
                  </p>
                </>
              )}

              {/* China AI 3-Month Relative Performance */}
              {reportData.weekly_performance.china_ai_relative && reportData.weekly_performance.china_ai_relative.length > 0 && (
                <>
                  <div className="chart-container" style={{ marginTop: '2rem' }}>
                    <RelativeLineChart
                      data={reportData.weekly_performance.china_ai_relative}
                      title="China Theme - 3 Month Trends"
                      height={400}
                      showTitle={true}
                    />
                  </div>
                  <p className="chart-caption">
                    3-month China AI-related theme performance trends (indexed to 100).
                  </p>
                </>
              )}
            </div>
          )}

          {/* 2.4 Foreign Exchange */}
          <div id="forex-weekly" className="subsection">
            <h3 className="subsection-title">Foreign Exchange</h3>
            <div className="chart-container">
              <RelativeLineChart
                data={reportData.weekly_performance.forex_relative}
                title="Major Currency Pairs - Relative Performance"
                height={400}
                showTitle={false}
              />
            </div>
            <p className="chart-caption">
              Major currency pairs indexed to 100 at period start (90 days), reflecting relative strength movements.
            </p>
          </div>

          {/* 2.4 Commodities */}
          <div id="commodities-weekly" className="subsection">
            <h3 className="subsection-title">Commodities</h3>
            <div className="chart-container">
              <RelativeLineChart
                data={reportData.weekly_performance.commodities_relative}
                title="Commodity Price Trends"
                height={400}
                showTitle={false}
              />
            </div>
            <p className="chart-caption">
              Key commodity price movements (90 days) reflecting supply-demand dynamics and macro trends.
            </p>
          </div>

          {/* 2.5 US Treasury Yields */}
          <div id="yields-weekly" className="subsection">
            <h3 className="subsection-title">US Treasury Yields</h3>
            <div className="chart-container">
              <RelativeLineChart
                data={reportData.weekly_performance.yields_data}
                title="US Treasury Yields"
                height={400}
                showTitle={false}
              />
            </div>
            <p className="chart-caption">
              Yield curve movements across key maturities (90 days), signaling market rate expectations.
            </p>
          </div>
        </section>

        {/* 3. Technical Views */}
        <section id="technical-views" className="report-section">
          <h2 className="section-title">Technical Views</h2>

          <TechnicalViewsSection
            assets={reportData.technical_views.indices}
            title="Equity Indices"
            sectionId="equity-technical"
            groupByRegion={true}
          />

          <TechnicalViewsSection
            assets={reportData.technical_views.sectors}
            title="US Sectors"
            sectionId="sectors-technical"
            groupByRegion={false}
          />

          {/* My Themes Stocks Section */}
          {reportData.my_themes_stocks && (
            <MyThemesSection
              myThemesStocks={reportData.my_themes_stocks}
              sectionId="my-themes-technical"
            />
          )}

          <TechnicalViewsSection
            assets={reportData.technical_views.forex}
            title="Foreign Exchange"
            sectionId="forex-technical"
            groupByRegion={false}
          />

          <TechnicalViewsSection
            assets={reportData.technical_views.commodities}
            title="Commodities"
            sectionId="commodities-technical"
            groupByRegion={false}
          />

          <TechnicalViewsSection
            assets={reportData.technical_views.yields}
            title="US Treasury Yields"
            sectionId="yields-technical"
            groupByRegion={false}
          />
        </section>

        {/* 4. Next Week Outlook */}
        <section id="next-week-outlook" className="report-section">
          <h2 className="section-title">Next Week Outlook</h2>

          <div id="economic-calendar" className="subsection">
            <h3 className="subsection-title">Economic Calendar</h3>
            <p className="section-intro">
              High/medium-impact US economic events scheduled for next week
            </p>
            <EconomicCalendarTable data={reportData.next_week_outlook.economic_calendar} />
          </div>

          <div id="earnings-calendar" className="subsection">
            <h3 className="subsection-title">Earnings Calendar</h3>
            <p className="section-intro">
              Only shows earnings for tickers configured in your Watchlist and Theme Settings
            </p>
            <EarningsCalendarWeek data={reportData.next_week_outlook.earnings_calendar} />
          </div>

          <div id="ipo-calendar" className="subsection">
            <h3 className="subsection-title">IPO Calendar</h3>
            <p className="section-intro">
              Upcoming initial public offerings on US and Hong Kong exchanges
            </p>
            <IPOCalendarTable data={reportData.next_week_outlook.ipo_calendar} />
          </div>
        </section>

        {/* Footer */}
        <footer className="report-footer">
          <hr />
          <p>
            <em>This report is for informational purposes only and does not constitute investment advice.</em>
          </p>
        </footer>
      </div>
    </div>
  );
};

export default MarketWeekly;

