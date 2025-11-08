/**
 * Market Weekly Report Types
 */

// 报告元数据
export interface ReportMeta {
  week_start: string;      // "2025-10-30"
  week_end: string;        // "2025-11-06"
  generated_at: string;    // "2025-11-06T10:00:00Z"
}

// 执行摘要
export interface ExecutiveSummary {
  top_performer: {
    name: string;
    change: number;
  };
  bottom_performer: {
    name: string;
    change: number;
  };
  regional_overview: {
    us_markets: string;
    eu_markets: string;
    asia_markets: string;
  };
}

// Week-over-Week 数据点
export interface WoWChange {
  name: string;
  change_pct: number;
}

// 保持向后兼容
export type WoWDataPoint = WoWChange;

// 相对表现数据点
export interface RelativePerformancePoint {
  date: string;
  [key: string]: number | string;  // 动态字段：各个资产的indexed值
}

// 周度表现数据
export interface WeeklyPerformance {
  indices_wow: WoWChange[];
  indices_relative: RelativePerformancePoint[];
  sectors_wow: WoWChange[];
  sectors_relative: RelativePerformancePoint[];
  forex_wow: WoWChange[];
  forex_relative: RelativePerformancePoint[];
  commodities_wow: WoWChange[];
  commodities_relative: RelativePerformancePoint[];
  yields_wow: WoWChange[];
  yields_data: RelativePerformancePoint[];
}

// 技术视图 - 资产信息
export interface AssetInfo {
  symbol: string;
  name: string;
  region?: string;  // 用于equity indices分组
  primary_ticker?: string;
}

// 技术视图数据
export interface TechnicalViews {
  indices: AssetInfo[];
  sectors: AssetInfo[];
  forex: AssetInfo[];
  commodities: AssetInfo[];
  yields: AssetInfo[];
}

// 经济日历事件
export interface EconomicEvent {
  date: string;
  time: string;
  event: string;
  country: string;
  actual: string;
  forecast: string;
  previous: string;
  impact?: number;
}

// 财报日历事件
export interface EarningsEvent {
  date: string;
  time: string;  // "Before Open" | "After Close" | "During Hours"
  ticker: string;
  company: string;
}

// IPO日历事件
export interface IPOEvent {
  date: string;
  ticker: string;
  company: string;
  exchange: string;
  deal_type?: string;
  price_range?: string;
  shares?: string;
}

// 下周展望
export interface NextWeekOutlook {
  economic_calendar: EconomicEvent[];
  earnings_calendar: EarningsEvent[];
  ipo_calendar: IPOEvent[];
}

// 完整报告数据
export interface MarketWeeklyReport {
  meta: ReportMeta;
  executive_summary: ExecutiveSummary;
  weekly_performance: WeeklyPerformance;
  technical_views: TechnicalViews;
  next_week_outlook: NextWeekOutlook;
}

// API响应格式
export interface MarketWeeklyResponse {
  success: boolean;
  data: MarketWeeklyReport;
  message?: string;
}

