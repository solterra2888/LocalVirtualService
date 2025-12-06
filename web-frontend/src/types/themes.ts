import { Dayjs } from 'dayjs';

export interface DimensionValue {
  value: string;
  display_name: string;
  description?: string;
  news_count: number;
  avg_confidence?: number;
}

/**
 * 维度操作符类型
 */
export type DimensionOperator = 'OR' | 'AND';

/**
 * 各维度操作符配置（用于 ThemeFilterState）
 */
export interface ThemeDimensionOperators {
  newsValue?: DimensionOperator;
  categories?: DimensionOperator;
  eventTypes?: DimensionOperator;
  impactScopes?: DimensionOperator;
  urgencyLevels?: DimensionOperator;
  relatedSectors?: DimensionOperator;
  relatedCountries?: DimensionOperator;
  sourceTypes?: DimensionOperator;
  schedulerSources?: DimensionOperator;
}

export interface ThemeFilterState {
  newsValue?: string[];
  categories: string[];
  eventTypes: string[];
  impactScopes: string[];
  urgencyLevels: string[];
  relatedSectors: string[];
  relatedCountries: string[];
  sourceTypes: string[];
  schedulerSources: string[];
  minConfidence: number;
  onlyPrimary: boolean;
  dateRange: [Dayjs, Dayjs] | null;
  market: string | null;
  // 🔥 新增：各维度内部操作符
  dimensionOperators?: ThemeDimensionOperators;
  // 🔥 新增：Ticker筛选
  tickerSymbols?: string[];
  useWatchlist?: boolean;
  watchlistGroups?: string[];
  tickerFilterMode?: 'include' | 'exclude';
  // 🔥 新增：搜索功能
  searchText?: string;
}

/**
 * 各维度操作符配置（用于 API 请求）
 */
export interface DimensionOperatorsRequest {
  news_value?: DimensionOperator;
  categories?: DimensionOperator;
  event_types?: DimensionOperator;
  impact_scopes?: DimensionOperator;
  urgency?: DimensionOperator;
  related_sectors?: DimensionOperator;
  related_countries?: DimensionOperator;
  source_types?: DimensionOperator;
  scheduler_sources?: DimensionOperator;
}

export interface ThemeFilterRequest {
  news_value?: string[];
  categories?: string[];
  event_types?: string[];
  impact_scopes?: string[];
  urgency_levels?: string[];
  related_sectors?: string[];
  related_countries?: string[];
  source_types?: string[];
  scheduler_sources?: string[];
  // 🔥 新增：各维度内部操作符
  dimension_operators?: DimensionOperatorsRequest;
  // 🔥 新增：Ticker筛选
  ticker_symbols?: string[];
  use_watchlist?: boolean;
  watchlist_groups?: string[];
  ticker_filter_mode?: 'include' | 'exclude';
  // 🔥 新增：搜索功能
  search_text?: string;
  min_confidence?: number;
  only_primary?: boolean;
  start_date?: string;
  end_date?: string;
  market?: string;
  page?: number;
  page_size?: number;
}

export interface NewsTheme {
  theme_name: string;
  source_type: string;
  dimension?: string;
  confidence?: number;
  is_primary: boolean;
  display_name?: string;
  category?: string;
}

export interface FilteredNews {
  id: number;
  title: string;
  content?: string;
  date: string;
  updated_at?: string;
  created_at?: string;
  source: string;
  link?: string;
  analysis_result?: any;
  analysis_status?: string;
  sentiment_polarity?: number;
  sentiment_neg?: number;
  sentiment_neu?: number;
  sentiment_pos?: number;
  symbols?: string[];
  themes: NewsTheme[];
}

export interface NewsFilterResponse {
  success: boolean;
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
  news: FilteredNews[];
  filter_summary: {
    news_value?: string[];
    categories: string[];
    event_types: string[];
    impact_scopes: string[];
    urgency_levels: string[];
    related_sectors: string[];
    related_countries: string[];
    source_types: string[];
    scheduler_sources: string[];  // 🔥 新增
    min_confidence: number;
    only_primary: boolean;
    date_range: {
      start?: string;
      end?: string;
    };
  };
}

export interface TrendingTheme {
  theme_name: string;
  display_name?: string;
  news_count: number;
  avg_confidence: number;
  trend: '↑' | '↓' | '→';
  daily_distribution: Array<{
    date: string;
    count: number;
  }>;
}

export interface TrendingThemesResponse {
  success: boolean;
  dimension: string;
  period_days: number;
  themes: TrendingTheme[];
  total_themes: number;
}

export interface DimensionValuesResponse {
  success: boolean;
  dimension: string;
  values: DimensionValue[];
  total: number;
}


