import { Dayjs } from 'dayjs';

export interface DimensionValue {
  value: string;
  display_name: string;
  description?: string;
  news_count: number;
  avg_confidence?: number;
}

export interface ThemeFilterState {
  categories: string[];
  eventTypes: string[];
  impactScopes: string[];
  urgencyLevels: string[];
  relatedSectors: string[];
  relatedCountries: string[];
  sourceTypes: string[];
  schedulerSources: string[];  // 🔥 新增：新闻来源筛选
  minConfidence: number;
  onlyPrimary: boolean;
  dateRange: [Dayjs, Dayjs] | null;
  market: string | null;
}

export interface ThemeFilterRequest {
  categories?: string[];
  event_types?: string[];
  impact_scopes?: string[];
  urgency_levels?: string[];
  related_sectors?: string[];
  related_countries?: string[];
  source_types?: string[];
  scheduler_sources?: string[];  // 🔥 新增：新闻来源筛选
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


