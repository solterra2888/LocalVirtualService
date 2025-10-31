/**
 * Unified Theme System Types
 * 统一主题系统类型定义
 */

export interface UnifiedTheme {
  news_id: number;
  theme_name: string;
  source_type: 'eodhd_auto_tag' | 'vector_retrieval' | 'llm_classification';
  source_id: string;
  source_table: string;
  confidence_score: number;
  rank_in_news: number;
  market: string;
  metadata: Record<string, any>;
  quality_score?: number;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export interface FusedTheme {
  theme_name: string;
  fusion_score: number;
  sources: Array<{
    source_type: string;
    confidence_score: number;
    rank_in_news: number;
    market: string;
  }>;
  markets: string[];
  avg_confidence: number;
  source_count: number;
  avg_rank: number;
}

export interface TrendingTheme {
  theme_name: string;
  news_count: number;
  avg_confidence: number;
  markets: string[];
  first_seen: string;
  last_seen: string;
  is_hot: boolean;
  growth_rate?: number;
}

export interface TrendingThemesResponse {
  items: TrendingTheme[];
  total: number;
  period_days: number;
  generated_at: string;
}

export interface ThemeMasterData {
  id: number;
  canonical_name: string;
  display_name: string;
  category?: string;
  description?: string;
  aliases: string[];
  is_active: boolean;
  metadata: Record<string, any>;
  created_at: string;
  updated_at: string;
}

export interface NewsWithThemes {
  id: number;
  title: string;
  content: string;
  source: string;
  date: string;
  link: string;
  sentiment_polarity?: number;
  symbols: string[];
  tags: string[];
  auto_tags: string[];
  themes: FusedTheme[];
  analysis_result?: any;
}

