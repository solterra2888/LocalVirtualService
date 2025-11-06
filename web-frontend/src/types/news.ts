// 新闻相关的类型定义

// 单个模板的分析结果
export interface SingleTemplateAnalysisResult {
  content: any;  // 可以是string或object（如classification的结构化数据）
  template_code: string;
  provider?: string;
  model?: string;
  tokens?: number;
  temperature?: number;
  retry_count?: number;
  analyzed_at?: string;
}

// 多模板分析结果（新格式）
export interface MultiTemplateAnalysisResult {
  [templateCode: string]: SingleTemplateAnalysisResult;
}

// 兼容旧格式和新格式的分析结果类型
export type NewsAnalysisResult = SingleTemplateAnalysisResult | MultiTemplateAnalysisResult | any;

// 投资主题匹配结果（从investmentThemes.ts导入）
export interface NewsThemeMatch {
  theme_id: string;
  theme_name: string;
  similarity: number;
  confidence: number;
  rank: number;
}

// 统一主题数据（从news_themes_unified表）
export interface NewsUnifiedTheme {
  id: number;
  theme_name: string;
  canonical_theme_name: string;
  source_type: 'eodhd_auto_tag' | 'vector_retrieval' | 'llm_classification' | 'manual' | 'hybrid';
  confidence_score: number;
  quality_score?: number;
  rank_in_news?: number;
  market?: string;
  is_verified: boolean;
  is_primary: boolean;
  metadata?: any;
  display_name?: string;
  category?: string;
  sub_category?: string;
  description?: string;
}

export interface NewsItem {
  id: number;
  date?: string;
  title: string;
  content?: string;
  link?: string;
  source?: string;
  sentiment_polarity?: number;
  sentiment_neg?: number;
  sentiment_neu?: number;
  sentiment_pos?: number;
  created_at?: string;
  updated_at?: string;
  scheduler_source?: string;  // 调度器来源
  symbols: string[];
  tags: string[];          // 用户自定义的topic tags
  auto_tags: string[];     // API原始tags
  analysis_result?: NewsAnalysisResult;  // AI分析结果
  theme_matches?: NewsThemeMatch[];  // 投资主题匹配结果（旧）
  unified_themes?: NewsUnifiedTheme[];  // 统一主题数据（新）
}

export interface NewsListResponse {
  news: NewsItem[];
  total: number;
  page: number;
  page_size: number;
  total_pages: number;
}

export interface NewsTopic {
  id: number;
  topic_name: string;
  description?: string;
  is_active: boolean;
  color?: string;
  created_at?: string;
  updated_at?: string;
}

export interface NewsSymbol {
  symbol_code: string;
  news_count: number;
}

export interface NewsTag {
  tag_name: string;
  news_count: number;
}

export interface NewsSearchParams {
  page?: number;
  page_size?: number;
  topic_id?: number;
  symbol_code?: string;
  start_date?: string;
  end_date?: string;
  search_text?: string;
  unified_theme_name?: string;  // 统一主题名称筛选
}

export interface NewsTopicKeyword {
  id: number;
  topic_id: number;
  keyword: string;
  weight: number;
  created_at?: string;
}

// 情感分析相关
export interface SentimentScore {
  polarity?: number;
  negative?: number;
  neutral?: number;
  positive?: number;
}

// 新闻过滤器状态
export interface NewsFilterState {
  selectedTopic?: number;
  selectedSymbol?: string;
  dateRange?: {
    start?: string;
    end?: string;
  };
  searchText?: string;
}

// EODHD新闻获取相关类型
export interface FetchNewsParams {
  symbols?: string;
  topic?: string;
  start_date?: string;
  end_date?: string;
  max_results?: number;
  topic_filter?: string;
  keywords?: string;
}

export interface FetchNewsResponse {
  success: boolean;
  message: string;
  data: {
    success_count: number;
    existing_count: number;
    failed_count: number;
    total_count: number;
    fetched_count: number;
  };
}

// 主题管理相关类型
export interface NewsTheme {
  id: number;
  topic_name: string;
  description?: string;
  is_active: boolean;
  color?: string;
  created_at?: string;
  keywords: string[];
}

export interface CreateThemeParams {
  topic_name: string;
  description?: string;
  keywords?: string;
  color?: string;
}

export interface UpdateThemeParams {
  topic_name?: string;
  description?: string;
  is_active?: boolean;
  color?: string;
}

export interface ThemeKeywordParams {
  keywords: string;
  weights?: string;
}

// API测试相关类型
export interface EODHDTestResponse {
  success: boolean;
  message: string;
  data: {
    api_available: boolean;
  };
} 