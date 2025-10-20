/**
 * 投资主题向量检索相关类型定义
 */

/**
 * 投资主题
 */
export interface InvestmentTheme {
  theme_id: string;
  theme_name: string;
  market: 'HK' | 'US' | 'CN';
  embedding_model?: string;
  created_at?: string;
  updated_at?: string;
}

/**
 * 主题匹配结果
 */
export interface ThemeMatch {
  theme_id: string;
  theme_name: string;
  market: 'HK' | 'US' | 'CN';
  similarity: number;
  confidence: number;
  rank: number;
  match_method?: string;
  matched_at?: string;
}

/**
 * 主题匹配请求
 */
export interface ThemeMatchRequest {
  news_text: string;
  news_id?: number;
  market?: 'HK' | 'US' | 'CN';
  top_k?: number;
  min_similarity?: number;
  save_to_db?: boolean;
}

/**
 * 主题统计信息
 */
export interface ThemeStatistics {
  total_themes: number;
  market_breakdown: MarketBreakdown[];
  match_statistics: MatchStatistics;
  embedding_model: string;
  embedding_dimension: number;
}

/**
 * 市场统计
 */
export interface MarketBreakdown {
  market: string;
  count: number;
  last_updated?: string;
}

/**
 * 匹配统计
 */
export interface MatchStatistics {
  news_with_themes: number;
  total_matches: number;
  avg_similarity: number;
}

/**
 * 初始化请求
 */
export interface InitializeThemesRequest {
  force_update: boolean;
}

/**
 * 初始化响应
 */
export interface InitializeThemesResponse {
  success: boolean;
  message: string;
  statistics: {
    total: number;
    processed: number;
    failed: number;
    skipped: number;
  };
}

/**
 * 新闻主题响应
 */
export interface NewsThemesResponse {
  success: boolean;
  news_id: number;
  themes: ThemeMatch[];
  count: number;
}

/**
 * 主题列表响应
 */
export interface ThemeListResponse {
  success: boolean;
  themes: InvestmentTheme[];
  total: number;
  limit: number;
  offset: number;
}

/**
 * 市场颜色映射
 */
export const MARKET_COLORS: Record<string, string> = {
  HK: 'green',
  US: 'blue',
  CN: 'red',
};

/**
 * 市场标签映射
 */
export const MARKET_LABELS: Record<string, string> = {
  HK: '香港',
  US: '美国',
  CN: '中国',
};

/**
 * 相似度等级
 */
export enum SimilarityLevel {
  VERY_HIGH = 'very_high',  // >= 0.9
  HIGH = 'high',            // >= 0.8
  MEDIUM = 'medium',        // >= 0.7
  LOW = 'low',              // >= 0.6
  VERY_LOW = 'very_low',    // < 0.6
}

/**
 * 获取相似度等级
 */
export function getSimilarityLevel(similarity: number): SimilarityLevel {
  if (similarity >= 0.9) return SimilarityLevel.VERY_HIGH;
  if (similarity >= 0.8) return SimilarityLevel.HIGH;
  if (similarity >= 0.7) return SimilarityLevel.MEDIUM;
  if (similarity >= 0.6) return SimilarityLevel.LOW;
  return SimilarityLevel.VERY_LOW;
}

/**
 * 获取相似度颜色
 */
export function getSimilarityColor(similarity: number): string {
  const level = getSimilarityLevel(similarity);
  switch (level) {
    case SimilarityLevel.VERY_HIGH:
      return 'success';
    case SimilarityLevel.HIGH:
      return 'processing';
    case SimilarityLevel.MEDIUM:
      return 'warning';
    case SimilarityLevel.LOW:
      return 'default';
    case SimilarityLevel.VERY_LOW:
      return 'error';
    default:
      return 'default';
  }
}

/**
 * 格式化相似度显示
 */
export function formatSimilarity(similarity: number): string {
  return `${(similarity * 100).toFixed(0)}%`;
}

