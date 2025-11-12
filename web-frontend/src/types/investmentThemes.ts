/**
 * Investment Theme Vector Retrieval Type Definitions
 */

/**
 * Investment Theme
 */
export interface InvestmentTheme {
  theme_id: string;
  theme_name: string;
  category?: string;  // Theme category/classification
  description_preview?: string;  // Theme description preview (LLM-generated or manually written)
  description_source?: 'llm_generated' | 'manual' | 'scraped' | 'simple_template';  // Description source
  embedding_model?: string;
  created_at?: string;
  updated_at?: string;
  
  // Theme management fields
  version?: number;  // Version number for optimistic locking
  last_manual_edit_at?: string;  // Last manual edit time
  last_manual_edit_by?: string;  // Last editor username
  is_enabled_for_matching?: boolean;  // Whether enabled for news matching
  
  // Vector embedding status
  has_embedding?: boolean;  // Whether this theme has been vectorized
  
  // Associated tickers
  tickers?: string[];  // Linked stock tickers (full symbols)
  verified_tickers?: number;
  total_tickers?: number;
  created_by?: string;
}

/**
 * Theme-Company Association
 */
export interface ThemeAssociation {
  ticker: string;
  market?: string | null;
  is_verified?: boolean | null;
  association_weight?: number | null;
}

/**
 * Theme Associations Response
 */
export interface ThemeAssociationsResponse {
  success: boolean;
  theme_id: string;
  associations: ThemeAssociation[];
  count: number;
}

/**
 * Theme Match Result
 */
export interface ThemeMatch {
  theme_id: string;
  theme_name: string;
  similarity: number;
  confidence: number;
  rank: number;
  match_method?: string;
  matched_at?: string;
}

/**
 * Theme Match Request
 */
export interface ThemeMatchRequest {
  news_text: string;
  news_id?: number;
  top_k?: number;
  min_similarity?: number;
  save_to_db?: boolean;
}


/**
 * Initialize Request
 */
export interface InitializeThemesRequest {
  force_update: boolean;
}

/**
 * Initialize Response (Celery Task)
 */
export interface InitializeThemesResponse {
  success: boolean;
  message: string;
  task_id: string;
  status_url: string;
}

/**
 * Task Status Response
 */
export interface TaskStatusResponse {
  task_id: string;
  status: 'PENDING' | 'PROGRESS' | 'SUCCESS' | 'FAILURE';
  progress: number;
  message: string;
  result?: {
    success: boolean;
    total?: number;
    processed?: number;
    failed?: number;
    skipped?: number;
    statistics?: any;
  };
  error?: string;
  total?: number;
  processed?: number;
  failed?: number;
  current_batch?: number;
  total_batches?: number;
}

/**
 * News Themes Response
 */
export interface NewsThemesResponse {
  success: boolean;
  news_id: number;
  themes: ThemeMatch[];
  count: number;
}

/**
 * Theme List Response
 */
export interface ThemeListResponse {
  success: boolean;
  themes: InvestmentTheme[];
  total: number;
  limit: number;
  offset: number;
}


/**
 * Similarity Level
 */
export enum SimilarityLevel {
  VERY_HIGH = 'very_high',  // >= 0.9
  HIGH = 'high',            // >= 0.8
  MEDIUM = 'medium',        // >= 0.7
  LOW = 'low',              // >= 0.6
  VERY_LOW = 'very_low',    // < 0.6
}

/**
 * Get Similarity Level
 */
export function getSimilarityLevel(similarity: number): SimilarityLevel {
  if (similarity >= 0.9) return SimilarityLevel.VERY_HIGH;
  if (similarity >= 0.8) return SimilarityLevel.HIGH;
  if (similarity >= 0.7) return SimilarityLevel.MEDIUM;
  if (similarity >= 0.6) return SimilarityLevel.LOW;
  return SimilarityLevel.VERY_LOW;
}

/**
 * Get Similarity Color
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
 * Format Similarity Display
 */
export function formatSimilarity(similarity: number): string {
  return `${(similarity * 100).toFixed(0)}%`;
}

/**
 * Description Source Label Mapping
 */
export const DESCRIPTION_SOURCE_LABELS: Record<string, { text: string; color: string }> = {
  llm_generated: { text: 'LLM Generated', color: 'green' },
  simple_template: { text: 'Simple Template', color: 'orange' },
  manual: { text: 'Manual', color: 'blue' },
  scraped: { text: 'Scraped', color: 'purple' },
};

/**
 * Get Description Source Label
 */
export function getDescriptionSourceLabel(source?: string): { text: string; color: string } {
  if (!source) return { text: 'Not Generated', color: 'default' };
  return DESCRIPTION_SOURCE_LABELS[source] || { text: 'Unknown', color: 'default' };
}

