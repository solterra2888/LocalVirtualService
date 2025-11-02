/**
 * 统一主题辅助函数
 * 用于处理和展示 news_themes_unified 的数据
 */

import type { NewsUnifiedTheme } from '../types/news';

// 来源类型配置
export interface SourceTypeConfig {
  key: string;
  label: string;
  color: string;
  icon: string;
  weight: number;
}

// 来源类型映射
export const SOURCE_TYPE_CONFIG: Record<string, SourceTypeConfig> = {
  manual: {
    key: 'manual',
    label: '人工',
    color: 'gold',
    icon: '👤',
    weight: 1.5
  },
  vector_retrieval: {
    key: 'vector_retrieval',
    label: '向量',
    color: 'blue',
    icon: '🔍',
    weight: 1.0
  },
  llm_classification: {
    key: 'llm_classification',
    label: 'LLM',
    color: 'purple',
    icon: '🤖',
    weight: 0.8
  },
  eodhd_auto_tag: {
    key: 'eodhd_auto_tag',
    label: 'API',
    color: 'green',
    icon: '📡',
    weight: 0.6
  },
  hybrid: {
    key: 'hybrid',
    label: '融合',
    color: 'cyan',
    icon: '⚡',
    weight: 1.2
  }
};

// 主题分类配置
export const THEME_CATEGORY_CONFIG: Record<string, { color: string; icon: string }> = {
  Technology: { color: 'blue', icon: '💻' },
  Energy: { color: 'orange', icon: '⚡' },
  Healthcare: { color: 'red', icon: '❤️' },
  Finance: { color: 'green', icon: '💰' },
  Consumer: { color: 'purple', icon: '🛒' },
  Industrial: { color: 'geekblue', icon: '🏭' },
  Materials: { color: 'volcano', icon: '⚙️' },
  'Real Estate': { color: 'magenta', icon: '🏢' },
  Utilities: { color: 'lime', icon: '🔌' },
  Communication: { color: 'cyan', icon: '📱' }
};

/**
 * 获取来源类型配置
 */
export function getSourceTypeConfig(sourceType: string): SourceTypeConfig {
  return SOURCE_TYPE_CONFIG[sourceType] || {
    key: sourceType,
    label: sourceType,
    color: 'default',
    icon: '🏷️',
    weight: 0.5
  };
}

/**
 * 获取主题分类配置
 */
export function getThemeCategoryConfig(category?: string): { color: string; icon: string } {
  if (!category) return { color: 'default', icon: '📌' };
  return THEME_CATEGORY_CONFIG[category] || { color: 'default', icon: '📌' };
}

/**
 * 按质量分数分组统一主题
 */
export function groupThemesByQuality(themes: NewsUnifiedTheme[]): {
  highQuality: NewsUnifiedTheme[];  // quality_score >= 0.8
  mediumQuality: NewsUnifiedTheme[];  // 0.6 <= quality_score < 0.8
  lowQuality: NewsUnifiedTheme[];  // quality_score < 0.6
} {
  const highQuality: NewsUnifiedTheme[] = [];
  const mediumQuality: NewsUnifiedTheme[] = [];
  const lowQuality: NewsUnifiedTheme[] = [];

  themes.forEach(theme => {
    const score = theme.quality_score || theme.confidence_score;
    if (score >= 0.8) {
      highQuality.push(theme);
    } else if (score >= 0.6) {
      mediumQuality.push(theme);
    } else {
      lowQuality.push(theme);
    }
  });

  return { highQuality, mediumQuality, lowQuality };
}

/**
 * 按标准化名称分组主题（合并同一主题的多个来源）
 */
export function groupThemesByCanonicalName(themes: NewsUnifiedTheme[]): Map<string, NewsUnifiedTheme[]> {
  const grouped = new Map<string, NewsUnifiedTheme[]>();
  
  themes.forEach(theme => {
    const key = theme.canonical_theme_name || theme.theme_name;
    if (!grouped.has(key)) {
      grouped.set(key, []);
    }
    grouped.get(key)!.push(theme);
  });

  return grouped;
}

/**
 * 获取主题的最佳展示信息
 */
export function getBestThemeDisplay(themes: NewsUnifiedTheme[]): {
  displayName: string;
  category?: string;
  subCategory?: string;
  description?: string;
  sources: string[];
  maxQualityScore: number;
  avgConfidence: number;
  isVerified: boolean;
  isPrimary: boolean;
  markets: string[];
} {
  if (themes.length === 0) {
    return {
      displayName: '',
      sources: [],
      maxQualityScore: 0,
      avgConfidence: 0,
      isVerified: false,
      isPrimary: false,
      markets: []
    };
  }

  // 优先使用有 display_name 的主题
  const themeWithDisplayName = themes.find(t => t.display_name);
  const displayName = themeWithDisplayName?.display_name || 
                     themes[0].canonical_theme_name || 
                     themes[0].theme_name;

  const category = themeWithDisplayName?.category || themes[0].category;
  const subCategory = themeWithDisplayName?.sub_category || themes[0].sub_category;
  const description = themeWithDisplayName?.description || themes[0].description;

  // 收集所有来源
  const sources = Array.from(new Set(themes.map(t => t.source_type)));

  // 计算最高质量分数
  const maxQualityScore = Math.max(...themes.map(t => t.quality_score || t.confidence_score));

  // 计算平均置信度
  const avgConfidence = themes.reduce((sum, t) => sum + t.confidence_score, 0) / themes.length;

  // 检查是否被验证或标记为主要
  const isVerified = themes.some(t => t.is_verified);
  const isPrimary = themes.some(t => t.is_primary);

  // 收集所有市场
  const markets = Array.from(new Set(themes.map(t => t.market).filter(Boolean))) as string[];

  return {
    displayName,
    category,
    subCategory,
    description,
    sources,
    maxQualityScore,
    avgConfidence,
    isVerified,
    isPrimary,
    markets
  };
}

/**
 * 格式化质量分数显示
 */
export function formatQualityScore(score?: number): string {
  if (!score) return 'N/A';
  return `${(score * 100).toFixed(0)}%`;
}

/**
 * 获取质量分数的颜色
 */
export function getQualityScoreColor(score?: number): string {
  if (!score) return 'default';
  if (score >= 0.8) return 'success';
  if (score >= 0.6) return 'warning';
  return 'default';
}

/**
 * 获取置信度分数的颜色
 */
export function getConfidenceScoreColor(score: number): string {
  if (score >= 0.8) return 'green';
  if (score >= 0.6) return 'orange';
  return 'volcano';
}

/**
 * 检查是否有有效的统一主题数据
 */
export function hasValidUnifiedThemes(themes?: NewsUnifiedTheme[]): boolean {
  return !!themes && themes.length > 0;
}

/**
 * 获取前N个最佳主题（按质量分数排序）
 */
export function getTopThemes(themes: NewsUnifiedTheme[], limit: number = 5): NewsUnifiedTheme[] {
  return [...themes]
    .sort((a, b) => {
      const scoreA = a.quality_score || a.confidence_score;
      const scoreB = b.quality_score || b.confidence_score;
      return scoreB - scoreA;
    })
    .slice(0, limit);
}

/**
 * 获取主要主题（is_primary = true）
 */
export function getPrimaryThemes(themes: NewsUnifiedTheme[]): NewsUnifiedTheme[] {
  return themes.filter(t => t.is_primary);
}

/**
 * 获取已验证主题（is_verified = true）
 */
export function getVerifiedThemes(themes: NewsUnifiedTheme[]): NewsUnifiedTheme[] {
  return themes.filter(t => t.is_verified);
}

