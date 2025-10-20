/**
 * 新闻分类标签辅助函数
 * 用于解析和展示 news_classification 的结果
 */

import type { NewsAnalysisResult } from '../types/news';

// 分类数据结构
export interface NewsClassificationData {
  primary_theme: string;
  primary_theme_confidence: number;
  event_type: string;
  event_type_confidence: number;
  impact_scope: string;
  urgency: string;
  related_sectors: string[];
  rationale: string;
}

// 分类标签配置
export interface ClassificationTagConfig {
  key: string;
  label: string;
  color: string;
  icon?: string;
}

// 分类字段配置映射
export const CLASSIFICATION_CONFIG: Record<string, ClassificationTagConfig> = {
  primary_theme: {
    key: 'primary_theme',
    label: '主题',
    color: 'blue',
    icon: '🏷️'
  },
  event_type: {
    key: 'event_type',
    label: '事件',
    color: 'green',
    icon: '📅'
  },
  impact_scope: {
    key: 'impact_scope',
    label: '影响',
    color: 'orange',
    icon: '📊'
  },
  urgency: {
    key: 'urgency',
    label: '时效',
    color: 'red',
    icon: '⚡'
  }
};

// 主题映射
export const PRIMARY_THEME_MAP: Record<string, string> = {
  'finance': '金融',
  'technology': '科技',
  'economic': '经济',
  'political': '政治',
  'geopolitical': '地缘政治',
  'energy': '能源',
  'conflict': '冲突',
  'regulatory': '监管',
  'corporate': '企业',
  'industry': '行业'
};

// 事件类型映射
export const EVENT_TYPE_MAP: Record<string, string> = {
  'earnings_report': '财报',
  'merger_acquisition': '并购',
  'product_launch': '产品发布',
  'leadership_change': '人事变动',
  'legal_regulatory': '法务监管',
  'market_movement': '市场波动',
  'partnership': '合作',
  'investment_funding': '投资融资',
  'dividend_buyback': '分红回购',
  'outlook_guidance': '展望指引',
  'general_news': '一般新闻'
};

// 影响范围映射
export const IMPACT_SCOPE_MAP: Record<string, string> = {
  'company_specific': '公司',
  'sector_wide': '行业',
  'market_wide': '市场',
  'regional': '区域',
  'global': '全球'
};

// 时效性映射
export const URGENCY_MAP: Record<string, string> = {
  'breaking': '突发',
  'timely': '及时',
  'routine': '常规',
  'background': '背景'
};

/**
 * 解析分类数据
 * @param classificationContent - 分类内容的字符串
 * @returns 解析后的分类数据，如果解析失败返回 null
 */
export function parseClassificationData(classificationContent: string): NewsClassificationData | null {
  try {
    // 移除可能的代码块标记
    let jsonStr = classificationContent;
    if (jsonStr.includes('```json')) {
      jsonStr = jsonStr.replace(/```json\n?/g, '').replace(/```\n?/g, '');
    }
    
    // 解析 JSON
    const data = JSON.parse(jsonStr);
    
    // 验证必要字段
    if (!data.primary_theme || !data.event_type || !data.impact_scope || !data.urgency) {
      return null;
    }
    
    return data as NewsClassificationData;
  } catch (error) {
    console.warn('Failed to parse classification data:', error);
    return null;
  }
}

/**
 * 从分析结果中获取分类数据
 * @param analysisResult - 分析结果
 * @returns 分类数据，如果不存在返回 null
 */
export function getClassificationFromAnalysis(analysisResult: NewsAnalysisResult | undefined): NewsClassificationData | null {
  if (!analysisResult) {
    return null;
  }

  // 检查是否为新格式（多模板）
  if (typeof analysisResult === 'object' && !('content' in analysisResult)) {
    const multiTemplate = analysisResult as any;
    const classificationResult = multiTemplate['news_classification'];
    
    if (classificationResult && classificationResult.content) {
      return parseClassificationData(classificationResult.content);
    }
  }
  
  // 检查是否为旧格式（单模板）
  if ('content' in analysisResult && 'template_code' in analysisResult) {
    const singleTemplate = analysisResult as any;
    if (singleTemplate.template_code === 'news_classification' && singleTemplate.content) {
      return parseClassificationData(singleTemplate.content);
    }
  }
  
  return null;
}

/**
 * 获取分类标签列表
 * @param classification - 分类数据
 * @returns 标签配置数组
 */
export function getClassificationTags(classification: NewsClassificationData): Array<{
  key: string;
  config: ClassificationTagConfig;
  value: string;
  displayValue: string;
}> {
  const tags: Array<{
    key: string;
    config: ClassificationTagConfig;
    value: string;
    displayValue: string;
  }> = [];

  // 主题标签
  if (classification.primary_theme) {
    tags.push({
      key: 'primary_theme',
      config: CLASSIFICATION_CONFIG.primary_theme,
      value: classification.primary_theme,
      displayValue: classification.primary_theme
    });
  }

  // 事件类型标签
  if (classification.event_type) {
    tags.push({
      key: 'event_type',
      config: CLASSIFICATION_CONFIG.event_type,
      value: classification.event_type,
      displayValue: classification.event_type
    });
  }

  // 影响范围标签
  if (classification.impact_scope) {
    tags.push({
      key: 'impact_scope',
      config: CLASSIFICATION_CONFIG.impact_scope,
      value: classification.impact_scope,
      displayValue: classification.impact_scope
    });
  }

  // 时效性标签
  if (classification.urgency) {
    tags.push({
      key: 'urgency',
      config: CLASSIFICATION_CONFIG.urgency,
      value: classification.urgency,
      displayValue: classification.urgency
    });
  }

  return tags;
}

/**
 * 检查是否有有效的分类数据
 * @param classification - 分类数据
 * @returns 是否有效
 */
export function hasValidClassification(classification: NewsClassificationData | null): boolean {
  return classification !== null && 
         !!classification.primary_theme && 
         !!classification.event_type && 
         !!classification.impact_scope && 
         !!classification.urgency;
}

/**
 * 获取相关行业标签
 * @param classification - 分类数据
 * @returns 相关行业数组
 */
export function getRelatedSectors(classification: NewsClassificationData | null): string[] {
  if (!classification || !classification.related_sectors) {
    return [];
  }
  
  return classification.related_sectors;
}

/**
 * 获取分类摘要信息
 * @param classification - 分类数据
 * @returns 摘要信息
 */
export function getClassificationSummary(classification: NewsClassificationData | null): {
  theme: string;
  event: string;
  impact: string;
  urgency: string;
  sectors: string[];
  confidence: {
    theme: number;
    event: number;
  };
} | null {
  if (!hasValidClassification(classification)) {
    return null;
  }

  return {
    theme: classification!.primary_theme,
    event: classification!.event_type,
    impact: classification!.impact_scope,
    urgency: classification!.urgency,
    sectors: classification!.related_sectors || [],
    confidence: {
      theme: classification!.primary_theme_confidence || 0,
      event: classification!.event_type_confidence || 0
    }
  };
}

