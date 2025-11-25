/**
 * 新闻分析结果辅助函数
 * 用于处理多模板分析结果的提取和兼容
 */

import type { NewsAnalysisResult, SingleTemplateAnalysisResult } from '../types/news';

/**
 * 判断是否为新格式（多模板）
 */
export function isMultiTemplateFormat(analysisResult: any): boolean {
  if (!analysisResult || typeof analysisResult !== 'object') {
    return false;
  }
  
  // 新格式：没有 content 字段，但有 template_code 作为键
  // 旧格式：有 content 字段和 template_code 字段
  return !('content' in analysisResult) && !('template_code' in analysisResult);
}

/**
 * 从分析结果中提取特定模板的内容
 * @param analysisResult - 分析结果（新格式或旧格式）
 * @param templateCode - 模板代码（如 'news_summary_v1'）
 * @returns 单个模板的分析结果，如果不存在则返回 null
 */
export function getAnalysisContent(
  analysisResult: NewsAnalysisResult | undefined,
  templateCode: string
): SingleTemplateAnalysisResult | null {
  if (!analysisResult) {
    return null;
  }

  // 新格式（多模板）
  if (isMultiTemplateFormat(analysisResult)) {
    const multiTemplate = analysisResult as any;
    return multiTemplate[templateCode] || null;
  }

  // 旧格式（单模板）- 检查 template_code 是否匹配
  const singleTemplate = analysisResult as SingleTemplateAnalysisResult;
  if (singleTemplate.template_code === templateCode) {
    return singleTemplate;
  }

  return null;
}

/**
 * 获取首选的 summary 模板（按优先级）
 * @param analysisResult - 分析结果
 * @returns 第一个找到的 summary 模板代码和结果
 */
export function getPreferredSummaryTemplate(
  analysisResult: NewsAnalysisResult | undefined
): { templateCode: string; result: SingleTemplateAnalysisResult } | null {
  if (!analysisResult) {
    return null;
  }

  // Summary 模板优先级列表（按顺序尝试）
  const summaryTemplatePriority = [
    'news_cctv_summary_v1',  // CCTV 专用模板优先
    'news_summary_v1',       // 通用模板
    'news_summary_safe_v1'   // 保守模板
  ];

  // 按优先级尝试获取 summary 模板
  for (const templateCode of summaryTemplatePriority) {
    const result = getAnalysisContent(analysisResult, templateCode);
    if (result && typeof result.content === 'string') {
      return { templateCode, result };
    }
  }

  // 如果没有预定义的 summary，检查是否是旧格式的 content
  if (!isMultiTemplateFormat(analysisResult)) {
    const singleTemplate = analysisResult as SingleTemplateAnalysisResult;
    if (singleTemplate.content && typeof singleTemplate.content === 'string' && singleTemplate.template_code) {
      return { templateCode: singleTemplate.template_code, result: singleTemplate };
    }
  }

  return null;
}

/**
 * 获取首选的分析内容（优先使用 summary 模板，否则使用原始内容）
 * @param analysisResult - 分析结果
 * @param fallbackContent - 当没有分析结果时的备用内容
 * @returns 要显示的内容
 */
export function getPreferredAnalysisContent(
  analysisResult: NewsAnalysisResult | undefined,
  fallbackContent?: string
): string | null {
  const preferred = getPreferredSummaryTemplate(analysisResult);
  if (preferred) {
    return preferred.result.content as string;
  }

  // 返回备用内容
  return fallbackContent || null;
}

/**
 * 获取所有可用的模板分析结果
 * @param analysisResult - 分析结果
 * @returns 所有模板的分析结果映射
 */
export function getAllAnalysisResults(
  analysisResult: NewsAnalysisResult | undefined
): { [templateCode: string]: SingleTemplateAnalysisResult } {
  if (!analysisResult) {
    return {};
  }

  // 新格式（多模板）
  if (isMultiTemplateFormat(analysisResult)) {
    return analysisResult as any;
  }

  // 旧格式（单模板），转换为新格式
  const singleTemplate = analysisResult as SingleTemplateAnalysisResult;
  if (singleTemplate.template_code) {
    return {
      [singleTemplate.template_code]: singleTemplate
    };
  }

  return {};
}

/**
 * 检查是否有特定模板的分析结果
 * @param analysisResult - 分析结果
 * @param templateCode - 模板代码
 * @returns 是否存在该模板的分析
 */
export function hasAnalysisForTemplate(
  analysisResult: NewsAnalysisResult | undefined,
  templateCode: string
): boolean {
  return getAnalysisContent(analysisResult, templateCode) !== null;
}

/**
 * 获取分析结果的摘要信息（用于显示标签等）
 * @param analysisResult - 分析结果
 * @returns 摘要信息
 */
export function getAnalysisSummaryInfo(
  analysisResult: NewsAnalysisResult | undefined
): {
  hasAnalysis: boolean;
  templateCount: number;
  templates: string[];
  totalTokens: number;
} {
  const allResults = getAllAnalysisResults(analysisResult);
  const templates = Object.keys(allResults);
  const totalTokens = templates.reduce((sum, template) => {
    return sum + (allResults[template].tokens || 0);
  }, 0);

  return {
    hasAnalysis: templates.length > 0,
    templateCount: templates.length,
    templates,
    totalTokens
  };
}

