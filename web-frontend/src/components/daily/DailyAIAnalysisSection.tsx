/**
 * Daily 抽屉 — AI 深度分析段.
 *
 * 薄封装: 直接转发给共享的 AIAnalysisSection.
 * 调用方 (DailyPreviewDrawer) 负责提取 content.
 *
 *   · Upload: processing_stats.final_report
 *   · Feed:   subscription_content_user_summaries
 *
 * 数据缺失 → 整段静默隐藏 (AIAnalysisSection 内部 return null).
 */

import React from 'react';
import AIAnalysisSection from '../shared/content-viewer/AIAnalysisSection';

interface DailyAIAnalysisSectionProps {
  content: string | null;
  themeMode: 'light' | 'dark';
}

const DailyAIAnalysisSection: React.FC<DailyAIAnalysisSectionProps> = ({ content, themeMode }) => (
  <AIAnalysisSection content={content} themeMode={themeMode} />
);

export default DailyAIAnalysisSection;
