/**
 * Daily 抽屉 — AI 深度分析段 (PR-D2).
 *
 * 与 AI Summary 段并列, 专门承载长篇 LLM 产物 (Markdown):
 *   · Upload: processing_stats.final_report
 *   · Feed:   subscription_content_user_summaries.summary (per-user 个性化)
 *
 * 数据缺失 → 整段静默隐藏 (返回 null);
 * 由父组件 DailyPreviewDrawer 决定 "Original 段是否折叠".
 *
 * UI:
 *   ┌─ 🔬 AI Analysis        [Personalized] [复制] [全屏]
 *   ├─ MarkdownRenderer (content)
 *   └─ (Modal 全屏阅读, 受控)
 */

import React from 'react';
import { App, Button, Modal, Space, Tag, Tooltip, Typography } from 'antd';
import {
  CopyOutlined,
  ExperimentOutlined,
  ExpandAltOutlined,
} from '@ant-design/icons';
import MarkdownRenderer from '../MarkdownRenderer';
import { DAILY_THEME_COLORS } from './dailyTheme';
import type { DailyItemDetail } from '../../services/daily';

const { Text } = Typography;

interface DailyAIAnalysisSectionProps {
  detail: DailyItemDetail | null;
  themeMode: 'light' | 'dark';
}

/**
 * 提取本卡片对应的 AI 深度分析正文.
 *
 * upload → final_report; feed → personalized_summary.
 * 返回 null 时父组件应整段隐藏.
 */
function extractAIContent(detail: DailyItemDetail | null): {
  content: string;
  isPersonalized: boolean;
} | null {
  if (!detail) return null;
  if (detail.source === 'upload' && detail.upload?.final_report) {
    return { content: detail.upload.final_report, isPersonalized: false };
  }
  if (detail.source === 'feed' && detail.feed?.personalized_summary) {
    return { content: detail.feed.personalized_summary, isPersonalized: true };
  }
  return null;
}

const DailyAIAnalysisSection: React.FC<DailyAIAnalysisSectionProps> = ({
  detail,
  themeMode,
}) => {
  const colors = DAILY_THEME_COLORS[themeMode];
  const { message } = App.useApp();
  const [fullscreenOpen, setFullscreenOpen] = React.useState(false);

  const ai = extractAIContent(detail);

  if (!ai) return null;

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(ai.content);
      message.success('Copied to clipboard');
    } catch (e) {
      message.error('Copy failed; please select and copy manually');
    }
  };

  return (
    <>
      <div
        style={{
          borderRadius: 10,
          padding: '14px 16px',
          background:
            themeMode === 'dark'
              ? 'rgba(124, 77, 255, 0.07)'
              : 'rgba(124, 77, 255, 0.05)',
          border: `1px solid ${colors.divider}`,
          borderLeft: `3px solid #7c4dff`,
        }}
      >
        {/* Header */}
        <div
          style={{
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'space-between',
            gap: 8,
            marginBottom: 10,
            flexWrap: 'wrap',
          }}
        >
          <Space size={6} align="center">
            <ExperimentOutlined style={{ color: '#7c4dff', fontSize: 14 }} />
            <Text strong style={{ color: colors.text, fontSize: 13 }}>
              AI Analysis
            </Text>
            {ai.isPersonalized && (
              <Tag color="purple" style={{ fontSize: 10, margin: 0 }}>
                Personalized
              </Tag>
            )}
          </Space>

          <Space size={2}>
            <Tooltip title="Copy to clipboard" mouseEnterDelay={0.4}>
              <Button
                size="small"
                type="text"
                icon={<CopyOutlined />}
                onClick={handleCopy}
              />
            </Tooltip>
            <Tooltip title="Fullscreen reader" mouseEnterDelay={0.4}>
              <Button
                size="small"
                type="text"
                icon={<ExpandAltOutlined />}
                onClick={() => setFullscreenOpen(true)}
              />
            </Tooltip>
          </Space>
        </div>

        {/* Markdown body */}
        <div
          style={{
            color: colors.text,
            fontSize: 13,
            lineHeight: 1.7,
          }}
        >
          <MarkdownRenderer content={ai.content} variant={themeMode} />
        </div>
      </div>

      {/* Fullscreen Modal */}
      <Modal
        title={
          <Space size={6} align="center">
            <ExperimentOutlined style={{ color: '#7c4dff' }} />
            <span>AI Analysis</span>
            {ai.isPersonalized && (
              <Tag color="purple" style={{ fontSize: 11, margin: 0 }}>
                Personalized
              </Tag>
            )}
          </Space>
        }
        open={fullscreenOpen}
        onCancel={() => setFullscreenOpen(false)}
        footer={
          <Space>
            <Button icon={<CopyOutlined />} onClick={handleCopy}>
              Copy
            </Button>
            <Button type="primary" onClick={() => setFullscreenOpen(false)}>
              Close
            </Button>
          </Space>
        }
        width="90vw"
        style={{ top: 24, maxWidth: 1100 }}
        bodyStyle={{
          maxHeight: 'calc(100vh - 200px)',
          overflowY: 'auto',
          padding: '20px 28px',
        }}
        destroyOnClose
      >
        <MarkdownRenderer content={ai.content} variant={themeMode} />
      </Modal>
    </>
  );
};

export default DailyAIAnalysisSection;
