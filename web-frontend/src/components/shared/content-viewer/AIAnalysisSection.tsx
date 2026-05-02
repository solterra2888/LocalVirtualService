/**
 * Shared AI Analysis block — long-form LLM Markdown.
 *
 * Visual shell matches AISummarySection (same card, top accent gradient, typography).
 *
 * Used by:
 *   · Daily  → upload final_report / feed personalized_summary
 *   · Feeds  → personalized_summary (+ actionsSlot for Generate / Reset)
 *
 * Visibility:
 *   content == null && !actionsSlot → return null
 *   content == null &&  actionsSlot → shell + placeholder + actions
 *   content != null                 → full Markdown + fullscreen + actionsSlot
 */

import React from 'react';
import { Button, Modal, Space, Tooltip, Typography } from 'antd';
import { AuditOutlined, ExpandAltOutlined } from '@ant-design/icons';
import MarkdownRenderer from '../../MarkdownRenderer';
import { DAILY_THEME_COLORS } from '../../daily/dailyTheme';
import type { AISummarySectionThemeColors } from './AISummarySection';
import { getFinanceInsightShellStyles } from './financeInsightCardShell';

const { Text } = Typography;

function resolveShellColors(
  themeMode: 'light' | 'dark',
  colors?: AISummarySectionThemeColors
): AISummarySectionThemeColors {
  if (colors) return colors;
  const d = DAILY_THEME_COLORS[themeMode];
  return {
    text: d.text,
    textMuted: d.textMuted,
    divider: d.divider,
    cardBg: d.cardBg,
    accent: d.accent,
  };
}

export interface AIAnalysisSectionProps {
  content: string | null;
  themeMode: 'light' | 'dark';
  /** Same tokens as AISummarySection — Feeds should pass feed theme for parity. */
  colors?: AISummarySectionThemeColors;
  actionsSlot?: React.ReactNode;
  reasoning?: string | null;
}

const AIAnalysisSection: React.FC<AIAnalysisSectionProps> = ({
  content,
  themeMode,
  colors: colorsProp,
  actionsSlot,
  reasoning,
}) => {
  const c = resolveShellColors(themeMode, colorsProp);
  const shell = getFinanceInsightShellStyles(themeMode, c);
  const [fullscreenOpen, setFullscreenOpen] = React.useState(false);

  if (!content && !actionsSlot) return null;

  return (
    <>
      <div style={shell.wrapper}>
        <div style={shell.accentBar} />
        <div style={{ padding: '12px 16px 16px' }}>
          <div style={shell.headerRow}>
            <Space size={8} align="center" wrap>
              <AuditOutlined style={{ color: shell.iconColor, fontSize: 15 }} />
              <Text style={shell.labelText}>Deep dive</Text>
              {!content && (
                <Text type="secondary" style={{ fontSize: 11 }}>
                  · not generated yet
                </Text>
              )}
            </Space>

            <Space size={2} align="center" wrap style={{ flexShrink: 0 }}>
              {content && (
                <Tooltip title="Fullscreen reader" mouseEnterDelay={0.4}>
                  <Button
                    size="small"
                    type="text"
                    icon={<ExpandAltOutlined />}
                    onClick={() => setFullscreenOpen(true)}
                  />
                </Tooltip>
              )}
              {actionsSlot}
            </Space>
          </div>

          {content && (
            <div style={{ color: c.text, fontSize: 13, lineHeight: 1.65 }}>
              <MarkdownRenderer content={content} variant={themeMode} />
            </div>
          )}

          {content && reasoning && (
            <Text
              type="secondary"
              style={{
                fontSize: 12,
                lineHeight: 1.55,
                display: 'block',
                fontStyle: 'italic',
                marginTop: 8,
              }}
            >
              {reasoning}
            </Text>
          )}
        </div>
      </div>

      {content && (
        <Modal
          title={
            <Space size={6} align="center">
              <AuditOutlined style={{ color: shell.iconColor }} />
              <span style={shell.labelText}>Deep dive</span>
            </Space>
          }
          open={fullscreenOpen}
          onCancel={() => setFullscreenOpen(false)}
          footer={
            <Button type="primary" onClick={() => setFullscreenOpen(false)}>
              Close
            </Button>
          }
          width="90vw"
          style={{ top: 24, maxWidth: 1100 }}
          styles={{
            body: {
              maxHeight: 'calc(100vh - 200px)',
              overflowY: 'auto',
              padding: '20px 28px',
            },
          }}
          destroyOnClose
        >
          <MarkdownRenderer content={content} variant={themeMode} />
        </Modal>
      )}
    </>
  );
};

export default AIAnalysisSection;
