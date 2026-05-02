/**
 * Collapsible "Original Content" section wrapper.
 *
 * Collapse rules:
 *   defaultExpanded = true (desktop) → always default expanded, even when hasAIAnalysis.
 *   defaultExpanded = false / omitted:
 *     hasAIAnalysis = true  → default collapsed
 *     hasAIAnalysis = false → default expanded
 *
 * After the user manually toggles, the auto-rule no longer fires until `resetKey` changes.
 * Pass `resetKey={item.id}` to reset the user-toggle flag when the viewed item changes.
 */

import React from 'react';
import { Collapse, Space, Typography } from 'antd';
import { FileTextOutlined } from '@ant-design/icons';

const { Text } = Typography;

export interface CollapsibleOriginalSectionProps {
  /**
   * Whether long-form AI analysis exists above this section.
   * When `defaultExpanded` is false, used to default-collapse the panel (mobile).
   */
  hasAIAnalysis: boolean;
  themeMode: 'light' | 'dark';
  children: React.ReactNode;
  /**
   * Change this value (e.g. item ID) to reset the user-toggle flag and re-apply
   * the hasAIAnalysis auto-rule. Useful when the parent item changes.
   */
  resetKey?: string | number;
  /**
   * Desktop: start with Original Content open even when AI Analysis is present.
   * Mobile / bottom-sheet: omit so long analysis keeps the panel collapsed by default.
   */
  defaultExpanded?: boolean;
}

const CollapsibleOriginalSection: React.FC<CollapsibleOriginalSectionProps> = ({
  hasAIAnalysis,
  themeMode,
  children,
  resetKey,
  defaultExpanded = false,
}) => {
  const [expanded, setExpanded] = React.useState(
    defaultExpanded ? true : !hasAIAnalysis
  );
  const [userToggled, setUserToggled] = React.useState(false);

  // Reset when the parent item changes (resetKey) — same as DailyPreviewDrawer's
  // setUserToggledOriginal(false) call in useEffect([item?.id]).
  React.useEffect(() => {
    setUserToggled(false);
  }, [resetKey]);

  // Apply auto-rule whenever hasAIAnalysis changes (e.g. detail loads) — only if
  // the user hasn't manually overridden since the last resetKey change.
  React.useEffect(() => {
    if (userToggled) return;
    setExpanded(defaultExpanded ? true : !hasAIAnalysis);
  }, [hasAIAnalysis, userToggled, defaultExpanded]);

  const labelColor = themeMode === 'dark' ? 'rgba(255,255,255,0.45)' : 'rgba(0,0,0,0.45)';

  return (
    <Collapse
      ghost
      activeKey={expanded ? ['original'] : []}
      onChange={(keys) => {
        const arr = Array.isArray(keys) ? keys : keys ? [keys] : [];
        setExpanded(arr.includes('original'));
        setUserToggled(true);
      }}
      items={[
        {
          key: 'original',
          label: (
            <Space size={6} align="center">
              <FileTextOutlined style={{ color: labelColor, fontSize: 13 }} />
              <Text
                style={{
                  fontSize: 11,
                  letterSpacing: 0.6,
                  textTransform: 'uppercase',
                  color: labelColor,
                }}
              >
                Original Content
              </Text>
            </Space>
          ),
          children,
          style: { padding: 0 },
        },
      ]}
      style={{ background: 'transparent' }}
    />
  );
};

export default CollapsibleOriginalSection;
