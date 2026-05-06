/**
 * Wrapper for Daily / Feeds body content under the "Original" (or related) tab.
 * Previously used Ant Design Collapse with an "Original Content" toggle; tabs
 * now separate views, so this is a pass-through layout wrapper only.
 */

import React from 'react';

export interface CollapsibleOriginalSectionProps {
  children: React.ReactNode;
}

const CollapsibleOriginalSection: React.FC<CollapsibleOriginalSectionProps> = ({
  children,
}) => <div style={{ width: '100%' }}>{children}</div>;

export default CollapsibleOriginalSection;
