/**
 * Shared modal styles utility.
 * All antd <Modal> components should use getModalStyles(themeMode) for the `styles` prop
 * to ensure consistent light/dark appearance across the app.
 *
 * Reference: FinvizDashboard – AI Analysis Details modal (the canonical style).
 */
import type React from 'react';

export type ThemeMode = 'light' | 'dark';

/** Dark-mode modal background color (slightly blue-tinted dark card) */
export const MODAL_DARK_BG = '#252d3d';

/** Dark-mode border / divider color */
export const MODAL_DARK_BORDER = 'rgba(255, 255, 255, 0.06)';

type ModalStylesMap = {
  wrapper?: React.CSSProperties;
  content?: React.CSSProperties;
  header?: React.CSSProperties;
  body?: React.CSSProperties;
  footer?: React.CSSProperties;
  mask?: React.CSSProperties;
};

/**
 * Returns the `styles` prop for an antd v5 <Modal>.
 * In light mode, returns an empty object (default antd light styling is correct).
 * In dark mode, applies the canonical blue-tinted dark background + border + shadow.
 *
 * Usage:
 *   <Modal styles={getModalStyles(themeMode)} ...>
 */
export function getModalStyles(themeMode: ThemeMode): ModalStylesMap {
  if (themeMode !== 'dark') return {};
  return {
    wrapper: { backdropFilter: 'brightness(0.7)' },
    content: {
      backgroundColor: MODAL_DARK_BG,
      border: `1px solid ${MODAL_DARK_BORDER}`,
      boxShadow: '0 8px 32px rgba(0,0,0,0.5), 0 0 0 1px rgba(255,255,255,0.06)',
    },
    header: {
      backgroundColor: MODAL_DARK_BG,
      borderBottomColor: MODAL_DARK_BORDER,
    },
    body: {
      backgroundColor: MODAL_DARK_BG,
    },
    footer: {
      backgroundColor: MODAL_DARK_BG,
      borderTopColor: MODAL_DARK_BORDER,
    },
  };
}
