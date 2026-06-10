import type { CSSProperties } from 'react';

/**
 * 全站统一的 AI 内容标签（sector / ticker / concept / 溢出计数）视觉标准。
 * 用于 Feeds、Daily、Sources（Upload Files 详情等）中带语义色的胶囊标签。
 */
export const AI_TAG_CHIP_FONT_SIZE = 9;

export type AiTagChipVariant =
  | 'ticker'
  | 'sector'
  | 'concept'
  | 'overflow'
  | 'country'
  | 'person'
  | 'theme'
  | 'signal-bullish'
  | 'signal-bearish'
  | 'signal-neutral'
  | 'signal-meta';

const PALETTE = {
  light: {
    ticker:           { bg: '#e3f2fd', fg: '#1565c0' },
    sector:           { bg: '#e8f5e9', fg: '#1b5e20' },
    concept:          { bg: '#f3e5f5', fg: '#6a1b9a' },
    overflow:         { bg: 'rgba(0, 0, 0, 0.06)',  fg: 'rgba(0, 0, 0, 0.72)' },
    country:          { bg: '#fff3e0', fg: '#e65100' },
    person:           { bg: '#e8eaf6', fg: '#283593' },
    theme:            { bg: '#e0f7fa', fg: '#00695c' },
    'signal-bullish': { bg: '#e8f5e9', fg: '#2e7d32' },
    'signal-bearish': { bg: '#fce4ec', fg: '#c62828' },
    'signal-neutral': { bg: '#f5f5f5', fg: '#616161' },
    'signal-meta':    { bg: '#fafafa', fg: '#757575' },
  },
  dark: {
    ticker:           { bg: 'rgba(33, 150, 243, 0.22)',  fg: '#90caf9' },
    sector:           { bg: 'rgba(76, 175, 80, 0.24)',   fg: '#a5d6a7' },
    concept:          { bg: 'rgba(156, 39, 176, 0.24)',  fg: '#ce93d8' },
    overflow:         { bg: 'rgba(255, 255, 255, 0.1)',  fg: 'rgba(255, 255, 255, 0.78)' },
    country:          { bg: 'rgba(255, 152, 0, 0.22)',   fg: '#ffcc80' },
    person:           { bg: 'rgba(63, 81, 181, 0.28)',   fg: '#9fa8da' },
    theme:            { bg: 'rgba(0, 150, 136, 0.22)',   fg: '#80cbc4' },
    'signal-bullish': { bg: 'rgba(76, 175, 80, 0.28)',   fg: '#a5d6a7' },
    'signal-bearish': { bg: 'rgba(244, 67, 54, 0.28)',   fg: '#ef9a9a' },
    'signal-neutral': { bg: 'rgba(255, 255, 255, 0.08)', fg: '#9e9e9e' },
    'signal-meta':    { bg: 'rgba(255, 255, 255, 0.06)', fg: '#757575' },
  },
} as const;

/** 字号、行高、圆角、无边框；不含主题色（可与 antd Tag `color` 叠用以做语义色） */
export const AI_TAG_CHIP_BASE: CSSProperties = {
  margin: 0,
  marginInlineEnd: 0,
  border: 'none',
  borderRadius: 9999,
  fontSize: AI_TAG_CHIP_FONT_SIZE,
  lineHeight: '12px',
  padding: '2px 8px',
  fontWeight: 500,
};

export function aiTagChipStyle(
  variant: AiTagChipVariant,
  themeMode: 'light' | 'dark'
): CSSProperties {
  const pal = PALETTE[themeMode === 'dark' ? 'dark' : 'light'][variant];
  return {
    ...AI_TAG_CHIP_BASE,
    background: pal.bg,
    color: pal.fg,
  };
}
