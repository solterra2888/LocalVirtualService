/** Shared Feed Reader / Mobile Feeds theme tokens (aligned with FeedReaderPage & MobileSourcesPage) */
export const FEED_THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    textMuted: '#999',
    codeBlockBg: '#f5f5f5',
    selectedBorder: '#1890ff',
    inputBg: undefined as string | undefined,
    successBg: '#f6ffed',
    successBorder: '#b7eb8f',
    boxMuted: '#f5f5f5',
    hoverBg: '#f5f5f5',
    selectedItemBg: '#e8f4ff',
  },
  dark: {
    pageBg: '#1E202B',
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    text: '#FFFFFF',
    textSecondary: '#A0A3AD',
    textMuted: '#A0A3AD',
    codeBlockBg: '#1E202B',
    selectedBorder: '#2196F3',
    inputBg: '#1E202B',
    hoverBg: 'rgba(255, 255, 255, 0.06)',
    successBg: 'rgba(82, 196, 26, 0.15)',
    successBorder: 'rgba(82, 196, 26, 0.45)',
    boxMuted: '#1E202B',
    selectedItemBg: 'rgba(33, 150, 243, 0.12)',
  },
} as const;

export type FeedThemeColors = (typeof FEED_THEME_COLORS)[keyof typeof FEED_THEME_COLORS];
