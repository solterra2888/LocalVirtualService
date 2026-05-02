/**
 * Institutional shell for Brief digest & Deep dive cards.
 *
 * Light: navy–gold accent (finance brief aesthetic).
 * Dark: align with app tokens — cool blue accent bar, neutral borders (gold reads muddy on #1a2234).
 */

import type { CSSProperties } from 'react';
import type { AISummarySectionThemeColors } from './AISummarySection';

export interface FinanceInsightShellStyles {
  wrapper: CSSProperties;
  accentBar: CSSProperties;
  headerRow: CSSProperties;
  /** Uppercase micro-label for “Brief digest” / “Deep dive” */
  labelText: CSSProperties;
  iconColor: string;
}

export function getFinanceInsightShellStyles(
  themeMode: 'light' | 'dark',
  colors: AISummarySectionThemeColors
): FinanceInsightShellStyles {
  const isDark = themeMode === 'dark';

  if (isDark) {
    return {
      wrapper: {
        borderRadius: 8,
        overflow: 'hidden',
        border: `1px solid ${colors.divider}`,
        background: colors.cardBg,
        boxShadow: '0 1px 0 rgba(255,255,255,0.05) inset',
      },
      accentBar: {
        height: 3,
        background: `linear-gradient(90deg, rgba(33,150,243,0.22) 0%, ${colors.accent} 48%, rgba(129,199,245,0.55) 100%)`,
      },
      headerRow: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        gap: 12,
        flexWrap: 'wrap',
        marginBottom: 10,
        paddingBottom: 8,
        borderBottom: `1px solid ${colors.divider}`,
      },
      labelText: {
        fontSize: 10,
        letterSpacing: 1.1,
        textTransform: 'uppercase',
        fontWeight: 600,
        color: colors.textMuted,
      },
      iconColor: colors.accent,
    };
  }

  return {
    wrapper: {
      borderRadius: 8,
      overflow: 'hidden',
      border: '1px solid #d1d6de',
      background: '#f4f5f7',
      boxShadow: '0 1px 2px rgba(21, 44, 72, 0.05), 0 6px 18px rgba(21, 44, 72, 0.06)',
    },
    accentBar: {
      height: 3,
      background:
        'linear-gradient(90deg, #132f52 0%, #2d5a8f 46%, #b8860b 100%)',
    },
    headerRow: {
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between',
      gap: 12,
      flexWrap: 'wrap',
      marginBottom: 10,
      paddingBottom: 8,
      borderBottom: '1px solid #e4e7ec',
    },
    labelText: {
      fontSize: 10,
      letterSpacing: 1.1,
      textTransform: 'uppercase',
      fontWeight: 600,
      color: '#3d4a5c',
    },
    iconColor: '#1a3a5c',
  };
}
