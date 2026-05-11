import React from 'react';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

/** Provider ids with bundled vector marks under `public/llm-providers/`. */
const BRAND_SVG_IDS = new Set(['qwen', 'doubao', 'glm']);

export interface LLMProviderBrandIconProps {
  providerId: string;
  /** Shown when `providerId` has no bundled SVG (e.g. future providers). */
  fallbackIcon?: string;
  size?: number;
  className?: string;
  style?: React.CSSProperties;
}

/**
 * Renders official-style brand marks for Qwen, Doubao (ByteDance family), and GLM (Zhipu-style mark).
 * Monochrome SVGs are lightened in dark mode for contrast on tinted cards.
 */
export const LLMProviderBrandIcon: React.FC<LLMProviderBrandIconProps> = ({
  providerId,
  fallbackIcon = '🤖',
  size = 20,
  style,
  className,
}) => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const base = process.env.PUBLIC_URL ?? '';

  if (!BRAND_SVG_IDS.has(providerId)) {
    return (
      <span
        className={className}
        style={{ fontSize: Math.round(size * 0.85), lineHeight: 1, ...style }}
        role="img"
        aria-hidden
      >
        {fallbackIcon}
      </span>
    );
  }

  const isColorMark = providerId === 'glm';
  const monoFilter = isDark && !isColorMark ? 'invert(1) grayscale(1) brightness(1.15)' : undefined;

  return (
    <img
      className={className}
      src={`${base}/llm-providers/${providerId}.svg`}
      alt=""
      width={size}
      height={size}
      draggable={false}
      style={{
        objectFit: 'contain',
        verticalAlign: 'middle',
        flexShrink: 0,
        display: 'inline-block',
        filter: monoFilter,
        ...style,
      }}
    />
  );
};
