import React from 'react';
import { Tooltip, Typography } from 'antd';
import { TwitterOutlined, YoutubeOutlined } from '@ant-design/icons';
import { NEWS_SOURCES, getNewsSourceFaviconUrl } from './feedChannelConfig';

const { Text } = Typography;

/**
 * Picker value encoding:
 * - Platform types: "twitter", "youtube"
 * - News sources: "news:Yahoo Finance", "news:CNBC", etc.
 *
 * Parent component parses the value to extract channel_type and channel_identifier.
 */

interface PlatformOption {
  value: string;
  label: string;
  icon: React.ReactElement;
  color: string;
  disabled?: boolean;
}

const PLATFORMS: PlatformOption[] = [
  { value: 'twitter', label: 'Twitter', icon: <TwitterOutlined />, color: '#1DA1F2' },
  { value: 'youtube', label: 'YouTube', icon: <YoutubeOutlined />, color: '#FF0000' },
];

const tileBase: React.CSSProperties = {
  display: 'flex',
  flexDirection: 'column',
  alignItems: 'center',
  justifyContent: 'center',
  width: 80,
  height: 72,
  borderRadius: 12,
  cursor: 'pointer',
  transition: 'border-color .2s, background-color .2s',
  userSelect: 'none',
};

/** sRGB relative luminance (0–1). Used so near-black brand colors stay readable on dark modal backgrounds. */
function hexLuminance(hex: string): number {
  const h = hex.replace('#', '');
  if (h.length !== 6) return 0.5;
  const r = parseInt(h.slice(0, 2), 16) / 255;
  const g = parseInt(h.slice(2, 4), 16) / 255;
  const b = parseInt(h.slice(4, 6), 16) / 255;
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

function newsTileUnselectedLabelColor(brandColor: string, isDark: boolean): string {
  if (!isDark) return brandColor;
  if (hexLuminance(brandColor) < 0.28) return 'rgba(255, 255, 255, 0.88)';
  return brandColor;
}

function newsTileUnselectedBackground(brandColor: string, isDark: boolean): string {
  if (!isDark) return `${brandColor}18`;
  if (hexLuminance(brandColor) < 0.28) return 'rgba(255, 255, 255, 0.08)';
  return `${brandColor}18`;
}

const FeedSourceIconPicker: React.FC<{
  value?: string;
  onChange?: (v: string) => void;
  disabled?: boolean;
  /** Dark modal: lift near-black brand colors so labels/tints are visible. */
  isDark?: boolean;
  /** Rendered directly under the Twitter / YouTube row, above News Sources (e.g. username input). */
  belowPlatforms?: React.ReactNode;
}> = ({ value, onChange, disabled, isDark = false, belowPlatforms }) => {
  const handleSelect = (v: string) => {
    if (!disabled) onChange?.(v);
  };

  return (
    <div>
      {/* Platforms row */}
      <div
        style={{
          display: 'flex',
          gap: 12,
          flexWrap: 'wrap',
          marginBottom: belowPlatforms != null ? 0 : 16,
        }}
      >
        {PLATFORMS.map((p) => {
          const selected = value === p.value;
          const isDisabled = p.disabled || disabled;
          return (
            <Tooltip key={p.value} title={p.disabled ? 'Coming Soon' : p.label}>
              <div
                role="button"
                tabIndex={0}
                onClick={() => !isDisabled && handleSelect(p.value)}
                onKeyDown={(e) => !isDisabled && (e.key === 'Enter' || e.key === ' ') && handleSelect(p.value)}
                style={{
                  ...tileBase,
                  border: `2px solid ${selected ? p.color : 'transparent'}`,
                  backgroundColor: selected ? `${p.color}14` : undefined,
                  color: p.color,
                  opacity: isDisabled ? 0.5 : 1,
                  cursor: isDisabled ? 'not-allowed' : 'pointer',
                }}
              >
                <span style={{ fontSize: 28, marginBottom: 4 }}>{p.icon}</span>
                <span
                  style={{
                    display: 'block',
                    fontSize: 12,
                    opacity: 0.9,
                    width: '100%',
                    minWidth: 0,
                    maxWidth: '100%',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                    textAlign: 'center',
                  }}
                >
                  {p.label}
                </span>
              </div>
            </Tooltip>
          );
        })}
      </div>

      {belowPlatforms != null ? belowPlatforms : null}

      {/* News sources section */}
      <Text
        type="secondary"
        style={{
          display: 'block',
          marginTop: belowPlatforms != null ? 16 : 0,
          marginBottom: 8,
          fontSize: 12,
        }}
      >
        News Sources
      </Text>
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}>
        {NEWS_SOURCES.map((src) => {
          const tileValue = `news:${src.identifier}`;
          const selected = value === tileValue;
          const isSrcDisabled = Boolean(src.disabled) || disabled;
          const bgColor = src.color;
          const textColor = bgColor === '#FFC107' ? '#333' : '#fff';
          const faviconUrl = getNewsSourceFaviconUrl(src.identifier);

          return (
            <Tooltip key={src.identifier} title={isSrcDisabled ? 'Coming soon' : src.label}>
              <div
                role="button"
                tabIndex={isSrcDisabled ? -1 : 0}
                aria-disabled={isSrcDisabled}
                onClick={() => !isSrcDisabled && handleSelect(tileValue)}
                onKeyDown={(e) =>
                  !isSrcDisabled && (e.key === 'Enter' || e.key === ' ') && handleSelect(tileValue)
                }
                style={{
                  ...tileBase,
                  width: 80,
                  height: 56,
                  border: `2px solid ${selected && !isSrcDisabled ? bgColor : 'transparent'}`,
                  backgroundColor: isSrcDisabled
                    ? isDark
                      ? 'rgba(255,255,255,0.04)'
                      : 'rgba(0,0,0,0.04)'
                    : selected
                      ? bgColor
                      : newsTileUnselectedBackground(bgColor, isDark),
                  color: isSrcDisabled
                    ? isDark
                      ? 'rgba(255,255,255,0.35)'
                      : 'rgba(0,0,0,0.35)'
                    : selected
                      ? textColor
                      : newsTileUnselectedLabelColor(bgColor, isDark),
                  fontSize: 11,
                  fontWeight: 600,
                  textAlign: 'center',
                  lineHeight: 1.2,
                  padding: '0 4px',
                  cursor: isSrcDisabled ? 'not-allowed' : 'pointer',
                  opacity: isSrcDisabled ? 0.55 : 1,
                  filter: isSrcDisabled ? 'grayscale(1)' : undefined,
                }}
              >
                {faviconUrl ? (
                  <span
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'center',
                      width: 22,
                      height: 22,
                      marginBottom: 2,
                      borderRadius: 4,
                      background: isSrcDisabled ? 'rgba(255,255,255,0.65)' : '#fff',
                      overflow: 'hidden',
                      flexShrink: 0,
                    }}
                  >
                    <img
                      src={faviconUrl}
                      alt=""
                      width={18}
                      height={18}
                      style={{ objectFit: 'contain' }}
                      loading="lazy"
                      decoding="async"
                    />
                  </span>
                ) : null}
                <span
                  style={{
                    width: '100%',
                    minWidth: 0,
                    maxWidth: '100%',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap',
                    display: 'block',
                  }}
                >
                  {src.label}
                </span>
              </div>
            </Tooltip>
          );
        })}
      </div>
    </div>
  );
};

export default FeedSourceIconPicker;

/**
 * Helper: parse the picker value into channel_type + channel_identifier.
 * - "twitter" → { channelType: "twitter", channelIdentifier: undefined }
 * - "news:Yahoo Finance" → { channelType: "news", channelIdentifier: "Yahoo Finance" }
 */
export function parsePickerValue(pickerValue: string): {
  channelType: string;
  channelIdentifier?: string;
} {
  if (pickerValue.startsWith('news:')) {
    return {
      channelType: 'news',
      channelIdentifier: pickerValue.slice(5),
    };
  }
  return { channelType: pickerValue };
}
