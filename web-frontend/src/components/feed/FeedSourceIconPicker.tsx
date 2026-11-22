import React, { useEffect, useState } from 'react';
import { Tooltip, Typography, Spin } from 'antd';
import { TwitterOutlined, YoutubeOutlined, StarOutlined, CustomerServiceOutlined, MessageOutlined, ReadOutlined } from '@ant-design/icons';
import { NEWS_SOURCES, getNewsSourceFaviconUrl } from './feedChannelConfig';
import { weiboSafeAvatarSrc, weiboSafeImageUrl } from './weiboSafeImage';
import AvatarImage from './AvatarImage';
import api from '../../services/api';

const { Text } = Typography;

/**
 * Picker value encoding:
 * - Platform types: "twitter", "youtube"
 * - News sources: "news:Yahoo Finance", "news:CNBC", etc.
 * - ZSXQ groups: "zsxq:{group_id}"
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

interface ZsxqGroup {
  group_id: string;
  group_name: string;
  avatar?: string | null;
}

interface SubstackPublication {
  publication_url: string;
  publication_name: string;
  avatar?: string | null;
}

const PLATFORMS: PlatformOption[] = [
  { value: 'twitter', label: 'Twitter', icon: <TwitterOutlined />, color: '#1DA1F2' },
  { value: 'youtube', label: 'YouTube', icon: <YoutubeOutlined />, color: '#FF0000' },
  { value: 'podcast', label: 'Podcast', icon: <CustomerServiceOutlined />, color: '#8B5CF6' },
  { value: 'weibo', label: '微博', icon: <MessageOutlined />, color: '#E6162D' },
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

const ZSXQ_COLOR = '#FF6B00';
const SUBSTACK_COLOR = '#FF6719';

const listCardGrid: React.CSSProperties = {
  display: 'grid',
  gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
  gap: 10,
  marginBottom: 16,
};

function listCardChrome(
  selected: boolean,
  accent: string,
  isDark: boolean,
  disabled?: boolean,
): React.CSSProperties {
  const borderColor = selected
    ? accent
    : isDark
      ? 'rgba(255,255,255,0.14)'
      : '#E5E7EB';
  const bgColor = selected
    ? `${accent}14`
    : isDark
      ? 'rgba(255,255,255,0.04)'
      : '#FFFFFF';
  const labelColor = selected
    ? accent
    : isDark
      ? 'rgba(255,255,255,0.88)'
      : '#374151';
  return {
    display: 'flex',
    flexDirection: 'row',
    alignItems: 'center',
    gap: 12,
    minHeight: 52,
    padding: '10px 14px',
    borderRadius: 10,
    border: `${selected ? 2 : 1}px solid ${borderColor}`,
    backgroundColor: bgColor,
    color: labelColor,
    cursor: disabled ? 'not-allowed' : 'pointer',
    transition: 'border-color .2s, background-color .2s',
    userSelect: 'none',
    opacity: disabled ? 0.5 : 1,
    filter: disabled ? 'grayscale(1)' : undefined,
  };
}

const listCardIconBox = (isDark: boolean, accent: string): React.CSSProperties => ({
  display: 'inline-flex',
  alignItems: 'center',
  justifyContent: 'center',
  width: 32,
  height: 32,
  borderRadius: 6,
  flexShrink: 0,
  background: isDark ? 'rgba(255,255,255,0.08)' : '#F3F4F6',
  color: accent,
  fontSize: 16,
  overflow: 'hidden',
});

const listCardLabel: React.CSSProperties = {
  display: 'block',
  fontSize: 13,
  fontWeight: 500,
  lineHeight: 1.3,
  minWidth: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
};

const FeedSourceIconPicker: React.FC<{
  value?: string;
  onChange?: (v: string) => void;
  disabled?: boolean;
  /** Dark modal: lift near-black brand colors so labels/tints are visible. */
  isDark?: boolean;
  /** Rendered directly under the Twitter / YouTube row, above News Sources (e.g. username input). */
  belowPlatforms?: React.ReactNode;
}> = ({ value, onChange, disabled, isDark = false, belowPlatforms }) => {
  const [zsxqGroups, setZsxqGroups] = useState<ZsxqGroup[]>([]);
  const [zsxqLoading, setZsxqLoading] = useState(false);
  const [substackPubs, setSubstackPubs] = useState<SubstackPublication[]>([]);
  const [substackLoading, setSubstackLoading] = useState(false);

  useEffect(() => {
    setZsxqLoading(true);
    api
      .get('/subscriptions/zsxq-groups')
      .then((res) => {
        const data = res.data ?? res;
        const groups: ZsxqGroup[] = Array.isArray(data) ? data : [];
        setZsxqGroups(groups);
        groups.forEach((grp) => {
          const url = weiboSafeImageUrl(grp.avatar);
          if (!url) return;
          const img = new Image();
          img.decoding = 'async';
          img.src = url;
        });
      })
      .catch(() => setZsxqGroups([]))
      .finally(() => setZsxqLoading(false));

    setSubstackLoading(true);
    api
      .get('/subscriptions/substack-publications')
      .then((res) => {
        const data = res.data ?? res;
        const pubs: SubstackPublication[] = Array.isArray(data) ? data : [];
        setSubstackPubs(pubs);
        // Preload avatar images so Add New Feed cards paint without pop-in
        pubs.forEach((pub) => {
          const url = (pub.avatar || '').trim();
          if (!url) return;
          const img = new Image();
          img.decoding = 'async';
          img.src = url;
        });
      })
      .catch(() => setSubstackPubs([]))
      .finally(() => setSubstackLoading(false));
  }, []);

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

      {/* 知识星球 section */}
      {zsxqLoading ? (
        <div style={{ marginTop: 16, marginBottom: 8 }}>
          <Spin size="small" /> <Text type="secondary" style={{ fontSize: 12, marginLeft: 8 }}>加载知识星球列表…</Text>
        </div>
      ) : zsxqGroups.length > 0 ? (
        <>
          <Text
            type="secondary"
            style={{
              display: 'block',
              marginTop: belowPlatforms != null ? 16 : 0,
              marginBottom: 8,
              fontSize: 12,
            }}
          >
            知识星球
          </Text>
          <div style={listCardGrid}>
            {zsxqGroups.map((grp) => {
              const tileValue = `zsxq:${grp.group_id}`;
              const selected = value === tileValue;
              const avatarUrl = weiboSafeAvatarSrc(grp.avatar);
              return (
                <Tooltip key={grp.group_id} title={grp.group_name}>
                  <div
                    role="button"
                    tabIndex={disabled ? -1 : 0}
                    onClick={() => !disabled && handleSelect(tileValue)}
                    onKeyDown={(e) => !disabled && (e.key === 'Enter' || e.key === ' ') && handleSelect(tileValue)}
                    style={listCardChrome(selected, ZSXQ_COLOR, isDark, disabled)}
                  >
                    {avatarUrl ? (
                      <AvatarImage
                        src={avatarUrl}
                        isDark={isDark}
                        fallback={
                          <span style={listCardIconBox(isDark, ZSXQ_COLOR)}>
                            <StarOutlined />
                          </span>
                        }
                      />
                    ) : (
                      <span style={listCardIconBox(isDark, ZSXQ_COLOR)}>
                        <StarOutlined />
                      </span>
                    )}
                    <span style={listCardLabel}>{grp.group_name}</span>
                  </div>
                </Tooltip>
              );
            })}
          </div>
        </>
      ) : null}

      {/* Substack publications section */}
      {substackLoading ? (
        <div style={{ marginTop: 16, marginBottom: 8 }}>
          <Spin size="small" /> <Text type="secondary" style={{ fontSize: 12, marginLeft: 8 }}>加载 Substack 列表…</Text>
        </div>
      ) : substackPubs.length > 0 ? (
        <>
          <Text
            type="secondary"
            style={{
              display: 'block',
              marginTop: 16,
              marginBottom: 8,
              fontSize: 12,
            }}
          >
            Substack
          </Text>
          <div style={listCardGrid}>
            {substackPubs.map((pub) => {
              const tileValue = `substack:${pub.publication_url}`;
              const selected = value === tileValue;
              const avatarUrl = pub.avatar || undefined;
              return (
                <Tooltip key={pub.publication_url} title={`${pub.publication_name} — ${pub.publication_url}`}>
                  <div
                    role="button"
                    tabIndex={disabled ? -1 : 0}
                    onClick={() => !disabled && handleSelect(tileValue)}
                    onKeyDown={(e) => !disabled && (e.key === 'Enter' || e.key === ' ') && handleSelect(tileValue)}
                    style={listCardChrome(selected, SUBSTACK_COLOR, isDark, disabled)}
                  >
                    {avatarUrl ? (
                      <AvatarImage
                        src={avatarUrl}
                        isDark={isDark}
                        fallback={
                          <span style={listCardIconBox(isDark, SUBSTACK_COLOR)}>
                            <ReadOutlined />
                          </span>
                        }
                      />
                    ) : (
                      <span style={listCardIconBox(isDark, SUBSTACK_COLOR)}>
                        <ReadOutlined />
                      </span>
                    )}
                    <span style={listCardLabel}>
                      {pub.publication_name || pub.publication_url}
                    </span>
                  </div>
                </Tooltip>
              );
            })}
          </div>
        </>
      ) : null}

      {/* News sources section */}
      <Text
        type="secondary"
        style={{
          display: 'block',
          marginTop: belowPlatforms != null || zsxqGroups.length > 0 ? 0 : 0,
          marginBottom: 8,
          fontSize: 12,
        }}
      >
        News Sources
      </Text>
      <div style={{ ...listCardGrid, marginBottom: 0 }}>
        {NEWS_SOURCES.map((src) => {
          const tileValue = `news:${src.identifier}`;
          const selected = value === tileValue;
          const isSrcDisabled = Boolean(src.disabled) || disabled;
          const accent = src.color;
          const faviconUrl = getNewsSourceFaviconUrl(src.identifier);

          return (
              <Tooltip key={src.identifier} title={isSrcDisabled ? 'Coming soon' : (src.note || src.label)}>
              <div
                role="button"
                tabIndex={isSrcDisabled ? -1 : 0}
                aria-disabled={isSrcDisabled}
                onClick={() => !isSrcDisabled && handleSelect(tileValue)}
                onKeyDown={(e) =>
                  !isSrcDisabled && (e.key === 'Enter' || e.key === ' ') && handleSelect(tileValue)
                }
                style={listCardChrome(selected && !isSrcDisabled, accent, isDark, isSrcDisabled)}
              >
                {faviconUrl ? (
                  <span style={listCardIconBox(isDark, accent)}>
                    <img
                      src={faviconUrl}
                      alt=""
                      width={20}
                      height={20}
                      style={{ objectFit: 'contain' }}
                      loading="lazy"
                      decoding="async"
                    />
                  </span>
                ) : (
                  <span style={listCardIconBox(isDark, accent)}>
                    <ReadOutlined />
                  </span>
                )}
                <span style={listCardLabel}>{src.label}</span>
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
 * - "zsxq:15555452148452" → { channelType: "zhishixingqiu", channelIdentifier: "15555452148452" }
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
  if (pickerValue.startsWith('zsxq:')) {
    return {
      channelType: 'zhishixingqiu',
      channelIdentifier: pickerValue.slice(5),
    };
  }
  if (pickerValue.startsWith('substack:')) {
    return {
      channelType: 'substack',
      channelIdentifier: pickerValue.slice(9),
    };
  }
  return { channelType: pickerValue };
}
