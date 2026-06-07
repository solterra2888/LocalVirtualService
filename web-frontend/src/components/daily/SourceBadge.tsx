/**
 * Daily 模块 — 来源徽章 (Source Badge).
 *
 * 显示在卡片右上角及筛选 chip 行中，带品牌图标标识每条卡片的数据来源。
 * 设计参考 Feeds 模块导航栏的频道类型筛选 (TwitterOutlined / YoutubeOutlined / ReadOutlined)，
 * 还原 Twitter、YouTube 和各类新闻媒体的品牌标签。
 *
 * 配色与 label 在 sourceBadgeConfig.ts 中，共 9 种。
 */

import React from 'react';
import { Tooltip } from 'antd';
import {
  TwitterOutlined,
  YoutubeOutlined,
  ReadOutlined,
  GlobalOutlined,
  FileTextOutlined,
  PictureOutlined,
  AudioOutlined,
  EditOutlined,
  AppleOutlined,
  StarOutlined,
} from '@ant-design/icons';
import type { DailySourceSubtype } from '../../services/daily';
import { SOURCE_BADGE_STYLES } from './sourceBadgeConfig';

/** 品牌 / 类型图标映射 — 参考 Feeds 模块的 feedChannelConfig */
const SUBTYPE_ICONS: Record<DailySourceSubtype, React.ReactNode> = {
  twitter:      <TwitterOutlined />,
  youtube_feed: <YoutubeOutlined />,
  youtube:      <YoutubeOutlined />,
  news:         <ReadOutlined />,
  web:          <GlobalOutlined />,
  document:     <FileTextOutlined />,
  image:        <PictureOutlined />,
  audio:        <AudioOutlined />,
  text:         <EditOutlined />,
  podcast:      <AppleOutlined />,
  zhishixingqiu: <StarOutlined />,
};

interface SourceBadgeProps {
  subtype: DailySourceSubtype;
  themeMode: 'light' | 'dark';
  /** 紧凑模式: 字号小一号 (列表稠密时使用) */
  compact?: boolean;
  /** 纯图标模式: 只显示图标, 隐藏文字 (卡片右上角使用) */
  iconOnly?: boolean;
  /** 是否可点击 (用于来源筛选 chip), 默认 false */
  clickable?: boolean;
  /** 是否处于"已选中"高亮状态 (筛选 chip 用) */
  active?: boolean;
  /** 计数后缀, 例如 "Twitter · 27", 用于过滤栏 */
  count?: number;
  onClick?: () => void;
  style?: React.CSSProperties;
  /** Tooltip 自定义文本, 默认用 fullName */
  tooltipTitle?: React.ReactNode;
}

const SourceBadge: React.FC<SourceBadgeProps> = ({
  subtype,
  themeMode,
  compact,
  iconOnly,
  clickable,
  active,
  count,
  onClick,
  style,
  tooltipTitle,
}) => {
  const config = SOURCE_BADGE_STYLES[subtype];
  if (!config) return null;
  const palette = themeMode === 'dark' ? config.dark : config.light;
  const icon = SUBTYPE_ICONS[subtype];

  const borderStyle = active
    ? `1.5px solid ${palette.fg}`
    : clickable
    ? '1px solid transparent'
    : `1px solid ${palette.fg}40`;

  const iconSize = compact ? 10 : 11;

  const baseStyle: React.CSSProperties = iconOnly
    ? {
        display: 'inline-flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontSize: iconSize,
        color: palette.fg,
        cursor: clickable ? 'pointer' : 'default',
        userSelect: 'none',
        flexShrink: 0,
        ...style,
      }
    : {
        display: 'inline-flex',
        alignItems: 'center',
        gap: compact ? 3 : 4,
        padding: compact ? '1px 6px' : '2px 8px',
        fontSize: iconSize,
        fontWeight: 600,
        letterSpacing: 0.3,
        lineHeight: 1.4,
        borderRadius: 4,
        background: palette.bg,
        color: palette.fg,
        cursor: clickable ? 'pointer' : 'default',
        userSelect: 'none',
        border: borderStyle,
        transition: 'all 0.15s',
        whiteSpace: 'nowrap',
        ...style,
      };

  const inner = (
    <span
      role={clickable ? 'button' : undefined}
      tabIndex={clickable ? 0 : -1}
      onClick={onClick}
      onKeyDown={
        clickable
          ? (e) => {
              if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                onClick?.();
              }
            }
          : undefined
      }
      style={baseStyle}
    >
      {icon && (
        <span style={{ fontSize: iconSize, lineHeight: 1, display: 'inline-flex', alignItems: 'center' }}>
          {icon}
        </span>
      )}
      {!iconOnly && config.label}
      {!iconOnly && typeof count === 'number' && (
        <span
          style={{
            opacity: 0.75,
            fontWeight: 500,
            fontSize: iconSize,
          }}
        >
          · {count}
        </span>
      )}
    </span>
  );

  return (
    <Tooltip title={tooltipTitle ?? config.fullName} mouseEnterDelay={0.4}>
      {inner}
    </Tooltip>
  );
};

export default SourceBadge;
