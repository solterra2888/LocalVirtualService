/**
 * Daily 模块 — 来源徽章 (Source Badge).
 *
 * 显示在卡片右上角的小芯片, 标识每条卡片的数据来源 (DOC / WEB / X / YT / NEWS …).
 * 设计参考 AI Notes Daily 的 Manage 视图右侧 GDOC / WEB 标签, 颜色与配色对齐
 * 当前系统的浅 / 深主题.
 *
 * 配色与 label 在 sourceBadgeConfig.ts 中, 共 9 种.
 */

import React from 'react';
import { Tooltip } from 'antd';
import type { DailySourceSubtype } from '../../services/daily';
import { SOURCE_BADGE_STYLES } from './sourceBadgeConfig';

interface SourceBadgeProps {
  subtype: DailySourceSubtype;
  themeMode: 'light' | 'dark';
  /** 紧凑模式: 字号小一号 (列表稠密时使用) */
  compact?: boolean;
  /** 是否可点击 (用于来源筛选 chip), 默认 false */
  clickable?: boolean;
  /** 是否处于"已选中"高亮状态 (筛选 chip 用) */
  active?: boolean;
  /** 计数后缀, 例如 "DOC · 27", 用于过滤栏 */
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

  // 卡片徽章（非 clickable）始终显示前景色半透明边框，保证 chip 形状可辨。
  // 过滤 chip（clickable）激活时显示实色边框，未激活时隐藏边框（靠背景色区分）。
  const borderStyle = active
    ? `1.5px solid ${palette.fg}`
    : clickable
    ? '1px solid transparent'
    : `1px solid ${palette.fg}40`; // 25 % 透明前景色 — 卡片徽章的轮廓线

  const baseStyle: React.CSSProperties = {
    display: 'inline-flex',
    alignItems: 'center',
    gap: 4,
    padding: compact ? '1px 6px' : '2px 8px',
    fontSize: compact ? 10 : 11,
    fontWeight: 600,
    letterSpacing: 0.4,
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
      {config.label}
      {typeof count === 'number' && (
        <span
          style={{
            opacity: 0.75,
            fontWeight: 500,
            fontSize: compact ? 10 : 11,
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
