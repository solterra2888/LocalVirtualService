/* =========================================================================
   Form 13F 桌面/移动端共用模块（仿 config/schedulerSources.tsx 的防漂移先例）:
   主题色板、金额/季度格式化、New/Put/Call 标签胶囊、Put/Call 判定。
   两端页面从这里 import，语义改动只改一处。
   ========================================================================= */
import React from 'react';
import { Tag } from 'antd';
import { aiTagChipStyle, type AiTagChipVariant } from '../shared/aiTagChipStyles';

// ===== 主题色板（桌面 Form13FPage / 移动 MobileForm13FPage 同源）=====

export const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    barBg: 'rgba(22, 119, 255, 0.08)',
    posColor: '#cf1322',
    negColor: '#389e0d',
  },
  dark: {
    pageBg: '#1a202c',
    cardBg: '#1a2234',
    cardBorder: '#364152',
    text: '#e0e6f1',
    textSecondary: 'rgba(255, 255, 255, 0.5)',
    barBg: 'rgba(22, 119, 255, 0.15)',
    posColor: '#ff7875',
    negColor: '#95de64',
  },
} as const;

/** 主题色板结构（light/dark 两种取值；放宽为 string，避免 as const 字面量类型互斥） */
export interface Form13fThemeColors {
  pageBg: string;
  cardBg: string;
  cardBorder: string;
  text: string;
  textSecondary: string;
  barBg: string;
  posColor: string;
  negColor: string;
}

// ===== 格式化 =====

export const fmtMoney = (v?: number | null): string => {
  if (v === null || v === undefined) return '—';
  const a = Math.abs(v);
  if (a >= 1e9) return (v / 1e9).toFixed(2) + 'B';
  if (a >= 1e6) return (v / 1e6).toFixed(1) + 'M';
  if (a >= 1e3) return (v / 1e3).toFixed(0) + 'K';
  return String(v);
};

export const fmtFull = (v?: number | null): string =>
  v === null || v === undefined ? '—' : v.toLocaleString('en-US');

export const fmtQuarter = (q: string): string =>
  q.replace(/^(\d{4})Q(\d)$/, '$1 Q$2');

// ===== Put/Call 判定（与后端 13F 信息表 PutCall 字段口径一致）=====

export const isPut = (putcall?: string | null) =>
  putcall === 'Put' || putcall === 'P';
export const isCall = (putcall?: string | null) =>
  putcall === 'Call' || putcall === 'C';

// ===== 持仓行内 New/Put/Call 标签：全站标签胶囊规范（aiTagChipStyle，见 UI_STYLE_SPEC.md 二）=====

export type StatusKind = 'new' | 'put' | 'call';

const STATUS_COPY: Record<StatusKind, string> = {
  new: 'New',
  put: 'Put',
  call: 'Call',
};

const STATUS_VARIANT: Record<StatusKind, AiTagChipVariant> = {
  new: 'signal-neutral',   // 新建持仓 = 中性状态高亮
  put: 'signal-bearish',   // Put = 看跌工具
  call: 'signal-bullish',  // Call = 看涨工具
};

export const StatusLabel: React.FC<{
  kind: StatusKind;
  themeMode: 'light' | 'dark';
}> = ({ kind, themeMode }) => (
  <Tag bordered={false} style={aiTagChipStyle(STATUS_VARIANT[kind], themeMode)}>
    {STATUS_COPY[kind]}
  </Tag>
);
