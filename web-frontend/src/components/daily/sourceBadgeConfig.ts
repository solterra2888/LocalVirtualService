/**
 * Daily 模块 — 来源徽章 (Source Badge) 配色与文案配置.
 *
 * 设计原则:
 *  - 9 种来源子类型 (与 backend/api/daily.py 的 SourceSubtype 一一对应)
 *  - 短代号 (3-5 字母, 与 AI Notes Daily 的 GDOC/WEB 风格一致)
 *  - 浅 / 深主题各一组色彩, 与系统主色 (#1890ff) 区分但不冲突
 *  - badge 设计为右上角小芯片, 不影响标题和标签的扫读节奏
 */

import type { DailySourceSubtype } from '../../services/daily';

export interface SourceBadgeStyle {
  /** 短代号, 显示在徽章上 (大写) */
  label: string;
  /** 完整名称, 用作 tooltip / a11y title */
  fullName: string;
  /** 浅色主题色 [背景, 文字] */
  light: { bg: string; fg: string };
  /** 深色主题色 [背景, 文字] */
  dark: { bg: string; fg: string };
}

/**
 * 注意: image / youtube_feed 的徽章 label 与 youtube / image 共用约定:
 *  - Upload `youtube` 与 Feed `youtube_feed` 共用 `YT` 文案 (用户视角都是 YouTube),
 *    内部 source_subtype 不同保证点击跳转正确 (Upload → /files, Feed → /feeds)
 */
export const SOURCE_BADGE_STYLES: Record<DailySourceSubtype, SourceBadgeStyle> = {
  // ─── Upload Files ───
  document: {
    label: 'Doc',
    fullName: 'Documents (PDF / Word / Excel / CSV)',
    light: { bg: '#e6f4ff', fg: '#0958d9' },
    dark: { bg: 'rgba(24, 144, 255, 0.18)', fg: '#69b1ff' },
  },
  image: {
    label: 'Image',
    fullName: 'Images',
    light: { bg: '#f9f0ff', fg: '#531dab' },
    dark: { bg: 'rgba(114, 46, 209, 0.22)', fg: '#b37feb' },
  },
  audio: {
    label: 'Audio',
    fullName: 'Audio (ASR Transcribed)',
    light: { bg: '#fff7e6', fg: '#ad4e00' },
    dark: { bg: 'rgba(250, 140, 22, 0.20)', fg: '#ffc069' },
  },
  youtube: {
    label: 'YT Link',
    fullName: 'YouTube Link (Upload)',
    light: { bg: '#fff1f0', fg: '#cf1322' },
    dark: { bg: 'rgba(245, 34, 45, 0.18)', fg: '#ff7875' },
  },
  web: {
    label: 'Web',
    fullName: 'Web Links',
    light: { bg: '#f6ffed', fg: '#389e0d' },
    dark: { bg: 'rgba(82, 196, 26, 0.18)', fg: '#95de64' },
  },
  text: {
    label: 'Note',
    fullName: 'Notes & Thoughts',
    light: { bg: '#e6fffb', fg: '#08979c' },
    dark: { bg: 'rgba(19, 194, 194, 0.18)', fg: '#5cdbd3' },
  },

  // ─── Feeds ───
  twitter: {
    label: 'Twitter',
    fullName: 'Twitter / X',
    light: { bg: '#e9e9e9', fg: '#262626' },
    dark:  { bg: 'rgba(255,255,255,0.14)', fg: '#e8e8e8' },
  },
  news: {
    label: 'News',
    fullName: 'News Feed',
    light: { bg: '#fffbe6', fg: '#876800' },
    dark: { bg: 'rgba(212, 177, 6, 0.20)', fg: '#ffd666' },
  },
  youtube_feed: {
    label: 'YT Feed',
    fullName: 'YouTube (Subscription)',
    light: { bg: '#fff1f0', fg: '#cf1322' },
    dark: { bg: 'rgba(245, 34, 45, 0.18)', fg: '#ff7875' },
  },
};

/**
 * 显示顺序: 先 Upload 各类, 再 Feed 各类 (与 NavBar Sources / Feeds 顺序对齐)
 */
export const SOURCE_BADGE_DISPLAY_ORDER: DailySourceSubtype[] = [
  'document',
  'image',
  'audio',
  'youtube',
  'web',
  'text',
  'twitter',
  'youtube_feed',
  'news',
];
