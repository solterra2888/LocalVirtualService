/**
 * 文件类型图标工具（按扩展名映射 Ant Design 图标 + 颜色）。
 *
 * 用于知识星球附件等"按扩展名展示文件"的场景。
 * FilesPage 已有自己的 getFileTypeIconDisplay（按 type 字符串），此处不强行统一，
 * 避免改动 7699 行的 FilesPage；未来可逐步收敛到此工具。
 */
import React from 'react';
import {
  FilePdfOutlined,
  FileWordOutlined,
  FileExcelOutlined,
  FilePptOutlined,
  SoundOutlined,
  FileImageOutlined,
  VideoCameraOutlined,
  FileZipOutlined,
  FileTextOutlined,
  FileOutlined,
} from '@ant-design/icons';

export interface FileIconInfo {
  tooltip: string;
  icon: React.ReactElement;
  color: string;
}

const EXT_MAP: Record<string, FileIconInfo> = {
  pdf: { tooltip: 'PDF', icon: <FilePdfOutlined />, color: '#f5222d' },
  doc: { tooltip: 'Word', icon: <FileWordOutlined />, color: '#1890ff' },
  docx: { tooltip: 'Word', icon: <FileWordOutlined />, color: '#1890ff' },
  xls: { tooltip: 'Excel', icon: <FileExcelOutlined />, color: '#52c41a' },
  xlsx: { tooltip: 'Excel', icon: <FileExcelOutlined />, color: '#52c41a' },
  ppt: { tooltip: 'PPT', icon: <FilePptOutlined />, color: '#fa541c' },
  pptx: { tooltip: 'PPT', icon: <FilePptOutlined />, color: '#fa541c' },
  // 音频
  mp3: { tooltip: 'Audio', icon: <SoundOutlined />, color: '#722ed1' },
  m4a: { tooltip: 'Audio', icon: <SoundOutlined />, color: '#722ed1' },
  wav: { tooltip: 'Audio', icon: <SoundOutlined />, color: '#722ed1' },
  aac: { tooltip: 'Audio', icon: <SoundOutlined />, color: '#722ed1' },
  flac: { tooltip: 'Audio', icon: <SoundOutlined />, color: '#722ed1' },
  ogg: { tooltip: 'Audio', icon: <SoundOutlined />, color: '#722ed1' },
  // 视频
  mp4: { tooltip: 'Video', icon: <VideoCameraOutlined />, color: '#ff4d4f' },
  mov: { tooltip: 'Video', icon: <VideoCameraOutlined />, color: '#ff4d4f' },
  mkv: { tooltip: 'Video', icon: <VideoCameraOutlined />, color: '#ff4d4f' },
  avi: { tooltip: 'Video', icon: <VideoCameraOutlined />, color: '#ff4d4f' },
  // 图片
  jpg: { tooltip: 'Image', icon: <FileImageOutlined />, color: '#fa8c16' },
  jpeg: { tooltip: 'Image', icon: <FileImageOutlined />, color: '#fa8c16' },
  png: { tooltip: 'Image', icon: <FileImageOutlined />, color: '#fa8c16' },
  gif: { tooltip: 'Image', icon: <FileImageOutlined />, color: '#fa8c16' },
  bmp: { tooltip: 'Image', icon: <FileImageOutlined />, color: '#fa8c16' },
  webp: { tooltip: 'Image', icon: <FileImageOutlined />, color: '#fa8c16' },
  svg: { tooltip: 'Image', icon: <FileImageOutlined />, color: '#fa8c16' },
  // 压缩包
  zip: { tooltip: 'Archive', icon: <FileZipOutlined />, color: '#13c2c2' },
  rar: { tooltip: 'Archive', icon: <FileZipOutlined />, color: '#13c2c2' },
  '7z': { tooltip: 'Archive', icon: <FileZipOutlined />, color: '#13c2c2' },
  // 文本
  txt: { tooltip: 'Text', icon: <FileTextOutlined />, color: '#2f54eb' },
  csv: { tooltip: 'CSV', icon: <FileTextOutlined />, color: '#2f54eb' },
  md: { tooltip: 'Markdown', icon: <FileTextOutlined />, color: '#2f54eb' },
  rtf: { tooltip: 'RTF', icon: <FileTextOutlined />, color: '#2f54eb' },
};

export function getFileIconByExt(ext?: string): FileIconInfo {
  const e = (ext || '').toLowerCase().replace(/^\./, '');
  return EXT_MAP[e] || { tooltip: 'File', icon: <FileOutlined />, color: '#8c8c8c' };
}

/** 字节数格式化为人类可读（KB/MB/GB）。 */
export function formatFileSize(bytes?: number): string {
  if (!bytes || bytes <= 0) return '';
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
  return `${(bytes / 1024 / 1024 / 1024).toFixed(2)} GB`;
}
