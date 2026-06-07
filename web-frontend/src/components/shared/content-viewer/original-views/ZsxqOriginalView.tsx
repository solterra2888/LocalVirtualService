/**
 * Shared 知识星球 original content renderer.
 *
 * Used by Daily → DailyOriginalView (feed/zhishixingqiu).
 */

import React from 'react';
import { Space, Typography } from 'antd';
import { LinkOutlined } from '@ant-design/icons';
import { DAILY_THEME_COLORS } from '../../../daily/dailyTheme';

const { Text } = Typography;

export interface ZsxqOriginalViewProps {
  title?: string | null;
  content?: string | null;
  url?: string | null;
  media_urls?: string[] | null;
  post_author?: string | null;
  group_name?: string | null;
  files?: string[];
  themeMode: 'light' | 'dark';
}

const ZsxqOriginalView: React.FC<ZsxqOriginalViewProps> = ({
  title,
  content,
  url,
  media_urls,
  post_author,
  group_name,
  files,
  themeMode,
}) => {
  const colors = DAILY_THEME_COLORS[themeMode];
  const images = (media_urls ?? []).filter(Boolean);

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
      {(group_name || post_author) && (
        <Text type="secondary" style={{ fontSize: 12, display: 'block' }}>
          {[group_name, post_author].filter(Boolean).join(' · ')}
        </Text>
      )}

      {title && (
        <Text
          strong
          style={{
            color: colors.text,
            fontSize: 18,
            lineHeight: '1.5',
            display: 'block',
          }}
        >
          {title}
        </Text>
      )}

      {content ? (
        <Text
          style={{
            color: colors.textSecondary,
            fontSize: 14,
            whiteSpace: 'pre-wrap',
            lineHeight: '1.75',
            wordBreak: 'break-word',
            display: 'block',
          }}
        >
          {content}
        </Text>
      ) : (
        <Text type="secondary" style={{ fontSize: 12 }}>
          正文未存储 — 请在知识星球查看原文。
        </Text>
      )}

      {images.length > 0 && (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8 }}>
          {images.map((imgUrl, idx) => (
            <a
              key={`${imgUrl}-${idx}`}
              href={imgUrl}
              target="_blank"
              rel="noopener noreferrer"
            >
              <img
                src={imgUrl}
                alt=""
                style={{
                  maxWidth: '100%',
                  maxHeight: 320,
                  borderRadius: 8,
                  border: `1px solid ${colors.divider}`,
                }}
              />
            </a>
          ))}
        </div>
      )}

      {files && files.length > 0 && (
        <div>
          <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>
            附件
          </Text>
          <Space direction="vertical" size={4}>
            {files.map((name) => (
              <Text key={name} style={{ fontSize: 13, color: colors.textSecondary }}>
                {name}
              </Text>
            ))}
          </Space>
        </div>
      )}

      {url && (
        <a
          href={url}
          target="_blank"
          rel="noopener noreferrer"
          style={{ color: colors.accent, fontSize: 13 }}
        >
          <LinkOutlined /> 在知识星球查看
        </a>
      )}
    </Space>
  );
};

export default ZsxqOriginalView;
