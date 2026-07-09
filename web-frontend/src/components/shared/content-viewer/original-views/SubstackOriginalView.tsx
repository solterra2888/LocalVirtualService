import React from 'react';
import { Typography, Tag, Space } from 'antd';

const { Title, Paragraph, Text } = Typography;

interface SubstackOriginalViewProps {
  title?: string | null;
  content?: string | null;
  url?: string | null;
  publicationName?: string | null;
  authorName?: string | null;
  contentKind?: 'post' | 'note' | string | null;
  isPaywalled?: boolean | null;
  noteType?: string | null;
  authorHandle?: string | null;
  themeMode?: 'light' | 'dark';
}

/**
 * Substack 原文视图：
 *  - post（文章）：渲染 title + HTML 正文 + paywall 徽标
 *  - note（动态）：渲染 body_text 纯文本 + 作者 + note_type 徽标
 */
const SubstackOriginalView: React.FC<SubstackOriginalViewProps> = ({
  title,
  content,
  url,
  publicationName,
  authorName,
  contentKind,
  isPaywalled,
  noteType,
  authorHandle,
  themeMode,
}) => {
  const isDark = themeMode === 'dark';
  const isNote = contentKind === 'note';
  const textColor = isDark ? 'rgba(255,255,255,0.85)' : undefined;
  const subColor = isDark ? 'rgba(255,255,255,0.45)' : undefined;

  return (
    <div>
      <Space size={6} wrap style={{ marginBottom: 8 }}>
        {publicationName && <Tag color="orange">{publicationName}</Tag>}
        {isNote ? (
          <Tag color="purple">动态</Tag>
        ) : (
          <Tag color="blue">文章</Tag>
        )}
        {isPaywalled && <Tag color="gold">付费</Tag>}
        {noteType && isNote && <Tag>{noteType}</Tag>}
      </Space>

      {!isNote && title && (
        <Title level={5} style={{ color: textColor, marginBottom: 8 }}>{title}</Title>
      )}

      <Paragraph style={{ color: textColor, whiteSpace: 'pre-wrap' }}>
        {isNote ? (
          content
        ) : (
          <div
            // Substack 文章正文为 HTML（来自 post.get_content() 的 body_html）
            dangerouslySetInnerHTML={{ __html: content || '' }}
          />
        )}
      </Paragraph>

      {(authorName || authorHandle) && (
        <div style={{ marginBottom: 4 }}>
          <Text type="secondary" style={{ color: subColor }}>
            {authorHandle ? `@${authorHandle}` : authorName}
          </Text>
        </div>
      )}

      {url && (
        <div>
          <a href={url} target="_blank" rel="noreferrer">
            原文链接
          </a>
        </div>
      )}
    </div>
  );
};

export default SubstackOriginalView;
