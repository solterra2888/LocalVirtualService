import React from 'react';
import { Tag, Tooltip } from 'antd';
import type { SubscriptionContent } from '../../services/subscriptions';

const compactTagStyle: React.CSSProperties = {
  margin: 0,
  fontSize: 10,
  lineHeight: '16px',
  padding: '0 5px',
  borderRadius: 4,
};

export function getSubstackContentKind(c: SubscriptionContent): 'post' | 'note' | null {
  if (c.channel_type !== 'substack') return null;
  const kind = c.metadata?.content_kind;
  if (kind === 'note' || kind === 'post') return kind;
  return String(c.content_id).startsWith('note_') ? 'note' : 'post';
}

export function isSubstackPaywalled(c: SubscriptionContent): boolean {
  return c.channel_type === 'substack' && Boolean(c.metadata?.is_paywalled);
}

function looksLikeHttpUrl(s: string): boolean {
  return /^https?:\/\//i.test(s.trim());
}

/** 列表/详情作者行：URL 形态的 author_name 回退到 metadata.author 等可读名称。 */
export function getSubstackDisplayAuthor(c: SubscriptionContent): string {
  const md = (c.metadata || {}) as Record<string, unknown>;
  const authorName = (c.author_name || '').trim();
  const metaAuthor = typeof md.author === 'string' ? md.author.trim() : '';
  const pubName = typeof md.publication_name === 'string' ? md.publication_name.trim() : '';
  const handle = typeof md.author_handle === 'string' ? md.author_handle.trim() : '';

  const kind = getSubstackContentKind(c);

  if (kind === 'note') {
    if (authorName && !looksLikeHttpUrl(authorName)) return authorName;
    if (handle) return `@${handle.replace(/^@/, '')}`;
    return metaAuthor || authorName;
  }

  if (authorName && !looksLikeHttpUrl(authorName)) return authorName;
  if (metaAuthor) return metaAuthor;
  if (pubName && !looksLikeHttpUrl(pubName)) return pubName;
  if (handle) return `@${handle.replace(/^@/, '')}`;

  const pubUrl = (c.author_identifier || (typeof md.publication_url === 'string' ? md.publication_url : '')).trim();
  try {
    const host = new URL(pubUrl).hostname;
    if (host.endsWith('.substack.com')) {
      const slug = host.slice(0, -'.substack.com'.length);
      if (slug && slug !== 'www') return slug;
    }
  } catch {
    // ignore invalid URL
  }

  return authorName || pubName || '';
}

export function getSubstackPublicationDisplayName(c: SubscriptionContent): string | null {
  const md = (c.metadata || {}) as Record<string, unknown>;
  const pubName = typeof md.publication_name === 'string' ? md.publication_name.trim() : '';
  if (pubName && !looksLikeHttpUrl(pubName)) return pubName;
  const author = getSubstackDisplayAuthor(c);
  return author || null;
}

/** Feeds 列表卡片作者行（含 YouTube show_name / Substack 回退）。 */
export function getFeedListAuthorLabel(c: SubscriptionContent): string {
  if (c.channel_type === 'youtube' && c.metadata?.show_name) {
    return String(c.metadata.show_name);
  }
  if (c.channel_type === 'substack') {
    return getSubstackDisplayAuthor(c) || c.author_identifier || '';
  }
  return c.author_name || (c.author_identifier ? `@${c.author_identifier}` : '');
}

/** 供 Daily 等仅有 feed 字段的场景复用 Substack 作者展示逻辑。 */
export function getSubstackDisplayAuthorFromItem(item: {
  author_name?: string | null;
  author_identifier?: string;
  content_id?: string;
  metadata?: Record<string, unknown> | null;
}): string {
  return getSubstackDisplayAuthor({
    id: 0,
    channel_type: 'substack',
    content_id: item.content_id ?? '',
    content: '',
    author_name: item.author_name ?? undefined,
    author_identifier: item.author_identifier ?? '',
    metadata: item.metadata ?? {},
    engagement_count: 0,
    view_count: 0,
    is_read: false,
  });
}

export function getSubstackPublicationDisplayNameFromItem(item: {
  author_name?: string | null;
  author_identifier?: string;
  content_id?: string;
  metadata?: Record<string, unknown> | null;
}): string | null {
  return getSubstackPublicationDisplayName({
    id: 0,
    channel_type: 'substack',
    content_id: item.content_id ?? '',
    content: '',
    author_name: item.author_name ?? undefined,
    author_identifier: item.author_identifier ?? '',
    metadata: item.metadata ?? {},
    engagement_count: 0,
    view_count: 0,
    is_read: false,
  });
}

/**
 * Substack 列表卡片徽标：动态(note) / 付费(post)。
 * 与 SubstackOriginalView 详情视图配色一致。
 */
const SubstackFeedBadges: React.FC<{ content: SubscriptionContent }> = ({ content }) => {
  const kind = getSubstackContentKind(content);
  if (!kind) return null;

  if (kind === 'note') {
    const noteType = content.metadata?.note_type;
    return (
      <Tooltip title={noteType ? `Substack 动态 · ${noteType}` : 'Substack 动态'}>
        <Tag color="purple" style={compactTagStyle}>
          动态
        </Tag>
      </Tooltip>
    );
  }

  if (isSubstackPaywalled(content)) {
    return (
      <Tooltip title="付费订阅文章">
        <Tag color="gold" style={compactTagStyle}>
          付费
        </Tag>
      </Tooltip>
    );
  }

  return null;
};

export default SubstackFeedBadges;
