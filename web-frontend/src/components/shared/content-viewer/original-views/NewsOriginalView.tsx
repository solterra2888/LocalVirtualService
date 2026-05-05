/**
 * Shared News original content renderer.
 *
 * Renders article body, tickers, categories, sentiment, and external read link.
 * Used by:
 *   · Daily  → DailyOriginalView (feed/news)
 *   · Feeds  → FeedArticleBody (channel_type === 'news')
 *   · (future) News module → same layout when AI Summary/Analysis is added
 */

import React from 'react';
import { Space, Tag, Typography } from 'antd';
import { LinkOutlined } from '@ant-design/icons';
import { DAILY_THEME_COLORS } from '../../../daily/dailyTheme';
import ReportTickerTags from '../../../ReportTickerTags';
import { aiTagChipStyle } from '../../aiTagChipStyles';

const { Text } = Typography;

export interface NewsOriginalViewProps {
  title?: string | null;
  content?: string | null;
  url?: string | null;
  symbols?: string[];
  categories?: string[];
  sentiment?: { polarity?: number } | null;
  themeMode: 'light' | 'dark';
}

const NewsOriginalView: React.FC<NewsOriginalViewProps> = ({
  title,
  content,
  url,
  symbols,
  categories,
  sentiment,
  themeMode,
}) => {
  const colors = DAILY_THEME_COLORS[themeMode];
  const polarity = sentiment?.polarity;

  return (
    <Space direction="vertical" size={12} style={{ width: '100%' }}>
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
          Article body not stored — read on the source.
        </Text>
      )}

      {symbols && symbols.length > 0 && (
        <div style={{ display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: '6px 8px' }}>
          <Text type="secondary" style={{ fontSize: 12, flexShrink: 0 }}>
            Tickers:
          </Text>
          <ReportTickerTags
            tickers={symbols}
            tagBordered={false}
            tagStyle={aiTagChipStyle('ticker', themeMode)}
            overflowTagStyle={aiTagChipStyle('overflow', themeMode)}
            maxVisible={10}
          />
        </div>
      )}

      {categories && categories.length > 0 && (
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
          <Text type="secondary" style={{ fontSize: 12, flexShrink: 0 }}>Tags:</Text>
          {categories.map((c) => (
            <Tag key={c} bordered={false} style={aiTagChipStyle('concept', themeMode)}>
              {c}
            </Tag>
          ))}
        </div>
      )}

      {typeof polarity === 'number' && (
        <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
          <Text type="secondary" style={{ fontSize: 12 }}>Sentiment:</Text>
          <Tag
            color={polarity > 0.1 ? 'green' : polarity < -0.1 ? 'red' : 'default'}
            style={{ fontSize: 11, margin: 0 }}
          >
            {polarity > 0.1 ? 'Positive' : polarity < -0.1 ? 'Negative' : 'Neutral'} (
            {polarity.toFixed(2)})
          </Tag>
        </div>
      )}

      {url && (
        <a
          href={url}
          target="_blank"
          rel="noopener noreferrer"
          style={{ color: colors.accent, fontSize: 13, display: 'inline-flex', alignItems: 'center', gap: 6 }}
        >
          <LinkOutlined /> Read original article
        </a>
      )}
    </Space>
  );
};

export default NewsOriginalView;
