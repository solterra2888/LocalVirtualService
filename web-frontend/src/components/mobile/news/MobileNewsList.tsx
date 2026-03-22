import React, { useState, useMemo } from 'react';
import { List, Typography, Tag, Space, Modal } from 'antd';
import { CalendarOutlined, CloseOutlined } from '@ant-design/icons';
import type { DashboardNewsItem } from '../../../services/dashboard';
import MarkdownRenderer from '../../MarkdownRenderer';
import { 
  getPreferredAnalysisContent, 
  hasAnalysisForTemplate,
  getAnalysisContent
} from '../../../utils/newsAnalysisHelper';
import { useDashboardSettings } from '../../../contexts/DashboardSettingsContext';
import './MobileNewsList.css';

const { Text, Paragraph } = Typography;

interface MobileNewsListProps {
  news: DashboardNewsItem[];
}

/**
 * 移动端新闻列表组件
 * 显示新闻卡片，点击可查看详情
 */
const MobileNewsList: React.FC<MobileNewsListProps> = ({ news }) => {
  const { themeMode } = useDashboardSettings();
  /** 列表卡片在 app-dark 下为深色底，弹窗 portaled 到 body 需单独套色，否则 dark markdown 会白字叠白底 */
  const markdownVariant = themeMode === 'dark' ? 'dark' : 'light';
  const [selectedNews, setSelectedNews] = useState<DashboardNewsItem | null>(null);

  const newsDetailModalStyles = useMemo(() => {
    const bodyBase = {
      padding: 0,
      maxHeight: '75vh' as const,
      overflowY: 'auto' as const,
    };
    if (themeMode === 'dark') {
      return {
        content: { background: '#1a2234' },
        header: {
          background: '#1a2234',
          borderBottom: '1px solid #364152',
        },
        body: { ...bodyBase, background: '#1a2234' },
      };
    }
    return { body: bodyBase };
  }, [themeMode]);
  const [detailVisible, setDetailVisible] = useState<boolean>(false);

  const formatDate = (dateStr?: string) => {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', {
      month: 'short',
      day: 'numeric',
      year: 'numeric'
    });
  };

  const handleNewsClick = (newsItem: DashboardNewsItem) => {
    setSelectedNews(newsItem);
    setDetailVisible(true);
  };

  const handleClose = () => {
    setDetailVisible(false);
    setSelectedNews(null);
  };

  const getSentimentColor = (polarity?: number) => {
    if (polarity === undefined || polarity === null) return 'default';
    if (polarity > 0.1) return 'success';
    if (polarity < -0.1) return 'error';
    return 'default';
  };

  const getSentimentLabel = (polarity?: number) => {
    if (polarity === undefined || polarity === null) return 'Neutral';
    if (polarity > 0.1) return 'Positive';
    if (polarity < -0.1) return 'Negative';
    return 'Neutral';
  };

  return (
    <>
      <div className="mobile-news-list">
        <List
          dataSource={news}
          renderItem={(item) => {
            const summaryContent = getPreferredAnalysisContent(
              item.analysis_result,
              item.content || undefined
            );
            const hasSummary = Boolean(summaryContent);
            const isAIContent = Boolean(item.analysis_result);

            return (
              <div 
                className="news-card"
                onClick={() => handleNewsClick(item)}
              >
                <div className="news-header">
                  <Space size={8}>
                    <CalendarOutlined style={{ color: '#8c8c8c', fontSize: '12px' }} />
                    <Text type="secondary" style={{ fontSize: '11px' }}>
                      {formatDate(item.date)}
                    </Text>
                  </Space>
                  <Space size={6}>
                    {isAIContent && (
                      <Tag color="processing" style={{ margin: 0, fontSize: '7px', lineHeight: '12px', padding: '0 5px' }}>
                        AI
                      </Tag>
                    )}
                    {item.sentiment_polarity !== undefined && (
                      <Tag 
                        color={getSentimentColor(item.sentiment_polarity)}
                        style={{ margin: 0, fontSize: '7px', lineHeight: '12px', padding: '0 5px' }}
                      >
                        {getSentimentLabel(item.sentiment_polarity)}
                      </Tag>
                    )}
                  </Space>
                </div>

                <div className="news-title">
                  <Text strong style={{ fontSize: '14px', lineHeight: 1.4 }}>
                    {item.title}
                  </Text>
                </div>

                {hasSummary && (
                  <div className="news-summary">
                    <MarkdownRenderer
                      content={summaryContent || ''}
                      className="news-summary-markdown"
                      variant={markdownVariant}
                      style={{ fontSize: '13px' }}
                    />
                    <div className="news-summary-fade" />
                  </div>
                )}

              </div>
            );
          }}
        />
      </div>

      {/* 新闻详情Modal */}
      <Modal
        open={detailVisible}
        onCancel={handleClose}
        footer={null}
        width="100%"
        rootClassName={`mobile-news-modal${themeMode === 'dark' ? ' mobile-news-modal--dark' : ''}`}
        style={{ 
          top: '4vh', 
          paddingBottom: 0,
          maxWidth: '100vw'
        }}
        styles={newsDetailModalStyles}
        closeIcon={
          <CloseOutlined
            style={{
              fontSize: '20px',
              color: themeMode === 'dark' ? '#a0a3ad' : undefined,
            }}
          />
        }
      >
        {selectedNews &&
          (() => {
            const summaryResult =
              getAnalysisContent(selectedNews.analysis_result, 'news_summary_v1') ||
              getAnalysisContent(selectedNews.analysis_result, 'news_analysis');
            const summaryContent = summaryResult?.content;
            const hasSummaryContent = Boolean(summaryContent);
            const hasOriginalContent = Boolean(selectedNews.content);

            return (
              <div
                className={`mobile-news-detail${themeMode === 'dark' ? ' mobile-news-detail--theme-dark' : ''}`}
              >
                <div className="news-detail-header">
                  <Text style={{ fontSize: '18px', fontWeight: 600, lineHeight: 1.4 }}>
                    {selectedNews.title}
                  </Text>
                </div>

                <div className="news-detail-meta">
                  <Space size={16} wrap>
                    <Space size={4}>
                      <CalendarOutlined style={{ color: '#8c8c8c' }} />
                      <Text type="secondary" style={{ fontSize: '12px' }}>{formatDate(selectedNews.date)}</Text>
                    </Space>
                    {selectedNews.source && (
                      <Text type="secondary" style={{ fontSize: '12px' }}>Source: {selectedNews.source}</Text>
                    )}
                    {selectedNews.sentiment_polarity !== undefined && (
                      <Tag color={getSentimentColor(selectedNews.sentiment_polarity)} style={{ fontSize: '11px', lineHeight: '18px', padding: '0 8px' }}>
                        {getSentimentLabel(selectedNews.sentiment_polarity)}
                      </Tag>
                    )}
                  </Space>
                </div>

                {selectedNews.symbols && selectedNews.symbols.length > 0 && (
                  <div className="news-detail-symbols">
                    <Space size={4} wrap>
                      {selectedNews.symbols.map(symbol => (
                        <Tag key={symbol} style={{ fontSize: '7px', lineHeight: '12px', padding: '0 5px' }}>{symbol}</Tag>
                      ))}
                    </Space>
                  </div>
                )}

                {hasSummaryContent && (
                  <div className="news-detail-section">
                    <Text
                      strong
                      className="news-detail-section-heading"
                      style={{ fontSize: '16px', marginBottom: 12, display: 'block' }}
                    >
                      AI Analysis
                    </Text>
                    <MarkdownRenderer content={summaryContent || ''} variant={markdownVariant} />
                  </div>
                )}

                {hasSummaryContent && hasOriginalContent && <div className="news-detail-divider" />}

                {hasOriginalContent && (
                  <div className="news-detail-section">
                    <Text
                      strong
                      className="news-detail-section-heading"
                      style={{ fontSize: '16px', marginBottom: 12, display: 'block' }}
                    >
                      Original Content
                    </Text>
                    <Paragraph
                      className="mobile-news-detail-original"
                      style={{ fontSize: '15px', lineHeight: 1.6, whiteSpace: 'pre-line' }}
                    >
                      {selectedNews.content}
                    </Paragraph>
                  </div>
                )}

                {hasAnalysisForTemplate(selectedNews.analysis_result, 'news_analysis') && !hasSummaryContent && (
                  <div className="news-detail-section">
                    <Text
                      strong
                      className="news-detail-section-heading"
                      style={{ fontSize: '16px', marginBottom: 12, display: 'block' }}
                    >
                      AI Analysis
                    </Text>
                    <MarkdownRenderer
                      content={getPreferredAnalysisContent(selectedNews.analysis_result, 'news_analysis') || ''}
                      variant={markdownVariant}
                    />
                  </div>
                )}
              </div>
            );
          })()}
      </Modal>
    </>
  );
};

export default MobileNewsList;

