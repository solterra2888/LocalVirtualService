/**
 * News Feed Timeline Component
 * 时间轴样式的新闻展示组件，参考My Watchlist的Related News样式
 */

import React, { useState } from 'react';
import {
  List,
  Card,
  Tag,
  Typography,
  Space,
  Button,
  Tooltip,
  Empty,
  Modal
} from 'antd';
import {
  CalendarOutlined,
  FireOutlined,
  RobotOutlined
} from '@ant-design/icons';
import { NewsItem } from '../types/news';
import { FusedTheme } from '../types/unifiedThemes';
import { newsUtils } from '../services/news';
import MarkdownRenderer from './MarkdownRenderer';
import { 
  getPreferredAnalysisContent, 
  hasAnalysisForTemplate, 
  getAllAnalysisResults,
  getAnalysisContent
} from '../utils/newsAnalysisHelper';
import {
  getClassificationFromAnalysis,
  getClassificationTags,
  hasValidClassification,
  getClassificationSummary,
  getRelatedSectors
} from '../utils/newsClassificationHelper';

const { Text, Paragraph } = Typography;

interface NewsFeedTimelineProps {
  newsData: NewsItem[];
  loading: boolean;
  onLoadMore?: () => void;
  hasMore?: boolean;
  loadingMore?: boolean;
  fusedThemesMap?: Map<number, FusedTheme[]>;
}

const THEME_COLORS = [
  '#1890ff', '#52c41a', '#faad14', '#f5222d', '#722ed1', 
  '#13c2c2', '#eb2f96', '#fa8c16', '#2f54eb', '#a0d911'
];

const getThemeColor = (index: number) => THEME_COLORS[index % THEME_COLORS.length];

const NewsFeedTimeline: React.FC<NewsFeedTimelineProps> = ({
  newsData,
  loading,
  onLoadMore,
  hasMore = false,
  loadingMore = false,
  fusedThemesMap = new Map()
}) => {
  const [detailModalVisible, setDetailModalVisible] = useState(false);
  const [selectedNews, setSelectedNews] = useState<NewsItem | null>(null);
  const [expandedNewsItems, setExpandedNewsItems] = useState<Set<number>>(new Set());

  const showNewsDetail = (news: NewsItem) => {
    setSelectedNews(news);
    setDetailModalVisible(true);
  };

  const closeDetailModal = () => {
    setDetailModalVisible(false);
    setSelectedNews(null);
  };

  const toggleNewsExpansion = (newsId: number) => {
    setExpandedNewsItems(prev => {
      const newSet = new Set(prev);
      if (newSet.has(newsId)) {
        newSet.delete(newsId);
      } else {
        newSet.add(newsId);
      }
      return newSet;
    });
  };

  const isNewsExpanded = (newsId: number) => expandedNewsItems.has(newsId);

  const getSentimentColor = (polarity?: number) => {
    if (!polarity) return 'default';
    if (polarity > 0.1) return 'green';
    if (polarity < -0.1) return 'red';
    return 'orange';
  };

  const getSentimentText = (polarity?: number) => {
    if (!polarity) return 'Neutral';
    if (polarity > 0.1) return 'Positive';
    if (polarity < -0.1) return 'Negative';
    return 'Neutral';
  };

  if (loading && newsData.length === 0) {
    return <Empty description="加载中..." image={Empty.PRESENTED_IMAGE_SIMPLE} />;
  }

  return (
    <>
      <div style={{ position: 'relative' }}>
        {newsData.length === 0 ? (
          <Empty description="没有新闻数据" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        ) : (
          <>
            {newsData.map((item, index) => {
              const fusedThemes = fusedThemesMap.get(item.id) || [];
              
              return (
                <div key={item.id} style={{ position: 'relative', marginBottom: '24px' }}>
                  {/* Timeline line */}
                  {index < newsData.length - 1 && (
                    <div
                      style={{
                        position: 'absolute',
                        left: '40px',
                        top: '60px',
                        bottom: '-24px',
                        width: '2px',
                        backgroundColor: '#d9d9d9',
                        zIndex: 1
                      }}
                    />
                  )}
                  
                  {/* Timeline dot */}
                  <div
                    style={{
                      position: 'absolute',
                      left: '36px',
                      top: '22px',
                      width: '8px',
                      height: '8px',
                      borderRadius: '50%',
                      backgroundColor: '#1890ff',
                      border: '1px solid #fff',
                      boxShadow: '0 0 0 1px #1890ff',
                      zIndex: 2
                    }}
                  />
                  
                  {/* Content */}
                  <div style={{ marginLeft: '60px', paddingLeft: '16px' }}>
                    <List.Item
                      extra={
                        <Space direction="vertical" align="end">
                          <Space size={[4, 4]} wrap>
                            {/* 情感标签 */}
                            <Tag 
                              color={getSentimentColor(item.sentiment_polarity)}
                              style={{ fontSize: '8px', margin: 0 }}
                            >
                              {getSentimentText(item.sentiment_polarity)}
                            </Tag>
                            {hasAnalysisForTemplate(item.analysis_result, 'news_summary_v1') && (
                              <Tag 
                                color="blue" 
                                icon={<RobotOutlined />} 
                                style={{ fontSize: '8px', margin: 0 }}
                              >
                                AI分析
                              </Tag>
                            )}
                          </Space>
                        </Space>
                      }
                    >
                      <List.Item.Meta
                        avatar={
                          <Space align="center" style={{ minWidth: '120px' }}>
                            <Text type="secondary" style={{ fontSize: '12px' }}>
                              <CalendarOutlined /> {item.date ? new Date(item.date).toLocaleDateString() : 'N/A'}
                            </Text>
                            <Text type="secondary" style={{ fontSize: '11px' }}>
                              {item.date ? new Date(item.date).toLocaleTimeString() : 'N/A'}
                            </Text>
                          </Space>
                        }
                        title={
                          <Space>
                            <span 
                              style={{ 
                                fontSize: '14px', 
                                cursor: 'pointer',
                                color: '#1890ff',
                                textDecoration: 'underline',
                                maxWidth: '800px',
                                overflow: 'hidden',
                                textOverflow: 'ellipsis',
                                whiteSpace: 'nowrap',
                                display: 'inline-block'
                              }}
                              title={item.title}
                              onClick={() => showNewsDetail(item)}
                            >
                              {item.title}
                            </span>
                            <Text type="secondary" style={{ fontSize: '12px' }}>
                              - {newsUtils.getSourceDisplayName(item.source)}
                            </Text>
                          </Space>
                        }
                        description={null}
                      />
                      <div style={{ fontSize: '13px', marginBottom: '8px' }}>
                        {(() => {
                          // 检查是否有AI分析结果
                          const hasAnalysis = hasAnalysisForTemplate(item.analysis_result, 'news_summary_v1');
                          const displayContent = getPreferredAnalysisContent(item.analysis_result, item.content);
                          
                          // 调试日志
                          if (item.analysis_result) {
                            console.log(`[News ${item.id}] Has analysis_result:`, {
                              hasAnalysis,
                              displayContent: displayContent?.substring(0, 100),
                              raw: item.analysis_result
                            });
                          }
                          
                          return displayContent ? (
                            <div>
                              <div style={{ 
                                maxHeight: isNewsExpanded(item.id) ? 'none' : '60px', 
                                overflow: isNewsExpanded(item.id) ? 'visible' : 'hidden',
                                position: 'relative'
                              }}>
                                <MarkdownRenderer 
                                  content={displayContent}
                                  style={{
                                    fontSize: '13px',
                                    lineHeight: '1.4'
                                  }}
                                />
                                {!isNewsExpanded(item.id) && (
                                  <div style={{
                                    position: 'absolute',
                                    bottom: 0,
                                    left: 0,
                                    right: 0,
                                    height: '20px',
                                    background: 'linear-gradient(transparent, white)',
                                    pointerEvents: 'none'
                                  }} />
                                )}
                              </div>
                              <Button
                                type="link"
                                size="small"
                                onClick={() => toggleNewsExpansion(item.id)}
                                style={{ 
                                  padding: '4px 0', 
                                  fontSize: '12px',
                                  height: 'auto'
                                }}
                              >
                                {isNewsExpanded(item.id) ? 'Collapse' : 'Expand'}
                              </Button>
                            </div>
                          ) : null;
                        })()}
                      </div>

                      {/* 主题标签 */}
                      {fusedThemes.length > 0 && (
                        <div style={{ marginBottom: '8px', marginTop: '8px' }}>
                          <Space size={[0, 8]} wrap>
                            <Text type="secondary" style={{ fontSize: '11px', marginRight: 4 }}>
                              <FireOutlined style={{ color: '#ff4d4f' }} /> 投资主题:
                            </Text>
                            {fusedThemes.slice(0, 5).map((theme: FusedTheme, idx: number) => (
                              <Tooltip
                                key={idx}
                                title={
                                  <div>
                                    <div>融合得分: {(theme.fusion_score * 100).toFixed(1)}%</div>
                                    <div>来源数: {theme.source_count}</div>
                                    <div>市场: {theme.markets.join(', ')}</div>
                                  </div>
                                }
                              >
                                <Tag color={getThemeColor(idx)} style={{ fontSize: '10px', cursor: 'pointer' }}>
                                  {theme.theme_name}
                                  {theme.fusion_score > 0.8 && ' 🔥'}
                                </Tag>
                              </Tooltip>
                            ))}
                            {fusedThemes.length > 5 && (
                              <Tag style={{ fontSize: '10px' }}>
                                +{fusedThemes.length - 5}
                              </Tag>
                            )}
                          </Space>
                        </div>
                      )}
                    </List.Item>
                  </div>
                </div>
              );
            })}
            
            {/* 加载更多按钮 */}
            {hasMore && (
              <div style={{ textAlign: 'center', marginTop: '24px', marginBottom: '24px' }}>
                <Button 
                  type="primary" 
                  loading={loadingMore}
                  onClick={onLoadMore}
                  disabled={loadingMore}
                >
                  {loadingMore ? 'Loading...' : 'Load More News'}
                </Button>
              </div>
            )}
            
            {/* 没有更多数据提示 */}
            {!hasMore && newsData.length > 0 && (
              <div style={{ textAlign: 'center', marginTop: '24px', marginBottom: '24px' }}>
                <Text type="secondary">All news loaded</Text>
              </div>
            )}
          </>
        )}
      </div>

      {/* AI Analysis Details 弹窗 */}
      <Modal
        title={
          <Space>
            <RobotOutlined style={{ color: '#1890ff' }} />
            <span>{selectedNews?.analysis_result ? 'AI Analysis Details' : 'News Details'}</span>
          </Space>
        }
        open={detailModalVisible}
        centered
        onCancel={closeDetailModal}
        footer={[
          <Button key="close" onClick={closeDetailModal}>
            Close
          </Button>,
          selectedNews?.link && (
            <Button 
              key="original" 
              type="primary"
              onClick={() => window.open(selectedNews.link, '_blank')}
            >
              View Original
            </Button>
          )
        ]}
        width={800}
      >
        {selectedNews && (
          <>
            <Card size="small" style={{ marginBottom: '16px', backgroundColor: '#f5f5f5' }}>
              <Typography.Title level={5} style={{ marginBottom: '12px' }}>
                {selectedNews.title}
              </Typography.Title>
              <Space size="small" wrap>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  <CalendarOutlined /> {selectedNews.date ? new Date(selectedNews.date).toLocaleString() : 'N/A'}
                </Text>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  Source: {newsUtils.getSourceDisplayName(selectedNews.source) || 'N/A'}
                </Text>
              </Space>
            </Card>

            {(() => {
              const allResults = getAllAnalysisResults(selectedNews.analysis_result);
              const hasAnyAnalysis = Object.keys(allResults).length > 0;
              const summaryResult = getAnalysisContent(selectedNews.analysis_result, 'news_summary_v1');

              if (hasAnyAnalysis) {
                return (
                  <>
                    {/* 优先显示 news_summary_v1 的结果 */}
                    {summaryResult && typeof summaryResult.content === 'string' && (
                      <>
                        <Card 
                          title="AI Analysis (Summary)" 
                          size="small"
                          headStyle={{ backgroundColor: '#e6f7ff', fontWeight: 'bold' }}
                        >
                          <MarkdownRenderer 
                            content={summaryResult.content}
                            style={{
                              fontSize: '12px',
                              lineHeight: '1.6'
                            }}
                          />
                          <div style={{ marginTop: '12px', fontSize: '10px', color: '#999' }}>
                            <Space size="small">
                              {summaryResult.provider && <span>Provider: {summaryResult.provider}</span>}
                              {summaryResult.model && <span>Model: {summaryResult.model}</span>}
                              {summaryResult.tokens && <span>Tokens: {summaryResult.tokens.toLocaleString()}</span>}
                              {summaryResult.temperature && <span>Temperature: {summaryResult.temperature}</span>}
                            </Space>
                          </div>
                        </Card>

                        {selectedNews.content && (
                          <Card 
                            title="Original Content" 
                            size="small"
                            style={{ marginTop: '16px' }}
                            headStyle={{ backgroundColor: '#f5f5f5' }}
                          >
                            <Paragraph 
                              ellipsis={{ rows: 3, expandable: true, symbol: 'Expand' }}
                              style={{ fontSize: '13px', color: '#666' }}
                            >
                              {selectedNews.content}
                            </Paragraph>
                          </Card>
                        )}

                        {/* Tags Card */}
                        <Card 
                          title="Tags" 
                          size="small"
                          style={{ marginTop: '16px' }}
                          headStyle={{ backgroundColor: '#f0f5ff', fontWeight: 'bold' }}
                        >
                          <Space direction="vertical" size="small" style={{ width: '100%' }}>
                            {/* Related Tickers */}
                            {selectedNews.symbols && selectedNews.symbols.length > 0 && (
                              <div>
                                <Text strong style={{ fontSize: '9px', color: '#666' }}>Related Tickers:</Text>
                                <Space size={[4, 4]} wrap style={{ marginTop: '12px' }}>
                                  {selectedNews.symbols.map(symbol => (
                                    <Tag key={symbol} color="default" style={{ fontSize: '9px' }}>
                                      {symbol}
                                    </Tag>
                                  ))}
                                </Space>
                              </div>
                            )}

                            {/* Classification Tags */}
                            {(() => {
                              const classification = getClassificationFromAnalysis(selectedNews.analysis_result);
                              if (!classification || !hasValidClassification(classification)) {
                                return null;
                              }

                              const tags = getClassificationTags(classification);
                              const relatedSectors = getRelatedSectors(classification);

                              return (
                                <>
                                  {/* 主要分类标签 */}
                                  <div>
                                    <Text strong style={{ fontSize: '9px', color: '#666' }}>Classification Tags:</Text>
                                    <Space size={[4, 4]} wrap style={{ marginTop: '12px' }}>
                                      {tags.map(({ key, config, value, displayValue }) => (
                                        <Tag 
                                          key={key} 
                                          color={config.color} 
                                          style={{ fontSize: '9px' }}
                                          title={`${config.label}: ${displayValue}`}
                                        >
                                          {displayValue}
                                        </Tag>
                                      ))}
                                    </Space>
                                  </div>

                                  {/* 相关行业 */}
                                  {relatedSectors.length > 0 && (
                                    <div>
                                      <Text strong style={{ fontSize: '9px', color: '#666' }}>Related Sectors:</Text>
                                      <Space size={[4, 4]} wrap style={{ marginTop: '12px' }}>
                                        {relatedSectors.map(sector => (
                                          <Tag key={sector} color="purple" style={{ fontSize: '9px' }}>
                                            {sector}
                                          </Tag>
                                        ))}
                                      </Space>
                                    </div>
                                  )}
                                </>
                              );
                            })()}

                            {/* Investment Themes */}
                            {fusedThemesMap.get(selectedNews.id) && fusedThemesMap.get(selectedNews.id)!.length > 0 && (
                              <div>
                                <Text strong style={{ fontSize: '9px', color: '#666' }}>Investment Themes:</Text>
                                <Space size={[4, 4]} wrap style={{ marginTop: '12px' }}>
                                  {fusedThemesMap.get(selectedNews.id)!.map((theme: FusedTheme, index: number) => (
                                    <Tooltip
                                      key={index}
                                      title={
                                        <div>
                                          <div>融合得分: {(theme.fusion_score * 100).toFixed(1)}%</div>
                                          <div>来源数: {theme.source_count}</div>
                                          <div>市场: {theme.markets.join(', ')}</div>
                                        </div>
                                      }
                                    >
                                      <Tag
                                        color={getThemeColor(index)}
                                        style={{ fontSize: '9px', cursor: 'pointer' }}
                                      >
                                        {theme.theme_name}
                                      </Tag>
                                    </Tooltip>
                                  ))}
                                </Space>
                              </div>
                            )}

                            {/* Auto Tags */}
                            {selectedNews.auto_tags && selectedNews.auto_tags.length > 0 && (
                              <div>
                                <Text strong style={{ fontSize: '9px', color: '#666' }}>Auto Tags:</Text>
                                <Space size={[4, 4]} wrap style={{ marginTop: '12px' }}>
                                  {selectedNews.auto_tags.map((tag, index) => (
                                    <Tag key={index} color="orange" style={{ fontSize: '9px' }}>
                                      {tag}
                                    </Tag>
                                  ))}
                                </Space>
                              </div>
                            )}

                            {/* Confidence Information */}
                            {(() => {
                              const classification = getClassificationFromAnalysis(selectedNews.analysis_result);
                              if (!classification || !hasValidClassification(classification)) {
                                return null;
                              }

                              const summary = getClassificationSummary(classification);
                              
                              if (summary && (summary.confidence.theme > 0 || summary.confidence.event > 0)) {
                                return (
                                  <div>
                                    <Text strong style={{ fontSize: '9px', color: '#666' }}>Confidence:</Text>
                                    <Space size="small" style={{ marginTop: '4px' }}>
                                      {summary.confidence.theme > 0 && (
                                        <Text style={{ fontSize: '9px' }}>
                                          Theme: {(summary.confidence.theme * 100).toFixed(0)}%
                                        </Text>
                                      )}
                                      {summary.confidence.event > 0 && (
                                        <Text style={{ fontSize: '9px' }}>
                                          Event: {(summary.confidence.event * 100).toFixed(0)}%
                                        </Text>
                                      )}
                                    </Space>
                                  </div>
                                );
                              }
                              return null;
                            })()}
                          </Space>
                        </Card>
                      </>
                    )}

                    {/* 显示其他模板的结果 */}
                    {Object.entries(allResults).map(([templateCode, result]) => {
                      if (templateCode === 'news_summary_v1' && typeof result.content === 'string') return null; // 已经显示过了
                      if (templateCode === 'news_classification') return null; // 不显示新闻分类分析
                      
                      return (
                        <Card 
                          key={templateCode}
                          title={`AI Analysis (${templateCode})`} 
                          size="small"
                          style={{ marginTop: '16px' }}
                          headStyle={{ backgroundColor: '#f0f5ff', fontWeight: 'bold' }}
                        >
                          <pre style={{ 
                            fontSize: '12px', 
                            whiteSpace: 'pre-wrap', 
                            wordBreak: 'break-word',
                            margin: 0
                          }}>
                            {typeof result.content === 'string' 
                              ? result.content 
                              : JSON.stringify(result.content, null, 2)}
                          </pre>
                          <div style={{ marginTop: '12px', fontSize: '10px', color: '#999' }}>
                            <Space size="small">
                              {result.provider && <span>Provider: {result.provider}</span>}
                              {result.model && <span>Model: {result.model}</span>}
                              {result.tokens && <span>Tokens: {result.tokens.toLocaleString()}</span>}
                              {result.temperature && <span>Temperature: {result.temperature}</span>}
                            </Space>
                          </div>
                        </Card>
                      );
                    })}
                  </>
                );
              } else {
                return (
                  <Card 
                    title="News Content" 
                    size="small"
                    headStyle={{ backgroundColor: '#f5f5f5', fontWeight: 'bold' }}
                  >
                    <Paragraph 
                      ellipsis={{ rows: 10, expandable: true, symbol: 'Expand' }}
                      style={{ fontSize: '14px' }}
                    >
                      {selectedNews.content || 'No content available'}
                    </Paragraph>
                  </Card>
                );
              }
            })()}
          </>
        )}
      </Modal>
    </>
  );
};

export default NewsFeedTimeline;

