import React, { useState, useEffect } from 'react';
import {
  Card,
  List,
  Input,
  Select,
  DatePicker,
  Button,
  Space,
  Tag,
  Typography,
  Row,
  Col,
  message,
  Modal,
  Badge,
  Divider,
  Tooltip,
  Empty,
  Alert,
  Form,
  Skeleton
} from 'antd';
import {
  SearchOutlined,
  ReloadOutlined,
  EyeOutlined,
  LinkOutlined,
  ClearOutlined,
  SettingOutlined,
  DeleteOutlined,
  WarningOutlined,
  CalendarOutlined,
  RobotOutlined
} from '@ant-design/icons';
import dayjs, { Dayjs } from 'dayjs';

import { newsAPI, newsUtils } from '../services/news';
import {
  NewsItem,
  NewsListResponse,
  NewsTopic,
  NewsSymbol,
  NewsSearchParams,
  NewsThemeMatch
} from '../types/news';

import ThemeManagementModal from '../components/ThemeManagementModal';
import MarkdownRenderer from '../components/MarkdownRenderer';
import { useAuthStore } from '../store/authStore';
import { getErrorMessage } from '../utils/errorHandler';
import { MARKET_COLORS, formatSimilarity } from '../types/investmentThemes';
import { getPreferredAnalysisContent, hasAnalysisForTemplate, getAnalysisContent, getAllAnalysisResults } from '../utils/newsAnalysisHelper';
import { 
  getClassificationFromAnalysis, 
  getClassificationTags, 
  hasValidClassification,
  getClassificationSummary,
  getRelatedSectors
} from '../utils/newsClassificationHelper';

const { Text, Title, Paragraph } = Typography;
const { RangePicker } = DatePicker;
const { Option } = Select;

interface NewsPageState {
  loading: boolean;
  newsData: NewsListResponse | null;
  topics: NewsTopic[];
  symbols: NewsSymbol[];
  searchParams: NewsSearchParams;
  selectedNews: NewsItem | null;
  detailModalVisible: boolean;
  themeManagementModalVisible: boolean;
  deleteModalVisible: boolean;
  deleteLoading: boolean;
  deleteStartDate: string | null;
  deleteEndDate: string | null;
  expandedNewsItems: Set<number>;
}

const NewsPage: React.FC = () => {
  const { user, isAuthenticated } = useAuthStore();
  const [state, setState] = useState<NewsPageState>({
    loading: false,
    newsData: null,
    topics: [],
    symbols: [],
    searchParams: {
      page: 1,
      page_size: 20
    },
    selectedNews: null,
    detailModalVisible: false,
    themeManagementModalVisible: false,
    deleteModalVisible: false,
    deleteLoading: false,
    deleteStartDate: null,
    deleteEndDate: null,
    expandedNewsItems: new Set()
  });

  // 加载新闻数据
  const loadNewsData = async (params: NewsSearchParams = state.searchParams) => {
    const startTime = Date.now();
    
    console.log('📰 [新闻列表] 开始加载新闻数据', {
      params,
      timestamp: new Date().toISOString()
    });
    
    setState(prev => ({ ...prev, loading: true }));
    try {
      const data = await newsAPI.getNewsList(params);
      const loadTime = Date.now() - startTime;
      
      console.log('✅ [新闻列表] 新闻数据加载成功', {
        total: data.total,
        page: data.page,
        page_size: data.page_size,
        total_pages: data.total_pages,
        news_count: data.news?.length || 0,
        load_time: `${loadTime}ms`
      });
      
      setState(prev => ({
        ...prev,
        newsData: data,
        searchParams: params,
        loading: false
      }));
    } catch (error) {
      const loadTime = Date.now() - startTime;
      
      console.error('❌ Failed to get news data [News List]', {
        error: error,
        params,
        load_time: `${loadTime}ms`,
        timestamp: new Date().toISOString()
      });
      
      message.error(`Failed to get news data: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, loading: false }));
    }
  };

  // 加载主题和股票代码数据
  const loadMetadata = async () => {
    try {
      console.log('start to load metadata...');
      const [topics, symbols] = await Promise.all([
        newsAPI.getTopics(),
        newsAPI.getSymbols()
      ]);
      console.log('loaded metadata successfully:', { topics: topics.length, symbols: symbols.length });
      setState(prev => ({
        ...prev,
        topics,
        symbols: symbols.slice(0, 50)
      }));
    } catch (error: any) {
      console.error('Failed to get metadata:', error);
      message.error(`Failed to get metadata: ${getErrorMessage(error)}`);
    }
  };

  // 初始化数据
  useEffect(() => {
    if (isAuthenticated && user) {
      console.log('NewsPage: 用户已认证', { user: user.username, token: !!localStorage.getItem('access_token') });
      loadNewsData();
      loadMetadata();
    } else {
      console.log('NewsPage: 用户未认证或认证状态未初始化');
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isAuthenticated, user]);

  // 处理搜索
  const handleSearch = () => {
    loadNewsData({ ...state.searchParams, page: 1 });
  };

  // 处理重置
  const handleReset = () => {
    const resetParams = { page: 1, page_size: 20 };
    setState(prev => ({ ...prev, searchParams: resetParams }));
    loadNewsData(resetParams);
  };

  // 打开删除新闻弹窗
  const openDeleteModal = () => {
    setState(prev => ({
      ...prev,
      deleteModalVisible: true,
      deleteStartDate: null,
      deleteEndDate: null
    }));
  };

  // 关闭删除新闻弹窗
  const closeDeleteModal = () => {
    setState(prev => ({
      ...prev,
      deleteModalVisible: false,
      deleteStartDate: null,
      deleteEndDate: null
    }));
  };

  // 处理日期范围变化
  const handleDateRangeChange = (dates: [Dayjs | null, Dayjs | null] | null) => {
    if (dates && dates[0] && dates[1]) {
      setState(prev => ({
        ...prev,
        deleteStartDate: dates[0]!.format('YYYY-MM-DD'),
        deleteEndDate: dates[1]!.format('YYYY-MM-DD')
      }));
    } else {
      setState(prev => ({
        ...prev,
        deleteStartDate: null,
        deleteEndDate: null
      }));
    }
  };

  // 执行删除新闻
  const handleDeleteNews = async () => {
    if (!state.deleteStartDate || !state.deleteEndDate) {
      message.error('Please select date range');
      return;
    }

    setState(prev => ({ ...prev, deleteLoading: true }));
    try {
      const response = await newsAPI.deleteNewsByDateRange({
        start_date: state.deleteStartDate,
        end_date: state.deleteEndDate
      });

      if (response.success) {
        message.success(response.message);
        closeDeleteModal();
        loadNewsData(); // 刷新新闻列表
        loadMetadata(); // 刷新元数据
      } else {
        message.error(response.message || 'Failed to delete news');
      }
    } catch (error) {
      console.error('Failed to delete news:', error);
      message.error(`Failed to delete news: ${getErrorMessage(error)}`);
    } finally {
      setState(prev => ({ ...prev, deleteLoading: false }));
    }
  };

  // 处理分页
  const handleTableChange = (page: number, pageSize?: number) => {
    const newParams = {
      ...state.searchParams,
      page,
      page_size: pageSize || 20
    };
    loadNewsData(newParams);
  };

  // 显示新闻详情
  const showNewsDetail = async (newsId: number) => {
    try {
      const newsDetail = await newsAPI.getNewsDetail(newsId);
      setState(prev => ({
        ...prev,
        selectedNews: newsDetail,
        detailModalVisible: true
      }));
    } catch (error) {
      console.error('Failed to get news detail:', error);
      message.error(`Failed to get news detail: ${getErrorMessage(error)}`);
    }
  };

  // 更新搜索参数
  const updateSearchParam = (key: keyof NewsSearchParams, value: any) => {
    setState(prev => ({
      ...prev,
      searchParams: {
        ...prev.searchParams,
        [key]: value
      }
    }));
  };

  // 新闻展开/收缩功能
  const toggleNewsExpansion = (newsId: number) => {
    setState(prev => {
      const newSet = new Set(prev.expandedNewsItems);
      if (newSet.has(newsId)) {
        newSet.delete(newsId);
      } else {
        newSet.add(newsId);
      }
      return { ...prev, expandedNewsItems: newSet };
    });
  };

  const isNewsExpanded = (newsId: number) => state.expandedNewsItems.has(newsId);

  // 情感标签辅助函数
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

  return (
    <div style={{ padding: '24px' }}>
      <style>
        {`
          .ant-select-dropdown .ant-select-item {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-panel {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-header {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-content {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-cell {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-cell-inner {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-year-panel .ant-picker-cell-inner {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-month-panel .ant-picker-cell-inner {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-decade-panel .ant-picker-cell-inner {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-header-view {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-header-super-prev-btn,
          .ant-picker-dropdown .ant-picker-header-super-next-btn {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-header-prev-btn,
          .ant-picker-dropdown .ant-picker-header-next-btn {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-header-prev-btn .anticon,
          .ant-picker-dropdown .ant-picker-header-next-btn .anticon {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-today-btn {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-ok {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-ok .ant-btn {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-time-panel {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-time-panel .ant-picker-time-panel-cell {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-time-panel .ant-picker-time-panel-cell-inner {
            font-size: 12px !important;
          }
          .ant-select-selection-item {
            font-size: 12px !important;
          }
          .ant-select-selection-placeholder {
            font-size: 12px !important;
          }
          .ant-input::placeholder {
            font-size: 12px !important;
          }
          .ant-picker-input > input {
            font-size: 12px !important;
          }
          .ant-picker-separator {
            font-size: 12px !important;
          }
          .ant-picker-range {
            font-size: 12px !important;
          }
          .ant-picker-range .ant-picker-input {
            font-size: 12px !important;
          }
          .ant-picker-range .ant-picker-input > input {
            font-size: 12px !important;
          }
          .ant-picker-range .ant-picker-separator {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-panel-container {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-panel-container .ant-picker-panel {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-panel-container .ant-picker-panel .ant-picker-content {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-panel-container .ant-picker-panel .ant-picker-content .ant-picker-cell {
            font-size: 12px !important;
          }
          .ant-picker-dropdown .ant-picker-panel-container .ant-picker-panel .ant-picker-content .ant-picker-cell .ant-picker-cell-inner {
            font-size: 12px !important;
          }
          .ant-input {
            height: 20px !important;
          }
          .ant-picker {
            height: 20px !important;
          }
          .ant-picker-input > input {
            height: 20px !important;
            line-height: 20px !important;
          }
          .ant-select {
            height: 20px !important;
          }
          .ant-select-selector {
            height: 20px !important;
            line-height: 20px !important;
          }
          .ant-select-selection-item {
            line-height: 20px !important;
          }
          .ant-select-selection-placeholder {
            line-height: 20px !important;
          }
          /* Tooltip样式覆盖 */
          .ant-tooltip-inner {
            background-color: rgba(255, 255, 255, 0.95) !important;
            color: #333 !important;
            border: 1px solid #d9d9d9 !important;
            box-shadow: 0 3px 6px -4px rgba(0, 0, 0, 0.12), 0 6px 16px 0 rgba(0, 0, 0, 0.08), 0 9px 28px 8px rgba(0, 0, 0, 0.05) !important;
          }
          .ant-tooltip-arrow::before {
            background-color: rgba(255, 255, 255, 0.95) !important;
            border: 1px solid #d9d9d9 !important;
          }
        `}
      </style>
      {/* Statistics Summary */}
      {state.newsData && (
        <div style={{ 
          marginBottom: 16, 
          padding: '8px 16px', 
          backgroundColor: '#fafafa', 
          borderRadius: '6px',
          border: '1px solid #f0f0f0'
        }}>
          <Row gutter={16} align="middle">
            <Col span={6}>
              <div style={{ textAlign: 'center' }}>
                <Text strong style={{ fontSize: '16px', color: '#1890ff' }}>
                  {state.newsData.total}
                </Text>
                <br />
                <Text type="secondary" style={{ fontSize: '11px' }}>Total News</Text>
              </div>
            </Col>
            <Col span={6}>
              <div style={{ textAlign: 'center' }}>
                <Text strong style={{ fontSize: '16px', color: '#52c41a' }}>
                  {state.topics.length}
                </Text>
                <br />
                <Text type="secondary" style={{ fontSize: '11px' }}>Active Topics</Text>
              </div>
            </Col>
            <Col span={6}>
              <div style={{ textAlign: 'center' }}>
                <Text strong style={{ fontSize: '16px', color: '#faad14' }}>
                  {state.symbols.length}
                </Text>
                <br />
                <Text type="secondary" style={{ fontSize: '11px' }}>Related Symbols</Text>
              </div>
            </Col>
            <Col span={6}>
              <div style={{ textAlign: 'center' }}>
                <Text strong style={{ fontSize: '16px', color: '#722ed1' }}>
                  {state.newsData.total_pages}
                </Text>
                <br />
                <Text type="secondary" style={{ fontSize: '11px' }}>Total Pages</Text>
              </div>
            </Col>
          </Row>
        </div>
      )}

      {/* Search and Filter Area */}
      <Card style={{ marginBottom: 16, padding: '8px' }}>
        <Row gutter={[16, 8]} align="middle">
          <Col span={6}>
            <div style={{ display: 'flex', alignItems: 'center', height: '100%' }}>
              <Input
                placeholder="Search news title or content"
                prefix={<SearchOutlined />}
                value={state.searchParams.search_text}
                onChange={(e) => updateSearchParam('search_text', e.target.value)}
                onPressEnter={handleSearch}
                size="small"
                style={{ fontSize: '12px', height: '20px', width: '100%' }}
              />
            </div>
          </Col>
          <Col span={4}>
            <div style={{ display: 'flex', alignItems: 'center', height: '100%' }}>
              <Select
                placeholder="Select topic"
                allowClear
                style={{ width: '100%', fontSize: '12px' }}
                value={state.searchParams.topic_id}
                onChange={(value) => updateSearchParam('topic_id', value)}
                size="small"
                dropdownStyle={{ fontSize: '12px' }}
              >
                {state.topics.map(topic => (
                  <Option key={topic.id} value={topic.id} style={{ fontSize: '12px' }}>
                    <Badge color={newsUtils.getTopicColor(topic)} text={topic.topic_name} />
                  </Option>
                ))}
              </Select>
            </div>
          </Col>
          <Col span={4}>
            <div style={{ display: 'flex', alignItems: 'center', height: '100%' }}>
              <Select
                placeholder="Select symbol"
                allowClear
                showSearch
                style={{ width: '100%', fontSize: '12px' }}
                value={state.searchParams.symbol_code}
                onChange={(value) => updateSearchParam('symbol_code', value)}
                filterOption={(input, option) =>
                  option?.children?.toString().toLowerCase().includes(input.toLowerCase()) ?? false
                }
                size="small"
                dropdownStyle={{ fontSize: '12px' }}
              >
                {state.symbols.map(symbol => (
                  <Option key={symbol.symbol_code} value={symbol.symbol_code} style={{ fontSize: '12px' }}>
                    {newsUtils.formatSymbolCode(symbol.symbol_code)} ({symbol.news_count})
                  </Option>
                ))}
              </Select>
            </div>
          </Col>
          <Col span={5}>
            <div style={{ display: 'flex', alignItems: 'center', height: '100%' }}>
                            <RangePicker
                style={{ width: '100%', fontSize: '12px', height: '20px' }}
                value={[
                  state.searchParams.start_date ? dayjs(state.searchParams.start_date) : null,
                  state.searchParams.end_date ? dayjs(state.searchParams.end_date) : null
                ]}
                onChange={(dates) => {
                  updateSearchParam('start_date', dates?.[0]?.format('YYYY-MM-DD'));
                  updateSearchParam('end_date', dates?.[1]?.format('YYYY-MM-DD'));
                }}
                size="small"
                inputReadOnly={false}
                placeholder={['开始日期', '结束日期']}
                format="YYYY-MM-DD"
              />
            </div>
          </Col>
          <Col span={5}>
            <div style={{ display: 'flex', alignItems: 'center', height: '100%' }}>
              <Space>
                <Button
                  type="primary"
                  icon={<SearchOutlined />}
                  onClick={handleSearch}
                  size="small"
                  style={{ fontSize: '12px' }}
                >
                  Search
                </Button>
                <Button
                  icon={<ClearOutlined />}
                  onClick={handleReset}
                  size="small"
                  style={{ fontSize: '12px' }}
                >
                  Reset
                </Button>
              </Space>
            </div>
          </Col>
        </Row>
      </Card>



      {/* News List */}
      <Card>
        <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
          <Text strong style={{ fontSize: '14px' }}>News List</Text>
          <Space>
            <Button
              danger
              icon={<DeleteOutlined />}
              onClick={openDeleteModal}
              size="small"
            >
              Delete News
            </Button>
            <Button
              icon={<SettingOutlined />}
              onClick={() => setState(prev => ({ ...prev, themeManagementModalVisible: true }))}
              size="small"
            >
              Theme Management
            </Button>
            <Button
              icon={<ReloadOutlined />}
              onClick={() => loadNewsData(state.searchParams)}
              loading={state.loading}
              size="small"
            >
              Refresh
            </Button>
          </Space>
        </div>
        
        {state.loading ? (
          <div>
            {Array.from({ length: 5 }).map((_, index) => (
              <Card key={index} style={{ marginBottom: '16px' }}>
                <Skeleton active paragraph={{ rows: 2 }} />
                <div style={{ marginTop: '12px' }}>
                  <Skeleton.Input style={{ width: '60%' }} size="small" />
          </div>
              </Card>
            ))}
          </div>
        ) : !state.newsData || state.newsData.news.length === 0 ? (
          <Empty description="No news data" image={Empty.PRESENTED_IMAGE_SIMPLE} />
        ) : (
          <>
          <div style={{ position: 'relative' }}>
              {state.newsData.news.map((item, index) => (
              <div key={item.id} style={{ position: 'relative', marginBottom: '24px' }}>
                {/* Timeline line */}
                {index < state.newsData!.news.length - 1 && (
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
                              onClick={() => {
                                setState(prev => ({
                                  ...prev,
                                  selectedNews: item,
                                  detailModalVisible: true
                                }));
                              }}
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
                          const displayContent = getPreferredAnalysisContent(item.analysis_result, item.content);
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
                  </List.Item>
                </div>
              </div>
            ))}
            </div>
            
            {/* Pagination */}
            <div style={{ marginTop: '24px', textAlign: 'center' }}>
              <Space>
                <Button
                  disabled={!state.newsData || state.newsData.page <= 1}
                  onClick={() => {
                    if (state.newsData) {
                      handleTableChange(state.newsData.page - 1, state.newsData.page_size);
                    }
                  }}
                >
                  Previous
                </Button>
                <Text>
                  Page {state.newsData?.page || 1} of {state.newsData?.total_pages || 1}
                  {' '}(Total: {state.newsData?.total || 0} items)
                </Text>
                <Button
                  disabled={!state.newsData || state.newsData.page >= state.newsData.total_pages}
                  onClick={() => {
                    if (state.newsData) {
                      handleTableChange(state.newsData.page + 1, state.newsData.page_size);
                    }
                  }}
                >
                  Next
                </Button>
                <Select
                  value={state.newsData?.page_size || 20}
                  onChange={(value) => {
                    handleTableChange(1, value);
                  }}
                  style={{ width: 100 }}
                  size="small"
                >
                  <Option value={10}>10 / page</Option>
                  <Option value={20}>20 / page</Option>
                  <Option value={50}>50 / page</Option>
                  <Option value={100}>100 / page</Option>
                </Select>
              </Space>
            </div>
          </>
        )}
      </Card>

      {/* AI Analysis Details Modal */}
      <Modal
        title={
          <Space>
            <RobotOutlined style={{ color: '#1890ff' }} />
            <span>{state.selectedNews?.analysis_result ? 'AI Analysis Details' : 'News Details'}</span>
          </Space>
        }
        open={state.detailModalVisible}
        onCancel={() => setState(prev => ({ ...prev, detailModalVisible: false }))}
        footer={[
          <Button key="close" onClick={() => setState(prev => ({ ...prev, detailModalVisible: false }))}>
            Close
          </Button>,
          state.selectedNews?.link && (
            <Button
              key="original" 
              type="primary"
              onClick={() => window.open(state.selectedNews?.link, '_blank')}
            >
              View Original
            </Button>
          )
        ]}
        width={800}
      >
        {state.selectedNews && (
          <>
            <Card size="small" style={{ marginBottom: '16px', backgroundColor: '#f5f5f5' }}>
              <Typography.Title level={5} style={{ marginBottom: '12px' }}>
                {state.selectedNews.title}
              </Typography.Title>
              <Space size="small" wrap>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  <CalendarOutlined /> {state.selectedNews.date ? new Date(state.selectedNews.date).toLocaleString() : 'N/A'}
                </Text>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  Source: {newsUtils.getSourceDisplayName(state.selectedNews.source) || 'N/A'}
                </Text>
              </Space>
            </Card>

            {(() => {
              const allResults = getAllAnalysisResults(state.selectedNews.analysis_result);
              const hasAnyAnalysis = Object.keys(allResults).length > 0;
              const summaryResult = getAnalysisContent(state.selectedNews.analysis_result, 'news_summary_v1');

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

                        {state.selectedNews.content && (
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
                              {state.selectedNews.content}
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
                            {state.selectedNews.symbols && state.selectedNews.symbols.length > 0 && (
                              <div>
                                <Text strong style={{ fontSize: '9px', color: '#666' }}>Related Tickers:</Text>
                                <Space size={[4, 4]} wrap style={{ marginTop: '12px' }}>
                  {state.selectedNews.symbols.map((symbol, index) => (
                                    <Tag key={index} color="default" style={{ fontSize: '9px' }}>
                      {newsUtils.formatSymbolCode(symbol)}
                    </Tag>
                  ))}
                                </Space>
              </div>
            )}

                            {/* Classification Tags */}
                            {(() => {
                              const classification = getClassificationFromAnalysis(state.selectedNews.analysis_result);
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

                            {/* Analysis Template Tags */}
                            {(() => {
                              const allResults = getAllAnalysisResults(state.selectedNews.analysis_result);
                              const templateCodes = Object.keys(allResults);

                              if (templateCodes.length > 0) {
                                return (
            <div>
                                    <Text strong style={{ fontSize: '9px', color: '#666' }}>Analysis Template:</Text>
                                    <Space size={[4, 4]} wrap style={{ marginTop: '12px' }}>
                                      {templateCodes.map(templateCode => (
                                        <Tag key={templateCode} color="green" style={{ fontSize: '9px' }}>
                                          {templateCode}
                                        </Tag>
                                      ))}
                                    </Space>
                                  </div>
                                );
                              }
                              return null;
                      })()}

                            {/* Confidence Information */}
                            {(() => {
                              const classification = getClassificationFromAnalysis(state.selectedNews.analysis_result);
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
                    <Paragraph style={{ fontSize: '13px', whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>
                      {state.selectedNews.content || 'No content available'}
                  </Paragraph>
                  </Card>
                );
              }
            })()}
          </>
        )}
      </Modal>

      {/* 删除新闻弹窗 */}
      <Modal
        title={
          <Space>
            <WarningOutlined style={{ color: '#ff4d4f' }} />
            <Text>按日期范围删除新闻</Text>
          </Space>
        }
        open={state.deleteModalVisible}
        onCancel={closeDeleteModal}
        onOk={handleDeleteNews}
        okText="确认删除"
        cancelText="取消"
        okButtonProps={{ 
          danger: true, 
          loading: state.deleteLoading,
          disabled: !state.deleteStartDate || !state.deleteEndDate
        }}
        width={500}
      >
        <Alert
          message="警告"
          description="此操作将永久删除选定日期范围内的新闻及所有相关数据（包括 news_auto_tags_relations、news_symbols、news_tags 等），此操作不可撤销！"
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
        />
        
        <Form layout="vertical">
          <Form.Item label="Select date range" required>
            <RangePicker
              style={{ width: '100%' }}
              onChange={handleDateRangeChange}
              format="YYYY-MM-DD"
              placeholder={['Start date', 'End date']}
            />
          </Form.Item>
          
          {state.deleteStartDate && state.deleteEndDate && (
            <Alert
              message={
                <Text>
                  您即将删除 {state.deleteStartDate} 至 {state.deleteEndDate} 之间的新闻
                </Text>
              }
              type="info"
              showIcon
            />
          )}
        </Form>
      </Modal>

      {/* 主题管理模态框 */}
      <ThemeManagementModal
        open={state.themeManagementModalVisible}
        onCancel={() => setState(prev => ({ ...prev, themeManagementModalVisible: false }))}
        onSuccess={() => {
          loadMetadata(); // 刷新主题数据
        }}
      />
    </div>
  );
};

export default NewsPage;