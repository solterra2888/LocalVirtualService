import React, { useState, useEffect } from 'react';
import {
  Card,
  Table,
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
  Form
} from 'antd';
import {
  SearchOutlined,
  ReloadOutlined,
  EyeOutlined,
  LinkOutlined,
  ClearOutlined,
  SettingOutlined,
  DeleteOutlined,
  WarningOutlined
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';
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
    deleteEndDate: null
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
      
      console.error('❌ [新闻列表] 获取新闻数据失败', {
        error: error,
        params,
        load_time: `${loadTime}ms`,
        timestamp: new Date().toISOString()
      });
      
      message.error(`获取新闻数据失败: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, loading: false }));
    }
  };

  // 加载主题和股票代码数据
  const loadMetadata = async () => {
    try {
      console.log('开始加载元数据...');
      const [topics, symbols] = await Promise.all([
        newsAPI.getTopics(),
        newsAPI.getSymbols()
      ]);
      console.log('元数据加载成功:', { topics: topics.length, symbols: symbols.length });
      setState(prev => ({
        ...prev,
        topics,
        symbols: symbols.slice(0, 50)
      }));
    } catch (error: any) {
      console.error('获取元数据失败:', error);
      message.error(`获取过滤选项失败: ${getErrorMessage(error)}`);
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
      message.error('请选择日期范围');
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
        message.error(response.message || '删除失败');
      }
    } catch (error) {
      console.error('删除新闻失败:', error);
      message.error(`删除新闻失败: ${getErrorMessage(error)}`);
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
      console.error('获取新闻详情失败:', error);
      message.error(`获取新闻详情失败: ${getErrorMessage(error)}`);
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

  // Table column definitions
  const columns: ColumnsType<NewsItem> = [
    {
      title: 'Title',
      dataIndex: 'title',
      key: 'title',
      width: 300,
      ellipsis: true,
      render: (title: string, record: NewsItem) => (
        <Tooltip 
          title={title}
          placement="topLeft"
          mouseEnterDelay={0.5}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <div style={{ flex: 1, minWidth: 0 }}>
              <Button
                type="link"
                onClick={() => showNewsDetail(record.id)}
                style={{ 
                  padding: 0, 
                  height: 'auto', 
                  textAlign: 'left', 
                  fontSize: '11px',
                  width: '100%',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap'
                }}
              >
                <Text 
                  strong 
                  style={{ 
                    fontSize: '11px',
                    display: 'block',
                    overflow: 'hidden',
                    textOverflow: 'ellipsis',
                    whiteSpace: 'nowrap'
                  }}
                >
                  {title}
                </Text>
              </Button>
            </div>
            {newsUtils.hasExternalLink(record) && (
              <Tooltip title="View Original">
                <Button
                  type="link"
                  size="small"
                  icon={<LinkOutlined />}
                  onClick={() => window.open(record.link, '_blank')}
                  style={{ 
                    fontSize: '10px', 
                    flexShrink: 0,
                    padding: '2px 4px',
                    minWidth: '24px'
                  }}
                />
              </Tooltip>
            )}
          </div>
        </Tooltip>
      )
    },
    {
      title: 'Source',
      dataIndex: 'source',
      key: 'source',
      width: 120,
      render: (source: string) => (
        <Text type="secondary" style={{ fontSize: '11px' }}>{newsUtils.getSourceDisplayName(source)}</Text>
      )
    },
    {
      title: 'Date',
      dataIndex: 'date',
      key: 'date',
      width: 160,
      render: (date: string) => (
        <Text type="secondary" style={{ fontSize: '11px' }}>{newsUtils.formatNewsDate(date)}</Text>
      )
    },
    {
      title: 'Sentiment',
      dataIndex: 'sentiment_polarity',
      key: 'sentiment',
      width: 100,
      render: (_, record: NewsItem) => {
        const sentiment = newsUtils.getSentimentLabel(record);
        return (
          <Tag color={sentiment.color} style={{ fontSize: '10px' }}>
            {sentiment.label}
          </Tag>
        );
      }
    },
    {
      title: 'Symbols',
      dataIndex: 'symbols',
      key: 'symbols',
      width: 150,
      render: (symbols: string[]) => (
        <Tooltip 
          title={
            <div>
              <div style={{ marginBottom: '8px', fontWeight: 'bold' }}>All Symbols:</div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                                 {symbols.map((symbol, index) => (
                   <Tag key={index} style={{ fontSize: '11px', margin: 0 }}>
                     {newsUtils.formatSymbolCode(symbol)}
                   </Tag>
                 ))}
              </div>
            </div>
          }
          placement="topLeft"
          mouseEnterDelay={0.5}
        >
          <div style={{ 
            display: 'flex',
            alignItems: 'center',
            gap: '4px',
            overflow: 'hidden'
          }}>
            <div style={{ 
              display: 'flex', 
              gap: '4px',
              overflow: 'hidden',
              flex: 1
            }}>
                                            {symbols.slice(0, 2).map((symbol, index) => (
                <Tag key={index} style={{ 
                  fontSize: '11px', 
                  margin: 0, 
                  flexShrink: 0,
                  maxWidth: '50px',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap'
                }}>
                  {newsUtils.formatSymbolCode(symbol)}
                </Tag>
              ))}
            </div>
            {symbols.length > 2 && (
              <span style={{ 
                fontSize: '10px', 
                margin: 0, 
                flexShrink: 0,
                color: '#1890ff',
                fontWeight: 'bold'
              }}>
                +{symbols.length - 2}
              </span>
            )}
          </div>
        </Tooltip>
      )
    },
        {
      title: 'Tags',
      dataIndex: 'tags',
      key: 'tags',
      width: 200,
      render: (tags: string[]) => (
        <Tooltip 
          title={
            <div>
              <div style={{ marginBottom: '8px', fontWeight: 'bold' }}>All Tags:</div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                                 {tags.map((tag, index) => (
                   <Tag key={index} color="blue" style={{ fontSize: '11px', margin: 0 }}>
                     {tag}
                   </Tag>
                 ))}
              </div>
            </div>
          }
          placement="topLeft"
          mouseEnterDelay={0.5}
        >
          <div style={{ 
            display: 'flex',
            alignItems: 'center',
            gap: '4px',
            overflow: 'hidden'
          }}>
            <div style={{ 
              display: 'flex', 
              gap: '4px',
              overflow: 'hidden',
              flex: 1
            }}>
              {tags.slice(0, 2).map((tag, index) => (
                <Tag key={index} color="blue" style={{ 
                  fontSize: '11px', 
                  margin: 0, 
                  flexShrink: 0,
                  maxWidth: '70px',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap'
                }}>
                  {tag}
                </Tag>
          ))}
            </div>
            {tags.length > 2 && (
              <span style={{ 
                fontSize: '10px', 
                margin: 0, 
                flexShrink: 0,
                color: '#1890ff',
                fontWeight: 'bold'
              }}>
                +{tags.length - 2}
              </span>
            )}
          </div>
        </Tooltip>
      )
    },
    {
      title: 'Auto Tags',
      dataIndex: 'auto_tags',
      key: 'auto_tags',
      width: 200,
      render: (autoTags: string[]) => (
        <Tooltip 
          title={
            <div>
              <div style={{ marginBottom: '8px', fontWeight: 'bold' }}>All Auto Tags:</div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                {autoTags && autoTags.map((tag, index) => (
                  <Tag key={index} color="orange" style={{ fontSize: '11px', margin: 0 }}>
                    {tag}
                  </Tag>
                ))}
              </div>
            </div>
          }
          placement="topLeft"
          mouseEnterDelay={0.5}
        >
          <div style={{ 
            display: 'flex',
            alignItems: 'center',
            gap: '4px',
            overflow: 'hidden'
          }}>
            <div style={{ 
              display: 'flex', 
              gap: '4px',
              overflow: 'hidden',
              flex: 1
            }}>
              {autoTags && autoTags.slice(0, 2).map((tag, index) => (
                <Tag key={index} color="orange" style={{ 
                  fontSize: '11px', 
                  margin: 0, 
                  flexShrink: 0,
                  maxWidth: '70px',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap'
                }}>
                  {tag}
                </Tag>
              ))}
            </div>
            {autoTags && autoTags.length > 2 && (
              <span style={{ 
                fontSize: '10px', 
                margin: 0, 
                flexShrink: 0,
                color: '#fa8c16',
                fontWeight: 'bold'
              }}>
                +{autoTags.length - 2}
              </span>
            )}
          </div>
        </Tooltip>
      )
    },
    {
      title: 'Investment Themes',
      dataIndex: 'theme_matches',
      key: 'theme_matches',
      width: 220,
      render: (themeMatches: NewsThemeMatch[], record: NewsItem) => {
        // 直接使用传入的主题匹配数据，不在这里使用 hooks
        const displayThemes = themeMatches || [];

        if (!displayThemes || displayThemes.length === 0) {
          return <Text type="secondary" style={{ fontSize: '11px' }}>-</Text>;
        }

        return (
          <Tooltip 
            title={
              <div>
                <div style={{ marginBottom: '8px', fontWeight: 'bold' }}>Matched Investment Themes:</div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                  {displayThemes.map((theme, index) => (
                    <div key={index} style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <Tag color={MARKET_COLORS[theme.market]} style={{ fontSize: '11px', margin: 0, minWidth: '35px', textAlign: 'center' }}>
                        {theme.market}
                      </Tag>
                      <span style={{ flex: 1 }}>{theme.theme_name}</span>
                      <Badge 
                        count={formatSimilarity(theme.similarity)} 
                        style={{ backgroundColor: theme.confidence >= 0.8 ? '#52c41a' : theme.confidence >= 0.7 ? '#1890ff' : '#faad14', fontSize: '10px' }}
                      />
                    </div>
                  ))}
                </div>
              </div>
            }
            placement="topLeft"
            mouseEnterDelay={0.3}
          >
            <div style={{ 
              display: 'flex',
              alignItems: 'center',
              gap: '4px',
              overflow: 'hidden'
            }}>
              <div style={{ 
                display: 'flex', 
                gap: '4px',
                overflow: 'hidden',
                flex: 1
              }}>
                {displayThemes.slice(0, 2).map((theme, index) => (
                  <Tag 
                    key={index} 
                    color={MARKET_COLORS[theme.market]} 
                    style={{ 
                      fontSize: '11px', 
                      margin: 0, 
                      flexShrink: 0,
                      maxWidth: '90px',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap'
                    }}
                  >
                    {theme.theme_name}
                  </Tag>
                ))}
              </div>
              {displayThemes.length > 2 && (
                <span style={{ 
                  fontSize: '10px', 
                  margin: 0, 
                  flexShrink: 0,
                  color: '#52c41a',
                  fontWeight: 'bold'
                }}>
                  +{displayThemes.length - 2}
                </span>
              )}
            </div>
          </Tooltip>
        );
      }
    },
    {
      title: 'Actions',
      key: 'action',
      width: 100,
      render: (_, record: NewsItem) => (
        <Space>
          <Button
            type="primary"
            size="small"
            icon={<EyeOutlined />}
            onClick={() => showNewsDetail(record.id)}
            style={{ fontSize: '10px' }}
          >
            Detail
          </Button>
        </Space>
      )
    }
  ];

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



      {/* News Table */}
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
        
        <Table
          columns={columns}
          dataSource={state.newsData?.news || []}
          rowKey="id"
          loading={state.loading}
          size="small"
          style={{ fontSize: '11px' }}
          pagination={{
            current: state.newsData?.page || 1,
            pageSize: state.newsData?.page_size || 20,
            total: state.newsData?.total || 0,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) => `Items ${range[0]}-${range[1]} of ${total}`,
            onChange: handleTableChange,
            onShowSizeChange: handleTableChange
          }}
          locale={{
            emptyText: <Empty description="No news data" />
          }}
        />
      </Card>

      {/* News Detail Modal */}
      <Modal
        title="News Detail"
        open={state.detailModalVisible}
        onCancel={() => setState(prev => ({ ...prev, detailModalVisible: false }))}
        footer={[
          <Button key="close" onClick={() => setState(prev => ({ ...prev, detailModalVisible: false }))}>
            Close
          </Button>,
          ...(state.selectedNews && newsUtils.hasExternalLink(state.selectedNews) ? [
            <Button
              key="external"
              type="primary"
              icon={<LinkOutlined />}
              onClick={() => window.open(state.selectedNews?.link, '_blank')}
            >
              View Original
            </Button>
          ] : [])
        ]}
        width={800}
        centered
        style={{ top: 0 }}
        bodyStyle={{ maxHeight: '80vh', overflow: 'auto' }}
      >
        {state.selectedNews && (
          <div>
            <Title level={4}>{state.selectedNews.title}</Title>
            
            <Row gutter={16} style={{ marginBottom: 16 }}>
              <Col span={12}>
                <Text type="secondary">Source: {newsUtils.getSourceDisplayName(state.selectedNews.source)}</Text>
              </Col>
              <Col span={12}>
                <Text type="secondary">Date: {newsUtils.formatNewsDate(state.selectedNews.date)}</Text>
              </Col>
            </Row>

            {/* Sentiment Analysis */}
            {state.selectedNews.sentiment_polarity !== undefined && (
              <div style={{ marginBottom: 16 }}>
                <Text strong>Sentiment Analysis: </Text>
                <Tag color={newsUtils.getSentimentLabel(state.selectedNews).color} style={{ marginLeft: 8 }}>
                  {newsUtils.getSentimentLabel(state.selectedNews).label}
                </Tag>
                <div style={{ marginTop: 8 }}>
                  <Text type="secondary">
                    Positive: {((state.selectedNews.sentiment_pos || 0) * 100).toFixed(1)}% | 
                    Neutral: {((state.selectedNews.sentiment_neu || 0) * 100).toFixed(1)}% | 
                    Negative: {((state.selectedNews.sentiment_neg || 0) * 100).toFixed(1)}%
                  </Text>
                </div>
              </div>
            )}

            {/* Related Symbols */}
            {state.selectedNews.symbols.length > 0 && (
              <div style={{ marginBottom: 16 }}>
                <Text strong>Related Symbols: </Text>
                <div style={{ marginTop: 8 }}>
                  {state.selectedNews.symbols.map((symbol, index) => (
                    <Tag key={index} style={{ marginBottom: 4 }}>
                      {newsUtils.formatSymbolCode(symbol)}
                    </Tag>
                  ))}
                </div>
              </div>
            )}

            {/* Tags */}
            {state.selectedNews.tags.length > 0 && (
              <div style={{ marginBottom: 16 }}>
                <Text strong>Tags: </Text>
                <div style={{ marginTop: 8 }}>
                  {state.selectedNews.tags.map((tag, index) => (
                    <Tag key={index} color="blue" style={{ marginBottom: 4 }}>
                      {tag}
                    </Tag>
                  ))}
                </div>
              </div>
            )}

            {/* Auto Tags */}
            {state.selectedNews.auto_tags && state.selectedNews.auto_tags.length > 0 && (
              <div style={{ marginBottom: 16 }}>
                <Text strong>Auto Tags: </Text>
                <div style={{ marginTop: 8 }}>
                  {state.selectedNews.auto_tags.map((tag, index) => (
                    <Tag key={index} color="orange" style={{ marginBottom: 4 }}>
                      {tag}
                    </Tag>
                  ))}
                </div>
              </div>
            )}

            <Divider />

            {/* News Content */}
            <div>
              {state.selectedNews.analysis_result ? (
                <>
                  <div style={{ marginBottom: 16 }}>
                    <Text strong>AI Analysis: </Text>
                    <Tag color="blue" style={{ marginLeft: 8 }}>
                      {(() => {
                        const result = state.selectedNews.analysis_result;
                        if (typeof result === 'object' && result && 'template_code' in result) {
                          return (result as any).template_code || 'AI Analysis';
                        }
                        return 'AI Analysis';
                      })()}
                    </Tag>
                  </div>
                  <Card 
                    title="Analysis Content" 
                    size="small"
                    headStyle={{ backgroundColor: '#e6f7ff', fontWeight: 'bold' }}
                    style={{ marginBottom: 16 }}
                  >
                    <MarkdownRenderer 
                      content={state.selectedNews.analysis_result.content}
                      style={{
                        fontSize: '14px',
                        lineHeight: '1.6'
                      }}
                    />
                  </Card>
                  
                  {state.selectedNews.content && (
                    <div>
                      <Text strong>Original Content: </Text>
                      <Paragraph style={{ marginTop: 8, whiteSpace: 'pre-wrap' }}>
                        {state.selectedNews.content}
                      </Paragraph>
                    </div>
                  )}
                </>
              ) : (
                <div>
                  <Text strong>Content: </Text>
                  <Paragraph style={{ marginTop: 8, whiteSpace: 'pre-wrap' }}>
                    {state.selectedNews.content || 'No detailed content available'}
                  </Paragraph>
                </div>
              )}
            </div>
          </div>
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
          <Form.Item label="选择日期范围" required>
            <RangePicker
              style={{ width: '100%' }}
              onChange={handleDateRangeChange}
              format="YYYY-MM-DD"
              placeholder={['开始日期', '结束日期']}
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