/**
 * News Timeline Page with Theme Filtering
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Layout,
  Card,
  Input,
  Button,
  Space,
  Tag,
  Typography,
  Row,
  Col,
  message,
  Spin,
  DatePicker,
  Tooltip,
  Badge
} from 'antd';
import {
  SearchOutlined,
  ReloadOutlined,
  FireOutlined,
  ClearOutlined,
  SyncOutlined,
  ThunderboltOutlined
} from '@ant-design/icons';
import dayjs from 'dayjs';

import { newsAPI } from '../services/news';
import unifiedThemesAPI from '../services/unifiedThemes';
import { NewsListResponse, NewsSearchParams } from '../types/news';
import { TrendingTheme, FusedTheme } from '../types/unifiedThemes';
import NewsFeedTimeline from '../components/NewsFeedTimeline';
import { useAuthStore } from '../store/authStore';
import { getErrorMessage } from '../utils/errorHandler';

const { Content, Sider } = Layout;
const { Title, Text } = Typography;
const { RangePicker } = DatePicker;

interface PageState {
  newsData: NewsListResponse | null;
  loading: boolean;
  loadingMore: boolean;
  searchParams: NewsSearchParams;
  trendingThemes: TrendingTheme[];
  selectedTheme: string | null;
  themesLoading: boolean;
  fusedThemesMap: Map<number, FusedTheme[]>;
  fusedThemesLoading: boolean;
  siderCollapsed: boolean;
}

const NewsTimelinePage: React.FC = () => {
  const { user, isAuthenticated } = useAuthStore();
  
  const [state, setState] = useState<PageState>({
    newsData: null,
    loading: false,
    loadingMore: false,
    searchParams: { page: 1, page_size: 20 },
    trendingThemes: [],
    selectedTheme: null,
    themesLoading: false,
    fusedThemesMap: new Map(),
    fusedThemesLoading: false,
    siderCollapsed: false
  });

  const loadTrendingThemes = useCallback(async (days: number = 7) => {
    setState(prev => ({ ...prev, themesLoading: true }));
    try {
      const response = await unifiedThemesAPI.getTrendingThemes({ days, min_news_count: 2, limit: 50 });
      setState(prev => ({ 
        ...prev, 
        trendingThemes: response?.items || [], 
        themesLoading: false 
      }));
    } catch (error) {
      console.error('Failed to load trending themes:', error);
      setState(prev => ({ ...prev, trendingThemes: [], themesLoading: false }));
    }
  }, []);

  const loadFusedThemes = useCallback(async (newsIds: number[]) => {
    if (newsIds.length === 0) return;
    setState(prev => ({ ...prev, fusedThemesLoading: true }));
    try {
      const batchResult = await unifiedThemesAPI.getBatchNewsFusedThemes(newsIds, { min_confidence: 0.3, top_n: 10 });
      const newMap = new Map<number, FusedTheme[]>();
      if (batchResult) {
        Object.entries(batchResult).forEach(([newsId, response]) => {
          newMap.set(parseInt(newsId), response?.themes || []);
        });
      }
      setState(prev => ({ ...prev, fusedThemesMap: newMap, fusedThemesLoading: false }));
    } catch (error) {
      console.error('Failed to load fused themes:', error);
      setState(prev => ({ ...prev, fusedThemesLoading: false }));
    }
  }, []);

  const loadNewsData = useCallback(async (params: NewsSearchParams = state.searchParams) => {
    setState(prev => ({ ...prev, loading: true }));
    try {
      const data = await newsAPI.getNewsList(params);
      setState(prev => ({ ...prev, newsData: data, searchParams: params, loading: false }));
      if (data?.news && data.news.length > 0) {
        loadFusedThemes(data.news.map(n => n.id));
      }
    } catch (error) {
      console.error('Failed to load news:', error);
      message.error(`Failed to load news: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, loading: false }));
    }
  }, [state.searchParams, loadFusedThemes]);

  const handleThemeFilter = useCallback((themeName: string | null) => {
    setState(prev => ({
      ...prev,
      selectedTheme: themeName,
      searchParams: { ...prev.searchParams, page: 1, unified_theme_name: themeName || undefined }
    }));
    loadNewsData({ ...state.searchParams, page: 1, unified_theme_name: themeName || undefined });
  }, [state.searchParams, loadNewsData]);

  const updateSearchParam = useCallback((key: keyof NewsSearchParams, value: any) => {
    setState(prev => ({ ...prev, searchParams: { ...prev.searchParams, [key]: value } }));
  }, []);

  const handleSearch = useCallback(() => {
    loadNewsData({ ...state.searchParams, page: 1 });
  }, [state.searchParams, loadNewsData]);

  const handleReset = useCallback(() => {
    setState(prev => ({ ...prev, selectedTheme: null, searchParams: { page: 1, page_size: 20 } }));
    loadNewsData({ page: 1, page_size: 20 });
  }, [loadNewsData]);

  const handleLoadMore = useCallback(async () => {
    if (!state.newsData || state.loadingMore) return;
    const nextPage = (state.newsData.page || 1) + 1;
    if (nextPage > (state.newsData.total_pages || 1)) return;
    setState(prev => ({ ...prev, loadingMore: true }));
    try {
      const params = { ...state.searchParams, page: nextPage };
      const data = await newsAPI.getNewsList(params);
      setState(prev => ({
        ...prev,
        newsData: { ...data, news: [...(prev.newsData?.news || []), ...(data?.news || [])] },
        searchParams: params,
        loadingMore: false
      }));
      if (data?.news && data.news.length > 0) {
        loadFusedThemes(data.news.map(n => n.id));
      }
    } catch (error) {
      console.error('Failed to load more news:', error);
      message.error(`Failed to load more: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, loadingMore: false }));
    }
  }, [state.newsData, state.searchParams, state.loadingMore, loadFusedThemes]);

  const handleSyncThemes = useCallback(async () => {
    message.loading({ content: 'Syncing...', key: 'sync', duration: 0 });
    try {
      await unifiedThemesAPI.syncRecentNews(30);
      message.success({ content: 'Sync started', key: 'sync' });
      setTimeout(() => {
        loadTrendingThemes();
        if (state.newsData?.news) {
          loadFusedThemes(state.newsData.news.map(n => n.id));
        }
      }, 3000);
    } catch (error) {
      console.error('Failed to sync themes:', error);
      message.error({ content: `Sync failed: ${getErrorMessage(error)}`, key: 'sync' });
    }
  }, [loadTrendingThemes, state.newsData, loadFusedThemes]);

  useEffect(() => {
    if (isAuthenticated && user) {
      loadNewsData();
      loadTrendingThemes();
    }
  }, [isAuthenticated, user, loadNewsData, loadTrendingThemes]);

  const hasMore = state.newsData ? (state.newsData.page || 1) < (state.newsData.total_pages || 1) : false;

  return (
    <Layout style={{ minHeight: 'calc(100vh - 64px)', background: '#f0f2f5' }}>
      <Sider width={280} style={{ background: '#fff', borderRight: '1px solid #f0f0f0', height: '100%', overflowY: 'auto' }}
        collapsed={state.siderCollapsed} collapsedWidth={0} breakpoint="lg"
        onCollapse={(collapsed) => setState(prev => ({ ...prev, siderCollapsed: collapsed }))}>
        <div style={{ padding: '16px' }}>
          <div style={{ marginBottom: 16, display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
            <Title level={5} style={{ margin: 0 }}>
              <FireOutlined style={{ color: '#ff4d4f', marginRight: 8 }} />热门主题
            </Title>
            <Tooltip title="刷新">
              <Button type="text" size="small" icon={<ReloadOutlined />} loading={state.themesLoading} onClick={() => loadTrendingThemes()} />
            </Tooltip>
          </div>
          <Spin spinning={state.themesLoading}>
            <Space direction="vertical" style={{ width: '100%' }} size="small">
              <Button block type={state.selectedTheme === null ? 'primary' : 'default'} onClick={() => handleThemeFilter(null)}
                style={{ textAlign: 'left', justifyContent: 'flex-start' }}>
                全部新闻
                {state.newsData && <Badge count={state.newsData.total} style={{ marginLeft: 'auto', backgroundColor: '#52c41a' }} overflowCount={9999} />}
              </Button>
              {state.trendingThemes.map((theme, index) => (
                <Button key={index} block type={state.selectedTheme === theme.theme_name ? 'primary' : 'default'}
                  onClick={() => handleThemeFilter(theme.theme_name)}
                  style={{ textAlign: 'left', height: 'auto', padding: '8px 12px', display: 'flex', flexDirection: 'column', alignItems: 'flex-start' }}>
                  <div style={{ width: '100%', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <Text strong={state.selectedTheme === theme.theme_name} ellipsis style={{ flex: 1, fontSize: '13px' }}>
                      {theme.theme_name}
                      {theme.is_hot && <ThunderboltOutlined style={{ color: '#ff4d4f', marginLeft: 4 }} />}
                    </Text>
                    <Badge count={theme.news_count} style={{ backgroundColor: '#1890ff' }} overflowCount={999} />
                  </div>
                  <Space size={4} style={{ marginTop: 4 }}>
                    {theme.markets?.slice(0, 3).map((market, idx) => (<Tag key={idx} style={{ fontSize: '10px', margin: 0 }}>{market}</Tag>))}
                  </Space>
                </Button>
              ))}
            </Space>
          </Spin>
        </div>
      </Sider>
      <Layout style={{ marginLeft: state.siderCollapsed ? 0 : 0 }}>
        <Content style={{ padding: '24px', background: '#f0f2f5' }}>
          <Card style={{ marginBottom: 16 }}>
            <Row gutter={[16, 16]}>
              <Col xs={24} sm={12} md={8} lg={6}>
                <Input placeholder="搜索新闻标题或内容" prefix={<SearchOutlined />} value={state.searchParams.search_text}
                  onChange={(e) => updateSearchParam('search_text', e.target.value)} onPressEnter={handleSearch} allowClear />
              </Col>
              <Col xs={24} sm={12} md={8} lg={6}>
                <RangePicker style={{ width: '100%' }}
                  value={[state.searchParams.start_date ? dayjs(state.searchParams.start_date) : null, state.searchParams.end_date ? dayjs(state.searchParams.end_date) : null]}
                  onChange={(dates) => {
                    updateSearchParam('start_date', dates?.[0]?.format('YYYY-MM-DD'));
                    updateSearchParam('end_date', dates?.[1]?.format('YYYY-MM-DD'));
                  }} format="YYYY-MM-DD" />
              </Col>
              <Col xs={24} sm={12} md={8} lg={6}>
                <Space>
                  <Button type="primary" icon={<SearchOutlined />} onClick={handleSearch}>搜索</Button>
                  <Button icon={<ClearOutlined />} onClick={handleReset}>重置</Button>
                  <Tooltip title="同步主题数据">
                    <Button icon={<SyncOutlined />} onClick={handleSyncThemes}>同步</Button>
                  </Tooltip>
                </Space>
              </Col>
            </Row>
            {state.selectedTheme && (
              <div style={{ marginTop: 16 }}>
                <Text type="secondary">当前筛选: </Text>
                <Tag closable onClose={() => handleThemeFilter(null)} color="blue">{state.selectedTheme}</Tag>
              </div>
            )}
          </Card>
          <Card bodyStyle={{ padding: '16px 24px' }}>
            <NewsFeedTimeline newsData={state.newsData?.news || []} loading={state.loading}
              onLoadMore={handleLoadMore} hasMore={hasMore} loadingMore={state.loadingMore} fusedThemesMap={state.fusedThemesMap} />
          </Card>
        </Content>
      </Layout>
    </Layout>
  );
};

export default NewsTimelinePage;
