/**
 * 投资主题管理页面
 * 展示和管理投资主题向量检索功能
 */

import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  Table,
  Button,
  Space,
  Tag,
  Typography,
  Row,
  Col,
  Statistic,
  message,
  Modal,
  Input,
  Select,
  Divider,
  Progress,
  Tooltip
} from 'antd';
import {
  ReloadOutlined,
  RocketOutlined,
  BarChartOutlined,
  SearchOutlined,
  SyncOutlined,
  ThunderboltOutlined
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

import {
  getThemeStatistics,
  listThemes,
  initializeThemeEmbeddings,
  matchNewsToThemes
} from '../services/investmentThemes';
import {
  InvestmentTheme,
  ThemeStatistics,
  MARKET_COLORS,
  MARKET_LABELS,
  ThemeMatch
} from '../types/investmentThemes';
import { getErrorMessage } from '../utils/errorHandler';

const { Title, Text, Paragraph } = Typography;
const { TextArea } = Input;
const { Option } = Select;

interface PageState {
  loading: boolean;
  statistics: ThemeStatistics | null;
  themes: InvestmentTheme[];
  total: number;
  currentPage: number;
  pageSize: number;
  selectedMarket: string | undefined;
  initializing: boolean;
  testModalVisible: boolean;
  testNewsText: string;
  testMarket: 'HK' | 'US' | 'CN' | undefined;
  testResults: ThemeMatch[];
  testLoading: boolean;
}

const InvestmentThemesPage: React.FC = () => {
  const [state, setState] = useState<PageState>({
    loading: false,
    statistics: null,
    themes: [],
    total: 0,
    currentPage: 1,
    pageSize: 20,
    selectedMarket: undefined,
    initializing: false,
    testModalVisible: false,
    testNewsText: '',
    testMarket: 'US',
    testResults: [],
    testLoading: false
  });

  // 加载统计信息
  const loadStatistics = useCallback(async () => {
    try {
      const stats = await getThemeStatistics();
      setState(prev => ({ ...prev, statistics: stats }));
    } catch (error) {
      message.error(`加载统计信息失败: ${getErrorMessage(error)}`);
    }
  }, []);

  // 加载主题列表
  const loadThemes = useCallback(async (page: number = state.currentPage, market?: string) => {
    setState(prev => ({ ...prev, loading: true }));
    try {
      const response = await listThemes(
        market,
        state.pageSize,
        (page - 1) * state.pageSize
      );
      
      setState(prev => ({
        ...prev,
        themes: response.themes,
        total: response.total,
        currentPage: page,
        loading: false
      }));
    } catch (error) {
      message.error(`加载主题列表失败: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, loading: false }));
    }
  }, [state.currentPage, state.pageSize]);

  // 初始化主题向量
  const handleInitialize = async (forceUpdate: boolean = false) => {
    Modal.confirm({
      title: forceUpdate ? '强制更新所有主题向量' : '初始化主题向量',
      content: forceUpdate 
        ? '这将重新生成所有主题的向量，可能需要几分钟时间。确认继续？'
        : '这将为新主题生成向量。已有向量的主题将被跳过。确认继续？',
      okText: '确认',
      cancelText: '取消',
      onOk: async () => {
        setState(prev => ({ ...prev, initializing: true }));
        try {
          const result = await initializeThemeEmbeddings(forceUpdate);
          
          if (result.success) {
            message.success(
              `初始化完成！处理: ${result.statistics.processed}, ` +
              `跳过: ${result.statistics.skipped}, ` +
              `失败: ${result.statistics.failed}`
            );
            
            // 刷新数据
            await loadStatistics();
            await loadThemes();
          } else {
            message.error(`初始化失败: ${result.message}`);
          }
        } catch (error) {
          message.error(`初始化失败: ${getErrorMessage(error)}`);
        } finally {
          setState(prev => ({ ...prev, initializing: false }));
        }
      }
    });
  };

  // 测试主题匹配
  const handleTestMatch = async () => {
    if (!state.testNewsText.trim()) {
      message.warning('请输入新闻文本');
      return;
    }

    setState(prev => ({ ...prev, testLoading: true }));
    try {
      const results = await matchNewsToThemes({
        news_text: state.testNewsText,
        market: state.testMarket,
        top_k: 10,
        min_similarity: 0.5
      });

      setState(prev => ({
        ...prev,
        testResults: results,
        testLoading: false
      }));

      if (results.length === 0) {
        message.info('未找到匹配的主题（相似度 < 0.5）');
      } else {
        message.success(`找到 ${results.length} 个匹配主题`);
      }
    } catch (error) {
      message.error(`匹配失败: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, testLoading: false }));
    }
  };

  // 初始加载
  useEffect(() => {
    loadStatistics();
    loadThemes();
  }, [loadStatistics, loadThemes]);

  // 表格列定义
  const columns: ColumnsType<InvestmentTheme> = [
    {
      title: '主题名称',
      dataIndex: 'theme_name',
      key: 'theme_name',
      width: 250,
      render: (name: string) => <Text strong>{name}</Text>
    },
    {
      title: '市场',
      dataIndex: 'market',
      key: 'market',
      width: 80,
      render: (market: string) => (
        <Tag color={MARKET_COLORS[market]}>
          {MARKET_LABELS[market] || market}
        </Tag>
      )
    },
    {
      title: '主题ID',
      dataIndex: 'theme_id',
      key: 'theme_id',
      width: 200,
      render: (id: string) => <Text type="secondary" style={{ fontSize: '12px' }}>{id}</Text>
    },
    {
      title: 'Embedding模型',
      dataIndex: 'embedding_model',
      key: 'embedding_model',
      width: 150,
      render: (model: string) => <Tag color="blue">{model || 'text-embedding-v3'}</Tag>
    },
    {
      title: '更新时间',
      dataIndex: 'updated_at',
      key: 'updated_at',
      width: 180,
      render: (date: string) => date ? new Date(date).toLocaleString('zh-CN') : '-'
    }
  ];

  // 市场筛选变化
  const handleMarketChange = (market: string | undefined) => {
    setState(prev => ({ ...prev, selectedMarket: market, currentPage: 1 }));
    loadThemes(1, market);
  };

  return (
    <div style={{ padding: '24px' }}>
      {/* 页面标题 */}
      <div style={{ marginBottom: '24px' }}>
        <Title level={2}>
          <ThunderboltOutlined /> 投资主题向量检索
        </Title>
        <Paragraph type="secondary">
          基于Qwen Embedding的智能主题匹配系统，自动将新闻与150+投资主题进行语义关联
        </Paragraph>
      </div>

      {/* 统计卡片 */}
      <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic
              title="主题总数"
              value={state.statistics?.total_themes || 0}
              prefix={<BarChartOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic
              title="HK市场主题"
              value={
                state.statistics?.market_breakdown.find(m => m.market === 'HK')?.count || 0
              }
              valueStyle={{ color: '#52c41a' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic
              title="US市场主题"
              value={
                state.statistics?.market_breakdown.find(m => m.market === 'US')?.count || 0
              }
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic
              title="已匹配新闻"
              value={state.statistics?.match_statistics.news_with_themes || 0}
              valueStyle={{ color: '#faad14' }}
            />
          </Card>
        </Col>
      </Row>

      {/* 性能指标 */}
      {state.statistics && (
        <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
          <Col span={24}>
            <Card title="系统性能" size="small">
              <Row gutter={16}>
                <Col span={8}>
                  <Text type="secondary">Embedding模型:</Text>{' '}
                  <Tag color="blue">{state.statistics.embedding_model}</Tag>
                </Col>
                <Col span={8}>
                  <Text type="secondary">向量维度:</Text>{' '}
                  <Tag color="purple">{state.statistics.embedding_dimension}</Tag>
                </Col>
                <Col span={8}>
                  <Text type="secondary">平均相似度:</Text>{' '}
                  <Tag color="green">
                    {(state.statistics.match_statistics.avg_similarity * 100).toFixed(1)}%
                  </Tag>
                </Col>
              </Row>
            </Card>
          </Col>
        </Row>
      )}

      {/* 操作按钮 */}
      <Card style={{ marginBottom: '24px' }}>
        <Space size="middle" wrap>
          <Button
            type="primary"
            icon={<RocketOutlined />}
            onClick={() => handleInitialize(false)}
            loading={state.initializing}
          >
            初始化主题向量
          </Button>
          <Button
            icon={<SyncOutlined />}
            onClick={() => handleInitialize(true)}
            loading={state.initializing}
          >
            强制更新所有向量
          </Button>
          <Button
            icon={<SearchOutlined />}
            onClick={() => setState(prev => ({ ...prev, testModalVisible: true }))}
          >
            测试主题匹配
          </Button>
          <Button
            icon={<ReloadOutlined />}
            onClick={() => {
              loadStatistics();
              loadThemes();
            }}
          >
            刷新
          </Button>
        </Space>

        <Divider />

        {/* 市场筛选 */}
        <Space>
          <Text>市场筛选:</Text>
          <Select
            style={{ width: 120 }}
            placeholder="全部市场"
            allowClear
            value={state.selectedMarket}
            onChange={handleMarketChange}
          >
            <Option value="HK">香港 (HK)</Option>
            <Option value="US">美国 (US)</Option>
            <Option value="CN">中国 (CN)</Option>
          </Select>
        </Space>
      </Card>

      {/* 主题列表表格 */}
      <Card title="投资主题列表" extra={<Text type="secondary">Total: {state.total}</Text>}>
        <Table
          columns={columns}
          dataSource={state.themes}
          rowKey="theme_id"
          loading={state.loading}
          pagination={{
            current: state.currentPage,
            pageSize: state.pageSize,
            total: state.total,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total) => `共 ${total} 个主题`,
            onChange: (page, pageSize) => {
              setState(prev => ({ ...prev, currentPage: page, pageSize: pageSize || 20 }));
              loadThemes(page, state.selectedMarket);
            }
          }}
          size="small"
        />
      </Card>

      {/* 测试匹配模态框 */}
      <Modal
        title="测试主题匹配"
        open={state.testModalVisible}
        onCancel={() => setState(prev => ({
          ...prev,
          testModalVisible: false,
          testNewsText: '',
          testResults: []
        }))}
        footer={null}
        width={800}
      >
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          <div>
            <Text strong>输入新闻文本:</Text>
            <TextArea
              rows={4}
              placeholder="请输入新闻标题和内容..."
              value={state.testNewsText}
              onChange={(e) => setState(prev => ({ ...prev, testNewsText: e.target.value }))}
              style={{ marginTop: '8px' }}
            />
          </div>

          <div>
            <Text strong>选择市场:</Text>
            <Select
              style={{ width: '100%', marginTop: '8px' }}
              value={state.testMarket || ''}
              onChange={(value: string) => setState(prev => ({ ...prev, testMarket: value === '' ? undefined : value as 'HK' | 'US' | 'CN' }))}
            >
              <Option value="HK">香港 (HK)</Option>
              <Option value="US">美国 (US)</Option>
              <Option value="">全部市场</Option>
            </Select>
          </div>

          <Button
            type="primary"
            icon={<SearchOutlined />}
            onClick={handleTestMatch}
            loading={state.testLoading}
            block
          >
            匹配主题
          </Button>

          {/* 匹配结果 */}
          {state.testResults.length > 0 && (
            <div>
              <Divider />
              <Title level={5}>匹配结果 (Top {state.testResults.length}):</Title>
              <Space direction="vertical" style={{ width: '100%' }} size="small">
                {state.testResults.map((result, index) => (
                  <Card key={index} size="small">
                    <Row gutter={16} align="middle">
                      <Col span={2}>
                        <Text strong style={{ fontSize: '16px' }}>#{result.rank}</Text>
                      </Col>
                      <Col span={10}>
                        <Text strong>{result.theme_name}</Text>
                      </Col>
                      <Col span={4}>
                        <Tag color={MARKET_COLORS[result.market]}>{result.market}</Tag>
                      </Col>
                      <Col span={8}>
                        <Tooltip title={`相似度: ${result.similarity.toFixed(4)}`}>
                          <Progress
                            percent={result.confidence * 100}
                            size="small"
                            format={(percent) => `${percent?.toFixed(0)}%`}
                            strokeColor={
                              result.confidence >= 0.8 ? '#52c41a' :
                              result.confidence >= 0.7 ? '#1890ff' : '#faad14'
                            }
                          />
                        </Tooltip>
                      </Col>
                    </Row>
                  </Card>
                ))}
              </Space>
            </div>
          )}
        </Space>
      </Modal>
    </div>
  );
};

export default InvestmentThemesPage;

