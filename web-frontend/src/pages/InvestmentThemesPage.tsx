/**
 * Investment Themes Management Page
 * Display and manage investment theme vector retrieval functionality
 */

import React, { useState, useEffect } from 'react';
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
  Tooltip,
  Dropdown,
  Menu,
  Alert
} from 'antd';
import {
  ReloadOutlined,
  BarChartOutlined,
  SearchOutlined,
  SyncOutlined,
  FundOutlined,
  SettingOutlined,
  ExperimentOutlined,
  WarningOutlined,
  InfoCircleOutlined,
  DownOutlined
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

import {
  getThemeStatistics,
  listThemes,
  initializeThemeEmbeddings,
  matchNewsToThemes,
  fillThemeDescriptions
} from '../services/investmentThemes';
import {
  InvestmentTheme,
  ThemeStatistics,
  MARKET_COLORS,
  MARKET_LABELS,
  ThemeMatch,
  getDescriptionSourceLabel
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

  // Load statistics
  const loadStatistics = async () => {
    try {
      const stats = await getThemeStatistics();
      setState(prev => ({ ...prev, statistics: stats }));
    } catch (error) {
      message.error(`Failed to load statistics: ${getErrorMessage(error)}`);
    }
  };

  // Load theme list
  const loadThemes = async (page?: number, market?: string) => {
    setState(prev => ({ ...prev, loading: true }));
    try {
      const currentPage = page || state.currentPage;
      const response = await listThemes(
        market,
        state.pageSize,
        (currentPage - 1) * state.pageSize
      );
      
      setState(prev => ({
        ...prev,
        themes: response.themes,
        total: response.total,
        currentPage: currentPage,
        loading: false
      }));
    } catch (error) {
      message.error(`Failed to load theme list: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, loading: false }));
    }
  };

  // 🌟 Sync Theme Data (merged functionality - replacing original handleInitialize)
  const handleSyncThemes = async () => {
    const coverageRate = state.statistics?.description_statistics?.coverage_rate || 0;
    const missingDescCount = (state.statistics?.description_statistics?.total || 0) - 
                             (state.statistics?.description_statistics?.with_description || 0);
    
    Modal.confirm({
      title: '🔄 Sync Theme Data',
      icon: <SyncOutlined />,
      width: 600,
      content: (
        <div>
          <Alert
            message="This operation will complete the following tasks"
            description={
              <div style={{ marginTop: '12px' }}>
                <div style={{ marginBottom: '8px' }}>
                  ✅ Generate detailed descriptions for <Text strong>{missingDescCount}</Text> new themes (using LLM)
                </div>
                <div style={{ marginBottom: '8px' }}>
                  ✅ Generate vectors for new themes (incremental update, skip existing)
                </div>
                <div>
                  ✅ Update statistics
                </div>
              </div>
            }
            type="info"
            showIcon
            style={{ marginBottom: '16px' }}
          />
          <Paragraph>
            <Text strong>Current Description Coverage:</Text>
            <Tag color={coverageRate >= 90 ? 'green' : 'orange'}>
              {coverageRate.toFixed(1)}%
            </Tag>
          </Paragraph>
          <Paragraph type="secondary">
            📌 Estimated Time: {missingDescCount > 20 ? '5-10' : '2-5'} minutes (background async execution)
          </Paragraph>
        </div>
      ),
      okText: 'Start Sync',
      cancelText: 'Cancel',
      onOk: async () => {
        try {
          // Step 1: Fill descriptions
          message.loading({ content: 'Step 1/2: Generating theme descriptions...', key: 'sync', duration: 0 });
          const descResult = await fillThemeDescriptions(false, state.selectedMarket);
          
          if (descResult.success) {
            // Step 2: Initialize vectors
            message.loading({ content: 'Step 2/2: Generating theme vectors...', key: 'sync', duration: 0 });
            const vectorResult = await initializeThemeEmbeddings(false);
            
            message.destroy('sync');
            
            if (vectorResult.success) {
              message.success({
                content: `🎉 Sync completed! Descriptions: ${descResult.statistics.total || 0}, Vectors: ${vectorResult.statistics.processed}`,
                duration: 6,
              });
              
              // Refresh data
              setTimeout(() => {
                loadStatistics();
                loadThemes();
              }, 2000);
            } else {
              message.warning(`Descriptions generated, but vector generation failed: ${vectorResult.message}`);
            }
          } else {
            message.destroy('sync');
            message.error(`Sync failed: ${descResult.message}`);
          }
        } catch (error) {
          message.destroy('sync');
          message.error(`Sync failed: ${getErrorMessage(error)}`);
        }
      }
    });
  };

  // 🌟 Optimize: Enhance Description Quality (fill descriptions only)
  const handleOptimizeDescriptions = async () => {
    const coverageRate = state.statistics?.description_statistics?.coverage_rate || 0;
    const missingCount = (state.statistics?.description_statistics?.total || 0) - 
                         (state.statistics?.description_statistics?.with_description || 0);
    const simpleTemplateCount = state.statistics?.description_statistics?.source_breakdown?.simple_template || 0;
    
    Modal.confirm({
      title: '✨ Optimize Description Quality',
      icon: <ExperimentOutlined />,
      width: 600,
      content: (
        <div>
          <Alert
            message="Use LLM to enhance theme description quality"
            description={
              <div style={{ marginTop: '12px' }}>
                <div style={{ marginBottom: '8px' }}>
                  📝 Missing Descriptions: <Text strong>{missingCount}</Text>
                </div>
                <div style={{ marginBottom: '8px' }}>
                  🔄 Simple Template Descriptions: <Text strong>{simpleTemplateCount}</Text> (can be optimized)
                </div>
                <div>
                  ✅ Upgrade all to LLM-generated detailed descriptions
                </div>
              </div>
            }
            type="success"
            showIcon
            style={{ marginBottom: '16px' }}
          />
          <Paragraph>
            <Text strong>Current Coverage:</Text>
            <Tag color={coverageRate >= 90 ? 'green' : 'orange'}>
              {coverageRate.toFixed(1)}%
            </Tag>
            <Text type="secondary" style={{ marginLeft: '8px' }}>
              → Expected 100% after optimization
            </Text>
          </Paragraph>
          <Paragraph type="secondary">
            💡 Use Case: When description coverage &lt; 90% or many simple template descriptions
          </Paragraph>
        </div>
      ),
      okText: 'Start Optimization',
      cancelText: 'Cancel',
      onOk: async () => {
        try {
          message.loading({ content: 'Optimizing theme descriptions...', key: 'optimize', duration: 0 });
          const result = await fillThemeDescriptions(false, state.selectedMarket);
          
          message.destroy('optimize');
          
          if (result.success) {
            message.success({
              content: result.message,
              duration: 5,
            });
            
            // Remind user to refresh later
            setTimeout(() => {
              message.info('Optimization task is running in background, please refresh later to see results', 3);
            }, 2000);
          } else {
            message.error(`Optimization failed: ${result.message}`);
          }
        } catch (error) {
          message.destroy('optimize');
          message.error(`Optimization failed: ${getErrorMessage(error)}`);
        }
      }
    });
  };

  // 🚨 Dangerous Operation: Force Update All Vectors
  const handleForceUpdateVectors = async () => {
    Modal.confirm({
      title: '⚠️ Warning: Force Update All Vectors',
      icon: <WarningOutlined style={{ color: '#ff4d4f' }} />,
      width: 650,
      content: (
        <div>
          <Alert
            message="This is a high-risk, high-cost operation"
            description={
              <div style={{ marginTop: '12px' }}>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  ⚠️ Will regenerate vectors for <Text strong style={{ color: '#ff4d4f' }}>all themes</Text> (150+)
                </div>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  ⏱️ Estimated Time: <Text strong>20-30 minutes</Text>
                </div>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  💰 Will repeatedly call LLM to generate descriptions (consuming large API quota)
                </div>
              </div>
            }
            type="error"
            showIcon
            style={{ marginBottom: '16px' }}
          />
          <Divider />
          <Paragraph>
            <Text strong>✅ Applicable Scenarios:</Text>
          </Paragraph>
          <ul style={{ paddingLeft: '20px', marginTop: '8px' }}>
            <li>Embedding model upgrade (e.g., text-embedding-v2 → v3)</li>
            <li>Vector dimension change</li>
          </ul>
          <Paragraph style={{ marginTop: '12px' }}>
            <Text strong>❌ Not Applicable Scenarios:</Text>
          </Paragraph>
          <ul style={{ paddingLeft: '20px', marginTop: '8px' }}>
            <li>Daily maintenance (please use "Sync Theme Data")</li>
            <li>Adding few new themes (please use "Sync Theme Data")</li>
          </ul>
          <Alert
            message="Are you sure you want to continue?"
            type="warning"
            showIcon
            style={{ marginTop: '16px' }}
          />
        </div>
      ),
      okText: 'Yes, I am sure',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk: async () => {
        setState(prev => ({ ...prev, initializing: true }));
        try {
          const result = await initializeThemeEmbeddings(true);
          
          if (result.success) {
            message.success(
              `Force update completed! Processed: ${result.statistics.processed}, ` +
              `Failed: ${result.statistics.failed}`
            );
            
            // Refresh data
            await loadStatistics();
            await loadThemes();
          } else {
            message.error(`Update failed: ${result.message}`);
          }
        } catch (error) {
          message.error(`Update failed: ${getErrorMessage(error)}`);
        } finally {
          setState(prev => ({ ...prev, initializing: false }));
        }
      }
    });
  };

  // 🚨 Dangerous Operation: Regenerate All Descriptions
  const handleForceRegenerateDescriptions = async () => {
    Modal.confirm({
      title: '⚠️ Warning: Regenerate All Descriptions',
      icon: <WarningOutlined style={{ color: '#ff4d4f' }} />,
      width: 650,
      content: (
        <div>
          <Alert
            message="This is a high-cost operation"
            description={
              <div style={{ marginTop: '12px' }}>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  ⚠️ Will call LLM to regenerate <Text strong style={{ color: '#ff4d4f' }}>all descriptions</Text> (150+)
                </div>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  ⏱️ Estimated Time: <Text strong>15-25 minutes</Text>
                </div>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  💰 LLM API calling cost is high
                </div>
              </div>
            }
            type="error"
            showIcon
            style={{ marginBottom: '16px' }}
          />
          <Divider />
          <Paragraph>
            <Text strong>✅ Applicable Scenarios:</Text>
          </Paragraph>
          <ul style={{ paddingLeft: '20px', marginTop: '8px' }}>
            <li>After major Prompt Template optimization</li>
            <li>Found historical description quality is generally poor</li>
          </ul>
          <Paragraph style={{ marginTop: '12px' }}>
            <Text strong>❌ Not Applicable Scenarios:</Text>
          </Paragraph>
          <ul style={{ paddingLeft: '20px', marginTop: '8px' }}>
            <li>Only few descriptions missing (please use "Optimize Description Quality")</li>
            <li>Daily maintenance (please use "Optimize Description Quality")</li>
          </ul>
          <Alert
            message="Are you sure you want to continue?"
            type="warning"
            showIcon
            style={{ marginTop: '16px' }}
          />
        </div>
      ),
      okText: 'Yes, I am sure',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk: async () => {
        try {
          message.loading({ content: 'Regenerating all descriptions...', key: 'forceRegen', duration: 0 });
          const result = await fillThemeDescriptions(true, state.selectedMarket);
          
          message.destroy('forceRegen');
          
          if (result.success) {
            message.success({
              content: result.message,
              duration: 5,
            });
            
            setTimeout(() => {
              message.info('Task is running in background, please refresh later to see results', 3);
            }, 2000);
          } else {
            message.error(`Operation failed: ${result.message}`);
          }
        } catch (error) {
          message.destroy('forceRegen');
          message.error(`Operation failed: ${getErrorMessage(error)}`);
        }
      }
    });
  };

  // Test theme matching
  const handleTestMatch = async () => {
    if (!state.testNewsText.trim()) {
      message.warning('Please enter news text');
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
        message.info('No matching themes found (similarity < 0.5)');
      } else {
        message.success(`Found ${results.length} matching themes`);
      }
    } catch (error) {
      message.error(`Matching failed: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, testLoading: false }));
    }
  };

  // Initial load (execute only once)
  useEffect(() => {
    loadStatistics();
    loadThemes();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // View description details
  const showDescriptionModal = (theme: InvestmentTheme) => {
    const sourceLabel = getDescriptionSourceLabel(theme.description_source);
    Modal.info({
      title: `${theme.theme_name} Theme Description`,
      width: 800,
      content: (
        <div>
          <Paragraph>
            <Text strong>Market:</Text>
            <Tag color={MARKET_COLORS[theme.market]}>{MARKET_LABELS[theme.market]}</Tag>
          </Paragraph>
          <Paragraph>
            <Text strong>Description Source:</Text>
            <Tag color={sourceLabel.color}>{sourceLabel.text}</Tag>
          </Paragraph>
          {theme.description_updated_at && (
            <Paragraph>
              <Text strong>Updated At:</Text>
              <Text type="secondary">{new Date(theme.description_updated_at).toLocaleString('en-US')}</Text>
            </Paragraph>
          )}
          <Divider />
          <Paragraph style={{ fontSize: '14px', lineHeight: '1.8' }}>
            {theme.description || <Text type="secondary">No description available</Text>}
          </Paragraph>
        </div>
      ),
    });
  };

  // Table column definitions
  const columns: ColumnsType<InvestmentTheme> = [
    {
      title: 'Theme Name',
      dataIndex: 'theme_name',
      key: 'theme_name',
      width: 200,
      fixed: 'left',
      render: (name: string) => <Text strong>{name}</Text>
    },
    {
      title: 'Market',
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
      title: 'Description Preview',
      dataIndex: 'description',
      key: 'description',
      width: 300,
      ellipsis: true,
      render: (text: string, record: InvestmentTheme) => (
        <Tooltip title={text || 'No description'}>
          <Text 
            ellipsis 
            style={{ cursor: text ? 'pointer' : 'default', color: text ? undefined : '#999' }}
            onClick={() => text && showDescriptionModal(record)}
          >
            {text || 'Not generated'}
          </Text>
        </Tooltip>
      )
    },
    {
      title: 'Description Source',
      dataIndex: 'description_source',
      key: 'description_source',
      width: 120,
      render: (source: string) => {
        const label = getDescriptionSourceLabel(source);
        return <Tag color={label.color}>{label.text}</Tag>;
      }
    },
    {
      title: 'Embedding Model',
      dataIndex: 'embedding_model',
      key: 'embedding_model',
      width: 150,
      render: (model: string) => <Tag color="blue">{model || 'text-embedding-v3'}</Tag>
    },
    {
      title: 'Updated At',
      dataIndex: 'updated_at',
      key: 'updated_at',
      width: 160,
      render: (date: string) => date ? new Date(date).toLocaleString('en-US') : '-'
    }
  ];

  // Market filter change
  const handleMarketChange = (market: string | undefined) => {
    setState(prev => ({ ...prev, selectedMarket: market, currentPage: 1 }));
    loadThemes(1, market);
  };

  return (
    <div style={{ padding: '24px' }}>
      {/* Page Title */}
      <div style={{ marginBottom: '24px' }}>
        <Title level={2}>
          <FundOutlined /> Investment Themes Vector Retrieval
        </Title>
        <Paragraph type="secondary">
          Intelligent theme matching system based on Qwen Embedding, automatically associating news with 150+ investment themes through semantic analysis
        </Paragraph>
      </div>

      {/* Statistics Cards */}
      <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic
              title="Total Themes"
              value={state.statistics?.total_themes || 0}
              prefix={<BarChartOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic
              title="Description Coverage"
              value={state.statistics?.description_statistics?.coverage_rate || 0}
              suffix="%"
              precision={1}
              valueStyle={{ 
                color: (state.statistics?.description_statistics?.coverage_rate || 0) >= 90 ? '#52c41a' : '#faad14'
              }}
            />
            <Progress
              percent={state.statistics?.description_statistics?.coverage_rate || 0}
              status={
                (state.statistics?.description_statistics?.coverage_rate || 0) === 100 
                  ? 'success' 
                  : 'active'
              }
              size="small"
              showInfo={false}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic
              title="HK Market Themes"
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
              title="US Market Themes"
              value={
                state.statistics?.market_breakdown.find(m => m.market === 'US')?.count || 0
              }
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
      </Row>
      
      {/* Second Row Statistics */}
      <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic
              title="LLM Generated"
              value={state.statistics?.description_statistics?.source_breakdown?.llm_generated || 0}
              valueStyle={{ color: '#52c41a' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic
              title="Simple Template"
              value={state.statistics?.description_statistics?.source_breakdown?.simple_template || 0}
              valueStyle={{ color: '#faad14' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic
              title="Manual"
              value={state.statistics?.description_statistics?.source_breakdown?.manual || 0}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={12} lg={6}>
          <Card>
            <Statistic
              title="Matched News"
              value={state.statistics?.match_statistics.news_with_themes || 0}
              valueStyle={{ color: '#722ed1' }}
            />
          </Card>
        </Col>
      </Row>

      {/* Performance Metrics */}
      {state.statistics && (
        <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
          <Col span={24}>
            <Card title="System Performance" size="small">
              <Row gutter={16}>
                <Col span={8}>
                  <Text type="secondary">Embedding Model:</Text>{' '}
                  <Tag color="blue">{state.statistics.embedding_model}</Tag>
                </Col>
                <Col span={8}>
                  <Text type="secondary">Vector Dimension:</Text>{' '}
                  <Tag color="purple">{state.statistics.embedding_dimension}</Tag>
                </Col>
                <Col span={8}>
                  <Text type="secondary">Avg Similarity:</Text>{' '}
                  <Tag color="green">
                    {(state.statistics.match_statistics.avg_similarity * 100).toFixed(1)}%
                  </Tag>
                </Col>
              </Row>
            </Card>
          </Col>
        </Row>
      )}

      {/* Action Buttons - Optimized Layout */}
      <Card style={{ marginBottom: '24px' }}>
        {/* Operation Guide */}
        <Alert
          message={
            <div>
              <InfoCircleOutlined style={{ marginRight: '8px' }} />
              <Text strong>Operation Guide:</Text>
              <Text type="secondary" style={{ marginLeft: '8px' }}>
                Recommended daily use: "Sync Theme Data" and "Test Match", use advanced maintenance operations with caution
              </Text>
            </div>
          }
          type="info"
          showIcon={false}
          style={{ marginBottom: '16px' }}
        />

        {/* Common Operations */}
        <div style={{ marginBottom: '16px' }}>
          <Text strong style={{ marginBottom: '8px', display: 'block' }}>📌 Common Operations</Text>
          <Space size="middle" wrap>
            <Tooltip title="Generate descriptions and vectors for new themes, suitable after Futu crawler adds new themes">
              <Button
                type="primary"
                icon={<SyncOutlined />}
                onClick={handleSyncThemes}
                size="large"
              >
                🔄 Sync Theme Data
              </Button>
            </Tooltip>
            
            <Tooltip title="Use LLM to enhance description quality, recommended when coverage < 90%">
              <Button
                type="default"
                icon={<ExperimentOutlined />}
                onClick={handleOptimizeDescriptions}
                size="large"
                style={{ 
                  background: 'linear-gradient(135deg, #667eea 0%, #764ba2 100%)',
                  color: 'white',
                  border: 'none'
                }}
              >
                ✨ Optimize Descriptions
              </Button>
            </Tooltip>
            
            <Tooltip title="Enter news text to test theme matching in real-time">
              <Button
                icon={<SearchOutlined />}
                onClick={() => setState(prev => ({ ...prev, testModalVisible: true }))}
                size="large"
              >
                🧪 Test Match
              </Button>
            </Tooltip>
            
            <Button
              icon={<ReloadOutlined />}
              onClick={() => {
                loadStatistics();
                loadThemes();
              }}
            >
              Refresh Stats
            </Button>
          </Space>
        </div>

        <Divider />

        {/* Advanced Options and Market Filter */}
        <Row justify="space-between" align="middle">
          <Col>
            {/* Advanced Options Dropdown Menu */}
            <Dropdown
              overlay={
                <Menu>
                  <Menu.Item
                    key="force-vectors"
                    icon={<WarningOutlined style={{ color: '#ff4d4f' }} />}
                    onClick={handleForceUpdateVectors}
                    danger
                  >
                    <Text type="danger">Force Update All Vectors</Text>
                    <div style={{ fontSize: '12px', color: '#999', marginTop: '4px' }}>
                      ⚠️ Only for model upgrade (20-30 mins)
                    </div>
                  </Menu.Item>
                  <Menu.Item
                    key="force-descriptions"
                    icon={<WarningOutlined style={{ color: '#ff4d4f' }} />}
                    onClick={handleForceRegenerateDescriptions}
                    danger
                  >
                    <Text type="danger">Regenerate All Descriptions</Text>
                    <div style={{ fontSize: '12px', color: '#999', marginTop: '4px' }}>
                      ⚠️ Only after Prompt optimization (15-25 mins)
                    </div>
                  </Menu.Item>
                  <Menu.Divider />
                  <Menu.Item
                    key="logs"
                    icon={<InfoCircleOutlined />}
                    disabled
                  >
                    View Execution Logs (In Development)
                  </Menu.Item>
                </Menu>
              }
              trigger={['click']}
            >
              <Button
                icon={<SettingOutlined />}
                style={{ color: '#999' }}
              >
                ⚙️ Advanced Options <DownOutlined />
              </Button>
            </Dropdown>
          </Col>

          <Col>
            {/* Market Filter */}
            <Space>
              <Text>Market Filter:</Text>
              <Select
                style={{ width: 120 }}
                placeholder="All Markets"
                allowClear
                value={state.selectedMarket}
                onChange={handleMarketChange}
              >
                <Option value="HK">Hong Kong (HK)</Option>
                <Option value="US">United States (US)</Option>
                <Option value="CN">China (CN)</Option>
              </Select>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* Theme List Table */}
      <Card title="Investment Themes List" extra={<Text type="secondary">Total: {state.total}</Text>}>
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
            showTotal: (total) => `Total ${total} themes`,
            onChange: (page, pageSize) => {
              setState(prev => ({ ...prev, currentPage: page, pageSize: pageSize || 20 }));
              loadThemes(page, state.selectedMarket);
            }
          }}
          size="small"
        />
      </Card>

      {/* Test Match Modal */}
      <Modal
        title="Test Theme Matching"
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
            <Text strong>Enter News Text:</Text>
            <TextArea
              rows={4}
              placeholder="Please enter news title and content..."
              value={state.testNewsText}
              onChange={(e) => setState(prev => ({ ...prev, testNewsText: e.target.value }))}
              style={{ marginTop: '8px' }}
            />
          </div>

          <div>
            <Text strong>Select Market:</Text>
            <Select
              style={{ width: '100%', marginTop: '8px' }}
              value={state.testMarket || ''}
              onChange={(value: string) => setState(prev => ({ ...prev, testMarket: value === '' ? undefined : value as 'HK' | 'US' | 'CN' }))}
            >
              <Option value="HK">Hong Kong (HK)</Option>
              <Option value="US">United States (US)</Option>
              <Option value="">All Markets</Option>
            </Select>
          </div>

          <Button
            type="primary"
            icon={<SearchOutlined />}
            onClick={handleTestMatch}
            loading={state.testLoading}
            block
          >
            Match Themes
          </Button>

          {/* Match Results */}
          {state.testResults.length > 0 && (
            <div>
              <Divider />
              <Title level={5}>Match Results (Top {state.testResults.length}):</Title>
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
                        <Tooltip title={`Similarity: ${result.similarity.toFixed(4)}`}>
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

