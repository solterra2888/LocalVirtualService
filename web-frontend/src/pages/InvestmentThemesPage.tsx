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
  listThemes,
  initializeThemeEmbeddings,
  matchNewsToThemes,
  fillThemeDescriptions,
  pollTaskUntilComplete
} from '../services/investmentThemes';
import {
  InvestmentTheme,
  MARKET_COLORS,
  MARKET_LABELS,
  ThemeMatch,
  getDescriptionSourceLabel
} from '../types/investmentThemes';
import { getErrorMessage } from '../utils/errorHandler';

const { Title, Text, Paragraph } = Typography;
const { TextArea } = Input;

interface PageState {
  loading: boolean;
  themes: InvestmentTheme[];
  total: number;
  currentPage: number;
  pageSize: number;
  initializing: boolean;
  testModalVisible: boolean;
  testNewsText: string;
  testResults: ThemeMatch[];
  testLoading: boolean;
}

const InvestmentThemesPage: React.FC = () => {
  const [state, setState] = useState<PageState>({
    loading: false,
    themes: [],
    total: 0,
    currentPage: 1,
    pageSize: 20,
    initializing: false,
    testModalVisible: false,
    testNewsText: '',
    testResults: [],
    testLoading: false
  });


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

  // 🌟 Sync Theme Data (merged functionality - using Celery async tasks)
  const handleSyncThemes = async () => {
    Modal.confirm({
      title: 'Sync Theme Data',
      icon: <SyncOutlined />,
      width: 600,
      content: (
        <div>
          <Alert
            message="This operation will complete the following tasks"
            description={
              <div style={{ marginTop: '12px' }}>
                <div style={{ marginBottom: '8px' }}>
                  Generate detailed descriptions for new themes (using LLM)
                </div>
                <div style={{ marginBottom: '8px' }}>
                  Generate vectors for new themes (incremental update, skip existing)
                </div>
              </div>
            }
            type="info"
            showIcon
            style={{ marginBottom: '16px' }}
          />
          <Paragraph type="secondary">
            Estimated Time: 2-10 minutes (background async execution using Celery)
          </Paragraph>
        </div>
      ),
      okText: 'Start Sync',
      cancelText: 'Cancel',
      onOk: async () => {
        try {
          // Step 1: Submit description generation task
          message.loading({ content: 'Step 1/2: Submitting theme description generation task...', key: 'sync', duration: 0 });
          const descTask = await fillThemeDescriptions(false);
          
          if (!descTask.success) {
            message.destroy('sync');
            message.error(`Failed to submit description task: ${descTask.message}`);
            return;
          }
          
          // Poll description task
          message.loading({ content: 'Step 1/2: Generating theme descriptions (0%)...', key: 'sync', duration: 0 });
          const descResult = await pollTaskUntilComplete(
            descTask.task_id,
            (status) => {
              const progressText = status.message || `Progress: ${status.progress}%`;
              message.loading({ 
                content: `Step 1/2: ${progressText}`, 
                key: 'sync', 
                duration: 0 
              });
            }
          );
          
          if (descResult.status === 'FAILURE') {
            message.destroy('sync');
            message.error(`Description generation failed: ${descResult.error || 'Unknown error'}`);
            return;
          }
          
          // Step 2: Submit vector generation task
          message.loading({ content: 'Step 2/2: Submitting theme vector generation task...', key: 'sync', duration: 0 });
          const vectorTask = await initializeThemeEmbeddings(false);
          
          if (!vectorTask.success) {
            message.destroy('sync');
            message.warning(`Descriptions generated, but failed to submit vector task: ${vectorTask.message}`);
            return;
          }
          
          // Poll vector task
          message.loading({ content: 'Step 2/2: Generating theme vectors (0%)...', key: 'sync', duration: 0 });
          const vectorResult = await pollTaskUntilComplete(
            vectorTask.task_id,
            (status) => {
              const progressText = status.message || `Progress: ${status.progress}%`;
              message.loading({ 
                content: `Step 2/2: ${progressText}`, 
                key: 'sync', 
                duration: 0 
              });
            }
          );
          
          message.destroy('sync');
          
          if (vectorResult.status === 'FAILURE') {
            message.warning(`Descriptions generated, but vector generation failed: ${vectorResult.error || 'Unknown error'}`);
          } else {
            const descStats = descResult.result?.statistics || descResult.result || {};
            const vectorStats = vectorResult.result?.statistics || vectorResult.result || {};
            message.success({
              content: `🎉 Sync completed! Descriptions: ${(descStats as any).processed || 0}/${(descStats as any).total || 0}, Vectors: ${(vectorStats as any).processed || 0}/${(vectorStats as any).total || 0}`,
              duration: 6,
            });
            
            // Refresh data
            setTimeout(() => {
              loadThemes();
            }, 2000);
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
    Modal.confirm({
      title: 'Optimize Description Quality',
      icon: <ExperimentOutlined />,
      width: 600,
      content: (
        <div>
          <Alert
            message="Use LLM to enhance theme description quality"
            description={
              <div style={{ marginTop: '12px' }}>
                <div style={{ marginBottom: '8px' }}>
                  Generate detailed descriptions for themes
                </div>
                <div>
                  Upgrade all to LLM-generated detailed descriptions
                </div>
              </div>
            }
            type="success"
            showIcon
            style={{ marginBottom: '16px' }}
          />
          <Paragraph type="secondary">
            Use Case: When description coverage is low or many simple template descriptions
          </Paragraph>
        </div>
      ),
      okText: 'Start Optimization',
      cancelText: 'Cancel',
      onOk: async () => {
        try {
          // Submit task
          message.loading({ content: 'Submitting description optimization task...', key: 'optimize', duration: 0 });
          const task = await fillThemeDescriptions(false);
          
          if (!task.success) {
            message.destroy('optimize');
            message.error(`Failed to submit task: ${task.message}`);
            return;
          }
          
          // Poll task
          message.loading({ content: 'Optimizing theme descriptions (0%)...', key: 'optimize', duration: 0 });
          const result = await pollTaskUntilComplete(
            task.task_id,
            (status) => {
              const progressText = status.message || `Progress: ${status.progress}%`;
              message.loading({ 
                content: `Optimizing: ${progressText}`, 
                key: 'optimize', 
                duration: 0 
              });
            }
          );
          
          message.destroy('optimize');
          
          if (result.status === 'FAILURE') {
            message.error(`Optimization failed: ${result.error || 'Unknown error'}`);
          } else {
            const stats = result.result?.statistics || result.result || {};
            message.success({
              content: `🎉 Optimization completed! Processed: ${(stats as any).processed || 0}/${(stats as any).total || 0}`,
              duration: 5,
            });
            
            // Refresh data
            setTimeout(() => {
              loadThemes();
            }, 2000);
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
      title: 'Warning: Force Update All Vectors',
      icon: <WarningOutlined style={{ color: '#ff4d4f' }} />,
      width: 650,
      content: (
        <div>
          <Alert
            message="This is a high-risk, high-cost operation"
            description={
              <div style={{ marginTop: '12px' }}>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  Will regenerate vectors for <Text strong style={{ color: '#ff4d4f' }}>all themes</Text> (150+)
                </div>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  Estimated Time: <Text strong>20-30 minutes</Text>
                </div>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  Will repeatedly call LLM to generate descriptions (consuming large API quota)
                </div>
              </div>
            }
            type="error"
            showIcon
            style={{ marginBottom: '16px' }}
          />
          <Divider />
          <Paragraph>
            <Text strong>Applicable Scenarios:</Text>
          </Paragraph>
          <ul style={{ paddingLeft: '20px', marginTop: '8px' }}>
            <li>Embedding model upgrade (e.g., text-embedding-v2 → v3)</li>
            <li>Vector dimension change</li>
          </ul>
          <Paragraph style={{ marginTop: '12px' }}>
            <Text strong>Not Applicable Scenarios:</Text>
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
          // Submit task
          message.loading({ content: 'Submitting force update task...', key: 'forceUpdate', duration: 0 });
          const task = await initializeThemeEmbeddings(true);
          
          if (!task.success) {
            message.destroy('forceUpdate');
            message.error(`Failed to submit task: ${task.message}`);
            setState(prev => ({ ...prev, initializing: false }));
            return;
          }
          
          // Poll task
          message.loading({ content: 'Force updating all vectors (0%)...', key: 'forceUpdate', duration: 0 });
          const result = await pollTaskUntilComplete(
            task.task_id,
            (status) => {
              const progressText = status.message || `Progress: ${status.progress}%`;
              message.loading({ 
                content: `Force updating: ${progressText}`, 
                key: 'forceUpdate', 
                duration: 0 
              });
            }
          );
          
          message.destroy('forceUpdate');
          
          if (result.status === 'FAILURE') {
            message.error(`Update failed: ${result.error || 'Unknown error'}`);
          } else {
            const stats = result.result?.statistics || result.result || {};
            message.success(
              `Force update completed! Processed: ${(stats as any).processed || 0}, ` +
              `Failed: ${(stats as any).failed || 0}`
            );
            
            // Refresh data
            await loadThemes();
          }
        } catch (error) {
          message.destroy('forceUpdate');
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
      title: 'Warning: Regenerate All Descriptions',
      icon: <WarningOutlined style={{ color: '#ff4d4f' }} />,
      width: 650,
      content: (
        <div>
          <Alert
            message="This is a high-cost operation"
            description={
              <div style={{ marginTop: '12px' }}>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  Will call LLM to regenerate <Text strong style={{ color: '#ff4d4f' }}>all descriptions</Text> (150+)
                </div>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  Estimated Time: <Text strong>15-25 minutes</Text>
                </div>
                <div style={{ marginBottom: '8px', color: '#ff4d4f' }}>
                  LLM API calling cost is high
                </div>
              </div>
            }
            type="error"
            showIcon
            style={{ marginBottom: '16px' }}
          />
          <Divider />
          <Paragraph>
            <Text strong>Applicable Scenarios:</Text>
          </Paragraph>
          <ul style={{ paddingLeft: '20px', marginTop: '8px' }}>
            <li>After major Prompt Template optimization</li>
            <li>Found historical description quality is generally poor</li>
          </ul>
          <Paragraph style={{ marginTop: '12px' }}>
            <Text strong>Not Applicable Scenarios:</Text>
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
          // Submit task
          message.loading({ content: 'Submitting force regenerate task...', key: 'forceRegen', duration: 0 });
          const task = await fillThemeDescriptions(true);
          
          if (!task.success) {
            message.destroy('forceRegen');
            message.error(`Failed to submit task: ${task.message}`);
            return;
          }
          
          // Poll task
          message.loading({ content: 'Regenerating all descriptions (0%)...', key: 'forceRegen', duration: 0 });
          const result = await pollTaskUntilComplete(
            task.task_id,
            (status) => {
              const progressText = status.message || `Progress: ${status.progress}%`;
              message.loading({ 
                content: `Regenerating: ${progressText}`, 
                key: 'forceRegen', 
                duration: 0 
              });
            }
          );
          
          message.destroy('forceRegen');
          
          if (result.status === 'FAILURE') {
            message.error(`Operation failed: ${result.error || 'Unknown error'}`);
          } else {
            const stats: any = result.result?.statistics || result.result || {};
            message.success({
              content: `🎉 All descriptions regenerated! Processed: ${stats.processed || 0}/${stats.total || 0}`,
              duration: 5,
            });
            
            setTimeout(() => {
              loadThemes();
            }, 2000);
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
    loadThemes();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // View description details
  const showDescriptionModal = (theme: InvestmentTheme) => {
    const sourceLabel = getDescriptionSourceLabel(theme.description_source);
    Modal.info({
      title: `${theme.theme_name} Theme Description`,
      width: 800,
      okText: 'Close',
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
      title: 'Description Preview',
      dataIndex: 'description',
      key: 'description',
      width: 300,
      ellipsis: true,
      render: (text: string, record: InvestmentTheme) => (
        <Text 
          ellipsis 
          style={{ cursor: text ? 'pointer' : 'default', color: text ? undefined : '#999' }}
          onClick={() => text && showDescriptionModal(record)}
        >
          {text || 'Not generated'}
        </Text>
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


  return (
    <div style={{ padding: '24px' }}>
      {/* Page Title */}
      <div style={{ marginBottom: '24px' }}>
        <Title level={2}>
          <FundOutlined /> Investment Themes Vector Retrieval
        </Title>
        <Paragraph type="secondary">
          Intelligent theme matching system based on Reverse Capital Embedding technique, automatically associating news with 150+ investment themes through semantic analysis
        </Paragraph>
      </div>


      {/* Action Buttons - Optimized Layout */}
      <Card style={{ marginBottom: '24px' }}>

         {/* Operations */}
         <div style={{ marginBottom: '16px' }}>
           <Space size="middle" wrap>
            <Tooltip title="Generate descriptions and vectors for new themes, suitable after Futu crawler adds new themes">
              <Button
                type="primary"
                icon={<SyncOutlined />}
                onClick={handleSyncThemes}
              >
                Sync Theme Data
              </Button>
            </Tooltip>
            
            <Tooltip title="Use LLM to enhance description quality, recommended when coverage < 90%">
              <Button
                type="default"
                icon={<ExperimentOutlined />}
                onClick={handleOptimizeDescriptions}
              >
                Optimize Descriptions
              </Button>
            </Tooltip>
            
            <Tooltip title="Enter news text to test theme matching in real-time">
              <Button
                icon={<SearchOutlined />}
                onClick={() => setState(prev => ({ ...prev, testModalVisible: true }))}
              >
                Test Match
              </Button>
            </Tooltip>
            
            {/* Advanced Options Dropdown Menu */}
            <Dropdown
              overlay={
                <Menu>
                  <Menu.Item
                    key="force-vectors"
                    icon={<WarningOutlined style={{ color: '#ff4d4f' }} />}
                    onClick={handleForceUpdateVectors}
                  >
                    <Text type="danger">Force Update All Vectors</Text>
                    <div style={{ fontSize: '12px', color: '#999', marginTop: '4px' }}>
                      Only for model upgrade (20-30 mins)
                    </div>
                  </Menu.Item>
                  <Menu.Item
                    key="force-descriptions"
                    icon={<WarningOutlined style={{ color: '#ff4d4f' }} />}
                    onClick={handleForceRegenerateDescriptions}
                  >
                    <Text type="danger">Regenerate All Descriptions</Text>
                    <div style={{ fontSize: '12px', color: '#999', marginTop: '4px' }}>
                      Only after Prompt optimization (15-25 mins)
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
              >
                Advanced Options <DownOutlined />
              </Button>
            </Dropdown>
            
            <Button
              icon={<ReloadOutlined />}
              onClick={() => {
                loadThemes();
              }}
            >
              Refresh Data
            </Button>
          </Space>
        </div>

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
              loadThemes(page);
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

