import React, { useState, useEffect, useCallback } from 'react';
import { 
  Card, 
  Button, 
  Input, 
  Switch, 
  Select, 
  Tabs, 
  message, 
  Row, 
  Col, 
  Typography, 
  Space, 
  Badge,
  Divider,
  Slider,
  InputNumber,
  Alert,
  Tag,
  Statistic,
  Spin,
  Tooltip,
  Table,
  Segmented,
  Progress
} from 'antd';
import { 
  SettingOutlined, 
  RobotOutlined,
  ExperimentOutlined,
  SaveOutlined,
  ReloadOutlined,
  EyeInvisibleOutlined,
  EyeTwoTone,
  WarningOutlined,
  DragOutlined,
  PlayCircleOutlined,
  LineChartOutlined,
  DownloadOutlined
} from '@ant-design/icons';
import { 
  LineChart, 
  Line, 
  AreaChart, 
  Area, 
  BarChart, 
  Bar, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip as RechartsTooltip, 
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell
} from 'recharts';
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
} from '@dnd-kit/core';
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  verticalListSortingStrategy,
} from '@dnd-kit/sortable';
import {
  useSortable,
} from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { llmConfigService, LLMConfig, LLMUsageStats } from '../services/llmConfigService';
import { api } from '../services/api';

const { Title, Paragraph, Text } = Typography;
const { TabPane } = Tabs;
const { Option } = Select;

// 可拖拽的排序项组件
interface SortableItemProps {
  id: string;
  provider: LLMProvider;
  index: number;
}

const SortableItem: React.FC<SortableItemProps> = ({ id, provider, index }) => {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({ id });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  };

  return (
    <div
      ref={setNodeRef}
      style={style}
      {...attributes}
      {...listeners}
    >
      <Card
        size="small"
        style={{
          marginBottom: 8,
          cursor: 'grab',
          border: isDragging ? '2px dashed #1890ff' : '1px solid #d9d9d9',
        }}
        bodyStyle={{ padding: '12px 16px' }}
      >
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <DragOutlined style={{ marginRight: 8, color: '#999' }} />
            <Badge 
              count={index + 1} 
              style={{ backgroundColor: '#52c41a', marginRight: 12 }}
            />
            <span style={{ fontSize: '16px', marginRight: 8 }}>{provider.icon}</span>
            <div>
              <Text strong>{provider.displayName}</Text>
              <br />
              <Text type="secondary" style={{ fontSize: '12px' }}>
                {provider.description}
              </Text>
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <Badge 
              status={provider.enabled ? 'success' : 'default'} 
              text={provider.enabled ? 'Enabled' : 'Disabled'} 
            />
            {provider.id === 'qwen' && (
              <Tag color="blue">Default</Tag>
            )}
          </div>
        </div>
      </Card>
    </div>
  );
};

interface LLMProvider {
  id: string;
  name: string;
  displayName: string;
  enabled: boolean;
  apiKey: string;
  baseUrl: string;
  models: string[];
  defaultModel: string;
  status: 'connected' | 'disconnected' | 'testing' | 'error';
  description: string;
  icon: string;
  maxTokens: number;
  temperature: number;
  topP: number;
}

const LLMConfigPage: React.FC = () => {
  const [config, setConfig] = useState<LLMConfig | null>(null);
  const [usageStats, setUsageStats] = useState<LLMUsageStats[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [testingProvider, setTestingProvider] = useState<string | null>(null);
  const [testingGlobal, setTestingGlobal] = useState<boolean>(false);
  const [saveStatus, setSaveStatus] = useState<'idle' | 'saving' | 'saved' | 'error'>('idle');
  const [hasUnsavedChanges, setHasUnsavedChanges] = useState<boolean>(false);
  
  // 拖拽排序相关状态
  const [fallbackOrder, setFallbackOrder] = useState<string[]>([]);
  
  // 监控统计增强状态
  const [monitoringTimeRange, setMonitoringTimeRange] = useState<string>('30d');
  const [monitoringProvider, setMonitoringProvider] = useState<string | undefined>(undefined);
  const [monitoringLoading, setMonitoringLoading] = useState<boolean>(false);
  const [dashboardData, setDashboardData] = useState<any>(null);
  const [timeSeriesData, setTimeSeriesData] = useState<any[]>([]);
  const [modelStatsData, setModelStatsData] = useState<any[]>([]);
  const [templateStatsData, setTemplateStatsData] = useState<any[]>([]);
  const [errorAnalysisData, setErrorAnalysisData] = useState<any[]>([]);
  const [detailedLogs, setDetailedLogs] = useState<any[]>([]);
  const [logsTotal, setLogsTotal] = useState<number>(0);
  const [logsPage, setLogsPage] = useState<number>(1);
  const [logsPageSize] = useState<number>(20);
  
  // 拖拽传感器
  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  // 加载配置数据
  useEffect(() => {
    loadConfig();
    loadUsageStats();
  }, []);

  const loadConfig = async () => {
    try {
      setLoading(true);
      setLoadError(null);
      const data = await llmConfigService.getConfig();
      
      // 为每个provider添加status字段（API返回的数据没有status）
      const configWithStatus: LLMConfig = {
        ...data,
        providers: data.providers.map(p => ({
          ...p,
          status: 'disconnected' as const
        }))
      };
      
      setConfig(configWithStatus);
      // 初始化故障转移顺序
      setFallbackOrder(data.fallbackOrder || []);
    } catch (error: any) {
      console.error('Failed to load configuration:', error);
      setLoadError(error?.response?.data?.detail || error.message || 'Failed to load configuration');
      message.error('Failed to load configuration, please check network connection');
    } finally {
      setLoading(false);
    }
  };

  const loadUsageStats = async () => {
    try {
      const stats = await llmConfigService.getUsageStats('30d');
      setUsageStats(stats);
    } catch (error) {
      console.error('Failed to load usage statistics:', error);
      // Don't show error message, statistics are secondary
    }
  };

  // 加载监控数据
  const loadMonitoringData = useCallback(async () => {
    if (!config) return;
    
    setMonitoringLoading(true);
    try {
      // 并行加载所有监控数据
      const [dashboard, timeSeries, modelStats, templateStats, errorAnalysis, logs] = await Promise.all([
        llmConfigService.getMonitoringDashboard(monitoringTimeRange, monitoringProvider),
        llmConfigService.getTimeSeriesData(monitoringTimeRange, monitoringProvider),
        llmConfigService.getModelStats(monitoringTimeRange, monitoringProvider),
        llmConfigService.getTemplateStats(monitoringTimeRange, monitoringProvider),
        llmConfigService.getErrorAnalysis(monitoringTimeRange, monitoringProvider),
        llmConfigService.getDetailedLogs(monitoringTimeRange, monitoringProvider, undefined, undefined, logsPageSize, (logsPage - 1) * logsPageSize)
      ]);

      setDashboardData(dashboard);
      setTimeSeriesData(timeSeries.data || []);
      setModelStatsData(modelStats.data || []);
      setTemplateStatsData(templateStats.data || []);
      setErrorAnalysisData(errorAnalysis.data || []);
      setDetailedLogs(logs.data || []);
      setLogsTotal(logs.total || 0);
    } catch (error) {
      console.error('Failed to load monitoring data:', error);
      message.error('加载监控数据失败');
    } finally {
      setMonitoringLoading(false);
    }
  }, [config, monitoringTimeRange, monitoringProvider, logsPage, logsPageSize]);

  // 监听时间范围和提供商筛选变化
  useEffect(() => {
    loadMonitoringData();
  }, [loadMonitoringData]);

  const updateProvider = (providerId: string, updates: Partial<LLMProvider>) => {
    if (!config) return;
    
    setConfig(prev => {
      if (!prev) return prev;
      return {
        ...prev,
        providers: prev.providers.map(provider => 
          provider.id === providerId ? { ...provider, ...updates } : provider
        )
      };
    });
    
    // Mark as having unsaved changes (except status field)
    if (!updates.status) {
      setHasUnsavedChanges(true);
    }
  };

  // Handle drag and drop sorting
  const handleDragEnd = (event: any) => {
    const { active, over } = event;

    if (active.id !== over.id) {
      setFallbackOrder((items) => {
        const oldIndex = items.indexOf(active.id);
        const newIndex = items.indexOf(over.id);
        const newOrder = arrayMove(items, oldIndex, newIndex);
        setHasUnsavedChanges(true);
        return newOrder;
      });
    }
  };

  // Reset fallback order
  const resetFallbackOrder = () => {
    if (!config) return;
    const defaultOrder = ['qwen', 'doubao', 'glm'];
    setFallbackOrder(defaultOrder);
    setHasUnsavedChanges(true);
  };

  // Test fallback order
  const testFallbackOrder = async () => {
    if (!config) return;
    
    setTestingGlobal(true);
    try {
      console.log('Testing fallback order:', fallbackOrder);
      console.log('Available providers:', config.providers.map(p => ({ id: p.id, enabled: p.enabled })));
      
      // Test each provider in the fallback order
      const testResults = [];
      
      if (fallbackOrder.length === 0) {
        message.warning('No fallback order configured. Please configure providers first.');
        return;
      }
      
      for (let i = 0; i < fallbackOrder.length; i++) {
        const providerId = fallbackOrder[i];
        const provider = config.providers.find(p => p.id === providerId);
        
        if (!provider) {
          testResults.push({
            provider: providerId,
            priority: i + 1,
            status: 'error',
            message: 'Provider not found'
          });
          continue;
        }
        
        if (!provider.enabled) {
          testResults.push({
            provider: provider.displayName,
            priority: i + 1,
            status: 'warning',
            message: 'Provider is disabled'
          });
          continue;
        }
        
        if (!provider.apiKey) {
          testResults.push({
            provider: provider.displayName,
            priority: i + 1,
            status: 'error',
            message: 'API key not configured'
          });
          continue;
        }
        
        try {
          console.log(`Testing provider ${i + 1}/${fallbackOrder.length}: ${provider.displayName}`);
          
          // Test the provider connection with timeout
          const testPromise = llmConfigService.testProvider(
            providerId,
            provider.apiKey,
            provider.baseUrl,
            provider.defaultModel,
            provider.maxTokens,
            provider.temperature,
            provider.topP
          );
          
          const timeoutPromise = new Promise((_, reject) => 
            setTimeout(() => reject(new Error('Test timeout after 30 seconds')), 30000)
          );
          
          const result = await Promise.race([testPromise, timeoutPromise]) as any;
          
          testResults.push({
            provider: provider.displayName,
            priority: i + 1,
            status: result.success ? 'success' : 'error',
            message: result.success ? 'Connection successful' : (result.error || 'Connection failed')
          });
          
          console.log(`Provider ${provider.displayName} test completed:`, result.success ? 'success' : 'failed');
        } catch (error: any) {
          console.error(`Provider ${provider.displayName} test failed:`, error);
          testResults.push({
            provider: provider.displayName,
            priority: i + 1,
            status: 'error',
            message: error?.response?.data?.detail || error.message || 'Test failed'
          });
        }
      }
      
      // Display test results
      const resultMessages = testResults.map(result => {
        const icon = result.status === 'success' ? '✅' : 
                    result.status === 'warning' ? '⚠️' : '❌';
        return `${icon} Priority ${result.priority}: ${result.provider} - ${result.message}`;
      }).join('\n');
      
      const successCount = testResults.filter(r => r.status === 'success').length;
      const warningCount = testResults.filter(r => r.status === 'warning').length;
      const errorCount = testResults.filter(r => r.status === 'error').length;
      
      if (errorCount === 0 && warningCount === 0) {
        message.success(
          `Fallback order test completed successfully! (${successCount}/${testResults.length} providers working)\n\n${resultMessages}`,
          8
        );
      } else if (errorCount === 0) {
        message.warning(
          `Fallback order test completed with warnings (${successCount}/${testResults.length} providers working):\n\n${resultMessages}`,
          10
        );
      } else {
        message.error(
          `Fallback order test found issues (${successCount}/${testResults.length} providers working):\n\n${resultMessages}`,
          10
        );
      }
      
    } catch (error: any) {
      console.error('Fallback order test failed:', error);
      console.error('Error details:', {
        message: error.message,
        response: error.response?.data,
        status: error.response?.status
      });
      
      let errorMessage = 'Test failed, please check configuration';
      if (error.response?.status === 401) {
        errorMessage = 'Authentication required. Please login first.';
      } else if (error.response?.status === 403) {
        errorMessage = 'Permission denied. Admin access required.';
      } else if (error.message) {
        errorMessage = error.message;
      }
      
      message.error(errorMessage);
    } finally {
      setTestingGlobal(false);
    }
  };

  const testConnection = async (providerId: string) => {
    if (!config) return;
    
    setTestingProvider(providerId);
    updateProvider(providerId, { status: 'testing' });
    
    try {
      const provider = config.providers.find(p => p.id === providerId);
      if (!provider?.apiKey) {
        updateProvider(providerId, { status: 'error' });
        message.error('API key cannot be empty');
        setTestingProvider(null);
        return;
      }

      const result = await llmConfigService.testProvider(
        providerId,
        provider.apiKey,
        provider.baseUrl,
        provider.defaultModel,
        provider.maxTokens,
        provider.temperature,
        provider.topP
      );
      
      if (result.success) {
        updateProvider(providerId, { status: 'connected' });
        message.success(
          `${provider.displayName} connection test successful! Please click the "Save Configuration" button in the top right to save to database`,
          5
        );
      } else {
        updateProvider(providerId, { status: 'error' });
        message.error(
          `${provider.displayName} connection test failed: ${result.error || 'Unknown error'}`,
          5
        );
      }
    } catch (error: any) {
      updateProvider(providerId, { status: 'error' });
      message.error(
        `Connection test failed: ${error?.response?.data?.detail || error.message || 'Network error'}`,
        5
      );
    } finally {
      setTestingProvider(null);
    }
  };

  const saveConfig = async () => {
    if (!config) return;
    
    setSaveStatus('saving');
    try {
      // 创建包含故障转移顺序的配置对象
      const configToSave = {
        ...config,
        fallbackOrder: fallbackOrder
      };
      
      await llmConfigService.saveConfig(configToSave);
      setSaveStatus('saved');
      message.success(
        'Configuration saved successfully, some changes may require service restart to take effect',
        4
      );
      
      // Reset save status after 2 seconds
      setTimeout(() => setSaveStatus('idle'), 2000);
      
      // Reload configuration to ensure data synchronization
      await loadConfig();
      
      // Clear unsaved changes flag
      setHasUnsavedChanges(false);
    } catch (error: any) {
      setSaveStatus('error');
      message.error(
        `Configuration save failed: ${error?.response?.data?.detail || error.message || 'Please try again'}`,
        5
      );
      setTimeout(() => setSaveStatus('idle'), 2000);
    }
  };

  const testGlobalSettings = async () => {
    setTestingGlobal(true);
    try {
      console.log('Starting global settings test...');
      console.log('API Base URL:', process.env.REACT_APP_API_BASE_URL || '/api');
      console.log('Token:', localStorage.getItem('access_token'));
      
      // Add timeout to prevent hanging
      const testPromise = api.post('/llm/test-global');
      const timeoutPromise = new Promise((_, reject) => 
        setTimeout(() => reject(new Error('Test timeout after 60 seconds')), 60000)
      );
      
      const result = await Promise.race([testPromise, timeoutPromise]) as any;
      
      // 显示测试结果
      const resultMessages = result.test_results.map((test: any) => {
        const icon = test.status === 'success' ? '✅' : 
                    test.status === 'warning' ? '⚠️' : 
                    test.status === 'error' ? '❌' : 'ℹ️';
        return `${icon} ${test.test}: ${test.message}`;
      }).join('\n');

      if (result.overall_status === 'success') {
        message.success(
          `Global settings test passed!\n\n${resultMessages}`,
          8
        );
      } else if (result.overall_status === 'warning') {
        message.warning(
          `Global settings test completed with warnings:\n\n${resultMessages}`,
          10
        );
      } else {
        message.error(
          `Global settings test found issues:\n\n${resultMessages}`,
          10
        );
      }
    } catch (error: any) {
      console.error('Global settings test failed:', error);
      console.error('Error details:', {
        message: error.message,
        response: error.response?.data,
        status: error.response?.status,
        statusText: error.response?.statusText
      });
      
      let errorMessage = 'Test failed';
      if (error.response?.status === 401) {
        errorMessage = 'Authentication required. Please login first.';
      } else if (error.response?.status === 403) {
        errorMessage = 'Permission denied. Admin access required.';
      } else if (error.response?.status === 404) {
        errorMessage = 'API endpoint not found. Please check backend configuration.';
      } else if (error.response?.data?.detail) {
        errorMessage = error.response.data.detail;
      } else if (error.message) {
        errorMessage = error.message;
      }
      
      message.error(`Test failed: ${errorMessage}`);
    } finally {
      setTestingGlobal(false);
    }
  };

  const getStatusText = (status: string) => {
    switch (status) {
      case 'connected':
        return 'Connected';
      case 'testing':
        return 'Testing...';
      case 'error':
        return 'Connection Failed';
      default:
        return 'Not Connected';
    }
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'connected':
        return 'success';
      case 'testing':
        return 'processing';
      case 'error':
        return 'error';
      default:
        return 'default';
    }
  };

  const renderProviderCard = (provider: LLMProvider) => (
    <Card 
      key={provider.id}
      style={{ marginBottom: 16 }}
      title={
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <span style={{ fontSize: '20px', marginRight: 8 }}>{provider.icon}</span>
            <div>
              <Title level={4} style={{ margin: 0 }}>{provider.displayName}</Title>
              <Text type="secondary">{provider.description}</Text>
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 16 }}>
            <Badge 
              status={getStatusColor(provider.status) as any} 
              text={getStatusText(provider.status)} 
            />
            <Switch
              checked={provider.enabled}
              onChange={(enabled) => updateProvider(provider.id, { enabled })}
            />
          </div>
        </div>
      }
    >
      <Row gutter={16}>
        <Col span={12}>
          <div style={{ marginBottom: 16 }}>
            <Text strong>API Key</Text>
            <Input.Password
              placeholder="Please enter API key"
              value={provider.apiKey}
              onChange={(e) => updateProvider(provider.id, { apiKey: e.target.value })}
              style={{ marginTop: 4, fontFamily: 'monospace' }}
              iconRender={(visible) => (visible ? <EyeTwoTone /> : <EyeInvisibleOutlined />)}
            />
          </div>
        </Col>
        <Col span={12}>
          <div style={{ marginBottom: 16 }}>
            <Text strong>API Endpoint</Text>
            <Input
              placeholder="API Base URL"
              value={provider.baseUrl}
              onChange={(e) => updateProvider(provider.id, { baseUrl: e.target.value })}
              style={{ marginTop: 4, fontFamily: 'monospace' }}
            />
          </div>
        </Col>
      </Row>

      <Row gutter={16}>
        <Col span={8}>
          <div style={{ marginBottom: 16 }}>
            <Text strong>Default Model</Text>
            <Select
              value={provider.defaultModel}
              onChange={(value) => updateProvider(provider.id, { defaultModel: value })}
              style={{ width: '100%', marginTop: 4 }}
            >
              {provider.models.map((model) => (
                <Option key={model} value={model}>
                  {model}
                </Option>
              ))}
            </Select>
          </div>
        </Col>
        <Col span={8}>
          <div style={{ marginBottom: 16 }}>
            <Text strong>Temperature ({provider.temperature})</Text>
            <Slider
              min={0}
              max={2}
              step={0.1}
              value={provider.temperature}
              onChange={(value) => updateProvider(provider.id, { temperature: value })}
              style={{ marginTop: 4 }}
            />
          </div>
        </Col>
        <Col span={8}>
          <div style={{ marginBottom: 16 }}>
            <Text strong>Max Tokens</Text>
            <InputNumber
              min={100}
              max={32000}
              value={provider.maxTokens}
              onChange={(value) => updateProvider(provider.id, { maxTokens: value || 1000 })}
              style={{ width: '100%', marginTop: 4 }}
            />
          </div>
        </Col>
      </Row>

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', paddingTop: 16, borderTop: '1px solid #f0f0f0' }}>
        <Space wrap>
          {provider.models.map((model) => (
            <Tag key={model} color="blue">{model}</Tag>
          ))}
        </Space>
        <Button
          type="primary"
          ghost
          icon={testingProvider === provider.id ? <ReloadOutlined spin /> : <ExperimentOutlined />}
          onClick={() => testConnection(provider.id)}
          disabled={!provider.apiKey || testingProvider === provider.id}
        >
          {testingProvider === provider.id ? 'Testing...' : 'Test Connection'}
        </Button>
      </div>
    </Card>
  );

  // 计算总体统计
  const totalStats = usageStats.reduce((acc, stat) => ({
    totalRequests: acc.totalRequests + stat.totalRequests,
    totalTokens: acc.totalTokens + stat.totalTokens,
    totalCost: acc.totalCost + stat.totalCost,
    avgSuccessRate: acc.avgSuccessRate + stat.successRate,
    avgResponseTime: acc.avgResponseTime + stat.avgResponseTime,
    count: acc.count + 1
  }), { totalRequests: 0, totalTokens: 0, totalCost: 0, avgSuccessRate: 0, avgResponseTime: 0, count: 0 });

  // 加载中状态
  if (loading) {
    return (
      <div style={{ 
        display: 'flex', 
        justifyContent: 'center', 
        alignItems: 'center', 
        minHeight: '100vh',
        flexDirection: 'column',
        gap: 16
      }}>
        <Spin size="large" />
        <Text type="secondary">Loading configuration...</Text>
      </div>
    );
  }

  // 加载错误状态
  if (loadError || !config) {
    return (
      <div style={{ padding: '24px' }}>
        <Alert
          message="Failed to load configuration"
          description={loadError || 'Unable to get configuration data'}
          type="error"
          showIcon
          icon={<WarningOutlined />}
          action={
            <Button size="small" onClick={loadConfig}>
              Reload
            </Button>
          }
        />
      </div>
    );
  }

  return (
    <div style={{ padding: '24px', background: '#f5f5f5', minHeight: '100vh' }}>
      {/* Header */}
      <div style={{ background: '#fff', padding: '24px', marginBottom: '24px', borderRadius: '8px' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div style={{ display: 'flex', alignItems: 'center' }}>
            <RobotOutlined style={{ fontSize: '32px', color: '#1890ff', marginRight: '12px' }} />
            <div>
              <Title level={2} style={{ margin: 0 }}>Large Language Model Configuration</Title>
              <Paragraph style={{ margin: 0, color: '#666' }}>
                Configure and manage Chinese mainstream large language model integration
              </Paragraph>
            </div>
          </div>
          <Space>
            <Button
              icon={<ReloadOutlined />}
              onClick={() => {
                loadConfig();
                loadUsageStats();
              }}
            >
              Refresh
            </Button>
            <Button
              type="primary"
              icon={saveStatus === 'saving' ? <ReloadOutlined spin /> : <SaveOutlined />}
              onClick={saveConfig}
              loading={saveStatus === 'saving'}
              size="large"
            >
              {saveStatus === 'saving' ? 'Saving...' : 'Save Configuration'}
            </Button>
          </Space>
        </div>
      </div>

      {/* Unsaved Changes Alert */}
      {hasUnsavedChanges && (
        <Alert
          message="You have unsaved configuration changes"
          description='Please click the "Save Configuration" button in the top right to save changes to database'
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
          closable
          onClose={() => setHasUnsavedChanges(false)}
        />
      )}

      {/* Save Status Alert */}
      {saveStatus === 'saved' && (
        <Alert
          message="Configuration saved successfully"
          description="Configuration has been saved to database, some changes may require service restart to take effect"
          type="success"
          showIcon
          style={{ marginBottom: 16 }}
          closable
          onClose={() => setSaveStatus('idle')}
        />
      )}

      {saveStatus === 'error' && (
        <Alert
          message="Failed to save configuration"
          description="Please check network connection and try again"
          type="error"
          showIcon
          style={{ marginBottom: 16 }}
          closable
          onClose={() => setSaveStatus('idle')}
        />
      )}

      <Tabs defaultActiveKey="providers" size="large">
        <TabPane 
          tab={
            <span>
              <RobotOutlined />
              Model Providers
            </span>
          } 
          key="providers"
        >
          <div>
            {config.providers.map(renderProviderCard)}
          </div>
        </TabPane>

        <TabPane 
          tab={
            <span>
              <SettingOutlined />
              Global Settings
            </span>
          } 
          key="global"
        >
          <Card title={
            <span>
              <SettingOutlined style={{ marginRight: 8 }} />
              Global Configuration
            </span>
          }
          extra={
            <Button
              type="primary"
              ghost
              icon={<ExperimentOutlined />}
              onClick={testGlobalSettings}
              loading={testingGlobal}
            >
              Test Global Settings
            </Button>
          }>
            <Row gutter={24}>
              <Col span={12}>
                <div style={{ marginBottom: 24 }}>
                  <Text strong>Default Model Provider</Text>
                  <Select
                    value={config.defaultProvider}
                    onChange={(value) => setConfig(prev => prev ? { ...prev, defaultProvider: value } : prev)}
                    style={{ width: '100%', marginTop: 8 }}
                  >
                    {config.providers.filter(p => p.enabled).map((provider) => (
                      <Option key={provider.id} value={provider.id}>
                        {provider.icon} {provider.displayName}
                      </Option>
                    ))}
                  </Select>
                </div>

                <div style={{ marginBottom: 24 }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div>
                      <Text strong>Enable Fallback</Text>
                      <br />
                      <Text type="secondary" style={{ fontSize: '12px' }}>
                        Automatically switch when primary model is unavailable
                      </Text>
                    </div>
                    <Switch
                      checked={config.fallbackEnabled}
                      onChange={(enabled) => setConfig(prev => prev ? { ...prev, fallbackEnabled: enabled } : prev)}
                    />
                  </div>
                </div>

                {/* 故障转移顺序配置 */}
                {config.fallbackEnabled && (
                  <div style={{ marginBottom: 24 }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 12 }}>
                      <div>
                        <Text strong>Fallback Order</Text>
                        <br />
                        <Text type="secondary" style={{ fontSize: '12px' }}>
                          Drag to adjust provider priority, smaller numbers have higher priority
                        </Text>
                      </div>
                      <Space>
                        <Tooltip title="Reset to default order">
                          <Button 
                            size="small" 
                            onClick={resetFallbackOrder}
                            icon={<ReloadOutlined />}
                          >
                            Reset
                          </Button>
                        </Tooltip>
                        <Tooltip title="Test fallback order">
                          <Button 
                            size="small" 
                            onClick={testFallbackOrder}
                            loading={testingGlobal}
                            icon={<PlayCircleOutlined />}
                          >
                            Test
                          </Button>
                        </Tooltip>
                      </Space>
                    </div>
                    
                    <Alert
                      message="Drag Instructions"
                      description="Drag the drag icon on the left to adjust order. Number 1 indicates highest priority. When the main provider fails, other providers will be tried in order."
                      type="info"
                      showIcon
                      style={{ marginBottom: 12 }}
                    />
                    
                    <DndContext
                      sensors={sensors}
                      collisionDetection={closestCenter}
                      onDragEnd={handleDragEnd}
                    >
                      <SortableContext items={fallbackOrder} strategy={verticalListSortingStrategy}>
                        {fallbackOrder.map((providerId, index) => {
                          const provider = config.providers.find(p => p.id === providerId);
                          if (!provider) return null;
                          
                          return (
                            <SortableItem
                              key={providerId}
                              id={providerId}
                              provider={provider}
                              index={index}
                            />
                          );
                        })}
                      </SortableContext>
                    </DndContext>
                    
                    {fallbackOrder.length === 0 && (
                      <Alert
                        message="No configured providers"
                        description="Please enable at least one provider in the 'Provider Configuration' tab first"
                        type="warning"
                        showIcon
                      />
                    )}
                  </div>
                )}
              </Col>

              <Col span={12}>
                <Alert
                  message="参数配置位置说明"
                  description={
                    <div>
                      <p style={{ marginBottom: 8 }}>Global Settings 只负责系统级策略（Provider选择、Fallback等）</p>
                      <p style={{ marginBottom: 8 }}>参数默认值（Temperature、Max Tokens、Timeout等）请在各个 <strong>Provider 配置</strong>中设置</p>
                      <p style={{ marginBottom: 0 }}>任务特定的优化参数请在 <strong>Prompt Templates</strong> 中配置</p>
                    </div>
                  }
                  type="info"
                  showIcon
                  style={{ marginBottom: 24 }}
                />
              </Col>
            </Row>

            <Divider />

            <Title level={4}>Monitoring & Logging</Title>
            <Row gutter={24}>
              <Col span={12}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
                  <div>
                    <Text strong>Enable Detailed Logging</Text>
                    <br />
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      Record API call and response details
                    </Text>
                  </div>
                  <Switch
                    checked={config.globalSettings.enableLogging}
                    onChange={(enabled) => setConfig(prev => prev ? {
                      ...prev,
                      globalSettings: {
                        ...prev.globalSettings,
                        enableLogging: enabled
                      }
                    } : prev)}
                  />
                </div>
              </Col>

              <Col span={12}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
                  <div>
                    <Text strong>Enable Performance Monitoring</Text>
                    <br />
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      Monitor response time and success rate
                    </Text>
                  </div>
                  <Switch
                    checked={config.globalSettings.enableMonitoring}
                    onChange={(enabled) => setConfig(prev => prev ? {
                      ...prev,
                      globalSettings: {
                        ...prev.globalSettings,
                        enableMonitoring: enabled
                      }
                    } : prev)}
                  />
                </div>
              </Col>
            </Row>
          </Card>
        </TabPane>

        <TabPane 
          tab={
            <span>
              <LineChartOutlined />
              Monitoring Statistics
            </span>
          } 
          key="monitoring"
        >
          <Spin spinning={monitoringLoading}>
            {/* 筛选器 */}
            <Card style={{ marginBottom: 16 }}>
              <Row gutter={16} align="middle">
                <Col span={8}>
                  <Space>
                    <Text strong>时间范围:</Text>
                    <Segmented
                      options={[
                        { label: '24小时', value: '1d' },
                        { label: '7天', value: '7d' },
                        { label: '30天', value: '30d' },
                      ]}
                      value={monitoringTimeRange}
                      onChange={(value) => setMonitoringTimeRange(value as string)}
                    />
                  </Space>
                </Col>
                <Col span={8}>
                  <Space>
                    <Text strong>提供商:</Text>
                    <Select
                      style={{ width: 200 }}
                      placeholder="All Providers"
                      allowClear
                      value={monitoringProvider}
                      onChange={setMonitoringProvider}
                    >
                      {config?.providers.map(p => (
                        <Option key={p.id} value={p.id}>
                          {p.icon} {p.displayName}
                        </Option>
                      ))}
                    </Select>
                  </Space>
                </Col>
                <Col span={8} style={{ textAlign: 'right' }}>
                  <Space>
                    <Button 
                      icon={<ReloadOutlined />}
                      onClick={loadMonitoringData}
                    >
                      刷新
                    </Button>
                    <Button 
                      icon={<DownloadOutlined />}
                      type="primary"
                    >
                      导出报告
                    </Button>
                  </Space>
                </Col>
              </Row>
            </Card>

            {/* 概览统计 */}
            {dashboardData && (
              <Row gutter={16} style={{ marginBottom: 24 }}>
                <Col span={6}>
                  <Card>
                    <Statistic
                      title="总请求数"
                      value={dashboardData.summary.total_requests}
                      suffix="次"
                      valueStyle={{ color: '#1890ff' }}
                    />
                  </Card>
                </Col>
                <Col span={6}>
                  <Card>
                    <Statistic
                      title="平均响应时间"
                      value={(dashboardData.summary.avg_response_time / 1000).toFixed(2)}
                      suffix="秒"
                      valueStyle={{ color: '#52c41a' }}
                    />
                  </Card>
                </Col>
                <Col span={6}>
                  <Card>
                    <Statistic
                      title="成功率"
                      value={dashboardData.summary.avg_success_rate.toFixed(1)}
                      suffix="%"
                      valueStyle={{ 
                        color: dashboardData.summary.avg_success_rate > 95 ? '#3f8600' : '#ff4d4f' 
                      }}
                    />
                  </Card>
                </Col>
                <Col span={6}>
                  <Card>
                    <Statistic
                      title="总成本"
                      value={dashboardData.summary.total_cost.toFixed(2)}
                      suffix={dashboardData.summary.currency}
                      valueStyle={{ color: '#fa8c16' }}
                    />
                  </Card>
                </Col>
              </Row>
            )}

            {/* 趋势图表 */}
            {timeSeriesData.length > 0 ? (
              <Row gutter={16} style={{ marginBottom: 24 }}>
                <Col span={12}>
                  <Card title="请求趋势" size="small">
                    <ResponsiveContainer width="100%" height={250}>
                      <AreaChart data={timeSeriesData}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis 
                          dataKey="timestamp" 
                          tickFormatter={(value) => new Date(value).toLocaleDateString()} 
                        />
                        <YAxis />
                        <RechartsTooltip />
                        <Area 
                          type="monotone" 
                          dataKey="requests" 
                          stroke="#1890ff" 
                          fill="#1890ff" 
                          fillOpacity={0.6}
                          name="请求数"
                        />
                      </AreaChart>
                    </ResponsiveContainer>
                  </Card>
                </Col>
                <Col span={12}>
                  <Card title="成功率趋势" size="small">
                    <ResponsiveContainer width="100%" height={250}>
                      <LineChart data={timeSeriesData}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis 
                          dataKey="timestamp" 
                          tickFormatter={(value) => new Date(value).toLocaleDateString()} 
                        />
                        <YAxis domain={[0, 100]} />
                        <RechartsTooltip />
                        <Line 
                          type="monotone" 
                          dataKey="success_rate" 
                          stroke="#52c41a" 
                          strokeWidth={2}
                          name="成功率 (%)"
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </Card>
                </Col>
                <Col span={12}>
                  <Card title="响应时间趋势" size="small">
                    <ResponsiveContainer width="100%" height={250}>
                      <LineChart data={timeSeriesData}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis 
                          dataKey="timestamp" 
                          tickFormatter={(value) => new Date(value).toLocaleDateString()} 
                        />
                        <YAxis />
                        <RechartsTooltip />
                        <Line 
                          type="monotone" 
                          dataKey="avg_response_time" 
                          stroke="#fa8c16" 
                          strokeWidth={2}
                          name="平均响应时间 (ms)"
                        />
                      </LineChart>
                    </ResponsiveContainer>
                  </Card>
                </Col>
                <Col span={12}>
                  <Card title="Token使用趋势" size="small">
                    <ResponsiveContainer width="100%" height={250}>
                      <BarChart data={timeSeriesData}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis 
                          dataKey="timestamp" 
                          tickFormatter={(value) => new Date(value).toLocaleDateString()} 
                        />
                        <YAxis />
                        <RechartsTooltip />
                        <Bar dataKey="total_tokens" fill="#722ed1" name="Token数量" />
                      </BarChart>
                    </ResponsiveContainer>
                  </Card>
                </Col>
              </Row>
            ) : (
              <Alert
                style={{ marginBottom: 24 }}
                message="暂无数据"
                description="当前时间范围内没有使用记录，请使用LLM服务后再查看统计数据"
                type="info"
                showIcon
              />
            )}

            {/* 按模板统计 */}
            {templateStatsData.length > 0 && (
              <Card 
                title="按模板统计" 
                size="small"
                style={{ marginBottom: 16 }}
              >
                <Table
                  dataSource={templateStatsData}
                  pagination={false}
                  size="small"
                  rowKey={(record) => `template-${record.template_id}`}
                  columns={[
                    {
                      title: '模板ID',
                      dataIndex: 'template_id',
                      key: 'template_id',
                      width: 100
                    },
                    {
                      title: '模板名称',
                      dataIndex: 'template_name',
                      key: 'template_name'
                    },
                    {
                      title: '请求数',
                      dataIndex: 'totalRequests',
                      key: 'totalRequests',
                      sorter: (a, b) => a.totalRequests - b.totalRequests,
                      render: (value) => value.toLocaleString()
                    },
                    {
                      title: '成功率',
                      dataIndex: 'successRate',
                      key: 'successRate',
                      sorter: (a, b) => a.successRate - b.successRate,
                      render: (value) => (
                        <Progress 
                          percent={value} 
                          size="small" 
                          status={value > 95 ? 'success' : value > 80 ? 'normal' : 'exception'}
                        />
                      )
                    },
                    {
                      title: '平均响应时间',
                      dataIndex: 'avgResponseTime',
                      key: 'avgResponseTime',
                      sorter: (a, b) => a.avgResponseTime - b.avgResponseTime,
                      render: (value) => `${value.toFixed(0)}ms`
                    },
                    {
                      title: 'Token总数',
                      dataIndex: 'totalTokens',
                      key: 'totalTokens',
                      sorter: (a, b) => a.totalTokens - b.totalTokens,
                      render: (value) => value.toLocaleString()
                    },
                    {
                      title: '总成本',
                      dataIndex: 'totalCost',
                      key: 'totalCost',
                      sorter: (a, b) => a.totalCost - b.totalCost,
                      render: (value) => `¥${value.toFixed(2)}`
                    }
                  ]}
                />
              </Card>
            )}

            {/* 按模型统计 */}
            {modelStatsData.length > 0 && (
              <Card 
                title="按模型统计" 
                size="small"
                style={{ marginBottom: 16 }}
              >
                <Table
                  dataSource={modelStatsData}
                  pagination={false}
                  size="small"
                  rowKey={(record) => `${record.provider}-${record.model}`}
                  columns={[
                    {
                      title: '提供商',
                      dataIndex: 'provider',
                      key: 'provider',
                      render: (text) => {
                        const provider = config?.providers.find(p => p.id === text);
                        return (
                          <Space>
                            <span>{provider?.icon || '🤖'}</span>
                            <span>{provider?.displayName || text}</span>
                          </Space>
                        );
                      }
                    },
                    {
                      title: '模型',
                      dataIndex: 'model',
                      key: 'model'
                    },
                    {
                      title: '请求数',
                      dataIndex: 'totalRequests',
                      key: 'totalRequests',
                      sorter: (a, b) => a.totalRequests - b.totalRequests,
                      render: (value) => value.toLocaleString()
                    },
                    {
                      title: '成功率',
                      dataIndex: 'successRate',
                      key: 'successRate',
                      sorter: (a, b) => a.successRate - b.successRate,
                      render: (value) => (
                        <Progress 
                          percent={value} 
                          size="small" 
                          status={value > 95 ? 'success' : value > 80 ? 'normal' : 'exception'}
                        />
                      )
                    },
                    {
                      title: '平均响应时间',
                      dataIndex: 'avgResponseTime',
                      key: 'avgResponseTime',
                      sorter: (a, b) => a.avgResponseTime - b.avgResponseTime,
                      render: (value) => `${value.toFixed(0)}ms`
                    },
                    {
                      title: 'Token总数',
                      dataIndex: 'totalTokens',
                      key: 'totalTokens',
                      sorter: (a, b) => a.totalTokens - b.totalTokens,
                      render: (value) => value.toLocaleString()
                    },
                    {
                      title: '总成本',
                      dataIndex: 'totalCost',
                      key: 'totalCost',
                      sorter: (a, b) => a.totalCost - b.totalCost,
                      render: (value) => `¥${value.toFixed(2)}`
                    }
                  ]}
                />
              </Card>
            )}

            {/* 错误分析 */}
            {errorAnalysisData.length > 0 && (
              <Card 
                title="错误分析" 
                size="small"
                style={{ marginBottom: 16 }}
              >
                <Row gutter={16}>
                  <Col span={12}>
                    <Table
                      dataSource={errorAnalysisData}
                      pagination={false}
                      size="small"
                      rowKey="error_type"
                      columns={[
                        {
                          title: '错误类型',
                          dataIndex: 'error_type',
                          key: 'error_type',
                          render: (text) => <Tag color="error">{text}</Tag>
                        },
                        {
                          title: '次数',
                          dataIndex: 'count',
                          key: 'count',
                          sorter: (a, b) => a.count - b.count
                        },
                        {
                          title: '占比',
                          dataIndex: 'percentage',
                          key: 'percentage',
                          render: (value) => `${value.toFixed(1)}%`
                        },
                        {
                          title: '最后发生',
                          dataIndex: 'last_occurrence',
                          key: 'last_occurrence',
                          render: (value) => new Date(value).toLocaleString()
                        }
                      ]}
                    />
                  </Col>
                  <Col span={12}>
                    <ResponsiveContainer width="100%" height={250}>
                      <PieChart>
                        <Pie
                          data={errorAnalysisData}
                          dataKey="count"
                          nameKey="error_type"
                          cx="50%"
                          cy="50%"
                          outerRadius={80}
                          label={(entry: any) => `${entry.error_type}: ${entry.percentage.toFixed(1)}%`}
                        >
                          {errorAnalysisData.map((entry, index) => (
                            <Cell key={`cell-${index}`} fill={['#ff4d4f', '#ff7a45', '#ffa940'][index % 3]} />
                          ))}
                        </Pie>
                        <RechartsTooltip />
                      </PieChart>
                    </ResponsiveContainer>
                  </Col>
                </Row>
              </Card>
            )}

            {/* 详细日志 */}
            {detailedLogs.length > 0 && (
              <Card 
                title="详细日志" 
                size="small"
                extra={
                  <Text type="secondary">共 {logsTotal} 条记录</Text>
                }
              >
                <Table
                  dataSource={detailedLogs}
                  pagination={{
                    current: logsPage,
                    pageSize: logsPageSize,
                    total: logsTotal,
                    onChange: (page) => setLogsPage(page),
                    showSizeChanger: false
                  }}
                  size="small"
                  rowKey="id"
                  scroll={{ x: 1200 }}
                  columns={[
                    {
                      title: '时间',
                      dataIndex: 'created_at',
                      key: 'created_at',
                      width: 180,
                      render: (value) => new Date(value).toLocaleString()
                    },
                    {
                      title: '模板ID',
                      dataIndex: 'template_id',
                      key: 'template_id',
                      width: 100,
                      render: (value) => value || '-'
                    },
                    {
                      title: '提供商',
                      dataIndex: 'provider',
                      key: 'provider',
                      width: 120,
                      render: (text) => {
                        const provider = config?.providers.find(p => p.id === text);
                        return `${provider?.icon || '🤖'} ${provider?.displayName || text}`;
                      }
                    },
                    {
                      title: '模型',
                      dataIndex: 'model',
                      key: 'model',
                      width: 150
                    },
                    {
                      title: '状态',
                      dataIndex: 'success',
                      key: 'success',
                      width: 100,
                      render: (value) => (
                        <Tag color={value ? 'success' : 'error'}>
                          {value ? '成功' : '失败'}
                        </Tag>
                      )
                    },
                    {
                      title: '响应时间',
                      dataIndex: 'response_time_ms',
                      key: 'response_time_ms',
                      width: 100,
                      render: (value) => `${value}ms`
                    },
                    {
                      title: 'Tokens',
                      dataIndex: 'tokens_used',
                      key: 'tokens_used',
                      width: 100
                    },
                    {
                      title: '成本',
                      dataIndex: 'cost',
                      key: 'cost',
                      width: 100,
                      render: (value) => `¥${value?.toFixed(4) || 0}`
                    },
                    {
                      title: '用户ID',
                      dataIndex: 'user_id',
                      key: 'user_id',
                      width: 100,
                      render: (text) => text || '-'
                    },
                    {
                      title: '错误信息',
                      dataIndex: 'error_message',
                      key: 'error_message',
                      render: (text) => text ? (
                        <Tooltip title={text}>
                          <Text type="danger" ellipsis style={{ maxWidth: 200 }}>
                            {text}
                          </Text>
                        </Tooltip>
                      ) : '-'
                    }
                  ]}
                />
              </Card>
            )}
          </Spin>
        </TabPane>
      </Tabs>
    </div>
  );
};

export default LLMConfigPage;
