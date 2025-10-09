import React, { useState, useEffect } from 'react';
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
  Spin
} from 'antd';
import { 
  SettingOutlined, 
  RobotOutlined,
  ExclamationCircleOutlined,
  ExperimentOutlined,
  SaveOutlined,
  ReloadOutlined,
  EyeInvisibleOutlined,
  EyeTwoTone,
  WarningOutlined
} from '@ant-design/icons';
import { llmConfigService, LLMConfig, LLMUsageStats } from '../services/llmConfigService';
import { api } from '../services/api';

const { Title, Paragraph, Text } = Typography;
const { TabPane } = Tabs;
const { Option } = Select;

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
    } catch (error: any) {
      console.error('加载配置失败:', error);
      setLoadError(error?.response?.data?.detail || error.message || '加载配置失败');
      message.error('加载配置失败，请检查网络连接');
    } finally {
      setLoading(false);
    }
  };

  const loadUsageStats = async () => {
    try {
      const stats = await llmConfigService.getUsageStats('30d');
      setUsageStats(stats);
    } catch (error) {
      console.error('加载使用统计失败:', error);
      // 不显示错误消息，统计数据是次要的
    }
  };

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
    
    // 标记有未保存的更改（除了status字段）
    if (!updates.status) {
      setHasUnsavedChanges(true);
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
        message.error('API密钥不能为空');
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
          `${provider.displayName} 连接测试成功！请点击右上角"保存配置"按钮保存到数据库`,
          5
        );
      } else {
        updateProvider(providerId, { status: 'error' });
        message.error(
          `${provider.displayName} 连接测试失败: ${result.error || '未知错误'}`,
          5
        );
      }
    } catch (error: any) {
      updateProvider(providerId, { status: 'error' });
      message.error(
        `连接测试失败: ${error?.response?.data?.detail || error.message || '网络错误'}`,
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
      await llmConfigService.saveConfig(config);
      setSaveStatus('saved');
      message.success(
        '配置保存成功，部分更改可能需要重启服务才能生效',
        4
      );
      
      // 2秒后重置保存状态
      setTimeout(() => setSaveStatus('idle'), 2000);
      
      // 重新加载配置以确保数据同步
      await loadConfig();
      
      // 清除未保存标记
      setHasUnsavedChanges(false);
    } catch (error: any) {
      setSaveStatus('error');
      message.error(
        `配置保存失败: ${error?.response?.data?.detail || error.message || '请重试'}`,
        5
      );
      setTimeout(() => setSaveStatus('idle'), 2000);
    }
  };

  const testGlobalSettings = async () => {
    setTestingGlobal(true);
    try {
      console.log('开始测试全局设置...');
      const result = await api.post('/llm/test-global');
      
      // 显示测试结果
      const resultMessages = result.test_results.map((test: any) => {
        const icon = test.status === 'success' ? '✅' : 
                    test.status === 'warning' ? '⚠️' : 
                    test.status === 'error' ? '❌' : 'ℹ️';
        return `${icon} ${test.test}: ${test.message}`;
      }).join('\n');

      if (result.overall_status === 'success') {
        message.success(
          `全局设置测试通过！\n\n${resultMessages}`,
          8
        );
      } else if (result.overall_status === 'warning') {
        message.warning(
          `全局设置测试完成，但有警告：\n\n${resultMessages}`,
          10
        );
      } else {
        message.error(
          `全局设置测试发现问题：\n\n${resultMessages}`,
          10
        );
      }
    } catch (error: any) {
      console.error('全局设置测试失败:', error);
      message.error(`测试失败: ${error?.response?.data?.detail || error.message || '网络错误'}`);
    } finally {
      setTestingGlobal(false);
    }
  };

  const getStatusText = (status: string) => {
    switch (status) {
      case 'connected':
        return '已连接';
      case 'testing':
        return '测试中...';
      case 'error':
        return '连接失败';
      default:
        return '未连接';
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
            <Text strong>API密钥</Text>
            <Input.Password
              placeholder="请输入API密钥"
              value={provider.apiKey}
              onChange={(e) => updateProvider(provider.id, { apiKey: e.target.value })}
              style={{ marginTop: 4, fontFamily: 'monospace' }}
              iconRender={(visible) => (visible ? <EyeTwoTone /> : <EyeInvisibleOutlined />)}
            />
          </div>
        </Col>
        <Col span={12}>
          <div style={{ marginBottom: 16 }}>
            <Text strong>API端点</Text>
            <Input
              placeholder="API基础URL"
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
            <Text strong>默认模型</Text>
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
            <Text strong>温度值 ({provider.temperature})</Text>
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
            <Text strong>最大Token</Text>
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
          {testingProvider === provider.id ? '测试中...' : '测试连接'}
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

  const avgSuccessRate = totalStats.count > 0 ? totalStats.avgSuccessRate / totalStats.count : 0;
  const avgResponseTime = totalStats.count > 0 ? totalStats.avgResponseTime / totalStats.count : 0;

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
        <Text type="secondary">正在加载配置...</Text>
      </div>
    );
  }

  // 加载错误状态
  if (loadError || !config) {
    return (
      <div style={{ padding: '24px' }}>
        <Alert
          message="加载配置失败"
          description={loadError || '无法获取配置数据'}
          type="error"
          showIcon
          icon={<WarningOutlined />}
          action={
            <Button size="small" onClick={loadConfig}>
              重新加载
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
              <Title level={2} style={{ margin: 0 }}>大语言模型配置</Title>
              <Paragraph style={{ margin: 0, color: '#666' }}>
                配置和管理中国主流大语言模型接入
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
              刷新
            </Button>
            <Button
              type="primary"
              icon={saveStatus === 'saving' ? <ReloadOutlined spin /> : <SaveOutlined />}
              onClick={saveConfig}
              loading={saveStatus === 'saving'}
              size="large"
            >
              {saveStatus === 'saving' ? '保存中...' : '保存配置'}
            </Button>
          </Space>
        </div>
      </div>

      {/* Unsaved Changes Alert */}
      {hasUnsavedChanges && (
        <Alert
          message="您有未保存的配置修改"
          description='请点击右上角的"保存配置"按钮将修改保存到数据库'
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
          message="配置已成功保存"
          description="配置已保存到数据库，部分更改可能需要重启服务才能生效"
          type="success"
          showIcon
          style={{ marginBottom: 16 }}
          closable
          onClose={() => setSaveStatus('idle')}
        />
      )}

      {saveStatus === 'error' && (
        <Alert
          message="保存配置失败"
          description="请检查网络连接后重试"
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
              模型提供商
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
              全局设置
            </span>
          } 
          key="global"
        >
          <Card title={
            <span>
              <SettingOutlined style={{ marginRight: 8 }} />
              全局配置
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
              测试全局设置
            </Button>
          }>
            <Row gutter={24}>
              <Col span={12}>
                <div style={{ marginBottom: 24 }}>
                  <Text strong>默认模型提供商</Text>
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
                      <Text strong>启用故障转移</Text>
                      <br />
                      <Text type="secondary" style={{ fontSize: '12px' }}>
                        当主要模型不可用时自动切换
                      </Text>
                    </div>
                    <Switch
                      checked={config.fallbackEnabled}
                      onChange={(enabled) => setConfig(prev => prev ? { ...prev, fallbackEnabled: enabled } : prev)}
                    />
                  </div>
                </div>
              </Col>

              <Col span={12}>
                <div style={{ marginBottom: 24 }}>
                  <Text strong>请求超时时间 (秒)</Text>
                  <InputNumber
                    min={5}
                    max={300}
                    value={config.globalSettings.timeout}
                    onChange={(value) => setConfig(prev => prev ? {
                      ...prev,
                      globalSettings: {
                        ...prev.globalSettings,
                        timeout: value || 30
                      }
                    } : prev)}
                    style={{ width: '100%', marginTop: 8 }}
                  />
                </div>

                <div style={{ marginBottom: 24 }}>
                  <Text strong>重试次数</Text>
                  <InputNumber
                    min={0}
                    max={10}
                    value={config.globalSettings.retryCount}
                    onChange={(value) => setConfig(prev => prev ? {
                      ...prev,
                      globalSettings: {
                        ...prev.globalSettings,
                        retryCount: value || 3
                      }
                    } : prev)}
                    style={{ width: '100%', marginTop: 8 }}
                  />
                </div>
              </Col>
            </Row>

            <Divider />

            <Title level={4}>监控与日志</Title>
            <Row gutter={24}>
              <Col span={12}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
                  <div>
                    <Text strong>启用详细日志</Text>
                    <br />
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      记录API调用和响应详情
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
                    <Text strong>启用性能监控</Text>
                    <br />
                    <Text type="secondary" style={{ fontSize: '12px' }}>
                      监控响应时间和成功率
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
              <ExclamationCircleOutlined />
              监控统计
            </span>
          } 
          key="monitoring"
        >
          <Row gutter={16} style={{ marginBottom: 24 }}>
            <Col span={8}>
              <Card>
                <Statistic
                  title="总请求数"
                  value={totalStats.totalRequests}
                  suffix="次"
                />
                <Text type="secondary">过去30天</Text>
              </Card>
            </Col>
            <Col span={8}>
              <Card>
                <Statistic
                  title="平均响应时间"
                  value={avgResponseTime}
                  suffix="秒"
                  precision={2}
                />
                <Text type="secondary">过去30天</Text>
              </Card>
            </Col>
            <Col span={8}>
              <Card>
                <Statistic
                  title="平均成功率"
                  value={avgSuccessRate}
                  suffix="%"
                  precision={1}
                  valueStyle={{ color: avgSuccessRate > 95 ? '#3f8600' : '#ff4d4f' }}
                />
                <Text type="secondary">过去30天</Text>
              </Card>
            </Col>
          </Row>

          <Card 
            title="模型使用统计" 
            extra={
              <Button 
                size="small" 
                icon={<ReloadOutlined />}
                onClick={loadUsageStats}
              >
                刷新统计
              </Button>
            }
          >
            {usageStats.length === 0 ? (
              <Alert
                message="暂无使用统计数据"
                description="开始使用LLM服务后，这里将显示详细的使用统计信息"
                type="info"
                showIcon
              />
            ) : (
              usageStats.map((stat) => {
                const provider = config.providers.find(p => p.id === stat.provider);
                return (
                  <Card 
                    key={stat.provider}
                    size="small" 
                    style={{ marginBottom: 16 }}
                    title={
                      <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                        <span>{provider?.icon || '🤖'}</span>
                        <span>{provider?.displayName || stat.provider}</span>
                      </div>
                    }
                  >
                    <Row gutter={16}>
                      <Col span={6}>
                        <Statistic
                          title="请求数"
                          value={stat.totalRequests}
                        />
                      </Col>
                      <Col span={6}>
                        <Statistic
                          title="响应时间"
                          value={stat.avgResponseTime}
                          suffix="秒"
                          precision={2}
                        />
                      </Col>
                      <Col span={6}>
                        <Statistic
                          title="成功率"
                          value={stat.successRate}
                          suffix="%"
                          precision={1}
                          valueStyle={{ color: stat.successRate > 95 ? '#3f8600' : '#ff4d4f' }}
                        />
                      </Col>
                      <Col span={6}>
                        <Statistic
                          title="总成本"
                          value={stat.totalCost}
                          suffix={stat.currency}
                          precision={2}
                        />
                      </Col>
                    </Row>
                  </Card>
                );
              })
            )}
          </Card>
        </TabPane>
      </Tabs>
    </div>
  );
};

export default LLMConfigPage;
