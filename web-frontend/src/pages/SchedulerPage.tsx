import React, { useState, useEffect, useCallback } from 'react';
import {
  Card,
  Row,
  Col,
  Button,
  Space,
  Tag,
  Typography,
  Switch,
  Statistic,
  Alert,
  message,
  Modal,
  Form,
  Input,
  Select,
  Tooltip,
  Tabs,
  DatePicker,
  InputNumber,

  Table,
  Badge,
  Popconfirm
} from 'antd';
import {
  ReloadOutlined,
  SettingOutlined,
  SyncOutlined,
  CheckCircleOutlined,
  ExclamationCircleOutlined,
  ClockCircleOutlined,
  DatabaseOutlined,
  GlobalOutlined,



  InfoCircleOutlined,
  CloudDownloadOutlined,
  TagOutlined,
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  BulbOutlined,
  ScheduleOutlined,
  FilterOutlined,
  BarChartOutlined
} from '@ant-design/icons';
import dayjs from 'dayjs';
import { api } from '../services/api';
import { newsAPI } from '../services/news';
import { schedulerAPI, ScheduledTask } from '../services/scheduler';

import DatabaseTableViewer from '../components/DatabaseTableViewer';
import TaskExecutionHistory from '../components/TaskExecutionHistory';

import DataSourceConfigModal from '../components/DataSourceConfigModal';

const { Title, Text } = Typography;
const { RangePicker } = DatePicker;
const { Option } = Select;

// 数据源配置
interface DataSource {
  id: string;
  name: string;
  type: 'eodhd' | 'alpha_vantage' | 'finviz' | 'finviz_screener' | 'eodhd_events' | 'eodhd_macro' | 'eodhd_exchange' | 'eodhd_fundamentals_low_freq' | 'other';
  description: string;
  enabled: boolean;
  status: 'idle' | 'running' | 'success' | 'error';
  lastRun?: string;
  lastSuccess?: string;
  quickActions: QuickAction[];
}

interface QuickAction {
  id: string;
  name: string;
  description: string;
  endpoint: string;
  params?: Record<string, any>;
  icon: React.ReactNode;
}

// 新闻主题接口
interface NewsTheme {
  id: number;
  topic_name: string;
  description?: string;
  color?: string;
  is_active: boolean;
  keywords: string[];
}



const SchedulerPage: React.FC = () => {
  const [dataSources, setDataSources] = useState<DataSource[]>([
    {
      id: 'eodhd_news',
      name: 'EODHD News Data',
      type: 'eodhd',
      description: 'Get latest stock news from EODHD API',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_cron_schedule',
          name: 'Configure Data Fetch Strategy',
          description: 'Set up Cron-based periodic data fetching with NY timezone every 15 minutes',
          endpoint: '/scheduler/tasks/eodhd-news',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'eodhd_events',
      name: 'EODHD Economic Events',
      type: 'eodhd_events',
      description: 'Get economic calendar event data from EODHD API',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_events_schedule',
          name: 'Configure Data Fetch Strategy',
          description: 'Set up Cron-based periodic economic events fetching',
          endpoint: '/scheduler/tasks/economic-events',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'eodhd_macro',
      name: 'EODHD Macro Indicators',
      type: 'eodhd_macro',
      description: 'Get macroeconomic indicator data from EODHD API',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_macro_schedule',
          name: 'Configure Data Fetch Strategy',
          description: 'Set up Cron-based periodic macro indicators fetching',
          endpoint: '/scheduler/tasks/macro-indicators',
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'eodhd_exchange',
      name: 'EODHD Exchange Data',
      type: 'eodhd_exchange',
      description: 'Get exchange ticker data from EODHD API for fundamentals analysis',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_exchange_schedule',
          name: 'Configure Exchange Data Fetch',
          description: 'Set up periodic exchange ticker data fetching from multiple exchanges',
          endpoint: '/scheduler/tasks/exchange-data',
          params: {},
          icon: <DatabaseOutlined />
        }
      ]
    },
    {
      id: 'eodhd_fundamentals_low_freq',
      name: 'EODHD Fundamentals Low Frequency',
      type: 'eodhd_fundamentals_low_freq',
      description: 'Get company fundamentals data from EODHD API with low frequency scheduling (General, Holders, outstandingShares, Earnings)',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_fundamentals_low_freq_schedule',
          name: 'Configure Data Fetch Strategy',
          description: 'Set up Cron-based periodic fundamentals data fetching for NYSE and NASDAQ companies',
          endpoint: '/scheduler/tasks/eodhd-fundamentals-low-freq',
          params: {},
          icon: <ScheduleOutlined />
        }
      ]
    },
    {
      id: 'alpha_vantage',
      name: 'Alpha Vantage',
      type: 'alpha_vantage',
      description: 'Financial data API - stock prices, technical indicators, etc. (Coming soon)',
      enabled: false,
      status: 'idle',
      quickActions: [
        {
          id: 'fetch_stock_data',
          name: 'Configure Data Fetch Strategy',
          description: 'Get real-time stock prices and historical data',
          endpoint: '/alpha-vantage/stock-data',
          icon: <GlobalOutlined />
        }
      ]
    },
    {
      id: 'finviz_screener',
      name: 'Finviz Screener',
      type: 'finviz_screener',
      description: 'Comprehensive stock screener data from Finviz with technical and fundamental metrics',
      enabled: true,
      status: 'idle',
      quickActions: [
        {
          id: 'configure_finviz_screener_schedule',
          name: 'Configure Screener Data Fetch',
          description: 'Set up periodic stock screener data fetching from Finviz',
          endpoint: '/scheduler/tasks/finviz-screener',
          params: {},
          icon: <FilterOutlined />
        }
      ]
    }
  ]);

  const [loading, setLoading] = useState(false);
  const [quickActionModal, setQuickActionModal] = useState<{
    visible: boolean;
    action: QuickAction | null;
    dataSource: DataSource | null;
  }>({
    visible: false,
    action: null,
    dataSource: null
  });
  
  // 新闻相关状态
  const [themes, setThemes] = useState<NewsTheme[]>([]);

  const [themeModalVisible, setThemeModalVisible] = useState(false);
  const [selectedTheme, setSelectedTheme] = useState<NewsTheme | null>(null);
  const [themeForm] = Form.useForm();
  
  // 定时任务相关状态
  const [scheduledTasks, setScheduledTasks] = useState<ScheduledTask[]>([]);

  const [dataSourceConfigVisible, setDataSourceConfigVisible] = useState(false);
  const [selectedDataSource, setSelectedDataSource] = useState<DataSource | null>(null);

  
  // 数据库表查看功能已移至DatabaseTableViewer组件
  
  const [form] = Form.useForm();

  // 页面加载时获取定时任务数据
  useEffect(() => {
    loadScheduledTasks();
  }, []);

  // 同步数据源状态与定时任务状态
  const syncDataSourceStatus = useCallback(() => {
    setDataSources(prev => prev.map(source => {
      // 找到对应的定时任务
      const relatedTasks = scheduledTasks.filter(task => {
        const taskType = task.type.toLowerCase();
        const sourceIdLower = source.id.toLowerCase();
        
        return (
          (sourceIdLower.includes('news') && taskType.includes('news')) ||
          (sourceIdLower.includes('events') && taskType.includes('events')) ||
          (sourceIdLower.includes('macro') && (taskType.includes('macro') || taskType.includes('indicators'))) ||
          (sourceIdLower.includes('exchange') && taskType.includes('exchange'))
        );
      });
      
      // 如果有相关任务，使用任务的enabled状态
      if (relatedTasks.length > 0) {
        // 只要有一个任务启用，就认为数据源启用
        const hasEnabledTask = relatedTasks.some(task => task.enabled);
        return { ...source, enabled: hasEnabledTask };
      }
      
      // 没有相关任务，保持原状态（Coming soon的数据源）
      return source;
    }));
  }, [scheduledTasks]);

  // 当定时任务列表更新时，同步数据源状态
  useEffect(() => {
    if (scheduledTasks.length > 0) {
      syncDataSourceStatus();
    }
  }, [scheduledTasks, syncDataSourceStatus]);

  // 获取状态标签
  const getStatusTag = (status: string) => {
    const statusConfig = {
      running: { color: 'processing', icon: <SyncOutlined spin />, text: 'Running' },
      success: { color: 'success', icon: <CheckCircleOutlined />, text: 'Success' },
      error: { color: 'error', icon: <ExclamationCircleOutlined />, text: 'Error' },
      idle: { color: 'default', icon: <ClockCircleOutlined />, text: 'Idle' }
    };
    
    const config = statusConfig[status as keyof typeof statusConfig];
    return (
      <Tag color={config.color} icon={config.icon}>
        {config.text}
      </Tag>
    );
  };



  // 加载主题数据
  const loadThemes = async () => {
    try {
      const response = await newsAPI.getThemes();
      setThemes(response.data || []);
    } catch (error) {
      console.error('Failed to get themes:', error);
      message.error('Failed to get themes');
    }
  };

  // 加载定时任务
  const loadScheduledTasks = async () => {
    try {
      setLoading(true);
      const response = await schedulerAPI.getTasks();
      if (response.success && response.data) {
        setScheduledTasks(response.data);
      } else {
        message.error(response.message);
      }
    } catch (error) {
      console.error('Failed to load scheduled tasks:', error);
      message.error('Failed to load scheduled tasks');
    } finally {
      setLoading(false);
    }
  };

  // 切换数据源启用状态 - 通过启用/禁用对应的定时任务
  const toggleDataSource = async (sourceId: string, enabled: boolean) => {
    try {
      setLoading(true);
      
      // 根据数据源ID找到对应的定时任务
      const relatedTasks = scheduledTasks.filter(task => {
        const taskType = task.type.toLowerCase();
        const sourceIdLower = sourceId.toLowerCase();
        
        // 映射关系：数据源ID -> 任务类型
        return (
          (sourceIdLower.includes('news') && taskType.includes('news')) ||
          (sourceIdLower.includes('events') && taskType.includes('events')) ||
          (sourceIdLower.includes('macro') && (taskType.includes('macro') || taskType.includes('indicators'))) ||
          (sourceIdLower.includes('exchange') && !sourceIdLower.includes('fundamentals') && taskType.includes('exchange')) ||
          (sourceIdLower.includes('fundamentals_low_freq') && taskType.includes('fundamentals_low_freq')) ||
          (sourceIdLower.includes('finviz_screener') && taskType.includes('finviz_screener'))
        );
      });
      
      if (relatedTasks.length === 0) {
        message.warning(`没有找到 ${sourceId} 对应的定时任务，请先配置数据获取策略`);
        return;
      }
      
      // 批量启用/禁用相关任务
      let successCount = 0;
      let errorCount = 0;
      
      for (const task of relatedTasks) {
        try {
          const result = enabled 
            ? await schedulerAPI.enableTask(task.id)
            : await schedulerAPI.disableTask(task.id);
          
          if (result.success) {
            successCount++;
          } else {
            errorCount++;
            console.error(`任务 ${task.id} 操作失败:`, result.message);
          }
        } catch (error) {
          errorCount++;
          console.error(`任务 ${task.id} 操作异常:`, error);
        }
      }
      
      if (successCount > 0) {
        // 更新前端状态
        setDataSources(prev => prev.map(source => 
          source.id === sourceId ? { ...source, enabled } : source
        ));
        
        // 重新加载任务列表以获取最新状态
        await loadScheduledTasks();
        
        const message_text = `${enabled ? '启用' : '禁用'}成功：${successCount} 个任务`;
        message.success(errorCount > 0 ? `${message_text}，${errorCount} 个任务失败` : message_text);
      } else {
        message.error(`操作失败：所有 ${relatedTasks.length} 个任务都无法${enabled ? '启用' : '禁用'}`);
      }
      
    } catch (error) {
      console.error('切换数据源状态失败:', error);
      message.error('操作失败');
    } finally {
      setLoading(false);
    }
  };

  // 执行快速操作
  const executeQuickAction = async (action: QuickAction, dataSource: DataSource, customParams?: any) => {
    try {
      setLoading(true);
      
      // 更新数据源状态为运行中
      setDataSources(prev => prev.map(source => 
        source.id === dataSource.id ? { ...source, status: 'running' } : source
      ));

      const params = { ...action.params, ...customParams };
      let response;

      response = await api.post(action.endpoint, params);

      // 更新数据源状态
      setDataSources(prev => prev.map(source => 
        source.id === dataSource.id ? { 
          ...source, 
          status: 'success',
          lastRun: dayjs().format('YYYY-MM-DD HH:mm:ss'),
          lastSuccess: dayjs().format('YYYY-MM-DD HH:mm:ss')
        } : source
      ));

      message.success(response.message || 'Operation successful');
      
    } catch (error: any) {
      console.error('Quick action error:', error);
      

      
      // 更新数据源状态为错误
      setDataSources(prev => prev.map(source => 
        source.id === dataSource.id ? { 
          ...source, 
          status: 'error',
          lastRun: dayjs().format('YYYY-MM-DD HH:mm:ss')
        } : source
      ));

      const errorMessage = error.response?.data?.detail || (error instanceof Error ? error.message : 'Operation failed');
      message.error(errorMessage);
    } finally {
      setLoading(false);
    }
  };

  // 显示快速操作对话框
  const showQuickActionModal = (action: QuickAction, dataSource: DataSource) => {
    // 如果是Cron调度配置，根据数据源类型决定使用哪个配置组件
    if (action.id.includes('configure') && action.id.includes('schedule')) {
      // 对于EODHD相关数据源，使用专门的配置模态框
      if (dataSource.id === 'eodhd_news' || dataSource.id === 'eodhd_events' || dataSource.id === 'eodhd_macro' || dataSource.id === 'eodhd_exchange' || dataSource.id === 'eodhd_fundamentals_low_freq' || dataSource.id === 'finviz_screener') {
        setSelectedDataSource(dataSource);
        setDataSourceConfigVisible(true);
      } else {
        // 其他数据源暂不支持配置
        message.info('该数据源配置功能即将推出');
      }
      return;
    }

    // 如果是简单操作，直接执行
    if (!action.params || Object.keys(action.params).length === 0) {
      executeQuickAction(action, dataSource);
      return;
    }

    // 显示参数配置对话框
    setQuickActionModal({
      visible: true,
      action,
      dataSource
    });
    
    // 如果是新闻获取，设置默认值并加载主题
    if (action.id === 'fetch_latest_news') {
      form.setFieldsValue({
        ...action.params,
        dateRange: [dayjs().subtract(7, 'day'), dayjs()]
      });
      loadThemes();
    } else {
      form.setFieldsValue(action.params || {});
    }
  };

  // 确认执行快速操作
  const confirmQuickAction = async () => {
    try {
      const values = await form.validateFields();
      await executeQuickAction(quickActionModal.action!, quickActionModal.dataSource!, values);
      setQuickActionModal({ visible: false, action: null, dataSource: null });
      form.resetFields();
    } catch (error) {
      console.error('Form validation error:', error);
    }
  };

  // 主题管理函数
  const createTheme = async (values: any) => {
    try {
      const response = await newsAPI.createTheme(values);
      if (response.success) {
        message.success('Theme created successfully');
        loadThemes();
        setThemeModalVisible(false);
        themeForm.resetFields();
      } else {
        message.error(response.message || 'Creation failed');
      }
    } catch (error) {
      console.error('Failed to create theme:', error);
      message.error('Failed to create theme');
    }
  };

  const updateTheme = async (themeId: number, values: any) => {
    try {
      const response = await newsAPI.updateTheme(themeId, values);
      if (response.success) {
        message.success('Theme updated successfully');
        loadThemes();
        setThemeModalVisible(false);
        setSelectedTheme(null);
        themeForm.resetFields();
      } else {
        message.error(response.message || 'Update failed');
      }
    } catch (error) {
      console.error('Failed to update theme:', error);
      message.error('Failed to update theme');
    }
  };

  const deleteTheme = async (themeId: number) => {
    try {
      const response = await newsAPI.deleteTheme(themeId);
      if (response.success) {
        message.success('Theme deleted successfully');
        loadThemes();
      } else {
        message.error(response.message || 'Deletion failed');
      }
    } catch (error) {
      console.error('Failed to delete theme:', error);
      message.error('Failed to delete theme');
    }
  };

  const initializeDefaultThemes = async () => {
    try {
      const response = await newsAPI.initializeDefaultThemes();
      if (response.success) {
        message.success(`Initialization successful, created ${response.data.created_count} default themes`);
        loadThemes();
      } else {
        message.error(response.message || 'Initialization failed');
      }
    } catch (error) {
      console.error('Failed to initialize themes:', error);
      message.error('Failed to initialize themes');
    }
  };

  // 定时任务管理函数






  const handleDataSourceConfigSuccess = (taskId: string) => {
    message.success(`数据获取策略配置成功！任务ID: ${taskId}`);
    setDataSourceConfigVisible(false);
    setSelectedDataSource(null);
    loadScheduledTasks(); // 重新加载任务列表
  };





  // 数据库表查看函数已移至DatabaseTableViewer组件

  // 刷新所有数据源状态
  const refreshAllSources = async () => {
    setLoading(true);
    try {
      // 加载定时任务列表
      await loadScheduledTasks();
      message.success('Status refreshed');
    } catch (error) {
      message.error('Refresh failed');
    } finally {
      setLoading(false);
    }
  };

  // 统计数据
  const enabledSources = dataSources.filter(s => s.enabled).length;
  const runningSources = dataSources.filter(s => s.status === 'running').length;
  const enabledTasks = scheduledTasks.filter(t => t.enabled).length;


  return (
    <div style={{ padding: '24px' }}>
      {/* 页面标题 */}
      <div style={{ marginBottom: '24px', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <div>
          <Title level={2} style={{ margin: 0 }}>
            <DatabaseOutlined style={{ marginRight: '8px' }} />
            Data Source Management
          </Title>
          <Text type="secondary" style={{ margin: '8px 0 0 0', display: 'block' }}>
            Simplified data fetching scheduler - Manage EODHD, Alpha Vantage, Finviz and other data sources
          </Text>
        </div>
        <Space>
          <Button 
            icon={<ReloadOutlined />} 
            onClick={refreshAllSources}
            loading={loading}
          >
            Refresh Status
          </Button>
        </Space>
      </div>

      {/* 统计卡片 */}
      <Row gutter={[16, 16]} style={{ marginBottom: '24px' }}>
        <Col xs={24} sm={8} md={6}>
          <Card>
            <Statistic
              title="Total Data Sources"
              value={dataSources.length}
              prefix={<DatabaseOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={8} md={6}>
          <Card>
            <Statistic
              title="Enabled"
              value={enabledSources}
              prefix={<CheckCircleOutlined />}
              valueStyle={{ color: '#52c41a' }}
              suffix={`/ ${dataSources.length}`}
            />
          </Card>
        </Col>

        <Col xs={24} sm={8} md={6}>
          <Card>
            <Statistic
              title="Running"
              value={runningSources}
              prefix={<SyncOutlined spin={runningSources > 0} />}
              valueStyle={{ color: '#faad14' }}
            />
          </Card>
        </Col>
        <Col xs={24} sm={8} md={6}>
          <Card>
            <Statistic
              title="Scheduled Tasks"
              value={scheduledTasks.length}
              prefix={<ScheduleOutlined />}
              valueStyle={{ color: '#722ed1' }}
              suffix={`(${enabledTasks} enabled)`}
            />
          </Card>
        </Col>
      </Row>

      {/* 系统状态提示 */}
      <Alert
        message="System Status: Data Source Management"
        description="EODHD news, economic events and macro indicators data sources are configured, Alpha Vantage and Finviz data sources coming soon"
        type="success"
        showIcon
        style={{ marginBottom: '24px' }}
      />

      {/* 数据源卡片 */}
      <Row gutter={[16, 16]}>
        {dataSources.map(source => (
          <Col xs={24} lg={12} xl={8} key={source.id}>
            <Card
              title={
                <Space>
                  <span>{source.name}</span>
                  {getStatusTag(source.status)}
                </Space>
              }
              extra={
                <Tooltip title={
                  source.description.includes('Coming soon') 
                    ? '此数据源即将推出' 
                    : !source.enabled && scheduledTasks.filter(task => {
                        const taskType = task.type.toLowerCase();
                        const sourceIdLower = source.id.toLowerCase();
                        return (
                          (sourceIdLower.includes('news') && taskType.includes('news')) ||
                          (sourceIdLower.includes('events') && taskType.includes('events')) ||
                          (sourceIdLower.includes('macro') && (taskType.includes('macro') || taskType.includes('indicators')))
                        );
                      }).length === 0
                    ? '请先配置数据获取策略'
                    : undefined
                }>
                  <Switch
                    checked={source.enabled}
                    onChange={(checked) => toggleDataSource(source.id, checked)}
                    disabled={source.description.includes('Coming soon') || loading}
                    loading={loading && source.status === 'running'}
                  />
                </Tooltip>
              }
              style={{ 
                height: '100%',
                opacity: source.enabled ? 1 : 0.6
              }}
            >
              <div style={{ marginBottom: '16px' }}>
                <Text type="secondary">{source.description}</Text>
              </div>

              {source.lastRun && (
                <div style={{ marginBottom: '16px' }}>
                  <Text type="secondary" style={{ fontSize: '12px' }}>
                    Last Run: {source.lastRun}
                  </Text>
                </div>
              )}

              <div style={{ marginBottom: '16px' }}>
                <Text strong>Configuration:</Text>
              </div>

              <Space direction="vertical" style={{ width: '100%' }}>
                {source.quickActions.map(action => (
                  <Tooltip key={action.id} title={action.description}>
                    <Button
                      block
                      icon={action.icon}
                      onClick={() => showQuickActionModal(action, source)}
                      disabled={!source.enabled}
                      loading={source.status === 'running'}
                    >
                      {action.name}
                    </Button>
                  </Tooltip>
                ))}
              </Space>
            </Card>
          </Col>
        ))}
      </Row>

      {/* 定时任务管理区域 */}
      <Card 
        title={
          <Space>
            <ScheduleOutlined />
            <span>Scheduled Tasks</span>
          </Space>
        }
        style={{ marginTop: '24px' }}
      >
        <Tabs 
          defaultActiveKey="tasks" 
          items={[
          {
            key: 'tasks',
            label: (
              <Space>
                <ScheduleOutlined />
                任务管理
              </Space>
            ),
            children: (
              <DatabaseTableViewer />
            )
          },
          {
            key: 'execution_history',
            label: (
              <Space>
                <BarChartOutlined />
                执行历史
              </Space>
            ),
            children: (
              <TaskExecutionHistory showTaskFilter={true} height={600} />
            )
          }
        ]} />
      </Card>





      {/* 数据源专用配置模态框 */}
      {selectedDataSource && (
        <DataSourceConfigModal
          open={dataSourceConfigVisible}
          onCancel={() => {
            setDataSourceConfigVisible(false);
            setSelectedDataSource(null);
          }}
          onSuccess={handleDataSourceConfigSuccess}
          dataSourceType={selectedDataSource.id as 'eodhd_news' | 'eodhd_events' | 'eodhd_macro' | 'eodhd_exchange'}
          dataSourceName={selectedDataSource.name}
        />
      )}

      {/* 快速操作参数配置对话框 */}
      <Modal
        title={
          <Space>
            <CloudDownloadOutlined />
            {quickActionModal.action?.name}
          </Space>
        }
        open={quickActionModal.visible}
        onOk={confirmQuickAction}
        onCancel={() => {
          setQuickActionModal({ visible: false, action: null, dataSource: null });
          form.resetFields();
        }}
        okText="Execute"
        cancelText="Cancel"
        width={800}
        destroyOnClose
      >
        {/* 进度条 */}


        {quickActionModal.action && (
          <div>
            <div style={{ marginBottom: '16px' }}>
              <Text type="secondary">{quickActionModal.action.description}</Text>
            </div>
            
            {quickActionModal.action.id === 'fetch_latest_news' ? (
              <Form form={form} layout="vertical">
                <Tabs defaultActiveKey="basic">
                  <Tabs.TabPane
                    tab={<span><SettingOutlined />Basic Settings</span>}
                    key="basic"
                  >
                    <Row gutter={16}>
                      <Col span={12}>
                        <Form.Item
                          name="symbols"
                          label="Stock Symbols"
                          tooltip="Enter stock symbols, multiple separated by commas, e.g.: AAPL.US,TSLA.US"
                        >
                          <Input placeholder="e.g.: AAPL.US,TSLA.US" />
                        </Form.Item>
                      </Col>
                      <Col span={12}>
                        <Form.Item
                          name="topic"
                          label="Topic Tags"
                          tooltip="EODHD API topic parameters, e.g.: technology, politics"
                        >
                          <Input placeholder="e.g.: technology, politics" />
                        </Form.Item>
                      </Col>
                      <Col span={24}>
                        <Form.Item
                          name="dateRange"
                          label="Date Range"
                          rules={[{ required: true, message: 'Please select date range' }]}
                        >
                          <RangePicker style={{ width: '100%' }} />
                        </Form.Item>
                      </Col>
                      <Col span={12}>
                        <Form.Item
                          name="max_results"
                          label="Max Results"
                          rules={[{ required: true, message: 'Please enter max results' }]}
                          extra="Maximum number of news articles to fetch, up to 1000"
                        >
                          <InputNumber min={1} max={1000} placeholder="100" style={{ width: '100%' }} />
                        </Form.Item>
                      </Col>
                    </Row>
                  </Tabs.TabPane>
                  
                  <Tabs.TabPane
                    tab={<span><InfoCircleOutlined />Advanced Settings</span>}
                    key="advanced"
                  >
                    <Row gutter={16}>
                      <Col span={12}>
                        <Form.Item
                          name="topic_filter"
                          label="Topic Filter"
                          tooltip="Filter using themes from database"
                        >
                          <Select placeholder="Select theme" allowClear>
                            {themes.filter(theme => theme.is_active).map(theme => (
                              <Option key={theme.id} value={theme.topic_name}>
                                <span style={{ color: theme.color || '#1890ff' }}>●</span> {theme.topic_name}
                              </Option>
                            ))}
                          </Select>
                        </Form.Item>
                      </Col>
                      <Col span={12}>
                        <Form.Item
                          name="keywords"
                          label="Custom Keywords"
                          tooltip="Custom keywords, multiple separated by commas"
                        >
                          <Input placeholder="e.g.: inflation,interest rate" />
                        </Form.Item>
                      </Col>
                      <Col span={24}>
                        <Alert
                          message="Advanced Filtering Instructions"
                          description={
                            <div>
                              <p>• <strong>Topic Filter</strong>: Smart filtering based on theme keywords in database</p>
                              <p>• <strong>Custom Keywords</strong>: Content matching using custom keywords</p>
                              <p>• <strong>Combined Usage</strong>: Can use multiple filter conditions simultaneously</p>
                            </div>
                          }
                          type="info"
                          showIcon
                          style={{ marginBottom: 16 }}
                        />
                      </Col>
                    </Row>
                  </Tabs.TabPane>
                  
                  <Tabs.TabPane
                    tab={<span><TagOutlined />Theme Management</span>}
                    key="themes"
                  >
                    <div style={{ marginBottom: 16 }}>
                      <Space wrap>
                        <Button
                          type="primary"
                          icon={<PlusOutlined />}
                          onClick={() => {
                            setSelectedTheme(null);
                            setThemeModalVisible(true);
                            themeForm.resetFields();
                          }}
                          size="small"
                        >
                          New Theme
                        </Button>
                        <Button
                          icon={<BulbOutlined />}
                          onClick={initializeDefaultThemes}
                          size="small"
                        >
                          Initialize Default Themes
                        </Button>
                        <Button
                          icon={<ReloadOutlined />}
                          onClick={loadThemes}
                          size="small"
                        >
                          Refresh
                        </Button>
                      </Space>
                    </div>

                    <Alert
                      message="Theme Management Instructions"
                      description="Themes are used for intelligent news classification and filtering. Each theme contains multiple keywords, and the system automatically adds corresponding tags to news based on these keywords."
                      type="info"
                      showIcon
                      style={{ marginBottom: 16 }}
                    />

                    <Table
                      size="small"
                      pagination={false}
                      dataSource={themes}
                      rowKey="id"
                      scroll={{ y: 300 }}
                      columns={[
                        {
                          title: 'Theme Name',
                          dataIndex: 'topic_name',
                          key: 'topic_name',
                          render: (text: string, record: NewsTheme) => (
                            <Space>
                              <Badge color={record.color || '#1890ff'} />
                              <Text strong style={{ fontSize: '12px' }}>{text}</Text>
                            </Space>
                          )
                        },
                        {
                          title: 'Keywords',
                          dataIndex: 'keywords',
                          key: 'keywords',
                          render: (keywords: string[]) => (
                            <div>
                              {keywords.slice(0, 3).map((keyword, index) => (
                                <Tag key={index} style={{ fontSize: '10px' }}>
                                  {keyword}
                                </Tag>
                              ))}
                              {keywords.length > 3 && (
                                <Text type="secondary" style={{ fontSize: '10px' }}>
                                  +{keywords.length - 3}
                                </Text>
                              )}
                            </div>
                          )
                        },
                        {
                          title: 'Status',
                          dataIndex: 'is_active',
                          key: 'is_active',
                          render: (active: boolean) => (
                            <Tag color={active ? 'green' : 'default'} style={{ fontSize: '10px' }}>
                              {active ? 'Active' : 'Disabled'}
                            </Tag>
                          )
                        },
                        {
                          title: 'Actions',
                          key: 'actions',
                          render: (record: NewsTheme) => (
                            <Space size="small">
                              <Button
                                type="link"
                                size="small"
                                icon={<EditOutlined />}
                                onClick={() => {
                                  setSelectedTheme(record);
                                  setThemeModalVisible(true);
                                  themeForm.setFieldsValue({
                                    topic_name: record.topic_name,
                                    description: record.description,
                                    color: record.color,
                                    is_active: record.is_active,
                                    keywords: record.keywords.join(', ')
                                  });
                                }}
                                style={{ fontSize: '10px', padding: '2px 4px' }}
                              />
                                                              <Popconfirm
                                  title="Are you sure you want to delete this theme?"
                                  onConfirm={() => deleteTheme(record.id)}
                                  okText="Confirm"
                                  cancelText="Cancel"
                                >
                                <Button
                                  type="link"
                                  size="small"
                                  icon={<DeleteOutlined />}
                                  danger
                                  style={{ fontSize: '10px', padding: '2px 4px' }}
                                />
                              </Popconfirm>
                            </Space>
                          )
                        }
                      ]}
                    />
                  </Tabs.TabPane>
                </Tabs>
              </Form>
            ) : (
              <Form form={form} layout="vertical">
                {quickActionModal.action.id === 'fetch_economic_events' && (
                  <>
                    <Form.Item
                      label="Result Limit"
                      name="limit"
                      rules={[{ required: true, type: 'number', min: 1, max: 1000 }]}
                    >
                      <Input type="number" placeholder="Maximum number of economic events to fetch" />
                    </Form.Item>
                    <Form.Item label="Start Date" name="from_date">
                      <Input type="date" placeholder="Optional, format: YYYY-MM-DD" />
                    </Form.Item>
                    <Form.Item label="End Date" name="to_date">
                      <Input type="date" placeholder="Optional, format: YYYY-MM-DD" />
                    </Form.Item>
                  </>
                )}
                
                {(quickActionModal.action.id === 'fetch_macro_indicators_usa' || 
                  quickActionModal.action.id === 'fetch_macro_indicators_chn') && (
                  <Form.Item label="Indicator Code" name="indicator">
                    <Select placeholder="Optional, select indicator (leave empty to get all indicators)">
                      <Select.Option value="gdp_current_usd">GDP (Current USD)</Select.Option>
                      <Select.Option value="gdp_per_capita_usd">GDP per Capita</Select.Option>
                      <Select.Option value="gdp_growth_annual">GDP Annual Growth Rate</Select.Option>
                      <Select.Option value="inflation_consumer_prices_annual">Consumer Price Inflation Rate</Select.Option>
                      <Select.Option value="unemployment_total">Unemployment Rate</Select.Option>
                      <Select.Option value="debt_percent_gdp">Debt as % of GDP</Select.Option>
                    </Select>
                  </Form.Item>
                )}
              </Form>
            )}
          </div>
        )}
      </Modal>

      {/* 主题编辑模态框 */}
      <Modal
        title={selectedTheme ? "Edit Theme" : "New Theme"}
        open={themeModalVisible}
        onOk={() => themeForm.submit()}
        onCancel={() => {
          setThemeModalVisible(false);
          setSelectedTheme(null);
          themeForm.resetFields();
        }}
        okText="Save"
        cancelText="Cancel"
      >
        <Form
          form={themeForm}
          layout="vertical"
          onFinish={(values) => {
            const formattedValues = {
              ...values,
              keywords: values.keywords.split(',').map((k: string) => k.trim()).filter((k: string) => k)
            };
            
            if (selectedTheme) {
              updateTheme(selectedTheme.id, formattedValues);
            } else {
              createTheme(formattedValues);
            }
          }}
        >
          <Form.Item
            label="Theme Name"
            name="topic_name"
            rules={[{ required: true, message: 'Please enter theme name' }]}
          >
            <Input placeholder="Enter theme name" />
          </Form.Item>
          
          <Form.Item
            label="Description"
            name="description"
          >
            <Input.TextArea placeholder="Enter theme description" rows={3} />
          </Form.Item>
          
          <Form.Item
            label="Color"
            name="color"
          >
            <Input placeholder="#1890ff" />
          </Form.Item>
          
          <Form.Item
            label="Keywords"
            name="keywords"
            rules={[{ required: true, message: 'Please enter keywords' }]}
            tooltip="Multiple keywords separated by commas"
          >
            <Input.TextArea 
              placeholder="Enter keywords separated by commas, e.g.: technology,AI,machine learning" 
              rows={3}
            />
          </Form.Item>
          
          <Form.Item
            label="Status"
            name="is_active"
            valuePropName="checked"
            initialValue={true}
          >
            <Switch checkedChildren="Enabled" unCheckedChildren="Disabled" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default SchedulerPage;
