import React, { useState, useEffect } from 'react';
import {
  Modal,
  Steps,
  Form,
  Input,
  Select,
  Button,
  Space,
  Card,
  Typography,
  Alert,
  Row,
  Col,
  InputNumber,
  Tooltip,
  message,
  Switch
} from 'antd';
import {
  ClockCircleOutlined,
  InfoCircleOutlined,
  CheckCircleOutlined,
  ScheduleOutlined,
  BulbOutlined,
  BarChartOutlined,
  QuestionCircleOutlined,
  DatabaseOutlined,
  LineChartOutlined,
  CalendarOutlined
} from '@ant-design/icons';
import { schedulerAPI, CreateEODHDNewsTaskParams, TimezoneOption } from '../services/scheduler';
import CronExpressionGuide from './CronExpressionGuide';

const { Title, Text } = Typography;
const { Option } = Select;

interface DataSourceConfigModalProps {
  open: boolean;
  onCancel: () => void;
  onSuccess: (taskId: string) => void;
  dataSourceType: 'eodhd_news' | 'watchlist_news' | 'eodhd_events' | 'eodhd_macro' | 'eodhd_fundamentals' | 'finviz_screener' | 'eodhd_stock_prices' | 'eodhd_eod_stock_prices' | 'futu_themes' | 'market_data' | 'earnings_calendar' | 'ipo_calendar';
  dataSourceName: string;
}

interface ConfigFormData {
  // 调度配置
  cronExpression: string;
  timezone: string;
  
  // 任务参数
  taskParams?: Record<string, any>;
}

const DataSourceConfigModal: React.FC<DataSourceConfigModalProps> = ({
  open,
  onCancel,
  onSuccess,
  dataSourceType,
  dataSourceName
}) => {
  const [currentStep, setCurrentStep] = useState(0);
  const [form] = Form.useForm<ConfigFormData>();
  const [loading, setLoading] = useState(false);

  const [timezoneOptions, setTimezoneOptions] = useState<TimezoneOption[]>([]);
  const [formData, setFormData] = useState<Partial<ConfigFormData>>({});
  const [showCronGuide, setShowCronGuide] = useState(false);

  // 数据源配置映射
  const dataSourceConfigs = {
    eodhd_news: {
      icon: <InfoCircleOutlined style={{ color: '#1890ff' }} />,
      params: ['max_results', 'recent_days', 'symbols', 'topic', 'keywords'],
      defaultSchedule: '*/15 * * * *',
      defaultName: 'EODHD News Auto Fetch',
      description: 'Get latest stock news and market information from EODHD API'
    },
    watchlist_news: {
      icon: <BulbOutlined style={{ color: '#f5222d' }} />,
      params: ['max_results', 'recent_days'],
      defaultSchedule: '*/15 * * * *',
      defaultName: 'My Watchlist News Auto Fetch',
      description: 'Get news for stocks in your watchlist automatically from EODHD API'
    },
    eodhd_events: {
      icon: <ScheduleOutlined style={{ color: '#52c41a' }} />,
      params: ['limit'],
      defaultSchedule: '0 9 * * 1-5',
      defaultName: 'Economic Events Auto Fetch',
      description: 'Get economic calendar event data including GDP, CPI and other important indicators'
    },
    eodhd_macro: {
      icon: <BarChartOutlined style={{ color: '#722ed1' }} />,
      params: ['country', 'indicator'],
      defaultSchedule: '0 10 1 * *',
      defaultName: 'Macro Indicators Auto Fetch',
      description: 'Get macroeconomic indicator data such as inflation rate, unemployment rate, etc.'
    },
    eodhd_fundamentals: {
      icon: <BarChartOutlined style={{ color: '#13c2c2' }} />,
      params: ['exchanges', 'data_types'],
      defaultSchedule: '0 6 * * 0',
      defaultName: 'EODHD Fundamentals Auto Fetch',
      description: 'Get comprehensive company fundamentals data from EODHD API for multiple markets'
    },
    finviz_screener: {
      icon: <DatabaseOutlined style={{ color: '#fa8c16' }} />,
      params: [],
      defaultSchedule: '0 8 * * 1-5',
      defaultName: 'Finviz Screener Data Auto Fetch',
      description: 'Get comprehensive stock screener data from Finviz with technical and fundamental metrics (no parameters required)'
    },
    eodhd_stock_prices: {
      icon: <BarChartOutlined style={{ color: '#1890ff' }} />,
      params: [],
      defaultSchedule: '*/5 * * * 1-5',
      defaultName: 'EODHD Stock Prices Auto Fetch',
      description: 'Get real-time stock prices for all watchlist stocks from EODHD API (every 5 minutes on weekdays)'
    },
    eodhd_eod_stock_prices: {
      icon: <BarChartOutlined style={{ color: '#52c41a' }} />,
      params: ['days_back'],
      defaultSchedule: '0 9 * * 1-5',
      defaultName: 'EODHD EOD Stock Prices Auto Fetch',
      description: 'Get historical end-of-day stock prices for all watchlist stocks from EODHD API (daily on weekdays)'
    },
    futu_themes: {
      icon: <DatabaseOutlined style={{ color: '#eb2f96' }} />,
      params: ['markets', 'max_pages', 'scrape_stocks'],
      defaultSchedule: '0 2 * * *',
      defaultName: 'Futu Themes Auto Fetch',
      description: 'Get investment themes and related stocks from Futu website (HK and US markets, daily at 2 AM HKT)'
    },
    market_data: {
      icon: <LineChartOutlined style={{ color: '#52c41a' }} />,
      params: ['asset_types', 'fetch_realtime', 'fetch_eod', 'eod_days'],
      defaultSchedule: '0 */6 * * *',
      defaultName: 'Market Data Auto Fetch',
      description: 'Get real-time and historical market data (indices, forex, commodities, bonds, sectors) every 6 hours'
    },
    earnings_calendar: {
      icon: <ClockCircleOutlined style={{ color: '#fa8c16' }} />,
      params: ['days', 'fetch_next_week', 'start_date', 'end_date'],
      defaultSchedule: '0 6 * * 0',
      defaultName: 'Earnings Calendar Auto Fetch',
      description: 'Get US earnings calendar data weekly (no S&P 500 filtering, all US stocks)'
    },
    ipo_calendar: {
      icon: <CalendarOutlined style={{ color: '#722ed1' }} />,
      params: ['days', 'fetch_next_week', 'start_date', 'end_date'],
      defaultSchedule: '0 6 * * 0',
      defaultName: 'IPO Calendar Auto Fetch',
      description: 'Get IPO calendar data weekly from EODHD API (all exchanges). Note: EODHD API does not support exchange filtering - all exchanges data will be returned.'
    }
  };

  const currentConfig = dataSourceConfigs[dataSourceType];

  const loadPresets = async () => {
    try {
      console.log('Loading timezone configuration...');
      const timezonesResponse = await schedulerAPI.getTimezonePresets();
      console.log('Timezone API response:', timezonesResponse);
      
      if (timezonesResponse.success) {
        console.log('Setting timezone options:', timezonesResponse.data);
        setTimezoneOptions(timezonesResponse.data || []);
      } else {
        console.error('Timezone API returned failure:', timezonesResponse.message);
      }
    } catch (error) {
      console.error('Failed to load timezone configuration:', error);
    }
  };

  const resetModal = () => {
    // EODHD Fundamentals不需要schedule configuration，直接跳到参数配置
    const startStep = dataSourceType === 'eodhd_fundamentals' ? 1 : 0;
    setCurrentStep(startStep);
    setFormData({});
    form.resetFields();
    
    // 设置默认值（不显示cron表达式）
    form.setFieldsValue({
      timezone: 'America/New_York'  // 默认选择美国纽约时区
    });
  };

  useEffect(() => {
    if (open) {
      loadPresets();
      resetModal();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  const handleNext = async () => {
    try {
      const values = await form.validateFields();
      const newFormData = { ...formData, ...values };
      setFormData(newFormData);
      
      // EODHD Fundamentals跳过schedule configuration，直接提交
      if (dataSourceType === 'eodhd_fundamentals') {
        await handleSubmit(newFormData);
      } else if (currentStep < 1) {
        setCurrentStep(currentStep + 1);
      } else {
        await handleSubmit(newFormData);
      }
    } catch (error) {
      console.error('表单验证失败:', error);
    }
  };

  const handlePrev = () => {
    setCurrentStep(currentStep - 1);
  };

  const handleSubmit = async (data: Partial<ConfigFormData>) => {
    try {
      setLoading(true);
      
      // 显示详细的处理状态信息
      if (dataSourceType === 'eodhd_news') {
        message.loading('Creating EODHD News strategy, this may take up to 2 minutes...', 0);
      }
      
      let result;
      if (dataSourceType === 'eodhd_fundamentals') {
        // EODHD基本面数据手动更新逻辑（不需要cron表达式）
        const fundamentalsParams = {
          exchanges: data.taskParams?.exchanges || ['NYSE', 'NASDAQ'],
          data_types: data.taskParams?.data_types || ['General', 'Financials', 'Highlights', 'Valuation', 'Technicals', 'Holders', 'outstandingShares', 'Earnings', 'SharesStats', 'AnalystRatings', 'SplitsDividends', 'InsiderTransactions']
        };
        result = await schedulerAPI.executeFundamentalsUpdate(fundamentalsParams);
      } else {
        const params: CreateEODHDNewsTaskParams = {
          name: currentConfig.defaultName,
          cron_expression: data.cronExpression!,
          timezone: data.timezone!,
          enabled: true, // 新创建的任务默认启用，用户可通过主页面switch控制
          ...data.taskParams
        };

        if (dataSourceType === 'eodhd_news') {
          result = await schedulerAPI.createEODHDNewsTask(params);
        } else if (dataSourceType === 'watchlist_news') {
          // Watchlist新闻任务的专用创建逻辑
          const watchlistParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            ...data.taskParams  // ✅ 展开所有参数，包括 LLM 分析参数
          };
          result = await schedulerAPI.createWatchlistNewsTask(watchlistParams);
        } else if (dataSourceType === 'finviz_screener') {
          // Finviz筛选数据任务的专用创建逻辑
          const finvizParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true
          };
          result = await schedulerAPI.createFinvizScreenerTask(finvizParams);
        } else if (dataSourceType === 'eodhd_stock_prices') {
          // EODHD股票价格任务的专用创建逻辑
          const stockPricesParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true
          };
          result = await schedulerAPI.createEODHDStockPricesTask(stockPricesParams);
        } else if (dataSourceType === 'eodhd_eod_stock_prices') {
          // EODHD历史股价任务的专用创建逻辑
          const eodStockPricesParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            days_back: data.taskParams?.days_back || 60
          };
          result = await schedulerAPI.createEODHDEODStockPricesTask(eodStockPricesParams);
        } else if (dataSourceType === 'futu_themes') {
          // Futu投资主题任务的专用创建逻辑
          const futuThemesParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            markets: data.taskParams?.markets,
            max_pages: data.taskParams?.max_pages,
            scrape_stocks: data.taskParams?.scrape_stocks !== undefined ? data.taskParams.scrape_stocks : true
          };
          result = await schedulerAPI.createFutuThemesTask(futuThemesParams);
        } else if (dataSourceType === 'market_data') {
          // 市场数据任务的专用创建逻辑
          const marketDataParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            asset_types: data.taskParams?.asset_types,
            fetch_realtime: data.taskParams?.fetch_realtime !== undefined ? data.taskParams.fetch_realtime : true,
            fetch_eod: data.taskParams?.fetch_eod !== undefined ? data.taskParams.fetch_eod : true,
            eod_days: data.taskParams?.eod_days || 5
          };
          result = await schedulerAPI.createMarketDataTask(marketDataParams);
        } else if (dataSourceType === 'earnings_calendar') {
          // 财报日历任务的专用创建逻辑
          const earningsCalendarParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            days: data.taskParams?.days || 7,
            fetch_next_week: data.taskParams?.fetch_next_week || false,
            start_date: data.taskParams?.start_date,
            end_date: data.taskParams?.end_date
          };
          result = await schedulerAPI.createEarningsCalendarTask(earningsCalendarParams);
        } else if (dataSourceType === 'ipo_calendar') {
          // IPO 日历任务的专用创建逻辑
          // 注意：EODHD API 不支持 exchanges 参数，已移除
          const ipoCalendarParams = {
            name: currentConfig.defaultName,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true,
            days: data.taskParams?.days || 30,
            fetch_next_week: data.taskParams?.fetch_next_week || false,
            start_date: data.taskParams?.start_date,
            end_date: data.taskParams?.end_date
          };
          result = await schedulerAPI.createIPOCalendarTask(ipoCalendarParams);
        } else {
          // 其他任务类型的创建逻辑
          const taskType = dataSourceType === 'eodhd_events' ? 'economic_events' : 
                          dataSourceType === 'eodhd_macro' ? 'macro_indicators' : 
                          dataSourceType;
          
          result = await schedulerAPI.createCronTask({
            name: currentConfig.defaultName,
            task_type: taskType,
            cron_expression: data.cronExpression!,
            timezone: data.timezone!,
            enabled: true, // 新创建的任务默认启用，用户可通过主页面switch控制
            params: data.taskParams
          });
        }
      }

      // 关闭loading消息
      message.destroy();

      if (result.success) {
        const successMessage = dataSourceType === 'eodhd_fundamentals' 
          ? 'Data update initiated successfully!' 
          : 'Data acquisition strategy configuration successful!';
        message.success(successMessage);
        onSuccess(result.data?.task_id || '');
        handleCancel();
      } else {
        message.error(result.message || (dataSourceType === 'eodhd_fundamentals' ? 'Data update failed' : 'Configuration failed'));
      }
    } catch (error: any) {
      console.error('Configuration failed:', error);
      
      // 关闭loading消息
      message.destroy();
      
      // 提供更详细的错误信息
      let errorMessage = 'Configuration failed';
      if (error.code === 'ECONNABORTED' || error.message?.includes('timeout')) {
        errorMessage = 'Request timeout. The strategy creation is taking longer than expected. Please check the task list in a moment to see if it was created successfully.';
      } else if (error.response?.status === 504) {
        errorMessage = 'Gateway timeout. The strategy creation is in progress but taking longer than expected. Please check the task list in a moment.';
      } else if (error.response?.data?.detail) {
        errorMessage = error.response.data.detail;
      } else if (error.message) {
        errorMessage = error.message;
      }
      
      message.error(errorMessage, 8); // 显示8秒
    } finally {
      setLoading(false);
    }
  };

  const handleCancel = () => {
    resetModal();
    onCancel();
  };

  const renderStepContent = () => {
    switch (currentStep) {
      case 0:
        return renderScheduleConfigStep();
      case 1:
        return renderTaskParamsStep();
      default:
        return null;
    }
  };

  const renderScheduleConfigStep = () => (
    <div>
      <div style={{ textAlign: 'center', marginBottom: 24 }}>
        <Title level={4}>
          <ClockCircleOutlined /> Schedule Configuration
        </Title>
        <Text type="secondary">
          Set the execution time and frequency for data acquisition for {dataSourceName}
        </Text>
      </div>

      {/* Data source information card */}
      <Card 
        style={{ marginBottom: 24, backgroundColor: '#f9f9f9' }}
        size="small"
      >
        <Row align="middle">
          <Col span={2}>
            {currentConfig.icon}
          </Col>
          <Col span={22}>
            <div>
              <Text strong>{dataSourceName}</Text>
              <div style={{ marginTop: 4 }}>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  {currentConfig.description}
                </Text>
              </div>
            </div>
          </Col>
        </Row>
      </Card>

      <Form.Item
        label={
          <Space>
            Cron Expression
            <Button
              type="text"
              size="small"
              icon={<QuestionCircleOutlined />}
              onClick={() => setShowCronGuide(true)}
              style={{ padding: 0, height: 'auto' }}
            >
              Usage Guide
            </Button>
          </Space>
        }
        name="cronExpression"
        rules={[
          { required: true, message: 'Please enter a Cron expression' },
          {
            validator: (_, value) => {
              if (!value) return Promise.resolve();
              const { valid, message: msg } = schedulerAPI.validateCronExpression(value);
              return valid ? Promise.resolve() : Promise.reject(new Error(msg));
            }
          }
        ]}
        extra="Format: Minutes Hours Day Month Week (e.g., */15 * * * * means execute every 15 minutes)"
      >
        <Input 
          placeholder="Enter Cron expression, e.g., */15 * * * *"
          suffix={
            <Tooltip title="Format: Minutes Hours Day Month Week">
              <InfoCircleOutlined />
            </Tooltip>
          }
        />
      </Form.Item>

      <Form.Item
        label="Timezone"
        name="timezone"
        rules={[{ required: true, message: 'Please select a timezone' }]}
      >
        <Select placeholder="Select timezone" showSearch>
          {timezoneOptions.map(tz => (
            <Option key={tz.value} value={tz.value}>
              {tz.label}
            </Option>
          ))}
        </Select>
      </Form.Item>




    </div>
  );

  const renderTaskParamsStep = () => {
    const renderParamFields = () => {
      switch (dataSourceType) {
        case 'eodhd_news':
          return (
            <>
              <Alert
                message="News Fetching Parameters"
                type="info"
                showIcon
                style={{ marginBottom: 16 }}
                description="Configure news data source and filtering parameters"
              />
              
              <Form.Item
                label="Max Results"
                name={['taskParams', 'max_results']}
                extra="Maximum number of news items to fetch per time, up to 1000"
              >
                <InputNumber 
                  min={1} 
                  max={1000} 
                  placeholder="100" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Recent Days"
                name={['taskParams', 'recent_days']}
                extra="Limit news to recent N days from current date. Leave empty to fetch all available news."
              >
                <InputNumber 
                  min={1} 
                  max={365} 
                  placeholder="Leave empty for all news" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Stock Symbols"
                name={['taskParams', 'symbols']}
                extra="Specify stock symbols, separated by commas, leave empty to get all"
              >
                <Input placeholder="AAPL,MSFT,GOOGL" />
              </Form.Item>
              
              <Form.Item
                label="Topic Tags"
                name={['taskParams', 'topic']}
                extra="News topic classification"
              >
                <Select placeholder="Select topic" allowClear>
                  <Option value="technology">Technology</Option>
                  <Option value="finance">Finance</Option>
                  <Option value="economics">Economics</Option>
                  <Option value="earnings">Earnings</Option>
                </Select>
              </Form.Item>
              
              <Form.Item
                label="Keywords"
                name={['taskParams', 'keywords']}
                extra="News content keywords, separated by commas"
              >
                <Input placeholder="AI,blockchain,cryptocurrency" />
              </Form.Item>
              
              <Form.Item
                label="Enable Auto Analysis"
                name={['taskParams', 'auto_analyze']}
                valuePropName="checked"
                extra="Automatically analyze news with LLM immediately after fetching. Recommended for real-time news analysis."
                initialValue={true}
              >
                <Switch 
                  checkedChildren="Enabled" 
                  unCheckedChildren="Disabled" 
                />
              </Form.Item>
              
              <Form.Item
                label="Use Celery (Async Processing)"
                name={['taskParams', 'use_celery']}
                valuePropName="checked"
                extra="Use Celery for async processing (STRONGLY RECOMMENDED for multi-user cloud applications to avoid blocking)"
                initialValue={true}
              >
                <Switch 
                  checkedChildren="Celery" 
                  unCheckedChildren="Direct" 
                />
              </Form.Item>
              
              <Form.Item
                label="Analysis Templates (LLM)"
                name={['taskParams', 'analysis_templates']}
                extra="Select one or more LLM prompt templates for text analysis (multiple templates will run in parallel)"
                initialValue={["news_summary_v1", "news_classification"]}
              >
                <Select 
                  mode="multiple"
                  placeholder="Select one or more analysis templates"
                  maxTagCount="responsive"
                >
                  <Option value="news_summary_v1">News Summary v1 (Default)</Option>
                  <Option value="news_sentiment_analysis">News Sentiment Analysis</Option>
                  <Option value="news_classification">News Classification</Option>
                </Select>
              </Form.Item>
              
              <Alert
                message={
                  <span>
                    <strong>LLM Analysis Cost:</strong> ~¥0.02 per news item per template
                  </span>
                }
                type="info"
                showIcon
                style={{ marginBottom: 16 }}
              />
              
              <Form.Item
                label="Analysis Batch Size"
                name={['taskParams', 'analysis_batch_size']}
                extra="Maximum number of news items to analyze per batch. Leave empty to analyze all new items."
                initialValue={1000}
              >
                <InputNumber 
                  min={1} 
                  max={5000} 
                  placeholder="1000" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Max Concurrent"
                name={['taskParams', 'max_concurrent']}
                extra="Number of concurrent LLM API calls (1-10). Higher values = faster but more API load."
                initialValue={8}
              >
                <InputNumber 
                  min={1} 
                  max={10} 
                  placeholder="8" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <div style={{ 
                borderTop: '1px solid #f0f0f0', 
                paddingTop: 24, 
                marginTop: 24, 
                marginBottom: 16 
              }}>
                <h4 style={{ marginBottom: 16, color: '#1890ff' }}>
                  🎯 Investment Theme Matching (Vector Search)
                </h4>
              </div>
              
              <Form.Item
                label="Enable Theme Matching"
                name={['taskParams', 'enable_theme_matching']}
                valuePropName="checked"
                extra="Automatically match news to 150+ investment themes using vector similarity search"
                initialValue={true}
              >
                <Switch 
                  checkedChildren="✓ Enabled" 
                  unCheckedChildren="✗ Disabled"
                />
              </Form.Item>
              
              <Alert
                message={
                  <Space size="large">
                    <span>⚡ <strong>Speed:</strong> 50-100ms/item</span>
                    <span>💰 <strong>Cost:</strong> ¥0.0007/item</span>
                    <span>🎯 <strong>Accuracy:</strong> 82%</span>
                  </Space>
                }
                type="success"
                showIcon
                icon={<CheckCircleOutlined />}
                style={{ marginBottom: 16 }}
                description="Vector-based theme matching is 140x cheaper and 20x faster than LLM analysis. Recommended to keep enabled."
              />
            </>
          );
          
        case 'watchlist_news':
          return (
            <>
              <Alert
                message="Stock symbols are automatically fetched from your watchlist"
                description="This scheduler will automatically get news for all stocks in your watchlist based on the primaryticker field from target_tracking_config table."
                type="info"
                showIcon
                style={{ marginBottom: 16 }}
              />
              
              <Form.Item
                label="Max Results"
                name={['taskParams', 'max_results']}
                extra="Maximum number of news items to fetch per time, up to 1000"
              >
                <InputNumber 
                  min={1} 
                  max={1000} 
                  placeholder="100" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Recent Days"
                name={['taskParams', 'recent_days']}
                extra="Limit news to recent N days from current date. Leave empty to fetch all available news."
              >
                <InputNumber 
                  min={1} 
                  max={365} 
                  placeholder="Leave empty for all news" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Enable Auto Analysis"
                name={['taskParams', 'auto_analyze']}
                valuePropName="checked"
                extra="Automatically analyze news with LLM immediately after fetching. Recommended for real-time news analysis."
                initialValue={true}
              >
                <Switch 
                  checkedChildren="Enabled" 
                  unCheckedChildren="Disabled" 
                />
              </Form.Item>
              
              <Form.Item
                label="Use Celery (Async Processing)"
                name={['taskParams', 'use_celery']}
                valuePropName="checked"
                extra="Use Celery for async processing (STRONGLY RECOMMENDED for multi-user cloud applications to avoid blocking)"
                initialValue={true}
              >
                <Switch 
                  checkedChildren="Celery" 
                  unCheckedChildren="Direct" 
                />
              </Form.Item>
              
              <Form.Item
                label="Analysis Templates (LLM)"
                name={['taskParams', 'analysis_templates']}
                extra="Select one or more LLM prompt templates for text analysis (multiple templates will run in parallel)"
                initialValue={["news_summary_v1", "news_classification"]}
              >
                <Select 
                  mode="multiple"
                  placeholder="Select one or more analysis templates"
                  maxTagCount="responsive"
                >
                  <Option value="news_summary_v1">News Summary v1 (Default)</Option>
                  <Option value="news_sentiment_analysis">News Sentiment Analysis</Option>
                  <Option value="news_classification">News Classification</Option>
                </Select>
              </Form.Item>
              
              <Alert
                message={
                  <span>
                    <strong>LLM Analysis Cost:</strong> ~¥0.02 per news item per template
                  </span>
                }
                type="info"
                showIcon
                style={{ marginBottom: 16 }}
              />
              
              <Form.Item
                label="Analysis Batch Size"
                name={['taskParams', 'analysis_batch_size']}
                extra="Maximum number of news items to analyze per batch. Leave empty to analyze all new items."
                initialValue={1000}
              >
                <InputNumber 
                  min={1} 
                  max={5000} 
                  placeholder="1000" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Max Concurrent"
                name={['taskParams', 'max_concurrent']}
                extra="Number of concurrent LLM API calls (1-10). Higher values = faster but more API load."
                initialValue={8}
              >
                <InputNumber 
                  min={1} 
                  max={10} 
                  placeholder="8" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <div style={{ 
                borderTop: '1px solid #f0f0f0', 
                paddingTop: 24, 
                marginTop: 24, 
                marginBottom: 16 
              }}>
                <h4 style={{ marginBottom: 16, color: '#1890ff' }}>
                  🎯 Investment Theme Matching (Vector Search)
                </h4>
              </div>
              
              <Form.Item
                label="Enable Theme Matching"
                name={['taskParams', 'enable_theme_matching']}
                valuePropName="checked"
                extra="Automatically match news to 150+ investment themes using vector similarity search"
                initialValue={true}
              >
                <Switch 
                  checkedChildren="✓ Enabled" 
                  unCheckedChildren="✗ Disabled"
                />
              </Form.Item>
              
              <Alert
                message={
                  <Space size="large">
                    <span>⚡ <strong>Speed:</strong> 50-100ms/item</span>
                    <span>💰 <strong>Cost:</strong> ¥0.0007/item</span>
                    <span>🎯 <strong>Accuracy:</strong> 82%</span>
                  </Space>
                }
                type="success"
                showIcon
                icon={<CheckCircleOutlined />}
                style={{ marginBottom: 16 }}
                description="Vector-based theme matching is 140x cheaper and 20x faster than LLM analysis. Recommended to keep enabled."
              />
            </>
          );
          
        case 'eodhd_events':
          return (
            <>
              <Form.Item
                label="Data Limit"
                name={['taskParams', 'limit']}
                extra="Maximum number of events to fetch per time, up to 1000"
              >
                <InputNumber 
                  min={1} 
                  max={1000} 
                  placeholder="100"
                  style={{ width: '100%' }}
                />
              </Form.Item>
            </>
          );
          
        case 'eodhd_macro':
          return (
            <>
              <Form.Item
                label="Country"
                name={['taskParams', 'country']}
                rules={[{ required: true, message: 'Please select a country' }]}
                extra="Specify which countries to get macro indicators for, multiple selection allowed"
              >
                <Select 
                  placeholder="Select country" 
                  mode="multiple"
                  showSearch
                  optionLabelProp="label"
                  filterOption={(input, option) => {
                    const label = String(option?.label || '');
                    return label.toLowerCase().includes(input.toLowerCase());
                  }}
                >
                  <Option value="USA" label="🇺🇸 United States (USA)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇺🇸</span>
                      <span>United States (USA)</span>
                    </div>
                  </Option>
                  <Option value="CHN" label="🇨🇳 China (CHN)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇳</span>
                      <span>China (CHN)</span>
                    </div>
                  </Option>
                  <Option value="JPN" label="🇯🇵 Japan (JPN)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇯🇵</span>
                      <span>Japan (JPN)</span>
                    </div>
                  </Option>
                  <Option value="DEU" label="��🇪 Germany (DEU)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇩🇪</span>
                      <span>Germany (DEU)</span>
                    </div>
                  </Option>
                  <Option value="GBR" label="🇬🇧 United Kingdom (GBR)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇬🇧</span>
                      <span>United Kingdom (GBR)</span>
                    </div>
                  </Option>
                  <Option value="FRA" label="🇫🇷 France (FRA)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇫🇷</span>
                      <span>France (FRA)</span>
                    </div>
                  </Option>
                  <Option value="ITA" label="🇮🇹 Italy (ITA)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇹</span>
                      <span>Italy (ITA)</span>
                    </div>
                  </Option>
                  <Option value="BRA" label="🇧🇷 Brazil (BRA)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇧🇷</span>
                      <span>Brazil (BRA)</span>
                    </div>
                  </Option>
                  <Option value="CAN" label="🇨🇦 Canada (CAN)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇦</span>
                      <span>Canada (CAN)</span>
                    </div>
                  </Option>
                  <Option value="RUS" label="🇷🇺 Russia (RUS)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇷🇺</span>
                      <span>Russia (RUS)</span>
                    </div>
                  </Option>
                  <Option value="IND" label="🇮🇳 India (IND)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇳</span>
                      <span>India (IND)</span>
                    </div>
                  </Option>
                  <Option value="KOR" label="🇰🇷 South Korea (KOR)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇰🇷</span>
                      <span>South Korea (KOR)</span>
                    </div>
                  </Option>
                  <Option value="ESP" label="🇪🇸 Spain (ESP)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇪🇸</span>
                      <span>Spain (ESP)</span>
                    </div>
                  </Option>
                  <Option value="AUS" label="🇦🇺 Australia (AUS)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇦🇺</span>
                      <span>Australia (AUS)</span>
                    </div>
                  </Option>
                  <Option value="MEX" label="🇲🇽 Mexico (MEX)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇲🇽</span>
                      <span>Mexico (MEX)</span>
                    </div>
                  </Option>
                  <Option value="IDN" label="🇮🇩 Indonesia (IDN)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇩</span>
                      <span>Indonesia (IDN)</span>
                    </div>
                  </Option>
                  <Option value="NLD" label="🇳🇱 Netherlands (NLD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇳🇱</span>
                      <span>Netherlands (NLD)</span>
                    </div>
                  </Option>
                  <Option value="SAU" label="🇸🇦 Saudi Arabia (SAU)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇸🇦</span>
                      <span>Saudi Arabia (SAU)</span>
                    </div>
                  </Option>
                  <Option value="TUR" label="🇹🇷 Turkey (TUR)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇹🇷</span>
                      <span>Turkey (TUR)</span>
                    </div>
                  </Option>
                  <Option value="TWN" label="🇹🇼 Taiwan (TWN)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇹🇼</span>
                      <span>Taiwan (TWN)</span>
                    </div>
                  </Option>
                </Select>
              </Form.Item>
              
              <Form.Item
                label="Indicator Type"
                name={['taskParams', 'indicator']}
                extra="Specify macro indicator types, multiple selection allowed, leave empty to get all indicators"
              >
                <Select 
                  placeholder="Select indicator type" 
                  mode="multiple"
                  allowClear
                  showSearch
                  optionLabelProp="label"
                  filterOption={(input, option) => {
                    const label = String(option?.label || '');
                    return label.toLowerCase().includes(input.toLowerCase());
                  }}
                >
                  <Option value="real_interest_rate" label="📊 Real Interest Rate (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📊</span>
                      <span>Real Interest Rate (%)</span>
                    </div>
                  </Option>
                  <Option value="population_total" label="�� Total Population">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👥</span>
                      <span>Total Population</span>
                    </div>
                  </Option>
                  <Option value="population_growth_annual" label="📈 Annual Population Growth (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📈</span>
                      <span>Annual Population Growth (%)</span>
                    </div>
                  </Option>
                  <Option value="inflation_consumer_prices_annual" label="💰 Annual Consumer Price Inflation (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💰</span>
                      <span>Annual Consumer Price Inflation (%)</span>
                    </div>
                  </Option>
                  <Option value="consumer_price_index" label="📊 Consumer Price Index (2010=100)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📊</span>
                      <span>Consumer Price Index (2010=100)</span>
                    </div>
                  </Option>
                  <Option value="gdp_current_usd" label="💵 GDP (Current Price USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💵</span>
                      <span>GDP (Current Price USD)</span>
                    </div>
                  </Option>
                  <Option value="gdp_per_capita_usd" label="👤 Per Capita GDP (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👤</span>
                      <span>Per Capita GDP (USD)</span>
                    </div>
                  </Option>
                  <Option value="gdp_growth_annual" label="📈 Annual GDP Growth (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📈</span>
                      <span>Annual GDP Growth (%)</span>
                    </div>
                  </Option>
                  <Option value="debt_percent_gdp" label="💳 Debt to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💳</span>
                      <span>Debt to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="net_trades_goods_services" label="🔄 Net Trade in Goods and Services (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🔄</span>
                      <span>Net Trade in Goods and Services (USD)</span>
                    </div>
                  </Option>
                  <Option value="inflation_gdp_deflator_annual" label="📊 Annual GDP Deflator Inflation (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📊</span>
                      <span>Annual GDP Deflator Inflation (%)</span>
                    </div>
                  </Option>
                  <Option value="agriculture_value_added_percent_gdp" label="🌾 Agriculture Value Added to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🌾</span>
                      <span>Agriculture Value Added to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="industry_value_added_percent_gdp" label="🏭 Industry Value Added to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🏭</span>
                      <span>Industry Value Added to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="services_value_added_percent_gdp" label="💼 Services Value Added to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💼</span>
                      <span>Services Value Added to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="exports_of_goods_services_percent_gdp" label="📤 Export of Goods and Services to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📤</span>
                      <span>Export of Goods and Services to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="imports_of_goods_services_percent_gdp" label="📥 Import of Goods and Services to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📥</span>
                      <span>Import of Goods and Services to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="gross_capital_formation_percent_gdp" label="🏗️ Gross Capital Formation to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🏗️</span>
                      <span>Gross Capital Formation to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="net_migration" label="🚶 Net Migration">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🚶</span>
                      <span>Net Migration</span>
                    </div>
                  </Option>
                  <Option value="gni_usd" label="💵 Gross National Income (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💵</span>
                      <span>Gross National Income (USD)</span>
                    </div>
                  </Option>
                  <Option value="gni_per_capita_usd" label="👤 Per Capita Gross National Income (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👤</span>
                      <span>Per Capita Gross National Income (USD)</span>
                    </div>
                  </Option>
                  <Option value="gni_ppp_usd" label="💱 Purchasing Power Parity Gross National Income (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💱</span>
                      <span>Purchasing Power Parity Gross National Income (USD)</span>
                    </div>
                  </Option>
                  <Option value="gni_per_capita_ppp_usd" label="👤 Per Capita Purchasing Power Parity Gross National Income (USD)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👤</span>
                      <span>Per Capita Purchasing Power Parity Gross National Income (USD)</span>
                    </div>
                  </Option>
                  <Option value="income_share_lowest_twenty" label="📊 Income Share of Lowest 20% (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📊</span>
                      <span>Income Share of Lowest 20% (%)</span>
                    </div>
                  </Option>
                  <Option value="life_expectancy" label="👴 Life Expectancy (Years)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👴</span>
                      <span>Life Expectancy (Years)</span>
                    </div>
                  </Option>
                  <Option value="fertility_rate" label="👶 Fertility Rate (Births per Woman)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👶</span>
                      <span>Fertility Rate (Births per Woman)</span>
                    </div>
                  </Option>
                  <Option value="prevalence_hiv_total" label="🩺 HIV Prevalence (15-49 Years %)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🩺</span>
                      <span>HIV Prevalence (15-49 Years %)</span>
                    </div>
                  </Option>
                  <Option value="co2_emissions_tons_per_capita" label="🌍 CO2 Emissions per Capita (Tons)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🌍</span>
                      <span>CO2 Emissions per Capita (Tons)</span>
                    </div>
                  </Option>
                  <Option value="surface_area_km" label="🗺️ Surface Area (Square Kilometers)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🗺️</span>
                      <span>Surface Area (Square Kilometers)</span>
                    </div>
                  </Option>
                  <Option value="poverty_poverty_lines_percent_population" label="🏚️ Poverty Line Population Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🏚️</span>
                      <span>Poverty Line Population Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="revenue_excluding_grants_percent_gdp" label="💰 Revenue to GDP Ratio (Excluding Grants %)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💰</span>
                      <span>Revenue to GDP Ratio (Excluding Grants %)</span>
                    </div>
                  </Option>
                  <Option value="cash_surplus_deficit_percent_gdp" label="💸 Cash Surplus/Deficit to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💸</span>
                      <span>Cash Surplus/Deficit to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="startup_procedures_register" label="🏢 Number of Startup Procedures">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🏢</span>
                      <span>Number of Startup Procedures</span>
                    </div>
                  </Option>
                  <Option value="market_cap_domestic_companies_percent_gdp" label="📈 Domestic Listed Companies Market Cap to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📈</span>
                      <span>Domestic Listed Companies Market Cap to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="mobile_subscriptions_per_hundred" label="📱 Mobile Phone Subscriptions per 100 People">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📱</span>
                      <span>Mobile Phone Subscriptions per 100 People</span>
                    </div>
                  </Option>
                  <Option value="internet_users_per_hundred" label="🌐 Internet Users per 100 People">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🌐</span>
                      <span>Internet Users per 100 People</span>
                    </div>
                  </Option>
                  <Option value="high_technology_exports_percent_total" label="🔬 High-Tech Exports to Manufactured Goods Exports Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🔬</span>
                      <span>High-Tech Exports to Manufactured Goods Exports Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="merchandise_trade_percent_gdp" label="📦 Merchandise Trade to GDP Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>📦</span>
                      <span>Merchandise Trade to GDP Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="total_debt_service_percent_gni" label="�� Total Debt Service to GNI Ratio (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>💳</span>
                      <span>Total Debt Service to GNI Ratio (%)</span>
                    </div>
                  </Option>
                  <Option value="unemployment_total_percent" label="👷 Total Unemployment Rate (%)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>👷</span>
                      <span>Total Unemployment Rate (%)</span>
                    </div>
                  </Option>
                </Select>
              </Form.Item>
            </>
          );
          
        case 'eodhd_eod_stock_prices':
          return (
            <>
              <Form.Item
                label="Days Back"
                name={['taskParams', 'days_back']}
                extra="Number of days to look back for historical price data (1-365 days)"
                initialValue={60}
              >
                <InputNumber 
                  min={1} 
                  max={365} 
                  placeholder="60" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Alert
                message="Historical Price Data"
                description="This scheduler will automatically fetch historical end-of-day price data for all stocks in your watchlist. Data includes open, high, low, close prices, volume, and adjusted close prices."
                type="info"
                showIcon
                style={{ marginTop: 16 }}
              />
            </>
          );
          
        case 'futu_themes':
          return (
            <>
              <Alert
                message="Futu Investment Themes Scraper"
                type="info"
                showIcon
                style={{ marginBottom: 16 }}
                description="Automatically scrape investment themes and related stocks from Futu website. Supports HK and US markets with daily full updates."
              />
              
              <Form.Item
                label="Markets"
                name={['taskParams', 'markets']}
                extra="Select markets to scrape. Leave empty to scrape all available markets (HK, US)."
              >
                <Select 
                  mode="multiple"
                  placeholder="Select markets (leave empty for all)"
                  allowClear
                >
                  <Option value="HK">🇭🇰 Hong Kong (HK)</Option>
                  <Option value="US">🇺🇸 United States (US)</Option>
                </Select>
              </Form.Item>
              
              <Form.Item
                label="Max Pages Per Market"
                name={['taskParams', 'max_pages']}
                extra="Maximum number of pages to scrape per market (1-100). Leave empty to scrape all pages. ⚡ Optimized for faster scraping."
              >
                <InputNumber 
                  min={1} 
                  max={100} 
                  placeholder="Leave empty for all pages" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
              
              <Form.Item
                label="Scrape Stocks"
                name={['taskParams', 'scrape_stocks']}
                valuePropName="checked"
                extra="Enable to scrape detailed stock list for each theme. Disable to only fetch theme metadata."
                initialValue={true}
              >
                <Switch 
                  checkedChildren="Enabled" 
                  unCheckedChildren="Disabled" 
                />
              </Form.Item>
            </>
          );
          
        case 'market_data':
          return (
            <>
              <Alert
                message="Market Data Fetcher (32 Assets: Indices, Forex, Commodities, Bonds, Sectors)"
                type="info"
                showIcon
                style={{ marginBottom: 16 }}
                description="Automatically fetch real-time and historical (EOD) market data from EODHD API. Supports 8 indices, 4 forex pairs, 5 commodities, 4 bond yields, and 11 sectors."
              />
              
              <Form.Item
                label="Asset Types"
                name={['taskParams', 'asset_types']}
                extra="Select asset types to fetch. Leave empty to fetch all types (recommended)."
              >
                <Select 
                  mode="multiple"
                  placeholder="Select asset types (leave empty for all)"
                  allowClear
                >
                  <Option value="index">📈 Indices (8 assets: S&P 500, Nasdaq, Dow Jones, etc.)</Option>
                  <Option value="forex">💱 Forex (4 pairs: USD/CNH, EUR/USD, GBP/USD, USD/JPY)</Option>
                  <Option value="commodity">🛢️ Commodities (5 assets: Gold, Silver, Copper, Oil, Natural Gas)</Option>
                  <Option value="bond">📊 Bond Yields (4 types: 3M, 1Y, 5Y, 10Y Treasury)</Option>
                  <Option value="sector">🏢 Sectors (11 sectors: Tech, Finance, Healthcare, etc.)</Option>
                </Select>
              </Form.Item>
              
              <Form.Item
                label="Fetch Realtime Data"
                name={['taskParams', 'fetch_realtime']}
                valuePropName="checked"
                extra="Enable to fetch current market prices and changes (recommended)."
                initialValue={true}
              >
                <Switch 
                  checkedChildren="Enabled" 
                  unCheckedChildren="Disabled" 
                />
              </Form.Item>
              
              <Form.Item
                label="Fetch EOD Historical Data"
                name={['taskParams', 'fetch_eod']}
                valuePropName="checked"
                extra="Enable to fetch end-of-day historical OHLC data (recommended)."
                initialValue={true}
              >
                <Switch 
                  checkedChildren="Enabled" 
                  unCheckedChildren="Disabled" 
                />
              </Form.Item>
              
              <Form.Item
                label="EOD Days"
                name={['taskParams', 'eod_days']}
                extra="Number of recent days to fetch EOD data (1-30 days)."
                initialValue={5}
              >
                <InputNumber 
                  min={1} 
                  max={30} 
                  placeholder="5" 
                  style={{ width: '100%' }}
                />
              </Form.Item>
            </>
          );

        case 'earnings_calendar':
          return (
            <>
              <Alert
                message="Earnings Calendar Fetcher"
                type="info"
                showIcon
                style={{ marginBottom: 16 }}
                description="Automatically fetch US earnings calendar from EODHD API. No S&P 500 filtering - all US stocks are included."
              />
              
              <Form.Item
                label="Fetch Mode"
                name={['taskParams', 'fetch_next_week']}
                valuePropName="checked"
                extra="Enable to fetch next week's data specifically, or disable to use custom settings."
                initialValue={false}
              >
                <Switch 
                  checkedChildren="Next Week" 
                  unCheckedChildren="Custom Period" 
                />
              </Form.Item>
              
              <Form.Item
                label="Days Ahead"
                name={['taskParams', 'days']}
                extra="Number of days ahead to fetch (1-90 days). Default is 7 days."
                initialValue={7}
              >
                <InputNumber 
                  min={1} 
                  max={90} 
                  placeholder="7" 
                  style={{ width: '100%' }}
                />
              </Form.Item>

              <Alert
                message="Optional: Custom Date Range"
                type="warning"
                showIcon
                style={{ marginTop: 16, marginBottom: 16 }}
                description="You can also specify exact start and end dates. If provided, these will override the 'Days Ahead' setting."
              />
              
              <Form.Item
                label="Start Date"
                name={['taskParams', 'start_date']}
                extra="Optional: Specific start date (YYYY-MM-DD format)"
              >
                <Input placeholder="e.g., 2025-11-10" />
              </Form.Item>
              
              <Form.Item
                label="End Date"
                name={['taskParams', 'end_date']}
                extra="Optional: Specific end date (YYYY-MM-DD format)"
              >
                <Input placeholder="e.g., 2025-11-17" />
              </Form.Item>
            </>
          );

        case 'ipo_calendar':
          return (
            <>
              <Alert
                message="IPO Calendar Fetcher"
                type="info"
                showIcon
                style={{ marginBottom: 16 }}
                description="Automatically fetch IPO calendar data from EODHD API. Note: EODHD API does not support exchange filtering - all exchanges data will be returned. If you need to filter by specific exchanges, you can query the database with WHERE conditions."
              />
              
              <Alert
                message="Exchange Filtering Not Supported"
                type="warning"
                showIcon
                style={{ marginBottom: 16 }}
                description="The EODHD IPO Calendar API does not support exchange filtering. All exchanges data will be fetched and stored. You can filter by exchange when querying the database."
              />
              
              <Form.Item
                label="Fetch Mode"
                name={['taskParams', 'fetch_next_week']}
                valuePropName="checked"
                extra="Enable to fetch next week's data specifically, or disable to use custom settings."
                initialValue={false}
              >
                <Switch 
                  checkedChildren="Next Week" 
                  unCheckedChildren="Custom Period" 
                />
              </Form.Item>
              
              <Form.Item
                label="Days Ahead"
                name={['taskParams', 'days']}
                extra="Number of days ahead to fetch (1-365 days). Default is 30 days."
                initialValue={30}
              >
                <InputNumber 
                  min={1} 
                  max={365} 
                  placeholder="30" 
                  style={{ width: '100%' }}
                />
              </Form.Item>

              <Alert
                message="Optional: Custom Date Range"
                type="warning"
                showIcon
                style={{ marginTop: 16, marginBottom: 16 }}
                description="You can also specify exact start and end dates. If provided, these will override the 'Days Ahead' setting."
              />
              
              <Form.Item
                label="Start Date"
                name={['taskParams', 'start_date']}
                extra="Optional: Specific start date (YYYY-MM-DD format)"
              >
                <Input placeholder="e.g., 2025-11-10" />
              </Form.Item>
              
              <Form.Item
                label="End Date"
                name={['taskParams', 'end_date']}
                extra="Optional: Specific end date (YYYY-MM-DD format)"
              >
                <Input placeholder="e.g., 2025-12-10" />
              </Form.Item>
            </>
          );
          
        case 'eodhd_fundamentals':
          return (
            <>
              <Form.Item
                label="Exchanges"
                name={['taskParams', 'exchanges']}
                rules={[{ required: true, message: 'Please select at least one exchange' }]}
                extra="Select exchanges to fetch fundamentals data from (Supports US, China, Hong Kong, Taiwan, Japan, UK markets)"
              >
                <Select 
                  placeholder="Select exchanges" 
                  mode="multiple"
                  showSearch
                  optionLabelProp="label"
                  filterOption={(input, option) => {
                    const label = String(option?.label || '');
                    return label.toLowerCase().includes(input.toLowerCase());
                  }}
                >
                  {/* 根据数据源类型显示不同的交易所选项 */}
                  {dataSourceType === 'eodhd_fundamentals' ? (
                    <>
                      {/* EODHD Fundamentals 支持多个国家和地区的交易所 */}
                      <Option value="NYSE" label="🏛️ New York Stock Exchange (NYSE)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏛️</span>
                          <span>New York Stock Exchange (NYSE)</span>
                        </div>
                      </Option>
                      <Option value="NASDAQ" label="💻 NASDAQ">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>💻</span>
                          <span>NASDAQ Global Market</span>
                        </div>
                      </Option>
                      <Option value="AMEX" label="🏦 American Stock Exchange (AMEX)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏦</span>
                          <span>American Stock Exchange (AMEX)</span>
                        </div>
                      </Option>
                      <Option value="SHG" label="🏢 Shanghai Stock Exchange (SHG)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏢</span>
                          <span>Shanghai Stock Exchange (SHG)</span>
                        </div>
                      </Option>
                      <Option value="SHE" label="🏪 Shenzhen Stock Exchange (SHE)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏪</span>
                          <span>Shenzhen Stock Exchange (SHE)</span>
                        </div>
                      </Option>
                      <Option value="HK" label="🏙️ Hong Kong Stock Exchange (HK)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏙️</span>
                          <span>Hong Kong Stock Exchange (HK)</span>
                        </div>
                      </Option>
                      <Option value="TWO" label="🏝️ Taiwan Stock Exchange (TWO)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🏝️</span>
                          <span>Taiwan Stock Exchange (TWO)</span>
                        </div>
                      </Option>
                      <Option value="JP" label="🗾 Tokyo Stock Exchange (JP)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🗾</span>
                          <span>Tokyo Stock Exchange (JP)</span>
                        </div>
                      </Option>
                      <Option value="LSE" label="🇬🇧 London Stock Exchange (LSE)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🇬🇧</span>
                          <span>London Stock Exchange (LSE)</span>
                        </div>
                      </Option>
                      <Option value="AIM" label="💡 London AIM Market (AIM)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>💡</span>
                          <span>London AIM Market (AIM)</span>
                        </div>
                      </Option>
                    </>
                  ) : (
                    <>
                      {/* 其他数据源的完整交易所列表 */}
                      <Option value="US" label="🇺🇸 USA Stocks (NYSE, NASDAQ, etc.)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🇺🇸</span>
                          <span>USA Stocks (NYSE, NASDAQ, NYSE ARCA, OTC markets)</span>
                        </div>
                      </Option>
                      
                      {/* European Exchanges */}
                      <Option value="LSE" label="🇬🇧 London Stock Exchange">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🇬🇧</span>
                          <span>London Stock Exchange (LSE)</span>
                        </div>
                      </Option>
                  <Option value="PA" label="🇫🇷 Euronext Paris">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇫🇷</span>
                      <span>Euronext Paris (PA)</span>
                    </div>
                  </Option>
                  <Option value="AS" label="🇳🇱 Euronext Amsterdam">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇳🇱</span>
                      <span>Euronext Amsterdam (AS)</span>
                    </div>
                  </Option>
                  <Option value="BR" label="🇧🇪 Euronext Brussels">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇧🇪</span>
                      <span>Euronext Brussels (BR)</span>
                    </div>
                  </Option>
                  <Option value="VX" label="🇨🇭 Swiss Exchange (SIX)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇭</span>
                      <span>Swiss Exchange (SIX) (VX)</span>
                    </div>
                  </Option>
                  <Option value="MC" label="🇪🇸 Madrid Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇪🇸</span>
                      <span>Madrid Stock Exchange (MC)</span>
                    </div>
                  </Option>
                  <Option value="F" label="🇩🇪 Frankfurt Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇩🇪</span>
                      <span>Frankfurt Stock Exchange (F)</span>
                    </div>
                  </Option>
                  
                  {/* Asia-Pacific Exchanges */}
                  <Option value="HK" label="🇭🇰 Hong Kong Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇭🇰</span>
                      <span>Hong Kong Stock Exchange (HK)</span>
                    </div>
                  </Option>
                  <Option value="SHG" label="🇨🇳 Shanghai Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇳</span>
                      <span>Shanghai Stock Exchange (SHG)</span>
                    </div>
                  </Option>
                  <Option value="SHE" label="🇨🇳 Shenzhen Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇳</span>
                      <span>Shenzhen Stock Exchange (SHE)</span>
                    </div>
                  </Option>
                  <Option value="KO" label="🇰🇷 Korea Stock Exchange (KRX)">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇰🇷</span>
                      <span>Korea Stock Exchange (KRX) (KO)</span>
                    </div>
                  </Option>
                  <Option value="TW" label="🇹🇼 Taiwan Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇹🇼</span>
                      <span>Taiwan Stock Exchange (TW)</span>
                    </div>
                  </Option>
                  <Option value="SG" label="🇸🇬 Singapore Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇸🇬</span>
                      <span>Singapore Exchange (SG)</span>
                    </div>
                  </Option>
                  <Option value="AU" label="🇦🇺 Australian Securities Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇦🇺</span>
                      <span>Australian Securities Exchange (AU)</span>
                    </div>
                  </Option>
                  
                  {/* North America Other */}
                  <Option value="TO" label="🇨🇦 Toronto Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇨🇦</span>
                      <span>Toronto Stock Exchange (TO)</span>
                    </div>
                  </Option>
                  
                  {/* Indian Exchanges */}
                  <Option value="BSE" label="🇮🇳 Bombay Stock Exchange">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇳</span>
                      <span>Bombay Stock Exchange (BSE)</span>
                    </div>
                  </Option>
                  <Option value="NSE" label="🇮🇳 National Stock Exchange of India">
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                      <span>🇮🇳</span>
                      <span>National Stock Exchange of India (NSE)</span>
                    </div>
                  </Option>
                  
                      {/* South American Exchanges */}
                      <Option value="SA" label="🇧🇷 São Paulo Stock Exchange (B3)">
                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                          <span>🇧🇷</span>
                          <span>São Paulo Stock Exchange (B3) (SA)</span>
                        </div>
                      </Option>
                    </>
                  )}
                </Select>
              </Form.Item>
              
              {/* Data Types selection for EODHD Fundamentals Low Frequency */}
              {dataSourceType === 'eodhd_fundamentals' && (
                <Form.Item
                  label="Data Types"
                  name={['taskParams', 'data_types']}
                  rules={[]}
                  extra="Select data types to fetch. Choose from 12 comprehensive data categories for complete company analysis."
                  initialValue={['General', 'Financials', 'Highlights', 'Valuation', 'Technicals', 'Holders', 'outstandingShares', 'Earnings', 'SharesStats', 'AnalystRatings', 'SplitsDividends', 'InsiderTransactions']}
                >
                  <Select 
                    placeholder="Select data types" 
                    mode="multiple"
                    style={{ width: '100%' }}
                    disabled={false}
                  >
                    <Option value="General" label="📊 General">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>📊</span>
                        <span>General (Company information, address, officers)</span>
                      </div>
                    </Option>
                    <Option value="Financials" label="💰 Financials">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>💰</span>
                        <span>Financials (Balance sheet, income statement, cash flow)</span>
                      </div>
                    </Option>
                    <Option value="Highlights" label="⭐ Highlights">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>⭐</span>
                        <span>Highlights (Key metrics: market cap, P/E, earnings, margins)</span>
                      </div>
                    </Option>
                    <Option value="Valuation" label="📈 Valuation">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>📈</span>
                        <span>Valuation (P/E ratios, enterprise value, price multiples)</span>
                      </div>
                    </Option>
                    <Option value="Technicals" label="📊 Technicals">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>📊</span>
                        <span>Technicals (Beta, 52-week range, moving averages, short data)</span>
                      </div>
                    </Option>
                    <Option value="Holders" label="👥 Holders">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>👥</span>
                        <span>Holders (Institutional holdings, fund holdings, ownership data)</span>
                      </div>
                    </Option>
                    <Option value="outstandingShares" label="📈 Outstanding Shares">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>📈</span>
                        <span>Outstanding Shares (Annual/quarterly shares outstanding data)</span>
                      </div>
                    </Option>
                    <Option value="Earnings" label="💸 Earnings">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>💸</span>
                        <span>Earnings (Annual earnings, history, trend, estimates)</span>
                      </div>
                    </Option>
                    <Option value="SharesStats" label="📊 Shares Stats">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>📊</span>
                        <span>Shares Stats (Outstanding, float, short interest, insider %)</span>
                      </div>
                    </Option>
                    <Option value="AnalystRatings" label="🎯 Analyst Ratings">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>🎯</span>
                        <span>Analyst Ratings (Strong buy/buy/hold/sell, target price)</span>
                      </div>
                    </Option>
                    <Option value="SplitsDividends" label="💰 Splits & Dividends">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>💰</span>
                        <span>Splits & Dividends (Dividend rate/yield, split history)</span>
                      </div>
                    </Option>
                    <Option value="InsiderTransactions" label="🔐 Insider Transactions">
                      <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span>🔐</span>
                        <span>Insider Transactions (Executive trading activity)</span>
                      </div>
                    </Option>
                  </Select>
                </Form.Item>
              )}
            </>
          );
          
        default:
          return null;
      }
    };

    return (
      <div>
        <div style={{ textAlign: 'center', marginBottom: 24 }}>
          <Title level={4}>
            <BulbOutlined /> Parameter Configuration
          </Title>
          <Text type="secondary">
            Configure specific data acquisition parameters for {dataSourceName}
          </Text>
        </div>

        {renderParamFields()}

        <Alert
          message="Parameter Description"
          description="All parameters are optional, leave empty for default configuration. The task will be automatically enabled after creation, and you can control its enable/disable status through the switch on the main page, or modify parameters through the edit function."
          type="info"
          showIcon
        />
      </div>
    );
  };

  const steps = dataSourceType === 'eodhd_fundamentals' ? [
    {
      title: 'Parameter Configuration',
      description: 'Configure data types and exchanges',
      icon: <BulbOutlined />
    }
  ] : [
    {
      title: 'Schedule Configuration',
      description: 'Set execution time',
      icon: <ClockCircleOutlined />
    },
    {
      title: 'Parameter Configuration',
      description: 'Configure acquisition parameters',
      icon: <BulbOutlined />
    }
  ];

  return (
    <Modal
      title={
        <Space>
          {currentConfig.icon}
          {dataSourceType === 'eodhd_fundamentals' 
            ? `Update Data for ${dataSourceName}` 
            : `Configure Data Acquisition Strategy for ${dataSourceName}`}
        </Space>
      }
      open={open}
      onCancel={handleCancel}
      width={800}
      footer={
        <div style={{ textAlign: 'right' }}>
          <Space>
            <Button onClick={handleCancel}>
              Cancel
            </Button>
            {currentStep > 0 && dataSourceType !== 'eodhd_fundamentals' && (
              <Button onClick={handlePrev}>
                Previous Step
              </Button>
            )}
            <Button 
              type="primary" 
              onClick={handleNext}
              loading={loading}
              icon={(
                dataSourceType === 'eodhd_fundamentals' || 
                currentStep === 1
              ) ? <CheckCircleOutlined /> : undefined}
            >
              {dataSourceType === 'eodhd_fundamentals' 
                ? 'Update Data' 
                : (currentStep === 1 ? 'Create Strategy' : 'Next Step')}
            </Button>
          </Space>
        </div>
      }
      destroyOnClose
    >
      <div style={{ padding: '20px 0' }}>
        <Steps 
          current={currentStep} 
          style={{ marginBottom: 32 }}
          items={steps}
        />

        <Form
          form={form}
          layout="vertical"
          size="middle"
        >
          {renderStepContent()}
        </Form>
      </div>
      
      {/* Cron expression usage guide */}
      <CronExpressionGuide
        open={showCronGuide}
        onCancel={() => setShowCronGuide(false)}
      />
    </Modal>
  );
};

export default DataSourceConfigModal;
