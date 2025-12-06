import React, { useState, useEffect } from 'react';
import {
  Select,
  Slider,
  Switch,
  Divider,
  Space,
  Typography,
  Tooltip,
  Spin,
  message,
  Row,
  Col,
  Radio,
  Checkbox
} from 'antd';
import { InfoCircleOutlined, StarOutlined } from '@ant-design/icons';
import { themesAPI } from '../services/themes';
import { FilterConfig, DimensionOperator, DimensionOperators } from '../types/newsPresets';
import type { DimensionValue } from '../types/themes';

const { Text, Title } = Typography;
const { Option } = Select;

// 维度操作符切换组件
interface OperatorToggleProps {
  value?: DimensionOperator;
  onChange?: (op: DimensionOperator) => void;
  disabled?: boolean;
}

const OperatorToggle: React.FC<OperatorToggleProps> = ({ value = 'OR', onChange, disabled }) => {
  return (
    <Radio.Group 
      size="small" 
      value={value} 
      onChange={(e) => onChange?.(e.target.value)}
      disabled={disabled}
      style={{ marginLeft: 8 }}
    >
      <Radio.Button value="OR" style={{ fontSize: '11px', padding: '0 8px' }}>
        <Tooltip title="满足任一条件即可 (OR)">OR</Tooltip>
      </Radio.Button>
      <Radio.Button value="AND" style={{ fontSize: '11px', padding: '0 8px' }}>
        <Tooltip title="必须同时满足所有条件 (AND)">AND</Tooltip>
      </Radio.Button>
    </Radio.Group>
  );
};

interface FilterConfigEditorProps {
  value?: FilterConfig;
  onChange?: (config: FilterConfig) => void;
  form?: any; // Ant Design Form 实例
}

const FilterConfigEditor: React.FC<FilterConfigEditorProps> = ({
  value = {},
  onChange,
  form
}) => {
  const [loading, setLoading] = useState(false);
  const [dimensionOptions, setDimensionOptions] = useState<{
    newsValues: DimensionValue[];
    categories: DimensionValue[];
    eventTypes: DimensionValue[];
    impactScopes: DimensionValue[];
    urgencyLevels: DimensionValue[];
    relatedSectors: DimensionValue[];
    relatedCountries: DimensionValue[];
    sourceTypes: DimensionValue[];
    schedulerSources: DimensionValue[];
  }>({
    newsValues: [],
    categories: [],
    eventTypes: [],
    impactScopes: [],
    urgencyLevels: [],
    relatedSectors: [],
    relatedCountries: [],
    sourceTypes: [],
    schedulerSources: []
  });

  // 🔥 新增：Watchlist groups（从用户的target_tracking_config获取）
  const [watchlistGroups, setWatchlistGroups] = useState<string[]>([]);

  // 加载所有维度的选项
  useEffect(() => {
    loadAllDimensions();
  }, []);

  const loadAllDimensions = async () => {
    setLoading(true);
    try {
      const [
        newsValues,
        categories,
        eventTypes,
        impactScopes,
        urgencyLevels,
        relatedSectors,
        relatedCountries,
        sourceTypes
      ] = await Promise.all([
        themesAPI.getDimensionValues('news_value'),
        themesAPI.getDimensionValues('primary_theme'),
        themesAPI.getDimensionValues('event_type'),
        themesAPI.getDimensionValues('impact_scope'),
        themesAPI.getDimensionValues('urgency'),
        themesAPI.getDimensionValues('related_sectors'),
        themesAPI.getDimensionValues('related_countries'),
        themesAPI.getDimensionValues('source_type')
      ]);

      setDimensionOptions({
        // News Value - 如果API返回为空，使用默认值
        newsValues: (newsValues.values && newsValues.values.length > 0) 
          ? newsValues.values.map(item => ({
              ...item,
              display_name: item.value === 'fact' ? 'Fact' : item.value === 'opinion' ? 'Opinion' : item.display_name
            }))
          : [
              { value: 'fact', display_name: 'Fact', news_count: 0 },
              { value: 'opinion', display_name: 'Opinion', news_count: 0 }
            ],
        categories: categories.values || [],
        eventTypes: eventTypes.values || [],
        impactScopes: impactScopes.values || [],
        urgencyLevels: urgencyLevels.values || [],
        relatedSectors: relatedSectors.values || [],
        relatedCountries: relatedCountries.values || [],
        sourceTypes: sourceTypes.values || [],
        // Scheduler sources 是固定的几个值，直接硬编码
        schedulerSources: [
          { value: 'eodhd_news', display_name: 'EODHD News', news_count: 0 },
          { value: 'watchlist_news', display_name: 'Watchlist News', news_count: 0 },
          { value: 'cctv_news', display_name: 'CCTV News', news_count: 0 }
        ]
      });
    } catch (error) {
      console.error('Failed to load dimension options:', error);
      message.error('Failed to load filter options');
    } finally {
      setLoading(false);
    }
  };

  // 触发外部 onChange
  const triggerChange = (changedValue: Partial<FilterConfig>) => {
    const newConfig = { ...value, ...changedValue };
    onChange?.(newConfig);
  };

  // 更新维度操作符
  const updateDimensionOperator = (dimension: keyof DimensionOperators, operator: DimensionOperator) => {
    const newOperators: DimensionOperators = {
      ...value.dimension_operators,
      [dimension]: operator
    };
    triggerChange({ dimension_operators: newOperators });
  };

  // 获取维度操作符
  const getDimensionOperator = (dimension: keyof DimensionOperators): DimensionOperator => {
    return value.dimension_operators?.[dimension] || 'OR';
  };

  if (loading) {
    return (
      <div style={{ textAlign: 'center', padding: '40px' }}>
        <Spin tip="Loading filter options..." />
      </div>
    );
  }

  return (
    <div style={{ maxHeight: '500px', overflowY: 'auto', paddingRight: '8px' }}>
      <style>{`
        .ant-select-multiple .ant-select-selector {
          min-height: 40px !important;
          padding: 4px 8px !important;
        }
        .ant-select-multiple .ant-select-selection-item {
          height: 28px !important;
          line-height: 26px !important;
          margin-top: 2px !important;
          margin-bottom: 2px !important;
          padding: 0 8px !important;
          font-size: 14px !important;
        }
        .ant-select-multiple .ant-select-selection-placeholder {
          line-height: 32px !important;
        }
      `}</style>
      <Space direction="vertical" size="large" style={{ width: '100%' }}>
        {/* 🔥 新增：新闻价值类型 News Value */}
        <div>
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
            <Title level={5} style={{ margin: 0 }}>
              News Value
              <Tooltip title="Filter by news type: Fact (factual news) or Opinion (opinion articles)">
                <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
              </Tooltip>
            </Title>
            {(value.news_value?.length || 0) > 1 && (
              <OperatorToggle
                value={getDimensionOperator('news_value')}
                onChange={(op) => updateDimensionOperator('news_value', op)}
              />
            )}
          </div>
          <Select
            mode="multiple"
            style={{ width: '100%', minHeight: '40px' }}
            placeholder="Select news value types (Fact/Opinion)"
            value={value.news_value || []}
            onChange={(val) => triggerChange({ news_value: val.length > 0 ? val : undefined })}
            maxTagCount="responsive"
            dropdownStyle={{ maxHeight: '300px', overflowY: 'auto' }}
          >
            {dimensionOptions.newsValues.map(item => (
              <Option key={item.value} value={item.value}>
                {item.display_name}
              </Option>
            ))}
          </Select>
          <Text type="secondary" style={{ fontSize: '11px', display: 'block', marginTop: 4 }}>
            Fact: Company announcements, earnings, M&A, analyst ratings | Opinion: Investment advice, predictions
          </Text>
        </div>

        {/* 分类 Category */}
        <div>
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
            <Title level={5} style={{ margin: 0 }}>
              Categories
              <Tooltip title="Select news categories to filter">
                <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
              </Tooltip>
            </Title>
            {(value.categories?.length || 0) > 1 && (
              <OperatorToggle
                value={getDimensionOperator('categories')}
                onChange={(op) => updateDimensionOperator('categories', op)}
              />
            )}
          </div>
          <Select
            mode="multiple"
            style={{ width: '100%', minHeight: '40px' }}
            placeholder="Select categories"
            value={value.categories || []}
            onChange={(val) => triggerChange({ categories: val.length > 0 ? val : undefined })}
            maxTagCount="responsive"
            dropdownStyle={{ maxHeight: '300px', overflowY: 'auto' }}
          >
            {dimensionOptions.categories.map(item => (
              <Option key={item.value} value={item.value}>
                {item.display_name}
              </Option>
            ))}
          </Select>
        </div>

        {/* 事件类型 Event Types */}
        <div>
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
            <Title level={5} style={{ margin: 0 }}>
              Event Types
              <Tooltip title="Select specific event types">
                <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
              </Tooltip>
            </Title>
            {(value.event_types?.length || 0) > 1 && (
              <OperatorToggle
                value={getDimensionOperator('event_types')}
                onChange={(op) => updateDimensionOperator('event_types', op)}
              />
            )}
          </div>
          <Select
            mode="multiple"
            style={{ width: '100%', minHeight: '40px' }}
            placeholder="Select event types"
            value={value.event_types || []}
            onChange={(val) => triggerChange({ event_types: val.length > 0 ? val : undefined })}
            maxTagCount="responsive"
            dropdownStyle={{ maxHeight: '300px', overflowY: 'auto' }}
          >
            {dimensionOptions.eventTypes.map(item => (
              <Option key={item.value} value={item.value}>
                {item.display_name}
              </Option>
            ))}
          </Select>
        </div>

        {/* 影响范围 Impact Scopes */}
        <div>
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
            <Title level={5} style={{ margin: 0 }}>
              Impact Scopes
              <Tooltip title="Select impact scopes">
                <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
              </Tooltip>
            </Title>
            {(value.impact_scopes?.length || 0) > 1 && (
              <OperatorToggle
                value={getDimensionOperator('impact_scopes')}
                onChange={(op) => updateDimensionOperator('impact_scopes', op)}
              />
            )}
          </div>
          <Select
            mode="multiple"
            style={{ width: '100%', minHeight: '40px' }}
            placeholder="Select impact scopes"
            value={value.impact_scopes || []}
            onChange={(val) => triggerChange({ impact_scopes: val.length > 0 ? val : undefined })}
            maxTagCount="responsive"
            dropdownStyle={{ maxHeight: '300px', overflowY: 'auto' }}
          >
            {dimensionOptions.impactScopes.map(item => (
              <Option key={item.value} value={item.value}>
                {item.display_name}
              </Option>
            ))}
          </Select>
        </div>

        {/* 紧急程度 Urgency */}
        <div>
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
            <Title level={5} style={{ margin: 0 }}>
              Urgency Levels
              <Tooltip title="Select urgency levels">
                <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
              </Tooltip>
            </Title>
            {(value.urgency?.length || 0) > 1 && (
              <OperatorToggle
                value={getDimensionOperator('urgency')}
                onChange={(op) => updateDimensionOperator('urgency', op)}
              />
            )}
          </div>
          <Select
            mode="multiple"
            style={{ width: '100%', minHeight: '40px' }}
            placeholder="Select urgency levels"
            value={value.urgency || []}
            onChange={(val) => triggerChange({ urgency: val.length > 0 ? val : undefined })}
            maxTagCount="responsive"
            dropdownStyle={{ maxHeight: '300px', overflowY: 'auto' }}
          >
            {dimensionOptions.urgencyLevels.map(item => (
              <Option key={item.value} value={item.value}>
                {item.display_name}
              </Option>
            ))}
          </Select>
        </div>

        {/* 相关行业 Related Sectors */}
        <div>
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
            <Title level={5} style={{ margin: 0 }}>
              Related Sectors
              <Tooltip title="Select related sectors">
                <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
              </Tooltip>
            </Title>
            {(value.related_sectors?.length || 0) > 1 && (
              <OperatorToggle
                value={getDimensionOperator('related_sectors')}
                onChange={(op) => updateDimensionOperator('related_sectors', op)}
              />
            )}
          </div>
          <Select
            mode="multiple"
            style={{ width: '100%', minHeight: '40px' }}
            placeholder="Select sectors"
            value={value.related_sectors || []}
            onChange={(val) => triggerChange({ related_sectors: val.length > 0 ? val : undefined })}
            maxTagCount="responsive"
            showSearch
            dropdownStyle={{ maxHeight: '300px', overflowY: 'auto' }}
          >
            {dimensionOptions.relatedSectors.map(item => (
              <Option key={item.value} value={item.value}>
                {item.display_name}
              </Option>
            ))}
          </Select>
        </div>

        {/* 相关国家 Related Countries */}
        <div>
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
            <Title level={5} style={{ margin: 0 }}>
              Related Countries
              <Tooltip title="Select related countries">
                <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
              </Tooltip>
            </Title>
            {(value.related_countries?.length || 0) > 1 && (
              <OperatorToggle
                value={getDimensionOperator('related_countries')}
                onChange={(op) => updateDimensionOperator('related_countries', op)}
              />
            )}
          </div>
          <Select
            mode="multiple"
            style={{ width: '100%', minHeight: '40px' }}
            placeholder="Select countries"
            value={value.related_countries || []}
            onChange={(val) => triggerChange({ related_countries: val.length > 0 ? val : undefined })}
            maxTagCount="responsive"
            showSearch
            dropdownStyle={{ maxHeight: '300px', overflowY: 'auto' }}
          >
            {dimensionOptions.relatedCountries.map(item => (
              <Option key={item.value} value={item.value}>
                {item.display_name}
              </Option>
            ))}
          </Select>
        </div>

        <Divider />

        {/* 来源类型 Source Types */}
        <div>
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
            <Title level={5} style={{ margin: 0 }}>
              Source Types
              <Tooltip title="Select news source types">
                <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
              </Tooltip>
            </Title>
            {(value.source_types?.length || 0) > 1 && (
              <OperatorToggle
                value={getDimensionOperator('source_types')}
                onChange={(op) => updateDimensionOperator('source_types', op)}
              />
            )}
          </div>
          <Select
            mode="multiple"
            style={{ width: '100%', minHeight: '40px' }}
            placeholder="Select source types"
            value={value.source_types || []}
            onChange={(val) => triggerChange({ source_types: val.length > 0 ? val : undefined })}
            maxTagCount="responsive"
            dropdownStyle={{ maxHeight: '300px', overflowY: 'auto' }}
          >
            {dimensionOptions.sourceTypes.map(item => (
              <Option key={item.value} value={item.value}>
                {item.display_name}
              </Option>
            ))}
          </Select>
        </div>

        {/* 调度来源 Scheduler Sources */}
        <div>
          <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
            <Title level={5} style={{ margin: 0 }}>
              Scheduler Sources
              <Tooltip title="Select scheduler sources">
                <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
              </Tooltip>
            </Title>
            {(value.scheduler_sources?.length || 0) > 1 && (
              <OperatorToggle
                value={getDimensionOperator('scheduler_sources')}
                onChange={(op) => updateDimensionOperator('scheduler_sources', op)}
              />
            )}
          </div>
          <Select
            mode="multiple"
            style={{ width: '100%', minHeight: '40px' }}
            placeholder="Select scheduler sources"
            value={value.scheduler_sources || []}
            onChange={(val) => triggerChange({ scheduler_sources: val.length > 0 ? val : undefined })}
            maxTagCount="responsive"
            dropdownStyle={{ maxHeight: '300px', overflowY: 'auto' }}
          >
            {dimensionOptions.schedulerSources.map((item: any) => (
              <Option key={item.value} value={item.value}>
                {item.display_name}
              </Option>
            ))}
          </Select>
        </div>

        <Divider />

        {/* 🔥 新增：Ticker筛选 */}
        <div>
          <Title level={5}>
            <StarOutlined style={{ marginRight: 8, color: '#1890ff' }} />
            Ticker Filter
            <Tooltip title="Filter news by specific tickers or your watchlist">
              <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
            </Tooltip>
          </Title>

          <Space direction="vertical" style={{ width: '100%' }} size="middle">
            {/* Use Watchlist 开关 */}
            <div>
              <Checkbox
                checked={value.use_watchlist || false}
                onChange={(e) => {
                  const useWatchlist = e.target.checked;
                  triggerChange({ 
                    use_watchlist: useWatchlist || undefined,
                    // 如果取消watchlist，同时清空watchlist_groups
                    watchlist_groups: useWatchlist ? value.watchlist_groups : undefined
                  });
                }}
              >
                <Text strong>Use my watchlist tickers</Text>
              </Checkbox>
              <div style={{ marginLeft: 24, marginTop: 4 }}>
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  Filter news related to tickers in your watchlist
                </Text>
              </div>
            </div>

            {/* Watchlist Groups 选择（当use_watchlist启用时） */}
            {value.use_watchlist && (
              <div style={{ marginLeft: 24 }}>
                <Text type="secondary" style={{ fontSize: '13px', display: 'block', marginBottom: 8 }}>
                  Select specific watchlist groups (optional):
                </Text>
                <Select
                  mode="multiple"
                  style={{ width: '100%', minHeight: '40px' }}
                  placeholder="All groups (leave empty for all)"
                  value={value.watchlist_groups || []}
                  onChange={(val) => triggerChange({ watchlist_groups: val.length > 0 ? val : undefined })}
                  maxTagCount="responsive"
                  allowClear
                >
                  {/* 常见的watchlist groups */}
                  <Option value="Technology">Technology</Option>
                  <Option value="Healthcare">Healthcare</Option>
                  <Option value="Financial">Financial</Option>
                  <Option value="Consumer Cyclical">Consumer Cyclical</Option>
                  <Option value="Consumer Defensive">Consumer Defensive</Option>
                  <Option value="Industrials">Industrials</Option>
                  <Option value="Energy">Energy</Option>
                  <Option value="Utilities">Utilities</Option>
                  <Option value="Real Estate">Real Estate</Option>
                  <Option value="Communication Services">Communication Services</Option>
                  <Option value="Basic Materials">Basic Materials</Option>
                </Select>
              </div>
            )}

            {/* 分割线 */}
            {value.use_watchlist && (
              <Divider style={{ margin: '12px 0' }}>
                <Text type="secondary" style={{ fontSize: '12px' }}>OR</Text>
              </Divider>
            )}

            {/* Manual Ticker Input */}
            <div>
              <Text type="secondary" style={{ fontSize: '13px', display: 'block', marginBottom: 8 }}>
                Or enter specific tickers manually:
              </Text>
              <Select
                mode="tags"
                style={{ width: '100%', minHeight: '40px' }}
                placeholder="e.g., AAPL.US, TSLA.US, 0700.HK"
                value={value.ticker_symbols || []}
                onChange={(val) => triggerChange({ ticker_symbols: val.length > 0 ? val : undefined })}
                maxTagCount="responsive"
                tokenSeparators={[',', ' ']}
                allowClear
              >
                {/* Tags模式允许用户输入任意值 */}
              </Select>
              <Text type="secondary" style={{ fontSize: '11px', display: 'block', marginTop: 4 }}>
                💡 Tip: Separate multiple tickers with comma or space
              </Text>
            </div>

            {/* Filter Mode：Include or Exclude */}
            {(value.use_watchlist || (value.ticker_symbols && value.ticker_symbols.length > 0)) && (
              <div>
                <Text type="secondary" style={{ fontSize: '13px', display: 'block', marginBottom: 8 }}>
                  Filter mode:
                </Text>
                <Radio.Group
                  value={value.ticker_filter_mode || 'include'}
                  onChange={(e) => triggerChange({ ticker_filter_mode: e.target.value })}
                >
                  <Radio value="include">
                    <Text>Include</Text>
                    <Text type="secondary" style={{ fontSize: '11px', marginLeft: 8 }}>
                      (Only show news from these tickers)
                    </Text>
                  </Radio>
                  <Radio value="exclude" style={{ marginTop: 8 }}>
                    <Text>Exclude</Text>
                    <Text type="secondary" style={{ fontSize: '11px', marginLeft: 8 }}>
                      (Hide news from these tickers)
                    </Text>
                  </Radio>
                </Radio.Group>
              </div>
            )}
          </Space>
        </div>

        <Divider />

        {/* 高级选项 */}
        <div>
          <Title level={5}>
            Min Confidence
            <Tooltip title="Minimum confidence score (0.0 - 1.0)">
              <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
            </Tooltip>
          </Title>
          <Slider
            min={0}
            max={1}
            step={0.1}
            value={value.min_confidence || 0}
            onChange={(val) => triggerChange({ min_confidence: val > 0 ? val : undefined })}
            marks={{
              0: '0',
              0.5: '0.5',
              1: '1.0'
            }}
          />
          <Text type="secondary" style={{ fontSize: '12px' }}>
            Current: {value.min_confidence || 0}
          </Text>
        </div>
      </Space>
    </div>
  );
};

export default FilterConfigEditor;

