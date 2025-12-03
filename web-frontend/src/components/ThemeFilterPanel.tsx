import React, { useState, useEffect } from 'react';
import {
  Card,
  Checkbox,
  Space,
  Button,
  Slider,
  DatePicker,
  Collapse,
  Spin,
  Empty,
  message,
  Tooltip
} from 'antd';
import {
  FilterOutlined,
  ClearOutlined,
  SearchOutlined,
  CalendarOutlined,
  InfoCircleOutlined,
  SaveOutlined
} from '@ant-design/icons';
import dayjs from 'dayjs';
import locale from 'antd/es/date-picker/locale/en_US';
import { themesAPI } from '../services/themes';
import type { DimensionValue, ThemeFilterState } from '../types/themes';

const { RangePicker } = DatePicker;
const { Panel } = Collapse;

interface ThemeFilterPanelProps {
  onFilterChange: (filters: ThemeFilterState) => void;
  onApply: (filters?: ThemeFilterState) => void;
  onSaveAsPreset?: () => void;  // 🔥 新增：保存为预设的回调
  loading?: boolean;
}

const ThemeFilterPanel: React.FC<ThemeFilterPanelProps> = ({
  onFilterChange,
  onApply,
  onSaveAsPreset,
  loading = false
}) => {
  const [filters, setFilters] = useState<ThemeFilterState>({
    categories: [],
    eventTypes: [],
    impactScopes: [],
    urgencyLevels: [],
    relatedSectors: [],
    relatedCountries: [],
    sourceTypes: [],
    schedulerSources: [],  // 🔥 新增
    minConfidence: 0.5,
    onlyPrimary: false,
    dateRange: null,
    market: null
  });

  // 维度数据
  const [dimensionData, setDimensionData] = useState<{
    categories: DimensionValue[];
    eventTypes: DimensionValue[];
    impactScopes: DimensionValue[];
    urgencyLevels: DimensionValue[];
    relatedSectors: DimensionValue[];
    relatedCountries: DimensionValue[];
    sourceTypes: DimensionValue[];
  }>({
    categories: [],
    eventTypes: [],
    impactScopes: [],
    urgencyLevels: [],
    relatedSectors: [],
    relatedCountries: [],
    sourceTypes: []
  });

  const [dimensionLoading, setDimensionLoading] = useState<{
    [key: string]: boolean;
  }>({});

  // 加载维度数据
  const loadDimensionValues = async (dimension: string, days: number = 30) => {
    setDimensionLoading(prev => ({ ...prev, [dimension]: true }));
    
    try {
      const response = await themesAPI.getDimensionValues(dimension, days);
      
      const dataKey = {
        'primary_theme': 'categories',
        'event_type': 'eventTypes',
        'impact_scope': 'impactScopes',
        'urgency': 'urgencyLevels',
        'related_sectors': 'relatedSectors',
        'related_countries': 'relatedCountries',
        'source_type': 'sourceTypes'
      }[dimension] as keyof typeof dimensionData;
      
      if (dataKey) {
        setDimensionData(prev => ({
          ...prev,
          [dataKey]: response.values
        }));
      }
    } catch (error) {
      console.error(`Load ${dimension} dimension data failed:`, error);
      message.error(`Load data failed`);
    } finally {
      setDimensionLoading(prev => ({ ...prev, [dimension]: false }));
    }
  };

  // 初始加载数据
  useEffect(() => {
    loadDimensionValues('primary_theme');
    loadDimensionValues('event_type');
    loadDimensionValues('impact_scope');
    loadDimensionValues('urgency');
    loadDimensionValues('source_type');
    loadDimensionValues('related_sectors');
    loadDimensionValues('related_countries');
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // 更新筛选条件
  const updateFilter = (key: keyof ThemeFilterState, value: any) => {
    const newFilters = { ...filters, [key]: value };
    setFilters(newFilters);
    onFilterChange(newFilters);
  };

  // 切换复选框
  const toggleCheckbox = (key: keyof ThemeFilterState, value: string) => {
    const current = filters[key] as string[];
    const newValue = current.includes(value)
      ? current.filter(v => v !== value)
      : [...current, value];
    updateFilter(key, newValue);
  };

  // 清空筛选
  const handleClear = () => {
    const clearedFilters: ThemeFilterState = {
      categories: [],
      eventTypes: [],
      impactScopes: [],
      urgencyLevels: [],
      relatedSectors: [],
      relatedCountries: [],
      sourceTypes: [],
      schedulerSources: [],  // 🔥 新增
      minConfidence: 0.5,
      onlyPrimary: false,
      dateRange: null,
      market: null
    };
    setFilters(clearedFilters);
    onFilterChange(clearedFilters);
    // 清空后自动应用筛选，直接传递清空后的筛选条件
    onApply(clearedFilters);
  };

  // 渲染复选框列表
  const renderCheckboxList = (
    items: DimensionValue[],
    selectedValues: string[],
    filterKey: keyof ThemeFilterState,
    loading: boolean
  ) => {
    if (loading) {
      return <Spin size="small" />;
    }

    if (items.length === 0) {
      return <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="No data" />;
    }

    return (
      <Space wrap style={{ width: '100%' }}>
        {items.map(item => (
          <Checkbox
            key={item.value}
            checked={selectedValues.includes(item.value)}
            onChange={() => toggleCheckbox(filterKey, item.value)}
            style={{ fontSize: '11px' }}
          >
            <span style={{ fontSize: '11px' }}>{item.display_name}</span>
          </Checkbox>
        ))}
      </Space>
    );
  };

  return (
    <>
      <style>
        {`
          .ant-collapse-header,
          .ant-collapse-content,
          .ant-checkbox-wrapper,
          .ant-select-selection-item,
          .ant-select-selection-placeholder,
          .ant-slider-mark-text {
            font-size: 11px !important;
          }
        `}
      </style>
      <Card
        title={
          <Space>
            <FilterOutlined />
            <span>Theme Filter</span>
            <Tooltip 
              title={
                <div style={{ fontSize: '12px' }}>
                  <div style={{ fontWeight: 'bold', marginBottom: '8px' }}>筛选逻辑说明：</div>
                  <div style={{ marginBottom: '4px' }}>
                    <strong>同一维度内：</strong>OR关系（满足任一即可）
                  </div>
                  <div style={{ marginBottom: '8px', paddingLeft: '12px' }}>
                    例如：标准分类选择「金融、科技」→ 返回金融<strong>或</strong>科技类的新闻
                  </div>
                  <div style={{ marginBottom: '4px' }}>
                    <strong>不同维度间：</strong>AND关系（必须同时满足）
                  </div>
                  <div style={{ paddingLeft: '12px' }}>
                    例如：标准分类选择「金融」<strong>且</strong>事件类型选择「财报」→ 返回既是金融类<strong>又</strong>是财报的新闻
                  </div>
                </div>
              }
              overlayStyle={{ maxWidth: '400px' }}
            >
              <InfoCircleOutlined style={{ color: '#1890ff', cursor: 'help' }} />
            </Tooltip>
          </Space>
        }
        extra={
          <Space direction="vertical" size={4} style={{ width: '100%' }}>
            <Space>
              <Button size="small" icon={<ClearOutlined />} onClick={handleClear}>
                Clear
              </Button>
              <Button
                size="small"
                type="primary"
                icon={<SearchOutlined />}
                onClick={() => onApply()}
                loading={loading}
              >
                Apply
              </Button>
            </Space>
            {onSaveAsPreset && (
              <Button
                size="small"
                icon={<SaveOutlined />}
                onClick={onSaveAsPreset}
                style={{ width: '100%' }}
              >
                Save as Preset
              </Button>
            )}
          </Space>
        }
        style={{ height: '100%' }}
      >
      {/* 筛选选项 */}
      <Collapse defaultActiveKey={['categories', 'related_sectors', 'event_type']} ghost>
        <Panel header={<span style={{ fontSize: '11px', fontWeight: 'bold' }}>Main Theme</span>} key="categories">
          <div style={{ maxHeight: 300, overflowY: 'auto' }}>
            {renderCheckboxList(
              dimensionData.categories,
              filters.categories,
              'categories',
              dimensionLoading.primary_theme || false
            )}
          </div>
        </Panel>

        <Panel header={<span style={{ fontSize: '11px', fontWeight: 'bold' }}>Related Sectors</span>} key="related_sectors">
          <div style={{ maxHeight: 300, overflowY: 'auto' }}>
            {renderCheckboxList(
              dimensionData.relatedSectors,
              filters.relatedSectors,
              'relatedSectors',
              dimensionLoading.related_sectors || false
            )}
          </div>
        </Panel>
        
        <Panel header={<span style={{ fontSize: '11px', fontWeight: 'bold' }}>Event Type</span>} key="event_type">
          {renderCheckboxList(
            dimensionData.eventTypes,
            filters.eventTypes,
            'eventTypes',
            dimensionLoading.event_type || false
          )}
        </Panel>

        <Panel header={<span style={{ fontSize: '11px', fontWeight: 'bold' }}>Impact Scope</span>} key="impact_scope">
          {renderCheckboxList(
            dimensionData.impactScopes,
            filters.impactScopes,
            'impactScopes',
            dimensionLoading.impact_scope || false
          )}
        </Panel>

        <Panel header={<span style={{ fontSize: '11px', fontWeight: 'bold' }}>Urgency</span>} key="urgency">
          {renderCheckboxList(
            dimensionData.urgencyLevels,
            filters.urgencyLevels,
            'urgencyLevels',
            dimensionLoading.urgency || false
          )}
        </Panel>

        <Panel header={<span style={{ fontSize: '11px', fontWeight: 'bold' }}>Countries/Regions</span>} key="related_countries">
          <div style={{ maxHeight: 300, overflowY: 'auto' }}>
            {renderCheckboxList(
              dimensionData.relatedCountries,
              filters.relatedCountries,
              'relatedCountries',
              dimensionLoading.related_countries || false
            )}
          </div>
        </Panel>

        <Panel header={<span style={{ fontSize: '11px', fontWeight: 'bold' }}>Source Type</span>} key="source_type">
          {renderCheckboxList(
            dimensionData.sourceTypes,
            filters.sourceTypes,
            'sourceTypes',
            dimensionLoading.source_type || false
          )}
        </Panel>

        {/* 🔥 新增：Scheduler Source 筛选 */}
        <Panel header={<span style={{ fontSize: '11px', fontWeight: 'bold' }}>Scheduler Source</span>} key="scheduler_source">
          <Space wrap style={{ width: '100%' }}>
            <Checkbox
              checked={filters.schedulerSources.includes('eodhd_news')}
              onChange={() => toggleCheckbox('schedulerSources', 'eodhd_news')}
              style={{ fontSize: '11px' }}
            >
              <span style={{ fontSize: '11px' }}>EODHD News</span>
            </Checkbox>
            <Checkbox
              checked={filters.schedulerSources.includes('watchlist_news')}
              onChange={() => toggleCheckbox('schedulerSources', 'watchlist_news')}
              style={{ fontSize: '11px' }}
            >
              <span style={{ fontSize: '11px' }}>Watchlist News</span>
            </Checkbox>
            <Checkbox
              checked={filters.schedulerSources.includes('cctv_news')}
              onChange={() => toggleCheckbox('schedulerSources', 'cctv_news')}
              style={{ fontSize: '11px' }}
            >
              <span style={{ fontSize: '11px' }}>CCTV News</span>
            </Checkbox>
          </Space>
        </Panel>

        {/* 置信度 */}
        <Panel header={<span style={{ fontSize: '11px', fontWeight: 'bold' }}>Confidence</span>} key="confidence">
          <div>
            <div style={{ marginBottom: 8, fontSize: '11px' }}>
              Confidence: {(filters.minConfidence * 100).toFixed(0)}%+
            </div>
            <Slider
              min={0}
              max={1}
              step={0.1}
              value={filters.minConfidence}
              onChange={(value) => updateFilter('minConfidence', value)}
              marks={{
                0: '0%',
                0.5: '50%',
                0.7: '70%',
                1: '100%'
              }}
            />
          </div>
        </Panel>

        {/* 日期范围 */}
        <Panel header={<span style={{ fontSize: '11px', fontWeight: 'bold' }}><CalendarOutlined /> Date Range</span>} key="date_range">
          <RangePicker
            style={{ width: '100%' }}
            size="small"
            value={filters.dateRange}
            onChange={(dates) => updateFilter('dateRange', dates)}
            locale={locale}
            presets={[
              { label: 'Last 7 Days', value: [dayjs().subtract(7, 'days'), dayjs()] },
              { label: 'Last 30 Days', value: [dayjs().subtract(30, 'days'), dayjs()] },
              { label: 'Last 3 Months', value: [dayjs().subtract(3, 'months'), dayjs()] }
            ]}
          />
        </Panel>

        {/* 其他选项 */}
        <Panel header={<span style={{ fontSize: '11px', fontWeight: 'bold' }}>Other Options</span>} key="other_options">
          <Checkbox
            checked={filters.onlyPrimary}
            onChange={(e) => updateFilter('onlyPrimary', e.target.checked)}
            style={{ fontSize: '11px' }}
          >
            <span style={{ fontSize: '11px' }}>Only Show Primary Theme</span>
          </Checkbox>
        </Panel>
      </Collapse>
    </Card>
    </>
  );
};

export default ThemeFilterPanel;


