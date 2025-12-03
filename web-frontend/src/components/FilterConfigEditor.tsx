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
  Col
} from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import { themesAPI } from '../services/themes';
import { FilterConfig } from '../types/newsPresets';
import type { DimensionValue } from '../types/themes';

const { Text, Title } = Typography;
const { Option } = Select;

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
    categories: DimensionValue[];
    eventTypes: DimensionValue[];
    impactScopes: DimensionValue[];
    urgencyLevels: DimensionValue[];
    relatedSectors: DimensionValue[];
    relatedCountries: DimensionValue[];
    sourceTypes: DimensionValue[];
    schedulerSources: DimensionValue[];
  }>({
    categories: [],
    eventTypes: [],
    impactScopes: [],
    urgencyLevels: [],
    relatedSectors: [],
    relatedCountries: [],
    sourceTypes: [],
    schedulerSources: []
  });

  // 加载所有维度的选项
  useEffect(() => {
    loadAllDimensions();
  }, []);

  const loadAllDimensions = async () => {
    setLoading(true);
    try {
      const [
        categories,
        eventTypes,
        impactScopes,
        urgencyLevels,
        relatedSectors,
        relatedCountries,
        sourceTypes
      ] = await Promise.all([
        themesAPI.getDimensionValues('primary_theme'),
        themesAPI.getDimensionValues('event_type'),
        themesAPI.getDimensionValues('impact_scope'),
        themesAPI.getDimensionValues('urgency'),
        themesAPI.getDimensionValues('related_sectors'),
        themesAPI.getDimensionValues('related_countries'),
        themesAPI.getDimensionValues('source_type')
      ]);

      setDimensionOptions({
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
        {/* 分类 Category */}
        <div>
          <Title level={5}>
            Categories
            <Tooltip title="Select news categories to filter">
              <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
            </Tooltip>
          </Title>
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
          <Title level={5}>
            Event Types
            <Tooltip title="Select specific event types">
              <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
            </Tooltip>
          </Title>
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
          <Title level={5}>
            Impact Scopes
            <Tooltip title="Select impact scopes">
              <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
            </Tooltip>
          </Title>
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
          <Title level={5}>
            Urgency Levels
            <Tooltip title="Select urgency levels">
              <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
            </Tooltip>
          </Title>
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
          <Title level={5}>
            Related Sectors
            <Tooltip title="Select related sectors">
              <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
            </Tooltip>
          </Title>
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
          <Title level={5}>
            Related Countries
            <Tooltip title="Select related countries">
              <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
            </Tooltip>
          </Title>
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
          <Title level={5}>
            Source Types
            <Tooltip title="Select news source types">
              <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
            </Tooltip>
          </Title>
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
          <Title level={5}>
            Scheduler Sources
            <Tooltip title="Select scheduler sources">
              <InfoCircleOutlined style={{ marginLeft: 8, fontSize: 14, color: '#999' }} />
            </Tooltip>
          </Title>
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

