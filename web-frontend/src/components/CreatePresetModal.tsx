import React, { useState, useEffect } from 'react';
import {
  Modal,
  Form,
  Input,
  Checkbox,
  Space,
  message,
  Tabs
} from 'antd';
import { SaveOutlined } from '@ant-design/icons';
import { NewsFilterPreset, FilterConfig } from '../types/newsPresets';
import { ThemeFilterState } from '../types/themes';
import FilterConfigEditor from './FilterConfigEditor';

const { TextArea } = Input;
const { TabPane } = Tabs;

interface CreatePresetModalProps {
  visible: boolean;
  onClose: () => void;
  onSave: (data: {
    preset_name: string;
    description?: string;
    filter_config: FilterConfig;
    scope_type?: 'global' | 'symbol_specific';
    symbol_code?: string;
    is_default?: boolean;
    is_favorite?: boolean;
  }) => Promise<void>;
  initialFilters?: ThemeFilterState;
  editingPreset?: NewsFilterPreset | null;
}

const CreatePresetModal: React.FC<CreatePresetModalProps> = ({
  visible,
  onClose,
  onSave,
  initialFilters,
  editingPreset
}) => {
  const [form] = Form.useForm();
  const [loading, setLoading] = useState(false);
  const [filterConfig, setFilterConfig] = useState<FilterConfig>({});

  const isEditing = !!editingPreset;

  useEffect(() => {
    if (visible) {
      if (editingPreset) {
        // 编辑模式：填充现有数据
        form.setFieldsValue({
          preset_name: editingPreset.preset_name,
          description: editingPreset.description,
          is_default: editingPreset.is_default,
          is_favorite: editingPreset.is_favorite
        });
        // 设置编辑模式的筛选配置
        setFilterConfig(editingPreset.filter_config || {});
      } else if (initialFilters) {
        // 创建模式：从当前筛选条件创建
        form.setFieldsValue({
          preset_name: '',
          description: '',
          is_default: false,
          is_favorite: false
        });
        // 从 ThemeFilterState 转换为 FilterConfig 作为初始值
        const initialConfig: FilterConfig = {
          categories: initialFilters.categories.length > 0 ? initialFilters.categories : undefined,
          event_types: initialFilters.eventTypes.length > 0 ? initialFilters.eventTypes : undefined,
          impact_scopes: initialFilters.impactScopes.length > 0 ? initialFilters.impactScopes : undefined,
          urgency: initialFilters.urgencyLevels.length > 0 ? initialFilters.urgencyLevels : undefined,
          related_sectors: initialFilters.relatedSectors.length > 0 ? initialFilters.relatedSectors : undefined,
          related_countries: initialFilters.relatedCountries.length > 0 ? initialFilters.relatedCountries : undefined,
          source_types: initialFilters.sourceTypes.length > 0 ? initialFilters.sourceTypes : undefined,
          scheduler_sources: initialFilters.schedulerSources.length > 0 ? initialFilters.schedulerSources : undefined,
          min_confidence: initialFilters.minConfidence,
          only_primary: initialFilters.onlyPrimary,
          market: initialFilters.market || undefined
        };
        setFilterConfig(initialConfig);
      }
    } else {
      // 关闭时重置
      form.resetFields();
      setFilterConfig({});
    }
  }, [visible, editingPreset, initialFilters, form]);

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields();
      setLoading(true);

      // 使用用户配置的 filterConfig
      await onSave({
        preset_name: values.preset_name,
        description: values.description,
        filter_config: filterConfig,
        scope_type: 'global', // 默认为全局预设
        is_default: values.is_default || false,
        is_favorite: values.is_favorite || false
      });

      // 成功后关闭对话框
      onClose();
      
      // 延迟显示成功消息，避免被关闭动画覆盖
      setTimeout(() => {
        message.success(isEditing ? 'Preset updated successfully' : 'Preset created successfully');
      }, 100);
    } catch (error: any) {
      if (error.errorFields) {
        // 表单验证错误
        message.error('Please fill in all required fields');
      } else {
        console.error('Failed to save preset:', error);
        message.error(`Failed to save preset: ${error.message || 'Unknown error'}`);
      }
    } finally {
      setLoading(false);
    }
  };

  // 生成筛选条件预览（用于Basic Info Tab）
  const getFilterPreview = () => {
    const items: string[] = [];
    
    if (filterConfig.categories && filterConfig.categories.length > 0) {
      items.push(`Categories: ${filterConfig.categories.length}`);
    }
    if (filterConfig.event_types && filterConfig.event_types.length > 0) {
      items.push(`Event Types: ${filterConfig.event_types.length}`);
    }
    if (filterConfig.related_sectors && filterConfig.related_sectors.length > 0) {
      items.push(`Sectors: ${filterConfig.related_sectors.length}`);
    }
    if (filterConfig.related_countries && filterConfig.related_countries.length > 0) {
      items.push(`Countries: ${filterConfig.related_countries.length}`);
    }
    if (filterConfig.urgency && filterConfig.urgency.length > 0) {
      items.push(`Urgency: ${filterConfig.urgency.length}`);
    }
    if (filterConfig.impact_scopes && filterConfig.impact_scopes.length > 0) {
      items.push(`Impact: ${filterConfig.impact_scopes.length}`);
    }
    if (filterConfig.source_types && filterConfig.source_types.length > 0) {
      items.push(`Sources: ${filterConfig.source_types.length}`);
    }
    if (filterConfig.scheduler_sources && filterConfig.scheduler_sources.length > 0) {
      items.push(`Scheduler: ${filterConfig.scheduler_sources.length}`);
    }
    if (filterConfig.only_primary) {
      items.push('Primary only');
    }
    if (filterConfig.min_confidence && filterConfig.min_confidence > 0) {
      items.push(`Min confidence: ${(filterConfig.min_confidence * 100).toFixed(0)}%`);
    }

    return items.length > 0 ? items.join(' • ') : 'No filters configured';
  };

  return (
    <Modal
      title={
        <Space>
          <SaveOutlined />
          <span>{isEditing ? 'Edit Preset' : 'Save as Preset'}</span>
        </Space>
      }
      open={visible}
      onCancel={onClose}
      onOk={handleSubmit}
      confirmLoading={loading}
      width={800}
      okText={isEditing ? 'Update' : 'Save'}
      cancelText="Cancel"
    >
      <Tabs defaultActiveKey="basic">
        <TabPane tab="Basic Info" key="basic">
          <Form
            form={form}
            layout="vertical"
            style={{ marginTop: '16px' }}
          >
            <Form.Item
              name="preset_name"
              label="Preset Name"
              rules={[
                { required: true, message: 'Please enter preset name' },
                { max: 100, message: 'Name too long (max 100 characters)' }
              ]}
            >
              <Input placeholder="e.g., 中美矛盾, 财报追踪, etc." />
            </Form.Item>

            <Form.Item
              name="description"
              label="Description (Optional)"
              rules={[
                { max: 500, message: 'Description too long (max 500 characters)' }
              ]}
            >
              <TextArea
                rows={2}
                placeholder="Brief description of this preset..."
              />
            </Form.Item>

            {/* 筛选条件预览 */}
            <div
              style={{
                padding: '12px',
                backgroundColor: '#f5f5f5',
                borderRadius: '6px',
                marginBottom: '16px'
              }}
            >
              <div style={{ fontSize: '11px', fontWeight: 600, marginBottom: '8px', color: '#666' }}>
                Filter Configuration Summary:
              </div>
              <div style={{ fontSize: '11px', color: '#666', lineHeight: '1.6' }}>
                {getFilterPreview()}
              </div>
              <div style={{ fontSize: '10px', color: '#999', marginTop: '8px' }}>
                💡 Switch to "Filter Configuration" tab to customize filters
              </div>
            </div>

            <Space direction="vertical" style={{ width: '100%' }}>
              <Form.Item name="is_favorite" valuePropName="checked" style={{ marginBottom: 0 }}>
                <Checkbox>Add to Favorites</Checkbox>
              </Form.Item>
              
              <Form.Item name="is_default" valuePropName="checked" style={{ marginBottom: 0 }}>
                <Checkbox>Set as Default Preset</Checkbox>
              </Form.Item>
            </Space>
          </Form>
        </TabPane>

        <TabPane tab="Filter Configuration" key="filter">
          <FilterConfigEditor
            value={filterConfig}
            onChange={setFilterConfig}
          />
        </TabPane>
      </Tabs>
    </Modal>
  );
};

export default CreatePresetModal;

