/**
 * 模板选择器组件
 * 用于选择Prompt模板
 */

import React, { useState, useEffect } from 'react';
import { Select, Spin, Tag, Tooltip, Space, Typography } from 'antd';
import { InfoCircleOutlined, RobotOutlined } from '@ant-design/icons';
import promptTemplateService from '../services/promptTemplateService';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

const { Text } = Typography;
const { Option } = Select;

const TEMPLATE_BOX_THEME = {
  light: { boxBg: '#f5f5f5', iconColor: '#1890ff', text: undefined as string | undefined },
  dark: { boxBg: '#212936', iconColor: '#60a5fa', text: 'rgba(255,255,255,0.85)' },
} as const;

interface TemplateSelectorProps {
  categoryCode: string;
  value?: number;
  onChange?: (value: number, template?: any) => void;
  placeholder?: string;
  disabled?: boolean;
  allowClear?: boolean;
  style?: React.CSSProperties;
  /** 选择模板后是否显示模板配置信息，默认 true */
  showConfigWhenSelected?: boolean;
}

interface Template {
  id: number;
  template_code: string;
  template_name: string;
  description?: string;
  recommended_provider?: string;
  recommended_model?: string;
  temperature: number;
  max_tokens: number;
  is_default: boolean;
  usage_count: number;
}

const TemplateSelector: React.FC<TemplateSelectorProps> = ({
  categoryCode,
  value,
  onChange,
  placeholder = 'Select Template',
  disabled = false,
  allowClear = true,
  style = {},
  showConfigWhenSelected = true,
}) => {
  const { themeMode } = useDashboardSettings();
  const boxTheme = TEMPLATE_BOX_THEME[themeMode];
  const [templates, setTemplates] = useState<Template[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    loadTemplates();
  }, [categoryCode]);

  const loadTemplates = async () => {
    if (!categoryCode) return;

    setLoading(true);
    try {
      const response = await promptTemplateService.listTemplates({
        category_id: undefined,
        scope: undefined,
        search: undefined,
        active_only: true,
      });

      // 过滤出指定category的模板
      const categoryTemplates = response.filter((t: any) => 
        t.category_code === categoryCode || 
        (t.category_name && t.category_name.includes('文件分析'))
      );

      setTemplates(categoryTemplates);

      // 如果有默认模板且当前未选择，自动选择默认模板
      if (!value && categoryTemplates.length > 0) {
        const defaultTemplate = categoryTemplates.find((t: Template) => t.is_default);
        if (defaultTemplate && onChange) {
          onChange(defaultTemplate.id, defaultTemplate);
        }
      }
    } catch (error) {
      console.error('加载模板列表失败:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleChange = (templateId: number) => {
    const selectedTemplate = templates.find((t) => t.id === templateId);
    if (onChange) {
      onChange(templateId, selectedTemplate);
    }
  };

  const renderTemplateOption = (template: Template) => {
    return (
      <Option key={template.id} value={template.id} label={template.template_name}>
        <Space direction="vertical" size={0} style={{ width: '100%' }}>
          <Space>
            <Text strong style={{ fontSize: '12px' }}>{template.template_name}</Text>
            {template.is_default && <Tag color="blue" style={{ fontSize: '9px' }}>默认</Tag>}
            {template.usage_count > 0 && (
              <Tag color="green" style={{ fontSize: '9px' }}>{template.usage_count}次使用</Tag>
            )}
          </Space>
          <Space size={4}>
            {template.recommended_provider && (
              <Tag color="purple" style={{ fontSize: '9px', margin: 0 }}>
                <RobotOutlined /> {template.recommended_provider}
              </Tag>
            )}
            {template.recommended_model && (
              <Tag color="cyan" style={{ fontSize: '9px', margin: 0 }}>
                {template.recommended_model}
              </Tag>
            )}
            <Tag style={{ fontSize: '9px', margin: 0 }}>
              Temp: {template.temperature}
            </Tag>
            <Tag style={{ fontSize: '9px', margin: 0 }}>
              MaxTokens: {template.max_tokens}
            </Tag>
          </Space>
        </Space>
      </Option>
    );
  };

  const getSelectedTemplateInfo = () => {
    if (!value) return null;
    const template = templates.find((t) => t.id === value);
    if (!template) return null;

    return (
      <div style={{
        marginTop: 8,
        padding: 8,
        background: boxTheme.boxBg,
        borderRadius: 4,
        color: boxTheme.text,
      }}>
        <Space direction="vertical" size={4} style={{ width: '100%' }}>
          <Space>
            <InfoCircleOutlined style={{ color: boxTheme.iconColor }} />
            <Text strong style={boxTheme.text ? { color: boxTheme.text } : undefined}>模板配置：</Text>
          </Space>
          <Text type="secondary" style={{ fontSize: '12px', color: boxTheme.text }}>
            提供商: {template.recommended_provider || '未指定'} |
            模型: {template.recommended_model || '未指定'} |
            温度: {template.temperature} |
            最大Token: {template.max_tokens}
          </Text>
        </Space>
      </div>
    );
  };

  return (
    <div>
      <Select
        value={value}
        onChange={handleChange}
        placeholder={placeholder}
        disabled={disabled}
        loading={loading}
        allowClear={allowClear}
        style={{ width: '100%', ...style }}
        dropdownStyle={{ fontSize: '12px' }}
        optionLabelProp="label"
        notFoundContent={loading ? <Spin size="small" /> : '暂无可用模板'}
      >
        {templates.map((template) => renderTemplateOption(template))}
      </Select>
      {value && showConfigWhenSelected && getSelectedTemplateInfo()}
    </div>
  );
};

export default TemplateSelector;

