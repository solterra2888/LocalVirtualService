/**
 * 模板变量编辑器组件
 * 根据模板定义动态生成表单
 */

import React, { useState, useEffect } from 'react';
import {
  Form,
  Input,
  InputNumber,
  Switch,
  Select,
  Space,
  Typography,
  Alert,
  Spin,
  Collapse,
} from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import promptTemplateService from '../services/promptTemplateService';

const { Text, Paragraph } = Typography;
const { TextArea } = Input;
const { Panel } = Collapse;

/**
 * 系统自动填充的变量列表
 * 这些变量不应该在UI中显示给用户配置，因为它们会由系统自动填充
 */
const AUTO_FILLED_VARIABLES = [
  'file_content',  // 文件内容会自动从上传的文件中提取
] as const;

interface TemplateVariablesEditorProps {
  templateId: number;
  values?: Record<string, any>;
  onChange?: (values: Record<string, any>) => void;
  disabled?: boolean;
}

interface VariableDefinition {
  type: string;
  required: boolean;
  default?: any;
  description?: string;
  options?: any[];
}

const TemplateVariablesEditor: React.FC<TemplateVariablesEditorProps> = ({
  templateId,
  values = {},
  onChange,
  disabled = false,
}) => {
  const [template, setTemplate] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [form] = Form.useForm();

  useEffect(() => {
    if (templateId) {
      loadTemplate();
    }
  }, [templateId]);

  useEffect(() => {
    if (template && template.variables) {
      // 设置表单初始值
      const initialValues: Record<string, any> = {};
      Object.entries(template.variables).forEach(([key, varDef]: [string, any]) => {
        if (values[key] !== undefined) {
          initialValues[key] = values[key];
        } else if (varDef.default !== undefined) {
          initialValues[key] = varDef.default;
        }
      });
      form.setFieldsValue(initialValues);

      // 通知父组件初始值
      if (onChange && Object.keys(initialValues).length > 0) {
        onChange(initialValues);
      }
    }
  }, [template]);

  const loadTemplate = async () => {
    setLoading(true);
    try {
      const response = await promptTemplateService.getTemplate(templateId);
      setTemplate(response);
    } catch (error) {
      console.error('加载模板详情失败:', error);
    } finally {
      setLoading(false);
    }
  };

  const handleValuesChange = (changedValues: any, allValues: any) => {
    if (onChange) {
      onChange(allValues);
    }
  };

  const renderVariableInput = (
    varName: string,
    varDef: VariableDefinition
  ) => {
    const commonProps = {
      disabled,
      placeholder: varDef.description || `请输入${varName}`,
    };

    switch (varDef.type) {
      case 'integer':
        return <InputNumber {...commonProps} style={{ width: '100%' }} />;

      case 'float':
        return (
          <InputNumber
            {...commonProps}
            step={0.1}
            style={{ width: '100%' }}
          />
        );

      case 'boolean':
        return <Switch disabled={disabled} />;

      case 'array':
        if (varDef.options && varDef.options.length > 0) {
          return (
            <Select
              {...commonProps}
              mode="multiple"
              options={varDef.options.map((opt) => ({
                label: opt,
                value: opt,
              }))}
            />
          );
        }
        return (
          <Select
            {...commonProps}
            mode="tags"
            tokenSeparators={[',']}
          />
        );

      case 'string':
      default:
        // 如果有options，显示为下拉选择
        if (varDef.options && varDef.options.length > 0) {
          return (
            <Select
              {...commonProps}
              options={varDef.options.map((opt) => ({
                label: opt,
                value: opt,
              }))}
            />
          );
        }

        // 如果默认值很长，使用TextArea
        const defaultLength = varDef.default
          ? String(varDef.default).length
          : 0;
        if (defaultLength > 100) {
          return (
            <TextArea
              {...commonProps}
              rows={4}
              showCount
              maxLength={2000}
            />
          );
        }

        return <Input {...commonProps} />;
    }
  };

  if (loading) {
    return (
      <div style={{ textAlign: 'center', padding: '20px' }}>
        <Spin tip="加载模板配置..." />
      </div>
    );
  }

  if (!template || !template.variables || Object.keys(template.variables).length === 0) {
    return (
      <Alert
        message="此模板无需配置变量"
        description="该模板不需要额外的变量配置，可以直接使用。"
        type="info"
        showIcon
      />
    );
  }

  // 分组：必填字段和可选字段
  const requiredVars: [string, VariableDefinition][] = [];
  const optionalVars: [string, VariableDefinition][] = [];

  Object.entries(template.variables).forEach(([key, varDef]: [string, any]) => {
    // 跳过系统自动填充的变量
    if (AUTO_FILLED_VARIABLES.includes(key as any)) return;

    if (varDef.required) {
      requiredVars.push([key, varDef]);
    } else {
      optionalVars.push([key, varDef]);
    }
  });

  return (
    <div style={{ marginTop: 16 }}>
      <Form
        form={form}
        layout="vertical"
        onValuesChange={handleValuesChange}
        disabled={disabled}
      >
        {/* 必填字段 */}
        {requiredVars.length > 0 && (
          <div style={{ marginBottom: 16 }}>
            <Text strong style={{ fontSize: '14px', color: '#ff4d4f' }}>
              <InfoCircleOutlined /> 必填参数
            </Text>
            <div style={{ marginTop: 8 }}>
              {requiredVars.map(([varName, varDef]) => (
                <Form.Item
                  key={varName}
                  name={varName}
                  label={
                    <Space>
                      <Text>{varName}</Text>
                      {varDef.description && (
                        <Text type="secondary" style={{ fontSize: '12px' }}>
                          ({varDef.description})
                        </Text>
                      )}
                    </Space>
                  }
                  rules={[
                    {
                      required: true,
                      message: `请输入${varName}`,
                    },
                  ]}
                  extra={
                    varDef.default !== undefined && (
                      <Text type="secondary" style={{ fontSize: '12px' }}>
                        默认值: {String(varDef.default)}
                      </Text>
                    )
                  }
                >
                  {renderVariableInput(varName, varDef)}
                </Form.Item>
              ))}
            </div>
          </div>
        )}

        {/* 可选字段 */}
        {optionalVars.length > 0 && (
          <Collapse
            ghost
            defaultActiveKey={optionalVars.length <= 3 ? ['optional'] : []}
          >
            <Panel
              header={
                <Text strong style={{ fontSize: '14px' }}>
                  <InfoCircleOutlined /> Optional Parameters ({optionalVars.length})
                </Text>
              }
              key="optional"
            >
              {optionalVars.map(([varName, varDef]) => (
                <Form.Item
                  key={varName}
                  name={varName}
                  label={
                    <Space>
                      <Text>{varName}</Text>
                      {varDef.description && (
                        <Text type="secondary" style={{ fontSize: '12px' }}>
                          ({varDef.description})
                        </Text>
                      )}
                    </Space>
                  }
                  extra={
                    varDef.default !== undefined && (
                      <Text type="secondary" style={{ fontSize: '12px' }}>
                        默认值: {String(varDef.default)}
                      </Text>
                    )
                  }
                >
                  {renderVariableInput(varName, varDef)}
                </Form.Item>
              ))}
            </Panel>
          </Collapse>
        )}
      </Form>

      {template.example_input && Object.keys(template.example_input).length > 0 && (
        <Alert
          message="示例输入"
          description={
            <pre style={{ margin: 0, fontSize: '12px' }}>
              {JSON.stringify(template.example_input, null, 2)}
            </pre>
          }
          type="info"
          style={{ marginTop: 16 }}
        />
      )}
    </div>
  );
};

export default TemplateVariablesEditor;

