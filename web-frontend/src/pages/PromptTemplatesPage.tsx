import React, { useState, useEffect, useCallback } from 'react';
import { 
  Card, 
  Button, 
  Input, 
  Select, 
  message, 
  Row, 
  Col, 
  Typography, 
  Space,
  Divider,
  Tag,
  Spin,
  Modal,
  Form,
  InputNumber,
  Switch,
  Tooltip,
  Empty,
  Drawer,
  Alert,
  Statistic,
  Collapse
} from 'antd';
import { 
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  ExperimentOutlined,
  StarFilled,
  FileTextOutlined,
  DownloadOutlined,
  SearchOutlined,
  ThunderboltOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined
} from '@ant-design/icons';
import promptTemplateService, {
  PromptCategory,
  PromptTemplateDetail,
  TemplateCreateData,
  TestTemplateResponse
} from '../services/promptTemplateService';
import VariableHelper from '../components/VariableHelper';

const { Title, Paragraph, Text } = Typography;
const { Option } = Select;
const { TextArea } = Input;
const { Panel } = Collapse;

const PromptTemplatesPage: React.FC = () => {
  // State management
  const [categories, setCategories] = useState<PromptCategory[]>([]);
  const [templates, setTemplates] = useState<PromptTemplateDetail[]>([]);
  const [selectedCategory, setSelectedCategory] = useState<number | null>(null);
  const [selectedTemplate, setSelectedTemplate] = useState<PromptTemplateDetail | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [searchText, setSearchText] = useState<string>('');
  const [scopeFilter, setScopeFilter] = useState<string>('all');

  // Modal states
  const [isEditorVisible, setIsEditorVisible] = useState<boolean>(false);
  const [isTestVisible, setIsTestVisible] = useState<boolean>(false);
  const [editingTemplate, setEditingTemplate] = useState<PromptTemplateDetail | null>(null);
  const [testing, setTesting] = useState<boolean>(false);
  const [testResult, setTestResult] = useState<TestTemplateResponse | null>(null);
  const [currentEditingCategoryId, setCurrentEditingCategoryId] = useState<number | undefined>();

  const [form] = Form.useForm();
  const [testForm] = Form.useForm();

  // Load template list
  const loadTemplates = useCallback(async () => {
    try {
      const params: any = {};
      if (selectedCategory) params.category_id = selectedCategory;
      if (searchText) params.search = searchText;
      if (scopeFilter !== 'all') params.scope = scopeFilter;

      const templatesData = await promptTemplateService.listTemplates(params);
      // 获取完整的模板详情
      const detailedTemplates = await Promise.all(
        templatesData.map(async (template) => {
          try {
            return await promptTemplateService.getTemplate(template.id);
          } catch (error) {
            console.warn(`Failed to load details for template ${template.id}:`, error);
            // 返回基本信息，转换为 PromptTemplateDetail 类型
            return {
              ...template,
              user_prompt_template: '',
              variables: {},
              output_format: { type: 'text' },
              tags: [],
              example_input: {},
              example_output: '',
              is_public: false,
              parent_template_id: undefined
            } as PromptTemplateDetail;
          }
        })
      );
      setTemplates(detailedTemplates);
    } catch (error: any) {
      message.error('Failed to load templates: ' + (error.message || 'Unknown error'));
    }
  }, [selectedCategory, searchText, scopeFilter]);

  // Load data
  const loadData = useCallback(async () => {
    try {
      setLoading(true);
      const [categoriesData, templatesData] = await Promise.all([
        promptTemplateService.getCategories(),
        promptTemplateService.listTemplates()
      ]);
      setCategories(categoriesData);
      
      // 获取完整的模板详情
      const detailedTemplates = await Promise.all(
        templatesData.map(async (template) => {
          try {
            return await promptTemplateService.getTemplate(template.id);
          } catch (error) {
            console.warn(`Failed to load details for template ${template.id}:`, error);
            // 返回基本信息，转换为 PromptTemplateDetail 类型
            return {
              ...template,
              user_prompt_template: '',
              variables: {},
              output_format: { type: 'text' },
              tags: [],
              example_input: {},
              example_output: '',
              is_public: false,
              parent_template_id: undefined
            } as PromptTemplateDetail;
          }
        })
      );
      setTemplates(detailedTemplates);
    } catch (error: any) {
      message.error('Failed to load data: ' + (error.message || 'Unknown error'));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData();
  }, [loadData]);

  useEffect(() => {
    if (selectedCategory !== null || searchText || scopeFilter !== 'all') {
      loadTemplates();
    }
  }, [selectedCategory, searchText, scopeFilter, loadTemplates]);

  // Create/Edit template
  const handleCreateTemplate = () => {
    setEditingTemplate(null);
    setCurrentEditingCategoryId(undefined);
    form.resetFields();
    setIsEditorVisible(true);
  };

  const handleEditTemplate = async (template: PromptTemplateDetail) => {
    try {
      const detail = await promptTemplateService.getTemplate(template.id);
      setEditingTemplate(detail);
      setCurrentEditingCategoryId(detail.category_id);
      form.setFieldsValue({
        template_name: detail.template_name,
        template_code: detail.template_code,
        category_id: detail.category_id,
        description: detail.description,
        system_prompt: detail.system_prompt,
        user_prompt_template: detail.user_prompt_template,
        recommended_provider: detail.recommended_provider,
        recommended_model: detail.recommended_model,
        temperature: detail.temperature,
        max_tokens: detail.max_tokens,
        scope: detail.scope,
        is_public: detail.is_public,
        tags: detail.tags
      });
      setIsEditorVisible(true);
    } catch (error: any) {
      message.error('Failed to load template details: ' + (error.message || 'Unknown error'));
    }
  };

  const handleSaveTemplate = async (values: any) => {
    try {
      if (editingTemplate) {
        // Update
        await promptTemplateService.updateTemplate(editingTemplate.id, values);
        message.success('Template updated successfully');
      } else {
        // Create
        await promptTemplateService.createTemplate(values as TemplateCreateData);
        message.success('Template created successfully');
      }
      setIsEditorVisible(false);
      loadTemplates();
    } catch (error: any) {
      message.error('Save failed: ' + (error.message || 'Unknown error'));
    }
  };

  // Delete template
  const handleDeleteTemplate = (template: PromptTemplateDetail) => {
    Modal.confirm({
      title: 'Confirm Delete',
      content: `Are you sure you want to delete template "${template.template_name}"?`,
      okText: 'Delete',
      okType: 'danger',
      cancelText: 'Cancel',
      onOk: async () => {
        try {
          await promptTemplateService.deleteTemplate(template.id);
          message.success('Template deleted successfully');
          loadTemplates();
        } catch (error: any) {
          message.error('Delete failed: ' + (error.message || 'Unknown error'));
        }
      }
    });
  };

  // Test template
  const handleTestTemplate = async (template: PromptTemplateDetail) => {
    try {
      const detail = await promptTemplateService.getTemplate(template.id);
      setSelectedTemplate(detail);
      
      // Set default values for test form
      const defaultValues: any = {};
      Object.entries(detail.variables || {}).forEach(([key, varDef]) => {
        if (varDef.default !== undefined) {
          defaultValues[key] = varDef.default;
        }
      });
      testForm.setFieldsValue(defaultValues);
      
      setTestResult(null);
      setIsTestVisible(true);
    } catch (error: any) {
      message.error('Failed to load template details: ' + (error.message || 'Unknown error'));
    }
  };

  const handleRunTest = async (values: any) => {
    if (!selectedTemplate) return;
    
    try {
      setTesting(true);
      const result = await promptTemplateService.testTemplate({
        template_id: selectedTemplate.id,
        variables: values
      });
      setTestResult(result);
      
      if (result.success) {
        message.success('Test successful');
      } else {
        message.error('Test failed: ' + result.error);
      }
    } catch (error: any) {
      message.error('Test failed: ' + (error.message || 'Unknown error'));
    } finally {
      setTesting(false);
    }
  };

  // Export template
  const handleExportTemplate = async (template: PromptTemplateDetail) => {
    try {
      await promptTemplateService.downloadTemplateExport(template.id, template.template_code);
      message.success('Export successful');
    } catch (error: any) {
      message.error('Export failed: ' + (error.message || 'Unknown error'));
    }
  };

  // Handle variable insertion
  const handleVariableInsert = (variableText: string) => {
    const userPromptField = form.getFieldInstance('user_prompt_template');
    if (userPromptField) {
      const textarea = userPromptField.resizableTextArea?.textArea;
      if (textarea) {
        const start = textarea.selectionStart;
        const end = textarea.selectionEnd;
        const currentValue = form.getFieldValue('user_prompt_template') || '';
        const newValue = currentValue.slice(0, start) + variableText + currentValue.slice(end);
        
        form.setFieldValue('user_prompt_template', newValue);
        
        // Focus back to textarea
        setTimeout(() => {
          textarea.focus();
          textarea.setSelectionRange(start + variableText.length, start + variableText.length);
        }, 0);
      }
    }
  };

  // Render category list
  const renderCategories = () => (
    <Card 
      title="Template Categories" 
      size="small"
      extra={
        <Button 
          type="text" 
          size="small"
          onClick={() => {
            setSelectedCategory(null);
            loadTemplates();
          }}
        >
          All
        </Button>
      }
    >
      <Space direction="vertical" style={{ width: '100%' }}>
        {categories.map(category => (
          <Card
            key={category.id}
            size="small"
            hoverable
            className={selectedCategory === category.id ? 'selected-category' : ''}
            onClick={() => setSelectedCategory(category.id)}
            style={{
              cursor: 'pointer',
              borderColor: selectedCategory === category.id ? '#1890ff' : undefined
            }}
          >
            <Space>
              <span style={{ fontSize: '20px' }}>{category.icon}</span>
              <div>
                <Text strong>{category.category_name}</Text>
                <br />
                <Text type="secondary" style={{ fontSize: '12px' }}>
                  {category.description}
                </Text>
              </div>
            </Space>
          </Card>
        ))}
      </Space>
    </Card>
  );

  // Render template list
  const renderTemplateList = () => {
    const filteredTemplates = templates.filter(t => {
      if (searchText && !t.template_name.toLowerCase().includes(searchText.toLowerCase()) && 
          !t.description?.toLowerCase().includes(searchText.toLowerCase())) {
        return false;
      }
      return true;
    });

    return (
      <div style={{ marginBottom: 16 }}>
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          {filteredTemplates.map(template => (
            <Card
              key={template.id}
              size="small"
              hoverable
              actions={[
                <Tooltip title="Edit">
                  <EditOutlined onClick={() => handleEditTemplate(template)} />
                </Tooltip>,
                <Tooltip title="Test">
                  <ExperimentOutlined onClick={() => handleTestTemplate(template)} />
                </Tooltip>,
                <Tooltip title="Export">
                  <DownloadOutlined onClick={() => handleExportTemplate(template)} />
                </Tooltip>,
                <Tooltip title="Delete">
                  <DeleteOutlined onClick={() => handleDeleteTemplate(template)} />
                </Tooltip>
              ]}
            >
              <Row justify="space-between" align="top">
                <Col span={18}>
                  <Space direction="vertical" size="small">
                    <Space>
                      <Text strong style={{ fontSize: '16px' }}>{template.template_name}</Text>
                      {template.is_default && <StarFilled style={{ color: '#faad14' }} />}
                      <Tag color={
                        template.scope === 'system' ? 'blue' :
                        template.scope === 'user' ? 'green' : 'orange'
                      }>
                        {template.scope === 'system' ? 'System' : 
                         template.scope === 'user' ? 'Private' : 'Shared'}
                      </Tag>
                    </Space>
                    <Text type="secondary" style={{ fontSize: '12px' }}>{template.description}</Text>
                    <Space size="small">
                      <Text type="secondary" style={{ fontSize: '12px' }}>
                        Template Code: {template.template_code}
                      </Text>
                      <Divider type="vertical" />
                      <Text type="secondary" style={{ fontSize: '12px' }}>
                        Category: {template.category_name}
                      </Text>
                    </Space>
                  </Space>
                </Col>
                <Col span={6} style={{ textAlign: 'right' }}>
                  <Space direction="vertical" size="small" align="end">
                    <Statistic 
                      title="Usage Count" 
                      value={template.usage_count} 
                      valueStyle={{ fontSize: '16px' }}
                    />
                    {template.success_rate !== undefined && template.success_rate !== null && (
                      <Tag color={template.success_rate >= 90 ? 'success' : 'warning'}>
                        Success Rate: {template.success_rate.toFixed(1)}%
                      </Tag>
                    )}
                    {template.recommended_model && (
                      <Tag>{template.recommended_model}</Tag>
                    )}
                  </Space>
                </Col>
              </Row>
            </Card>
          ))}
          {filteredTemplates.length === 0 && (
            <Empty description="No templates available" />
          )}
        </Space>
      </div>
    );
  };

  // Render editor
  const renderEditor = () => (
    <Modal
      title={editingTemplate ? 'Edit Template' : 'Create Template'}
      open={isEditorVisible}
      onCancel={() => setIsEditorVisible(false)}
      width={1200}
      footer={null}
    >
      <Row gutter={16}>
        <Col span={16}>
          <Form
            form={form}
            layout="vertical"
            onFinish={handleSaveTemplate}
            size="small"
          >
        <Row gutter={12}>
          <Col span={12}>
            <Form.Item
              label={<Text style={{ fontSize: '12px' }}>Template Name</Text>}
              name="template_name"
              rules={[{ required: true, message: 'Please enter template name' }]}
            >
              <Input 
                placeholder="e.g.: News Summary Generation" 
                style={{ fontSize: '12px' }}
                size="small"
              />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label={<Text style={{ fontSize: '12px' }}>Template Code</Text>}
              name="template_code"
              rules={[{ required: true, message: 'Please enter template code' }]}
            >
              <Input 
                placeholder="e.g.: news_summary_v1" 
                disabled={!!editingTemplate}
                style={{ fontSize: '12px' }}
                size="small"
              />
            </Form.Item>
          </Col>
        </Row>

        <Row gutter={12}>
          <Col span={12}>
            <Form.Item
              label={<Text style={{ fontSize: '12px' }}>Category</Text>}
              name="category_id"
              rules={[{ required: true, message: 'Please select a category' }]}
            >
              <Select 
                placeholder="Select category"
                size="small"
                style={{ fontSize: '12px' }}
                onChange={(value) => setCurrentEditingCategoryId(value)}
              >
                {categories.map(cat => (
                  <Option key={cat.id} value={cat.id}>
                    {cat.icon} {cat.category_name}
                  </Option>
                ))}
              </Select>
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label={<Text style={{ fontSize: '12px' }}>Scope</Text>}
              name="scope"
              initialValue="user"
            >
              <Select size="small" style={{ fontSize: '12px' }}>
                <Option value="user">Private</Option>
                <Option value="shared">Shared</Option>
                <Option value="system">System</Option>
              </Select>
            </Form.Item>
          </Col>
        </Row>

        <Form.Item
          label={<Text style={{ fontSize: '12px' }}>Description</Text>}
          name="description"
        >
          <TextArea 
            rows={6} 
            placeholder="Briefly describe the purpose of this template"
            style={{ fontSize: '12px' }}
            size="small"
          />
        </Form.Item>

        <Form.Item
          label={<Text style={{ fontSize: '12px' }}>System Prompt (Optional)</Text>}
          name="system_prompt"
        >
          <TextArea 
            rows={6} 
            placeholder="Define AI's role and behavior, e.g.: You are a professional financial analyst..."
            style={{ fontSize: '12px' }}
            size="small"
          />
        </Form.Item>

        <Form.Item
          label={<Text style={{ fontSize: '12px' }}>User Prompt Template</Text>}
          name="user_prompt_template"
          rules={[{ required: true, message: 'Please enter user prompt template' }]}
          extra="Use {{variable_name}} format to define variables"
        >
          <TextArea 
            rows={6} 
            placeholder="Please analyze the following news:\n\nTitle: {{title}}\nContent: {{content}}"
            style={{ fontSize: '12px' }}
            size="small"
          />
        </Form.Item>

        <Divider style={{ fontSize: '12px', margin: '12px 0' }}>Recommended Configuration</Divider>

        <Row gutter={12}>
          <Col span={12}>
            <Form.Item
              label={<Text style={{ fontSize: '12px' }}>Recommended Provider</Text>}
              name="recommended_provider"
            >
              <Select 
                placeholder="Select LLM provider" 
                allowClear
                size="small"
                style={{ fontSize: '12px' }}
              >
                <Option value="qwen">Qwen</Option>
                <Option value="doubao">Doubao</Option>
                <Option value="glm">GLM</Option>
              </Select>
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label={<Text style={{ fontSize: '12px' }}>Recommended Model</Text>}
              name="recommended_model"
            >
              <Input 
                placeholder="e.g.: qwen-plus"
                style={{ fontSize: '12px' }}
                size="small"
              />
            </Form.Item>
          </Col>
        </Row>

        <Row gutter={12}>
          <Col span={12}>
            <Form.Item
              label={<Text style={{ fontSize: '12px' }}>Temperature</Text>}
              name="temperature"
              initialValue={0.7}
            >
              <InputNumber 
                min={0} 
                max={2} 
                step={0.1} 
                style={{ width: '100%', fontSize: '12px' }}
                size="small"
              />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label={<Text style={{ fontSize: '12px' }}>Max Tokens</Text>}
              name="max_tokens"
              initialValue={2000}
            >
              <InputNumber 
                min={100} 
                max={10000} 
                step={100} 
                style={{ width: '100%', fontSize: '12px' }}
                size="small"
              />
            </Form.Item>
          </Col>
        </Row>

        <Form.Item
          label={<Text style={{ fontSize: '12px' }}>Tags</Text>}
          name="tags"
        >
          <Select 
            mode="tags" 
            placeholder="Add tags, press Enter to confirm"
            size="small"
            style={{ fontSize: '12px' }}
          />
        </Form.Item>

        <Form.Item
          label={<Text style={{ fontSize: '12px' }}>Public Sharing</Text>}
          name="is_public"
          valuePropName="checked"
          initialValue={false}
        >
          <Switch size="small" />
        </Form.Item>

        <Form.Item style={{ marginBottom: 0 }}>
          <Space size="small">
            <Button type="primary" htmlType="submit" size="small">
              Save
            </Button>
            <Button onClick={() => setIsEditorVisible(false)} size="small">
              Cancel
            </Button>
          </Space>
        </Form.Item>
          </Form>
        </Col>
        
        <Col span={8}>
          <VariableHelper
            categories={categories}
            templates={templates}
            onVariableInsert={handleVariableInsert}
            currentCategoryId={currentEditingCategoryId}
          />
        </Col>
      </Row>
    </Modal>
  );

  // Render test panel
  const renderTestPanel = () => {
    if (!selectedTemplate) return null;

    return (
      <Drawer
        title={`Test Template: ${selectedTemplate.template_name}`}
        placement="right"
        width={720}
        open={isTestVisible}
        onClose={() => setIsTestVisible(false)}
      >
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          <Alert
            message="Test Instructions"
            description="Fill in test variable values, click run test to view LLM response effects"
            type="info"
            showIcon
          />

          <Card title="Template Information" size="small">
            <Paragraph>
              <Text strong>Template Code:</Text> {selectedTemplate.template_code}
            </Paragraph>
            <Paragraph>
              <Text strong>Recommended Provider:</Text> {selectedTemplate.recommended_provider || 'Not set'}
            </Paragraph>
            <Paragraph>
              <Text strong>Recommended Model:</Text> {selectedTemplate.recommended_model || 'Not set'}
            </Paragraph>
          </Card>

          <Form
            form={testForm}
            layout="vertical"
            onFinish={handleRunTest}
          >
            <Card title="Test Variables" size="small">
              {Object.entries(selectedTemplate.variables || {}).map(([varName, varDef]) => (
                <Form.Item
                  key={varName}
                  label={
                    <Space>
                      <Text>{varName}</Text>
                      {varDef.required && <Tag color="red">Required</Tag>}
                      <Tag>{varDef.type}</Tag>
                    </Space>
                  }
                  name={varName}
                  rules={[
                    { required: varDef.required, message: `${varName} is required` }
                  ]}
                  extra={varDef.description}
                >
                  {varDef.type === 'string' ? (
                    <TextArea rows={3} placeholder={`Enter ${varName}`} />
                  ) : varDef.type === 'integer' || varDef.type === 'float' ? (
                    <InputNumber style={{ width: '100%' }} />
                  ) : varDef.type === 'boolean' ? (
                    <Switch />
                  ) : (
                    <Input placeholder={`Enter ${varName}`} />
                  )}
                </Form.Item>
              ))}
            </Card>

            <Form.Item>
              <Button 
                type="primary" 
                htmlType="submit" 
                icon={<ThunderboltOutlined />}
                loading={testing}
                block
              >
                Run Test
              </Button>
            </Form.Item>
          </Form>

          {testResult && (
            <Space direction="vertical" style={{ width: '100%' }} size="middle">
              <Card 
                title={
                  <Space>
                    <Text>Test Result</Text>
                    {testResult.success ? (
                      <CheckCircleOutlined style={{ color: '#52c41a' }} />
                    ) : (
                      <CloseCircleOutlined style={{ color: '#ff4d4f' }} />
                    )}
                  </Space>
                }
                size="small"
              >
                {testResult.success && (
                  <>
                    <Row gutter={16}>
                      <Col span={8}>
                        <Statistic 
                          title="Tokens Used" 
                          value={testResult.tokens_used || 0} 
                        />
                      </Col>
                      <Col span={8}>
                        <Statistic 
                          title="Cost" 
                          value={testResult.cost || 0} 
                          precision={4}
                          prefix="¥"
                        />
                      </Col>
                      <Col span={8}>
                        <Statistic 
                          title="Response Time" 
                          value={testResult.response_time || 0} 
                          precision={2}
                          suffix="s"
                        />
                      </Col>
                    </Row>
                  </>
                )}
                {testResult.error && (
                  <Alert message="Error" description={testResult.error} type="error" />
                )}
              </Card>

              {testResult.rendered_prompt && (
                <Collapse defaultActiveKey={['1']}>
                  <Panel header="Rendered Prompt" key="1">
                    {testResult.rendered_prompt.system_prompt && (
                      <>
                        <Text strong>System Prompt:</Text>
                        <Paragraph>
                          <pre style={{ 
                            background: '#f5f5f5', 
                            padding: '12px', 
                            borderRadius: '4px',
                            whiteSpace: 'pre-wrap'
                          }}>
                            {testResult.rendered_prompt.system_prompt}
                          </pre>
                        </Paragraph>
                      </>
                    )}
                    <Text strong>User Prompt:</Text>
                    <Paragraph>
                      <pre style={{ 
                        background: '#f5f5f5', 
                        padding: '12px', 
                        borderRadius: '4px',
                        whiteSpace: 'pre-wrap'
                      }}>
                        {testResult.rendered_prompt.user_prompt}
                      </pre>
                    </Paragraph>
                  </Panel>
                </Collapse>
              )}

              {testResult.llm_response && (
                <Card title="LLM Response" size="small">
                  <Paragraph>
                    <pre style={{ 
                      background: '#f5f5f5', 
                      padding: '12px', 
                      borderRadius: '4px',
                      whiteSpace: 'pre-wrap'
                    }}>
                      {testResult.llm_response}
                    </pre>
                  </Paragraph>
                </Card>
              )}

              {testResult.parsed_output && (
                <Card title="Parsed Output" size="small">
                  <Paragraph>
                    <pre style={{ 
                      background: '#f5f5f5', 
                      padding: '12px', 
                      borderRadius: '4px',
                      whiteSpace: 'pre-wrap'
                    }}>
                      {JSON.stringify(testResult.parsed_output, null, 2)}
                    </pre>
                  </Paragraph>
                </Card>
              )}
            </Space>
          )}
        </Space>
      </Drawer>
    );
  };

  return (
    <div style={{ padding: '24px' }}>
      <Title level={2}>
        <FileTextOutlined /> Prompt Template Management
      </Title>
      <Paragraph type="secondary">
        Manage and optimize your LLM Prompt templates with support for variables, version control, and online testing
      </Paragraph>

      <Spin spinning={loading}>
        <Row gutter={[16, 16]}>
          {/* Left: Category list */}
          <Col xs={24} md={6}>
            {renderCategories()}
          </Col>

          {/* Right: Template list and operations */}
          <Col xs={24} md={18}>
            <Card>
              <Space direction="vertical" style={{ width: '100%' }} size="middle">
                {/* Toolbar */}
                <Row gutter={[16, 16]} align="middle">
                  <Col flex="auto">
                    <Input
                      placeholder="Search template name or description..."
                      prefix={<SearchOutlined />}
                      value={searchText}
                      onChange={e => setSearchText(e.target.value)}
                      allowClear
                    />
                  </Col>
                  <Col>
                    <Select
                      style={{ width: 120 }}
                      value={scopeFilter}
                      onChange={setScopeFilter}
                    >
                      <Option value="all">All Scopes</Option>
                      <Option value="system">System Templates</Option>
                      <Option value="user">My Templates</Option>
                      <Option value="shared">Shared Templates</Option>
                    </Select>
                  </Col>
                  <Col>
                    <Button 
                      type="primary" 
                      icon={<PlusOutlined />}
                      onClick={handleCreateTemplate}
                    >
                      Create Template
                    </Button>
                  </Col>
                </Row>

                {/* Template list */}
                {renderTemplateList()}
              </Space>
            </Card>
          </Col>
        </Row>
      </Spin>

      {/* Editor modal */}
      {renderEditor()}

      {/* Test panel */}
      {renderTestPanel()}

    </div>
  );
};

export default PromptTemplatesPage;

