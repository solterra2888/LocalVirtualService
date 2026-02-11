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
  Collapse,
  ConfigProvider,
  theme as antdTheme
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
  CloseCircleOutlined,
  EyeOutlined
} from '@ant-design/icons';
import promptTemplateService, {
  PromptCategory,
  PromptTemplateDetail,
  TemplateCreateData,
  TestTemplateResponse
} from '../services/promptTemplateService';
import VariableHelper from '../components/VariableHelper';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

const { Title, Paragraph, Text } = Typography;

// Dark: card #1a2234 + Behance-style (rounded, soft shadow). Light: default.
const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    codeBlockBg: '#f5f5f5',
    selectedBorder: '#1890ff',
  },
  dark: {
    pageBg: '#1E202B',
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    text: '#FFFFFF',
    textSecondary: '#A0A3AD',
    codeBlockBg: '#1E202B',
    selectedBorder: '#2196F3',
    inputBg: '#1E202B',
    hoverBg: 'rgba(255, 255, 255, 0.06)',
  },
} as const;

// Behance-style: rounded corners, soft shadow, minimal border
const CARD_STYLE_BEHANCE = {
  borderRadius: 12,
  boxShadow: '0 4px 24px rgba(0, 0, 0, 0.12)',
  border: '1px solid rgba(255, 255, 255, 0.06)',
};
const CARD_STYLE_BEHANCE_DARK = {
  borderRadius: 12,
  boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)',
  border: '1px solid rgba(255, 255, 255, 0.06)',
};

// Create/Edit Template modal: unified control size and font
const EDITOR_FORM_CONTROL = {
  height: 32,
  fontSize: 14,
};
const EDITOR_FORM_TEXTAREA = { fontSize: 14 };

const { Option } = Select;
const { TextArea } = Input;
const { Panel } = Collapse;

// Provider-Model mapping configuration
const PROVIDER_MODEL_MAPPING: Record<string, string[]> = {
  'qwen': ['qwen-turbo', 'qwen-plus', 'qwen-max', 'qwen-max-longcontext',
           'qwen3-turbo', 'qwen3-plus', 'qwen3-max'],
  'doubao': ['doubao-seed-1-6-250615'],
  'glm': ['glm-4.6', 'glm-4', 'glm-4-flash']
};

const PromptTemplatesPage: React.FC = () => {
  const { user } = useAuthStore();
  const { themeMode } = useDashboardSettings();
  const isAdmin = user?.is_admin || false;
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  
  // State management
  const [categories, setCategories] = useState<PromptCategory[]>([]);
  const [templates, setTemplates] = useState<PromptTemplateDetail[]>([]);
  const [selectedCategory, setSelectedCategory] = useState<number | null>(null);
  const [selectedTemplate, setSelectedTemplate] = useState<PromptTemplateDetail | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [searchText, setSearchText] = useState<string>('');
  const [scopeFilter, setScopeFilter] = useState<string>('all');
  const [activeFilter, setActiveFilter] = useState<string>('active_only');
  const [selectedProvider, setSelectedProvider] = useState<string | undefined>();
  const [availableModels, setAvailableModels] = useState<string[]>([]);
  const [currentScope, setCurrentScope] = useState<string>('user');

  // Modal states
  const [isEditorVisible, setIsEditorVisible] = useState<boolean>(false);
  const [isTestVisible, setIsTestVisible] = useState<boolean>(false);
  const [isDeleteConfirmVisible, setIsDeleteConfirmVisible] = useState<boolean>(false);
  const [deletingTemplate, setDeletingTemplate] = useState<PromptTemplateDetail | null>(null);
  const [deleteConfirmText, setDeleteConfirmText] = useState<string>('');
  const [deleting, setDeleting] = useState<boolean>(false);
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

  // Handle provider change - update available models
  const handleProviderChange = (provider: string) => {
    setSelectedProvider(provider);
    const models = PROVIDER_MODEL_MAPPING[provider] || [];
    setAvailableModels(models);
    
    // Clear the model field when provider changes
    form.setFieldValue('recommended_model', undefined);
    
    // Auto-select first model if available
    if (models.length > 0) {
      form.setFieldValue('recommended_model', models[0]);
    }
  };

  // Handle scope change - control is_public field visibility
  const handleScopeChange = (scope: string) => {
    setCurrentScope(scope);
    
    // Auto-set is_public based on scope
    if (scope === 'user') {
      // Private: always not public
      form.setFieldValue('is_public', false);
    } else if (scope === 'system') {
      // System: always public (but field will be hidden)
      form.setFieldValue('is_public', true);
    }
    // For 'shared', let user decide via the Published toggle
  };

  // Create/Edit template
  const handleCreateTemplate = () => {
    setEditingTemplate(null);
    setCurrentEditingCategoryId(undefined);
    setSelectedProvider(undefined);
    setAvailableModels([]);
    setCurrentScope('user'); // Default to private
    form.resetFields();
    setIsEditorVisible(true);
  };

  const handleEditTemplate = async (template: PromptTemplateDetail) => {
    try {
      const detail = await promptTemplateService.getTemplate(template.id);
      const canEdit = canEditTemplate(detail);
      
      setEditingTemplate(detail);
      setCurrentEditingCategoryId(detail.category_id);
      
      // Initialize scope state
      setCurrentScope(detail.scope);
      
      // Initialize provider and models for cascade select
      if (detail.recommended_provider) {
        setSelectedProvider(detail.recommended_provider);
        const models = PROVIDER_MODEL_MAPPING[detail.recommended_provider] || [];
        setAvailableModels(models);
      }
      
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
        is_active: detail.is_active,
        tags: detail.tags
      });
      
      // Show warning if read-only
      if (!canEdit) {
        message.info('This is a read-only view. You cannot edit this template.');
      }
      
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
      // Extract error message from different error formats
      let errorMessage = 'Unknown error';
      
      if (error.response?.data?.detail) {
        // FastAPI HTTPException format
        errorMessage = error.response.data.detail;
      } else if (error.response?.data?.message) {
        // Custom error format
        errorMessage = error.response.data.message;
      } else if (error.message) {
        // Standard error message
        errorMessage = error.message;
      } else if (typeof error === 'string') {
        errorMessage = error;
      }
      
      message.error(`Save failed: ${errorMessage}`);
    }
  };

  // Delete template
  const handleDeleteTemplate = (template: PromptTemplateDetail) => {
    // System template requires special confirmation
    if (template.scope === 'system') {
      setDeletingTemplate(template);
      setDeleteConfirmText('');
      setIsDeleteConfirmVisible(true);
    } else {
      // Regular template - simple confirmation
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
    }
  };

  // Handle system template deletion with confirmation text
  const handleConfirmSystemTemplateDelete = async () => {
    if (!deletingTemplate) return;

    if (deleteConfirmText !== 'DELETE') {
      message.error('Please type "DELETE" to confirm deletion');
      return;
    }

    try {
      setDeleting(true);
      await promptTemplateService.deleteTemplate(deletingTemplate.id);
      message.success('System template deleted successfully');
      setIsDeleteConfirmVisible(false);
      setDeletingTemplate(null);
      setDeleteConfirmText('');
      loadTemplates();
    } catch (error: any) {
      message.error('Delete failed: ' + (error.message || 'Unknown error'));
    } finally {
      setDeleting(false);
    }
  };

  // Cancel system template deletion
  const handleCancelSystemTemplateDelete = () => {
    setIsDeleteConfirmVisible(false);
    setDeletingTemplate(null);
    setDeleteConfirmText('');
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

  const cardStyleBehance = isDark ? CARD_STYLE_BEHANCE_DARK : CARD_STYLE_BEHANCE;

  // Render category list (stretches to match right column, scrolls inside if needed)
  const renderCategories = () => (
    <Card 
      title="Template Categories" 
      size="small"
      style={{ ...cardStyleBehance, flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}
      bodyStyle={{
        ...(isDark ? { padding: '16px 20px' } : {}),
        flex: 1,
        overflow: 'auto',
        minHeight: 0,
      }}
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
      <Space direction="vertical" style={{ width: '100%' }} size={12}>
        {categories.map(category => (
          <Card
            key={category.id}
            size="small"
            hoverable
            className={selectedCategory === category.id ? 'selected-category' : ''}
            onClick={() => setSelectedCategory(category.id)}
            style={{
              ...cardStyleBehance,
              cursor: 'pointer',
              borderColor: selectedCategory === category.id ? colors.selectedBorder : undefined,
              ...(isDark && selectedCategory === category.id && {
                backgroundColor: 'rgba(33, 150, 243, 0.12)',
                borderLeft: '3px solid #2196F3',
              }),
            }}
            bodyStyle={{ padding: '12px 16px' }}
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

  // Helper function: check if user can edit template
  const canEditTemplate = (template: PromptTemplateDetail): boolean => {
    if (isAdmin) return true;
    if (template.scope === 'system') return false;
    return template.created_by === user?.id;
  };

  // Render template list
  const renderTemplateList = () => {
    const filteredTemplates = templates.filter(t => {
      if (searchText && !t.template_name.toLowerCase().includes(searchText.toLowerCase()) && 
          !t.description?.toLowerCase().includes(searchText.toLowerCase())) {
        return false;
      }
      // Filter by active status
      if (activeFilter === 'active_only' && !t.is_active) return false;
      if (activeFilter === 'inactive_only' && t.is_active) return false;
      return true;
    });

    return (
      <div style={{ marginBottom: 16 }}>
        <Space direction="vertical" style={{ width: '100%' }} size="middle">
          {filteredTemplates.map(template => {
            const canEdit = canEditTemplate(template);
            const isSystemTemplate = template.scope === 'system';
            
            // Build actions array based on permissions
            const actions = [
              canEdit ? (
                <Tooltip title="Edit">
                  <EditOutlined onClick={() => handleEditTemplate(template)} />
                </Tooltip>
              ) : (
                <Tooltip title="View (Read-only)">
                  <EyeOutlined onClick={() => handleEditTemplate(template)} />
                </Tooltip>
              ),
              <Tooltip title="Test">
                <ExperimentOutlined onClick={() => handleTestTemplate(template)} />
              </Tooltip>,
              <Tooltip title="Export">
                <DownloadOutlined onClick={() => handleExportTemplate(template)} />
              </Tooltip>
            ];
            
            // Only show delete button if user can edit
            if (canEdit) {
              actions.push(
                <Tooltip title="Delete">
                  <DeleteOutlined onClick={() => handleDeleteTemplate(template)} />
                </Tooltip>
              );
            }
            
            return (
              <Card
                key={template.id}
                size="small"
                hoverable
                actions={actions}
                style={cardStyleBehance}
                bodyStyle={{ padding: '16px 20px' }}
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
                        {!template.is_active && (
                          <Tag color="red">Inactive</Tag>
                        )}
                        {isSystemTemplate && !isAdmin && (
                          <Tag color="default">Read-only</Tag>
                        )}
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
            );
          })}
          {filteredTemplates.length === 0 && (
            <Empty description="No templates available" />
          )}
        </Space>
      </div>
    );
  };

  // Render editor
  const renderEditor = () => {
    const isReadOnly = editingTemplate ? !canEditTemplate(editingTemplate) : false;
    
    return (
      <Modal
        title={
          <Space>
            {editingTemplate ? (isReadOnly ? 'View Template (Read-only)' : 'Edit Template') : 'Create Template'}
            {isReadOnly && <Tag color="default">Read-only</Tag>}
          </Space>
        }
        open={isEditorVisible}
        onCancel={() => setIsEditorVisible(false)}
        width={1200}
        footer={null}
        styles={isDark ? {
          content: {
            borderRadius: 12,
            boxShadow: CARD_STYLE_BEHANCE_DARK.boxShadow,
            border: CARD_STYLE_BEHANCE_DARK.border,
            backgroundColor: colors.cardBg,
          },
          header: { color: colors.text, borderBottomColor: colors.cardBorder },
          body: { color: colors.text },
        } : {}}
      >
        <Row gutter={16}>
          <Col span={16}>
            {isReadOnly && (
              <Alert
                message="Read-only Mode"
                description="You can only view this template. System templates can only be edited by administrators."
                type="info"
                showIcon
                style={{ marginBottom: 16 }}
              />
            )}
            <Form
              form={form}
              layout="vertical"
              onFinish={handleSaveTemplate}
              disabled={isReadOnly}
            >
        <Row gutter={12}>
          <Col span={12}>
            <Form.Item
              label={<Text style={{ fontSize: 14 }}>Template Name</Text>}
              name="template_name"
              rules={[{ required: true, message: 'Please enter template name' }]}
            >
              <Input 
                placeholder="e.g.: News Summary Generation" 
                style={EDITOR_FORM_CONTROL}
              />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label={<Text style={{ fontSize: 14 }}>Template Code</Text>}
              name="template_code"
              rules={[{ required: true, message: 'Please enter template code' }]}
            >
              <Input 
                placeholder="e.g.: news_summary_v1" 
                disabled={!!editingTemplate}
                style={EDITOR_FORM_CONTROL}
              />
            </Form.Item>
          </Col>
        </Row>

        <Row gutter={12}>
          <Col span={12}>
            <Form.Item
              label={<Text style={{ fontSize: 14 }}>Category</Text>}
              name="category_id"
              rules={[{ required: true, message: 'Please select a category' }]}
            >
              <Select 
                placeholder="Select category"
                style={EDITOR_FORM_CONTROL}
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
              label={<Text style={{ fontSize: 14 }}>Visibility Scope</Text>}
              name="scope"
              initialValue="user"
              extra={
                currentScope === 'user' ? 'Only you can see and use this template' :
                currentScope === 'shared' ? 'Can be shared with other users (control publishing status below)' :
                currentScope === 'system' ? 'Available to all users as a standard template' : ''
              }
            >
              <Select 
                style={EDITOR_FORM_CONTROL}
                onChange={handleScopeChange}
              >
                <Option value="user">🔒 Private (Only Me)</Option>
                <Option value="shared">👥 Shared (Can Publish)</Option>
                {isAdmin && <Option value="system">🌟 System (Admin Only - Always Public)</Option>}
              </Select>
            </Form.Item>
          </Col>
        </Row>

        <Form.Item
          label={<Text style={{ fontSize: 14 }}>Description</Text>}
          name="description"
        >
          <TextArea 
            rows={6} 
            placeholder="Briefly describe the purpose of this template"
            style={EDITOR_FORM_TEXTAREA}
          />
        </Form.Item>

        <Form.Item
          label={<Text style={{ fontSize: 14 }}>System Prompt (Optional)</Text>}
          name="system_prompt"
        >
          <TextArea 
            rows={6} 
            placeholder="Define AI's role and behavior, e.g.: You are a professional financial analyst..."
            style={EDITOR_FORM_TEXTAREA}
          />
        </Form.Item>

        <Form.Item
          label={<Text style={{ fontSize: 14 }}>User Prompt Template</Text>}
          name="user_prompt_template"
          rules={[{ required: true, message: 'Please enter user prompt template' }]}
          extra="Use {{variable_name}} format to define variables"
        >
          <TextArea 
            rows={6} 
            placeholder="Please analyze the following news:\n\nTitle: {{title}}\nContent: {{content}}"
            style={EDITOR_FORM_TEXTAREA}
          />
        </Form.Item>

        <Divider style={{ fontSize: 14, margin: '12px 0' }}>Recommended Configuration (Required)</Divider>

        <Row gutter={12}>
          <Col span={12}>
            <Form.Item
              label={<Text style={{ fontSize: 14 }}>Recommended Provider <Text type="danger">*</Text></Text>}
              name="recommended_provider"
              rules={[{ required: true, message: 'Please select a provider' }]}
            >
              <Select 
                placeholder="Select LLM provider" 
                style={EDITOR_FORM_CONTROL}
                onChange={handleProviderChange}
              >
                <Option value="qwen">🔷 Qwen</Option>
                <Option value="doubao">🫘 Doubao</Option>
                <Option value="glm">🧠 GLM</Option>
              </Select>
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label={<Text style={{ fontSize: 14 }}>Recommended Model <Text type="danger">*</Text></Text>}
              name="recommended_model"
              rules={[{ required: true, message: 'Please select a model' }]}
              extra={selectedProvider ? `Available models for ${selectedProvider}` : 'Please select a provider first'}
            >
              <Select 
                placeholder={selectedProvider ? "Select model" : "Select provider first"}
                style={EDITOR_FORM_CONTROL}
                disabled={!selectedProvider || availableModels.length === 0}
              >
                {availableModels.map(model => (
                  <Option key={model} value={model}>{model}</Option>
                ))}
              </Select>
            </Form.Item>
          </Col>
        </Row>

        <Row gutter={12}>
          <Col span={12}>
            <Form.Item
              label={<Text style={{ fontSize: 14 }}>Temperature</Text>}
              name="temperature"
              initialValue={0.7}
            >
              <InputNumber 
                min={0} 
                max={2} 
                step={0.1} 
                style={{ width: '100%', ...EDITOR_FORM_CONTROL }}
              />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label={<Text style={{ fontSize: 14 }}>Max Tokens</Text>}
              name="max_tokens"
              initialValue={2000}
            >
              <InputNumber 
                min={100} 
                max={10000} 
                step={100} 
                style={{ width: '100%', ...EDITOR_FORM_CONTROL }}
              />
            </Form.Item>
          </Col>
        </Row>

        <Form.Item
          label={<Text style={{ fontSize: 14 }}>Tags</Text>}
          name="tags"
        >
          <Select 
            mode="tags" 
            placeholder="Add tags, press Enter to confirm"
            style={EDITOR_FORM_CONTROL}
          />
        </Form.Item>

        {/* Published Status - Only show for Shared scope */}
        {currentScope === 'shared' && (
          <Alert
            message="Shared Template Publishing"
            description={
              <>
                <Paragraph style={{ marginBottom: 8, fontSize: 14 }}>
                  Shared templates have two states:
                </Paragraph>
                <ul style={{ marginBottom: 0, paddingLeft: 20, fontSize: 14 }}>
                  <li><Text strong>Draft (Unpublished)</Text>: Only you can see and edit this template</li>
                  <li><Text strong>Published</Text>: All users can see and use this template</li>
                </ul>
              </>
            }
            type="info"
            showIcon
            style={{ marginBottom: 16, fontSize: 14 }}
          />
        )}

        <Row gutter={12}>
          {currentScope === 'shared' && (
            <Col span={12}>
              <Form.Item
                label={<Text style={{ fontSize: 14 }}>Published Status</Text>}
                name="is_public"
                valuePropName="checked"
                initialValue={false}
                extra="Enable to publish this template to all users"
              >
                <Switch 
                  size="small"
                  checkedChildren="Published" 
                  unCheckedChildren="Draft"
                />
              </Form.Item>
            </Col>
          )}
          <Col span={currentScope === 'shared' ? 12 : 24}>
            <Form.Item
              label={<Text style={{ fontSize: 14 }}>Active Status</Text>}
              name="is_active"
              valuePropName="checked"
              initialValue={true}
              extra="Inactive templates will not be usable"
            >
              <Switch 
                size="small"
                checkedChildren="Active" 
                unCheckedChildren="Inactive"
              />
            </Form.Item>
          </Col>
        </Row>

        <Form.Item style={{ marginBottom: 0 }}>
          <Space size="small">
            {!isReadOnly && (
              <Button
                type="primary"
                htmlType="submit"
                size="small"
                style={{ borderRadius: 8, fontWeight: 500, ...(isDark ? { background: '#2196F3', borderColor: '#2196F3' } : {}) }}
              >
                Save
              </Button>
            )}
            <Button
              onClick={() => setIsEditorVisible(false)}
              size="small"
              style={{ borderRadius: 8, ...(isDark ? { color: colors.textSecondary, borderColor: colors.cardBorder } : {}) }}
            >
              {isReadOnly ? 'Close' : 'Cancel'}
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
  };

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

          <Card title="Template Information" size="small" style={cardStyleBehance} bodyStyle={{ padding: '16px 20px' }}>
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
            <Card title="Test Variables" size="small" style={cardStyleBehance} bodyStyle={{ padding: '16px 20px' }}>
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
                style={{ borderRadius: 10, fontWeight: 500, height: 40, ...(isDark ? { background: '#2196F3', borderColor: '#2196F3' } : {}) }}
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
                style={cardStyleBehance}
                bodyStyle={{ padding: '16px 20px' }}
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
                            background: colors.codeBlockBg, 
                            padding: '12px', 
                            borderRadius: '8px',
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
                        background: colors.codeBlockBg, 
                        padding: '12px', 
                        borderRadius: '8px',
                        whiteSpace: 'pre-wrap'
                      }}>
                        {testResult.rendered_prompt.user_prompt}
                      </pre>
                    </Paragraph>
                  </Panel>
                </Collapse>
              )}

              {testResult.llm_response && (
                <Card title="LLM Response" size="small" style={cardStyleBehance} bodyStyle={{ padding: '16px 20px' }}>
                  <Paragraph>
                    <pre style={{ 
                      background: colors.codeBlockBg, 
                      padding: '12px', 
                      borderRadius: '8px',
                      whiteSpace: 'pre-wrap'
                    }}>
                      {testResult.llm_response}
                    </pre>
                  </Paragraph>
                </Card>
              )}

              {testResult.parsed_output && (
                <Card title="Parsed Output" size="small" style={cardStyleBehance} bodyStyle={{ padding: '16px 20px' }}>
                  <Paragraph>
                    <pre style={{ 
                      background: colors.codeBlockBg, 
                      padding: '12px', 
                      borderRadius: '8px',
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
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: colors.cardBg,
            colorBgElevated: (colors as any).cardBg,
            colorBorderSecondary: colors.cardBorder,
            colorPrimary: '#2196F3',
            colorText: colors.text,
            colorTextSecondary: colors.textSecondary,
            colorFillQuaternary: (colors as any).inputBg ?? '#1E202B',
            colorBgLayout: (colors as any).pageBg,
            borderRadius: 8,
            borderRadiusLG: 12,
          },
        }),
      }}
    >
    <div style={{ padding: '12px 40px 32px', backgroundColor: colors.pageBg, minHeight: '100%' }}>
      <Title level={2} style={{ color: colors.text, marginTop: 0, marginBottom: 4 }}>
        <FileTextOutlined /> Prompt Template Management
      </Title>
      <Paragraph type="secondary" style={{ color: colors.textSecondary, marginBottom: 20 }}>
        Manage and optimize your LLM Prompt templates with support for variables, version control, and online testing
      </Paragraph>

      <Spin spinning={loading}>
        <Row gutter={[16, 16]} style={{ alignItems: 'stretch' }}>
          {/* Left: Category list — stretches to match right column height */}
          <Col xs={24} md={6} style={{ display: 'flex' }}>
            {renderCategories()}
          </Col>

          {/* Right: Template list and operations */}
          <Col xs={24} md={18} style={{ display: 'flex' }}>
            <Card style={{ ...cardStyleBehance, flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }} bodyStyle={{ padding: '20px 24px', flex: 1, overflow: 'auto' }}>
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
                      {...(isDark && {
                        style: { backgroundColor: (colors as any).inputBg ?? '#1E202B' },
                      })}
                    />
                  </Col>
                  <Col>
                    <Select
                      style={{ width: 140, ...(isDark ? { backgroundColor: (colors as any).inputBg } : {}) }}
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
                    <Select
                      style={{ width: 130, ...(isDark ? { backgroundColor: (colors as any).inputBg } : {}) }}
                      value={activeFilter}
                      onChange={setActiveFilter}
                    >
                      <Option value="all">All Status</Option>
                      <Option value="active_only">Active Only</Option>
                      <Option value="inactive_only">Inactive Only</Option>
                    </Select>
                  </Col>
                  <Col>
                    <Button 
                      type="primary" 
                      icon={<PlusOutlined />}
                      onClick={handleCreateTemplate}
                      style={{
                        borderRadius: 10,
                        fontWeight: 500,
                        height: 36,
                        paddingLeft: 16,
                        paddingRight: 16,
                        ...(isDark ? { background: '#2196F3', borderColor: '#2196F3' } : {}),
                      }}
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

      {/* System Template Delete Confirmation Modal */}
      <Modal
        title={
          <Space>
            <Text strong style={{ color: '#ff4d4f' }}>⚠️ Delete System Template</Text>
          </Space>
        }
        open={isDeleteConfirmVisible}
        onCancel={handleCancelSystemTemplateDelete}
        onOk={handleConfirmSystemTemplateDelete}
        okText="Delete"
        okType="danger"
        okButtonProps={{ disabled: deleteConfirmText !== 'DELETE', loading: deleting }}
        cancelText="Cancel"
        width={600}
        destroyOnClose
      >
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          <Alert
            message="Dangerous Operation"
            description={
              <div>
                <Paragraph strong style={{ marginBottom: '8px' }}>
                  You are about to delete a <Text strong style={{ color: '#ff4d4f' }}>System Template</Text>:
                </Paragraph>
                <Paragraph style={{ marginBottom: '8px' }}>
                  <Text strong>Template Name:</Text> {deletingTemplate?.template_name}
                </Paragraph>
                <Paragraph style={{ marginBottom: '8px' }}>
                  <Text strong>Template Code:</Text> <Text code>{deletingTemplate?.template_code}</Text>
                </Paragraph>
                <Paragraph style={{ marginTop: '12px', color: '#ff4d4f' }}>
                  <Text strong>⚠️ Warning:</Text>
                </Paragraph>
                <ul style={{ marginTop: '8px', paddingLeft: '20px' }}>
                  <li>This action cannot be undone</li>
                  <li>System templates are critical for application functionality</li>
                  <li>Deleting this template may cause errors in dependent features</li>
                  <li>Any scheduled tasks using this template will fail</li>
                </ul>
              </div>
            }
            type="error"
            showIcon
            style={{ marginBottom: '16px' }}
          />

          <div>
            <Paragraph strong>
              To confirm deletion, please type <Text code style={{ color: '#ff4d4f' }}>DELETE</Text> in the box below:
            </Paragraph>
            <Input
              placeholder="Type DELETE to confirm"
              value={deleteConfirmText}
              onChange={(e) => setDeleteConfirmText(e.target.value)}
              onPressEnter={handleConfirmSystemTemplateDelete}
              style={{
                marginTop: '8px',
                borderColor: deleteConfirmText && deleteConfirmText !== 'DELETE' ? '#ff4d4f' : undefined
              }}
              status={deleteConfirmText && deleteConfirmText !== 'DELETE' ? 'error' : ''}
            />
            {deleteConfirmText && deleteConfirmText !== 'DELETE' && (
              <Text type="danger" style={{ fontSize: '12px', marginTop: '4px', display: 'block' }}>
                The text you entered does not match "DELETE"
              </Text>
            )}
          </div>
        </Space>
      </Modal>

    </div>
    </ConfigProvider>
  );
};

export default PromptTemplatesPage;

