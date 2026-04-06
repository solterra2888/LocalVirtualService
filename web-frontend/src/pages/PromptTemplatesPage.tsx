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
  StarOutlined,
  FileTextOutlined,
  DownloadOutlined,
  SearchOutlined,
  ThunderboltOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  EyeOutlined,
  LockOutlined
} from '@ant-design/icons';
import promptTemplateService, {
  PromptCategory,
  PromptTemplateDetail,
  TemplateCreateData,
  TestTemplateResponse,
  ModuleInfo,
} from '../services/promptTemplateService';
import VariableHelper from '../components/VariableHelper';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { getModalStyles } from '../utils/modalStyles';

const { Title, Paragraph, Text } = Typography;

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

const EDITOR_FORM_CONTROL = { height: 32, fontSize: 14 };
const EDITOR_FORM_TEXTAREA = { fontSize: 14 };

const { Option } = Select;
const { TextArea } = Input;
const { Panel } = Collapse;

const PROVIDER_MODEL_MAPPING: Record<string, string[]> = {
  'qwen': ['qwen-turbo', 'qwen-plus', 'qwen-max', 'qwen-max-longcontext',
           'qwen3-turbo', 'qwen3-plus', 'qwen3-max'],
  'doubao': ['doubao-seed-1-6-250615'],
  'glm': ['glm-4.6', 'glm-4', 'glm-4-flash']
};

/**
 * System templates whose template_code is hardcoded in backend services.
 * These must never be deleted — only their prompt content and LLM config
 * can be edited by admins.
 */
const MODULE_TO_CATEGORY: Record<string, string> = {
  twitter_phase2: 'report_generation',
  youtube_phase2: 'report_generation',
  file_analysis: 'file_analysis',
};

const CODE_BOUND_TEMPLATES = new Set([
  'news_summary_v1',
  'news_classification',
  'news_cctv_summary_v1',
  'theme_description_generator_v1',
  'twitter_daily_phase1_default',
  'twitter_daily_phase2_default',
  'youtube_daily_phase1_default',
  'youtube_daily_phase2_default',
  'audio_analysis_v1',
  'content_tag_v1',
]);

const PromptTemplatesPage: React.FC = () => {
  const { user } = useAuthStore();
  const { themeMode } = useDashboardSettings();
  const isAdmin = user?.is_admin || false;
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  
  const [categories, setCategories] = useState<PromptCategory[]>([]);
  const [templates, setTemplates] = useState<PromptTemplateDetail[]>([]);
  const [selectedTemplate, setSelectedTemplate] = useState<PromptTemplateDetail | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [searchText, setSearchText] = useState<string>('');
  const [scopeFilter, setScopeFilter] = useState<string>('all');
  const [selectedProvider, setSelectedProvider] = useState<string | undefined>();
  const [availableModels, setAvailableModels] = useState<string[]>([]);
  const [currentScope, setCurrentScope] = useState<string>('user');

  const [isEditorVisible, setIsEditorVisible] = useState<boolean>(false);
  const [isTestVisible, setIsTestVisible] = useState<boolean>(false);
  const [editingTemplate, setEditingTemplate] = useState<PromptTemplateDetail | null>(null);
  const [testing, setTesting] = useState<boolean>(false);
  const [testResult, setTestResult] = useState<TestTemplateResponse | null>(null);
  const [currentEditingCategoryId, setCurrentEditingCategoryId] = useState<number | undefined>();
  const [selectedModuleCode, setSelectedModuleCode] = useState<string | undefined>();

  const [modules, setModules] = useState<ModuleInfo[]>([]);

  // 「设为我的默认」弹窗状态
  const [setDefaultModalTemplate, setSetDefaultModalTemplate] = useState<PromptTemplateDetail | null>(null);
  const [setDefaultModuleCode, setSetDefaultModuleCode] = useState<string | undefined>();
  const [settingDefault, setSettingDefault] = useState(false);

  const [form] = Form.useForm();
  const [testForm] = Form.useForm();

  // ── Data loading ──────────────────────────────────────────────────────

  const loadTemplates = useCallback(async () => {
    try {
      const params: any = {};
      if (searchText) params.search = searchText;
      if (scopeFilter !== 'all') params.scope = scopeFilter;

      const templatesData = await promptTemplateService.listTemplates(params);
      const detailedTemplates = await Promise.all(
        templatesData.map(async (template) => {
          try {
            return await promptTemplateService.getTemplate(template.id);
          } catch (error) {
            console.warn(`Failed to load details for template ${template.id}:`, error);
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
  }, [searchText, scopeFilter]);

  // loadData 只负责加载分类和模块元数据（模板统一由 loadTemplates 负责）
  const loadData = useCallback(async () => {
    try {
      setLoading(true);
      const [categoriesData] = await Promise.all([
        promptTemplateService.getCategories(),
      ]);
      setCategories(categoriesData);

      try {
        const modulesData = await promptTemplateService.getModules();
        setModules(modulesData);
      } catch { /* non-critical */ }
    } catch (error: any) {
      message.error('Failed to load data: ' + (error.message || 'Unknown error'));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadData(); }, [loadData]);

  // searchText 或 scopeFilter 变化时重新拉取模板（含清空搜索时恢复全量列表）
  useEffect(() => {
    loadTemplates();
  }, [searchText, scopeFilter, loadTemplates]);

  // ── Handlers ──────────────────────────────────────────────────────────

  const handleProviderChange = (provider: string) => {
    setSelectedProvider(provider);
    const models = PROVIDER_MODEL_MAPPING[provider] || [];
    setAvailableModels(models);
    form.setFieldValue('recommended_model', undefined);
    if (models.length > 0) {
      form.setFieldValue('recommended_model', models[0]);
    }
  };

  const handleScopeChange = (scope: string) => {
    setCurrentScope(scope);
    if (scope === 'user') form.setFieldValue('is_public', false);
    else if (scope === 'system') {
      form.setFieldValue('is_public', true);
      form.setFieldValue('admin_module_code', undefined);
      setSelectedModuleCode(undefined);
    }
  };

  const handleCreateTemplate = () => {
    setEditingTemplate(null);
    setCurrentEditingCategoryId(undefined);
    setSelectedModuleCode(undefined);
    setSelectedProvider(undefined);
    setAvailableModels([]);
    setCurrentScope('user');
    form.resetFields();
    setIsEditorVisible(true);
  };

  const handleEditTemplate = async (template: PromptTemplateDetail) => {
    try {
      const detail = await promptTemplateService.getTemplate(template.id);
      setEditingTemplate(detail);
      setCurrentEditingCategoryId(detail.category_id);

      if (!isAdmin && detail.category_code) {
        setSelectedModuleCode(detail.category_code);
      }

      setCurrentScope(detail.scope);

      if (detail.recommended_provider) {
        setSelectedProvider(detail.recommended_provider);
        setAvailableModels(PROVIDER_MODEL_MAPPING[detail.recommended_provider] || []);
      }

      // 从 template_code 或 category_code 推断 module_code（user-scope 模板通用逻辑）
      const inferredModuleCode = Object.keys(MODULE_TO_CATEGORY).find(mc =>
        detail.template_code?.startsWith(`user_${mc}_`)
      ) || (detail.scope === 'user' ? detail.category_code : undefined);

      const formValues: any = {
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
      };

      if (!isAdmin) {
        // 非 Admin：回填 module_code 字段
        formValues.module_code = inferredModuleCode;
        setSelectedModuleCode(inferredModuleCode);
        setCurrentEditingCategoryId(detail.category_id);
      } else if (detail.scope === 'user') {
        // Admin 编辑 user-scope：回填 admin_module_code 字段，同步 selectedModuleCode
        formValues.admin_module_code = inferredModuleCode;
        setSelectedModuleCode(inferredModuleCode);
      }
      form.setFieldsValue(formValues);

      setIsEditorVisible(true);
    } catch (error: any) {
      message.error('Failed to load template details: ' + (error.message || 'Unknown error'));
    }
  };

  const handleSaveTemplate = async (values: any) => {
    try {
      const payload = { ...values };

      // admin_module_code 是表单专用字段，不传给后端模板接口
      const resolvedModuleCode = payload.admin_module_code ?? selectedModuleCode;
      delete payload.admin_module_code;

      // user-scope 模板（admin 或非 admin）走统一逻辑：
      //   - category 从 module 映射自动填充
      //   - template_code 自动生成（如未手动填写）
      //   - scope 锁定为 'user'，is_public 锁定为 false
      const isUserScope = !isAdmin || payload.scope === 'user';
      if (isUserScope) {
        if (resolvedModuleCode && !payload.category_id) {
          const catCode = MODULE_TO_CATEGORY[resolvedModuleCode] || resolvedModuleCode;
          const matchedCat = categories.find(c => c.category_code === catCode);
          if (matchedCat) {
            payload.category_id = matchedCat.id;
          } else {
            message.error(`No matching category for module "${resolvedModuleCode}"`);
            return;
          }
        }
        if (!editingTemplate && !payload.template_code) {
          payload.template_code = `user_${resolvedModuleCode}_${Date.now()}`;
        }
        if (!isAdmin) {
          payload.scope = 'user';
          payload.is_public = false;
        }
      }

      if (editingTemplate) {
        await promptTemplateService.updateTemplate(editingTemplate.id, payload);
        message.success('Template updated successfully');
      } else {
        const result = await promptTemplateService.createTemplate(payload as TemplateCreateData);

        // 创建成功后自动绑定模板偏好（user-scope 且有 module）
        if (result?.template_id && resolvedModuleCode && isUserScope) {
          try {
            await promptTemplateService.setPreference(resolvedModuleCode, result.template_id);
            message.success(`Template created and set as your default for "${modules.find(m => m.module_code === resolvedModuleCode)?.module_name || resolvedModuleCode}"`);
          } catch (prefErr: any) {
            message.success('Template created successfully');
            message.warning(`Could not auto-set preference: ${prefErr?.response?.data?.detail || prefErr?.message || 'Unknown error'}`);
          }
        } else {
          message.success('Template created successfully');
        }
      }
      setIsEditorVisible(false);
      loadTemplates();
    } catch (error: any) {
      let errorMessage = 'Unknown error';
      if (error.response?.data?.detail) errorMessage = error.response.data.detail;
      else if (error.response?.data?.message) errorMessage = error.response.data.message;
      else if (error.message) errorMessage = error.message;
      else if (typeof error === 'string') errorMessage = error;
      message.error(`Save failed: ${errorMessage}`);
    }
  };

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

  const handleTestTemplate = async (template: PromptTemplateDetail) => {
    try {
      const detail = await promptTemplateService.getTemplate(template.id);
      setSelectedTemplate(detail);
      const defaultValues: any = {};
      Object.entries(detail.variables || {}).forEach(([key, varDef]) => {
        if (varDef.default !== undefined) defaultValues[key] = varDef.default;
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
      if (result.success) message.success('Test successful');
      else message.error('Test failed: ' + result.error);
    } catch (error: any) {
      message.error('Test failed: ' + (error.message || 'Unknown error'));
    } finally {
      setTesting(false);
    }
  };

  const handleExportTemplate = async (template: PromptTemplateDetail) => {
    try {
      await promptTemplateService.downloadTemplateExport(template.id, template.template_code);
      message.success('Export successful');
    } catch (error: any) {
      message.error('Export failed: ' + (error.message || 'Unknown error'));
    }
  };

  // 推断模板关联的 module_code（非 Admin 创建的模板遵循 user_{moduleCode}_timestamp 命名规则）
  const inferModuleCode = (template: PromptTemplateDetail): string | undefined => {
    for (const mc of Object.keys(MODULE_TO_CATEGORY)) {
      if (template.template_code?.startsWith(`user_${mc}_`)) return mc;
    }
    return undefined;
  };

  const handleOpenSetDefault = (template: PromptTemplateDetail) => {
    const inferred = inferModuleCode(template);
    setSetDefaultModalTemplate(template);
    setSetDefaultModuleCode(inferred);
  };

  const handleConfirmSetDefault = async () => {
    if (!setDefaultModalTemplate || !setDefaultModuleCode) return;
    setSettingDefault(true);
    try {
      await promptTemplateService.setPreference(setDefaultModuleCode, setDefaultModalTemplate.id);
      const moduleName = modules.find(m => m.module_code === setDefaultModuleCode)?.module_name || setDefaultModuleCode;
      message.success(`"${setDefaultModalTemplate.template_name}" is now your default for "${moduleName}"`);
      setSetDefaultModalTemplate(null);
      setSetDefaultModuleCode(undefined);
    } catch (err: any) {
      message.error(`Failed to set preference: ${err?.response?.data?.detail || err?.message || 'Unknown error'}`);
    } finally {
      setSettingDefault(false);
    }
  };

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
        setTimeout(() => {
          textarea.focus();
          textarea.setSelectionRange(start + variableText.length, start + variableText.length);
        }, 0);
      }
    }
  };

  const cardStyleBehance = isDark ? CARD_STYLE_BEHANCE_DARK : CARD_STYLE_BEHANCE;

  // ── Permission helpers ────────────────────────────────────────────────

  const isCodeBound = (template: PromptTemplateDetail): boolean =>
    template.scope === 'system' && CODE_BOUND_TEMPLATES.has(template.template_code);

  const canEditTemplate = (template: PromptTemplateDetail): boolean => {
    if (isAdmin) return true;
    if (template.scope === 'system') return false;
    return template.created_by === user?.id;
  };

  const canDeleteTemplate = (template: PromptTemplateDetail): boolean => {
    if (isCodeBound(template)) return false;
    if (template.scope === 'system') return isAdmin;
    if (isAdmin) return true;
    return template.created_by === user?.id;
  };

  // ── Card grid ─────────────────────────────────────────────────────────

  const renderTemplateCards = () => {
    const filteredTemplates = templates.filter(t => {
      if (searchText &&
        !t.template_name.toLowerCase().includes(searchText.toLowerCase()) &&
        !t.description?.toLowerCase().includes(searchText.toLowerCase())) {
        return false;
      }
      return true;
    });

    if (filteredTemplates.length === 0) {
      return <Empty description="No templates found" style={{ padding: '60px 0' }} />;
    }

    return (
      <Row gutter={[16, 16]}>
        {filteredTemplates.map(template => {
          const isSystem = template.scope === 'system';
          const codeBound = isCodeBound(template);
          const editable = canEditTemplate(template);
          const deletable = canDeleteTemplate(template);

          return (
            <Col xs={24} sm={12} lg={8} key={template.id}>
              <Card
                hoverable
                style={{
                  ...cardStyleBehance,
                  height: '100%',
                  display: 'flex',
                  flexDirection: 'column',
                  ...(codeBound && { borderLeft: `3px solid ${isDark ? '#2196F3' : '#1890ff'}` }),
                  ...(isSystem && !codeBound && { borderLeft: `3px solid ${isDark ? '#faad14' : '#fa8c16'}` }),
                }}
                bodyStyle={{ padding: '16px 20px', flex: 1, display: 'flex', flexDirection: 'column' }}
                actions={[
                  editable ? (
                    <Tooltip title="Edit" key="edit">
                      <EditOutlined onClick={() => handleEditTemplate(template)} />
                    </Tooltip>
                  ) : (
                    <Tooltip title="View" key="view">
                      <EyeOutlined onClick={() => handleEditTemplate(template)} />
                    </Tooltip>
                  ),
                  <Tooltip title="Test" key="test">
                    <ExperimentOutlined onClick={() => handleTestTemplate(template)} />
                  </Tooltip>,
                  <Tooltip title="Export" key="export">
                    <DownloadOutlined onClick={() => handleExportTemplate(template)} />
                  </Tooltip>,
                  // 「设为我的默认」：user-scope 模板（可配模块）显示此按钮
                  ...(!isSystem ? [
                    <Tooltip title="Set as my default for a module" key="set-default">
                      <StarOutlined
                        style={{ color: '#faad14' }}
                        onClick={() => handleOpenSetDefault(template)}
                      />
                    </Tooltip>
                  ] : []),
                  ...(deletable ? [
                    <Tooltip title="Delete" key="delete">
                      <DeleteOutlined style={{ color: '#ff4d4f' }} onClick={() => handleDeleteTemplate(template)} />
                    </Tooltip>
                  ] : []),
                ]}
              >
                {/* Header: name + tags */}
                <div style={{ marginBottom: 8 }}>
                  <Space wrap size={4} style={{ marginBottom: 4 }}>
                    <Text strong style={{ fontSize: 15 }}>{template.template_name}</Text>
                    {template.is_default && <StarFilled style={{ color: '#faad14' }} />}
                  </Space>
                  <div>
                    <Space wrap size={4}>
                      {codeBound ? (
                        <Tooltip title="Code-bound: this template is hardcoded in backend services and cannot be deleted">
                          <Tag color="blue" icon={<LockOutlined />}>Code-bound</Tag>
                        </Tooltip>
                      ) : isSystem ? (
                        <Tag color="orange">System</Tag>
                      ) : (
                        <Tag color="green">User</Tag>
                      )}
                      {!template.is_active && <Tag color="red">Inactive</Tag>}
                      {template.recommended_model && (
                        <Tag style={{ fontSize: 11 }}>{template.recommended_model}</Tag>
                      )}
                    </Space>
                  </div>
                </div>

                {/* Description */}
                <Text
                  type="secondary"
                  style={{ fontSize: 12, flex: 1, display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}
                >
                  {template.description || 'No description'}
                </Text>

                {/* Footer info */}
                <div style={{ marginTop: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                  <Text type="secondary" style={{ fontSize: 11 }}>
                    {template.category_name || '—'}
                  </Text>
                  <Space size={12}>
                    <Tooltip title="Usage count">
                      <Text type="secondary" style={{ fontSize: 11 }}>
                        {template.usage_count ?? 0} uses
                      </Text>
                    </Tooltip>
                    {template.success_rate != null && (
                      <Tooltip title="Success rate">
                        <Text
                          type={template.success_rate >= 90 ? 'success' : 'warning'}
                          style={{ fontSize: 11 }}
                        >
                          {template.success_rate.toFixed(0)}%
                        </Text>
                      </Tooltip>
                    )}
                  </Space>
                </div>
              </Card>
            </Col>
          );
        })}
      </Row>
    );
  };

  // ── Editor modal ──────────────────────────────────────────────────────

  const renderEditor = () => {
    const isSystemEdit = editingTemplate?.scope === 'system';
    const isReadOnly = editingTemplate ? !canEditTemplate(editingTemplate) : false;

    return (
      <Modal
        title={
          <Space>
            {editingTemplate ? (isReadOnly ? 'View Template (Read-only)' : 'Edit Template') : 'Create Template'}
            {isReadOnly && <Tag color="default">Read-only</Tag>}
            {isSystemEdit && isAdmin && <Tag color="blue">System</Tag>}
          </Space>
        }
        open={isEditorVisible}
        onCancel={() => setIsEditorVisible(false)}
        width={1200}
        footer={null}
        styles={getModalStyles(themeMode)}
      >
        <Row gutter={16}>
          <Col span={16}>
            {isReadOnly && (
              <Alert
                message="Read-only Mode"
                description="System templates can only be edited by administrators."
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
              {isAdmin ? (
                <>
                  {/* Admin 始终显示 Scope 选择（编辑已有模板时只读） */}
                  <Form.Item
                    label={<Text style={{ fontSize: 14 }}>Visibility Scope</Text>}
                    name="scope"
                    initialValue="user"
                    extra={
                      currentScope === 'user'
                        ? 'Private template — only visible to you. Use this for personal daily report styles.'
                        : currentScope === 'system'
                        ? 'System template — available to all users as a standard option.'
                        : ''
                    }
                  >
                    <Select
                      style={EDITOR_FORM_CONTROL}
                      onChange={handleScopeChange}
                      disabled={!!editingTemplate}
                    >
                      <Option value="user">🔒 Private (Only Me)</Option>
                      <Option value="system">🌟 System (All Users)</Option>
                    </Select>
                  </Form.Item>

                  {currentScope === 'user' ? (
                    <>
                      {/* Admin user-scope：与非 Admin 一致——选 Module，auto 生成 template_code */}
                      <Row gutter={12}>
                        <Col span={12}>
                          <Form.Item
                            label={<Text style={{ fontSize: 14 }}>Template Name</Text>}
                            name="template_name"
                            rules={[{ required: true, message: 'Please enter template name' }]}
                          >
                            <Input placeholder="e.g.: My Twitter Report Style" style={EDITOR_FORM_CONTROL} />
                          </Form.Item>
                        </Col>
                        <Col span={12}>
                          <Form.Item
                            label={<Text style={{ fontSize: 14 }}>Target Module</Text>}
                            name="admin_module_code"
                            rules={[{ required: !editingTemplate, message: 'Please select a target module' }]}
                            extra="Choose which feature this template is for"
                          >
                            <Select
                              placeholder="Select target module"
                              style={EDITOR_FORM_CONTROL}
                              disabled={!!editingTemplate}
                              onChange={(value: string) => {
                                setSelectedModuleCode(value);
                                const catCode = MODULE_TO_CATEGORY[value] || value;
                                const matchedCat = categories.find(c => c.category_code === catCode);
                                if (matchedCat) {
                                  setCurrentEditingCategoryId(matchedCat.id);
                                  form.setFieldValue('category_id', matchedCat.id);
                                }
                              }}
                            >
                              {modules.filter(m => m.configurable).map(m => (
                                <Option key={m.module_code} value={m.module_code}>
                                  {m.module_name}
                                </Option>
                              ))}
                            </Select>
                          </Form.Item>
                        </Col>
                      </Row>
                    </>
                  ) : (
                    <>
                      {/* Admin system-scope：完整控制——手动 Template Name + Template Code + Category */}
                      <Row gutter={12}>
                        <Col span={12}>
                          <Form.Item
                            label={<Text style={{ fontSize: 14 }}>Template Name</Text>}
                            name="template_name"
                            rules={[{ required: true, message: 'Please enter template name' }]}
                          >
                            <Input placeholder="e.g.: News Summary Generation" style={EDITOR_FORM_CONTROL} />
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
                      <Form.Item
                        label={<Text style={{ fontSize: 14 }}>Category</Text>}
                        name="category_id"
                        rules={[{ required: true, message: 'Please select a category' }]}
                      >
                        <Select
                          placeholder="Select category"
                          style={EDITOR_FORM_CONTROL}
                          onChange={(value: number) => setCurrentEditingCategoryId(value)}
                        >
                          {categories.map(cat => (
                            <Option key={cat.id} value={cat.id}>
                              {cat.icon} {cat.category_name}
                            </Option>
                          ))}
                        </Select>
                      </Form.Item>
                    </>
                  )}
                </>
              ) : (
                <>
                  {/* Non-admin: Template Name + Target Module */}
                  <Row gutter={12}>
                    <Col span={12}>
                      <Form.Item
                        label={<Text style={{ fontSize: 14 }}>Template Name</Text>}
                        name="template_name"
                        rules={[{ required: true, message: 'Please enter template name' }]}
                      >
                        <Input placeholder="e.g.: My Twitter Report Style" style={EDITOR_FORM_CONTROL} />
                      </Form.Item>
                    </Col>
                    <Col span={12}>
                      <Form.Item
                        label={<Text style={{ fontSize: 14 }}>Target Module</Text>}
                        name="module_code"
                        rules={[{ required: true, message: 'Please select a target module' }]}
                        extra="Choose which feature this template is for"
                      >
                        <Select
                          placeholder="Select target module"
                          style={EDITOR_FORM_CONTROL}
                          disabled={!!editingTemplate}
                          onChange={(value: string) => {
                            setSelectedModuleCode(value);
                            const catCode = MODULE_TO_CATEGORY[value] || value;
                            const matchedCat = categories.find(c => c.category_code === catCode);
                            if (matchedCat) {
                              setCurrentEditingCategoryId(matchedCat.id);
                              form.setFieldValue('category_id', matchedCat.id);
                            }
                          }}
                        >
                          {modules
                            .filter(m => m.configurable)
                            .map(m => (
                              <Option key={m.module_code} value={m.module_code}>
                                {m.module_name}
                              </Option>
                            ))}
                        </Select>
                      </Form.Item>
                    </Col>
                  </Row>
                </>
              )}

              <Form.Item
                label={<Text style={{ fontSize: 14 }}>Description</Text>}
                name="description"
              >
                <TextArea
                  rows={4}
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
                  placeholder={'Please analyze the following news:\n\nTitle: {{title}}\nContent: {{content}}'}
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
                    <Select placeholder="Select LLM provider" style={EDITOR_FORM_CONTROL} onChange={handleProviderChange}>
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
                      placeholder={selectedProvider ? 'Select model' : 'Select provider first'}
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
                  <Form.Item label={<Text style={{ fontSize: 14 }}>Temperature</Text>} name="temperature" initialValue={0.7}>
                    <InputNumber min={0} max={2} step={0.1} style={{ width: '100%', ...EDITOR_FORM_CONTROL }} />
                  </Form.Item>
                </Col>
                <Col span={12}>
                  <Form.Item label={<Text style={{ fontSize: 14 }}>Max Tokens</Text>} name="max_tokens" initialValue={2000}>
                    <InputNumber min={100} max={10000} step={100} style={{ width: '100%', ...EDITOR_FORM_CONTROL }} />
                  </Form.Item>
                </Col>
              </Row>

              <Form.Item label={<Text style={{ fontSize: 14 }}>Tags</Text>} name="tags">
                <Select mode="tags" placeholder="Add tags, press Enter to confirm" style={EDITOR_FORM_CONTROL} />
              </Form.Item>

              <Form.Item
                label={<Text style={{ fontSize: 14 }}>Active Status</Text>}
                name="is_active"
                valuePropName="checked"
                initialValue={true}
                extra="Inactive templates will not be usable"
              >
                <Switch size="small" checkedChildren="Active" unCheckedChildren="Inactive" />
              </Form.Item>

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
              onVariableInsert={handleVariableInsert}
              currentCategoryId={currentEditingCategoryId}
              moduleCode={!isAdmin ? selectedModuleCode : undefined}
            />
          </Col>
        </Row>
      </Modal>
    );
  };

  // ── Test drawer ───────────────────────────────────────────────────────

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
          <Alert message="Test Instructions" description="Fill in test variable values, click run test to view LLM response effects" type="info" showIcon />

          <Card title="Template Information" size="small" style={cardStyleBehance} bodyStyle={{ padding: '16px 20px' }}>
            <Paragraph><Text strong>Template Code:</Text> {selectedTemplate.template_code}</Paragraph>
            <Paragraph><Text strong>Recommended Provider:</Text> {selectedTemplate.recommended_provider || 'Not set'}</Paragraph>
            <Paragraph><Text strong>Recommended Model:</Text> {selectedTemplate.recommended_model || 'Not set'}</Paragraph>
          </Card>

          <Form form={testForm} layout="vertical" onFinish={handleRunTest}>
            <Card title="Test Variables" size="small" style={cardStyleBehance} bodyStyle={{ padding: '16px 20px' }}>
              {Object.entries(selectedTemplate.variables || {}).map(([varName, varDef]) => (
                <Form.Item
                  key={varName}
                  label={<Space><Text>{varName}</Text>{varDef.required && <Tag color="red">Required</Tag>}<Tag>{varDef.type}</Tag></Space>}
                  name={varName}
                  rules={[{ required: varDef.required, message: `${varName} is required` }]}
                  extra={varDef.description}
                >
                  {varDef.type === 'string' ? <TextArea rows={3} placeholder={`Enter ${varName}`} />
                    : varDef.type === 'integer' || varDef.type === 'float' ? <InputNumber style={{ width: '100%' }} />
                    : varDef.type === 'boolean' ? <Switch />
                    : <Input placeholder={`Enter ${varName}`} />}
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
                title={<Space><Text>Test Result</Text>{testResult.success ? <CheckCircleOutlined style={{ color: '#52c41a' }} /> : <CloseCircleOutlined style={{ color: '#ff4d4f' }} />}</Space>}
                size="small" style={cardStyleBehance} bodyStyle={{ padding: '16px 20px' }}
              >
                {testResult.success && (
                  <Row gutter={16}>
                    <Col span={8}><Statistic title="Tokens Used" value={testResult.tokens_used || 0} /></Col>
                    <Col span={8}><Statistic title="Cost" value={testResult.cost || 0} precision={4} prefix="¥" /></Col>
                    <Col span={8}><Statistic title="Response Time" value={testResult.response_time || 0} precision={2} suffix="s" /></Col>
                  </Row>
                )}
                {testResult.error && <Alert message="Error" description={testResult.error} type="error" />}
              </Card>

              {testResult.rendered_prompt && (
                <Collapse defaultActiveKey={['1']}>
                  <Panel header="Rendered Prompt" key="1">
                    {testResult.rendered_prompt.system_prompt && (
                      <>
                        <Text strong>System Prompt:</Text>
                        <Paragraph>
                          <pre style={{ background: colors.codeBlockBg, padding: 12, borderRadius: 8, whiteSpace: 'pre-wrap' }}>
                            {testResult.rendered_prompt.system_prompt}
                          </pre>
                        </Paragraph>
                      </>
                    )}
                    <Text strong>User Prompt:</Text>
                    <Paragraph>
                      <pre style={{ background: colors.codeBlockBg, padding: 12, borderRadius: 8, whiteSpace: 'pre-wrap' }}>
                        {testResult.rendered_prompt.user_prompt}
                      </pre>
                    </Paragraph>
                  </Panel>
                </Collapse>
              )}

              {testResult.llm_response && (
                <Card title="LLM Response" size="small" style={cardStyleBehance} bodyStyle={{ padding: '16px 20px' }}>
                  <Paragraph>
                    <pre style={{ background: colors.codeBlockBg, padding: 12, borderRadius: 8, whiteSpace: 'pre-wrap' }}>
                      {testResult.llm_response}
                    </pre>
                  </Paragraph>
                </Card>
              )}

              {testResult.parsed_output && (
                <Card title="Parsed Output" size="small" style={cardStyleBehance} bodyStyle={{ padding: '16px 20px' }}>
                  <Paragraph>
                    <pre style={{ background: colors.codeBlockBg, padding: 12, borderRadius: 8, whiteSpace: 'pre-wrap' }}>
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

  // ── Page layout ───────────────────────────────────────────────────────

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
          {/* Toolbar */}
          <Row gutter={[16, 16]} align="middle" style={{ marginBottom: 16 }}>
            <Col flex="auto">
              <Input
                placeholder="Search template name or description..."
                prefix={<SearchOutlined />}
                value={searchText}
                onChange={e => setSearchText(e.target.value)}
                allowClear
                {...(isDark && { style: { backgroundColor: (colors as any).inputBg ?? '#1E202B' } })}
              />
            </Col>
            {isAdmin && (
              <Col>
                <Select
                  style={{ width: 160, ...(isDark ? { backgroundColor: (colors as any).inputBg } : {}) }}
                  value={scopeFilter}
                  onChange={setScopeFilter}
                >
                  <Option value="all">All Templates</Option>
                  <Option value="system">System Only</Option>
                  <Option value="user">User Only</Option>
                </Select>
              </Col>
            )}
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

          {/* Card grid */}
          {renderTemplateCards()}
        </Spin>

        {renderEditor()}
        {renderTestPanel()}

        {/* 「设为我的默认」确认弹窗 */}
        <Modal
          title={<Space><StarOutlined style={{ color: '#faad14' }} />Set as My Default Template</Space>}
          open={!!setDefaultModalTemplate}
          onCancel={() => { setSetDefaultModalTemplate(null); setSetDefaultModuleCode(undefined); }}
          onOk={handleConfirmSetDefault}
          okText="Set as Default"
          okButtonProps={{ loading: settingDefault, disabled: !setDefaultModuleCode }}
          cancelText="Cancel"
          styles={getModalStyles(themeMode)}
          width={480}
        >
          {setDefaultModalTemplate && (
            <Space direction="vertical" style={{ width: '100%' }} size="middle">
              <div>
                <Text>Set <Text strong>"{setDefaultModalTemplate.template_name}"</Text> as your preferred template for:</Text>
              </div>
              <Select
                style={{ width: '100%' }}
                placeholder="Select module"
                value={setDefaultModuleCode}
                onChange={(v: string) => setSetDefaultModuleCode(v)}
              >
                {modules.filter(m => m.configurable).map(m => (
                  <Option key={m.module_code} value={m.module_code}>
                    {m.module_name}
                    <Text type="secondary" style={{ fontSize: 12, marginLeft: 8 }}>— {m.description}</Text>
                  </Option>
                ))}
              </Select>
              <Alert
                type="info"
                showIcon
                message="After setting, all your generated daily reports will use this template instead of the system default."
              />
            </Space>
          )}
        </Modal>
      </div>
    </ConfigProvider>
  );
};

export default PromptTemplatesPage;
