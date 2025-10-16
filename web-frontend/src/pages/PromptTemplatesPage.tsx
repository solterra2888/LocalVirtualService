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
  PromptTemplate,
  PromptTemplateDetail,
  TemplateCreateData,
  TestTemplateResponse
} from '../services/promptTemplateService';

const { Title, Paragraph, Text } = Typography;
const { Option } = Select;
const { TextArea } = Input;
const { Panel } = Collapse;

const PromptTemplatesPage: React.FC = () => {
  // 状态管理
  const [categories, setCategories] = useState<PromptCategory[]>([]);
  const [templates, setTemplates] = useState<PromptTemplate[]>([]);
  const [selectedCategory, setSelectedCategory] = useState<number | null>(null);
  const [selectedTemplate, setSelectedTemplate] = useState<PromptTemplateDetail | null>(null);
  const [loading, setLoading] = useState<boolean>(true);
  const [searchText, setSearchText] = useState<string>('');
  const [scopeFilter, setScopeFilter] = useState<string>('all');

  // 模态框状态
  const [isEditorVisible, setIsEditorVisible] = useState<boolean>(false);
  const [isTestVisible, setIsTestVisible] = useState<boolean>(false);
  const [editingTemplate, setEditingTemplate] = useState<PromptTemplateDetail | null>(null);
  const [testing, setTesting] = useState<boolean>(false);
  const [testResult, setTestResult] = useState<TestTemplateResponse | null>(null);

  const [form] = Form.useForm();
  const [testForm] = Form.useForm();

  // 加载模板列表
  const loadTemplates = useCallback(async () => {
    try {
      const params: any = {};
      if (selectedCategory) params.category_id = selectedCategory;
      if (searchText) params.search = searchText;
      if (scopeFilter !== 'all') params.scope = scopeFilter;

      const templatesData = await promptTemplateService.listTemplates(params);
      setTemplates(templatesData);
    } catch (error: any) {
      message.error('加载模板失败: ' + (error.message || '未知错误'));
    }
  }, [selectedCategory, searchText, scopeFilter]);

  // 加载数据
  const loadData = useCallback(async () => {
    try {
      setLoading(true);
      const [categoriesData, templatesData] = await Promise.all([
        promptTemplateService.getCategories(),
        promptTemplateService.listTemplates()
      ]);
      setCategories(categoriesData);
      setTemplates(templatesData);
    } catch (error: any) {
      message.error('加载数据失败: ' + (error.message || '未知错误'));
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

  // 创建/编辑模板
  const handleCreateTemplate = () => {
    setEditingTemplate(null);
    form.resetFields();
    setIsEditorVisible(true);
  };

  const handleEditTemplate = async (template: PromptTemplate) => {
    try {
      const detail = await promptTemplateService.getTemplate(template.id);
      setEditingTemplate(detail);
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
      message.error('加载模板详情失败: ' + (error.message || '未知错误'));
    }
  };

  const handleSaveTemplate = async (values: any) => {
    try {
      if (editingTemplate) {
        // 更新
        await promptTemplateService.updateTemplate(editingTemplate.id, values);
        message.success('模板更新成功');
      } else {
        // 创建
        await promptTemplateService.createTemplate(values as TemplateCreateData);
        message.success('模板创建成功');
      }
      setIsEditorVisible(false);
      loadTemplates();
    } catch (error: any) {
      message.error('保存失败: ' + (error.message || '未知错误'));
    }
  };

  // 删除模板
  const handleDeleteTemplate = (template: PromptTemplate) => {
    Modal.confirm({
      title: '确认删除',
      content: `确定要删除模板 "${template.template_name}" 吗？`,
      okText: '删除',
      okType: 'danger',
      cancelText: '取消',
      onOk: async () => {
        try {
          await promptTemplateService.deleteTemplate(template.id);
          message.success('模板删除成功');
          loadTemplates();
        } catch (error: any) {
          message.error('删除失败: ' + (error.message || '未知错误'));
        }
      }
    });
  };

  // 测试模板
  const handleTestTemplate = async (template: PromptTemplate) => {
    try {
      const detail = await promptTemplateService.getTemplate(template.id);
      setSelectedTemplate(detail);
      
      // 为测试表单设置默认值
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
      message.error('加载模板详情失败: ' + (error.message || '未知错误'));
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
        message.success('测试成功');
      } else {
        message.error('测试失败: ' + result.error);
      }
    } catch (error: any) {
      message.error('测试失败: ' + (error.message || '未知错误'));
    } finally {
      setTesting(false);
    }
  };

  // 导出模板
  const handleExportTemplate = async (template: PromptTemplate) => {
    try {
      await promptTemplateService.downloadTemplateExport(template.id, template.template_code);
      message.success('导出成功');
    } catch (error: any) {
      message.error('导出失败: ' + (error.message || '未知错误'));
    }
  };

  // 渲染分类列表
  const renderCategories = () => (
    <Card 
      title="模板分类" 
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
          全部
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

  // 渲染模板列表
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
                <Tooltip title="编辑">
                  <EditOutlined onClick={() => handleEditTemplate(template)} />
                </Tooltip>,
                <Tooltip title="测试">
                  <ExperimentOutlined onClick={() => handleTestTemplate(template)} />
                </Tooltip>,
                <Tooltip title="导出">
                  <DownloadOutlined onClick={() => handleExportTemplate(template)} />
                </Tooltip>,
                <Tooltip title="删除">
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
                        {template.scope === 'system' ? '系统' : 
                         template.scope === 'user' ? '私有' : '共享'}
                      </Tag>
                    </Space>
                    <Text type="secondary">{template.description}</Text>
                    <Space size="small">
                      <Text type="secondary" style={{ fontSize: '12px' }}>
                        模板代码: {template.template_code}
                      </Text>
                      <Divider type="vertical" />
                      <Text type="secondary" style={{ fontSize: '12px' }}>
                        分类: {template.category_name}
                      </Text>
                    </Space>
                  </Space>
                </Col>
                <Col span={6} style={{ textAlign: 'right' }}>
                  <Space direction="vertical" size="small" align="end">
                    <Statistic 
                      title="使用次数" 
                      value={template.usage_count} 
                      valueStyle={{ fontSize: '16px' }}
                    />
                    {template.success_rate !== undefined && template.success_rate !== null && (
                      <Tag color={template.success_rate >= 90 ? 'success' : 'warning'}>
                        成功率: {template.success_rate.toFixed(1)}%
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
            <Empty description="暂无模板" />
          )}
        </Space>
      </div>
    );
  };

  // 渲染编辑器
  const renderEditor = () => (
    <Modal
      title={editingTemplate ? '编辑模板' : '创建模板'}
      open={isEditorVisible}
      onCancel={() => setIsEditorVisible(false)}
      width={900}
      footer={null}
    >
      <Form
        form={form}
        layout="vertical"
        onFinish={handleSaveTemplate}
      >
        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              label="模板名称"
              name="template_name"
              rules={[{ required: true, message: '请输入模板名称' }]}
            >
              <Input placeholder="例如：新闻摘要生成" />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label="模板代码"
              name="template_code"
              rules={[{ required: true, message: '请输入模板代码' }]}
            >
              <Input placeholder="例如：news_summary_v1" disabled={!!editingTemplate} />
            </Form.Item>
          </Col>
        </Row>

        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              label="所属分类"
              name="category_id"
              rules={[{ required: true, message: '请选择分类' }]}
            >
              <Select placeholder="选择分类">
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
              label="范围"
              name="scope"
              initialValue="user"
            >
              <Select>
                <Option value="user">私有</Option>
                <Option value="shared">共享</Option>
                <Option value="system">系统</Option>
              </Select>
            </Form.Item>
          </Col>
        </Row>

        <Form.Item
          label="描述"
          name="description"
        >
          <TextArea rows={2} placeholder="简要描述这个模板的用途" />
        </Form.Item>

        <Form.Item
          label="系统提示词 (可选)"
          name="system_prompt"
        >
          <TextArea 
            rows={4} 
            placeholder="定义AI的角色和行为，例如：你是一个专业的财经分析师..." 
          />
        </Form.Item>

        <Form.Item
          label="用户提示词模板"
          name="user_prompt_template"
          rules={[{ required: true, message: '请输入用户提示词模板' }]}
          extra="使用 {{variable_name}} 格式定义变量"
        >
          <TextArea 
            rows={8} 
            placeholder="请分析以下新闻：\n\n标题：{{title}}\n内容：{{content}}" 
          />
        </Form.Item>

        <Divider>推荐配置</Divider>

        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              label="推荐提供商"
              name="recommended_provider"
            >
              <Select placeholder="选择LLM提供商" allowClear>
                <Option value="qwen">Qwen</Option>
                <Option value="doubao">Doubao</Option>
                <Option value="glm">GLM</Option>
              </Select>
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label="推荐模型"
              name="recommended_model"
            >
              <Input placeholder="例如：qwen-plus" />
            </Form.Item>
          </Col>
        </Row>

        <Row gutter={16}>
          <Col span={12}>
            <Form.Item
              label="Temperature"
              name="temperature"
              initialValue={0.7}
            >
              <InputNumber min={0} max={2} step={0.1} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
          <Col span={12}>
            <Form.Item
              label="Max Tokens"
              name="max_tokens"
              initialValue={2000}
            >
              <InputNumber min={100} max={10000} step={100} style={{ width: '100%' }} />
            </Form.Item>
          </Col>
        </Row>

        <Form.Item
          label="标签"
          name="tags"
        >
          <Select mode="tags" placeholder="添加标签，回车确认" />
        </Form.Item>

        <Form.Item
          label="公开分享"
          name="is_public"
          valuePropName="checked"
          initialValue={false}
        >
          <Switch />
        </Form.Item>

        <Form.Item>
          <Space>
            <Button type="primary" htmlType="submit">
              保存
            </Button>
            <Button onClick={() => setIsEditorVisible(false)}>
              取消
            </Button>
          </Space>
        </Form.Item>
      </Form>
    </Modal>
  );

  // 渲染测试面板
  const renderTestPanel = () => {
    if (!selectedTemplate) return null;

    return (
      <Drawer
        title={`测试模板: ${selectedTemplate.template_name}`}
        placement="right"
        width={720}
        open={isTestVisible}
        onClose={() => setIsTestVisible(false)}
      >
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          <Alert
            message="测试说明"
            description="填入测试变量值，点击运行测试查看LLM响应效果"
            type="info"
            showIcon
          />

          <Card title="模板信息" size="small">
            <Paragraph>
              <Text strong>模板代码:</Text> {selectedTemplate.template_code}
            </Paragraph>
            <Paragraph>
              <Text strong>推荐提供商:</Text> {selectedTemplate.recommended_provider || '未设置'}
            </Paragraph>
            <Paragraph>
              <Text strong>推荐模型:</Text> {selectedTemplate.recommended_model || '未设置'}
            </Paragraph>
          </Card>

          <Form
            form={testForm}
            layout="vertical"
            onFinish={handleRunTest}
          >
            <Card title="测试变量" size="small">
              {Object.entries(selectedTemplate.variables || {}).map(([varName, varDef]) => (
                <Form.Item
                  key={varName}
                  label={
                    <Space>
                      <Text>{varName}</Text>
                      {varDef.required && <Tag color="red">必需</Tag>}
                      <Tag>{varDef.type}</Tag>
                    </Space>
                  }
                  name={varName}
                  rules={[
                    { required: varDef.required, message: `${varName} 是必需的` }
                  ]}
                  extra={varDef.description}
                >
                  {varDef.type === 'string' ? (
                    <TextArea rows={3} placeholder={`输入 ${varName}`} />
                  ) : varDef.type === 'integer' || varDef.type === 'float' ? (
                    <InputNumber style={{ width: '100%' }} />
                  ) : varDef.type === 'boolean' ? (
                    <Switch />
                  ) : (
                    <Input placeholder={`输入 ${varName}`} />
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
                运行测试
              </Button>
            </Form.Item>
          </Form>

          {testResult && (
            <Space direction="vertical" style={{ width: '100%' }} size="middle">
              <Card 
                title={
                  <Space>
                    <Text>测试结果</Text>
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
                          title="Tokens使用" 
                          value={testResult.tokens_used || 0} 
                        />
                      </Col>
                      <Col span={8}>
                        <Statistic 
                          title="成本" 
                          value={testResult.cost || 0} 
                          precision={4}
                          prefix="¥"
                        />
                      </Col>
                      <Col span={8}>
                        <Statistic 
                          title="响应时间" 
                          value={testResult.response_time || 0} 
                          precision={2}
                          suffix="秒"
                        />
                      </Col>
                    </Row>
                  </>
                )}
                {testResult.error && (
                  <Alert message="错误" description={testResult.error} type="error" />
                )}
              </Card>

              {testResult.rendered_prompt && (
                <Collapse defaultActiveKey={['1']}>
                  <Panel header="渲染后的Prompt" key="1">
                    {testResult.rendered_prompt.system_prompt && (
                      <>
                        <Text strong>系统提示词:</Text>
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
                    <Text strong>用户提示词:</Text>
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
                <Card title="LLM响应" size="small">
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
                <Card title="解析后的输出" size="small">
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
        <FileTextOutlined /> Prompt模板管理
      </Title>
      <Paragraph type="secondary">
        管理和优化你的LLM Prompt模板，支持变量化、版本控制和在线测试
      </Paragraph>

      <Spin spinning={loading}>
        <Row gutter={[16, 16]}>
          {/* 左侧：分类列表 */}
          <Col xs={24} md={6}>
            {renderCategories()}
          </Col>

          {/* 右侧：模板列表和操作 */}
          <Col xs={24} md={18}>
            <Card>
              <Space direction="vertical" style={{ width: '100%' }} size="middle">
                {/* 工具栏 */}
                <Row gutter={[16, 16]} align="middle">
                  <Col flex="auto">
                    <Input
                      placeholder="搜索模板名称或描述..."
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
                      <Option value="all">全部范围</Option>
                      <Option value="system">系统模板</Option>
                      <Option value="user">我的模板</Option>
                      <Option value="shared">共享模板</Option>
                    </Select>
                  </Col>
                  <Col>
                    <Button 
                      type="primary" 
                      icon={<PlusOutlined />}
                      onClick={handleCreateTemplate}
                    >
                      创建模板
                    </Button>
                  </Col>
                </Row>

                {/* 模板列表 */}
                {renderTemplateList()}
              </Space>
            </Card>
          </Col>
        </Row>
      </Spin>

      {/* 编辑器模态框 */}
      {renderEditor()}

      {/* 测试面板 */}
      {renderTestPanel()}
    </div>
  );
};

export default PromptTemplatesPage;

