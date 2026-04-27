/**
 * Prompt Templates Page — Phase 2 双态模型 (PR-C)
 *
 * 顶部 Tab:
 *   1. My Templates (所有用户)
 *      - 13 张卡片 (一 module 一张):
 *        7 张 USER_OWNED  → 可编辑 / 可重置 (POST .../reset)
 *        6 张 SYSTEM_FIXED → 只读 (admin 在 System Templates Tab 编辑)
 *      - 编辑只改 prompt 内容 + LLM 配置, 不再有 template_code / scope / module 选择
 *
 *   2. System Templates (仅 admin)
 *      - 列出所有 system-scope 模板, 包括各 module 的"默认 + 子变体"
 *        (例如 news_summary_v1 + news_cctv_summary_v1 都属于 news_summary module)
 *      - 编辑入口与 My Templates 共用同一组字段集
 *
 * 取消项 (相对旧版):
 *   - Create Template 按钮
 *   - Target Module / scope / template_code / category 表单字段
 *   - 用户偏好 (preference) 表 — 由"用户私有模板"取代
 *   - Set as Default Modal
 */

import React, { useEffect, useMemo, useState } from 'react';
import {
  Alert,
  Button,
  Card,
  Col,
  Drawer,
  Empty,
  Form,
  Input,
  InputNumber,
  message,
  Modal,
  Row,
  Select,
  Space,
  Spin,
  Tabs,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import {
  EditOutlined,
  ReloadOutlined,
  RobotOutlined,
  LockOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import promptTemplateService, {
  MyTemplateItem,
  PromptTemplate,
  PromptTemplateDetail,
  UpdateMyTemplatePayload,
} from '../services/promptTemplateService';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

const { Title, Paragraph, Text } = Typography;
const { TextArea } = Input;

const THEME = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    codeBg: '#f5f5f5',
    accent: '#1890ff',
  },
  dark: {
    pageBg: '#1E202B',
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    text: '#FFFFFF',
    textSecondary: '#A0A3AD',
    codeBg: '#0f1420',
    accent: '#60a5fa',
  },
} as const;

const PROVIDER_MODELS: Record<string, string[]> = {
  qwen: ['qwen-turbo', 'qwen-plus', 'qwen-max', 'qwen-max-longcontext', 'qwen3-turbo', 'qwen3-plus', 'qwen3-max'],
  doubao: ['doubao-seed-1-6-250615'],
  glm: ['glm-4.6', 'glm-4', 'glm-4-flash'],
};

interface EditableForm {
  system_prompt: string;
  user_prompt_template: string;
  recommended_provider: string;
  recommended_model: string;
  temperature: number;
  max_tokens: number;
}

const PromptTemplatesPage: React.FC = () => {
  const { user } = useAuthStore();
  const isAdmin = !!user?.is_admin;
  const { themeMode } = useDashboardSettings();
  const colors = THEME[themeMode];

  // ── State: My Templates ────────────────────────────────────────────────
  const [activeTab, setActiveTab] = useState<'my' | 'system'>('my');
  const [myItems, setMyItems] = useState<MyTemplateItem[]>([]);
  const [myLoading, setMyLoading] = useState(false);

  // ── State: System Templates (admin only) ───────────────────────────────
  const [sysItems, setSysItems] = useState<PromptTemplate[]>([]);
  const [sysLoading, setSysLoading] = useState(false);

  // ── State: Editor Drawer ───────────────────────────────────────────────
  type EditingTarget =
    | { kind: 'my'; item: MyTemplateItem }
    | { kind: 'system'; template: PromptTemplateDetail };
  const [editing, setEditing] = useState<EditingTarget | null>(null);
  const [drawerVisible, setDrawerVisible] = useState(false);
  const [saving, setSaving] = useState(false);
  const [form] = Form.useForm<EditableForm>();
  const watchedProvider = Form.useWatch('recommended_provider', form);

  const availableModels = useMemo(
    () => PROVIDER_MODELS[watchedProvider || ''] || [],
    [watchedProvider],
  );

  // ── Effects ────────────────────────────────────────────────────────────
  useEffect(() => {
    loadMyTemplates();
  }, []);

  useEffect(() => {
    if (activeTab === 'system' && isAdmin) loadSystemTemplates();
  }, [activeTab, isAdmin]);

  const loadMyTemplates = async () => {
    setMyLoading(true);
    try {
      const items = await promptTemplateService.listMyTemplates();
      setMyItems(items);
    } catch (err: any) {
      message.error(`加载模板失败: ${err?.response?.data?.detail || err?.message || ''}`);
    } finally {
      setMyLoading(false);
    }
  };

  const loadSystemTemplates = async () => {
    setSysLoading(true);
    try {
      const items = await promptTemplateService.listSystemTemplates();
      setSysItems(items);
    } catch (err: any) {
      message.error(`加载系统模板失败: ${err?.response?.data?.detail || err?.message || ''}`);
    } finally {
      setSysLoading(false);
    }
  };

  // ── Edit My Template ───────────────────────────────────────────────────
  const openEditMy = (item: MyTemplateItem) => {
    setEditing({ kind: 'my', item });
    form.setFieldsValue({
      system_prompt: item.system_prompt || '',
      user_prompt_template: item.user_prompt_template || '',
      recommended_provider: item.recommended_provider || 'qwen',
      recommended_model: item.recommended_model || '',
      temperature: item.temperature ?? 0.7,
      max_tokens: item.max_tokens ?? 2000,
    });
    setDrawerVisible(true);
  };

  // ── Edit System Template (admin) ───────────────────────────────────────
  const openEditSystem = async (template: PromptTemplate) => {
    try {
      const detail = await promptTemplateService.getTemplate(template.id);
      setEditing({ kind: 'system', template: detail });
      form.setFieldsValue({
        system_prompt: detail.system_prompt || '',
        user_prompt_template: detail.user_prompt_template || '',
        recommended_provider: detail.recommended_provider || 'qwen',
        recommended_model: detail.recommended_model || '',
        temperature: detail.temperature ?? 0.7,
        max_tokens: detail.max_tokens ?? 2000,
      });
      setDrawerVisible(true);
    } catch (err: any) {
      message.error(`加载模板详情失败: ${err?.message || ''}`);
    }
  };

  const handleSave = async (values: EditableForm) => {
    if (!editing) return;
    setSaving(true);
    try {
      const payload: UpdateMyTemplatePayload = {
        system_prompt: values.system_prompt,
        user_prompt_template: values.user_prompt_template,
        recommended_provider: values.recommended_provider,
        recommended_model: values.recommended_model,
        temperature: values.temperature,
        max_tokens: values.max_tokens,
      };

      if (editing.kind === 'my') {
        await promptTemplateService.updateMyTemplate(editing.item.module_code, payload);
        message.success(`已更新 "${editing.item.module_name}" 模板`);
        await loadMyTemplates();
      } else {
        await promptTemplateService.updateSystemTemplate(editing.template.id, payload);
        message.success(`已更新系统模板 "${editing.template.template_name}"`);
        await loadSystemTemplates();
      }
      setDrawerVisible(false);
      setEditing(null);
    } catch (err: any) {
      message.error(`保存失败: ${err?.response?.data?.detail || err?.message || ''}`);
    } finally {
      setSaving(false);
    }
  };

  const handleReset = (item: MyTemplateItem) => {
    Modal.confirm({
      title: '重置为系统默认?',
      content: `这会用系统默认模板覆盖你在 "${item.module_name}" 的当前内容. 此操作不可恢复.`,
      okText: '确认重置',
      okType: 'danger',
      cancelText: '取消',
      onOk: async () => {
        try {
          await promptTemplateService.resetMyTemplate(item.module_code);
          message.success(`"${item.module_name}" 已重置为系统默认`);
          await loadMyTemplates();
        } catch (err: any) {
          message.error(`重置失败: ${err?.response?.data?.detail || err?.message || ''}`);
        }
      },
    });
  };

  // ── Render: a single My Template card ──────────────────────────────────
  const renderMyCard = (item: MyTemplateItem) => {
    const isOwned = item.kind === 'user_owned';
    const isReadonly = !isOwned && !isAdmin;

    return (
      <Card
        key={item.module_code}
        size="small"
        style={{
          background: colors.cardBg,
          border: `1px solid ${colors.cardBorder}`,
          borderRadius: 10,
          height: '100%',
        }}
        bodyStyle={{ padding: 16 }}
      >
        <Space direction="vertical" size={10} style={{ width: '100%' }}>
          <Space style={{ justifyContent: 'space-between', width: '100%' }} align="start">
            <div>
              <Space size={6}>
                <Text strong style={{ color: colors.text, fontSize: 15 }}>
                  {item.module_name}
                </Text>
                {isOwned ? (
                  <Tag color="blue" style={{ margin: 0 }}>我的</Tag>
                ) : (
                  <Tag icon={<LockOutlined />} color="default" style={{ margin: 0 }}>
                    系统级 {isAdmin ? '' : '(只读)'}
                  </Tag>
                )}
              </Space>
              <div style={{ fontSize: 12, color: colors.textSecondary, marginTop: 2 }}>
                {item.module_description}
              </div>
            </div>
            <Space>
              {isOwned && (
                <Tooltip title="重置为系统默认">
                  <Button
                    size="small"
                    icon={<ReloadOutlined />}
                    onClick={() => handleReset(item)}
                  />
                </Tooltip>
              )}
              <Button
                type="primary"
                size="small"
                icon={<EditOutlined />}
                disabled={isReadonly}
                onClick={() => openEditMy(item)}
              >
                {isReadonly ? '只读' : '编辑'}
              </Button>
            </Space>
          </Space>

          <div
            style={{
              background: colors.codeBg,
              border: `1px solid ${colors.cardBorder}`,
              borderRadius: 6,
              padding: 8,
              fontSize: 12,
              color: colors.textSecondary,
              maxHeight: 80,
              overflow: 'hidden',
              fontFamily: 'monospace',
              whiteSpace: 'pre-wrap',
              lineHeight: 1.4,
            }}
          >
            {(item.user_prompt_template || '').slice(0, 220)}
            {item.user_prompt_template && item.user_prompt_template.length > 220 ? '…' : ''}
          </div>

          <Space size={4} wrap>
            {item.recommended_provider && (
              <Tag color="purple" style={{ fontSize: 10, margin: 0 }}>
                <RobotOutlined /> {item.recommended_provider}
              </Tag>
            )}
            {item.recommended_model && (
              <Tag color="cyan" style={{ fontSize: 10, margin: 0 }}>
                {item.recommended_model}
              </Tag>
            )}
            <Tag style={{ fontSize: 10, margin: 0 }}>Temp: {item.temperature}</Tag>
            <Tag style={{ fontSize: 10, margin: 0 }}>Max: {item.max_tokens}</Tag>
          </Space>
        </Space>
      </Card>
    );
  };

  // ── Render: System Templates table-like list ──────────────────────────
  const renderSystemList = () => {
    if (sysLoading) return <Spin />;
    if (!sysItems.length) return <Empty description="无系统模板" />;

    return (
      <Space direction="vertical" size={10} style={{ width: '100%' }}>
        {sysItems.map((tpl) => (
          <Card
            key={tpl.id}
            size="small"
            style={{
              background: colors.cardBg,
              border: `1px solid ${colors.cardBorder}`,
              borderRadius: 8,
            }}
            bodyStyle={{ padding: 14 }}
          >
            <Row align="middle" gutter={12}>
              <Col flex="auto">
                <Space size={6}>
                  <Text strong style={{ color: colors.text }}>
                    {tpl.template_name}
                  </Text>
                  <Tag color="geekblue">{tpl.template_code}</Tag>
                  {tpl.module_code && <Tag color="cyan">{tpl.module_code}</Tag>}
                </Space>
                <div style={{ fontSize: 12, color: colors.textSecondary, marginTop: 2 }}>
                  {tpl.description || '—'}
                </div>
              </Col>
              <Col>
                <Button
                  type="primary"
                  size="small"
                  icon={<EditOutlined />}
                  onClick={() => openEditSystem(tpl)}
                >
                  编辑
                </Button>
              </Col>
            </Row>
          </Card>
        ))}
      </Space>
    );
  };

  // ── Render: Editor Drawer ──────────────────────────────────────────────
  const editingTitle = editing
    ? editing.kind === 'my'
      ? `编辑: ${editing.item.module_name}`
      : `[Admin] 编辑系统模板: ${editing.template.template_name}`
    : '';

  const renderEditor = () => (
    <Drawer
      title={editingTitle}
      width={720}
      open={drawerVisible}
      onClose={() => {
        setDrawerVisible(false);
        setEditing(null);
      }}
      destroyOnClose
      footer={
        <Space style={{ width: '100%', justifyContent: 'flex-end' }}>
          <Button onClick={() => setDrawerVisible(false)}>取消</Button>
          <Button type="primary" onClick={() => form.submit()} loading={saving}>
            保存
          </Button>
        </Space>
      }
    >
      {editing && (
        <Form form={form} layout="vertical" onFinish={handleSave}>
          {editing.kind === 'my' && (
            <Alert
              type="info"
              showIcon
              message={`你正在编辑你自己的 "${editing.item.module_name}" 私有模板。其他用户不会受影响。`}
              style={{ marginBottom: 16 }}
            />
          )}
          {editing.kind === 'system' && (
            <Alert
              type="warning"
              showIcon
              message={`你正在编辑系统模板 "${editing.template.template_name}"，所有未自定义的用户都会立即使用这份新内容。`}
              style={{ marginBottom: 16 }}
            />
          )}

          <Form.Item
            label="System Prompt"
            name="system_prompt"
            extra="LLM 的 system 角色指令，定义助手身份与基调。"
          >
            <TextArea rows={4} placeholder="（可选）" />
          </Form.Item>

          <Form.Item
            label="User Prompt Template"
            name="user_prompt_template"
            rules={[{ required: true, message: '不能为空' }]}
            extra="支持 {{变量}} 占位符；用户调用时会被自动填充。"
          >
            <TextArea rows={12} />
          </Form.Item>

          <Row gutter={12}>
            <Col span={12}>
              <Form.Item label="Provider" name="recommended_provider">
                <Select
                  options={Object.keys(PROVIDER_MODELS).map((k) => ({ label: k, value: k }))}
                />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="Model" name="recommended_model">
                <Select
                  options={availableModels.map((m) => ({ label: m, value: m }))}
                  placeholder="选择 provider 后可选 model"
                />
              </Form.Item>
            </Col>
          </Row>

          <Row gutter={12}>
            <Col span={12}>
              <Form.Item label="Temperature" name="temperature">
                <InputNumber min={0} max={2} step={0.1} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="Max Tokens" name="max_tokens">
                <InputNumber min={1} max={8000} step={100} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>
        </Form>
      )}
    </Drawer>
  );

  // ── My Templates split: USER_OWNED first, SYSTEM_FIXED below ───────────
  const ownedItems = myItems.filter((x) => x.kind === 'user_owned');
  const systemItems = myItems.filter((x) => x.kind === 'system_fixed');

  return (
    <div style={{ padding: 24, background: colors.pageBg, minHeight: '100vh' }}>
      <Title level={3} style={{ color: colors.text, marginBottom: 4 }}>
        Prompt Templates
      </Title>
      <Paragraph type="secondary" style={{ marginBottom: 20 }}>
        每个功能模块对应你自己的一份私有模板（My Templates 中可编辑/重置）；
        系统级模板对所有用户共享，仅管理员可改 prompt 内容。
      </Paragraph>

      <Tabs
        activeKey={activeTab}
        onChange={(k) => setActiveTab(k as 'my' | 'system')}
        items={[
          {
            key: 'my',
            label: (
              <Space size={4}>
                <ThunderboltOutlined /> My Templates
              </Space>
            ),
            children: (
              <Spin spinning={myLoading}>
                <Title level={5} style={{ color: colors.text, marginTop: 4 }}>
                  我的（可编辑）
                </Title>
                {!ownedItems.length && !myLoading && (
                  <Empty description="未找到可编辑模板" />
                )}
                <Row gutter={[12, 12]}>
                  {ownedItems.map((item) => (
                    <Col xs={24} md={12} xl={8} key={item.module_code}>
                      {renderMyCard(item)}
                    </Col>
                  ))}
                </Row>

                <Title level={5} style={{ color: colors.text, marginTop: 24 }}>
                  系统级（{isAdmin ? '可在 System Templates Tab 编辑' : '只读'}）
                </Title>
                <Row gutter={[12, 12]}>
                  {systemItems.map((item) => (
                    <Col xs={24} md={12} xl={8} key={item.module_code}>
                      {renderMyCard(item)}
                    </Col>
                  ))}
                </Row>
              </Spin>
            ),
          },
          ...(isAdmin
            ? [
                {
                  key: 'system' as const,
                  label: (
                    <Space size={4}>
                      <LockOutlined /> System Templates (Admin)
                    </Space>
                  ),
                  children: (
                    <>
                      <Alert
                        type="warning"
                        showIcon
                        message="改动会立刻生效到所有未自定义对应模块的用户。请谨慎编辑。"
                        style={{ marginBottom: 12 }}
                      />
                      {renderSystemList()}
                    </>
                  ),
                },
              ]
            : []),
        ]}
      />

      {renderEditor()}
    </div>
  );
};

export default PromptTemplatesPage;
