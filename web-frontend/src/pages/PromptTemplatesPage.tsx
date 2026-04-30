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

import React, { useEffect, useMemo, useRef, useState } from 'react';
import type { TextAreaRef } from 'antd/es/input/TextArea';
import {
  Alert,
  App,
  Button,
  Card,
  Col,
  ConfigProvider,
  Drawer,
  Empty,
  Form,
  Input,
  InputNumber,
  Row,
  Select,
  Space,
  Spin,
  Tabs,
  Tag,
  Tooltip,
  Typography,
  Grid,
  theme as antdTheme,
} from 'antd';
import {
  ReloadOutlined,
  SettingOutlined,
  RobotOutlined,
  LockOutlined,
  ThunderboltOutlined,
  ExperimentOutlined,
} from '@ant-design/icons';
import promptTemplateService, {
  ModuleInfo,
  MyTemplateItem,
  PromptTemplate,
  PromptTemplateDetail,
  UpdateMyTemplatePayload,
} from '../services/promptTemplateService';
import InsertVariablePopover from '../components/InsertVariablePopover';
import { useAuthStore } from '../store/authStore';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';

const { Title, Paragraph, Text } = Typography;
const { TextArea } = Input;
const { useBreakpoint } = Grid;

// 与 Scheduler Management 页一致的外层容器圆角/阴影
const CARD_STYLE_BEHANCE = {
  borderRadius: 12,
  boxShadow: '0 4px 24px rgba(0, 0, 0, 0.12)',
  border: '1px solid #f0f0f0',
} as const;
const CARD_STYLE_BEHANCE_DARK = {
  borderRadius: 12,
  boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)',
  border: '1px solid rgba(255, 255, 255, 0.06)',
} as const;

const THEME = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    codeBg: '#f5f5f5',
    accent: '#1890ff',
    hoverRowBg: 'rgba(0, 0, 0, 0.02)',
  },
  dark: {
    pageBg: '#1E202B',
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    text: '#FFFFFF',
    textSecondary: '#A0A3AD',
    codeBg: '#0f1420',
    accent: '#60a5fa',
    hoverRowBg: 'rgba(255, 255, 255, 0.06)',
  },
} as const;

const PROVIDER_MODELS: Record<string, string[]> = {
  qwen: [
    'qwen-turbo', 'qwen-plus', 'qwen-max', 'qwen-max-longcontext',
    'qwen3-max',
    'qwen3.6-flash', 'qwen3.6-plus', 'qwen3.6-max-preview',
  ],
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

const PromptTemplatesPageContent: React.FC = () => {
  const { modal, message: appMessage } = App.useApp();
  const { user } = useAuthStore();
  const isAdmin = !!user?.is_admin;
  const { themeMode } = useDashboardSettings();
  const colors = THEME[themeMode];
  const isDark = themeMode === 'dark';
  const cardStyle = isDark ? CARD_STYLE_BEHANCE_DARK : CARD_STYLE_BEHANCE;
  const screens = useBreakpoint();
  const isLgUp = !!screens?.lg;

  // ── State: My Templates ────────────────────────────────────────────────
  const [activeTab, setActiveTab] = useState<'my' | 'system'>('my');
  const [myItems, setMyItems] = useState<MyTemplateItem[]>([]);
  const [myLoading, setMyLoading] = useState(false);

  // ── State: System Templates (admin only) ───────────────────────────────
  const [sysItems, setSysItems] = useState<PromptTemplate[]>([]);
  const [sysLoading, setSysLoading] = useState(false);
  // PR-C followup: 模块配置表用于把 system 模板分成 "全局共享 (configurable=false)"
  // 和 "USER_OWNED 种子模板 (configurable=true)" 两组, 让 admin 一目了然
  // 哪些编辑会立即生效, 哪些仅作种子.
  const [modules, setModules] = useState<ModuleInfo[]>([]);

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

  // 当前正在编辑的模板的 variables 字典 (供 Insert Variable 用)
  const editingVariables = useMemo(() => {
    if (!editing) return {};
    return (editing.kind === 'my'
      ? editing.item.variables
      : editing.template.variables) as Record<string, any> | undefined;
  }, [editing]);

  // User Prompt Template 的 TextArea ref, 用于 Insert Variable 按光标插入
  const promptTextAreaRef = useRef<TextAreaRef>(null);

  const insertTokenAtCursor = (token: string) => {
    const textarea = promptTextAreaRef.current?.resizableTextArea?.textArea;
    const current = form.getFieldValue('user_prompt_template') || '';
    if (!textarea) {
      form.setFieldValue('user_prompt_template', `${current}${token}`);
      return;
    }
    const start = textarea.selectionStart ?? current.length;
    const end = textarea.selectionEnd ?? current.length;
    const next = current.slice(0, start) + token + current.slice(end);
    form.setFieldValue('user_prompt_template', next);
    requestAnimationFrame(() => {
      textarea.focus();
      const cursor = start + token.length;
      try {
        textarea.setSelectionRange(cursor, cursor);
      } catch {
        // ignore
      }
    });
  };

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
      appMessage.error(`加载模板失败: ${err?.response?.data?.detail || err?.message || ''}`);
    } finally {
      setMyLoading(false);
    }
  };

  const loadSystemTemplates = async () => {
    setSysLoading(true);
    try {
      const [items, mods] = await Promise.all([
        promptTemplateService.listSystemTemplates(),
        promptTemplateService.getModules(),
      ]);
      setSysItems(items);
      setModules(mods);
    } catch (err: any) {
      appMessage.error(`加载系统模板失败: ${err?.response?.data?.detail || err?.message || ''}`);
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
      appMessage.error(`加载模板详情失败: ${err?.message || ''}`);
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
        appMessage.success(`已更新 "${editing.item.module_name}" 模板`);
        await loadMyTemplates();
      } else {
        await promptTemplateService.updateSystemTemplate(editing.template.id, payload);
        appMessage.success(`已更新系统模板 "${editing.template.template_name}"`);
        await loadSystemTemplates();
      }
      setDrawerVisible(false);
      setEditing(null);
    } catch (err: any) {
      appMessage.error(`保存失败: ${err?.response?.data?.detail || err?.message || ''}`);
    } finally {
      setSaving(false);
    }
  };

  const handleReset = (item: MyTemplateItem) => {
    modal.confirm({
      title: '重置为系统默认?',
      content: `这会用系统默认模板覆盖你在 "${item.module_name}" 的当前内容. 此操作不可恢复.`,
      okText: '确认重置',
      okType: 'danger',
      cancelText: '取消',
      onOk: async () => {
        try {
          await promptTemplateService.resetMyTemplate(item.module_code);
          appMessage.success(`"${item.module_name}" 已重置为系统默认`);
          await loadMyTemplates();
        } catch (err: any) {
          appMessage.error(`重置失败: ${err?.response?.data?.detail || err?.message || ''}`);
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
                {!isOwned && (
                  <Tag icon={<LockOutlined />} color="default" style={{ margin: 0 }}>
                    系统级 {isAdmin ? '' : '(只读)'}
                  </Tag>
                )}
              </Space>
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
              <Tooltip title={isReadonly ? '只读' : '编辑'}>
                <Button
                  type="default"
                  size="small"
                  icon={<SettingOutlined />}
                  disabled={isReadonly}
                  onClick={() => openEditMy(item)}
                  aria-label={isReadonly ? '只读' : '编辑'}
                  style={{
                    color: colors.text,
                    borderColor: colors.cardBorder,
                    background: isDark ? 'rgba(255,255,255,0.04)' : colors.cardBg,
                  }}
                />
              </Tooltip>
            </Space>
          </Space>

          {item.description && (
            <div
              style={{
                fontSize: 13,
                color: colors.textSecondary,
                lineHeight: 1.6,
              }}
            >
              {item.description}
            </div>
          )}

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

  // ── Render: System Templates — Scheduler Management 式左右分栏 + 行列表 ──
  const renderSystemList = () => {
    if (sysLoading) return <Spin />;

    const moduleConfigurableMap = new Map<string, boolean>(
      modules.map((m) => [m.module_code, m.configurable]),
    );

    const fixedItems = sysItems.filter((t) => !moduleConfigurableMap.get(t.module_code || ''));
    const seedItems = sysItems.filter((t) => !!moduleConfigurableMap.get(t.module_code || ''));

    const renderSystemListItem = (tpl: PromptTemplate, index: number, listLength: number) => (
      <div
        key={tpl.id}
        style={{
          display: 'flex',
          flexWrap: 'wrap',
          alignItems: 'center',
          gap: 16,
          padding: '16px 24px',
          borderBottom: index < listLength - 1 ? `1px solid ${colors.cardBorder}` : 'none',
          backgroundColor: 'transparent',
          transition: 'background-color 0.2s',
        }}
        onMouseEnter={(e) => {
          e.currentTarget.style.backgroundColor = colors.hoverRowBg;
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.backgroundColor = 'transparent';
        }}
      >
        <div style={{ flex: '1 1 220px', minWidth: 0 }}>
          <div
            style={{
              fontWeight: 600,
              fontSize: 15,
              color: colors.text,
              marginBottom: tpl.description ? 4 : 6,
            }}
          >
            {tpl.template_name}
          </div>
          {tpl.description && (
            <div style={{ fontSize: 13, color: colors.textSecondary, marginBottom: 8, lineHeight: 1.6 }}>
              {tpl.description}
            </div>
          )}
          <Space size={4} wrap>
            {tpl.recommended_provider && (
              <Tag color="purple" style={{ fontSize: 10, margin: 0 }}>
                <RobotOutlined /> {tpl.recommended_provider}
              </Tag>
            )}
            {tpl.recommended_model && (
              <Tag color="cyan" style={{ fontSize: 10, margin: 0 }}>
                {tpl.recommended_model}
              </Tag>
            )}
            <Tag style={{ fontSize: 10, margin: 0 }}>Temp: {tpl.temperature}</Tag>
            <Tag style={{ fontSize: 10, margin: 0 }}>Max: {tpl.max_tokens}</Tag>
          </Space>
        </div>
        <div style={{ display: 'flex', alignItems: 'center', flexShrink: 0 }}>
          <Tooltip title="编辑">
            <Button
              type="default"
              size="middle"
              icon={<SettingOutlined />}
              onClick={() => openEditSystem(tpl)}
              style={{
                borderRadius: 8,
                width: 36,
                height: 36,
                padding: 0,
                color: colors.text,
                borderColor: colors.cardBorder,
                background: isDark ? 'rgba(255,255,255,0.04)' : colors.cardBg,
              }}
              aria-label="编辑"
            />
          </Tooltip>
        </div>
      </div>
    );

    if (!sysItems.length) {
      return <Empty description="无系统模板" />;
    }

    return (
      <div
        style={{
          ...cardStyle,
          marginBottom: 0,
          overflow: 'hidden',
          backgroundColor: colors.cardBg,
        }}
      >
        <div
          style={{
            display: 'flex',
            flexWrap: 'wrap',
            gap: 24,
            padding: '20px 24px',
            borderBottom: `1px solid ${colors.cardBorder}`,
          }}
        >
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
            <span
              style={{
                fontSize: 24,
                fontWeight: 600,
                color: isDark ? '#fa8c16' : '#d46b08',
              }}
            >
              {fixedItems.length}
            </span>
            <span style={{ fontSize: 13, color: colors.textSecondary }}>System fixed</span>
          </div>
          <div style={{ display: 'flex', alignItems: 'baseline', gap: 8 }}>
            <span
              style={{
                fontSize: 24,
                fontWeight: 600,
                color: isDark ? '#52c41a' : '#389e0d',
              }}
            >
              {seedItems.length}
            </span>
            <span style={{ fontSize: 13, color: colors.textSecondary }}>Seeds</span>
          </div>
        </div>

        <Row
          gutter={[0, 0]}
          align="stretch"
          wrap
          style={{ margin: 0, display: 'flex' }}
        >
          <Col
            xs={24}
            lg={12}
            style={{
              padding: 0,
              display: 'flex',
              flexDirection: 'column',
              minHeight: 0,
            }}
          >
            <div
              style={{
                padding: '14px 24px 10px',
                borderBottom: `1px solid ${colors.cardBorder}`,
                flexShrink: 0,
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <LockOutlined
                  style={{ color: isDark ? '#fa8c16' : '#d46b08', fontSize: 16 }}
                />
                <span style={{ fontWeight: 600, fontSize: 15, color: colors.text }}>
                  System fixed
                </span>
              </div>
              <Text
                style={{ display: 'block', marginTop: 4, fontSize: 12, color: colors.textSecondary }}
              >
                编辑后所有用户下次调用即使用新内容（SYSTEM_FIXED 模块，无 per-user 副本）
              </Text>
            </div>
            <div
              style={{
                flex: 1,
                display: 'flex',
                flexDirection: 'column',
                minHeight: 0,
              }}
            >
              {fixedItems.length === 0 ? (
                <div
                  style={{
                    flex: 1,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    padding: 24,
                    minHeight: 160,
                  }}
                >
                  <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无" />
                </div>
              ) : (
                <>
                  {fixedItems.map((tpl, index) =>
                    renderSystemListItem(tpl, index, fixedItems.length),
                  )}
                  <div
                    style={{ flex: 1, minHeight: 0, background: 'transparent' }}
                    aria-hidden
                  />
                </>
              )}
            </div>
          </Col>
          <Col
            xs={24}
            lg={12}
            style={{
              padding: 0,
              display: 'flex',
              flexDirection: 'column',
              minHeight: 0,
              borderTop: isLgUp ? undefined : `1px solid ${colors.cardBorder}`,
              borderLeft: isLgUp ? `1px solid ${colors.cardBorder}` : undefined,
            }}
          >
            <div
              style={{
                padding: '14px 24px 10px',
                borderBottom: `1px solid ${colors.cardBorder}`,
                flexShrink: 0,
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                <ExperimentOutlined
                  style={{ color: isDark ? '#52c41a' : '#389e0d', fontSize: 16 }}
                />
                <span style={{ fontWeight: 600, fontSize: 15, color: colors.text }}>Seeds</span>
              </div>
              <Text
                style={{ display: 'block', marginTop: 4, fontSize: 12, color: colors.textSecondary }}
              >
                仅作 USER_OWNED 模块的默认种子；新用户与「重置」时采用，不覆盖现有用户副本
              </Text>
            </div>
            <div
              style={{
                flex: 1,
                display: 'flex',
                flexDirection: 'column',
                minHeight: 0,
              }}
            >
              {seedItems.length === 0 ? (
                <div
                  style={{
                    flex: 1,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    padding: 24,
                    minHeight: 160,
                  }}
                >
                  <Empty image={Empty.PRESENTED_IMAGE_SIMPLE} description="暂无" />
                </div>
              ) : (
                <>
                  {seedItems.map((tpl, index) =>
                    renderSystemListItem(tpl, index, seedItems.length),
                  )}
                  <div
                    style={{ flex: 1, minHeight: 0, background: 'transparent' }}
                    aria-hidden
                  />
                </>
              )}
            </div>
          </Col>
        </Row>
      </div>
    );
  };

  // ── Render: Editor Drawer ──────────────────────────────────────────────
  const renderDrawerTitle = () => {
    if (!editing) return '';
    const code = editing.kind === 'my' ? editing.item.template_code : editing.template.template_code;
    const line1 =
      editing.kind === 'my'
        ? `编辑: ${editing.item.module_name}`
        : `[Admin] 编辑系统模板: ${editing.template.template_name}`;
    return (
      <Space direction="vertical" size={2}>
        <span style={{ lineHeight: 1.3 }}>{line1}</span>
        {code && (
          <Text type="secondary" style={{ fontSize: 12 }}>
            template_code:{' '}
            <Text code copyable>
              {code}
            </Text>
          </Text>
        )}
      </Space>
    );
  };

  const renderEditor = () => (
    <Drawer
      title={renderDrawerTitle()}
      width={720}
      open={drawerVisible}
      onClose={() => {
        setDrawerVisible(false);
        setEditing(null);
      }}
      destroyOnClose
      styles={
        isDark
          ? {
              content: { background: colors.cardBg },
              body: { background: colors.cardBg, padding: 24 },
              header: {
                background: colors.cardBg,
                borderBottom: `1px solid ${colors.cardBorder}`,
                color: colors.text,
              },
              footer: {
                background: colors.cardBg,
                borderTop: `1px solid ${colors.cardBorder}`,
              },
            }
          : undefined
      }
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
              style={{ marginBottom: 16 }}
              message={
                <>
                  你正在编辑你自己的 "{editing.item.module_name}" 私有模板。修改只影响你自己。
                  <br />
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    Prompt 里用{' '}
                    <Text code style={{ fontSize: 11 }}>{'{{variable_name}}'}</Text>{' '}
                    引用变量（如文件正文、新闻标题），运行时由系统自动填充。
                    点击下方编辑器右上角 <Text strong>Insert Variable</Text> 查看可用变量。
                  </Text>
                </>
              }
            />
          )}
          {editing.kind === 'system' && (
            <Alert
              type="warning"
              showIcon
              style={{ marginBottom: 16 }}
              message={
                <>
                  你正在编辑系统模板 "{editing.template.template_name}"，所有未自定义的用户都会立即使用这份新内容。
                  <br />
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    Prompt 里用{' '}
                    <Text code style={{ fontSize: 11 }}>{'{{variable_name}}'}</Text>{' '}
                    引用变量，点击下方编辑器右上角 <Text strong>Insert Variable</Text> 查看可用变量。
                  </Text>
                </>
              }
            />
          )}

          <Form.Item
            label="System Prompt"
            name="system_prompt"
            extra="LLM 的 system 角色指令，定义助手身份与基调。"
          >
            <TextArea rows={4} placeholder="（可选）" />
          </Form.Item>

          <div
            style={{
              display: 'flex',
              justifyContent: 'space-between',
              alignItems: 'center',
              marginBottom: 4,
            }}
          >
            <Text strong style={{ fontSize: 14 }}>
              User Prompt Template <Text type="danger">*</Text>
            </Text>
            <InsertVariablePopover
              variables={editingVariables}
              onInsert={insertTokenAtCursor}
            />
          </div>
          <Form.Item
            name="user_prompt_template"
            rules={[{ required: true, message: '不能为空' }]}
            extra="支持 {{变量}} 占位符；用户调用时会被自动填充。"
          >
            <TextArea ref={promptTextAreaRef} rows={12} />
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
                <InputNumber min={1} max={65536} step={100} style={{ width: '100%' }} />
              </Form.Item>
            </Col>
          </Row>
        </Form>
      )}
    </Drawer>
  );

  // ── My Templates: 仅展示用户自己可编辑的 USER_OWNED 卡片 ───────────────
  // 简化决策 (2026-04-28): 旧版还展示 SYSTEM_FIXED 只读区, 与 System Templates
  // Tab (admin) 完全冗余, 普通用户也用不上. 删掉, 让此页对普通用户彻底单一目的:
  // "这就是我能编辑的全部模板".
  const ownedItems = myItems.filter((x) => x.kind === 'user_owned');

  return (
    <div style={{ padding: 24, background: colors.pageBg, minHeight: '100vh' }}>
      <Title level={3} style={{ color: colors.text, marginBottom: 4 }}>
        Prompt Templates
      </Title>
      <Paragraph style={{ marginBottom: 20, color: colors.textSecondary }}>
        在 My Templates 中编辑你自己的 LLM 模板。每个功能模块对应一份私有模板，
        重置 = 拉回系统默认（覆盖式，无历史）。
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
                  children: renderSystemList(),
                },
              ]
            : []),
        ]}
      />

      {renderEditor()}
    </div>
  );
};

/** 与 Files / Users 等页一致：ConfigProvider darkAlgorithm + App 以统一 Tabs/Drawer/Modal/Message 的暗色表现 */
const PromptTemplatesPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME[themeMode];

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: colors.cardBg,
            colorBgElevated: colors.cardBg,
            colorBorderSecondary: colors.cardBorder,
            colorPrimary: colors.accent,
            colorText: colors.text,
            colorTextSecondary: colors.textSecondary,
            colorFillQuaternary: 'rgba(255, 255, 255, 0.04)',
            colorBgLayout: colors.pageBg,
            colorSplit: colors.cardBorder,
            borderRadius: 8,
            borderRadiusLG: 10,
            colorBorder: colors.cardBorder,
          },
        }),
      }}
    >
      <App>
        <PromptTemplatesPageContent />
      </App>
    </ConfigProvider>
  );
};

export default PromptTemplatesPage;
