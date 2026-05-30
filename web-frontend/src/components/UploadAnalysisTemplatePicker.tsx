/**
 * UploadAnalysisTemplatePicker — Phase 2 + PR-C UX 改进
 *
 * 交互设计:
 *   - 开关 (Switch): 是否启用该模块的 AI 处理
 *   - 模板名 Tag: 显示当前用户在该 moduleCode 下的私有模板名
 *   - "Edit" 按钮: 点击后在当前页面弹出侧滑 Drawer, 就地编辑 prompt 内容 + LLM 配置
 *     保存后 Drawer 关闭, picker 自动刷新模板名, 上传流程不中断
 *
 * value/onChange 接口与旧版完全兼容 (templateId + templateVariables).
 */

import React, { useEffect, useMemo, useRef, useState } from 'react';
import type { TextAreaRef } from 'antd/es/input/TextArea';
import {
  Button,
  Drawer,
  Form,
  Input,
  InputNumber,
  message,
  Row,
  Col,
  Select,
  Skeleton,
  Space,
  Switch,
  Tag,
  Tooltip,
  Typography,
  Alert,
} from 'antd';
import {
  EditOutlined,
  InfoCircleOutlined,
  RobotOutlined,
  ReloadOutlined,
} from '@ant-design/icons';
import promptTemplateService, {
  MyTemplateItem,
  UpdateMyTemplatePayload,
} from '../services/promptTemplateService';
import TemplateVariablesEditor from './TemplateVariablesEditor';
import InsertVariablePopover from './InsertVariablePopover';

const { Text } = Typography;
const { TextArea } = Input;

/** Above FilesPage custom overlays (1100) and default antd Modal mask (1000). */
const TEMPLATE_EDIT_DRAWER_Z_INDEX = 1200;

// ── Provider → Model 映射 ────────────────────────────────────────────────
const PROVIDER_MODELS: Record<string, string[]> = {
  qwen: [
    'qwen-turbo', 'qwen-plus', 'qwen-max', 'qwen-max-longcontext',
    'qwen3-max',
    'qwen3.6-flash', 'qwen3.6-plus', 'qwen3.6-max-preview',
  ],
  doubao: ['doubao-seed-1-6-250615'],
  glm: ['glm-4.6', 'glm-4', 'glm-4-flash'],
  claude: ['anthropic/claude-sonnet-4.6', 'anthropic/claude-opus-4.7'],
};

// ── 公开类型 ─────────────────────────────────────────────────────────────

export interface UploadAnalysisTemplatePickerValue {
  /** 用户的私有模板 id (开关 ON 时自动填充, OFF 时 undefined) */
  templateId?: number;
  templateVariables?: Record<string, any>;
  /**
   * 方案 A: 用户在上传弹窗主动启用/禁用 AI 处理.
   * - undefined / true: 启用 (默认 ON, 走用户私有模板)
   * - false           : 禁用 (后端 trigger_auto_* 直接 return)
   * 此字段会被父组件透传到后端 (auto_*_enabled).
   */
  enabled?: boolean;
}

interface UploadAnalysisTemplatePickerProps {
  value: UploadAnalysisTemplatePickerValue;
  onChange: (value: UploadAnalysisTemplatePickerValue) => void;
  /**
   * 'optional'      : 文档/网页/文本笔记 — 用户没启用则不触发 analyze
   * 'audio-default' : 音频/YouTube      — 用户没启用则后端 fallback 系统默认
   */
  mode?: 'optional' | 'audio-default';
  /** USER_OWNED 模块标识; 默认 'file_analysis' */
  moduleCode?: string;
  /** 标签文案; 不传则按 moduleCode 走默认 */
  label?: string;
  /** 提示文案; 不传则按 mode/moduleCode 走默认 */
  hint?: string;
  textColor?: string;
  textSecondaryColor?: string;
  disabled?: boolean;
  /** 选中模板后是否展开变量编辑器; 默认 true */
  showVariablesEditor?: boolean;
  style?: React.CSSProperties;
}

interface EditForm {
  system_prompt: string;
  user_prompt_template: string;
  recommended_provider: string;
  recommended_model: string;
  temperature: number;
  max_tokens: number;
}

// ── 主组件 ────────────────────────────────────────────────────────────────

const UploadAnalysisTemplatePicker: React.FC<UploadAnalysisTemplatePickerProps> = ({
  value,
  onChange,
  mode = 'optional',
  moduleCode = 'file_analysis',
  label,
  hint,
  textColor,
  textSecondaryColor,
  disabled = false,
  showVariablesEditor = true,
  style,
}) => {
  const [myTemplate, setMyTemplate] = useState<MyTemplateItem | null>(null);
  const [loadingTpl, setLoadingTpl] = useState(false);

  // Drawer state
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [saving, setSaving] = useState(false);
  const [resetting, setResetting] = useState(false);
  const [form] = Form.useForm<EditForm>();
  const watchedProvider = Form.useWatch('recommended_provider', form);
  const availableModels = useMemo(
    () => PROVIDER_MODELS[watchedProvider || ''] || [],
    [watchedProvider],
  );

  // 持有 user_prompt_template 的 TextArea ref, 用于 Insert Variable 按光标位置插入
  const promptTextAreaRef = useRef<TextAreaRef>(null);

  // 把变量占位符插入到当前 Prompt TextArea 光标位置 (没有焦点时追加到末尾)
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
    // 把光标移到刚插入字符之后, 方便连续插入
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

  const isAudio = mode === 'audio-default';
  // 方案 A: enabled 显式控制; 缺省按 "未初始化" 处理 → 加载完模板后自动设 true
  const enabled = value.enabled ?? false;

  const resolvedLabel = label ?? defaultLabel(moduleCode);
  const resolvedHint =
    hint ??
    (isAudio
      ? 'Optional. Audio is auto-analyzed after transcription. Toggle on to use your private template instead of the system default.'
      : `Optional. If on, ${displayModuleName(moduleCode)} runs automatically using your private template.`);

  // ── Load template on mount / module change ───────────────────────────────
  const loadTemplate = async (quiet = false) => {
    if (!quiet) setLoadingTpl(true);
    try {
      const item = await promptTemplateService.getMyTemplate(moduleCode);
      setMyTemplate(item);
    } catch {
      setMyTemplate(null);
    } finally {
      if (!quiet) setLoadingTpl(false);
    }
  };

  // 首次加载用户私有模板 (随 moduleCode 变化触发)
  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoadingTpl(true);
      try {
        const item = await promptTemplateService.getMyTemplate(moduleCode);
        if (!cancelled) setMyTemplate(item);
      } catch {
        if (!cancelled) setMyTemplate(null);
      } finally {
        if (!cancelled) setLoadingTpl(false);
      }
    })();
    return () => { cancelled = true; };
  }, [moduleCode]);

  // 方案 A: "value 被父组件重置回空" 时自动 ON.
  // antd Modal 默认不卸载子组件, 所以 picker 实例在弹窗多次打开间是同一个;
  // 父组件 setAnalyzePicker({}) 后此 effect 触发, 实现"每次打开默认开启".
  // 用户主动 OFF 时 enabled === false (≠ undefined), 不会被这里再次自动 ON.
  useEffect(() => {
    if (
      myTemplate &&
      value.enabled === undefined &&
      value.templateId === undefined
    ) {
      onChange({
        templateId: myTemplate.template_id,
        templateVariables: value.templateVariables || {},
        enabled: true,
      });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [myTemplate, value.enabled, value.templateId]);

  // ── Toggle ───────────────────────────────────────────────────────────────
  const handleToggle = (checked: boolean) => {
    if (checked) {
      if (!myTemplate) return;
      // 方案 A: ON = 启用 AI 处理, 用用户私有模板
      onChange({
        templateId: myTemplate.template_id,
        templateVariables: {},
        enabled: true,
      });
    } else {
      // 方案 A: OFF = 禁用 AI 处理 (后端 trigger_auto_* 直接 return)
      onChange({
        templateId: undefined,
        templateVariables: {},
        enabled: false,
      });
    }
  };

  // ── Open edit drawer ─────────────────────────────────────────────────────
  const handleOpenEdit = () => {
    if (!myTemplate) return;
    form.setFieldsValue({
      system_prompt: myTemplate.system_prompt || '',
      user_prompt_template: myTemplate.user_prompt_template || '',
      recommended_provider: myTemplate.recommended_provider || 'qwen',
      recommended_model: myTemplate.recommended_model || '',
      temperature: myTemplate.temperature ?? 0.7,
      max_tokens: myTemplate.max_tokens ?? 2000,
    });
    setDrawerOpen(true);
  };

  // ── Save ─────────────────────────────────────────────────────────────────
  const handleSave = async (values: EditForm) => {
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
      await promptTemplateService.updateMyTemplate(moduleCode, payload);
      message.success(`Template "${resolvedLabel}" saved`);
      setDrawerOpen(false);
      // 刷新本地 template 信息 (e.g. 模板名可能变)
      await loadTemplate(true);
    } catch (err: any) {
      message.error(`Save failed: ${err?.response?.data?.detail || err?.message || ''}`);
    } finally {
      setSaving(false);
    }
  };

  // ── Reset to system default ──────────────────────────────────────────────
  const handleReset = async () => {
    setResetting(true);
    try {
      await promptTemplateService.resetMyTemplate(moduleCode);
      message.success('Reset to system default');
      // 重新拉最新 template 并回填表单
      const refreshed = await promptTemplateService.getMyTemplate(moduleCode);
      setMyTemplate(refreshed);
      form.setFieldsValue({
        system_prompt: refreshed.system_prompt || '',
        user_prompt_template: refreshed.user_prompt_template || '',
        recommended_provider: refreshed.recommended_provider || 'qwen',
        recommended_model: refreshed.recommended_model || '',
        temperature: refreshed.temperature ?? 0.7,
        max_tokens: refreshed.max_tokens ?? 2000,
      });
    } catch (err: any) {
      message.error(`Reset failed: ${err?.response?.data?.detail || err?.message || ''}`);
    } finally {
      setResetting(false);
    }
  };

  const handleVariablesChange = (vars: Record<string, any>) => {
    onChange({
      templateId: value.templateId,
      templateVariables: vars ?? {},
      enabled: value.enabled,
    });
  };

  // ── Render ───────────────────────────────────────────────────────────────
  return (
    <div style={style}>
      {/* 标题与 Switch 分列两侧，避免与「AI xx Template」挤在一起 */}
      <div
        style={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          gap: 16,
          marginBottom: 10,
          flexWrap: 'wrap',
        }}
      >
        <Space size={6} align="center" style={{ flex: '1 1 auto', minWidth: 0 }}>
          <Text strong style={textColor ? { color: textColor } : undefined}>
            {resolvedLabel}
          </Text>
          <Tooltip title={resolvedHint}>
            <InfoCircleOutlined
              style={{ color: textSecondaryColor || '#8c8c8c', fontSize: 13 }}
            />
          </Tooltip>
        </Space>
        {loadingTpl ? (
          <Skeleton.Button active size="default" style={{ flexShrink: 0 }} />
        ) : (
          <Switch
            checked={enabled}
            disabled={disabled || !myTemplate}
            onChange={handleToggle}
            style={{ flexShrink: 0 }}
          />
        )}
      </div>

      {/* 模板名 + 编辑（Switch 下方） */}
      {!loadingTpl &&
        (myTemplate ? (
          <Space size={10} align="center" wrap style={{ marginBottom: 4 }}>
            <Tag
              color={enabled ? 'blue' : 'default'}
              style={{ margin: 0, fontSize: 11 }}
            >
              <RobotOutlined /> {myTemplate.template_name}
            </Tag>
            <Tooltip title="Edit your private template inline on this page">
              <Button
                size="small"
                type="link"
                icon={<EditOutlined />}
                onClick={handleOpenEdit}
                disabled={disabled}
              >
                Edit my template
              </Button>
            </Tooltip>
          </Space>
        ) : (
          <Text type="secondary" style={{ fontSize: 12, display: 'block', marginBottom: 4 }}>
            Unable to load a private template for this module.
          </Text>
        ))}

      {/* Hint text */}
      <div
        style={{
          marginTop: 6,
          fontSize: 12,
          color: textSecondaryColor || 'rgba(0,0,0,0.45)',
        }}
      >
        {resolvedHint}
      </div>

      {/* Variables editor (when enabled) */}
      {showVariablesEditor && enabled && myTemplate && (
        <div style={{ marginTop: 12, textAlign: 'left' }}>
          <TemplateVariablesEditor
            templateId={myTemplate.template_id}
            values={value.templateVariables || {}}
            onChange={handleVariablesChange}
            disabled={disabled}
          />
        </div>
      )}

      {/* ── Inline Edit Drawer ─────────────────────────────────────────── */}
      <Drawer
        title={
          <Space direction="vertical" size={2}>
            <Space>
              <EditOutlined />
              <span>Edit: {myTemplate?.template_name}</span>
            </Space>
            {myTemplate?.template_code && (
              <Text type="secondary" style={{ fontSize: 12 }}>
                template_code:{' '}
                <Text code copyable>
                  {myTemplate.template_code}
                </Text>
              </Text>
            )}
          </Space>
        }
        width={680}
        zIndex={TEMPLATE_EDIT_DRAWER_Z_INDEX}
        open={drawerOpen}
        onClose={() => setDrawerOpen(false)}
        destroyOnClose
        extra={
          <Tooltip title="Reset prompt content to the system default (overwrites current content; cannot be undone)">
            <Button
              icon={<ReloadOutlined />}
              size="small"
              loading={resetting}
              onClick={handleReset}
            >
              Reset to default
            </Button>
          </Tooltip>
        }
        footer={
          <Space style={{ width: '100%', justifyContent: 'flex-end' }}>
            <Button onClick={() => setDrawerOpen(false)}>Cancel</Button>
            <Button type="primary" loading={saving} onClick={() => form.submit()}>
              Save
            </Button>
          </Space>
        }
      >
        <Alert
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
          message={
            <>
              This is your private template for &quot;{resolvedLabel}&quot;. Changes apply only to your account.
              <br />
              <Text type="secondary" style={{ fontSize: 12 }}>
                Use{' '}
                <Text code style={{ fontSize: 11 }}>{'{{variable_name}}'}</Text>{' '}
                in the prompt for placeholders (e.g. file body, file name); the system fills them at runtime.
                Use <Text strong>Insert Variable</Text> above the editor to see available variables.
              </Text>
            </>
          }
        />

        <Form form={form} layout="vertical" onFinish={handleSave}>
          <Form.Item
            label="System Prompt"
            name="system_prompt"
            extra="Defines the model's role and tone (optional)"
          >
            <TextArea rows={3} placeholder="(optional)" />
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
              variables={myTemplate?.variables}
              onInsert={insertTokenAtCursor}
            />
          </div>
          <Form.Item
            name="user_prompt_template"
            rules={[{ required: true, message: 'Required' }]}
            extra="Supports {{variable}} placeholders; values are filled when the analysis runs"
          >
            <TextArea ref={promptTextAreaRef} rows={10} />
          </Form.Item>

          <Row gutter={12}>
            <Col span={12}>
              <Form.Item label="Provider" name="recommended_provider">
                <Select
                  options={Object.keys(PROVIDER_MODELS).map((k) => ({
                    label: k,
                    value: k,
                  }))}
                />
              </Form.Item>
            </Col>
            <Col span={12}>
              <Form.Item label="Model" name="recommended_model">
                <Select
                  options={availableModels.map((m) => ({ label: m, value: m }))}
                  placeholder="Pick a model after choosing a provider"
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
      </Drawer>
    </div>
  );
};

// ── 工具函数 ─────────────────────────────────────────────────────────────

function defaultLabel(moduleCode: string): string {
  switch (moduleCode) {
    case 'file_analysis':   return 'AI Analysis Template';
    case 'audio_analysis':  return 'Audio Analysis Template';
    case 'youtube_analysis': return 'YouTube Analysis Template';
    case 'file_tagging':    return 'AI Tagging Template';
    case 'feed_summary':    return 'Feed Summary Template';
    case 'youtube_summary': return 'YouTube Summary Template';
    default:                return 'AI Template';
  }
}

function displayModuleName(moduleCode: string): string {
  switch (moduleCode) {
    case 'file_analysis':   return 'AI document analysis';
    case 'audio_analysis':  return 'audio analysis';
    case 'youtube_analysis': return 'YouTube link deep analysis';
    case 'file_tagging':    return 'auto tagging';
    case 'feed_summary':    return 'feed personalization';
    case 'youtube_summary': return 'YouTube summarization';
    default:                return 'AI processing';
  }
}

export default UploadAnalysisTemplatePicker;
