/**
 * UploadAnalysisTemplatePicker  — Phase 2 双态模型 (PR-C)
 *
 * 旧版本: 下拉框选择任意模板 + 变量编辑.
 * 新版本: 取消下拉. 每个 USER_OWNED 模块每用户只有 1 份私有模板 (在 My Templates
 *         里编辑), 这里不再让用户挑选, 改为:
 *           - 开关: 是否启用此模块的 AI 处理 (默认 OFF)
 *           - 显示当前用户在该模块的私有模板名 + 编辑链接 → /prompt-templates
 *           - 启用时显示 TemplateVariablesEditor 给用户填充本次运行变量
 *
 * 兼容父组件:
 *   value/onChange 仍然传递 { templateId, templateVariables }, 这样 UploadFilesModal
 *   等调用方不用改逻辑. 区别仅在 templateId 来源:
 *     - 旧: 用户在下拉里挑的模板 id
 *     - 新: 开关 ON 时, 自动填充该用户在 moduleCode 下的私有模板 id (后端会拿这个 id 走完整渲染); OFF 时 templateId=undefined
 *
 * audio-default 行为:
 *   - mode='audio-default' 表示音频/YouTube 场景: 即使开关 OFF, 后端在 trigger_auto_analyze
 *     也会硬编码 fallback 到 audio_analysis_v1, 所以这里只控制 "用户是否要用自己的私有模板覆盖默认".
 */

import React, { useEffect, useState } from 'react';
import { Space, Typography, Tooltip, Switch, Tag, Button, Skeleton } from 'antd';
import {
  InfoCircleOutlined,
  EditOutlined,
  RobotOutlined,
} from '@ant-design/icons';
import { Link } from 'react-router-dom';
import promptTemplateService, {
  MyTemplateItem,
} from '../services/promptTemplateService';
import TemplateVariablesEditor from './TemplateVariablesEditor';

const { Text } = Typography;

export interface UploadAnalysisTemplatePickerValue {
  templateId?: number;
  templateVariables?: Record<string, any>;
}

interface UploadAnalysisTemplatePickerProps {
  value: UploadAnalysisTemplatePickerValue;
  onChange: (value: UploadAnalysisTemplatePickerValue) => void;
  /**
   * 'optional'         : 文档/网页/文本笔记 - 用户没启用则不触发 analyze
   * 'audio-default'    : 音频/YouTube      - 用户没启用则后端用 system default fallback
   */
  mode?: 'optional' | 'audio-default';
  /** USER_OWNED 模块标识. 默认 'file_analysis'. */
  moduleCode?: string;
  /** 标签文案; 不传则按 mode/moduleCode 走默认 */
  label?: string;
  /** 标签下的提示文案; 不传则按 mode 走默认 */
  hint?: string;
  /** 主题色, 与各弹窗保持一致 */
  textColor?: string;
  textSecondaryColor?: string;
  disabled?: boolean;
  /** 选中模板后是否展开变量编辑器, 默认 true */
  showVariablesEditor?: boolean;
  style?: React.CSSProperties;
}

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
  const [loading, setLoading] = useState(false);

  const isAudio = mode === 'audio-default';
  const enabled = value.templateId != null;

  const resolvedLabel = label ?? defaultLabel(moduleCode);
  const resolvedHint =
    hint ??
    (isAudio
      ? 'Optional. The audio is auto-analyzed after transcription. Toggle on to override the system default with your private template.'
      : `Optional. If on, ${displayModuleName(moduleCode)} will run automatically using your private template.`);

  // ── Load user's owned template once on mount / module change ─────────────
  useEffect(() => {
    let cancelled = false;
    (async () => {
      setLoading(true);
      try {
        const item = await promptTemplateService.getMyTemplate(moduleCode);
        if (!cancelled) setMyTemplate(item);
      } catch {
        // 静默失败: 显示空白状态, 用户开关时报错
        if (!cancelled) setMyTemplate(null);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [moduleCode]);

  // ── Toggle enable/disable ────────────────────────────────────────────────
  const handleToggle = (checked: boolean) => {
    if (checked) {
      if (!myTemplate) return;
      onChange({
        templateId: myTemplate.template_id,
        templateVariables: {}, // 切换 ON 时清空变量
      });
    } else {
      onChange({
        templateId: undefined,
        templateVariables: {},
      });
    }
  };

  const handleVariablesChange = (vars: Record<string, any>) => {
    onChange({
      templateId: value.templateId,
      templateVariables: vars ?? {},
    });
  };

  return (
    <div style={style}>
      <Space size={6} style={{ marginBottom: 6 }}>
        <Text strong style={textColor ? { color: textColor } : undefined}>
          {resolvedLabel}
        </Text>
        <Tooltip title={resolvedHint}>
          <InfoCircleOutlined
            style={{ color: textSecondaryColor || '#8c8c8c', fontSize: 13 }}
          />
        </Tooltip>
      </Space>

      {loading ? (
        <Skeleton.Input active size="small" style={{ width: 240 }} />
      ) : (
        <Space size={10} align="center" wrap>
          <Switch
            checked={enabled}
            disabled={disabled || !myTemplate}
            onChange={handleToggle}
          />
          {myTemplate ? (
            <>
              <Tag
                color={enabled ? 'blue' : 'default'}
                style={{ margin: 0, fontSize: 11 }}
              >
                <RobotOutlined /> {myTemplate.template_name}
              </Tag>
              <Tooltip title="跳到 My Templates 编辑你的私有模板">
                <Link to="/prompt-templates">
                  <Button size="small" type="link" icon={<EditOutlined />}>
                    Edit my template
                  </Button>
                </Link>
              </Tooltip>
            </>
          ) : (
            <Text type="secondary" style={{ fontSize: 12 }}>
              （无法加载该模块的私有模板）
            </Text>
          )}
        </Space>
      )}

      <div
        style={{
          marginTop: 6,
          fontSize: 12,
          color: textSecondaryColor || 'rgba(0,0,0,0.45)',
        }}
      >
        {resolvedHint}
      </div>

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
    </div>
  );
};

function defaultLabel(moduleCode: string): string {
  switch (moduleCode) {
    case 'file_analysis':
      return 'AI Analysis Template';
    case 'audio_analysis':
      return 'Audio Analysis Template';
    case 'file_tagging':
      return 'AI Tagging Template';
    default:
      return 'AI Template';
  }
}

function displayModuleName(moduleCode: string): string {
  switch (moduleCode) {
    case 'file_analysis':
      return 'AI document analysis';
    case 'audio_analysis':
      return 'audio analysis';
    case 'file_tagging':
      return 'auto tagging';
    default:
      return 'AI processing';
  }
}

export default UploadAnalysisTemplatePicker;
