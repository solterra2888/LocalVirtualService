/**
 * UploadAnalysisTemplatePicker
 *
 * 在所有"上传 / 创建 Source"弹窗中都会展示的"AI 分析模板"选择器,
 * 与上传完成后自动并行触发的 analyze_document_task / analyze_audio_task 直接挂钩.
 *
 * - 不强制选择: 默认 None, 不会触发 analyze
 *   (音频/YouTube 在后端会自动降级到 audio_analysis_v1)
 * - 选中模板时, 会展开模板的可选变量编辑器, 与 FilesPage 右侧"Re-analyze"
 *   使用同一个 TemplateVariablesEditor, 行为完全一致.
 *
 * 父组件通过 `onChange({ templateId, templateVariables })` 拿到当前选择,
 * 然后在调用上传 API 时把它们透传给后端.
 */

import React from 'react';
import { Space, Typography, Tooltip } from 'antd';
import { InfoCircleOutlined } from '@ant-design/icons';
import TemplateSelector from './TemplateSelector';
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
   * 'optional'         : 文档/网页/文本笔记 - 用户没选则不触发 analyze
   * 'audio-default'    : 音频/YouTube       - 用户没选则降级 audio_analysis_v1
   */
  mode?: 'optional' | 'audio-default';
  /** 标签文案; 不传则按 mode 走默认 */
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
  label,
  hint,
  textColor,
  textSecondaryColor,
  disabled = false,
  showVariablesEditor = true,
  style,
}) => {
  const isAudio = mode === 'audio-default';

  const resolvedLabel = label ?? 'AI Analysis Template';
  const resolvedHint =
    hint ??
    (isAudio
      ? 'Optional. The audio will be auto-analyzed after transcription. Pick a template to override the default.'
      : 'Optional. If selected, AI analysis runs automatically right after the source is processed.');

  const handleTemplateChange = (templateId?: number) => {
    onChange({
      templateId: templateId ?? undefined,
      // 切换模板时清空变量, 与 FilesPage 右侧面板保持一致
      templateVariables: {},
    });
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

      <TemplateSelector
        categoryCode="file_analysis"
        value={value.templateId}
        onChange={(tid) => handleTemplateChange(tid)}
        placeholder={
          isAudio
            ? 'Default: Audio Meeting Analysis'
            : 'None - skip AI analysis'
        }
        disabled={disabled}
        allowClear
        showConfigWhenSelected={false}
        autoSelectDefault={false}
      />

      <div
        style={{
          marginTop: 6,
          fontSize: 12,
          color: textSecondaryColor || 'rgba(0,0,0,0.45)',
        }}
      >
        {resolvedHint}
      </div>

      {showVariablesEditor && value.templateId != null && (
        <div style={{ marginTop: 12, textAlign: 'left' }}>
          <TemplateVariablesEditor
            templateId={value.templateId}
            values={value.templateVariables || {}}
            onChange={handleVariablesChange}
            disabled={disabled}
          />
        </div>
      )}
    </div>
  );
};

export default UploadAnalysisTemplatePicker;
