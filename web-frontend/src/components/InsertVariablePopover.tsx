/**
 * InsertVariablePopover — 模板编辑器的 "Insert Variable" 快捷入口
 *
 * 背景:
 *   每个 LLM Prompt Template 在 llm_prompt_templates.variables 字段里定义
 *   一组变量 (e.g. file_content / additional_instructions / max_length).
 *   Prompt 文本通过 {{variable_name}} 占位符引用, 运行时由后端
 *   PromptRenderer 用真实数据替换. 不熟悉的用户根本不知道当前模板有哪些变量,
 *   也不知道怎么写占位符.
 *
 * 用法:
 *   <InsertVariablePopover
 *     variables={myTemplate.variables}
 *     onInsert={(token) => insertAtCursor(token)}
 *   />
 *
 * 行为:
 *   - 按钮: "Insert Variable"
 *   - 弹出层: 列出该模板定义的所有变量, 每条显示
 *       变量名 [类型] [required/optional] — 描述
 *   - 点击某条 → 调用 onInsert("{{var_name}}"), 父组件负责插入到光标位置
 *   - 顶部一段简短说明, 让用户知道 Variables 是什么, 占位符怎么写
 */

import React from 'react';
import { Button, Empty, Popover, Space, Tag, Typography } from 'antd';
import { ApiOutlined, CodeOutlined } from '@ant-design/icons';

const { Text, Paragraph } = Typography;

export interface VariableDefinition {
  type?: string;
  required?: boolean;
  default?: any;
  description?: string;
}

interface InsertVariablePopoverProps {
  variables: Record<string, VariableDefinition> | null | undefined;
  onInsert: (token: string) => void;
  buttonSize?: 'small' | 'middle' | 'large';
  /** 按钮显示的文字; 默认 "Insert Variable" */
  buttonLabel?: string;
  disabled?: boolean;
}

const InsertVariablePopover: React.FC<InsertVariablePopoverProps> = ({
  variables,
  onInsert,
  buttonSize = 'small',
  buttonLabel = 'Insert Variable',
  disabled,
}) => {
  const entries = Object.entries(variables || {});

  const content = (
    <div style={{ width: 360, maxHeight: 420, overflowY: 'auto' }}>
      <Paragraph
        type="secondary"
        style={{ marginBottom: 10, fontSize: 12, lineHeight: 1.5 }}
      >
        Variables 是这个模板调用时由系统自动填入的内容（例如文件正文、文件名、
        新闻标题）。在 Prompt 中用{' '}
        <Text code style={{ fontSize: 11 }}>{'{{variable_name}}'}</Text>{' '}
        引用，运行时会被替换为真实数据。点击下方变量即可插入到光标位置。
      </Paragraph>

      {entries.length === 0 ? (
        <Empty
          description="该模板未定义变量"
          imageStyle={{ height: 40 }}
          style={{ margin: '12px 0' }}
        />
      ) : (
        <Space direction="vertical" size={6} style={{ width: '100%' }}>
          {entries.map(([name, def]) => (
            <div
              key={name}
              role="button"
              tabIndex={0}
              onClick={() => onInsert(`{{${name}}}`)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault();
                  onInsert(`{{${name}}}`);
                }
              }}
              style={{
                cursor: 'pointer',
                padding: '8px 10px',
                borderRadius: 6,
                border: '1px solid #f0f0f0',
                background: '#fafafa',
                transition: 'background 120ms',
              }}
              onMouseEnter={(e) => {
                (e.currentTarget as HTMLDivElement).style.background = '#e6f4ff';
              }}
              onMouseLeave={(e) => {
                (e.currentTarget as HTMLDivElement).style.background = '#fafafa';
              }}
            >
              <Space size={6} wrap>
                <Text code strong style={{ fontSize: 12 }}>
                  {`{{${name}}}`}
                </Text>
                {def?.type && (
                  <Tag color="blue" style={{ margin: 0, fontSize: 10 }}>
                    {def.type}
                  </Tag>
                )}
                {def?.required ? (
                  <Tag color="red" style={{ margin: 0, fontSize: 10 }}>
                    required
                  </Tag>
                ) : (
                  <Tag color="default" style={{ margin: 0, fontSize: 10 }}>
                    optional
                  </Tag>
                )}
              </Space>
              {def?.description && (
                <div
                  style={{
                    fontSize: 11.5,
                    color: 'rgba(0,0,0,0.55)',
                    marginTop: 4,
                    lineHeight: 1.4,
                  }}
                >
                  {def.description}
                </div>
              )}
              {def?.default !== undefined && def?.default !== null && def?.default !== '' && (
                <div
                  style={{
                    fontSize: 11,
                    color: 'rgba(0,0,0,0.4)',
                    marginTop: 2,
                    fontFamily: 'monospace',
                  }}
                >
                  default: {String(def.default).slice(0, 80)}
                  {String(def.default).length > 80 ? '…' : ''}
                </div>
              )}
            </div>
          ))}
        </Space>
      )}
    </div>
  );

  return (
    <Popover
      content={content}
      title={
        <Space size={6}>
          <ApiOutlined />
          <span>Available Variables ({entries.length})</span>
        </Space>
      }
      trigger="click"
      placement="bottomRight"
    >
      <Button
        type="default"
        size={buttonSize}
        icon={<CodeOutlined />}
        disabled={disabled}
      >
        {buttonLabel}
      </Button>
    </Popover>
  );
};

export default InsertVariablePopover;
