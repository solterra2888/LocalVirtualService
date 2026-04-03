import React, { useState, useMemo } from 'react';
import { 
  Card, 
  Typography, 
  Tag, 
  Space, 
  Collapse, 
  Button, 
  Tooltip,
  Alert,
  Divider,
  Input
} from 'antd';
import { 
  InfoCircleOutlined, 
  CopyOutlined, 
  QuestionCircleOutlined,
  SearchOutlined
} from '@ant-design/icons';
import { PromptCategory } from '../services/promptTemplateService';

const { Text, Paragraph } = Typography;
const { Panel } = Collapse;

interface VariableHelperProps {
  categories: PromptCategory[];
  onVariableInsert?: (variable: string) => void;
  currentCategoryId?: number;
  moduleCode?: string;
}

interface VariableInfo {
  name: string;
  type: string;
  description: string;
  required: boolean;
  examples: string[];
  group: string;
  color: string;
}

/**
 * Static registry: category_code -> variable definitions.
 * Sourced from each backend service module's actual `variables` dict.
 * When a new module is added, register its variables here.
 */
const CATEGORY_VARIABLES: Record<string, VariableInfo[]> = {
  news_analysis: [
    { name: 'title',      type: 'string',  description: '新闻标题（news 表）',          required: true,  examples: ['Company Announcement', 'Market Report'], group: 'Data Fields', color: 'blue' },
    { name: 'content',    type: 'string',  description: '新闻正文内容（news 表）',       required: true,  examples: ['Full article text'],                     group: 'Data Fields', color: 'blue' },
    { name: 'symbols',    type: 'string',  description: '关联股票代码（逗号分隔）',       required: false, examples: ['AAPL,MSFT,GOOGL', '0700.HK,9988.HK'],   group: 'Data Fields', color: 'blue' },
    { name: 'max_length', type: 'number',  description: '摘要最大字数',                  required: false, examples: ['100', '200', '500'],                     group: 'Parameters',  color: 'cyan' },
  ],
  theme_analysis: [
    { name: 'theme_name',    type: 'string', description: '投资主题名称',              required: true,  examples: ['Electric Vehicles', 'AI'],                group: 'Data Fields', color: 'green' },
    { name: 'category',      type: 'string', description: '主题分类',                  required: false, examples: ['Technology', 'Healthcare'],                group: 'Data Fields', color: 'green' },
    { name: 'stock_symbols', type: 'string', description: '相关股票代码',              required: false, examples: ['TSLA,NIO', 'NVDA,AMD'],                   group: 'Data Fields', color: 'green' },
    { name: 'company_names', type: 'string', description: '相关公司名称',              required: false, examples: ['Tesla, NIO Inc.'],                         group: 'Data Fields', color: 'green' },
  ],
  twitter_phase1: [
    { name: 'author_name',         type: 'string', description: '推特作者显示名',           required: true,  examples: ['@elonmusk', 'Elon Musk'],             group: 'Data Fields', color: 'orange' },
    { name: 'report_date',         type: 'string', description: '报告日期',                 required: true,  examples: ['2025-04-03'],                          group: 'Data Fields', color: 'orange' },
    { name: 'tweet_count',         type: 'string', description: '本批次推文数量',           required: true,  examples: ['15', '30'],                            group: 'Data Fields', color: 'orange' },
    { name: 'tweets_text',         type: 'string', description: '推文原文合并文本',         required: true,  examples: ['[Tweet text...]'],                     group: 'Data Fields', color: 'orange' },
    { name: 'report_language',     type: 'string', description: '输出语言',                 required: false, examples: ['zh', 'en'],                            group: 'Parameters',  color: 'cyan' },
    { name: 'custom_instructions', type: 'string', description: '用户自定义指令',           required: false, examples: ['Focus on AI topics'],                  group: 'Parameters',  color: 'cyan' },
  ],
  twitter_phase2: [
    { name: 'report_date',              type: 'string', description: '报告日期',                 required: true,  examples: ['2025-04-03'],              group: 'Data Fields', color: 'orange' },
    { name: 'author_count',             type: 'string', description: '作者总数',                 required: true,  examples: ['12', '25'],                group: 'Data Fields', color: 'orange' },
    { name: 'tweet_count',              type: 'string', description: '推文总数',                 required: true,  examples: ['150', '300'],              group: 'Data Fields', color: 'orange' },
    { name: 'author_summaries',         type: 'string', description: '所有作者摘要合并文本',     required: true,  examples: ['[Author summaries...]'],   group: 'Data Fields', color: 'orange' },
    { name: 'top_tweets',               type: 'string', description: '热门推文文本',             required: true,  examples: ['[Top tweets...]'],         group: 'Data Fields', color: 'orange' },
    { name: 'report_language',          type: 'string', description: '输出语言',                 required: false, examples: ['zh', 'en'],                group: 'Parameters',  color: 'cyan' },
    { name: 'focus_topics_instruction', type: 'string', description: '关注主题指令',             required: false, examples: ['重点关注 AI / Crypto'],    group: 'Parameters',  color: 'cyan' },
    { name: 'custom_instructions',      type: 'string', description: '用户自定义指令',           required: false, examples: ['增加技术分析部分'],        group: 'Parameters',  color: 'cyan' },
  ],
  youtube_phase1: [
    { name: 'author_name',         type: 'string', description: 'YouTube 频道名称',        required: true,  examples: ['TechLead', '李永乐老师'],          group: 'Data Fields', color: 'red' },
    { name: 'report_date',         type: 'string', description: '报告日期',                 required: true,  examples: ['2025-04-03'],                      group: 'Data Fields', color: 'red' },
    { name: 'video_count',         type: 'string', description: '本批次视频数量',           required: true,  examples: ['5', '10'],                         group: 'Data Fields', color: 'red' },
    { name: 'videos_text',         type: 'string', description: '视频信息合并文本',         required: true,  examples: ['[Video transcripts...]'],          group: 'Data Fields', color: 'red' },
    { name: 'report_language',     type: 'string', description: '输出语言',                 required: false, examples: ['zh', 'en'],                        group: 'Parameters',  color: 'cyan' },
    { name: 'custom_instructions', type: 'string', description: '用户自定义指令',           required: false, examples: ['重点关注投资观点'],                group: 'Parameters',  color: 'cyan' },
  ],
  youtube_phase2: [
    { name: 'report_date',              type: 'string', description: '报告日期',                 required: true,  examples: ['2025-04-03'],                  group: 'Data Fields', color: 'red' },
    { name: 'channel_count',            type: 'string', description: '频道总数',                 required: true,  examples: ['8', '15'],                     group: 'Data Fields', color: 'red' },
    { name: 'video_count',              type: 'string', description: '视频总数',                 required: true,  examples: ['30', '60'],                    group: 'Data Fields', color: 'red' },
    { name: 'channel_summaries',        type: 'string', description: '所有频道摘要合并文本',     required: true,  examples: ['[Channel summaries...]'],      group: 'Data Fields', color: 'red' },
    { name: 'top_videos',               type: 'string', description: '热门视频文本',             required: true,  examples: ['[Top videos...]'],             group: 'Data Fields', color: 'red' },
    { name: 'report_language',          type: 'string', description: '输出语言',                 required: false, examples: ['zh', 'en'],                    group: 'Parameters',  color: 'cyan' },
    { name: 'focus_topics_instruction', type: 'string', description: '关注主题指令',             required: false, examples: ['重点关注科技与金融'],          group: 'Parameters',  color: 'cyan' },
    { name: 'custom_instructions',      type: 'string', description: '用户自定义指令',           required: false, examples: ['增加行业对比分析'],            group: 'Parameters',  color: 'cyan' },
  ],
  file_analysis: [
    { name: 'file_content',        type: 'string', description: '文件正文内容（自动注入）',  required: true,  examples: ['[File content...]'],               group: 'Data Fields', color: 'purple' },
    { name: 'additional_context',   type: 'string', description: '附加上下文信息',           required: false, examples: ['这是一份年度报告'],               group: 'Parameters',  color: 'cyan' },
  ],
  content_tagging: [
    { name: 'file_content',        type: 'string', description: '待打标签的文件内容',       required: true,  examples: ['[File content...]'],               group: 'Data Fields', color: 'purple' },
    { name: 'additional_context',   type: 'string', description: '附加上下文（如文件类型）', required: false, examples: ['PDF report about Q4 earnings'],    group: 'Parameters',  color: 'cyan' },
  ],
};

const VariableHelper: React.FC<VariableHelperProps> = ({
  categories,
  onVariableInsert,
  currentCategoryId,
  moduleCode
}) => {
  const [searchText, setSearchText] = useState<string>('');

  const currentCategoryCode = useMemo(() => {
    if (moduleCode) return moduleCode;
    if (!currentCategoryId || !categories.length) return undefined;
    return categories.find(c => c.id === currentCategoryId)?.category_code;
  }, [currentCategoryId, categories, moduleCode]);

  const currentVariables = useMemo(() => {
    if (!currentCategoryCode) return [];
    return CATEGORY_VARIABLES[currentCategoryCode] ?? [];
  }, [currentCategoryCode]);

  const filteredVariables = useMemo(() => {
    if (!searchText) return currentVariables;
    const q = searchText.toLowerCase();
    return currentVariables.filter(
      v => v.name.toLowerCase().includes(q) || v.description.toLowerCase().includes(q)
    );
  }, [currentVariables, searchText]);

  const groupedVariables = useMemo(() => {
    return filteredVariables.reduce((acc, v) => {
      (acc[v.group] ??= []).push(v);
      return acc;
    }, {} as Record<string, VariableInfo[]>);
  }, [filteredVariables]);

  const handleInsertVariable = (variableName: string) => {
    const variableText = `{{${variableName}}}`;
    if (onVariableInsert) {
      onVariableInsert(variableText);
    } else {
      navigator.clipboard.writeText(variableText);
    }
  };

  const renderQuickVariables = () => {
    if (!currentCategoryCode || currentVariables.length === 0) return null;

    const categoryName = categories.find(c => c.id === currentCategoryId)?.category_name || currentCategoryCode;

    return (
      <Card title="Quick Insert" size="small" style={{ marginBottom: 8 }}>
        <Space direction="vertical" style={{ width: '100%' }} size="small">
          <Alert
            message={`Category: ${categoryName}`}
            type="info"
            showIcon
            style={{ fontSize: '11px', padding: '4px 8px' }}
          />
          <div style={{ marginTop: 4 }}>
            <Space wrap size="small">
              {currentVariables.map(v => (
                <Tooltip key={v.name} title={`${v.description}${v.required ? ' (required)' : ''}`}>
                  <Tag
                    color={v.color}
                    style={{ cursor: 'pointer', fontSize: '11px' }}
                    onClick={() => handleInsertVariable(v.name)}
                  >
                    {`{{${v.name}}}`}
                  </Tag>
                </Tooltip>
              ))}
            </Space>
          </div>
        </Space>
      </Card>
    );
  };

  const renderVariableInfo = (variable: VariableInfo) => (
    <Card 
      key={variable.name}
      size="small" 
      style={{ marginBottom: 8 }}
      title={
        <Space>
          <Text code style={{ fontSize: '14px', fontWeight: 'bold' }}>
            {`{{${variable.name}}}`}
          </Text>
          <Tag color={variable.required ? 'red' : 'blue'}>
            {variable.type}
          </Tag>
          {variable.required && <Tag color="red">Required</Tag>}
        </Space>
      }
      extra={
        <Button 
          type="text" 
          size="small"
          icon={<CopyOutlined />}
          onClick={() => handleInsertVariable(variable.name)}
        >
          Insert
        </Button>
      }
    >
      <Space direction="vertical" size="small" style={{ width: '100%' }}>
        <Text type="secondary">{variable.description}</Text>
        <div>
          <Text strong>Examples: </Text>
          <Space wrap>
            {variable.examples.map((example, index) => (
              <Tag key={index} color="green">{example}</Tag>
            ))}
          </Space>
        </div>
      </Space>
    </Card>
  );

  return (
    <Card 
      title={
        <Space>
          <InfoCircleOutlined />
          <Text>Variable Helper</Text>
        </Space>
      }
      size="small"
      style={{ height: '100%' }}
    >
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        <Alert
          message="Variable Usage Instructions"
          description={
            <div>
              <Paragraph style={{ margin: 0, fontSize: '12px' }}>
                1. Use <Text code>{`{{variable_name}}`}</Text> format to define variables
              </Paragraph>
              <Paragraph style={{ margin: 0, fontSize: '12px' }}>
                2. Click variables to insert them into the editor
              </Paragraph>
              <Paragraph style={{ margin: 0, fontSize: '12px' }}>
                3. Variable names can only contain letters, numbers, and underscores
              </Paragraph>
            </div>
          }
          type="info"
          showIcon
          style={{ fontSize: '12px' }}
        />

        {!currentCategoryCode && (
          <Alert
            message="Please select a category"
            description="Select a category / module in the form above to see relevant variables"
            type="warning"
            showIcon
            style={{ fontSize: '11px', padding: '8px 12px' }}
          />
        )}

        {currentCategoryCode && currentVariables.length === 0 && (
          <Alert
            message="No variables registered"
            description={`Category "${currentCategoryCode}" does not have predefined variables yet.`}
            type="info"
            showIcon
            style={{ fontSize: '11px', padding: '8px 12px' }}
          />
        )}

        {currentCategoryCode && renderQuickVariables()}

        {currentCategoryCode && currentVariables.length > 0 && <Divider style={{ margin: '8px 0' }} />}

        {currentCategoryCode && currentVariables.length > 0 && (
          <Input
            size="small"
            placeholder="Search variables..."
            value={searchText}
            onChange={e => setSearchText(e.target.value)}
            allowClear
            prefix={<SearchOutlined />}
          />
        )}

        {currentCategoryCode && currentVariables.length > 0 ? (
          <div style={{ maxHeight: '400px', overflowY: 'auto' }}>
            {Object.keys(groupedVariables).length > 0 ? (
              <Collapse size="small" ghost defaultActiveKey={Object.keys(groupedVariables)}>
                {Object.entries(groupedVariables).map(([group, variables]) => (
                  <Panel 
                    key={group}
                    header={
                      <Space>
                        <Text strong>{group}</Text>
                        <Tag>{variables.length}</Tag>
                      </Space>
                    }
                  >
                    {variables.map(renderVariableInfo)}
                  </Panel>
                ))}
              </Collapse>
            ) : (
              <div style={{ textAlign: 'center', padding: '20px' }}>
                <QuestionCircleOutlined style={{ fontSize: '24px', color: '#ccc' }} />
                <Paragraph type="secondary" style={{ marginTop: '8px' }}>
                  No matching variables found
                </Paragraph>
              </div>
            )}
          </div>
        ) : !currentCategoryCode ? (
          <div style={{ textAlign: 'center', padding: '40px 20px' }}>
            <InfoCircleOutlined style={{ fontSize: '32px', color: '#d9d9d9' }} />
            <Paragraph type="secondary" style={{ marginTop: '12px' }}>
              Select a category to view available variables
            </Paragraph>
          </div>
        ) : null}
      </Space>
    </Card>
  );
};

export default VariableHelper;
