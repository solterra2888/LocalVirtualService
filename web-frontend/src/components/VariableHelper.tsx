import React, { useState, useEffect } from 'react';
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
  Row,
  Col,
  Input,
  Select
} from 'antd';
import { 
  InfoCircleOutlined, 
  CopyOutlined, 
  QuestionCircleOutlined
} from '@ant-design/icons';
import { PromptCategory, PromptTemplateDetail } from '../services/promptTemplateService';

const { Text, Paragraph } = Typography;
const { Panel } = Collapse;
const { Option } = Select;

interface VariableHelperProps {
  categories: PromptCategory[];
  templates: PromptTemplateDetail[];
  onVariableInsert?: (variable: string) => void;
}

interface VariableInfo {
  name: string;
  type: string;
  description: string;
  required: boolean;
  default?: any;
  examples: string[];
  category: string;
}

const VariableHelper: React.FC<VariableHelperProps> = ({
  categories,
  templates,
  onVariableInsert
}) => {
  const [selectedCategory, setSelectedCategory] = useState<string>('all');
  const [searchText, setSearchText] = useState<string>('');
  const [variableInfo, setVariableInfo] = useState<VariableInfo[]>([]);

  // 从现有模板中提取变量信息
  useEffect(() => {
    const extractVariables = () => {
      const variableMap = new Map<string, VariableInfo>();

      templates.forEach(template => {
        if (template.variables) {
          Object.entries(template.variables).forEach(([varName, varDef]: [string, any]) => {
            if (!variableMap.has(varName)) {
              const categoryName = categories.find(c => c.id === template.category_id)?.category_name || 'Unknown';
              
              variableMap.set(varName, {
                name: varName,
                type: varDef.type || 'string',
                description: varDef.description || 'No description available',
                required: varDef.required || false,
                default: varDef.default,
                examples: generateVariableExamples(varName, varDef.type),
                category: categoryName
              });
            }
          });
        }
      });

      setVariableInfo(Array.from(variableMap.values()));
    };

    extractVariables();
  }, [templates, categories]);

  // 生成变量示例
  const generateVariableExamples = (varName: string, type: string): string[] => {
    const examples: { [key: string]: string[] } = {
      title: ['新闻标题', '公司公告标题', '市场报告标题'],
      content: ['新闻正文内容', '详细描述', '完整文本'],
      symbols: ['AAPL,MSFT,GOOGL', '股票代码列表', '相关证券代码'],
      max_length: ['100', '200', '500'],
      date: ['2024-01-15', '今天', '最近一周'],
      language: ['中文', 'English', 'zh-CN'],
      format: ['JSON', 'Markdown', 'Plain Text'],
      temperature: ['0.7', '0.3', '1.0'],
      model: ['qwen-plus', 'doubao-seed', 'glm-4'],
      provider: ['qwen', 'doubao', 'glm']
    };

    return examples[varName] || [`示例${varName}值1`, `示例${varName}值2`];
  };

  // 过滤变量
  const filteredVariables = variableInfo.filter(variable => {
    const matchesCategory = selectedCategory === 'all' || variable.category === selectedCategory;
    const matchesSearch = !searchText || 
      variable.name.toLowerCase().includes(searchText.toLowerCase()) ||
      variable.description.toLowerCase().includes(searchText.toLowerCase());
    
    return matchesCategory && matchesSearch;
  });

  // 按类别分组
  const groupedVariables = filteredVariables.reduce((acc, variable) => {
    if (!acc[variable.category]) {
      acc[variable.category] = [];
    }
    acc[variable.category].push(variable);
    return acc;
  }, {} as Record<string, VariableInfo[]>);

  // 插入变量到编辑器
  const handleInsertVariable = (variableName: string) => {
    const variableText = `{{${variableName}}}`;
    if (onVariableInsert) {
      onVariableInsert(variableText);
    } else {
      // 复制到剪贴板
      navigator.clipboard.writeText(variableText);
    }
  };

  // 渲染变量信息
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
        
        {variable.default !== undefined && (
          <div>
            <Text strong>Default: </Text>
            <Text code>{String(variable.default)}</Text>
          </div>
        )}
        
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

  // 渲染常用变量
  const renderCommonVariables = () => {
    const commonVars = [
      { name: 'title', description: '标题内容' },
      { name: 'content', description: '正文内容' },
      { name: 'symbols', description: '股票代码' },
      { name: 'date', description: '日期' },
      { name: 'language', description: '语言' },
      { name: 'format', description: '输出格式' },
      { name: 'max_length', description: '最大长度' },
      { name: 'temperature', description: '温度参数' }
    ];

    return (
      <Card title="常用变量" size="small">
        <Space wrap>
          {commonVars.map(varInfo => (
            <Tooltip key={varInfo.name} title={varInfo.description}>
              <Tag 
                color="blue" 
                style={{ cursor: 'pointer' }}
                onClick={() => handleInsertVariable(varInfo.name)}
              >
                {`{{${varInfo.name}}}`}
              </Tag>
            </Tooltip>
          ))}
        </Space>
      </Card>
    );
  };

  return (
    <Card 
      title={
        <Space>
          <InfoCircleOutlined />
          <Text>变量助手</Text>
        </Space>
      }
      size="small"
      style={{ height: '100%' }}
    >
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        {/* 使用说明 */}
        <Alert
          message="变量使用说明"
          description={
            <div>
              <Paragraph style={{ margin: 0, fontSize: '12px' }}>
                1. 使用 <Text code>{`{{variable_name}}`}</Text> 格式定义变量
              </Paragraph>
              <Paragraph style={{ margin: 0, fontSize: '12px' }}>
                2. 点击变量可插入到编辑器中
              </Paragraph>
              <Paragraph style={{ margin: 0, fontSize: '12px' }}>
                3. 变量名只能包含字母、数字和下划线
              </Paragraph>
            </div>
          }
          type="info"
          showIcon
          style={{ fontSize: '12px' }}
        />

        {/* 常用变量 */}
        {renderCommonVariables()}

        <Divider style={{ margin: '8px 0' }} />

        {/* 筛选器 */}
        <Row gutter={8}>
          <Col span={12}>
            <Select
              size="small"
              style={{ width: '100%' }}
              value={selectedCategory}
              onChange={setSelectedCategory}
            >
              <Option value="all">所有类别</Option>
              {categories.map(category => (
                <Option key={category.id} value={category.category_name}>
                  {category.icon} {category.category_name}
                </Option>
              ))}
            </Select>
          </Col>
          <Col span={12}>
            <Input
              size="small"
              placeholder="搜索变量..."
              value={searchText}
              onChange={e => setSearchText(e.target.value)}
              allowClear
            />
          </Col>
        </Row>

        {/* 变量列表 */}
        <div style={{ maxHeight: '400px', overflowY: 'auto' }}>
          {Object.keys(groupedVariables).length > 0 ? (
            <Collapse size="small" ghost>
              {Object.entries(groupedVariables).map(([category, variables]) => (
                <Panel 
                  key={category}
                  header={
                    <Space>
                      <Text strong>{category}</Text>
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
                没有找到匹配的变量
              </Paragraph>
            </div>
          )}
        </div>

        {/* 提示信息 */}
        <Alert
          message="提示"
          description={
            <div>
              <Paragraph style={{ margin: 0, fontSize: '12px' }}>
                • 变量定义在模板的 "Variables" 字段中
              </Paragraph>
              <Paragraph style={{ margin: 0, fontSize: '12px' }}>
                • 支持的类型: string, integer, float, boolean, array, object
              </Paragraph>
              <Paragraph style={{ margin: 0, fontSize: '12px' }}>
                • 可以为变量设置默认值和描述
              </Paragraph>
            </div>
          }
          type="warning"
          showIcon
          style={{ fontSize: '12px' }}
        />
      </Space>
    </Card>
  );
};

export default VariableHelper;
