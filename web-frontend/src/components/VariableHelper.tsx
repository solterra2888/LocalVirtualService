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
  Input
} from 'antd';
import { 
  InfoCircleOutlined, 
  CopyOutlined, 
  QuestionCircleOutlined,
  SearchOutlined
} from '@ant-design/icons';
import { PromptCategory, PromptTemplateDetail } from '../services/promptTemplateService';

const { Text, Paragraph } = Typography;
const { Panel } = Collapse;

interface VariableHelperProps {
  categories: PromptCategory[];
  templates: PromptTemplateDetail[];
  onVariableInsert?: (variable: string) => void;
  currentCategoryId?: number;  // 当前编辑模板所属的分类ID
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
  onVariableInsert,
  currentCategoryId
}) => {
  const [searchText, setSearchText] = useState<string>('');
  const [variableInfo, setVariableInfo] = useState<VariableInfo[]>([]);

  // 根据 category 定义可用的变量（基于 category_code）
  const getCategoryAllowedVariables = (categoryCode?: string): string[] | null => {
    // null 表示显示所有变量（未选择category或未知category）
    if (!categoryCode) return null;
    
    const categoryVariableMap: { [key: string]: string[] } = {
      'news_analysis': [
        // News table fields
        'title', 'content', 'date', 'source', 'link', 'formatted_text',
        // Aggregated fields
        'symbols',
        // Template parameters
        'max_length', 'language', 'format', 'temperature', 'model', 'provider'
      ],
      'theme_analysis': [
        // futu_investment_themes fields
        'theme_id', 'theme_name', 'market', 'theme_url',
        'change_percent', 'gainers', 'losers', 'unchanged',
        'top_stock', 'top_stock_change', 'stock_count',
        // Aggregated from futu_theme_stocks
        'stock_symbols', 'company_names',
        // Template parameters
        'language', 'format', 'temperature', 'model', 'provider'
      ]
    };
    
    return categoryVariableMap[categoryCode] || null;
  };

  // 获取当前category的code
  const getCurrentCategoryCode = (): string | undefined => {
    if (!currentCategoryId || !categories.length) return undefined;
    const category = categories.find(c => c.id === currentCategoryId);
    return category?.category_code;
  };

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
      // News analysis variables (新闻分析变量 - from news table)
      title: ['News Title', 'Company Announcement Title', 'Market Report Title'],
      content: ['News Content', 'Detailed Description', 'Full Text'],
      date: ['2024-01-15', '2025-10-23', '2024-12-31'],
      source: ['EODHD', 'Reuters', 'Bloomberg'],
      link: ['https://news.example.com/...', 'News URL', 'Article link'],
      formatted_text: ['Formatted news text', 'Processed content', 'Clean text'],
      
      // Aggregated from news_symbols (通过JOIN获取)
      symbols: ['AAPL,MSFT,GOOGL', '0700.HK,9988.HK', 'Stock symbols list'],
      
      // Template parameters (模板参数 - 非数据库字段)
      max_length: ['100', '200', '500'],
      language: ['Chinese', 'English', 'zh-CN'],
      format: ['JSON', 'Markdown', 'Plain Text'],
      
      // Investment theme variables (投资主题变量 - from futu_investment_themes)
      theme_id: ['BK1288', 'BK0123', 'EV_US_001'],
      theme_name: ['Electric Vehicles (电动汽车)', 'Artificial Intelligence', '5G Technology'],
      market: ['HK', 'US', 'CN'],
      theme_url: ['https://www.futunn.com/...', 'Theme URL', 'Link to theme page'],
      change_percent: ['+8.61%', '-2.34%', '+15.28%'],
      gainers: ['12', '8', '20'],
      losers: ['5', '3', '10'],
      unchanged: ['3', '2', '5'],
      top_stock: ['Tesla Inc.', 'NVIDIA Corp.', 'Apple Inc.'],
      top_stock_change: ['+12.5%', '-3.2%', '+8.9%'],
      stock_count: ['25', '50', '100'],
      
      // Aggregated from futu_theme_stocks (通过JOIN futu_theme_stocks获取)
      stock_symbols: ['TSLA, NIO, RIVN', 'NVDA, MSFT, GOOGL', 'AAPL, MSFT'],
      company_names: ['Tesla, NIO, Rivian', 'NVIDIA, Microsoft, Google', 'Apple, Microsoft'],
      
      // LLM configuration variables (LLM配置变量)
      temperature: ['0.7', '0.3', '1.0'],
      model: ['qwen-plus', 'doubao-seed', 'glm-4'],
      provider: ['qwen', 'doubao', 'glm']
    };

    return examples[varName] || [`Example ${varName} Value 1`, `Example ${varName} Value 2`];
  };

  // 过滤变量
  const filteredVariables = variableInfo.filter(variable => {
    // 1. 首先根据 currentCategoryId 过滤（如果有设置的话）
    const currentCategoryCode = getCurrentCategoryCode();
    const allowedVars = getCategoryAllowedVariables(currentCategoryCode);
    
    // 如果有 currentCategoryId 且定义了允许的变量列表，则只显示允许的变量
    if (allowedVars && !allowedVars.includes(variable.name)) {
      return false;
    }
    
    // 2. 根据搜索文本过滤
    const matchesSearch = !searchText || 
      variable.name.toLowerCase().includes(searchText.toLowerCase()) ||
      variable.description.toLowerCase().includes(searchText.toLowerCase());
    
    return matchesSearch;
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

  // 渲染快捷变量卡片（根据分类过滤）
  const renderQuickVariables = () => {
    const currentCategoryCode = getCurrentCategoryCode();
    const allowedVars = getCategoryAllowedVariables(currentCategoryCode);
    
    const newsVars = [
      { name: 'title', description: 'News Title - from news table' },
      { name: 'content', description: 'News Content - from news table' },
      { name: 'symbols', description: 'Stock Symbols - aggregated' },
      { name: 'date', description: 'Date - from news table' }
    ];

    const themeVars = [
      { name: 'theme_name', description: 'Theme Name - from futu_investment_themes' },
      { name: 'market', description: 'Market (HK/US/CN)' },
      { name: 'stock_symbols', description: 'Stock Symbols - aggregated' },
      { name: 'company_names', description: 'Company Names - aggregated' }
    ];

    // 根据当前分类过滤变量
    const filterVars = (vars: Array<{name: string, description: string}>) => {
      if (!allowedVars) return vars;
      return vars.filter(v => allowedVars.includes(v.name));
    };

    const filteredNewsVars = filterVars(newsVars);
    const filteredThemeVars = filterVars(themeVars);

    // 根据分类决定显示哪些分组
    const showNews = currentCategoryCode === 'news_analysis';
    const showTheme = currentCategoryCode === 'theme_analysis';

    if (!showNews && !showTheme) return null;

    return (
      <Card title="Quick Insert" size="small" style={{ marginBottom: 8 }}>
        <Space direction="vertical" style={{ width: '100%' }} size="small">
          {currentCategoryCode && (
            <Alert
              message={`Filtered for: ${categories.find(c => c.id === currentCategoryId)?.category_name || 'Unknown Category'}`}
              type="info"
              showIcon
              style={{ fontSize: '11px', padding: '4px 8px' }}
            />
          )}

          {showNews && filteredNewsVars.length > 0 && (
            <div>
              <Text strong style={{ fontSize: '11px', color: '#666' }}>News Analysis</Text>
              <div style={{ marginTop: '4px' }}>
                <Space wrap size="small">
                  {filteredNewsVars.map(v => (
                    <Tooltip key={v.name} title={v.description}>
                      <Tag 
                        color="blue" 
                        style={{ cursor: 'pointer', fontSize: '11px' }}
                        onClick={() => handleInsertVariable(v.name)}
                      >
                        {`{{${v.name}}}`}
                      </Tag>
                    </Tooltip>
                  ))}
                </Space>
              </div>
            </div>
          )}

          {showTheme && filteredThemeVars.length > 0 && (
            <div>
              <Text strong style={{ fontSize: '11px', color: '#666' }}>Investment Themes</Text>
              <div style={{ marginTop: '4px' }}>
                <Space wrap size="small">
                  {filteredThemeVars.map(v => (
                    <Tooltip key={v.name} title={v.description}>
                      <Tag 
                        color="green" 
                        style={{ cursor: 'pointer', fontSize: '11px' }}
                        onClick={() => handleInsertVariable(v.name)}
                      >
                        {`{{${v.name}}}`}
                      </Tag>
                    </Tooltip>
                  ))}
                </Space>
              </div>
            </div>
          )}
        </Space>
      </Card>
    );
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
        {/* 使用说明 */}
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

        {/* 分类提示 */}
        {!currentCategoryId && (
          <Alert
            message="Please select a category"
            description="Select a category in the form above to see relevant variables"
            type="warning"
            showIcon
            style={{ fontSize: '11px', padding: '8px 12px' }}
          />
        )}

        {/* 快捷变量插入（根据分类过滤） */}
        {currentCategoryId && renderQuickVariables()}

        {currentCategoryId && <Divider style={{ margin: '8px 0' }} />}

        {/* 筛选器 */}
        <Row gutter={8}>
          {/* 只显示搜索框，category 由弹窗的 Category 下拉框控制 */}
          <Col span={24}>
            <Input
              size="small"
              placeholder="Search variables..."
              value={searchText}
              onChange={e => setSearchText(e.target.value)}
              allowClear
              prefix={<SearchOutlined />}
            />
          </Col>
        </Row>

        {/* 变量列表 */}
        {currentCategoryId ? (
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
                  No matching variables found
                </Paragraph>
              </div>
            )}
          </div>
        ) : (
          <div style={{ textAlign: 'center', padding: '40px 20px' }}>
            <InfoCircleOutlined style={{ fontSize: '32px', color: '#d9d9d9' }} />
            <Paragraph type="secondary" style={{ marginTop: '12px' }}>
              Select a category to view available variables
            </Paragraph>
          </div>
        )}

        {/* 提示信息 */}
        {currentCategoryId && (
          <Alert
            message="Tips"
            description={
              <div>
                <Paragraph style={{ margin: 0, fontSize: '12px' }}>
                  • Variables shown are filtered based on the selected category
                </Paragraph>
                <Paragraph style={{ margin: 0, fontSize: '12px' }}>
                  • Supported types: string, integer, float, boolean, array, object
                </Paragraph>
                <Paragraph style={{ margin: 0, fontSize: '12px' }}>
                  • Click a variable to insert it into the template
                </Paragraph>
              </div>
            }
            type="info"
            showIcon
            style={{ fontSize: '12px' }}
          />
        )}
      </Space>
    </Card>
  );
};

export default VariableHelper;
