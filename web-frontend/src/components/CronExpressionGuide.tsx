import React, { useState } from 'react';
import {
  Typography,
  Table,
  Card,
  Space,
  Tag,
  Alert,
  List,
  Button,
  Collapse,
  Row,
  Col,
  Drawer
} from 'antd';
import {
  ClockCircleOutlined,
  InfoCircleOutlined,
  CopyOutlined,
  CheckCircleOutlined,
  GlobalOutlined
} from '@ant-design/icons';

const { Title, Text, Paragraph } = Typography;
const { Panel } = Collapse;

interface CronExpressionGuideProps {
  open: boolean;
  onCancel: () => void;
}

const CronExpressionGuide: React.FC<CronExpressionGuideProps> = ({
  open,
  onCancel
}) => {
  const [copiedExpression, setCopiedExpression] = useState<string>('');

  // 复制表达式到剪贴板
  const copyToClipboard = (expression: string) => {
    navigator.clipboard.writeText(expression);
    setCopiedExpression(expression);
    setTimeout(() => setCopiedExpression(''), 2000);
  };

  // Cron表达式格式说明
  const cronFormatData = [
    {
      field: '分钟',
      range: '0-59',
      examples: ['0', '30', '*/15', '0,30', '0-30'],
      description: '指定分钟，0表示整点'
    },
    {
      field: '小时',
      range: '0-23',
      examples: ['9', '17', '*/2', '9,17', '9-17'],
      description: '指定小时，24小时制'
    },
    {
      field: '日期',
      range: '1-31',
      examples: ['1', '15', '*', '1,15', '1-15'],
      description: '指定月份中的日期'
    },
    {
      field: '月份',
      range: '1-12',
      examples: ['1', '6', '*', '1,6', '1-6'],
      description: '指定月份，1表示1月'
    },
    {
      field: '星期',
      range: '0-7',
      examples: ['1', '5', '*', '1-5', '0,6'],
      description: '0和7都表示周日，1-6表示周一到周六'
    }
  ];

  // 常用Cron表达式示例
  const commonExamples = [
    {
      expression: '* * * * *',
      description: '每分钟执行',
      useCase: '需要实时监控的场景',
      category: '高频'
    },
    {
      expression: '*/5 * * * *',
      description: '每5分钟执行',
      useCase: '数据更新频率较高的场景',
      category: '高频'
    },
    {
      expression: '*/15 * * * *',
      description: '每15分钟执行',
      useCase: '新闻数据获取、市场数据更新',
      category: '中频'
    },
    {
      expression: '0 * * * *',
      description: '每小时执行',
      useCase: '定期数据同步',
      category: '中频'
    },
    {
      expression: '0 9 * * 1-5',
      description: '工作日上午9点执行',
      useCase: '美股开盘前数据准备',
      category: '工作日'
    },
    {
      expression: '0 17 * * 1-5',
      description: '工作日下午5点执行',
      useCase: '美股收盘后数据汇总',
      category: '工作日'
    },
    {
      expression: '0 9,17 * * 1-5',
      description: '工作日早上9点和下午5点执行',
      useCase: '美股开盘和收盘时间数据获取',
      category: '工作日'
    },
    {
      expression: '0 10 * * 1',
      description: '每周一上午10点执行',
      useCase: '周报数据生成',
      category: '周频'
    },
    {
      expression: '0 9 1 * *',
      description: '每月1号上午9点执行',
      useCase: '月度数据汇总',
      category: '月频'
    },
    {
      expression: '0 0 1 1 *',
      description: '每年1月1日零点执行',
      useCase: '年度数据备份',
      category: '年频'
    }
  ];

  // 特殊字符说明
  const specialCharacters = [
    {
      symbol: '*',
      meaning: '表示所有值',
      example: '0 * * * *',
      description: '每小时的第0分钟执行'
    },
    {
      symbol: '/',
      meaning: '表示间隔',
      example: '*/15 * * * *',
      description: '每15分钟执行一次'
    },
    {
      symbol: ',',
      meaning: '表示多个值',
      example: '0 9,17 * * 1-5',
      description: '工作日9点和17点执行'
    },
    {
      symbol: '-',
      meaning: '表示范围',
      example: '0 9-17 * * 1-5',
      description: '工作日9点到17点每小时执行'
    },
    {
      symbol: '?',
      meaning: '不指定值（仅用于日期或星期）',
      example: '0 9 * * ?',
      description: '每天9点执行，不指定星期'
    }
  ];

  // 时区说明
  const timezoneInfo = [
    {
      timezone: 'America/New_York',
      label: '美国纽约时区 (EST/EDT)',
      description: '纽约时间，适用于美股相关数据获取',
      offset: 'UTC-5 (EST) / UTC-4 (EDT)',
      recommended: '美股数据、经济事件'
    },
    {
      timezone: 'Asia/Shanghai',
      label: '中国北京时区 (CST)',
      description: '北京时间，适用于中国相关数据获取',
      offset: 'UTC+8',
      recommended: '中国股市数据、宏观经济指标'
    }
  ];

  const columns = [
    {
      title: '字段',
      dataIndex: 'field',
      key: 'field',
      width: 60
    },
    {
      title: '取值范围',
      dataIndex: 'range',
      key: 'range',
      width: 80
    },
    {
      title: '示例',
      dataIndex: 'examples',
      key: 'examples',
      width: 300,
      render: (examples: string[]) => (
        <Space wrap>
          {examples.map((example, index) => (
            <Tag key={index} color="blue" style={{ fontSize: '12px' }}>{example}</Tag>
          ))}
        </Space>
      )
    },
    {
      title: '说明',
      dataIndex: 'description',
      key: 'description'
    }
  ];

  const exampleColumns = [
    {
      title: '表达式',
      dataIndex: 'expression',
      key: 'expression',
      width: 150,
      render: (expression: string) => (
        <Space>
          <Text code style={{ fontSize: '12px' }}>{expression}</Text>
          <Button
            type="text"
            size="small"
            icon={copiedExpression === expression ? <CheckCircleOutlined /> : <CopyOutlined />}
            onClick={() => copyToClipboard(expression)}
          />
        </Space>
      )
    },
    {
      title: '描述',
      dataIndex: 'description',
      key: 'description',
      width: 120
    },
    {
      title: '使用场景',
      dataIndex: 'useCase',
      key: 'useCase',
      width: 200
    },
    {
      title: '分类',
      dataIndex: 'category',
      key: 'category',
      width: 80,
      render: (category: string) => {
        const colors = {
          '高频': 'red',
          '中频': 'orange',
          '工作日': 'blue',
          '周频': 'green',
          '月频': 'purple',
          '年频': 'geekblue'
        };
        return <Tag color={colors[category as keyof typeof colors]} style={{ fontSize: '12px' }}>{category}</Tag>;
      }
    }
  ];

  return (
    <Drawer
      title={
        <Space>
          <ClockCircleOutlined />
          Cron表达式使用说明
        </Space>
      }
      open={open}
      onClose={onCancel}
      width={800}
      placement="right"
      destroyOnClose
    >
      <div style={{ padding: '0' }}>
        {/* 基本介绍 */}
        <Card size="small" style={{ marginBottom: 16 }}>
          <Title level={5}>
            <InfoCircleOutlined /> 什么是Cron表达式？
          </Title>
          <Paragraph style={{ fontSize: '14px', marginBottom: 12 }}>
            Cron表达式是一种用于指定定时任务执行时间的字符串格式。它由5个字段组成，分别表示分钟、小时、日期、月份和星期。
          </Paragraph>
          <Alert
            message="格式：分 时 日 月 星期"
            description="例如：0 9 * * 1-5 表示工作日上午9点执行"
            type="info"
            showIcon
          />
        </Card>

        {/* 格式说明表格 */}
        <Card title="Cron表达式格式说明" size="small" style={{ marginBottom: 16 }}>
          <Table
            dataSource={cronFormatData}
            columns={columns}
            pagination={false}
            size="small"
            scroll={{ x: 400 }}
          />
        </Card>

        {/* 特殊字符说明 */}
        <Card title="特殊字符说明" size="small" style={{ marginBottom: 16 }}>
          <List
            dataSource={specialCharacters}
            renderItem={(item) => (
              <List.Item>
                <List.Item.Meta
                  title={
                    <Space>
                      <Text code>{item.symbol}</Text>
                      <Text strong>{item.meaning}</Text>
                    </Space>
                  }
                  description={
                    <Space direction="vertical" size="small">
                      <Text>示例：<Text code>{item.example}</Text></Text>
                      <Text type="secondary">{item.description}</Text>
                    </Space>
                  }
                />
              </List.Item>
            )}
          />
        </Card>

        {/* 常用示例 */}
        <Card title="常用Cron表达式示例" size="small" style={{ marginBottom: 16 }}>
          <Table
            dataSource={commonExamples}
            columns={exampleColumns}
            pagination={false}
            size="small"
            scroll={{ x: 600 }}
          />
        </Card>

        {/* 时区说明 */}
        <Card title={
          <Space>
            <GlobalOutlined />
            时区说明
          </Space>
        } size="small" style={{ marginBottom: 16 }}>
          <Alert
            message="重要提示"
            description="Cron表达式的执行时间会根据选择的时区而变化。同一个表达式在不同时区会有不同的执行时间。"
            type="warning"
            showIcon
            style={{ marginBottom: 16 }}
          />
          
          <Row gutter={16}>
            {timezoneInfo.map((tz) => (
              <Col span={12} key={tz.timezone}>
                <Card size="small" style={{ marginBottom: 16 }}>
                  <Card.Meta
                    title={
                      <Space>
                        <Text strong>{tz.label}</Text>
                        <Tag color="green">推荐</Tag>
                      </Space>
                    }
                    description={
                      <Space direction="vertical" size="small">
                        <Text>{tz.description}</Text>
                        <Text type="secondary">时区偏移：{tz.offset}</Text>
                        <Text type="secondary">推荐用途：{tz.recommended}</Text>
                      </Space>
                    }
                  />
                </Card>
              </Col>
            ))}
          </Row>
        </Card>

        {/* 使用建议 */}
        <Card title="使用建议" size="small" style={{ marginBottom: 16 }}>
          <Collapse defaultActiveKey={['1']}>
            <Panel header="数据获取频率建议" key="1">
              <List>
                <List.Item>
                  <List.Item.Meta
                    title="高频数据（每5-15分钟）"
                    description="适用于新闻数据、实时市场数据等需要及时更新的信息"
                  />
                </List.Item>
                <List.Item>
                  <List.Item.Meta
                    title="中频数据（每小时）"
                    description="适用于定期数据同步、状态检查等任务"
                  />
                </List.Item>
                <List.Item>
                  <List.Item.Meta
                    title="低频数据（每日/每周/每月）"
                    description="适用于数据汇总、报告生成、备份等任务"
                  />
                </List.Item>
              </List>
            </Panel>
            
            <Panel header="时区选择建议" key="2">
              <List>
                <List.Item>
                  <List.Item.Meta
                    title="美股相关数据"
                    description="建议使用 America/New_York 时区，确保在美股交易时间获取数据"
                  />
                </List.Item>
                <List.Item>
                  <List.Item.Meta
                    title="中国相关数据"
                    description="建议使用 Asia/Shanghai 时区，确保在中国时间获取数据"
                  />
                </List.Item>
              </List>
            </Panel>
            
            <Panel header="注意事项" key="3">
              <List>
                <List.Item>
                  <List.Item.Meta
                    title="避免过于频繁的执行"
                    description="过于频繁的任务执行可能会影响系统性能，建议根据实际需求设置合理的执行频率"
                  />
                </List.Item>
                <List.Item>
                  <List.Item.Meta
                    title="考虑数据源的时间特性"
                    description="某些数据源在特定时间更新，建议将任务执行时间设置在数据更新后"
                  />
                </List.Item>
                <List.Item>
                  <List.Item.Meta
                    title="测试表达式"
                    description="在正式使用前，建议先测试cron表达式的正确性，确保任务按预期时间执行"
                  />
                </List.Item>
              </List>
            </Panel>
          </Collapse>
        </Card>

        {/* 快速参考 */}
        <Card title="快速参考" size="small" style={{ marginBottom: 16 }}>
          <Row gutter={[8, 8]}>
            <Col span={24}>
              <Card size="small" title="高频任务" style={{ marginBottom: 8 }}>
                <List size="small">
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>*/5 * * * *</Text> - 每5分钟
                  </List.Item>
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>*/15 * * * *</Text> - 每15分钟
                  </List.Item>
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>0 * * * *</Text> - 每小时
                  </List.Item>
                </List>
              </Card>
            </Col>
            <Col span={24}>
              <Card size="small" title="工作日任务" style={{ marginBottom: 8 }}>
                <List size="small">
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>0 9 * * 1-5</Text> - 工作日9点
                  </List.Item>
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>0 17 * * 1-5</Text> - 工作日17点
                  </List.Item>
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>0 9,17 * * 1-5</Text> - 工作日9点和17点
                  </List.Item>
                </List>
              </Card>
            </Col>
            <Col span={24}>
              <Card size="small" title="定期任务">
                <List size="small">
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>0 10 * * 1</Text> - 每周一10点
                  </List.Item>
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>0 9 1 * *</Text> - 每月1号9点
                  </List.Item>
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>0 0 1 1 *</Text> - 每年1月1日
                  </List.Item>
                </List>
              </Card>
            </Col>
          </Row>
        </Card>
      </div>
    </Drawer>
  );
};

export default CronExpressionGuide;
