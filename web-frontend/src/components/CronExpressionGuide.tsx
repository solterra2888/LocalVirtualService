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

  // Copy expression to clipboard
  const copyToClipboard = (expression: string) => {
    navigator.clipboard.writeText(expression);
    setCopiedExpression(expression);
    setTimeout(() => setCopiedExpression(''), 2000);
  };

  // Cron expression format description
  const cronFormatData = [
    {
      field: 'Minutes',
      range: '0-59',
      examples: ['0', '30', '*/15', '0,30', '0-30'],
      description: 'Specify minutes, 0 means on the hour'
    },
    {
      field: 'Hours',
      range: '0-23',
      examples: ['9', '17', '*/2', '9,17', '9-17'],
      description: 'Specify hours, 24-hour format'
    },
    {
      field: 'Day of Month',
      range: '1-31',
      examples: ['1', '15', '*', '1,15', '1-15'],
      description: 'Specify day of the month'
    },
    {
      field: 'Month',
      range: '1-12',
      examples: ['1', '6', '*', '1,6', '1-6'],
      description: 'Specify month, 1 means January'
    },
    {
      field: 'Day of Week',
      range: '0-6',
      examples: ['1', '5', '*', '1-5', '0,6'],
      description: '0 means Sunday, 1-6 means Monday to Saturday'
    }
  ];

  // Common Cron expression examples
  const commonExamples = [
    {
      expression: '* * * * *',
      description: 'Execute every minute',
      useCase: 'Scenarios requiring real-time monitoring',
      category: 'High Frequency'
    },
    {
      expression: '*/5 * * * *',
      description: 'Execute every 5 minutes',
      useCase: 'Scenarios with high data update frequency',
      category: 'High Frequency'
    },
    {
      expression: '*/15 * * * *',
      description: 'Execute every 15 minutes',
      useCase: 'News data acquisition, market data updates',
      category: 'Medium Frequency'
    },
    {
      expression: '0 * * * *',
      description: 'Execute every hour',
      useCase: 'Regular data synchronization',
      category: 'Medium Frequency'
    },
    {
      expression: '0 */2 * * *',
      description: 'Execute every 2 hours',
      useCase: 'Periodic data collection',
      category: 'Medium Frequency'
    },
    {
      expression: '0 9 * * 1-5',
      description: 'Execute at 9 AM on weekdays',
      useCase: 'Business hours data processing',
      category: 'Business Hours'
    },
    {
      expression: '0 9,17 * * 1-5',
      description: 'Execute at 9 AM and 5 PM on weekdays',
      useCase: 'Morning and evening data updates',
      category: 'Business Hours'
    },
    {
      expression: '0 0 * * 0',
      description: 'Execute at midnight on Sundays',
      useCase: 'Weekly data processing',
      category: 'Weekly'
    },
    {
      expression: '0 0 1 * *',
      description: 'Execute at midnight on the 1st of each month',
      useCase: 'Monthly data processing',
      category: 'Monthly'
    },
    {
      expression: '0 0 1 1 *',
      description: 'Execute at midnight on January 1st',
      useCase: 'Annual data processing',
      category: 'Annual'
    }
  ];

  // Special character description
  const specialCharacters = [
    {
      symbol: '*',
      meaning: 'Represents all values',
      example: '0 * * * *',
      description: 'Execute at 0th minute of every hour'
    },
    {
      symbol: '/',
      meaning: 'Represents interval',
      example: '*/15 * * * *',
      description: 'Execute every 15 minutes'
    },
    {
      symbol: ',',
      meaning: 'Represents multiple values',
      example: '0 9,17 * * 1-5',
      description: 'Execute at 9 AM and 5 PM on weekdays'
    },
    {
      symbol: '-',
      meaning: 'Represents range',
      example: '0 9-17 * * 1-5',
      description: 'Execute every hour from 9 AM to 5 PM on weekdays'
    },
    {
      symbol: '?',
      meaning: 'Does not specify a value (only for date or day of week)',
      example: '0 9 * * ?',
      description: 'Execute at 9 AM every day, without specifying the day of the week'
    }
  ];

  // Timezone description
  const timezoneInfo = [
    {
      timezone: 'America/New_York',
      label: 'America/New_York Time Zone (EST/EDT)',
      description: 'New York time, suitable for US stock market data acquisition',
      offset: 'UTC-5 (EST) / UTC-4 (EDT)',
      recommended: 'US stock data, economic events'
    },
    {
      timezone: 'Asia/Shanghai',
      label: 'Asia/Shanghai Time Zone (CST)',
      description: 'Beijing time, suitable for Chinese data acquisition',
      offset: 'UTC+8',
      recommended: 'Chinese stock data, macroeconomic indicators'
    }
  ];

  const columns = [
    {
      title: 'Field',
      dataIndex: 'field',
      key: 'field',
      width: 60
    },
    {
      title: 'Range',
      dataIndex: 'range',
      key: 'range',
      width: 80
    },
    {
      title: 'Example',
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
      title: 'Description',
      dataIndex: 'description',
      key: 'description'
    }
  ];

  const exampleColumns = [
    {
      title: 'Expression',
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
      title: 'Description',
      dataIndex: 'description',
      key: 'description',
      width: 120
    },
    {
      title: 'Use Case',
      dataIndex: 'useCase',
      key: 'useCase',
      width: 200
    },
    {
      title: 'Category',
      dataIndex: 'category',
      key: 'category',
      width: 80,
      render: (category: string) => {
        const colors = {
          'High Frequency': 'red',
          'Medium Frequency': 'orange',
          'Business Hours': 'blue',
          'Weekly': 'green',
          'Monthly': 'purple',
          'Annual': 'geekblue'
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
          Cron Expression Guide
        </Space>
      }
      open={open}
      onClose={onCancel}
      width={800}
      placement="right"
      destroyOnClose
    >
      <div style={{ padding: '0' }}>
        {/* Basic Introduction */}
        <Card size="small" style={{ marginBottom: 16 }}>
          <Title level={5}>
            <InfoCircleOutlined /> What is a Cron Expression?
          </Title>
          <Paragraph style={{ fontSize: '14px', marginBottom: 12 }}>
            A Cron expression is a string format used to specify the execution time of a scheduled task. It consists of 5 fields, representing minutes, hours, day of month, month, and day of week.
          </Paragraph>
          <Alert
            message="Format: Minute Hour Day Month Week"
            description="For example: 0 9 * * 1-5 means execute at 9 AM on weekdays"
            type="info"
            showIcon
          />
        </Card>

        {/* Format Description Table */}
        <Card title="Cron Expression Format Description" size="small" style={{ marginBottom: 16 }}>
          <Table
            dataSource={cronFormatData}
            columns={columns}
            pagination={false}
            size="small"
            scroll={{ x: 400 }}
          />
        </Card>

        {/* Special Character Description */}
        <Card title="Special Character Description" size="small" style={{ marginBottom: 16 }}>
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
                      <Text>Example: <Text code>{item.example}</Text></Text>
                      <Text type="secondary">{item.description}</Text>
                    </Space>
                  }
                />
              </List.Item>
            )}
          />
        </Card>

        {/* Common Examples */}
        <Card title="Common Cron Expression Examples" size="small" style={{ marginBottom: 16 }}>
          <Table
            dataSource={commonExamples}
            columns={exampleColumns}
            pagination={false}
            size="small"
            scroll={{ x: 600 }}
          />
        </Card>

        {/* Timezone Description */}
        <Card title={
          <Space>
            <GlobalOutlined />
            Timezone Description
          </Space>
        } size="small" style={{ marginBottom: 16 }}>
          <Alert
            message="Important Note"
            description="The execution time of a Cron expression will vary based on the selected timezone. The same expression will have different execution times in different timezones."
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
                        <Tag color="green">Recommended</Tag>
                      </Space>
                    }
                    description={
                      <Space direction="vertical" size="small">
                        <Text>{tz.description}</Text>
                        <Text type="secondary">Timezone Offset: {tz.offset}</Text>
                        <Text type="secondary">Recommended Use: {tz.recommended}</Text>
                      </Space>
                    }
                  />
                </Card>
              </Col>
            ))}
          </Row>
        </Card>

        {/* Usage Suggestions */}
        <Card title="Usage Suggestions" size="small" style={{ marginBottom: 16 }}>
          <Collapse defaultActiveKey={['1']}>
            <Panel header="Data Acquisition Frequency Suggestions" key="1">
              <List>
                <List.Item>
                  <List.Item.Meta
                    title="High Frequency (every 5-15 minutes)"
                    description="Suitable for news data, real-time market data, etc., requiring timely updates"
                  />
                </List.Item>
                <List.Item>
                  <List.Item.Meta
                    title="Medium Frequency (every hour)"
                    description="Suitable for periodic data synchronization, status checks, etc."
                  />
                </List.Item>
                <List.Item>
                  <List.Item.Meta
                    title="Low Frequency (daily/weekly/monthly)"
                    description="Suitable for data aggregation, report generation, backups, etc."
                  />
                </List.Item>
              </List>
            </Panel>
            
            <Panel header="Timezone Selection Suggestions" key="2">
              <List>
                <List.Item>
                  <List.Item.Meta
                    title="US Stock Related Data"
                    description="It is recommended to use the America/New_York timezone to ensure data acquisition during US stock trading hours"
                  />
                </List.Item>
                <List.Item>
                  <List.Item.Meta
                    title="Chinese Related Data"
                    description="It is recommended to use the Asia/Shanghai timezone to ensure data acquisition during Chinese time"
                  />
                </List.Item>
              </List>
            </Panel>
            
            <Panel header="Considerations" key="3">
              <List>
                <List.Item>
                  <List.Item.Meta
                    title="Avoid excessive execution"
                    description="Excessively frequent task execution may affect system performance. It is recommended to set a reasonable execution frequency based on actual needs."
                  />
                </List.Item>
                <List.Item>
                  <List.Item.Meta
                    title="Consider data source time characteristics"
                    description="Some data sources update at specific times. It is recommended to set the task execution time after the data update."
                  />
                </List.Item>
                <List.Item>
                  <List.Item.Meta
                    title="Test the expression"
                    description="Before officially using it, it is recommended to test the correctness of the cron expression to ensure that the task executes as expected at the specified time."
                  />
                </List.Item>
              </List>
            </Panel>
          </Collapse>
        </Card>

        {/* Quick Reference */}
        <Card title="Quick Reference" size="small" style={{ marginBottom: 16 }}>
          <Row gutter={[8, 8]}>
            <Col span={24}>
              <Card size="small" title="High Frequency Tasks" style={{ marginBottom: 8 }}>
                <List size="small">
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>*/5 * * * *</Text> - Every 5 minutes
                  </List.Item>
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>*/15 * * * *</Text> - Every 15 minutes
                  </List.Item>
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>0 * * * *</Text> - Every hour
                  </List.Item>
                </List>
              </Card>
            </Col>
            <Col span={24}>
              <Card size="small" title="Business Hours Tasks" style={{ marginBottom: 8 }}>
                <List size="small">
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>0 9 * * 1-5</Text> - 9 AM on weekdays
                  </List.Item>
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>0 17 * * 1-5</Text> - 5 PM on weekdays
                  </List.Item>
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>0 9,17 * * 1-5</Text> - 9 AM and 5 PM on weekdays
                  </List.Item>
                </List>
              </Card>
            </Col>
            <Col span={24}>
              <Card size="small" title="Periodic Tasks">
                <List size="small">
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>0 0 * * 0</Text> - Midnight on Sundays
                  </List.Item>
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>0 0 1 * *</Text> - Midnight on the 1st of each month
                  </List.Item>
                  <List.Item style={{ padding: '4px 0' }}>
                    <Text code style={{ fontSize: '12px' }}>0 0 1 1 *</Text> - Midnight on January 1st
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
