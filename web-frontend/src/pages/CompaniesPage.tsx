import React, { useState, useEffect } from 'react';
import {
  Card,
  Table,
  Input,
  Select,
  Button,
  Space,
  Row,
  Col,
  message,
  Statistic,
  Tag,
  Tooltip,
  Drawer,
  Typography,
  Divider,
  Alert,
  Modal
} from 'antd';
import {
  SearchOutlined,
  ReloadOutlined,
  DownloadOutlined,
  FilterOutlined,
  InfoCircleOutlined,
  SettingOutlined,
  DatabaseOutlined,
  GlobalOutlined
} from '@ant-design/icons';
import { useGlobalLogger } from '../contexts/LoggerContext';
import { exchangesApi } from '../services/api';

const { Search } = Input;
const { Option } = Select;
const { Title, Text } = Typography;

interface Company {
  code: string;
  name: string;
  exchange: string;
  type?: string;
  currency?: string;
  country?: string;
  isin?: string;
  data_source?: string;
  ticker_active: boolean;
  last_update?: string;
  sector?: string;
  industry?: string;
}

interface Exchange {
  code: string;
  name: string;
  country: string;
  timezone: string;
  mic_code?: string;
  enabled: boolean;
  last_update?: string;
  total_tickers: number;
  created_at?: string;
}

interface TickerStatistics {
  total: number;
  by_type: Record<string, number>;
  by_exchange: Record<string, number>;
  by_source: Record<string, number>;
}

const CompaniesPage: React.FC = () => {
  const globalLogger = useGlobalLogger();
  const [companies, setCompanies] = useState<Company[]>([]);
  const [exchanges, setExchanges] = useState<Exchange[]>([]);
  const [loading, setLoading] = useState(false);
  const [statistics, setStatistics] = useState<TickerStatistics | null>(null);
  const [selectedExchange, setSelectedExchange] = useState<string>('');
  const [selectedType, setSelectedType] = useState<string>('');
  const [searchText, setSearchText] = useState('');
  const [availableTypes, setAvailableTypes] = useState<string[]>([]);
  const [drawerVisible, setDrawerVisible] = useState(false);
  const [exchangeManagementVisible, setExchangeManagementVisible] = useState(false);
  const [selectedCompany, setSelectedCompany] = useState<Company | null>(null);

  // 获取交易所列表
  const fetchExchanges = async () => {
    try {
      const response = await exchangesApi.getExchanges();
      setExchanges(response);
      globalLogger.actions.addLog({ level: 'info', message: '交易所列表加载成功', source: 'CompaniesPage' });
    } catch (error) {
      globalLogger.actions.addLog({ level: 'error', message: `加载交易所列表失败: ${error}`, source: 'CompaniesPage' });
      message.error('加载交易所列表失败');
    }
  };

  // 获取公司列表
  const fetchCompanies = async () => {
    setLoading(true);
    try {
      const params: any = {
        limit: 1000
      };
      
      if (selectedExchange) {
        params.exchange = selectedExchange;
      }
      if (selectedType) {
        params.ticker_type = selectedType;
      }
      if (searchText) {
        params.search = searchText;
      }

      const response = await exchangesApi.getTickers(params);
      setCompanies(response);
      globalLogger.actions.addLog({ level: 'info', message: `加载了 ${response.length} 个公司`, source: 'CompaniesPage' });
    } catch (error) {
      globalLogger.actions.addLog({ level: 'error', message: `加载公司列表失败: ${error}`, source: 'CompaniesPage' });
      message.error('加载公司列表失败');
    } finally {
      setLoading(false);
    }
  };

  // 获取统计信息
  const fetchStatistics = async () => {
    try {
      const params: any = {};
      if (selectedExchange) {
        params.exchange = selectedExchange;
      }
      
      const response = await exchangesApi.getStatistics(params);
      setStatistics(response.data);
    } catch (error) {
      globalLogger.actions.addLog({ level: 'error', message: `加载统计信息失败: ${error}`, source: 'CompaniesPage' });
    }
  };

  // 获取可用类型
  const fetchAvailableTypes = async () => {
    try {
      const params: any = {};
      if (selectedExchange) {
        params.exchange = selectedExchange;
      }
      
      const response = await exchangesApi.getAvailableTypes(params);
      setAvailableTypes(response.data);
    } catch (error) {
      globalLogger.actions.addLog({ level: 'error', message: `加载股票类型失败: ${error}`, source: 'CompaniesPage' });
    }
  };

  // 初始化数据
  useEffect(() => {
    fetchExchanges();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    fetchCompanies();
    fetchStatistics();
    fetchAvailableTypes();
  }, [selectedExchange, selectedType, searchText]); // eslint-disable-line react-hooks/exhaustive-deps

  // 处理搜索
  const handleSearch = (value: string) => {
    setSearchText(value);
  };

  // 重置筛选
  const handleReset = () => {
    setSelectedExchange('');
    setSelectedType('');
    setSearchText('');
  };

  // 查看公司详情
  const handleViewDetails = (company: Company) => {
    setSelectedCompany(company);
    setDrawerVisible(true);
  };

  // 下载数据
  const handleDownload = () => {
    const csvContent = [
      ['Code', 'Name', 'Exchange', 'Type', 'Currency', 'Country', 'ISIN', 'Data Source', 'Active', 'Last Update', 'Sector', 'Industry'].join(','),
      ...companies.map(company => [
        company.code,
        `"${company.name || ''}"`,
        company.exchange,
        company.type || '',
        company.currency || '',
        company.country || '',
        company.isin || '',
        company.data_source || '',
        company.ticker_active ? 'Yes' : 'No',
        company.last_update || '',
        company.sector || '',
        company.industry || ''
      ].join(','))
    ].join('\n');

    const blob = new Blob([csvContent], { type: 'text/csv' });
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `companies_${new Date().toISOString().split('T')[0]}.csv`;
    a.click();
    window.URL.revokeObjectURL(url);
  };

  // 表格列配置
  const columns = [
    {
      title: 'Code',
      dataIndex: 'code',
      key: 'code',
      width: 120,
      fixed: 'left' as const,
      render: (text: string, record: Company) => (
        <Button
          type="link"
          onClick={() => handleViewDetails(record)}
          style={{ padding: 0, fontWeight: 'bold' }}
        >
          {text}
        </Button>
      ),
    },
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
      ellipsis: true,
      render: (text: string) => (
        <Tooltip title={text}>
          {text}
        </Tooltip>
      ),
    },
    {
      title: 'Exchange',
      dataIndex: 'exchange',
      key: 'exchange',
      width: 100,
      render: (text: string) => <Tag color="blue">{text}</Tag>,
    },
    {
      title: 'Type',
      dataIndex: 'type',
      key: 'type',
      width: 100,
      render: (text: string) => text ? <Tag color="green">{text}</Tag> : '-',
    },
    {
      title: 'Currency',
      dataIndex: 'currency',
      key: 'currency',
      width: 80,
    },
    {
      title: 'Country',
      dataIndex: 'country',
      key: 'country',
      width: 100,
    },
    {
      title: 'Sector',
      dataIndex: 'sector',
      key: 'sector',
      width: 120,
      ellipsis: true,
    },
    {
      title: 'Industry',
      dataIndex: 'industry',
      key: 'industry',
      width: 150,
      ellipsis: true,
    },
    {
      title: 'Data Source',
      dataIndex: 'data_source',
      key: 'data_source',
      width: 100,
      render: (text: string) => {
        const color = text === 'api' ? 'orange' : 'default';
        return <Tag color={color}>{text || 'manual'}</Tag>;
      },
    },
    {
      title: 'Active',
      dataIndex: 'ticker_active',
      key: 'ticker_active',
      width: 80,
      render: (active: boolean) => (
        <Tag color={active ? 'green' : 'red'}>
          {active ? 'Yes' : 'No'}
        </Tag>
      ),
    },
  ];

  return (
    <div style={{ padding: '24px' }}>
      {/* 顶部统计卡片 */}
      <Row gutter={16} style={{ marginBottom: 24 }}>
        <Col span={6}>
          <Card>
            <Statistic
              title="Total Companies"
              value={statistics?.total || 0}
              prefix={<DatabaseOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="Exchanges"
              value={Object.keys(statistics?.by_exchange || {}).length}
              prefix={<GlobalOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="API Sources"
              value={statistics?.by_source?.api || 0}
              prefix={<DownloadOutlined />}
            />
          </Card>
        </Col>
        <Col span={6}>
          <Card>
            <Statistic
              title="Manual Sources"
              value={statistics?.by_source?.manual || 0}
              prefix={<SettingOutlined />}
            />
          </Card>
        </Col>
      </Row>

      <Card
        title="Companies Database"
        extra={
          <Space>
            <Button
              icon={<SettingOutlined />}
              onClick={() => setExchangeManagementVisible(true)}
            >
              Exchange Management
            </Button>
            <Button
              icon={<DownloadOutlined />}
              onClick={handleDownload}
              disabled={companies.length === 0}
            >
              Export CSV
            </Button>
            <Button
              icon={<ReloadOutlined />}
              onClick={fetchCompanies}
              loading={loading}
            >
              Refresh
            </Button>
          </Space>
        }
      >
        {/* 筛选区域 */}
        <Row gutter={16} style={{ marginBottom: 16 }}>
          <Col span={6}>
            <Select
              placeholder="All Exchanges"
              style={{ width: '100%' }}
              allowClear
              value={selectedExchange || undefined}
              onChange={setSelectedExchange}
              showSearch
              filterOption={(input, option) =>
                (option?.label as string)?.toLowerCase().includes(input.toLowerCase())
              }
            >
              {exchanges.map(exchange => (
                <Option key={exchange.code} value={exchange.code}>
                  {exchange.code} - {exchange.name}
                </Option>
              ))}
            </Select>
          </Col>
          <Col span={4}>
            <Select
              placeholder="All Types"
              style={{ width: '100%' }}
              allowClear
              value={selectedType || undefined}
              onChange={setSelectedType}
            >
              {availableTypes.map(type => (
                <Option key={type} value={type}>
                  {type}
                </Option>
              ))}
            </Select>
          </Col>
          <Col span={8}>
            <Search
              placeholder="Search by code or name..."
              allowClear
              enterButton={<SearchOutlined />}
              onSearch={handleSearch}
            />
          </Col>
          <Col span={6}>
            <Space>
              <Button icon={<FilterOutlined />} onClick={handleReset}>
                Reset Filters
              </Button>
              <Tooltip title="View detailed statistics">
                <Button
                  icon={<InfoCircleOutlined />}
                  onClick={() => {
                    if (statistics) {
                      Modal.info({
                        title: 'Detailed Statistics',
                        width: 600,
                        content: (
                          <div>
                            <div style={{ marginBottom: 16 }}>
                              <strong>By Type:</strong>
                              <div>
                                {Object.entries(statistics.by_type).map(([type, count]) => (
                                  <Tag key={type} style={{ margin: '4px 4px 4px 0' }}>
                                    {type}: {count}
                                  </Tag>
                                ))}
                              </div>
                            </div>
                            <div style={{ marginBottom: 16 }}>
                              <strong>By Exchange:</strong>
                              <div>
                                {Object.entries(statistics.by_exchange).map(([exchange, count]) => (
                                  <Tag key={exchange} color="blue" style={{ margin: '4px 4px 4px 0' }}>
                                    {exchange}: {count}
                                  </Tag>
                                ))}
                              </div>
                            </div>
                            <div>
                              <strong>By Data Source:</strong>
                              <div>
                                {Object.entries(statistics.by_source).map(([source, count]) => (
                                  <Tag key={source} color="orange" style={{ margin: '4px 4px 4px 0' }}>
                                    {source}: {count}
                                  </Tag>
                                ))}
                              </div>
                            </div>
                          </div>
                        ),
                      });
                    }
                  }}
                >
                  Statistics
                </Button>
              </Tooltip>
            </Space>
          </Col>
        </Row>

        {/* 数据表格 */}
        <Table
          columns={columns}
          dataSource={companies}
          rowKey="code"
          loading={loading}
          scroll={{ x: 1200 }}
          pagination={{
            total: companies.length,
            pageSize: 50,
            showSizeChanger: true,
            showQuickJumper: true,
            showTotal: (total, range) =>
              `${range[0]}-${range[1]} of ${total} items`,
          }}
        />
      </Card>

      {/* 公司详情抽屉 */}
      <Drawer
        title={`Company Details: ${selectedCompany?.code}`}
        placement="right"
        size="large"
        onClose={() => setDrawerVisible(false)}
        open={drawerVisible}
      >
        {selectedCompany && (
          <div>
            <Title level={4}>{selectedCompany.name}</Title>
            <Divider />
            
            <Row gutter={[16, 16]}>
              <Col span={12}>
                <Text strong>Code:</Text>
                <div>{selectedCompany.code}</div>
              </Col>
              <Col span={12}>
                <Text strong>Exchange:</Text>
                <div><Tag color="blue">{selectedCompany.exchange}</Tag></div>
              </Col>
              <Col span={12}>
                <Text strong>Type:</Text>
                <div>{selectedCompany.type ? <Tag color="green">{selectedCompany.type}</Tag> : 'N/A'}</div>
              </Col>
              <Col span={12}>
                <Text strong>Currency:</Text>
                <div>{selectedCompany.currency || 'N/A'}</div>
              </Col>
              <Col span={12}>
                <Text strong>Country:</Text>
                <div>{selectedCompany.country || 'N/A'}</div>
              </Col>
              <Col span={12}>
                <Text strong>ISIN:</Text>
                <div>{selectedCompany.isin || 'N/A'}</div>
              </Col>
              <Col span={12}>
                <Text strong>Sector:</Text>
                <div>{selectedCompany.sector || 'N/A'}</div>
              </Col>
              <Col span={12}>
                <Text strong>Industry:</Text>
                <div>{selectedCompany.industry || 'N/A'}</div>
              </Col>
              <Col span={12}>
                <Text strong>Data Source:</Text>
                <div>
                  <Tag color={selectedCompany.data_source === 'api' ? 'orange' : 'default'}>
                    {selectedCompany.data_source || 'manual'}
                  </Tag>
                </div>
              </Col>
              <Col span={12}>
                <Text strong>Active:</Text>
                <div>
                  <Tag color={selectedCompany.ticker_active ? 'green' : 'red'}>
                    {selectedCompany.ticker_active ? 'Yes' : 'No'}
                  </Tag>
                </div>
              </Col>
              {selectedCompany.last_update && (
                <Col span={24}>
                  <Text strong>Last Update:</Text>
                  <div>{new Date(selectedCompany.last_update).toLocaleString()}</div>
                </Col>
              )}
            </Row>
          </div>
        )}
      </Drawer>

      {/* 交易所管理模态框 */}
      <Modal
        title="Exchange Data Management"
        open={exchangeManagementVisible}
        onCancel={() => setExchangeManagementVisible(false)}
        footer={null}
        width={1000}
      >
        <Alert
          message="Exchange Data Management"
          description="This feature allows you to fetch and manage exchange ticker data. Use the Data Source Management in Settings for automated scheduling."
          type="info"
          showIcon
          style={{ marginBottom: 16 }}
        />
        
        <Table
          dataSource={exchanges}
          rowKey="code"
          pagination={false}
          scroll={{ y: 400 }}
          columns={[
            {
              title: 'Code',
              dataIndex: 'code',
              key: 'code',
              width: 80,
            },
            {
              title: 'Name',
              dataIndex: 'name',
              key: 'name',
              ellipsis: true,
            },
            {
              title: 'Country',
              dataIndex: 'country',
              key: 'country',
              width: 100,
            },
            {
              title: 'Tickers',
              dataIndex: 'total_tickers',
              key: 'total_tickers',
              width: 80,
              render: (value: number) => value || 0,
            },
            {
              title: 'Last Update',
              dataIndex: 'last_update',
              key: 'last_update',
              width: 160,
              render: (value: string) => value ? new Date(value).toLocaleString() : 'Never',
            },
            {
              title: 'Status',
              dataIndex: 'enabled',
              key: 'enabled',
              width: 80,
              render: (enabled: boolean) => (
                <Tag color={enabled ? 'green' : 'red'}>
                  {enabled ? 'Enabled' : 'Disabled'}
                </Tag>
              ),
            },
          ]}
        />
        
        <div style={{ marginTop: 16, textAlign: 'center' }}>
          <Button type="primary" onClick={() => setExchangeManagementVisible(false)}>
            Close
          </Button>
        </div>
      </Modal>
    </div>
  );
};

export default CompaniesPage;
