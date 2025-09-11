import React, { useState } from 'react';
import {
  Card,
  Input,
  Button,
  Row,
  Col,
  Typography,
  Spin,
  message,
  Tabs,
  Table,
  Statistic,
  Progress,
  Tag,
  Space,
  Divider,
  Empty
} from 'antd';
import {
  SearchOutlined,
  ReloadOutlined,
  TrophyOutlined,
  DollarOutlined,
  LineChartOutlined,
  BankOutlined,
  TeamOutlined,
  GlobalOutlined
} from '@ant-design/icons';
import type { ColumnsType } from 'antd/es/table';

import { fundamentalsAPI, fundamentalsUtils, CompanyFundamentalsResponse } from '../services/fundamentals';
import { useAuthStore } from '../store/authStore';
import { getErrorMessage } from '../utils/errorHandler';

const { Title, Text, Paragraph } = Typography;
const { TabPane } = Tabs;

interface FundamentalsPageState {
  loading: boolean;
  searchQuery: string;
  fundamentalsData: CompanyFundamentalsResponse | null;
}

const FundamentalsPage: React.FC = () => {
  // const { user, isAuthenticated } = useAuthStore(); // Temporarily commented out unused variables
  const [state, setState] = useState<FundamentalsPageState>({
    loading: false,
    searchQuery: '',
    fundamentalsData: null
  });

  // Execute search
  const handleSearch = async () => {
    if (!state.searchQuery.trim()) {
      message.warning('Please enter a stock code or company name');
      return;
    }

    setState(prev => ({ ...prev, loading: true }));
    
    try {
      const data = await fundamentalsAPI.searchCompany(state.searchQuery.trim());
      setState(prev => ({
        ...prev,
        fundamentalsData: data,
        loading: false
      }));
      
      message.success(`Successfully loaded fundamental information for ${data.company.name}`);
    } catch (error) {
      console.error('Failed to query fundamental information:', error);
      message.error(`Query failed: ${getErrorMessage(error)}`);
      setState(prev => ({ ...prev, loading: false }));
    }
  };

  // Clear results
  const handleClear = () => {
    setState(prev => ({
      ...prev,
      searchQuery: '',
      fundamentalsData: null
    }));
  };

  // Render company basic information
  const renderCompanyInfo = () => {
    if (!state.fundamentalsData) return null;
    
    const { company } = state.fundamentalsData;
    
    return (
      <Card>
        <Row gutter={[24, 16]}>
          <Col span={24}>
            <div style={{ display: 'flex', alignItems: 'center', marginBottom: 16 }}>
              {company.logourl && (
                <img 
                  src={company.logourl} 
                  alt={company.name}
                  style={{ width: 48, height: 48, marginRight: 16, objectFit: 'contain' }}
                  onError={(e) => { (e.target as HTMLImageElement).style.display = 'none'; }}
                />
              )}
              <div>
                <Title level={3} style={{ margin: 0 }}>
                  {company.name}
                </Title>
                <Text type="secondary">
                  {company.code} | {company.exchange} | {company.countryname}
                </Text>
              </div>
            </div>
          </Col>
          
          <Col span={8}>
            <Statistic
              title="Exchange"
              value={company.exchange}
              prefix={<GlobalOutlined />}
            />
          </Col>
          <Col span={8}>
            <Statistic
              title="Sector"
              value={company.sector || 'N/A'}
              prefix={<BankOutlined />}
            />
          </Col>
          <Col span={8}>
            <Statistic
              title="Employees"
              value={company.fulltimeemployees || 'N/A'}
              prefix={<TeamOutlined />}
            />
          </Col>
          
          {company.description && (
            <Col span={24}>
              <Divider />
              <Text strong>Company Description:</Text>
              <Paragraph style={{ marginTop: 8, fontSize: '14px' }}>
                {company.description}
              </Paragraph>
            </Col>
          )}
        </Row>
      </Card>
    );
  };

  // Render key metrics
  const renderHighlights = () => {
    if (!state.fundamentalsData?.highlights) return <Empty description="No key metrics data available" />;
    
    const { highlights, company } = state.fundamentalsData;
    // const currency = company.currencysymbol || '$'; // Temporarily commented out unused variables
    
    // Calculate financial health score (using latest balance sheet data)
    const latestBalanceSheet = state.fundamentalsData.balance_sheet[0];
    const currentRatio = latestBalanceSheet?.totalcurrentassets && latestBalanceSheet?.totalcurrentliabilities 
      ? Number(latestBalanceSheet.totalcurrentassets) / Number(latestBalanceSheet.totalcurrentliabilities) : null;
    const healthScore = fundamentalsUtils.getFinancialHealthScore(
      currentRatio,
      null, // debt to equity ratio would need calculation
      highlights.returnonequityttm ?? null,
      highlights.profitmargin ?? null
    );
    
    return (
      <Card>
        <Row gutter={[16, 16]}>
          <Col span={6}>
            <Statistic
              title="Market Cap"
              value={fundamentalsUtils.formatCurrency(highlights.marketcapitalization, company.currencycode)}
              prefix={<DollarOutlined />}
              valueStyle={{ color: '#1890ff' }}
            />
          </Col>
          <Col span={6}>
            <Statistic
              title="P/E Ratio"
              value={fundamentalsUtils.formatNumber(highlights.peratio)}
              prefix={<LineChartOutlined />}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(highlights.peratio) }}
            />
          </Col>
          <Col span={6}>
            <Statistic
              title="Revenue (TTM)"
              value={fundamentalsUtils.formatCurrency(highlights.revenuettm, company.currencycode)}
              prefix={<BankOutlined />}
              valueStyle={{ color: '#52c41a' }}
            />
          </Col>
          <Col span={6}>
            <Statistic
              title="Financial Health"
              value={healthScore.level}
              prefix={<TrophyOutlined />}
              valueStyle={{ color: healthScore.color }}
              suffix={
                <Progress
                  percent={healthScore.score}
                  showInfo={false}
                  strokeColor={healthScore.color}
                  size="small"
                  style={{ width: 60, marginLeft: 8 }}
                />
              }
            />
          </Col>
          
          <Col span={6}>
            <Statistic
              title="EPS (TTM)"
              value={fundamentalsUtils.formatNumber(highlights.earningsshare)}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(highlights.earningsshare) }}
            />
          </Col>
          <Col span={6}>
            <Statistic
              title="Dividend Yield"
              value={fundamentalsUtils.formatPercentage(highlights.dividendyield)}
              valueStyle={{ color: '#faad14' }}
            />
          </Col>
          <Col span={6}>
            <Statistic
              title="ROE (TTM)"
              value={fundamentalsUtils.formatPercentage(highlights.returnonequityttm)}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(highlights.returnonequityttm) }}
            />
          </Col>
          <Col span={6}>
            <Statistic
              title="Profit Margin"
              value={fundamentalsUtils.formatPercentage(highlights.profitmargin)}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(highlights.profitmargin) }}
            />
          </Col>
        </Row>
      </Card>
    );
  };

  // Render financial statements tables
  const renderFinancialStatements = () => {
    if (!state.fundamentalsData) return null;

    const { balance_sheet, income_statement, cash_flow, company } = state.fundamentalsData;
    const currency = company.currencycode || 'USD';

    // Balance sheet column definitions
    const balanceSheetColumns: ColumnsType<any> = [
      {
        title: 'Date',
        dataIndex: 'date',
        key: 'date',
        render: (date: string) => fundamentalsUtils.formatDate(date)
      },
      {
        title: 'Period',
        dataIndex: 'period_type',
        key: 'period_type',
        render: (type: string) => <Tag color="blue">{type}</Tag>
      },
      {
        title: 'Total Assets',
        dataIndex: 'totalassets',
        key: 'totalassets',
        render: (value: number) => fundamentalsUtils.formatCurrency(value, currency)
      },
      {
        title: 'Total Liabilities',
        dataIndex: 'totalliab',
        key: 'totalliab',
        render: (value: number) => fundamentalsUtils.formatCurrency(value, currency)
      },
      {
        title: 'Shareholder Equity',
        dataIndex: 'totalstockholderequity',
        key: 'totalstockholderequity',
        render: (value: number) => fundamentalsUtils.formatCurrency(value, currency)
      },
      {
        title: 'Cash',
        dataIndex: 'cash',
        key: 'cash',
        render: (value: number) => fundamentalsUtils.formatCurrency(value, currency)
      }
    ];

    // Income statement column definitions
    const incomeStatementColumns: ColumnsType<any> = [
      {
        title: 'Date',
        dataIndex: 'date',
        key: 'date',
        render: (date: string) => fundamentalsUtils.formatDate(date)
      },
      {
        title: 'Period',
        dataIndex: 'period_type',
        key: 'period_type',
        render: (type: string) => <Tag color="green">{type}</Tag>
      },
      {
        title: 'Revenue',
        dataIndex: 'totalrevenue',
        key: 'totalrevenue',
        render: (value: number) => fundamentalsUtils.formatCurrency(value, currency)
      },
      {
        title: 'Gross Profit',
        dataIndex: 'grossprofit',
        key: 'grossprofit',
        render: (value: number) => fundamentalsUtils.formatCurrency(value, currency)
      },
      {
        title: 'Operating Income',
        dataIndex: 'operatingincome',
        key: 'operatingincome',
        render: (value: number) => fundamentalsUtils.formatCurrency(value, currency)
      },
      {
        title: 'Net Income',
        dataIndex: 'netincome',
        key: 'netincome',
        render: (value: number) => (
          <span style={{ color: fundamentalsUtils.getRatioColor(value) }}>
            {fundamentalsUtils.formatCurrency(value, currency)}
          </span>
        )
      }
    ];

    // Cash flow statement column definitions
    const cashFlowColumns: ColumnsType<any> = [
      {
        title: 'Date',
        dataIndex: 'date',
        key: 'date',
        render: (date: string) => fundamentalsUtils.formatDate(date)
      },
      {
        title: 'Period',
        dataIndex: 'period_type',
        key: 'period_type',
        render: (type: string) => <Tag color="orange">{type}</Tag>
      },
      {
        title: 'Operating Cash Flow',
        dataIndex: 'totalcashfromoperatingactivities',
        key: 'totalcashfromoperatingactivities',
        render: (value: number) => fundamentalsUtils.formatCurrency(value, currency)
      },
      {
        title: 'Investing Cash Flow',
        dataIndex: 'totalcashfrominvestingactivities',
        key: 'totalcashfrominvestingactivities',
        render: (value: number) => fundamentalsUtils.formatCurrency(value, currency)
      },
      {
        title: 'Financing Cash Flow',
        dataIndex: 'totalcashfromfinancingactivities',
        key: 'totalcashfromfinancingactivities',
        render: (value: number) => fundamentalsUtils.formatCurrency(value, currency)
      },
      {
        title: 'Free Cash Flow',
        dataIndex: 'cashfreeflowfromoperations',
        key: 'cashfreeflowfromoperations',
        render: (value: number) => (
          <span style={{ color: fundamentalsUtils.getRatioColor(value) }}>
            {fundamentalsUtils.formatCurrency(value, currency)}
          </span>
        )
      }
    ];

    return (
      <Tabs defaultActiveKey="balance_sheet">
        <TabPane tab="Balance Sheet" key="balance_sheet">
          <Table
            columns={balanceSheetColumns}
            dataSource={balance_sheet}
            rowKey="id"
            size="small"
            pagination={{ pageSize: 10 }}
            scroll={{ x: 800 }}
            locale={{ emptyText: <Empty description="No balance sheet data available" /> }}
          />
        </TabPane>
        <TabPane tab="Income Statement" key="income_statement">
          <Table
            columns={incomeStatementColumns}
            dataSource={income_statement}
            rowKey="id"
            size="small"
            pagination={{ pageSize: 10 }}
            scroll={{ x: 800 }}
            locale={{ emptyText: <Empty description="No income statement data available" /> }}
          />
        </TabPane>
        <TabPane tab="Cash Flow" key="cash_flow">
          <Table
            columns={cashFlowColumns}
            dataSource={cash_flow}
            rowKey="id"
            size="small"
            pagination={{ pageSize: 10 }}
            scroll={{ x: 800 }}
            locale={{ emptyText: <Empty description="No cash flow data available" /> }}
          />
        </TabPane>
      </Tabs>
    );
  };

  // Render valuation information
  const renderValuations = () => {
    if (!state.fundamentalsData?.valuations) return <Empty description="No valuation data available" />;
    
    const { valuations } = state.fundamentalsData;
    
    return (
      <Card>
        <Row gutter={[16, 16]}>
          <Col span={8}>
            <Statistic
              title="Trailing P/E"
              value={fundamentalsUtils.formatNumber(valuations.trailingpe)}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(valuations.trailingpe) }}
            />
          </Col>
          <Col span={8}>
            <Statistic
              title="Forward P/E"
              value={fundamentalsUtils.formatNumber(valuations.forwardpe)}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(valuations.forwardpe) }}
            />
          </Col>
          <Col span={8}>
            <Statistic
              title="Price/Sales (TTM)"
              value={fundamentalsUtils.formatNumber(valuations.pricesalesttm)}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(valuations.pricesalesttm) }}
            />
          </Col>
          <Col span={8}>
            <Statistic
              title="Price/Book (MRQ)"
              value={fundamentalsUtils.formatNumber(valuations.pricebookmrq)}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(valuations.pricebookmrq) }}
            />
          </Col>
          <Col span={8}>
            <Statistic
              title="Enterprise Value"
              value={fundamentalsUtils.formatCurrency(valuations.enterprisevalue)}
              valueStyle={{ color: '#1890ff' }}
            />
          </Col>
          <Col span={8}>
            <Statistic
              title="EV/Revenue"
              value={fundamentalsUtils.formatNumber(valuations.enterprisevaluerevenue)}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(valuations.enterprisevaluerevenue) }}
            />
          </Col>
        </Row>
      </Card>
    );
  };

  return (
    <div style={{ padding: '24px' }}>
      {/* Search area */}
      <Card style={{ marginBottom: 24 }}>
        <Row gutter={16} align="middle">
          <Col flex="auto">
            <Input
              placeholder="Enter stock code or company name to search (e.g., AAPL, Apple, Apple Inc.)"
              prefix={<SearchOutlined />}
              value={state.searchQuery}
              onChange={(e) => setState(prev => ({ ...prev, searchQuery: e.target.value }))}
              onPressEnter={handleSearch}
              size="large"
              style={{ fontSize: '16px' }}
            />
          </Col>
          <Col>
            <Space>
              <Button
                type="primary"
                icon={<SearchOutlined />}
                onClick={handleSearch}
                loading={state.loading}
                size="large"
              >
                Search
              </Button>
              <Button
                icon={<ReloadOutlined />}
                onClick={handleClear}
                size="large"
              >
                Clear
              </Button>
            </Space>
          </Col>
        </Row>
      </Card>

      {/* Search results */}
      {state.loading && (
        <Card>
          <div style={{ textAlign: 'center', padding: '40px' }}>
            <Spin size="large" />
            <div style={{ marginTop: 16 }}>
              <Text>Querying fundamental information...</Text>
            </div>
          </div>
        </Card>
      )}

      {state.fundamentalsData && !state.loading && (
        <div>
          {/* Company basic information */}
          <div style={{ marginBottom: 24 }}>
            <Title level={4}>Company Information</Title>
            {renderCompanyInfo()}
          </div>

          {/* Key metrics */}
          <div style={{ marginBottom: 24 }}>
            <Title level={4}>Key Metrics</Title>
            {renderHighlights()}
          </div>

          {/* Valuation information */}
          <div style={{ marginBottom: 24 }}>
            <Title level={4}>Valuation Metrics</Title>
            {renderValuations()}
          </div>

          {/* Financial statements */}
          <div style={{ marginBottom: 24 }}>
            <Title level={4}>Financial Statements</Title>
            <Card>
              {renderFinancialStatements()}
            </Card>
          </div>
        </div>
      )}

      {!state.fundamentalsData && !state.loading && (
        <Card>
          <Empty
            description={
              <span>
                Please enter a stock code or company name to search<br />
                Supported searches: Stock codes (e.g., AAPL), Company names (e.g., Apple Inc.)
              </span>
            }
          />
        </Card>
      )}
    </div>
  );
};

export default FundamentalsPage;
