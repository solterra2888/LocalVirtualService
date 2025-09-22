import React, { useState, useEffect } from 'react';
import {
  Modal,
  Card,
  Row,
  Col,
  Typography,
  Spin,
  message,
  Statistic,
  Tag,
  Space,
  Divider,
  Progress,
  Empty
} from 'antd';
import {
  InfoCircleOutlined,
  BarChartOutlined,
  StarOutlined,
  CalculatorOutlined,
  DashboardOutlined,
  DollarOutlined,
  LineChartOutlined,
  BankOutlined,
  BuildOutlined
} from '@ant-design/icons';

import { fundamentalsAPI, fundamentalsUtils, CompanyFundamentalsResponse } from '../services/fundamentals';
import { getErrorMessage } from '../utils/errorHandler';

const { Title, Text, Paragraph } = Typography;

interface FundamentalsOverviewModalProps {
  visible: boolean;
  onCancel: () => void;
  ticker: string;
  companyName: string;
}

const FundamentalsOverviewModal: React.FC<FundamentalsOverviewModalProps> = ({
  visible,
  onCancel,
  ticker,
  companyName
}) => {
  const [loading, setLoading] = useState(false);
  const [fundamentalsData, setFundamentalsData] = useState<CompanyFundamentalsResponse | null>(null);

  // 获取基本面数据
  const fetchFundamentalsData = async () => {
    if (!ticker) return;
    
    setLoading(true);
    try {
      const data = await fundamentalsAPI.searchCompany(ticker);
      setFundamentalsData(data);
    } catch (error) {
      console.error('Failed to fetch fundamentals data:', error);
      const errorMessage = getErrorMessage(error);
      message.error(`获取基本面数据失败: ${errorMessage}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (visible && ticker) {
      fetchFundamentalsData();
    }
  }, [visible, ticker]);

  const renderBasicInformation = () => {
    if (!fundamentalsData?.company) return null;
    
    const company = fundamentalsData.company;
    
    return (
      <Card 
        title={
          <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <InfoCircleOutlined style={{ color: '#1890ff' }} />
            Basic Information
          </span>
        }
        style={{ marginBottom: 16 }}
        size="small"
      >
        <Row gutter={[16, 8]}>
          <Col xs={12} sm={8}>
            <div>
              <Text strong style={{ fontSize: '12px' }}>Stock Code:</Text>
              <br />
              <Text style={{ fontSize: '12px' }}>{company.code}</Text>
            </div>
          </Col>
          <Col xs={12} sm={8}>
            <div>
              <Text strong style={{ fontSize: '12px' }}>Exchange:</Text>
              <br />
              <Text style={{ fontSize: '12px' }}>{company.exchange}</Text>
            </div>
          </Col>
          <Col xs={12} sm={8}>
            <div>
              <Text strong style={{ fontSize: '12px' }}>Currency:</Text>
              <br />
              <Text style={{ fontSize: '12px' }}>{company.currencycode || 'N/A'}</Text>
            </div>
          </Col>
          <Col xs={12} sm={8}>
            <div>
              <Text strong style={{ fontSize: '12px' }}>Country:</Text>
              <br />
              <Text style={{ fontSize: '12px' }}>{company.countryname || 'N/A'}</Text>
            </div>
          </Col>
          <Col xs={12} sm={8}>
            <div>
              <Text strong style={{ fontSize: '12px' }}>Sector:</Text>
              <br />
              <Text style={{ fontSize: '12px' }}>{company.sector || 'N/A'}</Text>
            </div>
          </Col>
          <Col xs={12} sm={8}>
            <div>
              <Text strong style={{ fontSize: '12px' }}>Industry:</Text>
              <br />
              <Text style={{ fontSize: '12px' }}>{company.industry || 'N/A'}</Text>
            </div>
          </Col>
          <Col xs={12} sm={8}>
            <div>
              <Text strong style={{ fontSize: '12px' }}>Employees:</Text>
              <br />
              <Text style={{ fontSize: '12px' }}>
                {company.fulltimeemployees ? company.fulltimeemployees.toLocaleString() : 'N/A'}
              </Text>
            </div>
          </Col>
          <Col xs={12} sm={8}>
            <div>
              <Text strong style={{ fontSize: '12px' }}>Type:</Text>
              <br />
              <Text style={{ fontSize: '12px' }}>{company.type || 'N/A'}</Text>
            </div>
          </Col>
        </Row>
        {company.description && (
          <>
            <Divider style={{ margin: '12px 0' }} />
            <div>
              <Text strong style={{ fontSize: '12px' }}>Description:</Text>
              <Paragraph 
                style={{ 
                  marginTop: 4, 
                  fontSize: '12px', 
                  lineHeight: '1.4',
                  marginBottom: 0
                }}
                ellipsis={{ rows: 3, expandable: true, symbol: '展开' }}
              >
                {company.description}
              </Paragraph>
            </div>
          </>
        )}
      </Card>
    );
  };

  const renderKeyMetrics = () => {
    if (!fundamentalsData?.highlights) return null;
    
    const highlights = fundamentalsData.highlights;
    const company = fundamentalsData.company;
    
    return (
      <Card 
        title={
          <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <BarChartOutlined style={{ color: '#52c41a' }} />
            Key Metrics
          </span>
        }
        style={{ marginBottom: 16 }}
        size="small"
      >
        <Row gutter={[8, 8]}>
          <Col xs={12} sm={8}>
            <Statistic
              title="Market Cap"
              value={fundamentalsUtils.formatCurrency(highlights.marketcapitalization, company.currencycode)}
              prefix={<DollarOutlined />}
              valueStyle={{ color: '#1890ff', fontSize: '14px' }}
            />
          </Col>
          <Col xs={12} sm={8}>
            <Statistic
              title="P/E Ratio"
              value={fundamentalsUtils.formatNumber(highlights.peratio)}
              prefix={<LineChartOutlined />}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(highlights.peratio), fontSize: '14px' }}
            />
          </Col>
          <Col xs={12} sm={8}>
            <Statistic
              title="Revenue (TTM)"
              value={fundamentalsUtils.formatCurrency(highlights.revenuettm, company.currencycode)}
              prefix={<BankOutlined />}
              valueStyle={{ color: '#52c41a', fontSize: '14px' }}
            />
          </Col>
          <Col xs={12} sm={8}>
            <Statistic
              title="EPS (TTM)"
              value={fundamentalsUtils.formatNumber(highlights.earningsshare)}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(highlights.earningsshare), fontSize: '14px' }}
            />
          </Col>
          <Col xs={12} sm={8}>
            <Statistic
              title="Dividend Yield"
              value={fundamentalsUtils.formatPercentage(highlights.dividendyield)}
              valueStyle={{ color: '#52c41a', fontSize: '14px' }}
            />
          </Col>
          <Col xs={12} sm={8}>
            <Statistic
              title="ROE"
              value={fundamentalsUtils.formatPercentage(highlights.returnonequityttm)}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(highlights.returnonequityttm), fontSize: '14px' }}
            />
          </Col>
        </Row>
      </Card>
    );
  };

  const renderAnalystRatings = () => {
    if (!fundamentalsData?.analyst_ratings || fundamentalsData.analyst_ratings.length === 0) return null;
    
    const ratings = fundamentalsData.analyst_ratings.slice(0, 3); // 只显示前3个
    
    return (
      <Card 
        title={
          <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <StarOutlined style={{ color: '#faad14' }} />
            Analyst Ratings
          </span>
        }
        style={{ marginBottom: 16 }}
        size="small"
      >
        {ratings.map((rating, index) => (
          <div key={index} style={{ 
            marginBottom: index < ratings.length - 1 ? '12px' : 0,
            padding: '8px', 
            border: '1px solid #f0f0f0', 
            borderRadius: '4px',
            backgroundColor: '#fafafa'
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
              <Text strong style={{ fontSize: '12px' }}>
                {rating.analyst_name || 'Unknown Analyst'}
              </Text>
              <Tag color={fundamentalsUtils.getRatingColor(rating.rating)} style={{ fontSize: '10px', padding: '2px 6px' }}>
                {rating.rating}
              </Tag>
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '11px', color: '#666' }}>
              <span>Target: {rating.target_price ? fundamentalsUtils.formatCurrency(rating.target_price, fundamentalsData.company.currencycode) : 'N/A'}</span>
              <span>{fundamentalsUtils.formatDate(rating.rating_date)}</span>
            </div>
          </div>
        ))}
      </Card>
    );
  };

  const renderValuationMetrics = () => {
    if (!fundamentalsData?.valuations) return null;
    
    const valuations = fundamentalsData.valuations;
    const company = fundamentalsData.company;
    
    return (
      <Card 
        title={
          <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <CalculatorOutlined style={{ color: '#faad14' }} />
            Valuation Metrics
          </span>
        }
        style={{ marginBottom: 16 }}
        size="small"
      >
        <Row gutter={[8, 8]}>
          <Col xs={12} sm={8}>
            <Statistic
              title="Trailing P/E"
              value={fundamentalsUtils.formatNumber(valuations.trailingpe)}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(valuations.trailingpe), fontSize: '14px' }}
            />
          </Col>
          <Col xs={12} sm={8}>
            <Statistic
              title="Forward P/E"
              value={fundamentalsUtils.formatNumber(valuations.forwardpe)}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(valuations.forwardpe), fontSize: '14px' }}
            />
          </Col>
          <Col xs={12} sm={8}>
            <Statistic
              title="Price/Sales"
              value={fundamentalsUtils.formatNumber(valuations.pricesalesttm)}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(valuations.pricesalesttm), fontSize: '14px' }}
            />
          </Col>
          <Col xs={12} sm={8}>
            <Statistic
              title="Price/Book"
              value={fundamentalsUtils.formatNumber(valuations.pricebookmrq)}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(valuations.pricebookmrq), fontSize: '14px' }}
            />
          </Col>
          <Col xs={12} sm={8}>
            <Statistic
              title="Enterprise Value"
              value={fundamentalsUtils.formatCurrency(valuations.enterprisevalue, company.currencycode)}
              valueStyle={{ color: '#1890ff', fontSize: '14px' }}
            />
          </Col>
          <Col xs={12} sm={8}>
            <Statistic
              title="EV/Revenue"
              value={fundamentalsUtils.formatNumber(valuations.enterprisevaluerevenue)}
              valueStyle={{ color: fundamentalsUtils.getRatioColor(valuations.enterprisevaluerevenue), fontSize: '14px' }}
            />
          </Col>
        </Row>
      </Card>
    );
  };

  const renderTechnicalIndicators = () => {
    if (!fundamentalsData?.technicals) return null;
    
    const technicals = fundamentalsData.technicals;
    const company = fundamentalsData.company;
    
    return (
      <Card 
        title={
          <span style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <DashboardOutlined style={{ color: '#13c2c2' }} />
            Technical Indicators
          </span>
        }
        style={{ marginBottom: 16 }}
        size="small"
      >
        <Row gutter={[8, 8]}>
          <Col xs={12} sm={8}>
            <Statistic
              title="Beta"
              value={fundamentalsUtils.formatNumber(technicals.beta)}
              valueStyle={{ 
                color: technicals.beta && technicals.beta > 1 ? '#ff4d4f' : '#52c41a',
                fontSize: '14px' 
              }}
            />
          </Col>
          <Col xs={12} sm={8}>
            <Statistic
              title="52-Week High"
              value={fundamentalsUtils.formatCurrency(technicals['52WeekHigh'], company.currencycode)}
              valueStyle={{ color: '#52c41a', fontSize: '14px' }}
            />
          </Col>
          <Col xs={12} sm={8}>
            <Statistic
              title="52-Week Low"
              value={fundamentalsUtils.formatCurrency(technicals['52WeekLow'], company.currencycode)}
              valueStyle={{ color: '#ff4d4f', fontSize: '14px' }}
            />
          </Col>
          <Col xs={12} sm={8}>
            <Statistic
              title="50-Day MA"
              value={fundamentalsUtils.formatCurrency(technicals['50DayMA'], company.currencycode)}
              valueStyle={{ color: '#1890ff', fontSize: '14px' }}
            />
          </Col>
          <Col xs={12} sm={8}>
            <Statistic
              title="200-Day MA"
              value={fundamentalsUtils.formatCurrency(technicals['200DayMA'], company.currencycode)}
              valueStyle={{ color: '#722ed1', fontSize: '14px' }}
            />
          </Col>
          <Col xs={12} sm={8}>
            <Statistic
              title="Short Ratio"
              value={fundamentalsUtils.formatNumber(technicals.shortratio)}
              valueStyle={{ 
                color: technicals.shortratio && technicals.shortratio > 5 ? '#ff4d4f' : 
                       technicals.shortratio && technicals.shortratio < 2 ? '#52c41a' : '#1890ff',
                fontSize: '14px' 
              }}
            />
          </Col>
        </Row>
      </Card>
    );
  };

  return (
    <Modal
      title={
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          <BuildOutlined style={{ color: '#1890ff' }} />
          <span>{companyName} - Fundamentals Overview</span>
        </div>
      }
      open={visible}
      onCancel={onCancel}
      footer={null}
      width={800}
      style={{ top: 20 }}
      destroyOnClose
    >
      <Spin spinning={loading}>
        {fundamentalsData ? (
          <div style={{ maxHeight: '70vh', overflowY: 'auto' }}>
            {renderBasicInformation()}
            {renderKeyMetrics()}
            {renderAnalystRatings()}
            {renderValuationMetrics()}
            {renderTechnicalIndicators()}
          </div>
        ) : !loading ? (
          <Empty 
            description="No fundamentals data available" 
            style={{ margin: '40px 0' }}
          />
        ) : null}
      </Spin>
    </Modal>
  );
};

export default FundamentalsOverviewModal;
