import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Card, Typography, Row, Col, Button } from 'antd';
import {
  StockOutlined,
  CalendarOutlined,
  LineChartOutlined
} from '@ant-design/icons';
import { useAuthStore } from '../store/authStore';

const { Title, Text } = Typography;

const MarketsPage: React.FC = () => {
  const navigate = useNavigate();
  const { isAuthenticated } = useAuthStore();

  // 如果未认证，显示登录提示
  if (!isAuthenticated) {
    return (
      <div style={{ padding: '24px', textAlign: 'center' }}>
        <Card>
          <Title level={3}>Login Required</Title>
          <Text type="secondary">
            Please login to access market data features
          </Text>
        </Card>
      </div>
    );
  }

  return (
    <div style={{ padding: '24px' }}>
      <div style={{ marginBottom: '24px' }}>
        <Title level={2}>
          <StockOutlined /> Markets
        </Title>
        <Text type="secondary">
          Market Data Analysis Platform - Choose your data category
        </Text>
      </div>

      <Row gutter={[24, 24]}>
        <Col xs={24} md={12}>
          <Card
            hoverable
            style={{ height: '200px', cursor: 'pointer' }}
            onClick={() => navigate('/markets/economic-events')}
          >
            <div style={{ textAlign: 'center', padding: '20px' }}>
              <CalendarOutlined style={{ fontSize: '48px', color: '#1890ff', marginBottom: '16px' }} />
              <Title level={3}>Economic Events</Title>
              <Text type="secondary">
                Real-time economic events and market indicators from major economies
              </Text>
              <div style={{ marginTop: '16px' }}>
                <Button type="primary" size="large">
                  View Economic Events
                </Button>
              </div>
            </div>
          </Card>
        </Col>

        <Col xs={24} md={12}>
          <Card
            hoverable
            style={{ height: '200px', cursor: 'pointer' }}
            onClick={() => navigate('/markets/macro-indicators')}
          >
            <div style={{ textAlign: 'center', padding: '20px' }}>
              <LineChartOutlined style={{ fontSize: '48px', color: '#52c41a', marginBottom: '16px' }} />
              <Title level={3}>Macro Indicators</Title>
              <Text type="secondary">
                Global macroeconomic indicators and economic data analysis
              </Text>
              <div style={{ marginTop: '16px' }}>
                <Button type="primary" size="large">
                  View Macro Indicators
                </Button>
              </div>
            </div>
          </Card>
        </Col>
      </Row>

      <Row style={{ marginTop: '24px' }}>
        <Col span={24}>
          <Card>
            <Title level={4}>About Market Data</Title>
            <Text type="secondary">
              Our market data platform provides comprehensive access to economic events and macro indicators 
              from major global economies. Choose from Economic Events for real-time market-moving data, 
              or Macro Indicators for historical economic analysis and trends.
            </Text>
          </Card>
        </Col>
      </Row>
    </div>
  );
};

export default MarketsPage;
