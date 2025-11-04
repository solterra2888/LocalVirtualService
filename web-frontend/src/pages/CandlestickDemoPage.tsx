import React, { useState } from 'react';
import { Card, Input, Space, Typography, Alert, Switch } from 'antd';
import CandlestickChart from '../components/CandlestickChart';

const { Title, Text } = Typography;
const { Search } = Input;

const CandlestickDemoPage: React.FC = () => {
  const [symbol, setSymbol] = useState<string>('AAPL.US');
  const [useMockData, setUseMockData] = useState<boolean>(false);

  const handleSymbolChange = (value: string) => {
    if (value.trim()) {
      setSymbol(value.trim());
    }
  };

  return (
    <div style={{ padding: '24px', maxWidth: '1400px', margin: '0 auto' }}>
      <Card>
        <Title level={2}>📊 Candlestick Chart Demo</Title>
        <Alert
          message="Candlestick Chart Demo"
          description={
            useMockData 
              ? "Using mock data (offline mode). Chart can be displayed without network connection."
              : "This page demonstrates the candlestick chart using lightweight-charts. Enter a stock symbol to view its price chart. Requires network connection to fetch data from API."
          }
          type={useMockData ? "warning" : "info"}
          showIcon
          style={{ marginBottom: '24px' }}
        />
        
        <Space direction="vertical" style={{ width: '100%' }} size="large">
          <Card title="Chart Controls">
            <Space direction="vertical" style={{ width: '100%' }}>
              <Space>
                <span>Stock Symbol:</span>
                <Search
                  placeholder="Enter symbol (e.g., AAPL.US, MSFT.US)"
                  defaultValue={symbol}
                  onSearch={handleSymbolChange}
                  style={{ width: 300 }}
                  enterButton="Load Chart"
                  disabled={useMockData}
                />
              </Space>
              <Space>
                <Text>Use Mock Data (Offline Mode):</Text>
                <Switch 
                  checked={useMockData} 
                  onChange={setUseMockData}
                  checkedChildren="ON"
                  unCheckedChildren="OFF"
                />
                {useMockData && (
                  <Text type="secondary" style={{ fontSize: '12px' }}>
                    Chart will work without network connection
                  </Text>
                )}
              </Space>
            </Space>
          </Card>

          <CandlestickChart
            symbol={symbol}
            companyName={useMockData ? `Mock Data - ${symbol}` : `Stock ${symbol}`}
            useMockData={useMockData}
          />
        </Space>
      </Card>
    </div>
  );
};

export default CandlestickDemoPage;

