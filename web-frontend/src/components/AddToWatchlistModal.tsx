import React, { useState, useEffect } from 'react';
import {
  Modal,
  Input,
  List,
  Typography,
  Tag,
  Button,
  Space,
  message,
  Empty,
  Spin,
  Form,
  Radio
} from 'antd';
import {
  SearchOutlined,
  PlusOutlined,
  StarFilled,
  StarOutlined
} from '@ant-design/icons';
import { dashboardAPI, type StockSearchResult } from '../services/dashboard';

const { Text } = Typography;

// 渲染星星图标的辅助函数
const renderStars = (count: number, filled: boolean = true) => {
  const stars = [];
  for (let i = 0; i < count; i++) {
    stars.push(
      filled ? (
        <StarFilled key={i} style={{ color: '#faad14', fontSize: '14px' }} />
      ) : (
        <StarOutlined key={i} style={{ color: '#d9d9d9', fontSize: '14px' }} />
      )
    );
  }
  return <Space size={2}>{stars}</Space>;
};

interface AddToWatchlistModalProps {
  open: boolean;
  onCancel: () => void;
  onSuccess: () => void;
}

const AddToWatchlistModal: React.FC<AddToWatchlistModalProps> = ({
  open,
  onCancel,
  onSuccess
}) => {
  const [form] = Form.useForm();
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [searchResults, setSearchResults] = useState<StockSearchResult[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [addingLoading, setAddingLoading] = useState<boolean>(false);
  const [selectedStock, setSelectedStock] = useState<StockSearchResult | null>(null);

  // 重置状态
  const resetModal = () => {
    setSearchQuery('');
    setSearchResults([]);
    setSelectedStock(null);
    form.resetFields();
  };

  // 搜索股票
  const searchStocks = async (query: string) => {
    if (!query.trim()) {
      setSearchResults([]);
      return;
    }

    setLoading(true);
    try {
      const results = await dashboardAPI.searchStocks(query);
      setSearchResults(results);
    } catch (error) {
      message.error('Failed to search stocks');
      console.error('Failed to search stocks:', error);
    } finally {
      setLoading(false);
    }
  };

  // 防抖搜索
  useEffect(() => {
    const timer = setTimeout(() => {
      searchStocks(searchQuery);
    }, 300);

    return () => clearTimeout(timer);
  }, [searchQuery]);

  // 选择股票
  const handleSelectStock = (stock: StockSearchResult) => {
    setSelectedStock(stock);
    form.setFieldsValue({
      symbol: stock.ticker,
      name: stock.company
    });
  };

  // 添加到watchlist
  const handleAddToWatchlist = async (values: any) => {
    if (!selectedStock) {
      message.error('Please select a stock first');
      return;
    }

    setAddingLoading(true);
    try {
      const params = {
        symbol: selectedStock.ticker,
        group_name: selectedStock.sector || 'Other', // 使用sector作为默认分组
        priority: values.priority || 1,
        notes: values.notes || ''
      };

      const result = await dashboardAPI.addToWatchlist(params);
      message.success(result.message);
      onSuccess();
      resetModal();
      onCancel();
    } catch (error: any) {
      if (error.response?.status === 409) {
        message.warning('This stock is already in your watchlist');
      } else {
        message.error('Failed to add to watchlist');
      }
      console.error('Failed to add to watchlist:', error);
    } finally {
      setAddingLoading(false);
    }
  };


  return (
    <Modal
      title="Add Stock to Watchlist"
      open={open}
      onCancel={() => {
        resetModal();
        onCancel();
      }}
      footer={null}
      width={700}
      destroyOnClose
    >
      <div style={{ marginBottom: '16px' }}>
        <Input
          placeholder="Search by ticker, company name, or sector..."
          prefix={<SearchOutlined />}
          value={searchQuery}
          onChange={e => setSearchQuery(e.target.value)}
          size="large"
        />
      </div>

      <div style={{ marginBottom: '16px', maxHeight: '300px', overflowY: 'auto' }}>
        <Spin spinning={loading}>
          {searchResults.length === 0 && searchQuery ? (
            <Empty description="No stocks found" />
          ) : (
            <List
              dataSource={searchResults}
              renderItem={(stock) => (
                <List.Item
                  style={{ 
                    cursor: 'pointer',
                    backgroundColor: selectedStock?.ticker === stock.ticker ? '#f0f8ff' : 'transparent'
                  }}
                  onClick={() => handleSelectStock(stock)}
                >
                  <List.Item.Meta
                    title={
                      <Space>
                        <Text strong style={{ color: '#1890ff' }}>{stock.ticker}</Text>
                        <Text>{stock.company}</Text>
                      </Space>
                    }
                    description={
                      <Space>
                        <Tag color="blue">{stock.sector}</Tag>
                        <Tag color="green">{stock.industry}</Tag>
                      </Space>
                    }
                  />
                </List.Item>
              )}
            />
          )}
        </Spin>
      </div>

      {selectedStock && (
        <div style={{ border: '1px solid #d9d9d9', borderRadius: '6px', padding: '16px', marginBottom: '16px' }}>
          <Text strong>Selected: </Text>
          <Tag color="blue" style={{ marginLeft: '8px' }}>
            {selectedStock.ticker} - {selectedStock.company}
          </Tag>
          <Text type="secondary" style={{ marginLeft: '8px' }}>
            Group: {selectedStock.sector || 'Other'}
          </Text>
          
          <Form
            form={form}
            layout="vertical"
            onFinish={handleAddToWatchlist}
            style={{ marginTop: '16px' }}
          >
            <Form.Item
              name="priority"
              label="Priority"
              initialValue={1}
              tooltip="Select priority level: 1 star = Low, 2 stars = Medium, 3 stars = High"
            >
              <Radio.Group>
                <Space direction="vertical" size="small">
                  <Radio value={1}>
                    <Space>
                      {renderStars(1)}
                      <Text>Low Priority</Text>
                    </Space>
                  </Radio>
                  <Radio value={2}>
                    <Space>
                      {renderStars(2)}
                      <Text>Medium Priority</Text>
                    </Space>
                  </Radio>
                  <Radio value={3}>
                    <Space>
                      {renderStars(3)}
                      <Text>High Priority</Text>
                    </Space>
                  </Radio>
                </Space>
              </Radio.Group>
            </Form.Item>

            <Form.Item
              name="notes"
              label="Notes"
            >
              <Input.TextArea 
                rows={2} 
                placeholder="Add some notes..."
              />
            </Form.Item>

            <Form.Item style={{ marginBottom: 0, textAlign: 'right' }}>
              <Space>
                <Button onClick={() => {
                  resetModal();
                  onCancel();
                }}>
                  Cancel
                </Button>
                <Button 
                  type="primary" 
                  htmlType="submit"
                  loading={addingLoading}
                  icon={<PlusOutlined />}
                >
                  Add to Watchlist
                </Button>
              </Space>
            </Form.Item>
          </Form>
        </div>
      )}
    </Modal>
  );
};

export default AddToWatchlistModal;
