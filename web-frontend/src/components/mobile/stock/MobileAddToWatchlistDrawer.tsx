import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  Drawer,
  Input,
  List,
  Typography,
  Space,
  Tag,
  message,
  Empty,
  Spin,
  Button,
  Form,
  Radio
} from 'antd';
import { SearchOutlined, PlusOutlined, StarFilled, StarOutlined } from '@ant-design/icons';
import { dashboardAPI, type StockSearchResult } from '../../../services/dashboard';
import { useDashboardSettings } from '../../../contexts/DashboardSettingsContext';
import './MobileAddToWatchlistDrawer.css';

const { Text } = Typography;

interface MobileAddToWatchlistDrawerProps {
  open: boolean;
  onClose: () => void;
  onSuccess: () => void;
}

const DEBOUNCE_DELAY = 300;

const renderStars = (count: number, isDark: boolean) => {
  const stars = [];
  const emptyColor = isDark ? '#5c6573' : '#d9d9d9';
  for (let i = 0; i < 3; i++) {
    const filled = i < count;
    stars.push(
      filled ? (
        <StarFilled key={i} style={{ color: '#faad14', fontSize: '14px' }} />
      ) : (
        <StarOutlined key={`empty-${i}`} style={{ color: emptyColor, fontSize: '14px' }} />
      )
    );
  }
  return <Space size={2}>{stars}</Space>;
};

const MobileAddToWatchlistDrawer: React.FC<MobileAddToWatchlistDrawerProps> = ({
  open,
  onClose,
  onSuccess
}) => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const [form] = Form.useForm();
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [searchResults, setSearchResults] = useState<StockSearchResult[]>([]);
  const [loading, setLoading] = useState<boolean>(false);
  const [addingLoading, setAddingLoading] = useState<boolean>(false);
  const [selectedStock, setSelectedStock] = useState<StockSearchResult | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const resetState = useCallback(() => {
    setSearchQuery('');
    setSearchResults([]);
    setSelectedStock(null);
    setAddingLoading(false);
    setLoading(false);
    form.resetFields();
  }, [form]);

  useEffect(() => {
    if (!open) {
      resetState();
    }
  }, [open, resetState]);

  const handleClose = () => {
    resetState();
    onClose();
  };

  const handleSelectStock = (stock: StockSearchResult) => {
    setSelectedStock(stock);
    form.setFieldsValue({
      priority: 1,
      notes: ''
    });
  };

  const searchStocks = async (query: string) => {
    if (!query.trim()) {
      setSearchResults([]);
      return;
    }

    setLoading(true);
    try {
      const results = await dashboardAPI.searchStocks(query, 50);
      setSearchResults(results);
    } catch (error) {
      message.error('Failed to search stocks');
      console.error('Failed to search stocks:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (!open) {
      return;
    }

    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }

    const timer = setTimeout(() => {
      void searchStocks(searchQuery);
    }, DEBOUNCE_DELAY);

    debounceRef.current = timer;

    return () => {
      clearTimeout(timer);
    };
  }, [searchQuery, open]);

  const groupedResults = useMemo(() => {
    if (!searchResults.length) return [];
    return searchResults.reduce<Record<string, StockSearchResult[]>>((acc, stock) => {
      const group = stock.sector || 'Other';
      if (!acc[group]) {
        acc[group] = [];
      }
      acc[group].push(stock);
      return acc;
    }, {});
  }, [searchResults]);

  const handleAddToWatchlist = async (values: { priority: number; notes?: string }) => {
    if (!selectedStock) {
      message.error('Please select a stock first');
      return;
    }

    setAddingLoading(true);
    try {
      const params = {
        symbol: selectedStock.ticker,
        group_name: selectedStock.sector || 'Other',
        priority: values.priority ?? 1,
        notes: values.notes || ''
      };

      const result = await dashboardAPI.addToWatchlist(params);
      message.success(result.message || 'Added to watchlist');
      onSuccess();
      resetState();
      onClose();
    } catch (error: any) {
      if (error?.response?.status === 409) {
        message.warning('This stock is already in your watchlist');
      } else {
        message.error('Failed to add to watchlist');
      }
      console.error('Failed to add to watchlist:', error);
    } finally {
      setAddingLoading(false);
    }
  };

  const drawerSurface = isDark ? '#1a2234' : undefined;
  const drawerBorder = isDark ? '#364152' : undefined;

  return (
    <Drawer
      className={`mobile-add-watchlist-drawer${isDark ? ' mobile-add-watchlist-drawer--dark' : ''}`}
      title="Add Stock"
      placement="bottom"
      closable
      height="90%"
      onClose={handleClose}
      open={open}
      destroyOnClose
      getContainer={() => document.querySelector('.App') ?? document.body}
      styles={{
        body: { paddingBottom: 80, background: drawerSurface },
        header: {
          background: drawerSurface,
          borderBottom: drawerBorder ? `1px solid ${drawerBorder}` : undefined,
          color: isDark ? '#e0e6f1' : undefined,
        },
        footer: { background: drawerSurface },
        mask: isDark ? { background: 'rgba(0, 0, 0, 0.65)' } : undefined,
        wrapper: isDark ? { background: 'transparent' } : undefined,
      }}
      extra={
        selectedStock ? (
          <Tag color="blue">
            {selectedStock.ticker}
          </Tag>
        ) : null
      }
    >
      <div style={{ marginBottom: 16 }}>
        <Input
          placeholder="Search ticker or company"
          prefix={<SearchOutlined />}
          value={searchQuery}
          onChange={e => setSearchQuery(e.target.value)}
          size="large"
          allowClear
        />
      </div>

      <div style={{ maxHeight: 280, overflowY: 'auto', marginBottom: 16 }}>
        <Spin spinning={loading}>
          {!searchQuery ? (
            <Empty description="Enter keywords to search" image={Empty.PRESENTED_IMAGE_SIMPLE} />
          ) : searchResults.length === 0 ? (
            <Empty description="No stocks found" image={Empty.PRESENTED_IMAGE_SIMPLE} />
          ) : (
            Object.entries(groupedResults).map(([groupName, stocks]) => (
              <div key={groupName} style={{ marginBottom: 12 }}>
                <Text strong style={{ display: 'block', marginBottom: 8 }}>
                  {groupName}
                </Text>
                <List
                  dataSource={stocks}
                  renderItem={(stock) => (
                    <List.Item
                      className={`mobile-add-watchlist-result-item${selectedStock?.ticker === stock.ticker ? ' is-selected' : ''}`}
                      onClick={() => handleSelectStock(stock)}
                    >
                      <List.Item.Meta
                        title={
                          <div style={{ display: 'flex', flexDirection: 'column' }}>
                            <Text strong>{stock.ticker}</Text>
                            <Text type="secondary" style={{ whiteSpace: 'normal', marginTop: 2 }}>
                              {stock.company}
                            </Text>
                          </div>
                        }
                        description={
                          <Space size={4} wrap>
                            {stock.exchange && <Tag color="default">{stock.exchange}</Tag>}
                            {stock.sector && <Tag color="blue">{stock.sector}</Tag>}
                            {stock.industry && (
                              <Tag color="green">
                                {stock.industry}
                              </Tag>
                            )}
                          </Space>
                        }
                      />
                    </List.Item>
                  )}
                />
              </div>
            ))
          )}
        </Spin>
      </div>

      {selectedStock && (
        <Form
          form={form}
          layout="vertical"
          onFinish={handleAddToWatchlist}
          initialValues={{ priority: 1, notes: '' }}
        >
          <Form.Item label="Selected Stock">
            <Space size={8} wrap>
              <Tag color="blue">{selectedStock.ticker}</Tag>
              <Text type="secondary">{selectedStock.company}</Text>
              {selectedStock.sector && (
                <Tag color="geekblue">
                  {selectedStock.sector}
                </Tag>
              )}
            </Space>
          </Form.Item>

          <Form.Item
            name="priority"
            label="Priority"
          >
            <Radio.Group>
              <Space direction="vertical">
                <Radio value={1}>
                  <Space>
                    {renderStars(1, isDark)}
                    <Text>Low Priority</Text>
                  </Space>
                </Radio>
                <Radio value={2}>
                  <Space>
                    {renderStars(2, isDark)}
                    <Text>Medium Priority</Text>
                  </Space>
                </Radio>
                <Radio value={3}>
                  <Space>
                    {renderStars(3, isDark)}
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
            <Input.TextArea rows={3} placeholder="Add some notes..." maxLength={200} />
          </Form.Item>

          <Form.Item style={{ marginBottom: 0 }}>
            <Button
              type="primary"
              htmlType="submit"
              block
              icon={<PlusOutlined />}
              loading={addingLoading}
            >
              Add to Watchlist
            </Button>
          </Form.Item>
        </Form>
      )}

      {!selectedStock && (
        <Button
          block
          onClick={handleClose}
          style={{ marginTop: 16 }}
        >
          Cancel
        </Button>
      )}
    </Drawer>
  );
};

export default MobileAddToWatchlistDrawer;

