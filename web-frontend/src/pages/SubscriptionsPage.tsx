/**
 * @deprecated This page has been removed from the application.
 * All subscription management features have been integrated into Feed Reader (/feeds).
 * This file is kept for reference only and will be deleted in v2.1.
 * 
 * Migration Guide: docs/FEED_READER_ENHANCEMENT_GUIDE.md
 * Removal Details: docs/SUBSCRIPTION_MANAGEMENT_REMOVAL.md
 * 
 * Date Deprecated: 2024-02-17
 */

import React, { useState, useEffect } from 'react';
import {
  Card,
  Table,
  Button,
  Space,
  Tag,
  Modal,
  Form,
  Input,
  Select,
  Switch,
  message,
  Tooltip,
  Statistic,
  Row,
  Col,
  Popconfirm,
  Badge,
  Typography,
  Drawer,
  Timeline,
  Empty,
  Alert,
} from 'antd';
import {
  PlusOutlined,
  EditOutlined,
  DeleteOutlined,
  ReloadOutlined,
  TwitterOutlined,
  YoutubeOutlined,
  GlobalOutlined,
  ThunderboltOutlined,
  CheckCircleOutlined,
  CloseCircleOutlined,
  ClockCircleOutlined,
  HistoryOutlined,
  UserOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { getModalStyles } from '../utils/modalStyles';
import {
  subscriptionsAPI,
  Subscription,
  CreateSubscriptionRequest,
  UpdateSubscriptionRequest,
  SubscriptionStats,
  FetchHistory,
} from '../services/subscriptions';

dayjs.extend(relativeTime);

const { Title, Text } = Typography;
const { Option } = Select;

// Theme colors
const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
  },
  dark: {
    pageBg: '#1E202B',
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    text: '#FFFFFF',
    textSecondary: '#A0A3AD',
  },
} as const;

const CARD_STYLE_BEHANCE = {
  borderRadius: 12,
  boxShadow: '0 4px 24px rgba(0, 0, 0, 0.12)',
  border: '1px solid #f0f0f0',
};

const CARD_STYLE_BEHANCE_DARK = {
  borderRadius: 12,
  boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)',
  border: '1px solid rgba(255, 255, 255, 0.06)',
};

// Channel type configuration
const CHANNEL_TYPES = [
  { value: 'twitter', label: 'Twitter', icon: <TwitterOutlined />, color: '#1DA1F2' },
  { value: 'youtube', label: 'YouTube', icon: <YoutubeOutlined />, color: '#FF0000' },
  { value: 'zhishixingqiu', label: '知识星球', icon: <GlobalOutlined />, color: '#52c41a' },
  { value: 'rss', label: 'RSS', icon: <GlobalOutlined />, color: '#orange' },
];

const SubscriptionsPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const colors = THEME_COLORS[themeMode];
  const cardStyle = isDark ? CARD_STYLE_BEHANCE_DARK : CARD_STYLE_BEHANCE;

  const [subscriptions, setSubscriptions] = useState<Subscription[]>([]);
  const [stats, setStats] = useState<SubscriptionStats | null>(null);
  const [loading, setLoading] = useState(false);
  const [modalVisible, setModalVisible] = useState(false);
  const [editingSubscription, setEditingSubscription] = useState<Subscription | null>(null);
  const [historyDrawerVisible, setHistoryDrawerVisible] = useState(false);
  const [selectedSubscription, setSelectedSubscription] = useState<Subscription | null>(null);
  const [fetchHistory, setFetchHistory] = useState<FetchHistory[]>([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [filterChannelType, setFilterChannelType] = useState<string | undefined>(undefined);
  const [filterEnabled, setFilterEnabled] = useState<boolean | undefined>(undefined);

  const [form] = Form.useForm();

  // Load data
  useEffect(() => {
    loadSubscriptions();
    loadStats();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [filterChannelType, filterEnabled]);

  const loadSubscriptions = async () => {
    setLoading(true);
    try {
      const response = await subscriptionsAPI.getUserSubscriptions(
        filterChannelType,
        filterEnabled
      );
      if (response.success && response.data) {
        setSubscriptions(response.data);
      } else {
        message.error(response.error || 'Failed to load subscriptions');
      }
    } catch (error) {
      console.error('Failed to load subscriptions:', error);
      message.error('Failed to load subscriptions');
    } finally {
      setLoading(false);
    }
  };

  const loadStats = async () => {
    try {
      const response = await subscriptionsAPI.getSubscriptionStats();
      if (response.success && response.data) {
        setStats(response.data);
      }
    } catch (error) {
      console.error('Failed to load stats:', error);
    }
  };

  const loadHistory = async (subscriptionId: number) => {
    setHistoryLoading(true);
    try {
      const response = await subscriptionsAPI.getSubscriptionHistory(subscriptionId);
      if (response.success && response.data) {
        setFetchHistory(response.data);
      } else {
        message.error(response.error || 'Failed to load history');
      }
    } catch (error) {
      console.error('Failed to load history:', error);
      message.error('Failed to load history');
    } finally {
      setHistoryLoading(false);
    }
  };

  const handleCreate = () => {
    setEditingSubscription(null);
    form.resetFields();
    form.setFieldsValue({
      enabled: true,
    });
    setModalVisible(true);
  };

  const handleEdit = (record: Subscription) => {
    setEditingSubscription(record);
    form.setFieldsValue({
      channel_type: record.channel_type,
      channel_identifier: record.channel_identifier,
      channel_name: record.channel_name,
      channel_description: record.channel_description,
      enabled: record.enabled,
    });
    setModalVisible(true);
  };

  const handleDelete = async (id: number) => {
    try {
      const response = await subscriptionsAPI.deleteSubscription(id);
      if (response.success) {
        message.success('Subscription deleted successfully');
        loadSubscriptions();
        loadStats();
      } else {
        message.error(response.error || 'Failed to delete subscription');
      }
    } catch (error) {
      console.error('Failed to delete subscription:', error);
      message.error('Failed to delete subscription');
    }
  };

  const handleToggleEnabled = async (record: Subscription) => {
    try {
      const response = await subscriptionsAPI.updateSubscription(record.id, {
        enabled: !record.enabled,
      });
      if (response.success) {
        message.success(`Subscription ${record.enabled ? 'disabled' : 'enabled'}`);
        loadSubscriptions();
        loadStats();
      } else {
        message.error(response.error || 'Failed to update subscription');
      }
    } catch (error) {
      console.error('Failed to update subscription:', error);
      message.error('Failed to update subscription');
    }
  };

  const handleTestFetch = async (record: Subscription) => {
    // Test Fetch 功能已弃用
    Modal.info({
      title: 'Test Fetch is Deprecated',
      content: (
        <div>
          <p><strong>Test Fetch functionality has been removed.</strong></p>
          <p style={{ marginTop: 12 }}>The system now automatically handles content fetching:</p>
          <ul style={{ marginTop: 8, paddingLeft: 20 }}>
            <li>On subscription creation: Reuses existing content or fetches from API</li>
            <li>Periodic updates: Scheduler runs automatically every 30 minutes</li>
            <li>Force refresh: Use Scheduler's "Run Now" feature if needed</li>
          </ul>
          <p style={{ marginTop: 12, color: '#666' }}>
            Note: This page is also deprecated. Please use <a href="/feed-reader">Feed Reader</a> instead.
          </p>
        </div>
      ),
    });
  };

  const handleViewHistory = (record: Subscription) => {
    setSelectedSubscription(record);
    setHistoryDrawerVisible(true);
    loadHistory(record.id);
  };

  const handleModalOk = async () => {
    try {
      const values = await form.validateFields();

      if (editingSubscription) {
        // Update
        const updateRequest: UpdateSubscriptionRequest = {
          channel_name: values.channel_name,
          channel_description: values.channel_description,
          enabled: values.enabled,
        };
        const response = await subscriptionsAPI.updateSubscription(
          editingSubscription.id,
          updateRequest
        );
        if (response.success) {
          message.success('Subscription updated successfully');
          setModalVisible(false);
          form.resetFields();
          loadSubscriptions();
          loadStats();
        } else {
          message.error(response.error || 'Failed to update subscription');
        }
      } else {
        // Create
        const createRequest: CreateSubscriptionRequest = {
          channel_type: values.channel_type,
          channel_identifier: values.channel_identifier,
          channel_name: values.channel_name,
          channel_description: values.channel_description,
          enabled: values.enabled,
        };
        
        const response = await subscriptionsAPI.createSubscription(createRequest);
        if (response.success) {
          setModalVisible(false);
          form.resetFields();
          
          // 🎉 优化：显示详细的成功提示
          if (values.enabled && values.channel_type === 'twitter') {
            message.success({
              content: (
                <div>
                  <div>✅ Subscription created successfully!</div>
                  <div style={{ fontSize: '12px', marginTop: '4px', opacity: 0.8 }}>
                    🔄 Fetching recent content (last 7 days)... This may take 30-60 seconds.
                  </div>
                </div>
              ),
              duration: 6,
            });
            
            // 3秒后刷新列表，显示初始获取的进度
            setTimeout(() => {
              loadSubscriptions();
              loadStats();
            }, 3000);
            
            // 再等30秒后刷新，显示首次获取的结果
            setTimeout(() => {
              loadSubscriptions();
              loadStats();
              message.info('Initial content fetch may still be in progress. Refresh the page if needed.');
            }, 30000);
          } else {
            message.success('Subscription created successfully');
          }
          
          loadSubscriptions();
          loadStats();
        } else {
          message.error(response.error || 'Failed to create subscription');
        }
      }
    } catch (error) {
      console.error('Form validation failed:', error);
    }
  };

  const handleModalCancel = () => {
    setModalVisible(false);
    form.resetFields();
    setEditingSubscription(null);
  };

  const getChannelIcon = (channelType: string) => {
    const config = CHANNEL_TYPES.find((c) => c.value === channelType);
    return config?.icon || <GlobalOutlined />;
  };

  const getChannelColor = (channelType: string) => {
    const config = CHANNEL_TYPES.find((c) => c.value === channelType);
    return config?.color || '#gray';
  };

  const columns = [
    {
      title: 'Channel',
      dataIndex: 'channel_type',
      key: 'channel_type',
      width: 120,
      render: (channelType: string) => (
        <Tag icon={getChannelIcon(channelType)} color={getChannelColor(channelType)}>
          {channelType.charAt(0).toUpperCase() + channelType.slice(1)}
        </Tag>
      ),
    },
    {
      title: 'Identifier / Name',
      key: 'identifier',
      render: (record: Subscription) => (
        <div>
          <div style={{ fontWeight: 500, color: colors.text }}>
            {record.channel_name || record.channel_identifier}
          </div>
          {record.channel_name && (
            <Text type="secondary" style={{ fontSize: 12 }}>
              @{record.channel_identifier}
            </Text>
          )}
        </div>
      ),
    },
    {
      title: 'Status',
      key: 'status',
      width: 100,
      render: (record: Subscription) => (
        <Space>
          <Switch
            checked={record.enabled}
            onChange={() => handleToggleEnabled(record)}
            size="small"
          />
          {record.consecutive_failures > 0 && (
            <Tooltip title={`${record.consecutive_failures} consecutive failures`}>
              <Badge status="error" />
            </Tooltip>
          )}
        </Space>
      ),
    },
    {
      title: 'Stats',
      key: 'stats',
      width: 150,
      render: (record: Subscription) => (
        <Space direction="vertical" size={0}>
          <Text style={{ fontSize: 12, color: colors.textSecondary }}>
            Total: {record.total_items_fetched}
          </Text>
          <Text style={{ fontSize: 12, color: colors.textSecondary }}>
            Last: {record.last_fetch_items_count || 0}
          </Text>
        </Space>
      ),
    },
    {
      title: 'Last Fetched',
      dataIndex: 'last_fetched_at',
      key: 'last_fetched_at',
      width: 150,
      render: (lastFetchedAt: string) =>
        lastFetchedAt ? (
          <Tooltip title={dayjs(lastFetchedAt).format('YYYY-MM-DD HH:mm:ss')}>
            <Text style={{ fontSize: 12, color: colors.textSecondary }}>
              {dayjs(lastFetchedAt).fromNow()}
            </Text>
          </Tooltip>
        ) : (
          <Text style={{ fontSize: 12, color: colors.textSecondary }}>Never</Text>
        ),
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 220,
      render: (record: Subscription) => (
        <Space>
          <Tooltip title="Edit">
            <Button
              type="text"
              size="small"
              icon={<EditOutlined />}
              onClick={() => handleEdit(record)}
            />
          </Tooltip>
          <Tooltip title="View History">
            <Button
              type="text"
              size="small"
              icon={<HistoryOutlined />}
              onClick={() => handleViewHistory(record)}
            />
          </Tooltip>
          {record.channel_type === 'twitter' && (
            <Tooltip title="Test Fetch">
              <Button
                type="text"
                size="small"
                icon={<ThunderboltOutlined />}
                onClick={() => handleTestFetch(record)}
              />
            </Tooltip>
          )}
          <Popconfirm
            title="Are you sure to delete this subscription?"
            onConfirm={() => handleDelete(record.id)}
            okText="Yes"
            cancelText="No"
          >
            <Tooltip title="Delete">
              <Button type="text" size="small" danger icon={<DeleteOutlined />} />
            </Tooltip>
          </Popconfirm>
        </Space>
      ),
    },
  ];

  return (
    <div style={{ backgroundColor: colors.pageBg, minHeight: '100vh', padding: 24 }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <Title level={2} style={{ margin: 0, color: colors.text }}>
          <UserOutlined style={{ marginRight: 12 }} />
          Subscription Management
        </Title>
        <Text type="secondary">
          Manage your Twitter, YouTube, and other channel subscriptions
        </Text>
      </div>

      {/* ⚠️ 废弃警告 */}
      <Alert
        message="📢 This page is being deprecated"
        description={
          <div>
            <p style={{ marginBottom: 8 }}>
              All subscription management features have been integrated into the <strong>Feed Reader</strong> page for a better user experience.
            </p>
            <p style={{ marginBottom: 8 }}>
              <strong>New features in Feed Reader:</strong>
            </p>
            <ul style={{ marginBottom: 8, paddingLeft: 20 }}>
              <li>✅ Add subscriptions directly while reading</li>
              <li>✅ Edit, Test Fetch, Delete from dropdown menu (click ⋯ icon)</li>
              <li>✅ View statistics with one click</li>
              <li>✅ Unread count badges</li>
              <li>✅ Auto-refresh with real-time notifications</li>
            </ul>
            <p>
              <strong>This page will be removed in the next version.</strong> Please use <a href="/feed-reader">Feed Reader</a> instead.
            </p>
          </div>
        }
        type="warning"
        showIcon
        closable
        style={{ marginBottom: 24 }}
      />

      {/* Statistics Cards */}
      {stats && (
        <Row gutter={16} style={{ marginBottom: 24 }}>
          <Col xs={24} sm={12} md={6}>
            <Card
              {...cardStyle}
              style={{ ...cardStyle, backgroundColor: colors.cardBg }}
            >
              <Statistic
                title={<span style={{ color: colors.textSecondary }}>Total Subscriptions</span>}
                value={stats.total_subscriptions}
                valueStyle={{ color: colors.text }}
                prefix={<GlobalOutlined />}
              />
            </Card>
          </Col>
          <Col xs={24} sm={12} md={6}>
            <Card
              {...cardStyle}
              style={{ ...cardStyle, backgroundColor: colors.cardBg }}
            >
              <Statistic
                title={<span style={{ color: colors.textSecondary }}>Enabled</span>}
                value={stats.enabled_subscriptions}
                valueStyle={{ color: '#52c41a' }}
                prefix={<CheckCircleOutlined />}
              />
            </Card>
          </Col>
          <Col xs={24} sm={12} md={6}>
            <Card
              {...cardStyle}
              style={{ ...cardStyle, backgroundColor: colors.cardBg }}
            >
              <Statistic
                title={<span style={{ color: colors.textSecondary }}>Total Items Fetched</span>}
                value={stats.total_items_fetched}
                valueStyle={{ color: colors.text }}
                prefix={<ThunderboltOutlined />}
              />
            </Card>
          </Col>
          <Col xs={24} sm={12} md={6}>
            <Card
              {...cardStyle}
              style={{ ...cardStyle, backgroundColor: colors.cardBg }}
            >
              <Statistic
                title={<span style={{ color: colors.textSecondary }}>Recent Failures</span>}
                value={stats.recent_failures}
                valueStyle={{ color: stats.recent_failures > 0 ? '#ff4d4f' : colors.text }}
                prefix={<CloseCircleOutlined />}
              />
            </Card>
          </Col>
        </Row>
      )}

      {/* Main Table Card */}
      <Card
        {...cardStyle}
        style={{ ...cardStyle, backgroundColor: colors.cardBg }}
        title={
          <Space>
            <Text strong style={{ color: colors.text }}>
              My Subscriptions
            </Text>
          </Space>
        }
        extra={
          <Space>
            <Select
              placeholder="Filter by channel"
              style={{ width: 150 }}
              allowClear
              value={filterChannelType}
              onChange={setFilterChannelType}
            >
              {CHANNEL_TYPES.map((type) => (
                <Option key={type.value} value={type.value}>
                  {type.icon} {type.label}
                </Option>
              ))}
            </Select>
            <Select
              placeholder="Filter by status"
              style={{ width: 120 }}
              allowClear
              value={filterEnabled}
              onChange={setFilterEnabled}
            >
              <Option value={true}>Enabled</Option>
              <Option value={false}>Disabled</Option>
            </Select>
            <Button icon={<ReloadOutlined />} onClick={loadSubscriptions} loading={loading}>
              Refresh
            </Button>
            <Button type="primary" icon={<PlusOutlined />} onClick={handleCreate}>
              Add Subscription
            </Button>
          </Space>
        }
      >
        <Table
          columns={columns}
          dataSource={subscriptions}
          rowKey="id"
          loading={loading}
          pagination={{
            pageSize: 10,
            showSizeChanger: true,
            showTotal: (total) => `Total ${total} items`,
          }}
        />
      </Card>

      {/* Create/Edit Modal */}
      <Modal
        title={editingSubscription ? 'Edit Subscription' : 'Add Subscription'}
        open={modalVisible}
        onOk={handleModalOk}
        onCancel={handleModalCancel}
        width={600}
        okText={editingSubscription ? 'Update' : 'Create'}
        styles={getModalStyles(themeMode)}
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="channel_type"
            label="Channel Type"
            rules={[{ required: true, message: 'Please select channel type' }]}
          >
            <Select
              placeholder="Select channel type"
              disabled={!!editingSubscription}
            >
              {CHANNEL_TYPES.map((type) => (
                <Option key={type.value} value={type.value}>
                  {type.icon} {type.label}
                </Option>
              ))}
            </Select>
          </Form.Item>

          <Form.Item
            name="channel_identifier"
            label="Channel Identifier"
            rules={[{ required: true, message: 'Please input channel identifier' }]}
            extra="Twitter username (without @), YouTube channel ID, etc."
          >
            <Input
              placeholder="e.g., elonmusk"
              disabled={!!editingSubscription}
            />
          </Form.Item>

          <Form.Item name="channel_name" label="Display Name (Optional)">
            <Input placeholder="Override display name (optional)" />
          </Form.Item>

          <Form.Item name="channel_description" label="Description">
            <Input.TextArea rows={2} placeholder="Optional description" />
          </Form.Item>

          <Form.Item name="enabled" label="Enabled" valuePropName="checked">
            <Switch />
          </Form.Item>
        </Form>
      </Modal>

      {/* History Drawer */}
      <Drawer
        title={
          selectedSubscription ? (
            <Space>
              {getChannelIcon(selectedSubscription.channel_type)}
              <span>
                {selectedSubscription.channel_name || selectedSubscription.channel_identifier}
              </span>
              <Text type="secondary" style={{ fontSize: 14 }}>
                - Fetch History
              </Text>
            </Space>
          ) : (
            'Fetch History'
          )
        }
        placement="right"
        width={600}
        open={historyDrawerVisible}
        onClose={() => {
          setHistoryDrawerVisible(false);
          setSelectedSubscription(null);
          setFetchHistory([]);
        }}
      >
        {historyLoading ? (
          <div style={{ textAlign: 'center', padding: 40 }}>Loading...</div>
        ) : fetchHistory.length > 0 ? (
          <Timeline>
            {fetchHistory.map((item) => (
              <Timeline.Item
                key={item.id}
                color={
                  item.status === 'success'
                    ? 'green'
                    : item.status === 'failed'
                    ? 'red'
                    : 'gray'
                }
                dot={
                  item.status === 'success' ? (
                    <CheckCircleOutlined />
                  ) : item.status === 'failed' ? (
                    <CloseCircleOutlined />
                  ) : (
                    <ClockCircleOutlined />
                  )
                }
              >
                <div style={{ marginBottom: 8 }}>
                  <Text strong>{dayjs(item.fetch_time).format('YYYY-MM-DD HH:mm:ss')}</Text>
                  <Text type="secondary" style={{ marginLeft: 8, fontSize: 12 }}>
                    ({dayjs(item.fetch_time).fromNow()})
                  </Text>
                </div>
                <Space direction="vertical" size={4}>
                  <Text>
                    Status: <Tag color={item.status === 'success' ? 'success' : 'error'}>{item.status}</Tag>
                  </Text>
                  <Text type="secondary">Fetched: {item.items_fetched}</Text>
                  <Text type="secondary">New: {item.items_new}</Text>
                  <Text type="secondary">Duplicated: {item.items_duplicated}</Text>
                  {item.execution_duration_ms && (
                    <Text type="secondary">Duration: {item.execution_duration_ms}ms</Text>
                  )}
                  {item.error_message && (
                    <Text type="danger" style={{ fontSize: 12 }}>
                      Error: {item.error_message}
                    </Text>
                  )}
                </Space>
              </Timeline.Item>
            ))}
          </Timeline>
        ) : (
          <Empty description="No fetch history yet" />
        )}
      </Drawer>
    </div>
  );
};

export default SubscriptionsPage;
