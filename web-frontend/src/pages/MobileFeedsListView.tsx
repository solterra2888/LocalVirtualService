import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  App,
  Avatar,
  Button,
  Drawer,
  Empty,
  Form,
  Input,
  List,
  Modal,
  Segmented,
  Space,
  Spin,
  Switch,
  Tag,
  Typography,
  Dropdown,
} from 'antd';
import type { MenuProps } from 'antd';
import {
  CheckOutlined,
  CommentOutlined,
  DeleteOutlined,
  EditOutlined,
  EyeInvisibleOutlined,
  EyeOutlined,
  FireOutlined,
  MenuOutlined,
  MessageOutlined,
  MoreOutlined,
  PlusOutlined,
  ReloadOutlined,
  RetweetOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useMobileFeeds } from '../contexts/MobileFeedsContext';
import { FEED_THEME_COLORS } from '../components/feed/feedTheme';
import { getChannelIcon } from '../components/feed/feedChannelConfig';
import {
  collapseWhitespaceForPreview,
  getFeedListHeadline,
  getHeatLevelConfig,
  showFeedListSecondaryExcerpt,
} from '../components/feed/feedUtils';
import FeedSourceIconPicker from '../components/feed/FeedSourceIconPicker';
import { getModalStyles } from '../utils/modalStyles';
import {
  subscriptionsAPI,
  Subscription,
  CreateSubscriptionRequest,
  SubscriptionContent,
} from '../services/subscriptions';
import './MobileFeedsPage.css';

dayjs.extend(relativeTime);

const { Text } = Typography;

/**
 * Mobile Feeds home: list-first, drawer for subscriptions, segmented All / Unread.
 */
const MobileFeedsListView: React.FC = () => {
  const { message } = App.useApp();
  const navigate = useNavigate();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = FEED_THEME_COLORS[themeMode];

  const {
    subscriptions,
    selectedSubscription,
    setSelectedSubscription,
    contents,
    loading,
    contentsLoading,
    loadSubscriptions,
    loadContents,
    loadMoreContents,
    hasMoreContents,
    loadingMore,
    contentTotal,
    contentOffset,
    markAsRead,
    onlyUnread,
    setOnlyUnread,
  } = useMobileFeeds();

  const [drawerOpen, setDrawerOpen] = useState(false);
  const [subscriptionFilter, setSubscriptionFilter] = useState<'active' | 'disabled'>('active');
  const [addModalVisible, setAddModalVisible] = useState(false);
  const [isSubmittingSubscription, setIsSubmittingSubscription] = useState(false);
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [editingSubscription, setEditingSubscription] = useState<Subscription | null>(null);

  const [form] = Form.useForm();
  const [editForm] = Form.useForm();

  const handleOpenArticle = (c: SubscriptionContent) => {
    if (!selectedSubscription) return;
    if (!c.is_read) {
      markAsRead(c.id);
    }
    navigate(`/feeds/article/${selectedSubscription.id}/${c.id}`, { state: { content: c } });
  };

  const handleCreateSubscription = async () => {
    try {
      const values = await form.validateFields();
      const request: CreateSubscriptionRequest = {
        channel_type: values.channel_type,
        channel_identifier: values.channel_identifier,
        enabled: values.enabled !== undefined ? values.enabled : true,
      };

      setIsSubmittingSubscription(true);
      const response = await subscriptionsAPI.createSubscription(request);
      if (response.success && response.data) {
        const newSubscription = response.data;
        setAddModalVisible(false);
        form.resetFields();
        message.success(`Subscription "@${values.channel_identifier}" created successfully`);
        await loadSubscriptions();
        setSelectedSubscription(newSubscription);

        if (newSubscription.channel_type === 'twitter') {
          const loadingMessage = message.loading({
            content: (
              <div>
                <div>Subscription created!</div>
                <div style={{ fontSize: '12px', marginTop: '4px', opacity: 0.8 }}>Loading content...</div>
              </div>
            ),
            duration: 0,
          });

          let pollCount = 0;
          const maxPolls = 12;
          const pollInterval = 5000;

          const pollForContent = async () => {
            try {
              pollCount++;
              const pollRes = await subscriptionsAPI.getSubscriptionContents(newSubscription.id, 20, 0, false);
              if (pollRes.success && pollRes.data && pollRes.data.contents.length > 0) {
                loadingMessage();
                message.success({ content: `✅ Loaded ${pollRes.data.contents.length} tweets!`, duration: 3 });
                await loadContents(newSubscription.id, onlyUnread);
                await loadSubscriptions();
                return true;
              }
              if (pollCount >= maxPolls) {
                loadingMessage();
                message.warning({
                  content: (
                    <div>
                      <div>Content is still loading in background.</div>
                      <div style={{ fontSize: '12px', marginTop: '4px', opacity: 0.8 }}>
                        Please check back in a minute or refresh manually.
                      </div>
                    </div>
                  ),
                  duration: 6,
                });
                return true;
              }
              setTimeout(pollForContent, pollInterval);
              return false;
            } catch {
              if (pollCount >= maxPolls) {
                loadingMessage();
                message.error('Failed to load content. Please refresh manually.');
                return true;
              }
              setTimeout(pollForContent, pollInterval);
              return false;
            }
          };

          setTimeout(pollForContent, 2000);
        } else {
          message.info({
            content: `Auto-fetch for ${newSubscription.channel_type} is coming soon`,
            duration: 3,
          });
        }
      } else {
        message.error(response.error || 'Failed to create subscription');
      }
    } catch (error: unknown) {
      const err = error as { errorFields?: unknown };
      if (!err?.errorFields) {
        message.error('Unexpected error, please try again');
        console.error('handleCreateSubscription error:', error);
      }
    } finally {
      setIsSubmittingSubscription(false);
    }
  };

  const handleEditSubscription = (subscription: Subscription) => {
    setEditingSubscription(subscription);
    editForm.setFieldsValue({
      channel_name: subscription.channel_name,
      channel_description: subscription.channel_description,
      enabled: subscription.enabled,
    });
    setEditModalVisible(true);
  };

  const handleSaveEdit = async () => {
    try {
      const values = await editForm.validateFields();
      if (!editingSubscription) return;

      const response = await subscriptionsAPI.updateSubscription(editingSubscription.id, {
        channel_name: values.channel_name,
        channel_description: values.channel_description,
        enabled: values.enabled,
      });

      if (response.success) {
        message.success('Subscription updated successfully');
        setEditModalVisible(false);
        editForm.resetFields();
        await loadSubscriptions();
        if (selectedSubscription?.id === editingSubscription.id && response.data) {
          setSelectedSubscription(response.data);
        }
      } else {
        message.error(response.error || 'Failed to update subscription');
      }
    } catch (error) {
      console.error('Failed to update subscription:', error);
    }
  };

  const handleDeleteSubscription = async (subscription: Subscription) => {
    try {
      const response = await subscriptionsAPI.deleteSubscription(subscription.id);
      if (response.success) {
        message.success('Subscription deleted successfully');
        await loadSubscriptions();
        if (selectedSubscription?.id === subscription.id) {
          setSelectedSubscription(null);
        }
      } else {
        message.error(response.error || 'Failed to delete subscription');
      }
    } catch (error) {
      console.error('Failed to delete subscription:', error);
      message.error('Failed to delete subscription');
    }
  };

  const handleToggleEnabled = async (subscription: Subscription) => {
    const newEnabledState = !subscription.enabled;
    try {
      const response = await subscriptionsAPI.updateSubscription(subscription.id, {
        enabled: newEnabledState,
      });
      if (response.success) {
        message.success(`@${subscription.channel_identifier} ${newEnabledState ? 'enabled' : 'disabled'}.`);
        await loadSubscriptions();
        if (selectedSubscription?.id === subscription.id && response.data) {
          setSelectedSubscription(response.data);
        }
      } else {
        message.error(response.error || 'Failed to update subscription');
      }
    } catch (error) {
      console.error('Failed to toggle subscription:', error);
      message.error('Failed to update subscription');
    }
  };

  const filteredSubscriptions = subscriptions.filter((sub) =>
    subscriptionFilter === 'active' ? sub.enabled : !sub.enabled
  );

  const subscriptionMenu = (subscription: Subscription): MenuProps['items'] => [
    {
      key: 'edit',
      label: 'Edit',
      icon: <EditOutlined />,
      onClick: () => handleEditSubscription(subscription),
    },
    {
      key: 'toggle',
      label: subscription.enabled ? 'Disable' : 'Enable',
      icon: subscription.enabled ? <EyeInvisibleOutlined /> : <EyeOutlined />,
      onClick: () => handleToggleEnabled(subscription),
    },
    { type: 'divider' },
    {
      key: 'delete',
      label: 'Delete',
      icon: <DeleteOutlined />,
      danger: true,
      onClick: () => {
        Modal.confirm({
          title: 'Delete Subscription',
          content: `Are you sure you want to delete @${subscription.channel_identifier}?`,
          okText: 'Delete',
          okType: 'danger',
          onOk: () => handleDeleteSubscription(subscription),
        });
      },
    },
  ];

  return (
    <div>
      <div className="mobile-feeds-toolbar">
        <Button
          type="text"
          icon={<MenuOutlined style={{ fontSize: 18 }} />}
          onClick={() => setDrawerOpen(true)}
          aria-label="Subscriptions"
        />
        <div className="mobile-feeds-toolbar-title" style={{ color: themeColors.text }}>
          {selectedSubscription
            ? selectedSubscription.channel_name || `@${selectedSubscription.channel_identifier}`
            : 'Feeds'}
        </div>
        <Space size={4}>
          <Button
            type="text"
            icon={<ReloadOutlined />}
            onClick={() => {
              loadSubscriptions();
              if (selectedSubscription) loadContents(selectedSubscription.id, onlyUnread);
            }}
            loading={loading}
          />
          <Button type="primary" size="small" icon={<PlusOutlined />} onClick={() => setAddModalVisible(true)} />
        </Space>
      </div>

      <div className="mobile-feeds-segment">
        <Segmented
          block
          value={onlyUnread ? 'unread' : 'all'}
          onChange={(v) => {
            setOnlyUnread(v === 'unread');
          }}
          options={[
            { label: 'All', value: 'all' },
            { label: 'Unread', value: 'unread' },
          ]}
        />
      </div>

      {loading && subscriptions.length === 0 ? (
        <div style={{ display: 'flex', justifyContent: 'center', padding: 48 }}>
          <Spin />
        </div>
      ) : !selectedSubscription ? (
        <div style={{ padding: 24 }}>
          <Empty description="Add a feed or pick one from the menu" />
        </div>
      ) : contentsLoading ? (
        <div style={{ display: 'flex', justifyContent: 'center', padding: 48 }}>
          <Spin />
        </div>
      ) : contents.length === 0 ? (
        <div style={{ padding: 24 }}>
          <Empty
            description={
              selectedSubscription.last_fetched_at
                ? 'No content in this filter'
                : 'Fetching content… try refresh in a moment'
            }
          />
        </div>
      ) : (
        <div className="mobile-feeds-list-scroll">
          {contents.map((c) => (
            <div
              key={c.id}
              role="button"
              tabIndex={0}
              onClick={() => handleOpenArticle(c)}
              onKeyDown={(e) => e.key === 'Enter' && handleOpenArticle(c)}
              className={`mobile-feeds-card ${!c.is_read ? 'mobile-feeds-card-unread' : ''}`}
              style={{
                opacity: c.is_read ? 0.72 : 1,
                cursor: 'pointer',
              }}
            >
              {/* 与 PC 中栏一致：标题+摘要区域限高 96px，各行最多 2 行（避免 flex/min-height 与 Typography 在移动端撑破限高） */}
              <div
                className="mobile-feeds-card-snippet"
                style={{
                  marginBottom: 8,
                }}
              >
                <div
                  style={{
                    display: 'flex',
                    justifyContent: 'space-between',
                    alignItems: 'flex-start',
                    gap: 8,
                  }}
                >
                  <span
                    style={{
                      color: themeColors.text,
                      fontWeight: 600,
                      fontSize: 14,
                      lineHeight: 1.4,
                      flex: 1,
                      minWidth: 0,
                      minHeight: 0,
                      display: '-webkit-box',
                      WebkitLineClamp: 2,
                      WebkitBoxOrient: 'vertical',
                      overflow: 'hidden',
                      wordBreak: 'break-word',
                      overflowWrap: 'anywhere',
                    }}
                  >
                    {collapseWhitespaceForPreview(getFeedListHeadline(c))}
                  </span>
                  {c.is_read && <CheckOutlined style={{ color: '#52c41a', flexShrink: 0, marginTop: 2 }} />}
                </div>
                {showFeedListSecondaryExcerpt(c) && (
                  <span
                    style={{
                      marginTop: 5,
                      color: themeColors.textSecondary,
                      fontSize: 13,
                      lineHeight: 1.4,
                      minHeight: 0,
                      display: '-webkit-box',
                      WebkitLineClamp: 2,
                      WebkitBoxOrient: 'vertical',
                      overflow: 'hidden',
                      wordBreak: 'break-word',
                      overflowWrap: 'anywhere',
                    }}
                  >
                    {collapseWhitespaceForPreview(c.content || '')}
                  </span>
                )}
              </div>
              <div>
                <Space size={6} wrap>
                  {c.is_retweet && <RetweetOutlined style={{ color: '#00ba7c', fontSize: 12 }} />}
                  {c.is_quote && <MessageOutlined style={{ color: '#1DA1F2', fontSize: 12 }} />}
                  {c.is_reply && <CommentOutlined style={{ color: '#657786', fontSize: 12 }} />}
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    {c.author_name || (c.author_identifier ? `@${c.author_identifier}` : '')}
                  </Text>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    ·
                  </Text>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    {c.published_at ? dayjs(c.published_at).fromNow() : ''}
                  </Text>
                  {c.heat_level != null && c.heat_level >= 1 && (
                    <FireOutlined style={{ color: getHeatLevelConfig(c.heat_level).color, fontSize: 13 }} />
                  )}
                </Space>
              </div>
            </div>
          ))}

          {hasMoreContents && (
            <div style={{ textAlign: 'center', padding: 16 }}>
              <Button type="link" loading={loadingMore} onClick={() => loadMoreContents()} style={{ color: themeColors.text }}>
                {loadingMore ? 'Loading…' : `Load more (${Math.max(0, contentTotal - contentOffset)} left)`}
              </Button>
            </div>
          )}
        </div>
      )}

      <Drawer
        title={<span style={{ color: themeColors.text }}>My feeds</span>}
        placement="left"
        width="min(100%, 360px)"
        onClose={() => setDrawerOpen(false)}
        open={drawerOpen}
        rootClassName={isDark ? 'mobile-feeds-drawer--dark' : 'mobile-feeds-drawer--light'}
        styles={{
          content: { background: themeColors.cardBg },
          header: {
            background: themeColors.cardBg,
            borderBottom: `1px solid ${themeColors.cardBorder}`,
          },
          body: { background: themeColors.cardBg, paddingTop: 12 },
          ...(isDark ? { mask: { background: 'rgba(0, 0, 0, 0.65)' } } : {}),
        }}
      >
        <Segmented
          block
          value={subscriptionFilter}
          onChange={(v) => setSubscriptionFilter(v as 'active' | 'disabled')}
          options={[
            { label: `Active (${subscriptions.filter((s) => s.enabled).length})`, value: 'active' },
            { label: `Disabled (${subscriptions.filter((s) => !s.enabled).length})`, value: 'disabled' },
          ]}
          style={{
            marginBottom: 12,
            ...(isDark && { background: themeColors.boxMuted }),
          }}
        />

        {filteredSubscriptions.length === 0 ? (
          <Empty
            description={
              <span style={{ color: themeColors.textSecondary }}>
                {subscriptionFilter === 'active' ? 'No active feeds' : 'No disabled feeds'}
              </span>
            }
          />
        ) : (
          <List
            dataSource={filteredSubscriptions}
            renderItem={(sub: Subscription) => (
              <List.Item style={{ padding: '8px 0', border: 'none' }}>
                <div
                  className="mobile-feeds-drawer-sub"
                  style={{
                    width: '100%',
                    cursor: 'pointer',
                    backgroundColor:
                      selectedSubscription?.id === sub.id ? themeColors.selectedItemBg : 'transparent',
                  }}
                  onClick={() => {
                    setSelectedSubscription(sub);
                    setDrawerOpen(false);
                  }}
                >
                  <Avatar
                    icon={getChannelIcon(sub.channel_type)}
                    style={{
                      backgroundColor:
                        sub.channel_type === 'twitter' ? '#1DA1F2' : sub.channel_type === 'youtube' ? '#FF0000' : '#ccc',
                    }}
                  />
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <Space size={6}>
                      <Text strong ellipsis style={{ maxWidth: 200, color: themeColors.text }}>
                        {sub.channel_name || `@${sub.channel_identifier}`}
                      </Text>
                      {!sub.enabled && (
                        <Tag color="error" style={{ margin: 0, fontSize: 10 }}>
                          Off
                        </Tag>
                      )}
                    </Space>
                    {sub.unread_count != null && sub.unread_count > 0 && (
                      <div>
                        <Tag
                          style={{
                            marginTop: 4,
                            background: themeColors.boxMuted,
                            color: themeColors.textSecondary,
                            border: `1px solid ${themeColors.cardBorder}`,
                          }}
                        >
                          {sub.unread_count > 99 ? '99+' : sub.unread_count} new
                        </Tag>
                      </div>
                    )}
                  </div>
                  <Dropdown menu={{ items: subscriptionMenu(sub) }} trigger={['click']} placement="bottomRight">
                    <Button
                      type="text"
                      size="small"
                      icon={<MoreOutlined />}
                      onClick={(e) => e.stopPropagation()}
                      style={{ color: themeColors.textSecondary }}
                    />
                  </Dropdown>
                </div>
              </List.Item>
            )}
          />
        )}
      </Drawer>

      <Modal
        title="Add feed"
        open={addModalVisible}
        onOk={handleCreateSubscription}
        onCancel={() => {
          if (!isSubmittingSubscription) setAddModalVisible(false);
        }}
        okText="Add"
        okButtonProps={{ loading: isSubmittingSubscription }}
        cancelButtonProps={{ disabled: isSubmittingSubscription }}
        styles={getModalStyles(themeMode)}
      >
        <Form form={form} layout="vertical">
          <Form.Item name="channel_type" label="Source" rules={[{ required: true, message: 'Select a source' }]}>
            <FeedSourceIconPicker />
          </Form.Item>
          <Form.Item
            noStyle
            shouldUpdate={(prev, cur) => prev.channel_type !== cur.channel_type}
          >
            {({ getFieldValue }) => {
              const channelType = getFieldValue('channel_type');
              return (
                <Form.Item
                  name="channel_identifier"
                  label={channelType === 'twitter' ? 'Twitter username' : 'Channel'}
                  rules={[{ required: true, message: 'Required' }]}
                  extra={
                    channelType === 'twitter'
                      ? 'Without @ (e.g. elonmusk)'
                      : 'YouTube channel handle or name'
                  }
                >
                  <Input placeholder={channelType === 'twitter' ? 'elonmusk' : 'channel'} />
                </Form.Item>
              );
            }}
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title="Edit subscription"
        open={editModalVisible}
        onOk={handleSaveEdit}
        onCancel={() => {
          setEditModalVisible(false);
          editForm.resetFields();
        }}
        okText="Save"
        styles={getModalStyles(themeMode)}
      >
        <Form form={editForm} layout="vertical">
          <Form.Item label="Channel" extra={`${editingSubscription?.channel_type}: @${editingSubscription?.channel_identifier}`}>
            <Text type="secondary">Type and identifier cannot be changed</Text>
          </Form.Item>
          <Form.Item name="channel_name" label="Display name">
            <Input placeholder="Optional" />
          </Form.Item>
          <Form.Item name="channel_description" label="Description">
            <Input.TextArea rows={2} />
          </Form.Item>
          <Form.Item name="enabled" label="Status" valuePropName="checked">
            <Switch checkedChildren="On" unCheckedChildren="Off" />
          </Form.Item>
        </Form>
      </Modal>
    </div>
  );
};

export default MobileFeedsListView;
