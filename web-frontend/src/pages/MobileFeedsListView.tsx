import React, { useState, useMemo, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Alert,
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
  Tooltip,
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
  PlayCircleOutlined,
  PauseCircleOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useMobileFeeds } from '../contexts/MobileFeedsContext';
import { FEED_THEME_COLORS } from '../components/feed/feedTheme';
import {
  getChannelColor,
  getChannelIcon,
  getNewsSourceColor,
  getNewsSourceFaviconUrl,
  getNewsSourceLabel,
  FEED_CHANNEL_TYPE_FILTER_TABS,
  matchesFeedChannelTypeFilter,
  type FeedChannelTypeFilterKey,
} from '../components/feed/feedChannelConfig';
import {
  collapseWhitespaceForPreview,
  getFeedListHeadline,
  getFeedListBodyExcerpt,
  getHeatLevelConfig,
  normalizeTwitterUsername,
  showFeedListSecondaryExcerpt,
  feedListPrimaryLineIsTweetBody,
  subscriptionContentMatchesSearchQuery,
  estimateSubscriptionContentReadingMinutes,
  getFeedAuthorAvatarSrc,
  feedListShowsAuthorAvatar,
} from '../components/feed/feedUtils';
import SubstackFeedBadges, { getFeedListAuthorLabel } from '../components/feed/SubstackFeedBadges';
import FeedSourceIconPicker, { parsePickerValue } from '../components/feed/FeedSourceIconPicker';
import { weiboSafeAvatarSrc } from '../components/feed/weiboSafeImage';
import { getModalStyles } from '../utils/modalStyles';
import SubscriptionDigestButton from '../components/feed/SubscriptionDigestButton';
import {
  subscriptionsAPI,
  Subscription,
  CreateSubscriptionRequest,
  SubscriptionContent,
} from '../services/subscriptions';
import './MobileFeedsPage.css';
import '../styles/mobilePageLoading.css';

dayjs.extend(relativeTime);

const { Text } = Typography;

/**
 * Mobile Feeds home: list-first, drawer for subscriptions, segmented All / Unread.
 */
const MobileFeedsListView: React.FC = () => {
  const { message, modal } = App.useApp();
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
  const [feedChannelTypeFilter, setFeedChannelTypeFilter] = useState<FeedChannelTypeFilterKey>('all');
  const [addModalVisible, setAddModalVisible] = useState(false);
  const [isSubmittingSubscription, setIsSubmittingSubscription] = useState(false);
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [editingSubscription, setEditingSubscription] = useState<Subscription | null>(null);
  const [contentSearchQuery, setContentSearchQuery] = useState('');
  const [selectedShowFilter, setSelectedShowFilter] = useState<string | null>(null);
  const showFilterStripRef = useRef<HTMLDivElement>(null);

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
      const { channelType, channelIdentifier: newsSource } = parsePickerValue(values.channel_type);
      let channelIdentifier = newsSource ?? values.channel_identifier;
      if (channelType === 'twitter' && typeof channelIdentifier === 'string') {
        channelIdentifier = normalizeTwitterUsername(channelIdentifier);
      }
      const request: CreateSubscriptionRequest = {
        channel_type: channelType,
        channel_identifier: channelIdentifier,
        enabled: values.enabled !== undefined ? values.enabled : true,
      };

      setIsSubmittingSubscription(true);
      const response = await subscriptionsAPI.createSubscription(request);
      if (response.success && response.data) {
        const newSubscription = response.data;
        setAddModalVisible(false);
        form.resetFields();
        message.success(`Subscription "@${channelIdentifier}" created successfully`);
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
        } else if (newSubscription.channel_type === 'podcast') {
          message.info({
            content: (
              <div>
                <div>Podcast subscription created!</div>
                <div style={{ fontSize: '12px', marginTop: '4px', opacity: 0.85 }}>
                  Episodes are fetched and transcribed in the background. Each episode may take
                  10–20+ minutes — please be patient. Transcripts will appear in the feed when ready.
                </div>
              </div>
            ),
            duration: 8,
          });
          await loadContents(newSubscription.id);
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

  const filteredSubscriptions = useMemo(
    () =>
      subscriptions
        .filter((sub) => (subscriptionFilter === 'active' ? sub.enabled : !sub.enabled))
        .filter((sub) => matchesFeedChannelTypeFilter(sub.channel_type, feedChannelTypeFilter)),
    [subscriptions, subscriptionFilter, feedChannelTypeFilter]
  );

  useEffect(() => {
    if (filteredSubscriptions.length === 0) {
      setSelectedSubscription(null);
      return;
    }
    setSelectedSubscription((prev) => {
      if (prev && filteredSubscriptions.some((s) => s.id === prev.id)) return prev;
      return filteredSubscriptions[0];
    });
  }, [filteredSubscriptions, setSelectedSubscription]);

  const selectedSubscriptionId = selectedSubscription?.id;
  useEffect(() => {
    setContentSearchQuery('');
    setSelectedShowFilter(null);
  }, [selectedSubscriptionId]);

  const availableShows = useMemo(() => {
    const names = new Set<string>();
    for (const c of contents) {
      if (c.channel_type === 'youtube' && c.metadata?.show_name) {
        names.add(c.metadata.show_name as string);
      }
    }
    return Array.from(names).sort();
  }, [contents]);

  /** 与桌面 FeedReaderPage 一致：触控板纵滚映射为横向滚动节目 chip */
  useEffect(() => {
    const el = showFilterStripRef.current;
    if (!el || availableShows.length === 0) return;

    const onWheel = (e: WheelEvent) => {
      if (el.scrollWidth <= el.clientWidth + 1) return;

      if (!e.shiftKey && Math.abs(e.deltaX) > Math.abs(e.deltaY)) {
        return;
      }

      const scrollDelta = e.shiftKey ? e.deltaY + e.deltaX : e.deltaY;
      const maxScroll = el.scrollWidth - el.clientWidth;
      const prev = el.scrollLeft;
      const next = Math.max(0, Math.min(maxScroll, prev + scrollDelta));

      if (next !== prev) {
        el.scrollLeft = next;
        e.preventDefault();
      }
    };

    el.addEventListener('wheel', onWheel, { passive: false });
    return () => el.removeEventListener('wheel', onWheel);
  }, [availableShows.length]);

  const filteredContents = useMemo(() => {
    let result = contents;
    const q = contentSearchQuery.trim();
    if (q) result = result.filter((c) => subscriptionContentMatchesSearchQuery(c, q));
    if (selectedShowFilter) result = result.filter((c) => c.metadata?.show_name === selectedShowFilter);
    return result;
  }, [contents, contentSearchQuery, selectedShowFilter]);

  const searchActive = Boolean(contentSearchQuery.trim());

  const toolbarSubline = (sub: Subscription | null) => {
    if (!sub) return null;
    if (sub.channel_type === 'news') {
      const label = getNewsSourceLabel(sub.channel_identifier);
      const main =
        (sub.channel_name && sub.channel_name.trim()) ||
        `@${sub.channel_identifier}`;
      const normalized = main.startsWith('@') ? main.slice(1) : main;
      if (normalized === label) return 'News';
      return label;
    }
    if (sub.channel_type === 'twitter') return `@${sub.channel_identifier}`;
    return sub.channel_identifier;
  };

  const subscriptionMenu = (subscription: Subscription): MenuProps['items'] => [
    {
      key: 'toggle',
      label: subscription.enabled ? 'Disable' : 'Enable',
      icon: subscription.enabled ? <EyeInvisibleOutlined /> : <EyeOutlined />,
      onClick: () => handleToggleEnabled(subscription),
    },
    {
      key: 'edit',
      label: 'Edit',
      icon: <EditOutlined />,
      onClick: () => handleEditSubscription(subscription),
    },
    { type: 'divider' },
    {
      key: 'delete',
      label: 'Delete',
      icon: <DeleteOutlined />,
      danger: true,
      onClick: () => {
        modal.confirm({
          title: 'Delete Subscription',
          content: `Are you sure you want to delete @${subscription.channel_identifier}?`,
          okText: 'Delete',
          okType: 'danger',
          cancelText: 'Cancel',
          centered: true,
          styles: getModalStyles(themeMode),
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
          style={{ color: themeColors.textSecondary }}
        />
        <div className="mobile-feeds-toolbar-title-block">
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: 10,
              minWidth: 0,
              width: '100%',
            }}
          >
            <div
              className="mobile-feeds-toolbar-title"
              style={{ color: themeColors.text, flex: 1, minWidth: 0 }}
            >
              {selectedSubscription
                ? selectedSubscription.channel_name || `@${selectedSubscription.channel_identifier}`
                : 'Feeds'}
            </div>
            {selectedSubscription && (
              <Text type="secondary" style={{ fontSize: 12, flexShrink: 0 }}>
                {searchActive ? `${filteredContents.length} / ` : ''}
                {contents.length} articles
              </Text>
            )}
          </div>
          {selectedSubscription && (
            <div className="mobile-feeds-toolbar-sub" style={{ color: themeColors.textSecondary }}>
              {toolbarSubline(selectedSubscription)}
            </div>
          )}
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
            style={{ color: themeColors.textSecondary }}
          />
          <Button
            type="primary"
            size="small"
            shape="round"
            icon={<PlusOutlined />}
            onClick={() => setAddModalVisible(true)}
            aria-label="Add feed"
          />
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

      {selectedSubscription && (
        <div className="mobile-feeds-search-wrap">
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            <div style={{ flex: 1, minWidth: 0 }}>
              <Input.Search
                allowClear
                size="small"
                placeholder="Search articles…"
                value={contentSearchQuery}
                onChange={(e) => setContentSearchQuery(e.target.value)}
                aria-label="Search bar"
                style={{ width: '100%' }}
              />
            </div>
            <SubscriptionDigestButton
              subscriptionId={selectedSubscription.id}
              channelName={
                selectedSubscription.channel_name ||
                selectedSubscription.channel_identifier
              }
              themeMode={themeMode}
              iconOnly
            />
          </div>
        </div>
      )}

      {selectedSubscription && availableShows.length > 0 && (
        <div
          ref={showFilterStripRef}
          className="mobile-feeds-show-filter-strip"
          style={{ borderBottom: `1px solid ${themeColors.cardBorder}` }}
        >
          {(['All', ...availableShows] as const).map((show) => {
            const isActive = show === 'All' ? selectedShowFilter === null : selectedShowFilter === show;
            return (
              <button
                key={show}
                type="button"
                onClick={() => setSelectedShowFilter(show === 'All' ? null : show)}
                style={{
                  flexShrink: 0,
                  padding: '2px 10px',
                  fontSize: 11,
                  lineHeight: '18px',
                  borderRadius: 10,
                  border: 'none',
                  cursor: 'pointer',
                  fontWeight: isActive ? 600 : 400,
                  backgroundColor: isActive
                    ? themeColors.selectedBorder
                    : isDark
                      ? 'rgba(255,255,255,0.07)'
                      : 'rgba(0,0,0,0.05)',
                  color: isActive ? '#fff' : themeColors.textSecondary,
                  transition: 'background-color 0.15s, color 0.15s',
                  letterSpacing: '0.01em',
                  whiteSpace: 'nowrap',
                }}
              >
                {show}
              </button>
            );
          })}
        </div>
      )}

      {loading && subscriptions.length === 0 ? (
        <div className="mobile-page-loading-wrap" aria-busy="true" aria-live="polite">
          <Spin size="large" />
        </div>
      ) : !selectedSubscription ? (
        <div style={{ padding: '32px 20px', textAlign: 'center' }}>
          <Empty
            description={
              <span style={{ color: themeColors.textSecondary }}>
                Add a feed with +, or open the menu to choose a subscription
              </span>
            }
          />
        </div>
      ) : contentsLoading ? (
        <div className="mobile-page-loading-wrap" aria-busy="true" aria-live="polite">
          <Spin size="large" />
        </div>
      ) : contents.length === 0 ? (
        <div style={{ padding: '32px 20px', textAlign: 'center' }}>
          <Empty
            description={
              <span style={{ color: themeColors.textSecondary }}>
                {selectedSubscription.last_fetched_at
                  ? 'No items match this filter'
                  : 'Loading content… pull to refresh or try again shortly'}
              </span>
            }
          />
        </div>
      ) : filteredContents.length === 0 ? (
        <div style={{ padding: '32px 20px', textAlign: 'center' }}>
          <Empty
            description={
              <span style={{ color: themeColors.textSecondary }}>
                No articles match your search or filters
              </span>
            }
          />
        </div>
      ) : (
        <div className="mobile-feeds-list-scroll">
          {filteredContents.map((c) => {
            const primaryIsTweetBody = feedListPrimaryLineIsTweetBody(c);
            const readingMin = estimateSubscriptionContentReadingMinutes(c);
            return (
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
                      fontWeight: primaryIsTweetBody ? 400 : 600,
                      fontSize: primaryIsTweetBody ? 12 : 13,
                      lineHeight: 1.45,
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
                      fontSize: 12,
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
                    {collapseWhitespaceForPreview(getFeedListBodyExcerpt(c))}
                  </span>
                )}
              </div>
              <div>
                <Space size={6} wrap>
                  <SubstackFeedBadges content={c} />
                  {feedListShowsAuthorAvatar(c) && (
                    <Avatar size={16} src={getFeedAuthorAvatarSrc(c)} style={{ flexShrink: 0 }} />
                  )}
                  {c.is_retweet && <RetweetOutlined style={{ color: '#00ba7c', fontSize: 12 }} />}
                  {c.is_quote && <MessageOutlined style={{ color: '#1DA1F2', fontSize: 12 }} />}
                  {c.is_reply && <CommentOutlined style={{ color: '#657786', fontSize: 12 }} />}
                  <Tooltip
                    title={
                      c.channel_type === 'youtube' &&
                      c.metadata?.show_name &&
                      c.author_name
                        ? c.author_name
                        : undefined
                    }
                  >
                    <Text type="secondary" style={{ fontSize: 12 }}>
                      {getFeedListAuthorLabel(c)}
                    </Text>
                  </Tooltip>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    ·
                  </Text>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    {c.published_at ? dayjs(c.published_at).fromNow() : ''}
                  </Text>
                  {readingMin != null && (
                    <>
                      <Text type="secondary" style={{ fontSize: 12 }}>·</Text>
                      <Tooltip title="Estimated reading time">
                        <span
                          style={{
                            display: 'inline-flex',
                            alignItems: 'center',
                            gap: 4,
                            color: themeColors.textSecondary,
                            fontSize: 12,
                          }}
                        >
                          <ClockCircleOutlined style={{ fontSize: 10 }} />
                          {readingMin} min
                        </span>
                      </Tooltip>
                    </>
                  )}
                  {c.heat_level != null && c.heat_level >= 1 && (
                    <>
                      {readingMin != null && (
                        <Text type="secondary" style={{ fontSize: 12 }}>·</Text>
                      )}
                      <FireOutlined style={{ color: getHeatLevelConfig(c.heat_level).color, fontSize: 13 }} />
                    </>
                  )}
                </Space>
              </div>
            </div>
            );
          })}

          {hasMoreContents && (
            <div className="mobile-feeds-load-more">
              <Button
                type="default"
                block
                loading={loadingMore}
                onClick={() => loadMoreContents()}
                style={{
                  color: themeColors.text,
                  borderColor: themeColors.cardBorder,
                  background: themeColors.cardBg,
                  borderRadius: 12,
                  height: 44,
                  fontWeight: 600,
                }}
              >
                {loadingMore ? 'Loading…' : `Load more · ${Math.max(0, contentTotal - contentOffset)} left`}
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
          body: {
            background: themeColors.cardBg,
            paddingTop: 4,
            paddingLeft: 0,
            paddingRight: 0,
            paddingBottom: 8,
            overflowY: 'auto',
          },
          ...(isDark ? { mask: { background: 'rgba(0, 0, 0, 0.65)' } } : {}),
        }}
      >
        <div className="mobile-feeds-drawer-body-pad">
        <div
          className={`mobile-feeds-drawer-filter-row${isDark ? ' mobile-feeds-drawer-filter-row--dark' : ''}`}
        >
          <div className="mobile-feeds-drawer-filter-surface">
            <div className="mobile-feeds-drawer-type-tabs" aria-label="Feed type">
              {FEED_CHANNEL_TYPE_FILTER_TABS.map(({ key, label, icon }) => (
                <Tooltip key={key} title={label}>
                  <button
                    type="button"
                    className={`mobile-feeds-drawer-type-tab ${feedChannelTypeFilter === key ? 'is-active' : ''}`}
                    onClick={() => setFeedChannelTypeFilter(key)}
                    aria-label={label}
                    aria-pressed={feedChannelTypeFilter === key}
                  >
                    <span className="mobile-feeds-drawer-type-tab-icon">{icon}</span>
                  </button>
                </Tooltip>
              ))}
            </div>
            <div className="mobile-feeds-drawer-filter-divider" aria-hidden />
            <div className="mobile-feeds-drawer-status-tabs" role="group" aria-label="Subscription status">
              <Tooltip title={`Active (${subscriptions.filter((s) => s.enabled).length})`}>
                <button
                  type="button"
                  className={`mobile-feeds-drawer-type-tab mobile-feeds-drawer-status-tab ${
                    subscriptionFilter === 'active' ? 'is-active' : ''
                  }`}
                  onClick={() => setSubscriptionFilter('active')}
                  aria-label="Active subscriptions"
                  aria-pressed={subscriptionFilter === 'active'}
                >
                  <span className="mobile-feeds-drawer-type-tab-icon">
                    <PlayCircleOutlined />
                  </span>
                </button>
              </Tooltip>
              <Tooltip title={`Disabled (${subscriptions.filter((s) => !s.enabled).length})`}>
                <button
                  type="button"
                  className={`mobile-feeds-drawer-type-tab mobile-feeds-drawer-status-tab ${
                    subscriptionFilter === 'disabled' ? 'is-active' : ''
                  }`}
                  onClick={() => setSubscriptionFilter('disabled')}
                  aria-label="Disabled subscriptions"
                  aria-pressed={subscriptionFilter === 'disabled'}
                >
                  <span className="mobile-feeds-drawer-type-tab-icon">
                    <PauseCircleOutlined />
                  </span>
                </button>
              </Tooltip>
            </div>
          </div>
        </div>

        {filteredSubscriptions.length === 0 ? (
          <Empty
            style={{ margin: '24px 0' }}
            description={
              <span style={{ color: themeColors.textSecondary }}>
                {feedChannelTypeFilter !== 'all'
                  ? `No ${FEED_CHANNEL_TYPE_FILTER_TABS.find((t) => t.key === feedChannelTypeFilter)?.label ?? 'matching'} feeds`
                  : subscriptionFilter === 'active'
                    ? 'No active feeds'
                    : 'No disabled feeds'}
              </span>
            }
          />
        ) : (
          <List
            dataSource={filteredSubscriptions}
            renderItem={(sub: Subscription) => {
              const newsFavicon =
                sub.channel_type === 'news' ? getNewsSourceFaviconUrl(sub.channel_identifier) : undefined;
              const channelAvatarSrc =
                ['twitter', 'weibo', 'substack', 'zhishixingqiu'].includes(sub.channel_type) && sub.channel_avatar
                  ? weiboSafeAvatarSrc(sub.channel_avatar)
                  : undefined;
              return (
              <List.Item style={{ padding: '8px 0', border: 'none' }}>
                <div
                  className={`mobile-feeds-drawer-sub${
                    selectedSubscription?.id === sub.id ? ' mobile-feeds-drawer-sub--selected' : ''
                  }`}
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
                    size={40}
                    src={channelAvatarSrc ?? newsFavicon}
                    icon={
                      channelAvatarSrc
                        ? undefined
                        : sub.channel_type === 'news' && newsFavicon
                          ? undefined
                          : getChannelIcon(sub.channel_type)
                    }
                    style={{
                      backgroundColor:
                        channelAvatarSrc
                          ? 'transparent'
                          : sub.channel_type === 'news'
                            ? newsFavicon
                              ? '#fff'
                              : getNewsSourceColor(sub.channel_identifier)
                            : getChannelColor(sub.channel_type),
                    }}
                  />
                  <div style={{ flex: 1, minWidth: 0, overflow: 'hidden' }}>
                    <div
                      style={{
                        display: 'flex',
                        alignItems: 'center',
                        gap: 6,
                        minWidth: 0,
                      }}
                    >
                      <Text
                        strong
                        ellipsis={{ tooltip: true }}
                        style={{
                          flex: 1,
                          minWidth: 0,
                          color: themeColors.text,
                        }}
                      >
                        {sub.channel_name || `@${sub.channel_identifier}`}
                      </Text>
                    </div>
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
              );
            }}
          />
        )}
        </div>
      </Drawer>

      <Modal
        title={
          <Space>
            <PlusOutlined />
            <span>Add New Feed</span>
          </Space>
        }
        open={addModalVisible}
        onOk={handleCreateSubscription}
        onCancel={() => {
          if (!isSubmittingSubscription) setAddModalVisible(false);
        }}
        okText="Add"
        cancelText="Cancel"
        okButtonProps={{ loading: isSubmittingSubscription }}
        cancelButtonProps={{ disabled: isSubmittingSubscription }}
        width="min(900px, calc(100vw - 32px))"
        centered
        destroyOnClose
        maskClosable={!isSubmittingSubscription}
        classNames={{
          content: isDark ? 'upload-files-modal upload-files-modal--dark' : 'upload-files-modal',
        }}
        maskStyle={{
          background: isDark ? 'rgba(0, 0, 0, 0.6)' : 'rgba(0, 0, 0, 0.25)',
          backdropFilter: 'blur(6px)',
          WebkitBackdropFilter: 'blur(6px)',
        }}
        styles={{
          ...getModalStyles(themeMode),
          body: { ...getModalStyles(themeMode).body, padding: '20px 24px 24px' },
        }}
      >
        <Form form={form} layout="vertical">
          <Form.Item label="Source">
            <Form.Item name="channel_type" rules={[{ required: true, message: 'Select a source' }]} noStyle>
              <FeedSourceIconPicker
                isDark={isDark}
                belowPlatforms={
                  <Form.Item noStyle shouldUpdate={(prev, cur) => prev.channel_type !== cur.channel_type}>
                    {({ getFieldValue }) => {
                      const rawChannelType = getFieldValue('channel_type') || '';
                      if (
                        rawChannelType !== 'twitter' &&
                        rawChannelType !== 'youtube' &&
                        rawChannelType !== 'podcast' &&
                        rawChannelType !== 'weibo'
                      ) {
                        return null;
                      }
                      const channelType = rawChannelType;
                      const label = channelType === 'twitter'
                        ? 'Twitter username'
                        : channelType === 'podcast'
                          ? 'Podcast RSS Feed URL'
                          : channelType === 'weibo'
                            ? '微博 UID 或主页链接'
                            : 'YouTube channel';
                      const placeholder = channelType === 'twitter'
                        ? '@elonmusk or elonmusk'
                        : channelType === 'podcast'
                          ? 'https://feeds.simplecast.com/Nh1wIaXT'
                          : channelType === 'weibo'
                            ? '1234567890 或 https://weibo.com/u/1234567890'
                            : 'channel';
                      const extra = channelType === 'twitter'
                        ? 'With or without @ (e.g. @elonmusk or elonmusk)'
                        : channelType === 'podcast'
                          ? 'Paste the RSS Feed URL of the podcast (e.g. from Apple Podcasts or Spotify)'
                          : channelType === 'weibo'
                            ? '粘贴博主 UID 或主页链接（支持 weibo.com/u/xxx、m.weibo.cn/u/xxx、纯数字 UID）'
                            : 'YouTube channel handle or name';
                      const rules =
                        channelType === 'twitter'
                          ? [
                              { required: true, message: 'Required' },
                              {
                                validator: (_: unknown, value: string) => {
                                  const n = normalizeTwitterUsername(value || '');
                                  if (!n) {
                                    return Promise.reject(new Error('Enter a username'));
                                  }
                                  return Promise.resolve();
                                },
                              },
                            ]
                          : channelType === 'podcast'
                            ? [
                                { required: true, message: 'Please input RSS Feed URL' },
                                {
                                  validator: (_: unknown, value: string) => {
                                    const v = (value || '').trim();
                                    if (v && !v.startsWith('http://') && !v.startsWith('https://')) {
                                      return Promise.reject(
                                        new Error('Must be a valid URL starting with http:// or https://')
                                      );
                                    }
                                    return Promise.resolve();
                                  },
                                },
                              ]
                            : channelType === 'weibo'
                              ? [{ required: true, message: '请输入微博 UID 或主页链接' }]
                              : [{ required: true, message: 'Required' }];
                      return (
                        <>
                          {channelType === 'podcast' && (
                            <Alert
                              type="warning"
                              showIcon
                              message="Transcription takes time"
                              description="New episodes are transcribed automatically in the background. Each episode typically takes 10–20+ minutes depending on length. Transcripts will appear in the feed when ready — please be patient."
                              style={{ marginBottom: 16 }}
                            />
                          )}
                          <Form.Item
                            name="channel_identifier"
                            label={label}
                            rules={rules}
                            extra={extra}
                          >
                            <Input placeholder={placeholder} />
                          </Form.Item>
                        </>
                      );
                    }}
                  </Form.Item>
                }
              />
            </Form.Item>
          </Form.Item>
        </Form>
      </Modal>

      <Modal
        title={
          <Space>
            <EditOutlined />
            <span>Edit Subscription</span>
          </Space>
        }
        open={editModalVisible}
        onOk={handleSaveEdit}
        onCancel={() => {
          setEditModalVisible(false);
          editForm.resetFields();
        }}
        okText="Save"
        cancelText="Cancel"
        width={520}
        centered
        destroyOnClose
        maskClosable
        classNames={{
          content: isDark ? 'upload-files-modal upload-files-modal--dark' : 'upload-files-modal',
        }}
        maskStyle={{
          background: isDark ? 'rgba(0, 0, 0, 0.6)' : 'rgba(0, 0, 0, 0.25)',
          backdropFilter: 'blur(6px)',
          WebkitBackdropFilter: 'blur(6px)',
        }}
        styles={{
          ...getModalStyles(themeMode),
          body: { ...getModalStyles(themeMode).body, padding: '20px 24px 24px' },
        }}
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
