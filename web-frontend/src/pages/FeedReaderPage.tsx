/**
 * Feed Reader Page - RSS 阅读器风格的三栏布局
 * 参考 Feeder.co 的设计
 */

import React, { useState, useEffect, useMemo, useCallback, useRef } from 'react';
import {
  Layout,
  List,
  Avatar,
  Button,
  Space,
  Modal,
  Form,
  Input,
  message,
  Typography,
  Tooltip,
  Empty,
  Spin,
  Dropdown,
  Switch,
  Tag,
  Checkbox,
  Segmented,
} from 'antd';
import {
  PlusOutlined,
  RetweetOutlined,
  MessageOutlined,
  CheckOutlined,
  CommentOutlined,
  FireOutlined,
  MoreOutlined,
  EditOutlined,
  DeleteOutlined,
  EyeInvisibleOutlined,
  EyeOutlined,
  PlayCircleOutlined,
  PauseCircleOutlined,
  CheckSquareOutlined,
  CloseOutlined,
  SettingOutlined,
  ClockCircleOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { getModalStyles } from '../utils/modalStyles';
import {
  subscriptionsAPI,
  Subscription,
  CreateSubscriptionRequest,
  SubscriptionContent,
} from '../services/subscriptions';
import { FEED_THEME_COLORS as THEME_COLORS } from '../components/feed/feedTheme';
import { aiTagChipStyle } from '../components/shared/aiTagChipStyles';
import {
  getFeedListHeadline,
  getHeatLevelConfig,
  normalizeTwitterUsername,
  playNewItemsNotificationSound,
  showFeedListSecondaryExcerpt,
  feedListPrimaryLineIsTweetBody,
  subscriptionContentMatchesSearchQuery,
  estimateSubscriptionContentReadingMinutes,
} from '../components/feed/feedUtils';
import {
  getChannelIcon,
  getNewsSourceColor,
  getNewsSourceFaviconUrl,
  FEED_CHANNEL_TYPE_FILTER_TABS,
  matchesFeedChannelTypeFilter,
  type FeedChannelTypeFilterKey,
} from '../components/feed/feedChannelConfig';
import FeedArticleBody from '../components/feed/FeedArticleBody';
import {
  type FeedArticleViewMode,
  adjacentFeedArticleViewMode,
} from '../components/feed/feedArticleViewMode';
import FeedSourceIconPicker, { parsePickerValue } from '../components/feed/FeedSourceIconPicker';
import FeedSummarySettingsDrawer from '../components/feed/FeedSummarySettingsDrawer';
import SubscriptionDigestButton from '../components/feed/SubscriptionDigestButton';
import { ConfigProvider, App, theme as antdTheme } from 'antd';

dayjs.extend(relativeTime);

const { Sider, Content } = Layout;
const { Text } = Typography;

/** 桌面 Feeds 右侧详情正文最大宽度（仅 FeedReaderPage）；Daily 预览抽屉独立宽度，不受此项影响 */
const FEED_READER_ARTICLE_MAX_WIDTH_PX = 800;

const FeedReaderPageInner: React.FC = () => {
  const { modal } = App.useApp();
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = THEME_COLORS[themeMode];

  const kbdKeyStyle = useMemo(
    () =>
      ({
        display: 'inline-block' as const,
        padding: '0 5px',
        fontSize: 10,
        lineHeight: '16px',
        borderRadius: 4,
        border: `1px solid ${themeColors.cardBorder}`,
        backgroundColor: isDark ? 'rgba(255,255,255,0.06)' : 'rgba(0,0,0,0.04)',
        fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
        color: themeColors.textSecondary,
      }) satisfies React.CSSProperties,
    [themeColors.cardBorder, themeColors.textSecondary, isDark]
  );

  // 状态管理
  const [subscriptions, setSubscriptions] = useState<Subscription[]>([]);
  const [selectedSubscription, setSelectedSubscription] = useState<Subscription | null>(null);
  const [contents, setContents] = useState<SubscriptionContent[]>([]);
  const [selectedContent, setSelectedContent] = useState<SubscriptionContent | null>(null);
  // 视图模式: 跨内容切换保持用户选择（YouTube 含 Transcript Tab）
  const [feedViewMode, setFeedViewMode] = useState<FeedArticleViewMode>('original');
  
  const [loading, setLoading] = useState(false);
  const [contentsLoading, setContentsLoading] = useState(false);
  const [addModalVisible, setAddModalVisible] = useState(false);
  const [isSubmittingSubscription, setIsSubmittingSubscription] = useState(false);

  // PR-N3: 个性化摘要设置 Drawer
  const [summarySettingsOpen, setSummarySettingsOpen] = useState(false);
  
  // 🔥 分页状态
  const [contentOffset, setContentOffset] = useState(0);
  const [contentTotal, setContentTotal] = useState(0);
  const [hasMoreContents, setHasMoreContents] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  
  // 🎨 增强功能：编辑订阅
  const [editModalVisible, setEditModalVisible] = useState(false);
  const [editingSubscription, setEditingSubscription] = useState<Subscription | null>(null);
  
  // 🔍 过滤器：Active / Disabled
  const [subscriptionFilter, setSubscriptionFilter] = useState<'active' | 'disabled'>('active');
  const [feedChannelTypeFilter, setFeedChannelTypeFilter] = useState<FeedChannelTypeFilterKey>('all');
  const [contentSearchQuery, setContentSearchQuery] = useState('');
  const [selectedShowFilter, setSelectedShowFilter] = useState<string | null>(null);
  const showFilterStripRef = useRef<HTMLDivElement>(null);

  // ☑️ Batch mode
  const [batchMode, setBatchMode] = useState(false);
  const [batchSelectedIds, setBatchSelectedIds] = useState<Set<number>>(new Set());
  const [batchLoading, setBatchLoading] = useState(false);

  const statusFilteredSubscriptions = useMemo(
    () =>
      subscriptions.filter((sub) =>
        subscriptionFilter === 'active' ? sub.enabled : !sub.enabled
      ),
    [subscriptions, subscriptionFilter]
  );

  const visibleSubscriptions = useMemo(
    () =>
      statusFilteredSubscriptions.filter((sub) =>
        matchesFeedChannelTypeFilter(sub.channel_type, feedChannelTypeFilter)
      ),
    [statusFilteredSubscriptions, feedChannelTypeFilter]
  );

  // Reset show filter when switching subscriptions
  const selectedSubscriptionId = selectedSubscription?.id;
  useEffect(() => {
    setSelectedShowFilter(null);
    setContentSearchQuery('');
  }, [selectedSubscriptionId]);

  // Distinct show names present in the current content list (YouTube only)
  const availableShows = useMemo(() => {
    const names = new Set<string>();
    for (const c of contents) {
      if (c.channel_type === 'youtube' && c.metadata?.show_name) {
        names.add(c.metadata.show_name as string);
      }
    }
    return Array.from(names).sort();
  }, [contents]);

  /** 节目 chip 横条：鼠标滚轮主要是 deltaY，映射为横向滚动；触控板横向手势保留 deltaX 默认行为；须 native listener + passive:false 才能 preventDefault */
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

  const showTranscriptTabInFeeds = selectedContent?.channel_type === 'youtube';

  const feedSegmentedOptions = useMemo(() => {
    const opts: { label: string; value: FeedArticleViewMode }[] = [
      { label: 'Original', value: 'original' },
      { label: 'AI Analysis', value: 'analysis' },
    ];
    if (showTranscriptTabInFeeds) {
      opts.push({ label: 'Transcript', value: 'transcript' });
    }
    return opts;
  }, [showTranscriptTabInFeeds]);

  useEffect(() => {
    if (!showTranscriptTabInFeeds && feedViewMode === 'transcript') {
      setFeedViewMode('original');
    }
  }, [selectedContent?.id, showTranscriptTabInFeeds, feedViewMode]);

  /** 切换 Active/Disabled 或频道类型时，若当前选中不在列表内则改选第一项 */
  useEffect(() => {
    if (visibleSubscriptions.length === 0) {
      setSelectedSubscription(null);
      return;
    }
    setSelectedSubscription((prev) => {
      if (prev && visibleSubscriptions.some((s) => s.id === prev.id)) return prev;
      return visibleSubscriptions[0];
    });
  }, [visibleSubscriptions]);

  const [form] = Form.useForm();
  const [editForm] = Form.useForm();

  // 加载订阅列表
  useEffect(() => {
    loadSubscriptions();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // 🔥 实时更新机制：定期轮询订阅列表，检查未读计数变化
  useEffect(() => {
    // 每3分钟轮询一次
    const POLL_INTERVAL = 3 * 60 * 1000; // 3 minutes
    
    const intervalId = setInterval(async () => {
      try {
        // 静默刷新订阅列表（不显示 loading）
        const response = await subscriptionsAPI.getUserSubscriptions(undefined, true);
        if (response.success && response.data) {
          const oldSubscriptions = subscriptions;
          const newSubscriptions = response.data;
          
          // 检查是否有未读计数变化
          const hasUnreadChanges = newSubscriptions.some((newSub) => {
            const oldSub = oldSubscriptions.find((s) => s.id === newSub.id);
            return oldSub && oldSub.unread_count !== newSub.unread_count;
          });
          
          // 计算新增未读数
          const totalOldUnread = oldSubscriptions.reduce((sum, s) => sum + (s.unread_count || 0), 0);
          const totalNewUnread = newSubscriptions.reduce((sum, s) => sum + (s.unread_count || 0), 0);
          const newUnreadCount = totalNewUnread - totalOldUnread;
          
          // 更新订阅列表
          setSubscriptions(newSubscriptions);
          
          // 如果有新内容，显示提示并播放提示音
          if (newUnreadCount > 0) {
            playNewItemsNotificationSound();
            message.info({
              content: `🔔 ${newUnreadCount} new message(s)${newUnreadCount > 1 ? 's' : ''} available`,
              duration: 5,
            });
          }
          
          // 如果当前选中的订阅有未读计数变化，刷新内容列表
          if (selectedSubscription && hasUnreadChanges) {
            const updatedSelectedSub = newSubscriptions.find((s) => s.id === selectedSubscription.id);
            if (updatedSelectedSub) {
              setSelectedSubscription(updatedSelectedSub);
            }
          }
        }
      } catch (error) {
        console.error('Failed to poll subscriptions:', error);
        // 静默失败，不打扰用户
      }
    }, POLL_INTERVAL);
    
    // 清理定时器
    return () => clearInterval(intervalId);
  }, [subscriptions, selectedSubscription]);

  // 加载选中订阅的内容（仅依赖 subscription id，避免 markAsRead 更新 unread_count 时触发重新加载造成循环）

  useEffect(() => {
    if (selectedSubscriptionId != null) {
      loadContents(selectedSubscriptionId);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedSubscriptionId]);

  const loadSubscriptions = async () => {
    setLoading(true);
    try {
      // 🔥 不传 enabled 参数，获取所有订阅（包括禁用的）
      const response = await subscriptionsAPI.getUserSubscriptions(undefined, undefined);
      if (response.success && response.data) {
        setSubscriptions(response.data);
        // 如果没有选中的订阅，默认选中第一个启用的订阅
        if (!selectedSubscription && response.data.length > 0) {
          const firstEnabled = response.data.find(s => s.enabled) || response.data[0];
          setSelectedSubscription(firstEnabled);
        }
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

  const loadContents = async (subscriptionId: number, onlyUnread: boolean = false) => {
    setContentsLoading(true);
    setSelectedContent(null);
    setContentOffset(0); // 🔥 重置分页
    
    try {
      const response = await subscriptionsAPI.getSubscriptionContents(
        subscriptionId,
        20, // 🔥 减少初始加载数量
        0,
        onlyUnread
      );
      if (response.success && response.data) {
        setContents(response.data.contents);
        setContentTotal(response.data.total);
        setContentOffset(20);
        setHasMoreContents(response.data.contents.length < response.data.total);
        
        // 默认选中第一条内容
        if (response.data.contents.length > 0) {
          setSelectedContent(response.data.contents[0]);
          // 标记为已读
          markAsRead(response.data.contents[0].id);
        }
      } else {
        message.error(response.error || 'Failed to load contents');
        setContents([]);
      }
    } catch (error) {
      console.error('Failed to load contents:', error);
      message.error('Failed to load contents');
      setContents([]);
    } finally {
      setContentsLoading(false);
    }
  };

  // 🔥 加载更多内容
  const loadMoreContents = async () => {
    if (!selectedSubscription || loadingMore || !hasMoreContents) return;
    
    setLoadingMore(true);
    try {
      const response = await subscriptionsAPI.getSubscriptionContents(
        selectedSubscription.id,
        20,
        contentOffset,
        false
      );
      if (response.success && response.data) {
        const newContents = response.data.contents;
        const newTotal = response.data.total;
        
        setContents(prev => [...prev, ...newContents]);
        setContentOffset(prev => prev + newContents.length);
        setHasMoreContents(contentOffset + newContents.length < newTotal);
      }
    } catch (error) {
      console.error('Failed to load more contents:', error);
      message.error('Failed to load more');
    } finally {
      setLoadingMore(false);
    }
  };

  const markAsRead = async (contentId: number) => {
    try {
      await subscriptionsAPI.markContentAsRead(contentId, true);
      // 更新本地状态：内容列表中的 is_read
      setContents(prev =>
        prev.map(c => (c.id === contentId ? { ...c, is_read: true } : c))
      );
      // 同步更新侧栏该订阅的未读数量，避免 author 栏 unread 不刷新
      if (selectedSubscription?.id != null) {
        setSubscriptions(prev =>
          prev.map(s =>
            s.id === selectedSubscription.id
              ? { ...s, unread_count: Math.max(0, (s.unread_count ?? 0) - 1) }
              : s
          )
        );
        setSelectedSubscription(prev =>
          prev ? { ...prev, unread_count: Math.max(0, (prev.unread_count ?? 0) - 1) } : null
        );
      }
    } catch (error) {
      console.error('Failed to mark as read:', error);
    }
  };

  const handleAddSubscription = () => {
    form.resetFields();
    form.setFieldsValue({ enabled: true });
    setAddModalVisible(true);
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
        
        // 隐藏 Modal
        setAddModalVisible(false);
        form.resetFields();
        
        // 显示成功提示
        message.success(
          `Subscription "@${channelIdentifier}" created successfully`
        );
        
        // 重新加载订阅列表
        await loadSubscriptions();
        
        // 自动选中新创建的订阅
        setSelectedSubscription(newSubscription);
        
        // 🎯 优化：智能轮询内容，直到内容准备好
        if (newSubscription.channel_type === 'twitter') {
          // 显示智能提示
          const loadingMessage = message.loading({
            content: (
              <div>
                <div>Subscription created!</div>
                <div style={{ fontSize: '12px', marginTop: '4px', opacity: 0.8 }}>
                  Loading content...
                </div>
              </div>
            ),
            duration: 0, // 不自动关闭
          });
          
          // 智能轮询：检查内容是否准备好
          let pollCount = 0;
          const maxPolls = 12; // 最多轮询12次（60秒）
          const pollInterval = 5000; // 每5秒检查一次
          
          const pollForContent = async () => {
            try {
              pollCount++;
              
              // 尝试加载内容
              const response = await subscriptionsAPI.getSubscriptionContents(
                newSubscription.id,
                20,
                0,
                false
              );
              
              if (response.success && response.data && response.data.contents.length > 0) {
                // ✅ 内容已准备好
                loadingMessage();
                message.success({
                  content: `✅ Loaded ${response.data.contents.length} tweets!`,
                  duration: 3,
                });
                
                // 加载内容到界面
                await loadContents(newSubscription.id);
                // 刷新订阅列表以更新 unread_count（Celery 拉取完成后后端已更新未读数）
                await loadSubscriptions();
                return true; // 停止轮询
              } else if (pollCount >= maxPolls) {
                // ⏰ 超时，停止轮询
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
                return true; // 停止轮询
              } else {
                // 🔄 继续轮询
                console.log(`[Poll ${pollCount}/${maxPolls}] Waiting for content...`);
                setTimeout(pollForContent, pollInterval);
                return false;
              }
            } catch (error) {
              console.error('Poll error:', error);
              
              if (pollCount >= maxPolls) {
                loadingMessage();
                message.error('Failed to load content. Please refresh manually.');
                return true;
              } else {
                // 出错后继续重试
                setTimeout(pollForContent, pollInterval);
                return false;
              }
            }
          };
          
          // 第一次检查在2秒后（可能是复用场景，内容立即可用）
          setTimeout(pollForContent, 2000);
        } else {
          // 其他渠道暂不支持自动获取
          message.info({
            content: `Auto-fetch for ${newSubscription.channel_type} is coming soon`,
            duration: 3,
          });
        }
      } else {
        message.error(response.error || 'Failed to create subscription');
      }
    } catch (error: any) {
      if (error?.errorFields) {
        // Ant Design form validation error — no user message needed, fields are highlighted
      } else {
        message.error('Unexpected error, please try again');
        console.error('handleCreateSubscription error:', error);
      }
    } finally {
      setIsSubmittingSubscription(false);
    }
  };

  const handleSelectSubscription = (subscription: Subscription) => {
    setSelectedSubscription(subscription);
  };

  const handleSelectContent = (content: SubscriptionContent) => {
    setSelectedContent(content);
    if (!content.is_read) {
      markAsRead(content.id);
    }
  };

  const searchActive = Boolean(contentSearchQuery.trim());
  useEffect(() => {
    if (!searchActive) return;
    if (!selectedContent) return;
    if (filteredContents.some((c) => c.id === selectedContent.id)) return;
    if (filteredContents.length > 0) {
      const next = filteredContents[0];
      setSelectedContent(next);
      if (!next.is_read) {
        void markAsRead(next.id);
      }
    } else {
      setSelectedContent(null);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- markAsRead closes over subscription; sync only on filter inputs
  }, [searchActive, filteredContents, selectedContent?.id, contents]);

  const scrollFeedListItemIntoView = useCallback((contentId: number) => {
    requestAnimationFrame(() => {
      document
        .querySelector(`[data-feed-content-id="${contentId}"]`)
        ?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    });
  }, []);

  const feedNavRef = React.useRef({
    selectedSubscription,
    contents: filteredContents,
    selectedContent,
  });
  feedNavRef.current = { selectedSubscription, contents: filteredContents, selectedContent };

  const handleSelectContentRef = React.useRef(handleSelectContent);
  handleSelectContentRef.current = handleSelectContent;

  /** 键盘: ↑↓ 在订阅文章列表中切换; ←→ 在 Original / AI Analysis / Transcript(YouTube) 间切换 */
  useEffect(() => {
    const shouldIgnoreKeyTarget = (target: EventTarget | null) => {
      if (!target || !(target instanceof HTMLElement)) return false;
      if (target.isContentEditable) return true;
      const t = target.tagName;
      if (t === 'INPUT' || t === 'TEXTAREA' || t === 'SELECT') return true;
      if (target.closest('.ant-modal') || target.closest('.ant-image-preview-wrap')) return true;
      return false;
    };

    const onKeyDown = (e: KeyboardEvent) => {
      if (e.defaultPrevented) return;
      const target = e.target;
      if (!(target instanceof HTMLElement)) return;
      if (shouldIgnoreKeyTarget(target)) return;
      if (!['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight'].includes(e.key)) return;

      const { selectedSubscription: sub, contents: list, selectedContent: sel } =
        feedNavRef.current;

      // ← / → : 切换详情区视图模式（焦点在 Segmented 内时交给组件自身）
      if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
        if (!sel) return;
        if (target.closest('.ant-segmented')) return;
        e.preventDefault();
        const showTr = sel.channel_type === 'youtube';
        setFeedViewMode((prev) =>
          adjacentFeedArticleViewMode(prev, e.key === 'ArrowRight' ? 1 : -1, true, showTr)
        );
        return;
      }

      // ↑ / ↓ : 在当前订阅的内容列表中移动（需已选订阅且有内容）
      if (!sub || list.length === 0) return;

      // 焦点在左侧订阅列表时不劫持上下键（避免与将来可聚焦订阅项冲突）
      if (target.closest('.feed-page-layout__sider-my-feeds')) return;

      const curIdx = sel ? list.findIndex((c) => c.id === sel.id) : -1;

      let nextIdx = curIdx;
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        if (curIdx === -1) {
          nextIdx = 0;
        } else if (curIdx < list.length - 1) {
          nextIdx = curIdx + 1;
        } else {
          return;
        }
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        if (curIdx === -1) {
          nextIdx = list.length - 1;
        } else if (curIdx > 0) {
          nextIdx = curIdx - 1;
        } else {
          return;
        }
      } else {
        return;
      }

      const next = list[nextIdx];
      if (next) {
        handleSelectContentRef.current(next);
        scrollFeedListItemIntoView(next.id);
      }
    };

    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [scrollFeedListItemIntoView]);

  // 🎨 增强功能：编辑订阅
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
        loadSubscriptions();
        
        // 如果编辑的是当前选中的订阅，更新它
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

  // 🗑️ 删除订阅
  const handleDeleteSubscription = async (subscription: Subscription) => {
    try {
      const response = await subscriptionsAPI.deleteSubscription(subscription.id);
      if (response.success) {
        message.success('Subscription deleted successfully');
        loadSubscriptions();
        
        // 如果删除的是当前选中的订阅，清空选中状态
        if (selectedSubscription?.id === subscription.id) {
          setSelectedSubscription(null);
          setContents([]);
          setSelectedContent(null);
        }
      } else {
        message.error(response.error || 'Failed to delete subscription');
      }
    } catch (error) {
      console.error('Failed to delete subscription:', error);
      message.error('Failed to delete subscription');
    }
  };

  // 👁️ 切换启用/禁用
  const handleToggleEnabled = async (subscription: Subscription) => {
    const newEnabledState = !subscription.enabled;
    
    try {
      const response = await subscriptionsAPI.updateSubscription(subscription.id, {
        enabled: newEnabledState,
      });
      if (response.success) {
        const actionText = newEnabledState ? 'enabled' : 'disabled';
        
        // 先刷新数据
        await loadSubscriptions();
        
        // 显示成功消息，带撤销按钮
        message.success({
          content: (
            <span>
              @{subscription.channel_identifier} {actionText}.{' '}
              {!newEnabledState && (
                <Button
                  type="link"
                  size="small"
                  onClick={() => {
                    message.destroy();
                    setSubscriptionFilter('disabled');
                  }}
                  style={{ padding: '0 4px', height: 'auto' }}
                >
                  View All Disabled
                </Button>
              )}
            </span>
          ),
          duration: 4,
        });
        
        // 更新当前选中的订阅
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

  // ☑️ Batch handlers
  const handleEnterBatchMode = () => {
    setBatchMode(true);
    setBatchSelectedIds(new Set());
  };

  const handleExitBatchMode = () => {
    setBatchMode(false);
    setBatchSelectedIds(new Set());
  };

  const handleToggleBatchItem = (id: number) => {
    setBatchSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const handleBatchSelectAll = () => {
    if (batchSelectedIds.size === visibleSubscriptions.length) {
      setBatchSelectedIds(new Set());
    } else {
      setBatchSelectedIds(new Set(visibleSubscriptions.map((s) => s.id)));
    }
  };

  const handleBatchUpdateEnabled = async (enabled: boolean) => {
    if (batchSelectedIds.size === 0) {
      message.warning('No subscriptions selected');
      return;
    }
    setBatchLoading(true);
    try {
      const ids = Array.from(batchSelectedIds);
      const results = await Promise.allSettled(
        ids.map((id) => subscriptionsAPI.updateSubscription(id, { enabled }))
      );
      const failed = results.filter((r) => r.status === 'rejected').length;
      const succeeded = ids.length - failed;
      const actionText = enabled ? 'activated' : 'disabled';
      if (failed === 0) {
        message.success(`${succeeded} subscription(s) ${actionText}`);
      } else {
        message.warning(`${succeeded} ${actionText}, ${failed} failed`);
      }
      handleExitBatchMode();
      await loadSubscriptions();
    } catch (error) {
      message.error('Failed to update subscriptions');
    } finally {
      setBatchLoading(false);
    }
  };

  return (
    <Layout
      className="feed-page-layout"
      style={{
        height: 'calc(100vh - 72px)',
        minHeight: 0,
        backgroundColor: themeColors.pageBg,
      }}
    >
      <Layout style={{ minHeight: 0, flex: 1 }}>
        {/* 左侧订阅列表 */}
        <Sider
          width={280}
          className="feed-page-layout__sider feed-page-layout__sider-my-feeds"
          style={{
            backgroundColor: themeColors.cardBg,
            borderRight: `1px solid ${themeColors.cardBorder}`,
            display: 'flex',
            flexDirection: 'column',
          }}
        >
          <div
            className="feed-page-layout__sider-head"
            style={{ padding: '16px 12px', borderBottom: `1px solid ${themeColors.cardBorder}` }}
          >
            {/* 第一行：标题 + Active / Disable / Batch */}
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
              <Text strong style={{ color: themeColors.textSecondary, fontSize: 12 }}>
                MY FEEDS ({visibleSubscriptions.length})
              </Text>
              <div
                className="feed-list-status-tabs"
                role="group"
                aria-label="Subscription status"
              >
                <Tooltip title={`Active (${subscriptions.filter((s) => s.enabled).length})`}>
                  <button
                    type="button"
                    className={`feed-list-type-tab feed-list-status-tab ${subscriptionFilter === 'active' ? 'is-active' : ''}`}
                    onClick={() => setSubscriptionFilter('active')}
                    aria-label="Active subscriptions"
                    aria-pressed={subscriptionFilter === 'active'}
                  >
                    <span className="feed-list-type-tab-icon"><PlayCircleOutlined /></span>
                  </button>
                </Tooltip>
                <Tooltip title={`Disabled (${subscriptions.filter((s) => !s.enabled).length})`}>
                  <button
                    type="button"
                    className={`feed-list-type-tab feed-list-status-tab ${subscriptionFilter === 'disabled' ? 'is-active' : ''}`}
                    onClick={() => setSubscriptionFilter('disabled')}
                    aria-label="Disabled subscriptions"
                    aria-pressed={subscriptionFilter === 'disabled'}
                  >
                    <span className="feed-list-type-tab-icon"><PauseCircleOutlined /></span>
                  </button>
                </Tooltip>
                <Tooltip title={batchMode ? 'Exit batch mode' : 'Batch select'}>
                  <button
                    type="button"
                    className={`feed-list-type-tab feed-list-status-tab ${batchMode ? 'is-active' : ''}`}
                    onClick={batchMode ? handleExitBatchMode : handleEnterBatchMode}
                    aria-label="Batch select"
                    aria-pressed={batchMode}
                  >
                    <span className="feed-list-type-tab-icon"><CheckSquareOutlined /></span>
                  </button>
                </Tooltip>
              </div>
            </div>

            {/* 第二行：频道类型 tabs + Add Feed 按钮 */}
            <div className="feed-list-filter-row">
              <div className="feed-list-type-tabs">
                {FEED_CHANNEL_TYPE_FILTER_TABS.map(({ key, label, icon }) => (
                  <Tooltip key={key} title={label}>
                    <button
                      type="button"
                      className={`feed-list-type-tab ${feedChannelTypeFilter === key ? 'is-active' : ''}`}
                      onClick={() => setFeedChannelTypeFilter(key)}
                      aria-label={label}
                    >
                      <span className="feed-list-type-tab-icon">{icon}</span>
                    </button>
                  </Tooltip>
                ))}
              </div>
              <Tooltip title="Add Feed">
                <button
                  type="button"
                  className="feed-list-type-tab"
                  onClick={handleAddSubscription}
                  aria-label="Add Feed"
                  style={{ color: '#2196F3' }}
                >
                  <span className="feed-list-type-tab-icon"><PlusOutlined /></span>
                </button>
              </Tooltip>
              <Tooltip title="Personalized summary settings">
                <button
                  type="button"
                  className="feed-list-type-tab"
                  onClick={() => setSummarySettingsOpen(true)}
                  aria-label="Personalized summary settings"
                  style={{ color: themeColors.textSecondary }}
                >
                  <span className="feed-list-type-tab-icon"><SettingOutlined /></span>
                </button>
              </Tooltip>
            </div>
          </div>

          <div className="feed-page-layout__sider-scroll">
          {loading ? (
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                flex: 1,
                minHeight: 200,
              }}
            >
              <Spin />
            </div>
          ) : subscriptions.length === 0 ? (
            <div
              style={{
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                flex: 1,
                minHeight: 200,
              }}
            >
              <Empty description="No subscriptions yet" />
            </div>
          ) : visibleSubscriptions.length === 0 ? (
              <div
                style={{
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  flex: 1,
                  minHeight: 200,
                }}
              >
                <Empty
                  description={
                    feedChannelTypeFilter !== 'all'
                      ? `No ${FEED_CHANNEL_TYPE_FILTER_TABS.find((t) => t.key === feedChannelTypeFilter)?.label ?? 'matching'} feeds`
                      : subscriptionFilter === 'active'
                        ? 'No active subscriptions'
                        : 'No disabled subscriptions'
                  }
                />
              </div>
            ) : (
              <List
                dataSource={visibleSubscriptions}
                renderItem={(subscription) => {
                const displayName =
                  subscription.channel_name || `@${subscription.channel_identifier}`;
                const newsFavicon =
                  subscription.channel_type === 'news'
                    ? getNewsSourceFaviconUrl(subscription.channel_identifier)
                    : undefined;
                const unreadCount = subscription.unread_count ?? 0;
                const hasUnread = unreadCount > 0;
                const isChecked = batchSelectedIds.has(subscription.id);
                const isSelected = selectedSubscription?.id === subscription.id;
                return (
                <List.Item
                  key={subscription.id}
                  style={{
                    padding: '12px 16px',
                    cursor: 'pointer',
                    backgroundColor:
                      batchMode
                        ? isChecked
                          ? isDark ? 'rgba(33,150,243,0.15)' : 'rgba(33,150,243,0.08)'
                          : 'transparent'
                        : isSelected
                          ? themeColors.selectedItemBg
                          : 'transparent',
                    borderLeft: batchMode
                      ? isChecked
                        ? '3px solid #2196F3'
                        : '3px solid transparent'
                      : isSelected
                        ? `3px solid ${themeColors.selectedBorder}`
                        : '3px solid transparent',
                    position: 'relative',
                    paddingLeft: batchMode ? 12 : 16,
                    paddingRight: batchMode ? 12 : hasUnread ? 56 : 44,
                    opacity: subscription.enabled ? 1 : 0.5,
                    filter: subscription.enabled ? 'none' : 'grayscale(30%)',
                  }}
                  onClick={() => {
                    if (batchMode) {
                      handleToggleBatchItem(subscription.id);
                    } else {
                      handleSelectSubscription(subscription);
                    }
                  }}
                  onMouseEnter={(e) => {
                    if (!batchMode && !isSelected) {
                      e.currentTarget.style.backgroundColor = themeColors.hoverBg;
                    }
                    if (!subscription.enabled) {
                      e.currentTarget.style.opacity = '0.7';
                    }
                  }}
                  onMouseLeave={(e) => {
                    if (!batchMode && !isSelected) {
                      e.currentTarget.style.backgroundColor = 'transparent';
                    }
                    if (!subscription.enabled) {
                      e.currentTarget.style.opacity = '0.5';
                    }
                  }}
                >
                  {/* ☑️ Batch mode checkbox */}
                  {batchMode && (
                    <Checkbox
                      checked={isChecked}
                      onChange={() => handleToggleBatchItem(subscription.id)}
                      onClick={(e) => e.stopPropagation()}
                      style={{ marginRight: 10, flexShrink: 0 }}
                    />
                  )}

                  {/* 未读数量：右上角、10px 字体，不遮挡 More 按钮 */}
                  {!batchMode && hasUnread && (
                    <span
                      style={{
                        position: 'absolute',
                        top: '50%',
                        transform: 'translateY(-50%)',
                        right: 40,
                        fontSize: 10,
                        lineHeight: 1,
                        color: themeColors.textSecondary,
                        backgroundColor: themeColors.boxMuted,
                        padding: '2px 6px',
                        borderRadius: 6,
                        minWidth: 16,
                        textAlign: 'center',
                      }}
                    >
                      {unreadCount > 99 ? '99+' : unreadCount}
                    </span>
                  )}
                  <div style={{ flex: 1, minWidth: 0, overflow: 'hidden' }}>
                    <List.Item.Meta
                      avatar={
                        <Avatar
                          size={24}
                          src={newsFavicon}
                          icon={
                            subscription.channel_type === 'news' && newsFavicon
                              ? undefined
                              : getChannelIcon(subscription.channel_type)
                          }
                          style={{
                            backgroundColor:
                              subscription.channel_type === 'twitter'
                                ? '#1DA1F2'
                                : subscription.channel_type === 'youtube'
                                  ? '#FF0000'
                                  : subscription.channel_type === 'news'
                                    ? newsFavicon
                                      ? '#fff'
                                      : getNewsSourceColor(subscription.channel_identifier)
                                    : '#ccc',
                          }}
                        />
                      }
                      title={
                        <div
                          style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: 6,
                            minWidth: 0,
                            width: '100%',
                          }}
                        >
                          <Text
                            strong
                            ellipsis={{ tooltip: displayName }}
                            style={{
                              flex: 1,
                              minWidth: 0,
                              color: themeColors.text,
                              fontSize: 12,
                              lineHeight: '20px',
                            }}
                          >
                            {displayName}
                          </Text>
                        </div>
                      }
                      description={undefined}
                    />
                  </div>
                  
                  {/* 🎨 操作菜单按钮（仅非 batch 模式显示）*/}
                  {!batchMode && (
                    <Dropdown
                      menu={{
                        items: [
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
                          {
                            type: 'divider',
                          },
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
                        ],
                      }}
                      trigger={['click']}
                      placement="bottomRight"
                    >
                      <Button
                        type="text"
                        size="small"
                        icon={<MoreOutlined />}
                        onClick={(e) => e.stopPropagation()}
                        style={{
                          position: 'absolute',
                          right: 8,
                          top: '50%',
                          transform: 'translateY(-50%)',
                          color: themeColors.textSecondary,
                        }}
                      />
                    </Dropdown>
                  )}
                </List.Item>
                );
                }}
            />
          )}
          </div>

          {/* ☑️ Batch action toolbar */}
          {batchMode && (
            <div
              style={{
                borderTop: `1px solid ${themeColors.cardBorder}`,
                backgroundColor: isDark ? 'rgba(33,150,243,0.08)' : 'rgba(33,150,243,0.05)',
                padding: '10px 12px',
                flexShrink: 0,
              }}
            >
              {/* Select-all row */}
              <div style={{ display: 'flex', alignItems: 'center', marginBottom: 8 }}>
                <Checkbox
                  indeterminate={
                    batchSelectedIds.size > 0 &&
                    batchSelectedIds.size < visibleSubscriptions.length
                  }
                  checked={
                    visibleSubscriptions.length > 0 &&
                    batchSelectedIds.size === visibleSubscriptions.length
                  }
                  onChange={handleBatchSelectAll}
                  style={{ marginRight: 8 }}
                />
                <Text style={{ fontSize: 12, color: themeColors.textSecondary }}>
                  {batchSelectedIds.size > 0
                    ? `${batchSelectedIds.size} / ${visibleSubscriptions.length} selected`
                    : 'Select all'}
                </Text>
              </div>
              {/* Action buttons */}
              <Space size={6} style={{ width: '100%' }}>
                <Button
                  size="small"
                  type="primary"
                  icon={<PlayCircleOutlined />}
                  loading={batchLoading}
                  disabled={batchSelectedIds.size === 0}
                  onClick={() => handleBatchUpdateEnabled(true)}
                  style={{ fontSize: 11, height: 26 }}
                >
                  Activate
                </Button>
                <Button
                  size="small"
                  icon={<PauseCircleOutlined />}
                  loading={batchLoading}
                  disabled={batchSelectedIds.size === 0}
                  onClick={() => handleBatchUpdateEnabled(false)}
                  style={{ fontSize: 11, height: 26, color: themeColors.textSecondary }}
                >
                  Disable
                </Button>
                <Button
                  size="small"
                  type="text"
                  icon={<CloseOutlined />}
                  onClick={handleExitBatchMode}
                  style={{ fontSize: 11, height: 26, color: themeColors.textSecondary, marginLeft: 'auto' }}
                >
                  Cancel
                </Button>
              </Space>
            </div>
          )}
        </Sider>

        {/* 中间内容列表 */}
        <Sider
          width={380}
          className="feed-page-layout__sider feed-page-layout__content-list"
          style={{
            backgroundColor: themeColors.cardBg,
            borderRight: `1px solid ${themeColors.cardBorder}`,
            overflowY: 'auto',
          }}
        >
          {selectedSubscription ? (
            <>
              <div
                style={{
                  padding: '16px',
                  borderBottom: `1px solid ${themeColors.cardBorder}`,
                }}
              >
                <Space direction="vertical" size={10} style={{ width: '100%' }}>
                  <div
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      justifyContent: 'space-between',
                      gap: 12,
                      width: '100%',
                      minWidth: 0,
                    }}
                  >
                    <Text
                      strong
                      style={{
                        color: themeColors.text,
                        fontSize: 16,
                        flex: 1,
                        minWidth: 0,
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        whiteSpace: 'nowrap',
                      }}
                    >
                      {selectedSubscription.channel_name ||
                        selectedSubscription.channel_identifier}
                    </Text>
                    <Text type="secondary" style={{ fontSize: 12, flexShrink: 0 }}>
                      {searchActive ? `${filteredContents.length} / ` : ''}
                      {contents.length} articles
                    </Text>
                  </div>
                  <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
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
                      subscriptionId={selectedSubscription?.id}
                      channelName={
                        selectedSubscription?.channel_name ||
                        selectedSubscription?.channel_identifier
                      }
                      themeMode={themeMode}
                    />
                  </div>
                </Space>
              </div>

              {/* 节目筛选 chip 行：仅当前订阅有 show_name 时渲染 */}
              {availableShows.length > 0 && (
                <div
                  ref={showFilterStripRef}
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    gap: 6,
                    padding: '8px 16px',
                    overflowX: 'auto',
                    borderBottom: `1px solid ${themeColors.cardBorder}`,
                    scrollbarWidth: 'none',
                    msOverflowStyle: 'none',
                  } as React.CSSProperties}
                >
                  {(['All', ...availableShows] as const).map((show) => {
                    const isActive = show === 'All' ? selectedShowFilter === null : selectedShowFilter === show;
                    return (
                      <button
                        key={show}
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
                            : isDark ? 'rgba(255,255,255,0.07)' : 'rgba(0,0,0,0.05)',
                          color: isActive
                            ? '#fff'
                            : themeColors.textSecondary,
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

              {contentsLoading ? (
                <div style={{ 
                  display: 'flex', 
                  alignItems: 'center', 
                  justifyContent: 'center',
                  height: 'calc(100vh - 300px)'
                }}>
                  <Spin />
                </div>
              ) : contents.length === 0 ? (
                <div style={{ 
                  display: 'flex', 
                  alignItems: 'center', 
                  justifyContent: 'center',
                  height: 'calc(100vh - 300px)'
                }}>
                  <Empty
                    description={
                      (() => {
                        // 🎯 智能空状态提示
                        if (!selectedSubscription.last_fetched_at) {
                          const createdTime = dayjs(selectedSubscription.created_at);
                          const minutesAgo = dayjs().diff(createdTime, 'minute');
                          
                          if (minutesAgo < 2) {
                            return (
                              <div>
                                <div style={{ marginBottom: 8 }}>🔄 Fetching initial content...</div>
                                <div style={{ fontSize: 12, opacity: 0.7 }}>
                                  This may take 30-60 seconds for the first time.
                                </div>
                                <div style={{ fontSize: 12, opacity: 0.7, marginTop: 4 }}>
                                  Getting last 2 days of contents from @{selectedSubscription.channel_identifier}
                                </div>
                              </div>
                            );
                          } else if (minutesAgo < 10) {
                            return (
                              <div>
                                <div style={{ marginBottom: 8 }}>⏳ Initial fetch in progress...</div>
                                <div style={{ fontSize: 12, opacity: 0.7 }}>
                                  Created {minutesAgo} minute{minutesAgo > 1 ? 's' : ''} ago. 
                                  Please wait or try refreshing.
                                </div>
                              </div>
                            );
                          } else {
                            return (
                            <div>
                              <div style={{ marginBottom: 8 }}>⚠️ No content fetched yet</div>
                              <div style={{ fontSize: 12, opacity: 0.7 }}>
                                The initial fetch may have failed or is taking longer than expected.
                              </div>
                              <div style={{ fontSize: 12, opacity: 0.7, marginTop: 4 }}>
                                Content will be fetched automatically every 30 minutes, or try refreshing the page.
                              </div>
                            </div>
                            );
                          }
                        } else {
                          return (
                            <div>
                              <div style={{ marginBottom: 8 }}>📭 No content available</div>
                              <div style={{ fontSize: 12, opacity: 0.7 }}>
                                No new content from @{selectedSubscription.channel_identifier}
                              </div>
                              <div style={{ fontSize: 12, opacity: 0.7, marginTop: 4 }}>
                                Last fetched: {dayjs(selectedSubscription.last_fetched_at).fromNow()}
                              </div>
                            </div>
                          );
                        }
                      })()
                    }
                  />
                </div>
              ) : filteredContents.length === 0 ? (
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    height: 'calc(100vh - 300px)',
                    padding: 16,
                  }}
                >
                  <Empty
                    description={
                      <span style={{ color: themeColors.textSecondary }}>
                        No articles match your search
                      </span>
                    }
                  />
                </div>
              ) : (
                <>
                  <List
                    dataSource={filteredContents}
                    renderItem={(content) => {
                      const primaryIsTweetBody = feedListPrimaryLineIsTweetBody(content);
                      const readingMin = estimateSubscriptionContentReadingMinutes(content);
                      return (
                      <List.Item
                        key={content.id}
                        data-feed-content-id={content.id}
                        onClick={() => handleSelectContent(content)}
                        style={{
                          padding: '16px',
                          cursor: 'pointer',
                          backgroundColor:
                            selectedContent?.id === content.id
                              ? themeColors.selectedItemBg
                              : 'transparent',
                          borderLeft:
                            selectedContent?.id === content.id
                              ? `3px solid ${themeColors.selectedBorder}`
                              : '3px solid transparent',
                          opacity: content.is_read ? 0.6 : 1,
                        }}
                        onMouseEnter={(e) => {
                          if (selectedContent?.id !== content.id) {
                            e.currentTarget.style.backgroundColor = themeColors.hoverBg;
                          }
                        }}
                        onMouseLeave={(e) => {
                          if (selectedContent?.id !== content.id) {
                            e.currentTarget.style.backgroundColor = 'transparent';
                          }
                        }}
                      >
                        <div
                          data-feed-content-id={content.id}
                          style={{ width: '100%', display: 'flex', flexDirection: 'column', gap: 6 }}
                        >
                          {/* title + 正文区域：严格限高，超出隐藏 */}
                          <div style={{ maxHeight: 96, overflow: 'hidden', display: 'flex', flexDirection: 'column', gap: 5 }}>
                            {/* 主行：YouTube 等为标题；Twitter 无 title 时为推文正文 */}
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 8 }}>
                              <span
                                style={{
                                  fontWeight: primaryIsTweetBody ? 400 : 600,
                                  color: themeColors.text,
                                  fontSize: primaryIsTweetBody ? 12 : 13,
                                  lineHeight: '1.45',
                                  display: '-webkit-box',
                                  WebkitLineClamp: 2,
                                  WebkitBoxOrient: 'vertical',
                                  overflow: 'hidden',
                                  wordBreak: 'break-word',
                                  flex: 1,
                                  minWidth: 0,
                                } as React.CSSProperties}
                              >
                                {getFeedListHeadline(content)}
                              </span>
                              {content.is_read && (
                                <CheckOutlined style={{ color: '#52c41a', flexShrink: 0, marginTop: 2 }} />
                              )}
                            </div>

                            {/* 正文：YouTube 描述等；Twitter 无 title 时主行已是推文，不再重复 */}
                            {showFeedListSecondaryExcerpt(content) && (
                              <Text
                                style={{
                                  color: themeColors.textSecondary,
                                  fontSize: 12,
                                  lineHeight: '1.4',
                                  display: '-webkit-box',
                                  WebkitLineClamp: 2,
                                  WebkitBoxOrient: 'vertical',
                                  overflow: 'hidden',
                                }}
                              >
                                {content.content}
                              </Text>
                            )}
                          </div>

                          {/* 末行：类型图标 + 作者名 · 时间 · 热度，始终可见，在限高区域外 */}
                          <Space size={6}>
                            {content.is_retweet && (
                              <Tooltip title="Retweet">
                                <RetweetOutlined style={{ color: '#00ba7c', fontSize: 12 }} />
                              </Tooltip>
                            )}
                            {content.is_quote && (
                              <Tooltip title="Quote Tweet">
                                <MessageOutlined style={{ color: '#1DA1F2', fontSize: 12 }} />
                              </Tooltip>
                            )}
                            {content.is_reply && (
                              <Tooltip title={`reply to @${content.in_reply_to_username || 'someone'}`}>
                                <CommentOutlined style={{ color: '#657786', fontSize: 12 }} />
                              </Tooltip>
                            )}
                            <Tooltip
                              title={
                                content.channel_type === 'youtube' &&
                                content.metadata?.show_name &&
                                content.author_name
                                  ? content.author_name
                                  : undefined
                              }
                            >
                              <Text type="secondary" style={{ fontSize: 12 }}>
                                {content.channel_type === 'youtube' &&
                                content.metadata?.show_name
                                  ? content.metadata.show_name
                                  : content.author_name ||
                                    (content.author_identifier
                                      ? `@${content.author_identifier}`
                                      : '')}
                              </Text>
                            </Tooltip>
                            <Text type="secondary" style={{ fontSize: 12 }}>·</Text>
                            <Text type="secondary" style={{ fontSize: 12 }}>
                              {content.published_at
                                ? dayjs(content.published_at).fromNow()
                                : 'Unknown'}
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
                            {content.heat_level && content.heat_level >= 1 && (
                              <>
                                {readingMin != null && (
                                  <Text type="secondary" style={{ fontSize: 12 }}>·</Text>
                                )}
                                <Tooltip title={getHeatLevelConfig(content.heat_level).text}>
                                  <FireOutlined
                                    style={{
                                      color: getHeatLevelConfig(content.heat_level).color,
                                      fontSize: 13,
                                    }}
                                  />
                                </Tooltip>
                              </>
                            )}
                          </Space>

                          {/* Phase 3: AI 标签 chips (仅有 tags 且非"未分类"时渲染, 节省视觉空间) */}
                          {content.tags
                            && content.tags.sector !== '未分类'
                            && content.tags.sector_en !== 'Uncategorized'
                            && (content.tags.tickers?.length || content.tags.sector) && (
                              <Space size={[8, 8]} wrap style={{ marginTop: 4 }}>
                                {content.tags.sector && (
                                  <Tag
                                    bordered={false}
                                    style={aiTagChipStyle('sector', themeMode)}
                                  >
                                    {content.tags.sector}
                                  </Tag>
                                )}
                                {content.tags.tickers && content.tags.tickers.length > 0 && (
                                  <>
                                    {content.tags.tickers.slice(0, 2).map((t) => (
                                      <Tag
                                        key={t}
                                        bordered={false}
                                        style={aiTagChipStyle('ticker', themeMode)}
                                      >
                                        {t}
                                      </Tag>
                                    ))}
                                    {content.tags.tickers.length > 2 && (
                                      <Tooltip title={content.tags.tickers.slice(2).join(', ')}>
                                        <Tag
                                          bordered={false}
                                          style={{
                                            ...aiTagChipStyle('overflow', themeMode),
                                            cursor: 'default',
                                          }}
                                        >
                                          +{content.tags.tickers.length - 2}
                                        </Tag>
                                      </Tooltip>
                                    )}
                                  </>
                                )}
                              </Space>
                            )}
                        </div>
                      </List.Item>
                    );
                    }}
                  />
                  
                  {/* 🔥 加载更多按钮 */}
                  {filteredContents.length > 0 && hasMoreContents && (
                    <div style={{ textAlign: 'center', padding: '16px' }}>
                      <Button
                        type="link"
                        loading={loadingMore}
                        onClick={loadMoreContents}
                        style={{ color: themeColors.text }}
                      >
                        {loadingMore ? 'Loading...' : `Load more (${contentTotal - contentOffset} remaining)`}
                      </Button>
                    </div>
                  )}
                  
                  {/* 🎯 没有更多内容提示 */}
                  {filteredContents.length > 0 && !hasMoreContents && (
                    <div style={{ textAlign: 'center', padding: '16px' }}>
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        No more content
                      </Text>
                    </div>
                  )}
                </>
              )}
            </>
          ) : (
            <div style={{ 
              display: 'flex', 
              alignItems: 'center', 
              justifyContent: 'center',
              height: 'calc(100vh - 200px)'
            }}>
              <Empty description="Select a feed to view contents" />
            </div>
          )}
        </Sider>

        {/* 右侧内容详情 */}
        <Content
          className="feed-page-layout__detail"
          style={{
            backgroundColor: themeColors.cardBg,
            overflowY: 'auto',
            padding: '0',
          }}
        >
          {selectedContent ? (
            <>
              {/* ── 视图模式 Tab 栏 — sticky, 内容区外部顶端 ── */}
              <div
                style={{
                  position: 'sticky',
                  top: 0,
                  zIndex: 20,
                  backgroundColor: themeColors.cardBg,
                  borderBottom: `1px solid ${themeColors.cardBorder}`,
                  padding: '10px 80px 12px',
                }}
              >
                <div className="feed-page-layout__view-mode-tabs">
                  <Segmented
                    value={feedViewMode}
                    onChange={(v) => setFeedViewMode(v as FeedArticleViewMode)}
                    options={feedSegmentedOptions}
                    style={{ minWidth: 220, width: '100%' }}
                    block
                  />
                </div>
                <div
                  style={{
                    display: 'flex',
                    alignItems: 'center',
                    flexWrap: 'wrap',
                    gap: 12,
                    marginTop: 8,
                  }}
                >
                  {contents.length > 0 && (
                    <div
                      style={{
                        display: 'flex',
                        alignItems: 'center',
                        flexWrap: 'wrap',
                        gap: 6,
                      }}
                    >
                      <Text type="secondary" style={{ fontSize: 11 }}>
                        Keyboard
                      </Text>
                      <kbd style={kbdKeyStyle}>↑</kbd>
                      <kbd style={kbdKeyStyle}>↓</kbd>
                      <Text type="secondary" style={{ fontSize: 11 }}>
                        prev / next article
                      </Text>
                    </div>
                  )}
                  {contents.length > 0 && (
                    <Text type="secondary" style={{ fontSize: 11 }}>
                      ·{' '}
                    </Text>
                  )}
                  <div
                    style={{
                      display: 'flex',
                      alignItems: 'center',
                      flexWrap: 'wrap',
                      gap: 6,
                    }}
                  >
                    <Text type="secondary" style={{ fontSize: 11 }}>
                      Keyboard
                    </Text>
                    <kbd style={kbdKeyStyle}>←</kbd>
                    <kbd style={kbdKeyStyle}>→</kbd>
                    <Text type="secondary" style={{ fontSize: 11 }}>
                      Switch Original / AI Analysis / Transcript
                    </Text>
                    <Text type="secondary" style={{ fontSize: 11 }}>
                      · Use Page Up and Page Down to scroll transcript and other long content.
                    </Text>
                  </div>
                </div>
              </div>

              <div style={{ maxWidth: FEED_READER_ARTICLE_MAX_WIDTH_PX, margin: '0 auto', paddingLeft: 24, paddingRight: 24, paddingTop: 32, paddingBottom: 40 }}>
              <FeedArticleBody
                selectedContent={selectedContent}
                themeColors={themeColors}
                isDark={isDark}
                themeMode={themeMode}
                viewMode={feedViewMode}
                onViewModeChange={setFeedViewMode}
                onTranscriptReady={(contentId) => {
                  setSelectedContent((prev) =>
                    prev && prev.id === contentId ? { ...prev, has_transcript: true } : prev
                  );
                  setContents((prev) =>
                    prev.map((c) => (c.id === contentId ? { ...c, has_transcript: true } : c))
                  );
                }}
                onTagsUpdated={(contentId, tags) => {
                  setSelectedContent((prev) =>
                    prev && prev.id === contentId ? { ...prev, tags } : prev
                  );
                  setContents((prev) =>
                    prev.map((c) => (c.id === contentId ? { ...c, tags } : c))
                  );
                }}
              />
              </div>
            </>
          ) : (
            <div style={{ 
              display: 'flex', 
              alignItems: 'center', 
              justifyContent: 'center',
              height: '100%',
              padding: '32px 80px',
            }}>
              <Empty description="Select an article to read" />
            </div>
          )}
        </Content>
      </Layout>

      {/* 添加订阅Modal — 与 Upload / Layout 等弹窗同一套 mask、居中、classNames */}
      <Modal
        title={
          <Space>
            <PlusOutlined />
            <span>Add New Feed</span>
          </Space>
        }
        open={addModalVisible}
        onOk={handleCreateSubscription}
        onCancel={() => { if (!isSubmittingSubscription) setAddModalVisible(false); }}
        okText="Add"
        cancelText="Cancel"
        okButtonProps={{ loading: isSubmittingSubscription }}
        cancelButtonProps={{ disabled: isSubmittingSubscription }}
        width={520}
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
          <Form.Item label="Feed Source">
            <Form.Item
              name="channel_type"
              rules={[{ required: true, message: 'Please select a feed source' }]}
              noStyle
            >
              <FeedSourceIconPicker
                isDark={isDark}
                belowPlatforms={
                  <Form.Item
                    noStyle
                    shouldUpdate={(prevValues, currentValues) =>
                      prevValues.channel_type !== currentValues.channel_type
                    }
                  >
                    {({ getFieldValue }) => {
                      const rawChannelType = getFieldValue('channel_type') || '';
                      if (rawChannelType !== 'twitter' && rawChannelType !== 'youtube') {
                        return null;
                      }
                      const channelType = rawChannelType;
                      return (
                        <Form.Item
                          name="channel_identifier"
                          label={
                            channelType === 'twitter' ? 'Twitter Username' : 'YouTube Channel'
                          }
                          rules={
                            channelType === 'twitter'
                              ? [
                                  { required: true, message: 'Please input identifier' },
                                  {
                                    validator: (_: unknown, value: string) => {
                                      const n = normalizeTwitterUsername(value || '');
                                      if (!n) {
                                        return Promise.reject(
                                          new Error('Please enter a username')
                                        );
                                      }
                                      return Promise.resolve();
                                    },
                                  },
                                ]
                              : [{ required: true, message: 'Please input identifier' }]
                          }
                          extra={
                            channelType === 'twitter'
                              ? 'With or without @ (e.g. @elonmusk or elonmusk)'
                              : 'Recommended: paste the @handle from youtube.com/@xxx (e.g., @markets → Bloomberg Television). You can also paste a UC… channel ID. Bare names without @ use the legacy username and may resolve to a different channel.'
                          }
                        >
                          <Input
                            placeholder={
                              channelType === 'twitter'
                                ? '@elonmusk or elonmusk'
                                : '@markets or UCIALMKvObZNtJ6AmdCLP7Lg'
                            }
                          />
                        </Form.Item>
                      );
                    }}
                  </Form.Item>
                }
              />
            </Form.Item>
          </Form.Item>
        </Form>
      </Modal>

      {/* 编辑订阅 Modal */}
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
          <Form.Item
            label="Channel"
            extra={`${editingSubscription?.channel_type}: @${editingSubscription?.channel_identifier}`}
          >
            <Text type="secondary">Channel type and identifier cannot be changed</Text>
          </Form.Item>

          <Form.Item
            name="channel_name"
            label="Display Name (Optional)"
          >
            <Input placeholder="Override display name from channel" />
          </Form.Item>

          <Form.Item
            name="channel_description"
            label="Description"
          >
            <Input.TextArea rows={2} placeholder="Optional description" />
          </Form.Item>

          <Form.Item
            name="enabled"
            label="Status"
            valuePropName="checked"
          >
            <Switch checkedChildren="Enabled" unCheckedChildren="Disabled" />
          </Form.Item>
        </Form>
      </Modal>

      {/* PR-N3: 个性化摘要设置 Drawer (右上角齿轮) */}
      <FeedSummarySettingsDrawer
        open={summarySettingsOpen}
        onClose={() => setSummarySettingsOpen(false)}
      />
    </Layout>
  );
};

// 外层：ConfigProvider + App，与 Files Page 一致，保证 Modal/Select/Input 等继承主题
const FeedReaderPage: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = THEME_COLORS[themeMode];

  return (
    <ConfigProvider
      theme={{
        algorithm: isDark ? antdTheme.darkAlgorithm : antdTheme.defaultAlgorithm,
        ...(isDark && {
          token: {
            colorBgContainer: themeColors.cardBg,
            colorBgElevated: themeColors.cardBg,
            colorBorderSecondary: themeColors.cardBorder,
            colorPrimary: '#2196F3',
            colorText: themeColors.text,
            colorTextSecondary: themeColors.textSecondary,
            colorFillQuaternary: themeColors.inputBg ?? '#1E202B',
            colorBgLayout: themeColors.pageBg,
            borderRadius: 8,
            borderRadiusLG: 12,
          },
        }),
      }}
    >
      <App>
        <FeedReaderPageInner />
      </App>
    </ConfigProvider>
  );
};

export default FeedReaderPage;
