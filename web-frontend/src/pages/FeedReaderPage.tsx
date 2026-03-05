/**
 * Feed Reader Page - RSS 阅读器风格的三栏布局
 * 参考 Feeder.co 的设计
 */

import React, { useState, useEffect, useRef, useCallback } from 'react';
import { useFileProcessing } from '../contexts/FileProcessingContext';
import {
  Layout,
  List,
  Avatar,
  Button,
  Space,
  Tag,
  Modal,
  Form,
  Input,
  Select,
  message,
  Typography,
  Tooltip,
  Empty,
  Spin,
  Card,
  Dropdown,
  Switch,
  Divider,
} from 'antd';
import {
  PlusOutlined,
  TwitterOutlined,
  YoutubeOutlined,
  GlobalOutlined,
  ReloadOutlined,
  RetweetOutlined,
  MessageOutlined,
  LinkOutlined,
  CheckOutlined,
  LikeOutlined,
  CommentOutlined,
  FireOutlined,
  MoreOutlined,
  EditOutlined,
  DeleteOutlined,
  EyeInvisibleOutlined,
  EyeOutlined,
  AlignLeftOutlined,
} from '@ant-design/icons';
import dayjs from 'dayjs';
import relativeTime from 'dayjs/plugin/relativeTime';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import {
  subscriptionsAPI,
  Subscription,
  CreateSubscriptionRequest,
  SubscriptionContent,
  getTweetById,
} from '../services/subscriptions';
import { filesAPI } from '../services/files';
import { ConfigProvider, App, theme as antdTheme } from 'antd';

dayjs.extend(relativeTime);

const { Sider, Content } = Layout;
const { Text, Paragraph } = Typography;
const { Option } = Select;

// 与 Files Page 一致：语义化色板 + Behance 风格（参考 FILES_PAGE_STYLE_REFERENCE.md）
const THEME_COLORS = {
  light: {
    pageBg: '#f5f5f5',
    cardBg: '#fff',
    cardBorder: '#f0f0f0',
    text: 'rgba(0, 0, 0, 0.88)',
    textSecondary: 'rgba(0, 0, 0, 0.45)',
    textMuted: '#999',
    codeBlockBg: '#f5f5f5',
    selectedBorder: '#1890ff',
    inputBg: undefined as string | undefined,
    successBg: '#f6ffed',
    successBorder: '#b7eb8f',
    boxMuted: '#f5f5f5',
    hoverBg: '#f5f5f5',
    selectedItemBg: '#e8f4ff',
  },
  dark: {
    pageBg: '#1E202B',
    cardBg: '#1a2234',
    cardBorder: 'rgba(255, 255, 255, 0.06)',
    text: '#FFFFFF',
    textSecondary: '#A0A3AD',
    textMuted: '#A0A3AD',
    codeBlockBg: '#1E202B',
    selectedBorder: '#2196F3',
    inputBg: '#1E202B',
    hoverBg: 'rgba(255, 255, 255, 0.06)',
    successBg: 'rgba(82, 196, 26, 0.15)',
    successBorder: 'rgba(82, 196, 26, 0.45)',
    boxMuted: '#1E202B',
    selectedItemBg: 'rgba(33, 150, 243, 0.12)',
  },
} as const;

// Behance 风格：圆角 12px、柔和阴影、细边框（dark 用于 modal）
const CARD_STYLE_BEHANCE_DARK = {
  borderRadius: 12,
  boxShadow: '0 8px 32px rgba(0, 0, 0, 0.4)',
  border: '1px solid rgba(255, 255, 255, 0.06)',
};

const getModalStyles = (isDark: boolean, colors: typeof THEME_COLORS.light | typeof THEME_COLORS.dark) => {
  if (!isDark) return {};
  const cardStyle = CARD_STYLE_BEHANCE_DARK;
  return {
    content: {
      ...cardStyle,
      backgroundColor: colors.cardBg,
    },
    header: {
      color: colors.text,
      borderBottomColor: colors.cardBorder,
    },
    body: {
      color: colors.text,
    },
  };
};

// 频道类型配置
const CHANNEL_TYPES = [
  { value: 'twitter', label: 'Twitter', icon: <TwitterOutlined />, color: '#1DA1F2' },
  { value: 'youtube', label: 'YouTube', icon: <YoutubeOutlined />, color: '#FF0000', disabled: false },
  { value: 'rss', label: 'RSS Feed', icon: <GlobalOutlined />, color: '#orange', disabled: true },
];

const FeedReaderPageInner: React.FC = () => {
  const { themeMode } = useDashboardSettings();
  const isDark = themeMode === 'dark';
  const themeColors = THEME_COLORS[themeMode];
  const tweetContainerRef = useRef<HTMLDivElement>(null);

  // 状态管理
  const [subscriptions, setSubscriptions] = useState<Subscription[]>([]);
  const [selectedSubscription, setSelectedSubscription] = useState<Subscription | null>(null);
  const [contents, setContents] = useState<SubscriptionContent[]>([]);
  const [selectedContent, setSelectedContent] = useState<SubscriptionContent | null>(null);
  const [originalTweet, setOriginalTweet] = useState<SubscriptionContent | null>(null);
  const [originalTweetLoading, setOriginalTweetLoading] = useState(false);
  
  const [loading, setLoading] = useState(false);
  const [contentsLoading, setContentsLoading] = useState(false);
  const [addModalVisible, setAddModalVisible] = useState(false);
  const [isSubmittingSubscription, setIsSubmittingSubscription] = useState(false);

  // ASR 转写状态（YouTube 无字幕视频手动触发）
  const [asrTaskId, setAsrTaskId] = useState<string | null>(null);
  const [asrProgress, setAsrProgress] = useState(0);
  const [asrStatus, setAsrStatus] = useState<'idle' | 'processing' | 'completed' | 'failed'>('idle');

  // 接入 NavBar 进度条
  const { addTask, tasks } = useFileProcessing();
  
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
          
          // 如果有新内容，显示提示
          if (newUnreadCount > 0) {
            message.info({
              content: `🔔 ${newUnreadCount} new item${newUnreadCount > 1 ? 's' : ''} available`,
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
  const selectedSubscriptionId = selectedSubscription?.id;
  useEffect(() => {
    if (selectedSubscriptionId != null) {
      loadContents(selectedSubscriptionId);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedSubscriptionId]);

  // 加载原推文（当选中 Reply 推文时）
  useEffect(() => {
    const loadOriginalTweet = async () => {
      if (selectedContent?.is_reply && selectedContent?.in_reply_to_id) {
        console.log('🔍 Loading original tweet:', {
          is_reply: selectedContent.is_reply,
          in_reply_to_id: selectedContent.in_reply_to_id,
          in_reply_to_username: selectedContent.in_reply_to_username
        });
        
        setOriginalTweetLoading(true);
        try {
          const tweet = await getTweetById(selectedContent.in_reply_to_id, 1); // TODO: 使用实际 user_id
          console.log('✅ Original tweet loaded:', tweet);
          setOriginalTweet(tweet);
        } catch (error) {
          console.error('❌ Failed to load original tweet:', error);
          setOriginalTweet(null);
        } finally {
          setOriginalTweetLoading(false);
        }
      } else {
        setOriginalTweet(null);
      }
    };
    
    loadOriginalTweet();
  }, [selectedContent]);

  // 加载 Twitter widgets.js 并渲染嵌入推文
  useEffect(() => {
    if (!originalTweetLoading && !originalTweet && selectedContent?.is_reply && selectedContent?.in_reply_to_id && tweetContainerRef.current) {
      // 清空容器
      tweetContainerRef.current.innerHTML = '';
      
      // 加载 Twitter widgets script
      const loadTwitterWidget = () => {
        if ((window as any).twttr?.widgets) {
          // 如果已经加载，直接创建推文
          (window as any).twttr.widgets.createTweet(
            selectedContent.in_reply_to_id,
            tweetContainerRef.current,
            {
              theme: themeMode === 'dark' ? 'dark' : 'light',
              conversation: 'none',
              cards: 'visible',
            }
          );
        } else {
          // 加载 Twitter widgets script
          const script = document.createElement('script');
          script.src = 'https://platform.twitter.com/widgets.js';
          script.async = true;
          script.onload = () => {
            if ((window as any).twttr?.widgets) {
              (window as any).twttr.widgets.createTweet(
                selectedContent.in_reply_to_id,
                tweetContainerRef.current,
                {
                  theme: themeMode === 'dark' ? 'dark' : 'light',
                  conversation: 'none',
                  cards: 'visible',
                }
              );
            }
          };
          document.body.appendChild(script);
        }
      };

      loadTwitterWidget();
    }
  }, [selectedContent, originalTweet, originalTweetLoading, themeMode]);

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

  // ── YouTube ASR 转写 ────────────────────────────────────────────────────────
  const handleTriggerASR = async () => {
    if (!selectedContent || asrStatus === 'processing') return;

    setAsrStatus('processing');
    setAsrProgress(5);

    const result = await subscriptionsAPI.triggerYouTubeASR(selectedContent.id);
    if (!result.success || !result.task_id) {
      message.error(result.error || 'Failed to start ASR transcription');
      setAsrStatus('failed');
      return;
    }

    const taskId = result.task_id;
    const videoTitle = selectedContent.title || `YouTube: ${selectedContent.content_id || 'video'}`;
    setAsrTaskId(taskId);

    // 注册到全局 FileProcessingContext → NavBar 进度条自动显示
    addTask(taskId, videoTitle, 'youtube');
    message.info('ASR transcription started — progress shown in the top bar');
  };

  // 监听 Context 中该任务的状态变化
  useEffect(() => {
    if (!asrTaskId || asrStatus !== 'processing') return;

    const task = tasks.get(asrTaskId);
    if (!task) return;

    const s = task.status.status;
    const resultStatus = (task.status as any).result?.status;
    const isCompleted = s === 'completed' && resultStatus !== 'failed';
    const isFailed = s === 'failed' || (s === 'completed' && resultStatus === 'failed');

    if (isCompleted) {
      setAsrStatus('completed');
      setAsrProgress(100);
      message.success('Transcription completed!');
      // Update current content's has_transcript immediately so the UI reflects the change
      if (selectedContent) {
        setSelectedContent(prev => prev ? { ...prev, has_transcript: true } : null);
        setContents(prev =>
          prev.map(c => (c.id === selectedContent.id ? { ...c, has_transcript: true } : c))
        );
      }
    } else if (isFailed) {
      setAsrStatus('failed');
      const errMsg = (task.status as any).result?.error || task.status.message || 'Transcription failed';
      message.error(`ASR failed: ${errMsg}`);
    } else {
      setAsrProgress(task.status.progress || 0);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tasks, asrTaskId]);

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
        
        // 隐藏 Modal
        setAddModalVisible(false);
        form.resetFields();
        
        // 显示成功提示
        message.success(
          `Subscription "@${values.channel_identifier}" created successfully`
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
    setAsrStatus('idle');
    setAsrProgress(0);
    setAsrTaskId(null);
    setSelectedContent(content);
    if (!content.is_read) {
      markAsRead(content.id);
    }
  };

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

  const getChannelIcon = (channelType: string) => {
    const config = CHANNEL_TYPES.find((c) => c.value === channelType);
    return config?.icon || <GlobalOutlined />;
  };

  // 获取热度等级的颜色和文本
  const getHeatLevelConfig = (heatLevel?: number) => {
    switch (heatLevel) {
      case 1:
        return { color: '#8c8c8c', text: 'Low heat', bgColor: 'rgba(140, 140, 140, 0.1)' };
      case 2:
        return { color: '#1890ff', text: 'Medium heat', bgColor: 'rgba(24, 144, 255, 0.1)' };
      case 3:
        return { color: '#fa8c16', text: 'High heat', bgColor: 'rgba(250, 140, 22, 0.1)' };
      case 4:
        return { color: '#f5222d', text: 'Very high heat', bgColor: 'rgba(245, 34, 45, 0.1)' };
      default:
        return { color: '#8c8c8c', text: 'Low heat', bgColor: 'rgba(140, 140, 140, 0.1)' };
    }
  };

  const renderTweetContent = (content: SubscriptionContent) => {
    console.log('🎨 Rendering tweet content:', {
      is_reply: content.is_reply,
      in_reply_to_username: content.in_reply_to_username,
      originalTweetLoading,
      originalTweet: originalTweet ? 'exists' : 'null'
    });
    
    return (
      <div>
        {/* Reply（回复）- 显示顺序：当前回复 → Replying to → 原推文 */}
        {content.is_reply && content.in_reply_to_username && (
          <div style={{ marginBottom: 16 }}>
            {/* 1. 当前回复内容（最上方）*/}
            <Paragraph style={{ color: themeColors.text, marginBottom: 12 }}>
              {content.content}
            </Paragraph>

            {/* 2. Replying to 标记（包含原推文）*/}
            <Card
              size="small"
              style={{
                marginBottom: 12,
                backgroundColor: themeColors.hoverBg,
                borderColor: themeColors.cardBorder,
                borderLeft: `3px solid #657786`,
                borderRadius: 12,
                border: `1px solid ${themeColors.cardBorder}`,
                boxShadow: isDark ? '0 4px 16px rgba(0,0,0,0.2)' : '0 2px 8px rgba(0,0,0,0.06)',
              }}
              bodyStyle={{ padding: 12 }}
            >
              <Space direction="vertical" size={12} style={{ width: '100%' }}>
                {/* 标题行 */}
                <Space style={{ width: '100%', justifyContent: 'space-between' }}>
                  <Space>
                    <CommentOutlined style={{ color: '#657786' }} />
                    <Text type="secondary">
                      Replying to @{content.in_reply_to_username}
                    </Text>
                  </Space>
                  
                  {/* 加载状态显示在右侧 */}
                  {originalTweetLoading && (
                    <Space size={4}>
                      <Spin size="small" />
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        Loading...
                      </Text>
                    </Space>
                  )}
                </Space>

                {/* 原推文内容（嵌入在卡片内）*/}
                {content.in_reply_to_id && content.in_reply_to_username && !originalTweetLoading && (
                  <div>
                    {originalTweet ? (
                      // 如果数据库中有原推文，显示完整内容
                      <div
                        style={{
                          backgroundColor: themeColors.cardBg,
                          border: `1px solid ${themeColors.cardBorder}`,
                          borderRadius: 8,
                          padding: 12,
                        }}
                      >
                        <Space direction="vertical" size={8} style={{ width: '100%' }}>
                          {/* 原推文作者 */}
                          <Space>
                            <Avatar size="small" style={{ backgroundColor: '#1DA1F2' }}>
                              {originalTweet.author_name?.[0] || 'T'}
                            </Avatar>
                            <Text strong style={{ color: themeColors.text }}>
                              {originalTweet.author_name}
                            </Text>
                            <Text type="secondary" style={{ fontSize: 12 }}>
                              @{originalTweet.author_identifier}
                            </Text>
                          </Space>
                          
                          {/* 原推文内容 */}
                          <Text style={{ color: themeColors.text }}>
                            {originalTweet.content}
                          </Text>
                          
                          {/* 原推文媒体 */}
                          {originalTweet.media_urls && originalTweet.media_urls.length > 0 && (
                            <div style={{ marginTop: 8 }}>
                              {originalTweet.media_urls.map((url, index) => (
                                <img
                                  key={index}
                                  src={url}
                                  alt="media"
                                  style={{
                                    maxWidth: '100%',
                                    borderRadius: 8,
                                    marginBottom: index < originalTweet.media_urls!.length - 1 ? 8 : 0,
                                  }}
                                />
                              ))}
                            </div>
                          )}
                          
                          {/* 原推文链接 */}
                          <a
                            href={originalTweet.url}
                            target="_blank"
                            rel="noopener noreferrer"
                            style={{ fontSize: 12 }}
                          >
                            <LinkOutlined /> View on Twitter
                          </a>
                        </Space>
                      </div>
                    ) : (
                      // 如果数据库中没有原推文，使用 Twitter widgets.js 嵌入
                      <div
                        ref={tweetContainerRef}
                        style={{
                          backgroundColor: themeColors.cardBg,
                          border: `1px solid ${themeColors.cardBorder}`,
                          borderRadius: 8,
                          padding: 12,
                          minHeight: '200px',
                        }}
                      >
                        {/* Twitter widget 将在这里渲染，自动适配高度 */}
                      </div>
                    )}
                  </div>
                )}
              </Space>
            </Card>
          </div>
        )}

        {/* Retweet（转推）内容 */}
        {content.is_retweet && content.retweeted_content && (
          <Card
            size="small"
            style={{
              marginBottom: 12,
              backgroundColor: themeColors.hoverBg,
              border: `1px solid ${themeColors.cardBorder}`,
              borderRadius: 12,
              boxShadow: isDark ? '0 4px 16px rgba(0,0,0,0.2)' : '0 2px 8px rgba(0,0,0,0.06)',
            }}
          >
            <Space direction="vertical" size={4} style={{ width: '100%' }}>
              <Space>
                <RetweetOutlined style={{ color: '#00ba7c' }} />
                <Text type="secondary">Retweeted from @{content.retweeted_author}</Text>
              </Space>
              <Text style={{ color: themeColors.text }}>{content.retweeted_content}</Text>
              {content.retweeted_url && (
                <a href={content.retweeted_url} target="_blank" rel="noopener noreferrer">
                  <LinkOutlined /> View original tweet
                </a>
              )}
            </Space>
          </Card>
        )}

        {/* Quote（引用）- 显示顺序：当前引用内容 → Quoted from → 被引用推文 */}
        {content.is_quote && content.quoted_content && (
          <div style={{ marginBottom: 16 }}>
            {/* 1. 当前引用内容（最上方）*/}
            <Paragraph style={{ color: themeColors.text, marginBottom: 12 }}>
              {content.content}
            </Paragraph>

            {/* 2. Quoted from 标记 + 被引用推文 */}
            <Card
              size="small"
              style={{
                marginBottom: 12,
                backgroundColor: themeColors.hoverBg,
                border: `1px solid ${themeColors.cardBorder}`,
                borderLeft: '3px solid #1DA1F2',
                borderRadius: 12,
                boxShadow: isDark ? '0 4px 16px rgba(0,0,0,0.2)' : '0 2px 8px rgba(0,0,0,0.06)',
              }}
            >
              <Space direction="vertical" size={8} style={{ width: '100%' }}>
                {/* 标题 */}
                <Space>
                  <MessageOutlined style={{ color: '#1DA1F2' }} />
                  <Text type="secondary">Quoted from @{content.quoted_author}</Text>
                </Space>
                
                {/* 被引用的推文内容 */}
                <div
                  style={{
                    backgroundColor: themeColors.cardBg,
                    border: `1px solid ${themeColors.cardBorder}`,
                    borderRadius: 8,
                    padding: 12,
                  }}
                >
                  <Text style={{ color: themeColors.text }}>{content.quoted_content}</Text>
                </div>
                
                {/* 链接 */}
                {content.quoted_url && (
                  <a 
                    href={content.quoted_url} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    style={{ fontSize: 12 }}
                  >
                    <LinkOutlined /> View quoted tweet
                  </a>
                )}
              </Space>
            </Card>
          </div>
        )}

        {/* 主要内容（非 Reply、非 Retweet、非 Quote 时显示）*/}
        {!content.is_retweet && !content.is_reply && !content.is_quote && (
          <Paragraph style={{ color: themeColors.text }}>{content.content}</Paragraph>
        )}
      </div>
    );
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
          className="feed-page-layout__sider"
          style={{
            backgroundColor: themeColors.cardBg,
            borderRight: `1px solid ${themeColors.cardBorder}`,
            overflowY: 'auto',
          }}
        >
          <div style={{ padding: '16px 12px', borderBottom: `1px solid ${themeColors.cardBorder}` }}>
            <Space style={{ width: '100%', justifyContent: 'space-between' }}>
              <Text strong style={{ color: themeColors.textSecondary, fontSize: 12 }}>
                MY FEEDS ({subscriptions.length})
              </Text>
              <Space size="small">
                <Tooltip title="Add Feed">
                  <Button
                    type="primary"
                    size="small"
                    shape="circle"
                    icon={<PlusOutlined style={{ fontSize: 12 }} />}
                    onClick={handleAddSubscription}
                    style={{ fontSize: 12 }}
                  />
                </Tooltip>
                <Tooltip title="refresh">
                  <Button
                    type="text"
                    size="small"
                    icon={<ReloadOutlined style={{ fontSize: 12 }} />}
                    onClick={loadSubscriptions}
                    loading={loading}
                    style={{
                      color: themeColors.textSecondary,
                      fontSize: 12,
                      width: 24,
                      height: 24,
                      minWidth: 24,
                      padding: 0,
                    }}
                  />
                </Tooltip>
              </Space>
            </Space>
            
            {/* 🔍 过滤器：Active / Disabled */}
            <div style={{ 
              padding: '12px 12px 8px',
              borderBottom: `1px solid ${themeColors.cardBorder}`,
              display: 'flex',
              gap: 8,
            }}>
              <Button
                type={subscriptionFilter === 'active' ? 'primary' : 'text'}
                size="small"
                onClick={() => setSubscriptionFilter('active')}
                style={{
                  fontSize: 11,
                  height: 24,
                  ...(subscriptionFilter === 'active' ? {} : { color: themeColors.textSecondary }),
                }}
              >
                Active ({subscriptions.filter(s => s.enabled).length})
              </Button>
              <Button
                type={subscriptionFilter === 'disabled' ? 'primary' : 'text'}
                size="small"
                onClick={() => setSubscriptionFilter('disabled')}
                style={{
                  fontSize: 11,
                  height: 24,
                  ...(subscriptionFilter === 'disabled' ? {} : { color: themeColors.textSecondary }),
                }}
              >
                Disabled ({subscriptions.filter(s => !s.enabled).length})
              </Button>
            </div>
          </div>

          {loading ? (
            <div style={{ 
              display: 'flex', 
              alignItems: 'center', 
              justifyContent: 'center',
              height: 'calc(100vh - 200px)'
            }}>
              <Spin />
            </div>
          ) : subscriptions.length === 0 ? (
            <div style={{ 
              display: 'flex', 
              alignItems: 'center', 
              justifyContent: 'center',
              height: 'calc(100vh - 200px)'
            }}>
              <Empty description="No subscriptions yet" />
            </div>
          ) : (() => {
            // 🔍 根据过滤器筛选：Active = enabled, Disabled = !enabled
            const filteredSubscriptions = subscriptions.filter(sub => {
              if (subscriptionFilter === 'active') return sub.enabled;
              return !sub.enabled; // 'disabled'
            });
            
            return filteredSubscriptions.length === 0 ? (
              <div style={{ 
                display: 'flex', 
                alignItems: 'center', 
                justifyContent: 'center',
                height: 'calc(100vh - 200px)'
              }}>
                <Empty
                  description={subscriptionFilter === 'active' ? 'No active subscriptions' : 'No disabled subscriptions'}
                />
              </div>
            ) : (
              <List
                dataSource={filteredSubscriptions}
                renderItem={(subscription) => (
                <List.Item
                  key={subscription.id}
                  style={{
                    padding: '12px 16px',
                    cursor: 'pointer',
                    backgroundColor:
                      selectedSubscription?.id === subscription.id
                        ? themeColors.selectedItemBg
                        : 'transparent',
                    borderLeft:
                      selectedSubscription?.id === subscription.id
                        ? `3px solid ${themeColors.selectedBorder}`
                        : '3px solid transparent',
                    position: 'relative',
                    paddingRight: 40,
                    // 🎨 禁用订阅的视觉效果
                    opacity: subscription.enabled ? 1 : 0.5,
                    filter: subscription.enabled ? 'none' : 'grayscale(30%)',
                  }}
                  onMouseEnter={(e) => {
                    if (selectedSubscription?.id !== subscription.id) {
                      e.currentTarget.style.backgroundColor = themeColors.hoverBg;
                    }
                    // 禁用订阅悬停时稍微提亮
                    if (!subscription.enabled) {
                      e.currentTarget.style.opacity = '0.7';
                    }
                  }}
                  onMouseLeave={(e) => {
                    if (selectedSubscription?.id !== subscription.id) {
                      e.currentTarget.style.backgroundColor = 'transparent';
                    }
                    // 恢复禁用订阅的透明度
                    if (!subscription.enabled) {
                      e.currentTarget.style.opacity = '0.5';
                    }
                  }}
                >
                  {/* 未读数量：右上角、10px 字体，不遮挡 More 按钮 */}
                  {subscription.unread_count !== undefined && subscription.unread_count > 0 && (
                    <span
                      style={{
                        position: 'absolute',
                        top: '50%',
                        transform: 'translateY(-50%)',
                        right: 36,
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
                      {subscription.unread_count > 99 ? '99+' : subscription.unread_count}
                    </span>
                  )}
                  <div onClick={() => handleSelectSubscription(subscription)} style={{ flex: 1 }}>
                    <List.Item.Meta
                      avatar={
                        <Avatar
                          icon={getChannelIcon(subscription.channel_type)}
                          style={{
                            backgroundColor:
                              subscription.channel_type === 'twitter'
                                ? '#1DA1F2'
                                : subscription.channel_type === 'youtube'
                                  ? '#FF0000'
                                  : '#ccc',
                          }}
                        />
                      }
                      title={
                        <Space size={6} style={{ flex: 1, minWidth: 0 }}>
                          <Text
                            strong
                            style={{
                              color: themeColors.text,
                              fontSize: 14,
                              overflow: 'hidden',
                              textOverflow: 'ellipsis',
                              whiteSpace: 'nowrap',
                            }}
                          >
                            {subscription.channel_name}
                          </Text>
                          {/* 🎨 禁用状态标签 */}
                          {!subscription.enabled && (
                            <Tag
                              color="error"
                              style={{
                                fontSize: 10,
                                padding: '0 4px',
                                lineHeight: '16px',
                                margin: 0,
                              }}
                            >
                              Disabled
                            </Tag>
                          )}
                        </Space>
                      }
                      description={undefined}
                    />
                  </div>
                  
                  {/* 🎨 操作菜单按钮 */}
                  <Dropdown
                    menu={{
                      items: [
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
                        {
                          type: 'divider',
                        },
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
                </List.Item>
              )}
            />
            );
          })()}
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
                <Space direction="vertical" size={4} style={{ width: '100%' }}>
                  <Text strong style={{ color: themeColors.text, fontSize: 16 }}>
                    {selectedSubscription.channel_name ||
                      selectedSubscription.channel_identifier}
                  </Text>
                  <Text type="secondary" style={{ fontSize: 12 }}>
                    {contents.length} articles
                  </Text>
                </Space>
              </div>

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
              ) : (
                <>
                  <List
                    dataSource={contents}
                    renderItem={(content) => (
                      <List.Item
                        key={content.id}
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
                        <div style={{ width: '100%', display: 'flex', flexDirection: 'column', gap: 6 }}>
                          {/* title + 正文区域：严格限高，超出隐藏 */}
                          <div style={{ maxHeight: 96, overflow: 'hidden', display: 'flex', flexDirection: 'column', gap: 5 }}>
                            {/* title行：最多2行截断 + 已读标记 */}
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 8 }}>
                              <span
                                style={{
                                  fontWeight: 600,
                                  color: themeColors.text,
                                  fontSize: 14,
                                  lineHeight: '1.4',
                                  display: '-webkit-box',
                                  WebkitLineClamp: 2,
                                  WebkitBoxOrient: 'vertical',
                                  overflow: 'hidden',
                                  wordBreak: 'break-word',
                                  flex: 1,
                                  minWidth: 0,
                                } as React.CSSProperties}
                              >
                                {content.title || ''}
                              </span>
                              {content.is_read && (
                                <CheckOutlined style={{ color: '#52c41a', flexShrink: 0, marginTop: 2 }} />
                              )}
                            </div>

                            {/* 正文：最多 2 行，超出省略号 */}
                            <Text
                              style={{
                                color: themeColors.textSecondary,
                                fontSize: 13,
                                lineHeight: '1.4',
                                display: '-webkit-box',
                                WebkitLineClamp: 2,
                                WebkitBoxOrient: 'vertical',
                                overflow: 'hidden',
                              }}
                            >
                              {content.content}
                            </Text>
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
                            <Text type="secondary" style={{ fontSize: 12 }}>
                              {content.author_name || (content.author_identifier ? `@${content.author_identifier}` : '')}
                            </Text>
                            <Text type="secondary" style={{ fontSize: 12 }}>·</Text>
                            <Text type="secondary" style={{ fontSize: 12 }}>
                              {content.published_at
                                ? dayjs(content.published_at).fromNow()
                                : 'Unknown'}
                            </Text>
                            {content.heat_level && content.heat_level >= 1 && (
                              <Tooltip title={getHeatLevelConfig(content.heat_level).text}>
                                <FireOutlined
                                  style={{
                                    color: getHeatLevelConfig(content.heat_level).color,
                                    fontSize: 13,
                                  }}
                                />
                              </Tooltip>
                            )}
                          </Space>
                        </div>
                      </List.Item>
                    )}
                  />
                  
                  {/* 🔥 加载更多按钮 */}
                  {contents.length > 0 && hasMoreContents && (
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
                  {contents.length > 0 && !hasMoreContents && (
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
            padding: '32px 80px',
          }}
        >
          {selectedContent ? (
            <div style={{ maxWidth: 600, margin: '0 auto', paddingLeft: 24, paddingRight: 24 }}>
              <Space direction="vertical" size={16} style={{ width: '100%' }}>
                {/* 作者信息 */}
                <div>
                  <Space>
                    <Avatar
                      icon={selectedContent.channel_type === 'youtube' ? <YoutubeOutlined /> : <TwitterOutlined />}
                      style={{ backgroundColor: selectedContent.channel_type === 'youtube' ? '#FF0000' : '#1DA1F2' }}
                    />
                    <div>
                      <div>
                        <Text strong style={{ color: themeColors.text }}>
                          {selectedContent.author_name || selectedContent.author_identifier}
                        </Text>
                      </div>
                      <Text type="secondary" style={{ fontSize: 12 }}>
                        {selectedContent.published_at
                          ? dayjs(selectedContent.published_at).format('MMM D, YYYY')
                          : 'Unknown'}
                      </Text>
                    </div>
                  </Space>
                </div>

                {/* YouTube 嵌入播放器 */}
                {selectedContent.channel_type === 'youtube' && selectedContent.content_id && (
                  <div style={{
                    position: 'relative',
                    width: '100%',
                    paddingTop: '56.25%', // 16:9
                    borderRadius: 12,
                    overflow: 'hidden',
                    backgroundColor: '#000',
                  }}>
                    <iframe
                      style={{
                        position: 'absolute',
                        top: 0,
                        left: 0,
                        width: '100%',
                        height: '100%',
                        border: 'none',
                        borderRadius: 12,
                      }}
                      src={`https://www.youtube.com/embed/${selectedContent.content_id}`}
                      title={selectedContent.title || 'YouTube Video'}
                      allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
                      allowFullScreen
                    />
                  </div>
                )}

                {/* 标题（YouTube 展示大标题） */}
                {selectedContent.channel_type === 'youtube' && selectedContent.title && (
                  <Text strong style={{ color: themeColors.text, fontSize: 18, lineHeight: '1.5', display: 'block' }}>
                    {selectedContent.title}
                  </Text>
                )}

                {/* 正文内容（非 YouTube 用 tweet 渲染，YouTube 显示描述文字） */}
                <div style={{ marginTop: selectedContent.channel_type === 'youtube' ? 0 : 16 }}>
                  {selectedContent.channel_type === 'youtube' ? (
                    <Text style={{ color: themeColors.textSecondary, fontSize: 14, whiteSpace: 'pre-wrap' }}>
                      {selectedContent.content}
                    </Text>
                  ) : (
                    renderTweetContent(selectedContent)
                  )}
                </div>

                {/* 媒体图片（仅非 YouTube） */}
                {selectedContent.channel_type !== 'youtube' &&
                  selectedContent.media_urls && selectedContent.media_urls.length > 0 && (
                  <div>
                    {selectedContent.media_urls.map((url, idx) => (
                      <img
                        key={idx}
                        src={url}
                        alt=""
                        style={{ width: '100%', borderRadius: 12, marginBottom: 12 }}
                      />
                    ))}
                  </div>
                )}

                {/* YouTube 互动数：观看数 / 点赞数 */}
                {selectedContent.channel_type === 'youtube' && selectedContent.metadata && (
                  <Space wrap size={16}>
                    {typeof selectedContent.metadata.view_count === 'number' && (
                      <Tooltip title="观看数">
                        <Space size={4}>
                          <EyeOutlined style={{ color: themeColors.textSecondary }} />
                          <Text type="secondary" style={{ fontSize: 13 }}>
                            {selectedContent.metadata.view_count.toLocaleString()}
                          </Text>
                        </Space>
                      </Tooltip>
                    )}
                    {typeof selectedContent.metadata.like_count === 'number' && (
                      <Tooltip title="点赞数">
                        <Space size={4}>
                          <LikeOutlined style={{ color: '#eb2f96' }} />
                          <Text type="secondary" style={{ fontSize: 13 }}>
                            {selectedContent.metadata.like_count.toLocaleString()}
                          </Text>
                        </Space>
                      </Tooltip>
                    )}
                    {typeof selectedContent.metadata.comment_count === 'number' && (
                      <Tooltip title="评论数">
                        <Space size={4}>
                          <CommentOutlined style={{ color: '#1890ff' }} />
                          <Text type="secondary" style={{ fontSize: 13 }}>
                            {selectedContent.metadata.comment_count.toLocaleString()}
                          </Text>
                        </Space>
                      </Tooltip>
                    )}
                  </Space>
                )}

                {/* Twitter 互动数 */}
                {selectedContent.channel_type !== 'youtube' && (
                  <Space wrap size={16} style={{ marginTop: 16 }}>
                    {selectedContent.metadata && (
                      <>
                        {typeof selectedContent.metadata.like_count === 'number' && (
                          <Tooltip title="点赞">
                            <Space size={4}>
                              <LikeOutlined style={{ color: '#eb2f96' }} />
                              <Text type="secondary" style={{ fontSize: 13 }}>
                                {selectedContent.metadata.like_count}
                              </Text>
                            </Space>
                          </Tooltip>
                        )}
                        {typeof selectedContent.metadata.retweet_count === 'number' && (
                          <Tooltip title="转发">
                            <Space size={4}>
                              <RetweetOutlined style={{ color: '#52c41a' }} />
                              <Text type="secondary" style={{ fontSize: 13 }}>
                                {selectedContent.metadata.retweet_count}
                              </Text>
                            </Space>
                          </Tooltip>
                        )}
                        {typeof selectedContent.metadata.reply_count === 'number' && (
                          <Tooltip title="回复">
                            <Space size={4}>
                              <CommentOutlined style={{ color: '#1890ff' }} />
                              <Text type="secondary" style={{ fontSize: 13 }}>
                                {selectedContent.metadata.reply_count}
                              </Text>
                            </Space>
                          </Tooltip>
                        )}
                        {typeof selectedContent.metadata.quote_count === 'number' && selectedContent.metadata.quote_count > 0 && (
                          <Tooltip title="引用">
                            <Space size={4}>
                              <MessageOutlined style={{ color: '#1890ff' }} />
                              <Text type="secondary" style={{ fontSize: 13 }}>
                                {selectedContent.metadata.quote_count}
                              </Text>
                            </Space>
                          </Tooltip>
                        )}
                      </>
                    )}
                  </Space>
                )}

                {/* 外链 */}
                {selectedContent.url && (
                  <div style={{ marginTop: 8 }}>
                    <a
                      href={selectedContent.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      style={{ color: themeColors.selectedBorder }}
                    >
                      <Space>
                        <LinkOutlined />
                        {selectedContent.channel_type === 'youtube' ? 'Watch on YouTube' : 'View on Twitter'}
                      </Space>
                    </a>
                  </div>
                )}

                {/* YouTube Transcript 状态区域 */}
                {selectedContent.channel_type === 'youtube' && (
                  <>
                    <Divider style={{ borderColor: themeColors.cardBorder, margin: '8px 0' }} />
                    {selectedContent.has_transcript ? (
                      <div
                        style={{
                          borderRadius: 8,
                          padding: '10px 16px',
                          display: 'flex',
                          alignItems: 'center',
                          gap: 8,
                          backgroundColor: isDark
                            ? 'rgba(82, 196, 26, 0.08)'
                            : '#f6ffed',
                          border: `1px solid ${isDark ? 'rgba(82, 196, 26, 0.25)' : '#b7eb8f'}`,
                        }}
                      >
                        <CheckOutlined style={{ color: '#52c41a', fontSize: 14 }} />
                        <Text style={{ color: themeColors.text, fontSize: 13 }}>
                          Transcript available
                        </Text>
                      </div>
                    ) : (
                      <div
                        style={{
                          border: `1px dashed ${themeColors.cardBorder}`,
                          borderRadius: 8,
                          padding: '12px 16px',
                          display: 'flex',
                          alignItems: 'center',
                          justifyContent: 'space-between',
                          gap: 12,
                          flexWrap: 'wrap',
                        }}
                      >
                        <Space>
                          <AlignLeftOutlined style={{ color: themeColors.textSecondary }} />
                          <Text style={{ color: themeColors.textSecondary, fontSize: 13 }}>
                            {asrStatus === 'processing'
                              ? `Transcribing… ${asrProgress}%`
                              : asrStatus === 'completed'
                              ? 'Transcription complete — please refresh'
                              : asrStatus === 'failed'
                              ? 'Transcription failed'
                              : 'No transcript available'}
                          </Text>
                          {asrStatus === 'failed' && (
                            <Tag color="red" style={{ fontSize: 11 }}>Failed</Tag>
                          )}
                        </Space>
                        {asrStatus !== 'completed' && (
                          <Tooltip title="Use Fun-ASR to generate transcript (costs ASR tokens)">
                            <Button
                              size="small"
                              type="primary"
                              loading={asrStatus === 'processing'}
                              disabled={asrStatus === 'processing'}
                              onClick={handleTriggerASR}
                              style={{ fontSize: 12, borderRadius: 6 }}
                            >
                              {asrStatus === 'failed' ? 'Retry ASR' : 'AI Transcribe'}
                            </Button>
                          </Tooltip>
                        )}
                      </div>
                    )}
                  </>
                )}
              </Space>
            </div>
          ) : (
            <div style={{ 
              display: 'flex', 
              alignItems: 'center', 
              justifyContent: 'center',
              height: 'calc(100vh - 200px)'
            }}>
              <Empty description="Select an article to read" />
            </div>
          )}
        </Content>
      </Layout>

      {/* 添加订阅Modal */}
      <Modal
        title="Add New Feed"
        open={addModalVisible}
        onOk={handleCreateSubscription}
        onCancel={() => { if (!isSubmittingSubscription) setAddModalVisible(false); }}
        okText="Add"
        okButtonProps={{ loading: isSubmittingSubscription }}
        cancelButtonProps={{ disabled: isSubmittingSubscription }}
        styles={getModalStyles(isDark, themeColors)}
      >
        <Form form={form} layout="vertical">
          <Form.Item
            name="channel_type"
            label="Feed Type"
            rules={[{ required: true, message: 'Please select feed type' }]}
          >
            <Select placeholder="Select feed type">
              {CHANNEL_TYPES.map((type) => (
                <Option key={type.value} value={type.value} disabled={type.disabled}>
                  <Space>
                    {type.icon}
                    <span>{type.label}</span>
                    {type.disabled && <Tag color="orange">Coming Soon</Tag>}
                  </Space>
                </Option>
              ))}
            </Select>
          </Form.Item>

          <Form.Item
            noStyle
            shouldUpdate={(prevValues, currentValues) =>
              prevValues.channel_type !== currentValues.channel_type
            }
          >
            {({ getFieldValue }) => {
              const channelType = getFieldValue('channel_type');
              return (
                <>
                  <Form.Item
                    name="channel_identifier"
                    label={
                      channelType === 'twitter'
                        ? 'Twitter Username'
                        : channelType === 'youtube'
                        ? 'YouTube Channel'
                        : 'Channel Identifier'
                    }
                    rules={[{ required: true, message: 'Please input identifier' }]}
                    extra={
                      channelType === 'twitter'
                        ? 'Enter without @ symbol (e.g., elonmusk)'
                        : channelType === 'youtube'
                        ? 'YouTube channel username or handle (e.g., creativeplanning or @creativeplanning)'
                        : 'Channel ID or username'
                    }
                  >
                    <Input 
                      placeholder={
                        channelType === 'twitter' 
                          ? 'e.g., elonmusk' 
                          : channelType === 'youtube'
                          ? 'e.g., creativeplanning'
                          : 'Enter channel identifier'
                      } 
                    />
                  </Form.Item>
                </>
              );
            }}
          </Form.Item>
        </Form>
      </Modal>

      {/* 🎨 Edit Subscription Modal */}
      <Modal
        title="Edit Subscription"
        open={editModalVisible}
        onOk={handleSaveEdit}
        onCancel={() => {
          setEditModalVisible(false);
          editForm.resetFields();
        }}
        okText="Save"
        styles={getModalStyles(isDark, themeColors)}
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
