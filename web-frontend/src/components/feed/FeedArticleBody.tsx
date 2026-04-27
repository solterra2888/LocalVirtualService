import React, { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react';
import {
  App,
  Avatar,
  Button,
  Divider,
  Modal,
  Space,
  Spin,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import dayjs from 'dayjs';
import {
  AlignLeftOutlined,
  BulbOutlined,
  CommentOutlined,
  EyeOutlined,
  FileTextOutlined,
  LikeOutlined,
  LinkOutlined,
  MessageOutlined,
  ReadOutlined,
  RetweetOutlined,
  RobotOutlined,
  SyncOutlined,
  TwitterOutlined,
  YoutubeOutlined,
} from '@ant-design/icons';
import { api } from '../../services/api';
import { getNewsSourceColor } from './feedChannelConfig';
import { useFileProcessing } from '../../contexts/FileProcessingContext';
import FeedTweetContent from './FeedTweetContent';
import type { FeedThemeColors } from './feedTheme';
import type {
  SubscriptionContent,
  SubscriptionContentTagsLite,
} from '../../services/subscriptions';
import { subscriptionsAPI, getTweetById } from '../../services/subscriptions';
import ReportTickerTags, { PC_INFO_TAGS_TAG_STYLE } from '../ReportTickerTags';
// PR-7: 个性化 summary picker
import UploadAnalysisTemplatePicker, {
  UploadAnalysisTemplatePickerValue,
} from '../UploadAnalysisTemplatePicker';

const { Text } = Typography;

/** Collapsed YouTube description panel height (~4 lines at 14px / 1.5 line-height). */
const YOUTUBE_DESCRIPTION_COLLAPSED_MAX_HEIGHT_PX = 88;

export interface FeedArticleBodyProps {
  selectedContent: SubscriptionContent | null;
  themeColors: FeedThemeColors;
  isDark: boolean;
  themeMode: 'light' | 'dark';
  /** When set, syncs transcript flag into parent (e.g. PC list selection) */
  onTranscriptReady?: (contentId: number) => void;
  /**
   * Phase 3: when AI tags are (re-)generated, parent can sync the lite payload
   * into its list state so the tag chips in the list column refresh without
   * a full reload.
   */
  onTagsUpdated?: (contentId: number, tags: SubscriptionContentTagsLite) => void;
}

/** Channels that participate in auto-tagging (see FEEDS_AUTO_TAGGING_PLAN.md §1). */
const AUTO_TAG_CHANNELS = new Set(['twitter', 'youtube']);

interface CeleryTaskStatusResponse {
  task_id: string;
  state: string;
  ready: boolean;
  successful?: boolean | null;
  failed?: boolean | null;
  info?: Record<string, unknown> | null;
  result?: Record<string, unknown> | null;
  error?: string | null;
}

/**
 * Full article / tweet / YouTube detail column — shared by FeedReaderPage and mobile Feeds.
 */
const FeedArticleBody: React.FC<FeedArticleBodyProps> = ({
  selectedContent,
  themeColors,
  isDark,
  themeMode,
  onTranscriptReady,
  onTagsUpdated,
}) => {
  const { message } = App.useApp();
  const tweetContainerRef = useRef<HTMLDivElement>(null);
  const [originalTweet, setOriginalTweet] = useState<SubscriptionContent | null>(null);
  const [originalTweetLoading, setOriginalTweetLoading] = useState(false);

  const [asrTaskId, setAsrTaskId] = useState<string | null>(null);
  const [asrProgress, setAsrProgress] = useState(0);
  const [asrStatus, setAsrStatus] = useState<'idle' | 'processing' | 'completed' | 'failed'>('idle');

  // Phase 3: AI 自动打标 + 摘要 (LEFT JOIN 在列表接口已返回 tags; 这里的 state
  // 只是 retag 完成后本地覆盖, 避免等父组件 reload)
  const [localTags, setLocalTags] = useState<SubscriptionContentTagsLite | null>(null);
  const [retagStatus, setRetagStatus] =
    useState<'idle' | 'dispatching' | 'polling' | 'completed' | 'failed'>('idle');
  const retagPollTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const clearRetagPoll = useCallback(() => {
    if (retagPollTimerRef.current) {
      clearTimeout(retagPollTimerRef.current);
      retagPollTimerRef.current = null;
    }
  }, []);

  // PR-7: per-user 个性化 summary 状态
  // localPersonalized: 本地最新拿到的个性化 summary (覆盖父组件 selectedContent.personalized_summary)
  const [localPersonalized, setLocalPersonalized] = useState<{
    summary: string | null;
    reasoning: string | null;
    template_id: number | null;
  } | null>(null);
  const [personalizeModalOpen, setPersonalizeModalOpen] = useState(false);
  const [personalizePicker, setPersonalizePicker] = useState<UploadAnalysisTemplatePickerValue>({});
  const [personalizeLoading, setPersonalizeLoading] = useState(false);

  const youtubeDescOuterRef = useRef<HTMLDivElement>(null);
  const youtubeDescInnerRef = useRef<HTMLDivElement>(null);
  const [youtubeDescExpanded, setYoutubeDescExpanded] = useState(false);
  const [youtubeDescOverflows, setYoutubeDescOverflows] = useState(false);

  const transcriptCacheRef = useRef<Map<number, string>>(new Map());
  const [transcriptModalOpen, setTranscriptModalOpen] = useState(false);
  const [transcriptModalLoading, setTranscriptModalLoading] = useState(false);
  const [transcriptModalText, setTranscriptModalText] = useState('');

  const { addTask, tasks } = useFileProcessing();

  const openTranscriptModal = async () => {
    if (!selectedContent?.id) return;
    const cid = selectedContent.id;
    setTranscriptModalOpen(true);
    const cached = transcriptCacheRef.current.get(cid);
    if (cached !== undefined) {
      setTranscriptModalText(cached);
      setTranscriptModalLoading(false);
      return;
    }
    setTranscriptModalText('');
    setTranscriptModalLoading(true);
    const res = await subscriptionsAPI.getSubscriptionContentTranscript(cid);
    if (res.success && res.data?.transcript) {
      transcriptCacheRef.current.set(cid, res.data.transcript);
      setTranscriptModalText(res.data.transcript);
    } else {
      message.error(res.error || 'Failed to load transcript');
    }
    setTranscriptModalLoading(false);
  };

  useEffect(() => {
    setYoutubeDescExpanded(false);
    setTranscriptModalOpen(false);
  }, [selectedContent?.id]);

  useLayoutEffect(() => {
    if (selectedContent?.channel_type !== 'youtube') {
      setYoutubeDescOverflows(false);
      return;
    }
    const outer = youtubeDescOuterRef.current;
    const inner = youtubeDescInnerRef.current;
    if (!outer || !inner) {
      setYoutubeDescOverflows(false);
      return;
    }
    if (!youtubeDescExpanded) {
      setYoutubeDescOverflows(inner.scrollHeight > outer.clientHeight + 1);
    }
  }, [selectedContent?.channel_type, selectedContent?.content, selectedContent?.id, youtubeDescExpanded]);

  useEffect(() => {
    const loadOriginalTweet = async () => {
      if (selectedContent?.is_reply && selectedContent?.in_reply_to_id) {
        setOriginalTweetLoading(true);
        try {
          const tweet = await getTweetById(selectedContent.in_reply_to_id, 1);
          setOriginalTweet(tweet);
        } catch (error) {
          console.error('Failed to load original tweet:', error);
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

  useEffect(() => {
    if (
      !originalTweetLoading &&
      !originalTweet &&
      selectedContent?.is_reply &&
      selectedContent?.in_reply_to_id &&
      tweetContainerRef.current
    ) {
      const el = tweetContainerRef.current;
      el.innerHTML = '';

      const loadTwitterWidget = () => {
        const twttr = (window as unknown as { twttr?: { widgets?: { createTweet: Function } } }).twttr;
        if (twttr?.widgets) {
          twttr.widgets.createTweet(selectedContent.in_reply_to_id, el, {
            theme: themeMode === 'dark' ? 'dark' : 'light',
            conversation: 'none',
            cards: 'visible',
          });
        } else {
          const script = document.createElement('script');
          script.src = 'https://platform.twitter.com/widgets.js';
          script.async = true;
          script.onload = () => {
            const w = (window as unknown as { twttr?: { widgets?: { createTweet: Function } } }).twttr;
            if (w?.widgets) {
              w.widgets.createTweet(selectedContent.in_reply_to_id, el, {
                theme: themeMode === 'dark' ? 'dark' : 'light',
                conversation: 'none',
                cards: 'visible',
              });
            }
          };
          document.body.appendChild(script);
        }
      };

      loadTwitterWidget();
    }
  }, [selectedContent, originalTweet, originalTweetLoading, themeMode]);

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
    addTask(taskId, videoTitle, 'youtube');
    message.info('ASR transcription started — progress shown in the top bar');
  };

  useEffect(() => {
    if (!asrTaskId || asrStatus !== 'processing') return;

    const task = tasks.get(asrTaskId);
    if (!task) return;

    const s = task.status.status;
    const resultStatus = (task.status as { result?: { status?: string; error?: string } }).result?.status;
    const isCompleted = s === 'completed' && resultStatus !== 'failed';
    const isFailed = s === 'failed' || (s === 'completed' && resultStatus === 'failed');

    if (isCompleted) {
      setAsrStatus('completed');
      setAsrProgress(100);
      message.success('Transcription completed!');
      if (selectedContent) {
        onTranscriptReady?.(selectedContent.id);
      }
    } else if (isFailed) {
      setAsrStatus('failed');
      const errMsg =
        (task.status as { result?: { error?: string }; message?: string }).result?.error ||
        task.status.message ||
        'Transcription failed';
      message.error(`ASR failed: ${errMsg}`);
    } else {
      setAsrProgress(task.status.progress || 0);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- message API stable; avoid re-running ASR listener
  }, [tasks, asrTaskId, asrStatus, selectedContent, onTranscriptReady]);

  useEffect(() => {
    setAsrStatus('idle');
    setAsrProgress(0);
    setAsrTaskId(null);
    // 切换内容时清理 AI 打标本地状态 (使用父 selectedContent.tags 重新渲染)
    setLocalTags(null);
    setRetagStatus('idle');
    clearRetagPoll();
    // PR-7: 切换内容时清理个性化本地状态 (用父 selectedContent.personalized_* 字段)
    setLocalPersonalized(null);
    setPersonalizeModalOpen(false);
    setPersonalizePicker({});
    setPersonalizeLoading(false);
  }, [selectedContent?.id, clearRetagPoll]);

  // 组件卸载时清理轮询定时器, 防止 setState on unmounted.
  useEffect(() => {
    return () => {
      clearRetagPoll();
    };
  }, [clearRetagPoll]);

  const displayTags = localTags ?? selectedContent?.tags ?? null;

  /** Poll Celery task status after dispatch; on success, fetch full tags row. */
  const pollRetagStatus = useCallback(
    (taskId: string, contentId: number, attempt: number) => {
      const MAX_ATTEMPTS = 30; // ~60s total (2s each)
      if (attempt >= MAX_ATTEMPTS) {
        setRetagStatus('failed');
        message.error('Re-tagging timed out — check logs or retry');
        return;
      }
      retagPollTimerRef.current = setTimeout(async () => {
        try {
          const status = await api.get<CeleryTaskStatusResponse>(
            `/subscriptions/tasks/${taskId}/status`
          );
          if (status.ready && status.successful) {
            // Refresh the lite tags payload for immediate UI feedback
            const tagsRes = await subscriptionsAPI.getContentTags(contentId);
            if (tagsRes.success && tagsRes.data) {
              const lite: SubscriptionContentTagsLite = {
                sector: tagsRes.data.sector,
                sector_en: tagsRes.data.sector_en,
                industry: tagsRes.data.industry,
                industry_en: tagsRes.data.industry_en,
                tickers: tagsRes.data.tickers || [],
                concepts: tagsRes.data.concepts || [],
                summary: tagsRes.data.summary,
                confidence: tagsRes.data.confidence,
                tagged_at: tagsRes.data.tagged_at,
              };
              setLocalTags(lite);
              onTagsUpdated?.(contentId, lite);
              setRetagStatus('completed');
              message.success('AI 标签已更新');
            } else {
              setRetagStatus('failed');
              message.error(tagsRes.error || 'Failed to refresh tags');
            }
            return;
          }
          if (status.ready && status.failed) {
            setRetagStatus('failed');
            const err = status.error || 'Unknown error';
            message.error(`Re-tagging failed: ${err}`);
            return;
          }
          pollRetagStatus(taskId, contentId, attempt + 1);
        } catch (e: any) {
          setRetagStatus('failed');
          message.error(e?.response?.data?.detail || 'Failed to poll retag status');
        }
      }, 2000);
    },
    [message, onTagsUpdated]
  );

  const handleRetag = useCallback(async () => {
    if (!selectedContent || retagStatus === 'dispatching' || retagStatus === 'polling') {
      return;
    }
    const contentId = selectedContent.id;
    setRetagStatus('dispatching');
    try {
      const res = await subscriptionsAPI.retagContent(contentId, { force: true });
      if (!res.success || !res.data?.task_id) {
        setRetagStatus('failed');
        message.error(res.error || 'Failed to dispatch re-tagging');
        return;
      }
      message.info('Re-tagging dispatched, polling status...');
      setRetagStatus('polling');
      pollRetagStatus(res.data.task_id, contentId, 0);
    } catch (e: any) {
      setRetagStatus('failed');
      message.error(e?.response?.data?.detail || 'Failed to dispatch re-tagging');
    }
  }, [selectedContent, retagStatus, message, pollRetagStatus]);

  // PR-7: 提交个性化任务. 派发后短暂轮询 (~1.5-3s 后) 拉最新 personalized_summary
  // 来局部刷新 UI, 不阻塞用户操作.
  const handlePersonalize = useCallback(async () => {
    if (!selectedContent || personalizeLoading) return;
    const contentId = selectedContent.id;
    setPersonalizeLoading(true);
    try {
      const res = await subscriptionsAPI.personalizeContentSummary(contentId, {
        templateId: personalizePicker.templateId ?? null,
        templateVariables: personalizePicker.templateVariables,
        force: true,
      });
      if (!res.success) {
        message.error(res.error || 'Failed to dispatch personalization');
        setPersonalizeLoading(false);
        return;
      }
      message.info('Personalization dispatched, refreshing in a moment...');
      setPersonalizeModalOpen(false);

      // 简化轮询: 间隔 1.5s 拉一次, 最多 8 次 (~12s)
      let attempt = 0;
      const MAX = 8;
      const tick = async () => {
        attempt += 1;
        try {
          const data = await api.get<{
            id: number;
            template_id: number | null;
            summary: string | null;
            reasoning: string | null;
          }>(`/subscriptions/contents/${contentId}/personalized-summary`);
          if (data?.summary) {
            setLocalPersonalized({
              summary: data.summary,
              reasoning: data.reasoning ?? null,
              template_id: data.template_id ?? null,
            });
            setPersonalizeLoading(false);
            message.success('Personalized summary ready');
            return;
          }
        } catch (_) {
          // 404 表示尚未生成 - 继续轮询
        }
        if (attempt < MAX) {
          setTimeout(tick, 1500);
        } else {
          setPersonalizeLoading(false);
          message.warning(
            'Personalization is still processing. Refresh later to see the result.'
          );
        }
      };
      setTimeout(tick, 1500);
    } catch (e: any) {
      setPersonalizeLoading(false);
      message.error(e?.response?.data?.detail || 'Failed to dispatch personalization');
    }
  }, [selectedContent, personalizeLoading, personalizePicker, message]);

  // PR-7: 取消个性化, 回退到全局 summary
  const handleRemovePersonalization = useCallback(async () => {
    if (!selectedContent) return;
    const res = await subscriptionsAPI.deletePersonalizedSummary(selectedContent.id);
    if (res.success) {
      setLocalPersonalized(null);
      // 父组件 selectedContent.personalized_* 字段也置空
      message.success('Reverted to global summary');
    } else {
      message.error(res.error || 'Failed to remove personalization');
    }
  }, [selectedContent, message]);

  if (!selectedContent) {
    return null;
  }

  const channelType = selectedContent.channel_type;
  const showAiTagsPanel = AUTO_TAG_CHANNELS.has(channelType);
  const tagsHasContent = !!displayTags;
  const confidence = displayTags?.confidence ?? null;
  const isLowConfidence = confidence !== null && confidence < 0.5;
  const isUncategorized =
    displayTags?.sector === '未分类' || displayTags?.sector_en === 'Uncategorized';
  const retagButtonLoading =
    retagStatus === 'dispatching' || retagStatus === 'polling';

  // PR-7: 个性化 summary 优先级
  // 1. localPersonalized (用户刚生成, 本地 state)
  // 2. selectedContent.personalized_summary (后端列表接口附加)
  // 3. displayTags.summary (全局 baseline)
  const personalizedSummary =
    localPersonalized?.summary ?? selectedContent.personalized_summary ?? null;
  const personalizedReasoning =
    localPersonalized?.reasoning ?? selectedContent.personalized_reasoning ?? null;
  const isPersonalized =
    !!personalizedSummary ||
    selectedContent.summary_source === 'personalized';
  const effectiveSummary = personalizedSummary || displayTags?.summary || null;

  return (
    <Space direction="vertical" size={16} style={{ width: '100%' }}>
      <div>
        <Space>
          <Avatar
            icon={
              selectedContent.channel_type === 'youtube'
                ? <YoutubeOutlined />
                : selectedContent.channel_type === 'news'
                ? <ReadOutlined />
                : <TwitterOutlined />
            }
            style={{
              backgroundColor:
                selectedContent.channel_type === 'youtube'
                  ? '#FF0000'
                  : selectedContent.channel_type === 'news'
                  ? getNewsSourceColor(selectedContent.author_identifier)
                  : '#1DA1F2',
            }}
          />
          <div>
            <div>
              <Text strong style={{ color: themeColors.text }}>
                {selectedContent.author_name || selectedContent.author_identifier}
              </Text>
            </div>
            <Text type="secondary" style={{ fontSize: 12 }}>
              {selectedContent.published_at ? dayjs(selectedContent.published_at).format('MMM D, YYYY') : 'Unknown'}
            </Text>
          </div>
        </Space>
      </div>

      {/* ── Phase 3: AI 摘要 + 标签 (仅 twitter/youtube, News 另有独立分析管线) ── */}
      {showAiTagsPanel && (
        <div
          style={{
            borderRadius: 10,
            padding: '12px 16px',
            background: isDark
              ? 'rgba(33, 150, 243, 0.07)'
              : themeColors.boxMuted,
            border: `1px solid ${themeColors.cardBorder}`,
            borderLeft: `3px solid ${themeColors.selectedBorder}`,
          }}
        >
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              justifyContent: 'space-between',
              gap: 12,
              flexWrap: 'wrap',
              marginBottom: tagsHasContent ? 8 : 0,
            }}
          >
            <Space size={6} align="center">
              <RobotOutlined style={{ color: themeColors.selectedBorder, fontSize: 14 }} />
              <Text strong style={{ color: themeColors.text, fontSize: 13 }}>
                AI Summary
              </Text>
              {displayTags?.tagged_at && (
                <Tooltip title={dayjs(displayTags.tagged_at).format('YYYY-MM-DD HH:mm:ss')}>
                  <Text type="secondary" style={{ fontSize: 11 }}>
                    · {dayjs(displayTags.tagged_at).fromNow()}
                  </Text>
                </Tooltip>
              )}
              {isLowConfidence && (
                <Tooltip title={`Low confidence (${(confidence! * 100).toFixed(0)}%) — AI 不太确定, 建议人工复核`}>
                  <Tag color="warning" style={{ fontSize: 10, lineHeight: '14px', padding: '0 5px', margin: 0 }}>
                    low conf.
                  </Tag>
                </Tooltip>
              )}
            </Space>
            <Space size={8} align="center" wrap style={{ flexShrink: 0 }}>
              {selectedContent.channel_type === 'youtube' && selectedContent.has_transcript && (
                <Tooltip title="Open full video transcript in a window">
                  <Button
                    size="small"
                    type="default"
                    icon={<FileTextOutlined />}
                    onClick={openTranscriptModal}
                    style={{ fontSize: 12, borderRadius: 6 }}
                  >
                    Transcript
                  </Button>
                </Tooltip>
              )}
              <Tooltip
                title={
                  tagsHasContent
                    ? 'Re-generate AI tags + summary (costs LLM tokens)'
                    : 'Generate AI tags + summary (costs LLM tokens)'
                }
              >
                <Button
                  size="small"
                  icon={<SyncOutlined spin={retagButtonLoading} />}
                  loading={retagButtonLoading}
                  onClick={handleRetag}
                  style={{ fontSize: 12, borderRadius: 6 }}
                >
                  {tagsHasContent ? 'Re-tag' : 'Tag with AI'}
                </Button>
              </Tooltip>
              {/* PR-7: 个性化 summary 按钮 (per-user override, 不影响其他用户视图) */}
              <Tooltip title="Generate a personalized summary using your own template (per-user, doesn't affect other subscribers).">
                <Button
                  size="small"
                  icon={<RobotOutlined />}
                  loading={personalizeLoading}
                  onClick={() => {
                    setPersonalizePicker({});
                    setPersonalizeModalOpen(true);
                  }}
                  style={{ fontSize: 12, borderRadius: 6 }}
                >
                  {isPersonalized ? 'Re-personalize' : 'Personalize'}
                </Button>
              </Tooltip>
              {isPersonalized && (
                <Tooltip title="Remove personalized summary, revert to the global one.">
                  <Button
                    size="small"
                    type="text"
                    danger
                    onClick={handleRemovePersonalization}
                    style={{ fontSize: 12, padding: '0 6px' }}
                  >
                    Reset
                  </Button>
                </Tooltip>
              )}
            </Space>
          </div>

          {tagsHasContent ? (
            <Space direction="vertical" size={8} style={{ width: '100%' }}>
              {/* PR-7: 个性化徽标 (仅当用户有自己的 summary 覆盖时显示) */}
              {isPersonalized && (
                <Tag
                  color="purple"
                  style={{ fontSize: 10, lineHeight: '16px', padding: '0 6px', margin: 0 }}
                >
                  ✨ Personalized
                </Tag>
              )}
              {effectiveSummary && (
                <Text
                  style={{
                    color: themeColors.text,
                    fontSize: 13,
                    lineHeight: 1.55,
                    display: 'block',
                  }}
                >
                  {effectiveSummary}
                </Text>
              )}
              {/* PR-7: 个性化模式下额外展示 reasoning (主观解读) */}
              {isPersonalized && personalizedReasoning && (
                <Text
                  type="secondary"
                  style={{
                    fontSize: 12,
                    lineHeight: 1.55,
                    display: 'block',
                    fontStyle: 'italic',
                  }}
                >
                  {personalizedReasoning}
                </Text>
              )}

              {/* 行业 / 子行业 badges (未分类样式灰化) */}
              {!isUncategorized && displayTags && (
                <Space size={[4, 4]} wrap>
                  <Tag color="blue" style={{ fontSize: 11, margin: 0 }}>
                    {displayTags.sector}
                    {displayTags.sector_en ? ` / ${displayTags.sector_en}` : ''}
                  </Tag>
                  {displayTags.industry && displayTags.industry !== displayTags.sector && (
                    <Tag color="cyan" style={{ fontSize: 11, margin: 0 }}>
                      {displayTags.industry}
                      {displayTags.industry_en ? ` / ${displayTags.industry_en}` : ''}
                    </Tag>
                  )}
                </Space>
              )}
              {isUncategorized && (
                <Text type="secondary" style={{ fontSize: 11 }}>
                  未分类 (内容不聚焦任何具体行业)
                </Text>
              )}

              {/* Tickers (复用 ReportTickerTags 组件) */}
              {displayTags && displayTags.tickers && displayTags.tickers.length > 0 && (
                <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
                  <Text type="secondary" style={{ fontSize: 11, flexShrink: 0 }}>Tickers:</Text>
                  <ReportTickerTags
                    tickers={displayTags.tickers}
                    tagColor="magenta"
                    tagStyle={PC_INFO_TAGS_TAG_STYLE}
                    maxVisible={6}
                  />
                </div>
              )}

              {/* Concepts (chips) */}
              {displayTags && displayTags.concepts && displayTags.concepts.length > 0 && (
                <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap' }}>
                  <Text type="secondary" style={{ fontSize: 11, flexShrink: 0 }}>
                    <BulbOutlined /> Concepts:
                  </Text>
                  {displayTags.concepts.map((c) => (
                    <Tag key={c} style={{ fontSize: 11, margin: 0 }}>{c}</Tag>
                  ))}
                </div>
              )}
            </Space>
          ) : (
            <Text type="secondary" style={{ fontSize: 12 }}>
              {retagButtonLoading
                ? 'AI is analyzing this content...'
                : '暂无 AI 标签 — 点击 "Tag with AI" 生成标签与摘要'}
            </Text>
          )}
        </div>
      )}

      {selectedContent.channel_type === 'news' && selectedContent.title && (
        <Text strong style={{ color: themeColors.text, fontSize: 18, lineHeight: '1.5', display: 'block', marginTop: 8 }}>
          {selectedContent.title}
        </Text>
      )}

      {selectedContent.channel_type === 'news' && (
        <>
          <Text style={{ color: themeColors.textSecondary, fontSize: 14, whiteSpace: 'pre-wrap', lineHeight: '1.7' }}>
            {selectedContent.content}
          </Text>

          {selectedContent.metadata?.symbols && (selectedContent.metadata.symbols as string[]).length > 0 && (
            <div
              style={{
                marginTop: 12,
                display: 'flex',
                flexWrap: 'wrap',
                alignItems: 'center',
                gap: '8px 8px',
              }}
            >
              <Text type="secondary" style={{ fontSize: 12, flexShrink: 0 }}>
                Ticker:
              </Text>
              <Space size={[4, 4]} wrap>
                {(selectedContent.metadata.symbols as string[]).map((sym) => (
                  <Tag key={sym} style={PC_INFO_TAGS_TAG_STYLE}>
                    {sym}
                  </Tag>
                ))}
              </Space>
            </div>
          )}

          {selectedContent.categories && selectedContent.categories.length > 0 && (
            <div style={{ marginTop: 8 }}>
              <Text type="secondary" style={{ fontSize: 12, marginRight: 8 }}>Tags:</Text>
              {selectedContent.categories.map((tag) => (
                <Tag key={tag} style={{ fontSize: 11 }}>{tag}</Tag>
              ))}
            </div>
          )}

          {selectedContent.metadata?.sentiment && (selectedContent.metadata.sentiment as { polarity?: number }).polarity != null && (
            <div style={{ marginTop: 8 }}>
              <Text type="secondary" style={{ fontSize: 12, marginRight: 8 }}>Sentiment:</Text>
              <Tag color={
                (selectedContent.metadata.sentiment as { polarity: number }).polarity > 0.1
                  ? 'green'
                  : (selectedContent.metadata.sentiment as { polarity: number }).polarity < -0.1
                  ? 'red'
                  : 'default'
              }>
                {(selectedContent.metadata.sentiment as { polarity: number }).polarity > 0.1
                  ? 'Positive'
                  : (selectedContent.metadata.sentiment as { polarity: number }).polarity < -0.1
                  ? 'Negative'
                  : 'Neutral'}
                {' '}({(selectedContent.metadata.sentiment as { polarity: number }).polarity.toFixed(2)})
              </Tag>
            </div>
          )}
        </>
      )}

      {selectedContent.channel_type === 'youtube' && selectedContent.content_id && (
        <div
          style={{
            position: 'relative',
            width: '100%',
            marginTop: showAiTagsPanel ? 20 : 0,
            paddingTop: '56.25%',
            borderRadius: 12,
            overflow: 'hidden',
            backgroundColor: '#000',
          }}
        >
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

      {selectedContent.channel_type === 'youtube' && selectedContent.title && (
        <Text strong style={{ color: themeColors.text, fontSize: 18, lineHeight: '1.5', display: 'block' }}>
          {selectedContent.title}
        </Text>
      )}

      {selectedContent.channel_type === 'youtube' && !!selectedContent.content?.trim() && (
        <div style={{ marginTop: 8 }}>
          <div
            ref={youtubeDescOuterRef}
            style={{
              maxHeight: youtubeDescExpanded ? 'none' : YOUTUBE_DESCRIPTION_COLLAPSED_MAX_HEIGHT_PX,
              overflow: youtubeDescExpanded ? 'visible' : 'hidden',
            }}
          >
            <div ref={youtubeDescInnerRef}>
              <Text style={{ color: themeColors.textSecondary, fontSize: 14, lineHeight: 1.5, whiteSpace: 'pre-wrap' }}>
                {selectedContent.content}
              </Text>
            </div>
          </div>
          {(youtubeDescOverflows || youtubeDescExpanded) && (
            <Button
              type="link"
              size="small"
              onClick={() => setYoutubeDescExpanded((v) => !v)}
              style={{ padding: 0, height: 'auto', marginTop: 4 }}
            >
              {youtubeDescExpanded ? 'Less' : 'More'}
            </Button>
          )}
        </div>
      )}
      {selectedContent.channel_type !== 'youtube' && selectedContent.channel_type !== 'news' && (
        <div style={{ marginTop: 16 }}>
          <FeedTweetContent
            content={selectedContent}
            themeColors={themeColors}
            isDark={isDark}
            originalTweet={originalTweet}
            originalTweetLoading={originalTweetLoading}
            tweetContainerRef={tweetContainerRef}
          />
        </div>
      )}

      {selectedContent.channel_type !== 'youtube' && selectedContent.media_urls && selectedContent.media_urls.length > 0 && (
        <div>
          {selectedContent.media_urls.map((url, idx) => (
            <img key={idx} src={url} alt="" style={{ width: '100%', borderRadius: 12, marginBottom: 12 }} />
          ))}
        </div>
      )}

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
              {selectedContent.channel_type === 'youtube'
                ? 'Watch on YouTube'
                : selectedContent.channel_type === 'news'
                ? 'Read Original Article'
                : 'View on Twitter'}
            </Space>
          </a>
        </div>
      )}

      {selectedContent.channel_type === 'youtube' && (
        <>
          {!selectedContent.has_transcript && (
            <>
              <Divider style={{ borderColor: themeColors.cardBorder, margin: '8px 0' }} />
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
                    <Tag color="red" style={{ fontSize: 11 }}>
                      Failed
                    </Tag>
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
            </>
          )}
          <Modal
            title="Transcript"
            open={transcriptModalOpen}
            onCancel={() => setTranscriptModalOpen(false)}
            footer={null}
            width={720}
            destroyOnClose
            bodyStyle={{ maxHeight: 'min(70vh, 640px)', overflow: 'auto', paddingTop: 8 }}
          >
            {transcriptModalLoading ? (
              <div style={{ display: 'flex', justifyContent: 'center', padding: 32 }}>
                <Spin />
              </div>
            ) : (
              <Text
                style={{
                  color: themeColors.text,
                  fontSize: 13,
                  lineHeight: 1.6,
                  whiteSpace: 'pre-wrap',
                  wordBreak: 'break-word',
                  display: 'block',
                  fontFamily: 'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace',
                }}
              >
                {transcriptModalText || '—'}
              </Text>
            )}
          </Modal>
        </>
      )}

      {/* PR-7: Personalize summary 模板选择 Modal */}
      <Modal
        title="Personalize this feed's summary"
        open={personalizeModalOpen}
        onCancel={() => {
          if (!personalizeLoading) {
            setPersonalizeModalOpen(false);
          }
        }}
        onOk={handlePersonalize}
        okText={isPersonalized ? 'Re-personalize' : 'Personalize'}
        confirmLoading={personalizeLoading}
        cancelText="Cancel"
        width={620}
        destroyOnClose
      >
        <Text type="secondary" style={{ display: 'block', marginBottom: 12, fontSize: 12 }}>
          Use your own template to generate a per-user summary for this content.
          Other users still see the global summary unaffected.
        </Text>
        <UploadAnalysisTemplatePicker
          value={personalizePicker}
          onChange={setPersonalizePicker}
          mode="optional"
          moduleCode={
            selectedContent?.channel_type === 'youtube'
              ? 'youtube_summary'
              : 'feed_summary'
          }
          label={
            selectedContent?.channel_type === 'youtube'
              ? 'YouTube Video Summary Template'
              : 'Feed Summary Template'
          }
          hint={
            selectedContent?.channel_type === 'youtube'
              ? 'Toggle on to use your private youtube_summary template (Edit it in Prompt Templates → My Templates).'
              : 'Toggle on to use your private feed_summary template (Edit it in Prompt Templates → My Templates).'
          }
          disabled={personalizeLoading}
        />
        {!personalizePicker.templateId && (
          <Text type="warning" style={{ display: 'block', marginTop: 8, fontSize: 12 }}>
            ⚠ Toggle the switch above to enable personalized summary using your private template.
          </Text>
        )}
      </Modal>
    </Space>
  );
};

export default FeedArticleBody;
