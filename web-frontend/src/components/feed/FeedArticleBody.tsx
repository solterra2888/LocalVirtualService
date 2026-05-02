import React, { useCallback, useEffect, useRef, useState } from 'react';
import {
  App,
  Avatar,
  Button,
  Divider,
  Modal,
  Space,
  Tag,
  Tooltip,
  Typography,
} from 'antd';
import dayjs from 'dayjs';
import {
  AlignLeftOutlined,
  BulbOutlined,
  CommentOutlined,
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
import YoutubeFeedOriginalBlock from './YoutubeFeedOriginalBlock';
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
// PR-N4: 读取用户对各渠道的自动个性化开关, 用于决定 "Generate / Regenerate" 兜底按钮可见性
import {
  feedSummarySettingsAPI,
  FeedSummaryChannel,
  FeedSummarySettingItem,
} from '../../services/feedSummarySettings';
import AIAnalysisSection from '../shared/content-viewer/AIAnalysisSection';
import AISummarySection from '../shared/content-viewer/AISummarySection';
import CollapsibleOriginalSection from '../shared/content-viewer/CollapsibleOriginalSection';
import NewsOriginalView from '../shared/content-viewer/original-views/NewsOriginalView';

const { Text } = Typography;

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
  /**
   * Desktop feed reader: Original Content starts expanded even with AI Analysis.
   * Omit on mobile article page to keep the tighter default.
   */
  originalContentDefaultExpanded?: boolean;
  /**
   * 视图模式 (由父组件管理以跨内容切换保持选择).
   * 未传入时回退到内部 state, 兼容移动端独立使用场景.
   */
  viewMode?: 'original' | 'analysis';
  onViewModeChange?: (mode: 'original' | 'analysis') => void;
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
  originalContentDefaultExpanded = false,
  viewMode: viewModeProp,
  onViewModeChange,
}) => {
  const { message } = App.useApp();
  const tweetContainerRef = useRef<HTMLDivElement>(null);
  const [originalTweet, setOriginalTweet] = useState<SubscriptionContent | null>(null);
  const [originalTweetLoading, setOriginalTweetLoading] = useState(false);

  // 视图模式: 优先使用父组件传入的受控值; 未传则用内部 state (mobile 兜底)
  const [viewModeInternal, setViewModeInternal] = useState<'original' | 'analysis'>('original');
  const viewMode = viewModeProp ?? viewModeInternal;
  const setViewMode = onViewModeChange ?? setViewModeInternal;

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

  // PR-N4: 用户对各渠道自动个性化的开关 (组件挂载时拉一次, 全生命周期内复用)
  const [feedSummarySettings, setFeedSummarySettings] = useState<
    Record<FeedSummaryChannel, FeedSummarySettingItem | null>
  >({
    twitter: null,
    youtube: null,
  });
  useEffect(() => {
    let cancelled = false;
    feedSummarySettingsAPI
      .list()
      .then((res) => {
        if (cancelled) return;
        const map: Record<FeedSummaryChannel, FeedSummarySettingItem | null> = {
          twitter: null,
          youtube: null,
        };
        for (const it of res.items || []) {
          map[it.channel_type] = it;
        }
        setFeedSummarySettings(map);
      })
      .catch((err) => {
        // 失败时退化为全 null, 按钮按"未开启自动 + 无个性化"逻辑隐藏 / 显示 Generate
        console.warn('Failed to load feed summary settings', err);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const { addTask, tasks } = useFileProcessing();

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
    // viewMode 不在此重置 — 由父组件决定是否跨内容持久化
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

  // PR-7: 个性化 summary
  // localPersonalized 优先 (用户刚生成), 否则回落到 selectedContent 字段
  const personalizedSummary =
    localPersonalized?.summary ?? selectedContent.personalized_summary ?? null;
  const personalizedReasoning =
    localPersonalized?.reasoning ?? selectedContent.personalized_reasoning ?? null;
  const isPersonalized =
    !!personalizedSummary ||
    selectedContent.summary_source === 'personalized';
  // AI Summary 块只展示全局 baseline；个性化内容移至 AIAnalysisSection
  const effectiveSummary = displayTags?.summary || null;

  // PR-N4: 兜底按钮可见性
  // 自动个性化开关 ON  → 全局打标完成后会自动跑; 用户主要看自动结果, 仅在 (a) 还没生成 / 失败  (b) 手动想换模板重跑 时露出按钮.
  // 自动个性化开关 OFF → 默认隐藏; 仅当历史已有 personalized 记录时让用户能 Re-generate.
  const channelSetting =
    channelType === 'twitter' || channelType === 'youtube'
      ? feedSummarySettings[channelType as FeedSummaryChannel]
      : null;
  const autoSummaryEnabled = !!channelSetting?.enabled;
  const showPersonalizeButton =
    showAiTagsPanel && (autoSummaryEnabled || isPersonalized);
  const personalizeButtonLabel = isPersonalized
    ? 'Regenerate'
    : autoSummaryEnabled
    ? 'Generate now'
    : 'Generate';
  const personalizeButtonTooltip = autoSummaryEnabled
    ? (isPersonalized
        ? 'Regenerate this feed\'s personalized summary using your current template.'
        : 'Auto personalization is on but this content has no per-user summary yet — generate it now.')
    : 'Generate a personalized summary for this content (one-off, auto personalization is off).';

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

      {/* ── AI Summary (shell 与 Daily 抽屉 AISummarySection 一致) ── */}
      {showAiTagsPanel && (
        <AISummarySection
          themeMode={themeMode}
          colors={{
            text: themeColors.text,
            textMuted: themeColors.textMuted,
            divider: themeColors.cardBorder,
            cardBg: themeColors.cardBg,
            accent: themeColors.selectedBorder,
          }}
          headerMeta={
            displayTags?.tagged_at || isLowConfidence ? (
              <>
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
              </>
            ) : undefined
          }
          headerActions={
            <Tooltip
              title={
                tagsHasContent
                  ? 'Re-tag — re-generate AI tags + summary (costs LLM tokens)'
                  : 'Tag with AI — generate tags + summary (costs LLM tokens)'
              }
            >
              <Button
                type="text"
                size="small"
                icon={<SyncOutlined spin={retagButtonLoading} />}
                loading={retagButtonLoading}
                onClick={handleRetag}
                aria-label={tagsHasContent ? 'Re-tag' : 'Tag with AI'}
                style={{ fontSize: 15, borderRadius: 6 }}
              />
            </Tooltip>
          }
        >
          {tagsHasContent ? (
            <Space direction="vertical" size={8} style={{ width: '100%' }}>
              {effectiveSummary && (
                <Text
                  style={{
                    color: themeColors.text,
                    fontSize: 13,
                    lineHeight: 1.65,
                    display: 'block',
                    whiteSpace: 'pre-wrap',
                  }}
                >
                  {effectiveSummary}
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
        </AISummarySection>
      )}

      {/* ── AI Analysis Tab — 仅在 viewMode === 'analysis' 时渲染 ── */}
      {showAiTagsPanel && viewMode === 'analysis' && (
        <AIAnalysisSection
          content={personalizedSummary}
          themeMode={themeMode}
          colors={{
            text: themeColors.text,
            textMuted: themeColors.textMuted,
            divider: themeColors.cardBorder,
            cardBg: themeColors.cardBg,
            accent: themeColors.selectedBorder,
          }}
          reasoning={personalizedReasoning}
          actionsSlot={
            showPersonalizeButton || isPersonalized ? (
              <Space size={4}>
                {showPersonalizeButton && (
                  <Tooltip title={personalizeButtonTooltip}>
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
                      {personalizeButtonLabel}
                    </Button>
                  </Tooltip>
                )}
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
            ) : undefined
          }
        />
      )}

      {/* ── Original Content Tab — news 始终显示; twitter/youtube 在 Original 模式下显示 ── */}
      {(!showAiTagsPanel || viewMode === 'original') && (
      <CollapsibleOriginalSection
        hasAIAnalysis={false}
        themeMode={themeMode}
        resetKey={selectedContent.id}
        defaultExpanded={showAiTagsPanel || originalContentDefaultExpanded}
      >
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          {selectedContent.channel_type === 'news' && (
            <NewsOriginalView
              title={selectedContent.title}
              content={selectedContent.content}
              url={selectedContent.url}
              symbols={
                Array.isArray(selectedContent.metadata?.symbols)
                  ? (selectedContent.metadata!.symbols as string[])
                  : []
              }
              categories={selectedContent.categories ?? []}
              sentiment={
                selectedContent.metadata?.sentiment as { polarity?: number } | null
              }
              themeMode={themeMode}
            />
          )}

          {selectedContent.channel_type === 'youtube' && (
            <YoutubeFeedOriginalBlock
              contentId={selectedContent.content_id ?? null}
              url={selectedContent.url ?? undefined}
              title={selectedContent.title ?? null}
              headingTitle={selectedContent.title?.trim() || null}
              description={selectedContent.content || ''}
              contentDbId={selectedContent.id}
              hasTranscript={!!selectedContent.has_transcript}
              metadata={selectedContent.metadata}
              themeMode={themeMode}
              colors={{
                text: themeColors.text,
                textSecondary: themeColors.textSecondary,
                textMuted: themeColors.textMuted,
                divider: themeColors.cardBorder,
                link: themeColors.selectedBorder,
              }}
              embedTopMargin={0}
              noTranscriptHint={false}
            />
          )}

          {selectedContent.channel_type !== 'youtube' && selectedContent.channel_type !== 'news' && (
            <FeedTweetContent
              content={selectedContent}
              themeColors={themeColors}
              isDark={isDark}
              originalTweet={originalTweet}
              originalTweetLoading={originalTweetLoading}
              tweetContainerRef={tweetContainerRef}
            />
          )}

          {selectedContent.channel_type !== 'youtube' && selectedContent.media_urls && selectedContent.media_urls.length > 0 && (
            <div>
              {selectedContent.media_urls.map((url, idx) => (
                <img key={idx} src={url} alt="" style={{ width: '100%', borderRadius: 12, marginBottom: 12 }} />
              ))}
            </div>
          )}

          {selectedContent.channel_type !== 'youtube' && (
            <Space wrap size={16}>
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

          {selectedContent.url && selectedContent.channel_type !== 'youtube' && (
            <a
              href={selectedContent.url}
              target="_blank"
              rel="noopener noreferrer"
              style={{ color: themeColors.selectedBorder }}
            >
              <Space>
                <LinkOutlined />
                {selectedContent.channel_type === 'news'
                  ? 'Read Original Article'
                  : 'View on Twitter'}
              </Space>
            </a>
          )}

          {selectedContent.channel_type === 'youtube' && !selectedContent.has_transcript && (
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
        </Space>
      </CollapsibleOriginalSection>
      )}

      {/*
        PR-N4: Personalize summary 弹窗 (兜底).
        主流程改为齿轮里设开关 + Celery fan-out 自动跑;
        本弹窗仅用于手动 Generate / Regenerate 单条 feed.
      */}
      <Modal
        title={isPersonalized ? 'Regenerate personalized summary' : 'Generate personalized summary'}
        open={personalizeModalOpen}
        onCancel={() => {
          if (!personalizeLoading) {
            setPersonalizeModalOpen(false);
          }
        }}
        onOk={handlePersonalize}
        okText={isPersonalized ? 'Regenerate' : 'Generate'}
        confirmLoading={personalizeLoading}
        cancelText="Cancel"
        width={620}
        destroyOnClose
      >
        <Text type="secondary" style={{ display: 'block', marginBottom: 12, fontSize: 12 }}>
          {autoSummaryEnabled
            ? '一次性手动触发: 用你的私有模板为这一条 feed 生成 per-user 摘要 (其他用户不受影响). 自动批量个性化已在齿轮中开启, 通常无需手动点击.'
            : '一次性手动触发: 用你的私有模板生成 per-user 摘要. 想为该渠道所有新 feed 自动个性化, 请到右上角齿轮中开启.'}
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
          // 变量 (author/title/text/channel/description/transcript) 由后端
          // FeedSummaryPersonalizationService 自动从 subscription_content 行注入,
          // 用户不需要也不应该手动填写, 所以关掉 picker 内的变量编辑器.
          showVariablesEditor={false}
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
