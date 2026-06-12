import React, { useCallback, useEffect, useMemo, useState } from 'react';
import { createPortal } from 'react-dom';
import { useNavigate, useParams, useLocation } from 'react-router-dom';
import { App, Button, Empty, Segmented, Spin, Typography } from 'antd';
import { ArrowLeftOutlined, LoadingOutlined } from '@ant-design/icons';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useMobileFeeds } from '../contexts/MobileFeedsContext';
import FeedArticleBody from '../components/feed/FeedArticleBody';
import { type FeedArticleViewMode } from '../components/feed/feedArticleViewMode';
import { FEED_THEME_COLORS } from '../components/feed/feedTheme';
import {
  subscriptionsAPI,
  SubscriptionContent,
} from '../services/subscriptions';
import './MobileFeedsPage.css';

const { Text } = Typography;

async function fetchContentById(
  subscriptionId: number,
  contentId: number
): Promise<SubscriptionContent | null> {
  let offset = 0;
  const limit = 50;
  for (let page = 0; page < 15; page++) {
    const res = await subscriptionsAPI.getSubscriptionContents(subscriptionId, limit, offset, false);
    if (!res.success || !res.data) return null;
    const found = res.data.contents.find((c) => c.id === contentId);
    if (found) return found;
    if (offset + res.data.contents.length >= res.data.total) break;
    offset += res.data.contents.length;
  }
  return null;
}

/**
 * Full-screen article / tweet / YouTube detail with back navigation.
 */
const MobileFeedArticlePage: React.FC = () => {
  const { message } = App.useApp();
  const navigate = useNavigate();
  const location = useLocation();
  const { subscriptionId, contentId } = useParams<{
    subscriptionId: string;
    contentId: string;
  }>();
  const { themeMode } = useDashboardSettings();
  const themeColors = FEED_THEME_COLORS[themeMode];
  const isDark = themeMode === 'dark';

  const {
    setContents,
    selectedSubscription,
    setSelectedSubscription,
    subscriptions,
    loadSubscriptions,
    contents,
    markAsRead,
    loadMoreContents,
    hasMoreContents,
    loadingMore,
  } = useMobileFeeds();

  const [content, setContent] = useState<SubscriptionContent | null>(null);
  const [loading, setLoading] = useState(true);
  /** Twitter / YouTube: Original | AI Analysis | Transcript(YouTube)；与 FeedArticleBody 联动 */
  const [feedViewMode, setFeedViewMode] = useState<FeedArticleViewMode>('original');

  const subId = subscriptionId ? Number(subscriptionId) : NaN;
  const cId = contentId ? Number(contentId) : NaN;

  useEffect(() => {
    if (!Number.isFinite(subId) || !Number.isFinite(cId)) {
      message.error('Invalid link');
      navigate('/feeds', { replace: true });
      return;
    }

    const fromState = (location.state as { content?: SubscriptionContent } | null)?.content;
    if (fromState && fromState.id === cId) {
      setContent(fromState);
      setLoading(false);
      return;
    }

    let cancelled = false;
    setLoading(true);
    fetchContentById(subId, cId).then((c) => {
      if (cancelled) return;
      if (c) {
        setContent(c);
      } else {
        message.warning('Could not load this item');
        navigate('/feeds', { replace: true });
      }
      setLoading(false);
    });

    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- refetch when article route changes
  }, [subId, cId, location.key]);

  /** 切换订阅时回到 Original；同订阅内上/下一条保留当前 Tab（含 Transcript） */
  useEffect(() => {
    setFeedViewMode('original');
  }, [subId]);

  useEffect(() => {
    const el = document.querySelector('.mobile-feeds-article-body');
    if (el instanceof HTMLElement) {
      el.scrollTop = 0;
    }
  }, [cId]);

  useEffect(() => {
    if (!Number.isFinite(subId)) return;
    const sub = subscriptions.find((s) => s.id === subId);
    if (sub && selectedSubscription?.id !== sub.id) {
      setSelectedSubscription(sub);
    }
  }, [subId, subscriptions, selectedSubscription?.id, setSelectedSubscription]);

  const onBack = () => {
    navigate(-1);
  };

  const showFeedViewModeTabs =
    !!content && (content.channel_type === 'twitter' || content.channel_type === 'youtube' || content.channel_type === 'podcast');

  const showTranscriptTabMobile = content?.channel_type === 'youtube' || content?.channel_type === 'podcast';

  const mobileFeedSegmentedOptions = useMemo(() => {
    const opts: { label: string; value: FeedArticleViewMode }[] = [
      { label: 'Original', value: 'original' },
      { label: 'AI Analysis', value: 'analysis' },
    ];
    if (showTranscriptTabMobile) {
      opts.push({ label: 'Transcript', value: 'transcript' });
    }
    return opts;
  }, [showTranscriptTabMobile]);

  useEffect(() => {
    if (!showTranscriptTabMobile && feedViewMode === 'transcript') {
      setFeedViewMode('original');
    }
  }, [content?.id, content?.channel_type, showTranscriptTabMobile, feedViewMode]);

  const listInSync = selectedSubscription?.id === subId;

  const currentIndex = useMemo(() => {
    if (!listInSync || !content) return -1;
    return contents.findIndex((c) => c.id === content.id);
  }, [listInSync, content, contents]);

  const goToContent = useCallback(
    (c: SubscriptionContent) => {
      if (!c.is_read) {
        markAsRead(c.id);
      }
      navigate(`/feeds/article/${subId}/${c.id}`, { state: { content: c }, replace: true });
    },
    [navigate, subId, markAsRead]
  );

  const handleNavNewer = useCallback(() => {
    if (currentIndex <= 0) return;
    const next = contents[currentIndex - 1];
    if (next) goToContent(next);
  }, [currentIndex, contents, goToContent]);

  const handleNavOlder = useCallback(async () => {
    if (currentIndex < 0) return;
    if (currentIndex < contents.length - 1) {
      const next = contents[currentIndex + 1];
      if (next) goToContent(next);
      return;
    }
    if (currentIndex === contents.length - 1 && hasMoreContents && !loadingMore) {
      const batch = await loadMoreContents();
      const first = batch?.[0];
      if (first) goToContent(first);
    }
  }, [currentIndex, contents, hasMoreContents, loadingMore, loadMoreContents, goToContent]);

  const canGoNewer = currentIndex > 0;
  const canGoOlder =
    currentIndex >= 0 &&
    (currentIndex < contents.length - 1 ||
      (currentIndex === contents.length - 1 && hasMoreContents));

  const showArticleNavFabs =
    !loading &&
    !!content &&
    listInSync &&
    contents.length > 0 &&
    currentIndex >= 0;

  const navFabPortalStyle = useMemo(
    () =>
      ({
        ['--nav-fab-text' as string]: themeColors.text,
        ['--nav-fab-muted' as string]: themeColors.textSecondary,
      }) as React.CSSProperties,
    [themeColors.text, themeColors.textSecondary]
  );

  const articleNavFabsPortal =
    showArticleNavFabs &&
    typeof document !== 'undefined' &&
    createPortal(
      <div
        className="mobile-feeds-article-nav-portal"
        data-theme={themeMode}
        style={navFabPortalStyle}
      >
        <div className="mobile-feeds-article-nav-fabs" aria-label="Feed item navigation">
          <span className="mobile-feeds-article-nav-fab-tooltip-wrap">
            <button
              type="button"
              className="mobile-feeds-article-nav-fab"
              disabled={!canGoNewer}
              onClick={handleNavNewer}
              aria-label="Newer in this subscription"
            >
              {'<'}
            </button>
          </span>
          <span className="mobile-feeds-article-nav-fab-tooltip-wrap">
            <button
              type="button"
              className={`mobile-feeds-article-nav-fab${loadingMore ? ' mobile-feeds-article-nav-fab--loading' : ''}`}
              disabled={!canGoOlder || loadingMore}
              onClick={() => void handleNavOlder()}
              aria-label="Older in this subscription"
            >
              {loadingMore ? <LoadingOutlined spin aria-hidden /> : '>'}
            </button>
          </span>
        </div>
      </div>,
      document.body
    );

  return (
    <>
    <div className="mobile-feeds-article-page" style={{ background: themeColors.cardBg }}>
      <div
        className="mobile-feeds-article-header"
        style={
          showFeedViewModeTabs
            ? { flexDirection: 'column', alignItems: 'stretch', gap: 10, paddingBottom: 12 }
            : undefined
        }
      >
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, width: '100%' }}>
          <Button type="text" icon={<ArrowLeftOutlined />} onClick={onBack} aria-label="Back" />
          <Text strong style={{ flex: 1, color: themeColors.text }}>
            Article
          </Text>
        </div>
        {showFeedViewModeTabs && (
          <Segmented
            value={feedViewMode}
            onChange={(v) => setFeedViewMode(v as FeedArticleViewMode)}
            options={mobileFeedSegmentedOptions}
            style={{ width: '100%' }}
            block
          />
        )}
      </div>

      <div className="mobile-feeds-article-body">
        {loading ? (
          <div style={{ display: 'flex', justifyContent: 'center', padding: 48 }}>
            <Spin />
          </div>
        ) : content ? (
          <FeedArticleBody
            selectedContent={content}
            themeColors={themeColors}
            isDark={isDark}
            themeMode={themeMode}
            viewMode={showFeedViewModeTabs ? feedViewMode : undefined}
            onViewModeChange={showFeedViewModeTabs ? setFeedViewMode : undefined}
            onTranscriptReady={(id) => {
              setContent((prev) => (prev && prev.id === id ? { ...prev, has_transcript: true } : prev));
              setContents((prev) => prev.map((c) => (c.id === id ? { ...c, has_transcript: true } : c)));
              loadSubscriptions();
            }}
            onTagsUpdated={(id, tags) => {
              setContent((prev) => (prev && prev.id === id ? { ...prev, tags } : prev));
              setContents((prev) => prev.map((c) => (c.id === id ? { ...c, tags } : c)));
            }}
          />
        ) : (
          <Empty description="No content" />
        )}
      </div>
    </div>
    {articleNavFabsPortal}
    </>
  );
};

export default MobileFeedArticlePage;
