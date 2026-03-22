import React, { useEffect, useState } from 'react';
import { useNavigate, useParams, useLocation } from 'react-router-dom';
import { App, Button, Empty, Spin, Typography } from 'antd';
import { ArrowLeftOutlined } from '@ant-design/icons';
import { useDashboardSettings } from '../contexts/DashboardSettingsContext';
import { useMobileFeeds } from '../contexts/MobileFeedsContext';
import FeedArticleBody from '../components/feed/FeedArticleBody';
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

  const { setContents, selectedSubscription, setSelectedSubscription, subscriptions, loadSubscriptions } =
    useMobileFeeds();

  const [content, setContent] = useState<SubscriptionContent | null>(null);
  const [loading, setLoading] = useState(true);

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

  return (
    <div className="mobile-feeds-article-page" style={{ background: themeColors.cardBg }}>
      <div className="mobile-feeds-article-header">
        <Button type="text" icon={<ArrowLeftOutlined />} onClick={onBack} aria-label="Back" />
        <Text strong style={{ flex: 1, color: themeColors.text }}>
          Article
        </Text>
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
            onTranscriptReady={(id) => {
              setContent((prev) => (prev && prev.id === id ? { ...prev, has_transcript: true } : prev));
              setContents((prev) => prev.map((c) => (c.id === id ? { ...c, has_transcript: true } : c)));
              loadSubscriptions();
            }}
          />
        ) : (
          <Empty description="No content" />
        )}
      </div>
    </div>
  );
};

export default MobileFeedArticlePage;
