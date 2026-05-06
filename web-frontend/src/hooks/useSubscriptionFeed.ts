import { useCallback, useEffect, useState } from 'react';
import type React from 'react';
import { message } from 'antd';
import {
  subscriptionsAPI,
  Subscription,
  SubscriptionContent,
} from '../services/subscriptions';
import { playNewItemsNotificationSound } from '../components/feed/feedUtils';

const CONTENT_PAGE_SIZE = 20;
const SELECTED_SUB_STORAGE_KEY = 'feeds.mobile.selectedSubscriptionId';

export interface UseSubscriptionFeedOptions {
  enablePolling?: boolean;
  persistSelectedSubscriptionId?: boolean;
}

export function useSubscriptionFeed(options: UseSubscriptionFeedOptions = {}) {
  const { enablePolling = true, persistSelectedSubscriptionId = false } = options;

  const [subscriptions, setSubscriptions] = useState<Subscription[]>([]);
  const [selectedSubscription, setSelectedSubscriptionState] = useState<Subscription | null>(null);
  const [contents, setContents] = useState<SubscriptionContent[]>([]);
  const [loading, setLoading] = useState(false);
  const [contentsLoading, setContentsLoading] = useState(false);
  const [contentOffset, setContentOffset] = useState(0);
  const [contentTotal, setContentTotal] = useState(0);
  const [hasMoreContents, setHasMoreContents] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [onlyUnread, setOnlyUnread] = useState(false);

  const persistId = useCallback((sub: Subscription | null) => {
    if (!persistSelectedSubscriptionId) return;
    try {
      if (sub?.id != null) {
        localStorage.setItem(SELECTED_SUB_STORAGE_KEY, String(sub.id));
      }
    } catch {
      /* ignore */
    }
  }, [persistSelectedSubscriptionId]);

  const setSelectedSubscription = useCallback(
    (value: React.SetStateAction<Subscription | null>) => {
      setSelectedSubscriptionState((prev) => {
        const next = typeof value === 'function' ? (value as (p: Subscription | null) => Subscription | null)(prev) : value;
        persistId(next);
        return next;
      });
    },
    [persistId]
  );

  const loadSubscriptions = useCallback(async () => {
    setLoading(true);
    try {
      const response = await subscriptionsAPI.getUserSubscriptions(undefined, undefined);
      if (response.success && response.data) {
        const data = response.data;
        setSubscriptions(data);

        setSelectedSubscription((prev) => {
          if (prev) {
            const updated = data.find((s) => s.id === prev.id);
            return updated ?? prev;
          }
          let initial: Subscription | null = null;
          if (persistSelectedSubscriptionId) {
            try {
              const raw = localStorage.getItem(SELECTED_SUB_STORAGE_KEY);
              if (raw) {
                const id = Number(raw);
                initial = data.find((s) => s.id === id) ?? null;
              }
            } catch {
              /* ignore */
            }
          }
          if (!initial && data.length > 0) {
            initial = data.find((s) => s.enabled) || data[0];
          }
          return initial;
        });
      } else {
        message.error(response.error || 'Failed to load subscriptions');
      }
    } catch (error) {
      console.error('Failed to load subscriptions:', error);
      message.error('Failed to load subscriptions');
    } finally {
      setLoading(false);
    }
  }, [persistSelectedSubscriptionId, setSelectedSubscription]);

  useEffect(() => {
    loadSubscriptions();
  }, [loadSubscriptions]);

  const loadContents = useCallback(
    async (subscriptionId: number, unreadOnly: boolean = onlyUnread) => {
      setContentsLoading(true);
      try {
        const response = await subscriptionsAPI.getSubscriptionContents(
          subscriptionId,
          CONTENT_PAGE_SIZE,
          0,
          unreadOnly
        );
        if (response.success && response.data) {
          setContents(response.data.contents);
          setContentTotal(response.data.total);
          const len = response.data.contents.length;
          setContentOffset(len);
          setHasMoreContents(len < response.data.total);
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
    },
    [onlyUnread]
  );

  const selectedSubscriptionId = selectedSubscription?.id;
  useEffect(() => {
    if (selectedSubscriptionId != null) {
      loadContents(selectedSubscriptionId, onlyUnread);
    } else {
      setContents([]);
    }
  }, [selectedSubscriptionId, onlyUnread, loadContents]);

  const loadMoreContents = useCallback(async (): Promise<SubscriptionContent[] | null> => {
    if (!selectedSubscription || loadingMore || !hasMoreContents) return null;

    setLoadingMore(true);
    try {
      const response = await subscriptionsAPI.getSubscriptionContents(
        selectedSubscription.id,
        CONTENT_PAGE_SIZE,
        contentOffset,
        false
      );
      if (response.success && response.data) {
        const newContents = response.data.contents;
        const newTotal = response.data.total;
        const nextOffset = contentOffset + newContents.length;
        setContents((prev) => [...prev, ...newContents]);
        setContentOffset(nextOffset);
        setHasMoreContents(nextOffset < newTotal);
        return newContents;
      }
      return null;
    } catch (error) {
      console.error('Failed to load more contents:', error);
      message.error('Failed to load more');
      return null;
    } finally {
      setLoadingMore(false);
    }
  }, [selectedSubscription, loadingMore, hasMoreContents, contentOffset]);

  const markAsRead = useCallback(
    async (contentId: number) => {
      try {
        await subscriptionsAPI.markContentAsRead(contentId, true);
        setContents((prev) => prev.map((c) => (c.id === contentId ? { ...c, is_read: true } : c)));
        if (selectedSubscription?.id != null) {
          const sid = selectedSubscription.id;
          setSubscriptions((prev) =>
            prev.map((s) =>
              s.id === sid ? { ...s, unread_count: Math.max(0, (s.unread_count ?? 0) - 1) } : s
            )
          );
          setSelectedSubscription((prev) =>
            prev ? { ...prev, unread_count: Math.max(0, (prev.unread_count ?? 0) - 1) } : null
          );
        }
      } catch (error) {
        console.error('Failed to mark as read:', error);
      }
    },
    [selectedSubscription?.id, setSelectedSubscription]
  );

  useEffect(() => {
    if (!enablePolling) return;

    const POLL_INTERVAL = 3 * 60 * 1000;
    const intervalId = setInterval(async () => {
      try {
        const response = await subscriptionsAPI.getUserSubscriptions(undefined, true);
        if (response.success && response.data) {
          const newSubscriptions = response.data;

          setSubscriptions((oldSubscriptions) => {
            const hasUnreadChanges = newSubscriptions.some((newSub) => {
              const oldSub = oldSubscriptions.find((s) => s.id === newSub.id);
              return oldSub && oldSub.unread_count !== newSub.unread_count;
            });

            const totalOldUnread = oldSubscriptions.reduce((sum, s) => sum + (s.unread_count || 0), 0);
            const totalNewUnread = newSubscriptions.reduce((sum, s) => sum + (s.unread_count || 0), 0);
            const newUnreadCount = totalNewUnread - totalOldUnread;

            if (newUnreadCount > 0) {
              playNewItemsNotificationSound();
              message.info({
                content: `🔔 ${newUnreadCount} new item${newUnreadCount > 1 ? 's' : ''} available`,
                duration: 5,
              });
            }

            if (hasUnreadChanges) {
              setSelectedSubscription((sel) => {
                if (!sel) return sel;
                const updated = newSubscriptions.find((s) => s.id === sel.id);
                return updated ?? sel;
              });
            }

            return newSubscriptions;
          });
        }
      } catch (error) {
        console.error('Failed to poll subscriptions:', error);
      }
    }, POLL_INTERVAL);

    return () => clearInterval(intervalId);
  }, [enablePolling, setSelectedSubscription]);

  return {
    subscriptions,
    setSubscriptions,
    selectedSubscription,
    setSelectedSubscription,
    contents,
    setContents,
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
  };
}
