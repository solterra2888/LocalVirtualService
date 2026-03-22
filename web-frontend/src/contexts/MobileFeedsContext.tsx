import React, { createContext, useContext } from 'react';
import { useSubscriptionFeed } from '../hooks/useSubscriptionFeed';

const MobileFeedsContext = createContext<ReturnType<typeof useSubscriptionFeed> | null>(null);

export function MobileFeedsProvider({ children }: { children: React.ReactNode }) {
  const value = useSubscriptionFeed({
    persistSelectedSubscriptionId: true,
    enablePolling: true,
  });
  return <MobileFeedsContext.Provider value={value}>{children}</MobileFeedsContext.Provider>;
}

export function useMobileFeeds() {
  const ctx = useContext(MobileFeedsContext);
  if (!ctx) {
    throw new Error('useMobileFeeds must be used within MobileFeedsProvider');
  }
  return ctx;
}
