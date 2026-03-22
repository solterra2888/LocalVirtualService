import type { SubscriptionContent } from '../../services/subscriptions';

/** New items notification (Web Audio API) */
export function playNewItemsNotificationSound() {
  try {
    const Ctx = window.AudioContext || (window as unknown as { webkitAudioContext?: typeof AudioContext }).webkitAudioContext;
    if (!Ctx) return;
    const ctx = new Ctx();
    const osc = ctx.createOscillator();
    const gain = ctx.createGain();
    osc.connect(gain);
    gain.connect(ctx.destination);
    osc.frequency.value = 800;
    osc.type = 'sine';
    gain.gain.setValueAtTime(0.25, ctx.currentTime);
    gain.gain.exponentialRampToValueAtTime(0.01, ctx.currentTime + 0.15);
    osc.start(ctx.currentTime);
    osc.stop(ctx.currentTime + 0.15);
  } catch {
    // ignore
  }
}

/** Collapse whitespace/newlines for list previews so line-clamp works on YouTube descriptions. */
export function collapseWhitespaceForPreview(s: string): string {
  return (s || '').replace(/\s+/g, ' ').trim();
}

/**
 * List card primary line: YouTube etc. use `title`; Twitter items usually have no title — use tweet text.
 */
export function getFeedListHeadline(c: SubscriptionContent): string {
  const t = (c.title || '').trim();
  if (t) return t;
  if (c.channel_type === 'twitter') {
    const body = (c.content || '').trim().replace(/\s+/g, ' ');
    if (body) return body;
    return c.author_name || (c.author_identifier ? `@${c.author_identifier}` : 'Post');
  }
  return t || 'Untitled';
}

/**
 * Whether to show the secondary excerpt line (`content`). For Twitter without `title`, the headline
 * already shows the tweet — skip the duplicate line.
 */
export function showFeedListSecondaryExcerpt(c: SubscriptionContent): boolean {
  if (c.channel_type === 'twitter' && !(c.title || '').trim()) return false;
  return Boolean((c.content || '').trim());
}

/** Heat level badge config for subscription content */
export function getHeatLevelConfig(heatLevel?: number) {
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
}
