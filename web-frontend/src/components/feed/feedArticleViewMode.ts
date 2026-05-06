/**
 * Shared view mode for Feeds reader + Daily preview drawer:
 * Original | AI Analysis | Transcript (YouTube only).
 */

export type FeedArticleViewMode = 'original' | 'analysis' | 'transcript';

/** Cycle through enabled tabs in order (skip disabled analysis when analysisEnabled is false). */
export function adjacentFeedArticleViewMode(
  current: FeedArticleViewMode,
  direction: 1 | -1,
  analysisEnabled: boolean,
  showTranscriptTab: boolean
): FeedArticleViewMode {
  const order: FeedArticleViewMode[] = ['original'];
  if (analysisEnabled) order.push('analysis');
  if (showTranscriptTab) order.push('transcript');
  const idx = order.indexOf(current);
  const start = idx >= 0 ? idx : 0;
  const len = order.length;
  if (len === 0) return 'original';
  let nextIdx = (start + direction) % len;
  if (nextIdx < 0) nextIdx += len;
  return order[nextIdx];
}
