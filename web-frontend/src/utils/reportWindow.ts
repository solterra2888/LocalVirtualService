import dayjs from 'dayjs';

/** Format rolling report window from metadata (UTC ISO strings). */
export function formatReportWindowRange(metadata?: {
  window_start?: string;
  window_end?: string;
}): string | null {
  const start = metadata?.window_start;
  const end = metadata?.window_end;
  if (!start || !end) return null;

  const startLocal = dayjs(start);
  const endLocal = dayjs(end);
  if (!startLocal.isValid() || !endLocal.isValid()) return null;

  return `${startLocal.format('MMM D, YYYY HH:mm')} – ${endLocal.format('MMM D, YYYY HH:mm')} (local)`;
}
