import { useEffect, useRef, useCallback } from 'react';

/** Celery 异步生成完成后前端需继续拉取，否则列表/详情会卡在 generating */
export const DAILY_REPORT_POLL_INTERVAL_MS = 2500;
export const DAILY_REPORT_POLL_MAX_TICKS = 360; // ~15 min

type ReportRow = { status: string; report_date: string };

export function useDailyReportStatusPoll() {
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const clearPoll = useCallback(() => {
    if (intervalRef.current !== null) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
  }, []);

  useEffect(() => () => clearPoll(), [clearPoll]);

  const startPoll = useCallback(
    (
      dateStr: string,
      opts: {
        fetchReport: (d: string) => Promise<{ success: boolean; data?: ReportRow | null }>;
        refreshList: () => Promise<void>;
        syncDetailIfViewing: () => boolean;
        setDetail: (r: ReportRow) => void;
        onDone: () => void;
      }
    ) => {
      clearPoll();
      let ticks = 0;

      const tick = async () => {
        ticks += 1;
        if (ticks > DAILY_REPORT_POLL_MAX_TICKS) {
          clearPoll();
          opts.onDone();
          return;
        }
        try {
          const r = await opts.fetchReport(dateStr);
          if (!r.success || !r.data) return;
          await opts.refreshList();
          if (opts.syncDetailIfViewing()) {
            opts.setDetail(r.data);
          }
          const st = r.data.status;
          if (st === 'completed' || st === 'failed') {
            clearPoll();
            opts.onDone();
          }
        } catch {
          /* 单次轮询失败忽略，下次继续 */
        }
      };

      void tick();
      intervalRef.current = setInterval(() => void tick(), DAILY_REPORT_POLL_INTERVAL_MS);
    },
    [clearPoll]
  );

  return { startPoll, clearPoll };
}
