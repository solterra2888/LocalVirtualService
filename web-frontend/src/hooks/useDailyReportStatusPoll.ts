import { useEffect, useRef, useCallback } from 'react';

/** Celery 异步生成完成后前端需继续拉取，否则列表/详情会卡在 generating */
export const DAILY_REPORT_POLL_INTERVAL_MS = 2500;
export const DAILY_REPORT_POLL_MAX_TICKS = 360; // ~15 min
/** Scheduler / 后台生成完成时，页面静默轮询最新报告 */
export const DAILY_REPORT_BACKGROUND_POLL_INTERVAL_MS = 10000;

type ReportRow = { status: string; report_date: string; updated_at?: string };

function reportSnapshot(r: ReportRow): string {
  return `${r.report_date}|${r.status}|${r.updated_at ?? ''}`;
}

export function useDailyReportStatusPoll() {
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const backgroundIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const lastStatusRef = useRef<string | null>(null);
  const backgroundSnapshotRef = useRef<string | null>(null);

  const clearPoll = useCallback(() => {
    if (intervalRef.current !== null) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
    lastStatusRef.current = null;
  }, []);

  const clearLatestWatch = useCallback(() => {
    if (backgroundIntervalRef.current !== null) {
      clearInterval(backgroundIntervalRef.current);
      backgroundIntervalRef.current = null;
    }
    backgroundSnapshotRef.current = null;
  }, []);

  useEffect(() => () => {
    clearPoll();
    clearLatestWatch();
  }, [clearPoll, clearLatestWatch]);

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

          const st = r.data.status;
          const statusChanged = lastStatusRef.current !== st;

          // Only update detail if status changed or report completed/failed
          if (statusChanged || st === 'completed' || st === 'failed') {
            if (opts.syncDetailIfViewing()) {
              opts.setDetail(r.data);
            }
            lastStatusRef.current = st;
            backgroundSnapshotRef.current = reportSnapshot(r.data);
          }

          if (st === 'completed' || st === 'failed') {
            // Final refresh when done
            await opts.refreshList();
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

  /** 页面打开时监听最新报告（Scheduler 后台生成完成后自动刷新） */
  const startLatestWatch = useCallback(
    (opts: {
      fetchLatest: () => Promise<{ success: boolean; data?: ReportRow | null }>;
      onUpdate: (r: ReportRow) => void;
      getInitialSnapshot?: () => string | null;
    }) => {
      clearLatestWatch();
      backgroundSnapshotRef.current = opts.getInitialSnapshot?.() ?? null;

      const tick = async () => {
        if (intervalRef.current !== null) return;
        if (document.hidden) return;

        try {
          const res = await opts.fetchLatest();
          if (!res.success || !res.data) return;

          const key = reportSnapshot(res.data);
          if (backgroundSnapshotRef.current === null) {
            backgroundSnapshotRef.current = key;
            return;
          }
          if (key !== backgroundSnapshotRef.current) {
            backgroundSnapshotRef.current = key;
            opts.onUpdate(res.data);
          }
        } catch {
          /* 单次轮询失败忽略 */
        }
      };

      void tick();
      backgroundIntervalRef.current = setInterval(
        () => void tick(),
        DAILY_REPORT_BACKGROUND_POLL_INTERVAL_MS
      );
    },
    [clearLatestWatch]
  );

  const syncLatestSnapshot = useCallback((r: ReportRow | null) => {
    backgroundSnapshotRef.current = r ? reportSnapshot(r) : null;
  }, []);

  return { startPoll, clearPoll, startLatestWatch, clearLatestWatch, syncLatestSnapshot };
}
