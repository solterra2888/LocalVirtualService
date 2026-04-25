/**
 * 全前端统一时间工具.
 *
 * 系统级契约（与 backend/utils/datetime_utils.py 对齐）:
 *   1. 后端 API 输出的 datetime 一律带 UTC tzinfo (ISO 8601 末尾 'Z'/+00:00).
 *   2. 前端展示一律按"用户时区"渲染:
 *        - 默认用浏览器自带的 IANA 时区 (Intl.DateTimeFormat().resolvedOptions().timeZone)
 *        - 用户可通过 DashboardSettingsContext.userTimezone 覆盖
 *   3. "今天 / 昨天" 等日期计算永远在用户时区里完成, 不能依赖 server / UTC 默认.
 *
 * 服务器实际部署在香港 (UTC+8). 美国用户 (PST/EST) 直接用 server local 时区会错位 1 天 —
 * 这是本工具存在的根本原因.
 *
 * 用法:
 *   const tz = useUserTimezone();   // from DashboardSettingsContext
 *   formatDateLabel(item.published_at, tz)        // "April 25, 2026"
 *   formatTime(item.published_at, tz)             // "APR 25 03:22"
 *   dateKeyInTz(item.published_at, tz)            // "2026-04-25"
 *   relativeTime(item.published_at)               // "2 hours ago"
 */

import dayjs, { Dayjs } from 'dayjs';
import utc from 'dayjs/plugin/utc';
import timezone from 'dayjs/plugin/timezone';
import relativeTimePlugin from 'dayjs/plugin/relativeTime';
import localizedFormat from 'dayjs/plugin/localizedFormat';

dayjs.extend(utc);
dayjs.extend(timezone);
dayjs.extend(relativeTimePlugin);
dayjs.extend(localizedFormat);

export const USER_TZ_STORAGE_KEY = 'user-display-timezone';

/** 浏览器自动检测到的 IANA 时区, 失败回退 UTC. */
export function detectBrowserTimezone(): string {
  try {
    const tz = Intl.DateTimeFormat().resolvedOptions().timeZone;
    return tz || 'UTC';
  } catch {
    return 'UTC';
  }
}

/** 把任意 ISO / Date / dayjs 输入归一化为带用户时区的 dayjs 对象. */
export function inUserTz(input: string | Date | Dayjs | null | undefined, tz: string): Dayjs {
  return dayjs.utc(input as any).tz(tz);
}

// ─── Daily 卡片场景常用 formatter ────────────────────────────────────────

/** 日期分组 key (YYYY-MM-DD), 取用户时区的"自然日". 用于 Daily 列表按日分组. */
export function dateKeyInTz(iso: string, tz: string): string {
  return inUserTz(iso, tz).format('YYYY-MM-DD');
}

/** 日期分组的展示标题. zh → "2026年4月25日", en → "April 25, 2026". */
export function formatDateLabel(
  iso: string,
  tz: string,
  locale: 'en' | 'zh' = inferLocale()
): string {
  const d = inUserTz(iso, tz);
  if (locale === 'zh') {
    return `${d.year()}年${d.month() + 1}月${d.date()}日`;
  }
  return d.format('MMMM D, YYYY');
}

/**
 * 卡片内紧凑时间标签.
 *   en (24h): "APR 25 03:22"
 *   en (12h): "APR 25 03:22 AM"
 *   zh:      "4月25日 03:22"
 */
export function formatTime(
  iso: string,
  tz: string,
  opts?: { locale?: 'en' | 'zh'; hour12?: boolean }
): string {
  const locale = opts?.locale ?? inferLocale();
  const hour12 = opts?.hour12 ?? (locale === 'en');
  const d = inUserTz(iso, tz);

  if (locale === 'zh') {
    return `${d.month() + 1}月${d.date()}日 ${d.format('HH:mm')}`;
  }
  const monthDay = d.format('MMM DD').toUpperCase();
  const time = hour12 ? d.format('hh:mm A') : d.format('HH:mm');
  return `${monthDay} ${time}`;
}

/** 相对时间, 例如 "2 hours ago". */
export function relativeTime(iso: string): string {
  return dayjs.utc(iso).fromNow();
}

/** 用户时区里的"今天" / "昨天" date string (YYYY-MM-DD). */
export function todayInTz(tz: string): string {
  return dayjs().tz(tz).format('YYYY-MM-DD');
}
export function yesterdayInTz(tz: string): string {
  return dayjs().tz(tz).subtract(1, 'day').format('YYYY-MM-DD');
}

// ─── 内部 ────────────────────────────────────────────────────────────────

function inferLocale(): 'en' | 'zh' {
  try {
    const lang = (typeof navigator !== 'undefined' && navigator.language) || 'en';
    return lang.toLowerCase().startsWith('zh') ? 'zh' : 'en';
  } catch {
    return 'en';
  }
}
