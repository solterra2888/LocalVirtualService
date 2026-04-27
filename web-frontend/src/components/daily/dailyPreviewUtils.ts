/**
 * Daily 抽屉预览 — 工具函数.
 *
 * 仅给 DailyPreviewDrawer / DailyOriginalView 内部使用,
 * 与 services/daily.ts 解耦, 避免污染顶层 service 文件.
 */

/**
 * 从任意 YouTube 链接 / 嵌入路径 / 短链中抽出 11 位 video_id.
 * 命中: youtube.com/watch?v=xxx, youtu.be/xxx, /embed/xxx, /shorts/xxx, 直接 11 位 ID.
 * 失败返回 null.
 */
export function extractYouTubeId(input: string | null | undefined): string | null {
  if (!input) return null;
  const s = input.trim();
  // 直接 11 位 ID
  if (/^[a-zA-Z0-9_-]{11}$/.test(s)) return s;
  const patterns = [
    /(?:youtube\.com\/watch\?v=|youtu\.be\/|youtube\.com\/embed\/|youtube\.com\/shorts\/)([a-zA-Z0-9_-]{11})/,
    /youtube\.com\/watch\?.*v=([a-zA-Z0-9_-]{11})/,
  ];
  for (const re of patterns) {
    const m = s.match(re);
    if (m && m[1]) return m[1];
  }
  return null;
}

/**
 * Upload audio / youtube 的 ASR JSON 可能两种形态:
 *   1) 阿里 Paraformer 标准: { sentences: [{ text, ... }, ...] }
 *   2) YouTube 字幕降级: { transcript: [{ text, ... }, ...] }
 * 把它们摊平为简单的 [{ text, t? }] 序列, 用于抽屉里只读浏览.
 */
export interface FlatTranscriptLine {
  text: string;
  /** 起始毫秒, 若有 */
  beginMs?: number;
  /** 说话人编号, 若有 (audio 多人对话才有意义) */
  speakerId?: number;
}

export function flattenAsr(asr: any): FlatTranscriptLine[] {
  if (!asr || typeof asr !== 'object') return [];
  const out: FlatTranscriptLine[] = [];
  if (Array.isArray(asr.sentences)) {
    for (const s of asr.sentences) {
      if (!s) continue;
      const text = (s.text ?? '').toString().trim();
      if (!text) continue;
      out.push({
        text,
        beginMs: typeof s.begin_time === 'number' ? s.begin_time : undefined,
        speakerId: typeof s.speaker_id === 'number' ? s.speaker_id : undefined,
      });
    }
  } else if (Array.isArray(asr.transcript)) {
    for (const s of asr.transcript) {
      if (!s) continue;
      const text = (typeof s === 'string' ? s : s.text ?? '').toString().trim();
      if (!text) continue;
      out.push({
        text,
        beginMs:
          typeof s === 'object' && typeof s.begin_time === 'number'
            ? s.begin_time
            : undefined,
      });
    }
  }
  return out;
}

/** "01:23" / "1:02:45" 形式的时间戳 (毫秒 → 字符串). */
export function formatTimestampMs(ms: number | undefined): string {
  if (typeof ms !== 'number' || !isFinite(ms) || ms < 0) return '';
  const totalSec = Math.floor(ms / 1000);
  const h = Math.floor(totalSec / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  const s = totalSec % 60;
  const pad = (n: number) => n.toString().padStart(2, '0');
  return h > 0 ? `${h}:${pad(m)}:${pad(s)}` : `${m}:${pad(s)}`;
}
