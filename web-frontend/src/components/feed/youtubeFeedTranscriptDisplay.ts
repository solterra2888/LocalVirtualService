/**
 * Normalize subscription_content.transcript_data for Feed / Daily YouTube blocks.
 * — YouTube captions: { transcript: [{ text, start, duration }] }
 * — Fun-ASR feed: { asr_data: { sentences: [{ text, begin_time, end_time, speaker_id }] } }
 */

export type YoutubeFeedTranscriptRow = {
  key: string;
  /** Seconds from start of media (YouTube) or from begin_time (ASR ms → s). */
  startSec: number;
  text: string;
  speakerLabel?: string;
};

function formatTimestamp(sec: number): string {
  if (!Number.isFinite(sec) || sec < 0) sec = 0;
  const total = Math.floor(sec);
  const s = total % 60;
  const m = Math.floor(total / 60) % 60;
  const h = Math.floor(total / 3600);
  if (h > 0) {
    return `${h}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
  }
  return `${m}:${String(s).padStart(2, '0')}`;
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

/**
 * Returns rows derived from transcript_data when structured payload exists; otherwise null
 * (caller falls back to flat `transcript` string).
 */
export function rowsFromSubscriptionTranscriptData(
  transcriptData: unknown
): YoutubeFeedTranscriptRow[] | null {
  if (!isRecord(transcriptData)) return null;

  const yt = transcriptData.transcript;
  if (Array.isArray(yt) && yt.length > 0) {
    const rows: YoutubeFeedTranscriptRow[] = [];
    yt.forEach((seg, i) => {
      if (!isRecord(seg)) return;
      const text = typeof seg.text === 'string' ? seg.text : '';
      if (!text.trim()) return;
      const start = typeof seg.start === 'number' ? seg.start : Number(seg.start) || 0;
      rows.push({
        key: `yt-${i}-${start}`,
        startSec: start,
        text: text.trim(),
      });
    });
    return rows.length > 0 ? rows : null;
  }

  const asrRaw = transcriptData.asr_data;
  if (!isRecord(asrRaw)) return null;
  const sentences = asrRaw.sentences;
  if (!Array.isArray(sentences) || sentences.length === 0) return null;

  const speakers = Array.isArray(asrRaw.speakers) ? asrRaw.speakers : [];
  const speakerMap = new Map<number, string>();
  for (const s of speakers) {
    if (isRecord(s) && typeof s.speaker_id === 'number') {
      const label = typeof s.label === 'string' ? s.label : `Speaker ${s.speaker_id}`;
      speakerMap.set(s.speaker_id, label);
    }
  }

  const rows: YoutubeFeedTranscriptRow[] = [];
  sentences.forEach((sent, i) => {
    if (!isRecord(sent)) return;
    const text = typeof sent.text === 'string' ? sent.text : '';
    if (!text.trim()) return;
    const beginMs =
      typeof sent.begin_time === 'number' ? sent.begin_time : Number(sent.begin_time) || 0;
    const startSec = beginMs / 1000;
    const sid = sent.speaker_id;
    let speakerLabel: string | undefined;
    if (typeof sid === 'number' && speakerMap.has(sid)) {
      speakerLabel = speakerMap.get(sid);
    } else if (typeof sid === 'number') {
      speakerLabel = `Speaker ${sid}`;
    }
    rows.push({
      key: `asr-${i}-${beginMs}`,
      startSec,
      text: text.trim(),
      speakerLabel,
    });
  });

  return rows.length > 0 ? rows : null;
}

export function formatTranscriptTime(startSec: number): string {
  return formatTimestamp(startSec);
}
