/**
 * 旧版后端曾写入 rc_agent_status = skipped（已废弃）。
 * 用于「是否允许补做 AI 分析」等逻辑时，与 completed 同等对待。
 */
export function normalizeRcAgentStatusForAnalyze(status: string | undefined | null): string {
  if (!status) return '';
  return status === 'skipped' ? 'completed' : status;
}

/**
 * `completed` = 管线跑完但未走通 AI 成功；文案需按首文件类型区分（PDF 不应显示「ASR」）。
 */
export function getCompletedPipelineLabel(firstFileType: string | undefined): {
  short: string;
  detail: string;
  tooltip: string;
} {
  const t = (firstFileType || '').toLowerCase();
  if (t === 'audio') {
    return {
      short: 'ASR complete',
      detail: 'ASR Complete',
      tooltip: 'Transcription complete; AI analysis may still be pending',
    };
  }
  // YouTube：completed 只表示预 AI 管线结束；字幕 API 常失败并改走 Fun-ASR，勿用「Transcript ready」以免暗示已取到官方字幕。
  if (t === 'youtube') {
    return {
      short: 'Processed',
      detail: 'Processing Complete',
      tooltip: 'Pre-AI pipeline finished; AI analysis may still be pending',
    };
  }
  return {
    short: 'Parsed',
    detail: 'Document Parsed',
    tooltip: 'Document parsed; AI analysis may still be pending',
  };
}

/** 与 TaskExecution 兼容的最小字段，供列表/详情统一展示 */
export interface ExecutionStatusDisplayInput {
  rc_agent_status?: string;
  first_file_type?: string;
}

/**
 * PC Sources 列表、移动端列表、ExecutionDetailsPanel 共用：与 rc_agent_status 一一对应的短文案与 tooltip。
 */
export function getExecutionStatusDisplay(execution: ExecutionStatusDisplayInput): {
  tooltip: string;
  label: string;
} {
  const raw = execution.rc_agent_status || '';
  if (raw === 'skipped') {
    return {
      tooltip: 'Legacy: processed without AI analysis',
      label: 'Processed',
    };
  }
  if (!raw) {
    return {
      tooltip: 'Unknown status',
      label: 'Unknown',
    };
  }

  const s = raw;

  switch (s) {
    case 'success':
      return {
        tooltip: 'AI analysis complete',
        label: 'Analyzed',
      };
    case 'failed':
      return {
        tooltip: 'Processing failed',
        label: 'Failed',
      };
    case 'pending':
      return {
        tooltip: 'Pending',
        label: 'Pending',
      };
    case 'uploaded':
      return {
        tooltip: 'Upload only (not analyzed)',
        label: 'Uploaded',
      };
    case 'analyzing':
      return {
        tooltip: 'AI analyzing…',
        label: 'Analyzing…',
      };
    case 'processing':
      return {
        tooltip: 'Processing…',
        label: 'Processing…',
      };
    case 'initializing':
      return {
        tooltip: 'Initializing…',
        label: 'Initializing…',
      };
    case 'completed': {
      const { short, tooltip } = getCompletedPipelineLabel(execution.first_file_type);
      return { tooltip, label: short };
    }
    default:
      return {
        tooltip: s || 'Unknown status',
        label: s || 'Unknown',
      };
  }
}

/** Ant Design Tag `color`，与状态语义一致（用于 PC 侧带色标签） */
export function getRcAgentStatusTagColor(rc: string | undefined): string {
  const r = rc || '';
  if (r === 'skipped') return 'default';
  switch (r) {
    case 'success':
      return 'success';
    case 'failed':
      return 'error';
    case 'uploaded':
      return 'blue';
    case 'analyzing':
    case 'processing':
    case 'initializing':
      return 'warning';
    case 'completed':
      return 'cyan';
    case 'pending':
      return 'processing';
    default:
      return 'default';
  }
}
