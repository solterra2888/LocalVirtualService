// 文件标签相关类型定义

export interface FileTags {
  id: number;
  file_id: number;
  sector: string;
  sector_en?: string;
  industry: string;
  industry_en?: string;
  tickers: string[];
  ticker_raw?: string;
  concepts?: string[];
  additional_tags?: Record<string, any>;
  confidence?: number;
  reasoning?: string;
  tagged_by?: number;
  tagged_at?: string;
  created_at?: string;
  updated_at?: string;
}

export interface FileTagsWithFileInfo extends FileTags {
  filename?: string;
  file_type?: string;
  processing_stats_id?: number;
}

export interface FileTagsUpdate {
  sector?: string;
  industry?: string;
  tickers?: string[];
  confidence?: number;
  reasoning?: string;
  additional_tags?: Record<string, any>;
}

export interface RetagRequest {
  file_id: number;
  force?: boolean;
}

export interface BatchRetagRequest {
  file_ids: number[];
  force?: boolean;
}

export interface TaggingTaskStatus {
  task_id: string;
  status: string;
  progress: number;
  message?: string;
  file_id?: number;
  error?: string;
  result?: FileTags;
}

export interface FileTagsStats {
  total_tagged_files: number;
  sectors: Record<string, number>;
  industries: Record<string, number>;
  top_tickers: Array<{ ticker: string; count: number }>;
  avg_confidence?: number;
}
