// 文件标签相关类型定义

export interface FileTagPerson {
  name: string;
  name_en?: string;
  role?: string;
  affiliation?: string | null;
}

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
  people?: FileTagPerson[];
  countries?: string[];
  tag_schema_version?: string;
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
  sector_en?: string;
  industry?: string;
  industry_en?: string;
  tickers?: string[];
  concepts?: string[];
  people?: FileTagPerson[];
  countries?: string[];
  tag_schema_version?: string;
  confidence?: number;
  reasoning?: string;
  additional_tags?: Record<string, any>;
}

export interface RetagRequest {
  file_id: number;
  force?: boolean;
  /** PR-5: 用户在 FilesPage Re-tag with template 入口选的模板 */
  template_id?: number | null;
  template_variables?: Record<string, any>;
}

export interface BatchRetagRequest {
  file_ids: number[];
  force?: boolean;
  /** PR-5: 批量入口同样支持运行时模板覆盖 */
  template_id?: number | null;
  template_variables?: Record<string, any>;
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
