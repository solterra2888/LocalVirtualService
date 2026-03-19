// 用户相关类型
export interface User {
  id: number;
  username: string;
  is_admin: boolean;
  created_at?: string;
  last_login?: string;
  avatar_url?: string;
}

export interface LoginRequest {
  username: string;
  password: string;
}

export interface LoginResponse {
  access_token: string;
  token_type: string;
  user: User;
}

export interface CreateUserRequest {
  username: string;
  password: string;
  is_admin: boolean;
}

export interface UpdateUserRequest {
  username?: string;
  password?: string;
  is_admin?: boolean;
}

// 文件相关类型
export enum FileType {
  PDF = 'pdf',
  EXCEL = 'excel',
  IMAGE = 'image',
  OTHER = 'other'
}

export enum ProcessingStatus {
  PENDING = 'pending',
  PROCESSING = 'processing',
  COMPLETED = 'completed',
  FAILED = 'failed'
}

export interface FileUploadResponse {
  filename: string;
  file_path: string;
  file_size: number;
  file_type: FileType;
  upload_time: string;
}

export interface FileDetail {
  id: number;
  task_id: string;
  filename: string;
  file_type: FileType;
  file_size: number;
  processing_time?: number;
  ai_time?: number;
  content_length?: number;
  status: ProcessingStatus;
  total_tokens?: number;
  cost?: number;
  error_message?: string;
  created_at: string;
  original_file_path?: string;
}

export interface ProcessingParams {
  language: string[];
  fast_mode: boolean;
  disable_ocr: boolean;
  max_pages: number;
  target_pages?: string;
  skip_diagonal_text: boolean;
  skip_table_extraction: boolean;
  extract_images: boolean;
  use_vendor_multimodal_model: boolean;
  vendor_multimodal_api_key?: string;
  polling_interval: number;
  use_ai_agent: boolean;
  ai_agent_server_url: string;
}

// 任务相关类型
export interface TaskType {
  id: string;
  name: string;
  token: string;
  creator: string;
  created_at: string;
  remarks?: string;
  input_fields: any[];
  is_default: boolean;
}

export interface TaskExecution {
  id: string;
  username: string;
  task_name: string;
  created_at: string;
  files_processed: number;
  files_failed: number;
  total_time: number;
  file_processing_time: number;
  ai_processing_time: number;
  total_tokens?: number;
  total_cost?: number;
  rc_agent_status?: string;
}

export interface TaskCreateRequest {
  name: string;
  token: string;
  remarks?: string;
  input_fields: any[];
}

// API响应类型
export interface ApiResponse<T = any> {
  data?: T;
  message?: string;
  error?: string;
}

// 分页类型
export interface PaginationParams {
  page: number;
  page_size: number;
}

// 应用状态类型
export interface AppState {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  loading: boolean;
}

// 文件处理状态
export interface FileProcessState {
  files: FileUploadResponse[];
  processing: boolean;
  progress: number;
  currentFile?: string;
  logs: string[];
}

// 导出新闻相关类型
export * from './news'; 