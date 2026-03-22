import { api } from './api';

// 文件类型定义
export interface FileUploadResponse {
  filename: string;
  file_path: string;
  file_size: number;
  file_type: string;
  upload_time: string;
}

export interface FileProcessRequest {
  file_paths: string[];
  task_type: string;
  custom_inputs: Record<string, any>;
  params: Record<string, any>;
  // 新增：分析模式相关字段
  analysis_mode?: 'remote_agent' | 'local_llm';
  template_id?: number;
  template_code?: string;
  template_variables?: Record<string, any>;
  llm_provider?: string;
  llm_model?: string;
}

export interface FileProcessResponse {
  task_id: string;
  message: string;
  files_count: number;
}

export interface TaskStatusResponse {
  task_id?: string;
  status: string;
  progress: number;
  message?: string;
  current_file?: string;
  files_processed: number;
  files_failed?: number;
  total_files: number;
  logs: Array<{
    timestamp: string;
    level: string;
    message: string;
  }>;
  result?: any;
  /** YouTube：无官方字幕，已切换 Fun-ASR（前端可弹窗提示用户等待） */
  asr_fallback_notice?: boolean;
  /** 音频上传：已提交 Fun-ASR，等待结果（前端可弹窗提示耐心等待） */
  asr_wait_notice?: boolean;
}

export interface FileDetail {
  id: number;
  task_id: string;
  filename: string;
  file_type: string;
  file_size: number;
  processing_time?: number;
  ai_time?: number;
  content_length?: number;
  status: string;
  total_tokens?: number;
  cost?: number;
  error_message?: string;
  created_at: string;
  original_file_path?: string;
  access_type?: string;
  shared_by?: string;
}

export interface FileValidationResponse {
  valid_files: string[];
  invalid_files: Array<{
    path: string;
    reason: string;
  }>;
  summary: {
    types: Record<string, { count: number; size: number }>;
    total_files: number;
    total_size_mb: number;
  };
}

export interface ProcessingParamsOptions {
  language_options: Array<{
    value: string[];
    label: string;
  }>;
  boolean_options: Array<{
    key: string;
    label: string;
    default: boolean;
  }>;
  numeric_options: Array<{
    key: string;
    label: string;
    default: number;
    min: number;
    max: number;
  }>;
  text_options: Array<{
    key: string;
    label: string;
    placeholder?: string;
    type?: string;
    default?: string;
  }>;
  defaults?: {
    ai_agent_server_url: string;
  };
}

export interface SupportedFileTypesResponse {
  supported_extensions: string[];
  descriptions: Record<string, string>;
}

export interface SectorOption {
  value: string;
  label: string;
  children: Array<{
    value: string;
    label: string;
  }>;
}

export interface TickerOption {
  id: number;
  code: string;
  name: string;
  exchange: string;
  sector: string;
  industry: string;
  label: string;
  value: string;
}

export interface FileTag {
  id: number;
  file_id: number;
  sector: string;
  industry: string;
  ticker?: string;
  tagged_by?: number;
  tagged_at: string;
  created_at: string;
  updated_at: string;
}

export interface FileTagCreate {
  file_id: number;
  sector: string;
  industry: string;
  ticker?: string;
}

export interface FileTagBatchCreate {
  file_ids: number[];
  sector: string;
  industry: string;
  ticker?: string;
}

export const filesAPI = {
  // 上传文件
  uploadFiles: async (files: File[]): Promise<FileUploadResponse[]> => {
    const formData = new FormData();
    files.forEach(file => {
      formData.append('files', file);
    });
    
    return api.upload<FileUploadResponse[]>('/files/upload', formData);
  },

  // 上传音频文件到 OSS（异步，立即返回任务 ID）
  uploadAudioFile: async (file: File): Promise<{
    success: boolean;
    message: string;
    task_id: string;
    filename: string;
    file_size: number;
    file_size_mb: number;
    status: string;
    note?: string;
  }> => {
    const formData = new FormData();
    formData.append('file', file);
    
    return api.upload('/files/upload-audio', formData);
  },

  // 上传 YouTube 链接并异步处理
  uploadYouTubeLink: async (youtubeUrl: string): Promise<{
    task_id: string;
    video_id: string;
    message: string;
  }> => {
    return api.post<{
      task_id: string;
      video_id: string;
      message: string;
    }>('/files/upload-youtube', {
      youtube_url: youtubeUrl
    });
  },

  // 上传文档并异步处理（推荐，避免 OSS 慢连接阻塞）
  uploadAndProcessDocument: async (
    file: File,
    options: {
      analysis_mode?: 'remote_agent' | 'local_llm';
      task_type?: string;
      template_id?: number;
      template_variables?: Record<string, any>;
      custom_inputs?: Record<string, any>;
      params?: Record<string, any>;
    }
  ): Promise<{
    success: boolean;
    message: string;
    task_id: string;
    filename: string;
    file_size: number;
    file_size_mb: number;
    status: string;
    note?: string;
  }> => {
    const formData = new FormData();
    formData.append('file', file);
    formData.append('analysis_mode', options.analysis_mode || 'local_llm');
    if (options.task_type) {
      formData.append('task_type', options.task_type);
    }
    if (options.template_id) {
      formData.append('template_id', options.template_id.toString());
    }
    formData.append('template_variables', JSON.stringify(options.template_variables || {}));
    formData.append('custom_inputs', JSON.stringify(options.custom_inputs || {}));
    formData.append('params', JSON.stringify(options.params || {}));
    
    return api.upload('/files/upload-and-process-document', formData);
  },

  // 获取支持的音频格式
  getSupportedAudioFormats: async (): Promise<{
    supported_formats: string[];
    max_file_size_gb: number;
    max_duration_hours: number;
    description: string;
  }> => {
    return api.get('/files/audio/supported-formats');
  },

  // 检查是否为音频文件
  isAudioFile: (filename: string): boolean => {
    const audioExtensions = [
      'aac', 'amr', 'avi', 'flac', 'flv', 'm4a', 'mkv', 'mov',
      'mp3', 'mp4', 'mpeg', 'ogg', 'opus', 'wav', 'webm', 'wma', 'wmv'
    ];
    const ext = filesAPI.getFileExtension(filename);
    return audioExtensions.includes(ext);
  },

  // 语音识别 - Fun-ASR（同步，等待结果）
  transcribeAudio: async (params: {
    audio_url: string;
    model?: string;
    language_hints?: string[];
    wait_for_result?: boolean;
    diarization_enabled?: boolean;
  }): Promise<{
    success: boolean;
    task_id: string;
    status: string;
    text: string;
    formatted_text: string;
    transcriptions: any[];
  }> => {
    return api.post('/files/audio/transcribe', {
      audio_url: params.audio_url,
      model: params.model || 'fun-asr',
      language_hints: params.language_hints || ['zh', 'en'],
      wait_for_result: params.wait_for_result !== false,
      diarization_enabled: params.diarization_enabled !== false
    });
  },

  // 提交语音识别任务（异步，不等待）
  submitTranscriptionTask: async (params: {
    audio_url: string;
    model?: string;
    language_hints?: string[];
    diarization_enabled?: boolean;
  }): Promise<{
    success: boolean;
    task_id: string;
    message: string;
  }> => {
    return api.post('/files/audio/transcribe-async', {
      audio_url: params.audio_url,
      model: params.model || 'fun-asr',
      language_hints: params.language_hints || ['zh', 'en'],
      diarization_enabled: params.diarization_enabled !== false
    });
  },

  // 查询语音识别结果
  getTranscriptionResult: async (taskId: string): Promise<{
    success: boolean;
    task_id: string;
    status: string;
    text?: string;
    formatted_text?: string;
    transcriptions?: any[];
    message?: string;
  }> => {
    return api.get(`/files/audio/transcribe/${taskId}`);
  },

  // 获取支持的文件类型
  getSupportedFileTypes: async (): Promise<SupportedFileTypesResponse> => {
    return api.get<SupportedFileTypesResponse>('/files/supported-types');
  },

  // 验证文件
  validateFiles: async (filePaths: string[]): Promise<FileValidationResponse> => {
    return api.post<FileValidationResponse>('/files/validate', { file_paths: filePaths });
  },

  // 处理文件
  processFiles: async (request: FileProcessRequest): Promise<FileProcessResponse> => {
    return api.post<FileProcessResponse>('/files/process', request);
  },

  // 获取任务状态
  getTaskStatus: async (taskId: string): Promise<TaskStatusResponse> => {
    return api.get<TaskStatusResponse>(`/files/tasks/${taskId}/status`);
  },

  // 停止任务
  stopTask: async (taskId: string): Promise<{ message: string }> => {
    return api.post<{ message: string }>(`/files/tasks/${taskId}/stop`);
  },

  // 获取任务文件详情
  getTaskFiles: async (taskId: string): Promise<FileDetail[]> => {
    return api.get<FileDetail[]>(`/files/tasks/${taskId}/files`);
  },

  // 删除上传的文件
  deleteUploadedFile: async (filename: string): Promise<{ message: string }> => {
    return api.delete<{ message: string }>(`/files/uploads/${filename}`);
  },

  // 重新排序文件
  reorderFiles: async (filePaths: string[]): Promise<{
    message: string;
    ordered_files: string[];
  }> => {
    return api.post('/files/reorder', { file_paths: filePaths });
  },

  // 获取处理参数选项
  getProcessingParams: async (): Promise<ProcessingParamsOptions> => {
    return api.get<ProcessingParamsOptions>('/files/processing-params');
  },

  // 获取行业分类
  getSectors: async (): Promise<SectorOption[]> => {
    return api.get<SectorOption[]>('/files/sectors');
  },

  // 搜索股票代码
  searchTickers: async (params: {
    sector?: string;
    industry?: string;
    search?: string;
  }): Promise<TickerOption[]> => {
    const queryParams = new URLSearchParams();
    if (params.sector) queryParams.append('sector', params.sector);
    if (params.industry) queryParams.append('industry', params.industry);
    if (params.search) queryParams.append('search', params.search);
    
    return api.get<TickerOption[]>(`/files/tickers?${queryParams.toString()}`);
  },

  // 文件标签相关API
  // 为文件设置标签
  createFileTag: async (fileId: number, tagData: Omit<FileTagCreate, 'file_id'>): Promise<FileTag> => {
    return api.post<FileTag>(`/files/${fileId}/tags`, {
      file_id: fileId,
      ...tagData
    });
  },

  // 获取文件标签
  getFileTag: async (fileId: number): Promise<FileTag> => {
    return api.get<FileTag>(`/files/${fileId}/tags`);
  },

  // 更新文件标签
  updateFileTag: async (fileId: number, tagData: Omit<FileTagCreate, 'file_id'>): Promise<FileTag> => {
    return api.put<FileTag>(`/files/${fileId}/tags`, tagData);
  },

  // 删除文件标签
  deleteFileTag: async (fileId: number): Promise<{ message: string }> => {
    return api.delete<{ message: string }>(`/files/${fileId}/tags`);
  },

  // 批量设置文件标签
  batchCreateFileTags: async (batchData: FileTagBatchCreate): Promise<{
    message: string;
    success_count: number;
    failed_count: number;
    failed_files: any[];
  }> => {
    return api.post(`/files/batch-tag`, batchData);
  },

  // 获取原始文件信息
  getOriginalFileInfo: async (fileId: number): Promise<{
    filename: string;
    file_type: string;
    file_size_mb: number;
    original_file_path: string;
    file_url: string;
    content?: string;
    asr_with_diarization?: {
      properties: {
        duration_ms: number;
        audio_format: string;
        channels: number[];
        sampling_rate: number;
      };
      speakers: Array<{
        speaker_id: number;
        label: string;
      }>;
      speaker_count: number;
      sentences: Array<{
        sentence_id: number;
        begin_time: number;
        end_time: number;
        text: string;
        speaker_id: number;
      }>;
      total_sentences: number;
    };
  }> => {
    return api.get<{
      filename: string;
      file_type: string;
      file_size_mb: number;
      original_file_path: string;
      file_url: string;
      content?: string;
      asr_with_diarization?: {
        properties: {
          duration_ms: number;
          audio_format: string;
          channels: number[];
          sampling_rate: number;
        };
        speakers: Array<{
          speaker_id: number;
          label: string;
        }>;
        speaker_count: number;
        sentences: Array<{
          sentence_id: number;
          begin_time: number;
          end_time: number;
          text: string;
          speaker_id: number;
        }>;
        total_sentences: number;
      };
    }>(`/files/original/${fileId}`);
  },

  // 更新 file_processing_details 的 content（Content 标签页编辑后保存）
  updateFileDetailContent: async (fileId: number, content: string): Promise<{ success: boolean; message: string }> => {
    return api.patch<{ success: boolean; message: string }>(`/files/details/${fileId}/content`, { content });
  },

  // 获取支持的文件类型扩展名（与 backend/core/llama_service.py get_supported_extensions 一致）
  getSupportedFileExtensions: (): string[] => {
    return [
      '.pdf',
      '.xlsx', '.xls', '.csv',
      '.doc', '.docx',
      '.jpg', '.jpeg', '.png', '.gif', '.bmp'
    ];
  },

  // 用于 <input accept> 的字符串，仅可选支持格式
  getAcceptFileTypes: (): string => {
    return filesAPI.getSupportedFileExtensions().join(',');
  },

  // 验证文件类型
  validateFileType: (file: File): boolean => {
    const supportedTypes = filesAPI.getSupportedFileExtensions();
    const fileName = file.name.toLowerCase();
    return supportedTypes.some(type => fileName.endsWith(type));
  },

  // 格式化文件大小
  formatFileSize: (bytes: number): string => {
    if (bytes === 0) return '0 Bytes';
    
    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  },

  // 获取文件类型图标
  getFileTypeIcon: (fileType: string): string => {
    switch (fileType.toLowerCase()) {
      case 'pdf':
        return 'file-pdf';
      case 'excel':
        return 'file-excel';
      case 'doc':
        return 'file-word';
      case 'image':
        return 'file-image';
      default:
        return 'file';
    }
  },

  // 获取文件扩展名
  getFileExtension: (filename: string): string => {
    return filename.split('.').pop()?.toLowerCase() || '';
  },

  // 检查文件是否为图片
  isImageFile: (filename: string): boolean => {
    const imageExtensions = ['jpg', 'jpeg', 'png', 'gif', 'bmp'];
    const ext = filesAPI.getFileExtension(filename);
    return imageExtensions.includes(ext);
  },

  // 检查文件是否为PDF
  isPdfFile: (filename: string): boolean => {
    return filesAPI.getFileExtension(filename) === 'pdf';
  },

  // 检查文件是否为Excel
  isExcelFile: (filename: string): boolean => {
    const excelExtensions = ['xlsx', 'xls'];
    const ext = filesAPI.getFileExtension(filename);
    return excelExtensions.includes(ext);
  },

  // 检查文件是否为DOC
  isDocFile: (filename: string): boolean => {
    const docExtensions = ['doc', 'docx'];
    const ext = filesAPI.getFileExtension(filename);
    return docExtensions.includes(ext);
  },

  // 获取文件类型标签颜色
  getFileTypeColor: (fileType: string): string => {
    switch (fileType.toLowerCase()) {
      case 'pdf':
        return 'red';
      case 'excel':
        return 'green';
      case 'doc':
        return 'blue';
      case 'image':
        return 'orange';
      default:
        return 'default';
    }
  },

  // 批量删除文件
  deleteFiles: async (filePaths: string[]): Promise<{ message: string }> => {
    const promises = filePaths.map(path => {
      const filename = path.split('/').pop() || '';
      return filesAPI.deleteUploadedFile(filename);
    });
    
    await Promise.all(promises);
    return { message: '文件删除成功' };
  },

  // 获取文件信息
  getFileInfo: (file: File): {
    name: string;
    size: number;
    type: string;
    extension: string;
    isSupported: boolean;
  } => {
    const extension = filesAPI.getFileExtension(file.name);
    return {
      name: file.name,
      size: file.size,
      type: file.type,
      extension,
      isSupported: filesAPI.validateFileType(file)
    };
  },

  // 创建文件预览URL
  createPreviewUrl: (file: File): string => {
    if (filesAPI.isImageFile(file.name)) {
      return URL.createObjectURL(file);
    }
    return '';
  },

  // 释放文件预览URL
  revokePreviewUrl: (url: string): void => {
    if (url) {
      URL.revokeObjectURL(url);
    }
  },

  // 更新说话人标签
  updateSpeakerLabel: async (fileId: string, speakerId: number, newLabel: string): Promise<{ success: boolean; message: string }> => {
    // api.patch 已通过 extractData 返回 response.data，勿再 .data
    return api.patch(`/files/${fileId}/speaker/${speakerId}?new_label=${encodeURIComponent(newLabel)}`);
  },

  /** 更新 Original 面板中单条转写（同步 asr_with_diarization + content） */
  updateAsrSegmentText: async (
    fileId: number | string,
    body: { text: string; segment_kind: 'audio' | 'youtube'; index: number }
  ): Promise<{ success: boolean; message: string; content_length?: number }> => {
    return api.patch(`/files/details/${fileId}/asr-segment`, body);
  },

  /** 删除 Original 面板中单条转写（同步 asr_with_diarization + content） */
  deleteAsrSegment: async (
    fileId: number | string,
    body: { segment_kind: 'audio' | 'youtube'; index: number }
  ): Promise<{ success: boolean; message: string; content_length?: number }> => {
    return api.delete(`/files/details/${fileId}/asr-segment`, { data: body });
  },

  /** 批量替换转写文本（全部句子/分段中 find -> replace，同步 content） */
  replaceAsrText: async (
    fileId: number | string,
    body: { find: string; replace: string }
  ): Promise<{ success: boolean; message: string; replace_count?: number; content_length?: number }> => {
    let data = await api.patch(`/files/details/${String(fileId)}/asr-replace`, body);
    if (typeof data === 'string') {
      try {
        data = JSON.parse(data);
      } catch {
        return { success: false, message: '', replace_count: 0 };
      }
    }
    if (data != null && typeof data === 'object') {
      return data as { success: boolean; message: string; replace_count?: number; content_length?: number };
    }
    return { success: false, message: '', replace_count: 0 };
  },

  // 音频AI分析 - 提交分析任务
  analyzeAudio: async (params: {
    execution_id: number;
    template_id?: number;
    template_variables?: Record<string, any>;
  }): Promise<{
    success: boolean;
    task_id: string;
    execution_id: number;
    message: string;
  }> => {
    return api.post('/files/audio/analyze', params);
  },

  // 音频AI分析 - 查询任务状态
  getAudioAnalysisStatus: async (taskId: string): Promise<{
    task_id: string;
    status: string;
    progress: number;
    message: string;
    execution_id?: number;
    total_tokens?: number;
    total_price?: number;
    llm_model?: string;
    llm_provider?: string;
    logs?: Array<{ timestamp: string; level: string; message: string }>;
  }> => {
    return api.get(`/files/audio/analyze/${taskId}/status`);
  },

  // 文档AI分析 - 提交分析任务
  analyzeDocument: async (params: {
    execution_id: number;
    template_id: number;
    template_variables?: Record<string, any>;
  }): Promise<{
    success: boolean;
    task_id: string;
    execution_id: number;
    message: string;
  }> => {
    return api.post('/files/document/analyze', params);
  },

  // 文档AI分析 - 查询任务状态
  getDocumentAnalysisStatus: async (taskId: string): Promise<{
    task_id: string;
    status: string;
    progress: number;
    message: string;
    execution_id?: number;
    total_tokens?: number;
    total_price?: number;
    llm_model?: string;
    llm_provider?: string;
    logs?: Array<{ timestamp: string; level: string; message: string }>;
  }> => {
    return api.get(`/files/document/analyze/${taskId}/status`);
  },

  // 文档重新解析（Re-parse）- 提交重新解析任务
  reparseDocument: async (params: {
    file_id: number;
    params?: {
      language?: string[];
      fast_mode?: boolean;
      disable_ocr?: boolean;
      max_pages?: number;
      target_pages?: string;
      skip_diagonal_text?: boolean;
      skip_table_extraction?: boolean;
      extract_images?: boolean;
      use_vendor_multimodal_model?: boolean;
      vendor_multimodal_api_key?: string;
      polling_interval?: number;
    };
  }): Promise<{
    success: boolean;
    task_id: string;
    file_id: number;
    filename: string;
    message: string;
    note?: string;
  }> => {
    return api.post('/files/document/reparse', params);
  },

  // 文档重新解析 - 查询任务状态
  getReparseStatus: async (taskId: string): Promise<{
    task_id: string;
    status: string;
    progress: number;
    message: string;
    file_id?: number;
    content_length?: number;
    processing_time?: number;
    logs?: Array<{ timestamp: string; level: string; message: string }>;
  }> => {
    return api.get(`/files/document/reparse/${taskId}/status`);
  },

  // === File Sharing ===

  shareFile: async (fileId: number, userIds: number[]): Promise<{
    shared_count: number;
    skipped: Array<{ user_id: number; reason: string }>;
  }> => {
    return api.post(`/files/${fileId}/share`, { user_ids: userIds });
  },

  unshareFile: async (fileId: number, userIds: number[]): Promise<{
    removed_count: number;
  }> => {
    return api.delete(`/files/${fileId}/share`, { data: { user_ids: userIds } });
  },

  getFileShares: async (fileId: number): Promise<{
    file_id: number;
    shares: Array<{
      id: number;
      file_id: number;
      shared_by_user_id: number;
      shared_by_username: string;
      shared_to_user_id: number;
      shared_to_username: string;
      created_at: string;
    }>;
  }> => {
    return api.get(`/files/${fileId}/shares`);
  },

  getShareableUsers: async (): Promise<Array<{ id: number; username: string }>> => {
    return api.get('/users/shareable');
  },
}; 