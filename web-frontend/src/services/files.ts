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
}

export interface FileProcessResponse {
  task_id: string;
  message: string;
  files_count: number;
}

export interface TaskStatusResponse {
  status: string;
  progress: number;
  current_file?: string;
  files_processed: number;
  files_failed: number;
  total_files: number;
  logs: Array<{
    timestamp: string;
    level: string;
    message: string;
  }>;
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
    llamaindex_api_key: string;
    ai_agent_server_url: string;
  };
}

export interface SupportedFileTypesResponse {
  supported_extensions: string[];
  descriptions: Record<string, string>;
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

  // 获取原始文件信息
  getOriginalFileInfo: async (fileId: number): Promise<{
    filename: string;
    file_type: string;
    file_size_mb: number;
    original_file_path: string;
    file_url: string;
  }> => {
    return api.get<{
      filename: string;
      file_type: string;
      file_size_mb: number;
      original_file_path: string;
      file_url: string;
    }>(`/files/original/${fileId}`);
  },

  // 获取支持的文件类型扩展名
  getSupportedFileExtensions: (): string[] => {
    return [
      '.pdf',
      '.xlsx', '.xls',
      '.doc', '.docx',
      '.jpg', '.jpeg', '.png', '.gif', '.bmp'
    ];
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
  }
}; 