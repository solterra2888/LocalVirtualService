// 文件标签API服务

import {
  FileTags,
  FileTagsWithFileInfo,
  FileTagsUpdate,
  RetagRequest,
  BatchRetagRequest,
  TaggingTaskStatus,
  FileTagsStats
} from '../types/fileTags';

const API_BASE_URL = '/api/file-tags';

class FileTagsService {
  private getAuthHeaders(): HeadersInit {
    const token = localStorage.getItem('access_token');
    return {
      'Content-Type': 'application/json',
      ...(token ? { 'Authorization': `Bearer ${token}` } : {})
    };
  }

  /**
   * 获取文件的标签信息
   */
  async getFileTags(fileId: number): Promise<FileTags> {
    const response = await fetch(`${API_BASE_URL}/${fileId}`, {
      headers: this.getAuthHeaders()
    });

    if (!response.ok) {
      if (response.status === 404) {
        throw new Error('文件标签不存在');
      }
      throw new Error(`获取标签失败: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * 列出文件标签（支持筛选）
   */
  async listFileTags(params?: {
    sector?: string;
    industry?: string;
    ticker?: string;
    limit?: number;
    offset?: number;
  }): Promise<FileTagsWithFileInfo[]> {
    const searchParams = new URLSearchParams();
    if (params?.sector) searchParams.set('sector', params.sector);
    if (params?.industry) searchParams.set('industry', params.industry);
    if (params?.ticker) searchParams.set('ticker', params.ticker);
    if (params?.limit) searchParams.set('limit', params.limit.toString());
    if (params?.offset) searchParams.set('offset', params.offset.toString());

    const url = `${API_BASE_URL}/?${searchParams.toString()}`;
    const response = await fetch(url, {
      headers: this.getAuthHeaders()
    });

    if (!response.ok) {
      throw new Error(`获取标签列表失败: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * 更新文件标签（手动编辑）
   */
  async updateFileTags(fileId: number, updates: FileTagsUpdate): Promise<FileTags> {
    const response = await fetch(`${API_BASE_URL}/${fileId}`, {
      method: 'PUT',
      headers: this.getAuthHeaders(),
      body: JSON.stringify(updates)
    });

    if (!response.ok) {
      throw new Error(`更新标签失败: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * 删除文件标签
   */
  async deleteFileTags(fileId: number): Promise<{ success: boolean; message: string }> {
    const response = await fetch(`${API_BASE_URL}/${fileId}`, {
      method: 'DELETE',
      headers: this.getAuthHeaders()
    });

    if (!response.ok) {
      throw new Error(`删除标签失败: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * 重新为文件打标签
   */
  async retagFile(request: RetagRequest): Promise<TaggingTaskStatus> {
    const response = await fetch(`${API_BASE_URL}/retag`, {
      method: 'POST',
      headers: this.getAuthHeaders(),
      body: JSON.stringify(request)
    });

    if (!response.ok) {
      throw new Error(`重新打标签失败: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * 批量重新打标签
   */
  async retagBatch(request: BatchRetagRequest): Promise<{
    task_id: string;
    status: string;
    total_files: number;
    message: string;
  }> {
    const response = await fetch(`${API_BASE_URL}/retag-batch`, {
      method: 'POST',
      headers: this.getAuthHeaders(),
      body: JSON.stringify(request)
    });

    if (!response.ok) {
      throw new Error(`批量打标签失败: ${response.statusText}`);
    }

    return response.json();
  }

  /**
   * 获取标签统计信息
   */
  async getTagsStats(): Promise<FileTagsStats> {
    const response = await fetch(`${API_BASE_URL}/stats/overview`, {
      headers: this.getAuthHeaders()
    });

    if (!response.ok) {
      throw new Error(`获取统计信息失败: ${response.statusText}`);
    }

    return response.json();
  }
}

export const fileTagsService = new FileTagsService();
