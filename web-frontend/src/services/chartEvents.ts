/**
 * 图表事件API服务
 */

import { api } from './api';
import { ChartEvent, EventFormData } from '../types/events';

// API响应类型
interface CreateEventResponse {
  success: boolean;
  message: string;
  event_id: string;
}

interface UpdateEventResponse {
  success: boolean;
  message: string;
}

interface DeleteEventResponse {
  success: boolean;
  message: string;
}

interface EventsSummaryResponse {
  total_events: number;
  by_type: Record<string, number>;
  by_symbol: Record<string, number>;
}

// API请求数据类型
interface CreateEventRequest {
  symbol: string;
  event_date: string;
  event_type: string;
  title: string;
  description?: string;
  position: string;
  related_url?: string;
  related_file_id?: string;
}

interface UpdateEventRequest {
  event_date?: string;
  event_type?: string;
  title?: string;
  description?: string;
  position?: string;
  related_url?: string;
  related_file_id?: string;
}

export class ChartEventsAPI {
  /**
   * 创建新的图表事件
   */
  static async createEvent(symbol: string, eventData: EventFormData): Promise<string> {
    try {
      const requestData: CreateEventRequest = {
        symbol: symbol,
        event_date: eventData.date,
        event_type: eventData.type,
        title: eventData.title,
        description: eventData.description,
        position: eventData.position || 'middle',
        related_url: eventData.relatedUrl,
        related_file_id: eventData.relatedFileId,
      };

      const response = await api.post<CreateEventResponse>(
        '/chart-events',
        requestData
      );

      if (response.success) {
        return response.event_id;
      } else {
        throw new Error(response.message || '创建事件失败');
      }
    } catch (error: any) {
      console.error('创建图表事件失败:', error);
      throw new Error(
        error.response?.data?.detail || error.message || '创建事件失败'
      );
    }
  }

  /**
   * 获取指定股票的所有事件
   */
  static async getEventsBySymbol(symbol: string): Promise<ChartEvent[]> {
    try {
      const response = await api.get<ChartEvent[]>(
        `/chart-events/${symbol}`
      );

      return response || [];
    } catch (error: any) {
      console.error('获取图表事件失败:', error);
      throw new Error(
        error.response?.data?.detail || error.message || '获取事件失败'
      );
    }
  }

  /**
   * 更新图表事件
   */
  static async updateEvent(
    eventId: string,
    eventData: EventFormData
  ): Promise<void> {
    try {
      const requestData: UpdateEventRequest = {
        event_date: eventData.date,
        event_type: eventData.type,
        title: eventData.title,
        description: eventData.description,
        position: eventData.position,
        related_url: eventData.relatedUrl,
        related_file_id: eventData.relatedFileId,
      };

      const response = await api.put<UpdateEventResponse>(
        `/chart-events/${eventId}`,
        requestData
      );

      if (!response.success) {
        throw new Error(response.message || '更新事件失败');
      }
    } catch (error: any) {
      console.error('更新图表事件失败:', error);
      throw new Error(
        error.response?.data?.detail || error.message || '更新事件失败'
      );
    }
  }

  /**
   * 删除图表事件
   */
  static async deleteEvent(eventId: string): Promise<void> {
    try {
      const response = await api.delete<DeleteEventResponse>(
        `/chart-events/${eventId}`
      );

      if (!response.success) {
        throw new Error(response.message || '删除事件失败');
      }
    } catch (error: any) {
      console.error('删除图表事件失败:', error);
      throw new Error(
        error.response?.data?.detail || error.message || '删除事件失败'
      );
    }
  }

  /**
   * 获取用户事件统计摘要
   */
  static async getUserEventsSummary(): Promise<EventsSummaryResponse> {
    try {
      const response = await api.get<EventsSummaryResponse>('/chart-events');
      return response;
    } catch (error: any) {
      console.error('获取事件统计失败:', error);
      throw new Error(
        error.response?.data?.detail || error.message || '获取统计失败'
      );
    }
  }

  /**
   * 导出用户的所有事件
   */
  static async exportUserEvents(): Promise<ChartEvent[]> {
    try {
      const response = await api.get<ChartEvent[]>(
        '/chart-events/export/all'
      );
      return response || [];
    } catch (error: any) {
      console.error('导出事件失败:', error);
      throw new Error(
        error.response?.data?.detail || error.message || '导出事件失败'
      );
    }
  }

  /**
   * 批量导入事件（通过逐个创建实现）
   */
  static async importEvents(symbol: string, events: EventFormData[]): Promise<number> {
    let successCount = 0;
    const errors: string[] = [];

    for (const eventData of events) {
      try {
        await this.createEvent(symbol, eventData);
        successCount++;
      } catch (error: any) {
        errors.push(`${eventData.title}: ${error.message}`);
      }
    }

    if (errors.length > 0) {
      console.warn('部分事件导入失败:', errors);
    }

    return successCount;
  }
}
