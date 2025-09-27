// 事件标记相关的类型定义

export interface ChartEvent {
  id: string;
  date: string; // YYYY-MM-DD 格式
  type: EventType;
  title: string;
  description?: string;
  icon?: string;
  color?: string;
  relatedUrl?: string; // 关联的新闻或文件链接
  relatedFileId?: string; // 关联的文件ID
  position?: EventPosition; // 在图表上的位置
  createdAt: string;
  updatedAt: string;
}

export enum EventType {
  NEWS = 'news',           // 新闻事件
  EARNINGS = 'earnings',   // 财报事件
  DIVIDEND = 'dividend',   // 分红事件
  SPLIT = 'split',         // 拆股事件
  MERGER = 'merger',       // 并购事件
  RESEARCH = 'research',   // 研究报告
  CUSTOM = 'custom',       // 自定义事件
  FILE = 'file'           // 文件事件
}

export enum EventPosition {
  TOP = 'top',
  MIDDLE = 'middle',
  BOTTOM = 'bottom'
}

export interface EventTypeConfig {
  label: string;
  icon: string;
  color: string;
  defaultPosition: EventPosition;
}

// Event type configurations
export const EVENT_TYPE_CONFIGS: Record<EventType, EventTypeConfig> = {
  [EventType.NEWS]: {
    label: 'News',
    icon: '📰',
    color: '#1890ff',
    defaultPosition: EventPosition.TOP
  },
  [EventType.EARNINGS]: {
    label: 'Earnings',
    icon: '📊',
    color: '#52c41a',
    defaultPosition: EventPosition.TOP
  },
  [EventType.DIVIDEND]: {
    label: 'Dividend',
    icon: '💰',
    color: '#faad14',
    defaultPosition: EventPosition.MIDDLE
  },
  [EventType.SPLIT]: {
    label: 'Stock Split',
    icon: '🔄',
    color: '#722ed1',
    defaultPosition: EventPosition.MIDDLE
  },
  [EventType.MERGER]: {
    label: 'M&A',
    icon: '🤝',
    color: '#eb2f96',
    defaultPosition: EventPosition.TOP
  },
  [EventType.RESEARCH]: {
    label: 'Research Report',
    icon: '📋',
    color: '#13c2c2',
    defaultPosition: EventPosition.BOTTOM
  },
  [EventType.CUSTOM]: {
    label: 'Custom',
    icon: '📝',
    color: '#8c8c8c',
    defaultPosition: EventPosition.MIDDLE
  },
  [EventType.FILE]: {
    label: 'File',
    icon: '📁',
    color: '#fa8c16',
    defaultPosition: EventPosition.BOTTOM
  }
};

// Form data for creating/editing events
export interface EventFormData {
  date: string;
  type: EventType;
  title: string;
  description?: string;
  position?: EventPosition;
  relatedUrl?: string;
  relatedFileId?: string;
}

// Event filter
export interface EventFilter {
  types?: EventType[];
  dateRange?: [string, string];
  showAll?: boolean;
}
