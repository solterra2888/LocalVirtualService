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

// 事件类型配置
export const EVENT_TYPE_CONFIGS: Record<EventType, EventTypeConfig> = {
  [EventType.NEWS]: {
    label: '新闻',
    icon: '📰',
    color: '#1890ff',
    defaultPosition: EventPosition.TOP
  },
  [EventType.EARNINGS]: {
    label: '财报',
    icon: '📊',
    color: '#52c41a',
    defaultPosition: EventPosition.TOP
  },
  [EventType.DIVIDEND]: {
    label: '分红',
    icon: '💰',
    color: '#faad14',
    defaultPosition: EventPosition.MIDDLE
  },
  [EventType.SPLIT]: {
    label: '拆股',
    icon: '🔄',
    color: '#722ed1',
    defaultPosition: EventPosition.MIDDLE
  },
  [EventType.MERGER]: {
    label: '并购',
    icon: '🤝',
    color: '#eb2f96',
    defaultPosition: EventPosition.TOP
  },
  [EventType.RESEARCH]: {
    label: '研究报告',
    icon: '📋',
    color: '#13c2c2',
    defaultPosition: EventPosition.BOTTOM
  },
  [EventType.CUSTOM]: {
    label: '自定义',
    icon: '📝',
    color: '#8c8c8c',
    defaultPosition: EventPosition.MIDDLE
  },
  [EventType.FILE]: {
    label: '文件',
    icon: '📁',
    color: '#fa8c16',
    defaultPosition: EventPosition.BOTTOM
  }
};

// 创建/编辑事件的表单数据
export interface EventFormData {
  date: string;
  type: EventType;
  title: string;
  description?: string;
  position?: EventPosition;
  relatedUrl?: string;
  relatedFileId?: string;
}

// 事件过滤器
export interface EventFilter {
  types?: EventType[];
  dateRange?: [string, string];
  showAll?: boolean;
}
