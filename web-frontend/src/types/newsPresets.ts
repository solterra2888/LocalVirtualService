/**
 * 用户新闻筛选预设类型定义
 */

/**
 * 维度内部逻辑操作符
 */
export type DimensionOperator = 'OR' | 'AND';

/**
 * 各维度操作符配置
 */
export interface DimensionOperators {
  news_value?: DimensionOperator;          // 默认 OR - 新闻价值类型
  categories?: DimensionOperator;          // 默认 OR
  event_types?: DimensionOperator;         // 默认 OR
  impact_scopes?: DimensionOperator;       // 默认 OR
  urgency?: DimensionOperator;             // 默认 OR
  related_sectors?: DimensionOperator;     // 默认 OR
  related_countries?: DimensionOperator;   // 默认 OR
  source_types?: DimensionOperator;        // 默认 OR
  scheduler_sources?: DimensionOperator;   // 默认 OR
}

/**
 * 筛选条件配置
 */
export interface FilterConfig {
  // 新闻价值分类 (新增)
  news_value?: string[];             // 新闻价值类型: fact, opinion
  
  // 主题分类维度
  categories?: string[];              // 主要主题分类 (political, financial, technology...)
  event_types?: string[];            // 事件类型
  impact_scopes?: string[];          // 影响范围
  urgency?: string[];                // 时效性
  related_sectors?: string[];        // 相关行业
  related_countries?: string[];      // 相关国家/地区
  
  // 来源筛选
  source_types?: string[];           // 来源类型 (llm_classification, vector_retrieval...)
  scheduler_sources?: string[];      // 调度器来源 (eodhd_news, watchlist_news, cctv_news)
  
  // 🔥 新增：Ticker筛选
  ticker_symbols?: string[];         // 指定ticker符号列表，如: ['AAPL.US', '0700.HK']
  use_watchlist?: boolean;           // 是否使用用户watchlist中的ticker
  watchlist_groups?: string[];       // 使用特定watchlist分组，如: ['Technology', 'Healthcare']
  ticker_filter_mode?: 'include' | 'exclude';  // Ticker过滤模式: 'include'(包含) | 'exclude'(排除)
  
  // 其他选项
  min_confidence?: number;           // 最低置信度
  only_primary?: boolean;            // 只显示主要主题
  market?: string;                   // 市场 (HK/US/CN/GLOBAL)
  
  // 🔥 新增：各维度内部逻辑操作符（默认 OR）
  dimension_operators?: DimensionOperators;
  
  // 逻辑关系（暂时保留，未来可能用到）
  custom_logic?: 'AND' | 'OR';
}

/**
 * 预设类型
 */
export type PresetType = 'system' | 'custom';

/**
 * 应用范围类型
 */
export type ScopeType = 'global' | 'symbol_specific';

/**
 * 新闻筛选预设
 */
export interface NewsFilterPreset {
  id: number;
  user_id: number;
  preset_name: string;
  preset_type: PresetType;
  description?: string;
  icon?: string;
  filter_config: FilterConfig;
  scope_type: ScopeType;
  symbol_code?: string;
  is_default: boolean;
  is_favorite: boolean;
  sort_order: number;
  usage_count: number;
  last_used_at?: string;
  created_at?: string;
  updated_at?: string;
}

/**
 * 创建预设请求
 */
export interface CreatePresetRequest {
  preset_name: string;
  description?: string;
  icon?: string;
  filter_config: FilterConfig;
  scope_type: ScopeType;
  symbol_code?: string;
  is_default?: boolean;
  is_favorite?: boolean;
  is_system_preset?: boolean;  // 是否创建为系统预设（仅admin可用）
}

/**
 * 更新预设请求
 */
export interface UpdatePresetRequest {
  preset_name?: string;
  description?: string;
  icon?: string;
  filter_config?: FilterConfig;
  is_default?: boolean;
  is_favorite?: boolean;
  sort_order?: number;
}

/**
 * 预设列表响应
 */
export interface PresetListResponse {
  success: boolean;
  total: number;
  presets: NewsFilterPreset[];
}

/**
 * 预设操作响应
 */
export interface PresetOperationResponse {
  success: boolean;
  message: string;
  preset?: NewsFilterPreset;
}

/**
 * 获取预设列表参数
 */
export interface GetPresetsParams {
  scope_type?: ScopeType;
  symbol_code?: string;
  include_system?: boolean;
}

/**
 * 系统预设配置（用于前端常量）
 */
export const SYSTEM_PRESETS_CONFIG = {
  FACTUAL_NEWS: {
    name: 'Fact',
    description: 'Only show factual news, filter opinion articles',
    filter: {
      news_value: ['fact'],
      urgency: ['urgent', 'timely'],
      min_confidence: 0.7
    }
  },
  IMPORTANT_NEWS: {
    name: '⭐ 重要新闻',
    icon: '⭐',
    description: '紧急和及时的高相关度新闻',
    filter: {
      urgency: ['urgent', 'timely'],
      min_confidence: 0.7,
      only_primary: true
    }
  },
  EARNINGS_SEASON: {
    name: '📊 财报季',
    icon: '📊',
    description: '财报发布和业绩相关新闻',
    filter: {
      news_value: ['fact'],
      event_types: ['earnings_report', 'financial_results'],
      min_confidence: 0.6
    }
  },
  MERGERS: {
    name: '🤝 并购重组',
    icon: '🤝',
    description: '并购、重组、战略投资相关',
    filter: {
      event_types: ['merger_acquisition', 'strategic_investment'],
      urgency: ['urgent', 'timely']
    }
  },
  REGULATORY: {
    name: '⚖️ 监管合规',
    icon: '⚖️',
    description: '监管处罚、合规问题',
    filter: {
      event_types: ['regulatory_news', 'legal_issue'],
      urgency: ['urgent', 'timely']
    }
  },
  PRODUCT_INNOVATION: {
    name: '🚀 产品创新',
    icon: '🚀',
    description: '新产品、新技术发布',
    filter: {
      event_types: ['product_launch', 'technology_innovation']
    }
  }
} as const;

/**
 * 事件类型选项（用于UI）
 */
export const EVENT_TYPE_OPTIONS = [
  { value: 'earnings_report', label: '财报发布', icon: '📊' },
  { value: 'financial_results', label: '财务数据', icon: '💰' },
  { value: 'merger_acquisition', label: '并购重组', icon: '🤝' },
  { value: 'strategic_investment', label: '战略投资', icon: '💼' },
  { value: 'regulatory_news', label: '监管动态', icon: '⚖️' },
  { value: 'legal_issue', label: '法律诉讼', icon: '⚖️' },
  { value: 'product_launch', label: '产品发布', icon: '🚀' },
  { value: 'technology_innovation', label: '技术创新', icon: '💡' },
  { value: 'management_change', label: '管理层变动', icon: '👔' },
  { value: 'general_news', label: '一般新闻', icon: '📰' }
];

/**
 * 时效性选项
 */
export const URGENCY_OPTIONS = [
  { value: 'urgent', label: '紧急', icon: '🔴', color: '#f44336' },
  { value: 'timely', label: '及时', icon: '🟡', color: '#ff9800' },
  { value: 'routine', label: '例行', icon: '⚪', color: '#9e9e9e' }
];

/**
 * 影响范围选项
 */
export const IMPACT_SCOPE_OPTIONS = [
  { value: 'company_specific', label: '公司独有', icon: '🏢' },
  { value: 'sector_wide', label: '行业相关', icon: '🏭' },
  { value: 'market_wide', label: '市场影响', icon: '📈' },
  { value: 'global', label: '全球事件', icon: '🌍' }
];

/**
 * 来源类型选项
 */
export const SOURCE_TYPE_OPTIONS = [
  { value: 'llm_classification', label: 'LLM分类' },
  { value: 'vector_retrieval', label: '向量检索' },
  { value: 'eodhd_auto_tag', label: 'EODHD标签' }
];

/**
 * 新闻价值类型选项 (新增)
 */
export const NEWS_VALUE_OPTIONS = [
  { value: 'fact', label: 'Fact' },
  { value: 'opinion', label: 'Opinion' }
];


