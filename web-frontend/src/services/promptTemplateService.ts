/**
 * Prompt模板服务
 * 
 * 提供Prompt模板的CRUD操作、测试、统计等功能
 */

import api from './api';

// ==================== 类型定义 ====================

export interface PromptCategory {
  id: number;
  category_code: string;
  category_name: string;
  description?: string;
  icon?: string;
  sort_order: number;
  is_active: boolean;
  created_at?: string;
  updated_at?: string;
}

export interface VariableDefinition {
  type: 'string' | 'integer' | 'float' | 'boolean' | 'array' | 'object';
  required: boolean;
  default?: any;
  description?: string;
}

export interface OutputFormat {
  type: 'text' | 'json';
  schema?: Record<string, any>;
}

export interface PromptTemplate {
  id: number;
  template_code: string;
  template_name: string;
  category_id: number;
  category_code?: string;
  category_name?: string;
  /** PR-5 (2026-04-27): 业务模块绑定字段, 与 user_template_preferences.module_code 对齐 */
  module_code?: string;
  description?: string;
  scope: 'system' | 'user' | 'shared';
  is_active: boolean;
  is_default: boolean;
  usage_count: number;
  success_rate?: number;
  recommended_provider?: string;
  recommended_model?: string;
  temperature: number;
  max_tokens: number;
  version: number;
  created_by?: number;
  created_by_name?: string;
  created_at?: string;
  updated_at?: string;
}

export interface PromptTemplateDetail extends PromptTemplate {
  system_prompt?: string;
  user_prompt_template: string;
  variables: Record<string, VariableDefinition>;
  output_format: OutputFormat;
  tags: string[];
  example_input: Record<string, any>;
  example_output?: string;
  is_public: boolean;
  avg_cost?: number;
  parent_template_id?: number;
}

export interface TemplateCreateData {
  template_code: string;
  template_name: string;
  category_id: number;
  /** PR-5: 业务模块代码; 与 user_template_preferences.module_code 对齐 */
  module_code?: string;
  system_prompt?: string;
  user_prompt_template: string;
  variables?: Record<string, VariableDefinition>;
  output_format?: OutputFormat;
  recommended_provider?: string;
  recommended_model?: string;
  temperature?: number;
  max_tokens?: number;
  scope?: 'system' | 'user' | 'shared';
  is_public?: boolean;
  description?: string;
  tags?: string[];
  example_input?: Record<string, any>;
  example_output?: string;
}

export interface TemplateUpdateData {
  template_name?: string;
  /** PR-5: 允许 admin 编辑时修正 module_code */
  module_code?: string;
  system_prompt?: string;
  user_prompt_template?: string;
  variables?: Record<string, VariableDefinition>;
  output_format?: OutputFormat;
  recommended_provider?: string;
  recommended_model?: string;
  temperature?: number;
  max_tokens?: number;
  description?: string;
  tags?: string[];
  example_input?: Record<string, any>;
  example_output?: string;
  is_active?: boolean;
  is_public?: boolean;
  is_default?: boolean;
}

export interface TestTemplateRequest {
  template_id?: number;
  template_code?: string;
  variables: Record<string, any>;
  provider?: string;
  model?: string;
}

export interface TestTemplateResponse {
  success: boolean;
  rendered_prompt: {
    system_prompt?: string;
    user_prompt?: string;
  };
  llm_response?: string;
  parsed_output?: Record<string, any>;
  tokens_used?: number;
  cost?: number;
  response_time?: number;
  error?: string;
}

export interface TemplateStats {
  template_id: number;
  period_days: number;
  total_uses: number;
  unique_users: number;
  avg_tokens: number;
  avg_cost: number;
  avg_response_time_ms: number;
  success_rate: number;
}

export interface UserTemplatePreference {
  id: number;
  module_code: string;
  template_id: number;
  template_code: string;
  template_name: string;
  scope: string;
  created_at?: string;
  updated_at?: string;
}

export interface ModuleInfo {
  module_code: string;
  module_name: string;
  description: string;
  configurable: boolean;
  default_template_code: string | null;
}

/**
 * Phase 2 (PR-C) My Templates 列表项 — 一个 module 一项.
 * - kind='user_owned': 该用户在该 USER_OWNED 模块下的私有模板, 可编辑可重置
 * - kind='system_fixed': 系统级模板, 普通用户只读, admin 通过 /system-templates 编辑
 */
export interface MyTemplateItem {
  module_code: string;
  module_name: string;
  module_description: string;
  kind: 'user_owned' | 'system_fixed';
  configurable: boolean;

  template_id: number;
  template_code: string;
  template_name: string;
  scope: 'system' | 'user' | 'shared';
  is_active: boolean;

  system_prompt: string | null;
  user_prompt_template: string;
  variables: Record<string, VariableDefinition>;
  output_format: OutputFormat;
  recommended_provider: string | null;
  recommended_model: string | null;
  temperature: number;
  max_tokens: number;
  description: string | null;

  parent_template_id: number | null;
  updated_at: string | null;
}

export interface UpdateMyTemplatePayload {
  system_prompt?: string;
  user_prompt_template?: string;
  variables?: Record<string, VariableDefinition>;
  output_format?: OutputFormat;
  recommended_provider?: string;
  recommended_model?: string;
  temperature?: number;
  max_tokens?: number;
}

// ==================== API服务 ====================

class PromptTemplateService {
  // === 分类管理 ===
  
  async getCategories(activeOnly: boolean = true): Promise<PromptCategory[]> {
    const response = await api.get('/llm/prompts/categories', {
      params: { active_only: activeOnly }
    });
    return response.data;
  }
  
  async createCategory(categoryData: Omit<PromptCategory, 'id' | 'created_at' | 'updated_at'>): Promise<{ success: boolean; category_id: number }> {
    const response = await api.post('/llm/prompts/categories', categoryData);
    return response.data;
  }
  
  // === 模板管理 ===
  
  async listTemplates(params?: {
    category_id?: number;
    scope?: 'system' | 'user' | 'shared';
    search?: string;
    active_only?: boolean;
  }): Promise<PromptTemplate[]> {
    const response = await api.get('/llm/prompts/templates', { params });
    return response.data;
  }
  
  async getTemplate(templateId: number): Promise<PromptTemplateDetail> {
    const response = await api.get(`/llm/prompts/templates/${templateId}`);
    return response.data;
  }
  
  async createTemplate(templateData: TemplateCreateData): Promise<{ success: boolean; template_id: number }> {
    const response = await api.post('/llm/prompts/templates', templateData);
    return response.data;
  }
  
  async updateTemplate(templateId: number, updates: TemplateUpdateData): Promise<{ success: boolean }> {
    const response = await api.put(`/llm/prompts/templates/${templateId}`, updates);
    return response.data;
  }
  
  async deleteTemplate(templateId: number): Promise<{ success: boolean }> {
    const response = await api.delete(`/llm/prompts/templates/${templateId}`);
    return response.data;
  }
  
  // === 模板测试 ===
  
  async testTemplate(request: TestTemplateRequest): Promise<TestTemplateResponse> {
    const response = await api.post('/llm/prompts/templates/test', request);
    return response.data;
  }
  
  // === 统计分析 ===
  
  async getTemplateStats(templateId: number, days: number = 30): Promise<TemplateStats> {
    const response = await api.get(`/llm/prompts/templates/${templateId}/stats`, {
      params: { days }
    });
    return response.data;
  }
  
  async getPopularTemplates(categoryId?: number, limit: number = 10): Promise<PromptTemplate[]> {
    const response = await api.get('/llm/prompts/templates/popular', {
      params: { category_id: categoryId, limit }
    });
    return response.data;
  }
  
  // === 导出模板 ===
  
  async exportTemplate(templateId: number): Promise<Blob> {
    const response = await api.get(`/llm/prompts/templates/export/${templateId}`, {
      responseType: 'blob'
    });
    return response.data;
  }
  
  // === 辅助方法 ===
  
  /**
   * 下载导出的模板文件
   */
  async downloadTemplateExport(templateId: number, templateCode: string): Promise<void> {
    const blob = await this.exportTemplate(templateId);
    const url = window.URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `template_${templateCode}.json`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    window.URL.revokeObjectURL(url);
  }
  
  /**
   * 解析模板中的变量
   * 从user_prompt_template中提取{{variable_name}}格式的变量
   */
  parseTemplateVariables(template: string): string[] {
    const regex = /\{\{(\w+)\}\}/g;
    const variables: string[] = [];
    let match;
    
    while ((match = regex.exec(template)) !== null) {
      if (!variables.includes(match[1])) {
        variables.push(match[1]);
      }
    }
    
    return variables;
  }

  /**
   * 获取变量库
   */
  async getVariablesLibrary(): Promise<any[]> {
    const response = await api.get('/llm/prompts/variables/library');
    return response.data.variables || [];
  }
  
  /**
   * 验证变量值是否符合定义
   */
  validateVariables(
    variables: Record<string, VariableDefinition>,
    values: Record<string, any>
  ): { valid: boolean; errors: string[] } {
    const errors: string[] = [];
    
    // 检查必需变量
    Object.entries(variables).forEach(([name, def]) => {
      if (def.required && !(name in values) && !def.default) {
        errors.push(`变量 "${name}" 是必需的`);
      }
    });
    
    // 检查类型
    Object.entries(values).forEach(([name, value]) => {
      if (name in variables) {
        const def = variables[name];
        const actualType = typeof value;
        const expectedType = def.type;
        
        const typeMatches = {
          string: actualType === 'string',
          integer: Number.isInteger(value),
          float: typeof value === 'number',
          boolean: actualType === 'boolean',
          array: Array.isArray(value),
          object: actualType === 'object' && !Array.isArray(value)
        };
        
        if (!typeMatches[expectedType]) {
          errors.push(`变量 "${name}" 的类型不正确，期望 ${expectedType}，实际 ${actualType}`);
        }
      }
    });
    
    return {
      valid: errors.length === 0,
      errors
    };
  }

  // === 用户模板偏好 ===

  async getModules(): Promise<ModuleInfo[]> {
    const response = await api.get('/llm/prompts/modules');
    return response.data.modules || [];
  }

  async getPreferences(): Promise<UserTemplatePreference[]> {
    const response = await api.get('/llm/prompts/preferences');
    return response.data.preferences || [];
  }

  async getPreference(moduleCode: string): Promise<UserTemplatePreference | null> {
    const response = await api.get(`/llm/prompts/preferences/${moduleCode}`);
    return response.data.preference || null;
  }

  async setPreference(moduleCode: string, templateId: number): Promise<{ success: boolean }> {
    const response = await api.put(`/llm/prompts/preferences/${moduleCode}`, {
      template_id: templateId,
    });
    return response.data;
  }

  async deletePreference(moduleCode: string): Promise<{ success: boolean }> {
    const response = await api.delete(`/llm/prompts/preferences/${moduleCode}`);
    return response.data;
  }

  // === Phase 2 (PR-C): My Templates 双态模型 ===

  /**
   * 列出当前用户的所有 module → 1 模板; USER_OWNED 给私有模板, SYSTEM_FIXED 给只读默认.
   */
  async listMyTemplates(): Promise<MyTemplateItem[]> {
    const response = await api.get('/llm/prompts/my-templates');
    return response.data.items || [];
  }

  async getMyTemplate(moduleCode: string): Promise<MyTemplateItem> {
    const response = await api.get(`/llm/prompts/my-templates/${moduleCode}`);
    return response.data.item;
  }

  /**
   * 编辑当前用户在指定 USER_OWNED 模块下的私有模板内容.
   * SYSTEM_FIXED 模块会返回 403, 走 /system-templates 由 admin 操作.
   */
  async updateMyTemplate(
    moduleCode: string,
    payload: UpdateMyTemplatePayload,
  ): Promise<{ success: boolean; item: MyTemplateItem }> {
    const response = await api.put(`/llm/prompts/my-templates/${moduleCode}`, payload);
    return response.data;
  }

  /**
   * 重置当前用户在指定 USER_OWNED 模块下的私有模板, 内容回到 system default (覆盖, 无历史).
   */
  async resetMyTemplate(
    moduleCode: string,
  ): Promise<{ success: boolean; item: MyTemplateItem }> {
    const response = await api.post(`/llm/prompts/my-templates/${moduleCode}/reset`);
    return response.data;
  }

  // === Phase 2 (PR-C): Admin → System Templates ===

  /**
   * [Admin] 列出所有 system-scope 模板 (用于 System Templates Tab).
   */
  async listSystemTemplates(): Promise<PromptTemplate[]> {
    const response = await api.get('/llm/prompts/system-templates');
    return response.data.templates || [];
  }

  /**
   * [Admin] 编辑 system-scope 模板内容.
   */
  async updateSystemTemplate(
    templateId: number,
    payload: UpdateMyTemplatePayload,
  ): Promise<{ success: boolean; template: PromptTemplateDetail }> {
    const response = await api.put(`/llm/prompts/system-templates/${templateId}`, payload);
    return response.data;
  }
}

const promptTemplateService = new PromptTemplateService();
export default promptTemplateService;

