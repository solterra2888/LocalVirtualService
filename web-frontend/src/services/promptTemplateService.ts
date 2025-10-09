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
  category_name?: string;
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

export interface UserPreference {
  id: number;
  category_id: number;
  category_name?: string;
  preferred_template_id: number;
  template_name?: string;
  custom_parameters: Record<string, any>;
  created_at?: string;
  updated_at?: string;
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
  
  // === 用户偏好 ===
  
  async getUserPreferences(): Promise<UserPreference[]> {
    const response = await api.get('/llm/prompts/preferences');
    return response.data;
  }
  
  async setUserPreference(categoryId: number, templateId: number, customParameters?: Record<string, any>): Promise<{ success: boolean }> {
    const response = await api.post('/llm/prompts/preferences', {
      category_id: categoryId,
      template_id: templateId,
      custom_parameters: customParameters
    });
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
}

const promptTemplateService = new PromptTemplateService();
export default promptTemplateService;

