# -*- coding: utf-8 -*-
"""
LLM Prompt模板API端点
"""

from fastapi import APIRouter, HTTPException, Depends, Query, UploadFile, File
from fastapi.responses import JSONResponse
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

from backend.services.llm_prompt_template_service import LLMPromptTemplateService
from backend.services.template_preference_service import TemplatePreferenceService
from backend.services.user_template_seeder_service import UserTemplateSeederService
from backend.core.prompt_renderer import PromptRenderer
from backend.core.llm_framework import llm_service, LLMMessage
from backend.auth.jwt_utils import get_current_user, get_current_admin_user
from backend.schemas.user import TokenData
from backend.utils.logger import get_logger

logger = get_logger("llm_prompt_templates_api")

router = APIRouter(
    prefix="/llm/prompts",
    tags=["Prompt模板管理"]
)

# ==================== Pydantic 模型 ====================

class CategoryCreate(BaseModel):
    category_code: str = Field(..., description="分类代码")
    category_name: str = Field(..., description="分类名称")
    description: Optional[str] = Field(None, description="描述")
    icon: Optional[str] = Field(None, description="图标")
    sort_order: int = Field(0, description="排序")

class CategoryResponse(BaseModel):
    id: int
    category_code: str
    category_name: str
    description: Optional[str]
    icon: Optional[str]
    sort_order: int
    is_active: bool
    created_at: Optional[str]
    updated_at: Optional[str]

class VariableDefinition(BaseModel):
    type: str = Field(..., description="变量类型: string/integer/float/boolean/array/object")
    required: bool = Field(False, description="是否必需")
    default: Optional[Any] = Field(None, description="默认值")
    description: Optional[str] = Field(None, description="描述")

class OutputFormatSchema(BaseModel):
    type: str = Field("text", description="输出类型: text/json")
    json_schema: Optional[Dict[str, Any]] = Field(None, description="JSON Schema定义")

class TemplateCreate(BaseModel):
    template_code: str = Field(..., description="模板代码，唯一标识")
    template_name: str = Field(..., description="模板名称")
    category_id: int = Field(..., description="所属分类ID")
    module_code: Optional[str] = Field(None, description="业务模块代码（与 user_template_preferences.module_code 对齐）")
    system_prompt: Optional[str] = Field(None, description="系统提示词")
    user_prompt_template: str = Field(..., description="用户提示词模板")
    variables: Dict[str, VariableDefinition] = Field(default_factory=dict, description="变量定义")
    output_format: OutputFormatSchema = Field(default_factory=OutputFormatSchema, description="输出格式")
    recommended_provider: str = Field(..., description="推荐的LLM提供商（必填）")
    recommended_model: str = Field(..., description="推荐的模型（必填）")
    temperature: float = Field(0.7, ge=0, le=2, description="温度参数")
    max_tokens: int = Field(2000, ge=1, description="最大token数")
    scope: str = Field("user", description="范围: system/user/shared")
    is_public: bool = Field(False, description="是否公开")
    description: Optional[str] = Field(None, description="描述")
    tags: List[str] = Field(default_factory=list, description="标签")
    example_input: Dict[str, Any] = Field(default_factory=dict, description="示例输入")
    example_output: Optional[str] = Field(None, description="示例输出")

class TemplateUpdate(BaseModel):
    template_name: Optional[str] = None
    module_code: Optional[str] = None
    system_prompt: Optional[str] = None
    user_prompt_template: Optional[str] = None
    variables: Optional[Dict[str, VariableDefinition]] = None
    output_format: Optional[OutputFormatSchema] = None
    recommended_provider: Optional[str] = None
    recommended_model: Optional[str] = None
    temperature: Optional[float] = Field(None, ge=0, le=2)
    max_tokens: Optional[int] = Field(None, ge=1)
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    example_input: Optional[Dict[str, Any]] = None
    example_output: Optional[str] = None
    is_active: Optional[bool] = None
    is_public: Optional[bool] = None
    is_default: Optional[bool] = None

class TemplateResponse(BaseModel):
    id: int
    template_code: str
    template_name: str
    category_id: Optional[int]
    category_name: Optional[str]
    category_code: Optional[str] = None
    module_code: Optional[str] = None
    description: Optional[str]
    scope: str
    is_active: bool
    is_default: bool
    usage_count: int
    success_rate: Optional[float]
    recommended_provider: Optional[str]
    recommended_model: Optional[str]
    temperature: float
    max_tokens: int
    version: int
    created_by: Optional[int]
    created_by_name: Optional[str]
    created_at: Optional[str]
    updated_at: Optional[str]

class TemplateDetailResponse(TemplateResponse):
    system_prompt: Optional[str]
    user_prompt_template: str
    variables: Dict[str, Any]
    output_format: Dict[str, Any]
    tags: List[str]
    example_input: Dict[str, Any]
    example_output: Optional[str]
    is_public: bool
    avg_cost: Optional[float]
    parent_template_id: Optional[int]

class TestTemplateRequest(BaseModel):
    template_id: Optional[int] = Field(None, description="模板ID")
    template_code: Optional[str] = Field(None, description="模板代码")
    variables: Dict[str, Any] = Field(..., description="测试变量")
    provider: Optional[str] = Field(None, description="指定LLM提供商")
    model: Optional[str] = Field(None, description="指定模型")

class TestTemplateResponse(BaseModel):
    success: bool
    rendered_prompt: Dict[str, str]
    llm_response: Optional[str]
    parsed_output: Optional[Dict[str, Any]]
    tokens_used: Optional[int]
    cost: Optional[float]
    response_time: Optional[float]
    error: Optional[str]

class TemplateStatsResponse(BaseModel):
    template_id: int
    period_days: int
    total_uses: int
    unique_users: int
    avg_tokens: float
    avg_cost: float
    avg_response_time_ms: float
    success_rate: float


# ==================== API端点 ====================

# === 分类管理 ===

@router.get("/categories", response_model=List[CategoryResponse], summary="获取所有模板分类")
async def list_prompt_categories(
    active_only: bool = Query(True, description="是否只返回活跃分类"),
    current_user: TokenData = Depends(get_current_user)
):
    """获取所有模板分类"""
    try:
        service = LLMPromptTemplateService()
        categories = service.list_categories(active_only=active_only)
        return categories
    except Exception as e:
        logger.error(f"Failed to list categories: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取分类失败: {str(e)}")


@router.post("/categories", summary="创建新分类（管理员）")
async def create_prompt_category(
    category_data: CategoryCreate,
    current_user: TokenData = Depends(get_current_admin_user)
):
    """创建新分类（管理员权限）"""
    try:
        service = LLMPromptTemplateService()
        category_id = service.create_category(category_data.dict())
        
        if category_id:
            return {"success": True, "category_id": category_id, "message": "分类创建成功"}
        else:
            raise HTTPException(status_code=500, detail="创建分类失败")
    except Exception as e:
        logger.error(f"Failed to create category: {str(e)}")
        raise HTTPException(status_code=500, detail=f"创建分类失败: {str(e)}")


# === 模板管理 ===

@router.get("/templates", response_model=List[TemplateResponse], summary="获取模板列表")
async def list_templates(
    category_id: Optional[int] = Query(None, description="分类ID过滤"),
    scope: Optional[str] = Query(None, description="范围过滤: system/user/shared"),
    search: Optional[str] = Query(None, description="搜索关键词"),
    active_only: bool = Query(True, description="是否只返回活跃模板"),
    current_user: TokenData = Depends(get_current_user)
):
    """获取模板列表，支持过滤和搜索"""
    try:
        service = LLMPromptTemplateService()
        templates = service.list_templates(
            category_id=category_id,
            scope=scope,
            user_id=current_user.user_id,
            is_admin=current_user.is_admin or False,
            search=search,
            active_only=active_only
        )
        return templates
    except Exception as e:
        logger.error(f"Failed to list templates: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取模板列表失败: {str(e)}")


@router.get("/templates/{template_id}", response_model=TemplateDetailResponse, summary="获取模板详情")
async def get_template(
    template_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """获取模板详情"""
    try:
        service = LLMPromptTemplateService()
        template = service.get_template(template_id)
        
        if not template:
            raise HTTPException(status_code=404, detail="模板不存在")
        
        return template
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get template: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取模板详情失败: {str(e)}")


@router.get("/variables/library", summary="获取变量库")
async def get_variables_library(
    current_user: TokenData = Depends(get_current_user)
):
    """获取所有模板中使用的变量信息"""
    try:
        service = LLMPromptTemplateService()
        variables = service.get_variables_library()
        
        return {
            "success": True,
            "variables": variables
        }
    except Exception as e:
        logger.error(f"Failed to get variables library: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取变量库失败: {str(e)}")


@router.post("/templates", summary="[DEPRECATED] 创建新模板 (Phase 2 仅 admin 可用)")
async def create_template(
    template_data: TemplateCreate,
    current_user: TokenData = Depends(get_current_user)
):
    """创建新模板.

    PR-C (2026-04-27): 在 Phase 2 双态模型下, 普通用户不应再创建任意 user-scope 模板;
    每用户每 USER_OWNED 模块通过懒 seed 自动获得唯一一行私有模板, 编辑入口
    走 PUT /my-templates/{module_code}. 此接口保留仅供 admin 创建/调整 system 模板使用.
    """
    if not current_user.is_admin:
        raise HTTPException(
            status_code=403,
            detail=(
                "Phase 2 双态模型下普通用户不再支持自由创建模板; "
                "请通过 PUT /llm/prompts/my-templates/{module_code} 编辑你的私有模板."
            ),
        )
    try:
        
        service = LLMPromptTemplateService()
        
        # 转换VariableDefinition为dict
        variables_dict = {}
        for key, var_def in template_data.variables.items():
            variables_dict[key] = var_def.dict()
        
        template_dict = template_data.dict()
        template_dict['variables'] = variables_dict
        template_dict['output_format'] = template_data.output_format.dict()
        
        template_id = service.create_template(
            template_dict,
            user_id=current_user.user_id
        )
        
        if template_id:
            logger.info(f"User {current_user.username} created template {template_id}")
            return {
                "success": True,
                "template_id": template_id,
                "message": "模板创建成功"
            }
        else:
            raise HTTPException(status_code=500, detail="创建模板失败")
    except HTTPException:
        raise
    except ValueError as e:
        # 处理业务逻辑错误（如 template_code 重复）
        error_msg = str(e)
        if 'already exists' in error_msg.lower():
            raise HTTPException(status_code=400, detail=error_msg)
        raise HTTPException(status_code=400, detail=error_msg)
    except Exception as e:
        logger.error(f"Failed to create template: {str(e)}")
        error_msg = str(e)
        # 检查是否是数据库唯一约束冲突
        if 'duplicate key' in error_msg.lower() or 'unique constraint' in error_msg.lower():
            if 'template_code' in error_msg.lower():
                raise HTTPException(
                    status_code=400, 
                    detail=f"Template code '{template_dict.get('template_code', 'unknown')}' already exists. Please use a different template code."
                )
        raise HTTPException(status_code=500, detail=f"创建模板失败: {error_msg}")


@router.put("/templates/{template_id}", summary="更新模板")
async def update_template(
    template_id: int,
    updates: TemplateUpdate,
    current_user: TokenData = Depends(get_current_user)
):
    """更新模板"""
    try:
        service = LLMPromptTemplateService()
        
        # 检查模板是否存在
        template = service.get_template(template_id)
        if not template:
            raise HTTPException(status_code=404, detail="模板不存在")
        
        # 检查权限：系统模板只能由管理员更新
        if template['scope'] == 'system' and not current_user.is_admin:
            raise HTTPException(status_code=403, detail="只有管理员可以更新系统模板")
        
        # 检查权限：非管理员只能更新自己的模板
        if not current_user.is_admin and template.get('created_by') != current_user.user_id:
            raise HTTPException(status_code=403, detail="您没有权限更新此模板")
        
        # 转换为dict，排除None值
        updates_dict = {k: v for k, v in updates.dict().items() if v is not None}
        
        # 转换VariableDefinition为dict
        if 'variables' in updates_dict and updates_dict['variables']:
            variables_dict = {}
            for key, var_def in updates_dict['variables'].items():
                variables_dict[key] = var_def if isinstance(var_def, dict) else var_def.dict()
            updates_dict['variables'] = variables_dict
        
        if 'output_format' in updates_dict and updates_dict['output_format']:
            output_format = updates_dict['output_format']
            updates_dict['output_format'] = output_format if isinstance(output_format, dict) else output_format.dict()
        
        success = service.update_template(
            template_id,
            updates_dict,
            user_id=current_user.user_id
        )
        
        if success:
            logger.info(f"User {current_user.username} updated template {template_id}")
            if llm_service.config_resolver:
                llm_service.config_resolver.clear_cache()
                logger.info("ConfigResolver cache cleared after template update")
            return {"success": True, "message": "模板更新成功"}
        else:
            raise HTTPException(status_code=500, detail="更新模板失败")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update template: {str(e)}")
        raise HTTPException(status_code=500, detail=f"更新模板失败: {str(e)}")


@router.delete("/templates/{template_id}", summary="删除模板")
async def delete_template(
    template_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """删除模板：系统模板软删除，用户/共享模板硬删除（从数据库物理删除）"""
    try:
        service = LLMPromptTemplateService()
        
        # 检查模板是否存在
        template = service.get_template(template_id)
        if not template:
            raise HTTPException(status_code=404, detail="模板不存在")
        
        # 检查权限：系统模板只能由管理员删除
        if template['scope'] == 'system' and not current_user.is_admin:
            raise HTTPException(status_code=403, detail="只有管理员可以删除系统模板")
        
        # 检查权限：非管理员只能删除自己的模板
        if not current_user.is_admin and template.get('created_by') != current_user.user_id:
            raise HTTPException(status_code=403, detail="您没有权限删除此模板")
        
        success = service.delete_template(template_id, user_id=current_user.user_id)
        
        if success:
            logger.info(f"User {current_user.username} deleted template {template_id}")
            return {"success": True, "message": "模板删除成功"}
        else:
            raise HTTPException(status_code=500, detail="删除模板失败")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete template: {str(e)}")
        raise HTTPException(status_code=500, detail=f"删除模板失败: {str(e)}")


# === 模板测试 ===

@router.post("/templates/test", response_model=TestTemplateResponse, summary="测试模板")
async def test_template(
    request: TestTemplateRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """测试模板效果，调用真实LLM"""
    try:
        import time
        start_time = time.time()
        
        # 1. 渲染prompt
        renderer = PromptRenderer()
        rendered = renderer.render(
            template_id=request.template_id,
            template_code=request.template_code,
            variables_data=request.variables,
        )
        
        # 2. 调用LLM
        messages = []
        if rendered.system_prompt:
            messages.append(LLMMessage(role="system", content=rendered.system_prompt))
        messages.append(LLMMessage(role="user", content=rendered.user_prompt))
        
        provider = request.provider or rendered.recommended_config.get('provider')
        model = request.model or rendered.recommended_config.get('model')
        
        response = await llm_service.chat_completion(
            messages,
            provider=provider,
            model=model,
            temperature=rendered.recommended_config.get('temperature'),
            max_tokens=rendered.recommended_config.get('max_tokens')
        )
        
        # 3. 解析输出
        parsed_output = renderer.parse_output(rendered.template_id, response.content)
        
        response_time = time.time() - start_time
        
        # 4. 记录使用日志
        service = LLMPromptTemplateService()
        service.log_template_usage(
            template_id=rendered.template_id,
            user_id=current_user.user_id,
            rendered_prompt=f"System: {rendered.system_prompt}\n\nUser: {rendered.user_prompt}",
            variables_data=rendered.variables_used,
            response_content=response.content,
            provider=response.provider,
            model=response.model,
            tokens_used=response.usage.get('total_tokens', 0),
            cost=response.cost,
            response_time_ms=int(response_time * 1000),
            success=True
        )
        
        return TestTemplateResponse(
            success=True,
            rendered_prompt={
                "system_prompt": rendered.system_prompt or "",
                "user_prompt": rendered.user_prompt
            },
            llm_response=response.content,
            parsed_output=parsed_output,
            tokens_used=response.usage.get('total_tokens'),
            cost=response.cost,
            response_time=response_time,
            error=None
        )
        
    except Exception as e:
        logger.error(f"Template test failed: {str(e)}")
        return TestTemplateResponse(
            success=False,
            rendered_prompt={},
            llm_response=None,
            parsed_output=None,
            tokens_used=None,
            cost=None,
            response_time=None,
            error=str(e)
        )


# === 统计分析 ===

@router.get("/templates/{template_id}/stats", response_model=TemplateStatsResponse, summary="获取模板使用统计")
async def get_template_stats(
    template_id: int,
    days: int = Query(30, ge=1, le=365, description="统计天数"),
    current_user: TokenData = Depends(get_current_user)
):
    """获取模板的使用统计"""
    try:
        service = LLMPromptTemplateService()
        stats = service.get_template_stats(template_id, days=days)
        
        if not stats:
            raise HTTPException(status_code=404, detail="未找到统计数据")
        
        return stats
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get template stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取统计数据失败: {str(e)}")


@router.get("/templates/popular", response_model=List[TemplateResponse], summary="获取热门模板")
async def get_popular_templates(
    category_id: Optional[int] = Query(None, description="分类ID过滤"),
    limit: int = Query(10, ge=1, le=50, description="返回数量"),
    current_user: TokenData = Depends(get_current_user)
):
    """获取热门模板（按使用次数排序）"""
    try:
        service = LLMPromptTemplateService()
        templates = service.get_popular_templates(category_id=category_id, limit=limit)
        return templates
    except Exception as e:
        logger.error(f"Failed to get popular templates: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取热门模板失败: {str(e)}")


# === 导出模板 ===

@router.get("/templates/export/{template_id}", summary="导出模板配置")
async def export_template(
    template_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """导出模板配置为JSON（用于分享或备份）"""
    try:
        service = LLMPromptTemplateService()
        template = service.get_template(template_id)
        
        if not template:
            raise HTTPException(status_code=404, detail="模板不存在")
        
        # 移除ID等自动生成的字段
        export_data = {
            'template_code': template['template_code'],
            'template_name': template['template_name'],
            'category_code': template.get('category_code'),
            'system_prompt': template.get('system_prompt'),
            'user_prompt_template': template['user_prompt_template'],
            'variables': template.get('variables', {}),
            'output_format': template.get('output_format', {}),
            'recommended_provider': template.get('recommended_provider'),
            'recommended_model': template.get('recommended_model'),
            'temperature': template.get('temperature'),
            'max_tokens': template.get('max_tokens'),
            'description': template.get('description'),
            'tags': template.get('tags', []),
            'example_input': template.get('example_input', {}),
            'example_output': template.get('example_output')
        }
        
        return JSONResponse(
            content=export_data,
            headers={
                "Content-Disposition": f"attachment; filename=template_{template['template_code']}.json"
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to export template: {str(e)}")
        raise HTTPException(status_code=500, detail=f"导出模板失败: {str(e)}")


# ==================== 用户模板偏好 ====================

class SetPreferenceRequest(BaseModel):
    template_id: int = Field(..., description="选择的模板ID")


class PreferenceResponse(BaseModel):
    id: int
    module_code: str
    template_id: int
    template_code: str
    template_name: str
    scope: str
    created_at: Optional[str]
    updated_at: Optional[str]


class ModuleInfo(BaseModel):
    module_code: str
    module_name: str
    description: str
    configurable: bool
    default_template_code: Optional[str]


@router.get("/modules", summary="获取可配模块列表")
async def list_configurable_modules(
    current_user: TokenData = Depends(get_current_user)
):
    """返回所有已注册的功能模块及其配置信息"""
    try:
        pref_service = TemplatePreferenceService()
        modules = pref_service.get_modules()
        return {"success": True, "modules": modules}
    except Exception as e:
        logger.error(f"Failed to list modules: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取模块列表失败: {str(e)}")


@router.get("/preferences", summary="获取当前用户所有模板偏好")
async def list_preferences(
    current_user: TokenData = Depends(get_current_user)
):
    """获取当前用户所有已设置的模板偏好"""
    try:
        pref_service = TemplatePreferenceService()
        preferences = pref_service.list_user_preferences(current_user.user_id)
        return {"success": True, "preferences": preferences}
    except Exception as e:
        logger.error(f"Failed to list preferences: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取偏好列表失败: {str(e)}")


@router.get("/preferences/{module_code}", summary="获取指定模块的模板偏好")
async def get_preference(
    module_code: str,
    current_user: TokenData = Depends(get_current_user)
):
    """获取当前用户在指定模块的模板偏好，未设置时返回 null"""
    try:
        pref_service = TemplatePreferenceService()
        preference = pref_service.get_preference(current_user.user_id, module_code)
        return {"success": True, "preference": preference}
    except Exception as e:
        logger.error(f"Failed to get preference: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取偏好失败: {str(e)}")


@router.put("/preferences/{module_code}", summary="设置模板偏好")
async def set_preference(
    module_code: str,
    body: SetPreferenceRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """设置当前用户在指定模块的模板偏好"""
    try:
        pref_service = TemplatePreferenceService()
        pref_service.set_preference(
            user_id=current_user.user_id,
            module_code=module_code,
            template_id=body.template_id,
            is_admin=current_user.is_admin or False,
        )
        return {"success": True, "message": "偏好设置成功"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to set preference: {str(e)}")
        raise HTTPException(status_code=500, detail=f"设置偏好失败: {str(e)}")


@router.delete("/preferences/{module_code}", summary="重置模板偏好为系统默认")
async def delete_preference(
    module_code: str,
    current_user: TokenData = Depends(get_current_user)
):
    """删除当前用户在指定模块的偏好，恢复为系统默认模板"""
    try:
        pref_service = TemplatePreferenceService()
        deleted = pref_service.delete_preference(current_user.user_id, module_code)
        if deleted:
            return {"success": True, "message": "已恢复为系统默认模板"}
        else:
            return {"success": True, "message": "该模块未设置过偏好"}
    except Exception as e:
        logger.error(f"Failed to delete preference: {str(e)}")
        raise HTTPException(status_code=500, detail=f"重置偏好失败: {str(e)}")


# ==================== PR-C: My Templates (Phase 2 双态模型) ====================
# 新模型: 每用户每 USER_OWNED 模块 1 行私有模板; SYSTEM_FIXED 模块全局共享.
# 这一组接口替代了原 "Create Template / 模板偏好" 复合工作流, 给前端
# 提供"双 Tab + 直接编辑"的简化入口.

class MyTemplateItem(BaseModel):
    """My Templates 列表项 (一个 module 一项)."""

    module_code: str
    module_name: str
    module_description: str
    kind: str = Field(..., description="'user_owned' | 'system_fixed'")
    configurable: bool

    template_id: int
    template_code: str
    template_name: str
    scope: str
    is_active: bool

    system_prompt: Optional[str]
    user_prompt_template: str
    variables: Dict[str, Any]
    output_format: Dict[str, Any]
    recommended_provider: Optional[str]
    recommended_model: Optional[str]
    temperature: float
    max_tokens: int
    description: Optional[str]

    parent_template_id: Optional[int] = Field(
        None, description="USER_OWNED 模板 clone 自的 system 模板 id"
    )
    updated_at: Optional[str]


class UpdateMyTemplateRequest(BaseModel):
    """编辑用户私有模板 (PUT /my-templates/{module_code}). 仅允许编辑 prompt 内容与 LLM 配置."""

    system_prompt: Optional[str] = None
    user_prompt_template: Optional[str] = None
    variables: Optional[Dict[str, VariableDefinition]] = None
    output_format: Optional[OutputFormatSchema] = None
    recommended_provider: Optional[str] = None
    recommended_model: Optional[str] = None
    temperature: Optional[float] = Field(None, ge=0, le=2)
    max_tokens: Optional[int] = Field(None, ge=1)


def _resolve_module_or_404(module_code: str) -> Dict[str, Any]:
    pref = TemplatePreferenceService()
    for m in pref.MODULE_REGISTRY:
        if m["module_code"] == module_code:
            return m
    raise HTTPException(status_code=404, detail=f"unknown module_code: {module_code}")


def _build_my_template_item(
    module: Dict[str, Any],
    template: Dict[str, Any],
    pref: TemplatePreferenceService,
) -> MyTemplateItem:
    is_user_owned = module["module_code"] in pref.USER_OWNED_MODULES
    return MyTemplateItem(
        module_code=module["module_code"],
        module_name=module["module_name"],
        module_description=module["description"],
        kind="user_owned" if is_user_owned else "system_fixed",
        configurable=module.get("configurable", False),
        template_id=template["id"],
        template_code=template["template_code"],
        template_name=template["template_name"],
        scope=template["scope"],
        is_active=bool(template.get("is_active", True)),
        system_prompt=template.get("system_prompt"),
        user_prompt_template=template.get("user_prompt_template", ""),
        variables=template.get("variables", {}) or {},
        output_format=template.get("output_format", {}) or {},
        recommended_provider=template.get("recommended_provider"),
        recommended_model=template.get("recommended_model"),
        temperature=float(template.get("temperature") or 0.7),
        max_tokens=int(template.get("max_tokens") or 2000),
        description=template.get("description"),
        parent_template_id=template.get("parent_template_id"),
        updated_at=template.get("updated_at"),
    )


@router.get("/my-templates", summary="列出当前用户的所有模板 (USER_OWNED + SYSTEM_FIXED)")
async def list_my_templates(
    current_user: TokenData = Depends(get_current_user)
):
    """返回每个 module 的 1 项, 包括用户私有模板 + 系统级只读模板.

    USER_OWNED 模块如缺私有模板会触发懒 seed.
    """
    try:
        pref = TemplatePreferenceService()
        seeder = UserTemplateSeederService(preference_service=pref)
        tpl_service = LLMPromptTemplateService()

        items: List[MyTemplateItem] = []
        for module in pref.MODULE_REGISTRY:
            module_code = module["module_code"]
            try:
                if module_code in pref.USER_OWNED_MODULES:
                    template = seeder.ensure_user_template(current_user.user_id, module_code)
                else:
                    default_code = pref.SYSTEM_DEFAULTS.get(module_code) or module.get("default_template_code")
                    if not default_code:
                        continue
                    template = tpl_service.get_template_by_code(default_code)
                    if not template:
                        continue
                items.append(_build_my_template_item(module, template, pref))
            except Exception as inner_e:
                logger.warning(f"list_my_templates: skip module {module_code}: {inner_e}")

        return {"success": True, "items": [item.dict() for item in items]}
    except Exception as e:
        logger.error(f"Failed to list my-templates: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取我的模板列表失败: {str(e)}")


@router.get("/my-templates/{module_code}", summary="获取当前用户在指定模块的当前模板")
async def get_my_template(
    module_code: str,
    current_user: TokenData = Depends(get_current_user)
):
    """USER_OWNED → 返回用户私有模板 (缺则懒 seed); SYSTEM_FIXED → 返回 system default."""
    try:
        pref = TemplatePreferenceService()
        module = _resolve_module_or_404(module_code)

        if module_code in pref.USER_OWNED_MODULES:
            seeder = UserTemplateSeederService(preference_service=pref)
            template = seeder.ensure_user_template(current_user.user_id, module_code)
        else:
            tpl_service = LLMPromptTemplateService()
            default_code = pref.SYSTEM_DEFAULTS.get(module_code) or module.get("default_template_code")
            if not default_code:
                raise HTTPException(status_code=404, detail=f"module '{module_code}' has no system default")
            template = tpl_service.get_template_by_code(default_code)
            if not template:
                raise HTTPException(status_code=404, detail=f"system default template not found")

        return {"success": True, "item": _build_my_template_item(module, template, pref).dict()}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get my-template '{module_code}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取模板失败: {str(e)}")


@router.put("/my-templates/{module_code}", summary="编辑当前用户在指定模块的私有模板")
async def update_my_template(
    module_code: str,
    body: UpdateMyTemplateRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """更新用户私有模板的 prompt / 变量 / LLM 配置.

    SYSTEM_FIXED 模块禁止 (走 admin 系统模板编辑入口).
    """
    pref = TemplatePreferenceService()
    module = _resolve_module_or_404(module_code)
    if module_code not in pref.USER_OWNED_MODULES:
        raise HTTPException(
            status_code=403,
            detail=f"module '{module_code}' is system-fixed; only admin can edit via /system-templates",
        )

    try:
        seeder = UserTemplateSeederService(preference_service=pref)
        owned = seeder.ensure_user_template(current_user.user_id, module_code)

        updates: Dict[str, Any] = {}
        if body.system_prompt is not None:
            updates["system_prompt"] = body.system_prompt
        if body.user_prompt_template is not None:
            updates["user_prompt_template"] = body.user_prompt_template
        if body.variables is not None:
            updates["variables"] = {k: v.dict() for k, v in body.variables.items()}
        if body.output_format is not None:
            updates["output_format"] = body.output_format.dict()
        if body.recommended_provider is not None:
            updates["recommended_provider"] = body.recommended_provider
        if body.recommended_model is not None:
            updates["recommended_model"] = body.recommended_model
        if body.temperature is not None:
            updates["temperature"] = body.temperature
        if body.max_tokens is not None:
            updates["max_tokens"] = body.max_tokens

        if not updates:
            return {"success": True, "message": "无需更新"}

        tpl_service = LLMPromptTemplateService()
        ok = tpl_service.update_template(owned["id"], updates, user_id=current_user.user_id)
        if not ok:
            raise HTTPException(status_code=500, detail="模板更新失败")

        # 取最新
        latest = tpl_service.get_template(owned["id"])
        return {
            "success": True,
            "item": _build_my_template_item(module, latest, pref).dict(),
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update my-template '{module_code}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"模板更新失败: {str(e)}")


@router.post("/my-templates/{module_code}/reset", summary="重置用户私有模板为系统默认 (覆盖, 无历史)")
async def reset_my_template(
    module_code: str,
    current_user: TokenData = Depends(get_current_user)
):
    """覆盖式重置: prompt / 变量 / LLM 配置全部回到 system default;
    template_id 不变, 用户其他引用关系不会断."""
    pref = TemplatePreferenceService()
    module = _resolve_module_or_404(module_code)
    if module_code not in pref.USER_OWNED_MODULES:
        raise HTTPException(
            status_code=403,
            detail=f"module '{module_code}' is system-fixed; reset not applicable",
        )

    try:
        seeder = UserTemplateSeederService(preference_service=pref)
        latest = seeder.reset_module(current_user.user_id, module_code)
        return {
            "success": True,
            "message": "已重置为系统默认",
            "item": _build_my_template_item(module, latest, pref).dict(),
        }
    except Exception as e:
        logger.error(f"Failed to reset my-template '{module_code}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"重置失败: {str(e)}")


# ==================== PR-C: Admin → System Templates ====================
# 管理员独立路径, 编辑系统级模板 (SYSTEM_FIXED 模块的全局共享 prompt).
# 普通用户不可见这套接口.


def _require_admin(current_user: TokenData) -> None:
    if not current_user.is_admin:
        raise HTTPException(status_code=403, detail="仅管理员可操作系统级模板")


@router.get("/system-templates", summary="[Admin] 列出所有 system-scope 模板")
async def admin_list_system_templates(
    current_user: TokenData = Depends(get_current_user)
):
    _require_admin(current_user)
    try:
        tpl_service = LLMPromptTemplateService()
        templates = tpl_service.list_templates(scope="system", active_only=True)
        return {"success": True, "templates": templates}
    except Exception as e:
        logger.error(f"Failed to list system templates: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取系统模板列表失败: {str(e)}")


@router.put("/system-templates/{template_id}", summary="[Admin] 编辑 system-scope 模板内容")
async def admin_update_system_template(
    template_id: int,
    body: UpdateMyTemplateRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """允许 admin 编辑 system 模板的 prompt / 变量 / LLM 配置 (复用 UpdateMyTemplateRequest 字段集)."""
    _require_admin(current_user)
    try:
        tpl_service = LLMPromptTemplateService()
        target = tpl_service.get_template(template_id)
        if not target:
            raise HTTPException(status_code=404, detail="模板不存在")
        if target.get("scope") != "system":
            raise HTTPException(
                status_code=400,
                detail="此接口仅用于 system-scope 模板; 用户私有模板请走 /my-templates",
            )

        updates: Dict[str, Any] = {}
        for key in (
            "system_prompt", "user_prompt_template",
            "recommended_provider", "recommended_model",
            "temperature", "max_tokens",
        ):
            val = getattr(body, key, None)
            if val is not None:
                updates[key] = val
        if body.variables is not None:
            updates["variables"] = {k: v.dict() for k, v in body.variables.items()}
        if body.output_format is not None:
            updates["output_format"] = body.output_format.dict()

        if not updates:
            return {"success": True, "message": "无需更新"}

        ok = tpl_service.update_template(template_id, updates, user_id=current_user.user_id)
        if not ok:
            raise HTTPException(status_code=500, detail="系统模板更新失败")

        return {"success": True, "template": tpl_service.get_template(template_id)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update system template {template_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"更新系统模板失败: {str(e)}")

