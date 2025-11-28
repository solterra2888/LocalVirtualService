# -*- coding: utf-8 -*-
"""
LLM配置API端点
"""

import os
import json
import time
from decimal import Decimal
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from typing import Dict, List, Optional, Any
from pydantic import BaseModel

from backend.core.llm_framework import (
    llm_service, 
    LLMMessage, 
    LLMProviderType,
    LLMModelStatus
)
from backend.auth.jwt_utils import get_current_admin_user
from backend.schemas.user import TokenData
from backend.utils.logger import get_logger
from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config

logger = get_logger("llm_config_api")

router = APIRouter(
    prefix="/llm",
    tags=["LLM配置"],
    dependencies=[Depends(get_current_admin_user)]
)

# 辅助函数
def _get_default_providers():
    """获取默认的提供商配置"""
    provider_definitions = {
        "qwen": {
            "displayName": "Qwen",
            "description": "Alibaba Cloud Qwen large language model with multiple specifications",
            "icon": "🔷",
            "models": ["qwen-turbo", "qwen-plus", "qwen-max", "qwen-max-longcontext"],
            "baseUrl": "https://dashscope.aliyuncs.com"
        },
        "doubao": {
            "displayName": "Doubao",
            "description": "ByteDance Doubao large language model with strong Chinese understanding",
            "icon": "🫘",
            "models": ["doubao-seed-1-6-250615"],
            "baseUrl": "https://ark.cn-beijing.volces.com"
        },
        "glm": {
            "displayName": "GLM",
            "description": "Tsinghua Zhipu AI large language model",
            "icon": "🧠",
            "models": ["glm-4.6", "glm-4", "glm-4-flash"],
            "baseUrl": "https://open.bigmodel.cn"
        }
    }
    
    providers = []
    for provider_id, definition in provider_definitions.items():
        provider_config = LLMProviderConfig(
            id=provider_id,
            name=provider_id,
            displayName=definition["displayName"],
            enabled=False,
            apiKey="",
            baseUrl=definition["baseUrl"],
            models=definition["models"],
            defaultModel=definition["models"][0],
            maxTokens=2000,
            temperature=0.7,
            topP=0.8,
            description=definition["description"],
            icon=definition["icon"]
        )
        providers.append(provider_config)
    
    return providers

# Pydantic模型定义
class LLMProviderConfig(BaseModel):
    id: str
    name: str
    displayName: str
    enabled: bool
    apiKey: str
    baseUrl: str
    models: List[str]
    defaultModel: str
    maxTokens: int
    temperature: float
    topP: float
    description: str
    icon: str

class GlobalSettings(BaseModel):
    """
    全局设置 - 只负责系统级策略
    注意：不再包含参数默认值（timeout, retryCount等）
    这些参数由 Provider Config 提供
    """
    enableLogging: bool
    enableMonitoring: bool

class LLMSystemConfig(BaseModel):
    defaultProvider: str
    fallbackEnabled: bool
    fallbackOrder: List[str]
    globalSettings: GlobalSettings
    providers: List[LLMProviderConfig]

class TestConnectionRequest(BaseModel):
    providerId: str
    apiKey: str
    baseUrl: str
    defaultModel: str
    maxTokens: int
    temperature: float
    topP: float

class TestConnectionResponse(BaseModel):
    success: bool
    provider: str
    model: str
    responseTime: float
    message: Optional[str] = None
    error: Optional[str] = None

class UsageStats(BaseModel):
    provider: str
    totalRequests: int
    successRate: float
    avgResponseTime: float
    totalTokens: int
    totalCost: float
    currency: str


@router.get("/config", response_model=LLMSystemConfig)
async def get_llm_config(current_user: TokenData = Depends(get_current_admin_user)):
    """获取LLM系统配置（从数据库）"""
    try:
        # 获取数据库连接
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        # 从数据库获取提供商配置
        db_providers = db_manager.get_llm_provider_configs(enabled_only=False)
        
        providers = []
        for row in db_providers:
            # 解析数据库行数据
            provider_config = LLMProviderConfig(
                id=row[1],  # provider_id
                name=row[2],  # provider_name
                displayName=row[3],  # display_name
                enabled=row[6],  # enabled
                apiKey=row[4],  # api_key
                baseUrl=row[5],  # base_url
                models=row[8] if row[8] else [],  # available_models (JSONB)
                defaultModel=row[7] if row[7] else "",  # default_model
                maxTokens=row[9] if row[9] else 2000,  # max_tokens
                temperature=float(row[10]) if row[10] else 0.7,  # temperature
                topP=float(row[11]) if row[11] else 0.8,  # top_p
                description=row[13] if row[13] else "",  # description
                icon=row[14] if row[14] else ""  # icon
            )
            providers.append(provider_config)
        
        # 如果数据库中没有配置，使用默认配置
        if not providers:
            logger.warning("数据库中没有LLM提供商配置，使用默认配置")
            providers = _get_default_providers()
        
        # 从数据库获取全局配置
        db_settings = db_manager.get_llm_global_settings()
        
        # 解析全局配置（处理JSONB字段可能是字符串的情况）
        def get_setting_value(key, default_value):
            """安全获取设置值，处理JSON字符串"""
            setting = db_settings.get(key, {})
            if not setting:
                return default_value
            value = setting.get('value')
            if value is None:
                return default_value
            # 如果是字符串，尝试解析JSON
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    return value
            return value
        
        default_provider = get_setting_value('default_provider', 'qwen')
        fallback_enabled = get_setting_value('fallback_enabled', True)
        fallback_order = get_setting_value('fallback_order', ['qwen', 'doubao', 'wenxin', 'glm'])
        enable_logging = get_setting_value('enable_logging', True)
        enable_monitoring = get_setting_value('enable_monitoring', True)
        
        # Global Settings 只包含系统级策略，不包含参数默认值
        global_settings = GlobalSettings(
            enableLogging=bool(enable_logging),
            enableMonitoring=bool(enable_monitoring)
        )
        
        config = LLMSystemConfig(
            defaultProvider=default_provider,
            fallbackEnabled=bool(fallback_enabled),
            fallbackOrder=fallback_order,
            globalSettings=global_settings,
            providers=providers
        )
        
        return config
        
    except Exception as e:
        logger.error(f"Failed to get LLM config: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"获取配置失败: {str(e)}")


@router.post("/config")
async def save_llm_config(
    config: LLMSystemConfig,
    current_user: TokenData = Depends(get_current_admin_user)
):
    """保存LLM系统配置到数据库"""
    try:
        logger.info(f"用户 {current_user.username} 开始更新LLM配置")
        
        # 获取数据库连接
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        # 保存每个提供商配置
        for provider in config.providers:
            config_data = {
                'provider_id': provider.id,
                'provider_name': provider.name,
                'display_name': provider.displayName,
                'api_key': provider.apiKey,
                'base_url': provider.baseUrl,
                'enabled': provider.enabled,
                'default_model': provider.defaultModel,
                'available_models': json.dumps(provider.models),
                'max_tokens': provider.maxTokens,
                'temperature': provider.temperature,
                'top_p': provider.topP,
                'description': provider.description,
                'icon': provider.icon,
                'is_default_provider': (provider.id == config.defaultProvider),
                'fallback_priority': config.fallbackOrder.index(provider.id) + 1 if provider.id in config.fallbackOrder else 999
            }
            
            success = db_manager.save_llm_provider_config(config_data, current_user.username)
            if not success:
                raise Exception(f"保存提供商 {provider.id} 配置失败")
        
        # 保存全局配置
        db_manager.update_llm_global_setting('default_provider', json.dumps(config.defaultProvider), current_user.username)
        db_manager.update_llm_global_setting('fallback_enabled', json.dumps(config.fallbackEnabled), current_user.username)
        db_manager.update_llm_global_setting('fallback_order', json.dumps(config.fallbackOrder), current_user.username)
        # Global Settings 只保存系统级策略，不保存参数默认值
        db_manager.update_llm_global_setting('enable_logging', json.dumps(config.globalSettings.enableLogging), current_user.username)
        db_manager.update_llm_global_setting('enable_monitoring', json.dumps(config.globalSettings.enableMonitoring), current_user.username)
        
        logger.info(f"用户 {current_user.username} 成功更新了LLM配置")
        
        # 重新加载LLM服务配置
        try:
            llm_service.reload_from_database()
            logger.info("LLM服务配置已重新加载")
        except AttributeError:
            logger.warning("LLM服务不支持动态重载，需要重启服务以应用新配置")
        
        return {
            "success": True, 
            "message": "配置保存成功",
            "note": "配置已保存到数据库，部分更改可能需要重启服务才能生效"
        }
        
    except Exception as e:
        logger.error(f"Failed to save LLM config: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"保存配置失败: {str(e)}")


@router.post("/test", response_model=TestConnectionResponse)
async def test_provider_connection(
    request: TestConnectionRequest,
    current_user: TokenData = Depends(get_current_admin_user)
):
    """测试提供商连接"""
    try:
        import time
        start_time = time.time()
        
        # 临时创建provider进行测试
        from backend.core.llm_framework import LLMConfig, QwenProvider, DoubaoProvider, GLMProvider
        
        provider_map = {
            "qwen": (QwenProvider, LLMProviderType.QWEN),
            "doubao": (DoubaoProvider, LLMProviderType.DOUBAO),
            "glm": (GLMProvider, LLMProviderType.GLM),
        }
        
        if request.providerId not in provider_map:
            raise HTTPException(status_code=400, detail="不支持的提供商")
        
        provider_class, provider_type = provider_map[request.providerId]
        
        # 使用请求中的参数创建测试配置
        test_config = LLMConfig(
            provider_type=provider_type,
            api_key=request.apiKey,
            base_url=request.baseUrl,
            default_model=request.defaultModel,
            models=[request.defaultModel],
            max_tokens=request.maxTokens,
            temperature=request.temperature,
            top_p=request.topP,
            timeout=30,
            retry_count=3
        )
        
        # 创建提供商实例
        provider = provider_class(test_config)
        
        # 执行连接测试
        success = await provider.test_connection()
        
        response_time = time.time() - start_time
        
        return TestConnectionResponse(
            success=success,
            provider=request.providerId,
            model=test_config.default_model,
            responseTime=response_time,
            message="连接测试成功" if success else "连接测试失败",
            error=None if success else "连接失败"
        )
        
    except Exception as e:
        logger.error(f"Connection test failed: {str(e)}")
        return TestConnectionResponse(
            success=False,
            provider=request.providerId,
            model="",
            responseTime=0.0,
            message="连接测试失败",
            error=str(e)
        )


@router.get("/status/{provider_id}")
async def get_provider_status(
    provider_id: str,
    current_user: TokenData = Depends(get_current_admin_user)
):
    """获取指定提供商状态"""
    try:
        status = llm_service.get_provider_status(provider_id)
        
        if "error" in status:
            return {
                "status": "disconnected",
                "lastChecked": "never",
                "error": status["error"]
            }
        
        return {
            "status": status.get("status", "disconnected"),
            "lastChecked": "2024-01-01T00:00:00Z",  # TODO: 实现实际的检查时间
            "models": status.get("models", []),
            "enabled": status.get("enabled", False)
        }
        
    except Exception as e:
        logger.error(f"Failed to get provider status: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取状态失败: {str(e)}")


@router.get("/stats", response_model=List[UsageStats])
async def get_usage_stats(
    range: str = "30d",
    current_user: TokenData = Depends(get_current_admin_user)
):
    """获取使用统计（从数据库）"""
    try:
        # 解析时间范围
        days = 30
        if range.endswith('d'):
            try:
                days = int(range[:-1])
            except ValueError:
                days = 30
        
        # 获取数据库连接
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        # 从数据库获取统计数据
        db_stats = db_manager.get_llm_usage_stats(provider_id=None, days=days)
        
        stats_list = []
        for row in db_stats:
            stats = UsageStats(
                provider=row[0],  # provider_id
                totalRequests=int(row[1]) if row[1] else 0,  # total_requests
                successRate=float(row[3]) if row[3] else 0.0,  # success_rate
                avgResponseTime=float(row[4]) / 1000 if row[4] else 0.0,  # avg_response_time (转换为秒)
                totalTokens=int(row[5]) if row[5] else 0,  # total_tokens
                totalCost=float(row[6]) if row[6] else 0.0,  # total_cost
                currency=row[7] if row[7] else "RMB"  # currency
            )
            stats_list.append(stats)
        
        # 如果数据库中没有统计数据，返回提供商的基本信息
        if not stats_list:
            logger.info("数据库中没有使用统计数据，返回空统计")
            # 从配置表获取已启用的提供商
            providers = db_manager.get_llm_provider_configs(enabled_only=True)
            for provider_row in providers:
                stats = UsageStats(
                    provider=provider_row[1],  # provider_id
                    totalRequests=provider_row[19] if provider_row[19] else 0,  # total_requests from config
                    successRate=100.0,
                    avgResponseTime=0.0,
                    totalTokens=provider_row[20] if provider_row[20] else 0,  # total_tokens from config
                    totalCost=float(provider_row[21]) if provider_row[21] else 0.0,  # total_cost from config
                    currency="RMB"
                )
                stats_list.append(stats)
        
        return stats_list
        
    except Exception as e:
        logger.error(f"Failed to get usage stats: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"获取统计失败: {str(e)}")


@router.post("/models")
async def get_provider_models(
    request: TestConnectionRequest,
    current_user: TokenData = Depends(get_current_admin_user)
):
    """获取提供商支持的模型列表"""
    try:
        # 预定义的模型列表
        model_mapping = {
            "qwen": ["qwen-turbo", "qwen-plus", "qwen-max", "qwen-max-longcontext"],
            "doubao": ["doubao-seed-1-6-250615"],
            "glm": ["glm-4.6", "glm-4", "glm-4-flash"]
        }
        
        models = model_mapping.get(request.providerId, [])
        
        return {"models": models}
        
    except Exception as e:
        logger.error(f"Failed to get provider models: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取模型列表失败: {str(e)}")


@router.post("/reset/{provider_id}")
async def reset_provider(
    provider_id: str,
    current_user: TokenData = Depends(get_current_admin_user)
):
    """重置提供商配置"""
    try:
        # TODO: 实现重置逻辑
        logger.info(f"用户 {current_user.username} 重置了提供商 {provider_id} 的配置")
        
        return {"success": True, "message": f"提供商 {provider_id} 配置已重置"}
        
    except Exception as e:
        logger.error(f"Failed to reset provider: {str(e)}")
        raise HTTPException(status_code=500, detail=f"重置配置失败: {str(e)}")


@router.get("/export")
async def export_config(current_user: TokenData = Depends(get_current_admin_user)):
    """导出配置"""
    try:
        # 获取当前配置
        config = await get_llm_config(current_user)
        
        # 移除敏感信息
        export_config = config.dict()
        for provider in export_config["providers"]:
            provider["apiKey"] = "***"  # 隐藏API密钥
        
        from fastapi.responses import JSONResponse
        import json
        
        response = JSONResponse(
            content=export_config,
            headers={
                "Content-Disposition": "attachment; filename=llm_config.json"
            }
        )
        
        return response
        
    except Exception as e:
        logger.error(f"Failed to export config: {str(e)}")
        raise HTTPException(status_code=500, detail=f"导出配置失败: {str(e)}")


@router.post("/import")
async def import_config(
    # TODO: 实现文件上传和配置导入
    current_user: TokenData = Depends(get_current_admin_user)
):
    """导入配置"""
    try:
        # TODO: 实现配置导入逻辑
        return {"success": True, "message": "配置导入成功"}
        
    except Exception as e:
        logger.error(f"Failed to import config: {str(e)}")
        raise HTTPException(status_code=500, detail=f"导入配置失败: {str(e)}")


@router.post("/test-global")
async def test_global_settings(current_user: TokenData = Depends(get_current_admin_user)):
    """测试全局设置功能"""
    try:
        logger.info(f"用户 {current_user.username} 开始测试全局设置")
        
        # 获取数据库连接
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        
        # 获取全局设置
        db_settings = db_manager.get_llm_global_settings()
        
        test_results = []
        
        # 解析设置值的辅助函数
        def get_setting_value(key, default_value):
            setting = db_settings.get(key, {})
            if not setting:
                return default_value
            value = setting.get('value')
            if value is None:
                return default_value
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    return value
            return value
        
        # 测试1: 默认提供商设置
        default_provider = get_setting_value('default_provider', 'qwen')
        providers = db_manager.get_llm_provider_configs(enabled_only=True)
        enabled_provider_ids = [p[1] for p in providers]  # provider_id在索引1
        
        if default_provider in enabled_provider_ids:
            test_results.append({
                "test": "默认提供商配置",
                "status": "success",
                "message": f"默认提供商 '{default_provider}' 已启用",
                "details": f"当前默认提供商: {default_provider}"
            })
        else:
            test_results.append({
                "test": "默认提供商配置",
                "status": "warning",
                "message": f"默认提供商 '{default_provider}' 未启用或不存在",
                "details": f"已启用的提供商: {enabled_provider_ids}"
            })
        
        # 测试2: 故障转移配置
        fallback_enabled = get_setting_value('fallback_enabled', True)
        fallback_order = get_setting_value('fallback_order', ['qwen', 'doubao', 'glm'])
        
        if fallback_enabled:
            valid_fallback_providers = [p for p in fallback_order if p in enabled_provider_ids]
            if len(valid_fallback_providers) >= 2:
                test_results.append({
                    "test": "故障转移配置",
                    "status": "success",
                    "message": f"故障转移已启用，有 {len(valid_fallback_providers)} 个可用提供商",
                    "details": f"故障转移顺序: {valid_fallback_providers}"
                })
            else:
                test_results.append({
                    "test": "故障转移配置",
                    "status": "warning",
                    "message": "故障转移已启用，但可用提供商少于2个",
                    "details": f"可用提供商: {valid_fallback_providers}"
                })
        else:
            test_results.append({
                "test": "故障转移配置",
                "status": "info",
                "message": "故障转移已禁用",
                "details": "系统将仅使用默认提供商"
            })
        
        # 测试3: 超时和重试设置
        global_timeout = get_setting_value('global_timeout', 30)
        global_retry_count = get_setting_value('global_retry_count', 3)
        
        if 5 <= global_timeout <= 300 and 0 <= global_retry_count <= 10:
            test_results.append({
                "test": "超时和重试配置",
                "status": "success",
                "message": "超时和重试设置在合理范围内",
                "details": f"超时: {global_timeout}秒, 重试: {global_retry_count}次"
            })
        else:
            test_results.append({
                "test": "超时和重试配置",
                "status": "warning",
                "message": "超时或重试设置可能不合理",
                "details": f"超时: {global_timeout}秒 (建议5-300), 重试: {global_retry_count}次 (建议0-10)"
            })
        
        # 测试4: 日志和监控设置
        enable_logging = get_setting_value('enable_logging', True)
        enable_monitoring = get_setting_value('enable_monitoring', True)
        
        test_results.append({
            "test": "日志和监控配置",
            "status": "info",
            "message": f"日志记录: {'启用' if enable_logging else '禁用'}, 性能监控: {'启用' if enable_monitoring else '禁用'}",
            "details": "这些设置影响系统性能分析和问题诊断能力"
        })
        
        # 测试5: 实际故障转移测试（如果启用）
        if fallback_enabled and len(valid_fallback_providers) >= 2:
            try:
                # 创建测试消息
                test_messages = [LLMMessage(role="user", content="测试全局设置")]
                
                # 尝试使用LLM服务进行实际调用测试（带超时）
                import asyncio
                try:
                    response = await asyncio.wait_for(
                        llm_service.chat_completion(test_messages),
                        timeout=30.0  # 30秒超时
                    )
                    
                    test_results.append({
                        "test": "实际调用测试",
                        "status": "success",
                        "message": f"成功通过 {response.provider} 提供商获得响应",
                        "details": f"响应时间: {response.response_time:.2f}秒, 使用模型: {response.model}"
                    })
                except asyncio.TimeoutError:
                    test_results.append({
                        "test": "实际调用测试",
                        "status": "warning",
                        "message": "调用测试超时（30秒）",
                        "details": "提供商响应较慢，但配置可能正常"
                    })
                except Exception as e:
                    test_results.append({
                        "test": "实际调用测试",
                        "status": "error",
                        "message": f"调用测试失败: {str(e)}",
                        "details": "请检查提供商配置和API密钥是否正确"
                    })
            except Exception as e:
                test_results.append({
                    "test": "实际调用测试",
                    "status": "error",
                    "message": f"测试初始化失败: {str(e)}",
                    "details": "无法创建测试环境"
                })
        
        # 计算整体状态
        success_count = len([r for r in test_results if r["status"] == "success"])
        warning_count = len([r for r in test_results if r["status"] == "warning"])
        error_count = len([r for r in test_results if r["status"] == "error"])
        
        overall_status = "success"
        if error_count > 0:
            overall_status = "error"
        elif warning_count > 0:
            overall_status = "warning"
        
        logger.info(f"全局设置测试完成: {success_count}个成功, {warning_count}个警告, {error_count}个错误")
        
        return {
            "success": True,
            "overall_status": overall_status,
            "summary": f"测试完成: {success_count}个成功, {warning_count}个警告, {error_count}个错误",
            "test_results": test_results,
            "timestamp": time.time(),
            "tested_by": current_user.username
        }
        
    except Exception as e:
        logger.error(f"Failed to test global settings: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"测试全局设置失败: {str(e)}")


@router.post("/reload")
async def reload_llm_config(current_user: TokenData = Depends(get_current_admin_user)):
    """强制重新加载LLM配置"""
    try:
        logger.info(f"用户 {current_user.username} 请求重新加载LLM配置")
        
        # 强制重新加载配置
        llm_service.reload_from_database()
        
        # 检查重新加载后的状态
        provider_count = len(llm_service.providers)
        enabled_providers = [p for p in llm_service.providers.keys()]
        
        logger.info(f"LLM配置重新加载完成，已加载 {provider_count} 个提供商: {enabled_providers}")
        
        return {
            "success": True,
            "message": "LLM配置重新加载成功",
            "provider_count": provider_count,
            "enabled_providers": enabled_providers,
            "default_provider": llm_service.default_provider,
            "fallback_enabled": llm_service.fallback_enabled,
            "fallback_order": llm_service.fallback_order
        }
        
    except Exception as e:
        logger.error(f"Failed to reload LLM config: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"重新加载配置失败: {str(e)}")
