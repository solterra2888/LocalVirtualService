# -*- coding: utf-8 -*-
"""
用户新闻筛选预设 API
支持用户保存和管理个性化的新闻筛选条件
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
import json
from datetime import datetime

from backend.schemas.user_news_presets import (
    CreatePresetRequest,
    UpdatePresetRequest,
    PresetResponse,
    PresetListResponse,
    PresetOperationResponse
)
from backend.auth.jwt_utils import get_current_user
from backend.schemas.user import TokenData
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import get_logger
from backend.utils.database_config import get_database_config

logger = get_logger("user_news_presets_api")

router = APIRouter(
    prefix="/user/news-presets",
    tags=["用户新闻筛选预设"]
)


# ==================== 系统预设配置 ====================

SYSTEM_PRESETS = [
    {
        "preset_name": "💎 事实性新闻",
        "preset_type": "system",
        "description": "只显示事实性新闻，过滤观点文章",
        "icon": "💎",
        "filter_config": {
            "news_value": ["fact"],
            "urgency": ["urgent", "timely"],
            "min_confidence": 0.7
        },
        "scope_type": "global"
    },
    {
        "preset_name": "⭐ 重要新闻",
        "preset_type": "system",
        "description": "紧急和及时的高相关度新闻",
        "icon": "⭐",
        "filter_config": {
            "urgency": ["urgent", "timely"],
            "min_confidence": 0.7,
            "only_primary": True
        },
        "scope_type": "global"
    },
    {
        "preset_name": "📊 财报季",
        "preset_type": "system",
        "description": "财报发布和业绩相关新闻",
        "icon": "📊",
        "filter_config": {
            "news_value": ["fact"],
            "event_types": ["earnings_report", "financial_results"],
            "min_confidence": 0.6
        },
        "scope_type": "global"
    },
    {
        "preset_name": "🤝 并购重组",
        "preset_type": "system",
        "description": "并购、重组、战略投资相关",
        "icon": "🤝",
        "filter_config": {
            "event_types": ["merger_acquisition", "strategic_investment"],
            "urgency": ["urgent", "timely"]
        },
        "scope_type": "global"
    },
    {
        "preset_name": "⚖️ 监管合规",
        "preset_type": "system",
        "description": "监管处罚、合规问题",
        "icon": "⚖️",
        "filter_config": {
            "event_types": ["regulatory_news", "legal_issue"],
            "urgency": ["urgent", "timely"]
        },
        "scope_type": "global"
    },
    {
        "preset_name": "🚀 产品创新",
        "preset_type": "system",
        "description": "新产品、新技术发布",
        "icon": "🚀",
        "filter_config": {
            "event_types": ["product_launch", "technology_innovation"]
        },
        "scope_type": "global"
    }
]


# ==================== API 端点 ====================

@router.get("/", response_model=PresetListResponse, summary="获取用户预设列表")
async def get_user_presets(
    scope_type: Optional[str] = Query(None, description="筛选范围类型: global | symbol_specific"),
    symbol_code: Optional[str] = Query(None, description="股票代码"),
    include_system: bool = Query(False, description="是否包含系统预设"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取用户的筛选预设列表
    
    - scope_type: 筛选范围类型（global | symbol_specific）
    - symbol_code: 股票代码（获取特定股票的预设）
    - include_system: 是否包含系统预设（从数据库读取）
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        cursor = conn.cursor()
        
        # 构建查询条件
        conditions = []
        params = []
        
        if include_system:
            # 包含系统预设和用户预设
            conditions.append("(user_id = %s OR is_system_global = true)")
            params.append(current_user.user_id)
        else:
            # 仅用户预设
            conditions.append("user_id = %s")
            params.append(current_user.user_id)
        
        if scope_type:
            conditions.append("scope_type = %s")
            params.append(scope_type)
        
        if symbol_code:
            conditions.append("symbol_code = %s")
            params.append(symbol_code)
        
        where_clause = " AND ".join(conditions)
        
        # 查询预设（包括系统预设和用户预设）
        query = f"""
            SELECT 
                id, user_id, preset_name, preset_type, description, icon,
                filter_config, scope_type, symbol_code, is_default, is_favorite,
                sort_order, usage_count, last_used_at, created_at, updated_at,
                is_system_global
            FROM user_news_filter_presets
            WHERE {where_clause}
            ORDER BY is_system_global DESC, is_favorite DESC, is_default DESC, 
                     sort_order ASC, usage_count DESC, created_at DESC
        """
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        presets = []
        for row in rows:
            preset = {
                'id': row[0],
                'user_id': row[1],
                'preset_name': row[2],
                'preset_type': row[3],
                'description': row[4],
                'icon': row[5],
                'filter_config': row[6],
                'scope_type': row[7],
                'symbol_code': row[8],
                'is_default': row[9],
                'is_favorite': row[10],
                'sort_order': row[11],
                'usage_count': row[12],
                'last_used_at': row[13].isoformat() if row[13] else None,
                'created_at': row[14].isoformat() if row[14] else None,
                'updated_at': row[15].isoformat() if row[15] else None
            }
            presets.append(preset)
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "total": len(presets),
            "presets": presets
        }
        
    except Exception as e:
        logger.error(f"获取用户预设失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取预设失败: {str(e)}")


@router.post("/", response_model=PresetOperationResponse, summary="创建新预设")
async def create_preset(
    request: CreatePresetRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """创建新的筛选预设（admin可以创建系统预设）"""
    try:
        # 权限检查：只有admin可以创建系统预设
        if request.is_system_preset and not current_user.is_admin:
            raise HTTPException(status_code=403, detail="只有管理员可以创建系统预设")
        
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        cursor = conn.cursor()
        
        # 系统预设不能设为默认或收藏
        if request.is_system_preset:
            is_default = False
            is_favorite = False
        else:
            is_default = request.is_default
            is_favorite = request.is_favorite
            
            # 如果设为默认，需要先取消同scope下的其他默认预设
            if is_default:
                update_query = """
                    UPDATE user_news_filter_presets
                    SET is_default = false
                    WHERE user_id = %s 
                      AND scope_type = %s
                      AND (symbol_code = %s OR (symbol_code IS NULL AND %s IS NULL))
                """
                cursor.execute(update_query, (
                    current_user.user_id,
                    request.scope_type,
                    request.symbol_code,
                    request.symbol_code
                ))
        
        # 插入新预设
        if request.is_system_preset:
            # 创建系统预设
            insert_query = """
                INSERT INTO user_news_filter_presets (
                    user_id, preset_name, preset_type, description, icon,
                    filter_config, scope_type, symbol_code, is_default, is_favorite,
                    is_system_global, created_by_admin_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, user_id, preset_name, preset_type, description, icon,
                          filter_config, scope_type, symbol_code, is_default, is_favorite,
                          sort_order, usage_count, last_used_at, created_at, updated_at
            """
            cursor.execute(insert_query, (
                None,  # 系统预设的 user_id 为 NULL
                request.preset_name,
                'system',
                request.description,
                request.icon,
                json.dumps(request.filter_config.dict(exclude_none=True)),
                request.scope_type,
                request.symbol_code,
                is_default,
                is_favorite,
                True,  # is_system_global
                current_user.user_id  # created_by_admin_id
            ))
        else:
            # 创建用户自定义预设
            insert_query = """
                INSERT INTO user_news_filter_presets (
                    user_id, preset_name, preset_type, description, icon,
                    filter_config, scope_type, symbol_code, is_default, is_favorite
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, user_id, preset_name, preset_type, description, icon,
                          filter_config, scope_type, symbol_code, is_default, is_favorite,
                          sort_order, usage_count, last_used_at, created_at, updated_at
            """
            cursor.execute(insert_query, (
                current_user.user_id,
                request.preset_name,
                'custom',
                request.description,
                request.icon,
                json.dumps(request.filter_config.dict(exclude_none=True)),
                request.scope_type,
                request.symbol_code,
                is_default,
                is_favorite
            ))
        
        row = cursor.fetchone()
        conn.commit()
        
        preset = {
            'id': row[0],
            'user_id': row[1],
            'preset_name': row[2],
            'preset_type': row[3],
            'description': row[4],
            'icon': row[5],
            'filter_config': row[6],
            'scope_type': row[7],
            'symbol_code': row[8],
            'is_default': row[9],
            'is_favorite': row[10],
            'sort_order': row[11],
            'usage_count': row[12],
            'last_used_at': row[13].isoformat() if row[13] else None,
            'created_at': row[14].isoformat() if row[14] else None,
            'updated_at': row[15].isoformat() if row[15] else None
        }
        
        cursor.close()
        conn.close()
        
        preset_type_desc = "系统预设" if request.is_system_preset else "自定义预设"
        logger.info(f"用户 {current_user.user_id} 创建{preset_type_desc}: {request.preset_name}")
        
        return {
            "success": True,
            "message": f"预设 '{request.preset_name}' 创建成功",
            "preset": preset
        }
        
    except Exception as e:
        logger.error(f"创建预设失败: {str(e)}")
        if conn:
            conn.rollback()
            conn.close()
        
        # 检查是否为重复名称错误
        if "unique_user_preset_name" in str(e):
            raise HTTPException(status_code=400, detail=f"预设名称 '{request.preset_name}' 已存在")
        
        raise HTTPException(status_code=500, detail=f"创建预设失败: {str(e)}")


@router.put("/{preset_id}", response_model=PresetOperationResponse, summary="更新预设")
async def update_preset(
    preset_id: int,
    request: UpdatePresetRequest,
    current_user: TokenData = Depends(get_current_user)
):
    """更新预设（admin可以编辑系统预设）"""
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        cursor = conn.cursor()
        
        # 查询预设信息
        cursor.execute("""
            SELECT id, preset_type, scope_type, symbol_code, user_id, is_system_global
            FROM user_news_filter_presets
            WHERE id = %s
        """, (preset_id,))
        
        existing = cursor.fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail="预设不存在")
        
        preset_id_db, preset_type, scope_type, symbol_code, owner_id, is_system_global = existing
        
        # 权限检查
        if is_system_global:
            # 系统预设只有admin可以编辑
            if not current_user.is_admin:
                raise HTTPException(status_code=403, detail="只有管理员可以编辑系统预设")
        else:
            # 用户自定义预设，只有创建者可以编辑
            if owner_id != current_user.user_id:
                raise HTTPException(status_code=403, detail="无权限编辑此预设")
        
        # 如果设为默认，需要先取消同scope下的其他默认预设
        if request.is_default and not is_system_global:
            # 只有用户预设才能设为默认
            update_query = """
                UPDATE user_news_filter_presets
                SET is_default = false
                WHERE user_id = %s 
                  AND scope_type = %s
                  AND (symbol_code = %s OR (symbol_code IS NULL AND %s IS NULL))
                  AND id != %s
            """
            cursor.execute(update_query, (
                current_user.user_id,
                scope_type,
                symbol_code,
                symbol_code,
                preset_id
            ))
        
        # 构建更新语句
        update_fields = []
        update_params = []
        
        if request.preset_name is not None:
            update_fields.append("preset_name = %s")
            update_params.append(request.preset_name)
        
        if request.description is not None:
            update_fields.append("description = %s")
            update_params.append(request.description)
        
        if request.icon is not None:
            update_fields.append("icon = %s")
            update_params.append(request.icon)
        
        if request.filter_config is not None:
            update_fields.append("filter_config = %s")
            update_params.append(json.dumps(request.filter_config.dict(exclude_none=True)))
        
        if request.is_default is not None:
            update_fields.append("is_default = %s")
            update_params.append(request.is_default)
        
        if request.is_favorite is not None:
            update_fields.append("is_favorite = %s")
            update_params.append(request.is_favorite)
        
        if request.sort_order is not None:
            update_fields.append("sort_order = %s")
            update_params.append(request.sort_order)
        
        if not update_fields:
            raise HTTPException(status_code=400, detail="没有要更新的字段")
        
        update_params.append(preset_id)
        
        # 构建WHERE条件：系统预设只需匹配ID，用户预设需匹配ID和user_id
        if is_system_global:
            where_condition = "id = %s"
        else:
            where_condition = "id = %s AND user_id = %s"
            update_params.append(current_user.user_id)
        
        update_query = f"""
            UPDATE user_news_filter_presets
            SET {', '.join(update_fields)}
            WHERE {where_condition}
            RETURNING id, user_id, preset_name, preset_type, description, icon,
                      filter_config, scope_type, symbol_code, is_default, is_favorite,
                      sort_order, usage_count, last_used_at, created_at, updated_at
        """
        
        cursor.execute(update_query, update_params)
        row = cursor.fetchone()
        conn.commit()
        
        preset = {
            'id': row[0],
            'user_id': row[1],
            'preset_name': row[2],
            'preset_type': row[3],
            'description': row[4],
            'icon': row[5],
            'filter_config': row[6],
            'scope_type': row[7],
            'symbol_code': row[8],
            'is_default': row[9],
            'is_favorite': row[10],
            'sort_order': row[11],
            'usage_count': row[12],
            'last_used_at': row[13].isoformat() if row[13] else None,
            'created_at': row[14].isoformat() if row[14] else None,
            'updated_at': row[15].isoformat() if row[15] else None
        }
        
        cursor.close()
        conn.close()
        
        logger.info(f"用户 {current_user.user_id} 更新预设 {preset_id}")
        
        return {
            "success": True,
            "message": "预设更新成功",
            "preset": preset
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新预设失败: {str(e)}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=f"更新预设失败: {str(e)}")


@router.delete("/{preset_id}", response_model=PresetOperationResponse, summary="删除预设")
async def delete_preset(
    preset_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """删除预设（admin可以删除系统预设）"""
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        cursor = conn.cursor()
        
        # 查询预设信息
        cursor.execute("""
            SELECT preset_name, preset_type, user_id, is_system_global
            FROM user_news_filter_presets
            WHERE id = %s
        """, (preset_id,))
        
        existing = cursor.fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail="预设不存在")
        
        preset_name, preset_type, owner_id, is_system_global = existing
        
        # 权限检查
        if is_system_global:
            # 系统预设只有admin可以删除
            if not current_user.is_admin:
                raise HTTPException(status_code=403, detail="只有管理员可以删除系统预设")
        else:
            # 用户自定义预设，只有创建者可以删除
            if owner_id != current_user.user_id:
                raise HTTPException(status_code=403, detail="无权限删除此预设")
        
        # 删除预设
        cursor.execute("""
            DELETE FROM user_news_filter_presets
            WHERE id = %s
        """, (preset_id,))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"用户 {current_user.user_id} 删除预设 {preset_id}: {preset_name}")
        
        return {
            "success": True,
            "message": f"预设 '{preset_name}' 已删除"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"删除预设失败: {str(e)}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=f"删除预设失败: {str(e)}")


@router.post("/{preset_id}/use", summary="使用预设（更新统计）")
async def use_preset(
    preset_id: int,
    current_user: TokenData = Depends(get_current_user)
):
    """
    使用预设（更新使用次数和最后使用时间）
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        cursor = conn.cursor()
        
        # 更新使用统计
        cursor.execute("""
            UPDATE user_news_filter_presets
            SET usage_count = usage_count + 1,
                last_used_at = CURRENT_TIMESTAMP
            WHERE id = %s AND user_id = %s
        """, (preset_id, current_user.user_id))
        
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="预设不存在或无权限访问")
        
        conn.commit()
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "message": "使用统计已更新"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"更新使用统计失败: {str(e)}")
        if conn:
            conn.rollback()
            conn.close()
        raise HTTPException(status_code=500, detail=f"更新统计失败: {str(e)}")


@router.post("/{preset_id}/duplicate", response_model=PresetOperationResponse, summary="复制预设")
async def duplicate_preset(
    preset_id: int,
    new_name: str = Query(..., description="新预设名称"),
    current_user: TokenData = Depends(get_current_user)
):
    """
    复制预设（可以复制系统预设或自己的预设并修改）
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        cursor = conn.cursor()
        
        # 获取源预设（可以是系统预设或用户预设）
        cursor.execute("""
            SELECT description, icon, filter_config, scope_type, symbol_code, is_system_global, user_id
            FROM user_news_filter_presets
            WHERE id = %s
        """, (preset_id,))
        
        existing = cursor.fetchone()
        if not existing:
            raise HTTPException(status_code=404, detail="预设不存在")
        
        description = existing[0]
        icon = existing[1]
        filter_config = json.dumps(existing[2])
        scope_type = existing[3]
        symbol_code = existing[4]
        is_system_global = existing[5]
        owner_id = existing[6]
        
        # 权限检查：只能复制系统预设或自己的预设
        if not is_system_global and owner_id != current_user.user_id:
            raise HTTPException(status_code=403, detail="无权限复制此预设")
        
        # 创建副本
        insert_query = """
            INSERT INTO user_news_filter_presets (
                user_id, preset_name, preset_type, description, icon,
                filter_config, scope_type, symbol_code, is_default, is_favorite
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id, user_id, preset_name, preset_type, description, icon,
                      filter_config, scope_type, symbol_code, is_default, is_favorite,
                      sort_order, usage_count, last_used_at, created_at, updated_at
        """
        
        cursor.execute(insert_query, (
            current_user.user_id,
            new_name,
            'custom',
            description,
            icon,
            filter_config,
            scope_type,
            symbol_code,
            False,  # 副本默认不是默认预设
            False   # 副本默认不收藏
        ))
        
        row = cursor.fetchone()
        conn.commit()
        
        preset = {
            'id': row[0],
            'user_id': row[1],
            'preset_name': row[2],
            'preset_type': row[3],
            'description': row[4],
            'icon': row[5],
            'filter_config': row[6],
            'scope_type': row[7],
            'symbol_code': row[8],
            'is_default': row[9],
            'is_favorite': row[10],
            'sort_order': row[11],
            'usage_count': row[12],
            'last_used_at': row[13].isoformat() if row[13] else None,
            'created_at': row[14].isoformat() if row[14] else None,
            'updated_at': row[15].isoformat() if row[15] else None
        }
        
        cursor.close()
        conn.close()
        
        logger.info(f"用户 {current_user.user_id} 复制预设 {preset_id} 为 {new_name}")
        
        return {
            "success": True,
            "message": f"预设 '{new_name}' 创建成功",
            "preset": preset
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"复制预设失败: {str(e)}")
        if conn:
            conn.rollback()
            conn.close()
        
        if "unique_user_preset_name" in str(e):
            raise HTTPException(status_code=400, detail=f"预设名称 '{new_name}' 已存在")
        
        raise HTTPException(status_code=500, detail=f"复制预设失败: {str(e)}")


@router.get("/system-presets", summary="获取系统预设")
async def get_system_presets(
    current_user: TokenData = Depends(get_current_user)
):
    """
    获取系统预设（从数据库读取，所有用户共享）
    """
    try:
        db_config = get_database_config()
        db_manager = DatabaseManager(db_config)
        conn = db_manager.get_connection()
        
        if not conn:
            raise HTTPException(status_code=500, detail="数据库连接失败")
        
        cursor = conn.cursor()
        
        # 从数据库查询系统预设
        query = """
            SELECT 
                id, user_id, preset_name, preset_type, description, icon,
                filter_config, scope_type, symbol_code, is_default, is_favorite,
                sort_order, usage_count, last_used_at, created_at, updated_at
            FROM user_news_filter_presets
            WHERE is_system_global = true
            ORDER BY sort_order ASC, created_at ASC
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        system_presets = []
        for row in rows:
            preset = {
                'id': row[0],
                'user_id': row[1],
                'preset_name': row[2],
                'preset_type': row[3],
                'description': row[4],
                'icon': row[5],
                'filter_config': row[6],
                'scope_type': row[7],
                'symbol_code': row[8],
                'is_default': row[9],
                'is_favorite': row[10],
                'sort_order': row[11],
                'usage_count': row[12],
                'last_used_at': row[13].isoformat() if row[13] else None,
                'created_at': row[14].isoformat() if row[14] else None,
                'updated_at': row[15].isoformat() if row[15] else None
            }
            system_presets.append(preset)
        
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "total": len(system_presets),
            "presets": system_presets
        }
        
    except Exception as e:
        logger.error(f"获取系统预设失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取系统预设失败: {str(e)}")


