# -*- coding: utf-8 -*-
"""
任务管理API路由
"""

from fastapi import APIRouter, HTTPException, status, Depends, Query, Body
from typing import List, Optional
from pydantic import BaseModel
import os

from backend.schemas.task import (
    TaskTypeCreateRequest,
    TaskTypeUpdateRequest,
    TaskTypeResponse,
    TaskExecutionResponse
)
from backend.auth.jwt_utils import get_current_user, get_current_admin_user
from backend.processors.task_manager import TaskManager
from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config

router = APIRouter()




@router.get("/types", response_model=List[TaskTypeResponse], summary="获取RC Agent列表")
async def get_task_types(current_user=Depends(get_current_user)):
    """
    获取RC Agent列表
    """
    db_conn_params = get_database_config()
    task_manager = TaskManager(current_user.username, current_user.is_admin, db_conn_params)
    
    # 加载AGENT凭据
    task_manager.load_agent_credentials()
    
    task_list = task_manager.get_task_list_for_ui()
    
    return [
        TaskTypeResponse(
            id=task['id'],
            name=task['name'],
            token=task['token'],
            creator=task.get('creator', ''),
            created_at=task.get('created_at'),
            remarks=task.get('remarks', ''),
            input_fields=task.get('input_fields', []),
            is_default=task.get('is_default', False),
            agent_type=task.get('agent_type', 'chatbot')
        )
        for task in task_list
    ]


@router.post("/types", response_model=TaskTypeResponse, summary="创建RC Agent")
async def create_task_type(
    request: TaskTypeCreateRequest,
    current_user=Depends(get_current_user)
):
    """
    创建新的RC Agent
    
    - **name**: 任务名称
    - **token**: RC Agent令牌
    - **remarks**: 备注
    - **input_fields**: 自定义输入字段
    """
    db_conn_params = get_database_config()
    task_manager = TaskManager(current_user.username, current_user.is_admin, db_conn_params)
    
    # 加载AGENT凭据
    task_manager.load_agent_credentials()
    
    try:
        success, message, task_id = task_manager.add_task_type(
            task_name=request.name,
            task_token=request.token,
            input_fields=request.input_fields,
            remarks=request.remarks,
            agent_type=request.agent_type
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=message
            )
        
        # 获取新创建的任务信息
        task_info = task_manager.get_task_info(task_id)
        
        return TaskTypeResponse(
            id=task_id,
            name=task_info['name'],
            token=task_info['token'],
            creator=current_user.username,
            created_at=task_info.get('created_at'),
            remarks=task_info.get('remarks', ''),
            input_fields=task_info.get('input_fields', []),
            is_default=task_info.get('is_default', False),
            agent_type=task_info.get('agent_type', 'chatbot')
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"创建RC Agent失败: {str(e)}"
        )


@router.put("/types/{task_id}", response_model=TaskTypeResponse, summary="更新RC Agent")
async def update_task_type(
    task_id: str,
    request: TaskTypeUpdateRequest,
    current_user=Depends(get_current_user)
):
    """
    更新RC Agent
    
    - **task_id**: 任务ID
    - **name**: 任务名称
    - **token**: RC Agent令牌
    - **remarks**: 备注
    - **input_fields**: 自定义输入字段
    """
    db_conn_params = get_database_config()
    task_manager = TaskManager(current_user.username, current_user.is_admin, db_conn_params)
    
    # 加载AGENT凭据
    task_manager.load_agent_credentials()
    
    # 检查任务是否存在
    task_info = task_manager.get_task_info(task_id)
    if not task_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"RC Agent不存在: {task_id}"
        )
    
    # 权限检查：普通用户只能编辑自己创建的AI Agent
    if not current_user.is_admin and task_info.get('created_by') != current_user.username:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="权限不足，只能编辑自己创建的AI Agent"
        )
    
    try:
        # 更新RC Agent
        success, message = task_manager.update_task_type(
            task_id=task_id,
            task_name=request.name,
            task_token=request.token,
            input_fields=request.input_fields,
            remarks=request.remarks,
            agent_type=request.agent_type
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=message
            )
        
        # 获取更新后的任务信息
        updated_task_info = task_manager.get_task_info(task_id)
        
        return TaskTypeResponse(
            id=task_id,
            name=updated_task_info['name'],
            token=updated_task_info['token'],
            creator=updated_task_info.get('creator', ''),
            created_at=updated_task_info.get('created_at'),
            remarks=updated_task_info.get('remarks', ''),
            input_fields=updated_task_info.get('input_fields', []),
            is_default=updated_task_info.get('is_default', False),
            agent_type=updated_task_info.get('agent_type', 'chatbot')
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新RC Agent失败: {str(e)}"
        )


@router.delete("/types/{task_id}", summary="删除RC Agent")
async def delete_task_type(
    task_id: str,
    current_user=Depends(get_current_user)
):
    """
    删除RC Agent
    
    - **task_id**: 任务ID
    """
    db_conn_params = get_database_config()
    task_manager = TaskManager(current_user.username, current_user.is_admin, db_conn_params)
    
    # 加载AGENT凭据
    task_manager.load_agent_credentials()
    
    # 检查任务是否存在
    task_info = task_manager.get_task_info(task_id)
    if not task_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"RC Agent不存在: {task_id}"
        )
    
    # 权限检查：普通用户只能删除自己创建的AI Agent
    if not current_user.is_admin and task_info.get('created_by') != current_user.username:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="权限不足，只能删除自己创建的AI Agent"
        )
    
    try:
        success, message = task_manager.delete_task_type(task_id)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=message
            )
        
        return {"message": message}
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除RC Agent失败: {str(e)}"
        )


@router.get("/executions", response_model=List[TaskExecutionResponse], summary="获取任务执行历史")
async def get_task_executions(
    current_user=Depends(get_current_user),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页大小"),
    search_type: Optional[str] = Query(None, description="搜索类型: file_name 或 content"),
    q: Optional[str] = Query(None, description="搜索关键词")
):
    """
    获取任务执行历史
    
    - **page**: 页码
    - **page_size**: 每页大小
    - **search_type**: 搜索类型（file_name 按文件名，content 按文件内容）
    - **q**: 搜索关键词，与 search_type 配合使用
    """
    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    
    # 获取任务执行历史
    offset = (page - 1) * page_size
    
    # 获取处理统计数据
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="无法连接到数据库"
        )
    
    cursor = conn.cursor()
    try:
        import json
        
        params = []
        where_conditions = []
        
        where_conditions.append("""(
            ps."user" = %s
            OR EXISTS (
                SELECT 1 FROM file_processing_details fpd2
                JOIN file_shares fs ON fpd2.id = fs.file_id
                WHERE fpd2.processing_stats_id = ps.id
                  AND fs.shared_to_user_id = %s
            )
        )""")
        params.extend([current_user.username, current_user.user_id])
        
        if q and q.strip():
            q_pattern = f"%{q.strip()}%"
            if search_type == "file_name":
                where_conditions.append(
                    "(SELECT filename FROM file_processing_details WHERE processing_stats_id = ps.id ORDER BY id ASC LIMIT 1) ILIKE %s"
                )
                params.append(q_pattern)
            elif search_type == "content":
                where_conditions.append(
                    "EXISTS (SELECT 1 FROM file_processing_details fpd WHERE fpd.processing_stats_id = ps.id AND fpd.content IS NOT NULL AND fpd.content::text ILIKE %s)"
                )
                params.append(q_pattern)
        
        where_clause = ("WHERE " + " AND ".join(where_conditions)) if where_conditions else ""
        
        access_type_expr = """CASE WHEN ps."user" = %s THEN 'owner' ELSE 'shared' END"""
        shared_by_expr = """(
            SELECT u.username FROM file_shares fs
            JOIN file_processing_details fpd3 ON fpd3.id = fs.file_id
            JOIN users u ON u.id = fs.shared_by_user_id
            WHERE fpd3.processing_stats_id = ps.id AND fs.shared_to_user_id = %s
            ORDER BY fs.created_at DESC LIMIT 1
        )"""
        select_params = [current_user.username, current_user.user_id]
        
        cursor.execute(f"""
        SELECT ps.id, ps.timestamp, ps.files_processed, ps.files_failed, 
               ps.total_time, ps.total_file_processing_time, ps.total_ai_time,
               ps.task_name, ps."user", ps.total_tokens, ps.total_price, ps.rc_agent_status,
               (SELECT filename FROM file_processing_details 
                WHERE processing_stats_id = ps.id 
                ORDER BY id ASC LIMIT 1) as first_filename,
               (SELECT file_type FROM file_processing_details 
                WHERE processing_stats_id = ps.id 
                ORDER BY id ASC LIMIT 1) as first_file_type,
               ps.llm_model, ps.llm_provider,
               (SELECT task_id FROM file_processing_details 
                WHERE processing_stats_id = ps.id 
                ORDER BY id ASC LIMIT 1) as first_task_id,
               ps.upload_params,
               {access_type_expr} as access_type,
               {shared_by_expr} as shared_by
        FROM processing_stats ps
        {where_clause}
        ORDER BY ps.id DESC 
        LIMIT %s OFFSET %s
        """, select_params + params + [page_size, offset])
        
        stats = cursor.fetchall()
        
        ps_ids = [stat[0] for stat in stats]
        share_info_map = {}
        if ps_ids:
            cursor.execute("""
                SELECT fpd.processing_stats_id,
                       u_by.id, u_by.username, u_by.avatar_url,
                       u_to.id, u_to.username, u_to.avatar_url
                FROM file_shares fs
                JOIN file_processing_details fpd ON fpd.id = fs.file_id
                JOIN users u_by ON u_by.id = fs.shared_by_user_id
                JOIN users u_to ON u_to.id = fs.shared_to_user_id
                WHERE fpd.processing_stats_id = ANY(%s)
                ORDER BY fs.created_at ASC
            """, (ps_ids,))
            for row in cursor.fetchall():
                ps_id = row[0]
                if ps_id not in share_info_map:
                    share_info_map[ps_id] = {
                        'shared_by': {'user_id': row[1], 'username': row[2], 'avatar_url': row[3]},
                        'shared_to': []
                    }
                to_user = {'user_id': row[4], 'username': row[5], 'avatar_url': row[6]}
                existing_ids = [u['user_id'] for u in share_info_map[ps_id]['shared_to']]
                if to_user['user_id'] not in existing_ids:
                    share_info_map[ps_id]['shared_to'].append(to_user)

        return [
            TaskExecutionResponse(
                id=str(stat[0]),
                username=stat[8],
                task_name=stat[7],
                created_at=stat[1],
                files_processed=stat[2],
                files_failed=stat[3],
                total_time=stat[4],
                file_processing_time=stat[5],
                ai_processing_time=stat[6],
                total_tokens=stat[9],
                total_cost=stat[10],
                rc_agent_status=stat[11] if len(stat) > 11 else None,
                first_filename=stat[12] if len(stat) > 12 else None,
                first_file_type=stat[13] if len(stat) > 13 else None,
                llm_model=stat[14] if len(stat) > 14 else None,
                llm_provider=stat[15] if len(stat) > 15 else None,
                first_task_id=stat[16] if len(stat) > 16 else None,
                upload_params=json.loads(stat[17]) if len(stat) > 17 and stat[17] else None,
                access_type=stat[18] if len(stat) > 18 else None,
                shared_by=stat[19] if len(stat) > 19 else None,
                share_info=share_info_map.get(stat[0]),
            )
            for stat in stats
        ]
        
    finally:
        conn.close()


@router.get("/executions/{execution_id}", response_model=TaskExecutionResponse, summary="获取任务执行详情")
async def get_task_execution(
    execution_id: str,
    current_user=Depends(get_current_user)
):
    """
    获取特定任务执行的详情
    
    - **execution_id**: 任务执行ID
    """
    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="无法连接到数据库"
        )
    
    cursor = conn.cursor()
    try:
        cursor.execute("""
        SELECT ps.id, ps.timestamp, ps.files_processed, ps.files_failed, 
               ps.total_time, ps.total_file_processing_time, ps.total_ai_time,
               ps.task_name, ps."user", ps.total_tokens, ps.total_price, ps.rc_agent_status,
               (SELECT filename FROM file_processing_details 
                WHERE processing_stats_id = ps.id 
                ORDER BY id ASC LIMIT 1) as first_filename,
               (SELECT file_type FROM file_processing_details 
                WHERE processing_stats_id = ps.id 
                ORDER BY id ASC LIMIT 1) as first_file_type,
               ps.llm_model, ps.llm_provider,
               (SELECT task_id FROM file_processing_details 
                WHERE processing_stats_id = ps.id 
                ORDER BY id ASC LIMIT 1) as first_task_id,
               ps.upload_params
        FROM processing_stats ps
        WHERE ps.id = %s
        """, (execution_id,))
        
        stat = cursor.fetchone()
        if not stat:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"任务执行不存在: {execution_id}"
            )
        
        import json
        
        is_owner = stat[8] == current_user.username
        has_shared_access = False
        if not is_owner:
            cursor.execute("""
                SELECT 1 FROM file_processing_details fpd
                JOIN file_shares fs ON fpd.id = fs.file_id
                WHERE fpd.processing_stats_id = %s AND fs.shared_to_user_id = %s
                LIMIT 1
            """, (execution_id, current_user.user_id))
            has_shared_access = cursor.fetchone() is not None
        
        if not is_owner and not has_shared_access:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="权限不足，只能查看自己的任务执行"
            )
        
        access_type = 'owner' if is_owner else 'shared'
        
        return TaskExecutionResponse(
            id=str(stat[0]),
            username=stat[8],
            task_name=stat[7],
            created_at=stat[1],
            files_processed=stat[2],
            files_failed=stat[3],
            total_time=stat[4],
            file_processing_time=stat[5],
            ai_processing_time=stat[6],
            total_tokens=stat[9],
            total_cost=stat[10],
            rc_agent_status=stat[11] if len(stat) > 11 else None,
            first_filename=stat[12] if len(stat) > 12 else None,
            first_file_type=stat[13] if len(stat) > 13 else None,
            llm_model=stat[14] if len(stat) > 14 else None,
            llm_provider=stat[15] if len(stat) > 15 else None,
            first_task_id=stat[16] if len(stat) > 16 else None,
            upload_params=json.loads(stat[17]) if len(stat) > 17 and stat[17] else None,
            access_type=access_type
        )
        
    finally:
        conn.close()


@router.delete("/executions/{execution_id}", summary="删除任务执行记录")
async def delete_task_execution(
    execution_id: str,
    current_user=Depends(get_current_user)
):
    """
    删除任务执行记录
    - 普通用户只能删除自己的记录
    - 管理员可以删除所有记录
    
    - **execution_id**: 任务执行ID
    """
    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="无法连接到数据库"
        )
    
    cursor = conn.cursor()
    try:
        # 检查记录是否存在并获取用户信息
        cursor.execute('SELECT id, "user" FROM processing_stats WHERE id = %s', (execution_id,))
        record = cursor.fetchone()
        if not record:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"任务执行不存在: {execution_id}"
            )
        
        record_id, record_user = record
        
        if record_user != current_user.username:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="权限不足，只能删除自己的任务执行记录"
            )
        
        # 删除记录（级联删除会自动删除相关的 file_processing_details）
        cursor.execute("DELETE FROM processing_stats WHERE id = %s", (execution_id,))
        conn.commit()
        
        return {"message": "任务执行记录删除成功"}
        
    except Exception as e:
        conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"删除任务执行记录失败: {str(e)}"
        )
    finally:
        conn.close()


class RenameExecutionRequest(BaseModel):
    filename: str


class UpdateFinalReportRequest(BaseModel):
    final_report: str


@router.patch("/executions/{execution_id}/final-report", summary="更新任务执行的 AI 分析报告内容")
async def update_execution_final_report(
    execution_id: str,
    body: UpdateFinalReportRequest,
    current_user=Depends(get_current_user)
):
    """
    更新 processing_stats 中的 final_report 字段（用于编辑 AI Analysis 报告内容）
    """
    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="无法连接到数据库"
        )
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT id, "user" FROM processing_stats WHERE id = %s',
            (execution_id,)
        )
        stat = cursor.fetchone()
        if not stat:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="任务执行不存在"
            )
        if stat[1] != current_user.username:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="权限不足"
            )
        content = body.final_report if body.final_report is not None else ''
        cursor.execute(
            "UPDATE processing_stats SET final_report = %s WHERE id = %s",
            (content, execution_id)
        )
        conn.commit()
        return {"success": True, "message": "Final report updated"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新失败: {str(e)}"
        )
    finally:
        conn.close()


@router.patch("/executions/{execution_id}/rename", summary="重命名执行记录的首个文件")
async def rename_execution_file(
    execution_id: str,
    body: RenameExecutionRequest,
    current_user=Depends(get_current_user)
):
    """
    重命名执行记录中首个文件的filename，新文件名不能与数据库中已有的filename重复
    """
    filename = (body.filename or "").strip()
    if not filename:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="文件名不能为空"
        )
    
    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="无法连接到数据库"
        )
    
    cursor = conn.cursor()
    try:
        cursor.execute(
            'SELECT ps.id, ps."user" FROM processing_stats ps WHERE ps.id = %s',
            (execution_id,)
        )
        stat = cursor.fetchone()
        if not stat:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="任务执行不存在"
            )
        if stat[1] != current_user.username:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="权限不足"
            )
        
        cursor.execute(
            "SELECT id, filename FROM file_processing_details WHERE processing_stats_id = %s ORDER BY id ASC LIMIT 1",
            (execution_id,)
        )
        first_file = cursor.fetchone()
        if not first_file:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="该执行记录下无文件"
            )
        file_id, current_filename = first_file[0], first_file[1]
        current_ext = os.path.splitext(current_filename or "")[1].lower()
        new_ext = os.path.splitext(filename)[1].lower()
        if current_ext and current_ext != new_ext:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"文件后缀必须保持不变 ({current_ext})"
            )
        
        cursor.execute(
            "SELECT id FROM file_processing_details WHERE filename = %s AND id != %s",
            (filename, file_id)
        )
        if cursor.fetchone():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="file name already exists, please use another name"
            )
        
        cursor.execute(
            "UPDATE file_processing_details SET filename = %s WHERE id = %s",
            (filename, file_id)
        )
        conn.commit()
        return {"message": "rename successfully", "filename": filename}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"rename failed: {str(e)}"
        )
    finally:
        conn.close()


@router.get("/executions/{execution_id}/details", summary="获取任务执行的详细信息")
async def get_task_execution_details(
    execution_id: str,
    current_user=Depends(get_current_user)
):
    """
    获取任务执行的详细信息，包括上传参数、日志、报告等
    
    - **execution_id**: 任务执行ID
    """
    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="无法连接到数据库"
        )
    
    cursor = conn.cursor()
    try:
        # 获取基本信息
        cursor.execute("""
        SELECT id, timestamp, files_processed, files_failed, 
               total_time, total_file_processing_time, total_ai_time,
               task_name, "user", total_tokens, total_price, upload_params,
               final_report, rc_agent_status
        FROM processing_stats 
        WHERE id = %s
        """, (execution_id,))
        
        stat = cursor.fetchone()
        if not stat:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"任务执行不存在: {execution_id}"
            )
        
        is_owner = stat[8] == current_user.username
        if not is_owner:
            cursor.execute("""
                SELECT 1 FROM file_processing_details fpd
                JOIN file_shares fs ON fpd.id = fs.file_id
                WHERE fpd.processing_stats_id = %s AND fs.shared_to_user_id = %s
                LIMIT 1
            """, (execution_id, current_user.user_id))
            if not cursor.fetchone():
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="权限不足，只能查看自己的任务执行"
                )
        
        # 获取文件处理详情
        cursor.execute("""
        SELECT id, filename, file_type, file_size_mb, processing_time,
               ai_time, content_length, status, total_tokens, cost,
               error_message, content, created_at, original_file_path
        FROM file_processing_details 
        WHERE processing_stats_id = %s
        ORDER BY id
        """, (execution_id,))
        
        files_details = cursor.fetchall()
        
        return {
            "execution": {
                "id": str(stat[0]),
                "username": stat[8],
                "task_name": stat[7],
                "created_at": stat[1],
                "files_processed": stat[2],
                "files_failed": stat[3],
                "total_time": stat[4],
                "file_processing_time": stat[5],
                "ai_processing_time": stat[6],
                "total_tokens": stat[9],
                "total_cost": stat[10],
                "upload_params": stat[11],
                "final_report": stat[12]
            },
            "files": [
                {
                    "id": file_detail[0],
                    "filename": file_detail[1],
                    "file_type": file_detail[2],
                    "file_size_mb": file_detail[3],
                    "processing_time": file_detail[4],
                    "ai_time": file_detail[5],
                    "content_length": file_detail[6],
                    "status": file_detail[7],
                    "total_tokens": file_detail[8],
                    "cost": file_detail[9],
                    "error_message": file_detail[10],
                    "content": file_detail[11],
                    "created_at": file_detail[12],
                    "original_file_path": file_detail[13] if len(file_detail) > 13 else None
                }
                for file_detail in files_details
            ]
        }
        
    finally:
        conn.close() 