# -*- coding: utf-8 -*-
"""
任务管理API路由
"""

from fastapi import APIRouter, HTTPException, status, Depends, Query, Body
from typing import List, Optional, Dict, Any
import json
from pydantic import BaseModel
import os

from backend.schemas.task import (
    TaskExecutionResponse
)
from backend.auth.jwt_utils import get_current_user, get_current_admin_user
from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.avatar import resolve_avatar_url

router = APIRouter()


# Sources「Document」筛选：file_processing_details.file_type 存的是扩展名大类（upload_and_process_document_task 等），不是字面量 document
SOURCES_DOCUMENT_FILE_TYPES = (
    'pdf',
    'word',
    'doc',
    'excel',
    'csv',
    'image',
    'other',
    'document',
)



def _fetch_ticker_tags_for_processing_stats_ids(cursor, ps_ids: List) -> Dict[Any, List[str]]:
    """按 processing_stats_id 聚合 file_tags.ticker（jsonb 数组），去重并保持顺序。"""
    if not ps_ids:
        return {}
    cursor.execute(
        """
        SELECT fpd.processing_stats_id, ft.ticker
        FROM file_processing_details fpd
        INNER JOIN file_tags ft ON ft.file_id = fpd.id
        WHERE fpd.processing_stats_id = ANY(%s)
        ORDER BY fpd.id ASC
        """,
        (ps_ids,),
    )
    out: Dict[Any, List[str]] = {}
    for row in cursor.fetchall() or []:
        ps_id = row[0]
        tval = row[1]
        items: List[str] = []
        if tval is None:
            continue
        if isinstance(tval, list):
            items = [str(x).strip() for x in tval if x is not None and str(x).strip()]
        elif isinstance(tval, str):
            s = tval.strip()
            if not s:
                continue
            if s.startswith("["):
                try:
                    parsed = json.loads(s)
                    if isinstance(parsed, list):
                        items = [str(x).strip() for x in parsed if str(x).strip()]
                except Exception:
                    items = [s]
            else:
                items = [s]
        if not items:
            continue
        if ps_id not in out:
            out[ps_id] = []
        seen = set(out[ps_id])
        for t in items:
            if t not in seen:
                out[ps_id].append(t)
                seen.add(t)
    return out


@router.get("/executions", response_model=List[TaskExecutionResponse], summary="获取任务执行历史")
async def get_task_executions(
    current_user=Depends(get_current_user),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(20, ge=1, le=100, description="每页大小"),
    search_type: Optional[str] = Query(None, description="搜索类型: file_name 或 content"),
    q: Optional[str] = Query(None, description="搜索关键词"),
    ticker: Optional[str] = Query(None, description="按ticker筛选（精确匹配file_tags中的ticker数组元素）"),
    folder_id: Optional[str] = Query(None, description="按文件夹筛选: 数字=指定文件夹, 'uncategorized'=未分类, 不传=全部"),
    file_type: Optional[str] = Query(
        None,
        description="按文件类型筛选。audio/youtube/podcast/web/text 等为精确匹配；document 表示全部文档类（pdf/word/excel/csv/image 等）",
    ),
):
    """
    获取任务执行历史
    
    - **page**: 页码
    - **page_size**: 每页大小
    - **search_type**: 搜索类型（file_name 按文件名，content 按文件内容）
    - **q**: 搜索关键词，与 search_type 配合使用
    - **ticker**: 按股票代码筛选，匹配file_tags中包含该ticker的执行记录
    - **folder_id**: 按文件夹筛选，数字=指定文件夹ID，'uncategorized'=未分类文件
    - **file_type**: `document` 匹配文档扩展名大类（pdf/word/excel 等）；其余值精确匹配 file_processing_details.file_type
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
        
        if ticker and ticker.strip():
            where_conditions.append(
                "EXISTS (SELECT 1 FROM file_processing_details fpd_t JOIN file_tags ft ON fpd_t.id = ft.file_id WHERE fpd_t.processing_stats_id = ps.id AND ft.ticker @> %s::jsonb)"
            )
            params.append(json.dumps([ticker.strip()]))
        
        if folder_id is not None and folder_id != '':
            if folder_id == 'uncategorized':
                where_conditions.append(
                    "NOT EXISTS (SELECT 1 FROM user_execution_folders uef_f WHERE uef_f.processing_stats_id = ps.id AND uef_f.username = %s)"
                )
                params.append(current_user.username)
            else:
                try:
                    fid = int(folder_id)
                    where_conditions.append(
                        "EXISTS (SELECT 1 FROM user_execution_folders uef_f WHERE uef_f.processing_stats_id = ps.id AND uef_f.username = %s AND uef_f.folder_id = %s)"
                    )
                    params.extend([current_user.username, fid])
                except ValueError:
                    pass

        if file_type and file_type.strip():
            ft_key = file_type.strip().lower()
            if ft_key == 'document':
                placeholders = ','.join(['%s'] * len(SOURCES_DOCUMENT_FILE_TYPES))
                where_conditions.append(
                    f"EXISTS (SELECT 1 FROM file_processing_details fpd_ft WHERE fpd_ft.processing_stats_id = ps.id AND fpd_ft.file_type IN ({placeholders}))"
                )
                params.extend(SOURCES_DOCUMENT_FILE_TYPES)
            else:
                where_conditions.append(
                    "EXISTS (SELECT 1 FROM file_processing_details fpd_ft WHERE fpd_ft.processing_stats_id = ps.id AND fpd_ft.file_type = %s)"
                )
                params.append(ft_key)

        where_clause = ("WHERE " + " AND ".join(where_conditions)) if where_conditions else ""
        
        access_type_expr = """CASE WHEN ps."user" = %s THEN 'owner' ELSE 'shared' END"""
        shared_by_expr = """(
            SELECT u.username FROM file_shares fs
            JOIN file_processing_details fpd3 ON fpd3.id = fs.file_id
            JOIN users u ON u.id = fs.shared_by_user_id
            WHERE fpd3.processing_stats_id = ps.id AND fs.shared_to_user_id = %s
            ORDER BY fs.created_at DESC LIMIT 1
        )"""
        uef_folder_id_expr = "(SELECT uef.folder_id FROM user_execution_folders uef WHERE uef.processing_stats_id = ps.id AND uef.username = %s LIMIT 1)"
        uef_folder_name_expr = "(SELECT ff.name FROM user_execution_folders uef JOIN file_folders ff ON ff.id = uef.folder_id WHERE uef.processing_stats_id = ps.id AND uef.username = %s LIMIT 1)"
        select_params = [current_user.username, current_user.user_id, current_user.username, current_user.username]
        
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
               {shared_by_expr} as shared_by,
               {uef_folder_id_expr} as folder_id,
               {uef_folder_name_expr} as folder_name,
               (SELECT display_name FROM file_processing_details
                WHERE processing_stats_id = ps.id
                ORDER BY id ASC LIMIT 1) as first_display_name
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
                        'shared_by': {'user_id': row[1], 'username': row[2], 'avatar_url': resolve_avatar_url(row[3])},
                        'shared_to': []
                    }
                to_user = {'user_id': row[4], 'username': row[5], 'avatar_url': resolve_avatar_url(row[6])}
                existing_ids = [u['user_id'] for u in share_info_map[ps_id]['shared_to']]
                if to_user['user_id'] not in existing_ids:
                    share_info_map[ps_id]['shared_to'].append(to_user)

        ticker_tags_map = _fetch_ticker_tags_for_processing_stats_ids(cursor, ps_ids)

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
                ticker_tags=ticker_tags_map.get(stat[0]),
                folder_id=stat[20] if len(stat) > 20 else None,
                folder_name=stat[21] if len(stat) > 21 else None,
                first_display_name=stat[22] if len(stat) > 22 else None,
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
               ps.upload_params,
               (SELECT display_name FROM file_processing_details
                WHERE processing_stats_id = ps.id
                ORDER BY id ASC LIMIT 1) as first_display_name,
               (SELECT uef.folder_id FROM user_execution_folders uef
                WHERE uef.processing_stats_id = ps.id AND uef.username = %s LIMIT 1) as folder_id,
               (SELECT ff.name FROM user_execution_folders uef
                JOIN file_folders ff ON ff.id = uef.folder_id
                WHERE uef.processing_stats_id = ps.id AND uef.username = %s LIMIT 1) as folder_name
        FROM processing_stats ps
        WHERE ps.id = %s
        """, (current_user.username, current_user.username, execution_id))
        
        stat = cursor.fetchone()
        if not stat:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"任务执行不存在: {execution_id}"
            )
        
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

        ticker_tags_map = _fetch_ticker_tags_for_processing_stats_ids(cursor, [stat[0]])
        
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
            access_type=access_type,
            ticker_tags=ticker_tags_map.get(stat[0]),
            first_display_name=stat[18] if len(stat) > 18 else None,
            folder_id=stat[19] if len(stat) > 19 else None,
            folder_name=stat[20] if len(stat) > 20 else None,
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
        
        # PG 级联删除会自动清掉 file_processing_details + document_chunks 行;
        # 但 OSS Vector Bucket 的向量数据需要应用层补充清理. 先把 chunk_ids 采集出来,
        # CASCADE 之后再异步把这批 key 从 Vector Bucket 删除.
        cursor.execute(
            """
            SELECT dc.id FROM document_chunks dc
            JOIN file_processing_details fpd ON fpd.id = dc.file_id
            WHERE fpd.processing_stats_id = %s
            """,
            (execution_id,),
        )
        vector_chunk_ids_to_drop = [int(r[0]) for r in cursor.fetchall()]

        # 删除记录（级联删除会自动删除相关的 file_processing_details + document_chunks）
        cursor.execute("DELETE FROM processing_stats WHERE id = %s", (execution_id,))
        conn.commit()

        # 异步清理 Vector Bucket. 不阻塞 API 响应; 失败也只是会留下孤儿向量,
        # reconcile_vector_bucket_task 每日扫描会兜底.
        if vector_chunk_ids_to_drop:
            try:
                from backend.tasks.vectorize_tasks import delete_vectors_by_keys_task
                delete_vectors_by_keys_task.apply_async(
                    args=(vector_chunk_ids_to_drop,), countdown=2
                )
            except Exception as ve:
                # 不阻断主流程
                pass

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
            "UPDATE file_processing_details SET filename = %s, display_name = NULL WHERE id = %s",
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
               error_message, content, created_at, original_file_path, display_name
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
                    "filename": (file_detail[14] if len(file_detail) > 14 and file_detail[14] else None) or file_detail[1],
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
                    "original_file_path": file_detail[13] if len(file_detail) > 13 else None,
                    "display_name": file_detail[14] if len(file_detail) > 14 else None
                }
                for file_detail in files_details
            ]
        }

    finally:
        conn.close()


@router.get("/executions/{execution_id}/report-pdf", summary="下载 AI 分析报告的文字版 PDF")
async def download_execution_report_pdf(
    execution_id: str,
    current_user=Depends(get_current_user)
):
    """
    把任务执行的 AI 分析报告（final_report，Markdown）在服务端渲染为**文字版** PDF 下载：
    文字可选中/可搜索，中英文自动回退字体，无需前端截图。

    - 权限与 details 端点一致：本人或被共享用户可下载
    - 文件名与前端图片版规则一致：`{Document Name} {YYYY-MM-DD} (Analyzed by {model}) (Text).pdf`
    """
    import urllib.parse
    from fastapi.responses import Response

    from backend.services.ai_report_pdf_service import generate_ai_report_pdf

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
        SELECT ps."user", ps.timestamp, ps.llm_model, ps.final_report,
               COALESCE(
                   (SELECT display_name FROM file_processing_details
                    WHERE processing_stats_id = ps.id
                    ORDER BY id ASC LIMIT 1),
                   (SELECT filename FROM file_processing_details
                    WHERE processing_stats_id = ps.id
                    ORDER BY id ASC LIMIT 1)
               ) as display_name
        FROM processing_stats ps
        WHERE ps.id = %s
        """, (execution_id,))

        stat = cursor.fetchone()
        if not stat:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"任务执行不存在: {execution_id}"
            )

        is_owner = stat[0] == current_user.username
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
                    detail="权限不足，只能下载自己的任务执行报告"
                )

        final_report = stat[3]
        if not final_report or not str(final_report).strip():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="该任务执行暂无 AI 分析报告，无法导出 PDF"
            )

        try:
            pdf_bytes, filename = generate_ai_report_pdf(
                str(final_report),
                document_name=stat[4],
                analyzed_at=stat[1],
                model_name=stat[2],
                author='GuanHe Daily',
            )
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="该任务执行暂无 AI 分析报告，无法导出 PDF"
            )
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"生成文字版 PDF 失败: {exc}"
            )

        # 同时给出 ASCII 回退名与 RFC 5987 编码名，兼容中文文件名
        ascii_fallback = filename.encode('ascii', errors='ignore').decode('ascii').strip() or 'ai-analysis-report.pdf'
        quoted_filename = urllib.parse.quote(filename)
        content_disposition = (
            f"attachment; filename=\"{ascii_fallback}\"; filename*=UTF-8''{quoted_filename}"
        )

        return Response(
            content=pdf_bytes,
            media_type="application/pdf",
            headers={
                "Content-Disposition": content_disposition,
                "Content-Length": str(len(pdf_bytes)),
            },
        )

    finally:
        conn.close() 