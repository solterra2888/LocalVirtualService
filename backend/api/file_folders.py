# -*- coding: utf-8 -*-
"""
文件夹管理 API — 一级文件夹，per-user
"""

from typing import List, Optional, Any
from pydantic import BaseModel, Field, field_validator
from fastapi import APIRouter, HTTPException, status, Depends, Query, Body

from backend.auth.jwt_utils import get_current_user
from backend.database.db_manager import DatabaseManager
from backend.utils.logger import info, error
from backend.utils.database_config import get_database_config

router = APIRouter()


# ── Pydantic models ──────────────────────────────────────────────

class FolderCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    color: Optional[str] = Field('', max_length=20)

class FolderUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    color: Optional[str] = Field(None, max_length=20)
    sort_order: Optional[int] = None

class FolderResponse(BaseModel):
    id: int
    name: str
    color: str
    sort_order: int
    file_count: int = 0
    created_at: Optional[str] = None

def _coerce_optional_int(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    if isinstance(v, bool):
        raise ValueError("invalid integer")
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        return int(s)
    raise ValueError("invalid integer")


def _coerce_int_list(v: Any) -> List[int]:
    if not isinstance(v, (list, tuple)):
        raise ValueError("execution_ids must be a list")
    out: List[int] = []
    for x in v:
        if x is None:
            continue
        if isinstance(x, bool):
            raise ValueError("invalid execution id")
        if isinstance(x, int):
            out.append(x)
        elif isinstance(x, float):
            out.append(int(x))
        elif isinstance(x, str):
            s = x.strip()
            if not s:
                continue
            out.append(int(s))
        else:
            out.append(int(x))
    return out


class MoveFileRequest(BaseModel):
    folder_id: Optional[int] = Field(None, description="Target folder id, null = move to root")

    @field_validator("folder_id", mode="before")
    @classmethod
    def _folder_id_before(cls, v: Any) -> Any:
        return _coerce_optional_int(v)


class BatchMoveRequest(BaseModel):
    execution_ids: List[int] = Field(..., min_length=1)
    folder_id: Optional[int] = Field(None, description="Target folder id, null = move to root")

    @field_validator("execution_ids", mode="before")
    @classmethod
    def _execution_ids_before(cls, v: Any) -> Any:
        return _coerce_int_list(v)

    @field_validator("folder_id", mode="before")
    @classmethod
    def _batch_folder_id_before(cls, v: Any) -> Any:
        return _coerce_optional_int(v)


# ── helpers ──────────────────────────────────────────────────────

def _get_conn():
    db_conn_params = get_database_config()
    db_manager = DatabaseManager(db_conn_params)
    conn = db_manager.get_connection()
    if not conn:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database connection failed")
    return conn


# ── CRUD endpoints ───────────────────────────────────────────────

@router.get("/folders", response_model=List[FolderResponse], summary="List folders for current user")
async def list_folders(current_user=Depends(get_current_user)):
    conn = _get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT f.id, f.name, f.color, f.sort_order, f.created_at,
                   COUNT(fpd.id) AS file_count
            FROM file_folders f
            LEFT JOIN file_processing_details fpd ON fpd.folder_id = f.id
            WHERE f.username = %s
            GROUP BY f.id
            ORDER BY f.sort_order, f.created_at
        """, (current_user.username,))
        rows = cursor.fetchall()
        return [
            FolderResponse(
                id=r[0], name=r[1], color=r[2] or '', sort_order=r[3] or 0,
                created_at=r[4].isoformat() if r[4] else None, file_count=r[5]
            )
            for r in rows
        ]
    finally:
        conn.close()


@router.post("/folders", response_model=FolderResponse, status_code=201, summary="Create folder")
async def create_folder(body: FolderCreate, current_user=Depends(get_current_user)):
    conn = _get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT MAX(sort_order) FROM file_folders WHERE username = %s",
            (current_user.username,),
        )
        max_order = cursor.fetchone()[0] or 0

        cursor.execute("""
            INSERT INTO file_folders (name, username, color, sort_order)
            VALUES (%s, %s, %s, %s)
            RETURNING id, name, color, sort_order, created_at
        """, (body.name.strip(), current_user.username, body.color or '', max_order + 1))
        r = cursor.fetchone()
        conn.commit()
        info(f"Folder created: {r[1]}", source="folders", user=current_user.username)
        return FolderResponse(
            id=r[0], name=r[1], color=r[2] or '', sort_order=r[3] or 0,
            created_at=r[4].isoformat() if r[4] else None, file_count=0,
        )
    except Exception as e:
        conn.rollback()
        if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Folder name already exists")
        error(f"Create folder failed: {e}", source="folders")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    finally:
        conn.close()


# 必须先于 PATCH /folders/{folder_id} 注册，否则 "batch-move" 会被当成 folder_id 导致 int 校验失败
@router.patch("/folders/batch-move", summary="Batch move executions to a folder")
async def batch_move_executions(body: BatchMoveRequest, current_user=Depends(get_current_user)):
    conn = _get_conn()
    cursor = conn.cursor()
    try:
        if body.folder_id is not None:
            cursor.execute("SELECT id FROM file_folders WHERE id = %s AND username = %s", (body.folder_id, current_user.username))
            if not cursor.fetchone():
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Target folder not found")

        ids = body.execution_ids
        placeholders = ",".join(["%s"] * len(ids))
        cursor.execute(
            f"""
            UPDATE file_processing_details SET folder_id = %s
            WHERE processing_stats_id IN ({placeholders})
              AND processing_stats_id IN (SELECT id FROM processing_stats WHERE "user" = %s)
            """,
            (body.folder_id, *ids, current_user.username),
        )
        conn.commit()
        return {"message": f"Moved {cursor.rowcount} files"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        error(f"Batch move failed: {e}", source="folders")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    finally:
        conn.close()


@router.patch("/folders/move/{execution_id}", summary="Move an execution (all its files) to a folder")
async def move_execution_to_folder(execution_id: int, body: MoveFileRequest, current_user=Depends(get_current_user)):
    """Move all file_processing_details under a processing_stats to target folder."""
    conn = _get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute('SELECT id FROM processing_stats WHERE id = %s AND "user" = %s', (execution_id, current_user.username))
        if not cursor.fetchone():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Execution not found or no permission")

        if body.folder_id is not None:
            cursor.execute("SELECT id FROM file_folders WHERE id = %s AND username = %s", (body.folder_id, current_user.username))
            if not cursor.fetchone():
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Target folder not found")

        cursor.execute(
            "UPDATE file_processing_details SET folder_id = %s WHERE processing_stats_id = %s",
            (body.folder_id, execution_id),
        )
        conn.commit()
        return {"message": "Moved successfully"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        error(f"Move execution failed: {e}", source="folders")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    finally:
        conn.close()


@router.patch("/folders/{folder_id}", response_model=FolderResponse, summary="Update folder")
async def update_folder(folder_id: int, body: FolderUpdate, current_user=Depends(get_current_user)):
    conn = _get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM file_folders WHERE id = %s AND username = %s", (folder_id, current_user.username))
        if not cursor.fetchone():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Folder not found")

        sets, vals = [], []
        if body.name is not None:
            sets.append("name = %s"); vals.append(body.name.strip())
        if body.color is not None:
            sets.append("color = %s"); vals.append(body.color)
        if body.sort_order is not None:
            sets.append("sort_order = %s"); vals.append(body.sort_order)
        if not sets:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No fields to update")

        vals.append(folder_id)
        cursor.execute(f"UPDATE file_folders SET {', '.join(sets)} WHERE id = %s RETURNING id, name, color, sort_order, created_at", vals)
        r = cursor.fetchone()

        cursor.execute("SELECT COUNT(*) FROM file_processing_details WHERE folder_id = %s", (folder_id,))
        cnt = cursor.fetchone()[0]

        conn.commit()
        return FolderResponse(
            id=r[0], name=r[1], color=r[2] or '', sort_order=r[3] or 0,
            created_at=r[4].isoformat() if r[4] else None, file_count=cnt,
        )
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Folder name already exists")
        error(f"Update folder failed: {e}", source="folders")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    finally:
        conn.close()


@router.delete("/folders/{folder_id}", summary="Delete folder (files move to root)")
async def delete_folder(folder_id: int, current_user=Depends(get_current_user)):
    conn = _get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT id FROM file_folders WHERE id = %s AND username = %s", (folder_id, current_user.username))
        if not cursor.fetchone():
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Folder not found")

        cursor.execute("UPDATE file_processing_details SET folder_id = NULL WHERE folder_id = %s", (folder_id,))
        cursor.execute("DELETE FROM file_folders WHERE id = %s", (folder_id,))
        conn.commit()
        info(f"Folder deleted: {folder_id}", source="folders", user=current_user.username)
        return {"message": "Folder deleted, files moved to root"}
    except HTTPException:
        raise
    except Exception as e:
        conn.rollback()
        error(f"Delete folder failed: {e}", source="folders")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
    finally:
        conn.close()
