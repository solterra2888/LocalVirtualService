# -*- coding: utf-8 -*-
"""Personal Access Tokens (PAT) for Agent / MCP — CRUD + verify."""

from __future__ import annotations

import json
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from passlib.context import CryptContext

from backend.database.db_manager import DatabaseManager
from backend.utils.database_config import get_database_config
from backend.utils.logger import get_logger

logger = get_logger("personal_token_service")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

PAT_PREFIX = "ght_"
# Lookup prefix length (must match verify_pat query)
PREFIX_LEN = 16

ALLOWED_SCOPES = frozenset({"daily:read", "reports:read"})
DEFAULT_SCOPES = ["daily:read", "reports:read"]


class PersonalTokenService:
    def __init__(self, db: Optional[DatabaseManager] = None) -> None:
        self._db = db or DatabaseManager(get_database_config())

    def create_token(
        self,
        user_id: int,
        name: str,
        scopes: Optional[List[str]] = None,
        expires_days: Optional[int] = None,
    ) -> Tuple[str, Dict[str, Any]]:
        """Create PAT; returns (plaintext_token_once, row_summary_for_list)."""
        cleaned = self._normalize_scopes(scopes)
        raw = PAT_PREFIX + secrets.token_urlsafe(32)
        prefix = raw[:PREFIX_LEN]
        token_hash = pwd_context.hash(raw)

        expires_at = None
        if expires_days is not None and expires_days > 0:
            expires_at = datetime.now(timezone.utc) + timedelta(days=expires_days)

        conn = self._db.get_connection()
        if not conn:
            raise RuntimeError("数据库连接失败")
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO api_personal_tokens
                    (user_id, name, prefix, token_hash, scopes, expires_at)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s)
                RETURNING id, created_at
                """,
                (user_id, name, prefix, token_hash, json.dumps(cleaned), expires_at),
            )
            row = cursor.fetchone()
            conn.commit()
            tid = int(row[0])
            created_at = row[1]
            summary = {
                "id": tid,
                "name": name,
                "prefix": prefix + "…",
                "scopes": cleaned,
                "expires_at": expires_at.isoformat() if expires_at else None,
                "created_at": created_at.isoformat() if hasattr(created_at, "isoformat") else str(created_at),
                "last_used_at": None,
                "revoked_at": None,
            }
            return raw, summary
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def list_tokens(self, user_id: int) -> List[Dict[str, Any]]:
        conn = self._db.get_connection()
        if not conn:
            raise RuntimeError("数据库连接失败")
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT id, name, prefix, scopes, expires_at, last_used_at, revoked_at, created_at
                FROM api_personal_tokens
                WHERE user_id = %s
                ORDER BY created_at DESC
                """,
                (user_id,),
            )
            out: List[Dict[str, Any]] = []
            for r in cursor.fetchall():
                scopes_raw = r[3]
                if isinstance(scopes_raw, str):
                    try:
                        scopes_raw = json.loads(scopes_raw)
                    except json.JSONDecodeError:
                        scopes_raw = []
                out.append(
                    {
                        "id": int(r[0]),
                        "name": r[1],
                        "prefix": (r[2] or "") + "…",
                        "scopes": list(scopes_raw) if scopes_raw else [],
                        "expires_at": r[4].isoformat() if r[4] else None,
                        "last_used_at": r[5].isoformat() if r[5] else None,
                        "revoked_at": r[6].isoformat() if r[6] else None,
                        "created_at": r[7].isoformat() if r[7] else None,
                    }
                )
            return out
        finally:
            conn.close()

    def revoke(self, user_id: int, token_id: int) -> bool:
        conn = self._db.get_connection()
        if not conn:
            raise RuntimeError("数据库连接失败")
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE api_personal_tokens
                SET revoked_at = NOW()
                WHERE id = %s AND user_id = %s AND revoked_at IS NULL
                RETURNING id
                """,
                (token_id, user_id),
            )
            ok = cursor.fetchone() is not None
            conn.commit()
            return ok
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def verify(self, raw_token: str) -> Optional[Dict[str, Any]]:
        """Validate PAT string; returns dict with user_id, pat_id, scopes, username or None."""
        if not raw_token or not raw_token.startswith(PAT_PREFIX):
            return None
        if len(raw_token) < PREFIX_LEN:
            return None
        lookup_prefix = raw_token[:PREFIX_LEN]

        conn = self._db.get_connection()
        if not conn:
            logger.warning("PAT verify: DB unavailable (fail-closed)")
            return None
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT t.id, t.user_id, t.token_hash, t.scopes, t.expires_at, t.revoked_at, u.username
                FROM api_personal_tokens t
                JOIN users u ON u.id = t.user_id
                WHERE t.prefix = %s
                """,
                (lookup_prefix,),
            )
            rows = cursor.fetchall()
            now = datetime.now(timezone.utc)
            for r in rows:
                tid, uid, thash, scopes_raw, exp, revoked, username = r
                if revoked is not None:
                    continue
                if exp is not None:
                    exp_aware = exp if exp.tzinfo else exp.replace(tzinfo=timezone.utc)
                    if now > exp_aware:
                        continue
                try:
                    if not pwd_context.verify(raw_token, thash):
                        continue
                except Exception as e:
                    logger.warning(f"PAT bcrypt verify error id={tid}: {e}")
                    continue

                if isinstance(scopes_raw, str):
                    try:
                        scopes_list = json.loads(scopes_raw)
                    except json.JSONDecodeError:
                        scopes_list = list(DEFAULT_SCOPES)
                elif isinstance(scopes_raw, list):
                    scopes_list = scopes_raw
                else:
                    scopes_list = list(DEFAULT_SCOPES)

                cursor.execute(
                    "UPDATE api_personal_tokens SET last_used_at = NOW() WHERE id = %s",
                    (tid,),
                )
                conn.commit()

                return {
                    "pat_id": int(tid),
                    "user_id": int(uid),
                    "username": username,
                    "scopes": [s for s in scopes_list if s in ALLOWED_SCOPES],
                }
            return None
        except Exception as e:
            logger.error(f"PAT verify failed: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
            return None
        finally:
            conn.close()

    @staticmethod
    def _normalize_scopes(scopes: Optional[List[str]]) -> List[str]:
        if not scopes:
            return list(DEFAULT_SCOPES)
        out = [s for s in scopes if s in ALLOWED_SCOPES]
        return out or list(DEFAULT_SCOPES)
