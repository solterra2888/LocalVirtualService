# -*- coding: utf-8 -*-
"""
知识星球凭证服务

管理管理员账号的 Cookie 凭证（Fernet 加密存储），提供：
  - Cookie 的录入、加密存储、解密读取
  - 主动校验（探活）
  - 从「我加入的星球」接口同步 zsxq_groups
  - 401 时自动将凭证标记为 expired
"""

import os
import re
import time
import uuid
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote

import psycopg2
import psycopg2.extras
import requests

from backend.utils.database_config import get_database_config
from backend.utils.env_loader import load_project_env

load_project_env()

logger = logging.getLogger(__name__)

# ---------- 加密工具 ----------

def _get_fernet():
    """懒加载 Fernet 实例；密钥来自 ZSXQ_COOKIE_ENC_KEY 环境变量。"""
    try:
        from cryptography.fernet import Fernet
    except ImportError as exc:
        raise RuntimeError(
            "cryptography 包未安装，请运行 pip install cryptography"
        ) from exc

    raw_key = os.getenv("ZSXQ_COOKIE_ENC_KEY", "").strip()
    if not raw_key:
        raise RuntimeError(
            "未设置 ZSXQ_COOKIE_ENC_KEY 环境变量，无法加解密知识星球凭证"
        )
    return Fernet(raw_key.encode())


def encrypt_cookie(cookie: str) -> str:
    return _get_fernet().encrypt(cookie.encode()).decode()


def decrypt_cookie(encrypted: str) -> str:
    return _get_fernet().decrypt(encrypted.encode()).decode()


# ---------- Cookie 清洗（复用 scripts/zsxq/my_zsxq.py 逻辑） ----------

def clean_cookie(cookie: str) -> str:
    """容错解析浏览器粘贴的 Cookie 字符串，返回规范化结果。"""
    cookie = cookie.strip()
    if "\n" in cookie:
        cookie = cookie.split("\n")[0]
    cookie = cookie.rstrip("\\")
    if cookie.startswith(("b'", 'b"')):
        cookie = cookie[2:-1]
    elif (cookie.startswith("'") and cookie.endswith("'")) or (
        cookie.startswith('"') and cookie.endswith('"')
    ):
        cookie = cookie[1:-1]
    cookie = cookie.replace("\\n", "")

    if cookie.startswith("zsxq_access_token=_c_"):
        cookie = cookie[len("zsxq_access_token="):]

    parts: Dict[str, str] = {}
    order: List[str] = []
    for segment in cookie.split(";"):
        segment = segment.strip()
        if not segment or "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        key, value = key.strip(), value.strip()
        if key not in parts:
            order.append(key)
        parts[key] = value

    if "zsxq_access_token" not in parts and parts:
        only = next(iter(parts.values())) if len(parts) == 1 and len(order) == 1 else None
        if only and "-" in only and "_" in only:
            return f"zsxq_access_token={only}"

    return "; ".join(f"{k}={parts[k]}" for k in order)


# ---------- DB helpers ----------

def _get_conn():
    cfg = get_database_config()
    return psycopg2.connect(
        host=cfg["db_host"],
        port=cfg["db_port"],
        dbname=cfg["db_name"],
        user=cfg["db_user"],
        password=cfg["db_password"],
    )


def _mask_cookie(cookie: str) -> str:
    """掩码 Cookie，绝不向前端暴露明文 token。"""
    token_match = re.search(r"zsxq_access_token=([^;]+)", cookie)
    if token_match:
        token = token_match.group(1)
        visible = token[:6] + "***" + token[-4:] if len(token) > 10 else "***"
        return f"zsxq_access_token={visible};..."
    return "***"


def normalize_avatar_url(value: Optional[str]) -> str:
    url = (value or "").strip()
    if not url:
        return ""
    # 知识星球偶发返回 builtin://N 占位符，非可加载图片
    if not url.startswith(("http://", "https://")):
        return ""
    return url


def extract_group_avatar(group: Dict[str, Any]) -> str:
    """从 /v2/groups 单条记录提取头像：优先星球封面，回退主理人头像。"""
    avatar = normalize_avatar_url(group.get("background_url"))
    if avatar:
        return avatar
    owner = group.get("owner") or {}
    if isinstance(owner, dict):
        return normalize_avatar_url(owner.get("avatar_url"))
    return ""


# ---------- 凭证 CRUD ----------

class ZsxqCredentialService:
    """知识星球凭证管理服务。"""

    # 知识星球 API
    BASE_URL = "https://api.zsxq.com"
    API_VERSION = "2.89.0"

    def _headers(self, cookie: str) -> Dict[str, str]:
        return {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Cache-Control": "no-cache",
            "Cookie": cookie,
            "Origin": "https://wx.zsxq.com",
            "Pragma": "no-cache",
            "Referer": "https://wx.zsxq.com/",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            "X-Request-Id": str(uuid.uuid4()),
            "X-Timestamp": str(int(time.time())),
            "X-Version": self.API_VERSION,
        }

    # ---- 录入 ----

    def create_credential(self, label: str, raw_cookie: str) -> Dict[str, Any]:
        """录入新凭证（加密存储），返回凭证 id。"""
        cleaned = clean_cookie(raw_cookie)
        if not cleaned:
            raise ValueError("Cookie 内容为空，请重新从浏览器复制")
        encrypted = encrypt_cookie(cleaned)

        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO zsxq_credentials (label, encrypted_cookie, status)
                    VALUES (%s, %s, 'unknown')
                    RETURNING id
                    """,
                    (label, encrypted),
                )
                cred_id = cur.fetchone()[0]
            conn.commit()

        logger.info("知识星球凭证已录入 id=%s label=%s", cred_id, label)
        return {"id": cred_id, "label": label, "status": "unknown"}

    def update_credential(self, cred_id: int, label: Optional[str] = None, raw_cookie: Optional[str] = None) -> bool:
        """更新凭证标签或 Cookie。"""
        updates: List[str] = ["updated_at = now()"]
        params: List[Any] = []

        if label is not None:
            updates.append("label = %s")
            params.append(label)
        if raw_cookie is not None:
            cleaned = clean_cookie(raw_cookie)
            encrypted = encrypt_cookie(cleaned)
            updates.append("encrypted_cookie = %s")
            updates.append("status = 'unknown'")
            updates.append("last_error = NULL")
            params.append(encrypted)

        if not updates:
            return False

        params.append(cred_id)
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE zsxq_credentials SET {', '.join(updates)} WHERE id = %s",
                    params,
                )
                updated = cur.rowcount
            conn.commit()
        return updated > 0

    def delete_credential(self, cred_id: int) -> bool:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM zsxq_credentials WHERE id = %s", (cred_id,))
                deleted = cur.rowcount
            conn.commit()
        return deleted > 0

    def list_credentials(self) -> List[Dict[str, Any]]:
        """列出所有凭证（Cookie 掩码，不返回明文）。"""
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, label, status, last_validated_at, last_error, created_at, updated_at
                    FROM zsxq_credentials
                    ORDER BY id
                    """
                )
                return [dict(r) for r in cur.fetchall()]

    def get_decrypted_cookie(self, cred_id: int) -> Optional[str]:
        """内部使用：解密并返回 Cookie 明文。"""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT encrypted_cookie FROM zsxq_credentials WHERE id = %s",
                    (cred_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return decrypt_cookie(row[0])

    def get_first_valid_cookie(self) -> Optional[Tuple[int, str]]:
        """返回第一个 valid/unknown 凭证的 (id, cookie_plaintext)；无则返回 None。"""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, encrypted_cookie
                    FROM zsxq_credentials
                    WHERE status IN ('valid', 'unknown')
                    ORDER BY id
                    LIMIT 1
                    """
                )
                row = cur.fetchone()
        if not row:
            return None
        return row[0], decrypt_cookie(row[1])

    def mark_expired(self, cred_id: int, error: str) -> None:
        """将凭证标记为 expired（由 401 触发）。"""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE zsxq_credentials
                    SET status = 'expired', last_error = %s, updated_at = now()
                    WHERE id = %s
                    """,
                    (error[:500], cred_id),
                )
            conn.commit()
        logger.warning("知识星球凭证 id=%s 已标记为 expired: %s", cred_id, error)

    def mark_valid(self, cred_id: int) -> None:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE zsxq_credentials
                    SET status = 'valid', last_validated_at = now(), last_error = NULL, updated_at = now()
                    WHERE id = %s
                    """,
                    (cred_id,),
                )
            conn.commit()

    # ---- 校验 ----

    def validate_credential(self, cred_id: int) -> Dict[str, Any]:
        """主动校验凭证是否有效（调用星球列表接口），更新状态并返回结果。"""
        cookie = self.get_decrypted_cookie(cred_id)
        if not cookie:
            return {"success": False, "error": "凭证不存在"}

        try:
            resp = requests.get(
                f"{self.BASE_URL}/v2/groups",
                headers=self._headers(cookie),
                timeout=15,
            )
        except requests.RequestException as e:
            with _get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE zsxq_credentials SET last_error = %s, updated_at = now() WHERE id = %s",
                        (str(e)[:500], cred_id),
                    )
                conn.commit()
            return {"success": False, "error": str(e)}

        if resp.status_code == 401:
            self.mark_expired(cred_id, "HTTP 401 Unauthorized")
            return {"success": False, "error": "Cookie 已过期 (HTTP 401)"}

        data = resp.json()
        if data.get("succeeded"):
            self.mark_valid(cred_id)
            groups = data.get("resp_data", {}).get("groups") or []
            return {"success": True, "groups_count": len(groups)}

        error = data.get("info") or data.get("resp_err") or str(data)
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE zsxq_credentials SET last_error = %s, last_validated_at = now(), updated_at = now() WHERE id = %s",
                    (str(error)[:500], cred_id),
                )
            conn.commit()
        return {"success": False, "error": error}

    # ---- 拉取并同步星球列表 ----

    def refresh_groups(self, cred_id: int) -> Dict[str, Any]:
        """
        拉取该凭证账号「我加入的星球」，写入/更新 zsxq_groups 表。
        返回同步结果摘要。
        """
        cookie = self.get_decrypted_cookie(cred_id)
        if not cookie:
            return {"success": False, "error": "凭证不存在"}

        try:
            resp = requests.get(
                f"{self.BASE_URL}/v2/groups",
                headers=self._headers(cookie),
                timeout=15,
            )
        except requests.RequestException as e:
            return {"success": False, "error": str(e)}

        if resp.status_code == 401:
            self.mark_expired(cred_id, "HTTP 401 Unauthorized during refresh_groups")
            return {"success": False, "error": "Cookie 已过期"}

        data = resp.json()
        if not data.get("succeeded"):
            return {"success": False, "error": data.get("info") or str(data)}

        self.mark_valid(cred_id)
        groups = data.get("resp_data", {}).get("groups") or []
        synced = 0

        with _get_conn() as conn:
            with conn.cursor() as cur:
                for g in groups:
                    group_id = str(g.get("group_id") or "")
                    group_name = g.get("name") or g.get("group_name") or ""
                    avatar = extract_group_avatar(g)
                    if not group_id:
                        continue
                    cur.execute(
                        """
                        INSERT INTO zsxq_groups (group_id, group_name, avatar, credential_id, enabled)
                        VALUES (%s, %s, %s, %s, true)
                        ON CONFLICT (group_id) DO UPDATE SET
                            group_name    = EXCLUDED.group_name,
                            avatar        = COALESCE(NULLIF(EXCLUDED.avatar, ''), zsxq_groups.avatar),
                            credential_id = EXCLUDED.credential_id,
                            updated_at    = now()
                        """,
                        (group_id, group_name, avatar, cred_id),
                    )
                    synced += 1
            conn.commit()

        logger.info("知识星球凭证 id=%s 同步星球 %d 个", cred_id, synced)
        return {"success": True, "synced": synced, "groups": [
            {
                "group_id": str(g.get("group_id", "")),
                "group_name": g.get("name") or g.get("group_name", ""),
                "avatar": extract_group_avatar(g) or None,
            }
            for g in groups
        ]}

    # ---- 手动维护星球 ----

    def upsert_group_manual(self, group_id: str, group_name: str, enabled: bool = True) -> Dict[str, Any]:
        """管理员手动添加/更新星球（不依赖 API，兜底方案）。"""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO zsxq_groups (group_id, group_name, enabled)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (group_id) DO UPDATE SET
                        group_name = EXCLUDED.group_name,
                        enabled    = EXCLUDED.enabled,
                        updated_at = now()
                    RETURNING id, avatar
                    """,
                    (group_id, group_name, enabled),
                )
                row = cur.fetchone()
                gid, avatar = row[0], (row[1] or "") if row else ""
            conn.commit()
        # Best-effort: 若头像仍空，用任一有效凭证 refresh 一次补齐
        if not (avatar or "").strip():
            avatar = self.ensure_group_avatar(group_id)
        return {
            "id": gid,
            "group_id": group_id,
            "group_name": group_name,
            "avatar": avatar or None,
        }

    def ensure_group_avatar(self, group_id: str) -> str:
        """返回星球头像；DB 无缓存时尝试用有效凭证 refresh_groups 补拉。"""
        gid = str(group_id or "").strip()
        if not gid:
            return ""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT NULLIF(TRIM(avatar), '')
                    FROM zsxq_groups
                    WHERE group_id = %s
                    """,
                    (gid,),
                )
                row = cur.fetchone()
        cached = (row[0] or "") if row else ""
        if cached:
            return cached

        # 用所有 valid 凭证各 sync 一次（与 Substack ensure 对齐，best-effort）
        for cred in self.list_credentials():
            if cred.get("status") != "valid":
                continue
            result = self.refresh_groups(int(cred["id"]))
            if not result.get("success"):
                continue
            with _get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT NULLIF(TRIM(avatar), '')
                        FROM zsxq_groups
                        WHERE group_id = %s
                        """,
                        (gid,),
                    )
                    row = cur.fetchone()
            avatar = (row[0] or "") if row else ""
            if avatar:
                return avatar
        return ""

    def list_groups(self, enabled_only: bool = True) -> List[Dict[str, Any]]:
        """返回可供用户订阅的星球列表。"""
        sql = """
            SELECT g.id, g.group_id, g.group_name, g.avatar, g.enabled,
                   g.backfill_complete, g.last_sync_at, g.last_sync_mode,
                   c.label AS credential_label, c.status AS credential_status
            FROM zsxq_groups g
            LEFT JOIN zsxq_credentials c ON g.credential_id = c.id
        """
        params: List[Any] = []
        if enabled_only:
            sql += " WHERE g.enabled = true"
        sql += " ORDER BY g.group_name"

        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql, params)
                return [dict(r) for r in cur.fetchall()]
