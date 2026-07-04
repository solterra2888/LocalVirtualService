# -*- coding: utf-8 -*-
"""
微博凭证服务

管理微博爬虫账号的 Cookie 凭证（Fernet 加密存储），提供：
  - Cookie 的录入、加密存储、解密读取
  - 主动校验（探活：m.weibo.cn/api/config 的 data.login）
  - 多凭证轮换（get_first_valid_cookie 取 valid/unknown）
  - 抓取时 Cookie 失效 → mark_expired（由 WeiboSubscriptionService 捕获
    crawl4weibo AuthenticationError 后回调）
  - weibo_accounts 博主元数据的 upsert / 列表 / 启停

架构对齐：zsxq_credential_service.py
差异：
  - 加密 key 用 WEIBO_COOKIE_ENC_KEY
  - 探活走 m.weibo.cn 轻量接口（不启动 crawl4weibo/playwright）
  - 无 refresh_groups（微博博主由用户订阅时自动 upsert，非批量拉取）
  - 新增 upsert_account / list_accounts / set_account_enabled
"""

import logging
import re
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras
import requests

from backend.utils.database_config import get_database_config
from backend.utils.env_loader import load_project_env

load_project_env()

logger = logging.getLogger(__name__)

# m.weibo.cn 访客态配置接口：未登录返回 data.login=false，登录态返回 true
WEIBO_CONFIG_URL = "https://m.weibo.cn/api/config"
_WEIBO_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1"
)


# ---------- 加密工具 ----------

def _get_fernet():
    """懒加载 Fernet 实例；密钥来自 WEIBO_COOKIE_ENC_KEY 环境变量。"""
    import os
    try:
        from cryptography.fernet import Fernet
    except ImportError as exc:
        raise RuntimeError("cryptography 包未安装，请运行 pip install cryptography") from exc

    raw_key = os.getenv("WEIBO_COOKIE_ENC_KEY", "").strip()
    if not raw_key:
        raise RuntimeError("未设置 WEIBO_COOKIE_ENC_KEY 环境变量，无法加解密微博凭证")
    return Fernet(raw_key.encode())


def encrypt_cookie(cookie: str) -> str:
    return _get_fernet().encrypt(cookie.encode()).decode()


def decrypt_cookie(encrypted: str) -> str:
    return _get_fernet().decrypt(encrypted.encode()).decode()


# ---------- Cookie 清洗 ----------

def clean_cookie(cookie: str) -> str:
    """容错解析浏览器粘贴的 Cookie 字符串，返回规范化结果（标准 k=v; k=v）。"""
    cookie = (cookie or "").strip()
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
    """掩码 Cookie，绝不向前端暴露明文。微博登录态主要在 SUB/SUBP。"""
    sub_match = re.search(r"SUB=([^;]+)", cookie)
    if sub_match:
        sub = sub_match.group(1)
        visible = sub[:6] + "***" + sub[-4:] if len(sub) > 10 else "***"
        return f"SUB={visible};..."
    return "***"


# ---------- 凭证 CRUD + 博主账户 ----------

class WeiboCredentialService:
    """微博凭证与博主账户管理服务。"""

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
                    INSERT INTO weibo_credentials (label, encrypted_cookie, status)
                    VALUES (%s, %s, 'unknown')
                    RETURNING id
                    """,
                    (label, encrypted),
                )
                cred_id = cur.fetchone()[0]
            conn.commit()

        logger.info("微博凭证已录入 id=%s label=%s", cred_id, label)
        return {"id": cred_id, "label": label, "status": "unknown"}

    def update_credential(
        self, cred_id: int, label: Optional[str] = None, raw_cookie: Optional[str] = None
    ) -> bool:
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

        if len(updates) == 1:
            return False

        params.append(cred_id)
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE weibo_credentials SET {', '.join(updates)} WHERE id = %s",
                    params,
                )
                updated = cur.rowcount
            conn.commit()
        return updated > 0

    def delete_credential(self, cred_id: int) -> bool:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM weibo_credentials WHERE id = %s", (cred_id,))
                deleted = cur.rowcount
            conn.commit()
        return deleted > 0

    def list_credentials(self) -> List[Dict[str, Any]]:
        """列出所有凭证（Cookie 掩码，不返回明文）。"""
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, label, status, last_validated_at, last_error,
                           created_at, updated_at
                    FROM weibo_credentials
                    ORDER BY id
                    """
                )
                return [dict(r) for r in cur.fetchall()]

    def get_decrypted_cookie(self, cred_id: int) -> Optional[str]:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT encrypted_cookie FROM weibo_credentials WHERE id = %s",
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
                    FROM weibo_credentials
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
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE weibo_credentials
                    SET status = 'expired', last_error = %s, updated_at = now()
                    WHERE id = %s
                    """,
                    (error[:500], cred_id),
                )
            conn.commit()
        logger.warning("微博凭证 id=%s 已标记为 expired: %s", cred_id, error)

    def mark_valid(self, cred_id: int) -> None:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE weibo_credentials
                    SET status = 'valid', last_validated_at = now(), last_error = NULL, updated_at = now()
                    WHERE id = %s
                    """,
                    (cred_id,),
                )
            conn.commit()

    # ---- 校验（探活）----

    def validate_credential(self, cred_id: int) -> Dict[str, Any]:
        """
        主动校验凭证是否有效。走 m.weibo.cn/api/config 轻量接口：
          - data.login == true  → mark_valid
          - 302/301 或 login=false → mark_expired
        不启动 crawl4weibo / playwright。
        """
        cookie = self.get_decrypted_cookie(cred_id)
        if not cookie:
            return {"success": False, "error": "凭证不存在"}

        try:
            resp = requests.get(
                WEIBO_CONFIG_URL,
                headers={
                    "Cookie": cookie,
                    "User-Agent": _WEIBO_UA,
                    "Referer": "https://m.weibo.cn/",
                    "Accept": "application/json, text/plain, */*",
                },
                timeout=15,
                allow_redirects=False,
            )
        except requests.RequestException as e:
            with _get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE weibo_credentials SET last_error = %s, updated_at = now() WHERE id = %s",
                        (str(e)[:500], cred_id),
                    )
                conn.commit()
            return {"success": False, "error": str(e)}

        # 重定向到登录页 → 无登录态
        if resp.status_code in (301, 302, 303, 307, 308):
            self.mark_expired(cred_id, f"HTTP {resp.status_code} 重定向到登录页")
            return {"success": False, "error": f"Cookie 无登录态 (HTTP {resp.status_code})"}

        try:
            data = resp.json() or {}
        except ValueError:
            self.mark_expired(cred_id, "m.weibo.cn/api/config 返回非 JSON")
            return {"success": False, "error": "探活返回非 JSON"}

        login = bool(data.get("data", {}).get("login", False))
        if login:
            self.mark_valid(cred_id)
            return {"success": True, "login": True}

        self.mark_expired(cred_id, "m.weibo.cn/api/config data.login=false")
        return {"success": False, "error": "Cookie 无登录态 (login=false)"}

    # ---- 博主账户（weibo_accounts）----

    def upsert_account(
        self,
        uid: str,
        screen_name: str,
        *,
        avatar: str = "",
        credential_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        订阅时调用：建立/更新博主元数据。
        仅更新昵称/头像/凭证；不覆盖增量锚点（recent_post_ids/min_max_create_time/backfill_complete）。
        """
        uid = str(uid)
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO weibo_accounts (uid, screen_name, avatar, credential_id, enabled)
                    VALUES (%s, %s, %s, %s, true)
                    ON CONFLICT (uid) DO UPDATE SET
                        screen_name   = EXCLUDED.screen_name,
                        avatar        = EXCLUDED.avatar,
                        credential_id = COALESCE(EXCLUDED.credential_id, weibo_accounts.credential_id),
                        updated_at    = now()
                    RETURNING id
                    """,
                    (uid, screen_name or "", avatar or "", credential_id),
                )
                row = cur.fetchone()
            conn.commit()
        aid = row[0] if row else None
        return {"id": aid, "uid": uid, "screen_name": screen_name}

    def get_account_meta(self, uid: str) -> Dict[str, Any]:
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT * FROM weibo_accounts WHERE uid = %s",
                    (str(uid),),
                )
                row = cur.fetchone()
        return dict(row) if row else {}

    def list_accounts(self, enabled_only: bool = True) -> List[Dict[str, Any]]:
        sql = """
            SELECT a.id, a.uid, a.screen_name, a.avatar, a.enabled,
                   a.backfill_complete, a.last_sync_at, a.last_sync_mode,
                   c.label AS credential_label, c.status AS credential_status
            FROM weibo_accounts a
            LEFT JOIN weibo_credentials c ON a.credential_id = c.id
        """
        if enabled_only:
            sql += " WHERE a.enabled = true"
        sql += " ORDER BY a.screen_name"

        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                return [dict(r) for r in cur.fetchall()]

    def set_account_enabled(self, uid: str, enabled: bool) -> bool:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE weibo_accounts SET enabled = %s, updated_at = now() WHERE uid = %s",
                    (enabled, str(uid)),
                )
                updated = cur.rowcount
            conn.commit()
        return updated > 0
