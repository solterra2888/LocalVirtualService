# -*- coding: utf-8 -*-
"""
Substack 凭证服务

管理管理员账号的 Cookie 凭证（Fernet 加密存储），提供：
  - Cookie（sid + lli 双值）的录入、加密存储、解密读取
  - 主动校验（探活 subscriptions API）
  - 同步订阅源并集（源A subscriptions + 源B following 作者主刊）→ substack_publications
  - 关注作者登记 → substack_followed_authors
  - 401 时自动将凭证标记为 expired

与 zsxq_credential_service 同构；差异：
  - encrypted_cookie 存加密 JSON {"substack.sid":..,"substack.lli":..}
  - _materialize_auth(cred_id)：解密 → 写临时 cookies.json → SubstackAuth（substack-api 只接受文件路径）
"""

import json
import logging
import os
import re
import tempfile
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import psycopg2
import psycopg2.extras
import requests

from backend.utils.database_config import get_database_config
from backend.utils.env_loader import load_project_env

load_project_env()

logger = logging.getLogger(__name__)

# ---------- 加密工具 ----------

def _get_fernet():
    """懒加载 Fernet 实例；密钥来自 SUBSTACK_COOKIE_ENC_KEY 环境变量。"""
    try:
        from cryptography.fernet import Fernet
    except ImportError as exc:
        raise RuntimeError(
            "cryptography 包未安装，请运行 pip install cryptography"
        ) from exc

    raw_key = os.getenv("SUBSTACK_COOKIE_ENC_KEY", "").strip()
    if not raw_key:
        raise RuntimeError(
            "未设置 SUBSTACK_COOKIE_ENC_KEY 环境变量，无法加解密 Substack 凭证"
        )
    return Fernet(raw_key.encode())


def encrypt_cookie(cookie_json: str) -> str:
    return _get_fernet().encrypt(cookie_json.encode()).decode()


def decrypt_cookie(encrypted: str) -> str:
    try:
        return _get_fernet().decrypt(encrypted.encode()).decode()
    except Exception as exc:
        from cryptography.fernet import InvalidToken
        if isinstance(exc, InvalidToken):
            raise RuntimeError(
                "Substack 凭证解密失败：SUBSTACK_COOKIE_ENC_KEY 与录入时不一致。"
                "请删除该凭证后重新录入 Cookie。"
            ) from exc
        raise


# ---------- Cookie 清洗（双值） ----------

SID_KEY = "substack.sid"
LLI_KEY = "substack.lli"


def _extract_cookie_value(raw: str, key: str) -> str:
    """从一段粘贴文本中提取某个 cookie 的值，容忍以下形态：
      - 裸值
      - `key=value`
      - 头串片段 `key=value; ...`
    """
    if raw is None:
        return ""
    raw = raw.strip()
    if not raw:
        return ""
    # 整段里找 key=value
    m = re.search(rf"{re.escape(key)}\s*=\s*([^;]+)", raw)
    if m:
        return m.group(1).strip()
    # 不含 '=' 且长度合理 → 当作裸值
    if "=" not in raw:
        return raw
    return ""


def clean_cookie(sid: str, lli: str = "") -> str:
    """把 sid + lli 两个值规整成 JSON 字符串 {"substack.sid":..,"substack.lli":..}。

    容忍前端任一字段粘贴完整头串；缺失 lli 时仅存 sid。
    """
    sid_val = _extract_cookie_value(sid or "", SID_KEY)
    lli_val = _extract_cookie_value(lli or "", LLI_KEY)

    payload: Dict[str, str] = {}
    if sid_val:
        payload[SID_KEY] = sid_val
    if lli_val:
        payload[LLI_KEY] = lli_val

    if SID_KEY not in payload:
        raise ValueError("缺少 substack.sid（必需），请从浏览器 Cookie 复制")

    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))


def parse_cookie_json(cookie_json: str) -> Dict[str, str]:
    """解密后的 JSON 字符串 → dict（缺失键补空串）。"""
    try:
        data = json.loads(cookie_json)
    except Exception:
        return {SID_KEY: "", LLI_KEY: ""}
    if not isinstance(data, dict):
        return {SID_KEY: "", LLI_KEY: ""}
    return {SID_KEY: str(data.get(SID_KEY) or ""), LLI_KEY: str(data.get(LLI_KEY) or "")}


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


def _mask_cookie(cookie_json: str) -> str:
    """掩码 Cookie，绝不向前端暴露明文 sid。"""
    data = parse_cookie_json(cookie_json)
    sid = data.get(SID_KEY, "")
    has_lli = bool(data.get(LLI_KEY))
    if sid:
        visible = sid[:6] + "***" + sid[-4:] if len(sid) > 10 else "***"
        lli_flag = "+lli" if has_lli else ""
        return f"substack.sid={visible}{lli_flag}"
    return "***"


# ---------- SubstackAuth 物化（核心差异点） ----------

def _write_temp_cookies(cookies: Dict[str, str]) -> str:
    """把 {sid, lli} 写成 substack-api 期望的 cookies.json 数组，返回临时文件路径。"""
    items = []
    for name in (SID_KEY, LLI_KEY):
        val = cookies.get(name)
        if val:
            items.append({
                "name": name,
                "value": val,
                "domain": ".substack.com",
                "path": "/",
                "secure": True,
            })
    if not items:
        raise ValueError("Cookie 内容为空（无 substack.sid）")

    fd, path = tempfile.mkstemp(suffix=".json", prefix="substack_cookies_")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(items, f)
    except Exception:
        try:
            os.unlink(path)
        except OSError:
            pass
        raise
    return path


# ---------- URL 归一化（复刻 scripts/substack/substack_common.py） ----------

def normalize_publication_url(value: str) -> str:
    value = (value or "").strip()
    if not value:
        raise ValueError("empty publication URL")
    if "://" not in value:
        if value.endswith(".substack.com") or "." in value:
            value = f"https://{value}"
        else:
            value = f"https://{value}.substack.com"
    parsed = urlparse(value)
    host = parsed.netloc.lower().removeprefix("www.")
    if not host:
        raise ValueError(f"invalid publication URL: {value!r}")
    return f"https://{host}"


def publication_url_aliases(value: str) -> List[str]:
    """为 publication 生成抓取候选 URL（保留顺序、去重）。

    自定义域名常见故障：
      - 裸域 archive 404，但 www 可用（如 shrubstack.com）
      - 自定义域名 TCP 超时，但 {slug}.substack.com 可用（如 geopoliticaldispatch.com）

    自定义域名优先试 www / *.substack.com，把可能超时的裸域放到最后。
    """
    try:
        primary = normalize_publication_url(value)
    except ValueError:
        return []
    host = urlparse(primary).netloc.lower()
    out: List[str] = []
    seen: set = set()

    def _add(url: str) -> None:
        u = (url or "").rstrip("/")
        if not u or u in seen:
            return
        seen.add(u)
        out.append(u)

    if host and host != "substack.com" and not host.endswith(".substack.com"):
        slug = host.split(".")[0]
        _add(f"https://www.{host}")
        if slug and slug not in ("www", "substack"):
            _add(f"https://{slug}.substack.com")
        _add(primary)
    else:
        _add(primary)
    return out


def is_retryable_archive_error(exc: BaseException) -> bool:
    """自定义域名 archive 失败时是否值得换候选 URL 重试。"""
    import requests as _requests

    if isinstance(exc, (TimeoutError, ConnectionError, OSError)):
        return True
    if isinstance(exc, _requests.exceptions.Timeout):
        return True
    if isinstance(exc, _requests.exceptions.ConnectionError):
        return True
    if isinstance(exc, _requests.exceptions.HTTPError):
        resp = getattr(exc, "response", None)
        status = getattr(resp, "status_code", None)
        return status in (404, 408, 425, 429, 500, 502, 503, 504)
    # urllib3 / 包装异常
    name = type(exc).__name__
    return name in {
        "ConnectTimeoutError",
        "MaxRetryError",
        "NewConnectionError",
        "ConnectTimeout",
        "ReadTimeout",
    }


def host_matches_author_handle(host: str, handle: str) -> bool:
    """判断 publication host 是否对应该作者 handle（用于 notes 归属对齐）。"""
    handle = (handle or "").strip().lower()
    host = (host or "").strip().lower().removeprefix("www.")
    if not handle or not host:
        return False
    if host == f"{handle}.substack.com":
        return True
    label = host.split(".")[0]
    if not label:
        return False
    # citrini ↔ citriniresearch.com；capitalflows ↔ capitalflows.substack.com
    return label == handle or label.startswith(handle) or handle.startswith(label)


def resolve_subscribed_publication_url(
    author_pub_url: str,
    author_handle: str,
    user_ids_by_pub: Dict[str, List[int]],
) -> Tuple[str, List[int]]:
    """把 notes 归属到用户实际订阅的 publication_url。

    优先精确/别名命中；否则用 handle 匹配已订阅 URL；再否则 orphan 回退。
    """
    try:
        norm = normalize_publication_url(author_pub_url)
    except ValueError:
        norm = (author_pub_url or "").strip() or "https://substack.com"

    if norm in user_ids_by_pub:
        return norm, list(user_ids_by_pub[norm])

    author_aliases = set(publication_url_aliases(norm))
    for sub_url, uids in user_ids_by_pub.items():
        if not uids:
            continue
        if sub_url in author_aliases:
            return sub_url, list(uids)
        if author_aliases.intersection(publication_url_aliases(sub_url)):
            return sub_url, list(uids)
        host = urlparse(sub_url).netloc.lower()
        if host_matches_author_handle(host, author_handle):
            return sub_url, list(uids)

    all_uids = sorted({uid for ids in user_ids_by_pub.values() for uid in ids})
    return norm, all_uids


def resolve_publication_display_name(
    *,
    publication_name: Optional[str] = None,
    author_name: Optional[str] = None,
    publication_url: Optional[str] = None,
) -> str:
    """Feeds 展示名：优先作者名（如「徐老猫」），其次刊物名，最后 URL。"""
    for candidate in (author_name, publication_name, publication_url):
        if candidate and str(candidate).strip():
            return str(candidate).strip()
    return publication_url or ""


def normalize_avatar_url(value: Optional[str]) -> str:
    """规范化头像 URL（去掉 query，与微博订阅逻辑一致）。"""
    s = (value or "").strip()
    if not s:
        return ""
    return s.split("?")[0]


def _avatar_from_publication_api(pub: Dict[str, Any]) -> str:
    """从 subscriptions / publication API 对象提取 logo / 作者头像。"""
    for key in ("author_photo_url", "logo_url", "cover_photo_url"):
        avatar = normalize_avatar_url(pub.get(key))
        if avatar:
            return avatar
    return ""


_PUBLIC_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


def fetch_publication_avatar_public(pub_url: str) -> str:
    """通过公开 API 拉取刊物主作者头像（无需 Cookie）。

    GET {publication_url}/api/v1/publication/users/ranked?public=true
    """
    try:
        url = normalize_publication_url(pub_url)
    except ValueError:
        return ""
    if url == "https://substack.com":
        return ""

    endpoint = f"{url.rstrip('/')}/api/v1/publication/users/ranked?public=true"
    try:
        resp = requests.get(endpoint, headers=_PUBLIC_HEADERS, timeout=20)
        resp.raise_for_status()
        authors = resp.json()
    except Exception as exc:
        logger.warning("Substack 公开头像拉取失败 url=%s: %s", url, exc)
        return ""

    if not isinstance(authors, list):
        return ""

    for author in authors:
        if author.get("is_byline_only"):
            continue
        avatar = normalize_avatar_url(author.get("photo_url"))
        if avatar:
            return avatar
    if authors:
        return normalize_avatar_url(authors[0].get("photo_url"))
    return ""


def publication_url_from_api(pub: Dict[str, Any]) -> Optional[str]:
    """从 subscriptions API 的 publication 对象解析根 URL。

    优先 {subdomain}.substack.com：自定义域名在部分网络环境下不可达（超时/404），
    而 *.substack.com 更稳定；订阅侧仍可使用用户选择的 custom domain。
    """
    subdomain = pub.get("subdomain")
    if subdomain:
        try:
            return normalize_publication_url(f"{subdomain}.substack.com")
        except ValueError:
            pass
    for field in ("base_url", "custom_domain"):
        raw = pub.get(field)
        if raw:
            try:
                return normalize_publication_url(raw)
            except ValueError:
                pass
    return None


# ---------- Substack 内部 API helper（基于 SubstackAuth） ----------

SUBSTACK_HOME = "https://substack.com"
_SUBS_API = "https://substack.com/api/v1/subscriptions"
_FOLLOWING_API = "https://substack.com/api/v1/feed/following"
_HOME_HEADERS = {"Referer": "https://substack.com/home"}


def _auth_get(auth, url: str, *, params: Optional[Dict[str, Any]] = None,
              retries: int = 3, timeout: int = 30) -> Any:
    """auth.get 的重试封装：429/5xx 与网络错误退避，其余错误抛出。"""
    last_err: Any = ""
    for attempt in range(retries):
        try:
            resp = auth.get(
                url,
                params=params,
                headers=_HOME_HEADERS,
                timeout=timeout,
            )
        except Exception as exc:
            last_err = exc
            time.sleep(1.5 * (attempt + 1))
            continue
        if resp.ok:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504):
            last_err = f"{resp.status_code} {url}: {resp.text[:200]}"
            time.sleep(1.5 * (attempt + 1))
            continue
        raise RuntimeError(f"{resp.status_code} {url}: {resp.text[:300]}")
    raise RuntimeError(f"GET 失败(已重试 {retries} 次): {url}: {last_err}")


def fetch_subscriptions_via_auth(auth) -> List[Dict[str, Any]]:
    """源 A：拉账号订阅的 publication 列表（复刻 substack_common.fetch_subscriptions）。"""
    data = _auth_get(auth, _SUBS_API, params={"tvOnly": "false"})
    results: List[Dict[str, Any]] = []
    seen: set = set()

    for item in (data.get("subscriptions") or []):
        pub = item.get("publication", item)
        url = publication_url_from_api(pub)
        if not url or url in seen:
            continue
        seen.add(url)
        results.append({
            "publication_id": pub.get("id"),
            "name": pub.get("name", ""),
            "url": url,
            "subdomain": pub.get("subdomain"),
            "custom_domain": pub.get("custom_domain"),
            "membership_state": item.get("membership_state") or item.get("type"),
            "avatar": _avatar_from_publication_api(pub),
        })

    for pub in (data.get("publications") or []):
        url = publication_url_from_api(pub)
        if not url or url in seen:
            continue
        seen.add(url)
        results.append({
            "publication_id": pub.get("id"),
            "name": pub.get("name", ""),
            "url": url,
            "subdomain": pub.get("subdomain"),
            "custom_domain": pub.get("custom_domain"),
            "membership_state": pub.get("membership_state"),
            "avatar": _avatar_from_publication_api(pub),
        })

    return results


def fetch_following_via_auth(auth) -> List[int]:
    """源 B：Notes 关注的 user_id 列表（去掉首个=自己）。"""
    data = _auth_get(auth, _FOLLOWING_API)
    if not isinstance(data, list):
        return []
    ids = [x for x in data if isinstance(x, int)]
    return ids[1:] if ids else []


def fetch_profile_owner_via_auth(auth, user_id: int) -> Optional[Dict[str, Any]]:
    """解析 user_id → 作者主刊信息（复刻 substack_common.fetch_profile_owner）。"""
    url = f"https://substack.com/api/v1/reader/feed/profile/{user_id}"
    data = _auth_get(auth, url)
    items = data.get("items") if isinstance(data, dict) else None
    if not items:
        return None

    owner: Optional[Dict[str, Any]] = None
    for it in items:
        for u in (it.get("context") or {}).get("users") or []:
            if u.get("id") == user_id:
                owner = u
                break
        if owner:
            break
    if owner is None:
        users0 = (items[0].get("context") or {}).get("users") or []
        owner = users0[0] if users0 else {}
        if not owner:
            return None

    pub = owner.get("primary_publication") or {}
    pub_url = publication_url_from_api(pub) or ""

    return {
        "user_id": user_id,
        "name": owner.get("name") or "",
        "handle": owner.get("handle") or "",
        "photo_url": normalize_avatar_url(owner.get("photo_url")),
        "publication_id": pub.get("id"),
        "publication_name": pub.get("name") or "",
        "publication_url": pub_url,
        "subdomain": pub.get("subdomain"),
        "custom_domain": pub.get("custom_domain"),
    }


def fetch_author_notes_via_auth(auth, user_id: int, *, cursor: Optional[str] = None,
                                limit: int = 25) -> Dict[str, Any]:
    """拉某作者动态 profile 流单页（复刻 substack_common.fetch_author_notes）。"""
    params: Dict[str, Any] = {"limit": str(limit)}
    if cursor:
        params["cursor"] = cursor
    return _auth_get(
        auth,
        f"https://substack.com/api/v1/reader/feed/profile/{user_id}",
        params=params,
    )


# ---------- 凭证 CRUD ----------

class SubstackCredentialService:
    """Substack 凭证管理服务。"""

    def _materialize_auth(self, cred_id: int):
        """解密 → 写临时 cookies.json → 返回 SubstackAuth（调用方负责 try/finally 清理）。

        返回 (auth, tmp_path)；若凭证不存在或 sid 缺失抛 RuntimeError。
        """
        try:
            from substack_api import SubstackAuth  # 延迟导入，避免未装包时影响模块加载
        except ImportError as exc:
            # 旧版 substack-api（<1.3.0）不导出 SubstackAuth → 转成 RuntimeError，
            # 由 validate/refresh 的 except 捕获，返回 200 + 清晰提示，而非裸 500。
            raise RuntimeError(
                "未安装或版本过低的 substack-api（需 >=1.3.0，参考 scripts/substack/requirements.txt）。"
                f"请在服务器执行 pip install -U \"substack-api>=1.3.0\" 后重启 backend。原因: {exc}"
            ) from exc

        cookie_json = self.get_decrypted_cookie(cred_id)
        if not cookie_json:
            raise RuntimeError(f"凭证 id={cred_id} 不存在")
        cookies = parse_cookie_json(cookie_json)
        if not cookies.get(SID_KEY):
            raise RuntimeError(f"凭证 id={cred_id} 缺少 substack.sid")

        tmp_path = _write_temp_cookies(cookies)
        try:
            auth = SubstackAuth(cookies_path=tmp_path)
        except Exception:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
        if not getattr(auth, "authenticated", False):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise RuntimeError(f"凭证 id={cred_id} SubstackAuth 认证失败（cookies 无效）")
        return auth, tmp_path

    # ---- 录入 ----

    def create_credential(self, label: str, sid: str, lli: str = "") -> Dict[str, Any]:
        cleaned = clean_cookie(sid, lli)
        encrypted = encrypt_cookie(cleaned)
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO substack_credentials (label, encrypted_cookie, status)
                    VALUES (%s, %s, 'unknown')
                    RETURNING id
                    """,
                    (label, encrypted),
                )
                cred_id = cur.fetchone()[0]
            conn.commit()
        logger.info("Substack 凭证已录入 id=%s label=%s", cred_id, label)
        return {"id": cred_id, "label": label, "status": "unknown"}

    def update_credential(
        self,
        cred_id: int,
        label: Optional[str] = None,
        sid: Optional[str] = None,
        lli: Optional[str] = None,
    ) -> bool:
        updates: List[str] = ["updated_at = now()"]
        params: List[Any] = []

        if label is not None:
            updates.append("label = %s")
            params.append(label)
        if sid is not None or lli is not None:
            # 任一提供则重洗 JSON（保留另一旧值）
            existing = self.get_decrypted_cookie(cred_id) or "{}"
            base = parse_cookie_json(existing)
            new_sid = _extract_cookie_value(sid, SID_KEY) if sid is not None else base.get(SID_KEY, "")
            new_lli = _extract_cookie_value(lli, LLI_KEY) if lli is not None else base.get(LLI_KEY, "")
            cleaned = clean_cookie(new_sid, new_lli)
            updates.append("encrypted_cookie = %s")
            updates.append("status = 'unknown'")
            updates.append("last_error = NULL")
            params.append(encrypt_cookie(cleaned))

        if len(updates) == 1:
            return False

        params.append(cred_id)
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"UPDATE substack_credentials SET {', '.join(updates)} WHERE id = %s",
                    params,
                )
                updated = cur.rowcount
            conn.commit()
        return updated > 0

    def delete_credential(self, cred_id: int) -> bool:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM substack_credentials WHERE id = %s", (cred_id,))
                deleted = cur.rowcount
            conn.commit()
        return deleted > 0

    def list_credentials(self) -> List[Dict[str, Any]]:
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, label, encrypted_cookie, status, last_validated_at,
                           last_error, created_at, updated_at
                    FROM substack_credentials
                    ORDER BY id
                    """
                )
                rows = [dict(r) for r in cur.fetchall()]
        for r in rows:
            r["masked_cookie"] = _mask_cookie(r.pop("encrypted_cookie"))
        return rows

    def get_decrypted_cookie(self, cred_id: int) -> Optional[str]:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT encrypted_cookie FROM substack_credentials WHERE id = %s",
                    (cred_id,),
                )
                row = cur.fetchone()
        if not row:
            return None
        return decrypt_cookie(row[0])

    def get_decrypted_cookies(self, cred_id: int) -> Dict[str, str]:
        cookie_json = self.get_decrypted_cookie(cred_id)
        if not cookie_json:
            return {SID_KEY: "", LLI_KEY: ""}
        return parse_cookie_json(cookie_json)

    def get_first_valid_cookie(self) -> Optional[Tuple[int, str]]:
        """返回首个 valid/unknown 凭证 (id, decrypted_json)；无则 None。"""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, encrypted_cookie
                    FROM substack_credentials
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
                    UPDATE substack_credentials
                    SET status = 'expired', last_error = %s, updated_at = now()
                    WHERE id = %s
                    """,
                    (error[:500], cred_id),
                )
            conn.commit()
        logger.warning("Substack 凭证 id=%s 已标记为 expired: %s", cred_id, error)

    def mark_valid(self, cred_id: int) -> None:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE substack_credentials
                    SET status = 'valid', last_validated_at = now(), last_error = NULL, updated_at = now()
                    WHERE id = %s
                    """,
                    (cred_id,),
                )
            conn.commit()

    # ---- 校验 ----

    def validate_credential(self, cred_id: int) -> Dict[str, Any]:
        """探活 subscriptions API，更新状态。"""
        try:
            auth, tmp_path = self._materialize_auth(cred_id)
        except RuntimeError as e:
            with _get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE substack_credentials SET last_error=%s, updated_at=now() WHERE id=%s",
                        (str(e)[:500], cred_id),
                    )
                conn.commit()
            return {"success": False, "error": str(e)}

        try:
            try:
                subs = fetch_subscriptions_via_auth(auth)
            except RuntimeError as exc:
                msg = str(exc)
                if "401" in msg or "403" in msg:
                    self.mark_expired(cred_id, msg)
                    return {"success": False, "error": f"Cookie 已过期 ({msg})"}
                raise
            self.mark_valid(cred_id)
            return {"success": True, "subscriptions_count": len(subs)}
        except Exception as e:
            with _get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE substack_credentials SET last_error=%s, last_validated_at=now(), updated_at=now() WHERE id=%s",
                        (str(e)[:500], cred_id),
                    )
                conn.commit()
            return {"success": False, "error": str(e)}
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    # ---- 同步订阅源并集 ----

    def refresh_publications(self, cred_id: int) -> Dict[str, Any]:
        """拉源 A subscriptions + 源 B following(解析主刊)，按 publication_url 合并 upsert。

        - substack_publications: is_subscribed / is_followed 标记来源侧
        - substack_followed_authors: per-author 登记
        """
        try:
            auth, tmp_path = self._materialize_auth(cred_id)
        except RuntimeError as e:
            return {"success": False, "error": str(e)}

        try:
            # 源 A
            try:
                subs = fetch_subscriptions_via_auth(auth)
            except RuntimeError as exc:
                if "401" in str(exc) or "403" in str(exc):
                    self.mark_expired(cred_id, str(exc))
                    return {"success": False, "error": "Cookie 已过期"}
                raise
            self.mark_valid(cred_id)

            # 源 B
            following_ids: List[int] = []
            authors: List[Dict[str, Any]] = []
            try:
                following_ids = fetch_following_via_auth(auth)
            except Exception as exc:
                logger.warning("Substack refresh: 拉取关注列表失败(跳过源B): %s", exc)

            for uid in following_ids:
                try:
                    info = fetch_profile_owner_via_auth(auth, uid)
                except Exception as exc:
                    logger.warning("Substack refresh: 解析作者 id=%s 失败: %s", uid, exc)
                    info = None
                if info:
                    authors.append(info)

            synced_subs = 0
            synced_authors = 0
            followed_pub_urls: set = set()

            with _get_conn() as conn:
                with conn.cursor() as cur:
                    # 源 A → publications, is_subscribed=true
                    for s in subs:
                        url = s.get("url")
                        if not url:
                            continue
                        cur.execute(
                            """
                            INSERT INTO substack_publications
                                (publication_url, publication_id, publication_name, avatar,
                                 is_subscribed, credential_id, enabled)
                            VALUES (%s, %s, %s, %s, true, %s, true)
                            ON CONFLICT (publication_url) DO UPDATE SET
                                publication_id   = COALESCE(EXCLUDED.publication_id, substack_publications.publication_id),
                                publication_name = COALESCE(NULLIF(EXCLUDED.publication_name, ''), substack_publications.publication_name),
                                avatar           = COALESCE(NULLIF(EXCLUDED.avatar, ''), substack_publications.avatar),
                                is_subscribed    = true,
                                credential_id    = COALESCE(EXCLUDED.credential_id, substack_publications.credential_id),
                                updated_at       = now()
                            """,
                            (url, s.get("publication_id"), s.get("name") or "", s.get("avatar") or "", cred_id),
                        )
                        synced_subs += 1

                    # 源 B → followed_authors + publications(is_followed=true)
                    for a in authors:
                        pub_url = a.get("publication_url") or "https://substack.com"
                        followed_pub_urls.add(pub_url)
                        cur.execute(
                            """
                            INSERT INTO substack_followed_authors
                                (substack_user_id, author_handle, author_name, avatar,
                                 publication_url, credential_id, enabled)
                            VALUES (%s, %s, %s, %s, %s, %s, true)
                            ON CONFLICT (substack_user_id) DO UPDATE SET
                                author_handle  = EXCLUDED.author_handle,
                                author_name    = EXCLUDED.author_name,
                                avatar         = COALESCE(NULLIF(EXCLUDED.avatar, ''), substack_followed_authors.avatar),
                                publication_url = EXCLUDED.publication_url,
                                credential_id  = COALESCE(EXCLUDED.credential_id, substack_followed_authors.credential_id),
                                updated_at     = now()
                            """,
                            (
                                a.get("user_id"),
                                a.get("handle") or "",
                                a.get("name") or "",
                                a.get("photo_url") or "",
                                pub_url,
                                cred_id,
                            ),
                        )
                        synced_authors += 1

                    # 把关注作者的主刊也加入 publications 并标 is_followed（写入刊物名 + 头像）
                    pub_url_names: Dict[str, str] = {}
                    pub_url_avatars: Dict[str, str] = {}
                    for a in authors:
                        url = a.get("publication_url") or ""
                        if not url:
                            continue
                        title = (a.get("publication_name") or "").strip()
                        if title and url not in pub_url_names:
                            pub_url_names[url] = title
                        avatar = (a.get("photo_url") or "").strip()
                        if avatar and url not in pub_url_avatars:
                            pub_url_avatars[url] = avatar

                    for pub_url in followed_pub_urls:
                        cur.execute(
                            """
                            INSERT INTO substack_publications
                                (publication_url, publication_name, avatar, is_followed, enabled)
                            VALUES (%s, %s, %s, true, true)
                            ON CONFLICT (publication_url) DO UPDATE SET
                                publication_name = COALESCE(
                                    NULLIF(EXCLUDED.publication_name, ''),
                                    substack_publications.publication_name
                                ),
                                avatar = COALESCE(
                                    NULLIF(EXCLUDED.avatar, ''),
                                    substack_publications.avatar
                                ),
                                is_followed = true,
                                updated_at  = now()
                            """,
                            (pub_url, pub_url_names.get(pub_url, ""), pub_url_avatars.get(pub_url, "")),
                        )
                conn.commit()

            self._backfill_missing_avatars(limit=30)

            logger.info(
                "Substack 凭证 id=%s 同步: 源A %d publications, 源B %d authors",
                cred_id, synced_subs, synced_authors,
            )
            return {
                "success": True,
                "synced_subscriptions": synced_subs,
                "synced_authors": synced_authors,
            }
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

    # ---- 手动维护 / 查询 ----

    def _backfill_missing_avatars(self, limit: int = 30) -> int:
        """对尚无头像的 publication 走公开 API 补拉（best-effort）。"""
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT publication_url
                    FROM substack_publications
                    WHERE NULLIF(TRIM(avatar), '') IS NULL
                      AND publication_url <> 'https://substack.com'
                    ORDER BY updated_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
                missing = [row[0] for row in cur.fetchall()]

        updated = 0
        for pub_url in missing:
            avatar = fetch_publication_avatar_public(pub_url)
            if not avatar:
                continue
            with _get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE substack_publications
                        SET avatar = %s, updated_at = now()
                        WHERE publication_url = %s
                          AND NULLIF(TRIM(avatar), '') IS NULL
                        """,
                        (avatar, pub_url),
                    )
                conn.commit()
            updated += 1
            time.sleep(0.3)
        return updated

    def ensure_publication_avatar(self, publication_url: str) -> str:
        """返回 publication 头像；DB 无缓存时走公开 API 拉取并写入。"""
        url = normalize_publication_url(publication_url)
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT p.avatar,
                           (
                               SELECT fa.avatar
                               FROM substack_followed_authors fa
                               WHERE fa.publication_url = p.publication_url
                                 AND NULLIF(TRIM(fa.avatar), '') IS NOT NULL
                               ORDER BY fa.author_name
                               LIMIT 1
                           ) AS author_avatar
                    FROM substack_publications p
                    WHERE p.publication_url = %s
                    """,
                    (url,),
                )
                row = cur.fetchone()

        cached = ""
        if row:
            cached = normalize_avatar_url(row.get("avatar") or row.get("author_avatar"))
        if cached:
            return cached

        avatar = fetch_publication_avatar_public(url)
        if avatar:
            with _get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE substack_publications
                        SET avatar = %s, updated_at = now()
                        WHERE publication_url = %s
                        """,
                        (avatar, url),
                    )
                conn.commit()
        return avatar

    def upsert_publication_manual(
        self,
        publication_url: str,
        publication_name: str = "",
        enabled: bool = True,
    ) -> Dict[str, Any]:
        url = normalize_publication_url(publication_url)
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO substack_publications
                        (publication_url, publication_name, enabled)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (publication_url) DO UPDATE SET
                        publication_name = EXCLUDED.publication_name,
                        enabled          = EXCLUDED.enabled,
                        updated_at       = now()
                    RETURNING id
                    """,
                    (url, publication_name, enabled),
                )
                pid = cur.fetchone()[0]
            conn.commit()
        return {"id": pid, "publication_url": url, "publication_name": publication_name}

    def list_publications(self, enabled_only: bool = False) -> List[Dict[str, Any]]:
        sql = """
            SELECT p.id, p.publication_url, p.publication_id, p.publication_name,
                   p.is_subscribed, p.is_followed, p.enabled,
                   p.backfill_complete, p.last_posts_sync_at, p.last_posts_sync_mode,
                   c.label AS credential_label, c.status AS credential_status,
                   COALESCE(
                       NULLIF(TRIM(p.avatar), ''),
                       (
                           SELECT NULLIF(TRIM(fa.avatar), '')
                           FROM substack_followed_authors fa
                           WHERE fa.publication_url = p.publication_url
                             AND NULLIF(TRIM(fa.avatar), '') IS NOT NULL
                           ORDER BY fa.author_name
                           LIMIT 1
                       )
                   ) AS avatar,
                   (
                       SELECT fa.author_name
                       FROM substack_followed_authors fa
                       WHERE fa.publication_url = p.publication_url
                         AND NULLIF(TRIM(fa.author_name), '') IS NOT NULL
                       ORDER BY fa.author_name
                       LIMIT 1
                   ) AS author_name
            FROM substack_publications p
            LEFT JOIN substack_credentials c ON p.credential_id = c.id
        """
        if enabled_only:
            sql += " WHERE p.enabled = true"
        sql += " ORDER BY p.publication_name, p.publication_url"
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                return [dict(r) for r in cur.fetchall()]

    def list_followed_authors(self, enabled_only: bool = False) -> List[Dict[str, Any]]:
        sql = """
            SELECT a.id, a.substack_user_id, a.author_handle, a.author_name, a.avatar,
                   a.publication_url, a.enabled, a.backfill_complete,
                   a.recent_note_ids, a.last_notes_sync_at,
                   c.label AS credential_label, c.status AS credential_status
            FROM substack_followed_authors a
            LEFT JOIN substack_credentials c ON a.credential_id = c.id
        """
        if enabled_only:
            sql += " WHERE a.enabled = true"
        sql += " ORDER BY a.author_name, a.author_handle"
        with _get_conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                return [dict(r) for r in cur.fetchall()]
