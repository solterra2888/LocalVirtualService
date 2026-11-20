#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""LLM 兜底:GLM 1301 审核拦截时的单发直连调用(CherryIN 网关,OpenAI 兼容)。

背景(2026-11-19):facts-report 管线 geopolitics 章节素材(俄乌/美伊/制裁
中文快讯原文)触发 GLM 内容审核 1301,zcode CLI 以 "Turn execution failed"
退出;同 payload 重试必然再挂,单章节失败放倒整条 40-60 分钟管线。实测
CherryIN 网关(backend llm_provider_configs 表,gemini/claude 已启用)对
同一批素材不拦截 —— 本模块在 1301 发生时把被拦调用改走网关单发完成。

配置来源(优先级 env > DB):
    MB_FALLBACK_PROVIDER  provider_id,默认 gemini(Claude 亦实测可用)
    MB_FALLBACK_MODEL     模型 id(缺省取 DB 行 default_model)
    MB_FALLBACK_BASE_URL  网关地址(缺省取 DB 行 base_url)
    MB_FALLBACK_API_KEY   API key(缺省取 DB 行 api_key)
    MB_FALLBACK_TIMEOUT   单次调用超时秒数(默认 600)
    MB_FALLBACK_FORCE     强制指定 agent 走兜底(演练用,如 geopolitics;
                          多 agent 逗号分隔;=1 表示全部)
读不到配置 → 兜底禁用,行为与引入本模块前一致。
"""

from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]

# 1301/1302/1303 = 智谱内容审核类错误码;"sensitive content" 兼容中英文变体
_MODERATION_RE = re.compile(
    r"\[130[123]\]|sensitive content|unsafe content|potentially unsafe"
    r"|content.*(filter|policy|violation)", re.I)

_ZCODE_LOG_DIR = Path.home() / ".zcode" / "cli" / "log"
_TRACE_RE = re.compile(r"traceId[:\s]+([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}"
                       r"-[0-9a-f]{4}-[0-9a-f]{12})", re.I)

# node SQLite ExperimentalWarning 等噪音行 —— 提取根因时丢弃
_NOISE_RE = re.compile(
    r"ExperimentalWarning|--trace-warnings|Loading environment variables", re.I)

DONE_MARKER = "<!-- SECTION DONE -->"


class ModerationBlockedError(RuntimeError):
    """GLM 内容审核拦截(1301)——确定性失败,同 payload 重试无意义。"""


class FallbackUnavailableError(RuntimeError):
    """兜底未配置或调用失败(网络/网关/解析),调用方按各自降级策略处理。"""


def moderation_blocked(stderr: str) -> bool:
    return bool(_MODERATION_RE.search(stderr or ""))


def classify_zcode_failure(stderr: str) -> bool:
    """判定一次 zcode 失败是否内容审核拦截。

    实测(2026-11-19):zcode CLI 的 stderr 只有 "Error: Turn execution
    failed (traceId: …)",1301 详情只落在 ~/.zcode/cli/log/zcode-<日期>.jsonl
    的 model.stream_attempt_cleanup.failed 事件里 —— 故先直接匹配 stderr,
    未命中再按 traceId 去当日(及前一日,防跨零点)CLI 日志找审核报错行。
    只在 zcode 失败的罕见路径上执行;日志按日轮转(数 MB),整读可接受
    (尾部窗口截读会错过当天早些的失败事件,同日实测踩过)。
    """
    stderr = stderr or ""
    if moderation_blocked(stderr):
        return True
    m = _TRACE_RE.search(stderr)
    if not m:
        return False
    trace = m.group(1)
    from datetime import date, timedelta
    for d in (date.today(), date.today() - timedelta(days=1)):
        f = _ZCODE_LOG_DIR / f"zcode-{d:%Y-%m-%d}.jsonl"
        if not f.exists():
            continue
        try:
            text = f.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for line in text.splitlines():
            if trace in line and _MODERATION_RE.search(line):
                return True
    return False


def extract_root_cause(stderr: str, tail_chars: int = 400) -> str:
    """从 zcode stderr 里提取真实 provider 错误行,过滤 node 噪音。

    原实现只取 stderr[-500:],常被两条 ExperimentalWarning 吃掉,真根因
    (1301 行)进不了 cron.log 与 Lark 告警(2026-11-19 排障实测)。
    """
    lines = [ln.strip() for ln in (stderr or "").splitlines() if ln.strip()]
    keep = [ln for ln in lines if not _NOISE_RE.search(ln)]
    if not keep:
        keep = lines[-3:]
    text = " | ".join(keep)
    return text[-tail_chars:]


# ── 按 agent 路由(MB_AGENT_PROVIDER;2026-11-19 outlook 常驻 sonnet-5.5) ──
# 格式: "agent1:model_spec[,agent2:model_spec...]",model_spec 带厂商前缀
# (anthropic/… → DB claude 行;google/… → gemini 行)。命中的 agent 单发
# 直连为"主路径",失败回落管线原 GLM agent 路径(run_pipeline._run_one_agent)。
def agent_provider_override(agent: str) -> str:
    val = (os.environ.get("MB_AGENT_PROVIDER")
           or _env_file_lookup("MB_AGENT_PROVIDER") or "").strip()
    for pair in val.split(","):
        pair = pair.strip()
        if not pair or ":" not in pair:
            continue
        name, _, spec = pair.partition(":")
        if name.strip() == agent:
            return spec.strip()
    return ""


# 模型前缀 → DB provider_id(决定用哪行的 base_url/api_key)
_PROVIDER_PREFIXES = {"anthropic/": "claude", "google/": "gemini",
                      "openai/": "claude"}  # openai/* 走 cherryin claude 行同网关


def resolve_model_config(model_spec: str) -> Dict[str, str]:
    """model_spec(如 anthropic/claude-sonnet-5.5)→ {base_url, api_key, model}。

    前缀找不到映射时回退全局兜底配置(gemini 行);env 的 BASE_URL/API_KEY
    覆盖仍生效(测试/演练用)。
    """
    provider = ""
    for prefix, pid in _PROVIDER_PREFIXES.items():
        if model_spec.startswith(prefix):
            provider = pid
            break
    if provider:
        cfg = dict(_db_provider_row(provider))
    else:
        cfg = {}
    cfg["model"] = model_spec
    cfg.setdefault("base_url", "")
    cfg.setdefault("api_key", "")
    if os.environ.get("MB_FALLBACK_BASE_URL"):
        cfg["base_url"] = os.environ["MB_FALLBACK_BASE_URL"]
    if os.environ.get("MB_FALLBACK_API_KEY"):
        cfg["api_key"] = os.environ["MB_FALLBACK_API_KEY"]
    return cfg




_Config = Dict[str, str]
_cached: Optional[_Config] = None


def _env_file_lookup(key: str) -> str:
    """同 build_snapshot._eodhd_api 的 .env 读取模式(cron 环境 DB_* 不在 env)。"""
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return ""
    m = re.search(rf"^{re.escape(key)}=(.+)$",
                  env_path.read_text(encoding="utf-8"), re.MULTILINE)
    return m.group(1).strip() if m else ""


def _load_config() -> _Config:
    global _cached
    if _cached is not None:
        return _cached
    provider = (os.environ.get("MB_FALLBACK_PROVIDER")
                or _env_file_lookup("MB_FALLBACK_PROVIDER") or "gemini")
    cfg: Dict[str, str] = {
        "provider": provider,
        "model": os.environ.get("MB_FALLBACK_MODEL", "")
                 or _env_file_lookup("MB_FALLBACK_MODEL"),
        "base_url": os.environ.get("MB_FALLBACK_BASE_URL", "")
                    or _env_file_lookup("MB_FALLBACK_BASE_URL"),
        "api_key": os.environ.get("MB_FALLBACK_API_KEY", "")
                   or _env_file_lookup("MB_FALLBACK_API_KEY"),
        "timeout": os.environ.get("MB_FALLBACK_TIMEOUT", "")
                   or _env_file_lookup("MB_FALLBACK_TIMEOUT") or "600",
    }
    if not (cfg["model"] and cfg["base_url"] and cfg["api_key"]):
        cfg.update(_db_provider_row(provider))
    _cached = cfg
    return cfg


def _db_provider_row(provider: str) -> Dict[str, str]:
    """从 backend 的 llm_provider_configs 读一行(base_url/default_model/api_key)。

    复用与 backend 相同的 DB 凭据(.env 的 DB_*;cron 环境已验证可用)。
    读不到(无 psycopg2/无行/无凭据)返回空 dict → 兜底禁用。
    """
    try:
        import psycopg2  # guanhe conda env 已装;其它环境优雅降级
    except ImportError:
        return {}
    try:
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST") or _env_file_lookup("DB_HOST")
                   or "localhost",
            port=os.environ.get("DB_PORT") or _env_file_lookup("DB_PORT") or "5432",
            dbname=os.environ.get("DB_NAME") or _env_file_lookup("DB_NAME"),
            user=os.environ.get("DB_USER") or _env_file_lookup("DB_USER"),
            password=os.environ.get("DB_PASSWORD")
                      or _env_file_lookup("DB_PASSWORD") or "",
            connect_timeout=5,
        )
        with conn, conn.cursor() as cur:
            cur.execute(
                "SELECT base_url, default_model, api_key FROM "
                "llm_provider_configs WHERE provider_id=%s AND enabled "
                "LIMIT 1", (provider,))
            row = cur.fetchone()
        conn.close()
    except Exception:  # noqa: BLE001 — DB 不可达不阻断主管线
        return {}
    if not row:
        return {}
    return {"base_url": row[0] or "", "model": row[1] or "",
            "api_key": row[2] or ""}


def fallback_available() -> bool:
    cfg = _load_config()
    return bool(cfg["model"] and cfg["base_url"] and cfg["api_key"])


def describe_fallback() -> str:
    cfg = _load_config()
    return (f"{cfg['provider']}/{cfg['model']} @ {cfg['base_url']}"
            if fallback_available() else "disabled")


def force_fallback(agent: str) -> bool:
    """MB_FALLBACK_FORCE 演练开关:该 agent 是否强制走兜底(不烧 GLM 调用)。

    值为 "1"/"all" → 全部;"geopolitics,plan" → 指定集合;逗号分隔。
    """
    val = (os.environ.get("MB_FALLBACK_FORCE")
           or _env_file_lookup("MB_FALLBACK_FORCE") or "").strip().lower()
    if not val or val in ("0", "no", "false"):
        return False
    if val in ("1", "all"):
        return True
    names = {x.strip() for x in val.split(",") if x.strip()}
    return agent in names


# ── OpenAI 兼容单发调用 ────────────────────────────────────────────────

def chat_completion(messages: List[Dict[str, str]],
                    max_tokens: int = 16384,
                    model_spec: str = "") -> Tuple[str, Dict[str, Any]]:
    """一次网关调用,返回 (正文, usage)。失败抛 FallbackUnavailableError。

    model_spec 非空 = 按 agent 路由的定向调用(如 anthropic/claude-sonnet-5.5,
    凭据按前缀解析);为空 = 全局兜底配置(MB_FALLBACK_*,默认 gemini)。
    """
    cfg = (resolve_model_config(model_spec) if model_spec
           else _load_config())
    timeout = (os.environ.get("MB_FALLBACK_TIMEOUT")
               or _env_file_lookup("MB_FALLBACK_TIMEOUT") or "600")
    if not (cfg.get("model") and cfg.get("base_url") and cfg.get("api_key")):
        raise FallbackUnavailableError(
            f"gateway not configured (model_spec={model_spec or '-'}, "
            f"base_url={'set' if cfg.get('base_url') else 'missing'}, "
            f"key={'set' if cfg.get('api_key') else 'missing'})")
    req = urllib.request.Request(
        cfg["base_url"].rstrip("/") + "/v1/chat/completions",
        data=json.dumps({
            "model": cfg["model"],
            "messages": messages,
            "max_tokens": max_tokens,
        }).encode("utf-8"),
        headers={"Authorization": f"Bearer {cfg['api_key']}",
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(
                req, timeout=int(timeout)) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", "replace")[:300]
        except Exception:  # noqa: BLE001
            pass
        raise FallbackUnavailableError(
            f"gateway HTTP {e.code}: {body}") from e
    except Exception as e:  # noqa: BLE001 — 网络/超时/解析统一降级
        raise FallbackUnavailableError(f"gateway call failed: {e}") from e
    try:
        content = data["choices"][0]["message"]["content"] or ""
    except (KeyError, IndexError, TypeError) as e:
        raise FallbackUnavailableError(
            f"unexpected gateway response: {str(data)[:300]}") from e
    return content, _normalize_usage(data.get("usage") or {})


def _normalize_usage(usage: Dict[str, Any]) -> Dict[str, Any]:
    """网关 prompt_tokens/completion_tokens → zcode 口径 inputTokens/…,
    供 run_pipeline._add_usage 直接累计。"""
    pd = usage.get("prompt_tokens_details") or {}
    return {
        "inputTokens": usage.get("prompt_tokens") or 0,
        "outputTokens": usage.get("completion_tokens") or 0,
        "cacheReadTokens": pd.get("cached_tokens") or 0,
    }


def parse_json_payload(text: str) -> Dict[str, Any]:
    """容错解析模型输出里的单个 JSON 对象(同 run_pipeline._parse_zcode_json
    语义:截取首个 { 到末个 };额外剥 ```json 围栏)。"""
    text = (text or "").strip()
    fence = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL)
    if fence:
        text = fence.group(1).strip()
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except json.JSONDecodeError:
        pass
    start, end = text.find("{"), text.rfind("}")
    if start >= 0 and end > start:
        obj = json.loads(text[start:end + 1])
        if isinstance(obj, dict):
            return obj
    raise FallbackUnavailableError(
        f"cannot parse fallback JSON output: {text[:300]!r}")


# ── 单发写文件兜底(run_agent 的 1301 替身) ────────────────────────────

FALLBACK_DELIVERY = """

## FALLBACK DELIVERY MODE (single-shot — read carefully)

You have NO tools and NO filesystem access in this mode. Disregard any
instruction above about writing files yourself; ALL content rules, source
restrictions, style rules and the claims-ledger contract (rules 1-11) still
apply unchanged.

The COMPLETE snapshot.json you are permitted to read is inlined below between
<snapshot> markers. Treat it exactly as the attached snapshot.json file.

Output EXACTLY ONE JSON object (no prose around it, no code fence) of the form:
{{"files": {{"<path>": "<full file content>", ...}}}}
containing an entry for EVERY output file demanded above (each section .md
file and each matching .claims.json ledger file). Keys must be the exact
relative paths named above. Each section .md content must END with the line
`{done_marker}`. Each .claims.json content must be a valid JSON array (not a
string). Write the JSON with standard escaping; do not truncate.

<snapshot>
{snapshot}
</snapshot>
"""


def build_single_shot_prompt(agent_prompt: str, snapshot_text: str,
                             extra_context: str = "") -> str:
    """extra_context 插在任务书与快照之间(如 outlook 需内联的第一段成品)。"""
    return (agent_prompt + extra_context
            + FALLBACK_DELIVERY.replace(
                "{done_marker}", DONE_MARKER).replace(
                "{snapshot}", snapshot_text))


def run_single_shot_files(agent_prompt: str, snapshot_text: str,
                          expected_files: List[str],
                          model_spec: str = "",
                          max_tokens: int = 16384,
                          extra_context: str = "",
                          debug_path: Optional[Path] = None,
                          ) -> Tuple[Dict[str, str], Dict[str, Any]]:
    """单发让模型一次产出全部交付文件,返回 ({相对路径: 内容}, usage)。

    expected_files 用于落盘前的白名单校验(防路径逃逸/漏交付由调用方判)。
    model_spec 非空 = 按 agent 路由的定向模型;max_tokens:outlook 这类
    长输出节用 32000(16384 实测会截断 JSON 信封)。

    2026-11-20 修复:输出信封缺陷(空字段/漏文件/坏 JSON,长输出尾部的
    采样波动,4 次实测 2 次)不再直接放弃 —— 带着具体缺陷提示原地重试
    一次再抛;原始响应无论成败都写 debug_path 备查(此前失败无原文可尸检)。
    """
    prompt = build_single_shot_prompt(agent_prompt, snapshot_text,
                                      extra_context)
    last_err: Optional[FallbackUnavailableError] = None
    for attempt in (1, 2):
        msg_prompt = prompt
        if attempt > 1 and last_err is not None:
            msg_prompt = (
                f"{prompt}\n\nIMPORTANT — your previous attempt was rejected: "
                f"{last_err}. Repeat the ENTIRE output, fixing exactly that: "
                f"every files value must be non-empty, valid, and complete "
                f"(claims ledgers are JSON arrays, not strings; do not "
                f"truncate the tail).")
        content, usage = chat_completion(
            [{"role": "user", "content": msg_prompt}], max_tokens=max_tokens,
            model_spec=model_spec)
        if debug_path is not None:
            try:
                debug_path.parent.mkdir(parents=True, exist_ok=True)
                debug_path.write_text(content, encoding="utf-8")
            except OSError:
                pass  # 落盘仅供排障,失败不影响主流程
        try:
            files = _validate_files_payload(content, expected_files)
            return files, usage
        except FallbackUnavailableError as e:
            last_err = e
            print(f"[fallback] single-shot attempt {attempt} rejected "
                  f"({str(e)[:160]})", flush=True)
    raise last_err  # type: ignore[misc]


def _validate_files_payload(content: str,
                            expected_files: List[str]) -> Dict[str, str]:
    """校验单发返回的 files 信封;不合格抛 FallbackUnavailableError。"""
    obj = parse_json_payload(content)
    files = obj.get("files")
    if not isinstance(files, dict) or not files:
        raise FallbackUnavailableError(
            f"output has no files object: {str(obj)[:200]!r}")
    out: Dict[str, str] = {}
    for path, body in files.items():
        path = str(path).strip().lstrip("./")
        if path not in expected_files:
            raise FallbackUnavailableError(
                f"returned unexpected path {path!r} "
                f"(expected one of {expected_files})")
        if not isinstance(body, str) or not body.strip():
            raise FallbackUnavailableError(
                f"returned empty content for {path}")
        out[path] = body
    return out


def write_fallback_files(ws: Path, files: Dict[str, str],
                         section_files: List[str]) -> None:
    """落盘兜底产物:章节文件缺 DONE_MARKER 则补;claims 必须是合法 JSON 数组。

    section_files = 需要 DONE_MARKER 的 .md 相对路径(claims 文件除外)。
    抛 FallbackUnavailableError 表示产物不合格,由调用方走原有失败路径。
    """
    for path, body in files.items():
        text = body.rstrip() + "\n"
        if path in section_files and DONE_MARKER not in text:
            text = text.rstrip() + f"\n{DONE_MARKER}\n"
        if path.endswith(".claims.json"):
            try:
                claims = json.loads(text)
            except json.JSONDecodeError as e:
                raise FallbackUnavailableError(
                    f"fallback claims {path} is not valid JSON: {e}") from e
            if not isinstance(claims, list):
                raise FallbackUnavailableError(
                    f"fallback claims {path} is not a JSON array")
            text = json.dumps(claims, ensure_ascii=False, indent=2) + "\n"
        dest = ws / path
        dest.parent.mkdir(parents=True, exist_ok=True)
        # 防路径逃逸:ws/path 必须仍在 ws 内(run_single_shot_files 已按
        # 白名单校验,这里再挡一层绝对路径/..)
        if dest.resolve().parent != ws.resolve() and \
                ws.resolve() not in dest.resolve().parents:
            raise FallbackUnavailableError(f"fallback path escape: {path}")
        dest.write_text(text, encoding="utf-8")
