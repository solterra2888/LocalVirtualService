#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Morning Brief 发布注册器 — facts-report 管线尾部 → DB(morning_brief_reports) + OSS。

由 cron_daily.sh 在 run_pipeline 结束后调用(注册失败不影响管线 rc,自行 Lark 告警):
    python register_morning_brief.py --ok 2026-11-09            # 管线成功
    python register_morning_brief.py --fail 2026-11-09 --rc 1   # 管线失败
    python register_morning_brief.py --ok 2026-11-09 --no-oss   # 本地联调
也可对历史工作区手动补挂。

日期口径: report_date = 美股交易日 T + 1 天(北京生成日,与 close_brief 一致;
正常调度时即"今天")。metadata.session_date = T(工作区目录名)。

HTML 三级: ① 复用 final/report.html(富版,新于 report.md 时)
          ② subprocess 跑 report_html_tools/md2html_report.py <T>(node 预渲染富版)
          ③ mistune 简版 + morning_brief_report.html.j2 壳兜底
发布前把 charts/* 相对路径图片内嵌为 base64(单文件自包含 —— /shared
iframe 与 OSS 签名链接下无 charts/ 兄弟目录,相对路径必 404 裂图)。
OSS: reports/MorningBrief/report_<YYYYMMDD>.html(确定性键,重跑覆盖同键;失败不致命)。
"""

from __future__ import annotations

import argparse
import base64
import json
import mimetypes
import re
import subprocess
import sys
import time
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Optional
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from backend.utils.datetime_utils import utcnow  # noqa: E402
from backend.utils.logger import get_logger  # noqa: E402

logger = get_logger("register_morning_brief")

FACTS_ROOT = REPO_ROOT / "backend" / "reports" / "facts-report"
MD2HTML = Path(__file__).resolve().parent / "report_html_tools" / "md2html_report.py"
OSS_KEY_PREFIX = "reports/MorningBrief"
MD2HTML_TIMEOUT_SEC = 600


def _report_date_for(session_date: str) -> date:
    """report_date = T + 1(北京生成日)。"""
    return date.fromisoformat(session_date) + timedelta(days=1)


def _rich_html(ws: Path, session_date: str) -> tuple[Optional[str], str]:
    """富版 report.html:复用 > 重新生成,两级;均失败返回 (None, 'mistune_fallback')。"""
    final = ws / "final"
    html_path = final / "report.html"
    md_path = final / "report.md"
    if html_path.exists() and md_path.exists() \
            and html_path.stat().st_mtime >= md_path.stat().st_mtime:
        html = html_path.read_text(encoding="utf-8")
        if html.strip():
            logger.info(f"[mb] 复用既有富版: {html_path}")
            return html, "reused"
    try:
        logger.info(f"[mb] 运行 md2html_report.py {session_date} …")
        subprocess.run(
            [sys.executable, str(MD2HTML), session_date],
            check=True, capture_output=True, text=True,
            timeout=MD2HTML_TIMEOUT_SEC,
        )
        if html_path.exists():
            html = html_path.read_text(encoding="utf-8")
            if html.strip():
                logger.info("[mb] md2html 生成富版成功")
                return html, "md2html"
    except Exception as exc:  # noqa: BLE001 — 兜底路径
        logger.warning(f"[mb] md2html 失败,走 mistune 兜底: {exc}")
    return None, "mistune_fallback"


# img src 引用,兼容两种形态:预渲染 DOM 的裸引号 src="…" 与嵌入 JSON
# payload 的转义引号 src=\"…\"(report.html 的 tab 数据以 <script> 内嵌)
_IMG_SRC_RE = re.compile(r'(src=)(\\?["\'])([^"\'\n\\]+?)\2')


def _inline_charts(html: str, ws: Path) -> tuple[str, list[str]]:
    """相对路径图片(charts/*)就地内嵌为 base64 data URI。

    发布物是单文件 HTML(DB html_content + OSS 单对象),charts/ 目录不随行;
    /shared/{token} iframe 与 OSS 签名链接下相对路径必 404。绝对引用
    (http/https/data/协议相对)与磁盘上不存在的引用原样保留。
    """
    final_dir = (ws / "final").resolve()
    inlined: list[str] = []

    def _sub(m: re.Match) -> str:
        path = m.group(3)
        if re.match(r"^(?:https?:|data:|//)", path):
            return m.group(0)
        f = (final_dir / path).resolve()
        if not f.is_file() or not f.is_relative_to(final_dir):
            return m.group(0)
        mime = mimetypes.guess_type(f.name)[0] or "image/png"
        b64 = base64.b64encode(f.read_bytes()).decode("ascii")
        inlined.append(path)
        return f"{m.group(1)}{m.group(2)}data:{mime};base64,{b64}{m.group(2)}"

    return _IMG_SRC_RE.sub(_sub, html), inlined


def _fallback_html(ws: Path, session_date: str, report_date: date) -> str:
    """mistune 简版 + j2 壳(与 close_brief 渲染同口径,含表格插件)。"""
    import mistune
    import mistune.plugins.table as table_plugin
    from jinja2 import Environment, FileSystemLoader, select_autoescape
    from markupsafe import Markup

    md = (ws / "final" / "report.md").read_text(encoding="utf-8")
    parser = mistune.Markdown(mistune.HTMLRenderer(),
                              plugins=[table_plugin.table])
    env = Environment(
        loader=FileSystemLoader(str(REPO_ROOT / "backend" / "templates")),
        autoescape=select_autoescape(["html"]),
    )
    now = utcnow()
    template = env.get_template("morning_brief_report.html.j2")
    return template.render(
        report_date=report_date.strftime("%B %d, %Y"),
        session_date=session_date,
        generated_at_utc=now.strftime("%Y-%m-%d %H:%M UTC"),
        generated_at_new_york=now.astimezone(
            ZoneInfo("America/New_York")).strftime("%b %d, %Y %H:%M ET"),
        generated_at_beijing=now.astimezone(
            ZoneInfo("Asia/Shanghai")).strftime("%b %d, %Y %H:%M Beijing"),
        workspace=str(ws),
        summary_html=Markup(parser(md)),
    )


def _state_metadata(ws: Path, session_date: str, html_src: str,
                    oss_uploaded: bool, oss_object: Optional[str]) -> Dict[str, Any]:
    """workspace state.json → 精简 metadata(缺失容忍)。"""
    meta: Dict[str, Any] = {
        "session_date": session_date,
        "workspace": str(ws),
        "html_source": html_src,          # reused | md2html | mistune_fallback
        "oss_uploaded": oss_uploaded,
        "oss_object_name": oss_object,
        "registered_at_utc": utcnow().isoformat(),
    }
    try:
        st = json.loads((ws / "state.json").read_text(encoding="utf-8"))
        for k in ("usage_total", "usage_by_phase", "timings"):
            if st.get(k) is not None:
                meta[k] = st[k]
        v = st.get("verify") or {}
        if isinstance(v, dict):
            meta["verify"] = {k: v.get(k) for k in
                              ("attempts", "status", "llm_fix_used")}
        meta["published"] = st.get("published")
    except Exception as exc:  # noqa: BLE001
        meta["state_error"] = str(exc)
    card = ws / "pipeline-overview.html"
    if card.exists():
        meta["pipeline_card"] = str(card)
    return meta


def register_ok(session_date: str, do_oss: bool = True) -> int:
    from backend.services.morning_brief_report_service import (
        MorningBriefReportService,
    )

    ws = FACTS_ROOT / session_date
    md_path = ws / "final" / "report.md"
    if not md_path.exists():
        raise FileNotFoundError(f"workspace report.md not found: {md_path}")

    report_date = _report_date_for(session_date)

    html, html_src = _rich_html(ws, session_date)
    if html is None:
        html = _fallback_html(ws, session_date, report_date)
        logger.info("[mb] 使用 mistune 兜底渲染")

    # 单文件自包含:相对路径图片内嵌(mistune 兜底版同样需要)
    html, inlined_imgs = _inline_charts(html, ws)
    if inlined_imgs:
        logger.info(f"[mb] 图片已内嵌为 data URI: {inlined_imgs}")

    oss_object: Optional[str] = None
    oss_uploaded = False
    if do_oss:
        try:
            from backend.services.oss_service import OSSService

            oss_object = (f"{OSS_KEY_PREFIX}/"
                          f"report_{report_date.strftime('%Y%m%d')}.html")
            res = OSSService().upload_bytes(
                object_name=oss_object,
                data=html.encode("utf-8"),
                content_type="text/html; charset=utf-8",
            )
            oss_uploaded = bool(res.get("success"))
            if not oss_uploaded:
                logger.warning(f"[mb] OSS 上传失败(保留 DB 版本): {res.get('error')}")
                oss_object = None
        except Exception as exc:  # noqa: BLE001 — OSS 故障不阻断注册
            logger.warning(f"[mb] OSS 上传异常(保留 DB 版本): {exc}")
            oss_object = None

    metadata = _state_metadata(ws, session_date, html_src, oss_uploaded,
                               oss_object)
    metadata["images_inlined"] = inlined_imgs

    # 数据新鲜度台账(us_close_chart._dump_ft_json 落盘):缺 T 收盘的资产
    # 写入元数据 + 推 Lark 告警 —— 报告照常发布(表中已是最近可得数据),
    # 但必须让所有人看见,杜绝静默展示过期行情(2026-11-12 SOX 事故的教训)。
    # 同一文件还提取实时行情规格 ft_live(行名→实时代码+周期参考价):
    # 报告打开时 /shared/{token}/quotes 据此就地刷新 FT 表(详见 service)。
    ft_warnings: list[str] = []
    ft_live: Optional[Dict[str, Any]] = None
    ft_path = ws / "final" / "charts" / "ft_sections.json"
    try:
        if ft_path.exists():
            _ftd = json.loads(ft_path.read_text(encoding="utf-8"))
            ft_warnings = [
                f"{s['name']}(截至 {s.get('as_of') or '无数据'})"
                for s in (_ftd.get("stale") or [])
            ]
            _spec_rows = [
                {"n": r.get("name"), "c": r.get("code"),
                 "u": r.get("unit") or "%", "r": r.get("refs") or {}}
                for _s in (_ftd.get("sections") or [])
                for r in (_s.get("rows") or []) if r.get("code")
            ]
            if _spec_rows:
                ft_live = {"t": _ftd.get("t"), "rows": _spec_rows}
    except Exception as exc:  # noqa: BLE001 — 台账读取失败只降级为无告警
        logger.warning(f"[mb] 新鲜度台账读取失败: {exc}")
    if ft_live:
        metadata["ft_live"] = ft_live
    # 涨跌榜按需 K 线规格:行序与网页 __RANK__ 一致,/shared/{token}/kline
    # 按 (market, 行序) 定符号 —— 服务端决定抓什么,客户端不能任意传符号。
    try:
        _sn = json.loads((ws / "snapshot.json").read_text(encoding="utf-8"))
        _vl = (((_sn.get("sources") or {}).get("market") or {})
               .get("volume_leaders")) or {}
        _ks = {m: [r.get("ysym") for r
                   in ((_vl.get(m) or {}).get("rows") or [])]
               for m in ("us", "hk", "cn")}
        if any(_ks.values()):
            metadata["rank_kline"] = _ks
        # 涨跌榜实时刷新规格(2026-11-13):行序对齐 rank_kline;每行 = EODHD
        # 实时代码 + 周期参考收盘(build_snapshot perf_refs)。报告打开时
        # /shared/{token}/quotes 就地刷新价格/当日/5日/1月/YTD —— 港/A 股
        # 收盘早于查看时刻,快照 T 日值会被刷新为最新收盘(对齐 Yahoo 展示)。
        def _eod_code(r: Dict[str, Any]) -> Optional[str]:
            ex, code = r.get("exchange"), str(r.get("code") or "")
            if not code:
                return None
            if ex == "US":
                return code + ".US"
            if ex == "HK":
                return code.zfill(4) + ".HK"
            if ex in ("SHG", "SHE"):
                return code + "." + ex
            return None
        _rl = {"t": _sn.get("report_date")}
        for _m in ("us", "hk", "cn"):
            _ents = []
            for _r in ((_vl.get(_m) or {}).get("rows") or []):
                _c = _eod_code(_r)
                _refs = _r.get("perf_refs")
                _ents.append({"c": _c, "r": _refs} if _c and _refs else None)
            if any(_ents):
                _rl[_m] = _ents          # None 占位保行序(kline 索引对齐)
        if len(_rl) > 1:
            metadata["rank_live"] = _rl
    except Exception as exc:  # noqa: BLE001 — 规格缺失只丢了点击蜡烛图
        logger.warning(f"[mb] rank_kline 规格读取失败: {exc}")
    if ft_warnings:
        metadata["data_warnings"] = ft_warnings
        logger.warning(f"[mb] ⚠ 数据新鲜度告警: {ft_warnings}")

    result = MorningBriefReportService().complete(
        report_date,
        file_path=str(md_path),
        oss_object_name=oss_object,
        html_content=html,
        metadata=metadata,
    )
    print(f"[mb] registered: {result}")

    if ft_warnings:
        try:
            from lark_alert import notify

            notify(
                f"【facts-report ⚠️】Morning Brief {result['report_date']} 行情数据不完整",
                [f"美股交易日: {session_date}",
                 "以下资产缺当日收盘,表中为最近可得数据:",
                 " · " + "\n · ".join(ft_warnings),
                 f"报告: /api/morning-brief-report/shared/{result['share_token']}"],
            )
        except Exception as exc:  # noqa: BLE001 — 告警失败不影响注册
            logger.warning(f"[mb] ⚠ 新鲜度 Lark 告警发送失败: {exc}")


def register_fail(session_date: str, rc: int) -> int:
    from backend.services.morning_brief_report_service import (
        MorningBriefReportService,
    )

    report_date = _report_date_for(session_date)
    MorningBriefReportService().fail(
        report_date, f"facts-report pipeline rc={rc} (workspace {session_date})")
    print(f"[mb] marked failed: {report_date}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--ok", metavar="SESSION_DATE",
                   help="管线成功:工作区日期 T(YYYY-MM-DD)")
    g.add_argument("--fail", metavar="SESSION_DATE", help="管线失败:工作区日期 T")
    ap.add_argument("--rc", type=int, default=1, help="失败退出码(配合 --fail)")
    ap.add_argument("--no-oss", action="store_true", help="跳过 OSS 上传(联调)")
    args = ap.parse_args()

    try:
        if args.ok:
            return register_ok(args.ok, do_oss=not args.no_oss)
        return register_fail(args.fail, args.rc)
    except Exception as exc:  # noqa: BLE001 — 注册失败自行告警,不抛给 cron
        logger.error(f"[mb] register error: {exc}", exc_info=True)
        try:
            from lark_alert import notify

            notify("【facts-report 🔴】Morning Brief 注册失败",
                   [f"错误: {exc}", "管线产物可能已生成但未落库,需人工 register"])
        except Exception:  # noqa: BLE001
            pass
        return 0  # 不改写管线 rc


if __name__ == "__main__":
    raise SystemExit(main())
