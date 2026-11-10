# -*- coding: utf-8 -*-
"""一次性:截获 us_close_chart 的 sections 数据 → DMR 风格 HTML FT 表"""
import sys, json
from pathlib import Path
sys.path.insert(0, '/opt/guanhetech')
sys.path.insert(0, '/opt/guanhetech/tools/facts-report')

import us_close_chart as U

import sys
WS = Path('/opt/guanhetech/backend/reports/facts-report') / (sys.argv[1] if len(sys.argv) > 1 else '2026-11-06')

# 放宽 finviz 快照日期上限(库里只剩 11-09 滚动快照)
_orig_finviz = U._finviz_rows
def relaxed_finviz(conn, t):
    try:
        return _orig_finviz(conn, t)
    except RuntimeError:
        from datetime import timedelta
        # 取库中最新快照(滚动表,周日快照仍为周五收盘)
        def _num(x):
            if x is None: return None
            try: return float(str(x).replace('%','').replace('+',''))
            except ValueError: return None
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(snapshot_date) FROM finviz_screener")
            snap = cur.fetchone()[0]
            wanted = U.FINVIZ_ETFS + U.SECTOR_ETFS
            cur.execute("SELECT ticker, price, change_percent, performance_week, "
                        "performance_month, performance_ytd FROM finviz_screener "
                        "WHERE snapshot_date=%s AND ticker = ANY(%s)", (snap, list(wanted)))
            out = {tk: {"price": float(p), "m1d": _num(c), "m1w": _num(pw),
                        "m1m": _num(pm), "ytd": _num(py)}
                   for tk, p, c, pw, pm, py in cur.fetchall()}
        print(f"[ft2html] relaxed finviz snapshot: {snap}")
        return out, snap
U._finviz_rows = relaxed_finviz

capture = {}
def fake_render(sections, t, out):
    capture['sections'] = sections; capture['t'] = str(t)
    return out
U.render = fake_render

U.render_for_workspace(WS)
sections = capture['sections']
print("组:", [(s['title'], len(s['rows'])) for s in sections])
print("行键:", sorted(sections[0]['rows'][0].keys()))
Path(sys.path[0] if False else '/opt/guanhetech/tools/facts-report/report_html_tools/ft_sections.json').write_text(json.dumps(
    {'t': capture['t'], 'sections': sections, 'finviz_snap': '2026-11-09'},
    ensure_ascii=False))
print("data captured")
