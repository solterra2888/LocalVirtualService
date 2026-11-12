# -*- coding: utf-8 -*-
"""一次性:facts-report final/report.md → DMR 风格四 tab HTML(final/report.html)"""
import json, re, base64
from pathlib import Path

import sys as _argv
_date = _argv.argv[1] if len(_argv.argv) > 1 else '2026-11-06'
WS = Path('/opt/guanhetech/backend/reports/facts-report') / _date
md = (WS / 'final' / 'report.md').read_text(encoding='utf-8')
BS = chr(92)

title = md.splitlines()[0].lstrip('# ').strip()
date = ''
m = re.search(r'—\s*(20\d\d-\d\d-\d\d)', md[:400])
if m: date = m.group(1)

secs = {}
order = []
for chunk in re.split(r'^## ', md, flags=re.M)[1:]:
    name = chunk.splitlines()[0].strip()
    secs[name] = '## ' + chunk
    order.append(name)

TAB_DEF = [
    ('大盘总览', ['大盘总览', '今日要闻', '核心驱动', '持仓与资金流']),
    ('美股全景', ['美股全景', '板块动态', '个股深挖']),
    ('今日涨跌榜', ['今日涨跌榜']),
    ('宏观', ['宏观速览', '回顾 · 宏观数据', '回顾 · 地缘政治', '美联储观察']),
    ('展望与策略', ['展望与策略']),
    ('数据流向', None),
]
# 宽松匹配(标题里的 · 与空格)
def find_sec(label):
    for name in order:
        if name == label or name.replace(' · ', '').replace(' ', '') == label.replace(' · ', '').replace(' ', ''):
            return name
    return None

tab_payload = []
for tab, labels in TAB_DEF:
    items = []
    for lab in (labels or []):
        name = find_sec(lab)
        if name:
            items.append({'h': name, 'md': secs[name]})
    tab_payload.append({'tab': tab, 'secs': items, 'sankey': labels is None})



# ── markdown 构建期渲染(node+marked ESM,页面零运行时)──
import subprocess
def md_batch_render(md_list):
    script = ("import { marked } from '/opt/guanhetech/web-frontend/node_modules/marked/lib/marked.esm.js'; "
              "let d=''; process.stdin.on('data',c=>d+=c).on('end',()=>{"
              "const a=JSON.parse(d); process.stdout.write(JSON.stringify(a.map(m=>marked.parse(m))));});")
    r = subprocess.run(['node', '--input-type=module', '-e', script],
                       input=json.dumps(md_list, ensure_ascii=False),
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError('node render failed: ' + r.stderr[:200])
    return json.loads(r.stdout)

_tab_md_list = []
for t in tab_payload:
    for it in t['secs']:
        _tab_md_list.append(it['md'])
_htmls = md_batch_render(_tab_md_list) if _tab_md_list else []
_k = 0
for t in tab_payload:
    for it in t['secs']:
        h = _htmls[_k]; _k += 1
        # 卡片标题由渲染层(s.h)输出,去掉正文里重复的首个 <h2>
        m2 = re.match(r'\s*<h2[^>]*>(.*?)</h2>', h)
        if m2 and m2.group(1).strip() == it['h'].strip():
            h = h[m2.end():].lstrip()
        it['html'] = h
        it.pop('md')

# ── FT 跨资产收盘表(替换 Tab1 大盘总览的 PNG)──
# 数据源:优先本工作区 final/charts/ft_sections.json(管线 render_for_workspace
# 每日刷新,与 01_markets.png 同批);回退一次性脚本捕获的静态副本(旧工作区)。
FT_TABLE = ''
FT_STALE = []
FT_HIST = []
ftp = WS / 'final' / 'charts' / 'ft_sections.json'
if not ftp.exists():
    ftp = Path(__file__).parent / 'ft_sections.json'
if ftp.exists():
    D = json.loads(ftp.read_text(encoding='utf-8'))
    FT_STALE = D.get('stale') or []   # 新鲜度台账(us_close_chart 落盘);非空→表格上方横幅
    def _cell(v, u):
        if v is None: return '<td class="num na">—</td>'
        cls = 'pos' if v > 0 else ('neg' if v < 0 else '')
        sv = f'{v:+.1f}%' if u == '%' else f'{v:+.0f}{u}'
        return f'<td class="num {cls}">{sv}</td>'
    def _barcell(v, scale, unit):
        """1日涨跌幅单元格:条形区占满整格(零轴居中),数值贴格子外侧边缘
        (涨右/跌左);标签带卡片底色胶囊,与条形重叠也保持可读。"""
        if v is None:
            return ('<div class="bwrap"><div class="zero"></div>'
                    '<span class="bv na" style="left:50%;'
                    'transform:translate(-50%,-50%)">—</span></div>')
        w = min(abs(v) / scale, 1.0) * 48   # 满格 48%:边缘留隙,条形不顶到数值胶囊
        pos_ = v > 0
        side = 'left:50%' if pos_ else 'right:50%'
        cls = 'bpos' if pos_ else 'bneg'
        bv = f'{v:+.2f}%' if unit == '%' else f'{v:+.1f}{unit}'
        vc = 'pos' if pos_ else 'neg'
        edge = 'right:0' if pos_ else 'left:0'
        return (f'<div class="bwrap"><div class="zero"></div>'
                f'<div class="bar {cls}" style="{side};width:{w:.1f}%"></div>'
                f'<span class="bv {vc}" style="{edge}">{bv}</span></div>')
    # 指数代码 → TradingView(DMR Temperature 同款映射与 URL);ETF/板块行情非指数,
    # 保留 finviz 个股页。之前因 finviz 无页面而隐藏的 R2K/SOX/DXY 因此解锁展示。
    from urllib.parse import quote as _urlq
    TV_INDEX = {
        'SPX': 'TVC:SPX', 'NDX': 'NASDAQ:NDX', 'DJIA': 'TVC:DJI',
        'R2K': 'TVC:RUT', 'SOX': 'NASDAQ:SOX', 'DXY': 'TVC:DXY',
        'IH': 'SSE:000016', 'IF': 'SSE:000300',   # 中金所期货 → 标的指数(DMR 同款)
        'IC': 'SSE:000905', 'IM': 'SSE:000852',
        '000001.SH': 'SSE:000001', '399001.SZ': 'SZSE:399001',   # 现货现金指数
        '399006.SZ': 'SZSE:399006',
        'HSI': 'HSI:HSI',        # TV 官方 symbol_search 确认
        'N225': 'TVC:NI225', 'KOSPI': 'KRX:KOSPI',               # 国际(DMR 同款)
        'FTSE': 'TVC:UKX', 'DAX': 'XETR:DAX',
        '2Y': 'TVC:US02Y', '10Y': 'TVC:US10Y', '30Y': 'TVC:US30Y',  # 美债收益率(DMR 同款)
    }
    def _tk_html(tk):
        if not tk:
            return ''
        if tk in TV_INDEX:
            href = (f'https://www.tradingview.com/chart/?symbol={_urlq(TV_INDEX[tk])}'
                    '&interval=D&range=6M')
            return (f'<a class="tk" href="{href}" target="_blank" rel="noopener" '
                    f'title="TradingView: {TV_INDEX[tk]}">{tk} ↗</a>')
        href = f'https://finviz.com/quote.ashx?t={tk}'
        return f'<a class="tk" href="{href}" target="_blank" rel="noopener">{tk} ↗</a>'
    def _spark_svg(hist, m1d=None):
        """近3月走势缩略图(内联SVG,零JS依赖,~80×26)。着色与"1日涨跌幅"同向:
        当日涨→绿/跌→红/无当日数据→灰(中性);折线形状仍为近3月收盘。
        亮暗模式自动适配;<15 根历史显示占位符。"""
        pts = (hist or [])[-63:]
        if len(pts) < 15:
            return '<span class="spna">—</span>'
        vals = [p[-1] for p in pts]   # 收盘:折线 [iso,c] 与蜡烛 [iso,o,h,l,c] 均为末位
        lo, hi = min(vals), max(vals)
        rng = (hi - lo) or 1.0
        W, H = 80, 26
        xy = [((i / (len(pts) - 1)) * (W - 6) + 3,
               H - 3 - (v - lo) / rng * (H - 6)) for i, v in enumerate(vals)]
        cls = 'pos' if (m1d or 0) > 0 else ('neg' if (m1d or 0) < 0 else 'neutral')
        line = ' '.join(f'{x:.1f},{y:.1f}' for x, y in xy)
        area = f'3,{H-1} ' + line + f' {W-3:.1f},{H-1}'
        lx, ly = xy[-1]
        return (f'<span class="spspark {cls}"><svg viewBox="0 0 {W} {H}" preserveAspectRatio="none">'
                f'<polygon points="{area}" fill="currentColor" opacity="0.12"/>'
                f'<polyline points="{line}" fill="none" stroke="currentColor" '
                f'stroke-width="1.4" stroke-linejoin="round" stroke-linecap="round"/>'
                f'<circle cx="{lx:.1f}" cy="{ly:.1f}" r="1.8" fill="currentColor"/></svg></span>')
    ft_rows = ''
    FT_HIST = []   # 点击行放大的交互图数据(ECharts),与 tr[data-ri] 索引对齐
    for sec in D['sections']:
        ft_rows += (f'<tr class="ftgrp"><td colspan="7" title="条形刻度: {sec["scale_lbl"]}">{sec["title"]}</td></tr>')
        for r in sec['rows']:
            u = r.get('unit') or '%'
            u = '%' if u in ('%', 'pct') else u
            d1 = r.get('m1d')
            ri = len(FT_HIST)
            FT_HIST.append({'n': r['name'], 'u': u, 'a': r.get('as_of'), 'h': r.get('hist') or []})
            ft_rows += (
                f'<tr data-ri="{ri}"><td class="ftasset"><b>{r["name"]}</b>' + _tk_html(r.get('ticker'))
                + f'</td><td class="num">{r.get("last_str") or "—"}<span class="ftunit">{"" if u in ("%", "bp") else " " + u}</span></td>'
                + f'<td class="sparkcell">{_spark_svg(r.get("hist"), r.get("m1d"))}</td>'
                + f'<td class="barcell">{_barcell(d1, sec["scale"], u)}</td>'
                + _cell(r.get('m1w'), u) + _cell(r.get('m1m'), u) + _cell(r.get('ytd'), u) + '</tr>')
    # 列宽固定(auto 布局下 104~337px 参差);走势列(3M缩略图)+1日涨跌幅列见 CSS
    FT_TABLE = ('<div class="ftwrap"><table class="fttbl">'
                '<colgroup><col style="width:19%">'
                '<col style="width:11%"><col style="width:9.5%"><col style="width:23%">'
                '<col style="width:12.5%"><col style="width:12.5%">'
                '<col style="width:12.5%"></colgroup>'
                '<thead><tr><th>资产</th><th class="num">收盘</th><th>走势</th><th class="ftday">1日涨跌幅</th>'
                '<th class="num">1周</th><th class="num">1月</th><th class="num">YTD</th></tr></thead>'
                f'<tbody>{ft_rows}</tbody></table>')
    # 新鲜度横幅:上游行情缺 T 日收盘时显著提示,绝不静默展示旧数据
    if FT_STALE:
        _det = ', '.join(f"{s['name']}({s.get('as_of') or '无数据'})" for s in FT_STALE)
        FT_TABLE = (f'<div class="ftstale">⚠ 数据完整性提醒:以下资产缺 {D["t"]} 收盘,'
                    f'表中为其最近可得数据 — {_det}</div>' + FT_TABLE)



# ── 溯源索引:标签点击/悬停的数据源 ──
import sys as _sys2
_sys2.path.insert(0, '/opt/guanhetech/tools/facts-report')
import provenance as _P
_snap = json.load(open(WS / 'snapshot.json'))
_src = _snap.get('sources') or {}
_idx = _P.item_index(_src) or {}
PROV = {}
for _id, it in _idx.items():
    _t = str(it.get('title') or '').strip()
    if not _t or _t.endswith(('...', '…')):
        _t = it.get('text') or _t
    PROV[_id] = {'a': str(it.get('author') or ''), 'u': str(it.get('url') or ''),
                 'x': _t[:150], 'l': it.get('info_level'), 'e': it.get('evidence_level'),
                 't': str(it.get('date_et') or '')[5:10] + ' ' + str(it.get('time_et') or '') if it.get('time_et') else ''}
_cb = _src.get('cnbc_closing_bell') or {}
def _cb_slot_link(slot):
    m = (_cb.get(slot) or {})
    if m.get('episode_url'):
        return m['episode_url']
    import transcript_prep as _TP
    t = m.get('video_title') or ''
    return _TP._cb_episode_link({'title': t, 'feed_url': None}, []) if t else ''
SHOWS = {
  'the_close': {'label': 'Bloomberg The Close', 'u': '', 'note': '彭博收盘节目完整转录'},
  'cnbc': {'label': 'CNBC Closing Bell(主段)', 'u': _cb_slot_link('main'),
           'note': ((_cb.get('main') or {}).get('video_title') or '官方播客单集') + ' · 音频即转录原始素材'},
  'cnbc_ot': {'label': 'CNBC Closing Bell Overtime(加时段)', 'u': _cb_slot_link('overtime'),
              'note': ((_cb.get('overtime') or {}).get('video_title') or '官方播客单集') + ' · 音频即转录原始素材'},
}
_tc = (_src.get('the_close') or {}).get('video_id')
_tt = (_src.get('the_close') or {}).get('video_title') or ''
if _tc:
    SHOWS['the_close']['u'] = 'https://www.youtube.com/watch?v=' + _tc
    SHOWS['the_close']['note'] = _tt
PROV_JS = json.dumps({'items': PROV, 'shows': SHOWS}, ensure_ascii=False)

# ── Tab5:交互桑基图(从流程卡模板提取的 gdr 组件,同源数据)──
TPL_CARD = Path('/opt/guanhetech/tools/facts-report/pipeline_card_template.html').read_text(encoding='utf-8')
ECH = Path('/opt/guanhetech/references/form13f_dashboard/assets/echarts.min.js').read_text(encoding='utf-8')
i0 = TPL_CARD.index('#gdr-drawer{'); i1 = TPL_CARD.index('.gdr-stages i{')
_gdr_css = TPL_CARD[i0:i1] + TPL_CARD[i1:TPL_CARD.index('}', TPL_CARD.index('.gdr-stages i{')) + 1]
_j0 = TPL_CARD.index('<div id="gdr-drawer">'); _j1 = TPL_CARD.index('</div>', TPL_CARD.index('<div id="gdr-dtip"></div>'))
_gdr_dom = TPL_CARD[_j0:_j1]
_k0 = TPL_CARD.index('(function () {\n  var SAN = window.__SANKEY__') if False else TPL_CARD.index('(function () {' + chr(10) + '  var SAN = window.__SANKEY__')
_k1 = TPL_CARD.index('</script>', _k0)
_gdr_js = TPL_CARD[_k0:_k1]
_gdr_js = _gdr_js.replace("mv.innerHTML = (window.marked && (marked.parse || marked)) ? (marked.parse ? marked.parse(t) : marked(t)) : '<pre>'+t+'</pre>';", "mv.innerHTML = t;")
_gdr_js = _gdr_js.replace("window.addEventListener('resize', function(){ chart.resize(); });",
                          "window.addEventListener('resize', function(){ chart.resize(); });\n  window.__gdrResize = function(){ chart.resize(); };")
import sys as _sys; _sys.path.insert(0, '/opt/guanhetech/tools/facts-report')
import gen_pipeline_card as _G
_sk = _G._sankey_payload(WS)
_sk_md_keys = list((_sk.get('md') or {}).keys())
if _sk_md_keys:
    _sk_htmls = md_batch_render([_sk['md'][k] for k in _sk_md_keys])
    for _k2, _kk in enumerate(_sk_md_keys):
        _sk['md'][_kk] = _sk_htmls[_k2]
SANKEY_JSON = json.dumps(_sk, ensure_ascii=False)

html = """<!DOCTYPE html><html lang="zh"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>__TITLE__</title>
<style>
:root{
 --bg-page:#f6f8fa; --bg-card:#ffffff; --bg-header:linear-gradient(180deg,#1b4b7d 0%,#1d5998 100%);
 --bg-table-header:#1b4b7d; --bg-hover:#f0f4f8;
 --text-primary:#111; --text-secondary:#3d4f5f; --text-muted:#6b7a8a; --text-header:#fff;
 --text-tab:rgba(255,255,255,.65); --text-tab-active:#fff;
 --border:rgba(224,224,224,.6); --border-subtle:#e8ecf1;
 --pos:#0a7c42; --neg:#c41230; --neutral:#888;
 --shadow-card:0 2px 12px rgba(0,0,0,.06);
}
.dark{
 --bg-page:#0B0E14; --bg-card:#141820; --bg-header:linear-gradient(180deg,#1b4b7d 0%,#1d5998 100%);
 --bg-table-header:#1b4b7d; --bg-hover:#1e2536;
 --text-primary:#edf0f4; --text-secondary:#b7c2cf; --text-muted:#7d8a99;
 --border:rgba(60,70,90,.5); --border-subtle:#232b3a; --shadow-card:0 2px 12px rgba(0,0,0,.4);
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg-page);color:var(--text-primary);
 font:15px/1.8 -apple-system,'PingFang SC','Microsoft YaHei',Segoe UI,sans-serif;transition:background .25s}
/* sticky:总标题栏+tab 栏滚动时锁定;z-index 50 —— 内容之上,桑基抽屉(59-61)/溯源气泡(70-71)之下 */
.report-header{position:sticky;top:0;z-index:50;background:var(--bg-header);padding:22px 26px 0;color:var(--text-header);box-shadow:0 2px 10px rgba(27,75,125,.35)}
.report-header .top{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap}
.report-header h1{margin:0;font-size:24px;line-height:1.45;max-width:74em}
.report-header-right{display:flex;align-items:center;gap:12px;font-size:14px;color:var(--text-tab)}
#themeBtn{background:transparent;border:1px solid rgba(255,255,255,.3);color:#fff;border-radius:8px;padding:3px 10px;cursor:pointer;font-size:14px}
.tab-nav{display:flex;gap:4px;margin-top:16px;flex-wrap:wrap}
.tab-btn{background:transparent;border:none;border-bottom:2px solid transparent;color:var(--text-tab);
 padding:9px 18px;font:600 15px/1 inherit;cursor:pointer}
.tab-btn:hover{background:rgba(255,255,255,.1);color:#fff}
.tab-btn.active{color:var(--text-tab-active);border-bottom-color:#fff}
main{max-width:1200px;margin:0 auto;padding:22px 22px 60px}
.tab-panel{display:none}.tab-panel.active{display:block}
section.card{background:var(--bg-card);border:1px solid var(--border);border-radius:10px;
 box-shadow:var(--shadow-card);padding:18px 22px;margin:0 0 18px}
section.card>h2{margin:0 0 4px;font-size:19px;color:#1b4b7d;
 border-left:4px solid #1b4b7d;padding-left:10px}
section.card>h2::before{content:'';display:none}
.dark section.card>h2{color:#9db8d8;border-left-color:#1d5998}
.md h3{font-size:16px;color:var(--text-secondary);margin:16px 0 8px}
.md p{margin:8px 0;color:var(--text-primary)}
.md li{margin:5px 0}
.md strong{color:var(--text-primary)}
.prov.linked{cursor:pointer;text-decoration:underline dotted #7a94b8;text-underline-offset:3px}
.prov.linked:hover{background:#dbe7f5}
#provtip{position:fixed;z-index:70;max-width:450px;display:none;background:#0f172aee;color:#e2e8f0;border-radius:10px;padding:8px 10px;font:12px/1.6 inherit;box-shadow:0 6px 24px rgba(0,0,0,.3);max-height:340px;overflow:auto}
#provtip .pcard{border-bottom:1px solid #334155;padding:4px 0}
#provtip .ph b{color:#7dd3fc;margin-right:6px}
#provtip .pg{color:#fcd34d;margin-right:8px;font-size:11px}
#provtip .pa{color:#93c5fd;margin-right:8px}
#provtip .pt{color:#94a3b8;font-size:11px}
#provtip .px{color:#cbd5e1;margin:3px 0;font-size:11.5px}
#provtip .pu a{color:#86efac;text-decoration:none;font-size:11.5px}
#provtip .pu .nolink{color:#64748b;font-size:11px}
#provpop{position:fixed;z-index:71;top:50%;left:50%;transform:translate(-50%,-50%);width:min(560px,92vw);max-height:78vh;overflow:auto;display:none;background:#fff;border-radius:14px;box-shadow:0 18px 60px rgba(0,0,0,.35);padding:18px 22px;font-size:13px;color:#0f172a}
#provpop .phead{font-size:15px;font-weight:700;margin-bottom:10px;display:flex;justify-content:space-between;align-items:center}
#provpop .pclose{background:none;border:none;font-size:20px;cursor:pointer;color:#64748b}
#provpop .pcard{border:1px solid #e2e8f0;border-radius:10px;padding:10px 14px;margin-bottom:10px;background:#f8fafc}
#provpop .ph{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap}
#provpop .ph b{color:#1d4ed8}
#provpop .pg{color:#b45309;font-size:11.5px}
#provpop .pa{color:#334155;font-weight:600}
#provpop .pt{color:#64748b;font-size:11.5px}
#provpop .px{margin:6px 0;color:#334155;line-height:1.7}
#provpop .pu a{color:#1d4ed8;text-decoration:none;font-weight:600}
#provpop .pu .nolink{color:#94a3b8;font-size:12px}
.md a,#gdr-md a{color:#2d5fa8;text-decoration:none;border-bottom:1px solid #9db4d0}
.dark .md a,.dark #gdr-md a{color:#7dd3fc;border-bottom-color:#2b4a6b}
.md a:hover,#gdr-md a:hover{border-bottom-color:currentColor}
.md img{max-width:100%;border-radius:8px;border:1px solid var(--border-subtle);display:block;margin:8px auto}
.md hr{border:none;border-top:1px solid var(--border-subtle);margin:14px 0}
.md blockquote{border-left:3px solid #2d4a6f;padding:2px 14px;color:var(--text-muted);margin:8px 0;background:var(--bg-hover);border-radius:0 8px 8px 0}
table{border-collapse:collapse;width:100%;margin:12px 0;font-size:13.5px}
th{background:var(--bg-table-header);color:#fff;font-weight:600;padding:8px 10px;text-align:left;white-space:nowrap}
td{padding:7px 10px;border-bottom:1px solid var(--border-subtle);white-space:nowrap}
@media(min-width:769px){table.ranktbl{table-layout:fixed}}
@media(min-width:769px){.fttbl{table-layout:fixed}}
/* 今日涨跌榜交互榜单(排序列头 + 分页栏) */
th.rk-sort{cursor:pointer;user-select:none}
th.rk-sort:hover{filter:brightness(1.18)}   /* 提亮原渐变;background 简写会覆盖深色渐变致白字不可见 */
.rankbar{display:flex;align-items:center;gap:10px;padding:8px 2px 2px;font-size:12.5px;color:var(--text-muted)}
.rk-btn{background:var(--bg-hover);color:var(--text-secondary);border:1px solid var(--border-subtle);border-radius:6px;padding:4px 12px;font:600 12.5px/1 inherit;cursor:pointer}
.rk-btn:hover:not([disabled]){background:var(--bg-card);border-color:var(--border)}
.rk-btn[disabled]{opacity:.45;cursor:default}
/* 移动端隐藏桑基下钻抽屉:off-screen 偏移按桌面 vw 校准,min-width:340px 在窄屏
   会留一截在屏幕内遮挡 tab/内容(不可点);抽屉本身 38vw 起步,手机上不可用 */
@media(max-width:768px){#gdr-drawer{display:none!important}}
tbody tr:nth-child(even){background:var(--bg-hover)}
tbody tr:hover{background:var(--bg-hover)}
td.num,th.num{text-align:right;font-family:ui-monospace,Consolas,monospace;font-size:12.8px}
.pos{color:var(--pos);font-weight:600}.neg{color:var(--neg);font-weight:600}
.prov{color:var(--text-muted);font-size:8px;background:var(--bg-hover);border-radius:4px;padding:0 3px;white-space:nowrap;vertical-align:0.5em}
li.viewline,p.viewline{border-left:3px solid #d97706;background:var(--bg-hover);border-radius:0 6px 6px 0;padding:6px 10px;list-style:none;margin-left:-28px;font-size:12px}
.srcblock{margin-top:14px;padding:10px 14px;background:var(--bg-hover);border-radius:8px;font-size:12.5px;color:var(--text-muted)}
.srcblock li,.srcblock p{font-size:8px;line-height:1.9}
.srcblock summary{font-size:12.5px}
.srcblock summary{cursor:pointer;font-weight:600;color:var(--text-secondary)}
.ftwrap{overflow-x:auto;margin:6px 0}
.fttbl{border-collapse:collapse;width:100%;font-size:13.5px}
.fttbl th{background:var(--bg-table-header);color:#fff;font-weight:600;padding:8px 11px;text-align:center;white-space:nowrap}
.fttbl th.num{text-align:center}
.fttbl td{padding:6px 11px;border-bottom:1px solid var(--border-subtle);white-space:nowrap}
tr.ftgrp td{background:#e7edf5;color:#1e3a5f;font-weight:700;font-size:12.5px;letter-spacing:.06em;padding:7px 11px;border-top:2px solid #cbd7e6}
.fttbl tbody tr{background:var(--bg-card)}
.fttbl tbody tr:hover{background:var(--bg-hover)}
.dark tr.ftgrp td{background:#1c2636;color:#9db8d8}
td.ftasset b{font-weight:600}
td.ftasset .tk,td.ftasset a.tk{color:#2d5fa8;font:11.5px ui-monospace,Consolas,monospace;margin-left:7px;text-decoration:none;border-bottom:1px dotted #9db4d0}
td.num{text-align:right;font-family:ui-monospace,Consolas,monospace;font-size:12.8px}
.ftunit{color:var(--text-muted);font-size:11px}
td.na{color:#b6beca}
.pos{color:var(--pos);font-weight:600}.neg{color:var(--neg);font-weight:600}
.barcell{min-width:220px}   /* 移动端 auto 布局留条形空间;桌面 fixed 布局忽略 */
.bwrap{position:relative;height:15px;width:100%;display:block}
.bwrap .zero{position:absolute;left:50%;top:-2px;bottom:-2px;width:1px;background:#c3ccd6}
.bar{position:absolute;top:1px;bottom:1px;border-radius:2px;min-width:2px}
.bpos{background:var(--pos)}.bneg{background:var(--neg)}
.bv{position:absolute;top:50%;transform:translateY(-50%);font:600 8px ui-monospace,Consolas,monospace;white-space:nowrap;background:var(--bg-card);padding:0 3px;border-radius:3px;z-index:1}
.bv.na{color:var(--text-muted)}
.fttbl th.ftday{text-align:center}.ftstale{background:#fef3c7;color:#92400e;border:1px solid #fcd34d;border-radius:8px;
 padding:8px 12px;font-size:13px;line-height:1.6;margin:0 0 10px}
.dark .ftstale{background:#332a12;color:#fbbf24;border-color:#665014}
/* ── 走势列(sparkline) + 点击放大弹窗 ── */
.sparkcell{white-space:nowrap;text-align:center}
.spspark{display:inline-flex;vertical-align:middle;width:80px;height:26px}
.spspark svg{display:block;width:100%;height:100%}
.spspark.pos{color:var(--pos)} .spspark.neg{color:var(--neg)} .spspark.neutral{color:var(--neutral)}
.spna{color:var(--text-muted)}
.fttbl tbody tr[data-ri]{cursor:pointer}
#ftmodal{position:fixed;inset:0;z-index:90;display:none;align-items:center;justify-content:center;padding:20px}
#ftmodal.open{display:flex}
.ftm-mask{position:absolute;inset:0;background:rgba(0,0,0,.55)}
.ftm-card{position:relative;width:min(880px,94vw);background:var(--bg-card);border:1px solid var(--border);
 border-radius:12px;box-shadow:0 16px 48px rgba(0,0,0,.3);padding:14px 16px 10px;max-height:92vh;
 display:flex;flex-direction:column}
.ftm-head{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:8px}
.ftm-title{font-weight:700;font-size:16px;color:var(--text-primary)}
.ftm-btns{display:flex;gap:6px;flex-wrap:wrap;margin-left:auto;align-items:center}
.ftm-btn{border:1px solid var(--border);background:transparent;color:var(--text-secondary);
 border-radius:7px;padding:3px 10px;font-size:12px;cursor:pointer}
.ftm-btn:hover{background:var(--bg-hover);color:var(--text-primary)}
.ftm-btn.on{background:var(--bg-hover);color:var(--text-primary);border-color:var(--text-muted)}
.ftm-x{border:none;background:transparent;color:var(--text-muted);font-size:16px;cursor:pointer;padding:2px 6px}
.ftm-x:hover{color:var(--text-primary)}
#ftm-chart{height:360px;width:100%;min-height:0}
.ftm-foot{font-size:12px;color:var(--text-muted);padding-top:6px}
td.ftasset span.tk.nolnk{color:var(--text-muted);border-bottom:none;cursor:default}
__GDRCSS__
.gdr-stagewrap{position:relative;height:24px;margin:2px 0 10px}
.gdr-stage{position:absolute;top:0;left:calc(17px + var(--i) * (100% - 366px) / 5);transform:translateX(-50%);color:#fff;font:600 12px/1.6 inherit;padding:3px 12px;border-radius:12px;white-space:nowrap;box-shadow:0 1px 4px rgba(0,0,0,.15)}
.wide-break{width:min(96vw,1680px);position:relative;left:50%;transform:translateX(-50%)}
@media(max-width:768px){main{padding:14px 10px 40px}section.card{padding:14px}td,th{padding:5px 7px;font-size:12.5px}.md{overflow-x:auto}}
</style></head><body>
<div class="report-header">
 <div class="top">
  <h1>__TITLE__</h1>
  <div class="report-header-right"><span>__DATE__</span><button id="themeBtn" onclick="toggleTheme()">🌙</button></div>
 </div>
 <div class="tab-nav" id="tabs"></div>
</div>
<main id="panels"></main>
<script>
var TABS = window.__TABS__ || [];
var FT_TABLE = window.__FT_TABLE__ || "";
function esc(s){ return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
(function(){
 var tabsEl = document.getElementById('tabs'), panelsEl = document.getElementById('panels');
 TABS.forEach(function(t, i){
  var b = document.createElement('button');
  b.className = 'tab-btn' + (i === 0 ? ' active' : '');
  b.textContent = t.tab; b.dataset.i = i;
  b.onclick = function(){
   document.querySelectorAll('.tab-btn').forEach(function(x){x.classList.remove('active');});
   b.classList.add('active');
   document.querySelectorAll('.tab-panel').forEach(function(p, j){p.classList.toggle('active', j === i);});
   if(t.sankey){ setTimeout(function(){ window.__gdrResize && window.__gdrResize(); }, 60); }
   window.scrollTo(0, 0);
  };
  tabsEl.appendChild(b);
  var panel = document.createElement('div');
  panel.className = 'tab-panel' + (i === 0 ? ' active' : '');
  if(t.sankey){
    var cw = document.createElement('div');
    cw.className = 'wide-break';
    cw.style.padding = '0 0 10px';
    cw.innerHTML = '<div class="gdr-stagewrap">' +
      ['<span class="gdr-stage" style="--i:0;background:#f59e0b">外部数据源</span>',
       '<span class="gdr-stage" style="--i:1;background:#3b82f6">① 六部切片</span>',
       '<span class="gdr-stage" style="--i:2;background:#10b981">② 写作</span>',
       '<span class="gdr-stage" style="--i:3;background:#ef4444">③ 核验/修订</span>',
       '<span class="gdr-stage" style="--i:4;background:#0d9488">④ 交付</span>'].join('') +
      '</div>' +
      '<div id="gdr-chart" style="height:800px;background:#fff;border:1px solid var(--border-subtle);border-radius:10px;"></div>' +
      '<div style="font-size:12px;color:var(--text-muted);margin-top:8px">点击下钻:源→切片管道=配给清单 · 切片节点=输入JSON · 栏目/管道=产出markdown+Prompt · report.md=整份报告。阶段色块与下方桑基图各列对齐。抽屉左缘可拖宽,Esc/点空白关闭。</div>';
    panel.appendChild(cw);
  }
  t.secs.forEach(function(s){
   var sec = document.createElement('section');
   sec.className = 'card';
   var h = document.createElement('h2'); h.textContent = s.h; sec.appendChild(h);
   var body = document.createElement('div'); body.className = 'md';
   body.innerHTML = (s.h === '大盘总览' && window.FT_TABLE) ? window.FT_TABLE : s.html;
   sec.appendChild(body);
   panel.appendChild(sec);
  });
  panelsEl.appendChild(panel);
 });
 decorate(document);
})();
// ── FT 表走势:点击行 → ECharts 交互大图(1M/3M/6M/1Y + MA;数据随 ft_sections.json 冻结) ──
(function(){
 var H = window.__FT_HIST__ || [];
 if(!H.length) return;   // echarts 在 </body> 前才注入,此处不判;首次点击时必已就绪
 var modal=null, chart=null, cur=-1, rng='3m', mas={5:true,10:true,30:true};
 var RB={'1m':21,'3m':63,'6m':126,'1y':260};
 function ensureModal(){
  if(modal) return;
  modal=document.createElement('div'); modal.id='ftmodal';
  modal.innerHTML='<div class="ftm-mask"></div><div class="ftm-card">'
   +'<div class="ftm-head"><span class="ftm-title" id="ftm-title"></span>'
   +'<span class="ftm-btns">'
   +'<button class="ftm-btn" data-r="1m">1M</button>'
   +'<button class="ftm-btn on" data-r="3m">3M</button>'
   +'<button class="ftm-btn" data-r="6m">6M</button>'
   +'<button class="ftm-btn" data-r="1y">1Y</button>'
   +'<span style="width:8px"></span>'
   +'<button class="ftm-btn on" data-m="5" style="color:#f59e0b">MA5</button>'
   +'<button class="ftm-btn on" data-m="10" style="color:#3b82f6">MA10</button>'
   +'<button class="ftm-btn on" data-m="30" style="color:#a855f7">MA30</button>'
   +'<button class="ftm-x" title="关闭 (Esc)">✕</button></span></div>'
   +'<div id="ftm-chart"></div><div class="ftm-foot" id="ftm-foot"></div></div>';
  document.body.appendChild(modal);
  modal.querySelector('.ftm-mask').onclick=closeM;
  modal.querySelector('.ftm-x').onclick=closeM;
  modal.querySelectorAll('.ftm-btn[data-r]').forEach(function(b){b.onclick=function(){
   rng=b.getAttribute('data-r');
   modal.querySelectorAll('.ftm-btn[data-r]').forEach(function(x){x.classList.toggle('on',x===b);});
   draw();};});
  modal.querySelectorAll('.ftm-btn[data-m]').forEach(function(b){b.onclick=function(){
   var m=+b.getAttribute('data-m'); mas[m]=!mas[m]; b.classList.toggle('on'); draw();};});
 }
 function maCalc(vals,n){
  var o=[]; for(var i=0;i<vals.length;i++){
   if(i<n-1){o.push(null);continue;}
   var s=0; for(var j=i-n+1;j<=i;j++) s+=vals[j]; o.push(s/n);}
  return o;
 }
 function draw(){
  if(typeof echarts === 'undefined') return;
  if(chart){chart.dispose();chart=null;}
  var d=H[cur]; var all=d.h||[]; var take=RB[rng]||63; var pts=all.slice(-take);
  var ds=pts.map(function(p){return p[0];});
  var isK=pts.length && pts[0].length===5;          // [iso,o,h,l,c] → 蜡烛;[iso,c] → 折线
  var vs=pts.map(function(p){return isK?p[4]:p[1];}); // 收盘序列(MA 与方向色基准)
  var dark=document.body.classList.contains('dark');
  var upC=dark?'#00e676':'#0a7c42', dnC=dark?'#ff5252':'#c41230';  // DMR 同款涨跌色
  chart=echarts.init(document.getElementById('ftm-chart'));
  var series;
  if(isK){
   series=[{name:d.n,type:'candlestick',
    data:pts.map(function(p){return [p[1],p[4],p[3],p[2]];}),   // ECharts: [open,close,low,high]
    itemStyle:{color:upC,color0:dnC,borderColor:upC,borderColor0:dnC}}];
  }else{
   var up=vs[vs.length-1]>=vs[0]; var lc=up?upC:dnC;
   series=[{name:d.n,type:'line',data:vs,showSymbol:false,
    lineStyle:{width:1.8,color:lc},itemStyle:{color:lc},
    areaStyle:{color:{type:'linear',x:0,y:0,x2:0,y2:1,
     colorStops:[{offset:0,color:lc+'33'},{offset:1,color:lc+'05'}]}}}];
  }
  [[5,'#f59e0b'],[10,'#3b82f6'],[30,'#a855f7']].forEach(function(m){
   if(mas[m[0]]) series.push({name:'MA'+m[0],type:'line',data:maCalc(vs,m[0]),
    showSymbol:false,lineStyle:{width:1,color:m[1]},itemStyle:{color:m[1]}});});
  chart.setOption({
   animation:false,grid:{left:58,right:14,top:14,bottom:26},
   tooltip:{trigger:'axis',backgroundColor:'rgba(15,23,42,.92)',
    borderColor:'rgba(255,255,255,.15)',textStyle:{color:'#e2e8f0',fontSize:12},
    formatter:function(ps){
     var h=ps[0].axisValue;
     ps.forEach(function(p){
      if(p.seriesType==='candlestick'){
       var v=p.data;
       h+='<br/>'+p.marker+'开 '+v[1]+' · 收 '+v[2]+'<br/>低 '+v[3]+' · 高 '+v[4];
      }else{
       h+='<br/>'+p.marker+p.seriesName+' '+p.data;
      }});
     return h;}},
   xAxis:{type:'category',data:ds,axisLabel:{color:'#7d8a99',fontSize:10,hideOverlap:true},
    axisLine:{lineStyle:{color:'#475569'}},axisTick:{show:false}},
   yAxis:{type:'value',scale:true,axisLabel:{color:'#7d8a99',fontSize:10},
    splitLine:{lineStyle:{color:'rgba(125,138,153,.18)'}}},
   series:series});
  var foot='数据截至 '+(d.a||(pts.length?pts[pts.length-1][0]:'—'))+' · 共 '+pts.length+' 个交易日'
   +(isK?' · 日K':' · 收盘折线(该资产无OHLC)');
  if(d.u==='bp') foot+=' · 收盘列为收益率 %(bp 变动见1日涨跌幅)';
  document.getElementById('ftm-foot').textContent=foot;
 }
 function openM(i){
  cur=i; ensureModal(); var d=H[i];
  document.getElementById('ftm-title').textContent=d.n;
  modal.classList.add('open');
  requestAnimationFrame(draw);
 }
 function closeM(){
  if(!modal) return;
  modal.classList.remove('open');
  if(chart){chart.dispose();chart=null;}
 }
 document.addEventListener('click',function(ev){
  var tr=ev.target.closest ? ev.target.closest('.fttbl tbody tr[data-ri]') : null;
  if(tr && !ev.target.closest('a') && tr.getAttribute('data-ri')!==null) openM(+tr.getAttribute('data-ri'));
 });
 document.addEventListener('keydown',function(ev){ if(ev.key==='Escape') closeM(); });
})();
function decorate(root){
 document.querySelectorAll('.md a, #gdr-md a').forEach(function(a){
   a.target = '_blank'; a.rel = 'noopener';
 });
 // 涨跌数字着色(文本节点)
 var walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, null);
 var jobs = [];
 while(walker.nextNode()){
  var t = walker.currentNode;
  var pe = t.parentElement;
  if(pe && (pe.tagName === 'SCRIPT' || pe.tagName === 'STYLE')) continue;
  if(/[-+][0-9]+([.][0-9]+)?%/.test(t.nodeValue)) jobs.push(t);
 }
 jobs.forEach(function(t){
  var span = document.createElement('span');
  span.innerHTML = t.nodeValue.replace(/([-+][0-9]+([.][0-9]+)?%)/g, function(m){
   return '<span class="' + (m.charAt(0) === '+' ? 'pos' : 'neg') + '">' + m + '</span>';
  });
  t.parentNode.replaceChild(span, t);
 });
 // 溯源标记徽章:可点/悬停(数据在 window.__PROV__)
 var provRe = new RegExp('〔[^〔〕]*〕', 'g');
 document.querySelectorAll('.md li, .md p').forEach(function(el){
  el.innerHTML = el.innerHTML.replace(provRe, function(m){
   var inner = m.slice(1, -1);
   var ids = (inner.match(/[fwcdrg][0-9]+/g) || []).filter(function(id){
    return window.__PROV__ && __PROV__.items[id]; });
   var show = '';
   if(!ids.length){
    if(/the close/i.test(inner)) show = 'the_close';
    else if(/overtime/i.test(inner)) show = 'cnbc_ot';
    else if(/closing bell/i.test(inner)) show = 'cnbc';
   }
   var cls = 'prov' + ((ids.length || show) ? ' linked' : '');
   var d = ids.length ? 'data-ids="' + ids.join(',') + '"' : (show ? 'data-show="' + show + '"' : '');
   return '<span class="' + cls + '" ' + d + '>' + m + '</span>';
  });
 });
 // 观点/推断行
 document.querySelectorAll('.md li, .md p').forEach(function(el){
  if(/^(观点|推断)[：:]/.test(el.textContent.trim())) el.classList.add('viewline');
 });
 // 本节来源卡片移除(溯源能力由内联标签承担)
 document.querySelectorAll('.md p').forEach(function(el){
  if(el.textContent.trim() === '本节来源' || (el.querySelector('strong') && el.querySelector('strong').textContent === '本节来源')){
   var nxt = el.nextElementSibling, rm = [el];
   while(nxt && (nxt.tagName === 'UL' || nxt.tagName === 'OL')){
    rm.push(nxt); nxt = nxt.nextElementSibling;
   }
   rm.forEach(function(x){ x.remove(); });
  }
 });
 // 表格数字列右对齐(FT 收盘表自带对齐,排除)。
 // 数值判定:数字/符号起头且不含 ASCII 字母 —— 9988.HK/300308.SZ 这类
 // 数字起头的证券代码不含此排除会被误判成数值列,代码列右移,与美股表对不齐。
 document.querySelectorAll('.md table:not(.fttbl)').forEach(function(tb){
  var rows = tb.querySelectorAll('tr');
  if(!rows.length) return;
  var n = rows[0].children.length;
  for(var c = 0; c < n; c++){
   var numish = true;
   for(var r = 1; r < rows.length && r <= 6; r++){
    var cell = rows[r].children[c]; if(!cell) continue;
    var v = cell.textContent.trim();
    var okNum = (/^[-+]?[0-9]/.test(v) || /^[+$]/.test(v)) && !/[A-Za-z]/.test(v);
    if(v && !okNum){ numish = false; break; }
   }
   if(numish){ rows[0].children[c].classList.add('num');
    for(var r2 = 1; r2 < rows.length; r2++){ if(rows[r2].children[c]) rows[r2].children[c].classList.add('num'); } }
  }
 });
 // 今日涨跌榜:美股/港股/A股三表共用统一列宽,纵向共线(auto 布局列宽随
 // 内容浮动,列起点参差)。仅桌面端 fixed;移动端维持 auto+横向滚动现状。
 document.querySelectorAll('section.card').forEach(function(sec){
  var h2 = sec.querySelector('h2');
  if(!h2 || h2.textContent.trim() !== '今日涨跌榜') return;
  sec.querySelectorAll('table').forEach(function(tb){
   if(!tb.rows.length || tb.rows[0].children.length !== 5) return;
   tb.classList.add('ranktbl');
   var ws = ['12%','38%','13%','13%','24%'];   // 代码/公司/收盘价/涨跌幅/成交额
   var cg = document.createElement('colgroup');
   ws.forEach(function(w){ var col = document.createElement('col'); col.style.width = w; cg.appendChild(col); });
   tb.insertBefore(cg, tb.firstChild);
  });
 });
 // ── 今日涨跌榜:交互榜单(window.__RANK__ 为快照冻结全量;每页 20,─────
 //    点击「涨跌幅/成交额」列头排序,空值恒垫底;无 payload 的市场保留静态表)
 if(window.__RANK__){
  var RK_PS = 20;
  document.querySelectorAll('section.card').forEach(function(sec){
   var h2 = sec.querySelector('h2');
   if(!h2 || h2.textContent.trim() !== '今日涨跌榜') return;
   var body = sec.querySelector('.md');
   var intro = body.querySelector('p');
   if(intro && intro.textContent.indexOf('各取前 8') >= 0)
    intro.textContent = '美股 ≥10 亿美元、港股 ≥10 亿港元、A股 ≥20 亿元人民币,按当日成交额降序;点击「涨跌幅 / 成交额」列头可重排,分页展示。';
   body.querySelectorAll('h3').forEach(function(h3){
    var mkt = null;
    for(var k in window.__RANK__){ if(window.__RANK__[k].label === h3.textContent.trim()){ mkt = k; break; } }
    if(!mkt) return;
    var tbl = h3.nextElementSibling;
    if(!tbl || tbl.tagName !== 'TABLE') return;
    var D = window.__RANK__[mkt];
    var st = {key: 't', dir: -1, page: 0};   // 默认成交额降序(即冻结序)
    var wrap = document.createElement('div'); wrap.className = 'rankint';
    tbl.replaceWith(wrap);
    function sortedRows(){
     return D.rows.slice().sort(function(a, b){
      var x = a[st.key], y = b[st.key];
      if(x == null) return 1; if(y == null) return -1;   // 空值恒垫底
      return (x - y) * st.dir;
     });
    }
    function render(){
     var rs = sortedRows();
     var pages = Math.max(1, Math.ceil(rs.length / RK_PS));
     if(st.page >= pages) st.page = 0;
     var pg = rs.slice(st.page * RK_PS, (st.page + 1) * RK_PS);
     var arr = function(k){ return st.key === k ? (st.dir < 0 ? ' ▼' : ' ▲') : ''; };
     var h = '<table class="ranktbl"><colgroup>'
       + '<col style="width:12%"><col style="width:38%"><col style="width:13%"><col style="width:13%"><col style="width:24%"></colgroup>'
       + '<thead><tr><th>代码</th><th>公司</th><th class="num">收盘价</th>'
       + '<th class="num rk-sort" data-k="g">涨跌幅' + arr('g') + '</th>'
       + '<th class="num rk-sort" data-k="t">成交额' + arr('t') + '</th></tr></thead><tbody>';
     pg.forEach(function(r){
      var pc = r.g == null ? '' : (r.g > 0 ? ' pos' : (r.g < 0 ? ' neg' : ''));
      h += '<tr><td>' + esc(r.c) + '</td><td>' + esc(r.n) + '</td>'
        + '<td class="num">' + (r.p == null ? '—' : Number(r.p).toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})) + '</td>'
        + '<td class="num' + pc + '">' + (r.g == null ? '—' : (r.g > 0 ? '+' : '') + r.g.toFixed(2) + '%') + '</td>'
        + '<td class="num">' + (r.t == null ? '—' : (r.t / 1e8).toFixed(1) + D.unit) + '</td></tr>';
     });
     h += '</tbody></table>'
        + '<div class="rankbar"><button class="rk-btn" data-a="p"' + (st.page === 0 ? ' disabled' : '') + '>‹ 上一页</button>'
        + '<span class="rk-info">第 ' + (st.page + 1) + ' / ' + pages + ' 页 · 共 ' + rs.length + ' 只</span>'
        + '<button class="rk-btn" data-a="n"' + (st.page >= pages - 1 ? ' disabled' : '') + '>下一页 ›</button></div>';
     wrap.innerHTML = h;
    }
    wrap.addEventListener('click', function(ev){
     var t = ev.target;
     if(!(t instanceof Element)) return;
     if(t.classList.contains('rk-sort')){
      var k = t.getAttribute('data-k');
      if(st.key === k){ st.dir = -st.dir; } else { st.key = k; st.dir = -1; }
      st.page = 0; render();
     } else if(t.classList.contains('rk-btn') && !t.disabled){
      st.page += (t.getAttribute('data-a') === 'n' ? 1 : -1); render();
     }
    });
    render();
   });
  });
 }
}
// ── 溯源标签交互:悬停摘要 / 点击弹原文卡 ──
var provTip = document.getElementById('provtip');
var provPop = document.getElementById('provpop');
function provCardHTML(id){
  var it = __PROV__.items[id];
  var url = it.u ? '<a href="' + it.u + '" target="_blank" rel="noopener">打开原链 ↗</a>' : '<span class="nolink">无公开链接(数据类)</span>';
  return '<div class="pcard"><div class="ph"><b>' + id + '</b>'
    + '<span class="pg">' + (it.l || '') + '/' + (it.e || '') + '</span>'
    + '<span class="pa">' + it.a + '</span><span class="pt">' + it.t + '</span></div>'
    + '<div class="px">' + it.x + '</div><div class="pu">' + url + '</div></div>';
}
function provShowCardHTML(key){
  var sh = __PROV__.shows[key] || {label: key, u: '', note: ''};
  var isAudio = /\\.mp3|simplecastaudio|podtrac/.test(sh.u || '');
  var u = sh.u ? '<a href="' + sh.u + '" target="_blank" rel="noopener">' + (isAudio ? '收听单集音频 ↗' : '观看节目视频 ↗') + '</a>'
               : '<span class="nolink">无公开节目链接</span>';
  return '<div class="pcard"><div class="ph"><span class="pa">' + sh.label + '</span><span class="pt">节目转录</span></div>'
    + (sh.note ? '<div class="px">' + sh.note + '</div>' : '')
    + '<div class="pu">' + u + '</div></div>';
}
document.addEventListener('mouseover', function(e){
  var t = e.target.closest ? e.target.closest('.prov') : null;
  if(!t || !t.classList.contains('linked') || !window.__PROV__){ if(provTip) provTip.style.display='none'; return; }
  provTip.innerHTML = t.dataset.ids ? t.dataset.ids.split(',').map(provCardHTML).join('')
                                    : provShowCardHTML(t.dataset.show);
  provTip.style.display = 'block';
  var r = t.getBoundingClientRect();
  provTip.style.left = Math.max(8, Math.min(r.left, window.innerWidth - 470)) + 'px';
  provTip.style.top = (r.bottom + 8) + 'px';
});
document.addEventListener('mouseout', function(e){
  if(e.target.closest && e.target.closest('.prov') && provTip) provTip.style.display = 'none';
});
document.addEventListener('click', function(e){
  var t = e.target.closest ? e.target.closest('.prov.linked') : null;
  if(!t || !window.__PROV__) return;
  e.preventDefault();
  if(t.dataset.show){
   var sh = __PROV__.shows[t.dataset.show] || {};
   if(sh.u){ window.open(sh.u, '_blank'); }
   else {
    provPop.innerHTML = '<div class="phead">' + (sh.label || t.dataset.show) + '<button class="pclose">×</button></div>'
      + provShowCardHTML(t.dataset.show);
    provPop.style.display = 'block';
    provPop.querySelector('.pclose').onclick = function(){ provPop.style.display = 'none'; };
   }
   return;
  }
  var ids = t.dataset.ids.split(',');
  provPop.innerHTML = '<div class="phead">溯源 ' + ids.length + ' 条<button class="pclose">×</button></div>'
    + ids.map(provCardHTML).join('');
  provPop.style.display = 'block';
  provPop.querySelector('.pclose').onclick = function(){ provPop.style.display = 'none'; };
});
document.addEventListener('click', function(e){
  if(provPop && provPop.style.display === 'block' && !provPop.contains(e.target)
     && !(e.target.closest && e.target.closest('.prov'))){
    provPop.style.display = 'none';
  }
});
document.addEventListener('keydown', function(e){
  if(e.key === 'Escape' && provPop) provPop.style.display = 'none';
});

function toggleTheme(){
 document.body.classList.toggle('dark');
 document.getElementById('themeBtn').textContent = document.body.classList.contains('dark') ? '☀️' : '🌙';
}
</script>__GDRDOM__
<script>window.__SANKEY__ = @@SANKEY@@;</script>
<script>__ECHARTS__</script>
<script>__GDRJS__</script>
</body></html>"""
# ── 今日涨跌榜交互数据:快照冻结的 volume_leaders 全量(md 文档版只取前 8)──
VL_MARKETS = (('us', '美股', '亿美元'), ('hk', '港股', '亿港元'), ('cn', 'A股', '亿元'))

def _vl_disp_code(r):
    """EODHD 内部后缀 → 展示码(与 run_pipeline._vl_code 同口径)。"""
    code, ex = str(r.get('code', '')), r.get('exchange')
    if ex == 'US':
        return code
    if ex == 'HK':
        return f"{code.zfill(4)}.HK"
    if ex == 'SHG':
        return f"{code}.SH"
    if ex == 'SHE':
        return f"{code}.SZ"
    return code

RANK = {}
_vl = (((_snap.get('sources') or {}).get('market') or {}).get('volume_leaders')) or {}
for _k, _label, _unit in VL_MARKETS:
    _rows = ((_vl.get(_k) or {}).get('rows')) or []
    if not _rows:
        continue
    RANK[_k] = {'label': _label, 'unit': _unit, 'rows': [
        {'c': _vl_disp_code(r), 'n': r.get('name_cn') or r.get('name') or '',
         'p': r.get('price'), 'g': r.get('change_percent'), 't': r.get('turnover')}
        for r in _rows]}
RANK_JSON = json.dumps(RANK, ensure_ascii=False)

payload_scripts = ''.join(
    '<script>window.__TABS__=(window.__TABS__||[]);window.__TABS__.push(' + json.dumps(t, ensure_ascii=False) + ');</script>' + chr(10)
    for t in tab_payload)
payload_scripts += '<script>window.__FT_TABLE__=' + json.dumps(FT_TABLE, ensure_ascii=False) + ';</script>' + chr(10)
payload_scripts += '<script>window.__FT_HIST__=' + json.dumps(FT_HIST, ensure_ascii=False) + ';</script>' + chr(10)
payload_scripts += '<div id="provtip"></div><div id="provpop"></div>' + chr(10)
payload_scripts += '<script>window.__PROV__=' + PROV_JS + ';</script>' + chr(10)
payload_scripts += '<script>window.__RANK__=' + RANK_JSON + ';</script>' + chr(10)
html = html.replace('<script>' + chr(10) + 'var TABS = window.__TABS__ || [];',
                    payload_scripts + '<script>' + chr(10) + 'var TABS = window.__TABS__ || [];', 1)
html = (html.replace('__TITLE__', title.replace('<', '&lt;'))
            .replace('__DATE__', date or WS.name)
            
            
            .replace('__GDRCSS__', _gdr_css)
            .replace('__GDRDOM__', _gdr_dom)
            .replace('@@SANKEY@@', SANKEY_JSON)
            .replace('__ECHARTS__', ECH)
            .replace('__GDRJS__', _gdr_js))
# 主 CSS 保留在 <head> 内联(渲染阻塞,首帧即带样式)。此前把首个 <style> 挪进
# text/plain 再等 DOMContentLoaded 注入,解析期间整页无样式 —— 5MB 文档会先闪
# 一帧未渲染内容(FOUC)。此处只保留桑基图的 load 后 resize。
html = html.replace('</body></html>',
    '<script>window.addEventListener("DOMContentLoaded", function(){'
    + 'setTimeout(function(){window.__gdrResize && window.__gdrResize();}, 50);'
    + '});</script></body></html>', 1)
out = WS / 'final' / 'report.html'
out.write_text(html, encoding='utf-8')
print(f"wrote {out} ({len(html)/1024:.0f} KB), tabs={[t['tab'] for t in tab_payload]}, 各tab栏目数={[len(t['secs']) for t in tab_payload]}")
