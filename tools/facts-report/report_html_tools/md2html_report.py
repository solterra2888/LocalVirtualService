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
    ('美股全景', ['美股全景', '今日涨跌榜', '板块动态', '个股深挖']),
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
FT_TABLE = ''
ftp = Path(__file__).parent / 'ft_sections.json'
if ftp.exists():
    D = json.loads(ftp.read_text(encoding='utf-8'))
    def _cell(v, u):
        if v is None: return '<td class="num na">—</td>'
        cls = 'pos' if v > 0 else ('neg' if v < 0 else '')
        sv = f'{v:+.1f}%' if u == '%' else f'{v:+.0f}{u}'
        return f'<td class="num {cls}">{sv}</td>'
    def _bar(v, scale):
        if v is None: return ''
        w = min(abs(v) / scale, 1.0) * 50
        side = 'left:50%' if v > 0 else 'right:50%'
        cls = 'bpos' if v > 0 else 'bneg'
        return (f'<div class="bwrap"><div class="zero"></div>'
                f'<div class="bar {cls}" style="{side};width:{w:.1f}%"></div></div>')
    # 指数→finviz 指数页(内部代码→finviz slug);finviz 无对应页面的代码不展示徽标
    FINVIZ_INDEX = {'SPX': 'SPX', 'NDX': 'NDX', 'DJIA': 'DJI'}
    HIDE_TICKER = {'R2K', 'SOX', 'DXY'}          # finviz 无指数/行情页
    GRAY_TICKER = {'IH', 'IF', 'IC', 'IM'}       # 中金所期货,仅标注不外链
    def _tk_html(tk):
        if not tk or tk in HIDE_TICKER:
            return ''
        if tk in GRAY_TICKER:
            return f'<span class="tk nolnk">{tk}</span>'
        href = (f'https://elite.finviz.com/index/{FINVIZ_INDEX[tk]}?p=d' if tk in FINVIZ_INDEX
                else f'https://finviz.com/quote.ashx?t={tk}')
        return f'<a class="tk" href="{href}" target="_blank" rel="noopener">{tk} ↗</a>'
    ft_rows = ''
    for sec in D['sections']:
        ft_rows += (f'<tr class="ftgrp"><td colspan="6" title="条形刻度: {sec["scale_lbl"]}">{sec["title"]}</td></tr>')
        for r in sec['rows']:
            u = r.get('unit') or '%'
            u = '%' if u in ('%', 'pct') else u
            d1 = r.get('m1d')
            bv = '—' if d1 is None else (f'{d1:+.2f}%' if u == '%' else f'{d1:+.1f}{u}')
            bvc = 'pos' if (d1 or 0) > 0 else 'neg'
            ft_rows += (
                f'<tr><td class="ftasset"><b>{r["name"]}</b>' + _tk_html(r.get('ticker'))
                + f'</td><td class="num">{r.get("last_str") or "—"}<span class="ftunit">{"" if u in ("%", "bp") else " " + u}</span></td>'
                + f'<td class="barcell"><span class="bv {bvc}">{bv}</span>{_bar(d1, sec["scale"])}</td>'
                + _cell(r.get('m1w'), u) + _cell(r.get('m1m'), u) + _cell(r.get('ytd'), u) + '</tr>')
    FT_TABLE = ('<div class="ftwrap"><table class="fttbl">'
                '<thead><tr><th>资产</th><th class="num">收盘</th><th>当日</th>'
                '<th class="num">1周</th><th class="num">1月</th><th class="num">YTD</th></tr></thead>'
                f'<tbody>{ft_rows}</tbody></table>'
                '<div class="ftnote">条形按各组刻度绘制(悬停组标题可见);利率组单位 bp。'
                'ETF/板块行情取自最新 finviz 快照(周末冻结即周五收盘),其余按报告日取数,口径与 us_close_chart 一致。</div></div>')



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
 --bg-page:#f6f8fa; --bg-card:#ffffff; --bg-header:linear-gradient(90deg,#2d4a6f 0%,#1e3a5f 100%);
 --bg-table-header:linear-gradient(180deg,#1e3a5f 0%,#152d4a 100%); --bg-hover:#f0f4f8;
 --text-primary:#111; --text-secondary:#3d4f5f; --text-muted:#6b7a8a; --text-header:#fff;
 --text-tab:rgba(255,255,255,.65); --text-tab-active:#fff;
 --border:rgba(224,224,224,.6); --border-subtle:#e8ecf1;
 --pos:#0a7c42; --neg:#c41230; --neutral:#888;
 --shadow-card:0 2px 12px rgba(0,0,0,.06);
}
.dark{
 --bg-page:#0B0E14; --bg-card:#141820; --bg-header:linear-gradient(90deg,#1a2235 0%,#141820 100%);
 --bg-table-header:linear-gradient(180deg,#1a2235 0%,#111622 100%); --bg-hover:#1e2536;
 --text-primary:#edf0f4; --text-secondary:#b7c2cf; --text-muted:#7d8a99;
 --border:rgba(60,70,90,.5); --border-subtle:#232b3a; --shadow-card:0 2px 12px rgba(0,0,0,.4);
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg-page);color:var(--text-primary);
 font:15px/1.8 -apple-system,'PingFang SC','Microsoft YaHei',Segoe UI,sans-serif;transition:background .25s}
.report-header{background:var(--bg-header);padding:22px 26px 0;color:var(--text-header)}
.report-header .top{display:flex;justify-content:space-between;align-items:flex-start;gap:12px;flex-wrap:wrap}
.report-header h1{margin:0;font-size:24px;line-height:1.45;max-width:74em}
.report-header-right{display:flex;align-items:center;gap:12px;font-size:14px;color:var(--text-tab)}
.badge{background:rgba(255,255,255,.14);border:1px solid rgba(255,255,255,.25);border-radius:999px;padding:2px 12px;font-size:12.5px}
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
section.card>h2{margin:0 0 4px;font-size:19px;color:var(--text-primary);
 border-left:4px solid #2d4a6f;padding-left:10px}
section.card>h2::before{content:'';display:none}
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
.fttbl th{background:var(--bg-table-header);color:#fff;font-weight:600;padding:8px 11px;text-align:left;white-space:nowrap}
.fttbl th.num{text-align:right}
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
.barcell{min-width:220px}
.bwrap{position:relative;height:13px;width:58%;display:inline-block;vertical-align:-2px}
.bwrap .zero{position:absolute;left:50%;top:-2px;bottom:-2px;width:1px;background:#c3ccd6}
.bar{position:absolute;top:1px;bottom:1px;border-radius:2px}
.bpos{background:var(--pos)}.bneg{background:var(--neg)}
.bv{font:600 12.5px ui-monospace,Consolas,monospace;display:inline-block;width:66px;text-align:left;margin-right:10px;vertical-align:1px;white-space:nowrap}
td.ftasset span.tk.nolnk{color:var(--text-muted);border-bottom:none;cursor:default}
.ftnote{font-size:11.5px;color:var(--text-muted);margin-top:8px;line-height:1.6}
__GDRCSS__
.gdr-stagewrap{position:relative;height:24px;margin:2px 0 10px}
.gdr-stage{position:absolute;top:0;left:calc(17px + var(--i) * (100% - 366px) / 5);transform:translateX(-50%);color:#fff;font:600 12px/1.6 inherit;padding:3px 12px;border-radius:12px;white-space:nowrap;box-shadow:0 1px 4px rgba(0,0,0,.15)}
.wide-break{width:min(96vw,1680px);position:relative;left:50%;transform:translateX(-50%)}
@media(max-width:768px){main{padding:14px 10px 40px}section.card{padding:14px}td,th{padding:5px 7px;font-size:12.5px}.md{overflow-x:auto}}
</style></head><body>
<div class="report-header">
 <div class="top">
  <h1>__TITLE__</h1>
  <div class="report-header-right"><span class="badge">事实版</span><span>__DATE__</span><button id="themeBtn" onclick="toggleTheme()">🌙</button></div>
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
   if(i === 4){ setTimeout(function(){ window.__gdrResize && window.__gdrResize(); }, 60); }
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
 // 表格数字列右对齐(FT 收盘表自带对齐,排除)
 document.querySelectorAll('.md table:not(.fttbl)').forEach(function(tb){
  var rows = tb.querySelectorAll('tr');
  if(!rows.length) return;
  var n = rows[0].children.length;
  for(var c = 0; c < n; c++){
   var numish = true;
   for(var r = 1; r < rows.length && r <= 6; r++){
    var cell = rows[r].children[c]; if(!cell) continue;
    var v = cell.textContent.trim();
    if(v && !/^[-+]?[0-9]/.test(v) && !/^[+$]/.test(v)){ numish = false; break; }
   }
   if(numish){ rows[0].children[c].classList.add('num');
    for(var r2 = 1; r2 < rows.length; r2++){ if(rows[r2].children[c]) rows[r2].children[c].classList.add('num'); } }
  }
 });
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
payload_scripts = ''.join(
    '<script>window.__TABS__=(window.__TABS__||[]);window.__TABS__.push(' + json.dumps(t, ensure_ascii=False) + ');</script>' + chr(10)
    for t in tab_payload)
payload_scripts += '<script>window.__FT_TABLE__=' + json.dumps(FT_TABLE, ensure_ascii=False) + ';</script>' + chr(10)
payload_scripts += '<div id="provtip"></div><div id="provpop"></div>' + chr(10)
payload_scripts += '<script>window.__PROV__=' + PROV_JS + ';</script>' + chr(10)
html = html.replace('<script>' + chr(10) + 'var TABS = window.__TABS__ || [];',
                    payload_scripts + '<script>' + chr(10) + 'var TABS = window.__TABS__ || [];', 1)
html = (html.replace('__TITLE__', title.replace('<', '&lt;'))
            .replace('__DATE__', date or WS.name)
            
            
            .replace('__GDRCSS__', _gdr_css)
            .replace('__GDRDOM__', _gdr_dom)
            .replace('@@SANKEY@@', SANKEY_JSON)
            .replace('__ECHARTS__', ECH)
            .replace('__GDRJS__', _gdr_js))
html = html.replace(
    '<style>', '<script type="text/plain" id="pagecss">', 1).replace(
    '</style>', '</script>', 1)
html = html.replace('</body></html>',
    '<script>window.addEventListener("DOMContentLoaded", function(){'
    + 'document.head.appendChild(Object.assign(document.createElement("style"),'
    + '{textContent:document.getElementById("pagecss").textContent}));'
    + 'setTimeout(function(){window.__gdrResize && window.__gdrResize();}, 50);'
    + '});</script></body></html>', 1)
out = WS / 'final' / 'report.html'
out.write_text(html, encoding='utf-8')
print(f"wrote {out} ({len(html)/1024:.0f} KB), tabs={[t['tab'] for t in tab_payload]}, 各tab栏目数={[len(t['secs']) for t in tab_payload]}")
