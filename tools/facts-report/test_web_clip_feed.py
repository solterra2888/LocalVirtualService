# -*- coding: utf-8 -*-
"""web_clip_feed_service 清洗函数离线单测 (不连 DB, 不发 celery)。

数据: /tmp/recent_clips.json = 近三天 242 条真实剪藏 (id/url/len/content),
      /tmp/recent_meta.json  = 对应元数据 (id/ts/domain/title/len)。
运行: python -I /opt/guanhetech/tools/facts-report/test_web_clip_feed.py
"""
import json
import os
import re
import sys
from datetime import datetime

sys.path.insert(0, '/opt/guanhetech')
os.environ.setdefault('WEB_CLIP_FEED_DOMAINS', 'bloomberg.com,reuters.com')

from backend.services.web_clip_feed_service import (  # noqa: E402
    clean_clip_text, feed_domains, first_headline, match_feed_domain,
    normalize_url, parse_published_at, recover_title, strip_invisible,
)

CLIPS = json.load(open('/tmp/recent_clips.json', encoding='utf-8'))
META = {m['id']: m for m in json.load(open('/tmp/recent_meta.json', encoding='utf-8'))}

INV = dict.fromkeys(map(ord, '\u200b\u200c\u200d\u2060\ufeff\u00ad'), None)
BOILER = re.compile(
    r'opens new tab|Purchase Licensing Rights|Trust Principles|Subscribe now'
    r'|adjust your cookie settings|Misinformation Monitor newsletter|Artificial Intelligencer')

fails = []


def check(name, cond, detail=''):
    status = 'PASS' if cond else 'FAIL'
    print(f'[{status}] {name}' + (f'  {detail}' if detail and not cond else ''))
    if not cond:
        fails.append(name)


# --- 1. 不可见字符全剥 ---
inv_total = sum(len(r['content']) - len(r['content'].translate(INV)) for r in CLIPS)
cleaned_all = [clean_clip_text(r['content']) for r in CLIPS]
residual = sum(len(c) - len(c.translate(INV)) for c in cleaned_all)
check('不可见字符清零', residual == 0, f'before={inv_total} after={residual}')

# --- 2. 样板残留率下降 ---
before_pct = sum(1 for r in CLIPS if BOILER.search(r['content'])) / len(CLIPS)
after_pct = sum(1 for c in cleaned_all if BOILER.search(c)) / len(CLIPS)
print(f'      样板命中条目占比: {before_pct:.0%} → {after_pct:.0%}')
check('样板残留率显著下降', after_pct < before_pct * 0.5, f'{before_pct:.0%}->{after_pct:.0%}')

# --- 3. 正文长度保持 (清洗只应削掉样板, 不应吃掉正文) ---
loss_ratios = sorted((len(c) / max(1, len(r['content'])) for r, c in zip(CLIPS, cleaned_all)))
p10, p50 = loss_ratios[len(loss_ratios) // 10], loss_ratios[len(loss_ratios) // 2]
print(f'      清洗后/原始长度比 p10={p10:.3f} p50={p50:.3f}')
check('正文保留 p50 ≥ 0.9', p50 >= 0.9, f'p50={p50:.3f}')
check('正文保留 p10 ≥ 0.75', p10 >= 0.75, f'p10={p10:.3f}')

# --- 4. 标题回填: 残根标题走 slug 恢复, 完整标题不动 ---
full_titles = [recover_title(META[r['id']]['title'] or '', c, r['url'])
               for r, c in zip(CLIPS, cleaned_all)]
stub_fixed = {  # 已知页面级截断残根 → 期望从 slug 恢复出可读长标题
    'Cash': 'cash-strapped-indian-states',
    'Tiger': 'tiger-backed-razorpay',
    'Google': 'google-tied-prepaid-bond',
    'Unloved 30': 'unloved-30-year-bond',
    'AI': 'ai-driven-firm-corgi',
}
n_stub_ok = 0
for r, t in zip(CLIPS, full_titles):
    orig = META[r['id']]['title'] or ''
    if orig in stub_fixed:
        ok = slug_ok = slug_of = None
        # slug 恢复后的标题应比残根长得多
        if len(t) > len(orig) + 15:
            n_stub_ok += 1
        else:
            print(f'      [stub 未恢复] {orig!r} → {t[:70]!r}')
print(f'      残根标题 slug 恢复: {n_stub_ok}/{len(stub_fixed)}')
check('残根标题 slug 恢复 ≥ 4/5', n_stub_ok >= 4, f'{n_stub_ok}/{len(stub_fixed)}')
unchanged = sum(1 for r, t in zip(CLIPS, full_titles)
                if len((META[r['id']]['title'] or '')) >= 25 and t == META[r['id']]['title'])
n_long = sum(1 for r in CLIPS if len(META[r['id']]['title'] or '') >= 25)
print(f'      完整标题不被误改: {unchanged}/{n_long}')
check('完整标题不被误改 ≥ 98%', unchanged >= n_long * 0.98, f'{unchanged}/{n_long}')

# --- 5. 发布时间解析 ---
# Bloomberg 样例: 带 "at 3:57 PM GMT+8" 行
bb = next(r for r in CLIPS if 'bloomberg' in r['url'] and 'US Stock Futures Fall' in r['content'])
pt = parse_published_at(clean_clip_text(bb['content']))
check('Bloomberg GMT+8 行解析为 UTC', pt == datetime(2026, 11, 20, 7, 57), str(pt))
# Reuters 样例: 只有 "November 20, 2026 · Reuters" 日期行 → 12:00 ET 保守锚点
rt = next(r for r in CLIPS if 'reuters' in r['url'] and "Nvidia's share slide" in r['content'])
pt2 = parse_published_at(clean_clip_text(rt['content']))
check('Reuters 日期行解析 (12:00 ET 锚点)', pt2 == datetime(2026, 11, 20, 17, 0), str(pt2))
# 扩展直传 ISO 优先
pt3 = parse_published_at('whatever\nNovember 20, 2026 · X', '2026-11-20T14:30:00+08:00')
check('扩展 ISO 时间优先', pt3 == datetime(2026, 11, 20, 6, 30), str(pt3))
# 全体: 解析结果都落在近三天窗口 (防止乱解析到 1900 年之类)
ok_dates = 0
for r, c in zip(CLIPS, cleaned_all):
    try:
        p = parse_published_at(c)
        if datetime(2026, 11, 17) <= p <= datetime(2026, 11, 22):
            ok_dates += 1
    except Exception:
        pass
print(f'      发布时间解析落窗: {ok_dates}/{len(CLIPS)}')
check('发布时间 100% 解析成功且落窗', ok_dates == len(CLIPS), f'{ok_dates}/{len(CLIPS)}')

# --- 6. URL 规范化 + 域名白名单 ---
check('白名单命中 bloomberg', match_feed_domain('https://www.bloomberg.com/news/articles/x') == 'bloomberg.com')
check('白名单命中 reuters 子域', match_feed_domain('https://uk.reuters.com/world/a-2026-11-20/') == 'reuters.com')
check('白名单拒绝其它域', match_feed_domain('https://finance.yahoo.com/x') is None)
check('URL 去 query', normalize_url('https://www.reuters.com/a-2026-11-20/?utm=xx#frag') ==
      'https://www.reuters.com/a-2026-11-20/')
long_url = 'https://www.bloomberg.com/news/articles/' + 'x' * 300
check('超长 URL 哈希回退 ≤255', len(normalize_url(long_url)) <= 255)

# --- 7. 端到端 dry (不落库): 白名单过滤 ---
n_eligible = sum(1 for r in CLIPS if match_feed_domain(r['url']))
print(f'      白名单可转投: {n_eligible}/{len(CLIPS)}')
check('全部 242 条都在白名单内 (本批全是 B/R)', n_eligible == len(CLIPS))

print()
print('FAILED:' if fails else 'ALL PASS', fails if fails else f'({len(CLIPS)} clips verified)')
sys.exit(1 if fails else 0)
