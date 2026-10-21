#!/usr/bin/env python3
"""
EDGAR 财报资料(Slides/Report)获取可行性探测脚本

验证内容:
1. ticker -> CIK 映射 (SEC 官方 company_tickers.json, 全量一份)
2. 公司近期 8-K / 10-Q 列表 (data.sec.gov/submissions, 免费, 需 User-Agent)
3. 8-K 的附件清单 (index.json): EX-99.1 = 业绩新闻稿(Report), EX-99.2 = 演示稿(Slides)
4. 输出可用的文档直链

用法:
    python backend/scripts/test_edgar_materials.py GPC IBM TSLA
不带参数时用默认样本: GPC IBM TSLA ALLY
限速: SEC 允许 10 请求/秒, 脚本内置 0.15s 间隔
"""
import sys
import time
import json
import requests

sys.path.insert(0, '.')
sys.path.insert(0, 'backend')

UA = {'User-Agent': 'GuanHeTerminal research probe admin@guanhetech.com'}
SEC = 'https://www.sec.gov'

TICKER_MAP_URL = f'{SEC}/files/company_tickers.json'


def get_ticker_map(session):
    r = session.get(TICKER_MAP_URL, timeout=30)
    r.raise_for_status()
    data = r.json()
    # {"0":{"cik_str":320193,"ticker":"AAPL","title":"Apple Inc."}, ...}
    return {v['ticker'].upper(): (v['cik_str'], v['title']) for v in data.values()}


def get_recent_filings(session, cik, forms, limit=40):
    """返回最近 filings 里指定 form 的 [(form, filingDate, accessionNumber, primaryDocument)]"""
    cik10 = str(cik).zfill(10)
    r = session.get(f'https://data.sec.gov/submissions/CIK{cik10}.json', timeout=30)
    r.raise_for_status()
    recent = r.json()['filings']['recent']
    out = []
    for form, fdate, acc, pdoc in zip(
        recent['form'], recent['filingDate'], recent['accessionNumber'], recent['primaryDocument']
    ):
        if form in forms:
            out.append((form, fdate, acc, pdoc))
        if len(out) >= limit:
            break
    return out


def get_filing_exhibits(session, cik, accession):
    """取某次 filing 的文档清单, 识别 EX-99.1 / EX-99.2 附件"""
    acc_nodash = accession.replace('-', '')
    url = f'{SEC}/Archives/edgar/data/{cik}/{acc_nodash}/index.json'
    r = session.get(url, timeout=30)
    r.raise_for_status()
    items = r.json()['directory']['item']
    exhibits = []
    for it in items:
        name = it['name'].lower()
        # 附件文档形如 ex991xxx.htm / ex99-1.htm / pressrelease.htm; 按文件名启发式识别
        if name.endswith(('.htm', '.html', '.pdf')):
            exhibits.append((it['name'], f"{SEC}/Archives/edgar/data/{cik}/{acc_nodash}/{it['name']}"))
    return exhibits


def guess_kind(filename: str) -> str:
    n = filename.lower().replace('_', '').replace('-', '').replace('.', '')
    # 附件命名惯例: ex991/exhibit991(新闻稿), ex992/exhibit992(演示稿), 部分公司用 earnq3 之类命名
    if 'ex992' in n or 'exhibit992' in n:
        return 'Slides(Ex-99.2)?'
    if 'ex991' in n or 'exhibit991' in n:
        return 'Report/PressRelease(Ex-99.1)?'
    if 'slide' in n or 'present' in n:
        return 'Slides?'
    if 'press' in n or 'release' in n or 'results' in n:
        return 'Report/PressRelease?'
    if 'earn' in n and 'q' in n:
        return 'Report/PressRelease?'
    return ''


def probe(session, ticker_map, ticker):
    print(f"\n===== {ticker} =====")
    if ticker not in ticker_map:
        print(f"  ✗ ticker 不在 SEC 映射表")
        return
    cik, title = ticker_map[ticker]
    print(f"  CIK {cik}  {title}")

    filings = get_recent_filings(session, cik, {'8-K', '10-Q'})
    if not filings:
        print("  ✗ 未取到 filings")
        return

    # 最近一次 10-Q
    tenq = next((f for f in filings if f[0] == '10-Q'), None)
    if tenq:
        form, fdate, acc, pdoc = tenq
        url = f"{SEC}/Archives/edgar/data/{cik}/{acc.replace('-', '')}/{pdoc}"
        print(f"  [10-Q] {fdate}  {url[:110]}")
    else:
        print("  [10-Q] 近期无")

    # 最近一次 8-K, 解析附件
    eightk = next((f for f in filings if f[0] == '8-K'), None)
    if eightk:
        form, fdate, acc, pdoc = eightk
        print(f"  [8-K ] {fdate}  accession {acc}")
        try:
            exhibits = get_filing_exhibits(session, cik, acc)
            for name, url in exhibits:
                kind = guess_kind(name)
                if kind:
                    print(f"      {kind:<28} {name}  {url[:110]}")
            if not any(guess_kind(n) for n, _ in exhibits):
                print(f"      (附件未识别出 99.1/99.2, 共 {len(exhibits)} 个文档: {[n for n, _ in exhibits][:6]})")
        except Exception as e:
            print(f"      ✗ index.json 获取失败: {e}")
    else:
        print("  [8-K ] 近期无")


def main():
    tickers = [a.upper() for a in sys.argv[1:]] or ['GPC', 'IBM', 'TSLA', 'ALLY']
    s = requests.Session()
    s.headers.update(UA)
    print(f"获取 ticker->CIK 映射 ...")
    tmap = get_ticker_map(s)
    print(f"  ✓ 共 {len(tmap)} 家公司")
    for t in tickers:
        try:
            probe(s, tmap, t)
        except Exception as e:
            print(f"\n===== {t} =====\n  ✗ 失败: {e}")
        time.sleep(0.15)
    print("\n探测完成")


if __name__ == '__main__':
    main()
