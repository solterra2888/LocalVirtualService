#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""source_quality 离线单测:不连库、不碰真实工作区。样例取自 2026-11 审计的真实条目。

跑法: python -I tools/facts-report/test_source_quality.py
"""

import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import source_quality as sq  # noqa: E402

FAILURES = []
T = date(2026, 11, 20)


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"[{status}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAILURES.append(name)


def et(hh, mm, d=T):
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=sq.ET).astimezone(timezone.utc)


# ── 1. 层级与原始出处 ──────────────────────────────────────────────────
check("B/R news clip is tier 1", sq.source_tier("Bloomberg", "news") == 1
      and sq.source_tier("Reuters", "news") == 1)
check("Reuters youtube is tier 2", sq.source_tier("Reuters", "youtube") == 2)
check("*Walter Bloomberg is a relay (tier 3)", sq.source_tier("*Walter Bloomberg", "twitter") == 3)
check("Yahoo is tier 4", sq.source_tier("Yahoo Finance", "news") == 4)
check("origin RTRS", sq.credited_origin(
    "SEC PROBE INTO NVIDIA IS EXPLORING POTENTIAL ACCOUNTING ISSUES, ACCORDING TO SOURCES: RTRS")
    == "Reuters")
check("origin FT suffix", sq.credited_origin(
    "US DEPARTMENT DELAYS MEGASPEED’S INCLUSION ON TRADE BLACKLIST - FT") == "FT")
check("origin earliest wins", sq.credited_origin(
    "$NVDA - *NVIDIA PREPARING TO OUST CEO: WSJ (via Reuters later)") == "WSJ")
check("no origin for Bloomberg index mention",
      sq.credited_origin("BLOOMBERG DOLLAR INDEX RISES 0.3%") is None)
check("relay with origin → tier 2", sq.effective_tier(3, "Reuters") == 2)
check("syndicated Reuters on Yahoo", sq.syndicated_origin(
    "NEW YORK, Nov 19 (Reuters) - Global stocks fell on Thursday") == "Reuters")

# ── 2. 剪藏正文清洗 ───────────────────────────────────────────────────
raw = ("K\nSource: https://www.bloomberg.com/news/articles/2026-11-19/k-pop\nK\n"
       "November 19, 2026 · Sohee Kim · Bloomberg\nNovember 19, 2026 at 10:02 AM GMT+8\n"
       "Takeaways  by Bloomberg AI\nSouth Korean entertainment companies are positioning.")
check("header stripped when page title is truncated",
      sq.clean_clip_body(raw, "K Pop Giants Position for China Return").startswith("South Korean"))
raw_r = ("Trading Day: World's biggest IPO? Pfft\nSource: https://www.reuters.com/x\n"
         "Trading Day: World's biggest IPO? Pfft\nNovember 20, 2026 · Jamie McGeever · Reuters\n"
         "Item 1 of 3 CEO of Anthropic speaks. REUTERS/Brendan McDermid\n"
         "[1/3]CEO of Anthropic speaks. REUTERS/Brendan McDermid\nCompanies\n"
         "ORLANDO, Florida, Nov 19 (Reuters) - US stocks slumped 2% on Thursday.\n"
         "Reporting by Jamie McGeever; Editing by Nia Williams")
body = sq.clean_clip_body(raw_r, "Trading Day: World's biggest IPO? Pfft")
check("Reuters caption/boilerplate/credit lines stripped",
      body == "ORLANDO, Florida, Nov 19 (Reuters) - US stocks slumped 2% on Thursday.", body)
check("folded caption '... Read more' stripped", sq.clean_clip_body(
    "x\nSource: https://r.com\nCEO of Anthropic and others celebrate at the NASDAQ "
    "MarketSite in New... Read more\nLONDON, Nov 20 (Reuters) - Body.", "x")
    == "LONDON, Nov 20 (Reuters) - Body.")

# ── 3. 稿件形态 / 过期复盘 ─────────────────────────────────────────────
check("SEO listicle", sq.classify_article("5 Unrivaled Stocks to Buy for the Next Decade", "") == "seo")
check("Simply Wall St ticker form", sq.classify_article(
    "Is Berner Kantonalbank (SWX:BEKN) Reasonably Priced After Its 172% Run?", "") == "seo")
check("PR wire", sq.classify_article(
    "Nu Holdings Ltd. Reports Third Quarter 2026 Financial Results",
    "SÃO PAULO, November 12, 2026--(BUSINESS WIRE)--Nu Holdings Ltd.") == "pr")
check("dividend notice is lowvalue", sq.classify_article(
    "WYNDHAM HOTELS & RESORTS DECLARES QUARTERLY CASH DIVIDEND", "") == "lowvalue")
check("AP data table slug is lowvalue", sq.classify_article("BC-Most Active Stocks", "") == "lowvalue")
check("GuruFocus is auto", sq.classify_article(
    "Salesforce Stocks Fall 9.45%", "This article first appeared on GuruFocus. Salesforce") == "auto")
check("real news stays news", sq.classify_article(
    "Fed officials expect more rate hikes, minutes show", "Federal Reserve officials") == "news")
stale = ("This article first appeared on GuruFocus. Anthropic (NASDAQ:ANT) closed its first "
         "trading day at $97.17 at 4:00 p.m. ET on November 19, up 17.07%")
check("stale recap (closed … on November 19) at T=11-20",
      sq.stale_session_date(stale, T) == date(2026, 11, 19))
check("same-day close is not stale", sq.stale_session_date(
    "SK hynix fell 7.01% at the November 20 regular-session close", T) is None)
check("year wrap: Dec 31 seen from Jan 2 is stale", sq.stale_session_date(
    "shares closed lower on December 31", date(2027, 1, 2)) == date(2026, 12, 31))

# ── 4. 相关度 ─────────────────────────────────────────────────────────
ctx = sq.build_relevance_context(
    {"sp500_constituents": [
        {"ticker": "NVDA", "company": "NVIDIA Corp", "change_percent": -14.0, "market_cap": 4e6},
        {"ticker": "AMD", "company": "Advanced Micro Devices Inc", "change_percent": -9.0,
         "market_cap": 3e5},
        {"ticker": "KO", "company": "Coca-Cola Co", "change_percent": 0.1, "market_cap": 2e5}]},
    {"today": [{"code": "INTU", "name": "Intuit Inc"}]}, ["CRWD"])
check("short name generic first word", sq.short_company_name("Advanced Micro Devices Inc")
      == "Advanced Micro")
check("entity by name", "nvidia" in ctx.entities("Nvidia's share slide knocks it below Alphabet"))
check("entity by Reuters RIC", "AMD" in ctx.entities("shares of AMD (AMD.O) fell"))
check("bare uppercase word is not a ticker hit", not ctx.entities("IT IS ALL NOW"))
check("relevant title outranks off-topic", ctx.score(
    "Nvidia Turmoil Lifts AMD, Broadcom", "") > ctx.score(
    "Candy Crush Saga Kicks Off Game On Season", "Candy Crush gives XBOX worlds a twist"))

# ── 5. flash 聚簇 ─────────────────────────────────────────────────────
def fl(i, author, text, hh, mm, lv="L1", tier=3):
    return {"id": i, "author": author, "text": text, "info_level": lv, "tier": tier,
            "tickers": [], "_ts": et(hh, mm), "_bucket": "session"}


trump = [fl("f65", "*Walter Bloomberg", "TRUMP: WILL NOT ALLOW COMPANIES TO BUILD DATA CENTERS OUTSIDE US", 12, 37),
         fl("f66", "zerohedge", "*TRUMP: WON'T ALLOW OUR COMPANIES TO BUILD DATA CENTERS ABROAD", 12, 37),
         fl("f67", "zerohedge", "*TRUMP: WILL PUT TAX ON COMPANIES BUILDING DATA CENTERS ABROAD", 12, 38),
         fl("f71", "LiveSquawk", "US President Trump: Won’t Allow Companies To Build Data Centres "
            "Outside US; If US Companies Do, Will Put Tremendous Tax On Them", 12, 39),
         fl("fx", "Investing.com", "*TRUMP: WON'T ALLOW OUR COMPANIES TO BUILD DATA CENTERS ABROAD", 15, 52)]
waller = [fl("w1", "LiveSquawk", "FED'S WALLER: SUPPORT 25BP CUT IN DECEMBER", 10, 0),
          fl("w2", "zerohedge", "FED'S WALLER: DECEMBER CUT NOT A DONE DEAL", 10, 5)]
cl = sq.cluster_flash([dict(x) for x in trump + waller])
sizes = sorted(len(c) for c in cl)
check("trump data-center reposts merge into one cluster within 45min",
      any(len(c) == 4 for c in cl), str(sizes))
check("same headline 3h later is a new cluster", any(
    len(c) == 1 and c[0]["id"] == "fx" for c in cl))
check("opposite Fed statements are not merged", not any(
    {"w1", "w2"} <= {m["id"] for m in c} for c in cl))
rep = sq.flash_representative([dict(x) for x in trump[:4]])
check("representative is the most complete text", rep["id"] == "f71" and rep["dup_n"] == 4
      and len(rep["also"]) == 2, str(rep.get("also")))

# ── 6. wire 选材 ──────────────────────────────────────────────────────
def wc(title, tier, rel, kind="news", lv="L2", hh=12, stale=False, ent=0):
    return {"title": title, "tier": tier, "_rel": rel, "_rel_entity": ent, "_kind": kind,
            "info_level": lv, "_stale": stale, "_ts": et(hh, 0),
            "_bucket": sq.time_bucket(et(hh, 0), T)}


_VOCAB = ("alpha bravo charlie delta echo foxtrot golf hotel india juliet kilo lima mike "
          "november oscar papa quebec romeo sierra tango uniform victor whiskey xray yankee "
          "zulu amber cobalt indigo scarlet").split()
cands = ([wc(f"{_VOCAB[i]} headline {_VOCAB[(i + 7) % 30]} report", 1, 5 - i % 5, hh=8 + i % 10)
          for i in range(30)]
         + [wc("Bloomberg L1 scoop on Nvidia accounting probe", 1, 0, lv="L1")]
         + [wc("5 Stocks to Buy Now", 4, 3, kind="seo"),
            wc("Company Reports Q3 Results", 4, 1, kind="pr", ent=0),
            wc("Nvidia Reports Q3 Results", 4, 4, kind="pr", ent=1),
            wc("Salesforce Stocks Fall 9.45%", 4, 4, kind="auto", ent=1, stale=True),
            wc("Candy Crush Saga Kicks Off", 4, 0),
            wc("Fed officials expect more rate hikes, minutes show", 2, 4)])
picked, stats = sq.select_wire(cands, t1_max=25, other_max=35,
                               other_max_when_t1=15, t1_floor_for_other_cut=10)
titles = {p["title"] for p in picked}
check("t1 capped at 25", stats["t1_selected"] == 25, str(stats))
check("t1 L1 always kept even with zero relevance",
      "Bloomberg L1 scoop on Nvidia accounting probe" in titles)
check("seo / entity-less pr / stale / off-topic dropped",
      not titles & {"5 Stocks to Buy Now", "Company Reports Q3 Results",
                    "Salesforce Stocks Fall 9.45%", "Candy Crush Saga Kicks Off"}, str(stats["dropped"]))
check("pr naming a mover kept", "Nvidia Reports Q3 Results" in titles)
check("tier-1 items come first", all(p["tier"] == 1 for p in picked[:25]))
check("every time bucket covered among tier 1",
      len({p["_bucket"] for p in picked if p["tier"] == 1}) == 3)

# ── 7. 计划/切片开关不默认开启 ────────────────────────────────────────
os.environ.pop("MB_EDITORIAL_V2", None)
import prompts  # noqa: E402
check("editorial v2 off by default", not prompts.editorial_v2()
      and "(newswire)" not in prompts.SECTION_SOURCE_MATRIX["sector_action"])
check("v1 plan prompt keeps the no-evidence rule",
      "no evidence lists" in prompts.build_plan_prompt(["drivers"], "digest"))
os.environ["MB_EDITORIAL_V2"] = "1"
check("v2 plan prompt asks for evidence", '"evidence"' in prompts.build_plan_prompt(
    ["drivers"], "digest"))
check("v2 writing rules carry SOURCE TIERS", "SOURCE TIERS" in prompts._common_rules("cn"))
os.environ.pop("MB_EDITORIAL_V2", None)

wire_item = {"title": "x", "text": "Body. " * 150 + "according to people familiar with the matter"}
src = {"news": {"wire": [dict(wire_item)]}}
prompts.flag_reported_items(src)
check("wire reported flag looks at the lede only", src["news"]["wire"][0]["reported"] is False)

print()
print(f"{len(FAILURES)} failure(s)" if FAILURES else "all passed")
sys.exit(1 if FAILURES else 0)
