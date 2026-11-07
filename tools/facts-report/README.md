# facts-report — 事实/观点分离版晨报

GuanHe-Daily-Report 的独立变体,用来落地郑翔需求文档「痛点 2:事实与观点混杂」
(见同目录 `郑翔需求文档.md`)。编排、核验、溯源(L/E 标记)与日报完全相同,
只改内容规则,且**不新增章节**——避免在日报已有内容上再叠一层重复。

日报目录 `tools/GuanHe-Daily-Report/` 与产物 `backend/reports/guanhe-daily-report/`
不受影响;两条管线代码独立、锁独立、产物目录独立。

## 与日报的差别

| 位置 | 日报 | facts-report |
|---|---|---|
| 01 节 | 「今日叙事」:2–3 句结论,无数字 | 「今日要闻」:3–5 条纯事实(谁、做了什么、关键数字),无观点/因果/渲染词 |
| 报告标题 | 两句编辑化结论 | 两句事实,句间不带因果 |
| 02 / 04 / 08 / 09 节 | 嘉宾、分析师观点可夹在事实句里 | 事实行只写事实;具名观点统一收进块末一行 `**观点：**` |
| 核心驱动 vs 今日要闻 | — | 驱动块可回指要闻事实,但不得原句原数字复述 |
| 03 持仓、06 个股深挖、11 展望 | — | 不变(本身就是解读层 / 已有分析师专栏) |

事实与观点的界定写在 `prompts.py` 公共规则第 12 条:当事方对自身的表态(央行谈政策、
公司谈自己的指引)算事实;分析师、策略师、嘉宾、KOL、研报、目标价、评级、情景概率,
以及对大盘涨跌「为什么」的解释(除非原因本身是当日事件)算观点。

## 核验

`verify_checks.py` 新增确定性检查:

- 「今日要闻」出现观点/预测/因果/渲染措辞(认为、有望、引发、导致、意味着、暴跌、
  恐慌…)→ **high**,触发修订轮。
- 02 / 04 / 08 / 09 节的事实行(不含 `**观点：**` / `**推断：**` 行)出现观点措辞
  (认为、看好、目标价、归因、源于…)→ medium,进复核备注,不门控。
  「预计」不在词表里:公司自己的指引属于事实。

LLM 轻量核验另加两条 medium:事实行里夹带具名观点;要闻条目在后文被原样复述。

## 运行

快照默认**借用日报同日快照**(复制 `snapshot.json` + `transcripts/`),两份报告数字
同源,也不重复调外部 API;日报当日没有快照时才需要自建。

```bash
cd tools/facts-report
$PY run_pipeline.py 2026-11-05 --style cn       # 自动从 guanhe-daily-report/2026-11-05 借快照
$PY run_pipeline.py 2026-11-05 --no-seed        # 不借;需先自建快照
$PY build_snapshot.py --date 2026-11-05         # 自建 → backend/reports/facts-report/2026-11-05/
$PY run_pipeline.py 2026-11-05 --fake-zcode     # 不调 LLM,冒烟测试编排与门控
```

其余参数与日报 `run_pipeline.py` 相同(`--no-plan`、`--no-provenance`、`--llm-verify`…)。

## 定时

`cron_daily.sh` 排在日报之后,优先借用日报 6 小时内写出的最新快照,否则自建:

```
20 10 * * 2-6 /bin/bash /opt/guanhetech/tools/facts-report/cron_daily.sh \
    >> /opt/guanhetech/backend/reports/facts-report/cron.log 2>&1
```

## 同步日报的改动

本目录是 2026-11-07 从 GuanHe-Daily-Report 复制的。日报后续的修复如需带过来,
对比时注意本目录有意改动的位置:`prompts.py`(规则 12、01 节任务、02/04/06/08/09
节措辞、轻量核验)、`verify_checks.py`(01 节标题、`_fact_line_views`)、
`run_pipeline.py`(产物目录、`seed_snapshot`、标题)、`build_snapshot.py` 与
`gen_pipeline_card.py`(产物目录)、`cron_daily.sh`。
