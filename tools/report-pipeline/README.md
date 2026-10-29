# report-pipeline — CNT 风格 Daily Market Brief 生成管线(v3)

用无头 ZCode(`zcode -p`)作为"超级 LLM 系统调用",按**章节分步生成 + 核验门控**定时产出市场简报。v3 核心是**严格的"章节 × 信源"映射**(对照 CNT 对应关系验证结论):

| 报告部分 | 章节 | 唯一叙事/数据源 |
|---|---|---|
| OVERALL MARKET | narrative / drivers / positioning | **The Close 摘要**(数字以节目口径为准,缺失标「不可得」) |
| EQUITY MARKET | sector_action / movers / deep_dives | **Closing Bell 摘要 + Finviz/板块结构化数据**(数字锚) |
| MACROS | macro | **经济日历 + macro_pillars**(曲线/FRED/自算概率/拍卖/Fed日历,不含节目) |

映射由 prompt 的 SOURCE RESTRICTION 约束、由核验器按 `SECTION_SOURCE_MATRIX` 执法
(越界归因 = high)。v2 曾允许每节引用全部信源,导致跨节内容重复与任务书臃肿,v3 收严。

## 全流程图

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 第一层 · 上游数据摄入(仓库现有服务,日常定时运行,不在本管线内)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  Bloomberg TV YouTube 转录 ──► the_close_summary_service ──► ①表 the_close_summaries
  (The Close 节目)              (LLM 八段式中文 PM Brief;快照另抽
                                 positioning_material = 3/4/5/6/8 段聚合)
  CNBC Closing Bell Podcast ──► cnbc_closing_bell_service ──► ②表 cnbc_closing_bell_summaries
  EODHD /economic-events ────► economic_events_service ────► ③表 economic_events
                                 (快照构建时对 [T-2,T] 做一次 actual 回填:
                                  chunked 重拉 + COALESCE upsert,只增不减)
  EODHD 行情 API ────────────► market_data_service ─────────► ④表 market_data_eod
  Finviz Elite export CSV ───► finviz_screener_service ────► ⑤表 finviz_screener
                                 (整表刷新、snapshot_date 打 Beijing 日期,
                                  快照锚定 ≤T+1,详见 build_snapshot 注释)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 第二层 · Stage 0 冻结快照(cron 第一棒)  脚本: build_snapshot.py + fetch_macro_pillars.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  ①② 各取 ≤T 最新 completed 摘要,披露 lag_days(同日锚定,不再静默混口径)
  ④ 16 个跨资产代码 ≤T 最新两根日线 → 1D 涨跌幅
  ⑤ S&P 500 分板块涨幅均值 + |涨跌幅| Top 30 movers(锚定 ≤T+1)
  ③ 近3天(含回填后的 actual)+未来7天,impact≥2,US/CN/JP/DE/KR
  ⑥ macro_pillars 六子块(全部免费源、确定性抓取、并行+看门狗超时):
      par_curve       Treasury 每日面值收益率曲线 CSV(水平+Δbp)
      fred_ranks      FRED DGS 全历史 → "highest since" 预计算断言
                      (≥20 个观测的创新高才生成 claim,否则只给 prior_peak)
      fred_extras     FRED 盈亏平衡/实际利率/HY-IG OAS/房贷/柴油
      effr + fed_odds Yahoo ZQ 合约价 vs EFFR(DFF)→ 自算 +25bp 加息概率
                      (CNT 自证的方法;FRED 全局节流+429 熔断)
      fomc_calendar   federalreserve.gov FOMC 日历(面板年份×会议块解析)
      auction_events  经济日历中的国债拍卖事件(今日 stop/未来拍卖)
      nowcasts        Cleveland Fed/GDPNow(可选,常失败只记 errors)
  ⑦ prior_odds       最近一期更早快照的加息概率(供 "prior: ~x%" 对比)
  ▼
  backend/reports/pipeline/{date}/snapshot.json   ★冻结:本轮生成与核验只读这一份

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 第三层 · 生成与核验(cron 第二棒)  脚本: run_pipeline.py + prompts.py
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  state.json ◄── 每步更新(断点/timings/usage_total)
  ▼
  五个内容 Agent + movers 确定性渲染(按 agent 分组串行;每步一次 zcode -p,--attach 快照)
  ├─ overall_market ─► 01+02  ★一次分析、两个文件:narrative(速览层,≤1 个
  │                      指数级数字、不与 drivers 重复)+ drivers(分析层,全部
  │                      数字/引语/Take)+ HEADLINE_CANDIDATES —— 同一信源拆两个
  │                      agent 会互相看不到对方输出、必然重复,故合并
  ├─ positioning    ──► 03  Primary/Secondary/Guest(素材=positioning_material)
  ├─ sector_action  ──► 04  板块记分牌+主题块
  ├─ movers         ──► 05  ★确定性渲染,数字不经 LLM,直接自快照出表
  │                        (gainers/losers 榜 + Finviz 筛选器链接)
  ├─ deep_dives     ──► 06  六字段模板:What happened/Key numbers/
  │                        Management said*/Analyst reaction*/Bull/Bear
  │                        (*仅当节目含原话/真实反应,否则整行省略)
  └─ macro          ──► 07  块矩阵路由(编排器纯代码判定,复刻 CNT"不是每期一致"):
        releases 出现⟺recent 有 actual ┆ auction ┆ fomc ┆ rate_expectations
        (自算概率 headline+prior 对比) ┆ forward catalysts(恒在,est vs prior)
  ▼
  核验 Agent(一次 zcode -p,带 Bash)
  ├─ 自写 verify/check.py:数字对账 + est/actual/prior 三元组 + since-claims
  │   ↔ fred_ranks + 概率 ±2pct + 归因映射(括号标签↔快照源)+ 结构 lint
  │  (标题精确/Take 行/deep-dive 字段/块矩阵一致性/HEADLINE 候选)
  └─ 叙事质量审查 ──► verify/verdict.json
  ▼
  Gate(纯 Python)
  ├─ pass ──► assemble:编辑化标题 + 口径块 + 七节;审计注释剥离到
  │            final/report.audit.md(sidecar),正文纯净(CNT 表面文体)
  │            ──► --publish-cmd 发布钩子
  ├─ fail ──► --resume 定向修订(≤2 轮)
  └─ 仍 fail ─► NEEDS_HUMAN_REVIEW.md + exit 2
```

## 产物目录(全审计落盘)

```
backend/reports/pipeline/{date}/          (gitignore 内)
├── snapshot.json             冻结快照(四源 + macro_pillars,~100KB)
├── state.json                断点续跑 + timings + usage_total + macro_blocks
├── sections/01..07_*.md      七章节(movers 为确定性渲染产物)
├── verify/check.py           核验 Agent 自写对账脚本(审计留档)
├── verify/check_results.json 逐条对账结果
├── verify/verdict.json       最终裁决 + 问题清单
├── final/report.md           成品(审计注释已剥离,表面即 CNT 文体)
├── final/report.audit.md     剥离的逐条审计注释(sidecar)
└── NEEDS_HUMAN_REVIEW.md     (仅三轮核验未过时)转人工队列
```

## 文件

| 文件 | 作用 |
|---|---|
| `build_snapshot.py` | Stage 0。聚合五源 + macro_pillars + prior_odds → 冻结快照 |
| `fetch_macro_pillars.py` | MACROS 确定性数据底座(六子块,并行+看门狗+FRED 熔断) |
| `prompts.py` | 七份章节任务书 + 核验任务书;STYLE_PROFILE(en/cn);ATTRIBUTION_MAP |
| `run_pipeline.py` | 编排器:状态机、movers 渲染、块矩阵、门控、定向修订、发布钩子 |

## 使用

```bash
PY=/root/miniconda3/envs/guanhe/bin/python
ZCODE=/root/.zcode/server/bin/zcode   # 或 export ZCODE_BIN=...

# 1. 构建快照(--no-backfill 可跳过 EODHD 回填省配额;实测含 macro_pillars ~2min)
$PY tools/report-pipeline/build_snapshot.py [--date 2026-10-27]

# 2. 跑管线(七章节 → 核验 → 门控 → final/report.md)
$PY tools/report-pipeline/run_pipeline.py 2026-10-27 --style en    # CNT 一致英文(默认)
$PY tools/report-pipeline/run_pipeline.py 2026-10-27 --style cn    # 中文正文

# 无 ZCode 环境下验证状态机/门控/重试逻辑(占位章节,核验先 fail 后 pass)
$PY tools/report-pipeline/run_pipeline.py 2026-10-27 --fake-zcode
```

退出码:`0` 已发布 / `1` 运行错误(可断点续跑) / `2` 核验未通过 → `NEEDS_HUMAN_REVIEW.md`。

## 定时

```cron
# 美股交易日后北京时间早晨(与 The Close/Closing Bell 摘要任务衔接)
# 注意: 核验 Agent 自写并运行对账脚本,实测可超 900s,cron 里给足 --timeout
30 8 * * 2-6  cd /opt/guanhetech && /root/miniconda3/envs/guanhe/bin/python \
  tools/report-pipeline/build_snapshot.py >> backend/reports/pipeline/cron.log 2>&1 && \
  /root/miniconda3/envs/guanhe/bin/python \
  tools/report-pipeline/run_pipeline.py $(date +\%F -d yesterday) --timeout 1800 \
  >> backend/reports/pipeline/cron.log 2>&1
```

## 设计规则(改代码前先读)

1. **快照不可变** — 生成与核验必须对同一份冻结数据,数字对账才成立。
2. **文件即契约** — Agent 只写指定文件,编排器只读文件,不解析对话文本。
3. **步级幂等** — `state.json` 记录每步状态;崩溃后重跑只补未完成步骤,不重付 token。
4. **门控是纯代码** — 发布/重试/转人工的决策绝不交给 LLM 判断。
5. **数字红线** — 章节 prompt 明令"唯一数据源是快照";核验 Agent 用自写的
   `check.py` 把草稿数字与快照逐一对账(确定性检查),对不上即 fail。
6. **定向修订** — 重试时 `--resume` 续接原会话,只喂问题清单,不重付全部上下文。
7. **成本核算** — 每次 `--json` 返回的 usage 累计进 `state.json`。
8. **审计分层(纪律与执法成对,但表面文体与 CNT 一致)**:
   - 正文用 CNT 式括号归因 `(The Close)/(FRED)/(Treasury curve)/...`,
     完整审计写行尾 HTML 注释 `<!-- audit: [data market.cross_asset] -->`;
   - 核验扫注释层(层标签指向错误层 = high);发布时剥离到 sidecar;
   - 「不可得」红线:缺失显式标注,禁止替代指标;虚假「不可得」= high;
   - 因果单源约束:单一 [节目] 锚点的因果结论必须带 Read:/推断: 前缀;
   - 口径/数据质量头部块:编排器确定性渲染(不经 LLM),含 lag_days 披露。
9. **句式红线(CNT 惯例,核验执法)** — 每个数字带参照系(actual/est/prior
   三元组或历史比较);"highest since" 只能引用 fred_ranks 预计算 claim;
   收益率变动用 bp;driver 块以一句话 Take 收尾;引语必须姓名+机构且
   逐字来自源;Management said/Analyst reaction 无原话则整行省略。
10. **MACROS 块矩阵** — 编排器按快照判定当日块(Releases⟺有 actual 等),
    prompt 只含相关块指令;核验做一致性 lint。

## 已知边界

- **nowcasts**(Cleveland Fed/GDPNow)页面重度 JS,本环境不可解析 → 常态缺失,
  只影响 Forward Catalysts 的 nowcast 行;如需补齐建议找官方数据文件 URL。
- **consensus 预期值**来自 EODHD 经济日历(CNT 作者本人也抱怨商业日历常错);
  fred_ranks 的 claim 已做"≥20 观测创新高"门槛,避免无意义断言。
- **prior_odds** 依赖上一期快照存在;第一期为空属正常, "(prior:)" 不写。
- finviz 表为整表刷新,历史日期无法补跑(锚定 ≤T+1,补跑旧日期会判不可得)。

## 扩展点

- **发布**:`--publish-cmd "..."`(环境变量 `$REPORT_FILE` 指向成品)可接
  现有报告服务的 HTML 渲染 / 入库 / share_url 流程。
- **并行化**:七个章节目前串行;除 macro(依赖块矩阵,其实也可先算)外
  互不依赖,可改并发(注意 zcode 并发配额)。
- **图表(P5)**:CNT 每期 3-8 张自制图(Source: FRED/EODHD 标注),可在
  assemble 阶段加 matplotlib 确定性出图,数据源全部现成。
- **按用户定制**:章节任务书从 `prompts.py` 换成读取 `llm_prompt_templates`
  中 `scope='user'` 的模板即可实现 per-user 报告。
