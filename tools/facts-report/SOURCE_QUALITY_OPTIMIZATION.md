# Morning Brief 素材质量优化(v9 信源层级)

> 2026-11-23 · 适用 `tools/facts-report` · 状态:本地已验证,待服务器 A/B 复测

## 1. 目标

编辑计划、6 个写稿编辑部与 outlook 拿到的素材要满足三点:一手、权威、相关。本轮不改 triage(cron 仍是 `--no-triage`),全部用确定性逻辑选材,具体做三件事:

1. **提高一手权威数据的权重。** Bloomberg / Reuters 剪藏全文单列为第 1 层,有独立配额,L1 全部收录。
2. **用逻辑滤掉低质素材。** 包括 SEO 稿、公司公告、自动生成稿、隔日复盘稿、与当日无关的稿件,以及多个转播号重复发的快讯。
3. **让计划和写稿以第 1 层为据(开关控制)。** 计划摘要置顶 B/R,故事带 `evidence` 条目,写作规则加信源优先级,个股组可读相关的 B/R。

## 2. 评估:过往 9 期报告的数据质量问题

评估范围是 11-10 至 11-20 共 9 期正式产物(`backend/reports/facts-report/{date}/`),用新增的 `eval_data_quality.py` 统一度量。

| 问题 | 9 期实测 | 影响 |
|---|---|---|
| B/R 全文没有进入 wire | wire 每期 50 条,第 1 层 **0 条**;当天可得的 B/R L1 有 4–13 篇,**全部漏掉** | 旧逻辑按 L 级取 top50。B/R 剪藏入库晚、L 级不占优,被转载稿挤掉 |
| wire 垃圾多 | 每期 SEO / 公告 / 自动稿 **12–34 条**,隔日复盘稿 0–5 条 | 一半名额被浪费;隔日复盘稿会把 T-1 的涨跌当成 T 日写 |
| wire 时段单一 | 50 条全部落在同一时段(按时间倒序截断) | 盘前、盘中、盘后覆盖缺失 |
| flash 重复 | 同一 headline 多号重发,残留重复 **23–49 条**,最大簇 20 条;转播号占比 0.73–0.88 | 300 个名额里大量重复,原始出处丢失 |
| geo 混入聚合站垃圾 | 每期 1–22 条 | 地缘节素材被稀释 |
| 计划没有依据条目 | 7 个故事里带第 1 层 evidence 的为 **0**;规范数值取自新闻 **0–3 条/期** | 计划的数字来源不可追溯,部分取自转载稿 |
| 成稿不引 B/R | 成稿引用 65–98 条,其中第 1 层 **0 条** | 最终报告没有用上一手权威信源 |
| digest 晨报为空 | digest 北京 08:25 后才发布,08:00 构建时通常未到 | 已在快照里写明原因(`DIGEST_EMPTY_NOTE`),由 geo / research / 英文 wire 兜底 |

## 3. 方案总览

| 层 | 改动 | 是否默认生效 |
|---|---|---|
| 快照选材层 | `source_quality.py` + `build_snapshot.py`:信源层级、wire / flash / geo 选材 | **是**(部署即生效) |
| 编辑计划 + 切片 v2 | `prompts.py` + `run_pipeline.py`:计划摘要置顶 B/R、`evidence`、信源优先级规则、个股组读 B/R | 否,由 `MB_EDITORIAL_V2=1` 开启 |
| 评估与复测工具 | `eval_data_quality.py`、`replay_news.py`、`FACTS_REPORT_ROOT`、`test_source_quality.py` | 工具,不影响正式运行 |

快照层先上线,只改变选材,不改提示词。v2 等 A/B 复测通过后再开。

## 4. 信源层级(`tier`)

`tier` 与原有 E 级(`info_level` / E2)并存:E 级描述信息性质,`tier` 描述信源本身。

| 层级 | 定义 | 例子 |
|---|---|---|
| 1 | Bloomberg / Reuters 的 `news` 频道剪藏全文 | Bloomberg、Reuters |
| 2 | 原创媒体与署名人物;转播号、聚合站如果正文注明了原始出处(`origin`),也升到第 2 层 | CNBC、CCTV、The Information、AP、Nick Timiraos、Bloomberg TV / Reuters YouTube、知识星球、Substack |
| 3 | 推特转播号、Seeking Alpha 新闻 | — |
| 4 | 其他新闻聚合 | Yahoo Finance、Globenewswire |

- **出处识别。** `credited_origin` 识别正文点名的 Reuters / RTRS、Bloomberg / BBG、WSJ、FT、The Information、NYT、CNBC、Axios、Semafor、Politico、AP、Nikkei Asia、Xinhua。`syndicated_origin` 识别 `(Reuters) -` 这类通稿抬头。
- **升层上限。** 注明出处只能升到第 2 层,不会升到第 1 层,因为不是全文。

## 5. 快照选材规则(默认生效)

### 5.1 wire(`news.wire`)

**相关度上下文**由以下标的组成,按公司简称和 ticker 写法(`$X`、`(X)`、`(NASDAQ:X)`、`(X.O)`)匹配:
- 当日 S&P 涨跌幅前 40;
- 市值前 30;
- `top_movers` 前 10;
- 今日及前一日盘后的财报股;
- flash L1 里出现的 ticker。

打分规则:
- 标题命中实体记 3 分(最多 2 个);
- 导语命中再加分;
- 宏观、地缘关键词(含中国相关词)加分;
- 标题含"市场"类词加 1 分。

**第 1 层(B/R)**
- 最多 25 篇(`_WIRE_T1_MAX`)。
- L1 全部收录;其余按相关度排序,并保证盘前、盘中、盘后三个时段(ET)各有保底名额。
- 正文先用 `clean_clip_body` 去掉剪藏模板:重复标题、Source 链接、署名、Takeaways、图片说明、记者署名尾行、Report Ad 等。
- 去模板后截到 3000 字(`_WIRE_T1_TEXT_CAP`)。

**第 2–4 层**:先过滤,再限额。

过滤规则:
- 稿件形态为 `seo`、`lowvalue`(含 AP 的 `BC-` 表格稿)时,一律丢弃;
- 导语把涨跌定在 T 日之前收盘的隔日复盘稿,一律丢弃;
- `pr`、`auto`(公告、GuruFocus、Earnings Snapshot)没有点名相关实体时丢弃;
- 普通新闻相关度为 0 时丢弃;
- 与第 1 层同题的稿件丢弃(`dup_of_t1`)。

限额:
- 第 1 层 ≥10 篇时,其余层只留 15 篇(`_WIRE_OTHER_MAX_WITH_T1`);否则留 35 篇(`_WIRE_OTHER_MAX`,对应 B/R 剪藏未到的情况)。
- 每篇截到 1200 字。

**选材统计**写在 `news.selection.wire`,包括候选数、各层输入、各原因丢弃数、第 1 层可得 / 入选、L1 可得 / 入选、时段分布。

### 5.2 flash(`news.flash`)

- **聚簇。** 45 分钟内标题共享 ≥4 个词,且占较短一方的 ≥70%,判为同一事件。
- **簇代表。** 优先级依次为:L 级最高、注明出处、层级高、正文最长。代表条目带 `dup_n`(转发数)和 `also`(其他来源)。
- **入选。** L1 全部入选;其余按"层级 → 是否带 ticker → 转发数"排序。每个时段保底 25 条,总上限仍是 300。
- **条目字段。** 每条带 `tier` 和 `origin`。

### 5.3 news_macro

| 池 | 改动 |
|---|---|
| geo | 非 news 频道和第 2 层直接保留。第 1 层要求标题命中地缘关键词。其余层要求稿件形态为 `news` 且标题命中。排序时标题命中优先,其次按层级。 |
| research | 关键词命中后按层级排序。 |
| digest | 为空时统一写 `DIGEST_EMPTY_NOTE`,替换原来只在周五出现的说明。 |

### 5.4 其他

- **构建顺序。** `build_snapshot()` 改为先拿行情再取新闻,这样相关度能用上当日异动股,也能多收到晚到的剪藏。
- **"已报道"判定。** `flag_reported_items` 对 wire 只看标题加正文前 600 字。原来看全文,B/R 长文容易被误标。

## 6. 编辑计划与切片 v2(`MB_EDITORIAL_V2`,默认关)

**开关读取。** 进程环境变量优先(`=0` 强制关);未设置时读仓库 `.env`(cron 不加载 `.env`,所以走 `llm_fallback._env_file_lookup`)。取值 `1`、`true`、`yes`、`on` 视为开启。

**计划输入(`render_plan_input`)**:
- 置顶 "Primary reporting — Bloomberg / Reuters" 块,每条列出作者、时间、L 级、标题、导语。
- flash L1 按层级和转发数排序,标注 `×N` 与 `[origin]`,最多 120 条。
- 依次列出其他 wire(带出处)、电话会、宏观池;宏观池标题被截断时用正文开头补全。

**计划输出(`plan.json`)**:
- 每个故事加 `evidence`,列出该故事所依据的条目 id,如 `["w3", "f12"]`。`_load_plan` 只保留快照里真实存在的 id,最多 6 个。
- 新增规则:规范数值优先取结构化数据,新闻只用于别处没有的数字,不得把前一交易日的涨跌当作当日。

**写稿**:
- `plan_brief` 把 evidence 转给负责该故事的节,并要求先读这些条目、以它们为据写要点并引用。
- 写作规则加 SOURCE TIERS 段:同一事实以第 1 层为准。

**个股组切片**:
- `sector_action`、`deep_dives` 的 `SECTION_SOURCE_MATRIX` 加上 newswire。
- `company_desk` 切片只保留第 1–2 层中点名相关个股的 wire:标题命中,或正文前 1500 字命中 ≥2 个实体,最多 12 篇(`DESK_WIRE_MAX`)。条目 id 不变。

## 7. 效果:9 期本地复测

**复测方法。** `replay_news.py` 读取正式快照,只重算 `news` / `news_macro`,行情、日历和节目转录原样保留。查询窗口上沿截到原快照的 `generated_at`(北京约 08:00),复现当时可见的数据。

| 日期 | wire 第 1 层(旧 → 新) | B/R L1 收录(新,可得数) | wire 垃圾 | 隔日复盘稿 | wire 时段数 | flash 残留重复(最大簇) | flash 转播占比 | geo 垃圾 |
|---|---|---|---|---|---|---|---|---|
| 11-10 | 0/50 → 25/40 | 5/5 | 31 → 0 | 3 → 0 | 1 → 3 | 27(4) → 3(3) | 0.85 → 0.59 | 22 → 0 |
| 11-11 | 0/50 → 25/40 | 4/4 | 22 → 1 | 3 → 0 | 1 → 3 | 31(10) → 1(2) | 0.73 → 0.70 | 11 → 0 |
| 11-12 | 0/50 → 25/40 | 13/13 | 31 → 0 | 0 → 0 | 1 → 3 | 49(14) → 1(2) | 0.83 → 0.63 | 18 → 0 |
| 11-13 | 0/50 → 25/40 | 13/13 | 12 → 0 | 0 → 0 | 1 → 3 | 23(10) → 3(2) | 0.73 → 0.70 | 1 → 0 |
| 11-16 | 0/50 → 25/40 | 5/5 | 21 → 0 | 0 → 0 | 1 → 3 | 30(9) → 1(2) | 0.88 → 0.74 | 10 → 0 |
| 11-17 | 0/50 → 25/40 | 7/7 | 26 → 0 | 0 → 0 | 1 → 3 | 32(6) → 0(1) | 0.83 → 0.57 | 10 → 0 |
| 11-18 | 0/50 → 25/40 | 6/6 | 34 → 0 | 4 → 0 | 1 → 3 | 23(5) → 1(2) | 0.83 → 0.62 | 9 → 0 |
| 11-19 | 0/50 → 25/40 | 9/9 | 32 → 0 | 0 → 0 | 1 → 3 | 39(20) → 0(1) | 0.87 → 0.63 | 7 → 0 |
| 11-20 | 0/50 → 25/40 | 8/8 | 30 → 0 | 5 → 0 | 1 → 3 | 39(7) → 2(2) | 0.85 → 0.73 | 10 → 0 |

- **wire 候选池。** 每期可得 B/R 53–76 篇,入选 25 篇。
- **丢弃规模(每期)。** SEO 111–229、公告 44–245、无关 88–214、自动稿 29–82、低价值 12–62、隔日复盘 1–20、与 B/R 同题 1–10。
- **其他检查。** `build_snapshot` 冒烟(接生产库只读,EODHD / FRED 打桩):wire 40(第 1 层 25)、flash 300,条目 id 正常,内部字段无泄漏。`--fake-zcode` 在 v2 关 / 开两种情况下都跑通(rc=0),正式目录未被改动。
- **未覆盖的部分。** 真实 LLM 的计划 / 成稿效果(evidence 是否落到第 1 层、成稿第 1 层引用数、token、核验)只能在服务器上验证,见第 9 节。

## 8. 代价

| 项目 | 变化 |
|---|---|
| wire 字数 | 约 59k → 81–89k(+约 25k) |
| flash 字数 | 约 48–56k → 53–58k(+约 3–5k) |
| geo / research | geo 略减,research 增加约 2–3k |
| v2 计划输入 | 19.7k → 30.5k 字(11-20) |
| v2 个股组切片 | +约 17k 字(11-20,6 篇相关 B/R) |

写稿阶段的主要 token 消耗来自多轮工具调用(11-20 正式 write 阶段输入约 10.5M,大部分走缓存)。所以 A/B 要实测 token 增幅,目标 ≤ +20%。

## 9. 服务器测试步骤

以下在 43.99.37.76 上执行,`PY=/root/miniconda3/envs/guanhe/bin/python`。

### 第 0 步:部署,保持 v2 关闭

需要提交的文件:
- 修改:`build_snapshot.py`、`prompts.py`、`run_pipeline.py`、`README.md`
- 新增:`source_quality.py`、`eval_data_quality.py`、`test_source_quality.py`、`replay_news.py`、本文档

部署后,周二 08:00 的 cron 会直接使用新的选材层,提示词与原来一致。

### 第 1 步:自检(约 1 分钟)

```bash
cd /opt/guanhetech/tools/facts-report
PY=/root/miniconda3/envs/guanhe/bin/python
$PY -I test_source_quality.py && $PY -I test_llm_fallback.py
grep NEWS_LLM_TAG_SOURCES /opt/guanhetech/.env      # 需包含 Bloomberg,Reuters(B/R 入库打 L 级)
grep MB_EDITORIAL_V2 /opt/guanhetech/.env || echo "v2 未设置(=关)"
```

### 第 2 步:重放新闻层 + 模拟跑

`replay_news.py` 在 `FACTS_REPORT_ROOT` 未设置或指向正式目录时拒绝运行。

```bash
FACTS_REPORT_ROOT=/tmp/mb_ab_v1 $PY replay_news.py 2026-11-18 2026-11-20
mkdir -p /tmp/mb_ab_v2 && cp -r /tmp/mb_ab_v1/2026-11-* /tmp/mb_ab_v2/
cp -r /tmp/mb_ab_v1 /tmp/mb_fake && FACTS_REPORT_ROOT=/tmp/mb_fake $PY run_pipeline.py 2026-11-20 --fake-zcode
```

### 第 3 步:真跑 A/B(不登记、不发布)

先在 shell 里执行 `cron_daily.sh` 第 26–36 行的 zcode 环境变量导出(`HOME`、`ZCODE_*`、`PATH`),然后运行:

```bash
FACTS_REPORT_ROOT=/tmp/mb_ab_v1 MB_EDITORIAL_V2=0 \
  $PY run_pipeline.py 2026-11-20 --style cn --max-parallel 2 > /tmp/ab_v1.log 2>&1
FACTS_REPORT_ROOT=/tmp/mb_ab_v2 MB_EDITORIAL_V2=1 \
  $PY run_pipeline.py 2026-11-20 --style cn --max-parallel 2 > /tmp/ab_v2.log 2>&1
```

两组串行跑,不要并行。7GB 机器上 3 路就会 OOM。时间够的话,11-18 也按同样方式跑一遍。

**对照组说明。** 两组用的是同一份新选材快照,只差 v2 开关。新选材本身相对旧快照的效果,看第 7 节的快照指标和周二正式产物。

### 第 4 步:打分

```bash
FACTS_REPORT_ROOT=/tmp/mb_ab_v1 $PY eval_data_quality.py 2026-11-20
FACTS_REPORT_ROOT=/tmp/mb_ab_v2 $PY eval_data_quality.py 2026-11-20
$PY eval_data_quality.py 2026-11-20      # 旧正式产物作基线
```

核验结果看各工作区的 `state.json`:`verify.attempts`、`verify.status`、`verify.deterministic_high_fails`、`usage_total`。

## 10. 验收标准

| 指标 | 目标 |
|---|---|
| wire 第 1 层入选 | 当天 B/R ≥10 篇时 ≥10;L1 召回 100% |
| wire 垃圾 / 隔日复盘稿 | ≤1 / 0 |
| flash 残留重复 | ≤5,最大簇 ≤3 |
| (v2)计划带 evidence 的故事 | ≥80%,其中含第 1 层条目的占多数 |
| (v2)规范数值取自新闻 | 0 |
| (v2)成稿引用第 1 层条目 | ≥15 |
| token 总量 | 相对 v1 增幅 ≤ +20% |
| 核验 | 确定性高危失败数不增加,`status=pass`,attempts 不增加 |

**判定。** v2 全部达标后,在服务器 `.env` 加 `MB_EDITORIAL_V2=1`,下一次 cron 生效。周二正式产物先看 `snapshot.json → sources.news.selection`,确认 `t1_selected` 和 `t1_l1_selected == t1_l1_available`。

## 11. 回滚

| 范围 | 做法 |
|---|---|
| 只回滚 v2 | `.env` 删除或设 `MB_EDITORIAL_V2=0`,不需要改代码 |
| 回滚选材层 | `git revert` 本次提交。选材层没有开关;快照字段只增不删(`tier`、`origin`、`dup_n`、`also`、`selection`),下游读旧字段不受影响 |

## 12. 已知限制与后续

- **digest。** 08:00 构建时 digest 仍为空(08:25 后才发布)。可以考虑在 `show_gate` 之后补刷一次 news_macro.digest。
- **research 重复。** research 池有大小写不同的重复标题,还没合并。
- **相关度依赖行情。** 相关度用到当日异动和市值榜;行情缺失时退化为财报股 + flash L1 ticker。
- **flash 聚簇是词重合判定。** 措辞差异大的同一事件可能没有合并。阈值偏保守:测试里两条相反的 Waller 表态不会被误合并。
- **复测窗口按 `published_at` 截。** `fetched_at` 会被重抓覆盖,不能当入库时间用。08:00 后才入库、但发布时间更早的条目,复测时仍然可见,所以结果相对真实 08:00 略偏乐观。

## 附:文件清单

| 文件 | 说明 |
|---|---|
| `source_quality.py` | 新增。纯函数:层级 / 出处、剪藏去模板、稿件分类、隔日稿判定、相关度、时段、flash 聚簇、wire / flash 选材 |
| `build_snapshot.py` | wire / flash / geo / research 改用 `source_quality`;构建顺序调整;`DIGEST_EMPTY_NOTE`;复测用 `REPLAY_AS_OF`(正式运行恒为 None);输出路径支持 `FACTS_REPORT_ROOT` |
| `prompts.py` | `editorial_v2()` 开关;SOURCE TIERS 规则;计划 `evidence` 字段与规则;`plan_brief` 转交 evidence;"已报道"判定只看导语 |
| `run_pipeline.py` | `FACTS_REPORT_ROOT`;v2 计划输入;`_load_plan` 校验 evidence;个股组 `_desk_wire` |
| `eval_data_quality.py` | 新增。只读打分:快照 / 计划 / 成稿 / 成本 |
| `replay_news.py` | 新增。A/B 复测:基于正式快照副本重放新闻层 |
| `test_source_quality.py` | 新增。离线单测(`python -I test_source_quality.py`) |
| `README.md` | v9 选材与 v2 开关说明 |
