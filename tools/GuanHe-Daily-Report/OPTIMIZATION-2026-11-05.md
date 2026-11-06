# guanhe-daily-report v8.4 优化记录与真机测试方案

- **日期**:2026-11-05
- **依据**:`2026-11-03-v83` 报告评估 + [TEST-2026-11-04.md](TEST-2026-11-04.md)
- **范围**:P0、P1 已完成；P2 未做（见第五节）
- **验证状态**:离线验证（用 v83 数据做单元验证 + `--fake-zcode` 全流程）已通过。**还没有用真实 ZCode 跑过**，第四节是真机测试步骤，跑完按第六节反馈。

---

## 一、评估结论（为什么要改）

v83 的内容足够丰富，结构化数字也准确，作为可发布初稿是合格的。但有三类系统性问题，也解释了为什么核验加修订吃掉了约 46% 的输入 token：

1. **编排**：各 agent 并行写作，没有人分配"哪个故事归哪一节"。同一件事在多节重复，数字也不一致，比如 Supermicro 盘后一节写 +6%、另一节写 +8%。Meta 7.50% 票息、$58bn 认购在 02/04/06/11 四节各出现一次。
2. **核验**：文本来源（快讯、转录）的台账数字只检查"这串数字是否出现在整个文本池里"，查不到也只记 medium。实验中故意改错的数字有 68% 能通过。另外，任何核验意见（包括 medium/low）都会触发修订，08 节的占位内容因此被改写成正文（P18）。
3. **数据层**：
   - 快讯时间戳整体早 8 小时：DB 返回不带时区的 UTC 时间，被当成北京时间处理。
   - 08 节抓不到 JOLTS：一是选题族里没有 JOLTS，二是日历 actual 晚到约 25 小时。
   - DST 造成日历重复行。

## 二、优化计划（优先级）

| 优先级 | 事项 | 状态 |
|---|---|---|
| P0 | 修 `_et_fields` 时区 bug | ✅ |
| P0 | 只让 high 触发修订；确定性节、占位节不进 LLM 修订；high 无处可修时直接转人工 | ✅ |
| P0 | "复核备注"移入 `report.audit.md`；09 节选举主题与焦点事件去重 | ✅ |
| P1 | 08 节：加 JOLTS/PCE 选题族；日历去重；用快讯打印值回填 actual；08/10 平静日改代码渲染 | ✅ |
| P1 | 台账按条目核验 + 实体/指标跨节一致性；LLM 核验默认 annotate（只写备注） | ✅ |
| P1 | 写作前编辑计划（故事归属 + 规范数值）；收紧 02/11 的建议分工 | ✅ |
| P2 | Movers 原因列/盘后异动表/ticker 别名；统一标点与译名；07 单位与"本周"范围；The Close 降级评估；03 节结构化仓位数据；图表 T 日校验 | ⏳ 未做 |

---

## 三、代码改动明细

### P0（v8.4 门控）

**`build_snapshot.py`**
- `_et_fields`：不带时区的时间先补 `tzinfo=utc` 再转美东。快讯、通讯社、电话会纪要三层的 `time_et`/`date_et` 都受影响。

**`run_pipeline.py`**
- **门控**：`verdict = fail` 当且仅当存在 high（`_high_issues`）。LLM 自报 fail 但没有 high，不拦发布。
- **修订路由**（`revise_sections` / `_revisable`）：只修 high。确定性节（05/07）永不修。占位节只接受确定性核验（`source=deterministic`）的问题。同一 agent 的重复问题合并。日志会打出 `[revise] skip <节>: locked section`。
- **转人工**：所有 high 都落在锁定节、没有可修的节时，直接写 `NEEDS_HUMAN_REVIEW.md`，不空转修订轮次。
- **复核备注**：`_review_notes` 汇总 LLM 意见、quality_notes 和确定性核验的非 high 残留，只写进 `report.audit.md` 的 "Review notes"，正文不再出现。
- **09 焦点事件**（`_geo_stats`）：焦点事件有一半以上条目已归入某个已开主题时，不再单开「焦点事件」，避免选举写两遍。
- **fake 模式**：`review_releases` 为空时，08 节输出占位，和真实合同保持一致。

### P1-a：08/10 节与宏观日历

**`build_snapshot.py`**
- **日历去重**：`_economic_events` 按 (日期, 国家, 事件, 比较口径, 期别) 去重。同一事件因 DST 或改时产生的重复行，保留有 actual、更新更晚的一行。
- **快讯回填 actual**（`fill_actuals_from_flash`）：T 日缺 actual 的日历行，从最早一条匹配的快讯取"数值 (Est. …, Prior …)"格式的打印值，并记录 `actual_source="news.flash[id=f21]"`。
  - 以下情况不回填：比较口径不符（MoM/YoY）、期别月份不符、国家不符、量级和预期/前值差太多（差值 >1 且比值不在 0.5–2 之间）。
  - 回填结果记在 `snapshot.notes.actuals_from_flash`。
- **条目 id**：快照末尾调用 `assign_item_ids`，规则为快讯 f#、通讯社 w#、纪要 c#、宏观日报 d#、研报 r#、地缘 g#。

**`run_pipeline.py`**
- `REVIEW_FAMILY_ORDER` 加入 `pce`、`jolts`。JOLTS 只匹配 `job openings`，避免误选 "JOLTs Job Quits"。
- **平静日代码渲染**（`QUIET_RENDERERS`）：
  - **10 美联储观察**：当 `fomc_deep=false`、快讯/通讯社里没有 Fed 官员讲话（`FED_SPEAK_RE`）、且有 `next_fomc_odds` 时，由代码渲染「利率与决议」和「市场定价路径」两块：决议/票数/异议理由（鹰鸽）、下一场概率（含前值）、远端合约路径、2y/10y。
  - **08 回顾·宏观数据**：没有选中任何发布时，由代码写占位。
  - 代码渲染的节在 `state.json` 记为 `mode: deterministic, note: quiet day`，并且会锁定（不修订、不核台账）。幂等重跑时保留原有的 state 记录。
- 启动时检查快照有没有条目 id，旧快照会自动补编号并写回（只加 id，不改内容）。

### P1-b：确定性核验（`verify_checks.py`）

- **按条目核验**：新闻数字必须引用具体条目，比如 `news.flash[id=f12]`、`news_macro.geo[id=g3]`，也支持多条件选择器 `[author=Reuters,time_cn=11-03 16:24]`。
  - 数字只在被引的那一条里找，支持 万/亿/万亿/K/M/B/T/million/bn 换算。
  - 容差按台账写出的精度计算：原文写 91%，撑不起台账里的 91.37%。
  - 被引条目里没有这个数字 → **high**，提示里会列出含这个数的条目 id。
  - 整桶引用（`news.flash`）→ 每节汇总一条 medium。
  - 文本池和快照里都找不到 → high。
- **跨节一致性**：台账增加可选字段 `entity` + `metric`。同一实体、同一指标在不同节的数值不同时：
  - 行情类（move/ah_move/close/level/yield/odds）判 high，财报类（revenue/eps/guide_*）判 medium。
  - 参照值的优先级：编辑计划规范值 > 已按路径核实的值 > 多数节。
  - 只路由给偏离的节，修改建议里直接给出参照值。找不到唯一参照时只记 medium。
- `state.json` 里标为 deterministic 的节（平静日的 08/10）跳过台账检查。
- `verified_canonicals`：计划里的规范值必须能在快照中核实，才会注入任务书、参与核验。任务书和核验共用同一份清单。

**`prompts.py`**：规则 11 改为要求按条目 id 引用新闻、禁止整桶引用，并说明 `entity`/`metric` 的取值约定。

### P1-c：LLM 核验 annotate 模式（`run_pipeline.py`）

- `--llm-verify` 新增 `annotate` 并设为默认。它跑的是和 light 同一份只读正文的核验，但所有 LLM 意见都标为 `advisory`，`_high_issues` 不把它们算进门控，只写进复核备注，标记为 `[high, advisory]`。
- `light` 保留旧行为（LLM 的 high 会触发修订）。fake 模式不打 advisory 标记，以便继续测试修订循环。

### P1-d：编辑计划（`run_pipeline.py` + `prompts.py`）

- 写作前调用一次 LLM（`run_plan`，超时 600s）。
  - 输入：`plan_input.md`，约 28K 字符。包括跨资产、广度、板块、S&P 涨跌前 10、财报（按市值排序）、宏观块判定、美国日历、L1 快讯、通讯社/纪要标题、宏观素材标题，全部带条目 id，不含节目转录。
  - 输出：`plan.json`。包含 4–8 个故事，每个故事有唯一归属节（`owner`）、可一句带过的节（`may_mention`）、angle、evidence，以及规范数值（`canonical`：entity/metric/value/path）。
- **可认领故事的节**由 `_plan_owners` 判定：drivers、sector_action、deep_dives、outlook 恒在；macro_review 只在有选题时、geopolitics 只在有主题时、fomc 只在非平静日可认领。归属到不可认领节的故事改派给 drivers。
- **任务书注入**（`plan_brief`）：每个 agent 会看到三部分，即自己负责的故事、可以一句带过的故事、其他节负责的故事（不得写），以及核实过的规范数值。
- `plan.json` 已存在且有效就直接复用；失败不阻断；fake 模式跳过；`--no-plan` 可关闭。
- **02/11 分工**：02 节的行动建议限定为针对该驱动的交易级建议；11 节「投资建议」限定为组合级（仓位、结构、超配/回避、改变判断的条件），禁止复述或改写 02 的行动建议。

### 观测性（本次新增）

- `state.json` 新增 `usage_by_phase`，分 plan/write/verify/revise 四项。修订的 token 之前完全没有记录，现在可以直接算出"核验 + 修订占比"。
- 新增 `eval_run.py`：一期报告的指标汇总，只读、不调 LLM、不连库。输出 Markdown，内容包括发布状态、token 分阶段、计划、章节模式、台账写法、核验残留、跨节重复数字、快照抽查。

### 文档

- `README.md` 已同步：编辑计划、平静日渲染、台账条目引用、核验规则、annotate、产物目录、`--no-plan`、选题族与回填。

---

## 四、真机测试方案

以下命令都在服务器仓库根目录 `/opt/guanhetech` 执行，`PY=/root/miniconda3/envs/guanhe/bin/python`。
**不要把输出用管道接 `| tail`**（TEST-2026-11-04 的 P5 教训：会吞掉日志和退出码），一律落盘到文件。

### 第 0 步：部署与静态检查（1 分钟）

```bash
cd /opt/guanhetech && git pull
PY=/root/miniconda3/envs/guanhe/bin/python
for f in run_pipeline verify_checks prompts build_snapshot eval_run; do
  $PY -c "import ast; ast.parse(open('tools/guanhe-daily-report/$f.py').read()); print('$f ok')"
done
```

### 第 1 步：离线回归（不调 LLM，约 1 分钟）

**1a. 用新核验器重查 v83**，验证新规则在真实台账上没有误报：

```bash
rm -rf /tmp/v83chk && cp -R backend/reports/guanhe-daily-report/2026-11-03-v83 /tmp/v83chk
$PY - <<'EOF'
import json, sys; sys.path.insert(0, 'tools/guanhe-daily-report')
from pathlib import Path
from collections import Counter
from prompts import assign_item_ids
import verify_checks as vc
ws = Path('/tmp/v83chk'); f = ws / 'snapshot.json'
s = json.loads(f.read_text()); assign_item_ids(s['sources'])
f.write_text(json.dumps(s, ensure_ascii=False, default=str))
rows = vc.run_checks(ws, 'cn')
print(Counter(r['severity'] for r in rows))
for r in rows: print(r['severity'], r['section'], r['claim'][:100])
EOF
rm -rf /tmp/v83chk
```

**预期**：`medium×7`，没有 high。其中 6 条是"ledger entries cite a whole text pool"（v83 台账是旧写法），1 条是 narrative 的 whitelist。

**1b. fake 全流程**，验证状态机、平静日渲染和门控：

```bash
D=2099-12-31; W=backend/reports/guanhe-daily-report/$D
rm -rf $W && mkdir -p $W && cp backend/reports/guanhe-daily-report/2026-11-03-v83/snapshot.json $W/
$PY tools/guanhe-daily-report/run_pipeline.py $D --fake-zcode > /tmp/fake.log 2>&1; echo rc=$?
grep -E "^\[(agent|verify|revise)|published" /tmp/fake.log
cat $W/sections/10_fomc.md
rm -rf $W
```

**预期**：
- `rc=0`。第 1 轮核验有 1 个 high（fake 故意写错的数字），修订 overall_market 后第 2 轮 0 high，然后发布。
- `10_fomc.md` 是代码渲染的中文两块：3.75%–4.00%、10:2、Cook 与 Hammack 鹰派异议、12月8–9日 ~82%（前值 ~78%）。

### 第 2 步：Stage 0 重建快照（约 3 分钟）

用一个**全新交易日** T（建议用最近一个，比如 2026-11-04）。这一天有 ISM 服务业 PMI 等 10:00 ET 发布的数据，适合检验时间戳修复和回填。

> 如果该日工作区已经用旧代码跑过，要先清掉旧产物，否则断点续跑会直接复用旧章节，测不到新逻辑：
> `cd backend/reports/guanhe-daily-report/$T && rm -f state.json plan.json plan_input.md sections/*.md sections/*.claims.json verify/verdict.json && cd /opt/guanhetech`

```bash
T=2026-11-04
$PY tools/guanhe-daily-report/build_snapshot.py --date $T > backend/reports/guanhe-daily-report/build_$T.log 2>&1; echo rc=$?
$PY - <<EOF
import json; s = json.load(open('backend/reports/guanhe-daily-report/$T/snapshot.json'))
src = s['sources']; fl = src['news']['flash']
print('ids:', sum(1 for x in fl if x.get('id')), '/', len(fl))
print('actuals_from_flash:', s.get('notes', {}).get('actuals_from_flash'))
for x in fl:
    if any(k in x['text'] for k in ('ISM', 'JOLTS', 'PMI', 'Payrolls', 'CPI')):
        print(x['id'], x.get('date_et', 'T'), x['time_et'], x['text'][:90])
EOF
```

**检查点**：
- 所有快讯条目都带 id。
- 数据类快讯的 `time_et` 和官方发布时刻一致，比如 ISM 服务业约 10:00、初请约 08:30。这次不应再出现 02:00 这种早 8 小时的时间。
- 如果日历 actual 缺失而快讯里有打印值，`actuals_from_flash` 会列出回填结果。核对一下数值和原快讯是否一致。

### 第 3 步：真实全流程（约 30–40 分钟）

```bash
T=2026-11-04
$PY tools/guanhe-daily-report/run_pipeline.py $T --timeout 3300 \
  > backend/reports/guanhe-daily-report/run_$T.log 2>&1; echo rc=$?
```

默认就是 `--llm-verify annotate`，并且开启编辑计划。运行中可以另开终端看日志：

```bash
grep -E "^\[(plan|agent|section|verify|revise)\]|published|pipeline error" backend/reports/guanhe-daily-report/run_$T.log
```

**日志中应该看到**：
- `[plan] editorial plan ...`，之后是 `[plan] {'status': 'done', 'stories': N, ...}`，N 在 4–8 之间。
- 08、10 如果是平静日，会是秒级完成（代码渲染）。
- 核验阶段如果修订，只会出现 high 对应的组；锁定节会打出 `[revise] skip`。

### 第 4 步：收集指标（秒级）

```bash
$PY tools/guanhe-daily-report/eval_run.py $T > backend/reports/guanhe-daily-report/eval_$T.md
cat backend/reports/guanhe-daily-report/eval_$T.md
```

**对照基线**：v83 的同一脚本输出见第七节附录。

| 指标 | v83 基线 | 本次目标 |
|---|---|---|
| 退出码 / 转人工 | rc=2，NEEDS_HUMAN_REVIEW | rc=0，无转人工 |
| 核验轮次 | 5 | ≤ 2 |
| verify + revise 占输入 token | ~46% | < 25% |
| 台账整桶引用 / 新闻类台账 | 218 / 236 | < 10% |
| 带 entity+metric 的台账条目 | 0 | 行情类数字大部分带上 |
| 出现在 ≥3 个 LLM 节的数字 | 11 个 | ≤ 4 个 |
| plan | 无 | done，4–8 个故事 |
| 08/10 平静日 | LLM 写 + 多轮修订 | mode=deterministic，秒级 |

### 第 5 步：人工通读（10–15 分钟，读 `final/report.md`）

逐项打勾：

1. **故事归属**：对照 `plan.json` 的 owner。同一故事是否只在归属节展开，其他节最多一句带过？重点看 04 和 06 是否还在重复同一批财报数字。
2. **数字一致**：同一只股票的涨幅、同一个收益率、同一个概率，全篇是否只有一个数？
3. **02 vs 11**：11「投资建议」是否是组合级，没有复述 02 的行动建议？
4. **08 节**：选中的发布（含回填值）写得对不对？如果是占位，是否确实当天没有一线数据？
5. **10 节**：平静日的代码渲染读起来是否通顺？有官员讲话的日子是否走了 LLM？
6. **复核备注**：正文里不应出现"复核备注""05 节""确定性核验"一类字样。打开 `final/report.audit.md` 的 Review notes，看 `[high, advisory]` 的 LLM 意见里有没有**真问题**被放过。这决定 annotate 是否适合继续作为默认。

### 第 6 步（可选）：幂等与开关

```bash
# 幂等重跑：应秒级复用、不花 token，state 里 08/10 的 mode 保持 deterministic
$PY tools/guanhe-daily-report/run_pipeline.py $T > /tmp/rerun.log 2>&1; echo rc=$?
```

### 第 7 步（可选，约 10M token）：同数据 A/B

想严格比较 v83 和 v8.4 时，用你生成 v83 时的同样方式新建一个工作区（比如 `2026-11-03-v84`），只放 v83 的 `snapshot.json`，然后跑第 3、4 步。注意 v83 的快照是旧代码建的，快讯时间戳仍有 8 小时偏差，也没有回填，所以这个 A/B 只比较编排和核验部分。

---

## 五、风险、回滚与未做事项

**首跑风险**：
- agent 可能不遵守按条目 id 引用。整桶引用只记 medium，不拦发布；但引用了条目而数字对不上会判 high，首跑可能因此多一轮修订。
- 跨节一致性的 high 依赖 agent 填写 `entity`/`metric`。不填时这项检查不生效，但也不会误报。
- 编辑计划会让第一段开始前多 1–3 分钟墙钟。

**回滚开关**（不用改代码）：
- `--no-plan`：关闭编辑计划。
- `--llm-verify light`：恢复 LLM high 触发修订。
- `--llm-verify off`：只用确定性核验。

**未做**（P2 和评估遗留）：
- 图表与正文数据不一致（v83 的图用的是 11-04 盘中的 Finviz 数据），需要加 T 日校验。**建议优先做**。
- 快讯按事件聚类，同一事件数字有更新时保留最新一条。
- 修订改为新开短会话（只带该节文件、问题和证据条目），降低修订成本。
- Movers 原因列 / 盘后异动表 / ticker 别名（FISV↔FIS）；统一标点与译名；07 节单位与"本周"范围；JOLTS `release_series` 标签（标成 level，实际存的是环比变动）；The Close 降级评估；03 节结构化仓位数据；标题日期标注北京时间。

---

## 六、测试完请反馈这些

1. 第 1 步两个离线检查的输出（是否符合预期）。
2. 第 2 步检查脚本的输出。
3. 第 3 步的 `rc`，以及 `grep` 出来的关键日志行。出错时附上 `run_$T.log` 末尾 100 行。
4. **第 4 步 `eval_$T.md` 全文**（最重要）。
5. `plan.json`，以及 `final/report.audit.md` 的 Review notes 段。
6. 第 5 步六项通读的结论，有问题的请附原文片段。
7. 如果出现 `NEEDS_HUMAN_REVIEW.md`，附上全文。

我会根据这些判断三件事：annotate 是否继续作为默认、编辑计划是否值回它的成本、条目引用和跨节一致性的规则要不要调。然后再进入 P2。

---

## 七、附录：v83 基线（`eval_run.py 2026-11-03-v83`）

```
- verify attempts: 5  final verdict: fail ; NEEDS_HUMAN_REVIEW: True
- total: in 11,494,132 / cache 10,813,440 / out 299,458(state.json 口径;DB 核账口径约 20.6M)
- 台账合计 405 条:条目引用 18、整桶引用 218、带 entity+metric 0
- 出现在 ≥3 个 LLM 节的数字:11 个
  15b / 7.50% / 58b → drivers, sector_action, deep_dives, outlook
  13.4b / 97% / 17b / 16.2b / 80% → drivers, sector_action, deep_dives
  82% / 78% → drivers, fomc, outlook
- macro_review 44 字符(占位,却经历 4 轮修订共 1,056s)
```

---

## 八、v8.5：2026-11-04 真机评估后的四项修正

依据 `backend/reports/guanhe-daily-report/2026-11-04` 真机结果。离线回归（用该日真实数据）与 `--fake-zcode` 全流程均已通过，尚未真机跑。

### 1. 数据锚点裁断 + LLM high 有限修订（问题 1：Tesla 盘前 5.6% 当成收盘涨跌发布）

- `verify_checks.data_anchors()`：用当日收盘口径数据做参照。
  - 成分股 `change_percent`/`price`。
  - `cross_asset` 的 `pct_change_1d`/`close`（代码与前缀都认，SPX/WTI 等常见写法有别名）。
  - 美债只认 `par_curve`（`levels_pct`/`change_bp`），且按小数位严格比对：0.5% 的相对容差会让 4.89 混过 4.90。
- 跨节一致性的参照顺序：数据锚点 > 计划规范值 > 已核实值 > 多数节。有锚点时只有一节写也核。
  - `move` 偏离给出的建议是：改用收盘值，或注明盘前/盘中并把 metric 标成 `pm_move`。
  - `pm_move` 不参与一致性核验。
- 规则 11：`move` 限定为当日收盘对收盘。新增 `pm_move`；多日、年初至今的涨跌不标 metric。
- annotate 模式下 LLM 意见的处理（`_gate_llm_issues`）：
  - **能路由到可修订节的 high** 驱动**一轮**修订，并在 state 里记 `verify.llm_fix_used`。
  - 之后的核验只跑确定性检查，不再调 LLM；上一轮的 LLM 意见标 `revised once; not re-reviewed`，写进复核备注。
  - medium/low，以及落在锁定节或占位节的 high，仍只作备注。
- 回放 11-04 的结果：Tesla 5.6%（02 节）、AMD 盘前 7% 标成 move（06 节）、08 节 US10Y 4.89% 三条都判 high，没有新增误报。

### 2. 美债口径统一 + 07 前瞻口径与单位（问题 2、3）

- **全报告美债只用 `macro_pillars.par_curve`**：
  - 写作切片快照、plan_input 都剔除 `US2Y/US10Y/US30Y.INDX`。
  - 08、10、11 的白名单加入 Treasury curve。
  - 规则要求两位小数（4.90%）；10 节平静日渲染改为 `.2f`。
- 图表：FRED 缺 T 日时，用快照 `par_curve` 补 T 日值，历史仍取 FRED，脚注写明来源；两者都没有才回退 EODHD。
- 07 本周前瞻：
  - 同一事件多行时，按同比 > 环比 > 无口径挑选，同口径内有预期的优先。这样 CPI 不再取到指数行（337），显示为"同比 预期 3.7%"。
  - 输出标注同比/环比。
  - 版式改为表格（日期 | 时间(美东) | 地区 | 事件 | 预期 | 前值）：事件用中文名，数值换成中文量级（19.6万、156亿、1.5万亿），按美东日期分日，同一天内按时间排序。
  - 既无预期也无前值的讲话、会议、报告不列；同一发布跨日的重复行（EODHD 双时间戳）只保留一行；FOMC 行改为"下次 FOMC 议息：12月8–9日"。
- 单位：新增共享的 `prompts.annotate_event_units()`，build_snapshot 构建时调用，编排器也会给旧快照补上。
  - 结果：非农、初请写成 K；JOLTS、EIA 写成 M；贸易、经常账写成 B；外储按量级写 T 或 B；利率、通胀、口径行写成 %；PMI、指数不带单位。
  - 08 任务书和 plan_input 里的数字也带上单位，展望节看到的是同一套单位。

### 3. P21 快讯回填守卫（问题 4：西班牙 PMI 57.2 被回填成美国 Composite PMI）

- 必须有本国的正面标识。美国用大小写敏感的 `US`/`U.S.`/🇺🇸，并排除其他国家的名称和旗帜。
- 按 ` - `/`|`/换行切段，只在包含事件名和 est 标记的那一段里取数，避免取到同条快讯里 Services 的数。
- 快讯时间须在日历发布时刻的 [-5, +90] 分钟窗口内。
- actual 存为数值。
- 回放结果：午盘快照原来取 f178 → 57.2，现在取 f37 → 55.9；ISM 仍是 f39 → 54.5。

### 4. 编辑计划瘦身

- `PLAN_TIMEOUT_SEC` 从 600 降到 300。
- prompt 改动：明确只用附件、只写一次 plan.json、不读其他文件；去掉 evidence；故事 4–7 个；规范值不超过 12 条，每条只写一个数（不写"53356.12 (+3.03%)"）。
- `_load_plan` 截断：故事不超过 8 个，规范值不超过 12 条。

### 真机测试（在第四节流程上补充）

```bash
T=2026-11-05
# 新数据日跑全流程后：
grep -E "\[plan\]|LLM high issue|skipped: LLM fix round|rates:" run_$T.log
python3 tools/guanhe-daily-report/eval_run.py $T > eval_$T.md
grep -n "预期" backend/reports/guanhe-daily-report/$T/final/report.md | head    # 07 单位/口径
grep -rn "10年期\|10Y\|10 年" backend/reports/guanhe-daily-report/$T/sections/*.md # 美债应全篇一致
```

### 请反馈

1. plan 阶段墙钟和 token（`eval` 的 tokens by phase）。目标墙钟 < 300s，输入 < 150K。
2. 日志里是否出现 `LLM high issue(s) get one revision round`，以及修订后 audit 备注里的 `revised once` 条目是否确实已修好。
3. 各节的美债数字是否一致，图表脚注写的是哪个来源。
4. 07 前瞻的单位和口径，以及 `actuals_from_flash` 回填了哪些行（看 snapshot 的 `actual_source`）。
5. 有没有因 `move` 锚点产生的误报 high，例如把多日涨跌标成了 move。

---

## 九、借鉴参考报告（`reference: AI_report_from_8.md`）的四项改动

参考报告是持仓周报，持仓监控、Model Action 不适用；以下四项可用于晨报，均已实现，离线与 `--fake-zcode` 通过。

1. **传闻标记**
   - 上游 `evidence_level` 太粗，11-04 的 f59（"US Diplomatic Sources… - Al Jadeed"）被标成 E2。
   - 新增 `prompts.flag_reported_items()`：按措辞（sources / reportedly / people familiar / 消息人士 / 据报道 / 传闻 等）给 flash、wire、geo、digest、research 条目打 `reported` 标记。build_snapshot 构建时调用，编排器也会给旧快照补上。
   - 11-04 回放：flash 命中 f15、f59、f73、f184，geo 命中 g30、g33、g36；wire 无误报。
   - 规则：reported 条目只能写成「据…报道 / 消息人士称」；标题、推断、基准情景和建议不能只依赖它，它只能作为「若…属实」的翻转条件。plan_input 中这类条目标 `[reported]`，也不能作规范值。
   - LLM 轻量核验：传闻写成事实判 high；建议只依赖传闻判 medium。
2. **02 节事实与推断分离**
   - 事实 bullet 只写事实。
   - 每个故事的收尾 bullet 改为「**推断：**<机制判断>；<交易级动作>，若<事件 / 日期 / 价位>，则<如何调整>」，不超过 70 字。
   - 轻量核验：判断写进了事实 bullet，或收尾缺少「若…则…」，判 medium。
3. **07 本周前瞻加「看点」列，并合并财报**
   - 看点按事件族映射，确定性生成，例如「就业 → 美联储路径、美元」「外需 → 人民币」「国债供给 → 长端利率」。
   - 财报：取市值不低于 500 亿美元的 S&P 500 成分股，每天最多 2 家、合计最多 6 家，时间显示盘前或盘后；同一家公司出现暂定和确认两个日期时，取带时段的那条。
4. **07 新增「利率与汇率」小表**
   - 内容：美债 2 年 / 10 年 / 30 年（par curve 口径）、2s10s 利差及其 1 日变动、欧元/美元、美元/日元。
   - DXY 不在快照里，所以不列。

**真机请反馈**：
- 02 节每个故事的「推断」读起来是否有信息量，有没有变成套话。
- 09、11 节对 reported 条目的措辞。
- 07 表格长度是否合适（目前宏观事件每天最多 6 行，另加最多 2 家财报）。

---

## 十、v8.5-fix：plan 单发与调用重试（2026-11-05 下午，当日真机验证）

11-04 重测暴露三重 plan 失败（游荡 1.74M/11 请求、CLI 挂死零进展、300s 线低于经验分布 391/549s），同日修复：

1. **plan 单发**：`_plan_once` 复用 triage 模式——不带 `--cwd`、不挂附件，digest 内联进 prompt，输出 stdout JSON 由编排器校验后落盘 plan.json（agent 不写文件），失败重试 1 次（20s 退避），`PLAN_TIMEOUT_SEC` 300→600。真机实测 **1 请求 / 27.3K 输入 / 180.8s**，7 故事 owner 全合规——第八节反馈项 1 的两个目标（<300s、<150K）双双达成。
   - 已知待调优：canonical 快照核实通过率 4/12——单发 agent 只见 digest、路径凭记忆；数据锚点为主参照不影响发布。可给 digest 加合法 canonical 路径图例。
2. **`run_agent` 自动重试**：非零退出重试 1 次（20s 退避）；TimeoutExpired 维持文件契约。当日 2× Turn execution failed 均可被此兜住。
3. **第一段启动错峰 3s/agent**：削多个 CLI 同时起跑对 db.sqlite 的启动争夺。
4. **P17**：verdict 复用判定纳入 `.claims.json` mtime（修订只动台账时不再复用过期 verdict）。
5. **actual 数值统一**：`_clean` 装载强转 + 编排器旧快照补丁幂等强转（回放 16 行字符串→数值；flash 回填与 DB 原生两条路径类型一致）。

验证：AST×3、fake 全流程 rc=0、离线回放（P17 不复用断言、actual 强转）全过；11-04 全量重跑结果见 TEST-2026-11-04.md 五点三。P23（LLM-fix 修订轮漏修）的后查/回执方案留下一批。

---

## 十一、v8.6:标题中文化 + 三市场成交额榜(2026-11-05/06)

1. **章节标题全中文化**(01-06 节与 07-11 统一):大盘总览/今日叙事/核心驱动/
   持仓与资金流/美股全景/板块动态/今日涨跌榜/个股深挖;副标题
   `*GuanHe 每日晨报 — 日期*`。三处强耦合同步:写作任务书、`verify_checks.
   HEADINGS`、assemble 图表定位(`startswith("## 板块动态")` 等)。
2. **今日涨跌榜改三市场成交额榜**:美股(finviz 成分股 price×volume,
   ≥$10亿)/港股(EODHD bulk ≥10亿港元)/A股(沪深合并 ≥20亿元),各前 8,
   列=代码/公司/收盘价/涨跌幅/成交额。Stage 0 冻结进 `market.volume_leaders`,
   渲染器只读。要点:bulk 按 T 显式取数;symbol-list Type=Common Stock 过滤
   指数/ETF;`0700`/`700` 与 `-OL` 去重;涨跌幅取 T 与前一交易日两次 bulk
   收盘价;展示码 SHG/SHE→SH/SZ;SECTION_ORDER 上移涨跌榜至板块动态前
   (文件名 05_movers.md 不动,续跑契约不破)。
3. **A/港股中文名**:腾讯行情接口一条请求批量补 `name_cn`(GBK;免密钥),
   失败回退英文;价格数字仍以 EODHD 冻结数据为准。
4. **finviz 盘中残片防护**:S&P500 总量 <20亿股判不完整,拒绝降级
   (盘中重建快照时当日行已被开盘后刷新覆盖)。
5. **P25/P26 核验器修复**:见 TEST 六。P25(.text 后缀误报)稳定版同源未修。

---

## 十二、v8.7:核验超限改为降级发布(2026-11-06)

按用户决定,核验不通过不再 rc=2 拦截转人工:轮次用尽或问题只涉锁定节时
**照常发布**,未决问题自动标注 —— 成品顶部一行指针(只数未决项)+
文末「⚠️ 核验备注」清单(severity/章节/问题/建议),advisory 意见仍只进
report.audit.md。旧拦截行为由 `--strict-verify` 恢复。assemble 顶部图表
插入点随之从固定下标 4 改为"正文首个 ## 标题前"(指针行会移动插入点)。

---

## 十三、每日期刊式流程卡(2026-11-06)

`gen_pipeline_card.py` + `pipeline_card_template.html`:发报成功后由 run_pipeline
自动调用(纯脚本、不调 LLM、不连库),把当日 snapshot/state/claims/check_results/
运行日志里的实测数据注入数据驱动模板,产出 `{ws}/pipeline-overview.html`——
产品全景(四步编辑部流程+实测徽标)、运营实测(原料清单/投入产出/栏目用量/
outlook 生成逻辑/质检漏斗)、数据流向桑基图(Admin 订阅细化到星球/推特/
substack/youtube/新闻)、栏目溯源、质检与发版五页。模板自带主题与标签页
(独立 HTML 零依赖);日志按信息量评分挑选(修订轮记录最多的一份),漏斗
缺中间记录时退化为终态展示;生成失败只提示不阻断发报。
