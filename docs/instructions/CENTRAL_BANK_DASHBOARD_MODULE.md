# 央行 Dashboard 模块（Fed FOMC）开发说明

> **创建**：2026-08-20
> **需求来源**：`docs/DASHBOARD_REQUIREMENTS.md` 3.3 央行模块（2026-08-19 会议）
> **状态**：V1 + V2（概率模块）已完成开发并实测通过（仅 Fed）
> **相关文档**：`FED_MODULE_USER_GUIDE.md`（Fed 模块使用手册；**第 3 节是全部数字的网页溯源**，面向投研同事手算复核）、`CENTRAL_BANK_PRICED_IN_GUIDE.md`（Priced in / 概率趋势短说明）、`LLM_ARCHITECTURE_OVERVIEW.md`（模板体系）、`SCHEDULER_DEVELOPMENT_GUIDE.md`（调度器）

---

## 1. 模块概述

从 federalreserve.gov 官网抓取 FOMC 利率决议声明与会议纪要原文，regex 结构化抽取利率决议（区间/票型/异议者），并通过 LLM 生成中文五段式概括；轮询 Fed 官方 RSS 维护 updates 流；基于 ZQ 30-Day Fed Funds 期货自算各会议加息/降息概率并每日快照。前端 `/central-bank` 页面四卡片展示。

**四条数据类型的分工**（模块设计核心）：

| 类型 | 来源 | 层 |
|---|---|---|
| 结构化数值 | Fed 原文 regex 抽取（主）+ EODHD `economic_events` 交叉验证 | `fomc_decisions` |
| 一手文本 | 声明 ~200 词 / 纪要 ~5,400 词 | `fomc_documents` |
| 目录/通知 | `press_all.xml` RSS（title/link/pubDate/category） | `fed_updates` |
| 市场预期 | ZQ 期货价 + EFFR 自算概率（时序快照） | `fomc_probabilities` |

**关键背景**（为什么这样设计）：

- Fed 官网**无官方 JSON API**，声明 URL 前缀（`monetary`）无规律，**RSS 是唯一可靠的文档发现渠道**
- 纪要 URL 有固定模式 `fomcminutes{会议末日YYYYMMDD}.htm`（已验证 2021–2026 稳定）
- 纪要发布滞后会议 3 周
- EODHD 只有数值（3.75），**没有**投票结构/异议者/措辞变化——这些信息增量只在原文里

---

## 2. 数据链路

```
RSS (press_all.xml) 每 30 分钟轮询
  → poll_fed_updates()         发现声明/纪要 → fomc_documents(pending)
  → fetch_document()           bs4 清洗 → fomc_documents(fetched)
  → extract_decision()         regex 抽取（仅 statement）→ fomc_decisions
  → generate_summary()         LLM 概括 → fomc_summaries
```

四张表（`migrations/create_fomc_dashboard.sql`，全部带 `central_bank` 字段默认 `'FED'` 预留央行扩展）：

| 表 | 职责 | 幂等 key |
|---|---|---|
| `fomc_documents` | 原文层（不可变） | `(central_bank, source_url)` |
| `fomc_decisions` | 结构化抽取层（regex 可升级重跑） | `(central_bank, meeting_date)` |
| `fomc_summaries` | LLM 概括层（模板可升级重生成） | `(document_id, template_id)` |
| `fed_updates` | RSS 流 | `(central_bank, guid)` |

**拆表 rationale**：原文/抽取/摘要三者生命周期不同（原文不可变、regex 规则升级需重跑、LLM 模板升级需重生成），分表可独立失效重算，避免大 TEXT 互相污染更新。

**溯源字段**：所有表带 `source_url / published_at / fetched_at / content_hash`；`fomc_summaries` 另有 `template_id / model / prompt_tokens / completion_tokens / prev_meeting_date`——前端 AI 卡片展示完整加工链，且可展开对照原文。

---

## 3. 文件清单

### 新增（8 个）

| 文件 | 职责 |
|---|---|
| `backend/database/migrations/create_fomc_dashboard.sql` | 四表 DDL（幂等 IF NOT EXISTS） |
| `backend/database/migrations/insert_fomc_summary_template.sql` | LLM 模板 `fomc_summary_v1` 种子（ON CONFLICT 可迭代） |
| `backend/database/migrations/add_fed_dashboard_scheduler_task_type.sql` | 调度类型 `fed_dashboard` + 每 30 分钟 cron 默认行 |
| `backend/services/fed_dashboard_service.py` | 核心服务（抓取/清洗/抽取/概括/API 组装，~1000 行） |
| `backend/tasks/fomc_tasks.py` | Celery 任务链（5 任务） |
| `backend/api/fomc.py` | REST 端点（4 个） |
| `web-frontend/src/services/fomc.ts` | 前端 API client（class + 单例） |
| `web-frontend/src/pages/CentralBankDashboardPage.tsx` | 页面（四卡片） |

### 修改（8 个）

| 文件 | 改动 |
|---|---|
| `backend/database/db_manager.py` | `_create_fomc_tables()` 挂 init；task_type 白名单加 `fed_dashboard` |
| `backend/services/template_preference_service.py` | `SYSTEM_DEFAULTS` + `SYSTEM_ONLY_MODULES` 注册 `fomc_summary` |
| `backend/celery_config.py` | task_routes 加 5 个显式任务名 |
| `backend/celery_app.py` | 导入 `fomc_tasks` 模块 |
| `backend/main.py` | include_router `/api/fomc` |
| `backend/services/scheduler_service.py` | `task_handlers` 注册 + `_execute_fed_dashboard_task` |
| `backend/api/scheduler.py` | 两处 Field pattern 白名单 |
| `web-frontend/src/App.tsx` + `components/Layout.tsx` | 路由 `/central-bank` + 菜单 "Central Banks"（BankOutlined） |

---

## 4. 核心组件

### 4.1 Service：`FedDashboardService`

**抓取链**（data_fetching 队列）：

- `create_robust_session()` — 复制 `economic_events_service.py:384` 模式（Retry total=3、backoff、status_forcelist=[429,500,502,503,504]、SSL 重建、timeout=(10,30)）
- `safe_get(url)` — **强制 UTF-8 解码**（`response.content.decode("utf-8")`）。⚠️ Fed 页面 HTTP 头无 charset，`response.text` 会误判 latin-1，en-dash 变 mojibake 导致票型 regex 失配——实测踩坑
- `poll_fed_updates()` — feedparser 解析 RSS → 幂等写 fed_updates + `_discover_fomc_document()`。过滤条件：category 含 'Monetary Policy' **或** title 含 FOMC/interest rate 等关键词（并集防 category 缺失）
- `fetch_document(doc_id)` — bs4 清洗，算 `content_hash`（sha256），**hash 未变跳过下游**（省 LLM 调用）
- `extract_decision(doc_id)` — regex 抽取（见 4.2），失败字段落 NULL + error_message 不抛错（卡片降级为原文链接）

**HTML 清洗规则（extraction_method = `bs4_v1`）**：起点锚 `For release at`（声明）/ `Minutes of the Federal Open Market Committee`（纪要）；终点截断 `Last Update`；剔除 script/style/nav。Fed 改版后清洗失败 → `fetch_status='failed'` → 卡片降级原文外链 + 兜底重试；规则升级改 `bs4_v2` 可批量重抓。

**LLM 链**（ai_processing 队列）：

- `generate_summary(doc_id, force)` — 照 `cnbc_closing_bell_service.py` `_call_llm` 范本：`llm_service.chat_completion_with_template(template_code='fomc_summary_v1', variables=..., timeout=SINGLE_LLM_TIMEOUT_SEC)`；SINGLE_LLM_RETRY 重试；幂等（completed 且非 force 跳过）
- **纪要 5,400+ 词策略性截断**（`_truncate_for_llm`，~24k 字符上限）：保留头部决议段+Staff Review（8k）+ 尾部 participants 观点（16k），中间插省略标记。不做两级摘要（丢信息且翻倍成本）

**API 组装**（同步）：`get_dashboard()` / `get_document_detail()` / `get_updates()` / `_eodhd_cross_check()`（从 `economic_events` 取同日 `'Fed Interest Rate Decision'` 事件对拍；⚠️ `date` 列是 text 类型需 `::text` 比较）

### 4.2 结构化抽取 regex（regex_v1）——实测踩坑记录

**⚠️ Unicode 归一化是第一道工序**（`_normalize_text`）：老声明（2026-01~04）用 U+2011 non-breaking hyphen（`3‑1/2`），新声明用 ASCII hyphen/en-dash；smart quotes 同理。归一化表见 service 头部 `_HYPHEN_VARIANTS`，抽取前统一转 ASCII。

| regex | 句式 | 备注 |
|---|---|---|
| `RE_RATE_RANGE` | `maintain/raise/lower the target range ... at 3-1/2 to 3-3/4 percent` | 分数记法 `_frac_to_float`（`3-1/2`→3.5）；兼容 `lower ... by 1/4 percentage point, to X to Y percent` |
| `RE_VOTE_SPLIT` | `by a 9-3 vote` | 新声明（2026-06 起） |
| `RE_VOTE_FOR` | `Voting for this action were A, Chair; B, Vice Chair; ...` | **老声明用分号+头衔**，`_split_names` 兼容分号/逗号两种分隔并剥离头衔；终止锚必须防中名缩写（`John C.`）截断 |
| `RE_DISSENT_BLOCK` + `RE_DISSENT_GROUP` | 多组异议：`A, who preferred to lower ...; and B and C, who supported ... but did not support inclusion of an easing bias` | **4 月出现双向异议**（dovish+hawkish 各一组）；组分隔符 `; and` 必须消费否则名单混入前导符 |

方向判定：`preferred to raise/increase` → hawkish；`lower/reduce/cut` → dovish；`easing bias / did not support inclusion` → hawkish（修辞异议）；无法判定 → unknown（保留票数）。

**抽取结果实例**（2026 年时间线，验证于 2026-08-20）：

| 会议 | 票型 | 异议 |
|---|---|---|
| 1/27 | 10:1 | Waller (dovish) |
| 3/17 | 11:1 | Miran (dovish) |
| 4/28 | 8:4 | Miran (dovish) + Hammack/Kashkari/Logan (hawkish) |
| 6/16 | 12:0 | 无 |
| 7/28 | 9:3 | Hammack/Kashkari/Logan (hawkish) |

### 4.3 Celery 任务（`backend/tasks/fomc_tasks.py`）

| 任务 | 队列 | 职责 |
|---|---|---|
| `fomc_poll_updates` | data_fetching | 轮询 RSS + 发现 + 派发抓取 + 兜底重试 |
| `fomc_fetch_document` | data_fetching | 单文档抓取+抽取 → 链式派发概括（内容有变化时） |
| `fomc_summarize` | ai_processing | LLM 概括（async 经 `_run_async` 包装） |
| `fomc_retry_pending` | data_fetching | 兜底重试 failed/pending + 补派概括 |
| `fomc_backfill_2026` | data_fetching | 历史回填（5 声明 URL 硬编码 + 5 纪要 URL 构造） |

跨队列派发用 `celery_app.send_task(name, kwargs)`（不用 canvas）。

### 4.4 LLM 模板 `fomc_summary_v1`

- **注册**：SYSTEM_FIXED（`template_preference_service.py` 的 `SYSTEM_DEFAULTS` + `SYSTEM_ONLY_MODULES`，全局共享非 per-user）
- **变量**：`doc_type` / `title` / `meeting_date` / `previous_decision`（上次区间/票型，供对照）/ `original_text`
- **输出**：中文 Markdown 固定五段——`## 决议` `## 投票与分歧` `## 经济评估` `## 前瞻信号` `## 措辞变化 vs 上次`
- **参数**：temperature 0.20，max_tokens 2000，timeout 走 `SINGLE_LLM_TIMEOUT_SEC`(600s)
- **Prompt 要点**（写进 system prompt）：密度词如实转译（many/several/a few 是判断委员会倾向的关键信号）；区分事实 vs 解读；未提及写"未提及"禁止编造；≤1200 字；人名/专有名词保留英文

已知无害 warning：`prompt_renderer` 对 `output_format: {"type": "markdown"}` 报 "Unknown output format type" 按 text 处理——不影响输出。

### 4.5 API（`backend/api/fomc.py`，prefix `/api/fomc`，全部 `Depends(get_current_user)`）

| 端点 | 用途 |
|---|---|
| `GET /dashboard` | 聚合：decision（含 previous 对照 + eodhd_cross_check）/ statement_summary / minutes_summary / recent_meetings(8) / updates(20) / probabilities(恒 null) |
| `GET /documents/{id}` | 原文+概括（AI 卡片展开对照，前端懒加载） |
| `GET /updates?page=&page_size=&monetary_only=` | updates 流分页 |
| `POST /refresh` | 派发 Celery（type: poll/fetch/summarize/backfill/retry），立即返回 task_id |

### 4.6 前端页面（`CentralBankDashboardPage.tsx`）

四卡片：**RateDecisionCard**（区间/票型/异议 tag——红=鹰🦅 蓝=鸽🕊/vs 上次/EODHD 交叉验证徽章：绿=一致 黄=mismatch 灰=n/a + Fed 原文外链）→ **AI Summary Tabs**（statement|minutes，MarkdownRenderer 渲染 + AI 加工链溯源行 + Collapse 懒加载原文对照）→ **FedUpdatesFeed**（antd List + 分页 + Monetary Policy only 开关）→ **ProbabilitiesPlaceholder**（Empty 占位「数据源调研中」）。

- generating 态 5s 轮询（上限 60 次），照 `useDailyReportStatusPoll` 模式
- 主题：`useDashboardSettings()` + `THEME_COLORS[themeMode]` inline style（照 EconomicEventsPage）
- 文案英文为主（仓库无 i18n）

---

## 5. 调度配置

- **task_scheduling 行**：`type='fed_dashboard'`，cron `*/30 * * * *`（UTC），`params: {"mode": "poll"}`
- **频率 rationale**：Fed 是低频源（8 会议/年），30 分钟粒度对"当天可见"足够，RSS 请求仅 ~14KB
- **mode**：`poll`（默认全链）/ `retry`（仅兜底）
- 新增任务类型需改四处（本模块已全部完成）：db_manager CHECK 白名单、migration SQL、scheduler_service handlers、api/scheduler.py 两处 pattern

**自愈机制**：纪要未发布时（如 2026-07 纪要 8/19 前后发布）回填留下 `failed` 行，后续每次轮询自动重试，Fed 挂出页面后链路自动走完，无需人工。

---

## 5.5 概率模块（V2，zq_v1 方法）

### 方法论

基于 **30-Day Fed Funds 期货（CME 代码 ZQ）** 自算各 FOMC 会议的加息/降息概率——与 CME FedWatch 同原料同公式，绕开其付费 API。核心恒等式：`合约价 = 100 − 合约月日均 EFFR 预期`。

```
implied = 100 − price                      # 合约隐含月均利率
hold = base_rate                           # 该会议不动（整月旧利率）
cut  = (base*d_pre + (base−0.25)*d_post) / 当月天数
hike = (base*d_pre + (base+0.25)*d_post) / 当月天数
概率 = implied 在情景间的线性插值，clip [0,1]；越界 → overflow 标记
```

三个关键设计（均为实测踩坑结论）：

- **基准利率用 EFFR 不用区间中值**：EFFR 常年偏离区间中值几 bp（实测 3.63 vs 3.625），直接决定概率方向。EFFR 取自 FRED（`EFFR` 序列，`FRED_API_KEY`），失败降级区间中值并标记 `rate_basis`
- **逐会议递推基准**：远月合约定价的是**累计路径**。首会议基准=EFFR，后续会议基准=前一次会议的**会后隐含利率** `post_rate = (implied*N − base*d_pre)/d_post`（不是合约月均 `implied_rate`）。用月均递推会把已定价的加息重复算进下一次会议（实测 10 月会从 ~50% 虚高到 ~98%）
- **合约选择规则**：`d_post = 当月天数 − 决议日 ≥ 10` 用当月合约，否则用下月合约（月末会议当月合约只剩 1-4 天权重不敏感；此时下月合约 `d_pre=0` 整月都是会后利率）。月码 `FGHJKMNQUVXZ`，symbol 如 `ZQU26.CBT`

### 数据源（全部实测 2026-08-20）

| 源 | 用途 | 状态 |
|---|---|---|
| Yahoo v8 chart `/finance/chart/ZQxxYY.CBT` | 在市月合约日 K | ✅ 近月齐全；**已到期合约 delisted 无数据** |
| Yahoo `ZQ=F` 连续合约 | 历史回填（当月段≈当月合约） | ✅ 2026 全年覆盖 |
| FRED `EFFR` | 日频基准利率 | ✅ `observation_start/end` 取历史 |
| EODHD | — | ❌ ZQ 品种 NA，放弃 |

### 两张表（`migrations/create_fomc_probabilities.sql`）

- `fomc_meetings`：会议日历（`fomccalendars.htm` 同步，2021–2027）。⚠️ 语义：`meeting_date`=**会议末日**（决议/声明发布日），与 `fomc_documents` / `fomc_decisions` / `fomc_probabilities` 的 `meeting_date` 对齐。`first_day`=会议首日（日历 `15-16` 的 15，仅展示/溯源）。`is_sep`（日历 `*` 后缀）、`is_notation`（"notation vote" 无决议，排除）。解析注意：年份锚面板顺序 2026,2025,…,**2027 在页尾**，切段须按全部锚排序取下一段；"30-1" 跨月日期取末日进位下月
- `fomc_probabilities`：每日快照。`UNIQUE(central_bank, meeting_date, asof_date)`；`p_cut/p_hold/p_hike` 冗余列（折线直查）+ `distribution` JSONB（含 overflow 标记）+ 溯源字段（合约代码/价格/EFFR/method）。`method`: `zq_v1`（在市合约）/ `zq_front_v1`（连续合约回填，分辨率 ±10-15pp）

### 任务与调度

- `fomc_compute_probabilities`（data_fetching）：同步日历 → 未来 4 次会议 → Yahoo+EFFR → upsert。**按日节流**（`_maybe_daily_probabilities`）；`force=True` 供手动刷新
- `fomc_backfill_probabilities`（data_fetching）：一次性回填。ZQ=F 全量日 K + EFFR 日序列各 1 请求 → 有决议记录的历史会议 × 会前 45 天窗口
- **调度设计**：`task_scheduling` 有 `UNIQUE(type)` 约束（一 type 一行）——概率**不单设调度行**，由 `fomc_poll_updates`（*/30min）末尾顺带派发，service 内按日节流保证每天只算一次。scheduler 手动触发入口：`fed_dashboard` 任务 `params.mode='probabilities'`

### API

- `GET /dashboard` 的 `probabilities` 字段：`asof_date` + `next_meetings[]`（最新快照+30 点内联 trend）+ `expectations_vs_actual[]`（LATERAL 取会前最后快照 vs 实际决议，hit 布尔）+ `method_note`
- `GET /probabilities?meeting_date=&days=90`：单会议完整趋势
- `POST /refresh` 新增 type：`probabilities`（force 快照）/ `probabilities_backfill`

### 前端（ProbabilitiesCard）

未来 4 次会议分布横条（🕊cut 鸽蓝/hold 灰/🦅hike 鹰红，纯 div 宽度）+ 倒计时/SEP 标记 + 会议 Select 切换 recharts 三线走势图（明暗主题适配）+ 预期差对照表（✓hit/✗miss）+ 溯源脚注（合约/EFFR/method/与 FedWatch 偏差声明）。Refresh 按钮并发派发 `poll` + `probabilities`。

### 已知边界

- 与 CME FedWatch 有几 pp 稳定偏差（25bp 步长假设、忽略季末效应）——卡片脚注声明
- 回填 method=`zq_front_v1` 分辨率弱（连续合约当月段近似），月末会议 ±10-15pp；method 字段区分，前端 hover 提示
- 2027 远月合约 Yahoo 未挂牌时该会议显示 pending（no_contract 跳过不报错）
- overflow（市场定价 ≥50bp）clip 到边界 + 标记，不静默

---

## 6. 运维操作

```bash
# 手动触发（等价前端 Refresh 按钮）
curl -X POST /api/fomc/refresh -H "Authorization: Bearer $TOKEN" \
     -H "Content-Type: application/json" -d '{"type": "poll"}'

# 重新生成某文档概括（模板升级后）
curl -X POST /api/fomc/refresh -d '{"type": "summarize", "document_id": 1, "force": true}'

# 回填历史（幂等，已 fetched 的跳过）
curl -X POST /api/fomc/refresh -d '{"type": "backfill"}'
```

**模板迭代**：直接改 `insert_fomc_summary_template.sql` 重新 psql 应用（ON CONFLICT DO UPDATE），再对目标文档 force summarize。新旧模板概括按 `(document_id, template_id)` 并存。

**regex 规则迭代**（Fed 改句式时）：改 service 中 regex → `extraction_method` 升 `regex_v2` → 对 statement 文档重跑 `extract_decision`（ON CONFLICT 覆盖）。

**数据质检 SQL**：

```sql
-- 抽取缺字段检查（error_message 非空 = 有字段没抽到）
SELECT meeting_date, vote_for, vote_against, error_message
FROM fomc_decisions ORDER BY meeting_date DESC;

-- 概括状态
SELECT d.meeting_date, s.doc_type, s.status, s.model, length(s.summary_content)
FROM fomc_summaries s JOIN fomc_documents d ON d.id = s.document_id
ORDER BY d.meeting_date DESC;

-- 三源交叉验证（Fed 抽取 vs EODHD）
SELECT fd.meeting_date, fd.decision_date, fd.rate_high_bp AS fed_bp,
       ee.actual AS eodhd_pct, ee.forecast, ee.previous
FROM fomc_decisions fd
LEFT JOIN economic_events ee
  ON ee.country='US' AND ee.event='Fed Interest Rate Decision'
 AND ee.date = fd.decision_date::text
ORDER BY fd.meeting_date DESC;
```

---

## 7. 已验证事项与遗留

**已实测验证**（2026-08-20）：

- RSS 轮询→发现→抓取→抽取→概括全链路端到端
- 2026 五次会议决议抽取全部正确（含 4 月双向异议多组解析）
- 9 份文档 LLM 概括全部 completed（qwen3-max），五段结构齐全，密度词转译如实
- EODHD 交叉验证：3.75 三源一致（Fed 原文 / regex 抽取 / economic_events）
- migration 幂等（重复执行验证过）
- 前端 `npm run build` 通过

**V2 概率模块已实测验证**（2026-08-20）：

- 会议日历同步：2021–2027 共 55 行（跨月 "30-1" 进位正确、notation vote 排除、SEP 标记无误）
- 在市快照：9/16 → 34% hike / 66% hold（ZQU26 + EFFR 3.63，与调研期手算一致）；远月递推后 30-39% hike（修复了不递推导致 100% 加息的 bug）
- 回填：5 会议 × 84 快照（ZQ=F 当月段 + EFFR 日序列）
- 预期差对照：2026 五次会议 5/5 hit（市场隐含 hold = 实际 hold）
- 三端点 curl 通过（dashboard / probabilities 趋势 / refresh 派发）
- 前端 build 通过，dev bundle 含概率卡

**遗留 / 后续**：

| 项 | 说明 |
|---|---|
| PBoC / BOJ | 表结构已预留 `central_bank` 字段；需调研两国央行官网 RSS/抓取管道后按同模式扩展 |
| 7 月纪要 | 发布后由 30 分钟调度自动发现（留有 pending/failed 行自愈）——首个全自动增量验证点 |
| 9 月会议 | 9/15–16；声明发布当日调度自动捕获；概率走势随每日快照积累 |
| EODHD 事件名依赖 | `'Fed Interest Rate Decision'` 非契约，EODHD 改名仅交叉验证列显示 n/a（主数据来自 Fed 原文，不阻塞） |
| 生产部署 | 需重启 backend / celery worker / scheduler 使新代码生效；migrations（create_fomc_dashboard.sql / insert_fomc_summary_template.sql / add_fed_dashboard_scheduler_task_type.sql / create_fomc_probabilities.sql）需 psql 应用（本机已应用） |
