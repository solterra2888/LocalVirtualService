# FOMC 模块 · 每 30 分钟 Scheduler 触发的全流程（含分支场景）

> **创建**：2026-08-20
> **配套文档**：`CENTRAL_BANK_DASHBOARD_MODULE.md`（模块完整设计）、`SCHEDULER_DEVELOPMENT_GUIDE.md`（调度器）
> **用途**：理解 `task_scheduling` 行 `fed_dashboard`（cron `*/30 * * * *`）每次触发后，数据如何流经各服务并落库——**特别标注各分支情况（如 Fed 无新文档时在哪停、写哪张表）**

---

## 一、Mermaid 全流程图（决策分支版）

```mermaid
flowchart TB
    %% ═══ 调度层 ═══
    subgraph SCHED["⏰ 阶段⓪ 调度层 · Scheduler 服务 :8001 · cron */30 * * * *"]
        TS[("🗂 task_scheduling<br/>type=fed_dashboard mode=poll")]
        TS --> APS["_execute_fed_dashboard_task<br/>APScheduler 唤醒"]
        APS --> LOCK{"PG 周期窗口锁<br/>本窗口已执行过？"}
        LOCK -->|"是（重复触发/重启）"| SKIPX["⛔ 跳过防重<br/>什么都不做"]
        LOCK -.->|"INSERT ON CONFLICT 留痕"| TEL[("🗂 task_execution_logs<br/>执行日志 ✓每窗口必写")]
        LOCK -->|"否"| DISP["Celery 派发<br/>fomc_poll_updates"]
    end

    DISP --> Q1[["Celery · data_fetching 队列"]]

    %% ═══ 阶段① 轮询 ═══
    subgraph P1["阶段① RSS 轮询 · 每次窗口都跑"]
        Q1 --> POLL["fomc_poll_updates_task"]
        RSS[["🌐 federalreserve.gov<br/>feeds/press_all.xml"]] -->|"GET ~14KB"| POLL
        POLL -->|"① 全部条目 upsert（无新条目则 0 行）"| FU[("🗂 fed_updates<br/>RSS 新闻流 ✓每次必写*")]
        POLL --> NEWDOC{"② RSS 里出现<br/>声明/纪要 URL 模式？"}
        NEWDOC -->|"否 ★最常见（95%窗口）<br/>只有普通新闻/无条目"| RETRY
        NEWDOC -->|"是（决议日/纪要日/页面修正）"| FD[("🗂 fomc_documents<br/>新增行 status=pending<br/>✓仅此场景写")]
        RETRY["③ 派发 fomc_fetch_document<br/>pending + failed"]
        RETRY --> HASFAILED{"有 failed/pending<br/>文档？"}
        HASFAILED -->|"无"| HEAL
        HASFAILED -->|"有（纪要未挂网 404 等）"| FETCH
        FD --> Q2["派发 fomc_fetch_document"]
        Q2 --> FETCH
        RETRY --> HEAL["③b 补派发 fomc_summarize<br/>fetched 但无 completed 概括"]
        HEAL --> Q3["④ 派发 fomc_compute_probabilities<br/>（无论有无新文档都派发）"]
    end

    %% ═══ 阶段② 抓取 ═══
    subgraph P2["阶段② 文档抓取链 · 仅当有 pending/failed 文档"]
        FETCH["fomc_fetch_document_task"]
        HTML[["🌐 federalreserve.gov<br/>声明/纪要页面"]] -->|"GET"| FETCH
        FETCH --> FETCHOK{"抓取+清洗<br/>成功？"}
        FETCHOK -->|"否（404 页面未挂/网络）"| FDFAIL[("🗂 fomc_documents<br/>status=failed<br/>→ 等下个窗口自愈重试")]
        FETCHOK -->|"是"| HASH{"content_hash<br/>与上次比变了？"}
        HASH -->|"没变且已有 completed 概括<br/>⛔ 跳过 LLM 省 token"| ENDS2["该文档链路结束"]
        HASH -->|"变了，或仍无 completed 概括"| FDU[("🗂 fomc_documents<br/>status=fetched 全文入库 ✓")]
        FDU --> ISSTMT{"doc_type 是<br/>statement？"}
        ISSTMT -->|"是"| EXTRACT["extract_decision<br/>纯 regex 零 LLM<br/>区间/票型/异议者/change_bp"]
        EXTRACT --> DEC[("🗂 fomc_decisions<br/>✓仅声明写")]
        ISSTMT -->|"否（minutes）<br/>跳过抽取直奔概括"| Q4
        DEC --> Q4["派发 fomc_summarize"]
    end

    %% ═══ 阶段③ AI 概括 ═══
    subgraph P3["阶段③ AI 概括 · ai_processing 队列 · 约1-2分钟"]
        Q4 --> SUM["fomc_summarize_task"]
        DEC -.->|"上次决议做对照上下文"| SUM
        LLM[["🤖 qwen3-max<br/>模板 fomc_summary_v1 五段式中文<br/>temp 0.2 · 纪要截断~24k字符"]] --> SUM
        SUM -->|"generating→completed<br/>template/model/token 留痕"| FS[("🗂 fomc_summaries<br/>✓仅 hash 变化时写")]
    end

    %% ═══ 阶段④ 概率快照 ═══
    subgraph P4["阶段④ 概率快照 · 每天首个窗口实质执行，其余 no-op"]
        Q3 --> THRO{"今日已有快照？<br/>（查 fomc_probabilities）"}
        THRO -->|"是 ★当天第2~48个窗口<br/>⛔ no-op 返回"| NOOP["结束"]
        THRO -->|"无（每天首个窗口）"| CAL["同步会议日历"]
        CALPG[["🌐 federalreserve.gov<br/>fomccalendars.htm"]] --> CAL
        CAL --> CALCHG{"日历有变化？"}
        CALCHG -->|"无（日年变一次）"| COMP
        CALCHG -->|"有（Fed 发次年日历）"| FM[("🗂 fomc_meetings<br/>✓仅变化时写")]
        FM --> COMP
        FRED[["🌐 FRED · EFFR"]] -->|"基准利率<br/>失败→降级 decisions 区间中值"| COMP["compute_probabilities<br/>未来4次会议 · 逐会议递推"]
        YAHOO[["🌐 Yahoo · ZQU26/ZQX26/ZQZ26/ZQG27"]] -->|"期货收盘价"| COMP
        COMP --> YHA{"合约取到价？"}
        YHA -->|"某月合约缺失<br/>该会议标 no_contract 跳过"| COMP
        YHA -->|"取到"| FP[("🗂 fomc_probabilities<br/>每日一行 ✓")]
    end

    %% ═══ 阶段⑤ 前端 ═══
    subgraph P5["阶段⑤ 前端读取 · GET /api/fomc/dashboard · 与调度异步、按需查询"]
        FE["CentralBankDashboardPage<br/>/central-bank"] --> API["get_dashboard 聚合"]
        API --> CARD1["① 决议卡<br/>区间/票型/异议"]
        API --> CARD2["② AI 概括卡<br/>声明|纪要 Tabs"]
        API --> CARD3["③ Fed Updates 流"]
        API --> CARD4["④ 概率卡<br/>横条+走势+预期差"]
        DEC --> CARD1
        EE[("🗂 economic_events<br/>EODHD 已有")] -.->|"交叉验证 ✓ 徽章"| CARD1
        FS --> CARD2
        FD -.->|"展开懒加载原文对照"| CARD2
        FU --> CARD3
        FP --> CARD4
        DEC -.->|"Actual 列 ✓hit/✗miss"| CARD4
    end
```

## 二、读图要点

| 节点形状 | 含义 |
|---|---|
| 圆柱 `🗂` | PostgreSQL 表（**实线箭头=写入，虚线=读取**；`✓` 标注该表在什么条件下被写） |
| 双线框 `🌐/🤖` | 外部服务（Fed 官网 / FRED / Yahoo / LLM） |
| 菱形 | 决策分支——**每条出线都标了条件**，`⛔` 表示提前结束/跳过 |
| `★` | 高频路径（绝大多数窗口走的分支） |

## 三、四大分支场景详解（对应图中菱形）

### 场景 A：Fed 无任何新文档（占 95% 的窗口）★日常态

```
窗口触发 → RSS 拉一遍
  ├─ fed_updates: 普通新闻条目有则 upsert，无则 0 行
  ├─ NEWDOC 判定"否" → 不产生 pending 文档 → 阶段②③完全不发生
  ├─ HASFAILED 判定"无" → 不重试
  └─ 概率: 当天首个窗口会实质执行；其余窗口 THRO 判"是" → no-op

实际开销: 1 个 HTTP 请求 + 少量 upsert，秒级结束
```

### 场景 B：决议日/纪要日（新文档出现）

```
RSS 发现声明或纪要 URL → fomc_documents(pending)
  → 派发抓取 → bs4 清洗 → hash 必然是新的（新文档）
  → statement 走 regex 抽取写 fomc_decisions；minutes 跳过
  → 派发 LLM → fomc_summaries（前端 generating 态 5s 轮询约 1-2 分钟）
```

### 场景 C：Fed 重新挂网但内容没变（hash 相同）

```
抓取成功 → content_hash 比对 → 没变
  → ⛔ 跳过 extract_decision 和 LLM 概括
  → 不写 fomc_decisions / fomc_summaries（token 零消耗）
  → fomc_documents 也只是状态刷新，全文不重复处理
```

### 场景 D：纪要尚未挂网（404，会后 3 周内的过渡期）

```
纪要 URL 按固定模式提前入库 pending → 抓取 404
  → fomc_documents 标 status=failed
  → 之后每个窗口派发 fomc_fetch_document（pending + failed 同一条链）
  → Fed 挂出页面那一刻走完抓取 + 概括，无需人工
  → 若曾漏派发概括：poll 还会对 fetched 且无 completed summary 的文档补派发
```

## 四、表与写入时机速查（按场景对号）

| 表 | 场景 A（无新文档） | 场景 B（新文档） | 场景 C（hash 未变） | 场景 D（404） |
|---|---|---|---|---|
| `task_execution_logs` | ✓ 每窗口 | ✓ | ✓ | ✓ |
| `fed_updates` | ✓（有新条目时） | ✓ | ✓ | ✓ |
| `fomc_documents` | — | ✓ pending→fetched | 状态刷新 | ✓ 标 failed |
| `fomc_decisions` | — | ✓ 仅 statement | — | — |
| `fomc_summaries` | — | ✓ | ⛔ 跳过 | — |
| `fomc_meetings` | 仅当天首窗口且日历有变 | 同左 | 同左 | 同左 |
| `fomc_probabilities` | 仅当天首窗口 | 同左 | 同左 | 同左 |
| `economic_events` | 只读（EODHD 管道维护） | 只读 | 只读 | 只读 |

## 五、三种典型场景的耗时对照

| 场景 | 实际执行 | 耗时量级 |
|---|---|---|
| **平常的 95% 时刻**（场景 A） | 仅阶段①：RSS→无新文档→概率已跑→结束 | 秒级 |
| **决议日 14:00 ET 后首个窗口**（场景 B） | ①→②→③→④全链自动 | ②秒级 ③约 1–2 分钟 |
| **纪要日**（场景 B 变体） | 同上但 doc_type=minutes，跳过抽取 | 同上 |

## 六、省钱 / 自愈 / 幂等三设计

1. **content_hash 去重**（场景 C）：Fed 偶尔修正页面重新挂网——hash 没变则跳过 LLM 概括，不烧 token
2. **双重自愈**（场景 D）：纪要 URL 按固定模式提前入库为 pending，Fed 未挂网时抓取 failed → 每 30 分钟对 pending+failed 派发 `fomc_fetch_document`（与新文档同一条抽取/概括链）→ 另对已 fetched 但无 completed 概括补派发 `fomc_summarize`。不要在 poll 内联 `retry_pending_documents`（会与 Celery 抢抓 pending，hash 写入后跳过概括）。
3. **全链幂等**：所有表写入均 `ON CONFLICT DO UPDATE` + UNIQUE 约束——任何一步重跑、任何任务重复派发，结果收敛到同一份数据，不会重复入库
