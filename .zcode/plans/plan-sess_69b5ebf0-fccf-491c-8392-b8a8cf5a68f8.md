# morning-brief 修复优化方案(plan 可靠性 + 重试 + P17 + actual 类型)

## Context

今天 11-04 v8.5 重测中 plan 三次失败,根因各异:①首次——plan agent 在 yolo+工作区 cwd 下无视 prompt 禁令游荡(11 次模型请求、1.74M 输入,设计值 ~50K/1-2 次),第 11 次请求撞瞬时 `Turn execution failed`;②续跑——CLI 级挂死 300s 零进展(疑似并发 SQLite 争夺);③300s 硬超时线低于经验分布(v8.4 实测 391/549s)。加上同日 16:32 一个写作 agent 被瞬时故障放倒,再次证明 `run_agent` 无重试是 cron 前置缺口。另有 P17(verdict 复用只比 .md 不比 claims,昨天实测空转)与 actual 类型不统一(DB 原生路径是字符串)。

修复后**今天全量重跑 11-04**(用户已确认,~10M/45min),完成 v8.4 vs v8.5+fix 的完整 A/B。

## 修复项

### 1. plan 单发改造(核心)— `run_pipeline.py` + `prompts.py`

- 新增 `_plan_once(zcode_bin, digest_text, owners)`:完整复用 triage 的成熟单发模式(`build_snapshot.py:1239-1268`)——命令 `[zcode, "-p", prompt, "--json", "--mode", "yolo", "--surface", "terminal"]`,**不带 `--cwd`、不挂附件**(工作区文件不可达,游荡在结构上不可能);digest 全文内联进 prompt;失败重试 1 次、20s 退避
- `build_plan_prompt`(prompts.py:1033)改写:内联 digest;输出指令从「Write plan.json / Output file: plan.json」改为「**只输出一个 JSON 对象,不要包裹在其他文字里,不要使用任何工具**」(对齐 triage 的输出收敛惯例);PLAN_OWNER_ROLES 与规则主体不动
- `run_plan`(run_pipeline.py:1318)调整:仍生成 plan_input.md(工件留档)→ 读文本 → `_plan_once` → 解析 `out.get("response")`(剥 ```json 围栏、截首尾大括号,容错同 `_parse_zcode_json`)→ **编排器校验后写 ws/plan.json**(agent 永不写文件)→ `_load_plan` 复用逻辑原样保留
- `PLAN_TIMEOUT_SEC` 300→600(单发预期 60-120s,600 为保险带);usage 照取 envelope,`usage_by_phase.plan` 记账不变

### 2. run_agent 自动重试 + 启动错峰 — `run_pipeline.py:133-162, 1380-1383`

- 非零退出(现 RuntimeError 路径)重试 1 次、20s 退避,日志 `[agent] transient failure, retrying once`;TimeoutExpired 维持现状(文件契约由调用方判定)
- 第一段 submit 循环加错峰:提交第 i 个 agent 前 `sleep(i*3)`(削 7 个 CLI 同时启动的 SQLite 争夺,最多 +18s 墙钟)

### 3. P17:verdict 复用纳入 claims mtime — `run_pipeline.py:1444`

- `section_files` 追加 `claims_path(ws, n)`(已 import,run_pipeline.py:61),`newest_section` 取 .md + .claims.json 的最大 mtime

### 4. actual 数值统一 — `build_snapshot.py` `_clean`(~806)

- economic_events 装载时对 actual 做容错强转(去逗号/%;可解析则 float,失败保留原串);编排器旧快照补丁批次(assign_item_ids/units/reported 同处)加同样的幂等强转

## 验证(按用户确认:今天全量重跑)

1. AST × 3 文件;`--fake-zcode` 全流程(rc=0)
2. **单发 plan 真机验证**:用现有 11-04 plan_input.md 直调 `_plan_once`(~85K/2min),对照 14:45 手工 plan 看质量(4-7 故事/owner 合规/canonical 经 `verified_canonicals` 通过率)
3. 离线回放:P17(touch claims 较新 → 断言不复用);actual 强转(midsession bak 的 ISM '54.5'→54.5)
4. **11-04 全量重跑**:清 sections/state/plan.json/verdict(保留 2026-11-04-v84bak)→ 全流程 → eval → v8.4 vs v8.5+fix 对照(核验轮次、plan done、`LLM high ... one revision round`、美债全篇一致、07 单位口径、move 锚点误报数)
5. 文档:TEST-2026-11-04.md 记 P23(plan 三因)与修复清单;OPTIMIZATION-2026-11-05.md 补「十、plan 单发与重试」

## 涉及文件

`tools/morning-brief/run_pipeline.py`(主)、`tools/morning-brief/prompts.py`、`tools/morning-brief/build_snapshot.py`、两个文档。`--no-plan`/`--llm-verify` 开关与降级语义不动:单发两次失败仍走既有 unavailable 路径,不阻断管线。