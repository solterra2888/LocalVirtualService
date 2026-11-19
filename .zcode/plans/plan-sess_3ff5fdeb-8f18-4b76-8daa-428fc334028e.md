# Morning Brief 1301 审核拦截：Gemini（CherryIN）单发兜底

## Context

2026-11-19 08:00 facts-report（Morning Brief）管线失败，当天 3 次重跑全部同因。根因（zcode CLI 日志 traceId `25ee4d9a` 等）：**GLM API 内容审核 1301** 拦截，触发点是 geopolitics 章节素材（俄乌/美伊/制裁/APEC 三边会晤的中文快讯原文）以及 `[plan]` 步骤的 digest。现有代码的放大问题：`run_agent` 对确定性拒绝盲重试一次（必然再挂）、单章节失败导致 40-60 分钟整跑报废、告警只显示 `Turn execution failed` 无根因。

**已实测验证**：CherryIN 网关（`open.cherryin.cc`，DB `llm_provider_configs` 已启用 gemini/claude，key 已配置）上 `google/gemini-3.1-pro-preview` 与 `anthropic/claude-sonnet-4.6` 对今天这批真实敏感素材（63KB 地缘快讯）正常完成中文写作，无拦截。**GLM 拦、Gemini/Claude 不拦，兜底成立。**

目标：1301 类审核拦截不再放倒整条管线 —— 被 GLM 拦截的调用改由 Gemini 单发兜底完成；根因进日志与告警；补跑 2026-11-18 报告。

## 改动

### 1. 新文件 `tools/facts-report/llm_fallback.py`（自包含，无新依赖）

- `ModerationBlockedError(RuntimeError)`；`moderation_blocked(stderr)` —— 匹配 `[1301]` / `sensitive content` / `unsafe content`
- provider 配置：读 DB `llm_provider_configs`（provider_id 默认 `gemini`；psycopg2 + 复用 `build_snapshot.py:752` 的 `.env` 读取模式拿 DB 凭据；guanhe conda env 已有 psycopg2/dotenv）。env 覆盖：`MB_FALLBACK_PROVIDER/MODEL/BASE_URL/API_KEY`；读不到配置 → 兜底禁用，行为同今日
- `chat_completion(messages)`：urllib 直调 OpenAI 兼容 `/v1/chat/completions`（与验证脚本同款调用方式）
- `single_shot_files(agent_prompt, snapshot_text, expected_files)`：在原任务书后追加「FALLBACK DELIVERY MODE」——无工具、快照全文内联、输出单个 JSON `{"files": {"<path>": "<完整文件内容>"}}`（含各 `.claims.json` 台账）；复用 `_parse_zcode_json`（`run_pipeline.py:131`）式容错解析，剥代码围栏

### 2. `tools/facts-report/run_pipeline.py` 五处改动

1. **`run_agent`（:144）**：非零退出先分类 —— 命中 1301 → 立即抛 `ModerationBlockedError`，跳过无意义的同 payload 重试；其它错误保留现有 20s 重试。错误消息改为提取 stderr 中真实 provider 错误行（过滤 node SQLite warning，不再只取末 500 字符）——根因随 `pipeline error` 进 cron.log 与 Lark 告警
2. **`_run_one_agent`（:1147）**：捕获 `ModerationBlockedError` → Gemini 单发兜底（原 prompt + `_sliced_snapshot`（:1085）全文内联）→ 校验返回路径 ⊆ 该 agent 预期输出文件（防路径逃逸）、章节文件缺 `<!-- SECTION DONE -->` 则补、claims 必须是合法 JSON 数组 → 落盘后走现有 `section_done`（:1046）校验；state 记 `mode: "gemini-fallback"`、usage 照常累计、打印 `[fallback] agent X: GLM 1301 → gemini 单发` 行。兜底自身失败 → 按现行为抛错整线失败（不静默丢章节）
3. **`_plan_once`（:1377）**：被拦 → Gemini 单发重试一次（plan 本就是单发 JSON，交付契约最简单）；失败走现有 `unavailable` 降级（不阻断）
4. **`revise_sections`（:1743）**：修订调用被拦 → 复用 `single_shot_files`（当前文件内容 + issues 清单内联，输出整文件替换）；失败则跳过该组修订，v8.7 语义下带标注发布
5. **`run_verifier`（:1556）**：LLM 叙事核验被拦 → 跳过 LLM 核验（确定性台账核验不受影响），日志注明，verdict 只含确定性结果

### 3. 演练开关

`MB_FALLBACK_FORCE=<agent名>`（如 `geopolitics`）：强制该 agent 走兜底路径，不烧 GLM 调用即可端到端演练。

### 4. 文档

`tools/facts-report/DEPLOYMENT.md` 增补：兜底机制、配置来源（DB 行 + env 覆盖）、Claude 亦验证可用可作备选、运维 runbook。

## 验证与恢复（按序）

1. **离线单测**：prompt 构造、JSON 解析、路径校验、DONE_MARKER/claims 补齐（伪造响应，不联网）
2. **回归冒烟**：`run_pipeline.py <日期> --fake-zcode` 全流程跑通（现有状态机/门控测试路径不受影响）
3. **在线演练**：`MB_FALLBACK_FORCE=geopolitics` 补跑 `run_pipeline.py 2026-11-18 --style cn --max-parallel 2` —— 幂等跳过已完成章节，仅补 geopolitics（兜底）→ outlook → verify → charts → assemble
4. **正式补跑**：去掉 FORCE 真实跑（GLM 1301 → 兜底自然触发，这就是今天的真实现场），检查 `[fallback]` 日志、09 章节内容、claims 过确定性核验；rc=0 后手动 `register_morning_brief.py --ok 2026-11-18` 注册发布（cron 正常时自动做）
5. 观察次日 08:00 定时任务自然运行

## 关键复用

`_parse_zcode_json`（:131）、`_sliced_snapshot`（:1085）、`SECTION_SPECS/AGENT_STAGES/agent_section_names`（prompts.py:868/937）、`section_done`（:1046）、`DONE_MARKER`（:83）、`_add_usage`、build_snapshot 的 `.env` 读取（:752）、claims 台账契约（prompts.py 规则 10/11）。

## 范围外（明确不做）

- 不改 zcode `provider_config.json` 挂 CherryIN（该构建 `ZCODE_PROVIDERS=["glm"]`，未文档化、恐坏主路径）
- 兜底失败时不自动渲染 geopolitics 占位符（保守：整线失败转人工，占位降级可作后续选项）
- 不做 provider 自动二跳（Gemini 挂了换 Claude）——Claude 已验证可用，留作配置项手切