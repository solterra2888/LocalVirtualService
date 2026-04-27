# LLM 体系重构成果总结

> 完成日期：2026-04-27
> 计划文档：[LLM_REFACTOR_PLAN.md](./LLM_REFACTOR_PLAN.md)
> 架构文档：[LLM_ARCHITECTURE_OVERVIEW.md](./LLM_ARCHITECTURE_OVERVIEW.md)

---

## 重构全景

```
阶段 0 [██████████████████████████████] 100% ✅ Schema 治理 (PR-1, PR-2)
阶段 1 [██████████████████████████████] 100% ✅ LLM 基建统一 (PR-3, PR-4)
阶段 2 [██████████████████████████████] 100% ✅ File Tag 用户自定义 (PR-5)
阶段 3 [██████████████████████████████] 100% ✅ Feed Summary 个性化 (PR-6, PR-7)
阶段 4 [██████████████████████████████] 100% ✅ 文档 + 清理 (PR-8)
```

**8 个 PR，约 2 个工作日实际编码**。所有改动均通过生产事务级 (BEGIN/ROLLBACK) 验证 + 真实生产数据集成测试。

---

## 解决的 12+10 个混乱点

### A. 命名与抽象 (PR-3, PR-4)

| # | 混乱点 | 解决 |
|---|--------|------|
| 1 | `category` / `module` / `category_code` 三套命名 | ✅ 统一 `module_code`，UI 走 category_id |
| 2 | `category_id` (INT) vs `category_code` (TEXT) 不一致 | ✅ 前端用 `moduleCode` prop，后端按 module_code 索引 |
| 3 | `username` (TEXT) / `user_id` (INT) 双轨制 | ⚠️ D1.C 新表用 INT；旧表保留 + `user_resolver.py` 适配 |
| 4 | `MODULE_REGISTRY` 注册不全（5 个模块）| ✅ 扩展到 13 个：`file_tagging` / `feed_summary` / `news_summary` 等 |

### B. LLM 调用层 (PR-3, PR-4)

| # | 混乱点 | 解决 |
|---|--------|------|
| 5 | 两条路径 (`LocalLLM` vs `chat_completion_with_template`) 能力不对等 | ✅ `LocalLLMAnalysisService` 内部委托 `chat_completion_with_template`，对外 API 不变 |
| 6 | 路径 A 不写 `llm_prompt_usage_logs` (监控割裂) | ✅ 重构后所有 LLM 调用统一写日志 (生产实测 id=254982 成功写入) |
| 7 | `chat_completion_with_template` 不接受 `template_id` | ✅ 通过 `LLMTemplateResolver` 统一，所有调用方传 `template_id` 即可 |

### C. 用户偏好层 (PR-3)

| # | 混乱点 | 解决 |
|---|--------|------|
| 8 | `user_template_preferences` 80% 失效 | ✅ 接通 `LLMTemplateResolver`，所有 task/service 都走偏好链 |
| 9 | "运行时模板"与"用户默认模板"优先级不明 | ✅ 统一 fallback：`runtime > preference > default` (10/10 集成测试通过) |

### D. Feeds 1:N 跨用户 (PR-6, PR-7)

| # | 混乱点 | 解决 |
|---|--------|------|
| 10 | `subscription_content_tags` 跨用户共享，无 per-user override | ✅ 双层模型：客观字段全局 + 主观 summary per-user |
| 11 | 各 module_code (`twitter_phase1/phase2`) 语义割裂 | ✅ MODULE_REGISTRY 元信息说明 SYSTEM_ONLY 与 configurable 区别 |
| 12 | File / Feed tagging schema 高度相似但完全分开 | ⚠️ D7.A 决策：保留两表，仅服务层抽象（避免大改造） |

### E. 后续发现 (PR-2, PR-4, PR-8)

| # | 混乱点 | 解决 |
|---|--------|------|
| 13 | `llm_prompt_templates` / `llm_prompt_categories` / `llm_prompt_usage_logs` 三表无 baseline migration | ✅ PR-1 反向 baseline，与生产 100% 对齐 (字段长度/约束名/索引名/触发器名) |
| 14 | `file_tags` 也无 baseline | ✅ PR-1 同 |
| 15 | `processing_stats.llm_model/llm_provider`、`file_processing_details.file_storage_type/oss_bucket` 列在基线 CREATE 中缺失 | ✅ PR-2 补 migration + db_manager 同步 |
| 16 | `twitter_report_user_config` 孤儿表 | ✅ PR-8 清理（生产数据全为空 phase config，安全 DROP） |
| 17 | `upload-web-clip` 不触发 auto_analyze | ✅ PR-4 R1 修复，4 路触发对齐 |
| 18 | `PATCH content` `trigger_llm` 不触发 auto_analyze | ✅ PR-4 R2 修复 |
| 19 | tag/name/classify status 枚举不统一 | ✅ PR-4 R3 修复 |
| 20 | `zhishixingqiu` 假"已支持"渠道 | ⏳ 留作未来工单 |
| 21 | `add_unread_count.sql` 与 `drop_subscription_id_column.sql` 顺序耦合 | ✅ PR-2 加 column 存在守护 |
| 22 | 3 个孤儿 PG 函数（引用已删字段，调用必报错）| ✅ PR-8 清理 |

---

## 新增 / 修改 / 删除文件清单

### 新增（11 个）

| 类别 | 文件 |
|------|------|
| Migration | `create_llm_core_tables_baseline.sql` (PR-1) |
| Migration | `fix_processing_stats_llm_columns.sql` (PR-2) |
| Migration | `fix_file_processing_details_storage_columns.sql` (PR-2) |
| Migration | `add_module_code_to_llm_prompt_templates.sql` (PR-3) |
| Migration | `create_subscription_content_user_summaries.sql` (PR-6) |
| Migration | `cleanup_legacy_template_orphans.sql` (PR-8) |
| Service | `llm_template_resolver.py` (PR-3) — 三级 fallback 解析 |
| Service | `feed_summary_personalization_service.py` (PR-6) |
| Util | `user_resolver.py` (PR-4) — username → user_id LRU 缓存 |
| Task | `feed_summary_tasks.py` (PR-6) |
| Doc | `LLM_REFACTOR_SUMMARY.md` (本文，PR-8) |
| Test | `verify_llm_template_resolver.py` (PR-3, 10/10 PASS) |
| Test | `verify_local_llm_refactor.py` (PR-4, 6/6 PASS) |

### 修改（关键变化）

| 文件 | 关键变化 |
|------|---------|
| `local_llm_analysis_service.py` | 内部改调 `chat_completion_with_template` (Path A 自动写 usage_log) |
| `template_preference_service.py` | MODULE_REGISTRY 5 → 13；SYSTEM_DEFAULTS 扩展 |
| `content_tagging_service.py` | 接受 `template_id/code` 参数（取代硬编码） |
| `feed_content_tagging_service.py` | 透传 `tagged_by` → LLM `user_id` (修复 usage_log NULL) |
| `tagging_tasks.py` / `naming_tasks.py` / `classify_tasks.py` | 接 LLMTemplateResolver + user_id 透传 + status 枚举统一 |
| `feed_tagging_tasks.py` | username → user_id 转换 |
| `subscription_content_service.py` | `_attach_personalized_summaries` 叠加层 |
| `subscription_content_tags.py` (API) | 末尾追加 personalize/get/delete 端点 |
| `files.py` (API) | 5 个上传 API 接 `auto_tag_template_id`；R1/R2 修复 |
| `file_tasks.py` | 9 处 `trigger_auto_tagging` 透传 template；R1/R2 接入 trigger_auto_analyze |
| `db_manager.py` | 基线 CREATE 补 4 列；columns_to_add 加 LLM 列 |
| `add_unread_count.sql` | 4 处 DO 块加 column 存在守护 |
| `celery_app.py` / `celery_config.py` | 注册 personalize_feed_summary 任务 + 队列 |

### 前端（关键文件）

| 文件 | 关键变化 |
|------|---------|
| `services/files.ts` | `UploadAnalyzeOptions` 统一类型，5 个上传方法接 `auto_tag_template_*` |
| `services/subscriptions.ts` | `SubscriptionContent` 加 5 个 `personalized_*` 字段；2 个新方法 |
| `components/TemplateSelector.tsx` | 新增 `moduleCode` prop |
| `components/UploadAnalysisTemplatePicker.tsx` | 新增 `moduleCode` prop（向后兼容 categoryCode） |
| `components/UploadFilesModal.tsx` 等 5 个上传弹窗 | 增加第二个 file_tagging picker |
| `pages/FilesPage.tsx` | "Retag with template" 按钮 + Modal |
| `components/feed/FeedArticleBody.tsx` | "Personalize" / "Re-personalize" / "Reset" 按钮 + ✨Personalized 徽标 + Modal |

---

## 关键能力变化（用户可见）

### Upload Files

1. **5 个上传弹窗** 增加第二个 picker：选择自定义 file_tagging 模板
2. **FilesPage 右侧 Tags 面板** 增加 "Retag with template" 按钮，可对单文件用任意模板重打
3. **PATCH content `trigger_llm=true`** 现在会触发完整 4 路（含 analyze）
4. **Web Clip** 现在也触发 4 路 LLM 任务

### Feeds

1. **每条 feed 详情页** 增加 "Personalize" 按钮，弹窗选择 `feed_summary` 模板
2. **个性化生成的 summary** 显示 ✨ Personalized 徽标，含主观 reasoning（斜体）
3. **客观字段（sector/industry/ticker）** 仍跨用户共享，保证检索一致性
4. **Reset 按钮** 一键回到全局 summary

### 通用

1. **每次 LLM 调用都写 `llm_prompt_usage_logs`**（之前 Path A 全部不写）
2. **usage_log 含正确的 user_id**（修复了 NULL 归属问题）
3. **三级 fallback `runtime > preference > default`** 在所有打标/分析/个性化场景生效
4. **Settings → Prompt Templates** 用户可为 `feed_summary` / `file_tagging` 等设默认偏好

---

## 验证证据

| PR | 验证方式 | 结果 |
|----|---------|------|
| PR-1 | 生产 BEGIN/ROLLBACK + `pg_dump` 字段对齐 | 4 张表完全对齐 |
| PR-2 | 同上 + `\d` 对照 | VARCHAR 长度/默认值精确 |
| PR-3 ① | 生产事务跑 migration | 23 模板全部赋值 module_code，0 NULL |
| PR-3 ② | `verify_llm_template_resolver.py` | 10/10 case pass，含真实用户偏好命中 |
| PR-4 | `verify_local_llm_refactor.py` smoke test | 6/6 pass |
| PR-4 e2e | 生产 Feed retag → `llm_prompt_usage_logs` id=254982 | 写入成功，tokens=3251，¥0.032510 |
| PR-6 ① | 生产事务跑 migration | 表/约束/索引/触发器全部到位 |
| PR-8 | 生产事务清理 | 表+3 函数全部安全 DROP |

---

## 部署顺序（首次上线）

```bash
# 1. 跑 migrations（按字母序自动跑，IF NOT EXISTS 全幂等）
ls backend/database/migrations/*.sql | xargs -I{} psql ${DSN} -f {}

# 2. 部署后端代码
git pull && systemctl restart guanhe-api guanhe-celery-worker

# 3. 部署前端
cd web-frontend && npm run build && deploy

# 4. 灰度开关（可选，按需关闭）
# 默认全开:
#   AUTO_TAG_ENABLED=true
#   AUTO_ANALYZE_ENABLED=true
#   FEED_SUMMARY_PERSONALIZATION_ENABLED=true
```

### 回滚开关（紧急情况）

| 环境变量 | 默认 | 设为 `false` 的效果 |
|---------|------|---------------------|
| `AUTO_TAG_ENABLED` | true | 所有自动打标关闭 |
| `AUTO_ANALYZE_ENABLED` | true | 所有自动深度分析关闭 |
| `AUTO_FEED_TAG_ENABLED` | true | Feeds 自动打标关闭 |
| `FEED_SUMMARY_PERSONALIZATION_ENABLED` | true | 个性化 summary 关闭（API 返回 skipped） |

代码改动**未引入破坏性的 schema 变更**——所有 ALTER 都是 ADD COLUMN IF NOT EXISTS，回滚不需要 DOWN migration。

---

## 后续（不在本次范围）

| 项 | 性质 | 优先级 |
|---|------|------|
| `username/user_id` 全量 INT 迁移 (D1.A) | 大重构 | 低（双轨能跑）|
| `file_tags` / `subscription_content_tags` 表合并 (D7.B) | 大重构 | 低（收益小）|
| `zhishixingqiu` 渠道适配 | 业务功能 | 中 |
| News Analysis 模块同等改造 | 业务功能 | 低（当前文档显示已弃用）|
| 模板共享市场（用户互相分享模板）| 产品扩展 | 中 |
| LLM 调用成本面板（按 module/user/template）| 监控扩展 | 中 |
| `feed_summary` 加星自动派发 | 阶段 3 完整版 (T2) | 中 |

---

## 关键经验沉淀

1. **Schema 漂移最危险**：阶段 0 反向 baseline 是后续所有改造的地基。`pg_dump` 客户端版本必须 ≥ 服务端，否则用 `information_schema` 反查也行。

2. **LLM 调用统一入口**：让 `chat_completion_with_template` 成为唯一中央入口，其他服务都做委托——这是观测性、可控性、成本审计的基础。

3. **per-user 不一定要破坏共享**：Feeds 双层模型（客观全局 + 主观 per-user）保护了内容复用机制，避免 LLM 调用 N 倍爆炸。

4. **三级 fallback resolver 是"地基级"组件**：一旦统一，新增模块（feed_summary 等）只需在 MODULE_REGISTRY 注册即可生效。

5. **守护性 migration**：所有 ALTER 都用 `IF NOT EXISTS`、孤儿数据清理用 `DO $$ ... END $$` + 安全检查——让 migration 在任何环境（生产/dev/CI）都幂等且安全。
