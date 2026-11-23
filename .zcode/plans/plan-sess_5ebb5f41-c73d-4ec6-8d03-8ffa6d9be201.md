# 今日涨跌榜「当日」口径修正

## 请先确认：我对规则的理解

上一轮的确认问题你没有作答，我按对「当日 = 报告生成日（北京日期 D）」最贴合的理解来设计。**如果第 1 条不是你的意思，请直接指出，美股部分需要改。**

| 市场 | 「当日」指哪一场交易 | 11-20 报告（D=11-20）的例子 |
|---|---|---|
| 美股 | 北京 D 日凌晨收盘的那一场（美东 D−1 交易日） | NVDA 145.98 / −3.30%，生成时就是最终值，之后不变 |
| 港股 / A股 | 北京 D 日自己的交易时段 | 生成时未开盘，先显示 11-19 收盘并标注；当天收盘后固定为 11-20 收盘 |

另一种读法是美股取美东 D 日那一场（NVDA 125.53）。它在报告生成 21 小时后才收盘，每天的旧报告都要事后改数，且内容与下一期报告重复，所以我没有采用。

## 背景

- 榜单行冻结在生成时，点击弹出的 K 线却实时拉最新数据，旧报告里行比图晚一天。
- 发布日和次日的实时刷新用 EODHD 实时接口的 `previousClose` 当昨收，美股闭市时该字段不可靠。实测 11-21 报告 140 行美股里 72 行当日被改写，21 行变成 0.00%。
- 港股/A股行只在生成当天和次日被刷新，之后退回上一交易日。
- SNDK 11-20 生效 8:1 拆股，当日涨跌由 T 与 T−1 两份未同口径的 EODHD 文件相除得出，显示 −89.51%，实际 −16.05%。

## 改动

### 1. 取数 `tools/facts-report/build_snapshot.py`

- `_volume_leaders_eodhd` 增加 `markets` 参数，支持只重算指定市场；每行新增 `prev_close`。
- 修一个连带问题：`_pick` 直接取 `day_rows[ex]`（`build_snapshot.py:886`），任一交易所当日无数据会让四个市场的榜单全部消失。改为单市场无数据只影响自己。
- 拆股修正：当日涨跌与 Yahoo 序列末段相差超过 1 个百分点，或当日 ≤ −30% / ≥ +200% 时，查 EODHD `splits/{code}`；当日有拆股则按比例修正昨收。
- `_rank_sparks` 增加 `markets` 参数。

### 2. 新脚本 `tools/facts-report/refresh_rank_rows.py`

- 默认用 D=T+1 的 EODHD 文件重算港股/A股行和走势，写回 `snapshot.json`，再执行 `md2html_report.py` 和 `register_morning_brief.py --ok`（分享 token 不变）。
- D 日港股和 A股都没有数据（周末、休市、文件未出）时不改动。
- `--markets us`：用 T 日文件重算美股行，用于一次性修正 SNDK。
- `--dry-run`：只打印结果。

### 3. 发布登记 `tools/facts-report/register_morning_brief.py`

- `rank_live` 改为新格式：只含港股/A股，每行带 D−1 收盘价作昨收锚。
- 新增 `rank_cut`：美股截止 T，港股/A股截止 D。

### 4. 服务 `backend/services/morning_brief_report_service.py`

- 实时刷新只接受行情日期等于 D 的港股/A股报价，昨收用冻结的 D−1 收盘，不再用实时接口的 `previousClose`。美股行不再刷新，旧格式报告不再下发榜单实时数据。
- K 线弹窗按 `rank_cut` 截断；没有该字段的旧报告按美股交易日截断。缓存键加入截止日。

### 5. 页面 `tools/facts-report/report_html_tools/md2html_report.py`

- 每个市场标题下加截止日说明：
  - 美股：「北京 D 日凌晨收盘 · 美东 T 交易日」
  - 港股/A股更新前：「暂为 11-19 收盘 · 11-20 有交易则开盘后自动更新」；更新后：「11-20 收盘」
- 走势图补实时点改为同日替换，不再每次轮询多一个点。
- 加行序版本校验：报告重渲染后，已打开的旧页面忽略实时补丁并提示刷新，避免数字贴到别的股票上。

### 6. 港股/A股收盘补数定时任务

- 新脚本 `tools/facts-report/asia_close_refresh.sh`：flock 防并发，只处理 T=昨天的工作区，失败 Lark 告警（与 `ft_refresh.sh` 同款）。
- `backend/services/scheduler_service.py`：新增 `morning_brief_asia_close` 处理器，与 `_execute_morning_brief_ft_refresh_task`（`:1597`）同款。
- `backend/database/db_manager.py:990`：任务类型白名单加新类型。
- 新增幂等迁移 `backend/database/migrations/add_morning_brief_asia_close_scheduler_task.sql`，cron `30 18,21 * * 2-6`（两次尝试，幂等）。

### 7. 测试与文档

- `backend/tests/test_morning_brief_live_quotes.py`：新增榜单实时层用例（日期过滤、昨收锚、同日替换标记、旧格式不下发）。
- `backend/tests/test_morning_brief_report_kline.py`：新增截止日用例。
- 新增 `tools/facts-report/test_rank_split_fix.py`（离线，mock EODHD）：SNDK 8:1 修正为 −16.05%，无拆股不动。
- `tools/facts-report/README.md` 追加一节说明新口径。

## 上线顺序

1. 改代码，跑单测。
2. 实测 EODHD 当日港股/A股文件的可得时间，据此确定首次时点。计划模式下无法联网验证，18:30 是估计值。
3. 检索 `morning_brief_ft_refresh` 的全部出现位置，确认任务类型没有遗漏的枚举点。
4. 重启后端与调度服务（需要 sudo，执行前会再向你确认）。
5. 执行迁移，启用定时任务。
6. 可选：`refresh_rank_rows.py 2026-11-20 --markets us`，修正已发布的 11-21 报告里的 SNDK。这会重新发布一份报告，默认不做，等你点头。

## 验证

- `pytest backend/tests/test_morning_brief_live_quotes.py backend/tests/test_morning_brief_report_kline.py -v`
- `cd tools/facts-report && python -I test_rank_split_fix.py`
- `python refresh_rank_rows.py 2026-11-20 --dry-run`：D=11-21 是周六，预期输出「无数据，不改动」。
- `python refresh_rank_rows.py 2026-11-20 --markets us --dry-run`：预期 SNDK 当日为 −16.05%。
- 重启后直接调用服务：11-20 报告的实时载荷只含港股/A股，且每条行情日期等于 D；K 线末根日期美股为 T、港股/A股不超过 D。
- 浏览器打开分享链接：
  - 11-20 报告 NVDA 行为 145.98 / −3.30%，弹窗末根为 11-19。
  - 港股/A股标题下有截止日说明。
  - 连续刷新几次，走势图点数不增加。
- 下一个工作日收盘后：确认 18:30 任务把港股/A股行更新为当日收盘，次日打开仍保持当日数据。

## 改完后你的两个例子

- **11-20 报告 NVDA**：行 145.98 / −3.30%，弹窗末根 11-19，标注「北京 11-20 凌晨收盘 · 美东 11-19 交易日」。
- **11-20 报告港股/A股**：08:00–09:30 显示 11-19 收盘并标注；当天收盘后固定为 11-20 收盘。
- **11-21 报告 SNDK**：执行上线顺序第 6 步后由 −89.51% 变为 −16.05%。

## 不在本次范围

- 大盘总览表的实时刷新，仍沿用现有的两天窗口规则。
- 文档版 `report.md` 里港股/A股的前 8 名表格，仍是生成时的数据；网页交互榜才是更新后的。
- Yahoo 偶发返回残缺序列（SNDK 在 11-20 报告里走势图缺失）只加日志告警，不做替代数据源。