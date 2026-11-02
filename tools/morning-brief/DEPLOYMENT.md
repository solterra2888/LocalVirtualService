# morning-brief 部署与使用说明(自 US-market-report 复制;cron 未安装,时间待定)

面向运维部署者的操作手册。架构与设计规则见 [README.md](README.md);本文只讲
**怎么装、怎么跑、怎么监控、坏了怎么修**。

---

## 1. 概述

每日自动生成 GuanHe 风格 Daily Market Brief(英文,`--style en`),产出:

```
backend/reports/pipeline/{报告日期}/final/report.md      # 成品(含图表)
backend/reports/pipeline/{报告日期}/final/charts/*.png   # 01_us_close / 02_finviz_map
```

- 报告日期 T = 美股交易日;在北京时间 **T+1 上午**生成(定时 09:20)。
- 全新生成约 **23 分钟**,LLM tokens 约 **9.4M 输入(96% 缓存命中)+ 190K 输出**。
- 重复运行是安全的:断点续跑,已完成章节秒级复用,不会重复花钱。

## 2. 系统依赖

| 依赖 | 要求 | 说明 |
|---|---|---|
| Python | `/root/miniconda3/envs/guanhe/bin/python` | 第三方包:matplotlib、squarify、pillow、requests、numpy、psycopg2(随 backend 依赖装好) |
| 数据库 | `.env` 中 `DB_*` 指向的 Postgres | 管线**只读**下游表;唯一写操作是经济日历 actual 回填(见 §6.6) |
| ZCode CLI | `/root/.zcode/server/bin/zcode` | 无头 LLM 调用(`-p --json --mode yolo`),即本管线所在服务器自带的 zcode |
| 外网 | FRED / EODHD / Yahoo Finance | 利率、ETF 历史、历史涨跌回填;均无需梯子直连 |
| 上游 ingest | 见 §9 | 节目转录、twitter 快讯、Finviz 快照由仓库现有定时任务持续入库,管线只消费 |

关键数据表(全部只读,除经济日历回填):
`subscription_content`(转录/快讯/通讯社全文)、`finviz_screener`(板块热力图与
US Close 图)、`economic_events`、`earnings_calendar`、`market_data_eod`、
`macro_pillars` 系列。

## 3. 密钥与配置

### 3.1 仓库 `.env`(`/opt/guanhetech/.env`)

管线用到的是以下几项,其余 key 与本管线无关:

| Key | 用途 | 缺失后果 |
|---|---|---|
| `DB_HOST/PORT/NAME/USER/PASSWORD` | 一切数据库访问 | 立即失败 |
| `EODHD_API_TOKEN` | 经济日历 actual 回填;FRED 未发布时的利率回落 | 日历无 actual、利率行缺当日值 |
| `ALPHAVANTAGE_API_KEY` | 备用数据源 | 目前路径未使用,可空 |

FRED 走公开 CSV 端点、Yahoo 走公开 chart API,均**无需 key**。

### 3.2 ZCode provider 配置(新机部署的关键坑)

zcode CLI 在**交互终端**由 harness 注入环境变量;**cron 的极简环境没有**,
缺了会报 `无法定位 CLI ZCode Built-in Provider Config` 并退出 1
(2026-10-29 09:20 实跑失败原因)。因此 `cron_daily.sh` 显式导出:

```bash
export HOME=/root
export ZCODE_DATA_BASE_DIR=/root
export ZCODE_PERSONAL_PROVIDER_CONFIG_FILE=/root/.zcode/v2/provider_config.json
export ZCODE_BUILTIN_PROVIDER_CONFIG_FILE=$(ls -1 \
    /root/.zcode/v2/runtime/provider/linux-x86_64/*/endpoint-*/zcode-builtin.json \
    | sort -V | tail -1)   # 路径含版本号与 endpoint hash,按版本取最新,zcode 升级自适应
```

迁移新机时:先在该机安装 zcode 并完成一次正常登录/配置(生成
`~/.zcode/v2/` 下凭证),上述脚本即可工作;无需手工搬运 hash 路径。

## 4. 部署步骤(新机 checklist)

```bash
# 1) 代码
git clone git@github.com:solterra288/guanhetech.git /opt/guanhetech && cd /opt/guanhetech

# 2) .env 按上表填好 DB_* 与 EODHD_API_TOKEN

# 3) 环境自检:数据库连通 + zcode 可用(交互终端直接跑)
/root/miniconda3/envs/guanhe/bin/python -c \
  "from backend.utils.database_config import get_database_config; print(get_database_config())"
/root/.zcode/server/bin/zcode -p "Reply with exactly one word: OK" --json \
  --mode yolo --surface terminal --cwd /tmp

# 4) 冒烟测试:不调 LLM 的状态机自测(秒级,应发布占位报告)
PY=/root/miniconda3/envs/guanhe/bin/python
$PY tools/US-market-report/build_snapshot.py
$PY tools/US-market-report/run_pipeline.py {刚解析出的日期} --fake-zcode

# 5) 装 crontab(见 §5),手动触发一次验证 cron 环境
env -i HOME=/root PATH=/usr/bin:/bin SHELL=/bin/bash \
  /bin/bash tools/US-market-report/cron_daily.sh
#    预期:几分钟后 cron.log 出现 "US-market-report cron end (rc=0)"

# 6) 首次正式生成(全新交易日约 23 分钟)
$PY tools/US-market-report/run_pipeline.py {日期} --style en
```

## 5. 定时运行(cron)

crontab 条目(已在本机配置):

```cron
# US-market-report: 北京时间 09:20 周二~周六(美股交易日次晨,排在 8:30/8:40 节目摘要任务之后)
20 9 * * 2-6 /bin/bash /opt/guanhetech/tools/US-market-report/cron_daily.sh >> /opt/guanhetech/backend/reports/pipeline/cron.log 2>&1
```

`cron_daily.sh` 逻辑:

1. 导出 §3.2 的 zcode provider 环境变量(找不到配置则立即报错退出);
2. `build_snapshot.py` —— 自动解析**最新 The Close 交易日**为报告日期 T,
   重建 `snapshot.json`(数据层 + 新闻层 + 转录内联,约 2~3 分钟);
3. `run_pipeline.py {T} --style en` —— 章节生成/核验/出图/组装;
4. `flock` 防重叠:上一轮未结束时本次直接跳过;
5. 日志行格式 `==== ... US-market-report cron end (rc=N) ====` 写入 cron.log。

改风格:编辑脚本顶部 `STYLE=en` → `cn`。改时间:改 crontab 第一字段。

> 注意:README 已知边界指出节目转录 ingest 在次日上午 10 点后才"实测稳定",
> cron 定在 09:20 属于激进取早 —— 若日志频繁出现转录缺失(errors 里记
> `the_close` / `cnbc_closing_bell`),把触发调到 10:00 之后即可。

## 6. 手动操作手册

以下 `$PY` 均指 `/root/miniconda3/envs/guanhe/bin/python`,在仓库根目录执行。

### 6.1 生成 / 重跑指定日期(标准入口)

```bash
$PY tools/US-market-report/run_pipeline.py 2026-10-30 --style en
```

语义 = 断点续跑:`snapshot.json` 必须已存在(没有则先跑 6.2);
章节已 done 的跳过、verdict 比章节新则复用,只补缺的部分。

### 6.2 只重建快照(数据层刷新)

```bash
$PY tools/US-market-report/build_snapshot.py                # 最新 The Close 交易日
$PY tools/US-market-report/build_snapshot.py --date 2026-10-30
$PY tools/US-market-report/build_snapshot.py --no-backfill  # 调试用,跳过 EODHD 回填省配额
```

### 6.3 强制全量重新生成(复用旧快照)

完成判定是**文件级**的(state.json + sections/*.md 的 DONE 标记),所以:

```bash
D=2026-10-30
cd /opt/guanhetech/backend/reports/pipeline/$D
rm -f state.json sections/*.md sections/*.claims.json verify/verdict.json
cd /opt/guanhetech
$PY tools/US-market-report/run_pipeline.py $D --style en
```

### 6.4 只重出图(不动正文)

```bash
$PY tools/US-market-report/render_charts.py backend/reports/pipeline/2026-10-30
# 之后重跑 6.1(秒级,复用路径)让 report.md 重新组装嵌入
```

历史日期热力图(finviz_screener 无历史快照时自动 Yahoo 回填当日涨跌):

```bash
$PY tools/US-market-report/finviz_map_chart.py --workspace backend/reports/pipeline/2026-10-30 --date 2026-10-16
```

### 6.5 核验强度

```bash
--llm-verify light   # 默认:确定性全绿后,轻量 LLM 只读七节查跨节矛盾
--llm-verify off     # 只跑确定性核验(省一轮 LLM)
--llm-verify full    # 旧版全量(挂快照),一般不用
--fake-zcode         # 不调 LLM 的状态机自测
```

### 6.6 写库说明(唯一的库写操作)

`build_snapshot.py` 默认对生产 `economic_events` 做 [T-2, T] 的 actual
COALESCE upsert(把 EODHD 实际值补进事件行)。只补空不覆盖;调试用
`--no-backfill` 跳过。

## 7. 监控

| 看什么 | 位置 | 正常样子 |
|---|---|---|
| 定时任务日志 | `backend/reports/pipeline/cron.log` | 每个交易日一行 `cron end (rc=0)` |
| 每步耗时/tokens | `{date}/state.json` 的 `timings` / `usage_total` | 见 §8 成本参考 |
| 核验结论 | `{date}/verify/verdict.json` | `"verdict": "pass"` |
| 转人工 | `{date}/NEEDS_HUMAN_REVIEW.md` | **不应常驻**;出现即人工介入(见 §10) |
| 成品 | `{date}/final/report.md` | 顶部有图,`(news flash)` 等归因标签齐全 |

**退出码**:`0` 已发布 / `1` 运行错误(看 cron.log) / `2` 核验超限转人工。

日常巡检一条命令:

```bash
tail -5 /opt/guanhetech/backend/reports/pipeline/cron.log
```

## 8. 成本参考(2026-10-30 完整生成实测)

| 环节 | 墙钟 | 输入 tokens | 输出 tokens |
|---|---|---|---|
| overall_market 组(narrative/drivers/positioning) | 694s | 3,353,046 | 52,533 |
| company_desk 组(sector_action/movers/deep_dives) | 955s | 3,013,295 | 68,035 |
| macro 组 | 813s | 2,784,696 | 53,864 |
| 核验+定向修订(LLM light) | ~400s | ~232K | ~15K |
| **合计(三组并行,墙钟=最慢组+核验)** | **~1360s** | **~9.4M** | **~190K** |

输入含 agent 多轮工具调用对快照/转录的重复读取,约 96% 命中 prompt 缓存;
净新增输入约 0.42M。复用路径重跑(无 LLM)约 3 分钟。

## 9. 上游 ingest 健康(管线外,但决定报告质量)

| 上游 | 表 | 停更影响 |
|---|---|---|
| Bloomberg The Close(YouTube 转录) | `subscription_content` | narrative/drivers/positioning 降级 |
| CNBC Closing Bell 播客(主段+OT) | `subscription_content` | sector_action/movers/deep_dives 降级 |
| twitter 快讯 9 白名单账号 | `subscription_content` | drivers 新闻事件块缺失 |
| Yahoo Finance / Seeking Alpha 全文 | `subscription_content` | wire/calls 层缺失 |
| Finviz screener 整表刷新 | `finviz_screener` | 两张图表失败(热力图无历史快照时仅当日不可回溯) |
| EODHD 经济日历 | `economic_events` | Macros Releases 块降级 |

缺哪个源,`snapshot.json` 的 `errors` 会记录,管线按缺源降级**不中断**;
巡检时顺手看一眼当日 `errors` 是否为空。

## 10. 故障排查

| 症状 | 原因 | 处置 |
|---|---|---|
| cron.log: `无法定位 CLI ZCode Built-in Provider Config` | cron 极简环境缺 provider 变量 | 确认 `cron_daily.sh` 的 §3.2 导出段仍在;`~/.zcode/v2/` 存在 |
| cron.log: `No such file or directory`(指 cron_daily.sh) | 脚本被误删 | `git checkout -- tools/US-market-report/cron_daily.sh`(已在 git,commit fb11de4) |
| `build_snapshot failed` | DB 不通 / `.env` 缺项 / 外网异常 | 看日志首个 traceback;按 §4.3 自检 |
| 转录缺失(`errors` 记 the_close / cnbc_closing_bell) | 上游 ingest 滞后 | 等入晚些重跑 6.2 + 6.1;持续缺 → 查上游 ingest 任务 |
| `rc=2` + `NEEDS_HUMAN_REVIEW.md` | 核验多轮仍有 high fail | 打开该文件看 issues 清单,人工修订章节后重跑 6.1 |
| 热力图/US Close 图缺 | `finviz_screener` 无 ≤T+1 快照 / FRED+EODHD 均失败 | 报告仍会发布(图缺省);查上游 Finviz 任务与 EODHD 配额 |
| 历史日期热力图报 `Yahoo backfill too incomplete` | 当日 Yahoo 数据覆盖 <80% | 该日期无法出图;属数据源限制 |
| zcode 超时(默认单 agent 1200s) | 上游 API 抖动 | 直接重跑 6.1,断点续跑只补缺;若章节文件其实已写完会自动识别 |

## 11. 相关文件

| 文件 | 作用 |
|---|---|
| `cron_daily.sh` | 定时入口(本手册 §5) |
| `build_snapshot.py` / `transcript_prep.py` | Stage 0 冻结快照 + 转录直查清洗 |
| `run_pipeline.py` | 编排:章节并行 → 核验门控 → 出图 → 组装 |
| `prompts.py` / `verify_checks.py` | 章节任务书 / 确定性对账 |
| `us_close_chart.py` / `finviz_map_chart.py` / `render_charts.py` | 两张 GuanHe 同款图表 |
| `README.md` | 架构、设计规则、已知边界(改代码前必读) |
