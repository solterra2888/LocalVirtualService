# guanhe-daily-report 部署与使用说明(v8:xiao 式宏观地缘晨报;cron 未安装,手动跑通中)

面向运维部署者的操作手册。架构与设计规则见 [README.md](README.md);本文只讲
**怎么装、怎么跑、怎么监控、坏了怎么修**。

---

## 1. 概述

每日自动生成 xiao 式中文晨报(01-06 节沿 GuanHe 版式,07-11 节为 v8 新 Macros:
宏观速览/回顾·宏观数据/回顾·地缘政治/美联储观察/展望与策略),产出:

```
backend/reports/guanhe-daily-report/{报告日期}/final/report.md      # 成品(含图表)
backend/reports/guanhe-daily-report/{报告日期}/final/charts/*.png   # 01_markets / 02_finviz_map
```

- 报告日期 T = 美股交易日;在北京时间 **T+1 上午**生成(**建议定时 10:00**:
  中文晨报素材约 08:51-09:22 发布,节目转录 10 点后才稳定)。
- 全新生成约 **35 分钟**(第一段六组并行 ≈ 最慢组 + 第二段 outlook + 核验),
  LLM tokens 约 **9.4M 输入(92.8% 缓存命中)+ 276K 输出**(2026-10-30 首跑
  实测,含 2 轮修订)。
- 重复运行是安全的:断点续跑,已完成章节秒级复用,不会重复花钱。

## 2. 系统依赖

| 依赖 | 要求 | 说明 |
|---|---|---|
| Python | `/root/miniconda3/envs/guanhe/bin/python` | 第三方包:matplotlib、squarify、pillow、requests、numpy、psycopg2(随 backend 依赖装好) |
| 数据库 | `.env` 中 `DB_*` 指向的 Postgres | 管线**只读**下游表;唯一写操作是经济日历 actual 回填(见 §6.6) |
| ZCode CLI | `/root/.zcode/server/bin/zcode` | 无头 LLM 调用(`-p --json --mode yolo`) |
| 外网 | FRED / EODHD / Yahoo Finance / 腾讯行情 | 利率、ETF 历史、历史涨跌回填、港/A股成交额榜与中文名;均无需梯子直连 |
| 上游 ingest | 见 §9 | 节目转录、twitter 快讯、Finviz 快照、Admin 订阅源(星球/推特/news/substack/youtube)抓取由仓库现有定时任务持续入库,管线只消费 |

关键数据表(全部只读,除经济日历回填):
`subscription_content`(转录/快讯/通讯社全文/**Admin 订阅的宏观地缘素材**)、
`finviz_screener`、`economic_events`、`earnings_calendar`、`market_data_eod`、
`macro_pillars` 系列、`fomc_decisions`、`fomc_probabilities`、`fomc_documents`。

## 3. 密钥与配置

### 3.1 仓库 `.env`(`/opt/guanhetech/.env`)

管线用到的是以下几项,其余 key 与本管线无关:

| Key | 用途 | 缺失后果 |
|---|---|---|
| `DB_HOST/PORT/NAME/USER/PASSWORD` | 一切数据库访问 | 立即失败 |
| `EODHD_API_TOKEN` | 经济日历 actual 回填;FRED 未发布时的利率回落;港股/A股成交额榜(bulk 端点) | 日历无 actual、利率行缺当日值、涨跌榜缺港股/A股两表 |
| `FRED_API_KEY` | 宏观底座(曲线/ranks/fed_odds) | macro_pillars 大部分子块缺失 |

FRED 走公开 CSV 端点(有 key 更稳)、Yahoo 走公开 chart API,均**无需梯子**。

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

# 2) .env 按上表填好 DB_* / EODHD_API_TOKEN / FRED_API_KEY

# 3) 环境自检:数据库连通 + zcode 可用(交互终端直接跑)
/root/miniconda3/envs/guanhe/bin/python -c \
  "from backend.utils.database_config import get_database_config; print(get_database_config())"
/root/.zcode/server/bin/zcode -p "Reply with exactly one word: OK" --json \
  --mode yolo --surface terminal --cwd /tmp

# 4) 冒烟测试:不调 LLM 的状态机自测(秒级,应发布占位报告)
PY=/root/miniconda3/envs/guanhe/bin/python
$PY tools/guanhe-daily-report/build_snapshot.py
$PY tools/guanhe-daily-report/run_pipeline.py {刚解析出的日期} --fake-zcode

# 5) 装 crontab(见 §5;当前阶段按用户决定暂缓,先手动跑通),手动触发一次验证 cron 环境
env -i HOME=/root PATH=/usr/bin:/bin SHELL=/bin/bash \
  /bin/bash tools/guanhe-daily-report/cron_daily.sh
#    预期:几十分钟后 cron.log 出现 "guanhe-daily-report cron end (rc=0)"

# 6) 首次正式生成(全新交易日约 25-35 分钟)
$PY tools/guanhe-daily-report/run_pipeline.py {日期}          # 默认 --style cn
```

## 5. 定时运行(cron)

**当前状态:已安装(2026-11-06 起)**,北京时间 **08:31 周二~周六**(用户选择,
尽量赶早交付;实测 08:31 The Close 转录已入库,但中文 digest 层(08:51-09:22
发布)会系统性缺失,靠 research/geo 兜底)。两个已知代价,检查效果时留意:

- 09:00 中文晨报素材(约 08:51-09:22 发布)与节目转录(10 点后稳定)可能未齐,
  首日若 01-03/07 节内容偏薄,先怀疑时点再怀疑代码;改回 10:00 只需编辑
  crontab 第一字段;
- 与旧管线 US-market-report(09:20)并行约 40 分钟 —— 锁与产物目录独立,
  zcode 并发有 3s 错峰 + 重试兜底(v8.5-fix)。

```cron
# guanhe-daily-report: 北京时间 08:31 周二~周六(美股交易日次晨)
31 8 * * 2-6 /bin/bash /opt/guanhetech/tools/GuanHe-Daily-Report/cron_daily.sh >> /opt/guanhetech/backend/reports/guanhe-daily-report/cron.log 2>&1
```

`cron_daily.sh` 逻辑(`PIPE` 已指向 `tools/guanhe-daily-report`):

1. 导出 §3.2 的 zcode provider 环境变量(找不到配置则立即报错退出);
2. `build_snapshot.py` —— 自动解析**最新 The Close 交易日**为报告日期 T,
   重建 `snapshot.json`(数据层 + 新闻层 + 宏观地缘素材层 + FOMC 表 + 转录内联,
   约 2~3 分钟);
3. `run_pipeline.py {T} --style cn` —— 两段章节生成/核验/出图/组装;
4. `flock` 防重叠:上一轮未结束时本次直接跳过;
5. 日志行格式 `==== ... guanhe-daily-report cron end (rc=N) ====` 写入 cron.log。

改风格:编辑脚本顶部 `STYLE=cn` → `en`。改时间:改 crontab 第一字段。

## 6. 手动操作手册

以下 `$PY` 均指 `/root/miniconda3/envs/guanhe/bin/python`,在仓库根目录执行。

### 6.0 操作纪律(踩坑检查单 —— 每次动手前先扫一遍)

以下每条都是实际踩过的坑,编号对应 [TEST-2026-11-04.md](TEST-2026-11-04.md):

1. **探测/检查脚本先 dump 真实返回结构,再取值**。禁止按直觉猜键名或形状
   (P1、P20 两次同源事故:`fetch_closing_bell` 返回的键是 `transcript` 不是
   `text`,键存在≠值非空;先 `list(d.keys())` / `inspect.signature` 再写格式化)。
   给自己写探测脚本时,第一步永远是把结构原样打出来。
2. **后台命令与重定向一律绝对路径**。shell 的 cwd 会在仓库内漂移,相对路径
   曾把快照写进 `tools/guanhe-daily-report/backend/...`(2026-11-05 双坑:日志和脚本
   路径同时踩)。
3. **日志全量落盘(`> file 2>&1`),判定以 `state.json` 为准**。禁止 `| tail`
   管道(吞中间日志、掩盖真实退出码,P5)。
4. **手工改章节 `.md` 必须成对重生成 `.claims.json`**——文件契约是一对;
   只改一半会让确定性核验继续报 high、并触发 verdict 复用空转(P17)。
5. **盘中不生成 T 日报告**。北京时间上午只跑 T-1 的美股日;同日重复构建时
   确认 triage.json 覆盖了完整交易时段(盘中生成的 triage 残片会被复用逻辑
   当作当日有效选材,必须 `--force-triage`,2026-11-05 实例)。
6. **排障常识**:zcode 子进程以 `node` 形态存在(cmdline 首段是 node 路径,
   grep `[z]code` 会误判全挂,P9);DB 是 Postgres,连接用
   `DatabaseManager(get_database_config())`,不是本地 sqlite。

### 6.1 生成 / 重跑指定日期(标准入口)

```bash
$PY tools/guanhe-daily-report/run_pipeline.py 2026-10-30            # 默认 cn
```

语义 = 断点续跑:`snapshot.json` 必须已存在(没有则先跑 6.2);
章节已 done 的跳过、verdict 比章节新则复用,只补缺的部分。

### 6.2 只重建快照(数据层刷新)

```bash
$PY tools/guanhe-daily-report/build_snapshot.py                # 最新 The Close 交易日
$PY tools/guanhe-daily-report/build_snapshot.py --date 2026-10-30
$PY tools/guanhe-daily-report/build_snapshot.py --no-backfill  # 调试用,跳过 EODHD 回填省配额
$PY tools/guanhe-daily-report/build_snapshot.py --no-triage    # 跳过 LLM 素材分拣(news_macro 走正则回退)
$PY tools/guanhe-daily-report/build_snapshot.py --force-triage # 已有 triage.json 也强制重跑分拣
$PY tools/guanhe-daily-report/build_snapshot.py --triage-limit 30  # 分拣冒烟(只送前 N 条候选)
```

v8.2 起构建默认先跑素材 triage(Admin 订阅轻扫 → 分块 LLM 分拣 → 写
`triage.json`),再建快照组池;已存在的 `triage.json` 自动复用不重复花
tokens。triage 失败自动回退 v8.1 正则路径,快照永远可建。

### 6.3 强制全量重新生成(复用旧快照)

完成判定是**文件级**的(state.json + sections/*.md 的 DONE 标记),所以:

```bash
D=2026-10-30
cd /opt/guanhetech/backend/reports/guanhe-daily-report/$D
rm -f state.json sections/*.md sections/*.claims.json verify/verdict.json
cd /opt/guanhetech
$PY tools/guanhe-daily-report/run_pipeline.py $D
```

### 6.4 只重出图(不动正文)

```bash
$PY tools/guanhe-daily-report/render_charts.py backend/reports/guanhe-daily-report/2026-10-30
# 之后重跑 6.1(秒级,复用路径)让 report.md 重新组装嵌入
```

### 6.5 核验强度

```bash
--llm-verify light   # 默认:确定性全绿后,轻量 LLM 只读各节正文查跨节矛盾
--llm-verify off     # 只跑确定性核验(省一轮 LLM)
--llm-verify full    # 旧版全量(挂快照),一般不用
--strict-verify      # 核验超限时回到旧行为:rc=2 转人工不发布
--fake-zcode         # 不调 LLM 的状态机自测

v8.7 起核验超限(轮次用尽/问题只涉锁定节)**默认不再拦截**:照常发布,未决
问题自动标注在报告顶部(指针)与文末(核验备注清单);复核意见仍全文进
report.audit.md。需要人工把关的时期用 --strict-verify。
```

### 6.6 写库说明(唯一的库写操作)

`build_snapshot.py` 默认对生产 `economic_events` 做 [T-2, T] 的 actual
COALESCE upsert(把 EODHD 实际值补进事件行)。只补空不覆盖;调试用
`--no-backfill` 跳过。

## 7. 监控

| 看什么 | 位置 | 正常样子 |
|---|---|---|
| 定时任务日志 | `backend/reports/guanhe-daily-report/cron.log` | 每个交易日一行 `cron end (rc=0)` |
| 每步耗时/tokens | `{date}/state.json` 的 `timings` / `usage_total` | 见 §8 成本参考 |
| 核验结论 | `{date}/verify/verdict.json` | `"verdict": "pass"` |
| 转人工(仅 --strict-verify) | `{date}/NEEDS_HUMAN_REVIEW.md` | 默认模式下不存在;降级发布的信号是成品顶部「⚠️ 核验备注」指针与文末清单 |
| 宏观素材层 | `{date}/snapshot.json` 的 `sources.news_macro` | digest/research/geo 三层非空(中英混合);周五 digest 空属已知边界(notes 有记录) |
| 每日流程卡 | `{date}/pipeline-overview.html` | 发报成功后自动生成(纯脚本零 LLM):产品全景/运营实测/数据流向桑基图/栏目溯源/质检漏斗,数据取自当日工作区工件 |
| 成品 | `{date}/final/report.md` | 顶部有图;07-11 节中文标题齐全,「展望与策略」以免责行收尾 |

**退出码**:`0` 已发布(含带核验备注降级发布) / `1` 运行错误(看 cron.log) / `2` 核验超限转人工(仅 `--strict-verify`)。

日常巡检一条命令:

```bash
tail -5 /opt/guanhetech/backend/reports/guanhe-daily-report/cron.log
```

## 8. 成本参考(2026-10-30 首跑实测,cn 风格,含 2 轮修订 + 人工修复重验)

| 环节 | 墙钟 | 备注 |
|---|---|---|
| overall_market 组(narrative/drivers/positioning) | 1364s | |
| company_desk 组(sector_action/deep_dives) | 1507s | 本日 finviz 缺失,多读新闻;正常有 finviz 会更快 |
| geopolitics / fomc / macro_review(新 Macros 三 agent,并行) | 1063s / 851s / 720s | |
| outlook(第二段) | 351s | 读成品综合,显著便宜 |
| 核验(light)+ 定向修订 ×2 轮 | ~1100s | 首跑核验轮次偏多属正常收敛过程 |
| **tokens 合计** | 输入 **9.4M**(92.8% 缓存命中)/ 输出 **276K** | 含全部修订轮 |
| **干净一次通过估算** | 墙钟 ~35 分钟、输入 ~8M | 第一段并行 ≈ 最慢组 + outlook + 1 轮核验 |

输入含 agent 多轮工具调用对快照/转录的重复读取;复用路径重跑(无 LLM)约 3-4 分钟。

## 9. 上游 ingest 健康(管线外,但决定报告质量)

| 上游 | 表 | 停更影响 |
|---|---|---|
| Bloomberg The Close(YouTube 转录) | `subscription_content` | narrative/drivers/positioning 降级 |
| CNBC Closing Bell 播客(主段+OT) | `subscription_content` | sector_action/movers/deep_dives 降级 |
| **Admin(user_id=1)订阅的全部 twitter**(v7 起无白名单,~35 账号) | `subscription_content` | drivers 新闻事件块缺失 |
| **Admin 订阅 news 全文源**(Yahoo/SA/CNBC/CCTV/Globenewswire) | `subscription_content` | wire/calls 层缺失 |
| **subscription_content_tags 打标**(含 L/E 分级,`info_level`/`evidence_level` 列) | `subscription_content_tags` | flash/wire 溢出退化为时序截断(未评级=NULL 不排除);triage 失败走正则兜底时字段不再缩水(P28:兜底已关联打标表,L4 同样排除) |
| **Admin(user_id=1)订阅源**(深蓝Alpha/调研鹅纪要等星球、Bloomberg TV/Reuters、CCTV、Geopolitical Dispatch、英文宏观推特号) | `subscription_content` + `subscription_list` | 对应素材缺失;整层退化表现为 news_macro 子层为空(errors 记录)。订阅增删即时改变源池,无需改代码 |
| **调研鹅纪要(知识星球)** | `subscription_content` | 券商宏观解读缺失 |
| Finviz screener 整表刷新 | `finviz_screener` | 两张图表失败 |
| EODHD 经济日历 | `economic_events` | 边际变化/速览表降级 |
| fomc_decisions/probabilities/documents 同步任务 | 同名表 | 美联储观察节降级 |

缺哪个源,`snapshot.json` 的 `errors` 会记录,管线按缺源降级**不中断**;
巡检时顺手看一眼当日 `errors` 是否为空。

## 10. 故障排查

| 症状 | 原因 | 处置 |
|---|---|---|
| cron.log: `无法定位 CLI ZCode Built-in Provider Config` | cron 极简环境缺 provider 变量 | 确认 `cron_daily.sh` 的 §3.2 导出段仍在;`~/.zcode/v2/` 存在 |
| cron.log: `No such file or directory`(指 cron_daily.sh) | 脚本被误删 | `git checkout -- tools/guanhe-daily-report/cron_daily.sh` |
| `build_snapshot failed` | DB 不通 / `.env` 缺项 / 外网异常 | 看日志首个 traceback;按 §4.3 自检 |
| 转录缺失(`errors` 记 the_close / cnbc_closing_bell) | 上游 ingest 滞后 | 等入晚些重跑 6.2 + 6.1;持续缺 → 查上游 ingest 任务 |
| `news_macro.digest` 空(非周五) | 当日无标题命中的晨报或抓取滞后 | 看 `subscription_content` 当日行与 Admin 订阅;research/geo 层可兜底 |
| `rc=2` + `NEEDS_HUMAN_REVIEW.md`(仅 --strict-verify) | 核验多轮仍有 high fail | 打开该文件看 issues 清单,人工修订章节后重跑 6.1;默认模式则看成品文末「核验备注」清单 |
| 热力图/Markets 图缺 | `finviz_screener` 无 ≤T+1 快照 / FRED+EODHD 均失败 | 报告仍会发布(图缺省);查上游 Finviz 任务与 EODHD 配额 |
| zcode 超时(默认单 agent 1800s) | 上游 API 抖动 | 直接重跑 6.1,断点续跑只补缺;若章节文件其实已写完会自动识别 |

## 11. 相关文件

| 文件 | 作用 |
|---|---|
| `cron_daily.sh` | 定时入口(本手册 §5) |
| `build_snapshot.py` / `transcript_prep.py` | Stage 0 冻结快照 + 转录直查清洗 |
| `run_pipeline.py` | 编排:两段并行 → 核验门控 → 出图 → 组装 |
| `prompts.py` / `verify_checks.py` | 章节任务书 / 确定性对账 |
| `us_close_chart.py` / `finviz_map_chart.py` / `render_charts.py` | 两张图表 |
| `README.md` | 架构、设计规则、已知边界(改代码前必读) |
