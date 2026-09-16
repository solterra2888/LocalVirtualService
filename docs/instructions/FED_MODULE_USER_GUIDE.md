# Fed 模块使用手册

给第一次打开 **Central Banks** 的投资研究同事。读完应能独立完成一次 FOMC 桌面作业：官方已经做了什么、市场正在赌什么、声明/纪要的词有没有裂缝、后面几次会还要不要再动。

入口：左侧菜单 **Central Banks**，地址 `/central-bank`。打开即是 Fed 桌面（本模块目前只覆盖美联储）。

---

## 0. 为什么值得每天（或每次会议前后）打开

专业投资人并不缺 Fed 信息，缺的是**同一屏上把三件不同性质的事实对齐**：

| 你真正要判断的 | 本模块对应的块 | 它是什么 |
|---|---|---|
| Fed **已经做了**什么 | **Stance** | 从官网声明原文抽出的决议、票型、异议 |
| 市场 **已经按什么在交易** | **Priced in** + **Probability trend** | CME 30 天联邦基金期货隐含概率，自算、可复核 |
| 委员会 **怎么说的** | **What changed in the text** | 声明/纪要原文 + 固定五段中文概括 |
| 会间还有没有新表态 | **Fed updates** | 官网 RSS，不是二手新闻 |

这四层故意分开。数字、价格、措辞混在一条 AI 摘要里，会让人分不清哪句是事实、哪句是定价、哪句是模型。本桌面的用法是：**Stance 是事实，Priced in 是价格，Text 是词，三者对不上的地方才值得写进笔记。**

它替代不了 CME FedWatch 付费终端，也替代不了自己读 SEP / 点阵图。它做的是把你每次开会前后都要做的对照，变成打开就能做的固定流程。

---

## 1. 页面怎么走

自上而下：

1. **Stance**（左）× **Priced in**（右）— 主对照。默认看下一次会的定价，对照上次官方决议。
2. **FOMC cycle** — 时间轴。实线框是已开完的会，虚线框是未来的会。点一下，Stance / Priced in / 文本跟着切换。
3. **What changed in the text** — Statement / Minutes 两个页签。默认露出「措辞变化」「前瞻信号」「经济评估」。
4. **Probability trend** — 这条定价最近怎么走、隐含月均利率路径、历史会前定价 vs 实际。
5. **Fed updates** — 官网新闻流。Monetary Policy 开关默认开着。

时间轴格子上的 **SEP** 表示该次会同时公布经济预测摘要（含点阵图）。Priced in **不管点阵图**，SEP 会议要把声明和 SEP 自己读完。

---

## 2. 一次标准作业（约 3 分钟）

按这个顺序，不要先盯着一个百分比。

### 第一步：Stance — 先钉住事实

看四件事：

- 目标区间（Fed Funds Target Range）
- 上次是 Hold / Hike / Cut，动了多少 bp
- 票型，以及相对上一会是否更分裂
- 异议者名字和方向（hawk = 想更高，dove = 想更低）

卡片右上角 **Statement** 链到 federalreserve.gov 原文。区间和票型来自声明正则抽取，不是模型猜的。有异议时，把人名记下来——后面读纪要时，这些人就是内部裂缝的锚。

### 第二步：Priced in — 再看市场已经买了什么

三个大数字：Hold / Hike / Cut，约 100%。颜色：灰 / 红 / 蓝。

读法只有一句：**高概率的那边若兑现，对利率期货意外不大；若落空，冲击更大。**

- Hike 90%+：交易的是「加完以后的路径 / 声明鹰鸽」，不是「加不加」。
- Hold 与 Hike 接近五五：会议日本身双向开放，仓位按两种结果都可能来想。
- 底栏 `7d hike +28pp`（或 hold / cut）：过去约 7 天，**当前主导方向**变了多少个百分点。刚转鹰时，声明若偏鸽，回撤往往更猛。

标题旁的问号滑过，有公式和数据源。点时间轴上**更后面**的虚线框时，Hike% 是「那次会上再加一次」，不是「到那天为止总共加了几次」。

例：9 月 Hike 92%、10 月 Hike 46%，意思是 9 月大概率已经加过的前提下，10 月再加一次约一半。到 10 月底「至少加过一次」的概率，接近 9 月那次的 92%，不是 46%。

点已经开过的会，Priced in 变成 **pre-meeting**：只显示会前定价。实际结果在下面 Probability trend 的表里。

### 第三步：Text — 找价和词的裂缝

页签 **Statement**（声明，会后即出）和 **Minutes**（纪要，约滞后 3 周）。页签上若写 `(prior)`，表示纪要还是上一会的，这次会的纪要未出。

默认三段：

1. **措辞变化 vs 上次** — 新增了什么、删了什么
2. **前瞻信号** — 下次可能怎么走、委员会在担心什么
3. **经济评估** — 增长 / 就业 / 通胀的用词

「决议」「投票」不再展开，和 Stance 重复。底部 **Read original** 弹层看清洗后的原文，**Fed.gov** 外链到官网。

把这段和 Priced in 对上：

- 价已经加满，声明仍偏鸽 → 回撤风险在措辞，不在决议本身
- 价还在 Hold，纪要里 hawk 密度词很重 → 下一段定价可能补涨
- SEP 会议：点阵图本模块算不到，必须自己读

### 第四步：Trend — 定价是刚转的还是已经横了

三条折线是每日快照。当前若市场只在 Hold 与 Hike 之间定价，Cut 会是 0%，图上不画蓝线（模型不会同时给加息和降息两边留权重，见第 5 节）。

怎么用：

- Hike 高位走平（>90%）：会议难再制造方向性惊喜，看声明和 SEP
- 一周内陡升/陡降：刚重新定价（非农、CPI、主席讲话之后常见）
- 红线和灰线在 50% 附近纠缠：意外空间大

**Implied average funds rate**（如 `3.74 → 3.98 → 4.10`）是各合约月的**日均 EFFR 预期**，不是会后目标区间。数字往上走偏紧，走平或往下偏松。不要读成「9 月加到 3.74、10 月再到 3.98」。

**Expectation vs actual**：已开完的会，会前主导方向 vs 实际。hit 只说明当时市场押的方向对了。连续 Hold 且市场一直 Hold 时，hit 很好看，鉴别力有限。点时间轴上某次过去的会，对应行会高亮。

### 第五步：Updates — 会间有没有新表态

RSS 来自 Fed 官网 `press_all.xml`。主席讲话、委员演讲、新声明出现在这里。会议当天「issues FOMC statement」和 Stance 是同一件事，主对照仍看上面两张卡。

---

## 3. 数据从哪来（全部可点开复核）

下面每一个数字都能在官网或公开行情页找到。页面上 Stance 的 **Statement**、文本底部的 **Fed.gov**，就是该条记录入库时的 `source_url`，应与下表构造规则一致。

### 3.1 字段 → 网址 → 你在页上对什么

| 页面上的数 | 打开 | 核对什么 |
|---|---|---|
| 会议日期、SEP 星号 | [FOMC Meeting Calendars](https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm) | 两日会取**第二天**（决议/声明日）。日期后有 `*` = SEP |
| Stance 区间、Hold/Hike/Cut、票型、异议名单 | 该次声明 HTML。规则：`https://www.federalreserve.gov/newsevents/pressreleases/monetary{YYYYMMDD}a.htm`（`YYYYMMDD` = 会议末日） | 正文 `target range … at X to Y percent`；`by an A-B vote`；`Voting for` / `who preferred to raise\|lower` |
| 上一会对照 | 时间轴上一次会的同一条声明 URL | 上次区间与票型 |
| Statement 原文 / AI 概括 | 同上声明 URL；卡片 **Fed.gov** / **Read original** | 概括只是中文速读，引用用这页 |
| Minutes 原文 / AI 概括 | `https://www.federalreserve.gov/monetarypolicy/fomcminutes{YYYYMMDD}.htm`（同样是会议末日） | 纪要约滞后 3 周。页签写 `(prior)` = 还是上一会的纪要 |
| Fed updates 标题与日期 | [press_all.xml](https://www.federalreserve.gov/feeds/press_all.xml) | `title` / `link` / `pubDate`。分类含 Monetary Policy 或标题含 FOMC 的会进默认开关 |
| Priced in 的期货价 \(P\) | 见 3.3：先定合约月，再打开 Yahoo / CME | 日收盘。\(\mathrm{implied}=100-P\) |
| Priced in 的基准 EFFR | [纽约联储 EFFR](https://www.newyorkfed.org/markets/reference-rates/effr) 或 [FRED EFFR](https://fred.stlouisfed.org/series/EFFR) | 快照日前（含当日）最近一个有效观测，不是区间中值 |
| Probability trend 折线 | 没有第三张网。每天把上面两样再算一遍存库 | 横轴日期 = 快照日；点 Yahoo/FRED 历史可手算任意一天 |
| Expectation vs actual 的 Actual | 该次声明 URL（与 Stance 同源） | 会前快照不含决议公布后的收盘 |
| 对照用（非本模块数据源） | [CME FedWatch Tool](https://www.cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html) | 只对方向；允许差几个百分点 |

实例（用会议末日拼 URL，可直接打开）：

| 会议末日 | 声明 | 纪要 |
|---|---|---|
| 2026-07-29 | [monetary20260729a.htm](https://www.federalreserve.gov/newsevents/pressreleases/monetary20260729a.htm) | [fomcminutes20260729.htm](https://www.federalreserve.gov/monetarypolicy/fomcminutes20260729.htm) |
| 2026-09-16 | [monetary20260916a.htm](https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm) | 会后约 3 周才挂 `fomcminutes20260916.htm` |

声明 URL 的 `monetary…a.htm` 前缀无其它规律，**新声明以 RSS 的 `<link>` 为准**；日历末日对得上时，也可以按上表自己拼。

RSS 与政策入口：

- 全部新闻 RSS：<https://www.federalreserve.gov/feeds/press_all.xml>
- 货币政策总览：<https://www.federalreserve.gov/monetarypolicy.htm>
- 近期声明列表：<https://www.federalreserve.gov/newsevents/pressreleases.htm>

### 3.2 EFFR：两个官网，同一个序列

本模块读 **FRED API** 序列 `EFFR`（asof 日前 7 天窗口内最后一个非缺失值）。投资人手算请用网页，不必申请 API key。

| 用途 | 网址 |
|---|---|
| 纽约联储官方有效利率（定义与当日值） | <https://www.newyorkfed.org/markets/reference-rates/effr> |
| FRED 图 + 表（可下 CSV） | <https://fred.stlouisfed.org/series/EFFR> |
| FRED 直接下 CSV | <https://fred.stlouisfed.org/graph/fredgraph.csv?id=EFFR> |
| 本模块实际请求（需自己的 FRED key） | `https://api.stlouisfed.org/fred/series/observations?series_id=EFFR&file_type=json&observation_start={asof-7d}&observation_end={asof}` |

核对时看**百分数**（如 3.63），不要用目标区间中值 3.625。FRED 没有 key 时本模块会降级用区间中值，Priced in 会系统性偏鹰约数个百分点——若你手算对不上，先看是不是这个。

### 3.3 ZQ 期货：CME 品种 + Yahoo 日收盘

品种是 CME **30-Day Federal Funds Futures**，代码 **ZQ**。本模块不拉 CME 付费接口，日收盘来自 Yahoo Finance 图表 API，符号 `{根}{月码}{年两位}.CBT`。

| 用途 | 网址 |
|---|---|
| CME 品种说明（合约规格、报价 = 100 − 月均 EFFR） | <https://www.cmegroup.com/markets/interest-rates/stirs/30-day-federal-fund.html> |
| CME 各月合约报价板 | <https://www.cmegroup.com/markets/interest-rates/stirs/30-day-federal-fund.quotes.html> |
| Yahoo 某月合约（把符号换掉） | `https://finance.yahoo.com/quote/{符号}/` 例：[ZQU26.CBT](https://finance.yahoo.com/quote/ZQU26.CBT/) |
| Yahoo 日 K JSON（手点用 `range`；本模块实际用 `period1`/`period2` 时间戳） | `https://query1.finance.yahoo.com/v8/finance/chart/{符号}?interval=1d&range=1mo` 例：[ZQU26.CBT chart](https://query1.finance.yahoo.com/v8/finance/chart/ZQU26.CBT?interval=1d&range=1mo) |
| 历史回填用的连续合约 | [ZQ=F](https://finance.yahoo.com/quote/ZQ=F/)（页面若标 `front-month approx.` 即用了它） |

月码（1–12 月）：

```
F G H J K M N Q U V X Z
1 2 3 4 5 6 7 8 9 10 11 12
```

例：2026 年 9 月 = **U** + **26** → `ZQU26.CBT`；11 月 → `ZQX26.CBT`；2027 年 1 月 → `ZQF27.CBT`。

**用哪一个合约**（与代码相同，可手选）：

- 决议日在合约月内，且会后剩余天数 \(d_{\mathrm{post}} = N - \text{决议日} \ge 10\) → **当月**
- 否则 → **下月**（月末会议当月只剩几天，对决议几乎不敏感；此时 \(d_{\mathrm{pre}}=0\)，整月都是会后利率）

2026-09-16：9 月有 30 天，\(d_{\mathrm{post}}=14\ge 10\) → `ZQU26.CBT`。
2026-10-28：10 月会后不足 10 天 → 用 11 月 `ZQX26.CBT`。

### 3.4 手工算一遍（建议对照页面）

以 **2026-09-16** 那次会、快照日当天为例。后面的会把第 6 步的 \(\mathrm{post\_rate}\) 当作新的 base 再做一次。

1. 日历确认末日与 SEP：<https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm>
2. 打开声明：<https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm>
   核对 Stance 的区间、票型、异议（会前没有新声明则 Stance 仍是上一会，例如 7 月 [monetary20260729a.htm](https://www.federalreserve.gov/newsevents/pressreleases/monetary20260729a.htm)）。
3. EFFR：<https://fred.stlouisfed.org/series/EFFR> 取快照日或之前最近值（例：3.63）。纽约联储对照：<https://www.newyorkfed.org/markets/reference-rates/effr>
4. 按 3.3 选出合约，打开 Yahoo 日收盘。9 月会 → <https://finance.yahoo.com/quote/ZQU26.CBT/>（例：\(P=96.26\)）
5. 代入第 4 节公式：\(N=30\)，\(d_{\mathrm{pre}}=16\)，\(d_{\mathrm{post}}=14\)，\(\mathrm{base}=3.63\)
   - \(\mathrm{implied}=100-96.26=3.74\)
   - \(\mathrm{hold}=3.63\)
   - \(\mathrm{hike}=(3.63\times 16 + 3.88\times 14)/30 \approx 3.7467\)
   - \(p_{\mathrm{hike}}=(3.74-3.63)/(3.7467-3.63)\approx 94\%\)，Hold 约 6%，Cut = 0
   与页面允许差 1–2pp（取价时点、四舍五入）。
6. 会后利率 \(\mathrm{post\_rate}=(3.74\times 30 - 3.63\times 16)/14 \approx 3.87\)。10 月会打开 <https://finance.yahoo.com/quote/ZQX26.CBT/>，用这个 base 再算「再加一次」。
7. 方向对照（可选）：<https://www.cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html>
   不要逐位对齐。FedWatch 有更多利率桶和季末处理；我们是 25bp 三档。

折线上任意一天：把第 3–5 步的「快照日」换成那一天的 FRED 观测和 Yahoo 该日收盘即可。周末往往重复周五的价。

### 3.5 快照里记下了哪些输入（不必猜用了哪份合约）

每天入库的那一行就是当日手算的原料。浏览器开发者工具 → Network → `GET /api/fomc/dashboard` → `probabilities.next_meetings[].latest`：

| JSON 字段 | 含义 | 打开哪一页核对 |
|---|---|---|
| `asof_date` | 快照日 | FRED / Yahoo 都取这一天（或之前最近交易日） |
| `futures_contract` | Yahoo 符号，如 `ZQU26.CBT` | `https://finance.yahoo.com/quote/{符号}/` |
| `futures_price` | 当日（或最近）日收盘 \(P\) | 同上页的日 K close |
| `effr` | 基准利率 | [FRED EFFR](https://fred.stlouisfed.org/series/EFFR) |
| `implied_rate` | \(100-P\) | 用价格自己乘减即可 |
| `p_hold` / `p_hike` / `p_cut` | 插值结果 | 第 4 节公式 |
| `method` | `zq_v1` = 在市月合约；`zq_front_v1` = 连续合约回填 | 后者分辨率差一截，见第 5 节 |

Priced in 卡片底部会露出同一组数：合约代码点进 Yahoo，EFFR 点进 FRED。`source_url` 在 Stance / 文本对象上，不是概率快照里；声明、纪要、RSS 条目的 `link` 都是当时抓到的官网地址。

**Stance 为什么可信**：主数据是声明 HTML，不是终端二次录入。卡片 Statement 就是那一页。

**Priced in 为什么不是编的**：恒等式来自 CME 合约定义（报价 = 100 − 月均 EFFR 预期），价来自 Yahoo 公开日 K，基准来自 NY Fed / FRED。没有 FedWatch 付费 API。

**Text 为什么标模型名**：概括是 LLM，不是官方中文。引用用 Fed.gov。

---

## 4. 概率怎么算（准确性）

方法名 **zq_v1**。与 FedWatch 同原料、同 25bp 步长；我们额外做了两件 FedWatch 页面不展示、但会改变远月数字的处理。

### 4.1 单次会议

设期货价 \(P\)，合约月天数 \(N\)，决议日前天数 \(d_{\mathrm{pre}}\)、会后天数 \(d_{\mathrm{post}}\)，基准利率 \(\mathrm{base}\)：

\[
\begin{aligned}
\mathrm{implied} &= 100 - P \\
\mathrm{hold} &= \mathrm{base} \\
\mathrm{cut} &= \bigl(\mathrm{base}\cdot d_{\mathrm{pre}} + (\mathrm{base}-0.25)\cdot d_{\mathrm{post}}\bigr)/N \\
\mathrm{hike} &= \bigl(\mathrm{base}\cdot d_{\mathrm{pre}} + (\mathrm{base}+0.25)\cdot d_{\mathrm{post}}\bigr)/N
\end{aligned}
\]

把 \(\mathrm{implied}\) 插在三种**月均**情景之间，得到 Hold / Hike / Cut，裁剪到 \([0,1]\)。

合约规则：会后剩余天数 \(\ge 10\) 用当月 ZQ，否则用下月（月末会议当月只剩几天，对决议不敏感）。

### 4.2 基准必须用 EFFR，不能用区间中值

有效联邦基金利率常年偏离目标区间中值几个 bp。2026-09-16 实测：NY Fed EFFR **3.63%**，区间中值 3.625%。用中值会把 9 月 Hike 从约 94% 抬到约 99%——系统性偏鹰。本模块优先 FRED `EFFR`，失败才退回中值。

### 4.3 远月必须用「会后利率」递推

远月 ZQ 定价的是**累计路径**。第一次会 \(\mathrm{base}=\) EFFR；之后：

\[
\mathrm{post\_rate} = \bigl(\mathrm{implied}\cdot N - \mathrm{base}\cdot d_{\mathrm{pre}}\bigr)/d_{\mathrm{post}}
\]

下一会用 \(\mathrm{post\_rate}\)，**不是**合约月均 \(\mathrm{implied}\)。若误用月均，会把「9 月已经加过的那次」再算进 10 月：实测 10 月 Hike 会从约 46% 虚高到约 98%。

### 4.4 三点模型，不是 FedWatch 全分布

\(\mathrm{implied}>\mathrm{hold}\) 时只在 Hold 与 Hike 之间分，**Cut 为 0**；反过来 Hike 为 0。图上全程为 0 的那条线不画，并有一句说明。这是方法选择，不是漏数。FedWatch 会把概率铺在 0 / 25 / 50 / 75bp 多个桶里，两边可以同时有尾巴；我们对齐的是「下一次 25bp 动作」的主导方向。

### 4.5 和外部数字怎么对

先打开第 3 节的网页，再对快照字段。完整逐步算例见 **3.4**。

| 核对项 | 打开 | 预期 |
|---|---|---|
| 会议末日 / SEP | [fomccalendars.htm](https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm) | 与时间轴日期一致；`*` = SEP |
| Stance 区间与票型 | 声明，如 [monetary20260916a.htm](https://www.federalreserve.gov/newsevents/pressreleases/monetary20260916a.htm) | 与卡片 Statement 同一 URL |
| EFFR | [FRED EFFR](https://fred.stlouisfed.org/series/EFFR) · [纽约联储](https://www.newyorkfed.org/markets/reference-rates/effr) | 等于快照 `effr`（约 3.63，不是 3.625） |
| 期货价 \(P\) | [ZQU26.CBT](https://finance.yahoo.com/quote/ZQU26.CBT/)（先按 3.3 换符号）或 [CME 报价板](https://www.cmegroup.com/markets/interest-rates/stirs/30-day-federal-fund.quotes.html) | 等于快照 `futures_price`；\(\mathrm{implied}=100-P\) |
| 9 月会手算 | 上两行 + 第 4.1 节公式 | \(P=96.26\)，EFFR \(3.63\) → implied \(3.74\)，Hike \(\approx 94\%\)，Hold \(\approx 6\%\)，Cut \(0\) |
| 10 月会递推 | [ZQX26.CBT](https://finance.yahoo.com/quote/ZQX26.CBT/) + 9 月 \(\mathrm{post\_rate}\approx 3.87\) | Hike 约一半，而不是 ~98% |
| 对照（非数据源） | [CME FedWatch](https://www.cmegroup.com/markets/interest-rates/cme-fedwatch-tool.html) | 方向一致；允许差几个百分点 |

单元测试锁住了上述恒等式（`backend/tests/test_fomc_probabilities.py`）：官方 EFFR、会后利率链式、以及「误用月均会把 10 月打成近乎必加」。

历史会的 Expectation vs actual 用**会前**最后一张快照（不含决议公布后的收盘），避免用已经知道结果的价格来给模型打分。

---

## 5. 不要这样用

- 不要把 92% 读成「Fed 有 92% 的可能性应该加息」。那是期货隐含概率，会错，也曾错。
- 不要把后面几次会的 Hike% 加总，当成年内加息次数。
- 不要把 implied path 的 3.74 → 3.98 读成会后目标利率台阶。
- 不要和 FedWatch 逐位核对到 1%。原料相同，细节不同；方向反了再查。
- 不要把 AI 概括当声明引用。引用用 Fed.gov 原文。
- 不要在 SEP 会议上只看 Priced in。点阵图和经济预测不在期货价里。
- 周末折线可能平一段：按日历日存快照，周六日往往重复周五的价，不是新信息。
- 历史回填若标了 `front-month approx.`（连续合约近似），分辨率比在市月合约差一截（约 ±10–15pp）。

---

## 6. 更新节奏

无需手动刷新。声明/纪要/RSS 约每 30 分钟扫一次；概率每个日历日一张快照。打开页面看到的就是库里最新的官方文本和当日（或最近一个交易日）定价。

---

## 7. 打开后的五问

1. 现在看的是哪一次会？（Priced in 右上角日期 / SEP，或时间轴高亮格）
2. Stance：官方上次做了什么？票型裂不裂？
3. Priced in：主导方向是谁、大概多少、7 天是在加强还是回撤？
4. 后面几次会上，市场还要不要再动一次？
5. 价和词有没有裂缝？若定价落空，谁更意外？

把这五句写进笔记，这个模块就完成了它该做的事：把官网事实、期货定价和委员会措辞放到同一张桌上，让你决定哪一条裂缝值得继续挖。
