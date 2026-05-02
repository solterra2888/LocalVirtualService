# 时区优化说明文档

> **背景**：应用服务器与家庭虚拟服务器均部署在香港（UTC+8）。用户分布在**美国**与**中国大陆**两个主要时段。若时间处理不规范，会出现日期错位、报告日期错乱等用户可见的问题。

---

## 一、为什么会出问题？先看一个真实案例

YouTube 告诉系统：Bloomberg 视频发布于 `2026-04-24 19:22 UTC`。

这个时间对应三个城市的写法完全不同：

| 城市 / 时区       | 同一个瞬间的写法   |
|-------------------|--------------------|
| UTC（国际标准）   | 4月24日 19:22      |
| 香港（UTC+8）     | **4月25日 03:22**  |
| 洛杉矶（UTC-7）   | 4月24日 12:22      |

**修复前**，系统的"标题"和"卡片"用了两套尺子：

```
┌─ April 24（标题：UTC日期）────────────────┐
│                                           │
│  📺 Bloomberg Brief...                    │
│                           APR 25  03:22   │  ← 卡片：香港本地时间
│                                           │
└───────────────────────────────────────────┘
```

同一条视频，标题写 **24 号**，卡片写 **25 号**——用户懵了。

**修复后**，全部改用"用户浏览器时区"这一把尺子：

- 香港用户看到：标题 "April 25"，卡片 "APR 25 03:22" ✅ 一致
- 洛杉矶用户看到：标题 "April 24"，卡片 "APR 24 12:22" ✅ 一致

---

## 二、共修复了四类问题

### 问题 1：数据库连接的"隐形炸弹"

**类比**：两人合伙记账，一人说"8点见"没说哪国时间，另一人默默按本地时间记下，对账时差了 13 小时。

**原因**：PostgreSQL 的 `NOW()` 函数和 Python 写入的"裸数字时间"，会按**数据库连接的当前时区**来解释。连接默认继承服务器系统时区——香港（UTC+8）。一旦将来迁机房或改系统配置，同一份代码写出来的时间就会漂移。

**修复**：每条数据库连接打开时，强制声明"我们说时间一律按 UTC 解释"：

```
options='-c TimeZone=UTC'   # 加在每次连接参数里
```

**效果**：无论服务器迁到哪里、操作系统时区怎么改，写入数据库的时间永远是 UTC，永不漂移。

---

### 问题 2：内容适配器的时间 fallback 偏移 8 小时

**背景**：当 YouTube / Twitter / RSS / 新闻来源没有提供发布时间时，系统会用"现在"作为兜底时间。

**修复前**：用的是 `datetime.now()`——取**服务器当地时间（香港，UTC+8）**。

这意味着：
- 一条缺少时间戳的推文，实际发布是 UTC `10:00`
- 系统 fallback 记成了香港 `18:00`（同一时刻但写法不同），存进数据库后与其他条目错位 8 小时

**修复后**：全部改用 `utcnow()`，严格写入 UTC：

```
# YouTube / Twitter / News / RSS 四个适配器统一改动
datetime.now()  →  utcnow()    # 来自 backend/utils/datetime_utils.py
```

---

### 问题 3：Daily Report 的"昨天"算错

**场景**：用户点击"生成昨天的报告"，后台要知道"昨天"是哪一天。

**修复前**：

```python
昨天 = date.today() - 1    # date.today() 取服务器（香港）的今天
```

举例：**洛杉矶用户深夜 11:00 PM 点击生成**：
- LA 当地时间：4月24日 23:00，他认为的"昨天" = 4月23日
- 同一时刻香港：4月25日 14:00，服务器认为今天是 25 号
- 服务器算出"昨天 = 4月24日"→ **报告日期错了整整一天**

**修复后**：API 新增 `tz`（用户时区）参数，按用户所在时区算"昨天"：

```json
POST /api/reports/twitter-daily/generate
{ "tz": "America/Los_Angeles" }
```

- 用户 LA 23:00 → 算出"LA 的昨天 = 4月23日" → 拿到正确报告 ✅
- 用户北京 23:00 → 算出"北京的昨天 = 4月24日" → 拿到正确报告 ✅

---

### 问题 4：前端日期分组与卡片时间"两套尺子"

即开篇 Bloomberg 视频的例子。

**修复前**：

| 步骤 | 用的尺子 | 结果 |
|------|----------|------|
| 分组标题 | UTC（直接切 ISO 字符串前 10 位） | "April 24" |
| 卡片时间 | 浏览器本地（香港 UTC+8） | "APR 25 03:22" |

**核心代码改动**：

```typescript
// 修复前：粗暴截取字符串前 10 位，拿到的是 UTC 那一天
const formatDateKey = (iso: string) => iso.slice(0, 10);

// 修复后：先换算到用户时区，再取年月日
const dateKeyInTz = (iso: string, tz: string) =>
  dayjs.utc(iso).tz(tz).format('YYYY-MM-DD');
```

**修复后**：新建 `web-frontend/src/utils/datetime.ts`，全部走"用户浏览器时区"：

| 步骤 | 用的尺子 | 结果（香港用户） |
|------|----------|------|
| 分组标题 | 用户时区（HK UTC+8） | "April 25" |
| 卡片时间 | 用户时区（HK UTC+8） | "APR 25 03:22" |

两者一致，不再打架。

#### 用 5 条真实数据验证

假设后端发来这 5 张卡片（时间均为 UTC）：

| # | 标题 | `published_at`（UTC） |
|---|------|-----------------------|
| A | 早盘新闻 | `2026-04-25 01:00` |
| B | Bloomberg Brief | `2026-04-24 19:22` ← 你那条 |
| C | Twitter 推文 | `2026-04-24 15:30` |
| D | YouTube 视频 | `2026-04-24 08:00` |
| E | 凌晨发的推 | `2026-04-23 23:50` |

**修复前**（按 UTC 分组，卡片按香港显示）：

```
┌─ April 24（UTC 日期）──────────────────────┐
│  B  Bloomberg Brief       APR 25  03:22   │  ⚠️ 标题24，卡片25
│  C  Twitter 推文          APR 24  23:30   │
│  D  YouTube 视频          APR 24  16:00   │
└────────────────────────────────────────────┘
┌─ April 23（UTC 日期）──────────────────────┐
│  E  凌晨发的推            APR 24  07:50   │  ⚠️ 标题23，卡片24
└────────────────────────────────────────────┘
```

**修复后 · 香港用户（UTC+8）**：UTC 时间先 +8 小时，再按香港"自然日"分组：

| # | UTC | → 香港时间 | 分到哪天 |
|---|-----|------------|----------|
| A | 04-25 01:00 | 04-25 09:00 | **4-25** |
| B | 04-24 19:22 | 04-25 03:22 | **4-25** ✅ |
| C | 04-24 15:30 | 04-24 23:30 | **4-24** |
| D | 04-24 08:00 | 04-24 16:00 | **4-24** |
| E | 04-23 23:50 | 04-24 07:50 | **4-24** ✅ |

```
┌─ April 25（香港自然日）────────────────────┐
│  A  早盘新闻              APR 25  09:00   │  ✅ 一致
│  B  Bloomberg Brief       APR 25  03:22   │  ✅ 一致
└────────────────────────────────────────────┘
┌─ April 24（香港自然日）────────────────────┐
│  C  Twitter 推文          APR 24  23:30   │  ✅ 一致
│  D  YouTube 视频          APR 24  16:00   │  ✅ 一致
│  E  凌晨发的推            APR 24  07:50   │  ✅ 一致
└────────────────────────────────────────────┘
```

**修复后 · 美西用户（UTC-7）**：UTC 时间先 -7 小时，再按洛杉矶"自然日"分组：

| # | UTC | → 洛杉矶时间 | 分到哪天 |
|---|-----|--------------|----------|
| A | 04-25 01:00 | 04-24 18:00 | **4-24** |
| B | 04-24 19:22 | 04-24 12:22 | **4-24** |
| C | 04-24 15:30 | 04-24 08:30 | **4-24** |
| D | 04-24 08:00 | 04-24 01:00 | **4-24** |
| E | 04-23 23:50 | 04-23 16:50 | **4-23** |

```
┌─ April 24（洛杉矶自然日）──────────────────┐
│  A  早盘新闻              APR 24  18:00   │  ✅ 一致
│  B  Bloomberg Brief       APR 24  12:22   │  ✅ 一致
│  C  Twitter 推文          APR 24  08:30   │  ✅ 一致
│  D  YouTube 视频          APR 24  01:00   │  ✅ 一致
└────────────────────────────────────────────┘
┌─ April 23（洛杉矶自然日）──────────────────┐
│  E  凌晨发的推            APR 23  16:50   │  ✅ 一致
└────────────────────────────────────────────┘
```

> 注意：香港用户"4-24 这组有 3 张"，洛杉矶用户"4-24 这组有 4 张"——这不是 bug，因为同一段时间对两地来说本来就跨了不同的"自然日"。后端数据、数据库都没动，改的只是**用谁的日历来分组**。

---

## 三、整体架构：三层契约

这次优化确立了一套"三层契约"，让系统各环节说同一种语言：

```
┌─────────────────────────────────────────────────────────────────┐
│  层 1：存储层（数据库）                                           │
│  规则：所有时间只存 UTC（无论服务器在哪里）                        │
│  保证：每条 DB 连接强制 SET TimeZone=UTC                          │
└─────────────────────────────────────────────────────────────────┘
                          ↓ API 输出
┌─────────────────────────────────────────────────────────────────┐
│  层 2：传输层（后端 API）                                         │
│  规则：输出时间一律带 UTC 标记（ISO 8601: ...Z 或 +00:00）         │
│  保证：_row_to_item 中 ensure_utc() 兜底                          │
└─────────────────────────────────────────────────────────────────┘
                          ↓ JSON 到前端
┌─────────────────────────────────────────────────────────────────┐
│  层 3：展示层（前端）                                              │
│  规则：展示时间一律按"用户时区"渲染                                │
│  保证：DashboardSettingsContext.userTimezone（默认浏览器自动检测） │
└─────────────────────────────────────────────────────────────────┘
```

---

## 四、改动文件清单

### 后端（Python）

| 文件 | 改动内容 |
|------|----------|
| `backend/utils/datetime_utils.py` ✨ 新建 | 集中工具函数：`utcnow / ensure_utc / yesterday_in_tz / day_window_utc` 等 |
| `backend/database/db_manager.py` | 每条 psycopg2 连接加 `-c TimeZone=UTC` |
| `backend/services/content_adapters/youtube_adapter.py` | `datetime.now()` → `utcnow()` |
| `backend/services/content_adapters/twitter_adapter.py` | `datetime.now()` → `utcnow()` |
| `backend/services/content_adapters/news_adapter.py` | `datetime.now()` → `utcnow()` |
| `backend/services/content_adapters/rss_adapter.py` | `datetime.now()` → `utcnow()` |
| `backend/api/twitter_daily_reports.py` | `_parse_date()` 新增 `tz` 参数；缺省用 UTC 算"昨天" |
| `backend/api/youtube_daily_reports.py` | 同上 |
| `backend/services/scheduler_service.py` | Twitter/YouTube 摘要任务的"昨天"走 `yesterday_in_tz()` |
| `backend/api/daily.py` | `_row_to_item` 用 `ensure_utc()` 包装 naive datetime |

### 前端（TypeScript / React）

| 文件 | 改动内容 |
|------|----------|
| `web-frontend/src/utils/datetime.ts` ✨ 新建 | dayjs+timezone 封装：`dateKeyInTz / formatDateLabel / formatTime / relativeTime` |
| `web-frontend/src/contexts/DashboardSettingsContext.tsx` | 新增 `userTimezone / setUserTimezone / resetUserTimezone`（默认浏览器自动检测） |
| `web-frontend/src/pages/DailyPage.tsx` | 分组 key + 分组标题改用用户时区 |
| `web-frontend/src/pages/MobileDailyPage.tsx` | 同上 |
| `web-frontend/src/components/daily/DailyCard.tsx` | 卡片时间改用用户时区 |

---

## 五、什么应该做，什么不应该做

| ❌ 禁止写法 | ✅ 推荐写法 | 原因 |
|---|---|---|
| `datetime.now()`（Python） | `utcnow()` | 服务器在 HK，写出来比 UTC 大 8 小时 |
| `datetime.utcnow()`（Python） | `utcnow()` | Python 3.12 已废弃，且不带时区标记 |
| `date.today()` 算"昨天" | `yesterday_in_tz(tz)` | 取服务器本地日期，美国用户会错一天 |
| 前端 `iso.slice(0, 10)` | `dateKeyInTz(iso, tz)` | 切的是 UTC 日期，与本地时间不一致 |
| 前端 `new Date(iso).toLocaleDateString('en-US')` | `formatDateLabel(iso, tz)` | 写死语言，缺乏统一时区入口 |

---

## 六、留待未来的事项

以下两项属于"数据库结构改造"，风险较高，本次未做，留作长期演进：

1. **把核心时间列从 `timestamp` 改成 `timestamptz`**

   目前 `subscription_content.published_at` 等列是不带时区标记的 `timestamp without time zone`，全靠"连接 SET TimeZone=UTC"这层约定保证正确。未来可迁移为 `timestamptz`，届时数据库本身就能保证时区正确，不再依赖约定。

   迁移 SQL 模板（仅参考，需在维护窗口执行）：
   ```sql
   ALTER TABLE subscription_content
     ALTER COLUMN published_at TYPE timestamptz
     USING published_at AT TIME ZONE 'UTC';
   ```

2. **Daily Report 的 `report_date` 支持"用户时区版本"**

   目前报告用 `DATE` 类型存"哪一天"，天然不带时区。若未来需要支持"用户 A 的 4-25 报告"与"用户 B 的 4-25 报告"分别按各自时区算，需新增 `report_date_tz` 字段与之配合。

---

## 七、关键工具函数速查

### 后端（`backend/utils/datetime_utils.py`）

| 函数 | 用途 |
|---|---|
| `utcnow()` | 获取带 UTC 标记的当前时间，替代 `datetime.now()` |
| `ensure_utc(dt)` | 把从数据库读出的 naive datetime 打上 UTC 标记 |
| `to_naive_utc(dt)` | 写入数据库前把任意时间转成 naive UTC |
| `yesterday_in_tz(tz)` | 按用户时区算"昨天"，替代 `date.today() - 1` |
| `day_window_utc(date, tz)` | 把"用户时区的某一天"换算为 UTC 起止区间，用于 SQL `WHERE published_at >= ? AND < ?` |

### 前端（`web-frontend/src/utils/datetime.ts`）

| 函数 | 用途 |
|---|---|
| `detectBrowserTimezone()` | 自动检测浏览器时区（如 `Asia/Shanghai` / `America/New_York`） |
| `dateKeyInTz(iso, tz)` | 生成分组 key（`YYYY-MM-DD`），按用户时区 |
| `formatDateLabel(iso, tz)` | 生成分组标题（如 `April 25, 2026`），支持 zh/en |
| `formatTime(iso, tz)` | 生成卡片时间（如 `APR 25 03:22`），按用户时区 |

---

## 七点五、Phase 2 系统级整治（2026-04-25）

> 第一轮整治后用户反馈"刚上传的文件 Daily 模块仍显示明天日期"。深挖发现 `file_processing_details.created_at` 是 Python `datetime.now()` 直接写入的 HK 本地时间。第一轮的"连接 SET TimeZone=UTC"对 Python naive datetime 入库**无效**（因为 psycopg2 是把 naive datetime 当字面量 `'2026-04-25 17:46:53'` 直接发给 PG，PG 当 timestamp without time zone 直接存进去，根本走不到 session timezone 转换）。

### 7.5.1 三道防线

| 防线 | 作用 | 实现位置 |
|---|---|---|
| **① psycopg2 适配器** | 系统级安全网。任何 naive datetime 写库都被自动当 HK 转 UTC；任何 aware datetime 都被转 UTC。**未来即使代码继续用 `datetime.now()` 也不会出错。** | `backend/utils/datetime_utils.py::install_psycopg2_utc_adapter`<br>启动时被 `backend/main.py` 与 `backend/celery_app.py` 调用 |
| **② Pydantic UtcDateTime** | API 响应里所有 `datetime` 字段统一以 ISO 8601 `+00:00` 输出，前端 `new Date()` 自动转用户本地。 | `backend/utils/datetime_utils.py::UtcDateTime` (Annotated + PlainSerializer)<br>各 schema 文件已批量改用 `UtcDateTime` |
| **③ 历史数据迁移** | 把存量"未来时间"行（明显是 HK 本地误存）整体 -8h 校正回 UTC。 | `backend/database/migrations/fix_file_processing_details_utc.sql` (Phase 1)<br>`backend/database/migrations/fix_all_naive_timestamps_to_utc.sql` (Phase 2) |

### 7.5.2 适配器到底做了什么

```python
# backend/utils/datetime_utils.py
def install_psycopg2_utc_adapter() -> None:
    def _adapter(dt: datetime):
        if dt.tzinfo is None:
            # naive: 视为服务器物理 TZ (HK), 转 UTC, 输出 naive UTC
            dt = dt.replace(tzinfo=SERVER_TZ).astimezone(UTC).replace(tzinfo=None)
        else:
            # aware: 转 UTC, 输出 naive UTC
            dt = dt.astimezone(UTC).replace(tzinfo=None)
        return psycopg2.extensions.AsIs(f"'{dt.isoformat()}'::timestamp")
    psycopg2.extensions.register_adapter(datetime, _adapter)
```

**例子**：

| Python 代码 | 适配器前（旧 BUG） | 适配器后（修复） |
|---|---|---|
| `cur.execute("INSERT...", [datetime.now()])` 在 HK 17:46 执行 | DB 存 `2026-04-25 17:46` (HK 本地，错了 8h) | DB 存 `2026-04-25 09:46` (UTC，正确) |
| `cur.execute("INSERT...", [utcnow()])` (aware UTC) | DB 存 `2026-04-25 09:46` ✅ | DB 存 `2026-04-25 09:46` ✅ |
| `cur.execute("INSERT...", [datetime.now(NY)])` aware NY | DB 存 NY 本地数字 ❌ | DB 存 `2026-04-25 09:46` UTC ✅ |

### 7.5.3 已迁移的高优先级表

| 表 | 列 | 迁移条件 |
|---|---|---|
| `file_processing_details` | `created_at` | 全表 -8h（Phase 1，所有行确认 HK 本地） |
| `subscription_content` | `fetched_at`, `published_at` | `> NOW() AT TIME ZONE 'UTC'` 的行 -8h |
| `subscription_list` | `created_at`, `updated_at`, `last_fetched_at`, `next_fetch_at` | 同上 |
| `subscription_content_tags`, `file_tags` | `created_at`, `tagged_at`, `updated_at` | 同上 |
| `user_daily_preferences`, `user_template_preferences`, `user_news_filter_presets` | `*_at` | 同上 |
| `user_execution_folders`, `file_folders`, `file_shares`, `users` | `created_at` | 同上 |
| `twitter_daily_reports`, `youtube_daily_reports` 及对应 `*_author_daily_summaries` | `created_at`, `updated_at` | 同上 |
| `news`, `news_themes_unified`, `theme_master_data` | 各 `*_at` | 同上 |

**安全保证**：迁移条件 `> NOW() AT TIME ZONE 'UTC'` 只命中"未来的时间"，因为合法 UTC 时间不可能在未来。这样既能修复 HK 本地误存的行，又**绝不会**误改通过 SQL `CURRENT_TIMESTAMP` 默认值（已经是 UTC 的）写入的合法行。

### 7.5.4 ⚠️ 适配器与 `utcnow_naive()` 的冲突（已修复）

适配器上线后立刻发现一个 BUG：刚上传的文件 Daily 仍显示比 Sources 早 8h。
根因是 `backend/utils/datetime_utils.utcnow_naive()` 与适配器**语义不兼容**：

| 函数 | 返回值 | 适配器看到后 | 结果 |
|---|---|---|---|
| `utcnow()` | aware UTC `10:36+00:00` | `astimezone(UTC)` → `10:36 naive` | ✅ DB 存 10:36 UTC |
| `datetime.now()` | naive HK `18:36` | "naive=HK" → 转 UTC → `10:36 naive` | ✅ DB 存 10:36 UTC |
| `utcnow_naive()` | **naive UTC** `10:36` | "naive=HK" → 当 18:36 HK 转 → `02:36 naive` | ❌ DB 存 02:36 UTC（早 8h） |

**修复**：

1. `utcnow_naive()` 已标记 `[DEPRECATED]`，docstring 写明陷阱与替代方案
2. 所有 DB 写入路径的 `utcnow_naive()` 已替换为 `utcnow()`（aware UTC）
3. 适配器期已写错的 2 行（`file_processing_details` id=1282, 1284）已 +8h 修复
4. 法则：**禁止再用 naive UTC 入库**。要么 aware UTC（`utcnow()`），要么 naive 本地（`datetime.now()` 配合适配器）

### 7.5.5 仍待处理（低优先级）

- `eodhd_fundamentals_service.py` 内部数据 fetch 用 `date.today()` 取"今天"做股票财务数据查询：影响仅限 fetch 触发时机，不影响展示，暂不改动。
- 财务/股价/macro 等 192 列 `timestamp without time zone` 中的非用户可见列：psycopg2 适配器已确保未来写入正确，存量数据保持原样（用户层不可见）。
- `subscription_content.published_at`：长期方案是把列类型改为 `timestamptz`（见六章 schema 演进 TODO）。

---

## 7.6 Twitter / News / EODHD 订阅拉取问题根治（2026-05-02）

> 排查触发：Walter Bloomberg (@DeItaone) 过去 48 小时 100 条推文数据库仅有 7 条（93% 缺失）。

### 7.6.1 问题汇总

| 模块 | 问题 | 根因 | 影响 |
|------|------|------|------|
| **Twitter** | 93% 推文缺失 | `twitterapi.io` `advanced_search` 索引延迟 12-20h；`smart_hours` 优化把窗口压到 2-3h 远小于延迟 | 🔴 致命：数据长期大量缺失 |
| **Twitter** | `fetched_at` / `last_fetched_at` +8h | `CURRENT_TIMESTAMP` 在某些 worker 实例 session TZ 未 UTC | 🟡 时区显示错乱 |
| **Twitter** | `published_at` 部分 +8h | psycopg2 UTC 适配器在同一批次内偶发未生效 | 🟡 时区显示错乱 |
| **Twitter** | `is_first_fetch` 误判 | author 强制小写后与 DB 大小写敏感 author_identifier 不匹配 | 🟢 低：只浪费 API 配额 |
| **News** | `fetched_at` +8h（141 行）| 同 Twitter：`CURRENT_TIMESTAMP` HK 时钟 | 🟡 时区显示错乱（无数据缺口）|
| **News** | `datetime.now()` 日期计算 | HK 午夜时 end_date 超前 1 天 | 🟢 低：下次 30min 自愈 |
| **EODHD 股价** | `datetime.now()` 日期计算 | 同上 | 🟢 低：days_back 窗口有冗余 |
| **YouTube** | 无问题 | home VM worker 已独立修好（7.5.5 节）| ✅ |

### 7.6.2 修复内容

| 文件 | 改动 |
|------|------|
| `backend/services/twitter_subscription_service.py` | 加 `min_lookback_hours=24` 下限；`default_query_hours` 24→48；`max_pages` 3→6/10；`_update_subscription_status` 用 `utcnow()` 显式传参；修 author 大小写不敏感比较 |
| `backend/services/subscription_content_service.py` | `fetched_at` 用 Python `utcnow()` 替代 SQL `CURRENT_TIMESTAMP` |
| `backend/services/content_adapters/twitter_adapter.py` | `published_at` 用 `ensure_utc()` 强制 aware UTC 兜底 |
| `backend/services/eodhd_news_feed_service.py` | `datetime.now()` → `utcnow()` 用于日期范围计算 |
| `backend/services/scheduler_service.py` | 两处 `recent_days` 日期计算 `datetime.now()` → `utcnow()` |
| `backend/services/eodhd_eod_stock_prices_service.py` | 两处 `datetime.now()` → `_utcnow()` 用于 EOD 日期范围 |
| `backend/database/migrations/fix_twitter_subscription_hk_clock_2026_05.sql` | 历史修正：twitter `fetched_at`/`published_at`/`last_fetched_at`/`fetch_time` HK 时钟行 -8h |

### 7.6.3 关键设计决策：Twitter lookback 窗口

`twitterapi.io` `advanced_search` 存在 **12-20h 搜索索引延迟**。推文发布后不会立即出现在搜索结果里，因此任何小于延迟时长的查询窗口都会导致推文永久丢失。

**根治方案**：强制最小 lookback 窗口为 `min_lookback_hours = 24h`（可通过 `TWITTER_MIN_LOOKBACK_HOURS` 环境变量调整）。重复内容由 `ON CONFLICT (channel_type, content_id)` 自然去重，零脏库风险。

```
smart_hours = max(min_lookback_hours, min(estimated_hours, default_query_hours))
```

### 7.6.4 部署注意

1. 重启远程 Celery `data_fetching` + `default` 队列 worker（让新代码生效）
2. 确认 `task_scheduling.twitter_subscription.enabled = true`（当前已禁用）
3. 新增环境变量（可选）：`TWITTER_DEFAULT_QUERY_HOURS=48`、`TWITTER_MIN_LOOKBACK_HOURS=24`

---

## 7.7 全代码库 `datetime.now()` 系统性清理（2026-05-02）

> 在修复 Twitter Feeds 后做了一次全 backend 扫描，发现还有 25+ 处 `datetime.now()` 在执行日期/时区敏感操作。

### 7.7.1 问题分类

代码里的 `datetime.now()` 用法分四类，本轮**只清理前两类**，第三类有适配器兜底（功能正常但不规范），第四类是无害用法：

| 用法分类 | 风险 | 处理 |
|---|---|---|
| **(A) 上游 API 日期参数** (`start_date`/`end_date`) | 🔴 HK 跨日 8 小时窗口 (00:00-08:00 HK) 内日期偏移 1 天，导致 EOD/IPO/财报/经济事件 API 返回空或错位 | ✅ 全部改用 `utcnow()` |
| **(B) 用户可见时间戳泄漏** (JSON metadata, API response, `read_at`/`analyzed_at`/`scraped_at`/`executed_at`) | 🟡 前端按浏览器时区渲染时显示偏 +8h | ✅ 全部改用 `utcnow().isoformat()` |
| **(C) DB INSERT 字面值** (`created_at`/`updated_at`/`last_login` 等) | 🟢 psycopg2 全局适配器兜底自动 HK→UTC，功能正常 | ⏸ 保持现状（详见 7.5.2 适配器机制）|
| **(D) 进程内时间测量** (`start = now(); ...; elapsed = now() - start`)、文件名时间戳、缓存 TTL | 🟢 完全无害（同进程内时间差与时区无关）| ⏸ 保持现状 |

### 7.7.2 本轮 (A)+(B) 修复清单

**(A) 上游 API 日期参数 — 9 个文件**

| 文件 | 用途 |
|---|---|
| `backend/services/market_data_service.py` | EOD 历史数据 from_date |
| `backend/tasks/data_tasks.py` | 单 ticker EOD start_date/end_date |
| `backend/services/scheduler_service.py` | 经济事件日历 from/to_date（3 处分支）|
| `backend/services/earnings_calendar_service.py` | 财报日历 fetch_date（去重 key 一部分）+ next_week + custom_period |
| `backend/services/ipo_calendar_service.py` | IPO 日历 today（2 处分支）|
| `backend/services/economic_events_service.py` | 经济事件最近/未来/按国家 3 个 API |
| `backend/services/watchlist_news_service.py` | watchlist 新闻日期范围 |
| `backend/api/dashboard.py` | 股票图表 + 投资组合历史价格日期范围（4 处）|
| `backend/services/investment_project_service.py` | 项目 ID 生成中的 YYYYMMDD 日期段 |

**(B) 用户可见时间戳 — 5 个文件**

| 文件 | 字段 |
|---|---|
| `backend/services/subscription_content_service.py` | `read_at`（已读时间）|
| `backend/services/news_analysis_service.py` | `analyzed_at`（分析时间，2 处）|
| `backend/api/news.py` | `executed_at`（删除统计 API 响应）|
| `backend/services/cctv_news_service.py` | `scraped_at` / `content_scraped_at`（3 处）|
| `backend/scrapers/futu_scraper.py` | `scraped_at` |

### 7.7.3 仍保留 `datetime.now()`（无需修改）

以下文件的 `datetime.now()` 属于 (C) 或 (D) 类，psycopg2 适配器或语义本身已正确，**不必清理**：

- `backend/services/eodhd_fundamentals_service.py` — 50+ 处 `datetime.now()` 用于 `created_at`/`updated_at`，适配器自动转 UTC
- `backend/api/files.py` / `backend/tasks/file_tasks.py` / `backend/workers/file_worker.py` — 文件上传 `upload_time`、INSERT 时间戳，适配器兜底
- `backend/auth/user_auth.py` — `last_login` INSERT
- `backend/services/market_data_service.py` 1003-1118 行 — 全是 elapsed timing
- `backend/services/oss_service.py` / `backend/api/markets.py` / `backend/api/scheduler.py` — 文件名时间戳（仅作唯一 ID，时区无关）
- `backend/main.py` 健康检查 timestamp（前端不消费）
- `backend/services/theme_unification_service.py` 缓存 TTL（同进程内时间差）

### 7.7.4 `date.today()` 残留

`backend/services/eodhd_fundamentals_service.py` 仍有 7 处 `date.today()` 用作 `data_date`。该服务通常在 HK 6:00 = UTC 22:00（同一天）触发，影响窗口仅是 HK 0:00-8:00 之间的手动重跑。**保持现状**（已在 7.5.5 节归类为低优先级）。

---

### 7.5.5 ⚠️ 家用虚拟机 Worker 的独立写入路径（已修复）

**症状**: YouTube Feed 'Why Americans Can't Buy Cheap Chinese EVs' 在 HK 当地 25 日 22:42 时,
前端 Daily 显示 `APR 26 02:01 AM` (跨日的"明天"). 数据库 `subscription_content`:
- `fetched_at = 2026-04-25 19:42:40` (HK 数值, 应为 UTC 11:42)
- `published_at = 2026-04-25 18:01:18` (HK 数值, 应为 UTC 10:01)

**根因**: 家用虚拟机部署的 **独立 Celery Worker** (`deployment/local_virtual_service/worker/`)
拥有自己的 `db.py`, 完全 **绕过** 主仓库的所有时区契约:

1. `psycopg2.connect()` 没有传入 `options='-c TimeZone=UTC'` → 连接 session 落入服务器
   默认时区 `Asia/Shanghai`. `CURRENT_TIMESTAMP::timestamp` 写入 `timestamp without time zone`
   列时, 数值是 HK 本地 (而非 UTC).
2. 没有注册 `psycopg2` UTC 适配器 → `dateutil.parse('...Z')` 返回的 aware UTC datetime,
   被 postgres 用 HK session 转换成 HK 本地后丢掉 tzinfo, 写入 +8h 的偏移值.
3. `_normalize_youtube` 的 fallback `published_dt = datetime.now()` 也是 HK 本地裸时间.

YouTube `subscription_content` 全部 2,976 行均由家用 Worker 写入, 因此**全表都受影响**.
News / Twitter 走主仓库路径, 不受影响.

**修复**:

| 文件 | 修改 |
|------|------|
| `deployment/local_virtual_service/worker/db.py` | 新增 `options='-c TimeZone=UTC'`; 注册 `_utc_datetime_adapter` (HK→UTC) |
| `deployment/local_virtual_service/worker/services.py` | `dateutil.parse` 后兜底贴 `tzinfo=timezone.utc`; fallback 改 `datetime.now(timezone.utc)` |
| `backend/database/migrations/fix_youtube_subscription_content_utc.sql` | 修正 `channel_type='youtube'` 中 `fetched_at/published_at > NOW UTC` 的行 -8h |

**部署提醒**: ⚠️ 家用 Worker 需要单独重新部署 (rsync 或 git pull + 重启 systemd 服务).
仅修改主仓库代码 + 重启远程服务**不够**.

---

## 八、相关文件索引

- `backend/utils/datetime_utils.py` — 后端时区工具库 + psycopg2 UTC 适配器 + UtcDateTime
- `backend/main.py` / `backend/celery_app.py` — 启动时调用 `install_psycopg2_utc_adapter()`
- `backend/database/db_manager.py` — 连接 `SET TimeZone='UTC'`
- `backend/database/migrations/fix_file_processing_details_utc.sql` — Phase 1 迁移
- `backend/database/migrations/fix_all_naive_timestamps_to_utc.sql` — Phase 2 迁移
- `backend/database/migrations/fix_youtube_subscription_content_utc.sql` — Phase 3 迁移 (家用 Worker)
- `backend/database/migrations/fix_twitter_subscription_hk_clock_2026_05.sql` — Phase 4 迁移 (Twitter + News HK 时钟)
- `backend/schemas/*.py` — datetime 字段统一改 UtcDateTime
- `backend/services/twitter_subscription_service.py` — Twitter 订阅抓取；`min_lookback_hours` 兜住 API 索引延迟
- `backend/services/subscription_content_service.py` — 通用订阅内容保存；`fetched_at` 显式 UTC
- `backend/services/content_adapters/twitter_adapter.py` — Twitter 适配器；`ensure_utc` 兜底
- `deployment/local_virtual_service/worker/db.py` — 家用 Worker 的连接层 (UTC adapter + TimeZone=UTC)
- `deployment/local_virtual_service/worker/services.py` — 家用 Worker 的内联 YouTube adapter (UTC fallback)
- `web-frontend/src/utils/datetime.ts` — 前端时区工具库
- `web-frontend/src/contexts/DashboardSettingsContext.tsx` — 用户时区偏好状态管理
- `docs/instructions/DAILY_MODULE.md` §13 — 时区契约完整版（含禁用/推荐对照表、schema 演进 TODO）

## 7.8 任务进度日志 timestamp 美国用户错位修复（2026-05-02 补丁）

### 背景
评估 `upload sources` 时间链路时发现：所有 Celery 任务进度日志（`tasks/file_tasks.py` / `tasks/naming_tasks.py` / `tasks/tagging_tasks.py` / `tasks/classify_tasks.py` / `tasks/feed_tagging_tasks.py` / `tasks/feed_summary_tasks.py`）写入 JSONB `logs[].timestamp` 时使用：

```python
'timestamp': datetime.now().isoformat()  # "2026-05-02T17:50:00" — naive, 无 tz 标记
```

前端 `LogViewer` / `useLogger` 用 `new Date(log.timestamp).toLocaleString()` 渲染。JS `new Date()` 解析**无时区标记的 ISO 字符串**时会**当作浏览器本地时区**：

| 用户位置 | 浏览器解释 | 真实时刻 | 误差 |
|---|---|---|---|
| 香港/中国 (UTC+8) | HK 17:50 | HK 17:50 | ✅ 巧合对（与服务器同时区） |
| 美东 (UTC-5) | NY 17:50 | NY 05:50 | ❌ 晚 12 小时 |
| 美西 (UTC-8) | LA 17:50 | LA 02:50 | ❌ 晚 15 小时 |

这是 §7.7 评估时被误归类为低优先级的"漏网之鱼"——只对**美国用户**有显著影响（HK/CN 用户因与服务器同 UTC+8 巧合显示正确）。

### 修复
全部改为 `utcnow().isoformat()`，输出 `"2026-05-02T09:50:00+00:00"`，前端解析时按浏览器时区正确转换。

涉及文件：
- `backend/tasks/file_tasks.py` — 12 处（11 个 log timestamp + 1 个 INSERT 字面值改 `utcnow()` 让适配器接管）
- `backend/tasks/naming_tasks.py` — 1 处
- `backend/tasks/tagging_tasks.py` — 1 处
- `backend/tasks/classify_tasks.py` — 1 处
- `backend/tasks/feed_tagging_tasks.py` — 1 处
- `backend/tasks/feed_summary_tasks.py` — 1 处
- `backend/api/files.py` — 1 处（YouTube 本地路径 `processing_stats` INSERT 改 `utcnow()`）
- `backend/api/dashboard.py` — 1 处（健康检查端点 timestamp）
- `backend/services/oss_service.py` — 2 处（OSS 上传返回的 `upload_time`）
- `backend/api/screener.py` — 1 处（手动数据更新返回的 `last_update`）

### 跨地区影响评估（修复后）

**对所有用户都是改善**：
- HK/CN 用户：之前巧合对，修复后仍正确（前端 dayjs 把 +00:00 转成 Asia/Shanghai 显示）
- 美东用户：之前 log 时间晚 12 小时，修复后正确显示 NY 本地时间
- 美西用户：之前 log 时间晚 15 小时，修复后正确显示 LA 本地时间
- 其他地区：浏览器自动检测时区，全部正确

### `Upload Sources` 时间链路总评

| 字段 | 写入路径 | 状态 |
|---|---|---|
| `files.upload_time` | Pydantic `UtcDateTime` 类型 + `datetime.now()` 经 psycopg2 适配器 → UTC | ✅ 正确 |
| `file_processing_details.created_at` | 大部分 `utcnow()` (aware UTC)，少部分 `datetime.now()` 经适配器 | ✅ 正确 |
| `processing_stats.timestamp` (本次修复) | 之前传 `.isoformat()` 字符串绕过适配器 → 已改 `utcnow()` 让适配器接管 | ✅ 修复完成 |
| 任务进度 `logs[].timestamp` (本次修复) | 之前 naive ISO 字符串 → 已改 `utcnow().isoformat()` 带 +00:00 | ✅ 修复完成 |
| OSS `upload_time` (本次修复) | 之前 naive ISO 字符串 → 已改 `utcnow().isoformat()` | ✅ 修复完成 |
