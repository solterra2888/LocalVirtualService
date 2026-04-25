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

**修复后**：新建 `web-frontend/src/utils/datetime.ts`，全部走"用户浏览器时区"：

| 步骤 | 用的尺子 | 结果（香港用户） |
|------|----------|------|
| 分组标题 | 用户时区（HK UTC+8） | "April 25" |
| 卡片时间 | 用户时区（HK UTC+8） | "APR 25 03:22" |

两者一致，不再打架。

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

## 八、相关文件索引

- `backend/utils/datetime_utils.py` — 后端时区工具库（含注释说明每个函数的用途）
- `web-frontend/src/utils/datetime.ts` — 前端时区工具库
- `web-frontend/src/contexts/DashboardSettingsContext.tsx` — 用户时区偏好状态管理
- `docs/instructions/DAILY_MODULE.md` §13 — 时区契约完整版（含禁用/推荐对照表、schema 演进 TODO）
