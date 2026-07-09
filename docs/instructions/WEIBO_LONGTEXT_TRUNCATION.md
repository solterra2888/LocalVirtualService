# 微博长文截断问题排查与修复方案

> **排查日期**：2026-07-09
> **影响范围**：Feeds → 微博渠道（`channel_type='weibo'`）的长博文
> **现象**：部分微博博文正文被截断，结尾恒为 `...全文`，全文未抓取完整

---

## 1. 概述

微博模块（架构对齐 zsxq，详见 `backend/services/weibo_subscription_service.py`）存在一个长文抓取缺陷：

- 微博移动端 API（`m.weibo.cn`）对长博文（`isLongText=true`）在**列表接口**只返回截断正文（约 140 字 + `...全文` 后缀），完整正文需要二次调用 `m.weibo.cn/statuses/show` 展开。
- 现有代码用 `_expand_long_posts` 做了这个二次展开，但**展开在访客模式下频繁失败**，且失败后截断正文被**永久写入** `subscription_content`，后续抓取无法修复。

对生产库（`guanhe_database`）的实测：**1283 条微博博文中 104 条（≈8%）正文被截断**，结尾为 `...全文`。

---

## 2. 现象与证据

### 2.1 数据库实测

```sql
-- 被截断的长文（结尾 ...全文），按长度倒序
SELECT id, author_name, length(content) AS len
FROM subscription_content
WHERE channel_type = 'weibo' AND content LIKE '%全文'
ORDER BY length(content) DESC;
```

| 指标 | 值 |
|------|----|
| 微博博文总数 | 1283 |
| 结尾为 `...全文` 的截断博文 | **104** |
| 截断占比 | ≈ 8% |
| 含 `全文` 但**非**截断结尾的（误判风险） | **0**（`content LIKE '%全文%' AND content NOT LIKE '%...全文'` 返回 0 行）|

截断样本（`id=1426067` 但斌，正文结尾明显断句）：

```
转：AI，泡沫即飞轮？MIT经济学家 Ricardo Caballero 在最新工作论文……传统金融认为，估值来自基本面。
未来现金流决定今天的价格。如果价格远远高于现 ...全文
```

### 2.2 截断分布

**按作者**（长文高频博主更集中）：

| 作者 | 截断数 |
|------|--------|
| 任泽平 | 24 |
| 钟本聪Dylan | 21 |
| 但斌 | 15 |
| 船老大美股日志 | 15 |
| 梁建章-关注人口问题 | 8 |
| 券商中国 | 8 |
| 李想 | 8 |
| 其余（李开复/老曾阿牛/经纬张颖/雷军） | 各 1–2 |

**按入库日期**（越近越严重，与访客模式累积风控一致）：

| 日期 | 截断数 |
|------|--------|
| 2026-07-04 | 18 |
| 2026-07-05 | 18 |
| 2026-07-06 | 22 |
| 2026-07-07 | 5 |
| 2026-07-08 | 3 |
| 2026-07-09 | **38** |

### 2.3 关键环境状态

```sql
SELECT id, label, status FROM weibo_credentials;   -- 结果：0 行（空表）
```

`weibo_credentials` **为空** → `WeiboSubscriptionService._get_active_credential()` 返回 `None` → 系统运行在**访客模式**（crawl4weibo 用 Playwright 自动获取访客 Cookie）。访客模式是微博反爬最严的通道，是截断频发的直接诱因。

### 2.4 展开机制本身是有效的（证明可修复）

库里存在已成功展开的长文（非 `...全文` 结尾）：

| id | 作者 | 正文长度 | 截断结尾? |
|----|------|----------|-----------|
| 1468387 | 任泽平 | **3811** | 否 |
| 1475238 | 但斌 | 2326 | 否 |
| 1466433 | 但斌 | 2139 | 否 |

微博列表接口对长文只返回 ~140 字，因此 3811 字的正文**必然**来自 `get_post_by_bid` 成功展开。**结论：展开链路可用，只是间歇性失败**，因而具备修复可行性。

---

## 3. 根因分析

截断由三个环节叠加造成，前两个是"为什么会截断"，第三个是"为什么截断后无法自愈"。

### 3.1 根因 A：访客模式下 `statuses/show` 频繁被反爬拦截

`WeiboClientWrapper._expand_long_posts` 调用 crawl4weibo 的 `get_post_by_bid(bid)`，后者请求：

```
GET https://m.weibo.cn/statuses/show?id=<bid>
```

实测（无有效访客 Cookie 时）该接口返回 **HTTP 200 + HTML**，内容为 **"Sina Visitor System"** 访客验证挑战页，而非 JSON：

```html
<!DOCTYPE html>
<html><head><title>Sina Visitor System</title>...</head>...
```

而 crawl4weibo 的 `_request`（`crawl4weibo/core/client.py:367-368`）：

```python
if response.status_code == 200:
    return response.json()    # ← 访客系统返回 HTML 时，这里抛 ValueError
```

`.json()` 在 HTML body 上抛出 `json.JSONDecodeError`（`ValueError` 子类），**它不是 `requests.exceptions.RequestException`**，因此**绕过**了下方的 `except requests.exceptions.RequestException` 重试逻辑，直接向上抛出。

> 此外，访客模式下还频繁出现 **HTTP 432**（微博反爬"频繁"）。`_request` 对 432 重试 3 次（每次 sleep 4–7s）后抛 `NetworkError`。

### 3.2 根因 B：展开失败的兜底逻辑过弱

`backend/services/weibo_subscription_service.py::_expand_long_posts`：

```python
LONG_TEXT_EXPAND_RETRIES = 2          # 单条长文展开重试次数
LONG_TEXT_EXPAND_INTERVAL = 5.0       # 长文之间间隔

for attempt in range(1, LONG_TEXT_EXPAND_RETRIES + 1):
    try:
        full = self._client.get_post_by_bid(post.bid, use_proxy=False)
        if full and getattr(full, "text", ""):
            post.text = full.text
        break
    except _C4WAuthError as e:
        raise WeiboCookieExpiredError(...)        # 凭证失效才上抛
    except Exception as e:                         # ← 其余全部兜底
        if attempt < LONG_TEXT_EXPAND_RETRIES:
            time.sleep(random.uniform(4, 7))
        else:
            logger.warning("...展开 %d 次均失败，保留截断文本")
```

问题：

1. 兜底 `except Exception` 吞掉了 3.1 中的 `ValueError`（访客系统 HTML）与 `NetworkError`（432），重试 2 次后**保留 `...全文` 截断正文**继续走入库流程。
2. **没有对展开结果做有效性校验**：即使 `get_post_by_bid` 返回了仍以 `...全文` 结尾的文本（极端情况），也会被当作成功写入。
3. 重试间隔（4–7s）与次数（2）在访客模式下不足以等到风控解除。

### 3.3 根因 C（关键）：截断正文一旦入库即永久化

`backend/services/subscription_content_service.py::_save_normalized_content` 的 `ON CONFLICT` 子句（第 163–184 行）**不包含 `content` 列**：

```sql
ON CONFLICT (channel_type, content_id) DO UPDATE SET
    user_ids = ARRAY(SELECT DISTINCT UNNEST(subscription_content.user_ids || EXCLUDED.user_ids)),
    engagement_count = EXCLUDED.engagement_count,
    metadata        = EXCLUDED.metadata,
    ...
    published_at    = EXCLUDED.published_at
    -- ⚠ 没有 content = ... 即使重新抓到全文也无法覆盖已存的截断正文
```

后果：

- 首次抓取因展开失败写入截断正文后，**即使后续某次抓取成功展开了全文，由于 `ON CONFLICT` 不更新 `content`，库中的截断正文永远不会被修复**。
- 增量抓取（`_crawl_incremental`）只翻第 1 页并在命中 `recent_post_ids` 时停止，许多被截断的旧帖早已不在第 1 页 → **连重新遇到并触发 `save_content` 的机会都没有**。
- 这就是为什么 104 条截断帖从产生之日起一直停留在截断状态。

### 3.4 链路全景

```
列表接口 getIndex 返回长文（text 被截断 + isLongText=true + ...全文 后缀）
        │
        ▼
WeiboClientWrapper.get_user_posts(expand=False)  ← 显式禁用库内置展开循环
        │
        ▼
_expand_long_posts() 逐条 get_post_by_bid(bid)
        │
        ├─ [访客模式] statuses/show 返回 200 + Sina Visitor System HTML
        │     └─ crawl4weibo._request 调 .json() → ValueError
        │           └─ 绕过库重试，上抛
        ├─ [访客模式] statuses/show 返回 432 → 重试 3 次失败 → NetworkError
        │
        ▼
_expand_long_posts 兜底 except Exception → 重试 2 次（4–7s）→ 仍失败
        │
        ▼
保留截断正文（...全文），继续入库
        │
        ▼
SubscriptionContentService.save_content
        │
        └─ ON CONFLICT 不更新 content 列  ← ★ 截断被永久固化
        │
        ▼
subscription_content.content 永久为 "...全文"（即使之后某次抓到全文也覆盖不了）
```

---

## 4. 修复方案

修复分四部分：**①自愈（让库能被修复）**、**②补全存量（清掉现有 104 条）**、**③展开健壮性（减少未来截断）**、**④运维（根治访客模式）**。

### 4.1 ① 让 `save_content` 支持正文自愈（共享层，最关键）

在 `_save_normalized_content` 的 `ON CONFLICT` 中**有条件更新 `content`**：仅当新正文比库中现有正文**更长**时才覆盖。这是跨所有渠道（twitter/youtube/news/…）都安全的规则——永不把好正文降级为短正文，同时让任何渠道"再次抓到更完整正文"时自动修复。

```sql
ON CONFLICT (channel_type, content_id) DO UPDATE SET
    user_ids = ARRAY(SELECT DISTINCT UNNEST(subscription_content.user_ids || EXCLUDED.user_ids)),
    ...
    content = CASE
        WHEN EXCLUDED.content IS NOT NULL
         AND char_length(EXCLUDED.content) > char_length(COALESCE(subscription_content.content, ''))
        THEN EXCLUDED.content
        ELSE subscription_content.content
    END,
    ...
```

> 备注：该 CASE 规则天然满足"截断正文（短）覆盖全文（长）"被拒绝、"全文（长）覆盖截断正文（短）"被接受。需同步回归测试 twitter/youtube/news 渠道，确认不会有渠道依赖"正文被刷新为等长不同内容"的语义（目前无此依赖）。

文件：`backend/services/subscription_content_service.py:163-184`。

### 4.2 ② 存量补全：一次性修复脚本 + 抓取尾部定期清扫

**(a) 一次性修复脚本**（清掉现有 104 条），新建 `scripts/repair_weibo_truncated.py`：

```
SELECT id, content_id, author_identifier AS uid, metadata->>'bid' AS bid
FROM subscription_content
WHERE channel_type = 'weibo' AND content LIKE '%全文';   -- 精确截断探测，0 误判
```

对每条：用 `WeiboClientWrapper`（登录态优先，回退访客）调 `get_post_by_bid(bid)`，校验返回正文更长且不以 `...全文` 结尾，再直接 `UPDATE subscription_content SET content = %s WHERE id = %s`。篇间 `LONG_TEXT_EXPAND_INTERVAL` 打散，单条失败仅记日志跳过（脚本可反复执行直到清零）。

**(b) 抓取尾部集成"小配额清扫"**，在 `WeiboSubscriptionService.fetch_and_store_posts` 末尾：

- 每次运行额外挑选 N 条（建议 `WEIBO_REPAIR_QUOTA`，默认 5）仍以 `...全文` 结尾的博文重新展开；
- 配额上限避免对微博瞬时压测加剧风控；
- 这样未来偶发截断也能在后续 tick 内自愈（配合 4.1 的 `content` 可更新）。

文件：`backend/services/weibo_subscription_service.py`、新增 `scripts/repair_weibo_truncated.py`。

### 4.3 ③ 提升展开链路健壮性

`WeiboClientWrapper._expand_long_posts` 与 `get_user_posts`：

1. **校验展开结果有效性**：`get_post_by_bid` 返回后，若 `full.text` 仍以 `...全文` 结尾或为空，视为失败进入重试，避免"假成功"。
2. **重试与节奏调优**（访客模式下需更保守）：

   ```python
   LONG_TEXT_EXPAND_RETRIES = 4                # 2 → 4
   LONG_TEXT_EXPAND_INTERVAL = 8.0             # 5 → 8（长文间隔）
   LONG_TEXT_EXPAND_RETRY_SLEEP = (6, 12)      # 单条重试间隔（原 4–7）
   ```
3. **可选**：在 `_request` 的 200 分支外层包一层 JSON 容错（monkey-patch 或上游 PR），把"Sina Visitor System"HTML 识别为可重试错误而非 `ValueError` 上抛。受限于 crawl4weibo 是第三方库，建议在本仓库包装层兜底捕获 `ValueError` 并计入重试。

文件：`backend/services/weibo_subscription_service.py:85-90, 253-283`。

### 4.4 ④ 运维：录入微博登录 Cookie（根治诱因）

访客模式是截断频发的根本诱因。**录入一个微博登录 Cookie 后，系统从访客模式切换到登录态**，`statuses/show` 命中 Sina Visitor System / 432 的概率大幅下降。

```bash
# 通过管理员接口录入（从 m.weibo.cn 浏览器 F12 复制完整 Cookie）
curl -X POST https://<host>/api/admin/weibo/credentials \
  -H "Authorization: Bearer <admin-token>" \
  -H "Content-Type: application/json" \
  -d '{"label":"weibo-account-1","cookie":"SUB=...; SUBP=...; ..."}'

# 录入后主动校验
curl -X POST https://<host>/api/admin/weibo/credentials/<id>/validate \
  -H "Authorization: Bearer <admin-token>"
```

录入 Cookie 后配合 4.2(a) 脚本回扫即可一次性清零存量截断。多凭证可轮换进一步抗风控。

> 该项为运维操作（无需改代码），但效果最直接，强烈建议与代码修复同步执行。

---

## 5. 验证清单

- [ ] 4.1 改动后，twitter/youtube/news 渠道回归测试：重新抓取已有内容时 `content` 仅在变长时更新，无回退。
- [ ] 4.2(a) 脚本执行后，`SELECT count(*) FROM subscription_content WHERE channel_type='weibo' AND content LIKE '%全文'` 归零（或显著下降）。
- [ ] 4.2(b) 抓取尾部清扫 + 4.3 健壮性上线后，观察一周，新增截断数趋近于 0。
- [ ] 4.4 录入 Cookie 后，抓取日志中 `展开 N 次均失败` 与访客模式告警减少。
- [ ] 对修复成功的样本（如 `id=1426067`），人工核对正文与微博官网一致、无 `...全文` 结尾。

---

## 6. 关键文件索引

| 文件 | 相关内容 |
|------|----------|
| `backend/services/subscription_content_service.py:163-184` | `ON CONFLICT` 子句（不含 `content`，截断永久化主因）|
| `backend/services/weibo_subscription_service.py:85-90` | 长文展开调优常量 |
| `backend/services/weibo_subscription_service.py:253-283` | `_expand_long_posts`（兜底过弱 + 无结果校验）|
| `backend/services/weibo_subscription_service.py:185-251` | `WeiboClientWrapper`（凭证/访客模式分支）|
| `backend/services/weibo_credential_service.py` | Cookie 录入/校验/轮换；当前生产 `weibo_credentials` 为空 |
| `crawl4weibo/core/client.py:367-368` | `_request` 的 200 → `.json()`（访客系统 HTML 触发 ValueError）|
| `crawl4weibo/core/client.py:606-660` | `get_post_by_bid`（`statuses/show?id=bid`）|
| `scripts/repair_weibo_truncated.py` | （待新增）存量截断补全脚本 |

---

## 附：诊断查询速查

```sql
-- 截断总量
SELECT count(*) FROM subscription_content
WHERE channel_type='weibo' AND content LIKE '%全文';

-- 按作者分布
SELECT author_name, count(*) FROM subscription_content
WHERE channel_type='weibo' AND content LIKE '%全文'
GROUP BY author_name ORDER BY count(*) DESC;

-- 凭证状态（空 = 访客模式）
SELECT id, label, status FROM weibo_credentials;

-- 展开成功的长文样本（证明链路可用）
SELECT id, author_name, length(content) FROM subscription_content
WHERE channel_type='weibo' AND length(content) > 1000 AND content NOT LIKE '%全文'
ORDER BY length(content) DESC LIMIT 5;
```
