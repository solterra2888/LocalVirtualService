# 知识星球话题采集

本地脚本：拉取知识星球话题 → 写入 PostgreSQL，支持首次补全 + 定时增量。

## 快速开始

```bash
pip install -r requirements.txt
cp .env.example .env          # 填写 DATABASE_URL
# 编辑 my_zsxq.py 顶部 COOKIE、groups.json 星球列表
python my_zsxq.py
```

启动后自动建表，无需手动执行 `schema.sql`。

## 配置说明

| 位置 | 内容 |
|------|------|
| `my_zsxq.py` → `COOKIE` | 浏览器登录 [wx.zsxq.com](https://wx.zsxq.com) 后 F12 → Network → 复制完整 Cookie |
| `.env` → `DATABASE_URL` | PostgreSQL 连接串；**密码含 `@` 等须 URL 编码**（`@→%40`） |
| `groups.json` | 星球列表，`group_id` + `group_name` |
| `.env` → `BACKFILL_DAYS` | 新星球首次补全天数，默认 15 |
| `.env` → `DATE_START` / `DATE_END` | 可选，手动指定日期区间（设了则只跑一轮） |

间隔、定时频率等参数在 `my_zsxq.py` 顶部常量区，一般不用改。

## 运行模式（一条命令自动切换）

```
python my_zsxq.py
```

1. **手动**：`.env` 里设置了 `DATE_START` 或 `DATE_END` → 按日期采一轮后退出  
2. **补全**：有星球未完成 `BACKFILL_DAYS` 历史拉取 → 先补全  
3. **定时增量**：全部补全完成后 → 常驻循环  
   - 活跃时段（08:00～次日 01:00）：每 600 秒  
   - 夜间（01:00～08:00）：每 3600 秒  

增量只拉**列表头部**，用 `topic_id` 与本地锚点对比；无新帖时通常 1 次 API 请求。

## 主要文件

| 文件 | 说明 |
|------|------|
| `my_zsxq.py` | 主程序 |
| `data/group_sync_state.json` | 每星球同步状态（补全标记、锚点 id、时间范围） |
| `logs/zsxq_YYYYMMDD.log` | 运行日志 |
| `schema.sql` | 表结构参考（程序会自动建表） |

## 数据表

`zsxq_groups_topics`：按 `topic_id` UPSERT，重复帖只更新不重复插入。

## 常见问题

**HTTP 401**  
Cookie 过期 → 重新从浏览器复制到 `COOKIE`。

**无法连接 PostgreSQL**  
检查地址/端口/网络；密码特殊字符是否已 URL 编码；URL 里出现多个 `@` 通常是密码未编码。

**某星球「非官方工具」**  
确认账号已加入该星球；或暂时从 `groups.json` 移除。

**强制某星球重新补全**  
删除 `data/group_sync_state.json` 里对应 group 条目，或设 `backfill_complete: false`。

**新增星球**  
加到 `groups.json`，重启脚本，会自动走 `BACKFILL_DAYS` 补全。

**部署迁移**  
代码为了降低服务器压力，增加了一个状态文件`data/group_sync_state.json`，会记录当前爬取的状态，迁移时最好一并复制，新部署的服务会去读取该文件。
