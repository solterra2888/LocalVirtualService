# YouTube 字幕清洗（`transcript_cleaned`）

本文档说明 Feed YouTube 条目在 `subscription_content` 中的**原始字幕**与**清洗后字幕**的关系、后端清洗管线、依赖环境与运维脚本。

## 背景

家用 Worker / API 写入的 `subscription_content.transcript` 多为**按行折断、格式混杂**的纯文本（时间戳前缀、`>>` 说话人、中英文混排、大量行末无标点等）。直接展示可读性差；早期版本曾因 CJK 检测正则缺陷导致英文被误判为中文合并路径，出现单词粘连。

清洗结果写入 **`transcript_cleaned`**，**不覆盖** `transcript`，便于回溯与二次处理。

## 数据库

| 列名 | 类型 | 说明 |
|------|------|------|
| `transcript` | `text` | 上游写入的原始字幕（YouTube API / yt-dlp / ASR 等）|
| `transcript_cleaned` | `text` | 规则 + NLP 兜底清洗后的可读文本；可为 `NULL`（未跑批处理或清洗失败未写入）|

首次加列可执行（幂等，列已存在则跳过）：

```bash
python scripts/migrate_add_transcript_cleaned.py
```

## API 行为（前端阅读）

`GET /api/subscriptions/contents/{content_db_id}/transcript`

- 响应 JSON **始终只有键** `transcript`（前端类型不变）。
- **优先**返回 `transcript_cleaned`；若为空则回退 **`transcript`**。
- 鉴权：`content_db_id` 对应行的 `user_ids` 须包含当前用户。

实现：`backend/api/subscriptions.py` 中 `get_subscription_content_transcript`。

## 清洗管线（`backend/services/transcript_cleaner.py`）

四阶段：

1. **格式检测**：时间戳行首、`>>` 说话人、CJK 字符占比（>30% 视为中文路径）等。
2. **字符规范化**：统一换行、去掉 `[music]` 类标记、剥离 `[0.12s - 4.60s]` 时间戳前缀等。
3. **断句重组**：英文行间合并加空格；中文路径无空格拼接；说话人块内合并且不跨块。
4. **段落成形**：正常标点英文用规则切句；**低标点密度**英文尝试 **spaCy**（`en_core_web_sm`，保留 `tok2vec` + `parser`）切句；仍无法切分时按词数分段兜底；中文无句末标点且过长时按字数分段。

常见修复与策略简述：

- **CJK 误判**：Unicode SMP 区间须用 `\U00020000-\U0002A6DF` 等形式，避免误匹配 ASCII。
- **缩写**：`p.m.`、`Mr.`、`U.S.` 等末尾句号不作为强制句末（简化规则集）。
- **无标点长英文**：spaCy 对全无标点的一维串往往只能给出单句 → 词数兜底分段。
- **spaCy 不可用**：自动降级为词数分段，不阻塞批处理。

## Python 依赖与 Conda 环境

- **`requirements.txt`**：`spacy>=3.7,<4`
- **英文模型**（句子边界，与当前代码加载方式一致）：

  ```bash
  python -m spacy download en_core_web_sm
  ```

团队约定 Conda 环境名为 **`rcTool`** 时，示例：

```bash
conda activate rcTool
pip install "spacy>=3.7,<4" -i https://pypi.tuna.tsinghua.edu.cn/simple
python -m spacy download en_core_web_sm
```

> 部署生产 / Celery Worker 的机器也需安装相同依赖，否则清洗会退化为兜底分段，段落质量下降。

## 批量回填 / 重算

脚本：`scripts/run_transcript_cleaning.py`

```bash
# 仅处理尚未写入 transcript_cleaned 的行（默认 channel_type=youtube）
python scripts/run_transcript_cleaning.py

# 强制重算已有 cleaned（算法升级后全量刷新）
python scripts/run_transcript_cleaning.py --force

# 抽样调试
python scripts/run_transcript_cleaning.py --limit 50 --dry-run
```

依赖：数据库连接由项目 `.env`（`DB_*`）提供，与 `docs/others/database.md` 一致。

## 与其它模块的关系

| 场景 | 使用的文本 |
|------|------------|
| Feeds / Daily 前端懒加载字幕 API | 优先 `transcript_cleaned`，否则 `transcript` |
| LLM Feed 打标、频道日报等**直接读库字段**的实现 | 仍以 **`transcript`** 为准（若将来希望摘要基于清洗文本，需单独改查询）|

## 相关文件索引

| 路径 | 说明 |
|------|------|
| `backend/services/transcript_cleaner.py` | 清洗核心逻辑 |
| `backend/api/subscriptions.py` | Transcript GET 优先 cleaned |
| `scripts/migrate_add_transcript_cleaned.py` | DDL 加列 |
| `scripts/run_transcript_cleaning.py` | 批量写回 `transcript_cleaned` |
| `web-frontend/src/components/feed/YoutubeFeedOriginalBlock.tsx` | Transcript UI（含字体大小等）|
