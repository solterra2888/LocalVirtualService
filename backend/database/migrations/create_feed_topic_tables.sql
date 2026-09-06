-- =====================================================
-- Feeds Dashboard — 主题词典 + 每日条目快照 (P0)
-- =====================================================
-- 创建时间: 2026-09-05
-- 说明: Feeds Dashboard (/feeds-dashboard) 的 P0 存储。
--   feed_topics             主题词典: auto-tag concepts 的归一化目标
--                           (canonical + 别名; 高频未命中词自动新建待审核)
--   feed_topic_daily_items  每日条目快照: 聚合器 (零 LLM 规则) 写入,
--                           Dashboard 读路径只 join 本表 + subscription_content
-- 设计要点:
--   - 不依赖 content_theme_matches / embedding; 主题从内容标签涌现
--   - report_date 为北京自然日标签 (与摘要类调度任务口径一致)
--   - 词典冷启动: scripts/seed_feed_topics.py (curated 基线 + 历史频次扫描)

CREATE TABLE IF NOT EXISTS feed_topics (
    id SERIAL PRIMARY KEY,
    topic_key TEXT NOT NULL,                        -- canonical 标识 (curated 用英文 slug; auto 直接用词本身)
    name_cn TEXT NOT NULL,                          -- 'AI算力'
    name_en TEXT,                                   -- 'AI Compute'
    aliases JSONB DEFAULT '[]'::jsonb,              -- ["AI基础设施","算力","GPU","人工智能"]
    category VARCHAR(50),                           -- 粗分组: 宏观/科技/能源/地缘/市场/公司 ...
    source VARCHAR(20) NOT NULL DEFAULT 'curated',  -- curated(人工 seed) | auto(聚合器高频词自动新建)
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,       -- 关闭后该主题不再聚合/展示
    needs_review BOOLEAN NOT NULL DEFAULT FALSE,    -- auto 新建的待管理员确认
    sort_order INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_feed_topics_topic_key UNIQUE (topic_key)
);

-- 检索索引 (P0 聚合器把词典整体载入内存匹配, 索引主要服务运维查询/后续扩展)
CREATE INDEX IF NOT EXISTS idx_feed_topics_aliases
    ON feed_topics USING gin(aliases);
CREATE INDEX IF NOT EXISTS idx_feed_topics_enabled
    ON feed_topics(is_enabled, sort_order);

-- updated_at 触发器 (复用全局函数)
DROP TRIGGER IF EXISTS update_feed_topics_updated_at ON feed_topics;
CREATE TRIGGER update_feed_topics_updated_at
    BEFORE UPDATE ON feed_topics
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS feed_topic_daily_items (
    id SERIAL PRIMARY KEY,
    report_date DATE NOT NULL,                      -- 北京自然日标签
    topic_id INT NOT NULL REFERENCES feed_topics(id) ON DELETE CASCADE,
    subscription_content_id INT NOT NULL
        REFERENCES subscription_content(id) ON DELETE CASCADE,
    is_primary_topic BOOLEAN NOT NULL DEFAULT TRUE, -- 条目命中多主题时仅一个主题计主 (concepts 顺序首位)
    importance VARCHAR(10) NOT NULL DEFAULT 'medium', -- high/medium/low (twitter 采信 tweet_signal, 其余规则评分)
    score DOUBLE PRECISION,                         -- 数值分 (0-1, 排序用)
    created_at TIMESTAMP(6) WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_feed_topic_daily_items UNIQUE (report_date, topic_id, subscription_content_id),
    CONSTRAINT check_feed_topic_daily_items_importance
        CHECK (importance IN ('high', 'medium', 'low'))
);

CREATE INDEX IF NOT EXISTS idx_ftdi_date_topic
    ON feed_topic_daily_items(report_date, topic_id);
CREATE INDEX IF NOT EXISTS idx_ftdi_date_content
    ON feed_topic_daily_items(report_date, subscription_content_id);

COMMENT ON TABLE  feed_topics IS 'Feeds Dashboard 主题词典: concepts 归一化目标 (curated + auto)';
COMMENT ON TABLE  feed_topic_daily_items IS 'Feeds Dashboard 每日条目快照: 聚合器全量重建 (幂等), 读路径零 LLM';
COMMENT ON COLUMN feed_topic_daily_items.importance IS 'high/medium/low; twitter 采信 tweet_signal_v1 LLM 判断, 其余渠道规则评分';
COMMENT ON COLUMN feed_topics.needs_review IS 'source=auto 的高频词自动新建, 待管理员在词典中确认/合并';

-- =====================================================
-- 验证
-- =====================================================
-- \d feed_topics
-- \d feed_topic_daily_items
