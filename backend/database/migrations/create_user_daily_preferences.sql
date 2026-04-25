-- Daily 模块每用户预设：选中的 source_subtype 列表
-- 空数组(或无记录) = 未设置 preset，等同于"展示全部"
-- 非空 = 仅展示这些 source_subtype 的内容（限制 chip 行 + 列表查询范围）
--
-- 仅 9 种合法值，校验放在后端 Pydantic + API 层处理：
--   document, image, audio, youtube, web, text, twitter, news, youtube_feed
--
-- 后端 API:  GET/PUT /api/daily/preset
-- 详见: backend/api/daily.py, backend/schemas/daily.py

CREATE TABLE IF NOT EXISTS user_daily_preferences (
    user_id INT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    source_subtypes TEXT[] NOT NULL DEFAULT '{}',
    updated_at TIMESTAMP DEFAULT NOW()
);

COMMENT ON TABLE user_daily_preferences
    IS 'Daily 聚合视图的每用户预设：限制只显示哪些 source_subtype。空数组=显示全部';
COMMENT ON COLUMN user_daily_preferences.source_subtypes
    IS '用户勾选的 source_subtype 列表 (子集自 9 种合法值)。空数组等同于未设置 preset';
