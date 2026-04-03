-- 用户模板偏好表
-- 存储每个用户针对每个功能模块的 Prompt Template 偏好
-- 当用户没有设置偏好时，各模块自动使用系统默认模板

CREATE TABLE IF NOT EXISTS user_template_preferences (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    module_code VARCHAR(64) NOT NULL,
    template_id INT NOT NULL REFERENCES llm_prompt_templates(id) ON DELETE CASCADE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(user_id, module_code)
);

CREATE INDEX IF NOT EXISTS idx_utp_user_module
    ON user_template_preferences(user_id, module_code);

COMMENT ON TABLE user_template_preferences IS '用户模板偏好：每个用户可为不同功能模块选择自定义 Prompt Template';
COMMENT ON COLUMN user_template_preferences.module_code IS '功能模块代码，如 twitter_phase2, youtube_phase2, file_analysis 等';
COMMENT ON COLUMN user_template_preferences.template_id IS '用户选择的模板ID，必须是用户自己创建的模板（admin 也可选 system 模板）';
