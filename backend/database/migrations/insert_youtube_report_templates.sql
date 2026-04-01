-- =====================================================
-- YouTube Daily Report — LLM Prompt Templates 种子数据
-- =====================================================
-- 创建时间: 2026-03-31
-- 说明: 插入 YouTube Daily Report 所需的 Phase 1 / Phase 2 prompt 模板。
--       模板变量使用 {{variable_name}} 格式，由 prompt_renderer 渲染。
--       若已存在同名 template_code，则跳过（ON CONFLICT DO NOTHING）。

-- Phase 1: 频道级视频摘要
INSERT INTO llm_prompt_templates (
    template_code, template_name, category_id,
    system_prompt, user_prompt_template,
    variables, output_format,
    recommended_provider, recommended_model,
    temperature, max_tokens,
    scope, is_public, is_active, is_default,
    description, tags
) VALUES (
    'youtube_daily_phase1_default',
    'YouTube Daily Phase1 — 频道视频摘要',
    NULL,
    '你是一位专业的视频内容分析师，擅长从 YouTube 视频的字幕或描述中提炼关键信息。

任务：为指定 YouTube 频道在指定日期发布的视频生成一份结构化摘要。

要求：
1. 逐个视频提炼核心观点、关键信息、重要数据
2. 标注每个要点对应的视频标题和 URL，方便溯源
3. 如果视频内容来源是"标题+描述"（而非字幕），请据此推断视频可能讨论的主题，但标明信息来源有限
4. 使用{{report_language}}撰写
5. 输出 Markdown 格式',
    '频道: {{author_name}}
日期: {{report_date}}
视频数量: {{video_count}}

以下是该频道当天发布的视频内容：

{{videos_text}}

{{custom_instructions}}

请为上述视频生成一份结构化摘要，包含：
1. **频道概览**：该频道当天发布内容的整体主题
2. **逐视频要点**：每个视频的核心观点（2-5 个要点），附带视频标题和链接
3. **关键数据/观点**：值得关注的具体数据、预测或独到见解',
    '{"author_name": {"type": "string", "required": true, "description": "频道名称"}, "report_date": {"type": "string", "required": true, "description": "报告日期"}, "video_count": {"type": "string", "required": true, "description": "视频数量"}, "videos_text": {"type": "string", "required": true, "description": "视频内容文本"}, "report_language": {"type": "string", "required": false, "default": "zh", "description": "输出语言 (zh/en)"}, "custom_instructions": {"type": "string", "required": false, "default": "", "description": "自定义指令"}}',
    '{"type": "markdown"}',
    NULL, NULL,
    0.5, 3000,
    'system', true, true, true,
    'YouTube Daily Report Phase 1: 为单个频道当天的视频生成结构化摘要',
    '["youtube", "daily_report", "phase1", "channel_summary"]'
)
ON CONFLICT (template_code) DO NOTHING;


-- Phase 2: 跨频道综合报告
INSERT INTO llm_prompt_templates (
    template_code, template_name, category_id,
    system_prompt, user_prompt_template,
    variables, output_format,
    recommended_provider, recommended_model,
    temperature, max_tokens,
    scope, is_public, is_active, is_default,
    description, tags
) VALUES (
    'youtube_daily_phase2_default',
    'YouTube Daily Phase2 — 跨频道综合报告',
    NULL,
    '你是一位资深的视频内容策展人与分析师，擅长从多个 YouTube 频道的视频中提炼跨领域洞察。

任务：基于多个频道的视频摘要，生成一份综合性的 YouTube 每日报告。

要求：
1. 提炼跨频道的共同主题、趋势和关键信息
2. 识别不同频道间的观点交叉或分歧
3. 突出最有价值的内容发现
4. 保留对原始视频的引用（标题 + URL）
5. 使用{{report_language}}撰写
6. 输出 Markdown 格式，结构清晰',
    '日期: {{report_date}}
频道数量: {{channel_count}}
视频总数: {{video_count}}

以下是各频道的视频摘要：

{{channel_summaries}}

---

热门视频（按观看/互动排序）：

{{top_videos}}

{{focus_topics_instruction}}
{{custom_instructions}}

请生成一份综合性的 YouTube 每日报告，包含：
1. **今日概览**：当天 YouTube 内容的整体画像（2-3 句话）
2. **核心主题**：跨频道归纳出的 3-5 个核心主题，每个主题下列出相关要点和来源视频
3. **重点发现**：最值得关注的观点、数据或趋势（引用具体视频）
4. **热门视频推荐**：基于内容价值和热度推荐 Top 3-5 视频',
    '{"report_date": {"type": "string", "required": true, "description": "报告日期"}, "channel_count": {"type": "string", "required": true, "description": "频道数量"}, "video_count": {"type": "string", "required": true, "description": "视频总数"}, "channel_summaries": {"type": "string", "required": true, "description": "各频道摘要合集"}, "top_videos": {"type": "string", "required": true, "description": "热门视频列表"}, "report_language": {"type": "string", "required": false, "default": "zh", "description": "输出语言 (zh/en)"}, "focus_topics_instruction": {"type": "string", "required": false, "default": "", "description": "重点主题指令（可选）"}, "custom_instructions": {"type": "string", "required": false, "default": "", "description": "自定义指令"}}',
    '{"type": "markdown"}',
    NULL, NULL,
    0.6, 4000,
    'system', true, true, true,
    'YouTube Daily Report Phase 2: 跨频道综合分析报告',
    '["youtube", "daily_report", "phase2", "synthesis"]'
)
ON CONFLICT (template_code) DO NOTHING;
