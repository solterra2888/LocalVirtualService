-- =====================================================
-- update_template_descriptions.sql
-- =====================================================
-- 目的: 为每张模板补充专属的 description，专门描述该模板的用途与可定制范围，
--       不再重复 MODULE_REGISTRY 中的模块说明。
-- 适用范围: 全部 16 张 system-scope 模板 + 分散写入的种子模板。
-- 安全性: 全部以 template_code 定位，重复执行无副作用。

-- ── USER_OWNED 模块种子模板（用户可在 My Templates 中编辑自己的副本）─────────────

UPDATE llm_prompt_templates
SET description = '综合多位 Twitter 作者的每日摘要，生成面向个人的日报正文。'
                  '可调整：输出语言、关注行业方向、摘要详细程度与段落结构。',
    updated_at = NOW()
WHERE template_code = 'twitter_daily_phase2_default';

UPDATE llm_prompt_templates
SET description = '综合多个 YouTube 频道的每日摘要，生成面向个人的视频日报。'
                  '可调整：输出语言、关注主题偏好与每频道摘要的详细程度。',
    updated_at = NOW()
WHERE template_code = 'youtube_daily_phase2_default';

UPDATE llm_prompt_templates
SET description = '对上传的文档/网页/文本进行深度结构化解读。'
                  '可调整：分析维度（风险、机会、核心论点等）、输出格式与语言。',
    updated_at = NOW()
WHERE template_code = 'file_analysis_v1';

UPDATE llm_prompt_templates
SET description = '对音频转写文本（播客/YouTube 视频）生成结构化摘要与关键信息卡片。'
                  '可调整：摘要框架、重点提取维度与输出语言。',
    updated_at = NOW()
WHERE template_code = 'audio_analysis_v1';

UPDATE llm_prompt_templates
SET description = '从文件内容自动提取行业分类、ticker、概念等结构化标签。'
                  '可调整：标签维度体系、优先级分类与输出字段格式。',
    updated_at = NOW()
WHERE template_code = 'content_tag_v1';

UPDATE llm_prompt_templates
SET description = '对单条 Twitter feed 内容生成个性化摘要，结果仅对自己可见。'
                  '可调整：摘要视角（投资/宏观/行业）、关注点与输出结构。',
    updated_at = NOW()
WHERE template_code = 'feed_summary_v1';

UPDATE llm_prompt_templates
SET description = '基于 YouTube 视频字幕/文字稿为单条视频生成个性化摘要。'
                  '可调整：摘要详细程度、关注维度（观点/数据/影响）与输出语言。',
    updated_at = NOW()
WHERE template_code = 'youtube_summary_v1';

-- ── SYSTEM_FIXED 模块模板（修改后全员立即生效，谨慎编辑）────────────────────────

UPDATE llm_prompt_templates
SET description = '按单个 Twitter 作者汇总当日推文，输出结构化作者日摘要（Phase 1）。'
                  '系统级共享模板，修改后所有用户 Phase 1 摘要立即使用新内容。',
    updated_at = NOW()
WHERE template_code = 'twitter_daily_phase1_default';

UPDATE llm_prompt_templates
SET description = '按单个 YouTube 频道汇总当日视频，输出结构化频道日摘要（Phase 1）。'
                  '系统级共享模板，修改后所有用户 Phase 1 摘要立即使用新内容。',
    updated_at = NOW()
WHERE template_code = 'youtube_daily_phase1_default';

UPDATE llm_prompt_templates
SET description = '基于文件内容自动生成简洁易读的展示名称。'
                  '系统级，修改后所有新命名任务立即使用新规则，慎改命名规范。',
    updated_at = NOW()
WHERE template_code = 'auto_name_v1';

UPDATE llm_prompt_templates
SET description = '将文件归入用户文件夹中语义最匹配的分类目录。'
                  '系统级，修改后所有新归类任务立即生效，慎改分类判断逻辑。',
    updated_at = NOW()
WHERE template_code = 'auto_classify_v1';

UPDATE llm_prompt_templates
SET description = '为 Twitter/YouTube 单条 feed 内容提取结构化标签，结果全局共享。'
                  '系统级，修改后所有新打标任务立即使用新规则，影响全体用户标签数据。',
    updated_at = NOW()
WHERE template_code = 'content_tag_feeds_v1';

UPDATE llm_prompt_templates
SET description = '对单条新闻提取核心要点与市场影响摘要，用于新闻流快速消化。'
                  '系统级，修改后所有新闻处理任务立即使用新摘要格式。',
    updated_at = NOW()
WHERE template_code = 'news_summary_v1';

UPDATE llm_prompt_templates
SET description = '对 CCTV 等媒体新闻生成专项摘要，侧重政策信号与官方口径解读。'
                  '系统级，修改后所有 CCTV 新闻处理任务立即生效。',
    updated_at = NOW()
WHERE template_code = 'news_cctv_summary_v1';

UPDATE llm_prompt_templates
SET description = '提取新闻的主题/事件/影响范围/紧急度等多维度结构化分类标签。'
                  '系统级，修改后所有新闻分类任务立即使用新分类维度。',
    updated_at = NOW()
WHERE template_code = 'news_classification';

UPDATE llm_prompt_templates
SET description = '为投资主题生成用于向量语义检索的描述文本。'
                  '系统级，修改后新创建及重新索引的主题立即使用新描述规则。',
    updated_at = NOW()
WHERE template_code = 'theme_description_generator_v1';
