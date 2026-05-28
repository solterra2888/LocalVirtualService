-- ============================================================
-- Ebook Map-Reduce Prompt Templates
-- 新增两个系统级模板：
--   ebook_chapter_summary_v1  ← Map 阶段：单章摘要
--   ebook_book_overview_v1    ← Reduce 阶段：全书概览
-- ============================================================

-- 1. Map 阶段：章节摘要
INSERT INTO llm_prompt_templates (
    template_code, template_name, scope, module_code,
    system_prompt, user_prompt_template,
    output_format, temperature, max_tokens,
    recommended_provider, recommended_model,
    description
)
VALUES (
    'ebook_chapter_summary_v1',
    'Ebook Chapter Summary (Map)',
    'system',
    'ebook_map_reduce',
    'You are an expert analyst summarising one chapter of a book.
Your goal is to produce a dense, precise summary that preserves key arguments, data points, named entities, and quotes.
Write in the same language as the input.
Length: 200-400 words. Do NOT start with "This chapter…".',
    'CHAPTER: {{chapter_title}}

CONTENT:
{{file_content}}

Provide a concise chapter summary (200-400 words) covering:
1. Core thesis or narrative of this chapter
2. Key facts, figures, or events
3. Important people / organisations mentioned
4. How this chapter connects to the book''s overall argument',
    '{}',
    0.3,
    600,
    'qwen',
    'qwen-turbo',
    'Ebook Map-Reduce: Map 阶段章节摘要模板，每章独立调用一次'
)
ON CONFLICT (template_code) DO UPDATE SET
    system_prompt        = EXCLUDED.system_prompt,
    user_prompt_template = EXCLUDED.user_prompt_template,
    temperature          = EXCLUDED.temperature,
    max_tokens           = EXCLUDED.max_tokens,
    recommended_provider = EXCLUDED.recommended_provider,
    recommended_model    = EXCLUDED.recommended_model,
    updated_at           = NOW();


-- 2. Reduce 阶段：全书概览
INSERT INTO llm_prompt_templates (
    template_code, template_name, scope, module_code,
    system_prompt, user_prompt_template,
    output_format, temperature, max_tokens,
    recommended_provider, recommended_model,
    description
)
VALUES (
    'ebook_book_overview_v1',
    'Ebook Book Overview (Reduce)',
    'system',
    'ebook_map_reduce',
    'You are an expert analyst synthesising chapter-level summaries into a comprehensive book overview.
Write in the same language as the summaries.
Your output should help a reader quickly understand the book''s full scope without reading it.',
    'Below are chapter-by-chapter summaries of a book. Synthesise them into a comprehensive overview.

CHAPTER SUMMARIES:
{{file_content}}

Write the book overview in this structure:
## 核心主题 / Core Theme
(1-2 sentences)

## 主要论点 / Main Arguments
(bullet list, 4-8 points)

## 关键事件与人物 / Key Events & People
(brief list)

## 结构概要 / Book Structure
(how chapters build on each other)

## 适合读者 / Best For
(who benefits most from this book)',
    '{}',
    0.4,
    1200,
    'qwen',
    'qwen3-max',
    'Ebook Map-Reduce: Reduce 阶段全书概览模板，合并所有章节摘要生成总报告'
)
ON CONFLICT (template_code) DO UPDATE SET
    system_prompt        = EXCLUDED.system_prompt,
    user_prompt_template = EXCLUDED.user_prompt_template,
    temperature          = EXCLUDED.temperature,
    max_tokens           = EXCLUDED.max_tokens,
    recommended_provider = EXCLUDED.recommended_provider,
    recommended_model    = EXCLUDED.recommended_model,
    updated_at           = NOW();
