-- Migration: rename_template_names_to_english.sql
-- Purpose: Rename all llm_prompt_templates.template_name from Chinese to English,
--          and enrich description with detailed module-context explanations.
-- Scope  : system-scope templates (all rows) + user-scope templates (batch by module_code)
-- Safety : template_name / description are display-only fields; no code logic uses their values.
--          All template lookups use template_code or id. This migration is fully non-breaking.

-- ============================================================
-- PART 1: System templates — individual updates
-- ============================================================

UPDATE llm_prompt_templates SET
    template_name = 'News Summary — Standard',
    description   = 'Generates a concise key-points summary and market impact analysis for individual news items. Extracts core thesis, macro signals, affected sectors, and potential investment implications. SYSTEM_FIXED — changes take effect immediately for all news processing tasks.'
WHERE template_code = 'news_summary_v1';

UPDATE llm_prompt_templates SET
    template_name = 'News Classification & Tagging',
    description   = 'Extracts multi-dimensional structured classification labels from news content: theme, event type, impact scope, urgency, and affected tickers. Output feeds the news_classification_dimensions table and powers investment theme matching. SYSTEM_FIXED — changes apply to all subsequent classification tasks.'
WHERE template_code = 'news_classification';

UPDATE llm_prompt_templates SET
    template_name = 'Investment Theme Description Generator',
    description   = 'Generates rich semantic description text for investment themes, optimized for vector embedding and cosine similarity retrieval via Qwen text-embedding-v3 (1024-dim). Modify with care — changes affect semantic matching accuracy for all newly indexed or re-indexed themes.'
WHERE template_code = 'theme_description_generator_v1';

UPDATE llm_prompt_templates SET
    template_name = 'CCTV News Analysis — Policy Signal Focus',
    description   = 'Specialized analysis for CCTV and state-media news, emphasizing policy signal extraction, regulatory intent interpretation, and official messaging analysis. Structured output highlights macro policy direction and potential market implications. SYSTEM_FIXED — changes take effect for all CCTV-source news tasks immediately.'
WHERE template_code = 'news_cctv_summary_v1';

UPDATE llm_prompt_templates SET
    template_name = 'General Document Analysis',
    description   = 'Deep structured analysis for uploaded documents, web pages, and text notes. Extracts core thesis, risk/opportunity assessment, key data points, and actionable investment takeaways. USER_OWNED seed for the file_analysis module — each user maintains a private copy editable via My Templates or the upload panel sidebar.'
WHERE template_code = 'file_analysis_v1';

UPDATE llm_prompt_templates SET
    template_name = 'Financial Report Analysis',
    description   = 'Specialized analysis for corporate financial reports (earnings releases, annual/quarterly reports). Extracts revenue/earnings performance vs. estimates, guidance changes, margin trends, balance sheet highlights, and management commentary. Outputs structured financial metrics with an investment implication summary.'
WHERE template_code = 'file_analysis_financial_report_v1';

UPDATE llm_prompt_templates SET
    template_name = 'Investment Research Report Analysis',
    description   = 'Parses sell-side research reports from investment banks and brokerages. Extracts investment rating, price target, key thesis, earnings estimates, valuation methodology, and primary risk factors. Outputs a structured brief for professional investors to rapidly assess a report''s conviction and rationale.'
WHERE template_code = 'file_analysis_investment_research_v1';

UPDATE llm_prompt_templates SET
    template_name = 'Audio / Meeting Transcript Analysis',
    description   = 'Generates a structured summary and key-information cards from audio transcription text (podcasts, YouTube videos, meeting recordings). Extracts speaker viewpoints, key data points, market insights, and notable quotes. USER_OWNED seed for the audio_analysis module — each user maintains a private copy editable via My Templates.'
WHERE template_code = 'audio_analysis_v1';

UPDATE llm_prompt_templates SET
    template_name = 'Industry Research Report Analysis',
    description   = 'Specialized for in-depth industry reports from brokerages and investment banks. Extracts industry trend analysis, competitive landscape, supply-chain structure, key investment themes, and sector-level risks. Outputs a structured industry brief for investors focused on sector-level opportunities.'
WHERE template_code = 'file_analysis_industry_research_v1';

UPDATE llm_prompt_templates SET
    template_name = 'File Auto-Tagging',
    description   = 'Automatically extracts structured financial tags from file content — sector, industry, equity tickers, and thematic concepts. Output is written to the file_tags table and used for portfolio-level filtering and content discovery. USER_OWNED seed for file_tagging — each user can customize tag dimensions and priority weighting in My Templates.'
WHERE template_code = 'content_tag_v1';

UPDATE llm_prompt_templates SET
    template_name = 'Twitter Daily Report — Phase 1: Author Summary',
    description   = 'Aggregates all tweets from a single Twitter author for the day into a structured per-author daily summary (Phase 1 of the two-stage pipeline). SYSTEM_FIXED shared template — the per-author summary is reused across all subscribers to that author, so changes take effect for all users immediately.'
WHERE template_code = 'twitter_daily_phase1_default';

UPDATE llm_prompt_templates SET
    template_name = 'Twitter Daily Report — Phase 2: Personal Digest',
    description   = 'Aggregates Phase 1 per-author summaries from all followed Twitter accounts into a personalized daily digest. Customize output language, industry focus, summary depth, and section structure. USER_OWNED seed for twitter_phase2 — each user maintains a private copy editable via My Templates.'
WHERE template_code = 'twitter_daily_phase2_default';

UPDATE llm_prompt_templates SET
    template_name = 'YouTube Daily Report — Phase 1: Channel Summary',
    description   = 'Aggregates new videos from a single YouTube channel for the day into a structured per-channel daily summary using video titles and transcripts (Phase 1 of the two-stage pipeline). SYSTEM_FIXED shared template — the per-channel summary is shared across all subscribers; changes take effect for all users immediately.'
WHERE template_code = 'youtube_daily_phase1_default';

UPDATE llm_prompt_templates SET
    template_name = 'YouTube Daily Report — Phase 2: Personal Digest',
    description   = 'Aggregates Phase 1 per-channel summaries from all subscribed YouTube channels into a personalized daily video digest. Customize output language, topic preferences, and per-channel summary depth. USER_OWNED seed for youtube_phase2 — each user maintains a private copy editable via My Templates.'
WHERE template_code = 'youtube_daily_phase2_default';

UPDATE llm_prompt_templates SET
    template_name = 'File Auto-Naming',
    description   = 'Automatically generates a concise, readable display name for uploaded files based on their content. Output replaces the raw filename shown in the UI. SYSTEM_FIXED — changes apply to all subsequent naming tasks for all users. Modify naming conventions carefully as they affect the entire file library.'
WHERE template_code = 'auto_name_v1';

UPDATE llm_prompt_templates SET
    template_name = 'File Auto-Classification',
    description   = 'Automatically assigns uploaded files to the semantically best-matching folder in the user''s file library by analyzing content against existing folder names. SYSTEM_FIXED — changes apply to all subsequent auto-classification tasks immediately.'
WHERE template_code = 'auto_classify_v1';

UPDATE llm_prompt_templates SET
    template_name = 'Feed Auto-Tagging (Global)',
    description   = 'Extracts structured tags from Twitter and YouTube feed items — sector, industry, equity tickers, investment concepts, and a baseline summary. Results are written to subscription_content_tags and shared globally across all subscribers (not per-user). SYSTEM_FIXED — only admins can modify; changes affect tagging output for all users and all downstream consumers including Daily reports and personalized summaries.'
WHERE template_code = 'content_tag_feeds_v1';

UPDATE llm_prompt_templates SET
    template_name = 'Twitter Feed Personalized Summary',
    description   = 'Generates a per-user personalized summary for individual Twitter feed items. Customize summary perspective (investment / macro / sector-specific), key focus areas, and output structure. Results are stored in subscription_content_user_summaries and visible only to the individual user. USER_OWNED seed for feed_summary — editable via the Feeds settings gear or My Templates.'
WHERE template_code = 'feed_summary_v1';

UPDATE llm_prompt_templates SET
    template_name = 'YouTube Feed Personalized Summary',
    description   = 'Generates a per-user personalized summary for individual YouTube feed videos using transcripts/subtitles. Customize summary depth, focus dimensions (viewpoints, data, market impact), and output language. Results are visible only to the individual user. USER_OWNED seed for youtube_summary (Feeds only) — completely independent from the youtube_analysis module used for uploaded YouTube links.'
WHERE template_code = 'youtube_summary_v1';

UPDATE llm_prompt_templates SET
    template_name = 'YouTube Upload Deep Analysis',
    description   = 'Default deep analysis template for YouTube links uploaded via the Upload Sources panel (Bloomberg-view style). Analyzes video content using transcript, title, channel, and description to produce an in-depth investment-focused report. USER_OWNED seed for youtube_analysis — completely independent from the Feeds youtube_summary module. Editable via My Templates or the upload panel sidebar.'
WHERE template_code = 'youtube_analysis_v1';

-- ============================================================
-- PART 2: User templates — batch rename by module_code
-- All user-scope rows inherit display names from their seed;
-- replace Chinese names with English equivalents + "(My)" suffix.
-- ============================================================

-- audio_analysis users
UPDATE llm_prompt_templates SET
    template_name = 'Audio / Meeting Transcript Analysis (My)'
WHERE scope = 'user' AND module_code = 'audio_analysis';

-- feed_summary users
UPDATE llm_prompt_templates SET
    template_name = 'Twitter Feed Personalized Summary (My)'
WHERE scope = 'user' AND module_code = 'feed_summary';

-- file_analysis users (standard seeds)
UPDATE llm_prompt_templates SET
    template_name = 'General Document Analysis (My)'
WHERE scope = 'user' AND module_code = 'file_analysis'
  AND template_name NOT LIKE '%v2%';

-- file_analysis user with custom v2 name (id=49, template_code=user_file_analysis_1776595968028)
UPDATE llm_prompt_templates SET
    template_name = 'General Document Analysis v2 (My)'
WHERE scope = 'user' AND module_code = 'file_analysis'
  AND template_name LIKE '%v2%';

-- file_tagging users
UPDATE llm_prompt_templates SET
    template_name = 'File Auto-Tagging (My)'
WHERE scope = 'user' AND module_code = 'file_tagging';

-- twitter_phase2 users (all, including legacy codes with wrong name)
UPDATE llm_prompt_templates SET
    template_name = 'Twitter Daily Report — Phase 2: Personal Digest (My)'
WHERE scope = 'user' AND module_code = 'twitter_phase2';

-- twitter_phase2 legacy row with empty module_code (id=46)
-- Only update template_name; do NOT set module_code as the owning user already has
-- a proper twitter_phase2 row (user_twitter_phase2_1776063616833) that would conflict.
UPDATE llm_prompt_templates SET
    template_name = 'Twitter Daily Report — Phase 2: Personal Digest (My)'
WHERE template_code = 'user_twitter_phase2_1776063604220';

-- youtube_analysis users
UPDATE llm_prompt_templates SET
    template_name = 'YouTube Upload Deep Analysis (My)'
WHERE scope = 'user' AND module_code = 'youtube_analysis';

-- youtube_phase2 users
UPDATE llm_prompt_templates SET
    template_name = 'YouTube Daily Report — Phase 2: Personal Digest (My)'
WHERE scope = 'user' AND module_code = 'youtube_phase2';

-- youtube_summary users
UPDATE llm_prompt_templates SET
    template_name = 'YouTube Feed Personalized Summary (My)'
WHERE scope = 'user' AND module_code = 'youtube_summary';

-- ============================================================
-- PART 3: User templates — description translation to English
-- ============================================================

UPDATE llm_prompt_templates SET
    description = 'Your personal audio/meeting analysis template. Generates structured summaries and key-info cards from podcast, YouTube video, or meeting transcripts. Customize: summary framework, key extraction dimensions, and output language.'
WHERE scope = 'user' AND module_code = 'audio_analysis';

UPDATE llm_prompt_templates SET
    description = 'Your personal Twitter feed summary template. Results are visible only to you and stored per-item in subscription_content_user_summaries. Customize: summary perspective (investment / macro / sector), key focus areas, and output structure.'
WHERE scope = 'user' AND module_code = 'feed_summary';

UPDATE llm_prompt_templates SET
    description = 'Your personal document analysis template. Performs deep structured analysis on uploaded documents, web pages, and text notes. Customize: analysis dimensions (risk, opportunity, core thesis), output format, and language.'
WHERE scope = 'user' AND module_code = 'file_analysis';

UPDATE llm_prompt_templates SET
    description = 'Your personal file auto-tagging template. Extracts structured financial tags — sector, industry, tickers, and concepts — from file content. Customize: tag dimension schema, priority classification, and output field format.'
WHERE scope = 'user' AND module_code = 'file_tagging';

UPDATE llm_prompt_templates SET
    description = 'Your personal Twitter daily digest template. Aggregates Phase 1 per-author summaries from all followed accounts into your personalized daily report. Customize: output language, industry focus, summary depth, and section structure.'
WHERE scope = 'user' AND module_code = 'twitter_phase2';

UPDATE llm_prompt_templates SET
    description = 'Your personal YouTube upload deep analysis template (Bloomberg-view style). Used for in-depth analysis of YouTube links submitted via Upload Sources. Customize: analysis depth, focus dimensions, and output format. Completely independent from the Feeds youtube_summary module.'
WHERE scope = 'user' AND module_code = 'youtube_analysis';

UPDATE llm_prompt_templates SET
    description = 'Your personal YouTube daily digest template. Aggregates Phase 1 per-channel summaries from all subscribed channels into your personalized daily video report. Customize: output language, topic preferences, and per-channel summary depth.'
WHERE scope = 'user' AND module_code = 'youtube_phase2';

UPDATE llm_prompt_templates SET
    description = 'Your personal YouTube feed summary template (Feeds only). Generates per-video personalized summaries from video transcripts/subtitles. Customize: summary depth, focus dimensions (viewpoints, data, market impact), and output language. Independent from the YouTube Upload analysis module.'
WHERE scope = 'user' AND module_code = 'youtube_summary';

-- Legacy row with empty module_code (template_code = user_twitter_phase2_1776063604220)
UPDATE llm_prompt_templates SET
    description = 'Legacy personal Twitter Phase 2 digest template. Aggregates per-author summaries into a personalized daily report. Editable in My Templates.'
WHERE scope = 'user' AND (module_code IS NULL OR module_code = '') AND template_name ILIKE '%Twitter%';
