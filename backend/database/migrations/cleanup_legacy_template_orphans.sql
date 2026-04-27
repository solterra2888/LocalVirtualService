-- =====================================================
-- 阶段 4 / PR-8: 清理 LLM 重构遗留的孤儿数据
-- =====================================================
-- 创建时间: 2026-04-27
-- 来源: docs/instructions/LLM_REFACTOR_PLAN.md §6 (PR-8)
--
-- 清理范围:
--   1. 孤儿表 twitter_report_user_config (Python 代码完全没引用过, 重构后改用
--      user_template_preferences[twitter_phase1|twitter_phase2])
--   2. 三个孤儿 PG 函数 (函数体引用已删除的 subscription_content.subscription_id,
--      任何调用都会报错):
--        - update_subscription_unread_count(int, int)
--        - refresh_all_unread_counts()
--        - trigger_update_unread_count_on_read() trigger
--
-- 安全策略:
--   - 表: 仅当所有行的 phase1/phase2_template_code 都为空 (= 与 user_template_preferences
--     等价, 删除不损失信息) 时自动 DROP; 否则 RAISE NOTICE 让运维手工 review.
--   - 函数: DROP IF EXISTS (无人调用且函数体已死代码), 安全删除.
--
-- 该 migration 幂等且安全, 跑在新环境无副作用 (对象不存在 IF EXISTS 跳过).

-- ==========================================================================
-- 1. twitter_report_user_config 表清理
-- ==========================================================================

DO $$
DECLARE
    has_real_config INT := 0;
    total_rows INT := 0;
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_name = 'twitter_report_user_config'
    ) THEN
        SELECT COUNT(*) INTO total_rows FROM twitter_report_user_config;

        SELECT COUNT(*) INTO has_real_config
        FROM twitter_report_user_config
        WHERE COALESCE(phase1_template_code, '') <> ''
           OR COALESCE(phase2_template_code, '') <> '';

        IF has_real_config > 0 THEN
            RAISE NOTICE 'twitter_report_user_config has % rows with real template configs (% / % total). '
                         'Skipping auto-drop. Manual migration to user_template_preferences required:',
                         has_real_config, has_real_config, total_rows;
            RAISE NOTICE '    INSERT INTO user_template_preferences (user_id, module_code, template_id)';
            RAISE NOTICE '    SELECT user_id, ''twitter_phase2'', t.id FROM twitter_report_user_config c';
            RAISE NOTICE '    JOIN llm_prompt_templates t ON t.template_code = c.phase2_template_code';
            RAISE NOTICE '    WHERE COALESCE(c.phase2_template_code, '''') <> '''' ON CONFLICT DO NOTHING;';
            RAISE NOTICE 'Then re-run this migration.';
        ELSE
            -- 全部行都是 NULL/空 phase config = 与 "无偏好" 完全等价, 安全删除
            RAISE NOTICE 'twitter_report_user_config: % rows, all empty phase configs. '
                         'Safe to drop (semantically equivalent to no user_template_preferences row).',
                         total_rows;
            DROP TABLE twitter_report_user_config CASCADE;
            RAISE NOTICE '✅ Dropped twitter_report_user_config';
        END IF;
    ELSE
        RAISE NOTICE 'twitter_report_user_config does not exist, skipping.';
    END IF;
END $$;

-- ==========================================================================
-- 2. 孤儿 PG 函数清理
-- ==========================================================================
-- 这三个函数在 add_unread_count.sql 中创建, 但函数体引用了已被
-- drop_subscription_id_column.sql 删除的 subscription_content.subscription_id 字段.
-- 任何调用都会报错 "column sc.subscription_id does not exist".
-- 应用层早已不调用 (改用 SubscriptionContentService 内部计算).
-- DROP 安全, 不影响业务.

DROP FUNCTION IF EXISTS update_subscription_unread_count(INT, INT) CASCADE;
DROP FUNCTION IF EXISTS refresh_all_unread_counts() CASCADE;
DROP FUNCTION IF EXISTS trigger_update_unread_count_on_read() CASCADE;

DO $$
BEGIN
    RAISE NOTICE '✅ Dropped legacy unread_count helper functions';
END $$;

-- ==========================================================================
-- 验证
-- ==========================================================================
-- 跑完后:
--   SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='twitter_report_user_config');
--   -- 应为 false
--   SELECT proname FROM pg_proc WHERE proname IN ('update_subscription_unread_count', 'refresh_all_unread_counts', 'trigger_update_unread_count_on_read');
--   -- 应为空
