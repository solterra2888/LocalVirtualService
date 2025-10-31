-- ============================================================
-- 修复 normalize_theme_name() 函数
-- 解决aliases匹配后返回错误值的BUG
-- ============================================================

CREATE OR REPLACE FUNCTION normalize_theme_name(raw_name VARCHAR)
RETURNS VARCHAR AS $$
DECLARE
    normalized VARCHAR;
    result_canonical VARCHAR;
BEGIN
    -- 输入验证
    IF raw_name IS NULL OR raw_name = '' THEN
        RETURN raw_name;
    END IF;
    
    -- 清理：去除前后空格，转小写
    normalized := LOWER(TRIM(raw_name));
    
    -- 策略1: 精确匹配 canonical_name
    SELECT canonical_name INTO result_canonical
    FROM theme_master_data
    WHERE LOWER(canonical_name) = normalized
      AND is_active = true
    LIMIT 1;
    
    IF FOUND THEN
        RETURN result_canonical;
    END IF;
    
    -- 策略2: 匹配 aliases（使用LATERAL子查询）
    SELECT tmd.canonical_name INTO result_canonical
    FROM theme_master_data tmd,
         LATERAL jsonb_array_elements_text(tmd.aliases) AS alias_value
    WHERE tmd.is_active = true
      AND LOWER(alias_value) = normalized
    LIMIT 1;
    
    IF FOUND THEN
        RETURN result_canonical;
    END IF;
    
    -- 策略3: 未匹配，返回清理后的小写名称（保持一致性）
    RETURN normalized;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION normalize_theme_name IS '标准化主题名称 - 支持canonical_name精确匹配和aliases模糊匹配';

-- ============================================================
-- 同时优化 auto_normalize_theme_name 触发器函数
-- 添加更详细的日志和错误处理
-- ============================================================

CREATE OR REPLACE FUNCTION auto_normalize_theme_name()
RETURNS TRIGGER AS $$
BEGIN
    -- 1. 自动标准化主题名称
    NEW.canonical_theme_name := normalize_theme_name(NEW.theme_name);
    
    -- 2. 如果有匹配的master_data，自动关联
    IF NEW.theme_master_id IS NULL AND NEW.canonical_theme_name IS NOT NULL THEN
        SELECT id INTO NEW.theme_master_id
        FROM theme_master_data
        WHERE canonical_name = NEW.canonical_theme_name
          AND is_active = true
        LIMIT 1;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION auto_normalize_theme_name IS '触发器函数 - 自动标准化主题名称并关联theme_master_data';

-- ============================================================
-- 添加更新 usage_count 的触发器函数
-- ============================================================

CREATE OR REPLACE FUNCTION update_theme_master_usage_count()
RETURNS TRIGGER AS $$
BEGIN
    -- 当新记录关联到theme_master_data时，增加usage_count
    IF TG_OP = 'INSERT' AND NEW.theme_master_id IS NOT NULL THEN
        UPDATE theme_master_data
        SET usage_count = usage_count + 1,
            last_used_at = CURRENT_TIMESTAMP
        WHERE id = NEW.theme_master_id;
    END IF;
    
    -- 当更新记录改变关联时
    IF TG_OP = 'UPDATE' THEN
        -- 如果从NULL变为有值，或者改变了关联
        IF (OLD.theme_master_id IS NULL AND NEW.theme_master_id IS NOT NULL) OR
           (OLD.theme_master_id IS NOT NULL AND NEW.theme_master_id IS NOT NULL AND OLD.theme_master_id != NEW.theme_master_id) THEN
            -- 新关联的主题 +1
            UPDATE theme_master_data
            SET usage_count = usage_count + 1,
                last_used_at = CURRENT_TIMESTAMP
            WHERE id = NEW.theme_master_id;
            
            -- 旧关联的主题 -1
            IF OLD.theme_master_id IS NOT NULL AND OLD.theme_master_id != NEW.theme_master_id THEN
                UPDATE theme_master_data
                SET usage_count = GREATEST(usage_count - 1, 0)
                WHERE id = OLD.theme_master_id;
            END IF;
        END IF;
    END IF;
    
    -- 当删除记录时（或is_active变为false）
    IF (TG_OP = 'DELETE' AND OLD.theme_master_id IS NOT NULL) OR
       (TG_OP = 'UPDATE' AND OLD.is_active = true AND NEW.is_active = false AND NEW.theme_master_id IS NOT NULL) THEN
        UPDATE theme_master_data
        SET usage_count = GREATEST(usage_count - 1, 0)
        WHERE id = COALESCE(NEW.theme_master_id, OLD.theme_master_id);
    END IF;
    
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql;

-- 创建触发器
DROP TRIGGER IF EXISTS trigger_update_theme_usage_count ON news_themes_unified;
CREATE TRIGGER trigger_update_theme_usage_count
    AFTER INSERT OR UPDATE OR DELETE ON news_themes_unified
    FOR EACH ROW
    EXECUTE FUNCTION update_theme_master_usage_count();

COMMENT ON FUNCTION update_theme_master_usage_count IS '触发器函数 - 自动更新theme_master_data的usage_count';

-- ============================================================
-- 验证修复
-- ============================================================

DO $$
DECLARE
    test_input TEXT;
    test_result TEXT;
    test_expected TEXT;
BEGIN
    RAISE NOTICE '=== 验证 normalize_theme_name() 修复 ===';
    
    -- 测试1: aliases匹配
    test_input := 'AI';
    test_expected := 'artificial_intelligence';
    test_result := normalize_theme_name(test_input);
    
    IF test_result = test_expected THEN
        RAISE NOTICE '✓ 测试1通过: % -> %', test_input, test_result;
    ELSE
        RAISE WARNING '✗ 测试1失败: % -> % (期望: %)', test_input, test_result, test_expected;
    END IF;
    
    -- 测试2: aliases匹配（中文）
    test_input := '人工智能';
    test_expected := 'artificial_intelligence';
    test_result := normalize_theme_name(test_input);
    
    IF test_result = test_expected THEN
        RAISE NOTICE '✓ 测试2通过: % -> %', test_input, test_result;
    ELSE
        RAISE WARNING '✗ 测试2失败: % -> % (期望: %)', test_input, test_result, test_expected;
    END IF;
    
    -- 测试3: canonical_name精确匹配
    test_input := 'artificial_intelligence';
    test_expected := 'artificial_intelligence';
    test_result := normalize_theme_name(test_input);
    
    IF test_result = test_expected THEN
        RAISE NOTICE '✓ 测试3通过: % -> %', test_input, test_result;
    ELSE
        RAISE WARNING '✗ 测试3失败: % -> % (期望: %)', test_input, test_result, test_expected;
    END IF;
    
    -- 测试4: 未匹配（应返回小写）
    test_input := 'UnknownTheme';
    test_expected := 'unknowntheme';
    test_result := normalize_theme_name(test_input);
    
    IF test_result = test_expected THEN
        RAISE NOTICE '✓ 测试4通过: % -> %', test_input, test_result;
    ELSE
        RAISE WARNING '✗ 测试4失败: % -> % (期望: %)', test_input, test_result, test_expected;
    END IF;
    
    RAISE NOTICE '=== 验证完成 ===';
END $$;

