-- =====================================================
-- Fix calculate_theme_quality_score function
-- 修复 calculate_theme_quality_score 函数
-- 日期: 2025-10-31
-- 描述: 更新函数以使用 theme_name 替代已删除的 canonical_theme_name
-- =====================================================

-- 更新 calculate_theme_quality_score 函数
-- 将所有 WHERE 子句中的 canonical_theme_name 替换为 theme_name

CREATE OR REPLACE FUNCTION calculate_theme_quality_score(
    p_news_id INTEGER,
    p_theme_name VARCHAR,
    p_source_type VARCHAR,
    p_confidence_score NUMERIC
)
RETURNS NUMERIC AS $$
DECLARE
    v_source_count INTEGER;
    v_avg_confidence NUMERIC;
    v_has_vector_match BOOLEAN;
    v_is_verified BOOLEAN;
    v_quality_score NUMERIC;
    
    -- 不同来源的基础权重
    v_source_weight NUMERIC;
BEGIN
    -- 设置来源权重
    v_source_weight := CASE p_source_type
        WHEN 'manual' THEN 1.5
        WHEN 'vector_retrieval' THEN 1.0
        WHEN 'llm_classification' THEN 0.8
        WHEN 'eodhd_auto_tag' THEN 0.6
        ELSE 0.5
    END;
    
    -- 获取该新闻该主题的来源数量
    SELECT COUNT(DISTINCT source_type)
    INTO v_source_count
    FROM news_themes_unified
    WHERE news_id = p_news_id
      AND theme_name = p_theme_name
      AND is_active = true;
    
    -- 获取平均置信度
    SELECT AVG(confidence_score)
    INTO v_avg_confidence
    FROM news_themes_unified
    WHERE news_id = p_news_id
      AND theme_name = p_theme_name
      AND is_active = true;
    
    -- 检查是否有高分vector匹配
    SELECT EXISTS (
        SELECT 1
        FROM news_themes_unified
        WHERE news_id = p_news_id
          AND theme_name = p_theme_name
          AND source_type = 'vector_retrieval'
          AND confidence_score >= 0.8
          AND is_active = true
    ) INTO v_has_vector_match;
    
    -- 检查是否被验证
    SELECT BOOL_OR(is_verified)
    INTO v_is_verified
    FROM news_themes_unified
    WHERE news_id = p_news_id
      AND theme_name = p_theme_name
      AND is_active = true;
    
    -- 计算质量分数
    v_quality_score := (
        -- 来源权重 * 置信度 (40%)
        v_source_weight * p_confidence_score * 0.4 +
        
        -- 平均置信度 (30%)
        COALESCE(v_avg_confidence, p_confidence_score) * 0.3 +
        
        -- 多源一致性加分 (20%)
        LEAST(v_source_count::NUMERIC / 3.0, 1.0) * 0.2 +
        
        -- 高质量vector匹配加分 (5%)
        CASE WHEN v_has_vector_match THEN 0.05 ELSE 0 END +
        
        -- 人工验证加分 (5%)
        CASE WHEN v_is_verified THEN 0.05 ELSE 0 END
    );
    
    -- 限制在0-1范围
    RETURN LEAST(GREATEST(v_quality_score, 0.0), 1.0);
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION calculate_theme_quality_score IS '计算主题质量分数 - 综合考虑来源、置信度、一致性等因素（已更新：使用 theme_name）';

-- =====================================================
-- 完成提示
-- =====================================================

DO $$
BEGIN
    RAISE NOTICE '✅ 已更新 calculate_theme_quality_score 函数：使用 theme_name 替代 canonical_theme_name';
END $$;

