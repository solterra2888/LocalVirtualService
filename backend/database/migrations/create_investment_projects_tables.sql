-- =====================================================
-- Investment Projects Module - Database Migration Script
-- 创建日期: 2025-11-23
-- 描述: 创建投资项目管理相关的所有表
-- =====================================================
-- 
-- 功能说明：
-- 1. 投资项目主表 (investment_projects)
-- 2. 项目成员表 (project_members)
-- 3. 项目里程碑表 (project_milestones)
-- 4. 项目新闻跟踪表 (project_news_tracking)
-- 5. 项目文件表 (project_files)
-- 6. 项目与Watchlist关联表 (project_watchlist_link)
--
-- 使用方法:
--   psql -h 111.230.48.119 -U postgres -d rc_database -f create_investment_projects_tables.sql
-- =====================================================

BEGIN;

-- ============================================================
-- 1. 创建投资项目主表
-- ============================================================

CREATE TABLE IF NOT EXISTS investment_projects (
    id SERIAL PRIMARY KEY,
    
    -- 基本信息
    project_code VARCHAR(50) UNIQUE NOT NULL,
    project_name VARCHAR(200) NOT NULL,
    project_type VARCHAR(20) NOT NULL,
    
    -- 关联信息
    ticker_symbol VARCHAR(50),
    industry VARCHAR(100),
    sector VARCHAR(100),
    region VARCHAR(50),
    
    -- 项目状态
    status VARCHAR(20) DEFAULT 'active',
    priority INTEGER DEFAULT 2,
    
    -- 项目描述
    description TEXT,
    investment_thesis TEXT,
    target_price DECIMAL(20, 4),
    current_position TEXT,
    
    -- 团队管理
    created_by INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    owner_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- 时间追踪
    start_date DATE,
    target_end_date DATE,
    actual_end_date DATE,
    
    -- 元数据
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 约束
    CONSTRAINT chk_project_type CHECK (project_type IN ('ticker', 'industry', 'sector')),
    CONSTRAINT chk_project_status CHECK (status IN ('active', 'paused', 'completed', 'archived')),
    CONSTRAINT chk_priority CHECK (priority BETWEEN 1 AND 3)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_projects_created_by ON investment_projects(created_by);
CREATE INDEX IF NOT EXISTS idx_projects_owner_id ON investment_projects(owner_id);
CREATE INDEX IF NOT EXISTS idx_projects_status ON investment_projects(status);
CREATE INDEX IF NOT EXISTS idx_projects_ticker ON investment_projects(ticker_symbol);
CREATE INDEX IF NOT EXISTS idx_projects_type ON investment_projects(project_type);
CREATE INDEX IF NOT EXISTS idx_projects_created_at ON investment_projects(created_at);

-- 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_investment_projects_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_investment_projects_updated_at ON investment_projects;
CREATE TRIGGER trg_investment_projects_updated_at
    BEFORE UPDATE ON investment_projects
    FOR EACH ROW
    EXECUTE FUNCTION update_investment_projects_updated_at();

-- 添加注释
COMMENT ON TABLE investment_projects IS '投资项目主表';
COMMENT ON COLUMN investment_projects.project_code IS '项目唯一编号，如: PROJ-AAPL-20251123';
COMMENT ON COLUMN investment_projects.project_type IS '项目类型: ticker, industry, sector';
COMMENT ON COLUMN investment_projects.status IS '项目状态: active, paused, completed, archived';
COMMENT ON COLUMN investment_projects.priority IS '优先级: 1=Low, 2=Medium, 3=High';

-- ============================================================
-- 2. 创建项目成员表
-- ============================================================

CREATE TABLE IF NOT EXISTS project_members (
    id SERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES investment_projects(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    
    -- 角色和权限
    role VARCHAR(20) DEFAULT 'member',
    can_edit BOOLEAN DEFAULT TRUE,
    can_invite BOOLEAN DEFAULT FALSE,
    
    -- 状态
    status VARCHAR(20) DEFAULT 'active',
    invited_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    invited_at TIMESTAMP WITH TIME ZONE,
    joined_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 元数据
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- 约束
    UNIQUE(project_id, user_id),
    CONSTRAINT chk_member_role CHECK (role IN ('owner', 'admin', 'member', 'viewer')),
    CONSTRAINT chk_member_status CHECK (status IN ('active', 'inactive', 'invited'))
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_project_members_project ON project_members(project_id);
CREATE INDEX IF NOT EXISTS idx_project_members_user ON project_members(user_id);
CREATE INDEX IF NOT EXISTS idx_project_members_status ON project_members(status);

-- 创建更新时间触发器
DROP TRIGGER IF EXISTS trg_project_members_updated_at ON project_members;
CREATE TRIGGER trg_project_members_updated_at
    BEFORE UPDATE ON project_members
    FOR EACH ROW
    EXECUTE FUNCTION update_investment_projects_updated_at();

-- 添加注释
COMMENT ON TABLE project_members IS '项目成员表';
COMMENT ON COLUMN project_members.role IS '角色: owner, admin, member, viewer';
COMMENT ON COLUMN project_members.status IS '状态: active, inactive, invited';

-- ============================================================
-- 3. 创建项目里程碑表
-- ============================================================

CREATE TABLE IF NOT EXISTS project_milestones (
    id SERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES investment_projects(id) ON DELETE CASCADE,
    
    -- 里程碑信息
    milestone_type VARCHAR(50) NOT NULL,
    title VARCHAR(200) NOT NULL,
    content TEXT,
    
    -- 关联文件
    file_ids INTEGER[],
    
    -- 决策相关
    decision_type VARCHAR(50),
    decision_rationale TEXT,
    
    -- 元数据
    created_by INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    milestone_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_milestone_type CHECK (
        milestone_type IN ('meeting_notes', 'research_report', 'decision', 'update', 'other')
    ),
    CONSTRAINT chk_decision_type CHECK (
        decision_type IS NULL OR decision_type IN ('buy', 'sell', 'hold', 'watch', 'pass')
    )
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_milestones_project ON project_milestones(project_id);
CREATE INDEX IF NOT EXISTS idx_milestones_type ON project_milestones(milestone_type);
CREATE INDEX IF NOT EXISTS idx_milestones_date ON project_milestones(milestone_date);
CREATE INDEX IF NOT EXISTS idx_milestones_created_at ON project_milestones(created_at);

-- 创建更新时间触发器
DROP TRIGGER IF EXISTS trg_project_milestones_updated_at ON project_milestones;
CREATE TRIGGER trg_project_milestones_updated_at
    BEFORE UPDATE ON project_milestones
    FOR EACH ROW
    EXECUTE FUNCTION update_investment_projects_updated_at();

-- 添加注释
COMMENT ON TABLE project_milestones IS '项目里程碑/阶段成果表';
COMMENT ON COLUMN project_milestones.milestone_type IS '类型: meeting_notes, research_report, decision, update, other';
COMMENT ON COLUMN project_milestones.decision_type IS '决策类型: buy, sell, hold, watch, pass';

-- ============================================================
-- 4. 创建项目新闻跟踪表
-- ============================================================

CREATE TABLE IF NOT EXISTS project_news_tracking (
    id SERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES investment_projects(id) ON DELETE CASCADE,
    news_id INTEGER NOT NULL REFERENCES news(id) ON DELETE CASCADE,
    
    -- 新闻标注
    relevance_score INTEGER,
    is_important BOOLEAN DEFAULT FALSE,
    user_notes TEXT,
    
    -- 操作记录
    added_by INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    added_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(project_id, news_id),
    CONSTRAINT chk_relevance_score CHECK (relevance_score IS NULL OR (relevance_score BETWEEN 1 AND 5))
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_project_news_project ON project_news_tracking(project_id);
CREATE INDEX IF NOT EXISTS idx_project_news_news ON project_news_tracking(news_id);
CREATE INDEX IF NOT EXISTS idx_project_news_important ON project_news_tracking(is_important);
CREATE INDEX IF NOT EXISTS idx_project_news_added_at ON project_news_tracking(added_at);

-- 创建更新时间触发器
DROP TRIGGER IF EXISTS trg_project_news_tracking_updated_at ON project_news_tracking;
CREATE TRIGGER trg_project_news_tracking_updated_at
    BEFORE UPDATE ON project_news_tracking
    FOR EACH ROW
    EXECUTE FUNCTION update_investment_projects_updated_at();

-- 添加注释
COMMENT ON TABLE project_news_tracking IS '项目新闻跟踪表';
COMMENT ON COLUMN project_news_tracking.relevance_score IS '相关性评分: 1-5';
COMMENT ON COLUMN project_news_tracking.is_important IS '是否标记为重要';

-- ============================================================
-- 5. 创建项目文件表
-- ============================================================

CREATE TABLE IF NOT EXISTS project_files (
    id SERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES investment_projects(id) ON DELETE CASCADE,
    
    -- 文件信息
    file_name VARCHAR(255) NOT NULL,
    file_path TEXT NOT NULL,
    file_type VARCHAR(50),
    file_size BIGINT,
    
    -- 文件分类
    category VARCHAR(50),
    description TEXT,
    
    -- 关联信息
    milestone_id INTEGER REFERENCES project_milestones(id) ON DELETE SET NULL,
    
    -- 元数据
    uploaded_by INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    uploaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_file_category CHECK (
        category IS NULL OR category IN ('meeting_notes', 'report', 'contract', 'presentation', 'other')
    )
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_project_files_project ON project_files(project_id);
CREATE INDEX IF NOT EXISTS idx_project_files_category ON project_files(category);
CREATE INDEX IF NOT EXISTS idx_project_files_milestone ON project_files(milestone_id);
CREATE INDEX IF NOT EXISTS idx_project_files_uploaded_at ON project_files(uploaded_at);

-- 添加注释
COMMENT ON TABLE project_files IS '项目文件表';
COMMENT ON COLUMN project_files.category IS '文件分类: meeting_notes, report, contract, presentation, other';

-- ============================================================
-- 6. 创建项目与Watchlist关联表
-- ============================================================

CREATE TABLE IF NOT EXISTS project_watchlist_link (
    id SERIAL PRIMARY KEY,
    project_id INTEGER NOT NULL REFERENCES investment_projects(id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    watchlist_id INTEGER NOT NULL REFERENCES target_tracking_config(id) ON DELETE CASCADE,
    
    -- 关联信息
    linked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    linked_by INTEGER REFERENCES users(id) ON DELETE SET NULL,
    
    UNIQUE(project_id, user_id, watchlist_id)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_project_watchlist_project ON project_watchlist_link(project_id);
CREATE INDEX IF NOT EXISTS idx_project_watchlist_user ON project_watchlist_link(user_id);
CREATE INDEX IF NOT EXISTS idx_project_watchlist_id ON project_watchlist_link(watchlist_id);

-- 添加注释
COMMENT ON TABLE project_watchlist_link IS '项目与用户Watchlist关联表';

-- ============================================================
-- 7. 创建有用的视图
-- ============================================================

-- 视图1: 项目成员统计视图
CREATE OR REPLACE VIEW v_project_member_stats AS
SELECT 
    p.id as project_id,
    p.project_code,
    p.project_name,
    COUNT(pm.id) as member_count,
    COUNT(CASE WHEN pm.role = 'owner' THEN 1 END) as owner_count,
    COUNT(CASE WHEN pm.role = 'admin' THEN 1 END) as admin_count,
    COUNT(CASE WHEN pm.status = 'active' THEN 1 END) as active_member_count
FROM investment_projects p
LEFT JOIN project_members pm ON p.id = pm.project_id
GROUP BY p.id, p.project_code, p.project_name;

COMMENT ON VIEW v_project_member_stats IS '项目成员统计视图';

-- 视图2: 项目活动概览视图
CREATE OR REPLACE VIEW v_project_activity_summary AS
SELECT 
    p.id as project_id,
    p.project_code,
    p.project_name,
    p.status,
    COUNT(DISTINCT pm.id) as member_count,
    COUNT(DISTINCT pn.id) as news_count,
    COUNT(DISTINCT pf.id) as file_count,
    COUNT(DISTINCT pms.id) as milestone_count,
    MAX(pms.milestone_date) as latest_milestone_date,
    MAX(pn.added_at) as latest_news_added_at
FROM investment_projects p
LEFT JOIN project_members pm ON p.id = pm.project_id AND pm.status = 'active'
LEFT JOIN project_news_tracking pn ON p.id = pn.project_id
LEFT JOIN project_files pf ON p.id = pf.project_id
LEFT JOIN project_milestones pms ON p.id = pms.project_id
GROUP BY p.id, p.project_code, p.project_name, p.status;

COMMENT ON VIEW v_project_activity_summary IS '项目活动概览视图';

-- ============================================================
-- 8. 插入示例数据（可选，生产环境请注释掉）
-- ============================================================

-- 注释掉示例数据，生产环境不需要
/*
-- 假设已有用户ID 1 (admin)
INSERT INTO investment_projects (
    project_code, project_name, project_type, ticker_symbol, sector, region,
    description, investment_thesis, priority, status,
    created_by, owner_id, start_date
) VALUES (
    'PROJ-TSLA-20251123',
    '特斯拉深度研究',
    'ticker',
    'TSLA.US',
    'Consumer Cyclical',
    'US',
    '研究特斯拉的技术优势和市场前景',
    '电动车领军企业，FSD技术领先，能源业务增长潜力大',
    3,
    'active',
    1,
    1,
    CURRENT_DATE
);

-- 添加项目创建者为owner
INSERT INTO project_members (project_id, user_id, role, can_edit, can_invite, status)
SELECT id, created_by, 'owner', true, true, 'active'
FROM investment_projects
WHERE project_code = 'PROJ-TSLA-20251123';
*/

-- ============================================================
-- 完成
-- ============================================================

COMMIT;

-- 显示创建结果
SELECT 'Investment Projects tables created successfully!' as status;

-- 显示表列表
SELECT 
    table_name,
    (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as column_count
FROM information_schema.tables t
WHERE table_schema = 'public' 
AND table_name IN (
    'investment_projects',
    'project_members', 
    'project_milestones',
    'project_news_tracking',
    'project_files',
    'project_watchlist_link'
)
ORDER BY table_name;

