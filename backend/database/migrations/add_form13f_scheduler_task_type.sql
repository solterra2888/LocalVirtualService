-- Add form13f to task_scheduling.valid_task_type constraint
-- 约束管理说明: valid_task_type 约束统一由 db_manager._update_task_scheduling_constraints
-- 在每次启动时按"表内已有类型 + 代码内置清单"合并重建（合并安全）。
-- 此处不再用静态清单 DROP/ADD 约束——静态清单落后于表内类型时会导致
-- ADD CONSTRAINT 违反已存在行, 进而毒化整个启动事务（2026-10 修复的存量 bug）。

-- 默认调度行: 每工作日 UTC 21:00 轮询 SEC 13F 数据页（发现新季度数据集→下载→导入）
-- SEC 每季度发布一次数据集，工作日每日一次轮询足够；触发成本低（单次 GET）
-- id 为应用层生成的 varchar（无默认值），此处显式提供
INSERT INTO task_scheduling (id, name, type, schedule_type, cron_expression, timezone, enabled, params, created_at)
SELECT 'form13f_Form 13F Quarterly Poll', 'Form 13F Quarterly Poll', 'form13f',
       'cron', '0 21 * * 1-5', 'UTC', true, '{"mode": "poll"}'::jsonb, CURRENT_TIMESTAMP
WHERE NOT EXISTS (SELECT 1 FROM task_scheduling WHERE type = 'form13f');

-- 预设机构（与静态原型 docs/form13f_dashboard/build_data.py PRESETS 一致）
INSERT INTO form13f_presets (label, match_name, sort_order) VALUES
    ('Bridgewater Associates', 'Bridgewater Associates, LP', 1),
    ('Berkshire Hathaway Inc', 'Berkshire Hathaway Inc', 2),
    ('ARK Investment Management', 'ARK Investment Management LLC', 3),
    ('Oaktree Capital Management', 'OAKTREE CAPITAL MANAGEMENT LP', 4),
    ('Appaloosa LP', 'APPALOOSA LP', 5),
    ('Pershing Square Capital Management', 'Pershing Square Capital Management, L.P.', 6),
    ('Baupost Group', 'BAUPOST GROUP LLC/MA', 7),
    ('Temasek Holdings', 'Temasek Holdings (Private) Ltd', 8),
    ('Hillhouse Capital (HHLR)', 'HHLR ADVISORS, LTD.', 9),
    ('Tiger Global Management', 'TIGER GLOBAL MANAGEMENT LLC', 10),
    ('Duquesne Family Office', 'Duquesne Family Office LLC', 11),
    ('Fisher Asset Management', 'Fisher Asset Management, LLC', 12),
    ('Soros Fund Management', 'SOROS FUND MANAGEMENT LLC', 13),
    ('Scion Asset Management', 'Scion Asset Management, LLC', 14),
    ('Baillie Gifford', 'BAILLIE GIFFORD & CO', 15),
    ('Lone Pine Capital', 'LONE PINE CAPITAL LLC', 16),
    ('Third Point LLC', 'Third Point LLC', 17),
    ('Miller Value Partners', 'Miller Value Partners, LLC', 18),
    ('Coatue Management', 'COATUE MANAGEMENT LLC', 19),
    ('Icahn Carl C', 'ICAHN CARL C', 20)
ON CONFLICT (label) DO NOTHING;
