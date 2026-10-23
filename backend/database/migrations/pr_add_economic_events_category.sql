-- 为 economic_events 增加类别列（对齐 Trading Economics Calendar 的 Category 筛选）
-- 分类规则: backend/services/economic_events_service.py 的 classify_event_category()
-- 新数据由抓取链路写入; 存量数据用 scripts/backfill_economic_events_actual.py 同源的
-- 分类器回填（直接调用 service 函数，见下方说明）。
--
-- 应用方式:
--   psql -f pr_add_economic_events_category.sql
--   然后运行一次性回填:
--   python -c "from backend.services.economic_events_service import EconomicEventsService; \
--              EconomicEventsService().backfill_event_categories()"

ALTER TABLE economic_events ADD COLUMN IF NOT EXISTS category text DEFAULT 'other';

CREATE INDEX IF NOT EXISTS idx_economic_events_category
    ON economic_events (category);
