-- =====================================================
-- Theme Report - 新增 csv_content 列
-- =====================================================
-- 创建时间: 2026-06-19
-- 说明: 为 theme_reports 增加 csv_content 列，存储 "Download spreadsheet"
--       导出的成分股收益 CSV（ticker, theme, Categories, 1D, 1W, 1M, 3M, 1Y）。
--       与 html_content 同模式：报告生成时一并写入，公开下载端点直接从 DB 返回，
--       无需 OSS 签名 URL（避免 24h 过期）。

ALTER TABLE theme_reports ADD COLUMN IF NOT EXISTS csv_content TEXT;

COMMENT ON COLUMN theme_reports.csv_content IS
    '成分股收益 CSV 文本（ticker,theme,Categories,1D,1W,1M,3M,1Y）；报告生成时写入，供 /theme-report/shared/{token}/csv 下载';
