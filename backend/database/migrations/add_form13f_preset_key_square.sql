-- Remove Key Square: ceased 13F holdings after 2024Q2; not kept as a watched filer.
-- Idempotent.

DELETE FROM form13f_holdings
 WHERE filer_id IN (SELECT id FROM form13f_filers WHERE cik = '0001662970');

DELETE FROM form13f_snapshots
 WHERE filer_id IN (SELECT id FROM form13f_filers WHERE cik = '0001662970');

DELETE FROM form13f_filers WHERE cik = '0001662970';

DELETE FROM form13f_presets
 WHERE label = 'Key Square' OR cik = '0001662970';
