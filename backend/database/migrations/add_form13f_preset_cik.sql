-- Form 13F 预设机构冻结 EDGAR CIK。身份不再用申报名匹配。
-- 可重复执行。

ALTER TABLE form13f_presets
    ADD COLUMN IF NOT EXISTS cik VARCHAR(10);

UPDATE form13f_presets SET cik = '0001350694' WHERE label = 'Bridgewater Associates';
UPDATE form13f_presets SET cik = '0001067983' WHERE label = 'Berkshire Hathaway Inc';
UPDATE form13f_presets SET cik = '0001697748' WHERE label = 'ARK Investment Management';
UPDATE form13f_presets SET cik = '0000949509' WHERE label = 'Oaktree Capital Management';
UPDATE form13f_presets SET cik = '0001656456' WHERE label = 'Appaloosa LP';
UPDATE form13f_presets SET cik = '0001336528' WHERE label = 'Pershing Square Capital Management';
UPDATE form13f_presets SET cik = '0001061768' WHERE label = 'Baupost Group';
UPDATE form13f_presets SET cik = '0001021944' WHERE label = 'Temasek Holdings';
UPDATE form13f_presets SET cik = '0001762304' WHERE label = 'Hillhouse Capital (HHLR)';
UPDATE form13f_presets SET cik = '0001167483' WHERE label = 'Tiger Global Management';
UPDATE form13f_presets SET cik = '0001536411' WHERE label = 'Duquesne Family Office';
UPDATE form13f_presets SET cik = '0000850529' WHERE label = 'Fisher Asset Management';
UPDATE form13f_presets SET cik = '0001029160' WHERE label = 'Soros Fund Management';
UPDATE form13f_presets SET cik = '0001649339' WHERE label = 'Scion Asset Management';
UPDATE form13f_presets SET cik = '0001088875' WHERE label = 'Baillie Gifford';
UPDATE form13f_presets SET cik = '0001061165' WHERE label = 'Lone Pine Capital';
UPDATE form13f_presets SET cik = '0001040273' WHERE label = 'Third Point LLC';
UPDATE form13f_presets SET cik = '0001135778' WHERE label = 'Miller Value Partners';
UPDATE form13f_presets SET cik = '0001135730' WHERE label = 'Coatue Management';
UPDATE form13f_presets SET cik = '0000921669' WHERE label = 'Icahn Carl C';

CREATE UNIQUE INDEX IF NOT EXISTS idx_f13f_presets_cik
    ON form13f_presets (cik)
    WHERE cik IS NOT NULL;

COMMENT ON COLUMN form13f_presets.cik IS '冻结的 EDGAR CIK（10 位）。拉取 13F-HR 的身份主键。';
COMMENT ON COLUMN form13f_holdings.value_usd IS '持仓市值（美元）。edgartools holdings 已把申报原单位折成美元。';
