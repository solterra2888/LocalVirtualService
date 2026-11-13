# DXY 数据源优化：FT 表 US dollar 行改为 EODHD 优先、Yahoo 回退

## Context

11-13 期晨报出现数据完整性横幅「US dollar(截至 2026-11-11)」。根因：FT 表三个汇率行中 EUR/USD、USD/JPY 走 EODHD（`market_data_eod`），唯独 DXY 因 `us_close_chart.py:183` 一条**过时的**"无 EODHD 授权"注释走 Yahoo（`DX-Y.NYB`）；而 Yahoo 的 ICE 美元指数当日收盘终值发布时间不固定（本例渲染后 1-1.5 小时才出），渲染时刻（~21:00 美东）常拿不到当日值 → stale 台账 → 横幅+Lark 告警。

**已实测验证**：EODHD EOD 端点有 `DXY.INDX`（正确代码；`DX.INDX`/`DXY.FOREX` 均 Not Found），11-12 收盘 103.128 已在，与 Yahoo 数值差仅 1-4bp；EODHD realtime 对 DXY 闭市返回 NA。结论：EOD 收盘切 EODHD 消除竞态，实时刷新保留 Yahoo 前缀。

**不选"加进上游 ingest 清单"的原因**：图表需要 ~340 天历史算 1W/1M/YTD（`_changes`/`_refs`），上游 ingest 只增量写近期行，还需一次性 400 天回填且耦合 ingest 调度（07:00 北京跑批）；直接 EODHD EOD 调用一次拿全量历史，自包含、零上游耦合，与本文件已有的 FRED/Yahoo 直连模式一致。

## 改动（单文件：`tools/facts-report/us_close_chart.py`）

### 1. 新增 helper `_eodhd_eod(code, t, days=400)`

- token 获取照抄 `build_snapshot.py:574-589 _eodhd_api` 模式：`os.environ["EODHD_API_TOKEN"]` → 回退正则解析 `REPO_ROOT/.env`（us_close_chart 目前无 .env 加载，需引入该小段；REPO_ROOT 按本文件路径推导）
- `GET https://eodhd.com/api/eod/{code}`，params `from=t-400d, to=t, fmt=json`，timeout 90
- 返回 `(series, ohlc)` 二元组，均降序：series=`[(date, close)]`，ohlc=`[(date,o,h,l,c)]`（同一次调用产出，过滤 `date>t` 与 close/open 为 None 的行，原始升序反转）；任何异常返回 `(None, None)`

### 2. 重写 DXY 块（`us_close_chart.py:454-463`）

```python
# ---- FX (DXY: EODHD DXY.INDX 优先,Yahoo 回退; EUR/USD & USD/JPY from EODHD)
dxy_s, dxy_ohlc = _eodhd_eod("DXY.INDX", t)
if dxy_s:
    print("[us_close] DXY via EODHD DXY.INDX")
else:
    dxy_s = _yahoo_series("DX-Y.NYB", t, adjusted=False)
    dxy_ohlc = _yahoo_ohlc("DX-Y.NYB", t)
    print("[us_close] DXY EODHD unavailable — Yahoo fallback")
if dxy_s:
    fx.append(_row("US dollar", "DXY", _minus(f"{dxy_s[0][1]:.2f}"), "%",
                   _changes(dxy_s), dxy_s[0][1], as_of=dxy_s[0][0],
                   hist=dxy_ohlc or dxy_s,
                   code="YAHOO:DX-Y.NYB",  # 实时刷新仍走 Yahoo(EODHD realtime 对 DXY 闭市 NA)
                   refs=_refs(dxy_s)))
else:
    print("[us_close] DXY unavailable via both EODHD and Yahoo — row skipped")
```

要点：`code` 保持 `YAHOO:DX-Y.NYB`（`morning_brief_report_service._fetch_live_quotes:350/379` 按 `YAHOO:` 前缀路由 Yahoo，实时层不动）；`refs`/`as_of`/`hist` 全部改由 EODHD 序列驱动；stale 判定逻辑（`_dump_ft_json:615` 严格组 `as_of != t`）无需改动。

### 3. 修正过时注释

`_yahoo_ohlc` docstring（:183「用于 DXY 等无 EODHD 授权的代码」）及 DXY 块旧注释，改为"Yahoo 回退用于 DXY（EODHD 优先）"。

## 重发布 11-13 期（用户已确认执行）

```bash
PY=/root/miniconda3/envs/guanhe/bin/python
WS=/opt/guanhetech/backend/reports/facts-report/2026-11-12
$PY tools/facts-report/us_close_chart.py --workspace $WS        # 重渲 01_markets.png + ft_sections.json
touch $WS/final/report.md                                       # 强制 register 重跑 md2html(复用条件是 html≥md mtime)
$PY tools/facts-report/register_morning_brief.py --ok 2026-11-12 # 重注册: token 轮换、OSS 同键覆盖
```

## 验证

1. **重渲后断言**：`ft_sections.json` 中 DXY 行 `as_of=="2026-11-12"`、`last≈103.128`、`m1d≈+0.21%`、`hist` 最后一天为 11-12 且含 OHLC；`stale` 列表为空；PNG 已更新。
2. **回退演练**：`EODHD_API_TOKEN=invalid $PY us_close_chart.py --workspace $WS` → 日志出现 "Yahoo fallback"、行仍产出（Yahoo 现已有 11-12 bar，as_of 应为 2026-11-12、last≈103.17）。
3. **重发布后断言**：DB `morning_brief_reports` 11-13 行 `metadata->'data_warnings'` 为 NULL、share_token 已换新；OSS `reports/MorningBrief/report_20261113.html` 覆盖成功（register 日志）；重渲将演练步骤污染的 ft_sections.json 覆盖回 EODHD 版后再注册（顺序注意：演练 → 正式重渲 → touch → register）。
4. **页面抽查**：`/api/morning-brief-report/shared/{新token}` 返回 HTML 无横幅、DXY 显示 103.13；`/shared/{token}/quotes` 正常返回（DXY 行走 Yahoo 路由不报错）。

## 范围外（明确不做）

- 另两份同源拷贝（GuanHe-Daily-Report、tools/morning-brief）不同步（用户已确认）
- 上游 ingest 增加 DXY.INDX、register 时延迟补拉重试（EODHD 优先后原竞态已消除，stale 横幅+Lark 兜底仍保留）