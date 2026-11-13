# 涨跌榜 W/M/YTD 修复：锚点拼接 + 口径对齐 Google

## Context

用户核对发现「今日涨跌榜」1月/YTD 与 Google 不一致（NVDA：我们 -23.24%/-6.63% vs Google ≈-27.3%/-10.9%）。排查结论：

1. **主因·锚点错位一天**：`_rank_perf_spark`（`build_snapshot.py:507`）在快照构建时刻（美东 ~19-21 点）拉 Yahoo 2 年日线，若 Yahoo 尚未滚出 T 日 bar（滚日时间浮动于美东 19:00-22:00，竞态），序列最后一根是 T-1 收盘，spark/W1/M1/YTD 全部以 T-1 为锚、参考日随之平移；而价格/当日涨跌列来自 EODHD bulk（已含 T 收盘）→ 同一行混用两个交易日。T=11-12 期美股 81/81 行全部错位，T=11-11 期 89/89 正常；港/A 股行不受影响。
2. **次因·价格口径**：涨跌榜用复权价（总回报），Google 用"拆股复权、股息不复权"的原始收盘 → 系统性差 ~0.2pp（NVDA，无拆股小股息）至 ~1pp（高分红股）。

目标：锚点永远与价格列同为 T 日收盘；口径与 Google/行情软件对齐；顺带修复今天（11-13 期）已发布报告。

## 改动（单文件：`tools/facts-report/build_snapshot.py`）

### 1. `_rank_perf_spark` 改造（:507-543）

- **序列基准改为原始收盘** `indicators.quote[0].close`（拆股已追溯复权、股息不复权 = Google 口径；adjclose 降级为缺失时的旧回退路径不变，但优先 raw）。函数 docstring 同步改写口径说明。
- **新增锚点拼接**（新增参数 `anchor_price`、`anchor_chg`，由调用方从行内传入 EODHD bulk 的 `price`/`change_percent`）：
  - 条件：序列最后一根日期 < T，且 anchor_price > 0、anchor_chg 非 None；
  - 校验：`implied_prev = anchor_price / (1 + anchor_chg/100)`，要求 `|implied_prev / raw_last_close - 1| ≤ 0.5%`（raw_last_close = 同响应中最后一根 ≤T 的原始收盘；对不上说明有拆股/两源打架 → 放弃拼接，保持旧行为）；
  - 通过则 `pts.append((T, anchor_price))`，spark/参考日（`_ref_close_desc`/`_ytd_close_desc`）自动以 T 为最新一根重算；
  - 返回 dict 增加 `"spliced": True/False` 供日志。

### 2. `_rank_sparks`（:546-571）传参 + 日志

- `_work` 调用改为 `_rank_perf_spark(r["ysym"], t, anchor_price=r.get("price"), anchor_chg=r.get("change_percent"))`；
- 末尾 print 增加拼接计数（如 `[rank_sparks] ... 锚点拼接 N 行`），便于日后观测 Yahoo 滚日竞态频率。
- **保持不动**：`r["ysym"]` 赋值（:556）与行序（turnover 降序，:741）——`metadata.rank_kline` 按行序索引对齐，不能变；us/hk/cn 三市场共用此路径，港/A 股正常日序列本就含 T，拼接为 no-op。

### 3. 明确不改

- kline 弹窗（register 时冻结的 ysym 规格驱动，`register_morning_brief.py:248-258`）不受影响；
- FT 大盘总览 ETF 行保持复权总回报（GuanHe 版式逆向验证过的口径）；
- 另两份管线拷贝（无此代码）不同步。

## 修复今天这期（T=2026-11-12 工作区）

```bash
# 1) 在现有快照上重算涨跌榜（Yahoo 现已含 11-12 bar，锚点自然正确；
#    顺带验证新代码：此时拼接不触发，走 raw 基准新口径）
python - <<'EOF'   # 读 snapshot.json → 调 _rank_sparks(vl, T) → 写回
EOF
# 2) 重渲染 + 重发布
$PY tools/facts-report/report_html_tools/md2html_report.py 2026-11-12
touch .../final/report.md && $PY tools/facts-report/register_morning_brief.py --ok 2026-11-12
```

## 验证

1. **拼接单测（monkeypatch 演练）**：伪造 Yahoo 响应剥掉最后一根 bar，以 NVDA T=2026-11-12 + anchor(166.19, -4.34) 调 `_rank_perf_spark`：期望 w1=-8.49%、m1=-27.28%、ytd=-10.89%（raw 口径）、spark 尾值 166.19、`spliced=True`；再喂不匹配的 anchor（如 price=100）→ `spliced=False`、行为同旧版。
2. **口径核对**：对 NVDA 用 raw 序列手算 w1/m1/ytd 与函数输出一致，且与 Google 页面 1月/YTD 数值吻合（±0.3pp 内，参考日可能有 1-2 天差异）。
3. **快照重算断言**：重跑后美股 81 行 `spark[-1] == price`（±0.02）；NVDA perf 更新为 -8.49/-27.28/-10.89；hk/cn 行数值不变；`[rank_sparks]` 日志行数正常。
4. **端到端**：md2html 后检查 `window.__RANK__` 中 NVDA 的 m/y 已更新、`rank_kline` 规格与重发布前一致（ysym/行序未动）；register 后抽查分享页涨跌榜数字与 `/kline` 弹窗正常。
5. **回归面**：`_rank_perf_spark` 全仓库唯一调用方就是 `_rank_sparks`（已确认），无其他消费方。