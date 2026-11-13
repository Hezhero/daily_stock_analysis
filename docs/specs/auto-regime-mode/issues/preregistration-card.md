# 票 03 预注册卡：同一天四口径 A/B 对照

> 状态：**预注册（未开跑、未判决）**。本卡在看过任何结果之前写定对照口径、估计量、
> 判据、变体计数与回滚方式；遵循 `docs/adr/0003`（不得事后改判据或补跑「对 auto 有利」的变体）。
> 对照实跑需**单独显式授权**（Q8/R4=否、spec Out of Scope：不跑完整回测 / WFO / 真实 PG 查询）。
> 取数依赖票 02 已落地的 `auto` 构建，但按用户 2026-11-12 裁决「与实现并行」，本卡**不阻塞 02**。

## 1. 目的与边界

关闭 `docs/specs/auto-regime-mode/spec.md` Further Notes 的「待验证项」：
`auto` 相对固定三档在同一天 / 同一数据快照下实际减少多少 `0/5` 天、对 CAGR / maxDD /
胜率的影响，以及弱市年份与仅 58 个 regime 连续段下的过拟合风险。

- **只产证据与判决记录**，不改默认值、不重判 10 月四张实验卡、不自动回滚 ADR 0006。
- `0 只股票` 仍按 `docs/backtest-runbook.md` §4 属正常观测；本卡不承诺减少 0 推荐或提高收益。

## 2. 对照设计（开跑前冻结）

| 项 | 预注册值 |
| --- | --- |
| 变体 | 恰好 4 个：`auto` / `enh2` / `enh` / `strict`（`MODES`，不得新增） |
| 基线 | `enh2`（冻结 PIT 基线口径） |
| 数据快照 | 四个 `--regime-mode` 跑**同一数据截止日 / 同一 `universe` / 同一 `.env` 静态开关**；`data_snapshot_hash` 对齐说明随结果附 |
| 估计量 | 资金约束 `--portfolio-sim` 的 `cagr_oos` / `maxDD_pct` / `win_rate`，以及验证窗口 `0/5` 可开仓天数 |
| 判据 | 各口径相对 `enh2` 的 Δ；`0/5` 天数变化；两类零推荐成因区分；弱市 / 58 段过拟合风险讨论 |
| 变体计数 | 写入 `variants.tested_count = 4`，`preregistered_primary = true`（防数据窥探） |
| 估计量口径 | 沿用 portfolio_sim summary JSON 的 `metrics` 字段，不另造指标 |

## 3. 两类零推荐成因（必须分开报）

- **空仓市况**：验证窗口 `market_ok_auto`（或对应固定列）=False 的 `0/5` 天 → 可开仓闸门。
- **信号稀疏 / handoff 门槛**：有可开仓日但共振 / handoff 后 `0 只股票`（2026-08-24 `enh 5/5` 反例）。

`compare_four_modes` 的 `zero_cause_split` 显式记录这两条，禁止把两者混为一谈。

## 4. 过拟合风险（开跑前已知）

- `bull 70.3% / range 20.0% / bear 9.7%`、仅 58 个 regime 连续段；
- 同型 `regime` 切换 trail 曾被证伪（`docs/backtest-optimization-design.md`）；
- 任何 `auto` 增益都标为**暂定**，须样本外确认，不据此再调阈值。

## 5. 判决产物与 WFO 记录

- 判决产物写到既有 `result/gates/` 登记出口：`result/gates/auto-regime-ab/verdict.json`。
- 显式记录 `wfo: {required: false, folds: null, reason: ...}`，**不静默跳过**：
  理由 = ADR 0006 的默认翻转是用户显式豁免《指标契约》预注册放行规则，本卡是确认性证据、
  不是可回滚默认的 gate（沿用 `scripts/portfolio_gate.py` 既有 `wfo.required=False` 契约）。
- `overall_pass: null` / `adjudicated: false` 表示预注册未判决；结果不利**不自动回滚**，
  回滚须另立票并作废 ADR 0006（`auto_default_rollback` 字段写明）。
- 结论回填 spec「待验证项」，但不改变票 02 的默认决定。

## 6. 可运行脚手架（不跑回测）

`scripts/auto_regime_ab_compare.py`：
- 纯函数 `compare_four_modes(arms)` / `build_verdict(comparison)`；
- `load_arms(paths)` 只读四份既有 `--portfolio-sim` summary JSON；
- CLI `python scripts/auto_regime_ab_compare.py --auto A.json --enh2 B.json --enh C.json --strict D.json`
  产出预注册 verdict 骨架到 `result/gates/auto-regime-ab/verdict.json`；
- **不连 PG、不调用回测脚本、不伪造缺失变体、不事后加变体**；缺 `zero_open_days_in_window` 直接报错。

测试：`tests/test_auto_regime_ab_compare.py`（12 passed）。

## 7. 开跑检查清单（待授权后才执行）

1. 确认四个 `--regime-mode` 跑使用同一 `data_snapshot_hash` 与 `universe`；
2. 每份 summary 显式提供 `zero_open_days_in_window`（来自验证区间日志，不得默认 0）；
3. 用 CLI 生成 verdict 骨架，人工填充 `overall_pass` 与判决理由；
4. 回填 spec「待验证项」与本卡 Comments；
5. 若出现「对 auto 有利」的额外变体或判据改动 → 按 ADR 0003 视为违规，作废重注册。
