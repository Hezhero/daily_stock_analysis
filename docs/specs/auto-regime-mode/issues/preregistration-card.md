# 票 03 预注册卡：同一天四口径 A/B 对照

> 状态：**预注册冻结 → 已实跑、观察性记录已落盘（2026-11-13）**。本卡在看过任何结果之前写定
> 对照口径、估计量、判据、变体计数与回滚方式（baseline `7ae970ef`）；实现轮曾事后加入数值 PASS gate，
> 两轴 review 判定违反 `docs/adr/0003`，已按用户裁决移除——当前**没有数值判据、没有自动 gate**，
> `overall_pass: null`，由 `human_verdict.reason` 记录人工观察结论。未补跑「对 auto 有利」的变体。
> 原 Q8/R4=否 与 spec Out of Scope「不跑完整回测 / WFO / 真实 PG 查询」的约束由 2026-11-13 用户
> 显式授权解除，仅限本卡四口径对照；未改默认值、未重判 10 月四张卡、未回滚 ADR 0006。
> 产物：`result/gates/auto-regime-ab/verdict.json`（`status=observations-recorded`，
> 含 `variants` / `human_verdict` / `interpretation`）。
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

> **判据（开跑前冻结，保持 baseline `7ae970ef` 原文）**：各口径相对 `enh2` 的 Δ；`0/5`
> 天数变化；两类零推荐成因区分；弱市 / 58 段过拟合风险讨论。**没有数值 PASS/FAIL 门槛**——
> §7.3 原定「人工填充 `overall_pass` 与判决理由」。2026-11-13 review 发现实现轮曾事后加入
> 数值 gate，按 ADR 0003 已移除；当前 verdict 为**观察性记录**（`overall_pass: null`，
> `human_verdict.reason` 由人工填写观察结论），不声称冻结判据 PASS。本卡是确认性证据，
> 不是可翻转默认的 gate（ADR 0006 豁免）。

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
- `overall_pass: null` / `adjudicated: false` / `status=observations-recorded` 表示**未按数值判据
  判决**（ADR 0003：baseline 未冻结数值门槛，§7.3 原定人工填充）；`human_verdict.reason` 记录
  人工观察结论。结果不利**不自动回滚**，回滚须另立票并作废 ADR 0006（`auto_default_rollback` 写明）。
- `variants.tested_count=4` / `preregistered_primary=true` 由 `assemble_run_evidence` 写入 verdict。
- 结论回填 spec「待验证项」，但不改变票 02 的默认决定。

## 6. 可运行脚手架与实跑接线

`scripts/auto_regime_ab_compare.py`（纯函数，不跑回测）：
- `compare_four_modes(arms)` / `build_verdict(comparison)`；`load_arms(paths)` 只读四份既有 summary JSON；
- CLI 产出预注册 verdict 骨架；**不连 PG、不伪造缺失变体、不事后加变体**；缺 `zero_open_days_in_window` 直接报错。
- 测试：`tests/test_auto_regime_ab_compare.py`（12 passed）。

`scripts/auto_regime_ab_run.py`（实跑后的产物接线，不跑回测本身）：
- `derive_snapshot_id(summary_path)` 从产物 stem 推导 `<end_date>_<universe>`；
- `extract_zero_open_days(log, col)` 从 driver 日志行 `验证区间 <col> 天数: X/5` 推导 `5-X`；
- `assemble_run_evidence(arms, snapshot_id, out_path)` 校验四臂产物 stem 与声明快照一致、
  组装**观察性记录**（`overall_pass: null`、含 `variants.tested_count=4` /
  `preregistered_primary=true` 与空 `human_verdict` 由人工填写），**无自动数值 PASS gate**；
- 测试：`tests/test_auto_regime_ab_run.py`（9 passed）。

`scripts/run_auto_regime_ab.sh`（四跑编排，2026-11-13 授权后执行）：串行 `auto → enh2 → enh → strict`，
各带 `--regime-mode <mode> --portfolio-sim --portfolio-sim-out result/gates/auto-regime-ab/runs/<mode> --no-handoff`，
`LOG_DIR` 分目录写日志；首跑构建准备态缓存，后三跑命中。四跑共享同一数据快照
（PG 最大 `trade_date=2026-11-12`，`check_data_freshness.py --as-of 2026-11-12` 六表全 OK）、
同一 `.env` 静态开关（`BACKTEST_CONFLUENCE=1` / `BACKTEST_REGIME_SOFT=1`）、同一 `universe=pit`。
`--no-handoff` 跳过实盘 `main.py` 子进程；portfolio-sim 产物在 `run_backtests` 内先行写出。

## 7. 开跑检查清单（2026-11-13 执行结果）

1. ✅ 四臂 `--regime-mode` 产物 stem 全部推导为 `20261112_pit`（`derive_snapshot_id` 校验），
   `universe=pit` 一致；PG 最大 `trade_date=2026-11-12`，`check_data_freshness.py --as-of 2026-11-12` 六表全 OK。
2. ✅ `zero_open_days_in_window` 由四份 driver 日志行 `验证区间 <col> 天数: 0/5` 推导为 5，
   未默认补 0；`scripts/auto_regime_ab_run.py::extract_zero_open_days` 缺行 / 非 5 日窗口直接报错。
3. ✅ `assemble_run_evidence` 产出**观察性记录**（`overall_pass: null`、`wfo.required=false`、
   `variants.tested_count=4` / `preregistered_primary=true`）；`human_verdict.reason` 由人工填写
   观察结论（`auto ≡ enh2`、四臂 `0/5`、中性结果），**无自动数值 PASS gate**。
   review 发现实现轮曾事后加入数值判据，按 ADR 0003 已移除（见 §2 注）。
4. ✅ spec「待验证项」、票 03 状态/Comments、CHANGELOG 已回填。
5. ✅ 未新增变体、未改判据（恢复 baseline 描述性判据）；四臂恰为预注册的
   `auto/enh2/enh/strict`（`tested_count=4`）。

**结论摘要**：`auto ≡ enh2` 逐位相同（Δ 全 0），四臂均 `0/5`，`auto` 未减少 0/5 天；
固定档位差异非 auto 增益。结果中性，默认值 `auto` 保持不变，未回滚。完整数字见 verdict.json。
