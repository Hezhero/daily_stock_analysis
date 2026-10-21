# 03: 卡 A：翻转 min_resonance_families 默认值（过门槛才翻）

**What to build:** 在 02 的翻转门槛下评估「≥1 家族」相对「≥2 家族」的消融。通过才把 `min_resonance_families` 的默认值从 2 改为 1（走 01 的登记覆盖语义），并把证据写入账本；不通过则保留 2 并记录失败原因。**不重跑消融。**

**Blocked by:** 01, 02

**Status:** ready-for-human（判决 FAIL，默认值保持 2，见账本 §10.5）

- [x] 用 02 对 ≥1 vs ≥2 的消融产出明确的通过／不通过判决。
- [x] 通过时：默认值变为 1，`config_hash` 相应改变；旧结果与产物并列保留，不覆盖历史产物。→（未触发：判决不通过，默认值保持 2，PIT `config_hash` 仍为 `acb32bd9…`）
- [x] 不通过时：默认值保持 2，账本记录失败原因，不继续在 2 与 3 之间扫参。
- [x] 账本追加该卡的判决、`config_hash` 与风险调整口径对比（成交增加带来的成本拖累累计入）。

**Notes:** ≥1 家族的消融已完成，是当前最强、最便宜的已测杠杆；本卡只差门槛。

**判决（2026-10-21）：不通过。** 主判据 `ci_excludes_zero` 不过：每信号净收益 Δ = **+0.115%**，95% CI **[−0.059%, +0.304%]** 含 0（`L=9`、`B=2000`、`n_dates=332`、`valid=2000`、`seed=10374700140155257347`）。其余 5 项判据全部 PASS（CAGR 4.20%→7.48%、Sharpe 0.444→0.677、maxDD 13.91%→10.48%、成交 560→829），但按 `docs/backtest-flip-gate-spec.md` §4 不得用护栏杆替代主判据。默认值保持 2，不在 2 与 3 之间扫参。

**落地：**

- **门槛入口**：ticket 02 只交付了库函数，真实判决没有可复现入口。本次为 `scripts/portfolio_gate.py` 增加 argparse CLI（`--baseline-summary/--baseline-signals/--candidate-summary/--candidate-signals/--card-id/--data-hash/--out-dir/--oos-end/--code-commit/--exploratory/--tested-count`），出口码 `0=PASS / 1=FAIL / 2=输入不可用`，判决写 content-addressed JSON 并追加 `registry.jsonl`；`tests/test_portfolio_gate.py` 补 3 例 CLI 回归（PASS→0、FAIL→1、输入不可用→2）。
- **两臂各重跑一次**：2026-10-17 的 fam1 消融产物落在临时目录且早于 ticket 02 的去簇信号产物（无 `signals.csv`），物理上无法喂门槛，因此按预注册重跑两臂，而不是复用旧消融数字。基线臂 `config_hash = acb32bd95d3c…`，其 `signals.csv` sha256 与 2026-10-19 冻结产物逐字节一致（`2A5DB331…`）。
- **候选臂 `config_hash = 39272e93e83593933a35481ce751cbae36429314`**；两臂 OOS 窗一致（`2022-01-17 ~ 2026-10-16`、`n_days=1400`、`oos_days=1147`），`data_hash = 8a554269…`。
- **产物**：`result/gates/card-a/{base,cand}/`（两臂 `signals.csv` 与 summary JSON）、`result/gates/card-a/v1/39272e93….json`（verdict）、`result/gates/registry.jsonl`（`tested_count=1`）。均为本地未跟踪产物；命令与关键数字已固化在账本 §10.5。
- **验证**：`python -m pytest tests/test_portfolio_gate.py -q` → 11 passed；`py_compile scripts/portfolio_gate.py` 通过；真实门禁执行一次，`EXIT=1`（FAIL 属预期结果，不是运行错误）。
