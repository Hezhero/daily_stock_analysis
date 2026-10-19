# 02: 可构建翻转门槛：池化样本外 Δ 的日期分块 bootstrap CI

**What to build:** 一个可复用的「翻转门槛」入口，替代「必须先建出完整 WFO」的不可构建门槛：吃基线运行与候选运行，给出明确的通过/不通过判决，供卡 A/B/C/D 共用。

**Blocked by:** None (can start immediately)

**Status:** ready-for-human

- [x] 观测单元为 `(strategy, signal_date, code)`；`next_open` 入场、扣成本后的净收益、记录实际持有期；直接池化样本外信号，不平均各折指标。
- [x] 日期分块 bootstrap：block 长度取实际持有期的 99 分位，重采样 2000 次，有效样本为日期数（不是信号笔数）；输出 95% 置信区间。
- [x] 判据：95% CI 排除 0；样本外 CAGR 不劣于基线、Sharpe 不劣于基线、maxDD 不劣于基线；同时 maxDD ≤25%、成交笔数 ≥100。
- [x] 合成数据单测：已知正向 Δ 判定通过，零／负 Δ 判定不通过。
- [x] 判决附带已测变体计数，支撑「预注册单次比较、不按结果反复换参」的纪律。

**Notes:** 完整 WFO 仅保留给改信号或改出场定义的改动；本门槛服务于参数与资金配置类改动。

**落地：** `scripts/portfolio_gate.py`（`derive_seed` / `build_observations` / `block_length` / `moving_block_bootstrap` / `evaluate_criteria` / `run_gate` / `write_verdict` / `append_registry` / `format_human`）+ `tests/test_portfolio_gate.py`（8 例）；驱动侧追加去簇信号产物 `result/portfolio_sim_<start>_<end>_signals.csv`（`scripts/backtest_5y_23strategies.py` 的 `_run_portfolio_sim_chain`/`_write_portfolio_sim_artifacts`）。规格与偏离记录见 `docs/backtest-flip-gate-spec.md`、`docs/adr/0003-bootstrap-flip-gate-instead-of-wfo-folds.md`。
**验证：** `pytest tests/test_portfolio_gate.py tests/test_portfolio_sim_integration.py tests/test_portfolio_sim.py -q` → 39 passed；`py_compile` 通过；改动文件 LSP 无诊断。实际门槛判定（跑一次真实基线/候选）尚未执行——需一次完整组合模拟（≈1.5h/次），留待卡 A 正式判定时按预注册执行。
