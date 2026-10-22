# 08: 门禁组合层估计量（独立立项，不重判既有卡）

**What to build:** 给 `scripts/portfolio_gate.py` 增加可选的组合层估计量：以样本外每日组合收益为观测单元，联合日期分块 bootstrap 估计 Δ 年化几何收益（百分点）。目的是让"只改资金规模或配置、不改信号集合"的卡也能被统计裁决——这类卡在 v1 信号级估计量下两臂信号逐字节相同，Δ 构造上恒为 0（见账本 §10.8）。

**Blocked by:** None（02 已落地）。**本票不重判 card-a / card-b。**

**Status:** ready-for-human

- [x] `--estimator {signal,portfolio}`，默认 `signal`；默认路径的 `GATE_SPEC_HASH`、估计量字段、判决结构与既有产物逐位不变。
- [x] 组合层输入：`--baseline-equity` / `--candidate-equity` 指向两臂 `_equity.csv`；`estimator=portfolio` 时必填，`signal` 时缺省且不得多余传入。
- [x] 观测单元为 OOS 交易日：先在全期权益序列上计算 `ret_t = equity_t / equity_{t-1} − 1`，再按 `oos_start..oos_end` 切片；两臂 OOS 日期集合必须完全一致，不一致直接报错（不静默取交集）。
- [x] 点估计 Δ = CAGR_cand − CAGR_base（百分点），`CAGR = (∏(1+r_t))^(252/n) − 1`；`ci_excludes_zero` 的观测值改为该 Δ。
- [x] 联合非环绕移动块 bootstrap：`L = max(21, ceil(max-arm p99 holding_bars))`，`k = ceil(n_dates/L)`，每次重抽的日期索引同时作用于两臂，2000 次，percentile 2.5/97.5。
- [x] 退化保护：有效重抽 < 1 或 Δ 分布极差为 0 时 `degenerate=true`，CI 不得用于通过（`ci_low > 0` 自然为假）。
- [x] 变体计数按 `card_id` 跨估计量合并：同一张卡用新估计量再跑一次，`tested_count` 必须 +1，不能借估计量切换重置。
- [x] 单测 ≥6 例：正向优效通过、相同权益曲线退化且不通过、日期错位报错、块长下限 21、CLI 缺 equity 参数报错、同参两次运行确定性；并含"默认 `signal` 路径未变"的回归断言。

**Notes:** 卡 A / 卡 B 的 FAIL 结论不变，也不得用新估计量翻案——那属于探索性变体。卡 C（票 05）若要使用组合层估计量，必须在开跑前写进自己的预注册，不能跑完再选口径。组合层估计量仍未覆盖选择偏倚与容量：日收益块 bootstrap 只量化抽样噪声。

**预注册（写定于实现前）：**

- **估计量 id**：`oos_daily_portfolio_cagr_block_delta`；观测单元 `oos_trading_day`。
- **主判据**：`ci_excludes_zero` 作用于 ΔCAGR（百分点）；其余 5 项判据与阈值同 `docs/backtest-flip-gate-spec.md` §4，不新增、不放宽。
- **块长规则**：`max(21, ceil(p99 holding_bars of each arm))`；21 为下限，理由是一个交易月的短程自相关不应被块长截断。
- **重抽**：2000 次，percentile CI，`np.random.PCG64`；种子派生沿用 §6，`estimator_id` 与块长参与派生。
- **输入口径**：`equity` 列恒定为正；同日重复行、非有限值、`n_dates < L` 一律报错。
- **适用边界**：仅用于"信号集合相同、资金配置不同"的卡；改信号或出场的卡仍走 v1 信号级估计量或完整 WFO。

**落地：** `scripts/portfolio_gate.py`（`PORTFOLIO_ESTIMATOR_ID` / `_PORTFOLIO_GATE_SPEC` / `PORTFOLIO_GATE_SPEC_HASH` / `build_portfolio_returns` / `portfolio_block_bootstrap` / `load_equity` / `run_gate(estimator=...)` / `format_human` 的 delta 标度 / CLI 三参数）+ `tests/test_portfolio_gate.py`（9 例）。规格见 `docs/backtest-flip-gate-spec.md` §9，立项记录见 `docs/backtest-optimization-design.md` §10.9。
**验证：** `pytest tests/test_portfolio_gate.py tests/test_portfolio_sim.py tests/test_portfolio_sim_integration.py tests/test_backtest_pit_universe.py -q` → 79 passed（本批新增默认 `signal` 路径的冻结断言）；`py_compile` 通过。默认路径对比 `HEAD` 版本模块的合成运行：payload（去 `created_at`）、registry 行（去 `recorded_at`）、`verdict_hash` 逐位一致，`GATE_SPEC_HASH` 仍为 `88e87917…`。未对 `result/gates/**` 跑新估计量，`registry.jsonl` 仍为 2 行。
