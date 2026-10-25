# 05: 卡 C：同日并发候选排序——共振优先 vs 期望优先

**What to build:** 把同日并发候选的排序优先级从「期望优先」翻成「共振优先」，其余键、上限、口径全部不动，只做一次预注册主比较。

**Blocked by:** 01, 02, 04

**Gate:** `v2-portfolio`
**Signal set:** `unchanged`

**Status:** ready-for-human（预注册比较已执行，判决 FAIL，默认保持 `lead_exp_first`；见文末判决）

## 现状更正（2026-10-23）

票初稿写的是「在候选折叠与成交之间引入同日并发信号选股 / 持仓上限排序，并保留趋势簇 ≤3、行业 ≤2」。核对代码后，这些**已经在跑**：

- `_entry_reject()`（`scripts/portfolio_sim.py:356-377`）已实现 `max_positions → already_held → family_cap → industry_cap → unbuyable → no_price_data` 六个拒绝条件。
- `_ordered_candidates()`（`:342-353`）已有排序：`entry_date ↑ → lead_exp ↓ → n_families ↓ → strength ↓ → code ↑ → strategy ↑`。
- `simulate()`（`:467` 起；排序入口 `:476`）按该顺序贪心填充，撞上限记 reject。
- `n_families` 由 `apply_declustered_resonance()`（`:958-986`）算好，即「去簇共振数」。

所以本卡唯一还没做的改动，是**换排序的前两位优先级**。照初稿「引入排序」去实现会得到一个 no-op：两臂 `signals.csv` 与 `equity.csv` 都会相同，ΔCAGR 恒为 0，白烧一次 `tested_count`。

## 预注册（开跑前固定；跑完不得更换口径）

| 项 | 冻结值 |
| --- | --- |
| 基线臂 | 现状不动：`lead_exp ↓ → n_families ↓ → strength ↓ → code ↑ → strategy ↑` |
| 候选臂 | `n_families ↓ → lead_exp ↓ → strength ↓ → code ↑ → strategy ↑`；仅交换前两位，后四位、`kind="mergesort"` 与 `na_position="last"` 保持不变 |
| 新旋钮 | `SimConfig.rank_mode ∈ {"lead_exp_first", "resonance_first"}`，默认 `"lead_exp_first"`；同时加入 `_REGISTERED_EXPERIMENT_DEFAULTS`（`scripts/backtest_5y_23strategies.py:4245`）与 `_HASH_OPTIONAL_KEYS`（`scripts/portfolio_sim.py:56`）。默认值不进 `config_hash`，因此 L-only `5eea05195af3d89432af8b2a049fde2517d6a53b` 与 PIT `acb32bd95d3c94c9c575b00008409c483cd04064` 两个冻结哈希必须逐位不变 |
| 并发上限 N | 即 `max_positions = 10`。本卡**不**新增「每日最多开 N 笔」闸门：同时改优先级与每日闸门会让过门归因不清，且与现金预算产生交互 |
| 不变项 | 度量口径（0.35% 往返成本、T+1 盯市、PIT 宇宙、`next_open` 入场）、`family_cap={trend:3}`、`industry_cap=2`、`min_resonance_families=2`、`max_holding_bars=10`、`size_mode=fixed_1n`、`target_positions=None`、`cash_buffer_pct=0.0` |
| 估计量 | v2 `oos_daily_portfolio_cagr_block_delta`（`docs/backtest-flip-gate-spec.md` §9）。**这是推论而非偏好**：`signals.csv` 产于 decluster 后、collapse 前（`scripts/backtest_5y_23strategies.py:4500-4502`、写入 `payload["signals"]` `:4512`/`:4523`），本卡不改信号池，v1 的 Δ 在构造上恒为 0（与卡 B 同一退化机制） |
| 主判据 | ΔCAGR 的 `ci_low > 0`（§9:124） |
| 护栏 | 同 §4 五项，阈值不放宽：`cagr_non_inferior`、`sharpe_non_inferior`、`maxdd_non_inferior`（均 δ=0）、`maxdd_cap ≤ 25.0`、`min_trades n_filled ≥ 100` |
| δ | 预注册 δ=0，不新增非劣性 margin。§4:57-59 已声明 δ=0 是描述性方向护栏、非形式化非劣性检验；引入 margin 属改 §4，不在本卡范围 |
| 变体计数 | 本卡首次主比较，`tested_count` 应为 **1**（与卡 A/B 同）。不试第三种顺序（如 `strength` 优先）；任何额外顺序都是探索性变体，只记录、不作翻转依据 |
| 臂数与重跑 | 两臂**各重跑一次**。不复用卡 A 的基线产物：v2 需要 `_equity.csv`，且卡 A 两臂窗口起点不同（`20210406` vs `20210514`），v2 要求两臂 OOS 日期集合完全一致 |

## 自检断言（跑完立即校验，失败即中止）

1. 两臂 `signals.csv` 的 sha256 **必须相同**。若不同，说明改动泄漏到了信号池，卡 C 的前提被破坏，该次运行作废（不得改口径后继续）。
2. 候选臂 `config_hash` 必须 ≠ 基线臂，且不等于 `5eea0519…` / `acb32bd9…`。
3. 基线臂 `config_hash` 必须仍等于 `acb32bd9…`（PIT 口径默认配置），以此证明 `rank_mode` 的默认值确实没进哈希。
4. 门禁 `--estimator portfolio` 下两臂 OOS 日期集合必须完全一致；不一致时门禁直接报错，不得取交集或补零。

## 采纳与驳回

- **通过**（主判据 + 五项护栏全过）：记录判决 JSON 与 `registry.jsonl`，默认值**不在本卡内翻**。
- **驳回**（任一 FAIL）：默认值保持 `lead_exp_first`，不改默认、不换口径、不扫参。
- **翻默认是独立后续动作**：`rank_mode` 在 `_HASH_OPTIONAL_KEYS` 中，而 `_config_hash()` 只 pop「等于当前 `SimConfig()` 默认值」的键（`scripts/portfolio_sim.py:446-453`），因此**只改默认值不会让哈希变化**。后续票翻默认时必须同时做三件事：① 把 `rank_mode` 移出 `_HASH_OPTIONAL_KEYS`；② 更新 `_REGISTERED_EXPERIMENT_DEFAULTS` 的期望值；③ 重算 `5eea0519…` 与 `acb32bd9…` 两个冻结基线哈希并在新票里固化。缺 ① 则基线静默复用。不得在卡 C 里顺手做这件事。

## 运行命令（两臂各一次）

```bash
# 基线臂（不带 rank_mode，走默认 lead_exp_first）
python scripts/backtest_5y_23strategies.py --portfolio-sim \
  --portfolio-sim-out result/gates/card-c/base

# 候选臂
python scripts/backtest_5y_23strategies.py --portfolio-sim \
  --portfolio-sim-rank-mode resonance_first \
  --portfolio-sim-out result/gates/card-c/cand

# 门禁：v2 组合层估计量
python scripts/portfolio_gate.py \
  --baseline-summary  result/gates/card-c/base/<stem>.json \
  --baseline-signals  result/gates/card-c/base/<stem>_signals.csv \
  --candidate-summary result/gates/card-c/cand/<stem>.json \
  --candidate-signals result/gates/card-c/cand/<stem>_signals.csv \
  --baseline-equity   result/gates/card-c/base/<stem>_equity.csv \
  --candidate-equity  result/gates/card-c/cand/<stem>_equity.csv \
  --estimator portfolio \
  --card-id card-c --data-hash <快照标识> --code-commit <实现提交>
```

已知别扭项：`--baseline-signals` / `--candidate-signals` 在 CLI 上是 `required=True`（`scripts/portfolio_gate.py:1174-1176`），即使 v2 不读它们也必须传。照传即可，本卡不修改该 CLI 语义。

- [x] `rank_mode` 旋钮落地（`SimConfig` + 登记表 + `_HASH_OPTIONAL_KEYS` + CLI `--portfolio-sim-rank-mode`），默认值下两个冻结哈希逐位不变。
- [x] 两臂各跑一次，四条自检断言全过。
- [x] 门禁 `--estimator portfolio` 产出判决 JSON 与 `registry.jsonl`（`tested_count=1`），报告平均现金占比、换手与成本拖累。
- [x] 改善若只存在于个别年份或个别 regime，不采纳（判决已 FAIL，未采纳；年度拆解见下，改善并非全期一致）。

## 落地（代码部分）

- **旋钮**：`SimConfig.rank_mode ∈ {"lead_exp_first","resonance_first"}`，默认 `lead_exp_first`；`_RANK_KEYS` 只让前两位排序键互换，`kind="mergesort"` / `na_position="last"` / 其余四位键不变。`rank_mode` 已入 `_HASH_OPTIONAL_KEYS`（默认值不进哈希）与 `_REGISTERED_EXPERIMENT_DEFAULTS`（默认值漂移守卫 + 显式覆盖告警）。
- **驱动**：`--portfolio-sim-rank-mode`（`choices=_RANK_MODES`，需同时启用 `--portfolio-sim`）；覆盖时 warning 并改 `config_hash`，日志新增 `rank_mode` 一项。
- **验证**：`pytest tests/test_portfolio_gate.py tests/test_portfolio_sim.py tests/test_portfolio_sim_integration.py tests/test_backtest_pit_universe.py -q` → **85 passed**（本卡新增 6 例，其余为既有回归）；`py_compile` 通过；`--help` 列出该 flag；`config_hash` 默认仍为 L-only `5eea0519…` / PIT `acb32bd9…`。
- **后续（2026-10-24 已执行）**：两臂真实回测与 `--estimator portfolio` 门禁判决已完成（见文末判决：FAIL），`Status` 转 `ready-for-human`，票 06 解锁。

## 判决（2026-10-24，按预注册执行，未改规则/基线/判据）

- **卡号 / 变体**：`card-c`；主比较一次，`preregistered_primary = true`，`tested_count = 1`；未试第三种顺序。
- **运行标识**：`code_commit = 5b4bfe16`；`data_snapshot_hash = 8a5542694c0b1dd10c1de9bb0bd87447f3ec71e48d981b25392f833f6f2d313d`；OOS `2022-01-17 → 2026-10-16`；产物 stem `portfolio_sim_20210514_20261016_pit`。

| 指标 | 基线 `lead_exp_first`（`acb32bd9…`） | 候选 `resonance_first`（`b742d01b…`） |
| --- | ---: | ---: |
| OOS CAGR | 4.203% | 5.581% |
| Sharpe | 0.4442 | 0.5623 |
| 最大回撤 | 13.907% | 13.579% |
| 成交笔数 | 560 | 558 |
| 平均持仓数 | 2.231 | 2.229 |
| 平均现金占比 | 77.69% | 77.71% |
| 换手（年化） | 112.63 | 112.79 |
| 成本拖累 | 51.35% | 51.22% |

- **判决结果**：**FAIL**。驳回项仅 `ci_excludes_zero`：ΔCAGR = **+1.377pct**，95% CI **[−0.883%, +3.909%]**，`se = 1.231`，下界未过零。
- **通过项**：`cagr_non_inferior`（5.581% ≥ 4.203%）、`sharpe_non_inferior`（0.5623 ≥ 0.4442）、`maxdd_non_inferior`（13.579% ≤ 13.907%）、`maxdd_cap`（13.579% ≤ 25%）、`min_trades`（558 ≥ 100）。即五项护栏全过、主判据未过：**点估计改善方向一致但证据不足以排除噪声**。
- **估计量**：`oos_daily_portfolio_cagr_block_delta`（v2 组合层），`resamples=2000`、`valid=2000`、`n_dates=1148`、`n_blocks=55`、`L=21`（两臂 `p99 holding_bars = 9`）、`seed=3768749268696168765`，`rng=numpy.random.PCG64`，`degenerate=false`。
- **四条自检**：
  1. 两臂 `signals.csv` sha256 同为 `2A5DB331A4AF48CBB699EF690C600E4740C56D597188311E60980815E7B9277D`，与卡 A/B 参考值一致 → 排序改动未泄漏到信号池。
  2. 候选 `config_hash = b742d01b88fc3545a6182726e0d7db7f2996b385` ≠ 基线，且不等于 `5eea0519…` / `acb32bd9…`。
  3. 基线 `config_hash = acb32bd95d3c94c9c575b00008409c483cd04064`，与 PIT 冻结值逐位一致 → `rank_mode` 默认值确未进哈希。
  4. 门禁 `--estimator portfolio` 下两臂 OOS 日期集合完全一致（各 1400 行，`2021-01-04 → 2026-10-16`），未取交集、未补零。
- **漏斗**：两臂 `raw=18477 / built=17873 / declustered=9904 / lead_eligible=9388 / collapsed=1522` 全同，仅 `filled` 由 560 变 558（`rejects_total` 17917 → 17919），`reconciled=true`。即改动只作用于 collapse 之后的同日排序竞争，成交减少 2 笔。
- **年度拆解（描述性，事后观察，不影响判决）**：候选减基线（pct）：2022 **+6.40**、2023 **−2.32**、2024 **+2.03**、2025 +0.37、2026 −0.28。改善集中在 2022/2024，2023 反向，非全期一致；按预注册「改善若只存在于个别年份或个别 regime，不采纳」本就不作翻转依据，与 `ci_excludes_zero` 结论同向。
- **结论**：不翻转默认值，`rank_mode` 默认保持 `lead_exp_first`，`resonance_first` 仅保留为显式 opt-in CLI 开关；不扫第三种排序、不改口径、不追加非劣性 margin。翻默认仍是独立后续动作（须同时移出 `_HASH_OPTIONAL_KEYS`、改 `_REGISTERED_EXPERIMENT_DEFAULTS`、重算两个冻结哈希，见「采纳与驳回」）。
- **证据**：`result/gates/card-c/v1/b742d01b88fc3545a6182726e0d7db7f2996b385.json`（`verdict_hash 324bdd1f4f40187f…`，`spec_hash 13f03e36d023…`，`code_commit 5b4bfe16`，`tested_count=1`，`registry_id c8dfe79373f5…`）；`result/gates/registry.jsonl` 追加一行。

### 执行偏差（不影响测量口径，逐条记录）

- **尝试 1 OOM**：首次基线臂在 `build_candidate_table` → `_canonical_panel` 处 `numpy._ArrayMemoryError`（申请 2.37 GiB，`(105, 6069134) float32`），留证 `result/gates/card-c/logs/attempt1-base-oom.log`。未改脚本、未调参，清空半成品目录后原地重跑同一条命令。
- **尝试 2 非交易日退出**：重跑时机器日期已跨到 2026-10-24（周六），两臂被 `is_trading_day(datetime.now())` 守卫秒退且 `exit=0`（留证 `attempt2-*`）。两臂均追加 `--force`；该 flag 只绕过当日交易日检查（`scripts/backtest_5y_23strategies.py:6402`），不改窗口、数据与判据。数据快照未变（PG `max(trade_date)=2026-10-16`，两臂 equity 行数与日期区间与卡 A/B 一致）。
- **`--no-handoff`**：两臂均加，避免 handoff 触发 `main.py` 子进程与真实推送；portfolio 产物在 handoff 之前写出，不影响测量。
- **`--oos-end 2026-10-16`**：两臂 summary 的 `window=null` 且 `metrics` 无 `oos_end`，门禁无法自解析，显式传入等于数据最大交易日的值（与卡 A/B registry 记录一致）。
- **runner 加产物断言**：因守卫退出码为 0，runner 改为每臂结束后校验 `portfolio_sim_*.json` 存在，缺失即 ABORT，避免把「秒退」当成功。

**Notes:** 排序键自带选择自由度，最易过拟合；本卡把自由度压到最低——只翻两位、只跑一次、候选臂一句话可描述、两臂信号池必须逐字节相同。任何「再试试另一种顺序」都属变体，须记录且不得用作翻转依据。
