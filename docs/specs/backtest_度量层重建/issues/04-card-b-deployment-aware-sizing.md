# 04: 卡 B：资金部署感知仓位

**What to build:** 把组合模拟器的单笔仓位预算从固定 1/N（前一日权益 ÷ 持仓上限）改为资金部署感知规则：优先使用真实可用的闲置现金，可选按波动率目标缩放，v1 保持不加杠杆。目标是在不突破回撤约束的前提下提高资金占用与样本外 CAGR，直击 68–78% 现金长期闲置的结构性根因。

**Blocked by:** 01, 02（03 为排序约定，非依赖：卡 A 判决已出且不通过，不阻塞本卡；见账本 §10.6）

**Gate:** `v1-signal`
**Signal set:** `unchanged`

**Status:** ready-for-human（预注册比较已执行，判决 FAIL，默认保持 `fixed_1n`；见文末判决与账本 §10.8）

- [x] 以 `SimConfig` 三个旋钮（`size_mode` / `target_positions` / `cash_buffer_pct`）实现，不新增平行模拟路径；v1 未启用波动率目标缩放（`_TARGET_VOL_ANNUAL` 保持 0.0），`_MAX_LEVERAGE` 保持 1.0，不引入杠杆。
- [x] 不变量测试：部署金额 ≤ 可用现金；持仓数 ≤ `max_positions`；`max_dd_floor_pct` 生效；结果确定性（同参两次运行逐位一致）。
- [x] 任何参数改动改变 `config_hash` 并留下侧边产物，不允许静默调参。
- [x] 经 02 门槛：样本外 CAGR 提升，Sharpe 与 maxDD 不劣于基线；平均现金占比下降；换手、成本拖累与容量变化一并报告。（**判决不通过**：maxDD 13.91%→16.82% 劣化 → 默认不翻转；换手 112.63→134.60、成本拖累 51.35%→61.99% 已报告，容量无独立指标未单独量化）
- [x] 回滚路径保留（关闭部署感知开关即回到固定 1/N）；账本记录判决与 `config_hash`。

**Notes:** 提高资金占用会放大回撤与集中度；T+1 下的现金缓冲不能压到零。与卡 C 共享资金约束基础设施，但分开跑以保持归因清晰。

**预注册（2026-10-21，开跑前写定）：**

- **卡号 / 变体**：`card-b`；主比较一次，`preregistered_primary = true`，`tested_count` 由 `result/gates/registry.jsonl` 派生。
- **规则（v1，写死）**：`size_mode=deploy_aware`，`target_positions=5`，`cash_buffer_pct=0.05`；每笔预算 `max(0, min(equity_prev / 5, cash - 0.05 * equity_prev))`。不加杠杆（`_MAX_LEVERAGE=1.0` 不启用），不改 `max_positions` 与其它度量口径。
- **基线**：同一数据快照的 `fixed_1n` 臂（`config_hash acb32bd9…`，即账本 §10.5 的卡 A 基线臂产物 `result/gates/card-a/base/`）；若开跑时数据快照已变（`data_hash` 不再等于 `8a554269…`），则基线也重跑，保证两臂同快照。
- **候选命令**：

```bash
python scripts/backtest_5y_23strategies.py --portfolio-sim \
  --portfolio-sim-size-mode deploy_aware \
  --portfolio-sim-target-positions 5 \
  --portfolio-sim-cash-buffer-pct 0.05 \
  --portfolio-sim-out result/gates/card-b/cand
```

- **判据**：`docs/backtest-flip-gate-spec.md` §4 全部 6 项（`ci_excludes_zero` + CAGR/Sharpe/maxDD 三项不劣 + maxDD ≤25% + 成交 ≥100）。
- **不做的**：不在 4/6/8 个目标持仓之间扫参；若 OOM 重跑，只重跑同一条命令，不调参。

**判决（2026-10-21，按预注册执行，未改规则/基线/判据）：FAIL**

| 指标 | 基线 `fixed_1n`（`acb32bd9…`） | 候选 `deploy_aware`（`d1d40cbc…`） |
| --- | ---: | ---: |
| OOS CAGR | 4.203% | 5.818% |
| Sharpe | 0.4442 | 0.4729 |
| 最大回撤 | 13.907% | **16.819%** |
| 成交笔数 | 560 | 440 |
| 平均持仓数 | 2.231 | 1.769 |
| 平均现金占比 | 77.69% | 73.11% |
| 成本拖累 | 51.35% | 61.99% |

- **驳回项**：`maxdd_non_inferior`（回撤劣化 +2.91pct，真实驳回）、`ci_excludes_zero`（估计量退化，见下）。
- **通过项**：`cagr_non_inferior`、`sharpe_non_inferior`、`maxdd_cap`（16.82% ≤ 25%）、`min_trades`（440 ≥ 100）。
- **估计量退化**：门禁估计量为信号级净收益均值差，两臂 `signals.csv` 逐字节相同（sha256 `2A5DB331…`），`delta=0.000%`、`se=0`、`degenerate=true`。该估计量无法裁决纯资金管理卡；**不在看过结果后更换口径以翻案**，默认值保持 `fixed_1n`，门禁支持组合层估计量属后续工作。
- **结论**：不翻转默认值，不在 4/6/8 之间扫参；`deploy_aware` 保留为显式 opt-in CLI 开关。
- **证据**：`result/gates/card-b/v1/d1d40cbcfd66903d8d4c6e06bc579daee129a539.json`（`verdict_hash a0e4e7c7…`，`code_commit 1704cadd`，`tested_count=1`，`registry_id c70136ea5954`）；账本 §10.8。
