# 04: 卡 B：资金部署感知仓位

**What to build:** 把组合模拟器的单笔仓位预算从固定 1/N（前一日权益 ÷ 持仓上限）改为资金部署感知规则：优先使用真实可用的闲置现金，可选按波动率目标缩放，v1 保持不加杠杆。目标是在不突破回撤约束的前提下提高资金占用与样本外 CAGR，直击 68–78% 现金长期闲置的结构性根因。

**Blocked by:** 01, 02（03 为排序约定，非依赖：卡 A 判决已出且不通过，不阻塞本卡；见账本 §10.6）

**Status:** ready-for-agent

- [ ] 复用既有全局配置 `_TARGET_VOL_ANNUAL` / `_MAX_LEVERAGE`，不新增平行实现；v1 保持 `_MAX_LEVERAGE=1.0`，不引入杠杆。
- [ ] 不变量测试：部署金额 ≤ 可用现金；持仓数 ≤ `max_positions`；`max_dd_floor_pct` 生效；结果确定性（同参两次运行逐位一致）。
- [ ] 任何参数改动改变 `config_hash` 并留下侧边产物，不允许静默调参。
- [ ] 经 02 门槛：样本外 CAGR 提升，Sharpe 与 maxDD 不劣于基线；平均现金占比下降；换手、成本拖累与容量变化一并报告。
- [ ] 回滚路径保留（关闭部署感知开关即回到固定 1/N）；账本记录判决与 `config_hash`。

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
