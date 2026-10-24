# 06: 卡 D：tushare_moneyflow 分层主力资金因子

**What to build:** 扩展数据加载读取 `tushare_moneyflow` 的超大单／大单分层字段，构造「大单 + 超大单净额」因子，登记为新策略或既有策略的过滤条件。相对现有 `net_mf_amount` 必须有增量贡献，不能只是重复已有资金流信息。优先级最低，基线稳定后再跑。

**Blocked by:** 01, 02, 03, 04, 05

**Status:** ready-for-agent

- [ ] 分层字段的覆盖率与缺失处理被记录；不改生产默认消费路径。
- [ ] 相对现有 `net_mf_amount` 有增量贡献；与 `inst_smart_break` 的相关性检查通过。
- [ ] 经 02 门槛；不通过则移除因子或关闭开关，旧数据路径与旧策略保持可运行。
- [ ] 复用既有策略注册与数据转换入口，不新增平行实现；账本记录判决与 `config_hash`。

**Notes:** 分层资金流可能存在字段缺失、口径变更与行业分布偏差，且与既有资金流因子高度相关；本卡不抢在 A/B/C 之前，避免在基线未稳时引入数据质量问题。

## 数据契约（测量前核对，2026-10-24）

- **字段**：`tushare_moneyflow.buy_lg_amount` / `sell_lg_amount` / `buy_elg_amount` /
  `sell_elg_amount` / `net_mf_amount`（单位：万元）。驱动侧已在 `load_data()` 里 JOIN 并派生
  `net_big_amount = buy_lg + buy_elg − sell_lg − sell_elg`（`scripts/backtest_5y_23strategies.py:865-866`），
  本卡**直接复用**该列，不新增 SQL、不新增数据转换入口。
- **覆盖率**（本地 PG `tushare`，逐表 `count(col)/count(*)`）：2020–2026 各年 `buy_lg_amount` /
  `buy_elg_amount` / `net_mf_amount` 非空率均为 **100%**；2026 年截至 **2026-10-16**（188 个交易日，
  992,590 行）。数据快照与卡 A/B/C 相同。
- **缺失处理**：`net_big_amount` 用 `COALESCE(...,0)`，任一分层字段缺失时按 0 参与净额计算；
  若整列不存在（旧缓存/旧 schema），`sig_smart_money_flow()` 返回全 False，不误触发。不做插补、
  不做行业中性化——本卡只做「有没有增量」的检验。
- **生产消费路径**：不改变。因子默认关闭，只有显式 `--enable-smart-money-flow` 才进入策略池。

## 预注册（Card D，2026-10-24 写定；跑完不得更换口径）

| 项 | 冻结值 |
| --- | --- |
| 卡号 / 变体 | `card-d`；主比较一次，`preregistered_primary = true`，`tested_count = 1` |
| 基线臂 | 默认策略池（`STRATEGIES` 23 项，其中 `chan_theory`/`dragon_head`/`box_oscillation` 默认停用 → 有效 20 项）；PIT 宇宙；`entry_timing=next_open`；`min_resonance_families=2`；`size_mode=fixed_1n`；`rank_mode=lead_exp_first`；`max_positions=10`；`max_holding_bars=10` |
| 候选臂 | 基线 + `smart_money_flow`（`sig_smart_money_flow`，`net_big_amount` 连续 3 日 > 0 + 小单净流出 + 站上 MA20 + 阳线），仅由 `--enable-smart-money-flow` 注册 |
| 估计量 | v1 `pooled-per-signal-mean-diff`（`--estimator signal`，默认路径）；块长 `max-arm-p99-bars`；重抽 2000；percentile CI 2.5/97.5；signal-weighted；日期轴 `union-signal-dates` |
| 为什么用 v1 | 本卡改变信号集合，v2 组合层估计量的预注册边界排除此类卡（`docs/backtest-flip-gate-spec.md` §9）。偏离「改信号走 WFO」的授权与边界见 `docs/adr/0004-card-d-signal-gate-without-wfo.md` |
| 主判据 | `ci_excludes_zero`：Δ `net_return` 的 `ci_low > 0` |
| 护栏 | §4 五项，阈值不放宽：`cagr_non_inferior`、`sharpe_non_inferior`、`maxdd_non_inferior`（均 δ=0）、`maxdd_cap ≤ 25.0`、`min_trades n_filled ≥ 100` |
| δ | 预注册 δ=0，不新增非劣性 margin；§4:57-59 已声明 δ=0 是描述性方向护栏 |
| OOS 窗口 | 与卡 C 同口径：`2022-01-17 → 2026-10-16`；门禁显式传 `--oos-end 2026-10-16` |
| 数据快照 | 与卡 A/B/C 相同（PG `max(trade_date)=2026-10-16`）；`--data-hash` 必须等于卡 C 的 `8a5542694c0b1dd10c1de9bb0bd87447f3ec71e48d981b25392f833f6f2d313d`，不等则 ABORT 后重新预注册 |
| 自检 1 | 两臂 `config_hash` 必须都等于 PIT 冻结值 `acb32bd95d3c94c9c575b00008409c483cd04064`。因子是信号池变更、不是 `SimConfig` 字段，因此两臂哈希**预期相同**；不同则说明改到了度量口径，作废 |
| 自检 2 | 两臂 `signals.csv` sha256 必须**不同**；基线不得出现 `strategy=smart_money_flow`，候选必须出现；报告新增行数与漏斗变化 |
| 自检 3 | 门禁 `--estimator signal` 下两臂 OOS 日期轴可正常并集；任一臂 OOS 信号为空即报错，不得补零 |
| 自检 4 | 两臂 `funnel.reconciled == true`；候选臂 `filled` 与基线差异需记录，不得用差异本身作为通过依据 |
| 增量与相关性 | 必须报告：① 因子信号对 `net_mf_amount` 过滤的增量（因子信号中通过 `_moneyflow_ok` 的比例，以及相对于 `inst_smart_break` 的信号重叠 Jaccard）；② 与 `inst_smart_break` 的相关系数。判据先验：Jaccard 过高（>0.5）或相关性过高（|r|>0.5）视为「重复已有信息」，即使门槛 PASS 也不得进入后续采纳票 |
| 变体计数 | 只跑这一种因子定义；不扫 3 日窗口、不改小单条件、不试第二种参数。任何改定义都算新变体，只记录、不作翻转依据 |
| 判决语义 | PASS = 主判据 + 五项护栏全过；否则 FAIL。**两种结果都不在本卡内翻转默认值**：PASS 只支持另开后续票讨论纳入默认池，FAIL 保持默认关闭。任何默认改动都必须等 WFO 落地后重跑 |

### 运行命令（两臂各一次；2026-10-24 为周六，需 `--force`）

```bash
# 基线臂
python scripts/backtest_5y_23strategies.py --portfolio-sim \
  --portfolio-sim-out result/gates/card-d/base \
  --force --no-handoff

# 候选臂
python scripts/backtest_5y_23strategies.py --portfolio-sim \
  --portfolio-sim-out result/gates/card-d/cand \
  --force --no-handoff --enable-smart-money-flow
```

```bash
# 门禁（v1 信号级）
python scripts/portfolio_gate.py \
  --baseline-summary  result/gates/card-d/base/<stem>.json \
  --baseline-signals  result/gates/card-d/base/<stem>_signals.csv \
  --candidate-summary result/gates/card-d/cand/<stem>.json \
  --candidate-signals result/gates/card-d/cand/<stem>_signals.csv \
  --card-id card-d --estimator signal \
  --data-hash 8a5542694c0b1dd10c1de9bb0bd87447f3ec71e48d981b25392f833f6f2d313d \
  --code-commit <实现提交> --oos-end 2026-10-16
```
