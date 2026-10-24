# 翻转门槛规格 v1：池化样本外 Δ 的日期分块 bootstrap CI

> 适用对象：实验卡 A/B/C 这类"参数或资金配置"变更。改信号或改出场定义的改动仍走完整 WFO。
> 唯一例外：卡 D（信号变更）经用户显式授权使用 v1 作一次性预注册比较，边界与后果见
> `docs/adr/0004-card-d-signal-gate-without-wfo.md`；该例外不推广到其他改信号／出场的卡。
> 本文件是 ticket 02（`docs/specs/backtest_度量层重建/issues/02-bootstrap-flip-gate.md`）的实现契约与预注册记录。
> 实现落点：`scripts/portfolio_gate.py`。决策背景见 `docs/adr/0003-*.md`。

## 1. 观测单元与输入

- 观测单元：`(strategy, signal_date, code)`；每条记录携带 `net_return` 与 `holding_bars`。
- 输入：
  - `result/portfolio_sim_<start>_<end>_signals.csv`——去簇后的信号表，列 = `_CAND_COLUMNS`
    （见 `scripts/portfolio_sim.py:102-127`）；
  - 同名 `..._.json`——`metrics.oos_start`、`config.explicit_cost_roundtrip`、`config_hash`、`window`。
- 净收益：`net_return = realized_ret - slip_entry - slip_exit - explicit_cost_roundtrip`。
  `realized_ret` 是 S1 的 `dyn_ret_{p}d_no`（`scripts/portfolio_sim.py:654`）**毛**路径收益，
  口径与成交行 `net_ret = gross_ret - slip_entry - slip_exit - explicit_cost_roundtrip`
  （`:369-371`）一致。成本从运行 `config` 读取，**不硬编码** `0.0035`。
- `holding_bars = exit_offset`（K 线根数）。**不得**使用成交表的 `holding_days`
  （那是日历日，`:379`）。
- 样本外过滤：`signal_date >= metrics.oos_start`（由 `compute_metrics` 产出）。
- 去重：`(strategy, signal_date, code)` 唯一；必需字段出现 NaN 直接报错，不静默丢弃。

## 2. Δ 估计量

Δ = 候选臂 OOS 信号的 `net_return` 均值 − 基线臂 OOS 信号的 `net_return` 均值。

- 各臂使用**自己**的信号集，不做交集限制——交集会丢掉"候选多出来的信号"，而那正是被检验的处理。
- 不做逐日配对差：基线在"仅候选有信号"的日期上是空的，会产生未定义或需补零的伪配对。
- 不用组合级比值（CAGR 之差等）：那是护栏端点，不是本 CI 的目标。
- 估量语义：**策略级每信号均值对比**，不是同一信号的配对处理效应。这一点必须写进判决产物。

## 3. 分块 bootstrap

- 日期轴 `T` = 两臂 OOS 信号日期的并集，`n_dates = |T|`。
- 块长 `L = max(ceil(p99_b), ceil(p99_c))`，其中 p99 取各臂 `holding_bars` 的 0.99 分位，
  `L >= 1`，**不设上限**。取两臂最大，因为块必须覆盖更长的依赖窗口。
- 非环绕移动块：`k = ceil(n_dates / L)`；每次重抽在 `1..(n_dates - L + 1)` 均匀取 `k` 个起点；
  按 `T[s : s+L-1]` 取块、拼接、截断到 `n_dates`。日期可跨块重复，这是机制本身。
- 同一次重抽的日期索引**同时**作用于两臂（联合重抽，保留跨臂相关）。不得两臂独立重抽。
- 池化按**信号笔数加权**（即每信号均值估计量），不是等日期加权。
- 重抽次数 2000；CI = Δ 分布的 2.5 / 97.5 百分位。同时报告 bootstrap 标准误与 `n_blocks`。
- 任一脚本外信号为空、或 `n_dates < L`，直接报错。若 Δ 分布方差为零或无有效重抽，
  置 `degenerate=True` 并照样返回（不静默通过）。

## 4. 判据

| id | 类型 | 判据 |
| --- | --- | --- |
| `ci_excludes_zero` | superiority | `ci_low > 0` |
| `cagr_non_inferior` | guardrail (δ=0) | `cagr_cand >= cagr_base` |
| `sharpe_non_inferior` | guardrail (δ=0) | `sharpe_cand >= sharpe_base` |
| `maxdd_non_inferior` | guardrail (δ=0) | `maxdd_cand <= maxdd_base` |
| `maxdd_cap` | absolute | `maxdd_cand <= 25.0` |
| `min_trades` | absolute | `n_filled_cand >= 100` |

- `ci_excludes_zero` 用双侧 95% CI 下界 > 0，等价于单侧优效性检验在 2.5% 水平，偏保守。
- **三个"不劣于"的 δ=0 是描述性方向护栏，不是形式化非劣性检验。** 形式化非劣性需要预先设定的
  margin δ>0 与单侧检验；本规格不发明 margin，改以 `type: "guardrail"` 标注，并在 `limitations`
  中明示。要让"不劣于"具备统计含义，须在**跑之前**预注册 δ。
- 组合护栏（CAGR/Sharpe/maxDD）用点估计，无不确定性量化；其抽样噪声可能造成误拒（掉功率），
  但不会抬高通过率——通过必须先过 `ci_excludes_zero`，故整体假阳性上界是该主判据的 α。
- `maxdd_cap` 与 `min_trades` 为确定性绝对门槛。不得为了通过而放宽。

## 5. 与《指标契约》§8.1 的偏离（预注册）

§8.1 的放行规则是 `CI 排除 0` **且** `≥5/8 折同号`。本门槛：

- 不建 WFO，无折，故"≥5/8 折同号"在本范围**不适用**；
- 依据 `docs/backtest-experiment-cards.md` §4.5：完整 WFO 只保留给"改信号或改出场定义"的改动，
  卡 A/B/C 属参数或资金配置变更，走本 bootstrap 门槛；
- 仓库当前无 WFO 引擎；
- 替代物：日期分块 bootstrap CI（本文件 §3）。

该偏离记录在四处：本文件、§7 预注册记录、判决产物的 `wfo` 字段、PR 描述与 `docs/CHANGELOG.md`。

可选（非门禁用）：把 OOS 等分为 8 段，报告 Δ 符号一致的段数，标注为"时间符号稳定性，非 WFO 折"。
切分方式须预先注册，且**不得**用于放行。

## 6. 随机性与可复现

- **无 `--seed` 入口。**
  `seed = int.from_bytes(sha256(gate_version|card_id|baseline_config_hash|candidate_config_hash|data_hash|oos_start|oos_end|L|resamples|estimator_id).digest()[:8], "big")`。
- `np.random.Generator(np.random.PCG64(seed))`；产物记录派生 seed、派生输入、RNG 名称与 numpy 版本。
- 确定性还要求：按 `(signal_date, code, strategy)` 稳定排序；不依赖 set/dict 迭代顺序；
  不做并行规约；bootstrap 用 numpy 计算，绕开 pandas rolling / numexpr 路径。
- `verdict_hash` = 去掉易变时间戳后的规范 JSON 的 sha256。

## 7. 预注册记录（跑之前写定）

- 卡号：card-a（`min_resonance_families` 2 → 1）；主比较**一次**，`preregistered_primary = true`。
- gate_version：v1；估计量 `pooled-per-signal-mean-diff`；块长规则 `max-arm-p99-bars`；
  重抽 2000；CI `percentile`；加权 `signal-weighted`；日期轴 `union-signal-dates`；判据见 §4；margin 0（描述性）。
- 变体计数：跑之前 = 1。任何对 gate 规格 / 估计量 / 块长 / 加权 / OOS 窗口 / 数据快照 / 配置哈希的
  改动都算**新变体**，必须追加到 `result/gates/registry.jsonl`；非预注册主比较的判决为探索性，
  **不得**用于翻转默认值。
- 跨卡多重性：四张卡各自独立预注册主比较，但卡间族错误率未受控（全零假设下 4 次 5% 检验
  ≈ 18.5%）。是否按"族"处理（如 Bonferroni）须在跑卡前声明；本门槛默认按独立决策，
  并在产物 `warnings` 中提示族错误率。

## 8. 已知局限

- 每信号净收益是**信号级路径收益**（`realized_ret` 减同样的成本项），不是实际成交收益。
- `L = h_max` 只覆盖持有窗口内的重叠，不覆盖更长的体制依赖。
- 有效独立块 ≈ `n_dates / L`，不是 `n_dates`；报告中不得把 `n_dates` 说成独立样本量。
- 池化样本若已被别的选择环节筛过（如 `collapse_candidates` 按 `rank_score` 取胜者），口径会偏；
  本门槛读**去簇**表，正是为避开这道额外筛选。
- 若候选配置本身是在同一段 OOS 上挑出来的，CI 无效（选择偏倚）；预注册与变体计数只能缓解、不能消除。
- 2000 次重抽的百分位分位数有蒙特卡洛误差；固定 seed 保证可复现。

## 9. v2 组合层估计量（ticket 08，预注册；未用于任何既有判决）

> 背景：卡 B 的两臂 `signals.csv` 逐字节相同，v1 信号级估计量的 Δ 在构造上恒为 0（`degenerate=true`），无法裁决"只改资金规模"的卡（见账本 §10.8）。本节预注册一个组合层估计量。**默认估计量仍是 v1**；card-a / card-b 的判决不因本节改变，也不得用新估计量重跑翻案。

| 项 | 冻结值 |
| --- | --- |
| 估计量 id | `oos_daily_portfolio_cagr_block_delta` |
| 观测单元 | OOS 交易日（两臂 `_equity.csv` 的 `equity` 列） |
| 日收益 | `ret_t = equity_t / equity_{t-1} − 1`，先在全期序列计算，再按 `oos_start..oos_end` 切片 |
| 点估计 | Δ = `CAGR_cand − CAGR_base`（百分点），`CAGR = (∏(1+ret_t))^(252/n) − 1` |
| 日期对齐 | 两臂 OOS 日期集合必须完全一致；不一致报错，不取交集、不补零 |
| 块长 | `L = max(21, ceil(p99 holding_bars of each arm))`；21 为一个交易月下限 |
| 重抽 | 非环绕移动块，`k = ceil(n_dates/L)`，起点均匀取自 `1..n_dates−L+1`，同一索引联合作用于两臂，2000 次 |
| CI | percentile 2.5 / 97.5 |
| 主判据 | `ci_excludes_zero` 读取 ΔCAGR 的 `ci_low > 0`；其余 5 项判据与阈值同 §4，不新增、不放宽 |
| 种子 | 沿用 §6 派生式，`estimator_id` 与块长参与 |
| 退化 | 有效重抽 < 1 或 Δ 分布极差为 0 → `degenerate=true`，CI 不得用于通过 |
| 变体计数 | 按 `card_id` 跨估计量合并；同一张卡切换估计量再跑，`tested_count` 必须 +1 |

- **适用边界**：仅用于"信号集合相同、资金或配置不同"的卡。改信号或出场定义的卡仍走 v1 或完整 WFO。
- **禁止**：用 v2 重跑 card-a / card-b 并据此翻转默认值。任何此类运行都是探索性变体，只能记录，不能作为翻转依据。
- **卡 C 的约束**：票 05 若采用 v2，必须在开跑前把估计量写进其预注册；不得先跑 v1 看结果再换 v2。
- **局限**：日收益块 bootstrap 假设块内平稳，消除不了选择偏倚；容量仍无独立指标，需在 PR 描述中单独说明。
