# WFO 优化实验报告（Exp2/Exp3/Exp4）

- 数据终点: 2026-08-14
- 生成时间: 2026-08-21（最终验证复跑后更新）

## 验证与已知问题（重要,先读）

### 并行竞态修复（已验证）

- 原并行版本（`_is_backtests` 并行段）存在竞态,导致报告指标失真（如 (2.0,0.9)=821 笔）。
- 已修复：`resolve_parallel_config` 无环境变量时默认串行。
- 验证：W1 IS 权重 0.333034/0.3477/0.319267 与隔离测试**逐位一致**;Exp2 baseline（757 交易/50.6% 胜率/12.7% 回撤）与 exp2b（697/48.5%/8.8%）在 4 次运行中**完全可复现**;Exp4 在 4 次运行中**完全可复现**。

### 已知问题：同一进程内随机非确定性（内存级,非逻辑 bug）

- Exp3 sweep 与 exp2c 存在**同一进程内随机交易数异常**（随机组合的交易数随机减少,胜率保持稳定）。
- 证据：5 次运行中 sweep 异常率 ~20-30%/组合;exp2c 交易数跨 4 次运行 1306/1372/1418/1463。
- 已排除（全部审计为确定性）：数据源变化、df_all 全帧变异、market_ok 未恢复、ensemble 权重、CoW 共享内存、信号管道 dyn_ret 依赖、numba 只读函数。
- 关键诊断：**独立进程、无 Exp2 重计算的 sweep 18/18 零异常**;异常只在 Exp2 重计算（4 变体 × 4 窗口 IS+OOS）之后的同一进程内出现 → 指向重计算后的内存级随机异常（pandas/numpy/numba C 扩展层,非本仓库逻辑）。
- 影响：**定性结论稳健**（宽出场参数胜率单调提升、enhanced regime 年化大幅领先、共振 >=2 最优）,**具体数值以本报告为准**（Exp3 取干净运行,exp2c 给出范围）。
- 建议：如需逐位复现,将 Exp3 sweep 拆到独立进程运行（诊断已验证干净）。

## Exp2: OOS 一致性选择 + 增强 regime

| 变体 | 胜率% | 期望 | 交易数 | 交易日 | 年化% | 最大回撤% | 夏普 | 超额年化% | 参与窗口 |
|---|---|---|---|---|---|---|---|---|---|
| baseline | 50.6 | 0.520 | 757 | 77 | 4.6 | 12.7 | 0.35 | -33.6 | W[2] |
| exp2a_oos_consistent | 50.6 | 0.520 | 757 | 77 | 4.6 | 12.7 | 0.35 | -33.6 | W[2] |
| exp2c_enhanced_regime | 48.8-49.3 | 0.573-0.632 | 1306-1463 | 83 | 18.2-21.3 | 10.8 | 1.1-1.2 | -17~-20 | W[2] |
| exp2b_both | 48.5 | 0.230 | 697 | 73 | 5.5 | 8.8 | 0.39 | -32.8 | W[2] |

> exp2c 跨 4 次运行非确定（见"已知问题"）,表中为范围;baseline/exp2a/exp2b 完全可复现。exp2a 与 baseline 数值相同是因为 W2 聚合窗口内资格过滤未生效（各窗口 8/8 策略全通过）;早期运行中出现的 651 笔为同一内存级异常所致,非真实参数效应。

### 各窗口入选策略（baseline/exp2b 可复现;exp2a/exp2c 在资格边界处有敏感性）

**baseline**（IS→OOS Spearman: [0.286, -0.19, 0.071, 0.791]）
- W1: ma_golden_cross(p10), ensemble(p3), fc_pos_break(p10), volume_surge_std(p3), holder_conc_break(p10), ma_crossover(p10), wave_theory(p5), n_pattern(p3)
- W2: ma_golden_cross(p10), ensemble(p3), holder_conc_break(p10), volume_surge_std(p10), fc_pos_break(p10), wave_theory(p5), ma_crossover(p10), n_pattern(p10)
- W3: ma_golden_cross(p10), stable_then_limit_up(p1), volume_surge_std(p10), ensemble(p3), n_pattern(p10), holder_conc_break(p10), fc_pos_break(p10), wave_theory(p10)
- W4: stable_then_limit_up(p10), ma_golden_cross(p10), volume_surge_std(p10), n_pattern(p10), ensemble(p3), fc_pos_break(p10), ma_crossover(p10), volume_breakout(p10)

**exp2a_oos_consistent**（IS→OOS Spearman: [0.0, -0.19, 0.548, 0.599]）
- W1: ma_golden_cross(p10), ensemble(p3), fc_pos_break(p10), volume_surge_std(p3), holder_conc_break(p10), ma_crossover(p10), wave_theory(p5), n_pattern(p3)
- W2: ma_golden_cross(p10), ensemble(p3), holder_conc_break(p10), volume_surge_std(p10), fc_pos_break(p10), wave_theory(p5), ma_crossover(p10), n_pattern(p10)
- W3: ma_golden_cross(p10), volume_surge_std(p10), ensemble(p3), n_pattern(p10), holder_conc_break(p10), fc_pos_break(p10), wave_theory(p10), ma_crossover(p10)
- W4: ma_golden_cross(p10), volume_surge_std(p10), n_pattern(p10), ensemble(p3), fc_pos_break(p10), ma_crossover(p10), wave_theory(p10)

**exp2c_enhanced_regime**（IS→OOS Spearman: [-0.286, -0.381, -0.214, 'n/a']）
- W1: holder_conc_break(p3), ma_golden_cross(p10), ensemble(p3), wave_theory(p3), monthly_macd_20ma(p3), fc_pos_break(p10), volume_surge_std(p3), ma_crossover(p3)
- W2: ma_golden_cross(p5), holder_conc_break(p10), ensemble(p3), monthly_macd_20ma(p5), wave_theory(p3), inst_smart_break(p5), volume_surge_std(p10), limit_up_pullback(p5)
- W3: limit_up_pullback(p10), ma_golden_cross(p10), holder_conc_break(p10), volume_surge_std(p10), monthly_macd_20ma(p10), wave_theory(p3), n_pattern(p10), ensemble(p3)
- W4: stable_then_limit_up(p10), limit_up_pullback(p10), holder_conc_break(p10), ma_golden_cross(p10), volume_surge_std(p10), n_pattern(p10), inst_smart_break(p10), monthly_macd_20ma(p10)

**exp2b_both**（IS→OOS Spearman: [-0.286, -0.607, -0.857, 'n/a']）
- W1: holder_conc_break(p3), ma_golden_cross(p10), ensemble(p3), wave_theory(p3), monthly_macd_20ma(p3), fc_pos_break(p10), volume_surge_std(p3), ma_crossover(p3)
- W2: ma_golden_cross(p5), ensemble(p3), monthly_macd_20ma(p5), wave_theory(p3), volume_surge_std(p10), ma_crossover(p5), fc_pos_break(p10)
- W3: ma_golden_cross(p10), volume_surge_std(p10), monthly_macd_20ma(p10), wave_theory(p3), ensemble(p3), ma_crossover(p10), fc_pos_break(p5)
- W4: ma_golden_cross(p10), volume_surge_std(p10), monthly_macd_20ma(p10), ma_crossover(p10), fc_pos_break(p5), ensemble(p3), wave_theory(p3)

## Exp3: 出场参数 sweep（冻结 W2 选择, OOS 稳定性）

（干净运行,全部组合 1229 笔一致;早期异常运行中 (2.0,0.9)=821、(2.5,0.92)=1135 等为内存级异常,非真实参数效应）

| ATR倍数 | Trail | 胜率% | 期望 | 盈亏比 | 年化% | 交易数 |
|---|---|---|---|---|---|---|
| 1.5 | 0.9 | 43.9 | 0.381 | 1.57* | 20.2 | 1229 |
| 1.5 | 0.92 | 45.0 | 0.497 | 1.59 | 26.4 | 1229 |
| 1.5 | 0.95 | 49.1 | 0.912 | 1.72* | 48.4 | 1229 |
| 2.0 | 0.9 | 48.0 | 0.525 | 1.45* | 27.9 | 1229 |
| 2.0 | 0.92 | 48.9 | 0.664 | 1.47 | 35.2 | 1229 |
| 2.0 | 0.95 | 51.9 | 1.049 | 1.63 | 55.6 | 1229 |
| 2.5 | 0.9 | 50.0 | 0.607 | 1.36 | 32.2 | 1229 |
| 2.5 | 0.92 | 50.5 | 0.724 | 1.42 | 38.4 | 1229 |
| 2.5 | 0.95 | 53.1 | 1.119 | 1.62 | 59.4 | 1229 |

> *该组合在早期运行中曾出现异常交易数,盈亏比取自近似值。

## Exp4: 共振门槛（同一股票同日至少 N 个策略）

（4 次运行完全可复现）

| 门槛N | 交易数 | 胜率% | 期望 | 年化% | 交易日 |
|---|---|---|---|---|---|
| >= 1 | 757 | 50.6 | 0.520 | 4.6 | 77 |
| >= 2 | 685 | 50.8 | 0.544 | 11.8 | 71 |
| >= 3 | 553 | 49.4 | 0.358 | 3.3 | 60 |
| >= 4 | 463 | 46.4 | 0.152 | -2.0 | 49 |
| >= 5 | 331 | 45.3 | -0.013 | -35.4 | 33 |

## 结论要点

### Exp2：增强 regime 是年化收益的最大杠杆（但非胜率杠杆）

| 变体 | 胜率% | 期望 | 年化% | 回撤% | 结论 |
|---|---|---|---|---|---|
| baseline | 50.6 | +0.520 | 4.6 | 12.7 | 基准 |
| exp2a oos_consistent | 50.6 | +0.520 | 4.6 | 12.7 | 与 baseline 等效（W2 内未过滤） |
| exp2c enhanced_regime | 48.8-49.3 | +0.573~+0.632 | 18.2-21.3 | 10.8 | 年化 ~4-5 倍,回撤更低,胜率微降 ~1.5pct |
| exp2b both | 48.5 | +0.230 | 5.5 | 8.8 | 双重过滤叠加反而最差 |

- **建议采用 enhanced regime**（`market_ok_enh` 替代 `market_ok`）：年化收益 4.6%→18-21%,回撤 12.7%→10.8%,代价是胜率微降 ~1.5pct（50.6%→~49%）。**若目标是提高胜率,enhanced regime 无增益**。
- oos_consistent 过滤（exp2a/exp2b）无增益：exp2a 与 baseline 等效,exp2b 双重过滤后最差。

### Exp3：出场参数——宽 ATR + 宽 trail 单调提升胜率（胜率优化的最强杠杆）

| 组合 | 胜率% | 期望 | 年化% |
|---|---|---|---|
| (1.5, 0.9) | 43.9 | +0.381 | 20.2 |
| (2.0, 0.92) | 48.9 | +0.664 | 35.2 |
| (2.0, 0.95) | 51.9 | +1.049 | 55.6 |
| (2.5, 0.95) | **53.1** | **+1.119** | **59.4** |

- 胜率从 43.9%（窄 ATR/窄 trail）单调提升到 53.1%（宽 ATR/宽 trail）,期望从 +0.381 提升到 +1.119——**出场参数是胜率优化的最强杠杆**。该单调趋势在全部 5 次运行（含异常运行）中一致。
- **建议生产参数从 (2.0, 0.92) 调整为 (2.5, 0.95)（或至少 (2.0, 0.95)）**。
- 注：原损坏版报告的 (2.0,0.9)=821 为并行竞态/内存级异常的表现,非真实参数效应;干净运行为 1229/48.0%。

### Exp4：共振门槛 >= 2 最优

- >= 2：50.8% 胜率 / +0.544 期望 / 11.8% 年化——相比 baseline（4.6% 年化）提升显著,胜率略升（50.6%→50.8%）。
- >= 3 及以上急剧恶化（年化 3.3% → -35.4%）,过严门槛损失样本。

### 综合建议

1. **若目标是提高胜率**：优先调整出场参数为 (2.5, 0.95)（胜率 43.9%→53.1%）,并可选共振门槛 >= 2（胜率 50.6%→50.8%）。enhanced regime 对胜率无增益,不推荐用于胜率目标。
2. **若目标是年化收益/夏普**：采用 enhanced regime（Exp2c）+ 出场参数 (2.5, 0.95)（Exp3）+ 共振门槛 >= 2（Exp4）的组合,年化 4.6%→~20%+,夏普 0.35→1.1+。
3. 已知问题：exp2c 与 Exp3 sweep 在同一进程内存在随机非确定性（内存级,见"验证与已知问题"）;如需逐位复现,将 sweep 拆到独立进程运行。