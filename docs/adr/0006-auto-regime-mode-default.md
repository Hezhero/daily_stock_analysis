# 0006. 市场环境口径默认改为逐日自动判断

- 状态: 已接受（2026-11-12，用户显式拍板）；已实现（2026-11-12，集成分支 `auto-regime-mode-integration`）

## 背景

`scripts/backtest_5y_23strategies.py` 的可开仓判定长期由 `--regime-mode {enh2,enh,strict}`
三选一手动指定，默认 `enh2`（`docs/CHANGELOG.md` 的 2026-09-09 条目是唯一明文，
引入 commit `a1e0b99d` 标题不含口径决策语义）。**没有任何 ADR、spec 或实验报告记录
「为什么 `enh2` 是默认」**、0.45 / 0.85 阈值的来源，或三档的 A/B 对照数据；`_REGIME_MODE`
既不在 `_METRIC_PROFILE_*` 冻结画像，也不在 `_REGISTERED_EXPERIMENT_DEFAULTS` 登记清单，
更不进 `config_hash` —— 口径选择长期处于治理空白。

2026-09-19 的五份优化建议已提出过几乎同型的方案：动态广度阈值与按波动率分层
在 `enh` / `enh2` 间切换（`优化建议_doubao2.1t_20260919.md`）、`market_ok_grade` 三态与
`--regime-mode enh2_soft`（`优化建议_minimaxM3_20260919.md`）。综合对比以 **X5「双向风险」**
把「全面放宽市场过滤器」列为不做，改选 `C2` 防守族半仓（已实现为显式 opt-in 的
`--defensive-half-pos`）。此后无 commit、无实跑。

日志侧，`market_ok_enh2` 在 2026-10-29 起连续 0/5 天可开仓、handoff 0 只股票、按策略纪律
跳过 `main.py`（`docs/backtest-runbook.md` §4 定义为正常观测）。但 `enh 5/5` 满可开仓仍可能
0 只（`logs/backtest_5y_20260824.log`），说明零推荐有市况闸门与信号稀疏两条独立成因，
换口径不能单独解决。

## 决策

把 `--regime-mode` 的默认值从 `enh2` 改为 `auto`：不传参即逐日自动判断，显式传
`{enh2,enh,strict}` 即为覆盖并在启动日志标注。目标仅是**免去人工传参**，不承诺减少
0 推荐、不承诺提高收益；`0 只股票`仍按 `docs/backtest-runbook.md` §4 属正常观测。

- **粒度**：逐日、只用截至当日 as-of 的市况列值（`docs/wfo-backtest-design.md` §10：
  「预计算无泄漏」）。不用「每轮按最近 N 日选一次」——`docs/backtest-optimization-design.md`
  已记录同型前视修复（「最新交易日近 window 日胜率套用整条序列」）。
- **规则**：保守门控。默认与冲突回退均为 `enh2`；仅在 `regime=bull` 且 `breadth≥0.45`
  且 `vol_pct≤0.85` 时放宽到 `enh`；`bear`、高波动、数据缺失或判定冲突一律保持 `enh2`
  并对缺失打 WARNING。`strict` 永不自动选中。
- **词表**：只复用回测自带的 `bull/range/bear` + `breadth` + `vol_pct`，不引入实盘链路的
  `trending_up/…`、`强势/偏暖/震荡/偏弱`、`进攻/均衡/防守` 三套词表（无代码级映射）。
- **与软加权正交**：`BACKTEST_REGIME_SOFT=1` / `--regime-soft` 仍在同一轮运行内按市况调整
  策略信号，不被取代、不改动；`--defensive-half-pos` 保持显式 opt-in，不与自动选档叠加。
- **治理归类**：`compute_market_ok` 三列算法（定义）冻结；`auto` 选择器规则版本、阈值与
  回退行为作为**已登记实验参数**（对齐 `result/指标契约_metric_contract_v1_20261017.md` §2.1
  「定义冻结、阈值登记」的示范）。规则版本 `auto_v1` 单值计入 `config_hash`；逐日选档结果是
  数据的确定性函数，不单独入哈希。
- **观测前置**：逐日市况表须落盘，启动日志须能表达口径序列与显式覆盖；否则
  `docs/backtest-runbook.md` §4「先对齐 config_hash 再比 CAGR」与 §5 排障清单会失效。

## 后果

- 这是**翻转默认值**，与 ADR 0004「判决不翻转任何默认值」、
  `result/指标契约_metric_contract_v1_20261017.md` §「预注册放行规则」（95% date-block CI
  排除 0 且 ≥5/8 折同号）及 X5「不做」相冲突；用户已显式豁免上述门槛，并以本 ADR 记录。
- `enh2` 退化为显式覆盖值。冻结 PIT 基线、`docs/backtest-runbook.md` §4 的 10-19 基线、
  10 月四张实验卡、2026-10-29 起的全部 0/5 日志，均基于「默认 `enh2`」口径；此后复现这些
  结果须显式传 `--regime-mode enh2`，并在指标契约加修订行、重算登记 `config_hash`。
- 本机每日回测（仓库外调度，仅由 `logs/` 与 `reports/` 产物推断）不传参，口径语义随之改变；
  上一轮「11 月零推荐」的归因结论须加「基于 enh2 默认」限定才成立。
- 既有 `--regime-mode` / `--strict` 保留为逃生舱；`auto` 只能是第 4 个取值，不得引入
  「只能开不能关」式 env 开关叠加（依 `AGENTS.md` 稳定性护栏）。
- 与 X5 的区别须写清：本决策不做「全市场软化」，只在**已满足严条件的 bull 日**放宽到 `enh`；
  防守族半仓的保守路径仍保留为显式 opt-in。若 A/B 显示弱市净贡献为负，回滚为把默认值
  改回 `enh2` 并作废本 ADR。

## 实现记录（2026-11-12）

- CLI 默认翻为 `auto`（第 4 个取值，非新开关）；显式 `enh2` / `enh` / `strict` 覆盖并
  在启动日志标注「覆盖(档位)」；`--strict` 逃生舱优先级不变。
- 主 seam = `select_auto_regime_mode()` 逐日解析统一入口，产出 `auto_mode` 与
  `market_ok_auto`（取冻结列值、不重算定义）；`_regime_col("auto")` 指向该列，
  `_entry_mask` / 验证统计 / handoff / 打印无需另开分支。
- 观测 seam = `result/daily_regime.csv`（既有 result/ 出口，含逐日七列 + auto 序列列）与
  启动日志「规则版本 / 覆盖标记 / 逐日选档序列」。
- 治理：`regime_selector` 登记为已登记实验参数（默认值 `enh2` 与冻结基线一致）；运行
  `auto` 时驱动显式注入规则版本 `auto_v1`（≠ 默认值 → 经 `_HASH_OPTIONAL_KEYS` 弹出后
  仍留在 `config_hash`），显式 `--regime-mode enh2` 的冻结基线哈希逐位不变。
  采用「保持已发布哈希 + 运行时注入」而非重算基线，以同时满足默认翻转可察觉与历史
  结果逐位可复现（详见 `result/指标契约_metric_contract_v1_20261017.md` §2.1）。
- 待验证项（A/B 实验卡，票 03）：同一天四口径对照、`0/5` 改善幅度、CAGR/maxDD/胜率与
  过拟合风险；本实现不承诺减少 0 推荐或提高收益，回滚方式见「后果」段。
