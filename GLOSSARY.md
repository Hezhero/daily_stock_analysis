# 术语表（GLOSSARY）

本文件是本仓库的**单上下文术语表**（single-context glossary），按 `docs/agents/domain.md`
的约定放在仓库根目录，用于统一回测度量层、组合模拟器与实验登记讨论中的词汇。讨论、issue、
测试命名和文档都应使用这里定义的术语；如果需要的概念不在表中，先确认它是不是项目真实使用的
语言，再决定是否补充。术语的具体实现语义以 `scripts/portfolio_sim.py`、
`scripts/backtest_5y_23strategies.py` 与 `result/指标契约_metric_contract_v1_20261017.md` 为准，
本表只负责把词汇说清楚。

## 组合级

以**单一账户、有限资金、逐日盯市**得到的净值曲线为观测对象的口径。它与信号级口径相对：后者把
每条策略 sleeve 当成独立、满仓、可各自取最优持有期，再做无权重平均。同一个策略集合在两种口径下
可以给出完全不同的头条数字（信号级平均年化 28.7% 对组合级 OOS CAGR 3.88%），所以两者不可混用。

## 资金约束

组合模拟器 `simulate()` 中只维护一个真实现金账户，持仓数不超过 `SimConfig.max_positions`
（当前 ≤10），每个入场只能用**当日开盘前**的可用现金，且当天退出回款不能回头为当天开盘成交提供
资金。它是判断一套信号"实物上能否照此下单"的核心约束，也是组合级口径与信号级口径分岔的主要来源。

## 样本外 CAGR

在预热 `SimConfig.warmup_days`（252 个交易日）之后到期末的完整净值曲线上，按整个 OOS 历日跨度
年化的复合增长率。它是当前唯一主指标；胜率、单笔期望等只作诊断，不得单独作为通过条件。契约同时
要求 maxDD 与最小成交笔数门槛，并报告 bootstrap 置信区间。

## 去簇共振

同一信号日、同一只股票上，来自**不同家族**的非排除策略同时命中，按去重后的家族数
`n_families` 计数。实现入口是 `apply_declustered_resonance()`：先剔除 `SimConfig.vote_exclude`
中的策略，再对 `(signal_date, code, family)` 去重后 `nunique()`，只有
`n_families >= SimConfig.min_resonance_families` 的候选才保留。

## 家族

`portfolio_sim.STRATEGY_FAMILY` 把 24 条策略归入 `trend` / `reversal` / `limitup` /
`fundamental` 四类（其中 `chan_theory` / `dragon_head` / `box_oscillation` 已默认停用，
实际运行 21 条），用于共振计数与持仓簇上限。当前 `SimConfig.family_cap` 只限制
`trend <= 3`，其余家族不设上限；家族的划分属于度量层冻结项，实验不能顺手改。

## 家族去重

同一信号日、同一股票上，同一家族的多条策略**只算一次**，避免同源信号被重复计成"共振"。
`apply_declustered_resonance()` 先按 `(signal_date, code, family)` 去重，再统计不同家族数，
因此 3 条趋势策略命中与 1 条趋势策略命中的共振贡献相同。它解决的是"高度相关策略堆叠"造成的
虚假共振，而不是策略数量本身。

## 领先期望

`apply_lead_expectation()` 为每个候选附加的 `lead_exp`：取同一策略在信号日 `t` 之前**已经平仓**
（`exit_date < t`）的候选的平均 `realized_ret`。它要求至少 `SimConfig.expectation_min_trades`
（30）笔历史才有效，用 `np.searchsorted` 保证严格因果，因此是排序用的**领先**期望而不是事后期望。

## 盯市(MTM)

每个交易日收盘按持仓市值重估账户，`equity = cash + position_value`，把持仓内的浮盈亏路径计入
净值、回撤与夏普。`simulate()` 在每日 C 段生成权益行，空仓日也记一条记录；这与"只在平仓时记一笔
收益、再按持有期均匀分摊"的旧做法相对，后者看不到持仓内回撤，会低估方差并抬高夏普。

## PIT 宇宙

point-in-time 股票池：按 `delist_date` 等时点信息构造，使窗口内**曾经上市**以及**后续退市**的
股票在其可交易期间进入样本。它相对当前 `list_status='L'` 的静态池，后者会把窗口内退市的股票整体
剔除，从而系统性抬高反转、涨停、小市值类策略的绩效。PIT 落地属于 T0.4，报告需并列给出 PIT 与
`L-only` 两个池的结果。

## 口径

一组明确的测量约定，包括股票池、成本档、成交价假设、是否样本外、是否逐日盯市等。同一个数字在
不同口径下不可直接比较，所以任何验收报告都必须并列写明池口径（PIT / `L-only`）、成本档、
成交价假设与是否 OOS。"先冻结口径，再谈提升"说的就是这件事。

## bootstrap CI

对池化样本外**每信号 Δ** 按日期做 moving-block 重采样得到的置信区间，block 长度取实际持有期的
99 分位，重采样 2000 次，有效样本是 `n_dates` 而不是信号笔数。当前放行规则是：95% 的分块置信区间
排除 0，且至少 5/8 折同号，才允许翻转默认值；它替代了"必须先建出完整 WFO"的不可构建门槛。

## 实验卡

一张显式登记的待验证改动卡，必须写明改动内容、判据、证据门槛与回滚方式，避免实验被悄悄混进
默认值。当前登记表包括卡 A（`min_resonance_families = 1`，消融已完成、待 bootstrap 门槛）、
卡 B（资金部署感知仓位）、卡 C（并发信号选股 / 持仓上限排序）与低优先级的卡 D（moneyflow 因子）。

## 度量契约

`result/指标契约_metric_contract_v1_20261017.md` 定义的测量层规格。它冻结的是**度量口径**
（0.35% 往返成本、T+1 盯市、PIT 宇宙、去簇共振定义、`next_open` 入场），不是可部署参数；
`min_resonance_families` 已从"冻结常量"降级为登记实验参数。修订契约语义须同步更新守卫与文档。

## config_hash

`portfolio_sim._config_hash()` 对 `SimConfig` 全字段做 `sort_keys` 的 JSON 序列化后取 SHA-1，
写入模拟产物 `meta`。它是可复现性的指纹：任何实验参数改动都必须改变 `config_hash` 并留下侧边
产物，使"已登记的重跑"与"静默调参"可以被区分。当前已发布基线的哈希为
`5eea05195af3d89432af8b2a049fde2517d6a53b`。
