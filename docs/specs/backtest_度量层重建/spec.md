# Spec: 回测度量层重建 + 实验登记（资金约束 / 去簇共振）

> 状态：**已定稿**（Round 1 + Round 2 全部锁定，本地入库）。产物保存在本地文件
> `docs/specs/backtest_度量层重建/spec.md`，纳入 git 版本管理；**不发布到任何外部追踪器**
> （本机未安装、且明确不安装 `gh`）。本地 markdown 追踪约定见 `docs/agents/issue-tracker.md`。
> 来源：`/grill-with-docs`（grilling + domain-modeling）会话。Round 1 锁定（组合级 / 生产导向 /
> 风险调整裁判 / 本轮交付诊断+方案+文档 / 参数与结构性改动都允许）；Round 2 锁定见
> `Implementation Decisions` 与文末「决策记录」。

## Problem Statement

这套 5 年多策略回测的头条数字（信号级：平均胜率 55.6% / 平均总收益 322% / 平均年化 28.7% /
平均夏普 4.68）衡量的是一个"信号级、无限资金、逐策略取最优持有期"的系统，实物上无法照此下单；
受资金约束的真实组合只有约 **3.9% CAGR / 43.7% 胜率 / 夏普 0.42**，且 **68–78% 资金长期闲置**。

更糟的是，当前唯一被冻结为"权威度量层"的《指标契约》§2.1 把 `min_resonance_families=2`
锁死（`_portfolio_sim_config()` 非 2 即 `RuntimeError`），而同一批工作当日给出的消融证明
**≥1 家族在四项指标上全面更优**：

| 口径 | CAGR | 胜率 | maxDD | 闲置现金 |
|---|---|---|---|---|
| ≥2 家族（当前冻结默认） | 3.88% | 43.67% | 13.11% | 77.7% |
| ≥1 家族（消融） | 8.19% | 45.23% | 10.48% | 67.8% |

于是：优化决策建立在一套"要么测的不是真正在跑的系统、要么自相矛盾"的证据之上。与此同时，
出场侧已被多轮 A/B 反复证伪/饱和（见 `docs/backtest-optimization-design.md` §9），真正未验证的
高先验杠杆——**资金约束下的并发信号选股与仓位部署**——还没被测。

## Solution

先收敛度量与计划，再动手：

1. 把"度量契约"改为**只冻结度量口径**（成本 / 盯市 / PIT / 去簇共振定义 / 入场口径），
   不再锁死可部署参数。
2. 把 `min_resonance_families` 降级为一张"**已登记待验证**"的实验卡，用**可构建**的证据
   （池化样本外每信号 Δ 的日期分块 bootstrap，风险调整后不劣于基线）当翻转门槛。
3. 新增"**资金部署感知仓位**"实验卡，直击 68–78% 闲置现金的根因（每笔固定 1/10 仓位规则）。
4. 把"新因子（`tushare_moneyflow` 分层主力资金）"登记为低优先级实验卡。
5. 产出：诊断报告、方案（含实验卡登记表）、`GLOSSARY.md` 与首批 ADR。

**本阶段不修改回测代码**（交付 = 诊断 + 方案 + 文档）。

## User Stories

1. 作为研究者，我要一个**资金约束下的样本外 CAGR（风险调整后）**作为唯一裁判，以便不被信号级放大数字误导。
2. 作为研究者，我要**胜率降为诊断项**，以便不为刷胜率而牺牲收益。
3. 作为研究者，我要度量契约**只冻结度量口径**，以便可部署参数不被误当成度量基准。
4. 作为研究者，我要 `min_resonance_families` 被当作**已登记实验参数**，以便它的 ≥2 默认值不再不可触碰。
5. 作为研究者，我要一个**可构建的证据门槛**（池化样本外 Δ 的日期分块 bootstrap），以便翻转默认值不被"建不出来的 WFO"无限阻塞。
6. 作为研究者，我要 **≥1 家族共振**实验被登记（消融已跑完），以便已知最强杠杆离采纳只差一道门槛。
7. 作为研究者，我要一张**资金部署 / 仓位**实验卡，以便 68–78% 闲置现金的根因被直接攻击。
8. 作为研究者，我要仓位实验**复用既有 `_TARGET_VOL_ANNUAL` / `_MAX_LEVERAGE` 配置**，以便不新增平行实现。
9. 作为研究者，我要 **moneyflow / 因子卡**登记为低优先级，以便未做也未证伪的因子不被悄然漏掉。
10. 作为研究者，我要每张实验卡**声明判据与回滚方式**，以便实验可被无歧义评审。
11. 作为研究者，我要**诊断报告**讲清两套互相打架的记分板（信号级 55.6%/322% vs 组合级 43.7%/22%），以便未来读者不再重踩。
12. 作为研究者，我要**修正方案 §8（Track 0 被当成未来工作）**，以便计划不再指导重复已完成的工作。
13. 作为研究者，我要 **`GLOSSARY.md`** 统一术语，以便讨论与代码使用同一套词汇。
14. 作为研究者，我要"冻结方法不冻结参数""现金拖累根因 = 固定 1/N 仓位规则"两条 **ADR**，以便决策可追溯。
15. 作为 agent 执行者，我要实验表达为**纯函数级最小改动**，以便能在既有 S1 缝上测试。
16. 作为 agent 执行者，我要**契约守卫行为被写明**，以便知道改冻结口径牵动什么。
17. 作为研究者，我要**确定性与 `config_hash` 在任何改动下保持**，以便结果可复现（numexpr 缺陷不再污染基线）。
18. 作为下游生产链路，我要任何默认值改动**在 handoff / `main.py` 同口径体现**，以便实盘与回测一致。
19. 作为研究者，我要一份**明确的 out-of-scope 清单**，以便努力不外溢。
20. 作为研究者，我要**诊断 + 方案 + 文档先交付、代码后行**，以便共识达成前不动系统。

## Implementation Decisions

- **涉及模块**：`scripts/portfolio_sim.py`（度量 / 实验核心）、`scripts/backtest_5y_23strategies.py`
  （组合链 + 契约守卫 + CLI）、`result/指标契约_metric_contract_v1_*.md`（修订）、
  `docs/backtest-optimization-design.md`（账本追加）、新增 `GLOSSARY.md` 与 `docs/adr/`。
- **契约冻结语义修订**：冻结 = 度量口径（成本 0.35% 往返 / T+1 盯市 / PIT 宇宙 / 去簇共振定义 /
  `next_open` 入场）；`min_resonance_families` 从"冻结常量"降为"实验参数"。
- **契约守卫语义**：`_portfolio_sim_config()` 由"`min_resonance_families` 必须 == 2"改为
  "必须匹配**冻结度量画像**；实验参数显式登记后允许覆盖（附 warning 且改变 `config_hash`）"。
- **实验卡登记表**：
  - 卡 A：`min_resonance_families = 1`（消融已完成，待门槛）。
  - 卡 B：资金部署感知仓位（波动率目标 / 用闲置现金；复用既有全局配置）。
  - 卡 C：并发信号选股 / 持仓上限排序（计划书原文 Card A）。
  - 卡 D：`tushare_moneyflow` 分层主力资金因子（低优先级，基线稳定后再跑）。
- **实验优先级（Round 2 锁定）**：卡 A 消融已完成，过 bootstrap 门槛即可翻转；新增实验按
  **卡 B（资金部署感知仓位）→ 卡 C（并发信号选股 / 持仓上限排序）**顺序推进；卡 D 最低，基线稳定后再跑。
- **门槛**：池化样本外每信号 Δ 的日期分块 bootstrap CI，风险调整口径（CAGR / Sharpe / maxDD）
  不劣于基线方可翻转；WFO 保留给*改信号 / 出场定义*的改动。
- **可复现性**：任何实验参数改动必须改变 `config_hash` 并落进侧边产物。
- **不新增 CLI**：优先复用 `--portfolio-sim-min-families`；确需才加，并同步 `.env.example` 与文档。
- **ADR**：0001「冻结度量而非参数」；0002「现金拖累根因 = 固定 1/N 仓位规则」。
- **GLOSSARY 术语**：组合级 / 资金约束 / 样本外 CAGR / 去簇共振 / 家族 / 家族去重 / 领先期望 /
  盯市(MTM) / PIT 宇宙 / 口径 / bootstrap CI / 实验卡 / 度量契约 / config_hash。

## Testing Decisions

- **Seam（已与用户确认，零新增缝）**：
  - **S1（主，单元）** = `scripts/portfolio_sim.py` 纯管线
    `build_candidate_table → apply_declustered_resonance → apply_lead_expectation →
    collapse_candidates → simulate → compute_metrics`；先例 `tests/test_portfolio_sim.py`。
  - **S2（集成）** = driver 链 `_run_portfolio_sim_chain` + `compute_dynamic_exit_returns` + 产物写入；
    先例 `tests/test_portfolio_sim_integration.py`。
  - **S3（守卫）** = `_portfolio_sim_config()` 冻结画像校验 + 实验覆盖语义。
- **好测试只验外部行为**：给定 config + 合成 candidates/bars，断言**漏斗闭合**（候选 = 成交 + 各拒绝原因）、
  **现金对账**、**确定性（`config_hash` 稳定）**、指标值；不测内部实现细节。
- **被测模块**：`scripts/portfolio_sim.py`（单元）；driver 链（集成）。
- **新增测试**：契约守卫行为（实验覆盖改哈希 + 告警；度量画像不符则 `RuntimeError`）；
  仓位实验不变量（部署 ≤ 现金、持仓 ≤ 上限、`max_dd_floor_pct` 生效）。

## Out of Scope

- 出场参数再调（§9 已饱和 / 证伪）。
- 本阶段不做完整多年 WFO 引擎（T0.3）。
- 新增策略。
- **本轮不修改任何回测代码**（本阶段交付 = 诊断 + 方案 + 文档）。
- 生产 / Web / Desktop 改动。

## Further Notes

- **归档方式**：本 spec 保存在 `docs/specs/backtest_度量层重建/spec.md`，随 git 版本管理；不发布到外部
  追踪器（本地 markdown 追踪，约定见 `docs/agents/issue-tracker.md`）。
- **决策记录（Round 2，已锁定）**：
  - Q1：契约改为**只冻结度量口径**，`min_resonance_families` 降级为登记实验 + 可构建 bootstrap 门槛。
  - Q2：**资金部署感知仓位**（卡 B）先于卡 C（并发信号选股）。
  - Q3：**moneyflow** 登记为低优先级卡 D，基线稳定后再跑。
- **numexpr 2.14.1 缺陷背景**：回测进程已 `compute.use_numexpr=False`；T0.1 基线与 family 消融曾被污染
  （详见 `docs/CHANGELOG.md`）。
- **一次完整回测 ≈ 1.5h**；偏好可逆旋钮实验。
- **既有产物**：`scripts/portfolio_sim.py`、`tests/test_portfolio_sim.py`、
  `tests/test_portfolio_sim_integration.py`、`result/指标契约_metric_contract_v1_20261017.md`、
  `result/backtest_胜率收益率提升规划_v2_20261017.md`、`docs/backtest-optimization-design.md`。
