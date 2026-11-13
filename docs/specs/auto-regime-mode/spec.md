# Spec: 市场环境口径默认改为逐日自动判断（auto）

> 状态：**已定稿**（`/grill-with-docs` 会话，Q1–Q23 + Q18′ 全部裁决；用户 2026-11-12 显式拍板）。
> 产物保存在本地文件 `docs/specs/auto-regime-mode/spec.md`，纳入 git 版本管理；本地 markdown 追踪约定见
> `docs/agents/issue-tracker.md`。设计取舍与豁免记录见 `docs/adr/0006-auto-regime-mode-default.md`。
> **本 spec 的票 01/02 行为已于 2026-11-12 在集成分支 `auto-regime-mode-integration` 实现**；
> 票 03（同一天四口径 A/B 实跑）按 Q8/R4=否 与 Out of Scope **未开跑**，仅交付预注册卡与对照
> 脚手架、状态 `ready-for-human`。设计文本保留作为口径来源，实现细节见
> `docs/adr/0006-auto-regime-mode-default.md` 的「实现记录」与各票 Comments。
> 术语一律使用 `GLOSSARY.md`「市场环境与市况」段定义；遵守 `docs/adr/0001`–`0006` 与
> `result/指标契约_metric_contract_v1_20261017.md` §2.1。

## Problem Statement

这套 5 年多策略回测的「可开仓日」由 `--regime-mode {enh2,enh,strict}` 三选一手动指定，默认 `enh2`。
使用者每次运行都要自己判断该传哪一档，而三档是三套条件、不是松紧梯度、彼此也不构成包含关系，
手工选档既缺依据又容易与当轮行情错配。更麻烦的是，这个默认值本身几乎没有治理依据：

- 「为什么 `enh2` 是默认」没有任何 ADR / spec / A/B 数据或审批记录；`docs/CHANGELOG.md` 的一行是唯一明文，
  引入它的 commit 标题不含口径决策语义，0.45 / 0.85 两个阈值的来源也无记录。
- 口径既不在《指标契约》§2.1 的冻结度量画像，也不在 `_REGISTERED_EXPERIMENT_DEFAULTS` 登记清单，
  更不进 `config_hash` —— 它处于治理空白，改动它不会被任何守卫察觉。
- 2026-09-19 的五份优化建议已提出过几乎同型的方案（动态广度阈值、按波动率分层切 `enh`/`enh2`、
  `market_ok_grade` 三态），综合对比以 X5「双向风险」列为不做，改选防守族半仓 opt-in。此后零 commit、零实跑。
- 2026-10-29 起 `market_ok_enh2` 连续 0/5 天可开仓、handoff 0 只股票、按策略纪律跳过 `main.py`；
  但 `enh 5/5` 满可开仓仍可能 0 只（成因在共振/handoff 层），说明换口径不是零推荐的唯一解。

使用者想要的是：**别再手工传参，让系统按当日大盘自行判断用哪一档；若显式传了档位，就明确覆盖并在日志可见。**
目标仅是免手工，不承诺减少 0 推荐、不承诺提高收益。

## Solution

把 `--regime-mode` 的默认值从 `enh2` 改为 `auto`：

1. 不传参即逐日自动判断；显式传 `enh2` / `enh` / `strict` 即为覆盖，并在启动日志打印覆盖标记。
2. 逐日只用截至当日 as-of 可得的市况输入（`bull/range/bear`、`breadth`、`vol_pct`），不引入前视。
3. 规则保守门控：默认与冲突回退均为 `enh2`；仅在 `regime=bull` 且 `breadth≥0.45` 且 `vol_pct≤0.85`
   时放宽到 `enh`；`bear`、高波动、数据缺失或判定冲突一律保持 `enh2`；`regime`/`breadth` 缺失打
   WARNING，`vol_pct` 预热期 NaN（当日无值，非加载失败）只记 INFO（详见 Implementation Decisions
   「缺失/异常语义」）；`strict` 永不自动选中。
4. `enh2` 退化为显式覆盖值，同时是 `auto` 的基准与回退档。既有 `--strict` / `--regime-mode` 保留为逃生舱。
5. 治理上按《指标契约》§2.1「定义冻结、阈值登记」的示范拆两层：三列算法定义冻结；
   `auto` 选择器的规则版本、阈值与回退行为作为已登记实验参数，规则版本单值计入 `config_hash`。
6. 观测前置：逐日市况表落盘，启动日志能表达规则版本、逐日选档序列与显式覆盖标记。
7. 与既存 `BACKTEST_REGIME_SOFT` / `--regime-soft` 和 `--defensive-half-pos` 正交：不取代、不改动、不与自动选档叠加。

**交付 = 可运行实现 + 回归测试 + 文档同步**；文档侧（GLOSSARY、ADR 0006、CHANGELOG、runbook、指标契约）
已在设计轮落盘，本 spec 对应的是**剩余代码改动**。

## User Stories

1. 作为研究者，我要不传 `--regime-mode` 时系统自行逐日选档，以便不必每轮手工判断行情该用哪一档。
2. 作为研究者，我要显式传 `enh2` / `enh` / `strict` 时恒定覆盖自动判断，以便复现历史结果与排障。
3. 作为研究者，我要启动日志明确标出「本次是自动判断还是显式覆盖」，以便分辨两次结果差异的来源。
4. 作为研究者，我要自动判断只使用截至当日可得的数据，以便回测不引入前视偏差。
5. 作为研究者，我要 `bull` 且广度/波动率双确认时才放宽到 `enh`，以便弱市不被自动放水。
6. 作为研究者，我要 `bear`、高波动、数据缺失或判定冲突时回退 `enh2` 并打 WARNING，以便缺失不被静默当成「满足条件」。
7. 作为研究者，我要 `strict` 永不自动选中，以便自动口径不会比现有默认更严。
8. 作为研究者，我要三档 `market_ok*` 的算法定义保持冻结，以便度量基准不被选择器悄悄改写。
9. 作为研究者，我要 `auto` 规则版本计入 `config_hash`，以便区分「已登记重跑」与「静默调参」。
10. 作为研究者，我要逐日选档序列是数据的确定性函数，以便同一数据快照可逐位复现。
11. 作为研究者，我要逐日市况表落盘，以便离线复算「哪天被判成哪一档」并验证选得对不对。
12. 作为研究者，我要启动日志能表达规则版本与逐日选档序列，以便排障对齐清单仍然有效。
13. 作为研究者，我要 `BACKTEST_REGIME_SOFT` / `--regime-soft` 行为不变，以便策略分族/软加权与口径选择互不干扰。
14. 作为研究者，我要 `--defensive-half-pos` 仍为显式 opt-in 且不与自动选档叠加，以便保守路径不被双重放宽。
15. 作为研究者，我要 `--strict` 与显式 `--regime-mode strict` 的既有优先级保持，以便旧命令不失效。
16. 作为研究者，我要复现 2026-10-19 冻结 PIT 基线、10 月四张实验卡、2026-10-29 起 0/5 日志时显式传 `enh2`，以便历史结论不被新默认污染。
17. 作为研究者，我要 `0 只股票` 仍按 runbook §4 属正常观测，以便「免手工」不被误当成「保证出推荐」。
18. 作为研究者，我要能区分「空仓市况导致的 0 只」与「信号稀疏/门槛导致的 0 只」，以便不把两类成因混为一谈。
19. 作为 agent 执行者，我要改动集中在尽可能少的 seam 上，以便能测试先行、逐票落地。
20. 作为维护者，我要「默认翻转为 `auto`」这一决定及其对 ADR 0004 / 指标契约放行门槛的显式豁免有 ADR 可查，以便 review 时不重复争论。
21. 作为维护者，我要 CLI 行为变更同步 `.env.example`、`docs/CHANGELOG.md`、指标契约与 runbook，以便文档与实现不分叉。
22. 作为维护者，我要实现后能用既有冻结哈希复现基线，以便确认默认翻转没有静默改变度量基准。

## Implementation Decisions

- **默认翻转**：`--regime-mode` 默认由 `enh2` 改为 `auto`；`auto` 是第 4 个取值，不是新开关、不是新的 env 开关。
  显式传档位即覆盖并在启动日志标注。旧 `--strict` 与显式 `strict` 的优先级语义不变。
- **逐日粒度**：选择器在「已算好逐日市况列」的层面工作，只用当日行；不使用「每轮开头按最近 N 日选一次」
  （该形态在 `docs/backtest-optimization-design.md` 已被记录为前视并修复），也不使用全期占比、
  `stationarity_score`、季度 WF 权重等事后量。
- **输入集**：只复用回测自带的 `bull/range/bear`、`breadth`、`vol_pct`（以及其派生列）。不引入实盘链路的
  `trending_up/…`、`强势/偏暖/震荡/偏弱`、`进攻/均衡/防守` 三套词表——它们之间没有代码级映射，
  且 handoff 只传股票代码不传参数。
- **规则形态（保守门控）**：基准档 `enh2`；放宽条件为 `regime=bull & breadth≥0.45 & vol_pct≤0.85` → `enh`；
  其余（含 bear、高波动、缺失、冲突）一律 `enh2`；`strict` 不在自动输出集内。
  规则常量（阈值、回退档、放宽条件）与规则版本一起登记，可被测试直接断言。
- **缺失/异常语义**：`breadth` / `vol_pct` / `regime` 缺失或异常时一律回退 `enh2`，不静默放行。
  这与既有 `fillna(1.0)` / `fillna(0.0)` 的容错默认值不同，需在实现中显式区分「加载失败」与「当日无值」：
  `regime` / `breadth` 缺失或非法值属**加载失败** → 回退 `enh2` 并打 WARNING；`vol_pct` 在扩展波动率
  分位预热期（前 ~20 日）天然为 NaN 属**当日无值**（非缺失），仍按保守口径回退 `enh2`，但只记 INFO
  （否则每次 auto 运行都会刷无意义 WARNING）。两类都保证「缺失不被静默当成满足条件」。
- **治理归类**：`market_ok` / `market_ok_enh` / `market_ok_enh2` 三列的算法定义属冻结项，不得被选择器覆盖；
  `auto` 选择器的规则版本（如 `auto_v1`）、阈值与回退行为属已登记实验参数，沿用
  `_REGISTERED_EXPERIMENT_DEFAULTS` 既有守卫（未登记参数拒绝覆盖、显式登记覆盖 → warning + `config_hash` 改变）。
- **可复现性**：规则版本作为**单值**纳入 `config_hash`；逐日选档结果是数据的确定性函数，不单独入哈希。
  默认翻转会触发 `rank_mode` 同型的「默认翻转不改变哈希」陷阱。
  **实现裁决（与最初设想的偏离，已记录）**：`regime_selector` 保留在 `_HASH_OPTIONAL_KEYS` 内、
  登记默认值取冻结值 `enh2`；`auto` 运行由驱动显式注入 `auto_v1`（≠ 登记默认值，故弹出后仍留在哈希里），
  显式 `--regime-mode enh2` 则保持已发布的冻结基线哈希逐位不变。采用「保持已发布哈希 + 运行时注入」
  而非「移出 optional keys + 重算基线」，是为了同时满足 US-9（auto 改变哈希）与 US-22 / 票 02
  「冻结基线逐位可复现」两条要求；详见《指标契约》§2.1 与 ADR 0006「实现记录」。
  若未来把 `auto_v1` 设为登记默认值，再按「移出 + 重算」处置并公告全部冻结基线哈希。
- **观测**：逐日市况表写入既有 result/ 产物出口（不新增平行目录），逐日七列 + auto 选档两列，
  **完整有序序列**以 CSV 为准（可离线复算「哪天被判成哪一档」）；启动日志分两处表达：
  既有「市场环境」串（配置段、早于 `compute_market_ok`）追加规则版本与覆盖标记；
  merge 点另打一行「逐日市况」摘要，含规则版本、口径标记、三列计数与逐日选档**档位分布摘要**
  （`enh:N,enh2:M`，非有序序列，有序序列在 CSV）。两处规则版本/覆盖标记判据一致，
  保持 `docs/backtest-runbook.md` §3「先确认这两行」的可读性（config_hash 仍由组合模拟报告打印）。
- **与软加权正交**：`BACKTEST_REGIME_SOFT` / `--regime-soft` / `--regime-filter` 继续在同一轮运行内按
  `regime` 调整策略信号；`--defensive-half-pos` 保持显式 opt-in，不与自动选档叠加。
- **不做全市场软化**：与 2026-09-19 X5 的否决保持一致；`auto` 只在「已满足严条件的 bull 日」放宽，
  不是 `enh2_soft`、不是把广度阈值整体下调。
- **同口径契约**：handoff 与 `main.py` 的行为不因本次改动新增参数传递；`auto` 只影响回测侧可开仓判定，
  沿用 ADR 0001 / 0002 的既有同口径要求。
- **豁免记录**：默认翻转与 ADR 0004「判决不翻转任何默认值」、《指标契约》预注册放行规则、X5 冲突，
  由用户显式豁免并已记入 ADR 0006；实现轮不再重开该争论。
- **文档同步面**：`.env.example`（若新增可见配置）、`docs/CHANGELOG.md`（已有设计条目，实现轮补实现条目）、
  《指标契约》修订行、runbook 参数语义与日志段、GLOSSARY 词条（设计轮已改）。
- **不含具体文件路径与代码片段**：按 spec 模板约定，具体落点由 tickets 在探索后确定。

## Testing Decisions

- **什么算好测试**：只断言外部可观察行为（给定 CLI/env 与逐日市况表，选哪列、日志写什么、`config_hash` 变不变、
  缺失时是否回退并告警），不断言选择器内部实现细节、不 mock 掉真实风险层。
- **测试 seam（已确认：1 主 + 1 观测）**：用户 2026-11-12 明确确认按推荐值采用「1 主 seam
  （口径解析统一入口）+ 1 观测 seam（启动日志/逐日市况表）」，不再降级为单一 seam、也不新开第三个。
  - 主 seam = 「口径解析 / 可开仓判定列选择」这一既有统一入口（回测、验证、handoff、统计共用）。
    所有下游沿用该入口，不在 `_entry_mask` 内另开分支。断言：默认 `auto` 的逐日选档、显式覆盖恒定、
    覆盖标记、缺失回退、保守门控条件、规则版本单值入 `config_hash`、同参两次运行逐位一致。
  - 观测 seam = 启动日志与逐日市况表产物。断言：规则版本/序列摘要/覆盖标记可表达、逐日六列表落盘、
    `result/` 产物不含新的平行目录。
- **要测的行为清单**（对应 User Stories）：默认免手工、显式覆盖、前视约束（用「未来行改动不影响历史行选择」
  的因果断言）、保守门控、`strict` 不自动选中、缺失回退 + WARNING、软加权/防守族半仓不受影响、
  冻结基线复现（显式 `enh2` 时哈希逐位不变）、默认翻转后哈希按登记改变。
- **prior art**：`tests/test_portfolio_sim_integration.py` 的四路径（画像篡改 → `RuntimeError`、
  登记默认值漂移 → `RuntimeError`、已登记覆盖 → warning + `config_hash` 改变且可复现、
  未登记覆盖 → `ValueError`）；契约守卫与实验卡定向套件（票 01 收口记录 87 passed）；
  `scripts/check_spec_tickets.py` 对票元数据的校验；`scripts/check_ai_assets.py` 对治理资产的校验。
- **验证命令**：改 Python 后按 `docs/agents/validation-matrix.md` 走 `py_compile` 与定向
  `pwsh -File scripts/pytest.ps1 <files>`，交付前按矩阵说明全量 gate 覆盖与缺口；
  回测实跑只在需要关闭 A/B 待验证项时单独授权，不作为本 spec 的必跑项。

## Out of Scope

- 不实现「减少 0 推荐」或「提高收益/胜率」的产品语义反转；`0 只股票`仍是正常观测。
- 不处理共振 / handoff 门槛层的零推荐成因（2026-08-24 反例已证明它与可开仓口径独立）。
- 不改 `compute_market_ok` 三列算法、不改 0.45 / 0.85 作为 `enh2` 固定阈值的定义。
- 不引入实盘链路的市况词表、不建立跨链路映射表、不通过 handoff 传参。
- 不改 `--regime-soft` / `--regime-filter` / `--defensive-half-pos` / `--confluence-only` 的既有语义。
- 不新增「只能开不能关」式 env 开关，不叠加互斥模式。
- 不跑同一天三口径 A/B 对照、不跑完整 WFO、不重判 10 月四张实验卡（这些是 Further Notes 里的后续证据工作）。
- 不做本机仓库外调度器排查、不连 PG 查事件表新鲜度、不改实盘 `main.py` 日报链路。
- 不在此 spec 内 `git commit` / `tag` / `push`；文档侧 5 处未提交改动的处置由用户另行拍板。

## Further Notes

- **设计与实现均已落盘**：设计轮完成 `GLOSSARY.md`（市场环境与市况段）、ADR 0006、
  `docs/CHANGELOG.md` `[Unreleased]`、`docs/backtest-runbook.md` §2/§3、指标契约修订行与
  §2.1 条款；实现轮（票 01/02）已消除 runbook / CHANGELOG / ADR 里「当前代码尚未实现
  `auto`」的标注，并把 `regime_selector` 登记、逐日市况表、启动日志序列摘要落地。
  config_hash 陷阱按「保持已发布哈希 + 运行时注入 `auto_v1`」处置，见指标契约 §2.1。
- **待验证项（开跑前已知）**：无同一天三口径 A/B 对照；`auto` 实际减少多少 `0/5` 天、对 CAGR/maxDD/胜率
  的影响未知；`bull 70.3% / range 20.0% / bear 9.7%`、仅 58 个 regime 连续段下的过拟合风险未测；
  同型 `regime` 切换 trail 曾被证伪。
- **A/B 实验卡排位（已裁决：与实现并行、取数依赖 02）**：用户 2026-11-12 明确「与实现并行」，
  `/to-tickets` 进一步裁决该票 `Blocked by: 02`——同一天三口径对照、`0/5` 改善幅度与过拟合验证
  作为实验卡票，其**取数依赖 02 已落地的构建，但 02 不依赖它**，因此不列为实现的前置阻塞
  （因 Q18′=B 已显式豁免放行门槛，再拿「先跑证据」当第一票等于变相推翻裁决）。该票应引用
  `docs/adr/0006` 的豁免；结果用于关闭上述待验证项，但不 gate 住实现票的落地。
- **日志证据缺口**：现有日志不打印逐日 `market_ok*` 布尔值 / `breadth` / `vol_pct` / regime 三态，
  `result/**/*.csv` 表头也无这些列 —— 这正是「观测前置」user story 的由来。
- **历史复现口径**：冻结 PIT 基线（`docs/backtest-runbook.md` §4）、10 月四张卡、2026-10-29 起全部 0/5 日志
  都基于「默认 `enh2`」；默认翻为 `auto` 后，复现它们须显式传 `--regime-mode enh2`。
  上一轮「11 月零推荐」归因结论也须加「基于 enh2 默认」限定。
- **票元数据约束**：后续 `/to-tickets` 产出的每张票必须声明 `Gate:` / `Signal set:` / `Gate rationale:`
  （见 `docs/agents/issue-tracker.md` 与 `scripts/check_spec_tickets.py`）。本改动默认属
  `Gate: none` / `Signal set: changed`（可开仓判定变化会改变信号集）；若某张票被登记为 v1-signal
  比较，须引用 ADR 0006 的豁免作为 rationale。
- **回滚**：把 `--regime-mode` 默认改回 `enh2`、作废 ADR 0006（状态改 deprecated）、撤销指标契约
  2026-11-12 修订行；文档侧 5 处文件可用 `git checkout --` 单独恢复，ADR 0006 为新建文件直接删除。
