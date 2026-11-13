# 03: 同一天三口径 A/B 实验卡

**What to build:** 以预注册方式产出「`auto` 相对固定三档」的对照证据：同一天 / 同一数据快照下比较 `auto` / `enh2` / `enh` / `strict`，量化 `auto` 实际减少多少 `0/5` 天、对 CAGR / maxDD / 胜率的影响，以及 58 个 regime 连续段下的过拟合风险，关闭 spec 的待验证项。本票只产证据与判决记录，不改默认值、不重判 10 月四张实验卡。

**Blocked by:** 02（`auto` 主实现：取数需要已落地的构建）。此依赖只约束本票自身何时能开工，**不阻塞 02**；按用户 2026-11-12 裁决，本票与实现并行安排、不作为实现的前置阻塞。

**Gate:** `v1-signal`
**Signal set:** `changed`
**Gate rationale:** `docs/adr/0006-auto-regime-mode-default.md`「后果」段记录用户 2026-11-12 对默认翻转的显式豁免；本对照涉及信号集变化，按 v1-signal 登记，预注册口径引用该 ADR，不事后换估计量。

**Status:** ready-for-human

- [x] 开跑前写定预注册：对照日期/数据快照、估计量、判据、变体计数、回滚方式；不在看过结果后改判据或补跑「对 auto 有利」的变体（遵循 `docs/adr/0003` 纪律）。→ `issues/preregistration-card.md`
- [ ] 产出同一天四口径（`auto` / `enh2` / `enh` / `strict`）的可比结果，附数据快照与 `config_hash` 对齐说明。**待显式授权开跑**（Q8/R4=否、spec Out of Scope 明列不跑回测）。
- [ ] 量化 `0/5` 天数变化，并区分「空仓市况导致的 0 只」与「信号稀疏/门槛导致的 0 只」两类成因。**脚手架已就绪**（`zero_cause_split` / `zero_open_days`），真实数值待开跑。
- [ ] 报告 CAGR / maxDD / 胜率差异与弱市年份（`bull 70.3% / range 20.0% / bear 9.7%`、58 个 regime 连续段）的过拟合风险；同型 regime 切换 trail 曾被证伪的结论纳入讨论。**脚手架已就绪**（`deltas_vs_enh2` / `overfitting_risk`），真实数值待开跑。
- [x] 判决产物显式记录 `wfo: {required: false, ...}`，不静默跳过；结果写入既有 `result/gates/` 登记出口。→ `result/gates/auto-regime-ab/preregistration.json`（占位，声明未开跑）+ `build_verdict()`/`main()` 产出骨架。
- [ ] 结论回填 spec 的「待验证项」，但不因结果不利而自动回滚 02 的默认决定（回滚须另立票并作废 ADR 0006）。**待开跑后执行**。

**Note:** 用户 2026-11-12 裁决「与实现并行」；本票取数依赖 02、但 02 不依赖本票。术语遵循 `GLOSSARY.md`。

## Comments

- 状态 `ready-for-human` 而非 `done`：按 Q8/R4=否 与 spec Out of Scope「不跑同一天三口径 A/B 对照、
  不跑完整 WFO、不连 PG」，本实现轮**只交付预注册卡 + 可运行对照脚手架**，四口径回测实跑与
  判决须人类显式授权后另起一轮。这是边界，不是半成品遗留。
- 交付物：`docs/specs/auto-regime-mode/issues/preregistration-card.md`（版本控制，不带 `NN-` 前缀以免被 `check_spec_tickets.py` 当票校验）、
  `scripts/auto_regime_ab_compare.py`（纯函数 + CLI，不连 PG / 不跑回测 / 不伪造变体）、
  `tests/test_auto_regime_ab_compare.py`（12 passed）、
  `result/gates/auto-regime-ab/preregistration.json`（gitignored 本地占位）。
- `wfo: {required: false, folds: null, reason: ...}` 显式记录，沿用 `scripts/portfolio_gate.py`
  既有契约；`overall_pass: null` / `adjudicated: false` 表示预注册未判决。
- 回滚约束：结果不利不自动回滚票 02 默认决定；`auto_default_rollback` 字段写明须另立票 +
  作废 ADR 0006。
- **两轴 review 记录（2026-11-12）**：Standards 轴指出 `auto_regime_ab_compare.py` 与
  `scripts/portfolio_gate.py` 的 `build_verdict` / 判决产物路径构成平行实现（J4）。**接受为有意的边界**：
  本票只做「预注册 + 对照脚手架」，产物是 `status=pre-registered` / `adjudicated=false` 的骨架，
  不是 `portfolio_gate.py` 那种 content-addressed、带 `verdict_hash` / registry 行的**已判决**产物；
  复用后者会错误暗示对照已跑完并已裁决（与 Q8/R4=否、spec Out of Scope 冲突）。等四口径实跑
  授权后，判决阶段应优先评估复用 `portfolio_gate.py` 的 `write_verdict` / registry，避免长期双轨。
