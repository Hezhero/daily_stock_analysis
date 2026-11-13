# 03: 同一天三口径 A/B 实验卡

**What to build:** 以预注册方式产出「`auto` 相对固定三档」的对照证据：同一天 / 同一数据快照下比较 `auto` / `enh2` / `enh` / `strict`，量化 `auto` 实际减少多少 `0/5` 天、对 CAGR / maxDD / 胜率的影响，以及 58 个 regime 连续段下的过拟合风险，关闭 spec 的待验证项。本票只产证据与判决记录，不改默认值、不重判 10 月四张实验卡。

**Blocked by:** 02（`auto` 主实现：取数需要已落地的构建）。此依赖只约束本票自身何时能开工，**不阻塞 02**；按用户 2026-11-12 裁决，本票与实现并行安排、不作为实现的前置阻塞。

**Gate:** `v1-signal`
**Signal set:** `changed`
**Gate rationale:** `docs/adr/0006-auto-regime-mode-default.md`「后果」段记录用户 2026-11-12 对默认翻转的显式豁免；本对照涉及信号集变化，按 v1-signal 登记，预注册口径引用该 ADR，不事后换估计量。

**Status:** done

- [x] 开跑前写定预注册：对照日期/数据快照、估计量、判据、变体计数、回滚方式；不在看过结果后改判据或补跑「对 auto 有利」的变体（遵循 `docs/adr/0003` 纪律）。→ `issues/preregistration-card.md`
- [x] 产出同一天四口径（`auto` / `enh2` / `enh` / `strict`）的可比结果，附数据快照与 `config_hash` 对齐说明。2026-11-13 用户显式授权后实跑完成，四臂同快照 `20261112_pit`。
- [x] 量化 `0/5` 天数变化，并区分「空仓市况导致的 0 只」与「信号稀疏/门槛导致的 0 只」两类成因。结果：四臂验证窗口均 `0/5`（5 天全空仓），`delta_vs_enh2=0`。
- [x] 报告 CAGR / maxDD / 胜率差异与弱市年份（`bull 70.3% / range 20.0% / bear 9.7%`、58 个 regime 连续段）的过拟合风险；同型 regime 切换 trail 曾被证伪的结论纳入讨论。见 `result/gates/auto-regime-ab/verdict.json` 与下方 Comments。
- [x] 判决产物显式记录 `wfo: {required: false, ...}`，不静默跳过；结果写入既有 `result/gates/` 登记出口。→ `result/gates/auto-regime-ab/verdict.json`（**观察性记录**，`status=observations-recorded` / `overall_pass: null`，含 `human_verdict` / `interpretation`）。
- [x] 结论回填 spec 的「待验证项」，但不因结果不利而自动回滚 02 的默认决定（回滚须另立票并作废 ADR 0006）。本轮结果中性，未回滚。

**Note:** 用户 2026-11-12 裁决「与实现并行」；本票取数依赖 02、但 02 不依赖本票。术语遵循 `GLOSSARY.md`。

## Comments

- **历史记录（2026-11-12，已被下方「实跑结果」取代，保留仅作审计）**：初版状态 `ready-for-human`，
  按当时 Q8/R4=否 与 spec Out of Scope 只交付预注册卡 + 对照脚手架，实跑与判决待人类授权；
  `overall_pass: null` / `adjudicated: false` 表示当时未判决。2026-11-13 用户显式授权后已实跑并判决，
  本票现为 `done`；`result/gates/auto-regime-ab/verdict.json` 为唯一当前判决产物，
  早期的 `preregistration.json` 占位与「骨架」表述已失效。
- 交付物（版本控制）：`issues/preregistration-card.md`（不带 `NN-` 前缀以免被 `check_spec_tickets.py`
  当票校验）、`scripts/auto_regime_ab_compare.py`（纯函数 + CLI）、
  `scripts/auto_regime_ab_run.py`（实跑产物接线）、`scripts/run_auto_regime_ab.sh`（四跑编排）、
  `tests/test_auto_regime_ab_compare.py`（12 passed）、`tests/test_auto_regime_ab_run.py`（9 passed）。
- `wfo: {required: false, folds: null, reason: ...}` 显式记录，沿用 `scripts/portfolio_gate.py`
  既有契约；当前 verdict 为 `status=observations-recorded` / `adjudicated=false` /
  `overall_pass: null`（**无数值 gate，人工观察结论见 `human_verdict.reason`**，见下方实跑结果）。
- 回滚约束：结果不利不自动回滚票 02 默认决定；`auto_default_rollback` 字段写明须另立票 +
  作废 ADR 0006。本轮结果中性，未回滚。
- **两轴 review 记录（2026-11-12）**：Standards 轴指出 `auto_regime_ab_compare.py` 与
  `scripts/portfolio_gate.py` 的 `build_verdict` / 判决产物路径构成平行实现（J4）。当时**接受为有意的边界**：
  产物是 `status=pre-registered` 骨架，不是 `portfolio_gate.py` 那种 content-addressed、带
  `verdict_hash` / registry 行的**已判决**产物。2026-11-13 实跑后仍用本卡 scaffold 产出
  **观察性记录**（保持预注册口径不变），未接入 `portfolio_gate.py` registry；若未来重跑或新卡，
  应优先评估复用 `write_verdict` / registry，避免长期双轨。

## 实跑结果（2026-11-13，用户显式授权）

- 四跑串行 `auto → enh2 → enh → strict`，全部 rc=0；`scripts/run_auto_regime_ab.sh` 编排，
  各臂 `--portfolio-sim --no-handoff --portfolio-sim-out result/gates/auto-regime-ab/runs/<mode>`，
  `LOG_DIR` 分目录。同一快照 `20261112_pit`（PG 最大 trade_date=2026-11-12，
  `check_data_freshness.py --as-of 2026-11-12` 六表全 OK）、同一 `universe=pit`、
  同一 `.env` 静态开关（`BACKTEST_CONFLUENCE=1` / `BACKTEST_REGIME_SOFT=1`）。
- 产物：`result/gates/auto-regime-ab/runs/<mode>/portfolio_sim_*_20261112_pit.json`（4 份）+ 日志
  `logs/auto-regime-ab/<mode>/backtest_5y_20261113.log`（4 份，均被 gitignore，属本地证据）。
  **观察性记录**：`result/gates/auto-regime-ab/verdict.json`（`status=observations-recorded`，
  `overall_pass: null`，含 `variants` / `human_verdict` / `interpretation`）。
- **核心发现（如实记录，不包装）**：`auto` 与 `enh2` 的 `cagr_oos` / `maxDD_pct` / `win_rate` /
  `n_trades` / rejects / funnel **逐位完全相同**，Δ 全为 0。`auto` 在 1419 个交易日里判 `enh` 334 天，
  其中 314 天 `market_ok_enh=True` 且这 314 天 **全部** `market_ok_enh2=True`——保守门控「只在
  已满足 enh2 双确认的 bull 日放宽」在该快照上**没有产生任何新的可开仓日**，信号集与成交不变。
- **0/5 观测**：四臂验证窗口均 `0/5`（5 天全空仓），`delta_vs_enh2=0`；`auto` **没有减少 0/5 天**。
  两类成因中本轮为空仓市况主导（固定档位与 auto 皆 0/5），未出现「有可开仓日但 handoff 0 只」的
  独立样本；2026-08-24 的信号稀疏反例不在此快照窗口内。
- **固定档位差异**：`enh` / `strict` 相对 `enh2` 的 CAGR_OOS +0.34 / +0.36 pct、maxDD −1.03 / −1.08 pct，
  `strict` 胜率更低；这是固定档位本身的差异，**不是 auto 的增益**。
- **判决（观察性记录，无数值 gate）**：baseline `7ae970ef` 的预注册卡只冻结了**描述性判据**
  （各口径相对 `enh2` 的 Δ / `0/5` 变化 / 两类成因 / 过拟合讨论），§7.3 原定「人工填充
  `overall_pass` 与判决理由」。实现轮曾事后加入数值 PASS gate，两轴 review 判定违反 ADR 0003，
  已按用户裁决**移除**：当前 `overall_pass: null`、`adjudicated: false`，`human_verdict.reason`
  由人工填写观察结论（`auto ≡ enh2`、四臂 `0/5`、中性结果），**不作 PASS/FAIL 判定、不声称
  「auto 更优」**。ADR 0006 只承诺免手工，不承诺改善收益 / 减少 0 推荐。过拟合风险
  （58 个 regime 段、同型切换曾被证伪）维持「任何增益暂定」结论。
- **哈希对齐局限**：`enh2` / `enh` / `strict` 三臂 `config_hash` 同为 `acb32bd9…`（票 02 仅在
  `mode=="auto"` 注入 `auto_v1`），固定三档哈希不可区分；已写入 verdict 的
  `interpretation.hash_alignment_limit`，**不事后改判据**。
- **快照证明**：不只靠产物文件名的 end date；四臂 summary 的**实测窗口**一致
  （`n_days=1419` / `oos_start=2022-01-17` / `oos_days=1166`），`assemble_run_evidence` 跨臂
  交叉校验该窗口，不一致即报错。start date 因各臂首信号日不同而异
  （`20210514` / `20210428` / `20210528`），故必须用实测窗口而非文件名单独断言。
- **零成因拆分局限**：卡 §3 的两类成因在 verdict 中以**定义**形式记录，未做逐臂归属；
  `--no-handoff` 使「信号稀疏」通道不可观测，因此本轮**不能**证伪「空仓市况主导」。
- **回滚**：结果中性、未出现不利证据，且 ADR 0006 默认翻转是用户显式豁免，**未回滚**；
  回滚须另立票 + 作废 ADR 0006。默认值 `auto` 保持不变。
- **接线代码**：`scripts/auto_regime_ab_run.py`（`derive_snapshot_id` / `extract_zero_open_days` /
  `assemble_run_evidence`）+ `tests/test_auto_regime_ab_run.py`（9 passed）；
  `scripts/run_auto_regime_ab.sh`（四跑编排）。产物是 gitignored 本地证据，不入库。
