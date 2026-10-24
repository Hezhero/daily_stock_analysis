# 票索引

度量层重建的票按编号推进；**下一个可用编号：10**。

状态取值见 `docs/agents/triage-labels.md`（`needs-triage` / `needs-info` / `ready-for-agent` / `ready-for-human` / `wontfix`）。`ready-for-agent` 且依赖全清的那一张就是当前 frontier。

| 票 | 主题 | 状态 | 依赖 |
| --- | --- | --- | --- |
| [01](01-contract-guard-metric-profile-override.md) | 契约守卫：`min_resonance_families` 由冻结常量改为度量画像 + 登记覆盖 | ready-for-human | — |
| [02](02-bootstrap-flip-gate.md) | 翻转门槛：池化样本外 Δ 的日期分块 bootstrap CI | ready-for-human | — |
| [03](03-card-a-flip-min-resonance-families.md) | 卡 A：翻转 `min_resonance_families` 默认值 | ready-for-human（FAIL，默认保持 2） | 01, 02 |
| [04](04-card-b-deployment-aware-sizing.md) | 卡 B：资金部署感知仓位 | ready-for-human（FAIL，默认保持 `fixed_1n`） | 01, 02 |
| [05](05-card-c-concurrent-signal-selection.md) | 卡 C：候选排序前两位对调（共振优先 vs 期望优先） | ready-for-human（FAIL，默认保持 `lead_exp_first`） | 01, 02, 04 |
| [06](06-card-d-moneyflow-factor.md) | 卡 D：`tushare_moneyflow` 分层主力资金因子 | ready-for-agent | 01–05 |
| [07](07-pit-universe.md) | PIT 宇宙落地（roadmap T0.4） | ready-for-human（默认已切 `pit`） | — |
| [08](08-gate-portfolio-level-estimator.md) | 门禁组合层估计量（不重判既有卡） | ready-for-human | — |
| [09](09-local-full-suite-baseline.md) | 本地全量 pytest 基线不可用 | ready-for-human | — |

## 开新票前的两句提醒

- 卡类票（03–06）在**开跑前**必须写定预注册：排序键、估计量选择、裁决口径。跑完再选口径等于翻案。
- 05 / 06 可以用 `--estimator portfolio`（见票 08 与 `docs/backtest-flip-gate-spec.md` §9），但一旦选了，必须在预注册里写明，不能事后切换。
