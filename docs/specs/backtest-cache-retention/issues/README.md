# 票索引

准备态缓存保留策略的票按编号推进；**下一个可用编号：03**。

状态取值见 `docs/agents/triage-labels.md`（含终态 `done`）。`ready-for-agent` 且依赖全清的那一张就是当前 frontier。

| 票 | 主题 | 状态 | 依赖 |
| --- | --- | --- | --- |
| [01](01-prepared-cache-retention.md) | 准备态缓存保留策略（`--cache-keep`） | done | — |
| [02](02-no-grouped-retention.md) | 缓存保留**不**按口径分组（显式裁决） | done | 01 |

## 开新票前的两句提醒

- 每张票必须声明 `Gate:` / `Signal set:` /（条件性）`Gate rationale:`，并让 `Status:` 与本索引一致；`python scripts/check_spec_tickets.py` 在 CI 的 `ai-governance` 里强校验。
- 术语见 `GLOSSARY.md`「回测运行产物」段；缓存目录与用法的权威说明在 `docs/backtest-runbook.md` §3.1。
